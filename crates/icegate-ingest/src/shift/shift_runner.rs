use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use icegate_common::retrier::{Retrier, RetrierConfig};
use icegate_jobmanager::{Error, ImmutableTask, JobManager};
use icegate_queue::{QueueReader, Topic};
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::{
    SegmentToRead, ShiftInput, ShiftOutput,
    executor::{TaskStatus, parse_task_input},
    iceberg_storage::Storage,
    row_groups_merger::{
        NoopRowGroupsMergerObserver, RowGroupsMerger, RowGroupsMergerObserver, SortedBatchMergerConfig,
    },
};
use crate::wal::SortColumnsDescriptor;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
/// Reason for shift task failure.
pub enum ShiftTaskFailureReason {
    /// Failed to read from the WAL queue.
    QueueRead,
    /// Failed to write parquet files.
    Write,
    /// No batches were available to process.
    EmptyBatches,
    /// No parquet files produced by the shift task.
    NoParquet,
    /// Failed to (de)serialize task payloads.
    Serialization,
    /// Task cancelled before completion.
    Cancelled,
}

impl ShiftTaskFailureReason {
    /// Return a stable string representation for metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QueueRead => "queue_read",
            Self::Write => "write",
            Self::EmptyBatches => "empty_batches",
            Self::NoParquet => "no_parquet",
            Self::Serialization => "serialization",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Shift task failure with reason and underlying error.
#[derive(Debug)]
pub struct ShiftTaskFailure {
    reason: ShiftTaskFailureReason,
    error: Error,
}

impl ShiftTaskFailure {
    /// Create a new shift task failure.
    pub const fn new(reason: ShiftTaskFailureReason, error: Error) -> Self {
        Self { reason, error }
    }

    /// Return the failure reason.
    pub const fn reason(&self) -> ShiftTaskFailureReason {
        self.reason
    }

    /// Convert into the underlying error.
    pub fn into_error(self) -> Error {
        self.error
    }
}

/// Result of a shift task execution.
pub struct ShiftTaskResult {
    /// Task execution status.
    pub status: TaskStatus,
    /// Total record batches processed.
    pub record_batches_total: usize,
    /// Total rows written.
    pub rows_total: usize,
    /// Parquet files produced.
    pub parquet_files_total: usize,
    /// Total bytes written.
    pub bytes_written_total: u64,
}

/// Runner interface for shift tasks.
#[async_trait]
pub trait ShiftTaskRunner: Send + Sync {
    /// Execute a shift task.
    async fn run(
        &self,
        task: Arc<dyn ImmutableTask>,
        manager: &dyn JobManager,
        cancel_token: &CancellationToken,
    ) -> Result<ShiftTaskResult, ShiftTaskFailure>;
}

/// Shift task runner implementation.
pub struct ShiftTaskRunnerImpl<Q, S> {
    queue_reader: Arc<Q>,
    storage: Arc<S>,
    topic: Topic,
    output_batch_size: usize,
    segment_read_parallelism: usize,
    sort_descriptor: &'static SortColumnsDescriptor,
    retrier: Retrier,
    row_groups_merger_observer: Arc<dyn RowGroupsMergerObserver>,
}

impl<Q, S> ShiftTaskRunnerImpl<Q, S>
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
    const DEFAULT_SEGMENT_READ_PARALLELISM: usize = 8;

    /// Create a new shift task runner.
    ///
    /// # Errors
    ///
    /// Returns an error if `output_batch_size` is zero.
    pub fn new(
        queue_reader: Arc<Q>,
        storage: Arc<S>,
        topic: impl Into<String>,
        output_batch_size: usize,
        sort_descriptor: &'static SortColumnsDescriptor,
    ) -> std::result::Result<Self, crate::error::IngestError> {
        if output_batch_size == 0 {
            return Err(crate::error::IngestError::Config(
                "row_group_size must be greater than zero".to_string(),
            ));
        }

        Ok(Self {
            queue_reader,
            storage,
            topic: topic.into(),
            output_batch_size,
            segment_read_parallelism: Self::DEFAULT_SEGMENT_READ_PARALLELISM,
            sort_descriptor,
            retrier: Retrier::new(RetrierConfig::default()),
            row_groups_merger_observer: Arc::new(NoopRowGroupsMergerObserver),
        })
    }

    /// Set WAL segment read parallelism for shift execution.
    ///
    /// # Errors
    ///
    /// Returns an error if `segment_read_parallelism` is zero.
    pub fn with_segment_read_parallelism(
        mut self,
        segment_read_parallelism: usize,
    ) -> std::result::Result<Self, crate::error::IngestError> {
        if segment_read_parallelism == 0 {
            return Err(crate::error::IngestError::Config(
                "shift_segment_read_parallelism must be greater than zero".to_string(),
            ));
        }
        self.segment_read_parallelism = segment_read_parallelism;
        Ok(self)
    }

    /// Set merger observer for row group lifecycle and merge timing.
    #[must_use]
    pub fn with_row_groups_merger_observer(mut self, observer: Arc<dyn RowGroupsMergerObserver>) -> Self {
        self.row_groups_merger_observer = observer;
        self
    }
}

#[async_trait]
impl<Q, S> ShiftTaskRunner for ShiftTaskRunnerImpl<Q, S>
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
    #[tracing::instrument(name="shift_run", skip(self, task, manager, cancel_token), fields(task_id = %task.id()))]
    async fn run(
        &self,
        task: Arc<dyn ImmutableTask>,
        manager: &dyn JobManager,
        cancel_token: &CancellationToken,
    ) -> Result<ShiftTaskResult, ShiftTaskFailure> {
        let task_id = *task.id();
        if cancel_token.is_cancelled() {
            manager
                .complete_task(&task_id, Vec::new())
                .map_err(|err| ShiftTaskFailure::new(ShiftTaskFailureReason::Cancelled, err))?;
            return Ok(ShiftTaskResult {
                status: TaskStatus::Cancelled,
                record_batches_total: 0,
                rows_total: 0,
                parquet_files_total: 0,
                bytes_written_total: 0,
            });
        }

        let input: ShiftInput = parse_task_input(task.as_ref())
            .map_err(|err| ShiftTaskFailure::new(ShiftTaskFailureReason::Serialization, err))?;
        if input.segments.is_empty() {
            error!("shift: no segments provided, skipping");
            manager
                .complete_task(&task_id, Vec::new())
                .map_err(|err| ShiftTaskFailure::new(ShiftTaskFailureReason::EmptyBatches, err))?;
            return Ok(ShiftTaskResult {
                status: TaskStatus::Empty,
                record_batches_total: 0,
                rows_total: 0,
                parquet_files_total: 0,
                bytes_written_total: 0,
            });
        }

        let record_batches_total = input.segments.iter().map(|segment| segment.row_groups.len()).sum();
        let write_result = self
            .write_row_groups_with_retry(input.segments.as_slice(), cancel_token)
            .await
            .map_err(|err| ShiftTaskFailure::new(err.reason, err.error))?;

        if write_result.rows_written == 0 {
            return Err(ShiftTaskFailure::new(
                ShiftTaskFailureReason::EmptyBatches,
                Error::TaskExecution("shift produced no rows to write".to_string()),
            ));
        }

        if write_result.data_files.is_empty() {
            return Err(ShiftTaskFailure::new(
                ShiftTaskFailureReason::NoParquet,
                Error::TaskExecution("shift produced no parquet files to commit".to_string()),
            ));
        }

        let parquet_files = write_result
            .data_files
            .iter()
            .map(|data_file| data_file.file_path().to_string())
            .collect::<Vec<_>>();

        let bytes_written_total = write_result
            .data_files
            .iter()
            .map(iceberg::spec::DataFile::file_size_in_bytes)
            .sum();

        let output = ShiftOutput {
            parquet_files,
            trace_context: icegate_common::extract_current_trace_context(),
        };
        let output_payload = serde_json::to_vec(&output).map_err(|err| {
            ShiftTaskFailure::new(
                ShiftTaskFailureReason::Serialization,
                Error::TaskExecution(format!("failed to serialize shift output: {err}")),
            )
        })?;
        // TODO(med): If shift task failed, the old files physically remain in the object storage. We get garbage/leaked files.

        manager
            .complete_task(&task_id, output_payload)
            .map_err(|err| ShiftTaskFailure::new(ShiftTaskFailureReason::Serialization, err))?;

        Ok(ShiftTaskResult {
            status: TaskStatus::Ok,
            record_batches_total,
            rows_total: write_result.rows_written,
            parquet_files_total: write_result.data_files.len(),
            bytes_written_total,
        })
    }
}

impl<Q, S> ShiftTaskRunnerImpl<Q, S>
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
    async fn write_row_groups_with_retry(
        &self,
        segments: &[SegmentToRead],
        cancel_token: &CancellationToken,
    ) -> Result<crate::shift::iceberg_storage::WrittenDataFiles, ShiftWriteError> {
        // Since we use streaming and merging via k-way, in case of problems, we need to completely restart the flow along with reading.
        let attempt = AtomicUsize::new(0);
        let result = self
            .retrier
            .retry::<_, _, Result<crate::shift::iceberg_storage::WrittenDataFiles, ShiftWriteError>, ShiftWriteError>(
                || {
                    let current_attempt = attempt.fetch_add(1, Ordering::SeqCst) + 1;
                    async move {
                        match self.write_row_groups_once(segments, cancel_token).await {
                            Ok(result) => Ok((false, Ok(result))),
                            Err(err) => {
                                let retryable =
                                    matches!(err.reason, ShiftTaskFailureReason::Write) && err.is_retryable();
                                if retryable {
                                    tracing::warn!(
                                        attempt = current_attempt,
                                        error = %err,
                                        "shift write attempt failed, retrying with reopened WAL streams"
                                    );
                                }
                                Ok((retryable, Err(err)))
                            }
                        }
                    }
                },
                cancel_token,
            )
            .await?;

        match result {
            Ok(written) => Ok(written),
            Err(err) => Err(err),
        }
    }

    async fn write_row_groups_once(
        &self,
        segments: &[SegmentToRead],
        cancel_token: &CancellationToken,
    ) -> Result<crate::shift::iceberg_storage::WrittenDataFiles, ShiftWriteError> {
        let mut merger = RowGroupsMerger::new(
            Arc::clone(&self.queue_reader),
            segments,
            SortedBatchMergerConfig {
                row_group_size: self.output_batch_size,
                read_parallelism: self.segment_read_parallelism,
                topic: self.topic.clone(),
                cancel_token: cancel_token.clone(),
                sort_descriptor: self.sort_descriptor,
            },
        )
        .map_err(ShiftWriteError::queue_read)?
        .with_observer(Arc::clone(&self.row_groups_merger_observer));
        merger.prefetch_first_group().await.map_err(ShiftWriteError::queue_read)?;
        let merged_stream = merger.into_stream();
        self.storage
            .write_record_batches(merged_stream, cancel_token)
            .await
            .map_err(ShiftWriteError::from)
    }
}

struct ShiftWriteError {
    reason: ShiftTaskFailureReason,
    error: Error,
    source: Option<crate::error::IngestError>,
}

impl ShiftWriteError {
    fn queue_read(err: crate::error::IngestError) -> Self {
        if matches!(err, crate::error::IngestError::Cancelled) {
            return <Self as icegate_common::RetryError>::cancelled();
        }
        Self {
            reason: ShiftTaskFailureReason::QueueRead,
            error: Error::TaskExecution(err.to_string()),
            source: Some(err),
        }
    }

    fn is_retryable(&self) -> bool {
        self.source.as_ref().is_some_and(crate::error::IngestError::is_retryable)
    }
}

impl icegate_common::RetryError for ShiftWriteError {
    fn cancelled() -> Self {
        Self {
            reason: ShiftTaskFailureReason::Cancelled,
            error: Error::TaskExecution("shift task cancelled during write retry".to_string()),
            source: Some(crate::error::IngestError::Cancelled),
        }
    }

    fn max_attempts() -> Self {
        Self {
            reason: ShiftTaskFailureReason::Write,
            error: Error::TaskExecution("max retry attempts reached".to_string()),
            source: Some(crate::error::IngestError::MaxAttemptsReached),
        }
    }
}

impl From<crate::error::IngestError> for ShiftWriteError {
    fn from(err: crate::error::IngestError) -> Self {
        let reason = match err {
            crate::error::IngestError::ShiftQueueRead(_) => ShiftTaskFailureReason::QueueRead,
            crate::error::IngestError::Cancelled => ShiftTaskFailureReason::Cancelled,
            _ => ShiftTaskFailureReason::Write,
        };
        Self {
            reason,
            error: Error::TaskExecution(err.to_string()),
            source: Some(err),
        }
    }
}

impl std::fmt::Display for ShiftWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow::{
        array::{Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use futures::TryStreamExt;
    use iceberg::{
        Catalog, NamespaceIdent, TableCreation,
        io::FileIO,
        spec::{
            DataContentType, DataFile, DataFileBuilder, DataFileFormat, NestedField, PartitionSpec, PrimitiveType,
            Schema as IcebergSchema, Struct, Transform, Type,
        },
        table::Table,
    };
    use icegate_common::{
        ICEGATE_NAMESPACE,
        catalog::{CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle},
        schema::{COL_CLOUD_ACCOUNT_ID, COL_SERVICE_NAME, COL_TENANT_ID, COL_TIMESTAMP},
    };
    use icegate_jobmanager::{ImmutableTask, JobManager, TaskCode, TaskDefinition};
    use icegate_queue::{RowGroupPlanEntry, SegmentsPlan};
    use parquet::{
        arrow::arrow_writer::ArrowWriter,
        arrow::{PARQUET_FIELD_ID_META_KEY, arrow_reader::ParquetRecordBatchReaderBuilder},
        file::{
            properties::WriterProperties,
            reader::{FileReader, SerializedFileReader},
            statistics::Statistics,
        },
    };
    use tokio::{
        sync::{Mutex, Notify},
        time::{sleep, timeout},
    };
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{ShiftTaskFailureReason, ShiftTaskRunner, ShiftTaskRunnerImpl, ShiftWriteError};
    use crate::{
        error::{IngestError, Result},
        shift::{
            PlannedRowGroup, SHIFT_TASK_CODE, SegmentToRead, ShiftConfig, ShiftInput, ShiftOutput,
            executor::TaskStatus,
            iceberg_storage::{IcebergStorage, Storage, WrittenDataFiles, writer_max_parquet_bytes},
            plan_runner::{PlanTaskRunner, PlanTaskRunnerImpl},
            timeout::TimeoutEstimator,
        },
        wal::{SortColumnsDescriptor, logs_row_group_boundary_range_from_batch, sort_logs},
    };

    struct TestTask {
        id: Uuid,
        code: TaskCode,
        input: Vec<u8>,
        output: Vec<u8>,
        error: String,
        depends_on: Vec<Uuid>,
    }

    impl TestTask {
        fn new(input: &ShiftInput) -> Self {
            Self {
                id: Uuid::new_v4(),
                code: TaskCode::new("shift"),
                input: serde_json::to_vec(input).expect("serialize shift input"),
                output: Vec::new(),
                error: String::new(),
                depends_on: Vec::new(),
            }
        }
    }

    impl ImmutableTask for TestTask {
        fn id(&self) -> &Uuid {
            &self.id
        }
        fn code(&self) -> &TaskCode {
            &self.code
        }
        fn get_input(&self) -> &[u8] {
            &self.input
        }
        fn get_output(&self) -> &[u8] {
            &self.output
        }
        fn get_error(&self) -> &str {
            &self.error
        }
        fn depends_on(&self) -> &[Uuid] {
            &self.depends_on
        }
        fn is_expired(&self) -> bool {
            false
        }
        fn is_completed(&self) -> bool {
            false
        }
        fn is_failed(&self) -> bool {
            false
        }
        fn attempts(&self) -> u32 {
            0
        }
    }

    struct NoopJobManager;

    impl JobManager for NoopJobManager {
        fn add_task(&self, _task_def: TaskDefinition) -> std::result::Result<Uuid, icegate_jobmanager::Error> {
            panic!("add_task is not expected in shift runner tests");
        }
        fn complete_task(
            &self,
            _task_id: &Uuid,
            _output: Vec<u8>,
        ) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("complete_task is not expected in shift runner tests");
        }
        fn fail_task(&self, _task_id: &Uuid, _error_msg: &str) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("fail_task is not expected in shift runner tests");
        }
        fn set_next_start_at(
            &self,
            _next_start_at: DateTime<Utc>,
        ) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("set_next_start_at is not expected in shift runner tests");
        }
        fn get_task(&self, _task_id: &Uuid) -> std::result::Result<Arc<dyn ImmutableTask>, icegate_jobmanager::Error> {
            panic!("get_task is not expected in shift runner tests");
        }
        fn get_tasks_by_code(
            &self,
            _code: &TaskCode,
        ) -> std::result::Result<Vec<Arc<dyn ImmutableTask>>, icegate_jobmanager::Error> {
            panic!("get_tasks_by_code is not expected in shift runner tests");
        }
    }

    struct RecordingJobManager {
        completed: std::sync::Mutex<Vec<(Uuid, Vec<u8>)>>,
    }

    impl RecordingJobManager {
        fn new() -> Self {
            Self {
                completed: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl JobManager for RecordingJobManager {
        fn add_task(&self, _task_def: TaskDefinition) -> std::result::Result<Uuid, icegate_jobmanager::Error> {
            panic!("add_task is not expected in shift runner tests");
        }

        fn complete_task(&self, task_id: &Uuid, output: Vec<u8>) -> std::result::Result<(), icegate_jobmanager::Error> {
            self.completed.lock().expect("completed lock").push((*task_id, output));
            Ok(())
        }

        fn fail_task(&self, _task_id: &Uuid, _error_msg: &str) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("fail_task is not expected in shift runner tests");
        }

        fn set_next_start_at(
            &self,
            _next_start_at: DateTime<Utc>,
        ) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("set_next_start_at is not expected in shift runner tests");
        }

        fn get_task(&self, _task_id: &Uuid) -> std::result::Result<Arc<dyn ImmutableTask>, icegate_jobmanager::Error> {
            panic!("get_task is not expected in shift runner tests");
        }

        fn get_tasks_by_code(
            &self,
            _code: &TaskCode,
        ) -> std::result::Result<Vec<Arc<dyn ImmutableTask>>, icegate_jobmanager::Error> {
            panic!("get_tasks_by_code is not expected in shift runner tests");
        }
    }

    struct FakeQueueReader {
        batches_by_offset: HashMap<u64, Vec<arrow::record_batch::RecordBatch>>,
        delay_by_offset: HashMap<u64, Duration>,
        fail_offset: Option<u64>,
        started_reads: Option<Arc<AtomicUsize>>,
        active_reads: Option<Arc<AtomicUsize>>,
        max_active_reads: Option<Arc<AtomicUsize>>,
        concurrency_gate: Option<Arc<ReadConcurrencyGate>>,
    }

    struct ReadConcurrencyGate {
        required_parallel_reads: usize,
        entered_reads: AtomicUsize,
        is_open: AtomicBool,
        notify: Notify,
        wait_timeout: Duration,
    }

    impl ReadConcurrencyGate {
        fn new(required_parallel_reads: usize, wait_timeout: Duration) -> Self {
            Self {
                required_parallel_reads,
                entered_reads: AtomicUsize::new(0),
                is_open: AtomicBool::new(false),
                notify: Notify::new(),
                wait_timeout,
            }
        }

        async fn wait_until_open(&self) -> icegate_queue::Result<()> {
            let notified = self.notify.notified();
            if self.is_open.load(Ordering::SeqCst) {
                return Ok(());
            }
            let entered = self.entered_reads.fetch_add(1, Ordering::SeqCst) + 1;
            if entered >= self.required_parallel_reads {
                self.is_open.store(true, Ordering::SeqCst);
                self.notify.notify_waiters();
                return Ok(());
            }

            timeout(self.wait_timeout, notified).await.map_err(|_| {
                icegate_queue::QueueError::Metadata(
                    "read concurrency gate timed out: segment reads did not overlap".to_string(),
                )
            })?;
            Ok(())
        }
    }

    fn update_max_seen(max: &AtomicUsize, value: usize) {
        let mut observed = max.load(Ordering::SeqCst);
        while value > observed {
            match max.compare_exchange(observed, value, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(new_observed) => observed = new_observed,
            }
        }
    }

    struct ActiveReadGuard {
        counter: Option<Arc<AtomicUsize>>,
    }

    impl ActiveReadGuard {
        fn new(counter: Option<Arc<AtomicUsize>>) -> Self {
            Self { counter }
        }
    }

    impl Drop for ActiveReadGuard {
        fn drop(&mut self) {
            if let Some(counter) = &self.counter {
                counter.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }

    #[async_trait]
    impl icegate_queue::QueueReader for FakeQueueReader {
        async fn plan_segments(
            &self,
            _topic: &icegate_queue::Topic,
            _start_offset: u64,
            _fields: &[icegate_queue::ExtractField],
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::SegmentsPlan> {
            panic!("plan_segments is not expected in shift runner tests");
        }

        async fn read_segment(
            &self,
            _topic: &icegate_queue::Topic,
            offset: u64,
            record_batch_idxs: &[usize],
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::RecordBatchStream> {
            let record_batch_idxs = record_batch_idxs.to_vec();
            if let Some(started_reads) = &self.started_reads {
                started_reads.fetch_add(1, Ordering::SeqCst);
            }
            let _active_guard = self.active_reads.as_ref().map_or_else(
                || ActiveReadGuard::new(None),
                |active_reads| {
                    let current = active_reads.fetch_add(1, Ordering::SeqCst) + 1;
                    if let Some(max_active_reads) = &self.max_active_reads {
                        update_max_seen(max_active_reads, current);
                    }
                    ActiveReadGuard::new(Some(Arc::clone(active_reads)))
                },
            );
            if let Some(gate) = &self.concurrency_gate {
                gate.wait_until_open().await?;
            }
            if let Some(delay) = self.delay_by_offset.get(&offset) {
                sleep(*delay).await;
            }
            if self.fail_offset == Some(offset) {
                return Err(icegate_queue::QueueError::Metadata(format!(
                    "read failed for segment {offset}"
                )));
            }
            Ok(Box::pin(futures::stream::iter(
                self.batches_by_offset
                    .get(&offset)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                    .filter_map(move |(idx, batch)| record_batch_idxs.contains(&idx).then_some(batch))
                    .map(Ok),
            )))
        }
    }

    struct StreamFailingQueueReader {
        batches_by_offset: HashMap<u64, Vec<arrow::record_batch::RecordBatch>>,
        fail_after_batch_offset: Option<(u64, usize)>,
    }

    #[async_trait]
    impl icegate_queue::QueueReader for StreamFailingQueueReader {
        async fn plan_segments(
            &self,
            _topic: &icegate_queue::Topic,
            _start_offset: u64,
            _fields: &[icegate_queue::ExtractField],
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::SegmentsPlan> {
            panic!("plan_segments is not expected in shift runner tests");
        }

        async fn read_segment(
            &self,
            _topic: &icegate_queue::Topic,
            offset: u64,
            record_batch_idxs: &[usize],
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::RecordBatchStream> {
            let mut outputs = Vec::new();
            for &batch_idx in record_batch_idxs {
                let Some(batch) = self
                    .batches_by_offset
                    .get(&offset)
                    .and_then(|batches| batches.get(batch_idx).cloned())
                else {
                    continue;
                };

                outputs.push(Ok(batch));
                if let Some((fail_offset, fail_after_batch_index)) = self.fail_after_batch_offset
                    && fail_offset == offset
                    && batch_idx + 1 == fail_after_batch_index
                {
                    outputs.push(Err(icegate_queue::QueueError::Metadata(format!(
                        "stream read failed for segment {offset}"
                    ))));
                    break;
                }
            }
            Ok(Box::pin(futures::stream::iter(outputs)))
        }
    }

    struct FakeStorage {
        writes: Mutex<Vec<Vec<arrow::record_batch::RecordBatch>>>,
        write_calls: AtomicUsize,
        fail_attempts_remaining: AtomicUsize,
        fail_retryable: bool,
        returned_data_files: Vec<DataFile>,
    }

    impl FakeStorage {
        fn always_fail() -> Self {
            Self {
                writes: Mutex::new(Vec::new()),
                write_calls: AtomicUsize::new(0),
                fail_attempts_remaining: AtomicUsize::new(usize::MAX),
                fail_retryable: false,
                returned_data_files: Vec::new(),
            }
        }

        fn fail_then_succeed(fail_attempts: usize, returned_data_files: Vec<DataFile>) -> Self {
            Self {
                writes: Mutex::new(Vec::new()),
                write_calls: AtomicUsize::new(0),
                fail_attempts_remaining: AtomicUsize::new(fail_attempts),
                fail_retryable: true,
                returned_data_files,
            }
        }
    }

    #[async_trait]
    impl Storage for FakeStorage {
        async fn get_last_offset(&self, _cancel_token: &CancellationToken) -> Result<Option<u64>> {
            panic!("get_last_offset is not expected in shift runner tests");
        }

        async fn write_record_batches(
            &self,
            batches: crate::shift::iceberg_storage::BoxRecordBatchStream,
            _cancel_token: &CancellationToken,
        ) -> Result<WrittenDataFiles> {
            self.write_calls.fetch_add(1, Ordering::SeqCst);
            let attempt_batches = batches.try_collect::<Vec<_>>().await?;
            let rows_written = attempt_batches.iter().map(arrow::record_batch::RecordBatch::num_rows).sum();
            self.writes.lock().await.push(attempt_batches);
            if self
                .fail_attempts_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then_some(remaining.saturating_sub(1))
                })
                .is_ok()
            {
                return Err(if self.fail_retryable {
                    IngestError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "transient storage write failure",
                    ))
                } else {
                    IngestError::Shift("storage write failure".to_string())
                });
            }
            Ok(WrittenDataFiles {
                data_files: self.returned_data_files.clone(),
                rows_written,
            })
        }

        async fn get_data_files(
            &self,
            _parquet_paths: &[String],
            _cancel_token: &CancellationToken,
        ) -> Result<Vec<iceberg::spec::DataFile>> {
            panic!("get_data_files is not expected in shift runner tests");
        }

        async fn commit(
            &self,
            _data_files: Vec<iceberg::spec::DataFile>,
            _record_type: &str,
            _last_offset: u64,
            _cancel_token: &CancellationToken,
        ) -> Result<usize> {
            panic!("commit is not expected in shift runner tests");
        }
    }

    fn test_batch(value: i64) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("cloud_account_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("value", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("acc")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![value])) as ArrayRef,
            ],
        )
        .expect("batch")
    }

    #[allow(clippy::needless_pass_by_value)]
    fn logs_batch_for_shift(
        rows: Vec<(Option<&str>, Option<&str>, Option<i64>, i64)>,
    ) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("cloud_account_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("value", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(cloud_account_id, _, _, _)| *cloud_account_id)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, service_name, _, _)| *service_name).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, _, timestamp, _)| *timestamp).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, _, value)| *value).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .expect("logs batch")
    }

    fn ordered_single_row_batch(
        service_name: &'static str,
        timestamp_micros: i64,
        value: i64,
    ) -> arrow::record_batch::RecordBatch {
        logs_batch_for_shift(vec![(Some("acc"), Some(service_name), Some(timestamp_micros), value)])
    }

    fn values_from_batches(batches: &[arrow::record_batch::RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("int64 array");
                (0..values.len()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect()
    }

    fn parquet_bytes_from_batches(batches: &[arrow::record_batch::RecordBatch]) -> Vec<u8> {
        let mut buffer = Vec::new();
        let props = WriterProperties::builder().set_max_row_group_size(2).build();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, batches[0].schema(), Some(props)).expect("arrow writer");
            for batch in batches {
                writer.write(batch).expect("write batch");
                writer.flush().expect("flush row group");
            }
            writer.close().expect("close writer");
        }
        buffer
    }

    fn service_name_bounds_from_parquet(parquet_bytes: Vec<u8>) -> Vec<(String, String)> {
        let reader = SerializedFileReader::new(Bytes::from(parquet_bytes)).expect("serialized reader");
        reader
            .metadata()
            .row_groups()
            .iter()
            .map(|row_group| {
                let stats = row_group
                    .columns()
                    .get(1)
                    .and_then(|column| column.statistics())
                    .expect("service_name stats");
                let Statistics::ByteArray(stats) = stats else {
                    panic!("service_name must have byte array stats");
                };
                let min = std::str::from_utf8(stats.min_bytes_opt().expect("min bytes"))
                    .expect("utf8 min")
                    .to_string();
                let max = std::str::from_utf8(stats.max_bytes_opt().expect("max bytes"))
                    .expect("utf8 max")
                    .to_string();
                (min, max)
            })
            .collect()
    }

    fn test_data_file(path: &str, rows: u64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(128)
            .record_count(rows)
            .partition_spec_id(0)
            .partition(Struct::empty())
            .build()
            .expect("data file")
    }

    fn planned_row_groups(
        batches: &[arrow::record_batch::RecordBatch],
        row_group_idxs: &[usize],
    ) -> Vec<PlannedRowGroup> {
        row_group_idxs
            .iter()
            .map(|row_group_idx| {
                let batch = batches.get(*row_group_idx).expect("row group batch");
                PlannedRowGroup {
                    row_group_idx: *row_group_idx,
                    row_group_bytes: 1,
                    boundary_range: logs_row_group_boundary_range_from_batch(batch).expect("boundary range"),
                }
            })
            .collect()
    }

    #[allow(clippy::needless_pass_by_value)]
    fn logs_ingest_batch(
        rows: Vec<(&str, Option<&str>, Option<&str>, Option<i64>, i64)>,
    ) -> arrow::record_batch::RecordBatch {
        fn field_with_id(name: &str, data_type: DataType, nullable: bool, field_id: i32) -> Field {
            Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                field_id.to_string(),
            )]))
        }

        let schema = Arc::new(Schema::new(vec![
            field_with_id(COL_TENANT_ID, DataType::Utf8, false, 1),
            field_with_id(COL_CLOUD_ACCOUNT_ID, DataType::Utf8, true, 2),
            field_with_id(COL_SERVICE_NAME, DataType::Utf8, true, 3),
            field_with_id(COL_TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None), true, 4),
            field_with_id("row_id", DataType::Int64, false, 5),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(tenant_id, _, _, _, _)| *tenant_id).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(_, cloud_account_id, _, _, _)| *cloud_account_id)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, _, service_name, _, _)| *service_name).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, _, _, timestamp, _)| *timestamp).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, _, _, row_id)| *row_id).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .expect("logs ingest batch")
    }

    fn row_ids_from_batches(batches: &[arrow::record_batch::RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                let row_ids = batch.column(4).as_any().downcast_ref::<Int64Array>().expect("row_id");
                (0..batch.num_rows()).map(|row_idx| row_ids.value(row_idx)).collect::<Vec<_>>()
            })
            .collect()
    }

    fn tenant_ids_from_batches(batches: &[arrow::record_batch::RecordBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|batch| {
                let tenant_ids = batch.column(0).as_any().downcast_ref::<StringArray>().expect("tenant_id");
                (0..batch.num_rows())
                    .map(|row_idx| tenant_ids.value(row_idx).to_string())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[derive(Clone)]
    struct E2eWalSegment {
        offset: u64,
        row_groups: Vec<icegate_queue::PreparedWalRowGroup>,
    }

    struct E2eQueueReader {
        plan: SegmentsPlan,
        segments: HashMap<u64, Vec<icegate_queue::PreparedWalRowGroup>>,
    }

    #[async_trait]
    impl icegate_queue::QueueReader for E2eQueueReader {
        async fn plan_segments(
            &self,
            _topic: &icegate_queue::Topic,
            _start_offset: u64,
            _fields: &[icegate_queue::ExtractField],
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<SegmentsPlan> {
            Ok(self.plan.clone())
        }

        async fn read_segment(
            &self,
            _topic: &icegate_queue::Topic,
            offset: u64,
            record_batch_idxs: &[usize],
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::RecordBatchStream> {
            let row_groups = self.segments.get(&offset).cloned().unwrap_or_default();
            let requested = record_batch_idxs.to_vec();
            let batches = row_groups
                .into_iter()
                .enumerate()
                .filter_map(|(idx, row_group)| requested.contains(&idx).then_some(row_group.batch))
                .map(Ok)
                .collect::<Vec<_>>();
            Ok(Box::pin(futures::stream::iter(batches)))
        }
    }

    struct E2ePlanStorage;

    #[async_trait]
    impl Storage for E2ePlanStorage {
        async fn get_last_offset(&self, _cancel_token: &CancellationToken) -> Result<Option<u64>> {
            Ok(None)
        }

        async fn write_record_batches(
            &self,
            _batches: crate::shift::iceberg_storage::BoxRecordBatchStream,
            _cancel_token: &CancellationToken,
        ) -> Result<WrittenDataFiles> {
            panic!("write_record_batches is not expected in plan stage");
        }

        async fn get_data_files(
            &self,
            _parquet_paths: &[String],
            _cancel_token: &CancellationToken,
        ) -> Result<Vec<DataFile>> {
            panic!("get_data_files is not expected in plan stage");
        }

        async fn commit(
            &self,
            _data_files: Vec<DataFile>,
            _record_type: &str,
            _last_offset: u64,
            _cancel_token: &CancellationToken,
        ) -> Result<usize> {
            panic!("commit is not expected in plan stage");
        }
    }

    struct E2eParquetStorage {
        inner: IcebergStorage,
        written_data_files: Mutex<Vec<DataFile>>,
    }

    #[async_trait]
    impl Storage for E2eParquetStorage {
        async fn get_last_offset(&self, cancel_token: &CancellationToken) -> Result<Option<u64>> {
            self.inner.get_last_offset(cancel_token).await
        }

        async fn write_record_batches(
            &self,
            batches: crate::shift::iceberg_storage::BoxRecordBatchStream,
            cancel_token: &CancellationToken,
        ) -> Result<WrittenDataFiles> {
            let written = self.inner.write_record_batches(batches, cancel_token).await?;
            self.written_data_files.lock().await.extend(written.data_files.iter().cloned());
            Ok(written)
        }

        async fn get_data_files(
            &self,
            parquet_paths: &[String],
            cancel_token: &CancellationToken,
        ) -> Result<Vec<DataFile>> {
            self.inner.get_data_files(parquet_paths, cancel_token).await
        }

        async fn commit(
            &self,
            data_files: Vec<DataFile>,
            record_type: &str,
            last_offset: u64,
            cancel_token: &CancellationToken,
        ) -> Result<usize> {
            self.inner.commit(data_files, record_type, last_offset, cancel_token).await
        }
    }

    #[derive(Clone)]
    struct AddedTaskDefinition {
        id: Uuid,
        code: TaskCode,
        input: Vec<u8>,
    }

    struct E2eManager {
        added_tasks: std::sync::Mutex<Vec<AddedTaskDefinition>>,
        completed_tasks: std::sync::Mutex<Vec<Uuid>>,
    }

    impl E2eManager {
        fn new() -> Self {
            Self {
                added_tasks: std::sync::Mutex::new(Vec::new()),
                completed_tasks: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl JobManager for E2eManager {
        fn add_task(&self, task_def: TaskDefinition) -> std::result::Result<Uuid, icegate_jobmanager::Error> {
            let task_id = Uuid::new_v4();
            self.added_tasks.lock().expect("added tasks lock").push(AddedTaskDefinition {
                id: task_id,
                code: task_def.code().clone(),
                input: task_def.input().to_vec(),
            });
            Ok(task_id)
        }

        fn complete_task(
            &self,
            task_id: &Uuid,
            _output: Vec<u8>,
        ) -> std::result::Result<(), icegate_jobmanager::Error> {
            self.completed_tasks.lock().expect("completed tasks lock").push(*task_id);
            Ok(())
        }

        fn fail_task(&self, _task_id: &Uuid, _error_msg: &str) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("fail_task is not expected in e2e test");
        }

        fn set_next_start_at(
            &self,
            _next_start_at: DateTime<Utc>,
        ) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("set_next_start_at is not expected in e2e test");
        }

        fn get_task(&self, _task_id: &Uuid) -> std::result::Result<Arc<dyn ImmutableTask>, icegate_jobmanager::Error> {
            panic!("get_task is not expected in e2e test");
        }

        fn get_tasks_by_code(
            &self,
            _code: &TaskCode,
        ) -> std::result::Result<Vec<Arc<dyn ImmutableTask>>, icegate_jobmanager::Error> {
            panic!("get_tasks_by_code is not expected in e2e test");
        }
    }

    fn build_segments_plan(segments: &[E2eWalSegment]) -> SegmentsPlan {
        let mut entries: Vec<RowGroupPlanEntry> = Vec::new();
        for segment in segments {
            for (row_group_idx, row_group) in segment.row_groups.iter().enumerate() {
                let tenant_ids = row_group
                    .batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("tenant_id");
                let tenant_id = tenant_ids.value(0).to_string();
                let mut extracted = HashMap::new();
                extracted.insert(
                    crate::shift::plan_runner::PLAN_FIELD_TENANT_ID.to_string(),
                    icegate_queue::ExtractedValue::Utf8(tenant_id),
                );
                if let Some(payload) = row_group.metadata.clone() {
                    extracted.insert(
                        crate::shift::plan_runner::PLAN_FIELD_BOUNDARY_RANGE.to_string(),
                        icegate_queue::ExtractedValue::Utf8(payload),
                    );
                }
                // Extract physical timestamp min/max from the batch column (index 3).
                // All test timestamps are tiny values (< 1 second) so they always
                // fall within day 0; a TimestampMicrosRange is required because
                // CURRENT_PLANNER_PARTITION_SPEC has required=true for the day field.
                let ts_col = row_group
                    .batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("timestamp column at index 3");
                let mut valid_ts = (0..ts_col.len()).filter(|&i| ts_col.is_valid(i)).map(|i| ts_col.value(i));
                if let Some(first) = valid_ts.next() {
                    let (min_ts, max_ts) = valid_ts.fold((first, first), |(mn, mx), v| (mn.min(v), mx.max(v)));
                    extracted.insert(
                        crate::shift::plan_runner::PLAN_FIELD_TIMESTAMP_RANGE.to_string(),
                        icegate_queue::ExtractedValue::TimestampMicrosRange(min_ts, max_ts),
                    );
                }
                entries.push(RowGroupPlanEntry {
                    wal_offset: segment.offset,
                    row_group_idx,
                    row_group_bytes: 1,
                    extracted,
                });
            }
        }

        let row_groups_total = entries.len();
        SegmentsPlan {
            entries,
            last_segment_offset: segments.iter().map(|segment| segment.offset).max(),
            segments_count: segments.len(),
            row_groups_total,
            input_bytes_total: row_groups_total as u64,
        }
    }

    async fn create_e2e_logs_table(table_name: &str) -> (Arc<dyn Catalog>, Table) {
        let catalog_config = CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: format!("memory://shift-runner-e2e-{}", Uuid::new_v4()),
            properties: HashMap::new(),
            cache: None,
        };
        let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop())
            .await
            .expect("memory catalog");
        let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");

        let schema = IcebergSchema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, COL_TENANT_ID, Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(2, COL_CLOUD_ACCOUNT_ID, Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(3, COL_SERVICE_NAME, Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(4, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)).into(),
                NestedField::required(5, "row_id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("e2e logs schema");
        let partition_spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(1)
            .add_partition_field(COL_TENANT_ID, COL_TENANT_ID, Transform::Identity)
            .expect("tenant partition")
            .add_partition_field(COL_TIMESTAMP, "timestamp_day", Transform::Day)
            .expect("timestamp partition")
            .build()
            .expect("partition spec");
        let sort_order = icegate_common::schema::logs_sort_order(&schema).expect("logs sort order");
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(table_name.to_string())
                    .schema(schema)
                    .partition_spec(partition_spec)
                    .sort_order(sort_order)
                    .build(),
            )
            .await
            .expect("create logs table");
        (catalog, table)
    }

    #[derive(Debug, Eq, PartialEq)]
    struct LogOutputRow {
        tenant_id: String,
        cloud_account_id: Option<String>,
        service_name: Option<String>,
        timestamp: i64,
        row_id: i64,
    }

    fn nullable_string_value(array: &StringArray, row_idx: usize) -> Option<String> {
        (!array.is_null(row_idx)).then(|| array.value(row_idx).to_string())
    }

    fn rows_from_record_batches(batches: &[arrow::record_batch::RecordBatch]) -> Vec<LogOutputRow> {
        batches
            .iter()
            .flat_map(|batch| {
                let tenant_ids = batch
                    .column_by_name(COL_TENANT_ID)
                    .expect("tenant_id column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("tenant_id string");
                let cloud_account_ids = batch
                    .column_by_name(COL_CLOUD_ACCOUNT_ID)
                    .expect("cloud_account_id column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("cloud_account_id string");
                let service_names = batch
                    .column_by_name(COL_SERVICE_NAME)
                    .expect("service_name column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("service_name string");
                let timestamps = batch
                    .column_by_name(COL_TIMESTAMP)
                    .expect("timestamp column")
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("timestamp micros");
                let row_ids = batch
                    .column_by_name("row_id")
                    .expect("row_id column")
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("row_id int64");

                (0..batch.num_rows())
                    .map(|row_idx| LogOutputRow {
                        tenant_id: tenant_ids.value(row_idx).to_string(),
                        cloud_account_id: nullable_string_value(cloud_account_ids, row_idx),
                        service_name: nullable_string_value(service_names, row_idx),
                        timestamp: timestamps.value(row_idx),
                        row_id: row_ids.value(row_idx),
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn log_sort_key_cmp(left: &LogOutputRow, right: &LogOutputRow) -> std::cmp::Ordering {
        left.cloud_account_id
            .cmp(&right.cloud_account_id)
            .then_with(|| left.service_name.cmp(&right.service_name))
            .then_with(|| right.timestamp.cmp(&left.timestamp))
    }

    async fn read_parquet_output_rows(file_io: &FileIO, path: &str) -> Vec<LogOutputRow> {
        let bytes = file_io
            .new_input(path)
            .expect("parquet input")
            .read()
            .await
            .expect("read parquet");
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("parquet reader builder")
            .build()
            .expect("parquet record reader");
        let batches = reader.collect::<std::result::Result<Vec<_>, _>>().expect("read record batches");
        rows_from_record_batches(&batches)
    }

    async fn assert_parquet_row_group_bounds_match_sorted_rows(file_io: &FileIO, path: &str) {
        let bytes = file_io
            .new_input(path)
            .expect("parquet input")
            .read()
            .await
            .expect("read parquet");
        let reader = SerializedFileReader::new(bytes).expect("serialized parquet reader");
        let metadata = reader.metadata();
        assert!(metadata.num_row_groups() > 0, "parquet file must contain row groups");

        let record_reader = ParquetRecordBatchReaderBuilder::try_new(
            file_io
                .new_input(path)
                .expect("parquet input")
                .read()
                .await
                .expect("read parquet"),
        )
        .expect("parquet reader builder")
        .build()
        .expect("parquet record reader");
        let batches = record_reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("read record batches");
        let rows = rows_from_record_batches(&batches);
        let mut row_offset = 0usize;
        for row_group_idx in 0..metadata.num_row_groups() {
            let row_group = metadata.row_group(row_group_idx);
            let row_count = usize::try_from(row_group.num_rows()).expect("row group row count");
            let row_group_rows = &rows[row_offset..row_offset + row_count];
            row_offset += row_count;

            for window in row_group_rows.windows(2) {
                assert_ne!(
                    log_sort_key_cmp(&window[0], &window[1]),
                    std::cmp::Ordering::Greater,
                    "row group {row_group_idx} must not contradict logs sort order"
                );
            }

            let service_stats = row_group
                .columns()
                .get(2)
                .and_then(|column| column.statistics())
                .expect("service_name statistics");
            let Statistics::ByteArray(service_stats) = service_stats else {
                panic!("service_name must have byte-array statistics");
            };
            let min_service =
                std::str::from_utf8(service_stats.min_bytes_opt().expect("service min")).expect("service min utf8");
            let max_service =
                std::str::from_utf8(service_stats.max_bytes_opt().expect("service max")).expect("service max utf8");
            let actual_min_service = row_group_rows
                .iter()
                .filter_map(|row| row.service_name.as_deref())
                .min()
                .expect("actual min service");
            let actual_max_service = row_group_rows
                .iter()
                .filter_map(|row| row.service_name.as_deref())
                .max()
                .expect("actual max service");
            assert_eq!(min_service, actual_min_service);
            assert_eq!(max_service, actual_max_service);
        }
        assert_eq!(row_offset, rows.len(), "metadata row counts must cover all rows");
    }

    fn test_timeouts() -> TimeoutEstimator {
        TimeoutEstimator::new(&crate::shift::config::ShiftTimeoutsConfig {
            plan_base_ms: 1,
            shift_base_ms: 1,
            shift_per_record_batch_ms: 1,
            shift_per_segment_ms: 1,
            commit_base_ms: 1,
            commit_per_parquet_file_ms: 1,
        })
        .expect("timeouts")
    }

    #[tokio::test]
    async fn run_preserves_segment_order_when_reads_complete_out_of_order() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([
                (1, vec![test_batch(1)]),
                (2, vec![test_batch(2)]),
                (3, vec![test_batch(3)]),
            ]),
            delay_by_offset: HashMap::from([
                (1, Duration::from_millis(80)),
                (2, Duration::from_millis(5)),
                (3, Duration::from_millis(20)),
            ]),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::always_fail());
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted")
        .with_segment_read_parallelism(3)
        .expect("non-zero segment read parallelism must be accepted");
        let segment_1 = vec![test_batch(1)];
        let segment_2 = vec![test_batch(2)];
        let segment_3 = vec![test_batch(3)];
        let input = ShiftInput {
            segments: vec![
                SegmentToRead {
                    segment_offset: 1,
                    row_groups: planned_row_groups(&segment_1, &[0]),
                },
                SegmentToRead {
                    segment_offset: 2,
                    row_groups: planned_row_groups(&segment_2, &[0]),
                },
                SegmentToRead {
                    segment_offset: 3,
                    row_groups: planned_row_groups(&segment_3, &[0]),
                },
            ],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = NoopJobManager;
        let cancel = CancellationToken::new();

        let Err(err) = runner.run(task, &manager, &cancel).await else {
            panic!("storage write is expected to fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::Write);

        let writes = storage.writes.lock().await;
        assert_eq!(writes.len(), 1);
        assert_eq!(values_from_batches(&writes[0]), vec![1, 2, 3]);
        drop(writes);
    }

    #[tokio::test]
    async fn run_merges_row_groups_within_single_segment_globally() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(
                1,
                vec![
                    logs_batch_for_shift(vec![
                        (Some("acc-1"), Some("svc-3"), Some(30), 1),
                        (Some("acc-1"), Some("svc-4"), Some(20), 2),
                    ]),
                    logs_batch_for_shift(vec![
                        (Some("acc-1"), Some("svc-2"), Some(40), 3),
                        (Some("acc-1"), Some("svc-5"), Some(10), 4),
                    ]),
                ],
            )]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::always_fail());
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            4,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted");
        let segment_1 = vec![
            logs_batch_for_shift(vec![
                (Some("acc-1"), Some("svc-3"), Some(30), 1),
                (Some("acc-1"), Some("svc-4"), Some(20), 2),
            ]),
            logs_batch_for_shift(vec![
                (Some("acc-1"), Some("svc-2"), Some(40), 3),
                (Some("acc-1"), Some("svc-5"), Some(10), 4),
            ]),
        ];
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&segment_1, &[0, 1]),
            }],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = NoopJobManager;
        let cancel = CancellationToken::new();

        let Err(err) = runner.run(task, &manager, &cancel).await else {
            panic!("storage write is expected to fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::Write);

        let writes = storage.writes.lock().await;
        assert_eq!(writes.len(), 1);
        assert_eq!(values_from_batches(&writes[0]), vec![3, 1, 2, 4]);
        drop(writes);
    }

    #[tokio::test]
    async fn run_merger_stream_produces_non_overlapping_service_name_bounds_when_reencoded() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(
                1,
                vec![
                    logs_batch_for_shift(vec![
                        (Some("acc-1"), Some("svc-3"), Some(30), 1),
                        (Some("acc-1"), Some("svc-4"), Some(20), 2),
                    ]),
                    logs_batch_for_shift(vec![
                        (Some("acc-1"), Some("svc-2"), Some(40), 3),
                        (Some("acc-1"), Some("svc-5"), Some(10), 4),
                    ]),
                ],
            )]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::always_fail());
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            2,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted");
        let segment_1 = vec![
            logs_batch_for_shift(vec![
                (Some("acc-1"), Some("svc-3"), Some(30), 1),
                (Some("acc-1"), Some("svc-4"), Some(20), 2),
            ]),
            logs_batch_for_shift(vec![
                (Some("acc-1"), Some("svc-2"), Some(40), 3),
                (Some("acc-1"), Some("svc-5"), Some(10), 4),
            ]),
        ];
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&segment_1, &[0, 1]),
            }],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = NoopJobManager;
        let cancel = CancellationToken::new();

        let Err(err) = runner.run(task, &manager, &cancel).await else {
            panic!("storage write is expected to fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::Write);

        let writes = storage.writes.lock().await;
        assert_eq!(writes.len(), 1);
        let parquet_bytes = parquet_bytes_from_batches(&writes[0]);
        drop(writes);
        let bounds = service_name_bounds_from_parquet(parquet_bytes);

        assert_eq!(
            bounds,
            vec![
                ("svc-2".to_string(), "svc-3".to_string()),
                ("svc-4".to_string(), "svc-5".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn run_retries_transient_write_failure_with_reopened_wal_streams() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([
                (1, vec![test_batch(1)]),
                (2, vec![test_batch(2)]),
                (3, vec![test_batch(3)]),
            ]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::fail_then_succeed(
            1,
            vec![test_data_file("s3://warehouse/logs/part-00001.parquet", 3)],
        ));
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            2,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted")
        .with_segment_read_parallelism(3)
        .expect("non-zero segment read parallelism must be accepted");
        let segment_1 = vec![test_batch(1)];
        let segment_2 = vec![test_batch(2)];
        let segment_3 = vec![test_batch(3)];
        let input = ShiftInput {
            segments: vec![
                SegmentToRead {
                    segment_offset: 1,
                    row_groups: planned_row_groups(&segment_1, &[0]),
                },
                SegmentToRead {
                    segment_offset: 2,
                    row_groups: planned_row_groups(&segment_2, &[0]),
                },
                SegmentToRead {
                    segment_offset: 3,
                    row_groups: planned_row_groups(&segment_3, &[0]),
                },
            ],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = RecordingJobManager::new();
        let cancel = CancellationToken::new();

        let result = runner.run(task, &manager, &cancel).await.expect("write retry must succeed");

        assert_eq!(result.status, TaskStatus::Ok);
        assert_eq!(result.rows_total, 3);
        assert_eq!(result.parquet_files_total, 1);
        assert_eq!(result.bytes_written_total, 128);
        assert_eq!(storage.write_calls.load(Ordering::SeqCst), 2);

        let writes = storage.writes.lock().await;
        assert_eq!(writes.len(), 2);
        assert_eq!(values_from_batches(&writes[0]), vec![1, 2, 3]);
        assert_eq!(values_from_batches(&writes[1]), vec![1, 2, 3]);
        assert_eq!(
            writes[1].iter().map(arrow::record_batch::RecordBatch::num_rows).sum::<usize>(),
            3
        );
        drop(writes);

        let completed = manager.completed.lock().expect("completed lock");
        assert_eq!(completed.len(), 1);
        let output: ShiftOutput = serde_json::from_slice(&completed[0].1).expect("shift output");
        drop(completed);
        assert_eq!(
            output.parquet_files,
            vec!["s3://warehouse/logs/part-00001.parquet".to_string()]
        );
    }

    #[tokio::test]
    async fn run_fails_fast_on_queue_read_error_and_skips_storage_write() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(1, vec![test_batch(1)]), (3, vec![test_batch(3)])]),
            delay_by_offset: HashMap::from([(1, Duration::from_millis(40)), (2, Duration::from_millis(5))]),
            fail_offset: Some(2),
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::fail_then_succeed(0, Vec::new()));
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted")
        .with_segment_read_parallelism(3)
        .expect("non-zero segment read parallelism must be accepted");
        let segment_1 = vec![test_batch(1)];
        let segment_2 = vec![test_batch(2)];
        let segment_3 = vec![test_batch(3)];
        let input = ShiftInput {
            segments: vec![
                SegmentToRead {
                    segment_offset: 1,
                    row_groups: planned_row_groups(&segment_1, &[0]),
                },
                SegmentToRead {
                    segment_offset: 2,
                    row_groups: planned_row_groups(&segment_2, &[0]),
                },
                SegmentToRead {
                    segment_offset: 3,
                    row_groups: planned_row_groups(&segment_3, &[0]),
                },
            ],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = NoopJobManager;
        let cancel = CancellationToken::new();

        let Err(err) = runner.run(task, &manager, &cancel).await else {
            panic!("queue read must fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::QueueRead);
        assert_eq!(
            storage.write_calls.load(Ordering::SeqCst),
            0,
            "queue read failure before write pipeline start must not call storage write"
        );
    }

    #[tokio::test]
    async fn run_fails_on_late_queue_read_and_preserves_typed_reason() {
        let queue_reader = Arc::new(StreamFailingQueueReader {
            batches_by_offset: HashMap::from([(
                1,
                vec![
                    ordered_single_row_batch("svc", 30, 1),
                    ordered_single_row_batch("svc", 20, 2),
                    ordered_single_row_batch("svc", 10, 3),
                ],
            )]),
            fail_after_batch_offset: Some((1, 2)),
        });
        let storage = Arc::new(FakeStorage::fail_then_succeed(0, Vec::new()));
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted");
        let segment_1 = vec![
            ordered_single_row_batch("svc", 30, 1),
            ordered_single_row_batch("svc", 20, 2),
            ordered_single_row_batch("svc", 10, 3),
        ];
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&segment_1, &[0, 1, 2]),
            }],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = NoopJobManager;
        let cancel = CancellationToken::new();

        let Err(err) = runner.run(task, &manager, &cancel).await else {
            panic!("late queue read must fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::QueueRead);
        assert_eq!(
            storage.write_calls.load(Ordering::SeqCst),
            1,
            "late queue read after first prefetch batch is allowed to start storage write pipeline"
        );
        assert_eq!(storage.writes.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn run_proves_concurrent_reads_for_parallelism_greater_than_one() {
        let active_reads = Arc::new(AtomicUsize::new(0));
        let max_active_reads = Arc::new(AtomicUsize::new(0));
        let segments = [
            vec![logs_batch_for_shift(vec![
                (Some("acc"), Some("svc"), Some(100), 1),
                (Some("acc"), Some("svc"), Some(70), 2),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("acc"), Some("svc"), Some(95), 3),
                (Some("acc"), Some("svc"), Some(65), 4),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("acc"), Some("svc"), Some(90), 5),
                (Some("acc"), Some("svc"), Some(60), 6),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("acc"), Some("svc"), Some(85), 7),
                (Some("acc"), Some("svc"), Some(55), 8),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("acc"), Some("svc"), Some(80), 9),
                (Some("acc"), Some("svc"), Some(50), 10),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("acc"), Some("svc"), Some(75), 11),
                (Some("acc"), Some("svc"), Some(45), 12),
            ])],
        ];
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([
                (1, segments[0].clone()),
                (2, segments[1].clone()),
                (3, segments[2].clone()),
                (4, segments[3].clone()),
                (5, segments[4].clone()),
                (6, segments[5].clone()),
            ]),
            delay_by_offset: HashMap::from([
                (1, Duration::from_millis(40)),
                (2, Duration::from_millis(40)),
                (3, Duration::from_millis(40)),
                (4, Duration::from_millis(40)),
                (5, Duration::from_millis(40)),
                (6, Duration::from_millis(40)),
            ]),
            fail_offset: None,
            started_reads: None,
            active_reads: Some(Arc::clone(&active_reads)),
            max_active_reads: Some(Arc::clone(&max_active_reads)),
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::always_fail());
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted")
        .with_segment_read_parallelism(2)
        .expect("non-zero segment read parallelism must be accepted");
        let input = ShiftInput {
            segments: vec![
                SegmentToRead {
                    segment_offset: 1,
                    row_groups: planned_row_groups(&segments[0], &[0]),
                },
                SegmentToRead {
                    segment_offset: 2,
                    row_groups: planned_row_groups(&segments[1], &[0]),
                },
                SegmentToRead {
                    segment_offset: 3,
                    row_groups: planned_row_groups(&segments[2], &[0]),
                },
                SegmentToRead {
                    segment_offset: 4,
                    row_groups: planned_row_groups(&segments[3], &[0]),
                },
                SegmentToRead {
                    segment_offset: 5,
                    row_groups: planned_row_groups(&segments[4], &[0]),
                },
                SegmentToRead {
                    segment_offset: 6,
                    row_groups: planned_row_groups(&segments[5], &[0]),
                },
            ],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = NoopJobManager;
        let cancel = CancellationToken::new();

        let Err(err) = runner.run(task, &manager, &cancel).await else {
            panic!("storage write is expected to fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::Write);
        assert!(
            max_active_reads.load(Ordering::SeqCst) <= 2,
            "max in-flight reads must not exceed configured parallelism"
        );
        assert!(
            max_active_reads.load(Ordering::SeqCst) >= 2,
            "parallel read path must overlap at least two in-flight reads"
        );
        assert_eq!(active_reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn run_keeps_reads_strictly_sequential_for_parallelism_one() {
        let active_reads = Arc::new(AtomicUsize::new(0));
        let max_active_reads = Arc::new(AtomicUsize::new(0));
        let segments = [
            vec![ordered_single_row_batch("svc", 30, 1)],
            vec![ordered_single_row_batch("svc", 30, 2)],
            vec![ordered_single_row_batch("svc", 30, 3)],
        ];
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([
                (1, segments[0].clone()),
                (2, segments[1].clone()),
                (3, segments[2].clone()),
            ]),
            delay_by_offset: HashMap::from([
                (1, Duration::from_millis(25)),
                (2, Duration::from_millis(25)),
                (3, Duration::from_millis(25)),
            ]),
            fail_offset: None,
            started_reads: None,
            active_reads: Some(Arc::clone(&active_reads)),
            max_active_reads: Some(Arc::clone(&max_active_reads)),
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::always_fail());
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted")
        .with_segment_read_parallelism(1)
        .expect("non-zero segment read parallelism must be accepted");
        let input = ShiftInput {
            segments: vec![
                SegmentToRead {
                    segment_offset: 1,
                    row_groups: planned_row_groups(&segments[0], &[0]),
                },
                SegmentToRead {
                    segment_offset: 2,
                    row_groups: planned_row_groups(&segments[1], &[0]),
                },
                SegmentToRead {
                    segment_offset: 3,
                    row_groups: planned_row_groups(&segments[2], &[0]),
                },
            ],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = NoopJobManager;
        let cancel = CancellationToken::new();

        let Err(err) = runner.run(task, &manager, &cancel).await else {
            panic!("storage write is expected to fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::Write);
        assert_eq!(
            max_active_reads.load(Ordering::SeqCst),
            1,
            "parallelism=1 must keep exactly one in-flight read"
        );
        assert_eq!(active_reads.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn new_rejects_zero_output_batch_size() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::new(),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::fail_then_succeed(0, Vec::new()));

        let result = ShiftTaskRunnerImpl::new(
            queue_reader,
            storage,
            "logs",
            0,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        );

        match result {
            Ok(_) => panic!("zero row_group_size must be rejected"),
            Err(crate::error::IngestError::Config(_)) => {}
            Err(other) => panic!("expected config error, got: {other}"),
        }
    }

    #[test]
    fn with_segment_read_parallelism_rejects_zero() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::new(),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::fail_then_succeed(0, Vec::new()));

        let result = ShiftTaskRunnerImpl::new(
            queue_reader,
            storage,
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted")
        .with_segment_read_parallelism(0);

        match result {
            Ok(_) => panic!("zero shift_segment_read_parallelism must be rejected"),
            Err(crate::error::IngestError::Config(_)) => {}
            Err(other) => panic!("expected config error, got: {other}"),
        }
    }

    #[tokio::test]
    async fn run_respects_shift_segment_read_parallelism_for_reading() {
        let active_reads = Arc::new(AtomicUsize::new(0));
        let max_active_reads = Arc::new(AtomicUsize::new(0));
        let segments = [
            vec![ordered_single_row_batch("svc-01", 30, 1)],
            vec![ordered_single_row_batch("svc-02", 20, 2)],
            vec![ordered_single_row_batch("svc-03", 10, 3)],
        ];
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([
                (1, segments[0].clone()),
                (2, segments[1].clone()),
                (3, segments[2].clone()),
            ]),
            delay_by_offset: HashMap::from([
                (1, Duration::from_millis(40)),
                (2, Duration::from_millis(40)),
                (3, Duration::from_millis(40)),
            ]),
            fail_offset: None,
            started_reads: None,
            active_reads: Some(Arc::clone(&active_reads)),
            max_active_reads: Some(Arc::clone(&max_active_reads)),
            concurrency_gate: None,
        });
        let storage = Arc::new(FakeStorage::always_fail());
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted")
        .with_segment_read_parallelism(2)
        .expect("non-zero segment read parallelism must be accepted");
        let input = ShiftInput {
            segments: vec![
                SegmentToRead {
                    segment_offset: 1,
                    row_groups: planned_row_groups(&segments[0], &[0]),
                },
                SegmentToRead {
                    segment_offset: 2,
                    row_groups: planned_row_groups(&segments[1], &[0]),
                },
                SegmentToRead {
                    segment_offset: 3,
                    row_groups: planned_row_groups(&segments[2], &[0]),
                },
            ],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = NoopJobManager;
        let cancel = CancellationToken::new();

        let Err(err) = runner.run(task, &manager, &cancel).await else {
            panic!("storage write is expected to fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::Write);
        assert!(
            max_active_reads.load(Ordering::SeqCst) <= 2,
            "shift_segment_read_parallelism must cap in-flight reads"
        );
    }

    #[test]
    fn shift_write_error_classifies_queue_read_by_type_not_by_message() {
        let typed = ShiftWriteError::from(IngestError::ShiftQueueRead(
            "arbitrary queue read failure text".to_string(),
        ));
        assert_eq!(typed.reason, ShiftTaskFailureReason::QueueRead);

        let plain_shift_with_same_words = ShiftWriteError::from(IngestError::Shift(
            "failed to open WAL segment 7 row group 0: but this is plain Shift variant".to_string(),
        ));
        assert_eq!(plain_shift_with_same_words.reason, ShiftTaskFailureReason::Write);
    }

    #[tokio::test]
    async fn run_stops_reading_and_returns_cancelled_after_cancellation() {
        let started_reads = Arc::new(AtomicUsize::new(0));
        let active_reads = Arc::new(AtomicUsize::new(0));
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([
                (1, vec![test_batch(1)]),
                (2, vec![test_batch(2)]),
                (3, vec![test_batch(3)]),
                (4, vec![test_batch(4)]),
            ]),
            delay_by_offset: HashMap::from([
                (1, Duration::from_secs(5)),
                (2, Duration::from_secs(5)),
                (3, Duration::from_secs(5)),
                (4, Duration::from_secs(5)),
            ]),
            fail_offset: None,
            started_reads: Some(Arc::clone(&started_reads)),
            active_reads: Some(Arc::clone(&active_reads)),
            max_active_reads: None,
            concurrency_gate: Some(Arc::new(ReadConcurrencyGate::new(2, Duration::from_secs(2)))),
        });
        let storage = Arc::new(FakeStorage::fail_then_succeed(0, Vec::new()));
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted")
        .with_segment_read_parallelism(2)
        .expect("non-zero segment read parallelism must be accepted");
        let segments = (1..=4).map(test_batch).map(|batch| vec![batch]).collect::<Vec<_>>();
        let input = ShiftInput {
            segments: vec![
                SegmentToRead {
                    segment_offset: 1,
                    row_groups: planned_row_groups(&segments[0], &[0]),
                },
                SegmentToRead {
                    segment_offset: 2,
                    row_groups: planned_row_groups(&segments[1], &[0]),
                },
                SegmentToRead {
                    segment_offset: 3,
                    row_groups: planned_row_groups(&segments[2], &[0]),
                },
                SegmentToRead {
                    segment_offset: 4,
                    row_groups: planned_row_groups(&segments[3], &[0]),
                },
            ],
            trace_context: None,
        };
        let task = Arc::new(TestTask::new(&input));
        let manager = NoopJobManager;
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();

        let run_handle = tokio::spawn(async move { runner.run(task, &manager, &cancel_for_task).await });
        sleep(Duration::from_millis(50)).await;
        cancel.cancel();

        let run_result = timeout(Duration::from_secs(1), run_handle)
            .await
            .expect("runner must stop promptly after cancellation")
            .expect("shift runner task must join successfully");

        let Err(err) = run_result else {
            panic!("cancellation must fail shift run");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::Cancelled);
        assert_eq!(
            storage.write_calls.load(Ordering::SeqCst),
            0,
            "cancellation before write pipeline start must not call storage write"
        );
        // No partial parquet output may exist after cancellation. The mock's
        // `writes` accumulator collects every batch that reached
        // `write_record_batches`; an empty vec proves the runner cancelled
        // the queue stream before any data crossed the writer boundary.
        assert!(
            storage.writes.lock().await.is_empty(),
            "no partial parquet payload may have been buffered into storage under cancellation"
        );
        // FakeStorage::get_data_files and FakeStorage::commit both panic on
        // call; the runner reaching those methods would have aborted the
        // tokio task before this point, so simply reaching the post-join
        // assertions here is also a guarantee that neither the
        // file-finalization nor the snapshot-commit phase ran.
        assert!(
            started_reads.load(Ordering::SeqCst) <= 2,
            "cancellation must stop scheduling reads beyond the in-flight parallelism window"
        );
        assert_eq!(active_reads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn run_end_to_end_writes_rows_sorted_in_actual_parquet_files() {
        let ingest_segment_100 = logs_ingest_batch(vec![
            ("tenant-b", Some("acc-2"), Some("svc-z"), Some(10), 900),
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 101),
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 102),
            ("tenant-b", Some("acc-2"), Some("svc-y"), Some(20), 901),
            ("tenant-a", Some("acc-1"), Some("svc-0"), Some(110), 103),
            ("tenant-a", Some("acc-2"), Some("svc-a"), Some(50), 104),
        ]);
        let ingest_segment_101 = logs_ingest_batch(vec![
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 201),
            ("tenant-b", Some("acc-2"), Some("svc-x"), Some(30), 902),
            ("tenant-a", Some("acc-1"), Some("svc-2"), Some(90), 202),
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 203),
            ("tenant-b", Some("acc-1"), Some("svc-a"), Some(70), 903),
        ]);
        let prepared_100 = sort_logs(&ingest_segment_100, 2, None)
            .expect("prepare WAL segment 100")
            .expect("segment 100 row groups");
        let prepared_101 = sort_logs(&ingest_segment_101, 2, None)
            .expect("prepare WAL segment 101")
            .expect("segment 101 row groups");
        let wal_segments = vec![
            E2eWalSegment {
                offset: 100,
                row_groups: prepared_100.write_request.row_groups,
            },
            E2eWalSegment {
                offset: 101,
                row_groups: prepared_101.write_request.row_groups,
            },
        ];
        let queue_reader = Arc::new(E2eQueueReader {
            plan: build_segments_plan(&wal_segments),
            segments: wal_segments
                .iter()
                .map(|segment| (segment.offset, segment.row_groups.clone()))
                .collect(),
        });

        let plan_runner = PlanTaskRunnerImpl::new(
            Arc::clone(&queue_reader),
            Arc::new(E2ePlanStorage),
            Arc::new(ShiftConfig::default()),
            test_timeouts(),
            "logs",
            &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
        );
        let plan_manager = E2eManager::new();
        let cancel = CancellationToken::new();
        let plan_result = plan_runner
            .run(Uuid::new_v4(), &plan_manager, &cancel)
            .await
            .expect("plan runner should schedule shift tasks");
        assert_eq!(plan_result.status, TaskStatus::Ok);
        assert_eq!(plan_result.shift_task_ids.len(), 2);

        let table_name = format!("logs_e2e_{}", Uuid::new_v4().simple());
        let (catalog, table) = create_e2e_logs_table(&table_name).await;
        let mut shift_config = ShiftConfig::default();
        shift_config.write.row_group_size = 2;
        let storage = Arc::new(E2eParquetStorage {
            inner: IcebergStorage::new(
                Arc::clone(&catalog),
                table_name.clone(),
                &shift_config,
                writer_max_parquet_bytes(shift_config.read.upper_bound_input_mb_per_task * 1024 * 1024),
                &[],
                &[],
            ),
            written_data_files: Mutex::new(Vec::new()),
        });
        let expected_row_ids_by_tenant = HashMap::from([
            ("tenant-a".to_string(), vec![103, 101, 102, 201, 203, 202, 104]),
            ("tenant-b".to_string(), vec![903, 902, 901, 900]),
        ]);

        let shift_inputs = plan_manager
            .added_tasks
            .lock()
            .expect("added tasks lock")
            .iter()
            .filter(|task| task.code == TaskCode::new(SHIFT_TASK_CODE))
            .map(|task| {
                let input: ShiftInput = serde_json::from_slice(&task.input).expect("shift input");
                (task.id, input)
            })
            .collect::<Vec<_>>();

        for (task_id, shift_input) in shift_inputs {
            let shift_runner = ShiftTaskRunnerImpl::new(
                Arc::clone(&queue_reader),
                Arc::clone(&storage),
                table_name.clone(),
                2,
                SortColumnsDescriptor::logs().expect("logs descriptor"),
            )
            .expect("non-zero output_batch_size must be accepted")
            .with_segment_read_parallelism(2)
            .expect("valid read parallelism");
            let manager = RecordingJobManager::new();
            let task = Arc::new(TestTask {
                id: task_id,
                code: TaskCode::new(SHIFT_TASK_CODE),
                input: serde_json::to_vec(&shift_input).expect("serialize shift input"),
                output: Vec::new(),
                error: String::new(),
                depends_on: Vec::new(),
            });

            let result = shift_runner.run(task, &manager, &cancel).await.expect("shift task run");
            assert_eq!(result.status, TaskStatus::Ok);
            assert_eq!(
                result.parquet_files_total, 1,
                "one planned tenant chunk should write one parquet file"
            );

            let completed = manager.completed.lock().expect("completed task lock");
            assert_eq!(completed.len(), 1);
            let output: ShiftOutput = serde_json::from_slice(&completed[0].1).expect("shift output");
            drop(completed);
            assert_eq!(output.parquet_files.len(), 1);
        }

        let written_files = storage.written_data_files.lock().await.clone();
        assert_eq!(
            written_files.len(),
            2,
            "two tenant shift tasks must write two parquet files"
        );

        let mut seen_by_tenant = HashMap::new();
        for data_file in written_files {
            let rows = read_parquet_output_rows(table.file_io(), data_file.file_path()).await;
            let first_tenant = rows.first().expect("parquet rows").tenant_id.clone();
            assert!(
                rows.iter().all(|row| row.tenant_id == first_tenant),
                "one parquet file must contain one tenant partition"
            );
            for window in rows.windows(2) {
                assert_ne!(
                    log_sort_key_cmp(&window[0], &window[1]),
                    std::cmp::Ordering::Greater,
                    "parquet rows must be monotonic by logs sort order"
                );
            }
            let row_ids = rows.iter().map(|row| row.row_id).collect::<Vec<_>>();
            assert_eq!(
                &row_ids,
                expected_row_ids_by_tenant.get(&first_tenant).expect("expected tenant rows"),
                "parquet rows must preserve WAL-stable order for equal sort keys"
            );
            assert_parquet_row_group_bounds_match_sorted_rows(table.file_io(), data_file.file_path()).await;
            seen_by_tenant.insert(first_tenant, row_ids);
        }
        assert_eq!(seen_by_tenant.len(), expected_row_ids_by_tenant.len());
    }

    #[tokio::test]
    async fn run_end_to_end_mixed_partition_planning_and_shift_merge_preserves_boundaries_and_order() {
        let ingest_segment_100 = logs_ingest_batch(vec![
            ("tenant-b", Some("acc-2"), Some("svc-z"), Some(10), 900),
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 101),
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 102),
            ("tenant-b", Some("acc-2"), Some("svc-y"), Some(20), 901),
            ("tenant-a", Some("acc-1"), Some("svc-0"), Some(110), 103),
            ("tenant-a", Some("acc-2"), Some("svc-a"), Some(50), 104),
        ]);
        let ingest_segment_101 = logs_ingest_batch(vec![
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 201),
            ("tenant-b", Some("acc-2"), Some("svc-x"), Some(30), 902),
            ("tenant-a", Some("acc-1"), Some("svc-2"), Some(90), 202),
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 203),
            ("tenant-b", Some("acc-1"), Some("svc-a"), Some(70), 903),
        ]);
        let prepared_100 = sort_logs(&ingest_segment_100, 2, None)
            .expect("prepare WAL segment 100")
            .expect("segment 100 row groups");
        let prepared_101 = sort_logs(&ingest_segment_101, 2, None)
            .expect("prepare WAL segment 101")
            .expect("segment 101 row groups");

        let wal_segments = vec![
            E2eWalSegment {
                offset: 100,
                row_groups: prepared_100.write_request.row_groups,
            },
            E2eWalSegment {
                offset: 101,
                row_groups: prepared_101.write_request.row_groups,
            },
        ];
        let segments_map = wal_segments
            .iter()
            .map(|segment| (segment.offset, segment.row_groups.clone()))
            .collect::<HashMap<_, _>>();
        let queue_reader = Arc::new(E2eQueueReader {
            plan: build_segments_plan(&wal_segments),
            segments: segments_map,
        });

        let plan_runner = PlanTaskRunnerImpl::new(
            Arc::clone(&queue_reader),
            Arc::new(E2ePlanStorage),
            Arc::new(ShiftConfig::default()),
            test_timeouts(),
            "logs",
            &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
        );
        let manager = E2eManager::new();
        let cancel = CancellationToken::new();
        let plan_result = plan_runner
            .run(Uuid::new_v4(), &manager, &cancel)
            .await
            .expect("plan runner should schedule shift tasks");

        assert_eq!(plan_result.status, TaskStatus::Ok);
        assert_eq!(
            plan_result.shift_task_ids.len(),
            2,
            "two partition buckets must produce two shift tasks"
        );

        let added_tasks = manager.added_tasks.lock().expect("added tasks lock").clone();
        let shift_inputs = added_tasks
            .iter()
            .filter(|task| task.code == TaskCode::new(SHIFT_TASK_CODE))
            .map(|task| {
                let input: ShiftInput = serde_json::from_slice(&task.input).expect("shift input");
                (task.id, input)
            })
            .collect::<Vec<_>>();
        assert_eq!(shift_inputs.len(), 2);

        let expected_row_ids_by_partition_value = HashMap::from([
            ("tenant-a".to_string(), vec![103, 101, 102, 201, 203, 202, 104]),
            ("tenant-b".to_string(), vec![903, 902, 901, 900]),
        ]);

        for (task_id, shift_input) in shift_inputs {
            let storage = Arc::new(FakeStorage::fail_then_succeed(
                0,
                vec![test_data_file(
                    &format!("s3://warehouse/logs/{task_id}/part-0001.parquet"),
                    1,
                )],
            ));
            let shift_runner = ShiftTaskRunnerImpl::new(
                Arc::clone(&queue_reader),
                Arc::clone(&storage),
                "logs",
                2,
                SortColumnsDescriptor::logs().expect("logs descriptor"),
            )
            .expect("non-zero output_batch_size must be accepted")
            .with_segment_read_parallelism(2)
            .expect("valid read parallelism");
            let manager = RecordingJobManager::new();
            let task = Arc::new(TestTask {
                id: task_id,
                code: TaskCode::new(SHIFT_TASK_CODE),
                input: serde_json::to_vec(&shift_input).expect("serialize shift input"),
                output: Vec::new(),
                error: String::new(),
                depends_on: Vec::new(),
            });

            let result = shift_runner.run(task, &manager, &cancel).await.expect("shift task run");
            assert_eq!(result.status, TaskStatus::Ok);

            let writes = storage.writes.lock().await;
            assert_eq!(writes.len(), 1, "shift task must write one merged stream");

            let tenant_ids = tenant_ids_from_batches(&writes[0]);
            let first_tenant = tenant_ids.first().expect("written tenant ids").clone();
            assert!(
                tenant_ids.iter().all(|tenant_id| tenant_id == &first_tenant),
                "partition bucket boundary violated: expected one tenant per output, got {tenant_ids:?}",
            );

            let actual_row_ids = row_ids_from_batches(&writes[0]);
            drop(writes);
            let expected_row_ids = expected_row_ids_by_partition_value
                .get(&first_tenant)
                .expect("expected rows by partition value");
            assert_eq!(
                &actual_row_ids, expected_row_ids,
                "merged order must match sort order and WAL-stable tie-breakers"
            );
        }
    }

    /// Behavioural pin of the writer failover budget.
    ///
    /// `IcebergStorage` uses a writer rollover budget of
    /// `upper_bound_bytes × WRITER_FILE_SIZE_FAILOVER_FACTOR`
    /// (currently ×2). In normal operation the planner shapes one chunk per
    /// shift task and the writer never rolls over; rollover is the failover
    /// path. This test forces the failover to trigger by giving storage a
    /// pathologically small budget (2 bytes), while the planner runs with the
    /// default config so a single shift task covers the whole tenant.
    /// coded parquet output (footer + statistics + data) overshoots the tiny
    /// budget and must split into multiple data files.
    ///
    /// Removing the failover multiplier or short-circuiting `target_file_size`
    /// in [`IcebergStorage::write_parquet_files_once`] causes this test to
    /// fail. The paired "no rollover under default config" invariant is
    /// already pinned by `run_end_to_end_writes_rows_sorted_in_actual_parquet_files`
    /// (asserts `parquet_files_total == 1`).
    #[tokio::test]
    async fn shift_task_rolls_over_data_files_when_writer_budget_is_pathologically_small() {
        let ingest_segment = logs_ingest_batch(vec![
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 101),
            ("tenant-a", Some("acc-1"), Some("svc-1"), Some(100), 102),
            ("tenant-a", Some("acc-1"), Some("svc-0"), Some(110), 103),
            ("tenant-a", Some("acc-1"), Some("svc-2"), Some(120), 104),
            ("tenant-a", Some("acc-1"), Some("svc-3"), Some(130), 105),
            ("tenant-a", Some("acc-1"), Some("svc-4"), Some(140), 106),
        ]);
        let prepared = sort_logs(&ingest_segment, 2, None)
            .expect("prepare WAL segment")
            .expect("segment row groups");
        let wal_segments = vec![E2eWalSegment {
            offset: 100,
            row_groups: prepared.write_request.row_groups,
        }];
        let queue_reader = Arc::new(E2eQueueReader {
            plan: build_segments_plan(&wal_segments),
            segments: wal_segments
                .iter()
                .map(|segment| (segment.offset, segment.row_groups.clone()))
                .collect(),
        });

        // Plan with default config: produces a single shift task for the tenant.
        let plan_runner = PlanTaskRunnerImpl::new(
            Arc::clone(&queue_reader),
            Arc::new(E2ePlanStorage),
            Arc::new(ShiftConfig::default()),
            test_timeouts(),
            "logs",
            &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
        );
        let plan_manager = E2eManager::new();
        let cancel = CancellationToken::new();
        let plan_result = plan_runner
            .run(Uuid::new_v4(), &plan_manager, &cancel)
            .await
            .expect("plan runner");
        assert_eq!(plan_result.status, TaskStatus::Ok);
        assert_eq!(
            plan_result.shift_task_ids.len(),
            1,
            "single tenant => single shift task"
        );

        // Storage with pathological budget = 2 bytes (writer_max_parquet_bytes(1)).
        // Encoded parquet for 6 rows is hundreds of bytes (footer alone), so
        // every row group flushed by `RollingFileWriterBuilder` overshoots the
        // budget and starts a new file.
        let table_name = format!("logs_failover_{}", Uuid::new_v4().simple());
        let (catalog, _table) = create_e2e_logs_table(&table_name).await;
        let mut storage_config = ShiftConfig::default();
        storage_config.write.row_group_size = 2;
        let storage = Arc::new(E2eParquetStorage {
            inner: IcebergStorage::new(
                Arc::clone(&catalog),
                table_name.clone(),
                &storage_config,
                writer_max_parquet_bytes(1), // 2 bytes: forces rollover on every row group
                &[],
                &[],
            ),
            written_data_files: Mutex::new(Vec::new()),
        });

        let shift_inputs = plan_manager
            .added_tasks
            .lock()
            .expect("added tasks lock")
            .iter()
            .filter(|task| task.code == TaskCode::new(SHIFT_TASK_CODE))
            .map(|task| {
                let input: ShiftInput = serde_json::from_slice(&task.input).expect("shift input");
                (task.id, input)
            })
            .collect::<Vec<_>>();
        assert_eq!(shift_inputs.len(), 1);

        let (task_id, shift_input) = shift_inputs.into_iter().next().expect("shift task");
        let shift_runner = ShiftTaskRunnerImpl::new(
            Arc::clone(&queue_reader),
            Arc::clone(&storage),
            table_name.clone(),
            2,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size")
        .with_segment_read_parallelism(2)
        .expect("valid read parallelism");
        let manager = RecordingJobManager::new();
        let task = Arc::new(TestTask {
            id: task_id,
            code: TaskCode::new(SHIFT_TASK_CODE),
            input: serde_json::to_vec(&shift_input).expect("serialize shift input"),
            output: Vec::new(),
            error: String::new(),
            depends_on: Vec::new(),
        });

        let result = shift_runner.run(task, &manager, &cancel).await.expect("shift task run");
        assert_eq!(result.status, TaskStatus::Ok);
        assert!(
            result.parquet_files_total > 1,
            "tiny writer budget must trigger rollover into multiple parquet files; got {}",
            result.parquet_files_total
        );
    }
}
