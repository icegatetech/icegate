use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use icegate_common::retrier::{Retrier, RetrierConfig};
use icegate_queue::{QueueReader, Topic};
use jobmanager::{Error, TaskContext};
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::{
    SegmentToRead, ShiftInput, ShiftOutput,
    executor::{TaskStatus, parse_task_input},
    iceberg_storage::Storage,
    row_groups_merger::{
        NoopRowGroupsMergerObserver, RowGroupsMerger, RowGroupsMergerObserver, SortedBatchMergerConfig, WalMergeSource,
        wal_inputs_from_segments,
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
    /// Serialized [`ShiftOutput`] the task completes with; empty when it wrote no parquet files.
    pub output: Vec<u8>,
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
    async fn run(&self, ctx: &TaskContext) -> Result<ShiftTaskResult, ShiftTaskFailure>;
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
    #[tracing::instrument(name="shift_run", skip(self, ctx), fields(task_id = %ctx.id()))]
    async fn run(&self, ctx: &TaskContext) -> Result<ShiftTaskResult, ShiftTaskFailure> {
        let cancel_token = ctx.cancel_token();
        if cancel_token.is_cancelled() {
            return Ok(ShiftTaskResult {
                status: TaskStatus::Cancelled,
                output: Vec::new(),
                record_batches_total: 0,
                rows_total: 0,
                parquet_files_total: 0,
                bytes_written_total: 0,
            });
        }

        let input: ShiftInput = parse_task_input(ctx.input())
            .map_err(|err| ShiftTaskFailure::new(ShiftTaskFailureReason::Serialization, err))?;
        self.shift_segments(input, cancel_token).await
    }
}

impl<Q, S> ShiftTaskRunnerImpl<Q, S>
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
    /// Merge the planned WAL row groups into Iceberg parquet files and describe what was written.
    ///
    /// The task payload is already decoded here, so this is the whole of a shift task minus the
    /// jobmanager plumbing around it.
    async fn shift_segments(
        &self,
        input: ShiftInput,
        cancel_token: &CancellationToken,
    ) -> Result<ShiftTaskResult, ShiftTaskFailure> {
        if input.segments.is_empty() {
            error!("shift: no segments provided, skipping");
            return Ok(ShiftTaskResult {
                status: TaskStatus::Empty,
                output: Vec::new(),
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
                Error::Other("shift produced no rows to write".to_string()),
            ));
        }

        if write_result.data_files.is_empty() {
            return Err(ShiftTaskFailure::new(
                ShiftTaskFailureReason::NoParquet,
                Error::Other("shift produced no parquet files to commit".to_string()),
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
                Error::Other(format!("failed to serialize shift output: {err}")),
            )
        })?;

        Ok(ShiftTaskResult {
            status: TaskStatus::Ok,
            output: output_payload,
            record_batches_total,
            rows_total: write_result.rows_written,
            parquet_files_total: write_result.data_files.len(),
            bytes_written_total,
        })
    }

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
        let source = Arc::new(WalMergeSource::new(
            Arc::clone(&self.queue_reader) as Arc<dyn QueueReader>,
            self.topic.clone(),
        ));
        let mut merger = RowGroupsMerger::new(
            source,
            wal_inputs_from_segments(segments),
            SortedBatchMergerConfig {
                row_group_size: self.output_batch_size,
                read_parallelism: self.segment_read_parallelism,
                cancel_token: cancel_token.clone(),
                sort_descriptor: self.sort_descriptor,
            },
        )
        .map_err(|err| ShiftWriteError::queue_read(bridge_merge_error(err, cancel_token)))?
        .with_observer(Arc::clone(&self.row_groups_merger_observer));
        merger
            .prefetch_first_group()
            .await
            .map_err(|err| ShiftWriteError::queue_read(bridge_merge_error(err, cancel_token)))?;

        // The shared merger yields `icegate_common::Error`; bridge it back to
        // `IngestError` for the storage write pipeline, re-detecting
        // cancellation via the shared token so a cancelled merge still surfaces
        // as `IngestError::Cancelled` (the merger has no `Cancelled` variant of
        // its own).
        let write_cancel_token = cancel_token.clone();
        let merged_stream = merger
            .into_stream()
            .map_err(move |err| bridge_merge_error(err, &write_cancel_token))
            .boxed();
        self.storage
            .write_record_batches(merged_stream, cancel_token)
            .await
            .map_err(ShiftWriteError::from)
    }
}

/// Bridge a merger `icegate_common::Error` back into `IngestError`.
///
/// The merger collapses every failure (including cancellation and WAL reads)
/// into `icegate_common::Error::Write`. To preserve the Shifter's typed failure
/// accounting, this reclassifies that flat error:
///
/// 1. If the shared cancel token fired, report `IngestError::Cancelled` (the
///    merger has no `Cancelled` variant of its own).
/// 2. Else, if the failure was stamped by [`WalMergeSource`], report
///    `IngestError::ShiftQueueRead` so the `QueueRead` failure reason survives.
/// 3. Otherwise fall back to the standard `From` mapping (`Write` -> `Shift`).
fn bridge_merge_error(err: icegate_common::Error, cancel_token: &CancellationToken) -> crate::error::IngestError {
    if cancel_token.is_cancelled() {
        return crate::error::IngestError::Cancelled;
    }
    if super::row_groups_merger::is_wal_source_error(&err) {
        return crate::error::IngestError::ShiftQueueRead(err.to_string());
    }
    err.into()
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
            error: Error::Other(err.to_string()),
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
            error: Error::Other("shift task cancelled during write retry".to_string()),
            source: Some(crate::error::IngestError::Cancelled),
        }
    }

    fn max_attempts() -> Self {
        Self {
            reason: ShiftTaskFailureReason::Write,
            error: Error::Other("max retry attempts reached".to_string()),
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
            error: Error::Other(err.to_string()),
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
        array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
    };
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use iceberg::spec::DataFile;
    use parquet::{
        arrow::arrow_writer::ArrowWriter,
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

    use super::{ShiftTaskFailureReason, ShiftTaskRunnerImpl, ShiftWriteError};
    use crate::{
        error::{IngestError, Result},
        shift::{
            PlannedRowGroup, SegmentToRead, ShiftInput, ShiftOutput,
            executor::TaskStatus,
            iceberg_storage::{Storage, WrittenDataFiles},
            test_utils::data_file as test_data_file,
        },
        wal::{SortColumnsDescriptor, logs_row_group_boundary_range_from_batch},
    };

    struct FakeQueueReader {
        batches_by_offset: HashMap<u64, Vec<arrow::record_batch::RecordBatch>>,
        delay_by_offset: HashMap<u64, Duration>,
        fail_offset: Option<u64>,
        started_reads: Option<Arc<AtomicUsize>>,
        active_reads: Option<Arc<AtomicUsize>>,
        max_active_reads: Option<Arc<AtomicUsize>>,
        concurrency_gate: Option<Arc<ReadConcurrencyGate>>,
        release_gate: Option<Arc<ReadReleaseGate>>,
    }

    /// Holds every read until enough of them are in flight at once, so a test asserting that reads
    /// overlap proves it by construction: reads that never overlap time out here instead of
    /// depending on how long a sleep happened to take.
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

        /// Register one read at the gate and wait there until the gate opens.
        async fn enter_read(&self) -> icegate_queue::Result<()> {
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

        /// Wait for the moment the required reads are in flight, without counting as one of them.
        async fn wait_until_open(&self) -> icegate_queue::Result<()> {
            loop {
                let notified = self.notify.notified();
                if self.is_open.load(Ordering::SeqCst) {
                    return Ok(());
                }
                timeout(self.wait_timeout, notified).await.map_err(|_| {
                    icegate_queue::QueueError::Metadata(
                        "read concurrency gate timed out: segment reads did not overlap".to_string(),
                    )
                })?;
            }
        }
    }

    /// Holds the read of one segment until the other reads have finished, which is what makes
    /// "the reads completed out of order" a property of the fixture rather than of their timing.
    struct ReadReleaseGate {
        held_offset: u64,
        reads_before_release: usize,
        finished_reads: AtomicUsize,
        notify: Notify,
        wait_timeout: Duration,
    }

    impl ReadReleaseGate {
        fn new(held_offset: u64, reads_before_release: usize, wait_timeout: Duration) -> Self {
            Self {
                held_offset,
                reads_before_release,
                finished_reads: AtomicUsize::new(0),
                notify: Notify::new(),
                wait_timeout,
            }
        }

        /// Wait until the segment at `offset` is allowed to finish its read.
        async fn wait_for_turn(&self, offset: u64) -> icegate_queue::Result<()> {
            if offset != self.held_offset {
                return Ok(());
            }
            loop {
                let notified = self.notify.notified();
                if self.finished_reads.load(Ordering::SeqCst) >= self.reads_before_release {
                    return Ok(());
                }
                timeout(self.wait_timeout, notified).await.map_err(|_| {
                    icegate_queue::QueueError::Metadata(
                        "read release gate timed out: the later segments never finished first".to_string(),
                    )
                })?;
            }
        }

        /// Report that the read of `offset` finished.
        fn report_finished(&self, offset: u64) {
            if offset == self.held_offset {
                return;
            }
            self.finished_reads.fetch_add(1, Ordering::SeqCst);
            self.notify.notify_waiters();
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
                gate.enter_read().await?;
            }
            if let Some(gate) = &self.release_gate {
                gate.wait_for_turn(offset).await?;
            }
            if let Some(delay) = self.delay_by_offset.get(&offset) {
                sleep(*delay).await;
            }
            if self.fail_offset == Some(offset) {
                return Err(icegate_queue::QueueError::Metadata(format!(
                    "read failed for segment {offset}"
                )));
            }
            let batches = futures::stream::iter(
                self.batches_by_offset
                    .get(&offset)
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                    .filter_map(move |(idx, batch)| record_batch_idxs.contains(&idx).then_some(batch))
                    .map(Ok),
            );
            if let Some(gate) = &self.release_gate {
                gate.report_finished(offset);
            }
            Ok(Box::pin(batches))
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
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("value", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("svc")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![value])) as ArrayRef,
            ],
        )
        .expect("batch")
    }

    #[allow(clippy::needless_pass_by_value)]
    fn logs_batch_for_shift(rows: Vec<(Option<&str>, Option<i64>, i64)>) -> arrow::record_batch::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("value", DataType::Int64, false),
        ]));
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(service_name, _, _)| *service_name).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, timestamp, _)| *timestamp).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
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
        logs_batch_for_shift(vec![(Some(service_name), Some(timestamp_micros), value)])
    }

    fn values_from_batches(batches: &[arrow::record_batch::RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("int64 array");
                (0..values.len()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect()
    }

    fn parquet_bytes_from_batches(batches: &[arrow::record_batch::RecordBatch]) -> Vec<u8> {
        let mut buffer = Vec::new();
        let props = WriterProperties::builder().set_max_row_group_row_count(Some(2)).build();
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
                    .first()
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

    /// Equal sort keys are broken by WAL position, so the merged order must follow the segments,
    /// not the order their reads came back in. The release gate holds the first segment until both
    /// later ones have finished, which is what makes the reads complete out of order here.
    #[tokio::test]
    async fn shift_segments_preserves_segment_order_when_reads_complete_out_of_order() {
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
            release_gate: Some(Arc::new(ReadReleaseGate::new(1, 2, Duration::from_secs(2)))),
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
        let cancel = CancellationToken::new();

        let Err(err) = runner.shift_segments(input, &cancel).await else {
            panic!("storage write is expected to fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::Write);

        let writes = storage.writes.lock().await;
        assert_eq!(writes.len(), 1);
        assert_eq!(values_from_batches(&writes[0]), vec![1, 2, 3]);
        drop(writes);
    }

    #[tokio::test]
    async fn shift_segments_merges_row_groups_within_a_single_segment_globally() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(
                1,
                vec![
                    logs_batch_for_shift(vec![(Some("svc-3"), Some(30), 1), (Some("svc-4"), Some(20), 2)]),
                    logs_batch_for_shift(vec![(Some("svc-2"), Some(40), 3), (Some("svc-5"), Some(10), 4)]),
                ],
            )]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
            release_gate: None,
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
            logs_batch_for_shift(vec![(Some("svc-3"), Some(30), 1), (Some("svc-4"), Some(20), 2)]),
            logs_batch_for_shift(vec![(Some("svc-2"), Some(40), 3), (Some("svc-5"), Some(10), 4)]),
        ];
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&segment_1, &[0, 1]),
            }],
            trace_context: None,
        };
        let cancel = CancellationToken::new();

        let Err(err) = runner.shift_segments(input, &cancel).await else {
            panic!("storage write is expected to fail");
        };
        assert_eq!(err.reason(), ShiftTaskFailureReason::Write);

        let writes = storage.writes.lock().await;
        assert_eq!(writes.len(), 1);
        assert_eq!(values_from_batches(&writes[0]), vec![3, 1, 2, 4]);
        drop(writes);
    }

    #[tokio::test]
    async fn shift_segments_produces_non_overlapping_service_name_bounds_when_reencoded() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(
                1,
                vec![
                    logs_batch_for_shift(vec![(Some("svc-3"), Some(30), 1), (Some("svc-4"), Some(20), 2)]),
                    logs_batch_for_shift(vec![(Some("svc-2"), Some(40), 3), (Some("svc-5"), Some(10), 4)]),
                ],
            )]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
            release_gate: None,
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
            logs_batch_for_shift(vec![(Some("svc-3"), Some(30), 1), (Some("svc-4"), Some(20), 2)]),
            logs_batch_for_shift(vec![(Some("svc-2"), Some(40), 3), (Some("svc-5"), Some(10), 4)]),
        ];
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&segment_1, &[0, 1]),
            }],
            trace_context: None,
        };
        let cancel = CancellationToken::new();

        let Err(err) = runner.shift_segments(input, &cancel).await else {
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
    async fn shift_segments_retries_a_transient_write_failure_with_reopened_wal_streams() {
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
            release_gate: None,
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
        let cancel = CancellationToken::new();

        let result = runner.shift_segments(input, &cancel).await.expect("write retry must succeed");

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

        let output: ShiftOutput = serde_json::from_slice(&result.output).expect("shift output");
        assert_eq!(
            output.parquet_files,
            vec!["s3://warehouse/logs/part-00001.parquet".to_string()]
        );
    }

    /// A planned segment that reads back empty leaves the merge with nothing to write. Completing
    /// the task would hand the commit task a snapshot claiming a WAL offset whose rows never
    /// reached the table, so the shift task fails closed instead. The guard sits behind the write,
    /// so the write attempt itself is expected.
    #[tokio::test]
    async fn shift_segments_fails_when_the_merge_produced_no_rows() {
        let planned_segment = vec![test_batch(1)];
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::new(),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
            release_gate: None,
        });
        let storage = Arc::new(FakeStorage::fail_then_succeed(0, Vec::new()));
        let runner = ShiftTaskRunnerImpl::new(
            Arc::clone(&queue_reader),
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted");
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&planned_segment, &[0]),
            }],
            trace_context: None,
        };

        let Err(err) = runner.shift_segments(input, &CancellationToken::new()).await else {
            panic!("a merge that produced no row must fail the shift task");
        };

        assert_eq!(err.reason(), ShiftTaskFailureReason::EmptyBatches);
        assert_eq!(storage.write_calls.load(Ordering::SeqCst), 1);
    }

    /// Rows were merged and written, yet the writer reported no parquet file. Without this guard
    /// the commit task would receive an empty file list and still create a snapshot carrying the
    /// planned WAL offset, hiding every one of those rows from the table.
    #[tokio::test]
    async fn shift_segments_fails_when_the_writer_produced_no_parquet_file() {
        let planned_segment = vec![test_batch(1)];
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(1, planned_segment.clone())]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
            release_gate: None,
        });
        let storage = Arc::new(FakeStorage::fail_then_succeed(0, Vec::new()));
        let runner = ShiftTaskRunnerImpl::new(
            Arc::clone(&queue_reader),
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted");
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&planned_segment, &[0]),
            }],
            trace_context: None,
        };

        let Err(err) = runner.shift_segments(input, &CancellationToken::new()).await else {
            panic!("a write that produced no parquet file must fail the shift task");
        };

        assert_eq!(err.reason(), ShiftTaskFailureReason::NoParquet);
        assert_eq!(
            storage.writes.lock().await.first().map(Vec::len),
            Some(1),
            "the fixture must reach the guard with rows actually written"
        );
    }

    /// The other half of the retry rule: a write failure the error type classifies as terminal is
    /// reported after a single attempt. Retrying it would reopen every WAL stream and repeat the
    /// whole merge for a failure that cannot succeed.
    #[tokio::test]
    async fn shift_segments_does_not_retry_a_terminal_write_failure() {
        let planned_segment = vec![test_batch(1)];
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(1, planned_segment.clone())]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
            release_gate: None,
        });
        let storage = Arc::new(FakeStorage::always_fail());
        let runner = ShiftTaskRunnerImpl::new(
            Arc::clone(&queue_reader),
            Arc::clone(&storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted");
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&planned_segment, &[0]),
            }],
            trace_context: None,
        };

        let Err(err) = runner.shift_segments(input, &CancellationToken::new()).await else {
            panic!("a terminal write failure must fail the shift task");
        };

        assert_eq!(err.reason(), ShiftTaskFailureReason::Write);
        assert_eq!(
            storage.write_calls.load(Ordering::SeqCst),
            1,
            "a terminal write failure must not be retried"
        );
    }

    #[tokio::test]
    async fn shift_segments_fails_fast_on_a_queue_read_error_and_skips_the_storage_write() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(1, vec![test_batch(1)]), (3, vec![test_batch(3)])]),
            delay_by_offset: HashMap::from([(1, Duration::from_millis(40)), (2, Duration::from_millis(5))]),
            fail_offset: Some(2),
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
            release_gate: None,
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
        let cancel = CancellationToken::new();

        let Err(err) = runner.shift_segments(input, &cancel).await else {
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
    async fn shift_segments_fails_on_a_late_queue_read_and_preserves_the_typed_reason() {
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
        let cancel = CancellationToken::new();

        let Err(err) = runner.shift_segments(input, &cancel).await else {
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

    /// Above one, the configured parallelism must actually overlap reads: the gate only opens once
    /// two of them are inside the reader at the same time, so a sequential read path fails here
    /// with the gate's timeout instead of passing unnoticed.
    #[tokio::test]
    async fn shift_segments_overlaps_reads_for_parallelism_greater_than_one() {
        let active_reads = Arc::new(AtomicUsize::new(0));
        let max_active_reads = Arc::new(AtomicUsize::new(0));
        let segments = [
            vec![logs_batch_for_shift(vec![
                (Some("svc"), Some(100), 1),
                (Some("svc"), Some(70), 2),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("svc"), Some(95), 3),
                (Some("svc"), Some(65), 4),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("svc"), Some(90), 5),
                (Some("svc"), Some(60), 6),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("svc"), Some(85), 7),
                (Some("svc"), Some(55), 8),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("svc"), Some(80), 9),
                (Some("svc"), Some(50), 10),
            ])],
            vec![logs_batch_for_shift(vec![
                (Some("svc"), Some(75), 11),
                (Some("svc"), Some(45), 12),
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
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: Some(Arc::clone(&active_reads)),
            max_active_reads: Some(Arc::clone(&max_active_reads)),
            concurrency_gate: Some(Arc::new(ReadConcurrencyGate::new(2, Duration::from_secs(2)))),
            release_gate: None,
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
        let cancel = CancellationToken::new();

        let Err(err) = runner.shift_segments(input, &cancel).await else {
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
    async fn shift_segments_keeps_reads_strictly_sequential_for_parallelism_one() {
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
            release_gate: None,
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
        let cancel = CancellationToken::new();

        let Err(err) = runner.shift_segments(input, &cancel).await else {
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
            release_gate: None,
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
            release_gate: None,
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

    /// A shutdown that lands while the reads are in flight must stop the task with the cancelled
    /// reason and leave nothing half-written. The cancellation is triggered by the gate opening,
    /// which is the moment the reads are provably in flight.
    #[tokio::test]
    async fn shift_segments_stops_reading_and_fails_with_cancelled_after_cancellation() {
        let started_reads = Arc::new(AtomicUsize::new(0));
        let active_reads = Arc::new(AtomicUsize::new(0));
        let read_gate = Arc::new(ReadConcurrencyGate::new(2, Duration::from_secs(2)));
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
            concurrency_gate: Some(Arc::clone(&read_gate)),
            release_gate: None,
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
        let cancel = CancellationToken::new();
        let cancel_for_task = cancel.clone();

        let run_handle = tokio::spawn(async move { runner.shift_segments(input, &cancel_for_task).await });
        read_gate
            .wait_until_open()
            .await
            .expect("the reads must be in flight before the cancellation");
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
}
