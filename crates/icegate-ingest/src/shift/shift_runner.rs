use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use icegate_common::{
    merge::SchemaTransitionMergeSource,
    retrier::{Retrier, RetrierConfig},
    schema_transition::SchemaTransitionError,
};
use icegate_queue::{QueueReader, Topic};
use jobmanager::{Error, TaskContext};
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::{
    SegmentToRead, ShiftInput, ShiftOutput,
    executor::{TaskStatus, parse_task_input},
    iceberg_storage::{Storage, WriteTarget},
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
            .map_err(ShiftTaskFailure::from)?;

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
        let target = self
            .storage
            .load_write_target(cancel_token)
            .await
            .map_err(ShiftWriteError::from)?;
        let wal_source = Arc::new(WalMergeSource::new(
            Arc::clone(&self.queue_reader) as Arc<dyn QueueReader>,
            self.topic.clone(),
        ));
        let source = Arc::new(SchemaTransitionMergeSource::new(
            wal_source,
            Arc::clone(target.arrow_schema()),
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
        if let Err(err) = merger.prefetch_first_group().await {
            return Err(match bridge_merge_error(err, cancel_token) {
                crate::error::IngestError::SchemaTransition(error) => {
                    self.transition_write_error(error, cancel_token).await
                }
                other => ShiftWriteError::queue_read(other),
            });
        }

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
        match self.storage.write_record_batches(target, merged_stream, cancel_token).await {
            Ok(written) => Ok(written),
            Err(crate::error::IngestError::SchemaTransition(error)) => {
                Err(self.transition_write_error(error, cancel_token).await)
            }
            Err(other) => Err(ShiftWriteError::from(other)),
        }
    }

    /// Turn a schema-transition failure into a write failure.
    ///
    /// A batch carrying a column the table lacks means the cached table may
    /// predate a migration, so the table is read from the catalog again. When
    /// that table has the column, the retry of
    /// [`Self::write_row_groups_with_retry`] writes into it; when it does not,
    /// the failure is final, because the verdict rests on the catalog rather than
    /// on a table another read left in the cache.
    async fn transition_write_error(
        &self,
        error: SchemaTransitionError,
        cancel_token: &CancellationToken,
    ) -> ShiftWriteError {
        let SchemaTransitionError::ColumnNotInTable { column } = &error else {
            return ShiftWriteError::from(crate::error::IngestError::SchemaTransition(error));
        };
        let has_column = match self.storage.reload_write_target(cancel_token).await {
            Ok(reloaded) => reloaded.arrow_schema().field_with_name(column).is_ok(),
            Err(err) => return ShiftWriteError::from(err),
        };
        let write_error = ShiftWriteError::from(crate::error::IngestError::SchemaTransition(error));
        if has_column {
            write_error
        } else {
            write_error.into_final()
        }
    }
}

/// Bridge a merger `icegate_common::Error` back into `IngestError`.
///
/// The merger collapses cancellation and WAL reads into
/// `icegate_common::Error::Write`, while a schema-transition failure passes
/// through it as `icegate_common::Error::SchemaTransition`. To preserve the
/// Shifter's typed failure accounting, this reclassifies the error:
///
/// 1. If the shared cancel token fired, report `IngestError::Cancelled` (the
///    merger has no `Cancelled` variant of its own).
/// 2. Else, if the failure was stamped by [`WalMergeSource`], report
///    `IngestError::ShiftQueueRead` so the `QueueRead` failure reason survives.
/// 3. Otherwise fall back to the standard `From` mapping (`Write` -> `Shift`,
///    `SchemaTransition` -> `SchemaTransition`).
fn bridge_merge_error(err: icegate_common::Error, cancel_token: &CancellationToken) -> crate::error::IngestError {
    if cancel_token.is_cancelled() {
        return crate::error::IngestError::Cancelled;
    }
    if super::row_groups_merger::is_wal_source_error(&err) {
        return crate::error::IngestError::ShiftQueueRead(err.to_string());
    }
    err.into()
}

/// A write-path failure: the typed reason plus the error that produced it.
///
/// The reason is not derivable from the error alone — the same
/// `IngestError` reaches this type both from a WAL read and from the storage
/// write, and only the call site knows which. The error text is rendered on
/// demand rather than stored, so the two can never disagree.
struct ShiftWriteError {
    reason: ShiftTaskFailureReason,
    source: crate::error::IngestError,
    /// Set when the write path has established that a retry would meet the same
    /// failure, overriding a `source` that is retryable in general.
    is_final: bool,
}

impl ShiftWriteError {
    fn queue_read(err: crate::error::IngestError) -> Self {
        if matches!(err, crate::error::IngestError::Cancelled) {
            return <Self as icegate_common::RetryError>::cancelled();
        }
        Self {
            reason: ShiftTaskFailureReason::QueueRead,
            source: err,
            is_final: false,
        }
    }

    /// Mark the failure as one no retry of this task can resolve.
    const fn into_final(mut self) -> Self {
        self.is_final = true;
        self
    }

    fn is_retryable(&self) -> bool {
        !self.is_final && self.source.is_retryable()
    }
}

impl icegate_common::RetryError for ShiftWriteError {
    fn cancelled() -> Self {
        Self {
            reason: ShiftTaskFailureReason::Cancelled,
            source: crate::error::IngestError::Cancelled,
            is_final: false,
        }
    }

    fn max_attempts() -> Self {
        Self {
            reason: ShiftTaskFailureReason::Write,
            source: crate::error::IngestError::MaxAttemptsReached,
            is_final: false,
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
            source: err,
            is_final: false,
        }
    }
}

impl From<ShiftWriteError> for ShiftTaskFailure {
    fn from(err: ShiftWriteError) -> Self {
        Self::new(err.reason, Error::Other(err.to_string()))
    }
}

impl std::fmt::Display for ShiftWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.reason {
            // `IngestError::Cancelled` renders as a bare "operation cancelled",
            // which names no subject at all. Name the shift task and stop there:
            // three stages reach this arm — the WAL prefetch through
            // [`ShiftWriteError::queue_read`], the storage write through `From`,
            // and the retry loop through [`RetryError::cancelled`] — and `reason`
            // collapses to `Cancelled` for all of them, so any stage this text
            // named would be wrong for the other two.
            ShiftTaskFailureReason::Cancelled => f.write_str("shift task cancelled"),
            _ => self.source.fmt(f),
        }
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
    use futures::TryStreamExt;
    use iceberg::{
        spec::{DataFile, PrimitiveType, Type},
        transaction::{AddColumn, ApplyTransactionAction, Transaction},
    };
    use icegate_common::{
        OPERATIONS_TABLE, OPERATIONS_TOPIC,
        parquet_encoding::{OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS},
        resolve_committed_offset,
        schema::{COL_EVALUATIONS, operations_partition_spec, operations_schema, operations_sort_order},
    };
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
        infra::metrics::ShiftMetrics,
        shift::{
            IcebergStorage, PlannedRowGroup, SegmentToRead, ShiftInput, ShiftOutput,
            executor::TaskStatus,
            iceberg_storage::{Storage, WriteTarget, WrittenDataFiles},
            instrumentation::StorageWithMetrics,
            pipeline_it_tests::{
                build_catalog, create_table, decode_evaluations, evaluated_traces_request, expected_stored_evaluations,
                shift_config, start_object_storage, write_wal_segments,
            },
            test_utils::data_file as test_data_file,
        },
        transform::{operations_to_record_batch, test_support::test_tenant},
        wal::{
            SortColumnsDescriptor, deserialize_row_group_boundary_range, logs_row_group_boundary_range_from_batch,
            sort_operations,
        },
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

    /// A catalog failure as `IcebergStorage` reports it.
    fn catalog_error(is_retryable: bool) -> IngestError {
        IngestError::Iceberg(
            iceberg::Error::new(iceberg::ErrorKind::Unexpected, "catalog failure").with_retryable(is_retryable),
        )
    }

    struct FakeStorage {
        writes: Mutex<Vec<Vec<arrow::record_batch::RecordBatch>>>,
        write_calls: AtomicUsize,
        fail_attempts_remaining: AtomicUsize,
        fail_retryable: bool,
        returned_data_files: Vec<DataFile>,
        /// Table schema served until the first completed reload, standing for a table cached before
        /// a migration; `None` serves the schema of [`test_batch`] from the start.
        stale_table_schema: Option<arrow::datatypes::SchemaRef>,
        /// Whether `stale_table_schema` is still served after a reload, standing for a catalog whose
        /// table never gained the column.
        is_stale_after_reload: bool,
        /// Table schema served after a reload, standing for a table a migration evolved; `None`
        /// serves the schema of [`test_batch`].
        evolved_table_schema: Option<arrow::datatypes::SchemaRef>,
        /// How many reloads fail with a transient catalog error.
        transient_reload_failures: AtomicUsize,
        /// Whether every load fails with a terminal catalog error.
        is_load_refused: bool,
        load_calls: AtomicUsize,
        reload_calls: AtomicUsize,
        /// Reloads that returned a table; a failed reload leaves the cached table in place.
        completed_reloads: AtomicUsize,
    }

    impl FakeStorage {
        fn always_fail() -> Self {
            Self {
                writes: Mutex::new(Vec::new()),
                write_calls: AtomicUsize::new(0),
                fail_attempts_remaining: AtomicUsize::new(usize::MAX),
                fail_retryable: false,
                returned_data_files: Vec::new(),
                stale_table_schema: None,
                is_stale_after_reload: false,
                evolved_table_schema: None,
                transient_reload_failures: AtomicUsize::new(0),
                is_load_refused: false,
                load_calls: AtomicUsize::new(0),
                reload_calls: AtomicUsize::new(0),
                completed_reloads: AtomicUsize::new(0),
            }
        }

        fn fail_then_succeed(fail_attempts: usize, returned_data_files: Vec<DataFile>) -> Self {
            Self {
                writes: Mutex::new(Vec::new()),
                write_calls: AtomicUsize::new(0),
                fail_attempts_remaining: AtomicUsize::new(fail_attempts),
                fail_retryable: true,
                returned_data_files,
                stale_table_schema: None,
                is_stale_after_reload: false,
                evolved_table_schema: None,
                transient_reload_failures: AtomicUsize::new(0),
                is_load_refused: false,
                load_calls: AtomicUsize::new(0),
                reload_calls: AtomicUsize::new(0),
                completed_reloads: AtomicUsize::new(0),
            }
        }

        /// Storage whose cached table predates `stale_table_schema`'s missing columns until it is
        /// reloaded; every write succeeds.
        fn with_stale_table_schema(
            stale_table_schema: arrow::datatypes::SchemaRef,
            returned_data_files: Vec<DataFile>,
        ) -> Self {
            Self {
                stale_table_schema: Some(stale_table_schema),
                ..Self::fail_then_succeed(0, returned_data_files)
            }
        }

        /// Storage serving `table_schema` before and after a reload, standing for a catalog a reload
        /// cannot change.
        fn with_table_schema_unchanged_by_reload(table_schema: arrow::datatypes::SchemaRef) -> Self {
            Self {
                is_stale_after_reload: true,
                ..Self::with_stale_table_schema(table_schema, Vec::new())
            }
        }

        /// The table schema a load serves at this point of the test.
        fn current_table_schema(&self) -> arrow::datatypes::SchemaRef {
            let is_reloaded = self.completed_reloads.load(Ordering::SeqCst) > 0;
            match &self.stale_table_schema {
                Some(stale) if self.is_stale_after_reload || !is_reloaded => Arc::clone(stale),
                _ => self.evolved_table_schema.clone().unwrap_or_else(|| test_batch(0).schema()),
            }
        }
    }

    #[async_trait]
    impl Storage for FakeStorage {
        type Target = arrow::datatypes::SchemaRef;

        async fn get_last_offset(&self, _cancel_token: &CancellationToken) -> Result<Option<u64>> {
            panic!("get_last_offset is not expected in shift runner tests");
        }

        async fn load_write_target(&self, _cancel_token: &CancellationToken) -> Result<arrow::datatypes::SchemaRef> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            if self.is_load_refused {
                return Err(catalog_error(false));
            }
            Ok(self.current_table_schema())
        }

        async fn reload_write_target(&self, _cancel_token: &CancellationToken) -> Result<arrow::datatypes::SchemaRef> {
            self.reload_calls.fetch_add(1, Ordering::SeqCst);
            if self.is_load_refused {
                return Err(catalog_error(false));
            }
            let is_transient_failure = self
                .transient_reload_failures
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then_some(remaining.saturating_sub(1))
                })
                .is_ok();
            if is_transient_failure {
                return Err(catalog_error(true));
            }
            self.completed_reloads.fetch_add(1, Ordering::SeqCst);
            Ok(self.current_table_schema())
        }

        async fn write_record_batches(
            &self,
            _target: arrow::datatypes::SchemaRef,
            batches: crate::shift::iceberg_storage::BoxRecordBatchStream,
            _cancel_token: &CancellationToken,
        ) -> Result<WrittenDataFiles> {
            self.write_calls.fetch_add(1, Ordering::SeqCst);
            let attempt_batches = batches.try_collect::<Vec<_>>().await?;
            let rows_written = attempt_batches.iter().map(arrow::record_batch::RecordBatch::num_rows).sum();
            self.writes.lock().await.push(attempt_batches);
            if self
                .fail_attempts_remaining
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
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

    /// The schema of [`test_batch`] without `column`: a table that lacks it.
    fn schema_without(column: &str) -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(
            test_batch(0)
                .schema()
                .fields()
                .iter()
                .filter(|field| field.name() != column)
                .cloned()
                .collect::<Vec<_>>(),
        ))
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

    /// A WAL batch carrying a column the cached table lacks means the table was cached before a
    /// migration added it: the runner reads the table again and the retry shifts against the evolved
    /// table instead of failing the task.
    #[tokio::test]
    async fn shift_segments_reloads_a_table_cached_before_a_column_was_added_and_retries() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(1, vec![test_batch(1)])]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
            release_gate: None,
        });
        let current_schema = test_batch(0).schema();
        let storage = Arc::new(FakeStorage::with_stale_table_schema(
            schema_without("value"),
            vec![test_data_file("s3://warehouse/logs/part-00001.parquet", 1)],
        ));
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            2,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted");
        let segment = vec![test_batch(1)];
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&segment, &[0]),
            }],
            trace_context: None,
        };

        let result = runner
            .shift_segments(input, &CancellationToken::new())
            .await
            .expect("the retry against the evolved table must succeed");

        assert_eq!(result.status, TaskStatus::Ok);
        assert_eq!(storage.reload_calls.load(Ordering::SeqCst), 1);
        let writes = storage.writes.lock().await;
        assert_eq!(writes.len(), 1, "only the retry reaches the writer");
        assert_eq!(values_from_batches(&writes[0]), vec![1]);
        assert_eq!(writes[0][0].schema(), current_schema);
    }

    /// A WAL batch carrying a column the table never gains: the reloaded table still lacks it, so
    /// the task fails as a write failure at once instead of retrying the same table until its
    /// attempts or its deadline run out.
    #[tokio::test]
    async fn shift_segments_fails_without_retrying_when_the_reloaded_table_still_lacks_the_column() {
        let queue_reader = Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(1, vec![test_batch(1)])]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
            release_gate: None,
        });
        let storage = Arc::new(FakeStorage::with_table_schema_unchanged_by_reload(schema_without(
            "value",
        )));
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            "logs",
            2,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted");
        let segment = vec![test_batch(1)];
        let input = ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(&segment, &[0]),
            }],
            trace_context: None,
        };

        let failure = runner
            .shift_segments(input, &CancellationToken::new())
            .await
            .err()
            .expect("a column the table never gains must fail the task");

        assert_eq!(failure.reason(), ShiftTaskFailureReason::Write);
        assert_eq!(
            storage.reload_calls.load(Ordering::SeqCst),
            1,
            "a retry would reload again: the failure must end the task after one reload"
        );
        assert!(storage.writes.lock().await.is_empty(), "no attempt reaches the writer");
    }

    /// A queue reader serving one segment at offset 1.
    fn queue_reader_of_one_segment(batches: Vec<arrow::record_batch::RecordBatch>) -> Arc<FakeQueueReader> {
        Arc::new(FakeQueueReader {
            batches_by_offset: HashMap::from([(1, batches)]),
            delay_by_offset: HashMap::new(),
            fail_offset: None,
            started_reads: None,
            active_reads: None,
            max_active_reads: None,
            concurrency_gate: None,
            release_gate: None,
        })
    }

    /// A runner over `storage` that writes one row per output batch.
    fn logs_runner(
        queue_reader: Arc<FakeQueueReader>,
        storage: &Arc<FakeStorage>,
    ) -> ShiftTaskRunnerImpl<FakeQueueReader, FakeStorage> {
        ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(storage),
            "logs",
            1,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted")
    }

    /// A shift task over every row group of the one segment at offset 1.
    fn input_of_one_segment(batches: &[arrow::record_batch::RecordBatch]) -> ShiftInput {
        let row_group_idxs = (0..batches.len()).collect::<Vec<_>>();
        ShiftInput {
            segments: vec![SegmentToRead {
                segment_offset: 1,
                row_groups: planned_row_groups(batches, &row_group_idxs),
            }],
            trace_context: None,
        }
    }

    /// `batch` with a trailing nullable `Utf8` column `extra` holding `value` in every row.
    fn batch_with_extra_column(
        batch: &arrow::record_batch::RecordBatch,
        value: &str,
    ) -> arrow::record_batch::RecordBatch {
        let mut fields = batch.schema().fields().to_vec();
        fields.push(Arc::new(Field::new("extra", DataType::Utf8, true)));
        let mut columns = batch.columns().to_vec();
        columns.push(Arc::new(StringArray::from(vec![Some(value); batch.num_rows()])) as ArrayRef);
        arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("batch with extra")
    }

    /// The first row group fits the cached table, so the prefetch passes and the write starts; only
    /// the second one, read from the running write, reveals the column the cached table lacks. The
    /// table must be read again from this path too, or every retry would fail against the same table.
    #[tokio::test]
    async fn shift_segments_reloads_the_table_when_a_later_row_group_carries_a_column_it_lacks() {
        let first_group = ordered_single_row_batch("svc", 30, 1);
        let second_group = batch_with_extra_column(&ordered_single_row_batch("svc", 20, 2), "from-the-wal");
        let segment = vec![first_group, second_group.clone()];
        let storage = Arc::new(FakeStorage {
            evolved_table_schema: Some(second_group.schema()),
            ..FakeStorage::with_stale_table_schema(
                test_batch(0).schema(),
                vec![test_data_file("s3://warehouse/logs/part-00001.parquet", 2)],
            )
        });
        let runner = logs_runner(queue_reader_of_one_segment(segment.clone()), &storage);

        let result = runner
            .shift_segments(input_of_one_segment(&segment), &CancellationToken::new())
            .await
            .expect("the retry against the evolved table must succeed");

        assert_eq!(result.status, TaskStatus::Ok);
        assert_eq!(
            storage.write_calls.load(Ordering::SeqCst),
            2,
            "the first attempt must reach the writer"
        );
        assert_eq!(storage.reload_calls.load(Ordering::SeqCst), 1);
        let writes = storage.writes.lock().await;
        assert_eq!(writes.len(), 1, "only the retry completes its write");
        let rows = writes[0]
            .iter()
            .flat_map(|batch| {
                let values = batch
                    .column_by_name("value")
                    .expect("value column")
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("value is Int64");
                let extras = batch
                    .column_by_name("extra")
                    .expect("extra column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("extra is Utf8");
                (0..batch.num_rows())
                    .map(|row| {
                        (
                            values.value(row),
                            extras.is_valid(row).then(|| extras.value(row).to_string()),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        drop(writes);
        assert_eq!(rows, vec![(1, None), (2, Some("from-the-wal".to_string()))]);
    }

    /// A retyped column and a required column the batch lacks are not a stale cache: a reload cannot
    /// resolve them, so the task fails at once, without reading the table again or reaching the
    /// writer.
    #[tokio::test]
    async fn shift_segments_fails_without_a_reload_on_a_schema_difference_other_than_an_extra_column() {
        let batch_schema = test_batch(0).schema();
        let retyped_value = Arc::new(Schema::new(
            batch_schema
                .fields()
                .iter()
                .map(|field| {
                    if field.name() == "value" {
                        Arc::new(Field::new("value", DataType::Int32, false))
                    } else {
                        Arc::clone(field)
                    }
                })
                .collect::<Vec<_>>(),
        ));
        let mut fields_with_required_column = batch_schema.fields().to_vec();
        fields_with_required_column.push(Arc::new(Field::new("required_column", DataType::Utf8, false)));
        let required_column_missing = Arc::new(Schema::new(fields_with_required_column));

        for (case, table_schema) in [
            ("retyped column", retyped_value),
            ("required column missing from the batch", required_column_missing),
        ] {
            let segment = vec![test_batch(1)];
            let storage = Arc::new(FakeStorage::with_table_schema_unchanged_by_reload(table_schema));
            let runner = logs_runner(queue_reader_of_one_segment(segment.clone()), &storage);

            let failure = runner
                .shift_segments(input_of_one_segment(&segment), &CancellationToken::new())
                .await
                .err()
                .unwrap_or_else(|| panic!("{case}: the task must fail"));

            assert_eq!(failure.reason(), ShiftTaskFailureReason::Write, "{case}");
            assert_eq!(storage.reload_calls.load(Ordering::SeqCst), 0, "{case}: no reload");
            assert_eq!(storage.load_calls.load(Ordering::SeqCst), 1, "{case}: no retry");
            assert_eq!(storage.write_calls.load(Ordering::SeqCst), 0, "{case}: no write");
        }
    }

    /// A catalog hiccup during the reload says nothing about the table's columns, so it must not
    /// turn into the final "the table never gained the column" verdict. The next attempt still meets
    /// the stale table, which is why the reload runs twice.
    #[tokio::test]
    async fn shift_segments_retries_when_the_reload_fails_transiently() {
        let segment = vec![test_batch(1)];
        let storage = Arc::new(FakeStorage {
            transient_reload_failures: AtomicUsize::new(1),
            ..FakeStorage::with_stale_table_schema(
                schema_without("value"),
                vec![test_data_file("s3://warehouse/logs/part-00001.parquet", 1)],
            )
        });
        let runner = logs_runner(queue_reader_of_one_segment(segment.clone()), &storage);

        let result = runner
            .shift_segments(input_of_one_segment(&segment), &CancellationToken::new())
            .await
            .expect("the retry after the transient reload failure must succeed");

        assert_eq!(result.status, TaskStatus::Ok);
        assert_eq!(storage.reload_calls.load(Ordering::SeqCst), 2);
        let writes = storage.writes.lock().await;
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0][0].schema(), test_batch(0).schema());
    }

    /// A catalog failure on the first load is a write-side failure, not a WAL read: the failure
    /// reason is what the shift metrics and alerts split on.
    #[tokio::test]
    async fn shift_segments_reports_a_failed_table_load_as_a_write_failure() {
        let segment = vec![test_batch(1)];
        let storage = Arc::new(FakeStorage {
            is_load_refused: true,
            ..FakeStorage::fail_then_succeed(0, Vec::new())
        });
        let runner = logs_runner(queue_reader_of_one_segment(segment.clone()), &storage);

        let failure = runner
            .shift_segments(input_of_one_segment(&segment), &CancellationToken::new())
            .await
            .err()
            .expect("a refused table load must fail the task");

        assert_eq!(failure.reason(), ShiftTaskFailureReason::Write);
        assert_eq!(storage.write_calls.load(Ordering::SeqCst), 0);
    }

    /// The production stack end to end around a migration: a real WAL, a real table cached before
    /// the migration, and the metrics wrapper the Shifter puts in front of the storage. The segment
    /// is wider than the cached table, so the reload has to pass through the wrapper for the retry
    /// to see the evolved table; that table is in turn wider than the segment, as it is for an old
    /// pod once a newer release has migrated it. Lives here because `shift_segments` is private to
    /// this module; the fixture comes from [`crate::shift::pipeline_it_tests`].
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shift_segments_through_the_metrics_wrapper_reloads_a_stale_table_and_fills_a_column_the_wal_lacks() {
        const ENVIRONMENT_COLUMN: &str = "environment";

        let object_storage = start_object_storage().await;
        let run_id = uuid::Uuid::new_v4().simple().to_string();
        let cancel = CancellationToken::new();

        let (batch, _) = operations_to_record_batch(&evaluated_traces_request(), &test_tenant("tenant-a"))
            .expect("operations transform");
        let row_groups = sort_operations(&batch.expect("three operations rows"), 2, None)
            .expect("prepare WAL segment")
            .expect("segment row groups")
            .write_request
            .row_groups;
        let planned_row_groups = row_groups
            .iter()
            .enumerate()
            .map(|(row_group_idx, row_group)| PlannedRowGroup {
                row_group_idx,
                row_group_bytes: 1,
                boundary_range: deserialize_row_group_boundary_range(
                    row_group.metadata.as_deref().expect("row group metadata"),
                )
                .expect("boundary range"),
            })
            .collect::<Vec<_>>();
        let queue_reader = write_wal_segments(
            &object_storage,
            &format!("wal-{run_id}"),
            OPERATIONS_TOPIC,
            (OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS),
            vec![row_groups],
        )
        .await;

        let catalog = build_catalog(&object_storage, &format!("catalog-{run_id}")).await;
        let code_schema = operations_schema().expect("operations schema");
        let evaluations_type = code_schema
            .field_by_name(COL_EVALUATIONS)
            .expect("evaluations field")
            .field_type
            .as_ref()
            .clone();
        let previous_schema = iceberg::spec::Schema::builder()
            .with_fields(
                code_schema
                    .as_struct()
                    .fields()
                    .iter()
                    .filter(|field| field.name != COL_EVALUATIONS)
                    .cloned(),
            )
            .build()
            .expect("schema without evaluations");
        let ident = create_table(
            &catalog,
            OPERATIONS_TABLE,
            previous_schema,
            operations_partition_spec(&code_schema).expect("operations partition spec"),
            operations_sort_order(&code_schema).expect("operations sort order"),
        )
        .await;

        // The default TTL could expire on a slow container and refresh the cache by itself, which
        // would pass this test without the reload it is about.
        let mut config = shift_config(&object_storage, &format!("jobs-{run_id}"));
        config.write.table_cache_ttl_secs = 3600;
        let storage = Arc::new(StorageWithMetrics::new(
            Arc::new(IcebergStorage::new(
                Arc::clone(&catalog),
                OPERATIONS_TABLE,
                &config,
                64 * 1024 * 1024,
                OPERATIONS_BLOOM_COLUMNS,
                OPERATIONS_COLUMN_ENCODINGS,
            )),
            ShiftMetrics::new_disabled(),
            OPERATIONS_TOPIC,
        ));
        let runner = ShiftTaskRunnerImpl::new(
            queue_reader,
            Arc::clone(&storage),
            OPERATIONS_TOPIC,
            config.write.row_group_size,
            SortColumnsDescriptor::operations().expect("operations descriptor"),
        )
        .expect("non-zero output_batch_size must be accepted");

        storage.load_write_target(&cancel).await.expect("cache the previous table");
        let table = catalog.load_table(&ident).await.expect("load previous table");
        let tx = Transaction::new(&table);
        let tx = tx
            .update_schema()
            .add_column(AddColumn::optional(COL_EVALUATIONS, evaluations_type))
            .add_column(AddColumn::optional(
                ENVIRONMENT_COLUMN,
                Type::Primitive(PrimitiveType::String),
            ))
            .apply(tx)
            .expect("add columns applies");
        tx.commit(catalog.as_ref()).await.expect("add columns commits");
        let cached = storage.load_write_target(&cancel).await.expect("load cached table");
        assert!(
            cached.arrow_schema().field_with_name(COL_EVALUATIONS).is_err(),
            "the fixture must leave the previous table in the cache"
        );

        let result = runner
            .shift_segments(
                ShiftInput {
                    segments: vec![SegmentToRead {
                        segment_offset: 0,
                        row_groups: planned_row_groups,
                    }],
                    trace_context: None,
                },
                &cancel,
            )
            .await
            .unwrap_or_else(|failure| panic!("shift must succeed: {:?}", failure.into_error()));

        assert_eq!(result.status, TaskStatus::Ok);
        let output: ShiftOutput = serde_json::from_slice(&result.output).expect("shift output");
        let data_files = storage
            .get_data_files(&output.parquet_files, &cancel)
            .await
            .expect("data files of the shift output");
        storage
            .commit(data_files, OPERATIONS_TABLE, 0, &cancel)
            .await
            .expect("commit the shift output");

        let table = catalog.load_table(&ident).await.expect("load evolved table");
        assert_eq!(
            resolve_committed_offset(table.metadata()).expect("committed offset"),
            Some(0)
        );
        let batches = table
            .scan()
            .select_all()
            .build()
            .expect("operations scan")
            .to_arrow()
            .await
            .expect("operations scan stream")
            .try_collect::<Vec<_>>()
            .await
            .expect("operations batches");
        let mut rows = Vec::new();
        for batch in &batches {
            rows.extend(decode_evaluations(batch));
            let environment = batch.column_by_name(ENVIRONMENT_COLUMN).expect("environment column");
            assert_eq!(environment.null_count(), batch.num_rows(), "environment reads as NULL");
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(rows, expected_stored_evaluations());
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

    /// Cancellation reaches the `Cancelled` reason from the WAL prefetch as well as from the
    /// storage write, and the reason keeps no record of which one it was. The rendered text must
    /// therefore name no stage, or it would misreport two of the three call sites.
    #[test]
    fn shift_write_error_renders_cancellation_without_naming_a_stage() {
        let from_prefetch = ShiftWriteError::queue_read(IngestError::Cancelled);
        assert_eq!(from_prefetch.reason, ShiftTaskFailureReason::Cancelled);
        assert_eq!(from_prefetch.to_string(), "shift task cancelled");

        let from_storage_write = ShiftWriteError::from(IngestError::Cancelled);
        assert_eq!(from_storage_write.reason, ShiftTaskFailureReason::Cancelled);
        assert_eq!(from_storage_write.to_string(), "shift task cancelled");

        let from_retry_loop = <ShiftWriteError as icegate_common::RetryError>::cancelled();
        assert_eq!(from_retry_loop.reason, ShiftTaskFailureReason::Cancelled);
        assert_eq!(from_retry_loop.to_string(), "shift task cancelled");
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
