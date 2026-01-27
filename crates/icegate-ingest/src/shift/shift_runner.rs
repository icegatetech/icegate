use std::sync::Arc;

use async_trait::async_trait;
use icegate_jobmanager::{Error, ImmutableTask, JobManager};
use icegate_queue::{QueueReader, Topic};
use tokio_util::sync::CancellationToken;
use tracing::error;

use super::{
    ShiftInput, ShiftOutput,
    executor::{TaskStatus, parse_task_input},
    iceberg_storage::Storage,
};

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
}

impl<Q, S> ShiftTaskRunnerImpl<Q, S>
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
    /// Create a new shift task runner.
    pub fn new(queue_reader: Arc<Q>, storage: Arc<S>, topic: impl Into<String>) -> Self {
        Self {
            queue_reader,
            storage,
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl<Q, S> ShiftTaskRunner for ShiftTaskRunnerImpl<Q, S>
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
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

        let mut batches = Vec::new();
        for segment in &input.segments {
            let segment_batches = self
                .queue_reader
                .read_segment(
                    &self.topic,
                    segment.segment_offset,
                    &segment.record_batch_idxs,
                    cancel_token,
                )
                .await
                .map_err(|err| {
                    ShiftTaskFailure::new(
                        ShiftTaskFailureReason::QueueRead,
                        Error::TaskExecution(format!(
                            "failed to read segment {} row groups: {err}",
                            segment.segment_offset
                        )),
                    )
                })?;
            batches.extend(segment_batches);
        }

        if batches.is_empty() {
            return Err(ShiftTaskFailure::new(
                ShiftTaskFailureReason::EmptyBatches,
                Error::TaskExecution("shift produced no record batches to write".to_string()),
            ));
        }

        let record_batches_total = batches.len();
        let write_result = self.storage.write_record_batches(batches, cancel_token).await.map_err(|err| {
            ShiftTaskFailure::new(ShiftTaskFailureReason::Write, Error::TaskExecution(err.to_string()))
        })?;

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

        let output = ShiftOutput { parquet_files };
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
