//! Task executors for shift operations.
//!
//! Implements the plan -> shift -> commit pipeline for WAL segments processing.

use std::sync::Arc;

use async_trait::async_trait;
use jobmanager::{TaskContext, TaskExecutor, TaskOutcome, TaskResult};
use serde::{Deserialize, Serialize};

use super::{commit_runner::CommitTaskRunner, plan_runner::PlanTaskRunner, shift_runner::ShiftTaskRunner};
use crate::wal::RowGroupBoundaryRange;

/// Task code for plan segments.
pub const PLAN_TASK_CODE: &str = "plan";
/// Task code for shifting segments into Iceberg.
pub const SHIFT_TASK_CODE: &str = "shift";
/// Task code for committing shifted data to Iceberg.
pub const COMMIT_TASK_CODE: &str = "commit";

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
/// How a task runner finished the task it was given.
///
/// Every variant is a run that produced no error: a failure is reported as `Err` by the runner and
/// never reaches this type, so a status here always closes or re-opens the task rather than
/// failing it.
pub enum TaskStatus {
    /// Task completed successfully.
    Ok,
    /// Task was cancelled.
    Cancelled,
    /// Task produced no work to do.
    Empty,
}

impl TaskStatus {
    /// Return a stable string representation for metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Cancelled => "cancelled",
            Self::Empty => "empty",
        }
    }

    /// Outcome the worker applies to a task that finished with this status,
    /// storing `output` as its result.
    ///
    /// A cancelled task resolves to [`TaskOutcome::Cancelled`]: the shutdown that stopped it left
    /// its work undone, so the task stays open and is executed again instead of being recorded as
    /// a completion that shifted nothing.
    fn into_outcome(self, output: Vec<u8>) -> TaskOutcome {
        match self {
            Self::Cancelled => TaskOutcome::Cancelled,
            Self::Ok | Self::Empty => TaskOutcome::Completed(output),
        }
    }
}

/// Planned row group to read from WAL.
#[derive(Debug, Serialize, Deserialize)]
pub struct PlannedRowGroup {
    /// Row group index inside the WAL segment.
    pub row_group_idx: usize,
    /// Compressed row group size in bytes.
    pub row_group_bytes: u64,
    /// Inclusive merge-key boundary range for this row group.
    pub boundary_range: RowGroupBoundaryRange,
}

/// Segment metadata used for shift input. Segments are WAL files.
#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentToRead {
    /// segment offset.
    pub segment_offset: u64,
    /// Planned row groups inside the segment.
    pub row_groups: Vec<PlannedRowGroup>,
}

/// Input for the shift task.
#[derive(Debug, Serialize, Deserialize)]
pub struct ShiftInput {
    /// segments to read and shift.
    pub segments: Vec<SegmentToRead>,
    /// W3C trace context from parent plan span
    #[serde(default)]
    pub trace_context: Option<String>,
}

/// Output of the shift task.
#[derive(Debug, Serialize, Deserialize)]
pub struct ShiftOutput {
    /// Parquet files produced by the shift task.
    pub parquet_files: Vec<String>,
    /// W3C trace context from shift span
    #[serde(default)]
    pub trace_context: Option<String>,
}

/// Input for the commit task.
#[derive(Debug, Serialize, Deserialize)]
pub struct CommitInput {
    /// Highest segments offset to commit in snapshot summary.
    pub last_offset: u64,
    /// W3C trace context from parent plan span
    #[serde(default)]
    pub trace_context: Option<String>,
}

/// Executor of the plan task: reads the WAL plan and fans out the shift and commit tasks.
pub struct PlanExecutor<R> {
    runner: Arc<R>,
}

impl<R> PlanExecutor<R> {
    /// Wraps `runner` as the executor registered under [`PLAN_TASK_CODE`].
    pub const fn new(runner: Arc<R>) -> Self {
        Self { runner }
    }
}

#[async_trait]
impl<R> TaskExecutor for PlanExecutor<R>
where
    R: PlanTaskRunner + 'static,
{
    async fn execute(&self, ctx: TaskContext) -> TaskResult {
        let result = self.runner.run(&ctx).await?;
        Ok(result.status.into_outcome(Vec::new()))
    }
}

/// Executor of the shift task: writes one chunk of WAL row groups as Iceberg parquet files.
pub struct ShiftExecutor<R> {
    runner: Arc<R>,
}

impl<R> ShiftExecutor<R> {
    /// Wraps `runner` as the executor registered under [`SHIFT_TASK_CODE`].
    pub const fn new(runner: Arc<R>) -> Self {
        Self { runner }
    }
}

#[async_trait]
impl<R> TaskExecutor for ShiftExecutor<R>
where
    R: ShiftTaskRunner + 'static,
{
    /// The written parquet paths are the task's output payload: the commit task reads them back
    /// from the tasks it depends on, so returning them is what hands the fan-out its input.
    async fn execute(&self, ctx: TaskContext) -> TaskResult {
        let result = self
            .runner
            .run(&ctx)
            .await
            .map_err(super::shift_runner::ShiftTaskFailure::into_error)?;
        Ok(result.status.into_outcome(result.output))
    }
}

/// Executor of the commit task: commits the shifted parquet files as one Iceberg snapshot.
pub struct CommitExecutor<R> {
    runner: Arc<R>,
}

impl<R> CommitExecutor<R> {
    /// Wraps `runner` as the executor registered under [`COMMIT_TASK_CODE`].
    pub const fn new(runner: Arc<R>) -> Self {
        Self { runner }
    }
}

#[async_trait]
impl<R> TaskExecutor for CommitExecutor<R>
where
    R: CommitTaskRunner + 'static,
{
    async fn execute(&self, ctx: TaskContext) -> TaskResult {
        let result = self
            .runner
            .run(&ctx)
            .await
            .map_err(super::commit_runner::CommitTaskFailure::into_error)?;
        Ok(result.status.into_outcome(Vec::new()))
    }
}

pub(crate) fn parse_task_input<T: for<'de> Deserialize<'de>>(input: &[u8]) -> Result<T, jobmanager::Error> {
    serde_json::from_slice(input).map_err(|e| jobmanager::Error::Other(format!("failed to parse task input: {e}")))
}

#[cfg(test)]
mod tests {
    use super::{TaskOutcome, TaskStatus};

    /// A shutdown observed mid-task must leave the task open. Completing it instead would record
    /// an iteration that shifted nothing as successful, and its WAL segments would only be picked
    /// up by the next plan.
    #[test]
    fn a_cancelled_task_is_left_open_rather_than_completed() {
        assert_eq!(
            TaskStatus::Cancelled.into_outcome(b"ignored".to_vec()),
            TaskOutcome::Cancelled
        );
    }

    /// Every other status closes the task, carrying whatever payload the runner produced - the
    /// shift task's parquet paths are what the commit task reads back.
    #[test]
    fn a_finished_task_completes_with_the_payload_it_produced() {
        for status in [TaskStatus::Ok, TaskStatus::Empty] {
            assert_eq!(
                status.into_outcome(b"output".to_vec()),
                TaskOutcome::Completed(b"output".to_vec()),
                "status {status:?} must complete the task"
            );
        }
    }
}
