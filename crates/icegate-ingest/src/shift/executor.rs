//! Task executors for shift operations.
//!
//! Implements the plan -> shift -> commit pipeline for WAL segments processing.

use std::sync::Arc;

use icegate_jobmanager::{ImmutableTask, JobManager, registry::TaskExecutorFn};
use serde::{Deserialize, Serialize};

use super::{commit_runner::CommitTaskRunner, plan_runner::PlanTaskRunner, shift_runner::ShiftTaskRunner};

/// Task code for plan segments.
pub const PLAN_TASK_CODE: &str = "plan";
/// Task code for shifting segments into Iceberg.
pub const SHIFT_TASK_CODE: &str = "shift";
/// Task code for committing shifted data to Iceberg.
pub const COMMIT_TASK_CODE: &str = "commit";

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
/// Task execution status used for metrics and control flow.
pub enum TaskStatus {
    /// Task completed successfully.
    Ok,
    /// Task failed with an error.
    Error,
    /// Task exceeded its timeout.
    Timeout,
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
            Self::Error => "error",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Empty => "empty",
        }
    }
}

/// Segment metadata used for shift input. Segments are WAL files.
#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentToRead {
    /// segment offset.
    pub segment_offset: u64,
    /// Batch offsets for this tenant inside the segment. Batches are grouped by column.
    pub record_batch_idxs: Vec<usize>,
}

/// Input for the shift task.
#[derive(Debug, Serialize, Deserialize)]
pub struct ShiftInput {
    /// Tenant identifier handled by this task.
    pub tenant_id: String,
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

/// Shared executor dependencies for shift tasks.
#[allow(clippy::struct_field_names)]
pub struct Executor<PR, SR, CR> {
    plan_runner: Arc<PR>,
    shift_runner: Arc<SR>,
    commit_runner: Arc<CR>,
}

impl<PR, SR, CR> Executor<PR, SR, CR>
where
    PR: PlanTaskRunner + 'static,
    SR: ShiftTaskRunner + 'static,
    CR: CommitTaskRunner + 'static,
{
    /// Creates a new executor and initializes shared dependencies.
    pub const fn new(plan_runner: Arc<PR>, shift_runner: Arc<SR>, commit_runner: Arc<CR>) -> Self {
        Self {
            plan_runner,
            shift_runner,
            commit_runner,
        }
    }
    /// Creates executor for the plan task.
    pub fn plan_executor(self: Arc<Self>) -> TaskExecutorFn {
        Arc::new(
            move |task: Arc<dyn ImmutableTask>, manager: &dyn JobManager, cancel_token| {
                let executor = Arc::clone(&self);
                let task_id = *task.id();

                let fut = async move {
                    executor.plan_runner.run(task_id, manager, &cancel_token).await?;
                    Ok(())
                };

                Box::pin(fut)
            },
        )
    }

    /// Creates executor for the shift task.
    pub fn shift_executor(self: Arc<Self>) -> TaskExecutorFn {
        Arc::new(
            move |task: Arc<dyn ImmutableTask>, manager: &dyn JobManager, cancel_token| {
                let executor = Arc::clone(&self);

                let fut = async move {
                    executor
                        .shift_runner
                        .run(task, manager, &cancel_token)
                        .await
                        .map(|_| ())
                        .map_err(super::shift_runner::ShiftTaskFailure::into_error)
                };

                Box::pin(fut)
            },
        )
    }

    /// Creates executor for the commit task.
    pub fn commit_executor(self: Arc<Self>) -> TaskExecutorFn {
        Arc::new(
            move |task: Arc<dyn ImmutableTask>, manager: &dyn JobManager, cancel_token| {
                let executor = Arc::clone(&self);

                let fut = async move {
                    executor
                        .commit_runner
                        .run(task, manager, &cancel_token)
                        .await
                        .map(|_| ())
                        .map_err(super::commit_runner::CommitTaskFailure::into_error)
                };

                Box::pin(fut)
            },
        )
    }
}

pub(crate) fn parse_task_input<T: for<'de> Deserialize<'de>>(
    task: &dyn ImmutableTask,
) -> Result<T, icegate_jobmanager::Error> {
    serde_json::from_slice(task.get_input())
        .map_err(|e| icegate_jobmanager::Error::TaskExecution(format!("failed to parse task input: {e}")))
}
