use std::sync::Arc;

use async_trait::async_trait;
use icegate_queue::Topic;
use jobmanager::{Error, TaskContext};

use super::{
    executor::{CommitInput, ShiftOutput, TaskStatus},
    iceberg_storage::Storage,
};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
/// Reason for commit task failure.
pub enum CommitFailureReason {
    /// Failed to build data files from parquet metadata.
    GetDataFiles,
    /// Failed to commit Iceberg snapshot.
    Commit,
    /// No parquet files available to commit.
    NoParquet,
    /// Failed to (de)serialize task payloads.
    Serialization,
}

impl CommitFailureReason {
    /// Return a stable string representation for metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GetDataFiles => "get_data_files",
            Self::Commit => "commit",
            Self::NoParquet => "no_parquet",
            Self::Serialization => "serialization",
        }
    }
}

/// Commit task failure with reason and underlying error.
pub struct CommitTaskFailure {
    reason: CommitFailureReason,
    error: Error,
}

impl CommitTaskFailure {
    /// Create a new commit task failure.
    pub const fn new(reason: CommitFailureReason, error: Error) -> Self {
        Self { reason, error }
    }

    /// Return the failure reason.
    pub const fn reason(&self) -> CommitFailureReason {
        self.reason
    }

    /// Convert into the underlying error.
    pub fn into_error(self) -> Error {
        self.error
    }
}

/// Result of a commit task execution.
pub struct CommitResult {
    /// Task execution status.
    pub status: TaskStatus,
    /// True if commit was skipped because offset was already committed.
    pub already_committed: bool,
}

/// Runner interface for commit tasks.
#[async_trait]
pub trait CommitTaskRunner: Send + Sync {
    /// Execute a commit task.
    async fn run(&self, ctx: &TaskContext) -> Result<CommitResult, CommitTaskFailure>;
}

/// Commit task runner implementation.
pub struct CommitTaskRunnerImpl<S> {
    storage: Arc<S>,
    topic: Topic,
}

impl<S> CommitTaskRunnerImpl<S>
where
    S: Storage + 'static,
{
    /// Create a new commit task runner.
    pub fn new(storage: Arc<S>, topic: impl Into<String>) -> Self {
        Self {
            storage,
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl<S> CommitTaskRunner for CommitTaskRunnerImpl<S>
where
    S: Storage + 'static,
{
    #[tracing::instrument(name="commit_run", skip(self, ctx), fields(task_id = %ctx.id()))]
    async fn run(&self, ctx: &TaskContext) -> Result<CommitResult, CommitTaskFailure> {
        let cancel_token = ctx.cancel_token();
        if cancel_token.is_cancelled() {
            return Ok(CommitResult {
                status: TaskStatus::Cancelled,
                already_committed: false,
            });
        }

        let input: CommitInput = super::executor::parse_task_input(ctx.input())
            .map_err(|err| CommitTaskFailure::new(CommitFailureReason::Serialization, err))?;

        // TODO(low): Move to CommitTaskRunnerWithMetrics aka instrumentation.
        // Link to parent plan span
        if let Some(ref tc) = input.trace_context {
            icegate_common::add_span_link(tc);
        }

        let committed_offset = self
            .storage
            .get_last_offset(cancel_token)
            .await
            .map_err(|err| CommitTaskFailure::new(CommitFailureReason::Commit, Error::Other(err.to_string())))?;
        if committed_offset.is_some_and(|offset| offset >= input.last_offset) {
            tracing::info!(
                "commit: offset {} already committed (last_offset={})",
                committed_offset.unwrap_or(0),
                input.last_offset
            );
            return Ok(CommitResult {
                status: TaskStatus::Ok,
                already_committed: true,
            });
        }

        let shift_task_ids = ctx.task().depends_on();
        if shift_task_ids.is_empty() {
            // Guard: the plan task creates the commit task with its shift tasks as dependencies
            // and fails the iteration rather than planning zero of them, so a dependency-less
            // commit task means the fan-out was assembled wrongly.
            return Err(CommitTaskFailure::new(
                CommitFailureReason::NoParquet,
                Error::Other("commit task has no dependencies".to_string()),
            ));
        }

        let mut parquet_files = Vec::new();
        let mut shift_trace_contexts = Vec::new();

        for dep_task_id in shift_task_ids {
            let dep_task = ctx
                .job()
                .get_task(dep_task_id)
                .map_err(|err| CommitTaskFailure::new(CommitFailureReason::Commit, err))?;
            if dep_task.get_output().is_empty() {
                return Err(CommitTaskFailure::new(
                    CommitFailureReason::NoParquet,
                    Error::Other(format!("shift task '{dep_task_id}' produced empty output")),
                ));
            }

            let output: ShiftOutput = serde_json::from_slice(dep_task.get_output()).map_err(|err| {
                CommitTaskFailure::new(
                    CommitFailureReason::Serialization,
                    Error::Other(format!("failed to parse shift output for '{dep_task_id}': {err}")),
                )
            })?;
            parquet_files.extend(output.parquet_files);

            // Collect shift trace contexts
            if let Some(tc) = output.trace_context {
                shift_trace_contexts.push(tc);
            }
        }

        // Link all parent shift spans
        icegate_common::add_span_links(shift_trace_contexts.iter().map(String::as_str));

        if parquet_files.is_empty() {
            // Guard: a shift task that wrote no parquet file fails with its own `NoParquet`
            // reason, so every dependency that completed carries at least one path.
            return Err(CommitTaskFailure::new(
                CommitFailureReason::NoParquet,
                Error::Other("commit received no parquet files from shift tasks".to_string()),
            ));
        }

        let data_files =
            self.storage.get_data_files(&parquet_files, cancel_token).await.map_err(|err| {
                CommitTaskFailure::new(CommitFailureReason::GetDataFiles, Error::Other(err.to_string()))
            })?;

        self.storage
            .commit(data_files, &self.topic, input.last_offset, cancel_token)
            .await
            .map_err(|err| CommitTaskFailure::new(CommitFailureReason::Commit, Error::Other(err.to_string())))?;

        Ok(CommitResult {
            status: TaskStatus::Ok,
            already_committed: false,
        })
    }
}
