use std::sync::Arc;

use async_trait::async_trait;
use icegate_jobmanager::{Error, ImmutableTask, JobManager};
use icegate_queue::Topic;
use tokio_util::sync::CancellationToken;

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
    /// Task cancelled before completion.
    Cancelled,
}

impl CommitFailureReason {
    /// Return a stable string representation for metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GetDataFiles => "get_data_files",
            Self::Commit => "commit",
            Self::NoParquet => "no_parquet",
            Self::Serialization => "serialization",
            Self::Cancelled => "cancelled",
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
    async fn run(
        &self,
        task: Arc<dyn ImmutableTask>,
        manager: &dyn JobManager,
        cancel_token: &CancellationToken,
    ) -> Result<CommitResult, CommitTaskFailure>;
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
    #[tracing::instrument(name="commit_run", skip(self, task, manager, cancel_token), fields(task_id = %task.id()))]
    async fn run(
        &self,
        task: Arc<dyn ImmutableTask>,
        manager: &dyn JobManager,
        cancel_token: &CancellationToken,
    ) -> Result<CommitResult, CommitTaskFailure> {
        let task_id = *task.id();
        if cancel_token.is_cancelled() {
            manager
                .complete_task(&task_id, Vec::new())
                .map_err(|err| CommitTaskFailure::new(CommitFailureReason::Cancelled, err))?;
            return Ok(CommitResult {
                status: TaskStatus::Cancelled,
                already_committed: false,
            });
        }

        let input: CommitInput = super::executor::parse_task_input(task.as_ref())
            .map_err(|err| CommitTaskFailure::new(CommitFailureReason::Serialization, err))?;

        // Link to parent plan span
        if let Some(ref tc) = input.trace_context {
            icegate_common::add_span_link(tc);
        }

        let committed_offset = self.storage.get_last_offset(cancel_token).await.map_err(|err| {
            CommitTaskFailure::new(CommitFailureReason::Commit, Error::TaskExecution(err.to_string()))
        })?;
        if committed_offset.is_some_and(|offset| offset >= input.last_offset) {
            tracing::info!(
                "commit: offset {} already committed (last_offset={})",
                committed_offset.unwrap_or(0),
                input.last_offset
            );
            manager
                .complete_task(&task_id, Vec::new())
                .map_err(|err| CommitTaskFailure::new(CommitFailureReason::Commit, err))?;
            return Ok(CommitResult {
                status: TaskStatus::Ok,
                already_committed: true,
            });
        }

        if task.depends_on().is_empty() {
            return Err(CommitTaskFailure::new(
                CommitFailureReason::NoParquet,
                Error::TaskExecution("commit task has no dependencies".to_string()),
            ));
        }

        let mut parquet_files = Vec::new();
        let mut shift_trace_contexts = Vec::new();

        for dep_task_id in task.depends_on() {
            let dep_task = manager
                .get_task(dep_task_id)
                .map_err(|err| CommitTaskFailure::new(CommitFailureReason::Commit, err))?;
            if dep_task.get_output().is_empty() {
                return Err(CommitTaskFailure::new(
                    CommitFailureReason::NoParquet,
                    Error::TaskExecution(format!("shift task '{dep_task_id}' produced empty output")),
                ));
            }

            let output: ShiftOutput = serde_json::from_slice(dep_task.get_output()).map_err(|err| {
                CommitTaskFailure::new(
                    CommitFailureReason::Serialization,
                    Error::TaskExecution(format!("failed to parse shift output for '{dep_task_id}': {err}")),
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
            return Err(CommitTaskFailure::new(
                CommitFailureReason::NoParquet,
                Error::TaskExecution("commit received no parquet files from shift tasks".to_string()),
            ));
        }

        let data_files = self.storage.get_data_files(&parquet_files, cancel_token).await.map_err(|err| {
            CommitTaskFailure::new(CommitFailureReason::GetDataFiles, Error::TaskExecution(err.to_string()))
        })?;

        self.storage
            .commit(data_files, &self.topic, input.last_offset, cancel_token)
            .await
            .map_err(|err| {
                CommitTaskFailure::new(CommitFailureReason::Commit, Error::TaskExecution(err.to_string()))
            })?;

        manager
            .complete_task(&task_id, Vec::new())
            .map_err(|err| CommitTaskFailure::new(CommitFailureReason::Commit, err))?;

        Ok(CommitResult {
            status: TaskStatus::Ok,
            already_committed: false,
        })
    }
}
