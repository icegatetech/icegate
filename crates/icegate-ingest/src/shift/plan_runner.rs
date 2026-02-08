use std::sync::Arc;

use async_trait::async_trait;
use icegate_jobmanager::{Error, JobManager, TaskCode, TaskDefinition};
use icegate_queue::{QueueReader, SegmentsPlan, Topic};
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

use super::{
    SHIFT_TASK_CODE, SegmentToRead, ShiftConfig, ShiftInput, executor::TaskStatus, iceberg_storage::Storage,
    timeout::TimeoutEstimator,
};

/// Result of executing a plan task.
pub struct PlanTaskResult {
    /// Task execution status.
    pub status: TaskStatus,
    /// Starting offset used for planning.
    pub start_offset: u64,
    /// Last segment offset observed in the plan.
    pub last_offset: Option<u64>,
    /// Number of segments scanned.
    pub segments_count: usize,
    /// Total record batches across all segments.
    pub record_batches_total: usize,
    /// Scheduled shift task identifiers.
    pub shift_task_ids: Vec<uuid::Uuid>,
}

/// Runner interface for plan tasks.
#[async_trait]
pub trait PlanTaskRunner: Send + Sync {
    /// Execute a plan task.
    async fn run(
        &self,
        task_id: Uuid,
        manager: &dyn JobManager,
        cancel_token: &CancellationToken,
    ) -> Result<PlanTaskResult, Error>;
}

/// Plan task runner implementation.
pub struct PlanTaskRunnerImpl<Q, S> {
    queue_reader: Arc<Q>,
    storage: Arc<S>,
    shift_config: Arc<ShiftConfig>,
    timeouts: TimeoutEstimator,
    topic: Topic,
}

impl<Q, S> PlanTaskRunnerImpl<Q, S>
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
    /// Create a new plan task runner.
    pub fn new(
        queue_reader: Arc<Q>,
        storage: Arc<S>,
        shift_config: Arc<ShiftConfig>,
        timeouts: TimeoutEstimator,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            queue_reader,
            storage,
            shift_config,
            timeouts,
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl<Q, S> PlanTaskRunner for PlanTaskRunnerImpl<Q, S>
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
    #[tracing::instrument(name="plan_run", skip(self, manager, cancel_token), fields(task_id = %task_id))]
    async fn run(
        &self,
        task_id: Uuid,
        manager: &dyn JobManager,
        cancel_token: &CancellationToken,
    ) -> Result<PlanTaskResult, Error> {
        // NOTICE: There is no guarantee of order inside the segments (WAL files), we just accept data from clients. Most likely, clients sort data by time (but this is not accurate).

        if cancel_token.is_cancelled() {
            manager.complete_task(&task_id, Vec::new())?;
            return Ok(PlanTaskResult {
                status: TaskStatus::Cancelled,
                start_offset: 0,
                last_offset: None,
                segments_count: 0,
                record_batches_total: 0,
                shift_task_ids: Vec::new(),
            });
        }

        let start_offset = self
            .storage
            .get_last_offset(cancel_token)
            .await
            .map_err(|e| Error::TaskExecution(format!("failed to read committed offset: {e}")))?
            .map_or(0, |offset| offset + 1);

        info!("plan: topic '{}' starting from offset {}", self.topic, start_offset);

        let plan = self
            .queue_reader
            .plan_segments(
                &self.topic,
                start_offset,
                "tenant_id",
                self.shift_config.read.max_record_batches_per_task,
                self.shift_config.read.max_input_bytes_per_task,
                cancel_token,
            )
            .await
            .map_err(|e| Error::TaskExecution(format!("failed to plan segment record batches: {e}")))?;

        if plan.last_segment_offset.is_none() {
            // TODO(high): now we are completing the job iteration and producing a lot of files, it may be worth restarting the task.
            info!("plan: no segments found for topic '{}'", self.topic);
            manager.complete_task(&task_id, Vec::new())?;
            return Ok(PlanTaskResult {
                status: TaskStatus::Empty,
                start_offset,
                last_offset: None,
                segments_count: plan.segments_count,
                record_batches_total: plan.record_batches_total,
                shift_task_ids: Vec::new(),
            });
        }

        let segments_count = plan.segments_count;
        let record_batches_total = plan.record_batches_total;
        let last_offset = plan.last_segment_offset;
        let shift_task_ids = schedule_shift_tasks(manager, plan, &self.timeouts)?;

        let last_offset_value = last_offset.unwrap_or(0);
        info!(
            "plan: scheduling shift for {} tasks (last_offset={})",
            shift_task_ids.len(),
            last_offset_value
        );

        let commit_input = super::CommitInput {
            last_offset: last_offset_value,
            trace_context: icegate_common::extract_current_trace_context(),
        };
        let commit_timeout = self
            .timeouts
            .commit_timeout(shift_task_ids.len())
            .map_err(|e| Error::TaskExecution(e.to_string()))?;
        let commit_task = TaskDefinition::new(
            TaskCode::new(super::COMMIT_TASK_CODE),
            serde_json::to_vec(&commit_input)
                .map_err(|e| Error::TaskExecution(format!("failed to serialize commit input: {e}")))?,
            commit_timeout,
        )?
        .with_dependencies(shift_task_ids.clone());

        manager.add_task(commit_task)?;
        manager.complete_task(&task_id, Vec::new())?;

        Ok(PlanTaskResult {
            status: TaskStatus::Ok,
            start_offset,
            last_offset,
            segments_count,
            record_batches_total,
            shift_task_ids,
        })
    }
}

fn schedule_shift_tasks(
    manager: &dyn JobManager,
    plan: SegmentsPlan,
    timeouts: &TimeoutEstimator,
) -> Result<Vec<uuid::Uuid>, Error> {
    let mut shift_task_ids = Vec::new();
    for group in plan.groups {
        let segments = group
            .segments
            .into_iter()
            .map(|segment| SegmentToRead {
                segment_offset: segment.segment_offset,
                record_batch_idxs: segment.record_batch_idxs,
            })
            .collect::<Vec<_>>();

        if segments.is_empty() {
            continue;
        }

        let task_id = create_shift_task(
            manager,
            &group.group_col_val,
            segments,
            group.segments_count,
            group.record_batches_total,
            timeouts,
        )?;
        shift_task_ids.push(task_id);
    }

    if shift_task_ids.is_empty() {
        return Err(Error::TaskExecution("no tenant segments to schedule".to_string()));
    }

    Ok(shift_task_ids)
}

fn create_shift_task(
    manager: &dyn JobManager,
    tenant_id: &str,
    segments: Vec<SegmentToRead>,
    segments_count: usize,
    record_batches_total: usize,
    timeouts: &TimeoutEstimator,
) -> Result<uuid::Uuid, Error> {
    let trace_context = icegate_common::extract_current_trace_context();

    let shift_input = ShiftInput {
        tenant_id: tenant_id.to_string(),
        segments,
        trace_context,
    };

    let shift_timeout = timeouts
        .shift_timeout(segments_count, record_batches_total)
        .map_err(|e| Error::TaskExecution(e.to_string()))?;
    let shift_task = TaskDefinition::new(
        TaskCode::new(SHIFT_TASK_CODE),
        serde_json::to_vec(&shift_input)
            .map_err(|e| Error::TaskExecution(format!("failed to serialize shift input: {e}")))?,
        shift_timeout,
    )?;

    manager.add_task(shift_task)
}
