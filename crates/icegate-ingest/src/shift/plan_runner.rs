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
use crate::wal_sort::deserialize_logs_row_group_metadata;

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
        let shift_task_ids = schedule_shift_tasks(
            manager,
            self.queue_reader.as_ref(),
            &self.topic,
            plan,
            &self.timeouts,
            cancel_token,
        )
        .await?;

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

async fn schedule_shift_tasks(
    manager: &dyn JobManager,
    queue_reader: &dyn QueueReader,
    topic: &Topic,
    plan: SegmentsPlan,
    timeouts: &TimeoutEstimator,
    cancel_token: &CancellationToken,
) -> Result<Vec<uuid::Uuid>, Error> {
    let mut shift_task_ids = Vec::new();
    for group in plan.groups {
        let mut segments = Vec::with_capacity(group.segments.len());
        for segment in group.segments {
            let metadata_by_row_group = queue_reader
                .read_segment_row_group_metadata(topic, segment.segment_offset, cancel_token)
                .await
                .map_err(|e| {
                    Error::TaskExecution(format!(
                        "failed to read row-group metadata for WAL segment {}: {e}",
                        segment.segment_offset
                    ))
                })?;
            let row_groups = segment
                .row_groups
                .into_iter()
                .map(|row_group| {
                    let metadata = metadata_by_row_group.get(&row_group.row_group_idx).ok_or_else(|| {
                        Error::TaskExecution(format!(
                            "missing logs row-group metadata for WAL segment {} row group {}",
                            segment.segment_offset, row_group.row_group_idx
                        ))
                    })?;
                    let boundary_key = deserialize_logs_row_group_metadata(metadata)
                        .map_err(|e| Error::TaskExecution(e.to_string()))?;
                    Ok(super::PlannedRowGroup {
                        row_group_idx: row_group.row_group_idx,
                        row_group_bytes: row_group.row_group_bytes,
                        boundary_key,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?;
            segments.push(SegmentToRead {
                segment_offset: segment.segment_offset,
                row_groups,
            });
        }

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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use icegate_jobmanager::{ImmutableTask, JobManager, TaskCode, TaskDefinition};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::schedule_shift_tasks;
    use crate::{shift::timeout::TimeoutEstimator, wal_sort::serialize_logs_row_group_metadata};

    struct RecordingManager {
        task_ids: std::sync::Mutex<Vec<Uuid>>,
    }

    impl RecordingManager {
        fn new() -> Self {
            Self {
                task_ids: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl JobManager for RecordingManager {
        fn add_task(&self, _task_def: TaskDefinition) -> std::result::Result<Uuid, icegate_jobmanager::Error> {
            let task_id = Uuid::new_v4();
            self.task_ids.lock().expect("task ids lock").push(task_id);
            Ok(task_id)
        }

        fn complete_task(
            &self,
            _task_id: &Uuid,
            _output: Vec<u8>,
        ) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("complete_task is not expected in plan runner tests");
        }

        fn fail_task(&self, _task_id: &Uuid, _error_msg: &str) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("fail_task is not expected in plan runner tests");
        }

        fn set_next_start_at(
            &self,
            _next_start_at: DateTime<Utc>,
        ) -> std::result::Result<(), icegate_jobmanager::Error> {
            panic!("set_next_start_at is not expected in plan runner tests");
        }

        fn get_task(&self, _task_id: &Uuid) -> std::result::Result<Arc<dyn ImmutableTask>, icegate_jobmanager::Error> {
            panic!("get_task is not expected in plan runner tests");
        }

        fn get_tasks_by_code(
            &self,
            _code: &TaskCode,
        ) -> std::result::Result<Vec<Arc<dyn ImmutableTask>>, icegate_jobmanager::Error> {
            panic!("get_tasks_by_code is not expected in plan runner tests");
        }
    }

    struct FakeQueueReader {
        metadata_by_segment: HashMap<u64, HashMap<usize, String>>,
    }

    #[async_trait]
    impl icegate_queue::QueueReader for FakeQueueReader {
        async fn plan_segments(
            &self,
            _topic: &icegate_queue::Topic,
            _start_offset: u64,
            _group_by_column_name: &str,
            _max_record_batches_per_task: usize,
            _max_input_bytes_per_task: u64,
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::SegmentsPlan> {
            panic!("plan_segments is not expected in plan runner tests");
        }

        async fn read_segment(
            &self,
            _topic: &icegate_queue::Topic,
            _offset: u64,
            _record_batch_idxs: &[usize],
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::RecordBatchStream> {
            panic!("read_segment is not expected in plan runner tests");
        }

        async fn read_segment_row_group_metadata(
            &self,
            _topic: &icegate_queue::Topic,
            offset: u64,
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<HashMap<usize, String>> {
            Ok(self.metadata_by_segment.get(&offset).cloned().unwrap_or_default())
        }
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

    fn single_row_group_plan() -> icegate_queue::SegmentsPlan {
        icegate_queue::SegmentsPlan {
            groups: vec![icegate_queue::GroupedSegmentsPlan {
                group_col_val: "tenant-a".to_string(),
                segments: vec![icegate_queue::SegmentRecordBatchIdxs {
                    segment_offset: 7,
                    row_groups: vec![icegate_queue::PlannedRowGroup {
                        row_group_idx: 0,
                        row_group_bytes: 1,
                    }],
                }],
                segments_count: 1,
                record_batches_total: 1,
                input_bytes_total: 1,
            }],
            last_segment_offset: Some(7),
            segments_count: 1,
            record_batches_total: 1,
            input_bytes_total: 1,
        }
    }

    #[tokio::test]
    async fn schedule_shift_tasks_rejects_placeholder_boundary_metadata() {
        let queue_reader = FakeQueueReader {
            metadata_by_segment: HashMap::from([(7, HashMap::from([(0, "{}".to_string())]))]),
        };
        let manager = RecordingManager::new();

        let err = schedule_shift_tasks(
            &manager,
            &queue_reader,
            &"logs".to_string(),
            single_row_group_plan(),
            &test_timeouts(),
            &CancellationToken::new(),
        )
        .await
        .expect_err("placeholder metadata must be rejected");

        assert!(err.to_string().contains("missing field"));
    }

    #[tokio::test]
    async fn schedule_shift_tasks_accepts_boundary_metadata_serialized_by_wal_sort() {
        let queue_reader = FakeQueueReader {
            metadata_by_segment: HashMap::from([(
                7,
                HashMap::from([(
                    0,
                    serialize_logs_row_group_metadata(&icegate_common::RowGroupBoundaryKey {
                        cloud_account_id: Some("acc-1".to_string()),
                        service_name: Some("svc-2".to_string()),
                        timestamp_micros: Some(30),
                    })
                    .expect("serialize metadata"),
                )]),
            )]),
        };
        let manager = RecordingManager::new();

        let task_ids = schedule_shift_tasks(
            &manager,
            &queue_reader,
            &"logs".to_string(),
            single_row_group_plan(),
            &test_timeouts(),
            &CancellationToken::new(),
        )
        .await
        .expect("serialized metadata must be accepted");

        assert_eq!(task_ids.len(), 1);
    }
}
