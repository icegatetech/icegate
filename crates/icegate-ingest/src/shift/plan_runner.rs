use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use icegate_jobmanager::{Error, JobManager, TaskCode, TaskDefinition};
use icegate_queue::{QueueReader, SegmentsPlan, Topic};
use tokio_util::sync::CancellationToken;
use tracing::info;
use uuid::Uuid;

#[cfg(test)]
pub(super) use super::planner::PLAN_FIELD_BOUNDARY_RANGE;
#[cfg(test)]
pub(super) use super::planner_partitioning::{PLAN_FIELD_TENANT_ID, PLAN_FIELD_TIMESTAMP_RANGE};
use super::{
    SHIFT_TASK_CODE, SegmentToRead, ShiftConfig, ShiftInput,
    executor::TaskStatus,
    iceberg_storage::Storage,
    planner::{PlanRowGroup, PlannedChunk, PlannerConfig, plan_row_groups},
    planner_partitioning::PlannerPartitionSpec,
    timeout::TimeoutEstimator,
};

/// Planner summary carried in [`PlanTaskResult`] for the instrumentation layer
/// to emit telemetry without touching [`ShiftMetrics`].
#[derive(Debug)]
pub(super) struct PlanSummary {
    pub(super) n_row_groups: usize,
    pub(super) n_clusters: usize,
    pub(super) n_chunks: usize,
    pub(super) max_cluster_bytes: u64,
    pub(super) oversized_clusters: usize,
    pub(super) tail_merged: bool,
    pub(super) cross_partition_row_groups: usize,
    pub(super) shift_task_input_bytes: Vec<u64>,
}

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
    /// Planner summary for telemetry emission in the instrumentation layer.
    pub(super) plan_summary: Option<PlanSummary>,
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
    partition_spec: &'static PlannerPartitionSpec,
    extract_fields: Box<[icegate_queue::ExtractField]>,
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
        partition_spec: &'static PlannerPartitionSpec,
    ) -> Self {
        Self {
            queue_reader,
            storage,
            shift_config,
            timeouts,
            topic: topic.into(),
            extract_fields: partition_spec.extract_fields().into_boxed_slice(),
            partition_spec,
        }
    }

    fn schedule_shift_tasks(
        &self,
        manager: &dyn JobManager,
        plan: SegmentsPlan,
        cancel_token: &CancellationToken,
    ) -> Result<(Vec<uuid::Uuid>, PlanSummary), Error> {
        let cancelled_err = || Error::TaskExecution("plan task cancelled during shift task scheduling".to_string());

        if cancel_token.is_cancelled() {
            return Err(cancelled_err());
        }

        let row_groups = self
            .partition_spec
            .plan_entries_to_row_groups(plan.entries)
            .map_err(|e| Error::TaskExecution(e.to_string()))?;
        let limits = PlannerConfig::from_bounds(
            self.shift_config.read.lower_bound_input_mb_per_task * 1024 * 1024,
            self.shift_config.read.upper_bound_input_mb_per_task * 1024 * 1024,
            self.shift_config.read.max_record_batches_per_task,
        );

        let n_row_groups = row_groups.len();
        let (chunks, stats) =
            plan_row_groups(row_groups, &limits, cancel_token).map_err(|e| Error::TaskExecution(e.to_string()))?;
        let summary = PlanSummary {
            n_row_groups,
            n_clusters: stats.clusters,
            n_chunks: chunks.len(),
            max_cluster_bytes: stats.max_cluster_bytes,
            oversized_clusters: stats.oversized_clusters,
            tail_merged: stats.tail_merged,
            cross_partition_row_groups: stats.cross_partition_row_groups,
            shift_task_input_bytes: chunks.iter().map(|c| c.total_bytes).collect(),
        };
        let mut shift_task_ids = Vec::new();
        for chunk in chunks {
            if cancel_token.is_cancelled() {
                return Err(cancelled_err());
            }
            let task_id = self.schedule_shift_task_for_chunk(manager, chunk)?;
            shift_task_ids.push(task_id);
        }

        if shift_task_ids.is_empty() {
            return Err(Error::TaskExecution("no row groups to schedule".to_string()));
        }

        Ok((shift_task_ids, summary))
    }

    /// Convert one planner chunk into a `ShiftInput` and submit it as a shift
    /// task. Row groups inside the chunk are bucketed by `wal_offset` so the
    /// shift runner can read each segment exactly once; segment iteration order
    /// inside the resulting `ShiftInput` is non-deterministic.
    fn schedule_shift_task_for_chunk(
        &self,
        manager: &dyn JobManager,
        chunk: PlannedChunk,
    ) -> Result<uuid::Uuid, Error> {
        let total_rg = chunk.row_groups.len();
        let mut by_segment: HashMap<u64, Vec<super::PlannedRowGroup>> = HashMap::new();
        for PlanRowGroup {
            wal_offset,
            row_group_idx,
            row_group_bytes,
            boundary_range,
            partition: _,
        } in chunk.row_groups
        {
            by_segment.entry(wal_offset).or_default().push(super::PlannedRowGroup {
                row_group_idx,
                row_group_bytes,
                boundary_range,
            });
        }

        let segments: Vec<SegmentToRead> = by_segment
            .into_iter()
            .map(|(segment_offset, row_groups)| SegmentToRead {
                segment_offset,
                row_groups,
            })
            .collect();
        let segments_count = segments.len();
        self.create_shift_task(manager, segments, segments_count, total_rg)
    }

    fn create_shift_task(
        &self,
        manager: &dyn JobManager,
        segments: Vec<SegmentToRead>,
        segments_count: usize,
        record_batches_total: usize,
    ) -> Result<uuid::Uuid, Error> {
        let trace_context = icegate_common::extract_current_trace_context();

        let shift_input = ShiftInput {
            segments,
            trace_context,
        };

        let shift_timeout = self
            .timeouts
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
                plan_summary: None,
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
            .plan_segments(&self.topic, start_offset, &self.extract_fields, cancel_token)
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
                record_batches_total: plan.row_groups_total,
                shift_task_ids: Vec::new(),
                plan_summary: None,
            });
        }

        let segments_count = plan.segments_count;
        let record_batches_total = plan.row_groups_total;
        let last_offset = plan.last_segment_offset;
        let (shift_task_ids, plan_summary) = self.schedule_shift_tasks(manager, plan, cancel_token)?;

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
            plan_summary: Some(plan_summary),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use iceberg::spec::DataFile;
    use icegate_jobmanager::{ImmutableTask, JobManager, TaskCode, TaskDefinition};
    use icegate_queue::ExtractedValue;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{PLAN_FIELD_BOUNDARY_RANGE, PLAN_FIELD_TENANT_ID, PLAN_FIELD_TIMESTAMP_RANGE, PlanTaskRunnerImpl};
    use crate::{
        error::Result as IngestResult,
        shift::{
            ShiftConfig,
            iceberg_storage::{BoxRecordBatchStream, Storage, WrittenDataFiles},
            timeout::TimeoutEstimator,
        },
        wal::serialize_row_group_boundary_range,
    };

    /// Build a `RowGroupPlanEntry` for tests with the planner's expected
    /// `extracted` fields populated.
    fn plan_entry(
        partition_value: Option<&str>,
        wal_offset: u64,
        row_group_idx: usize,
        row_group_bytes: u64,
        boundary_payload: Option<String>,
    ) -> icegate_queue::RowGroupPlanEntry {
        plan_entry_with_ts(
            partition_value,
            wal_offset,
            row_group_idx,
            row_group_bytes,
            boundary_payload,
            Some((1, 2)),
        )
    }

    fn plan_entry_with_ts(
        partition_value: Option<&str>,
        wal_offset: u64,
        row_group_idx: usize,
        row_group_bytes: u64,
        boundary_payload: Option<String>,
        timestamp_range: Option<(i64, i64)>,
    ) -> icegate_queue::RowGroupPlanEntry {
        let mut extracted = HashMap::new();
        if let Some(value) = partition_value {
            extracted.insert(
                PLAN_FIELD_TENANT_ID.to_string(),
                ExtractedValue::Utf8(value.to_string()),
            );
        }
        if let Some(p) = boundary_payload {
            extracted.insert(PLAN_FIELD_BOUNDARY_RANGE.to_string(), ExtractedValue::Utf8(p));
        }
        if let Some((min_ts, max_ts)) = timestamp_range {
            extracted.insert(
                PLAN_FIELD_TIMESTAMP_RANGE.to_string(),
                ExtractedValue::TimestampMicrosRange(min_ts, max_ts),
            );
        }
        icegate_queue::RowGroupPlanEntry {
            wal_offset,
            row_group_idx,
            row_group_bytes,
            extracted,
        }
    }

    struct RecordingManager {
        task_ids: std::sync::Mutex<Vec<Uuid>>,
        task_payloads: std::sync::Mutex<Vec<Vec<u8>>>,
    }

    impl RecordingManager {
        fn new() -> Self {
            Self {
                task_ids: std::sync::Mutex::new(Vec::new()),
                task_payloads: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn payloads(&self) -> Vec<Vec<u8>> {
            self.task_payloads.lock().expect("payloads lock").clone()
        }
    }

    impl JobManager for RecordingManager {
        fn add_task(&self, task_def: TaskDefinition) -> std::result::Result<Uuid, icegate_jobmanager::Error> {
            let task_id = Uuid::new_v4();
            self.task_ids.lock().expect("task ids lock").push(task_id);
            self.task_payloads
                .lock()
                .expect("payloads lock")
                .push(task_def.input().to_vec());
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

    #[derive(Default)]
    struct FakeQueueReader;

    struct FakeStorage;

    #[async_trait]
    impl Storage for FakeStorage {
        async fn get_last_offset(&self, _cancel_token: &CancellationToken) -> IngestResult<Option<u64>> {
            panic!("get_last_offset is not expected in plan runner tests");
        }

        async fn write_record_batches(
            &self,
            _batches: BoxRecordBatchStream,
            _cancel_token: &CancellationToken,
        ) -> IngestResult<WrittenDataFiles> {
            panic!("write_record_batches is not expected in plan runner tests");
        }

        async fn get_data_files(
            &self,
            _parquet_paths: &[String],
            _cancel_token: &CancellationToken,
        ) -> IngestResult<Vec<DataFile>> {
            panic!("get_data_files is not expected in plan runner tests");
        }

        async fn commit(
            &self,
            _data_files: Vec<DataFile>,
            _record_type: &str,
            _last_offset: u64,
            _cancel_token: &CancellationToken,
        ) -> IngestResult<usize> {
            panic!("commit is not expected in plan runner tests");
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

    fn test_runner(queue_reader: FakeQueueReader) -> PlanTaskRunnerImpl<FakeQueueReader, FakeStorage> {
        PlanTaskRunnerImpl::new(
            Arc::new(queue_reader),
            Arc::new(FakeStorage),
            Arc::new(ShiftConfig::default()),
            test_timeouts(),
            "logs",
            &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
        )
    }

    fn single_row_group_plan() -> icegate_queue::SegmentsPlan {
        icegate_queue::SegmentsPlan {
            entries: vec![plan_entry(Some("tenant-a"), 7, 0, 1, None)],
            last_segment_offset: Some(7),
            segments_count: 1,
            row_groups_total: 1,
            input_bytes_total: 1,
        }
    }

    fn set_boundary(entry: &mut icegate_queue::RowGroupPlanEntry, payload: String) {
        entry
            .extracted
            .insert(PLAN_FIELD_BOUNDARY_RANGE.to_string(), ExtractedValue::Utf8(payload));
    }

    /// Build a single-component sort-key boundary `(account, service, ts DESC)`
    /// where `ts` carries the same physical value in `min_key` and `max_key`.
    fn point_boundary_payload(account: &str, service: &str, ts: i64) -> String {
        let key = || {
            crate::wal::RowGroupBoundaryKey::new(vec![
                crate::wal::test_utils::boundary_component_string(Some(account.to_string()), false, true),
                crate::wal::test_utils::boundary_component_string(Some(service.to_string()), false, true),
                crate::wal::test_utils::boundary_component_timestamp_micros(Some(ts), true, true),
            ])
        };
        serialize_row_group_boundary_range(&crate::wal::RowGroupBoundaryRange {
            names: Arc::from([
                "cloud_account_id".to_string(),
                "service_name".to_string(),
                "timestamp".to_string(),
            ]),
            min_key: key(),
            max_key: key(),
        })
        .expect("serialize metadata")
    }

    #[tokio::test]
    async fn schedule_shift_tasks_stops_immediately_when_cancelled() {
        let runner = test_runner(FakeQueueReader);
        let manager = RecordingManager::new();
        let plan = single_row_group_plan();
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();

        let err = runner
            .schedule_shift_tasks(&manager, plan, &cancel_token)
            .expect_err("cancelled token must stop scheduling");

        assert!(err.to_string().contains("cancelled"));
        assert!(manager.task_ids.lock().expect("task ids lock").is_empty());
    }

    /// An empty `tenant_id` extracted from a WAL row group must fail the plan
    /// task end-to-end before any shift task is scheduled. The underlying
    /// invariant lives in `plan_entries_to_row_groups`; this guard catches
    /// regressions where the runner could swallow the error or partially
    /// submit work before the validator runs.
    #[tokio::test]
    async fn schedule_shift_tasks_fails_fast_on_empty_tenant_id() {
        let runner = test_runner(FakeQueueReader);
        let manager = RecordingManager::new();
        // Two entries: first valid, second with empty tenant_id. The plan must
        // surface the error and submit zero shift tasks even though the first
        // row group on its own would have been schedulable.
        let payload = point_boundary_payload("acc-1", "svc-1", 10);
        let plan = icegate_queue::SegmentsPlan {
            entries: vec![
                plan_entry(Some("tenant-a"), 7, 0, 1, Some(payload.clone())),
                plan_entry(Some(""), 7, 1, 1, Some(payload)),
            ],
            last_segment_offset: Some(7),
            segments_count: 1,
            row_groups_total: 2,
            input_bytes_total: 2,
        };

        let err = runner
            .schedule_shift_tasks(&manager, plan, &CancellationToken::new())
            .expect_err("empty tenant_id must abort plan task");

        assert!(err.to_string().contains("tenant_id"));
        assert!(
            manager.task_ids.lock().expect("task ids lock").is_empty(),
            "no shift task may be submitted when validation fails"
        );
    }

    #[tokio::test]
    async fn schedule_shift_tasks_rejects_placeholder_boundary_metadata() {
        let runner = test_runner(FakeQueueReader);
        let manager = RecordingManager::new();
        let mut plan = single_row_group_plan();
        set_boundary(&mut plan.entries[0], "{}".to_string());

        let err = runner
            .schedule_shift_tasks(&manager, plan, &CancellationToken::new())
            .expect_err("placeholder metadata must be rejected");

        assert!(err.to_string().contains("missing field"));
    }

    #[tokio::test]
    async fn schedule_shift_tasks_accepts_boundary_metadata_serialized_by_wal_sort() {
        let runner = test_runner(FakeQueueReader);
        let manager = RecordingManager::new();
        let mut plan = single_row_group_plan();
        set_boundary(&mut plan.entries[0], point_boundary_payload("acc-1", "svc-2", 30));

        let (task_ids, _) = runner
            .schedule_shift_tasks(&manager, plan, &CancellationToken::new())
            .expect("serialized metadata must be accepted");

        assert_eq!(task_ids.len(), 1);
    }

    #[tokio::test]
    async fn shift_input_does_not_include_tenant_id() {
        let runner = test_runner(FakeQueueReader);
        let manager = RecordingManager::new();
        let mut plan = single_row_group_plan();
        set_boundary(&mut plan.entries[0], point_boundary_payload("acc-1", "svc-2", 30));

        runner
            .schedule_shift_tasks(&manager, plan, &CancellationToken::new())
            .expect("plan must schedule");

        let payloads = manager.payloads();
        assert_eq!(payloads.len(), 1);
        let payload: serde_json::Value = serde_json::from_slice(&payloads[0]).expect("shift input json");
        assert!(payload.get("tenant_id").is_none());
        assert!(payload.get("segments").is_some());
    }

    #[tokio::test]
    async fn schedule_shift_tasks_handles_mixed_partition_buckets_in_single_pass() {
        let runner = test_runner(FakeQueueReader);
        let manager = RecordingManager::new();
        let payload = point_boundary_payload("acc-1", "svc-1", 10);
        let plan = icegate_queue::SegmentsPlan {
            entries: vec![
                plan_entry(Some("tenant-a"), 7, 0, 1, Some(payload.clone())),
                plan_entry(Some("tenant-b"), 7, 1, 1, Some(payload)),
            ],
            last_segment_offset: Some(7),
            segments_count: 1,
            row_groups_total: 2,
            input_bytes_total: 2,
        };

        let (task_ids, summary) = runner
            .schedule_shift_tasks(&manager, plan, &CancellationToken::new())
            .expect("mixed partition-bucket plan must schedule successfully");

        assert_eq!(task_ids.len(), 2);
        assert_eq!(summary.n_row_groups, 2);
        assert_eq!(summary.n_chunks, 2);
    }

    /// Build a `(account, service, ts DESC)` boundary range with single-value
    /// account/service slots, used to simulate a sorted WAL row group inside
    /// a single service.
    fn timestamp_boundary(account: &str, service: &str, min_ts: i64, max_ts: i64) -> String {
        // Sort key: account ASC, service ASC, ts DESC (per logs schema).
        // Under DESC ordering, the row's MIN value sits at max_ts and the MAX
        // value at min_ts.
        serialize_row_group_boundary_range(&crate::wal::RowGroupBoundaryRange {
            names: Arc::from([
                "cloud_account_id".to_string(),
                "service_name".to_string(),
                "timestamp".to_string(),
            ]),
            min_key: crate::wal::RowGroupBoundaryKey::new(vec![
                crate::wal::test_utils::boundary_component_string(Some(account.to_string()), false, true),
                crate::wal::test_utils::boundary_component_string(Some(service.to_string()), false, true),
                crate::wal::test_utils::boundary_component_timestamp_micros(Some(max_ts), true, true),
            ]),
            max_key: crate::wal::RowGroupBoundaryKey::new(vec![
                crate::wal::test_utils::boundary_component_string(Some(account.to_string()), false, true),
                crate::wal::test_utils::boundary_component_string(Some(service.to_string()), false, true),
                crate::wal::test_utils::boundary_component_timestamp_micros(Some(min_ts), true, true),
            ]),
        })
        .expect("serialize metadata")
    }

    /// Reconstruct the `(min_key, max_key)` of every shift task by inspecting
    /// the encoded `ShiftInput` payloads recorded by `RecordingManager`.
    fn shift_task_bounds(
        manager: &RecordingManager,
    ) -> Vec<(crate::wal::RowGroupBoundaryKey, crate::wal::RowGroupBoundaryKey)> {
        manager
            .payloads()
            .into_iter()
            .map(|payload| {
                let input: super::ShiftInput = serde_json::from_slice(&payload).expect("decode shift input");
                let mut min: Option<crate::wal::RowGroupBoundaryKey> = None;
                let mut max: Option<crate::wal::RowGroupBoundaryKey> = None;
                for segment in &input.segments {
                    for rg in &segment.row_groups {
                        let rg_min = rg.boundary_range.min_key.clone();
                        let rg_max = rg.boundary_range.max_key.clone();
                        min = Some(min.map_or_else(
                            || rg_min.clone(),
                            |cur| {
                                if rg_min.compare(&cur) == std::cmp::Ordering::Less {
                                    rg_min.clone()
                                } else {
                                    cur
                                }
                            },
                        ));
                        max = Some(max.map_or_else(
                            || rg_max.clone(),
                            |cur| {
                                if rg_max.compare(&cur) == std::cmp::Ordering::Greater {
                                    rg_max.clone()
                                } else {
                                    cur
                                }
                            },
                        ));
                    }
                }
                (min.expect("min"), max.expect("max"))
            })
            .collect()
    }

    /// 6 services × 10 row groups, each row group disjoint inside a service:
    /// after sort/cluster we expect one cluster per service, and the bin-pack
    /// must emit shift tasks whose boundary ranges are pairwise disjoint
    /// (because cluster boundaries fall on service-name transitions).
    #[tokio::test]
    async fn schedule_shift_tasks_emits_pairwise_disjoint_chunks_for_six_service_workload() {
        let runner = test_runner(FakeQueueReader);
        let manager = RecordingManager::new();

        // 1 MB per row group × 10 row groups × 6 services = 60 MB total.
        // Shrink bounds to force multiple tasks: lower=12 MB, upper=24 MB.
        let mut shift_config = ShiftConfig::default();
        shift_config.read.lower_bound_input_mb_per_task = 12;
        shift_config.read.upper_bound_input_mb_per_task = 24;
        let runner = PlanTaskRunnerImpl::new(
            runner.queue_reader,
            runner.storage,
            Arc::new(shift_config),
            test_timeouts(),
            "logs",
            &crate::shift::CURRENT_PLANNER_PARTITION_SPEC,
        );

        let mut entries = Vec::new();
        for svc in 0..6 {
            let service = format!("svc-{svc:02}");
            for idx in 0..10 {
                let min_ts: i64 = i64::from(svc) * 1_000 + i64::from(idx) * 10;
                let max_ts: i64 = min_ts + 9;
                entries.push(plan_entry_with_ts(
                    Some("tenant-a"),
                    1,
                    usize::try_from(svc * 10 + idx).expect("idx"),
                    1024 * 1024,
                    Some(timestamp_boundary("acc-1", &service, min_ts, max_ts)),
                    Some((min_ts, max_ts)),
                ));
            }
        }
        let row_groups_total = entries.len();
        let plan = icegate_queue::SegmentsPlan {
            entries,
            last_segment_offset: Some(1),
            segments_count: 1,
            row_groups_total,
            input_bytes_total: row_groups_total as u64 * 1024 * 1024,
        };

        let (task_ids, _) = runner
            .schedule_shift_tasks(&manager, plan, &CancellationToken::new())
            .expect("plan must schedule shift tasks");

        assert!(task_ids.len() >= 2, "workload must split into multiple shift tasks");

        // All output chunks must be pairwise disjoint:
        // for any pair `i < j`, `bounds[i].max < bounds[j].min` under the
        // composite sort key.
        let bounds = shift_task_bounds(&manager);
        for i in 0..bounds.len() {
            for j in (i + 1)..bounds.len() {
                let (_, ref a_max) = bounds[i];
                let (ref b_min, _) = bounds[j];
                let order = a_max.compare(b_min);
                assert!(
                    order == std::cmp::Ordering::Less,
                    "shift task #{i} max={a_max:?} must be strictly less than task #{j} min={b_min:?}"
                );
            }
        }
    }
}
