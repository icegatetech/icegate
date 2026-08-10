use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use icegate_queue::{QueueReader, SegmentsPlan, Topic};
use jobmanager::{Error, TaskCode, TaskContext, TaskDefinition, TaskRef};
use tokio_util::sync::CancellationToken;
use tracing::info;

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

/// What planning a WAL plan produced.
///
/// Cancellation is a variant rather than an error because a shutdown observed while planning is
/// not a failure of the plan task: nothing was created, and the task is executed again later.
#[derive(Debug)]
pub(super) enum ShiftTaskPlan {
    /// The shift tasks to create, with the planner summary for telemetry.
    Planned {
        /// Shift task definitions in planning order; never empty.
        definitions: Vec<TaskDefinition>,
        /// Planner summary for telemetry emission in the instrumentation layer.
        summary: PlanSummary,
    },
    /// A shutdown was observed while planning; no definition was produced.
    Cancelled,
}

/// What reading the WAL plan produced.
///
/// Cancellation is a variant for the same reason as in [`ShiftTaskPlan`]: a shutdown observed
/// while reading is not a failure of the plan task, and the task is executed again later.
#[derive(Debug)]
enum QueuePlan {
    /// The offset planning starts from, and the WAL plan read above it.
    Read {
        /// First WAL offset not yet committed to Iceberg.
        start_offset: u64,
        /// Segments the queue offers from `start_offset` on.
        plan: SegmentsPlan,
    },
    /// A shutdown was observed while reading; nothing was planned.
    Cancelled {
        /// Offset planning would have started from, or `None` when the shutdown arrived before
        /// the committed offset was read.
        start_offset: Option<u64>,
    },
}

/// Result of executing a plan task.
pub struct PlanTaskResult {
    /// Task execution status.
    pub status: TaskStatus,
    /// Offset planning started from, or `None` when the committed offset was never read because
    /// a shutdown stopped the task first.
    pub start_offset: Option<u64>,
    /// Last segment offset observed in the plan.
    pub last_offset: Option<u64>,
    /// Number of segments scanned.
    pub segments_count: usize,
    /// Total record batches across all segments.
    pub record_batches_total: usize,
    /// Number of shift tasks scheduled by this plan.
    pub shift_tasks_scheduled: usize,
    /// Planner summary for telemetry emission in the instrumentation layer.
    pub(super) plan_summary: Option<PlanSummary>,
}

/// Runner interface for plan tasks.
#[async_trait]
pub trait PlanTaskRunner: Send + Sync {
    /// Execute a plan task.
    async fn run(&self, ctx: &TaskContext) -> Result<PlanTaskResult, Error>;
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

    /// Read the committed offset and the WAL plan above it.
    ///
    /// The token state, not the error type, is what classifies a shutdown here - the same rule
    /// `shift_runner::bridge_merge_error` applies on the shift path. A shutdown that catches
    /// either call in flight arrives typed (`IngestError::Cancelled` / `QueueError::Cancelled`)
    /// only when the retrier's own token branch won the race against the interrupted operation;
    /// the operation's own error reaches the caller unchanged otherwise, and every mapping between
    /// the two dependencies and `jobmanager::Error` is free to drop the variant. Classifying by
    /// token keeps a shutdown from being counted as a queue-read failure of the plan task.
    async fn read_queue_plan(&self, cancel_token: &CancellationToken) -> Result<QueuePlan, Error> {
        if cancel_token.is_cancelled() {
            return Ok(QueuePlan::Cancelled { start_offset: None });
        }

        let committed_offset = match self.storage.get_last_offset(cancel_token).await {
            Ok(committed_offset) => committed_offset,
            Err(_) if cancel_token.is_cancelled() => return Ok(QueuePlan::Cancelled { start_offset: None }),
            Err(err) => return Err(Error::Other(format!("failed to read committed offset: {err}"))),
        };
        // Mirror the query side (`extract_wal_offset` caller) exactly: a
        // saturating step keeps the WAL/Iceberg boundary identical on both
        // sides and avoids wrapping a near-`u64::MAX` offset back to 0, which
        // would re-shift the whole queue.
        let start_offset = committed_offset.map_or(0, |offset| offset.saturating_add(1));

        info!("plan: topic '{}' starting from offset {}", self.topic, start_offset);

        match self
            .queue_reader
            .plan_segments(&self.topic, start_offset, &self.extract_fields, cancel_token)
            .await
        {
            Ok(plan) => Ok(QueuePlan::Read { start_offset, plan }),
            Err(_) if cancel_token.is_cancelled() => Ok(QueuePlan::Cancelled {
                start_offset: Some(start_offset),
            }),
            Err(err) => Err(Error::Other(format!("failed to plan segment record batches: {err}"))),
        }
    }

    /// Turn a WAL plan into the shift tasks to create, without creating any of them.
    ///
    /// Building every definition before the first task is created is what keeps a planning error
    /// from leaving a half-created fan-out behind.
    fn plan_shift_tasks(&self, plan: SegmentsPlan, cancel_token: &CancellationToken) -> Result<ShiftTaskPlan, Error> {
        if cancel_token.is_cancelled() {
            return Ok(ShiftTaskPlan::Cancelled);
        }

        let row_groups = self
            .partition_spec
            .plan_entries_to_row_groups(plan.entries)
            .map_err(|e| Error::Other(e.to_string()))?;
        let limits = PlannerConfig::from_bounds(
            self.shift_config.read.lower_bound_input_mb_per_task * 1024 * 1024,
            self.shift_config.read.upper_bound_input_mb_per_task * 1024 * 1024,
            self.shift_config.read.max_record_batches_per_task,
        );

        let n_row_groups = row_groups.len();
        let (chunks, stats) = match plan_row_groups(row_groups, &limits, cancel_token) {
            Ok(planned) => planned,
            // The planner checks the token between its own phases and reports a shutdown as an
            // ordinary error, so the token is what separates the two cases here.
            Err(_) if cancel_token.is_cancelled() => return Ok(ShiftTaskPlan::Cancelled),
            Err(err) => return Err(Error::Other(err.to_string())),
        };
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
        let mut definitions = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            if cancel_token.is_cancelled() {
                return Ok(ShiftTaskPlan::Cancelled);
            }
            definitions.push(self.define_shift_task_for_chunk(chunk)?);
        }

        if definitions.is_empty() {
            return Err(Error::Other("no row groups to schedule".to_string()));
        }

        Ok(ShiftTaskPlan::Planned { definitions, summary })
    }

    /// Convert one planner chunk into a `ShiftInput` and describe it as a shift
    /// task. Row groups inside the chunk are bucketed by `wal_offset` so the
    /// shift runner can read each segment exactly once; segment iteration order
    /// inside the resulting `ShiftInput` is non-deterministic.
    fn define_shift_task_for_chunk(&self, chunk: PlannedChunk) -> Result<TaskDefinition, Error> {
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
        self.define_shift_task(segments, segments_count, total_rg)
    }

    fn define_shift_task(
        &self,
        segments: Vec<SegmentToRead>,
        segments_count: usize,
        record_batches_total: usize,
    ) -> Result<TaskDefinition, Error> {
        let trace_context = icegate_common::extract_current_trace_context();

        let shift_input = ShiftInput {
            segments,
            trace_context,
        };

        let shift_timeout = self
            .timeouts
            .shift_timeout(segments_count, record_batches_total)
            .map_err(|e| Error::Other(e.to_string()))?;
        // TODO(med): check task max attempts constant (default 5)
        Ok(
            TaskDefinition::new(TaskCode::new(SHIFT_TASK_CODE), shift_timeout).with_input(
                serde_json::to_vec(&shift_input)
                    .map_err(|e| Error::Other(format!("failed to serialize shift input: {e}")))?,
            ),
        )
    }
}

#[async_trait]
impl<Q, S> PlanTaskRunner for PlanTaskRunnerImpl<Q, S>
where
    Q: QueueReader + 'static,
    S: Storage + 'static,
{
    #[tracing::instrument(name="plan_run", skip(self, ctx), fields(task_id = %ctx.id()))]
    async fn run(&self, ctx: &TaskContext) -> Result<PlanTaskResult, Error> {
        // NOTICE: There is no guarantee of order inside the segments (WAL files), we just accept data from clients. Most likely, clients sort data by time (but this is not accurate).
        let cancel_token = ctx.cancel_token();

        let (start_offset, plan) = match self.read_queue_plan(cancel_token).await? {
            QueuePlan::Read { start_offset, plan } => (start_offset, plan),
            // No plan was read, so the counters have nothing to report; the task stays open and
            // is planned again after the restart. The offset is reported as far as it got: it is
            // unknown when the shutdown preceded the committed-offset read.
            QueuePlan::Cancelled { start_offset } => {
                return Ok(PlanTaskResult {
                    status: TaskStatus::Cancelled,
                    start_offset,
                    last_offset: None,
                    segments_count: 0,
                    record_batches_total: 0,
                    shift_tasks_scheduled: 0,
                    plan_summary: None,
                });
            }
        };

        if plan.last_segment_offset.is_none() {
            // TODO(high): now we are completing the job iteration and producing a lot of files, it may be worth restarting the task.
            info!("plan: no segments found for topic '{}'", self.topic);
            return Ok(PlanTaskResult {
                status: TaskStatus::Empty,
                start_offset: Some(start_offset),
                last_offset: None,
                segments_count: plan.segments_count,
                record_batches_total: plan.row_groups_total,
                shift_tasks_scheduled: 0,
                plan_summary: None,
            });
        }

        let segments_count = plan.segments_count;
        let record_batches_total = plan.row_groups_total;
        let last_offset = plan.last_segment_offset;
        let (shift_definitions, plan_summary) = match self.plan_shift_tasks(plan, cancel_token)? {
            ShiftTaskPlan::Planned { definitions, summary } => (definitions, summary),
            // The task is left open and planned again later, so a shutdown is reported as a
            // cancellation rather than as a plan failure the on-call would have to explain.
            ShiftTaskPlan::Cancelled => {
                return Ok(PlanTaskResult {
                    status: TaskStatus::Cancelled,
                    start_offset: Some(start_offset),
                    last_offset,
                    segments_count,
                    record_batches_total,
                    shift_tasks_scheduled: 0,
                    plan_summary: None,
                });
            }
        };

        let shift_tasks_scheduled = shift_definitions.len();
        let last_offset_value = last_offset.unwrap_or(0);
        let commit_input = super::CommitInput {
            last_offset: last_offset_value,
            trace_context: icegate_common::extract_current_trace_context(),
        };
        let commit_timeout = self
            .timeouts
            .commit_timeout(shift_tasks_scheduled)
            .map_err(|e| Error::Other(e.to_string()))?;
        // TODO(med): check task max attempts constant (default 5)
        let commit_definition = TaskDefinition::new(TaskCode::new(super::COMMIT_TASK_CODE), commit_timeout).with_input(
            serde_json::to_vec(&commit_input)
                .map_err(|e| Error::Other(format!("failed to serialize commit input: {e}")))?,
        );

        // TODO(high): Add a transactional runtime-task batch API to jobmanager and use it here.
        // It must validate and persist the shift tasks plus the commit task atomically, so an
        // add_task failure cannot leave a partial fan-out in the job state.
        let mut shift_tasks: Vec<TaskRef> = Vec::with_capacity(shift_tasks_scheduled);
        for definition in shift_definitions {
            shift_tasks.push(ctx.job().add_task(definition)?);
        }

        info!(
            "plan: scheduling shift for {} tasks (last_offset={})",
            shift_tasks_scheduled, last_offset_value
        );

        let commit_task = commit_definition.with_dependencies(shift_tasks);
        ctx.job().add_task(commit_task)?;

        Ok(PlanTaskResult {
            status: TaskStatus::Ok,
            start_offset: Some(start_offset),
            last_offset,
            segments_count,
            record_batches_total,
            shift_tasks_scheduled,
            plan_summary: Some(plan_summary),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use iceberg::spec::DataFile;
    use icegate_queue::ExtractedValue;
    use jobmanager::TaskDefinition;
    use tokio_util::sync::CancellationToken;

    use super::{
        PLAN_FIELD_BOUNDARY_RANGE, PLAN_FIELD_TENANT_ID, PLAN_FIELD_TIMESTAMP_RANGE, PlanSummary, PlanTaskRunnerImpl,
        QueuePlan, ShiftTaskPlan,
    };
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

    /// Input payload of every planned shift task, in planning order.
    fn shift_task_payloads(definitions: &[TaskDefinition]) -> Vec<Vec<u8>> {
        definitions.iter().map(|definition| definition.input().to_vec()).collect()
    }

    /// Unwrap a plan the runner is expected to have produced, failing the test on a reported
    /// shutdown so that a cancellation never masquerades as an empty plan.
    fn planned_shift_tasks(plan: ShiftTaskPlan) -> (Vec<TaskDefinition>, PlanSummary) {
        match plan {
            ShiftTaskPlan::Planned { definitions, summary } => (definitions, summary),
            ShiftTaskPlan::Cancelled => panic!("planning must not report a shutdown"),
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

    /// How a dependency double answers the one call the test drives it through.
    #[derive(Copy, Clone)]
    enum DependencyBehavior {
        /// Answers with the given committed offset and an empty plan.
        Answer(Option<u64>),
        /// Fails the call while the token stays untouched.
        Fail,
        /// Cancels the token it was given and then fails, which is what a queue or storage call
        /// caught by a shutdown does: the cancellation reaches the caller as an ordinary error.
        ShutdownThenFail,
    }

    /// Queue reader and storage double for the paths `read_queue_plan` classifies.
    struct FakeDependency(DependencyBehavior);

    impl FakeDependency {
        /// Apply the behaviour to `cancel_token` and report whether the call must fail.
        fn apply_behavior(&self, cancel_token: &CancellationToken) -> bool {
            match self.0 {
                DependencyBehavior::Answer(_) => false,
                DependencyBehavior::Fail => true,
                DependencyBehavior::ShutdownThenFail => {
                    cancel_token.cancel();
                    true
                }
            }
        }

        /// Committed offset the storage half answers a successful call with.
        const fn committed_offset(&self) -> Option<u64> {
            match self.0 {
                DependencyBehavior::Answer(committed_offset) => committed_offset,
                DependencyBehavior::Fail | DependencyBehavior::ShutdownThenFail => None,
            }
        }
    }

    #[async_trait]
    impl Storage for FakeDependency {
        async fn get_last_offset(&self, cancel_token: &CancellationToken) -> IngestResult<Option<u64>> {
            if self.apply_behavior(cancel_token) {
                return Err(crate::error::IngestError::Shift(
                    "committed offset read failed".to_string(),
                ));
            }
            Ok(self.committed_offset())
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
    impl icegate_queue::QueueReader for FakeDependency {
        async fn plan_segments(
            &self,
            _topic: &icegate_queue::Topic,
            _start_offset: u64,
            _fields: &[icegate_queue::ExtractField],
            cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::SegmentsPlan> {
            if self.apply_behavior(cancel_token) {
                return Err(icegate_queue::QueueError::Io(std::io::Error::other(
                    "segment plan read failed",
                )));
            }
            Ok(icegate_queue::SegmentsPlan {
                entries: Vec::new(),
                last_segment_offset: None,
                segments_count: 0,
                row_groups_total: 0,
                input_bytes_total: 0,
            })
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
    }

    fn test_runner<Q, S>(queue_reader: Q, storage: S) -> PlanTaskRunnerImpl<Q, S>
    where
        Q: icegate_queue::QueueReader + 'static,
        S: Storage + 'static,
    {
        PlanTaskRunnerImpl::new(
            Arc::new(queue_reader),
            Arc::new(storage),
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

    /// Build a single-component sort-key boundary `(service, ts DESC)`
    /// where `ts` carries the same physical value in `min_key` and `max_key`.
    fn point_boundary_payload(service: &str, ts: i64) -> String {
        let key = || {
            crate::wal::RowGroupBoundaryKey::new(vec![
                crate::wal::test_utils::boundary_component_string(Some(service.to_string()), false, true),
                crate::wal::test_utils::boundary_component_timestamp_micros(Some(ts), true, true),
            ])
        };
        serialize_row_group_boundary_range(&crate::wal::RowGroupBoundaryRange {
            names: Arc::from(["service_name".to_string(), "timestamp".to_string()]),
            min_key: key(),
            max_key: key(),
        })
        .expect("serialize metadata")
    }

    /// A shutdown is not a planning failure: reporting it as one would have the plan task counted
    /// as a queue-read error and its metrics label say so.
    #[test]
    fn plan_shift_tasks_reports_a_shutdown_instead_of_failing() {
        let runner = test_runner(FakeQueueReader, FakeStorage);
        let plan = single_row_group_plan();
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();

        let planned = runner
            .plan_shift_tasks(plan, &cancel_token)
            .expect("a cancelled token must not fail planning");

        assert!(matches!(planned, ShiftTaskPlan::Cancelled));
    }

    /// A token already cancelled when the plan task starts must not reach storage at all: both
    /// doubles panic on any call, so this also pins the early exit to the step it belongs to.
    #[tokio::test]
    async fn read_queue_plan_reports_a_shutdown_observed_before_the_first_read() {
        let runner = test_runner(FakeQueueReader, FakeStorage);
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();

        let queue_plan = runner
            .read_queue_plan(&cancel_token)
            .await
            .expect("a cancelled token must not fail the plan task");

        assert!(matches!(queue_plan, QueuePlan::Cancelled { start_offset: None }));
    }

    /// A shutdown that catches the committed-offset read in flight comes back from storage as an
    /// ordinary error. Passing it on as a plan failure is what made every restart raise the
    /// `queue_plan` failure counter of the plan task. No offset was read, so none is reported.
    #[tokio::test]
    async fn read_queue_plan_reports_a_shutdown_that_interrupted_the_committed_offset_read() {
        let runner = test_runner(FakeQueueReader, FakeDependency(DependencyBehavior::ShutdownThenFail));

        let queue_plan = runner
            .read_queue_plan(&CancellationToken::new())
            .await
            .expect("an interrupted committed-offset read must not fail the plan task");

        assert!(matches!(queue_plan, QueuePlan::Cancelled { start_offset: None }));
    }

    /// Same for the queue: the plan read is where a shutdown spends most of its chances of
    /// arriving mid-call, and the queue reports it as an error just like storage does. Here the
    /// committed offset was already read, so the offset planning would have started from is known
    /// and travels with the cancellation instead of being replaced by a made-up number.
    #[tokio::test]
    async fn read_queue_plan_reports_a_shutdown_that_interrupted_the_segment_plan_read() {
        let runner = test_runner(
            FakeDependency(DependencyBehavior::ShutdownThenFail),
            FakeDependency(DependencyBehavior::Answer(None)),
        );

        let queue_plan = runner
            .read_queue_plan(&CancellationToken::new())
            .await
            .expect("an interrupted segment plan read must not fail the plan task");

        assert!(matches!(queue_plan, QueuePlan::Cancelled { start_offset: Some(0) }));
    }

    /// The other half of the classification: with the token untouched the error is a real failure
    /// and must stay one, or a broken queue would be silently reported as a shutdown forever.
    #[tokio::test]
    async fn read_queue_plan_fails_on_an_error_without_a_shutdown() {
        let runner = test_runner(
            FakeDependency(DependencyBehavior::Fail),
            FakeDependency(DependencyBehavior::Answer(None)),
        );

        let err = runner
            .read_queue_plan(&CancellationToken::new())
            .await
            .expect_err("a queue failure outside a shutdown must fail the plan task");

        assert!(err.to_string().contains("failed to plan segment record batches"));
    }

    /// The storage half of the same rule: a committed-offset read that fails while no shutdown is
    /// in progress is a real failure, and the plan task must report it as one.
    #[tokio::test]
    async fn read_queue_plan_fails_on_a_committed_offset_read_error() {
        let runner = test_runner(FakeQueueReader, FakeDependency(DependencyBehavior::Fail));

        let err = runner
            .read_queue_plan(&CancellationToken::new())
            .await
            .expect_err("a storage failure outside a shutdown must fail the plan task");

        assert!(err.to_string().contains("failed to read committed offset"));
    }

    /// Planning resumes at the first offset the committed snapshot does not cover. The query side
    /// derives the same boundary from that snapshot, so an off-by-one here either re-shifts a
    /// segment that is already in the table or leaves one behind forever.
    #[tokio::test]
    async fn read_queue_plan_starts_from_the_offset_after_the_committed_one() {
        let runner = test_runner(
            FakeDependency(DependencyBehavior::Answer(None)),
            FakeDependency(DependencyBehavior::Answer(Some(41))),
        );

        let queue_plan = runner
            .read_queue_plan(&CancellationToken::new())
            .await
            .expect("a plan read above the committed offset must succeed");

        assert!(matches!(queue_plan, QueuePlan::Read { start_offset: 42, .. }));
    }

    /// The one case the step above must not wrap: at the highest WAL offset the next start offset
    /// stays where it is, because wrapping to zero would re-shift the entire queue.
    #[tokio::test]
    async fn read_queue_plan_saturates_the_start_offset_at_the_maximum_wal_offset() {
        let runner = test_runner(
            FakeDependency(DependencyBehavior::Answer(None)),
            FakeDependency(DependencyBehavior::Answer(Some(u64::MAX))),
        );

        let queue_plan = runner
            .read_queue_plan(&CancellationToken::new())
            .await
            .expect("a plan read at the maximum committed offset must succeed");

        assert!(matches!(
            queue_plan,
            QueuePlan::Read {
                start_offset: u64::MAX,
                ..
            }
        ));
    }

    /// A segment the queue listed but holds no row group of leaves the plan with a last segment
    /// offset and nothing to schedule (`ParquetQueueReader::plan_segments` reports exactly that).
    /// Failing the plan task is the only correct answer: scheduling the commit task alone would
    /// move the WAL offset past a segment no shift task ever read.
    #[test]
    fn plan_shift_tasks_fails_when_the_plan_holds_no_row_groups() {
        let runner = test_runner(FakeQueueReader, FakeStorage);
        let plan = icegate_queue::SegmentsPlan {
            entries: Vec::new(),
            last_segment_offset: Some(7),
            segments_count: 1,
            row_groups_total: 0,
            input_bytes_total: 0,
        };

        runner
            .plan_shift_tasks(plan, &CancellationToken::new())
            .expect_err("a plan holding no row group must fail the plan task");
    }

    /// An empty `tenant_id` extracted from a WAL row group must fail the plan
    /// task end-to-end before any shift task is scheduled. The underlying
    /// invariant lives in `plan_entries_to_row_groups`; this guard catches
    /// regressions where the runner could swallow the error or partially
    /// submit work before the validator runs.
    #[test]
    fn plan_shift_tasks_fails_fast_on_empty_tenant_id() {
        let runner = test_runner(FakeQueueReader, FakeStorage);
        // Two entries: first valid, second with empty tenant_id. The plan must
        // surface the error and define zero shift tasks even though the first
        // row group on its own would have been schedulable.
        let payload = point_boundary_payload("svc-1", 10);
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
            .plan_shift_tasks(plan, &CancellationToken::new())
            .expect_err("empty tenant_id must abort plan task");

        assert!(err.to_string().contains("tenant_id"));
    }

    #[test]
    fn plan_shift_tasks_rejects_placeholder_boundary_metadata() {
        let runner = test_runner(FakeQueueReader, FakeStorage);
        let mut plan = single_row_group_plan();
        set_boundary(&mut plan.entries[0], "{}".to_string());

        let err = runner
            .plan_shift_tasks(plan, &CancellationToken::new())
            .expect_err("placeholder metadata must be rejected");

        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn plan_shift_tasks_accepts_boundary_metadata_serialized_by_wal_sort() {
        let runner = test_runner(FakeQueueReader, FakeStorage);
        let mut plan = single_row_group_plan();
        set_boundary(&mut plan.entries[0], point_boundary_payload("svc-2", 30));

        let (shift_tasks, _) = planned_shift_tasks(
            runner
                .plan_shift_tasks(plan, &CancellationToken::new())
                .expect("serialized metadata must be accepted"),
        );

        assert_eq!(shift_tasks.len(), 1);
    }

    #[test]
    fn plan_shift_tasks_handles_mixed_partition_buckets_in_single_pass() {
        let runner = test_runner(FakeQueueReader, FakeStorage);
        let payload = point_boundary_payload("svc-1", 10);
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

        let (shift_tasks, summary) = planned_shift_tasks(
            runner
                .plan_shift_tasks(plan, &CancellationToken::new())
                .expect("mixed partition-bucket plan must schedule successfully"),
        );

        assert_eq!(shift_tasks.len(), 2);
        assert_eq!(summary.n_row_groups, 2);
        assert_eq!(summary.n_chunks, 2);
    }

    /// Build a `(service, ts DESC)` boundary range with single-value service
    /// slot, used to simulate a sorted WAL row group inside a single service.
    fn timestamp_boundary(service: &str, min_ts: i64, max_ts: i64) -> String {
        // Sort key: service ASC, ts DESC (per logs schema).
        // Under DESC ordering, the row's MIN value sits at max_ts and the MAX
        // value at min_ts.
        serialize_row_group_boundary_range(&crate::wal::RowGroupBoundaryRange {
            names: Arc::from(["service_name".to_string(), "timestamp".to_string()]),
            min_key: crate::wal::RowGroupBoundaryKey::new(vec![
                crate::wal::test_utils::boundary_component_string(Some(service.to_string()), false, true),
                crate::wal::test_utils::boundary_component_timestamp_micros(Some(max_ts), true, true),
            ]),
            max_key: crate::wal::RowGroupBoundaryKey::new(vec![
                crate::wal::test_utils::boundary_component_string(Some(service.to_string()), false, true),
                crate::wal::test_utils::boundary_component_timestamp_micros(Some(min_ts), true, true),
            ]),
        })
        .expect("serialize metadata")
    }

    /// Reconstruct the `(min_key, max_key)` of every shift task by inspecting
    /// the `ShiftInput` payload each planned task carries.
    fn shift_task_bounds(
        definitions: &[TaskDefinition],
    ) -> Vec<(crate::wal::RowGroupBoundaryKey, crate::wal::RowGroupBoundaryKey)> {
        shift_task_payloads(definitions)
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
    #[test]
    fn plan_shift_tasks_emits_pairwise_disjoint_chunks_for_six_service_workload() {
        let runner = test_runner(FakeQueueReader, FakeStorage);

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
                    Some(timestamp_boundary(&service, min_ts, max_ts)),
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

        let (shift_tasks, _) = planned_shift_tasks(
            runner
                .plan_shift_tasks(plan, &CancellationToken::new())
                .expect("plan must schedule shift tasks"),
        );

        assert!(shift_tasks.len() >= 2, "workload must split into multiple shift tasks");

        // All output chunks must be pairwise disjoint:
        // for any pair `i < j`, `bounds[i].max < bounds[j].min` under the
        // composite sort key.
        let bounds = shift_task_bounds(&shift_tasks);
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
