//! Task executors for shift operations.
//!
//! Implements the plan -> shift -> commit pipeline for WAL segments processing.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use iceberg::spec::DataFile;
use icegate_jobmanager::{ImmutableTask, JobManager, TaskCode, TaskDefinition, registry::TaskExecutorFn};
use icegate_queue::{QueueReader, SegmentsPlan, Topic};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use super::{
    config::ShiftConfig,
    iceberg_storage::{IcebergStorage, WrittenDataFiles},
    timeout::TimeoutEstimator,
};

/// Task code for plan segments.
pub const PLAN_TASK_CODE: &str = "plan";
/// Task code for shifting segments into Iceberg.
pub const SHIFT_TASK_CODE: &str = "shift";
/// Task code for committing shifted data to Iceberg.
pub const COMMIT_TASK_CODE: &str = "commit";

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
}

/// Output of the shift task.
#[derive(Debug, Serialize, Deserialize)]
pub struct ShiftOutput {
    /// Parquet files produced by the shift task.
    pub parquet_files: Vec<String>,
}

/// Input for the commit task.
#[derive(Debug, Serialize, Deserialize)]
pub struct CommitInput {
    /// Highest segments offset to commit in snapshot summary.
    pub last_offset: u64,
}

/// Queue reader dependency surface for shift executors.
#[async_trait]
pub trait QueueReaderApi: Send + Sync {
    /// Build a plan of record batches to process.
    async fn plan_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        group_by_column_name: &str,
        max_record_batches_per_task: usize,
        cancel_token: &CancellationToken,
    ) -> icegate_queue::Result<SegmentsPlan>;

    /// Read record batches for a specific segment.
    async fn read_segment(
        &self,
        topic: &Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        cancel_token: &CancellationToken,
    ) -> icegate_queue::Result<Vec<RecordBatch>>;
}

/// Iceberg storage dependency surface for shift executors.
#[async_trait]
pub trait IcebergStorageApi: Send + Sync {
    /// Fetch the last committed offset, if any.
    async fn get_committed_offset(&self, cancel_token: &CancellationToken) -> crate::error::Result<Option<u64>>;

    /// Write parquet files from record batches without committing.
    async fn write_parquet_files(
        &self,
        batches: Vec<RecordBatch>,
        cancel_token: &CancellationToken,
    ) -> crate::error::Result<WrittenDataFiles>;

    /// Build Iceberg data files from parquet paths.
    async fn data_files_from_parquet_paths(
        &self,
        parquet_paths: &[String],
        cancel_token: &CancellationToken,
    ) -> crate::error::Result<Vec<DataFile>>;

    /// Commit data files to Iceberg with the provided offset.
    async fn commit_data_files(
        &self,
        data_files: Vec<DataFile>,
        record_type: &str,
        last_offset: u64,
        cancel_token: &CancellationToken,
    ) -> crate::error::Result<usize>;
}

#[async_trait]
impl QueueReaderApi for QueueReader {
    async fn plan_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        group_by_column_name: &str,
        max_record_batches_per_task: usize,
        cancel_token: &CancellationToken,
    ) -> icegate_queue::Result<SegmentsPlan> {
        self.plan_segments(
            topic,
            start_offset,
            group_by_column_name,
            max_record_batches_per_task,
            cancel_token,
        )
        .await
    }

    async fn read_segment(
        &self,
        topic: &Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        cancel_token: &CancellationToken,
    ) -> icegate_queue::Result<Vec<RecordBatch>> {
        self.read_segment(topic, offset, record_batch_idxs, cancel_token).await
    }
}

#[async_trait]
impl IcebergStorageApi for IcebergStorage {
    async fn get_committed_offset(&self, cancel_token: &CancellationToken) -> crate::error::Result<Option<u64>> {
        self.get_last_offset(cancel_token).await
    }

    async fn write_parquet_files(
        &self,
        batches: Vec<RecordBatch>,
        cancel_token: &CancellationToken,
    ) -> crate::error::Result<WrittenDataFiles> {
        self.write_record_batches(batches, cancel_token).await
    }

    async fn data_files_from_parquet_paths(
        &self,
        parquet_paths: &[String],
        cancel_token: &CancellationToken,
    ) -> crate::error::Result<Vec<DataFile>> {
        self.get_data_files(parquet_paths, cancel_token).await
    }

    async fn commit_data_files(
        &self,
        data_files: Vec<DataFile>,
        record_type: &str,
        last_offset: u64,
        cancel_token: &CancellationToken,
    ) -> crate::error::Result<usize> {
        self.commit(data_files, record_type, last_offset, cancel_token)
            .await
    }
}

/// Shared executor dependencies for shift tasks.
pub struct Executor {
    queue_reader: Arc<dyn QueueReaderApi>,
    storage: Arc<dyn IcebergStorageApi>,
    shift_config: Arc<ShiftConfig>,
    topic: Topic,
    timeouts: TimeoutEstimator,
}

impl Executor {
    /// Creates a new executor and initializes shared dependencies.
    pub fn new(
        queue_reader: Arc<dyn QueueReaderApi>,
        shift_config: Arc<ShiftConfig>,
        storage: Arc<dyn IcebergStorageApi>,
        timeouts: TimeoutEstimator,
        topic: impl Into<String>,
    ) -> Self {
        Self {
            queue_reader,
            storage,
            shift_config,
            topic: topic.into(),
            timeouts,
        }
    }
    /// Creates executor for the plan task.
    pub fn plan_executor(self: Arc<Self>) -> TaskExecutorFn {
        // NOTICE: There is no guarantee of order inside the segments (WAL files), we just accept data from clients. Most likely, clients sort data by time (but this is not accurate).

        Arc::new(
            move |task: Arc<dyn ImmutableTask>, manager: &dyn JobManager, cancel_token| {
                let executor = Arc::clone(&self);
                let task_id = *task.id();

                let fut = async move {
                    if cancel_token.is_cancelled() {
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let topic = executor.topic.clone();

                    let start_offset = executor
                        .storage
                        .get_committed_offset(&cancel_token)
                        .await
                        .map_err(|e| {
                            icegate_jobmanager::Error::TaskExecution(format!("failed to read committed offset: {e}"))
                        })?
                        .map_or(0, |offset| offset + 1);

                    tracing::info!("plan: topic '{}' starting from offset {}", topic, start_offset);

                    let plan = executor
                        .queue_reader
                        .plan_segments(
                            &topic,
                            start_offset,
                            "tenant_id",
                            executor.shift_config.read.max_record_batches_per_task,
                            &cancel_token,
                        )
                        .await
                        .map_err(|e| {
                            icegate_jobmanager::Error::TaskExecution(format!(
                                "failed to plan segment record batches: {e}"
                            ))
                        })?;

                    if plan.last_segment_offset.is_none() {
                        // TODO(high): now we are completing the job iteration and producing a lot of files, it may be worth restarting the task.
                        tracing::info!("plan: no segments found for topic '{}'", topic);
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let last_offset = plan.last_segment_offset.unwrap_or(0);
                    let shift_task_ids = Self::schedule_shift_tasks(manager, plan, &executor.timeouts)?;

                    tracing::info!(
                        "plan: scheduling shift for {} tasks (last_offset={})",
                        shift_task_ids.len(),
                        last_offset
                    );

                    let commit_input = CommitInput { last_offset };
                    let commit_timeout = executor
                        .timeouts
                        .commit_timeout(shift_task_ids.len())
                        .map_err(|e| icegate_jobmanager::Error::TaskExecution(e.to_string()))?;
                    let commit_task = TaskDefinition::new(
                        TaskCode::new(COMMIT_TASK_CODE),
                        serde_json::to_vec(&commit_input).map_err(|e| {
                            icegate_jobmanager::Error::TaskExecution(format!("failed to serialize commit input: {e}"))
                        })?,
                        commit_timeout,
                    )?
                    .with_dependencies(shift_task_ids);

                    manager.add_task(commit_task)?;
                    manager.complete_task(&task_id, Vec::new())
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
                let task_id = *task.id();

                let fut = async move {
                    if cancel_token.is_cancelled() {
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let input: ShiftInput = parse_task_input(task.as_ref())?;
                    if input.segments.is_empty() {
                        tracing::error!("shift: no segments provided, skipping");
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let mut batches: Vec<RecordBatch> = Vec::new();

                    for segment in &input.segments {
                        let segment_batches = executor
                            .queue_reader
                            .read_segment(
                                &executor.topic,
                                segment.segment_offset,
                                &segment.record_batch_idxs,
                                &cancel_token,
                            )
                            .await
                            .map_err(|e| {
                                icegate_jobmanager::Error::TaskExecution(format!(
                                    "failed to read segment {} row groups: {e}",
                                    segment.segment_offset
                                ))
                            })?;
                        batches.extend(segment_batches);
                    }

                    if batches.is_empty() {
                        return Err(icegate_jobmanager::Error::TaskExecution(
                            "shift produced no record batches to write".to_string(),
                        ));
                    }

                    let write_result = executor
                        .storage
                        .write_parquet_files(batches, &cancel_token)
                        .await
                        .map_err(|e| icegate_jobmanager::Error::TaskExecution(e.to_string()))?;

                    if write_result.data_files.is_empty() {
                        return Err(icegate_jobmanager::Error::TaskExecution(
                            "shift produced no parquet files to commit".to_string(),
                        ));
                    }

                    // TODO(low): remove prefix from parquet files (to avoid saving unnecessary date in the job). The prefix is in storage.
                    let parquet_files = write_result
                        .data_files
                        .iter()
                        .map(|data_file| data_file.file_path().to_string())
                        .collect::<Vec<_>>();

                    let output = ShiftOutput { parquet_files };
                    let output_payload = serde_json::to_vec(&output).map_err(|e| {
                        icegate_jobmanager::Error::TaskExecution(format!("failed to serialize shift output: {e}"))
                    })?;
                    manager.complete_task(&task_id, output_payload)
                    // TODO(med): If shift task failed, the old files physically remain in the object storage. We get garbage/leaked files.
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
                let task_id = *task.id();

                let fut = async move {
                    if cancel_token.is_cancelled() {
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let input: CommitInput = parse_task_input(task.as_ref())?;
                    // Let's check if we have already recorded a commit, but did not have time to complete the job.
                    let committed_offset = executor.storage.get_committed_offset(&cancel_token).await.map_err(|e| {
                        icegate_jobmanager::Error::TaskExecution(format!("failed to read committed offset: {e}"))
                    })?;
                    if committed_offset.is_some_and(|offset| offset >= input.last_offset) {
                        tracing::info!(
                            "commit: offset {} already committed (last_offset={})",
                            committed_offset.unwrap_or(0),
                            input.last_offset
                        );
                        return manager.complete_task(&task_id, Vec::new());
                    }
                    if task.depends_on().is_empty() {
                        return Err(icegate_jobmanager::Error::TaskExecution(
                            "commit task has no dependencies".to_string(),
                        ));
                    }

                    let mut parquet_files = Vec::new();
                    for dep_task_id in task.depends_on() {
                        let dep_task = manager.get_task(dep_task_id)?;
                        if dep_task.get_output().is_empty() {
                            return Err(icegate_jobmanager::Error::TaskExecution(format!(
                                "shift task '{dep_task_id}' produced empty output"
                            )));
                        }

                        let output: ShiftOutput = serde_json::from_slice(dep_task.get_output()).map_err(|e| {
                            icegate_jobmanager::Error::TaskExecution(format!(
                                "failed to parse shift output for '{dep_task_id}': {e}"
                            ))
                        })?;
                        parquet_files.extend(output.parquet_files);
                    }

                    if parquet_files.is_empty() {
                        return Err(icegate_jobmanager::Error::TaskExecution(
                            "commit received no parquet files from shift tasks".to_string(),
                        ));
                    }

                    let data_files = executor
                        .storage
                        .data_files_from_parquet_paths(&parquet_files, &cancel_token)
                        .await
                        .map_err(|e| icegate_jobmanager::Error::TaskExecution(e.to_string()))?;

                    executor
                        .storage
                        .commit_data_files(data_files, &executor.topic, input.last_offset, &cancel_token)
                        .await
                        .map_err(|e| icegate_jobmanager::Error::TaskExecution(e.to_string()))?;

                    manager.complete_task(&task_id, Vec::new())
                };

                Box::pin(fut)
            },
        )
    }

    fn schedule_shift_tasks(
        manager: &dyn JobManager,
        plan: SegmentsPlan,
        timeouts: &TimeoutEstimator,
    ) -> Result<Vec<uuid::Uuid>, icegate_jobmanager::Error> {
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
            return Err(icegate_jobmanager::Error::TaskExecution(
                "no tenant segments to schedule".to_string(),
            ));
        }

        Ok(shift_task_ids)
    }
}

fn parse_task_input<T: for<'de> Deserialize<'de>>(task: &dyn ImmutableTask) -> Result<T, icegate_jobmanager::Error> {
    serde_json::from_slice(task.get_input())
        .map_err(|e| icegate_jobmanager::Error::TaskExecution(format!("failed to parse task input: {e}")))
}

fn create_shift_task(
    manager: &dyn JobManager,
    tenant_id: &str,
    segments: Vec<SegmentToRead>,
    segments_count: usize,
    record_batches_total: usize,
    timeouts: &TimeoutEstimator,
) -> Result<uuid::Uuid, icegate_jobmanager::Error> {
    let shift_input = ShiftInput {
        tenant_id: tenant_id.to_string(),
        segments,
    };

    let shift_timeout = timeouts
        .shift_timeout(segments_count, record_batches_total)
        .map_err(|e| icegate_jobmanager::Error::TaskExecution(e.to_string()))?;
    let shift_task = TaskDefinition::new(
        TaskCode::new(SHIFT_TASK_CODE),
        serde_json::to_vec(&shift_input)
            .map_err(|e| icegate_jobmanager::Error::TaskExecution(format!("failed to serialize shift input: {e}")))?,
        shift_timeout,
    )?;

    manager.add_task(shift_task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use chrono::Duration as ChronoDuration;
    use icegate_queue::{GroupedSegmentsPlan, SegmentRecordBatcheIdxs};
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
    use mockall::mock;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, MutexGuard};
    use uuid::Uuid;

    type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        match mutex.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    #[derive(Clone)]
    struct TestTask {
        id: Uuid,
        code: TaskCode,
        input: Vec<u8>,
        output: Vec<u8>,
        error_msg: String,
        depends_on: Vec<Uuid>,
        completed: bool,
        failed: bool,
        attempts: u32,
        timeout: ChronoDuration,
    }

    impl TestTask {
        fn new(code: TaskCode, input: Vec<u8>) -> Self {
            Self {
                id: Uuid::new_v4(),
                code,
                input,
                output: Vec::new(),
                error_msg: String::new(),
                depends_on: Vec::new(),
                completed: false,
                failed: false,
                attempts: 0,
                timeout: ChronoDuration::seconds(1),
            }
        }

        fn with_dependencies(mut self, depends_on: Vec<Uuid>) -> Self {
            self.depends_on = depends_on;
            self
        }

        fn with_output(mut self, output: Vec<u8>) -> Self {
            self.output = output;
            self
        }
    }

    impl ImmutableTask for TestTask {
        fn id(&self) -> &Uuid {
            &self.id
        }

        fn code(&self) -> &TaskCode {
            &self.code
        }

        fn get_input(&self) -> &[u8] {
            &self.input
        }

        fn get_output(&self) -> &[u8] {
            &self.output
        }

        fn get_error(&self) -> &str {
            &self.error_msg
        }

        fn depends_on(&self) -> &[Uuid] {
            &self.depends_on
        }

        fn is_expired(&self) -> bool {
            false
        }

        fn is_completed(&self) -> bool {
            self.completed
        }

        fn is_failed(&self) -> bool {
            self.failed
        }

        fn attempts(&self) -> u32 {
            self.attempts
        }
    }

    #[derive(Default)]
    struct ManagerState {
        tasks: HashMap<Uuid, TestTask>,
        added_task_ids: Vec<Uuid>,
        completed_outputs: HashMap<Uuid, Vec<u8>>,
    }

    struct FakeJobManager {
        state: Mutex<ManagerState>,
    }

    impl FakeJobManager {
        fn new() -> Self {
            Self {
                state: Mutex::new(ManagerState::default()),
            }
        }

        fn insert_task(&self, task: TestTask) {
            let mut state = lock(&self.state);
            state.tasks.insert(task.id, task);
        }

        fn tasks_by_code(&self, code: &str) -> Vec<TestTask> {
            let state = lock(&self.state);
            state
                .tasks
                .values()
                .filter(|task| task.code.as_str() == code)
                .cloned()
                .collect()
        }

        fn completed_output(&self, task_id: &Uuid) -> Option<Vec<u8>> {
            let state = lock(&self.state);
            state.completed_outputs.get(task_id).cloned()
        }

        fn added_task_ids(&self) -> Vec<Uuid> {
            let state = lock(&self.state);
            state.added_task_ids.clone()
        }
    }

    impl JobManager for FakeJobManager {
        fn add_task(&self, task_def: TaskDefinition) -> Result<Uuid, icegate_jobmanager::Error> {
            let task_id = Uuid::new_v4();
            let task = TestTask {
                id: task_id,
                code: task_def.code().clone(),
                input: task_def.input().to_vec(),
                output: Vec::new(),
                error_msg: String::new(),
                depends_on: task_def.depends_on().to_vec(),
                completed: false,
                failed: false,
                attempts: 0,
                timeout: task_def.timeout(),
            };
            let mut state = lock(&self.state);
            state.tasks.insert(task_id, task);
            state.added_task_ids.push(task_id);
            Ok(task_id)
        }

        fn complete_task(&self, task_id: &Uuid, output: Vec<u8>) -> Result<(), icegate_jobmanager::Error> {
            let mut state = lock(&self.state);
            let task = state.tasks.get_mut(task_id).ok_or_else(|| {
                icegate_jobmanager::Error::Other(format!("task {task_id} not found for completion"))
            })?;
            task.output = output.clone();
            task.completed = true;
            state.completed_outputs.insert(*task_id, output);
            Ok(())
        }

        fn fail_task(&self, task_id: &Uuid, error_msg: &str) -> Result<(), icegate_jobmanager::Error> {
            let mut state = lock(&self.state);
            let task = state.tasks.get_mut(task_id).ok_or_else(|| {
                icegate_jobmanager::Error::Other(format!("task {task_id} not found for failure"))
            })?;
            task.error_msg = error_msg.to_string();
            task.failed = true;
            Ok(())
        }

        fn get_task(&self, task_id: &Uuid) -> Result<Arc<dyn ImmutableTask>, icegate_jobmanager::Error> {
            let state = lock(&self.state);
            let task = state
                .tasks
                .get(task_id)
                .cloned()
                .ok_or_else(|| icegate_jobmanager::Error::Other(format!("task {task_id} not found")))?;
            Ok(Arc::new(task))
        }

        fn get_tasks_by_code(
            &self,
            code: &TaskCode,
        ) -> Result<Vec<Arc<dyn ImmutableTask>>, icegate_jobmanager::Error> {
            let state = lock(&self.state);
            let tasks = state
                .tasks
                .values()
                .filter(|task| task.code.as_str() == code.as_str())
                .cloned()
                .map(|task| Arc::new(task) as Arc<dyn ImmutableTask>)
                .collect();
            Ok(tasks)
        }
    }

    mod mocks {
        use super::*;

        mock! {
            pub QueueReader {}

            #[async_trait]
            impl QueueReaderApi for QueueReader {
                async fn plan_segments(
                    &self,
                    topic: &Topic,
                    start_offset: u64,
                    group_by_column_name: &str,
                    max_record_batches_per_task: usize,
                    cancel_token: &CancellationToken,
                ) -> icegate_queue::Result<SegmentsPlan>;

                async fn read_segment(
                    &self,
                    topic: &Topic,
                    offset: u64,
                    record_batch_idxs: &[usize],
                    cancel_token: &CancellationToken,
                ) -> icegate_queue::Result<Vec<RecordBatch>>;
            }
        }

        mock! {
            pub IcebergStorage {}

            #[async_trait]
            impl IcebergStorageApi for IcebergStorage {
                async fn get_committed_offset(
                    &self,
                    cancel_token: &CancellationToken,
                ) -> crate::error::Result<Option<u64>>;

                async fn write_parquet_files(
                    &self,
                    batches: Vec<RecordBatch>,
                    cancel_token: &CancellationToken,
                ) -> crate::error::Result<WrittenDataFiles>;

                async fn data_files_from_parquet_paths(
                    &self,
                    parquet_paths: &[String],
                    cancel_token: &CancellationToken,
                ) -> crate::error::Result<Vec<DataFile>>;

                async fn commit_data_files(
                    &self,
                    data_files: Vec<DataFile>,
                    record_type: &str,
                    last_offset: u64,
                    cancel_token: &CancellationToken,
                ) -> crate::error::Result<usize>;
            }
        }
    }

    fn test_shift_config(max_record_batches_per_task: usize) -> Arc<ShiftConfig> {
        let mut config = ShiftConfig::default();
        config.read.max_record_batches_per_task = max_record_batches_per_task;
        Arc::new(config)
    }

    fn test_timeouts() -> Result<TimeoutEstimator, crate::error::IngestError> {
        let config = super::config::ShiftTimeoutsConfig {
            plan_base_ms: 1_000,
            shift_base_ms: 1_000,
            shift_per_record_batch_ms: 10,
            shift_per_segment_ms: 100,
            commit_base_ms: 1_000,
            commit_per_parquet_file_ms: 500,
        };
        TimeoutEstimator::new(&config)
    }

    fn sample_batch() -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
        let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        Ok(RecordBatch::try_new(schema, vec![array])?)
    }

    fn data_file(path: &str) -> Result<DataFile, Box<dyn std::error::Error + Send + Sync>> {
        Ok(DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .record_count(1)
            .file_size_in_bytes(1)
            .build()?)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plan_executor_no_segments_completes() -> TestResult {
        let plan = SegmentsPlan {
            groups: Vec::new(),
            last_segment_offset: None,
            segments_count: 0,
            record_batches_total: 0,
        };
        let mut queue_reader = mocks::MockQueueReader::new();
        queue_reader
            .expect_plan_segments()
            .withf(|topic, start_offset, group_by, max, _| {
                topic.as_str() == "logs" && *start_offset == 0 && group_by == "tenant_id" && *max == 8
            })
            .returning(move |_, _, _, _, _| Ok(plan.clone()));
        let mut storage = mocks::MockIcebergStorage::new();
        storage
            .expect_get_committed_offset()
            .returning(|_| Ok(None));
        let shift_config = test_shift_config(8);
        let timeouts = test_timeouts()?;
        let executor = Arc::new(Executor::new(
            Arc::new(queue_reader),
            shift_config,
            Arc::new(storage),
            timeouts,
            "logs",
        ));

        let task = TestTask::new(TaskCode::new(PLAN_TASK_CODE), Vec::new());
        let manager = FakeJobManager::new();
        manager.insert_task(task.clone());

        let result = executor
            .plan_executor()(Arc::new(task.clone()), &manager, CancellationToken::new())
            .await;
        assert!(result.is_ok());
        assert!(manager.completed_output(&task.id).is_some());
        assert!(manager.added_task_ids().is_empty());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plan_executor_schedules_shift_and_commit() -> TestResult {
        let plan = SegmentsPlan {
            groups: vec![
                GroupedSegmentsPlan {
                    group_col_val: "tenant-a".to_string(),
                    segments: vec![SegmentRecordBatcheIdxs {
                        segment_offset: 10,
                        record_batch_idxs: vec![0, 1],
                    }],
                    segments_count: 1,
                    record_batches_total: 2,
                },
                GroupedSegmentsPlan {
                    group_col_val: "tenant-b".to_string(),
                    segments: vec![SegmentRecordBatcheIdxs {
                        segment_offset: 11,
                        record_batch_idxs: vec![0],
                    }],
                    segments_count: 1,
                    record_batches_total: 1,
                },
            ],
            last_segment_offset: Some(11),
            segments_count: 2,
            record_batches_total: 3,
        };
        let mut queue_reader = mocks::MockQueueReader::new();
        queue_reader
            .expect_plan_segments()
            .withf(|topic, start_offset, group_by, max, _| {
                topic.as_str() == "logs" && *start_offset == 2 && group_by == "tenant_id" && *max == 16
            })
            .returning(move |_, _, _, _, _| Ok(plan.clone()));
        let mut storage = mocks::MockIcebergStorage::new();
        storage
            .expect_get_committed_offset()
            .returning(|_| Ok(Some(1)));
        let shift_config = test_shift_config(16);
        let timeouts = test_timeouts()?;
        let expected_commit_timeout = timeouts.commit_timeout(2)?;
        let executor = Arc::new(Executor::new(
            Arc::new(queue_reader),
            shift_config,
            Arc::new(storage),
            timeouts,
            "logs",
        ));

        let task = TestTask::new(TaskCode::new(PLAN_TASK_CODE), Vec::new());
        let manager = FakeJobManager::new();
        manager.insert_task(task.clone());

        let result = executor
            .plan_executor()(Arc::new(task.clone()), &manager, CancellationToken::new())
            .await;
        assert!(result.is_ok());
        assert!(manager.completed_output(&task.id).is_some());

        let shift_tasks = manager.tasks_by_code(SHIFT_TASK_CODE);
        assert_eq!(shift_tasks.len(), 2);

        let commit_tasks = manager.tasks_by_code(COMMIT_TASK_CODE);
        assert_eq!(commit_tasks.len(), 1);
        let commit_task = &commit_tasks[0];
        let commit_input: CommitInput = serde_json::from_slice(&commit_task.input)?;
        assert_eq!(commit_input.last_offset, 11);
        assert_eq!(commit_task.timeout, expected_commit_timeout);

        let mut shift_ids: Vec<Uuid> = shift_tasks.iter().map(|task| task.id).collect();
        let mut commit_deps = commit_task.depends_on.clone();
        shift_ids.sort();
        commit_deps.sort();
        assert_eq!(commit_deps, shift_ids);

        let shift_inputs: Vec<ShiftInput> = shift_tasks
            .iter()
            .map(|task| serde_json::from_slice(&task.input))
            .collect::<Result<_, _>>()?;
        assert!(shift_inputs.iter().any(|input| input.tenant_id == "tenant-a"));
        assert!(shift_inputs.iter().any(|input| input.tenant_id == "tenant-b"));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shift_executor_empty_segments_completes() -> TestResult {
        let queue_reader = Arc::new(mocks::MockQueueReader::new());
        let storage = Arc::new(mocks::MockIcebergStorage::new());
        let shift_config = test_shift_config(4);
        let timeouts = test_timeouts()?;
        let executor = Arc::new(Executor::new(
            queue_reader,
            shift_config,
            storage,
            timeouts,
            "logs",
        ));

        let input = ShiftInput {
            tenant_id: "tenant".to_string(),
            segments: Vec::new(),
        };
        let task = TestTask::new(TaskCode::new(SHIFT_TASK_CODE), serde_json::to_vec(&input)?);
        let manager = FakeJobManager::new();
        manager.insert_task(task.clone());

        let result = executor
            .shift_executor()(Arc::new(task.clone()), &manager, CancellationToken::new())
            .await;
        assert!(result.is_ok());
        assert!(manager.completed_output(&task.id).is_some());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shift_executor_fails_on_empty_batches() -> TestResult {
        let mut queue_reader = mocks::MockQueueReader::new();
        queue_reader
            .expect_read_segment()
            .returning(|_, _, _, _| Ok(Vec::new()));
        let storage = Arc::new(mocks::MockIcebergStorage::new());
        let shift_config = test_shift_config(4);
        let timeouts = test_timeouts()?;
        let executor = Arc::new(Executor::new(
            Arc::new(queue_reader),
            shift_config,
            storage,
            timeouts,
            "logs",
        ));

        let input = ShiftInput {
            tenant_id: "tenant".to_string(),
            segments: vec![SegmentToRead {
                segment_offset: 1,
                record_batch_idxs: vec![0],
            }],
        };
        let task = TestTask::new(TaskCode::new(SHIFT_TASK_CODE), serde_json::to_vec(&input)?);
        let manager = FakeJobManager::new();
        manager.insert_task(task.clone());

        let result = executor
            .shift_executor()(Arc::new(task.clone()), &manager, CancellationToken::new())
            .await;
        assert!(result.is_err());
        assert!(manager.completed_output(&task.id).is_none());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shift_executor_successful_output() -> TestResult {
        let batch = sample_batch()?;
        let mut queue_reader = mocks::MockQueueReader::new();
        let batch_clone = batch.clone();
        queue_reader
            .expect_read_segment()
            .returning(move |_, _, _, _| Ok(vec![batch_clone.clone()]));
        let mut storage = mocks::MockIcebergStorage::new();
        let write_result = WrittenDataFiles {
            data_files: vec![data_file("s3://bucket/file-1.parquet")?],
            rows_written: 10,
        };
        storage
            .expect_write_parquet_files()
            .withf(|batches, _| batches.len() == 1)
            .returning(move |_, _| Ok(write_result.clone()));
        let shift_config = test_shift_config(4);
        let timeouts = test_timeouts()?;
        let executor = Arc::new(Executor::new(
            Arc::new(queue_reader),
            shift_config,
            Arc::new(storage),
            timeouts,
            "logs",
        ));

        let input = ShiftInput {
            tenant_id: "tenant".to_string(),
            segments: vec![SegmentToRead {
                segment_offset: 1,
                record_batch_idxs: vec![0],
            }],
        };
        let task = TestTask::new(TaskCode::new(SHIFT_TASK_CODE), serde_json::to_vec(&input)?);
        let manager = FakeJobManager::new();
        manager.insert_task(task.clone());

        let result = executor
            .shift_executor()(Arc::new(task.clone()), &manager, CancellationToken::new())
            .await;
        assert!(result.is_ok());
        let output = manager
            .completed_output(&task.id)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "missing output"))?;
        let parsed: ShiftOutput = serde_json::from_slice(&output)?;
        assert_eq!(parsed.parquet_files, vec!["s3://bucket/file-1.parquet".to_string()]);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commit_executor_already_committed() -> TestResult {
        let queue_reader = Arc::new(mocks::MockQueueReader::new());
        let mut storage = mocks::MockIcebergStorage::new();
        storage
            .expect_get_committed_offset()
            .returning(|_| Ok(Some(5)));
        let shift_config = test_shift_config(4);
        let timeouts = test_timeouts()?;
        let executor = Arc::new(Executor::new(
            queue_reader,
            shift_config,
            Arc::new(storage),
            timeouts,
            "logs",
        ));

        let input = CommitInput { last_offset: 5 };
        let task = TestTask::new(TaskCode::new(COMMIT_TASK_CODE), serde_json::to_vec(&input)?);
        let manager = FakeJobManager::new();
        manager.insert_task(task.clone());

        let result = executor
            .commit_executor()(Arc::new(task.clone()), &manager, CancellationToken::new())
            .await;
        assert!(result.is_ok());
        assert!(manager.completed_output(&task.id).is_some());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commit_executor_requires_dependencies() -> TestResult {
        let queue_reader = Arc::new(mocks::MockQueueReader::new());
        let mut storage = mocks::MockIcebergStorage::new();
        storage
            .expect_get_committed_offset()
            .returning(|_| Ok(None));
        let shift_config = test_shift_config(4);
        let timeouts = test_timeouts()?;
        let executor = Arc::new(Executor::new(
            queue_reader,
            shift_config,
            Arc::new(storage),
            timeouts,
            "logs",
        ));

        let input = CommitInput { last_offset: 10 };
        let task = TestTask::new(TaskCode::new(COMMIT_TASK_CODE), serde_json::to_vec(&input)?);
        let manager = FakeJobManager::new();
        manager.insert_task(task.clone());

        let result = executor
            .commit_executor()(Arc::new(task.clone()), &manager, CancellationToken::new())
            .await;
        assert!(result.is_err());
        assert!(manager.completed_output(&task.id).is_none());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn commit_executor_successful_commit() -> TestResult {
        let queue_reader = Arc::new(mocks::MockQueueReader::new());
        let mut storage = mocks::MockIcebergStorage::new();
        storage
            .expect_get_committed_offset()
            .returning(|_| Ok(None));
        let data_files = vec![
            data_file("s3://bucket/file-1.parquet")?,
            data_file("s3://bucket/file-2.parquet")?,
        ];
        let expected_paths = vec![
            "s3://bucket/file-1.parquet".to_string(),
            "s3://bucket/file-2.parquet".to_string(),
            "s3://bucket/file-1.parquet".to_string(),
            "s3://bucket/file-2.parquet".to_string(),
        ];
        let data_files_clone = data_files.clone();
        storage
            .expect_data_files_from_parquet_paths()
            .withf(move |paths, _| *paths == expected_paths.as_slice())
            .returning(move |_, _| Ok(data_files_clone.clone()));
        storage
            .expect_commit_data_files()
            .withf(|files, record_type, last_offset, _| {
                files.len() == 2 && *record_type == "logs" && *last_offset == 42
            })
            .returning(|_, _, _, _| Ok(2));
        let shift_config = test_shift_config(4);
        let timeouts = test_timeouts()?;
        let executor = Arc::new(Executor::new(
            queue_reader,
            shift_config,
            Arc::new(storage),
            timeouts,
            "logs",
        ));

        let shift_output = ShiftOutput {
            parquet_files: vec![
                "s3://bucket/file-1.parquet".to_string(),
                "s3://bucket/file-2.parquet".to_string(),
            ],
        };
        let shift_output_payload = serde_json::to_vec(&shift_output)?;
        let dep_one_id = Uuid::new_v4();
        let dep_two_id = Uuid::new_v4();
        let dep_one_task = TestTask::new(TaskCode::new(SHIFT_TASK_CODE), Vec::new())
            .with_output(shift_output_payload.clone());
        let dep_two_task = TestTask::new(TaskCode::new(SHIFT_TASK_CODE), Vec::new())
            .with_output(shift_output_payload.clone());
        let dep_one_task = TestTask { id: dep_one_id, ..dep_one_task };
        let dep_two_task = TestTask { id: dep_two_id, ..dep_two_task };

        let input = CommitInput { last_offset: 42 };
        let task = TestTask::new(TaskCode::new(COMMIT_TASK_CODE), serde_json::to_vec(&input)?)
            .with_dependencies(vec![dep_one_id, dep_two_id]);
        let manager = FakeJobManager::new();
        manager.insert_task(task.clone());
        manager.insert_task(dep_one_task);
        manager.insert_task(dep_two_task);

        let result = executor
            .commit_executor()(Arc::new(task.clone()), &manager, CancellationToken::new())
            .await;
        assert!(result.is_ok());
        assert!(manager.completed_output(&task.id).is_some());
        Ok(())
    }
}
