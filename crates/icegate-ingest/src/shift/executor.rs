//! Task executors for shift operations.
//!
//! Implements the plan -> shift -> commit pipeline for WAL processing.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use chrono::Duration as ChronoDuration;
use iceberg::Catalog;
use icegate_jobmanager::{ImmutableTask, JobManager, TaskCode, TaskDefinition, registry::TaskExecutorFn};
use icegate_queue::{QueueReader, SegmentsPlan};
use serde::{Deserialize, Serialize};

use super::{config::ShiftConfig, iceberg_storage::IcebergStorage};

/// Task code for plan WAL segments.
pub const PLAN_TASK_CODE: &str = "plan";
/// Task code for shifting WAL segments into Iceberg.
pub const SHIFT_TASK_CODE: &str = "shift";
/// Task code for committing shifted data to Iceberg.
pub const COMMIT_TASK_CODE: &str = "commit";

// TODO(crit): добавить расчет таймаута тасок

/// WAL segment metadata used for shift input. Segments are WAL files.
#[derive(Debug, Serialize, Deserialize)]
pub struct SegmentToRead {
    /// WAL segment offset.
    pub segment_offset: u64,
    /// Batch offsets for this tenant inside the segment. Batches are grouped by column.
    pub record_batch_idxs: Vec<usize>,
}

/// Input for the shift task.
#[derive(Debug, Serialize, Deserialize)]
pub struct ShiftInput {
    /// Tenant identifier handled by this task.
    pub tenant_id: String,
    /// WAL segments to read and shift.
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
    /// Highest WAL offset to commit in snapshot summary.
    pub last_offset: u64,
}

/// Shared executor dependencies for shift tasks.
pub struct Executor {
    queue_reader: Arc<QueueReader>,
    storage: Arc<IcebergStorage>,
    shift_config: Arc<ShiftConfig>,
    topic: String,
}

impl Executor {
    /// Creates a new executor and initializes shared dependencies.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        queue_reader: Arc<QueueReader>,
        shift_config: Arc<ShiftConfig>,
        topic: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, icegate_jobmanager::Error> {
        shift_config
            .validate()
            .map_err(|e| icegate_jobmanager::Error::TaskExecution(format!("invalid shift config: {e}")))?;

        let storage = Arc::new(IcebergStorage::new(catalog, table, shift_config.as_ref()));

        Ok(Self {
            queue_reader,
            storage,
            shift_config,
            topic: topic.into(),
        })
    }
    /// Creates executor for the plan task.
    pub fn plan_executor(self: Arc<Self>) -> TaskExecutorFn {
        // NOTICE: There is no guarantee of order inside the WAL files (segments), we just accept data from clients. Most likely, clients sort data by time (but this is not accurate).

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
                            icegate_jobmanager::Error::TaskExecution(format!("failed to plan WAL record batches: {e}"))
                        })?;

                    if plan.last_segment_offset.is_none() {
                        // TODO(high): now we are completing the job iteration and producing a lot of files, it may be worth restarting the task.
                        tracing::info!("plan: no WAL segments found for topic '{}'", topic);
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let last_offset = plan.last_segment_offset.unwrap_or(0);
                    let shift_task_ids = Self::schedule_shift_tasks(manager, plan)?;

                    tracing::info!(
                        "plan: scheduling shift for {} tasks (last_offset={})",
                        shift_task_ids.len(),
                        last_offset
                    );

                    let commit_input = CommitInput { last_offset };
                    let commit_task = TaskDefinition::new(
                        TaskCode::new(COMMIT_TASK_CODE),
                        serde_json::to_vec(&commit_input).map_err(|e| {
                            icegate_jobmanager::Error::TaskExecution(format!("failed to serialize commit input: {e}"))
                        })?,
                        ChronoDuration::minutes(10),
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
                        tracing::error!("shift: no WAL files provided, skipping");
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let mut batches: Vec<RecordBatch> = Vec::new();

                    for wal_file in &input.segments {
                        let wal_offset = wal_file.segment_offset;
                        let wal_batches = executor
                            .queue_reader
                            .read_segment(&executor.topic, wal_offset, &wal_file.record_batch_idxs, &cancel_token)
                            .await
                            .map_err(|e| {
                                icegate_jobmanager::Error::TaskExecution(format!(
                                    "failed to read WAL segment {wal_offset} row groups: {e}"
                                ))
                            })?;
                        batches.extend(wal_batches);
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
    ) -> Result<Vec<uuid::Uuid>, icegate_jobmanager::Error> {
        let mut shift_task_ids = Vec::new();
        for group in plan.groups {
            let wal_files = group
                .segments
                .into_iter()
                .map(|segment| SegmentToRead {
                    segment_offset: segment.segment_offset,
                    record_batch_idxs: segment.record_batch_idxs,
                })
                .collect::<Vec<_>>();

            if wal_files.is_empty() {
                continue;
            }

            let task_id = create_shift_task(manager, &group.group_col_val, wal_files)?;
            shift_task_ids.push(task_id);
        }

        if shift_task_ids.is_empty() {
            return Err(icegate_jobmanager::Error::TaskExecution(
                "no tenant WAL files to schedule".to_string(),
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
    wal_files: Vec<SegmentToRead>,
) -> Result<uuid::Uuid, icegate_jobmanager::Error> {
    let shift_input = ShiftInput {
        tenant_id: tenant_id.to_string(),
        segments: wal_files,
    };

    let shift_task = TaskDefinition::new(
        TaskCode::new(SHIFT_TASK_CODE),
        serde_json::to_vec(&shift_input)
            .map_err(|e| icegate_jobmanager::Error::TaskExecution(format!("failed to serialize shift input: {e}")))?,
        ChronoDuration::minutes(10),
    )?;

    manager.add_task(shift_task)
}
