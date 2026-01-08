//! Task executors for shift operations.
//!
//! Implements the prepare_wal -> shift -> commit pipeline for WAL processing.

use arrow::record_batch::RecordBatch;
use chrono::Duration as ChronoDuration;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use icegate_common::{StorageBackend, StorageConfig, ICEGATE_NAMESPACE};
use icegate_jobmanager::{registry::TaskExecutorFn, ImmutableTask, JobManager, TaskCode, TaskDefinition};
use icegate_queue::QueueReader;
use object_store::aws::AmazonS3Builder;
use parquet::file::statistics::Statistics;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

use super::{
    config::ShiftConfig, offset::get_committed_offset, parquet_meta_reader::data_files_from_parquet_paths,
    writer::Writer,
};

/// Task code for preparing WAL segments.
pub const PREPARE_WAL_TASK_CODE: &str = "prepare_wal";
/// Task code for shifting WAL segments into Iceberg.
pub const SHIFT_TASK_CODE: &str = "shift";
/// Task code for committing shifted data to Iceberg.
pub const COMMIT_TASK_CODE: &str = "commit";

// TODO(crit): добавить расчет таймаута тасок

/// Input for the prepare_wal task.
#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareWalInput {}

/// WAL segment metadata used for shift input.
#[derive(Debug, Serialize, Deserialize)]
pub struct WalFile {
    /// WAL segment offset.
    pub offset: u64,
    /// Row group offsets for this tenant inside the segment.
    pub row_groups: Vec<usize>,
}

/// Input for the shift task.
#[derive(Debug, Serialize, Deserialize)]
pub struct ShiftInput {
    /// Tenant identifier handled by this task.
    pub tenant_id: String,
    /// WAL segments to read and shift.
    pub wal_files: Vec<WalFile>,
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
    catalog: Arc<dyn Catalog>,
    queue_reader: Arc<QueueReader<object_store::aws::AmazonS3>>,
    shift_config: Arc<ShiftConfig>,
    topic: String,
    table: String,
}

impl Executor {
    /// Creates a new executor and initializes shared dependencies.
    pub async fn new(
        catalog: Arc<dyn Catalog>,
        storage_config: Arc<StorageConfig>,
        shift_config: Arc<ShiftConfig>,
        topic: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, icegate_jobmanager::Error> {
        let store = create_s3_store_for_queue(&storage_config, &shift_config.queue.base_path).await?;
        let queue_reader = QueueReader::new(&shift_config.queue.base_path, Arc::new(store));

        Ok(Self {
            catalog,
            queue_reader: Arc::new(queue_reader),
            shift_config,
            topic: topic.into(),
            table: table.into(),
        })
    }

    async fn load_table(&self) -> Result<iceberg::table::Table, icegate_jobmanager::Error> {
        let table_ident = TableIdent::new(NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), self.table.clone());
        self.catalog.load_table(&table_ident).await.map_err(|e| {
            icegate_jobmanager::Error::TaskExecution(format!("failed to load table '{}': {e}", self.table))
        })
    }
    /// Creates executor for the prepare_wal task.
    pub fn prepare_wal_executor(self: Arc<Self>) -> TaskExecutorFn {
        // NOTICE: There is no guarantee of order in the WAL files, we just accept data from clients. Most likely, clients sort data by time (but this is not accurate).

        Arc::new(
            move |task: Arc<dyn ImmutableTask>, manager: &dyn JobManager, cancel_token| {
                let executor = Arc::clone(&self);
                let task_id = *task.id();

                let fut = async move {
                    if cancel_token.is_cancelled() {
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let _input: PrepareWalInput = parse_task_input(task.as_ref())?;
                    let topic = executor.topic.clone();

                    let table = executor.load_table().await?;
                    let start_offset = get_committed_offset(&table).map_or(0, |offset| offset + 1);

                    tracing::info!("prepare_wal: topic '{}' starting from offset {}", topic, start_offset);

                    let segments = executor
                        .queue_reader
                        .list_segments_from(&topic, start_offset)
                        .await
                        .map_err(|e| icegate_jobmanager::Error::Other(format!("failed to list segments: {e}")))?;

                    if segments.is_empty() {
                        // TODO(crit): подумать, стоит ли тут завершать джобу или просто ретраить.
                        tracing::info!("prepare_wal: no WAL segments found for topic '{}'", topic);
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    if segments.is_empty() {
                        tracing::info!("prepare_wal: no readable WAL segments for topic '{}'", topic);
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let mut tenant_wal_files: HashMap<String, Vec<WalFile>> = HashMap::new();
                    let mut last_offset = start_offset;
                    for segment in segments {
                        let wal_offset = segment.offset;
                        if wal_offset > last_offset {
                            last_offset = wal_offset;
                        }
                        let parquet_meta = executor
                            .queue_reader
                            .read_parquet_metadata(&topic, wal_offset)
                            .await
                            .map_err(|e| {
                                icegate_jobmanager::Error::Other(format!(
                                    "failed to read WAL metadata {wal_offset}: {e}"
                                ))
                            })?;
                        let row_groups_by_tenant =
                            list_row_groups_by_tenant(&parquet_meta, "tenant_id").map_err(|e| {
                                icegate_jobmanager::Error::Other(format!(
                                    "failed to read tenant row groups for WAL {wal_offset}: {e}"
                                ))
                            })?;
                        if row_groups_by_tenant.is_empty() {
                            return Err(icegate_jobmanager::Error::Other(format!(
                                "no tenant row groups found for WAL {wal_offset}"
                            )));
                        }

                        for (tenant_id, row_groups) in row_groups_by_tenant {
                            tenant_wal_files.entry(tenant_id).or_default().push(WalFile {
                                offset: wal_offset,
                                row_groups,
                            });
                        }
                    }

                    tracing::info!(
                        "prepare_wal: scheduling shift for {} WAL segments (last_offset={})",
                        tenant_wal_files.len(),
                        last_offset
                    );

                    if tenant_wal_files.is_empty() {
                        return Err(icegate_jobmanager::Error::Other(
                            "no tenant WAL files to schedule".to_string(),
                        ));
                    }

                    let mut shift_task_ids = Vec::with_capacity(tenant_wal_files.len());
                    for (tenant_id, mut wal_files) in tenant_wal_files {
                        wal_files.sort_by_key(|wal_file| wal_file.offset);
                        let shift_input = ShiftInput { tenant_id, wal_files };

                        let shift_task = TaskDefinition::new(
                            TaskCode::new(SHIFT_TASK_CODE),
                            serde_json::to_vec(&shift_input).map_err(|e| {
                                icegate_jobmanager::Error::Other(format!("failed to serialize shift input: {e}"))
                            })?,
                            ChronoDuration::minutes(10),
                        )?;

                        let shift_task_id = manager.add_task(shift_task)?;
                        shift_task_ids.push(shift_task_id);
                    }

                    let commit_input = CommitInput { last_offset };
                    let commit_task = TaskDefinition::new(
                        TaskCode::new(COMMIT_TASK_CODE),
                        serde_json::to_vec(&commit_input).map_err(|e| {
                            icegate_jobmanager::Error::Other(format!("failed to serialize commit input: {e}"))
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
                    if input.wal_files.is_empty() {
                        tracing::info!("shift: no WAL files provided, skipping"); // TODO(crit): error
                        return manager.complete_task(&task_id, Vec::new());
                    }

                    let table = executor.load_table().await?;
                    let mut batches: Vec<RecordBatch> = Vec::new();

                    // TODO(crit): добавить ограничение по объему, хотя, возможно, оно должно быть сверху
                    for wal_file in &input.wal_files {
                        let wal_offset = wal_file.offset;
                        let wal_batches = executor
                            .queue_reader
                            .read_segment_row_groups(&executor.topic, wal_offset, &wal_file.row_groups)
                            .await
                            .map_err(|e| {
                                icegate_jobmanager::Error::Other(format!(
                                    "failed to read WAL segment {wal_offset} row groups: {e}"
                                ))
                            })?;
                        batches.extend(wal_batches);
                    }

                    if batches.is_empty() {
                        return Err(icegate_jobmanager::Error::Other(
                            "shift produced no record batches to write".to_string(),
                        ));
                    }

                    // TODO(crit): подумать, как можно минимизировать вызов load_table
                    let writer = Writer::new(
                        table,
                        Arc::clone(&executor.catalog),
                        executor.shift_config.row_group_size,
                        executor.shift_config.max_file_size_bytes(),
                    );
                    let write_result = writer
                        .write_parquet_files(batches)
                        .await
                        .map_err(|e| icegate_jobmanager::Error::Other(e.to_string()))?;

                    if write_result.data_files.is_empty() {
                        return Err(icegate_jobmanager::Error::Other(
                            "shift produced no parquet files to commit".to_string(),
                        ));
                    }

                    // TODO(crit): минимизировать строку с файлом
                    let parquet_files = write_result
                        .data_files
                        .iter()
                        .map(|data_file| data_file.file_path().to_string())
                        .collect::<Vec<_>>();

                    let output = ShiftOutput { parquet_files };
                    let output_payload = serde_json::to_vec(&output).map_err(|e| {
                        icegate_jobmanager::Error::Other(format!("failed to serialize shift output: {e}"))
                    })?;
                    manager.complete_task(&task_id, output_payload)
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
                    let table = executor.load_table().await?;

                    if task.depends_on().is_empty() {
                        return Err(icegate_jobmanager::Error::Other(
                            "commit task has no dependencies".to_string(),
                        ));
                    }

                    let mut parquet_files = Vec::new();
                    for dep_task_id in task.depends_on() {
                        let dep_task = manager.get_task(dep_task_id)?;
                        if dep_task.get_output().is_empty() {
                            return Err(icegate_jobmanager::Error::Other(format!(
                                "shift task '{dep_task_id}' produced empty output"
                            )));
                        }

                        let output: ShiftOutput = serde_json::from_slice(dep_task.get_output()).map_err(|e| {
                            icegate_jobmanager::Error::Other(format!(
                                "failed to parse shift output for '{dep_task_id}': {e}"
                            ))
                        })?;
                        parquet_files.extend(output.parquet_files);
                    }

                    if parquet_files.is_empty() {
                        return Err(icegate_jobmanager::Error::Other(
                            "commit received no parquet files from shift tasks".to_string(),
                        ));
                    }

                    let data_files = data_files_from_parquet_paths(&table, &parquet_files)
                        .await
                        .map_err(|e| icegate_jobmanager::Error::Other(e.to_string()))?;

                    let writer = Writer::new(
                        table,
                        Arc::clone(&executor.catalog),
                        executor.shift_config.row_group_size,
                        executor.shift_config.max_file_size_bytes(),
                    );
                    writer
                        .commit_data_files(data_files, &executor.topic, input.last_offset)
                        .await
                        .map_err(|e| icegate_jobmanager::Error::Other(e.to_string()))?;

                    manager.complete_task(&task_id, Vec::new())
                };

                Box::pin(fut)
            },
        )
    }
}

fn parse_task_input<T: for<'de> Deserialize<'de>>(task: &dyn ImmutableTask) -> Result<T, icegate_jobmanager::Error> {
    serde_json::from_slice(task.get_input())
        .map_err(|e| icegate_jobmanager::Error::Other(format!("failed to parse task input: {e}")))
}

fn list_row_groups_by_tenant(
    parquet_meta: &parquet::file::metadata::ParquetMetaData,
    tenant_column: &str,
) -> Result<HashMap<String, Vec<usize>>, icegate_jobmanager::Error> {
    let schema = parquet_meta.file_metadata().schema_descr();
    let tenant_col_idx = schema
        .columns()
        .iter()
        .position(|col| col.name() == tenant_column)
        .ok_or_else(|| {
            icegate_jobmanager::Error::Other(format!("tenant column '{tenant_column}' not found in parquet schema"))
        })?;

    let mut by_tenant: HashMap<String, Vec<usize>> = HashMap::new();
    for (row_group_idx, row_group) in parquet_meta.row_groups().iter().enumerate() {
        let column = row_group.column(tenant_col_idx);
        let stats = column.statistics().ok_or_else(|| {
            icegate_jobmanager::Error::Other(format!(
                "missing statistics for '{tenant_column}' in row group {row_group_idx}"
            ))
        })?;
        let tenant_id = tenant_from_stats(stats, tenant_column, row_group_idx)?;
        by_tenant.entry(tenant_id).or_default().push(row_group_idx);
    }

    Ok(by_tenant)
}

fn tenant_from_stats(
    stats: &Statistics,
    tenant_column: &str,
    row_group_idx: usize,
) -> Result<String, icegate_jobmanager::Error> {
    match stats {
        Statistics::ByteArray(byte_stats) => {
            let min = byte_stats.min_bytes_opt().ok_or_else(|| {
                icegate_jobmanager::Error::Other(format!(
                    "missing min statistic for '{tenant_column}' in row group {row_group_idx}"
                ))
            })?;
            let max = byte_stats.max_bytes_opt().ok_or_else(|| {
                icegate_jobmanager::Error::Other(format!(
                    "missing max statistic for '{tenant_column}' in row group {row_group_idx}"
                ))
            })?;

            if min != max {
                return Err(icegate_jobmanager::Error::Other(format!(
                    "row group {row_group_idx} contains multiple tenants for '{tenant_column}'"
                )));
            }

            let tenant_id = std::str::from_utf8(min).map_err(|e| {
                icegate_jobmanager::Error::Other(format!(
                    "invalid utf8 in '{tenant_column}' stats for row group {row_group_idx}: {e}"
                ))
            })?;

            if tenant_id.is_empty() {
                return Err(icegate_jobmanager::Error::Other(format!(
                    "empty tenant id in stats for '{tenant_column}' row group {row_group_idx}"
                )));
            }

            Ok(tenant_id.to_string())
        }
        _ => Err(icegate_jobmanager::Error::Other(format!(
            "unsupported stats type for '{tenant_column}' in row group {row_group_idx}"
        ))),
    }
}

/// Creates an S3 object store for reading from the queue.
async fn create_s3_store_for_queue(
    storage_config: &StorageConfig,
    queue_base_path: &str,
) -> std::result::Result<object_store::aws::AmazonS3, icegate_jobmanager::Error> {
    // Parse S3 URL: s3://bucket/prefix
    let path_without_scheme = queue_base_path.strip_prefix("s3://").unwrap_or(queue_base_path);
    let (bucket, _prefix) = path_without_scheme
        .split_once('/')
        .map_or_else(|| (path_without_scheme, ""), |(b, p)| (b, p));

    // Get S3 config from storage backend for endpoint/region settings
    let (endpoint, region) = match &storage_config.backend {
        StorageBackend::S3(s3_config) => (s3_config.endpoint.clone(), s3_config.region.clone()),
        _ => (None, "us-east-1".to_string()),
    };

    let mut builder = AmazonS3Builder::new().with_bucket_name(bucket).with_region(&region);

    if let Some(endpoint) = &endpoint {
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    }

    if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
        builder = builder.with_access_key_id(access_key);
    }
    if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
        builder = builder.with_secret_access_key(secret_key);
    }

    let store = builder
        .build()
        .map_err(|e| icegate_jobmanager::Error::Other(format!("failed to create S3 object store for queue: {e}")))?;

    Ok(store)
}
