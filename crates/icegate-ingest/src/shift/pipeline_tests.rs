use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use iceberg::{Catalog, NamespaceIdent, TableCreation};
use object_store::memory::InMemory;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use icegate_common::{
    catalog::{CatalogBackend, CatalogBuilder, CatalogConfig}, schema::{logs_partition_spec, logs_schema, logs_sort_order}, ICEGATE_NAMESPACE,
    LOGS_TABLE,
    LOGS_TOPIC,
};
use icegate_jobmanager::{
    InMemoryStorage, JobCode, JobDefinition, JobRegistry, JobsManager, JobsManagerConfig, Metrics, TaskCode,
    TaskDefinition, WorkerConfig,
};
use icegate_queue::{channel, ParquetQueueReader, QueueConfig, QueueWriter, WriteRequest, WriteResult};

use super::timeout::TimeoutEstimator;
use super::{Executor, IcebergStorage, ShiftConfig, COMMIT_TASK_CODE, PLAN_TASK_CODE, SHIFT_TASK_CODE};
use crate::transform::logs_to_record_batch;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_commits_single_tenant() -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();
    let store = Arc::new(InMemory::new());
    let queue_base_path = "queue";
    let offsets = write_queue_segments(Arc::clone(&store), queue_base_path, vec![("tenant-a", 1)]).await?;
    let expected_offset = *offsets
        .iter()
        .max()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "expected at least one offset"))?;

    let temp_dir = tempfile::tempdir()?;
    let catalog = create_catalog(temp_dir.path()).await?;
    let shift_config = test_shift_config();
    let storage = Arc::new(IcebergStorage::new(Arc::clone(&catalog), LOGS_TABLE, &shift_config));

    let queue_reader = Arc::new(ParquetQueueReader::new(queue_base_path, store));
    assert_queue_plan(queue_reader.as_ref(), &shift_config).await?;
    let handle = start_jobs_manager(queue_reader, Arc::new(shift_config), Arc::clone(&storage))?;

    wait_for_committed_offset(storage.as_ref(), expected_offset).await?;
    handle.shutdown().await?;

    let parquet_files = collect_parquet_files(temp_dir.path())?;
    assert!(!parquet_files.is_empty(), "expected parquet files in Iceberg data path");
    assert_has_tenants(&parquet_files, &["tenant-a"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pipeline_commits_multi_tenant_grouping() -> TestResult {
    let _ = tracing_subscriber::fmt::try_init();
    let store = Arc::new(InMemory::new());
    let queue_base_path = "queue";
    let offsets = write_queue_segments(
        Arc::clone(&store),
        queue_base_path,
        vec![("tenant-a", 1), ("tenant-b", 1)],
    )
    .await?;
    let expected_offset = *offsets
        .iter()
        .max()
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::Other, "expected at least one offset"))?;

    let temp_dir = tempfile::tempdir()?;
    let catalog = create_catalog(temp_dir.path()).await?;
    let shift_config = test_shift_config();
    let storage = Arc::new(IcebergStorage::new(Arc::clone(&catalog), LOGS_TABLE, &shift_config));

    let queue_reader = Arc::new(ParquetQueueReader::new(queue_base_path, store));
    assert_queue_plan(queue_reader.as_ref(), &shift_config).await?;
    let handle = start_jobs_manager(queue_reader, Arc::new(shift_config), Arc::clone(&storage))?;

    wait_for_committed_offset(storage.as_ref(), expected_offset).await?;
    handle.shutdown().await?;

    let parquet_files = collect_parquet_files(temp_dir.path())?;
    assert!(parquet_files.len() >= 2, "expected parquet files for multiple tenants");
    assert_has_tenants(&parquet_files, &["tenant-a", "tenant-b"]);
    Ok(())
}

fn test_shift_config() -> ShiftConfig {
    let mut config = ShiftConfig::default();
    config.read.max_record_batches_per_task = 1;
    config.jobsmanager.poll_interval_ms = 10;
    config.jobsmanager.iteration_interval_millisecs = 100;
    config
}

async fn write_queue_segments(
    store: Arc<InMemory>,
    base_path: &str,
    segments: Vec<(&str, usize)>,
) -> Result<Vec<u64>, Box<dyn std::error::Error + Send + Sync>> {
    let queue_config = QueueConfig::new(base_path).with_max_records_per_flush(1);
    let (tx, rx) = channel(queue_config.channel_capacity);
    let writer = QueueWriter::new(queue_config, store);
    let writer_handle = writer.start(rx);

    let mut offsets = Vec::new();
    for (tenant_id, record_count) in segments {
        let batch = logs_batch(tenant_id, record_count)?;
        let offset = write_batch(&tx, LOGS_TOPIC, batch).await?;
        offsets.push(offset);
    }

    drop(tx);
    writer_handle.await??;
    Ok(offsets)
}

fn logs_batch(
    tenant_id: &str,
    record_count: usize,
) -> Result<arrow::record_batch::RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let request = export_logs_request(record_count)?;
    Ok(logs_to_record_batch(&request, Some(tenant_id)).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::Other, "failed to build record batch")
    })?)
}

fn export_logs_request(
    record_count: usize,
) -> Result<ExportLogsServiceRequest, Box<dyn std::error::Error + Send + Sync>> {
    let base_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("system time error: {e}")))?
        .as_nanos();
    let base_nanos = u64::try_from(base_nanos)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "system time in nanos exceeds u64"))?;
    let log_records = (0..record_count)
        .map(|idx| {
            let idx_u64 = u64::try_from(idx)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "record count exceeds u64"))?;
            Ok(LogRecord {
                time_unix_nano: base_nanos + idx_u64,
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    Ok(ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    })
}

async fn write_batch(
    tx: &icegate_queue::WriteChannel,
    topic: &str,
    batch: arrow::record_batch::RecordBatch,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    tx.send(WriteRequest {
        topic: topic.to_string(),
        batch,
        group_by_column: Some("tenant_id".to_string()),
        response_tx,
    })
    .await?;

    match response_rx.await? {
        WriteResult::Success { offset, .. } => Ok(offset),
        WriteResult::Failed { reason } => Err(reason.into()),
    }
}

async fn create_catalog(warehouse_path: &Path) -> Result<Arc<dyn Catalog>, Box<dyn std::error::Error + Send + Sync>> {
    let warehouse = format!("file://{}", warehouse_path.to_string_lossy());
    let config = CatalogConfig {
        backend: CatalogBackend::Memory,
        warehouse,
        properties: HashMap::new(),
    };
    let catalog = CatalogBuilder::from_config(&config).await?;
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }

    let schema = logs_schema()?;
    let partition_spec = logs_partition_spec(&schema)?;
    let sort_order = logs_sort_order(&schema)?;
    let table_creation = TableCreation::builder()
        .name(LOGS_TABLE.to_string())
        .schema(schema)
        .partition_spec(partition_spec)
        .sort_order(sort_order)
        .build();
    catalog.create_table(&namespace, table_creation).await?;

    Ok(catalog)
}

fn start_jobs_manager(
    queue_reader: Arc<ParquetQueueReader>,
    shift_config: Arc<ShiftConfig>,
    storage: Arc<IcebergStorage>,
) -> Result<icegate_jobmanager::JobsManagerHandle, icegate_jobmanager::Error> {
    let timeouts =
        TimeoutEstimator::new(&shift_config.timeouts).map_err(|e| icegate_jobmanager::Error::Other(e.to_string()))?;
    let plan_timeout = timeouts.plan_timeout();
    let executor: Arc<Executor<ParquetQueueReader, IcebergStorage>> =
        Arc::new(Executor::new(queue_reader, shift_config, storage, timeouts, LOGS_TOPIC));

    let initial_task = TaskDefinition::new(TaskCode::new(PLAN_TASK_CODE), Vec::new(), plan_timeout)?;
    let mut executors = HashMap::new();
    executors.insert(TaskCode::new(PLAN_TASK_CODE), Arc::clone(&executor).plan_executor());
    executors.insert(TaskCode::new(SHIFT_TASK_CODE), Arc::clone(&executor).shift_executor());
    executors.insert(TaskCode::new(COMMIT_TASK_CODE), Arc::clone(&executor).commit_executor());

    let job_def =
        JobDefinition::new(JobCode::new("shift_logs_test"), vec![initial_task], executors)?.with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = Arc::new(InMemoryStorage::new());

    let manager = JobsManager::new(
        storage,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: WorkerConfig {
                poll_interval: Duration::from_millis(10),
                poll_interval_randomization: Duration::from_millis(0),
                ..Default::default()
            },
        },
        job_registry,
        Metrics::new_disabled(),
    )?;

    manager.start()
}

async fn wait_for_committed_offset(storage: &IcebergStorage, expected: u64) -> TestResult {
    let cancel_token = CancellationToken::new();
    timeout(Duration::from_secs(10), async {
        loop {
            if storage.get_last_offset(&cancel_token).await? == Some(expected) {
                return Ok::<(), crate::error::IngestError>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    Ok(())
}

fn collect_parquet_files(root: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if root.is_dir() {
        for entry in std::fs::read_dir(root)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                files.extend(collect_parquet_files(&path)?);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

fn assert_has_tenants(paths: &[PathBuf], tenants: &[&str]) {
    let path_strings: Vec<String> = paths.iter().map(|p| p.to_string_lossy().to_string()).collect();
    for tenant in tenants {
        let needle = format!("tenant_id={tenant}");
        assert!(
            path_strings.iter().any(|path| path.contains(&needle)),
            "expected parquet file path with '{needle}'"
        );
    }
}

async fn assert_queue_plan(queue_reader: &ParquetQueueReader, shift_config: &ShiftConfig) -> TestResult {
    let cancel_token = CancellationToken::new();
    let topic = LOGS_TOPIC.to_string();
    let plan = queue_reader
        .plan_segments(
            &topic,
            0,
            "tenant_id",
            shift_config.read.max_record_batches_per_task,
            &cancel_token,
        )
        .await?;
    if plan.last_segment_offset.is_none() {
        return Err(std::io::Error::new(std::io::ErrorKind::Other, "queue plan returned no segments").into());
    }
    Ok(())
}
