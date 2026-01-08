//! Shifter binary - moves data from WAL to Iceberg tables.
//!
//! This binary runs as a standalone service that continuously reads segments
//! from the WAL queue and writes them to Iceberg tables using the JobManager
//! infrastructure for reliability and exactly-once delivery guarantees.

use std::{collections::HashMap, env, sync::Arc, time::Duration};

use chrono::Duration as ChronoDuration;
use icegate_common::{catalog::CatalogBuilder, S3Config, StorageBackend, StorageConfig, LOGS_TABLE};
use icegate_ingest::shift::{
    COMMIT_TASK_CODE,
    PREPARE_WAL_TASK_CODE,
    SHIFT_TASK_CODE,
    Executor,
    PrepareWalInput,
    ShiftConfig,
};
use icegate_jobmanager::{s3_storage::{JobStateCodecKind, S3StorageConfig}, CachedStorage, JobDefinition, JobRegistry, JobsManager, JobsManagerConfig, Metrics, RetrierConfig, S3Storage, TaskCode, TaskDefinition, WorkerConfig};
use tracing::{info, Level};

// TODO(crit): remove to ingest

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting Shifter job");

    // TODO(crit): конфиг
    // Load configuration from environment
    let catalog_uri = env::var("ICEBERG_CATALOG_URI").unwrap_or_else(|_| "http://gg3.local:19120/iceberg".to_string());
    let warehouse = env::var("ICEBERG_WAREHOUSE").unwrap_or_else(|_| "s3://warehouse/".to_string());
    let catalog_prefix = env::var("ICEBERG_CATALOG_PREFIX").unwrap_or_else(|_| "main".to_string());

    let s3_endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://gg3.local:9000".to_string());
    let s3_bucket = env::var("S3_BUCKET").unwrap_or_else(|_| "warehouse".to_string());
    let s3_region = env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    let queue_base_path = env::var("QUEUE_BASE_PATH").unwrap_or_else(|_| "queue/".to_string());
    let access_key = env::var("AWS_ACCESS_KEY_ID").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());

    // Build catalog
    let mut catalog_properties = HashMap::new();
    catalog_properties.insert("prefix".to_string(), catalog_prefix);

    let catalog_config = icegate_common::catalog::CatalogConfig {
        backend: icegate_common::catalog::CatalogBackend::Rest { uri: catalog_uri },
        warehouse,
        properties: catalog_properties,
    };

    let catalog = CatalogBuilder::from_config(&catalog_config).await?;
    info!("Catalog initialized");

    // Build storage config for object store
    let storage_config = Arc::new(StorageConfig {
        backend: StorageBackend::S3(S3Config {
            bucket: s3_bucket.clone(),
            region: s3_region.clone(),
            endpoint: Some(s3_endpoint.clone()),
        }),
        properties: Default::default(),
    });

    // Build shift config
    let shift_config = Arc::new(ShiftConfig::new(queue_base_path));


    let s3_config = S3StorageConfig {
        endpoint: s3_endpoint.to_string(),
        access_key_id: access_key,
        secret_access_key: secret_key,
        bucket_name: "jobs".to_string(),
        use_ssl: false,
        region: s3_region.to_string(),
        bucket_prefix: "shifter".to_string(),
        job_state_codec: JobStateCodecKind::Json,
        request_timeout: Duration::from_secs(5),
        retrier_config: RetrierConfig::default(),
    };

    let topic = "logs".to_string();
    let table = LOGS_TABLE.to_string();
    let initial_input = PrepareWalInput {};
    let initial_task = TaskDefinition::new(
        TaskCode::new(PREPARE_WAL_TASK_CODE),
        serde_json::to_vec(&initial_input)?,
        ChronoDuration::minutes(10), // TODO(crit): timeout
    )?;

    // Create executors
    let executor = Arc::new(
        Executor::new(
            catalog.clone(),
            storage_config.clone(),
            shift_config.clone(),
            topic,
            table,
        )
        .await?,
    );
    let prepare_wal_executor = Arc::clone(&executor).prepare_wal_executor();
    let shift_executor = Arc::clone(&executor).shift_executor();
    let commit_executor = Arc::clone(&executor).commit_executor();

    // Create JobDefinition
    let mut executors = HashMap::new();
    executors.insert(TaskCode::new(PREPARE_WAL_TASK_CODE), prepare_wal_executor);
    executors.insert(TaskCode::new(SHIFT_TASK_CODE), shift_executor);
    executors.insert(TaskCode::new(COMMIT_TASK_CODE), commit_executor);

    let job_def = JobDefinition::new(
        "shift_logs".into(),
        vec![initial_task],
        executors,
    )?;

    // Create JobRegistry
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    info!("Job registry created");

    // Create S3Storage for job persistence
    let s3_storage = Arc::new(S3Storage::new(s3_config, job_registry.clone(), Metrics::new_disabled()).await?);

    let cached_storage = Arc::new(CachedStorage::new(s3_storage, Metrics::new_disabled()));

    // Create JobsManager
    let manager_config = JobsManagerConfig {
        worker_count: 1, // One worker for logs
        worker_config: WorkerConfig {
            poll_interval: Duration::from_secs(1),
            ..Default::default()
        },
    };

    let manager = JobsManager::new(cached_storage, manager_config, job_registry, Metrics::new_disabled())?;

    // Start manager with signal handling
    info!("Starting Shifter job manager (press Ctrl+C to stop)");
    let handle = manager.start()?;

    // Wait for shutdown signal
    tokio::signal::ctrl_c().await?;
    info!("Received interrupt signal, shutting down...");

    handle.shutdown().await?;
    info!("Shifter stopped");

    Ok(())
}
