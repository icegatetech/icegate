#![allow(missing_docs)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Duration as ChronoDuration;
use icegate_jobmanager::{
    CachedStorage, Error, ImmutableTask, JobDefinition, JobManager, JobRegistry, JobsManager, JobsManagerConfig,
    Metrics, RetrierConfig, S3Storage, TaskDefinition, registry::TaskExecutorFn,
    s3_storage::{JobStateCodecKind, S3StorageConfig},
};
use serde::{Deserialize, Serialize};
use tracing::{Level, info};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct TaskData {
    message: String,
    value: i32,
    timestamp: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt().with_max_level(Level::INFO).init();

    info!("Starting JSON model job example");

    // 1. Setup Storage
    let s3_config = S3StorageConfig {
        endpoint: "http://localhost:9000".to_string(),
        access_key_id: "minioadmin".to_string(),
        secret_access_key: "minioadmin".to_string(),
        bucket_name: "jobs".to_string(),
        use_ssl: false,
        region: "us-east-1".to_string(),
        bucket_prefix: "jobs".to_string(),
        job_state_codec: JobStateCodecKind::Json,
        request_timeout: Duration::from_secs(5),
        retrier_config: RetrierConfig::default(),
    };

    // Storage needs job definitions for reading job settings (enrich_job).
    // We'll build JobRegistry first and share it with storage + manager.

    // Define tasks
    let task1_def = TaskDefinition::new("step1".into(), Vec::new(), ChronoDuration::seconds(5))?;
    let task2_def = TaskDefinition::new("step2".into(), Vec::new(), ChronoDuration::seconds(5))?;

    // Register executors
    let mut executors = HashMap::new();

    // Step 1: Generate Data
    let step1_executor: TaskExecutorFn = Arc::new(
        |task: Arc<dyn ImmutableTask>, manager: &dyn JobManager, _cancel_token| {
            let fut = async move {
                info!("Step 1 executing...");
                let timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|e| Error::Other(format!("system time error: {e}")))?;
                let data = TaskData {
                    message: "Hello from Step 1".to_string(),
                    value: 42,
                    timestamp: timestamp.as_secs(),
                };

                let output_json = serde_json::to_vec(&data)?;
                info!("Step 1 completing with data: {:?}", data);

                // Use the manager API to persist the output.
                manager.complete_task(task.id(), output_json)
            };
            Box::pin(fut)
        },
    );

    // Step 2: Process Data
    let step2_executor: TaskExecutorFn = Arc::new(
        |task: Arc<dyn ImmutableTask>, manager: &dyn JobManager, _cancel_token| {
            let fut = async move {
                info!("Step 2 executing...");

                // Let's assume for this example the input is what we care about.
                let input = task.get_input();
                if input.is_empty() {
                    info!("Step 2 received empty input");
                } else {
                    let data: TaskData = serde_json::from_slice(input)?;
                    info!("Step 2 received data: {:?}", data);
                }

                manager.complete_task(task.id(), Vec::new())
            };
            Box::pin(fut)
        },
    );

    executors.insert("step1".into(), step1_executor);
    executors.insert("step2".into(), step2_executor);

    let job_def = JobDefinition::new(
        format!("JSON model job-{}", Uuid::new_v4()).into(),
        vec![task1_def, task2_def],
        executors,
    )?
    .with_max_iterations(3)?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    // retrier was unused.
    let s3_storage = Arc::new(S3Storage::new(s3_config, job_registry.clone(), Metrics::new_disabled()).await?);

    let cached_storage = Arc::new(CachedStorage::new(s3_storage, Metrics::new_disabled()));

    // job_code was unused.

    // 2. Start Manager
    let manager_config = JobsManagerConfig {
        worker_count: 2,
        ..Default::default()
    };

    // ManagerEnv is for tests. We should use JobsManager directly.
    let manager = JobsManager::new(
        cached_storage.clone(),
        manager_config,
        Arc::clone(&job_registry),
        Metrics::new_disabled(),
    )?;

    let manager_handle = manager.start()?;

    info!("Manager started. Waiting for job execution...");

    // Wait for some time
    tokio::time::sleep(Duration::from_secs(10)).await;

    manager_handle.shutdown().await?;

    info!("Example finished.");

    Ok(())
}
