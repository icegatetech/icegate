// This minimal example demonstrates how to configure and run a simple job with a single task.
// It sets up the necessary components: logger, storage (S3), job definition, and the job manager
// itself.

#![allow(missing_docs)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Duration as ChronoDuration;
use icegate_jobmanager::{
    Error, JobCode, JobDefinition, JobRegistry, JobsManager, JobsManagerConfig, Metrics, RetrierConfig, TaskCode,
    TaskDefinition,
    registry::TaskExecutorFn,
    s3_storage::{S3Storage, S3StorageConfig},
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    run_simple_job().await
}

async fn run_simple_job() -> Result<(), Error> {
    // Initialize tracing/logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter("icegate_jobmanager=debug")
        .init();

    tracing::info!("Starting simple job example");

    // 1. Define the initial task
    let task_def = TaskDefinition::new(TaskCode::new("my task code"), Vec::new(), ChronoDuration::seconds(5))?;

    // 2. Map executor for task
    let task_executor: TaskExecutorFn = Arc::new(|task, manager, _cancel_token| {
        // Extract task ID before async block to avoid Send trait issues
        let task_id = task.id().to_string();

        Box::pin(async move {
            tracing::info!("Executing task: {}", task_id);

            // Simulate work
            tokio::time::sleep(Duration::from_millis(100)).await;

            tracing::info!("Task completed: {}", task_id);
            manager.complete_task(&task_id, b"done".to_vec())
        })
    });

    let mut task_executors = HashMap::new();
    task_executors.insert(TaskCode::new("my task code"), task_executor);

    // 3. Define the job
    let job_def = JobDefinition::new(
        JobCode::new("simple job"),
        vec![task_def],
        task_executors,
        0, // max_iterations (0 = unlimited)
    )?;

    // 4. Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);

    // 5. Initialize S3 Storage (MinIO)
    let storage = S3Storage::new(
        S3StorageConfig {
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "minioadmin".to_string(),
            secret_access_key: "minioadmin".to_string(),
            bucket_name: "jobs".to_string(),
            use_ssl: false,
            region: "us-east-1".to_string(),
            bucket_prefix: "jobs".to_string(),
            request_timeout: Duration::from_secs(5),
            retrier_config: RetrierConfig::default(),
        },
        job_registry.clone(),
        Metrics::new_disabled(),
    )
    .await?;

    // 6. Initialize JobsManager
    let manager = JobsManager::new(
        Arc::new(storage),
        JobsManagerConfig::default(),
        Arc::clone(&job_registry),
        Metrics::new_disabled(),
    )?;

    // 7. Start the manager with signal handling
    tracing::info!("Starting job manager (press Ctrl+C to stop)");

    let handle = manager.start()?;
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| Error::Other(format!("ctrl_c error: {e}")))?;
    tracing::info!("Received interrupt signal, shutting down...");
    handle.shutdown().await?;

    tracing::info!("Job manager stopped");
    Ok(())
}
