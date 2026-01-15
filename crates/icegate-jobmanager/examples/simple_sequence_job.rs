// This example demonstrates a job with two sequential tasks.
// The first task 'first_step' simulates intermittent failures.
// Upon success, it creates the 'second_step' task.

#![allow(missing_docs)]

use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Duration as ChronoDuration;
use icegate_jobmanager::{
    Error, JobCode, JobDefinition, JobRegistry, JobsManager, JobsManagerConfig, Metrics, RetrierConfig, TaskCode,
    TaskDefinition, WorkerConfig,
    registry::TaskExecutorFn,
    s3_storage::{JobStateCodecKind, S3Storage, S3StorageConfig},
};
use rand::Rng;

#[tokio::main]
async fn main() -> Result<(), Error> {
    run_simple_seq_job().await
}

async fn run_simple_seq_job() -> Result<(), Error> {
    // Initialize tracing/logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter("icegate_jobmanager=debug")
        .init();

    tracing::info!("Starting simple sequence job example");

    // 1. Define the 'Simple Job' with first task
    let task_def = TaskDefinition::new(TaskCode::new("first_step"), Vec::new(), ChronoDuration::seconds(5))?;

    // Create executors for both steps
    let first_step_executor: TaskExecutorFn = Arc::new(|task, manager, _cancel_token| {
        let task_id = *task.id();

        Box::pin(async move {
            tracing::info!("[FirstStep] Started, task_id: {}", task_id);

            // Simulate work
            tokio::time::sleep(Duration::from_millis(500)).await;

            // Simulate flaky failure (30% chance)
            if rand::rng().random_bool(0.3) {
                tracing::warn!("[FirstStep] Random simulated failure");
                return Err(icegate_jobmanager::Error::Other("random simulated failure".to_string()));
            }

            tracing::info!("[FirstStep] Work completed successfully. Scheduling next step.");

            // Create the second task
            let next_task = TaskDefinition::new(
                TaskCode::new("second_step"),
                b"data from step 1".to_vec(),
                ChronoDuration::seconds(5),
            )?;

            manager.add_task(next_task)?;

            manager.complete_task(&task_id, b"done".to_vec())
        })
    });

    let second_step_executor: TaskExecutorFn = Arc::new(|task, manager, _cancel_token| {
        let task_id = *task.id();
        let input = String::from_utf8_lossy(task.get_input()).to_string();

        Box::pin(async move {
            tracing::info!("[SecondStep] Started, input: {}", input);

            // Simulate work
            tokio::time::sleep(Duration::from_millis(200)).await;

            tracing::info!("[SecondStep] Finished.");

            manager.complete_task(&task_id, Vec::new())
        })
    });

    let mut task_executors = HashMap::new();
    task_executors.insert(TaskCode::new("first_step"), first_step_executor);
    task_executors.insert(TaskCode::new("second_step"), second_step_executor);

    let job_def = JobDefinition::new(JobCode::new("simple sequence job"), vec![task_def], task_executors)?;

    // Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);

    // 2. Initialize S3 Storage (MinIO)
    let storage = S3Storage::new(
        S3StorageConfig {
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
        },
        job_registry.clone(),
        Metrics::new_disabled(),
    )
    .await?;

    // 3. Initialize JobsManager with 5 workers and custom worker config
    let config = JobsManagerConfig {
        worker_count: 5,
        worker_config: WorkerConfig {
            poll_interval: Duration::from_millis(500),
            poll_interval_randomization: Duration::from_millis(50),
            max_poll_interval: Duration::from_secs(2),
            retrier_config: RetrierConfig::default(),
        },
    };

    let manager = JobsManager::new(
        Arc::new(storage),
        config,
        Arc::clone(&job_registry),
        Metrics::new_disabled(),
    )?;

    // 4. Start the manager
    tracing::info!("Starting manager with 5 workers (press Ctrl+C to stop)...");

    let handle = manager.start()?;
    tokio::signal::ctrl_c()
        .await
        .map_err(|e| Error::Other(format!("ctrl_c error: {e}")))?;
    tracing::info!("Received interrupt signal, shutting down...");
    handle.shutdown().await?;

    tracing::info!("Manager stopped");
    Ok(())
}
