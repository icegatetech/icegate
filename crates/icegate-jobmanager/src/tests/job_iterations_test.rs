use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use chrono::Duration as ChronoDuration;
use tokio_util::sync::CancellationToken;

use super::common::{manager_env::ManagerEnv, minio_env::MinIOEnv};
use crate::{
    JobCode, JobDefinition, JobRegistry, JobStatus, JobsManagerConfig, Metrics, TaskCode, TaskDefinition, WorkerConfig,
    registry::TaskExecutorFn,
    s3_storage::{S3Storage, S3StorageConfig},
};

/// TestJobIterations verifies that a job can complete and restart for multiple iterations
#[tokio::test]
async fn test_job_iterations() -> Result<(), Box<dyn std::error::Error>> {
    let _log_guard = super::common::logging::init_test_logging();

    // 1. Start MinIO
    let minio_env = MinIOEnv::new().await?;

    // 2. Track iterations
    let expected_iterations = 3u64;
    let iteration_count = Arc::new(AtomicU64::new(0));

    let iteration_count_clone = Arc::clone(&iteration_count);

    let executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let count = Arc::clone(&iteration_count_clone);
        let task_id = task.id().to_string();

        Box::pin(async move {
            let current = count.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::info!("Executing iteration {}", current);

            // Complete the task - job will automatically restart for next iteration
            manager.complete_task(&task_id, b"done".to_vec())
        })
    });

    let task_def = TaskDefinition::new(TaskCode::new("iteration_task"), Vec::new(), ChronoDuration::seconds(5))?;

    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("iteration_task"), executor);

    let job_def = JobDefinition::new(
        JobCode::new("test_iterations_job"),
        vec![task_def],
        executors,
        expected_iterations,
    )?;

    // 3. Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    // 4. Create storage
    let storage = S3Storage::new(
        S3StorageConfig {
            endpoint: minio_env.endpoint().to_string(),
            access_key_id: minio_env.username().to_string(),
            secret_access_key: minio_env.password().to_string(),
            bucket_name: "test-jobs".to_string(),
            use_ssl: false,
            region: "us-east-1".to_string(),
            bucket_prefix: "jobs".to_string(),
            request_timeout: Duration::from_secs(5),
            retrier_config: Default::default(),
        },
        job_registry.clone(),
        Metrics::new_disabled(),
    )
    .await?;

    // 5. Start manager
    let config = JobsManagerConfig {
        worker_count: 1,
        worker_config: WorkerConfig {
            poll_interval: Duration::from_millis(100),
            poll_interval_randomization: Duration::from_millis(10),
            retrier_config: Default::default(),
            ..Default::default()
        },
    };

    let mut manager_env = ManagerEnv::new(Arc::new(storage), config, Arc::clone(&job_registry), vec![job_def])?;

    // 6. Wait for all iterations to complete
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(15)).await?;
    manager_env.stop().await;

    // 7. Verify correct number of iterations
    assert_eq!(
        iteration_count.load(Ordering::SeqCst),
        expected_iterations,
        "should have completed all iterations"
    );

    // Verify final job state
    let cancel_token = CancellationToken::new();
    let job = manager_env.storage().get_job(&JobCode::new("test_iterations_job"), &cancel_token).await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(job.iter_num(), expected_iterations, "job should be at final iteration");

    Ok(())
}
