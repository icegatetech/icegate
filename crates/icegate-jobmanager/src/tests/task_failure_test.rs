use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use chrono::Duration as ChronoDuration;
use tokio_util::sync::CancellationToken;

use super::common::{manager_env::ManagerEnv, minio_env::MinIOEnv};
use crate::{
    Error, JobCode, JobDefinition, JobRegistry, JobStatus, JobsManagerConfig, Metrics, RetrierConfig, TaskCode,
    TaskDefinition, WorkerConfig,
    registry::TaskExecutorFn,
    s3_storage::{JobStateCodecKind, S3Storage, S3StorageConfig},
};

/// `TestTaskFailureAndRetry` verifies that failed tasks are retried
#[tokio::test]
async fn test_task_failure_and_retry() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    // 1. Start MinIO
    let minio_env = MinIOEnv::new().await?;

    // 2. Track attempts
    let attempt_count = Arc::new(AtomicI32::new(0));

    let attempt_count_clone = Arc::clone(&attempt_count);

    let executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let count = Arc::clone(&attempt_count_clone);
        let task_id = *task.id();

        Box::pin(async move {
            let attempt = count.fetch_add(1, Ordering::SeqCst) + 1;

            // Fail on first attempt, succeed on second
            if attempt == 1 {
                tracing::warn!("Attempt {}: simulating failure", attempt);
                return Err(Error::Other("simulated failure".to_string()));
            }

            tracing::info!("Attempt {}: succeeding", attempt);
            manager.complete_task(&task_id, b"success".to_vec())
        })
    });

    let task_def = TaskDefinition::new(TaskCode::new("flaky_task"), Vec::new(), ChronoDuration::seconds(5))?;

    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("flaky_task"), executor);

    let job_def =
        JobDefinition::new(JobCode::new("test_retry_job"), vec![task_def], executors)?.with_max_iterations(1)?;

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
            job_state_codec: JobStateCodecKind::Json,
            request_timeout: Duration::from_secs(5),
            retrier_config: RetrierConfig::default(),
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
            retrier_config: RetrierConfig::default(),
            ..Default::default()
        },
    };

    let mut manager_env = ManagerEnv::new(Arc::new(storage), config, Arc::clone(&job_registry), vec![job_def])?;

    // 6. Wait for completion
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(15)).await?;
    manager_env.stop().await;

    // 7. Verify task was attempted multiple times
    assert!(
        attempt_count.load(Ordering::SeqCst) >= 2,
        "task should be retried after failure"
    );

    // Verify final job state
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_retry_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);

    Ok(())
}
