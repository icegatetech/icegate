use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use chrono::Duration as ChronoDuration;
use tokio_util::sync::CancellationToken;

use super::common::{manager_env::ManagerEnv, minio_env::MinIOEnv};
use crate::{
    JobCode, JobDefinition, JobRegistry, JobStatus, JobsManagerConfig, Metrics, RetrierConfig, TaskCode,
    TaskDefinition, WorkerConfig,
    registry::TaskExecutorFn,
    s3_storage::{S3Storage, S3StorageConfig},
};

/// `TestTaskDeadlineExpiry` verifies that a task started by one worker is re-picked by another worker
/// after its deadline expires.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_task_deadline_expiry() -> Result<(), Box<dyn std::error::Error>> {
    let expected_attempts: u32 = 4;

    // 1. Start MinIO
    let minio_env = MinIOEnv::new().await?;

    // 2. Track task execution attempts
    let attempt_count = Arc::new(AtomicU32::new(0));

    let attempt_count_clone = Arc::clone(&attempt_count);

    let executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let count = Arc::clone(&attempt_count_clone);
        let task_id = task.id().to_string();

        Box::pin(async move {
            let attempt = count.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::info!("Task attempt {} started", attempt);

            if attempt <= 3 {
                // First attempt: exceed deadline so another worker can re-pick.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            // Complete successfully (first attempt might be stolen).
            manager.complete_task(&task_id, b"success".to_vec())
        })
    });

    let task_def = TaskDefinition::new(
        TaskCode::new("hanging_task"),
        Vec::new(),
        ChronoDuration::milliseconds(100),
    )?;

    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("hanging_task"), executor);

    let job_def = JobDefinition::new(
        JobCode::new("test_deadline_job"),
        vec![task_def.clone()],
        executors,
        1, // max_iterations
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
            request_timeout: Duration::from_millis(200),
            retrier_config: RetrierConfig::default(),
        },
        job_registry.clone(),
        Metrics::new_disabled(),
    )
    .await?;

    // 5. Start manager with 2 workers
    let config = JobsManagerConfig {
        worker_count: 3,
        worker_config: WorkerConfig {
            poll_interval: Duration::from_millis(10),
            poll_interval_randomization: Duration::from_millis(0),
            retrier_config: RetrierConfig::default(),
            ..Default::default()
        },
    };

    let mut manager_env = ManagerEnv::new(Arc::new(storage), config, Arc::clone(&job_registry), vec![job_def])?;

    // 6. Wait for job completion (should re-pick after deadline expires)
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(15)).await?;
    manager_env.stop().await;

    // 7. Verify task was attempted multiple times
    assert_eq!(
        attempt_count.load(Ordering::SeqCst),
        expected_attempts,
        "task should be attempted {expected_attempts} due to deadline expiry"
    );

    // Verify final job state
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_deadline_job"), &cancel_token)
        .await?;
    let tasks = job.get_tasks_by_code(task_def.code());
    let attempts = tasks.first().map_or(0, |t| t.attempts());
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(
        attempts, expected_attempts,
        "expected task to be restarted after deadline"
    );

    Ok(())
}
