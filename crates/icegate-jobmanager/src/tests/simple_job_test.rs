use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
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

/// `TestSimpleJobExecution` verifies basic job execution with one task
#[tokio::test]
async fn test_simple_job_execution() -> Result<(), Box<dyn std::error::Error>> {
    let max_iterations = 1u64;

    // 1. Start MinIO
    let minio_env = MinIOEnv::new().await?;

    // 2. Define job with single task
    let executed = Arc::new(AtomicBool::new(false));
    let task_input_captured = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let executed_clone = Arc::clone(&executed);
    let task_input_clone = Arc::clone(&task_input_captured);

    let executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let executed = Arc::clone(&executed_clone);
        let task_input = Arc::clone(&task_input_clone);
        let task_id = task.id().to_string();
        let input = task.get_input().to_vec();

        Box::pin(async move {
            *task_input.lock() = input;
            executed.store(true, Ordering::SeqCst);
            manager.complete_task(&task_id, b"result".to_vec())
        })
    });

    let task_def = TaskDefinition::new(
        TaskCode::new("simple_task"),
        b"test-input".to_vec(),
        ChronoDuration::seconds(5),
    )?;

    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("simple_task"), executor);

    let job_def = JobDefinition::new(
        JobCode::new("test_simple_job"),
        vec![task_def],
        executors,
        max_iterations,
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
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(10)).await?;
    manager_env.stop().await;

    // 7. Verify
    assert!(executed.load(Ordering::SeqCst), "task should be executed");
    assert_eq!(
        *task_input_captured.lock(),
        b"test-input".to_vec(),
        "task should receive correct input"
    );

    // Verify job state in storage
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_simple_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(job.iter_num(), max_iterations);

    Ok(())
}

/// `TestMultiTaskSequence` verifies a job with sequential tasks
#[tokio::test]
async fn test_multi_task_sequence() -> Result<(), Box<dyn std::error::Error>> {
    let max_iterations = 1u64;

    // 1. Start MinIO
    let minio_env = MinIOEnv::new().await?;

    // 2. Define job with two sequential tasks
    let first_task_executed = Arc::new(AtomicBool::new(false));
    let second_task_executed = Arc::new(AtomicBool::new(false));

    let first_executed_clone = Arc::clone(&first_task_executed);
    let first_task_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let executed = Arc::clone(&first_executed_clone);
        let task_id = task.id().to_string();

        Box::pin(async move {
            executed.store(true, Ordering::SeqCst);

            // Create second task
            let second_task_def = TaskDefinition::new(
                TaskCode::new("second_task"),
                b"from-first".to_vec(),
                ChronoDuration::seconds(5),
            )?;

            manager.add_task(second_task_def)?;
            manager.complete_task(&task_id, b"first-done".to_vec())
        })
    });

    let second_executed_clone = Arc::clone(&second_task_executed);
    let second_task_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let executed = Arc::clone(&second_executed_clone);
        let task_id = task.id().to_string();
        let input = task.get_input().to_vec();

        Box::pin(async move {
            executed.store(true, Ordering::SeqCst);
            assert_eq!(input, b"from-first".to_vec(), "should receive data from first task");
            manager.complete_task(&task_id, b"second-done".to_vec())
        })
    });

    let first_task_def = TaskDefinition::new(TaskCode::new("first_task"), Vec::new(), ChronoDuration::seconds(5))?;

    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("first_task"), first_task_executor);
    executors.insert(TaskCode::new("second_task"), second_task_executor);

    let job_def = JobDefinition::new(
        JobCode::new("test_sequence_job"),
        vec![first_task_def],
        executors,
        max_iterations,
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

    // 7. Verify
    assert!(first_task_executed.load(Ordering::SeqCst), "first task should execute");
    assert!(
        second_task_executed.load(Ordering::SeqCst),
        "second task should execute"
    );

    // Verify job state in storage
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_sequence_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(job.iter_num(), max_iterations);

    Ok(())
}
