use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::Duration as ChronoDuration;
use tokio_util::sync::CancellationToken;

use super::common::{manager_env::ManagerEnv, minio_env::MinIOEnv};
use crate::{
    registry::TaskExecutorFn, s3_storage::{JobStateCodecKind, S3Storage, S3StorageConfig}, JobCode, JobDefinition, JobRegistry, JobStatus, JobsManagerConfig, Metrics,
    RetrierConfig, TaskCode,
    TaskDefinition,
    WorkerConfig,
};

/// `TestTaskDependencies` verifies that task dependencies are respected.
#[tokio::test]
async fn test_task_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let max_iterations = 1u64;

    // 1. Start MinIO
    let minio_env = MinIOEnv::new().await?;

    // 2. Track task execution
    let dep_a_done = Arc::new(AtomicBool::new(false));
    let dep_b_done = Arc::new(AtomicBool::new(false));
    let final_done = Arc::new(AtomicBool::new(false));

    let init_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let task_id = *task.id();

        Box::pin(async move {
            let dep_a_def = TaskDefinition::new(TaskCode::new("dep_a"), b"dep-a".to_vec(), ChronoDuration::seconds(5))?;
            let dep_b_def = TaskDefinition::new(TaskCode::new("dep_b"), b"dep-b".to_vec(), ChronoDuration::seconds(5))?;

            let dep_a_id = manager.add_task(dep_a_def)?;
            let dep_b_id = manager.add_task(dep_b_def)?;

            let final_def = TaskDefinition::new(
                TaskCode::new("final_task"),
                b"final".to_vec(),
                ChronoDuration::seconds(5),
            )?
            .with_dependencies(vec![dep_a_id, dep_b_id]);

            manager.add_task(final_def)?;
            manager.complete_task(&task_id, b"init-done".to_vec())
        })
    });

    let dep_a_done_clone = Arc::clone(&dep_a_done);
    let dep_a_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let dep_a_done = Arc::clone(&dep_a_done_clone);
        let task_id = *task.id();

        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            dep_a_done.store(true, Ordering::SeqCst);
            manager.complete_task(&task_id, b"dep-a-done".to_vec())
        })
    });

    let dep_b_done_clone = Arc::clone(&dep_b_done);
    let dep_b_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let dep_b_done = Arc::clone(&dep_b_done_clone);
        let task_id = *task.id();

        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            dep_b_done.store(true, Ordering::SeqCst);
            manager.complete_task(&task_id, b"dep-b-done".to_vec())
        })
    });

    let final_done_clone = Arc::clone(&final_done);
    let dep_a_done_clone = Arc::clone(&dep_a_done);
    let dep_b_done_clone = Arc::clone(&dep_b_done);
    let final_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let final_done = Arc::clone(&final_done_clone);
        let dep_a_done = Arc::clone(&dep_a_done_clone);
        let dep_b_done = Arc::clone(&dep_b_done_clone);
        let task_id = *task.id();

        Box::pin(async move {
            assert!(
                dep_a_done.load(Ordering::SeqCst),
                "dep_a should be completed before final task runs"
            );
            assert!(
                dep_b_done.load(Ordering::SeqCst),
                "dep_b should be completed before final task runs"
            );
            final_done.store(true, Ordering::SeqCst);
            manager.complete_task(&task_id, b"final-done".to_vec())
        })
    });

    let init_def = TaskDefinition::new(TaskCode::new("init_task"), b"init".to_vec(), ChronoDuration::seconds(5))?;

    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("init_task"), init_executor);
    executors.insert(TaskCode::new("dep_a"), dep_a_executor);
    executors.insert(TaskCode::new("dep_b"), dep_b_executor);
    executors.insert(TaskCode::new("final_task"), final_executor);

    let job_def = JobDefinition::new(
        JobCode::new("test_task_dependencies_job"),
        vec![init_def],
        executors,
    )?
    .with_max_iterations(max_iterations)?;

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
        worker_count: 3,
        worker_config: WorkerConfig {
            poll_interval: Duration::from_millis(50),
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
    assert!(dep_a_done.load(Ordering::SeqCst), "dep_a should execute");
    assert!(dep_b_done.load(Ordering::SeqCst), "dep_b should execute");
    assert!(final_done.load(Ordering::SeqCst), "final task should execute");

    // Verify job state in storage
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_task_dependencies_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(job.iter_num(), max_iterations);

    Ok(())
}
