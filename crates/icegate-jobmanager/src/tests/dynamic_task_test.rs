use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, Ordering},
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

/// `TestDynamicTaskCreation` verifies that an executor can create new tasks dynamically
#[tokio::test]
async fn test_dynamic_task_creation() -> Result<(), Box<dyn std::error::Error>> {


    // 1. Start MinIO
    let minio_env = MinIOEnv::new().await?;

    // 2. Track task execution
    let dynamic_task_count = 5;
    let init_task_executed = Arc::new(AtomicBool::new(false));
    let dynamic_tasks_executed = Arc::new(AtomicI32::new(0));

    let init_executed_clone = Arc::clone(&init_task_executed);

    // Initial task creates multiple dynamic tasks
    let init_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let executed = Arc::clone(&init_executed_clone);
        let task_id = task.id().to_string();

        Box::pin(async move {
            executed.store(true, Ordering::SeqCst);

            // Create multiple dynamic tasks
            #[allow(clippy::cast_possible_truncation)]
            for i in 0..dynamic_task_count {
                let dynamic_task_def =
                    TaskDefinition::new(TaskCode::new("dynamic_task"), vec![i as u8], ChronoDuration::seconds(5))?;
                manager.add_task(dynamic_task_def)?;
            }

            manager.complete_task(&task_id, Vec::new())
        })
    });

    let dynamic_executed_clone = Arc::clone(&dynamic_tasks_executed);

    // Dynamic task executor
    let dynamic_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let executed = Arc::clone(&dynamic_executed_clone);
        let task_id = task.id().to_string();

        Box::pin(async move {
            executed.fetch_add(1, Ordering::SeqCst);
            manager.complete_task(&task_id, b"done".to_vec())
        })
    });

    let init_task_def = TaskDefinition::new(TaskCode::new("init_task"), Vec::new(), ChronoDuration::seconds(5))?;

    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("init_task"), init_executor);
    executors.insert(TaskCode::new("dynamic_task"), dynamic_executor);

    let job_def = JobDefinition::new(
        JobCode::new("test_dynamic_job"),
        vec![init_task_def],
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

    // 7. Verify all tasks executed
    assert!(
        init_task_executed.load(Ordering::SeqCst),
        "init task should be executed"
    );
    assert_eq!(
        dynamic_tasks_executed.load(Ordering::SeqCst),
        i32::try_from(dynamic_task_count)?,
        "all dynamic tasks should be executed"
    );

    // Verify final job state
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_dynamic_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);

    // Verify task count: 1 init + N dynamic
    let tasks = job.get_tasks_by_code(&TaskCode::new("dynamic_task"));
    assert_eq!(
        tasks.len(),
        dynamic_task_count,
        "should have correct number of dynamic tasks"
    );

    Ok(())
}
