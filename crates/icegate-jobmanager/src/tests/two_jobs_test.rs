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

/// Runs two jobs in parallel with two workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_two_jobs_concurrent() -> Result<(), Box<dyn std::error::Error>> {
    let _log_guard = super::common::logging::init_test_logging();

    let tasks_per_iter = 3usize;
    let max_iterations = 3u64;

    // 1. Start MinIO
    let minio_env = MinIOEnv::new().await?;

    let job_a = JobCode::new("test_two_jobs_a");
    let job_b = JobCode::new("test_two_jobs_b");
    let task_code = TaskCode::new("simple_task");

    // 2. Track executions per job
    let job_a_count = Arc::new(AtomicU64::new(0));
    let job_b_count = Arc::new(AtomicU64::new(0));

    let job_a_count_clone = Arc::clone(&job_a_count);
    let job_b_count_clone = Arc::clone(&job_b_count);

    let executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let job_a_count = Arc::clone(&job_a_count_clone);
        let job_b_count = Arc::clone(&job_b_count_clone);
        let task_id = task.id().to_string();
        let payload = task.get_input().to_vec();

        Box::pin(async move {
            match payload.as_slice() {
                b"job_a" => {
                    job_a_count.fetch_add(1, Ordering::SeqCst);
                },
                b"job_b" => {
                    job_b_count.fetch_add(1, Ordering::SeqCst);
                },
                _ => {},
            }

            manager.complete_task(&task_id, b"done".to_vec())
        })
    });

    let mut executors_a = HashMap::new();
    executors_a.insert(task_code.clone(), Arc::clone(&executor));
    let mut executors_b = HashMap::new();
    executors_b.insert(task_code.clone(), Arc::clone(&executor));

    let mut tasks_a = Vec::new();
    for _ in 0..tasks_per_iter {
        tasks_a.push(TaskDefinition::new(
            task_code.clone(),
            b"job_a".to_vec(),
            ChronoDuration::seconds(2),
        )?);
    }

    let mut tasks_b = Vec::new();
    for _ in 0..tasks_per_iter {
        tasks_b.push(TaskDefinition::new(
            task_code.clone(),
            b"job_b".to_vec(),
            ChronoDuration::seconds(2),
        )?);
    }

    let job_def_a = JobDefinition::new(job_a.clone(), tasks_a, executors_a, max_iterations)?;
    let job_def_b = JobDefinition::new(job_b.clone(), tasks_b, executors_b, max_iterations)?;

    // 3. Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![job_def_a.clone(), job_def_b.clone()])?);

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

    // 5. Start manager with 2 workers
    let config = JobsManagerConfig {
        worker_count: 2,
        worker_config: WorkerConfig {
            poll_interval: Duration::from_millis(100),
            poll_interval_randomization: Duration::from_millis(10),
            retrier_config: Default::default(),
            ..Default::default()
        },
    };

    let mut manager_env = ManagerEnv::new(Arc::new(storage), config, Arc::clone(&job_registry), vec![
        job_def_a.clone(),
        job_def_b.clone(),
    ])?;

    // 6. Wait for completion
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(30)).await?;
    manager_env.stop().await;

    let expected_executions = (tasks_per_iter as u64) * max_iterations;
    assert_eq!(
        job_a_count.load(Ordering::SeqCst),
        expected_executions,
        "job A tasks should be executed for all iterations"
    );
    assert_eq!(
        job_b_count.load(Ordering::SeqCst),
        expected_executions,
        "job B tasks should be executed for all iterations"
    );

    // 7. Verify final job state
    let cancel_token = CancellationToken::new();
    let job_a_state = manager_env.storage().get_job(&job_a, &cancel_token).await?;
    assert_eq!(*job_a_state.status(), JobStatus::Completed);
    assert_eq!(job_a_state.iter_num(), max_iterations, "job A iteration mismatch");
    assert_eq!(
        job_a_state.tasks_as_iter().count(),
        tasks_per_iter,
        "job A tasks count mismatch"
    );
    let job_a_timeouts: u64 = job_a_state.tasks_as_iter().map(|t| (t.attempt() - 1).max(0) as u64).sum();
    assert_eq!(job_a_timeouts, 0, "job A should not have timeouts");

    let job_b_state = manager_env.storage().get_job(&job_b, &cancel_token).await?;
    assert_eq!(*job_b_state.status(), JobStatus::Completed);
    assert_eq!(job_b_state.iter_num(), max_iterations, "job B iteration mismatch");
    assert_eq!(
        job_b_state.tasks_as_iter().count(),
        tasks_per_iter,
        "job B tasks count mismatch"
    );
    let job_b_timeouts: u64 = job_b_state.tasks_as_iter().map(|t| u64::from(t.attempt() - 1)).sum();
    assert_eq!(job_b_timeouts, 0, "job B should not have timeouts");

    Ok(())
}
