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
use crate::storage::Storage;
use crate::{
    CachedStorage, JobCode, JobDefinition, JobRegistry, JobStatus, JobsManagerConfig, Metrics, RetrierConfig, TaskCode,
    TaskDefinition, WorkerConfig,
    registry::TaskExecutorFn,
    s3_storage::{JobStateCodecKind, S3Storage, S3StorageConfig},
};

/// Runs two jobs in parallel with two workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_two_jobs_concurrent() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let tasks_per_iter = 3usize;
    let max_iterations = 3u64;

    // 1. Start MinIO
    let minio_env = MinIOEnv::new().await?;

    let primary_job_code = JobCode::new("test_two_jobs_a");
    let secondary_job_code = JobCode::new("test_two_jobs_b");
    let task_code = TaskCode::new("simple_task");

    // 2. Track executions per job
    let primary_job_count = Arc::new(AtomicU64::new(0));
    let secondary_job_count = Arc::new(AtomicU64::new(0));

    let primary_job_count_clone = Arc::clone(&primary_job_count);
    let secondary_job_count_clone = Arc::clone(&secondary_job_count);

    let executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let primary_job_count = Arc::clone(&primary_job_count_clone);
        let secondary_job_count = Arc::clone(&secondary_job_count_clone);
        let task_id = *task.id();
        let payload = task.get_input().to_vec();

        Box::pin(async move {
            match payload.as_slice() {
                b"job_a" => {
                    primary_job_count.fetch_add(1, Ordering::SeqCst);
                }
                b"job_b" => {
                    secondary_job_count.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }

            manager.complete_task(&task_id, b"done".to_vec())
        })
    });

    let mut primary_executors = HashMap::new();
    primary_executors.insert(task_code.clone(), Arc::clone(&executor));
    let mut secondary_executors = HashMap::new();
    secondary_executors.insert(task_code.clone(), Arc::clone(&executor));

    let mut primary_tasks = Vec::new();
    for _ in 0..tasks_per_iter {
        primary_tasks.push(TaskDefinition::new(
            task_code.clone(),
            b"job_a".to_vec(),
            ChronoDuration::seconds(2),
        )?);
    }

    let mut secondary_tasks = Vec::new();
    for _ in 0..tasks_per_iter {
        secondary_tasks.push(TaskDefinition::new(
            task_code.clone(),
            b"job_b".to_vec(),
            ChronoDuration::seconds(2),
        )?);
    }

    let primary_job_def = JobDefinition::new(primary_job_code.clone(), primary_tasks, primary_executors)?
        .with_max_iterations(max_iterations)?;
    let secondary_job_def = JobDefinition::new(secondary_job_code.clone(), secondary_tasks, secondary_executors)?
        .with_max_iterations(max_iterations)?;

    // 3. Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![
        primary_job_def.clone(),
        secondary_job_def.clone(),
    ])?);

    // 4. Create storage
    let storage = Arc::new(
        S3Storage::new(
            S3StorageConfig {
                endpoint: minio_env.endpoint().to_string(),
                access_key_id: minio_env.username().to_string(),
                secret_access_key: minio_env.password().to_string(),
                bucket_name: "test-jobs".to_string(),
                use_ssl: false,
                region: "us-east-1".to_string(),
                bucket_prefix: "jobs".to_string(),
                job_state_codec: JobStateCodecKind::Json,
                request_timeout: Duration::from_millis(100),
                retrier_config: RetrierConfig::default(),
            },
            job_registry.clone(),
            Metrics::new_disabled(),
        )
        .await?,
    );
    let storage = Arc::new(CachedStorage::new(
        storage.clone() as Arc<dyn Storage>,
        Metrics::new_disabled(),
    ));

    // 5. Start manager with 2 workers
    let config = JobsManagerConfig {
        worker_count: 2,
        worker_config: WorkerConfig {
            poll_interval: Duration::from_millis(100),
            poll_interval_randomization: Duration::from_millis(10),
            retrier_config: RetrierConfig::default(),
            ..Default::default()
        },
    };

    let mut manager_env = ManagerEnv::new(
        storage,
        config,
        Arc::clone(&job_registry),
        vec![primary_job_def.clone(), secondary_job_def.clone()],
    )?;

    // 6. Wait for completion
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(30)).await?;
    manager_env.stop().await;

    // Due to concurrency, the actual task execution may exceed the expected one.
    let expected_executions = (tasks_per_iter as u64) * max_iterations;
    assert!(
        primary_job_count.load(Ordering::SeqCst) >= expected_executions,
        "job A tasks should be executed for all iterations"
    );
    assert!(
        secondary_job_count.load(Ordering::SeqCst) >= expected_executions,
        "job B tasks should be executed for all iterations"
    );

    // 7. Verify final job state
    let cancel_token = CancellationToken::new();
    let primary_job_state = manager_env.storage().get_job(&primary_job_code, &cancel_token).await?;
    assert_eq!(*primary_job_state.status(), JobStatus::Completed);
    assert_eq!(primary_job_state.iter_num(), max_iterations, "job A iteration mismatch");
    assert_eq!(
        primary_job_state.tasks_as_iter().count(),
        tasks_per_iter,
        "job A tasks count mismatch"
    );
    let primary_job_timeouts: u64 = primary_job_state.tasks_as_iter().map(|t| u64::from(t.attempt() - 1)).sum();
    assert_eq!(primary_job_timeouts, 0, "job A should not have timeouts");

    let secondary_job_state = manager_env.storage().get_job(&secondary_job_code, &cancel_token).await?;
    assert_eq!(*secondary_job_state.status(), JobStatus::Completed);
    assert_eq!(
        secondary_job_state.iter_num(),
        max_iterations,
        "job B iteration mismatch"
    );
    assert_eq!(
        secondary_job_state.tasks_as_iter().count(),
        tasks_per_iter,
        "job B tasks count mismatch"
    );
    let secondary_job_timeouts: u64 = secondary_job_state.tasks_as_iter().map(|t| u64::from(t.attempt() - 1)).sum();
    assert_eq!(secondary_job_timeouts, 0, "job B should not have timeouts");

    Ok(())
}
