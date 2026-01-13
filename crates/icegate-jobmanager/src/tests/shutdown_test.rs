use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use chrono::Duration as ChronoDuration;
use tokio::{
    sync::{Mutex, oneshot},
    time::timeout,
};

use crate::storage::in_memory::InMemoryStorage;
use crate::{
    JobCode, JobDefinition, JobRegistry, JobsManager, JobsManagerConfig, Metrics, TaskCode, TaskDefinition,
    WorkerConfig, registry::TaskExecutorFn,
};

#[test]
fn start_returns_error_when_worker_count_is_zero() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let storage = Arc::new(InMemoryStorage::new());

    let executor: TaskExecutorFn = Arc::new(|_, _, _| Box::pin(async { Ok(()) }));
    let task_def = TaskDefinition::new(TaskCode::new("noop"), Vec::new(), ChronoDuration::seconds(1))?;
    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("noop"), executor);

    let job_def =
        JobDefinition::new(JobCode::new("zero_worker_job"), vec![task_def], executors)?.with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);

    let Err(_err) = JobsManager::new(
        storage,
        JobsManagerConfig {
            worker_count: 0,
            worker_config: WorkerConfig::default(),
        },
        job_registry,
        Metrics::new_disabled(),
    ) else {
        panic!("manager creation should fail with zero workers")
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shutdown_cancels_executor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let storage = Arc::new(InMemoryStorage::new());
    let (started_tx, started_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let cancelled = Arc::new(AtomicBool::new(false));

    let cancelled_flag = Arc::clone(&cancelled);
    let started_tx = Arc::clone(&started_tx);
    let executor: TaskExecutorFn = Arc::new(move |task, _manager, cancel_token| {
        let cancelled_flag = Arc::clone(&cancelled_flag);
        let started_tx = Arc::clone(&started_tx);
        let _task_id = *task.id();

        Box::pin(async move {
            let value = started_tx.lock().await.take();
            if let Some(tx) = value {
                let _ = tx.send(());
            }

            tokio::select! {
                () = cancel_token.cancelled() => {
                    cancelled_flag.store(true, Ordering::SeqCst);
                    Ok(())
                }
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    Ok(())
                }
            }
        })
    });

    let task_def = TaskDefinition::new(TaskCode::new("long_task"), Vec::new(), ChronoDuration::seconds(10))?;
    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("long_task"), executor);

    let job_def =
        JobDefinition::new(JobCode::new("shutdown_job"), vec![task_def], executors)?.with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);

    let manager = JobsManager::new(
        storage,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: WorkerConfig {
                poll_interval: Duration::from_millis(50),
                poll_interval_randomization: Duration::from_millis(0),
                ..Default::default()
            },
        },
        job_registry,
        Metrics::new_disabled(),
    )?;

    let handle = manager.start()?;

    timeout(Duration::from_secs(5), started_rx).await??;
    timeout(Duration::from_secs(5), handle.shutdown()).await??;

    assert!(cancelled.load(Ordering::SeqCst), "executor should observe cancellation");

    Ok(())
}
