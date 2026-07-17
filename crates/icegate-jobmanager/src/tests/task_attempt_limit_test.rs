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

use super::common::manager_env::ManagerEnv;
use crate::core::task::DEFAULT_MAX_ATTEMPTS;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    Error, JobCode, JobDefinition, JobRegistry, JobStatus, JobsManagerConfig, Storage, TaskCode, TaskDefinition,
    WorkerConfig, registry::TaskExecutorFn,
};

/// Attempt budget the failing task is given explicitly in the dependency test.
const MAX_ATTEMPTS: u32 = 2;
/// How long a poll loop waits for the job to reach an expected state.
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);
/// Interval between job-state polls.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Jobs-manager settings shared by the tests: one worker, tight polling, so a
/// failing task burns its attempt budget quickly and deterministically.
fn manager_config() -> JobsManagerConfig {
    JobsManagerConfig {
        worker_count: 1,
        worker_config: WorkerConfig {
            poll_interval: Duration::from_millis(20),
            poll_interval_randomization: Duration::from_millis(5),
            max_poll_interval: Duration::from_millis(50),
            ..Default::default()
        },
    }
}

/// An executor that always fails, counting how many times it was invoked.
fn failing_executor(attempts: &Arc<AtomicU32>) -> TaskExecutorFn {
    let attempts = Arc::clone(attempts);
    Arc::new(move |_task, _manager, _cancel_token| {
        let attempts = Arc::clone(&attempts);
        Box::pin(async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(Error::Other("always fails".to_string()))
        })
    })
}

/// Poll the stored job until `predicate` holds, returning an error on timeout.
async fn wait_for_job<F>(
    storage: &Arc<dyn Storage>,
    job_code: &JobCode,
    predicate: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Fn(&crate::Job) -> bool,
{
    let cancel_token = CancellationToken::new();
    let start = tokio::time::Instant::now();
    loop {
        if let Ok(job) = storage.get_job(job_code, &cancel_token).await {
            if predicate(&job) {
                return Ok(());
            }
        }
        if start.elapsed() > WAIT_TIMEOUT {
            return Err("timeout waiting for the expected job state".into());
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// A task that keeps failing must stop being retried once it spends the attempt
/// budget: the iteration ends as `Failed`, the task dependent on it never runs,
/// and the next iteration starts on schedule and replans from scratch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_exhausted_task_fails_iteration_and_next_iteration_starts() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("attempt_limit_job");
    let failing_attempts = Arc::new(AtomicU32::new(0));
    let dependent_attempts = Arc::new(AtomicU32::new(0));

    let plan_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let task_id = *task.id();
        Box::pin(async move {
            let failing_def = TaskDefinition::new(TaskCode::new("failing"), Vec::new(), ChronoDuration::seconds(5))?
                .with_max_attempts(MAX_ATTEMPTS)?;
            let failing_id = manager.add_task(failing_def)?;

            let dependent_def =
                TaskDefinition::new(TaskCode::new("dependent"), Vec::new(), ChronoDuration::seconds(5))?
                    .with_dependencies(vec![failing_id]);
            manager.add_task(dependent_def)?;

            manager.complete_task(&task_id, Vec::new())
        })
    });

    let dependent_attempts_clone = Arc::clone(&dependent_attempts);
    let dependent_executor: TaskExecutorFn = Arc::new(move |task, manager, _cancel_token| {
        let dependent_attempts = Arc::clone(&dependent_attempts_clone);
        let task_id = *task.id();
        Box::pin(async move {
            dependent_attempts.fetch_add(1, Ordering::SeqCst);
            manager.complete_task(&task_id, Vec::new())
        })
    });

    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("plan"), plan_executor);
    executors.insert(TaskCode::new("failing"), failing_executor(&failing_attempts));
    executors.insert(TaskCode::new("dependent"), dependent_executor);

    let plan_def = TaskDefinition::new(TaskCode::new("plan"), Vec::new(), ChronoDuration::seconds(5))?;
    let job_def = JobDefinition::new(job_code.clone(), vec![plan_def], executors)?
        // Long enough that the failed iteration is observable before the next one
        // starts, short enough to keep the test quick.
        .with_iteration_interval(ChronoDuration::seconds(2))?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
    let mut manager_env = ManagerEnv::new(
        Arc::clone(&storage),
        manager_config(),
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    // The first iteration must end as Failed, not hang retrying forever.
    wait_for_job(&storage, &job_code, |job| {
        matches!(job.status(), JobStatus::Failed) && job.iter_num() == 1
    })
    .await?;

    assert_eq!(
        failing_attempts.load(Ordering::SeqCst),
        MAX_ATTEMPTS,
        "the failing task must be retried exactly up to its attempt budget"
    );
    assert_eq!(
        dependent_attempts.load(Ordering::SeqCst),
        0,
        "a task blocked behind a terminally failed dependency must never run"
    );

    // The failed iteration must not stop the job: the next one starts on schedule.
    wait_for_job(&storage, &job_code, |job| job.iter_num() > 1).await?;
    manager_env.stop().await;

    Ok(())
}

/// Without an explicit budget a task gets [`DEFAULT_MAX_ATTEMPTS`] tries before
/// the iteration is failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_default_attempt_limit_applies_without_explicit_budget() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("default_attempt_limit_job");
    let failing_attempts = Arc::new(AtomicU32::new(0));

    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("failing"), failing_executor(&failing_attempts));

    let failing_def = TaskDefinition::new(TaskCode::new("failing"), Vec::new(), ChronoDuration::seconds(5))?;
    let job_def = JobDefinition::new(job_code.clone(), vec![failing_def], executors)?.with_max_iterations(1)?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
    let mut manager_env = ManagerEnv::new(
        Arc::clone(&storage),
        manager_config(),
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    wait_for_job(&storage, &job_code, |job| matches!(job.status(), JobStatus::Failed)).await?;
    manager_env.stop().await;

    assert_eq!(
        failing_attempts.load(Ordering::SeqCst),
        DEFAULT_MAX_ATTEMPTS,
        "a task without an explicit budget must use the default attempt limit"
    );

    Ok(())
}
