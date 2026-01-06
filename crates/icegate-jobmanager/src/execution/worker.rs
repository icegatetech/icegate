use std::{any::Any, collections::HashMap, panic::AssertUnwindSafe, sync::Arc};

use futures_util::FutureExt;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use uuid::Uuid;

use crate::execution::job_manager::JobManagerImpl;
use crate::{
    InternalError, Job, JobCode, JobDefinition, JobError, JobRegistry, Metrics, Retrier, RetrierConfig, Storage,
    StorageError, Task, TaskCode,
};
// TODO(low): implement subscription mechanism for job updates between workers - if worker
// received/saved job, other workers should update their state to reduce races. Can be done via
// storage wrapper.

#[derive(Clone)]
pub struct WorkerConfig {
    /// Base poll interval for storage.
    pub poll_interval: Duration,
    /// Random jitter added to the poll interval.
    pub poll_interval_randomization: Duration,
    /// Maximum poll interval when backing off.
    pub max_poll_interval: Duration,
    /// Retry policy for storage operations.
    pub retrier_config: RetrierConfig,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_millis(200),
            poll_interval_randomization: Duration::from_millis(50),
            max_poll_interval: Duration::from_secs(2),
            retrier_config: RetrierConfig::default(),
        }
    }
}

struct JobCacheEntry {
    version: String,
    next_poll: std::time::Instant,
    exhausted: bool, // true if job reached maxIterations
}

struct JobMergeContext<'a> {
    current_job: &'a Job,
    saved_job: Job,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SaveOutcome {
    Saved,
    Skipped,
    Stolen,
}

enum MergeDecision {
    Retry(Job),
    Done(Job, SaveOutcome),
}

fn panic_payload_to_string(panic: &(dyn Any + Send)) -> String {
    #[allow(clippy::option_if_let_else)]
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

// Worker - iterates through jobs and executes tasks from jobs. Can execute only one task at a time.
// Task processing concurrency is controlled by number of workers.
pub(crate) struct Worker {
    id: String,
    job_registry: Arc<JobRegistry>,
    storage: Arc<dyn Storage>,
    config: WorkerConfig,
    retrier: Retrier,
    metrics: Metrics,

    // Cache to minimize S3 poll requests
    job_cache: RwLock<HashMap<JobCode, JobCacheEntry>>,
}

// TODO(med): cancel task if timeout

impl Worker {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub fn new(
        job_registry: Arc<JobRegistry>,
        storage: Arc<dyn Storage>,
        config: WorkerConfig,
        metrics: Metrics,
    ) -> Self {
        let retrier = Retrier::new(config.retrier_config.clone());

        Self {
            id: Uuid::new_v4().to_string(),
            job_registry,
            storage,
            config,
            retrier,
            metrics,
            job_cache: RwLock::new(HashMap::new()),
        }
    }

    #[tracing::instrument(
        skip(self, cancel_token),
        fields(worker_id = %self.id)
    )]
    pub async fn start(&self, cancel_token: CancellationToken) -> Result<(), InternalError> {
        info!("Starting worker {}", self.id);

        let mut poll_interval = self.config.poll_interval;
        let mut wait_duration = poll_interval;

        loop {
            tokio::select! {
                () = cancel_token.cancelled() => {
                    info!("Stopping worker {}", self.id);
                    return Ok(());
                }
                () = sleep(wait_duration) => {}
            }

            let work_done = self.process_jobs(&cancel_token).await;

            // Adaptive polling: if no work, increase interval
            poll_interval = if work_done {
                self.config.poll_interval
            } else {
                std::cmp::min(poll_interval * 2, self.config.max_poll_interval)
            };

            // Reduce strong concurrency between workers
            let jitter_ms = if self.config.poll_interval_randomization.is_zero() {
                self.config.poll_interval
            } else {
                #[allow(clippy::cast_possible_truncation)]
                Duration::from_millis(
                    rand::rng().random_range(0..self.config.poll_interval_randomization.as_millis() as u64),
                )
            };

            wait_duration = jitter_ms + poll_interval;
        }
    }

    async fn process_jobs(&self, cancel_token: &CancellationToken) -> bool {
        let job_codes = self.job_registry.list_jobs();
        let mut work_done = false;

        for job_code in job_codes {
            if self.process_job(&job_code, cancel_token).await {
                work_done = true;
                debug!("Job processed: {}", job_code);
                continue;
            }
            debug!("Job processing skipped: {}", job_code);
        }

        work_done
    }

    async fn process_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> bool {
        if !self.should_poll_job(job_code) {
            return false;
        }

        let job = match self.storage.get_job(job_code, cancel_token).await {
            Ok(job) => job,
            Err(StorageError::NotFound) => match self.create_new_job(job_code, cancel_token).await {
                Ok(job) => job,
                Err(InternalError::Cancelled) => {
                    debug!("Job creation cancelled");
                    return false;
                }
                Err(e) => {
                    error!("Failed to create new job {}: {}", job_code, e);
                    return false;
                }
            },
            // go to process job
            Err(StorageError::Cancelled) => {
                debug!("Job processing cancelled");
                return false;
            }
            Err(e) => {
                error!("Failed to get job {} for processing: {}", job_code, e);
                return false;
            }
        };

        self.update_cache(job_code.clone(), job.version().to_string(), false);

        self.try_process_job(job, cancel_token).await
    }

    fn should_poll_job(&self, job_code: &JobCode) -> bool {
        let cache = self.job_cache.read();
        if let Some(entry) = cache.get(job_code) {
            // Don't poll jobs that exhausted iteration limit
            if entry.exhausted {
                return false;
            }
            std::time::Instant::now() > entry.next_poll
        } else {
            true
        }
    }

    // Jobs can only be created from code, creation outside code makes no sense since there won't be
    // task handlers in code.
    #[tracing::instrument(skip(self, cancel_token), fields(worker_id = %self.id, job_code = %code))]
    async fn create_new_job(&self, code: &JobCode, cancel_token: &CancellationToken) -> Result<Job, InternalError> {
        let job_def = self.job_registry.get_job(code)?;
        let tasks = self.create_tasks_from_job_def(&job_def);

        let job = Job::new(
            code.clone(),
            tasks,
            HashMap::new(),
            self.id.clone(),
            job_def.max_iterations(),
        );

        let (job, outcome) = self
            .save_job_state(job, cancel_token, move |ctx| {
                let JobMergeContext { saved_job, .. } = ctx;
                debug!(
                    "Job '{}' already created (id: {}) by worker '{}'",
                    code,
                    saved_job.id(),
                    saved_job.updated_by_worker_id()
                );
                // TODO(low): in saveJobState on ConcurrentModification error we re-read job, which is unnecessary
                // in this case.
                Ok(MergeDecision::Done(saved_job, SaveOutcome::Skipped)) // Someone beat us to it
            })
            .await?;

        if outcome == SaveOutcome::Saved {
            info!("New job '{}' created (id: {}) by worker '{}'", code, job.id(), self.id);
        }

        Ok(job)
    }

    #[tracing::instrument(
        skip(self, job, cancel_token),
        fields(
            worker_id = %self.id,
            job_code = %job.code(),
            job_id = %job.id(),
            iter = job.iter_num()
        )
    )]
    async fn try_process_job(&self, mut job: Job, cancel_token: &CancellationToken) -> bool {
        if job.is_ready_to_next_iteration() {
            job = match self.start_new_job_iteration(job, cancel_token).await {
                Ok(job) => job,
                Err(InternalError::Cancelled) => {
                    debug!("Job iteration start cancelled");
                    return false;
                }
                Err(e) => {
                    error!("Failed to start job iteration: {}", e);
                    return true;
                }
            };
        } else if job.is_processed() && job.is_iteration_limit_reached() {
            self.update_cache(job.code().clone(), job.version().to_string(), true);
            return false;
        }

        if job.is_ready_for_processing() {
            self.pick_and_execute_task(job, cancel_token).await.unwrap_or_else(|e| {
                if matches!(e, InternalError::Cancelled) {
                    debug!("Job processing cancelled");
                    return false;
                }
                error!("Failed to execute task: {}", e);
                true
            })
        } else {
            false
        }
    }

    // This can be either first job run or next iteration of job run.
    #[tracing::instrument(
        skip(self, job, cancel_token),
        fields(
            worker_id = %self.id,
            job_code = %job.code(),
            job_id = %job.id(),
            iter = job.iter_num()
        )
    )]
    async fn start_new_job_iteration(
        &self,
        mut job: Job,
        cancel_token: &CancellationToken,
    ) -> Result<Job, InternalError> {
        let job_def = self.job_registry.get_job(job.code())?;
        let tasks = self.create_tasks_from_job_def(&job_def);

        job.next_iteration(tasks, self.id.clone(), job_def.max_iterations())?;

        let (job, outcome) = self
            .save_job_state(job, cancel_token, move |ctx| {
                let JobMergeContext { saved_job, .. } = ctx;
                debug!(
                    "Job '{}' already started new iteration (id: {}, iter: {}) by worker '{}'",
                    saved_job.code(),
                    saved_job.id(),
                    saved_job.iter_num(),
                    saved_job.updated_by_worker_id()
                );
                // TODO(low): in saveJobState on ErrConcurrentModification we re-read job, which is unnecessary in
                // this case.
                Ok(MergeDecision::Done(saved_job, SaveOutcome::Skipped)) // Someone beat us to it
            })
            .await?;

        if outcome == SaveOutcome::Saved {
            info!(
                "New job '{}' iteration started (id: {}, iter: {}) by worker '{}'",
                job.code(),
                job.id(),
                job.iter_num(),
                self.id
            );
        }

        Ok(job)
    }

    async fn pick_and_execute_task(
        &self,
        mut job: Job,
        cancel_token: &CancellationToken,
    ) -> Result<bool, InternalError> {
        // TODO(low): think about what to do here, likely we have invalid job state
        let Some(task_id) = job.pick_task_to_execute(&self.id)? else {
            debug!("Tasks for job {} not found", job.code());
            return Ok(false);
        };

        match job.start_task(&task_id, self.id.clone()) {
            Ok(()) => {}
            Err(JobError::TaskWorkerMismatch) => return Ok(true),
            Err(e) => return Err(InternalError::from(e)),
        }

        debug!("Task '{}' started", task_id);

        let task_id_clone = task_id.clone();
        let worker_id = self.id.clone();
        let (job, outcome) = self
            .save_job_state(job, cancel_token, move |ctx| {
                let JobMergeContext { mut saved_job, .. } = ctx;
                match saved_job.start_task(&task_id_clone, worker_id.clone()) {
                    Ok(()) => {
                        debug!("Job has concurrent modification when picking task - retry");
                        Ok(MergeDecision::Retry(saved_job))
                    }
                    Err(JobError::TaskWorkerMismatch) => {
                        debug!("Job has concurrent modification when picking task - skip");
                        Ok(MergeDecision::Done(saved_job, SaveOutcome::Stolen))
                    }
                    Err(e) => {
                        Err(InternalError::from(e)) // Don't retry
                    }
                }
            })
            .await?;

        if outcome == SaveOutcome::Stolen {
            info!("Job was stolen");
            return Ok(true);
        }

        info!("Started processing task '{}' (job iter: {})", task_id, job.iter_num());

        self.execute_task(job, &task_id, cancel_token.clone()).await?;

        Ok(true)
    }

    #[allow(clippy::map_unwrap_or)]
    #[tracing::instrument(
        skip(self, job, cancel_token),
        fields(
            worker_id = %self.id,
            job_code = %job.code(),
            job_id = %job.id(),
            iter = job.iter_num(),
            task_id = %task_id,
            task_code = %job
                .get_task(task_id)
                .map(|task| task.code().clone())
                .unwrap_or_else(|_| TaskCode::new("unknown"))
        )
    )]
    async fn execute_task(
        &self,
        job: Job,
        task_id: &str,
        cancel_token: CancellationToken,
    ) -> Result<(), InternalError> {
        let task_id = task_id.to_string();
        let task = job.get_task(&task_id)?;

        let executor = self.job_registry.get_task_executor(job.code(), &task.code().clone())?;

        // Execute task (wrap Job with moving)
        let wrapper_job = RwLock::new(job);
        let job_manager = JobManagerImpl::new(&wrapper_job, self.id.clone());

        let result = AssertUnwindSafe(executor(task, &job_manager, cancel_token.clone()))
            .catch_unwind()
            .await;
        let result = match result {
            Ok(result) => result.map_err(InternalError::from),
            Err(panic) => Err(InternalError::Other(format!(
                "executor panicked: {}",
                panic_payload_to_string(&*panic)
            ))),
        };

        if matches!(result, Err(InternalError::Cancelled)) {
            return Err(InternalError::Cancelled);
        }

        if cancel_token.is_cancelled() {
            return Err(InternalError::Cancelled);
        }

        // Recover ownership. Since executor is done and dropped (it was awaited), and job_manager is local,
        // we should satisfy unique access.
        drop(job_manager);
        let mut job = wrapper_job.into_inner();

        // TODO(low): think about to fail the task when expired
        if result.is_ok() && job.get_task(&task_id)?.is_expired() {
            info!("Task '{}' exceeded deadline", task_id);
        }

        // Handle result
        match result {
            Err(e) => {
                info!("Task '{}' execution failed: {}", task_id, e);
                job.fail_task(&task_id, &e.to_string())?;

                let worker_id = self.id.clone();
                let task_id_clone = task_id.clone();
                _ = self
                    .save_processed_task(job, &task_id, &cancel_token, move |ctx| {
                        let JobMergeContext {
                            current_job,
                            mut saved_job,
                        } = ctx;
                        match saved_job.merge_with_processed_task(current_job, &worker_id, &task_id_clone) {
                            Ok(()) => {
                                debug!("Retry to save failed task");
                                Ok(MergeDecision::Retry(saved_job))
                            }
                            Err(JobError::TaskWorkerMismatch) => {
                                debug!("Task has stolen when try to save failed task - skip");
                                Ok(MergeDecision::Done(saved_job, SaveOutcome::Stolen))
                            }
                            Err(e) => Err(InternalError::from(e)),
                        }
                    })
                    .await?;
            }
            Ok(()) => {
                info!("Task '{}' handled successfully", task_id);

                let worker_id = self.id.clone();
                let task_id_clone = task_id.clone();

                job.try_to_complete(&worker_id)?;

                job = self
                    .save_processed_task(job, &task_id, &cancel_token, move |ctx| {
                        let JobMergeContext {
                            current_job,
                            mut saved_job,
                        } = ctx;
                        match saved_job.merge_with_processed_task(current_job, &worker_id, &task_id_clone) {
                            Ok(()) => {
                                // conditions for job completion might have been met (another worker completed task)
                                saved_job.try_to_complete(&worker_id)?;
                                debug!("Retry to save completed task");
                                Ok(MergeDecision::Retry(saved_job))
                            }
                            Err(JobError::TaskWorkerMismatch) => {
                                debug!("Task has stolen when try to save completed task - skip");
                                Ok(MergeDecision::Done(saved_job, SaveOutcome::Stolen))
                            }
                            Err(e) => Err(InternalError::from(e)),
                        }
                    })
                    .await?;

                if job.is_processed() {
                    self.job_completed(&job);
                }
            }
        }

        Ok(())
    }

    async fn save_job_state<F>(
        &self,
        job: Job,
        cancel_token: &CancellationToken,
        concurrent_modification_handler: F,
    ) -> Result<(Job, SaveOutcome), InternalError>
    where
        F: for<'a> Fn(JobMergeContext<'a>) -> Result<MergeDecision, InternalError> + Send + Sync,
    {
        let storage = Arc::clone(&self.storage);
        let job_code = job.code().clone();
        let handler = Arc::new(concurrent_modification_handler);
        // Keep job state here so retrier doesn't need to carry it.
        let wrapped_job = Arc::new(Mutex::new(Some(job)));

        let outcome = self
            .retrier
            .retry(
                {
                    let storage = Arc::clone(&storage);
                    let job_code = job_code.clone();
                    let handler = Arc::clone(&handler);
                    let wrapped_job = Arc::clone(&wrapped_job);
                    move || {
                        let storage = Arc::clone(&storage);
                        let job_code = job_code.clone();
                        let handler = Arc::clone(&handler);
                        let wrapped_job = Arc::clone(&wrapped_job);
                        async move {
                            // Take job ownership for this attempt without holding the lock across await.
                            let mut current_job = {
                                let mut guard = wrapped_job.lock();
                                guard.take().ok_or_else(|| InternalError::Other("job state missing".into()))?
                            };
                            match storage.save_job(&mut current_job, cancel_token).await {
                                Ok(()) => {
                                    // Save succeeded: store updated job and stop retrying.
                                    *wrapped_job.lock() = Some(current_job);
                                    Ok((false, SaveOutcome::Saved))
                                }
                                Err(e) if e.is_conflict() => {
                                    // Conflict: refresh from storage, merge (if needed), and decide if we retry.
                                    let saved_job = storage.get_job(&job_code, cancel_token).await?; // TODO(med): getting a job is not always necessary, for example, it is not necessary when taking a task to work.
                                    match handler(JobMergeContext {
                                        current_job: &current_job,
                                        saved_job,
                                    })? {
                                        MergeDecision::Retry(updated_job) => {
                                            *wrapped_job.lock() = Some(updated_job);
                                            Ok((true, SaveOutcome::Saved))
                                        }
                                        MergeDecision::Done(updated_job, outcome) => {
                                            *wrapped_job.lock() = Some(updated_job);
                                            Ok((false, outcome))
                                        }
                                    }
                                }
                                Err(e) => Err(InternalError::from(e)),
                            }
                        }
                    }
                },
                cancel_token,
            )
            .await?;

        // Return the last job state after retries.
        let updated_job = wrapped_job
            .lock()
            .take()
            .ok_or_else(|| InternalError::Other("job state missing".into()))?;
        Ok((updated_job, outcome))
    }

    async fn save_processed_task<F>(
        &self,
        job: Job,
        task_id: &str,
        cancel_token: &CancellationToken,
        concurrent_modification_handler: F,
    ) -> Result<Job, InternalError>
    where
        F: for<'a> Fn(JobMergeContext<'a>) -> Result<MergeDecision, InternalError> + Send + Sync,
    {
        let (job, outcome) = self.save_job_state(job, cancel_token, concurrent_modification_handler).await?;

        if outcome == SaveOutcome::Stolen {
            info!("Task '{}' was stolen during save, skipping merge", task_id);
            return Ok(job);
        }

        debug!(
            "Job '{}' saved with processed task '{}' (iter: {}, version: {})",
            job.code(),
            task_id,
            job.iter_num(),
            job.version()
        );

        if let Some(tsk) = job.tasks_as_iter().find(|task| task.id() == task_id) {
            // Calculate duration if start/complete times are available
            let duration = match (tsk.completed_at(), tsk.started_at()) {
                (Some(completed), Some(started)) => completed
                    .signed_duration_since(started)
                    .to_std()
                    .unwrap_or(Duration::from_secs(0)),
                _ => Duration::from_secs(0),
            };

            self.metrics
                .record_task_processed(job.code(), tsk.code(), tsk.status(), duration);
        }

        Ok(job)
    }

    fn job_completed(&self, job: &Job) {
        info!("Job {} completed (iter: {})", job.code(), job.iter_num());

        // Calculate duration
        let duration = job.completed_at().map_or_else(
            || Duration::from_secs(0),
            |completed| {
                completed
                    .signed_duration_since(job.started_at())
                    .to_std()
                    .unwrap_or(Duration::from_secs(0))
            },
        );

        self.metrics
            .record_job_iteration_complete(job.code(), &crate::JobStatus::Completed, duration);
    }

    fn update_cache(&self, job_code: JobCode, version: String, exhausted: bool) {
        let mut cache = self.job_cache.write();
        cache.insert(
            job_code,
            JobCacheEntry {
                version,
                next_poll: std::time::Instant::now() + self.config.poll_interval,
                exhausted,
            },
        );
    }

    fn create_tasks_from_job_def(&self, job_def: &JobDefinition) -> Vec<Task> {
        job_def
            .initial_tasks()
            .iter()
            .map(|task_def| Task::new(self.id.clone(), task_def))
            .collect()
    }
}
