use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, ImmutableTask, Task, TaskCode, TaskDefinition, error::JobError, registry::TaskExecutorFn};

/// Job identifier used to select a job definition and persisted state.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JobCode(String);

impl JobCode {
    pub fn new(code: impl Into<String>) -> Self {
        Self(code.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for JobCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for JobCode {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for JobCode {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

// TODO(low): add a status for jobs that should not be repeated. For example, a job should only run
// N times (or only once).
/// Job lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    Started,   // New job created or new iteration started - tasks can be picked up for work.
    Running,   // Job is in progress: tasks are executing
    Completed, // Job completed successfully
    Failed,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started => write!(f, "started"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

impl JobStatus {
    // Checks if transition to new status is allowed and returns error if not
    fn can_transition_to(&self, new: &Self) -> Result<(), JobError> {
        let allowed = match self {
            Self::Started => matches!(new, Self::Running | Self::Failed),
            Self::Running => matches!(new, Self::Running | Self::Completed | Self::Failed),
            Self::Completed => matches!(new, Self::Started),
            Self::Failed => matches!(new, Self::Started),
        };

        if allowed {
            Ok(())
        } else {
            Err(JobError::InvalidStatusTransition {
                from: self.to_string(),
                to: new.to_string(),
            })
        }
    }

    fn transition_to(&mut self, new: Self) -> Result<(), JobError> {
        self.can_transition_to(&new)?;
        *self = new;
        Ok(())
    }
}

/// Immutable job definition with initial tasks and executors.
#[derive(Clone)]
pub struct JobDefinition {
    // TODO(med): add schedule time - after successful completion of a job, set `next_start_at` field and worker should
    // remember the next start time to avoid reading this job unnecessarily (only read at next scheduled start).
    code: JobCode,
    initial_tasks: Vec<TaskDefinition>,
    task_executors: HashMap<TaskCode, TaskExecutorFn>,
    max_iterations: u64, // 0 = unlimited
}

impl JobDefinition {
    pub fn new(
        code: JobCode,
        initial_tasks: Vec<TaskDefinition>,
        task_executors: HashMap<TaskCode, TaskExecutorFn>,
        max_iterations: u64,
    ) -> Result<Self, Error> {
        if initial_tasks.is_empty() {
            return Err(Error::Other("initial tasks cannot be empty".into()));
        }
        if task_executors.is_empty() {
            return Err(Error::Other("task executors cannot be empty".into()));
        }

        for task in &initial_tasks {
            if !task_executors.contains_key(task.code()) {
                return Err(Error::Other(format!(
                    "cannot find task executor for initial task {}",
                    task.code()
                )));
            }
        }

        Ok(Self {
            code,
            initial_tasks,
            task_executors,
            max_iterations,
        })
    }

    pub const fn code(&self) -> &JobCode {
        &self.code
    }

    pub fn initial_tasks(&self) -> &[TaskDefinition] {
        &self.initial_tasks
    }

    pub fn task_executors(&self) -> &HashMap<TaskCode, TaskExecutorFn> {
        &self.task_executors
    }

    pub const fn max_iterations(&self) -> u64 {
        self.max_iterations
    }
}

#[derive(Clone)]
pub(crate) struct Job {
    id: String,
    code: JobCode,
    iter_num: u64,
    status: JobStatus,
    tasks_by_id: HashMap<String, Arc<Task>>, // Arc makes cloning cheap - only pointer is cloned
    updated_by_worker_id: String,
    started_at: DateTime<Utc>,
    running_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    next_start_at: Option<DateTime<Utc>>,
    metadata: HashMap<String, serde_json::Value>,
    version: String,
    max_iterations: u64, // 0 = unlimited, from JobDefinition TODO(low): extract to new settings structure
}

impl Job {
    pub(crate) fn new(
        code: JobCode,
        tasks: Vec<Task>,
        metadata: HashMap<String, serde_json::Value>,
        worker_id: String,
        max_iterations: u64,
    ) -> Self {
        let mut tasks_by_id = HashMap::new();
        for task in tasks {
            tasks_by_id.insert(task.id().to_string(), Arc::new(task));
        }

        Self {
            id: Uuid::new_v4().to_string(),
            code,
            iter_num: 1,
            status: JobStatus::Started,
            tasks_by_id,
            updated_by_worker_id: worker_id,
            started_at: Utc::now(),
            running_at: None,
            completed_at: None,
            next_start_at: None,
            metadata,
            version: String::new(),
            max_iterations,
        }
    }

    pub(crate) fn restore(
        id: String,
        code: JobCode,
        version: String,
        iter_num: u64,
        status: JobStatus,
        tasks: Vec<Task>,
        updated_by_worker_id: String,
        started_at: DateTime<Utc>,
        running_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        next_start_at: Option<DateTime<Utc>>,
        metadata: HashMap<String, serde_json::Value>,
        max_iterations: u64,
    ) -> Self {
        let mut tasks_by_id = HashMap::new();
        for task in tasks {
            tasks_by_id.insert(task.id().to_string(), Arc::new(task));
        }

        Self {
            id,
            code,
            iter_num,
            status,
            tasks_by_id,
            updated_by_worker_id,
            started_at,
            running_at,
            completed_at,
            next_start_at,
            metadata,
            version,
            max_iterations,
        }
    }

    // NextIteration prepares the job for the next iteration
    pub(crate) fn next_iteration(
        &mut self,
        tasks: Vec<Task>,
        worker_id: String,
        max_iterations: u64,
    ) -> Result<(), JobError> {
        if !self.is_ready_to_next_iteration() {
            return Err(JobError::Other("job is not ready to next iteration".into()));
        }

        self.status.transition_to(JobStatus::Started)?;

        let old_id = self.id.clone();
        let old_iter_num = self.iter_num;
        let old_metadata = self.metadata.clone();

        *self = Self::new(self.code.clone(), tasks, old_metadata, worker_id, max_iterations);
        self.id = old_id;
        // TODO(low): in the future, a mechanism for restarting the sequence is needed (currently the
        // maximum sequence is 10^20). Sequential uuid will not work, as there may be a race when creating a
        // new job by different workers.
        self.iter_num = old_iter_num + 1;
        self.started_at = Utc::now();

        Ok(())
    }

    pub(crate) fn add_task(&mut self, task_def: TaskDefinition, worker_id: &str) -> Result<(), JobError> {
        let task = Task::new(worker_id.to_string(), task_def);

        if self.tasks_by_id.contains_key(task.id()) {
            return Err(JobError::Other(format!(
                "task with id {} already registered in job",
                task.id()
            )));
        }

        self.tasks_by_id.insert(task.id().to_string(), Arc::new(task));
        Ok(())
    }

    pub(crate) fn start_task(&mut self, task_id: &str, worker_id: String) -> Result<(), JobError> {
        let task_arc = self.get_task_arc_mut(task_id)?;

        let task = Arc::make_mut(task_arc); // Copy on write: clone only if refcount > 1
        task.start(worker_id.clone())?;
        self.updated_by_worker_id = worker_id;

        Ok(())
    }

    pub(crate) fn complete_task(&mut self, task_id: &str, output: Vec<u8>) -> Result<(), JobError> {
        let task_arc = self.get_task_arc_mut(task_id)?;

        let task = Arc::make_mut(task_arc);
        task.complete(output)
    }

    pub(crate) fn fail_task(&mut self, task_id: &str, error_msg: &str) -> Result<(), JobError> {
        let task_arc = self.get_task_arc_mut(task_id)?;

        let task = Arc::make_mut(task_arc);
        task.fail(error_msg)
    }

    pub(crate) fn pick_task_to_execute(&mut self, worker_id: &str) -> Result<Option<String>, JobError> {
        if !matches!(self.status, JobStatus::Running) {
            self.work(worker_id)?;
        }

        // TODO(low): with a large number of tasks in the job, iteration can add overhead. Solution: pending
        // tasks can be cached.
        for (task_id, task) in &self.tasks_by_id {
            // Since map iteration is randomized, no additional randomization is needed.
            if task.can_be_picked_up() {
                return Ok(Some(task_id.clone()));
            }
        }

        if self.all_tasks_completed() {
            return Err(JobError::Other(format!(
                "wrong running job {} state - all tasks complete",
                self.code
            )));
        }

        Ok(None)
    }

    // Accessors
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) const fn code(&self) -> &JobCode {
        &self.code
    }

    pub(crate) const fn iter_num(&self) -> u64 {
        self.iter_num
    }

    pub(crate) const fn status(&self) -> &JobStatus {
        &self.status
    }

    pub(crate) fn version(&self) -> &str {
        &self.version
    }

    pub(crate) const fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    pub(crate) const fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }

    pub(crate) const fn running_at(&self) -> Option<DateTime<Utc>> {
        self.running_at
    }

    pub(crate) const fn next_start_at(&self) -> Option<DateTime<Utc>> {
        self.next_start_at
    }

    pub(crate) const fn metadata(&self) -> &HashMap<String, serde_json::Value> {
        &self.metadata
    }

    pub(crate) fn updated_by_worker_id(&self) -> &str {
        &self.updated_by_worker_id
    }

    // State checks
    pub(crate) const fn is_started(&self) -> bool {
        matches!(self.status, JobStatus::Started)
    }

    pub(crate) const fn is_processed(&self) -> bool {
        matches!(self.status, JobStatus::Completed | JobStatus::Failed)
    }

    pub(crate) const fn is_ready_for_processing(&self) -> bool {
        matches!(self.status, JobStatus::Started | JobStatus::Running)
    }

    pub(crate) fn is_ready_to_next_iteration(&self) -> bool {
        if !matches!(self.status, JobStatus::Completed | JobStatus::Failed) {
            return false;
        }

        if self.is_iteration_limit_reached() {
            return false;
        }

        if let Some(next_start) = self.next_start_at {
            Utc::now() > next_start
        } else {
            true
        }
    }

    // State mutations
    pub(crate) fn update_version(&mut self, version: String) {
        self.version = version;
    }

    pub(crate) fn work(&mut self, worker_id: &str) -> Result<(), JobError> {
        self.status.transition_to(JobStatus::Running)?;
        self.updated_by_worker_id = worker_id.to_string();
        self.running_at = Some(Utc::now());
        Ok(())
    }

    pub(crate) fn try_to_complete(&mut self, worker_id: &str) -> Result<bool, JobError> {
        if !self.all_tasks_completed() {
            return Ok(false);
        }

        self.status.transition_to(JobStatus::Completed)?;
        self.updated_by_worker_id = worker_id.to_string();
        self.completed_at = Some(Utc::now());

        Ok(true)
    }

    #[allow(dead_code)]
    pub(crate) fn fail(&mut self, worker_id: &str) -> Result<(), JobError> {
        self.status.transition_to(JobStatus::Failed)?;
        self.updated_by_worker_id = worker_id.to_string();
        self.completed_at = Some(Utc::now());
        Ok(())
    }

    // Merging. Call this method after worker handled a task.
    pub(crate) fn merge_with_processed_task(
        &mut self,
        worker_job: &Self,
        worker_id: &str,
        task_id: &str,
    ) -> Result<(), JobError> {
        if self.id != worker_job.id {
            return Err(JobError::Other(format!(
                "merge job '{}' failed - IDs are different",
                self.code
            )));
        }

        let exist_task = self.get_task_arc(task_id)?;
        if !exist_task.processing_by_worker().is_empty() && exist_task.processing_by_worker() != worker_id {
            // This can happen when the current worker has lost a task due to a timeout.
            return Err(JobError::TaskWorkerMismatch);
        }

        self.status.transition_to(worker_job.status.clone()).map_err(|e| {
            // TODO(low): its may be ok when job was already saved as completed, but current worker is late with
            // failed task (current worker job status is running)
            JobError::Other(format!("merge job '{}' status failed: {}", self.code, e))
        })?;

        // Merge tasks created or processed by this worker
        // IMPORTANT. Since tasks can be created in the executor, we need to merge of all tasks that the
        // current worker has created or updated.
        for (exist_task_id, task_arc) in &worker_job.tasks_by_id {
            // A task can be created by an executor or a worker can update a task.
            // A task is processed by one worker, but created tasks must be merged too.
            if (task_arc.created_by_worker() == worker_id && task_arc.processing_by_worker().is_empty())
                || task_arc.processing_by_worker() == worker_id
            {
                self.tasks_by_id.insert(exist_task_id.clone(), Arc::clone(task_arc));
            }
        }

        self.updated_by_worker_id = worker_id.to_string();
        if let Some(completed) = worker_job.completed_at {
            self.completed_at = Some(completed);
        }
        if let Some(running) = worker_job.running_at {
            self.running_at = Some(running);
        }

        Ok(())
    }

    pub(crate) const fn is_iteration_limit_reached(&self) -> bool {
        // TODO(low): add a special status that we no longer run the job and remove this check at the
        // complete stage
        self.max_iterations > 0 && self.iter_num >= self.max_iterations
    }

    #[allow(dead_code)]
    pub(crate) fn should_poll(&self) -> bool {
        if let Some(next_start) = self.next_start_at {
            Utc::now() > next_start
        } else {
            true
        }
    }

    pub(crate) fn tasks_as_iter(&self) -> impl Iterator<Item = &Task> {
        self.tasks_by_id.values().map(std::convert::AsRef::as_ref)
    }

    pub(crate) fn get_task(&self, task_id: &str) -> Result<Arc<dyn ImmutableTask>, JobError> {
        self.get_task_arc(task_id).map(|task| Arc::clone(task) as Arc<dyn ImmutableTask>)
    }

    pub(crate) fn get_tasks_by_code(&self, code: &TaskCode) -> Vec<Arc<dyn ImmutableTask>> {
        self.tasks_by_id
            .values()
            .filter(|task| task.code() == code)
            .map(|task| Arc::clone(task) as Arc<dyn ImmutableTask>)
            .collect()
    }

    pub(crate) fn tasks_as_string(&self) -> String {
        let mut summary = String::new();
        let mut count = 0;
        for (id, task) in &self.tasks_by_id {
            summary.push_str(&format!(
                "id: {}; code: {}; status: {}; ",
                id,
                task.code(),
                task.status()
            ));
            count += 1;
            if count > 3 {
                summary.push_str("...");
                break;
            }
        }
        summary
    }

    pub(crate) fn all_tasks_completed(&self) -> bool {
        !self.tasks_by_id.is_empty() && self.tasks_by_id.values().all(|task| task.is_completed())
    }

    fn get_task_arc(&self, task_id: &str) -> Result<&Arc<Task>, JobError> {
        self.tasks_by_id.get(task_id).ok_or(JobError::TaskNotFound)
    }

    fn get_task_arc_mut(&mut self, task_id: &str) -> Result<&mut Arc<Task>, JobError> {
        self.tasks_by_id.get_mut(task_id).ok_or(JobError::TaskNotFound)
    }
}
