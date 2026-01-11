use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, ImmutableTask, JobError, Task, TaskCode, TaskDefinition, TaskStatus, registry::TaskExecutorFn};

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
            Self::Completed | Self::Failed => matches!(new, Self::Started),
        };

        if allowed {
            Ok(())
        } else {
            Err(JobError::InvalidStatusTransition {
                from: self.clone(),
                to: new.clone(),
            })
        }
    }

    fn transition_to(&mut self, new: Self) -> Result<(), JobError> {
        self.can_transition_to(&new)?;
        *self = new;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TaskLimits {
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
}

impl Default for TaskLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 100 * 1024 * 1024,
            max_output_bytes: 100 * 1024 * 1024,
        }
    }
}

/// Immutable job definition with initial tasks and executors.
#[derive(Clone)]
pub struct JobDefinition {
    code: JobCode,
    initial_tasks: Vec<TaskDefinition>,
    task_executors: HashMap<TaskCode, TaskExecutorFn>,
    max_iterations: Option<u64>, // None = unlimited
    iteration_interval: Option<Duration>,
    task_limits: TaskLimits,
}

impl JobDefinition {
    pub fn new(
        code: JobCode,
        initial_tasks: Vec<TaskDefinition>,
        task_executors: HashMap<TaskCode, TaskExecutorFn>,
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
            max_iterations: None,
            iteration_interval: None,
            task_limits: TaskLimits::default(),
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

    pub const fn max_iterations(&self) -> Option<u64> {
        self.max_iterations
    }

    pub const fn iteration_interval(&self) -> Option<Duration> {
        self.iteration_interval
    }

    pub const fn task_limits(&self) -> TaskLimits {
        self.task_limits
    }

    pub fn with_max_iterations(mut self, max_iterations: u64) -> Result<Self, Error> {
        if max_iterations == 0 {
            return Err(Error::Other("job max iterations must be positive".into()));
        }
        self.max_iterations = Some(max_iterations);
        Ok(self)
    }

    pub fn with_iteration_interval(mut self, iteration_interval: Duration) -> Result<Self, Error> {
        if iteration_interval <= Duration::zero() {
            return Err(Error::Other("job iteration interval must be positive".into()));
        }
        self.iteration_interval = Some(iteration_interval);
        Ok(self)
    }

    #[must_use]
    pub const fn with_task_limits(mut self, task_limits: TaskLimits) -> Self {
        self.task_limits = task_limits;
        self
    }
}

#[derive(Clone)]
pub(crate) struct Job {
    id: Uuid,
    code: JobCode,
    iter_num: u64,
    status: JobStatus,
    tasks_by_id: HashMap<Uuid, Arc<Task>>, // Arc makes cloning cheap - only pointer is cloned
    updated_by_worker_id: Uuid,
    started_at: DateTime<Utc>,
    running_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    next_start_at: Option<DateTime<Utc>>,
    metadata: HashMap<String, serde_json::Value>,
    version: String,
    max_iterations: Option<u64>, // None = unlimited, from JobDefinition TODO(low): extract to new settings structure
    iteration_interval: Option<Duration>,
    task_limits: TaskLimits,
}

impl Job {
    pub(crate) fn new(
        code: JobCode,
        task_defs: Vec<TaskDefinition>,
        metadata: HashMap<String, serde_json::Value>,
        worker_id: Uuid,
        max_iterations: Option<u64>,
        iteration_interval: Option<Duration>,
        task_limits: TaskLimits,
    ) -> Result<Self, JobError> {
        let mut tasks_by_id = HashMap::new();
        for task_def in task_defs {
            Self::validate_task_input(task_def.input(), task_limits)?;
            let task = Task::new(worker_id, &task_def);
            tasks_by_id.insert(*task.id(), Arc::new(task));
        }

        Ok(Self {
            id: Uuid::new_v4(),
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
            iteration_interval,
            task_limits,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        id: Uuid,
        code: JobCode,
        version: String,
        iter_num: u64,
        status: JobStatus,
        tasks: Vec<Task>,
        updated_by_worker_id: Uuid,
        started_at: DateTime<Utc>,
        running_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        next_start_at: Option<DateTime<Utc>>,
        metadata: HashMap<String, serde_json::Value>,
        max_iterations: Option<u64>,
        iteration_interval: Option<Duration>,
        task_limits: TaskLimits,
    ) -> Self {
        let mut tasks_by_id = HashMap::new();
        for task in tasks {
            tasks_by_id.insert(*task.id(), Arc::new(task));
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
            iteration_interval,
            task_limits,
        }
    }

    // Prepares the job for the next iteration
    pub(crate) fn next_iteration(&mut self, task_defs: Vec<TaskDefinition>, worker_id: Uuid) -> Result<(), JobError> {
        if !self.is_ready_to_next_iteration() {
            return Err(JobError::Other("job is not ready to next iteration".into()));
        }

        self.status.transition_to(JobStatus::Started)?;

        let old_id = self.id;
        let old_iter_num = self.iter_num;
        let old_metadata = self.metadata.clone();

        *self = Self::new(
            self.code.clone(),
            task_defs,
            old_metadata,
            worker_id,
            self.max_iterations,
            self.iteration_interval,
            self.task_limits,
        )?;
        self.id = old_id;
        // TODO(low): in the future, a mechanism for restarting the sequence is needed (currently the maximum sequence is 10^20).
        // Sequential uuid will not work, as there may be a race when creating a new job by different workers.
        self.iter_num = old_iter_num + 1;
        self.started_at = Utc::now();

        Ok(())
    }

    pub(crate) fn add_task(&mut self, task_def: &TaskDefinition, worker_id: Uuid) -> Result<Uuid, JobError> {
        Self::validate_task_input(task_def.input(), self.task_limits)?;
        let task = Task::new(worker_id, task_def);

        if self.tasks_by_id.contains_key(task.id()) {
            return Err(JobError::Other(format!(
                "task with id {} already registered in job",
                task.id()
            )));
        }

        // Validate dependencies exist
        for dep_id in task_def.depends_on() {
            if !self.tasks_by_id.contains_key(dep_id) {
                return Err(JobError::Other(format!("dependency task '{dep_id}' not found")));
            }
        }

        let task_id = *task.id();
        self.tasks_by_id.insert(task_id, Arc::new(task));
        Ok(task_id)
    }

    pub(crate) fn start_task(&mut self, task_id: &Uuid, worker_id: Uuid) -> Result<(), JobError> {
        let task_arc = self.get_task_arc_mut(task_id)?;

        let task = Arc::make_mut(task_arc); // Copy on write: clone only if refcount > 1
        task.start(worker_id)?;
        self.updated_by_worker_id = worker_id;

        Ok(())
    }

    pub(crate) fn complete_task(&mut self, task_id: &Uuid, output: Vec<u8>) -> Result<(), JobError> {
        Self::validate_task_output(&output, self.task_limits)?;
        let task_arc = self.get_task_arc_mut(task_id)?;

        let task = Arc::make_mut(task_arc);
        task.complete(output)
    }

    pub(crate) fn fail_task(&mut self, task_id: &Uuid, error_msg: &str) -> Result<(), JobError> {
        let task_arc = self.get_task_arc_mut(task_id)?;

        let task = Arc::make_mut(task_arc);
        task.fail(error_msg)
    }

    pub(crate) fn pick_task_to_execute(&mut self, worker_id: &Uuid) -> Result<Option<Uuid>, JobError> {
        if !matches!(self.status, JobStatus::Running) {
            self.work(worker_id)?;
        }

        // TODO(low): with a large number of tasks in the job, iteration can add overhead. Solution: pending tasks can be cached.
        // Since map iteration is randomized, no additional randomization is needed.
        let mut blocked_to_unblock: Option<Uuid> = None;
        for (task_id, task_arc) in &self.tasks_by_id {
            let status = task_arc.status();

            match status {
                TaskStatus::Completed => {}
                TaskStatus::Blocked => {
                    if !self.dependencies_satisfied(task_arc.as_ref()) {
                        continue;
                    }
                    blocked_to_unblock = Some(*task_id);
                    break;
                }
                TaskStatus::Todo | TaskStatus::Failed | TaskStatus::Started => {
                    if task_arc.can_be_picked_up() {
                        return Ok(Some(*task_id));
                    }
                }
            }
        }

        if let Some(task_id) = blocked_to_unblock {
            let task_arc = self.get_task_arc_mut(&task_id)?;
            let task = Arc::make_mut(task_arc);
            task.unblock();
            if task.can_be_picked_up() {
                return Ok(Some(task_id));
            }
        }

        if self.all_tasks_completed() {
            return Err(JobError::Other(format!(
                "wrong running job {} state - all tasks complete",
                self.code
            )));
        }

        if self.is_deadlocked() {
            return Err(JobError::Other(format!(
                "job {} deadlock: blocked tasks with unmet dependencies",
                self.code
            )));
        }

        Ok(None)
    }

    // Accessors
    pub(crate) const fn id(&self) -> &Uuid {
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

    pub(crate) const fn updated_by_worker_id(&self) -> Uuid {
        self.updated_by_worker_id
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

        if let Some(interval) = self.iteration_interval {
            if Utc::now() < self.started_at + interval {
                return false;
            }
        }

        self.next_start_at.map_or(true, |next_start| Utc::now() > next_start)
    }

    // State mutations
    pub(crate) fn update_version(&mut self, version: String) {
        self.version = version;
    }

    fn validate_task_input(input: &[u8], task_limits: TaskLimits) -> Result<(), JobError> {
        if input.len() > task_limits.max_input_bytes {
            return Err(JobError::Other(format!(
                "task input size {} exceeds limit {}",
                input.len(),
                task_limits.max_input_bytes
            )));
        }
        Ok(())
    }

    fn validate_task_output(output: &[u8], task_limits: TaskLimits) -> Result<(), JobError> {
        if output.len() > task_limits.max_output_bytes {
            return Err(JobError::Other(format!(
                "task output size {} exceeds limit {}",
                output.len(),
                task_limits.max_output_bytes
            )));
        }
        Ok(())
    }

    pub(crate) fn work(&mut self, worker_id: &Uuid) -> Result<(), JobError> {
        self.status.transition_to(JobStatus::Running)?;
        self.updated_by_worker_id = *worker_id;
        self.running_at = Some(Utc::now());
        Ok(())
    }

    pub(crate) fn try_to_complete(&mut self, worker_id: &Uuid) -> Result<bool, JobError> {
        if !self.all_tasks_completed() {
            return Ok(false);
        }

        self.status.transition_to(JobStatus::Completed)?;
        self.updated_by_worker_id = *worker_id;
        self.completed_at = Some(Utc::now());

        Ok(true)
    }

    #[allow(dead_code)]
    pub(crate) fn fail(&mut self, worker_id: &Uuid) -> Result<(), JobError> {
        self.status.transition_to(JobStatus::Failed)?;
        self.updated_by_worker_id = *worker_id;
        self.completed_at = Some(Utc::now());
        Ok(())
    }

    // Merging. Call this method after worker handled a task.
    pub(crate) fn merge_with_processed_task(
        &mut self,
        worker_job: &Self,
        worker_id: &Uuid,
        task_id: &Uuid,
    ) -> Result<(), JobError> {
        if self.id != worker_job.id {
            return Err(JobError::Other(format!(
                "merge job '{}' failed - IDs are different",
                self.code
            )));
        }

        let exist_task = self.get_task_arc(task_id)?;
        if exist_task.processing_by_worker().is_some() && exist_task.processing_by_worker() != Some(*worker_id) {
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
            if (task_arc.created_by_worker() == *worker_id && task_arc.processing_by_worker().is_none())
                || task_arc.processing_by_worker() == Some(*worker_id)
            {
                self.tasks_by_id.insert(*exist_task_id, Arc::clone(task_arc));
            }
        }

        self.updated_by_worker_id = *worker_id;
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
        match self.max_iterations {
            Some(max_iterations) => self.iter_num >= max_iterations,
            None => false,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn should_poll(&self) -> bool {
        self.next_start_at.map_or(true, |next_start| Utc::now() > next_start)
    }

    pub(crate) fn tasks_as_iter(&self) -> impl Iterator<Item = &Task> {
        self.tasks_by_id.values().map(std::convert::AsRef::as_ref)
    }

    pub(crate) fn get_task(&self, task_id: &Uuid) -> Result<Arc<dyn ImmutableTask>, JobError> {
        self.get_task_arc(task_id)
            .map(|task| Arc::clone(task) as Arc<dyn ImmutableTask>)
    }

    pub(crate) fn get_tasks_by_code(&self, code: &TaskCode) -> Vec<Arc<dyn ImmutableTask>> {
        self.tasks_by_id
            .values()
            .filter(|task| task.code() == code)
            .map(|task| Arc::clone(task) as Arc<dyn ImmutableTask>)
            .collect()
    }

    pub(crate) fn tasks_as_string(&self) -> String {
        use std::fmt::Write as _;
        let mut summary = String::new();
        let mut count = 0;
        for (id, task) in &self.tasks_by_id {
            let _ = write!(
                summary,
                "id: {}; code: {}; status: {}; ",
                id,
                task.code(),
                task.status()
            );
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

    fn dependencies_satisfied(&self, task: &Task) -> bool {
        task.depends_on()
            .iter()
            .all(|dep_id| self.tasks_by_id.get(dep_id).is_some_and(|t| t.is_completed()))
    }

    fn is_deadlocked(&self) -> bool {
        let has_blocked = self
            .tasks_by_id
            .values()
            .any(|task| matches!(task.status(), TaskStatus::Blocked) && !self.dependencies_satisfied(task));
        let has_started = self
            .tasks_by_id
            .values()
            .any(|task| matches!(task.status(), TaskStatus::Started));
        has_blocked && !has_started
    }

    fn get_task_arc(&self, task_id: &Uuid) -> Result<&Arc<Task>, JobError> {
        self.tasks_by_id.get(task_id).ok_or(JobError::TaskNotFound)
    }

    fn get_task_arc_mut(&mut self, task_id: &Uuid) -> Result<&mut Arc<Task>, JobError> {
        self.tasks_by_id.get_mut(task_id).ok_or(JobError::TaskNotFound)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use chrono::{DateTime, Duration, Utc};
    use uuid::Uuid;

    use super::*;

    fn make_task(id: Uuid, code: &str, status: TaskStatus, depends_on: Vec<Uuid>) -> Task {
        Task::restore(
            id,
            TaskCode::new(code),
            status,
            None,
            Uuid::new_v4(),
            Duration::seconds(5),
            None,
            None,
            None,
            0,
            Vec::new(),
            Vec::new(),
            String::new(),
            depends_on,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn restore_job(
        id: Uuid,
        status: JobStatus,
        tasks: Vec<Task>,
        iter_num: u64,
        max_iterations: Option<u64>,
        iteration_interval: Option<Duration>,
        updated_by_worker_id: Uuid,
        running_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        next_start_at: Option<DateTime<Utc>>,
        metadata: HashMap<String, serde_json::Value>,
    ) -> Job {
        Job::restore(
            id,
            JobCode::new("job"),
            String::new(),
            iter_num,
            status,
            tasks,
            updated_by_worker_id,
            Utc::now(),
            running_at,
            completed_at,
            next_start_at,
            metadata,
            max_iterations,
            iteration_interval,
            TaskLimits::default(),
        )
    }

    #[test]
    fn test_pick_task_to_execute_todo() {
        let task_id = Uuid::from_u128(1);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(101),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, Some(task_id));
    }

    #[test]
    fn test_pick_task_to_execute_blocked_with_unmet_deps() {
        let shift_id = Uuid::from_u128(2);
        let commit_id = Uuid::from_u128(3);
        let shift = make_task(shift_id, "shift", TaskStatus::Todo, Vec::new());
        let commit = make_task(commit_id, "commit", TaskStatus::Blocked, vec![shift_id]);
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(102),
            JobStatus::Started,
            vec![shift, commit],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, Some(shift_id));
        let commit_status = job.get_task_arc(&commit_id).unwrap().status().clone();
        assert_eq!(commit_status, TaskStatus::Blocked);
    }

    #[test]
    fn test_pick_task_to_execute_unblocks_when_deps_complete() {
        let shift_id = Uuid::from_u128(4);
        let commit_id = Uuid::from_u128(5);
        let shift = make_task(shift_id, "shift", TaskStatus::Completed, Vec::new());
        let commit = make_task(commit_id, "commit", TaskStatus::Blocked, vec![shift_id]);
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(103),
            JobStatus::Started,
            vec![shift, commit],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, Some(commit_id));
        let commit_status = job.get_task_arc(&commit_id).unwrap().status().clone();
        assert_eq!(commit_status, TaskStatus::Todo);
    }

    #[test]
    fn test_pick_task_to_execute_returns_none_when_no_pickable() {
        let started = make_task(Uuid::from_u128(6), "started", TaskStatus::Started, Vec::new());
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(104),
            JobStatus::Started,
            vec![started],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert!(picked.is_none());
    }

    #[test]
    fn test_pick_task_to_execute_failed_task() {
        let failed_id = Uuid::from_u128(7);
        let failed = make_task(failed_id, "failed", TaskStatus::Failed, Vec::new());
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(105),
            JobStatus::Started,
            vec![failed],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, Some(failed_id));
    }

    #[test]
    fn test_pick_task_to_execute_expired_started_task() {
        let expired = Task::restore(
            Uuid::from_u128(8),
            TaskCode::new("expired"),
            TaskStatus::Started,
            None,
            Uuid::from_u128(101),
            Duration::seconds(5),
            Some(Utc::now() - Duration::seconds(10)),
            None,
            Some(Utc::now() - Duration::seconds(1)),
            0,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );

        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(106),
            JobStatus::Started,
            vec![expired],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let picked = job.pick_task_to_execute(&worker_id).unwrap();
        assert_eq!(picked, Some(Uuid::from_u128(8)));
    }

    #[test]
    fn test_pick_task_to_execute_all_tasks_completed_error() {
        let completed = make_task(Uuid::from_u128(9), "done", TaskStatus::Completed, Vec::new());
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(107),
            JobStatus::Started,
            vec![completed],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.pick_task_to_execute(&worker_id).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_pick_task_to_execute_deadlock_blocked_error() {
        let commit = make_task(
            Uuid::from_u128(10),
            "commit",
            TaskStatus::Blocked,
            vec![Uuid::from_u128(11)],
        );
        let worker_id = Uuid::from_u128(100);
        let mut job = restore_job(
            Uuid::from_u128(108),
            JobStatus::Started,
            vec![commit],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.pick_task_to_execute(&worker_id).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_next_iteration_success() {
        let old_task_id = Uuid::from_u128(20);
        let old_task = make_task(old_task_id, "old", TaskStatus::Completed, Vec::new());
        let mut metadata = HashMap::new();
        metadata.insert("key".to_string(), serde_json::Value::String("value".to_string()));
        let job_id = Uuid::from_u128(21);
        let worker_id = Uuid::from_u128(22);

        let mut job = restore_job(
            job_id,
            JobStatus::Completed,
            vec![old_task],
            3,
            Some(5),
            None,
            Uuid::from_u128(200),
            None,
            Some(Utc::now()),
            None,
            metadata.clone(),
        );

        let new_task = TaskDefinition::new(TaskCode::new("new"), Vec::new(), Duration::seconds(5)).unwrap();
        let before = Utc::now();
        job.next_iteration(vec![new_task], worker_id).unwrap();

        assert_eq!(job.id, job_id);
        assert_eq!(job.iter_num, 4);
        assert!(matches!(job.status, JobStatus::Started));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert_eq!(job.max_iterations, Some(5));
        assert_eq!(job.metadata, metadata);
        assert!(job.started_at >= before);
        assert_eq!(job.tasks_by_id.len(), 1);
        assert!(!job.tasks_by_id.contains_key(&old_task_id));
    }

    #[test]
    fn test_next_iteration_not_ready_status() {
        let task = make_task(Uuid::from_u128(30), "todo", TaskStatus::Todo, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(31),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(300),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let err = job.next_iteration(Vec::new(), Uuid::from_u128(301)).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_next_iteration_limit_reached() {
        let task = make_task(Uuid::from_u128(32), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(33),
            JobStatus::Completed,
            vec![task],
            2,
            Some(2),
            None,
            Uuid::from_u128(302),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        let err = job.next_iteration(Vec::new(), Uuid::from_u128(303)).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_next_iteration_next_start_in_future() {
        let task = make_task(Uuid::from_u128(34), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(35),
            JobStatus::Completed,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(304),
            None,
            Some(Utc::now()),
            Some(Utc::now() + Duration::seconds(60)),
            HashMap::new(),
        );

        let err = job.next_iteration(Vec::new(), Uuid::from_u128(305)).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_next_iteration_interval_not_elapsed() {
        let task = make_task(Uuid::from_u128(36), "done", TaskStatus::Completed, Vec::new());
        let job = Job::restore(
            Uuid::from_u128(37),
            JobCode::new("job"),
            String::new(),
            1,
            JobStatus::Completed,
            vec![task],
            Uuid::from_u128(306),
            Utc::now(),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
            None,
            Some(Duration::seconds(60)),
            TaskLimits::default(),
        );

        assert!(!job.is_ready_to_next_iteration());
    }

    #[test]
    fn test_next_iteration_resets_timestamps_and_next_start() {
        let old_task_id = Uuid::from_u128(36);
        let old_task = make_task(old_task_id, "old", TaskStatus::Completed, Vec::new());
        let job_id = Uuid::from_u128(37);
        let worker_id = Uuid::from_u128(38);

        let mut job = restore_job(
            job_id,
            JobStatus::Completed,
            vec![old_task],
            1,
            None,
            None,
            Uuid::from_u128(300),
            Some(Utc::now() - Duration::seconds(5)),
            Some(Utc::now() - Duration::seconds(1)),
            Some(Utc::now() - Duration::seconds(1)),
            HashMap::new(),
        );

        let new_task = TaskDefinition::new(TaskCode::new("new"), Vec::new(), Duration::seconds(5)).unwrap();
        job.next_iteration(vec![new_task], worker_id).unwrap();

        assert_eq!(job.id, job_id);
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(job.running_at.is_none());
        assert!(job.completed_at.is_none());
        assert!(job.next_start_at.is_none());
    }

    #[test]
    fn test_next_iteration_from_failed_status() {
        let old_task_id = Uuid::from_u128(40);
        let old_task = make_task(old_task_id, "old", TaskStatus::Failed, Vec::new());
        let job_id = Uuid::from_u128(41);
        let worker_id = Uuid::from_u128(42);

        let mut job = restore_job(
            job_id,
            JobStatus::Failed,
            vec![old_task],
            2,
            None,
            None,
            Uuid::from_u128(310),
            Some(Utc::now() - Duration::seconds(5)),
            Some(Utc::now() - Duration::seconds(1)),
            None,
            HashMap::new(),
        );

        let new_task = TaskDefinition::new(TaskCode::new("new"), Vec::new(), Duration::seconds(5)).unwrap();
        job.next_iteration(vec![new_task], worker_id).unwrap();

        assert_eq!(job.id, job_id);
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(matches!(job.status, JobStatus::Started));
    }

    #[test]
    fn test_add_task_with_dependencies_ok() {
        let dep_id = Uuid::from_u128(40);
        let dep_task = make_task(dep_id, "dep", TaskStatus::Completed, Vec::new());
        let worker_id = Uuid::from_u128(400);
        let mut job = restore_job(
            Uuid::from_u128(401),
            JobStatus::Started,
            vec![dep_task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let task_def = TaskDefinition::new(TaskCode::new("child"), vec![1, 2, 3], Duration::seconds(5))
            .unwrap()
            .with_dependencies(vec![dep_id]);
        let task_id = job.add_task(&task_def, Uuid::from_u128(401)).unwrap();

        let task = job.get_task_arc(&task_id).unwrap();
        assert!(matches!(task.status(), TaskStatus::Blocked));
        assert_eq!(task.depends_on(), vec![dep_id]);
    }

    #[test]
    fn test_add_task_missing_dependency() {
        let task = make_task(Uuid::from_u128(41), "root", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(402);
        let mut job = restore_job(
            Uuid::from_u128(403),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let task_def = TaskDefinition::new(TaskCode::new("child"), Vec::new(), Duration::seconds(5))
            .unwrap()
            .with_dependencies(vec![Uuid::from_u128(999)]);
        let err = job.add_task(&task_def, Uuid::from_u128(403)).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_start_task_ok() {
        let task_id = Uuid::from_u128(50);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(500);
        let mut job = restore_job(
            Uuid::from_u128(501),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(501);
        job.start_task(&task_id, worker_id).unwrap();
        let task = job.get_task_arc(&task_id).unwrap();
        assert!(matches!(task.status(), TaskStatus::Started));
        assert_eq!(task.processing_by_worker(), Some(worker_id));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert_eq!(task.attempt(), 1);
    }

    #[test]
    fn test_start_task_not_found() {
        let task = make_task(Uuid::from_u128(51), "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(502);
        let mut job = restore_job(
            Uuid::from_u128(503),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.start_task(&Uuid::from_u128(999), Uuid::from_u128(503)).unwrap_err();
        assert!(matches!(err, JobError::TaskNotFound));
    }

    #[test]
    fn test_start_task_worker_mismatch() {
        let task_id = Uuid::from_u128(52);
        let task = Task::restore(
            task_id,
            TaskCode::new("started"),
            TaskStatus::Started,
            Some(Uuid::from_u128(600)),
            Uuid::from_u128(601),
            Duration::seconds(5),
            Some(Utc::now()),
            None,
            Some(Utc::now() + Duration::seconds(60)),
            1,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let worker_id = Uuid::from_u128(504);
        let mut job = restore_job(
            Uuid::from_u128(505),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.start_task(&task_id, Uuid::from_u128(602)).unwrap_err();
        assert!(matches!(err, JobError::TaskWorkerMismatch));
    }

    #[test]
    fn test_complete_task_ok() {
        let task_id = Uuid::from_u128(60);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(600);
        let mut job = restore_job(
            Uuid::from_u128(601),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        job.start_task(&task_id, Uuid::from_u128(601)).unwrap();
        job.complete_task(&task_id, vec![1, 2]).unwrap();
        let task = job.get_task_arc(&task_id).unwrap();
        assert!(matches!(task.status(), TaskStatus::Completed));
        assert_eq!(task.output(), vec![1, 2]);
        assert!(task.completed_at().is_some());
    }

    #[test]
    fn test_complete_task_wrong_status() {
        let task_id = Uuid::from_u128(61);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(602);
        let mut job = restore_job(
            Uuid::from_u128(603),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.complete_task(&task_id, vec![1]).unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_fail_task_ok() {
        let task_id = Uuid::from_u128(70);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(700);
        let mut job = restore_job(
            Uuid::from_u128(701),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        job.start_task(&task_id, Uuid::from_u128(701)).unwrap();
        job.fail_task(&task_id, "boom").unwrap();
        let task = job.get_task_arc(&task_id).unwrap();
        assert!(matches!(task.status(), TaskStatus::Failed));
        assert_eq!(task.error_msg(), "boom");
        assert!(task.completed_at().is_some());
    }

    #[test]
    fn test_fail_task_wrong_status() {
        let task_id = Uuid::from_u128(71);
        let task = make_task(task_id, "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(702);
        let mut job = restore_job(
            Uuid::from_u128(703),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.fail_task(&task_id, "boom").unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_work_ok() {
        let task = make_task(Uuid::from_u128(80), "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(800);
        let mut job = restore_job(
            Uuid::from_u128(801),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(801);
        job.work(&worker_id).unwrap();
        assert!(matches!(job.status, JobStatus::Running));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(job.running_at.is_some());
    }

    #[test]
    fn test_work_invalid_transition() {
        let task = make_task(Uuid::from_u128(81), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(82),
            JobStatus::Completed,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(802),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        let err = job.work(&Uuid::from_u128(803)).unwrap_err();
        assert!(matches!(err, JobError::InvalidStatusTransition { .. }));
    }

    #[test]
    fn test_try_to_complete_ok() {
        let task = make_task(Uuid::from_u128(90), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(91),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(900),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(901);
        let completed = job.try_to_complete(&worker_id).unwrap();
        assert!(completed);
        assert!(matches!(job.status, JobStatus::Completed));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(job.completed_at.is_some());
    }

    #[test]
    fn test_try_to_complete_not_ready() {
        let task = make_task(Uuid::from_u128(92), "todo", TaskStatus::Todo, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(93),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(902),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let completed = job.try_to_complete(&Uuid::from_u128(903)).unwrap();
        assert!(!completed);
        assert!(matches!(job.status, JobStatus::Running));
        assert!(job.completed_at.is_none());
    }

    #[test]
    fn test_try_to_complete_invalid_transition() {
        let task = make_task(Uuid::from_u128(94), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(95),
            JobStatus::Started,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(904),
            None,
            None,
            None,
            HashMap::new(),
        );

        let err = job.try_to_complete(&Uuid::from_u128(905)).unwrap_err();
        assert!(matches!(err, JobError::InvalidStatusTransition { .. }));
    }

    #[test]
    fn test_fail_ok() {
        let task = make_task(Uuid::from_u128(100), "todo", TaskStatus::Todo, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(101),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1000),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(1001);
        job.fail(&worker_id).unwrap();
        assert!(matches!(job.status, JobStatus::Failed));
        assert_eq!(job.updated_by_worker_id, worker_id);
        assert!(job.completed_at.is_some());
    }

    #[test]
    fn test_fail_invalid_transition() {
        let task = make_task(Uuid::from_u128(102), "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(103),
            JobStatus::Completed,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1002),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        let err = job.fail(&Uuid::from_u128(1003)).unwrap_err();
        assert!(matches!(err, JobError::InvalidStatusTransition { .. }));
    }

    #[test]
    fn test_merge_with_processed_task_different_id() {
        let task = make_task(Uuid::from_u128(110), "todo", TaskStatus::Todo, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(111),
            JobStatus::Running,
            vec![task.clone()],
            1,
            None,
            None,
            Uuid::from_u128(1100),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );
        let worker_job = restore_job(
            Uuid::from_u128(112),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1101),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let err = job
            .merge_with_processed_task(&worker_job, &Uuid::from_u128(1101), &Uuid::from_u128(110))
            .unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_merge_with_processed_task_worker_mismatch() {
        let task_id = Uuid::from_u128(120);
        let task = Task::restore(
            task_id,
            TaskCode::new("started"),
            TaskStatus::Started,
            Some(Uuid::from_u128(1200)),
            Uuid::from_u128(1201),
            Duration::seconds(5),
            Some(Utc::now()),
            None,
            Some(Utc::now() + Duration::seconds(60)),
            1,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let mut job = restore_job(
            Uuid::from_u128(121),
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1202),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );
        let worker_job = job.clone();

        let err = job
            .merge_with_processed_task(&worker_job, &Uuid::from_u128(1203), &task_id)
            .unwrap_err();
        assert!(matches!(err, JobError::TaskWorkerMismatch));
    }

    #[test]
    fn test_merge_with_processed_task_ok() {
        let base_task_id = Uuid::from_u128(130);
        let base_task = make_task(base_task_id, "base", TaskStatus::Started, Vec::new());

        let mut job = restore_job(
            Uuid::from_u128(131),
            JobStatus::Running,
            vec![base_task],
            1,
            None,
            None,
            Uuid::from_u128(1300),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let worker_id = Uuid::from_u128(1301);
        let created_task_id = Uuid::from_u128(132);
        let created_task = Task::restore(
            created_task_id,
            TaskCode::new("created"),
            TaskStatus::Todo,
            None,
            worker_id,
            Duration::seconds(5),
            None,
            None,
            None,
            0,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let processed_task_id = Uuid::from_u128(133);
        let processed_task = Task::restore(
            processed_task_id,
            TaskCode::new("processed"),
            TaskStatus::Started,
            Some(worker_id),
            Uuid::from_u128(1302),
            Duration::seconds(5),
            Some(Utc::now()),
            None,
            Some(Utc::now() + Duration::seconds(60)),
            1,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );
        let other_task_id = Uuid::from_u128(134);
        let other_task = Task::restore(
            other_task_id,
            TaskCode::new("other"),
            TaskStatus::Todo,
            None,
            Uuid::from_u128(1303),
            Duration::seconds(5),
            None,
            None,
            None,
            0,
            Vec::new(),
            Vec::new(),
            String::new(),
            Vec::new(),
        );

        let worker_job = restore_job(
            job.id,
            JobStatus::Completed,
            vec![created_task, processed_task, other_task],
            1,
            None,
            None,
            worker_id,
            Some(Utc::now()),
            Some(Utc::now()),
            None,
            HashMap::new(),
        );

        job.merge_with_processed_task(&worker_job, &worker_id, &base_task_id).unwrap();

        assert!(matches!(job.status, JobStatus::Completed));
        assert!(job.completed_at.is_some());
        assert!(job.running_at.is_some());
        assert!(job.tasks_by_id.contains_key(&base_task_id));
        assert!(job.tasks_by_id.contains_key(&created_task_id));
        assert!(job.tasks_by_id.contains_key(&processed_task_id));
        assert!(!job.tasks_by_id.contains_key(&other_task_id));
    }

    #[test]
    fn test_merge_with_processed_task_invalid_transition() {
        let task_id = Uuid::from_u128(140);
        let task = make_task(task_id, "done", TaskStatus::Completed, Vec::new());
        let mut job = restore_job(
            Uuid::from_u128(141),
            JobStatus::Completed,
            vec![task.clone()],
            1,
            None,
            None,
            Uuid::from_u128(1400),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
        );
        let worker_job = restore_job(
            job.id,
            JobStatus::Running,
            vec![task],
            1,
            None,
            None,
            Uuid::from_u128(1401),
            Some(Utc::now()),
            None,
            None,
            HashMap::new(),
        );

        let err = job
            .merge_with_processed_task(&worker_job, &Uuid::from_u128(1401), &task_id)
            .unwrap_err();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_update_version() {
        let task = make_task(Uuid::from_u128(150), "todo", TaskStatus::Todo, Vec::new());
        let worker_id = Uuid::from_u128(1500);
        let mut job = restore_job(
            Uuid::from_u128(1501),
            JobStatus::Started,
            vec![task],
            1,
            Some(1),
            None,
            worker_id,
            None,
            None,
            None,
            HashMap::new(),
        );

        job.update_version("v1".to_string());
        assert_eq!(job.version(), "v1");
    }

    #[test]
    fn test_task_limits_default() {
        let limits = TaskLimits::default();
        assert!(limits.max_input_bytes > 100);
        assert!(limits.max_output_bytes > 100);
    }

    #[test]
    fn test_job_definition_with_task_limits() {
        let mut executors = HashMap::new();
        let executor: crate::registry::TaskExecutorFn = Arc::new(|_, _, _| Box::pin(async { Ok(()) }));
        executors.insert(TaskCode::new("noop"), executor);
        let task_def = TaskDefinition::new(TaskCode::new("noop"), Vec::new(), Duration::seconds(5)).unwrap();
        let limits = TaskLimits {
            max_input_bytes: 1,
            max_output_bytes: 2,
        };

        let job_def = JobDefinition::new(JobCode::new("job"), vec![task_def], executors)
            .unwrap()
            .with_task_limits(limits);

        assert_eq!(job_def.task_limits().max_input_bytes, 1);
        assert_eq!(job_def.task_limits().max_output_bytes, 2);
    }

    #[test]
    fn test_job_new_rejects_oversized_input() {
        let task_def = TaskDefinition::new(TaskCode::new("too_big"), vec![0; 5], Duration::seconds(5)).unwrap();
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 10,
        };

        let err = Job::new(
            JobCode::new("job"),
            vec![task_def],
            HashMap::new(),
            Uuid::from_u128(1600),
            None,
            None,
            limits,
        )
        .err()
        .unwrap();

        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_add_task_rejects_oversized_input() {
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 10,
        };
        let init_def = TaskDefinition::new(TaskCode::new("init"), vec![0; 1], Duration::seconds(5)).unwrap();
        let mut job = Job::new(
            JobCode::new("job"),
            vec![init_def],
            HashMap::new(),
            Uuid::from_u128(1700),
            None,
            None,
            limits,
        )
        .unwrap();

        let task_def = TaskDefinition::new(TaskCode::new("child"), vec![0; 5], Duration::seconds(5)).unwrap();
        let err = job.add_task(&task_def, Uuid::from_u128(1701)).err().unwrap();
        assert!(matches!(err, JobError::Other(_)));
    }

    #[test]
    fn test_complete_task_rejects_oversized_output() {
        let limits = TaskLimits {
            max_input_bytes: 4,
            max_output_bytes: 4,
        };
        let init_def = TaskDefinition::new(TaskCode::new("init"), vec![0; 1], Duration::seconds(5)).unwrap();
        let mut job = Job::new(
            JobCode::new("job"),
            vec![init_def],
            HashMap::new(),
            Uuid::from_u128(1800),
            None,
            None,
            limits,
        )
        .unwrap();

        let task_id = *job.tasks_as_iter().next().unwrap().id();
        job.start_task(&task_id, Uuid::from_u128(1801)).unwrap();

        let err = job.complete_task(&task_id, vec![0; 5]).err().unwrap();
        assert!(matches!(err, JobError::Other(_)));
    }
}
