use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use uuid::Uuid;

use crate::{Error, ImmutableTask, Job, TaskCode, TaskDefinition};

// TODO(med): add method to complete all job iterations
// TODO(med): add method to complete current job iteration

// JobManager - client interface for task executors
pub trait JobManager: Send + Sync {
    fn add_task(&self, task_def: TaskDefinition) -> Result<Uuid, Error>;
    fn complete_task(&self, task_id: &Uuid, output: Vec<u8>) -> Result<(), Error>;
    fn fail_task(&self, task_id: &Uuid, error_msg: &str) -> Result<(), Error>;
    fn set_next_start_at(&self, next_start_at: DateTime<Utc>) -> Result<(), Error>;
    fn get_task(&self, task_id: &Uuid) -> Result<Arc<dyn ImmutableTask>, Error>;
    fn get_tasks_by_code(&self, code: &TaskCode) -> Result<Vec<Arc<dyn ImmutableTask>>, Error>;
}

// JobManagerImpl - internal implementation
pub(crate) struct JobManagerImpl<'a> {
    job: &'a RwLock<Job>,
    worker_id: Uuid,
}

impl<'a> JobManagerImpl<'a> {
    pub(crate) const fn new(job: &'a RwLock<Job>, worker_id: Uuid) -> Self {
        Self { job, worker_id }
    }
}

impl JobManager for JobManagerImpl<'_> {
    fn add_task(&self, task_def: TaskDefinition) -> Result<Uuid, Error> {
        self.job
            .write()
            .add_task(&task_def, self.worker_id)
            .map_err(|e| Error::Other(e.to_string()))
    }

    fn complete_task(&self, task_id: &Uuid, output: Vec<u8>) -> Result<(), Error> {
        self.job
            .write()
            .complete_task(task_id, output)
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(())
    }

    fn fail_task(&self, task_id: &Uuid, error_msg: &str) -> Result<(), Error> {
        self.job
            .write()
            .fail_task(task_id, error_msg)
            .map_err(|e| Error::Other(e.to_string()))?;
        Ok(())
    }

    fn set_next_start_at(&self, next_start_at: DateTime<Utc>) -> Result<(), Error> {
        self.job.write().set_next_start_at(next_start_at);
        Ok(())
    }

    fn get_task(&self, task_id: &Uuid) -> Result<Arc<dyn ImmutableTask>, Error> {
        self.job.read().get_task(task_id).map_err(|e| Error::Other(e.to_string()))
    }

    fn get_tasks_by_code(&self, code: &TaskCode) -> Result<Vec<Arc<dyn ImmutableTask>>, Error> {
        Ok(self.job.read().get_tasks_by_code(code))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Duration;

    use super::*;
    use crate::{JobCode, TaskLimits};

    #[test]
    fn set_next_start_at_updates_job_under_lock() {
        let task_def = TaskDefinition::new(TaskCode::new("task"), Vec::new(), Duration::seconds(5)).unwrap();
        let job = Job::new(
            JobCode::new("job"),
            vec![task_def],
            HashMap::new(),
            Uuid::from_u128(1),
            None,
            None,
            TaskLimits::default(),
        )
        .unwrap();
        let job = RwLock::new(job);
        let manager = JobManagerImpl::new(&job, Uuid::from_u128(2));

        let next_start_at = Utc::now() + Duration::seconds(30);
        manager.set_next_start_at(next_start_at).unwrap();

        assert_eq!(job.read().next_start_at(), Some(next_start_at));
    }
}
