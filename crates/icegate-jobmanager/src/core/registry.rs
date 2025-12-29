use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use tokio_util::sync::CancellationToken;

use crate::{Error, ImmutableTask, JobCode, JobDefinition, JobDefinitionRegistry, JobManager, TaskCode};

/// Task executor function signature.
///
/// JobManager is borrowed so executors can't move it into background tasks.
/// CancellationToken allows executor to stop early on shutdown.
pub type TaskExecutorFn = Arc<
    dyn for<'a> Fn(
            Arc<dyn ImmutableTask>,
            &'a dyn JobManager,
            CancellationToken,
        ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'a>>
        + Send
        + Sync,
>;

/// Immutable collection of job definitions and task executors.
#[derive(Clone)]
pub struct JobRegistry {
    jobs_by_code: HashMap<JobCode, JobDefinition>,
    task_executors_by_key: HashMap<String, TaskExecutorFn>,
}

impl JobRegistry {
    pub fn new(jobs: Vec<JobDefinition>) -> Result<Self, Error> {
        if jobs.is_empty() {
            return Err(Error::Other("jobs cannot be empty".into()));
        }

        let mut jobs_by_code = HashMap::with_capacity(jobs.len());
        let mut task_executors_by_key = HashMap::new();

        for job_def in jobs {
            let job_code = job_def.code().clone();
            if job_code.as_str().is_empty() {
                return Err(Error::Other("job code cannot be empty".into()));
            }
            if jobs_by_code.contains_key(&job_code) {
                return Err(Error::Other(format!("job {} has duplicate", job_code)));
            }

            for (task_code, executor) in job_def.task_executors() {
                let key = Self::task_executor_key(&job_code, task_code);
                task_executors_by_key.insert(key, executor.clone());
            }

            jobs_by_code.insert(job_code, job_def);
        }

        Ok(Self {
            jobs_by_code,
            task_executors_by_key,
        })
    }

    pub(crate) fn get_job(&self, code: &JobCode) -> Result<JobDefinition, Error> {
        self.jobs_by_code
            .get(code)
            .cloned()
            .ok_or_else(|| Error::Other(format!("job definition {} not found", code)))
    }

    pub(crate) fn get_task_executor(&self, job_code: &JobCode, task_code: &TaskCode) -> Result<TaskExecutorFn, Error> {
        let key = Self::task_executor_key(job_code, task_code);
        self.task_executors_by_key.get(&key).cloned().ok_or_else(|| {
            Error::Other(format!(
                "executor for task {} and job {} not exist",
                task_code, job_code
            ))
        })
    }

    // TODO(low): add iterator
    pub(crate) fn list_jobs(&self) -> Vec<JobCode> {
        self.jobs_by_code.keys().cloned().collect()
    }

    fn task_executor_key(job_code: &JobCode, task_code: &TaskCode) -> String {
        format!("{}:{}", job_code, task_code)
    }
}

impl JobDefinitionRegistry for JobRegistry {
    fn get_job(&self, code: &JobCode) -> Result<JobDefinition, Error> {
        self.get_job(code)
    }
}
