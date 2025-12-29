use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Error, error::JobError};

/// Task identifier used in job definitions and execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskCode(String);

impl TaskCode {
    pub fn new(code: impl Into<String>) -> Self {
        Self(code.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TaskCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for TaskCode {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for TaskCode {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Task lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Todo,      // task is waiting to be picked up by a worker
    Started,   // task is currently being executed by a worker
    Completed, // task finished successfully
    Failed,    // task execution failed, task will be processed again
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Todo => write!(f, "todo"),
            Self::Started => write!(f, "started"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Definition of a task to be executed.
#[derive(Debug, Clone)]
pub struct TaskDefinition {
    // TODO(med): add maximum number of failed execution attempts
    code: TaskCode,
    input: Vec<u8>,
    timeout: Duration,
}

impl TaskDefinition {
    pub fn new(code: TaskCode, input: Vec<u8>, timeout: Duration) -> Result<Self, Error> {
        if timeout <= Duration::zero() {
            return Err(Error::Other("task timeout must be positive".into()));
        }
        Ok(Self {
            code,
            input,
            timeout,
        })
    }

    pub const fn code(&self) -> &TaskCode {
        &self.code
    }

    /// Return the input payload.
    pub fn input(&self) -> &[u8] {
        &self.input
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

/// Read-only view of a task for workers and clients.
pub trait ImmutableTask: Send + Sync {
    fn id(&self) -> &str;
    fn code(&self) -> &TaskCode;
    fn get_input(&self) -> &[u8];
    fn get_output(&self) -> &[u8];
    fn get_error(&self) -> &str;
    fn is_expired(&self) -> bool;
    fn is_completed(&self) -> bool;
    fn is_failed(&self) -> bool;
    fn attempts(&self) -> u32;
}

// Task - internal task representation
#[derive(Debug, Clone)]
pub(crate) struct Task {
    id: String,
    code: TaskCode,
    status: TaskStatus,
    processing_by_worker: String,
    created_by_worker: String,
    timeout: Duration,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    deadline_at: Option<DateTime<Utc>>,
    attempt: u32,
    input: Vec<u8>,
    output: Vec<u8>,
    error_msg: String,
}

impl Task {
    pub(crate) fn new(created_by_worker: String, task_def: TaskDefinition) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            code: task_def.code().clone(),
            status: TaskStatus::Todo,
            processing_by_worker: String::new(),
            created_by_worker,
            timeout: task_def.timeout(),
            started_at: None,
            completed_at: None,
            deadline_at: None,
            attempt: 0,
            input: task_def.input().to_vec(),
            output: Vec::new(),
            error_msg: String::new(),
        }
    }

    pub(crate) fn restore(
        id: String,
        code: TaskCode,
        status: TaskStatus,
        processing_by_worker: String,
        created_by_worker: String,
        timeout: Duration,
        started_at: Option<DateTime<Utc>>,
        completed_at: Option<DateTime<Utc>>,
        deadline_at: Option<DateTime<Utc>>,
        attempt: u32,
        input: Vec<u8>,
        output: Vec<u8>,
        error_msg: String,
    ) -> Self {
        Self {
            id,
            code,
            status,
            processing_by_worker,
            created_by_worker,
            timeout,
            started_at,
            completed_at,
            deadline_at,
            attempt,
            input,
            output,
            error_msg,
        }
    }

    // Accessors
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) const fn code(&self) -> &TaskCode {
        &self.code
    }

    pub(crate) const fn status(&self) -> &TaskStatus {
        &self.status
    }

    pub(crate) fn processing_by_worker(&self) -> &str {
        &self.processing_by_worker
    }

    pub(crate) fn created_by_worker(&self) -> &str {
        &self.created_by_worker
    }

    pub(crate) fn timeout(&self) -> Duration {
        self.timeout
    }

    pub(crate) const fn attempt(&self) -> u32 {
        self.attempt
    }

    pub(crate) const fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
    }

    pub(crate) const fn completed_at(&self) -> Option<DateTime<Utc>> {
        self.completed_at
    }

    pub(crate) const fn deadline_at(&self) -> Option<DateTime<Utc>> {
        self.deadline_at
    }

    pub(crate) fn input(&self) -> &[u8] {
        &self.input
    }

    pub(crate) fn output(&self) -> &[u8] {
        &self.output
    }

    pub(crate) fn error_msg(&self) -> &str {
        &self.error_msg
    }

    // State checks
    pub(crate) fn is_expired(&self) -> bool {
        if let Some(deadline) = self.deadline_at {
            Utc::now() > deadline
        } else {
            false
        }
    }

    pub(crate) const fn is_completed(&self) -> bool {
        matches!(self.status, TaskStatus::Completed)
    }

    pub(crate) const fn is_failed(&self) -> bool {
        matches!(self.status, TaskStatus::Failed)
    }

    pub(crate) const fn is_started(&self) -> bool {
        matches!(self.status, TaskStatus::Started)
    }

    pub(crate) fn is_processed(&self) -> bool {
        self.is_completed() || self.is_failed()
    }

    pub(crate) fn can_be_picked_up(&self) -> bool {
        match self.status {
            TaskStatus::Todo => true,
            TaskStatus::Started => self.is_expired(),
            TaskStatus::Failed => true, // TODO(low): add limit on number of attempts
            TaskStatus::Completed => false,
        }
    }

    pub(crate) fn start(&mut self, worker_id: String) -> Result<(), JobError> {
        if !self.can_be_picked_up() {
            if self.processing_by_worker != worker_id {
                return Err(JobError::TaskWorkerMismatch);
            }
            return Err(JobError::Other(format!(
                "cannot start task (id: {}; code: {}; status: {:?}; deadline at: {:?})",
                self.id, self.code, self.status, self.deadline_at
            )));
        }

        self.status = TaskStatus::Started;
        self.processing_by_worker = worker_id;
        let now = Utc::now();
        self.started_at = Some(now);
        self.deadline_at = Some(now + self.timeout);
        self.attempt += 1;

        Ok(())
    }

    pub(crate) fn complete(&mut self, output: Vec<u8>) -> Result<(), JobError> {
        if !matches!(self.status, TaskStatus::Started) {
            return Err(JobError::Other(format!(
                "cannot complete task (id: {}; code: {}) with status {:?}",
                self.id, self.code, self.status
            )));
        }

        self.output = output;
        self.status = TaskStatus::Completed;
        self.completed_at = Some(Utc::now());

        Ok(())
    }

    pub(crate) fn fail(&mut self, error_msg: &str) -> Result<(), JobError> {
        if !matches!(self.status, TaskStatus::Started) {
            return Err(JobError::Other(format!(
                "cannot fail task (id: {}; code: {}) with status {:?}",
                self.id, self.code, self.status
            )));
        }

        self.error_msg = error_msg.to_string();
        self.status = TaskStatus::Failed;
        self.completed_at = Some(Utc::now());

        Ok(())
    }
}

// ImmutableTask implementation for Task
impl ImmutableTask for Task {
    fn id(&self) -> &str {
        &self.id
    }

    fn code(&self) -> &TaskCode {
        self.code()
    }

    fn get_input(&self) -> &[u8] {
        self.input()
    }

    fn get_output(&self) -> &[u8] {
        self.output()
    }

    fn get_error(&self) -> &str {
        self.error_msg()
    }

    fn is_expired(&self) -> bool {
        self.is_expired()
    }

    fn is_completed(&self) -> bool {
        self.is_completed()
    }

    fn is_failed(&self) -> bool {
        self.is_failed()
    }

    fn attempts(&self) -> u32 {
        self.attempt()
    }
}
