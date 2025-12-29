pub(crate) mod cached;
pub(crate) mod s3;

use async_trait::async_trait;
use thiserror::Error as ThisError;
use tokio_util::sync::CancellationToken;

use crate::{Error, Job, JobCode, JobDefinition, retrier::RetryError};

// StorageError - storage-specific errors
#[derive(ThisError, Debug, Clone)]
pub(crate) enum StorageError {
    #[error("job not found")]
    NotFound,

    #[error("concurrent modification detected")]
    ConcurrentModification,

    #[error("job not modified")]
    NotModified,

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("s3 error: {0}")]
    S3(String),

    #[error("timeout")]
    Timeout,

    #[error("operation cancelled")]
    Cancelled,

    #[error("rate limited")]
    RateLimited,

    #[error("service unavailable")]
    ServiceUnavailable,

    #[error("auth error")]
    Auth,

    #[error("{0}")]
    Other(String),
}

impl StorageError {
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            StorageError::Timeout | StorageError::RateLimited | StorageError::ServiceUnavailable
        )
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, StorageError::ConcurrentModification)
    }

    pub fn is_cancelled(&self) -> bool {
        matches!(self, StorageError::Cancelled)
    }
}

impl RetryError for StorageError {
    fn cancelled() -> Self {
        StorageError::Cancelled
    }

    fn max_attempts() -> Self {
        StorageError::Other("max retry attempts reached".into())
    }
}

pub(crate) type StorageResult<T> = Result<T, StorageError>;

// JobMeta - lightweight job metadata without full job state
#[derive(Debug, Clone)]
pub(crate) struct JobMeta {
    pub code: JobCode,
    pub iter_num: u64,
    pub version: String,
}

/// Storage backend trait for persisting job state.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Get latest job by code
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job>;

    /// Get job by specific metadata
    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job>;

    /// Find job metadata without loading full job
    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta>;

    /// Save job with optimistic locking. Updates job.version on success. Returns
    /// ConcurrentModification error if version mismatch.
    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()>;
}

/// Provides job definitions for storage enrichment.
pub trait JobDefinitionRegistry: Send + Sync {
    fn get_job(&self, code: &JobCode) -> Result<JobDefinition, Error>;
}
