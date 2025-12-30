pub(crate) mod cached;
pub(crate) mod s3;

use async_trait::async_trait;
use thiserror::Error as ThisError;
use tokio_util::sync::CancellationToken;

use crate::infra::retrier::RetryError;
use crate::{Error, Job, JobCode, JobDefinition};

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
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Timeout | Self::RateLimited | Self::ServiceUnavailable)
    }

    pub const fn is_conflict(&self) -> bool {
        matches!(self, Self::ConcurrentModification)
    }

    pub const fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}

impl RetryError for StorageError {
    fn cancelled() -> Self {
        Self::Cancelled
    }

    fn max_attempts() -> Self {
        Self::Other("max retry attempts reached".into())
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
#[allow(private_interfaces)] // TODO(low): think about how to open Job to public
pub trait Storage: Send + Sync {
    /// Get latest job by code
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job>;

    /// Get job by specific metadata
    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job>;

    /// Find job metadata without loading full job
    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta>;

    /// Save job with optimistic locking. Updates job.version on success. Returns
    /// If version mismatch.
    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()>;
}

/// Provides job definitions for storage enrichment.
pub trait JobDefinitionRegistry: Send + Sync {
    fn get_job(&self, code: &JobCode) -> Result<JobDefinition, Error>;
}
