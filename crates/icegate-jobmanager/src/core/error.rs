use thiserror::Error;

use crate::{infra::retrier::RetryError, storage::StorageError};

/// Public error type for jobmanager operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Serialization or deserialization failed.
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),

    /// Operation was canceled.
    #[error("operation cancelled")]
    Cancelled,

    /// Generic error message.
    #[error("{0}")]
    Other(String),
}

/// Result type for public APIs.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) enum InternalError {
    #[error(transparent)]
    Storage(StorageError),

    #[error(transparent)]
    Job(JobError),

    #[error("operation cancelled")]
    Cancelled,

    #[error("max retry attempts reached")]
    MaxAttemptsReached,

    #[error("{0}")]
    Other(String),
}

#[derive(Error, Debug)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) enum JobError {
    #[error("task not found")]
    TaskNotFound,

    #[error("task worker mismatch")]
    TaskWorkerMismatch,

    #[error("invalid job status transition from {from} to {to}")]
    InvalidStatusTransition { from: String, to: String },

    #[error("{0}")]
    Other(String),
}

impl From<JobError> for InternalError {
    fn from(err: JobError) -> Self {
        Self::Job(err)
    }
}

impl From<StorageError> for InternalError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::Cancelled => Self::Cancelled,
            _ => Self::Storage(err),
        }
    }
}

impl From<Error> for InternalError {
    fn from(err: Error) -> Self {
        match err {
            Error::Cancelled => Self::Cancelled,
            Error::Serialization(e) => Self::Other(e.to_string()),
            Error::Other(msg) => Self::Other(msg),
        }
    }
}

impl RetryError for InternalError {
    fn cancelled() -> Self {
        Self::Cancelled
    }

    fn max_attempts() -> Self {
        Self::MaxAttemptsReached
    }
}
