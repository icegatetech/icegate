use thiserror::Error;

use crate::{retrier::RetryError, storage::StorageError};

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
        InternalError::Job(err)
    }
}

impl From<StorageError> for InternalError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::Cancelled => InternalError::Cancelled,
            _ => InternalError::Storage(err),
        }
    }
}

impl From<Error> for InternalError {
    fn from(err: Error) -> Self {
        match err {
            Error::Cancelled => InternalError::Cancelled,
            Error::Serialization(e) => InternalError::Other(e.to_string()),
            Error::Other(msg) => InternalError::Other(msg),
        }
    }
}

impl RetryError for InternalError {
    fn cancelled() -> Self {
        InternalError::Cancelled
    }

    fn max_attempts() -> Self {
        InternalError::MaxAttemptsReached
    }
}
