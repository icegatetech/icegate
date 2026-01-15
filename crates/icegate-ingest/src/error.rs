//! Error types for ingest operations.

use std::{error::Error, io};

/// Result type alias for ingest operations.
pub type Result<T> = std::result::Result<T, IngestError>;

/// Errors that can occur during data ingestion.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    /// Protobuf/JSON decoding error.
    #[error("decode error: {0}")]
    Decode(String),

    /// Invalid request parameters.
    #[error("{0}")]
    Validation(String),

    /// Feature not yet implemented.
    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// I/O error (WAL queue communication).
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// Queue write error.
    #[error("queue error: {0}")]
    Queue(#[from] icegate_queue::QueueError),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Underlying Iceberg error (from catalog operations).
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),

    /// Arrow error.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Runtime error
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// Shift operation error
    #[error("shift error: {0}")]
    Shift(String),

    /// Operation cancelled.
    #[error("operation cancelled")]
    Cancelled,

    /// Retry attempts exhausted.
    #[error("max retry attempts reached")]
    MaxAttemptsReached,

    /// Other errors
    #[error("other error: {0}")]
    Other(#[from] Box<dyn Error + Send + Sync>),

    /// Multiple errors
    #[error("multiple errors: {0:?}")]
    Multiple(Vec<Self>),
}

impl From<icegate_common::error::CommonError> for IngestError {
    fn from(err: icegate_common::error::CommonError) -> Self {
        use icegate_common::error::CommonError;
        match err {
            CommonError::Config(msg) => Self::Config(msg),
            CommonError::Toml(e) => Self::Config(e.to_string()),
            CommonError::Yaml(e) => Self::Config(e.to_string()),
            CommonError::Iceberg(e) => Self::Iceberg(e),
            CommonError::Io(e) => Self::Io(e),
            CommonError::ObjectStore(e) => Self::Config(format!("object store error: {e}")),
        }
    }
}

impl IngestError {
    /// Returns true when the error can be retried safely.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Iceberg(err) => err.retryable(),
            Self::Io(err) => is_retryable_io(err),
            Self::Queue(err) => err.is_retryable(),
            Self::Other(err) => is_retryable_error_chain(err.as_ref()),
            Self::Multiple(errors) => errors.iter().all(Self::is_retryable),
            Self::Decode(_)
            | Self::Validation(_)
            | Self::NotImplemented(_)
            | Self::Config(_)
            | Self::Cancelled
            | Self::MaxAttemptsReached
            | Self::Arrow(_)
            | Self::Join(_)
            | Self::Shift(_) => false,
        }
    }
}

fn is_retryable_error_chain(err: &(dyn Error + 'static)) -> bool {
    let mut current: Option<&(dyn Error + 'static)> = Some(err);
    while let Some(error) = current {
        if let Some(io_err) = error.downcast_ref::<io::Error>() {
            if is_retryable_io(io_err) {
                return true;
            }
        }

        if let Some(iceberg_err) = error.downcast_ref::<iceberg::Error>() {
            if iceberg_err.retryable() {
                return true;
            }
        }

        current = error.source();
    }
    false
}

fn is_retryable_io(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::NotConnected
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
    )
}

impl icegate_common::RetryError for IngestError {
    fn cancelled() -> Self {
        Self::Cancelled
    }

    fn max_attempts() -> Self {
        Self::MaxAttemptsReached
    }
}
