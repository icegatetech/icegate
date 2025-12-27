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

    /// Runtime error
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),

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
