//! Error types for the queue crate.

use std::io;

/// Result type for queue operations.
pub type Result<T> = std::result::Result<T, QueueError>;

/// Errors that can occur in queue operations.
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    /// Error writing a segment to object storage.
    #[error("failed to write segment {topic}/{offset}: {source}")]
    Write {
        /// Topic name.
        topic: String,
        /// Segment offset.
        offset: u64,
        /// Underlying error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Error reading a segment from object storage.
    #[error("failed to read segment {topic}/{offset}: {source}")]
    Read {
        /// Topic name.
        topic: String,
        /// Segment offset.
        offset: u64,
        /// Underlying error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Segment already exists (412 Precondition Failed).
    #[error("segment already exists: {topic}/{offset}")]
    AlreadyExists {
        /// Topic name.
        topic: String,
        /// Segment offset.
        offset: u64,
    },

    /// Error during recovery.
    #[error("recovery failed for topic {topic}: {reason}")]
    Recovery {
        /// Topic name.
        topic: String,
        /// Reason for failure.
        reason: String,
    },

    /// Invalid segment path.
    #[error("invalid segment path: {path}")]
    InvalidPath {
        /// The invalid path.
        path: String,
    },

    /// Parquet encoding/decoding error.
    #[error("parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Arrow error.
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Object store error.
    #[error("object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// IO error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// JSON serialization error.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// Channel closed.
    #[error("channel closed")]
    ChannelClosed,

    /// Operation cancelled.
    #[error("operation cancelled")]
    Cancelled,

    /// Retry attempts exhausted.
    #[error("max retry attempts reached")]
    MaxAttemptsReached,

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Metadata or data layout error.
    #[error("metadata error: {0}")]
    Metadata(String),

    /// Multiple errors occurred.
    #[error("multiple errors occurred: {0:?}")]
    Multiple(Vec<Self>),
}

impl icegate_common::RetryError for QueueError {
    fn cancelled() -> Self {
        Self::Cancelled
    }

    fn max_attempts() -> Self {
        Self::MaxAttemptsReached
    }
}
