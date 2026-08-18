//! Error types for the queue crate.

use std::io;

use icegate_common::{is_retryable_error_source, is_retryable_object_store_error};

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

    /// Segments the caller asked to read from are gone from the topic.
    ///
    /// Contiguity below the requested offset is this crate's invariant, so the
    /// gap is reported here rather than left for a caller to discover by
    /// comparing offsets. Reported as facts only: what was asked for and what
    /// survives. Whoever configured the retention that removed them is the one
    /// who can say what to do about it.
    #[error(
        "WAL segments {requested_offset}..={} of topic '{topic}' are gone: the lowest surviving \
         segment is {lowest_offset}",
        lowest_offset.saturating_sub(1)
    )]
    SegmentsGone {
        /// Topic the listing was asked for.
        topic: String,
        /// Offset the caller asked to read from.
        requested_offset: u64,
        /// Lowest offset the topic still holds.
        lowest_offset: u64,
    },

    /// A segment inside the listed range is gone, so the run starts where the
    /// caller asked but breaks further up.
    ///
    /// The other shape of the same loss, and the one retention cleanup leaves
    /// behind when it skips a segment the store refuses to delete and reclaims
    /// its neighbours. A caller handed the shorter list would read the segments
    /// above the break as if they followed the ones below it, and the rows in
    /// between would vanish without a trace.
    #[error(
        "WAL segment {missing_offset} of topic '{topic}' is gone: the listing from \
         {requested_offset} is not contiguous"
    )]
    SegmentMissing {
        /// Topic the listing was asked for.
        topic: String,
        /// Offset the caller asked to read from.
        requested_offset: u64,
        /// Lowest offset the topic no longer holds inside the listed range.
        missing_offset: u64,
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

    /// Join error
    #[error("join error: {0}")]
    Join(#[from] tokio::task::JoinError),
}

impl icegate_common::RetryError for QueueError {
    fn cancelled() -> Self {
        Self::Cancelled
    }

    fn max_attempts() -> Self {
        Self::MaxAttemptsReached
    }
}

impl QueueError {
    /// Returns true when the error can be retried safely.
    ///
    /// Classification of a storage fault lives in `icegate-common`
    /// ([`is_retryable_object_store_error`]): this crate does not depend on
    /// opendal, and opendal is where the transient/permanent status of an S3
    /// answer is actually recorded.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Write { source, .. } | Self::Read { source, .. } => is_retryable_error_source(source.as_ref()),
            Self::ObjectStore(err) => is_retryable_object_store_error(err),
            // The chain walk starts at the error itself, so an already typed
            // `io::Error` is classified by the same list.
            Self::Io(err) => is_retryable_error_source(err),
            Self::Multiple(errors) => errors.iter().all(Self::is_retryable),
            Self::Cancelled
            | Self::MaxAttemptsReached
            | Self::AlreadyExists { .. }
            | Self::Recovery { .. }
            | Self::InvalidPath { .. }
            // Listing again cannot bring back a deleted file.
            | Self::SegmentsGone { .. }
            | Self::SegmentMissing { .. }
            | Self::Parquet(_)
            | Self::Arrow(_)
            | Self::Json(_)
            | Self::ChannelClosed
            | Self::Config(_)
            | Self::Metadata(_)
            | Self::Join(_) => false,
        }
    }
}
