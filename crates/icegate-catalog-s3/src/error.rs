//! Error types for the S3-backed catalog.

use iceberg::{NamespaceIdent, TableIdent};

/// Catalog result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by the S3-backed catalog.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Storage-level failure.
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    /// Table was not found.
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),

    /// Table already exists.
    #[error("table already exists: {0}")]
    TableAlreadyExists(TableIdent),

    /// Namespace was not found.
    #[error("namespace not found: {0}")]
    NamespaceNotFound(NamespaceIdent),

    /// Commit conflict caused by stale `ETag`.
    #[error("commit conflict")]
    CommitConflict,

    /// Metadata is invalid.
    #[error("invalid metadata: {0}")]
    InvalidMetadata(String),

    /// Upstream Iceberg error.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
}

/// Storage errors produced by the object store wrapper.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Object was not found.
    #[error("object not found: {0}")]
    NotFound(String),

    /// Conditional write precondition failed.
    #[error("precondition failed")]
    PreconditionFailed,

    /// Object already exists for create-if-absent writes.
    #[error("object already exists: {0}")]
    AlreadyExists(String),

    /// Generic I/O failure.
    #[error("io error: {0}")]
    Io(String),
}
