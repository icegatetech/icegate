//! Error types for the S3-backed catalog.

use iceberg::{ErrorKind, NamespaceIdent, TableIdent};

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

    /// Namespace is not empty.
    #[error("namespace not empty: {0}")]
    NamespaceNotEmpty(NamespaceIdent),

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

impl From<object_store::Error> for StorageError {
    fn from(error: object_store::Error) -> Self {
        match error {
            object_store::Error::NotFound { path, .. } => Self::NotFound(path),
            object_store::Error::Precondition { .. } => Self::PreconditionFailed,
            object_store::Error::AlreadyExists { path, .. } => Self::AlreadyExists(path),
            other => Self::Io(other.to_string()),
        }
    }
}

impl From<Error> for iceberg::Error {
    fn from(error: Error) -> Self {
        match error {
            Error::Iceberg(iceberg_error) => iceberg_error,
            Error::Storage(StorageError::NotFound(path)) => {
                Self::new(ErrorKind::Unexpected, format!("Object not found: {path}"))
            }
            Error::Storage(StorageError::PreconditionFailed) => {
                Self::new(ErrorKind::CatalogCommitConflicts, "CAS precondition failed").with_retryable(true)
            }
            Error::Storage(StorageError::AlreadyExists(path)) => Self::new(
                ErrorKind::CatalogCommitConflicts,
                format!("Object already exists: {path}"),
            )
            .with_retryable(true),
            Error::Storage(StorageError::Io(message)) => Self::new(ErrorKind::Unexpected, message),
            Error::TableNotFound(table) => Self::new(ErrorKind::TableNotFound, format!("Table not found: {table}")),
            Error::TableAlreadyExists(table) => {
                Self::new(ErrorKind::TableAlreadyExists, format!("Table already exists: {table}"))
            }
            Error::NamespaceNotFound(namespace) => Self::new(
                ErrorKind::NamespaceNotFound,
                format!("Namespace not found: {namespace}"),
            ),
            Error::NamespaceNotEmpty(namespace) => Self::new(
                ErrorKind::PreconditionFailed,
                format!("Namespace not empty: {namespace}"),
            ),
            Error::CommitConflict => {
                Self::new(ErrorKind::CatalogCommitConflicts, "catalog commit conflict").with_retryable(true)
            }
            Error::InvalidMetadata(message) => Self::new(ErrorKind::DataInvalid, message),
        }
    }
}
