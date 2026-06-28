//! Error types for the S3-backed catalog.

use iceberg::{ErrorKind, NamespaceIdent, TableIdent};

use crate::infra::retrier::RetryError;

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

    /// Commit conflict caused by stale `ETag` or table-state CAS mismatch.
    #[error("commit conflict")]
    CommitConflict,

    /// Metadata is invalid.
    #[error("invalid metadata: {0}")]
    InvalidMetadata(String),

    /// Operation cancelled via [`tokio_util::sync::CancellationToken`].
    #[error("operation cancelled")]
    Cancelled,

    /// Catalog-root CAS retry budget exhausted without success.
    #[error("catalog CAS retry attempts exhausted")]
    CasMaxAttempts,

    /// Storage transient retry budget exhausted without success.
    #[error("storage retry attempts exhausted")]
    StorageRetryExhausted,

    /// Internal invariant violation: a should-be-unreachable state was reached.
    ///
    /// Distinct from [`Self::CasMaxAttempts`]: the retry loop reported success,
    /// yet the post-loop state contradicts that. Surfacing this rather than a
    /// retry-exhaustion error keeps the diagnostic honest if the invariant breaks.
    #[error("internal invariant violated: {0}")]
    Internal(&'static str),

    /// Upstream Iceberg error.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
}

impl Error {
    /// Conflict-class errors: CAS lost on root or table-state.
    #[must_use]
    pub const fn is_conflict(&self) -> bool {
        match self {
            Self::CommitConflict => true,
            Self::Storage(storage) => storage.is_conflict(),
            _ => false,
        }
    }

    /// Transient-class errors that warrant a retry with backoff.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match self {
            Self::Storage(storage) => storage.is_retryable(),
            _ => false,
        }
    }
}

impl RetryError for Error {
    fn cancelled() -> Self {
        Self::Cancelled
    }

    fn max_attempts() -> Self {
        Self::CasMaxAttempts
    }
}

/// Storage errors produced by the object store wrapper.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Object was not found.
    #[error("object not found: {0}")]
    NotFound(String),

    /// Conditional write precondition failed (412).
    #[error("precondition failed")]
    PreconditionFailed,

    /// Object already exists for create-if-absent writes.
    #[error("object already exists: {0}")]
    AlreadyExists(String),

    /// Transient failure: timeout, throttling, or 5xx upstream.
    #[error("transient storage failure: {0}")]
    Transient(String),

    /// Generic non-retryable I/O failure.
    #[error("io error: {0}")]
    Io(String),
}

impl StorageError {
    /// CAS conflict signals (precondition failed or create-if-absent collision).
    #[must_use]
    pub const fn is_conflict(&self) -> bool {
        matches!(self, Self::PreconditionFailed | Self::AlreadyExists(_))
    }

    /// Transient-class errors safe to retry with backoff.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

impl From<object_store::Error> for StorageError {
    fn from(error: object_store::Error) -> Self {
        match error {
            object_store::Error::NotFound { path, .. } => Self::NotFound(path),
            object_store::Error::Precondition { .. } => Self::PreconditionFailed,
            object_store::Error::AlreadyExists { path, .. } => Self::AlreadyExists(path),
            // Permanent auth failures: `object_store` 0.12 maps S3 403/401 to these
            // structured variants (see its `RetryError::error`). Classify them
            // explicitly as non-retryable so bad credentials fail on the first
            // attempt instead of burning the storage retry budget on every commit.
            // Matched ahead of the catch-all so a future reordering of the upstream
            // enum cannot silently reroute auth errors into a retryable arm.
            object_store::Error::PermissionDenied { .. } | object_store::Error::Unauthenticated { .. } => {
                Self::Io(error.to_string())
            }
            // `object_store` 0.12 collapses 5xx, throttling (429), request timeouts,
            // and connect/transport faults into `Generic`, with the recoverable
            // signal living only in the boxed source string — the source is its
            // crate-private `RetryError`, so the HTTP status is not structurally
            // reachable for a downcast. The rare permanent cases that also land here
            // (400 Bad Request, bare redirects, a wrong endpoint) cannot be split out
            // without string heuristics, so they are treated as transient too; the
            // capped storage backoff curve (`config::storage_retrier_config_default`)
            // bounds that misclassification at ~2s worst case, seconds not minutes.
            object_store::Error::Generic { source, .. } => Self::Transient(source.to_string()),
            object_store::Error::JoinError { source } => Self::Transient(source.to_string()),
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
            Error::Storage(StorageError::Transient(message)) => {
                Self::new(ErrorKind::Unexpected, message).with_retryable(true)
            }
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
            Error::Cancelled => Self::new(ErrorKind::Unexpected, "operation cancelled"),
            Error::CasMaxAttempts => Self::new(
                ErrorKind::CatalogCommitConflicts,
                "catalog CAS retry attempts exhausted",
            )
            .with_retryable(true),
            Error::StorageRetryExhausted => {
                Self::new(ErrorKind::Unexpected, "storage retry attempts exhausted").with_retryable(true)
            }
            // Invariant breach, not a transient or conflict condition: never retryable.
            Error::Internal(reason) => {
                Self::new(ErrorKind::Unexpected, format!("internal invariant violated: {reason}"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    /// Box a message as the `source` payload expected by `object_store::Error`
    /// variants.
    fn boxed_source(message: &str) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        message.into()
    }

    #[test]
    fn object_store_generic_maps_to_transient_retryable() {
        // `object_store` surfaces 5xx/throttling/timeouts as `Generic`; these must
        // stay retryable so the storage backoff curve can ride out transient faults.
        let storage = StorageError::from(object_store::Error::Generic {
            store: "s3",
            source: boxed_source("throttled"),
        });

        assert!(matches!(storage, StorageError::Transient(_)));
        assert!(storage.is_retryable());
        assert!(!storage.is_conflict());
    }

    #[test]
    fn object_store_permission_denied_maps_to_non_retryable_io() {
        // S3 403 -> `PermissionDenied`: a permanent auth failure must fail fast,
        // never inflate into a `StorageRetryExhausted` by burning the retry budget.
        let storage = StorageError::from(object_store::Error::PermissionDenied {
            path: "catalog/root.json".to_string(),
            source: boxed_source("forbidden"),
        });

        assert!(matches!(storage, StorageError::Io(_)));
        assert!(!storage.is_retryable());
        assert!(!storage.is_conflict());
    }

    #[test]
    fn object_store_unauthenticated_maps_to_non_retryable_io() {
        // S3 401 -> `Unauthenticated`: bad/missing credentials are permanent and
        // must not be retried on the hot ingest path.
        let storage = StorageError::from(object_store::Error::Unauthenticated {
            path: "catalog/root.json".to_string(),
            source: boxed_source("unauthorized"),
        });

        assert!(matches!(storage, StorageError::Io(_)));
        assert!(!storage.is_retryable());
        assert!(!storage.is_conflict());
    }

    #[test]
    fn object_store_not_found_maps_to_not_found() {
        let storage = StorageError::from(object_store::Error::NotFound {
            path: "root.json".to_string(),
            source: boxed_source("missing"),
        });

        assert!(matches!(storage, StorageError::NotFound(_)));
        assert!(!storage.is_retryable());
        assert!(!storage.is_conflict());
    }

    #[test]
    fn object_store_precondition_maps_to_precondition_conflict() {
        let storage = StorageError::from(object_store::Error::Precondition {
            path: "root.json".to_string(),
            source: boxed_source("etag mismatch"),
        });

        assert!(matches!(storage, StorageError::PreconditionFailed));
        assert!(storage.is_conflict());
        assert!(!storage.is_retryable());
    }

    #[test]
    fn object_store_already_exists_maps_to_already_exists_conflict() {
        let storage = StorageError::from(object_store::Error::AlreadyExists {
            path: "root.json".to_string(),
            source: boxed_source("exists"),
        });

        assert!(matches!(storage, StorageError::AlreadyExists(_)));
        assert!(storage.is_conflict());
        assert!(!storage.is_retryable());
    }

    #[test]
    fn object_store_unhandled_variant_maps_to_io() {
        // Any variant outside the explicit arms falls through to a non-retryable
        // I/O error; `NotImplemented` stands in for that catch-all.
        let storage = StorageError::from(object_store::Error::NotImplemented);

        assert!(matches!(storage, StorageError::Io(_)));
        assert!(!storage.is_retryable());
        assert!(!storage.is_conflict());
    }

    #[tokio::test]
    async fn object_store_join_error_maps_to_transient_retryable() {
        // A real `JoinError` only arises from an aborted/panicked task, so drive
        // one out of the runtime rather than fabricating it.
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();
        let join_error = handle.await.expect_err("aborted task yields a JoinError");

        let storage = StorageError::from(object_store::Error::JoinError { source: join_error });

        assert!(matches!(storage, StorageError::Transient(_)));
        assert!(storage.is_retryable());
    }

    #[test]
    fn storage_is_conflict_matrix() {
        assert!(StorageError::PreconditionFailed.is_conflict());
        assert!(StorageError::AlreadyExists("x".into()).is_conflict());
        assert!(!StorageError::NotFound("x".into()).is_conflict());
        assert!(!StorageError::Transient("x".into()).is_conflict());
        assert!(!StorageError::Io("x".into()).is_conflict());
    }

    #[test]
    fn storage_is_retryable_matrix() {
        assert!(StorageError::Transient("x".into()).is_retryable());
        assert!(!StorageError::PreconditionFailed.is_retryable());
        assert!(!StorageError::NotFound("x".into()).is_retryable());
        assert!(!StorageError::AlreadyExists("x".into()).is_retryable());
        assert!(!StorageError::Io("x".into()).is_retryable());
    }

    #[test]
    fn error_is_conflict_proxies_storage_and_matches_commit_conflict() {
        assert!(Error::CommitConflict.is_conflict());
        assert!(Error::Storage(StorageError::PreconditionFailed).is_conflict());
        assert!(Error::Storage(StorageError::AlreadyExists("x".into())).is_conflict());
        assert!(!Error::Storage(StorageError::Transient("x".into())).is_conflict());
        assert!(!Error::CasMaxAttempts.is_conflict());
        assert!(!Error::StorageRetryExhausted.is_conflict());
        assert!(!Error::Cancelled.is_conflict());
    }

    #[test]
    fn error_is_retryable_proxies_storage_transient_only() {
        assert!(Error::Storage(StorageError::Transient("x".into())).is_retryable());
        assert!(!Error::Storage(StorageError::PreconditionFailed).is_retryable());
        assert!(!Error::CommitConflict.is_retryable());
        assert!(!Error::CasMaxAttempts.is_retryable());
        assert!(!Error::StorageRetryExhausted.is_retryable());
        assert!(!Error::Cancelled.is_retryable());
    }

    #[test]
    fn retry_error_contract() {
        assert!(matches!(<Error as RetryError>::cancelled(), Error::Cancelled));
        assert!(matches!(<Error as RetryError>::max_attempts(), Error::CasMaxAttempts));
    }

    #[test]
    fn iceberg_mapping_marks_retryable_for_conflicts_and_cas_exhaustion() {
        let iceberg_error: iceberg::Error = Error::CommitConflict.into();
        assert_eq!(iceberg_error.kind(), ErrorKind::CatalogCommitConflicts);

        let iceberg_error: iceberg::Error = Error::CasMaxAttempts.into();
        assert_eq!(iceberg_error.kind(), ErrorKind::CatalogCommitConflicts);

        let iceberg_error: iceberg::Error = Error::Cancelled.into();
        assert_eq!(iceberg_error.kind(), ErrorKind::Unexpected);
    }

    #[test]
    fn iceberg_mapping_marks_internal_as_non_retryable_unexpected() {
        // An invariant breach is neither transient nor a conflict: it must not be
        // retried, and it must stay distinct from CAS exhaustion at the boundary.
        let iceberg_error: iceberg::Error = Error::Internal("slot empty").into();

        assert_eq!(iceberg_error.kind(), ErrorKind::Unexpected);
        assert!(!iceberg_error.retryable());
    }

    #[test]
    fn iceberg_mapping_marks_storage_exhaustion_as_retryable_unexpected() {
        let iceberg_error: iceberg::Error = Error::StorageRetryExhausted.into();

        assert_eq!(iceberg_error.kind(), ErrorKind::Unexpected);
        assert!(iceberg_error.retryable());
    }
}
