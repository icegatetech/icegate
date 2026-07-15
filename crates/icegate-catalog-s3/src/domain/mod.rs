//! Catalog domain entities and invariants.

mod root;

use iceberg::{NamespaceIdent, TableIdent};

/// Errors raised by catalog business rules and domain invariants.
///
/// Internal to the crate: every variant is folded into a flat
/// [`crate::Error`](crate::error::Error) before it reaches a caller, so the
/// domain layer stays off the public surface.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DomainError {
    /// The requested namespace does not exist.
    #[error("namespace not found: {0}")]
    NamespaceNotFound(NamespaceIdent),
    /// The namespace cannot be deleted while it still contains state.
    #[error("namespace not empty: {0}")]
    NamespaceNotEmpty(NamespaceIdent),
    /// The requested table does not exist.
    #[error("table not found: {0}")]
    TableNotFound(TableIdent),
    /// A distinct table already occupies the requested identifier.
    #[error("table already exists: {0}")]
    TableAlreadyExists(TableIdent),
    /// A table update cannot be applied to the current domain state.
    #[error("commit conflict")]
    CommitConflict,
    /// A persisted domain invariant is malformed or violated.
    #[error("invalid metadata: {0}")]
    InvalidMetadata(String),
    /// Iceberg rejected a domain metadata transition.
    #[error("iceberg error: {0}")]
    Iceberg(#[from] iceberg::Error),
}

pub(crate) use root::{
    CatalogRoot, CatalogTableLink, IcebergTableMetadata, MergeOutcome, NamespaceEntry, NamespaceKey, TableId, TableKey,
    TableMetadataLocation, TableUpdate,
};
