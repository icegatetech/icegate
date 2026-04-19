//! Storage abstraction for catalog state.

use async_trait::async_trait;
use iceberg::spec::TableMetadata;

use crate::error::Result;
use crate::model::CatalogRoot;

#[cfg(test)]
pub(crate) mod in_memory;
pub(crate) mod s3;

/// Opaque version token for compare-and-swap operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Version {
    /// Root object does not exist yet.
    Absent,
    /// Root object exists and is identified by `ETag`.
    Etag(String),
}

#[async_trait]
/// Storage backend used by the S3 catalog state machine.
pub(crate) trait CatalogStorage: Send + Sync {
    /// Load current catalog root and its version for compare-and-swap.
    ///
    /// `Version` stays separate from [`CatalogRoot`] to keep the domain model
    /// free of storage-specific CAS tokens. Callers pair both values for the
    /// subsequent [`CatalogStorage::save_root`] call.
    async fn load_root(&self) -> Result<(CatalogRoot, Version)>;

    /// Save root atomically if the expected version still matches.
    async fn save_root(&self, root: CatalogRoot, expected: &Version) -> Result<()>;

    /// Read and deserialize table metadata from a storage URI.
    async fn read_table_metadata(&self, location: &str) -> Result<TableMetadata>;

    /// Write metadata to a precomputed storage URI.
    async fn write_table_metadata(&self, location: &str, metadata: &TableMetadata) -> Result<()>;
}
