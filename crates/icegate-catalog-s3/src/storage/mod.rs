//! Storage abstraction for catalog state.

use async_trait::async_trait;
use iceberg::spec::TableMetadata;

use crate::error::Result;
use crate::model::CatalogRoot;

pub(crate) mod in_memory;
pub(crate) mod s3;

/// Opaque version token for compare-and-swap operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Version(pub(crate) String);

// TODO(crit): почему version отдельно от root? Кажется, это часть root

#[async_trait]
pub(crate) trait CatalogStorage: Send + Sync {
    /// Load current catalog root and its version for compare-and-swap.
    async fn load_root(&self) -> Result<(CatalogRoot, Version)>;

    /// Save root atomically if the expected version still matches.
    async fn save_root(&self, root: CatalogRoot, expected: &Version) -> Result<()>;

    /// Read and deserialize table metadata from a storage URI.
    async fn read_table_metadata(&self, location: &str) -> Result<TableMetadata>;

    /// Write metadata and return its storage URI.
    async fn write_table_metadata(&self, table_id: &str, sequence_number: i64, metadata: &TableMetadata) -> Result<String>;

    /// Default table location for newly created tables.
    fn default_table_location(&self, table_id: &str) -> String;
}
