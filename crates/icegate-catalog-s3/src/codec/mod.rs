//! Catalog serialization codecs.

use std::sync::Arc;

use bytes::Bytes;
use iceberg::spec::TableMetadata;

use crate::error::Result;
use crate::model::CatalogRoot;

pub(crate) mod json;

/// Strategy for serializing catalog root and table metadata.
pub(crate) trait CatalogCodec: Send + Sync {
    /// Serialize catalog root.
    fn encode_root(&self, root: &CatalogRoot) -> Result<Bytes>;

    /// Deserialize catalog root.
    fn decode_root(&self, bytes: &[u8]) -> Result<CatalogRoot>;

    /// Serialize table metadata.
    fn encode_metadata(&self, metadata: &TableMetadata) -> Result<Bytes>;

    /// Deserialize table metadata.
    fn decode_metadata(&self, bytes: &[u8]) -> Result<TableMetadata>;

    /// File extension for serialized metadata files.
    fn file_extension(&self) -> &'static str;
}

// TODO(crit): это должно быть в конфиге, здесь только общие компоненты
/// Catalog serialization codec kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CatalogCodecKind {
    /// JSON serialization for root and metadata files.
    #[default]
    Json,
}

impl CatalogCodecKind {
    pub(crate) fn into_codec(self) -> Arc<dyn CatalogCodec> {
        match self {
            Self::Json => Arc::new(json::JsonCatalogCodec),
        }
    }
}
