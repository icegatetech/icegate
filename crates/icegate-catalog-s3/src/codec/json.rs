//! JSON catalog codec.

use bytes::Bytes;
use iceberg::spec::TableMetadata;

use crate::codec::CatalogCodec;
use crate::error::{Error, Result};
use crate::model::CatalogRoot;

/// JSON implementation of [`CatalogCodec`].
#[derive(Debug)]
pub(crate) struct JsonCatalogCodec;

impl CatalogCodec for JsonCatalogCodec {
    fn encode_root(&self, root: &CatalogRoot) -> Result<Bytes> {
        serde_json::to_vec(root)
            .map(Bytes::from)
            .map_err(|error| Error::InvalidMetadata(format!("Failed to serialize catalog root: {error}")))
    }

    fn decode_root(&self, bytes: &[u8]) -> Result<CatalogRoot> {
        serde_json::from_slice(bytes).map_err(|error| Error::InvalidMetadata(format!("Invalid catalog root: {error}")))
    }

    fn encode_metadata(&self, metadata: &TableMetadata) -> Result<Bytes> {
        serde_json::to_vec(metadata)
            .map(Bytes::from)
            .map_err(|error| Error::InvalidMetadata(format!("Failed to serialize metadata: {error}")))
    }

    fn decode_metadata(&self, bytes: &[u8]) -> Result<TableMetadata> {
        serde_json::from_slice(bytes)
            .map_err(|error| Error::InvalidMetadata(format!("Invalid table metadata: {error}")))
    }

    fn file_extension(&self) -> &'static str {
        "json"
    }
}
