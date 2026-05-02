//! Catalog metadata serialization codecs.

use bytes::Bytes;

use crate::error::Result;
use crate::model::CatalogRoot;

pub(crate) mod json;

/// Strategy for serializing catalog metadata objects.
///
/// This codec is used only for catalog root payloads.
///
/// It does not apply to Iceberg table metadata documents (`metadata.json`) and
/// does not define formats for Iceberg table data files (`data/`, manifests,
/// etc.).
pub(crate) trait CatalogCodec: Send + Sync {
    /// Serialize catalog root.
    fn encode_root(&self, root: &CatalogRoot) -> Result<Bytes>;

    /// Deserialize catalog root.
    fn decode_root(&self, bytes: &[u8]) -> Result<CatalogRoot>;

    /// Root filename used by this codec.
    fn root_filename(&self) -> &'static str;
}
