//! JSON codec for catalog metadata payloads.

use bytes::Bytes;

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

    fn root_filename(&self) -> &'static str {
        "root.json"
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use iceberg::{NamespaceIdent, TableIdent};
    use uuid::Uuid;

    use super::*;
    use crate::model::{CatalogRoot, CatalogTableLink, TableId, TableKey, TableMetadataLocation};

    fn sample_root() -> CatalogRoot {
        let mut root = CatalogRoot::default();
        let namespace = NamespaceIdent::from_vec(vec!["a".to_string(), "b".to_string()]).expect("namespace");
        root.create_namespace(&namespace, std::collections::HashMap::new());
        root.link_table(
            TableKey::from_ident(&TableIdent::new(namespace, "tbl".to_string())),
            CatalogTableLink::new(
                TableId::from(Uuid::from_u128(1)),
                TableMetadataLocation::new(
                    "s3://bucket/warehouse/catalog/tables/table-id/metadata/00000-uuid.metadata.json".to_string(),
                ),
            ),
        )
        .expect("create root entry");
        root
    }

    #[test]
    fn encode_root_decode_root_round_trip() {
        let codec = JsonCatalogCodec;
        let root = sample_root();

        let encoded = codec.encode_root(&root).expect("encode root");
        let decoded = codec.decode_root(&encoded).expect("decode root");
        let reencoded = codec.encode_root(&decoded).expect("re-encode root");

        assert_eq!(encoded, reencoded);
    }

    #[test]
    fn decode_root_fails_on_invalid_json() {
        let codec = JsonCatalogCodec;
        let error = codec
            .decode_root(br#"{"tables": ["bad"]}"#)
            .expect_err("invalid root payload must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }
}
