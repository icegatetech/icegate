//! JSON persistence codec for catalog roots.

use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{
    CatalogRoot, CatalogTableLink, NamespaceEntry, NamespaceKey, TableId, TableKey, TableMetadataLocation,
};
use crate::error::{Error, Result};
use crate::storage::codec::CatalogCodec;

const ROOT_FORMAT_VERSION: u8 = 1;

/// JSON implementation of [`CatalogCodec`].
#[derive(Debug)]
pub(crate) struct JsonCatalogCodec;

#[derive(Debug, Serialize, Deserialize)]
struct CatalogRootDto {
    #[serde(rename = "root-format-version")]
    root_format_version: u8,
    namespaces: Vec<NamespaceRecordDto>,
    tables: Vec<TableRecordDto>,
    tombstones: Vec<TombstoneRecordDto>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NamespaceRecordDto {
    namespace: Vec<String>,
    properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableRecordDto {
    namespace: Vec<String>,
    name: String,
    table_id: Uuid,
    metadata_location: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TombstoneRecordDto {
    table_id: Uuid,
    metadata_location: String,
}

impl JsonCatalogCodec {
    fn encode_root_dto(root: &CatalogRoot) -> CatalogRootDto {
        let mut namespaces = root
            .namespace_entries()
            .map(|(key, entry)| NamespaceRecordDto {
                namespace: key.parts().to_vec(),
                properties: entry.properties().clone(),
            })
            .collect::<Vec<_>>();
        namespaces.sort_by(|left, right| left.namespace.cmp(&right.namespace));

        let mut tables = root
            .table_entries()
            .map(|(key, entry)| TableRecordDto {
                namespace: key.namespace().parts().to_vec(),
                name: key.name().to_string(),
                table_id: *entry.table_id().as_uuid(),
                metadata_location: entry.metadata_location().as_str().to_string(),
            })
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| (&left.namespace, &left.name).cmp(&(&right.namespace, &right.name)));

        let mut tombstones = root
            .tombstone_entries()
            .map(|(table_id, entry)| TombstoneRecordDto {
                table_id: *table_id.as_uuid(),
                metadata_location: entry.metadata_location().as_str().to_string(),
            })
            .collect::<Vec<_>>();
        tombstones.sort_by_key(|record| record.table_id);

        CatalogRootDto {
            root_format_version: ROOT_FORMAT_VERSION,
            namespaces,
            tables,
            tombstones,
        }
    }

    fn decode_root_dto(dto: CatalogRootDto) -> Result<CatalogRoot> {
        if dto.root_format_version != ROOT_FORMAT_VERSION {
            return Err(Error::InvalidMetadata(format!(
                "Unsupported catalog root format version: {}",
                dto.root_format_version
            )));
        }

        let namespaces = dto
            .namespaces
            .into_iter()
            .map(|record| {
                Ok((
                    NamespaceKey::new(record.namespace)?,
                    NamespaceEntry::new(record.properties),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let tables = dto
            .tables
            .into_iter()
            .map(|record| {
                Ok((
                    TableKey::new(NamespaceKey::new(record.namespace)?, record.name)?,
                    CatalogTableLink::new(
                        TableId::from(record.table_id),
                        TableMetadataLocation::new(record.metadata_location),
                    ),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let tombstones = dto
            .tombstones
            .into_iter()
            .map(|record| {
                let mut entry = CatalogTableLink::new(
                    TableId::from(record.table_id),
                    TableMetadataLocation::new(record.metadata_location),
                );
                entry.mark_tombstoned();
                (TableId::from(record.table_id), entry)
            })
            .collect::<Vec<_>>();

        CatalogRoot::new(namespaces, tables, tombstones).map_err(|error| Error::InvalidMetadata(error.to_string()))
    }
}

impl CatalogCodec for JsonCatalogCodec {
    fn encode_root(&self, root: &CatalogRoot) -> Result<Bytes> {
        serde_json::to_vec(&Self::encode_root_dto(root))
            .map(Bytes::from)
            .map_err(|error| Error::InvalidMetadata(format!("Failed to serialize catalog root: {error}")))
    }

    fn decode_root(&self, bytes: &[u8]) -> Result<CatalogRoot> {
        let dto = serde_json::from_slice(bytes)
            .map_err(|error| Error::InvalidMetadata(format!("Invalid catalog root: {error}")))?;
        Self::decode_root_dto(dto)
    }

    fn root_filename(&self) -> &'static str {
        "root.json"
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::collections::HashMap;

    use uuid::Uuid;

    use super::*;
    use crate::domain::{CatalogRoot, CatalogTableLink, TableId, TableKey, TableMetadataLocation};

    const EVENTS_METADATA_LOCATION: &str =
        "s3://bucket/warehouse/catalog/tables/events-id/metadata/00000-events.metadata.json";
    const ANIMALS_METADATA_LOCATION: &str =
        "s3://bucket/warehouse/catalog/tables/animals-id/metadata/00002-animals.metadata.json";
    const TOMBSTONE_METADATA_LOCATION: &str =
        "s3://bucket/warehouse/catalog/tables/dropped-id/metadata/00007-dropped.metadata.json";

    fn table_link(uuid: u128, metadata_location: &str) -> CatalogTableLink {
        CatalogTableLink::new(
            TableId::from(Uuid::from_u128(uuid)),
            TableMetadataLocation::new(metadata_location.to_string()),
        )
    }

    /// A root touching every persisted field: a nested namespace with a dot
    /// inside a part, a namespace with properties, dotted table names, and a
    /// tombstone. Entries are deliberately supplied in non-sorted order so the
    /// encoded output proves the codec sorts rather than echoes.
    fn sample_root() -> CatalogRoot {
        let mut tombstone = table_link(3, TOMBSTONE_METADATA_LOCATION);
        tombstone.mark_tombstoned();
        CatalogRoot::new(
            vec![
                (
                    NamespaceKey::new(vec!["zoo".to_string()]).expect("namespace key"),
                    NamespaceEntry::new(HashMap::from([("owner".to_string(), "dogs".to_string())])),
                ),
                (
                    NamespaceKey::new(vec!["dogs".to_string(), "owners.and.handlers".to_string()])
                        .expect("namespace key"),
                    NamespaceEntry::new(HashMap::new()),
                ),
            ],
            vec![
                (
                    TableKey::new(
                        NamespaceKey::new(vec!["zoo".to_string()]).expect("namespace key"),
                        "animals.v1".to_string(),
                    )
                    .expect("table key"),
                    table_link(2, ANIMALS_METADATA_LOCATION),
                ),
                (
                    TableKey::new(
                        NamespaceKey::new(vec!["dogs".to_string(), "owners.and.handlers".to_string()])
                            .expect("namespace key"),
                        "events.v2".to_string(),
                    )
                    .expect("table key"),
                    table_link(1, EVENTS_METADATA_LOCATION),
                ),
            ],
            vec![(TableId::from(Uuid::from_u128(3)), tombstone)],
        )
        .expect("sample root")
    }

    /// The persisted wire format, written out by hand from the format
    /// definition rather than produced by the codec — the independent oracle
    /// both directions are checked against.
    fn expected_wire_format() -> serde_json::Value {
        serde_json::json!({
            "root-format-version": 1,
            "namespaces": [
                {"namespace": ["dogs", "owners.and.handlers"], "properties": {}},
                {"namespace": ["zoo"], "properties": {"owner": "dogs"}},
            ],
            "tables": [
                {
                    "namespace": ["dogs", "owners.and.handlers"],
                    "name": "events.v2",
                    "table_id": "00000000-0000-0000-0000-000000000001",
                    "metadata_location": EVENTS_METADATA_LOCATION,
                },
                {
                    "namespace": ["zoo"],
                    "name": "animals.v1",
                    "table_id": "00000000-0000-0000-0000-000000000002",
                    "metadata_location": ANIMALS_METADATA_LOCATION,
                },
            ],
            "tombstones": [
                {
                    "table_id": "00000000-0000-0000-0000-000000000003",
                    "metadata_location": TOMBSTONE_METADATA_LOCATION,
                },
            ],
        })
    }

    /// `serde_json::Value` equality is order-sensitive for arrays, so this also
    /// pins the deterministic sort: the fixture supplies entries out of order
    /// and the expected arrays are ordered by namespace parts, then name, then
    /// tombstone id.
    #[test]
    fn encode_root_writes_the_exact_persisted_format() {
        let encoded = JsonCatalogCodec.encode_root(&sample_root()).expect("encode root");

        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&encoded).expect("encoded root JSON"),
            expected_wire_format(),
        );
    }

    #[test]
    fn decode_root_restores_every_entry_losslessly_from_the_persisted_format() {
        let payload = serde_json::to_vec(&expected_wire_format()).expect("wire format bytes");

        let root = JsonCatalogCodec.decode_root(&payload).expect("decode root");

        let mut namespaces = root
            .namespace_entries()
            .map(|(key, entry)| (key.parts().to_vec(), entry.properties().clone()))
            .collect::<Vec<_>>();
        namespaces.sort_by(|left, right| left.0.cmp(&right.0));
        assert_eq!(
            namespaces,
            vec![
                (
                    vec!["dogs".to_string(), "owners.and.handlers".to_string()],
                    HashMap::new(),
                ),
                (
                    vec!["zoo".to_string()],
                    HashMap::from([("owner".to_string(), "dogs".to_string())]),
                ),
            ],
        );

        let mut tables = root
            .table_entries()
            .map(|(key, entry)| {
                (
                    key.namespace().parts().to_vec(),
                    key.name().to_string(),
                    *entry.table_id().as_uuid(),
                    entry.metadata_location().as_str().to_string(),
                )
            })
            .collect::<Vec<_>>();
        tables.sort();
        assert_eq!(
            tables,
            vec![
                (
                    vec!["dogs".to_string(), "owners.and.handlers".to_string()],
                    "events.v2".to_string(),
                    Uuid::from_u128(1),
                    EVENTS_METADATA_LOCATION.to_string(),
                ),
                (
                    vec!["zoo".to_string()],
                    "animals.v1".to_string(),
                    Uuid::from_u128(2),
                    ANIMALS_METADATA_LOCATION.to_string(),
                ),
            ],
        );

        let tombstones = root
            .tombstone_entries()
            .map(|(table_id, entry)| (*table_id.as_uuid(), entry.metadata_location().as_str().to_string()))
            .collect::<Vec<_>>();
        assert_eq!(
            tombstones,
            vec![(Uuid::from_u128(3), TOMBSTONE_METADATA_LOCATION.to_string())],
        );
    }

    /// Every payload here reaches the decoder through the production S3 read
    /// path, so each broken invariant must fail the load rather than construct
    /// a root the rest of the catalog assumes is consistent.
    #[test]
    fn decode_root_rejects_malformed_and_inconsistent_payloads() {
        const LOCATION: &str = "s3://b/w/catalog/tables/t/metadata/00000-t.metadata.json";
        let table = |namespace: &str, name: &str, uuid: u128| {
            format!(
                r#"{{"namespace":["{namespace}"],"name":"{name}","table_id":"{}","metadata_location":"{LOCATION}"}}"#,
                Uuid::from_u128(uuid),
            )
        };
        let tombstone = |uuid: u128| {
            format!(
                r#"{{"table_id":"{}","metadata_location":"{LOCATION}"}}"#,
                Uuid::from_u128(uuid),
            )
        };
        let root = |namespaces: &str, tables: &str, tombstones: &str| {
            format!(
                r#"{{"root-format-version":1,"namespaces":[{namespaces}],"tables":[{tables}],"tombstones":[{tombstones}]}}"#,
            )
        };
        let namespace = r#"{"namespace":["ns"],"properties":{}}"#;

        let cases: Vec<(&str, String)> = vec![
            ("malformed JSON", "{".to_string()),
            (
                "truncated JSON",
                r#"{"root-format-version":1,"namespaces":["#.to_string(),
            ),
            ("missing format version", r#"{"tables":[]}"#.to_string()),
            (
                "unsupported format version",
                r#"{"root-format-version":2,"namespaces":[],"tables":[],"tombstones":[]}"#.to_string(),
            ),
            (
                "empty namespace part",
                root(r#"{"namespace":[""],"properties":{}}"#, "", ""),
            ),
            (
                "namespace without parts",
                root(r#"{"namespace":[],"properties":{}}"#, "", ""),
            ),
            ("empty table name", root(namespace, &table("ns", "", 1), "")),
            ("table without a namespace record", root("", &table("ns", "t", 1), "")),
            (
                "metadata location without a version prefix",
                root(
                    namespace,
                    &format!(
                        r#"{{"namespace":["ns"],"name":"t","table_id":"{}","metadata_location":"s3://b/w/catalog/tables/t/metadata/head.json"}}"#,
                        Uuid::from_u128(1),
                    ),
                    "",
                ),
            ),
            (
                "duplicate namespace record",
                root(&format!("{namespace},{namespace}"), "", ""),
            ),
            (
                "duplicate table record",
                root(
                    namespace,
                    &format!("{},{}", table("ns", "t", 1), table("ns", "t", 2)),
                    "",
                ),
            ),
            (
                "duplicate tombstone record",
                root("", "", &format!("{},{}", tombstone(1), tombstone(1))),
            ),
            (
                "duplicate table id across active tables",
                root(
                    namespace,
                    &format!("{},{}", table("ns", "a", 1), table("ns", "b", 1)),
                    "",
                ),
            ),
            (
                "duplicate table id between an active table and a tombstone",
                root(namespace, &table("ns", "t", 1), &tombstone(1)),
            ),
        ];

        for (case, payload) in cases {
            let error = JsonCatalogCodec
                .decode_root(payload.as_bytes())
                .expect_err(&format!("{case} must fail decoding"));

            assert!(matches!(error, Error::InvalidMetadata(_)), "{case}: {error}");
        }
    }
}
