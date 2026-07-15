//! Iceberg REST wire DTOs and transport-only request values.

use std::collections::HashMap;
use std::num::NonZeroUsize;

use serde::{Deserialize, Deserializer, Serialize};

/// Response for `GET /v1/config`.
#[derive(Debug, Serialize)]
pub(crate) struct CatalogConfigResponse {
    pub(crate) defaults: HashMap<String, String>,
    pub(crate) overrides: HashMap<String, String>,
    pub(crate) endpoints: Vec<String>,
}

/// Namespace response representation.
#[derive(Debug, Serialize)]
pub(crate) struct NamespaceResponse {
    pub(crate) namespace: Vec<String>,
    pub(crate) properties: HashMap<String, String>,
}

/// Paginated namespace response.
///
/// `next-page-token` is always serialized: the spec makes `null` the end-of-
/// listing signal for a server that supports pagination, and omitting the field
/// is how a server declares it does not.
#[derive(Debug, Serialize)]
pub(crate) struct ListNamespacesResponse {
    pub(crate) namespaces: Vec<Vec<String>>,
    #[serde(rename = "next-page-token")]
    pub(crate) next_page_token: Option<String>,
}

/// Iceberg table identifier response representation.
#[derive(Debug, Serialize)]
pub(crate) struct TableIdentifierResponse {
    pub(crate) namespace: Vec<String>,
    pub(crate) name: String,
}

/// Paginated table response.
///
/// See [`ListNamespacesResponse`] for why `next-page-token` is never skipped.
#[derive(Debug, Serialize)]
pub(crate) struct ListTablesResponse {
    pub(crate) identifiers: Vec<TableIdentifierResponse>,
    #[serde(rename = "next-page-token")]
    pub(crate) next_page_token: Option<String>,
}

/// Table metadata response.
#[derive(Debug, Serialize)]
pub(crate) struct LoadTableResponse {
    #[serde(rename = "metadata-location")]
    pub(crate) metadata_location: String,
    pub(crate) metadata: serde_json::Value,
    pub(crate) config: HashMap<String, String>,
    #[serde(rename = "storage-credentials", skip_serializing_if = "Option::is_none")]
    pub(crate) storage_credentials: Option<serde_json::Value>,
}

/// Iceberg-standard HTTP error envelope.
#[derive(Debug, Serialize)]
pub(crate) struct IcebergErrorResponse {
    pub(crate) error: IcebergErrorModel,
}

/// Iceberg-standard HTTP error payload.
#[derive(Debug, Serialize)]
pub(crate) struct IcebergErrorModel {
    pub(crate) message: String,
    #[serde(rename = "type")]
    pub(crate) error_type: String,
    pub(crate) code: u16,
}

/// Query parameters shared by catalog collection endpoints.
#[derive(Debug, Deserialize)]
pub(crate) struct CollectionQuery {
    #[serde(rename = "pageToken")]
    pub(crate) page_token: Option<String>,
    /// The spec gives `pageSize` a minimum of one, so the bound is the type:
    /// zero, a negative value, and a value past `usize` are one malformed
    /// parameter, rejected before a handler decides whether to paginate at all.
    #[serde(default, rename = "pageSize", deserialize_with = "deserialize_optional_page_size")]
    pub(crate) page_size: Option<NonZeroUsize>,
}

/// Parse `pageSize` out of its wire text.
///
/// Routed through `String` because [`NamespaceListQuery`] flattens this type:
/// `serde(flatten)` buffers each value as self-describing content, which for a
/// urlencoded query is always a string, and no numeric type deserializes from
/// one.
fn deserialize_optional_page_size<'de, D>(deserializer: D) -> Result<Option<NonZeroUsize>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| value.parse().map_err(serde::de::Error::custom))
        .transpose()
}

/// Path parameters for namespace endpoints.
#[derive(Debug, Deserialize)]
pub(crate) struct NamespacePath {
    pub(crate) namespace: String,
}

/// Path parameters for table endpoints.
#[derive(Debug, Deserialize)]
pub(crate) struct TablePath {
    pub(crate) namespace: String,
    pub(crate) table: String,
}

/// Query parameters for listing namespaces.
#[derive(Debug, Deserialize)]
pub(crate) struct NamespaceListQuery {
    pub(crate) parent: Option<String>,
    #[serde(flatten)]
    pub(crate) collection: CollectionQuery,
}

/// Query parameters for loading table metadata.
#[derive(Debug, Deserialize)]
pub(crate) struct LoadTableQuery {
    pub(crate) snapshots: Option<SnapshotSelection>,
}

/// Metadata snapshot projection requested by a client.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum SnapshotSelection {
    /// Return every stored snapshot.
    #[default]
    All,
    /// Return only snapshots referenced by table refs.
    Refs,
}

impl SnapshotSelection {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Refs => "refs",
        }
    }
}
