//! Domain model for catalog state.

use std::collections::HashMap;

use iceberg::{NamespaceIdent, TableIdent};
use serde::{Deserialize, Serialize};

/// Root catalog state stored in catalog storage.
/// IMPORTANT. CatalogRoot should encapsulate its state, change its state only through methods.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct CatalogRoot {
    // TODO(crit): нужно сделать мутабельные и немутабельные методы, а не использовать напрямую HashMap сверху. Нужна инкапсуляция. Т.е. пройтись по catalog.rs, найти все обращения внутрь root и сделать методы для этих обращений.
    /// Table entries keyed by `{namespace}.{table}`.
    #[serde(default)]
    pub(crate) tables: HashMap<String, TableEntry>,
}

/// Table entry inside the catalog root.
/// IMPORTANT. CatalogRoot should encapsulate its state, change its state only through methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableEntry { // TODO(crit): нужен конструктор; нужна инкапсуляция - в идеале, чтобы менять только через CatalogRoot
    /// Stable table identifier.
    pub(crate) table_id: String,
    /// Logical status.
    pub(crate) status: TableStatus,
    /// Full metadata location URI.
    pub(crate) metadata_location: String,
    /// Latest committed sequence number.
    pub(crate) commit: i64,
}

/// Table status in the catalog root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum TableStatus {
    /// Table is active and queryable.
    Active,
    /// Table is tombstoned.
    Tombstoned,
}

// TODO(crit): make table key as type and replace String
/// Build root key as `{namespace}.{table}`.
#[must_use]
pub(crate) fn table_key(ident: &TableIdent) -> String {
    format!("{}.{}", ident.namespace(), ident.name())
}

// TODO(crit): make table key as type and replace String
/// Parse namespace and table name from a root key.
#[must_use]
pub(crate) fn split_table_key(key: &str) -> Option<(NamespaceIdent, String)> {
    let (namespace, table_name) = key.rsplit_once('.')?;
    if namespace.is_empty() || table_name.is_empty() {
        return None;
    }

    let namespace_parts = namespace.split('.').map(std::string::ToString::to_string).collect::<Vec<_>>();

    let namespace_ident = NamespaceIdent::from_vec(namespace_parts).ok()?;
    Some((namespace_ident, table_name.to_string()))
}
