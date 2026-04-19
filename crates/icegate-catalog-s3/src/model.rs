//! Domain model for catalog state.

use std::collections::{BTreeSet, HashMap};

use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::{NamespaceIdent, TableCreation, TableIdent, TableRequirement, TableUpdate};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Error;

/// Version of an Iceberg metadata file commit.
///
/// This value matches the numeric prefix in metadata filenames like
/// `00042-uuid.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MetadataVersion(i64);

impl MetadataVersion {
    /// Initial metadata version for newly created tables.
    pub(crate) const fn initial() -> Self {
        Self(0)
    }

    /// Create metadata version from a parsed numeric value.
    fn from_i64(value: i64) -> Result<Self, Error> {
        if value < 0 {
            return Err(Error::InvalidMetadata(format!("Negative metadata version: {value}")));
        }

        Ok(Self(value))
    }

    /// Parse metadata version from an Iceberg metadata file location.
    pub(crate) fn from_location(location: &str) -> Result<Self, Error> {
        let filename = location
            .rsplit('/')
            .next()
            .filter(|segment| !segment.is_empty())
            .ok_or_else(|| Error::InvalidMetadata(format!("Invalid metadata location: {location}")))?;
        let version = filename
            .split_once('-')
            .map(|(prefix, _)| prefix)
            .ok_or_else(|| Error::InvalidMetadata(format!("Missing metadata version in location: {location}")))?;
        let parsed = version.parse::<i64>().map_err(|error| {
            Error::InvalidMetadata(format!(
                "Invalid metadata version '{version}' in location {location}: {error}"
            ))
        })?;

        Self::from_i64(parsed)
    }

    /// Return the next metadata version.
    pub fn next(self) -> Result<Self, Error> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| Error::InvalidMetadata(format!("Metadata version overflow: {}", self.0)))
    }

    /// Return the numeric value stored in `root.json`.
    pub const fn as_i64(self) -> i64 {
        self.0
    }
}

/// Prepared table commit.
#[derive(Debug, Clone)]
pub(crate) struct PreparedTableCommit {
    pub(crate) identifier: TableIdent,
    pub(crate) key: TableKey,
    pub(crate) metadata_location: String,
    pub(crate) metadata: TableMetadata,
}

/// Domain state that manages Iceberg table metadata updates.
#[derive(Debug, Clone)]
pub(crate) struct TableMetadataState {
    current_metadata_location: String,
    current_metadata: TableMetadata,
}

impl TableMetadataState {
    pub(crate) const fn new(current_metadata_location: String, current_metadata: TableMetadata) -> Self {
        Self {
            current_metadata_location,
            current_metadata,
        }
    }

    /// Build initial table metadata and wrap it into a `TableMetadataState`.
    ///
    /// # Errors
    ///
    /// Returns an error if the `TableCreation` is invalid or metadata cannot be built.
    pub(crate) fn create(creation: TableCreation, uuid: Uuid) -> Result<Self, Error> {
        let metadata = TableMetadataBuilder::from_table_creation(creation)?
            .assign_uuid(uuid)
            .build()?
            .metadata;
        let metadata_location = Self::next_metadata_path(metadata.location(), MetadataVersion::initial());
        Ok(Self::new(metadata_location, metadata))
    }

    pub(crate) const fn metadata(&self) -> &TableMetadata {
        &self.current_metadata
    }

    pub(crate) fn metadata_location(&self) -> &str {
        &self.current_metadata_location
    }

    pub(crate) fn prepare_commit(
        self,
        identifier: TableIdent,
        key: TableKey,
        requirements: Vec<TableRequirement>,
        updates: Vec<TableUpdate>,
    ) -> Result<PreparedTableCommit, Error> {
        for requirement in requirements {
            requirement.check(Some(&self.current_metadata))?;
        }

        let mut builder = self.current_metadata.into_builder(Some(self.current_metadata_location.clone()));
        for update in updates {
            builder = update.apply(builder)?;
        }

        let build = builder.build()?;
        if build.metadata.location().is_empty() {
            return Err(Error::InvalidMetadata(format!("empty table location for {identifier}")));
        }

        let metadata_version = MetadataVersion::from_location(&self.current_metadata_location)?.next()?;
        let metadata_location = Self::next_metadata_path(build.metadata.location(), metadata_version);

        Ok(PreparedTableCommit {
            identifier,
            key,
            metadata_location,
            metadata: build.metadata,
        })
    }

    fn next_metadata_path(table_location: &str, version: MetadataVersion) -> String {
        format!(
            "{}/metadata/{:05}-{}.metadata.json",
            table_location.trim_end_matches('/'),
            version.as_i64(),
            Uuid::new_v4()
        )
    }
}

/// Table entry inside the catalog root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TableEntry {
    /// Stable table identifier.
    table_id: String,
    /// Logical status.
    status: TableStatus,
    /// Full metadata location URI.
    metadata_location: String,
}

impl TableEntry {
    pub(crate) const fn new(table_id: String, metadata_location: String) -> Self {
        Self {
            table_id,
            status: TableStatus::Active,
            metadata_location,
        }
    }

    pub(crate) fn table_id(&self) -> &str {
        &self.table_id
    }

    pub(crate) fn metadata_location(&self) -> &str {
        &self.metadata_location
    }

    pub(crate) fn is_active(&self) -> bool {
        self.status == TableStatus::Active
    }

    fn validate_metadata_location(&self) -> Result<(), Error> {
        let _ = MetadataVersion::from_location(&self.metadata_location)?;
        Ok(())
    }
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

/// Stable table identifier stored in the root index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct TableId(String);

impl TableId {
    fn from_entry(entry: &TableEntry) -> Self {
        Self(entry.table_id.clone())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Root key as `{namespace}.{table}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct TableKey(String);

impl TableKey {
    pub(crate) fn from_ident(ident: &TableIdent) -> Self {
        Self(format!("{}.{}", ident.namespace(), ident.name()))
    }

    pub(crate) fn split(&self) -> Option<(NamespaceIdent, String)> {
        let (namespace, table_name) = self.0.rsplit_once('.')?;
        if namespace.is_empty() || table_name.is_empty() {
            return None;
        }

        let namespace_parts = namespace.split('.').map(ToString::to_string).collect::<Vec<_>>();

        let namespace_ident = NamespaceIdent::from_vec(namespace_parts).ok()?;
        Some((namespace_ident, table_name.to_string()))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Root key as `{namespace}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct NamespaceKey(String);

impl NamespaceKey {
    pub(crate) fn from_ident(ident: &NamespaceIdent) -> Self {
        Self(ident.join("."))
    }

    pub(crate) fn split(&self) -> Option<NamespaceIdent> {
        if self.0.is_empty() {
            return None;
        }

        let namespace_parts = self.0.split('.').map(ToString::to_string).collect::<Vec<_>>();
        if namespace_parts.iter().any(String::is_empty) {
            return None;
        }

        NamespaceIdent::from_vec(namespace_parts).ok()
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Namespace record inside the catalog root.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct NamespaceEntry {
    /// Namespace properties.
    #[serde(default)]
    properties: HashMap<String, String>,
}

impl NamespaceEntry {
    pub(crate) const fn new(properties: HashMap<String, String>) -> Self {
        Self { properties }
    }

    pub(crate) const fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub(crate) fn replace_properties(&mut self, properties: HashMap<String, String>) {
        self.properties = properties;
    }
}

/// Root catalog state stored in catalog storage.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct CatalogRoot {
    /// Namespace records indexed by `{namespace}`.
    #[serde(default)]
    namespaces: HashMap<NamespaceKey, NamespaceEntry>,
    /// Active table entries indexed by `{namespace}.{table}`.
    #[serde(default)]
    tables: HashMap<TableKey, TableEntry>,
    /// Tombstoned table entries indexed by stable table id.
    #[serde(default)]
    tombstones: HashMap<TableId, TableEntry>,
}

impl CatalogRoot {
    pub(crate) fn namespace_entries(&self) -> impl Iterator<Item = (&NamespaceKey, &NamespaceEntry)> {
        self.namespaces.iter()
    }

    /// List direct child namespaces under `parent`, or top-level namespaces if `parent` is `None`.
    ///
    /// Implicit namespaces derived from table keys are included alongside explicitly created ones.
    /// The result is deterministically ordered.
    pub(crate) fn list_namespaces(&self, parent: Option<&NamespaceIdent>) -> Vec<NamespaceIdent> {
        let namespaces: BTreeSet<NamespaceIdent> =
            self.namespace_entries().filter_map(|(key, _)| key.split()).collect();

        match parent {
            None => namespaces.into_iter().filter(|ns| ns.as_ref().len() == 1).collect(),
            Some(parent_ns) => {
                let parent_parts = parent_ns.as_ref();
                namespaces
                    .into_iter()
                    .filter(|ns| {
                        let parts = ns.as_ref();
                        parts.len() == parent_parts.len() + 1 && &parts[..parent_parts.len()] == parent_parts.as_slice()
                    })
                    .collect()
            }
        }
    }

    pub(crate) fn get_namespace(&self, namespace: &NamespaceIdent) -> Option<&NamespaceEntry> {
        self.namespaces.get(&NamespaceKey::from_ident(namespace))
    }

    pub(crate) fn create_namespace(&mut self, namespace: &NamespaceIdent, properties: HashMap<String, String>) -> bool {
        let key = NamespaceKey::from_ident(namespace);
        if self.namespaces.contains_key(&key) {
            return false;
        }

        self.namespaces.insert(key, NamespaceEntry::new(properties));
        true
    }

    pub(crate) fn update_namespace(&mut self, namespace: &NamespaceIdent, properties: HashMap<String, String>) -> bool {
        let Some(entry) = self.namespaces.get_mut(&NamespaceKey::from_ident(namespace)) else {
            return false;
        };

        entry.replace_properties(properties);
        true
    }

    /// Drop a namespace, enforcing the invariant that it must be empty.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NamespaceNotEmpty`] if the namespace has active tables or descendant namespaces.
    /// Returns [`Error::NamespaceNotFound`] if the namespace does not exist.
    pub(crate) fn drop_namespace(&mut self, namespace: &NamespaceIdent) -> Result<(), Error> {
        if self.has_active_tables_in_namespace_tree(namespace) || self.has_namespace_descendants(namespace) {
            return Err(Error::NamespaceNotEmpty(namespace.clone()));
        }

        if self.namespaces.remove(&NamespaceKey::from_ident(namespace)).is_none() {
            return Err(Error::NamespaceNotFound(namespace.clone()));
        }

        Ok(())
    }

    fn has_namespace_descendants(&self, namespace: &NamespaceIdent) -> bool {
        let prefix = format!("{}.", NamespaceKey::from_ident(namespace).as_str());
        self.namespaces.keys().any(|key| key.as_str().starts_with(&prefix))
    }

    fn has_active_tables_in_namespace_tree(&self, namespace: &NamespaceIdent) -> bool {
        let prefix = format!("{}.", NamespaceKey::from_ident(namespace).as_str());
        self.active_entries()
            .any(|(table_key, _)| table_key.as_str().starts_with(&prefix))
    }

    pub(crate) fn get_active(&self, key: &TableKey) -> Option<&TableEntry> {
        self.tables.get(key).filter(|entry| entry.is_active())
    }

    pub(crate) fn active_entries(&self) -> impl Iterator<Item = (&TableKey, &TableEntry)> {
        self.tables.iter().filter(|(_, entry)| entry.is_active())
    }

    pub(crate) fn active_tables_in_namespace(&self, namespace: &NamespaceIdent) -> Vec<TableIdent> {
        let mut tables = self
            .active_entries()
            .filter_map(|(key, _)| {
                let (key_namespace, table_name) = key.split()?;
                if &key_namespace != namespace {
                    return None;
                }
                Some(TableIdent::new(key_namespace, table_name))
            })
            .collect::<Vec<_>>();
        tables.sort_by(|left, right| left.name().cmp(right.name()));
        tables
    }

    /// Create a new table from scratch: resolve location, build metadata, validate, and insert.
    ///
    /// Generates a UUID internally. Falls back to `{tables_uri_prefix}/{uuid}` when
    /// `creation.location` is `None`.
    pub(crate) fn create_table(
        &mut self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
        tables_uri_prefix: &str,
    ) -> Result<(TableMetadataState, TableIdent), Error> {
        if self.get_namespace(namespace).is_none() {
            return Err(Error::NamespaceNotFound(namespace.clone()));
        }

        let uuid = Uuid::new_v4();
        let table_location = creation
            .location
            .clone()
            .unwrap_or_else(|| format!("{tables_uri_prefix}/{uuid}"));
        let creation = TableCreation {
            location: Some(table_location),
            ..creation
        };
        let ident = TableIdent::new(namespace.clone(), creation.name.clone());
        let key = TableKey::from_ident(&ident);
        let state = TableMetadataState::create(creation, uuid)?;
        let entry = TableEntry::new(
            state.metadata().uuid().to_string(),
            state.metadata_location().to_string(),
        );
        self.link_table(key, entry)?;
        Ok((state, ident))
    }

    /// Link an existing table entry (pre-built key and entry) into the catalog.
    ///
    /// Used for `register_table` where the metadata location comes from the caller.
    pub(crate) fn link_table(&mut self, key: TableKey, entry: TableEntry) -> Result<(), Error> {
        entry.validate_metadata_location()?;

        let (namespace, _) = key
            .split()
            .ok_or_else(|| Error::InvalidMetadata(format!("Invalid table key: {}", key.as_str())))?;
        if self.get_namespace(&namespace).is_none() {
            return Err(Error::NamespaceNotFound(namespace));
        }

        if self.get_active(&key).is_some() {
            return Err(Error::TableAlreadyExists(Self::table_ident_from_key(&key)?));
        }

        let table_id = TableId::from_entry(&entry);
        if self
            .active_entries()
            .any(|(_, active_entry)| active_entry.table_id() == table_id.as_str())
        {
            return Err(Error::InvalidMetadata(format!(
                "Duplicate table id in catalog root: {}",
                table_id.as_str()
            )));
        }

        self.tables.insert(key, entry);
        Ok(())
    }

    pub(crate) fn apply_commit(&mut self, key: &TableKey, metadata_location: String) -> Result<(), Error> {
        let entry = self.tables.get_mut(key).ok_or(Error::CommitConflict)?;
        if !entry.is_active() {
            return Err(Error::CommitConflict);
        }

        let old_version = MetadataVersion::from_location(&entry.metadata_location)?;
        let new_version = MetadataVersion::from_location(&metadata_location)?;
        if new_version.as_i64() != old_version.as_i64() + 1 {
            return Err(Error::CommitConflict);
        }

        entry.metadata_location = metadata_location;
        Ok(())
    }

    pub(crate) fn tombstone(&mut self, key: &TableKey) -> Result<(), Error> {
        let table_ident = Self::table_ident_from_key(key)?;
        let mut entry = self
            .tables
            .remove(key)
            .ok_or_else(|| Error::TableNotFound(table_ident.clone()))?;
        if !entry.is_active() {
            return Err(Error::TableNotFound(table_ident));
        }

        entry.status = TableStatus::Tombstoned;
        let table_id = TableId::from_entry(&entry);
        self.tombstones.insert(table_id, entry);
        Ok(())
    }

    pub(crate) fn rename(&mut self, src: &TableKey, dst: TableKey) -> Result<(), Error> {
        let (dst_namespace, _) = dst
            .split()
            .ok_or_else(|| Error::InvalidMetadata(format!("Invalid table key: {}", dst.as_str())))?;
        if self.get_namespace(&dst_namespace).is_none() {
            return Err(Error::NamespaceNotFound(dst_namespace));
        }

        if self.get_active(&dst).is_some() {
            return Err(Error::TableAlreadyExists(Self::table_ident_from_key(&dst)?));
        }

        let source_ident = Self::table_ident_from_key(src)?;
        let entry = self.tables.get(src).ok_or_else(|| Error::TableNotFound(source_ident))?;
        if !entry.is_active() {
            return Err(Error::TableNotFound(Self::table_ident_from_key(src)?));
        }

        let Some(entry) = self.tables.remove(src) else {
            return Err(Error::TableNotFound(Self::table_ident_from_key(src)?));
        };
        self.tables.insert(dst, entry);
        Ok(())
    }

    fn table_ident_from_key(key: &TableKey) -> Result<TableIdent, Error> {
        let (namespace, table_name) = key
            .split()
            .ok_or_else(|| Error::InvalidMetadata(format!("Invalid table key: {}", key.as_str())))?;
        Ok(TableIdent::new(namespace, table_name))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::collections::HashMap;

    use iceberg::TableCreation;
    use iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type, UnboundPartitionSpec,
    };

    use super::*;

    fn namespace(parts: &[&str]) -> NamespaceIdent {
        NamespaceIdent::from_vec(parts.iter().map(|part| (*part).to_string()).collect()).expect("namespace")
    }

    fn table_ident(namespace: NamespaceIdent, name: &str) -> TableIdent {
        TableIdent::new(namespace, name.to_string())
    }

    fn table_key(namespace: NamespaceIdent, name: &str) -> TableKey {
        TableKey::from_ident(&table_ident(namespace, name))
    }

    fn table_entry(version: i64) -> TableEntry {
        TableEntry::new(
            format!("table-id-{version}"),
            format!("memory://catalog/tables/table-id/metadata/{version:05}-uuid.metadata.json"),
        )
    }

    fn test_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema")
    }

    fn test_table_metadata(location: &str) -> TableMetadata {
        TableMetadataBuilder::new(
            test_schema(),
            UnboundPartitionSpec::default(),
            SortOrder::unsorted_order(),
            location.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .build()
        .expect("table metadata")
        .metadata
    }

    #[test]
    fn metadata_version_from_location_parses_valid() {
        let version =
            MetadataVersion::from_location("s3://bucket/table/metadata/00042-uuid.json").expect("metadata version");

        assert_eq!(version.as_i64(), 42);
    }

    #[test]
    fn metadata_version_from_location_fails_on_empty_filename() {
        let error = MetadataVersion::from_location("s3://bucket/").expect_err("trailing slash must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[test]
    fn metadata_version_from_location_fails_on_missing_dash() {
        let error =
            MetadataVersion::from_location("s3://bucket/metadata/nondash.json").expect_err("missing dash must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[test]
    fn metadata_version_from_location_fails_on_nonnumeric_prefix() {
        let error = MetadataVersion::from_location("s3://bucket/metadata/abc-uuid.json")
            .expect_err("non-numeric prefix must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[test]
    fn metadata_version_from_location_parses_metadata_json_suffix() {
        let version = MetadataVersion::from_location("s3://bucket/table/metadata/00007-uuid.metadata.json")
            .expect("metadata version with .metadata.json suffix");

        assert_eq!(version.as_i64(), 7);
    }

    #[test]
    fn metadata_version_next_overflows() {
        let error = MetadataVersion(i64::MAX).next().expect_err("overflow must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[test]
    fn next_metadata_path_uses_metadata_json_suffix() {
        let state = TableMetadataState::create(test_table_creation("s3://bucket/tables/tbl"), Uuid::new_v4())
            .expect("create table state");

        let location = state.metadata_location();

        assert!(
            location.ends_with(".metadata.json"),
            "expected .metadata.json suffix, got: {location}"
        );
        assert!(
            !location.ends_with("..metadata.json"),
            "must not have double dot before .metadata.json: {location}"
        );
    }

    fn test_table_creation(location: &str) -> TableCreation {
        TableCreation::builder()
            .name("tbl".to_string())
            .schema(test_schema())
            .location(location.to_string())
            .build()
    }

    #[test]
    fn create_location_has_no_double_slash() {
        let with_slash = TableMetadataState::create(test_table_creation("s3://bucket/tables/tbl/"), Uuid::new_v4())
            .expect("create with trailing slash location");
        let without_slash = TableMetadataState::create(test_table_creation("s3://bucket/tables/tbl"), Uuid::new_v4())
            .expect("create without trailing slash location");

        assert!(
            with_slash
                .metadata_location()
                .starts_with("s3://bucket/tables/tbl/metadata/00000-")
        );
        assert!(
            without_slash
                .metadata_location()
                .starts_with("s3://bucket/tables/tbl/metadata/00000-")
        );
        assert!(with_slash.metadata_location().ends_with(".metadata.json"));
        assert!(without_slash.metadata_location().ends_with(".metadata.json"));

        let with_uuid = with_slash
            .metadata_location()
            .rsplit('/')
            .next()
            .expect("metadata filename with trailing slash location")
            .strip_prefix("00000-")
            .expect("version prefix")
            .trim_end_matches(".metadata.json");
        let without_uuid = without_slash
            .metadata_location()
            .rsplit('/')
            .next()
            .expect("metadata filename without trailing slash location")
            .strip_prefix("00000-")
            .expect("version prefix")
            .trim_end_matches(".metadata.json");

        Uuid::parse_str(with_uuid).expect("valid uuid in filename with trailing slash");
        Uuid::parse_str(without_uuid).expect("valid uuid in filename without trailing slash");
    }

    #[test]
    fn prepare_commit_builds_next_metadata_successfully() {
        let namespace = namespace(&["ns"]);
        let identifier = table_ident(namespace.clone(), "tbl");
        let key = table_key(namespace, "tbl");
        let current_metadata_location = "memory://catalog/tables/tbl/metadata/00000-initial.metadata.json".to_string();
        let metadata = test_table_metadata("memory://catalog/tables/tbl");

        let prepared = TableMetadataState::new(current_metadata_location, metadata.clone())
            .prepare_commit(identifier.clone(), key.clone(), Vec::new(), Vec::new())
            .expect("prepare commit");

        assert_eq!(prepared.identifier, identifier);
        assert_eq!(prepared.key, key);
        assert_eq!(prepared.metadata.uuid(), metadata.uuid());
        assert!(
            prepared
                .metadata_location
                .starts_with("memory://catalog/tables/tbl/metadata/00001-")
        );
    }

    #[test]
    fn prepare_commit_fails_when_built_table_location_is_empty() {
        let identifier = table_ident(namespace(&["ns"]), "tbl");
        let key = table_key(namespace(&["ns"]), "tbl");
        let current_metadata_location = "memory://catalog/tables/tbl/metadata/00000-initial.metadata.json".to_string();

        let error = TableMetadataState::new(current_metadata_location, test_table_metadata(""))
            .prepare_commit(identifier, key, Vec::new(), Vec::new())
            .expect_err("empty table location must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[test]
    fn table_key_split_supports_nested_namespace() {
        let key = TableKey("a.b.tbl".to_string());

        let (namespace_ident, table_name) = key.split().expect("split table key");

        assert_eq!(namespace_ident, namespace(&["a", "b"]));
        assert_eq!(table_name, "tbl");
    }

    #[test]
    fn table_key_split_fails_for_invalid_key() {
        let key = TableKey(".tbl".to_string());

        assert!(key.split().is_none());
    }

    #[test]
    fn create_table_success() {
        let namespace = namespace(&["ns"]);
        let key = table_key(namespace.clone(), "tbl");
        let ident = table_ident(namespace.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());

        root.link_table(key.clone(), table_entry(0)).expect("create table");

        let entry = root.get_active(&key).expect("active entry");
        assert_eq!(entry.table_id(), "table-id-0");
        assert_eq!(CatalogRoot::table_ident_from_key(&key).expect("table ident"), ident);
    }

    #[test]
    fn create_table_fails_when_active_exists() {
        let namespace = namespace(&["ns"]);
        let key = table_key(namespace.clone(), "tbl");
        let ident = table_ident(namespace.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");

        let error = root.link_table(key, table_entry(1)).expect_err("active duplicate must fail");

        assert!(matches!(error, Error::TableAlreadyExists(existing) if existing == ident));
    }

    #[test]
    fn create_table_fails_on_invalid_metadata_location() {
        let namespace = namespace(&["ns"]);
        let key = table_key(namespace, "tbl");
        let mut root = CatalogRoot::default();

        let error = root
            .link_table(
                key,
                TableEntry::new(
                    "table-id".to_string(),
                    "memory://catalog/tables/table-id/metadata/invalid.json".to_string(),
                ),
            )
            .expect_err("invalid metadata location must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[test]
    fn create_table_fails_when_namespace_missing() {
        let namespace = namespace(&["nonexistent"]);
        let key = table_key(namespace.clone(), "tbl");
        let mut root = CatalogRoot::default();

        let error = root.link_table(key, table_entry(0)).expect_err("missing namespace must fail");

        assert!(matches!(error, Error::NamespaceNotFound(ns) if ns == namespace));
    }

    #[test]
    fn create_table_succeeds_over_tombstone() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        let original_table_id = root
            .tables
            .get(&key)
            .expect("active table before tombstone")
            .table_id()
            .to_string();
        root.tombstone(&key).expect("tombstone");

        root.link_table(key.clone(), table_entry(1)).expect("recreate over tombstone");

        let entry = root.get_active(&key).expect("active entry after recreate");
        assert_eq!(
            MetadataVersion::from_location(entry.metadata_location())
                .expect("metadata version")
                .as_i64(),
            1
        );
        assert_eq!(
            root.tombstones.iter().filter(|(_, entry)| !entry.is_active()).count(),
            1
        );
        assert!(
            root.tombstones
                .iter()
                .any(|(table_id, entry)| table_id.as_str() == original_table_id && !entry.is_active())
        );
    }

    #[test]
    fn link_table_succeeds_when_uuid_is_tombstoned() {
        let ns = namespace(&["ns"]);
        let first_key = table_key(ns.clone(), "original");
        let second_key = table_key(ns.clone(), "restored");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(first_key.clone(), table_entry(0)).expect("seed table");
        let tombstoned_id = root
            .tables
            .get(&first_key)
            .expect("active table before tombstone")
            .table_id()
            .to_string();
        root.tombstone(&first_key).expect("tombstone");

        root.link_table(second_key.clone(), table_entry(0))
            .expect("re-register tombstoned uuid under new name");

        let entry = root.get_active(&second_key).expect("active entry under new name");
        assert_eq!(entry.table_id(), tombstoned_id.as_str());
    }

    #[test]
    fn tombstone_success() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");

        root.tombstone(&key).expect("tombstone");

        assert!(root.get_active(&key).is_none());
        assert!(root.tombstones.iter().any(|(_, entry)| !entry.is_active()));
    }

    #[test]
    fn tombstone_fails_when_not_found() {
        let namespace = namespace(&["ns"]);
        let ident = table_ident(namespace.clone(), "missing");
        let key = table_key(namespace, "missing");
        let mut root = CatalogRoot::default();

        let error = root.tombstone(&key).expect_err("missing key must fail");

        assert!(matches!(error, Error::TableNotFound(table) if table == ident));
    }

    #[test]
    fn tombstone_fails_when_already_tombstoned() {
        let namespace = namespace(&["ns"]);
        let ident = table_ident(namespace.clone(), "tbl");
        let key = table_key(namespace.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        root.tombstone(&key).expect("first tombstone");

        let error = root.tombstone(&key).expect_err("second tombstone must fail");

        assert!(matches!(error, Error::TableNotFound(table) if table == ident));
    }

    #[test]
    fn rename_success() {
        let namespace = namespace(&["ns"]);
        let src = table_key(namespace.clone(), "src");
        let dst = table_key(namespace.clone(), "dst");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(src.clone(), table_entry(0)).expect("seed table");

        root.rename(&src, dst.clone()).expect("rename");

        assert!(!root.tables.contains_key(&src));
        assert!(root.get_active(&dst).is_some());
    }

    #[test]
    fn rename_fails_when_source_not_found() {
        let namespace = namespace(&["ns"]);
        let src_ident = table_ident(namespace.clone(), "src");
        let src = table_key(namespace.clone(), "src");
        let dst = table_key(namespace.clone(), "dst");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());

        let error = root.rename(&src, dst).expect_err("missing source must fail");

        assert!(matches!(error, Error::TableNotFound(table) if table == src_ident));
    }

    #[test]
    fn rename_fails_when_dest_is_active() {
        let namespace = namespace(&["ns"]);
        let src = table_key(namespace.clone(), "src");
        let dst_ident = table_ident(namespace.clone(), "dst");
        let dst = table_key(namespace.clone(), "dst");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(src.clone(), table_entry(0)).expect("seed src");
        root.link_table(dst.clone(), table_entry(1)).expect("seed dst");

        let error = root.rename(&src, dst).expect_err("active destination must fail");

        assert!(matches!(error, Error::TableAlreadyExists(table) if table == dst_ident));
        assert!(root.get_active(&src).is_some());
    }

    #[test]
    fn rename_succeeds_when_dest_is_tombstoned() {
        let namespace = namespace(&["ns"]);
        let src = table_key(namespace.clone(), "src");
        let dst = table_key(namespace.clone(), "dst");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(src.clone(), table_entry(0)).expect("seed src");
        root.link_table(dst.clone(), table_entry(1)).expect("seed dst");
        let tombstoned_table_id = root.tables.get(&dst).expect("active destination table").table_id().to_string();
        root.tombstone(&dst).expect("tombstone dst");

        root.rename(&src, dst.clone()).expect("rename over tombstone");

        assert!(!root.tables.contains_key(&src));
        let entry = root.get_active(&dst).expect("active destination");
        assert_eq!(
            MetadataVersion::from_location(entry.metadata_location())
                .expect("metadata version")
                .as_i64(),
            0
        );
        assert!(
            root.tombstones
                .iter()
                .any(|(table_id, entry)| table_id.as_str() == tombstoned_table_id && !entry.is_active())
        );
    }

    #[test]
    fn apply_commit_success() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");

        root.apply_commit(
            &key,
            "memory://catalog/tables/table-id/metadata/00001-next.metadata.json".to_string(),
        )
        .expect("apply commit");

        let entry = root.get_active(&key).expect("active entry");
        assert_eq!(
            MetadataVersion::from_location(entry.metadata_location())
                .expect("metadata version")
                .as_i64(),
            1
        );
    }

    #[test]
    fn apply_commit_fails_when_not_found() {
        let key = table_key(namespace(&["ns"]), "missing");
        let mut root = CatalogRoot::default();

        let error = root
            .apply_commit(
                &key,
                "memory://catalog/tables/table-id/metadata/00001-next.metadata.json".to_string(),
            )
            .expect_err("missing key must fail");

        assert!(matches!(error, Error::CommitConflict));
    }

    #[test]
    fn apply_commit_fails_when_tombstoned() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        root.tombstone(&key).expect("tombstone");

        let error = root
            .apply_commit(
                &key,
                "memory://catalog/tables/table-id/metadata/00001-next.metadata.json".to_string(),
            )
            .expect_err("tombstoned table must fail");

        assert!(matches!(error, Error::CommitConflict));
    }

    #[test]
    fn apply_commit_fails_on_non_sequential_version() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(1)).expect("seed table");

        let skip_error = root
            .apply_commit(
                &key,
                "memory://catalog/tables/table-id/metadata/00003-next.metadata.json".to_string(),
            )
            .expect_err("skipping a version must fail");
        assert!(matches!(skip_error, Error::CommitConflict));

        let regress_error = root
            .apply_commit(
                &key,
                "memory://catalog/tables/table-id/metadata/00001-prev.metadata.json".to_string(),
            )
            .expect_err("regressing a version must fail");
        assert!(matches!(regress_error, Error::CommitConflict));
    }

    #[test]
    fn apply_commit_fails_when_new_metadata_location_is_invalid() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");

        let error = root
            .apply_commit(
                &key,
                "memory://catalog/tables/table-id/metadata/invalid.json".to_string(),
            )
            .expect_err("invalid metadata location must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
        let entry = root.get_active(&key).expect("active entry remains unchanged");
        assert_eq!(
            MetadataVersion::from_location(entry.metadata_location())
                .expect("metadata version")
                .as_i64(),
            0
        );
    }

    #[test]
    fn active_tables_in_namespace_filters_and_sorts() {
        let ns = namespace(&["ns"]);
        let other_ns = namespace(&["other"]);
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.create_namespace(&other_ns, HashMap::new());

        root.link_table(table_key(ns.clone(), "b"), table_entry(0))
            .expect("create table b");
        root.link_table(table_key(ns.clone(), "a"), table_entry(1))
            .expect("create table a");
        root.link_table(table_key(other_ns.clone(), "x"), table_entry(2))
            .expect("create other namespace table");

        let tables = root.active_tables_in_namespace(&ns);

        assert_eq!(tables, vec![table_ident(ns.clone(), "a"), table_ident(ns, "b")]);
        assert_eq!(
            root.active_tables_in_namespace(&other_ns),
            vec![table_ident(other_ns, "x")]
        );
    }

    #[test]
    fn rename_tombstoned_table_preserves_entry() {
        let namespace = NamespaceIdent::new("ns".to_string());
        let src_ident = TableIdent::new(namespace.clone(), "src".to_string());
        let dst_ident = TableIdent::new(namespace.clone(), "dst".to_string());
        let src_key = TableKey::from_ident(&src_ident);
        let dst_key = TableKey::from_ident(&dst_ident);

        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(
            src_key.clone(),
            TableEntry::new(
                "table-id".to_string(),
                "memory://catalog/tables/table-id/metadata/00001-1.metadata.json".to_string(),
            ),
        )
        .expect("create table");
        root.tombstone(&src_key).expect("tombstone table");

        let result = root.rename(&src_key, dst_key);

        assert!(matches!(result, Err(Error::TableNotFound(ident)) if ident == src_ident));
        assert!(root.tombstones.iter().any(|(_, entry)| !entry.is_active()));
        assert!(root.get_active(&src_key).is_none());
    }

    #[test]
    fn rename_fails_when_destination_namespace_missing() {
        let src_ns = namespace(&["src_ns"]);
        let dst_ns = namespace(&["nonexistent"]);
        let src = table_key(src_ns.clone(), "tbl");
        let dst = table_key(dst_ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&src_ns, HashMap::new());
        root.link_table(src.clone(), table_entry(0)).expect("seed source table");

        let error = root.rename(&src, dst).expect_err("missing destination namespace must fail");

        assert!(matches!(error, Error::NamespaceNotFound(ns) if ns == dst_ns));
    }

    #[test]
    fn link_table_fails_when_namespace_missing() {
        let namespace = namespace(&["nonexistent"]);
        let key = table_key(namespace.clone(), "tbl");
        let mut root = CatalogRoot::default();

        let error = root.link_table(key, table_entry(0)).expect_err("missing namespace must fail");

        assert!(matches!(error, Error::NamespaceNotFound(ns) if ns == namespace));
    }

    #[test]
    fn list_namespaces_empty_catalog() {
        let root = CatalogRoot::default();

        assert!(root.list_namespaces(None).is_empty());
        assert!(root.list_namespaces(Some(&namespace(&["a"]))).is_empty());
    }

    #[test]
    fn list_namespaces_top_level_returns_distinct_roots() {
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace(&["a"]), HashMap::new());
        root.create_namespace(&namespace(&["a", "b"]), HashMap::new());
        root.create_namespace(&namespace(&["a", "c"]), HashMap::new());
        root.create_namespace(&namespace(&["z"]), HashMap::new());

        let result = root.list_namespaces(None);

        assert_eq!(result, vec![namespace(&["a"]), namespace(&["z"])]);
    }

    #[test]
    fn list_namespaces_with_parent_returns_direct_children() {
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace(&["a", "b"]), HashMap::new());
        root.create_namespace(&namespace(&["a", "c"]), HashMap::new());
        root.create_namespace(&namespace(&["z"]), HashMap::new());

        let result = root.list_namespaces(Some(&namespace(&["a"])));

        assert_eq!(result, vec![namespace(&["a", "b"]), namespace(&["a", "c"])]);
    }

    #[test]
    fn list_namespaces_ignores_namespaces_from_table_keys() {
        let ns = namespace(&["a", "b"]);
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(table_key(ns, "tbl"), table_entry(0)).expect("create table");

        let result = root.list_namespaces(None);

        assert!(
            result.is_empty(),
            "top-level list must not include 'a' since it was never explicitly created"
        );
    }

    #[test]
    fn list_namespaces_parent_not_matching_returns_empty() {
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace(&["a", "b"]), HashMap::new());

        let result = root.list_namespaces(Some(&namespace(&["x"])));

        assert!(result.is_empty());
    }

    #[test]
    fn drop_namespace_success() {
        let ns = namespace(&["ns"]);
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());

        root.drop_namespace(&ns).expect("drop empty namespace");

        assert!(root.get_namespace(&ns).is_none());
    }

    #[test]
    fn drop_namespace_fails_when_not_found() {
        let ns = namespace(&["missing"]);
        let mut root = CatalogRoot::default();

        let error = root.drop_namespace(&ns).expect_err("missing namespace must fail");

        assert!(matches!(error, Error::NamespaceNotFound(n) if n == ns));
    }

    #[test]
    fn drop_namespace_fails_when_has_active_table() {
        let ns = namespace(&["ns"]);
        let child_ns = namespace(&["ns", "child"]);
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.create_namespace(&child_ns, HashMap::new());
        root.link_table(table_key(child_ns, "tbl"), table_entry(0))
            .expect("create table");

        let error = root.drop_namespace(&ns).expect_err("namespace with descendant table must fail");

        assert!(matches!(error, Error::NamespaceNotEmpty(n) if n == ns));
    }

    #[test]
    fn drop_namespace_fails_when_has_descendant_namespace() {
        let ns = namespace(&["ns"]);
        let child_ns = namespace(&["ns", "child"]);
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.create_namespace(&child_ns, HashMap::new());

        let error = root
            .drop_namespace(&ns)
            .expect_err("namespace with descendant namespace must fail");

        assert!(matches!(error, Error::NamespaceNotEmpty(n) if n == ns));
    }
}
