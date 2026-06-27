//! Domain model for catalog state.

use std::collections::{BTreeSet, HashMap};

use iceberg::TableUpdate as IcebergTableUpdate;
use iceberg::spec::{TableMetadata, TableMetadataBuilder};
use iceberg::{NamespaceIdent, TableCreation, TableIdent, TableRequirement};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Error;

/// Version of an Iceberg metadata file commit.
///
/// This value matches the numeric prefix in metadata filenames like
/// `00042-uuid.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
struct MetadataVersion(u32);

impl MetadataVersion {
    /// Initial metadata version for newly created tables.
    const fn initial() -> Self {
        Self(0)
    }

    /// Parse metadata version from an Iceberg metadata file location.
    fn from_location(location: &str) -> Result<Self, Error> {
        let filename = location
            .rsplit('/')
            .next()
            .filter(|segment| !segment.is_empty())
            .ok_or_else(|| Error::InvalidMetadata(format!("Invalid metadata location: {location}")))?;
        let version = filename
            .split_once('-')
            .map(|(prefix, _)| prefix)
            .ok_or_else(|| Error::InvalidMetadata(format!("Missing metadata version in location: {location}")))?;

        version.parse::<u32>().map(Self).map_err(|error| {
            Error::InvalidMetadata(format!(
                "Invalid metadata version '{version}' in location {location}: {error}"
            ))
        })
    }

    /// Return the next metadata version.
    fn next(self) -> Result<Self, Error> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(|| Error::InvalidMetadata(format!("Metadata version overflow: {}", self.0)))
    }

    /// Return the numeric value of this metadata version.
    const fn as_u32(self) -> u32 {
        self.0
    }

    /// Check whether `self` is the immediate predecessor (version N) of `other` (version N+1).
    ///
    /// Used to decide whether metadata built over `self` can be
    /// linked into a root entry currently pointing at `self` without rebuilding.
    fn is_direct_predecessor_of(self, other: Self) -> bool {
        self.next().is_ok_and(|expected| expected.as_u32() == other.as_u32())
    }
}

/// A table prepared for publication into the catalog root.
///
/// Single in-memory entity for both write paths: [`TableUpdate::create`] (a new
/// table) and [`IcebergTableMetadata::prepare_commit`] (a new version of an existing
/// table). Built once outside the CAS retry loop so the UUID-suffixed metadata path
/// is stable across retries. The root key and the root-facing link are derived on
/// demand, never stored, so each table's data lives in exactly one place.
#[derive(Debug, Clone)]
pub(crate) struct TableUpdate {
    /// Full table identifier (namespace + name).
    pub(crate) full_name: TableIdent,
    /// Root entry this update was prepared against; absent for brand-new tables.
    pub(crate) persisted: Option<CatalogTableLink>,
    /// Metadata file written to storage but not yet linked from the root.
    pub(crate) updated: IcebergTableMetadata,
}

impl TableUpdate {
    /// Build the inputs for a brand-new table without mutating the catalog root.
    ///
    /// The UUID and resulting metadata file are computed once and reused across retries,
    /// so a CAS conflict on the root does not leak a metadata file per attempt. Pair with
    /// [`CatalogRoot::create_table`] inside a retry loop to publish.
    ///
    /// # Errors
    ///
    /// Returns an error if the `TableCreation` is invalid or metadata cannot be built.
    pub(crate) fn create(
        namespace: &NamespaceIdent,
        creation: TableCreation,
        tables_uri_prefix: &str,
    ) -> Result<Self, Error> {
        let uuid = Uuid::new_v4();
        let table_location = creation
            .location
            .clone()
            .unwrap_or_else(|| format!("{tables_uri_prefix}/{uuid}"));
        let creation = TableCreation {
            location: Some(table_location),
            ..creation
        };
        let full_name = TableIdent::new(namespace.clone(), creation.name.clone());
        let updated = IcebergTableMetadata::create(creation, uuid)?;
        Ok(Self {
            full_name,
            persisted: None,
            updated,
        })
    }

    /// Root key `{namespace}.{table}`, derived from the identifier.
    pub(crate) fn to_key(&self) -> TableKey {
        TableKey::from_ident(&self.full_name)
    }

    /// Root-facing link (stable table id + metadata location), derived from updated metadata.
    pub(crate) fn to_link(&self) -> CatalogTableLink {
        CatalogTableLink::new(
            TableId::from(self.updated.metadata().uuid()),
            self.updated.metadata_location().clone(),
        )
    }

    /// Metadata version baked into the updated metadata location.
    fn metadata_version(&self) -> Result<MetadataVersion, Error> {
        MetadataVersion::from_location(self.updated.metadata_location().as_str())
    }

    fn persisted(&self) -> Result<&CatalogTableLink, Error> {
        self.persisted
            .as_ref()
            .ok_or_else(|| Error::InvalidMetadata("prepared commit missing persisted root entry".to_string()))
    }
}

/// Borrowed view of one table's commit state, passed to
/// [`CatalogRoot::merge_transaction`].
///
/// Keeps the domain layer independent of the orchestration-owned
/// `PendingCommit` (in `catalog.rs`): the catalog projects each retry-round
/// entry into this ref so `model.rs` never depends on the upper layer.
pub(crate) struct PreparedCommitRef<'a> {
    /// Root key of the table being committed.
    pub(crate) key: &'a TableKey,
    /// Metadata file prepared on top of the observed head, or `None` when the
    /// commit still needs to be prepared before it can be merged.
    pub(crate) table: Option<&'a TableUpdate>,
}

/// Outcome of [`CatalogRoot::merge_transaction`].
#[derive(Debug)]
pub(crate) enum MergeTransactionResult {
    /// All prepared pointers applied — caller proceeds with `save_root`.
    Ready,
    /// Some prepared metadata files are no longer direct successors of the
    /// current heads. Caller must rebuild the listed entries on top of the
    /// freshly loaded root and call `merge_transaction` again.
    NeedRebuild(TransactionMergeOutcome),
}

/// Listing of prepared commits that must be rebuilt before the next merge attempt.
#[derive(Debug)]
pub(crate) struct TransactionMergeOutcome {
    /// Keys of prepared commits that no longer sit on top of their table's head.
    pub(crate) rebuild: Vec<TableKey>,
}

/// Domain state that manages Iceberg table metadata updates.
#[derive(Debug, Clone)]
pub(crate) struct IcebergTableMetadata {
    metadata_location: TableMetadataLocation,
    metadata: TableMetadata,
}

impl IcebergTableMetadata {
    pub(crate) const fn new(metadata_location: TableMetadataLocation, metadata: TableMetadata) -> Self {
        Self {
            metadata_location,
            metadata,
        }
    }

    /// Build initial table metadata and wrap it into a `TableMetadataState`.
    ///
    /// # Errors
    ///
    /// Returns an error if the `TableCreation` is invalid or metadata cannot be built.
    pub(crate) fn create(creation: TableCreation, uuid: Uuid) -> Result<Self, Error> {
        // `from_table_creation` reassigns every field id to a fresh sequence —
        // the Iceberg default for a brand-new table. icegate, however, depends
        // on *fixed* schema field ids end to end: the WAL and the shifter write
        // Arrow data tagged with the ids from `icegate-common`'s hand-built
        // schemas, so a reassigned table no longer matches the data written into
        // it (the shifter fails with "Field id N not found in struct array").
        // A REST catalog never reassigns — it persists the schema the client
        // sent verbatim — which is why the same tables work there. Reproduce
        // that here: re-apply the caller's original schema, partition spec, and
        // sort order on top of the reassigned skeleton. `add_schema` /
        // `add_default_*` keep the supplied field ids verbatim (only `new`
        // reassigns), so the persisted table matches the WAL.
        let original_schema = creation.schema.clone();
        let original_spec = creation.partition_spec.clone();
        let original_sort_order = creation.sort_order.clone();

        let mut builder = TableMetadataBuilder::from_table_creation(creation)?
            .assign_uuid(uuid)
            .add_current_schema(original_schema)?;
        if let Some(spec) = original_spec {
            builder = builder.add_default_partition_spec(spec)?;
        }
        if let Some(sort_order) = original_sort_order {
            builder = builder
                .add_sort_order(sort_order)?
                .set_default_sort_order(i64::from(TableMetadataBuilder::LAST_ADDED))?;
        }
        let metadata = builder.build()?.metadata;
        let metadata_location = Self::next_metadata_path(metadata.location(), MetadataVersion::initial());
        Ok(Self::new(TableMetadataLocation::from(metadata_location), metadata))
    }

    pub(crate) const fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    pub(crate) const fn metadata_location(&self) -> &TableMetadataLocation {
        &self.metadata_location
    }

    pub(crate) fn prepare_commit(
        self,
        full_name: TableIdent,
        persisted: CatalogTableLink,
        requirements: Vec<TableRequirement>,
        updates: Vec<IcebergTableUpdate>,
    ) -> Result<TableUpdate, Error> {
        for requirement in requirements {
            requirement.check(Some(&self.metadata))?;
        }

        let mut builder = self.metadata.into_builder(Some(self.metadata_location.as_str().to_string()));
        for update in updates {
            builder = update.apply(builder)?;
        }

        let build = builder.build()?;
        if build.metadata.location().is_empty() {
            return Err(Error::InvalidMetadata(format!("empty table location for {full_name}")));
        }

        let metadata_version = MetadataVersion::from_location(self.metadata_location.as_str())?.next()?;
        let metadata_location = Self::next_metadata_path(build.metadata.location(), metadata_version);

        Ok(TableUpdate {
            full_name,
            persisted: Some(persisted),
            updated: Self::new(TableMetadataLocation::from(metadata_location), build.metadata),
        })
    }

    fn next_metadata_path(table_location: &str, version: MetadataVersion) -> String {
        format!(
            "{}/metadata/{:05}-{}.metadata.json",
            table_location.trim_end_matches('/'),
            version.as_u32(),
            Uuid::new_v4()
        )
    }
}

/// Table entry inside the catalog root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CatalogTableLink {
    /// Stable table identifier.
    table_id: TableId,
    /// Logical status.
    status: TableStatus,
    /// Full metadata location URI.
    metadata_location: TableMetadataLocation,
}

impl CatalogTableLink {
    pub(crate) const fn new(table_id: TableId, metadata_location: TableMetadataLocation) -> Self {
        Self {
            table_id,
            status: TableStatus::Active,
            metadata_location,
        }
    }

    pub(crate) const fn table_id(&self) -> &TableId {
        &self.table_id
    }

    pub(crate) const fn metadata_location(&self) -> &TableMetadataLocation {
        &self.metadata_location
    }

    pub(crate) fn is_active(&self) -> bool {
        self.status == TableStatus::Active
    }

    fn validate_metadata_location(&self) -> Result<(), Error> {
        let _ = MetadataVersion::from_location(self.metadata_location.as_str())?;
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct TableId(Uuid);

impl TableId {
    pub(crate) const fn new(table_id: Uuid) -> Self {
        Self(table_id)
    }

    const fn from_entry(entry: &CatalogTableLink) -> Self {
        entry.table_id
    }

    pub(crate) const fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl From<Uuid> for TableId {
    fn from(table_id: Uuid) -> Self {
        Self::new(table_id)
    }
}

/// Full Iceberg table metadata file location stored in the root index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct TableMetadataLocation(String);

impl TableMetadataLocation {
    pub(crate) const fn new(metadata_location: String) -> Self {
        Self(metadata_location)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for TableMetadataLocation {
    fn from(metadata_location: String) -> Self {
        Self::new(metadata_location)
    }
}

impl From<&str> for TableMetadataLocation {
    fn from(metadata_location: &str) -> Self {
        Self::new(metadata_location.to_string())
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
    tables: HashMap<TableKey, CatalogTableLink>,
    /// Tombstoned table entries indexed by stable table id.
    #[serde(default)]
    tombstones: HashMap<TableId, CatalogTableLink>,
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
        let key = NamespaceKey::from_ident(namespace);
        if !self.namespaces.contains_key(&key) {
            return Err(Error::NamespaceNotFound(namespace.clone()));
        }

        if self.has_active_tables_in_namespace_tree(namespace) || self.has_namespace_descendants(namespace) {
            return Err(Error::NamespaceNotEmpty(namespace.clone()));
        }

        self.namespaces.remove(&key);
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

    pub(crate) fn get_active(&self, key: &TableKey) -> Option<&CatalogTableLink> {
        self.tables.get(key).filter(|entry| entry.is_active())
    }

    pub(crate) fn active_entries(&self) -> impl Iterator<Item = (&TableKey, &CatalogTableLink)> {
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

    /// Check whether `namespace` exists; used as a pre-flight before mutating ops.
    pub(crate) fn require_namespace(&self, namespace: &NamespaceIdent) -> Result<(), Error> {
        if self.get_namespace(namespace).is_none() {
            Err(Error::NamespaceNotFound(namespace.clone()))
        } else {
            Ok(())
        }
    }

    /// Insert a freshly prepared table into the catalog root.
    ///
    /// The metadata file referenced by `prepared_table.to_link()` is expected to be
    /// already published on storage so the link is the final visible step.
    ///
    /// # Errors
    ///
    /// - [`Error::NamespaceNotFound`] — the target namespace does not exist.
    /// - [`Error::TableAlreadyExists`] — a different table already occupies this key.
    /// - [`Error::InvalidMetadata`] — entry invariants violated (bad metadata location
    ///   format, duplicate UUID under another key).
    pub(crate) fn create_table(&mut self, prepared_table: &TableUpdate) -> Result<(), Error> {
        let key = prepared_table.to_key();
        let link = prepared_table.to_link();

        if let Some(existing) = self.get_active(&key) {
            // Lost-response convergence: an earlier CAS from this client landed (same
            // UUID-suffixed metadata location, same generated table_id), but the response
            // was lost. The retry sees its own write and must succeed.
            if existing.table_id() == link.table_id() && existing.metadata_location() == link.metadata_location() {
                return Ok(());
            }
            return Err(Error::TableAlreadyExists(prepared_table.full_name.clone()));
        }

        self.link_table(key, link)?;
        Ok(())
    }

    /// Validate that a freshly prepared table can be linked into this root.
    ///
    /// This is a strict preflight for callers that have not yet written table
    /// metadata. It intentionally does not apply the lost-response convergence
    /// path from [`Self::create_table`], because before the metadata write an
    /// already occupied name is always a deterministic rejection.
    pub(crate) fn validate_new_table(&self, prepared_table: &TableUpdate) -> Result<(), Error> {
        let key = prepared_table.to_key();
        let link = prepared_table.to_link();
        self.validate_new_table_link(&key, &link)
    }

    /// Merge a batch of prepared commits into this root.
    ///
    /// For every entry whose prepared table is absent or whose active root entry
    /// still has the same table id but no longer points at the commit-base
    /// metadata location, the key is collected into
    /// [`TransactionMergeOutcome::rebuild`]. Otherwise the prepared pointer is
    /// linked via [`Self::apply_commit`].
    ///
    /// Lost-response convergence: when the current head already points at the
    /// exact metadata file to publish (our own prior CAS physically landed but the
    /// ack was lost), the commit is treated as applied — no rebuild, no
    /// advance — so the retry reports success instead of stacking a redundant
    /// successor. This mirrors the convergence handling in
    /// [`Self::create_table`] and `register_table`.
    ///
    /// When any rebuild is required, the root is left in whatever state the
    /// already-applied subset produced — the caller must discard this `self`
    /// and reload the root before retrying.
    ///
    /// # Errors
    ///
    /// Propagates [`Error::CommitConflict`] when the active root entry has a
    /// different table id, or from [`Self::apply_commit`] when the exact
    /// commit-base CAS no longer holds. Those are terminal table-state conflicts, not
    /// rebuild signals.
    pub(crate) fn merge_transaction(
        &mut self,
        commits: &[PreparedCommitRef<'_>],
    ) -> Result<MergeTransactionResult, Error> {
        let mut rebuild = Vec::new();

        for commit in commits {
            let Some(table) = commit.table else {
                rebuild.push(commit.key.clone());
                continue;
            };

            let entry = self
                .get_active(commit.key)
                .ok_or_else(|| Error::TableNotFound(table.full_name.clone()))?;
            let persisted = table.persisted()?;

            if entry.table_id() != persisted.table_id() {
                return Err(Error::CommitConflict);
            }

            // Lost-response convergence: the head already points at the exact
            // UUID-suffixed file we prepared, so our own prior CAS physically
            // landed and only the ack was lost. Treat as applied — skipping
            // both the rebuild branch (which would stack a redundant N+2) and
            // `apply_commit` (which would reject equal versions as a conflict).
            if entry.metadata_location() == table.updated.metadata_location() {
                continue;
            }

            if entry.metadata_location() != persisted.metadata_location() {
                rebuild.push(commit.key.clone());
                continue;
            }

            let current_version = MetadataVersion::from_location(entry.metadata_location().as_str())?;
            let publish_version = table.metadata_version()?;
            if !current_version.is_direct_predecessor_of(publish_version) {
                rebuild.push(commit.key.clone());
                continue;
            }

            self.apply_commit(commit.key, persisted, table.updated.metadata_location().clone())?;
        }

        if rebuild.is_empty() {
            Ok(MergeTransactionResult::Ready)
        } else {
            Ok(MergeTransactionResult::NeedRebuild(TransactionMergeOutcome { rebuild }))
        }
    }

    /// Link an existing table entry (pre-built key and entry) into the catalog.
    ///
    /// Used for `register_table` where the metadata location comes from the caller.
    pub(crate) fn link_table(&mut self, key: TableKey, entry: CatalogTableLink) -> Result<(), Error> {
        self.validate_new_table_link(&key, &entry)?;
        self.tables.insert(key, entry);
        Ok(())
    }

    fn validate_new_table_link(&self, key: &TableKey, entry: &CatalogTableLink) -> Result<(), Error> {
        entry.validate_metadata_location()?;

        let (namespace, _) = key
            .split()
            .ok_or_else(|| Error::InvalidMetadata(format!("Invalid table key: {}", key.as_str())))?;
        self.require_namespace(&namespace)?;

        if self.get_active(key).is_some() {
            return Err(Error::TableAlreadyExists(Self::table_ident_from_key(key)?));
        }

        let table_id = TableId::from_entry(entry);
        if self
            .active_entries()
            .any(|(_, active_entry)| active_entry.table_id() == &table_id)
        {
            return Err(Error::InvalidMetadata(format!(
                "Duplicate table id in catalog root: {}",
                table_id.as_uuid()
            )));
        }

        Ok(())
    }

    pub(crate) fn apply_commit(
        &mut self,
        key: &TableKey,
        persisted: &CatalogTableLink,
        metadata_location: TableMetadataLocation,
    ) -> Result<(), Error> {
        let entry = self.tables.get_mut(key).ok_or(Error::CommitConflict)?;
        if !entry.is_active() {
            return Err(Error::CommitConflict);
        }

        if entry.table_id() != persisted.table_id() || entry.metadata_location() != persisted.metadata_location() {
            return Err(Error::CommitConflict);
        }

        let old_version = MetadataVersion::from_location(entry.metadata_location.as_str())?;
        let new_version = MetadataVersion::from_location(metadata_location.as_str())?;
        let expected = old_version.as_u32().checked_add(1).ok_or(Error::CommitConflict)?;
        if new_version.as_u32() != expected {
            return Err(Error::CommitConflict);
        }

        entry.metadata_location = metadata_location;
        Ok(())
    }

    pub(crate) fn tombstone(&mut self, key: &TableKey, table_id: &TableId) -> Result<(), Error> {
        let table_ident = Self::table_ident_from_key(key)?;

        if let Some(entry) = self.get_active(key) {
            if entry.table_id() != table_id {
                return Err(Error::TableNotFound(table_ident));
            }

            let mut entry = self
                .tables
                .remove(key)
                .ok_or_else(|| Error::TableNotFound(table_ident.clone()))?;
            entry.status = TableStatus::Tombstoned;
            self.tombstones.insert(TableId::from_entry(&entry), entry);
            return Ok(());
        }

        if self
            .tombstones
            .get(table_id)
            .is_some_and(|entry| !entry.is_active() && entry.table_id() == table_id)
        {
            return Ok(());
        }

        Err(Error::TableNotFound(table_ident))
    }

    pub(crate) fn rename(&mut self, src: &TableKey, dst: TableKey, table_id: &TableId) -> Result<(), Error> {
        let (dst_namespace, _) = dst
            .split()
            .ok_or_else(|| Error::InvalidMetadata(format!("Invalid table key: {}", dst.as_str())))?;
        if self.get_namespace(&dst_namespace).is_none() {
            return Err(Error::NamespaceNotFound(dst_namespace));
        }

        if let Some(destination) = self.get_active(&dst) {
            if destination.table_id() == table_id && self.get_active(src).is_none() {
                return Ok(());
            }
            return Err(Error::TableAlreadyExists(Self::table_ident_from_key(&dst)?));
        }

        let source_ident = Self::table_ident_from_key(src)?;
        let source = self.tables.get(src).ok_or_else(|| Error::TableNotFound(source_ident.clone()))?;
        if !source.is_active() || source.table_id() != table_id {
            return Err(Error::TableNotFound(source_ident));
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
    use std::sync::Arc;

    use iceberg::TableCreation;
    use iceberg::spec::{
        FormatVersion, MapType, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
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

    fn table_id(value: u128) -> TableId {
        TableId::from(Uuid::from_u128(value))
    }

    fn metadata_location(value: impl Into<String>) -> TableMetadataLocation {
        TableMetadataLocation::new(value.into())
    }

    fn table_entry(version: i64) -> CatalogTableLink {
        CatalogTableLink::new(
            table_id(u128::try_from(version).expect("non-negative version") + 1),
            metadata_location(format!(
                "memory://catalog/tables/table-id/metadata/{version:05}-uuid.metadata.json"
            )),
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

        assert_eq!(version.as_u32(), 42);
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

        assert_eq!(version.as_u32(), 7);
    }

    #[test]
    fn metadata_version_next_overflows() {
        let error = MetadataVersion(u32::MAX).next().expect_err("overflow must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[test]
    fn next_metadata_path_uses_metadata_json_suffix() {
        let state = IcebergTableMetadata::create(test_table_creation("s3://bucket/tables/tbl"), Uuid::new_v4())
            .expect("create table state");

        let location = state.metadata_location();

        assert!(
            location.as_str().ends_with(".metadata.json"),
            "expected .metadata.json suffix, got: {}",
            location.as_str()
        );
        assert!(
            !location.as_str().ends_with("..metadata.json"),
            "must not have double dot before .metadata.json: {}",
            location.as_str()
        );
    }

    fn test_table_creation(location: &str) -> TableCreation {
        TableCreation::builder()
            .name("tbl".to_string())
            .schema(test_schema())
            .location(location.to_string())
            .build()
    }

    /// A schema mirroring icegate's interleaved id layout: a `Map` field whose
    /// nested key/value ids (3/4) sit *between* top-level fields. Iceberg's
    /// fresh-id reassignment numbers every top-level field before any nested
    /// one, so it would move the map key/value off 3/4.
    fn interleaved_map_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "service", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    2,
                    "attributes",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::required(3, "key", Type::Primitive(PrimitiveType::String))),
                        Arc::new(NestedField::required(
                            4,
                            "value",
                            Type::Primitive(PrimitiveType::String),
                        )),
                    )),
                )
                .into(),
                NestedField::required(5, "name", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema")
    }

    #[test]
    fn create_preserves_caller_field_ids() {
        // The S3 catalog must persist the schema the caller supplied verbatim
        // (like a REST catalog), not iceberg's reassigned ids — icegate's
        // WAL/shifter write Arrow data tagged with the original field ids.
        let creation = TableCreation::builder()
            .name("spans".to_string())
            .schema(interleaved_map_schema())
            .location("s3://bucket/tables/spans".to_string())
            .build();

        let state = IcebergTableMetadata::create(creation, Uuid::new_v4()).expect("create table state");
        let schema = state.metadata().current_schema();

        let attributes = schema.field_by_name("attributes").expect("attributes field");
        let Type::Map(map) = attributes.field_type.as_ref() else {
            panic!("attributes field is not a map");
        };
        assert_eq!(map.key_field.id, 3, "map key field id must be preserved");
        assert_eq!(map.value_field.id, 4, "map value field id must be preserved");
        assert_eq!(schema.field_by_name("service").expect("service field").id, 1);
        assert_eq!(schema.field_by_name("name").expect("name field").id, 5);
    }

    #[test]
    fn create_location_has_no_double_slash() {
        let with_slash = IcebergTableMetadata::create(test_table_creation("s3://bucket/tables/tbl/"), Uuid::new_v4())
            .expect("create with trailing slash location");
        let without_slash = IcebergTableMetadata::create(test_table_creation("s3://bucket/tables/tbl"), Uuid::new_v4())
            .expect("create without trailing slash location");

        assert!(
            with_slash
                .metadata_location()
                .as_str()
                .starts_with("s3://bucket/tables/tbl/metadata/00000-")
        );
        assert!(
            without_slash
                .metadata_location()
                .as_str()
                .starts_with("s3://bucket/tables/tbl/metadata/00000-")
        );
        assert!(with_slash.metadata_location().as_str().ends_with(".metadata.json"));
        assert!(without_slash.metadata_location().as_str().ends_with(".metadata.json"));

        let with_uuid = with_slash
            .metadata_location()
            .as_str()
            .rsplit('/')
            .next()
            .expect("metadata filename with trailing slash location")
            .strip_prefix("00000-")
            .expect("version prefix")
            .trim_end_matches(".metadata.json");
        let without_uuid = without_slash
            .metadata_location()
            .as_str()
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
        let active_metadata_location = "memory://catalog/tables/tbl/metadata/00000-initial.metadata.json".to_string();
        let metadata = test_table_metadata("memory://catalog/tables/tbl");
        let persisted = CatalogTableLink::new(
            TableId::from(metadata.uuid()),
            metadata_location(active_metadata_location.clone()),
        );

        let prepared = IcebergTableMetadata::new(metadata_location(active_metadata_location), metadata.clone())
            .prepare_commit(identifier.clone(), persisted.clone(), Vec::new(), Vec::new())
            .expect("prepare commit");

        assert_eq!(prepared.full_name, identifier);
        let prepared_persisted = prepared.persisted.as_ref().expect("persisted root entry");
        assert_eq!(prepared_persisted.table_id(), persisted.table_id());
        assert_eq!(prepared_persisted.metadata_location(), persisted.metadata_location());
        assert_eq!(prepared.to_key(), key);
        assert_eq!(prepared.updated.metadata().uuid(), metadata.uuid());
        assert!(
            prepared
                .updated
                .metadata_location()
                .as_str()
                .starts_with("memory://catalog/tables/tbl/metadata/00001-")
        );
    }

    #[test]
    fn prepared_table_derives_key_and_link() {
        let namespace = namespace(&["ns"]);
        let identifier = table_ident(namespace.clone(), "tbl");
        let location = "memory://catalog/tables/tbl/metadata/00000-x.metadata.json".to_string();
        let metadata = test_table_metadata("memory://catalog/tables/tbl");

        let prepared = TableUpdate {
            full_name: identifier,
            persisted: None,
            updated: IcebergTableMetadata::new(metadata_location(location.clone()), metadata.clone()),
        };

        assert_eq!(prepared.to_key(), table_key(namespace, "tbl"));
        let link = prepared.to_link();
        assert_eq!(link.table_id(), &TableId::from(metadata.uuid()));
        assert_eq!(link.metadata_location().as_str(), location);
    }

    #[test]
    fn prepare_commit_fails_when_built_table_location_is_empty() {
        let identifier = table_ident(namespace(&["ns"]), "tbl");
        let active_metadata_location = "memory://catalog/tables/tbl/metadata/00000-initial.metadata.json".to_string();

        let error = IcebergTableMetadata::new(metadata_location(active_metadata_location), test_table_metadata(""))
            .prepare_commit(
                identifier,
                CatalogTableLink::new(
                    TableId::from(Uuid::new_v4()),
                    metadata_location("memory://catalog/tables/tbl/metadata/00000-initial.metadata.json"),
                ),
                Vec::new(),
                Vec::new(),
            )
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
        assert_eq!(entry.table_id(), &table_id(1));
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
                CatalogTableLink::new(
                    table_id(42),
                    metadata_location("memory://catalog/tables/table-id/metadata/invalid.json"),
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
        let original_table_id = *root.tables.get(&key).expect("active table before tombstone").table_id();
        root.tombstone(&key, &original_table_id).expect("tombstone");

        root.link_table(key.clone(), table_entry(1)).expect("recreate over tombstone");

        let entry = root.get_active(&key).expect("active entry after recreate");
        assert_eq!(
            MetadataVersion::from_location(entry.metadata_location().as_str())
                .expect("metadata version")
                .as_u32(),
            1
        );
        assert_eq!(
            root.tombstones.iter().filter(|(_, entry)| !entry.is_active()).count(),
            1
        );
        assert!(
            root.tombstones
                .iter()
                .any(|(table_id, entry)| table_id == &original_table_id && !entry.is_active())
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
        let tombstoned_id = *root.tables.get(&first_key).expect("active table before tombstone").table_id();
        root.tombstone(&first_key, &tombstoned_id).expect("tombstone");

        root.link_table(second_key.clone(), table_entry(0))
            .expect("re-register tombstoned uuid under new name");

        let entry = root.get_active(&second_key).expect("active entry under new name");
        assert_eq!(entry.table_id(), &tombstoned_id);
    }

    #[test]
    fn tombstone_success() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        let table_id = *root.get_active(&key).expect("active table").table_id();

        root.tombstone(&key, &table_id).expect("tombstone");

        assert!(root.get_active(&key).is_none());
        assert!(root.tombstones.iter().any(|(_, entry)| !entry.is_active()));
    }

    #[test]
    fn tombstone_fails_when_not_found() {
        let namespace = namespace(&["ns"]);
        let ident = table_ident(namespace.clone(), "missing");
        let key = table_key(namespace, "missing");
        let mut root = CatalogRoot::default();

        let error = root.tombstone(&key, &table_id(9_999)).expect_err("missing key must fail");

        assert!(matches!(error, Error::TableNotFound(table) if table == ident));
    }

    #[test]
    fn tombstone_converges_when_matching_tombstone_exists() {
        let namespace = namespace(&["ns"]);
        let key = table_key(namespace.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        let table_id = *root.get_active(&key).expect("active table").table_id();
        root.tombstone(&key, &table_id).expect("first tombstone");

        root.tombstone(&key, &table_id).expect("matching tombstone must converge");

        assert!(root.get_active(&key).is_none());
    }

    #[test]
    fn tombstone_rejects_foreign_active_occupant() {
        let namespace = namespace(&["ns"]);
        let ident = table_ident(namespace.clone(), "tbl");
        let key = table_key(namespace.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed original table");
        let original_table_id = *root.get_active(&key).expect("active table").table_id();
        root.tombstone(&key, &original_table_id).expect("drop original table");
        root.link_table(key.clone(), table_entry(1)).expect("seed foreign table");

        let error = root
            .tombstone(&key, &original_table_id)
            .expect_err("foreign occupant must not be dropped");

        assert!(matches!(error, Error::TableNotFound(table) if table == ident));
        assert_eq!(
            root.get_active(&key).expect("foreign table remains").table_id(),
            &table_id(2)
        );
    }

    #[test]
    fn rename_success() {
        let namespace = namespace(&["ns"]);
        let src = table_key(namespace.clone(), "src");
        let dst = table_key(namespace.clone(), "dst");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(src.clone(), table_entry(0)).expect("seed table");
        let table_id = *root.get_active(&src).expect("active source").table_id();

        root.rename(&src, dst.clone(), &table_id).expect("rename");

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

        let error = root.rename(&src, dst, &table_id(9_999)).expect_err("missing source must fail");

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
        let source_table_id = *root.get_active(&src).expect("active source").table_id();

        let error = root
            .rename(&src, dst, &source_table_id)
            .expect_err("active destination must fail");

        assert!(matches!(error, Error::TableAlreadyExists(table) if table == dst_ident));
        assert!(root.get_active(&src).is_some());
    }

    #[test]
    fn rename_converges_when_destination_has_same_table_id() {
        let namespace = namespace(&["ns"]);
        let src = table_key(namespace.clone(), "src");
        let dst = table_key(namespace.clone(), "dst");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(src.clone(), table_entry(0)).expect("seed src");
        let table_id = *root.get_active(&src).expect("active source").table_id();
        root.rename(&src, dst.clone(), &table_id).expect("first rename");

        root.rename(&src, dst.clone(), &table_id)
            .expect("matching destination must converge");

        assert!(root.get_active(&src).is_none());
        assert_eq!(root.get_active(&dst).expect("renamed table").table_id(), &table_id);
    }

    #[test]
    fn rename_rejects_foreign_destination_occupant() {
        let namespace = namespace(&["ns"]);
        let src = table_key(namespace.clone(), "src");
        let dst_ident = table_ident(namespace.clone(), "dst");
        let dst = table_key(namespace.clone(), "dst");
        let mut root = CatalogRoot::default();
        root.create_namespace(&namespace, HashMap::new());
        root.link_table(src.clone(), table_entry(0)).expect("seed src");
        let source_table_id = *root.get_active(&src).expect("active source").table_id();
        root.tombstone(&src, &source_table_id).expect("remove source");
        root.link_table(dst.clone(), table_entry(1)).expect("seed foreign destination");

        let error = root
            .rename(&src, dst, &source_table_id)
            .expect_err("foreign destination must reject convergence");

        assert!(matches!(error, Error::TableAlreadyExists(table) if table == dst_ident));
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
        let source_table_id = *root.get_active(&src).expect("active source").table_id();
        let tombstoned_table_id = *root.tables.get(&dst).expect("active destination table").table_id();
        root.tombstone(&dst, &tombstoned_table_id).expect("tombstone dst");

        root.rename(&src, dst.clone(), &source_table_id).expect("rename over tombstone");

        assert!(!root.tables.contains_key(&src));
        let entry = root.get_active(&dst).expect("active destination");
        assert_eq!(
            MetadataVersion::from_location(entry.metadata_location().as_str())
                .expect("metadata version")
                .as_u32(),
            0
        );
        assert!(
            root.tombstones
                .iter()
                .any(|(table_id, entry)| table_id == &tombstoned_table_id && !entry.is_active())
        );
    }

    #[test]
    fn apply_commit_success() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        let base_entry = root.get_active(&key).expect("active base entry").clone();

        root.apply_commit(
            &key,
            &base_entry,
            metadata_location("memory://catalog/tables/table-id/metadata/00001-next.metadata.json"),
        )
        .expect("apply commit");

        let entry = root.get_active(&key).expect("active entry");
        assert_eq!(
            MetadataVersion::from_location(entry.metadata_location().as_str())
                .expect("metadata version")
                .as_u32(),
            1
        );
    }

    #[test]
    fn apply_commit_fails_when_persisted_identity_mismatches() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        let base_entry = root.get_active(&key).expect("active base entry").clone();
        let persisted = CatalogTableLink::new(table_id(9_998), base_entry.metadata_location().clone());

        let error = root
            .apply_commit(
                &key,
                &persisted,
                metadata_location("memory://catalog/tables/table-id/metadata/00001-next.metadata.json"),
            )
            .expect_err("different table id must fail");

        assert!(matches!(error, Error::CommitConflict));
        assert_eq!(
            root.get_active(&key).expect("active entry").metadata_location().as_str(),
            base_entry.metadata_location().as_str()
        );
    }

    #[test]
    fn apply_commit_fails_when_persisted_location_mismatches() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        let base_entry = root.get_active(&key).expect("active base entry").clone();
        let persisted = CatalogTableLink::new(
            *base_entry.table_id(),
            metadata_location("memory://catalog/tables/table-id/metadata/00000-other.metadata.json"),
        );

        let error = root
            .apply_commit(
                &key,
                &persisted,
                metadata_location("memory://catalog/tables/table-id/metadata/00001-next.metadata.json"),
            )
            .expect_err("different persisted metadata location must fail");

        assert!(matches!(error, Error::CommitConflict));
        assert_eq!(
            root.get_active(&key).expect("active entry").metadata_location().as_str(),
            base_entry.metadata_location().as_str()
        );
    }

    #[test]
    fn apply_commit_fails_when_not_found() {
        let key = table_key(namespace(&["ns"]), "missing");
        let mut root = CatalogRoot::default();
        let persisted = CatalogTableLink::new(
            table_id(1),
            metadata_location("memory://catalog/tables/table-id/metadata/00000-uuid.metadata.json"),
        );

        let error = root
            .apply_commit(
                &key,
                &persisted,
                metadata_location("memory://catalog/tables/table-id/metadata/00001-next.metadata.json"),
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
        let base_entry = root.get_active(&key).expect("active base entry").clone();
        root.tombstone(&key, base_entry.table_id()).expect("tombstone");

        let error = root
            .apply_commit(
                &key,
                &base_entry,
                metadata_location("memory://catalog/tables/table-id/metadata/00001-next.metadata.json"),
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
        let base_entry = root.get_active(&key).expect("active base entry").clone();

        let skip_error = root
            .apply_commit(
                &key,
                &base_entry,
                metadata_location("memory://catalog/tables/table-id/metadata/00003-next.metadata.json"),
            )
            .expect_err("skipping a version must fail");
        assert!(matches!(skip_error, Error::CommitConflict));

        let regress_error = root
            .apply_commit(
                &key,
                &base_entry,
                metadata_location("memory://catalog/tables/table-id/metadata/00001-prev.metadata.json"),
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
        let base_entry = root.get_active(&key).expect("active base entry").clone();

        let error = root
            .apply_commit(
                &key,
                &base_entry,
                metadata_location("memory://catalog/tables/table-id/metadata/invalid.json"),
            )
            .expect_err("invalid metadata location must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
        let entry = root.get_active(&key).expect("active entry remains unchanged");
        assert_eq!(
            MetadataVersion::from_location(entry.metadata_location().as_str())
                .expect("metadata version")
                .as_u32(),
            0
        );
    }

    /// Build a `TableUpdate` (prepared commit) without going through the
    /// storage round-trip. `merge_transaction` keys off `persisted` and the
    /// version baked into `updated_location`, so the metadata contents are
    /// irrelevant here.
    fn prepared_commit(full_name: TableIdent, persisted: CatalogTableLink, updated_location: &str) -> TableUpdate {
        TableUpdate {
            full_name,
            persisted: Some(persisted),
            updated: IcebergTableMetadata::new(
                metadata_location(updated_location),
                test_table_metadata("memory://catalog/tables/tbl"),
            ),
        }
    }

    /// Metadata location matching the layout produced by [`table_entry`], so
    /// string comparisons against a seeded entry hold exactly.
    fn versioned_location(version: u32) -> String {
        format!("memory://catalog/tables/table-id/metadata/{version:05}-uuid.metadata.json")
    }

    fn active_version(root: &CatalogRoot, key: &TableKey) -> u32 {
        MetadataVersion::from_location(root.get_active(key).expect("active entry").metadata_location().as_str())
            .expect("metadata version")
            .as_u32()
    }

    #[test]
    fn merge_transaction_ready_applies_successor() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        let base = root.get_active(&key).expect("base entry").clone();
        let update = prepared_commit(table_ident(ns, "tbl"), base, &versioned_location(1));

        let result = root
            .merge_transaction(&[PreparedCommitRef {
                key: &key,
                table: Some(&update),
            }])
            .expect("ready merge");

        assert!(matches!(result, MergeTransactionResult::Ready));
        assert_eq!(active_version(&root, &key), 1);
    }

    #[test]
    fn merge_transaction_unprepared_entry_needs_rebuild() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");

        let result = root
            .merge_transaction(&[PreparedCommitRef { key: &key, table: None }])
            .expect("merge with empty slot");

        match result {
            MergeTransactionResult::NeedRebuild(outcome) => assert_eq!(outcome.rebuild, vec![key]),
            MergeTransactionResult::Ready => panic!("expected NeedRebuild, got Ready"),
        }
    }

    #[test]
    fn merge_transaction_table_id_mismatch_conflicts() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        // ABA recreate: a different table now occupies this key (new table id),
        // so the prepared pointer must be rejected as a hard conflict.
        let persisted = CatalogTableLink::new(table_id(9_999), metadata_location(versioned_location(0)));
        let update = prepared_commit(table_ident(ns, "tbl"), persisted, &versioned_location(1));

        let error = root
            .merge_transaction(&[PreparedCommitRef {
                key: &key,
                table: Some(&update),
            }])
            .expect_err("table id mismatch must conflict");

        assert!(matches!(error, Error::CommitConflict));
    }

    #[test]
    fn merge_transaction_lost_ack_converges_without_advance() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        // Head already sits at the exact file we prepared (version 1): our prior
        // CAS landed but the ack was lost. table_entry(1) carries table id 2.
        root.link_table(key.clone(), table_entry(1)).expect("seed table");
        let persisted = CatalogTableLink::new(table_id(2), metadata_location(versioned_location(0)));
        let update = prepared_commit(table_ident(ns, "tbl"), persisted, &versioned_location(1));

        let result = root
            .merge_transaction(&[PreparedCommitRef {
                key: &key,
                table: Some(&update),
            }])
            .expect("lost-ack merge");

        assert!(matches!(result, MergeTransactionResult::Ready));
        // Convergence, not advance: the head stays at version 1 (no redundant N+2).
        assert_eq!(active_version(&root, &key), 1);
    }

    #[test]
    fn merge_transaction_head_moved_needs_rebuild() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        // Head moved to version 2 (table id 3) under us; the prepared commit was
        // built against version 1, so it no longer sits on the head.
        root.link_table(key.clone(), table_entry(2)).expect("seed table");
        let persisted = CatalogTableLink::new(table_id(3), metadata_location(versioned_location(1)));
        let update = prepared_commit(
            table_ident(ns, "tbl"),
            persisted,
            "memory://catalog/tables/table-id/metadata/00002-other.metadata.json",
        );

        let result = root
            .merge_transaction(&[PreparedCommitRef {
                key: &key,
                table: Some(&update),
            }])
            .expect("head-moved merge");

        match result {
            MergeTransactionResult::NeedRebuild(outcome) => assert_eq!(outcome.rebuild, vec![key]),
            MergeTransactionResult::Ready => panic!("expected NeedRebuild, got Ready"),
        }
    }

    #[test]
    fn merge_transaction_non_sequential_publish_needs_rebuild() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key.clone(), table_entry(0)).expect("seed table");
        let base = root.get_active(&key).expect("base entry").clone();
        // Publish version 2 over head version 0: not a direct predecessor, so the
        // commit must rebuild even though persisted still matches the head.
        let update = prepared_commit(table_ident(ns, "tbl"), base, &versioned_location(2));

        let result = root
            .merge_transaction(&[PreparedCommitRef {
                key: &key,
                table: Some(&update),
            }])
            .expect("non-sequential merge");

        match result {
            MergeTransactionResult::NeedRebuild(outcome) => assert_eq!(outcome.rebuild, vec![key]),
            MergeTransactionResult::Ready => panic!("expected NeedRebuild, got Ready"),
        }
    }

    #[test]
    fn merge_transaction_missing_entry_table_not_found() {
        let ns = namespace(&["ns"]);
        let key = table_key(ns.clone(), "tbl");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        // Namespace exists but the table entry does not: a prepared commit
        // against a vanished table is a hard not-found, not a rebuild signal.
        let persisted = CatalogTableLink::new(table_id(1), metadata_location(versioned_location(0)));
        let update = prepared_commit(table_ident(ns, "tbl"), persisted, &versioned_location(1));

        let error = root
            .merge_transaction(&[PreparedCommitRef {
                key: &key,
                table: Some(&update),
            }])
            .expect_err("absent entry must fail");

        assert!(matches!(error, Error::TableNotFound(_)));
    }

    #[test]
    fn merge_transaction_partial_batch_rebuilds_only_unprepared() {
        let ns = namespace(&["ns"]);
        let key_ready = table_key(ns.clone(), "tbl_ready");
        let key_rebuild = table_key(ns.clone(), "tbl_rebuild");
        let mut root = CatalogRoot::default();
        root.create_namespace(&ns, HashMap::new());
        root.link_table(key_ready.clone(), table_entry(0)).expect("seed ready");
        // Distinct table id so the duplicate-id guard in link_table accepts it.
        root.link_table(
            key_rebuild.clone(),
            CatalogTableLink::new(
                table_id(42),
                metadata_location("memory://catalog/tables/table-rebuild/metadata/00000-uuid.metadata.json"),
            ),
        )
        .expect("seed rebuild");
        let base_ready = root.get_active(&key_ready).expect("ready base").clone();
        let update_ready = prepared_commit(table_ident(ns, "tbl_ready"), base_ready, &versioned_location(1));

        let result = root
            .merge_transaction(&[
                PreparedCommitRef {
                    key: &key_ready,
                    table: Some(&update_ready),
                },
                PreparedCommitRef {
                    key: &key_rebuild,
                    table: None,
                },
            ])
            .expect("mixed merge");

        match result {
            MergeTransactionResult::NeedRebuild(outcome) => assert_eq!(outcome.rebuild, vec![key_rebuild]),
            MergeTransactionResult::Ready => panic!("expected NeedRebuild, got Ready"),
        }
        // The ready entry is applied even though the batch needs a rebuild: the
        // caller discards this root, so partial advance is expected and harmless.
        assert_eq!(active_version(&root, &key_ready), 1);
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
            CatalogTableLink::new(
                table_id(42),
                metadata_location("memory://catalog/tables/table-id/metadata/00001-1.metadata.json"),
            ),
        )
        .expect("create table");
        root.tombstone(&src_key, &table_id(42)).expect("tombstone table");

        let result = root.rename(&src_key, dst_key, &table_id(42));

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
        let table_id = *root.get_active(&src).expect("active source").table_id();

        let error = root
            .rename(&src, dst, &table_id)
            .expect_err("missing destination namespace must fail");

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
