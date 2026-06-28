#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::{FormatVersion, SortOrder, TableMetadataBuilder, UnboundPartitionSpec};
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use super::common::{
    CachedTestEnv, create_table, make_cached_catalog, make_catalog, make_in_memory_catalog,
    make_in_memory_catalog_cached, make_in_memory_catalog_with_storage, metadata_version_in, test_schema,
    update_request,
};
use crate::catalog::S3Catalog;
use crate::config::{S3CatalogConfig, cas_retrier_config_default};
use crate::error::Error;
use crate::infra::retrier::{Retrier, RetrierConfig};
use crate::root::{CatalogRoot, CatalogTableLink, IcebergTableMetadata, TableId, TableKey, TableMetadataLocation};
use crate::storage::cached::CachedCatalogStorage;
use crate::storage::{CatalogStorage, LoadOutcome, Version};

fn unwrap_loaded(outcome: LoadOutcome) -> (Arc<CatalogRoot>, Version) {
    match outcome {
        LoadOutcome::Loaded { root, version } => (root, version),
        LoadOutcome::Absent => (Arc::new(CatalogRoot::default()), Version::Absent),
        LoadOutcome::NotModified => panic!("unexpected NotModified for unconditional load"),
    }
}

fn test_catalog(storage: Arc<dyn CatalogStorage>, file_io: FileIO, tables_uri_prefix: String) -> S3Catalog {
    S3Catalog::with_storage(
        storage,
        file_io,
        tables_uri_prefix,
        Retrier::new(cas_retrier_config_default()),
        tokio_util::sync::CancellationToken::new(),
    )
}

fn initial_metadata_location(table_location: &str) -> String {
    format!(
        "{}/metadata/00000-{}.metadata.json",
        table_location.trim_end_matches('/'),
        Uuid::new_v4()
    )
}

fn root_tables_json(root: &CatalogRoot) -> Value {
    serde_json::to_value(root)
        .expect("serialize root")
        .get("tables")
        .cloned()
        .expect("tables field")
}

fn root_tombstones_json(root: &CatalogRoot) -> Value {
    serde_json::to_value(root)
        .expect("serialize root")
        .get("tombstones")
        .cloned()
        .expect("tombstones field")
}

fn root_namespaces_json(root: &CatalogRoot) -> Value {
    serde_json::to_value(root)
        .expect("serialize root")
        .get("namespaces")
        .cloned()
        .expect("namespaces field")
}

/// Hook invoked inside `save_root` before the version check. Allows tests to
/// simulate a concurrent writer that committed *between* the catalog's
/// `load_root` and `save_root` — the mutation is applied to the stored root
/// and its version is bumped, so the caller's `expected` token becomes stale
/// and the save returns `CommitConflict`. The hook fires at most once and is
/// then cleared.
type BeforeSaveRootHook = Box<dyn Fn(&mut CatalogRoot) + Send + Sync>;

/// One-shot async hook fired immediately after a lost-ack `save_root` lands the
/// caller's root but before it reports the synthetic conflict. Lets a test model
/// a concurrent writer that advances the table on top of the just-landed head,
/// so the caller's prepared file becomes an ancestor of the new head before its
/// retry reloads.
type AfterLostAckHook = Box<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

struct ConflictOnSaveStorage {
    root: RwLock<Option<(Arc<CatalogRoot>, u64)>>,
    metadata: dashmap::DashMap<String, Bytes>,
    version: AtomicU64,
    /// Total `load_root` calls. Lets tests assert the optimistic retry loop
    /// reloads the root exactly once per attempt (K+1 for K conflicts), rather
    /// than paying an extra conflict-time reload (2*K+1).
    load_root_calls: AtomicUsize,
    /// Counter of remaining synthetic CAS conflicts. Each call to `save_root`
    /// while this is non-zero decrements it and returns `CommitConflict`
    /// without touching the stored root — models a transient root-only race.
    fail_next_save_roots: AtomicUsize,
    /// Counter of remaining lost-ack saves. Each call to `save_root` while this
    /// is non-zero performs the real version-checked write (storing the
    /// caller's root and bumping the version) but then returns `CommitConflict`
    /// — models a CAS that physically landed on storage while the success
    /// response was lost in transit.
    lost_ack_save_roots: AtomicUsize,
    /// Conflict-injecting hook. Fires once on the next `save_root` before the
    /// version compare, then is taken out of the slot.
    before_save_root_hook: Mutex<Option<BeforeSaveRootHook>>,
    /// Concurrent-advance hook. Fires once right after a lost-ack `save_root`
    /// lands the caller's root, then is taken out of the slot.
    after_lost_ack_hook: Mutex<Option<AfterLostAckHook>>,
}

impl ConflictOnSaveStorage {
    fn new() -> Self {
        Self {
            root: RwLock::new(None),
            metadata: dashmap::DashMap::new(),
            version: AtomicU64::new(0),
            load_root_calls: AtomicUsize::new(0),
            fail_next_save_roots: AtomicUsize::new(0),
            lost_ack_save_roots: AtomicUsize::new(0),
            before_save_root_hook: Mutex::new(None),
            after_lost_ack_hook: Mutex::new(None),
        }
    }

    fn fail_next_save_root(&self) {
        self.fail_next_save_roots.store(1, Ordering::SeqCst);
    }

    fn lose_ack_on_next_save_root(&self) {
        self.lost_ack_save_roots.store(1, Ordering::SeqCst);
    }

    fn fail_next_n_save_roots(&self, n: usize) {
        self.fail_next_save_roots.store(n, Ordering::SeqCst);
    }

    fn load_root_call_count(&self) -> usize {
        self.load_root_calls.load(Ordering::SeqCst)
    }

    async fn set_before_save_root_hook<F>(&self, hook: F)
    where
        F: Fn(&mut CatalogRoot) + Send + Sync + 'static,
    {
        *self.before_save_root_hook.lock().await = Some(Box::new(hook));
    }

    async fn set_after_lost_ack_hook<F>(&self, hook: F)
    where
        F: Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync + 'static,
    {
        *self.after_lost_ack_hook.lock().await = Some(Box::new(hook));
    }

    fn metadata_locations(&self) -> Vec<String> {
        let mut locations = self.metadata.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>();
        locations.sort();
        locations
    }

    async fn root_state(&self) -> Arc<CatalogRoot> {
        unwrap_loaded(self.load_root(None).await.expect("load root")).0
    }
}

#[async_trait]
impl CatalogStorage for ConflictOnSaveStorage {
    async fn load_root(&self, known: Option<&Version>) -> crate::Result<LoadOutcome> {
        self.load_root_calls.fetch_add(1, Ordering::SeqCst);
        let guard = self.root.read().await;
        match guard.as_ref() {
            Some((root, version)) => {
                let etag = version.to_string();
                if let Some(Version::Etag(provided)) = known {
                    if provided == &etag {
                        return Ok(LoadOutcome::NotModified);
                    }
                }
                Ok(LoadOutcome::Loaded {
                    root: Arc::clone(root),
                    version: Version::Etag(etag),
                })
            }
            None => Ok(LoadOutcome::Absent),
        }
    }

    async fn save_root(&self, root: Arc<CatalogRoot>, expected: &Version) -> crate::Result<Version> {
        // Pure transient-conflict counter: fire CommitConflict without
        // mutating stored state. Models root-only CAS races (other writer
        // touched an unrelated table).
        let pending = self.fail_next_save_roots.load(Ordering::SeqCst);
        if pending > 0 {
            self.fail_next_save_roots.store(pending - 1, Ordering::SeqCst);
            return Err(Error::CommitConflict);
        }

        // Lost-ack conflict: perform the real version-checked write but report
        // a conflict, so the caller believes the save failed while storage has
        // actually advanced to the caller's own root. Only consumes the counter
        // when the CAS token matches; a stale token falls through to the normal
        // path below (which reports the conflict on its own).
        let lost_ack = self.lost_ack_save_roots.load(Ordering::SeqCst);
        if lost_ack > 0 {
            let mut guard = self.root.write().await;
            let matches = guard.as_ref().map_or_else(
                || *expected == Version::Absent,
                |(_, current_version)| *expected == Version::Etag(current_version.to_string()),
            );
            if matches {
                self.lost_ack_save_roots.store(lost_ack - 1, Ordering::SeqCst);
                let next_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
                *guard = Some((root, next_version));
                drop(guard);
                // Model a concurrent writer that advances the table on top of our
                // just-landed head before the retry reloads.
                let hook = self.after_lost_ack_hook.lock().await.take();
                if let Some(hook) = hook {
                    hook().await;
                }
                return Err(Error::CommitConflict);
            }
        }

        // Hook-driven conflict: mutate stored root + bump version *before*
        // the compare. Models a concurrent writer who landed a commit
        // between this caller's load_root and save_root.
        let hook = self.before_save_root_hook.lock().await.take();
        if let Some(hook) = hook {
            let mut guard = self.root.write().await;
            let mut next_root = guard.as_ref().map_or_else(CatalogRoot::default, |(r, _)| (**r).clone());
            hook(&mut next_root);
            let next_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
            *guard = Some((Arc::new(next_root), next_version));
            drop(guard);
            return Err(Error::CommitConflict);
        }

        let mut guard = self.root.write().await;
        if let Some((_, current_version)) = guard.as_ref() {
            if *expected != Version::Etag(current_version.to_string()) {
                return Err(Error::CommitConflict);
            }
        } else if *expected != Version::Absent {
            return Err(Error::CommitConflict);
        }

        let next_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        *guard = Some((root, next_version));
        drop(guard);
        Ok(Version::Etag(next_version.to_string()))
    }

    async fn read_table_metadata(&self, location: &str) -> crate::Result<Arc<iceberg::spec::TableMetadata>> {
        let payload = self
            .metadata
            .get(location)
            .map(|entry| Bytes::clone(entry.value()))
            .ok_or_else(|| Error::InvalidMetadata(format!("Metadata location not found: {location}")))?;
        let metadata: iceberg::spec::TableMetadata = serde_json::from_slice(&payload)
            .map_err(|error| Error::InvalidMetadata(format!("Invalid table metadata: {error}")))?;
        Ok(Arc::new(metadata))
    }

    async fn write_table_metadata(&self, location: &str, metadata: &iceberg::spec::TableMetadata) -> crate::Result<()> {
        let payload = serde_json::to_vec(metadata)
            .map(Bytes::from)
            .map_err(|error| Error::InvalidMetadata(format!("Failed to serialize metadata: {error}")))?;
        self.metadata.insert(location.to_string(), payload);
        Ok(())
    }
}

#[tokio::test]
async fn create_and_load_table_with_in_memory_storage() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("ns1".to_string());

    let created = create_table(&catalog, &namespace, "tbl").await;
    let loaded = catalog.load_table(created.identifier()).await.expect("load table");

    assert_eq!(loaded.identifier(), created.identifier());
    assert_eq!(loaded.metadata_location(), created.metadata_location());
}

#[tokio::test]
async fn create_table_uses_default_or_explicit_location_for_metadata_files() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let default_created = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("default-location".to_string())
                .schema(test_schema())
                .build(),
        )
        .await
        .expect("create with default location");
    let default_uuid = default_created.metadata().uuid();
    let default_table_location = format!("memory://catalog/tables/{default_uuid}");
    let default_metadata_location = default_created
        .metadata_location()
        .expect("default metadata location")
        .to_string();
    assert_eq!(default_created.metadata().location(), default_table_location.as_str());
    assert!(default_metadata_location.starts_with(&format!("{default_table_location}/metadata/00000-")));

    let explicit_table_location = "memory://custom/path/explicit".to_string();
    let explicit_created = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("explicit-location".to_string())
                .location(explicit_table_location.clone())
                .schema(test_schema())
                .build(),
        )
        .await
        .expect("create with explicit location");
    let explicit_metadata_location = explicit_created
        .metadata_location()
        .expect("explicit metadata location")
        .to_string();
    assert_eq!(explicit_created.metadata().location(), explicit_table_location.as_str());
    assert!(explicit_metadata_location.starts_with(&format!("{explicit_table_location}/metadata/00000-")));

    let root = unwrap_loaded(storage.load_root(None).await.expect("load root")).0;
    let default_entry = root
        .get_active(&TableKey::from_ident(default_created.identifier()))
        .expect("default entry");
    let explicit_entry = root
        .get_active(&TableKey::from_ident(explicit_created.identifier()))
        .expect("explicit entry");
    assert_eq!(default_entry.table_id().as_uuid(), &default_uuid);
    assert_eq!(explicit_entry.table_id().as_uuid(), &explicit_created.metadata().uuid());
}

#[tokio::test]
async fn register_table_and_load() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let table_ident = TableIdent::new(namespace, "registered".to_string());
    let table_id = "external-table";
    let table_metadata = TableMetadataBuilder::new(
        test_schema(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        format!("memory://catalog/tables/{table_id}"),
        FormatVersion::V2,
        std::collections::HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("table metadata")
    .metadata;
    let metadata_location = initial_metadata_location(table_metadata.location());
    storage
        .write_table_metadata(&metadata_location, &table_metadata)
        .await
        .expect("write metadata");

    let registered = catalog
        .register_table(&table_ident, metadata_location.clone())
        .await
        .expect("register table");
    let loaded = catalog.load_table(&table_ident).await.expect("load table");

    assert_eq!(registered.identifier(), &table_ident);
    assert_eq!(registered.metadata_location(), Some(metadata_location.as_str()));
    assert_eq!(loaded.metadata_location(), Some(metadata_location.as_str()));
}

#[tokio::test]
async fn register_table_uses_metadata_uuid_and_update_stays_in_same_prefix() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let table_ident = TableIdent::new(namespace, "registered".to_string());
    let table_location = "memory://external/warehouse/tables/registered".to_string();
    let table_metadata = TableMetadataBuilder::new(
        test_schema(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        table_location.clone(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("table metadata")
    .metadata;
    let metadata_location = initial_metadata_location(table_metadata.location());
    storage
        .write_table_metadata(&metadata_location, &table_metadata)
        .await
        .expect("write metadata");

    let registered = catalog
        .register_table(&table_ident, metadata_location.clone())
        .await
        .expect("register table");
    let updated = catalog
        .update_table(update_request(&registered, "k", "v1").await)
        .await
        .expect("update registered table");
    let updated_location = updated.metadata_location().expect("updated metadata location").to_string();
    let metadata_prefix = format!("{table_location}/metadata/");

    assert!(metadata_location.starts_with(&metadata_prefix));
    assert!(updated_location.starts_with(&metadata_prefix));
    assert_eq!(metadata_version_in(&updated_location), 1);

    let root = unwrap_loaded(storage.load_root(None).await.expect("load root")).0;
    let entry = root.get_active(&TableKey::from_ident(&table_ident)).expect("active root entry");
    assert_eq!(entry.table_id().as_uuid(), &table_metadata.uuid());
    assert_eq!(entry.metadata_location().as_str(), updated_location.as_str());
}

#[tokio::test]
async fn register_table_invalid_location() {
    let catalog = make_in_memory_catalog();
    let table_ident = TableIdent::new(NamespaceIdent::new("ns1".to_string()), "registered".to_string());
    let error = catalog
        .register_table(
            &table_ident,
            "memory://catalog/tables/external/metadata/invalid.json".to_string(),
        )
        .await
        .expect_err("registering invalid metadata location must fail");

    assert_eq!(error.kind(), ErrorKind::DataInvalid);
}

#[tokio::test]
async fn register_table_fails_when_namespace_not_found() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("nonexistent".to_string());
    let table_ident = TableIdent::new(namespace, "registered".to_string());
    let table_metadata = TableMetadataBuilder::new(
        test_schema(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        "memory://catalog/tables/external-table".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("table metadata")
    .metadata;
    let metadata_location = initial_metadata_location(table_metadata.location());
    storage
        .write_table_metadata(&metadata_location, &table_metadata)
        .await
        .expect("write metadata");

    let error = catalog
        .register_table(&table_ident, metadata_location)
        .await
        .expect_err("register in missing namespace must fail");

    assert_eq!(error.kind(), ErrorKind::NamespaceNotFound);
}

#[tokio::test]
async fn register_table_fails_when_metadata_object_missing() {
    let catalog = make_in_memory_catalog();
    let table_ident = TableIdent::new(NamespaceIdent::new("ns1".to_string()), "registered".to_string());
    catalog
        .register_table(
            &table_ident,
            "memory://catalog/tables/external/metadata/00000-missing.json".to_string(),
        )
        .await
        .expect_err("missing metadata object must fail");

    let loaded = catalog
        .load_table(&table_ident)
        .await
        .expect_err("table must not be registered");
    assert_eq!(loaded.kind(), ErrorKind::TableNotFound);
}

#[tokio::test]
async fn register_table_already_exists() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table_ident = TableIdent::new(namespace.clone(), "registered".to_string());
    create_table(&catalog, &namespace, "registered").await;

    let table_metadata = TableMetadataBuilder::new(
        test_schema(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        "memory://catalog/tables/external-table".to_string(),
        FormatVersion::V2,
        std::collections::HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("table metadata")
    .metadata;
    let metadata_location = initial_metadata_location(table_metadata.location());
    storage
        .write_table_metadata(&metadata_location, &table_metadata)
        .await
        .expect("write metadata");

    let error = catalog
        .register_table(&table_ident, metadata_location)
        .await
        .expect_err("registering over active table must fail");

    assert_eq!(error.kind(), ErrorKind::TableAlreadyExists);
}

#[tokio::test]
async fn register_table_rejects_duplicate_table_uuid_across_different_names() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let first_ident = TableIdent::new(namespace.clone(), "registered-1".to_string());
    let second_ident = TableIdent::new(namespace, "registered-2".to_string());
    let table_metadata = TableMetadataBuilder::new(
        test_schema(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        "memory://external/tables/shared".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("table metadata")
    .metadata;
    let metadata_location = initial_metadata_location(table_metadata.location());
    storage
        .write_table_metadata(&metadata_location, &table_metadata)
        .await
        .expect("write metadata");

    catalog
        .register_table(&first_ident, metadata_location.clone())
        .await
        .expect("first register");

    let error = catalog
        .register_table(&second_ident, metadata_location)
        .await
        .expect_err("duplicate table uuid must fail");
    assert_eq!(error.kind(), ErrorKind::DataInvalid);
}

#[tokio::test]
async fn s3_storage_writes_metadata_using_catalog_layout() {
    let env = make_catalog().await;
    let table_id = "external-table";
    let table_metadata = TableMetadataBuilder::new(
        test_schema(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        format!("{}/{table_id}", env.tables_uri_prefix),
        FormatVersion::V2,
        std::collections::HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("table metadata")
    .metadata;

    let metadata_location = initial_metadata_location(table_metadata.location());
    env.s3_storage
        .write_table_metadata(&metadata_location, &table_metadata)
        .await
        .expect("write metadata");
    let loaded = env
        .s3_storage
        .read_table_metadata(&metadata_location)
        .await
        .expect("read metadata");

    assert!(metadata_location.starts_with("s3://"));
    assert!(metadata_location.contains("/warehouse/catalog/tables/external-table/metadata/00000-"));
    assert!(
        std::path::Path::new(&metadata_location)
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
    );
    assert_eq!(loaded.location(), table_metadata.location());
    assert_eq!(loaded.last_column_id(), table_metadata.last_column_id());
}

#[tokio::test]
async fn list_namespaces_with_parent() {
    let catalog = make_in_memory_catalog();
    let namespace_left = NamespaceIdent::from_vec(vec!["a".to_string(), "b".to_string()]).expect("namespace a.b");
    let namespace_right = NamespaceIdent::from_vec(vec!["a".to_string(), "c".to_string()]).expect("namespace a.c");
    let parent = NamespaceIdent::new("a".to_string());

    create_table(&catalog, &namespace_left, "t1").await;
    create_table(&catalog, &namespace_right, "t1").await;

    let namespaces = catalog.list_namespaces(Some(&parent)).await.expect("list namespaces");

    assert_eq!(namespaces, vec![namespace_left, namespace_right]);
}

#[tokio::test]
async fn list_namespaces_without_parent() {
    let catalog = make_in_memory_catalog();
    let namespace_a = NamespaceIdent::new("a".to_string());
    let namespace_z = NamespaceIdent::new("z".to_string());
    let child_left = NamespaceIdent::from_vec(vec!["a".to_string(), "b".to_string()]).expect("namespace a.b");
    let child_right = NamespaceIdent::from_vec(vec!["a".to_string(), "c".to_string()]).expect("namespace a.c");

    catalog.create_namespace(&namespace_a, HashMap::new()).await.expect("create a");
    catalog.create_namespace(&namespace_z, HashMap::new()).await.expect("create z");
    create_table(&catalog, &child_left, "t1").await;
    create_table(&catalog, &child_right, "t1").await;

    let namespaces = catalog.list_namespaces(None).await.expect("list namespaces");

    assert_eq!(namespaces, vec![namespace_a, namespace_z]);
}

#[tokio::test]
async fn list_tables_in_namespace() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let other_namespace = NamespaceIdent::new("ns2".to_string());

    let table_a = create_table(&catalog, &namespace, "a").await;
    let table_b = create_table(&catalog, &namespace, "b").await;
    create_table(&catalog, &other_namespace, "c").await;

    let tables = catalog.list_tables(&namespace).await.expect("list tables");

    assert_eq!(tables, vec![table_a.identifier().clone(), table_b.identifier().clone()]);
}

#[tokio::test]
async fn rename_table_preserves_metadata_location() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let created = create_table(&catalog, &namespace, "tbl").await;
    let renamed = TableIdent::new(namespace, "tbl-renamed".to_string());
    let original_metadata_location = created.metadata_location().expect("created metadata location").to_string();
    let original_table_location = created.metadata().location().to_string();

    catalog
        .rename_table(created.identifier(), &renamed)
        .await
        .expect("rename table");

    assert!(!catalog.table_exists(created.identifier()).await.expect("source exists"));
    assert!(catalog.table_exists(&renamed).await.expect("renamed exists"));
    let loaded = catalog.load_table(&renamed).await.expect("load renamed table");
    assert_eq!(loaded.metadata_location(), Some(original_metadata_location.as_str()));
    assert_eq!(loaded.metadata().location(), original_table_location.as_str());
}

#[tokio::test]
async fn load_table_not_found() {
    let catalog = make_in_memory_catalog();
    let table_ident = TableIdent::new(NamespaceIdent::new("ns1".to_string()), "missing".to_string());

    let error = catalog
        .load_table(&table_ident)
        .await
        .expect_err("loading missing table must fail");

    assert_eq!(error.kind(), ErrorKind::TableNotFound);
}

#[tokio::test]
async fn drop_table_not_found() {
    let catalog = make_in_memory_catalog();
    let table_ident = TableIdent::new(NamespaceIdent::new("ns1".to_string()), "missing".to_string());

    let error = catalog
        .drop_table(&table_ident)
        .await
        .expect_err("dropping missing table must fail");

    assert_eq!(error.kind(), ErrorKind::TableNotFound);
}

#[tokio::test]
async fn rename_table_source_not_found() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let src = TableIdent::new(namespace.clone(), "missing".to_string());
    let dest = TableIdent::new(namespace, "renamed".to_string());

    let error = catalog
        .rename_table(&src, &dest)
        .await
        .expect_err("renaming missing source must fail");

    assert_eq!(error.kind(), ErrorKind::TableNotFound);
}

#[tokio::test]
async fn rename_table_fails_when_destination_namespace_not_found() {
    let catalog = make_in_memory_catalog();
    let src_namespace = NamespaceIdent::new("existing".to_string());
    let dst_namespace = NamespaceIdent::new("nonexistent".to_string());
    let src = create_table(&catalog, &src_namespace, "tbl").await;
    let dest = TableIdent::new(dst_namespace, "tbl".to_string());

    let error = catalog
        .rename_table(src.identifier(), &dest)
        .await
        .expect_err("rename to missing namespace must fail");

    assert_eq!(error.kind(), ErrorKind::NamespaceNotFound);
}

#[tokio::test]
async fn rename_table_dest_already_exists() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let src = create_table(&catalog, &namespace, "src").await;
    let dest = create_table(&catalog, &namespace, "dst").await;

    let error = catalog
        .rename_table(src.identifier(), dest.identifier())
        .await
        .expect_err("renaming into active destination must fail");

    assert_eq!(error.kind(), ErrorKind::TableAlreadyExists);
}

#[tokio::test]
async fn table_exists_active_and_tombstoned() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table_ident = TableIdent::new(namespace.clone(), "tbl".to_string());

    assert!(!catalog.table_exists(&table_ident).await.expect("table should not exist"));

    let created = create_table(&catalog, &namespace, "tbl").await;
    assert!(catalog.table_exists(created.identifier()).await.expect("table should exist"));

    catalog.drop_table(created.identifier()).await.expect("drop table");
    assert!(
        !catalog
            .table_exists(created.identifier())
            .await
            .expect("tombstoned table should not exist")
    );
}

#[tokio::test]
async fn commit_transaction_empty() {
    let catalog = make_in_memory_catalog();

    let committed = catalog
        .commit_transaction(Vec::new())
        .await
        .expect("empty transaction must succeed");

    assert!(committed.is_empty());
}

#[tokio::test]
async fn commit_transaction_table_not_found() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let expected_ident = table.identifier().clone();
    let request = update_request(&table, "k", "v1").await;
    catalog.drop_table(table.identifier()).await.expect("drop table");

    let error = catalog
        .commit_transaction(vec![request])
        .await
        .expect_err("commit for missing table must fail");

    assert!(matches!(error, Error::TableNotFound(ident) if ident == expected_ident));
}

#[tokio::test]
async fn commit_transaction_is_atomic_when_one_commit_is_invalid() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table_a = create_table(&catalog, &namespace, "a").await;
    let table_b = create_table(&catalog, &namespace, "b").await;
    let table_a_location = table_a.metadata_location().expect("table a metadata location").to_string();

    let request_a = update_request(&table_a, "k", "va").await;
    let request_b = update_request(&table_b, "k", "vb").await;
    catalog.drop_table(table_b.identifier()).await.expect("drop table b");

    let error = catalog
        .commit_transaction(vec![request_a, request_b])
        .await
        .expect_err("transaction with invalid commit must fail");

    assert!(matches!(error, Error::TableNotFound(ident) if ident == *table_b.identifier()));

    let loaded_a = catalog.load_table(table_a.identifier()).await.expect("load table a");
    assert_eq!(loaded_a.metadata_location(), Some(table_a_location.as_str()));

    let dropped_b = catalog
        .load_table(table_b.identifier())
        .await
        .expect_err("table b stays dropped");
    assert_eq!(dropped_b.kind(), ErrorKind::TableNotFound);

    let root = unwrap_loaded(storage.load_root(None).await.expect("load root")).0;
    assert_eq!(
        root.get_active(&TableKey::from_ident(table_a.identifier()))
            .expect("active table a")
            .metadata_location()
            .as_str(),
        table_a_location.as_str()
    );
    assert!(root.get_active(&TableKey::from_ident(table_b.identifier())).is_none());
}

#[tokio::test]
async fn commit_transaction_rejects_duplicate_table_in_same_request() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let original_metadata_location = table.metadata_location().expect("metadata location").to_string();

    let error = catalog
        .commit_transaction(vec![
            update_request(&table, "k", "v1").await,
            update_request(&table, "k", "v2").await,
        ])
        .await
        .expect_err("duplicate table commit must fail");

    assert!(matches!(error, Error::CommitConflict));

    let loaded = catalog.load_table(table.identifier()).await.expect("load unchanged table");
    assert_eq!(loaded.metadata_location(), Some(original_metadata_location.as_str()));

    let root = unwrap_loaded(storage.load_root(None).await.expect("load root")).0;
    assert_eq!(
        root.get_active(&TableKey::from_ident(table.identifier()))
            .expect("active table")
            .metadata_location()
            .as_str(),
        original_metadata_location.as_str()
    );
}

#[tokio::test]
async fn namespace_exists_checks_exact_namespace_records() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::from_vec(vec!["a".to_string(), "b".to_string()]).expect("namespace a.b");
    let parent = NamespaceIdent::new("a".to_string());

    assert!(!catalog.namespace_exists(&namespace).await.expect("namespace should not exist"));
    assert!(
        !catalog
            .namespace_exists(&parent)
            .await
            .expect("parent namespace should not exist")
    );

    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create exact namespace");
    assert!(catalog.namespace_exists(&namespace).await.expect("namespace should exist"));
    assert!(
        !catalog
            .namespace_exists(&parent)
            .await
            .expect("ancestor namespace should still be absent if not explicitly created")
    );
}

#[tokio::test]
async fn create_table_fails_when_namespace_not_found() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("nonexistent".to_string());

    let error = catalog
        .create_table(
            &namespace,
            TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
        )
        .await
        .expect_err("create table in missing namespace must fail");

    assert_eq!(error.kind(), ErrorKind::NamespaceNotFound);
    assert!(
        storage.metadata_locations().is_empty(),
        "deterministic namespace validation must run before metadata write"
    );
}

#[tokio::test]
async fn create_table_already_exists_does_not_write_metadata() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    create_table(&catalog, &namespace, "tbl").await;
    let metadata_before = storage.metadata_locations();

    let error = catalog
        .create_table(
            &namespace,
            TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
        )
        .await
        .expect_err("creating an active table again must fail");

    assert_eq!(error.kind(), ErrorKind::TableAlreadyExists);
    assert_eq!(
        storage.metadata_locations(),
        metadata_before,
        "deterministic table-exists validation must run before metadata write"
    );
}

#[tokio::test]
async fn empty_namespace_survives_reload_and_can_be_dropped() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::from_vec(vec!["a".to_string(), "b".to_string()]).expect("namespace a.b");

    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create empty namespace");
    let root = unwrap_loaded(storage.load_root(None).await.expect("load root after create")).0;
    let namespaces = root_namespaces_json(&root).as_object().cloned().expect("namespaces object");
    assert!(namespaces.contains_key("a.b"));

    let reloaded_catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let loaded = reloaded_catalog
        .get_namespace(&namespace)
        .await
        .expect("get namespace after reload");
    assert!(loaded.properties().is_empty());

    reloaded_catalog.drop_namespace(&namespace).await.expect("drop empty namespace");
    let final_catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let error = final_catalog
        .get_namespace(&namespace)
        .await
        .expect_err("dropped namespace must not be found");
    assert_eq!(error.kind(), ErrorKind::NamespaceNotFound);
}

#[tokio::test]
async fn namespace_properties_are_persisted_and_replaced() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let create_properties = HashMap::from([("owner".to_string(), "team-a".to_string())]);
    let update_properties = HashMap::from([
        ("owner".to_string(), "team-b".to_string()),
        ("retention".to_string(), "30d".to_string()),
    ]);

    catalog
        .create_namespace(&namespace, create_properties)
        .await
        .expect("create namespace with properties");
    catalog
        .update_namespace(&namespace, update_properties.clone())
        .await
        .expect("update namespace properties");

    let reloaded_catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let loaded = reloaded_catalog
        .get_namespace(&namespace)
        .await
        .expect("get namespace after update");
    assert_eq!(loaded.properties(), &update_properties);
}

#[tokio::test]
async fn create_namespace_rejects_duplicate_with_same_properties() {
    // Strict Iceberg contract, no race: a second create of an existing namespace
    // must fail with NamespaceAlreadyExists even when the properties are
    // identical. Unlike create_table/register_table, the namespace op has no
    // stable token to distinguish a self-retry from a duplicate, so it does not
    // converge — see `S3Catalog::create_namespace`.
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let properties = HashMap::from([("owner".to_string(), "team-a".to_string())]);

    catalog
        .create_namespace(&namespace, properties.clone())
        .await
        .expect("first create succeeds");

    let error = catalog
        .create_namespace(&namespace, properties)
        .await
        .expect_err("duplicate create with identical properties must fail");

    assert_eq!(error.kind(), ErrorKind::NamespaceAlreadyExists);
}

#[tokio::test]
async fn drop_parent_namespace_with_descendant_tables_is_rejected() {
    let catalog = make_in_memory_catalog();
    let parent = NamespaceIdent::new("a".to_string());
    let child = NamespaceIdent::from_vec(vec!["a".to_string(), "b".to_string()]).expect("namespace a.b");

    catalog
        .create_namespace(&parent, HashMap::new())
        .await
        .expect("create parent namespace");
    let table = create_table(&catalog, &child, "tbl").await;

    let error = catalog
        .drop_namespace(&parent)
        .await
        .expect_err("parent with descendant tables must not be dropped");
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);

    catalog.drop_table(table.identifier()).await.expect("drop table");
    catalog.drop_namespace(&child).await.expect("drop child namespace");
    catalog
        .drop_namespace(&parent)
        .await
        .expect("drop parent after descendant tables and child namespace are removed");
}

#[tokio::test]
async fn create_after_drop_same_name_sequential() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let original = create_table(&catalog, &namespace, "tbl").await;

    catalog.drop_table(original.identifier()).await.expect("drop original");
    let recreated = create_table(&catalog, &namespace, "tbl").await;
    let loaded = catalog.load_table(recreated.identifier()).await.expect("load recreated table");

    assert_eq!(loaded.identifier(), recreated.identifier());
    assert_eq!(loaded.metadata_location(), recreated.metadata_location());
    assert_ne!(loaded.metadata_location(), original.metadata_location());

    let root = unwrap_loaded(storage.load_root(None).await.expect("load root")).0;
    assert_eq!(
        root.get_active(&TableKey::from_ident(recreated.identifier()))
            .expect("active recreated entry")
            .metadata_location()
            .as_str(),
        recreated.metadata_location().expect("recreated metadata location")
    );
    let tables = root_tables_json(&root).as_object().cloned().expect("tables object");
    let tombstones = root_tombstones_json(&root).as_object().cloned().expect("tombstones object");
    assert_eq!(tables.len(), 1);
    assert_eq!(tombstones.len(), 1);
    assert!(tombstones.values().any(|entry| {
        entry.get("status") == Some(&Value::String("tombstoned".to_string()))
            && entry.get("metadata_location")
                == Some(&Value::String(
                    original.metadata_location().expect("original metadata location").to_string(),
                ))
    }));
}

#[tokio::test]
async fn rename_over_tombstoned_name_preserves_old_tombstone() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let src = create_table(&catalog, &namespace, "src").await;
    let dst = create_table(&catalog, &namespace, "dst").await;
    let dst_key = TableKey::from_ident(dst.identifier());

    catalog.drop_table(dst.identifier()).await.expect("drop destination");
    let renamed_ident = TableIdent::new(namespace, "dst".to_string());
    catalog
        .rename_table(src.identifier(), &renamed_ident)
        .await
        .expect("rename source over tombstoned name");

    let root = unwrap_loaded(storage.load_root(None).await.expect("load root after rename")).0;
    let active = root.get_active(&dst_key).expect("active renamed entry");
    assert_eq!(
        active.metadata_location().as_str(),
        src.metadata_location().expect("source metadata location")
    );
    let tombstones = root_tombstones_json(&root).as_object().cloned().expect("tombstones object");
    assert!(tombstones.values().any(|entry| {
        entry.get("status") == Some(&Value::String("tombstoned".to_string()))
            && entry.get("metadata_location")
                == Some(&Value::String(
                    dst.metadata_location().expect("dst metadata location").to_string(),
                ))
    }));
}

#[tokio::test]
async fn property_only_commits_advance_metadata_version() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let created = create_table(&catalog, &namespace, "tbl").await;

    assert_eq!(
        metadata_version_in(created.metadata_location().expect("created metadata location")),
        0
    );
    assert_eq!(created.metadata().last_sequence_number(), 0);

    let first = catalog
        .commit_transaction(vec![update_request(&created, "k", "v1").await])
        .await
        .expect("first commit")
        .into_iter()
        .next()
        .expect("single table response");
    let second = catalog
        .commit_transaction(vec![update_request(&first, "k", "v2").await])
        .await
        .expect("second commit")
        .into_iter()
        .next()
        .expect("single table response");

    assert_ne!(first.metadata_location(), second.metadata_location());
    assert_eq!(
        first.metadata().last_sequence_number(),
        second.metadata().last_sequence_number()
    );
    assert_eq!(
        metadata_version_in(first.metadata_location().expect("first metadata location")),
        1
    );
    assert_eq!(
        metadata_version_in(second.metadata_location().expect("second metadata location")),
        2
    );

    let (root, _) = unwrap_loaded(storage.load_root(None).await.expect("load root"));
    let key = TableKey::from_ident(second.identifier());
    let entry = root.get_active(&key).expect("active root entry");
    assert_eq!(metadata_version_in(entry.metadata_location().as_str()), 2);
}

#[tokio::test]
async fn single_and_multi_table_commits_ignore_last_sequence_number() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table_a = create_table(&catalog, &namespace, "a").await;
    let table_b = create_table(&catalog, &namespace, "b").await;

    let mut committed = catalog
        .commit_transaction(vec![
            update_request(&table_a, "k", "v1").await,
            update_request(&table_b, "k", "v1").await,
        ])
        .await
        .expect("multi-table commit")
        .into_iter();
    let committed_a = committed.next().expect("table a response");
    let committed_b = committed.next().expect("table b response");

    assert_eq!(
        committed_a.metadata().last_sequence_number(),
        table_a.metadata().last_sequence_number()
    );
    assert_eq!(
        committed_b.metadata().last_sequence_number(),
        table_b.metadata().last_sequence_number()
    );
    assert_eq!(
        metadata_version_in(committed_a.metadata_location().expect("table a metadata location")),
        1
    );
    assert_eq!(
        metadata_version_in(committed_b.metadata_location().expect("table b metadata location")),
        1
    );

    let updated_a = catalog
        .commit_transaction(vec![update_request(&committed_a, "k", "v2").await])
        .await
        .expect("single-table commit")
        .into_iter()
        .next()
        .expect("table a response");

    assert_eq!(
        updated_a.metadata().last_sequence_number(),
        committed_a.metadata().last_sequence_number()
    );
    assert_eq!(
        metadata_version_in(updated_a.metadata_location().expect("updated table a metadata location")),
        2
    );

    let (root, _) = unwrap_loaded(storage.load_root(None).await.expect("load root"));
    let entry_a = root
        .get_active(&TableKey::from_ident(updated_a.identifier()))
        .expect("table a root entry");
    let entry_b = root
        .get_active(&TableKey::from_ident(committed_b.identifier()))
        .expect("table b root entry");
    assert_eq!(metadata_version_in(entry_a.metadata_location().as_str()), 2);
    assert_eq!(metadata_version_in(entry_b.metadata_location().as_str()), 1);
}

#[tokio::test]
async fn create_vs_create() {
    let env = make_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    env.catalog
        .create_namespace(&ns, HashMap::new())
        .await
        .expect("create namespace");

    let task1 = env.catalog.create_table(
        &ns,
        TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
    );
    let task2 = env.catalog.create_table(
        &ns,
        TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
    );

    let (r1, r2) = tokio::join!(task1, task2);
    assert!(r1.is_ok() ^ r2.is_ok());

    let winner = r1.as_ref().ok().or_else(|| r2.as_ref().ok()).expect("one create winner");
    let loaded = env.catalog.load_table(winner.identifier()).await.expect("load winning table");
    assert_eq!(loaded.metadata_location(), winner.metadata_location());

    let root = unwrap_loaded(env.s3_storage.load_root(None).await.expect("load root")).0;
    let active = root
        .get_active(&TableKey::from_ident(winner.identifier()))
        .expect("active root entry");
    assert_eq!(
        active.metadata_location().as_str(),
        winner.metadata_location().expect("winner metadata location")
    );
}

#[tokio::test]
async fn drop_namespace_requires_empty_namespace() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;

    let error = catalog
        .drop_namespace(&namespace)
        .await
        .expect_err("non-empty namespace must not be dropped");
    assert_eq!(error.kind(), ErrorKind::PreconditionFailed);

    catalog.drop_table(table.identifier()).await.expect("drop table");
    catalog
        .drop_namespace(&namespace)
        .await
        .expect("empty explicitly-created namespace must be droppable");
    assert!(!catalog.namespace_exists(&namespace).await.expect("namespace should be gone"));
}

#[tokio::test]
async fn drop_namespace_not_found() {
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("missing".to_string());

    let error = catalog
        .drop_namespace(&namespace)
        .await
        .expect_err("missing namespace must not be dropped");

    assert_eq!(error.kind(), ErrorKind::NamespaceNotFound);
}

#[tokio::test]
async fn commit_vs_commit() {
    let env = make_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&env.catalog, &ns, "tbl").await;

    let commit1 = env.catalog.commit_transaction(vec![update_request(&table, "k", "v1").await]);
    let commit2 = env.catalog.commit_transaction(vec![update_request(&table, "k", "v2").await]);

    let (r1, r2) = tokio::join!(commit1, commit2);
    assert!(r1.is_ok());
    assert!(r2.is_ok());

    let loaded = env.catalog.load_table(table.identifier()).await.expect("load table after race");
    let loaded_location = loaded.metadata_location().expect("loaded metadata location");
    let committed_location_exists = r1
        .as_ref()
        .expect("first commit")
        .iter()
        .chain(r2.as_ref().expect("second commit"))
        .any(|table| table.metadata_location().expect("committed metadata location") == loaded_location);
    assert!(committed_location_exists);
    assert_eq!(metadata_version_in(loaded_location), 2);

    let root = unwrap_loaded(env.s3_storage.load_root(None).await.expect("load root")).0;
    assert_eq!(
        root.get_active(&TableKey::from_ident(table.identifier()))
            .expect("active root entry")
            .metadata_location()
            .as_str(),
        loaded.metadata_location().expect("loaded metadata location")
    );
}

#[tokio::test]
async fn commit_vs_drop() {
    let env = make_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&env.catalog, &ns, "tbl").await;

    let commit = env.catalog.commit_transaction(vec![update_request(&table, "k", "v").await]);
    let drop_op = env.catalog.drop_table(table.identifier());

    let (r1, r2) = tokio::join!(commit, drop_op);
    // Unlike the rename races below, `(Ok, Ok)` is a legitimate serialization
    // here: commit keeps the entry active under the same `table_id`, so if it
    // wins the first root CAS, `drop_table` rereads the fresh root, still finds
    // the active entry by `table_id`, and tombstones it — both operations
    // succeed (commit-happened-before-drop). Only the loser of a *both-active*
    // race can fail, so at least one operation must succeed.
    assert!(r1.is_ok() || r2.is_ok());

    let root = unwrap_loaded(env.s3_storage.load_root(None).await.expect("load root")).0;
    let key = TableKey::from_ident(table.identifier());
    match (r1, r2) {
        (Ok(_), Ok(())) => {
            // commit-happened-before-drop: the table ends tombstoned.
            let error = env.catalog.load_table(table.identifier()).await.expect_err("dropped table");
            assert_eq!(error.kind(), ErrorKind::TableNotFound);
            assert!(root.get_active(&key).is_none());
        }
        (Ok(response), Err(_)) => {
            let committed = response.into_iter().next().expect("committed table");
            let loaded = env.catalog.load_table(table.identifier()).await.expect("load committed table");
            assert_eq!(loaded.metadata_location(), committed.metadata_location());
            assert_eq!(
                root.get_active(&key).expect("active root entry").metadata_location().as_str(),
                committed.metadata_location().expect("committed metadata location")
            );
        }
        (Err(_), Ok(())) => {
            let error = env.catalog.load_table(table.identifier()).await.expect_err("dropped table");
            assert_eq!(error.kind(), ErrorKind::TableNotFound);
            assert!(root.get_active(&key).is_none());
        }
        (Err(_), Err(_)) => unreachable!("at least one operation must succeed"),
    }
}

#[tokio::test]
async fn cached_s3_revalidates_root_via_304() {
    let env: CachedTestEnv = make_cached_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    // Prime the root cache through the production composition.
    create_table(&env.catalog, &ns, "tbl").await;

    // Isolate the reads under test from the priming traffic above.
    env.counting.reset();

    let ident = TableIdent::new(ns, "tbl".to_string());
    assert!(env.catalog.table_exists(&ident).await.expect("first exists"));
    assert!(env.catalog.table_exists(&ident).await.expect("second exists"));

    // Each read revalidates the cached root against S3 and receives a 304; the
    // body is served from cache, so the inner backend never refetches it. This is
    // the real `If-None-Match` round-trip the feature exists for, end-to-end.
    assert_eq!(env.counting.not_modified(), 2, "both reads must short-circuit on a 304");
    assert_eq!(env.counting.loaded(), 0, "a fresh cache must not refetch the root body");
}

#[tokio::test]
async fn cached_s3_picks_up_external_root_change() {
    let env: CachedTestEnv = make_cached_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    // Prime this catalog's cache with the current root.
    create_table(&env.catalog, &ns, "tbl").await;

    // A second catalog over the SAME bucket, wired straight to raw S3 — it
    // bypasses the cache under test and bumps the real root etag.
    let external = test_catalog(
        env.s3_storage.clone(),
        FileIO::new_with_memory(),
        env.tables_uri_prefix.clone(),
    );
    create_table(&external, &ns, "tbl_external").await;

    env.counting.reset();

    // The cached catalog still holds the pre-change etag; its conditional read
    // now misses the 304, refetches the fresh root body, and sees the new table.
    let external_ident = TableIdent::new(ns, "tbl_external".to_string());
    assert!(
        env.catalog.table_exists(&external_ident).await.expect("external table visible"),
        "cache must revalidate and pick up the externally written root"
    );
    assert!(
        env.counting.loaded() >= 1,
        "a stale etag must force a root body refetch"
    );
    assert_eq!(env.counting.not_modified(), 0, "a stale etag cannot produce a 304");
}

#[tokio::test]
async fn rename_vs_rename() {
    let env = make_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&env.catalog, &ns, "tbl").await;

    let dest1 = TableIdent::new(ns.clone(), "tbl_a".to_string());
    let dest2 = TableIdent::new(ns, "tbl_b".to_string());

    let r1 = env.catalog.rename_table(table.identifier(), &dest1);
    let r2 = env.catalog.rename_table(table.identifier(), &dest2);

    let (v1, v2) = tokio::join!(r1, r2);
    // Strict XOR holds in both CAS orders here (unlike `commit_vs_drop`): the
    // winning rename removes the source key from `tables`, so the loser rereads a
    // root where the source is gone and fails with `TableNotFound`.
    assert!(v1.is_ok() ^ v2.is_ok());

    let root = unwrap_loaded(env.s3_storage.load_root(None).await.expect("load root")).0;
    let source_key = TableKey::from_ident(table.identifier());
    let dest1_key = TableKey::from_ident(&dest1);
    let dest2_key = TableKey::from_ident(&dest2);

    assert!(root.get_active(&source_key).is_none());
    let dest1_exists = env.catalog.table_exists(&dest1).await.expect("dest1 exists");
    let dest2_exists = env.catalog.table_exists(&dest2).await.expect("dest2 exists");
    assert!(dest1_exists ^ dest2_exists);
    assert_eq!(root.get_active(&dest1_key).is_some(), dest1_exists);
    assert_eq!(root.get_active(&dest2_key).is_some(), dest2_exists);
}

#[tokio::test]
async fn rename_vs_drop() {
    let env = make_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&env.catalog, &ns, "tbl").await;
    let dest = TableIdent::new(ns, "tbl_renamed".to_string());

    let rename = env.catalog.rename_table(table.identifier(), &dest);
    let drop_op = env.catalog.drop_table(table.identifier());

    let (r1, r2) = tokio::join!(rename, drop_op);
    // Strict XOR holds in both CAS orders here (unlike `commit_vs_drop`): whoever
    // wins removes the source key from `tables` (rename moves it, drop tombstones
    // it), so the loser rereads a root without the source and fails.
    assert!(r1.is_ok() ^ r2.is_ok());

    let root = unwrap_loaded(env.s3_storage.load_root(None).await.expect("load root")).0;
    let source_key = TableKey::from_ident(table.identifier());
    let dest_key = TableKey::from_ident(&dest);
    match (r1, r2) {
        (Ok(()), Err(_)) => {
            let loaded = env.catalog.load_table(&dest).await.expect("load renamed table");
            assert!(env.catalog.load_table(table.identifier()).await.is_err());
            assert!(root.get_active(&source_key).is_none());
            assert_eq!(
                root.get_active(&dest_key)
                    .expect("active renamed entry")
                    .metadata_location()
                    .as_str(),
                loaded.metadata_location().expect("renamed metadata location")
            );
        }
        (Err(_), Ok(())) => {
            assert!(env.catalog.load_table(&dest).await.is_err());
            assert!(env.catalog.load_table(table.identifier()).await.is_err());
            assert!(root.get_active(&source_key).is_none());
            assert!(root.get_active(&dest_key).is_none());
        }
        _ => unreachable!("exactly one operation must succeed"),
    }
}

#[tokio::test]
async fn create_vs_drop_same_name() {
    let env = make_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&env.catalog, &ns, "tbl").await;
    let drop_op = env.catalog.drop_table(table.identifier());
    let create_op = env.catalog.create_table(
        &ns,
        TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
    );

    let (drop_result, create_result) = tokio::join!(drop_op, create_op);
    assert!(drop_result.is_ok() || create_result.is_ok());

    let root = unwrap_loaded(env.s3_storage.load_root(None).await.expect("load root")).0;
    let key = TableKey::from_ident(table.identifier());
    match (drop_result, create_result) {
        (Ok(()) | Err(_), Ok(created)) => {
            let loaded = env
                .catalog
                .load_table(created.identifier())
                .await
                .expect("load recreated table");
            assert_eq!(loaded.identifier(), created.identifier());
            assert_eq!(loaded.metadata_location(), created.metadata_location());
            assert_eq!(
                root.get_active(&key)
                    .expect("active recreated entry")
                    .metadata_location()
                    .as_str(),
                created.metadata_location().expect("created metadata location")
            );
        }
        (Ok(()), Err(_)) => {
            let loaded = env.catalog.load_table(table.identifier()).await.expect_err("dropped table");
            assert_eq!(loaded.kind(), ErrorKind::TableNotFound);
            assert!(root.get_active(&key).is_none());
        }
        _ => unreachable!("exactly one operation must succeed"),
    }
}

#[tokio::test]
async fn tx_ab_vs_single_a() {
    let env = make_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    let a = create_table(&env.catalog, &ns, "a").await;
    let b = create_table(&env.catalog, &ns, "b").await;

    let tx_ab = env.catalog.commit_transaction(vec![
        update_request(&a, "k", "v1").await,
        update_request(&b, "k", "v1").await,
    ]);
    let single_a = env.catalog.commit_transaction(vec![update_request(&a, "k", "v2").await]);

    let (r1, r2) = tokio::join!(tx_ab, single_a);
    assert!(r1.is_ok());
    assert!(r2.is_ok());

    let loaded_a = env.catalog.load_table(a.identifier()).await.expect("load table a");
    let loaded_b = env.catalog.load_table(b.identifier()).await.expect("load table b");
    let root = unwrap_loaded(env.s3_storage.load_root(None).await.expect("load root")).0;

    assert_eq!(
        metadata_version_in(loaded_a.metadata_location().expect("a location")),
        2
    );
    assert_eq!(
        metadata_version_in(loaded_b.metadata_location().expect("b location")),
        1
    );

    assert_eq!(
        root.get_active(&TableKey::from_ident(a.identifier()))
            .expect("active a")
            .metadata_location()
            .as_str(),
        loaded_a.metadata_location().expect("loaded a location")
    );
    assert_eq!(
        root.get_active(&TableKey::from_ident(b.identifier()))
            .expect("active b")
            .metadata_location()
            .as_str(),
        loaded_b.metadata_location().expect("loaded b location")
    );
}

#[tokio::test]
async fn tx_ab_vs_tx_ac() {
    let env = make_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    let a = create_table(&env.catalog, &ns, "a").await;
    let b = create_table(&env.catalog, &ns, "b").await;
    let c = create_table(&env.catalog, &ns, "c").await;

    let tx_ab = env.catalog.commit_transaction(vec![
        update_request(&a, "k", "ab").await,
        update_request(&b, "k", "ab").await,
    ]);
    let tx_ac = env.catalog.commit_transaction(vec![
        update_request(&a, "k", "ac").await,
        update_request(&c, "k", "ac").await,
    ]);

    let (r1, r2) = tokio::join!(tx_ab, tx_ac);
    assert!(r1.is_ok());
    assert!(r2.is_ok());

    let loaded_a = env.catalog.load_table(a.identifier()).await.expect("load table a");
    let loaded_b = env.catalog.load_table(b.identifier()).await.expect("load table b");
    let loaded_c = env.catalog.load_table(c.identifier()).await.expect("load table c");
    let root = unwrap_loaded(env.s3_storage.load_root(None).await.expect("load root")).0;
    assert_eq!(
        metadata_version_in(loaded_a.metadata_location().expect("a location")),
        2
    );
    assert_eq!(
        metadata_version_in(loaded_b.metadata_location().expect("b location")),
        1
    );
    assert_eq!(
        metadata_version_in(loaded_c.metadata_location().expect("c location")),
        1
    );

    assert_eq!(
        root.get_active(&TableKey::from_ident(a.identifier()))
            .expect("active a")
            .metadata_location()
            .as_str(),
        loaded_a.metadata_location().expect("loaded a location")
    );
    assert_eq!(
        root.get_active(&TableKey::from_ident(b.identifier()))
            .expect("active b")
            .metadata_location()
            .as_str(),
        loaded_b.metadata_location().expect("loaded b location")
    );
    assert_eq!(
        root.get_active(&TableKey::from_ident(c.identifier()))
            .expect("active c")
            .metadata_location()
            .as_str(),
        loaded_c.metadata_location().expect("loaded c location")
    );
}

#[tokio::test]
async fn commit_transaction_retries_root_cas_conflict_and_succeeds() {
    // A transient root-CAS conflict (e.g. another writer winning the race)
    // is recoverable: we should re-merge the already prepared metadata onto
    // the freshly loaded root and retry the CAS. The prepared metadata file
    // is reused — only one extra metadata object is written.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let original_metadata_location = table.metadata_location().expect("original metadata location").to_string();

    storage.fail_next_save_root();
    let updated = catalog
        .commit_transaction(vec![update_request(&table, "k", "v1").await])
        .await
        .expect("retry must succeed after a single CAS conflict")
        .into_iter()
        .next()
        .expect("commit returns one table");
    let updated_location = updated.metadata_location().expect("updated metadata location").to_string();

    let metadata_locations = storage.metadata_locations();
    // Exactly two: original (00000-*) and the prepared successor (00001-*).
    // Critically, NO third file — the retry must not rebuild metadata.
    assert_eq!(
        metadata_locations.len(),
        2,
        "retry must reuse the prepared metadata file"
    );
    assert!(metadata_locations.iter().any(|loc| loc == &original_metadata_location));
    assert!(metadata_locations.iter().any(|loc| loc == &updated_location));
    assert!(updated_location.contains("/metadata/00001-"));

    let root = storage.root_state().await;
    let active = root
        .get_active(&TableKey::from_ident(table.identifier()))
        .expect("active root entry");
    assert_eq!(active.metadata_location().as_str(), updated_location.as_str());
}

#[tokio::test]
async fn create_table_conflict_with_foreign_winner_fails_as_already_exists() {
    // After a CAS conflict on `create_table`, the read-only conflict probe must
    // report `TableAlreadyExists` when a *different* table (foreign table_id and
    // metadata location) has grabbed the same name in the meantime.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let key = TableKey::from_ident(&TableIdent::new(namespace.clone(), "tbl".to_string()));
    // A concurrent writer links a different table under the same name between
    // our load_root and save_root.
    let foreign_entry = CatalogTableLink::new(
        TableId::from(Uuid::new_v4()),
        TableMetadataLocation::new("memory://catalog/tables/foreign/metadata/00000-foreign.metadata.json".to_string()),
    );
    storage
        .set_before_save_root_hook(move |root| {
            root.link_table(key.clone(), foreign_entry.clone())
                .expect("inject foreign table at the same key");
        })
        .await;

    let error = catalog
        .create_table(
            &namespace,
            TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
        )
        .await
        .expect_err("create must fail when a foreign table won the name");
    assert_eq!(error.kind(), ErrorKind::TableAlreadyExists);
}

#[tokio::test]
async fn update_root_reloads_once_per_attempt_on_conflict() {
    // Each root CAS conflict must cost exactly one reload on the retry — not the
    // two round-trips a separate conflict-time reload would add. For K conflicts
    // the loop loads the root K+1 times (K failed attempts plus the success),
    // never 2*K+1.
    const CONFLICTS: usize = 3;
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("seed namespace");

    let loads_before = storage.load_root_call_count();
    storage.fail_next_n_save_roots(CONFLICTS);
    catalog
        .update_namespace(&namespace, HashMap::from([("k".to_string(), "v".to_string())]))
        .await
        .expect("update must succeed after transient root conflicts");

    assert_eq!(
        storage.load_root_call_count() - loads_before,
        CONFLICTS + 1,
        "each conflict must cost exactly one reload (K+1 total), not 2*K+1",
    );
}

/// Build a synthetic "competitor" commit at version N+1 against the table's
/// current head and persist its metadata file. Returns the new metadata
/// location, ready to be patched into root via `apply_commit` from a
/// before-save hook to simulate another writer winning the race.
async fn prepare_competitor_commit(
    storage: &Arc<ConflictOnSaveStorage>,
    identifier: &TableIdent,
) -> TableMetadataLocation {
    let key = TableKey::from_ident(identifier);
    let persisted = {
        let root = storage.root_state().await;
        root.get_active(&key).expect("active entry").clone()
    };
    let metadata = Arc::unwrap_or_clone(
        storage
            .read_table_metadata(persisted.metadata_location().as_str())
            .await
            .expect("read current metadata"),
    );
    let prepared = IcebergTableMetadata::new(persisted.metadata_location().clone(), metadata)
        .prepare_commit(identifier.clone(), persisted, Vec::new(), Vec::new())
        .expect("build competitor commit");
    storage
        .write_table_metadata(
            prepared.updated.metadata_location().as_str(),
            prepared.updated.metadata(),
        )
        .await
        .expect("persist competitor metadata");
    prepared.updated.metadata_location().clone()
}

/// Publish a competitor commit on top of the table's current head, advancing the
/// stored root to the new successor. Models a concurrent writer that lands a
/// fresh version while another commit is mid-retry.
async fn publish_competitor_commit(storage: &Arc<ConflictOnSaveStorage>, identifier: &TableIdent) {
    let key = TableKey::from_ident(identifier);
    let competitor = prepare_competitor_commit(storage, identifier).await;
    loop {
        let (root, version) = unwrap_loaded(storage.load_root(None).await.expect("load root"));
        let mut next_root = (*root).clone();
        let head = next_root.get_active(&key).expect("active head").clone();
        next_root
            .apply_commit(&key, &head, competitor.clone())
            .expect("advance head to competitor");
        if storage.save_root(Arc::new(next_root), &version).await.is_ok() {
            break;
        }
    }
}

#[tokio::test]
async fn commit_transaction_rebuilds_on_table_state_conflict() {
    // A *table-state* conflict (somebody else moved my target table while I
    // was preparing metadata on top of N) cannot be patched up by re-merging —
    // the prepared file is now an orphan of an older head. The retry loop must
    // drop the stale prepared metadata, read the fresh head, re-validate requirements,
    // and write a brand-new successor (N+2).
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let key = TableKey::from_ident(table.identifier());

    let competitor_location = prepare_competitor_commit(&storage, table.identifier()).await;
    let key_for_hook = key.clone();
    let competitor_for_hook = competitor_location.clone();
    storage
        .set_before_save_root_hook(move |root| {
            let entry = root.get_active(&key_for_hook).expect("active hook entry").clone();
            root.apply_commit(&key_for_hook, &entry, competitor_for_hook.clone())
                .expect("inject competitor commit into root");
        })
        .await;

    let updated = catalog
        .commit_transaction(vec![update_request(&table, "k", "v1").await])
        .await
        .expect("rebuild after table-state conflict must succeed")
        .into_iter()
        .next()
        .expect("one table");
    let winner_location = updated.metadata_location().expect("winner location").to_string();

    assert!(
        winner_location.contains("/metadata/00002-"),
        "winner must be rebuilt at N=2, got {winner_location}",
    );
    let root = storage.root_state().await;
    assert_eq!(
        root.get_active(&key).expect("active winner").metadata_location().as_str(),
        winner_location.as_str(),
    );

    let locations = storage.metadata_locations();
    let n2 = locations.iter().filter(|l| l.contains("/metadata/00002-")).count();
    let n1 = locations.iter().filter(|l| l.contains("/metadata/00001-")).count();
    let n0 = locations.iter().filter(|l| l.contains("/metadata/00000-")).count();
    assert_eq!(n2, 1, "exactly one N=2 file (the rebuilt successor)");
    assert_eq!(
        n1, 2,
        "exactly two N=1 files: the competitor and our now-orphaned prepared metadata",
    );
    assert_eq!(n0, 1, "exactly one N=0 file (initial create)");
    assert!(locations.iter().any(|l| l == competitor_location.as_str()));
}

#[tokio::test]
async fn commit_transaction_fails_when_table_is_recreated_during_commit() {
    // ABA guard: a prepared N=1 file from the old table must never be applied to
    // a new table recreated under the same namespace/name at N=0.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let original = create_table(&catalog, &namespace, "tbl").await;
    let key = TableKey::from_ident(original.identifier());
    let original_location = original.metadata_location().expect("original metadata location").to_string();

    let recreated_uuid = Uuid::new_v4();
    let recreated_creation = TableCreation::builder()
        .name("tbl".to_string())
        .schema(test_schema())
        .location(format!("memory://catalog/tables/{recreated_uuid}"))
        .build();
    let recreated_metadata =
        IcebergTableMetadata::create(recreated_creation, recreated_uuid).expect("build recreated metadata");
    let recreated_table_id = recreated_uuid;
    let recreated_location = recreated_metadata.metadata_location().as_str().to_string();
    storage
        .write_table_metadata(
            recreated_metadata.metadata_location().as_str(),
            recreated_metadata.metadata(),
        )
        .await
        .expect("persist recreated metadata");
    let recreated_entry = CatalogTableLink::new(
        TableId::from(recreated_table_id),
        TableMetadataLocation::new(recreated_location.clone()),
    );
    let key_for_hook = key.clone();
    storage
        .set_before_save_root_hook(move |root| {
            let table_id = *root.get_active(&key_for_hook).expect("active original table").table_id();
            root.tombstone(&key_for_hook, &table_id).expect("drop original table");
            root.link_table(key_for_hook.clone(), recreated_entry.clone())
                .expect("recreate table under the same key");
        })
        .await;

    let error = catalog
        .commit_transaction(vec![update_request(&original, "k", "v1").await])
        .await
        .expect_err("commit against a recreated table must fail");

    assert!(matches!(error, Error::CommitConflict));

    let root = storage.root_state().await;
    let active = root.get_active(&key).expect("active recreated table");
    assert_eq!(active.table_id().as_uuid(), &recreated_table_id);
    assert_eq!(active.metadata_location().as_str(), recreated_location.as_str());
    assert_ne!(active.metadata_location().as_str(), original_location.as_str());

    let loaded = catalog.load_table(original.identifier()).await.expect("load recreated table");
    assert_eq!(loaded.metadata_location(), Some(recreated_location.as_str()));
}

#[tokio::test]
async fn commit_transaction_converges_after_lost_ack() {
    // Lost-ack convergence: our root CAS physically landed (the prepared N=1
    // pointer is now the head) but the success response was lost and surfaced
    // as CommitConflict. The retry must recognise its own write — reusing the
    // prepared metadata, not rebuilding a redundant N=2 successor — and report
    // success without advancing the head.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let key = TableKey::from_ident(table.identifier());

    storage.lose_ack_on_next_save_root();
    let updated = catalog
        .commit_transaction(vec![update_request(&table, "k", "v1").await])
        .await
        .expect("commit must converge on its own landed write after a lost ack")
        .into_iter()
        .next()
        .expect("one table");
    let winner_location = updated.metadata_location().expect("winner location").to_string();

    assert!(
        winner_location.contains("/metadata/00001-"),
        "winner must stay at the prepared N=1, got {winner_location}",
    );

    let locations = storage.metadata_locations();
    assert_eq!(
        locations.len(),
        2,
        "exactly two metadata files (00000-, 00001-) — no N=2 rebuild, got {locations:?}",
    );
    assert!(locations.iter().any(|l| l.contains("/metadata/00000-")));
    assert!(locations.iter().any(|l| l.contains("/metadata/00001-")));
    assert!(
        !locations.iter().any(|l| l.contains("/metadata/00002-")),
        "a redundant N=2 successor must not be written, got {locations:?}",
    );

    let root = storage.root_state().await;
    assert_eq!(
        root.get_active(&key).expect("active head").metadata_location().as_str(),
        winner_location.as_str(),
        "head must remain the single landed N=1 file",
    );
}

#[tokio::test]
async fn commit_transaction_treats_landed_commit_in_lineage_as_applied() {
    // Lost-ack + concurrent advance: our root CAS physically landed (the prepared
    // N=1 pointer became the head) but the success response was lost and surfaced
    // as CommitConflict. Before the retry reloaded, a concurrent writer advanced
    // the table to N=2 on top of our N=1, so our prepared file is now an ancestor
    // of the head. The retry must recognise it already sits in the head lineage
    // and report success WITHOUT rebuilding a redundant successor on the newer
    // head — which would duplicate the update or trip a requirement check on
    // already-committed state.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let key = TableKey::from_ident(table.identifier());

    storage.lose_ack_on_next_save_root();
    let hook_storage = storage.clone();
    let hook_identifier = table.identifier().clone();
    storage
        .set_after_lost_ack_hook(move || {
            let storage = hook_storage.clone();
            let identifier = hook_identifier.clone();
            Box::pin(async move {
                publish_competitor_commit(&storage, &identifier).await;
            })
        })
        .await;

    let updated = catalog
        .commit_transaction(vec![update_request(&table, "k", "v1").await])
        .await
        .expect("commit must treat its landed commit in the head lineage as applied")
        .into_iter()
        .next()
        .expect("one table");
    let our_location = updated.metadata_location().expect("our location").to_string();

    // We get back our own landed N=1, never a rebuilt successor.
    assert!(
        our_location.contains("/metadata/00001-"),
        "returned table must be our landed N=1, got {our_location}",
    );

    // The head stays at the concurrent writer's N=2 — never regressed to our N=1,
    // never rebuilt to N=3.
    let root = storage.root_state().await;
    let head_location = root
        .get_active(&key)
        .expect("active head")
        .metadata_location()
        .as_str()
        .to_string();
    assert!(
        head_location.contains("/metadata/00002-"),
        "head must remain the concurrent N=2, got {head_location}",
    );

    let locations = storage.metadata_locations();
    assert_eq!(
        locations.len(),
        3,
        "exactly three metadata files (00000-, our 00001-, concurrent 00002-) — no N=3 rebuild, got {locations:?}",
    );
    assert!(
        !locations.iter().any(|l| l.contains("/metadata/00003-")),
        "a redundant successor must not be rebuilt, got {locations:?}",
    );
}

#[tokio::test]
async fn drop_table_converges_after_lost_ack() {
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let key = TableKey::from_ident(table.identifier());
    let table_id = table.metadata().uuid();

    storage.lose_ack_on_next_save_root();
    catalog
        .drop_table(table.identifier())
        .await
        .expect("drop must converge on its own landed tombstone after a lost ack");

    let root = storage.root_state().await;
    assert!(root.get_active(&key).is_none());
    let tombstones = root_tombstones_json(&root).as_object().cloned().expect("tombstones object");
    assert!(tombstones.contains_key(&table_id.to_string()));
}

#[tokio::test]
async fn drop_namespace_converges_after_lost_ack() {
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    storage.lose_ack_on_next_save_root();
    catalog
        .drop_namespace(&namespace)
        .await
        .expect("drop must converge on its own landed removal after a lost ack");

    assert!(storage.root_state().await.get_namespace(&namespace).is_none());
}

#[tokio::test]
async fn drop_table_rejects_foreign_occupant_after_lost_ack() {
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let key = TableKey::from_ident(table.identifier());
    let foreign_table_id = Uuid::new_v4();
    let foreign_entry = CatalogTableLink::new(
        TableId::from(foreign_table_id),
        TableMetadataLocation::new(format!(
            "memory://catalog/tables/{foreign_table_id}/metadata/00000-foreign.metadata.json"
        )),
    );
    let key_for_hook = key.clone();
    storage
        .set_before_save_root_hook(move |root| {
            root.link_table(key_for_hook.clone(), foreign_entry.clone())
                .expect("foreign table takes the dropped name");
        })
        .await;

    storage.lose_ack_on_next_save_root();
    let error = catalog
        .drop_table(table.identifier())
        .await
        .expect_err("drop must reject a foreign table occupying the dropped name");

    assert_eq!(error.kind(), ErrorKind::TableNotFound);
    let root = storage.root_state().await;
    assert_eq!(
        root.get_active(&key).expect("foreign occupant remains").table_id().as_uuid(),
        &foreign_table_id,
    );
}

#[tokio::test]
async fn rename_table_converges_after_lost_ack() {
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let renamed = TableIdent::new(namespace, "tbl_renamed".to_string());
    let src_key = TableKey::from_ident(table.identifier());
    let dst_key = TableKey::from_ident(&renamed);
    let table_id = table.metadata().uuid();

    storage.lose_ack_on_next_save_root();
    catalog
        .rename_table(table.identifier(), &renamed)
        .await
        .expect("rename must converge on its own landed destination after a lost ack");

    let root = storage.root_state().await;
    assert!(root.get_active(&src_key).is_none());
    assert_eq!(
        root.get_active(&dst_key).expect("renamed table").table_id().as_uuid(),
        &table_id,
    );
}

#[tokio::test]
async fn rename_table_rejects_foreign_destination_after_lost_ack() {
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let renamed = TableIdent::new(namespace, "tbl_renamed".to_string());
    let dst_key = TableKey::from_ident(&renamed);
    let foreign_table_id = Uuid::new_v4();
    let foreign_entry = CatalogTableLink::new(
        TableId::from(foreign_table_id),
        TableMetadataLocation::new(format!(
            "memory://catalog/tables/{foreign_table_id}/metadata/00000-foreign.metadata.json"
        )),
    );
    let dst_key_for_hook = dst_key.clone();
    storage
        .set_before_save_root_hook(move |root| {
            let table_id = *root
                .get_active(&dst_key_for_hook)
                .expect("active landed rename target")
                .table_id();
            root.tombstone(&dst_key_for_hook, &table_id).expect("drop landed rename target");
            root.link_table(dst_key_for_hook.clone(), foreign_entry.clone())
                .expect("foreign table takes the rename destination");
        })
        .await;

    storage.lose_ack_on_next_save_root();
    let error = catalog
        .rename_table(table.identifier(), &renamed)
        .await
        .expect_err("rename must reject a foreign table occupying the destination");

    assert_eq!(error.kind(), ErrorKind::TableAlreadyExists);
    let root = storage.root_state().await;
    assert_eq!(
        root.get_active(&dst_key)
            .expect("foreign destination remains")
            .table_id()
            .as_uuid(),
        &foreign_table_id,
    );
}

#[tokio::test]
async fn commit_transaction_multi_table_rebuilds_only_moved_table() {
    // When a multi-table commit hits a table-state conflict on one table,
    // only that table must be rebuilt; the prepared metadata for the
    // untouched tables must be reused.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table_a = create_table(&catalog, &namespace, "tbl_a").await;
    let table_b = create_table(&catalog, &namespace, "tbl_b").await;
    let key_b = TableKey::from_ident(table_b.identifier());

    let competitor_b = prepare_competitor_commit(&storage, table_b.identifier()).await;
    let key_for_hook = key_b.clone();
    storage
        .set_before_save_root_hook(move |root| {
            let entry = root.get_active(&key_for_hook).expect("active hook entry").clone();
            root.apply_commit(&key_for_hook, &entry, competitor_b.clone())
                .expect("inject competitor commit for table_b");
        })
        .await;

    let commits = vec![
        update_request(&table_a, "k", "va").await,
        update_request(&table_b, "k", "vb").await,
    ];
    let updated = catalog
        .commit_transaction(commits)
        .await
        .expect("multi-table commit must succeed after partial rebuild");
    assert_eq!(updated.len(), 2);
    let by_ident: HashMap<_, _> = updated
        .iter()
        .map(|t| (t.identifier().clone(), t.metadata_location().expect("loc").to_string()))
        .collect();
    let winner_a = by_ident.get(table_a.identifier()).expect("winner a");
    let winner_b = by_ident.get(table_b.identifier()).expect("winner b");
    assert!(
        winner_a.contains("/metadata/00001-"),
        "table_a must reuse its prepared N=1 metadata, got {winner_a}",
    );
    assert!(
        winner_b.contains("/metadata/00002-"),
        "table_b must be rebuilt at N=2, got {winner_b}",
    );
}

#[tokio::test]
async fn commit_transaction_root_conflicts_do_not_inflate_metadata() {
    // Pure root-CAS conflicts must NOT cause metadata-file inflation: the
    // prepared successor is re-merged onto each fresh root without re-writing
    // table metadata. Verify across multiple consecutive conflicts.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;

    storage.fail_next_n_save_roots(3);
    let updated = catalog
        .commit_transaction(vec![update_request(&table, "k", "v1").await])
        .await
        .expect("must succeed after consecutive root conflicts")
        .into_iter()
        .next()
        .expect("one table");
    let winner = updated.metadata_location().expect("winner").to_string();

    let locations = storage.metadata_locations();
    // No rebuilds: N=0 from create + N=1 prepared once and reused on every
    // retry. Hard cap at 2 to lock down the "no inflation" invariant.
    assert_eq!(
        locations.len(),
        2,
        "root-only conflicts must not write extra metadata files, got {locations:?}",
    );
    assert!(winner.contains("/metadata/00001-"));
}

#[tokio::test]
async fn commit_transaction_round_trips_through_cached_storage() {
    // Smoke-test the production composition: every call routed through
    // `CachedCatalogStorage`. Guards against accidental coupling between
    // the catalog and a *raw* storage backend that would silently bypass
    // the cache decorator in tests.
    let catalog = make_in_memory_catalog_cached();
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let original_location = table.metadata_location().expect("orig location").to_string();

    let updated = catalog
        .commit_transaction(vec![update_request(&table, "k", "v1").await])
        .await
        .expect("commit via cached storage")
        .into_iter()
        .next()
        .expect("one table");
    let updated_location = updated.metadata_location().expect("updated location").to_string();
    assert_ne!(updated_location, original_location);
    assert!(updated_location.contains("/metadata/00001-"));

    let reloaded = catalog.load_table(table.identifier()).await.expect("reload cached");
    assert_eq!(
        reloaded.metadata_location().expect("reloaded location"),
        updated_location
    );
    assert!(catalog.namespace_exists(&namespace).await.expect("ns exists"));
}

#[tokio::test]
async fn commit_transaction_root_cas_conflicts_reload_through_cache() {
    // Deterministically drive the invalidate-and-reload-on-conflict path
    // *through the cache*, with zero dependence on the runtime scheduler.
    //
    // `ConflictOnSaveStorage` returns `CommitConflict` on the next N `save_root`
    // calls without advancing the stored version, so a single commit hits N
    // synthetic root-CAS conflicts. Each conflict must, inside
    // `CachedCatalogStorage`:
    //   1. invalidate the cached root in `save_root` (the conflict arm), then
    //   2. reload the fresh root on the retry's `load_root(None)`.
    // The prepared metadata file is reused across retries (no rebuild), so only
    // the original and one successor object are written.
    //
    // A `join_all` race on the single-thread runtime would instead serialise
    // and never produce a real CAS loser, leaving this path uncovered.
    const CONFLICTS: usize = 3;
    let inner = Arc::new(ConflictOnSaveStorage::new());
    let cached: Arc<dyn CatalogStorage> = Arc::new(CachedCatalogStorage::new(
        inner.clone(),
        S3CatalogConfig::default().metadata_cache_cap,
    ));
    let catalog = test_catalog(cached, FileIO::new_with_memory(), "memory://catalog/tables".to_string());
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;

    inner.fail_next_n_save_roots(CONFLICTS);
    let updated = catalog
        .commit_transaction(vec![update_request(&table, "k", "v1").await])
        .await
        .expect("commit succeeds after N cache-mediated CAS conflicts")
        .into_iter()
        .next()
        .expect("one table");
    let updated_location = updated.metadata_location().expect("updated location").to_string();
    assert!(updated_location.contains("/metadata/00001-"));

    // No metadata inflation: the prepared successor is reused across every retry.
    let metadata_locations = inner.metadata_locations();
    assert_eq!(
        metadata_locations.len(),
        2,
        "retries must reuse the prepared metadata file"
    );

    // Catalog (through the cache) and inner storage (source of truth) agree on
    // the single committed successor.
    let loaded = catalog.load_table(table.identifier()).await.expect("load after conflicts");
    assert_eq!(loaded.metadata_location().expect("loaded location"), updated_location);
    let root = inner.root_state().await;
    assert_eq!(
        root.get_active(&TableKey::from_ident(table.identifier()))
            .expect("active root entry")
            .metadata_location()
            .as_str(),
        updated_location
    );
}

#[tokio::test]
async fn create_table_converges_after_lost_ack() {
    // Lost-ack convergence for create (symmetric with the commit/drop/rename
    // lost-ack tests): our root CAS physically landed (the table is now linked)
    // but the ack was lost and surfaced as CommitConflict. The retry must
    // recognise its own write — same table_id and metadata_location — and report
    // success instead of TableAlreadyExists, without writing a second metadata
    // file.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    storage.lose_ack_on_next_save_root();
    let created = catalog
        .create_table(
            &namespace,
            TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
        )
        .await
        .expect("create must converge on its own landed link after a lost ack");
    let created_location = created.metadata_location().expect("created metadata location").to_string();

    let key = TableKey::from_ident(created.identifier());
    let root = storage.root_state().await;
    assert_eq!(
        root.get_active(&key)
            .expect("active table after lost-ack create")
            .metadata_location()
            .as_str(),
        created_location.as_str(),
    );
    // The create wrote its metadata once; convergence must not write a second.
    assert_eq!(
        storage.metadata_locations().len(),
        1,
        "lost-ack convergence must not re-write table metadata"
    );
}

#[tokio::test]
async fn register_table_converges_after_lost_ack() {
    // Lost-ack convergence for register, mirroring create_table: the retry sees
    // its own already-linked entry (same table_id and metadata_location) and
    // converges to success rather than failing TableAlreadyExists.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let table_ident = TableIdent::new(namespace, "registered".to_string());
    let table_metadata = TableMetadataBuilder::new(
        test_schema(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        "memory://catalog/tables/external".to_string(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .expect("metadata builder")
    .build()
    .expect("table metadata")
    .metadata;
    let metadata_location = initial_metadata_location(table_metadata.location());
    storage
        .write_table_metadata(&metadata_location, &table_metadata)
        .await
        .expect("write metadata");

    storage.lose_ack_on_next_save_root();
    let registered = catalog
        .register_table(&table_ident, metadata_location.clone())
        .await
        .expect("register must converge on its own landed link after a lost ack");

    assert_eq!(registered.metadata_location(), Some(metadata_location.as_str()));
    let root = storage.root_state().await;
    assert_eq!(
        root.get_active(&TableKey::from_ident(&table_ident))
            .expect("active registered table")
            .metadata_location()
            .as_str(),
        metadata_location.as_str(),
    );
}

#[tokio::test]
async fn create_namespace_does_not_converge_after_lost_ack() {
    // create_namespace deliberately does NOT converge: it has no stable identity
    // token to tell a self-retry from a genuine duplicate, so a lost ack on its
    // own landed write surfaces as a spurious NamespaceAlreadyExists rather than
    // a silent success. Locks the documented decision in S3Catalog::create_namespace
    // so a future "converge namespaces too" change cannot pass silently.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());

    storage.lose_ack_on_next_save_root();
    let error = catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect_err("create_namespace must not converge after a lost ack");

    assert_eq!(error.kind(), ErrorKind::NamespaceAlreadyExists);
    // The landed write is still durable: the namespace exists exactly once.
    assert!(storage.root_state().await.get_namespace(&namespace).is_some());
}

#[tokio::test]
async fn commit_transaction_exhausts_cas_budget_with_max_attempts_error() {
    // A never-resolving root-CAS conflict must terminate with CasMaxAttempts once
    // the retry budget is spent — the optimistic loop on this critical path must
    // not spin forever. Uses a tiny budget plus more synthetic conflicts than it
    // allows, so the loop can never win.
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let small_budget = RetrierConfig {
        max_attempts: 3,
        rand_delay: Duration::ZERO,
        delays: vec![Duration::ZERO],
    };
    let catalog = S3Catalog::with_storage(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
        Retrier::new(small_budget),
        tokio_util::sync::CancellationToken::new(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;

    storage.fail_next_n_save_roots(100);
    let error = catalog
        .commit_transaction(vec![update_request(&table, "k", "v1").await])
        .await
        .expect_err("exhausted CAS budget must fail");

    assert!(matches!(error, Error::CasMaxAttempts));
}

#[tokio::test]
async fn concurrent_commits_to_same_table_serialize_on_real_s3() {
    // Real-S3 guarantee the in-memory hook tests cannot give: many writers
    // committing to the SAME table concurrently must all succeed via reload +
    // rebuild, and real S3 conditional-PUT CAS must serialize them with no lost
    // update — the head advances by exactly the number of writers.
    let env = make_catalog().await;
    let ns = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&env.catalog, &ns, "tbl").await;

    let c0 = env.catalog.commit_transaction(vec![update_request(&table, "k", "v0").await]);
    let c1 = env.catalog.commit_transaction(vec![update_request(&table, "k", "v1").await]);
    let c2 = env.catalog.commit_transaction(vec![update_request(&table, "k", "v2").await]);
    let c3 = env.catalog.commit_transaction(vec![update_request(&table, "k", "v3").await]);
    let c4 = env.catalog.commit_transaction(vec![update_request(&table, "k", "v4").await]);

    let (r0, r1, r2, r3, r4) = tokio::join!(c0, c1, c2, c3, c4);
    r0.expect("concurrent commit 0 must eventually succeed");
    r1.expect("concurrent commit 1 must eventually succeed");
    r2.expect("concurrent commit 2 must eventually succeed");
    r3.expect("concurrent commit 3 must eventually succeed");
    r4.expect("concurrent commit 4 must eventually succeed");

    let loaded = env
        .catalog
        .load_table(table.identifier())
        .await
        .expect("load after concurrent commits");
    assert_eq!(
        metadata_version_in(loaded.metadata_location().expect("loaded location")),
        5,
        "real S3 CAS must serialize all writers with no lost update",
    );

    let root = unwrap_loaded(env.s3_storage.load_root(None).await.expect("load root")).0;
    assert_eq!(
        root.get_active(&TableKey::from_ident(table.identifier()))
            .expect("active root entry")
            .metadata_location()
            .as_str(),
        loaded.metadata_location().expect("loaded metadata location"),
    );
}
