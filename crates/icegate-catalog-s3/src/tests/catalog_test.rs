#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::{FormatVersion, SortOrder, TableMetadataBuilder, UnboundPartitionSpec};
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableCreation, TableIdent};
use serde_json::Value;
use tokio::sync::RwLock;
use uuid::Uuid;

use super::common::{
    create_table, make_catalog, make_in_memory_catalog, make_in_memory_catalog_with_storage, test_schema,
    update_request,
};
use crate::catalog::S3Catalog;
use crate::error::Error;
use crate::model::{CatalogRoot, MetadataVersion, TableKey};
use crate::storage::{CatalogStorage, Version};

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

struct ConflictOnSaveStorage {
    root: RwLock<Option<(CatalogRoot, u64)>>,
    metadata: dashmap::DashMap<String, Bytes>,
    version: AtomicU64,
    fail_next_save_root: AtomicBool,
}

impl ConflictOnSaveStorage {
    fn new() -> Self {
        Self {
            root: RwLock::new(None),
            metadata: dashmap::DashMap::new(),
            version: AtomicU64::new(0),
            fail_next_save_root: AtomicBool::new(false),
        }
    }

    fn fail_next_save_root(&self) {
        self.fail_next_save_root.store(true, Ordering::SeqCst);
    }

    fn metadata_locations(&self) -> Vec<String> {
        let mut locations = self.metadata.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>();
        locations.sort();
        locations
    }

    async fn root_state(&self) -> CatalogRoot {
        self.load_root().await.expect("load root").0
    }
}

#[async_trait]
impl CatalogStorage for ConflictOnSaveStorage {
    async fn load_root(&self) -> crate::Result<(CatalogRoot, Version)> {
        let guard = self.root.read().await;
        Ok(match guard.as_ref() {
            Some((root, version)) => (root.clone(), Version::Etag(version.to_string())),
            None => (CatalogRoot::default(), Version::Absent),
        })
    }

    async fn save_root(&self, root: CatalogRoot, expected: &Version) -> crate::Result<()> {
        if self.fail_next_save_root.swap(false, Ordering::SeqCst) {
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
        Ok(())
    }

    async fn read_table_metadata(&self, location: &str) -> crate::Result<iceberg::spec::TableMetadata> {
        let payload = self
            .metadata
            .get(location)
            .map(|entry| Bytes::clone(entry.value()))
            .ok_or_else(|| Error::InvalidMetadata(format!("Metadata location not found: {location}")))?;
        serde_json::from_slice(&payload)
            .map_err(|error| Error::InvalidMetadata(format!("Invalid table metadata: {error}")))
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
    let default_uuid = default_created.metadata().uuid().to_string();
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

    let root = storage.load_root().await.expect("load root").0;
    let default_entry = root
        .get_active(&TableKey::from_ident(default_created.identifier()))
        .expect("default entry");
    let explicit_entry = root
        .get_active(&TableKey::from_ident(explicit_created.identifier()))
        .expect("explicit entry");
    assert_eq!(default_entry.table_id(), default_uuid.as_str());
    assert_eq!(
        explicit_entry.table_id(),
        &explicit_created.metadata().uuid().to_string()
    );
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
    assert_eq!(
        MetadataVersion::from_location(&updated_location)
            .expect("metadata version")
            .as_u32(),
        1
    );

    let root = storage.load_root().await.expect("load root").0;
    let entry = root.get_active(&TableKey::from_ident(&table_ident)).expect("active root entry");
    assert_eq!(entry.table_id(), &table_metadata.uuid().to_string());
    assert_eq!(entry.metadata_location(), updated_location.as_str());
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

    let root = storage.load_root().await.expect("load root").0;
    assert_eq!(
        root.get_active(&TableKey::from_ident(table_a.identifier()))
            .expect("active table a")
            .metadata_location(),
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

    let root = storage.load_root().await.expect("load root").0;
    assert_eq!(
        root.get_active(&TableKey::from_ident(table.identifier()))
            .expect("active table")
            .metadata_location(),
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
    let catalog = make_in_memory_catalog();
    let namespace = NamespaceIdent::new("nonexistent".to_string());

    let error = catalog
        .create_table(
            &namespace,
            TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
        )
        .await
        .expect_err("create table in missing namespace must fail");

    assert_eq!(error.kind(), ErrorKind::NamespaceNotFound);
}

#[tokio::test]
async fn empty_namespace_survives_reload_and_can_be_dropped() {
    let (catalog, storage) = make_in_memory_catalog_with_storage();
    let namespace = NamespaceIdent::from_vec(vec!["a".to_string(), "b".to_string()]).expect("namespace a.b");

    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create empty namespace");
    let root = storage.load_root().await.expect("load root after create").0;
    let namespaces = root_namespaces_json(&root).as_object().cloned().expect("namespaces object");
    assert!(namespaces.contains_key("a.b"));

    let reloaded_catalog = S3Catalog::with_storage(
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
    let final_catalog = S3Catalog::with_storage(
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

    let reloaded_catalog = S3Catalog::with_storage(
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

    let root = storage.load_root().await.expect("load root").0;
    assert_eq!(
        root.get_active(&TableKey::from_ident(recreated.identifier()))
            .expect("active recreated entry")
            .metadata_location(),
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

    let root = storage.load_root().await.expect("load root after rename").0;
    let active = root.get_active(&dst_key).expect("active renamed entry");
    assert_eq!(
        active.metadata_location(),
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
        MetadataVersion::from_location(created.metadata_location().expect("created metadata location"))
            .expect("metadata version")
            .as_u32(),
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
        MetadataVersion::from_location(first.metadata_location().expect("first metadata location"))
            .expect("metadata version")
            .as_u32(),
        1
    );
    assert_eq!(
        MetadataVersion::from_location(second.metadata_location().expect("second metadata location"))
            .expect("metadata version")
            .as_u32(),
        2
    );

    let (root, _) = storage.load_root().await.expect("load root");
    let key = TableKey::from_ident(second.identifier());
    let entry = root.get_active(&key).expect("active root entry");
    assert_eq!(
        MetadataVersion::from_location(entry.metadata_location())
            .expect("metadata version")
            .as_u32(),
        2
    );
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
        MetadataVersion::from_location(committed_a.metadata_location().expect("table a metadata location"))
            .expect("metadata version")
            .as_u32(),
        1
    );
    assert_eq!(
        MetadataVersion::from_location(committed_b.metadata_location().expect("table b metadata location"))
            .expect("metadata version")
            .as_u32(),
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
        MetadataVersion::from_location(updated_a.metadata_location().expect("updated table a metadata location"))
            .expect("metadata version")
            .as_u32(),
        2
    );

    let (root, _) = storage.load_root().await.expect("load root");
    let entry_a = root
        .get_active(&TableKey::from_ident(updated_a.identifier()))
        .expect("table a root entry");
    let entry_b = root
        .get_active(&TableKey::from_ident(committed_b.identifier()))
        .expect("table b root entry");
    assert_eq!(
        MetadataVersion::from_location(entry_a.metadata_location())
            .expect("metadata version")
            .as_u32(),
        2
    );
    assert_eq!(
        MetadataVersion::from_location(entry_b.metadata_location())
            .expect("metadata version")
            .as_u32(),
        1
    );
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

    let root = env.s3_storage.load_root().await.expect("load root").0;
    let active = root
        .get_active(&TableKey::from_ident(winner.identifier()))
        .expect("active root entry");
    assert_eq!(
        active.metadata_location(),
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
    assert!(r1.is_ok() ^ r2.is_ok());

    let winner = r1
        .as_ref()
        .ok()
        .and_then(|tables| tables.first())
        .or_else(|| r2.as_ref().ok().and_then(|tables| tables.first()))
        .expect("one commit winner");
    let loaded = env.catalog.load_table(table.identifier()).await.expect("load table after race");
    assert_eq!(loaded.metadata_location(), winner.metadata_location());
    assert_eq!(
        MetadataVersion::from_location(loaded.metadata_location().expect("loaded metadata location"))
            .expect("metadata version")
            .as_u32(),
        1
    );

    let root = env.s3_storage.load_root().await.expect("load root").0;
    assert_eq!(
        root.get_active(&TableKey::from_ident(table.identifier()))
            .expect("active root entry")
            .metadata_location(),
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
    assert!(r1.is_ok() ^ r2.is_ok());

    let root = env.s3_storage.load_root().await.expect("load root").0;
    let key = TableKey::from_ident(table.identifier());
    match (r1, r2) {
        (Ok(response), Err(_)) => {
            let committed = response.into_iter().next().expect("committed table");
            let loaded = env.catalog.load_table(table.identifier()).await.expect("load committed table");
            assert_eq!(loaded.metadata_location(), committed.metadata_location());
            assert_eq!(
                root.get_active(&key).expect("active root entry").metadata_location(),
                committed.metadata_location().expect("committed metadata location")
            );
        }
        (Err(_), Ok(())) => {
            let loaded = env.catalog.load_table(table.identifier()).await.expect_err("dropped table");
            assert_eq!(loaded.kind(), ErrorKind::TableNotFound);
            assert!(root.get_active(&key).is_none());
        }
        _ => unreachable!("exactly one operation must succeed"),
    }
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
    assert!(v1.is_ok() ^ v2.is_ok());

    let root = env.s3_storage.load_root().await.expect("load root").0;
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
    assert!(r1.is_ok() ^ r2.is_ok());

    let root = env.s3_storage.load_root().await.expect("load root").0;
    let source_key = TableKey::from_ident(table.identifier());
    let dest_key = TableKey::from_ident(&dest);
    match (r1, r2) {
        (Ok(()), Err(_)) => {
            let loaded = env.catalog.load_table(&dest).await.expect("load renamed table");
            assert!(env.catalog.load_table(table.identifier()).await.is_err());
            assert!(root.get_active(&source_key).is_none());
            assert_eq!(
                root.get_active(&dest_key).expect("active renamed entry").metadata_location(),
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

    let root = env.s3_storage.load_root().await.expect("load root").0;
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
                root.get_active(&key).expect("active recreated entry").metadata_location(),
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
    assert!(r1.is_ok() ^ r2.is_ok());

    let loaded_a = env.catalog.load_table(a.identifier()).await.expect("load table a");
    let loaded_b = env.catalog.load_table(b.identifier()).await.expect("load table b");
    let root = env.s3_storage.load_root().await.expect("load root").0;

    if let Ok(response) = r1 {
        let mut committed = response.into_iter();
        let committed_a = committed.next().expect("committed a");
        let committed_b = committed.next().expect("committed b");
        assert_eq!(loaded_a.metadata_location(), committed_a.metadata_location());
        assert_eq!(loaded_b.metadata_location(), committed_b.metadata_location());
        assert_eq!(
            MetadataVersion::from_location(loaded_a.metadata_location().expect("a location"))
                .expect("metadata version")
                .as_u32(),
            1
        );
        assert_eq!(
            MetadataVersion::from_location(loaded_b.metadata_location().expect("b location"))
                .expect("metadata version")
                .as_u32(),
            1
        );
    } else {
        assert_eq!(
            MetadataVersion::from_location(loaded_a.metadata_location().expect("a location"))
                .expect("metadata version")
                .as_u32(),
            1
        );
        assert_eq!(
            MetadataVersion::from_location(loaded_b.metadata_location().expect("b location"))
                .expect("metadata version")
                .as_u32(),
            0
        );
    }

    assert_eq!(
        root.get_active(&TableKey::from_ident(a.identifier()))
            .expect("active a")
            .metadata_location(),
        loaded_a.metadata_location().expect("loaded a location")
    );
    assert_eq!(
        root.get_active(&TableKey::from_ident(b.identifier()))
            .expect("active b")
            .metadata_location(),
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
    assert!(r1.is_ok() ^ r2.is_ok());

    let loaded_a = env.catalog.load_table(a.identifier()).await.expect("load table a");
    let loaded_b = env.catalog.load_table(b.identifier()).await.expect("load table b");
    let loaded_c = env.catalog.load_table(c.identifier()).await.expect("load table c");
    let root = env.s3_storage.load_root().await.expect("load root").0;
    assert_eq!(
        MetadataVersion::from_location(loaded_a.metadata_location().expect("a location"))
            .expect("metadata version")
            .as_u32(),
        1
    );

    if r1.is_ok() {
        assert_eq!(
            MetadataVersion::from_location(loaded_b.metadata_location().expect("b location"))
                .expect("metadata version")
                .as_u32(),
            1
        );
        assert_eq!(
            MetadataVersion::from_location(loaded_c.metadata_location().expect("c location"))
                .expect("metadata version")
                .as_u32(),
            0
        );
    } else {
        assert_eq!(
            MetadataVersion::from_location(loaded_a.metadata_location().expect("a location"))
                .expect("metadata version")
                .as_u32(),
            1
        );
        assert_eq!(
            MetadataVersion::from_location(loaded_b.metadata_location().expect("b location"))
                .expect("metadata version")
                .as_u32(),
            0
        );
        assert_eq!(
            MetadataVersion::from_location(loaded_c.metadata_location().expect("c location"))
                .expect("metadata version")
                .as_u32(),
            1
        );
    }

    assert_eq!(
        root.get_active(&TableKey::from_ident(a.identifier()))
            .expect("active a")
            .metadata_location(),
        loaded_a.metadata_location().expect("loaded a location")
    );
    assert_eq!(
        root.get_active(&TableKey::from_ident(b.identifier()))
            .expect("active b")
            .metadata_location(),
        loaded_b.metadata_location().expect("loaded b location")
    );
    assert_eq!(
        root.get_active(&TableKey::from_ident(c.identifier()))
            .expect("active c")
            .metadata_location(),
        loaded_c.metadata_location().expect("loaded c location")
    );
}

#[tokio::test]
async fn commit_transaction_leaves_orphan_metadata_when_root_save_conflicts() {
    let storage = Arc::new(ConflictOnSaveStorage::new());
    let catalog = S3Catalog::with_storage(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    let namespace = NamespaceIdent::new("ns1".to_string());
    let table = create_table(&catalog, &namespace, "tbl").await;
    let original_metadata_location = table.metadata_location().expect("original metadata location").to_string();

    storage.fail_next_save_root();
    let error = catalog
        .commit_transaction(vec![update_request(&table, "k", "v1").await])
        .await
        .expect_err("save_root conflict must fail");

    assert!(matches!(error, Error::CommitConflict));

    let metadata_locations = storage.metadata_locations();
    assert_eq!(metadata_locations.len(), 2);
    assert!(
        metadata_locations
            .iter()
            .any(|location| location == &original_metadata_location)
    );
    assert!(metadata_locations.iter().any(|location| {
        location.starts_with("memory://catalog/tables/")
            && location.contains("/metadata/00001-")
            && std::path::Path::new(location)
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
    }));

    let root = storage.root_state().await;
    let active = root
        .get_active(&TableKey::from_ident(table.identifier()))
        .expect("active root entry remains");
    assert_eq!(active.metadata_location(), original_metadata_location.as_str());

    let loaded = catalog.load_table(table.identifier()).await.expect("load original table");
    assert_eq!(loaded.metadata_location(), Some(original_metadata_location.as_str()));
}
