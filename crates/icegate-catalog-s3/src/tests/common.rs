use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use bytes::Bytes;
use dashmap::DashMap;
use iceberg::io::FileIO;
use iceberg::spec::{NestedField, PrimitiveType, Schema, TableMetadata, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation, TableIdent};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::CatalogCodecKind;
use crate::catalog::S3Catalog;
use crate::config::S3CatalogConfig;
use crate::config::cas_retrier_config_default;
use crate::error::{Error, Result};
use crate::infra::retrier::Retrier;
use crate::root::CatalogRoot;
use crate::storage::cached::CachedCatalogStorage;
use crate::storage::s3::S3CatalogStorage;
use crate::storage::{CatalogStorage, LoadOutcome, Version};

fn test_catalog(storage: Arc<dyn CatalogStorage>, file_io: FileIO, tables_uri_prefix: String) -> S3Catalog {
    S3Catalog::with_storage(
        storage,
        file_io,
        tables_uri_prefix,
        Retrier::new(cas_retrier_config_default()),
        tokio_util::sync::CancellationToken::new(),
    )
}

/// In-memory storage backend for unit tests.
pub(crate) struct InMemoryCatalogStorage {
    root: RwLock<Option<(Arc<CatalogRoot>, u64)>>,
    metadata: DashMap<String, Bytes>,
    version: AtomicU64,
}

impl InMemoryCatalogStorage {
    /// Create a new in-memory catalog storage.
    pub(crate) fn new() -> Self {
        Self {
            root: RwLock::new(None),
            metadata: DashMap::new(),
            version: AtomicU64::new(0),
        }
    }

    pub(crate) fn metadata_locations(&self) -> Vec<String> {
        let mut locations = self.metadata.iter().map(|entry| entry.key().clone()).collect::<Vec<_>>();
        locations.sort();
        locations
    }
}

#[async_trait]
impl CatalogStorage for InMemoryCatalogStorage {
    async fn load_root(&self, known: Option<&Version>) -> Result<LoadOutcome> {
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

    async fn save_root(&self, root: Arc<CatalogRoot>, expected: &Version) -> Result<Version> {
        let mut guard = self.root.write().await;
        if let Some((_, current_version)) = guard.as_ref() {
            if *expected != Version::Etag(current_version.to_string()) {
                drop(guard);
                return Err(Error::CommitConflict);
            }
        } else if *expected != Version::Absent {
            drop(guard);
            return Err(Error::CommitConflict);
        }

        let next_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        *guard = Some((root, next_version));
        drop(guard);
        Ok(Version::Etag(next_version.to_string()))
    }

    async fn read_table_metadata(&self, location: &str) -> Result<Arc<TableMetadata>> {
        let payload = self
            .metadata
            .get(location)
            .map(|entry| Bytes::clone(entry.value()))
            .ok_or_else(|| Error::InvalidMetadata(format!("Metadata location not found: {location}")))?;
        let metadata: TableMetadata = serde_json::from_slice(&payload)
            .map_err(|error| Error::InvalidMetadata(format!("Invalid table metadata: {error}")))?;
        Ok(Arc::new(metadata))
    }

    async fn write_table_metadata(&self, location: &str, metadata: &TableMetadata) -> Result<()> {
        let payload = serde_json::to_vec(metadata)
            .map(Bytes::from)
            .map_err(|error| Error::InvalidMetadata(format!("Failed to serialize metadata: {error}")))?;
        self.metadata.insert(location.to_string(), payload);
        Ok(())
    }
}

const MINIO_USER: &str = "minioadmin";
const MINIO_PASSWORD: &str = "minioadmin";

pub(crate) struct TestEnv {
    _minio: testcontainers::ContainerAsync<MinIO>,
    pub(crate) catalog: S3Catalog,
    pub(crate) s3_storage: Arc<S3CatalogStorage>,
    pub(crate) tables_uri_prefix: String,
}

/// Start a fresh `MinIO` container with a unique bucket and a raw S3 storage
/// backend over it. Shared by the raw ([`make_catalog`]) and cached
/// ([`make_cached_catalog`]) harnesses so the bucket bootstrap lives in one
/// place.
#[allow(clippy::expect_used)]
async fn bootstrap_minio_storage() -> (testcontainers::ContainerAsync<MinIO>, Arc<S3CatalogStorage>, String) {
    let minio = MinIO::default().start().await.expect("start minio");
    let port = minio.get_host_port_ipv4(9000).await.expect("minio port");
    let endpoint = format!("http://127.0.0.1:{port}");
    let bucket = format!("catalog-{}", Uuid::new_v4());

    let cfg = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(endpoint.clone())
        .credentials_provider(Credentials::new(MINIO_USER, MINIO_PASSWORD, None, None, "test"))
        .load()
        .await;
    let client = aws_sdk_s3::Client::new(&cfg);
    client.create_bucket().bucket(&bucket).send().await.expect("create bucket");

    let storage = Arc::new(
        S3CatalogStorage::new(
            &S3CatalogConfig {
                bucket: bucket.clone(),
                region: "us-east-1".to_string(),
                endpoint: Some(endpoint),
                access_key_id: Some(MINIO_USER.to_string()),
                secret_access_key: Some(MINIO_PASSWORD.to_string()),
                warehouse: "warehouse".to_string(),
                codec: CatalogCodecKind::Json,
                ..S3CatalogConfig::default()
            },
            tokio_util::sync::CancellationToken::new(),
        )
        .expect("storage"),
    );

    let tables_uri_prefix = format!("s3://{bucket}/warehouse/catalog/tables");
    (minio, storage, tables_uri_prefix)
}

pub(crate) async fn make_catalog() -> TestEnv {
    let (minio, storage, tables_uri_prefix) = bootstrap_minio_storage().await;
    let catalog = test_catalog(storage.clone(), FileIO::new_with_memory(), tables_uri_prefix.clone());

    TestEnv {
        _minio: minio,
        catalog,
        s3_storage: storage,
        tables_uri_prefix,
    }
}

/// Counters and a catalog over the production composition `Cached(Counting(S3))`.
///
/// The counting layer sits between the cache and the real S3 backend so a test
/// can tell whether a cache revalidation hit a 304 (`not_modified`) or had to
/// refetch the root body (`loaded`). `s3_storage` is the same raw backend the
/// cache wraps, exposed so a test can drive an out-of-band writer that bypasses
/// the cache.
pub(crate) struct CachedTestEnv {
    _minio: testcontainers::ContainerAsync<MinIO>,
    pub(crate) catalog: S3Catalog,
    pub(crate) counting: Arc<LoadCountingStorage>,
    pub(crate) s3_storage: Arc<S3CatalogStorage>,
    pub(crate) tables_uri_prefix: String,
}

/// Build the production S3 + cache composition over a fresh `MinIO` bucket.
///
/// This is the only harness that exercises the real `If-None-Match` round-trip
/// end-to-end: the cache revalidates against S3, and `object_store` maps a 304
/// into [`LoadOutcome::NotModified`]. The unit tests in `cached.rs` only simulate
/// that against an in-memory backend.
pub(crate) async fn make_cached_catalog() -> CachedTestEnv {
    let (minio, storage, tables_uri_prefix) = bootstrap_minio_storage().await;
    let counting = Arc::new(LoadCountingStorage::new(storage.clone()));
    let counting_dyn: Arc<dyn CatalogStorage> = counting.clone();
    let cached: Arc<dyn CatalogStorage> = Arc::new(CachedCatalogStorage::new(
        counting_dyn,
        S3CatalogConfig::default().metadata_cache_cap,
    ));
    let catalog = test_catalog(cached, FileIO::new_with_memory(), tables_uri_prefix.clone());

    CachedTestEnv {
        _minio: minio,
        catalog,
        counting,
        s3_storage: storage,
        tables_uri_prefix,
    }
}

/// Storage decorator that records how the inner backend answered each
/// `load_root`, letting cache tests assert a revalidation served a 304 rather
/// than refetching the root body. Pass-through for every other method.
pub(crate) struct LoadCountingStorage {
    inner: Arc<dyn CatalogStorage>,
    loaded: AtomicUsize,
    not_modified: AtomicUsize,
}

impl LoadCountingStorage {
    fn new(inner: Arc<dyn CatalogStorage>) -> Self {
        Self {
            inner,
            loaded: AtomicUsize::new(0),
            not_modified: AtomicUsize::new(0),
        }
    }

    /// Count of `load_root` calls the inner backend answered with a full body.
    pub(crate) fn loaded(&self) -> usize {
        self.loaded.load(Ordering::SeqCst)
    }

    /// Count of `load_root` calls the inner backend short-circuited with a 304.
    pub(crate) fn not_modified(&self) -> usize {
        self.not_modified.load(Ordering::SeqCst)
    }

    /// Zero both counters so a test can isolate the reads it cares about.
    pub(crate) fn reset(&self) {
        self.loaded.store(0, Ordering::SeqCst);
        self.not_modified.store(0, Ordering::SeqCst);
    }
}

#[async_trait]
impl CatalogStorage for LoadCountingStorage {
    async fn load_root(&self, known: Option<&Version>) -> Result<LoadOutcome> {
        let outcome = self.inner.load_root(known).await?;
        match &outcome {
            LoadOutcome::Loaded { .. } => {
                self.loaded.fetch_add(1, Ordering::SeqCst);
            }
            LoadOutcome::NotModified => {
                self.not_modified.fetch_add(1, Ordering::SeqCst);
            }
            LoadOutcome::Absent => {}
        }
        Ok(outcome)
    }

    async fn save_root(&self, root: Arc<CatalogRoot>, expected: &Version) -> Result<Version> {
        self.inner.save_root(root, expected).await
    }

    async fn read_table_metadata(&self, location: &str) -> Result<Arc<TableMetadata>> {
        self.inner.read_table_metadata(location).await
    }

    async fn write_table_metadata(&self, location: &str, metadata: &TableMetadata) -> Result<()> {
        self.inner.write_table_metadata(location, metadata).await
    }
}

pub(crate) fn make_in_memory_catalog() -> S3Catalog {
    make_in_memory_catalog_with_storage().0
}

pub(crate) fn make_in_memory_catalog_with_storage() -> (S3Catalog, Arc<InMemoryCatalogStorage>) {
    let storage = Arc::new(InMemoryCatalogStorage::new());
    let catalog = test_catalog(
        storage.clone(),
        FileIO::new_with_memory(),
        "memory://catalog/tables".to_string(),
    );
    (catalog, storage)
}

/// In-memory catalog wired through the `CachedCatalogStorage` decorator,
/// mirroring the production composition where every call goes through the
/// cache. Used by tests that must exercise the prod-path end-to-end rather
/// than the raw storage backend.
pub(crate) fn make_in_memory_catalog_cached() -> S3Catalog {
    make_in_memory_catalog_cached_with_storage().0
}

/// Cached production composition plus a handle to the inner in-memory storage.
///
/// Concurrency tests assert real catalog state through `inner` — reading the
/// source of truth directly, bypassing the cache layer under test.
pub(crate) fn make_in_memory_catalog_cached_with_storage() -> (S3Catalog, Arc<InMemoryCatalogStorage>) {
    let inner = Arc::new(InMemoryCatalogStorage::new());
    let cached: Arc<dyn CatalogStorage> = Arc::new(CachedCatalogStorage::new(
        inner.clone(),
        S3CatalogConfig::default().metadata_cache_cap,
    ));
    let catalog = test_catalog(cached, FileIO::new_with_memory(), "memory://catalog/tables".to_string());
    (catalog, inner)
}

#[allow(clippy::expect_used)]
pub(crate) fn test_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_identifier_field_ids(vec![1])
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .expect("schema")
}

#[allow(clippy::expect_used)]
pub(crate) async fn create_table(catalog: &S3Catalog, namespace: &NamespaceIdent, name: &str) -> Table {
    if !catalog.namespace_exists(namespace).await.expect("check namespace") {
        catalog
            .create_namespace(namespace, HashMap::new())
            .await
            .expect("create namespace");
    }
    catalog
        .create_table(
            namespace,
            TableCreation::builder().name(name.to_string()).schema(test_schema()).build(),
        )
        .await
        .expect("create table")
}

#[derive(Debug)]
struct CapturingCatalog {
    table: Table,
    captured_commit: Mutex<Option<TableCommit>>,
}

impl CapturingCatalog {
    fn new(table: &Table) -> Self {
        Self {
            table: table.clone(),
            captured_commit: Mutex::new(None),
        }
    }

    #[allow(clippy::expect_used)]
    fn take_commit(self) -> TableCommit {
        self.captured_commit
            .into_inner()
            .expect("captured commit lock")
            .expect("captured table commit")
    }
}

#[async_trait]
impl Catalog for CapturingCatalog {
    async fn list_namespaces(&self, _parent: Option<&NamespaceIdent>) -> IcebergResult<Vec<NamespaceIdent>> {
        unreachable!("not used in commit capture")
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        unreachable!("not used in commit capture")
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        unreachable!("not used in commit capture")
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> IcebergResult<bool> {
        unreachable!("not used in commit capture")
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        unreachable!("not used in commit capture")
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
        unreachable!("not used in commit capture")
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        unreachable!("not used in commit capture")
    }

    async fn create_table(&self, _namespace: &NamespaceIdent, _creation: TableCreation) -> IcebergResult<Table> {
        unreachable!("not used in commit capture")
    }

    async fn load_table(&self, _table: &TableIdent) -> IcebergResult<Table> {
        Ok(self.table.clone())
    }

    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        unreachable!("not used in commit capture")
    }

    async fn table_exists(&self, _table: &TableIdent) -> IcebergResult<bool> {
        unreachable!("not used in commit capture")
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        unreachable!("not used in commit capture")
    }

    async fn register_table(&self, _table: &TableIdent, _metadata_location: String) -> IcebergResult<Table> {
        unreachable!("not used in commit capture")
    }

    #[allow(clippy::expect_used)]
    async fn update_table(&self, commit: TableCommit) -> IcebergResult<Table> {
        *self.captured_commit.lock().expect("captured commit lock") = Some(commit);
        Ok(self.table.clone())
    }
}

#[allow(clippy::expect_used)]
pub(crate) async fn update_request(table: &Table, key: &str, value: &str) -> TableCommit {
    let tx = Transaction::new(table);
    let tx = tx
        .update_table_properties()
        .set(key.to_string(), value.to_string())
        .apply(tx)
        .expect("build update transaction");
    let capture = CapturingCatalog::new(table);
    tx.commit(&capture).await.expect("commit capture");
    capture.take_commit()
}

/// Metadata version parsed from a `.../metadata/{NNNNN}-{uuid}.metadata.json`
/// location. Test-only: keeps tests off the private `model::MetadataVersion`.
pub(crate) fn metadata_version_in(location: &str) -> u32 {
    location
        .rsplit("/metadata/")
        .next()
        .and_then(|tail| tail.split('-').next())
        .and_then(|version| version.parse().ok())
        .unwrap_or_else(|| panic!("no metadata version in {location}"))
}
