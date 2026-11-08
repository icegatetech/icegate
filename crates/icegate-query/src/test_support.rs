//! Test-only doubles and bootstrap shared by the crate's inline tests.
//!
//! Compiled only under `cfg(test)`; nothing here is part of the crate's API.

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::future::pending;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{
    ArrayRef, MapBuilder, MapFieldNames, StringArray, StringBuilder, TimestampMicrosecondArray, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::{StreamExt as _, TryStreamExt as _};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Namespace, NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation, TableIdent};
use icegate_common::iceberg_write::{CommonRecordBatchStream, WriteConfig, write_record_batches_to_parquet};
use icegate_common::parquet_encoding::{LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS};
use icegate_common::schema::{
    self, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES, COL_OBSERVED_TIMESTAMP, COL_RESOURCE_ATTRIBUTES,
    COL_SCOPE_ATTRIBUTES, COL_TENANT_ID, COL_TIMESTAMP,
};
use icegate_common::{
    CatalogBackend, CatalogBuilder, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, LOGS_TABLE, MemoryPressure,
    MemoryPressureConfig, MemoryPressureSampler, SPANS_TABLE, TenantId, UsageReader, icegate_table_ident,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt as _,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::engine::{QueryEngine, QueryEngineConfig};
use crate::infra::metrics::QueryMetrics;
use crate::opensearch::SystemObjectStore;
use crate::opensearch::catalog::{CatalogSettings, IndexCatalog, IndexEntry, TenantSession};
use crate::opensearch::system_index::{SystemIndexLimits, SystemIndexStore};
use crate::test_rows;
/// The WAL offset every fixture commit records; a fixture writes the WAL segments of a table
/// with a commit from `FIXTURE_COMMITTED_WAL_OFFSET + 1` on.
pub use crate::test_rows::COMMITTED_WAL_OFFSET as FIXTURE_COMMITTED_WAL_OFFSET;
/// The cells and rows a fixture writes, shared with the `OpenSearch` integration binary.
pub use crate::test_rows::{Cell as TestCell, Row as TestRow};

/// `UsageReader` whose usage sits above the default high watermark;
/// [`build_pressured_memory`] asserts that it does.
struct NearlyFullMemoryReader;

impl UsageReader for NearlyFullMemoryReader {
    fn limit_bytes(&self) -> u64 {
        100
    }

    fn read_working_set_bytes(&self) -> icegate_common::error::Result<u64> {
        Ok(95)
    }
}

/// Build a [`MemoryPressure`] handle that reports pressure under the default
/// [`MemoryPressureConfig`], without touching a real cgroup.
///
/// Asserts the handle is under pressure before returning it, so a watermark
/// default moving past the reader's usage fails here rather than letting the
/// bypass cases — a probe expected to answer under pressure — pass with no
/// pressure to bypass.
pub fn build_pressured_memory() -> MemoryPressure {
    let sampler =
        MemoryPressureSampler::with_reader(&MemoryPressureConfig::default(), Arc::new(NearlyFullMemoryReader));
    let pressure = sampler.handle();
    sampler.sample_once().expect("sample the stub reader");
    assert!(
        pressure.is_under_pressure(),
        "the stub reader's usage must cross the default high watermark"
    );
    pressure
}

/// Catalog whose every call never completes.
///
/// Models the failure the query deadline exists for — a catalog that accepted
/// the request and then went silent — without a sleep whose duration the test
/// would have to guess.
#[derive(Debug)]
pub struct StallingCatalog;

#[async_trait]
impl Catalog for StallingCatalog {
    async fn list_namespaces(&self, _parent: Option<&NamespaceIdent>) -> IcebergResult<Vec<NamespaceIdent>> {
        pending().await
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<Namespace> {
        pending().await
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<Namespace> {
        pending().await
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> IcebergResult<bool> {
        pending().await
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        pending().await
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
        pending().await
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        pending().await
    }

    async fn create_table(&self, _namespace: &NamespaceIdent, _creation: TableCreation) -> IcebergResult<Table> {
        pending().await
    }

    async fn load_table(&self, _table: &TableIdent) -> IcebergResult<Table> {
        pending().await
    }

    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        pending().await
    }

    async fn purge_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        pending().await
    }

    async fn table_exists(&self, _table: &TableIdent) -> IcebergResult<bool> {
        pending().await
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        pending().await
    }

    async fn register_table(&self, _table: &TableIdent, _metadata_location: String) -> IcebergResult<Table> {
        pending().await
    }

    async fn update_table(&self, _commit: TableCommit) -> IcebergResult<Table> {
        pending().await
    }
}

/// Build an engine over [`StallingCatalog`] whose queries may run for
/// `max_query_duration_secs` before the transports cut them off.
pub fn build_stalling_engine(max_query_duration_secs: u64) -> Arc<QueryEngine> {
    let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let wal_reader =
        Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(&wal_store), 8192).expect("build WAL reader"));
    let config = QueryEngineConfig {
        max_query_duration_secs,
        ..QueryEngineConfig::default()
    };
    Arc::new(QueryEngine::new(
        Arc::new(StallingCatalog),
        config,
        wal_store,
        wal_reader,
    ))
}

/// Serve `router` on an OS-assigned local port and return its base URL together
/// with the server task, so a test drives it over real HTTP rather than through
/// `tower::ServiceExt::oneshot`.
pub async fn serve_router(router: Router) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind test listener");
    let addr = listener.local_addr().expect("listener address");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    (format!("http://{addr}"), handle)
}

/// An engine over an in-memory catalog holding `logs` with its production schema, partition
/// spec and sort order, so tenants land in separate partitions as in production.
pub struct LogsWarehouse {
    /// The engine sessions are opened on.
    pub engine: Arc<QueryEngine>,
    /// The catalog rows are committed through; the engine reads the same one.
    pub catalog: Arc<dyn Catalog>,
    /// Held for its `Drop`, which removes the warehouse directory.
    _directory: tempfile::TempDir,
}

/// An in-memory catalog over a temporary warehouse, with the IceGate namespace created and no
/// table in it. The directory removes the warehouse on drop, so the caller holds it while the
/// catalog is used.
pub async fn build_icegate_catalog() -> (tempfile::TempDir, Arc<dyn Catalog>) {
    let warehouse = tempfile::tempdir().expect("a warehouse directory");
    let catalog = CatalogBuilder::from_config(
        &CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: warehouse.path().to_str().expect("a UTF-8 path").to_string(),
            properties: HashMap::new(),
            cache: None,
        },
        &IoHandle::noop(),
        CancellationToken::new(),
    )
    .await
    .expect("an in-memory catalog");
    catalog
        .create_namespace(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("the IceGate namespace");
    (warehouse, catalog)
}

/// The tenant `id`; a test names only valid tenants.
pub fn build_tenant(id: &str) -> TenantId {
    TenantId::new(id).expect("a valid tenant")
}

/// Build a [`LogsWarehouse`] with an empty `logs` table and an empty in-memory WAL.
pub async fn build_logs_warehouse() -> LogsWarehouse {
    let (directory, catalog) = build_icegate_catalog().await;
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    let logs_schema = schema::logs_schema().expect("the logs schema");
    let creation = TableCreation::builder()
        .name(LOGS_TABLE.to_string())
        .partition_spec(schema::logs_partition_spec(&logs_schema).expect("the logs partition spec"))
        .sort_order(schema::logs_sort_order(&logs_schema).expect("the logs sort order"))
        .schema(logs_schema)
        .build();
    catalog.create_table(&namespace, creation).await.expect("the logs table");
    let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let wal_reader =
        Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(&wal_store), 8192).expect("a WAL reader"));
    let engine = Arc::new(QueryEngine::new(
        Arc::clone(&catalog),
        QueryEngineConfig::default(),
        wal_store,
        wal_reader,
    ));
    LogsWarehouse {
        engine,
        catalog,
        _directory: directory,
    }
}

/// One `logs` row for component tests. Every other column is null or empty, and the observed and
/// ingested timestamps equal `timestamp`.
pub struct LogAttributesRow<'a> {
    /// The tenant the row belongs to.
    pub tenant_id: &'a str,
    /// Event time as an RFC 3339 instant.
    pub timestamp: &'a str,
    /// `log_attributes` entries, in the order given.
    pub log_attributes: &'a [(&'a str, &'a str)],
}

/// Write `rows` into `logs` through the production Parquet writer with the production `logs`
/// bloom-filter columns and column encodings, and commit them as one append (one new snapshot).
///
/// `tests/opensearch/data.rs` copies this writer: integration tests cannot reach `cfg(test)` items.
pub async fn commit_log_attributes_rows(catalog: &Arc<dyn Catalog>, rows: &[LogAttributesRow<'_>]) {
    let table = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("the logs table");
    let arrow_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).expect("the logs Arrow schema"),
    );
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| build_log_attributes_column(field, rows))
        .collect::<Result<Vec<ArrayRef>, Box<dyn Error>>>()
        .expect("the logs columns");
    let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), columns).expect("a logs batch");
    let batches: CommonRecordBatchStream = Box::pin(futures::stream::iter([Ok(batch)]));
    let write_config = WriteConfig {
        row_group_size: 8192,
        data_page_size_limit_bytes: 1024 * 1024,
        max_file_size_bytes: 128 * 1024 * 1024,
        bloom_filter_columns: LOGS_BLOOM_COLUMNS,
        column_encodings: LOGS_COLUMN_ENCODINGS,
    };
    let written_files =
        write_record_batches_to_parquet(table.clone(), write_config, batches, &CancellationToken::new())
            .await
            .expect("a Parquet write");
    let append_action = Transaction::new(&table).fast_append().add_data_files(written_files.data_files);
    append_action
        .apply(Transaction::new(&table))
        .expect("an append")
        .commit(&**catalog)
        .await
        .expect("a committed append");
}

/// The column `field` of `logs` for `rows`. The error is a column whose type the fixture does
/// not expect: the `logs` schema changed under it.
fn build_log_attributes_column(field: &Field, rows: &[LogAttributesRow<'_>]) -> Result<ArrayRef, Box<dyn Error>> {
    let column: ArrayRef = match field.name().as_str() {
        COL_TENANT_ID => Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.tenant_id))),
        COL_TIMESTAMP | COL_OBSERVED_TIMESTAMP | COL_INGESTED_TIMESTAMP => {
            let DataType::Timestamp(TimeUnit::Microsecond, timezone) = field.data_type() else {
                return Err(format!("{} is not a microsecond timestamp", field.name()).into());
            };
            let micros = rows
                .iter()
                .map(|row| Ok(row.timestamp.parse::<DateTime<Utc>>()?.timestamp_micros()))
                .collect::<Result<Vec<i64>, chrono::ParseError>>()?;
            Arc::new(TimestampMicrosecondArray::from(micros).with_timezone_opt(timezone.clone()))
        }
        COL_LOG_ATTRIBUTES => build_string_map_column(field, rows.iter().map(|row| row.log_attributes))?,
        COL_RESOURCE_ATTRIBUTES | COL_SCOPE_ATTRIBUTES => build_string_map_column(field, rows.iter().map(|_| &[][..]))?,
        _ => new_null_array(field.data_type(), rows.len()),
    };
    Ok(column)
}

/// A MAP column built with the entry, key and value fields of `field` itself, so the Iceberg
/// field-id metadata `RecordBatch::try_new` compares is the table's own.
fn build_string_map_column<'a>(
    field: &Field,
    rows: impl Iterator<Item = &'a [(&'a str, &'a str)]>,
) -> Result<ArrayRef, Box<dyn Error>> {
    let DataType::Map(entries, _) = field.data_type() else {
        return Err(format!("{} is not a MAP column", field.name()).into());
    };
    let DataType::Struct(entry_fields) = entries.data_type() else {
        return Err(format!("the entries of {} are not a struct", field.name()).into());
    };
    let (Some(key_field), Some(value_field)) = (entry_fields.first(), entry_fields.get(1)) else {
        return Err(format!("the entries of {} lack a key or a value", field.name()).into());
    };
    let names = MapFieldNames {
        entry: entries.name().clone(),
        key: key_field.name().clone(),
        value: value_field.name().clone(),
    };
    let mut builder = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new())
        .with_keys_field(Arc::clone(key_field))
        .with_values_field(Arc::clone(value_field));
    for pairs in rows {
        for (key, value) in pairs {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true)?;
    }
    Ok(Arc::new(builder.finish()))
}

/// Build an in-memory Iceberg catalog over `memory://warehouse` with no namespace in it. Its
/// storage keys any path string, `s3://` included, so a table created in it at an S3 location
/// touches no object store.
pub async fn build_catalog_without_namespace() -> Arc<dyn Catalog> {
    CatalogBuilder::from_config(
        &CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: "memory://warehouse".to_string(),
            properties: HashMap::new(),
            cache: None,
        },
        &IoHandle::noop(),
        CancellationToken::new(),
    )
    .await
    .expect("an in-memory catalog")
}

/// Build an in-memory Iceberg catalog holding the IceGate namespace and one `logs` table created
/// at `table_location` with its production schema, partition spec and sort order, for the checks
/// that compare locations against the catalog's tables.
pub async fn build_catalog_with_logs_table_at(table_location: &str) -> Arc<dyn Catalog> {
    let catalog = build_catalog_without_namespace().await;
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("the IceGate namespace");
    let logs_schema = schema::logs_schema().expect("the logs schema");
    let creation = TableCreation::builder()
        .name(LOGS_TABLE.to_string())
        .location(table_location.to_string())
        .partition_spec(schema::logs_partition_spec(&logs_schema).expect("the logs partition spec"))
        .sort_order(schema::logs_sort_order(&logs_schema).expect("the logs sort order"))
        .schema(logs_schema)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("the logs table at the stated location");
    catalog
}

/// Build the catalog of [`build_catalog_with_logs_table_at`] with a `spans` table beside the `logs`
/// one, created at `spans_location` with its production schema, partition spec and sort order, for
/// the checks that must compare against every table the catalog lists.
pub async fn build_catalog_with_logs_and_spans_tables_at(
    logs_location: &str,
    spans_location: &str,
) -> Arc<dyn Catalog> {
    let catalog = build_catalog_with_logs_table_at(logs_location).await;
    let spans_schema = schema::spans_schema().expect("the spans schema");
    let creation = TableCreation::builder()
        .name(SPANS_TABLE.to_string())
        .location(spans_location.to_string())
        .partition_spec(schema::spans_partition_spec(&spans_schema).expect("the spans partition spec"))
        .sort_order(schema::spans_sort_order(&spans_schema).expect("the spans sort order"))
        .schema(spans_schema)
        .build();
    catalog
        .create_table(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), creation)
        .await
        .expect("the spans table at the stated location");
    catalog
}

/// A system-index store over an empty `InMemory` object store, for the router and catalog tests
/// that need an `OpenSearchState` but never write a system index.
pub fn build_in_memory_system_index_store() -> Arc<SystemIndexStore> {
    let system_object_store = SystemObjectStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        "s3://crate-tests/opensearch-system",
    )
    .expect("an S3 location");
    Arc::new(SystemIndexStore::new(
        system_object_store,
        SystemIndexLimits {
            max_snapshot_bytes: 1 << 20,
            cache_max_bytes: 1 << 20,
            write_deadline: Duration::from_secs(30),
            max_scroll_lifetime: Duration::from_mins(15),
        },
    ))
}

/// How many times a test yields to a background garbage-collection pass: far more than one pass
/// over a handful of objects takes, each of whose requests [`ClockedObjectStore`] yields once.
pub const PASS_YIELD_BUDGET: usize = 1_000;

/// Yields as often as a started pass is given to finish, so a pass started before this call has
/// done its work after it. On tokio's paused clock no timer fires meanwhile.
pub async fn yield_to_background_passes() {
    for _ in 0..PASS_YIELD_BUDGET {
        tokio::task::yield_now().await;
    }
}

/// Yields until `condition` holds, checking it before every yield, and panics with
/// `failure_message` once [`PASS_YIELD_BUDGET`] checks have failed.
pub async fn wait_for_condition<F, Fut>(mut condition: F, failure_message: &str)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    for _ in 0..PASS_YIELD_BUDGET {
        if condition().await {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("{failure_message}");
}

/// An object store over `InMemory` whose `Last-Modified` times come from a test clock.
///
/// Every put and copy is stamped with the clock's time when it lands, and every GET, HEAD and
/// listing reports that stamp, so tests state the store's clock instead of sleeping. Every put and
/// GET yields to the scheduler first, so operations of two futures joined in one task interleave as
/// those of two replicas do. It also refuses writes, deletes and listings of chosen paths, can
/// answer a chosen GET or delete as not-found after deleting the object, which is what a read or a
/// delete racing another deleter observes, and can write chosen objects just before a chosen GET,
/// which is what a read racing another writer observes.
#[derive(Debug)]
pub struct ClockedObjectStore {
    /// Holds the objects.
    inner: InMemory,
    /// The store's clock.
    now: Mutex<DateTime<Utc>>,
    /// The `Last-Modified` of every path written, shared with the listing streams.
    stamps: Arc<Mutex<HashMap<Path, DateTime<Utc>>>>,
    /// Paths whose writes are refused.
    paths_refused_on_put: Mutex<HashSet<Path>>,
    /// Paths whose next GET or HEAD answers not-found.
    paths_missing_on_get: Mutex<HashSet<Path>>,
    /// Paths whose next delete answers not-found, shared with the delete streams.
    paths_missing_on_delete: Arc<Mutex<HashSet<Path>>>,
    /// Paths whose deletes are refused, shared with the delete streams.
    paths_refused_on_delete: Arc<Mutex<HashSet<Path>>>,
    /// Prefixes whose listings are refused.
    prefixes_refused_on_list: Mutex<HashSet<Path>>,
    /// The writes the next GET or HEAD of a path makes before it is served, in order.
    puts_on_next_get: Mutex<HashMap<Path, Vec<(Path, PutPayload)>>>,
}

impl ClockedObjectStore {
    /// A store whose clock reads `now` until [`Self::set_time`] moves it.
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            inner: InMemory::new(),
            now: Mutex::new(now),
            stamps: Arc::new(Mutex::new(HashMap::new())),
            paths_refused_on_put: Mutex::new(HashSet::new()),
            paths_missing_on_get: Mutex::new(HashSet::new()),
            paths_missing_on_delete: Arc::new(Mutex::new(HashSet::new())),
            paths_refused_on_delete: Arc::new(Mutex::new(HashSet::new())),
            prefixes_refused_on_list: Mutex::new(HashSet::new()),
            puts_on_next_get: Mutex::new(HashMap::new()),
        }
    }

    /// Set the store's clock; later writes carry `now` as their `Last-Modified`.
    pub fn set_time(&self, now: DateTime<Utc>) {
        *self.now.lock().expect("clock") = now;
    }

    /// Refuse every later write to `path` with a transient error.
    pub fn refuse_puts_to(&self, path: Path) {
        self.paths_refused_on_put.lock().expect("refused puts").insert(path);
    }

    /// Refuse every later delete of `path` with a transient error, keeping the object.
    pub fn refuse_deletes_of(&self, path: Path) {
        self.paths_refused_on_delete.lock().expect("refused deletes").insert(path);
    }

    /// Refuse every later listing of exactly `prefix` with a transient error.
    pub fn refuse_listings_of(&self, prefix: Path) {
        self.prefixes_refused_on_list.lock().expect("refused listings").insert(prefix);
    }

    /// Answer the next GET or HEAD of `path` with not-found, after deleting the object.
    pub fn report_next_get_as_missing(&self, path: Path) {
        self.paths_missing_on_get.lock().expect("missing reads").insert(path);
    }

    /// Answer the next delete of `path` with not-found, after deleting the object.
    pub fn report_next_delete_as_missing(&self, path: Path) {
        self.paths_missing_on_delete.lock().expect("missing deletes").insert(path);
    }

    /// Write `payload` to `location`, stamped with the clock's time, just before the next GET or
    /// HEAD of `trigger` is served. Several calls for one trigger write in call order.
    pub fn put_on_next_get(&self, trigger: Path, location: Path, payload: PutPayload) {
        self.puts_on_next_get
            .lock()
            .expect("puts on get")
            .entry(trigger)
            .or_default()
            .push((location, payload));
    }

    /// Record the clock's time as the `Last-Modified` of `path`.
    fn record_stamp(&self, path: &Path) {
        let now = *self.now.lock().expect("clock");
        self.stamps.lock().expect("stamps").insert(path.clone(), now);
    }

    /// Whether listings of exactly `prefix` are refused.
    fn is_listing_refused(&self, prefix: &Path) -> bool {
        self.prefixes_refused_on_list.lock().expect("refused listings").contains(prefix)
    }
}

impl fmt::Display for ClockedObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ClockedObjectStore({})", self.inner)
    }
}

/// Replace `metadata.last_modified` with the stamp the double recorded for its path.
fn apply_stamp(stamps: &Mutex<HashMap<Path, DateTime<Utc>>>, mut metadata: ObjectMeta) -> ObjectMeta {
    // Read into a local, so the lock is released before the metadata changes.
    let stamp = stamps.lock().expect("stamps").get(&metadata.location).copied();
    if let Some(stamp) = stamp {
        metadata.last_modified = stamp;
    }
    metadata
}

/// The transient error the double answers a refused `request` of `path` with.
fn build_refusal(request: &str, path: &Path) -> object_store::Error {
    object_store::Error::Generic {
        store: "ClockedObjectStore",
        source: format!("{request} {path} are refused").into(),
    }
}

#[async_trait]
impl ObjectStore for ClockedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        tokio::task::yield_now().await;
        if self.paths_refused_on_put.lock().expect("refused puts").contains(location) {
            return Err(build_refusal("writes to", location));
        }
        let result = self.inner.put_opts(location, payload, options).await?;
        self.record_stamp(location);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        tokio::task::yield_now().await;
        let triggered_puts = self.puts_on_next_get.lock().expect("puts on get").remove(location);
        for (triggered_location, payload) in triggered_puts.into_iter().flatten() {
            self.inner.put(&triggered_location, payload).await?;
            self.record_stamp(&triggered_location);
        }
        let is_reported_missing = self.paths_missing_on_get.lock().expect("missing reads").remove(location);
        if is_reported_missing {
            self.inner.delete(location).await?;
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: "deleted by another deleter".into(),
            });
        }
        let mut result = self.inner.get_opts(location, options).await?;
        result.meta = apply_stamp(&self.stamps, result.meta);
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, ObjectStoreResult<Path>>,
    ) -> BoxStream<'static, ObjectStoreResult<Path>> {
        let paths_refused_on_delete = Arc::clone(&self.paths_refused_on_delete);
        let paths_missing_on_delete = Arc::clone(&self.paths_missing_on_delete);
        // A refused path reaches `InMemory` as an error item, which it passes through undeleted.
        let admitted_locations = locations
            .map(move |location| {
                let path = location?;
                if paths_refused_on_delete.lock().expect("refused deletes").contains(&path) {
                    return Err(build_refusal("deletes of", &path));
                }
                Ok(path)
            })
            .boxed();
        self.inner
            .delete_stream(admitted_locations)
            .map(move |delete_outcome| {
                let path = delete_outcome?;
                if paths_missing_on_delete.lock().expect("missing deletes").remove(&path) {
                    return Err(object_store::Error::NotFound {
                        path: path.to_string(),
                        source: "deleted by another deleter".into(),
                    });
                }
                Ok(path)
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        if let Some(refused_prefix) = prefix.filter(|prefix| self.is_listing_refused(prefix)) {
            return futures::stream::iter([Err(build_refusal("listings of", refused_prefix))]).boxed();
        }
        let stamps = Arc::clone(&self.stamps);
        self.inner
            .list(prefix)
            .map_ok(move |metadata| apply_stamp(&stamps, metadata))
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        let mut result = self.inner.list_with_delimiter(prefix).await?;
        result.objects = result
            .objects
            .into_iter()
            .map(|metadata| apply_stamp(&self.stamps, metadata))
            .collect();
        Ok(result)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> ObjectStoreResult<()> {
        self.inner.copy_opts(from, to, options).await?;
        self.record_stamp(to);
        Ok(())
    }
}

/// An in-memory warehouse with every production table, an in-memory WAL and an `OpenSearch`
/// catalog over both, for crate-internal component tests of the search path.
pub struct SearchFixture {
    /// The Iceberg catalog holding the tables.
    pub catalog: Arc<dyn Catalog>,
    /// The WAL object store the engine reads hot segments from.
    pub wal_store: Arc<dyn ObjectStore>,
    /// The engine, WAL reading enabled.
    pub engine: Arc<QueryEngine>,
    /// The catalog sessions are opened on.
    index_catalog: IndexCatalog,
    /// Held for its `Drop`, which removes the warehouse directory.
    _warehouse: tempfile::TempDir,
}

impl SearchFixture {
    /// Create the warehouse, every table [`test_rows::create_production_tables`] creates and the
    /// engine.
    pub async fn new() -> Self {
        let (warehouse, catalog) = build_icegate_catalog().await;
        test_rows::create_production_tables(&catalog)
            .await
            .expect("the production tables");

        let wal_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_reader =
            Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(&wal_store), 8192).expect("WAL reader"));
        let engine = Arc::new(QueryEngine::new(
            Arc::clone(&catalog),
            QueryEngineConfig {
                wal_query_enabled: true,
                ..QueryEngineConfig::default()
            },
            Arc::clone(&wal_store),
            wal_reader,
        ));
        let index_catalog = IndexCatalog::new(
            Arc::clone(&engine),
            Arc::new(QueryMetrics::new_disabled()),
            build_in_memory_system_index_store(),
            CatalogSettings {
                field_discovery_window: Duration::from_hours(24),
                max_query_memory_bytes: 1_073_741_824,
            },
        );
        Self {
            catalog,
            wal_store,
            engine,
            index_catalog,
            _warehouse: warehouse,
        }
    }

    /// The Arrow schema of `table`'s current Iceberg schema, field ids included.
    pub async fn read_arrow_schema(&self, table: &str) -> SchemaRef {
        test_rows::read_arrow_schema(&self.catalog, table)
            .await
            .expect("the table's Arrow schema")
    }

    /// Write `rows` into `table` as one append; see [`test_rows::commit_rows`].
    pub async fn commit_rows(&self, table: &str, rows: &[TestRow<'_>]) {
        test_rows::commit_rows(&self.catalog, table, rows).await.expect("commit rows");
    }

    /// Write `batch`, built over [`Self::read_arrow_schema`] of `table`, as one append; see
    /// [`test_rows::commit_batch`].
    pub async fn commit_batch(&self, table: &str, batch: RecordBatch) {
        test_rows::commit_batch(&self.catalog, table, batch)
            .await
            .expect("commit batch");
    }

    /// Write `rows` as the WAL segment `offset` of `table`'s topic; see
    /// [`test_rows::write_wal_segment`].
    pub async fn write_wal_segment(&self, table: &str, offset: u64, rows: &[TestRow<'_>]) {
        test_rows::write_wal_segment(&self.wal_store, &self.catalog, table, offset, rows)
            .await
            .expect("write segment");
    }

    /// Open a session scoped to `tenant`. The first session builds the engine's catalog
    /// provider and later sessions reuse it, so a test writes all its rows before it opens a
    /// session.
    pub async fn open_session(&self, tenant: &str) -> TenantSession {
        self.index_catalog
            .open_session(&build_tenant(tenant))
            .await
            .expect("open session")
    }
}

/// The data index `name` of `session`.
pub fn find_index_entry<'a>(session: &'a TenantSession, name: &str) -> &'a IndexEntry {
    session
        .indices()
        .entries()
        .iter()
        .find(|entry| entry.name() == name)
        .unwrap_or_else(|| panic!("index `{name}` is not listed"))
}
