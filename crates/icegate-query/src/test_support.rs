//! Test-only doubles and bootstrap shared by the crate's inline tests.
//!
//! Compiled only under `cfg(test)`; nothing here is part of the crate's API.

use std::collections::HashMap;
use std::error::Error;
use std::future::pending;
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use chrono::{DateTime, Utc};
use datafusion::arrow::array::{
    ArrayRef, MapBuilder, MapFieldNames, StringArray, StringBuilder, TimestampMicrosecondArray, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
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
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::engine::{QueryEngine, QueryEngineConfig};

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
