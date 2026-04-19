//! Common test harness for Tempo integration tests.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::cast_possible_truncation,
    clippy::too_many_lines
)]

use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{
            ArrayRef, Int32Array, Int64Array, ListArray, MapBuilder, MapFieldNames, RecordBatch, StringArray,
            StringBuilder, TimestampMicrosecondArray,
        },
        buffer::OffsetBuffer,
        datatypes::{DataType, Field},
    },
    parquet::file::properties::WriterProperties,
};
use iceberg::{
    Catalog,
    spec::DataFileFormat,
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        IcebergWriter, IcebergWriterBuilder,
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
    },
};
use icegate_common::{
    CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, SPANS_TABLE, catalog::CatalogBuilder, schema,
};
use icegate_query::{
    engine::{QueryEngine, QueryEngineConfig},
    tempo::TempoConfig,
};
use reqwest::Client;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

/// Test server running a Tempo HTTP API on an ephemeral port.
pub struct TestServer {
    pub client: Client,
    pub base_url: String,
    pub cancel_token: CancellationToken,
    server_handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    /// Start a Tempo test server backed by a fresh in-memory iceberg
    /// catalog with a local temp-dir warehouse. Returns the server handle
    /// and the catalog Arc for use by table-write helpers.
    pub async fn start() -> Result<(Self, Arc<dyn Catalog>), Box<dyn std::error::Error>> {
        let warehouse_path = tempfile::tempdir()?;
        let warehouse_str = warehouse_path.path().to_str().unwrap().to_string();

        let catalog_config = CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: warehouse_str.clone(),
            properties: std::collections::HashMap::default(),
            cache: None,
        };

        // Pick a random-but-free ephemeral port. We bind via TempoConfig
        // on 0.0.0.0:0 effectively by using the OS assigned port. TempoConfig
        // has no port_tx plumbing, so instead we probe a free port first.
        let probe = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let actual_port = probe.local_addr()?.port();
        drop(probe);

        let tempo_config = TempoConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: actual_port,
        };

        let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop()).await?;

        let namespace_ident = iceberg::NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
        if !catalog.namespace_exists(&namespace_ident).await? {
            catalog
                .create_namespace(&namespace_ident, std::collections::HashMap::new())
                .await?;
        }

        let spans_schema = schema::spans_schema()?;
        let table_creation = iceberg::TableCreation::builder()
            .name(SPANS_TABLE.to_string())
            .schema(spans_schema)
            .build();
        let _ = catalog.create_table(&namespace_ident, table_creation).await?;

        // Tempo server doesn't use WAL — point it at an empty in-memory one.
        let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let wal_reader = Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(&wal_store), 8192).unwrap());
        let engine_config = QueryEngineConfig::default();
        let query_engine = Arc::new(QueryEngine::new(
            Arc::clone(&catalog),
            engine_config,
            wal_store,
            wal_reader,
        ));
        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        let server_engine = Arc::clone(&query_engine);

        let server_handle = tokio::spawn(async move {
            icegate_query::tempo::run(server_engine, tempo_config, cancel_token_clone)
                .await
                .unwrap();
        });

        // Wait for the server to be reachable.
        let client = Client::new();
        let base_url = format!("http://127.0.0.1:{actual_port}");
        for _ in 0..50 {
            if client.get(format!("{base_url}/ready")).send().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Box::leak(Box::new(warehouse_path));

        Ok((
            Self {
                client,
                base_url,
                cancel_token,
                server_handle,
            },
            catalog,
        ))
    }

    /// Shut the server down and wait briefly for the task to exit.
    pub async fn shutdown(self) {
        self.cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.server_handle).await;
    }
}

/// Write a tiny batch of span records for a given tenant. Each call writes
/// three spans: two for `service_name="frontend"` and one for
/// `service_name="backend"`, with a handful of attribute keys including a
/// well-known resource-prefix one (`k8s.namespace.name`) and a span-scope
/// one (`http.method`).
pub async fn write_test_spans(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    tenant_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    // Index columns by name so we don't depend on the fixed field order.
    let col_idx = |name: &str| -> usize { arrow_schema.index_of(name).unwrap() };

    // Build each column by name; we assemble them in schema order further
    // below via the `by_name` map so row order survives any reordering of
    // the iceberg schema fields.
    let tenant_id_arr: ArrayRef = Arc::new(StringArray::from(vec![tenant_id, tenant_id, tenant_id]));
    let cloud_account_id: ArrayRef = Arc::new(StringArray::from(vec![Some("acc-1"), Some("acc-1"), Some("acc-1")]));
    let service_name: ArrayRef = Arc::new(StringArray::from(vec![
        Some("frontend"),
        Some("frontend"),
        Some("backend"),
    ]));
    let trace_id: ArrayRef = Arc::new(StringArray::from(vec![
        "0102030405060708090a0b0c0d0e0f10",
        "1112131415161718191a1b1c1d1e1f20",
        "2122232425262728292a2b2c2d2e2f30",
    ]));
    let span_id: ArrayRef = Arc::new(StringArray::from(vec![
        "0102030405060708",
        "1112131415161718",
        "2122232425262728",
    ]));
    let parent_span_id: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, None, None]));
    let timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros,
        now_micros - 1000,
        now_micros - 2000,
    ]));
    let end_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros + 1_000_000,
        now_micros - 1000 + 500_000,
        now_micros - 2000 + 250_000,
    ]));
    let ingested_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros, now_micros, now_micros,
    ]));
    let duration_micros: ArrayRef = Arc::new(Int64Array::from(vec![1_000_000i64, 500_000, 250_000]));
    let trace_state: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, None, None]));
    let name: ArrayRef = Arc::new(StringArray::from(vec![
        "GET /api/health",
        "GET /api/users",
        "query users",
    ]));
    let kind: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(2), Some(3)]));
    let status_code: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(0), Some(1)]));
    let status_message: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>, None, Some("db timeout")]));

    // Build resource_attributes map (OTel Resource — e.g. k8s.namespace.name).
    let resource_attributes = build_attribute_map(
        &arrow_schema,
        col_idx("resource_attributes"),
        &[
            &[("k8s.namespace.name", "prod")],
            &[("k8s.namespace.name", "prod")],
            &[("k8s.namespace.name", "prod")],
        ],
    )?;
    // Build span_attributes map (OTel Span attrs — e.g. http.method, db.statement).
    let span_attributes = build_attribute_map(
        &arrow_schema,
        col_idx("span_attributes"),
        &[
            &[("http.method", "GET"), ("http.url", "/api/health")],
            &[("http.method", "GET"), ("http.url", "/api/users")],
            &[("db.statement", "SELECT * FROM users")],
        ],
    )?;

    let flags: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(0), Some(0)]));
    let dropped_attributes_count: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(0), Some(0)]));
    let dropped_events_count: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(0), Some(0)]));
    let dropped_links_count: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(0), Some(0)]));

    // Empty events/links lists — schema allows nullable list, but arrow
    // still wants a list array of length 3 with all-null / empty entries.
    let events: ArrayRef = empty_list_array_for_field(arrow_schema.field(col_idx("events")), 3);
    let links: ArrayRef = empty_list_array_for_field(arrow_schema.field(col_idx("links")), 3);

    // Assemble columns in schema order.
    let mut by_name: std::collections::HashMap<&str, ArrayRef> = std::collections::HashMap::new();
    by_name.insert("tenant_id", tenant_id_arr);
    by_name.insert("cloud_account_id", cloud_account_id);
    by_name.insert("service_name", service_name);
    by_name.insert("trace_id", trace_id);
    by_name.insert("span_id", span_id);
    by_name.insert("parent_span_id", parent_span_id);
    by_name.insert("timestamp", timestamp);
    by_name.insert("end_timestamp", end_timestamp);
    by_name.insert("ingested_timestamp", ingested_timestamp);
    by_name.insert("duration_micros", duration_micros);
    by_name.insert("trace_state", trace_state);
    by_name.insert("name", name);
    by_name.insert("kind", kind);
    by_name.insert("status_code", status_code);
    by_name.insert("status_message", status_message);
    by_name.insert("resource_attributes", resource_attributes);
    by_name.insert("span_attributes", span_attributes);
    by_name.insert("flags", flags);
    by_name.insert("dropped_attributes_count", dropped_attributes_count);
    by_name.insert("dropped_events_count", dropped_events_count);
    by_name.insert("dropped_links_count", dropped_links_count);
    by_name.insert("events", events);
    by_name.insert("links", links);

    let columns: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|f| by_name.remove(f.name().as_str()).expect("column missing"))
        .collect();

    let batch = RecordBatch::try_new(arrow_schema.clone(), columns)?;

    let unique_suffix = format!("tempo-{tenant_id}-{now_micros}");
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(unique_suffix, None, DataFileFormat::Parquet);
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );
    let rolling_file_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_file_writer_builder);
    let mut data_file_writer = data_file_writer_builder.build(None).await?;

    data_file_writer.write(batch).await?;
    let data_files = data_file_writer.close().await?;

    let tx = Transaction::new(table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(Transaction::new(table))?;
    tx.commit(&**catalog).await?;

    Ok(())
}

/// Build an empty `ListArray` matching the given field definition.
///
/// Used to satisfy the iceberg `events` and `links` columns which are
/// defined as `List<Struct>`. We write `length` all-empty lists.
fn empty_list_array_for_field(field: &Field, length: usize) -> ArrayRef {
    let DataType::List(inner_field) = field.data_type() else {
        panic!("expected List datatype for field {}", field.name());
    };
    // Offsets: length+1 zeros, meaning every row is [inner[0..0]) — empty.
    let offsets = OffsetBuffer::new(vec![0i32; length + 1].into());
    let values = datafusion::arrow::array::new_empty_array(inner_field.data_type());
    Arc::new(ListArray::new(inner_field.clone(), offsets, values, None))
}

/// Build a `MapArray` for an iceberg `MAP<STRING,STRING>` column given
/// per-row key/value pairs. Used for the post-split `resource_attributes`
/// and `span_attributes` columns.
fn build_attribute_map(
    arrow_schema: &datafusion::arrow::datatypes::Schema,
    col_index: usize,
    rows: &[&[(&str, &str)]],
) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    let field = arrow_schema.field(col_index);
    let (key_field, value_field) = match field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (fields[0].clone(), fields[1].clone()),
            _ => panic!("Expected Struct type for map entries in {}", field.name()),
        },
        _ => panic!("Expected Map type for field {}", field.name()),
    };
    let field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut builder = MapBuilder::new(Some(field_names), StringBuilder::new(), StringBuilder::new())
        .with_keys_field(key_field)
        .with_values_field(value_field);

    for row in rows {
        for (k, v) in *row {
            builder.keys().append_value(*k);
            builder.values().append_value(*v);
        }
        builder.append(true)?;
    }
    Ok(Arc::new(builder.finish()))
}
