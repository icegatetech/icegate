//! Shared bootstrap for Flight SQL integration tests.
//!
//! Spins up an in-memory Iceberg catalog with all four observability
//! tables (`logs`, `spans`, `events`, `metrics`), starts the Flight SQL
//! gRPC server on an OS-assigned port, and exposes a connected
//! [`FlightSqlServiceClient`] for tests to drive.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap
)]

use std::sync::Arc;
use std::time::Duration;

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryArray, FixedSizeBinaryBuilder, Int32Array, Int64Array, ListArray, MapBuilder,
    MapFieldNames, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion::parquet::file::properties::WriterProperties;
use futures::TryStreamExt;
use iceberg::Catalog;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use icegate_common::testing::server_task::{DrainOutcome, SHUTDOWN_TIMEOUT, drain_server_task};
use icegate_common::{
    CatalogBackend, CatalogConfig, EVENTS_TABLE, ICEGATE_NAMESPACE, IoHandle, LOGS_TABLE, METRICS_TABLE, PRICES_TABLE,
    SPANS_TABLE, TENANT_ID_HEADER, catalog::CatalogBuilder, schema,
};
use icegate_query::{
    engine::{QueryEngine, QueryEngineConfig},
    flight_sql::FlightSqlConfig,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

/// Severity values cycled across written log rows.
const SEVERITIES: [&str; 3] = ["INFO", "WARN", "ERROR"];

/// How long to wait for the server to report the port it bound.
const PORT_TIMEOUT: Duration = Duration::from_secs(10);

/// Connected Flight SQL test server.
///
/// Holds the cancellation token, the spawned server task, and a tonic
/// `Channel` that callers can wrap in `FlightSqlServiceClient` instances
/// — separate clients let a single test exercise multiple tenant
/// identities without re-handshaking.
pub struct TestServer {
    pub channel: Channel,
    pub cancel_token: CancellationToken,
    server_handle: tokio::task::JoinHandle<()>,
    /// Owns the temporary warehouse directory; held purely for its
    /// `Drop`, which removes the directory when the server is dropped.
    #[allow(dead_code)]
    temp_dir: tempfile::TempDir,
}

impl TestServer {
    /// Bind a Flight SQL server on an ephemeral port and return a
    /// channel + the underlying Iceberg catalog (so tests can write
    /// fixture rows).
    pub async fn start() -> Result<(Self, Arc<dyn Catalog>), Box<dyn std::error::Error>> {
        let warehouse_path = tempfile::tempdir()?;
        let warehouse_str = warehouse_path.path().to_str().unwrap().to_string();
        let catalog_config = CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: warehouse_str,
            properties: std::collections::HashMap::default(),
            cache: None,
        };
        let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new()).await?;

        create_namespace_and_tables(&catalog).await?;

        let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let wal_reader = Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(&wal_store), 8192).unwrap());
        let engine_config = QueryEngineConfig::default();
        let query_engine = Arc::new(QueryEngine::new(
            Arc::clone(&catalog),
            engine_config,
            wal_store,
            wal_reader,
        ));

        let flight_sql_config = FlightSqlConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 0,
            max_message_size: 16 * 1024 * 1024,
        };

        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        let server_engine = Arc::clone(&query_engine);
        let (port_tx, port_rx) = oneshot::channel::<u16>();

        let mut server_handle = tokio::spawn(async move {
            icegate_query::flight_sql::run_with_port_tx(
                server_engine,
                flight_sql_config,
                cancel_token_clone,
                Some(port_tx),
                icegate_common::MemoryPressure::inert(),
            )
            .await
            .unwrap();
        });

        // Drain the task on either failure before unwinding: `warehouse_path` drops
        // with this frame, so a detached server would go on serving from a deleted
        // directory. `drain_server_task` resumes the task's own panic when it has
        // one, which is the only place the real startup error survives.
        let actual_port = match tokio::time::timeout(PORT_TIMEOUT, port_rx).await {
            Ok(Ok(port)) => port,
            Ok(Err(_recv_err)) => {
                cancel_token.cancel();
                drain_server_task(&mut server_handle, "Flight SQL").await;
                panic!("Flight SQL server dropped the port channel before reporting a bound port");
            }
            Err(_elapsed) => {
                cancel_token.cancel();
                drain_server_task(&mut server_handle, "Flight SQL").await;
                panic!("Timed out waiting for Flight SQL server to bind");
            }
        };

        let endpoint = Endpoint::from_shared(format!("http://127.0.0.1:{actual_port}"))?;
        let channel = endpoint.connect().await?;

        Ok((
            Self {
                channel,
                cancel_token,
                server_handle,
                // Own the temp dir so it is cleaned up when the server is
                // dropped instead of leaked for the whole process.
                temp_dir: warehouse_path,
            },
            catalog,
        ))
    }

    /// Build a Flight SQL client and attach the tenant header.
    ///
    /// `set_header` ensures `x-scope-orgid` is included on every RPC,
    /// matching the production gateway-fronted pattern.
    pub fn client(&self, tenant_id: Option<&str>) -> FlightSqlServiceClient<Channel> {
        let mut client = FlightSqlServiceClient::new(self.channel.clone());
        if let Some(tid) = tenant_id {
            client.set_header(TENANT_ID_HEADER, tid);
        }
        client
    }

    /// Shutdown the server, draining the spawn task.
    ///
    /// Surfaces a panic from the server task and fails on timeout rather
    /// than swallowing both, so a crashing or hung server can't silently
    /// pass the test (and leak the background task).
    pub async fn shutdown(mut self) {
        self.cancel_token.cancel();
        match drain_server_task(&mut self.server_handle, "Flight SQL").await {
            DrainOutcome::Finished => {}
            DrainOutcome::Aborted => panic!(
                "Flight SQL server did not shut down within {}s",
                SHUTDOWN_TIMEOUT.as_secs()
            ),
        }
    }
}

async fn create_namespace_and_tables(catalog: &Arc<dyn Catalog>) -> Result<(), Box<dyn std::error::Error>> {
    let namespace_ident = iceberg::NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    if !catalog.namespace_exists(&namespace_ident).await? {
        catalog
            .create_namespace(&namespace_ident, std::collections::HashMap::new())
            .await?;
    }

    for (name, schema) in [
        (LOGS_TABLE, schema::logs_schema()?),
        (SPANS_TABLE, schema::spans_schema()?),
        (EVENTS_TABLE, schema::events_schema()?),
        (METRICS_TABLE, schema::metrics_schema()?),
        (PRICES_TABLE, schema::prices_schema()?),
    ] {
        let creation = iceberg::TableCreation::builder().name(name.to_string()).schema(schema).build();
        let _ = catalog.create_table(&namespace_ident, creation).await?;
    }
    Ok(())
}

/// Drive `client.execute(sql)` and collect every endpoint's results
/// into a flat `Vec<RecordBatch>`.
///
/// The Flight SQL spec allows a server to return multiple endpoints per
/// `FlightInfo` (for distributed execution); the IceGate server returns
/// exactly one, but the helper handles both shapes so future changes
/// don't break tests.
pub async fn execute_sql(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let info = client.execute(sql.to_owned(), None).await?;
    let mut batches = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint.ticket.ok_or("flight endpoint missing ticket")?;
        let stream = client.do_get(ticket).await?;
        let mut collected: Vec<RecordBatch> = collect_stream(stream).await?;
        batches.append(&mut collected);
    }
    Ok(batches)
}

async fn collect_stream(stream: FlightRecordBatchStream) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let batches: Vec<RecordBatch> = stream.try_collect().await?;
    Ok(batches)
}

/// Extract a single `i64` count from the first row, first column of the
/// returned batches. Panics if the shape doesn't match — tests should
/// only call this after asserting the SQL returns a scalar count.
pub fn count_from_batches(batches: &[RecordBatch]) -> i64 {
    let batch = batches.first().expect("expected at least one batch");
    let col = batch.column(0);
    let arr = col
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("count column should be Int64");
    arr.value(0)
}

/// Write one span row per `(tenant_id, name)` entry into a SINGLE Parquet
/// data file and commit it.
///
/// `spans` is the table that exposes projection-order bugs: `name` and the
/// hidden `tenant_id` are both non-nullable strings, so a projection handed
/// to the Iceberg reader out of schema order comes back relabelled rather
/// than reordered, and nothing in the Arrow types gives that away. Callers
/// supply `name` per row so a test can assert the exact values a projection
/// returns, not merely a row count.
///
/// Deliberately narrow, in the spirit of [`write_logs_file`]: every column
/// the schema requires is populated, but attributes, events, and links carry
/// only enough content to be valid. `crates/icegate-query/tests/tempo` owns
/// the richer span fixture used for trace formatting.
pub async fn write_spans_file(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    spans: &[(&str, &str)],
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let row_count = spans.len();
    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    let mut trace_id_builder = FixedSizeBinaryBuilder::new(16);
    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    for i in 0..row_count {
        trace_id_builder.append_value((i as u128).to_le_bytes())?;
        span_id_builder.append_value((i as u64).to_le_bytes())?;
    }

    let attribute_rows: Vec<&[(&str, &str)]> = vec![&[("http.method", "GET")]; row_count];
    // scope_attributes carries no content in this narrow fixture, same as the
    // scope/log levels in `write_logs_file` below.
    let empty_rows: Vec<&[(&str, &str)]> = vec![&[]; row_count];
    let timestamps: Vec<i64> = (0..row_count).map(|i| now_micros - (i as i64) * 1000).collect();
    let zeros: ArrayRef = Arc::new(Int32Array::from(vec![Some(0); row_count]));

    // Assemble by name so the fixture survives any reordering of the
    // canonical schema's fields.
    let mut by_name: std::collections::HashMap<&str, ArrayRef> = std::collections::HashMap::new();
    by_name.insert(
        schema::COL_TENANT_ID,
        Arc::new(StringArray::from(spans.iter().map(|(t, _)| *t).collect::<Vec<_>>())),
    );
    by_name.insert(
        schema::COL_SERVICE_NAME,
        Arc::new(StringArray::from(vec![Some("svc"); row_count])),
    );
    by_name.insert(schema::COL_TRACE_ID, Arc::new(trace_id_builder.finish()));
    by_name.insert(schema::COL_SPAN_ID, Arc::new(span_id_builder.finish()));
    by_name.insert(
        schema::COL_PARENT_SPAN_ID,
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            std::iter::repeat_n(None::<&[u8]>, row_count),
            8,
        )?),
    );
    by_name.insert(
        schema::COL_TIMESTAMP,
        Arc::new(TimestampMicrosecondArray::from(timestamps.clone())),
    );
    by_name.insert(
        schema::COL_END_TIMESTAMP,
        Arc::new(TimestampMicrosecondArray::from(timestamps)),
    );
    by_name.insert(
        schema::COL_INGESTED_TIMESTAMP,
        Arc::new(TimestampMicrosecondArray::from(vec![now_micros; row_count])),
    );
    by_name.insert(
        schema::COL_DURATION_MICROS,
        Arc::new(Int64Array::from(vec![1_000i64; row_count])),
    );
    by_name.insert(
        schema::COL_TRACE_STATE,
        Arc::new(StringArray::from(vec![None::<&str>; row_count])),
    );
    by_name.insert(
        schema::COL_NAME,
        Arc::new(StringArray::from(spans.iter().map(|(_, n)| *n).collect::<Vec<_>>())),
    );
    by_name.insert(schema::COL_KIND, Arc::new(Int32Array::from(vec![Some(2); row_count])));
    by_name.insert(schema::COL_STATUS_CODE, Arc::clone(&zeros));
    by_name.insert(
        schema::COL_STATUS_MESSAGE,
        Arc::new(StringArray::from(vec![None::<&str>; row_count])),
    );
    by_name.insert(
        schema::COL_RESOURCE_ATTRIBUTES,
        build_attribute_map(&arrow_schema, schema::COL_RESOURCE_ATTRIBUTES, &attribute_rows)?,
    );
    by_name.insert(
        schema::COL_SPAN_ATTRIBUTES,
        build_attribute_map(&arrow_schema, schema::COL_SPAN_ATTRIBUTES, &attribute_rows)?,
    );
    by_name.insert(
        schema::COL_SCOPE_ATTRIBUTES,
        build_attribute_map(&arrow_schema, schema::COL_SCOPE_ATTRIBUTES, &empty_rows)?,
    );
    // `icegate_common::schema` exports no `COL_*` constant for these four, so
    // they stay literals; the emptiness assertion below still catches drift.
    by_name.insert("flags", Arc::clone(&zeros));
    by_name.insert("dropped_attributes_count", Arc::clone(&zeros));
    by_name.insert("dropped_events_count", Arc::clone(&zeros));
    by_name.insert("dropped_links_count", zeros);
    by_name.insert(
        schema::COL_EVENTS,
        empty_list_array(&arrow_schema, schema::COL_EVENTS, row_count),
    );
    by_name.insert(
        schema::COL_LINKS,
        empty_list_array(&arrow_schema, schema::COL_LINKS, row_count),
    );

    let columns: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|f| {
            by_name
                .remove(f.name().as_str())
                .unwrap_or_else(|| panic!("spans fixture is missing column `{}`", f.name()))
        })
        .collect();
    assert!(
        by_name.is_empty(),
        "spans fixture builds columns the schema does not have: {:?}",
        by_name.keys()
    );

    let batch = RecordBatch::try_new(arrow_schema, columns)?;
    commit_data_file(table, catalog, batch, &format!("spans-{now_micros}")).await
}

/// A `LIST` column where every row is an empty list.
fn empty_list_array(schema: &datafusion::arrow::datatypes::Schema, name: &str, length: usize) -> ArrayRef {
    let field = schema.field_with_name(name).expect("list column present in schema");
    let DataType::List(inner_field) = field.data_type() else {
        panic!("expected a List column for `{name}`");
    };
    // length+1 zero offsets: every row spans values[0..0), i.e. an empty list.
    let offsets = OffsetBuffer::new(vec![0i32; length + 1].into());
    let values = datafusion::arrow::array::new_empty_array(inner_field.data_type());
    Arc::new(ListArray::new(Arc::clone(inner_field), offsets, values, None))
}

/// Build a `MAP<STRING,STRING>` column from per-row key/value pairs.
fn build_attribute_map(
    schema: &datafusion::arrow::datatypes::Schema,
    name: &str,
    rows: &[&[(&str, &str)]],
) -> Result<ArrayRef, Box<dyn std::error::Error>> {
    let field = schema.field_with_name(name).expect("map column present in schema");
    let (key_field, value_field) = match field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (fields[0].clone(), fields[1].clone()),
            other => panic!("expected Struct entries for `{name}`, got {other:?}"),
        },
        other => panic!("expected a Map column for `{name}`, got {other:?}"),
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
        for (key, value) in *row {
            builder.keys().append_value(*key);
            builder.values().append_value(*value);
        }
        builder.append(true)?;
    }
    Ok(Arc::new(builder.finish()))
}

/// Write one batch as a single Parquet data file and commit it to `table`.
async fn commit_data_file(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    batch: RecordBatch,
    file_suffix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    let file_name_generator = DefaultFileNameGenerator::new(file_suffix.to_string(), None, DataFileFormat::Parquet);
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

/// Write three rows of log data for the given tenant.
///
/// Thin wrapper over [`write_logs_file`] for the common single-tenant
/// case; preserves the original call shape used across the tenant tests.
pub async fn write_test_logs_for_tenant(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    tenant_id: &str,
    service_name: &str,
    body_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    write_logs_file(
        table,
        catalog,
        &[tenant_id, tenant_id, tenant_id],
        service_name,
        body_prefix,
    )
    .await
}

/// Write one log row per entry in `tenant_ids` into a SINGLE Parquet data
/// file and commit it.
///
/// When the slice holds more than one distinct tenant the rows are
/// *co-located in one file*, so file-level statistics for `tenant_id` span
/// every tenant present — isolation then cannot come from file/row-group
/// pruning and must be enforced by the wrapper's row-level filter. That is
/// exactly the production WAL hot-segment layout the tenant wrapper has to
/// defend against.
///
/// Schema layout follows `icegate_common::schema::logs_schema`; the helper
/// is intentionally narrower than the production writer in
/// `tests/loki/harness.rs` — Flight SQL tests only need enough rows to
/// assert tenant isolation, not full attribute-key coverage.
#[allow(clippy::too_many_lines)]
pub async fn write_logs_file(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    tenant_ids: &[&str],
    service_name: &str,
    body_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let row_count = tenant_ids.len();
    let unique_suffix = format!(
        "{}-{}",
        tenant_ids.first().copied().unwrap_or("empty"),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    );
    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;

    let tenant_id_arr: ArrayRef = Arc::new(StringArray::from(tenant_ids.to_vec()));
    let service_name_arr: ArrayRef = Arc::new(StringArray::from(vec![Some(service_name); row_count]));

    let timestamps: Vec<i64> = (0..row_count).map(|i| now_micros - (i as i64) * 1000).collect();
    let timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(timestamps.clone()));
    let observed_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(timestamps));
    let ingested_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![now_micros; row_count]));

    let severity_text: ArrayRef = Arc::new(StringArray::from(
        (0..row_count)
            .map(|i| Some(SEVERITIES[i % SEVERITIES.len()]))
            .collect::<Vec<_>>(),
    ));
    let body: ArrayRef = Arc::new(StringArray::from(
        (0..row_count)
            .map(|i| Some(format!("{body_prefix} message {}", i + 1)))
            .collect::<Vec<_>>(),
    ));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    // The logs schema splits attributes across three per-OTLP-level MAP
    // columns (see `icegate_common::schema::logs_schema`) rather than the
    // single merged `attributes` column this fixture used before that
    // split. `tenant.marker` is resource-level (constant per reporting
    // resource) and stored dotted — like real ingest output — so a test
    // that reads it back exercises the '.' -> '_' wire-name normalization
    // instead of a fixture that never needed it; mirrors the convention in
    // `tests/loki/harness.rs::write_test_logs_for_tenant`. scope/log levels
    // are legitimately empty: this fixture carries no per-scope or
    // per-record attributes.
    let resource_pairs: Vec<[(&str, &str); 1]> = tenant_ids.iter().map(|t| [("tenant.marker", *t)]).collect();
    let resource_rows: Vec<&[(&str, &str)]> = resource_pairs.iter().map(<[(&str, &str); 1]>::as_slice).collect();
    let resource_attributes = build_attribute_map(&arrow_schema, schema::COL_RESOURCE_ATTRIBUTES, &resource_rows)?;

    let empty_rows: Vec<&[(&str, &str)]> = vec![&[]; row_count];
    let scope_attributes = build_attribute_map(&arrow_schema, schema::COL_SCOPE_ATTRIBUTES, &empty_rows)?;
    let log_attributes = build_attribute_map(&arrow_schema, schema::COL_LOG_ATTRIBUTES, &empty_rows)?;

    // Distinct trace/span ids per row, derived from the row index so the
    // values stay unique without a fixed lookup table. The full index is
    // encoded little-endian (a `u128` fills the 16-byte trace id, a `u64`
    // the 8-byte span id) so ids don't collide once `row_count` exceeds
    // 256 — a single-byte index would wrap.
    let mut trace_id_builder = FixedSizeBinaryBuilder::new(16);
    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    for i in 0..row_count {
        trace_id_builder.append_value((i as u128).to_le_bytes())?;
        span_id_builder.append_value((i as u64).to_le_bytes())?;
    }
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            tenant_id_arr,
            service_name_arr,
            timestamp,
            observed_timestamp,
            ingested_timestamp,
            trace_id,
            span_id,
            severity_text,
            body,
            resource_attributes,
            scope_attributes,
            log_attributes,
        ],
    )?;

    commit_data_file(table, catalog, batch, &unique_suffix).await
}
