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
    ArrayRef, FixedSizeBinaryBuilder, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray,
};
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
use icegate_common::{
    CatalogBackend, CatalogConfig, EVENTS_TABLE, ICEGATE_NAMESPACE, IoHandle, LOGS_TABLE, METRICS_TABLE, SPANS_TABLE,
    TENANT_ID_HEADER, catalog::CatalogBuilder, schema,
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

        let server_handle = tokio::spawn(async move {
            icegate_query::flight_sql::run_with_port_tx(
                server_engine,
                flight_sql_config,
                cancel_token_clone,
                Some(port_tx),
            )
            .await
            .unwrap();
        });

        let actual_port = tokio::time::timeout(Duration::from_secs(10), port_rx)
            .await
            .expect("Timed out waiting for Flight SQL server to start")
            .expect("Failed to receive port from server");

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
        match tokio::time::timeout(Duration::from_secs(5), &mut self.server_handle).await {
            Ok(Ok(())) => {}
            Ok(Err(join_err)) if join_err.is_panic() => std::panic::resume_unwind(join_err.into_panic()),
            Ok(Err(join_err)) => panic!("Flight SQL server task failed to join: {join_err}"),
            Err(_elapsed) => {
                self.server_handle.abort();
                panic!("Flight SQL server did not shut down within 5s");
            }
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

    let attributes_field = arrow_schema
        .field_with_name("attributes")
        .expect("logs schema must contain an `attributes` field");
    let (key_field, value_field) = match attributes_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (fields[0].clone(), fields[1].clone()),
            other => panic!("expected Struct entries, got {other:?}"),
        },
        other => panic!("expected Map attributes column, got {other:?}"),
    };

    let field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut attributes_builder = MapBuilder::new(Some(field_names), StringBuilder::new(), StringBuilder::new())
        .with_keys_field(key_field)
        .with_values_field(value_field);
    for tenant_id in tenant_ids {
        attributes_builder.keys().append_value("tenant_marker");
        attributes_builder.values().append_value(tenant_id);
        attributes_builder.append(true)?;
    }
    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

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
            attributes,
        ],
    )?;

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
