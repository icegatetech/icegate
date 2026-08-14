//! Common test harness for Loki integration tests
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{
            ArrayRef, FixedSizeBinaryBuilder, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder,
            TimestampMicrosecondArray,
        },
        datatypes::{DataType, FieldRef, Schema},
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
    CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, LOGS_TABLE, catalog::CatalogBuilder, schema,
};
use icegate_query::{
    engine::{QueryEngine, QueryEngineConfig},
    loki::LokiConfig,
};
use reqwest::Client;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

// ============================================================================
// Attribute map fixture builders
// ============================================================================
//
// The logs table stores attributes as three separate MAP columns —
// `resource_attributes`, `scope_attributes`, `log_attributes` — one per OTLP
// level (see SCHEMA.md), each with its own Iceberg field-id metadata. Every
// fixture in this test suite needs to build all three, so the shared
// (key_field, value_field) extraction and MapArray construction live here
// instead of being copy-pasted per file.

/// Extract the key/value `Field`s of the MAP-typed column at `field_idx` in
/// `arrow_schema`.
///
/// Each of `resource_attributes` / `scope_attributes` / `log_attributes`
/// carries distinct Iceberg field-id metadata, so callers MUST pass the index
/// matching the level they are about to build with [`build_attribute_map`] —
/// reusing another level's fields here makes the built array's `DataType`
/// disagree with the target schema field, and `RecordBatch::try_new` rejects
/// that as a genuine mismatch rather than accepting it silently.
///
/// # Panics
/// Panics if `field_idx` is not a `Map<Struct>` column — a fixture-authoring
/// error the test should fail loudly on, not a runtime condition to recover
/// from.
pub fn map_entry_fields(arrow_schema: &Schema, field_idx: usize) -> (FieldRef, FieldRef) {
    match arrow_schema.field(field_idx).data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (Arc::clone(&fields[0]), Arc::clone(&fields[1])),
            other => panic!("expected Struct type for map entries, got {other:?}"),
        },
        other => panic!("expected Map type for attributes field, got {other:?}"),
    }
}

/// Build one level's attribute `MapArray` from each row's `(key, value)`
/// pairs. `rows.len()` becomes the array's row count, so callers building a
/// multi-column `RecordBatch` must pass one entry per row even when a given
/// row's map is empty. `key_field`/`value_field` must come from
/// [`map_entry_fields`] for the same column position this array is placed at.
pub fn build_attribute_map(key_field: FieldRef, value_field: FieldRef, rows: &[&[(&str, &str)]]) -> ArrayRef {
    let field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let mut builder = MapBuilder::new(Some(field_names), StringBuilder::new(), StringBuilder::new())
        .with_keys_field(key_field)
        .with_values_field(value_field);
    for pairs in rows {
        for (k, v) in *pairs {
            builder.keys().append_value(*k);
            builder.values().append_value(*v);
        }
        builder.append(true).expect("append map row");
    }
    Arc::new(builder.finish())
}

/// Test server configuration and handles
pub struct TestServer {
    pub client: Client,
    pub base_url: String,
    pub cancel_token: CancellationToken,
    server_handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    /// Start a new test server on an ephemeral port (OS-assigned)
    ///
    /// Uses port 0 to let the OS assign an available port, avoiding port conflicts
    /// when running tests in parallel.
    pub async fn start() -> Result<(Self, Arc<dyn Catalog>), Box<dyn std::error::Error>> {
        let warehouse_path = tempfile::tempdir()?;
        let warehouse_str = warehouse_path.path().to_str().unwrap().to_string();

        let catalog_config = CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: warehouse_str.clone(),
            properties: std::collections::HashMap::default(),
            cache: None,
        };

        // Use port 0 for ephemeral port assignment
        let loki_config = LokiConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 0,
        };

        let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new()).await?;

        // Create namespace and table
        let namespace_ident = iceberg::NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());

        if !catalog.namespace_exists(&namespace_ident).await? {
            catalog
                .create_namespace(&namespace_ident, std::collections::HashMap::new())
                .await?;
        }

        let schema = schema::logs_schema()?;

        let table_creation = iceberg::TableCreation::builder()
            .name(LOGS_TABLE.to_string())
            .schema(schema)
            .build();

        let _ = catalog.create_table(&namespace_ident, table_creation).await?;

        // Start server with port notification channel.
        // Provide an in-memory WAL store — WAL scan finds 0 segments,
        // which is correct for tests that only write to Iceberg.
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

        // Create oneshot channel to receive the actual bound port
        let (port_tx, port_rx) = oneshot::channel::<u16>();

        let server_handle = tokio::spawn(async move {
            let disabled_metrics = Arc::new(icegate_query::infra::metrics::QueryMetrics::new_disabled());
            icegate_query::loki::run_with_port_tx(
                server_engine,
                loki_config,
                cancel_token_clone,
                Some(port_tx),
                disabled_metrics,
                icegate_common::MemoryPressure::inert(),
            )
            .await
            .unwrap();
        });

        // Wait for the server to bind and receive the actual port
        let actual_port = tokio::time::timeout(Duration::from_secs(10), port_rx)
            .await
            .expect("Timed out waiting for server to start")
            .expect("Failed to receive port from server");

        // Leak the tempdir to keep it alive for the duration of the test
        // (it will be cleaned up when the process exits)
        Box::leak(Box::new(warehouse_path));

        Ok((
            Self {
                client: Client::new(),
                base_url: format!("http://127.0.0.1:{actual_port}"),
                cancel_token,
                server_handle,
            },
            catalog,
        ))
    }

    /// Shutdown the test server
    pub async fn shutdown(self) {
        self.cancel_token.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), self.server_handle).await;
    }
}

/// Write test log data to an Iceberg table for a specific tenant
pub async fn write_test_logs_for_tenant(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    tenant_id: &str,
    service_name: &str,
    body_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let unique_suffix = format!(
        "{}-{}",
        tenant_id,
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    );
    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;

    let tenant_id_arr: ArrayRef = Arc::new(StringArray::from(vec![tenant_id, tenant_id, tenant_id]));
    let service_name_arr: ArrayRef = Arc::new(StringArray::from(vec![
        Some(service_name),
        Some(service_name),
        Some(service_name),
    ]));
    let timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros,
        now_micros - 1000,
        now_micros - 2000,
    ]));
    let observed_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros,
        now_micros - 1000,
        now_micros - 2000,
    ]));
    let ingested_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros, now_micros, now_micros,
    ]));
    let severity_text: ArrayRef = Arc::new(StringArray::from(vec![Some("INFO"), Some("WARN"), Some("ERROR")]));
    let body: ArrayRef = Arc::new(StringArray::from(vec![
        Some(format!("{} message 1", body_prefix)),
        Some(format!("{} message 2", body_prefix)),
        Some(format!("{} message 3", body_prefix)),
    ]));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    // Test data trace_id and span_id values as hex strings
    let trace_ids = [
        "0102030405060708090a0b0c0d0e0f10",
        "1112131415161718191a1b1c1d1e1f20",
        "2122232425262728292a2b2c2d2e2f30",
    ];
    let span_ids = ["0102030405060708", "1112131415161718", "2122232425262728"];
    let bodies = [
        format!("{} message 1", body_prefix),
        format!("{} message 2", body_prefix),
        format!("{} message 3", body_prefix),
    ];

    // `tenant.marker` is constant for every record this writer instance
    // emits, so it belongs at the resource level; `body` duplicates the
    // typed column per record, so it stays a log attribute. Stored dotted
    // ('tenant.marker', not 'tenant_marker') so the read path's '.' -> '_'
    // normalisation is actually exercised — both spellings produce the same
    // wire label, so nothing downstream depends on which one is stored.
    let resource_pairs: [(&str, &str); 1] = [("tenant.marker", tenant_id)];
    let resource_rows: [&[(&str, &str)]; 3] = [&resource_pairs, &resource_pairs, &resource_pairs];
    let (resource_key_field, resource_value_field) = map_entry_fields(&arrow_schema, 9);
    let resource_attributes = build_attribute_map(resource_key_field, resource_value_field, &resource_rows);

    // Scope level: the instrumentation library that produced the record.
    // Populated so a regression that stopped reading `scope_attributes`
    // entirely cannot pass — an empty map at this level is indistinguishable
    // from a dropped one.
    let scope_pairs: [(&str, &str); 1] = [("otel.scope.name", "test-instrumentation")];
    let scope_rows: [&[(&str, &str)]; 3] = [&scope_pairs, &scope_pairs, &scope_pairs];
    let (scope_key_field, scope_value_field) = map_entry_fields(&arrow_schema, 10);
    let scope_attributes = build_attribute_map(scope_key_field, scope_value_field, &scope_rows);

    let log_pairs: Vec<[(&str, &str); 1]> = bodies.iter().map(|b| [("body", b.as_str())]).collect();
    let log_rows: Vec<&[(&str, &str)]> = log_pairs.iter().map(<[(&str, &str); 1]>::as_slice).collect();
    let (log_key_field, log_value_field) = map_entry_fields(&arrow_schema, 11);
    let log_attributes = build_attribute_map(log_key_field, log_value_field, &log_rows);

    // trace_id is now stored as raw 16-byte FIXED_LEN_BYTE_ARRAY; decode hex
    // fixtures into bytes to match the production schema.
    let mut trace_id_builder = FixedSizeBinaryBuilder::new(16);
    for tid in trace_ids {
        let bytes = hex::decode(tid).expect("trace_id hex");
        trace_id_builder.append_value(&bytes).expect("trace_id length 16");
    }
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    for sid in span_ids {
        let bytes = hex::decode(sid).expect("span_id hex");
        span_id_builder.append_value(&bytes).expect("span_id length 8");
    }
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

    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
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
    let action = tx.fast_append();
    let action = action.add_data_files(data_files);
    let tx = action.apply(Transaction::new(table))?;
    tx.commit(&**catalog).await?;

    Ok(())
}

/// Write standard test log data to an Iceberg table
#[allow(clippy::too_many_lines)]
pub async fn write_test_logs(table: &Table, catalog: &Arc<dyn Catalog>) -> Result<(), Box<dyn std::error::Error>> {
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    let tenant_id: ArrayRef = Arc::new(StringArray::from(vec!["test-tenant", "test-tenant", "test-tenant"]));
    let service_name: ArrayRef = Arc::new(StringArray::from(vec![
        Some("frontend"),
        Some("frontend"),
        Some("backend"),
    ]));
    let timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros,
        now_micros - 1000,
        now_micros - 2000,
    ]));
    let observed_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros,
        now_micros - 1000,
        now_micros - 2000,
    ]));
    let ingested_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros, now_micros, now_micros,
    ]));
    let severity_text: ArrayRef = Arc::new(StringArray::from(vec![Some("INFO"), Some("WARN"), Some("ERROR")]));
    let body: ArrayRef = Arc::new(StringArray::from(vec![
        Some("User logged in successfully"),
        Some("Page rendered in 120ms"),
        Some("Database connection slow"),
    ]));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    // Test data trace_id and span_id values as hex strings
    let trace_ids = [
        "0102030405060708090a0b0c0d0e0f10",
        "1112131415161718191a1b1c1d1e1f20",
        "2122232425262728292a2b2c2d2e2f30",
    ];
    let span_ids = ["0102030405060708", "1112131415161718", "2122232425262728"];
    let bodies = [
        "User logged in successfully",
        "Page rendered in 120ms",
        "Database connection slow",
    ];

    // Indexed top-level columns are NOT mirrored into the attributes maps —
    // the read pipeline reconstructs the merged labels view from the typed
    // columns at materialisation time (see loki/formatters.rs::extract_labels).
    //
    // The bulk of these attributes are genuinely log-record-level (they
    // describe THIS event), but the scope level carries `otel.scope.name` on
    // every row: with all three maps read, merged, and normalized by the same
    // pipeline, a fixture that left a level empty could not tell a dropped
    // level from an absent one. `resource_attributes` is covered by
    // grouping.rs's node/pod/instance fixture.
    //
    // Keys are stored dotted (`user.id`, not `user_id`) so the read path's
    // '.' -> '_' normalisation is actually exercised; pipeline.rs/labels.rs
    // assert on the post-normalization wire names (`user_id`, `request_id`),
    // which are unchanged by the storage spelling.
    // Row 0: "User logged in successfully"
    let row0_pairs: [(&str, &str); 3] = [("user.id", "user-123"), ("request.id", "req-456"), ("body", bodies[0])];
    // Row 1: "Page rendered in 120ms"
    let row1_pairs: [(&str, &str); 3] = [
        ("http.target", "/dashboard"),
        ("http.duration_ms", "120"),
        ("body", bodies[1]),
    ];
    // Row 2: "Database connection slow"
    let row2_pairs: [(&str, &str); 3] = [
        ("server.address", "db-primary"),
        ("db.query_time_ms", "250"),
        ("body", bodies[2]),
    ];

    let (resource_key_field, resource_value_field) = map_entry_fields(&arrow_schema, 9);
    let resource_attributes = build_attribute_map(resource_key_field, resource_value_field, &[&[], &[], &[]]);

    let scope_pairs: [(&str, &str); 1] = [("otel.scope.name", "test-instrumentation")];
    let (scope_key_field, scope_value_field) = map_entry_fields(&arrow_schema, 10);
    let scope_attributes = build_attribute_map(
        scope_key_field,
        scope_value_field,
        &[&scope_pairs, &scope_pairs, &scope_pairs],
    );

    let (log_key_field, log_value_field) = map_entry_fields(&arrow_schema, 11);
    let log_attributes = build_attribute_map(log_key_field, log_value_field, &[&row0_pairs, &row1_pairs, &row2_pairs]);

    // trace_id / span_id are now FIXED_LEN_BYTE_ARRAY — decode hex fixtures.
    let mut trace_id_builder = FixedSizeBinaryBuilder::new(16);
    for tid in trace_ids {
        let bytes = hex::decode(tid).expect("trace_id hex");
        trace_id_builder.append_value(&bytes).expect("trace_id length 16");
    }
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    for sid in span_ids {
        let bytes = hex::decode(sid).expect("span_id hex");
        span_id_builder.append_value(&bytes).expect("span_id length 8");
    }
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            tenant_id,
            service_name,
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

    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    let file_name_generator = DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);

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
    let action = tx.fast_append();
    let action = action.add_data_files(data_files);
    let tx = action.apply(Transaction::new(table))?;
    tx.commit(&**catalog).await?;

    Ok(())
}
