//! Benchmark harness for Loki query benchmarks
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryBuilder, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::parquet::file::properties::WriterProperties;
use iceberg::Catalog;
use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use icegate_common::testing::server_task::{DrainOutcome, PORT_BIND_TIMEOUT, SHUTDOWN_TIMEOUT, drain_server_task};
use icegate_common::{
    CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, LOGS_TABLE, catalog::CatalogBuilder, schema,
};
use icegate_query::engine::{QueryEngine, QueryEngineConfig};
use icegate_query::loki::LokiConfig;
use rand::Rng;
use reqwest::Client;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Build a `FixedSizeBinary(byte_width)` array by decoding `hex` once and
/// repeating the resulting bytes `count` times. Used to seed identical
/// `trace_id` / `span_id` values across many synthetic rows.
fn build_fixed_binary_array(hex: &str, count: usize, byte_width: i32) -> ArrayRef {
    let bytes = hex::decode(hex).expect("fixed binary hex");
    let mut builder = FixedSizeBinaryBuilder::with_capacity(count, byte_width);
    for _ in 0..count {
        builder.append_value(&bytes).expect("append fixed binary");
    }
    Arc::new(builder.finish())
}

/// Build a `MAP<Utf8,Utf8>` column from per-row key/value pairs, typed
/// exactly like the named attribute-map field of `arrow_schema`. Shared by
/// all three `write_benchmark_logs*` fixtures below now that logs attributes
/// are split into one column per OTLP level (`resource_attributes`,
/// `scope_attributes`, `log_attributes`) instead of a single merged
/// `attributes` map — see `icegate_common::schema::logs_schema`.
fn build_attribute_map(arrow_schema: &Schema, name: &str, rows: &[&[(&str, &str)]]) -> ArrayRef {
    let field = arrow_schema
        .field_with_name(name)
        .expect("attribute map column present in schema");
    let (key_field, value_field) = match field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (fields[0].clone(), fields[1].clone()),
            other => panic!("expected Struct type for `{name}` map entries, got {other:?}"),
        },
        other => panic!("expected Map type for `{name}` field, got {other:?}"),
    };
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
    /// Owns the temporary warehouse directory; held purely for its
    /// `Drop`, which removes the directory when the server is dropped.
    #[allow(dead_code)]
    temp_dir: tempfile::TempDir,
}

impl TestServer {
    /// Start a new test server on an ephemeral port
    pub async fn start() -> Result<(Self, Arc<dyn Catalog>), Box<dyn std::error::Error>> {
        let warehouse_path = tempfile::tempdir()?;
        let warehouse_str = warehouse_path.path().to_str().unwrap().to_string();

        let catalog_config = CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: warehouse_str.clone(),
            properties: std::collections::HashMap::default(),
            cache: None,
        };

        let loki_config = LokiConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 0,
        };

        let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new()).await?;

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

        // Provide an in-memory WAL store — WAL scan finds 0 segments,
        // which is correct for benchmarks that only write to Iceberg.
        let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let wal_reader = Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(&wal_store), 8192).unwrap());
        // Use very long refresh interval to prevent the background refresh task
        // from firing during benchmarks. The rebuild_lock contention between
        // background refresh and get_provider causes deadlocks with
        // criterion's block_on loop.
        let engine_config = QueryEngineConfig {
            refresh_interval_secs: 86400,
            max_age_secs: 86400,
            ..QueryEngineConfig::default()
        };
        let query_engine = Arc::new(QueryEngine::new(
            Arc::clone(&catalog),
            engine_config,
            wal_store,
            wal_reader,
        ));
        query_engine.start_background_refresh();

        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        let server_engine = Arc::clone(&query_engine);

        let (port_tx, port_rx) = oneshot::channel::<u16>();

        let mut server_handle = tokio::spawn(async move {
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

        let actual_port = match tokio::time::timeout(PORT_BIND_TIMEOUT, port_rx).await {
            Ok(Ok(port)) => port,
            Ok(Err(_recv_err)) => {
                cancel_token.cancel();
                drain_server_task(&mut server_handle, "Loki benchmark").await;
                panic!("Loki benchmark server dropped the port channel before reporting a bound port");
            }
            Err(_elapsed) => {
                cancel_token.cancel();
                drain_server_task(&mut server_handle, "Loki benchmark").await;
                panic!("Timed out waiting for Loki benchmark server to bind");
            }
        };

        Ok((
            Self {
                client: Client::new(),
                base_url: format!("http://127.0.0.1:{actual_port}"),
                cancel_token,
                server_handle,
                temp_dir: warehouse_path,
            },
            catalog,
        ))
    }

    /// Shut the server down, draining the spawn task.
    ///
    /// Surfaces a panic from the server task and fails on timeout rather
    /// than swallowing both, so a crashing or hung server can't silently
    /// skew a benchmark (and leak the background task).
    pub async fn shutdown(mut self) {
        self.cancel_token.cancel();
        match drain_server_task(&mut self.server_handle, "Loki benchmark").await {
            DrainOutcome::Finished => {}
            DrainOutcome::Aborted => panic!(
                "Loki benchmark server did not shut down within {}s",
                SHUTDOWN_TIMEOUT.as_secs()
            ),
        }
    }
}

/// Write benchmark log data with standard labels
pub async fn write_benchmark_logs(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let unique_suffix = format!(
        "bench-{}",
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    );
    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;

    // Spread logs across 5 minutes
    let time_range_micros = 5 * 60 * 1_000_000i64;
    let time_step = time_range_micros / i64::try_from(count).unwrap_or(1);

    let mut tenant_ids = Vec::with_capacity(count);
    let mut service_names = Vec::with_capacity(count);
    let mut timestamps = Vec::with_capacity(count);
    let mut observed_timestamps = Vec::with_capacity(count);
    let mut ingested_timestamps = Vec::with_capacity(count);
    let mut severity_texts = Vec::with_capacity(count);
    let mut bodies = Vec::with_capacity(count);

    let body_strs: Vec<String> = (0..count).map(|i| format!("Request processed successfully {}", i)).collect();

    for (i, body_str) in body_strs.iter().enumerate() {
        tenant_ids.push("test-tenant");
        service_names.push(Some("api"));
        let ts = now_micros - (i64::try_from(i).unwrap_or(0) * time_step);
        timestamps.push(ts);
        observed_timestamps.push(ts);
        ingested_timestamps.push(now_micros);
        severity_texts.push(Some("INFO"));
        bodies.push(Some(body_str.as_str()));
    }

    let tenant_id_arr: ArrayRef = Arc::new(StringArray::from(tenant_ids));
    let service_name_arr: ArrayRef = Arc::new(StringArray::from(service_names));
    let timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(timestamps));
    let observed_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(observed_timestamps));
    let ingested_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(ingested_timestamps));
    let severity_text_arr: ArrayRef = Arc::new(StringArray::from(severity_texts));
    let body_arr: ArrayRef = Arc::new(StringArray::from(bodies));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    // `dataset`/`env` are constant for every row this benchmark writes, so —
    // mirroring the resource-vs-log split documented in
    // `tests/loki/harness.rs` — both describe the reporting resource, not an
    // individual log record, and land in `resource_attributes`. Neither is
    // stored dotted: both are single-token bench-only labels with no natural
    // dotted split, and the `loki_queries` benchmarks match on these exact
    // bare wire names (`.` -> `_` normalization would only change that name
    // if a dot were present — see `map_normalized_lookup::normalize_key`).
    let resource_pairs: [(&str, &str); 2] = [("dataset", "baseline"), ("env", "prod")];
    let resource_rows: Vec<&[(&str, &str)]> = vec![resource_pairs.as_slice(); count];
    let resource_attributes_arr = build_attribute_map(&arrow_schema, schema::COL_RESOURCE_ATTRIBUTES, &resource_rows);
    let empty_rows: Vec<&[(&str, &str)]> = vec![&[]; count];
    let scope_attributes_arr = build_attribute_map(&arrow_schema, schema::COL_SCOPE_ATTRIBUTES, &empty_rows);
    let log_attributes_arr = build_attribute_map(&arrow_schema, schema::COL_LOG_ATTRIBUTES, &empty_rows);

    // trace_id / span_id are FIXED_LEN_BYTE_ARRAY (16 / 8) in the logs
    // schema; decode the hex fixtures into raw bytes once and reuse them
    // for every row.
    let trace_id_arr = build_fixed_binary_array("0102030405060708090a0b0c0d0e0f10", count, 16);
    let span_id_arr = build_fixed_binary_array("0102030405060708", count, 8);

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id_arr,
            service_name_arr,
            timestamp_arr,
            observed_timestamp_arr,
            ingested_timestamp_arr,
            trace_id_arr,
            span_id_arr,
            severity_text_arr,
            body_arr,
            resource_attributes_arr,
            scope_attributes_arr,
            log_attributes_arr,
        ],
    )?;

    write_batch_to_table(table, catalog, batch, &unique_suffix).await
}

/// Write benchmark logs with numeric attributes for unwrap operations
pub async fn write_benchmark_logs_with_numeric_attrs(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let unique_suffix = format!(
        "bench-numeric-{}",
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    );
    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;

    let time_range_micros = 5 * 60 * 1_000_000i64;
    let time_step = time_range_micros / i64::try_from(count).unwrap_or(1);

    // Generate random values before async context to avoid Send issues
    let random_values: Vec<(i32, i32, i32)> = {
        let mut rng = rand::rng();
        (0..count)
            .map(|_| {
                (
                    rng.random_range(10..500),
                    rng.random_range(50..2000),
                    rng.random_range(100..50000),
                )
            })
            .collect()
    };

    let mut tenant_ids = Vec::with_capacity(count);
    let mut service_names = Vec::with_capacity(count);
    let mut timestamps = Vec::with_capacity(count);
    let mut observed_timestamps = Vec::with_capacity(count);
    let mut ingested_timestamps = Vec::with_capacity(count);
    let mut severity_texts = Vec::with_capacity(count);
    let mut bodies = Vec::with_capacity(count);

    let body_strs: Vec<String> = (0..count).map(|i| format!("Request processed {}", i)).collect();

    for (i, body_str) in body_strs.iter().enumerate() {
        tenant_ids.push("test-tenant");
        service_names.push(Some("api"));
        let ts = now_micros - (i64::try_from(i).unwrap_or(0) * time_step);
        timestamps.push(ts);
        observed_timestamps.push(ts);
        ingested_timestamps.push(now_micros);
        severity_texts.push(Some("INFO"));
        bodies.push(Some(body_str.as_str()));
    }

    let tenant_id_arr: ArrayRef = Arc::new(StringArray::from(tenant_ids));
    let service_name_arr: ArrayRef = Arc::new(StringArray::from(service_names));
    let timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(timestamps));
    let observed_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(observed_timestamps));
    let ingested_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(ingested_timestamps));
    let severity_text_arr: ArrayRef = Arc::new(StringArray::from(severity_texts));
    let body_arr: ArrayRef = Arc::new(StringArray::from(bodies));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    // `dataset` is constant for every row (resource-level, matching
    // `write_benchmark_logs` above). `latency`/`request_time`/`response_size`
    // are per-request measurements that genuinely describe THIS log record,
    // not the reporting resource, so they land in `log_attributes` instead.
    // `request_time`/`response_size` are stored dotted (`request.time`,
    // `response.size`) — the embedded `_` is exactly where a `.` normalizes
    // back to on read (see `map_normalized_lookup::normalize_key`), so the
    // `loki_queries` benchmarks' bare `request_time`/`response_size` matchers
    // still resolve. `dataset`/`latency` have no such split point without
    // changing their post-normalization wire name, so they stay undotted.
    let resource_pairs: [(&str, &str); 1] = [("dataset", "numeric")];
    let resource_rows: Vec<&[(&str, &str)]> = vec![resource_pairs.as_slice(); count];
    let resource_attributes_arr = build_attribute_map(&arrow_schema, schema::COL_RESOURCE_ATTRIBUTES, &resource_rows);
    let empty_rows: Vec<&[(&str, &str)]> = vec![&[]; count];
    let scope_attributes_arr = build_attribute_map(&arrow_schema, schema::COL_SCOPE_ATTRIBUTES, &empty_rows);

    let value_strings: Vec<(String, String, String)> = random_values
        .iter()
        .map(|(latency, request_time, response_size)| {
            (latency.to_string(), request_time.to_string(), response_size.to_string())
        })
        .collect();
    let log_pairs: Vec<[(&str, &str); 3]> = value_strings
        .iter()
        .map(|(latency, request_time, response_size)| {
            [
                ("latency", latency.as_str()),
                ("request.time", request_time.as_str()),
                ("response.size", response_size.as_str()),
            ]
        })
        .collect();
    let log_rows: Vec<&[(&str, &str)]> = log_pairs.iter().map(<[(&str, &str); 3]>::as_slice).collect();
    let log_attributes_arr = build_attribute_map(&arrow_schema, schema::COL_LOG_ATTRIBUTES, &log_rows);

    // trace_id / span_id are FIXED_LEN_BYTE_ARRAY (16 / 8) in the logs
    // schema; decode the hex fixtures into raw bytes once and reuse them
    // for every row.
    let trace_id_arr = build_fixed_binary_array("0102030405060708090a0b0c0d0e0f10", count, 16);
    let span_id_arr = build_fixed_binary_array("0102030405060708", count, 8);

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id_arr,
            service_name_arr,
            timestamp_arr,
            observed_timestamp_arr,
            ingested_timestamp_arr,
            trace_id_arr,
            span_id_arr,
            severity_text_arr,
            body_arr,
            resource_attributes_arr,
            scope_attributes_arr,
            log_attributes_arr,
        ],
    )?;

    write_batch_to_table(table, catalog, batch, &unique_suffix).await
}

/// Write benchmark logs with varied labels for grouping operations
pub async fn write_benchmark_logs_with_varied_labels(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let unique_suffix = format!(
        "bench-grouped-{}",
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    );
    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;

    let time_range_micros = 5 * 60 * 1_000_000i64;
    let time_step = time_range_micros / i64::try_from(count).unwrap_or(1);

    let namespaces = ["prod", "staging", "dev"];
    let pods = ["pod-1", "pod-2", "pod-3", "pod-4", "pod-5"];

    let mut tenant_ids = Vec::with_capacity(count);
    let mut service_names = Vec::with_capacity(count);
    let mut timestamps = Vec::with_capacity(count);
    let mut observed_timestamps = Vec::with_capacity(count);
    let mut ingested_timestamps = Vec::with_capacity(count);
    let mut severity_texts = Vec::with_capacity(count);
    let mut bodies = Vec::with_capacity(count);

    let body_strs: Vec<String> = (0..count).map(|i| format!("Request {} processed", i)).collect();

    for (i, body_str) in body_strs.iter().enumerate() {
        tenant_ids.push("test-tenant");
        service_names.push(Some("api"));
        let ts = now_micros - (i64::try_from(i).unwrap_or(0) * time_step);
        timestamps.push(ts);
        observed_timestamps.push(ts);
        ingested_timestamps.push(now_micros);
        severity_texts.push(Some("INFO"));
        bodies.push(Some(body_str.as_str()));
    }

    let tenant_id_arr: ArrayRef = Arc::new(StringArray::from(tenant_ids));
    let service_name_arr: ArrayRef = Arc::new(StringArray::from(service_names));
    let timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(timestamps));
    let observed_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(observed_timestamps));
    let ingested_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(ingested_timestamps));
    let severity_text_arr: ArrayRef = Arc::new(StringArray::from(severity_texts));
    let body_arr: ArrayRef = Arc::new(StringArray::from(bodies));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    // `namespace`/`pod` mirror OTel's canonical `k8s.namespace.name` /
    // `k8s.pod.name` resource attributes, and `dataset` is this benchmark's
    // own constant-per-write resource marker (see `write_benchmark_logs`
    // above) — all three genuinely describe the reporting resource, not the
    // individual log record, so all three land in `resource_attributes`
    // (varying per row is realistic here: a flattened multi-resource table
    // legitimately holds rows contributed by many different pods). Kept
    // undotted: the real `k8s.namespace.name` spelling would normalize to
    // wire label `k8s_namespace_name`, breaking the `by (namespace, pod)` /
    // `without (pod)` grouping the `loki_queries` benchmarks match on.
    let resource_pairs: Vec<[(&str, &str); 3]> = (0..count)
        .map(|i| {
            [
                ("dataset", "varied"),
                ("namespace", namespaces[i % namespaces.len()]),
                ("pod", pods[i % pods.len()]),
            ]
        })
        .collect();
    let resource_rows: Vec<&[(&str, &str)]> = resource_pairs.iter().map(<[(&str, &str); 3]>::as_slice).collect();
    let resource_attributes_arr = build_attribute_map(&arrow_schema, schema::COL_RESOURCE_ATTRIBUTES, &resource_rows);
    let empty_rows: Vec<&[(&str, &str)]> = vec![&[]; count];
    let scope_attributes_arr = build_attribute_map(&arrow_schema, schema::COL_SCOPE_ATTRIBUTES, &empty_rows);
    let log_attributes_arr = build_attribute_map(&arrow_schema, schema::COL_LOG_ATTRIBUTES, &empty_rows);

    // trace_id / span_id are FIXED_LEN_BYTE_ARRAY (16 / 8) in the logs
    // schema; decode the hex fixtures into raw bytes once and reuse them
    // for every row.
    let trace_id_arr = build_fixed_binary_array("0102030405060708090a0b0c0d0e0f10", count, 16);
    let span_id_arr = build_fixed_binary_array("0102030405060708", count, 8);

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id_arr,
            service_name_arr,
            timestamp_arr,
            observed_timestamp_arr,
            ingested_timestamp_arr,
            trace_id_arr,
            span_id_arr,
            severity_text_arr,
            body_arr,
            resource_attributes_arr,
            scope_attributes_arr,
            log_attributes_arr,
        ],
    )?;

    write_batch_to_table(table, catalog, batch, &unique_suffix).await
}

/// Helper to write a `RecordBatch` to an Iceberg table
async fn write_batch_to_table(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    batch: RecordBatch,
    unique_suffix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let location_generator = DefaultLocationGenerator::new(table.metadata())?;
    let file_name_generator = DefaultFileNameGenerator::new(unique_suffix.to_string(), None, DataFileFormat::Parquet);

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
