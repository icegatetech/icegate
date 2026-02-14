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
    ArrayRef, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::DataType;
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
use icegate_common::{CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, LOGS_TABLE, catalog::CatalogBuilder, schema};
use icegate_query::engine::{QueryEngine, QueryEngineConfig};
use icegate_query::loki::LokiConfig;
use rand::Rng;
use reqwest::Client;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

/// Test server configuration and handles
pub struct TestServer {
    pub client: Client,
    pub base_url: String,
    pub cancel_token: CancellationToken,
    server_handle: tokio::task::JoinHandle<()>,
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
        };

        let loki_config = LokiConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 0,
        };

        let catalog = CatalogBuilder::from_config(&catalog_config).await?;

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

        let query_engine = Arc::new(
            QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
                .await
                .expect("Failed to create QueryEngine"),
        );

        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        let server_engine = Arc::clone(&query_engine);

        let (port_tx, port_rx) = oneshot::channel::<u16>();

        let server_handle = tokio::spawn(async move {
            let disabled_metrics = std::sync::Arc::new(icegate_query::infra::metrics::QueryMetrics::new_disabled());
            icegate_query::loki::run_with_port_tx(
                server_engine,
                loki_config,
                cancel_token_clone,
                Some(port_tx),
                disabled_metrics,
            )
            .await
            .unwrap();
        });

        let actual_port = match tokio::time::timeout(Duration::from_secs(10), port_rx).await {
            Ok(Ok(port)) => port,
            Ok(Err(_)) => {
                cancel_token.cancel();
                panic!("Failed to receive port from server");
            }
            Err(_) => {
                cancel_token.cancel();
                panic!("Timed out waiting for server to start");
            }
        };

        // SAFETY: Intentionally leak the TempDir to keep it alive for the benchmark/server lifetime.
        // The spawned server requires a persistent filesystem path that outlives this function.
        // This is an acceptable leak in the benchmark harness context.
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
    let mut account_ids = Vec::with_capacity(count);
    let mut service_names = Vec::with_capacity(count);
    let mut timestamps = Vec::with_capacity(count);
    let mut observed_timestamps = Vec::with_capacity(count);
    let mut ingested_timestamps = Vec::with_capacity(count);
    let mut severity_texts = Vec::with_capacity(count);
    let mut bodies = Vec::with_capacity(count);

    let body_strs: Vec<String> = (0..count).map(|i| format!("Request processed successfully {}", i)).collect();

    for (i, body_str) in body_strs.iter().enumerate() {
        tenant_ids.push("test-tenant");
        account_ids.push(Some("acc-1"));
        service_names.push(Some("api"));
        let ts = now_micros - (i64::try_from(i).unwrap_or(0) * time_step);
        timestamps.push(ts);
        observed_timestamps.push(ts);
        ingested_timestamps.push(now_micros);
        severity_texts.push(Some("INFO"));
        bodies.push(Some(body_str.as_str()));
    }

    let tenant_id_arr: ArrayRef = Arc::new(StringArray::from(tenant_ids));
    let account_id_arr: ArrayRef = Arc::new(StringArray::from(account_ids));
    let service_name_arr: ArrayRef = Arc::new(StringArray::from(service_names));
    let timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(timestamps));
    let observed_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(observed_timestamps));
    let ingested_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(ingested_timestamps));
    let severity_text_arr: ArrayRef = Arc::new(StringArray::from(severity_texts));
    let body_arr: ArrayRef = Arc::new(StringArray::from(bodies));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    let attributes_field = arrow_schema
        .field_with_name("attributes")
        .expect("Schema must contain an 'attributes' field");
    let (key_field, value_field) = match attributes_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (fields[0].clone(), fields[1].clone()),
            _ => panic!("Expected Struct type for map entries"),
        },
        _ => panic!("Expected Map type for attributes field"),
    };

    let field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let key_builder = StringBuilder::new();
    let value_builder = StringBuilder::new();
    let mut attributes_builder = MapBuilder::new(Some(field_names), key_builder, value_builder)
        .with_keys_field(key_field)
        .with_values_field(value_field);

    for _ in 0..count {
        attributes_builder.keys().append_value("env");
        attributes_builder.values().append_value("prod");
        attributes_builder.append(true)?;
    }

    let attributes_arr: ArrayRef = Arc::new(attributes_builder.finish());

    // Create hex-encoded trace and span IDs (matching logs schema)
    let mut trace_id_builder = StringBuilder::new();
    let mut span_id_builder = StringBuilder::new();

    for _ in 0..count {
        trace_id_builder.append_value("0102030405060708090a0b0c0d0e0f10");
        span_id_builder.append_value("0102030405060708");
    }

    let trace_id_arr: ArrayRef = Arc::new(trace_id_builder.finish());
    let span_id_arr: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id_arr,
            account_id_arr,
            service_name_arr,
            timestamp_arr,
            observed_timestamp_arr,
            ingested_timestamp_arr,
            trace_id_arr,
            span_id_arr,
            severity_text_arr,
            body_arr,
            attributes_arr,
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
    let mut account_ids = Vec::with_capacity(count);
    let mut service_names = Vec::with_capacity(count);
    let mut timestamps = Vec::with_capacity(count);
    let mut observed_timestamps = Vec::with_capacity(count);
    let mut ingested_timestamps = Vec::with_capacity(count);
    let mut severity_texts = Vec::with_capacity(count);
    let mut bodies = Vec::with_capacity(count);

    let body_strs: Vec<String> = (0..count).map(|i| format!("Request processed {}", i)).collect();

    for (i, body_str) in body_strs.iter().enumerate() {
        tenant_ids.push("test-tenant");
        account_ids.push(Some("acc-1"));
        service_names.push(Some("api"));
        let ts = now_micros - (i64::try_from(i).unwrap_or(0) * time_step);
        timestamps.push(ts);
        observed_timestamps.push(ts);
        ingested_timestamps.push(now_micros);
        severity_texts.push(Some("INFO"));
        bodies.push(Some(body_str.as_str()));
    }

    let tenant_id_arr: ArrayRef = Arc::new(StringArray::from(tenant_ids));
    let account_id_arr: ArrayRef = Arc::new(StringArray::from(account_ids));
    let service_name_arr: ArrayRef = Arc::new(StringArray::from(service_names));
    let timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(timestamps));
    let observed_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(observed_timestamps));
    let ingested_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(ingested_timestamps));
    let severity_text_arr: ArrayRef = Arc::new(StringArray::from(severity_texts));
    let body_arr: ArrayRef = Arc::new(StringArray::from(bodies));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    let attributes_field = arrow_schema
        .field_with_name("attributes")
        .expect("Schema must contain an 'attributes' field");
    let (key_field, value_field) = match attributes_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (fields[0].clone(), fields[1].clone()),
            _ => panic!("Expected Struct type for map entries"),
        },
        _ => panic!("Expected Map type for attributes field"),
    };

    let field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let key_builder = StringBuilder::new();
    let value_builder = StringBuilder::new();
    let mut attributes_builder = MapBuilder::new(Some(field_names), key_builder, value_builder)
        .with_keys_field(key_field)
        .with_values_field(value_field);

    for (latency, request_time, response_size) in &random_values {
        attributes_builder.keys().append_value("latency");
        attributes_builder.values().append_value(latency.to_string());
        attributes_builder.keys().append_value("request_time");
        attributes_builder.values().append_value(request_time.to_string());
        attributes_builder.keys().append_value("response_size");
        attributes_builder.values().append_value(response_size.to_string());
        attributes_builder.append(true)?;
    }

    let attributes_arr: ArrayRef = Arc::new(attributes_builder.finish());

    // Create hex-encoded trace and span IDs (matching logs schema)
    let mut trace_id_builder = StringBuilder::new();
    let mut span_id_builder = StringBuilder::new();

    for _ in 0..count {
        trace_id_builder.append_value("0102030405060708090a0b0c0d0e0f10");
        span_id_builder.append_value("0102030405060708");
    }

    let trace_id_arr: ArrayRef = Arc::new(trace_id_builder.finish());
    let span_id_arr: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id_arr,
            account_id_arr,
            service_name_arr,
            timestamp_arr,
            observed_timestamp_arr,
            ingested_timestamp_arr,
            trace_id_arr,
            span_id_arr,
            severity_text_arr,
            body_arr,
            attributes_arr,
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
    let mut account_ids = Vec::with_capacity(count);
    let mut service_names = Vec::with_capacity(count);
    let mut timestamps = Vec::with_capacity(count);
    let mut observed_timestamps = Vec::with_capacity(count);
    let mut ingested_timestamps = Vec::with_capacity(count);
    let mut severity_texts = Vec::with_capacity(count);
    let mut bodies = Vec::with_capacity(count);

    let body_strs: Vec<String> = (0..count).map(|i| format!("Request {} processed", i)).collect();

    for (i, body_str) in body_strs.iter().enumerate() {
        tenant_ids.push("test-tenant");
        account_ids.push(Some("acc-1"));
        service_names.push(Some("api"));
        let ts = now_micros - (i64::try_from(i).unwrap_or(0) * time_step);
        timestamps.push(ts);
        observed_timestamps.push(ts);
        ingested_timestamps.push(now_micros);
        severity_texts.push(Some("INFO"));
        bodies.push(Some(body_str.as_str()));
    }

    let tenant_id_arr: ArrayRef = Arc::new(StringArray::from(tenant_ids));
    let account_id_arr: ArrayRef = Arc::new(StringArray::from(account_ids));
    let service_name_arr: ArrayRef = Arc::new(StringArray::from(service_names));
    let timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(timestamps));
    let observed_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(observed_timestamps));
    let ingested_timestamp_arr: ArrayRef = Arc::new(TimestampMicrosecondArray::from(ingested_timestamps));
    let severity_text_arr: ArrayRef = Arc::new(StringArray::from(severity_texts));
    let body_arr: ArrayRef = Arc::new(StringArray::from(bodies));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    let attributes_field = arrow_schema
        .field_with_name("attributes")
        .expect("Schema must contain an 'attributes' field");
    let (key_field, value_field) = match attributes_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (fields[0].clone(), fields[1].clone()),
            _ => panic!("Expected Struct type for map entries"),
        },
        _ => panic!("Expected Map type for attributes field"),
    };

    let field_names = MapFieldNames {
        entry: "key_value".to_string(),
        key: "key".to_string(),
        value: "value".to_string(),
    };
    let key_builder = StringBuilder::new();
    let value_builder = StringBuilder::new();
    let mut attributes_builder = MapBuilder::new(Some(field_names), key_builder, value_builder)
        .with_keys_field(key_field)
        .with_values_field(value_field);

    for i in 0..count {
        let namespace = namespaces[i % namespaces.len()];
        let pod = pods[i % pods.len()];

        attributes_builder.keys().append_value("namespace");
        attributes_builder.values().append_value(namespace);
        attributes_builder.keys().append_value("pod");
        attributes_builder.values().append_value(pod);
        attributes_builder.append(true)?;
    }

    let attributes_arr: ArrayRef = Arc::new(attributes_builder.finish());

    // Create hex-encoded trace and span IDs (matching logs schema)
    let mut trace_id_builder = StringBuilder::new();
    let mut span_id_builder = StringBuilder::new();

    for _ in 0..count {
        trace_id_builder.append_value("0102030405060708090a0b0c0d0e0f10");
        span_id_builder.append_value("0102030405060708");
    }

    let trace_id_arr: ArrayRef = Arc::new(trace_id_builder.finish());
    let span_id_arr: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id_arr,
            account_id_arr,
            service_name_arr,
            timestamp_arr,
            observed_timestamp_arr,
            ingested_timestamp_arr,
            trace_id_arr,
            span_id_arr,
            severity_text_arr,
            body_arr,
            attributes_arr,
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
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
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
