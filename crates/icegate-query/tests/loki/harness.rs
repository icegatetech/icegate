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
            ArrayRef, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray,
        },
        datatypes::DataType,
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
use icegate_common::{CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, LOGS_TABLE, catalog::CatalogBuilder, schema};
use icegate_query::{
    engine::{QueryEngine, QueryEngineConfig},
    loki::LokiConfig,
};
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
        };

        // Use port 0 for ephemeral port assignment
        let loki_config = LokiConfig {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 0,
        };

        let catalog = CatalogBuilder::from_config(&catalog_config).await?;

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

        // Start server with port notification channel
        let query_engine = Arc::new(
            QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
                .await
                .expect("Failed to create QueryEngine"),
        );

        let cancel_token = CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();
        let server_engine = Arc::clone(&query_engine);

        // Create oneshot channel to receive the actual bound port
        let (port_tx, port_rx) = oneshot::channel::<u16>();

        let server_handle = tokio::spawn(async move {
            icegate_query::loki::run_with_port_tx(server_engine, loki_config, cancel_token_clone, Some(port_tx))
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
    let cloud_account_id: ArrayRef = Arc::new(StringArray::from(vec![Some("acc-1"), Some("acc-1"), Some("acc-1")]));
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

    let attributes_field = arrow_schema.field(10);
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

    // Test data trace_id and span_id values as hex strings
    let trace_ids = [
        "0102030405060708090a0b0c0d0e0f10",
        "1112131415161718191a1b1c1d1e1f20",
        "2122232425262728292a2b2c2d2e2f30",
    ];
    let span_ids = ["0102030405060708", "1112131415161718", "2122232425262728"];
    let severity_texts = ["INFO", "WARN", "ERROR"];
    let bodies = [
        format!("{} message 1", body_prefix),
        format!("{} message 2", body_prefix),
        format!("{} message 3", body_prefix),
    ];

    for i in 0..3 {
        // tenant_marker
        attributes_builder.keys().append_value("tenant_marker");
        attributes_builder.values().append_value(tenant_id);
        // service_name (duplicate indexed column into attributes)
        attributes_builder.keys().append_value("service_name");
        attributes_builder.values().append_value(service_name);
        // cloud_account_id (duplicate indexed column into attributes)
        attributes_builder.keys().append_value("cloud_account_id");
        attributes_builder.values().append_value("acc-1");
        // trace_id (duplicate indexed column into attributes)
        attributes_builder.keys().append_value("trace_id");
        attributes_builder.values().append_value(trace_ids[i]);
        // span_id (duplicate indexed column into attributes)
        attributes_builder.keys().append_value("span_id");
        attributes_builder.values().append_value(span_ids[i]);
        // severity_text (duplicate indexed column into attributes)
        attributes_builder.keys().append_value("severity_text");
        attributes_builder.values().append_value(severity_texts[i]);
        // body (duplicate indexed column into attributes)
        attributes_builder.keys().append_value("body");
        attributes_builder.values().append_value(bodies[i].as_str());
        attributes_builder.append(true)?;
    }

    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

    let mut trace_id_builder = StringBuilder::new();
    // Hex-encoded trace IDs (16 bytes → 32 hex chars)
    for tid in trace_ids {
        trace_id_builder.append_value(tid);
    }
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    let mut span_id_builder = StringBuilder::new();
    // Hex-encoded span IDs (8 bytes → 16 hex chars)
    for sid in span_ids {
        span_id_builder.append_value(sid);
    }
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id_arr,
            cloud_account_id,
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
    let cloud_account_id: ArrayRef = Arc::new(StringArray::from(vec![Some("acc-1"), Some("acc-1"), Some("acc-1")]));
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

    let attributes_field = arrow_schema.field(10);
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

    // Test data trace_id and span_id values as hex strings
    let trace_ids = [
        "0102030405060708090a0b0c0d0e0f10",
        "1112131415161718191a1b1c1d1e1f20",
        "2122232425262728292a2b2c2d2e2f30",
    ];
    let span_ids = ["0102030405060708", "1112131415161718", "2122232425262728"];
    let severity_texts = ["INFO", "WARN", "ERROR"];
    let bodies = [
        "User logged in successfully",
        "Page rendered in 120ms",
        "Database connection slow",
    ];

    // Row 0
    attributes_builder.keys().append_value("user_id");
    attributes_builder.values().append_value("user-123");
    attributes_builder.keys().append_value("request_id");
    attributes_builder.values().append_value("req-456");
    // Duplicate indexed columns into attributes
    attributes_builder.keys().append_value("service_name");
    attributes_builder.values().append_value("frontend");
    attributes_builder.keys().append_value("cloud_account_id");
    attributes_builder.values().append_value("acc-1");
    attributes_builder.keys().append_value("trace_id");
    attributes_builder.values().append_value(trace_ids[0]);
    attributes_builder.keys().append_value("span_id");
    attributes_builder.values().append_value(span_ids[0]);
    attributes_builder.keys().append_value("severity_text");
    attributes_builder.values().append_value(severity_texts[0]);
    attributes_builder.keys().append_value("body");
    attributes_builder.values().append_value(bodies[0]);
    attributes_builder.append(true)?;
    // Row 1
    attributes_builder.keys().append_value("page");
    attributes_builder.values().append_value("/dashboard");
    attributes_builder.keys().append_value("latency_ms");
    attributes_builder.values().append_value("120");
    // Duplicate indexed columns into attributes
    attributes_builder.keys().append_value("service_name");
    attributes_builder.values().append_value("frontend");
    attributes_builder.keys().append_value("cloud_account_id");
    attributes_builder.values().append_value("acc-1");
    attributes_builder.keys().append_value("trace_id");
    attributes_builder.values().append_value(trace_ids[1]);
    attributes_builder.keys().append_value("span_id");
    attributes_builder.values().append_value(span_ids[1]);
    attributes_builder.keys().append_value("severity_text");
    attributes_builder.values().append_value(severity_texts[1]);
    attributes_builder.keys().append_value("body");
    attributes_builder.values().append_value(bodies[1]);
    attributes_builder.append(true)?;
    // Row 2
    attributes_builder.keys().append_value("db_host");
    attributes_builder.values().append_value("db-primary");
    attributes_builder.keys().append_value("query_time_ms");
    attributes_builder.values().append_value("250");
    // Duplicate indexed columns into attributes
    attributes_builder.keys().append_value("service_name");
    attributes_builder.values().append_value("backend");
    attributes_builder.keys().append_value("cloud_account_id");
    attributes_builder.values().append_value("acc-1");
    attributes_builder.keys().append_value("trace_id");
    attributes_builder.values().append_value(trace_ids[2]);
    attributes_builder.keys().append_value("span_id");
    attributes_builder.values().append_value(span_ids[2]);
    attributes_builder.keys().append_value("severity_text");
    attributes_builder.values().append_value(severity_texts[2]);
    attributes_builder.keys().append_value("body");
    attributes_builder.values().append_value(bodies[2]);
    attributes_builder.append(true)?;

    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

    let mut trace_id_builder = StringBuilder::new();
    // Hex-encoded trace IDs (16 bytes → 32 hex chars)
    for tid in trace_ids {
        trace_id_builder.append_value(tid);
    }
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    let mut span_id_builder = StringBuilder::new();
    // Hex-encoded span IDs (8 bytes → 16 hex chars)
    for sid in span_ids {
        span_id_builder.append_value(sid);
    }
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id,
            cloud_account_id,
            service_name,
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
