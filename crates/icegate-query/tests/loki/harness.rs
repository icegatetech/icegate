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
            ArrayRef, FixedSizeBinaryBuilder, Int32Array, MapBuilder, MapFieldNames, RecordBatch, StringArray,
            StringBuilder, TimestampMicrosecondArray,
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
    let account_id: ArrayRef = Arc::new(StringArray::from(vec![Some("acc-1"), Some("acc-1"), Some("acc-1")]));
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
    let severity_number: ArrayRef = Arc::new(Int32Array::from(vec![Some(9), Some(9), Some(13)]));
    let severity_text: ArrayRef = Arc::new(StringArray::from(vec![Some("INFO"), Some("INFO"), Some("WARN")]));
    let body: ArrayRef = Arc::new(StringArray::from(vec![
        Some(format!("{} message 1", body_prefix)),
        Some(format!("{} message 2", body_prefix)),
        Some(format!("{} message 3", body_prefix)),
    ]));
    let flags: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>, None, None]));
    let dropped_attributes_count: ArrayRef = Arc::new(Int32Array::from(vec![0, 0, 0]));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    let attributes_field = arrow_schema.field(11);
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

    for _ in 0..3 {
        attributes_builder.keys().append_value("tenant_marker");
        attributes_builder.values().append_value(tenant_id);
        attributes_builder.append(true)?;
    }

    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

    let mut trace_id_builder = FixedSizeBinaryBuilder::new(16);
    trace_id_builder.append_value([
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    ])?;
    trace_id_builder.append_value([
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
    ])?;
    trace_id_builder.append_value([
        0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
    ])?;
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    span_id_builder.append_value([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])?;
    span_id_builder.append_value([0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18])?;
    span_id_builder.append_value([0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28])?;
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id_arr,
            account_id,
            service_name_arr,
            timestamp,
            observed_timestamp,
            ingested_timestamp,
            trace_id,
            span_id,
            severity_number,
            severity_text,
            body,
            attributes,
            flags,
            dropped_attributes_count,
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
pub async fn write_test_logs(table: &Table, catalog: &Arc<dyn Catalog>) -> Result<(), Box<dyn std::error::Error>> {
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    let tenant_id: ArrayRef = Arc::new(StringArray::from(vec!["test-tenant", "test-tenant", "test-tenant"]));
    let account_id: ArrayRef = Arc::new(StringArray::from(vec![Some("acc-1"), Some("acc-1"), Some("acc-1")]));
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
    let severity_number: ArrayRef = Arc::new(Int32Array::from(vec![Some(9), Some(9), Some(13)]));
    let severity_text: ArrayRef = Arc::new(StringArray::from(vec![Some("INFO"), Some("INFO"), Some("WARN")]));
    let body: ArrayRef = Arc::new(StringArray::from(vec![
        Some("User logged in successfully"),
        Some("Page rendered in 120ms"),
        Some("Database connection slow"),
    ]));
    let flags: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>, None, None]));
    let dropped_attributes_count: ArrayRef = Arc::new(Int32Array::from(vec![0, 0, 0]));

    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    let attributes_field = arrow_schema.field(11);
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

    // Row 0
    attributes_builder.keys().append_value("user_id");
    attributes_builder.values().append_value("user-123");
    attributes_builder.keys().append_value("request_id");
    attributes_builder.values().append_value("req-456");
    attributes_builder.append(true)?;
    // Row 1
    attributes_builder.keys().append_value("page");
    attributes_builder.values().append_value("/dashboard");
    attributes_builder.keys().append_value("latency_ms");
    attributes_builder.values().append_value("120");
    attributes_builder.append(true)?;
    // Row 2
    attributes_builder.keys().append_value("db_host");
    attributes_builder.values().append_value("db-primary");
    attributes_builder.keys().append_value("query_time_ms");
    attributes_builder.values().append_value("250");
    attributes_builder.append(true)?;

    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

    let mut trace_id_builder = FixedSizeBinaryBuilder::new(16);
    trace_id_builder.append_value([
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
    ])?;
    trace_id_builder.append_value([
        0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
    ])?;
    trace_id_builder.append_value([
        0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30,
    ])?;
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    span_id_builder.append_value([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])?;
    span_id_builder.append_value([0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18])?;
    span_id_builder.append_value([0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28])?;
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id,
            account_id,
            service_name,
            timestamp,
            observed_timestamp,
            ingested_timestamp,
            trace_id,
            span_id,
            severity_number,
            severity_text,
            body,
            attributes,
            flags,
            dropped_attributes_count,
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
