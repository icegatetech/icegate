//! Integration tests for Loki API with `LogQL` support
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation
)] // Test code can use unwrap/expect and println

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
    spec::DataFileFormat,
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
            ParquetWriterBuilder,
        },
        IcebergWriter, IcebergWriterBuilder,
    },
    Catalog,
};
use icegate::{
    common::{catalog::CatalogBuilder, schema, CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, LOGS_TABLE},
    query::{
        engine::{QueryEngine, QueryEngineConfig},
        loki::LokiConfig,
    },
};
use reqwest::Client;
use serde_json::Value;
use tokio::time::{sleep, Duration};

/// Helper function to write test log data to an Iceberg table for a specific
/// tenant.
async fn write_test_logs_for_tenant(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    tenant_id: &str,
    service_name: &str,
    body_prefix: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    // Generate unique suffix based on tenant and timestamp to avoid file conflicts
    let unique_suffix = format!(
        "{}-{}",
        tenant_id,
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    );
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    // Build arrays for the logs schema
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

    // Create RecordBatch with Arrow schema from Iceberg schema
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    // Extract key and value field definitions from Iceberg schema
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
    attributes_builder.keys().append_value("tenant_marker");
    attributes_builder.values().append_value(tenant_id);
    attributes_builder.append(true)?;
    // Row 1
    attributes_builder.keys().append_value("tenant_marker");
    attributes_builder.values().append_value(tenant_id);
    attributes_builder.append(true)?;
    // Row 2
    attributes_builder.keys().append_value("tenant_marker");
    attributes_builder.values().append_value(tenant_id);
    attributes_builder.append(true)?;

    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

    // Build trace_id array (16 bytes)
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

    // Build span_id array (8 bytes)
    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    span_id_builder.append_value([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])?;
    span_id_builder.append_value([0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18])?;
    span_id_builder.append_value([0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28])?;
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
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
    ])?;

    // Create writers and write data with unique file name
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

    // Commit the data files using transaction
    let tx = Transaction::new(table);
    let action = tx.fast_append();
    let action = action.add_data_files(data_files);
    let tx = action.apply(Transaction::new(table))?;
    tx.commit(&**catalog).await?;

    Ok(())
}

/// Helper function to write test log data to an Iceberg table.
async fn write_test_logs(table: &Table, catalog: &Arc<dyn Catalog>) -> Result<(), Box<dyn std::error::Error>> {
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    // Build arrays for the logs schema
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

    // Create RecordBatch with Arrow schema from Iceberg schema
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    // Extract key and value field definitions from Iceberg schema (preserving
    // metadata)
    let attributes_field = arrow_schema.field(11); // attributes is at index 11
    let (key_field, value_field) = match attributes_field.data_type() {
        DataType::Map(entries_field, _) => match entries_field.data_type() {
            DataType::Struct(fields) => (fields[0].clone(), fields[1].clone()),
            _ => panic!("Expected Struct type for map entries"),
        },
        _ => panic!("Expected Map type for attributes field"),
    };

    // Configure MapBuilder with field names and metadata matching Iceberg schema
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
    // Row 0: user_id, request_id
    attributes_builder.keys().append_value("user_id");
    attributes_builder.values().append_value("user-123");
    attributes_builder.keys().append_value("request_id");
    attributes_builder.values().append_value("req-456");
    attributes_builder.append(true)?;
    // Row 1: page, latency_ms
    attributes_builder.keys().append_value("page");
    attributes_builder.values().append_value("/dashboard");
    attributes_builder.keys().append_value("latency_ms");
    attributes_builder.values().append_value("120");
    attributes_builder.append(true)?;
    // Row 2: db_host, query_time_ms
    attributes_builder.keys().append_value("db_host");
    attributes_builder.values().append_value("db-primary");
    attributes_builder.keys().append_value("query_time_ms");
    attributes_builder.values().append_value("250");
    attributes_builder.append(true)?;

    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

    // Build trace_id array (16 bytes for W3C trace ID)
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

    // Build span_id array (8 bytes for W3C span ID)
    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    span_id_builder.append_value([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08])?;
    span_id_builder.append_value([0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18])?;
    span_id_builder.append_value([0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28])?;
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
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
    ])?;

    // Create writers and write data
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

    // Commit the data files using transaction
    let tx = Transaction::new(table);
    let action = tx.fast_append();
    let action = action.add_data_files(data_files);
    let tx = action.apply(Transaction::new(table))?;
    tx.commit(&**catalog).await?;

    Ok(())
}

#[tokio::test]
async fn test_loki_query_range_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure catalog and Loki server
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
        port: 3101, // Use different port to avoid conflicts
    };

    // 2. Initialize catalog
    let catalog = CatalogBuilder::from_config(&catalog_config).await?;

    // 3. Create namespace and table using schema from common::schema
    let namespace_ident = iceberg::NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());

    if !catalog.namespace_exists(&namespace_ident).await? {
        catalog
            .create_namespace(&namespace_ident, std::collections::HashMap::new())
            .await?;
    }

    // Use logs schema from common::schema
    let schema = schema::logs_schema()?;

    let table_creation = iceberg::TableCreation::builder()
        .name(LOGS_TABLE.to_string())
        .schema(schema)
        .build();

    let _ = catalog.create_table(&namespace_ident, table_creation).await?;

    // 4. Insert test data into the logs table
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // 5. Start IceGate Server (reuse the same catalog to share data)
    let query_engine = Arc::new(
        QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
            .await
            .expect("Failed to create QueryEngine"),
    );
    let server_loki_config = loki_config.clone();
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let server_engine = Arc::clone(&query_engine);

    let server_handle = tokio::spawn(async move {
        icegate::query::loki::run(server_engine, server_loki_config, cancel_token_clone)
            .await
            .unwrap();
    });

    // Wait for server to start
    sleep(Duration::from_secs(2)).await;

    // 6. Query Loki API with tenant header
    let client = Client::new();
    let resp = client
        .get("http://127.0.0.1:3101/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"}")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);

    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "streams");

    // Result should NOT be empty since we inserted test data
    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "result should not be empty, got: {}", body);

    // Verify the stream structure
    let stream = &result[0];
    assert!(stream["stream"].is_object(), "stream should have labels");
    assert!(stream["values"].is_array(), "stream should have values");
    assert!(
        !stream["values"].as_array().unwrap().is_empty(),
        "stream should be non-empty"
    );

    // Verify attributes are present in stream labels
    let labels = &stream["stream"];
    assert_eq!(
        labels["service_name"], "frontend",
        "service_name label should be present"
    );
    // Check that at least one of our test attributes is present
    let has_attributes = labels["user_id"].is_string() && labels["request_id"].is_string()
        || labels["latency_ms"].is_string() && labels["page"].is_string();
    assert!(
        has_attributes,
        "stream should contain attributes from test data, got: {}",
        labels
    );

    // Verify stats structure is present
    assert!(
        body["data"]["stats"]["summary"]["execTime"].is_number(),
        "execTime should be present"
    );
    assert!(
        body["data"]["stats"]["summary"]["totalLinesProcessed"].as_u64().unwrap_or(0) > 0,
        "totalLinesProcessed should be > 0"
    );

    // Cleanup
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_loki_explain_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure catalog and Loki server
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
        port: 3102, // Use different port to avoid conflicts
    };

    // 2. Initialize catalog
    let catalog = CatalogBuilder::from_config(&catalog_config).await?;

    // 3. Create namespace and table using schema from common::schema
    let namespace_ident = iceberg::NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());

    if !catalog.namespace_exists(&namespace_ident).await? {
        catalog
            .create_namespace(&namespace_ident, std::collections::HashMap::new())
            .await?;
    }

    // Use logs schema from common::schema
    let schema = schema::logs_schema()?;

    let table_creation = iceberg::TableCreation::builder()
        .name(LOGS_TABLE.to_string())
        .schema(schema)
        .build();

    let _ = catalog.create_table(&namespace_ident, table_creation).await?;

    // 4. Insert test data into the logs table
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // 5. Start IceGate Server
    let query_engine = Arc::new(
        QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
            .await
            .expect("Failed to create QueryEngine"),
    );
    let server_loki_config = loki_config.clone();
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let server_engine = Arc::clone(&query_engine);

    let server_handle = tokio::spawn(async move {
        icegate::query::loki::run(server_engine, server_loki_config, cancel_token_clone)
            .await
            .unwrap();
    });

    // Wait for server to start
    sleep(Duration::from_secs(2)).await;

    let client = Client::new();

    // 6. Test explain endpoint with valid LogQL query (include tenant header)
    let resp = client
        .get("http://127.0.0.1:3102/loki/api/v1/explain")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"}")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["query"], "{service_name=\"frontend\"}");

    // Verify plan structure
    assert!(body["plans"].is_object(), "plans should be an object");
    assert!(body["plans"]["logical"].is_string(), "logical plan should be a string");
    assert!(
        body["plans"]["optimized"].is_string(),
        "optimized plan should be a string"
    );
    assert!(
        body["plans"]["physical"].is_string(),
        "physical plan should be a string"
    );

    // Verify plans contain expected content
    let logical = body["plans"]["logical"].as_str().unwrap();
    let optimized = body["plans"]["optimized"].as_str().unwrap();
    let physical = body["plans"]["physical"].as_str().unwrap();

    assert!(logical.contains("Filter"), "logical plan should contain Filter");
    assert!(!optimized.is_empty(), "optimized plan should not be empty");
    assert!(!physical.is_empty(), "physical plan should not be empty");

    // 7. Test explain endpoint with complex query (line filter)
    let resp = client
        .get("http://127.0.0.1:3102/loki/api/v1/explain")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"} |= \"error\"")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let logical = body["plans"]["logical"].as_str().unwrap();
    let optimized = body["plans"]["optimized"].as_str().unwrap();
    assert!(
        logical.contains("contains"),
        "logical plan should contain 'contains' for line filter"
    );
    assert!(!optimized.is_empty(), "optimized plan should not be empty");

    // 8. Test explain endpoint with invalid query
    let resp = client
        .get("http://127.0.0.1:3102/loki/api/v1/explain")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "invalid query syntax {")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 400, "Invalid query should return 400");
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "bad_data");
    assert!(body["error"].as_str().unwrap().contains("Parse error"));

    // Cleanup
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_loki_query_range_metric_response() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure catalog and Loki server
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
        port: 3103, // Use different port to avoid conflicts
    };

    // 2. Initialize catalog
    let catalog = CatalogBuilder::from_config(&catalog_config).await?;

    // 3. Create namespace and table
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

    // 4. Insert test data
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // 5. Start Loki Server
    let query_engine = Arc::new(
        QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
            .await
            .expect("Failed to create QueryEngine"),
    );
    let server_loki_config = loki_config.clone();
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let server_engine = Arc::clone(&query_engine);

    let server_handle = tokio::spawn(async move {
        icegate::query::loki::run(server_engine, server_loki_config, cancel_token_clone)
            .await
            .unwrap();
    });

    // Wait for server to start
    sleep(Duration::from_secs(2)).await;

    // 6. Query with metric expression (count_over_time)
    let client = Client::new();
    let resp = client
        .get("http://127.0.0.1:3103/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            ("query", "count_over_time({service_name=\"frontend\"}[5m])"),
            ("step", "60s"), // Use duration format with 's' suffix
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    // Verify metric response format
    assert_eq!(
        body["data"]["resultType"], "matrix",
        "Metric queries should return resultType 'matrix', got: {}",
        body["data"]["resultType"]
    );

    // Result should have metric key (not stream)
    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];

        // Metric queries use "metric" key for labels (not "stream")
        assert!(
            series["metric"].is_object(),
            "metric series should have 'metric' labels, got: {}",
            series
        );
        assert!(series["stream"].is_null(), "metric series should NOT have 'stream' key");

        // Values should be [timestamp, value] pairs
        let values = series["values"].as_array().expect("values should be an array");
        if !values.is_empty() {
            let sample = &values[0];
            let sample_array = sample.as_array().expect("sample should be an array");
            assert_eq!(
                sample_array.len(),
                2,
                "sample should have 2 elements [timestamp, value]"
            );

            // Timestamp should be a number (decimal seconds)
            assert!(
                sample_array[0].is_number(),
                "timestamp should be a number (decimal seconds), got: {}",
                sample_array[0]
            );

            // Value should be a string (numeric value as string)
            assert!(
                sample_array[1].is_string(),
                "value should be a string, got: {}",
                sample_array[1]
            );
        }
    }

    // Verify stats are present
    assert!(
        body["data"]["stats"]["summary"]["execTime"].is_number(),
        "execTime should be present"
    );

    // Cleanup
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_loki_query_range_bytes_over_time_metric() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure catalog and Loki server
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
        port: 3104, // Use different port to avoid conflicts
    };

    // 2. Initialize catalog
    let catalog = CatalogBuilder::from_config(&catalog_config).await?;

    // 3. Create namespace and table
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

    // 4. Insert test data
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // 5. Start Loki Server
    let query_engine = Arc::new(
        QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
            .await
            .expect("Failed to create QueryEngine"),
    );
    let server_loki_config = loki_config.clone();
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let server_engine = Arc::clone(&query_engine);

    let server_handle = tokio::spawn(async move {
        icegate::query::loki::run(server_engine, server_loki_config, cancel_token_clone)
            .await
            .unwrap();
    });

    // Wait for server to start
    sleep(Duration::from_secs(2)).await;

    // 6. Query with bytes_over_time metric expression
    let client = Client::new();
    let resp = client
        .get("http://127.0.0.1:3104/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "bytes_over_time({service_name=\"frontend\"}[5m])"), ("step", "60s")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["data"]["resultType"], "matrix",
        "bytes_over_time() should return matrix type"
    );

    // Verify matrix structure
    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        assert!(series["metric"].is_object(), "should have 'metric' key for labels");

        // Values should contain numeric byte counts
        let values = series["values"].as_array().expect("values should be an array");
        if !values.is_empty() {
            let sample = &values[0];
            let sample_array = sample.as_array().expect("sample should be an array");
            // Value should be parseable as a number (byte count)
            let value_str = sample_array[1].as_str().expect("value should be a string");
            assert!(
                value_str.parse::<f64>().is_ok(),
                "value should be a numeric string, got: {}",
                value_str
            );
        }
    }

    // Cleanup
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_loki_log_vs_metric_response_types() -> Result<(), Box<dyn std::error::Error>> {
    // This test verifies the difference between log and metric query responses
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
        port: 3105, // Use different port to avoid conflicts
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

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    let query_engine = Arc::new(
        QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
            .await
            .expect("Failed to create QueryEngine"),
    );
    let server_loki_config = loki_config.clone();
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let server_engine = Arc::clone(&query_engine);

    let server_handle = tokio::spawn(async move {
        icegate::query::loki::run(server_engine, server_loki_config, cancel_token_clone)
            .await
            .unwrap();
    });

    sleep(Duration::from_secs(2)).await;

    let client = Client::new();

    // Test 1: Log query should return "streams" with "stream" labels
    let log_resp = client
        .get("http://127.0.0.1:3105/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"}")])
        .send()
        .await?;

    let log_body: Value = log_resp.json().await?;
    assert_eq!(
        log_body["data"]["resultType"], "streams",
        "Log query should return 'streams'"
    );

    let log_result = log_body["data"]["result"].as_array().unwrap();
    if !log_result.is_empty() {
        assert!(
            log_result[0]["stream"].is_object(),
            "Log query should have 'stream' key"
        );
        assert!(
            log_result[0]["metric"].is_null(),
            "Log query should NOT have 'metric' key"
        );

        // Log timestamps are nanosecond strings
        let log_values = log_result[0]["values"].as_array().unwrap();
        if !log_values.is_empty() {
            assert!(
                log_values[0][0].is_string(),
                "Log timestamps should be strings (nanoseconds)"
            );
        }
    }

    // Test 2: Metric query should return "matrix" with "metric" labels
    let metric_resp = client
        .get("http://127.0.0.1:3105/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "count_over_time({service_name=\"frontend\"}[5m])"), ("step", "60s")])
        .send()
        .await?;

    let metric_body: Value = metric_resp.json().await?;
    assert_eq!(
        metric_body["data"]["resultType"], "matrix",
        "Metric query should return 'matrix'"
    );

    let metric_result = metric_body["data"]["result"].as_array().unwrap();
    if !metric_result.is_empty() {
        assert!(
            metric_result[0]["metric"].is_object(),
            "Metric query should have 'metric' key"
        );
        assert!(
            metric_result[0]["stream"].is_null(),
            "Metric query should NOT have 'stream' key"
        );

        // Metric timestamps are decimal seconds (numbers)
        let metric_values = metric_result[0]["values"].as_array().unwrap();
        if !metric_values.is_empty() {
            assert!(
                metric_values[0][0].is_number(),
                "Metric timestamps should be numbers (decimal seconds)"
            );
        }
    }

    // Cleanup
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_loki_query_range_rate() -> Result<(), Box<dyn std::error::Error>> {
    // Test rate() function - counts logs per second over a time range
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
        port: 3106,
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

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    let query_engine = Arc::new(
        QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
            .await
            .expect("Failed to create QueryEngine"),
    );
    let server_loki_config = loki_config.clone();
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let server_engine = Arc::clone(&query_engine);

    let server_handle = tokio::spawn(async move {
        icegate::query::loki::run(server_engine, server_loki_config, cancel_token_clone)
            .await
            .unwrap();
    });

    sleep(Duration::from_secs(2)).await;

    let client = Client::new();

    // Query with rate() - should return matrix format with per-second rate
    let resp = client
        .get("http://127.0.0.1:3106/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "rate({service_name=\"frontend\"}[5m])"), ("step", "60s")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["data"]["resultType"], "matrix",
        "rate() should return matrix type, got: {}",
        body["data"]["resultType"]
    );

    // Verify matrix structure
    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        assert!(series["metric"].is_object(), "should have 'metric' key for labels");

        // Values should be rate values (count / range_seconds)
        let values = series["values"].as_array().expect("values should be an array");
        if !values.is_empty() {
            let sample = &values[0];
            let sample_array = sample.as_array().expect("sample should be an array");

            // Timestamp should be decimal seconds
            assert!(
                sample_array[0].is_number(),
                "timestamp should be a number, got: {}",
                sample_array[0]
            );

            // Value should be a numeric string (rate per second)
            let value_str = sample_array[1].as_str().expect("value should be a string");
            let rate_value: f64 = value_str.parse().expect("value should be parseable as f64");
            assert!(
                rate_value >= 0.0,
                "rate value should be non-negative, got: {}",
                rate_value
            );
        }
    }

    // Cleanup
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    Ok(())
}

#[tokio::test]
async fn test_loki_query_range_bytes_rate() -> Result<(), Box<dyn std::error::Error>> {
    // Test bytes_rate() function - bytes per second over a time range
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
        port: 3107,
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

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    let query_engine = Arc::new(
        QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
            .await
            .expect("Failed to create QueryEngine"),
    );
    let server_loki_config = loki_config.clone();
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let server_engine = Arc::clone(&query_engine);

    let server_handle = tokio::spawn(async move {
        icegate::query::loki::run(server_engine, server_loki_config, cancel_token_clone)
            .await
            .unwrap();
    });

    sleep(Duration::from_secs(2)).await;

    let client = Client::new();

    // Query with bytes_rate() - should return matrix format with bytes per second
    let resp = client
        .get("http://127.0.0.1:3107/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "bytes_rate({service_name=\"frontend\"}[5m])"), ("step", "60s")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["data"]["resultType"], "matrix",
        "bytes_rate() should return matrix type, got: {}",
        body["data"]["resultType"]
    );

    // Verify matrix structure
    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        assert!(series["metric"].is_object(), "should have 'metric' key for labels");

        // Values should be byte rate values (sum(bytes) / range_seconds)
        let values = series["values"].as_array().expect("values should be an array");
        if !values.is_empty() {
            let sample = &values[0];
            let sample_array = sample.as_array().expect("sample should be an array");

            // Timestamp should be decimal seconds
            assert!(
                sample_array[0].is_number(),
                "timestamp should be a number, got: {}",
                sample_array[0]
            );

            // Value should be a numeric string (bytes per second)
            let value_str = sample_array[1].as_str().expect("value should be a string");
            let rate_value: f64 = value_str.parse().expect("value should be parseable as f64");
            assert!(
                rate_value >= 0.0,
                "bytes_rate value should be non-negative, got: {}",
                rate_value
            );
        }
    }

    // Cleanup
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    Ok(())
}

/// Test that tenant isolation is enforced - tenant A cannot see tenant B's
/// data.
#[tokio::test]
async fn test_tenant_isolation() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure catalog and Loki server
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
        port: 3108, // Use different port to avoid conflicts
    };

    // 2. Initialize catalog
    let catalog = CatalogBuilder::from_config(&catalog_config).await?;

    // 3. Create namespace and table
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

    // 4. Insert test data for TWO different tenants
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;

    // Write logs for tenant-alpha
    write_test_logs_for_tenant(&table, &catalog, "tenant-alpha", "alpha-service", "Alpha").await?;

    // Reload table to get updated metadata after first write
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;

    // Write logs for tenant-beta
    write_test_logs_for_tenant(&table, &catalog, "tenant-beta", "beta-service", "Beta").await?;

    // 5. Start IceGate Server
    let query_engine = Arc::new(
        QueryEngine::new(Arc::clone(&catalog), QueryEngineConfig::default())
            .await
            .expect("Failed to create QueryEngine"),
    );
    let server_loki_config = loki_config.clone();
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();
    let server_engine = Arc::clone(&query_engine);

    let server_handle = tokio::spawn(async move {
        icegate::query::loki::run(server_engine, server_loki_config, cancel_token_clone)
            .await
            .unwrap();
    });

    // Wait for server to start
    sleep(Duration::from_secs(2)).await;

    let client = Client::new();

    // 6. Query as tenant-alpha - should only see alpha's logs
    let resp_alpha = client
        .get("http://127.0.0.1:3108/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "tenant-alpha")
        .query(&[("query", "{service_name=\"alpha-service\"}")])
        .send()
        .await?;

    let status_alpha = resp_alpha.status();
    let body_alpha: Value = resp_alpha.json().await?;

    assert_eq!(status_alpha, 200, "Alpha query failed: {}", body_alpha);
    assert_eq!(body_alpha["status"], "success");

    let result_alpha = body_alpha["data"]["result"].as_array().expect("result should be an array");
    assert!(!result_alpha.is_empty(), "tenant-alpha should see their own logs");

    // Verify alpha only sees alpha-service logs
    for stream in result_alpha {
        let service = stream["stream"]["service_name"].as_str().unwrap_or("");
        assert_eq!(
            service, "alpha-service",
            "tenant-alpha should only see alpha-service, but saw: {}",
            service
        );

        // Also verify log bodies contain "Alpha" prefix
        let values = stream["values"].as_array().expect("values should be array");
        for value in values {
            let log_line = value[1].as_str().unwrap_or("");
            assert!(
                log_line.starts_with("Alpha"),
                "tenant-alpha should only see Alpha logs, but saw: {}",
                log_line
            );
        }
    }

    // 7. Query as tenant-beta - should only see beta's logs
    let resp_beta = client
        .get("http://127.0.0.1:3108/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "tenant-beta")
        .query(&[("query", "{service_name=\"beta-service\"}")])
        .send()
        .await?;

    let status_beta = resp_beta.status();
    let body_beta: Value = resp_beta.json().await?;

    assert_eq!(status_beta, 200, "Beta query failed: {}", body_beta);
    assert_eq!(body_beta["status"], "success");

    let result_beta = body_beta["data"]["result"].as_array().expect("result should be an array");
    assert!(!result_beta.is_empty(), "tenant-beta should see their own logs");

    // Verify beta only sees beta-service logs
    for stream in result_beta {
        let service = stream["stream"]["service_name"].as_str().unwrap_or("");
        assert_eq!(
            service, "beta-service",
            "tenant-beta should only see beta-service, but saw: {}",
            service
        );

        // Also verify log bodies contain "Beta" prefix
        let values = stream["values"].as_array().expect("values should be array");
        for value in values {
            let log_line = value[1].as_str().unwrap_or("");
            assert!(
                log_line.starts_with("Beta"),
                "tenant-beta should only see Beta logs, but saw: {}",
                log_line
            );
        }
    }

    // 8. Query as tenant-alpha but try to access beta's service - should see
    //    nothing
    // This tests cross-tenant isolation: even if you know another tenant's service
    // name, you shouldn't be able to see their data
    let resp_cross = client
        .get("http://127.0.0.1:3108/loki/api/v1/query_range")
        .header("X-Scope-OrgID", "tenant-alpha")
        .query(&[("query", "{service_name=\"beta-service\"}")])
        .send()
        .await?;

    let status_cross = resp_cross.status();
    let body_cross: Value = resp_cross.json().await?;

    assert_eq!(status_cross, 200, "Cross-tenant query failed: {}", body_cross);
    assert_eq!(body_cross["status"], "success");

    let result_cross = body_cross["data"]["result"].as_array().expect("result should be an array");
    assert!(
        result_cross.is_empty(),
        "tenant-alpha should NOT see beta-service logs (cross-tenant isolation), but saw: {}",
        body_cross
    );

    // Cleanup
    cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), server_handle).await;

    Ok(())
}
