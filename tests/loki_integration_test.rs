//! Integration tests for Loki API with LogQL support
#![allow(clippy::unwrap_used, clippy::expect_used)] // Test code can use unwrap/expect

use datafusion::arrow::array::{ArrayRef, FixedSizeBinaryBuilder, Int32Array, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::parquet::file::properties::WriterProperties;
use iceberg::spec::DataFileFormat;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::table::Table;
use iceberg::Catalog;
use icegate::common::catalog::CatalogBuilder;
use icegate::common::schema;
use icegate::common::{CatalogConfig, CatalogBackend, ICEGATE_NAMESPACE, LOGS_TABLE};
use icegate::query::engine::{QueryEngine, QueryEngineConfig};
use icegate::query::loki::LokiConfig;
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

/// Helper function to write test log data to an Iceberg table.
async fn write_test_logs(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
) -> Result<(), Box<dyn std::error::Error>> {
    let now_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;

    // Build arrays for the logs schema
    let tenant_id: ArrayRef = Arc::new(StringArray::from(vec!["test-tenant", "test-tenant", "test-tenant"]));
    let account_id: ArrayRef = Arc::new(StringArray::from(vec![Some("acc-1"), Some("acc-1"), Some("acc-1")]));
    let service_name: ArrayRef = Arc::new(StringArray::from(vec![Some("frontend"), Some("frontend"), Some("backend")]));
    let timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![now_micros, now_micros - 1000, now_micros - 2000]));
    let observed_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![now_micros, now_micros - 1000, now_micros - 2000]));
    let ingested_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![now_micros, now_micros, now_micros]));
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
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())?);

    // Extract key and value field definitions from Iceberg schema (preserving metadata)
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
    trace_id_builder.append_value([0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                                   0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10])?;
    trace_id_builder.append_value([0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
                                   0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20])?;
    trace_id_builder.append_value([0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
                                   0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30])?;
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    // Build span_id array (8 bytes for W3C span ID)
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

    // Create writers and write data
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new("data".to_string(), None, DataFileFormat::Parquet);

    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
        None,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    // Use the table's default partition spec id (0 for unpartitioned tables)
    let partition_spec_id = table.metadata().default_partition_spec().spec_id();
    let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, partition_spec_id);
    let mut data_file_writer = data_file_writer_builder.build().await?;

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
        properties: Default::default(),
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
        icegate::query::loki::run(
            server_engine,
            server_loki_config,
            cancel_token_clone,
        )
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
    assert!(!stream["values"].as_array().unwrap().is_empty(), "stream should be non-empty");

    // Verify attributes are present in stream labels
    let labels = &stream["stream"];
    assert_eq!(labels["service_name"], "frontend", "service_name label should be present");
    // Check that at least one of our test attributes is present
    let has_attributes =
        labels["user_id"].is_string() && labels["request_id"].is_string()
        || labels["latency_ms"].is_string() && labels["page"].is_string();
    assert!(has_attributes, "stream should contain attributes from test data, got: {}", labels);

    // Verify stats structure is present
    assert!(body["data"]["stats"]["summary"]["execTime"].is_number(), "execTime should be present");
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
        properties: Default::default(),
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
        icegate::query::loki::run(
            server_engine,
            server_loki_config,
            cancel_token_clone,
        )
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
    assert!(body["plans"]["optimized"].is_string(), "optimized plan should be a string");
    assert!(body["plans"]["physical"].is_string(), "physical plan should be a string");

    // Verify plans contain expected content
    let logical = body["plans"]["logical"].as_str().unwrap();
    let optimized = body["plans"]["optimized"].as_str().unwrap();
    let physical = body["plans"]["physical"].as_str().unwrap();

    assert!(logical.contains("Filter"), "logical plan should contain Filter");
    assert!(optimized.len() > 0, "optimized plan should not be empty");
    assert!(physical.len() > 0, "physical plan should not be empty");

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
    assert!(logical.contains("contains"), "logical plan should contain 'contains' for line filter");
    assert!(optimized.len() > 0, "optimized plan should not be empty");

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
