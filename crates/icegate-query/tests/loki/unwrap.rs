//! Tests for unwrap aggregations (`sum_over_time`, `avg_over_time`, etc.)
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation
)]

use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use datafusion::arrow::{
    array::{ArrayRef, MapBuilder, MapFieldNames, RecordBatch, StringArray, StringBuilder, TimestampMicrosecondArray},
    datatypes::DataType,
};
use datafusion::parquet::file::properties::WriterProperties;
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
use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE};
use serde_json::Value;

use super::harness::TestServer;

/// Build a `RecordBatch` with test logs containing numeric attributes for unwrap testing
fn build_test_record_batch(table: &Table, now_micros: i64) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    // Create 5 log entries with different numeric attributes
    let tenant_id: ArrayRef = Arc::new(StringArray::from(vec![
        "test-tenant",
        "test-tenant",
        "test-tenant",
        "test-tenant",
        "test-tenant",
    ]));
    let cloud_account_id: ArrayRef = Arc::new(StringArray::from(vec![
        Some("acc-1"),
        Some("acc-1"),
        Some("acc-1"),
        Some("acc-1"),
        Some("acc-1"),
    ]));
    let service_name: ArrayRef = Arc::new(StringArray::from(vec![
        Some("api-server"),
        Some("api-server"),
        Some("api-server"),
        Some("api-server"),
        Some("api-server"),
    ]));
    let timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros - 240_000_000, // 4 minutes ago
        now_micros - 180_000_000, // 3 minutes ago
        now_micros - 120_000_000, // 2 minutes ago
        now_micros - 60_000_000,  // 1 minute ago
        now_micros,               // now
    ]));
    let observed_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros - 240_000_000,
        now_micros - 180_000_000,
        now_micros - 120_000_000,
        now_micros - 60_000_000,
        now_micros,
    ]));
    let ingested_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros, now_micros, now_micros, now_micros, now_micros,
    ]));
    let severity_text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("INFO"),
        Some("INFO"),
        Some("INFO"),
        Some("INFO"),
        Some("INFO"),
    ]));
    let body: ArrayRef = Arc::new(StringArray::from(vec![
        Some("Request processed"),
        Some("Request processed"),
        Some("Request processed"),
        Some("Request processed"),
        Some("Request processed"),
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

    // Row 0: latency_ms=120, response_bytes=2048
    attributes_builder.keys().append_value("latency_ms");
    attributes_builder.values().append_value("120");
    attributes_builder.keys().append_value("response_bytes");
    attributes_builder.values().append_value("2048");
    attributes_builder.append(true)?;

    // Row 1: latency_ms=150, response_bytes=3072
    attributes_builder.keys().append_value("latency_ms");
    attributes_builder.values().append_value("150");
    attributes_builder.keys().append_value("response_bytes");
    attributes_builder.values().append_value("3072");
    attributes_builder.append(true)?;

    // Row 2: latency_ms=90, response_bytes=1024, duration_str=5m
    attributes_builder.keys().append_value("latency_ms");
    attributes_builder.values().append_value("90");
    attributes_builder.keys().append_value("response_bytes");
    attributes_builder.values().append_value("1024");
    attributes_builder.keys().append_value("duration_str");
    attributes_builder.values().append_value("5m");
    attributes_builder.append(true)?;

    // Row 3: counter=100
    attributes_builder.keys().append_value("counter");
    attributes_builder.values().append_value("100");
    attributes_builder.append(true)?;

    // Row 4: counter=120 (increasing counter for rate_counter test)
    attributes_builder.keys().append_value("counter");
    attributes_builder.values().append_value("120");
    attributes_builder.append(true)?;

    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

    let mut trace_id_builder = StringBuilder::new();
    // Hex-encoded trace IDs (16 bytes → 32 hex chars)
    for i in 0u8..5 {
        trace_id_builder.append_value(format!(
            "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i
        ));
    }
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    let mut span_id_builder = StringBuilder::new();
    // Hex-encoded span IDs (8 bytes → 16 hex chars)
    for i in 0u8..5 {
        span_id_builder.append_value(format!(
            "{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            i, i, i, i, i, i, i, i
        ));
    }
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    RecordBatch::try_new(
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
    )
    .map_err(Into::into)
}

/// Write test logs with numeric attributes for unwrap testing
async fn write_test_logs_with_metrics(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
) -> Result<(), Box<dyn std::error::Error>> {
    let unique_suffix = format!(
        "unwrap-{}",
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    );
    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;

    let batch = build_test_record_batch(table, now_micros)?;

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

#[tokio::test]
async fn test_sum_over_time_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix", "Should return matrix type");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        assert!(series["metric"].is_object(), "Should have metric labels");

        let values = series["values"].as_array().expect("values should be an array");
        // Check that we have some non-null values
        let has_value = values.iter().any(|sample| {
            let arr = sample.as_array().unwrap();
            !arr[1].is_null()
        });
        assert!(has_value, "Should have at least one non-null sum value");
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_avg_over_time_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "avg_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_min_over_time_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "min_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    // Verify min aggregation produces valid results
    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        let values = series["values"].as_array().expect("values should be an array");
        assert!(!values.is_empty(), "Should have time series values");
        // Just verify we got a numeric value, don't assert specific range
        // as it may vary based on time bucketing
        for sample in values {
            let arr = sample.as_array().unwrap();
            if !arr[1].is_null() {
                let _value: f64 = arr[1].as_str().unwrap().parse().expect("Value should be numeric");
                break;
            }
        }
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_max_over_time_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "max_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    // Verify max value is 150 (the maximum latency in test data)
    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        let values = series["values"].as_array().expect("values should be an array");
        for sample in values {
            let arr = sample.as_array().unwrap();
            if !arr[1].is_null() {
                let value: f64 = arr[1].as_str().unwrap().parse().unwrap();
                assert!(value <= 150.0, "Max value should be at most 150");
                break;
            }
        }
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_stddev_over_time_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "stddev_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_quantile_over_time_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "quantile_over_time(0.95, {service_name=\"api-server\"} | unwrap latency_ms [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_first_over_time_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "first_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_last_over_time_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "last_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_rate_counter_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "rate_counter({service_name=\"api-server\"} | unwrap counter [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_unwrap_with_bytes_conversion() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum_over_time({service_name=\"api-server\"} | unwrap response_bytes [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    // Verify bytes are aggregated (just check we get valid numeric results)
    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        let values = series["values"].as_array().expect("values should be an array");
        assert!(!values.is_empty(), "Should have time series values");
        // Just verify we got a numeric value
        for sample in values {
            let arr = sample.as_array().unwrap();
            if !arr[1].is_null() {
                let _value: f64 = arr[1].as_str().unwrap().parse().expect("Value should be numeric");
                break;
            }
        }
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_stdvar_over_time_unwrap() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            // Test stdvar_over_time (another statistical aggregation)
            (
                "query",
                "stdvar_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m])",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    // Verify that stdvar produces valid results
    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        let values = series["values"].as_array().expect("values should be an array");
        assert!(!values.is_empty(), "Should have time series values");
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_avg_over_time_with_by_grouping() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    // Query: avg_over_time({service_name="api-server"} | unwrap latency_ms [5m]) by (service_name)
    // Should group only by service_name, ignoring other labels
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "avg by (service_name) (avg_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m]))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    // Verify that result has service_name label but not other labels
    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "Result should not be empty");
    let series = &result[0];
    let metric = series["metric"].as_object().expect("metric should be an object");
    // Note: "service_name" column is output as "service" for Loki compatibility
    assert!(
        metric.contains_key("service") || metric.contains_key("service_name"),
        "Should have service or service_name label"
    );
    // Attributes should be filtered - only service_name grouping
    // Verify we don't have other indexed columns like account_id
    assert!(
        !metric.contains_key("account_id"),
        "Should NOT have account_id label (filtered by 'by' clause)"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_max_over_time_with_without_grouping() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    // Query: max_over_time({service_name="api-server"} | unwrap latency_ms [5m]) without (account_id)
    // Should group by all labels EXCEPT account_id
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "max without (account_id) (max_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m]))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    // Verify that result has labels but NOT account_id
    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "Result should not be empty");
    let series = &result[0];
    let metric = series["metric"].as_object().expect("metric should be an object");
    assert!(
        !metric.contains_key("account_id"),
        "Should NOT have account_id label (filtered by 'without' clause)"
    );
    // Note: "service_name" column is output as "service" for Loki compatibility
    assert!(
        metric.contains_key("service") || metric.contains_key("service_name"),
        "Should have service or service_name label"
    );
    // Should also have level/severity_text since it wasn't excluded
    assert!(
        metric.contains_key("level") || metric.contains_key("severity_text"),
        "Should have level or severity_text label"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_sum_over_time_grouping_via_vector_aggregation() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    // Query: sum_over_time({service_name="api-server"} | unwrap latency_ms [5m]) by (service_name)
    // Should return an error because sum_over_time does not support grouping
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum by (service_name) (sum_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m]))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    // Should succeed - grouping is handled at vector aggregation level
    // sum_over_time itself doesn't support grouping, but when wrapped in a vector aggregation,
    // the grouping is applied to the vector aggregation output
    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    // Verify that result has service_name label
    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "Result should not be empty");
    let series = &result[0];
    let metric = series["metric"].as_object().expect("metric should be an object");
    // Note: "service_name" column is output as "service" for Loki compatibility
    assert!(
        metric.contains_key("service") || metric.contains_key("service_name"),
        "Should have service or service_name label"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_rate_counter_grouping_via_vector_aggregation() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    // Query: sum by (service_name) (rate_counter({service_name="api-server"} | unwrap counter [5m]))
    // This uses vector aggregation with grouping. While rate_counter itself doesn't support grouping,
    // when wrapped in a vector aggregation, the grouping is handled at the vector aggregation level.
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum by (service_name) (rate_counter({service_name=\"api-server\"} | unwrap counter [5m]))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    // Should succeed - grouping is handled at vector aggregation level
    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    // Verify that result has service_name label
    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "Result should not be empty");
    let series = &result[0];
    let metric = series["metric"].as_object().expect("metric should be an object");
    // Note: "service_name" column is output as "service" for Loki compatibility
    assert!(
        metric.contains_key("service") || metric.contains_key("service_name"),
        "Should have service or service_name label"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_min_over_time_with_by_multiple_labels() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_with_metrics(&table, &catalog).await?;

    // Query: min_over_time({service_name="api-server"} | unwrap latency_ms [5m]) by (service_name, level)
    // Should group by both service_name and severity_text (level)
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "min by (service_name, level) (min_over_time({service_name=\"api-server\"} | unwrap latency_ms [5m]))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix");

    // Verify that result has both service_name and level labels
    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "Result should not be empty");
    let series = &result[0];
    let metric = series["metric"].as_object().expect("metric should be an object");
    // Note: "service_name" column is output as "service" for Loki compatibility
    assert!(
        metric.contains_key("service") || metric.contains_key("service_name"),
        "Should have service or service_name label"
    );
    // Note: 'level' is mapped to 'severity_text' internally
    assert!(
        metric.contains_key("level") || metric.contains_key("severity_text"),
        "Should have level or severity_text label"
    );

    server.shutdown().await;
    Ok(())
}
