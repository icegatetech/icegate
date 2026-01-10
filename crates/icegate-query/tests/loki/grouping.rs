//! Tests for cross-type grouping merge semantics
//!
//! Validates the behavior when combining By and Without groupings in nested aggregations.
#![allow(
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;

use datafusion::arrow::{
    array::{
        ArrayRef, FixedSizeBinaryBuilder, Int32Array, MapBuilder, MapFieldNames, RecordBatch, StringArray,
        StringBuilder, TimestampMicrosecondArray,
    },
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

/// Build a `RecordBatch` with test logs containing multiple labels for grouping tests
#[allow(clippy::too_many_lines)]
fn build_grouping_test_record_batch(table: &Table, now_micros: i64) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    // Create 6 log entries with different label combinations
    let tenant_id: ArrayRef = Arc::new(StringArray::from(vec![
        "test-tenant",
        "test-tenant",
        "test-tenant",
        "test-tenant",
        "test-tenant",
        "test-tenant",
    ]));
    let account_id: ArrayRef = Arc::new(StringArray::from(vec![
        Some("acc-1"),
        Some("acc-1"),
        Some("acc-1"),
        Some("acc-1"),
        Some("acc-1"),
        Some("acc-1"),
    ]));
    let service_name: ArrayRef = Arc::new(StringArray::from(vec![
        Some("api"),
        Some("api"),
        Some("api"),
        Some("backend"),
        Some("backend"),
        Some("backend"),
    ]));
    let timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros,
        now_micros - 1000,
        now_micros - 2000,
        now_micros - 3000,
        now_micros - 4000,
        now_micros - 5000,
    ]));
    let observed_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros,
        now_micros - 1000,
        now_micros - 2000,
        now_micros - 3000,
        now_micros - 4000,
        now_micros - 5000,
    ]));
    let ingested_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros, now_micros, now_micros, now_micros, now_micros, now_micros,
    ]));
    let severity_number: ArrayRef = Arc::new(Int32Array::from(vec![
        Some(9),
        Some(9),
        Some(9),
        Some(9),
        Some(9),
        Some(9),
    ]));
    let severity_text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("INFO"),
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
        Some("Task completed"),
        Some("Task completed"),
        Some("Task completed"),
    ]));
    let flags: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>, None, None, None, None, None]));
    let dropped_attributes_count: ArrayRef = Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0, 0]));

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

    // Row 0: api service, node=node1, pod=pod-a, instance=inst1, value=100
    attributes_builder.keys().append_value("node");
    attributes_builder.values().append_value("node1");
    attributes_builder.keys().append_value("pod");
    attributes_builder.values().append_value("pod-a");
    attributes_builder.keys().append_value("instance");
    attributes_builder.values().append_value("inst1");
    attributes_builder.keys().append_value("value");
    attributes_builder.values().append_value("100");
    attributes_builder.append(true)?;

    // Row 1: api service, node=node1, pod=pod-b, instance=inst2, value=150
    attributes_builder.keys().append_value("node");
    attributes_builder.values().append_value("node1");
    attributes_builder.keys().append_value("pod");
    attributes_builder.values().append_value("pod-b");
    attributes_builder.keys().append_value("instance");
    attributes_builder.values().append_value("inst2");
    attributes_builder.keys().append_value("value");
    attributes_builder.values().append_value("150");
    attributes_builder.append(true)?;

    // Row 2: api service, node=node2, pod=pod-c, instance=inst3, value=200
    attributes_builder.keys().append_value("node");
    attributes_builder.values().append_value("node2");
    attributes_builder.keys().append_value("pod");
    attributes_builder.values().append_value("pod-c");
    attributes_builder.keys().append_value("instance");
    attributes_builder.values().append_value("inst3");
    attributes_builder.keys().append_value("value");
    attributes_builder.values().append_value("200");
    attributes_builder.append(true)?;

    // Row 3: backend service, node=node1, pod=pod-d, instance=inst4, value=120
    attributes_builder.keys().append_value("node");
    attributes_builder.values().append_value("node1");
    attributes_builder.keys().append_value("pod");
    attributes_builder.values().append_value("pod-d");
    attributes_builder.keys().append_value("instance");
    attributes_builder.values().append_value("inst4");
    attributes_builder.keys().append_value("value");
    attributes_builder.values().append_value("120");
    attributes_builder.append(true)?;

    // Row 4: backend service, node=node2, pod=pod-e, instance=inst5, value=180
    attributes_builder.keys().append_value("node");
    attributes_builder.values().append_value("node2");
    attributes_builder.keys().append_value("pod");
    attributes_builder.values().append_value("pod-e");
    attributes_builder.keys().append_value("instance");
    attributes_builder.values().append_value("inst5");
    attributes_builder.keys().append_value("value");
    attributes_builder.values().append_value("180");
    attributes_builder.append(true)?;

    // Row 5: backend service, node=node2, pod=pod-f, instance=inst6, value=220
    attributes_builder.keys().append_value("node");
    attributes_builder.values().append_value("node2");
    attributes_builder.keys().append_value("pod");
    attributes_builder.values().append_value("pod-f");
    attributes_builder.keys().append_value("instance");
    attributes_builder.values().append_value("inst6");
    attributes_builder.keys().append_value("value");
    attributes_builder.values().append_value("220");
    attributes_builder.append(true)?;

    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

    let mut trace_id_builder = FixedSizeBinaryBuilder::new(16);
    for i in 0..6 {
        trace_id_builder.append_value([i, i, i, i, i, i, i, i, i, i, i, i, i, i, i, i])?;
    }
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    for i in 0..6 {
        span_id_builder.append_value([i, i, i, i, i, i, i, i])?;
    }
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    RecordBatch::try_new(
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
    )
    .map_err(Into::into)
}

/// Write test logs with multiple labels for grouping tests
async fn write_grouping_test_logs(table: &Table, catalog: &Arc<dyn Catalog>) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let unique_suffix = format!("grouping-{}", SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos());
    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as i64;

    let batch = build_grouping_test_record_batch(table, now_micros)?;

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
async fn test_cross_type_grouping_by_inner_without_outer() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3250).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_grouping_test_logs(&table, &catalog).await?;

    // Query: sum without (pod) (avg_over_time({service_name="api"} | unwrap value [1m]) by (node))
    // Expected behavior: inner By(node) is discarded, outer Without(pod) is used
    // Result should group by all labels EXCEPT pod (so: service_name, node, instance)
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum without (pod) (avg_over_time({service_name=\"api\"} | unwrap value [1m]) by (node))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(
        !result.is_empty(),
        "Expected non-empty result after writing grouping test data for cross-type By/Without merge"
    );

    // Verify the result structure
    for series in result {
        let metric = series["metric"].as_object().expect("metric should be an object");

        // Without(pod) means pod should NOT appear in the metric labels
        assert!(
            !metric.contains_key("pod"),
            "pod label should not appear in result with Without(pod) grouping"
        );

        // Other labels (service_name, node, instance) should be present
        // (at least some of them, depending on the data)
        println!("Series metric labels: {:?}", metric.keys().collect::<Vec<_>>());
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_cross_type_grouping_without_inner_by_outer() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3251).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_grouping_test_logs(&table, &catalog).await?;

    // Query: sum by (service_name) (avg_over_time({service_name="api"} | unwrap value [1m]) without (instance))
    // Expected behavior: inner Without(instance) is discarded, outer By(service_name) is used
    // Result should group ONLY by service_name
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum by (service_name) (avg_over_time({service_name=\"api\"} | unwrap value [1m]) without (instance))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(
        !result.is_empty(),
        "Expected non-empty result after writing grouping test data for cross-type Without/By merge"
    );

    // Verify the result structure
    for series in result {
        let metric = series["metric"].as_object().expect("metric should be an object");

        // By(service_name) means ONLY service_name should appear in the result
        // (plus possibly __name__ and other internal labels)
        println!("Series metric labels: {:?}", metric.keys().collect::<Vec<_>>());

        // service_name should be present (output as "service" for Loki compatibility)
        assert!(
            metric.contains_key("service") || metric.contains_key("service_name"),
            "service_name should be present in result with By(service_name) grouping"
        );

        // Other labels (node, pod, instance) should NOT be present
        for label in &["node", "pod", "instance"] {
            assert!(
                !metric.contains_key(*label),
                "{} label should not appear in result with By(service_name) grouping",
                label
            );
        }
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_same_type_grouping_by_merge() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3252).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_grouping_test_logs(&table, &catalog).await?;

    // Query: sum by (service_name, account_id) (avg_over_time({service_name="api"} | unwrap value [1m]) by (account_id))
    // Tests same-type grouping (By + By)
    // Expected behavior: inner By(account_id) and outer By(service_name, account_id) are merged (union)
    // Result: By(service_name, account_id) - both labels present in output
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum by (service_name, account_id) (avg_over_time({service_name=\"api\"} | unwrap value [1m]) by (account_id))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(
        !result.is_empty(),
        "Expected non-empty result after writing grouping test data for same-type By/By merge"
    );

    // Verify the result structure
    for series in result {
        let metric = series["metric"].as_object().expect("metric should be an object");

        println!("Series metric labels: {:?}", metric.keys().collect::<Vec<_>>());

        // Both labels from outer By should be present
        // account_id (output as "cloud_account_id")
        assert!(
            metric.contains_key("cloud_account_id") || metric.contains_key("account_id"),
            "account_id should be present in output"
        );

        // service_name (output as "service")
        assert!(
            metric.contains_key("service") || metric.contains_key("service_name"),
            "service_name should be present in output"
        );

        // Other labels (pod, instance, node) should NOT be present
        for label in &["pod", "instance", "node"] {
            assert!(
                !metric.contains_key(*label),
                "{} label should not appear in result",
                label
            );
        }
    }

    server.shutdown().await;
    Ok(())
}

/// Test grouping by binary columns (`trace_id`, `span_id`)
///
/// Verifies that `FixedSizeBinary` columns can be used in GROUP BY operations.
#[tokio::test]
async fn test_grouping_by_binary_columns() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3253).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_binary_grouping_test_logs(&table, &catalog).await?;

    // Test 1: Group by trace_id (FixedSizeBinary(16))
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum by (trace_id) (count_over_time({service_name=\"api\"}[1m]))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    println!("Test grouping by trace_id:");
    println!("Status: {}", status);
    println!("Body: {}", serde_json::to_string_pretty(&body)?);

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(
        !result.is_empty(),
        "Expected non-empty result after writing binary grouping test data (grouping by trace_id)"
    );

    // Verify trace_id is present in results
    for series in result {
        let metric = series["metric"].as_object().expect("metric should be an object");
        assert!(
            metric.contains_key("trace_id"),
            "trace_id should be present in grouped output"
        );
    }

    // Test 2: Group by span_id (FixedSizeBinary(8))
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum by (span_id) (count_over_time({service_name=\"api\"}[1m]))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    println!("\nTest grouping by span_id:");
    println!("Status: {}", status);
    println!("Body: {}", serde_json::to_string_pretty(&body)?);

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(
        !result.is_empty(),
        "Expected non-empty result after writing binary grouping test data (grouping by span_id)"
    );

    // Verify span_id is present in results
    for series in result {
        let metric = series["metric"].as_object().expect("metric should be an object");
        assert!(
            metric.contains_key("span_id"),
            "span_id should be present in grouped output"
        );
    }

    server.shutdown().await;
    Ok(())
}

/// Write test logs with distinct binary column values for grouping tests
async fn write_binary_grouping_test_logs(
    table: &Table,
    catalog: &Arc<dyn iceberg::Catalog>,
) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let unique_suffix = format!(
        "binary-grouping-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let now_micros = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as i64;

    let batch = build_binary_grouping_test_batch(table, now_micros)?;

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

/// Test grouping by attributes from the MAP column
///
/// Verifies that labels stored in the attributes MAP are correctly filtered and used for grouping.
/// This test addresses the original issue where attribute MAP grouping was not working correctly.
#[tokio::test]
async fn test_attribute_map_grouping() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3254).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_grouping_test_logs(&table, &catalog).await?;

    // Test 1: Group by mix of indexed column (service_name) and attribute MAP labels (pod, node)
    // Query: sum by (service_name, pod, node) (sum_over_time({cloud_account_id="acc-1"} | unwrap value [1m]))
    // - service_name is an indexed column
    // - pod and node are from the attributes MAP
    // Note: Using sum_over_time with unwrap because attribute MAP filtering is only implemented
    // for unwrap-based range aggregations (see build_grouping_with_filtered_attrs)
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum by (service_name, pod, node) (sum_over_time({cloud_account_id=\"acc-1\"} | unwrap value [1m]))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    println!("Test grouping by mixed indexed and MAP attributes:");
    println!("Status: {}", status);
    println!("Body: {}", serde_json::to_string_pretty(&body)?);

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(
        !result.is_empty(),
        "Expected non-empty result after writing grouping test data (grouping by mixed attributes)"
    );

    // Each series should have distinct combinations of service_name, pod, and node
    // We wrote 6 logs with different combinations:
    // - api/pod-a/node1, api/pod-b/node1, api/pod-c/node2
    // - backend/pod-d/node1, backend/pod-e/node2, backend/pod-f/node2
    assert!(
        result.len() >= 4,
        "Expected at least 4 distinct series for different service_name/pod/node combinations, got {}",
        result.len()
    );

    // Verify each series has the expected labels
    for series in result {
        let metric = series["metric"].as_object().expect("metric should be an object");

        // service_name should be present (output as "service")
        assert!(
            metric.contains_key("service") || metric.contains_key("service_name"),
            "service_name should be present in grouped output"
        );

        // pod should be present (from attributes MAP)
        assert!(
            metric.contains_key("pod"),
            "pod (from attributes MAP) should be present in grouped output"
        );

        // node should be present (from attributes MAP)
        assert!(
            metric.contains_key("node"),
            "node (from attributes MAP) should be present in grouped output"
        );

        // instance should NOT be present (not in grouping clause)
        assert!(
            !metric.contains_key("instance"),
            "instance should not be present in output (not in by clause)"
        );

        println!("Series labels: {:?}", metric);
    }

    // Test 2: Group by only MAP attributes (pod, instance)
    // This tests that MAP-only grouping works correctly without any indexed columns
    let resp2 = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            (
                "query",
                "sum by (pod, instance) (sum_over_time({cloud_account_id=\"acc-1\"} | unwrap value [1m]))",
            ),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status2 = resp2.status();
    let body2: Value = resp2.json().await?;

    println!("\nTest grouping by MAP attributes only:");
    println!("Status: {}", status2);
    println!("Body: {}", serde_json::to_string_pretty(&body2)?);

    assert_eq!(status2, 200, "Response body: {}", body2);
    assert_eq!(body2["status"], "success");

    let result2 = body2["data"]["result"].as_array().expect("result should be an array");
    assert!(
        !result2.is_empty(),
        "Expected non-empty result for MAP-only attribute grouping"
    );

    // All 6 logs have distinct pod+instance combinations
    assert_eq!(
        result2.len(),
        6,
        "Expected 6 distinct series for different pod/instance combinations, got {}",
        result2.len()
    );

    // Verify each series has pod and instance but NOT service_name
    for series in result2 {
        let metric = series["metric"].as_object().expect("metric should be an object");

        assert!(
            metric.contains_key("pod"),
            "pod should be present in MAP-only grouped output"
        );
        assert!(
            metric.contains_key("instance"),
            "instance should be present in MAP-only grouped output"
        );

        // service_name should NOT be present (not in grouping clause)
        assert!(
            !metric.contains_key("service") && !metric.contains_key("service_name"),
            "service_name should not be present in output (not in by clause)"
        );

        println!("MAP-only series labels: {:?}", metric);
    }

    server.shutdown().await;
    Ok(())
}

/// Build a `RecordBatch` with test logs containing distinct binary column values
fn build_binary_grouping_test_batch(table: &Table, now_micros: i64) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    // Create 4 log entries with different trace_id/span_id combinations
    let tenant_id: ArrayRef = Arc::new(StringArray::from(vec![
        "test-tenant",
        "test-tenant",
        "test-tenant",
        "test-tenant",
    ]));

    let timestamps: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros,
        now_micros + 1000,
        now_micros + 2000,
        now_micros + 3000,
    ]));

    // Create distinct trace_id values (FixedSizeBinary(16))
    let mut trace_id_builder = FixedSizeBinaryBuilder::with_capacity(4, 16);
    trace_id_builder.append_value([1u8; 16])?;
    trace_id_builder.append_value([2u8; 16])?;
    trace_id_builder.append_value([1u8; 16])?; // Duplicate trace_id
    trace_id_builder.append_value([3u8; 16])?;
    let trace_id: ArrayRef = Arc::new(trace_id_builder.finish());

    // Create distinct span_id values (FixedSizeBinary(8))
    let mut span_id_builder = FixedSizeBinaryBuilder::with_capacity(4, 8);
    span_id_builder.append_value([10u8; 8])?;
    span_id_builder.append_value([20u8; 8])?;
    span_id_builder.append_value([30u8; 8])?;
    span_id_builder.append_value([10u8; 8])?; // Duplicate span_id
    let span_id: ArrayRef = Arc::new(span_id_builder.finish());

    let service_name: ArrayRef = Arc::new(StringArray::from(vec![
        Some("api"),
        Some("api"),
        Some("api"),
        Some("api"),
    ]));

    let cloud_account_id: ArrayRef = Arc::new(StringArray::from(vec![
        Some("acc-1"),
        Some("acc-1"),
        Some("acc-1"),
        Some("acc-1"),
    ]));

    let severity_text: ArrayRef = Arc::new(StringArray::from(vec![
        Some("INFO"),
        Some("INFO"),
        Some("INFO"),
        Some("INFO"),
    ]));

    let severity_number: ArrayRef = Arc::new(Int32Array::from(vec![Some(9), Some(9), Some(9), Some(9)]));

    let body: ArrayRef = Arc::new(StringArray::from(vec![
        "Test log 1",
        "Test log 2",
        "Test log 3",
        "Test log 4",
    ]));

    // Build attributes MAP with minimal data
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

    // Create minimal empty maps for each row
    for _ in 0..4 {
        attributes_builder.append(true)?; // Empty map entry
    }
    let attributes: ArrayRef = Arc::new(attributes_builder.finish());

    // Need to add missing columns
    let observed_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros,
        now_micros + 1000,
        now_micros + 2000,
        now_micros + 3000,
    ]));

    let ingested_timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        now_micros, now_micros, now_micros, now_micros,
    ]));

    let flags: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>, None, None, None]));
    let dropped_attributes_count: ArrayRef = Arc::new(Int32Array::from(vec![0, 0, 0, 0]));

    RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            tenant_id,
            cloud_account_id,
            service_name,
            timestamps,
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
    )
    .map_err(Into::into)
}
