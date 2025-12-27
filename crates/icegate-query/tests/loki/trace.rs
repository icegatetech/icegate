//! Tests for `trace_id` and `span_id` queries (binary columns with hex
//! encoding)
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation
)]

use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE};
use serde_json::Value;

use super::harness::{TestServer, write_test_logs};

#[tokio::test]
async fn test_query_by_trace_id() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3213).await?;

    let table = catalog.load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?).await?;
    write_test_logs(&table, &catalog).await?;

    // Query by trace_id (hex string)
    // Test data has trace_id = [0x01, 0x02, ..., 0x10] =
    // "0102030405060708090a0b0c0d0e0f10"
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{trace_id=\"0102030405060708090a0b0c0d0e0f10\"}")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "trace_id query failed: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(
        !result.is_empty(),
        "trace_id query should return results, got: {}",
        body
    );

    // Verify the trace_id label is present in the stream
    let stream = &result[0];
    assert!(
        stream["stream"]["trace_id"].is_string(),
        "trace_id should be in stream labels, got: {}",
        stream
    );
    assert_eq!(
        stream["stream"]["trace_id"], "0102030405060708090a0b0c0d0e0f10",
        "trace_id should match queried value"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_query_by_span_id() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3214).await?;

    let table = catalog.load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?).await?;
    write_test_logs(&table, &catalog).await?;

    // Query by span_id (hex string)
    // Test data has span_id = [0x01, 0x02, ..., 0x08] = "0102030405060708"
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{span_id=\"0102030405060708\"}")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "span_id query failed: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "span_id query should return results, got: {}", body);

    // Verify the span_id label is present in the stream
    let stream = &result[0];
    assert!(
        stream["stream"]["span_id"].is_string(),
        "span_id should be in stream labels, got: {}",
        stream
    );
    assert_eq!(
        stream["stream"]["span_id"], "0102030405060708",
        "span_id should match queried value"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_combined_trace_query() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3215).await?;

    let table = catalog.load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?).await?;
    write_test_logs(&table, &catalog).await?;

    // Combined query with service_name and span_id
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\",span_id=\"0102030405060708\"}")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "combined query failed: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(
        !result.is_empty(),
        "combined query should return results, got: {}",
        body
    );

    // Verify both labels are present
    let stream = &result[0];
    assert_eq!(stream["stream"]["service_name"], "frontend");
    assert_eq!(stream["stream"]["span_id"], "0102030405060708");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_nonexistent_trace_id() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3216).await?;

    let table = catalog.load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?).await?;
    write_test_logs(&table, &catalog).await?;

    // Query with non-existent trace_id should return empty
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{trace_id=\"ffffffffffffffffffffffffffffffff\"}")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "non-existent trace_id query failed: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(
        result.is_empty(),
        "non-existent trace_id should return empty results, got: {}",
        body
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_trace_id_label_values() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3217).await?;

    let table = catalog.load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?).await?;
    write_test_logs(&table, &catalog).await?;

    // Query trace_id label values endpoint
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/label/trace_id/values", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "trace_id label values failed: {}", body);
    assert_eq!(body["status"], "success");

    let values = body["data"].as_array().expect("data should be an array");
    assert!(
        !values.is_empty(),
        "trace_id label values should not be empty, got: {}",
        body
    );

    // Values should be hex strings
    let value_strs: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        value_strs.contains(&"0102030405060708090a0b0c0d0e0f10"),
        "trace_id values should contain test data hex string, got: {:?}",
        value_strs
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_span_id_label_values() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3218).await?;

    let table = catalog.load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?).await?;
    write_test_logs(&table, &catalog).await?;

    // Query span_id label values endpoint
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/label/span_id/values", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "span_id label values failed: {}", body);
    assert_eq!(body["status"], "success");

    let values = body["data"].as_array().expect("data should be an array");
    assert!(
        !values.is_empty(),
        "span_id label values should not be empty, got: {}",
        body
    );

    // Values should be hex strings
    let value_strs: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        value_strs.contains(&"0102030405060708"),
        "span_id values should contain test data hex string, got: {:?}",
        value_strs
    );

    server.shutdown().await;
    Ok(())
}
