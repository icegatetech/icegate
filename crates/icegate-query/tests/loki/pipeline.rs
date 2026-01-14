//! Tests for `LogQL` pipeline stages (drop, keep, etc.)
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
async fn test_drop_with_simple_names() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Query with drop to remove user_id attribute
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"} | drop user_id")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "result should not be empty");

    // Find the stream that originally had user_id and request_id
    // (it should now have only request_id after drop)
    let stream = result
        .iter()
        .find(|s| s["stream"].as_object().unwrap().contains_key("request_id"))
        .expect("Should find stream with request_id");

    let labels = &stream["stream"];

    // Verify user_id attribute is not present in the stream
    assert!(
        !labels.as_object().unwrap().contains_key("user_id"),
        "user_id should be dropped from labels"
    );

    // Verify request_id attribute is still present (not dropped)
    assert_eq!(labels["request_id"], "req-456", "request_id should still be present");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_keep_with_simple_names() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Query with keep to only keep user_id attribute
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"} | keep user_id")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "result should not be empty");

    // Find the stream that has user_id after keep operation
    let stream = result
        .iter()
        .find(|s| s["stream"].as_object().unwrap().contains_key("user_id"))
        .expect("Should find stream with user_id after keep");

    let labels = stream["stream"].as_object().expect("stream labels should be an object");

    assert_eq!(labels["user_id"], "user-123", "user_id should be present");

    // Other attributes should be removed (request_id should not be present)
    assert!(
        !labels.contains_key("request_id"),
        "request_id attribute should be removed by keep operation"
    );

    // Indexed columns should still be present (they are not affected by keep)
    assert!(
        labels.contains_key("service_name"),
        "Indexed columns like service_name should still be present"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_drop_with_equals_matcher() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Query with drop user_id="user-123" to conditionally remove the attribute
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"} | drop user_id=\"user-123\"")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "result should not be empty");

    // Verify user_id is dropped because it matches "user-123"
    let stream = &result[0];
    let labels = &stream["stream"];
    assert!(
        !labels.as_object().unwrap().contains_key("user_id"),
        "user_id should be dropped (matches value)"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_drop_with_regex_matcher() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Query with drop using regex matcher on user_id
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"} | drop user_id=~\"user-.*\"")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "result should not be empty");

    // user_id should be dropped because it matches the regex "user-.*"
    let stream = &result[0];
    let labels = &stream["stream"];
    assert!(
        !labels.as_object().unwrap().contains_key("user_id"),
        "user_id should be dropped (matches regex)"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_keep_with_matcher() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Query with keep using matcher on user_id
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"} | keep user_id=\"user-123\"")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "result should not be empty");

    // Find the stream that has user_id after keep operation
    // (it should have only user_id, not request_id)
    let stream = result
        .iter()
        .find(|s| s["stream"].as_object().unwrap().contains_key("user_id"))
        .expect("Should find stream with user_id after keep");

    let labels = &stream["stream"];

    // Only user_id should be kept (and it matches "user-123")
    assert_eq!(
        labels["user_id"], "user-123",
        "user_id should be kept with correct value"
    );

    // request_id should be removed because it doesn't match the keep condition
    assert!(
        !labels.as_object().unwrap().contains_key("request_id"),
        "request_id should be removed by keep operation"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_drop_mixed_simple_and_matchers() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Query combining simple name and matcher in drop
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[(
            "query",
            "{service_name=\"frontend\"} | drop request_id, user_id=\"user-123\"",
        )])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "result should not be empty");

    // Both request_id (simple) and user_id (matcher) should be dropped
    let stream = &result[0];
    let labels = stream["stream"].as_object().unwrap();
    assert!(
        !labels.contains_key("request_id") && !labels.contains_key("user_id"),
        "Both attributes should be dropped"
    );

    server.shutdown().await;
    Ok(())
}
