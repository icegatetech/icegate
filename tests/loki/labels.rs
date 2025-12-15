//! Tests for label metadata endpoints (labels, label values, series)
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation
)]

use icegate::common::{ICEGATE_NAMESPACE, LOGS_TABLE};
use serde_json::Value;

use super::harness::{write_test_logs, TestServer};

#[tokio::test]
async fn test_labels_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3208).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/labels", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let labels = body["data"].as_array().expect("data should be an array");
    let label_strs: Vec<&str> = labels.iter().filter_map(|v| v.as_str()).collect();

    // Should include indexed columns
    assert!(
        label_strs.contains(&"service_name"),
        "labels should include 'service_name', got: {:?}",
        label_strs
    );
    assert!(
        label_strs.contains(&"severity_text"),
        "labels should include 'severity_text', got: {:?}",
        label_strs
    );
    assert!(
        label_strs.contains(&"account_id"),
        "labels should include 'account_id', got: {:?}",
        label_strs
    );

    // Should include `level` as alias of `severity_text` for Grafana compatibility
    assert!(
        label_strs.contains(&"level"),
        "labels should include 'level' (alias of severity_text), got: {:?}",
        label_strs
    );

    // Should include attribute keys from test data
    assert!(
        label_strs.contains(&"user_id") || label_strs.contains(&"page") || label_strs.contains(&"db_host"),
        "labels should include attribute keys from test data, got: {:?}",
        label_strs
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_label_values_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3209).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Test label values for 'service_name'
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/label/service_name/values", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let values = body["data"].as_array().expect("data should be an array");
    let value_strs: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();

    assert!(
        value_strs.contains(&"frontend"),
        "service_name values should include 'frontend', got: {:?}",
        value_strs
    );
    assert!(
        value_strs.contains(&"backend"),
        "service_name values should include 'backend', got: {:?}",
        value_strs
    );

    // Test label values for 'level' (maps to severity_text)
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/label/level/values", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let values = body["data"].as_array().expect("data should be an array");
    let value_strs: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();

    assert!(
        value_strs.contains(&"INFO"),
        "level values should include 'INFO', got: {:?}",
        value_strs
    );
    assert!(
        value_strs.contains(&"WARN"),
        "level values should include 'WARN', got: {:?}",
        value_strs
    );

    // Test label values for an attribute (not indexed column)
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/label/user_id/values", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let values = body["data"].as_array().expect("data should be an array");
    if !values.is_empty() {
        let value_strs: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();
        assert!(
            value_strs.contains(&"user-123"),
            "user_id values should include 'user-123', got: {:?}",
            value_strs
        );
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_series_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3210).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Test series endpoint with match[] parameter
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/series", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("match[]", "{service_name=~\".+\"}")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let series_list = body["data"].as_array().expect("data should be an array");
    assert!(!series_list.is_empty(), "series should not be empty, got: {}", body);

    // Each series should be a label map with both severity_text and level
    for series in series_list {
        assert!(series.is_object(), "each series should be an object, got: {}", series);
        assert!(
            series["service_name"].is_string(),
            "series should have service_name label, got: {}",
            series
        );
        // Verify `level` is included (alias of severity_text for Grafana)
        assert!(
            series["level"].is_string(),
            "series should have 'level' label (alias of severity_text), got: {}",
            series
        );
        // Verify severity_text is also included (OTel original)
        assert!(
            series["severity_text"].is_string(),
            "series should have 'severity_text' label, got: {}",
            series
        );
    }

    // Test series endpoint without match[] - should return error
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/series", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 400, "Missing match[] should return 400, got: {}", body);
    assert_eq!(body["status"], "error");

    server.shutdown().await;
    Ok(())
}
