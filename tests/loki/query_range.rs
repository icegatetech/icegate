//! Tests for basic `query_range` and explain endpoints
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
async fn test_query_range_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3201).await?;

    // Insert test data
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Query Loki API with tenant header
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "{service_name=\"frontend\"}")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "streams");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    assert!(!result.is_empty(), "result should not be empty, got: {}", body);

    // Verify stream structure
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

    // Verify stats structure
    assert!(
        body["data"]["stats"]["summary"]["execTime"].is_number(),
        "execTime should be present"
    );
    assert!(
        body["data"]["stats"]["summary"]["totalLinesProcessed"].as_u64().unwrap_or(0) > 0,
        "totalLinesProcessed should be > 0"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_log_vs_metric_response_types() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3203).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    // Log query should return "streams" with "stream" labels
    let log_resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
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

        let log_values = log_result[0]["values"].as_array().unwrap();
        if !log_values.is_empty() {
            assert!(
                log_values[0][0].is_string(),
                "Log timestamps should be strings (nanoseconds)"
            );
        }
    }

    // Metric query should return "matrix" with "metric" labels
    let metric_resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
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

        let metric_values = metric_result[0]["values"].as_array().unwrap();
        if !metric_values.is_empty() {
            assert!(
                metric_values[0][0].is_number(),
                "Metric timestamps should be numbers (decimal seconds)"
            );
        }
    }

    server.shutdown().await;
    Ok(())
}
