//! Tests for metric queries (`count_over_time`, rate, `bytes_rate`,
//! `bytes_over_time`)
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
async fn test_count_over_time_metric() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3204).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            ("query", "count_over_time({service_name=\"frontend\"}[5m])"),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["data"]["resultType"], "matrix",
        "Metric queries should return resultType 'matrix'"
    );

    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        assert!(
            series["metric"].is_object(),
            "metric series should have 'metric' labels"
        );
        assert!(series["stream"].is_null(), "metric series should NOT have 'stream' key");

        let values = series["values"].as_array().expect("values should be an array");
        // Find first non-null value to validate format
        for sample in values {
            let sample_array = sample.as_array().expect("sample should be an array");
            assert_eq!(
                sample_array.len(),
                2,
                "sample should have 2 elements [timestamp, value]"
            );
            assert!(sample_array[0].is_number(), "timestamp should be a number");
            // Value can be null (empty aggregation window) or string (numeric value)
            assert!(
                sample_array[1].is_null() || sample_array[1].is_string(),
                "value should be a string or null"
            );
            if !sample_array[1].is_null() {
                break; // Found a non-null value, format is valid
            }
        }
    }

    assert!(
        body["data"]["stats"]["summary"]["execTime"].is_number(),
        "execTime should be present"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_bytes_over_time_metric() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3205).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            ("query", "bytes_over_time({service_name=\"frontend\"}[5m])"),
            ("step", "60s"),
        ])
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

    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        assert!(series["metric"].is_object(), "should have 'metric' key for labels");

        let values = series["values"].as_array().expect("values should be an array");
        // Find first non-null value (null values are valid for empty windows)
        for sample in values {
            let sample_array = sample.as_array().expect("sample should be an array");
            if sample_array[1].is_null() {
                continue; // Skip null values (empty aggregation windows)
            }
            let value_str = sample_array[1].as_str().expect("value should be a string");
            assert!(
                value_str.parse::<f64>().is_ok(),
                "value should be a numeric string, got: {}",
                value_str
            );
            break;
        }
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_rate() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3206).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[("query", "rate({service_name=\"frontend\"}[5m])"), ("step", "60s")])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(body["data"]["resultType"], "matrix", "rate() should return matrix type");

    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        assert!(series["metric"].is_object(), "should have 'metric' key for labels");

        let values = series["values"].as_array().expect("values should be an array");
        // Find first non-null value (null values are valid for empty windows)
        for sample in values {
            let sample_array = sample.as_array().expect("sample should be an array");
            assert!(sample_array[0].is_number(), "timestamp should be a number");
            if sample_array[1].is_null() {
                continue; // Skip null values (empty aggregation windows)
            }
            let value_str = sample_array[1].as_str().expect("value should be a string");
            let rate_value: f64 = value_str.parse().expect("value should be parseable as f64");
            assert!(
                rate_value >= 0.0,
                "rate value should be non-negative, got: {}",
                rate_value
            );
            break;
        }
    }

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_bytes_rate() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start(3207).await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs(&table, &catalog).await?;

    let resp = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
        .header("X-Scope-OrgID", "test-tenant")
        .query(&[
            ("query", "bytes_rate({service_name=\"frontend\"}[5m])"),
            ("step", "60s"),
        ])
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["data"]["resultType"], "matrix",
        "bytes_rate() should return matrix type"
    );

    let result = body["data"]["result"].as_array().expect("result should be an array");
    if !result.is_empty() {
        let series = &result[0];
        assert!(series["metric"].is_object(), "should have 'metric' key for labels");

        let values = series["values"].as_array().expect("values should be an array");
        // Find first non-null value (null values are valid for empty windows)
        for sample in values {
            let sample_array = sample.as_array().expect("sample should be an array");
            assert!(sample_array[0].is_number(), "timestamp should be a number");
            if sample_array[1].is_null() {
                continue; // Skip null values (empty aggregation windows)
            }
            let value_str = sample_array[1].as_str().expect("value should be a string");
            let rate_value: f64 = value_str.parse().expect("value should be parseable as f64");
            assert!(
                rate_value >= 0.0,
                "bytes_rate value should be non-negative, got: {}",
                rate_value
            );
            break;
        }
    }

    server.shutdown().await;
    Ok(())
}
