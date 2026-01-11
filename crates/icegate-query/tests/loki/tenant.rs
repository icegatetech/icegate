//! Tests for tenant isolation
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args,
    clippy::cast_possible_truncation
)]

use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE};
use serde_json::Value;

use super::harness::{TestServer, write_test_logs_for_tenant};

#[tokio::test]
async fn test_tenant_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    // Insert test data for two different tenants
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-alpha", "alpha-service", "Alpha").await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-beta", "beta-service", "Beta").await?;

    // Query as tenant-alpha - should only see alpha's logs
    let resp_alpha = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
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

    // Query as tenant-beta - should only see beta's logs
    let resp_beta = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
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

    // Query as tenant-alpha but try to access beta's service - should see nothing
    let resp_cross = server
        .client
        .get(format!("{}/loki/api/v1/query_range", server.base_url))
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

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn test_labels_tenant_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    // Insert test data for two different tenants
    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-gamma", "gamma-service", "Gamma").await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?)
        .await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-delta", "delta-service", "Delta").await?;

    // Query label values for service_name as tenant-gamma
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/label/service_name/values", server.base_url))
        .header("X-Scope-OrgID", "tenant-gamma")
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let values = body["data"].as_array().expect("data should be an array");
    let value_strs: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();

    // Gamma should see gamma-service
    assert!(
        value_strs.contains(&"gamma-service"),
        "tenant-gamma should see 'gamma-service', got: {:?}",
        value_strs
    );

    // Gamma should NOT see delta-service (tenant isolation)
    assert!(
        !value_strs.contains(&"delta-service"),
        "tenant-gamma should NOT see 'delta-service' (tenant isolation), got: {:?}",
        value_strs
    );

    // Query label values for service_name as tenant-delta
    let resp = server
        .client
        .get(format!("{}/loki/api/v1/label/service_name/values", server.base_url))
        .header("X-Scope-OrgID", "tenant-delta")
        .send()
        .await?;

    let status = resp.status();
    let body: Value = resp.json().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body["status"], "success");

    let values = body["data"].as_array().expect("data should be an array");
    let value_strs: Vec<&str> = values.iter().filter_map(|v| v.as_str()).collect();

    // Delta should see delta-service
    assert!(
        value_strs.contains(&"delta-service"),
        "tenant-delta should see 'delta-service', got: {:?}",
        value_strs
    );

    // Delta should NOT see gamma-service (tenant isolation)
    assert!(
        !value_strs.contains(&"gamma-service"),
        "tenant-delta should NOT see 'gamma-service' (tenant isolation), got: {:?}",
        value_strs
    );

    server.shutdown().await;
    Ok(())
}
