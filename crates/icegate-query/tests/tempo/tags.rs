//! Tests for Tempo tag-discovery endpoints:
//! `/api/search/tags`, `/api/v2/search/tags`,
//! `/api/search/tag/{name}/values`.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args
)]

use icegate_common::{ICEGATE_NAMESPACE, SPANS_TABLE};
use serde_json::Value;

use super::harness::{TestServer, write_test_spans};

#[tokio::test]
async fn v1_tags_returns_attribute_keys_and_intrinsics() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    let resp = server
        .client
        .get(format!("{}/api/search/tags", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .send()
        .await?;
    let status = resp.status();
    let body: Value = resp.json().await?;
    assert_eq!(status, 200, "Response body: {}", body);

    let tags = body["tagNames"].as_array().expect("tagNames array");
    let tag_strs: Vec<&str> = tags.iter().filter_map(|v| v.as_str()).collect();

    // Attribute keys from the written spans should appear.
    assert!(
        tag_strs.contains(&"http.method"),
        "expected http.method in {:?}",
        tag_strs
    );
    assert!(
        tag_strs.contains(&"k8s.namespace.name"),
        "expected k8s.namespace.name in {:?}",
        tag_strs
    );
    // TraceQL intrinsics should appear.
    assert!(tag_strs.contains(&"duration"), "expected duration in {:?}", tag_strs);
    assert!(tag_strs.contains(&"name"), "expected name intrinsic in {:?}", tag_strs);

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn v2_tags_groups_by_scope() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    let resp = server
        .client
        .get(format!("{}/api/v2/search/tags", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .send()
        .await?;
    let status = resp.status();
    let body: Value = resp.json().await?;
    assert_eq!(status, 200, "Response body: {}", body);

    let scopes = body["scopes"].as_array().expect("scopes array");
    let by_name: std::collections::HashMap<&str, Vec<&str>> = scopes
        .iter()
        .map(|s| {
            let name = s["name"].as_str().unwrap();
            let tags: Vec<&str> = s["tags"].as_array().unwrap().iter().filter_map(|v| v.as_str()).collect();
            (name, tags)
        })
        .collect();

    // Resource scope must include the k8s.* key and the service_name column.
    let resource = by_name.get("resource").expect("resource scope present");
    assert!(
        resource.contains(&"k8s.namespace.name"),
        "expected k8s.namespace.name in resource scope: {:?}",
        resource
    );
    assert!(
        resource.contains(&"service_name"),
        "expected service_name in resource scope: {:?}",
        resource
    );

    // Span scope must include http.method and NOT include k8s.* keys.
    let span = by_name.get("span").expect("span scope present");
    assert!(
        span.contains(&"http.method"),
        "expected http.method in span scope: {:?}",
        span
    );
    assert!(
        !span.contains(&"k8s.namespace.name"),
        "did not expect k8s.namespace.name in span scope: {:?}",
        span
    );

    // Intrinsic scope must include the standard TraceQL names.
    let intrinsic = by_name.get("intrinsic").expect("intrinsic scope present");
    assert!(intrinsic.contains(&"duration"), "expected duration: {:?}", intrinsic);
    assert!(intrinsic.contains(&"name"), "expected name: {:?}", intrinsic);
    assert!(intrinsic.contains(&"status"), "expected status: {:?}", intrinsic);

    // Event and link scopes must exist (even if empty).
    assert!(by_name.contains_key("event"), "event scope missing");
    assert!(by_name.contains_key("link"), "link scope missing");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn v2_tags_scope_filter_returns_only_that_scope() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    let resp = server
        .client
        .get(format!("{}/api/v2/search/tags?scope=span", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .send()
        .await?;
    let status = resp.status();
    let body: Value = resp.json().await?;
    assert_eq!(status, 200, "Response body: {}", body);

    let scopes = body["scopes"].as_array().expect("scopes array");
    assert_eq!(scopes.len(), 1, "expected single scope, got {:?}", scopes);
    assert_eq!(scopes[0]["name"], "span");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tag_values_for_resource_service_name() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    let resp = server
        .client
        .get(format!(
            "{}/api/search/tag/resource.service.name/values",
            server.base_url
        ))
        .header("X-Scope-OrgID", "tempo-tenant")
        .send()
        .await?;
    let status = resp.status();
    let body: Value = resp.json().await?;
    assert_eq!(status, 200, "Response body: {}", body);

    let values: Vec<&str> = body["tagValues"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        values.contains(&"frontend"),
        "expected frontend in tagValues: {:?}",
        values
    );
    assert!(
        values.contains(&"backend"),
        "expected backend in tagValues: {:?}",
        values
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tag_values_for_intrinsic_name() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    let resp = server
        .client
        .get(format!("{}/api/search/tag/name/values", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .send()
        .await?;
    let status = resp.status();
    let body: Value = resp.json().await?;
    assert_eq!(status, 200, "Response body: {}", body);

    let values: Vec<&str> = body["tagValues"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        values.contains(&"GET /api/health"),
        "expected 'GET /api/health' in tagValues: {:?}",
        values
    );
    assert!(
        values.contains(&"query users"),
        "expected 'query users' in tagValues: {:?}",
        values
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tag_values_for_span_attribute() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    let resp = server
        .client
        .get(format!("{}/api/search/tag/span.http.method/values", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .send()
        .await?;
    let status = resp.status();
    let body: Value = resp.json().await?;
    assert_eq!(status, 200, "Response body: {}", body);

    let values: Vec<&str> = body["tagValues"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(values.contains(&"GET"), "expected GET in tagValues: {:?}", values);

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tags_tenant_isolation() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans(&table, &catalog, "tempo-tenant").await?;

    // Request with a DIFFERENT tenant — should see no spans and therefore no
    // attribute keys. Intrinsics will still be present.
    let resp = server
        .client
        .get(format!("{}/api/v2/search/tags", server.base_url))
        .header("X-Scope-OrgID", "other-tenant")
        .send()
        .await?;
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await?;

    let scopes = body["scopes"].as_array().unwrap();
    let by_name: std::collections::HashMap<&str, Vec<&str>> = scopes
        .iter()
        .map(|s| {
            let name = s["name"].as_str().unwrap();
            let tags: Vec<&str> = s["tags"].as_array().unwrap().iter().filter_map(|v| v.as_str()).collect();
            (name, tags)
        })
        .collect();

    // Other tenant should see nothing in span scope (no attributes).
    let span = by_name.get("span").expect("span scope present");
    assert!(
        !span.contains(&"http.method"),
        "leaked http.method to other tenant: {:?}",
        span
    );

    server.shutdown().await;
    Ok(())
}
