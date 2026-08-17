//! Tests for the Tempo `TraceQL` search endpoint: `GET /api/search`.
//!
//! Complements `tags.rs` (tag *discovery*) by exercising the ANTLR-parsed
//! `TraceQL` grammar and the `DataFusion` selector translation end to end:
//! parse -> plan -> execute -> format -> HTTP JSON.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args
)]

use icegate_common::{ICEGATE_NAMESPACE, SPANS_TABLE};
use serde_json::Value;

use super::harness::{TestServer, write_test_spans_with_scope_attributes};

/// Run one `TraceQL` search query against the running server and return the
/// hex `spanID`s of every matched span, across every trace in the response.
async fn matched_span_ids(server: &TestServer, query: &str) -> Vec<String> {
    let resp = server
        .client
        .get(format!("{}/api/search", server.base_url))
        .header("X-Scope-OrgID", "tempo-tenant")
        .query(&[("q", query)])
        .send()
        .await
        .unwrap_or_else(|e| panic!("request failed for query={query:?}: {e}"));
    let status = resp.status();
    let body: Value = resp
        .json()
        .await
        .unwrap_or_else(|e| panic!("bad JSON for query={query:?}: {e}"));
    assert_eq!(status, 200, "query={query} response body: {body}");

    body["traces"]
        .as_array()
        .expect("traces array")
        .iter()
        .flat_map(|t| t["spanSets"].as_array().expect("spanSets array"))
        .flat_map(|ss| ss["spans"].as_array().expect("spans array"))
        .filter_map(|s| s["spanID"].as_str())
        .map(str::to_string)
        .collect()
}

/// C2 regression: `TraceQL`/Tempo has no instrumentation-scope query scope
/// (the owner's correction that replaced the earlier, incorrect `scope.`
/// selector) — `OTel` `InstrumentationScope.attributes` (physically the
/// `scope_attributes` column) are reachable ONLY through `span.<key>`, the
/// same way any other span-scope attribute is. Before this fix,
/// `span_attribute_lhs` read only `span_attributes`, so `{ span.<key> = … }`
/// silently returned zero spans for an attribute whose only carrier is
/// `scope_attributes` — even though `/api/v2/search/tags` and
/// `/api/search/tag/{name}/values` already advertised and resolved it (see
/// `tags.rs`).
///
/// Row 2 only (backend / "query users" / span 2122232425262728) carries the
/// scope attributes; rows 0/1 stay empty so their fixed `span_attributes`
/// (`http.method=GET`) can't be confused with a match. `k8s.namespace.name`
/// additionally proves `span.` does NOT fall through to
/// `resource_attributes`, which carries that same key (with the same
/// `"prod"` value) on every row via the fixed fixture.
#[tokio::test]
async fn search_span_selector_reads_scope_only_attributes() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans_with_scope_attributes(
        &table,
        &catalog,
        "tempo-tenant",
        &[&[], &[], &[("http.method", "PUT"), ("k8s.namespace.name", "prod")]],
    )
    .await?;

    for query in [
        r#"{ span.http.method = "PUT" }"#,
        r#"{ span.k8s.namespace.name = "prod" }"#,
    ] {
        let span_ids = matched_span_ids(&server, query).await;
        assert_eq!(
            span_ids,
            vec!["2122232425262728"],
            "query={query} expected only the scope-tagged span, got {:?}",
            span_ids
        );
    }

    server.shutdown().await;
    Ok(())
}

/// C2 regression, unscoped form: `{ .<key> = "value" }` (`Scope::Any`) must
/// also reach a `scope_attributes`-only attribute, because its span-side
/// comparison composes `span_attribute_lhs` — the same function
/// [`search_span_selector_reads_scope_only_attributes`] proves reads
/// `scope_attributes`. Uses `http.method` only (not `k8s.namespace.name`
/// as above): the unscoped shorthand additionally ORs in
/// `resource_attributes`, which already carries `k8s.namespace.name=prod`
/// on every row, so that key would incorrectly match all three rows here
/// instead of isolating row 2.
#[tokio::test]
async fn search_unscoped_selector_reads_scope_only_attributes() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans_with_scope_attributes(&table, &catalog, "tempo-tenant", &[&[], &[], &[("http.method", "PUT")]])
        .await?;

    let span_ids = matched_span_ids(&server, r#"{ .http.method = "PUT" }"#).await;
    assert_eq!(
        span_ids,
        vec!["2122232425262728"],
        "expected only the scope-tagged span, got {:?}",
        span_ids
    );

    server.shutdown().await;
    Ok(())
}

/// C2 collision-precedence regression: when the SAME row carries the SAME
/// key in both `span_attributes` and `scope_attributes` with DIFFERENT
/// values, `span_attributes` must win — the exact precedence ingest's
/// pre-migration physical fold had (a span-level key silently overwrote a
/// same-named scope-level key). Row 0's fixed `span_attributes` already
/// carries `http.method="GET"` (see `write_spans_fixture` in
/// `harness.rs`); this test adds a colliding `scope_attributes` value of
/// `"POST"` on that same row.
///
/// Proven three ways: querying for the shadowed scope value must return
/// nothing (not even row 0) — proving it's fully shadowed, not just a
/// tiebreak an easy-to-misdiagnose "returns nothing" could hide; querying
/// for the span value must still match normally through `span.`; and the
/// same must hold through the unscoped `.` shorthand, which composes the
/// same `span_attribute_lhs`.
#[tokio::test]
async fn search_span_selector_prefers_span_attributes_over_scope_attributes_on_collision()
-> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table = catalog
        .load_table(&iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?)
        .await?;
    write_test_spans_with_scope_attributes(
        &table,
        &catalog,
        "tempo-tenant",
        &[&[("http.method", "POST")], &[], &[]],
    )
    .await?;

    for (query, expected) in [
        (r#"{ span.http.method = "POST" }"#, vec![]),
        (
            r#"{ span.http.method = "GET" }"#,
            vec!["0102030405060708".to_string(), "1112131415161718".to_string()],
        ),
        (
            r#"{ .http.method = "GET" }"#,
            vec!["0102030405060708".to_string(), "1112131415161718".to_string()],
        ),
    ] {
        let span_ids = matched_span_ids(&server, query).await;
        assert_eq!(
            span_ids, expected,
            "query={query} expected {:?}, got {:?}",
            expected, span_ids
        );
    }

    server.shutdown().await;
    Ok(())
}
