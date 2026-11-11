//! `_cat/indices` and `_cat/plugins` over real HTTP: per-tenant figures and rendering. Each
//! request is a literal, and each expected answer is stated here.

use std::error::Error;
use std::sync::Arc;

use iceberg::{Catalog, NamespaceIdent, TableCreation};
use icegate_common::{ICEGATE_NAMESPACE, schema};
use reqwest::Method;
use serde_json::{Value, json};

use super::data::{LogRow, PriceRow, RowWriteError, commit_log_rows, commit_price_rows};
use super::harness::TestServer;

/// The tenant every request names unless a test states its own.
const TENANT: &str = "tenant-a";

/// `count` rows of `tenant_id`, one microsecond apart from 2025-10-09T08:53:20Z.
fn build_rows(tenant_id: &str, count: i64) -> Vec<LogRow<'_>> {
    (0..count)
        .map(|offset| LogRow {
            tenant_id,
            timestamp_micros: 1_760_000_000_000_000 + offset,
            service_name: Some("checkout"),
            severity_text: Some("INFO"),
            body: Some("row"),
            resource_attributes: &[],
            log_attributes: &[],
        })
        .collect()
}

/// Create the table `name` in the IceGate namespace, unpartitioned, with the `prices` schema, which
/// carries no `tenant_id`.
async fn create_table_without_tenant_id(catalog: &Arc<dyn Catalog>, name: &str) -> Result<(), Box<dyn Error>> {
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema::prices_schema()?)
        .build();
    catalog
        .create_table(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), creation)
        .await?;
    Ok(())
}

/// `GET path` as `tenant`: the status and the JSON body. The error is a failed request or a body
/// that is not JSON.
async fn get_cat_json(server: &TestServer, tenant: &str, path: &str) -> reqwest::Result<(u16, Value)> {
    let response = server.request(Method::GET, Some(tenant), path).send().await?;
    let status = response.status().as_u16();
    Ok((status, response.json().await?))
}

/// `GET path` as [`TENANT`]: the status, the `content-type` (empty when absent or not visible ASCII)
/// and the body as text. The error is a failed request or a body that cannot be read.
async fn get_cat_text(server: &TestServer, path: &str) -> reqwest::Result<(u16, String, String)> {
    let response = server.request(Method::GET, Some(TENANT), path).send().await?;
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    Ok((status, content_type, response.text().await?))
}

/// Both tenants are committed in one append, into one snapshot. Only partition pruning separates
/// their figures, and a tenant with no rows sees zero, never the table's total.
#[tokio::test]
async fn cat_indices_counts_only_the_requesting_tenants_rows() {
    let (server, catalog) = TestServer::start().await;
    let mut log_rows = build_rows("tenant-a", 3);
    log_rows.extend(build_rows("tenant-b", 5));
    commit_log_rows(&catalog, &log_rows).await.expect("commit rows");

    let path = "/_cat/indices/logs?format=json&h=index,docs.count,store.size,pri.store.size&bytes=b";
    let (status_a, rows_of_tenant_a) = get_cat_json(&server, "tenant-a", path).await.expect("a JSON answer");
    let (status_b, rows_of_tenant_b) = get_cat_json(&server, "tenant-b", path).await.expect("a JSON answer");
    let (status_c, rows_of_tenant_c) = get_cat_json(&server, "tenant-c", path).await.expect("a JSON answer");

    assert_eq!((status_a, status_b, status_c), (200, 200, 200));
    assert_eq!(rows_of_tenant_a[0]["docs.count"], "3");
    assert_eq!(rows_of_tenant_b[0]["docs.count"], "5");
    assert_eq!(
        rows_of_tenant_c,
        json!([{"index": "logs", "docs.count": "0", "store.size": "0", "pri.store.size": "0"}])
    );
    for tenant_rows in [&rows_of_tenant_a, &rows_of_tenant_b] {
        let store_size: u64 = tenant_rows[0]["store.size"]
            .as_str()
            .expect("a string size")
            .parse()
            .expect("a byte count");
        assert!(store_size > 0, "{tenant_rows}");
        assert_eq!(tenant_rows[0]["store.size"], tenant_rows[0]["pri.store.size"]);
    }
    server.shutdown().await;
}

/// Without an expression every index the tenant sees is listed, in name order, exact-name tables
/// included: the global `prices` table counted whole (empty here) and its views
/// `prices_effective` and `prices_effective_by_time_of_day`, whose counts are null. The span
/// profile index `otel-v1-apm-span-000001` is listed beside `spans`, the table it reads and counts, and
/// so are the three ss4o indices beside `logs`, `metrics` and `spans`.
#[tokio::test]
async fn cat_indices_without_an_expression_lists_exact_name_tables_and_a_view_count_as_null() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = get_cat_json(&server, TENANT, "/_cat/indices?format=json&h=index,docs.count")
        .await
        .expect("a JSON answer");

    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!([
            {"index": "events", "docs.count": "0"},
            {"index": "logs", "docs.count": "0"},
            {"index": "metrics", "docs.count": "0"},
            {"index": "operations", "docs.count": "0"},
            {"index": "otel-v1-apm-span-000001", "docs.count": "0"},
            {"index": "prices", "docs.count": "0"},
            {"index": "prices_effective", "docs.count": null},
            {"index": "prices_effective_by_time_of_day", "docs.count": null},
            {"index": "spans", "docs.count": "0"},
            {"index": "ss4o_logs-icegate-default", "docs.count": "0"},
            {"index": "ss4o_metrics-icegate-default", "docs.count": "0"},
            {"index": "ss4o_traces-icegate-default", "docs.count": "0"}
        ])
    );
    server.shutdown().await;
}

/// Without an expression every index is listed, exact-name tables included, which no wildcard
/// selects. Like a wildcard, the listing honours whether `expand_wildcards` expands to open
/// indices: `expand_wildcards=none` lists nothing, and a value that names no expansion is refused,
/// as it is under an expression.
#[tokio::test]
async fn cat_indices_without_an_expression_honours_expand_wildcards() {
    let (server, _catalog) = TestServer::start().await;

    let (no_expansion_status, no_expansion_body) =
        get_cat_json(&server, TENANT, "/_cat/indices?format=json&expand_wildcards=none")
            .await
            .expect("a JSON answer");
    let (unknown_expansion_status, unknown_expansion_body) =
        get_cat_json(&server, TENANT, "/_cat/indices?format=json&expand_wildcards=bogus")
            .await
            .expect("a JSON answer");

    assert_eq!(no_expansion_status, 200);
    assert_eq!(no_expansion_body, json!([]));
    assert_eq!(unknown_expansion_status, 400);
    assert_eq!(unknown_expansion_body["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

/// A listing whose `h` names no figure column counts nothing, so a table whose figures cannot be
/// counted is still listed. `rates` carries no `tenant_id` and is not a global table, so no
/// tenant's share of it can be counted: the same listing with `docs.count` answers 500.
#[tokio::test]
async fn cat_indices_without_a_figure_column_lists_a_table_it_cannot_count() {
    let (server, catalog) = TestServer::start().await;
    create_table_without_tenant_id(&catalog, "rates")
        .await
        .expect("the rates table");

    let (status, body) = get_cat_json(&server, TENANT, "/_cat/indices/rates?format=json&h=index,status,health")
        .await
        .expect("a JSON answer");
    let (count_status, count_body) =
        get_cat_json(&server, TENANT, "/_cat/indices/rates?format=json&h=index,docs.count")
            .await
            .expect("a JSON answer");

    assert_eq!(status, 200);
    assert_eq!(body, json!([{"index": "rates", "status": "open", "health": "green"}]));
    assert_eq!(count_status, 500);
    assert_eq!(count_body["error"]["type"], "exception");
    server.shutdown().await;
}

#[tokio::test]
async fn cat_indices_renders_text_with_an_optional_header() {
    let (server, _catalog) = TestServer::start().await;

    let (header_status, content_type, text_with_header) =
        get_cat_text(&server, "/_cat/indices/logs?v&h=index,docs.count")
            .await
            .expect("a text answer");
    let (headerless_status, _, text_without_header) = get_cat_text(&server, "/_cat/indices/logs?h=index,docs.count")
        .await
        .expect("a text answer");

    assert_eq!((header_status, headerless_status), (200, 200));
    assert_eq!(content_type, "text/plain; charset=UTF-8");
    assert_eq!(text_with_header, "index docs.count\nlogs 0\n");
    assert_eq!(text_without_header, "logs 0\n");
    server.shutdown().await;
}

/// A null cell (a view's count) is an empty text cell: the separator before it stays, so the row
/// ends in a space.
#[tokio::test]
async fn cat_indices_renders_a_view_count_as_an_empty_text_cell() {
    let (server, _catalog) = TestServer::start().await;

    let (status, _, text) = get_cat_text(&server, "/_cat/indices/prices_effective?v&h=index,docs.count")
        .await
        .expect("a text answer");

    assert_eq!(status, 200);
    assert_eq!(text, "index docs.count\nprices_effective \n");
    server.shutdown().await;
}

/// The same route answers the known column, so the refusal is the column's own.
#[tokio::test]
async fn cat_indices_refuses_an_unknown_column() {
    let (server, _catalog) = TestServer::start().await;

    let (known_status, _) = get_cat_json(&server, TENANT, "/_cat/indices?format=json&h=index")
        .await
        .expect("a JSON answer");
    let (status, body) = get_cat_json(&server, TENANT, "/_cat/indices?format=json&h=index,nonsense")
        .await
        .expect("a JSON answer");

    assert_eq!(known_status, 200);
    assert_eq!(status, 400);
    assert_eq!(body["status"], 400);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

/// Only plain byte counts (`bytes=b`) are served besides the default human sizes; any other unit
/// is refused rather than rendered in a unit the caller did not ask for.
#[tokio::test]
async fn cat_indices_refuses_a_byte_unit_other_than_b() {
    let (server, _catalog) = TestServer::start().await;

    let (plain_bytes_status, _) = get_cat_json(&server, TENANT, "/_cat/indices?format=json&bytes=b")
        .await
        .expect("a JSON answer");
    let (status, body) = get_cat_json(&server, TENANT, "/_cat/indices?format=json&bytes=kb")
        .await
        .expect("a JSON answer");

    assert_eq!(plain_bytes_status, 200);
    assert_eq!(status, 400);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

#[tokio::test]
async fn cat_indices_answers_404_for_a_missing_index() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = get_cat_json(&server, TENANT, "/_cat/indices/nosuch?format=json")
        .await
        .expect("a JSON answer");

    assert_eq!(status, 404);
    assert_eq!(body["error"]["type"], "index_not_found_exception");
    assert_eq!(body["error"]["index"], "nosuch");
    server.shutdown().await;
}

/// An unknown parameter could change what the listing means, so both endpoints refuse it. Each
/// path answers without the parameter, so the refusal is the parameter's own.
#[tokio::test]
async fn cat_endpoints_refuse_an_unknown_parameter() {
    let (server, _catalog) = TestServer::start().await;

    for path in ["/_cat/indices?format=json", "/_cat/plugins?format=json"] {
        let (known_status, _) = get_cat_json(&server, TENANT, path).await.expect("a JSON answer");
        let unknown_parameter_path = format!("{path}&unknown_knob=1");
        let (status, body) = get_cat_json(&server, TENANT, &unknown_parameter_path)
            .await
            .expect("a JSON answer");

        assert_eq!(known_status, 200, "{path}");
        assert_eq!(status, 400, "{unknown_parameter_path}");
        assert_eq!(
            body["error"]["type"], "illegal_argument_exception",
            "{unknown_parameter_path}"
        );
    }
    server.shutdown().await;
}

/// An index-picker-style request names its columns with `h`: each row holds exactly those
/// columns, in the order `h` gives them.
#[tokio::test]
async fn cat_indices_with_an_h_list_renders_exactly_those_columns_in_that_order() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = get_cat_json(
        &server,
        TENANT,
        "/_cat/indices?format=json&h=index,status,health,docs.count",
    )
    .await
    .expect("a JSON answer");

    assert_eq!(status, 200);
    let rows = body.as_array().expect("rows");
    assert_eq!(rows.len(), 12);
    for row in rows {
        let columns: Vec<&str> = row.as_object().expect("a row object").keys().map(String::as_str).collect();
        assert_eq!(columns, ["index", "status", "health", "docs.count"], "{row}");
    }
    assert_eq!(
        rows[1],
        json!({"index": "logs", "status": "open", "health": "green", "docs.count": "0"})
    );
    server.shutdown().await;
}

/// The trailing-slash form names no index, so it answers like `/_cat/indices`. Without `h`, each
/// row has the default columns in IceGate's order and every value is a string. `uuid` is checked
/// structurally: 22 URL-safe characters, stable across the two requests, distinct per index.
#[tokio::test]
async fn cat_indices_with_a_trailing_slash_lists_the_same_rows_in_the_default_column_order() {
    let (server, _catalog) = TestServer::start().await;

    let (trailing_slash_status, trailing_slash_body) = get_cat_json(&server, TENANT, "/_cat/indices/?format=json")
        .await
        .expect("a JSON answer");
    let (_, bare_path_body) = get_cat_json(&server, TENANT, "/_cat/indices?format=json")
        .await
        .expect("a JSON answer");

    assert_eq!(trailing_slash_status, 200);
    assert_eq!(trailing_slash_body, bare_path_body);
    let rows = trailing_slash_body.as_array().expect("rows");
    let find_row = |name: &str| {
        rows.iter()
            .find(|row| row["index"] == name)
            .expect("a row for the index")
            .clone()
    };
    let logs_row = find_row("logs");
    let columns: Vec<&str> = logs_row.as_object().expect("a row object").keys().map(String::as_str).collect();
    assert_eq!(
        columns,
        [
            "health",
            "status",
            "index",
            "uuid",
            "pri",
            "rep",
            "docs.count",
            "docs.deleted",
            "store.size",
            "pri.store.size"
        ]
    );
    let uuid = logs_row["uuid"].as_str().expect("a string uuid").to_string();
    assert_eq!(uuid.len(), 22);
    assert!(
        uuid.chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
    );
    assert_ne!(find_row("spans")["uuid"], logs_row["uuid"]);
    let mut logs_row_without_uuid = logs_row.clone();
    logs_row_without_uuid.as_object_mut().expect("a row object").remove("uuid");
    assert_eq!(
        logs_row_without_uuid,
        json!({
            "health": "green", "status": "open", "index": "logs",
            "pri": "1", "rep": "0", "docs.count": "0", "docs.deleted": "0",
            "store.size": "0b", "pri.store.size": "0b"
        })
    );
    server.shutdown().await;
}

/// Ten of the tenant's log rows (counted by `logs` and `ss4o_logs-icegate-default`) and nine rows
/// of the global `prices` table: two counts whose text order ("10" before "9") is the reverse of
/// their numeric order, so a sort by rendered text cannot pass for a numeric one.
async fn commit_ten_logs_and_nine_prices(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_log_rows(catalog, &build_rows(TENANT, 10)).await?;
    let models: Vec<String> = (0..9).map(|position| format!("model-{position}")).collect();
    let price_rows: Vec<PriceRow<'_>> = models
        .iter()
        .map(|model| PriceRow {
            model,
            input_usd_per_1m: None,
        })
        .collect();
    commit_price_rows(catalog, &price_rows).await?;
    Ok(())
}

/// `s` orders rows by a figure numerically, either way, and a null figure (a view's count) sorts
/// last in both directions.
#[tokio::test]
async fn cat_indices_sorts_by_a_figure_numerically_with_nulls_last() {
    let (server, catalog) = TestServer::start().await;
    commit_ten_logs_and_nine_prices(&catalog)
        .await
        .expect("ten log rows and nine prices");

    let listing = "/_cat/indices/prices_effective,logs,prices?format=json&h=index,docs.count";
    let (descending_status, descending_body) = get_cat_json(&server, TENANT, &format!("{listing}&s=docs.count:desc"))
        .await
        .expect("a JSON answer");
    let (ascending_status, ascending_body) = get_cat_json(&server, TENANT, &format!("{listing}&s=docs.count"))
        .await
        .expect("a JSON answer");

    assert_eq!((descending_status, ascending_status), (200, 200));
    assert_eq!(
        descending_body,
        json!([
            {"index": "logs", "docs.count": "10"},
            {"index": "prices", "docs.count": "9"},
            {"index": "prices_effective", "docs.count": null}
        ])
    );
    assert_eq!(
        ascending_body,
        json!([
            {"index": "prices", "docs.count": "9"},
            {"index": "logs", "docs.count": "10"},
            {"index": "prices_effective", "docs.count": null}
        ])
    );
    server.shutdown().await;
}

/// Later `s` keys break the ties of earlier ones, and a key may name a column `h` does not
/// render: the rows are ordered by it all the same.
#[tokio::test]
async fn cat_indices_breaks_ties_with_later_sort_keys_and_sorts_by_an_unrendered_column() {
    let (server, catalog) = TestServer::start().await;
    commit_ten_logs_and_nine_prices(&catalog)
        .await
        .expect("ten log rows and nine prices");

    let listing = "/_cat/indices/prices,logs,ss4o_logs-icegate-default?format=json";
    let (tie_status, tie_body) = get_cat_json(
        &server,
        TENANT,
        &format!("{listing}&h=index,docs.count&s=docs.count:desc,index:desc"),
    )
    .await
    .expect("a JSON answer");
    let (unrendered_status, unrendered_body) =
        get_cat_json(&server, TENANT, &format!("{listing}&h=index&s=docs.count,index"))
            .await
            .expect("a JSON answer");

    assert_eq!((tie_status, unrendered_status), (200, 200));
    assert_eq!(
        tie_body,
        json!([
            {"index": "ss4o_logs-icegate-default", "docs.count": "10"},
            {"index": "logs", "docs.count": "10"},
            {"index": "prices", "docs.count": "9"}
        ])
    );
    assert_eq!(
        unrendered_body,
        json!([
            {"index": "prices"},
            {"index": "logs"},
            {"index": "ss4o_logs-icegate-default"}
        ])
    );
    server.shutdown().await;
}

/// `s=index:desc` reverses the name order of the whole listing, and the text rendering carries
/// the same order.
#[tokio::test]
async fn cat_indices_sorts_by_index_name_in_text_and_json() {
    let (server, _catalog) = TestServer::start().await;

    let (json_status, json_body) = get_cat_json(&server, TENANT, "/_cat/indices?format=json&h=index&s=index:desc")
        .await
        .expect("a JSON answer");
    let (text_status, _, text) = get_cat_text(&server, "/_cat/indices/logs,events,spans?h=index&s=index:desc")
        .await
        .expect("a text answer");

    assert_eq!((json_status, text_status), (200, 200));
    let names: Vec<&str> = json_body
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["index"].as_str().expect("a string index"))
        .collect();
    assert_eq!(
        names,
        [
            "ss4o_traces-icegate-default",
            "ss4o_metrics-icegate-default",
            "ss4o_logs-icegate-default",
            "spans",
            "prices_effective_by_time_of_day",
            "prices_effective",
            "prices",
            "otel-v1-apm-span-000001",
            "operations",
            "metrics",
            "logs",
            "events"
        ]
    );
    assert_eq!(text, "spans\nlogs\nevents\n");
    server.shutdown().await;
}

/// An `s` key naming no column, a direction other than `asc` or `desc`, or an empty key is the
/// caller's mistake. The same listing answers with a valid key, so each refusal is the key's own.
#[tokio::test]
async fn cat_indices_refuses_an_unknown_sort_column_or_direction() {
    let (server, _catalog) = TestServer::start().await;

    let (valid_status, _) = get_cat_json(&server, TENANT, "/_cat/indices?format=json&s=index:asc")
        .await
        .expect("a JSON answer");
    assert_eq!(valid_status, 200);
    for sort in ["nonsense", "index:sideways", "", "index,", "index:"] {
        let path = format!("/_cat/indices?format=json&s={sort}");
        let (status, body) = get_cat_json(&server, TENANT, &path).await.expect("a JSON answer");

        assert_eq!(status, 400, "{path}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{path}");
    }
    server.shutdown().await;
}

/// A plugin listing with an upper-case `format=JSON`, which asks for JSON because `format` is
/// matched case-insensitively: the plugin APIs IceGate emulates and no other, in component order,
/// named after the node and versioned `<version>.0`.
#[tokio::test]
async fn cat_plugins_lists_the_emulated_plugins_for_an_upper_case_json_format() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = get_cat_json(&server, TENANT, "/_cat/plugins?format=JSON")
        .await
        .expect("a JSON answer");

    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!([
            {"name": "icegate-node", "component": "opensearch-observability", "version": "3.9.0.0"},
            {"name": "icegate-node", "component": "opensearch-sql", "version": "3.9.0.0"}
        ])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn cat_plugins_renders_text_with_an_optional_header() {
    let (server, _catalog) = TestServer::start().await;

    let (header_status, _, text_with_header) = get_cat_text(&server, "/_cat/plugins?v").await.expect("a text answer");
    let (headerless_status, _, text_without_header) =
        get_cat_text(&server, "/_cat/plugins").await.expect("a text answer");

    assert_eq!((header_status, headerless_status), (200, 200));
    assert_eq!(
        text_with_header,
        "name component version\nicegate-node opensearch-observability 3.9.0.0\nicegate-node opensearch-sql 3.9.0.0\n"
    );
    assert_eq!(
        text_without_header,
        "icegate-node opensearch-observability 3.9.0.0\nicegate-node opensearch-sql 3.9.0.0\n"
    );
    server.shutdown().await;
}
