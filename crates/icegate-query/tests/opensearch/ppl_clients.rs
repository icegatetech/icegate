//! Client-shaped PPL requests and tenant isolation. Each request is a Workbench-, Discover-,
//! Observability-Logs- or Grafana-style shape over rows written here, and every expected row is stated
//! by hand. These tests pin IceGate's answers only; client compatibility is the end-to-end job's.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
use iceberg::Catalog;
use iceberg::spec::{NestedField, PrimitiveType, Type};
use icegate_common::schema::{
    COL_BODY, COL_CURRENCY, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES, COL_MIN_INPUT_TOKENS, COL_MODEL,
    COL_OBSERVED_TIMESTAMP, COL_PROVIDER, COL_REGION, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME,
    COL_SERVICE_TIER, COL_SEVERITY_TEXT, COL_SOURCE, COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID,
    COL_VALID_FROM, COL_VALID_FROM_SOURCE,
};
use icegate_common::{LOGS_TABLE, PRICES_TABLE};
use serde_json::{Map, Value, json};

use super::data::{
    COMMITTED_WAL_OFFSET, build_service_log_row_at_micros, commit_log_rows, commit_table_columns,
    create_exact_name_table, required_string, write_log_wal_segment,
};
use super::harness::{PplReply, TestServer};
use super::requests::send_request;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";
const PPL_PATH: &str = "/_plugins/_ppl";
const PPL_ERROR_CONTENT_TYPE: &str = "application/json; charset=UTF-8";

/// 2026-10-08T12:00:00Z in microseconds, computed outside the code.
const NOON_MICROS: i64 = 1_791_460_800_000_000;
/// One minute in microseconds.
const MINUTE_MICROS: i64 = 60_000_000;

/// `valid_to` is a column of the `prices_effective` view only; no table and no shared constant has it.
const VALID_TO: &str = "valid_to";

/// The test-created `marks` table and its two own columns.
const MARKS_TABLE: &str = "marks";
const MARK_COLUMN: &str = "mark";
const WEIGHT_COLUMN: &str = "weight";

/// The time window a Discover-style request inserts after its first pipe.
const DISCOVER_TIME_WHERE: &str = concat!(
    "WHERE `timestamp` >= '2026-10-08 12:00:00.000' ",
    "AND `timestamp` <= '2026-10-08 13:00:00.000'"
);

/// A Discover-style query id; its value is not interpreted.
const QUERY_ID: &str = "1f0c3a5e-5b7e-4c8e-9a52-6f1d2c3b4a59";

/// The tenant-A rows behind every client shape (12:00 is `NOON_MICROS`), with one tenant-B row in the batch.
async fn commit_client_rows(catalog: &Arc<dyn Catalog>) {
    commit_log_rows(
        catalog,
        &[
            build_service_log_row_at_micros(TENANT_A, NOON_MICROS - 1, "checkout", "INFO", "early"),
            build_service_log_row_at_micros(TENANT_A, NOON_MICROS, "checkout", "INFO", "a"),
            build_service_log_row_at_micros(TENANT_A, NOON_MICROS + 10 * MINUTE_MICROS, "cart", "WARN", "b"),
            build_service_log_row_at_micros(TENANT_A, NOON_MICROS + 30 * MINUTE_MICROS, "checkout", "WARN", "c"),
            build_service_log_row_at_micros(TENANT_A, NOON_MICROS + 60 * MINUTE_MICROS, "checkout", "ERROR", "d"),
            build_service_log_row_at_micros(
                TENANT_A,
                NOON_MICROS + 60 * MINUTE_MICROS + 1_000,
                "checkout",
                "ERROR",
                "late, retried",
            ),
            build_service_log_row_at_micros(
                TENANT_B,
                NOON_MICROS + 30 * MINUTE_MICROS,
                "checkout",
                "ERROR",
                "tenant b",
            ),
        ],
    )
    .await
    .expect("commit logs");
}

/// The JSON body of a 200 answer.
fn read_ok_json(reply: &PplReply) -> Value {
    assert_eq!(reply.status, 200, "{}", reply.text);
    reply.parse_json_body()
}

/// Rows as name → value objects, for answers whose column order is not the contract under test.
fn read_rows_as_objects(body: &Value) -> Vec<Map<String, Value>> {
    let names: Vec<&str> = body["schema"]
        .as_array()
        .expect("schema")
        .iter()
        .map(|column| column["name"].as_str().expect("column name"))
        .collect();
    body["datarows"]
        .as_array()
        .expect("datarows")
        .iter()
        .map(|row| {
            names
                .iter()
                .zip(row.as_array().expect("row"))
                .map(|(name, value)| ((*name).to_owned(), value.clone()))
                .collect()
        })
        .collect()
}

fn assert_field_not_found(reply: &PplReply, field: &str) {
    assert_eq!(reply.status, 400, "{}", reply.text);
    assert_eq!(reply.content_type, PPL_ERROR_CONTENT_TYPE);
    let body = reply.parse_json_body();
    assert_eq!(body["status"], 400, "{body}");
    assert_eq!(body["error"]["type"], "SemanticCheckException", "{body}");
    assert_eq!(body["error"]["code"], "FIELD_NOT_FOUND", "{body}");
    assert_eq!(body["error"]["reason"], format!("Field [{field}] not found."), "{body}");
    assert_eq!(body["error"]["context"]["field"], field, "{body}");
    assert!(
        body["error"]["details"].as_str().is_some_and(|details| !details.is_empty()),
        "{body}"
    );
}

fn build_column(name: &str, column_type: &str) -> Value {
    json!({"name": name, "type": column_type})
}

fn build_string_array(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values.iter().copied()))
}

/// The columns of the test-created `marks` table: a tenant-scoped exact-name table.
fn build_marks_fields() -> Vec<NestedField> {
    vec![
        required_string(1, COL_TENANT_ID),
        required_string(2, MARK_COLUMN),
        NestedField::optional(3, WEIGHT_COLUMN, Type::Primitive(PrimitiveType::Long)),
    ]
}

#[tokio::test]
async fn a_workbench_style_query_download_and_explain_answer_as_stated() {
    let (server, catalog) = TestServer::start().await;
    commit_client_rows(&catalog).await;
    // The Workbench-style body is exactly `{query}`.
    let query = "source=logs | where severity_text = 'ERROR' | fields service_name, severity_text, body | sort body";

    let table = read_ok_json(&server.post_ppl(TENANT_A, PPL_PATH, &json!({ "query": query })).await);
    assert_eq!(
        table,
        json!({
            "schema": [
                build_column(COL_SERVICE_NAME, "string"),
                build_column(COL_SEVERITY_TEXT, "string"),
                build_column(COL_BODY, "string")
            ],
            "datarows": [["checkout", "ERROR", "d"], ["checkout", "ERROR", "late, retried"]],
            "total": 2,
            "size": 2
        })
    );

    let download = server
        .post_ppl(TENANT_A, "/_plugins/_ppl?format=csv", &json!({ "query": query }))
        .await;
    assert_eq!(download.status, 200, "{}", download.text);
    assert_eq!(
        download.text,
        "service_name,severity_text,body\ncheckout,ERROR,d\ncheckout,ERROR,\"late, retried\""
    );

    let explanation = read_ok_json(
        &server
            .post_ppl(TENANT_A, "/_plugins/_ppl/_explain", &json!({ "query": query }))
            .await,
    );
    assert!(
        explanation["calcite"]["logical"].as_str().is_some_and(|plan| !plan.is_empty()),
        "{explanation}"
    );
    assert!(
        explanation["calcite"]["physical"].as_str().is_some_and(|plan| !plan.is_empty()),
        "{explanation}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_workbench_style_index_tree_lists_the_tenants_indices() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(&catalog, MARKS_TABLE, build_marks_fields())
        .await
        .expect("create marks");
    let (status, kibana_creation) = send_request(&server, reqwest::Method::PUT, TENANT_A, "/.kibana", Some(&json!({})))
        .await
        .expect("create .kibana");
    assert_eq!(status, 200, "{kibana_creation}");

    let body = read_ok_json(
        &server
            .post_ppl(TENANT_A, "/_plugins/_sql", &json!({"query": "SHOW tables LIKE '%'"}))
            .await,
    );
    assert_eq!(body["schema"][2]["name"], "TABLE_NAME", "{body}");
    let names: Vec<&str> = body["datarows"]
        .as_array()
        .expect("datarows")
        .iter()
        .map(|row| row[2].as_str().expect("index name at column 2"))
        .collect();
    // The exact listing pins membership, the absence of dot indices and the order by index name.
    assert_eq!(
        names,
        [
            "events",
            "logs",
            "marks",
            "metrics",
            "operations",
            "otel-v1-apm-span",
            "prices",
            "prices_effective",
            "prices_effective_by_time_of_day",
            "spans",
            "ss4o_logs-icegate-default",
            "ss4o_metrics-icegate-default",
            "ss4o_traces-icegate-default",
        ]
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_discover_ppl_style_documents_query_with_inserted_time_where_and_sort_answers_the_stated_rows() {
    let (server, catalog) = TestServer::start().await;
    commit_client_rows(&catalog).await;
    let body = json!({
        "query": format!("source=logs | {DISCOVER_TIME_WHERE} | sort - `timestamp`"),
        "fetch_size": 500,
        "lang": "PPL",
        "queryId": QUERY_ID,
        "highlight": {
            "pre_tags": ["@opensearch-dashboards-highlighted-field@"],
            "post_tags": ["@/opensearch-dashboards-highlighted-field@"],
            "fields": {"*": {}},
            "fragment_size": 2_147_483_647
        }
    });
    let answer = read_ok_json(&server.post_ppl(TENANT_A, "/_plugins/_ppl?format=jdbc", &body).await);
    let build_row = |service: &str, time: &str, severity: &str, text: &str| {
        json!([service, time, time, time, null, null, severity, text, {}, {}, {}])
    };
    assert_eq!(
        answer,
        json!({
            "schema": [
                build_column(COL_SERVICE_NAME, "string"),
                build_column(COL_TIMESTAMP, "timestamp"),
                build_column(COL_OBSERVED_TIMESTAMP, "timestamp"),
                build_column(COL_INGESTED_TIMESTAMP, "timestamp"),
                build_column(COL_TRACE_ID, "string"),
                build_column(COL_SPAN_ID, "string"),
                build_column(COL_SEVERITY_TEXT, "string"),
                build_column(COL_BODY, "string"),
                build_column(COL_RESOURCE_ATTRIBUTES, "struct"),
                build_column(COL_SCOPE_ATTRIBUTES, "struct"),
                build_column(COL_LOG_ATTRIBUTES, "struct")
            ],
            "datarows": [
                build_row("checkout", "2026-10-08 13:00:00", "ERROR", "d"),
                build_row("checkout", "2026-10-08 12:30:00", "WARN", "c"),
                build_row("cart", "2026-10-08 12:10:00", "WARN", "b"),
                build_row("checkout", "2026-10-08 12:00:00", "INFO", "a")
            ],
            "total": 4,
            "size": 4
        })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_discover_ppl_style_histogram_answers_the_stated_buckets() {
    let (server, catalog) = TestServer::start().await;
    commit_client_rows(&catalog).await;
    let answer = read_ok_json(
        &server
            .post_ppl(
                TENANT_A,
                "/_plugins/_ppl?format=jdbc",
                &json!({
                    "query": format!("source=logs | {DISCOVER_TIME_WHERE} | stats count() by span(`timestamp`, 30m)"),
                    "fetch_size": 2000,
                    "queryId": QUERY_ID
                }),
            )
            .await,
    );
    // Column 0 is the count, column 1 the bucket key; rows in ascending key order.
    assert_eq!(
        answer,
        json!({
            "schema": [build_column("count()", "bigint"), build_column("span(timestamp,30m)", "timestamp")],
            "datarows": [
                [2, "2026-10-08 12:00:00"],
                [1, "2026-10-08 12:30:00"],
                [1, "2026-10-08 13:00:00"]
            ],
            "total": 3,
            "size": 3
        })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_histogram_over_stats_output_missing_its_time_field_is_a_400_naming_the_field() {
    let (server, catalog) = TestServer::start().await;
    commit_client_rows(&catalog).await;
    let reply = server
        .post_ppl(
            TENANT_A,
            PPL_PATH,
            &json!({"query": concat!(
                "source=logs | stats count() as events by service_name, severity_text ",
                "| stats count() by span(timestamp, 1h)"
            )}),
        )
        .await;
    assert_field_not_found(&reply, COL_TIMESTAMP);
    server.shutdown().await;
}

#[tokio::test]
async fn a_histogram_over_projected_fields_answers_200_with_its_buckets() {
    let (server, catalog) = TestServer::start().await;
    commit_client_rows(&catalog).await;
    let answer = read_ok_json(
        &server
            .post_ppl(
                TENANT_A,
                PPL_PATH,
                &json!({"query": concat!(
                    "source=logs | fields timestamp, service_name, severity_text, body ",
                    "| stats count() by span(timestamp, 1h)"
                )}),
            )
            .await,
    );
    assert_eq!(
        answer,
        json!({
            "schema": [build_column("count()", "bigint"), build_column("span(timestamp,1h)", "timestamp")],
            "datarows": [
                [1, "2026-10-08 11:00:00"],
                [3, "2026-10-08 12:00:00"],
                [2, "2026-10-08 13:00:00"]
            ],
            "total": 3,
            "size": 3
        })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn an_observability_logs_style_query_and_viz_count_chart_answer_as_stated() {
    let (server, catalog) = TestServer::start().await;
    commit_client_rows(&catalog).await;
    let window = "where timestamp >= '2026-10-08 12:00:00.000000' and timestamp <= '2026-10-08 13:00:00.000000'";

    let events = read_ok_json(
        &server
            .post_ppl(
                TENANT_A,
                PPL_PATH,
                &json!({"query": format!("source=logs | {window} | fields severity_text, body | sort body")}),
            )
            .await,
    );
    assert_eq!(
        events["datarows"],
        json!([["INFO", "a"], ["WARN", "b"], ["WARN", "c"], ["ERROR", "d"]])
    );
    assert_eq!(events["total"], 4);
    assert_eq!(events["size"], 4);

    // The viz answer keys the count column `count()`; the count field comes first, the bucket key second.
    let chart = read_ok_json(
        &server
            .post_ppl(
                TENANT_A,
                "/_plugins/_ppl?format=viz",
                &json!({"query": format!("source=logs | {window} | stats count() by span(timestamp, 1h)")}),
            )
            .await,
    );
    assert_eq!(
        chart,
        json!({
            "data": {
                "count()": [3, 1],
                "span(timestamp,1h)": ["2026-10-08 12:00:00", "2026-10-08 13:00:00"]
            },
            "metadata": {
                "fields": [
                    {"name": "count()", "type": "long"},
                    {"name": "span(timestamp,1h)", "type": "timestamp"}
                ]
            },
            "size": 2,
            "status": 200
        })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_grafana_ppl_style_query_with_a_time_where_after_the_first_pipe_answers_two_columns() {
    let (server, catalog) = TestServer::start().await;
    commit_client_rows(&catalog).await;
    let answer = read_ok_json(
        &server
            .post_ppl(
                TENANT_A,
                PPL_PATH,
                &json!({"query": concat!(
                    "source=logs ",
                    "| where `timestamp` >= timestamp('2026-10-08 12:00:00') ",
                    "and `timestamp` <= timestamp('2026-10-08 13:00:00') ",
                    "| stats count() by span(timestamp, 30m)"
                )}),
            )
            .await,
    );
    // Exactly two columns, a number and a time, in this order.
    assert_eq!(
        answer,
        json!({
            "schema": [build_column("count()", "bigint"), build_column("span(timestamp,30m)", "timestamp")],
            "datarows": [
                [2, "2026-10-08 12:00:00"],
                [1, "2026-10-08 12:30:00"],
                [1, "2026-10-08 13:00:00"]
            ],
            "total": 3,
            "size": 3
        })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn an_ad_hoc_where_on_a_by_field_answers_the_stated_rows() {
    let (server, catalog) = TestServer::start().await;
    commit_client_rows(&catalog).await;
    for (filter, expected) in [
        ("= 'checkout'", json!({"events": 5, "service_name": "checkout"})),
        ("!= 'checkout'", json!({"events": 1, "service_name": "cart"})),
    ] {
        let answer = read_ok_json(
            &server
                .post_ppl(
                    TENANT_A,
                    PPL_PATH,
                    &json!({"query": format!(
                        "source=logs | stats count() as events by service_name | where `service_name` {filter}"
                    )}),
                )
                .await,
        );
        let rows: Vec<Value> = read_rows_as_objects(&answer).into_iter().map(Value::Object).collect();
        assert_eq!(Value::Array(rows), json!([expected]), "{filter}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn an_ad_hoc_where_on_a_field_stats_dropped_is_field_not_found() {
    let (server, catalog) = TestServer::start().await;
    commit_client_rows(&catalog).await;
    let reply = server
        .post_ppl(
            TENANT_A,
            PPL_PATH,
            &json!({"query": concat!(
                "source=logs | stats count() as events by service_name ",
                "| where `severity_text` = 'ERROR'"
            )}),
        )
        .await;
    assert_field_not_found(&reply, COL_SEVERITY_TEXT);
    server.shutdown().await;
}

#[tokio::test]
async fn plain_source_multi_index_and_exact_name_tables_never_return_another_tenants_rows() {
    let (server, catalog) = TestServer::start().await;
    // Both tenants in one Iceberg batch and in one WAL segment file.
    commit_log_rows(
        &catalog,
        &[
            build_service_log_row_at_micros(TENANT_A, NOON_MICROS, "checkout", "INFO", "a1"),
            build_service_log_row_at_micros(TENANT_A, NOON_MICROS, "checkout", "INFO", "a2"),
            build_service_log_row_at_micros(TENANT_B, NOON_MICROS, "checkout", "INFO", "b1"),
        ],
    )
    .await
    .expect("commit logs");
    write_log_wal_segment(
        &server.wal_store,
        &catalog,
        COMMITTED_WAL_OFFSET + 1,
        &[
            build_service_log_row_at_micros(TENANT_A, NOON_MICROS, "checkout", "INFO", "a3"),
            build_service_log_row_at_micros(TENANT_B, NOON_MICROS, "checkout", "INFO", "b2"),
        ],
    )
    .await
    .expect("write WAL segment");
    create_exact_name_table(&catalog, MARKS_TABLE, build_marks_fields())
        .await
        .expect("create marks");
    commit_table_columns(
        &catalog,
        MARKS_TABLE,
        3,
        &[
            (COL_TENANT_ID, build_string_array(&[TENANT_A, TENANT_A, TENANT_B])),
            (MARK_COLUMN, build_string_array(&["a-mark-1", "a-mark-2", "b-mark-1"])),
            (WEIGHT_COLUMN, Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
        ],
    )
    .await
    .expect("commit marks");

    let logs_count = format!("source={LOGS_TABLE} | stats count()");
    let marks_listing = format!("source={MARKS_TABLE} | fields {MARK_COLUMN} | sort {MARK_COLUMN}");
    let both_count = format!("source={LOGS_TABLE},{MARKS_TABLE} | stats count()");
    // The union row by row: a leaked row of the other tenant changes a row even where a count
    // would not (one leaked, one dropped). NULL sorts first, so the marks come before the logs.
    let both_listing =
        format!("source={LOGS_TABLE},{MARKS_TABLE} | fields body, {MARK_COLUMN} | sort body, {MARK_COLUMN}");
    for (tenant, query, expected) in [
        (TENANT_A, logs_count.as_str(), json!([[3]])),
        (TENANT_B, logs_count.as_str(), json!([[2]])),
        (
            TENANT_A,
            "source=logs | fields body | sort body",
            json!([["a1"], ["a2"], ["a3"]]),
        ),
        (
            TENANT_B,
            "source=logs | fields body | sort body",
            json!([["b1"], ["b2"]]),
        ),
        (TENANT_A, marks_listing.as_str(), json!([["a-mark-1"], ["a-mark-2"]])),
        (TENANT_B, marks_listing.as_str(), json!([["b-mark-1"]])),
        (TENANT_A, both_count.as_str(), json!([[5]])),
        (TENANT_B, both_count.as_str(), json!([[3]])),
        (
            TENANT_A,
            both_listing.as_str(),
            json!([
                [null, "a-mark-1"],
                [null, "a-mark-2"],
                ["a1", null],
                ["a2", null],
                ["a3", null]
            ]),
        ),
        (
            TENANT_B,
            both_listing.as_str(),
            json!([[null, "b-mark-1"], ["b1", null], ["b2", null]]),
        ),
    ] {
        let answer = read_ok_json(&server.post_ppl(tenant, PPL_PATH, &json!({ "query": query })).await);
        assert_eq!(answer["datarows"], expected, "{tenant}: {query}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn source_prices_effective_returns_identical_rows_for_every_tenant_including_valid_to() {
    let (server, catalog) = TestServer::start().await;
    // 2026-01-01, 2026-06-01 and 2026-03-01 at 00:00 UTC in microseconds, computed outside the code.
    let valid_from: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        1_767_225_600_000_000,
        1_780_272_000_000_000,
        1_772_323_200_000_000,
    ]));
    commit_table_columns(
        &catalog,
        PRICES_TABLE,
        3,
        &[
            (COL_PROVIDER, build_string_array(&["openai", "openai", "anthropic"])),
            (COL_MODEL, build_string_array(&["gpt-4o", "gpt-4o", "claude"])),
            (
                COL_SERVICE_TIER,
                build_string_array(&["standard", "standard", "standard"]),
            ),
            (COL_REGION, build_string_array(&["global", "global", "global"])),
            (
                COL_MIN_INPUT_TOKENS,
                Arc::new(Int64Array::from(vec![0, 0, 0])) as ArrayRef,
            ),
            (COL_VALID_FROM, valid_from),
            (
                COL_VALID_FROM_SOURCE,
                build_string_array(&["vendor", "vendor", "vendor"]),
            ),
            (COL_CURRENCY, build_string_array(&["USD", "USD", "USD"])),
            (COL_SOURCE, build_string_array(&["test", "test", "test"])),
        ],
    )
    .await
    .expect("commit prices");

    let fields = format!("{COL_PROVIDER}, {COL_MODEL}, {COL_VALID_FROM}, {VALID_TO}");
    let query = format!("source=prices_effective | fields {fields} | sort {COL_MODEL}, {COL_VALID_FROM}");
    let expected = json!({
        "schema": [
            build_column(COL_PROVIDER, "string"),
            build_column(COL_MODEL, "string"),
            build_column(COL_VALID_FROM, "timestamp"),
            build_column(VALID_TO, "timestamp")
        ],
        "datarows": [
            ["anthropic", "claude", "2026-03-01 00:00:00", null],
            ["openai", "gpt-4o", "2026-01-01 00:00:00", "2026-06-01 00:00:00"],
            ["openai", "gpt-4o", "2026-06-01 00:00:00", null]
        ],
        "total": 3,
        "size": 3
    });
    for tenant in [TENANT_A, TENANT_B] {
        let answer = read_ok_json(&server.post_ppl(tenant, PPL_PATH, &json!({ "query": query })).await);
        assert_eq!(answer, expected, "{tenant}");
    }
    server.shutdown().await;
}
