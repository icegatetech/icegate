//! The `viz`, `csv` and `raw` formats and `_explain` over real HTTP, including the plan shapes that
//! are a contract: timestamp literals and leading `where` conjuncts pushed into both scans. Inputs are
//! literal PPL text over rows written here; expected bodies are stated by hand.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, FixedSizeBinaryArray, Int64Array, ListBuilder, StringArray, StringBuilder, TimestampMicrosecondArray,
};
use iceberg::Catalog;
use icegate_common::OPERATIONS_TABLE;
use icegate_common::schema::{
    COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP,
    COL_TRACE_ID,
};
use serde_json::{Value, json};

use super::data::{LogRow, RowWriteError, commit_log_rows, commit_table_columns, write_log_wal_segment};
use super::harness::{PplReply, TestServer};
use super::test_rows::{COMMITTED_WAL_OFFSET, FINISH_REASONS, OPERATION_NAME};

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";
const PPL_JSON_CONTENT_TYPE: &str = "application/json; charset=UTF-8";
const PPL_ERROR_CONTENT_TYPE: &str = "text/plain; charset=UTF-8";

/// 2026-10-08T12:00:00Z in microseconds, computed outside the code.
const NOON_MICROS: i64 = 1_791_460_800_000_000;
/// One minute in microseconds.
const MINUTE_MICROS: i64 = 60_000_000;

const fn build_log_row<'a>(
    tenant_id: &'a str,
    timestamp_micros: i64,
    service_name: Option<&'a str>,
    severity_text: Option<&'a str>,
    body: Option<&'a str>,
) -> LogRow<'a> {
    LogRow {
        tenant_id,
        timestamp_micros,
        service_name,
        severity_text,
        body,
        resource_attributes: &[],
        log_attributes: &[],
    }
}

async fn post_ppl_query(server: &TestServer, path_and_query: &str, query: &str) -> PplReply {
    server.post_ppl(TENANT_A, path_and_query, &json!({ "query": query })).await
}

/// The one line of `plan` naming `node`.
fn find_plan_line<'a>(plan: &'a str, node: &str) -> &'a str {
    plan.lines()
        .find(|line| line.contains(node))
        .unwrap_or_else(|| panic!("no {node} in the plan:\n{plan}"))
}

/// Whether `text` holds `number` as a whole number, not as the prefix of a longer one (a nanosecond
/// literal starts with the same digits as the microsecond one).
fn contains_whole_number(text: &str, number: &str) -> bool {
    text.match_indices(number).any(|(start, _)| {
        !text[start + number.len()..]
            .chars()
            .next()
            .is_some_and(|next| next.is_ascii_digit())
    })
}

/// The `calcite.logical` and `calcite.physical` texts of an explain answer.
fn read_explained_plans(reply: &PplReply) -> (String, String) {
    assert_eq!(reply.status, 200, "{}", reply.text);
    let body = reply.parse_json_body();
    (
        body["calcite"]["logical"].as_str().expect("logical plan text").to_owned(),
        body["calcite"]["physical"].as_str().expect("physical plan text").to_owned(),
    )
}

/// The `error` object of an OpenSearch-envelope 400, after checking its status and type.
fn read_illegal_argument_error(reply: &PplReply) -> Value {
    assert_eq!(reply.status, 400, "{}", reply.text);
    let body = reply.parse_json_body();
    assert_eq!(body["status"], 400);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    assert_eq!(body["error"]["root_cause"][0]["type"], "illegal_argument_exception");
    body["error"].clone()
}

#[tokio::test]
async fn viz_renders_columns_with_its_own_type_names() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[
            build_log_row(TENANT_A, NOON_MICROS, Some("checkout"), Some("INFO"), Some("a")),
            build_log_row(
                TENANT_A,
                NOON_MICROS + 30 * MINUTE_MICROS,
                Some("checkout"),
                Some("INFO"),
                Some("b"),
            ),
            build_log_row(
                TENANT_A,
                NOON_MICROS + 30 * MINUTE_MICROS,
                Some("cart"),
                Some("INFO"),
                Some("c"),
            ),
            build_log_row(
                TENANT_A,
                NOON_MICROS + 75 * MINUTE_MICROS,
                Some("checkout"),
                Some("INFO"),
                Some("d"),
            ),
            build_log_row(
                TENANT_B,
                NOON_MICROS,
                Some("checkout"),
                Some("INFO"),
                Some("other tenant"),
            ),
        ],
    )
    .await
    .expect("commit logs");

    let reply = post_ppl_query(
        &server,
        "/_plugins/_ppl?format=viz",
        "source=logs | eval weight = 1.5 | stats count() as events, sum(weight) as total_weight by span(timestamp, 1h) as hour, service_name | fields hour, service_name, events, total_weight | sort hour, service_name",
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(reply.content_type, PPL_JSON_CONTENT_TYPE);
    assert_eq!(
        reply.parse_json_body(),
        json!({
            "data": {
                "hour": ["2026-10-08 12:00:00", "2026-10-08 12:00:00", "2026-10-08 13:00:00"],
                "service_name": ["cart", "checkout", "checkout"],
                "events": [1, 2, 1],
                "total_weight": [1.5, 3.0, 1.5]
            },
            "metadata": {
                "fields": [
                    {"name": "hour", "type": "timestamp"},
                    {"name": "service_name", "type": "keyword"},
                    {"name": "events", "type": "long"},
                    {"name": "total_weight", "type": "double"}
                ]
            },
            "size": 3,
            "status": 200
        })
    );
    server.shutdown().await;
}

/// Rows whose bodies exercise every quoting case, one severity per row so `sort` fixes the order.
async fn commit_quoting_rows(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_log_rows(
        catalog,
        &[
            build_log_row(TENANT_A, NOON_MICROS, None, Some("A"), Some("a,b")),
            build_log_row(TENANT_A, NOON_MICROS, None, Some("B"), Some("say \"hi\"")),
            build_log_row(TENANT_A, NOON_MICROS, None, Some("C"), Some("line1\nline2")),
            build_log_row(TENANT_A, NOON_MICROS, None, Some("D"), None),
            build_log_row(TENANT_A, NOON_MICROS, None, Some("E"), Some("plain")),
            build_log_row(TENANT_A, NOON_MICROS, None, Some("F"), Some("x|y")),
            build_log_row(TENANT_B, NOON_MICROS, None, Some("A"), Some("other tenant")),
        ],
    )
    .await
}

#[tokio::test]
async fn csv_quotes_cells_with_commas_quotes_or_line_breaks_and_renders_null_empty() {
    let (server, catalog) = TestServer::start().await;
    commit_quoting_rows(&catalog).await.expect("commit logs");
    let reply = post_ppl_query(
        &server,
        "/_plugins/_ppl?format=csv",
        "source=logs | fields severity_text, body | sort severity_text",
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(
        reply.text,
        "severity_text,body\nA,\"a,b\"\nB,\"say \"\"hi\"\"\"\nC,\"line1\nline2\"\nD,\nE,plain\nF,x|y"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn raw_separates_cells_with_pipes_unquoted() {
    let (server, catalog) = TestServer::start().await;
    commit_quoting_rows(&catalog).await.expect("commit logs");
    let reply = post_ppl_query(
        &server,
        "/_plugins/_ppl?format=raw",
        "source=logs | fields severity_text, body | sort severity_text",
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(
        reply.text,
        "severity_text|body\nA|a,b\nB|say \"hi\"\nC|line1\nline2\nD|\nE|plain\nF|x|y"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn csv_and_raw_render_maps_as_braced_key_value_lists_with_flat_keys() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[
            LogRow {
                tenant_id: TENANT_A,
                timestamp_micros: NOON_MICROS,
                service_name: None,
                severity_text: Some("m1"),
                body: None,
                resource_attributes: &[],
                log_attributes: &[("http.method", "GET"), ("Zone", "eu")],
            },
            build_log_row(TENANT_A, NOON_MICROS, None, Some("m2"), None),
            LogRow {
                tenant_id: TENANT_B,
                timestamp_micros: NOON_MICROS,
                service_name: None,
                severity_text: Some("m1"),
                body: None,
                resource_attributes: &[],
                log_attributes: &[("other", "tenant")],
            },
        ],
    )
    .await
    .expect("commit logs");
    let query = "source=logs | fields severity_text, log_attributes | sort severity_text";

    let csv_reply = post_ppl_query(&server, "/_plugins/_ppl?format=csv", query).await;
    assert_eq!(csv_reply.status, 200, "{}", csv_reply.text);
    assert_eq!(
        csv_reply.text,
        "severity_text,log_attributes\nm1,\"{Zone=eu, http.method=GET}\"\nm2,{}"
    );
    let raw_reply = post_ppl_query(&server, "/_plugins/_ppl?format=raw", query).await;
    assert_eq!(raw_reply.status, 200, "{}", raw_reply.text);
    assert_eq!(
        raw_reply.text,
        "severity_text|log_attributes\nm1|{Zone=eu, http.method=GET}\nm2|{}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn csv_and_raw_render_lists_and_large_numbers_in_plain_form() {
    let (server, catalog) = TestServer::start().await;
    // `finish_reasons`: a two-element list, an empty list, a NULL list, and a tenant-B list.
    let mut finish_reasons = ListBuilder::new(StringBuilder::new());
    finish_reasons.values().append_value("stop");
    finish_reasons.values().append_value("length");
    finish_reasons.append(true);
    finish_reasons.append(true);
    finish_reasons.append(false);
    finish_reasons.values().append_value("other tenant");
    finish_reasons.append(true);
    let micros: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![NOON_MICROS; 4]));
    let ids = |width: usize| -> ArrayRef {
        Arc::new(FixedSizeBinaryArray::try_from_iter((1_u8..=4).map(|byte| vec![byte; width])).expect("fixed-size ids"))
    };
    commit_table_columns(
        &catalog,
        OPERATIONS_TABLE,
        4,
        &[
            (
                COL_TENANT_ID,
                Arc::new(StringArray::from(vec![TENANT_A, TENANT_A, TENANT_A, TENANT_B])) as ArrayRef,
            ),
            (COL_TRACE_ID, ids(16)),
            (COL_SPAN_ID, ids(8)),
            (COL_TIMESTAMP, Arc::clone(&micros)),
            (COL_END_TIMESTAMP, Arc::clone(&micros)),
            (COL_INGESTED_TIMESTAMP, micros),
            (
                COL_DURATION_MICROS,
                Arc::new(Int64Array::from(vec![0_i64; 4])) as ArrayRef,
            ),
            (
                OPERATION_NAME,
                Arc::new(StringArray::from(vec!["op1", "op2", "op3", "op-b"])) as ArrayRef,
            ),
            (FINISH_REASONS, Arc::new(finish_reasons.finish()) as ArrayRef),
        ],
    )
    .await
    .expect("commit operations");
    // A large double: the flat formats print it in plain decimal form, never with an exponent.
    let query = format!(
        "source=operations | eval big = cast('1e21' as double) | fields {OPERATION_NAME}, {FINISH_REASONS}, big | sort {OPERATION_NAME}"
    );

    let csv_reply = post_ppl_query(&server, "/_plugins/_ppl?format=csv", &query).await;
    assert_eq!(csv_reply.status, 200, "{}", csv_reply.text);
    assert_eq!(
        csv_reply.text,
        "operation_name,finish_reasons,big\n\
         op1,\"[stop, length]\",1000000000000000000000.0\n\
         op2,[],1000000000000000000000.0\n\
         op3,,1000000000000000000000.0"
    );
    let raw_reply = post_ppl_query(&server, "/_plugins/_ppl?format=raw", &query).await;
    assert_eq!(raw_reply.status, 200, "{}", raw_reply.text);
    assert_eq!(
        raw_reply.text,
        "operation_name|finish_reasons|big\n\
         op1|[stop, length]|1000000000000000000000.0\n\
         op2|[]|1000000000000000000000.0\n\
         op3||1000000000000000000000.0"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn csv_and_raw_answer_with_the_json_content_type() {
    let (server, _catalog) = TestServer::start().await;
    for path in [
        "/_plugins/_ppl?format=csv",
        "/_plugins/_ppl?format=raw",
        "/_plugins/_ppl?format=jdbc",
    ] {
        let reply = post_ppl_query(&server, path, "source=logs | fields body").await;
        assert_eq!(reply.status, 200, "{path}: {}", reply.text);
        assert_eq!(reply.content_type, PPL_JSON_CONTENT_TYPE, "{path}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn explain_answers_calcite_logical_and_physical_text() {
    let (server, _catalog) = TestServer::start().await;
    let reply = post_ppl_query(
        &server,
        "/_plugins/_ppl/_explain",
        "source=logs | where severity_text = 'ERROR' | fields body",
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(reply.content_type.split(';').next(), Some("application/json"));
    let body = reply.parse_json_body();
    let Value::Object(top) = &body else {
        panic!("explain body is not an object: {body}")
    };
    assert_eq!(top.keys().collect::<Vec<_>>(), ["calcite"]);
    let Value::Object(calcite) = &body["calcite"] else {
        panic!("calcite is not an object: {body}")
    };
    let mut keys: Vec<&String> = calcite.keys().collect();
    keys.sort();
    assert_eq!(keys, ["logical", "physical"]);
    let (logical_plan, physical_plan) = read_explained_plans(&reply);
    assert!(
        find_plan_line(&logical_plan, "TableScan").contains("logs"),
        "{logical_plan}"
    );
    assert!(!physical_plan.trim().is_empty());
    server.shutdown().await;
}

#[tokio::test]
async fn explain_of_an_unknown_source_is_index_not_found_404() {
    let (server, _catalog) = TestServer::start().await;
    let reply = post_ppl_query(&server, "/_plugins/_ppl/_explain", "source=nope | fields body").await;
    assert_eq!(reply.status, 404, "{}", reply.text);
    assert_eq!(reply.content_type, PPL_ERROR_CONTENT_TYPE);
    let body = reply.parse_json_body();
    assert_eq!(body["status"], 404);
    assert_eq!(body["error"]["type"], "IndexNotFoundException");
    assert_eq!(body["error"]["code"], "INDEX_NOT_FOUND");
    assert!(
        body["error"]["reason"].as_str().is_some_and(|reason| reason.contains("nope")),
        "{body}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn an_unsupported_format_is_a_400() {
    let (server, _catalog) = TestServer::start().await;
    for (path, format_name) in [
        ("/_plugins/_ppl?format=json", "json"),
        ("/_plugins/_ppl?format=yaml", "yaml"),
        ("/_plugins/_ppl/_explain?format=yaml", "yaml"),
    ] {
        let error = read_illegal_argument_error(&post_ppl_query(&server, path, "source=logs").await);
        assert_eq!(error["format"], format_name, "{path}: {error}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn filter_path_on_csv_or_raw_is_a_400_and_jdbc_keeps_it() {
    let (server, _catalog) = TestServer::start().await;
    // The text formats are not JSON, so `filter_path` cannot apply to them.
    for (path, format_name) in [
        ("/_plugins/_ppl?format=csv&filter_path=x", "csv"),
        ("/_plugins/_ppl?format=raw&filter_path=x", "raw"),
    ] {
        let error = read_illegal_argument_error(&post_ppl_query(&server, path, "source=logs").await);
        assert_eq!(error["parameter"], "filter_path", "{path}: {error}");
        assert_eq!(error["format"], format_name, "{path}: {error}");
    }
    let reply = post_ppl_query(
        &server,
        "/_plugins/_ppl?format=jdbc&filter_path=schema",
        "source=logs | fields body",
    )
    .await;
    assert_eq!(reply.status, 200, "{}", reply.text);
    assert_eq!(
        reply.parse_json_body(),
        json!({"schema": [{"name": "body", "type": "string"}]})
    );
    server.shutdown().await;
}

#[tokio::test]
async fn an_unknown_explain_parameter_is_a_400_naming_it() {
    let (server, _catalog) = TestServer::start().await;
    let reply = post_ppl_query(&server, "/_plugins/_ppl/_explain?verbose=true", "source=logs").await;
    // The serde refusal of an unknown parameter carries no structured key, so its reason is the
    // only place that names it.
    let error = read_illegal_argument_error(&reply);
    assert!(
        error["reason"].as_str().is_some_and(|reason| reason.contains("verbose")),
        "the reason must name verbose: {error}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_timestamp_literal_reaches_the_wal_and_iceberg_scans() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[
            build_log_row(TENANT_A, NOON_MICROS, None, Some("INFO"), Some("committed")),
            build_log_row(TENANT_B, NOON_MICROS, None, Some("INFO"), Some("b-committed")),
        ],
    )
    .await
    .expect("commit logs");
    write_log_wal_segment(
        &server.wal_store,
        &catalog,
        COMMITTED_WAL_OFFSET + 1,
        &[
            build_log_row(TENANT_A, NOON_MICROS + 1, None, Some("INFO"), Some("hot")),
            build_log_row(TENANT_B, NOON_MICROS + 1, None, Some("INFO"), Some("b-hot")),
        ],
    )
    .await
    .expect("write WAL segment");
    let query = "source=logs | where timestamp >= '2026-10-08 12:00:00' | fields body | sort body";

    let rows_reply = post_ppl_query(&server, "/_plugins/_ppl", query).await;
    assert_eq!(
        rows_reply.parse_json_body()["datarows"],
        json!([["committed"], ["hot"]]),
        "{}",
        rows_reply.text
    );

    let (logical_plan, physical_plan) =
        read_explained_plans(&post_ppl_query(&server, "/_plugins/_ppl/_explain", query).await);
    assert!(
        find_plan_line(&logical_plan, "TableScan").contains("TimestampMicrosecond(1791460800000000,"),
        "the scan must receive a microsecond literal:\n{logical_plan}"
    );
    let iceberg_line = find_plan_line(&physical_plan, "IcegateIcebergScan");
    let iceberg_predicate = iceberg_line.split_once("predicate:[").map_or("", |(_, rest)| rest);
    assert!(
        iceberg_predicate.contains("timestamp"),
        "the Iceberg scan must prune on timestamp:\n{physical_plan}"
    );
    let wal_line = find_plan_line(&physical_plan, "DataSourceExec");
    let wal_predicate = wal_line.split_once("predicate=").map_or("", |(_, rest)| rest);
    assert!(
        contains_whole_number(wal_predicate, "1791460800000000"),
        "the WAL scan must prune on the microsecond literal:\n{physical_plan}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_leading_where_pushes_through_eval() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[
            build_log_row(TENANT_A, NOON_MICROS, Some("checkout"), Some("ERROR"), Some("x")),
            build_log_row(TENANT_A, NOON_MICROS, Some("cart"), Some("INFO"), Some("y")),
            build_log_row(TENANT_B, NOON_MICROS, Some("checkout"), Some("ERROR"), Some("b-x")),
        ],
    )
    .await
    .expect("commit logs");
    write_log_wal_segment(
        &server.wal_store,
        &catalog,
        COMMITTED_WAL_OFFSET + 1,
        &[
            build_log_row(TENANT_A, NOON_MICROS, Some("checkout"), Some("WARN"), Some("z")),
            build_log_row(TENANT_B, NOON_MICROS, Some("checkout"), Some("WARN"), Some("b-z")),
        ],
    )
    .await
    .expect("write WAL segment");
    let query = "source=logs | eval level = lower(severity_text) | where service_name = 'checkout' | fields level, body | sort body";

    let rows_reply = post_ppl_query(&server, "/_plugins/_ppl", query).await;
    assert_eq!(
        rows_reply.parse_json_body()["datarows"],
        json!([["error", "x"], ["warn", "z"]]),
        "{}",
        rows_reply.text
    );

    let (logical_plan, physical_plan) =
        read_explained_plans(&post_ppl_query(&server, "/_plugins/_ppl/_explain", query).await);
    assert!(
        find_plan_line(&logical_plan, "TableScan").contains("checkout"),
        "{logical_plan}"
    );
    let iceberg_line = find_plan_line(&physical_plan, "IcegateIcebergScan");
    assert!(
        iceberg_line
            .split_once("predicate:[")
            .is_some_and(|(_, rest)| rest.contains("service_name")),
        "{physical_plan}"
    );
    assert!(
        find_plan_line(&physical_plan, "DataSourceExec")
            .split_once("predicate=")
            .is_some_and(|(_, rest)| rest.contains("checkout")),
        "{physical_plan}"
    );
    server.shutdown().await;
}
