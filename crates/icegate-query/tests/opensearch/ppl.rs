//! `/_plugins/_ppl` over real HTTP. Every request is a literal, every row is written by the test
//! with explicit values, and every expected result is stated here.
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use iceberg::Catalog;
use iceberg::spec::{NestedField, PrimitiveType, Type};
use icegate_common::schema::{
    COL_BODY, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES, COL_OBSERVED_TIMESTAMP, COL_RESOURCE_ATTRIBUTES,
    COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID,
    logs_schema,
};
use icegate_common::{MemoryPressureConfig, MemoryPressureSampler, SPANS_TABLE};
use icegate_query::opensearch::{OpenSearchConfig, PplLimitsConfig};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{
    Cell, LogRow, OperationRow, PriceRow, Row, RowWriteError, commit_log_rows, commit_operation_rows,
    commit_price_rows, commit_rows, create_exact_name_table, required_string,
};
use super::harness::{PplReply, ServerOptions, TestServer};
use super::search::NearlyFullMemory;
use super::test_rows::TEMPERATURE;

const TENANT: &str = "acme";

const OTHER_TENANT: &str = "globex";

const PPL_PATH: &str = "/_plugins/_ppl";

const PPL_ERROR_CONTENT_TYPE: &str = "application/json; charset=UTF-8";

/// 2026-10-08T12:00:00Z in microseconds since the epoch.
const NOON_MICROS: i64 = 1_791_460_800_000_000;

/// The W3C trace-context example ids, `0af7651916cd43dd8448eb211c80319c` / `b7ad6b7169203331`.
const TRACE_ID: [u8; 16] = [
    0x0a, 0xf7, 0x65, 0x19, 0x16, 0xcd, 0x43, 0xdd, 0x84, 0x48, 0xeb, 0x21, 0x1c, 0x80, 0x31, 0x9c,
];

const SPAN_ID: [u8; 8] = [0xb7, 0xad, 0x6b, 0x71, 0x69, 0x20, 0x33, 0x31];

/// Three `acme` rows and one `globex` row, written in one batch:
///
/// | tenant | timestamp | service_name | severity_text | body | resource / log attributes |
/// |---|---|---|---|---|---|
/// | acme | 12:00:00 | api | ERROR | disk full | `host.name=node-a` / `http=x`, `http.method=GET` |
/// | acme | 12:00:01.25 | NULL | INFO | ok | — |
/// | acme | 12:00:02.000001 | web | WARN | NULL | `host.name=node-b` / — |
/// | globex | 12:00:00.5 | api | ERROR | globex secret | — |
async fn seed_logs(catalog: &Arc<dyn Catalog>) {
    let rows = [
        LogRow {
            tenant_id: TENANT,
            timestamp_micros: NOON_MICROS,
            service_name: Some("api"),
            severity_text: Some("ERROR"),
            body: Some("disk full"),
            resource_attributes: &[("host.name", "node-a")],
            log_attributes: &[("http", "x"), ("http.method", "GET")],
        },
        LogRow {
            tenant_id: TENANT,
            timestamp_micros: NOON_MICROS + 1_250_000,
            service_name: None,
            severity_text: Some("INFO"),
            body: Some("ok"),
            resource_attributes: &[],
            log_attributes: &[],
        },
        LogRow {
            tenant_id: TENANT,
            timestamp_micros: NOON_MICROS + 2_000_001,
            service_name: Some("web"),
            severity_text: Some("WARN"),
            body: None,
            resource_attributes: &[("host.name", "node-b")],
            log_attributes: &[],
        },
        LogRow {
            tenant_id: OTHER_TENANT,
            timestamp_micros: NOON_MICROS + 500_000,
            service_name: Some("api"),
            severity_text: Some("ERROR"),
            body: Some("globex secret"),
            resource_attributes: &[],
            log_attributes: &[],
        },
    ];
    commit_log_rows(catalog, &rows).await.expect("commit logs");
}

/// One operation of `tenant` at `timestamp_micros`, 10 ms long, with `temperature`.
const fn build_test_operation_row(tenant: &str, timestamp_micros: i64, temperature: Option<f64>) -> OperationRow<'_> {
    OperationRow {
        tenant_id: tenant,
        trace_id: TRACE_ID,
        span_id: SPAN_ID,
        timestamp_micros,
        end_timestamp_micros: timestamp_micros + 10_000,
        operation_name: "chat",
        request_model: None,
        input_tokens: None,
        temperature,
        finish_reasons: None,
    }
}

/// A PPL answer: status, content type and body. Every PPL body is JSON, whatever its content
/// type says.
struct PplAnswer {
    status: StatusCode,
    content_type: String,
    body: Value,
}

/// `reply` with its body parsed.
fn read_answer(reply: PplReply) -> PplAnswer {
    PplAnswer {
        status: StatusCode::from_u16(reply.status).expect("HTTP status"),
        body: reply.parse_json_body(),
        content_type: reply.content_type,
    }
}

async fn post_ppl(server: &TestServer, tenant: &str, path_and_query: &str, body: &Value) -> PplAnswer {
    read_answer(server.post_ppl(tenant, path_and_query, body).await)
}

async fn run_query(server: &TestServer, query: &str) -> PplAnswer {
    post_ppl(server, TENANT, PPL_PATH, &json!({"query": query})).await
}

/// The datarows of an answer that must be a 200.
fn read_rows(answer: &PplAnswer) -> &Value {
    assert_eq!(answer.status, StatusCode::OK, "answer {}", answer.body);
    &answer.body["datarows"]
}

/// The column names of an answer's `schema`, in order.
fn read_column_names(answer: &PplAnswer) -> Vec<String> {
    answer.body["schema"]
        .as_array()
        .expect("schema")
        .iter()
        .map(|column| column["name"].as_str().expect("column name").to_string())
        .collect()
}

/// A PPL error envelope with `status`, `type` and `code`, labelled as JSON, with a non-empty
/// `reason` and `details`.
fn assert_ppl_error(answer: &PplAnswer, status: StatusCode, error_type: &str, code: &str) {
    assert_eq!(answer.status, status, "answer {}", answer.body);
    assert_eq!(answer.content_type, PPL_ERROR_CONTENT_TYPE);
    assert_eq!(answer.body["status"], json!(status.as_u16()));
    assert_eq!(answer.body["error"]["type"], json!(error_type));
    assert_eq!(answer.body["error"]["code"], json!(code));
    let reason = answer.body["error"]["reason"].as_str().expect("reason");
    let details = answer.body["error"]["details"].as_str().expect("details");
    assert!(!reason.is_empty() && !details.is_empty(), "answer {}", answer.body);
}

/// The `error` object of an answer in the `OpenSearch` envelope `illegal_argument_exception`,
/// 400. Where the PPL handler's refusal carries a key, that key tells it apart from the listener's
/// no-handler refusal, which has the same status and type and carries none; a keyless refusal
/// (invalid JSON, a body that is not an object) relies on the keyed cases sent to the same route
/// to show the handler answered.
fn read_illegal_argument_error(answer: &PplAnswer) -> &Value {
    assert_eq!(answer.status, StatusCode::BAD_REQUEST, "answer {}", answer.body);
    assert_eq!(answer.body["status"], json!(400));
    assert_eq!(answer.body["error"]["type"], json!("illegal_argument_exception"));
    &answer.body["error"]
}

#[tokio::test]
async fn a_source_query_answers_jdbc_with_the_schema_in_table_column_order() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(&server, "source=logs").await;
    assert_eq!(answer.status, StatusCode::OK, "answer {}", answer.body);
    let expected_names: Vec<String> = logs_schema()
        .expect("logs schema")
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .filter(|name| name != COL_TENANT_ID)
        .collect();
    assert_eq!(read_column_names(&answer), expected_names);
    let expected_types = [
        (COL_SERVICE_NAME, "string"),
        (COL_TIMESTAMP, "timestamp"),
        (COL_OBSERVED_TIMESTAMP, "timestamp"),
        (COL_INGESTED_TIMESTAMP, "timestamp"),
        (COL_TRACE_ID, "string"),
        (COL_SPAN_ID, "string"),
        (COL_SEVERITY_TEXT, "string"),
        (COL_BODY, "string"),
        (COL_RESOURCE_ATTRIBUTES, "struct"),
        (COL_SCOPE_ATTRIBUTES, "struct"),
        (COL_LOG_ATTRIBUTES, "struct"),
    ];
    let schema = answer.body["schema"].as_array().expect("schema");
    for (name, expected) in expected_types {
        let column = schema
            .iter()
            .find(|column| column["name"] == json!(name))
            .expect("column listed");
        assert_eq!(column["type"], json!(expected), "column {name}");
    }
    assert_eq!(answer.body["total"], json!(3));
    assert_eq!(answer.body["size"], json!(3));
    assert!(answer.body.get("status").is_none());
    assert!(answer.body.get("cursor").is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn where_fields_sort_and_head_compose_as_stated() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(
        &server,
        "source=logs | where severity_text != 'INFO' | fields severity_text, service_name | sort - severity_text | head 1",
    )
    .await;
    assert_eq!(read_column_names(&answer), vec![COL_SEVERITY_TEXT, COL_SERVICE_NAME]);
    assert_eq!(read_rows(&answer), &json!([["WARN", "web"]]));
    server.shutdown().await;
}

#[tokio::test]
async fn head_with_from_skips_rows() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(&server, "source=logs | sort timestamp | fields body | head 1 from 1").await;
    assert_eq!(read_rows(&answer), &json!([["ok"]]));
    let answer = run_query(&server, "source=logs | sort timestamp | fields body | head 5 from 3").await;
    assert_eq!(read_rows(&answer), &json!([]));
    server.shutdown().await;
}

#[tokio::test]
async fn the_search_command_form_answers_like_source() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    for query in [
        "source=logs | sort timestamp | fields body",
        "search source=logs | sort timestamp | fields body",
        "SEARCH SOURCE = logs | SORT timestamp | FIELDS body",
    ] {
        let answer = run_query(&server, query).await;
        assert_eq!(
            read_rows(&answer),
            &json!([["disk full"], ["ok"], [null]]),
            "query {query}"
        );
    }
    server.shutdown().await;
}

#[tokio::test]
async fn fields_minus_drops_the_listed_columns() {
    // The scan-level projection itself is pinned by the plan-shape test
    // `fields_minus_projects_only_the_remaining_columns_at_the_scan` in `ppl/plan.rs`.
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(
        &server,
        "source=logs | fields - resource_attributes, scope_attributes, log_attributes, observed_timestamp, \
         ingested_timestamp, trace_id, span_id | sort timestamp",
    )
    .await;
    assert_eq!(
        read_column_names(&answer),
        vec![COL_SERVICE_NAME, COL_TIMESTAMP, COL_SEVERITY_TEXT, COL_BODY]
    );
    assert_eq!(
        read_rows(&answer),
        &json!([
            ["api", "2026-10-08 12:00:00", "ERROR", "disk full"],
            [null, "2026-10-08 12:00:01.25", "INFO", "ok"],
            ["web", "2026-10-08 12:00:02.000001", "WARN", null]
        ])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn three_valued_logic_in_where_drops_null_comparisons() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    // The row without service_name compares NULL, which drops it under `!=` and under `not`.
    let cases = [
        (
            "source=logs | where service_name != 'api' | fields severity_text",
            json!([["WARN"]]),
        ),
        (
            "source=logs | where not (service_name = 'api') | fields severity_text",
            json!([["WARN"]]),
        ),
        (
            "source=logs | where service_name = 'api' or severity_text = 'INFO' | sort timestamp | fields severity_text",
            json!([["ERROR"], ["INFO"]]),
        ),
        (
            "source=logs | where service_name = 'api' xor severity_text = 'WARN' | sort timestamp | fields severity_text",
            json!([["ERROR"], ["WARN"]]),
        ),
    ];
    for (query, expected) in cases {
        let answer = run_query(&server, query).await;
        assert_eq!(read_rows(&answer), &expected, "query {query}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn an_unknown_source_is_a_404_index_not_found_with_its_code() {
    let (server, _catalog) = TestServer::start().await;
    let answer = run_query(&server, "source=nope | head 1").await;
    assert_ppl_error(
        &answer,
        StatusCode::NOT_FOUND,
        "IndexNotFoundException",
        "INDEX_NOT_FOUND",
    );
    assert!(answer.body["error"]["reason"].as_str().expect("reason").contains("nope"));
    assert_eq!(answer.body["error"]["context"]["index"], json!("nope"));
    server.shutdown().await;
}

#[tokio::test]
async fn a_dotted_three_part_source_is_index_not_found() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(&server, "source=iceberg.icegate.logs").await;
    assert_ppl_error(
        &answer,
        StatusCode::NOT_FOUND,
        "IndexNotFoundException",
        "INDEX_NOT_FOUND",
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_source_in_another_case_names_no_index() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(&server, "source=Logs").await;
    assert_ppl_error(
        &answer,
        StatusCode::NOT_FOUND,
        "IndexNotFoundException",
        "INDEX_NOT_FOUND",
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_field_reference_in_another_case_is_field_not_found() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(&server, "source=logs | where Severity_Text = 'ERROR'").await;
    assert_ppl_error(
        &answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "FIELD_NOT_FOUND",
    );
    assert_eq!(answer.body["error"]["context"]["field"], json!("Severity_Text"));
    server.shutdown().await;
}

#[tokio::test]
async fn a_syntax_error_answers_invalid_query_with_details_naming_the_token_as_json() {
    let (server, _catalog) = TestServer::start().await;
    let answer = run_query(&server, "source=logs | where = 1").await;
    assert_ppl_error(&answer, StatusCode::BAD_REQUEST, "SyntaxCheckException", "SYNTAX_ERROR");
    assert_eq!(answer.body["error"]["reason"], json!("Invalid Query"));
    let details = answer.body["error"]["details"].as_str().expect("details");
    assert!(
        details.contains("'='") && details.contains("column 20"),
        "details {details}"
    );
    // A query with no `source` is a syntax error too.
    let answer = run_query(&server, "where severity_text = 'ERROR'").await;
    assert_ppl_error(&answer, StatusCode::BAD_REQUEST, "SyntaxCheckException", "SYNTAX_ERROR");
    server.shutdown().await;
}

#[tokio::test]
async fn a_request_without_query_or_with_an_unknown_format_is_an_illegal_argument_envelope() {
    let (server, _catalog) = TestServer::start().await;
    // A missing or non-string `query` names the key, and the value when one was given; a body
    // that is not an object names no key.
    let cases = [
        (json!({}), Some("query"), None),
        (json!({"query": 5}), Some("query"), Some(json!(5))),
        (json!(["source=logs"]), None, None),
    ];
    for (body, expected_key, expected_value) in cases {
        let answer = post_ppl(&server, TENANT, PPL_PATH, &body).await;
        let error = read_illegal_argument_error(&answer);
        assert_eq!(
            error.get("key").cloned(),
            expected_key.map(|key| json!(key)),
            "body {body}"
        );
        assert_eq!(error.get("value").cloned(), expected_value, "body {body}");
    }
    let answer = post_ppl(
        &server,
        TENANT,
        "/_plugins/_ppl?format=yaml",
        &json!({"query": "source=logs"}),
    )
    .await;
    assert_eq!(read_illegal_argument_error(&answer)["format"], json!("yaml"));
    server.shutdown().await;
}

#[tokio::test]
async fn an_unknown_ppl_query_parameter_is_an_illegal_argument_envelope() {
    let (server, _catalog) = TestServer::start().await;
    let answer = post_ppl(
        &server,
        TENANT,
        "/_plugins/_ppl?unknown_knob=1",
        &json!({"query": "source=logs"}),
    )
    .await;
    read_illegal_argument_error(&answer);
    let ignored_parameter_answer = post_ppl(
        &server,
        TENANT,
        "/_plugins/_ppl?sanitize=false",
        &json!({"query": "source=logs"}),
    )
    .await;
    assert_eq!(
        ignored_parameter_answer.status,
        StatusCode::OK,
        "an ignored parameter is accepted: {}",
        ignored_parameter_answer.body
    );
    server.shutdown().await;
}

#[tokio::test]
async fn invalid_json_is_an_illegal_argument_envelope() {
    let (server, _catalog) = TestServer::start().await;
    let response = server
        .request(Method::POST, Some(TENANT), PPL_PATH)
        .header(CONTENT_TYPE, "application/json")
        .body("{\"query\": \"source=logs\"")
        .send()
        .await
        .expect("PPL request");
    let answer = read_answer(PplReply::from_response(response).await);
    let error = read_illegal_argument_error(&answer);
    assert!(error.get("key").is_none(), "error {error}");
    server.shutdown().await;
}

#[tokio::test]
async fn fetch_size_bounds_the_returned_rows_like_a_trailing_head() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let body = json!({"query": "source=logs | sort timestamp | fields body", "fetch_size": 2});
    let answer = post_ppl(&server, TENANT, PPL_PATH, &body).await;
    assert_eq!(read_rows(&answer), &json!([["disk full"], ["ok"]]));
    assert_eq!(answer.body["total"], json!(2));
    assert_eq!(answer.body["size"], json!(2));
    let trailing_head = run_query(&server, "source=logs | sort timestamp | fields body | head 2").await;
    assert_eq!(read_rows(&trailing_head), read_rows(&answer));
    server.shutdown().await;
}

#[tokio::test]
async fn a_zero_fetch_size_bounds_nothing() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let body = json!({"query": "source=logs | sort timestamp | fields body", "fetch_size": 0});
    let answer = post_ppl(&server, TENANT, PPL_PATH, &body).await;
    assert_eq!(read_rows(&answer), &json!([["disk full"], ["ok"], [null]]));
    server.shutdown().await;
}

#[tokio::test]
async fn an_invalid_fetch_size_is_an_illegal_argument_envelope() {
    let (server, _catalog) = TestServer::start().await;
    for fetch_size in [json!(-1), json!(1.5), json!("10")] {
        let body = json!({"query": "source=logs", "fetch_size": fetch_size});
        let answer = post_ppl(&server, TENANT, PPL_PATH, &body).await;
        let error = read_illegal_argument_error(&answer);
        assert_eq!(error["key"], json!("fetch_size"), "fetch_size {fetch_size}");
        assert_eq!(error["value"], fetch_size, "fetch_size {fetch_size}");
    }
    server.shutdown().await;
}

/// A listener whose PPL output is limited to two rows.
async fn start_with_two_row_limit() -> (TestServer, Arc<dyn Catalog>) {
    TestServer::start_with_config(OpenSearchConfig {
        ppl: PplLimitsConfig {
            max_result_rows: Some(2),
        },
        ..OpenSearchConfig::default()
    })
    .await
}

#[tokio::test]
async fn output_at_the_configured_row_limit_is_answered_and_one_row_more_is_a_400() {
    let (server, catalog) = start_with_two_row_limit().await;
    seed_logs(&catalog).await;
    let below_limit_answer = run_query(&server, "source=logs | where severity_text = 'ERROR' | fields body").await;
    assert_eq!(read_rows(&below_limit_answer), &json!([["disk full"]]));
    let at_limit_answer = run_query(
        &server,
        "source=logs | where severity_text != 'INFO' | sort timestamp | fields body",
    )
    .await;
    assert_eq!(read_rows(&at_limit_answer), &json!([["disk full"], [null]]));
    let above_limit_answer = run_query(&server, "source=logs | fields body").await;
    assert_ppl_error(
        &above_limit_answer,
        StatusCode::BAD_REQUEST,
        "IllegalArgumentException",
        "RESOURCE_LIMIT_EXCEEDED",
    );
    assert_eq!(
        above_limit_answer.body["error"]["reason"],
        json!(
            "The query returns more than 2 rows, the configured opensearch.ppl.max_result_rows; add | head 2 to read \
             the first rows."
        )
    );
    assert_eq!(above_limit_answer.body["error"]["context"]["limit"], json!(2));
    server.shutdown().await;
}

#[tokio::test]
async fn head_and_fetch_size_at_or_below_the_row_limit_are_never_an_error() {
    let (server, catalog) = start_with_two_row_limit().await;
    seed_logs(&catalog).await;
    let head_cases = [
        (
            "source=logs | sort timestamp | head 2 | fields body",
            json!([["disk full"], ["ok"]]),
        ),
        (
            "source=logs | sort timestamp | head 1 | fields body",
            json!([["disk full"]]),
        ),
    ];
    for (query, expected_rows) in head_cases {
        let answer = run_query(&server, query).await;
        assert_eq!(answer.status, StatusCode::OK, "query {query}: {}", answer.body);
        assert_eq!(read_rows(&answer), &expected_rows, "query {query}");
    }
    for fetch_size in [2_usize, 1] {
        let body = json!({"query": "source=logs", "fetch_size": fetch_size});
        let answer = post_ppl(&server, TENANT, PPL_PATH, &body).await;
        assert_eq!(read_rows(&answer).as_array().map(Vec::len), Some(fetch_size));
    }
    server.shutdown().await;
}

#[tokio::test]
async fn ids_render_as_lowercase_hex_and_maps_as_one_object_with_dotted_keys_unsplit() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    commit_operation_rows(&catalog, &[build_test_operation_row(TENANT, NOON_MICROS, None)])
        .await
        .expect("commit operations");
    let id_answer = run_query(&server, "source=operations | fields trace_id, span_id").await;
    assert_eq!(
        read_rows(&id_answer),
        &json!([["0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"]])
    );
    let map_answer = run_query(
        &server,
        "source=logs | where severity_text = 'ERROR' | fields resource_attributes, log_attributes",
    )
    .await;
    assert_eq!(
        read_rows(&map_answer),
        &json!([[{"host.name": "node-a"}, {"http": "x", "http.method": "GET"}]])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn timestamps_render_without_t_or_zone_and_trailing_zeros_dropped() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(&server, "source=logs | sort timestamp | fields timestamp").await;
    assert_eq!(
        read_rows(&answer),
        &json!([
            ["2026-10-08 12:00:00"],
            ["2026-10-08 12:00:01.25"],
            ["2026-10-08 12:00:02.000001"]
        ])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn nan_and_infinities_render_as_strings() {
    let (server, catalog) = TestServer::start().await;
    let rows_written = [
        build_test_operation_row(TENANT, NOON_MICROS, Some(f64::NAN)),
        build_test_operation_row(TENANT, NOON_MICROS + 1, Some(f64::INFINITY)),
        build_test_operation_row(TENANT, NOON_MICROS + 2, Some(f64::NEG_INFINITY)),
        build_test_operation_row(TENANT, NOON_MICROS + 3, Some(0.5)),
    ];
    commit_operation_rows(&catalog, &rows_written).await.expect("commit operations");
    let answer = run_query(&server, "source=operations | sort timestamp | fields temperature").await;
    assert_eq!(
        read_rows(&answer),
        &json!([["NaN"], ["Infinity"], ["-Infinity"], [0.5]])
    );
    assert_eq!(answer.body["schema"], json!([{"name": TEMPERATURE, "type": "double"}]));
    server.shutdown().await;
}

#[tokio::test]
async fn a_system_index_source_is_a_400_semantic_error() {
    let (server, _catalog) = TestServer::start().await;
    let create_response = server
        .request(Method::PUT, Some(TENANT), "/.kibana_1")
        .json(&json!({"mappings": {"properties": {"type": {"type": "keyword"}}}}))
        .send()
        .await
        .expect("create .kibana_1");
    assert!(
        create_response.status().is_success(),
        "create .kibana_1: {}",
        create_response.status()
    );
    let answer = run_query(&server, "source=.kibana_1").await;
    assert_ppl_error(
        &answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "SEMANTIC_ERROR",
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_highlight_body_field_is_accepted_and_no_highlight_column_is_returned() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    // A body with time hints and a highlight request: the hints do not filter, the query text
    // alone decides.
    let body = json!({
        "query": "source=logs | sort timestamp | fields body",
        "highlight": {
            "pre_tags": ["@opensearch-dashboards-highlighted-field@"],
            "post_tags": ["@/opensearch-dashboards-highlighted-field@"],
            "fields": {"*": {}},
            "fragment_size": 2_147_483_647
        },
        "datasource": "default",
        "sessionId": "session-1",
        "lang": "ppl",
        "queryId": "8f2a6c1e-3b0d-4f6e-9a51-0c7d2e4b5a90",
        "time_field": "timestamp",
        "start_time": "2026-10-08 12:00:01.000",
        "end_time": "2026-10-08 12:00:01.500"
    });
    let answer = post_ppl(&server, TENANT, "/_plugins/_ppl?format=jdbc", &body).await;
    assert_eq!(read_column_names(&answer), vec![COL_BODY]);
    assert_eq!(read_rows(&answer), &json!([["disk full"], ["ok"], [null]]));
    server.shutdown().await;
}

#[tokio::test]
async fn ppl_never_returns_another_tenants_rows() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let query = json!({"query": "source=logs | sort timestamp | fields body"});
    let acme = post_ppl(&server, TENANT, PPL_PATH, &query).await;
    assert_eq!(read_rows(&acme), &json!([["disk full"], ["ok"], [null]]));
    let globex = post_ppl(&server, OTHER_TENANT, PPL_PATH, &query).await;
    assert_eq!(read_rows(&globex), &json!([["globex secret"]]));
    let filtered_query = json!({"query": "source=logs | where body = 'globex secret' | fields body"});
    let leak = post_ppl(&server, TENANT, PPL_PATH, &filtered_query).await;
    assert_eq!(read_rows(&leak), &json!([]));
    server.shutdown().await;
}

#[tokio::test]
async fn middleware_refusals_on_the_ppl_route_answer_in_the_envelope() {
    let body = json!({"query": "source=logs"});

    // Deadline: a catalog that stops answering after startup is cut off in the 503 envelope.
    let stall_switch = Arc::new(AtomicBool::new(false));
    let (stalled_server, _catalog) = TestServer::start_with_options(
        OpenSearchConfig::default(),
        ServerOptions {
            max_query_duration_secs: 1,
            catalog_stall_switch: Some(Arc::clone(&stall_switch)),
            ..ServerOptions::default()
        },
    )
    .await;
    stall_switch.store(true, Ordering::SeqCst);
    let deadline = post_ppl(&stalled_server, TENANT, PPL_PATH, &body).await;
    assert_eq!(
        deadline.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "answer {}",
        deadline.body
    );
    assert_eq!(deadline.body["status"], json!(503));
    assert_eq!(
        deadline.body["error"]["type"],
        json!("query_deadline_exceeded_exception")
    );
    stalled_server.shutdown().await;

    // Memory pressure: shed in the 503 envelope with Retry-After.
    let sampler = MemoryPressureSampler::with_reader(&MemoryPressureConfig::default(), Arc::new(NearlyFullMemory));
    sampler.sample_once().expect("sample");
    let (shedding_server, _catalog) = TestServer::start_with_options(
        OpenSearchConfig::default(),
        ServerOptions {
            pressure: sampler.handle(),
            ..ServerOptions::default()
        },
    )
    .await;
    let response = shedding_server
        .request(Method::POST, Some(TENANT), PPL_PATH)
        .json(&body)
        .send()
        .await
        .expect("PPL request under memory pressure");
    assert!(response.headers().contains_key("retry-after"));
    let shed_answer = read_answer(PplReply::from_response(response).await);
    assert_eq!(
        shed_answer.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "answer {}",
        shed_answer.body
    );
    assert_eq!(shed_answer.body["status"], json!(503));
    assert_eq!(shed_answer.body["error"]["type"], json!("memory_pressure_exception"));
    shedding_server.shutdown().await;

    // A body over max_request_body_bytes: 413 in the envelope.
    let (limited_server, _catalog) = TestServer::start_with_config(OpenSearchConfig {
        max_request_body_bytes: 256,
        ..OpenSearchConfig::default()
    })
    .await;
    let oversized_body = json!({"query": format!("source=logs | where body = '{}'", "x".repeat(300))});
    let oversized_answer = post_ppl(&limited_server, TENANT, PPL_PATH, &oversized_body).await;
    assert_eq!(
        oversized_answer.status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "answer {}",
        oversized_answer.body
    );
    assert_eq!(oversized_answer.body["status"], json!(413));
    assert_eq!(
        oversized_answer.body["error"]["type"],
        json!("illegal_argument_exception")
    );

    // No tenant under the multi-tenant policy: refused in the 400 envelope.
    let response = limited_server
        .request(Method::POST, None, PPL_PATH)
        .json(&body)
        .send()
        .await
        .expect("PPL request without a tenant");
    let tenant_refusal = read_answer(PplReply::from_response(response).await);
    assert_eq!(
        tenant_refusal.status,
        StatusCode::BAD_REQUEST,
        "answer {}",
        tenant_refusal.body
    );
    assert_eq!(tenant_refusal.body["status"], json!(400));
    assert_eq!(
        tenant_refusal.body["error"]["type"],
        json!("illegal_argument_exception")
    );
    limited_server.shutdown().await;
}

#[tokio::test]
async fn a_field_typed_differently_across_the_source_indices_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    // An exact-name table whose `service_name` is a long, where `logs` has a string.
    create_exact_name_table(
        &catalog,
        "audit",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::optional(2, COL_SERVICE_NAME, Type::Primitive(PrimitiveType::Long)),
        ],
    )
    .await
    .expect("create audit");
    let answer = run_query(&server, "source=logs,audit | fields service_name").await;
    assert_ppl_error(
        &answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "SEMANTIC_ERROR",
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_comma_listed_source_unions_the_rows_of_every_index() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    // Two `acme` spans and one `globex` span in one batch. `spans` has no `severity_text`, so
    // its rows read NULL there. Both tables have a `resource_attributes` map whose nested Iceberg
    // field ids differ; the union reads it from both.
    let span_rows = [
        Row {
            tenant: TENANT,
            cells: vec![(COL_TIMESTAMP, Cell::Micros(NOON_MICROS + 750_000))],
        },
        Row {
            tenant: TENANT,
            cells: vec![(COL_TIMESTAMP, Cell::Micros(NOON_MICROS + 3_000_000))],
        },
        Row {
            tenant: OTHER_TENANT,
            cells: vec![(COL_TIMESTAMP, Cell::Micros(NOON_MICROS + 1_000_000))],
        },
    ];
    commit_rows(&catalog, SPANS_TABLE, &span_rows).await.expect("commit spans");
    let answer = run_query(
        &server,
        "source=logs,spans | sort timestamp | fields timestamp, severity_text",
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "answer {}", answer.body);
    assert_eq!(
        read_rows(&answer),
        &json!([
            ["2026-10-08 12:00:00", "ERROR"],
            ["2026-10-08 12:00:00.75", null],
            ["2026-10-08 12:00:01.25", "INFO"],
            ["2026-10-08 12:00:02.000001", "WARN"],
            ["2026-10-08 12:00:03", null]
        ])
    );
    let map_answer = run_query(
        &server,
        "source=logs,spans | sort timestamp | fields timestamp, resource_attributes",
    )
    .await;
    assert_eq!(map_answer.status, StatusCode::OK, "answer {}", map_answer.body);
    // The spans rows and the second log row were written with an empty map.
    assert_eq!(
        read_rows(&map_answer),
        &json!([
            ["2026-10-08 12:00:00", {"host.name": "node-a"}],
            ["2026-10-08 12:00:00.75", {}],
            ["2026-10-08 12:00:01.25", {}],
            ["2026-10-08 12:00:02.000001", {"host.name": "node-b"}],
            ["2026-10-08 12:00:03", {}]
        ])
    );
    // Wildcards resolving to the same two indices union the same rows.
    let wildcard_answer = run_query(
        &server,
        "source=lo*,sp* | sort timestamp | fields timestamp, severity_text",
    )
    .await;
    assert_eq!(
        wildcard_answer.status,
        StatusCode::OK,
        "answer {}",
        wildcard_answer.body
    );
    assert_eq!(read_rows(&wildcard_answer), read_rows(&answer));
    server.shutdown().await;
}

#[tokio::test]
async fn sort_puts_null_first_ascending_and_last_descending() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let ascending_answer = run_query(&server, "source=logs | sort service_name | fields service_name").await;
    assert_eq!(read_rows(&ascending_answer), &json!([[null], ["api"], ["web"]]));
    let descending_answer = run_query(&server, "source=logs | sort - service_name | fields service_name").await;
    assert_eq!(read_rows(&descending_answer), &json!([["web"], ["api"], [null]]));
    server.shutdown().await;
}

/// The one `acme` row of the function table: `api`, `ERROR`, `disk full`, at 12:00:00.
const ERROR_ROW: &str = "source=logs | where severity_text = 'ERROR'";

#[tokio::test]
async fn each_supported_function_returns_the_stated_value() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    // Each expected value is derived by hand from the function's stated contract (README).
    let cases = [
        ("if(severity_text = 'ERROR', 1, 0)", json!(1)),
        (
            "case(severity_text = 'INFO', 'i', severity_text = 'ERROR', 'e' else 'x')",
            json!("e"),
        ),
        ("case(severity_text = 'INFO', 'i')", json!(null)),
        (
            "coalesce(nullif(body, 'disk full'), service_name, 'none')",
            json!("api"),
        ),
        ("ifnull(service_name, 'x')", json!("api")),
        ("nullif(severity_text, 'ERROR')", json!(null)),
        ("isnull(body)", json!(false)),
        ("isnotnull(body)", json!(true)),
        ("concat(severity_text, '-', service_name)", json!("ERROR-api")),
        ("concat(severity_text, nullif(body, 'disk full'))", json!(null)),
        ("lower(severity_text)", json!("error")),
        ("upper('disk')", json!("DISK")),
        ("trim('  x  ')", json!("x")),
        ("ltrim('  x  ')", json!("x  ")),
        ("rtrim('  x  ')", json!("  x")),
        ("substring('abcdef', 2, 3)", json!("bcd")),
        ("substring('abcdef', 3)", json!("cdef")),
        ("substr('abcdef', 3)", json!("cdef")),
        ("length('h\u{e9}llo')", json!(6)),
        ("locate('full', body)", json!(6)),
        ("like(body, 'DISK%')", json!(true)),
        ("like(body, 'DISK%', true)", json!(false)),
        ("like(body, 'disk_full', true)", json!(true)),
        ("body like 'DISK%'", json!(true)),
        ("md5('abc')", json!("900150983cd24fb0d6963f7d28e17f72")),
        ("abs(-3)", json!(3)),
        ("floor(2.7)", json!(2)),
        ("ceil(2.1)", json!(3)),
        ("ceiling(2.1)", json!(3)),
        ("round(2.567, 2)", json!(2.57)),
        ("round(2.5)", json!(3.0)),
        ("mod(7, 3)", json!(1)),
        ("7 % 3", json!(1)),
        ("-7 % 3", json!(-1)),
        // `*` binds tighter than `+`.
        ("1 + 2 * 3", json!(7)),
        ("severity_text in ('WARN', 'ERROR')", json!(true)),
        ("severity_text not in ('ERROR')", json!(false)),
        ("2 between 1 and 2", json!(true)),
        ("5 not between 1 and 2", json!(true)),
        ("service_name is null", json!(false)),
        ("service_name is not null", json!(true)),
        // `logs` stores the `service.name` resource attribute in its `service_name` column.
        ("resource_attributes.service.name", json!("api")),
        (
            "timestamp('2026-10-08 12:00:00.123456')",
            json!("2026-10-08 12:00:00.123456"),
        ),
        (
            "date_format(timestamp, '%Y/%m/%d %H:%i:%s')",
            json!("2026/10/08 12:00:00"),
        ),
        ("unix_timestamp(timestamp)", json!(1_791_460_800.0)),
        ("from_unixtime(1791460800.5)", json!("2026-10-08 12:00:00.5")),
        ("from_unixtime(1791460800, '%Y/%m/%d')", json!("2026/10/08")),
        (
            "from_unixtime(1791460800.5, '%Y-%m-%d %H:%i:%s.%f')",
            json!("2026-10-08 12:00:00.500000"),
        ),
        // A string that is not a literal converts through a cast, not the literal parser.
        (
            "timestamp(concat('2026-10-08 ', '12:00:00'))",
            json!("2026-10-08 12:00:00"),
        ),
        ("cast(timestamp as string)", json!("2026-10-08 12:00:00")),
    ];
    for (expression, expected) in cases {
        let query = format!("{ERROR_ROW} | eval v = {expression} | fields v");
        let answer = run_query(&server, &query).await;
        assert_eq!(read_rows(&answer), &json!([[expected]]), "expression {expression}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn now_returns_the_request_time() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let before_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs_f64();
    let answer = run_query(
        &server,
        &format!("{ERROR_ROW} | eval a = unix_timestamp(), b = unix_timestamp(now()) | fields a, b"),
    )
    .await;
    let after_seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_secs_f64();
    let row = &read_rows(&answer)[0];
    let (request_seconds, now_seconds) = (row[0].as_f64().expect("a"), row[1].as_f64().expect("b"));
    assert!(
        (before_seconds - 1.0..=after_seconds + 1.0).contains(&request_seconds),
        "now {request_seconds} outside [{before_seconds}, {after_seconds}]"
    );
    assert!(
        (request_seconds - now_seconds).abs() < 1e-6,
        "now() is taken once per request"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn an_unknown_function_is_a_400_naming_it() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(&server, "source=logs | eval x = reverse(body)").await;
    assert_ppl_error(
        &answer,
        StatusCode::BAD_REQUEST,
        "UnsupportedOperationException",
        "UNSUPPORTED_OPERATION",
    );
    assert_eq!(answer.body["error"]["context"]["function"], json!("reverse"));
    server.shutdown().await;
}

#[tokio::test]
async fn a_dotted_map_path_reads_one_key_and_is_null_when_absent() {
    let (server, catalog) = TestServer::start().await;
    let rows_written = [LogRow {
        tenant_id: TENANT,
        timestamp_micros: NOON_MICROS,
        service_name: Some("api"),
        severity_text: Some("INFO"),
        body: Some("map"),
        resource_attributes: &[("host.name", "node-a")],
        log_attributes: &[("http", "x"), ("http.method", "GET"), ("a`b", "tick")],
    }];
    commit_log_rows(&catalog, &rows_written).await.expect("commit logs");
    let answer = run_query(
        &server,
        "source=logs | fields log_attributes.http.method, log_attributes.`http.method`, log_attributes.http, \
         log_attributes.`a``b`, log_attributes.missing, resource_attributes.`host.name`",
    )
    .await;
    // The unquoted and the backtick-quoted spellings name the same field, which is listed once.
    assert_eq!(
        read_column_names(&answer),
        vec![
            "log_attributes.http.method",
            "log_attributes.http",
            "log_attributes.a`b",
            "log_attributes.missing",
            "resource_attributes.host.name"
        ]
    );
    assert_eq!(read_rows(&answer), &json!([["GET", "x", "tick", null, "node-a"]]));
    let filtered_answer = run_query(
        &server,
        "source=logs | where log_attributes.`http.method` = 'GET' | fields body",
    )
    .await;
    assert_eq!(read_rows(&filtered_answer), &json!([["map"]]));
    server.shutdown().await;
}

#[tokio::test]
async fn string_comparisons_and_sorts_use_byte_order() {
    let (server, catalog) = TestServer::start().await;
    let build_service_row = |timestamp_micros: i64, service_name: &'static str| LogRow {
        tenant_id: TENANT,
        timestamp_micros,
        service_name: Some(service_name),
        severity_text: Some("INFO"),
        body: None,
        resource_attributes: &[],
        log_attributes: &[],
    };
    let rows_written = [
        build_service_row(NOON_MICROS, "api"),
        build_service_row(NOON_MICROS + 1, "Zeta"),
        build_service_row(NOON_MICROS + 2, "\u{e9}clair"),
        build_service_row(NOON_MICROS + 3, "zulu"),
    ];
    commit_log_rows(&catalog, &rows_written).await.expect("commit logs");
    // UTF-8 bytes: 'Z' 0x5A < 'a' 0x61 < 'z' 0x7A < 'é' 0xC3 0xA9.
    let sorted_answer = run_query(&server, "source=logs | sort service_name | fields service_name").await;
    assert_eq!(
        read_rows(&sorted_answer),
        &json!([["Zeta"], ["api"], ["zulu"], ["\u{e9}clair"]])
    );
    let greater_answer = run_query(
        &server,
        "source=logs | where service_name > 'zulu' | fields service_name",
    )
    .await;
    assert_eq!(read_rows(&greater_answer), &json!([["\u{e9}clair"]]));
    let smallest_answer = run_query(&server, "source=logs | where service_name < 'a' | fields service_name").await;
    assert_eq!(read_rows(&smallest_answer), &json!([["Zeta"]]));
    server.shutdown().await;
}

#[tokio::test]
async fn integer_division_truncates_and_decimal_columns_read_as_double() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    // 3.25 USD per million input tokens, a Decimal(38, 10) holding 32_500_000_000. Written before
    // the first query: the engine refreshes the snapshot of a table the WAL does not feed on an
    // interval, not per request.
    commit_price_rows(
        &catalog,
        &[PriceRow {
            model: "gpt-test",
            input_usd_per_1m: Some(32_500_000_000),
        }],
    )
    .await
    .expect("commit prices");
    let division_answer = run_query(
        &server,
        &format!(
            "{ERROR_ROW} | eval a = 7 / 2, b = -7 / 2, c = 7.0 / 2, d = 1 / 0, e = mod(7, 0) | fields a, b, c, d, e"
        ),
    )
    .await;
    assert_eq!(read_rows(&division_answer), &json!([[3, -3, 3.5, null, null]]));
    let decimal_answer = run_query(&server, "source=prices | fields model, input_usd_per_1m").await;
    assert_eq!(
        decimal_answer.body["schema"],
        json!([{"name": "model", "type": "string"}, {"name": "input_usd_per_1m", "type": "double"}])
    );
    assert_eq!(read_rows(&decimal_answer), &json!([["gpt-test", 3.25]]));
    server.shutdown().await;
}

#[tokio::test]
async fn array_functions_return_null_on_a_null_list_and_a_value_on_an_empty_list() {
    let (server, catalog) = TestServer::start().await;
    let build_reasons_row = |timestamp_micros: i64, finish_reasons: Option<&'static [&'static str]>| OperationRow {
        finish_reasons,
        ..build_test_operation_row(TENANT, timestamp_micros, None)
    };
    let rows_written = [
        build_reasons_row(NOON_MICROS, Some(&["stop", "length"])),
        build_reasons_row(NOON_MICROS + 1, Some(&[])),
        build_reasons_row(NOON_MICROS + 2, None),
    ];
    commit_operation_rows(&catalog, &rows_written).await.expect("commit operations");
    let answer = run_query(
        &server,
        "source=operations | sort timestamp | eval e = exists(finish_reasons, x -> x = 'stop'), \
         n = array_length(finish_reasons), j = mvjoin(finish_reasons, ','), i = mvindex(finish_reasons, 0), \
         l = mvindex(finish_reasons, -1), s = mvindex(finish_reasons, 0, 1), f = forall(finish_reasons, x -> x = 'stop'), \
         k = filter(finish_reasons, x -> x != 'stop'), u = transform(finish_reasons, x -> upper(x)) \
         | fields e, n, j, i, l, s, f, k, u",
    )
    .await;
    assert_eq!(
        read_rows(&answer),
        &json!([
            [
                true,
                2,
                "stop,length",
                "stop",
                "length",
                ["stop", "length"],
                false,
                ["length"],
                ["STOP", "LENGTH"]
            ],
            [false, 0, "", null, null, [], true, [], []],
            [null, null, null, null, null, null, null, null, null]
        ])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn locate_is_case_sensitive_and_md5_renders_lowercase_hex() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(
        &server,
        &format!(
            "{ERROR_ROW} | eval a = locate('full', body), b = locate('FULL', body), c = md5(body) | fields a, b, c"
        ),
    )
    .await;
    // md5("disk full") computed outside IceGate (`printf 'disk full' | md5`).
    assert_eq!(read_rows(&answer), &json!([[6, 0, "0ce552d660ffab58758aa955f2c9cd7d"]]));
    server.shutdown().await;
}

#[tokio::test]
async fn timestampdiff_in_microseconds_returns_a_long() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(
        &server,
        "source=logs | sort timestamp | eval d = timestampdiff(MICROSECOND, timestamp('2026-10-08 12:00:00'), timestamp), \
         s = timestampdiff(SECOND, timestamp('2026-10-08 12:00:00'), timestamp) | fields d, s",
    )
    .await;
    assert_eq!(
        answer.body["schema"],
        json!([{"name": "d", "type": "bigint"}, {"name": "s", "type": "bigint"}])
    );
    assert_eq!(read_rows(&answer), &json!([[0, 0], [1_250_000, 1], [2_000_001, 2]]));
    // The rows lie 0, 1.25 and 2.000001 seconds after noon: whole milliseconds truncate toward
    // zero, and every coarser unit is 0.
    let coarse_units_answer = run_query(
        &server,
        "source=logs | sort timestamp | eval ms = timestampdiff(MILLISECOND, timestamp('2026-10-08 12:00:00'), timestamp), \
         mi = timestampdiff(MINUTE, timestamp('2026-10-08 12:00:00'), timestamp), \
         h = timestampdiff(HOUR, timestamp('2026-10-08 12:00:00'), timestamp), \
         d = timestampdiff(DAY, timestamp('2026-10-08 12:00:00'), timestamp), \
         w = timestampdiff(WEEK, timestamp('2026-10-08 12:00:00'), timestamp) | fields ms, mi, h, d, w",
    )
    .await;
    assert_eq!(
        read_rows(&coarse_units_answer),
        &json!([[0, 0, 0, 0, 0], [1250, 0, 0, 0, 0], [2000, 0, 0, 0, 0]])
    );
    let refused_answer = run_query(
        &server,
        "source=logs | eval d = timestampdiff(MONTH, timestamp, timestamp)",
    )
    .await;
    assert_ppl_error(
        &refused_answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "SEMANTIC_ERROR",
    );
    server.shutdown().await;
}

#[tokio::test]
async fn cast_to_each_supported_type() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(
        &server,
        &format!(
            "{ERROR_ROW} | eval a = cast('2.5' as double), b = cast(2.9 as long), c = cast('7' as int), \
             d = cast(42 as string), e = cast('true' as boolean), f = cast('2026-10-08 12:00:00.5' as timestamp), \
             g = cast('1.5' as float), h = cast('8' as integer) \
             | fields a, b, c, d, e, f, g, h"
        ),
    )
    .await;
    assert_eq!(
        answer.body["schema"],
        json!([
            {"name": "a", "type": "double"},
            {"name": "b", "type": "bigint"},
            {"name": "c", "type": "int"},
            {"name": "d", "type": "string"},
            {"name": "e", "type": "boolean"},
            {"name": "f", "type": "timestamp"},
            {"name": "g", "type": "double"},
            {"name": "h", "type": "int"}
        ])
    );
    assert_eq!(
        read_rows(&answer),
        &json!([[2.5, 2, 7, "42", true, "2026-10-08 12:00:00.5", 1.5, 8]])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_string_column_that_is_not_a_timestamp_fails_the_query_at_execution() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    // A column's values are unknown at planning, so `disk full` is refused only when it is cast.
    let answer = run_query(&server, &format!("{ERROR_ROW} | eval v = timestamp(body) | fields v")).await;
    assert_ppl_error(
        &answer,
        StatusCode::INTERNAL_SERVER_ERROR,
        "IllegalStateException",
        "EXECUTION_ERROR",
    );
    let details = answer.body["error"]["details"].as_str().expect("details");
    assert!(details.contains("disk full"), "details {details}");
    server.shutdown().await;
}

#[tokio::test]
async fn rename_and_eval_feed_later_commands() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    let answer = run_query(
        &server,
        "source=logs | rename severity_text as level | eval loud = upper(level), level = lower(level), n = 1, m = n + 1 \
         | where loud = 'ERROR' | fields level, loud, m",
    )
    .await;
    assert_eq!(read_rows(&answer), &json!([["error", "ERROR", 2]]));
    // Renaming onto an existing field removes that field; renaming a field onto itself keeps it.
    let replacing_answer = run_query(
        &server,
        &format!(
            "{ERROR_ROW} | rename service_name as body, severity_text as severity_text | fields body, severity_text"
        ),
    )
    .await;
    assert_eq!(read_rows(&replacing_answer), &json!([["api", "ERROR"]]));
    let missing_field_answer = run_query(&server, "source=logs | rename nope as x").await;
    assert_ppl_error(
        &missing_field_answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "FIELD_NOT_FOUND",
    );
    server.shutdown().await;
}

#[tokio::test]
async fn each_refused_expression_is_a_400_with_its_stated_reason() {
    let (server, catalog) = TestServer::start().await;
    seed_logs(&catalog).await;
    commit_operation_rows(&catalog, &[build_test_operation_row(TENANT, NOON_MICROS, None)])
        .await
        .expect("commit operations");
    // Each reason is IceGate's own wording, asserted literally.
    let semantic_cases = [
        (
            "source=logs | eval v = lower(body, body)",
            "function [lower] takes 1 argument(s), not 2",
        ),
        (
            "source=logs | eval v = timestamp(5)",
            "timestamp() takes a string or a timestamp, not Int64; use from_unixtime for numbers",
        ),
        (
            "source=logs | eval v = date_format(timestamp, '%U')",
            "date_format does not support the specifier %U",
        ),
        (
            "source=logs | eval v = date_format(timestamp, body)",
            "date_format takes a string literal here",
        ),
        (
            "source=logs | eval v = like(body, 'x', severity_text)",
            "like takes true or false as its third argument",
        ),
        (
            "source=logs | eval v = mvindex(body, service_name)",
            "mvindex takes integer literals as indexes",
        ),
        (
            "source=operations | eval v = forall(finish_reasons, (x, y) -> x = y)",
            "the lambda of forall takes one parameter",
        ),
        (
            "source=logs | rename service* as svc",
            "rename takes field names, not the pattern [service*]",
        ),
        ("source=logs | rename * as svc", "rename takes field names, not *"),
        (
            "source=logs | eval v = coalesce(x -> x = 'a')",
            "a lambda is only an argument of an array function",
        ),
        (
            "source=operations | eval v = exists(finish_reasons, 'stop')",
            "exists takes a lambda as its second argument",
        ),
        (
            "source=logs | fields - service_name | where resource_attributes.service.name = 'api'",
            "field [resource_attributes.service.name] reads the column [service_name], which an earlier command \
             removed, renamed or replaced",
        ),
        (
            "source=logs | rename service_name as svc, body as service_name | where resource_attributes.service.name = 'api'",
            "field [resource_attributes.service.name] reads the column [service_name], which an earlier command \
             removed, renamed or replaced",
        ),
        (
            "source=logs | eval service_name = 'x' | where resource_attributes.service.name = 'api'",
            "field [resource_attributes.service.name] reads the column [service_name], which an earlier command \
             removed, renamed or replaced",
        ),
        // `logs` stores the attribute in a column, `spans` keeps it in its resource map.
        (
            "source=logs,spans | where resource_attributes.service.name = 'api'",
            "field [resource_attributes.service.name] reads a column in some source indices and not in others",
        ),
    ];
    for (query, expected_reason) in semantic_cases {
        let answer = run_query(&server, query).await;
        assert_ppl_error(
            &answer,
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "SEMANTIC_ERROR",
        );
        assert_eq!(answer.body["error"]["reason"], json!(expected_reason), "query {query}");
    }
    // The reason names the field's Arrow type after this prefix.
    let not_a_list_answer = run_query(&server, "source=logs | eval v = forall(body, x -> x = 'a')").await;
    assert_ppl_error(
        &not_a_list_answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "SEMANTIC_ERROR",
    );
    let reason = not_a_list_answer.body["error"]["reason"].as_str().expect("reason");
    assert!(
        reason.starts_with("forall takes an array as its first argument, not "),
        "reason {reason}"
    );
    let cast_answer = run_query(&server, "source=logs | eval v = cast(body as date)").await;
    assert_ppl_error(
        &cast_answer,
        StatusCode::BAD_REQUEST,
        "UnsupportedOperationException",
        "UNSUPPORTED_OPERATION",
    );
    assert_eq!(cast_answer.body["error"]["context"]["function"], json!("cast as date"));
    server.shutdown().await;
}

/// The aggregation rows of `acme`, with one `globex` row beside them in the same batch:
///
/// | body | timestamp (UTC) | service_name | severity_text |
/// |---|---|---|---|
/// | a1 | 2026-10-04 23:59:59 (Sunday) | api | ERROR |
/// | a2 | 2026-10-05 00:00:00 (Monday) | api | INFO |
/// | a3 | 2026-10-05 10:30:45.25 | web | INFO |
/// | a4 | 2026-10-07 13:15:00 (Wednesday) | NULL | ERROR |
/// | a5 | 2026-12-31 23:00:00 | db | NULL |
/// | a6 | 2027-01-01 00:00:00 | api | INFO |
/// | a7 | 2026-03-31 12:00:00 | web | WARN |
/// | g1 (globex) | 2026-10-05 00:00:00 | api | ERROR |
async fn seed_aggregation_logs(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    let row = |tenant_id: &'static str,
               body: &'static str,
               timestamp_micros: i64,
               service_name: Option<&'static str>,
               severity_text: Option<&'static str>| LogRow {
        tenant_id,
        timestamp_micros,
        service_name,
        severity_text,
        body: Some(body),
        resource_attributes: &[],
        log_attributes: &[],
    };
    let rows_written = [
        row(TENANT, "a1", 1_791_158_399_000_000, Some("api"), Some("ERROR")),
        row(TENANT, "a2", 1_791_158_400_000_000, Some("api"), Some("INFO")),
        row(TENANT, "a3", 1_791_196_245_250_000, Some("web"), Some("INFO")),
        row(TENANT, "a4", 1_791_378_900_000_000, None, Some("ERROR")),
        row(TENANT, "a5", 1_798_758_000_000_000, Some("db"), None),
        row(TENANT, "a6", 1_798_761_600_000_000, Some("api"), Some("INFO")),
        row(TENANT, "a7", 1_774_958_400_000_000, Some("web"), Some("WARN")),
        row(OTHER_TENANT, "g1", 1_791_158_400_000_000, Some("api"), Some("ERROR")),
    ];
    commit_log_rows(catalog, &rows_written).await
}

/// One `acme` operation per value of `input_tokens`, one microsecond apart from noon, in order.
async fn seed_token_operations(catalog: &Arc<dyn Catalog>, input_tokens: &[Option<i64>]) -> Result<(), RowWriteError> {
    let rows_written: Vec<OperationRow<'_>> = (0_i64..)
        .zip(input_tokens)
        .map(|(offset, tokens)| OperationRow {
            input_tokens: *tokens,
            ..build_test_operation_row(TENANT, NOON_MICROS + offset, None)
        })
        .collect();
    commit_operation_rows(catalog, &rows_written).await
}

#[tokio::test]
async fn stats_without_by_on_empty_input_answers_one_row() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    let answer = run_query(
        &server,
        "source=logs | where severity_text = 'NONE' | stats count() as n, dc(service_name) as d, \
         values(service_name) as v, min(body) as m",
    )
    .await;
    assert_eq!(read_column_names(&answer), vec!["n", "d", "v", "m"]);
    assert_eq!(read_rows(&answer), &json!([[0, 0, [], null]]));
    server.shutdown().await;
}

#[tokio::test]
async fn stats_by_span_buckets_fixed_units_from_the_epoch() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    let answer = run_query(&server, "source=logs | stats count() as n by span(timestamp, 12h)").await;
    assert_eq!(read_column_names(&answer), vec!["n", "span(timestamp,12h)"]);
    assert_eq!(
        read_rows(&answer),
        &json!([
            [1, "2026-03-31 12:00:00"],
            [1, "2026-10-04 12:00:00"],
            [2, "2026-10-05 00:00:00"],
            [1, "2026-10-07 12:00:00"],
            [1, "2026-12-31 12:00:00"],
            [1, "2027-01-01 00:00:00"]
        ])
    );
    // 7-minute buckets count from 1970-01-01T00:00Z, not from the hour: 10:30:45.25 falls in the
    // bucket starting at 10:24.
    let answer = run_query(
        &server,
        "source=logs | where body = 'a3' | stats count() as n by span(timestamp, 7m) as bucket",
    )
    .await;
    assert_eq!(read_rows(&answer), &json!([[1, "2026-10-05 10:24:00"]]));
    server.shutdown().await;
}

#[tokio::test]
async fn week_month_quarter_and_year_spans_align_to_utc_calendar_starts() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    let cases = [
        // Weeks start on Monday: a1 (Sunday 10-04) belongs to the week of Monday 09-28.
        (
            "1w",
            json!([
                [1, "2026-03-30 00:00:00"],
                [1, "2026-09-28 00:00:00"],
                [3, "2026-10-05 00:00:00"],
                [2, "2026-12-28 00:00:00"]
            ]),
        ),
        // 7d is a fixed span from the epoch (a Thursday), not a week.
        (
            "7d",
            json!([
                [1, "2026-03-26 00:00:00"],
                [4, "2026-10-01 00:00:00"],
                [2, "2026-12-31 00:00:00"]
            ]),
        ),
        (
            "1M",
            json!([
                [1, "2026-03-01 00:00:00"],
                [4, "2026-10-01 00:00:00"],
                [1, "2026-12-01 00:00:00"],
                [1, "2027-01-01 00:00:00"]
            ]),
        ),
        // Two-month buckets count from January 1970: Jan-Feb, Mar-Apr, …, Sep-Oct, Nov-Dec.
        (
            "2M",
            json!([
                [1, "2026-03-01 00:00:00"],
                [4, "2026-09-01 00:00:00"],
                [1, "2026-11-01 00:00:00"],
                [1, "2027-01-01 00:00:00"]
            ]),
        ),
        (
            "1q",
            json!([
                [1, "2026-01-01 00:00:00"],
                [5, "2026-10-01 00:00:00"],
                [1, "2027-01-01 00:00:00"]
            ]),
        ),
        ("1y", json!([[6, "2026-01-01 00:00:00"], [1, "2027-01-01 00:00:00"]])),
    ];
    for (span, expected) in cases {
        let query = format!("source=logs | stats count() as n by span(timestamp, {span}) as bucket");
        let answer = run_query(&server, &query).await;
        assert_eq!(read_rows(&answer), &expected, "span {span}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn every_span_unit_spelling_of_the_pinned_lexer_has_its_stated_meaning() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    // a3 is 2026-10-05 10:30:45.25, a Monday.
    let spellings = [
        (&["ms", "MS", "millisecond"][..], "2026-10-05 10:30:45.25"),
        (
            &["s", "S", "sec", "secs", "second", "seconds"][..],
            "2026-10-05 10:30:45",
        ),
        (&["m", "min", "MINS", "minute", "minutes"][..], "2026-10-05 10:30:00"),
        (&["h", "hr", "hrs", "hour", "Hours"][..], "2026-10-05 10:00:00"),
        (&["d", "D", "day", "days"][..], "2026-10-05 00:00:00"),
        (&["w", "week", "weeks"][..], "2026-10-05 00:00:00"),
        (&["M", "mon", "month", "months"][..], "2026-10-01 00:00:00"),
        (&["q", "qtr", "qtrs", "quarter", "quarters"][..], "2026-10-01 00:00:00"),
        (&["y", "Y", "year", "years"][..], "2026-01-01 00:00:00"),
    ];
    for (units, expected) in spellings {
        for unit in units {
            let query =
                format!("source=logs | where body = 'a3' | stats count() as n by span(timestamp, 1{unit}) as bucket");
            let answer = run_query(&server, &query).await;
            assert_eq!(read_rows(&answer), &json!([[1, expected]]), "unit {unit}");
        }
    }
    // a4 is a Wednesday: a day bucket and a week bucket differ.
    for (unit, expected) in [("d", "2026-10-07 00:00:00"), ("w", "2026-10-05 00:00:00")] {
        let query =
            format!("source=logs | where body = 'a4' | stats count() as n by span(timestamp, 1{unit}) as bucket");
        let answer = run_query(&server, &query).await;
        assert_eq!(read_rows(&answer), &json!([[1, expected]]), "unit {unit}");
    }
    // The lexer admits these spellings in a span length; they name no unit a span buckets by.
    for unit in ["us", "cs", "ds", "milliseconds", "yr", "yrs"] {
        let query = format!("source=logs | stats count() as n by span(timestamp, 1{unit})");
        let answer = run_query(&server, &query).await;
        assert_ppl_error(
            &answer,
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "SEMANTIC_ERROR",
        );
    }
    server.shutdown().await;
}

#[tokio::test]
async fn an_unknown_span_unit_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    seed_token_operations(&catalog, &[Some(1)])
        .await
        .expect("seed token operations");
    for query in [
        "source=logs | stats count() by span(timestamp, 1yr)",
        "source=logs | stats count() by span(timestamp, 1.5h)",
        "source=logs | stats count() by span(timestamp, 10)",
        "source=logs | stats count() by span(timestamp, 0h)",
        "source=operations | stats count() by span(input_tokens, 1h)",
        // 4e9 days overflow i64 microseconds: refused, not wrapped.
        "source=logs | stats count() by span(timestamp, 4000000000d)",
        // 1e20 is a whole number at or above 2^63: refused, not saturated to i64::MAX.
        "source=operations | stats count() by span(input_tokens, 100000000000000000000)",
    ] {
        let answer = run_query(&server, query).await;
        assert_ppl_error(
            &answer,
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "SEMANTIC_ERROR",
        );
    }
    server.shutdown().await;
}

#[tokio::test]
async fn bucket_nullable_keeps_or_drops_null_groups_as_stated() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    let nullable_answer = run_query(&server, "source=logs | stats count() as n by service_name").await;
    assert_eq!(
        read_rows(&nullable_answer),
        &json!([[1, null], [3, "api"], [1, "db"], [2, "web"]])
    );
    let explicit_answer = run_query(
        &server,
        "source=logs | stats bucket_nullable=true count() as n by service_name",
    )
    .await;
    assert_eq!(read_rows(&explicit_answer), read_rows(&nullable_answer));
    let dropped_answer = run_query(
        &server,
        "source=logs | stats bucket_nullable=false count() as n by service_name",
    )
    .await;
    assert_eq!(read_rows(&dropped_answer), &json!([[3, "api"], [1, "db"], [2, "web"]]));
    server.shutdown().await;
}

#[tokio::test]
async fn chained_stats_where_stats_composes() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    // Per severity: ERROR 2, INFO 3, WARN 1, NULL 1; two groups have more than one row.
    let answer = run_query(
        &server,
        "source=logs | stats count() as n by severity_text | where n > 1 | stats count() as groups, sum(n) as rows",
    )
    .await;
    assert_eq!(read_rows(&answer), &json!([[2, 5]]));
    server.shutdown().await;
}

/// 3000 `acme` operations: `request_model` is `model-<i mod 2000>`, `operation_name` is `chat`
/// for even `i` and `embed` for odd `i`, plus one operation without a model.
async fn seed_model_operations(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    let models: Vec<String> = (0..3000).map(|index| format!("model-{}", index % 2000)).collect();
    let mut rows_written: Vec<OperationRow<'_>> = (0_i64..)
        .zip(&models)
        .map(|(index, model)| OperationRow {
            operation_name: if index % 2 == 0 { "chat" } else { "embed" },
            request_model: Some(model.as_str()),
            ..build_test_operation_row(TENANT, NOON_MICROS + index, None)
        })
        .collect();
    rows_written.push(build_test_operation_row(TENANT, NOON_MICROS + 3000, None));
    commit_operation_rows(catalog, &rows_written).await
}

#[tokio::test]
async fn dc_is_exact() {
    let (server, catalog) = TestServer::start().await;
    seed_model_operations(&catalog).await.expect("seed model operations");
    let total_answer = run_query(
        &server,
        "source=operations | stats dc(request_model) as d, count() as n",
    )
    .await;
    assert_eq!(read_rows(&total_answer), &json!([[2000, 3001]]));
    // 2000 is even, so the parity of i and of i mod 2000 agree: each name sees 1000 models.
    let grouped_answer = run_query(
        &server,
        "source=operations | stats distinct_count(request_model) as d by operation_name",
    )
    .await;
    assert_eq!(read_rows(&grouped_answer), &json!([[1000, "chat"], [1000, "embed"]]));
    server.shutdown().await;
}

#[tokio::test]
async fn values_is_distinct_sorted_null_free_and_uncapped() {
    let (server, catalog) = TestServer::start().await;
    // Both tables are written before the first query: the engine reads table metadata from a
    // catalog view it refreshes on an interval, so rows committed after a query may not show yet.
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    seed_model_operations(&catalog).await.expect("seed model operations");
    let grouped_answer = run_query(
        &server,
        "source=logs | stats values(service_name) as v by severity_text",
    )
    .await;
    assert_eq!(
        read_rows(&grouped_answer),
        &json!([
            [["db"], null],
            [["api"], "ERROR"],
            [["api", "web"], "INFO"],
            [["web"], "WARN"]
        ])
    );
    let ungrouped_answer = run_query(&server, "source=operations | stats values(request_model) as v").await;
    let values = read_rows(&ungrouped_answer)[0][0].as_array().expect("values list");
    assert_eq!(values.len(), 2000);
    // Byte order: "model-0" < "model-1" < "model-10" < … < "model-999".
    assert_eq!(&values[..3], &[json!("model-0"), json!("model-1"), json!("model-10")]);
    assert_eq!(values.last(), Some(&json!("model-999")));
    server.shutdown().await;
}

#[tokio::test]
async fn dedup_keeps_the_first_row_of_the_incoming_order_and_keepempty_keeps_null_keys() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    let latest_per_service = run_query(
        &server,
        "source=logs | sort - timestamp | dedup service_name | fields service_name, timestamp",
    )
    .await;
    assert_eq!(
        read_rows(&latest_per_service),
        &json!([
            ["api", "2027-01-01 00:00:00"],
            ["db", "2026-12-31 23:00:00"],
            ["web", "2026-10-05 10:30:45.25"]
        ])
    );
    let null_kept_answer = run_query(
        &server,
        "source=logs | sort - timestamp | dedup service_name keepempty=true | fields service_name, timestamp",
    )
    .await;
    assert_eq!(
        read_rows(&null_kept_answer),
        &json!([
            ["api", "2027-01-01 00:00:00"],
            ["db", "2026-12-31 23:00:00"],
            [null, "2026-10-07 13:15:00"],
            ["web", "2026-10-05 10:30:45.25"]
        ])
    );
    let two_per_severity = run_query(
        &server,
        "source=logs | sort timestamp | dedup 2 severity_text | fields severity_text, body",
    )
    .await;
    assert_eq!(
        read_rows(&two_per_severity),
        &json!([
            ["WARN", "a7"],
            ["ERROR", "a1"],
            ["INFO", "a2"],
            ["INFO", "a3"],
            ["ERROR", "a4"]
        ])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn dedup_keeps_the_first_rows_of_the_sort_after_its_column_is_renamed_removed_or_replaced() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    // Newest first: a6 (api), a5 (db), a4 (NULL), a3 (web), a2 (api), a1 (api), a7 (web). Each
    // query keeps the latest row per service whatever a command between the sort and the dedup
    // did to `timestamp`: api a6, db a5, web a3, in that order, and no hidden column shows.
    let cases = [
        (
            "source=logs | sort - timestamp | rename timestamp as ts | dedup service_name | fields service_name, body",
            ["service_name", "body"],
        ),
        (
            "source=logs | sort - timestamp | rename timestamp as timestamp | dedup service_name | fields service_name, body",
            ["service_name", "body"],
        ),
        (
            "source=logs | sort - timestamp | rename timestamp as @timestamp | dedup service_name | fields service_name, body",
            ["service_name", "body"],
        ),
        (
            "source=logs | sort - timestamp | fields service_name, body | dedup service_name",
            ["service_name", "body"],
        ),
        (
            "source=logs | sort - timestamp | fields - timestamp | dedup service_name | fields service_name, body",
            ["service_name", "body"],
        ),
        (
            "source=logs | sort - timestamp | eval timestamp = body | dedup service_name | fields service_name, body",
            ["service_name", "body"],
        ),
        (
            "source=logs | sort - timestamp | eval timestamp = body | fields service_name, body | dedup service_name",
            ["service_name", "body"],
        ),
        (
            "source=logs | sort - timestamp | bin timestamp span=1d | dedup service_name | fields service_name, body",
            ["service_name", "body"],
        ),
        // `timestamp` now holds the body.
        (
            "source=logs | sort - timestamp | rename body as timestamp | dedup service_name | fields service_name, timestamp",
            ["service_name", "timestamp"],
        ),
    ];
    for (query, column_names) in cases {
        let answer = run_query(&server, query).await;
        assert_eq!(read_column_names(&answer), column_names, "query {query:?}");
        assert_eq!(
            read_rows(&answer),
            &json!([["api", "a6"], ["db", "a5"], ["web", "a3"]]),
            "query {query:?}"
        );
    }
    // Without a final `fields`, the output is the source columns but `timestamp`, in table order.
    let removed_sort_answer = run_query(
        &server,
        "source=logs | sort - timestamp | fields - timestamp | dedup service_name",
    )
    .await;
    let expected_names: Vec<String> = logs_schema()
        .expect("logs schema")
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.name.clone())
        .filter(|name| name != COL_TENANT_ID && name != COL_TIMESTAMP)
        .collect();
    assert_eq!(read_column_names(&removed_sort_answer), expected_names);
    // A later sort sets a new order: the first body per service, ascending.
    let resorted_answer = run_query(
        &server,
        "source=logs | sort - timestamp | fields service_name, body | sort body | dedup service_name",
    )
    .await;
    assert_eq!(
        read_rows(&resorted_answer),
        &json!([["api", "a1"], ["web", "a3"], ["db", "a5"]])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_field_name_starting_with_the_reserved_prefix_is_a_400_naming_it() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    // The plans' own columns start with this prefix, among them the hidden sort columns that no
    // output shows: a field so named would vanish from the answer or read one of them.
    let cases = [
        (
            "source=logs | eval __ppl_order_x = 1 | fields body, __ppl_order_x",
            "__ppl_order_x",
        ),
        (
            "source=logs | sort - timestamp | rename body as __ppl_order_0 | dedup service_name",
            "__ppl_order_0",
        ),
        (
            "source=logs | sort - timestamp | fields service_name, body | fields __ppl_order_0",
            "__ppl_order_0",
        ),
        ("source=logs | where __ppl_main = 'x'", "__ppl_main"),
    ];
    for (query, name) in cases {
        let answer = run_query(&server, query).await;
        assert_ppl_error(
            &answer,
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "SEMANTIC_ERROR",
        );
        assert_eq!(
            answer.body["error"]["reason"],
            json!(format!("field name [{name}] starts with the reserved prefix [__ppl_]")),
            "query {query}"
        );
    }
    // A name that only contains the prefix is a field like any other.
    let unreserved_answer = run_query(
        &server,
        "source=logs | where body = 'a3' | eval my__ppl_order_x = 1 | fields body, my__ppl_order_x",
    )
    .await;
    assert_eq!(read_column_names(&unreserved_answer), vec!["body", "my__ppl_order_x"]);
    assert_eq!(read_rows(&unreserved_answer), &json!([["a3", 1]]));
    server.shutdown().await;
}

#[tokio::test]
async fn top_and_rare_rank_values_as_stated() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    let top_answer = run_query(&server, "source=logs | top 2 service_name").await;
    assert_eq!(read_column_names(&top_answer), vec!["service_name", "count"]);
    assert_eq!(read_rows(&top_answer), &json!([["api", 3], ["web", 2]]));
    // Ties rank by the field value, ascending with NULL last.
    let rare_answer = run_query(&server, "source=logs | rare service_name").await;
    assert_eq!(
        read_rows(&rare_answer),
        &json!([["db", 1], [null, 1], ["web", 2], ["api", 3]])
    );
    let grouped_answer = run_query(&server, "source=logs | top 1 severity_text by service_name").await;
    assert_eq!(
        read_column_names(&grouped_answer),
        vec!["service_name", "severity_text", "count"]
    );
    assert_eq!(
        read_rows(&grouped_answer),
        &json!([
            [null, "ERROR", 1],
            ["api", "INFO", 2],
            ["db", null, 1],
            ["web", "INFO", 1]
        ])
    );
    let options_answer = run_query(
        &server,
        "source=logs | top 5 countfield='hits' usenull=false service_name",
    )
    .await;
    assert_eq!(read_column_names(&options_answer), vec!["service_name", "hits"]);
    assert_eq!(read_rows(&options_answer), &json!([["api", 3], ["web", 2], ["db", 1]]));
    let hidden_count_answer = run_query(&server, "source=logs | top 1 showcount=false service_name").await;
    assert_eq!(read_rows(&hidden_count_answer), &json!([["api"]]));
    let clash_answer = run_query(&server, "source=logs | eval count = 1 | top service_name").await;
    assert_ppl_error(
        &clash_answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "SEMANTIC_ERROR",
    );
    server.shutdown().await;
}

#[tokio::test]
async fn timechart_puts_time_first_then_the_by_field_then_the_aggregate_without_filling_empty_buckets() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    let by_service_answer = run_query(
        &server,
        "source=logs | timechart timefield=timestamp span=1d count() by service_name",
    )
    .await;
    assert_eq!(
        read_column_names(&by_service_answer),
        vec!["timestamp", "service_name", "count()"]
    );
    // No bucket for 2026-10-06, which holds no row; the NULL series is `NULL`.
    assert_eq!(
        read_rows(&by_service_answer),
        &json!([
            ["2026-03-31 00:00:00", "web", 1],
            ["2026-10-04 00:00:00", "api", 1],
            ["2026-10-05 00:00:00", "api", 1],
            ["2026-10-05 00:00:00", "web", 1],
            ["2026-10-07 00:00:00", "NULL", 1],
            ["2026-12-31 00:00:00", "db", 1],
            ["2027-01-01 00:00:00", "api", 1]
        ])
    );
    let total_answer = run_query(
        &server,
        "source=logs | timechart timefield=timestamp span=1y count() as n",
    )
    .await;
    assert_eq!(read_column_names(&total_answer), vec!["timestamp", "n"]);
    assert_eq!(
        read_rows(&total_answer),
        &json!([["2026-01-01 00:00:00", 6], ["2027-01-01 00:00:00", 1]])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn timechart_with_more_series_than_its_limit_groups_the_rest_as_the_stated_rule() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    // Series totals over all buckets: api 3, web 2, db 1; limit=1 keeps api. web and db merge
    // into OTHER (count merges by sum); the NULL series is `NULL`. Labels sort byte-wise.
    let other_answer = run_query(
        &server,
        "source=logs | timechart timefield=timestamp span=1y limit=1 count() by service_name",
    )
    .await;
    assert_eq!(
        read_rows(&other_answer),
        &json!([
            ["2026-01-01 00:00:00", "NULL", 1],
            ["2026-01-01 00:00:00", "OTHER", 3],
            ["2026-01-01 00:00:00", "api", 2],
            ["2027-01-01 00:00:00", "api", 1]
        ])
    );
    // useother=false drops every series beyond the limit, the NULL series included.
    let dropped_answer = run_query(
        &server,
        "source=logs | timechart timefield=timestamp span=1y limit=1 useother=false count() by service_name",
    )
    .await;
    assert_eq!(
        read_rows(&dropped_answer),
        &json!([["2026-01-01 00:00:00", "api", 2], ["2027-01-01 00:00:00", "api", 1]])
    );
    // Series rank by the sum of the aggregation, so a limit over a string aggregation is refused;
    // limit=0 ranks nothing and keeps the answer.
    let string_ranked_answer = run_query(
        &server,
        "source=logs | timechart timefield=timestamp span=1y limit=1 min(body) by service_name",
    )
    .await;
    assert_ppl_error(
        &string_ranked_answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "SEMANTIC_ERROR",
    );
    let details = string_ranked_answer.body["error"]["details"].as_str().expect("details");
    assert!(details.contains("[min(body)]"), "details {details}");
    let string_unranked_answer = run_query(
        &server,
        "source=logs | timechart timefield=timestamp span=1y limit=0 min(body) by service_name",
    )
    .await;
    assert_eq!(
        read_rows(&string_unranked_answer),
        &json!([
            ["2026-01-01 00:00:00", "api", "a1"],
            ["2026-01-01 00:00:00", "db", "a5"],
            ["2026-01-01 00:00:00", "web", "a3"],
            ["2026-01-01 00:00:00", null, "a4"],
            ["2027-01-01 00:00:00", "api", "a6"]
        ])
    );
    // limit=0 keeps every series under its own value; the NULL series stays null.
    let unlimited_answer = run_query(
        &server,
        "source=logs | timechart timefield=timestamp span=1y limit=0 count() by service_name",
    )
    .await;
    assert_eq!(
        read_rows(&unlimited_answer),
        &json!([
            ["2026-01-01 00:00:00", "api", 2],
            ["2026-01-01 00:00:00", "db", 1],
            ["2026-01-01 00:00:00", "web", 2],
            ["2026-01-01 00:00:00", null, 1],
            ["2027-01-01 00:00:00", "api", 1]
        ])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn timechart_on_a_native_index_without_timefield_is_field_not_found() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    let missing_field_answer = run_query(&server, "source=logs | timechart span=1d count()").await;
    assert_ppl_error(
        &missing_field_answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "FIELD_NOT_FOUND",
    );
    assert_eq!(
        missing_field_answer.body["error"]["context"]["field"],
        json!("@timestamp")
    );
    let renamed_answer = run_query(
        &server,
        "source=logs | rename timestamp as @timestamp | timechart span=1M count()",
    )
    .await;
    assert_eq!(read_column_names(&renamed_answer), vec!["@timestamp", "count()"]);
    assert_eq!(
        read_rows(&renamed_answer),
        &json!([
            ["2026-03-01 00:00:00", 1],
            ["2026-10-01 00:00:00", 4],
            ["2026-12-01 00:00:00", 1],
            ["2027-01-01 00:00:00", 1]
        ])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn span_time_columns_are_never_null() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    // `t` is NULL on every row but the two ERROR rows; those rows form no bucket.
    let answer = run_query(
        &server,
        "source=logs | eval t = case(severity_text = 'ERROR', timestamp) | stats count() as n by span(t, 1d) as day",
    )
    .await;
    assert_eq!(
        read_rows(&answer),
        &json!([[1, "2026-10-04 00:00:00"], [1, "2026-10-07 00:00:00"]])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_numeric_span_over_null_forms_a_null_group_unless_bucket_nullable_is_false() {
    let (server, catalog) = TestServer::start().await;
    seed_token_operations(
        &catalog,
        &[Some(15), Some(25), Some(-5), Some(130), None, Some(25), Some(7)],
    )
    .await
    .expect("seed token operations");
    // An integer span truncates toward zero: -5 and 7 share the bucket 0.
    let nullable_answer = run_query(
        &server,
        "source=operations | stats count() as n by span(input_tokens, 10)",
    )
    .await;
    assert_eq!(read_column_names(&nullable_answer), vec!["n", "span(input_tokens,10)"]);
    assert_eq!(
        read_rows(&nullable_answer),
        &json!([[1, null], [2, 0], [1, 10], [2, 20], [1, 130]])
    );
    let dropped_answer = run_query(
        &server,
        "source=operations | stats bucket_nullable=false count() as n by span(input_tokens, 10)",
    )
    .await;
    assert_eq!(read_rows(&dropped_answer), &json!([[2, 0], [1, 10], [2, 20], [1, 130]]));
    server.shutdown().await;
}

#[tokio::test]
async fn bin_buckets_numeric_and_time_values() {
    let (server, catalog) = TestServer::start().await;
    seed_aggregation_logs(&catalog).await.expect("seed aggregation logs");
    // The last value is 2^53 + 1, which an `f64` cannot hold.
    seed_token_operations(
        &catalog,
        &[
            Some(15),
            Some(25),
            Some(-5),
            Some(130),
            None,
            Some(25),
            Some(7),
            Some(9_007_199_254_740_993),
        ],
    )
    .await
    .expect("seed token operations");
    let numbers_answer = run_query(
        &server,
        "source=operations | sort timestamp | bin input_tokens span=10 | fields input_tokens",
    )
    .await;
    assert_eq!(
        numbers_answer.body["schema"],
        json!([{"name": "input_tokens", "type": "string"}])
    );
    assert_eq!(
        read_rows(&numbers_answer),
        &json!([
            ["10-20"],
            ["20-30"],
            ["-10-0"],
            ["130-140"],
            [null],
            ["20-30"],
            ["0-10"],
            ["9007199254740990-9007199254741000"]
        ])
    );
    // Integers bin exactly: 2^53 + 1 is its own bin of width 1.
    let exact_answer = run_query(
        &server,
        "source=operations | where input_tokens > 1000 | bin input_tokens span=1 | fields input_tokens",
    )
    .await;
    assert_eq!(
        read_rows(&exact_answer),
        &json!([["9007199254740993-9007199254740994"]])
    );
    // a3 is 2026-10-05 10:30:45.25; 2026-10-05 is day 20731 after the epoch, so its 2-day bin
    // starts on day 20730.
    let cases = [
        ("250ms", json!("2026-10-05 10:30:45.25")),
        ("1h", json!("2026-10-05 10:00:00")),
        ("2", json!("2026-10-05 10:00:00")),
        ("2d", json!("2026-10-04 00:00:00")),
        ("1M", json!("2026-10")),
        // 5e9 µs (83 min 20 s) buckets from the epoch: 10:30:45.25 is 20:45.25 into the bucket.
        ("5000000000us", json!("2026-10-05 10:10:00")),
        // a3 is day 20731 and month 681 after the epoch, fewer than these lengths, so its bins
        // start at the epoch's day and month; 106751991 days and 3443612 months are the longest
        // spans whose bins stay inside the timestamp range.
        ("50000000d", json!("1970-01-01 00:00:00")),
        ("106751991d", json!("1970-01-01 00:00:00")),
        ("3443612M", json!("1970-01")),
    ];
    for (span, expected) in cases {
        let query = format!("source=logs | where body = 'a3' | bin timestamp span={span} | fields timestamp");
        let answer = run_query(&server, &query).await;
        assert_eq!(read_rows(&answer), &json!([[expected]]), "span {span}");
    }
    for query in [
        "source=logs | bin timestamp span=1w",
        "source=operations | bin input_tokens span=2.5",
        "source=logs | bin timestamp bins=3",
        // 1e20 is a whole number at or above 2^63: refused, not saturated to i64::MAX.
        "source=operations | bin input_tokens span=100000000000000000000",
        // 5e9 hours overflow i64 microseconds.
        "source=logs | bin timestamp span=5000000000h",
        // One day or month past the longest calendar spans.
        "source=logs | bin timestamp span=106751992d",
        "source=logs | bin timestamp span=3443613M",
    ] {
        let answer = run_query(&server, query).await;
        assert_ppl_error(
            &answer,
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "SEMANTIC_ERROR",
        );
    }
    // 5e18 fits an i64 but its double does not: the bin bounds would wrap.
    let wrapping_width_answer =
        run_query(&server, "source=operations | bin input_tokens span=5000000000000000000").await;
    assert_ppl_error(
        &wrapping_width_answer,
        StatusCode::BAD_REQUEST,
        "SemanticCheckException",
        "SEMANTIC_ERROR",
    );
    let details = wrapping_width_answer.body["error"]["details"].as_str().expect("details");
    assert!(details.contains("[5000000000000000000]"), "details {details}");
    server.shutdown().await;
}

#[tokio::test]
async fn a_bin_past_the_64_bit_range_renders_exact_bounds_and_an_unrepresentable_quotient_fails_at_execution() {
    let (server, catalog) = TestServer::start().await;
    // In timestamp order: the two 64-bit integer limits, then three `chat` doubles, the last past
    // i64::MAX; then two doubles whose quotient by 10 floors to no 64-bit integer, `embed` 1e300
    // and `rerank` NaN.
    let operation_at =
        |offset: i64, temperature: Option<f64>| build_test_operation_row(TENANT, NOON_MICROS + offset, temperature);
    let rows_written = [
        OperationRow {
            input_tokens: Some(i64::MAX),
            ..operation_at(0, None)
        },
        OperationRow {
            input_tokens: Some(i64::MIN),
            ..operation_at(1, None)
        },
        operation_at(2, Some(-0.5)),
        operation_at(3, Some(25.5)),
        operation_at(4, Some(1e19)),
        OperationRow {
            operation_name: "embed",
            ..operation_at(5, Some(1e300))
        },
        OperationRow {
            operation_name: "rerank",
            ..operation_at(6, Some(f64::NAN))
        },
    ];
    commit_operation_rows(&catalog, &rows_written).await.expect("commit operations");
    // The bin of i64::MAX ends past it; the bin of i64::MIN starts past it.
    let integer_answer = run_query(
        &server,
        "source=operations | where isnotnull(input_tokens) | sort timestamp | bin input_tokens span=10 \
         | fields input_tokens",
    )
    .await;
    assert_eq!(
        read_rows(&integer_answer),
        &json!([
            ["9223372036854775800-9223372036854775810"],
            ["-9223372036854775810--9223372036854775800"]
        ])
    );
    // Width 1: the bin of i64::MAX ends at 2^63.
    let unit_width_answer = run_query(
        &server,
        "source=operations | where input_tokens > 0 | bin input_tokens span=1 | fields input_tokens",
    )
    .await;
    assert_eq!(
        read_rows(&unit_width_answer),
        &json!([["9223372036854775807-9223372036854775808"]])
    );
    // Doubles floor their quotient: -0.5 bins below zero; 1e19 lies past i64::MAX, its quotient
    // 1e18 does not.
    let double_answer = run_query(
        &server,
        "source=operations | where operation_name = 'chat' and isnotnull(temperature) | sort timestamp \
         | bin temperature span=10 | fields temperature",
    )
    .await;
    assert_eq!(
        read_rows(&double_answer),
        &json!([["-10-0"], ["20-30"], ["10000000000000000000-10000000000000000010"]])
    );
    // 1e299 (the floored quotient of 1e300) and NaN are no 64-bit integer: the query fails at
    // execution instead of answering a bin.
    for operation_name in ["embed", "rerank"] {
        let unrepresentable_answer = run_query(
            &server,
            &format!(
                "source=operations | where operation_name = '{operation_name}' | bin temperature span=10 \
                 | fields temperature"
            ),
        )
        .await;
        assert_ppl_error(
            &unrepresentable_answer,
            StatusCode::INTERNAL_SERVER_ERROR,
            "IllegalStateException",
            "EXECUTION_ERROR",
        );
    }
    server.shutdown().await;
}

#[tokio::test]
async fn percentile_values_lie_within_the_stated_tolerance() {
    let (server, catalog) = TestServer::start().await;
    let tokens: Vec<Option<i64>> = (1..=100).map(Some).collect();
    seed_token_operations(&catalog, &tokens).await.expect("seed token operations");
    let answer = run_query(
        &server,
        "source=operations | stats percentile(input_tokens, 50) as p50, p90(input_tokens) as p90, \
         percentile_approx(input_tokens, 25) as p25, median(input_tokens) as median",
    )
    .await;
    assert_eq!(
        answer.body["schema"],
        json!([
            {"name": "p50", "type": "double"},
            {"name": "p90", "type": "double"},
            {"name": "p25", "type": "double"},
            {"name": "median", "type": "double"}
        ])
    );
    // Exact quantiles of 1..=100 by linear interpolation: 50.5, 90.1, 25.75. The percentile is
    // approximate; the stated tolerance is 2.0.
    let row = &read_rows(&answer)[0];
    for (index, exact) in [(0, 50.5), (1, 90.1), (2, 25.75), (3, 50.5)] {
        let value = row[index].as_f64().expect("percentile value");
        assert!((value - exact).abs() <= 2.0, "column {index}: {value} vs {exact}");
    }
    server.shutdown().await;
}

/// `parse`, `fillnull`, `describe` and timestamp literals over real HTTP. Inputs are literal PPL text
/// over rows written here; expected rows are stated by hand from the rules in `ppl/README.md`.
mod commands_and_literals {
    use icegate_common::schema::{
        COL_BODY, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES, COL_OBSERVED_TIMESTAMP, COL_RESOURCE_ATTRIBUTES,
        COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID,
    };
    use icegate_common::{LOGS_TABLE, OPERATIONS_TABLE, SPANS_TABLE};
    use reqwest::StatusCode;
    use serde_json::{Value, json};

    use super::super::data::{
        COMMITTED_WAL_OFFSET, Cell, LogRow, Row, commit_log_rows, commit_rows, write_log_wal_segment,
    };
    use super::super::harness::TestServer;
    use super::super::test_rows::{OPERATION_NAME, STREAM, TEMPERATURE};
    use super::{
        NOON_MICROS, OTHER_TENANT, PplAnswer, TENANT, assert_ppl_error, read_rows, run_query, seed_token_operations,
    };

    /// `error.type` and `error.code` of a query the planner refuses as invalid.
    const SEMANTIC_ERROR: (&str, &str) = ("SemanticCheckException", "SEMANTIC_ERROR");
    /// `error.type` and `error.code` of a command or function IceGate does not plan.
    const UNSUPPORTED_OPERATION: (&str, &str) = ("UnsupportedOperationException", "UNSUPPORTED_OPERATION");

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

    /// Inside the default field-discovery window whatever the wall clock reads, so `describe` lists
    /// the MAP keys committed here.
    fn compute_recent_micros() -> i64 {
        (chrono::Utc::now() - chrono::Duration::hours(1)).timestamp_micros()
    }

    /// `reason` and `details` of a PPL error envelope, joined, to check what the error names.
    fn read_error_text(answer: &PplAnswer) -> String {
        format!(
            "{} {}",
            answer.body["error"]["reason"].as_str().unwrap_or_default(),
            answer.body["error"]["details"].as_str().unwrap_or_default()
        )
    }

    /// A 400 PPL error envelope of `expected_error` (`error.type`, `error.code`) that names `named`.
    fn assert_ppl_400(answer: &PplAnswer, expected_error: (&str, &str), named: &str) {
        let (error_type, code) = expected_error;
        assert_ppl_error(answer, StatusCode::BAD_REQUEST, error_type, code);
        assert!(
            read_error_text(answer).contains(named),
            "error must name {named:?}: {}",
            answer.body
        );
    }

    #[tokio::test]
    async fn parse_extracts_named_groups_into_new_fields() {
        let (server, catalog) = TestServer::start().await;
        commit_log_rows(
            &catalog,
            &[
                build_log_row(
                    TENANT,
                    NOON_MICROS,
                    Some("checkout"),
                    Some("A"),
                    Some("Payment failed: connection timeout"),
                ),
                build_log_row(TENANT, NOON_MICROS, Some("checkout"), Some("B"), Some("no colon here")),
                build_log_row(TENANT, NOON_MICROS, Some("checkout"), Some("C"), None),
                build_log_row(
                    TENANT,
                    NOON_MICROS,
                    Some("checkout"),
                    Some("D"),
                    Some("Disk full: /var 98%"),
                ),
                build_log_row(TENANT, NOON_MICROS, Some("checkout"), Some("E"), Some("abc def")),
                build_log_row(
                    OTHER_TENANT,
                    NOON_MICROS,
                    Some("checkout"),
                    Some("A"),
                    Some("Other tenant: secret"),
                ),
            ],
        )
        .await
        .expect("commit logs");

        let reply = run_query(&server, "source=logs | parse body '(?<errmsg>[^:]+): (?<detail>.+)' | fields severity_text, errmsg, detail | sort severity_text",
        )
        .await;
        assert_eq!(
            read_rows(&reply),
            &json!([
                ["A", "Payment failed", "connection timeout"],
                ["B", "", ""],
                ["C", null, null],
                ["D", "Disk full", "/var 98%"],
                ["E", "", ""]
            ])
        );

        // The pattern must match the whole value: "abc" alone matching is no match.
        let reply = run_query(
            &server,
            "source=logs | where severity_text = 'E' | parse body '(?<word>[a-z]+)' | fields word",
        )
        .await;
        assert_eq!(read_rows(&reply), &json!([[""]]));

        // A group named like an existing field replaces its values; an optional group that did not
        // take part in the match yields "".
        let reply = run_query(&server, "source=logs | where severity_text = 'D' | parse body '(?<severity_text>[A-Za-z]+) (?<unit>kB )?full: (?<path>[^ ]+) .*' | fields severity_text, unit, path",
        )
        .await;
        assert_eq!(read_rows(&reply), &json!([["Disk", "", "/var"]]));
        server.shutdown().await;
    }

    #[tokio::test]
    async fn parse_with_an_unsupported_pattern_or_a_non_string_source_is_a_400() {
        let (server, _catalog) = TestServer::start().await;
        for (query, named) in [
            ("source=logs | parse body '[a-z]+'", "[a-z]+"),
            ("source=logs | parse body '(?<a>x'", "(?<a>x"),
            ("source=logs | parse body '(?<a>x)(?=y)'", "(?<a>x)(?=y)"),
            ("source=logs | parse body '(?<=y)(?<a>x)'", "(?<=y)(?<a>x)"),
            ("source=logs | parse body '(?<a>x)\\k<a>'", "(?<a>x)\\k<a>"),
            ("source=logs | parse body '(?<a>x)\\1'", "(?<a>x)\\1"),
            // Unbalanced on its own; wrapped in the anchoring group it would escape both anchors.
            ("source=logs | parse body 'x)|(?<a>.*'", "x)|(?<a>.*"),
            // Balanced on its own; the trailing comment would swallow the anchoring group's end.
            ("source=logs | parse body '(?x)(?<a>.*) #'", "(?x)(?<a>.*) #"),
            ("source=logs | parse body '(?<__ppl_a>.*)'", "__ppl_a"),
            ("source=logs | parse timestamp '(?<a>.+)'", "timestamp"),
        ] {
            assert_ppl_400(&run_query(&server, query).await, SEMANTIC_ERROR, named);
        }
        server.shutdown().await;
    }

    #[tokio::test]
    async fn fillnull_replaces_nulls_with_the_stated_values() {
        let (server, catalog) = TestServer::start().await;
        commit_log_rows(
            &catalog,
            &[
                build_log_row(TENANT, NOON_MICROS, Some("checkout"), Some("A"), None),
                build_log_row(TENANT, NOON_MICROS, None, Some("B"), Some("ok")),
                build_log_row(TENANT, NOON_MICROS, None, Some("C"), None),
                build_log_row(OTHER_TENANT, NOON_MICROS, None, Some("A"), None),
            ],
        )
        .await
        .expect("commit logs");
        seed_token_operations(&catalog, &[Some(3), None])
            .await
            .expect("seed token operations");

        for (query, expected) in [
            (
                "source=logs | fillnull with 'unknown' in service_name, body | fields severity_text, service_name, body | sort severity_text",
                json!([
                    ["A", "checkout", "unknown"],
                    ["B", "unknown", "ok"],
                    ["C", "unknown", "unknown"]
                ]),
            ),
            (
                "source=logs | fillnull using service_name = 'svc', body = 'none' | fields severity_text, service_name, body | sort severity_text",
                json!([["A", "checkout", "none"], ["B", "svc", "ok"], ["C", "svc", "none"]]),
            ),
            (
                "source=logs | fillnull value='-' body | fields severity_text, service_name, body | sort severity_text",
                json!([["A", "checkout", "-"], ["B", null, "ok"], ["C", null, "-"]]),
            ),
            (
                "source=logs | fields severity_text, service_name, body | fillnull with '?' | sort severity_text",
                json!([["A", "checkout", "?"], ["B", "?", "ok"], ["C", "?", "?"]]),
            ),
        ] {
            assert_eq!(read_rows(&run_query(&server, query).await), &expected, "{query}");
        }

        assert_ppl_400(
            &run_query(&server, "source=logs | fillnull value=0 service_name").await,
            SEMANTIC_ERROR,
            "service_name",
        );
        assert_ppl_400(
            &run_query(&server, "source=logs | fillnull using body = 'a', body = 'b'").await,
            SEMANTIC_ERROR,
            "body",
        );
        // An id column renders as a hex string but accepts no replacement.
        assert_ppl_400(
            &run_query(&server, &format!("source=logs | fillnull with 'x' in {COL_TRACE_ID}")).await,
            SEMANTIC_ERROR,
            COL_TRACE_ID,
        );
        // Without a field list every column must accept the value; `logs` has timestamps and MAPs.
        assert_ppl_400(
            &run_query(&server, "source=logs | fillnull with 'x'").await,
            SEMANTIC_ERROR,
            "fillnull",
        );
        // A number fills a numeric column only when the column's type holds it exactly: the integer
        // column `input_tokens` takes 7 but refuses 1.5, which it would truncate.
        assert_eq!(
            read_rows(
                &run_query(
                    &server,
                    "source=operations | fillnull value=7 input_tokens | sort timestamp | fields input_tokens"
                )
                .await
            ),
            &json!([[3], [7]])
        );
        assert_ppl_400(
            &run_query(&server, "source=operations | fillnull value=1.5 input_tokens").await,
            SEMANTIC_ERROR,
            "field 'input_tokens' (type: bigint) cannot hold the replacement value 1.5",
        );
        let missing_field_reply = run_query(&server, "source=logs | fillnull with 'x' in no_such_field").await;
        assert_ppl_error(
            &missing_field_reply,
            StatusCode::BAD_REQUEST,
            "SemanticCheckException",
            "FIELD_NOT_FOUND",
        );
        assert_eq!(missing_field_reply.body["error"]["context"]["field"], "no_such_field");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn fillnull_fills_numeric_boolean_and_timestamp_columns_with_values_they_hold() {
        let (server, catalog) = TestServer::start().await;
        // `set` has every filled column set, `unset` none of them. `timestamp` is a required
        // column, so the queries derive a nullable `seen` from it: NULL for `unset`, at 12:00:01.
        let operation_rows = [
            Row {
                tenant: TENANT,
                cells: vec![
                    (OPERATION_NAME, Cell::Text("set")),
                    (COL_TIMESTAMP, Cell::Micros(NOON_MICROS)),
                    ("input_tokens", Cell::Long(3)),
                    (TEMPERATURE, Cell::Double(0.5)),
                    (STREAM, Cell::Bool(true)),
                ],
            },
            Row {
                tenant: TENANT,
                cells: vec![
                    (OPERATION_NAME, Cell::Text("unset")),
                    (COL_TIMESTAMP, Cell::Micros(NOON_MICROS + 1_000_000)),
                ],
            },
            Row {
                tenant: OTHER_TENANT,
                cells: vec![
                    (OPERATION_NAME, Cell::Text("other-tenant")),
                    (COL_TIMESTAMP, Cell::Micros(NOON_MICROS)),
                ],
            },
        ];
        commit_rows(&catalog, OPERATIONS_TABLE, &operation_rows)
            .await
            .expect("commit operations");
        // One span; its `status_code` (an `int` column) is NULL.
        let span_rows = [Row {
            tenant: TENANT,
            cells: vec![(COL_TIMESTAMP, Cell::Micros(NOON_MICROS))],
        }];
        commit_rows(&catalog, SPANS_TABLE, &span_rows).await.expect("commit spans");

        // The double column takes the integer 2; the timestamp column takes a timestamp.
        let filled_reply = run_query(
            &server,
            "source=operations | eval seen = nullif(timestamp, timestamp('2026-10-08 12:00:01')) \
             | fillnull using input_tokens = 7, temperature = 2, stream = false, \
             seen = timestamp('2026-10-09 00:00:00') | sort operation_name \
             | fields operation_name, input_tokens, temperature, stream, seen",
        )
        .await;
        assert_eq!(
            read_rows(&filled_reply),
            &json!([
                ["set", 3, 0.5, true, "2026-10-08 12:00:00"],
                ["unset", 7, 2.0, false, "2026-10-09 00:00:00"]
            ])
        );
        // The largest 64-bit and 32-bit integers fill a `bigint` and an `int` column.
        let bigint_reply = run_query(
            &server,
            "source=operations | fillnull value=9223372036854775807 input_tokens | sort operation_name \
             | fields input_tokens",
        )
        .await;
        assert_eq!(read_rows(&bigint_reply), &json!([[3], [9_223_372_036_854_775_807_i64]]));
        let int_reply = run_query(
            &server,
            "source=spans | fillnull value=2147483647 status_code | fields status_code",
        )
        .await;
        assert_eq!(read_rows(&int_reply), &json!([[2_147_483_647]]));

        // An `int` column refuses a fraction and a literal one above its range, naming both.
        for (query, named) in [
            (
                "source=spans | fillnull value=1.5 status_code",
                "field 'status_code' (type: int) cannot hold the replacement value 1.5",
            ),
            (
                "source=spans | fillnull value=2147483648 status_code",
                "field 'status_code' (type: int) cannot hold the replacement value 2147483648",
            ),
        ] {
            assert_ppl_400(&run_query(&server, query).await, SEMANTIC_ERROR, named);
        }
        server.shutdown().await;
    }

    #[tokio::test]
    async fn parse_and_fillnull_keep_the_order_of_a_sort_on_a_column_they_rewrite() {
        let (server, catalog) = TestServer::start().await;
        commit_log_rows(
            &catalog,
            &[
                build_log_row(TENANT, NOON_MICROS, Some("b"), Some("from-b1"), Some("b1")),
                build_log_row(TENANT, NOON_MICROS, Some("a"), Some("from-a2"), Some("a2")),
                build_log_row(TENANT, NOON_MICROS, None, Some("from-null"), Some("c3")),
                build_log_row(OTHER_TENANT, NOON_MICROS, None, Some("other-tenant"), Some("a0")),
            ],
        )
        .await
        .expect("commit logs");

        // `dedup` keeps the first row of the incoming order. Sorted by body the order is a2, b1, c3;
        // sorted by the parsed body ("2", "1", "3") b1 would come first.
        let reply = run_query(&server, "source=logs | sort body | parse body '.(?<body>.)' | eval group = 1 | dedup 1 group | fields severity_text, body",
        )
        .await;
        assert_eq!(read_rows(&reply), &json!([["from-a2", "2"]]));

        // Sorted by service_name NULL comes first; sorted by the filled value ("z") it would come
        // last, after "a".
        let reply = run_query(&server, "source=logs | sort service_name | fillnull with 'z' in service_name | eval group = 1 | dedup 1 group | fields severity_text, service_name",
        )
        .await;
        assert_eq!(read_rows(&reply), &json!([["from-null", "z"]]));
        server.shutdown().await;
    }

    #[tokio::test]
    async fn describe_lists_the_jdbc_column_rows_in_schema_order_with_flat_map_keys() {
        let (server, catalog) = TestServer::start().await;
        let recent_micros = compute_recent_micros();
        commit_log_rows(
            &catalog,
            &[
                LogRow {
                    tenant_id: TENANT,
                    timestamp_micros: recent_micros,
                    service_name: Some("checkout"),
                    severity_text: Some("INFO"),
                    body: Some("payment accepted"),
                    resource_attributes: &[],
                    log_attributes: &[("Zone", "eu"), ("http", "1.1"), ("http.method", "GET")],
                },
                LogRow {
                    tenant_id: OTHER_TENANT,
                    timestamp_micros: recent_micros,
                    service_name: Some("checkout"),
                    severity_text: Some("INFO"),
                    body: Some("other tenant"),
                    resource_attributes: &[],
                    log_attributes: &[("b_only", "x")],
                },
            ],
        )
        .await
        .expect("commit logs");

        let reply = run_query(&server, &format!("describe {LOGS_TABLE}")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        let body = &reply.body;
        let expected_schema: Vec<Value> = [
            ("TABLE_CAT", "string"),
            ("TABLE_SCHEM", "string"),
            ("TABLE_NAME", "string"),
            ("COLUMN_NAME", "string"),
            ("DATA_TYPE", "int"),
            ("TYPE_NAME", "string"),
            ("COLUMN_SIZE", "int"),
            ("BUFFER_LENGTH", "int"),
            ("DECIMAL_DIGITS", "int"),
            ("NUM_PREC_RADIX", "int"),
            ("NULLABLE", "int"),
            ("REMARKS", "string"),
            ("COLUMN_DEF", "string"),
            ("SQL_DATA_TYPE", "int"),
            ("SQL_DATETIME_SUB", "int"),
            ("CHAR_OCTET_LENGTH", "int"),
            ("ORDINAL_POSITION", "int"),
            ("IS_NULLABLE", "string"),
            ("SCOPE_CATALOG", "string"),
            ("SCOPE_SCHEMA", "string"),
            ("SCOPE_TABLE", "string"),
            ("SOURCE_DATA_TYPE", "int"),
            ("IS_AUTOINCREMENT", "string"),
            ("IS_GENERATEDCOLUMN", "string"),
        ]
        .iter()
        .map(|(name, column_type)| json!({ "name": name, "type": column_type }))
        .collect();
        assert_eq!(body["schema"], Value::Array(expected_schema));

        // Field-model order: logs schema columns without tenant_id, each MAP followed by its keys in
        // byte order ("Zone" < "http" < "http.method"); tenant B's key is absent.
        let zone_key = format!("{COL_LOG_ATTRIBUTES}.Zone");
        let http_key = format!("{COL_LOG_ATTRIBUTES}.http");
        let http_method_key = format!("{COL_LOG_ATTRIBUTES}.http.method");
        let expected_fields: [(&str, &str); 14] = [
            (COL_SERVICE_NAME, "string"),
            (COL_TIMESTAMP, "timestamp"),
            (COL_OBSERVED_TIMESTAMP, "timestamp"),
            (COL_INGESTED_TIMESTAMP, "timestamp"),
            (COL_TRACE_ID, "string"),
            (COL_SPAN_ID, "string"),
            (COL_SEVERITY_TEXT, "string"),
            (COL_BODY, "string"),
            (COL_RESOURCE_ATTRIBUTES, "struct"),
            (COL_SCOPE_ATTRIBUTES, "struct"),
            (COL_LOG_ATTRIBUTES, "struct"),
            (zone_key.as_str(), "string"),
            (http_key.as_str(), "string"),
            (http_method_key.as_str(), "string"),
        ];
        let expected_rows: Vec<Value> = expected_fields
            .iter()
            .enumerate()
            .map(|(position, (field, type_name))| {
                json!([
                    "icegate", null, LOGS_TABLE, field, null, type_name, null, null, null, 10, 2, null, null, null,
                    null, null, position, "", null, null, null, null, "NO", ""
                ])
            })
            .collect();
        assert_eq!(body["datarows"], Value::Array(expected_rows));
        assert_eq!(body["total"], json!(expected_fields.len()));
        assert_eq!(body["size"], json!(expected_fields.len()));

        // Later commands read the JDBC column names.
        let reply = run_query(
            &server,
            &format!("describe {LOGS_TABLE} | where TYPE_NAME = 'struct' | sort ORDINAL_POSITION | fields COLUMN_NAME"),
        )
        .await;
        assert_eq!(
            read_rows(&reply),
            &json!([[COL_RESOURCE_ATTRIBUTES], [COL_SCOPE_ATTRIBUTES], [COL_LOG_ATTRIBUTES]])
        );
        server.shutdown().await;
    }

    #[tokio::test]
    async fn describe_of_an_unknown_index_is_index_not_found_and_of_several_or_system_indices_a_400() {
        let (server, _catalog) = TestServer::start().await;
        let unknown_index_reply = run_query(&server, "describe nope").await;
        assert_ppl_error(
            &unknown_index_reply,
            StatusCode::NOT_FOUND,
            "IndexNotFoundException",
            "INDEX_NOT_FOUND",
        );
        assert!(
            read_error_text(&unknown_index_reply).contains("nope"),
            "{}",
            unknown_index_reply.body
        );

        let several_indices_reply = run_query(&server, &format!("describe {LOGS_TABLE},{SPANS_TABLE}")).await;
        assert_ppl_400(&several_indices_reply, SEMANTIC_ERROR, SPANS_TABLE);

        let kibana_creation = server
            .request(reqwest::Method::PUT, Some(TENANT), "/.kibana")
            .header("content-type", "application/json")
            .body("{}")
            .send()
            .await
            .expect("create .kibana");
        assert_eq!(kibana_creation.status().as_u16(), 200);
        assert_ppl_400(&run_query(&server, "describe .kibana").await, SEMANTIC_ERROR, ".kibana");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn patterns_and_take_are_400s_naming_them() {
        let (server, _catalog) = TestServer::start().await;
        let patterns_reply = run_query(&server, "source=logs | patterns body").await;
        assert_ppl_400(&patterns_reply, UNSUPPORTED_OPERATION, "patterns");
        assert_eq!(patterns_reply.body["error"]["context"]["command"], "patterns");
        let take_reply = run_query(&server, "source=logs | stats take(body, 2)").await;
        assert_ppl_400(&take_reply, UNSUPPORTED_OPERATION, "take");
        assert_eq!(take_reply.body["error"]["context"]["function"], "take");
        server.shutdown().await;
    }

    #[tokio::test]
    async fn discover_style_timestamp_strings_fold_to_microsecond_literals() {
        let (server, catalog) = TestServer::start().await;
        commit_log_rows(
            &catalog,
            &[
                build_log_row(TENANT, NOON_MICROS - 1, Some("checkout"), Some("a"), None),
                build_log_row(TENANT, NOON_MICROS, Some("checkout"), Some("b"), None),
                build_log_row(TENANT, NOON_MICROS + 1, Some("checkout"), Some("c"), None),
                build_log_row(OTHER_TENANT, NOON_MICROS + 1, Some("checkout"), Some("z"), None),
            ],
        )
        .await
        .expect("commit logs");
        write_log_wal_segment(
            &server.wal_store,
            &catalog,
            COMMITTED_WAL_OFFSET + 1,
            &[
                build_log_row(TENANT, NOON_MICROS + 123_456, Some("checkout"), Some("d"), None),
                build_log_row(TENANT, NOON_MICROS + 1_000_000, Some("checkout"), Some("e"), None),
                build_log_row(OTHER_TENANT, NOON_MICROS + 123_456, Some("checkout"), Some("y"), None),
            ],
        )
        .await
        .expect("write WAL segment");

        for (query, expected) in [
            (
                // A Discover-style millisecond window: 12:00:00.000 through 12:00:00.999 inclusive.
                "source=logs | WHERE `timestamp` >= '2026-10-08 12:00:00.000' AND `timestamp` <= '2026-10-08 12:00:00.999' | fields severity_text | sort severity_text",
                json!([["b"], ["c"], ["d"]]),
            ),
            (
                "source=logs | where timestamp >= '2026-10-08 12:00:00.000001' and timestamp <= '2026-10-08 12:00:00.123456' | fields severity_text | sort severity_text",
                json!([["c"], ["d"]]),
            ),
            (
                "source=logs | where timestamp = timestamp('2026-10-08 12:00:00.123456') | fields severity_text",
                json!([["d"]]),
            ),
            (
                "source=logs | where timestamp < '2026-10-08 12:00:00' | fields severity_text",
                json!([["a"]]),
            ),
        ] {
            assert_eq!(read_rows(&run_query(&server, query).await), &expected, "{query}");
        }
        server.shutdown().await;
    }

    #[tokio::test]
    async fn more_than_six_fraction_digits_is_a_400() {
        let (server, _catalog) = TestServer::start().await;
        for query in [
            "source=logs | where timestamp >= '2026-10-08 12:00:00.1234567'",
            "source=logs | where timestamp >= timestamp('2026-10-08 12:00:00.1234567')",
            "source=logs | where timestamp between '2026-10-08 12:00:00.1234567' and '2026-10-09'",
            "source=logs | where timestamp in ('2026-10-08 12:00:00.1234567')",
        ] {
            assert_ppl_400(
                &run_query(&server, query).await,
                SEMANTIC_ERROR,
                "2026-10-08 12:00:00.1234567",
            );
        }
        let six_digit_reply = run_query(&server, "source=logs | where timestamp >= '2026-10-08 12:00:00.123456'").await;
        assert_eq!(six_digit_reply.status, StatusCode::OK, "{}", six_digit_reply.body);
        server.shutdown().await;
    }
}
