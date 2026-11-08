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
    Cell, LogRow, OperationRow, PriceRow, Row, commit_log_rows, commit_operation_rows, commit_price_rows, commit_rows,
    create_exact_name_table, required_string,
};
use super::harness::{ServerOptions, TestServer};
use super::search::NearlyFullMemory;
use super::test_rows::TEMPERATURE;

const TENANT: &str = "acme";

const OTHER_TENANT: &str = "globex";

const PPL_PATH: &str = "/_plugins/_ppl";

const TEXT_PLAIN: &str = "text/plain; charset=UTF-8";

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

async fn read_answer(response: reqwest::Response) -> PplAnswer {
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let text = response.text().await.expect("response body");
    let body = serde_json::from_str(&text).expect("JSON body");
    PplAnswer {
        status,
        content_type,
        body,
    }
}

async fn post_ppl(server: &TestServer, tenant: &str, path_and_query: &str, body: &Value) -> PplAnswer {
    let response = server
        .request(Method::POST, Some(tenant), path_and_query)
        .json(body)
        .send()
        .await
        .expect("PPL request");
    read_answer(response).await
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

/// A PPL error envelope with `status`, `type` and `code`, as `text/plain`, with a non-empty
/// `reason` and `details`.
fn assert_ppl_error(answer: &PplAnswer, status: StatusCode, error_type: &str, code: &str) {
    assert_eq!(answer.status, status, "answer {}", answer.body);
    assert_eq!(answer.content_type, TEXT_PLAIN);
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
async fn a_syntax_error_answers_invalid_query_with_details_naming_the_token_as_text_plain() {
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
    let answer = read_answer(response).await;
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
    let shed_answer = read_answer(response).await;
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
    let tenant_refusal = read_answer(response).await;
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
