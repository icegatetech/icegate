//! Data-index `_search` over real HTTP. Every request is a literal, every row is written with
//! explicit values, and every expected result is stated here.
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use iceberg::spec::{NestedField, PrimitiveType, Type};
use icegate_common::schema::{
    COL_BODY, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES, COL_NAME, COL_OBSERVED_TIMESTAMP, COL_RESOURCE_ATTRIBUTES,
    COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID,
};
use icegate_common::{LOGS_TABLE, MemoryPressureConfig, MemoryPressureSampler, SPANS_TABLE, UsageReader};
use icegate_query::opensearch::{OpenSearchConfig, SearchLimitsConfig, SystemIndicesConfig};
use reqwest::Method;
use serde_json::{Value, json};

use super::data::{Cell, Row, commit_rows, create_exact_name_table, required_string, write_wal_segment};
use super::harness::{ServerOptions, TestServer};
use super::test_rows::COMMITTED_WAL_OFFSET;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";
/// 2025-10-09T08:53:20Z.
const T0: i64 = 1_760_000_000_000_000;
const TRACE_ID: [u8; 16] = [
    0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
];
const SPAN_ID: [u8; 8] = [0xaa, 0xbb, 0xcc, 0xdd, 0x00, 0x11, 0x22, 0x33];

/// Send `body` to `path` as `tenant` and read the status and the JSON answer.
async fn post_json(server: &TestServer, tenant: Option<&str>, path: &str, body: &Value) -> (u16, Value) {
    let response = server
        .request(Method::POST, tenant, path)
        .json(body)
        .send()
        .await
        .expect("send");
    let status = response.status().as_u16();
    (status, response.json().await.expect("JSON answer"))
}

/// The `_source.<column>` values of the hits, in hit order.
fn read_source_values(answer: &Value, column: &str) -> Vec<Value> {
    answer["hits"]["hits"]
        .as_array()
        .expect("hits array")
        .iter()
        .map(|hit| hit["_source"][column].clone())
        .collect()
}

/// The same values, sorted, for hit lists whose order is not a contract.
fn read_sorted_source_texts(answer: &Value, column: &str) -> Vec<String> {
    let mut texts: Vec<String> = read_source_values(answer, column)
        .into_iter()
        .map(|value| value.as_str().expect("a text value").to_string())
        .collect();
    texts.sort();
    texts
}

fn build_log_row<'a>(tenant: &'a str, micros: i64, body: &'a str, severity: &'a str) -> Row<'a> {
    Row {
        tenant,
        cells: vec![
            (COL_TIMESTAMP, Cell::Micros(micros)),
            (COL_OBSERVED_TIMESTAMP, Cell::Micros(micros)),
            (COL_INGESTED_TIMESTAMP, Cell::Micros(micros)),
            (COL_BODY, Cell::Text(body)),
            (COL_SEVERITY_TEXT, Cell::Text(severity)),
        ],
    }
}

#[tokio::test]
async fn search_returns_the_matching_rows_with_index_and_source() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            Row {
                tenant: TENANT_A,
                cells: vec![
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                    (COL_TIMESTAMP, Cell::Micros(1_760_000_000_123_456)),
                    (COL_OBSERVED_TIMESTAMP, Cell::Micros(1_760_000_000_123_456)),
                    (COL_INGESTED_TIMESTAMP, Cell::Micros(1_760_000_001_000_000)),
                    (COL_TRACE_ID, Cell::Bytes(&TRACE_ID)),
                    (COL_SEVERITY_TEXT, Cell::Text("ERROR")),
                    (COL_BODY, Cell::Text("payment declined")),
                    (COL_LOG_ATTRIBUTES, Cell::Map(&[("http.method", "GET"), ("http", "x")])),
                ],
            },
            build_log_row(TENANT_A, T0, "all good", "INFO"),
            build_log_row(TENANT_B, T0, "tenant b error", "ERROR"),
        ],
    )
    .await
    .expect("commit");

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"term": {"severity_text": "ERROR"}}}),
    )
    .await;

    assert_eq!(status, 200, "{answer}");
    assert!(answer["took"].is_u64(), "{answer}");
    assert_eq!(answer["timed_out"], false);
    assert_eq!(
        answer["_shards"],
        json!({"total": 1, "successful": 1, "skipped": 0, "failed": 0})
    );
    assert_eq!(answer["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    assert_eq!(answer["hits"]["max_score"], json!(1.0));
    let hits = answer["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["_index"], "logs");
    assert_eq!(hits[0]["_score"], json!(1.0));
    assert_eq!(
        hits[0]["_source"],
        json!({
            COL_SERVICE_NAME: "checkout",
            COL_TIMESTAMP: "2025-10-09T08:53:20.123456Z",
            COL_OBSERVED_TIMESTAMP: "2025-10-09T08:53:20.123456Z",
            COL_INGESTED_TIMESTAMP: "2025-10-09T08:53:21.000000Z",
            COL_TRACE_ID: "0123456789abcdeffedcba9876543210",
            COL_SEVERITY_TEXT: "ERROR",
            COL_BODY: "payment declined",
            COL_RESOURCE_ATTRIBUTES: {},
            COL_SCOPE_ATTRIBUTES: {},
            COL_LOG_ATTRIBUTES: {"http.method": "GET", "http": "x"}
        })
    );
    server.shutdown().await;
}

/// Rows of tenant A and tenant B in one append: tenant A's two rows are counted either way.
async fn start_server_with_two_tenant_a_rows() -> TestServer {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0, "a-one", "INFO"),
            build_log_row(TENANT_B, T0, "b-one", "INFO"),
            build_log_row(TENANT_A, T0 + 1, "a-two", "INFO"),
        ],
    )
    .await
    .expect("commit");
    server
}

#[tokio::test]
async fn rest_total_hits_as_int_renders_the_total_as_an_integer() {
    let server = start_server_with_two_tenant_a_rows().await;

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search?rest_total_hits_as_int=true",
        &json!({}),
    )
    .await;

    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["hits"]["total"], json!(2));
    assert_eq!(read_sorted_source_texts(&answer, COL_BODY), vec!["a-one", "a-two"]);
    server.shutdown().await;
}

#[tokio::test]
async fn a_search_of_size_zero_counts_the_matches_and_returns_no_hits() {
    let server = start_server_with_two_tenant_a_rows().await;

    let (status, answer) = post_json(&server, Some(TENANT_A), "/logs/_search", &json!({"size": 0})).await;

    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    assert_eq!(answer["hits"]["max_score"], Value::Null);
    assert_eq!(answer["hits"]["hits"], json!([]));
    server.shutdown().await;
}

/// A data answer is bounded by `max_answer_bytes` like a system answer: a hit whose `_source`
/// alone passes the bound refuses the whole answer, while the count of the same rows fits.
#[tokio::test]
async fn a_data_search_answer_past_max_answer_bytes_is_refused_whole() {
    let system_indices = SystemIndicesConfig {
        max_snapshot_bytes: 1024,
        max_answer_bytes: 2048,
        ..SystemIndicesConfig::default()
    };
    system_indices.validate().expect("valid answer bound");
    let (server, catalog) = TestServer::start_with_config(OpenSearchConfig {
        system_indices,
        ..OpenSearchConfig::default()
    })
    .await;
    let long_body = "x".repeat(4096);
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0, &long_body, "INFO"),
            build_log_row(TENANT_B, T0, "b-short", "INFO"),
        ],
    )
    .await
    .expect("commit");

    let (status, refused_answer) = post_json(&server, Some(TENANT_A), "/logs/_search", &json!({})).await;
    assert_eq!(status, 400, "{refused_answer}");
    assert_eq!(refused_answer["error"]["type"], "illegal_argument_exception");

    let (status, count_answer) = post_json(&server, Some(TENANT_A), "/logs/_search", &json!({"size": 0})).await;
    assert_eq!(status, 200, "{count_answer}");
    assert_eq!(count_answer["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    server.shutdown().await;
}

#[tokio::test]
async fn an_index_name_in_another_case_is_a_404_on_search() {
    let (server, _catalog) = TestServer::start().await;

    let (status, answer) = post_json(&server, Some(TENANT_A), "/Logs/_search", &json!({})).await;

    assert_eq!(status, 404, "{answer}");
    assert_eq!(answer["error"]["type"], "index_not_found_exception");
    server.shutdown().await;
}

#[tokio::test]
async fn a_search_over_two_indices_unions_their_rows_and_a_missing_field_matches_nothing_there() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[Row {
            tenant: TENANT_A,
            cells: vec![
                (COL_TIMESTAMP, Cell::Micros(T0)),
                (COL_SERVICE_NAME, Cell::Text("checkout")),
                (COL_BODY, Cell::Text("log-row")),
            ],
        }],
    )
    .await
    .expect("commit logs");
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[Row {
            tenant: TENANT_A,
            cells: vec![
                (COL_TIMESTAMP, Cell::Micros(T0)),
                (COL_TRACE_ID, Cell::Bytes(&TRACE_ID)),
                (COL_SPAN_ID, Cell::Bytes(&SPAN_ID)),
                (COL_SERVICE_NAME, Cell::Text("checkout")),
                (COL_NAME, Cell::Text("span-row")),
            ],
        }],
    )
    .await
    .expect("commit spans");

    // The pages merge in index-name order, and `from` skips across the boundary between them.
    for (body, expected_indices) in [
        (
            json!({"query": {"term": {"service_name": "checkout"}}}),
            vec!["logs", "spans"],
        ),
        (
            json!({"query": {"term": {"service_name": "checkout"}}, "from": 1, "size": 1}),
            vec!["spans"],
        ),
    ] {
        let (status, both_indices_answer) = post_json(&server, Some(TENANT_A), "/logs,spans/_search", &body).await;
        assert_eq!(status, 200, "{body}: {both_indices_answer}");
        assert_eq!(both_indices_answer["hits"]["total"]["value"], 2, "{body}");
        let indices: Vec<&str> = both_indices_answer["hits"]["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| hit["_index"].as_str().expect("index"))
            .collect();
        assert_eq!(indices, expected_indices, "{body}");
    }

    let (status, spans_only) = post_json(
        &server,
        Some(TENANT_A),
        "/logs,spans/_search",
        &json!({"query": {"term": {"name": "span-row"}}}),
    )
    .await;
    assert_eq!(status, 200, "{spans_only}");
    assert_eq!(spans_only["hits"]["total"]["value"], 1);
    assert_eq!(spans_only["hits"]["hits"][0]["_index"], "spans");

    let (status, logs_only) = post_json(
        &server,
        Some(TENANT_A),
        "/logs,spans/_search",
        &json!({"query": {"exists": {"field": "body"}}}),
    )
    .await;
    assert_eq!(status, 200, "{logs_only}");
    assert_eq!(logs_only["hits"]["total"]["value"], 1);
    assert_eq!(logs_only["hits"]["hits"][0]["_index"], "logs");
    server.shutdown().await;
}

#[tokio::test]
async fn a_size_at_the_result_window_is_accepted_and_one_above_is_a_search_phase_400() {
    let config = OpenSearchConfig {
        search: SearchLimitsConfig { max_result_window: 3 },
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0, "one", "INFO"),
            build_log_row(TENANT_A, T0 + 1, "two", "INFO"),
            build_log_row(TENANT_A, T0 + 2, "three", "INFO"),
            build_log_row(TENANT_A, T0 + 3, "four", "INFO"),
        ],
    )
    .await
    .expect("commit");

    for (body, expected_hits) in [
        (json!({"size": 2}), 2_usize),
        (json!({"size": 3}), 3),
        (json!({"from": 1, "size": 2}), 2),
    ] {
        let (status, answer) = post_json(&server, Some(TENANT_A), "/logs/_search", &body).await;
        assert_eq!(status, 200, "{body}: {answer}");
        assert_eq!(
            answer["hits"]["hits"].as_array().expect("hits").len(),
            expected_hits,
            "{body}"
        );
        assert_eq!(answer["hits"]["total"]["value"], 4, "{body}");
    }
    for body in [json!({"size": 4}), json!({"from": 1, "size": 3})] {
        let (status, answer) = post_json(&server, Some(TENANT_A), "/logs/_search", &body).await;
        assert_eq!(status, 400, "{body}: {answer}");
        assert_eq!(answer["error"]["type"], "search_phase_execution_exception", "{body}");
        assert_eq!(
            answer["error"]["root_cause"][0]["type"], "illegal_argument_exception",
            "{body}"
        );
    }
    let (status, answer) = post_json(&server, Some(TENANT_A), "/logs/_search?size=4", &json!({})).await;
    assert_eq!(status, 400, "{answer}");
    assert_eq!(answer["error"]["type"], "search_phase_execution_exception");
    server.shutdown().await;
}

/// The result window bounds a system-index search and a system scroll's page as it bounds a data
/// search: the same search-phase 400, at the same boundary.
#[tokio::test]
async fn a_system_search_or_scroll_past_the_result_window_is_the_same_search_phase_400() {
    let config = OpenSearchConfig {
        search: SearchLimitsConfig { max_result_window: 3 },
        ..OpenSearchConfig::default()
    };
    let (server, _catalog) = TestServer::start_with_config(config).await;
    let response = server
        .request(Method::PUT, Some(TENANT_A), "/.kibana_1")
        .json(&json!({"mappings": {"properties": {"type": {"type": "keyword"}}}}))
        .send()
        .await
        .expect("send");
    assert_eq!(response.status().as_u16(), 200);
    let (status, created_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/.kibana_1/_doc/config:1?refresh=wait_for",
        &json!({"type": "config"}),
    )
    .await;
    assert_eq!(status, 201, "{created_answer}");

    for path in ["/.kibana_1/_search", "/.kibana_1/_search?scroll=1m"] {
        let (status, answer) = post_json(&server, Some(TENANT_A), path, &json!({"size": 3})).await;
        assert_eq!(status, 200, "{path}: {answer}");
        assert_eq!(answer["hits"]["hits"].as_array().expect("hits").len(), 1, "{path}");

        let (status, answer) = post_json(&server, Some(TENANT_A), path, &json!({"size": 4})).await;
        assert_eq!(status, 400, "{path}: {answer}");
        assert_eq!(answer["error"]["type"], "search_phase_execution_exception", "{path}");
        assert_eq!(
            answer["error"]["root_cause"][0]["type"], "illegal_argument_exception",
            "{path}"
        );
    }
    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/.kibana_1/_search",
        &json!({"from": 1, "size": 3}),
    )
    .await;
    assert_eq!(status, 400, "{answer}");
    assert_eq!(answer["error"]["type"], "search_phase_execution_exception");
    server.shutdown().await;
}

#[tokio::test]
async fn an_empty_wildcard_search_answers_zero_hits() {
    let (server, _catalog) = TestServer::start().await;

    let response = server
        .request(Method::GET, Some(TENANT_A), "/nomatch*/_search")
        .send()
        .await
        .expect("send");

    assert_eq!(response.status().as_u16(), 200);
    let answer: Value = response.json().await.expect("JSON");
    assert_eq!(answer["hits"]["total"], json!({"value": 0, "relation": "eq"}));
    assert_eq!(answer["hits"]["hits"], json!([]));
    assert_eq!(answer["hits"]["max_score"], Value::Null);
    server.shutdown().await;
}

#[tokio::test]
async fn an_unknown_concrete_index_is_404_unless_ignore_unavailable() {
    let (server, _catalog) = TestServer::start().await;

    let (status, missing_answer) = post_json(&server, Some(TENANT_A), "/nosuch/_search", &json!({})).await;
    assert_eq!(status, 404, "{missing_answer}");
    assert_eq!(missing_answer["error"]["type"], "index_not_found_exception");

    let (status, ignored_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/nosuch/_search?ignore_unavailable=true",
        &json!({}),
    )
    .await;
    assert_eq!(status, 200, "{ignored_answer}");
    assert_eq!(ignored_answer["hits"]["total"]["value"], 0);
    server.shutdown().await;
}

#[tokio::test]
async fn count_on_a_data_index_is_a_400() {
    let (server, _catalog) = TestServer::start().await;

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_count",
        &json!({"query": {"match_all": {}}}),
    )
    .await;

    assert_eq!(status, 400, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(
        answer["error"]["index"], "logs",
        "the refusal names the index: {answer}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn nested_on_a_data_index_is_a_parsing_exception() {
    let (server, _catalog) = TestServer::start().await;

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"nested": {"path": "references", "query": {"match_all": {}}}}}),
    )
    .await;

    assert_eq!(status, 400, "{answer}");
    assert_eq!(answer["error"]["type"], "parsing_exception");
    server.shutdown().await;
}

#[tokio::test]
async fn seq_no_primary_term_on_a_data_index_is_a_400() {
    let (server, _catalog) = TestServer::start().await;

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"seq_no_primary_term": true}),
    )
    .await;

    assert_eq!(status, 400, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

/// A [`UsageReader`] above the default high watermark, so the shedding layer refuses.
struct NearlyFullMemory;

impl UsageReader for NearlyFullMemory {
    fn limit_bytes(&self) -> u64 {
        100
    }

    fn read_working_set_bytes(&self) -> icegate_common::Result<u64> {
        Ok(95)
    }
}

#[tokio::test]
async fn middleware_refusals_on_the_search_route_answer_in_the_envelope() {
    let body = json!({"query": {"match_all": {}}});

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
        .request(Method::POST, Some(TENANT_A), "/logs/_search")
        .json(&body)
        .send()
        .await
        .expect("send");
    assert_eq!(response.status().as_u16(), 503);
    assert!(response.headers().contains_key("retry-after"));
    let shed_answer: Value = response.json().await.expect("JSON");
    assert_eq!(shed_answer["status"], 503);
    assert_eq!(
        shed_answer["error"]["root_cause"][0]["type"], "memory_pressure_exception",
        "{shed_answer}"
    );
    shedding_server.shutdown().await;

    // Deadline: a catalog that stops answering after startup is cut off in the same 503 envelope.
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
    let (status, deadline_answer) = post_json(&stalled_server, Some(TENANT_A), "/logs/_search", &body).await;
    assert_eq!(status, 503, "{deadline_answer}");
    assert_eq!(
        deadline_answer["error"]["root_cause"][0]["type"], "query_deadline_exceeded_exception",
        "{deadline_answer}"
    );
    stalled_server.shutdown().await;

    // A body one byte over max_request_body_bytes is answered 413 in the envelope.
    let (limited_server, _catalog) = TestServer::start_with_config(OpenSearchConfig {
        max_request_body_bytes: 64,
        ..OpenSearchConfig::default()
    })
    .await;
    let oversized_body = format!("{{\"query\":{{\"match_all\":{{}}}},\"size\":1{}}}", " ".repeat(64 - 34));
    assert_eq!(oversized_body.len(), 65);
    let response = limited_server
        .request(Method::POST, Some(TENANT_A), "/logs/_search")
        .header("content-type", "application/json")
        .body(oversized_body)
        .send()
        .await
        .expect("send");
    assert_eq!(response.status().as_u16(), 413);
    let too_large_answer: Value = response.json().await.expect("JSON");
    assert_eq!(too_large_answer["status"], 413);
    assert_eq!(
        too_large_answer["error"]["type"], "illegal_argument_exception",
        "{too_large_answer}"
    );

    // No tenant under the multi policy: refused in the 400 envelope.
    let (status, refused_answer) = post_json(&limited_server, None, "/logs/_search", &body).await;
    assert_eq!(status, 400, "{refused_answer}");
    assert_eq!(refused_answer["error"]["type"], "illegal_argument_exception");
    limited_server.shutdown().await;
}

#[tokio::test]
async fn search_never_returns_another_tenants_rows_from_one_wal_segment_or_one_parquet_file() {
    let (server, catalog) = TestServer::start().await;
    // One append holding both tenants (the writer splits it by the tenant partition) and one
    // WAL segment holding both tenants' rows interleaved in a single Parquet file. Tenant B's
    // rows carry a severity no tenant A row has, so any leak shows up in a filtered search too.
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0, "a-iceberg", "ERROR"),
            build_log_row(TENANT_B, T0, "b-iceberg", "WARN"),
        ],
    )
    .await
    .expect("commit");
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &[
            build_log_row(TENANT_B, T0 + 1, "b-wal", "WARN"),
            build_log_row(TENANT_A, T0 + 1, "a-wal", "ERROR"),
            build_log_row(TENANT_B, T0 + 2, "b-wal-2", "WARN"),
        ],
    )
    .await
    .expect("segment");

    let (status, tenant_a) = post_json(&server, Some(TENANT_A), "/logs/_search", &json!({})).await;
    assert_eq!(status, 200, "{tenant_a}");
    assert_eq!(
        read_sorted_source_texts(&tenant_a, COL_BODY),
        vec!["a-iceberg", "a-wal"]
    );
    assert_eq!(tenant_a["hits"]["total"]["value"], 2);

    let (status, tenant_b) = post_json(&server, Some(TENANT_B), "/logs/_search", &json!({})).await;
    assert_eq!(status, 200, "{tenant_b}");
    assert_eq!(
        read_sorted_source_texts(&tenant_b, COL_BODY),
        vec!["b-iceberg", "b-wal", "b-wal-2"]
    );
    assert_eq!(tenant_b["hits"]["total"]["value"], 3);

    let (status, probe_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"term": {"severity_text": "WARN"}}}),
    )
    .await;
    assert_eq!(status, 200, "{probe_answer}");
    assert_eq!(
        probe_answer["hits"]["total"]["value"], 0,
        "tenant B's rows never match for tenant A: {probe_answer}"
    );
    assert_eq!(probe_answer["hits"]["hits"], json!([]));
    server.shutdown().await;
}

#[tokio::test]
async fn a_search_on_an_exact_name_table_without_a_timestamp_column_is_a_400_naming_it() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(
        &catalog,
        "marks",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::optional(2, "mark_id", Type::Primitive(PrimitiveType::String)),
            NestedField::optional(3, "score", Type::Primitive(PrimitiveType::Long)),
        ],
    )
    .await
    .expect("create marks");

    let (status, answer) = post_json(&server, Some(TENANT_A), "/marks/_search", &json!({})).await;

    assert_eq!(status, 400, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(
        answer["error"]["index"], "marks",
        "the refusal names the index: {answer}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_search_on_an_exact_name_table_with_a_timestamp_column_returns_its_rows() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(
        &catalog,
        "scans",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, "scan_id", Type::Primitive(PrimitiveType::String)),
        ],
    )
    .await
    .expect("create scans");
    commit_rows(
        &catalog,
        "scans",
        &[
            Row {
                tenant: TENANT_A,
                cells: vec![(COL_TIMESTAMP, Cell::Micros(T0)), ("scan_id", Cell::Text("scan-a"))],
            },
            Row {
                tenant: TENANT_B,
                cells: vec![(COL_TIMESTAMP, Cell::Micros(T0)), ("scan_id", Cell::Text("scan-b"))],
            },
        ],
    )
    .await
    .expect("commit scans");

    let (status, answer) = post_json(&server, Some(TENANT_A), "/scans/_search", &json!({})).await;

    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["hits"]["total"]["value"], 1);
    assert_eq!(answer["hits"]["hits"][0]["_index"], "scans");
    assert_eq!(
        answer["hits"]["hits"][0]["_source"],
        json!({COL_TIMESTAMP: "2025-10-09T08:53:20.000000Z", "scan_id": "scan-a"})
    );
    server.shutdown().await;
}
