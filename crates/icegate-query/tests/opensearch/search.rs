//! Data-index `_search` over real HTTP. Every request is a literal, every row is written with
//! explicit values, and every expected result is stated here.
#![allow(clippy::expect_used)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::array::{
    ArrayRef, Date32Array, Decimal128Array, Float32Array, Int32Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::DataType;
use iceberg::spec::{NestedField, PrimitiveType, Type};
use icegate_common::schema::{
    COL_BODY, COL_DURATION_MICROS, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES, COL_NAME, COL_OBSERVED_TIMESTAMP,
    COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TENANT_ID,
    COL_TIMESTAMP, COL_TRACE_ID,
};
use icegate_common::{
    LOGS_TABLE, MemoryPressureConfig, MemoryPressureSampler, OPERATIONS_TABLE, SPANS_TABLE, UsageReader,
};
use icegate_query::opensearch::{OpenSearchConfig, SearchLimitsConfig, SystemIndicesConfig};
use reqwest::Method;
use serde_json::{Map, Value, json};

use super::answers::read_sorted_source_texts;
use super::data::{Cell, Row, RowWriteError, commit_rows, create_exact_name_table, required_string, write_wal_segment};
use super::harness::{ServerOptions, TestServer};
use super::test_rows::{
    COMMITTED_WAL_OFFSET, OPERATION_NAME, STREAM, TEMPERATURE, TOP_K, build_operation_row, commit_batch,
    read_arrow_schema,
};

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
    assert_eq!(
        read_sorted_source_texts(&answer, COL_BODY).expect("sorted source texts"),
        vec!["a-one", "a-two"]
    );
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
        search: SearchLimitsConfig {
            max_result_window: 3,
            ..SearchLimitsConfig::default()
        },
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
        search: SearchLimitsConfig {
            max_result_window: 3,
            ..SearchLimitsConfig::default()
        },
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
pub(super) struct NearlyFullMemory;

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
        read_sorted_source_texts(&tenant_a, COL_BODY).expect("sorted source texts"),
        vec!["a-iceberg", "a-wal"]
    );
    assert_eq!(tenant_a["hits"]["total"]["value"], 2);

    let (status, tenant_b) = post_json(&server, Some(TENANT_B), "/logs/_search", &json!({})).await;
    assert_eq!(status, 200, "{tenant_b}");
    assert_eq!(
        read_sorted_source_texts(&tenant_b, COL_BODY).expect("sorted source texts"),
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

/// The exact-name table of the decimal case, its decimal column and its label column.
const RATES_TABLE: &str = "rates";
const RATE_COLUMN: &str = "rate";
const RATE_LABEL_COLUMN: &str = "rate_label";

/// A `rates` row of `tenant` labelled `label`, its rate given unscaled at scale 10.
fn build_rate_row<'a>(tenant: &'a str, label: &'a str, unscaled_rate: i128) -> Row<'a> {
    Row {
        tenant,
        cells: vec![
            (COL_TIMESTAMP, Cell::Micros(T0)),
            (RATE_COLUMN, Cell::Decimal(unscaled_rate)),
            (RATE_LABEL_COLUMN, Cell::Text(label)),
        ],
    }
}

#[tokio::test]
async fn numeric_boolean_and_date_terms_filter_rows_over_http() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        OPERATIONS_TABLE,
        &[
            build_operation_row(
                TENANT_A,
                "a-warm",
                T0,
                vec![
                    (TEMPERATURE, Cell::Double(0.7)),
                    (STREAM, Cell::Bool(true)),
                    (TOP_K, Cell::Long(40)),
                ],
            ),
            build_operation_row(
                TENANT_A,
                "a-cold",
                T0,
                vec![(TEMPERATURE, Cell::Double(0.0)), (STREAM, Cell::Bool(false))],
            ),
            build_operation_row(TENANT_A, "a-unset", T0, vec![]),
            build_operation_row(
                TENANT_B,
                "b-warm",
                T0,
                vec![
                    (TEMPERATURE, Cell::Double(0.7)),
                    (STREAM, Cell::Bool(true)),
                    (TOP_K, Cell::Long(40)),
                ],
            ),
        ],
    )
    .await
    .expect("commit operations");
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0, "a-on-the-day", "INFO"),
            build_log_row(TENANT_A, T0 + 86_400_000_000, "a-next-day", "INFO"),
            build_log_row(TENANT_B, T0, "b-on-the-day", "INFO"),
        ],
    )
    .await
    .expect("commit logs");
    create_exact_name_table(
        &catalog,
        RATES_TABLE,
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(
                3,
                RATE_COLUMN,
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 38,
                    scale: 10,
                }),
            ),
            NestedField::optional(4, RATE_LABEL_COLUMN, Type::Primitive(PrimitiveType::String)),
        ],
    )
    .await
    .expect("create rates");
    commit_rows(
        &catalog,
        RATES_TABLE,
        &[
            build_rate_row(TENANT_A, "a-zero", 0),
            build_rate_row(TENANT_A, "a-one", 10_000_000_000),
            build_rate_row(TENANT_B, "b-one", 10_000_000_000),
        ],
    )
    .await
    .expect("commit rates");

    let operation_cases = [
        (json!({"term": {"temperature": 0.7}}), vec!["a-warm"]),
        (json!({"range": {"temperature": {"gt": 0}}}), vec!["a-warm"]),
        (json!({"term": {"stream": true}}), vec!["a-warm"]),
        (
            json!({"bool": {"must_not": [{"term": {"stream": true}}]}}),
            vec!["a-cold", "a-unset"],
        ),
        (json!({"term": {"top_k": "4e1"}}), vec!["a-warm"]),
    ];
    for (query, expected) in operation_cases {
        let (status, answer) =
            post_json(&server, Some(TENANT_A), "/operations/_search", &json!({"query": query})).await;
        assert_eq!(status, 200, "{query}: {answer}");
        assert_eq!(
            read_sorted_source_texts(&answer, OPERATION_NAME).expect("sorted source texts"),
            expected,
            "{query}"
        );
    }

    let (status, day) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"term": {"timestamp": "2025-10-09"}}}),
    )
    .await;
    assert_eq!(status, 200, "{day}");
    assert_eq!(
        read_sorted_source_texts(&day, COL_BODY).expect("sorted source texts"),
        vec!["a-on-the-day"]
    );

    // The smallest positive double rounds to 0 at the decimal's scale; compared as a double,
    // `gt 0` still excludes the zero rate.
    let (status, rates) = post_json(
        &server,
        Some(TENANT_A),
        "/rates/_search",
        &json!({"query": {"range": {"rate": {"gt": 0}}}}),
    )
    .await;
    assert_eq!(status, 200, "{rates}");
    assert_eq!(
        read_sorted_source_texts(&rates, RATE_LABEL_COLUMN).expect("sorted source texts"),
        vec!["a-one"]
    );

    let (status, refusal_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/operations/_search",
        &json!({"query": {"term": {"stream": "yes"}}}),
    )
    .await;
    assert_eq!(status, 400, "{refusal_answer}");
    assert_eq!(refusal_answer["error"]["type"], "search_phase_execution_exception");
    assert_eq!(
        refusal_answer["error"]["root_cause"][0]["type"],
        "query_shard_exception"
    );
    server.shutdown().await;
}

/// The `_source.<label_column>`, `_id` and `sort` of every hit, in hit order.
fn read_hit_identities(answer: &Value, label_column: &str) -> Result<Vec<(String, String, Value)>, String> {
    answer["hits"]["hits"]
        .as_array()
        .ok_or_else(|| format!("the answer has no hits array: {answer}"))?
        .iter()
        .map(|hit| {
            let label = hit["_source"][label_column]
                .as_str()
                .ok_or_else(|| format!("a hit has no `{label_column}` text: {hit}"))?;
            let id = hit["_id"].as_str().ok_or_else(|| format!("a hit has no `_id`: {hit}"))?;
            Ok((label.to_string(), id.to_string(), hit["sort"].clone()))
        })
        .collect()
}

/// Rows of tenant A in one millisecond and one microsecond, across Iceberg and the WAL, and
/// tenant B rows at the same instants in the same files. Tenant A's bodies are `r0`…`r6`.
async fn seed_one_millisecond(server: &TestServer, catalog: &Arc<dyn iceberg::Catalog>) -> Result<(), RowWriteError> {
    commit_rows(
        catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0 - 1, "r0", "INFO"),
            build_log_row(TENANT_A, T0 + 100, "r1", "INFO"),
            build_log_row(TENANT_A, T0 + 100, "r2", "INFO"),
            build_log_row(TENANT_A, T0 + 999, "r5", "INFO"),
            build_log_row(TENANT_B, T0 + 100, "b1", "INFO"),
            build_log_row(TENANT_B, T0 + 250, "b4", "INFO"),
        ],
    )
    .await?;
    write_wal_segment(
        &server.wal_store,
        catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &[
            build_log_row(TENANT_B, T0 + 100, "b2", "INFO"),
            build_log_row(TENANT_A, T0 + 100, "r3", "INFO"),
            build_log_row(TENANT_A, T0 + 250, "r4", "INFO"),
            build_log_row(TENANT_A, T0 + 1_000, "r6", "INFO"),
            build_log_row(TENANT_B, T0 + 1_000, "b6", "INFO"),
        ],
    )
    .await
}

/// A `[timestamp, _doc]` sort in `order`, carrying the `unmapped_type` a context-style request
/// sends on both clauses.
fn build_time_doc_sort(order: &str) -> Value {
    json!([
        {"timestamp": {"order": order, "unmapped_type": "boolean"}},
        {"_doc": {"order": order, "unmapped_type": "boolean"}}
    ])
}

/// Every hit of tenant A at `search_path` in `[timestamp, _doc]` `order`, `page_size` per page,
/// following `search_after`; each labelled by its `_source.<label_column>`.
async fn page_through_hits(
    server: &TestServer,
    search_path: &str,
    label_column: &str,
    order: &str,
    page_size: usize,
) -> Result<Vec<(String, String, Value)>, String> {
    let mut seen_hits = Vec::new();
    let mut anchor: Option<Value> = None;
    for _ in 0..20 {
        let mut body = json!({"size": page_size, "sort": build_time_doc_sort(order)});
        if let Some(anchor_values) = &anchor {
            body["search_after"] = anchor_values.clone();
        }
        let (status, answer) = post_json(server, Some(TENANT_A), search_path, &body).await;
        if status != 200 {
            return Err(format!("a page answered {status}: {answer}"));
        }
        let page = read_hit_identities(&answer, label_column)?;
        let is_last_page = page.len() < page_size;
        anchor = page.last().map(|(_, _, sort)| sort.clone());
        seen_hits.extend(page);
        if is_last_page {
            return Ok(seen_hits);
        }
    }
    Err(format!("paging did not finish within 20 pages: {seen_hits:?}"))
}

#[tokio::test]
async fn several_rows_in_one_millisecond_and_one_microsecond_sort_by_time_then_doc_without_duplicates() {
    let (server, catalog) = TestServer::start().await;
    seed_one_millisecond(&server, &catalog).await.expect("seed");

    let ascending_hits = page_through_hits(&server, "/logs/_search", COL_BODY, "asc", 3)
        .await
        .expect("paging");
    let bodies: Vec<&str> = ascending_hits.iter().map(|(body, _, _)| body.as_str()).collect();
    assert_eq!(bodies.len(), 7, "{bodies:?}");
    assert_eq!(bodies[0], "r0");
    let mut same_microsecond: Vec<&str> = bodies[1..4].to_vec();
    same_microsecond.sort_unstable();
    assert_eq!(same_microsecond, vec!["r1", "r2", "r3"]);
    assert_eq!(&bodies[4..], &["r4", "r5", "r6"]);
    for window in ascending_hits.windows(2) {
        let (earlier_sort, later_sort) = (&window[0].2, &window[1].2);
        let earlier_pair = (
            earlier_sort[0].as_i64().expect("time"),
            earlier_sort[1].as_i64().expect("doc"),
        );
        let later_pair = (
            later_sort[0].as_i64().expect("time"),
            later_sort[1].as_i64().expect("doc"),
        );
        assert!(earlier_pair < later_pair, "{earlier_sort} then {later_sort}");
    }
    assert_eq!(
        ascending_hits[0].2[0],
        json!(1_759_999_999_999_i64),
        "the date sort value is floor(µs / 1000)"
    );
    assert_eq!(ascending_hits[1].2[0], json!(1_760_000_000_000_i64));

    let descending_hits = page_through_hits(&server, "/logs/_search", COL_BODY, "desc", 3)
        .await
        .expect("paging");
    let reversed_hits: Vec<(String, String, Value)> = ascending_hits.iter().rev().cloned().collect();
    assert_eq!(descending_hits, reversed_hits);
    server.shutdown().await;
}

#[tokio::test]
async fn a_mid_millisecond_anchor_pages_forward_and_backward_exactly() {
    let (server, catalog) = TestServer::start().await;
    seed_one_millisecond(&server, &catalog).await.expect("seed");
    let ascending_hits = page_through_hits(&server, "/logs/_search", COL_BODY, "asc", 3)
        .await
        .expect("paging");
    let anchor = ascending_hits
        .iter()
        .find(|(body, _, _)| body == "r4")
        .map(|(_, _, sort)| sort.clone())
        .expect("r4");

    let (status, forward_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"size": 10, "sort": build_time_doc_sort("asc"), "search_after": anchor}),
    )
    .await;
    assert_eq!(status, 200, "{forward_answer}");
    let forward_bodies: Vec<String> = read_hit_identities(&forward_answer, COL_BODY)
        .expect("hit identities")
        .into_iter()
        .map(|(body, _, _)| body)
        .collect();
    assert_eq!(forward_bodies, vec!["r5", "r6"]);

    let (status, backward_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"size": 10, "sort": build_time_doc_sort("desc"), "search_after": anchor}),
    )
    .await;
    assert_eq!(status, 200, "{backward_answer}");
    let backward_bodies: Vec<String> = read_hit_identities(&backward_answer, COL_BODY)
        .expect("hit identities")
        .into_iter()
        .map(|(body, _, _)| body)
        .collect();
    assert_eq!(backward_bodies.len(), 4, "{backward_bodies:?}");
    let mut same_microsecond = backward_bodies[..3].to_vec();
    same_microsecond.sort();
    assert_eq!(same_microsecond, vec!["r1", "r2", "r3"]);
    assert_eq!(backward_bodies[3], "r0");
    server.shutdown().await;
}

#[tokio::test]
async fn a_context_style_predecessor_and_successor_pair_returns_each_row_exactly_once() {
    let (server, catalog) = TestServer::start().await;
    seed_one_millisecond(&server, &catalog).await.expect("seed");
    let ascending_hits = page_through_hits(&server, "/logs/_search", COL_BODY, "asc", 3)
        .await
        .expect("paging");
    let anchor_id = ascending_hits
        .iter()
        .find(|(body, _, _)| body == "r2")
        .map(|(_, id, _)| id.clone())
        .expect("r2");

    // An anchor-style fetch: the anchor by id, with the context sort.
    let (status, anchor_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({
            "query": {"bool": {"must": [{"constant_score": {"filter": {"ids": {"values": [anchor_id]}}}}]}},
            "size": 1,
            "sort": build_time_doc_sort("desc")
        }),
    )
    .await;
    assert_eq!(status, 200, "{anchor_answer}");
    let anchor = read_hit_identities(&anchor_answer, COL_BODY).expect("hit identities");
    assert_eq!(anchor.len(), 1);
    assert_eq!(anchor[0].0, "r2");
    let anchor_sort = anchor[0].2.clone();

    // Predecessor- and successor-style requests: a time window, the anchor excluded by id,
    // search_after the anchor's sort values, the direction reversed for predecessors.
    let build_neighbour_request = |direction: &str, bound: &str, instant: &str| {
        json!({
            "query": {"bool": {"must": [{"bool": {
                "must": {"constant_score": {"filter": {"range": {"timestamp": {bound: instant, "format": "strict_date_optional_time"}}}}},
                "must_not": {"ids": {"values": [anchor_id]}}
            }}]}},
            "sort": build_time_doc_sort(direction),
            "search_after": anchor_sort,
            "size": 5
        })
    };
    let (status, predecessors) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &build_neighbour_request("desc", "lte", "2025-10-09T08:53:21.000Z"),
    )
    .await;
    assert_eq!(status, 200, "{predecessors}");
    let (status, successors) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &build_neighbour_request("asc", "gte", "2025-10-09T08:53:19.000Z"),
    )
    .await;
    assert_eq!(status, 200, "{successors}");

    let mut seen_bodies: Vec<String> = read_hit_identities(&predecessors, COL_BODY)
        .expect("hit identities")
        .into_iter()
        .map(|(body, _, _)| body)
        .collect();
    seen_bodies.push("r2".to_string());
    seen_bodies.extend(
        read_hit_identities(&successors, COL_BODY)
            .expect("hit identities")
            .into_iter()
            .map(|(body, _, _)| body),
    );
    let mut sorted_bodies = seen_bodies.clone();
    sorted_bodies.sort();
    assert_eq!(
        sorted_bodies,
        vec!["r0", "r1", "r2", "r3", "r4", "r5", "r6"],
        "each row exactly once: {seen_bodies:?}"
    );

    // `r3` was read from the WAL: its `_id`, hashed from the WAL batch, finds it through the
    // hash the `ids` filter computes inside the scan.
    let wal_row_id = ascending_hits
        .iter()
        .find(|(body, _, _)| body == "r3")
        .map(|(_, id, _)| id.clone())
        .expect("r3");
    let (status, wal_row_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"ids": {"values": [wal_row_id]}}}),
    )
    .await;
    assert_eq!(status, 200, "{wal_row_answer}");
    let wal_row_hits: Vec<(String, String)> = read_hit_identities(&wal_row_answer, COL_BODY)
        .expect("hit identities")
        .into_iter()
        .map(|(body, id, _)| (body, id))
        .collect();
    assert_eq!(wal_row_hits, vec![("r3".to_string(), wal_row_id)]);
    server.shutdown().await;
}

#[tokio::test]
async fn identical_rows_share_id_and_doc() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(&catalog, LOGS_TABLE, &[build_log_row(TENANT_A, T0 + 7, "same", "INFO")])
        .await
        .expect("commit");
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &[build_log_row(TENANT_A, T0 + 7, "same", "INFO")],
    )
    .await
    .expect("segment");

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"sort": build_time_doc_sort("asc")}),
    )
    .await;

    assert_eq!(status, 200, "{answer}");
    let hits = read_hit_identities(&answer, COL_BODY).expect("hit identities");
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].1, hits[1].1, "equal rows share _id");
    assert_eq!(hits[0].2, hits[1].2, "equal rows share their sort values");
    assert_eq!(hits[0].1.len(), 32);
    server.shutdown().await;
}

/// A `spans` row of `tenant` at `micros` with the stated natural key and `name`.
fn build_span_row<'a>(tenant: &'a str, micros: i64, trace_id: &'a [u8], name: &'a str) -> Row<'a> {
    Row {
        tenant,
        cells: vec![
            (COL_TIMESTAMP, Cell::Micros(micros)),
            (COL_TRACE_ID, Cell::Bytes(trace_id)),
            (COL_SPAN_ID, Cell::Bytes(&SPAN_ID)),
            (COL_NAME, Cell::Text(name)),
        ],
    }
}

#[tokio::test]
async fn an_ids_query_finds_a_span_by_its_natural_key_and_a_log_by_its_hashed_id() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[build_span_row(TENANT_A, T0, &TRACE_ID, "checkout")],
    )
    .await
    .expect("commit spans");
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0 + 5, "keep", "INFO"),
            build_log_row(TENANT_A, T0 + 5, "other", "INFO"),
        ],
    )
    .await
    .expect("commit logs");

    let span_document_id = "0123456789abcdeffedcba9876543210aabbccdd00112233";
    let (status, span) = post_json(
        &server,
        Some(TENANT_A),
        "/spans/_search",
        &json!({"query": {"ids": {"values": [span_document_id]}}}),
    )
    .await;
    assert_eq!(status, 200, "{span}");
    assert_eq!(span["hits"]["total"]["value"], 1);
    assert_eq!(span["hits"]["hits"][0]["_id"], span_document_id);

    let (_, every_log) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"sort": build_time_doc_sort("asc")}),
    )
    .await;
    let keep_id = read_hit_identities(&every_log, COL_BODY)
        .expect("hit identities")
        .into_iter()
        .find(|(body, _, _)| body == "keep")
        .map(|(_, id, _)| id)
        .expect("keep");
    let (status, keep_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"ids": {"values": [keep_id, "nope"]}}}),
    )
    .await;
    assert_eq!(status, 200, "{keep_answer}");
    assert_eq!(
        read_sorted_source_texts(&keep_answer, COL_BODY).expect("sorted source texts"),
        vec!["keep"]
    );
    assert_eq!(keep_answer["hits"]["hits"][0]["_id"], keep_id);

    // Several hashed ids in one query each select their own row.
    let log_ids: Vec<String> = read_hit_identities(&every_log, COL_BODY)
        .expect("hit identities")
        .into_iter()
        .map(|(_, id, _)| id)
        .collect();
    assert_eq!(log_ids.len(), 2);
    let (status, every_id_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"ids": {"values": log_ids}}}),
    )
    .await;
    assert_eq!(status, 200, "{every_id_answer}");
    assert_eq!(
        read_sorted_source_texts(&every_id_answer, COL_BODY).expect("sorted source texts"),
        vec!["keep", "other"]
    );

    let (status, malformed_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"ids": {"values": ["nope", span_document_id]}}}),
    )
    .await;
    assert_eq!(status, 200, "{malformed_answer}");
    assert_eq!(
        malformed_answer["hits"]["total"]["value"], 0,
        "an id of another form matches nothing"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn an_ids_query_with_a_colliding_id_never_returns_another_tenants_row() {
    let (server, catalog) = TestServer::start().await;
    // Identical content, so identical ids, for both tenants; plus a row only tenant B has.
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0, "shared", "INFO"),
            build_log_row(TENANT_B, T0, "shared", "INFO"),
            build_log_row(TENANT_B, T0 + 1, "b-only", "INFO"),
        ],
    )
    .await
    .expect("commit");

    let (_, tenant_b) = post_json(
        &server,
        Some(TENANT_B),
        "/logs/_search",
        &json!({"sort": build_time_doc_sort("asc")}),
    )
    .await;
    let tenant_b_ids = read_hit_identities(&tenant_b, COL_BODY).expect("hit identities");
    assert_eq!(tenant_b_ids.len(), 2);
    let shared_id = tenant_b_ids[0].1.clone();
    let b_only_id = tenant_b_ids[1].1.clone();

    let (status, shared_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"ids": {"values": [shared_id]}}}),
    )
    .await;
    assert_eq!(status, 200, "{shared_answer}");
    assert_eq!(
        shared_answer["hits"]["total"]["value"], 1,
        "only tenant A's copy: {shared_answer}"
    );

    let (status, foreign_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"ids": {"values": [b_only_id]}}}),
    )
    .await;
    assert_eq!(status, 200, "{foreign_answer}");
    assert_eq!(foreign_answer["hits"]["total"]["value"], 0);
    assert_eq!(foreign_answer["hits"]["hits"], json!([]));
    server.shutdown().await;
}

#[tokio::test]
async fn sort_on_an_unmapped_field_without_unmapped_type_is_a_query_shard_400() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(&catalog, LOGS_TABLE, &[build_log_row(TENANT_A, T0, "one", "INFO")])
        .await
        .expect("commit");

    let (status, refused_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"sort": [{"no_such_field": {"order": "asc"}}]}),
    )
    .await;
    assert_eq!(status, 400, "{refused_answer}");
    assert_eq!(refused_answer["error"]["type"], "search_phase_execution_exception");
    assert_eq!(
        refused_answer["error"]["root_cause"][0]["type"],
        "query_shard_exception"
    );

    let (status, accepted_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"sort": [{"no_such_field": {"order": "asc", "unmapped_type": "keyword"}}]}),
    )
    .await;
    assert_eq!(status, 200, "{accepted_answer}");
    assert_eq!(accepted_answer["hits"]["hits"][0]["sort"], json!([null]));
    assert_eq!(accepted_answer["hits"]["hits"][0]["_score"], Value::Null);
    assert_eq!(accepted_answer["hits"]["max_score"], Value::Null);

    for sort in [json!([{"body": "asc"}]), json!([{"log_attributes": "asc"}])] {
        let (status, refused_answer) =
            post_json(&server, Some(TENANT_A), "/logs/_search", &json!({"sort": sort})).await;
        assert_eq!(status, 400, "{sort}: {refused_answer}");
        assert_eq!(
            refused_answer["error"]["root_cause"][0]["type"], "query_shard_exception",
            "{sort}"
        );
    }
    let (status, refused_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"sort": [{"timestamp": {"order": "asc", "numeric_type": "fortnight"}}]}),
    )
    .await;
    assert_eq!(status, 400, "{refused_answer}");
    assert_eq!(refused_answer["error"]["type"], "illegal_argument_exception");

    // An accepted numeric_type changes nothing: the date still renders as epoch milliseconds.
    let (status, date_nanos_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"sort": [{"timestamp": {"order": "asc", "numeric_type": "date_nanos"}}]}),
    )
    .await;
    assert_eq!(status, 200, "{date_nanos_answer}");
    assert_eq!(
        date_nanos_answer["hits"]["hits"][0]["sort"],
        json!([1_760_000_000_000_i64])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_field_typed_differently_across_indices_cannot_be_sorted() {
    let (server, catalog) = TestServer::start().await;
    // Both tables exist before the first request: the engine caches the catalog provider the
    // first request builds, so a table created later is missing from the session's indices.
    create_exact_name_table(
        &catalog,
        "scans",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, COL_NAME, Type::Primitive(PrimitiveType::Long)),
        ],
    )
    .await
    .expect("create scans");
    // `duration_micros` is a `long` on spans and a `date` here. Both sort as 64-bit integers,
    // but epoch milliseconds and raw microsecond counts must not merge into one order.
    create_exact_name_table(
        &catalog,
        "timings",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, COL_DURATION_MICROS, Type::Primitive(PrimitiveType::Timestamp)),
        ],
    )
    .await
    .expect("create timings");

    let (status, refused_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/scans,spans/_search",
        &json!({"sort": [{"name": "asc"}]}),
    )
    .await;
    assert_eq!(status, 400, "{refused_answer}");
    assert_eq!(refused_answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(refused_answer["error"]["field"], "name");
    assert_eq!(refused_answer["error"]["indices"], json!(["scans", "spans"]));

    let (status, by_time_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/scans,spans/_search",
        &json!({"sort": [{"timestamp": "desc"}]}),
    )
    .await;
    assert_eq!(
        status, 200,
        "a field typed alike in both indices sorts: {by_time_answer}"
    );

    let (status, refused_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/spans,timings/_search",
        &json!({"sort": [{"duration_micros": "asc"}]}),
    )
    .await;
    assert_eq!(status, 400, "{refused_answer}");
    assert_eq!(refused_answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(refused_answer["error"]["field"], "duration_micros");
    assert_eq!(refused_answer["error"]["indices"], json!(["spans", "timings"]));
    server.shutdown().await;
}

#[tokio::test]
async fn hits_of_two_indices_tied_on_the_sort_key_come_in_index_name_order() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(&catalog, LOGS_TABLE, &[build_log_row(TENANT_A, T0, "tied", "INFO")])
        .await
        .expect("commit logs");
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[build_span_row(TENANT_A, T0, &TRACE_ID, "tied")],
    )
    .await
    .expect("commit spans");

    // One row in each index at the same instant: the sort key ties, so the index first in name
    // order comes first in either direction, whatever order the expression names them in.
    for order in ["asc", "desc"] {
        let (status, answer) = post_json(
            &server,
            Some(TENANT_A),
            "/spans,logs/_search",
            &json!({"sort": [{"timestamp": order}]}),
        )
        .await;
        assert_eq!(status, 200, "{answer}");
        let indices: Vec<&str> = answer["hits"]["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| hit["_index"].as_str().expect("index"))
            .collect();
        assert_eq!(indices, ["logs", "spans"], "{order}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn search_after_with_a_wrong_value_count_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(&catalog, LOGS_TABLE, &[build_log_row(TENANT_A, T0, "one", "INFO")])
        .await
        .expect("commit");
    // The largest `_doc` value is `1000 · 2^43 - 1`; one above it is no `_doc` value.
    let first_invalid_doc = 1_000_i64 << 43;

    for body in [
        json!({"sort": build_time_doc_sort("asc"), "search_after": [1_760_000_000_000_i64]}),
        json!({"sort": build_time_doc_sort("asc"), "search_after": [1_760_000_000_000_i64, 0, 0]}),
        json!({"search_after": [1_760_000_000_000_i64]}),
        json!({"from": 1, "sort": build_time_doc_sort("asc"), "search_after": [1_760_000_000_000_i64, 0]}),
        json!({"sort": build_time_doc_sort("asc"), "search_after": [1_760_000_000_000_i64, -1]}),
        json!({"sort": build_time_doc_sort("asc"), "search_after": [1_760_000_000_000_i64, first_invalid_doc]}),
    ] {
        let (status, refused_answer) = post_json(&server, Some(TENANT_A), "/logs/_search", &body).await;
        assert_eq!(status, 400, "{body}: {refused_answer}");
        assert_eq!(refused_answer["error"]["type"], "illegal_argument_exception", "{body}");
    }
    for anchor_doc in [0, first_invalid_doc - 1] {
        let (status, answer) = post_json(
            &server,
            Some(TENANT_A),
            "/logs/_search",
            &json!({"sort": build_time_doc_sort("asc"), "search_after": [1_759_999_999_999_i64, anchor_doc]}),
        )
        .await;
        assert_eq!(status, 200, "{anchor_doc}: {answer}");
        assert_eq!(
            answer["hits"]["hits"].as_array().expect("hits").len(),
            1,
            "{anchor_doc}"
        );
    }
    server.shutdown().await;
}

/// A `logs` row of `tenant` at `micros` with `body`, and `service_name` when one is given.
fn build_service_log_row<'a>(tenant: &'a str, micros: i64, body: &'a str, service_name: Option<&'a str>) -> Row<'a> {
    let mut cells = vec![(COL_TIMESTAMP, Cell::Micros(micros)), (COL_BODY, Cell::Text(body))];
    if let Some(service_name) = service_name {
        cells.push((COL_SERVICE_NAME, Cell::Text(service_name)));
    }
    Row { tenant, cells }
}

#[tokio::test]
async fn a_missing_sort_value_sorts_last_in_either_order_and_anchors_search_after() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_service_log_row(TENANT_A, T0, "m1", Some("beta")),
            build_service_log_row(TENANT_A, T0 + 1_000, "m2", None),
            build_service_log_row(TENANT_B, T0, "b1", Some("aaa")),
            build_service_log_row(TENANT_A, T0 + 2_000, "m3", Some("alpha")),
            build_service_log_row(TENANT_A, T0 + 3_000, "m4", None),
        ],
    )
    .await
    .expect("commit");
    let build_epoch_millis = |offset: i64| json!(1_760_000_000_000_i64 + offset);

    let cases = [
        (
            json!({"sort": [{"service_name": "asc"}, {"timestamp": "asc"}]}),
            vec![
                ("m3", json!(["alpha", build_epoch_millis(2)])),
                ("m1", json!(["beta", build_epoch_millis(0)])),
                ("m2", json!([null, build_epoch_millis(1)])),
                ("m4", json!([null, build_epoch_millis(3)])),
            ],
        ),
        (
            json!({"sort": [{"service_name": "desc"}, {"timestamp": "asc"}]}),
            vec![
                ("m1", json!(["beta", build_epoch_millis(0)])),
                ("m3", json!(["alpha", build_epoch_millis(2)])),
                ("m2", json!([null, build_epoch_millis(1)])),
                ("m4", json!([null, build_epoch_millis(3)])),
            ],
        ),
        (
            json!({"sort": [{"service_name": "asc"}, {"timestamp": "asc"}], "search_after": ["beta", build_epoch_millis(0)]}),
            vec![
                ("m2", json!([null, build_epoch_millis(1)])),
                ("m4", json!([null, build_epoch_millis(3)])),
            ],
        ),
        (
            json!({"sort": [{"service_name": "desc"}, {"timestamp": "asc"}], "search_after": [null, build_epoch_millis(1)]}),
            vec![("m4", json!([null, build_epoch_millis(3)]))],
        ),
    ];
    for (body, expected_hits) in cases {
        let (status, answer) = post_json(&server, Some(TENANT_A), "/logs/_search", &body).await;
        assert_eq!(status, 200, "{body}: {answer}");
        let hits: Vec<(String, Value)> = read_hit_identities(&answer, COL_BODY)
            .expect("hit identities")
            .into_iter()
            .map(|(body, _, sort)| (body, sort))
            .collect();
        let expected_hits: Vec<(String, Value)> =
            expected_hits.into_iter().map(|(body, sort)| (body.to_string(), sort)).collect();
        assert_eq!(hits, expected_hits, "{body}");
    }
    server.shutdown().await;
}

/// The exact-name table of the sort-kind case, and its keyword, double and boolean columns.
const READINGS_TABLE: &str = "readings";
const READING_LABEL_COLUMN: &str = "label";
const READING_VALUE_COLUMN: &str = "reading";
const READING_FLAG_COLUMN: &str = "flag";

/// A `readings` row of `tenant` at `T0` with the stated label, reading and flag.
fn build_reading_row<'a>(tenant: &'a str, label: &'a str, reading: f64, flag: bool) -> Row<'a> {
    Row {
        tenant,
        cells: vec![
            (COL_TIMESTAMP, Cell::Micros(T0)),
            (READING_LABEL_COLUMN, Cell::Text(label)),
            (READING_VALUE_COLUMN, Cell::Double(reading)),
            (READING_FLAG_COLUMN, Cell::Bool(flag)),
        ],
    }
}

/// Each sort kind orders by its own rule and renders its own sort value: a keyword as the
/// string, a double as the number, a boolean as `0` or `1`, a hex id as lowercase hex.
#[tokio::test]
async fn keyword_double_boolean_and_hex_id_fields_sort_and_render_by_their_kind() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(
        &catalog,
        READINGS_TABLE,
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, READING_LABEL_COLUMN, Type::Primitive(PrimitiveType::String)),
            NestedField::optional(4, READING_VALUE_COLUMN, Type::Primitive(PrimitiveType::Double)),
            NestedField::optional(5, READING_FLAG_COLUMN, Type::Primitive(PrimitiveType::Boolean)),
        ],
    )
    .await
    .expect("create readings");
    commit_rows(
        &catalog,
        READINGS_TABLE,
        &[
            build_reading_row(TENANT_A, "b", 2.5, true),
            build_reading_row(TENANT_A, "a", -1.0, false),
            build_reading_row(TENANT_B, "0", -100.0, false),
            build_reading_row(TENANT_A, "c", 10.0, true),
        ],
    )
    .await
    .expect("commit readings");
    let high_trace_id = [0xff_u8; 16];
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_span_row(TENANT_A, T0, &high_trace_id, "high"),
            build_span_row(TENANT_A, T0, &TRACE_ID, "low"),
        ],
    )
    .await
    .expect("commit spans");

    let reading_cases = [
        (
            json!([{"label": "desc"}]),
            vec![("c", json!(["c"])), ("b", json!(["b"])), ("a", json!(["a"]))],
        ),
        (
            json!([{"reading": "asc"}]),
            vec![("a", json!([-1.0])), ("b", json!([2.5])), ("c", json!([10.0]))],
        ),
        (
            json!([{"flag": "desc"}, {"label": "asc"}]),
            vec![("b", json!([1, "b"])), ("c", json!([1, "c"])), ("a", json!([0, "a"]))],
        ),
    ];
    for (sort, expected_hits) in reading_cases {
        let (status, answer) = post_json(&server, Some(TENANT_A), "/readings/_search", &json!({"sort": sort})).await;
        assert_eq!(status, 200, "{sort}: {answer}");
        let hits: Vec<(Value, Value)> = answer["hits"]["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| (hit["_source"][READING_LABEL_COLUMN].clone(), hit["sort"].clone()))
            .collect();
        let expected_hits: Vec<(Value, Value)> = expected_hits
            .into_iter()
            .map(|(label, sort_values)| (json!(label), sort_values))
            .collect();
        assert_eq!(hits, expected_hits, "{sort}");
    }

    let (status, spans) = post_json(
        &server,
        Some(TENANT_A),
        "/spans/_search",
        &json!({"sort": [{"trace_id": "desc"}]}),
    )
    .await;
    assert_eq!(status, 200, "{spans}");
    assert_eq!(read_source_values(&spans, COL_NAME), vec![json!("high"), json!("low")]);
    let span_sort_values: Vec<Value> = spans["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| hit["sort"].clone())
        .collect();
    assert_eq!(
        span_sort_values,
        vec![
            json!(["ffffffffffffffffffffffffffffffff"]),
            json!(["0123456789abcdeffedcba9876543210"])
        ]
    );
    server.shutdown().await;
}

/// Each `search_after` value must be a value of its own sort key, so an anchor of another form
/// is refused rather than read as some other position.
#[tokio::test]
async fn a_search_after_value_that_fits_no_sort_key_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(&catalog, LOGS_TABLE, &[build_log_row(TENANT_A, T0, "one", "INFO")])
        .await
        .expect("commit");

    let cases = [
        (
            "a text value for a date",
            "/logs/_search",
            build_time_doc_sort("asc"),
            json!(["abc", 0]),
        ),
        (
            "a fraction for a date",
            "/logs/_search",
            build_time_doc_sort("asc"),
            json!([1.5, 0]),
        ),
        (
            "an array value",
            "/logs/_search",
            build_time_doc_sort("asc"),
            json!([[1], 0]),
        ),
        (
            "an object value",
            "/logs/_search",
            build_time_doc_sort("asc"),
            json!([{"a": 1}, 0]),
        ),
        (
            "a missing _doc value",
            "/logs/_search",
            build_time_doc_sort("asc"),
            json!([1_760_000_000_000_i64, null]),
        ),
        (
            "a text value for a double",
            "/operations/_search",
            json!([{TEMPERATURE: "asc"}]),
            json!(["x"]),
        ),
        (
            "a non-hex value for an id",
            "/spans/_search",
            json!([{"trace_id": "asc"}]),
            json!(["zz"]),
        ),
        (
            "a short hex value for an id",
            "/spans/_search",
            json!([{"trace_id": "asc"}]),
            json!(["0123"]),
        ),
    ];
    for (case, path, sort, anchor) in cases {
        let (status, refused_answer) = post_json(
            &server,
            Some(TENANT_A),
            path,
            &json!({"sort": sort, "search_after": anchor}),
        )
        .await;
        assert_eq!(status, 400, "{case}: {refused_answer}");
        assert_eq!(refused_answer["error"]["type"], "illegal_argument_exception", "{case}");
    }
    server.shutdown().await;
}

/// `unmapped_type` lets an index lacking the field take part: its rows all miss the value, so
/// they sort after every present one in either order. An `unmapped_type` that is no sortable
/// type of a data index is refused, whether or not an index lacks the field.
#[tokio::test]
async fn an_unmapped_type_sorts_an_index_lacking_the_field_last_and_an_unknown_one_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(&catalog, LOGS_TABLE, &[build_log_row(TENANT_A, T0, "log", "INFO")])
        .await
        .expect("commit logs");
    let high_trace_id = [0xff_u8; 16];
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_span_row(TENANT_A, T0, &high_trace_id, "b-span"),
            build_span_row(TENANT_A, T0, &TRACE_ID, "a-span"),
            build_span_row(TENANT_B, T0, &TRACE_ID, "0-span"),
        ],
    )
    .await
    .expect("commit spans");

    for (order, expected_hits) in [
        (
            "asc",
            [
                ("spans", json!(["a-span"])),
                ("spans", json!(["b-span"])),
                ("logs", json!([null])),
            ],
        ),
        (
            "desc",
            [
                ("spans", json!(["b-span"])),
                ("spans", json!(["a-span"])),
                ("logs", json!([null])),
            ],
        ),
    ] {
        let (status, answer) = post_json(
            &server,
            Some(TENANT_A),
            "/logs,spans/_search",
            &json!({"sort": [{"name": {"order": order, "unmapped_type": "keyword"}}]}),
        )
        .await;
        assert_eq!(status, 200, "{order}: {answer}");
        let hits: Vec<(String, Value)> = answer["hits"]["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| (hit["_index"].as_str().expect("index").to_string(), hit["sort"].clone()))
            .collect();
        let expected_hits: Vec<(String, Value)> = expected_hits
            .into_iter()
            .map(|(index, sort_values)| (index.to_string(), sort_values))
            .collect();
        assert_eq!(hits, expected_hits, "{order}");
    }

    for (path, unmapped_type) in [("/logs,spans/_search", "geo_point"), ("/spans/_search", "fortnight")] {
        let (status, refused_answer) = post_json(
            &server,
            Some(TENANT_A),
            path,
            &json!({"sort": [{"name": {"order": "asc", "unmapped_type": unmapped_type}}]}),
        )
        .await;
        assert_eq!(status, 400, "{unmapped_type}: {refused_answer}");
        assert_eq!(
            refused_answer["error"]["type"], "illegal_argument_exception",
            "{unmapped_type}"
        );
    }
    server.shutdown().await;
}

/// The exact-name table of the second sort-kind case, and its label, date, integer, float and
/// decimal columns. The `Row` helper fills no date, 32-bit integer or float column, so the rows
/// are written as one batch built here.
const VISITS_TABLE: &str = "visits";
const VISIT_LABEL_COLUMN: &str = "label";
const VISIT_DAY_COLUMN: &str = "day";
const VISIT_COUNT_COLUMN: &str = "visit_count";
const VISIT_RATIO_COLUMN: &str = "ratio";
const VISIT_AMOUNT_COLUMN: &str = "amount";

/// One `visits` row: its tenant, label, day (days since the epoch), count, ratio, and amount as
/// its unscaled value at scale 2.
struct VisitRow<'a> {
    tenant: &'a str,
    label: &'a str,
    day: i32,
    visit_count: i32,
    ratio: f32,
    unscaled_amount: i128,
}

/// Write `rows` into `visits` at `T0` through the production writer as one append.
async fn commit_visit_rows(catalog: &Arc<dyn iceberg::Catalog>, rows: &[VisitRow<'_>]) -> Result<(), RowWriteError> {
    let schema = read_arrow_schema(catalog, VISITS_TABLE).await?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| -> Result<ArrayRef, RowWriteError> {
            Ok(match (field.name().as_str(), field.data_type()) {
                (COL_TENANT_ID, _) => Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.tenant))),
                (COL_TIMESTAMP, DataType::Timestamp(_, timezone)) => Arc::new(
                    TimestampMicrosecondArray::from_iter_values(rows.iter().map(|_| T0))
                        .with_timezone_opt(timezone.clone()),
                ),
                (VISIT_LABEL_COLUMN, _) => Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.label))),
                (VISIT_DAY_COLUMN, _) => Arc::new(Date32Array::from_iter_values(rows.iter().map(|row| row.day))),
                (VISIT_COUNT_COLUMN, _) => {
                    Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.visit_count)))
                }
                (VISIT_RATIO_COLUMN, _) => Arc::new(Float32Array::from_iter_values(rows.iter().map(|row| row.ratio))),
                (VISIT_AMOUNT_COLUMN, _) => Arc::new(
                    Decimal128Array::from_iter_values(rows.iter().map(|row| row.unscaled_amount))
                        .with_precision_and_scale(10, 2)?,
                ),
                (unknown_column, data_type) => {
                    return Err(format!("`visits` has no column `{unknown_column}` of type {data_type}").into());
                }
            })
        })
        .collect::<Result<Vec<ArrayRef>, RowWriteError>>()?;
    commit_batch(catalog, VISITS_TABLE, RecordBatch::try_new(schema, columns)?).await
}

/// A date column of days sorts by its epoch milliseconds (`days · 86 400 000`), a 32-bit integer
/// as a long, a float and a decimal as doubles.
#[tokio::test]
async fn date_integer_float_and_decimal_fields_sort_and_render_by_their_kind() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(
        &catalog,
        VISITS_TABLE,
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, VISIT_LABEL_COLUMN, Type::Primitive(PrimitiveType::String)),
            NestedField::optional(4, VISIT_DAY_COLUMN, Type::Primitive(PrimitiveType::Date)),
            NestedField::optional(5, VISIT_COUNT_COLUMN, Type::Primitive(PrimitiveType::Int)),
            NestedField::optional(6, VISIT_RATIO_COLUMN, Type::Primitive(PrimitiveType::Float)),
            NestedField::optional(
                7,
                VISIT_AMOUNT_COLUMN,
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 10,
                    scale: 2,
                }),
            ),
        ],
    )
    .await
    .expect("create visits");
    commit_visit_rows(
        &catalog,
        &[
            VisitRow {
                tenant: TENANT_A,
                label: "v1",
                day: 20_000,
                visit_count: 3,
                ratio: 0.5,
                unscaled_amount: 1_250,
            },
            VisitRow {
                tenant: TENANT_A,
                label: "v2",
                day: -1,
                visit_count: -7,
                ratio: 2.25,
                unscaled_amount: -25,
            },
            VisitRow {
                tenant: TENANT_B,
                label: "b",
                day: 0,
                visit_count: 0,
                ratio: 99.0,
                unscaled_amount: 0,
            },
            VisitRow {
                tenant: TENANT_A,
                label: "v3",
                day: 20_001,
                visit_count: 10,
                ratio: -1.5,
                unscaled_amount: 10_000,
            },
        ],
    )
    .await
    .expect("commit visits");

    let cases = [
        (
            json!([{"day": "asc"}]),
            vec![
                ("v2", json!([-86_400_000_i64])),
                ("v1", json!([1_728_000_000_000_i64])),
                ("v3", json!([1_728_086_400_000_i64])),
            ],
        ),
        (
            json!([{"visit_count": "desc"}]),
            vec![("v3", json!([10])), ("v1", json!([3])), ("v2", json!([-7]))],
        ),
        (
            json!([{"ratio": "asc"}]),
            vec![("v3", json!([-1.5])), ("v1", json!([0.5])), ("v2", json!([2.25]))],
        ),
        (
            json!([{"amount": "desc"}]),
            vec![("v3", json!([100.0])), ("v1", json!([12.5])), ("v2", json!([-0.25]))],
        ),
    ];
    for (sort, expected_hits) in cases {
        let (status, answer) = post_json(&server, Some(TENANT_A), "/visits/_search", &json!({"sort": sort})).await;
        assert_eq!(status, 200, "{sort}: {answer}");
        let hits: Vec<(Value, Value)> = answer["hits"]["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| (hit["_source"][VISIT_LABEL_COLUMN].clone(), hit["sort"].clone()))
            .collect();
        let expected_hits: Vec<(Value, Value)> = expected_hits
            .into_iter()
            .map(|(label, sort_values)| (json!(label), sort_values))
            .collect();
        assert_eq!(hits, expected_hits, "{sort}");
    }
    server.shutdown().await;
}

/// The `sort` values of `hits` as `(date millis, _doc)` pairs, in hit order; a missing date is
/// `None`.
fn read_time_doc_pairs(hits: &[(String, String, Value)]) -> Result<Vec<(Option<i64>, i64)>, String> {
    hits.iter()
        .map(|(label, _, sort)| {
            let doc = sort[1].as_i64().ok_or_else(|| format!("{label} has no _doc value: {sort}"))?;
            Ok((sort[0].as_i64(), doc))
        })
        .collect()
}

/// Rows tied on the whole SQL part of a `[timestamp, _doc]` sort (one instant of millisecond
/// precision), more of them than a page holds: paging with `search_after` still returns every
/// row exactly once, in `_doc` order, although each fetch of the tied rows is limited to the
/// page. Logs order by the canonical row hash (rows in Iceberg and in the WAL), spans by the
/// span-key hash.
#[tokio::test]
async fn rows_tied_on_the_sql_part_of_the_sort_beyond_a_page_are_paged_in_doc_order_exactly_once() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0, "m0", "INFO"),
            build_log_row(TENANT_A, T0, "m1", "INFO"),
            build_log_row(TENANT_B, T0, "b0", "INFO"),
            build_log_row(TENANT_A, T0, "m2", "INFO"),
            build_log_row(TENANT_A, T0, "m3", "INFO"),
        ],
    )
    .await
    .expect("commit logs");
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &[
            build_log_row(TENANT_A, T0, "m4", "INFO"),
            build_log_row(TENANT_B, T0, "b1", "INFO"),
            build_log_row(TENANT_A, T0, "m5", "INFO"),
            build_log_row(TENANT_A, T0, "m6", "INFO"),
        ],
    )
    .await
    .expect("segment");
    let trace_ids: Vec<[u8; 16]> = (1..=6).map(|byte| [byte; 16]).collect();
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_span_row(TENANT_A, T0, &trace_ids[0], "s0"),
            build_span_row(TENANT_A, T0, &trace_ids[1], "s1"),
            build_span_row(TENANT_B, T0, &trace_ids[5], "b"),
            build_span_row(TENANT_A, T0, &trace_ids[2], "s2"),
            build_span_row(TENANT_A, T0, &trace_ids[3], "s3"),
            build_span_row(TENANT_A, T0, &trace_ids[4], "s4"),
        ],
    )
    .await
    .expect("commit spans");

    let cases = [
        (
            "/logs/_search",
            COL_BODY,
            vec!["m0", "m1", "m2", "m3", "m4", "m5", "m6"],
        ),
        ("/spans/_search", COL_NAME, vec!["s0", "s1", "s2", "s3", "s4"]),
    ];
    for (search_path, label_column, expected_labels) in cases {
        for order in ["asc", "desc"] {
            let hits = page_through_hits(&server, search_path, label_column, order, 2)
                .await
                .expect("paging");
            let mut labels: Vec<&str> = hits.iter().map(|(label, _, _)| label.as_str()).collect();
            labels.sort_unstable();
            assert_eq!(labels, expected_labels, "{search_path} {order}: each row exactly once");
            let pairs = read_time_doc_pairs(&hits).expect("sort values");
            assert!(
                pairs.iter().all(|(time, _)| *time == Some(1_760_000_000_000)),
                "{search_path} {order}: one millisecond: {pairs:?}"
            );
            for adjacent_pairs in pairs.windows(2) {
                let (earlier_doc, later_doc) = (adjacent_pairs[0].1, adjacent_pairs[1].1);
                if order == "asc" {
                    assert!(earlier_doc < later_doc, "{search_path} asc: {pairs:?}");
                } else {
                    assert!(earlier_doc > later_doc, "{search_path} desc: {pairs:?}");
                }
            }
        }
    }
    server.shutdown().await;
}

/// The exact-name table of the date-timestamp case, and its label column. Its `timestamp` is an
/// optional Iceberg `date`, which no [`Row`] cell fills, so the rows are written as one batch.
const HOLIDAYS_TABLE: &str = "holidays";
const HOLIDAY_LABEL_COLUMN: &str = "label";

/// Write `rows` (tenant, day since the epoch or none, label) into `holidays` through the
/// production writer as one append.
async fn commit_holiday_rows(
    catalog: &Arc<dyn iceberg::Catalog>,
    rows: &[(&str, Option<i32>, &str)],
) -> Result<(), RowWriteError> {
    let schema = read_arrow_schema(catalog, HOLIDAYS_TABLE).await?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| -> Result<ArrayRef, RowWriteError> {
            Ok(match field.name().as_str() {
                COL_TENANT_ID => Arc::new(StringArray::from_iter_values(rows.iter().map(|(tenant, _, _)| *tenant))),
                COL_TIMESTAMP => Arc::new(Date32Array::from_iter(rows.iter().map(|(_, day, _)| *day))),
                HOLIDAY_LABEL_COLUMN => {
                    Arc::new(StringArray::from_iter_values(rows.iter().map(|(_, _, label)| *label)))
                }
                unknown_column => return Err(format!("`holidays` has no column `{unknown_column}`").into()),
            })
        })
        .collect::<Result<Vec<ArrayRef>, RowWriteError>>()?;
    commit_batch(catalog, HOLIDAYS_TABLE, RecordBatch::try_new(schema, columns)?).await
}

/// A row's `_id` carries the microsecond its own `timestamp` holds: a date's first microsecond,
/// and `0` for a NULL. `ids` with that `_id` finds the row, and a `[timestamp, _doc]` sort over
/// the date column answers, the dateless row last.
#[tokio::test]
async fn the_id_of_a_row_with_a_date_or_a_null_timestamp_finds_that_row() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(
        &catalog,
        HOLIDAYS_TABLE,
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::optional(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Date)),
            NestedField::optional(3, HOLIDAY_LABEL_COLUMN, Type::Primitive(PrimitiveType::String)),
        ],
    )
    .await
    .expect("create holidays");
    commit_holiday_rows(
        &catalog,
        &[
            (TENANT_A, None, "undated"),
            (TENANT_B, Some(20_000), "other"),
            (TENANT_A, Some(20_000), "dated"),
        ],
    )
    .await
    .expect("commit holidays");

    let (status, sorted_answer) = post_json(
        &server,
        Some(TENANT_A),
        "/holidays/_search",
        &json!({"sort": build_time_doc_sort("asc")}),
    )
    .await;
    assert_eq!(status, 200, "{sorted_answer}");
    let hits = read_hit_identities(&sorted_answer, HOLIDAY_LABEL_COLUMN).expect("hit identities");
    let labels: Vec<&str> = hits.iter().map(|(label, _, _)| label.as_str()).collect();
    assert_eq!(labels, vec!["dated", "undated"]);
    // Day 20 000 is 1 728 000 000 000 ms; its first microsecond, 1 728 000 000 000 000, is
    // 0x0006239b5a2c0000.
    assert_eq!(hits[0].2[0], json!(1_728_000_000_000_i64));
    assert_eq!(hits[1].2[0], Value::Null);
    assert!(hits[0].1.starts_with("0006239b5a2c0000"), "{}", hits[0].1);
    assert!(hits[1].1.starts_with("0000000000000000"), "{}", hits[1].1);

    for (label, id, _) in &hits {
        let (status, id_answer) = post_json(
            &server,
            Some(TENANT_A),
            "/holidays/_search",
            &json!({"query": {"ids": {"values": [id]}}}),
        )
        .await;
        assert_eq!(status, 200, "{label}: {id_answer}");
        let found_hits: Vec<(String, String)> = read_hit_identities(&id_answer, HOLIDAY_LABEL_COLUMN)
            .expect("hit identities")
            .into_iter()
            .map(|(found_label, found_id, _)| (found_label, found_id))
            .collect();
        assert_eq!(found_hits, vec![(label.clone(), id.clone())], "{label}");
    }
    server.shutdown().await;
}

/// Two tenant A log rows on 2025-10-09 (one with sub-millisecond digits), and a tenant B row.
async fn seed_two_logs(catalog: &Arc<dyn iceberg::Catalog>) -> Result<(), RowWriteError> {
    commit_rows(
        catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, 1_760_000_000_123_456, "first", "INFO"),
            build_log_row(TENANT_A, 1_760_000_001_000_000, "second", "WARN"),
            build_log_row(TENANT_B, 1_760_000_000_500_000, "tenant-b", "INFO"),
        ],
    )
    .await
}

#[tokio::test]
async fn a_discover_style_request_renders_docvalue_fields_version_and_no_highlight() {
    let (server, catalog) = TestServer::start().await;
    seed_two_logs(&catalog).await.expect("seed");

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search?ignore_unavailable=true&track_total_hits=true&timeout=30000ms&preference=1760000000000",
        &json!({
            "version": true,
            "size": 500,
            "sort": [{"timestamp": {"order": "desc", "unmapped_type": "boolean"}}],
            "stored_fields": ["*"],
            "script_fields": {},
            "docvalue_fields": [{"field": "timestamp", "format": "date_time"}],
            "_source": {"excludes": []},
            "highlight": {
                "pre_tags": ["@opensearch-dashboards-highlighted-field@"],
                "post_tags": ["@/opensearch-dashboards-highlighted-field@"],
                "fields": {"*": {}},
                "fragment_size": 2_147_483_647
            },
            "query": {"bool": {
                "must": [],
                "filter": [{"range": {"timestamp": {
                    "gte": "2025-10-09T00:00:00.000Z",
                    "lte": "2025-10-09T23:59:59.999Z",
                    "format": "strict_date_optional_time"
                }}}],
                "should": [],
                "must_not": []
            }}
        }),
    )
    .await;

    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    assert_eq!(answer["hits"]["max_score"], Value::Null);
    let hits = answer["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0]["_source"][COL_BODY], "second", "sorted by time, newest first");
    assert_eq!(hits[0]["_version"], 1);
    assert_eq!(hits[0]["_score"], Value::Null);
    assert_eq!(
        hits[0]["fields"],
        json!({(COL_TIMESTAMP): ["2025-10-09T08:53:21.000Z"]})
    );
    assert_eq!(
        hits[1]["fields"],
        json!({(COL_TIMESTAMP): ["2025-10-09T08:53:20.123Z"]})
    );
    assert_eq!(hits[1]["sort"], json!([1_760_000_000_123_i64]));
    for hit in hits {
        assert!(hit.get("highlight").is_none(), "{hit}");
        assert!(
            hit["_source"][COL_SEVERITY_TEXT].is_string(),
            "the whole source is kept: {hit}"
        );
    }
    server.shutdown().await;
}

#[tokio::test]
async fn a_grafana_logs_style_request_carries_the_time_field_as_one_rfc3339_nano_string() {
    let (server, catalog) = TestServer::start().await;
    seed_two_logs(&catalog).await.expect("seed");

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({
            "size": 500,
            "query": {"bool": {"filter": [{"range": {"timestamp": {
                "gte": "1759968000000", "lte": "1760054399999", "format": "epoch_millis"
            }}}]}},
            "sort": [{"timestamp": {"order": "desc", "unmapped_type": "boolean"}}, {"_doc": {"order": "desc"}}],
            "script_fields": {},
            "fields": [{"field": "timestamp", "format": "strict_date_optional_time_nanos"}],
            "docvalue_fields": ["timestamp"]
        }),
    )
    .await;

    assert_eq!(status, 200, "{answer}");
    let hits = answer["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 2);
    assert_eq!(
        hits[0]["fields"],
        json!({(COL_TIMESTAMP): ["2025-10-09T08:53:21.000000Z"]})
    );
    assert_eq!(
        hits[1]["fields"],
        json!({(COL_TIMESTAMP): ["2025-10-09T08:53:20.123456Z"]})
    );
    server.shutdown().await;
}

/// A concrete `fields` name reaches a MAP key whose stored key holds dots (one no pattern of the
/// request reaches), a promoted resource key and a hex id; a pattern reaches MAP entries by their
/// `<column>.<stored key>` paths. A name that names nothing, and a row holding none of the
/// values, add no key. The rows lie a millisecond apart, so the time sort orders them.
#[tokio::test]
async fn fields_render_map_keys_promoted_resource_keys_hex_ids_and_pattern_matched_map_entries() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            Row {
                tenant: TENANT_A,
                cells: vec![
                    (COL_TIMESTAMP, Cell::Micros(T0 + 1_000)),
                    (COL_SERVICE_NAME, Cell::Text("checkout")),
                    (COL_TRACE_ID, Cell::Bytes(&TRACE_ID)),
                    (COL_BODY, Cell::Text("with-attributes")),
                    (COL_LOG_ATTRIBUTES, Cell::Map(&[("http.method", "GET"), ("http", "x")])),
                    (COL_RESOURCE_ATTRIBUTES, Cell::Map(&[("host.name", "web-1")])),
                ],
            },
            build_log_row(TENANT_A, T0, "plain", "INFO"),
            Row {
                tenant: TENANT_B,
                cells: vec![
                    (COL_TIMESTAMP, Cell::Micros(T0 + 2_000)),
                    (COL_SERVICE_NAME, Cell::Text("tenant-b-service")),
                    (COL_LOG_ATTRIBUTES, Cell::Map(&[("http.method", "POST")])),
                ],
            },
        ],
    )
    .await
    .expect("commit");

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({
            "sort": [{"timestamp": "desc"}],
            "fields": [
                "log_attributes.http.method",
                "resource_attributes.host.name",
                "resource_attributes.service.name",
                "trace_id",
                "log_attributes.*",
                "log_attributes.absent"
            ]
        }),
    )
    .await;

    assert_eq!(status, 200, "{answer}");
    let hits = answer["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 2, "{answer}");
    assert_eq!(hits[0]["_source"][COL_BODY], "with-attributes");
    assert_eq!(
        hits[0]["fields"],
        json!({
            (format!("{COL_LOG_ATTRIBUTES}.http.method")): ["GET"],
            (format!("{COL_LOG_ATTRIBUTES}.http")): ["x"],
            (format!("{COL_RESOURCE_ATTRIBUTES}.host.name")): ["web-1"],
            (format!("{COL_RESOURCE_ATTRIBUTES}.service.name")): ["checkout"],
            (COL_TRACE_ID): ["0123456789abcdeffedcba9876543210"]
        })
    );
    assert_eq!(hits[1]["_source"][COL_BODY], "plain");
    assert!(hits[1].get("fields").is_none(), "{}", hits[1]);
    server.shutdown().await;
}

/// A `fields` pattern renders every scalar column it matches, in the pattern's own format, and
/// nothing for a column it does not match. A pattern reaching a MAP column renders only the
/// column's entries by their paths, never the MAP column itself as one key.
#[tokio::test]
async fn field_patterns_render_matched_scalar_columns_in_their_format_and_maps_only_by_entry_paths() {
    let (server, catalog) = TestServer::start().await;
    seed_two_logs(&catalog).await.expect("seed");
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[Row {
            tenant: TENANT_A,
            cells: vec![
                (COL_TIMESTAMP, Cell::Micros(T0)),
                (COL_BODY, Cell::Text("with-attributes")),
                (COL_SEVERITY_TEXT, Cell::Text("ERROR")),
                (COL_LOG_ATTRIBUTES, Cell::Map(&[("http.method", "GET"), ("http", "x")])),
                (COL_RESOURCE_ATTRIBUTES, Cell::Map(&[("host.name", "web-1")])),
            ],
        }],
    )
    .await
    .expect("commit");

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({
            "sort": [{"timestamp": "desc"}],
            "fields": [{"field": "time*", "format": "epoch_millis"}, "severity_*"]
        }),
    )
    .await;

    assert_eq!(status, 200, "{answer}");
    let fields: Vec<&Value> = answer["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| &hit["fields"])
        .collect();
    assert_eq!(
        fields,
        [
            &json!({(COL_TIMESTAMP): ["1760000001000"], (COL_SEVERITY_TEXT): ["WARN"]}),
            &json!({(COL_TIMESTAMP): ["1760000000123"], (COL_SEVERITY_TEXT): ["INFO"]}),
            &json!({(COL_TIMESTAMP): ["1760000000000"], (COL_SEVERITY_TEXT): ["ERROR"]}),
        ],
        "{answer}"
    );

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"sort": [{"timestamp": "asc"}], "size": 1, "fields": ["*"]}),
    )
    .await;

    assert_eq!(status, 200, "{answer}");
    let hits = answer["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 1, "{answer}");
    let fields = hits[0]["fields"].as_object().expect("fields");
    assert_eq!(fields.get(COL_BODY), Some(&json!(["with-attributes"])), "{answer}");
    for (column, expected_paths) in [
        (
            COL_LOG_ATTRIBUTES,
            vec![
                format!("{COL_LOG_ATTRIBUTES}.http"),
                format!("{COL_LOG_ATTRIBUTES}.http.method"),
            ],
        ),
        (
            COL_RESOURCE_ATTRIBUTES,
            vec![format!("{COL_RESOURCE_ATTRIBUTES}.host.name")],
        ),
        (COL_SCOPE_ATTRIBUTES, Vec::new()),
    ] {
        let mut paths: Vec<&String> = fields.keys().filter(|key| key.starts_with(column)).collect();
        paths.sort();
        assert_eq!(paths, expected_paths.iter().collect::<Vec<_>>(), "{answer}");
    }
    assert_eq!(
        fields.get(&format!("{COL_LOG_ATTRIBUTES}.http.method")),
        Some(&json!(["GET"]))
    );
    assert_eq!(fields.get(&format!("{COL_LOG_ATTRIBUTES}.http")), Some(&json!(["x"])));
    assert_eq!(
        fields.get(&format!("{COL_RESOURCE_ATTRIBUTES}.host.name")),
        Some(&json!(["web-1"]))
    );
    server.shutdown().await;
}

#[tokio::test]
async fn track_total_hits_integer_with_rest_total_hits_as_int_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    seed_two_logs(&catalog).await.expect("seed");

    for (path, body) in [
        (
            "/logs/_search?rest_total_hits_as_int=true&track_total_hits=100",
            json!({}),
        ),
        (
            "/logs/_search?rest_total_hits_as_int=true",
            json!({"track_total_hits": 100}),
        ),
    ] {
        let (status, refused) = post_json(&server, Some(TENANT_A), path, &body).await;
        assert_eq!(status, 400, "{path} {body}: {refused}");
        assert_eq!(refused["error"]["type"], "illegal_argument_exception");
    }
    let (status, integer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search?rest_total_hits_as_int=true",
        &json!({}),
    )
    .await;
    assert_eq!(status, 200, "{integer}");
    assert_eq!(integer["hits"]["total"], json!(2));
    server.shutdown().await;
}

#[tokio::test]
async fn size_zero_still_answers_a_hits_array() {
    let (server, catalog) = TestServer::start().await;
    seed_two_logs(&catalog).await.expect("seed");

    let (status, answer) = post_json(&server, Some(TENANT_A), "/logs/_search?size=0", &json!({})).await;

    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["hits"]["hits"], json!([]));
    assert_eq!(answer["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    assert_eq!(answer["hits"]["max_score"], Value::Null);
    server.shutdown().await;
}

#[tokio::test]
async fn script_fields_with_an_entry_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    seed_two_logs(&catalog).await.expect("seed");

    let (status, refused) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"script_fields": {"doubled": {"script": {"source": "doc['x'].value * 2"}}}}),
    )
    .await;
    assert_eq!(status, 400, "{refused}");
    assert_eq!(refused["error"]["type"], "illegal_argument_exception");

    let (status, accepted) = post_json(&server, Some(TENANT_A), "/logs/_search", &json!({"script_fields": {}})).await;
    assert_eq!(status, 200, "{accepted}");
    server.shutdown().await;
}

/// Tenant A has three committed and two WAL rows in `logs` and one span; tenant B's rows in the
/// same append and the same segment are never counted. A total is `eq` only when it is exact.
#[tokio::test]
async fn hits_total_is_exact_whenever_reported_as_exact() {
    let (server, catalog) = TestServer::start().await;
    commit_rows(
        &catalog,
        LOGS_TABLE,
        &[
            build_log_row(TENANT_A, T0, "one", "INFO"),
            build_log_row(TENANT_A, T0 + 1, "two", "INFO"),
            build_log_row(TENANT_A, T0 + 2, "three", "INFO"),
            build_log_row(TENANT_B, T0 + 3, "b-one", "INFO"),
        ],
    )
    .await
    .expect("commit logs");
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &[
            build_log_row(TENANT_A, T0 + 3, "four", "INFO"),
            build_log_row(TENANT_B, T0 + 4, "b-two", "INFO"),
            build_log_row(TENANT_A, T0 + 4, "five", "INFO"),
        ],
    )
    .await
    .expect("segment");
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[build_span_row(TENANT_A, T0, &TRACE_ID, "only-span")],
    )
    .await
    .expect("commit spans");

    let cases = [
        (
            "/logs/_search?track_total_hits=3",
            Some(json!({"value": 3, "relation": "gte"})),
        ),
        (
            "/logs/_search?track_total_hits=4",
            Some(json!({"value": 4, "relation": "gte"})),
        ),
        (
            "/logs/_search?track_total_hits=5",
            Some(json!({"value": 5, "relation": "eq"})),
        ),
        (
            "/logs/_search?track_total_hits=6",
            Some(json!({"value": 5, "relation": "eq"})),
        ),
        (
            "/logs/_search?track_total_hits=true",
            Some(json!({"value": 5, "relation": "eq"})),
        ),
        ("/logs/_search", Some(json!({"value": 5, "relation": "eq"}))),
        ("/logs/_search?track_total_hits=false", None),
        (
            "/logs/_search?track_total_hits=false&rest_total_hits_as_int=true",
            Some(json!(-1)),
        ),
        (
            "/logs,spans/_search?track_total_hits=5",
            Some(json!({"value": 5, "relation": "gte"})),
        ),
        (
            "/logs,spans/_search?track_total_hits=6",
            Some(json!({"value": 6, "relation": "eq"})),
        ),
    ];
    for (path, expected) in cases {
        let (status, answer) = post_json(&server, Some(TENANT_A), path, &json!({})).await;
        assert_eq!(status, 200, "{path}: {answer}");
        assert_eq!(answer["hits"].get("total").cloned(), expected, "{path}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn score_and_max_score_are_null_when_sorting_on_a_field_and_one_otherwise() {
    let (server, catalog) = TestServer::start().await;
    seed_two_logs(&catalog).await.expect("seed");

    for (sort, expected) in [
        (None, json!(1.0)),
        (Some(json!([{"_score": {"order": "desc"}}])), json!(1.0)),
        (Some(json!([{"timestamp": "desc"}])), Value::Null),
        (Some(json!([{"_score": "desc"}, {"timestamp": "desc"}])), Value::Null),
        (Some(json!([{"_doc": "asc"}])), Value::Null),
    ] {
        let mut body = json!({});
        if let Some(sort) = &sort {
            body["sort"] = sort.clone();
        }
        let (status, answer) = post_json(&server, Some(TENANT_A), "/logs/_search", &body).await;
        assert_eq!(status, 200, "{body}: {answer}");
        assert_eq!(answer["hits"]["max_score"], expected, "{body}");
        let scores: Vec<Value> = answer["hits"]["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| hit["_score"].clone())
            .collect();
        assert_eq!(scores, vec![expected.clone(), expected.clone()], "{body}");
    }
    // Unsorted hits come in no promised order, so they are compared as a set.
    let (_, unsorted) = post_json(&server, Some(TENANT_A), "/logs/_search", &json!({})).await;
    assert_eq!(
        read_sorted_source_texts(&unsorted, COL_BODY).expect("sorted source texts"),
        vec!["first", "second"]
    );
    server.shutdown().await;
}

#[tokio::test]
async fn requesting_a_field_typed_differently_across_indices_in_fields_or_docvalue_fields_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    // The table exists before the first request: the engine caches the catalog provider the
    // first request builds.
    create_exact_name_table(
        &catalog,
        "scans",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, COL_NAME, Type::Primitive(PrimitiveType::Long)),
        ],
    )
    .await
    .expect("create scans");

    for body in [
        json!({"fields": ["name"]}),
        json!({"docvalue_fields": [{"field": "name"}]}),
        json!({"fields": ["na*"]}),
    ] {
        let (status, refused) = post_json(&server, Some(TENANT_A), "/scans,spans/_search", &body).await;
        assert_eq!(status, 400, "{body}: {refused}");
        assert_eq!(refused["error"]["type"], "illegal_argument_exception", "{body}");
        assert_eq!(refused["error"]["field"], COL_NAME, "{body}");
        assert_eq!(
            refused["error"]["indices"],
            json!({"long": ["scans"], "keyword": ["spans"]}),
            "{body}"
        );
    }
    for body in [json!({"fields": ["timestamp"]}), json!({"_source": true})] {
        let (status, answer) = post_json(&server, Some(TENANT_A), "/scans,spans/_search", &body).await;
        assert_eq!(status, 200, "{body}: {answer}");
    }
    let (status, refused) = post_json(
        &server,
        Some(TENANT_A),
        "/spans/_search",
        &json!({"fields": [{"field": "name", "format": "date_time"}]}),
    )
    .await;
    assert_eq!(status, 400, "a format on a keyword field: {refused}");
    assert_eq!(refused["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

#[tokio::test]
async fn terminate_after_reports_not_terminated_early_and_source_false_omits_source() {
    let (server, catalog) = TestServer::start().await;
    seed_two_logs(&catalog).await.expect("seed");

    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"terminate_after": 100, "_source": false}),
    )
    .await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["terminated_early"], false);
    assert_eq!(answer["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    let hits = answer["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 2);
    for hit in hits {
        assert!(hit.get("_source").is_none(), "no _source key: {hit}");
        assert!(hit["_id"].is_string(), "{hit}");
    }

    let (status, plain) = post_json(&server, Some(TENANT_A), "/logs/_search", &json!({})).await;
    assert_eq!(status, 200, "{plain}");
    assert!(
        plain.get("terminated_early").is_none(),
        "only terminate_after adds the key: {plain}"
    );
    server.shutdown().await;
}

/// 2026-10-08T12:00:00Z in microseconds: the base timestamp of the full-text rows.
const FULL_TEXT_BASE_MICROS: i64 = 1_791_460_800_000_000;

/// A log row of the full-text tests. Each row stores its id as the `row` log
/// attribute, so a hit names the row it came from; a `None` body stays NULL.
fn build_full_text_log_row<'a>(
    tenant: &'a str,
    offset_micros: i64,
    body: Option<&'a str>,
    severity_text: &'a str,
    log_attributes: &'a [(&'a str, &'a str)],
) -> Row<'a> {
    let micros = FULL_TEXT_BASE_MICROS + offset_micros;
    let mut cells = vec![
        (COL_TIMESTAMP, Cell::Micros(micros)),
        (COL_OBSERVED_TIMESTAMP, Cell::Micros(micros)),
        (COL_INGESTED_TIMESTAMP, Cell::Micros(micros)),
        (COL_SEVERITY_TEXT, Cell::Text(severity_text)),
        (COL_LOG_ATTRIBUTES, Cell::Map(log_attributes)),
    ];
    if let Some(body) = body {
        cells.push((COL_BODY, Cell::Text(body)));
    }
    Row { tenant, cells }
}

/// Five rows of tenant A and two of tenant B sharing their values: rows 1, 2 and b1
/// in one Iceberg commit, rows 3, 4, 5 and b2 in the WAL segment after it.
async fn seed_full_text_rows(server: &TestServer, catalog: &Arc<dyn iceberg::Catalog>) -> Result<(), RowWriteError> {
    let iceberg_rows = [
        build_full_text_log_row(
            TENANT_A,
            1,
            Some("Connection refused by upstream"),
            "ERROR",
            &[("http.method", "GET"), ("row", "1"), ("user.email", "ann@example.com")],
        ),
        build_full_text_log_row(
            TENANT_A,
            2,
            Some("Retrying connection in 5s"),
            "WARN",
            &[("http.method", "POST"), ("row", "2")],
        ),
        build_full_text_log_row(
            TENANT_B,
            1,
            Some("Connection refused by upstream"),
            "ERROR",
            &[("http.method", "GET"), ("row", "b1")],
        ),
    ];
    let wal_rows = [
        build_full_text_log_row(TENANT_A, 3, Some("user@example.com logged in"), "INFO", &[("row", "3")]),
        build_full_text_log_row(TENANT_A, 4, None, "ERROR", &[("region", "eu-west-1"), ("row", "4")]),
        build_full_text_log_row(TENANT_A, 5, Some("!!!"), "DEBUG", &[("row", "5")]),
        build_full_text_log_row(
            TENANT_B,
            4,
            Some("Retrying connection in 5s"),
            "ERROR",
            &[("region", "eu-west-1"), ("row", "b2")],
        ),
    ];
    commit_rows(catalog, LOGS_TABLE, &iceberg_rows).await?;
    write_wal_segment(
        &server.wal_store,
        catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &wal_rows,
    )
    .await
}

/// The `row` log attribute of each hit: the ids of the rows a search returned.
fn read_full_text_hit_rows(answer: &Value) -> BTreeSet<String> {
    answer["hits"]["hits"]
        .as_array()
        .expect("a hits array")
        .iter()
        .map(|hit| {
            hit["_source"]["log_attributes"]["row"]
                .as_str()
                .expect("each hit names its row")
                .to_owned()
        })
        .collect()
}

fn build_row_ids(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|id| (*id).to_owned()).collect()
}

#[tokio::test]
async fn the_full_text_semantics_table_matches_the_stated_rows() {
    let (server, catalog) = TestServer::start().await;
    seed_full_text_rows(&server, &catalog).await.expect("the full-text rows");
    // Tokens of the bodies: row 1 [connection, refused, by, upstream], row 2 [retrying,
    // connection, in, 5s], row 3 [user, example.com, logged, in], row 4 NULL, row 5 none.
    let table: Vec<(Value, &[&str])> = vec![
        (json!({"match": {"body": "connection"}}), &["1", "2"]),
        (
            json!({"match": {"body": {"query": "connection upstream", "operator": "and"}}}),
            &["1"],
        ),
        (json!({"match": {"body": "user@example.com"}}), &["3"]),
        (json!({"match": {"severity_text": "ERROR"}}), &["1", "4"]),
        (
            json!({"match": {"body": {"query": "!!!", "zero_terms_query": "all"}}}),
            &["1", "2", "3", "4", "5"],
        ),
        (json!({"match_phrase": {"body": "refused by"}}), &["1"]),
        (json!({"match_phrase": {"body": "by refused"}}), &[]),
        (json!({"term": {"body": "connection"}}), &["1", "2"]),
        (json!({"term": {"body": "Connection"}}), &[]),
        (
            json!({"term": {"body": {"value": "CONNECTION", "case_insensitive": true}}}),
            &["1", "2"],
        ),
        (json!({"term": {"severity_text": "error"}}), &[]),
        // `terms` compares each value with the tokens unanalyzed: `Error` matches no token.
        (json!({"terms": {"body": ["upstream", "Error", "logged"]}}), &["1", "3"]),
        (json!({"terms": {"body": ["Error"]}}), &[]),
        (json!({"prefix": {"body": "conn"}}), &["1", "2"]),
        // A prefix or wildcard on a text field is lowercased like the tokens.
        (json!({"prefix": {"body": "Conn"}}), &["1", "2"]),
        (
            json!({"prefix": {"body": {"value": "RETR", "case_insensitive": true}}}),
            &["2"],
        ),
        (json!({"wildcard": {"body": "c*n"}}), &["1", "2"]),
        (json!({"wildcard": {"body": "C*N"}}), &["1", "2"]),
        (json!({"range": {"body": {"gte": "upstream"}}}), &["1", "3"]),
        // Exclusive at the bound: row 1's `upstream` itself is out, row 3's `user` sorts after it.
        (json!({"range": {"body": {"gt": "upstream"}}}), &["3"]),
        // No bound: every row holding at least one token; row 5's `!!!` holds none.
        (json!({"range": {"body": {}}}), &["1", "2", "3"]),
        (json!({"exists": {"field": "body"}}), &["1", "2", "3", "5"]),
        (
            json!({"multi_match": {"query": "GET", "fields": ["log_attributes.http.method", "body"]}}),
            &["1"],
        ),
        (
            json!({"simple_query_string": {"query": "refused | retrying", "fields": ["body"]}}),
            &["1", "2"],
        ),
        (
            json!({"multi_match": {"query": "refused by", "type": "phrase", "fields": ["body"]}}),
            &["1"],
        ),
        (
            json!({"simple_query_string": {"query": "\"refused by\" | retr*", "fields": ["body"]}}),
            &["1", "2"],
        ),
        // A negated text with no token drops its group: it neither excludes nor selects.
        (
            json!({"simple_query_string": {"query": "-!!!", "fields": ["body"]}}),
            &[],
        ),
        (
            json!({"bool": {"must_not": [{"match": {"body": "connection"}}]}}),
            &["3", "4", "5"],
        ),
    ];
    for (query, expected) in table {
        let (status, answer) = post_json(
            &server,
            Some(TENANT_A),
            "/logs/_search",
            &json!({"query": query.clone(), "size": 100}),
        )
        .await;
        assert_eq!(status, 200, "{query}: {answer}");
        assert_eq!(read_full_text_hit_rows(&answer), build_row_ids(expected), "{query}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn a_star_default_matches_any_value_of_a_map_column_including_a_key_only_in_the_wal() {
    let (server, catalog) = TestServer::start().await;
    seed_full_text_rows(&server, &catalog).await.expect("the full-text rows");
    // `region` exists only in the WAL segment, on row 4 and on tenant B's row b2.
    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"multi_match": {"query": "eu-west-1", "fields": ["*"]}}, "size": 100}),
    )
    .await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(read_full_text_hit_rows(&answer), build_row_ids(&["4"]));
    server.shutdown().await;
}

#[tokio::test]
async fn match_phrase_prefix_matches_every_value_sharing_the_prefix_on_the_data_path() {
    let (server, catalog) = TestServer::start().await;
    let bodies: Vec<String> = (0..80).map(|index| format!("deploy step-{index:02}")).collect();
    let ids: Vec<String> = (0..80).map(|index| format!("p{index:02}")).collect();
    let attributes: Vec<[(&str, &str); 1]> = ids.iter().map(|id| [("row", id.as_str())]).collect();
    let rows: Vec<Row<'_>> = bodies
        .iter()
        .zip(&attributes)
        .enumerate()
        .map(|(index, (body, attributes))| {
            let offset = i64::try_from(index).expect("eighty rows");
            build_full_text_log_row(TENANT_A, offset, Some(body.as_str()), "INFO", attributes)
        })
        .collect();
    let other_rows = [
        build_full_text_log_row(TENANT_A, 100, Some("deploy ok"), "INFO", &[("row", "x")]),
        build_full_text_log_row(TENANT_B, 100, Some("deploy step-00"), "INFO", &[("row", "b")]),
    ];
    commit_rows(&catalog, LOGS_TABLE, &rows[..40])
        .await
        .expect("the Iceberg rows commit");
    // Two hot segments after the commit.
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &rows[40..],
    )
    .await
    .expect("the first WAL segment is written");
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 2,
        &other_rows,
    )
    .await
    .expect("the second WAL segment is written");
    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"match_phrase_prefix": {"body": "deploy st"}}, "size": 100}),
    )
    .await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(answer["hits"]["total"], json!({"value": 80, "relation": "eq"}));
    let expected_ids: BTreeSet<String> = ids.iter().cloned().collect();
    assert_eq!(read_full_text_hit_rows(&answer), expected_ids);
    server.shutdown().await;
}

#[tokio::test]
async fn a_zero_token_match_matches_nothing() {
    let (server, catalog) = TestServer::start().await;
    seed_full_text_rows(&server, &catalog).await.expect("the full-text rows");
    for query in [
        json!({"match": {"body": "!!!"}}),
        json!({"match_phrase": {"body": "..."}}),
    ] {
        let (status, answer) = post_json(
            &server,
            Some(TENANT_A),
            "/logs/_search",
            &json!({"query": query.clone(), "size": 100}),
        )
        .await;
        assert_eq!(status, 200, "{query}: {answer}");
        assert_eq!(answer["hits"]["total"]["value"], json!(0), "{query}");
        assert_eq!(read_full_text_hit_rows(&answer), BTreeSet::new(), "{query}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn an_unqualified_term_is_lenient_per_field() {
    let (server, catalog) = TestServer::start().await;
    seed_full_text_rows(&server, &catalog).await.expect("the full-text rows");
    // Under the all-fields default, `ERROR` is not a date or a hex id: those fields add
    // nothing, and the keyword `severity_text` matches rows 1 and 4.
    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"simple_query_string": {"query": "ERROR"}}, "size": 100}),
    )
    .await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(read_full_text_hit_rows(&answer), build_row_ids(&["1", "4"]));
    // Named fields make the leaf strict: `ERROR` on the hex id `trace_id` is a 400.
    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"multi_match": {"query": "ERROR", "fields": ["trace_id", "severity_text"]}}}),
    )
    .await;
    assert_eq!(status, 400, "{answer}");
    assert_eq!(answer["error"]["type"], "search_phase_execution_exception");
    assert_eq!(answer["error"]["root_cause"][0]["type"], "query_shard_exception");
    server.shutdown().await;
}

#[tokio::test]
async fn a_pattern_selecting_some_keys_of_a_map_column_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    seed_full_text_rows(&server, &catalog).await.expect("the full-text rows");
    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"multi_match": {"query": "GET", "fields": ["log_attributes.http*"]}}}),
    )
    .await;
    assert_eq!(status, 400, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

/// Ingest moves `service.name` out of the resource attributes into `service_name`, so a
/// pattern over every resource attribute reaches that column too.
#[tokio::test]
async fn a_resource_attributes_pattern_reaches_the_promoted_service_name_column() {
    let (server, catalog) = TestServer::start().await;
    let mut rows = [
        build_full_text_log_row(TENANT_A, 1, Some("charged"), "INFO", &[("row", "1")]),
        build_full_text_log_row(TENANT_A, 2, Some("charged"), "INFO", &[("row", "2")]),
        build_full_text_log_row(TENANT_B, 1, Some("charged"), "INFO", &[("row", "b1")]),
    ];
    for (row, service_name) in rows.iter_mut().zip(["checkout", "billing", "checkout"]) {
        row.cells.push((COL_SERVICE_NAME, Cell::Text(service_name)));
    }
    commit_rows(&catalog, LOGS_TABLE, &rows).await.expect("the Iceberg rows commit");
    let (status, answer) = post_json(
        &server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({
            "query": {"multi_match": {"query": "checkout", "fields": ["resource_attributes.*"]}},
            "size": 100
        }),
    )
    .await;
    assert_eq!(status, 200, "{answer}");
    assert_eq!(read_full_text_hit_rows(&answer), build_row_ids(&["1"]));
    server.shutdown().await;
}

/// A log row of the full-text tests carrying a trace id and a span id.
fn build_traced_log_row<'a>(
    tenant: &'a str,
    offset_micros: i64,
    trace_id: &'a [u8; 16],
    span_id: &'a [u8; 8],
    log_attributes: &'a [(&'a str, &'a str)],
) -> Row<'a> {
    let mut row = build_full_text_log_row(tenant, offset_micros, Some("request handled"), "INFO", log_attributes);
    row.cells.push((COL_TRACE_ID, Cell::Bytes(trace_id)));
    row.cells.push((COL_SPAN_ID, Cell::Bytes(span_id)));
    row
}

/// Searches tenant A's `logs` with a `query_string` holding `text`, and any further leaf keys.
async fn search_query_string(server: &TestServer, text: &str, options: &Map<String, Value>) -> (u16, Value) {
    let mut leaf = options.clone();
    leaf.insert(String::from("query"), Value::String(text.to_owned()));
    post_json(
        server,
        Some(TENANT_A),
        "/logs/_search",
        &json!({"query": {"query_string": leaf}, "size": 100}),
    )
    .await
}

#[tokio::test]
async fn a_query_string_syntax_error_is_a_search_phase_exception_with_a_query_shard_root_cause() {
    let (server, catalog) = TestServer::start().await;
    seed_full_text_rows(&server, &catalog).await.expect("the full-text rows");
    let (status, body) = search_query_string(&server, "a/b", &Map::new()).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["status"], json!(400));
    assert_eq!(body["error"]["type"], "search_phase_execution_exception");
    assert_eq!(body["error"]["phase"], "query");
    assert_eq!(body["error"]["grouped"], json!(true));
    assert_eq!(body["error"]["root_cause"][0]["type"], "query_shard_exception");
    assert_eq!(body["error"]["root_cause"][0]["index"], "logs");
    // Fuzzy syntax is refused while the request is parsed, before any index is read.
    let (status, body) = search_query_string(&server, "roam~", &Map::new()).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["type"], "parsing_exception");
    assert_eq!(body["error"]["construct"], "fuzzy");
    server.shutdown().await;
}

#[tokio::test]
async fn a_trace_to_logs_style_unqualified_span_and_trace_pair_matches_the_log_carrying_both() {
    let (server, catalog) = TestServer::start().await;
    let trace = [0x11_u8; 16];
    let other_trace = [0x44_u8; 16];
    let span = [0x22_u8; 8];
    let other_span = [0x33_u8; 8];
    let iceberg_rows = [
        build_traced_log_row(TENANT_A, 1, &trace, &span, &[("row", "both")]),
        build_traced_log_row(TENANT_A, 2, &trace, &other_span, &[("row", "trace-only")]),
        build_traced_log_row(TENANT_B, 1, &trace, &span, &[("row", "other-tenant")]),
    ];
    let wal_rows = [
        build_traced_log_row(TENANT_A, 3, &other_trace, &span, &[("row", "span-only")]),
        build_traced_log_row(TENANT_B, 4, &trace, &span, &[("row", "other-tenant-wal")]),
    ];
    commit_rows(&catalog, LOGS_TABLE, &iceberg_rows)
        .await
        .expect("the Iceberg rows commit");
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &wal_rows,
    )
    .await
    .expect("the WAL segment is written");
    // Unqualified quoted ids under the all-fields default: each one matches the id field of its
    // width and nothing else.
    let query = format!("\"{}\" AND \"{}\"", "22".repeat(8), "11".repeat(16));
    let (status, body) = search_query_string(&server, &query, &Map::new()).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(read_full_text_hit_rows(&body), build_row_ids(&["both"]));
    server.shutdown().await;
}

#[tokio::test]
async fn a_lucene_filter_on_a_dotted_map_key_matches_its_stored_key() {
    let (server, catalog) = TestServer::start().await;
    let iceberg_rows = [
        build_full_text_log_row(
            TENANT_A,
            1,
            Some("GET handled"),
            "INFO",
            &[("http", "x"), ("http.method", "GET"), ("row", "d1")],
        ),
        build_full_text_log_row(
            TENANT_A,
            2,
            Some("POST handled"),
            "INFO",
            &[("http.method", "POST"), ("row", "d2")],
        ),
        build_full_text_log_row(
            TENANT_B,
            1,
            Some("GET handled"),
            "INFO",
            &[("http", "x"), ("http.method", "GET"), ("row", "b1")],
        ),
    ];
    let wal_rows = [
        build_full_text_log_row(
            TENANT_A,
            3,
            Some("GET handled"),
            "INFO",
            &[("http", "y"), ("http.method", "GET"), ("row", "d3")],
        ),
        build_full_text_log_row(
            TENANT_B,
            2,
            Some("GET handled"),
            "INFO",
            &[("http.method", "GET"), ("row", "b2")],
        ),
    ];
    commit_rows(&catalog, LOGS_TABLE, &iceberg_rows)
        .await
        .expect("the Iceberg rows commit");
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &wal_rows,
    )
    .await
    .expect("the WAL segment is written");
    let rows: [(String, &[&str]); 2] = [
        (format!("{COL_LOG_ATTRIBUTES}.http.method:GET"), &["d1", "d3"]),
        (format!("{COL_LOG_ATTRIBUTES}.http:x"), &["d1"]),
    ];
    for (query, expected) in rows {
        let (status, body) = search_query_string(&server, &query, &Map::new()).await;
        assert_eq!(status, 200, "{query}: {body}");
        assert_eq!(read_full_text_hit_rows(&body), build_row_ids(expected), "{query}");
    }
    server.shutdown().await;
}

/// Zero-token clauses are dropped before the occurrence rules apply: a query left with no clause
/// matches nothing, and a dropped clause does not change what its neighbour matches.
#[tokio::test]
async fn query_string_zero_token_clauses_match_the_stated_rows() {
    let (server, catalog) = TestServer::start().await;
    seed_full_text_rows(&server, &catalog).await.expect("the full-text rows");
    // An empty `severity_text` in each tenant, in one batch, with no body.
    let empty_severity_rows = [
        build_full_text_log_row(TENANT_A, 6, None, "", &[("row", "e1")]),
        build_full_text_log_row(TENANT_B, 6, None, "", &[("row", "be1")]),
    ];
    commit_rows(&catalog, LOGS_TABLE, &empty_severity_rows)
        .await
        .expect("the empty-severity rows commit");
    // `connection` is a token of rows 1 and 2 (and of tenant B's rows); `!!!` yields no token.
    let rows: [(String, &[&str]); 5] = [
        (format!("{COL_BODY}:\"!!!\""), &[]),
        (format!("-{COL_BODY}:\"!!!\""), &[]),
        (format!("connection AND {COL_BODY}:\"!!!\""), &["1", "2"]),
        (format!("connection AND ({COL_BODY}:\"!!!\")"), &["1", "2"]),
        // An empty whole value on the keyword-like fields: only row e1's `severity_text` is one.
        (String::from("\"\""), &["e1"]),
    ];
    for (query, expected) in rows {
        let (status, body) = search_query_string(&server, &query, &Map::new()).await;
        assert_eq!(status, 200, "{query}: {body}");
        assert_eq!(read_full_text_hit_rows(&body), build_row_ids(expected), "{query}");
    }
    server.shutdown().await;
}

/// How deeply `query_string` groups may nest: the limit the query process accepts.
const QUERY_STRING_DEPTH_LIMIT: usize = 128;

/// Groups nested to the limit go through the whole data path (translation, resolution, lowering
/// and planning) and answer their rows; one level more is refused before anything is read.
#[tokio::test]
async fn query_string_groups_nested_to_the_limit_answer_the_stated_rows_and_one_more_level_is_a_400() {
    let (server, catalog) = TestServer::start().await;
    seed_full_text_rows(&server, &catalog).await.expect("the full-text rows");
    let nest_groups = |depth: usize| format!("{}{COL_BODY}:connection{}", "(".repeat(depth), ")".repeat(depth));
    // A left-deep chain `((connection OR logged) AND NOT refused) ...`: each level wraps the
    // previous one with one more clause, cycling through four steps. Over the body tokens of
    // tenant A (connection: 1, 2; logged: 3; refused: 1; upstream: 1; in: 2, 3) the first cycle
    // goes {1, 2} -> {1, 2, 3} -> {2, 3} -> {1, 2, 3} -> {2, 3}, and every later cycle returns
    // to {2, 3}, so the 128 levels (32 whole cycles) end at {2, 3}.
    let chain_steps = [
        format!("OR {COL_BODY}:logged"),
        format!("AND NOT {COL_BODY}:refused"),
        format!("OR {COL_BODY}:upstream"),
        format!("AND {COL_BODY}:in"),
    ];
    let left_deep_chain = chain_steps
        .iter()
        .cycle()
        .take(QUERY_STRING_DEPTH_LIMIT)
        .fold(format!("{COL_BODY}:connection"), |chain, step| {
            format!("({chain} {step})")
        });
    let rows: [(String, &[&str]); 2] = [
        (nest_groups(QUERY_STRING_DEPTH_LIMIT), &["1", "2"]),
        (left_deep_chain, &["2", "3"]),
    ];
    for (query, expected) in rows {
        let (status, body) = search_query_string(&server, &query, &Map::new()).await;
        assert_eq!(status, 200, "{query}: {body}");
        assert_eq!(read_full_text_hit_rows(&body), build_row_ids(expected), "{query}");
    }
    let (status, body) = search_query_string(&server, &nest_groups(QUERY_STRING_DEPTH_LIMIT + 1), &Map::new()).await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["error"]["type"], "parsing_exception");
    assert_eq!(body["error"]["limit"], json!(QUERY_STRING_DEPTH_LIMIT));
    server.shutdown().await;
}

/// `time_zone` reads an unquoted date term and a range bound on a named date field in the zone,
/// and a quoted term in UTC. The rows sit on both sides of the Asia/Dubai (UTC+4) day 2026-10-08,
/// which runs from 2026-10-07T20:00:00Z to 2026-10-08T20:00:00Z.
#[tokio::test]
async fn query_string_time_zone_reads_date_terms_and_ranges_as_stated() {
    const HOUR_MICROS: i64 = 3_600_000_000;
    const MILLI_MICROS: i64 = 1_000;
    let (server, catalog) = TestServer::start().await;
    // Offsets from 2026-10-08T12:00:00Z.
    let before_dubai_day = -16 * HOUR_MICROS - MILLI_MICROS;
    let dubai_day_start = -16 * HOUR_MICROS;
    let dubai_day_end = 8 * HOUR_MICROS - MILLI_MICROS;
    let after_dubai_day = 8 * HOUR_MICROS;
    let after_utc_day = 12 * HOUR_MICROS;
    let iceberg_rows = [
        build_full_text_log_row(TENANT_A, before_dubai_day, Some("tick"), "INFO", &[("row", "t1")]),
        build_full_text_log_row(TENANT_A, dubai_day_start, Some("tick"), "INFO", &[("row", "t2")]),
        build_full_text_log_row(TENANT_A, dubai_day_end, Some("tick"), "INFO", &[("row", "t3")]),
        build_full_text_log_row(TENANT_B, dubai_day_start, Some("tick"), "INFO", &[("row", "b2")]),
    ];
    let wal_rows = [
        build_full_text_log_row(TENANT_A, after_dubai_day, Some("tick"), "INFO", &[("row", "t4")]),
        build_full_text_log_row(TENANT_A, after_utc_day, Some("tick"), "INFO", &[("row", "t5")]),
        build_full_text_log_row(TENANT_B, dubai_day_end, Some("tick"), "INFO", &[("row", "b3")]),
    ];
    commit_rows(&catalog, LOGS_TABLE, &iceberg_rows)
        .await
        .expect("the Iceberg rows commit");
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &wal_rows,
    )
    .await
    .expect("the WAL segment is written");
    let mut in_dubai = Map::new();
    in_dubai.insert(String::from("time_zone"), json!("Asia/Dubai"));
    let rows: [(String, &[&str]); 3] = [
        // The Dubai day.
        (format!("{COL_TIMESTAMP}:2026-10-08"), &["t2", "t3"]),
        // Quoted: the UTC day, 2026-10-08T00:00:00Z to 2026-10-09T00:00:00Z.
        (format!("{COL_TIMESTAMP}:\"2026-10-08\""), &["t3", "t4"]),
        // From the start of the Dubai day on.
        (format!("{COL_TIMESTAMP}:[2026-10-08 TO *]"), &["t2", "t3", "t4", "t5"]),
    ];
    for (query, expected) in rows {
        let (status, body) = search_query_string(&server, &query, &in_dubai).await;
        assert_eq!(status, 200, "{query}: {body}");
        assert_eq!(read_full_text_hit_rows(&body), build_row_ids(expected), "{query}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn query_string_on_a_system_index_matches_the_stated_documents() {
    let (server, _catalog) = TestServer::start().await;
    let mappings = json!({
        "properties": {"type": {"type": "keyword"}, "index-pattern": {"properties": {"title": {"type": "text"}}}}
    });
    for tenant in [TENANT_A, TENANT_B] {
        let response = server
            .request(Method::PUT, Some(tenant), "/.kibana_7")
            .json(&json!({"mappings": mappings.clone(), "settings": {"number_of_shards": 1}}))
            .send()
            .await
            .expect("the index creation answers");
        assert_eq!(response.status().as_u16(), 200, "{tenant}");
    }
    let documents = [
        (
            TENANT_A,
            "index-pattern%3Alogs",
            json!({"type": "index-pattern", "index-pattern": {"title": "logs-*"}}),
        ),
        (
            TENANT_A,
            "index-pattern%3Aotel",
            json!({"type": "index-pattern", "index-pattern": {"title": "logs-otel"}}),
        ),
        (
            TENANT_A,
            "index-pattern%3Ametrics",
            json!({"type": "index-pattern", "index-pattern": {"title": "metrics-*"}}),
        ),
        (TENANT_A, "config%3A3.9.0", json!({"type": "config"})),
        (
            TENANT_B,
            "index-pattern%3Alogs-b",
            json!({"type": "index-pattern", "index-pattern": {"title": "logs-b"}}),
        ),
    ];
    for (tenant, id, source) in documents {
        let response = server
            .request(Method::PUT, Some(tenant), &format!("/.kibana_7/_doc/{id}"))
            .json(&source)
            .send()
            .await
            .expect("the document write answers");
        assert_eq!(response.status().as_u16(), 201, "{id}");
    }
    let rows: [(&str, &[&str]); 3] = [
        (
            "index-pattern.title:logs*",
            &["index-pattern:logs", "index-pattern:otel"],
        ),
        (
            "type:index-pattern AND NOT index-pattern.title:metrics",
            &["index-pattern:logs", "index-pattern:otel"],
        ),
        ("type:config", &["config:3.9.0"]),
    ];
    for (query, expected) in rows {
        let (status, body) = post_json(
            &server,
            Some(TENANT_A),
            "/.kibana_7/_search",
            &json!({"query": {"query_string": {"query": query}}, "size": 100}),
        )
        .await;
        assert_eq!(status, 200, "{query}: {body}");
        let ids: BTreeSet<String> = body["hits"]["hits"]
            .as_array()
            .expect("a hits array")
            .iter()
            .map(|hit| hit["_id"].as_str().expect("an id").to_owned())
            .collect();
        assert_eq!(ids, build_row_ids(expected), "{query}");
    }
    server.shutdown().await;
}

/// A system index answers a malformed pattern before reading any document: the refusal does not
/// depend on which `bool` clause a document fails first, nor on whether the index holds any. A
/// `/re/` term on a mapped field is checked in the form that field matches it.
#[tokio::test]
async fn a_malformed_regexp_on_a_system_index_is_a_400_whatever_the_clause_order_or_documents() {
    let (server, _catalog) = TestServer::start().await;
    let mappings = json!({
        "properties": {"title": {"type": "text", "fields": {"keyword": {"type": "keyword"}}}}
    });
    for index in ["/.kibana_1", "/.kibana_2"] {
        let response = server
            .request(Method::PUT, Some(TENANT_A), index)
            .json(&json!({"mappings": mappings.clone()}))
            .send()
            .await
            .expect("the index creation answers");
        assert_eq!(response.status().as_u16(), 200, "{index}");
    }
    // `.kibana_1` holds one document, which the `term` clause does not match; `.kibana_2` none.
    let response = server
        .request(Method::PUT, Some(TENANT_A), "/.kibana_1/_doc/dashboard%3A1")
        .json(&json!({"title": "abc"}))
        .send()
        .await
        .expect("the document write answers");
    assert_eq!(response.status().as_u16(), 201);
    let (status, body) = post_json(
        &server,
        Some(TENANT_A),
        "/.kibana_1/_count",
        &json!({"query": {"match_all": {}}}),
    )
    .await;
    assert_eq!((status, body["count"].clone()), (200, json!(1)), "{body}");
    let term_clause = json!({"term": {"title.keyword": "nope"}});
    let regexp_clause = json!({"regexp": {"title": "(a"}});
    let queries = [
        json!({"bool": {"must": [term_clause.clone(), regexp_clause.clone()]}}),
        json!({"bool": {"must": [regexp_clause, term_clause]}}),
        json!({"query_string": {"query": "title:/(a/"}}),
    ];
    for path in [
        "/.kibana_1/_search",
        "/.kibana_2/_search",
        "/.kibana_1/_count",
        "/.kibana_2/_count",
    ] {
        for query in &queries {
            let (status, body) = post_json(&server, Some(TENANT_A), path, &json!({"query": query})).await;
            assert_eq!(status, 400, "{path} {query}: {body}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{path} {query}");
        }
    }
    // A `/re/` term is lowercased on the text field `title` and read as written on the keyword
    // `title.keyword`: `[a-Z]` is valid only lowercased (`[a-z]`), `[Z-a]` only as written. The
    // count is `.kibana_1`'s (`abc`); `.kibana_2` holds no document, and refuses alike. A term with
    // no field prefix selects `*`, which matches both declared fields, so it is refused in either
    // invalid form even where `title` matches `abc` first.
    let case_rows = [
        ("title:/[a-Z]+/", Some(1)),
        ("title:/[Z-a]/", None),
        ("title.keyword:/[a-Z]+/", None),
        ("title.keyword:/[Z-a]+/", Some(0)),
        ("/[a-Z]+/", None),
        ("/[Z-a]+/", None),
    ];
    for (query_text, kibana_1_count) in case_rows {
        let query = json!({"query_string": {"query": query_text}});
        for (index, expected_count) in [(".kibana_1", kibana_1_count), (".kibana_2", kibana_1_count.map(|_| 0))] {
            let (search_status, search_body) = post_json(
                &server,
                Some(TENANT_A),
                &format!("/{index}/_search"),
                &json!({"query": query}),
            )
            .await;
            let (count_status, count_body) = post_json(
                &server,
                Some(TENANT_A),
                &format!("/{index}/_count"),
                &json!({"query": query}),
            )
            .await;
            match expected_count {
                Some(count) => {
                    assert_eq!(
                        (search_status, search_body["hits"]["total"]["value"].clone()),
                        (200, json!(count)),
                        "{index} {query_text}: {search_body}"
                    );
                    assert_eq!(
                        (count_status, count_body["count"].clone()),
                        (200, json!(count)),
                        "{index} {query_text}: {count_body}"
                    );
                }
                None => {
                    for (status, body) in [(search_status, &search_body), (count_status, &count_body)] {
                        assert_eq!(status, 400, "{index} {query_text}: {body}");
                        assert_eq!(
                            body["error"]["type"], "illegal_argument_exception",
                            "{index} {query_text}"
                        );
                    }
                }
            }
        }
    }
    server.shutdown().await;
}

/// Three rows of tenant A and two of tenant B sharing their values: rows 1, 2 and b1 in one
/// Iceberg commit, rows 3 and b2 in the WAL segment after it.
async fn seed_regex_rows(server: &TestServer, catalog: &Arc<dyn iceberg::Catalog>) -> Result<(), RowWriteError> {
    let iceberg_rows = [
        build_full_text_log_row(TENANT_A, 1, Some("Connection Refused"), "ERROR", &[("row", "1")]),
        build_full_text_log_row(TENANT_A, 2, Some("retrying connection"), "error", &[("row", "2")]),
        build_full_text_log_row(TENANT_B, 1, Some("Connection Refused"), "ERROR", &[("row", "b1")]),
    ];
    let wal_rows = [
        build_full_text_log_row(TENANT_A, 3, Some("Error budget"), "WARN", &[("row", "3")]),
        build_full_text_log_row(TENANT_B, 3, Some("retrying connection"), "error", &[("row", "b2")]),
    ];
    commit_rows(catalog, LOGS_TABLE, &iceberg_rows).await?;
    write_wal_segment(
        &server.wal_store,
        catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &wal_rows,
    )
    .await
}

/// Searches tenant A's `logs` with `query` and checks the rows each answer names.
async fn assert_regex_rows(server: &TestServer, table: &[(Value, &[&str])]) {
    for (query, expected_rows) in table {
        let (status, body) = post_json(
            server,
            Some(TENANT_A),
            "/logs/_search",
            &json!({"query": query, "size": 100}),
        )
        .await;
        assert_eq!(status, 200, "{query}: {body}");
        assert_eq!(read_full_text_hit_rows(&body), build_row_ids(expected_rows), "{query}");
    }
}

#[tokio::test]
async fn a_query_string_regex_on_a_keyword_field_matches_verbatim() {
    let (server, catalog) = TestServer::start().await;
    seed_regex_rows(&server, &catalog).await.expect("the regex rows");
    // Severity: row 1 ERROR, row 2 error, row 3 WARN; rows: 1, 2, 3 as `log_attributes.row`.
    assert_regex_rows(
        &server,
        &[
            (json!({"query_string": {"query": "severity_text:/ERR.*/"}}), &["1"]),
            (json!({"query_string": {"query": "severity_text:/err.*/"}}), &["2"]),
            (json!({"regexp": {"severity_text": "ERR.*"}}), &["1"]),
            (
                json!({"regexp": {"severity_text": {"value": "err.*", "case_insensitive": true}}}),
                &["1", "2"],
            ),
            (json!({"regexp": {"log_attributes.row": "[12]"}}), &["1", "2"]),
            (
                json!({"query_string": {"query": "log_attributes.\\*:/[23]/"}}),
                &["2", "3"],
            ),
        ],
    )
    .await;
    server.shutdown().await;
}

#[tokio::test]
async fn a_regexp_on_a_text_field_matches_per_token_without_normalizing_the_pattern() {
    let (server, catalog) = TestServer::start().await;
    seed_regex_rows(&server, &catalog).await.expect("the regex rows");
    // Tokens: row 1 [connection, refused], row 2 [retrying, connection], row 3 [error, budget].
    assert_regex_rows(
        &server,
        &[
            (json!({"regexp": {"body": "conn.*"}}), &["1", "2"]),
            (json!({"regexp": {"body": "Conn.*"}}), &[]),
            (
                json!({"regexp": {"body": {"value": "Conn.*", "case_insensitive": true}}}),
                &["1", "2"],
            ),
            (json!({"regexp": {"body": "err.*"}}), &["3"]),
            (json!({"regexp": {"body": "connection refused"}}), &[]),
            (json!({"query_string": {"query": "body:/Conn.*/"}}), &["1", "2"]),
        ],
    )
    .await;
    server.shutdown().await;
}

#[tokio::test]
async fn a_regexp_on_an_id_or_numeric_field_is_refused_and_skipped_under_lenient() {
    let (server, catalog) = TestServer::start().await;
    seed_regex_rows(&server, &catalog).await.expect("the regex rows");
    // The logs table has no numeric column; `duration_micros` of `spans` is a long. One span
    // of each tenant, its trace id `11` repeated, so its hex form matches `11.*` as text, and
    // its duration 0, so its decimal form matches `0.*` as text.
    let trace_id = [0x11_u8; 16];
    let span_rows: Vec<Row<'_>> = [TENANT_A, TENANT_B]
        .into_iter()
        .map(|tenant| {
            let mut row = build_span_row(tenant, FULL_TEXT_BASE_MICROS, &trace_id, "GET /orders");
            row.cells.push((COL_DURATION_MICROS, Cell::Long(0)));
            row
        })
        .collect();
    commit_rows(&catalog, SPANS_TABLE, &span_rows).await.expect("the spans commit");
    let strict_rows = [
        ("/logs/_search", json!({"regexp": {"trace_id": "11.*"}})),
        (
            "/logs/_search",
            json!({"query_string": {"query": "trace_id:/11.*/", "default_field": "body"}}),
        ),
        ("/spans/_search", json!({"regexp": {"duration_micros": "0.*"}})),
        (
            "/spans/_search",
            json!({"query_string": {"query": "duration_micros:/0.*/", "default_field": "name"}}),
        ),
    ];
    for (path, strict_query) in strict_rows {
        let (status, body) = post_json(&server, Some(TENANT_A), path, &json!({"query": strict_query.clone()})).await;
        assert_eq!(status, 400, "{strict_query}: {body}");
        assert_eq!(
            body["error"]["type"], "search_phase_execution_exception",
            "{strict_query}"
        );
        assert_eq!(
            body["error"]["root_cause"][0]["type"], "query_shard_exception",
            "{strict_query}"
        );
    }
    // Under the all-fields default the query string is lenient: the id and the numeric field
    // add nothing, while the span itself is visible to tenant A.
    let lenient_rows = [
        (json!({"query_string": {"query": "trace_id:/11.*/"}}), 0),
        (json!({"query_string": {"query": "duration_micros:/0.*/"}}), 0),
        (json!({"match_all": {}}), 1),
    ];
    for (lenient_query, expected_total) in lenient_rows {
        let (status, body) = post_json(
            &server,
            Some(TENANT_A),
            "/spans/_search",
            &json!({"query": lenient_query.clone(), "size": 100}),
        )
        .await;
        assert_eq!(status, 200, "{lenient_query}: {body}");
        assert_eq!(body["hits"]["total"]["value"], json!(expected_total), "{lenient_query}");
    }
    server.shutdown().await;
}

/// The pattern is translated before the field is resolved, so a malformed one is a 400 even on
/// a field the index lacks; an enabled operator the dialect does not translate is named.
#[tokio::test]
async fn a_malformed_or_untranslatable_regexp_is_a_400_whatever_field_it_names() {
    let (server, catalog) = TestServer::start().await;
    seed_regex_rows(&server, &catalog).await.expect("the regex rows");
    for (query, expected_type, construct) in [
        (
            json!({"regexp": {"no_such_field": "(a"}}),
            "illegal_argument_exception",
            Value::Null,
        ),
        (json!({"regexp": {"body": "a&b"}}), "parsing_exception", json!("&")),
        (
            json!({"query_string": {"query": "severity_text:/~a/"}}),
            "parsing_exception",
            json!("~"),
        ),
    ] {
        let (status, body) = post_json(
            &server,
            Some(TENANT_A),
            "/logs/_search",
            &json!({"query": query.clone()}),
        )
        .await;
        assert_eq!(status, 400, "{query}: {body}");
        assert_eq!(body["error"]["type"], expected_type, "{query}");
        assert_eq!(body["error"]["construct"], construct, "{query}");
    }
    server.shutdown().await;
}
