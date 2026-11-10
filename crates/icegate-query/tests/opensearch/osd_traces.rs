//! The requests `OpenSearch` Dashboards 3.9's Traces table sends to `otel-v1-apm-span`, over real
//! HTTP, against the rows of the Grafana trace fixture. Every expected value is stated here.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::SPANS_TABLE;
use icegate_common::schema::{COL_NAME, COL_PARENT_SPAN_ID};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{Cell, RowWriteError, T0, TENANT_A, build_span_row, commit_rows};
use super::grafana_traces::{commit_trace_rows, search_span_index};
use super::harness::TestServer;
use super::requests::send_request;

/// OSD 3.9's `last_updated` script, with the whitespace it is sent with.
const LAST_UPDATED_SOURCE: &str = "\n                  if (doc.containsKey('traceGroupFields.endTime') && !doc['traceGroupFields.endTime'].empty) {\n                    return doc['traceGroupFields.endTime'].value;\n                  }\n                  if (doc.containsKey('endTime') && !doc['endTime'].empty) {\n                    return doc['endTime'].value;\n                  }\n                ";

/// Trace `0f…` of tenant A, beside the Grafana trace fixture: a root starting at `T0 + 50 000 µs`
/// and lasting `10 000 µs`, and an asynchronous child starting at `T0 + 55 000 µs` and lasting
/// `100 000 µs`, so the child ends after its root.
async fn commit_async_child_trace(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    const TRACE_ASYNC: [u8; 16] = [0x0f; 16];
    let rows = vec![
        build_span_row(
            TENANT_A,
            &TRACE_ASYNC,
            &[0x51; 8],
            (T0 + 50_000, 10_000),
            vec![(COL_NAME, Cell::Text("POST /enqueue"))],
        ),
        build_span_row(
            TENANT_A,
            &TRACE_ASYNC,
            &[0x52; 8],
            (T0 + 55_000, 100_000),
            vec![
                (COL_PARENT_SPAN_ID, Cell::Bytes(&[0x51; 8])),
                (COL_NAME, Cell::Text("process job")),
            ],
        ),
    ];
    commit_rows(catalog, SPANS_TABLE, &rows).await
}

/// The traces in key order, each with the `max` of the `last_updated` script.
fn build_last_updated_body() -> Value {
    json!({
        "size": 0,
        "aggs": {"traces": {
            "terms": {"field": "traceId", "size": 1000, "order": {"_key": "asc"}},
            "aggs": {
                "last_updated": {"max": {"script": {"source": LAST_UPDATED_SOURCE, "lang": "painless"}}}
            }
        }}
    })
}

/// Expected values derived by hand from the fixtures: trace A's root ends at `T0 + 152 175 µs`
/// (its child ends earlier, at `T0 + 51 000 µs`), epoch millisecond `1 760 000 000 152`; trace B's
/// root ends at `T0 + 10 000 + 735 519 µs`, epoch millisecond `1 760 000 000 745`; trace C has no
/// stored root, so its one span's own end, `T0 + 30 000 + 2 000 µs`, epoch millisecond
/// `1 760 000 000 032`, answers; trace `0f…`'s root ends at `T0 + 60 000 µs`, epoch millisecond
/// `1 760 000 000 060`, and every span of it reads that, so its child's later end
/// (`T0 + 155 000 µs`) never answers.
///
/// A `max` with only a script reads a numeric values source whose format is raw, so it answers
/// `value` alone, without `value_as_string`, even for the date the script returns.
#[tokio::test]
async fn last_updated_reads_the_root_end_time_and_falls_back_to_the_span_end_time() {
    let (server, catalog) = TestServer::start().await;
    commit_trace_rows(&catalog).await.expect("commit traces");
    commit_async_child_trace(&catalog).await.expect("commit the async-child trace");

    let item = search_span_index(&server, &build_last_updated_body())
        .await
        .expect("last updated");
    let buckets = &item["aggregations"]["traces"]["buckets"];

    assert_eq!(buckets[0]["key"], "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a");
    assert_eq!(buckets[0]["last_updated"], json!({"value": 1_760_000_000_152.0}));
    assert_eq!(buckets[1]["key"], "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b");
    assert_eq!(buckets[1]["last_updated"], json!({"value": 1_760_000_000_745.0}));
    assert_eq!(buckets[2]["key"], "0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d");
    assert_eq!(buckets[2]["last_updated"], json!({"value": 1_760_000_000_032.0}));
    assert_eq!(buckets[3]["key"], "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f");
    assert_eq!(buckets[3]["last_updated"], json!({"value": 1_760_000_000_060.0}));
    assert_eq!(buckets.as_array().map(Vec::len), Some(4), "{item}");
    server.shutdown().await;
}

/// The last-updated script reads `traceGroupFields`, which only the span index computes: on the
/// native `spans` index it is refused, naming the aggregation and the index.
#[tokio::test]
async fn the_last_updated_script_on_a_native_index_is_refused() {
    let (server, catalog) = TestServer::start().await;
    commit_trace_rows(&catalog).await.expect("commit traces");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/spans/_search",
        Some(&json!({"size": 0, "aggs": {"last_updated": {"max": {"script": {"source": LAST_UPDATED_SOURCE, "lang": "painless"}}}}})),
    )
    .await
    .expect("native search");

    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(answer["error"]["aggregation"], "last_updated", "{answer}");
    assert_eq!(answer["error"]["index"], "spans", "{answer}");
    server.shutdown().await;
}
