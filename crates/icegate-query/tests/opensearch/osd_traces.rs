//! The requests `OpenSearch` Dashboards 3.9's Traces table sends to `otel-v1-apm-span`, over real
//! HTTP, against the rows of the Grafana trace fixture. Every expected value is stated here.
#![allow(clippy::expect_used)]

use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::TENANT_A;
use super::grafana_traces::{commit_trace_rows, search_span_index};
use super::harness::TestServer;
use super::requests::send_request;

/// OSD 3.9's `last_updated` script, with the whitespace it is sent with.
const LAST_UPDATED_SOURCE: &str = "\n                  if (doc.containsKey('traceGroupFields.endTime') && !doc['traceGroupFields.endTime'].empty) {\n                    return doc['traceGroupFields.endTime'].value;\n                  }\n                  if (doc.containsKey('endTime') && !doc['endTime'].empty) {\n                    return doc['endTime'].value;\n                  }\n                ";

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

/// Expected values derived by hand from the fixture: trace A's root ends at `T0 + 152 175 µs`
/// (its child ends earlier, at `T0 + 51 000 µs`), epoch millisecond `1 760 000 000 152`; trace B's
/// root ends at `T0 + 10 000 + 735 519 µs`, epoch millisecond `1 760 000 000 745`; trace C has no
/// stored root, so its one span's own end, `T0 + 30 000 + 2 000 µs`, epoch millisecond
/// `1 760 000 000 032`, answers.
#[tokio::test]
async fn last_updated_reads_the_root_end_time_and_falls_back_to_the_span_end_time() {
    let (server, catalog) = TestServer::start().await;
    commit_trace_rows(&catalog).await.expect("commit traces");

    let item = search_span_index(&server, &build_last_updated_body())
        .await
        .expect("last updated");
    let buckets = &item["aggregations"]["traces"]["buckets"];

    assert_eq!(buckets[0]["key"], "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a");
    assert_eq!(buckets[0]["last_updated"]["value"], json!(1_760_000_000_152.0));
    assert_eq!(
        buckets[0]["last_updated"]["value_as_string"],
        "2025-10-09T08:53:20.152Z"
    );
    assert_eq!(buckets[1]["key"], "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b");
    assert_eq!(buckets[1]["last_updated"]["value"], json!(1_760_000_000_745.0));
    assert_eq!(
        buckets[1]["last_updated"]["value_as_string"],
        "2025-10-09T08:53:20.745Z"
    );
    assert_eq!(buckets[2]["key"], "0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d");
    assert_eq!(buckets[2]["last_updated"]["value"], json!(1_760_000_000_032.0));
    assert_eq!(
        buckets[2]["last_updated"]["value_as_string"],
        "2025-10-09T08:53:20.032Z"
    );
    assert_eq!(buckets.as_array().map(Vec::len), Some(3), "{item}");
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
