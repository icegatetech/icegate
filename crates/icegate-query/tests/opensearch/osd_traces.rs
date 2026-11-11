//! The requests `OpenSearch` Dashboards 3.9's Traces table sends to `otel-v1-apm-span`, over real
//! HTTP, against the rows of the Grafana trace fixture. Every expected value is stated here. That
//! the Traces table sends them is pinned by an end-to-end case of
//! `scripts/opensearch-e2e/osd.spec.ts`:
//! `trace analytics traces table shows each trace with its latency, errors and group`.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::SPANS_TABLE;
use icegate_common::schema::{COL_NAME, COL_PARENT_SPAN_ID};
use icegate_query::opensearch::OpenSearchConfig;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{Cell, RowWriteError, SPAN_INDEX, T0, TENANT_A, build_span_row, commit_rows};
use super::grafana_traces::{commit_trace_rows, search_span_index};
use super::harness::TestServer;
use super::requests::send_request;

/// The index expression every OSD 3.9 Trace Analytics request names, as a `_search` path.
const TRACE_ANALYTICS_SEARCH_PATH: &str = "/otel-v1-apm-span-*,opensearch_dashboards_sample_data_otel_spans*/_search";

/// OSD 3.9's `latency` script, with the whitespace it is sent with: no `return 0` after the `if`.
const LATENCY_SOURCE: &str = "\n                if (doc.containsKey('traceGroupFields.durationInNanos') && !doc['traceGroupFields.durationInNanos'].empty) {\n                  return Math.round(doc['traceGroupFields.durationInNanos'].value / 10000) / 100.0\n                }\n                ";

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

/// The body OSD 3.9's Traces table sends, as captured from the stand, with the `now-5m`/`now`
/// bounds of its `startTime` range replaced by epoch milliseconds one minute either side of `T0`.
/// OSD puts `index` in the body for its own server; here it is the path of the `_search`.
fn build_osd_trace_table_body() -> Value {
    json!({
        "size": 0,
        "query": {"bool": {
            "must": [],
            "filter": [{"range": {"startTime": {"gte": 1_759_999_940_000_i64, "lte": 1_760_000_060_000_i64}}}],
            "should": [],
            "must_not": []
        }},
        "aggs": {
            "unique_traces": {"cardinality": {"field": "traceId"}},
            "traces": {
                "terms": {"field": "traceId", "size": 500, "order": {"last_updated": "desc"}, "execution_hint": "map"},
                "aggs": {
                    "latency": {"max": {"script": {"source": LATENCY_SOURCE, "lang": "painless"}}},
                    "trace_group": {"terms": {"field": "traceGroup", "size": 1}},
                    "error_count": {"filter": {"term": {"traceGroupFields.statusCode": "2"}}},
                    "last_updated": {"max": {"script": {"source": LAST_UPDATED_SOURCE, "lang": "painless"}}}
                }
            }
        },
        "track_total_hits": false
    })
}

/// Expected values derived by hand from the fixtures (tenant A holds traces A, B, C and `0f…`):
///
/// | trace | spans | root                         | latency  | last updated (epoch ms)  |
/// |-------|-------|------------------------------|----------|--------------------------|
/// | B     | 2     | `GET /cart`, 735 519 µs      | `735.51` | `1 760 000 000 745`      |
/// | A     | 2     | `GET /checkout`, 152 175 µs  | `152.17` | `1 760 000 000 152`      |
/// | `0f…` | 2     | `POST /enqueue`, 10 000 µs   | `10.0`   | `1 760 000 000 060`      |
/// | C     | 1     | not stored                   | `null`   | `1 760 000 000 032`      |
///
/// Latency is `floor(duration_ns / 10 000) / 100` of the root; trace C's only span has no
/// `traceGroupFields.durationInNanos`, so the script returns nothing and the `max` is null. The
/// buckets are ordered by last updated, descending, which is not key order. Trace A's root has
/// status 2, and `traceGroupFields.statusCode` is a trace-level field every span of the trace
/// carries, so trace A's `error_count` counts both its spans. `track_total_hits: false` leaves
/// `hits.total` out. Tenant B's rows would make `unique_traces` 5, trace A's count 3, and add a
/// bucket `0e…`.
#[tokio::test]
async fn the_osd_traces_table_request_answers_every_trace_ordered_by_last_updated() {
    let (server, catalog) = TestServer::start().await;
    commit_trace_rows(&catalog).await.expect("commit traces");
    commit_async_child_trace(&catalog).await.expect("commit the async-child trace");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        TRACE_ANALYTICS_SEARCH_PATH,
        Some(&build_osd_trace_table_body()),
    )
    .await
    .expect("traces table");

    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer["hits"].get("total"), None, "{answer}");
    assert_eq!(answer["hits"]["hits"], json!([]), "{answer}");
    let aggregations = &answer["aggregations"];
    assert_eq!(aggregations["unique_traces"], json!({"value": 4}), "{answer}");
    assert_eq!(
        aggregations["traces"]["buckets"],
        json!([
            {
                "key": "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b",
                "doc_count": 2,
                "latency": {"value": 735.51},
                "trace_group": {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": [{"key": "GET /cart", "doc_count": 2}]
                },
                "error_count": {"doc_count": 0},
                "last_updated": {"value": 1_760_000_000_745.0}
            },
            {
                "key": "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a",
                "doc_count": 2,
                "latency": {"value": 152.17},
                "trace_group": {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": [{"key": "GET /checkout", "doc_count": 2}]
                },
                "error_count": {"doc_count": 2},
                "last_updated": {"value": 1_760_000_000_152.0}
            },
            {
                "key": "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f",
                "doc_count": 2,
                "latency": {"value": 10.0},
                "trace_group": {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": [{"key": "POST /enqueue", "doc_count": 2}]
                },
                "error_count": {"doc_count": 0},
                "last_updated": {"value": 1_760_000_000_060.0}
            },
            {
                "key": "0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d",
                "doc_count": 1,
                "latency": {"value": null},
                "trace_group": {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": []
                },
                "error_count": {"doc_count": 0},
                "last_updated": {"value": 1_760_000_000_032.0}
            }
        ]),
        "{answer}"
    );
    server.shutdown().await;
}

/// Three traces lie in the window and the limit is two. The request's `terms.size` is lowered from
/// OSD's 500 to the limit, because a `terms` asking for more traces than the limit is refused
/// before any trace is selected; at the limit, ordering by `last_updated` beside the
/// `unique_traces` sibling selects every trace of the window, one more than the limit.
#[tokio::test]
async fn the_osd_traces_table_over_more_traces_than_the_cross_row_limit_is_refused() {
    let config = OpenSearchConfig {
        max_cross_row_traces: 2,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    commit_trace_rows(&catalog).await.expect("commit traces");
    let mut body = build_osd_trace_table_body();
    body["aggs"]["traces"]["terms"]["size"] = json!(2);

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        TRACE_ANALYTICS_SEARCH_PATH,
        Some(&body),
    )
    .await
    .expect("traces table");

    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{answer}");
    assert_eq!(answer["error"]["index"], json!(SPAN_INDEX), "{answer}");
    assert_eq!(answer["error"]["limit"], json!(2), "{answer}");
    server.shutdown().await;
}
