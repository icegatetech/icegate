//! Trace-list and trace-by-id style requests on `otel-v1-apm-span`, over real HTTP. The rows are
//! written with explicit values; every expected bucket and document is stated here.
#![allow(clippy::expect_used)]

use std::error::Error;
use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::SPANS_TABLE;
use icegate_common::schema::{
    COL_ATTRIBUTES, COL_KIND, COL_LINKS, COL_NAME, COL_PARENT_SPAN_ID, COL_SPAN_ID, COL_STATUS_CODE, COL_TRACE_ID,
    COL_TRACE_STATE,
};
use icegate_query::opensearch::OpenSearchConfig;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{
    Cell, Row, RowWriteError, SPAN_ALIAS, SPAN_INDEX, T0, TENANT_A, TENANT_B, build_span_row, commit_rows,
};
use super::harness::TestServer;
use super::requests::{send_ndjson, send_request};

/// `T0` in epoch milliseconds.
const T0_MILLIS: i64 = 1_760_000_000_000;

const TRACE_A: [u8; 16] = [0x0a; 16];
const TRACE_B: [u8; 16] = [0x0b; 16];
const TRACE_C: [u8; 16] = [0x0d; 16];
const TRACE_A_HEX: &str = "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a";
const TRACE_B_HEX: &str = "0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b";
const TRACE_C_HEX: &str = "0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d0d";
/// A trace only tenant B holds, keyed above every trace of tenant A.
const TENANT_B_TRACE: [u8; 16] = [0x0e; 16];

/// The latency script, sent as a multi-line text.
const LATENCY_SOURCE: &str = "\n                if (doc.containsKey('traceGroupFields.durationInNanos') && !doc['traceGroupFields.durationInNanos'].empty) {\n                  return Math.round(doc['traceGroupFields.durationInNanos'].value / 10000) / 100.0\n                }\n                return 0\n                ";

/// Tenant A: trace A (root `GET /checkout`, error, 152 175 µs, and a child), trace B (root
/// `GET /cart`, status unset, 735 519 µs, and a child), trace C (a child whose root is not
/// stored). Tenant B: a root of trace A sharing the span id of tenant A's root, so a leak would
/// add a hit, a span to trace A's bucket, or another root; and the root of trace `0e…`
/// (`900 000 µs`), which only tenant B holds, so a leak would add a key after `0d…` and a longer
/// latency.
fn build_trace_rows() -> Vec<Row<'static>> {
    vec![
        build_span_row(
            TENANT_A,
            &TRACE_A,
            &[0x11; 8],
            (T0, 152_175),
            vec![
                (COL_NAME, Cell::Text("GET /checkout")),
                (COL_KIND, Cell::Int(2)),
                (COL_STATUS_CODE, Cell::Int(2)),
            ],
        ),
        build_span_row(
            TENANT_A,
            &TRACE_A,
            &[0x12; 8],
            (T0 + 1_000, 50_000),
            vec![
                (COL_PARENT_SPAN_ID, Cell::Bytes(&[0x11; 8])),
                (COL_NAME, Cell::Text("SELECT orders")),
            ],
        ),
        build_span_row(
            TENANT_A,
            &TRACE_B,
            &[0x21; 8],
            (T0 + 10_000, 735_519),
            vec![(COL_NAME, Cell::Text("GET /cart"))],
        ),
        build_span_row(
            TENANT_A,
            &TRACE_B,
            &[0x22; 8],
            (T0 + 20_000, 1_000),
            vec![
                (COL_PARENT_SPAN_ID, Cell::Bytes(&[0x21; 8])),
                (COL_NAME, Cell::Text("render")),
            ],
        ),
        build_span_row(
            TENANT_A,
            &TRACE_C,
            &[0x31; 8],
            (T0 + 30_000, 2_000),
            vec![
                (COL_PARENT_SPAN_ID, Cell::Bytes(&[0x30; 8])),
                (COL_NAME, Cell::Text("orphan")),
            ],
        ),
        build_span_row(
            TENANT_B,
            &TRACE_A,
            &[0x11; 8],
            (T0, 9_000),
            vec![(COL_NAME, Cell::Text("tenant-b-root")), (COL_STATUS_CODE, Cell::Int(2))],
        ),
        build_span_row(
            TENANT_B,
            &TENANT_B_TRACE,
            &[0x41; 8],
            (T0 + 40_000, 900_000),
            vec![(COL_NAME, Cell::Text("tenant-b-batch"))],
        ),
    ]
}

/// The rows of [`build_trace_rows`], in one commit.
pub(super) async fn commit_trace_rows(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_rows(catalog, SPANS_TABLE, &build_trace_rows()).await
}

/// One `_msearch` item against the span index as tenant A: the answer of that item, whatever its
/// status, and an error when the `_msearch` itself does not answer `200`.
async fn fetch_msearch_item(server: &TestServer, body: &Value) -> Result<Value, Box<dyn Error>> {
    let header = json!({"index": SPAN_ALIAS, "ignore_unavailable": true});
    let (status, answer) = send_ndjson(
        server,
        TENANT_A,
        "/_msearch?max_concurrent_shard_requests=5",
        format!("{header}\n{body}\n"),
    )
    .await?;
    if status != StatusCode::OK {
        return Err(format!("_msearch answered {status}: {answer}").into());
    }
    Ok(answer["responses"][0].clone())
}

/// [`fetch_msearch_item`] for an item that must answer `200`.
pub(super) async fn search_span_index(server: &TestServer, body: &Value) -> Result<Value, Box<dyn Error>> {
    let item = fetch_msearch_item(server, body).await?;
    if item["status"] != 200 {
        return Err(format!("the _msearch item answered {item}").into());
    }
    Ok(item)
}

/// The bucket keys of the `traces` aggregation of `item`, in answer order.
fn read_trace_keys(item: &Value) -> Result<Vec<String>, Box<dyn Error>> {
    item["aggregations"]["traces"]["buckets"]
        .as_array()
        .ok_or_else(|| format!("no traces buckets in {item}"))?
        .iter()
        .map(|bucket| {
            bucket["key"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| format!("no key in {bucket}").into())
        })
        .collect()
}

/// A trace-list style body over `[T0 - 1 min, T0 + 1 min]`.
fn build_trace_list_body() -> Value {
    json!({
        "size": 10,
        "query": {"bool": {"must": [{"range": {"startTime": {"gte": T0_MILLIS - 60_000, "lte": T0_MILLIS + 60_000}}}]}},
        "aggs": {"traces": {
            "terms": {"field": "traceId", "size": 1000, "order": {"_key": "asc"}},
            "aggs": {
                "latency": {"max": {"script": {"source": LATENCY_SOURCE, "lang": "painless"}}},
                "trace_group": {"terms": {"field": "traceGroup", "size": 1}},
                "error_count": {"filter": {"term": {"traceGroupFields.statusCode": "2"}}},
                "last_updated": {"max": {"field": "traceGroupFields.endTime"}}
            }
        }}
    })
}

/// The trace list of [`build_trace_list_body`] answering at most `size` traces.
fn build_sized_trace_list_body(size: u64) -> Value {
    let mut body = build_trace_list_body();
    body["aggs"]["traces"]["terms"]["size"] = json!(size);
    body
}

/// Expected values derived by hand: trace A's root lasts `152 175 µs = 152 175 000 ns`, so
/// `floor(152 175 000 / 10 000) / 100 = 152.17`; trace B's root `735 519 000 ns` gives `735.51`;
/// trace A's root ends at `T0 + 152 175 µs`, epoch millisecond `1 760 000 000 152` (`…20.152Z`);
/// trace B's root starts at `T0 + 10 000 µs` and ends `735 519 µs` later, at epoch millisecond
/// `1 760 000 000 745`; the error count of a trace counts its spans when its root's status is 2;
/// the keys sort as hex (`0a…` < `0b…` < `0d…`). Tenant B's rows would make the total 7, trace
/// A's count 3, and add the key `0e…` after `0d…`.
#[tokio::test]
async fn a_trace_list_style_request_answers_buckets_with_no_null_latency_or_error_count() {
    let (server, catalog) = TestServer::start().await;
    commit_trace_rows(&catalog).await.expect("commit traces");

    let item = search_span_index(&server, &build_trace_list_body()).await.expect("trace list");

    assert_eq!(item["hits"]["total"], json!({"value": 5, "relation": "eq"}));
    assert_eq!(
        read_trace_keys(&item).expect("keys"),
        [TRACE_A_HEX, TRACE_B_HEX, TRACE_C_HEX]
    );
    let buckets = &item["aggregations"]["traces"]["buckets"];

    assert_eq!(buckets[0]["doc_count"], 2);
    assert_eq!(buckets[0]["latency"]["value"], json!(152.17));
    assert_eq!(
        buckets[0]["trace_group"]["buckets"],
        json!([{"key": "GET /checkout", "doc_count": 2}])
    );
    assert_eq!(buckets[0]["error_count"]["doc_count"], 2);
    assert_eq!(buckets[0]["last_updated"]["value"], json!(1_760_000_000_152.0));
    assert_eq!(
        buckets[0]["last_updated"]["value_as_string"],
        "2025-10-09T08:53:20.152Z"
    );

    assert_eq!(buckets[1]["doc_count"], 2);
    assert_eq!(buckets[1]["latency"]["value"], json!(735.51));
    assert_eq!(
        buckets[1]["trace_group"]["buckets"],
        json!([{"key": "GET /cart", "doc_count": 2}])
    );
    assert_eq!(buckets[1]["error_count"]["doc_count"], 0);
    assert_eq!(buckets[1]["last_updated"]["value"], json!(1_760_000_000_745.0));

    // Trace C has no stored root: latency and error count are 0, never null.
    assert_eq!(buckets[2]["doc_count"], 1);
    assert_eq!(buckets[2]["latency"]["value"], json!(0.0));
    assert_eq!(buckets[2]["trace_group"]["buckets"], json!([]));
    assert_eq!(buckets[2]["error_count"]["doc_count"], 0);
    assert_eq!(buckets[2]["last_updated"]["value"], Value::Null);
    server.shutdown().await;
}

/// Three traces lie in the window, one more than the limit of two. A search that is no plain trace
/// list (a `terms` on another field, a second top-level aggregation beside `traces`), a trace list
/// whose buckets are not exactly its first keys (an `include`, an `exclude`, a `missing`, a
/// `min_doc_count` above 1, an order by count), or one whose query or sort reads a root, selects
/// every trace of the window and is refused, which proves the fixture holds more traces than the
/// limit. The key-ordered trace list looks up the roots of its first two keys only, so it answers
/// them.
#[tokio::test]
async fn a_trace_list_style_request_over_more_traces_than_the_cross_row_limit_answers_its_first_keys() {
    let config = OpenSearchConfig {
        max_cross_row_traces: 2,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    commit_trace_rows(&catalog).await.expect("commit traces");
    let mut counted_body = build_sized_trace_list_body(2);
    counted_body["aggs"]["traces"]["terms"]["min_doc_count"] = json!(2);
    let mut count_ordered_body = build_sized_trace_list_body(2);
    count_ordered_body["aggs"]["traces"]["terms"]["order"] = json!({"_count": "desc"});
    let mut root_filtered_body = build_sized_trace_list_body(2);
    root_filtered_body["query"]["bool"]["filter"] = json!([{"exists": {"field": "traceGroup"}}]);
    let mut root_sorted_body = build_sized_trace_list_body(2);
    root_sorted_body["sort"] = json!([{"traceGroupFields.durationInNanos": "desc"}]);
    let mut included_body = build_sized_trace_list_body(2);
    included_body["aggs"]["traces"]["terms"]["include"] = json!([TRACE_A_HEX, TRACE_B_HEX, TRACE_C_HEX]);
    let mut excluded_body = build_sized_trace_list_body(2);
    excluded_body["aggs"]["traces"]["terms"]["exclude"] = json!([TRACE_A_HEX]);
    let mut defaulted_body = build_sized_trace_list_body(2);
    defaulted_body["aggs"]["traces"]["terms"]["missing"] = json!(TRACE_A_HEX);
    let mut span_keyed_body = build_sized_trace_list_body(2);
    span_keyed_body["aggs"]["traces"]["terms"]["field"] = json!("spanId");
    let mut sibling_body = build_sized_trace_list_body(2);
    sibling_body["aggs"]["longest"] = json!({"max": {"field": "durationInNanos"}});

    for bounded_body in [
        counted_body,
        count_ordered_body,
        root_filtered_body,
        root_sorted_body,
        included_body,
        excluded_body,
        defaulted_body,
        span_keyed_body,
        sibling_body,
    ] {
        let refused_item = fetch_msearch_item(&server, &bounded_body).await.expect("bounded trace list");
        assert_eq!(refused_item["status"], 400, "{refused_item}");
        assert_eq!(
            refused_item["error"]["type"], "illegal_argument_exception",
            "{refused_item}"
        );
        assert_eq!(refused_item["error"]["index"], SPAN_INDEX, "{refused_item}");
        assert_eq!(refused_item["error"]["limit"], 2, "{refused_item}");
    }
    let item = search_span_index(&server, &build_sized_trace_list_body(2))
        .await
        .expect("trace list");

    assert_eq!(read_trace_keys(&item).expect("keys"), [TRACE_A_HEX, TRACE_B_HEX]);
    let buckets = &item["aggregations"]["traces"]["buckets"];
    assert_eq!(buckets[0]["latency"]["value"], json!(152.17));
    assert_eq!(
        buckets[0]["trace_group"]["buckets"],
        json!([{"key": "GET /checkout", "doc_count": 2}])
    );
    assert_eq!(buckets[1]["latency"]["value"], json!(735.51));
    assert_eq!(
        buckets[1]["trace_group"]["buckets"],
        json!([{"key": "GET /cart", "doc_count": 2}])
    );
    server.shutdown().await;
}

/// The descending trace list looks up the roots of its first two keys in descending order, over a
/// window of three traces with a limit of two. This test stores trace C's root (`GET /batch`,
/// `508 628 µs = 508 628 000 ns`, so `floor(508 628 000 / 10 000) / 100 = 508.62`), which only a
/// descending selection looks up: an ascending one would select traces A and B and render trace
/// C's latency 0 and no trace group. Trace B's root gives `735.51`. Tenant A then holds six spans;
/// tenant B's root of trace C starts earlier under the same span id, so a leak would change trace
/// C's latency, trace group and count, and its other rows would put `0e…` first and raise the
/// total.
#[tokio::test]
async fn a_descending_trace_list_over_more_traces_than_the_cross_row_limit_answers_its_last_keys() {
    let config = OpenSearchConfig {
        max_cross_row_traces: 2,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    let mut rows = build_trace_rows();
    rows.extend([
        build_span_row(
            TENANT_A,
            &TRACE_C,
            &[0x30; 8],
            (T0 + 29_000, 508_628),
            vec![(COL_NAME, Cell::Text("GET /batch"))],
        ),
        build_span_row(
            TENANT_B,
            &TRACE_C,
            &[0x30; 8],
            (T0 + 28_000, 1_000),
            vec![(COL_NAME, Cell::Text("tenant-b-batch-root"))],
        ),
    ]);
    commit_rows(&catalog, SPANS_TABLE, &rows).await.expect("commit traces");
    let mut body = build_sized_trace_list_body(2);
    body["aggs"]["traces"]["terms"]["order"] = json!({"_key": "desc"});

    let item = search_span_index(&server, &body).await.expect("trace list");

    assert_eq!(item["hits"]["total"], json!({"value": 6, "relation": "eq"}));
    assert_eq!(read_trace_keys(&item).expect("keys"), [TRACE_C_HEX, TRACE_B_HEX]);
    let buckets = &item["aggregations"]["traces"]["buckets"];
    assert_eq!(buckets[0]["doc_count"], 2);
    assert_eq!(buckets[0]["latency"]["value"], json!(508.62));
    assert_eq!(
        buckets[0]["trace_group"]["buckets"],
        json!([{"key": "GET /batch", "doc_count": 2}])
    );
    assert_eq!(buckets[1]["latency"]["value"], json!(735.51));
    assert_eq!(
        buckets[1]["trace_group"]["buckets"],
        json!([{"key": "GET /cart", "doc_count": 2}])
    );
    server.shutdown().await;
}

/// A one-key trace list selects trace A only, yet its page holds every span of the window: each
/// hit renders the trace group of its own trace's root, looked up for the page whatever the
/// selection attached. Trace A's spans give `GET /checkout` and trace B's `GET /cart`; trace C's
/// root is not stored, so its span renders no `traceGroup`. The limit of two, below the window's
/// three traces, proves the key-ordered selection runs. Tenant B holds roots of traces B and C
/// under tenant A's span ids (`0x21`, `0x30`): a leak would rename trace B's group, give trace C a
/// group, add hits and raise the total above 5.
#[tokio::test]
async fn a_trace_list_page_renders_the_trace_group_of_traces_outside_its_keys() {
    let config = OpenSearchConfig {
        max_cross_row_traces: 2,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;
    let mut rows = build_trace_rows();
    rows.extend([
        build_span_row(
            TENANT_B,
            &TRACE_B,
            &[0x21; 8],
            (T0 + 9_000, 1_000),
            vec![(COL_NAME, Cell::Text("tenant-b-cart"))],
        ),
        build_span_row(
            TENANT_B,
            &TRACE_C,
            &[0x30; 8],
            (T0 + 28_000, 1_000),
            vec![(COL_NAME, Cell::Text("tenant-b-batch-root"))],
        ),
    ]);
    commit_rows(&catalog, SPANS_TABLE, &rows).await.expect("commit traces");

    let item = search_span_index(&server, &build_sized_trace_list_body(1))
        .await
        .expect("trace list");

    assert_eq!(item["hits"]["total"], json!({"value": 5, "relation": "eq"}));
    assert_eq!(read_trace_keys(&item).expect("keys"), [TRACE_A_HEX]);
    // The order of an unsorted page is no contract, so the hits are compared by span id.
    let mut trace_groups: Vec<(&str, Option<&Value>)> = item["hits"]["hits"]
        .as_array()
        .expect("hits")
        .iter()
        .map(|hit| {
            let source = &hit["_source"];
            (source["spanId"].as_str().expect("spanId"), source.get("traceGroup"))
        })
        .collect();
    trace_groups.sort_unstable_by_key(|(span_id, _)| *span_id);
    let checkout = json!("GET /checkout");
    let cart = json!("GET /cart");
    assert_eq!(
        trace_groups,
        [
            ("1111111111111111", Some(&checkout)),
            ("1212121212121212", Some(&checkout)),
            ("2121212121212121", Some(&cart)),
            ("2222222222222222", Some(&cart)),
            ("3131313131313131", None),
        ],
        "{item}"
    );
    server.shutdown().await;
}

/// Trace A's spans of tenant A, each carrying trace A's root fields: `152 175 µs` is
/// `152 175 000 ns`, and the root ends at `T0 + 152 175 µs`, `…20.152175Z`. Tenant B's root of
/// trace A shares the root's span id, so a leak would add a hit and a different root.
#[tokio::test]
async fn a_trace_by_id_style_request_answers_every_span_of_the_trace() {
    let (server, catalog) = TestServer::start().await;
    commit_trace_rows(&catalog).await.expect("commit traces");

    let item = search_span_index(
        &server,
        &json!({
            "size": 1000,
            "query": {"bool": {"must": [
                {"range": {"startTime": {"lte": T0_MILLIS + 60_000, "gte": T0_MILLIS - 60_000}}},
                {"term": {"traceId": TRACE_A_HEX}}
            ]}}
        }),
    )
    .await
    .expect("trace by id");

    let hits = item["hits"]["hits"].as_array().expect("hits");
    let mut span_ids: Vec<&str> = hits
        .iter()
        .map(|hit| hit["_source"]["spanId"].as_str().expect("spanId"))
        .collect();
    span_ids.sort_unstable();
    assert_eq!(
        span_ids,
        ["1111111111111111", "1212121212121212"],
        "tenant B's root of trace A is never returned"
    );
    for hit in hits {
        assert_eq!(hit["_source"]["traceId"], TRACE_A_HEX);
        assert_eq!(hit["_source"]["traceGroup"], "GET /checkout");
        assert_eq!(
            hit["_source"]["traceGroupFields"],
            json!({"endTime": "2025-10-09T08:53:20.152175Z", "durationInNanos": 152_175_000, "statusCode": 2})
        );
    }
    server.shutdown().await;
}

/// Tenant A's root of trace A fails with status 2 and stores no event; tenant B's root shares its
/// span id, so a leak would add a second hit.
#[tokio::test]
async fn a_failing_span_carries_no_synthesized_error_event() {
    let (server, catalog) = TestServer::start().await;
    commit_trace_rows(&catalog).await.expect("commit traces");

    let item = search_span_index(&server, &json!({"query": {"term": {"spanId": "1111111111111111"}}}))
        .await
        .expect("failing span");

    let hits = item["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 1, "{item}");
    let source = &hits[0]["_source"];
    assert_eq!(source["name"], "GET /checkout");
    assert_eq!(source["status"], json!({"code": 2}));
    assert_eq!(source["events"], json!([]), "no event is derived from the status");
    server.shutdown().await;
}

/// A link renders its ids as hex, its attribute key escaped (`.` as `@`), and its required
/// dropped count, which the row helper fills with zero. Tenant B's span shares the span id and
/// links elsewhere, so a leak would add a hit.
#[tokio::test]
async fn a_span_with_links_renders_hex_link_ids() {
    let (server, catalog) = TestServer::start().await;
    let link: &[(&str, Cell<'_>)] = &[
        (COL_TRACE_ID, Cell::Bytes(&TRACE_B)),
        (COL_SPAN_ID, Cell::Bytes(&[0x21; 8])),
        (COL_TRACE_STATE, Cell::Text("vendor=1")),
        (COL_ATTRIBUTES, Cell::Map(&[("link.kind", "follows")])),
    ];
    let tenant_b_link: &[(&str, Cell<'_>)] = &[
        (COL_TRACE_ID, Cell::Bytes(&TRACE_C)),
        (COL_SPAN_ID, Cell::Bytes(&[0x31; 8])),
    ];
    let links: &[&[(&str, Cell<'_>)]] = &[link];
    let tenant_b_links: &[&[(&str, Cell<'_>)]] = &[tenant_b_link];
    commit_rows(
        &catalog,
        SPANS_TABLE,
        &[
            build_span_row(
                TENANT_A,
                &TRACE_A,
                &[0x13; 8],
                (T0, 1_000),
                vec![(COL_NAME, Cell::Text("consume")), (COL_LINKS, Cell::Structs(links))],
            ),
            build_span_row(
                TENANT_B,
                &TRACE_A,
                &[0x13; 8],
                (T0, 1_000),
                vec![
                    (COL_NAME, Cell::Text("tenant-b-consume")),
                    (COL_LINKS, Cell::Structs(tenant_b_links)),
                ],
            ),
        ],
    )
    .await
    .expect("commit");

    let item = search_span_index(&server, &json!({"query": {"term": {"spanId": "1313131313131313"}}}))
        .await
        .expect("linked span");

    let hits = item["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 1, "{item}");
    assert_eq!(
        hits[0]["_source"]["links"],
        json!([{
            "traceId": TRACE_B_HEX,
            "spanId": "2121212121212121",
            "traceState": "vendor=1",
            "attributes": {"link@kind": "follows"},
            "droppedAttributesCount": 0
        }])
    );
    server.shutdown().await;
}

/// Three one-minute buckets from `08:53`; every span starts within the first minute after
/// `08:53:00`, so the second and third are empty. The first bucket's latency is the longest
/// root's, trace B's `735.51`, and its plain `max` the longest own duration, trace B's root at
/// `735 519 000 ns`. Tenant B's spans would make the first bucket count 7, and its root of trace
/// `0e…` (`900 000 µs`) would raise both maxima.
#[tokio::test]
async fn null_rendered_fields_render_zero_only_in_the_data_prepper_profile() {
    let (server, catalog) = TestServer::start().await;
    commit_trace_rows(&catalog).await.expect("commit traces");

    let item = search_span_index(
        &server,
        &json!({
            "size": 0,
            "aggs": {"minutes": {
                "date_histogram": {
                    "field": "startTime",
                    "fixed_interval": "1m",
                    "min_doc_count": 0,
                    "extended_bounds": {"min": T0_MILLIS, "max": T0_MILLIS + 120_000}
                },
                "aggs": {
                    "latency": {"max": {"script": {"source": LATENCY_SOURCE, "lang": "painless"}}},
                    "longest": {"max": {"field": "durationInNanos"}}
                }
            }}
        }),
    )
    .await
    .expect("latency histogram");
    let buckets = item["aggregations"]["minutes"]["buckets"].as_array().expect("buckets");
    assert_eq!(buckets.len(), 3, "{item}");
    assert_eq!(buckets[0]["doc_count"], 5);
    assert_eq!(buckets[0]["latency"]["value"], json!(735.51));
    assert_eq!(buckets[0]["longest"]["value"], json!(735_519_000.0));
    for empty in &buckets[1..] {
        assert_eq!(empty["doc_count"], 0);
        assert_eq!(
            empty["latency"]["value"],
            json!(0.0),
            "the latency script never renders null"
        );
        assert_eq!(
            empty["longest"]["value"],
            Value::Null,
            "a plain max of an empty bucket stays null"
        );
    }

    // The latency script reads traceGroupFields: on a native index it is refused.
    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/spans/_search",
        Some(&json!({"size": 0, "aggs": {"latency": {"max": {"script": {"source": LATENCY_SOURCE, "lang": "painless"}}}}})),
    )
    .await
    .expect("native search");
    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(answer["error"]["aggregation"], "latency", "{answer}");
    assert_eq!(answer["error"]["index"], "spans", "{answer}");
    server.shutdown().await;
}
