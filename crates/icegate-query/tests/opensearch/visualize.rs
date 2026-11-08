//! Visualize-style aggregation requests: one request per acceptance type over
//! literal rows, with the buckets and values stated by hand. They pin IceGate's
//! answers to these request shapes; client compatibility is pinned only by the
//! end-to-end job.
#![allow(clippy::expect_used)]

use serde_json::{Value, json};

use super::aggregations::{BASE_MILLIS, DurationSpan, answer_tenant_a_search, search_spans, write_duration_spans};
use super::harness::TestServer;

/// The rows every test here reads. Tenant-a, minutes after `BASE_MILLIS`:
///
/// | `minute` | `service` | `name` | `duration_micros` |
/// |----------|-----------|--------|-------------------|
/// | 0        | checkout  | GET    | 100               |
/// | 0        | checkout  | POST   | 300               |
/// | 1        | cart      | GET    | 200               |
/// | 1        | checkout  | GET    | 500               |
/// | 2        | cart      | GET    | 400               |
/// | 3        | payment   | POST   | 1000              |
///
/// Tenant-b's rows share the file: a leak would add the `intruder` service,
/// add a third `cart` row and change checkout's minute-0 figures.
fn build_visualize_rows() -> Vec<DurationSpan> {
    let row = |tenant, minute, service, name, duration_micros| DurationSpan {
        tenant,
        minute,
        service,
        name,
        duration_micros,
    };
    vec![
        row("tenant-a", 0, "checkout", "GET", 100),
        row("tenant-a", 0, "checkout", "POST", 300),
        row("tenant-a", 1, "cart", "GET", 200),
        row("tenant-a", 1, "checkout", "GET", 500),
        row("tenant-a", 2, "cart", "GET", 400),
        row("tenant-a", 3, "payment", "POST", 1000),
        row("tenant-b", 0, "checkout", "GET", 900_000),
        row("tenant-b", 1, "intruder", "GET", 50),
        row("tenant-b", 1, "cart", "GET", 70),
        row("tenant-b", 2, "intruder", "POST", 60),
    ]
}

/// The time filter of every request: the first hour after `BASE_MILLIS`.
fn build_time_filter() -> Value {
    json!({"range": {"timestamp": {
        "gte": "2026-10-08T10:00:00.000Z",
        "lte": "2026-10-08T10:59:59.999Z",
        "format": "strict_date_optional_time"
    }}})
}

/// A Visualize-style body around `aggs`.
fn build_visualize_body(aggs: &Value) -> Value {
    json!({
        "aggs": aggs,
        "size": 0,
        "stored_fields": ["*"],
        "script_fields": {},
        "docvalue_fields": [{"field": "timestamp", "format": "date_time"}],
        "_source": {"excludes": []},
        "query": {"bool": {
            "must": [],
            "filter": [{"match_all": {}}, build_time_filter()],
            "should": [],
            "must_not": []
        }}
    })
}

/// A minute-wide `date_histogram` in UTC with `min_doc_count: 1`.
fn build_minute_histogram() -> Value {
    json!({
        "field": "timestamp",
        "fixed_interval": "1m",
        "time_zone": "UTC",
        "min_doc_count": 1,
        "extended_bounds": {"min": BASE_MILLIS, "max": BASE_MILLIS + 180_000}
    })
}

/// The bucket array of `aggregation`.
fn read_buckets(aggregation: &Value) -> &[Value] {
    aggregation["buckets"].as_array().expect("buckets array")
}

/// `(key, doc_count)` of each bucket of `aggregation`, which has no sub-aggregation.
fn summarize_counts(aggregation: &Value) -> Vec<(Value, u64)> {
    read_buckets(aggregation)
        .iter()
        .map(|bucket| (bucket["key"].clone(), bucket["doc_count"].as_u64().expect("doc_count")))
        .collect()
}

/// `(key, doc_count, <metric>.value)` of each bucket of `aggregation`.
fn summarize_buckets(aggregation: &Value, metric: &str) -> Vec<(Value, u64, Option<f64>)> {
    read_buckets(aggregation)
        .iter()
        .map(|bucket| {
            (
                bucket["key"].clone(),
                bucket["doc_count"].as_u64().expect("doc_count"),
                bucket[metric]["value"].as_f64(),
            )
        })
        .collect()
}

#[tokio::test]
async fn an_area_chart_style_request_answers_the_stated_buckets() {
    let body = answer_tenant_a_search(
        &build_visualize_rows(),
        &build_visualize_body(&json!({"2": {
            "date_histogram": build_minute_histogram(),
            "aggs": {"1": {"avg": {"field": "duration_micros"}}}
        }})),
    )
    .await;
    assert_eq!(
        summarize_buckets(&body["aggregations"]["2"], "1"),
        vec![
            (json!(BASE_MILLIS), 2, Some(200.0)),
            (json!(BASE_MILLIS + 60_000), 2, Some(350.0)),
            (json!(BASE_MILLIS + 120_000), 1, Some(400.0)),
            (json!(BASE_MILLIS + 180_000), 1, Some(1000.0)),
        ]
    );
}

#[tokio::test]
async fn a_vertical_bar_style_request_answers_the_stated_buckets() {
    let body = answer_tenant_a_search(
        &build_visualize_rows(),
        &build_visualize_body(&json!({"2": {
            "date_histogram": build_minute_histogram(),
            "aggs": {"3": {
                "terms": {"field": "service_name", "order": {"1": "desc"}, "size": 5},
                "aggs": {"1": {"sum": {"field": "duration_micros"}}}
            }}
        }})),
    )
    .await;
    let minutes = read_buckets(&body["aggregations"]["2"]);
    let series: Vec<Vec<(Value, u64, Option<f64>)>> =
        minutes.iter().map(|minute| summarize_buckets(&minute["3"], "1")).collect();
    assert_eq!(
        series,
        vec![
            vec![(json!("checkout"), 2, Some(400.0))],
            vec![(json!("checkout"), 1, Some(500.0)), (json!("cart"), 1, Some(200.0))],
            vec![(json!("cart"), 1, Some(400.0))],
            vec![(json!("payment"), 1, Some(1000.0))],
        ]
    );
}

#[tokio::test]
async fn a_horizontal_bar_style_request_answers_the_stated_buckets() {
    let body = answer_tenant_a_search(
        &build_visualize_rows(),
        &build_visualize_body(&json!({"2": {
            "terms": {"field": "service_name", "order": {"1": "desc"}, "size": 5},
            "aggs": {"1": {"max": {"field": "duration_micros"}}}
        }})),
    )
    .await;
    assert_eq!(
        summarize_buckets(&body["aggregations"]["2"], "1"),
        vec![
            (json!("payment"), 1, Some(1000.0)),
            (json!("checkout"), 3, Some(500.0)),
            (json!("cart"), 2, Some(400.0)),
        ]
    );
}

#[tokio::test]
async fn a_line_style_request_answers_the_stated_buckets() {
    let body = answer_tenant_a_search(
        &build_visualize_rows(),
        &build_visualize_body(&json!({"2": {
            "date_histogram": build_minute_histogram(),
            "aggs": {"1": {"cumulative_sum": {"buckets_path": "_count"}}}
        }})),
    )
    .await;
    assert_eq!(
        summarize_buckets(&body["aggregations"]["2"], "1"),
        vec![
            (json!(BASE_MILLIS), 2, Some(2.0)),
            (json!(BASE_MILLIS + 60_000), 2, Some(4.0)),
            (json!(BASE_MILLIS + 120_000), 1, Some(5.0)),
            (json!(BASE_MILLIS + 180_000), 1, Some(6.0)),
        ]
    );
}

#[tokio::test]
async fn a_two_level_pie_style_request_answers_the_stated_buckets() {
    // The request repeats metric "1" at both levels.
    let body = answer_tenant_a_search(
        &build_visualize_rows(),
        &build_visualize_body(&json!({"2": {
            "terms": {"field": "service_name", "order": {"1": "desc"}, "size": 5},
            "aggs": {
                "1": {"sum": {"field": "duration_micros"}},
                "3": {
                    "terms": {"field": "name", "order": {"1": "desc"}, "size": 5},
                    "aggs": {"1": {"sum": {"field": "duration_micros"}}}
                }
            }
        }})),
    )
    .await;
    let services = &body["aggregations"]["2"];
    assert_eq!(
        summarize_buckets(services, "1"),
        vec![
            (json!("payment"), 1, Some(1000.0)),
            (json!("checkout"), 3, Some(900.0)),
            (json!("cart"), 2, Some(600.0)),
        ]
    );
    let names: Vec<Vec<(Value, u64, Option<f64>)>> = read_buckets(services)
        .iter()
        .map(|service| summarize_buckets(&service["3"], "1"))
        .collect();
    assert_eq!(
        names,
        vec![
            vec![(json!("POST"), 1, Some(1000.0))],
            vec![(json!("GET"), 2, Some(600.0)), (json!("POST"), 1, Some(300.0))],
            vec![(json!("GET"), 2, Some(600.0))],
        ]
    );
}

#[tokio::test]
async fn a_metric_style_request_answers_the_stated_values() {
    let mut request = build_visualize_body(&json!({
        "1": {"avg": {"field": "duration_micros"}},
        "2": {"cardinality": {"field": "service_name"}}
    }));
    request["track_total_hits"] = json!(true);
    let body = answer_tenant_a_search(&build_visualize_rows(), &request).await;
    assert_eq!(body["hits"]["total"], json!({"value": 6, "relation": "eq"}));
    // (100 + 300 + 200 + 500 + 400 + 1000) / 6
    assert_eq!(body["aggregations"]["1"]["value"].as_f64(), Some(416.666_666_666_666_7));
    assert_eq!(body["aggregations"]["2"]["value"].as_u64(), Some(3));
}

#[tokio::test]
async fn a_gauge_style_request_answers_the_stated_buckets() {
    let body = answer_tenant_a_search(
        &build_visualize_rows(),
        &build_visualize_body(&json!({"2": {
            "terms": {"field": "service_name", "order": {"1": "desc"}, "size": 5},
            "aggs": {"1": {"avg": {"field": "duration_micros"}}}
        }})),
    )
    .await;
    // cart and checkout tie at 300: the tie breaks by key ascending.
    assert_eq!(
        summarize_buckets(&body["aggregations"]["2"], "1"),
        vec![
            (json!("payment"), 1, Some(1000.0)),
            (json!("cart"), 2, Some(300.0)),
            (json!("checkout"), 3, Some(300.0)),
        ]
    );
}

#[tokio::test]
async fn a_goal_style_request_answers_the_stated_buckets() {
    let body = answer_tenant_a_search(
        &build_visualize_rows(),
        &build_visualize_body(&json!({"2": {"range": {
            "field": "duration_micros",
            "ranges": [{"to": 300}, {"from": 300, "to": 600}, {"from": 600}],
            "keyed": true
        }}})),
    )
    .await;
    let buckets = body["aggregations"]["2"]["buckets"].as_object().expect("keyed buckets");
    let mut ranges: Vec<(Option<f64>, Option<f64>, u64)> = buckets
        .values()
        .map(|bucket| {
            (
                bucket["from"].as_f64(),
                bucket["to"].as_f64(),
                bucket["doc_count"].as_u64().expect("doc_count"),
            )
        })
        .collect();
    ranges.sort_by(|left, right| left.0.unwrap_or(f64::MIN).total_cmp(&right.0.unwrap_or(f64::MIN)));
    // Below 300: 100, 200. [300, 600): 300, 500, 400. 600 and above: 1000.
    assert_eq!(
        ranges,
        vec![
            (None, Some(300.0), 2),
            (Some(300.0), Some(600.0), 3),
            (Some(600.0), None, 1)
        ]
    );
    for bucket in buckets.values() {
        assert!(
            bucket.get("from").is_none_or(Value::is_number),
            "an open end is absent, never null"
        );
        assert!(
            bucket.get("to").is_none_or(Value::is_number),
            "an open end is absent, never null"
        );
    }
}

#[tokio::test]
async fn a_data_table_style_request_with_an_other_bucket_answers_the_stated_rows() {
    let (server, catalog) = TestServer::start().await;
    write_duration_spans(&server, &catalog, &build_visualize_rows()).await;
    let top_answer = search_spans(
        &server,
        "tenant-a",
        &build_visualize_body(
            &json!({"2": {"terms": {"field": "service_name", "order": {"_count": "desc"}, "size": 2}}}),
        ),
    )
    .await;
    assert_eq!(
        summarize_counts(&top_answer["aggregations"]["2"]),
        vec![(json!("checkout"), 3), (json!("cart"), 2)]
    );
    // The "other" request: rows with the field, none of the listed terms.
    let other_answer = search_spans(
        &server,
        "tenant-a",
        &build_visualize_body(
            &json!({"other-filter": {"filters": {"filters": {"-checkout-cart": {"bool": {
                "must": [],
                "filter": [build_time_filter(), {"exists": {"field": "service_name"}}],
                "should": [],
                "must_not": [
                    {"match_phrase": {"service_name": "checkout"}},
                    {"match_phrase": {"service_name": "cart"}}
                ]
            }}}}}}),
        ),
    )
    .await;
    // Only payment remains; tenant-b's intruder rows would make it 3.
    assert_eq!(
        other_answer["aggregations"]["other-filter"]["buckets"]["-checkout-cart"]["doc_count"],
        json!(1)
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_heat_map_style_request_answers_the_stated_buckets() {
    let body = answer_tenant_a_search(
        &build_visualize_rows(),
        &build_visualize_body(&json!({"2": {
            "date_histogram": build_minute_histogram(),
            "aggs": {"3": {"terms": {"field": "service_name", "order": {"_count": "desc"}, "size": 5}}}
        }})),
    )
    .await;
    let cells: Vec<Vec<(Value, u64)>> = read_buckets(&body["aggregations"]["2"])
        .iter()
        .map(|minute| summarize_counts(&minute["3"]))
        .collect();
    assert_eq!(
        cells,
        vec![
            vec![(json!("checkout"), 2)],
            vec![(json!("cart"), 1), (json!("checkout"), 1)],
            vec![(json!("cart"), 1)],
            vec![(json!("payment"), 1)],
        ]
    );
}

#[tokio::test]
async fn a_tag_cloud_style_request_answers_the_stated_buckets() {
    let body = answer_tenant_a_search(
        &build_visualize_rows(),
        &build_visualize_body(&json!({"2": {
            "terms": {"field": "service_name", "order": {"_count": "desc"}, "size": 3}
        }})),
    )
    .await;
    assert_eq!(
        summarize_counts(&body["aggregations"]["2"]),
        vec![(json!("checkout"), 3), (json!("cart"), 2), (json!("payment"), 1)]
    );
}

#[tokio::test]
async fn a_controls_style_options_list_and_range_request_answers_the_stated_keys_and_bounds() {
    let (server, catalog) = TestServer::start().await;
    write_duration_spans(&server, &catalog, &build_visualize_rows()).await;
    let options = search_spans(
        &server,
        "tenant-a",
        &json!({
            "size": 0,
            "timeout": "1000ms",
            "terminate_after": 100_000,
            "aggs": {"termsAgg": {"terms": {
                "field": "service_name",
                "order": {"_count": "desc"},
                "size": 5,
                "include": ".*[cC][aA].*"
            }}},
            "query": {"bool": {"filter": [build_time_filter()]}}
        }),
    )
    .await;
    assert_eq!(
        summarize_counts(&options["aggregations"]["termsAgg"]),
        vec![(json!("cart"), 2)]
    );
    assert_eq!(options["timed_out"], json!(false));
    assert_eq!(options["terminated_early"], json!(false));
    let range = search_spans(
        &server,
        "tenant-a",
        &json!({"size": 0, "aggs": {
            "maxAgg": {"max": {"field": "duration_micros"}},
            "minAgg": {"min": {"field": "duration_micros"}}
        }}),
    )
    .await;
    assert_eq!(range["aggregations"]["maxAgg"]["value"].as_f64(), Some(1000.0));
    assert_eq!(range["aggregations"]["minAgg"]["value"].as_f64(), Some(100.0));
    server.shutdown().await;
}

#[tokio::test]
async fn a_histogram_preflight_style_min_max_request_answers_the_bounds() {
    let (server, catalog) = TestServer::start().await;
    write_duration_spans(&server, &catalog, &build_visualize_rows()).await;
    // Minutes 1 and 2 only: durations 200, 500 and 400.
    let window = json!({"range": {"timestamp": {
        "gte": "2026-10-08T10:01:00.000Z",
        "lte": "2026-10-08T10:02:59.999Z",
        "format": "strict_date_optional_time"
    }}});
    let preflight = search_spans(
        &server,
        "tenant-a",
        &json!({
            "size": 0,
            "aggs": {
                "maxAgg": {"max": {"field": "duration_micros"}},
                "minAgg": {"min": {"field": "duration_micros"}}
            },
            "query": {"bool": {"filter": [window]}}
        }),
    )
    .await;
    assert_eq!(preflight["aggregations"]["minAgg"]["value"].as_f64(), Some(200.0));
    assert_eq!(preflight["aggregations"]["maxAgg"]["value"].as_f64(), Some(500.0));
    let histogram = search_spans(
        &server,
        "tenant-a",
        &json!({
            "size": 0,
            "aggs": {"2": {"histogram": {"field": "duration_micros", "interval": 200, "min_doc_count": 1}}},
            "query": {"bool": {"filter": [window]}}
        }),
    )
    .await;
    let buckets: Vec<(Option<f64>, u64)> = read_buckets(&histogram["aggregations"]["2"])
        .iter()
        .map(|bucket| (bucket["key"].as_f64(), bucket["doc_count"].as_u64().expect("doc_count")))
        .collect();
    // 200 → [200, 400); 400 and 500 → [400, 600).
    assert_eq!(buckets, vec![(Some(200.0), 1), (Some(400.0), 2)]);
    server.shutdown().await;
}
