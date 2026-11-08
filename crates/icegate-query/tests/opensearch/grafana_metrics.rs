//! Metrics-editor-style aggregation requests (legacy `interval`, epoch-ms
//! bounds, `extended_bounds`) and a variables-style terms request over literal
//! rows, with the buckets and values stated by hand. They pin IceGate's answers
//! to these request shapes; client compatibility is pinned only by the
//! end-to-end job.
#![allow(clippy::expect_used)]

use serde_json::{Value, json};

use super::aggregations::{BASE_MILLIS, DurationSpan, answer_tenant_a_search, build_duration_span};

/// A tenant-a row of the metrics tests.
const fn build_tenant_a_span(minute: i64, service: &'static str, duration_micros: i64) -> DurationSpan {
    build_duration_span("tenant-a", minute, service, duration_micros)
}

/// A tenant-b row in the same file, which a leak would count.
const fn build_intruder_span(minute: i64, duration_micros: i64) -> DurationSpan {
    build_duration_span("tenant-b", minute, "intruder", duration_micros)
}

/// A metrics-editor-style body: four one-minute buckets from `BASE_MILLIS`
/// with empty buckets filled, `children` under the histogram.
fn build_metrics_body(children: &Value) -> Value {
    json!({
        "size": 0,
        "query": {"bool": {"filter": [
            {"range": {"timestamp": {"gte": BASE_MILLIS, "lte": BASE_MILLIS + 239_999, "format": "epoch_millis"}}},
            {"query_string": {"analyze_wildcard": true, "query": "*"}}
        ]}},
        "aggs": {"2": {
            "date_histogram": {
                "interval": "1m",
                "field": "timestamp",
                "min_doc_count": 0,
                "extended_bounds": {"min": BASE_MILLIS, "max": BASE_MILLIS + 180_000},
                "format": "epoch_millis"
            },
            "aggs": children
        }}
    })
}

/// The buckets of the histogram `"2"`.
fn read_minute_buckets(body: &Value) -> &[Value] {
    body["aggregations"]["2"]["buckets"].as_array().expect("histogram buckets")
}

#[tokio::test]
async fn a_metrics_editor_style_average_and_percentiles_without_percents_request_answers_the_stated_values() {
    // Every minute holds one distinct duration, so each percentile equals it;
    // minute 2 is empty for tenant-a and tenant-b's row there must not fill it.
    let rows = [
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(1, "cart", 250),
        build_tenant_a_span(1, "cart", 250),
        build_tenant_a_span(3, "payment", 400),
        build_intruder_span(0, 999_999),
        build_intruder_span(2, 5),
    ];
    let body = answer_tenant_a_search(
        &rows,
        &build_metrics_body(&json!({
            "1": {"avg": {"field": "duration_micros"}},
            "3": {"percentiles": {"field": "duration_micros", "percents": ["25", "50", "75", "95", "99"]}},
            "4": {"percentiles": {"field": "duration_micros"}}
        })),
    )
    .await;
    let buckets = read_minute_buckets(&body);
    let keys: Vec<&Value> = buckets.iter().map(|bucket| &bucket["key"]).collect();
    assert_eq!(
        keys,
        vec![
            &json!(BASE_MILLIS),
            &json!(BASE_MILLIS + 60_000),
            &json!(BASE_MILLIS + 120_000),
            &json!(BASE_MILLIS + 180_000)
        ]
    );
    let doc_counts: Vec<&Value> = buckets.iter().map(|bucket| &bucket["doc_count"]).collect();
    assert_eq!(doc_counts, vec![&json!(3), &json!(2), &json!(0), &json!(1)]);
    let averages: Vec<Option<f64>> = buckets.iter().map(|bucket| bucket["1"]["value"].as_f64()).collect();
    assert_eq!(averages, vec![Some(100.0), Some(250.0), None, Some(400.0)]);
    for (bucket, value) in buckets.iter().zip([json!(100.0), json!(250.0), json!(null), json!(400.0)]) {
        assert_eq!(
            bucket["3"],
            json!({"values": {"25.0": value, "50.0": value, "75.0": value, "95.0": value, "99.0": value}})
        );
        assert_eq!(
            bucket["4"],
            json!({"values": {
                "1.0": value, "5.0": value, "25.0": value, "50.0": value,
                "75.0": value, "95.0": value, "99.0": value
            }})
        );
    }
}

#[tokio::test]
async fn a_metrics_editor_style_extended_stats_request_answers_the_stated_bounds() {
    let rows = [
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(0, "checkout", 200),
        build_tenant_a_span(0, "checkout", 300),
        build_tenant_a_span(0, "checkout", 600),
        build_intruder_span(0, 7),
    ];
    let body = answer_tenant_a_search(
        &rows,
        &build_metrics_body(&json!({"1": {"extended_stats": {"field": "duration_micros", "sigma": 2}}})),
    )
    .await;
    let buckets = read_minute_buckets(&body);
    let keys: Vec<&Value> = buckets.iter().map(|bucket| &bucket["key"]).collect();
    assert_eq!(
        keys,
        vec![
            &json!(BASE_MILLIS),
            &json!(BASE_MILLIS + 60_000),
            &json!(BASE_MILLIS + 120_000),
            &json!(BASE_MILLIS + 180_000)
        ]
    );
    // Minute 0: avg 300, population σ = √35000, sampling σ = √(140000 / 3).
    let bounds = &buckets[0]["1"]["std_deviation_bounds"];
    for (key, expected) in [
        ("upper", 674.165_738_677_394_1),
        ("lower", -74.165_738_677_394_13),
        ("upper_sampling", 732.049_379_893_857_3),
        ("lower_sampling", -132.049_379_893_857_35),
    ] {
        let actual = bounds[key].as_f64().unwrap_or_else(|| panic!("{key} is a number"));
        assert!((actual - expected).abs() <= 1e-9, "{key}: {actual} != {expected}");
    }
    assert_eq!(buckets[0]["1"]["count"], json!(4));
    // Minutes 1 to 3 are empty: zero count and null bounds.
    for bucket in &buckets[1..] {
        assert_eq!(bucket["1"]["count"], json!(0));
        assert_eq!(bucket["1"]["std_deviation_bounds"]["upper"], json!(null));
        assert_eq!(bucket["1"]["std_deviation_bounds"]["lower"], json!(null));
    }
}

#[tokio::test]
async fn a_metrics_editor_style_derivative_with_unit_request_answers_the_stated_values() {
    // Sums per minute: 300, 900, 600, empty.
    let rows = [
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(0, "checkout", 200),
        build_tenant_a_span(1, "checkout", 900),
        build_tenant_a_span(2, "checkout", 600),
        build_intruder_span(3, 1_000),
    ];
    let body = answer_tenant_a_search(
        &rows,
        &build_metrics_body(&json!({
            "1": {"sum": {"field": "duration_micros"}},
            "3": {"derivative": {"buckets_path": "1", "unit": "1s"}}
        })),
    )
    .await;
    let derivatives: Vec<Option<Value>> = read_minute_buckets(&body)
        .iter()
        .map(|bucket| bucket.get("3").cloned())
        .collect();
    // 600 per 60 s is 10 per second; -300 per 60 s is -5.
    assert_eq!(
        derivatives,
        vec![
            None,
            Some(json!({"value": 600.0, "normalized_value": 10.0})),
            Some(json!({"value": -300.0, "normalized_value": -5.0})),
            Some(json!({"value": null, "normalized_value": null})),
        ]
    );
}

#[tokio::test]
async fn a_metrics_editor_style_moving_avg_request_answers_the_stated_values() {
    // Averages per minute: 100, 300, 500, 200.
    let rows = [
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(1, "checkout", 300),
        build_tenant_a_span(2, "checkout", 500),
        build_tenant_a_span(3, "checkout", 200),
        build_intruder_span(1, 1_000_000),
    ];
    let body = answer_tenant_a_search(
        &rows,
        &build_metrics_body(&json!({
            "1": {"avg": {"field": "duration_micros"}},
            "3": {"moving_avg": {"buckets_path": "1", "model": "simple", "window": 5}}
        })),
    )
    .await;
    let averages: Vec<Option<Value>> = read_minute_buckets(&body)
        .iter()
        .map(|bucket| bucket.get("3").cloned())
        .collect();
    // Each bucket averages the values before it; the first has none.
    assert_eq!(
        averages,
        vec![
            None,
            Some(json!({"value": 100.0})),
            Some(json!({"value": 200.0})),
            Some(json!({"value": 300.0})),
        ]
    );
}

#[tokio::test]
async fn a_metrics_editor_style_bucket_script_request_answers_the_stated_values() {
    // Sums per minute: 300 over 2 rows, 900 over 1, 600 over 1, none.
    let rows = [
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(0, "checkout", 200),
        build_tenant_a_span(1, "checkout", 900),
        build_tenant_a_span(2, "checkout", 600),
        build_intruder_span(0, 30_000),
    ];
    let body = answer_tenant_a_search(
        &rows,
        &build_metrics_body(&json!({
            "1": {"sum": {"field": "duration_micros"}},
            "3": {"bucket_script": {
                "buckets_path": {"var1": "1", "var2": "_count"},
                "script": "params.var1 / params.var2"
            }}
        })),
    )
    .await;
    let ratios: Vec<Option<Value>> = read_minute_buckets(&body)
        .iter()
        .map(|bucket| bucket.get("3").cloned())
        .collect();
    assert_eq!(
        ratios,
        vec![
            Some(json!({"value": 150.0})),
            Some(json!({"value": 900.0})),
            Some(json!({"value": 600.0})),
            None,
        ]
    );
}

#[tokio::test]
async fn a_metrics_editor_style_terms_ordered_by_a_percentile_request_answers_the_stated_buckets() {
    // Each service holds one distinct duration: checkout 100, cart 250, payment 400.
    let rows = [
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(1, "cart", 250),
        build_tenant_a_span(1, "cart", 250),
        build_tenant_a_span(3, "payment", 400),
        build_intruder_span(0, 9_999_999),
    ];
    let body = answer_tenant_a_search(
        &rows,
        &json!({
            "size": 0,
            "query": {"bool": {"filter": [
                {"range": {"timestamp": {"gte": BASE_MILLIS, "lte": BASE_MILLIS + 239_999, "format": "epoch_millis"}}}
            ]}},
            "aggs": {"2": {
                "terms": {"field": "service_name", "size": 10, "order": {"3[50.0]": "desc"}, "min_doc_count": 1},
                "aggs": {
                    "3": {"percentiles": {"field": "duration_micros"}},
                    "4": {
                        "date_histogram": {
                            "interval": "1m", "field": "timestamp", "min_doc_count": 0,
                            "extended_bounds": {"min": BASE_MILLIS, "max": BASE_MILLIS + 180_000},
                            "format": "epoch_millis"
                        },
                        "aggs": {"3": {"percentiles": {"field": "duration_micros"}}}
                    }
                }
            }}
        }),
    )
    .await;
    let services = body["aggregations"]["2"]["buckets"].as_array().expect("terms buckets");
    let ranked: Vec<(&Value, &Value)> = services
        .iter()
        .map(|bucket| (&bucket["key"], &bucket["3"]["values"]["50.0"]))
        .collect();
    assert_eq!(
        ranked,
        vec![
            (&json!("payment"), &json!(400.0)),
            (&json!("cart"), &json!(250.0)),
            (&json!("checkout"), &json!(100.0)),
        ]
    );
    // payment's own histogram holds its one row in minute 3.
    let payment_counts: Vec<&Value> = services[0]["4"]["buckets"]
        .as_array()
        .expect("histogram buckets")
        .iter()
        .map(|bucket| &bucket["doc_count"])
        .collect();
    assert_eq!(payment_counts, vec![&json!(0), &json!(0), &json!(0), &json!(1)]);
}

#[tokio::test]
async fn a_variables_style_terms_request_answers_the_stated_keys() {
    let rows = [
        build_tenant_a_span(0, "checkout", 100),
        build_tenant_a_span(1, "cart", 250),
        build_tenant_a_span(3, "payment", 400),
        build_intruder_span(2, 5),
    ];
    let body = answer_tenant_a_search(
        &rows,
        &json!({
            "size": 0,
            "query": {"bool": {"filter": [
                {"range": {"timestamp": {"gte": BASE_MILLIS, "lte": BASE_MILLIS + 239_999, "format": "epoch_millis"}}}
            ]}},
            "aggs": {"1": {"terms": {"field": "service_name", "size": 500, "order": {"_key": "asc"}, "min_doc_count": 1}}}
        }),
    )
    .await;
    let keys: Vec<&Value> = body["aggregations"]["1"]["buckets"]
        .as_array()
        .expect("terms buckets")
        .iter()
        .map(|bucket| &bucket["key"])
        .collect();
    assert_eq!(keys, vec![&json!("cart"), &json!("checkout"), &json!("payment")]);
}
