//! The `nested` aggregation over the histogram buckets of the ss4o metrics index, as OSD 3.9's
//! Metrics page sends it, and its refusals. Every row is written with explicit values and every
//! expected count is stated here. That the Metrics page sends it is pinned by an end-to-end case of
//! `scripts/opensearch-e2e/osd.spec.ts`:
//! `observability metrics plots the bins of a histogram metric`.
#![allow(clippy::expect_used)]

use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::METRICS_TABLE;
use icegate_common::schema::{COL_BUCKET_COUNTS, COL_EXPLICIT_BOUNDS, COL_VALUE_INT};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{Cell, RowWriteError, T0, TENANT_A, TENANT_B, commit_rows};
use super::harness::TestServer;
use super::profiles::build_metric_row_at;
use super::requests::send_request;

const SS4O_METRICS_SEARCH_PATH: &str = "/ss4o_metrics-icegate-default/_search";
const SS4O_METRICS: &str = "ss4o_metrics-icegate-default";
/// The histogram metric of [`commit_bin_count_metrics`].
const HISTOGRAM_METRIC: &str = "http.server.duration";
/// The sum metric of [`commit_bin_count_metrics`], which has no buckets.
const SUM_METRIC: &str = "http.server.requests";
/// One minute, in microseconds.
const MINUTE_MICROS: i64 = 60_000_000;
/// The window around tenant A's two histogram rows, in the format OSD writes for a metric: `T0`
/// is `2025-10-09T08:53:20Z` and the second row lies one minute later.
const WINDOW_START: &str = "2025-10-09T08:53:20.000000Z";
const WINDOW_END: &str = "2025-10-09T08:54:20.999000Z";

/// Tenant A's histogram at `T0` (bounds `[1.0, 5.0]`, counts `[2, 3, 4]`), at `T0 + 1 min`
/// (counts `[1, 1, 1]`) and at `T0 + 5 min`, past the window (counts `[50, 50, 50]`), and its
/// sum at `T0`; tenant B's histogram of the same name at `T0` (counts `[100, 100, 100]`). The
/// buckets of a row with bounds `[1.0, 5.0]` run up to `1.0`, from `1.0` to `5.0`, and from `5.0`.
async fn commit_bin_count_metrics(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    let histogram_cells = |counts: &'static [i64]| {
        vec![
            (COL_BUCKET_COUNTS, Cell::Longs(counts)),
            (COL_EXPLICIT_BOUNDS, Cell::Doubles(&[1.0, 5.0])),
        ]
    };
    commit_rows(
        catalog,
        METRICS_TABLE,
        &[
            build_metric_row_at(TENANT_A, T0, HISTOGRAM_METRIC, "histogram", histogram_cells(&[2, 3, 4])),
            build_metric_row_at(
                TENANT_A,
                T0 + MINUTE_MICROS,
                HISTOGRAM_METRIC,
                "histogram",
                histogram_cells(&[1, 1, 1]),
            ),
            build_metric_row_at(
                TENANT_A,
                T0 + 5 * MINUTE_MICROS,
                HISTOGRAM_METRIC,
                "histogram",
                histogram_cells(&[50, 50, 50]),
            ),
            build_metric_row_at(TENANT_A, T0, SUM_METRIC, "sum", vec![(COL_VALUE_INT, Cell::Long(7))]),
            build_metric_row_at(
                TENANT_B,
                T0,
                HISTOGRAM_METRIC,
                "histogram",
                histogram_cells(&[100, 100, 100]),
            ),
        ],
    )
    .await
}

/// The bin-count request OSD 3.9's Metrics page sends once per sample bucket: `min` and `max` are
/// the sample bucket's bounds as strings, `start` and `end` the window.
fn build_bin_count_body(min: &str, max: &str, start: &str, end: &str) -> Value {
    json!({
        "size": 0,
        "query": {"bool": {"must": [
            {"term": {"name": {"value": HISTOGRAM_METRIC}}},
            {"range": {"time": {"gte": start, "lte": end}}}
        ]}},
        "aggs": {"nested_buckets": {
            "nested": {"path": "buckets"},
            "aggs": {"bucket_range": {
                "filter": {"range": {"buckets.max": {"gt": min, "lte": max}}},
                "aggs": {"bucket_count": {"sum": {"field": "buckets.count"}}}
            }}
        }}
    })
}

/// `POST body` to `path` as tenant A: the `aggregations` of a `200`.
async fn fetch_aggregations(server: &TestServer, path: &str, body: &Value) -> Value {
    let (status, answer) = send_request(server, Method::POST, TENANT_A, path, Some(body))
        .await
        .expect("search request");
    assert_eq!(status, StatusCode::OK, "{answer}");
    answer["aggregations"].clone()
}

/// `POST body` to `path` as tenant A: the status and the `error` object.
async fn fetch_error(server: &TestServer, path: &str, body: &Value) -> (StatusCode, Value) {
    let (status, answer) = send_request(server, Method::POST, TENANT_A, path, Some(body))
        .await
        .expect("search request");
    (status, answer["error"].clone())
}

/// The bin count of the bucket from `1.0` to `5.0`: the window holds tenant A's rows at `T0` and
/// `T0 + 1 min`, six bucket objects, of which one per row has `max` `5.0`, counting `3` and `1`.
/// The row past the window and tenant B's row add nothing; a window holding no row answers no
/// object and a zero sum.
#[tokio::test]
async fn osd_metric_bin_count_sums_one_bucket_range_over_the_window() {
    let (server, catalog) = TestServer::start().await;
    commit_bin_count_metrics(&catalog).await.expect("commit metrics");

    let aggs = fetch_aggregations(
        &server,
        SS4O_METRICS_SEARCH_PATH,
        &build_bin_count_body("1.0", "5.0", WINDOW_START, WINDOW_END),
    )
    .await;
    let empty_aggs = fetch_aggregations(
        &server,
        SS4O_METRICS_SEARCH_PATH,
        &build_bin_count_body(
            "1.0",
            "5.0",
            "2025-10-09T09:53:20.000000Z",
            "2025-10-09T09:54:20.999000Z",
        ),
    )
    .await;

    assert_eq!(aggs["nested_buckets"]["doc_count"], json!(6), "{aggs}");
    assert_eq!(aggs["nested_buckets"]["bucket_range"]["doc_count"], json!(2), "{aggs}");
    assert_eq!(
        aggs["nested_buckets"]["bucket_range"]["bucket_count"]["value"],
        json!(4.0),
        "{aggs}"
    );
    assert_eq!(
        empty_aggs,
        json!({"nested_buckets": {"doc_count": 0, "bucket_range": {"doc_count": 0, "bucket_count": {"value": 0.0}}}})
    );
    server.shutdown().await;
}

/// The range bounds are strings as `Number.prototype.toString()` writes the sample bucket's
/// bounds: the open ends as `-3.4028234663852886e+38` and `3.4028234663852886e+38`, whole bounds
/// without a fraction. Each sample bucket of the window counts its own objects: up to `1` holds
/// `2` and `1`, `1` to `5` holds `3` and `1`, from `5` holds `4` and `1`.
#[tokio::test]
async fn osd_metric_bin_counts_read_bounds_written_as_javascript_number_strings() {
    let (server, catalog) = TestServer::start().await;
    commit_bin_count_metrics(&catalog).await.expect("commit metrics");

    for (min, max, expected_count) in [
        ("-3.4028234663852886e+38", "1", 3.0),
        ("1", "5", 4.0),
        ("5", "3.4028234663852886e+38", 5.0),
    ] {
        let aggs = fetch_aggregations(
            &server,
            SS4O_METRICS_SEARCH_PATH,
            &build_bin_count_body(min, max, WINDOW_START, WINDOW_END),
        )
        .await;

        assert_eq!(
            aggs,
            json!({"nested_buckets": {
                "doc_count": 6,
                "bucket_range": {"doc_count": 2, "bucket_count": {"value": expected_count}}
            }}),
            "({min}, {max}]"
        );
    }
    server.shutdown().await;
}

/// Inside `nested`, `sum`, `min`, `max`, `value_count` and `avg` read the members of every
/// object of the matching rows, and `filter` with `term`, `terms`, `exists`, `range` and `bool`
/// selects objects by their members, nested filters included. The window's rows hold the objects
/// `(min, max, count)`: `(-M, 1, 2)`, `(1, 5, 3)`, `(5, M, 4)`, `(-M, 1, 1)`, `(1, 5, 1)`,
/// `(5, M, 1)`; the sum row in the window has none. A sibling outside `nested` reads the rows.
#[tokio::test]
async fn metrics_and_filters_inside_nested_read_the_members_of_each_object() {
    let (server, catalog) = TestServer::start().await;
    commit_bin_count_metrics(&catalog).await.expect("commit metrics");
    let body = json!({
        "size": 0,
        "query": {"range": {"time": {"gte": WINDOW_START, "lte": WINDOW_END}}},
        "aggs": {
            "objects": {
                "nested": {"path": "buckets"},
                "aggs": {
                    "count_sum": {"sum": {"field": "buckets.count"}},
                    "lowest_max": {"min": {"field": "buckets.max"}},
                    "highest_count": {"max": {"field": "buckets.count"}},
                    "mins": {"value_count": {"field": "buckets.min"}},
                    "count_mean": {"avg": {"field": "buckets.count"}},
                    "min_one": {
                        "filter": {"term": {"buckets.min": "1"}},
                        "aggs": {"count_sum": {"sum": {"field": "buckets.count"}}}
                    },
                    "count_one_or_four": {"filter": {"terms": {"buckets.count": [1, 4]}}},
                    "with_count": {"filter": {"exists": {"field": "buckets.count"}}},
                    "above_one": {
                        "filter": {"bool": {"must_not": [{"range": {"buckets.max": {"lte": 1}}}]}},
                        "aggs": {"count_one": {
                            "filter": {"term": {"buckets.count": 1}},
                            "aggs": {"count_sum": {"sum": {"field": "buckets.count"}}}
                        }}
                    }
                }
            },
            "rows": {"value_count": {"field": "name"}}
        }
    });

    let aggs = fetch_aggregations(&server, SS4O_METRICS_SEARCH_PATH, &body).await;

    assert_eq!(
        aggs,
        json!({
            "objects": {
                "doc_count": 6,
                "count_sum": {"value": 12.0},
                "lowest_max": {"value": 1.0},
                "highest_count": {"value": 4.0},
                "mins": {"value": 6},
                "count_mean": {"value": 2.0},
                "min_one": {"doc_count": 2, "count_sum": {"value": 4.0}},
                "count_one_or_four": {"doc_count": 4},
                "with_count": {"doc_count": 6},
                "above_one": {
                    "doc_count": 4,
                    "count_one": {"doc_count": 2, "count_sum": {"value": 2.0}}
                }
            },
            "rows": {"value": 3}
        })
    );
    server.shutdown().await;
}

/// `nested` reads a field the index declares as nested. Any other path of `ss4o_metrics`, and
/// `buckets` on `logs`, which declares none, is a 400 naming the aggregation, the path and the
/// index.
#[tokio::test]
async fn a_nested_aggregation_on_an_undeclared_path_is_refused_naming_it() {
    let (server, catalog) = TestServer::start().await;
    commit_bin_count_metrics(&catalog).await.expect("commit metrics");
    let nested_body = |path: &str| {
        json!({"size": 0, "aggs": {"objects": {
            "nested": {"path": path},
            "aggs": {"count_sum": {"sum": {"field": format!("{path}.count")}}}
        }}})
    };

    for (index, path) in [
        (SS4O_METRICS, "attributes"),
        (SS4O_METRICS, "buckets.count"),
        (SS4O_METRICS, "name"),
        ("logs", "buckets"),
    ] {
        let (status, error) = fetch_error(&server, &format!("/{index}/_search"), &nested_body(path)).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{index} {path}: {error}");
        assert_eq!(
            error["type"],
            json!("illegal_argument_exception"),
            "{index} {path}: {error}"
        );
        assert_eq!(error["aggregation"], json!("objects"), "{index} {path}: {error}");
        assert_eq!(error["path"], json!(path), "{index} {path}: {error}");
        assert_eq!(error["index"], json!(index), "{index} {path}: {error}");
    }
    server.shutdown().await;
}

/// Inside `nested` only `filter` and the `sum`, `min`, `max`, `value_count` and `avg` metrics
/// run; any other aggregation there, at any depth, is a 400 naming it and its type. `nested`
/// itself runs at the top level only, and `reverse_nested` is no aggregation IceGate answers.
#[tokio::test]
async fn an_unsupported_aggregation_inside_nested_is_refused_naming_it() {
    let (server, catalog) = TestServer::start().await;
    commit_bin_count_metrics(&catalog).await.expect("commit metrics");
    let nested_body = |sub_aggregations: Value| json!({"size": 0, "aggs": {"objects": {"nested": {"path": "buckets"}, "aggs": sub_aggregations}}});

    for (body, aggregation, aggregation_type) in [
        (
            nested_body(json!({"by_max": {"terms": {"field": "buckets.max"}}})),
            "by_max",
            "terms",
        ),
        (
            nested_body(json!({"by_time": {"date_histogram": {"field": "time", "fixed_interval": "1m"}}})),
            "by_time",
            "date_histogram",
        ),
        (
            nested_body(json!({"inner": {"nested": {"path": "buckets"}}})),
            "inner",
            "nested",
        ),
        (
            nested_body(json!({"rows": {"reverse_nested": {}}})),
            "rows",
            "reverse_nested",
        ),
        (
            nested_body(json!({"upper": {
                "filter": {"range": {"buckets.min": {"gte": 1}}},
                "aggs": {"stats_count": {"stats": {"field": "buckets.count"}}}
            }})),
            "stats_count",
            "stats",
        ),
        (
            json!({"size": 0, "aggs": {"by_name": {
                "terms": {"field": "name"},
                "aggs": {"objects": {"nested": {"path": "buckets"}}}
            }}}),
            "objects",
            "nested",
        ),
    ] {
        let (status, error) = fetch_error(&server, SS4O_METRICS_SEARCH_PATH, &body).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {error}");
        assert_eq!(error["type"], json!("illegal_argument_exception"), "{body}: {error}");
        assert_eq!(error["aggregation"], json!(aggregation), "{body}: {error}");
        assert_eq!(error["aggregation_type"], json!(aggregation_type), "{body}: {error}");
    }
    let (status, error) = fetch_error(
        &server,
        SS4O_METRICS_SEARCH_PATH,
        &json!({"size": 0, "aggs": {"rows": {"reverse_nested": {}}}}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(error["type"], json!("parsing_exception"), "{error}");
    assert_eq!(error["construct"], json!("reverse_nested"), "{error}");
    server.shutdown().await;
}

/// Inside `nested`, a metric or a filter leaf reads the members of the nested objects only: a
/// field of the row is a 400 naming it and the path, and a query other than `term`, `terms`,
/// `range`, `exists`, `bool`, `constant_score`, `match_all` and `match_none` is a 400 naming it.
#[tokio::test]
async fn a_row_field_or_another_query_inside_nested_is_refused_naming_it() {
    let (server, catalog) = TestServer::start().await;
    commit_bin_count_metrics(&catalog).await.expect("commit metrics");
    let nested_body = |sub_aggregations: Value| json!({"size": 0, "aggs": {"objects": {"nested": {"path": "buckets"}, "aggs": sub_aggregations}}});

    for (body, field) in [
        (nested_body(json!({"value_sum": {"sum": {"field": "value"}}})), "value"),
        (
            nested_body(json!({"named": {"filter": {"term": {"name": HISTOGRAM_METRIC}}}})),
            "name",
        ),
        (
            nested_body(json!({"other": {"filter": {"bool": {"filter": [{"exists": {"field": "buckets.sum"}}]}}}})),
            "buckets.sum",
        ),
    ] {
        let (status, error) = fetch_error(&server, SS4O_METRICS_SEARCH_PATH, &body).await;

        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}: {error}");
        assert_eq!(error["type"], json!("illegal_argument_exception"), "{body}: {error}");
        assert_eq!(error["field"], json!(field), "{body}: {error}");
        assert_eq!(error["path"], json!("buckets"), "{body}: {error}");
    }
    let (status, error) = fetch_error(
        &server,
        SS4O_METRICS_SEARCH_PATH,
        &nested_body(json!({"matched": {"filter": {"match": {"buckets.count": 1}}}})),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(error["type"], json!("parsing_exception"), "{error}");
    assert_eq!(error["construct"], json!("match"), "{error}");
    server.shutdown().await;
}
