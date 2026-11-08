//! Aggregations over real HTTP. Each request is a literal, the rows are written by the test, and
//! every expected bucket is stated here.
#![allow(clippy::expect_used)]

use iceberg::spec::{NestedField, PrimitiveType, Type};
use icegate_common::schema::{
    COL_DURATION_MICROS, COL_NAME, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP,
    COL_TRACE_ID,
};
use icegate_common::{LOGS_TABLE, SPANS_TABLE};
use icegate_query::opensearch::OpenSearchConfig;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{
    Cell, LogRow, Row, build_log_row_at_minute, build_service_log_row_at_minute, build_wal_log_row_at_minute,
    commit_log_rows, commit_rows, compute_micros_at_minute, create_exact_name_table, required_string,
    write_wal_segment,
};
use super::harness::TestServer;
use super::requests::send_request;
use super::test_rows::COMMITTED_WAL_OFFSET;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// An hourly histogram from 10:00Z with `extended_bounds.max` at `max` epoch milliseconds.
fn build_hourly_histogram_request(min_doc_count: u64, max: i64) -> Value {
    json!({
        "size": 0,
        "aggs": {"hours": {"date_histogram": {
            "field": "timestamp", "fixed_interval": "1h", "min_doc_count": min_doc_count,
            "extended_bounds": {"min": 1_791_453_600_000_i64, "max": max}
        }}}
    })
}

#[tokio::test]
async fn buckets_at_max_buckets_are_answered_and_one_more_is_a_400() {
    let mut config = OpenSearchConfig::default();
    config.search.max_buckets = 3;
    let (server, catalog) = TestServer::start_with_config(config).await;
    commit_log_rows(
        &catalog,
        &[
            build_log_row_at_minute(TENANT_A, 15, "INFO", "tenant-a first"),
            build_log_row_at_minute(TENANT_A, 135, "INFO", "tenant-a second"),
        ],
    )
    .await
    .expect("commit logs");

    // Below: two data buckets; the bounds apply only with `min_doc_count: 0`.
    let (below_status, below) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&build_hourly_histogram_request(1, 1_791_460_800_000)),
    )
    .await
    .expect("search request");
    // At: 10:00, 11:00 (empty) and 12:00.
    let (at_status, at) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&build_hourly_histogram_request(0, 1_791_460_800_000)),
    )
    .await
    .expect("search request");
    // Above: the bounds reach 13:00, a fourth bucket.
    let (above_status, above) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&build_hourly_histogram_request(0, 1_791_464_400_000)),
    )
    .await
    .expect("search request");

    assert_eq!(below_status, StatusCode::OK);
    assert_eq!(
        below["aggregations"]["hours"]["buckets"].as_array().map(Vec::len),
        Some(2)
    );
    assert_eq!(at_status, StatusCode::OK);
    assert_eq!(
        at["aggregations"]["hours"]["buckets"],
        json!([
            {"key_as_string": "2026-10-08T10:00:00.000Z", "key": 1_791_453_600_000_i64, "doc_count": 1},
            {"key_as_string": "2026-10-08T11:00:00.000Z", "key": 1_791_457_200_000_i64, "doc_count": 0},
            {"key_as_string": "2026-10-08T12:00:00.000Z", "key": 1_791_460_800_000_i64, "doc_count": 1}
        ])
    );
    assert_eq!(above_status, StatusCode::BAD_REQUEST);
    assert_eq!(above["error"]["type"], "search_phase_execution_exception");
    assert_eq!(above["error"]["root_cause"][0]["type"], "too_many_buckets_exception");
    assert_eq!(above["error"]["root_cause"][0]["max_buckets"], 3);
    server.shutdown().await;
}

#[tokio::test]
async fn a_discover_style_histogram_request_answers_keys_and_counts() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[
            build_log_row_at_minute(TENANT_A, 5, "ERROR", "tenant-a parquet 10:05"),
            build_log_row_at_minute(TENANT_B, 10, "ERROR", "tenant-b parquet 10:10"),
            build_log_row_at_minute(TENANT_A, 20, "INFO", "tenant-a parquet 10:20"),
            build_log_row_at_minute(TENANT_A, 70, "INFO", "tenant-a parquet 11:10"),
        ],
    )
    .await
    .expect("commit logs");
    // The segment follows the WAL offset the Parquet commit records.
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        COMMITTED_WAL_OFFSET + 1,
        &[
            build_wal_log_row_at_minute(TENANT_A, 40, "WARN", "tenant-a wal 10:40"),
            build_wal_log_row_at_minute(TENANT_B, 50, "WARN", "tenant-b wal 10:50"),
        ],
    )
    .await
    .expect("write WAL segment");
    let body = json!({
        "size": 0,
        "query": {"bool": {
            "must": [],
            "filter": [{"range": {"timestamp": {
                "gte": "2026-10-08T10:00:00.000Z", "lte": "2026-10-08T11:59:59.999Z", "format": "strict_date_optional_time"
            }}}],
            "should": [],
            "must_not": []
        }},
        "aggs": {"2": {"date_histogram": {
            "field": "timestamp", "fixed_interval": "30m", "time_zone": "Europe/Berlin", "min_doc_count": 1
        }}}
    });
    let path = "/logs/_search?ignore_unavailable=true&track_total_hits=true&timeout=30000ms&preference=1760000000000";

    let (status_a, answer_a) = send_request(&server, Method::POST, TENANT_A, path, Some(&body))
        .await
        .expect("search request");
    let (status_b, answer_b) = send_request(&server, Method::POST, TENANT_B, path, Some(&body))
        .await
        .expect("search request");

    assert_eq!(status_a, StatusCode::OK);
    assert_eq!(answer_a["hits"]["total"], json!({"value": 4, "relation": "eq"}));
    assert_eq!(
        answer_a["aggregations"],
        json!({"2": {"buckets": [
            {"key_as_string": "2026-10-08T12:00:00.000+02:00", "key": 1_791_453_600_000_i64, "doc_count": 2},
            {"key_as_string": "2026-10-08T12:30:00.000+02:00", "key": 1_791_455_400_000_i64, "doc_count": 1},
            {"key_as_string": "2026-10-08T13:00:00.000+02:00", "key": 1_791_457_200_000_i64, "doc_count": 1}
        ]}})
    );
    assert_eq!(status_b, StatusCode::OK);
    assert_eq!(answer_b["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    assert_eq!(
        answer_b["aggregations"],
        json!({"2": {"buckets": [
            {"key_as_string": "2026-10-08T12:00:00.000+02:00", "key": 1_791_453_600_000_i64, "doc_count": 1},
            {"key_as_string": "2026-10-08T12:30:00.000+02:00", "key": 1_791_455_400_000_i64, "doc_count": 1}
        ]}})
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_histogram_over_a_keyword_field_is_a_400_naming_it() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(&catalog, &[build_log_row_at_minute(TENANT_A, 5, "INFO", "tenant-a")])
        .await
        .expect("commit logs");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(
            &json!({"size": 0, "aggs": {"2": {"date_histogram": {"field": "severity_text", "fixed_interval": "1h"}}}}),
        ),
    )
    .await
    .expect("search request");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(answer["error"]["field"], "severity_text");
    assert_eq!(answer["error"]["field_type"], "keyword");
    assert_eq!(answer["error"]["index"], "logs");
    assert_eq!(answer["error"]["aggregation"], "2");
    assert_eq!(answer["error"]["aggregation_type"], "date_histogram");
    server.shutdown().await;
}

/// An exact-name table whose `severity_text` is a `long`, beside `logs`, whose `severity_text` is
/// a keyword. It is created before the listener's first session, so the first catalog listing
/// holds it.
#[tokio::test]
async fn terms_over_a_field_typed_differently_across_indices_is_an_aggregation_execution_400() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(
        &catalog,
        "audit_events",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, COL_SEVERITY_TEXT, Type::Primitive(PrimitiveType::Long)),
        ],
    )
    .await
    .expect("create audit_events");
    commit_log_rows(&catalog, &[build_log_row_at_minute(TENANT_A, 5, "INFO", "tenant-a")])
        .await
        .expect("commit logs");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs,audit_events/_search",
        Some(&json!({"size": 0, "aggs": {"levels": {"terms": {"field": "severity_text"}}}})),
    )
    .await
    .expect("search request");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(answer["error"]["type"], "aggregation_execution_exception");
    assert_eq!(answer["error"]["field"], "severity_text");
    assert_eq!(
        answer["error"]["indices_by_type"],
        json!({"keyword": ["logs"], "long": ["audit_events"]})
    );
    server.shutdown().await;
}

/// An exact-name table whose `trace_id` is a plain string, beside `logs`, whose `trace_id` is an
/// encoded id. Both map the field as a keyword, but the rows plan cannot hold both stored types in
/// one column.
#[tokio::test]
async fn terms_over_ids_stored_differently_across_indices_is_an_aggregation_execution_400() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(
        &catalog,
        "audit_trace",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, COL_TRACE_ID, Type::Primitive(PrimitiveType::String)),
        ],
    )
    .await
    .expect("create audit_trace");
    commit_log_rows(&catalog, &[build_log_row_at_minute(TENANT_A, 5, "INFO", "tenant-a")])
        .await
        .expect("commit logs");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs,audit_trace/_search",
        Some(&json!({"size": 0, "aggs": {"traces": {"terms": {"field": "trace_id"}}}})),
    )
    .await
    .expect("search request");

    // The stored types are named as Arrow writes them: the encoded `trace_id` of `logs` is a
    // 16-byte fixed-size binary, the exact-name table's string is `Utf8`.
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(answer["error"]["type"], "aggregation_execution_exception");
    assert_eq!(answer["error"]["field"], "trace_id");
    assert_eq!(
        answer["error"]["indices_by_type"],
        json!({"FixedSizeBinary(16)": ["logs"], "Utf8": ["audit_trace"]})
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_search_exceeding_the_query_memory_limit_is_a_400_naming_the_limit() {
    let config = OpenSearchConfig {
        enabled: true,
        max_query_memory_bytes: 1024,
        ..OpenSearchConfig::default()
    };
    let (server, catalog) = TestServer::start_with_config(config).await;

    // 2000 distinct service names, one row each: the grouped aggregate's state alone is far
    // above 1 KiB, whatever the batch layout, so the pool refuses it.
    let services: Vec<String> = (0..2000).map(|index| format!("service-{index:04}")).collect();
    let rows: Vec<_> = services
        .iter()
        .zip(0_i64..)
        .map(|(service, minute)| {
            build_service_log_row_at_minute(
                TENANT_A,
                minute,
                service.as_str(),
                "INFO",
                "a row of the memory-limit test",
            )
        })
        .collect();
    commit_log_rows(&catalog, &rows).await.expect("commit the rows");

    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&json!({
            "size": 0,
            "aggs": {"services": {"terms": {"field": "service_name", "size": 10}}}
        })),
    )
    .await
    .expect("send the search");

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["status"], 400);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    assert_eq!(body["error"]["root_cause"][0]["type"], "illegal_argument_exception");
    assert_eq!(body["error"]["limit"], 1024);

    server.shutdown().await;
}

/// `spans` maps `duration_micros` as a long; the exact-name table `durations_as_text` maps it as
/// a keyword. The table is created before the listener's first session, so the first catalog
/// listing holds it.
#[tokio::test]
async fn a_metric_or_histogram_over_a_field_typed_differently_across_indices_is_an_aggregation_execution_400() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(
        &catalog,
        "durations_as_text",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, COL_DURATION_MICROS, Type::Primitive(PrimitiveType::String)),
        ],
    )
    .await
    .expect("create durations_as_text");

    for aggregation in [
        json!({"avg": {"field": "duration_micros"}}),
        json!({"histogram": {"field": "duration_micros", "interval": 100}}),
    ] {
        let (status, answer) = send_request(
            &server,
            Method::POST,
            TENANT_A,
            "/spans,durations_as_text/_search",
            Some(&json!({"size": 0, "aggs": {"by_duration": aggregation.clone()}})),
        )
        .await
        .expect("search request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{aggregation}");
        assert_eq!(
            answer["error"]["type"], "aggregation_execution_exception",
            "{aggregation}"
        );
        assert_eq!(answer["error"]["field"], COL_DURATION_MICROS, "{aggregation}");
        assert_eq!(
            answer["error"]["indices_by_type"],
            json!({"keyword": ["durations_as_text"], "long": ["spans"]}),
            "{aggregation}"
        );
    }
    server.shutdown().await;
}

/// A row of the exact-name table `audit_events`: `service_name` at `minute` past 10:00Z.
fn build_audit_row<'a>(tenant: &'a str, minute: i64, service_name: &'a str) -> Row<'a> {
    Row {
        tenant,
        cells: vec![
            (COL_TIMESTAMP, Cell::Micros(compute_micros_at_minute(minute))),
            (COL_SERVICE_NAME, Cell::Text(service_name)),
        ],
    }
}

/// `logs` maps `service_name` and `severity_text`; `audit_events` maps `service_name` only, so a
/// bucket query on `severity_text` matches none of its rows. The table is created before the
/// listener's first session, so the first catalog listing holds it.
#[tokio::test]
async fn filter_and_filters_buckets_lower_their_queries_per_index() {
    let (server, catalog) = TestServer::start().await;
    create_exact_name_table(
        &catalog,
        "audit_events",
        vec![
            required_string(1, COL_TENANT_ID),
            NestedField::required(2, COL_TIMESTAMP, Type::Primitive(PrimitiveType::Timestamp)),
            NestedField::optional(3, COL_SERVICE_NAME, Type::Primitive(PrimitiveType::String)),
        ],
    )
    .await
    .expect("create audit_events");
    // Tenant B's rows would raise `errors` to 3 and `checkout_only` to 6 if they leaked.
    commit_log_rows(
        &catalog,
        &[
            build_service_log_row_at_minute(TENANT_A, 5, "checkout", "ERROR", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_B, 7, "checkout", "ERROR", "a tenant-b row"),
            build_service_log_row_at_minute(TENANT_A, 10, "cart", "ERROR", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_A, 20, "checkout", "INFO", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_A, 30, "checkout", "WARN", "an aggregation row"),
        ],
    )
    .await
    .expect("commit logs");
    commit_rows(
        &catalog,
        "audit_events",
        &[
            build_audit_row(TENANT_A, 15, "checkout"),
            build_audit_row(TENANT_B, 16, "checkout"),
            build_audit_row(TENANT_A, 25, "cart"),
        ],
    )
    .await
    .expect("commit audit_events");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs,audit_events/_search",
        Some(&json!({
            "size": 0,
            "aggs": {
                "levels": {"filters": {"filters": {
                    "errors": {"term": {"severity_text": "ERROR"}},
                    "infos": {"term": {"severity_text": "INFO"}}
                }}},
                "checkout_only": {
                    "filter": {"term": {"service_name": "checkout"}},
                    "aggs": {"severities": {"terms": {"field": "severity_text"}}}
                }
            }
        })),
    )
    .await
    .expect("search request");

    // errors: the two ERROR logs; infos: the INFO log. checkout_only: three logs and one
    // audit_events row, whose missing severity forms no severities bucket.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        answer["aggregations"],
        json!({
            "levels": {"buckets": {"errors": {"doc_count": 2}, "infos": {"doc_count": 1}}},
            "checkout_only": {
                "doc_count": 4,
                "severities": {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": [
                    {"key": "ERROR", "doc_count": 1},
                    {"key": "INFO", "doc_count": 1},
                    {"key": "WARN", "doc_count": 1}
                ]}
            }
        })
    );
    server.shutdown().await;
}

/// Both tenants share services and severities in one commit, so a row read across tenants
/// changes a count.
#[tokio::test]
async fn filters_date_ranges_and_cardinality_count_only_the_requesting_tenants_rows() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[
            build_service_log_row_at_minute(TENANT_A, 5, "checkout", "ERROR", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_A, 10, "cart", "ERROR", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_A, 20, "checkout", "INFO", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_A, 40, "cart", "INFO", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_A, 70, "search", "INFO", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_B, 5, "checkout", "ERROR", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_B, 15, "checkout", "INFO", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_B, 50, "cart", "WARN", "an aggregation row"),
            build_service_log_row_at_minute(TENANT_B, 55, "payments", "WARN", "an aggregation row"),
        ],
    )
    .await
    .expect("commit logs");
    // `levels` and `windows` overlap, so both unnest; the cardinalities run in branches of their
    // own.
    let body = json!({
        "size": 0,
        "aggs": {
            "levels": {
                "filters": {"filters": {
                    "errors": {"term": {"severity_text": "ERROR"}},
                    "all": {"match_all": {}}
                }},
                "aggs": {"services": {"cardinality": {"field": "service_name"}}}
            },
            "windows": {"date_range": {"field": "timestamp", "ranges": [
                {"from": "2026-10-08T10:00:00.000Z", "to": "2026-10-08T10:30:00.000Z"},
                {"from": "2026-10-08T10:15:00.000Z", "to": "2026-10-08T11:00:00.000Z"}
            ]}},
            "services": {"cardinality": {"field": "service_name"}}
        }
    });
    let windows = |early: u64, late: u64| {
        json!({"buckets": [
            {
                "key": "2026-10-08T10:00:00.000Z-2026-10-08T10:30:00.000Z",
                "from": 1_791_453_600_000.0, "from_as_string": "2026-10-08T10:00:00.000Z",
                "to": 1_791_455_400_000.0, "to_as_string": "2026-10-08T10:30:00.000Z",
                "doc_count": early
            },
            {
                "key": "2026-10-08T10:15:00.000Z-2026-10-08T11:00:00.000Z",
                "from": 1_791_454_500_000.0, "from_as_string": "2026-10-08T10:15:00.000Z",
                "to": 1_791_457_200_000.0, "to_as_string": "2026-10-08T11:00:00.000Z",
                "doc_count": late
            }
        ]})
    };

    let (status_a, answer_a) = send_request(&server, Method::POST, TENANT_A, "/logs/_search", Some(&body))
        .await
        .expect("search request");
    let (status_b, answer_b) = send_request(&server, Method::POST, TENANT_B, "/logs/_search", Some(&body))
        .await
        .expect("search request");

    // tenant-a: errors {checkout, cart}; all five rows {checkout, cart, search}; 10:00–10:30
    // holds minutes 5, 10 and 20, 10:15–11:00 minutes 20 and 40.
    assert_eq!(status_a, StatusCode::OK);
    assert_eq!(
        answer_a["aggregations"],
        json!({
            "levels": {"buckets": {
                "errors": {"doc_count": 2, "services": {"value": 2}},
                "all": {"doc_count": 5, "services": {"value": 3}}
            }},
            "windows": windows(3, 2),
            "services": {"value": 3}
        })
    );
    // tenant-b: errors {checkout}; all four rows {checkout, cart, payments}; 10:00–10:30 holds
    // minutes 5 and 15, 10:15–11:00 minutes 15, 50 and 55.
    assert_eq!(status_b, StatusCode::OK);
    assert_eq!(
        answer_b["aggregations"],
        json!({
            "levels": {"buckets": {
                "errors": {"doc_count": 1, "services": {"value": 1}},
                "all": {"doc_count": 4, "services": {"value": 3}}
            }},
            "windows": windows(2, 3),
            "services": {"value": 3}
        })
    );
    server.shutdown().await;
}

/// The instant [`compute_micros_at_minute`] gives minute 0, in epoch milliseconds: the first minute
/// of the rows the aggregation integration tests write.
pub(super) const BASE_MILLIS: i64 = compute_micros_at_minute(0) / 1_000;

/// Tenant A logs at 10:01–10:05 (INFO a1, ERROR a2, INFO a3, ERROR a4, INFO a5) and one tenant B
/// ERROR at 10:06, newer than every tenant A row, so a hit leaking across tenants tops its bucket.
const fn build_top_hits_logs() -> [LogRow<'static>; 6] {
    [
        build_log_row_at_minute(TENANT_A, 1, "INFO", "a1"),
        build_log_row_at_minute(TENANT_A, 2, "ERROR", "a2"),
        build_log_row_at_minute(TENANT_A, 3, "INFO", "a3"),
        build_log_row_at_minute(TENANT_A, 4, "ERROR", "a4"),
        build_log_row_at_minute(TENANT_A, 5, "INFO", "a5"),
        build_log_row_at_minute(TENANT_B, 6, "ERROR", "b6"),
    ]
}

/// The `body` of every hit of the `top_hits` result `top_hits`, in hit order.
fn list_hit_bodies(top_hits: &Value) -> Vec<Value> {
    top_hits["hits"]["hits"]
        .as_array()
        .expect("a hits array")
        .iter()
        .map(|hit| hit["_source"]["body"].clone())
        .collect()
}

#[tokio::test]
async fn top_hits_render_through_the_hits_renderer_per_bucket() {
    let (server, catalog) = TestServer::start().await;
    // (tenant, minute, service, id byte): trace id = 16 × byte, span id = 8 × byte.
    let spans: [(&str, i64, &str, u8); 6] = [
        (TENANT_A, 0, "checkout", 1),
        (TENANT_A, 1, "checkout", 2),
        (TENANT_A, 2, "checkout", 3),
        (TENANT_A, 0, "cart", 4),
        (TENANT_A, 1, "cart", 5),
        // tenant-b's latest checkout span would top tenant-a's checkout bucket.
        (TENANT_B, 3, "checkout", 6),
    ];
    let trace_ids: Vec<[u8; 16]> = spans.iter().map(|span| [span.3; 16]).collect();
    let span_ids: Vec<[u8; 8]> = spans.iter().map(|span| [span.3; 8]).collect();
    let rows: Vec<Row<'_>> = spans
        .iter()
        .enumerate()
        .map(|(index, &(tenant, minute, service, _))| Row {
            tenant,
            cells: vec![
                (COL_TIMESTAMP, Cell::Micros(compute_micros_at_minute(minute))),
                (COL_SERVICE_NAME, Cell::Text(service)),
                (COL_NAME, Cell::Text("GET /cart")),
                (COL_TRACE_ID, Cell::Bytes(&trace_ids[index])),
                (COL_SPAN_ID, Cell::Bytes(&span_ids[index])),
            ],
        })
        .collect();
    write_wal_segment(&server.wal_store, &catalog, SPANS_TABLE, 0, &rows)
        .await
        .expect("one WAL segment holding both tenants");
    let (status, body) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/spans/_search",
        Some(&json!({
            "size": 0,
            "aggs": {"2": {
                "terms": {"field": "service_name", "order": {"_key": "asc"}},
                "aggs": {"1": {"top_hits": {
                    "size": 2,
                    "sort": [{"timestamp": {"order": "desc"}}],
                    "_source": {"includes": ["service_name", "name"]},
                    "docvalue_fields": [{"field": "timestamp", "format": "strict_date_time"}]
                }}}
            }}
        })),
    )
    .await
    .expect("search request");
    assert_eq!(status, StatusCode::OK, "{body}");
    let buckets = body["aggregations"]["2"]["buckets"].as_array().expect("terms buckets");
    assert_eq!(buckets.len(), 2);
    // (service, bucket total, [(id byte, minute)] newest first)
    let expected = [("cart", 2, [(5_u8, 1_i64), (4, 0)]), ("checkout", 3, [(3, 2), (2, 1)])];
    for (bucket, (service, total, hits)) in buckets.iter().zip(expected) {
        assert_eq!(bucket["key"], json!(service));
        let top = &bucket["1"]["hits"];
        assert_eq!(top["total"], json!({"value": total, "relation": "eq"}), "{service}");
        assert_eq!(top["max_score"], json!(null), "{service}");
        let bucket_hits = top["hits"].as_array().expect("hits array");
        assert_eq!(bucket_hits.len(), 2, "{service}");
        for (hit, (id_byte, minute)) in bucket_hits.iter().zip(hits) {
            // A span's `_id` is the lowercase hex of trace id ‖ span id.
            assert_eq!(hit["_id"], json!(format!("{id_byte:02x}").repeat(24)), "{service}");
            assert_eq!(hit["_index"], json!("spans"));
            assert_eq!(hit["_score"], json!(null));
            assert_eq!(hit["_source"], json!({"service_name": service, "name": "GET /cart"}));
            assert_eq!(hit["sort"], json!([BASE_MILLIS + minute * 60_000]));
            assert_eq!(
                hit["fields"]["timestamp"],
                json!([format!("2026-10-08T10:0{minute}:00.000Z")])
            );
        }
    }
    server.shutdown().await;
}

#[tokio::test]
async fn top_hits_at_the_top_and_under_filters_list_a_row_in_every_bucket_it_matches() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(&catalog, &build_top_hits_logs()).await.expect("commit logs");
    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&json!({
            "size": 0,
            "aggs": {
                "newest": {"top_hits": {
                    "size": 1,
                    "sort": [{"timestamp": {"order": "desc"}}],
                    "_source": {"includes": ["body"]}
                }},
                "none": {"top_hits": {"size": 0}},
                "split": {
                    "filters": {"filters": {
                        "all": {"match_all": {}},
                        "errors": {"term": {"severity_text": "ERROR"}}
                    }},
                    "aggs": {"latest": {"top_hits": {
                        "size": 2,
                        "sort": [{"timestamp": {"order": "desc"}}],
                        "_source": {"includes": ["body"]}
                    }}}
                }
            }
        })),
    )
    .await
    .expect("search request");
    assert_eq!(status, StatusCode::OK, "{answer}");
    let aggregations = &answer["aggregations"];
    assert_eq!(
        aggregations["newest"]["hits"]["total"],
        json!({"value": 5, "relation": "eq"})
    );
    assert_eq!(list_hit_bodies(&aggregations["newest"]), [json!("a5")]);
    assert_eq!(
        aggregations["none"],
        json!({"hits": {"total": {"value": 5, "relation": "eq"}, "max_score": null, "hits": []}})
    );
    let all = &aggregations["split"]["buckets"]["all"];
    assert_eq!(all["doc_count"], json!(5));
    assert_eq!(list_hit_bodies(&all["latest"]), [json!("a5"), json!("a4")]);
    let errors = &aggregations["split"]["buckets"]["errors"];
    assert_eq!(errors["doc_count"], json!(2));
    assert_eq!(errors["latest"]["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    // a4 belongs to both buckets, so it is a hit of both.
    assert_eq!(list_hit_bodies(&errors["latest"]), [json!("a4"), json!("a2")]);
    assert_eq!(
        errors["latest"]["hits"]["hits"][0]["sort"],
        json!([compute_micros_at_minute(4) / 1_000])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn top_hits_under_a_cut_terms_level_list_the_kept_buckets_hits() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(&catalog, &build_top_hits_logs()).await.expect("commit logs");
    // `size: 1` in key order keeps ERROR, whose key reaches the branch through the
    // surviving-bucket join; INFO holds the newest tenant A row.
    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&json!({
            "size": 0,
            "aggs": {"severities": {
                "terms": {"field": "severity_text", "size": 1, "order": {"_key": "asc"}},
                "aggs": {"latest": {"top_hits": {
                    "size": 1,
                    "sort": [{"timestamp": "desc"}],
                    "_source": {"includes": ["body"]}
                }}}
            }}
        })),
    )
    .await
    .expect("search request");
    assert_eq!(status, StatusCode::OK, "{answer}");
    let severities = &answer["aggregations"]["severities"];
    assert_eq!(severities["sum_other_doc_count"], json!(3));
    let buckets = severities["buckets"].as_array().expect("terms buckets");
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["key"], json!("ERROR"));
    assert_eq!(
        buckets[0]["latest"]["hits"]["total"],
        json!({"value": 2, "relation": "eq"})
    );
    assert_eq!(list_hit_bodies(&buckets[0]["latest"]), [json!("a4")]);
    server.shutdown().await;
}

#[tokio::test]
async fn a_top_hits_over_two_indices_and_an_extended_stats_over_a_keyword_are_400s_naming_them() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(&catalog, &build_top_hits_logs()).await.expect("commit logs");
    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs,spans/_search",
        Some(&json!({"size": 0, "aggs": {"latest": {"top_hits": {}}}})),
    )
    .await
    .expect("search request");
    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(answer["error"]["aggregation"], "latest");
    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&json!({"size": 0, "aggs": {"spread": {"extended_stats": {"field": "severity_text"}}}})),
    )
    .await
    .expect("search request");
    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    assert_eq!(answer["error"]["field"], "severity_text");
    assert_eq!(answer["error"]["field_type"], "keyword");
    assert_eq!(answer["error"]["index"], "logs");
    assert_eq!(answer["error"]["aggregation"], "spread");
    assert_eq!(answer["error"]["aggregation_type"], "extended_stats");
    server.shutdown().await;
}
