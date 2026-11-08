//! Aggregations over real HTTP. Each request is a literal, the rows are written by the test, and
//! every expected bucket is stated here.
#![allow(clippy::expect_used)]

use iceberg::spec::{NestedField, PrimitiveType, Type};
use icegate_common::LOGS_TABLE;
use icegate_common::schema::{COL_DURATION_MICROS, COL_SEVERITY_TEXT, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID};
use icegate_query::opensearch::OpenSearchConfig;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{
    build_log_row_at_minute, build_service_log_row_at_minute, build_wal_log_row_at_minute, commit_log_rows,
    create_exact_name_table, required_string, write_wal_segment,
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
