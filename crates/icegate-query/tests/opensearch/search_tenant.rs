//! Tenant isolation of the search surfaces over real HTTP: `_search`, `_msearch`, `ids`
//! lookups, the surrounding-documents view, aggregations and multi-index unions. Both tenants'
//! logs share one Parquet commit and one WAL segment; both tenants' spans share one document id.
#![allow(clippy::expect_used)]

use std::error::Error;
use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::schema::{COL_BODY, COL_NAME, COL_SERVICE_NAME, COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID};
use icegate_common::{LOGS_TABLE, SPANS_TABLE};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::answers::read_sorted_source_texts;
use super::data::{
    Cell, Row, RowWriteError, build_log_row_at_minute, build_wal_log_row_at_minute, commit_log_rows, commit_rows,
    compute_micros_at_minute, write_wal_segment,
};
use super::harness::TestServer;
use super::requests::{send_ndjson, send_request};

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// The minute both tenants' spans happen at, counted from the base of
/// [`compute_micros_at_minute`].
const SPAN_MINUTE: i64 = 10;

/// The trace and span ids both tenants' spans carry.
const SHARED_TRACE_ID: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
const SHARED_SPAN_ID: [u8; 8] = [0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8];
/// The lowercase hex of `SHARED_TRACE_ID` followed by `SHARED_SPAN_ID`: the document id of both
/// spans.
const SHARED_DOCUMENT_ID: &str = "0102030405060708090a0b0c0d0e0f10a1a2a3a4a5a6a7a8";

/// A `checkout` span of `tenant` at [`SPAN_MINUTE`] carrying the shared trace and span ids.
fn build_span_row<'a>(tenant: &'a str, name: &'a str) -> Row<'a> {
    Row {
        tenant,
        cells: vec![
            (COL_TIMESTAMP, Cell::Micros(compute_micros_at_minute(SPAN_MINUTE))),
            (COL_TRACE_ID, Cell::Bytes(&SHARED_TRACE_ID)),
            (COL_SPAN_ID, Cell::Bytes(&SHARED_SPAN_ID)),
            (COL_NAME, Cell::Text(name)),
            (COL_SERVICE_NAME, Cell::Text("checkout")),
        ],
    }
}

/// Tenant A: three logs (10:05 `INFO` and 10:07 `ERROR` in Parquet, 10:08 `INFO` in the WAL) and
/// the span `a-span`. Tenant B: two logs (10:06 `WARN` in Parquet, 10:09 `DEBUG` in the WAL) and
/// the span `b-span`.
async fn seed_both_tenants(server: &TestServer, catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_log_rows(
        catalog,
        &[
            build_log_row_at_minute(TENANT_A, 5, "INFO", "tenant-a parquet first"),
            build_log_row_at_minute(TENANT_B, 6, "WARN", "tenant-b parquet"),
            build_log_row_at_minute(TENANT_A, 7, "ERROR", "tenant-a parquet second"),
        ],
    )
    .await?;
    // Offset 1 follows the WAL offset the Parquet commit records.
    write_wal_segment(
        &server.wal_store,
        catalog,
        LOGS_TABLE,
        1,
        &[
            build_wal_log_row_at_minute(TENANT_A, 8, "INFO", "tenant-a wal"),
            build_wal_log_row_at_minute(TENANT_B, 9, "DEBUG", "tenant-b wal"),
        ],
    )
    .await?;
    commit_rows(
        catalog,
        SPANS_TABLE,
        &[build_span_row(TENANT_A, "a-span"), build_span_row(TENANT_B, "b-span")],
    )
    .await
}

/// `POST path` with `body` as `tenant` through [`send_request`]: the JSON answer of a `200`, and
/// an error naming the status and answer of anything else.
async fn fetch_ok_answer(server: &TestServer, tenant: &str, path: &str, body: &Value) -> Result<Value, Box<dyn Error>> {
    let (status, answer) = send_request(server, Method::POST, tenant, path, Some(body)).await?;
    if status != StatusCode::OK {
        return Err(format!("{path} as {tenant} answered {status}: {answer}").into());
    }
    Ok(answer)
}

#[tokio::test]
async fn search_msearch_ids_and_context_views_never_return_another_tenants_rows() {
    let (server, catalog) = TestServer::start().await;
    seed_both_tenants(&server, &catalog).await.expect("seed both tenants");
    let match_all_query = json!({"query": {"match_all": {}}, "size": 10});
    let shared_id_query = json!({"query": {"ids": {"values": [SHARED_DOCUMENT_ID]}}});
    let build_context_sort = |order: &str| {
        json!([
            {"timestamp": {"order": order, "unmapped_type": "boolean"}},
            {"_doc": {"order": order, "unmapped_type": "boolean"}}
        ])
    };
    let build_context_anchor = |id: &str| {
        json!({
            "query": {"bool": {"must": [{"constant_score": {"filter": {"ids": {"values": [id]}}}}]}},
            "size": 1,
            "version": true,
            "sort": build_context_sort("desc")
        })
    };

    let match_all_answer = fetch_ok_answer(&server, TENANT_B, "/logs/_search", &match_all_query)
        .await
        .expect("match all answer");
    let (multi_search_status, multi_search_answer) = send_ndjson(
        &server,
        TENANT_B,
        "/_msearch",
        concat!(
            r#"{"index":"logs"}"#,
            "\n",
            r#"{"query":{"match_all":{}},"size":10}"#,
            "\n"
        )
        .to_string(),
    )
    .await
    .expect("msearch request");
    let span_for_b = fetch_ok_answer(&server, TENANT_B, "/spans/_search", &shared_id_query)
        .await
        .expect("span for b");
    let span_for_a = fetch_ok_answer(&server, TENANT_A, "/spans/_search", &shared_id_query)
        .await
        .expect("span for a");
    let anchor_for_b = fetch_ok_answer(
        &server,
        TENANT_B,
        "/spans/_search",
        &build_context_anchor(SHARED_DOCUMENT_ID),
    )
    .await
    .expect("anchor for b");
    // A context view of tenant B's 10:06 log: its anchor, then the documents after it. Tenant A's
    // 10:07 and 10:08 logs lie between the anchor and tenant B's next log.
    let tenant_b_parquet_log = fetch_ok_answer(
        &server,
        TENANT_B,
        "/logs/_search",
        &json!({"query": {"match_phrase": {"body": "tenant-b parquet"}}}),
    )
    .await
    .expect("tenant b parquet log");
    assert_eq!(
        tenant_b_parquet_log["hits"]["total"],
        json!({"value": 1, "relation": "eq"})
    );
    let anchor_log_id = tenant_b_parquet_log["hits"]["hits"][0]["_id"]
        .as_str()
        .expect("_id")
        .to_string();
    let log_anchor_for_b = fetch_ok_answer(
        &server,
        TENANT_B,
        "/logs/_search",
        &build_context_anchor(&anchor_log_id),
    )
    .await
    .expect("log anchor for b");
    assert_eq!(log_anchor_for_b["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    let log_anchor_sort = log_anchor_for_b["hits"]["hits"][0]["sort"].clone();
    let successors_for_b = fetch_ok_answer(
        &server,
        TENANT_B,
        "/logs/_search",
        &json!({
            "query": {"bool": {"must": [{"bool": {
                "must": {"constant_score": {"filter": {"range": {"timestamp": {
                    "gte": "2026-10-08T10:06:00.000Z", "format": "strict_date_optional_time"
                }}}}},
                "must_not": {"ids": {"values": [anchor_log_id]}}
            }}]}},
            "sort": build_context_sort("asc"),
            "search_after": log_anchor_sort,
            "size": 5
        }),
    )
    .await
    .expect("successors for b");
    let tenant_a_wal_log = fetch_ok_answer(
        &server,
        TENANT_A,
        "/logs/_search",
        &json!({"query": {"match_phrase": {"body": "tenant-a wal"}}}),
    )
    .await
    .expect("tenant a wal log");
    assert_eq!(tenant_a_wal_log["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    let foreign_log_id = tenant_a_wal_log["hits"]["hits"][0]["_id"].as_str().expect("_id").to_string();
    let a_log_for_b = fetch_ok_answer(
        &server,
        TENANT_B,
        "/logs/_search",
        &json!({"query": {"ids": {"values": [foreign_log_id]}}}),
    )
    .await
    .expect("a log for b");

    assert_eq!(match_all_answer["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    assert_eq!(
        read_sorted_source_texts(&match_all_answer, COL_BODY).expect("sorted bodies"),
        vec!["tenant-b parquet", "tenant-b wal"]
    );
    assert_eq!(multi_search_status, StatusCode::OK, "{multi_search_answer}");
    assert_eq!(multi_search_answer["responses"][0]["status"], 200);
    assert_eq!(
        read_sorted_source_texts(&multi_search_answer["responses"][0], COL_BODY).expect("sorted bodies"),
        vec!["tenant-b parquet", "tenant-b wal"]
    );
    assert_eq!(span_for_b["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    assert_eq!(span_for_b["hits"]["hits"][0]["_id"], SHARED_DOCUMENT_ID);
    assert_eq!(
        read_sorted_source_texts(&span_for_b, COL_NAME).expect("sorted names"),
        vec!["b-span"]
    );
    assert_eq!(
        read_sorted_source_texts(&span_for_a, COL_NAME).expect("sorted names"),
        vec!["a-span"]
    );
    assert_eq!(
        read_sorted_source_texts(&anchor_for_b, COL_NAME).expect("sorted names"),
        vec!["b-span"]
    );
    assert_eq!(
        read_sorted_source_texts(&log_anchor_for_b, COL_BODY).expect("sorted bodies"),
        vec!["tenant-b parquet"]
    );
    assert_eq!(
        read_sorted_source_texts(&successors_for_b, COL_BODY).expect("sorted bodies"),
        vec!["tenant-b wal"]
    );
    assert_eq!(a_log_for_b["hits"]["total"], json!({"value": 0, "relation": "eq"}));
    server.shutdown().await;
}

#[tokio::test]
async fn terms_aggregations_never_count_another_tenants_rows() {
    let (server, catalog) = TestServer::start().await;
    seed_both_tenants(&server, &catalog).await.expect("seed both tenants");
    let severity_terms_query = json!({"size": 0, "aggs": {"levels": {"terms": {"field": "severity_text"}}}});

    let answer_for_b = fetch_ok_answer(&server, TENANT_B, "/logs/_search", &severity_terms_query)
        .await
        .expect("answer for b");
    let answer_for_a = fetch_ok_answer(&server, TENANT_A, "/logs/_search", &severity_terms_query)
        .await
        .expect("answer for a");

    // Tenant B's two keys tie at one document each and follow by key ascending.
    assert_eq!(
        answer_for_b["aggregations"],
        json!({"levels": {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [{"key": "DEBUG", "doc_count": 1}, {"key": "WARN", "doc_count": 1}]
        }})
    );
    assert_eq!(
        answer_for_a["aggregations"],
        json!({"levels": {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [{"key": "INFO", "doc_count": 2}, {"key": "ERROR", "doc_count": 1}]
        }})
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_multi_index_union_never_returns_another_tenants_rows() {
    let (server, catalog) = TestServer::start().await;
    seed_both_tenants(&server, &catalog).await.expect("seed both tenants");
    let match_all_query = json!({"query": {"match_all": {}}, "size": 10});

    let answer_for_b = fetch_ok_answer(&server, TENANT_B, "/logs,spans/_search", &match_all_query)
        .await
        .expect("answer for b");
    let answer_for_a = fetch_ok_answer(&server, TENANT_A, "/logs,spans/_search", &match_all_query)
        .await
        .expect("answer for a");

    let count_hits_of_index = |answer: &Value, index: &str| {
        answer["hits"]["hits"]
            .as_array()
            .expect("hits array")
            .iter()
            .filter(|hit| hit["_index"] == index)
            .count()
    };
    assert_eq!(answer_for_b["hits"]["total"], json!({"value": 3, "relation": "eq"}));
    assert_eq!(count_hits_of_index(&answer_for_b, "logs"), 2);
    assert_eq!(count_hits_of_index(&answer_for_b, "spans"), 1);
    for hit in answer_for_b["hits"]["hits"].as_array().expect("hits array") {
        let source = &hit["_source"];
        let source_text = source["body"]
            .as_str()
            .or_else(|| source["name"].as_str())
            .expect("a body or a name");
        assert!(
            source_text.starts_with("tenant-b") || source_text == "b-span",
            "{source_text}"
        );
    }
    assert_eq!(answer_for_a["hits"]["total"], json!({"value": 4, "relation": "eq"}));
    assert_eq!(count_hits_of_index(&answer_for_a, "logs"), 3);
    assert_eq!(count_hits_of_index(&answer_for_a, "spans"), 1);
    server.shutdown().await;
}
