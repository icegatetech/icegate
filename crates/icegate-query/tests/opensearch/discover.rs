//! Discover-style requests over real HTTP: the document table with its histogram, the
//! single-document view and the surrounding-documents view. Every request is a literal, and every
//! expected hit, sort value and bucket is stated here.
//!
//! Every test reads tenant A's five logs of [`commit_five_checkout_and_cart_logs`]: 10:05 checkout
//! ERROR "payment declined for order 17", 10:20 checkout INFO "payment accepted for order 18",
//! 10:40 cart ERROR "cart service unavailable", 11:10 checkout ERROR "payment declined for order
//! 19" and 12:30 checkout ERROR "payment declined for order 20".
#![allow(clippy::expect_used)]

use std::error::Error;

use icegate_common::schema::COL_BODY;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::answers::read_source_texts;
use super::data::commit_five_checkout_and_cart_logs;
use super::harness::TestServer;
use super::requests::send_request;

const TENANT_A: &str = "tenant-a";

/// The `_id` and `_index` of the one log whose severity is `INFO` (10:20). Any answer but a `200`
/// with exactly one hit is an error.
async fn find_info_log(server: &TestServer) -> Result<(String, String), Box<dyn Error>> {
    let (status, answer) = send_request(
        server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&json!({"query": {"term": {"severity_text": "INFO"}}})),
    )
    .await?;
    if status != StatusCode::OK || answer["hits"]["total"] != json!({"value": 1, "relation": "eq"}) {
        return Err(format!("the INFO lookup answered {status}: {answer}").into());
    }
    let hit = &answer["hits"]["hits"][0];
    let id = hit["_id"].as_str().ok_or_else(|| format!("no _id in {hit}"))?;
    let index = hit["_index"].as_str().ok_or_else(|| format!("no _index in {hit}"))?;
    Ok((id.to_string(), index.to_string()))
}

#[tokio::test]
async fn a_discover_style_page_with_histogram_returns_the_stated_hits_total_and_buckets() {
    let (server, catalog) = TestServer::start().await;
    commit_five_checkout_and_cart_logs(&catalog, TENANT_A)
        .await
        .expect("commit five logs");
    let body = json!({
        "query": {"bool": {
            "must": [{"query_string": {
                "query": "severity_text:ERROR AND service_name:checkout",
                "analyze_wildcard": true,
                "time_zone": "Europe/Berlin"
            }}],
            "filter": [{"range": {"timestamp": {
                "gte": "2026-10-08T10:00:00.000Z", "lte": "2026-10-08T11:59:59.999Z", "format": "strict_date_optional_time"
            }}}],
            "should": [],
            "must_not": []
        }},
        "sort": [{"timestamp": {"order": "desc", "unmapped_type": "boolean"}}],
        "size": 500,
        "version": true,
        "stored_fields": ["*"],
        "script_fields": {},
        "docvalue_fields": [{"field": "timestamp", "format": "date_time"}],
        "_source": {"excludes": []},
        "highlight": {
            "pre_tags": ["@opensearch-dashboards-highlighted-field@"],
            "post_tags": ["@/opensearch-dashboards-highlighted-field@"],
            "fields": {"*": {}},
            "fragment_size": 2_147_483_647
        },
        "aggs": {"2": {"date_histogram": {
            "field": "timestamp", "fixed_interval": "30m", "time_zone": "Europe/Berlin", "min_doc_count": 1
        }}}
    });

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search?ignore_unavailable=true&track_total_hits=true&timeout=30000ms&preference=1760000000000",
        Some(&body),
    )
    .await
    .expect("Discover page request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer["timed_out"], false);
    assert_eq!(
        answer["_shards"],
        json!({"total": 1, "successful": 1, "skipped": 0, "failed": 0})
    );
    assert_eq!(answer["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    assert_eq!(answer["hits"]["max_score"], Value::Null);
    assert_eq!(
        read_source_texts(&answer, COL_BODY).expect("hit bodies"),
        vec!["payment declined for order 19", "payment declined for order 17"]
    );
    let hits = answer["hits"]["hits"].as_array().expect("hits array");
    for (hit, (sort_millis, rendered)) in hits.iter().zip([
        (1_791_457_800_000_i64, "2026-10-08T11:10:00.000Z"),
        (1_791_453_900_000_i64, "2026-10-08T10:05:00.000Z"),
    ]) {
        assert_eq!(hit["_index"], "logs");
        assert_eq!(hit["_version"], 1);
        assert_eq!(hit["_score"], Value::Null);
        assert_eq!(hit["sort"], json!([sort_millis]));
        assert_eq!(hit["fields"]["timestamp"], json!([rendered]));
        assert!(hit.get("highlight").is_none(), "no highlight is answered");
    }
    assert_eq!(
        answer["aggregations"],
        json!({"2": {"buckets": [
            {"key_as_string": "2026-10-08T12:00:00.000+02:00", "key": 1_791_453_600_000_i64, "doc_count": 1},
            {"key_as_string": "2026-10-08T13:00:00.000+02:00", "key": 1_791_457_200_000_i64, "doc_count": 1}
        ]}})
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_single_document_style_ids_lookup_through_the_hit_index_returns_the_row() {
    let (server, catalog) = TestServer::start().await;
    commit_five_checkout_and_cart_logs(&catalog, TENANT_A)
        .await
        .expect("commit five logs");
    let (id, index) = find_info_log(&server).await.expect("the INFO log");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        &format!("/{index}/_search"),
        Some(&json!({
            "query": {"ids": {"values": [id]}},
            "stored_fields": ["*"],
            "_source": true,
            "script_fields": {},
            "docvalue_fields": [{"field": "timestamp", "format": "date_time"}]
        })),
    )
    .await
    .expect("single-document request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    assert_eq!(answer["hits"]["hits"][0]["_id"], json!(id));
    assert_eq!(
        read_source_texts(&answer, COL_BODY).expect("hit bodies"),
        vec!["payment accepted for order 18"]
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_context_style_anchor_lookup_returns_the_anchor_and_its_sort_values() {
    let (server, catalog) = TestServer::start().await;
    commit_five_checkout_and_cart_logs(&catalog, TENANT_A)
        .await
        .expect("commit five logs");
    let (id, _) = find_info_log(&server).await.expect("the INFO log");
    let sort = json!([
        {"timestamp": {"order": "desc", "unmapped_type": "boolean"}},
        {"_doc": {"order": "desc", "unmapped_type": "boolean"}}
    ]);

    let (anchor_status, anchor) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&json!({
            "query": {"bool": {"must": [{"constant_score": {"filter": {"ids": {"values": [id]}}}}]}},
            "size": 1,
            "version": true,
            "sort": sort
        })),
    )
    .await
    .expect("anchor request");
    assert_eq!(anchor_status, StatusCode::OK, "{anchor}");
    assert_eq!(
        read_source_texts(&anchor, COL_BODY).expect("anchor body"),
        vec!["payment accepted for order 18"]
    );
    let anchor_sort = anchor["hits"]["hits"][0]["sort"].as_array().expect("sort values").clone();
    assert_eq!(anchor_sort.len(), 2);
    assert_eq!(
        anchor_sort[0],
        json!(1_791_454_800_000_i64),
        "the anchor's epoch millisecond"
    );
    // The anchor's microsecond is zero, so its `_doc` value is the low 43 bits of its row hash.
    let doc_sort_value = anchor_sort[1].as_i64().expect("an integer _doc value");
    assert!((0..1_i64 << 43).contains(&doc_sort_value), "{doc_sort_value}");

    let build_neighbour_request = |bound: &str, order: &str| {
        json!({
            "query": {"bool": {"must": [{"bool": {
                "must": {"constant_score": {"filter": {"range": {"timestamp": {
                    bound: "2026-10-08T10:20:00.000Z", "format": "strict_date_optional_time"
                }}}}},
                "must_not": {"ids": {"values": [id]}}
            }}]}},
            "sort": [
                {"timestamp": {"order": order, "unmapped_type": "boolean"}},
                {"_doc": {"order": order, "unmapped_type": "boolean"}}
            ],
            "search_after": anchor_sort,
            "size": 5
        })
    };
    let (successors_status, successors) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&build_neighbour_request("gte", "asc")),
    )
    .await
    .expect("successors request");
    let (predecessors_status, predecessors) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/logs/_search",
        Some(&build_neighbour_request("lte", "desc")),
    )
    .await
    .expect("predecessors request");

    assert_eq!(successors_status, StatusCode::OK, "{successors}");
    assert_eq!(
        read_source_texts(&successors, COL_BODY).expect("successor bodies"),
        vec![
            "cart service unavailable",
            "payment declined for order 19",
            "payment declined for order 20"
        ]
    );
    assert_eq!(predecessors_status, StatusCode::OK, "{predecessors}");
    assert_eq!(
        read_source_texts(&predecessors, COL_BODY).expect("predecessor bodies"),
        vec!["payment declined for order 17"]
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_star_search_with_hidden_indices_is_a_400_naming_the_system_indices() {
    let (server, catalog) = TestServer::start().await;
    commit_five_checkout_and_cart_logs(&catalog, TENANT_A)
        .await
        .expect("commit five logs");
    let (creation_status, creation_answer) = send_request(
        &server,
        Method::PUT,
        TENANT_A,
        "/.kibana",
        Some(&json!({"mappings": {"dynamic": "strict", "properties": {"type": {"type": "keyword"}}}})),
    )
    .await
    .expect("create .kibana");
    assert_eq!(creation_status, StatusCode::OK, "{creation_answer}");

    let (status, answer) = send_request(
        &server,
        Method::POST,
        TENANT_A,
        "/*/_search?expand_wildcards=all",
        Some(&json!({"query": {"match_all": {}}})),
    )
    .await
    .expect("star search request");

    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
    assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    // The refusal carries no structured key for the system indices; the reason naming them is
    // what tells the caller which side to drop.
    assert!(
        answer["error"]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains(".kibana")),
        "the reason names the system index: {}",
        answer["error"]["reason"]
    );
    server.shutdown().await;
}
