//! `_msearch` over real HTTP. Every NDJSON body is a literal, and every expected item is stated
//! here.
#![allow(clippy::expect_used)]

use std::error::Error;
use std::sync::Arc;

use iceberg::Catalog;
use icegate_common::LOGS_TABLE;
use icegate_common::schema::COL_BODY;
use icegate_query::opensearch::{OpenSearchConfig, SearchLimitsConfig, SystemIndicesConfig};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::data::{
    RowWriteError, build_log_row_at_minute, build_wal_log_row_at_minute, commit_log_rows, write_wal_segment,
};
use super::harness::TestServer;
use super::requests::{send_ndjson, send_request};

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// Tenant A's three logs: one `ERROR`, two `INFO`.
async fn commit_three_logs(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    commit_log_rows(
        catalog,
        &[
            build_log_row_at_minute(TENANT_A, 5, "ERROR", "tenant-a payment declined"),
            build_log_row_at_minute(TENANT_A, 20, "INFO", "tenant-a payment accepted"),
            build_log_row_at_minute(TENANT_A, 40, "INFO", "tenant-a order shipped"),
        ],
    )
    .await
}

/// Create `.kibana` for `tenant`, mapping only the keyword `type`, holding one document per
/// `(id, type)` pair of `documents`.
async fn create_kibana_index(
    server: &TestServer,
    tenant: &str,
    documents: &[(&str, &str)],
) -> Result<(), Box<dyn Error>> {
    let index = json!({
        "mappings": {"dynamic": "strict", "properties": {"type": {"type": "keyword"}}},
        "settings": {"number_of_shards": 1}
    });
    let (status, answer) = send_request(server, Method::PUT, tenant, "/.kibana", Some(&index)).await?;
    if status != StatusCode::OK {
        return Err(format!("creating .kibana answered {status}: {answer}").into());
    }
    for (id, document_type) in documents {
        let (status, answer) = send_request(
            server,
            Method::PUT,
            tenant,
            &format!("/.kibana/_create/{id}"),
            Some(&json!({"type": document_type})),
        )
        .await?;
        if status != StatusCode::CREATED {
            return Err(format!("creating {id} answered {status}: {answer}").into());
        }
    }
    Ok(())
}

/// The `_source` bodies of an item's hits, in hit order; `None` when the item holds no hits array
/// or a hit has no string body.
fn read_item_bodies(item: &Value) -> Option<Vec<&str>> {
    item["hits"]["hits"]
        .as_array()?
        .iter()
        .map(|hit| hit["_source"][COL_BODY].as_str())
        .collect()
}

#[tokio::test]
async fn msearch_answers_items_in_request_order_and_one_items_400_leaves_the_others_200() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let body = concat!(
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"term":{"severity_text":"ERROR"}},"size":10}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"no_such_leaf":{"field":"x"}}}"#,
        "\n",
        r#"{"index":["logs"]}"#,
        "\n",
        r#"{"query":{"term":{"severity_text":"INFO"}},"size":0}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch", body.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    assert!(answer["took"].is_u64());
    let responses = answer["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0]["status"], 200);
    assert_eq!(responses[0]["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    assert_eq!(
        read_item_bodies(&responses[0]).expect("item bodies"),
        vec!["tenant-a payment declined"]
    );
    assert_eq!(responses[1]["status"], 400);
    assert_eq!(responses[1]["error"]["type"], "parsing_exception");
    assert_eq!(responses[1]["error"]["root_cause"][0]["type"], "parsing_exception");
    assert_eq!(responses[2]["status"], 200);
    assert_eq!(responses[2]["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    assert_eq!(responses[2]["hits"]["hits"], json!([]));
    server.shutdown().await;
}

#[tokio::test]
async fn a_query_string_syntax_error_in_one_item_is_that_items_400() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let body = concat!(
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"query_string":{"query":"severity_text:(ERROR"}}}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"query_string":{"query":"severity_text:ERROR"}}}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch", body.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let responses = answer["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["status"], 400);
    assert_eq!(responses[0]["error"]["type"], "search_phase_execution_exception");
    assert_eq!(responses[0]["error"]["phase"], "query");
    assert_eq!(responses[0]["error"]["grouped"], true);
    assert_eq!(responses[0]["error"]["root_cause"][0]["type"], "query_shard_exception");
    assert_eq!(responses[1]["status"], 200);
    assert_eq!(
        read_item_bodies(&responses[1]).expect("item bodies"),
        vec!["tenant-a payment declined"]
    );
    server.shutdown().await;
}

/// The mixed item's two halves each answer 200 on their own, so mixing them is the only cause of
/// its refusal.
#[tokio::test]
async fn msearch_items_mixing_system_and_data_indices_fail_alone() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    create_kibana_index(&server, TENANT_A, &[]).await.expect("create .kibana");
    let body = concat!(
        r#"{"index":"logs,.kibana"}"#,
        "\n",
        r#"{"query":{"match_all":{}}}"#,
        "\n",
        r#"{"index":".kibana"}"#,
        "\n",
        r#"{"query":{"match_all":{}}}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":0}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch", body.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let responses = answer["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 3);
    assert_eq!(responses[0]["status"], 400);
    assert_eq!(responses[0]["error"]["type"], "illegal_argument_exception");
    assert_eq!(responses[1]["status"], 200);
    assert_eq!(responses[1]["hits"]["total"], json!({"value": 0, "relation": "eq"}));
    assert_eq!(responses[2]["status"], 200);
    assert_eq!(responses[2]["hits"]["total"], json!({"value": 3, "relation": "eq"}));
    server.shutdown().await;
}

#[tokio::test]
async fn msearch_tolerates_max_concurrent_shard_requests_preference_and_timeout() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let body = concat!(
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":0}"#,
        "\n"
    );

    let (status, answer) = send_ndjson(
        &server,
        TENANT_A,
        "/_msearch?max_concurrent_shard_requests=5&ignore_throttled=true&preference=1&timeout=30s",
        body.to_string(),
    )
    .await
    .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer["responses"][0]["status"], 200);
    assert_eq!(
        answer["responses"][0]["hits"]["total"],
        json!({"value": 3, "relation": "eq"})
    );
    server.shutdown().await;
}

/// Without a path index, a header naming no index falls back to the every-visible-index wildcard
/// expression, so it follows the URL's wildcard options: `expand_wildcards=none` empties it and
/// `allow_no_indices=false` answers it 404, while a header naming `logs` still reads it.
#[tokio::test]
async fn an_indexless_msearch_header_reads_the_path_index_or_else_follows_the_url_wildcard_options() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let path_body = concat!("{}\n", r#"{"query":{"match_all":{}},"size":0}"#, "\n");
    let pathless_body = concat!(
        "{}\n",
        r#"{"query":{"match_all":{}},"size":0}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":0}"#,
        "\n",
    );

    let (path_status, path_answer) = send_ndjson(&server, TENANT_A, "/logs/_msearch", path_body.to_string())
        .await
        .expect("msearch request");
    let (pathless_status, pathless_answer) = send_ndjson(
        &server,
        TENANT_A,
        "/_msearch?expand_wildcards=none&allow_no_indices=false",
        pathless_body.to_string(),
    )
    .await
    .expect("msearch request");

    assert_eq!(path_status, StatusCode::OK, "{path_answer}");
    assert_eq!(path_answer["responses"][0]["status"], 200);
    assert_eq!(
        path_answer["responses"][0]["hits"]["total"],
        json!({"value": 3, "relation": "eq"}),
        "a header naming no index reads the path's index"
    );
    assert_eq!(pathless_status, StatusCode::OK, "{pathless_answer}");
    assert_eq!(pathless_answer["responses"][0]["status"], 404);
    assert_eq!(
        pathless_answer["responses"][0]["error"]["type"],
        "index_not_found_exception"
    );
    assert_eq!(pathless_answer["responses"][1]["status"], 200);
    assert_eq!(
        pathless_answer["responses"][1]["hits"]["total"],
        json!({"value": 3, "relation": "eq"})
    );
    server.shutdown().await;
}

#[tokio::test]
async fn a_header_index_option_overrides_the_url_default() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let body = concat!(
        r#"{"index":"no_such_index"}"#,
        "\n",
        r#"{"query":{"match_all":{}}}"#,
        "\n",
        r#"{"index":"no_such_index","ignore_unavailable":false}"#,
        "\n",
        r#"{"query":{"match_all":{}}}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch?ignore_unavailable=true", body.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(
        answer["responses"][0]["status"], 200,
        "the URL default skips the missing name"
    );
    assert_eq!(
        answer["responses"][0]["hits"]["total"],
        json!({"value": 0, "relation": "eq"})
    );
    assert_eq!(
        answer["responses"][1]["status"], 404,
        "the header overrides the URL default"
    );
    assert_eq!(answer["responses"][1]["error"]["type"], "index_not_found_exception");
    server.shutdown().await;
}

#[tokio::test]
async fn msearch_url_parameters_apply_to_every_item() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let counting_body = concat!(
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"term":{"severity_text":"ERROR"}},"size":0}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":0}"#,
        "\n",
    );
    let wildcard_body = concat!(
        r#"{"index":"nomatch*"}"#,
        "\n",
        r#"{"query":{"match_all":{}}}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":0}"#,
        "\n",
    );

    let (integer_status, integer_answer) = send_ndjson(
        &server,
        TENANT_A,
        "/_msearch?rest_total_hits_as_int=true",
        counting_body.to_string(),
    )
    .await
    .expect("msearch request");
    let (strict_status, strict_answer) = send_ndjson(
        &server,
        TENANT_A,
        "/_msearch?allow_no_indices=false",
        wildcard_body.to_string(),
    )
    .await
    .expect("msearch request");

    assert_eq!(integer_status, StatusCode::OK, "{integer_answer}");
    assert_eq!(integer_answer["responses"][0]["status"], 200);
    assert_eq!(integer_answer["responses"][0]["hits"]["total"], json!(1));
    assert_eq!(integer_answer["responses"][1]["status"], 200);
    assert_eq!(integer_answer["responses"][1]["hits"]["total"], json!(3));
    assert_eq!(strict_status, StatusCode::OK, "{strict_answer}");
    assert_eq!(strict_answer["responses"][0]["status"], 404);
    assert_eq!(
        strict_answer["responses"][0]["error"]["type"],
        "index_not_found_exception"
    );
    assert_eq!(strict_answer["responses"][1]["status"], 200);
    assert_eq!(
        strict_answer["responses"][1]["hits"]["total"],
        json!({"value": 3, "relation": "eq"})
    );
    server.shutdown().await;
}

/// The URL's `size` overrides each item body's `size`, whether the body names one or not, as it
/// overrides a `_search` body's; the totals still count every matching row.
#[tokio::test]
async fn the_url_size_overrides_every_items_size() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let body = concat!(
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":10}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"term":{"severity_text":"INFO"}}}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch?size=0", body.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let responses = answer["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["status"], 200);
    assert_eq!(responses[0]["hits"]["total"], json!({"value": 3, "relation": "eq"}));
    assert_eq!(responses[0]["hits"]["hits"], json!([]));
    assert_eq!(responses[1]["status"], 200);
    assert_eq!(responses[1]["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    assert_eq!(responses[1]["hits"]["hits"], json!([]));
    server.shutdown().await;
}

/// The URL's `track_total_hits` threshold overrides each item body's key: an item matching more
/// rows than the threshold counts to it with relation `gte`, one matching exactly the threshold
/// counts exactly.
#[tokio::test]
async fn the_url_track_total_hits_overrides_every_items_key() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let body = concat!(
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":0,"track_total_hits":true}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"term":{"severity_text":"INFO"}},"size":0}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch?track_total_hits=2", body.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let responses = answer["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["status"], 200);
    assert_eq!(responses[0]["hits"]["total"], json!({"value": 2, "relation": "gte"}));
    assert_eq!(responses[1]["status"], 200);
    assert_eq!(responses[1]["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    server.shutdown().await;
}

/// A window at `max_result_window` is served and one past it fails only its own item, on data and
/// system indices alike.
#[tokio::test]
async fn msearch_items_past_the_result_window_fail_alone() {
    let (server, catalog) = TestServer::start_with_config(OpenSearchConfig {
        search: SearchLimitsConfig {
            max_result_window: 3,
            ..SearchLimitsConfig::default()
        },
        ..OpenSearchConfig::default()
    })
    .await;
    commit_three_logs(&catalog).await.expect("commit logs");
    create_kibana_index(&server, TENANT_A, &[]).await.expect("create .kibana");
    let body = concat!(
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":3}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"from":1,"size":3}"#,
        "\n",
        r#"{"index":".kibana"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":3}"#,
        "\n",
        r#"{"index":".kibana"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":4}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch", body.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let responses = answer["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 4);
    assert_eq!(responses[0]["status"], 200);
    assert_eq!(
        responses[0]["hits"]["hits"].as_array().map(Vec::len),
        Some(3),
        "{}",
        responses[0]
    );
    assert_eq!(responses[2]["status"], 200);
    assert_eq!(responses[2]["hits"]["total"], json!({"value": 0, "relation": "eq"}));
    for refused_item in [&responses[1], &responses[3]] {
        assert_eq!(refused_item["status"], 400, "{refused_item}");
        assert_eq!(
            refused_item["error"]["type"], "search_phase_execution_exception",
            "{refused_item}"
        );
        assert_eq!(
            refused_item["error"]["root_cause"][0]["type"], "illegal_argument_exception",
            "{refused_item}"
        );
    }
    server.shutdown().await;
}

/// Each item answer is bounded by `max_answer_bytes` on its own. Every `.kibana` hit fits the
/// bound, and so does one hit whose `type` a sort copies into `sort`, but two such hits pass it;
/// one log body alone passes it. Each refused item has a control that differs from it only in
/// answer size and answers 200, so the bound is the only cause of the refusal. Two fitting
/// `.kibana` items together pass the bound, and both still answer 200.
#[tokio::test]
async fn msearch_items_past_max_answer_bytes_fail_alone() {
    let system_indices = SystemIndicesConfig {
        max_snapshot_bytes: 6000,
        max_answer_bytes: 6000,
        ..SystemIndicesConfig::default()
    };
    system_indices.validate().expect("valid answer bound");
    let (server, catalog) = TestServer::start_with_config(OpenSearchConfig {
        system_indices,
        ..OpenSearchConfig::default()
    })
    .await;
    let long_body = "x".repeat(7000);
    commit_log_rows(&catalog, &[build_log_row_at_minute(TENANT_A, 5, "INFO", &long_body)])
        .await
        .expect("commit logs");
    let long_type = "9".repeat(2000);
    create_kibana_index(&server, TENANT_A, &[("config:a", &long_type), ("config:b", &long_type)])
        .await
        .expect("create .kibana");
    let body = concat!(
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}}}"#,
        "\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"_source":false}"#,
        "\n",
        r#"{"index":".kibana"}"#,
        "\n",
        r#"{"sort":[{"type":"asc"}]}"#,
        "\n",
        r#"{"index":".kibana"}"#,
        "\n",
        r#"{"sort":[{"type":"asc"}],"size":1}"#,
        "\n",
        r#"{"index":".kibana"}"#,
        "\n",
        "{}\n",
        r#"{"index":".kibana"}"#,
        "\n",
        "{}\n",
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"size":0}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch", body.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let responses = answer["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 7);
    for refused_item in [&responses[0], &responses[2]] {
        assert_eq!(refused_item["status"], 400, "{refused_item}");
        assert_eq!(
            refused_item["error"]["type"], "illegal_argument_exception",
            "{refused_item}"
        );
    }
    assert_eq!(responses[1]["status"], 200, "{}", responses[1]);
    assert_eq!(
        responses[1]["hits"]["total"],
        json!({"value": 1, "relation": "eq"}),
        "the logs query is accepted once its answer is small"
    );
    assert_eq!(responses[3]["status"], 200, "{}", responses[3]);
    assert_eq!(
        responses[3]["hits"]["hits"].as_array().map(Vec::len),
        Some(1),
        "the sort on `type` is accepted once its answer is small: {}",
        responses[3]
    );
    assert_eq!(responses[3]["hits"]["hits"][0]["sort"], json!([long_type]));
    for fitting_item in [&responses[4], &responses[5]] {
        assert_eq!(fitting_item["status"], 200, "{fitting_item}");
        assert_eq!(fitting_item["hits"]["total"], json!({"value": 2, "relation": "eq"}));
    }
    assert!(
        serde_json::to_vec(&[&responses[4], &responses[5]])
            .expect("serialize the fitting items")
            .len()
            > 6000,
        "the two fitting items together pass the bound"
    );
    assert_eq!(responses[6]["status"], 200);
    assert_eq!(responses[6]["hits"]["total"], json!({"value": 1, "relation": "eq"}));
    server.shutdown().await;
}

#[tokio::test]
async fn msearch_never_returns_another_tenants_rows() {
    let (server, catalog) = TestServer::start().await;
    commit_log_rows(
        &catalog,
        &[
            build_log_row_at_minute(TENANT_A, 5, "INFO", "tenant-a first parquet row"),
            build_log_row_at_minute(TENANT_B, 6, "INFO", "tenant-b parquet row"),
            build_log_row_at_minute(TENANT_A, 7, "INFO", "tenant-a second parquet row"),
        ],
    )
    .await
    .expect("commit logs");
    // Offset 1 follows the WAL offset the Parquet commit records.
    write_wal_segment(
        &server.wal_store,
        &catalog,
        LOGS_TABLE,
        1,
        &[
            build_wal_log_row_at_minute(TENANT_A, 8, "INFO", "tenant-a wal row"),
            build_wal_log_row_at_minute(TENANT_B, 9, "INFO", "tenant-b wal row"),
        ],
    )
    .await
    .expect("write WAL segment");
    let body = concat!(
        r#"{"index":"logs"}"#,
        "\n",
        r#"{"query":{"match_all":{}},"size":10}"#,
        "\n",
        r#"{"index":"log*"}"#,
        "\n",
        r#"{"query":{"query_string":{"query":"tenant"}},"size":10}"#,
        "\n",
    );

    let (status_b, answer_b) = send_ndjson(&server, TENANT_B, "/_msearch", body.to_string())
        .await
        .expect("msearch request");
    let (status_a, answer_a) = send_ndjson(&server, TENANT_A, "/_msearch", body.to_string())
        .await
        .expect("msearch request");

    assert_eq!(status_b, StatusCode::OK, "{answer_b}");
    let responses_b = answer_b["responses"].as_array().expect("responses");
    assert_eq!(responses_b.len(), 2);
    for item in responses_b {
        assert_eq!(item["status"], 200, "{item}");
        assert_eq!(item["hits"]["total"], json!({"value": 2, "relation": "eq"}));
        let mut bodies = read_item_bodies(item).expect("item bodies");
        bodies.sort_unstable();
        assert_eq!(bodies, vec!["tenant-b parquet row", "tenant-b wal row"]);
    }
    assert_eq!(status_a, StatusCode::OK, "{answer_a}");
    let responses_a = answer_a["responses"].as_array().expect("responses");
    assert_eq!(responses_a.len(), 2);
    for item in responses_a {
        assert_eq!(item["status"], 200, "{item}");
        assert_eq!(item["hits"]["total"], json!({"value": 3, "relation": "eq"}));
        let mut bodies = read_item_bodies(item).expect("item bodies");
        bodies.sort_unstable();
        assert_eq!(
            bodies,
            vec![
                "tenant-a first parquet row",
                "tenant-a second parquet row",
                "tenant-a wal row"
            ]
        );
    }
    server.shutdown().await;
}

/// A request whose items cannot all be framed, read or defaulted is refused as one envelope, with no
/// `responses`: an invalid path index, an invalid URL `expand_wildcards`, a body cut after its last
/// search, and a header naming an unsupported key, which the refusal carries as `key`.
#[tokio::test]
async fn an_unreadable_msearch_request_is_refused_whole() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let valid_body = concat!(r#"{"index":"logs"}"#, "\n", r#"{"size":0}"#, "\n");
    let cases = [
        ("/_foo/_msearch", valid_body, None),
        ("/_msearch?expand_wildcards=sideways", valid_body, None),
        ("/_msearch", concat!(r#"{"index":"logs"}"#, "\n", r#"{"size":0}"#), None),
        (
            "/_msearch",
            concat!(
                r#"{"index":"logs"}"#,
                "\n",
                r#"{"size":0}"#,
                "\n",
                r#"{"index":"logs","typed_keys":true}"#,
                "\n",
                r#"{"size":0}"#,
                "\n",
            ),
            Some("typed_keys"),
        ),
    ];

    for (path, body, expected_key) in cases {
        let (status, answer) = send_ndjson(&server, TENANT_A, path, body.to_string())
            .await
            .expect("msearch request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {answer}");
        assert_eq!(answer["status"], 400, "{path}");
        assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{path}");
        assert_eq!(answer.get("responses"), None, "{path}");
        assert_eq!(
            answer["error"].get("key").and_then(Value::as_str),
            expected_key,
            "{path}"
        );
    }
    server.shutdown().await;
}

/// Each refused path answers 200 without the unknown parameter, so the parameter is the only cause
/// of the refusal.
#[tokio::test]
async fn an_unknown_msearch_parameter_is_refused_in_the_envelope() {
    let (server, catalog) = TestServer::start().await;
    commit_three_logs(&catalog).await.expect("commit logs");
    let body = concat!(r#"{"index":"logs"}"#, "\n", r#"{"query":{"match_all":{}}}"#, "\n");

    for path in ["/_msearch", "/logs/_msearch"] {
        let (status, answer) = send_ndjson(&server, TENANT_A, &format!("{path}?unknown_knob=1"), body.to_string())
            .await
            .expect("msearch request");
        let (control_status, control_answer) = send_ndjson(&server, TENANT_A, path, body.to_string())
            .await
            .expect("msearch request");

        assert_eq!(status, StatusCode::BAD_REQUEST, "{path}: {answer}");
        assert_eq!(answer["status"], 400, "{path}");
        assert_eq!(answer["error"]["type"], "illegal_argument_exception", "{path}");
        assert_eq!(answer.get("responses"), None, "{path}");
        assert_eq!(control_status, StatusCode::OK, "{path}: {control_answer}");
        assert_eq!(control_answer["responses"][0]["status"], 200, "{path}");
    }
    let (status, answer) = send_ndjson(
        &server,
        TENANT_A,
        "/_msearch?max_concurrent_searches=5",
        body.to_string(),
    )
    .await
    .expect("msearch request");
    assert_eq!(status, StatusCode::OK, "an ignored parameter is accepted: {answer}");
    server.shutdown().await;
}
