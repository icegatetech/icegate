//! Grafana-logs-style `_msearch` requests over real HTTP: the logs query, the logs volume and a
//! variable's values. Every NDJSON body is a literal, and every expected item is stated here.
//!
//! Every test reads tenant A's five logs of [`commit_five_checkout_and_cart_logs`]: 10:05 checkout
//! ERROR "payment declined for order 17", 10:20 checkout INFO "payment accepted for order 18",
//! 10:40 cart ERROR "cart service unavailable", 11:10 checkout ERROR "payment declined for order
//! 19" and 12:30 checkout ERROR "payment declined for order 20".
#![allow(clippy::expect_used)]

use reqwest::StatusCode;
use serde_json::json;

use super::data::commit_five_checkout_and_cart_logs;
use super::harness::TestServer;
use super::requests::send_ndjson;

const TENANT_A: &str = "tenant-a";

/// The logs query reads 10:00:00.000 to 11:59:59.999 and `service_name:checkout`: the 10:05,
/// 10:20 and 11:10 rows. The volume reads every row of that range: `ERROR` at 10:05, 10:40 and
/// 11:10, `INFO` at 10:20. No row lacks `severity_text`, so no `unknown` bucket appears.
#[tokio::test]
async fn a_grafana_logs_style_msearch_answers_the_logs_query_and_the_logs_volume() {
    let (server, catalog) = TestServer::start().await;
    commit_five_checkout_and_cart_logs(&catalog, TENANT_A)
        .await
        .expect("commit five logs");
    let msearch_body = concat!(
        r#"{"ignore_unavailable":true,"index":"logs","search_type":"query_then_fetch"}"#,
        "\n",
        r#"{"size":500,"query":{"bool":{"filter":[{"range":{"timestamp":{"gte":1791453600000,"lte":1791460799999,"format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"service_name:checkout"}}]}},"sort":[{"timestamp":{"order":"desc","unmapped_type":"boolean"}},{"_doc":{"order":"desc"}}],"script_fields":{},"fields":[{"field":"timestamp","format":"strict_date_optional_time_nanos"}]}"#,
        "\n",
        r#"{"ignore_unavailable":true,"index":"logs","search_type":"query_then_fetch"}"#,
        "\n",
        r#"{"size":0,"query":{"bool":{"filter":[{"range":{"timestamp":{"gte":"1791453600000","lte":"1791460799999","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"*"}}]}},"aggs":{"2":{"terms":{"field":"severity_text","size":500,"order":{"_count":"desc"},"min_doc_count":0,"missing":"unknown"},"aggs":{"3":{"date_histogram":{"interval":"1h","field":"timestamp","min_doc_count":0,"extended_bounds":{"min":1791453600000,"max":1791460799999},"format":"epoch_millis"}}}}}}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(
        &server,
        TENANT_A,
        "/_msearch?max_concurrent_shard_requests=5",
        msearch_body.to_string(),
    )
    .await
    .expect("logs query and volume msearch");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let logs = &answer["responses"][0];
    assert_eq!(logs["status"], 200);
    assert_eq!(logs["hits"]["total"], json!({"value": 3, "relation": "eq"}));
    let hits = logs["hits"]["hits"].as_array().expect("hits array");
    let expected_hits = [
        ("payment declined for order 19", "2026-10-08T11:10:00.000000Z"),
        ("payment accepted for order 18", "2026-10-08T10:20:00.000000Z"),
        ("payment declined for order 17", "2026-10-08T10:05:00.000000Z"),
    ];
    assert_eq!(hits.len(), expected_hits.len());
    for (hit, (expected_body, timestamp)) in hits.iter().zip(expected_hits) {
        assert_eq!(hit["_source"]["body"], expected_body);
        assert_eq!(
            hit["fields"]["timestamp"],
            json!([timestamp]),
            "one RFC 3339 string per hit"
        );
    }
    let volume = &answer["responses"][1];
    assert_eq!(volume["status"], 200);
    let build_hour_buckets = |ten_o_clock_count: u64, eleven_o_clock_count: u64| {
        json!({"buckets": [
            {"key_as_string": "1791453600000", "key": 1_791_453_600_000_i64, "doc_count": ten_o_clock_count},
            {"key_as_string": "1791457200000", "key": 1_791_457_200_000_i64, "doc_count": eleven_o_clock_count}
        ]})
    };
    assert_eq!(
        volume["aggregations"],
        json!({"2": {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
                {"key": "ERROR", "doc_count": 3, "3": build_hour_buckets(2, 1)},
                {"key": "INFO", "doc_count": 1, "3": build_hour_buckets(1, 0)}
            ]
        }})
    );
    server.shutdown().await;
}

/// The variable request reads until 13:59:59.999: every row.
#[tokio::test]
async fn a_grafana_variables_style_terms_msearch_answers_the_stated_keys() {
    let (server, catalog) = TestServer::start().await;
    commit_five_checkout_and_cart_logs(&catalog, TENANT_A)
        .await
        .expect("commit five logs");
    let msearch_body = concat!(
        r#"{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["logs"]}"#,
        "\n",
        r#"{"size":0,"query":{"bool":{"filter":[{"range":{"timestamp":{"gte":"1791453600000","lte":"1791467999999","format":"epoch_millis"}}}]}},"aggs":{"1":{"terms":{"field":"service_name","size":500,"order":{"_key":"asc"},"min_doc_count":1}}}}"#,
        "\n",
    );

    let (status, answer) = send_ndjson(&server, TENANT_A, "/_msearch", msearch_body.to_string())
        .await
        .expect("variables msearch");

    assert_eq!(status, StatusCode::OK, "{answer}");
    let item = &answer["responses"][0];
    assert_eq!(item["status"], 200);
    assert_eq!(
        item["aggregations"],
        json!({"1": {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": [
                {"key": "cart", "doc_count": 1},
                {"key": "checkout", "doc_count": 4}
            ]
        }})
    );
    server.shutdown().await;
}
