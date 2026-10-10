//! Index resolution and settings over real HTTP: `_resolve/index` and `<index>/_settings`. Each
//! request is a literal, and each expected answer is stated here.

use reqwest::Method;
use serde_json::{Value, json};

use super::harness::TestServer;

/// The tenant every request names.
const TENANT: &str = "tenant-a";

/// `GET path` as [`TENANT`]: the status and the JSON body. The error is a failed request or a body
/// that is not JSON.
async fn get_json(server: &TestServer, path: &str) -> reqwest::Result<(u16, Value)> {
    let response = server.request(Method::GET, Some(TENANT), path).send().await?;
    let status = response.status().as_u16();
    Ok((status, response.json().await?))
}

#[tokio::test]
async fn resolve_index_lists_the_telemetry_indices_sorted_and_open() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = get_json(&server, "/_resolve/index/*").await.expect("a JSON answer");

    let build_open_index = |name: &str| json!({"name": name, "attributes": ["open"]});
    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!({
            "indices": [
                build_open_index("events"),
                build_open_index("logs"),
                build_open_index("metrics"),
                build_open_index("operations"),
                build_open_index("spans")
            ],
            "aliases": [],
            "data_streams": []
        })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn resolve_index_answers_200_with_empty_lists_for_a_missing_name() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = get_json(&server, "/_resolve/index/nope").await.expect("a JSON answer");

    assert_eq!(status, 200);
    assert_eq!(body, json!({"indices": [], "aliases": [], "data_streams": []}));
    server.shutdown().await;
}

/// A comma list that arrives percent-encoded in the path (`%2C`) is decoded before it is parsed.
#[tokio::test]
async fn resolve_index_decodes_a_percent_encoded_comma_list() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = get_json(&server, "/_resolve/index/logs%2Cspans").await.expect("a JSON answer");

    assert_eq!(status, 200);
    assert_eq!(
        body["indices"],
        json!([
            {"name": "logs", "attributes": ["open"]},
            {"name": "spans", "attributes": ["open"]}
        ])
    );
    server.shutdown().await;
}

/// `expand_wildcards=none` stops a wildcard from expanding; a concrete name still resolves.
#[tokio::test]
async fn resolve_index_under_expand_wildcards_none_resolves_only_concrete_names() {
    let (server, _catalog) = TestServer::start().await;

    let (wildcard_status, wildcard_body) = get_json(&server, "/_resolve/index/*?expand_wildcards=none")
        .await
        .expect("a JSON answer");
    let (concrete_status, concrete_body) = get_json(&server, "/_resolve/index/logs?expand_wildcards=none")
        .await
        .expect("a JSON answer");

    assert_eq!((wildcard_status, concrete_status), (200, 200));
    assert_eq!(wildcard_body["indices"], json!([]));
    assert_eq!(
        concrete_body["indices"],
        json!([{"name": "logs", "attributes": ["open"]}])
    );
    server.shutdown().await;
}

#[tokio::test]
async fn an_exact_name_table_resolves_by_name_but_not_by_wildcard() {
    let (server, _catalog) = TestServer::start().await;

    let (_, exact_name_body) = get_json(&server, "/_resolve/index/prices").await.expect("a JSON answer");
    let (_, wildcard_body) = get_json(&server, "/_resolve/index/p*").await.expect("a JSON answer");

    assert_eq!(
        exact_name_body["indices"],
        json!([{"name": "prices", "attributes": ["open"]}])
    );
    assert_eq!(wildcard_body["indices"], json!([]));
    server.shutdown().await;
}

/// A shard-count lookup in the style of a datasource: `GET` with `flat_settings=true`, an NDJSON
/// content type and no body. The flat and the nested forms both report one shard, as a string.
#[tokio::test]
async fn settings_report_one_shard_flat_and_nested() {
    let (server, _catalog) = TestServer::start().await;

    let flat_response = server
        .request(
            Method::GET,
            Some(TENANT),
            "/logs/_settings/index.number_of_shards?flat_settings=true",
        )
        .header("content-type", "application/x-ndjson")
        .send()
        .await
        .expect("flat settings");
    let flat_status = flat_response.status().as_u16();
    let flat_body: Value = flat_response.json().await.expect("a JSON body");
    let (nested_status, nested_body) = get_json(&server, "/logs/_settings/index.number_of_shards")
        .await
        .expect("a JSON answer");

    assert_eq!((flat_status, nested_status), (200, 200));
    assert_eq!(
        flat_body,
        json!({"logs": {"settings": {"index.number_of_shards": "1"}}})
    );
    assert_eq!(
        nested_body,
        json!({"logs": {"settings": {"index": {"number_of_shards": "1"}}}})
    );
    server.shutdown().await;
}

#[tokio::test]
async fn settings_of_a_missing_index_is_the_404_envelope() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = get_json(&server, "/nope/_settings/index.number_of_shards")
        .await
        .expect("a JSON answer");

    assert_eq!(status, 404);
    assert_eq!(body["status"], 404);
    assert_eq!(body["error"]["type"], "index_not_found_exception");
    assert_eq!(body["error"]["index"], "nope");
    server.shutdown().await;
}

#[tokio::test]
async fn settings_of_a_missing_index_under_ignore_unavailable_is_an_empty_object() {
    let (server, _catalog) = TestServer::start().await;

    let (status, body) = get_json(
        &server,
        "/nope/_settings/index.number_of_shards?ignore_unavailable=true",
    )
    .await
    .expect("a JSON answer");

    assert_eq!(status, 200);
    assert_eq!(body, json!({}));
    server.shutdown().await;
}

/// `allow_no_indices=false` turns the empty object into the `404` naming the whole expression.
#[tokio::test]
async fn settings_of_an_empty_match_is_an_empty_object_unless_allow_no_indices_is_false() {
    let (server, _catalog) = TestServer::start().await;

    let (empty_status, empty_body) = get_json(&server, "/nope*/_settings/index.number_of_shards")
        .await
        .expect("a JSON answer");
    let (strict_status, strict_body) = get_json(
        &server,
        "/nope*/_settings/index.number_of_shards?allow_no_indices=false",
    )
    .await
    .expect("a JSON answer");

    assert_eq!(empty_status, 200);
    assert_eq!(empty_body, json!({}));
    assert_eq!(strict_status, 404);
    assert_eq!(strict_body["error"]["type"], "index_not_found_exception");
    assert_eq!(strict_body["error"]["index"], "nope*");
    server.shutdown().await;
}

/// The control request proves `l*` selects `logs` by default, so the empty object is
/// `expand_wildcards=none` stopping the expansion, not a pattern that matches nothing.
#[tokio::test]
async fn settings_under_expand_wildcards_none_select_no_index_by_wildcard() {
    let (server, _catalog) = TestServer::start().await;

    let (expanded_status, expanded_body) = get_json(&server, "/l*/_settings/index.number_of_shards")
        .await
        .expect("a JSON answer");
    let (unexpanded_status, unexpanded_body) =
        get_json(&server, "/l*/_settings/index.number_of_shards?expand_wildcards=none")
            .await
            .expect("a JSON answer");

    assert_eq!((expanded_status, unexpanded_status), (200, 200));
    assert_eq!(
        expanded_body,
        json!({"logs": {"settings": {"index": {"number_of_shards": "1"}}}})
    );
    assert_eq!(unexpanded_body, json!({}));
    server.shutdown().await;
}

/// Only the shard count is served. The same index answers `200` for it, so the `400` is the
/// handler refusing the setting, not an unmatched route.
#[tokio::test]
async fn settings_of_a_setting_other_than_the_shard_count_is_a_400() {
    let (server, _catalog) = TestServer::start().await;

    let (served_status, _) = get_json(&server, "/logs/_settings/index.number_of_shards")
        .await
        .expect("a JSON answer");
    let (status, body) = get_json(&server, "/logs/_settings/index.number_of_replicas")
        .await
        .expect("a JSON answer");

    assert_eq!(served_status, 200);
    assert_eq!(status, 400);
    assert_eq!(body["status"], 400);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

/// Index names match case-sensitively: `Logs` is an unknown index, never the `logs` table. It is
/// the `404` envelope on `_settings`, and resolves to no index on `_resolve/index`.
#[tokio::test]
async fn an_index_name_in_another_case_names_no_index() {
    let (server, _catalog) = TestServer::start().await;

    let (settings_status, settings_body) = get_json(&server, "/Logs/_settings/index.number_of_shards")
        .await
        .expect("a JSON answer");
    let (resolve_status, resolve_body) = get_json(&server, "/_resolve/index/Logs").await.expect("a JSON answer");

    assert_eq!(settings_status, 404);
    assert_eq!(settings_body["error"]["type"], "index_not_found_exception");
    assert_eq!(settings_body["error"]["index"], "Logs");
    assert_eq!(resolve_status, 200);
    assert_eq!(resolve_body["indices"], json!([]));
    server.shutdown().await;
}

/// The control request proves the route is served, so the `400` is the expression refused, not an
/// unmatched route.
#[tokio::test]
async fn an_underscore_prefixed_index_name_is_a_400() {
    let (server, _catalog) = TestServer::start().await;

    let (control_status, _) = get_json(&server, "/_resolve/index/logs").await.expect("a JSON answer");
    let (status, body) = get_json(&server, "/_resolve/index/_internal").await.expect("a JSON answer");

    assert_eq!(control_status, 200);
    assert_eq!(status, 400);
    assert_eq!(body["status"], 400);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

/// A path segment that is not UTF-8 once percent-decoded (`%FF`) is refused in the envelope, never
/// with a plain-text body. The control request proves the route is served, so the `400` is the
/// path parameter refused, not an unmatched route.
#[tokio::test]
async fn a_path_segment_that_is_not_utf8_is_the_400_envelope() {
    let (server, _catalog) = TestServer::start().await;

    let (control_status, _) = get_json(&server, "/_resolve/index/logs").await.expect("a JSON answer");
    let (status, body) = get_json(&server, "/_resolve/index/%FF").await.expect("a JSON answer");

    assert_eq!(control_status, 200);
    assert_eq!(status, 400);
    assert_eq!(body["status"], 400);
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

/// An unknown parameter and a value outside a parameter's grammar are refused on both endpoints.
/// The control requests prove both routes are served, so each `400` is the parameters refused.
#[tokio::test]
async fn an_unknown_parameter_or_an_unserved_value_is_a_400_on_both_endpoints() {
    let (server, _catalog) = TestServer::start().await;

    let (resolve_control_status, _) = get_json(&server, "/_resolve/index/logs").await.expect("a JSON answer");
    let (settings_control_status, _) = get_json(&server, "/logs/_settings/index.number_of_shards")
        .await
        .expect("a JSON answer");

    assert_eq!((resolve_control_status, settings_control_status), (200, 200));
    for path in [
        "/_resolve/index/logs?unknown_knob=1",
        "/_resolve/index/logs?expand_wildcards=everything",
        "/logs/_settings/index.number_of_shards?unknown_knob=1",
        "/logs/_settings/index.number_of_shards?ignore_unavailable=yes",
        "/logs/_settings/index.number_of_shards?expand_wildcards=open,",
    ] {
        let (status, body) = get_json(&server, path).await.expect("a JSON answer");

        assert_eq!(status, 400, "{path}");
        assert_eq!(body["status"], 400, "{path}");
        assert_eq!(body["error"]["type"], "illegal_argument_exception", "{path}");
    }
    server.shutdown().await;
}
