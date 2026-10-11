//! System-index endpoints over the real listener: index creation and description, and the
//! tenant boundary of the store.
#![allow(clippy::expect_used)]

use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use super::harness::TestServer;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// A migration-style index body: a strict mapping with stored property hashes, one shard. Written
/// as text so the test can compare the stored mapping byte for byte.
const KIBANA_INDEX_BODY: &str = r#"{"mappings":{"dynamic":"strict","properties":{"type":{"type":"keyword"},"updated_at":{"type":"date"},"migrationVersion":{"dynamic":"true","type":"object"}},"_meta":{"migrationMappingPropertyHashes":{"type":"2f4316de49999235636386fe51dc06c1","updated_at":"00da57df13e94e9d98437d13ace4bfe0"}}},"settings":{"number_of_shards":1,"auto_expand_replicas":"0-1"}}"#;

async fn put_index(server: &TestServer, tenant: &str, index: &str, body: &str) -> reqwest::Response {
    server
        .request(Method::PUT, Some(tenant), &format!("/{index}"))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("PUT index")
}

async fn get_index(server: &TestServer, tenant: &str, index: &str) -> reqwest::Response {
    server
        .request(Method::GET, Some(tenant), &format!("/{index}"))
        .send()
        .await
        .expect("GET index")
}

async fn read_json(response: reqwest::Response) -> Value {
    response.json().await.expect("a JSON body")
}

#[tokio::test]
async fn get_of_a_missing_system_index_is_the_404_envelope() {
    let (server, _catalog) = TestServer::start().await;

    let response = get_index(&server, TENANT_A, ".kibana").await;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = read_json(response).await;
    assert_eq!(body["status"], 404);
    assert_eq!(body["error"]["type"], "index_not_found_exception");
    assert_eq!(body["error"]["root_cause"][0]["type"], "index_not_found_exception");
    assert_eq!(body["error"]["index"], ".kibana");
    server.shutdown().await;
}

/// The body is compared as text: the stored mapping must come back byte for byte, key order
/// included, under `{<index>: {aliases, mappings, settings}}`.
#[tokio::test]
async fn put_index_then_get_returns_the_mappings_and_settings_verbatim() {
    let (server, _catalog) = TestServer::start().await;

    let create_response = put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;
    assert_eq!(create_response.status(), StatusCode::OK);
    assert_eq!(
        read_json(create_response).await,
        json!({"acknowledged": true, "shards_acknowledged": true, "index": ".kibana_1"})
    );
    let describe_response = get_index(&server, TENANT_A, ".kibana_1").await;

    assert_eq!(describe_response.status(), StatusCode::OK);
    assert_eq!(
        describe_response.text().await.expect("body"),
        r#"{".kibana_1":{"aliases":{},"mappings":{"dynamic":"strict","properties":{"type":{"type":"keyword"},"updated_at":{"type":"date"},"migrationVersion":{"dynamic":"true","type":"object"}},"_meta":{"migrationMappingPropertyHashes":{"type":"2f4316de49999235636386fe51dc06c1","updated_at":"00da57df13e94e9d98437d13ace4bfe0"}}},"settings":{"number_of_shards":1,"auto_expand_replicas":"0-1"}}}"#
    );
    server.shutdown().await;
}

#[tokio::test]
async fn head_of_an_existing_index_is_200_without_a_body_and_of_a_missing_one_404() {
    let (server, _catalog) = TestServer::start().await;
    put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;

    let existing_response = server
        .request(Method::HEAD, Some(TENANT_A), "/.kibana_1")
        .send()
        .await
        .expect("HEAD existing");
    let missing_response = server
        .request(Method::HEAD, Some(TENANT_A), "/.kibana_2")
        .send()
        .await
        .expect("HEAD missing");

    assert_eq!(existing_response.status(), StatusCode::OK);
    assert!(existing_response.bytes().await.expect("body").is_empty());
    assert_eq!(missing_response.status(), StatusCode::NOT_FOUND);
    assert!(missing_response.bytes().await.expect("body").is_empty());
    server.shutdown().await;
}

#[tokio::test]
async fn put_of_an_existing_index_is_resource_already_exists_400() {
    let (server, _catalog) = TestServer::start().await;
    put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;

    let second_response = put_index(&server, TENANT_A, ".kibana_1", r#"{"mappings":{"dynamic":false}}"#).await;

    assert_eq!(second_response.status(), StatusCode::BAD_REQUEST);
    let body = read_json(second_response).await;
    assert_eq!(body["error"]["type"], "resource_already_exists_exception");
    assert_eq!(body["error"]["index"], ".kibana_1");
    let described_index = read_json(get_index(&server, TENANT_A, ".kibana_1").await).await;
    assert_eq!(
        described_index[".kibana_1"]["mappings"]["dynamic"], "strict",
        "the first mapping stays"
    );
    server.shutdown().await;
}

/// A non-dot name is a data index or would create one: the write is refused with the write block,
/// whose reason is the block's protocol text.
#[tokio::test]
async fn creating_a_non_dot_index_is_refused_403_cluster_block() {
    let (server, _catalog) = TestServer::start().await;

    for index in ["logs", "my-index"] {
        let response = put_index(&server, TENANT_A, index, "{}").await;

        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{index}");
        let body = read_json(response).await;
        assert_eq!(body["error"]["type"], "cluster_block_exception");
        assert_eq!(
            body["error"]["reason"],
            format!("index [{index}] blocked by: [FORBIDDEN/8/index write (api)];")
        );
    }
    server.shutdown().await;
}

/// The index-level read describes system indices only; a data index has no such description.
#[tokio::test]
async fn get_of_a_data_index_is_400_illegal_argument() {
    let (server, _catalog) = TestServer::start().await;

    let response = get_index(&server, TENANT_A, "logs").await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(read_json(response).await["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

/// A dot-prefixed name is a system index, but index names are lower case: the create is refused
/// and stores nothing.
#[tokio::test]
async fn put_of_an_upper_case_dot_name_is_400_illegal_argument() {
    let (server, _catalog) = TestServer::start().await;

    let response = put_index(&server, TENANT_A, ".Kibana", "{}").await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(read_json(response).await["error"]["type"], "illegal_argument_exception");
    assert_eq!(
        get_index(&server, TENANT_A, ".Kibana").await.status(),
        StatusCode::NOT_FOUND,
        "the refused create stored nothing"
    );
    server.shutdown().await;
}

/// The create body holds `mappings` and `settings` only; any other key is refused as a parse
/// error, and nothing is stored.
#[tokio::test]
async fn put_with_an_unknown_body_key_is_400_parsing_exception() {
    let (server, _catalog) = TestServer::start().await;

    let response = put_index(&server, TENANT_A, ".kibana_1", r#"{"mappings":{},"aliases":{}}"#).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(read_json(response).await["error"]["type"], "parsing_exception");
    assert_eq!(
        get_index(&server, TENANT_A, ".kibana_1").await.status(),
        StatusCode::NOT_FOUND,
        "the refused create stored nothing"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn tenant_b_does_not_see_tenant_a_system_index() {
    let (server, _catalog) = TestServer::start().await;
    put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;

    let unseen_response = get_index(&server, TENANT_B, ".kibana_1").await;
    assert_eq!(unseen_response.status(), StatusCode::NOT_FOUND);
    let create_response = put_index(&server, TENANT_B, ".kibana_1", r#"{"mappings":{"dynamic":false}}"#).await;
    assert_eq!(create_response.status(), StatusCode::OK, "tenant b's name is free");

    let index_for_tenant_a = read_json(get_index(&server, TENANT_A, ".kibana_1").await).await;
    let index_for_tenant_b = read_json(get_index(&server, TENANT_B, ".kibana_1").await).await;
    assert_eq!(index_for_tenant_a[".kibana_1"]["mappings"]["dynamic"], "strict");
    assert_eq!(index_for_tenant_b[".kibana_1"]["mappings"], json!({"dynamic": false}));
    server.shutdown().await;
}

/// An empty or blank body creates the index with empty mappings and settings, and a body
/// without settings stores empty settings.
#[tokio::test]
async fn put_with_an_empty_body_or_without_settings_stores_empty_mappings_and_settings() {
    let (server, _catalog) = TestServer::start().await;

    let cases = [
        (".kibana_empty", ""),
        (".kibana_blank", " \n\t"),
        (".kibana_mappings", r#"{"mappings":{"dynamic":"strict"}}"#),
    ];
    for (index, body) in cases {
        let response = put_index(&server, TENANT_A, index, body).await;

        assert_eq!(response.status(), StatusCode::OK, "{index}");
    }

    assert_eq!(
        read_json(get_index(&server, TENANT_A, ".kibana_empty").await).await,
        json!({".kibana_empty": {"aliases": {}, "mappings": {}, "settings": {}}})
    );
    assert_eq!(
        read_json(get_index(&server, TENANT_A, ".kibana_blank").await).await,
        json!({".kibana_blank": {"aliases": {}, "mappings": {}, "settings": {}}})
    );
    assert_eq!(
        read_json(get_index(&server, TENANT_A, ".kibana_mappings").await).await,
        json!({".kibana_mappings": {"aliases": {}, "mappings": {"dynamic": "strict"}, "settings": {}}})
    );
    server.shutdown().await;
}

/// The shard waits and timeouts of a create are accepted and ignored; an unknown parameter is
/// refused on both methods, and a refused create stores nothing.
#[tokio::test]
async fn put_accepts_its_ignored_parameters_and_both_methods_refuse_an_unknown_one() {
    let (server, _catalog) = TestServer::start().await;

    let accepted_response = put_index(
        &server,
        TENANT_A,
        ".kibana_1?wait_for_active_shards=1&timeout=30s&cluster_manager_timeout=30s&master_timeout=30s",
        KIBANA_INDEX_BODY,
    )
    .await;
    let refused_put_response = put_index(&server, TENANT_A, ".kibana_2?unknown_knob=1", KIBANA_INDEX_BODY).await;
    let refused_get_response = get_index(&server, TENANT_A, ".kibana_1?unknown_knob=1").await;

    assert_eq!(accepted_response.status(), StatusCode::OK);
    assert_eq!(refused_put_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json(refused_put_response).await["error"]["type"],
        "illegal_argument_exception"
    );
    assert_eq!(refused_get_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json(refused_get_response).await["error"]["type"],
        "illegal_argument_exception"
    );
    assert_eq!(
        get_index(&server, TENANT_A, ".kibana_2").await.status(),
        StatusCode::NOT_FOUND,
        "the refused create stored nothing"
    );
    server.shutdown().await;
}

/// A single `_`-prefixed segment names an endpoint, never an index: one that no route serves is
/// refused like an unmatched route, on every method of the index-level route.
#[tokio::test]
async fn an_underscore_prefixed_segment_is_refused_as_an_endpoint_no_route_serves() {
    let (server, _catalog) = TestServer::start().await;

    let get_response = get_index(&server, TENANT_A, "_no_such_endpoint").await;
    let put_response = put_index(&server, TENANT_A, "_no_such_endpoint", "{}").await;
    let head_response = server
        .request(Method::HEAD, Some(TENANT_A), "/_no_such_endpoint")
        .send()
        .await
        .expect("HEAD");

    for response in [get_response, put_response] {
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(read_json(response).await["error"]["type"], "illegal_argument_exception");
    }
    assert_eq!(head_response.status(), StatusCode::BAD_REQUEST);
    server.shutdown().await;
}
