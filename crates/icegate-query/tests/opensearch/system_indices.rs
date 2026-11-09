//! System-index endpoints over the real listener: index creation and description, aliases, refresh,
//! index templates, hidden system indices beside the data indices, and the tenant boundary of the
//! store.
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

/// `method path` as `tenant` with the JSON body `body`.
async fn send_json(
    server: &TestServer,
    method: Method,
    tenant: &str,
    path: &str,
    body: Value,
) -> reqwest::Result<reqwest::Response> {
    server.request(method, Some(tenant), path).json(&body).send().await
}

/// `method path` as `tenant` without a body.
async fn send_bodiless_request(
    server: &TestServer,
    method: Method,
    tenant: &str,
    path: &str,
) -> reqwest::Result<reqwest::Response> {
    server.request(method, Some(tenant), path).send().await
}

/// `.kibana_1` with `KIBANA_INDEX_BODY` and the alias `.kibana` on it, for `tenant`. The error is
/// a failed request or a refusal of either step.
async fn create_aliased_kibana(server: &TestServer, tenant: &str) -> reqwest::Result<()> {
    server
        .request(Method::PUT, Some(tenant), "/.kibana_1")
        .header("content-type", "application/json")
        .body(KIBANA_INDEX_BODY)
        .send()
        .await?
        .error_for_status()?;
    send_json(
        server,
        Method::POST,
        tenant,
        "/_aliases",
        json!({"actions": [{"add": {"index": ".kibana_1", "alias": ".kibana"}}]}),
    )
    .await?
    .error_for_status()?;
    Ok(())
}

/// A migration-style swap: one request removes the alias from the old index and adds it to the
/// new one; a request whose later action fails applies none of its actions.
#[tokio::test]
async fn aliases_are_added_swapped_and_removed_atomically() {
    let (server, _catalog) = TestServer::start().await;
    create_aliased_kibana(&server, TENANT_A).await.expect("the aliased .kibana_1");
    put_index(&server, TENANT_A, ".kibana_2", KIBANA_INDEX_BODY).await;

    let swap_response = send_json(
        &server,
        Method::POST,
        TENANT_A,
        "/_aliases",
        json!({"actions": [
            {"remove": {"index": ".kibana_1", "alias": ".kibana"}},
            {"add": {"index": ".kibana_2", "alias": ".kibana"}}
        ]}),
    )
    .await
    .expect("request");

    assert_eq!(swap_response.status(), StatusCode::OK);
    assert_eq!(read_json(swap_response).await, json!({"acknowledged": true}));
    assert_eq!(
        read_json(
            send_bodiless_request(&server, Method::GET, TENANT_A, "/_alias/.kibana")
                .await
                .expect("request")
        )
        .await,
        json!({".kibana_2": {"aliases": {".kibana": {}}}})
    );
    let refused_response = send_json(
        &server,
        Method::POST,
        TENANT_A,
        "/_aliases",
        json!({"actions": [
            {"add": {"index": ".kibana_2", "alias": ".kibana_extra"}},
            {"add": {"index": ".missing", "alias": ".kibana_other"}}
        ]}),
    )
    .await
    .expect("request");
    assert_eq!(refused_response.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        send_bodiless_request(&server, Method::GET, TENANT_A, "/_alias/.kibana_extra")
            .await
            .expect("request")
            .status(),
        StatusCode::NOT_FOUND,
        "the first action of the refused request is not applied"
    );
    let remove_index_response = send_json(
        &server,
        Method::POST,
        TENANT_A,
        "/_aliases",
        json!({"actions": [{"remove_index": {"index": ".kibana_1"}}]}),
    )
    .await
    .expect("request");
    assert_eq!(remove_index_response.status(), StatusCode::OK);
    assert_eq!(
        get_index(&server, TENANT_A, ".kibana_1").await.status(),
        StatusCode::NOT_FOUND
    );
    let data_index_response = send_json(
        &server,
        Method::POST,
        TENANT_A,
        "/_aliases",
        json!({"actions": [{"add": {"index": "logs", "alias": ".logs"}}]}),
    )
    .await
    .expect("request");
    assert_eq!(data_index_response.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        read_json(data_index_response).await["error"]["type"],
        "cluster_block_exception"
    );
    let malformed_response = send_json(
        &server,
        Method::POST,
        TENANT_A,
        "/_aliases",
        json!({"actions": [{"rename": {"index": ".kibana_2", "alias": ".k"}}]}),
    )
    .await
    .expect("request");
    assert_eq!(malformed_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read_json(malformed_response).await["error"]["type"],
        "parsing_exception"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn get_alias_of_a_missing_alias_is_404() {
    let (server, _catalog) = TestServer::start().await;
    put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;

    let response = send_bodiless_request(&server, Method::GET, TENANT_A, "/_alias/.kibana")
        .await
        .expect("request");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_eq!(read_json(response).await["status"], 404);
    server.shutdown().await;
}

/// `GET /_alias/<name>` takes a comma list of names and patterns: a pattern lists every alias it
/// matches, and a missing name beside a present one answers what is present.
#[tokio::test]
async fn get_alias_matches_patterns_and_comma_lists() {
    let (server, _catalog) = TestServer::start().await;
    create_aliased_kibana(&server, TENANT_A).await.expect("the aliased .kibana_1");

    for path in ["/_alias/.kib*", "/_alias/.missing,.kibana"] {
        let response = send_bodiless_request(&server, Method::GET, TENANT_A, path)
            .await
            .expect("request");

        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(
            read_json(response).await,
            json!({".kibana_1": {"aliases": {".kibana": {}}}}),
            "{path}"
        );
    }
    server.shutdown().await;
}

#[tokio::test]
async fn refresh_is_an_acknowledged_no_op_by_get_and_post() {
    let (server, _catalog) = TestServer::start().await;
    put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;

    for method in [Method::GET, Method::POST] {
        let response = send_bodiless_request(&server, method.clone(), TENANT_A, "/.kibana_1/_refresh")
            .await
            .expect("request");

        assert_eq!(response.status(), StatusCode::OK, "{method}");
        assert_eq!(
            read_json(response).await,
            json!({"_shards": {"total": 1, "successful": 1, "failed": 0}}),
            "{method}"
        );
    }
    server.shutdown().await;
}

/// The text form with `v` is the header line alone: it names the template columns in order, and
/// no template follows.
#[tokio::test]
async fn cat_templates_is_an_empty_list_and_template_delete_is_acknowledged() {
    let (server, _catalog) = TestServer::start().await;

    let templates_response = send_bodiless_request(
        &server,
        Method::GET,
        TENANT_A,
        "/_cat/templates/opensearch_dashboards_index_template*?format=json",
    )
    .await
    .expect("request");
    let text_templates_response = send_bodiless_request(
        &server,
        Method::GET,
        TENANT_A,
        "/_cat/templates/opensearch_dashboards_index_template*?v",
    )
    .await
    .expect("request");
    let delete_response = send_bodiless_request(
        &server,
        Method::DELETE,
        TENANT_A,
        "/_template/opensearch_dashboards_index_template",
    )
    .await
    .expect("request");

    assert_eq!(templates_response.status(), StatusCode::OK);
    assert_eq!(read_json(templates_response).await, json!([]));
    assert_eq!(text_templates_response.status(), StatusCode::OK);
    assert_eq!(
        text_templates_response.text().await.expect("body"),
        "name index_patterns order version composed_of\n"
    );
    assert_eq!(delete_response.status(), StatusCode::OK);
    assert_eq!(read_json(delete_response).await, json!({"acknowledged": true}));
    server.shutdown().await;
}

/// The timeouts and index options these endpoints ignore are accepted; an unknown parameter is
/// refused on each of them, so a misspelt option is never silently dropped.
#[tokio::test]
async fn alias_refresh_and_template_endpoints_accept_their_ignored_parameters_and_refuse_an_unknown_one() {
    let (server, _catalog) = TestServer::start().await;
    put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;
    let alias_body = json!({"actions": [{"add": {"index": ".kibana_1", "alias": ".kibana"}}]});

    let accepted_responses = [
        send_json(
            &server,
            Method::POST,
            TENANT_A,
            "/_aliases?timeout=30s&cluster_manager_timeout=30s&master_timeout=30s",
            alias_body.clone(),
        )
        .await
        .expect("request"),
        send_bodiless_request(
            &server,
            Method::POST,
            TENANT_A,
            "/.kibana_1/_refresh?ignore_unavailable=true&allow_no_indices=true&expand_wildcards=all",
        )
        .await
        .expect("request"),
        send_bodiless_request(
            &server,
            Method::DELETE,
            TENANT_A,
            "/_template/x?timeout=30s&cluster_manager_timeout=30s&master_timeout=30s",
        )
        .await
        .expect("request"),
        send_bodiless_request(
            &server,
            Method::GET,
            TENANT_A,
            "/_cat/templates/x?format=json&local=true&cluster_manager_timeout=30s&master_timeout=30s",
        )
        .await
        .expect("request"),
    ];
    let refused_responses = [
        send_json(&server, Method::POST, TENANT_A, "/_aliases?unknown_knob=1", alias_body)
            .await
            .expect("request"),
        send_bodiless_request(&server, Method::GET, TENANT_A, "/_alias/.kibana?unknown_knob=1")
            .await
            .expect("request"),
        send_bodiless_request(&server, Method::GET, TENANT_A, "/.kibana_1/_alias?unknown_knob=1")
            .await
            .expect("request"),
        send_bodiless_request(&server, Method::GET, TENANT_A, "/.kibana_1/_refresh?unknown_knob=1")
            .await
            .expect("request"),
        send_bodiless_request(&server, Method::DELETE, TENANT_A, "/_template/x?unknown_knob=1")
            .await
            .expect("request"),
        send_bodiless_request(&server, Method::GET, TENANT_A, "/_cat/templates/x?unknown_knob=1")
            .await
            .expect("request"),
    ];

    for response in accepted_responses {
        let url = response.url().to_string();
        assert_eq!(response.status(), StatusCode::OK, "{url}");
    }
    for response in refused_responses {
        let url = response.url().to_string();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{url}");
        assert_eq!(
            read_json(response).await["error"]["type"],
            "illegal_argument_exception",
            "{url}"
        );
    }
    server.shutdown().await;
}

#[tokio::test]
async fn get_index_through_its_alias_returns_the_concrete_index_with_its_aliases() {
    let (server, _catalog) = TestServer::start().await;
    create_aliased_kibana(&server, TENANT_A).await.expect("the aliased .kibana_1");
    let stored_mappings: Value = serde_json::from_str::<Value>(KIBANA_INDEX_BODY).expect("JSON")["mappings"].clone();

    let index_through_alias = read_json(get_index(&server, TENANT_A, ".kibana").await).await;
    let aliases_through_alias = read_json(
        send_bodiless_request(&server, Method::GET, TENANT_A, "/.kibana/_alias")
            .await
            .expect("request"),
    )
    .await;
    let aliases_through_index = read_json(
        send_bodiless_request(&server, Method::GET, TENANT_A, "/.kibana_1/_alias")
            .await
            .expect("request"),
    )
    .await;

    assert_eq!(
        index_through_alias,
        json!({".kibana_1": {
            "aliases": {".kibana": {}},
            "mappings": stored_mappings,
            "settings": {"number_of_shards": 1, "auto_expand_replicas": "0-1"}
        }})
    );
    assert_eq!(
        aliases_through_alias,
        json!({".kibana_1": {"aliases": {".kibana": {}}}})
    );
    assert_eq!(
        aliases_through_index,
        json!({".kibana_1": {"aliases": {".kibana": {}}}})
    );
    server.shutdown().await;
}

/// Data indices have no index-level metadata endpoint: their aliases are refused naming them.
#[tokio::test]
async fn aliases_of_a_data_index_is_a_400_naming_it() {
    let (server, _catalog) = TestServer::start().await;

    let response = send_bodiless_request(&server, Method::GET, TENANT_A, "/logs/_alias")
        .await
        .expect("request");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = read_json(response).await;
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    assert!(
        body["error"]["reason"].as_str().is_some_and(|reason| reason.contains("[logs]")),
        "{body}"
    );
    server.shutdown().await;
}

/// IceGate's rule: dot-prefixed indices are hidden from wildcards unless `expand_wildcards`
/// includes hidden; a pattern starting with `.` lists them regardless.
#[tokio::test]
async fn resolve_and_cat_indices_list_a_system_index_only_when_hidden_indices_are_asked_for() {
    let (server, _catalog) = TestServer::start().await;
    create_aliased_kibana(&server, TENANT_A).await.expect("the aliased .kibana_1");

    let open_resolution = read_json(
        send_bodiless_request(&server, Method::GET, TENANT_A, "/_resolve/index/*")
            .await
            .expect("request"),
    )
    .await;
    let hidden_resolution = read_json(
        send_bodiless_request(&server, Method::GET, TENANT_A, "/_resolve/index/*?expand_wildcards=all")
            .await
            .expect("request"),
    )
    .await;
    let dot_pattern_resolution = read_json(
        send_bodiless_request(&server, Method::GET, TENANT_A, "/_resolve/index/.kib*")
            .await
            .expect("request"),
    )
    .await;

    let list_index_names = |resolution: &Value| -> Vec<String> {
        resolution["indices"]
            .as_array()
            .expect("indices")
            .iter()
            .map(|index| index["name"].as_str().expect("a name").to_string())
            .collect()
    };
    assert_eq!(
        list_index_names(&open_resolution),
        [
            "events",
            "logs",
            "metrics",
            "operations",
            "otel-v1-apm-span",
            "spans",
            "ss4o_logs-icegate-default",
            "ss4o_metrics-icegate-default",
            "ss4o_traces-icegate-default"
        ]
    );
    assert_eq!(open_resolution["aliases"], json!([]));
    assert_eq!(
        list_index_names(&hidden_resolution),
        [
            ".kibana_1",
            "events",
            "logs",
            "metrics",
            "operations",
            "otel-v1-apm-span",
            "spans",
            "ss4o_logs-icegate-default",
            "ss4o_metrics-icegate-default",
            "ss4o_traces-icegate-default"
        ]
    );
    assert_eq!(
        hidden_resolution["indices"][0],
        json!({"name": ".kibana_1", "attributes": ["hidden", "open"]})
    );
    assert_eq!(
        hidden_resolution["aliases"],
        json!([{"name": ".kibana", "indices": [".kibana_1"]}])
    );
    assert_eq!(
        dot_pattern_resolution,
        json!({
            "indices": [{"name": ".kibana_1", "attributes": ["hidden", "open"]}],
            "aliases": [{"name": ".kibana", "indices": [".kibana_1"]}],
            "data_streams": []
        })
    );

    let open_cat_rows = read_json(
        send_bodiless_request(&server, Method::GET, TENANT_A, "/_cat/indices?format=json")
            .await
            .expect("request"),
    )
    .await;
    let hidden_cat_rows = read_json(
        send_bodiless_request(
            &server,
            Method::GET,
            TENANT_A,
            "/_cat/indices?format=json&expand_wildcards=all",
        )
        .await
        .expect("request"),
    )
    .await;
    let alias_cat_rows = read_json(
        send_bodiless_request(&server, Method::GET, TENANT_A, "/_cat/indices/.kibana?format=json")
            .await
            .expect("request"),
    )
    .await;

    let select_system_rows = |rows: &Value| -> Vec<Value> {
        rows.as_array()
            .expect("rows")
            .iter()
            .filter(|row| row["index"].as_str().is_some_and(|name| name.starts_with('.')))
            .cloned()
            .collect()
    };
    assert!(select_system_rows(&open_cat_rows).is_empty(), "{open_cat_rows}");
    let hidden_system_rows = select_system_rows(&hidden_cat_rows);
    assert_eq!(hidden_system_rows.len(), 1, "{hidden_cat_rows}");
    let alias_system_rows = select_system_rows(&alias_cat_rows);
    assert_eq!(alias_system_rows.len(), 1, "{alias_cat_rows}");
    for row in [&hidden_system_rows[0], &alias_system_rows[0]] {
        assert_eq!(row["index"], ".kibana_1");
        assert_eq!(row["health"], "green");
        assert_eq!(row["status"], "open");
        assert_eq!(row["pri"], "1");
        assert_eq!(row["rep"], "0");
        assert_eq!(row["docs.count"], "0");
        assert_eq!(row["docs.deleted"], "0");
        assert_eq!(row["store.size"], Value::Null);
        assert_eq!(row["pri.store.size"], Value::Null);
        assert!(row["uuid"].as_str().is_some_and(|uuid| uuid.len() == 22), "{row}");
    }
    server.shutdown().await;
}

#[tokio::test]
async fn field_caps_over_logs_and_kibana_is_a_400_naming_kibana() {
    let (server, _catalog) = TestServer::start().await;
    put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;

    let response = send_bodiless_request(&server, Method::GET, TENANT_A, "/logs,.kibana_1/_field_caps?fields=*")
        .await
        .expect("request");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = read_json(response).await;
    assert_eq!(body["error"]["type"], "illegal_argument_exception");
    assert!(
        body["error"]["reason"]
            .as_str()
            .is_some_and(|reason| reason.contains(".kibana_1")),
        "{body}"
    );
    server.shutdown().await;
}

#[tokio::test]
async fn field_caps_of_a_system_index_derives_fields_from_its_stored_mapping() {
    let (server, _catalog) = TestServer::start().await;
    put_index(
        &server,
        TENANT_A,
        ".kibana_1",
        r#"{"mappings":{"dynamic":"strict","properties":{"type":{"type":"keyword"},"title":{"type":"text","fields":{"keyword":{"type":"keyword"}}},"references":{"type":"nested","properties":{"id":{"type":"keyword"}}}}}}"#,
    )
    .await;

    let response = send_bodiless_request(&server, Method::GET, TENANT_A, "/.kibana_1/_field_caps?fields=*")
        .await
        .expect("request");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        read_json(response).await,
        json!({
            "indices": [".kibana_1"],
            "fields": {
                "references": {"nested": {"type": "nested", "searchable": false, "aggregatable": false}},
                "references.id": {"keyword": {"type": "keyword", "searchable": true, "aggregatable": true}},
                "title": {"text": {"type": "text", "searchable": true, "aggregatable": false}},
                "title.keyword": {"keyword": {"type": "keyword", "searchable": true, "aggregatable": true}},
                "type": {"keyword": {"type": "keyword", "searchable": true, "aggregatable": true}},
                "_id": {"_id": {"type": "_id", "searchable": true, "aggregatable": false}},
                "_index": {"_index": {"type": "_index", "searchable": false, "aggregatable": false}},
                "_source": {"_source": {"type": "_source", "searchable": false, "aggregatable": false}}
            }
        })
    );
    server.shutdown().await;
}

/// The stored mapping comes back byte for byte, keyed by the concrete index the alias names.
#[tokio::test]
async fn mapping_of_a_system_index_is_its_stored_mapping() {
    let (server, _catalog) = TestServer::start().await;
    create_aliased_kibana(&server, TENANT_A).await.expect("the aliased .kibana_1");

    let response = send_bodiless_request(&server, Method::GET, TENANT_A, "/.kibana/_mapping")
        .await
        .expect("request");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.text().await.expect("body"),
        r#"{".kibana_1":{"mappings":{"dynamic":"strict","properties":{"type":{"type":"keyword"},"updated_at":{"type":"date"},"migrationVersion":{"dynamic":"true","type":"object"}},"_meta":{"migrationMappingPropertyHashes":{"type":"2f4316de49999235636386fe51dc06c1","updated_at":"00da57df13e94e9d98437d13ace4bfe0"}}}}}"#
    );
    server.shutdown().await;
}

/// An expression naming a missing data index and a missing system index is a 404 naming the data
/// index, on a reading endpoint (`_field_caps`, `_search`) as on a listing one (`_cat/indices`):
/// every endpoint resolves the data side first.
#[tokio::test]
async fn a_missing_data_and_system_name_is_a_404_naming_the_data_index_on_every_endpoint() {
    let (server, _catalog) = TestServer::start().await;
    put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;

    for path in [
        "/missing-data,.missing-system/_field_caps?fields=*",
        "/missing-data,.missing-system/_search",
        "/_cat/indices/missing-data,.missing-system?format=json",
    ] {
        let response = send_bodiless_request(&server, Method::GET, TENANT_A, path)
            .await
            .expect("request");

        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        let body = read_json(response).await;
        assert_eq!(body["error"]["type"], "index_not_found_exception", "{path}: {body}");
        assert_eq!(body["error"]["index"], "missing-data", "{path}: {body}");
    }
    server.shutdown().await;
}

/// Field mappings are served for data indices only; a system index answers its stored mapping on
/// `_mapping` instead.
#[tokio::test]
async fn field_mapping_of_a_system_index_is_a_400() {
    let (server, _catalog) = TestServer::start().await;
    put_index(&server, TENANT_A, ".kibana_1", KIBANA_INDEX_BODY).await;

    let response = send_bodiless_request(&server, Method::GET, TENANT_A, "/.kibana_1/_mapping/field/type")
        .await
        .expect("request");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(read_json(response).await["error"]["type"], "illegal_argument_exception");
    server.shutdown().await;
}

#[tokio::test]
async fn settings_of_a_system_index_report_one_shard() {
    let (server, _catalog) = TestServer::start().await;
    create_aliased_kibana(&server, TENANT_A).await.expect("the aliased .kibana_1");

    let nested_settings = read_json(
        send_bodiless_request(
            &server,
            Method::GET,
            TENANT_A,
            "/.kibana/_settings/index.number_of_shards",
        )
        .await
        .expect("request"),
    )
    .await;
    let flat_settings = read_json(
        send_bodiless_request(
            &server,
            Method::GET,
            TENANT_A,
            "/.kibana_1/_settings/index.number_of_shards?flat_settings=true",
        )
        .await
        .expect("request"),
    )
    .await;

    assert_eq!(
        nested_settings,
        json!({".kibana_1": {"settings": {"index": {"number_of_shards": "1"}}}})
    );
    assert_eq!(
        flat_settings,
        json!({".kibana_1": {"settings": {"index.number_of_shards": "1"}}})
    );
    server.shutdown().await;
}

#[tokio::test]
async fn every_index_endpoint_under_tenant_b_sees_none_of_tenant_a_indices_or_aliases() {
    let (server, _catalog) = TestServer::start().await;
    create_aliased_kibana(&server, TENANT_A).await.expect("the aliased .kibana_1");

    for path in [
        "/.kibana",
        "/.kibana_1",
        "/_alias/.kibana",
        "/.kibana_1/_alias",
        "/.kibana/_mapping",
        "/.kibana_1/_field_caps?fields=*",
        "/.kibana_1/_settings/index.number_of_shards",
        "/_cat/indices/.kibana_1?format=json",
    ] {
        let response = send_bodiless_request(&server, Method::GET, TENANT_B, path)
            .await
            .expect("request");

        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
    let resolution = read_json(
        send_bodiless_request(&server, Method::GET, TENANT_B, "/_resolve/index/*?expand_wildcards=all")
            .await
            .expect("request"),
    )
    .await;
    assert!(
        resolution["indices"]
            .as_array()
            .expect("indices")
            .iter()
            .all(|index| index["name"].as_str().is_some_and(|name| !name.starts_with('.'))),
        "{resolution}"
    );
    assert_eq!(resolution["aliases"], json!([]));
    let cat_rows = read_json(
        send_bodiless_request(
            &server,
            Method::GET,
            TENANT_B,
            "/_cat/indices?format=json&expand_wildcards=all",
        )
        .await
        .expect("request"),
    )
    .await;
    assert!(
        cat_rows
            .as_array()
            .expect("rows")
            .iter()
            .all(|row| row["index"].as_str().is_some_and(|name| !name.starts_with('.'))),
        "{cat_rows}"
    );
    let alias_response = send_json(
        &server,
        Method::POST,
        TENANT_B,
        "/_aliases",
        json!({"actions": [{"add": {"index": ".kibana_1", "alias": ".mine"}}]}),
    )
    .await
    .expect("request");
    assert_eq!(alias_response.status(), StatusCode::NOT_FOUND);
    server.shutdown().await;
}
