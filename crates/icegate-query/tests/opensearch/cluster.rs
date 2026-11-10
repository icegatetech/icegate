//! Cluster endpoints over real HTTP. Every request is a literal, and every expected answer is
//! stated in the test.

use icegate_query::opensearch::OpenSearchConfig;
use reqwest::Method;
use serde_json::{Value, json};

use super::harness::TestServer;

/// The tenant every request names.
const TENANT: &str = "tenant-a";

/// Whether `text` has the shape of a reported identifier: 22 URL-safe base64 characters.
fn is_url_safe_identifier(text: &str) -> bool {
    text.len() == 22
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

/// `GET /` on `server`, asserting a `200`. The error is a failed request or a body that is not
/// JSON.
async fn fetch_root(server: &TestServer) -> reqwest::Result<Value> {
    let response = server.request(Method::GET, Some(TENANT), "/").send().await?;
    assert_eq!(response.status(), 200);
    response.json().await
}

#[tokio::test]
async fn get_root_reports_the_configured_version_as_opensearch() {
    let (server, _catalog) = TestServer::start_with_config(OpenSearchConfig {
        version: "3.7.0".to_string(),
        cluster_name: "stand".to_string(),
        ..OpenSearchConfig::default()
    })
    .await;

    let body = fetch_root(&server).await.expect("GET / answers JSON");

    assert_eq!(body["version"]["number"], "3.7.0");
    assert_eq!(body["version"]["distribution"], "opensearch");
    assert_eq!(body["cluster_name"], "stand");
    assert_eq!(body["name"], "stand-node");
    assert!(
        body["cluster_uuid"].as_str().is_some_and(is_url_safe_identifier),
        "{body}"
    );
    server.shutdown().await;
}

/// The whole main response under the default configuration, every key and value stated, so a
/// change to any reported field fails here. `cluster_uuid` is asserted by shape and stability: the
/// same on a second call and on a second listener configured with the same cluster name.
#[tokio::test]
async fn get_root_answers_the_main_response_of_the_emulated_build() {
    let (first, _first_catalog) = TestServer::start().await;
    let (second, _second_catalog) = TestServer::start().await;

    let mut body = fetch_root(&first).await.expect("GET / answers JSON");
    let repeated_body = fetch_root(&first).await.expect("a repeated GET / answers JSON");
    let second_listener_body = fetch_root(&second).await.expect("GET / on the second listener answers JSON");

    let cluster_uuid = body
        .as_object_mut()
        .expect("an object")
        .remove("cluster_uuid")
        .expect("cluster_uuid is present");
    let cluster_uuid = cluster_uuid.as_str().expect("a string").to_string();
    assert!(is_url_safe_identifier(&cluster_uuid), "{cluster_uuid}");
    assert_eq!(repeated_body["cluster_uuid"], cluster_uuid);
    assert_eq!(second_listener_body["cluster_uuid"], cluster_uuid);
    assert_eq!(
        body,
        json!({
            "name": "icegate-node",
            "cluster_name": "icegate",
            "version": {
                "distribution": "opensearch",
                "number": "3.9.0",
                "build_type": "tar",
                "build_hash": "0000000000000000000000000000000000000000",
                "build_date": "1970-01-01T00:00:00.000Z",
                "build_snapshot": false,
                "lucene_version": "10.5.1",
                "minimum_wire_compatibility_version": "2.19.0",
                "minimum_index_compatibility_version": "2.0.0"
            },
            "tagline": "The OpenSearch Project: https://opensearch.org/"
        })
    );
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn head_root_answers_200() {
    let (server, _catalog) = TestServer::start().await;

    let response = server.request(Method::HEAD, Some(TENANT), "/").send().await.expect("HEAD /");

    assert_eq!(response.status(), 200);
    server.shutdown().await;
}

/// Whatever it is asked to wait for, the single node is green at once: the waiting parameters are
/// accepted and ignored.
#[tokio::test]
async fn cluster_health_is_green_for_one_node_whatever_it_waits_for() {
    let (server, _catalog) = TestServer::start().await;

    let response = server
        .request(
            Method::GET,
            Some(TENANT),
            "/_cluster/health?wait_for_status=green&timeout=5s&wait_for_nodes=1&level=cluster",
        )
        .send()
        .await
        .expect("GET /_cluster/health");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("JSON body");
    assert_eq!(
        body,
        json!({
            "cluster_name": "icegate",
            "status": "green",
            "timed_out": false,
            "number_of_nodes": 1,
            "number_of_data_nodes": 1,
            "discovered_cluster_manager": true,
            "active_primary_shards": 0,
            "active_shards": 0,
            "relocating_shards": 0,
            "initializing_shards": 0,
            "unassigned_shards": 0,
            "delayed_unassigned_shards": 0,
            "number_of_pending_tasks": 0,
            "number_of_in_flight_fetch": 0,
            "task_max_waiting_in_queue_millis": 0,
            "active_shards_percent_as_number": 100.0
        })
    );
    server.shutdown().await;
}

#[tokio::test]
async fn an_unknown_parameter_is_refused_in_the_envelope() {
    let (server, _catalog) = TestServer::start().await;

    // Each path answers without the parameter, so the refusal is the parameter's own.
    for path in ["/", "/_nodes", "/_cluster/health", "/_plugins/_query/_datasources"] {
        let known_response = server.request(Method::GET, Some(TENANT), path).send().await.expect("request");
        let unknown_parameter_path = format!("{path}?unknown_knob=1");
        let response = server
            .request(Method::GET, Some(TENANT), &unknown_parameter_path)
            .send()
            .await
            .expect("request");

        assert_eq!(known_response.status(), 200, "{path}");
        assert_eq!(response.status(), 400, "{unknown_parameter_path}");
        let body: Value = response.json().await.expect("JSON body");
        assert_eq!(body["status"], 400, "{unknown_parameter_path}");
        assert_eq!(
            body["error"]["type"], "illegal_argument_exception",
            "{unknown_parameter_path}"
        );
    }
    server.shutdown().await;
}

/// A version-poll-style request: this `filter_path` and a product-origin header. The answer keeps
/// exactly the version, the IP and the publish address of the one node, keyed by a stable node
/// id. The publish address is the harness's bound address, read from its base URL.
#[tokio::test]
async fn a_filtered_version_poll_returns_only_version_ip_and_publish_address() {
    let (server, _catalog) = TestServer::start().await;
    let publish_address = server.base_url.strip_prefix("http://").expect("an http base URL").to_string();

    let response = server
        .request(
            Method::GET,
            Some(TENANT),
            "/_nodes?filter_path=nodes.*.version%2Cnodes.*.http.publish_address%2Cnodes.*.ip",
        )
        .header("x-opensearch-product-origin", "opensearch-dashboards")
        .send()
        .await
        .expect("GET /_nodes");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("JSON body");
    let root = body.as_object().expect("an object");
    assert_eq!(root.len(), 1, "only `nodes` survives the filter: {body}");
    let nodes = body["nodes"].as_object().expect("a nodes object");
    assert_eq!(nodes.len(), 1);
    let (node_id, node) = nodes.iter().next().expect("one node");
    assert!(is_url_safe_identifier(node_id), "{node_id}");
    assert_eq!(
        node,
        &json!({"version": "3.9.0", "ip": "127.0.0.1", "http": {"publish_address": publish_address}})
    );
    server.shutdown().await;
}

/// IceGate defines no query data source, so the lookup is the static empty list.
#[tokio::test]
async fn query_datasources_answers_an_empty_list() {
    let (server, _catalog) = TestServer::start().await;

    let response = server
        .request(Method::GET, Some(TENANT), "/_plugins/_query/_datasources")
        .send()
        .await
        .expect("GET /_plugins/_query/_datasources");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("JSON body");
    assert_eq!(body, json!([]));
    server.shutdown().await;
}
