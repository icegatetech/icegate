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
