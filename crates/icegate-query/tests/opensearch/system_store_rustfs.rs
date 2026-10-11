//! The system-index store against real RustFS, built through the production constructor
//! `build_system_object_store` from a storage configuration as a deployment states it.
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use icegate_common::testing::{S3TestContainer, create_s3_bucket, create_s3_object_store};
use icegate_common::{S3Config, StorageBackend, StorageConfig};
use icegate_query::QueryConfig;
use icegate_query::opensearch::build_system_object_store;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt as _};
use reqwest::{Method, StatusCode};
use serde_json::json;
use tokio::sync::Barrier;

use super::harness::TestServer;

const BUCKET: &str = "opensearch-system-store";
/// Upper bound for a race of two writers, so a hung request fails the test instead of the run.
const RACE_TIMEOUT: Duration = Duration::from_secs(30);

/// Start RustFS with an empty bucket and the query configuration a deployment storing its tables
/// in that bucket carries, with the listener enabled. The container guard must outlive every
/// store built from the configuration.
async fn start_rustfs() -> (S3TestContainer, QueryConfig) {
    let container = S3TestContainer::start().await.expect("rustfs container starts");
    create_s3_bucket(container.endpoint(), BUCKET).await.expect("bucket created");
    let mut config = QueryConfig {
        storage: StorageConfig {
            backend: StorageBackend::S3(S3Config {
                bucket: BUCKET.to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some(container.endpoint().to_string()),
                access_key_id: Some(container.username().to_string()),
                secret_access_key: Some(container.password().to_string()),
            }),
        },
        ..QueryConfig::default()
    };
    config.opensearch.enabled = true;
    (container, config)
}

// The listener runs the probe before binding, so a started listener is a passed probe.
#[tokio::test]
async fn the_startup_probe_passes_against_rustfs_through_the_production_constructor() {
    let (_container, config) = start_rustfs().await;
    let system_object_store = build_system_object_store(&config).expect("the production client");
    assert_eq!(
        system_object_store.location(),
        "s3://opensearch-system-store/opensearch-system"
    );

    let (server, _catalog) = TestServer::start_with_system_store(config.opensearch.clone(), system_object_store).await;
    let response = server
        .request(Method::GET, Some("probe-tenant"), "/")
        .send()
        .await
        .expect("GET /");

    assert_eq!(response.status(), StatusCode::OK);
    server.shutdown().await;
}

/// Two listeners, each with a store built independently through the production constructor:
/// two replicas of one deployment.
async fn start_replicas(config: &QueryConfig) -> (TestServer, TestServer) {
    let first_store = build_system_object_store(config).expect("the first client");
    let second_store = build_system_object_store(config).expect("the second client");
    let (first_replica, _) = TestServer::start_with_system_store(config.opensearch.clone(), first_store).await;
    let (second_replica, _) = TestServer::start_with_system_store(config.opensearch.clone(), second_store).await;
    (first_replica, second_replica)
}

async fn put_index(server: &TestServer, tenant: &str, index: &str) -> StatusCode {
    server
        .request(Method::PUT, Some(tenant), &format!("/{index}"))
        .json(&json!({"mappings": {"dynamic": "strict"}}))
        .send()
        .await
        .expect("PUT index")
        .status()
}

async fn describe_index(server: &TestServer, tenant: &str, index: &str) -> StatusCode {
    server
        .request(Method::GET, Some(tenant), &format!("/{index}"))
        .send()
        .await
        .expect("GET index")
        .status()
}

// Which replica swaps first is a scheduling outcome and is not asserted; the contract is that
// both writes land.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_store_instances_racing_on_an_absent_pointer_both_land_their_writes() {
    let (_container, config) = start_rustfs().await;
    let (first_replica, second_replica) = start_replicas(&config).await;
    let barrier = Arc::new(Barrier::new(2));

    // Scoped, so the racing futures' borrows of the replicas end before the replicas shut down.
    let (first_status, second_status) = {
        let racing_puts = [(&first_replica, ".kibana_a"), (&second_replica, ".kibana_b")].map(|(server, index)| {
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                put_index(server, "race", index).await
            }
        });
        let [first_put, second_put] = racing_puts;
        tokio::time::timeout(RACE_TIMEOUT, async { tokio::join!(first_put, second_put) })
            .await
            .expect("both writes finish")
    };

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    for server in [&first_replica, &second_replica] {
        for index in [".kibana_a", ".kibana_b"] {
            assert_eq!(describe_index(server, "race", index).await, StatusCode::OK, "{index}");
        }
    }
    first_replica.shutdown().await;
    second_replica.shutdown().await;
}

#[tokio::test]
async fn a_replica_reading_after_another_replicas_write_sees_it() {
    let (_container, config) = start_rustfs().await;
    let (first_replica, second_replica) = start_replicas(&config).await;
    // The first replica caches the tenant's pointer before the second one writes.
    assert_eq!(put_index(&first_replica, "fresh", ".kibana_0").await, StatusCode::OK);
    assert_eq!(
        describe_index(&first_replica, "fresh", ".kibana_0").await,
        StatusCode::OK
    );

    assert_eq!(put_index(&second_replica, "fresh", ".kibana_1").await, StatusCode::OK);

    assert_eq!(
        describe_index(&first_replica, "fresh", ".kibana_1").await,
        StatusCode::OK
    );
    first_replica.shutdown().await;
    second_replica.shutdown().await;
}

/// The tenant segment is the tenant identifier verbatim; `:` must survive every S3 request the
/// store sends.
#[tokio::test]
async fn a_colon_tenant_segment_round_trips_create_update_conditional_get_and_head() {
    let (container, config) = start_rustfs().await;
    let (server, _catalog) = TestServer::start_with_system_store(
        config.opensearch.clone(),
        build_system_object_store(&config).expect("client"),
    )
    .await;

    assert_eq!(put_index(&server, "a:b", ".kibana_1").await, StatusCode::OK, "create");
    assert_eq!(
        put_index(&server, "a:b", ".kibana_2").await,
        StatusCode::OK,
        "update with the ETag"
    );
    assert_eq!(describe_index(&server, "a:b", ".kibana_1").await, StatusCode::OK);
    assert_eq!(
        describe_index(&server, "a:b", ".kibana_2").await,
        StatusCode::OK,
        "a conditional GET"
    );
    let head_response = server
        .request(Method::HEAD, Some("a:b"), "/.kibana_1")
        .send()
        .await
        .expect("HEAD");
    assert_eq!(head_response.status(), StatusCode::OK);

    let direct_store = create_s3_object_store(container.endpoint(), BUCKET).expect("a direct client");
    let pointer_path = Path::from("opensearch-system/a:b/current");
    let pointer_meta = direct_store
        .head(&pointer_path)
        .await
        .expect("the pointer is under the verbatim segment");
    let etag = pointer_meta.e_tag.expect("RustFS returns an ETag");
    let unchanged_answer = direct_store
        .get_opts(
            &pointer_path,
            GetOptions {
                if_none_match: Some(etag),
                ..GetOptions::default()
            },
        )
        .await;
    assert!(
        matches!(unchanged_answer, Err(object_store::Error::NotModified { .. })),
        "{unchanged_answer:?}"
    );
    server.shutdown().await;
}
