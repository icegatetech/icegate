//! The system-index store against real RustFS, built through the production constructor
//! `build_system_object_store` from a storage configuration as a deployment states it.
#![allow(clippy::expect_used)]

use icegate_common::testing::{S3TestContainer, create_s3_bucket};
use icegate_common::{S3Config, StorageBackend, StorageConfig};
use icegate_query::QueryConfig;
use icegate_query::opensearch::build_system_object_store;
use reqwest::{Method, StatusCode};

use super::harness::TestServer;

const BUCKET: &str = "opensearch-system-store";

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
    let system_store = build_system_object_store(&config).expect("the production client");
    assert_eq!(
        system_store.location(),
        "s3://opensearch-system-store/opensearch-system"
    );

    let (server, _catalog) = TestServer::start_with_system_store(config.opensearch.clone(), system_store).await;
    let response = server
        .request(Method::GET, Some("probe-tenant"), "/")
        .send()
        .await
        .expect("GET /");

    assert_eq!(response.status(), StatusCode::OK);
    server.shutdown().await;
}
