//! Shared test utilities for icegate-maintain integration tests.
//!
//! Centralizes the object-store bootstrap contract — container start, bucket
//! creation, and concrete [`S3Catalog`] construction — shared verbatim across
//! the maintain integration tests.

// Each integration-test binary pulls this module in via `mod common;` and may
// exercise only a subset of these helpers, so unused-item lints are expected.
#![allow(dead_code, clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;

use iceberg::io::FileIOBuilder;
use icegate_catalog_s3::{CatalogCodecKind, S3Catalog, S3CatalogConfig};
use icegate_common::catalog::IoHandle;
use icegate_common::testing::{S3TestContainer, create_s3_bucket};

/// Object-store bucket every maintain integration test provisions and targets.
pub const BUCKET_NAME: &str = "warehouse";

/// Connection parameters for a running object store.
#[derive(Clone)]
pub struct StorageConn {
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

/// Stand up object storage and capture its connection parameters.
pub async fn setup_object_store() -> (S3TestContainer, StorageConn) {
    let store = S3TestContainer::start().await.expect("start object storage");
    create_s3_bucket(store.endpoint(), BUCKET_NAME).await.expect("create bucket");
    let conn = StorageConn {
        endpoint: store.endpoint().to_string(),
        access_key: store.username().to_string(),
        secret_key: store.password().to_string(),
    };
    (store, conn)
}

/// Build a concrete [`S3Catalog`] against the object store.
pub async fn build_s3_catalog(conn: &StorageConn) -> S3Catalog {
    let io = IoHandle::noop();
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert("warehouse".to_string(), format!("s3://{BUCKET_NAME}"));
    props.insert("s3.endpoint".to_string(), conn.endpoint.clone());
    props.insert("s3.path-style-access".to_string(), "true".to_string());
    props.insert("s3.access-key-id".to_string(), conn.access_key.clone());
    props.insert("s3.secret-access-key".to_string(), conn.secret_key.clone());
    props.insert("s3.region".to_string(), "us-east-1".to_string());
    let file_io = FileIOBuilder::new(io.storage_factory()).with_props(props).build();

    S3Catalog::new(
        S3CatalogConfig {
            bucket: BUCKET_NAME.to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some(conn.endpoint.clone()),
            access_key_id: Some(conn.access_key.clone()),
            secret_access_key: Some(conn.secret_key.clone()),
            warehouse: BUCKET_NAME.to_string(),
            codec: CatalogCodecKind::Json,
            ..S3CatalogConfig::default()
        },
        file_io,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("build S3 catalog")
}
