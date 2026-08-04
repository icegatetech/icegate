//! The object stores an [`OperatorRegistry`] hands out for `s3://` paths.
//!
//! An integration test rather than a unit test because building an S3 operator
//! reads the ambient AWS profile (`opendal`'s S3 builder calls
//! `AwsConfig::from_profile`), which unit tests may not do. Nothing here
//! performs a request: `Operator::new` is lazy, and no store is read or
//! written.
//!
//! ```text
//! cargo test -p icegate-common --test operator_registry_it
//! ```

#![allow(clippy::expect_used)]

use std::sync::Arc;

use icegate_common::storage::{OperatorRegistry, S3Config, StorageBackend, StorageLayersConfig};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;

const BUCKET: &str = "warehouse";

/// A backend addressing a local endpoint with explicit credentials, so no
/// ambient credential chain decides what the operator signs with.
fn backend() -> StorageBackend {
    StorageBackend::S3(S3Config {
        bucket: BUCKET.to_string(),
        region: "us-east-1".to_string(),
        endpoint: Some("http://127.0.0.1:9000".to_string()),
        access_key_id: Some("key".to_string()),
        secret_access_key: Some("secret".to_string()),
    })
}

/// A registry without a meter: no layer wraps the operators, so only the
/// caching decides what a resolution returns.
fn registry() -> OperatorRegistry {
    OperatorRegistry::new(&StorageLayersConfig::default(), Some(&backend()))
}

/// The GC sweep resolves a store on every iteration, and the WAL on every
/// restart of its reader. Each store carries an `OpenDAL` operator that the
/// metrics layer retains for the lifetime of the process, so the second
/// resolution must return the first one.
#[test]
fn a_repeated_path_returns_the_store_already_built() {
    let registry = registry();
    let path = format!("s3://{BUCKET}/tables/logs");

    let (first, first_prefix) = registry.resolve_object_store(&path).expect("first store");
    let (second, second_prefix) = registry.resolve_object_store(&path).expect("second store");

    assert!(Arc::ptr_eq(&first, &second), "the store was rebuilt");
    assert_eq!(first_prefix, "tables/logs");
    assert_eq!(first_prefix, second_prefix);
}

/// The metrics layer is the reason an operator is never freed, and a registry
/// without a meter never applies it — so the resolution above only proves the
/// caching, not the path that leaks. Wired with a real meter provider, as a
/// running component wires one, a repeated path must still be served the store
/// built the first time: that store is held by the cached operator, so a
/// rebuilt operator would hand out a different one.
#[test]
fn a_repeated_path_reuses_the_store_of_an_operator_carrying_the_metrics_layer() {
    let registry = OperatorRegistry::new(
        &StorageLayersConfig {
            meter: Some(SdkMeterProvider::builder().build().meter("operator-registry-test")),
            ..StorageLayersConfig::default()
        },
        Some(&backend()),
    );
    let path = format!("s3://{BUCKET}/tables/logs");

    let (first, first_prefix) = registry.resolve_object_store(&path).expect("first store");
    let (second, second_prefix) = registry.resolve_object_store(&path).expect("second store");

    assert!(Arc::ptr_eq(&first, &second), "the store was rebuilt");
    assert_eq!(first_prefix, "tables/logs");
    assert_eq!(first_prefix, second_prefix);
}

/// A table pointed at a new location inside the same bucket is served by the
/// store already built: the store addresses the bucket, and the location only
/// decides the prefix returned with it.
#[test]
fn a_new_location_in_one_bucket_reuses_the_store() {
    let registry = registry();

    let (built_for, _) = registry
        .resolve_object_store(&format!("s3://{BUCKET}/tables/logs"))
        .expect("first store");
    let (moved, moved_prefix) = registry
        .resolve_object_store(&format!("s3://{BUCKET}/moved/logs"))
        .expect("store for the new location");

    assert!(Arc::ptr_eq(&built_for, &moved), "the store was rebuilt");
    assert_eq!(moved_prefix, "moved/logs");
}

/// Two buckets are two identities: one store may not serve the other, or the
/// sweep would list and delete under a prefix of the wrong bucket.
#[test]
fn distinct_buckets_get_distinct_stores() {
    let registry = registry();

    let (warehouse, _) = registry
        .resolve_object_store(&format!("s3://{BUCKET}/tables"))
        .expect("warehouse store");
    let (other, other_prefix) = registry.resolve_object_store("s3://other-bucket/tables").expect("other store");

    assert!(!Arc::ptr_eq(&warehouse, &other), "two buckets must not share one store");
    assert_eq!(other_prefix, "tables");
}

/// `s3a://` is the same identity as `s3://`: the scheme names the client the
/// path came from, not the bucket it addresses.
#[test]
fn the_s3a_scheme_reaches_the_same_store() {
    let registry = registry();

    let (s3, _) = registry
        .resolve_object_store(&format!("s3://{BUCKET}/tables"))
        .expect("s3 store");
    let (s3a, s3a_prefix) = registry
        .resolve_object_store(&format!("s3a://{BUCKET}/tables"))
        .expect("s3a store");

    assert!(Arc::ptr_eq(&s3, &s3a), "s3a:// built a second store");
    assert_eq!(s3a_prefix, "tables");
}

/// A bucket root without a prefix is a valid base path: the whole bucket is
/// listed, and the prefix is empty.
#[test]
fn a_bucket_root_resolves_with_an_empty_prefix() {
    let registry = registry();

    let (_, prefix) = registry
        .resolve_object_store(&format!("s3://{BUCKET}"))
        .expect("bucket root store");

    assert_eq!(prefix, "");
}
