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

use std::sync::{Arc, mpsc};
use std::time::Duration;

use icegate_common::error::CommonError;
use icegate_common::storage::{OperatorRegistry, S3Config, StorageBackend, StorageLayersConfig};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;

const BUCKET: &str = "warehouse";

/// Bound on a resolution that must not block. Deliberately generous: it detects
/// a deadlock, it does not assert a latency — no resolution here performs I/O.
const RESOLUTION_TIMEOUT: Duration = Duration::from_secs(30);

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

/// The GC sweep resolves a store on every iteration and the WAL on every
/// restart of its reader, so a rebuilt store is a per-iteration leak: the store
/// is held by the operator behind it, and an operator carrying the metrics
/// layer is never freed. The registry is wired with a real meter provider, the
/// way a running component wires one, because that layer is applied only when a
/// meter is configured.
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

/// A bucket root is a valid base path — `s3://queue/` is the value the Docker
/// configurations ship — and it addresses the whole bucket, so the prefix is
/// empty. The trailing slash matters: it is what decides whether the bucket and
/// the prefix are split at all.
#[test]
fn a_bucket_root_resolves_with_an_empty_prefix() {
    let registry = registry();

    // (base path, prefix within the store)
    let cases = [
        (format!("s3://{BUCKET}"), ""),
        (format!("s3://{BUCKET}/"), ""),
        (format!("s3a://{BUCKET}/"), ""),
        (format!("s3://{BUCKET}/tables/logs"), "tables/logs"),
    ];

    for (base_path, expected) in cases {
        let (_, prefix) = registry.resolve_object_store(&base_path).expect("store");

        assert_eq!(prefix, expected, "{base_path}");
    }
}

/// A local base path is how the WAL is configured in development. The directory
/// is held by the store itself, so the prefix handed back to the queue
/// configuration is empty rather than the path — a prefix carrying the path
/// would address every object twice under it.
///
/// Such a path also carries no operator, and that is observable without the
/// cached-operator count the unit tests use: one path resolved twice is served
/// by two stores, because nothing was kept from the first resolution. It must
/// be the **same** path twice — two spellings of one directory are two
/// resolutions whether the registry caches or not, so they would prove nothing.
///
/// The `file://` prefix and the bare path are both accepted, and a local path
/// cannot be resolved in a unit test because opening the store touches the
/// filesystem.
#[test]
fn a_local_base_path_resolves_with_an_empty_prefix_and_is_built_per_call() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().to_str().expect("a UTF-8 temporary path");
    let url_path = format!("file://{path}");
    let registry = registry();

    let (first, url_prefix) = registry.resolve_object_store(&url_path).expect("store for a file:// base path");
    let (second, repeated_prefix) = registry
        .resolve_object_store(&url_path)
        .expect("store for the repeated base path");
    let (_, bare_prefix) = registry.resolve_object_store(path).expect("store for a bare base path");

    assert!(!Arc::ptr_eq(&first, &second), "a local path reached the operator cache");
    assert_eq!(url_prefix, "");
    assert_eq!(repeated_prefix, "");
    assert_eq!(bare_prefix, "");
}

/// The base path comes from YAML and is checked only for emptiness, so a value
/// naming no bucket reaches the registry, where building its operator fails
/// while the cache entry for that identity is held.
///
/// The second resolution is of the **same** identity, and that is the point: it
/// is what proves the entry was released rather than left held or left
/// occupied. A different identity would land on a different key, and most
/// likely a different shard, and prove neither. That the entry is not left
/// holding the failure is a consequence — nothing is inserted on the failing
/// path — and a repeat that returned a cached failure would be
/// indistinguishable from this one, so the invariant asserted here is
/// reachability, not freshness.
///
/// Both attempts run on a spawned thread whose result is awaited under a
/// bound. A held entry would make the repeat block on the shard lock the first
/// attempt never released, and re-entering that shard from the thread already
/// holding it deadlocks, so unbounded the regression would hang the run instead
/// of failing it.
#[test]
fn an_unbuildable_bucket_fails_and_leaves_the_registry_usable() {
    let registry = Arc::new(registry());

    let attempts_registry = Arc::clone(&registry);
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let attempts = [
            attempts_registry.resolve_object_store("s3://"),
            attempts_registry.resolve_object_store("s3://"),
        ];
        // The receiver is gone only when the wait below already timed out and
        // failed the test, so the send has nothing left to report to.
        let _ = sender.send(attempts);
    });

    let attempts = receiver
        .recv_timeout(RESOLUTION_TIMEOUT)
        .expect("the repeat blocked, so the failing build left its cache entry held");

    for (index, result) in attempts.into_iter().enumerate() {
        let attempt = index + 1;
        let Err(error) = result else {
            panic!("attempt {attempt}: a base path naming no bucket must fail");
        };
        assert!(matches!(error, CommonError::Iceberg(_)), "attempt {attempt}: {error}");
    }

    let (_, prefix) = registry
        .resolve_object_store(&format!("s3://{BUCKET}/tables"))
        .expect("a correctly named bucket must still resolve");

    assert_eq!(prefix, "tables");
}
