//! Object store builder utilities.
//!
//! Provides functions to create object stores from configuration.
//! S3 stores are backed by [OpenDAL](https://opendal.apache.org/) with
//! retry, caching, and `OpenTelemetry` observability layers.

use std::sync::Arc;

use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};
use object_store_opendal::OpendalStore;
use opendal::Operator;
use opendal::layers::{OtelMetricsLayer, OtelTraceLayer, RetryLayer};
use opendal::services::S3;

use super::StorageBackend;
use crate::error::{CommonError, Result};

/// Result containing the object store and the normalized base path.
///
/// The normalized base path is the path within the store (e.g., the S3 prefix
/// without the bucket, or empty string for local filesystem).
pub type ObjectStoreWithPath = (Arc<dyn ObjectStore>, String);

/// Create an S3-backed object store using `OpenDAL` with observability layers.
///
/// Parses the S3 URL, reads credentials from environment, and builds the store
/// with the following layer stack (outermost first):
///
/// 1. **`OtelMetricsLayer`** — `OpenTelemetry` storage metrics (sees cache hits)
/// 2. **`FoyerLayer`** — shared hybrid cache (if `cache` is provided)
/// 3. **`OtelTraceLayer`** — `OpenTelemetry` distributed tracing (S3 calls only)
/// 4. **`RetryLayer`** — automatic retries with exponential backoff
///
/// The resulting [`Operator`] is wrapped in [`OpendalStore`] to satisfy
/// the `Arc<dyn ObjectStore>` interface expected by all downstream code.
///
/// # Arguments
///
/// * `base_path` - S3 URL in the format `s3://bucket/prefix`
/// * `backend` - Optional storage backend configuration for endpoint/region settings
/// * `cache` - Optional foyer cache shared with the Iceberg catalog's IO layer
/// * `cache_object_size_limit` - Maximum object size (bytes) eligible for caching.
///   Objects larger than this bypass the cache. `None` uses the `FoyerLayer` default
///   (no limit).
///
/// # Returns
///
/// A tuple of (object store, normalized base path) where the base path is just
/// the prefix portion (bucket is handled by the store).
pub fn create_s3_store(
    base_path: &str,
    backend: Option<&StorageBackend>,
    cache: Option<&iceberg::io::FoyerCache>,
    cache_object_size_limit: Option<usize>,
) -> Result<ObjectStoreWithPath> {
    // Parse S3 URL: s3://bucket/prefix
    let path_without_scheme = base_path.strip_prefix("s3://").unwrap_or(base_path);
    let (bucket, prefix) = path_without_scheme.split_once('/').map_or_else(
        || (path_without_scheme.to_string(), String::new()),
        |(b, p)| (b.to_string(), p.to_string()),
    );

    // Get S3 config from storage backend for endpoint/region settings
    let (endpoint, region) = match backend {
        Some(StorageBackend::S3(s3_config)) => (s3_config.endpoint.clone(), s3_config.region.clone()),
        _ => (None, "us-east-1".to_string()),
    };

    // Read AWS credentials from environment (optional for IAM role/instance-profile auth).
    // Both access_key and secret_key must be set or neither — partial configuration
    // is an error. session_token is optional but requires both access_key and secret_key.
    let access_key = std::env::var("AWS_ACCESS_KEY_ID").ok().filter(|v| !v.is_empty());
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok().filter(|v| !v.is_empty());
    let session_token = std::env::var("AWS_SESSION_TOKEN").ok().filter(|v| !v.is_empty());

    if access_key.is_some() != secret_key.is_some() {
        return Err(CommonError::Config(
            "AWS credentials partially configured; set both AWS_ACCESS_KEY_ID and \
             AWS_SECRET_ACCESS_KEY, or neither"
                .to_string(),
        ));
    }

    if session_token.is_some() && access_key.is_none() {
        return Err(CommonError::Config(
            "AWS_SESSION_TOKEN requires both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY".to_string(),
        ));
    }

    // Build OpenDAL S3 service
    let mut s3 = S3::default().bucket(&bucket).region(&region);
    if let Some(ref key_id) = access_key {
        s3 = s3.access_key_id(key_id);
    }
    if let Some(ref secret) = secret_key {
        s3 = s3.secret_access_key(secret);
    }
    if let Some(ref token) = session_token {
        s3 = s3.session_token(token);
    }

    if let Some(ep) = &endpoint {
        s3 = s3.endpoint(ep);
    }

    // Build Operator with layers (outermost applied last, executed first).
    // Desired execution order (outermost → innermost):
    //
    //   OtelMetrics → [FoyerCache] → OtelTrace → Retry → S3
    //
    // Metrics observe every request (including cache hits), while traces
    // only cover actual S3 round-trips beneath the cache.
    //
    // Each `.layer()` call changes the generic type of `OperatorBuilder`, so
    // the FoyerLayer branch must be handled as a separate code path.
    let meter = opentelemetry::global::meter("wal-opendal");
    let base = Operator::new(s3)
        .map_err(|e| CommonError::Config(format!("Failed to build OpenDAL S3 operator: {e}")))?
        .layer(RetryLayer::new())
        .layer(OtelTraceLayer);

    let operator = if let Some(foyer_cache) = cache {
        let mut foyer_layer = iceberg::io::FoyerLayer::new(foyer_cache.clone());
        if let Some(limit) = cache_object_size_limit {
            foyer_layer = foyer_layer.with_size_limit(..limit);
        }
        base.layer(foyer_layer)
            .layer(OtelMetricsLayer::builder().register(&meter))
            .finish()
    } else {
        base.layer(OtelMetricsLayer::builder().register(&meter)).finish()
    };

    let store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(operator));

    Ok((store, prefix))
}

/// Create a local filesystem-backed object store.
///
/// # Arguments
///
/// * `base_path` - Local path, optionally prefixed with `file://`
///
/// # Returns
///
/// A tuple of (object store, normalized base path) where the base path is empty
/// (the full path is handled by the store prefix).
pub fn create_local_store(base_path: &str) -> Result<ObjectStoreWithPath> {
    let path = base_path.strip_prefix("file://").unwrap_or(base_path);
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(path)?);
    Ok((store, String::new()))
}

/// Create an in-memory object store.
///
/// # Arguments
///
/// * `base_path` - The base path to preserve in the returned normalized path
///
/// # Returns
///
/// A tuple of (object store, normalized base path) where the base path is
/// preserved as-is for use within the memory store.
pub fn create_memory_store(base_path: &str) -> ObjectStoreWithPath {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    (store, base_path.to_string())
}

/// Create the appropriate object store based on the base path scheme.
///
/// Automatically selects the storage backend based on the path prefix:
/// - `s3://` - Amazon S3 or S3-compatible storage (via `OpenDAL` with layers)
/// - `file://` or absolute path (`/`) - Local filesystem
/// - Other - In-memory storage
///
/// # Arguments
///
/// * `base_path` - The storage path with optional scheme prefix
/// * `backend` - Optional storage backend configuration (used for S3 endpoint/region)
/// * `cache` - Optional foyer cache for S3 read caching (ignored for non-S3 backends)
/// * `cache_object_size_limit` - Maximum object size (bytes) eligible for caching
///   (ignored for non-S3 backends). See [`create_s3_store`].
///
/// # Returns
///
/// A tuple of (object store, normalized base path) appropriate for the storage type.
pub fn create_object_store(
    base_path: &str,
    backend: Option<&StorageBackend>,
    cache: Option<&iceberg::io::FoyerCache>,
    cache_object_size_limit: Option<usize>,
) -> Result<ObjectStoreWithPath> {
    if base_path.starts_with("s3://") {
        create_s3_store(base_path, backend, cache, cache_object_size_limit)
    } else if base_path.starts_with("file://") || base_path.starts_with('/') {
        create_local_store(base_path)
    } else {
        Ok(create_memory_store(base_path))
    }
}
