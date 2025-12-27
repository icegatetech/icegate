//! Object store builder utilities.
//!
//! Provides functions to create object stores from configuration.

use std::sync::Arc;

use object_store::{ObjectStore, aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory};

use super::StorageBackend;
use crate::error::{CommonError, Result};

/// Result containing the object store and the normalized base path.
///
/// The normalized base path is the path within the store (e.g., the S3 prefix
/// without the bucket, or empty string for local filesystem).
pub type ObjectStoreWithPath = (Arc<dyn ObjectStore>, String);

/// Create an S3-backed object store.
///
/// Parses the S3 URL, reads credentials from environment, and builds the store.
///
/// # Arguments
///
/// * `base_path` - S3 URL in the format `s3://bucket/prefix`
/// * `backend` - Optional storage backend configuration for endpoint/region settings
///
/// # Returns
///
/// A tuple of (object store, normalized base path) where the base path is just
/// the prefix portion (bucket is handled by the store).
pub fn create_s3_store(base_path: &str, backend: Option<&StorageBackend>) -> Result<ObjectStoreWithPath> {
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

    let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket).with_region(&region);

    if let Some(endpoint) = &endpoint {
        builder = builder.with_endpoint(endpoint).with_allow_http(true);
    }

    // Read and validate AWS credentials from environment
    let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
        .map_err(|_| CommonError::Config("AWS_ACCESS_KEY_ID environment variable is not set".to_string()))?;
    if access_key_id.is_empty() {
        return Err(CommonError::Config(
            "AWS_ACCESS_KEY_ID environment variable is empty".to_string(),
        ));
    }

    let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| CommonError::Config("AWS_SECRET_ACCESS_KEY environment variable is not set".to_string()))?;
    if secret_access_key.is_empty() {
        return Err(CommonError::Config(
            "AWS_SECRET_ACCESS_KEY environment variable is empty".to_string(),
        ));
    }

    builder = builder.with_access_key_id(access_key_id);
    builder = builder.with_secret_access_key(secret_access_key);

    let store: Arc<dyn ObjectStore> = Arc::new(builder.build()?);

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
/// - `s3://` - Amazon S3 or S3-compatible storage
/// - `file://` or absolute path (`/`) - Local filesystem
/// - Other - In-memory storage
///
/// # Arguments
///
/// * `base_path` - The storage path with optional scheme prefix
/// * `backend` - Optional storage backend configuration (used for S3 endpoint/region)
///
/// # Returns
///
/// A tuple of (object store, normalized base path) appropriate for the storage type.
pub fn create_object_store(base_path: &str, backend: Option<&StorageBackend>) -> Result<ObjectStoreWithPath> {
    if base_path.starts_with("s3://") {
        create_s3_store(base_path, backend)
    } else if base_path.starts_with("file://") || base_path.starts_with('/') {
        create_local_store(base_path)
    } else {
        Ok(create_memory_store(base_path))
    }
}
