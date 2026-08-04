//! Object stores that carry no `OpenDAL` operator.
//!
//! Local and in-memory stores retain nothing and are built per call. The
//! object stores that do carry an operator are served by
//! [`OperatorRegistry`](super::registry::OperatorRegistry), which builds each
//! one once.

use std::sync::Arc;

use object_store::{ObjectStore, local::LocalFileSystem, memory::InMemory};

use crate::error::Result;

/// Result containing the object store and the normalized base path.
///
/// The normalized base path is the path within the store (e.g., the S3 prefix
/// without the bucket, or empty string for local filesystem).
pub type ObjectStoreWithPath = (Arc<dyn ObjectStore>, String);

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
///
/// # Errors
///
/// Returns an error if the directory cannot be opened.
pub(crate) fn create_local_store(base_path: &str) -> Result<ObjectStoreWithPath> {
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
pub(crate) fn create_memory_store(base_path: &str) -> ObjectStoreWithPath {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    (store, base_path.to_string())
}
