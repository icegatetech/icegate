//! In-memory catalog storage for unit tests.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use iceberg::spec::TableMetadata;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::model::CatalogRoot;
use crate::storage::{CatalogStorage, Version};

/// In-memory storage backend for unit tests.
pub(crate) struct InMemoryCatalogStorage {
    root: RwLock<Option<(CatalogRoot, u64)>>,
    metadata: DashMap<String, Bytes>,
    version: AtomicU64,
}

impl InMemoryCatalogStorage {
    /// Create a new in-memory catalog storage.
    pub(crate) fn new() -> Self {
        Self {
            root: RwLock::new(None),
            metadata: DashMap::new(),
            version: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl CatalogStorage for InMemoryCatalogStorage {
    async fn load_root(&self) -> Result<(CatalogRoot, Version)> {
        let guard = self.root.read().await;
        Ok(match guard.as_ref() {
            Some((root, version)) => (root.clone(), Version::Etag(version.to_string())),
            None => (CatalogRoot::default(), Version::Absent),
        })
    }

    async fn save_root(&self, root: CatalogRoot, expected: &Version) -> Result<()> {
        let mut guard = self.root.write().await;
        if let Some((_, current_version)) = guard.as_ref() {
            if *expected != Version::Etag(current_version.to_string()) {
                drop(guard);
                return Err(Error::CommitConflict);
            }
        } else if *expected != Version::Absent {
            drop(guard);
            return Err(Error::CommitConflict);
        }

        let next_version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        *guard = Some((root, next_version));
        drop(guard);
        Ok(())
    }

    async fn read_table_metadata(&self, location: &str) -> Result<TableMetadata> {
        let payload = self
            .metadata
            .get(location)
            .map(|entry| Bytes::clone(entry.value()))
            .ok_or_else(|| Error::InvalidMetadata(format!("Metadata location not found: {location}")))?;
        serde_json::from_slice(&payload)
            .map_err(|error| Error::InvalidMetadata(format!("Invalid table metadata: {error}")))
    }

    async fn write_table_metadata(&self, location: &str, metadata: &TableMetadata) -> Result<()> {
        let payload = serde_json::to_vec(metadata)
            .map(Bytes::from)
            .map_err(|error| Error::InvalidMetadata(format!("Failed to serialize metadata: {error}")))?;
        self.metadata.insert(location.to_string(), payload);
        Ok(())
    }
}
