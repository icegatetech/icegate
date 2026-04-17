//! In-memory catalog storage for unit tests.
#![cfg_attr(not(test), allow(dead_code))]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use iceberg::spec::TableMetadata;
use tokio::sync::RwLock;

use crate::codec::CatalogCodec;
use crate::error::{Error, Result};
use crate::model::CatalogRoot;
use crate::storage::{CatalogStorage, Version};

const ABSENT_VERSION: &str = "absent";

/// In-memory storage backend for unit tests.
pub(crate) struct InMemoryCatalogStorage {
    root: RwLock<Option<(CatalogRoot, u64)>>,
    metadata: DashMap<String, Bytes>,
    version: AtomicU64,
    codec: Arc<dyn CatalogCodec>,
}

impl InMemoryCatalogStorage {
    /// Create a new in-memory catalog storage.
    pub(crate) fn new(codec: Arc<dyn CatalogCodec>) -> Self {
        Self {
            root: RwLock::new(None),
            metadata: DashMap::new(),
            version: AtomicU64::new(0),
            codec,
        }
    }
}

#[async_trait]
impl CatalogStorage for InMemoryCatalogStorage {
    async fn load_root(&self) -> Result<(CatalogRoot, Version)> {
        let guard = self.root.read().await;
        Ok(match guard.as_ref() {
            Some((root, version)) => (root.clone(), Version(version.to_string())),
            None => (CatalogRoot::default(), Version(ABSENT_VERSION.to_string())),
        })
    }

    async fn save_root(&self, root: CatalogRoot, expected: &Version) -> Result<()> {
        let mut guard = self.root.write().await;
        if let Some((_, current_version)) = guard.as_ref() {
            if expected.0 != current_version.to_string() {
                drop(guard);
                return Err(Error::CommitConflict);
            }
        } else if expected.0 != ABSENT_VERSION {
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
            .map(|entry| entry.clone())
            .ok_or_else(|| Error::InvalidMetadata(format!("Metadata location not found: {location}")))?;
        self.codec.decode_metadata(&payload)
    }

    async fn write_table_metadata(&self, table_id: &str, sequence_number: i64, metadata: &TableMetadata) -> Result<String> {
        let location = format!(
            "memory://catalog/tables/{table_id}/metadata/{sequence_number:05}-{}.{}",
            self.version.fetch_add(1, Ordering::SeqCst) + 1,
            self.codec.file_extension()
        );
        let payload = self.codec.encode_metadata(metadata)?;
        self.metadata.insert(location.clone(), payload);
        Ok(location)
    }

    fn default_table_location(&self, table_id: &str) -> String {
        format!("memory://catalog/tables/{table_id}")
    }
}
