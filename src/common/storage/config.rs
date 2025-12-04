//! Storage configuration
//!
//! Defines configuration structures for storage backends.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage backend type
    pub backend: StorageBackend,
    /// Additional storage-specific properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Storage backend types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackend {
    /// In-memory storage (for testing)
    Memory,
    /// Local filesystem storage
    FileSystem {
        /// Root path for storage
        root_path: String,
    },
    /// Amazon S3 storage
    S3(S3Config),
}

/// S3 storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// AWS region
    pub region: String,
    /// Optional custom endpoint (for S3-compatible storage)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
}

impl StorageConfig {
    /// Validate the storage configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        match &self.backend {
            StorageBackend::FileSystem { root_path } => {
                if root_path.trim().is_empty() {
                    return Err("FileSystem root path cannot be empty".into());
                }
            }
            StorageBackend::S3(s3_config) => {
                if s3_config.bucket.trim().is_empty() {
                    return Err("S3 bucket cannot be empty".into());
                }
                if s3_config.region.trim().is_empty() {
                    return Err("S3 region cannot be empty".into());
                }
            }
            StorageBackend::Memory => {
                // No validation needed for memory storage
            }
        }

        Ok(())
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::Memory,
            properties: HashMap::new(),
        }
    }
}
