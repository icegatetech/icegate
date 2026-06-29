//! Storage configuration
//!
//! Defines configuration structures for storage backends.

use serde::{Deserialize, Serialize};

use crate::error::{CommonError, Result};

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage backend type
    pub backend: StorageBackend,
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
#[derive(Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// AWS region
    pub region: String,
    /// Optional custom endpoint (for S3-compatible storage)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Optional explicit access key id. When set together with
    /// [`Self::secret_access_key`] it takes precedence over the ambient `AWS_*`
    /// environment / IAM credential chain. Leave unset for IAM role or
    /// instance-profile auth (the production default).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    /// Optional explicit secret access key, paired with [`Self::access_key_id`].
    /// Redacted in [`Debug`] output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
}

// Manual `Debug` so an accidental log of a `StorageConfig` can never leak the
// secret access key; the rest of the fields are non-sensitive.
impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl StorageConfig {
    /// Validate the storage configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid
    pub fn validate(&self) -> Result<()> {
        match &self.backend {
            StorageBackend::FileSystem { root_path } => {
                if root_path.trim().is_empty() {
                    return Err(CommonError::Config("FileSystem root path cannot be empty".into()));
                }
            }
            StorageBackend::S3(s3_config) => {
                if s3_config.bucket.trim().is_empty() {
                    return Err(CommonError::Config("S3 bucket cannot be empty".into()));
                }
                if s3_config.region.trim().is_empty() {
                    return Err(CommonError::Config("S3 region cannot be empty".into()));
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
        }
    }
}
