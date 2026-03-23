//! Catalog configuration
//!
//! Defines configuration structures for Iceberg catalog backends.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{CommonError, Result};
use crate::storage::PrefetchConfig;

/// Catalog configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogConfig {
    /// Type of catalog
    pub backend: CatalogBackend,
    /// Warehouse location
    pub warehouse: String,
    /// Additional catalog-specific properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// Optional IO cache for `FileIO` reads.
    ///
    /// When set, wraps the Iceberg `FileIO` with a foyer hybrid cache
    /// (memory + disk) to reduce S3 round-trips for repeated reads.
    #[serde(default)]
    pub cache: Option<CacheConfig>,
    /// Optional Parquet column-chunk prefetch configuration.
    ///
    /// When set, detects footer reads on `.parquet` files and proactively
    /// fetches column chunks into the cache before the query engine
    /// requests them.
    #[serde(default)]
    pub prefetch: Option<PrefetchConfig>,
}

/// IO cache configuration for the `FileIO` layer.
///
/// When enabled, wraps the Iceberg `FileIO` with a foyer hybrid cache
/// (memory + disk) to reduce S3 round-trips for repeated reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Memory cache capacity in mebibytes (e.g., 64).
    pub memory_size_mb: usize,
    /// Directory for disk cache storage.
    pub disk_dir: String,
    /// Disk cache capacity in mebibytes (e.g., 256).
    pub disk_size_mb: usize,
    /// Optional TTL (in seconds) for caching `stat` (HEAD) responses.
    ///
    /// When set, S3 HEAD requests for immutable objects are cached in
    /// memory and reused until the TTL expires, avoiding redundant
    /// round-trips during query execution.
    #[serde(default)]
    pub stat_ttl_secs: Option<u64>,
    /// Maximum value size (in mebibytes) to cache on writes.
    ///
    /// Files larger than this are written to storage but not cached,
    /// preventing large data files from evicting smaller WAL segments.
    /// Defaults to 2 `MiB` when not set.
    #[serde(default)]
    pub max_write_cache_size_mb: Option<usize>,
}

/// Types of catalogs supported
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogBackend {
    /// In-memory catalog (for testing)
    Memory,
    /// REST catalog (production, e.g. Nessie)
    Rest {
        /// REST endpoint URI
        uri: String,
    },
    /// AWS S3 Tables catalog (production, AWS-managed Iceberg)
    S3Tables {
        /// S3 Tables table bucket ARN
        ///
        /// Format: `arn:aws:s3tables:<region>:<account>:bucket/<name>`
        table_bucket_arn: String,
    },
    /// AWS Glue catalog (production, AWS-managed Iceberg)
    Glue {
        /// AWS Glue catalog identifier (optional).
        ///
        /// When omitted, the default AWS account catalog is used.
        /// Format: 12-digit AWS account ID (e.g., `123456789012`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        catalog_id: Option<String>,
    },
}

impl CatalogConfig {
    /// Validate the catalog configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid
    pub fn validate(&self) -> Result<()> {
        // Validate warehouse path is not empty
        if self.warehouse.trim().is_empty() {
            return Err(CommonError::Config("Warehouse location cannot be empty".into()));
        }

        // Validate catalog type specific requirements
        match &self.backend {
            CatalogBackend::Rest { uri } => {
                if uri.trim().is_empty() {
                    return Err(CommonError::Config("REST catalog URI cannot be empty".into()));
                }
                // Basic URI validation
                if !uri.starts_with("http://") && !uri.starts_with("https://") {
                    return Err(CommonError::Config(
                        "REST catalog URI must start with http:// or https://".into(),
                    ));
                }
            }
            CatalogBackend::Memory => {
                // No specific validation needed for memory catalog
            }
            CatalogBackend::S3Tables { table_bucket_arn } => {
                if table_bucket_arn.trim().is_empty() {
                    return Err(CommonError::Config("S3 Tables table_bucket_arn cannot be empty".into()));
                }
                if !table_bucket_arn.starts_with("arn:aws:s3tables:") {
                    return Err(CommonError::Config(
                        "S3 Tables table_bucket_arn must start with arn:aws:s3tables:".into(),
                    ));
                }
            }
            CatalogBackend::Glue { catalog_id } => {
                if let Some(id) = catalog_id {
                    if id.len() != 12 || !id.bytes().all(|b| b.is_ascii_digit()) {
                        return Err(CommonError::Config(
                            "Glue catalog_id must be a 12-digit AWS account ID".into(),
                        ));
                    }
                }
            }
        }

        // Validate cache config if present
        if let Some(ref cache) = self.cache {
            if cache.memory_size_mb == 0 {
                return Err(CommonError::Config(
                    "Cache memory_size_mb must be greater than 0".into(),
                ));
            }
            if cache.disk_dir.trim().is_empty() {
                return Err(CommonError::Config("Cache disk_dir cannot be empty".into()));
            }
            if cache.disk_size_mb == 0 {
                return Err(CommonError::Config("Cache disk_size_mb must be greater than 0".into()));
            }
            if cache.stat_ttl_secs == Some(0) {
                return Err(CommonError::Config(
                    "Cache stat_ttl_secs must be greater than 0 when set".into(),
                ));
            }
        }

        // Validate prefetch config if present
        if let Some(ref prefetch) = self.prefetch {
            if prefetch.max_prefetch_bytes == 0 {
                return Err(CommonError::Config(
                    "Prefetch max_prefetch_bytes must be greater than 0".into(),
                ));
            }
        }

        Ok(())
    }
}

impl Default for CatalogConfig {
    fn default() -> Self {
        Self {
            backend: CatalogBackend::Memory,
            warehouse: "/tmp/icegate/warehouse".to_string(),
            properties: HashMap::new(),
            cache: None,
            prefetch: None,
        }
    }
}
