//! Catalog configuration
//!
//! Defines configuration structures for Iceberg catalog backends.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{CommonError, Result};

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
}

/// Default maximum object size eligible for caching (16 `MiB`).
///
/// Large enough for Iceberg metadata files (manifests, manifest lists,
/// `metadata.json`) but small enough to prevent pulling entire Parquet
/// data files into memory on cache miss.
const DEFAULT_CACHE_OBJECT_SIZE_LIMIT_MB: usize = 16;

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
    /// Maximum size of a single object eligible for caching, in mebibytes.
    ///
    /// Objects larger than this limit bypass the cache entirely and are
    /// read directly from the backend. This prevents the `FoyerLayer`
    /// from pulling very large Parquet files into memory on cache miss.
    ///
    /// Defaults to 16 `MiB` when not specified, which is large enough for
    /// Iceberg metadata files but avoids caching full data files.
    #[serde(default = "default_object_size_limit_mb")]
    pub object_size_limit_mb: usize,
}

/// Returns the default object size limit for serde deserialization.
const fn default_object_size_limit_mb() -> usize {
    DEFAULT_CACHE_OBJECT_SIZE_LIMIT_MB
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
                    if id.trim().is_empty() {
                        return Err(CommonError::Config(
                            "Glue catalog_id, when specified, cannot be empty".into(),
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
            if cache.object_size_limit_mb == 0 {
                return Err(CommonError::Config(
                    "Cache object_size_limit_mb must be greater than 0".into(),
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
        }
    }
}
