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
}

/// Types of catalogs supported
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CatalogBackend {
    /// In-memory catalog (for testing)
    Memory,
    /// REST catalog (production)
    Rest {
        /// REST endpoint URI
        uri: String,
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
