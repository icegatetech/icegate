//! Catalog configuration
//!
//! Defines configuration structures for Iceberg catalog backends.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Validate warehouse path is not empty
        if self.warehouse.trim().is_empty() {
            return Err("Warehouse location cannot be empty".into());
        }

        // Validate catalog type specific requirements
        match &self.backend {
            CatalogBackend::Rest {
                uri,
            } => {
                if uri.trim().is_empty() {
                    return Err("REST catalog URI cannot be empty".into());
                }
                // Basic URI validation
                if !uri.starts_with("http://") && !uri.starts_with("https://") {
                    return Err("REST catalog URI must start with http:// or https://".into());
                }
            },
            CatalogBackend::Memory => {
                // No specific validation needed for memory catalog
            },
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
        }
    }
}
