//! Maintain binary configuration
//!
//! Root configuration for the maintain binary, containing catalog and storage
//! configurations needed for maintenance operations.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::common::{CatalogConfig, StorageConfig};

/// Maintain binary configuration
///
/// Root configuration struct for the maintain binary. Contains catalog and storage
/// configuration needed for maintenance operations like migrations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MaintainConfig {
    /// Iceberg catalog configuration
    pub catalog: CatalogConfig,
    /// Storage backend configuration
    pub storage: StorageConfig,
}

impl MaintainConfig {
    /// Load configuration from a file (TOML or YAML)
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)?;

        let config: Self = match path.extension().and_then(|e| e.to_str()) {
            Some("toml") => toml::from_str(&content)?,
            Some("yaml" | "yml") => serde_yaml::from_str(&content)?,
            _ => {
                // Try TOML first, then YAML
                toml::from_str(&content)
                    .or_else(|_| serde_yaml::from_str(&content))
                    .map_err(|e| format!("Failed to parse config: {e}"))?
            }
        };

        config.validate()?;
        Ok(config)
    }

    /// Validate all configurations
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration is invalid
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.catalog.validate()?;
        self.storage.validate()?;
        Ok(())
    }
}
