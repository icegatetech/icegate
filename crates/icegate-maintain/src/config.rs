//! Maintain binary configuration
//!
//! Root configuration for the maintain binary, containing catalog and storage
//! configurations needed for maintenance operations.

use std::path::Path;

use icegate_common::{CatalogConfig, MetricsConfig, StorageConfig};
use serde::{Deserialize, Serialize};

use crate::compact::config::CompactionConfig;

/// Maintain binary configuration
///
/// Root configuration struct for the maintain binary. Contains catalog and
/// storage configuration needed for maintenance operations like migrations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MaintainConfig {
    /// Iceberg catalog configuration
    pub catalog: CatalogConfig,
    /// Storage backend configuration
    pub storage: StorageConfig,
    /// Parquet compaction configuration
    ///
    /// Defaulted when absent from the config file so existing migrate configs
    /// (which have no `compaction` key) continue to load unchanged.
    #[serde(default)]
    pub compaction: CompactionConfig,
    /// Prometheus metrics endpoint configuration for the long-running `run`
    /// service. Disabled by default and ignored by the one-shot `migrate`
    /// commands; when enabled, `run` installs the global meter provider (so the
    /// compactor's `CompactMetrics` record) and serves `/metrics`.
    #[serde(default)]
    pub metrics: MetricsConfig,
}

impl MaintainConfig {
    /// Load configuration from a file (TOML or YAML)
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let config: Self = icegate_common::load_config_file(path.as_ref())?;
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
        self.metrics.validate()?;
        Ok(())
    }
}
