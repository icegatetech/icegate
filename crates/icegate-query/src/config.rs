//! Query binary configuration
//!
//! Root configuration for the query binary, containing catalog, storage,
//! and all query API server configurations (Loki, Prometheus, Tempo).

use std::path::Path;

use icegate_common::{check_port_conflicts, load_config_file, CatalogConfig, StorageConfig};
use serde::{Deserialize, Serialize};

use super::{engine::QueryEngineConfig, loki::LokiConfig, prometheus::PrometheusConfig, tempo::TempoConfig};
use crate::error::Result;

/// Query binary configuration
///
/// Root configuration struct for the query binary. Contains catalog and storage
/// configuration shared across all query servers, plus individual server
/// configs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Iceberg catalog configuration
    pub catalog: CatalogConfig,
    /// Storage backend configuration
    pub storage: StorageConfig,
    /// Query engine configuration (shared across all APIs)
    #[serde(default)]
    pub engine: QueryEngineConfig,
    /// Loki API server
    pub loki: LokiConfig,
    /// Prometheus API server
    pub prometheus: PrometheusConfig,
    /// Tempo API server
    pub tempo: TempoConfig,
}

impl QueryConfig {
    /// Load configuration from a file (TOML or YAML)
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config: Self = load_config_file(path.as_ref())?;
        config.validate()?;
        Ok(config)
    }

    /// Validate all configurations
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration is invalid
    pub fn validate(&self) -> Result<()> {
        self.catalog.validate()?;
        self.storage.validate()?;
        self.engine.validate()?;
        self.loki.validate()?;
        self.prometheus.validate()?;
        self.tempo.validate()?;

        // Check for port conflicts among enabled servers
        check_port_conflicts(&[&self.loki, &self.prometheus, &self.tempo])?;

        Ok(())
    }
}
