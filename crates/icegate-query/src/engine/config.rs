//! Query engine configuration
//!
//! Configuration for the shared `QueryEngine` that manages `DataFusion` session
//! creation and caches the `IcebergCatalogProvider` to avoid per-query metadata
//! fetches.

use serde::{Deserialize, Serialize};

/// Configuration for the `QueryEngine`
///
/// Controls `DataFusion` session parameters and catalog provider caching
/// behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEngineConfig {
    /// `DataFusion` batch size for query execution
    ///
    /// Controls how many rows are processed at once during query execution.
    /// Higher values may improve throughput but increase memory usage.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Number of target partitions for parallel execution
    ///
    /// Controls the degree of parallelism in query execution.
    /// Typically set to the number of CPU cores.
    #[serde(default = "default_target_partitions")]
    pub target_partitions: usize,

    /// Catalog name to register with `DataFusion`
    ///
    /// The name used to reference the Iceberg catalog in SQL queries
    /// (e.g., `SELECT * FROM iceberg.icegate.logs`).
    #[serde(default = "default_catalog_name")]
    pub catalog_name: String,

    /// Interval in seconds for refreshing the `IcebergCatalogProvider` cache
    ///
    /// Set to 0 to disable periodic refresh (provider only created at startup).
    /// The cache is refreshed in a background task to avoid blocking queries.
    #[serde(default = "default_provider_refresh_seconds")]
    pub provider_refresh_seconds: u64,
}

const fn default_batch_size() -> usize {
    8192
}

const fn default_target_partitions() -> usize {
    4
}

fn default_catalog_name() -> String {
    "iceberg".to_string()
}

const fn default_provider_refresh_seconds() -> u64 {
    60
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            target_partitions: default_target_partitions(),
            catalog_name: default_catalog_name(),
            provider_refresh_seconds: default_provider_refresh_seconds(),
        }
    }
}

impl QueryEngineConfig {
    /// Validate configuration values
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration value is invalid
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.batch_size == 0 {
            return Err("batch_size must be greater than 0".into());
        }
        if self.target_partitions == 0 {
            return Err("target_partitions must be greater than 0".into());
        }
        if self.catalog_name.trim().is_empty() {
            return Err("catalog_name cannot be empty".into());
        }
        Ok(())
    }
}
