//! Query engine configuration.
//!
//! Configuration for the `QueryEngine` that manages `DataFusion` session
//! creation with fresh catalog providers built per session.

use serde::{Deserialize, Serialize};

use crate::error::{QueryError, Result};

/// Default batch size for `DataFusion` query execution.
const DEFAULT_BATCH_SIZE: usize = 8192;

/// Default number of target partitions for parallel execution.
const DEFAULT_TARGET_PARTITIONS: usize = 4;

/// Default catalog name to register with `DataFusion`.
const DEFAULT_CATALOG_NAME: &str = "iceberg";

/// Configuration for the `QueryEngine`
///
/// Controls `DataFusion` session parameters and catalog provider behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryEngineConfig {
    /// `DataFusion` batch size for query execution
    ///
    /// Controls how many rows are processed at once during query execution.
    /// Higher values may improve throughput but increase memory usage.
    pub batch_size: usize,

    /// Number of target partitions for parallel execution
    ///
    /// Controls the degree of parallelism in query execution.
    /// Typically set to the number of CPU cores.
    pub target_partitions: usize,

    /// Catalog name to register with `DataFusion`
    ///
    /// The name used to reference the Iceberg catalog in SQL queries
    /// (e.g., `SELECT * FROM iceberg.icegate.logs`).
    pub catalog_name: String,

    /// WAL base path for reading hot data (e.g., `s3://queue/`).
    ///
    /// The query engine merges WAL (Write-Ahead Log) segments with Iceberg
    /// data for near-real-time queries. This field is **required** — the
    /// query service will refuse to start if it is empty.
    pub wal_base_path: String,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            target_partitions: DEFAULT_TARGET_PARTITIONS,
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            wal_base_path: String::new(),
        }
    }
}

impl QueryEngineConfig {
    /// Validate configuration values
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration value is invalid
    pub fn validate(&self) -> Result<()> {
        if self.batch_size == 0 {
            return Err(QueryError::Config("batch_size must be greater than 0".into()));
        }
        if self.target_partitions == 0 {
            return Err(QueryError::Config("target_partitions must be greater than 0".into()));
        }
        if self.catalog_name.trim().is_empty() {
            return Err(QueryError::Config("catalog_name cannot be empty".into()));
        }
        if self.wal_base_path.trim().is_empty() {
            return Err(QueryError::Config("wal_base_path must be configured".into()));
        }
        Ok(())
    }
}
