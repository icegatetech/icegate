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

/// Default interval (in seconds) between background catalog refreshes.
const DEFAULT_REFRESH_INTERVAL_SECS: u64 = 15;

/// Default maximum age (in seconds) before the cached provider is considered stale.
const DEFAULT_MAX_AGE_SECS: u64 = 30;

/// Default Parquet metadata size hint in bytes for WAL file footer reads.
///
/// 64 KB is large enough to capture the entire footer of typical WAL files
/// (~125 KB each) in a single S3 read, eliminating the 8-byte probe round-trip.
const DEFAULT_WAL_METADATA_SIZE_HINT: usize = 65_536;

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

    /// Interval (in seconds) between background catalog provider refreshes.
    ///
    /// The background task rebuilds the catalog provider every this many
    /// seconds so queries see fresh metadata without blocking.
    pub refresh_interval_secs: u64,

    /// Maximum age (in seconds) before a cached catalog provider is
    /// considered too stale.
    ///
    /// If the background refresh fails for longer than this, queries will
    /// block on a synchronous rebuild. Must be >= `refresh_interval_secs`.
    pub max_age_secs: u64,

    /// Whether to include WAL (hot) segments in query results.
    ///
    /// When `false` (default), queries only read committed Iceberg data.
    /// Enable this to query data that has not yet been shifted to Iceberg.
    ///
    /// **Note:** This setting only affects DataFusion query plans (log/trace
    /// queries). The `/labels`, `/label_values`, and `/series` metadata scan
    /// endpoints always read from Iceberg only and are not affected by this
    /// flag.
    pub wal_query_enabled: bool,

    /// Parquet metadata size hint in bytes for WAL file footer reads.
    ///
    /// When set, DataFusion reads this many bytes from the file tail in a
    /// single request instead of an 8-byte probe followed by a full footer
    /// read. For WAL files (~125 KB), 64 KB captures the entire footer in
    /// one read. Set to `None` to use the DataFusion default.
    pub wal_metadata_size_hint: Option<usize>,
}

impl Default for QueryEngineConfig {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            target_partitions: DEFAULT_TARGET_PARTITIONS,
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            refresh_interval_secs: DEFAULT_REFRESH_INTERVAL_SECS,
            max_age_secs: DEFAULT_MAX_AGE_SECS,
            wal_query_enabled: false,
            wal_metadata_size_hint: Some(DEFAULT_WAL_METADATA_SIZE_HINT),
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
        if self.refresh_interval_secs == 0 {
            return Err(QueryError::Config(
                "refresh_interval_secs must be greater than 0".into(),
            ));
        }
        if self.max_age_secs == 0 {
            return Err(QueryError::Config("max_age_secs must be greater than 0".into()));
        }
        if self.max_age_secs < self.refresh_interval_secs {
            return Err(QueryError::Config(
                "max_age_secs must be >= refresh_interval_secs".into(),
            ));
        }
        if let Some(hint) = self.wal_metadata_size_hint {
            if hint == 0 {
                return Err(QueryError::Config(
                    "wal_metadata_size_hint must be greater than 0 when set".into(),
                ));
            }
        }
        Ok(())
    }
}
