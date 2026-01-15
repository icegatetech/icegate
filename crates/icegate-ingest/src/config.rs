//! Ingest binary configuration
//!
//! Root configuration for the ingest binary, containing catalog, storage,
//! and all OTLP receiver configurations (HTTP, gRPC).

use std::path::Path;

use icegate_common::{CatalogConfig, StorageConfig, check_port_conflicts, load_config_file};
use icegate_queue::QueueConfig;
use serde::{Deserialize, Serialize};

use super::{otlp_grpc::OtlpGrpcConfig, otlp_http::OtlpHttpConfig};
use crate::error::Result;
use crate::shift::ShiftConfig;

/// Ingest binary configuration
///
/// Root configuration struct for the ingest binary. Contains catalog and
/// storage configuration shared across all ingest servers, plus individual
/// server configs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestConfig {
    /// Iceberg catalog configuration
    pub catalog: CatalogConfig,
    /// Storage backend configuration
    pub storage: StorageConfig,
    /// Queue configuration for WAL-based ingestion
    #[serde(default)]
    pub queue: Option<QueueConfig>,
    /// Shift configuration for moving WAL to Iceberg
    #[serde(default)]
    pub shift: ShiftConfig,
    /// OTLP HTTP server
    pub otlp_http: OtlpHttpConfig,
    /// OTLP gRPC server
    pub otlp_grpc: OtlpGrpcConfig,
}

impl IngestConfig {
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
        self.otlp_http.validate()?;
        self.otlp_grpc.validate()?;
        self.shift.validate()?;

        // Check for port conflicts among enabled servers
        check_port_conflicts(&[&self.otlp_http, &self.otlp_grpc])?;

        Ok(())
    }
}
