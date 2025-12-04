//! Ingest binary configuration
//!
//! Root configuration for the ingest binary, containing catalog, storage,
//! and all OTLP receiver configurations (HTTP, gRPC).

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::{otlp_grpc::OtlpGrpcConfig, otlp_http::OtlpHttpConfig};
use crate::common::{check_port_conflicts, load_config_file, CatalogConfig, StorageConfig};

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
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let config: Self = load_config_file(path.as_ref())?;
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
        self.otlp_http.validate()?;
        self.otlp_grpc.validate()?;

        // Check for port conflicts among enabled servers
        check_port_conflicts(&[&self.otlp_http, &self.otlp_grpc])?;

        Ok(())
    }
}
