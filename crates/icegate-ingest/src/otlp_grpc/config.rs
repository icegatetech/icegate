//! OTLP gRPC server configuration

use icegate_common::ServerConfig;
use serde::{Deserialize, Serialize};

use crate::error::{IngestError, Result};

/// Default host for OTLP gRPC server.
const DEFAULT_HOST: &str = "0.0.0.0";

/// Default port for OTLP gRPC server (OTLP/gRPC standard).
const DEFAULT_PORT: u16 = 4317;

/// OTLP gRPC server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OtlpGrpcConfig {
    /// Whether this server is enabled
    pub enabled: bool,
    /// Host to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
}

impl OtlpGrpcConfig {
    /// Validate OTLP gRPC configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid
    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.host.trim().is_empty() {
            return Err(IngestError::Config("OTLP gRPC host cannot be empty".into()));
        }
        Ok(())
    }
}

impl Default for OtlpGrpcConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
        }
    }
}

impl ServerConfig for OtlpGrpcConfig {
    fn name(&self) -> &'static str {
        "OTLP gRPC"
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn port(&self) -> u16 {
        self.port
    }
}
