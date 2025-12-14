//! OTLP HTTP server configuration

use serde::{Deserialize, Serialize};

use crate::common::ServerConfig;

/// Default host for OTLP HTTP server.
const DEFAULT_HOST: &str = "0.0.0.0";

/// Default port for OTLP HTTP server (OTLP/HTTP standard).
const DEFAULT_PORT: u16 = 4318;

/// OTLP HTTP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct OtlpHttpConfig {
    /// Whether this server is enabled
    pub enabled: bool,
    /// Host to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
}

impl OtlpHttpConfig {
    /// Validate OTLP HTTP configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.enabled && self.host.trim().is_empty() {
            return Err("OTLP HTTP host cannot be empty".into());
        }
        Ok(())
    }
}

impl Default for OtlpHttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
        }
    }
}

impl ServerConfig for OtlpHttpConfig {
    fn name(&self) -> &'static str {
        "OTLP HTTP"
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn port(&self) -> u16 {
        self.port
    }
}
