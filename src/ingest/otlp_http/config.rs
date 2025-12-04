//! OTLP HTTP server configuration

use serde::{Deserialize, Serialize};

use crate::common::ServerConfig;

/// OTLP HTTP server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpHttpConfig {
    /// Whether this server is enabled
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Host to bind to
    #[serde(default = "default_host")]
    pub host: String,
    /// Port to listen on
    #[serde(default = "default_port")]
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
            host: default_host(),
            port: default_port(),
        }
    }
}

const fn default_true() -> bool {
    true
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

const fn default_port() -> u16 {
    4318
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
