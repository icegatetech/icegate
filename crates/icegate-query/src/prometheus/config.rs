//! Prometheus server configuration

use icegate_common::ServerConfig;
use serde::{Deserialize, Serialize};

use crate::error::{QueryError, Result};

/// Default host for Prometheus server.
const DEFAULT_HOST: &str = "0.0.0.0";

/// Default port for Prometheus server.
const DEFAULT_PORT: u16 = 9090;

/// Prometheus server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PrometheusConfig {
    /// Whether this server is enabled
    pub enabled: bool,
    /// Host to bind to
    pub host: String,
    /// Port to listen on
    pub port: u16,
}

impl PrometheusConfig {
    /// Validate Prometheus configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid
    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.host.trim().is_empty() {
            return Err(QueryError::Config("Prometheus host cannot be empty".into()));
        }
        Ok(())
    }
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
        }
    }
}

impl ServerConfig for PrometheusConfig {
    fn name(&self) -> &'static str {
        "Prometheus"
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn port(&self) -> u16 {
        self.port
    }
}
