//! Prometheus server configuration

use serde::{Deserialize, Serialize};

use crate::common::ServerConfig;

/// Prometheus server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusConfig {
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

impl PrometheusConfig {
    /// Validate Prometheus configuration
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.enabled && self.host.trim().is_empty() {
            return Err("Prometheus host cannot be empty".into());
        }
        Ok(())
    }
}

impl Default for PrometheusConfig {
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
    9090
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
