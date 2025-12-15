//! Tempo server configuration

use icegate_common::ServerConfig;
use serde::{Deserialize, Serialize};

/// Tempo server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TempoConfig {
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

impl TempoConfig {
    /// Validate Tempo configuration
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.enabled && self.host.trim().is_empty() {
            return Err("Tempo host cannot be empty".into());
        }
        Ok(())
    }
}

impl Default for TempoConfig {
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
    3200
}

impl ServerConfig for TempoConfig {
    fn name(&self) -> &'static str {
        "Tempo"
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn port(&self) -> u16 {
        self.port
    }
}
