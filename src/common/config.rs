//! Common configuration utilities.
//!
//! Provides shared traits and functions for configuration loading and validation
//! to reduce duplication across query and ingest binaries.

use std::path::Path;

use serde::de::DeserializeOwned;

/// Trait for server configurations that can have port conflicts checked.
///
/// Implement this trait for any server configuration that:
/// - Can be enabled/disabled
/// - Binds to a network port
pub trait ServerConfig {
    /// Human-readable name for error messages (e.g., "Loki", "OTLP HTTP")
    fn name(&self) -> &'static str;

    /// Whether this server is enabled
    fn enabled(&self) -> bool;

    /// The port this server listens on
    fn port(&self) -> u16;
}

/// Check for port conflicts among enabled servers.
///
/// # Errors
///
/// Returns an error if two or more enabled servers are configured to use the same port.
pub fn check_port_conflicts(servers: &[&dyn ServerConfig]) -> Result<(), Box<dyn std::error::Error>> {
    let enabled: Vec<_> = servers.iter().filter(|s| s.enabled()).collect();

    for i in 0..enabled.len() {
        for j in (i + 1)..enabled.len() {
            if enabled[i].port() == enabled[j].port() {
                return Err(format!(
                    "Port {} is configured for both {} and {}",
                    enabled[i].port(),
                    enabled[i].name(),
                    enabled[j].name()
                )
                .into());
            }
        }
    }

    Ok(())
}

/// Load a configuration from a file path.
///
/// Supports TOML and YAML formats. Format is determined by file extension,
/// or auto-detected if extension is not recognized.
///
/// # Errors
///
/// Returns an error if:
/// - The file cannot be read
/// - The file cannot be parsed as TOML or YAML
pub fn load_config_file<T: DeserializeOwned>(path: &Path) -> Result<T, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;

    match path.extension().and_then(|e| e.to_str()) {
        Some("toml") => Ok(toml::from_str(&content)?),
        Some("yaml" | "yml") => Ok(serde_yaml::from_str(&content)?),
        _ => {
            // Try TOML first, then YAML
            toml::from_str(&content)
                .or_else(|_| serde_yaml::from_str(&content))
                .map_err(|e| format!("Failed to parse config: {e}").into())
        }
    }
}
