//! Flight SQL gRPC server configuration.

use icegate_common::ServerConfig;
use serde::{Deserialize, Serialize};

use crate::error::{QueryError, Result};

/// Default host for the Flight SQL gRPC server.
const DEFAULT_HOST: &str = "0.0.0.0";

/// Default port for the Flight SQL gRPC server.
///
/// 8815 is the Apache Arrow Flight community-standard port.
const DEFAULT_PORT: u16 = 8815;

/// Default maximum gRPC message size in bytes.
///
/// Single Arrow IPC dictionary messages can exceed the tonic default of
/// 4 `MiB`. 16 `MiB` is a safe headroom for analytical workloads while still
/// bounding memory pressure under adversarial inputs.
const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Flight SQL gRPC server configuration.
///
/// Exposes the IceGate `DataFusion` engine over Apache Arrow Flight SQL so
/// BI tools, ADBC clients, and the `DataFusion` CLI can issue ad-hoc SQL
/// directly against the merged WAL + Iceberg view of the catalog. Tenant
/// isolation is enforced per-request by the session provider; this struct
/// only governs transport-level concerns.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FlightSqlConfig {
    /// Whether the Flight SQL server is enabled.
    pub enabled: bool,
    /// Host or interface to bind to.
    pub host: String,
    /// TCP port to listen on.
    pub port: u16,
    /// Maximum gRPC encode/decode message size in bytes.
    pub max_message_size: usize,
}

impl FlightSqlConfig {
    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the server is enabled with an empty host or a
    /// zero `max_message_size`.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.host.trim().is_empty() {
            return Err(QueryError::Config("Flight SQL host cannot be empty".into()));
        }
        if self.max_message_size == 0 {
            return Err(QueryError::Config(
                "Flight SQL max_message_size must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

impl Default for FlightSqlConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: DEFAULT_HOST.to_string(),
            port: DEFAULT_PORT,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }
}

impl ServerConfig for FlightSqlConfig {
    fn name(&self) -> &'static str {
        "FlightSQL"
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn port(&self) -> u16 {
        self.port
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_enabled_on_8815() {
        let cfg = FlightSqlConfig::default();
        assert!(cfg.enabled);
        assert_eq!(cfg.port, DEFAULT_PORT);
        assert_eq!(cfg.host, DEFAULT_HOST);
        assert_eq!(cfg.max_message_size, DEFAULT_MAX_MESSAGE_SIZE);
    }

    #[test]
    fn validate_rejects_empty_host_when_enabled() {
        let cfg = FlightSqlConfig {
            enabled: true,
            host: String::new(),
            ..FlightSqlConfig::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_allows_empty_host_when_disabled() {
        let cfg = FlightSqlConfig {
            enabled: false,
            host: String::new(),
            ..FlightSqlConfig::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_max_message_size() {
        let cfg = FlightSqlConfig {
            max_message_size: 0,
            ..FlightSqlConfig::default()
        };
        assert!(cfg.validate().is_err());
    }
}
