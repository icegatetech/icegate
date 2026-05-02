//! OTLP HTTP server configuration

use icegate_common::ServerConfig;
use serde::{Deserialize, Serialize};

use crate::error::{IngestError, Result};

/// Default host for OTLP HTTP server.
const DEFAULT_HOST: &str = "0.0.0.0";

/// Default port for OTLP HTTP server (OTLP/HTTP standard).
const DEFAULT_PORT: u16 = 4318;

/// Default maximum decompressed request body size for OTLP HTTP endpoints (16 `MiB`).
///
/// The Axum framework default is 2 `MiB`, which is below realistic batch sizes produced
/// by the `OpenTelemetry` Collector's `otlp_http` exporter and causes oversized batches
/// to be rejected with HTTP 413 and dropped as non-retryable.
const DEFAULT_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Default for [`OtlpHttpConfig::max_body_bytes`] when deserializing from config files
/// that omit the field.
const fn default_max_body_bytes() -> usize {
    DEFAULT_MAX_BODY_BYTES
}

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
    /// Maximum accepted request body size, in bytes.
    ///
    /// The limit is checked *after* `RequestDecompressionLayer` runs, so it applies to the
    /// decompressed payload size — that is the figure that ultimately drives parser memory
    /// usage. Defaults to 16 `MiB`.
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,
}

impl OtlpHttpConfig {
    /// Validate OTLP HTTP configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid
    pub fn validate(&self) -> Result<()> {
        if self.enabled && self.host.trim().is_empty() {
            return Err(IngestError::Config("OTLP HTTP host cannot be empty".into()));
        }
        if self.enabled && self.max_body_bytes == 0 {
            return Err(IngestError::Config(
                "OTLP HTTP max_body_bytes must be greater than zero".into(),
            ));
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
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
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
