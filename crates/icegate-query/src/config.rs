//! Query binary configuration
//!
//! Root configuration for the query binary, containing catalog, storage,
//! and all query API server configurations (Loki, Prometheus, Tempo).

use std::path::Path;

use icegate_common::{
    CatalogConfig, MemoryPressureConfig, MetricsConfig, StorageConfig, TracingConfig, check_port_conflicts,
    load_config_file,
};
use icegate_queue::QueueConfig;
use serde::{Deserialize, Serialize};

use super::{
    engine::QueryEngineConfig, flight_sql::FlightSqlConfig, loki::LokiConfig, prometheus::PrometheusConfig,
    tempo::TempoConfig,
};
use crate::error::Result;

/// Query binary configuration
///
/// Root configuration struct for the query binary. Contains catalog and storage
/// configuration shared across all query servers, plus individual server
/// configs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Iceberg catalog configuration
    pub catalog: CatalogConfig,
    /// Storage backend configuration
    pub storage: StorageConfig,
    /// Query engine configuration (shared across all APIs)
    #[serde(default)]
    pub engine: QueryEngineConfig,
    /// Queue configuration for WAL segment reading
    #[serde(default)]
    pub queue: QueueConfig,
    /// Loki API server
    pub loki: LokiConfig,
    /// Prometheus API server
    pub prometheus: PrometheusConfig,
    /// Tempo API server
    pub tempo: TempoConfig,
    /// Apache Arrow Flight SQL gRPC server.
    ///
    /// Optional: when the `flight_sql` block is absent the server defaults
    /// to disabled, so configs written before Flight SQL existed keep
    /// parsing instead of failing on a missing field.
    #[serde(default)]
    pub flight_sql: FlightSqlConfig,
    /// Tracing configuration
    #[serde(default)]
    pub tracing: TracingConfig,
    /// Prometheus metrics configuration
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Memory-pressure request-shedding guard configuration
    #[serde(default)]
    pub memory_pressure: MemoryPressureConfig,
}

impl QueryConfig {
    /// Load configuration from a file (TOML or YAML)
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let config: Self = load_config_file(path.as_ref())?;
        config.validate()?;
        Ok(config)
    }

    /// Validate all configurations
    ///
    /// # Errors
    ///
    /// Returns an error if any configuration is invalid
    pub fn validate(&self) -> Result<()> {
        self.catalog.validate()?;
        self.storage.validate()?;
        self.engine.validate()?;
        self.queue
            .validate()
            .map_err(|e| crate::error::QueryError::Config(e.to_string()))?;
        self.loki.validate()?;
        self.prometheus.validate()?;
        self.tempo.validate()?;
        self.flight_sql.validate()?;
        self.tracing.validate()?;
        self.metrics.validate()?;
        self.memory_pressure.validate()?;

        // Check for port conflicts among enabled servers
        check_port_conflicts(&[
            &self.loki,
            &self.prometheus,
            &self.tempo,
            &self.flight_sql,
            &self.metrics,
        ])?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::QueryError;

    /// An out-of-range watermark must surface through `QueryConfig::validate`
    /// as a `CommonError::Config` mapped to `QueryError::Config`. The `expect`
    /// on the unmutated config proves the failure is attributable solely to
    /// the `memory_pressure` block, not an unrelated default.
    #[test]
    fn invalid_memory_pressure_watermark_is_rejected() {
        let mut config = QueryConfig::default();
        // `QueryConfig::default()` is not valid as-is: `queue.common.base_path`
        // is empty and `tracing` defaults to enabled without an OTLP endpoint.
        // Satisfy both so the baseline validates and the only failure the
        // mutation below can introduce comes from the `memory_pressure` block.
        config.queue.common.base_path = "s3://warehouse/queue".to_string();
        config.tracing.enabled = false;
        config.validate().expect("baseline config should validate");
        // 0 < low < high <= 1 is violated: high > 1.0.
        config.memory_pressure.high_watermark = 1.5;
        assert!(matches!(config.validate(), Err(QueryError::Config(_))));
    }
}
