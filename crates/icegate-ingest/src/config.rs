//! Ingest binary configuration
//!
//! Root configuration for the ingest binary, containing catalog, storage,
//! and all OTLP receiver configurations (HTTP, gRPC).

use std::path::Path;

use icegate_common::{
    CatalogConfig, MemoryPressureConfig, OperationalConfig, StorageConfig, TenantPolicy, TracingConfig,
    check_port_conflicts, load_config_file,
};
use icegate_queue::QueueConfig;
use serde::{Deserialize, Serialize};

use super::{otlp_grpc::OtlpGrpcConfig, otlp_http::OtlpHttpConfig};
use crate::error::Result;
use crate::shift::ShiftConfig;

/// Ingest binary configuration
///
/// Root configuration struct for the ingest binary. Contains catalog and
/// storage configuration shared across all ingest servers, plus individual
/// server configs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestConfig {
    /// Iceberg catalog configuration
    pub catalog: CatalogConfig,
    /// Storage backend configuration
    pub storage: StorageConfig,
    /// Queue configuration for WAL-based ingestion
    #[serde(default)]
    pub queue: Option<QueueConfig>,
    /// Shift configuration for moving WAL to Iceberg
    #[serde(default)]
    pub shift: ShiftConfig,
    /// How the tenant of an incoming request is decided.
    #[serde(default)]
    pub tenant: TenantPolicy,
    /// OTLP HTTP server
    pub otlp_http: OtlpHttpConfig,
    /// OTLP gRPC server
    pub otlp_grpc: OtlpGrpcConfig,
    /// Operations (LLM observability) materialization
    #[serde(default)]
    pub operations: OperationsConfig,
    /// Metrics configuration
    #[serde(default)]
    pub metrics: OperationalConfig,
    /// Tracing configuration
    #[serde(default)]
    pub tracing: TracingConfig,
    /// Memory-pressure request-shedding guard. Inert (never sheds) when no finite
    /// cgroup memory limit is detected, so leaving it enabled is safe in dev/CI.
    #[serde(default)]
    pub memory_pressure: MemoryPressureConfig,
}

/// Operations (LLM observability) materialization configuration.
///
/// `operations` is a best-effort typed projection of LLM/GenAI trace spans
/// (TRI-72), forked from the traces ingest path. Disabling it skips the
/// per-request operations transform on the traces hot path entirely — no second
/// pass over the spans and no per-span `AttributeView` allocation — so
/// deployments that do not query LLM observability pay none of its cost.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationsConfig {
    /// Whether to materialize the `operations` table from trace spans. Defaults
    /// to `true`, preserving the always-on behaviour when the section is absent.
    #[serde(default = "default_operations_enabled")]
    pub enabled: bool,
}

/// Default for [`OperationsConfig::enabled`]: operations materialization is on
/// unless a deployment opts out.
const fn default_operations_enabled() -> bool {
    true
}

impl Default for OperationsConfig {
    fn default() -> Self {
        Self {
            enabled: default_operations_enabled(),
        }
    }
}

impl IngestConfig {
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
        self.tenant.validate()?;
        self.otlp_http.validate()?;
        self.otlp_grpc.validate()?;
        self.shift.validate()?;
        self.metrics.validate()?;
        self.tracing.validate()?;
        self.memory_pressure.validate()?;
        if let Some(queue) = &self.queue {
            queue.validate()?;
        }

        // Check for port conflicts among enabled servers
        check_port_conflicts(&[&self.otlp_http, &self.otlp_grpc, &self.metrics])?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use icegate_common::TenantPolicy;

    use super::IngestConfig;
    use crate::error::IngestError;

    /// A default config with the two blocks that do not validate as-is filled
    /// in: tracing (enabled with no OTLP endpoint) and the shift job storage.
    fn valid_config() -> IngestConfig {
        let mut config = IngestConfig::default();
        config.tracing.enabled = false;
        config.shift.jobsmanager.storage.endpoint = "http://localhost:9000".to_string();
        config.shift.jobsmanager.storage.bucket = "warehouse".to_string();
        config
    }

    /// A `memory_pressure` validation failure must propagate out of
    /// `IngestConfig::validate`, proving the field is wired into the aggregate
    /// validator rather than merely deserialized.
    #[test]
    fn validate_rejects_invalid_memory_pressure() {
        let mut config = valid_config();

        // Baseline validates: any error after flipping only `memory_pressure`
        // therefore originates in its validator.
        config.validate().expect("baseline ingest config is valid");

        // Violates `0 < low < high <= 1` (low >= high).
        config.memory_pressure.low_watermark = 0.95;
        config.memory_pressure.high_watermark = 0.90;

        assert!(matches!(config.validate(), Err(IngestError::Config(_))));
    }

    /// A `tenant` validation failure must propagate out of
    /// `IngestConfig::validate`, proving the policy is wired into the aggregate
    /// validator rather than merely deserialized.
    #[test]
    fn validate_rejects_an_invalid_single_tenant_id() {
        let mut config = valid_config();
        config.validate().expect("baseline ingest config is valid");

        config.tenant = TenantPolicy::Single {
            id: "bad/tenant".to_string(),
        };

        assert!(matches!(config.validate(), Err(IngestError::Config(_))));
    }

    /// The operational listener binds its port whatever `metrics.enabled` says,
    /// so `check_port_conflicts` must see that port even with the Prometheus
    /// endpoint off — otherwise the clash surfaces as a failed bind after the
    /// receivers have already started.
    #[test]
    fn validate_rejects_a_disabled_metrics_port_taken_by_otlp_http() {
        let mut config = valid_config();
        config.validate().expect("baseline ingest config is valid");

        config.metrics.enabled = false;
        config.metrics.port = config.otlp_http.port;

        assert!(matches!(config.validate(), Err(IngestError::Config(_))));
    }

    /// A config with no `tenant` section keeps writing to the tenant it wrote to
    /// before the policy existed. The identifier is spelled out rather than read
    /// from `DEFAULT_TENANT_ID`: it is the value already in the deployed tables,
    /// so changing the constant must fail here.
    #[test]
    fn default_config_serves_the_default_tenant() {
        assert_eq!(
            IngestConfig::default().tenant,
            TenantPolicy::Single {
                id: "default".to_string()
            }
        );
    }
}
