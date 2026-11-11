//! Query binary configuration
//!
//! Root configuration for the query binary, containing catalog, storage,
//! and all query API server configurations (Loki, Prometheus, Tempo, Flight SQL, `OpenSearch`).

use std::path::Path;

use icegate_common::{
    CatalogConfig, MemoryPressureConfig, OperationalConfig, StorageConfig, TenantPolicy, TlsConfig, TracingConfig,
    check_port_conflicts, load_config_file,
};
use icegate_queue::QueueConfig;
use serde::{Deserialize, Serialize};

use super::{
    engine::QueryEngineConfig, flight_sql::FlightSqlConfig, loki::LokiConfig, opensearch::OpenSearchConfig,
    prometheus::PrometheusConfig, tempo::TempoConfig,
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
    /// How this deployment decides whose tenant a request reads.
    ///
    /// Optional: when the `tenant` block is absent the policy defaults to
    /// `single` on `default`, so configs written before the policy existed keep
    /// parsing. A request without the header, or naming `default`, is answered
    /// as before. Two answers changed: a request naming another tenant, once
    /// served that tenant's rows, and one with a malformed value, once served
    /// `default`'s rows, are now both refused. A deployment reading several
    /// tenants sets `tenant: !multi`.
    #[serde(default)]
    pub tenant: TenantPolicy,
    /// Server-side TLS for every listener of this process: Loki, Prometheus, Tempo, Flight SQL
    /// and the operational one. Absent means all of them serve plaintext.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
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
    /// `OpenSearch`-compatible HTTP API.
    ///
    /// Optional: when the `opensearch` block is absent the listener defaults to disabled, so
    /// configs written before it existed keep parsing.
    #[serde(default)]
    pub opensearch: OpenSearchConfig,
    /// Tracing configuration
    #[serde(default)]
    pub tracing: TracingConfig,
    /// Prometheus metrics configuration
    #[serde(default)]
    pub metrics: OperationalConfig,
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
    /// Returns an error if any configuration is invalid, including a document
    /// that enables no query protocol at all: the binary loads this document
    /// only to serve them.
    ///
    /// The Helm chart does not restate that last rule — `configmap-query.yaml`
    /// renders the five `enabled` flags as values state them, so a release with
    /// every protocol off installs and this is where it is refused. Deliberate:
    /// unlike `ingest.metrics.host`, whose misconfiguration the chart refuses at
    /// render time because it surfaces only as a probe failing for no stated
    /// reason, this one names itself — `main` prints the error to stderr and the
    /// process exits, so the pod reports the refusal rather than running ready
    /// and answering nothing.
    pub fn validate(&self) -> Result<()> {
        self.catalog.validate()?;
        self.storage.validate()?;
        self.engine.validate()?;
        self.queue
            .validate()
            .map_err(|e| crate::error::QueryError::Config(e.to_string()))?;
        self.tenant.validate()?;
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        self.loki.validate()?;
        self.prometheus.validate()?;
        self.tempo.validate()?;
        self.flight_sql.validate()?;
        self.opensearch.validate()?;
        self.tracing.validate()?;
        self.metrics.validate()?;
        self.memory_pressure.validate()?;

        // The five protocols are what the binary exists to serve, so a document
        // enabling none of them is refused rather than started. Nothing later
        // can catch it: the operational listener carrying `/health` runs
        // whatever `metrics` says, so such a deployment reports itself ready
        // while answering no query at all.
        if !(self.loki.enabled
            || self.prometheus.enabled
            || self.tempo.enabled
            || self.flight_sql.enabled
            || self.opensearch.enabled)
        {
            return Err(crate::error::QueryError::Config(
                "no query protocol is enabled: set at least one of loki.enabled, prometheus.enabled, \
                 tempo.enabled, flight_sql.enabled or opensearch.enabled"
                    .to_string(),
            ));
        }

        // Check for port conflicts among enabled servers
        check_port_conflicts(&[
            &self.loki,
            &self.prometheus,
            &self.tempo,
            &self.flight_sql,
            &self.opensearch,
            &self.metrics,
        ])?;

        // The listener's startup refuses table locations it lists through the catalog; only this
        // document knows the WAL base path, so that half of the overlap rule is checked here.
        if self.opensearch.enabled {
            let location = crate::opensearch::resolve_system_indices_location(self)?;
            crate::opensearch::refuse_overlapping_locations(&location, &[self.queue.common.base_path.as_str()])?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use icegate_common::DEFAULT_TLS_RELOAD_INTERVAL_SECS;

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

    /// A `tls` validation failure must propagate out of `QueryConfig::validate`, proving the
    /// block is wired into the aggregate validator rather than merely deserialized: without it
    /// the failure would surface only when the certificate is loaded at startup.
    #[test]
    fn validate_rejects_a_tls_block_with_an_empty_certificate_path() {
        let mut config = QueryConfig::default();
        config.queue.common.base_path = "s3://warehouse/queue".to_string();
        config.tracing.enabled = false;
        config.validate().expect("baseline config should validate");

        config.tls = Some(TlsConfig {
            certificate_path: PathBuf::new(),
            private_key_path: PathBuf::from("/tls/tls.key"),
            reload_interval_secs: DEFAULT_TLS_RELOAD_INTERVAL_SECS,
        });

        assert!(matches!(config.validate(), Err(QueryError::Config(_))));
    }

    /// A baseline this module's protocol cases mutate: valid, and with all five
    /// protocols off so each case states its own. `queue.common.base_path` and
    /// `tracing` are settled for the same reason as in the test above.
    fn build_config_without_protocols() -> QueryConfig {
        let mut config = QueryConfig::default();
        config.queue.common.base_path = "s3://warehouse/queue".to_string();
        config.tracing.enabled = false;
        // Spelled out because three of the five default to enabled.
        config.loki.enabled = false;
        config.prometheus.enabled = false;
        config.tempo.enabled = false;
        config.flight_sql.enabled = false;
        config.opensearch.enabled = false;
        config
    }

    /// Metrics stay enabled to pin the part that used to be wrong: the run
    /// command decided on the count of spawned server tasks, the operational
    /// listener carrying `/health` is now spawned whatever `metrics` says, and
    /// such a deployment therefore stayed up reporting itself ready while
    /// answering no query.
    #[test]
    fn a_configuration_enabling_no_query_protocol_is_rejected() {
        let mut config = build_config_without_protocols();
        config.metrics.enabled = true;

        assert!(matches!(config.validate(), Err(QueryError::Config(_))));
    }

    /// One enabled protocol is enough, and each of the five counts on its own:
    /// a protocol left out of the check would have its deployment refused at
    /// startup.
    #[test]
    fn each_protocol_alone_satisfies_validation() {
        let cases: [(&str, fn(&mut QueryConfig)); 5] = [
            ("loki", |config| config.loki.enabled = true),
            ("prometheus", |config| config.prometheus.enabled = true),
            ("tempo", |config| config.tempo.enabled = true),
            ("flight_sql", |config| config.flight_sql.enabled = true),
            ("opensearch", |config| {
                config.opensearch.enabled = true;
                config.opensearch.system_indices.location = Some("s3://warehouse/opensearch-system".to_string());
            }),
        ];

        for (protocol, enable) in cases {
            let mut config = build_config_without_protocols();
            enable(&mut config);

            config
                .validate()
                .unwrap_or_else(|err| panic!("{protocol} alone must satisfy validation: {err}"));
        }
    }

    /// Two enabled listeners on one port cannot both bind; the second protocol would fail at
    /// startup with an address-in-use error instead of naming the conflict. The baseline
    /// validates first, so the refusal is the shared port and nothing else.
    #[test]
    fn opensearch_on_another_protocols_port_is_rejected() {
        let mut config = build_config_without_protocols();
        config.loki.enabled = true;
        config.opensearch.enabled = true;
        // A resolvable location, so the port conflict is the only refusal left.
        config.opensearch.system_indices.location = Some("s3://warehouse/opensearch-system".to_string());
        config.validate().expect("opensearch on its own port validates");
        config.opensearch.port = config.loki.port;

        assert!(matches!(config.validate(), Err(QueryError::Config(_))));
    }

    /// The listener's store needs the conditional writes of an S3 location, so an enabled listener
    /// over memory or filesystem storage names its own location or the document is refused.
    #[test]
    fn opensearch_on_a_non_s3_storage_without_a_system_location_is_rejected() {
        let mut config = build_config_without_protocols();
        config.opensearch.enabled = true;
        config.opensearch.system_indices.location = Some("s3://warehouse/opensearch-system".to_string());
        config.validate().expect("memory storage with a configured location validates");
        config.opensearch.system_indices.location = None;

        assert!(matches!(config.validate(), Err(QueryError::Config(_))));
    }

    /// The WAL cleaner deletes what it finds under the queue's base path, so a system location
    /// inside it would lose snapshot objects; one containing it would list WAL segments.
    #[test]
    fn a_system_indices_location_overlapping_the_wal_base_path_is_rejected() {
        let mut config = build_config_without_protocols();
        config.opensearch.enabled = true;
        config.queue.common.base_path = "s3://queue/".to_string();
        config.storage.backend = icegate_common::StorageBackend::S3(icegate_common::S3Config {
            bucket: "warehouse".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        });
        config
            .validate()
            .expect("the default location s3://warehouse/opensearch-system is beside the WAL");

        for location in ["s3://queue", "s3://queue/opensearch-system", "s3a://queue/"] {
            config.opensearch.system_indices.location = Some(location.to_string());

            assert!(
                matches!(config.validate(), Err(QueryError::Config(message)) if message.contains("overlaps `s3://queue/`")),
                "{location} overlaps the WAL base path"
            );
        }
    }
}
