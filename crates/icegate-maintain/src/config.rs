//! Maintain binary configuration
//!
//! Root configuration for the maintain binary, containing catalog and storage
//! configurations needed for maintenance operations.

use std::path::Path;

use icegate_common::{CatalogConfig, MetricsConfig, StorageConfig, TracingConfig};
use serde::{Deserialize, Serialize};

use crate::compact::config::CompactionConfig;
use crate::error::MaintainError;
use crate::gc::config::GcConfig;
use crate::migrate::config::SnapshotExpirationConfig;

/// Maintain binary configuration
///
/// Root configuration struct for the maintain binary. Contains catalog and
/// storage configuration needed for maintenance operations like migrations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MaintainConfig {
    /// Iceberg catalog configuration
    pub catalog: CatalogConfig,
    /// Storage backend configuration
    pub storage: StorageConfig,
    /// Parquet compaction configuration
    ///
    /// Defaulted when absent from the config file so existing migrate configs
    /// (which have no `compaction` key) continue to load unchanged.
    #[serde(default)]
    pub compaction: CompactionConfig,
    /// Orphan-file garbage-collection configuration for the long-running `run`
    /// service. Disabled by default; ignored by the one-shot `migrate` commands.
    #[serde(default)]
    pub gc: GcConfig,
    /// LLM pricing crawler configuration for the long-running `run` service.
    /// Disabled by default; ignored by the one-shot `migrate` commands.
    #[serde(default)]
    pub pricing: crate::pricing::config::PricingConfig,
    /// Snapshot-retention policy `migrate create` stamps onto the tables it
    /// creates. Read only by the one-shot `migrate` commands: expiration itself
    /// runs on every writer's commit, off the table's own properties.
    #[serde(default)]
    pub snapshot_expiration: SnapshotExpirationConfig,
    /// Prometheus metrics endpoint configuration for the long-running `run`
    /// service. Disabled by default and ignored by the one-shot `migrate`
    /// commands; when enabled, `run` installs the global meter provider (so the
    /// compactor's `CompactMetrics` record) and serves `/metrics`.
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// `OpenTelemetry` tracing for the long-running `run` service, which
    /// initialises the subscriber from this block once the config is loaded. The
    /// one-shot `migrate` commands never reach it: they install a plain JSON
    /// logger in `main` before any config file is read.
    #[serde(default)]
    pub tracing: TracingConfig,
}

impl MaintainConfig {
    /// Load configuration from a file (TOML or YAML)
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let config: Self = icegate_common::load_config_file(path.as_ref())?;
        config.validate()?;
        Ok(config)
    }

    /// Validate the always-required shared configuration: catalog, storage, and
    /// the (optional) metrics endpoint.
    ///
    /// The component-specific `compaction`, `gc`, `pricing`, and `tracing`
    /// blocks are deliberately NOT validated here. Each carries requirements the
    /// one-shot `migrate` commands never satisfy — job-state storage for the
    /// first three, an OTLP endpoint for `tracing`, whose default is enabled —
    /// and those commands share this `MaintainConfig` while using only the
    /// `catalog` and `storage` blocks. Each is instead validated where the `run`
    /// service consumes it (`Compactor::new` / `GcRunner::new` /
    /// `PricingRunner::new`, and the tracing init in the `run` command), so a
    /// `migrate` config that omits them still loads. (Validating `gc` here
    /// previously broke `migrate create` on the minimal migrate config.)
    ///
    /// `snapshot_expiration` is the exception: it is the `migrate` commands' own
    /// block, it requires nothing of the environment, and its values leave the
    /// process as table properties every later writer resolves — a bad window
    /// caught here is a failed `migrate`, the same window caught on a commit is
    /// a failed ingest.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog, storage, metrics, or snapshot-expiration
    /// configuration is invalid, or if the orphan sweep would run with no grace
    /// period (see [`Self::validate_orphan_grace_period`]).
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.catalog.validate()?;
        self.storage.validate()?;
        self.metrics.validate()?;
        self.snapshot_expiration.validate()?;
        self.validate_orphan_grace_period()?;
        Ok(())
    }

    /// Reject an orphan sweep that runs with no grace period.
    ///
    /// The deployment contract (`crates/icegate-maintain/README.md`) is
    /// `query.engine.max_age_secs < gc.orphans.min_age_secs`: the query engine
    /// caches a catalog provider — a fixed table state and the file list behind
    /// it — for `max_age_secs`, so a file must stay unreferenced longer than
    /// that before the sweep may take it.
    ///
    /// Maintain cannot check the full contract — `query.engine.max_age_secs`
    /// lives in another component's config and is not visible here. A zero grace
    /// period is the one case it can: `max_age_secs` is required to be positive
    /// (`icegate-query` `engine/config.rs`), so zero violates the ordering
    /// whatever that value is. The Helm chart checks the general case
    /// (`icegate.validateRetentionWindow`); Compose and hand-written configs
    /// have this and the `snapshot_expiration` bounds, nothing wider.
    ///
    /// The check does not read `snapshot_expiration`. That block says what
    /// `migrate create` stamps onto new tables, not whether this deployment's
    /// tables expire snapshots — an existing table keeps the policy it was
    /// created with, and the `run` service is configured from a file
    /// (`configmap-maintain.yaml`) that carries no `snapshot_expiration` block
    /// at all. Nor is expiration the only source of the hazard: a grace period
    /// of zero also lets the sweep take a compaction output written but not yet
    /// referenced by any manifest.
    ///
    /// # Errors
    ///
    /// Returns an error when `gc` and `gc.orphans` are both enabled and
    /// `gc.orphans.min_age_secs` is zero.
    fn validate_orphan_grace_period(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.gc.enabled && self.gc.orphans.enabled && self.gc.orphans.min_age_secs == 0 {
            return Err(Box::new(MaintainError::Config(
                "gc.orphans.min_age_secs must be greater than zero: a swept file has to stay \
                 unreferenced for longer than query.engine.max_age_secs (positive by definition) \
                 so a cached catalog provider cannot plan a read against a file already deleted, \
                 and the grace period is also what keeps a compaction output not yet referenced \
                 by any manifest out of the sweep's reach"
                    .to_string(),
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::MaintainConfig;

    /// A `migrate`-style config — catalog + storage only, with NO `gc` or
    /// `compaction` block — must validate. The one-shot `migrate` commands share
    /// `MaintainConfig` but never set the component-specific job-state storage,
    /// so validating those blocks here would (and once did) break `migrate`.
    #[test]
    fn migrate_style_config_without_gc_or_compaction_validates() {
        let yaml = r"
catalog:
  backend: !rest
    uri: http://nessie:19120/iceberg
  warehouse: s3://warehouse/
  properties:
    prefix: main
storage:
  backend: !s3
    bucket: warehouse
    region: us-east-1
    endpoint: http://localhost:9000
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse migrate-style config");
        config
            .validate()
            .expect("migrate-style config (no gc/compaction) must validate");
        // `TracingConfig` defaults to enabled, and its own validator rejects
        // "enabled with no endpoint". A migrate config carries no `tracing`
        // block and no OTLP endpoint, so `validate` must not reach that check —
        // only the `run` command does, once it is about to build the exporter.
        assert!(config.tracing.enabled, "the tracing default is enabled");
        assert_eq!(config.tracing.otlp_endpoint, None);
    }

    /// Serde drops unknown keys in silence, so a chart-rendered `tracing` block
    /// that does not match the struct would leave the service on defaults —
    /// exporting nowhere while the `ConfigMap` says otherwise. Parse the block in
    /// the shape `configmap-maintain.yaml` renders it, down to the scalar type:
    /// Helm renders the chart's `sampleRatio: 1.0` as the integer `1`, so this is
    /// also what proves an integer scalar reaches an `f64` field.
    #[test]
    fn the_tracing_block_the_chart_renders_lands_on_the_config() {
        let yaml = r#"
catalog:
  backend: !rest
    uri: http://nessie:19120/iceberg
  warehouse: s3://warehouse/
storage:
  backend: !s3
    bucket: warehouse
    region: us-east-1
    endpoint: http://localhost:9000
tracing:
  enabled: true
  otlp_endpoint: "http://jaeger:4317"
  sample_ratio: 1
"#;
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse chart-style tracing config");

        assert!(config.tracing.enabled);
        assert_eq!(config.tracing.otlp_endpoint.as_deref(), Some("http://jaeger:4317"));
        assert!((config.tracing.sample_ratio - 1.0).abs() < f64::EPSILON);
        config.tracing.validate().expect("a configured endpoint validates");
    }

    /// The chart's other branch: with `maintain.tracing.enabled=false` the
    /// `ConfigMap` renders no `otlp_endpoint` at all (the `required` guard sits
    /// inside the enabled branch). That shape must load AND validate, since
    /// `run` validates the block before building any exporter — the endpoint
    /// requirement only applies while tracing is enabled.
    #[test]
    fn the_disabled_tracing_block_the_chart_renders_validates_without_an_endpoint() {
        let yaml = r"
catalog:
  backend: !rest
    uri: http://nessie:19120/iceberg
  warehouse: s3://warehouse/
storage:
  backend: !s3
    bucket: warehouse
    region: us-east-1
    endpoint: http://localhost:9000
tracing:
  enabled: false
  sample_ratio: 1
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse chart-style disabled tracing config");

        assert!(!config.tracing.enabled);
        assert_eq!(config.tracing.otlp_endpoint, None);
        config
            .tracing
            .validate()
            .expect("disabled tracing needs no endpoint to validate");
    }

    /// A fractional ratio reaches the config only when an operator overrides the
    /// chart's default `1.0`, which Helm renders as the integer `1` — so the
    /// fractional scalar is a path of its own, covered here rather than in the
    /// chart-shape test above.
    #[test]
    fn fractional_sample_ratio_parses() {
        let yaml = r"
catalog:
  backend: !rest
    uri: http://nessie:19120/iceberg
  warehouse: s3://warehouse/
storage:
  backend: !s3
    bucket: warehouse
    region: us-east-1
    endpoint: http://localhost:9000
tracing:
  enabled: true
  otlp_endpoint: http://jaeger:4317
  sample_ratio: 0.5
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse fractional sample_ratio");

        assert!((config.tracing.sample_ratio - 0.5).abs() < f64::EPSILON);
        config.tracing.validate().expect("0.5 is within [0.0, 1.0]");
    }

    /// The `snapshot_expiration` block in the shape `configmap-migrate.yaml`
    /// renders it. Serde drops unknown keys in silence, so a renamed field would
    /// leave `migrate create` stamping the built-in defaults onto every table
    /// while the `ConfigMap` claims otherwise — and the window a table is created
    /// with is the window it keeps.
    #[test]
    fn the_snapshot_expiration_block_the_chart_renders_lands_on_the_config() {
        let yaml = r"
catalog:
  backend: !rest
    uri: http://nessie:19120/iceberg
  warehouse: s3://warehouse/
storage:
  backend: !s3
    bucket: warehouse
    region: us-east-1
    endpoint: http://localhost:9000
snapshot_expiration:
  enabled: true
  min_snapshots_to_keep: 10
  max_snapshot_age_ms: 600000
  metadata_previous_versions_max: 20
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse chart-style expiration config");

        assert!(config.snapshot_expiration.enabled);
        assert_eq!(config.snapshot_expiration.min_snapshots_to_keep, 10);
        assert_eq!(config.snapshot_expiration.max_snapshot_age_ms, 600_000);
        assert_eq!(config.snapshot_expiration.metadata_previous_versions_max, 20);
        config.validate().expect("the chart-rendered block must validate");
    }

    /// A window that cannot be satisfied must fail at load, where it is one
    /// failed `migrate`, rather than reaching a table property and failing every
    /// subsequent commit by every writer.
    #[test]
    fn a_config_with_an_impossible_retention_window_fails_to_validate() {
        let yaml = r"
catalog:
  backend: !rest
    uri: http://nessie:19120/iceberg
  warehouse: s3://warehouse/
storage:
  backend: !s3
    bucket: warehouse
    region: us-east-1
    endpoint: http://localhost:9000
snapshot_expiration:
  min_snapshots_to_keep: 0
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse config with a zero window");

        assert!(config.validate().is_err());
    }

    /// A config whose sweep runs with no grace period. `enabled` is spelled out
    /// in every block the check reads, so a default flipping later cannot make
    /// the case pass by accident.
    const ZERO_ORPHAN_GRACE_PERIOD: &str = r"
catalog:
  backend: !rest
    uri: http://nessie:19120/iceberg
  warehouse: s3://warehouse/
storage:
  backend: !s3
    bucket: warehouse
    region: us-east-1
    endpoint: http://localhost:9000
snapshot_expiration:
  enabled: true
gc:
  enabled: true
  orphans:
    enabled: true
    min_age_secs: 0
";

    /// Sweeping with no grace period deletes files a cached query provider may
    /// still plan against, and `query.engine.max_age_secs` — the width of that
    /// cache — is required to be positive, so the ordering is violated for any
    /// value of it. The chart rejects this too; a Compose or hand-written config
    /// reaches only here.
    #[test]
    fn a_zero_orphan_grace_period_is_rejected() {
        let config: MaintainConfig =
            serde_yaml::from_str(ZERO_ORPHAN_GRACE_PERIOD).expect("parse config with a zero grace period");

        assert!(
            config.validate().is_err(),
            "a zero grace period must be rejected while the sweep is enabled"
        );
    }

    /// The same zero grace period with expiration stamped off. The verdict must
    /// not move: `snapshot_expiration` describes what `migrate create` writes
    /// onto new tables, not the policy the tables of this deployment carry, and
    /// the `run` service's own config file has no such block to read.
    #[test]
    fn a_zero_orphan_grace_period_is_rejected_with_expiration_disabled() {
        let yaml = ZERO_ORPHAN_GRACE_PERIOD.replace(
            "snapshot_expiration:\n  enabled: true",
            "snapshot_expiration:\n  enabled: false",
        );
        let config: MaintainConfig = serde_yaml::from_str(&yaml).expect("parse config with expiration disabled");

        assert!(!config.snapshot_expiration.enabled, "the replacement must have landed");
        assert!(
            config.validate().is_err(),
            "the grace-period contract does not depend on what migrate stamps onto new tables"
        );
    }

    /// The `run` service is configured from `configmap-maintain.yaml`, which
    /// renders `gc` but no `snapshot_expiration` block — the shape that made the
    /// earlier conditional check unreachable in Kubernetes. A positive grace
    /// period is what that config needs to validate.
    #[test]
    fn a_positive_orphan_grace_period_validates_without_an_expiration_block() {
        let yaml = ZERO_ORPHAN_GRACE_PERIOD
            .replace("snapshot_expiration:\n  enabled: true\n", "")
            .replace("min_age_secs: 0", "min_age_secs: 604800");
        let config: MaintainConfig = serde_yaml::from_str(&yaml).expect("parse chart-style maintain config");

        config.validate().expect("the sweep validates on a positive grace period");
    }

    /// Serde ignores unknown keys, so a `ConfigMap` key with no matching field
    /// is dropped in silence rather than rejected — the chart shipped
    /// `pricing.billing_region` that way. This parses the block as the chart
    /// renders it, so a field that goes missing again fails here.
    #[test]
    fn the_pricing_block_the_chart_renders_lands_on_the_config() {
        let yaml = r"
catalog:
  backend: !rest
    uri: http://nessie:19120/iceberg
  warehouse: s3://warehouse/
storage:
  backend: !s3
    bucket: warehouse
    region: us-east-1
    endpoint: http://localhost:9000
pricing:
  enabled: false
  interval_secs: 21600
  timeout_secs: 60
  crawl_timeout_secs: 600
  max_change_ratio: 10
  min_model_count_ratio: 0.8
  max_response_bytes: 134217728
  billing_region:
    aws.bedrock: us-east-1
  sources:
    - name: openrouter
      url: https://openrouter.ai/api/v1/models
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse chart-style pricing config");
        assert_eq!(config.pricing.crawl_timeout_secs, 600);
        assert_eq!(config.pricing.billing_region_for("aws.bedrock"), "us-east-1");
        assert_eq!(
            config.pricing.billing_region_for("anthropic"),
            crate::pricing::config::GLOBAL_REGION
        );
    }
}
