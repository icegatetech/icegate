//! Maintain binary configuration
//!
//! Root configuration for the maintain binary, containing catalog and storage
//! configurations needed for maintenance operations.

use std::path::Path;

use icegate_common::{CatalogConfig, MetricsConfig, StorageConfig, TracingConfig};
use icegate_queue::QueueConfig;
use serde::{Deserialize, Serialize};

use crate::compact::config::CompactionConfig;
use crate::error::MaintainError;
use crate::gc::config::GcConfig;
use crate::migrate::config::SnapshotExpirationConfig;
use crate::wal_cleanup::config::WalCleanupConfig;

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
    /// WAL segment cleanup for the long-running `run` service. Runs on its own
    /// worker pool ([`crate::wal_cleanup::runner`]). Disabled by default; ignored by the
    /// one-shot `migrate` commands.
    ///
    /// The config key is the field name, `wal_cleanup`. Serde drops an unknown
    /// key in silence, so a config file that spells it otherwise leaves cleanup
    /// on its built-in defaults — switched off — while the file says otherwise;
    /// the chart and Compose render this exact key, and a test parses the block
    /// in the shape the chart produces.
    #[serde(default)]
    pub wal_cleanup: WalCleanupConfig,
    /// WAL queue location. Cleanup addresses the queue through it, so it must
    /// name the same `base_path` the ingest service writes to — a different
    /// bucket than the tables live in, resolved through the same storage
    /// credentials.
    #[serde(default)]
    pub queue: QueueConfig,
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

    /// Validate everything that can be judged from the config file alone:
    /// catalog, storage, the (optional) metrics endpoint, snapshot expiration,
    /// the orphan grace period, and the WAL-cleanup tunables together with the
    /// queue location they address.
    ///
    /// What is NOT validated here is a service's job-state storage — the
    /// `jobsmanager` block of `compaction`, `gc`, `pricing` and `wal_cleanup`
    /// alike.
    /// The one-shot `migrate` commands share this `MaintainConfig` and carry
    /// none of those blocks, so their defaults hold an empty endpoint and
    /// bucket; checking them here would fail a command that never starts the
    /// service they configure. (Validating `gc` here once did exactly that to
    /// `migrate create`.) Each runner validates its own block instead, at the
    /// point it builds the pool: `Compactor::new`, `GcRunner::new`,
    /// `PricingRunner::new`, `WalCleanupRunner::new`. `tracing` is left out for
    /// the same shape of reason — its default is enabled and it then requires an
    /// OTLP endpoint a `migrate` config has no reason to carry — and is
    /// validated by the `run` command before the exporter is built.
    ///
    /// `snapshot_expiration` is validated here because it is the `migrate`
    /// commands' own block: it requires nothing of the environment, and its
    /// values leave the process as table properties every later writer resolves
    /// — a bad window caught here is a failed `migrate`, the same window caught
    /// on a commit is a failed ingest. The WAL block qualifies on the same
    /// terms: its tunables are pure numbers, and cleanup stays off in a
    /// `migrate` config, which makes both checks below no-ops there.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog, storage, metrics, snapshot-expiration or
    /// WAL-cleanup configuration is invalid, if the orphan sweep would run with
    /// no grace period (see [`Self::validate_orphan_grace_period`]), or if
    /// cleanup is enabled against a queue location it cannot address (see
    /// [`Self::validate_wal_queue_location`]).
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.catalog.validate()?;
        self.storage.validate()?;
        self.metrics.validate()?;
        self.snapshot_expiration.validate()?;
        self.validate_orphan_grace_period()?;
        self.wal_cleanup.validate()?;
        self.validate_wal_queue_location()?;
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

    /// Reject a queue location WAL cleanup cannot address.
    ///
    /// Bound to `wal_cleanup.enabled`, which is what lets [`Self::validate`] run it on
    /// every config load: the one-shot `migrate` commands carry no `queue` block
    /// at all, and they leave cleanup disabled, so the check returns before it
    /// reads one.
    ///
    /// The check is what turns a missing or misspelled `queue.common.base_path`
    /// into a failed startup. `OperatorRegistry::resolve_object_store` answers an
    /// unrecognised path with a fresh in-memory store, and cleanup against that
    /// store finds no segment ever, so the queue grows without a single error in
    /// the log — only a `wal_cleanup.segments.found` pinned at zero.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] when cleanup is enabled and the queue
    /// block is invalid or names no persistent object store.
    fn validate_wal_queue_location(&self) -> Result<(), MaintainError> {
        if !self.wal_cleanup.enabled {
            return Ok(());
        }
        let base_path = &self.queue.common.base_path;
        self.queue.validate().map_err(|e| {
            MaintainError::Config(format!(
                "wal cleanup is enabled, so the queue block must be the one ingest writes to: {e}"
            ))
        })?;
        if !icegate_common::is_persistent_base_path(base_path) {
            return Err(MaintainError::Config(format!(
                "queue.common.base_path {base_path:?} names no object store: a path without an \
                 s3://, s3a://, file:// or / prefix resolves to process memory, where wal cleanup \
                 would report zero segments found on every cycle while the queue keeps growing"
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

    /// The `wal_cleanup` and `queue` blocks in the shape `configmap-maintain.yaml`
    /// renders them. Serde drops unknown keys in silence, so a renamed field
    /// would leave cleanup running on built-in defaults while the `ConfigMap`
    /// claims otherwise — and for a destructive loop that means deleting on a
    /// retention window nobody configured.
    #[test]
    fn the_wal_cleanup_block_the_chart_renders_lands_on_the_config() {
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
queue:
  common:
    base_path: s3://queue/
wal_cleanup:
  enabled: true
  dry_run: false
  logs_enabled: true
  spans_enabled: true
  metrics_enabled: true
  operations_enabled: false
  keep_segments_count: 2000
  max_deletes_per_cycle: 1000
  delete_concurrency: 8
  cleanup_timeout_secs: 120
  jobsmanager:
    scan_interval_secs: 300
    worker_count: 2
    poll_interval_ms: 1000
    storage:
      endpoint: http://localhost:9000
      bucket: jobs
      prefix: wal_cleanup
      region: us-east-1
      job_state_codec: json
      request_timeout_secs: 5
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse chart-style wal_cleanup config");

        assert!(config.wal_cleanup.enabled);
        assert!(!config.wal_cleanup.operations_enabled);
        assert_eq!(config.wal_cleanup.keep_segments_count, 2_000);
        assert_eq!(config.wal_cleanup.max_deletes_per_cycle, 1_000);
        assert_eq!(config.wal_cleanup.delete_concurrency, 8);
        assert_eq!(config.wal_cleanup.cleanup_timeout_secs, 120);
        assert_eq!(config.wal_cleanup.jobsmanager.scan_interval_secs, 300);
        assert_eq!(config.wal_cleanup.jobsmanager.worker_count, 2);
        assert_eq!(config.wal_cleanup.jobsmanager.storage.prefix, "wal_cleanup");
        assert_eq!(config.wal_cleanup.jobsmanager.storage.bucket, "jobs");
        assert_eq!(config.queue.common.base_path, "s3://queue/");
        config.validate().expect("the chart-rendered block must validate");
        // The runner's own gate, which `validate` deliberately leaves alone: the
        // chart renders a populated job-state storage, so it passes here too.
        config
            .wal_cleanup
            .jobsmanager
            .validate("wal_cleanup")
            .expect("the chart-rendered job-state storage must validate");
    }

    /// A config with neither block still loads: the one-shot `migrate` commands
    /// share this struct and carry no `wal_cleanup`/`queue` keys, and their
    /// defaults leave cleanup switched off.
    #[test]
    fn a_config_without_a_wal_cleanup_block_leaves_cleanup_disabled() {
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
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse config without a wal_cleanup block");

        assert!(!config.wal_cleanup.enabled);
        assert!(config.queue.common.base_path.is_empty());
        config
            .validate()
            .expect("a config without cleanup, and with no job-state storage, must load");
    }

    /// Cleanup enabled with no `queue` block. The empty `base_path` resolves to
    /// an in-memory store, so every cycle would report zero segments found while
    /// the real queue grows — the pod must refuse to start instead.
    #[test]
    fn enabled_cleanup_without_a_queue_block_is_rejected() {
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
wal_cleanup:
  enabled: true
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse cleanup without a queue block");

        let error = config
            .validate()
            .expect_err("enabled cleanup with no queue location must fail startup");
        assert!(
            error.to_string().contains("base_path"),
            "the failure must name the field to fix, got: {error}"
        );
    }

    /// The same refusal for a path that parses but names no store: `s3:/` with
    /// one slash is the typo the operator actually makes, and it is accepted by
    /// serde, by `validate`, and by the object-store resolution alike.
    #[test]
    fn enabled_cleanup_with_an_unrecognised_queue_scheme_is_rejected() {
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
queue:
  common:
    base_path: 's3:/queue/'
wal_cleanup:
  enabled: true
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse cleanup with a misspelled scheme");

        assert!(
            config.validate().is_err(),
            "a path resolving to process memory must fail startup"
        );
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
