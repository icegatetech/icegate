//! Maintain binary configuration
//!
//! Root configuration for the maintain binary, containing catalog and storage
//! configurations needed for maintenance operations.

use std::path::Path;

use icegate_common::{CatalogConfig, MetricsConfig, StorageConfig};
use serde::{Deserialize, Serialize};

use crate::compact::config::CompactionConfig;
use crate::gc::config::GcConfig;

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
    /// Prometheus metrics endpoint configuration for the long-running `run`
    /// service. Disabled by default and ignored by the one-shot `migrate`
    /// commands; when enabled, `run` installs the global meter provider (so the
    /// compactor's `CompactMetrics` record) and serves `/metrics`.
    #[serde(default)]
    pub metrics: MetricsConfig,
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
    /// The component-specific `compaction`, `gc`, and `pricing` blocks are
    /// deliberately NOT validated here. Each carries required job-state storage
    /// that the one-shot `migrate` commands never set — those commands share
    /// this `MaintainConfig` but use only `catalog` + `storage`. Each block is
    /// instead validated when its background loop is constructed in the `run`
    /// service (`Compactor::new` / `GcRunner::new` / `PricingRunner::new`), so a
    /// `migrate` config that omits the `gc`/`compaction`/`pricing` block still
    /// loads. (Validating `gc` here previously broke `migrate create` on the
    /// minimal migrate config.)
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog, storage, or metrics configuration is
    /// invalid.
    pub fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.catalog.validate()?;
        self.storage.validate()?;
        self.metrics.validate()?;
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
