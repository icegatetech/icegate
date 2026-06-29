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
    /// The component-specific `compaction` and `gc` blocks are deliberately NOT
    /// validated here. Both carry required job-state storage that the one-shot
    /// `migrate` commands never set — those commands share this `MaintainConfig`
    /// but use only `catalog` + `storage`. Each block is instead validated when
    /// its background loop is constructed in the `run` service
    /// (`Compactor::new` / `GcRunner::new`), so a `migrate` config that omits the
    /// `gc`/`compaction` block still loads. (Validating `gc` here previously
    /// broke `migrate create` on the minimal migrate config.)
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
    endpoint: http://minio:9000
";
        let config: MaintainConfig = serde_yaml::from_str(yaml).expect("parse migrate-style config");
        config
            .validate()
            .expect("migrate-style config (no gc/compaction) must validate");
    }
}
