//! Configuration for the orphan-file garbage collector.

use serde::{Deserialize, Serialize};

use crate::compact::config::{CompactionJobsManagerConfig, JobsStorageConfig};
use crate::error::MaintainError;

/// Tunables for the orphan-file sweep (the only GC phase in this build).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GcOrphansConfig {
    /// Whether the orphan sweep runs. When `false` the per-table GC task is a
    /// no-op (it still completes each cycle).
    pub enabled: bool,
    /// When `true`, compute and emit metrics for orphans but delete nothing.
    pub dry_run: bool,
    /// Grace period in seconds: an object is eligible for deletion only if its
    /// `last_modified` is older than this. Protects freshly written files and
    /// in-flight compaction outputs not yet referenced by a manifest. `0` means
    /// no grace period.
    pub min_age_secs: u64,
    /// When `true`, also sweep orphaned Iceberg metadata files (manifests,
    /// manifest lists, superseded `*.metadata.json`); when `false`, only data
    /// files under `data/` are swept.
    pub include_metadata: bool,
    /// Maximum number of concurrent best-effort object deletions.
    pub delete_concurrency: usize,
    /// Per-table sweep task deadline, in seconds.
    pub sweep_timeout_secs: u64,
}

impl Default for GcOrphansConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dry_run: false,
            min_age_secs: 604_800,
            include_metadata: true,
            delete_concurrency: 16,
            sweep_timeout_secs: 3_600,
        }
    }
}

impl GcOrphansConfig {
    /// Validate the orphan-sweep tunables.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if `delete_concurrency` or
    /// `sweep_timeout_secs` is zero. `min_age_secs` of `0` is allowed (no grace).
    pub fn validate(&self) -> Result<(), MaintainError> {
        if self.delete_concurrency == 0 {
            return Err(MaintainError::Config(
                "gc.orphans.delete_concurrency must be greater than zero".to_string(),
            ));
        }
        if self.sweep_timeout_secs == 0 {
            return Err(MaintainError::Config(
                "gc.orphans.sweep_timeout_secs must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Configuration for the background orphan-file garbage collector.
///
/// Reuses [`CompactionJobsManagerConfig`] for the worker pool and job-state
/// storage so operators configure both background loops the same way. The
/// `Default` here overrides the job-state `prefix` to `"gc"` and the discovery
/// cadence to daily so GC never collides with compaction's job state.
// The five `*_enabled` flags are independent per-table toggles, mirroring
// `CompactionConfig`.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GcConfig {
    /// Master switch: when `false`, the `run` service does not start the GC
    /// runner at all (so an upgrade never silently begins deleting).
    pub enabled: bool,
    /// Whether GC is enabled for the `logs` table.
    pub logs_enabled: bool,
    /// Whether GC is enabled for the `spans` table.
    pub spans_enabled: bool,
    /// Whether GC is enabled for the `events` table.
    pub events_enabled: bool,
    /// Whether GC is enabled for the `metrics` table.
    pub metrics_enabled: bool,
    /// Whether GC is enabled for the `operations` table.
    pub operations_enabled: bool,
    /// Orphan-sweep tunables.
    pub orphans: GcOrphansConfig,
    /// Worker pool, discovery interval, and job-state storage. Reused from
    /// compaction; the `Default` below sets a distinct `"gc"` storage prefix and
    /// a daily `scan_interval_secs`.
    pub jobsmanager: CompactionJobsManagerConfig,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            logs_enabled: true,
            spans_enabled: true,
            events_enabled: true,
            metrics_enabled: true,
            operations_enabled: true,
            orphans: GcOrphansConfig::default(),
            jobsmanager: CompactionJobsManagerConfig {
                scan_interval_secs: 86_400,
                storage: JobsStorageConfig {
                    prefix: "gc".to_string(),
                    ..JobsStorageConfig::default()
                },
                ..CompactionJobsManagerConfig::default()
            },
        }
    }
}

impl GcConfig {
    /// Validate the orphan-sweep tunables and the job-state storage config.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if the orphan tunables or the
    /// [`CompactionJobsManagerConfig`] are invalid. (Job-state storage errors
    /// are reported with a `compaction.jobsmanager.storage.*` field path because
    /// the validator is shared; the values come from the `[gc.jobsmanager]`
    /// block.)
    pub fn validate(&self) -> Result<(), MaintainError> {
        self.orphans.validate()?;
        self.jobsmanager.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::GcConfig;

    /// A `GcConfig` whose jobs-storage is populated enough to pass validation.
    fn valid_config() -> GcConfig {
        let mut config = GcConfig::default();
        config.jobsmanager.storage.endpoint = "http://localhost:9000".to_string();
        config.jobsmanager.storage.bucket = "warehouse".to_string();
        config
    }

    #[test]
    fn defaults_are_off_with_conservative_grace() {
        let config = GcConfig::default();
        assert!(!config.enabled);
        assert!(config.logs_enabled);
        assert!(config.operations_enabled);
        assert!(config.orphans.enabled);
        assert!(!config.orphans.dry_run);
        assert_eq!(config.orphans.min_age_secs, 604_800);
        assert!(config.orphans.include_metadata);
        assert_eq!(config.orphans.delete_concurrency, 16);
        assert_eq!(config.jobsmanager.scan_interval_secs, 86_400);
        assert_eq!(config.jobsmanager.storage.prefix, "gc");
    }

    #[test]
    fn validate_accepts_a_populated_config() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn validate_rejects_zero_delete_concurrency() {
        let mut config = valid_config();
        config.orphans.delete_concurrency = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_zero_sweep_timeout() {
        let mut config = valid_config();
        config.orphans.sweep_timeout_secs = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_jobs_storage() {
        // Default storage has an empty endpoint/bucket.
        assert!(GcConfig::default().validate().is_err());
    }

    #[test]
    fn validate_allows_zero_min_age() {
        // `min_age_secs == 0` is the documented "no grace period" setting.
        let mut config = valid_config();
        config.orphans.min_age_secs = 0;
        assert!(config.validate().is_ok());
    }
}
