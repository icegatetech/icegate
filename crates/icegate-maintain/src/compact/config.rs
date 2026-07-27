//! Compaction configuration.
//!
//! Configuration for the long-running Parquet compaction service. The shape
//! mirrors ingest's shift configuration so operators get consistent knobs for
//! both background data-movement loops, but every field is optional in the
//! config file via `#[serde(default)]` so a minimal config still loads.

use std::time::Duration;

use jobmanager::{JobStateCodecKind, S3StorageConfig};
use serde::{Deserialize, Serialize};

use crate::error::MaintainError;

/// Job state serialization format.
///
/// Mirrors ingest's shift codec selection so the on-disk job-state encoding is
/// configured identically for both background loops.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStateCodec {
    /// JSON-encoded job state.
    #[default]
    Json,
    /// CBOR-encoded job state.
    Cbor,
}

impl From<JobStateCodec> for JobStateCodecKind {
    fn from(codec: JobStateCodec) -> Self {
        match codec {
            JobStateCodec::Json => Self::Json,
            JobStateCodec::Cbor => Self::Cbor,
        }
    }
}

/// Job storage configuration for compaction operations.
///
/// Maintain-local mirror of ingest's `shift::config::JobsStorageConfig`. It is
/// duplicated rather than imported because `icegate-maintain` depends only on
/// `icegate-common` and `jobmanager`; pulling in `icegate-ingest` solely for
/// this struct would couple maintenance to the ingest/WAL crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JobsStorageConfig {
    /// S3 endpoint URL.
    pub endpoint: String,
    /// Bucket name for job state.
    pub bucket: String,
    /// Prefix for job state objects.
    pub prefix: String,
    /// AWS region name.
    pub region: String,
    /// Whether to use HTTPS for the endpoint.
    pub use_ssl: bool,
    /// Job state serialization codec.
    pub job_state_codec: JobStateCodec,
    /// Request timeout for S3 operations, in seconds.
    pub request_timeout_secs: u64,
    /// Access key ID for S3 (falls back to env if not set).
    pub access_key_id: Option<String>,
    /// Secret access key for S3 (falls back to env if not set).
    pub secret_access_key: Option<String>,
}

impl Default for JobsStorageConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            bucket: String::new(),
            prefix: "compactor".to_string(),
            region: "us-east-1".to_string(),
            use_ssl: false,
            job_state_codec: JobStateCodec::default(),
            request_timeout_secs: 5,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

impl JobsStorageConfig {
    /// Validate job storage configuration values.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if any required field is empty or the
    /// request timeout is zero.
    pub fn validate(&self) -> Result<(), MaintainError> {
        if self.endpoint.trim().is_empty() {
            return Err(MaintainError::Config(
                "compaction.jobsmanager.storage.endpoint cannot be empty".to_string(),
            ));
        }
        if self.bucket.trim().is_empty() {
            return Err(MaintainError::Config(
                "compaction.jobsmanager.storage.bucket cannot be empty".to_string(),
            ));
        }
        if self.prefix.trim().is_empty() {
            return Err(MaintainError::Config(
                "compaction.jobsmanager.storage.prefix cannot be empty".to_string(),
            ));
        }
        if self.region.trim().is_empty() {
            return Err(MaintainError::Config(
                "compaction.jobsmanager.storage.region cannot be empty".to_string(),
            ));
        }
        if self.request_timeout_secs == 0 {
            return Err(MaintainError::Config(
                "compaction.jobsmanager.storage.request_timeout_secs must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    /// Convert to a jobmanager [`S3StorageConfig`].
    ///
    /// Credentials are taken from the explicit fields when set, otherwise from
    /// the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if credentials are configured as empty
    /// strings or cannot be resolved from the environment.
    pub fn to_s3_storage_config(&self) -> Result<S3StorageConfig, MaintainError> {
        let access_key_id = self.resolve_access_key_id()?;
        let secret_access_key = self.resolve_secret_access_key()?;

        Ok(S3StorageConfig {
            endpoint: self.endpoint.clone(),
            access_key_id,
            secret_access_key,
            bucket_name: self.bucket.clone(),
            use_ssl: self.use_ssl,
            region: self.region.clone(),
            bucket_prefix: self.prefix.clone(),
            job_state_codec: self.job_state_codec.into(),
            request_timeout: Duration::from_secs(self.request_timeout_secs),
            retrier_config: jobmanager::RetrierConfig::default(),
        })
    }

    fn resolve_access_key_id(&self) -> Result<String, MaintainError> {
        Self::resolve_credential(
            self.access_key_id.as_deref(),
            "AWS_ACCESS_KEY_ID",
            "compaction.jobsmanager.storage.access_key_id",
        )
    }

    fn resolve_secret_access_key(&self) -> Result<String, MaintainError> {
        Self::resolve_credential(
            self.secret_access_key.as_deref(),
            "AWS_SECRET_ACCESS_KEY",
            "compaction.jobsmanager.storage.secret_access_key",
        )
    }

    /// Resolve a credential from an explicit value or an environment variable.
    ///
    /// `explicit` is the optional value from the config file, `env_var` is the
    /// AWS environment variable to fall back to, and `field` names the config
    /// field for error messages.
    fn resolve_credential(explicit: Option<&str>, env_var: &str, field: &str) -> Result<String, MaintainError> {
        if let Some(value) = explicit {
            if value.trim().is_empty() {
                return Err(MaintainError::Config(format!("{field} cannot be empty")));
            }
            return Ok(value.to_string());
        }

        let value = std::env::var(env_var)
            .map_err(|_| MaintainError::Config(format!("{env_var} environment variable is not set")))?;
        if value.trim().is_empty() {
            return Err(MaintainError::Config(format!(
                "{env_var} environment variable is empty"
            )));
        }
        Ok(value)
    }
}

/// Default number of concurrent rewrite workers.
///
/// Mirrors ingest's `default_jobs_manager_worker_count`: half of the available
/// CPU parallelism (rounded up), leaving headroom for other work on the node.
fn default_worker_count() -> usize {
    std::thread::available_parallelism().map_or(1, |parallelism| parallelism.get().div_ceil(2))
}

/// Jobs-manager settings for the compaction service.
///
/// Mirrors ingest's `shift::config::ShiftJobsManagerConfig` so both background
/// loops expose operators the same
/// `jobsmanager.{worker_count, poll_interval_ms, scan_interval_secs, storage}`
/// shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompactionJobsManagerConfig {
    /// Number of concurrent rewrite tasks (jobmanager `JobsManagerConfig.worker_count`).
    pub worker_count: usize,
    /// Jobmanager worker poll interval, in milliseconds.
    pub poll_interval_ms: u64,
    /// Period of the discovery loop, in seconds (maps to the jobmanager
    /// iteration interval).
    pub scan_interval_secs: u64,
    /// Jobs-state storage (S3), the same shape ingest's shift uses.
    pub storage: JobsStorageConfig,
}

impl Default for CompactionJobsManagerConfig {
    fn default() -> Self {
        Self {
            worker_count: default_worker_count(),
            poll_interval_ms: 1_000,
            scan_interval_secs: 300,
            storage: JobsStorageConfig::default(),
        }
    }
}

impl CompactionJobsManagerConfig {
    /// Validate the jobs-manager tunables and the job-state storage.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if a tunable is zero or the
    /// [`JobsStorageConfig`] is invalid.
    pub fn validate(&self) -> Result<(), MaintainError> {
        if self.worker_count == 0 {
            return Err(MaintainError::Config(
                "compaction.jobsmanager.worker_count must be greater than zero".to_string(),
            ));
        }
        if self.poll_interval_ms == 0 {
            return Err(MaintainError::Config(
                "compaction.jobsmanager.poll_interval_ms must be greater than zero".to_string(),
            ));
        }
        if self.scan_interval_secs == 0 {
            return Err(MaintainError::Config(
                "compaction.jobsmanager.scan_interval_secs must be greater than zero".to_string(),
            ));
        }
        self.storage.validate()
    }
}

/// Data-compaction tunables: how small Parquet data files are discovered,
/// bin-packed into rewrite groups, and rewritten into fewer, larger files.
///
/// Maps to the `compaction.data` config section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DataCompactionConfig {
    /// Desired OUTPUT parquet file size, in bytes.
    ///
    /// Measured against `DataFile.file_size_in_bytes` when deciding whether a
    /// file is already at target.
    pub target_file_size_bytes: u64,
    /// Hard cap on a rewrite group's summed INPUT bytes (the bin-packing budget).
    pub max_group_input_bytes: u64,
    /// Skip a partition with at most this many files and no sub-target tail.
    pub min_input_files: usize,
    /// Tolerated number of sub-target files when deciding to skip a partition.
    pub max_skippable_tail_files: usize,
    /// Largest-to-smallest size ratio allowed within one rewrite group.
    ///
    /// A file is merged with a larger one only when it is at least
    /// `1 / max_merge_size_ratio` of the group's largest file, so a small file is
    /// not repeatedly re-read into a much larger one; smaller files are merged
    /// with each other instead. The gate only applies while the group's largest
    /// file is at or above [`Self::target_file_size_bytes`]. Must be at least 1;
    /// a value of 0 is rejected when the compactor is constructed.
    pub max_merge_size_ratio: u64,
    /// Deadline for a single REWRITE task (merge + encode + commit), in seconds.
    ///
    /// Kept separate from [`CompactionJobsManagerConfig::scan_interval_secs`] so a
    /// rewrite that legitimately runs longer than the discovery period is not
    /// declared expired — which would let another worker pick it up and duplicate
    /// the in-flight rewrite. Size it to a worst-case group: reading
    /// `max_group_input_bytes` of Parquet, k-way-merging, re-encoding, and
    /// committing.
    pub rewrite_timeout_secs: u64,
    /// Parquet row group size, in rows.
    pub row_group_size: usize,
    /// Maximum Parquet data page size, in bytes.
    pub data_page_size_limit_bytes: usize,
}

impl Default for DataCompactionConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 128 * 1024 * 1024,
            max_group_input_bytes: 256 * 1024 * 1024,
            min_input_files: 4,
            max_skippable_tail_files: 0,
            max_merge_size_ratio: 2,
            rewrite_timeout_secs: 3_600,
            row_group_size: 20_000,
            data_page_size_limit_bytes: 2 * 1024 * 1024,
        }
    }
}

impl DataCompactionConfig {
    /// Validate the data-compaction tunables.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if any tunable is out of range.
    pub fn validate(&self) -> Result<(), MaintainError> {
        if self.target_file_size_bytes == 0 {
            return Err(MaintainError::Config(
                "compaction.data.target_file_size_bytes must be greater than zero".to_string(),
            ));
        }
        // A group budget below the target file size can never bin-pack enough
        // input to produce a target-sized output, so compaction could never reach
        // its goal. This also rejects the `max_group_input_bytes = 0` case
        // (0 < any positive target), which would otherwise silently disable all
        // compaction.
        if self.max_group_input_bytes < self.target_file_size_bytes {
            return Err(MaintainError::Config(format!(
                "compaction.data.max_group_input_bytes ({}) must be at least target_file_size_bytes ({})",
                self.max_group_input_bytes, self.target_file_size_bytes
            )));
        }
        if self.max_merge_size_ratio == 0 {
            return Err(MaintainError::Config(
                "compaction.data.max_merge_size_ratio must be greater than or equal to 1".to_string(),
            ));
        }
        if self.rewrite_timeout_secs == 0 {
            return Err(MaintainError::Config(
                "compaction.data.rewrite_timeout_secs must be greater than zero".to_string(),
            ));
        }
        if self.row_group_size == 0 {
            return Err(MaintainError::Config(
                "compaction.data.row_group_size must be greater than zero".to_string(),
            ));
        }
        if self.data_page_size_limit_bytes == 0 {
            return Err(MaintainError::Config(
                "compaction.data.data_page_size_limit_bytes must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Manifest-compaction tunables: how the small DATA manifests of the current
/// snapshot are repacked into fewer, larger ones.
///
/// Maps to the `compaction.manifest` config section. The per-table toggles stay
/// in [`CompactionConfig`]: manifest compaction repacks the manifests of
/// whichever tables compaction is enabled for.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ManifestCompactionConfig {
    /// Target byte size of each OUTPUT manifest: entries are packed into an output
    /// manifest until its estimated manifest-byte size reaches this value. A
    /// packing target, NOT a trigger by itself (see [`Self::candidate_size_ratio`]).
    pub target_size_bytes: u64,
    /// Fraction of [`Self::target_size_bytes`] below which a manifest is a repack
    /// candidate; a manifest at or above `candidate_size_ratio * target` bytes is
    /// already large enough and is left alone. Must be in `(0.0, 1.0]`.
    pub candidate_size_ratio: f64,
    /// Upper bound on how many input manifests one `compact_manifest` commit
    /// repacks, keeping a single commit incremental; any remaining candidates are
    /// picked up on later iterations. Must be at least 2.
    pub max_manifests_per_commit: usize,
    /// Deadline for a single `compact_manifest` task (repack + commit), in seconds.
    pub rewrite_timeout_secs: u64,
}

impl Default for ManifestCompactionConfig {
    fn default() -> Self {
        Self {
            target_size_bytes: 8 * 1024 * 1024,
            candidate_size_ratio: 0.75,
            max_manifests_per_commit: 64,
            rewrite_timeout_secs: 600,
        }
    }
}

impl ManifestCompactionConfig {
    /// Validate the manifest-compaction tunables.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if any tunable is out of range.
    pub fn validate(&self) -> Result<(), MaintainError> {
        if self.target_size_bytes == 0 {
            return Err(MaintainError::Config(
                "compaction.manifest.target_size_bytes must be greater than zero".to_string(),
            ));
        }
        // `is_finite` rejects NaN/±inf (whose ordering comparisons below would
        // otherwise let a NaN slip through), leaving only a real ratio in
        // `(0.0, 1.0]`. A ratio outside that range makes candidate selection
        // degenerate (0 selects nothing; > 1 would admit already-large manifests).
        if !self.candidate_size_ratio.is_finite() || self.candidate_size_ratio <= 0.0 || self.candidate_size_ratio > 1.0
        {
            return Err(MaintainError::Config(format!(
                "compaction.manifest.candidate_size_ratio ({}) must be in (0.0, 1.0]",
                self.candidate_size_ratio
            )));
        }
        // A group of fewer than two manifests can never repack into fewer, so the
        // per-commit cap must admit at least two inputs.
        if self.max_manifests_per_commit < 2 {
            return Err(MaintainError::Config(
                "compaction.manifest.max_manifests_per_commit must be at least 2".to_string(),
            ));
        }
        if self.rewrite_timeout_secs == 0 {
            return Err(MaintainError::Config(
                "compaction.manifest.rewrite_timeout_secs must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// Configuration for the compaction process.
///
/// Holds what both compaction kinds share — the per-table toggles and the
/// jobs-manager settings — and nests the kind-specific tunables under
/// [`data`](Self::data) and [`manifest`](Self::manifest). A table's toggle
/// governs both kinds: manifest compaction repacks the manifests of the tables
/// data compaction runs on.
// The five `*_enabled` flags are independent per-table toggles (logs, spans,
// events, metrics, operations), not a hidden state machine; modelling them as
// enums would add noise without improving clarity.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompactionConfig {
    /// Whether compaction is enabled for the `logs` table.
    pub logs_enabled: bool,
    /// Whether compaction is enabled for the `spans` table.
    pub spans_enabled: bool,
    /// Whether compaction is enabled for the `events` table.
    pub events_enabled: bool,
    /// Whether compaction is enabled for the `metrics` table.
    pub metrics_enabled: bool,
    /// Whether compaction is enabled for the `operations` table.
    pub operations_enabled: bool,
    /// Data-compaction tunables (`compaction.data`).
    pub data: DataCompactionConfig,
    /// Manifest-compaction tunables (`compaction.manifest`).
    pub manifest: ManifestCompactionConfig,
    /// Jobs-manager settings (worker pool, discovery interval, job-state storage),
    /// nested to mirror ingest's `shift.jobsmanager`.
    pub jobsmanager: CompactionJobsManagerConfig,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            logs_enabled: true,
            spans_enabled: true,
            events_enabled: true,
            metrics_enabled: true,
            operations_enabled: true,
            data: DataCompactionConfig::default(),
            manifest: ManifestCompactionConfig::default(),
            jobsmanager: CompactionJobsManagerConfig::default(),
        }
    }
}

impl CompactionConfig {
    /// Validate every compaction group and the job-state storage config.
    ///
    /// Every field is `#[serde(default)]`, so a malformed config file loads
    /// silently with zeros in places that make the planner degenerate rather
    /// than erroring. This catches those up front. For example a
    /// `data.max_group_input_bytes` of 0 makes [`crate::compact::data::planner`] place
    /// every file in its own single-file group, all of which it drops as
    /// non-beneficial — so the service would run forever compacting nothing.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if any tunable is out of range or the
    /// [`JobsStorageConfig`] is invalid.
    pub fn validate(&self) -> Result<(), MaintainError> {
        self.data.validate()?;
        self.manifest.validate()?;
        self.jobsmanager.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let c = CompactionConfig::default();
        assert_eq!(c.data.target_file_size_bytes, 128 * 1024 * 1024);
        assert!(c.data.max_group_input_bytes >= c.data.target_file_size_bytes);
        assert_eq!(c.data.min_input_files, 4);
        assert_eq!(c.data.max_skippable_tail_files, 0);
        assert_eq!(c.data.max_merge_size_ratio, 2);
        assert!(c.logs_enabled && c.spans_enabled && c.events_enabled && c.metrics_enabled && c.operations_enabled);
        assert_eq!(c.manifest.target_size_bytes, 8 * 1024 * 1024);
        // Exact float equality trips clippy::float_cmp; compare within epsilon.
        assert!((c.manifest.candidate_size_ratio - 0.75).abs() < f64::EPSILON);
        assert_eq!(c.manifest.max_manifests_per_commit, 64);
        assert_eq!(c.manifest.rewrite_timeout_secs, 600);
    }

    #[test]
    fn default_worker_count_matches_available_parallelism() {
        let c = CompactionConfig::default();
        assert_eq!(c.jobsmanager.worker_count, default_worker_count());
    }

    /// Defaults plus a populated job-state storage. The default `JobsStorageConfig`
    /// has empty endpoint/bucket (a real deployment must set them), so a bare
    /// default does not validate; tests of the numeric tunables start from here.
    fn valid_config() -> CompactionConfig {
        CompactionConfig {
            jobsmanager: CompactionJobsManagerConfig {
                storage: JobsStorageConfig {
                    endpoint: "http://localhost:9000".to_string(),
                    bucket: "jobs".to_string(),
                    ..JobsStorageConfig::default()
                },
                ..CompactionJobsManagerConfig::default()
            },
            ..CompactionConfig::default()
        }
    }

    #[test]
    fn valid_config_passes_validation() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn zero_max_group_input_bytes_is_rejected() {
        // Sergey's example: a zero budget silently disables all compaction.
        let config = CompactionConfig {
            data: DataCompactionConfig {
                max_group_input_bytes: 0,
                ..DataCompactionConfig::default()
            },
            ..valid_config()
        };
        assert!(matches!(config.validate(), Err(MaintainError::Config(_))));
    }

    #[test]
    fn max_group_input_below_target_is_rejected() {
        let config = CompactionConfig {
            data: DataCompactionConfig {
                target_file_size_bytes: 128 * 1024 * 1024,
                max_group_input_bytes: 64 * 1024 * 1024,
                ..DataCompactionConfig::default()
            },
            ..valid_config()
        };
        assert!(matches!(config.validate(), Err(MaintainError::Config(_))));
    }

    #[test]
    fn zero_target_file_size_is_rejected() {
        let config = CompactionConfig {
            data: DataCompactionConfig {
                target_file_size_bytes: 0,
                ..DataCompactionConfig::default()
            },
            ..valid_config()
        };
        assert!(matches!(config.validate(), Err(MaintainError::Config(_))));
    }

    #[test]
    fn zero_size_merge_ratio_is_rejected() {
        let config = CompactionConfig {
            data: DataCompactionConfig {
                max_merge_size_ratio: 0,
                ..DataCompactionConfig::default()
            },
            ..valid_config()
        };
        assert!(matches!(config.validate(), Err(MaintainError::Config(_))));
    }

    #[test]
    fn zero_data_rewrite_timeout_is_rejected() {
        let config = CompactionConfig {
            data: DataCompactionConfig {
                rewrite_timeout_secs: 0,
                ..DataCompactionConfig::default()
            },
            ..valid_config()
        };
        assert!(matches!(config.validate(), Err(MaintainError::Config(_))));
    }

    #[test]
    fn zero_target_manifest_size_is_rejected() {
        let config = CompactionConfig {
            manifest: ManifestCompactionConfig {
                target_size_bytes: 0,
                ..ManifestCompactionConfig::default()
            },
            ..valid_config()
        };
        assert!(matches!(config.validate(), Err(MaintainError::Config(_))));
    }

    #[test]
    fn candidate_size_ratio_out_of_range_is_rejected() {
        // Zero, negative, above one, and NaN must all be rejected.
        for ratio in [0.0, -0.1, 1.5, f64::NAN] {
            let config = CompactionConfig {
                manifest: ManifestCompactionConfig {
                    candidate_size_ratio: ratio,
                    ..ManifestCompactionConfig::default()
                },
                ..valid_config()
            };
            assert!(matches!(config.validate(), Err(MaintainError::Config(_))));
        }
    }

    #[test]
    fn candidate_size_ratio_at_upper_bound_is_accepted() {
        let config = CompactionConfig {
            manifest: ManifestCompactionConfig {
                candidate_size_ratio: 1.0,
                ..ManifestCompactionConfig::default()
            },
            ..valid_config()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn max_manifests_per_commit_below_two_is_rejected() {
        for value in [0, 1] {
            let config = CompactionConfig {
                manifest: ManifestCompactionConfig {
                    max_manifests_per_commit: value,
                    ..ManifestCompactionConfig::default()
                },
                ..valid_config()
            };
            assert!(matches!(config.validate(), Err(MaintainError::Config(_))));
        }
    }

    #[test]
    fn zero_manifest_rewrite_timeout_is_rejected() {
        let config = CompactionConfig {
            manifest: ManifestCompactionConfig {
                rewrite_timeout_secs: 0,
                ..ManifestCompactionConfig::default()
            },
            ..valid_config()
        };
        assert!(matches!(config.validate(), Err(MaintainError::Config(_))));
    }

    #[test]
    fn invalid_jobs_storage_is_rejected() {
        // The bare default has empty storage endpoint/bucket, which must fail
        // through the delegated JobsStorageConfig::validate().
        assert!(matches!(
            CompactionConfig::default().validate(),
            Err(MaintainError::Config(_))
        ));
    }
}
