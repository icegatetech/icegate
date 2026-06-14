//! Compaction configuration.
//!
//! Configuration for the long-running Parquet compaction service. The shape
//! mirrors ingest's shift configuration so operators get consistent knobs for
//! both background data-movement loops, but every field is optional in the
//! config file via `#[serde(default)]` so a minimal config still loads.

use std::time::Duration;

use icegate_jobmanager::s3_storage::{JobStateCodecKind, S3StorageConfig};
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
/// `icegate-common` and `icegate-jobmanager`; pulling in `icegate-ingest`
/// solely for this struct would couple maintenance to the ingest/WAL crate.
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
                "compaction.jobs_storage.endpoint cannot be empty".to_string(),
            ));
        }
        if self.bucket.trim().is_empty() {
            return Err(MaintainError::Config(
                "compaction.jobs_storage.bucket cannot be empty".to_string(),
            ));
        }
        if self.prefix.trim().is_empty() {
            return Err(MaintainError::Config(
                "compaction.jobs_storage.prefix cannot be empty".to_string(),
            ));
        }
        if self.region.trim().is_empty() {
            return Err(MaintainError::Config(
                "compaction.jobs_storage.region cannot be empty".to_string(),
            ));
        }
        if self.request_timeout_secs == 0 {
            return Err(MaintainError::Config(
                "compaction.jobs_storage.request_timeout_secs must be greater than zero".to_string(),
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
            retrier_config: icegate_jobmanager::RetrierConfig::default(),
        })
    }

    fn resolve_access_key_id(&self) -> Result<String, MaintainError> {
        Self::resolve_credential(
            self.access_key_id.as_deref(),
            "AWS_ACCESS_KEY_ID",
            "compaction.jobs_storage.access_key_id",
        )
    }

    fn resolve_secret_access_key(&self) -> Result<String, MaintainError> {
        Self::resolve_credential(
            self.secret_access_key.as_deref(),
            "AWS_SECRET_ACCESS_KEY",
            "compaction.jobs_storage.secret_access_key",
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

/// Configuration for the Parquet compaction process.
///
/// Controls how small Parquet data files in Iceberg tables are discovered,
/// bin-packed into rewrite groups, and rewritten into fewer, larger files.
// The four `*_enabled` flags are independent per-table toggles (logs, spans,
// events, metrics), not a hidden state machine; modelling them as enums would
// add noise without improving clarity.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CompactionConfig {
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
    /// Period of the discovery loop, in seconds (maps to the jobmanager
    /// iteration interval).
    pub scan_interval_secs: u64,
    /// Deadline for a single REWRITE task (merge + encode + commit), in seconds.
    ///
    /// Kept separate from [`Self::scan_interval_secs`] so a rewrite that
    /// legitimately runs longer than the discovery period is not declared
    /// expired — which would let another worker pick it up and duplicate the
    /// in-flight rewrite. Size it to a worst-case group: reading
    /// `max_group_input_bytes` of Parquet, k-way-merging, re-encoding, and
    /// committing.
    pub rewrite_timeout_secs: u64,
    /// Number of concurrent rewrite tasks (jobmanager `JobsManagerConfig.worker_count`).
    pub worker_count: usize,
    /// Jobmanager worker poll interval, in milliseconds.
    pub poll_interval_ms: u64,
    /// Parquet row group size, in rows.
    pub row_group_size: usize,
    /// Maximum Parquet data page size, in bytes.
    pub data_page_size_limit_bytes: usize,
    /// Whether compaction is enabled for the `logs` table.
    pub logs_enabled: bool,
    /// Whether compaction is enabled for the `spans` table.
    pub spans_enabled: bool,
    /// Whether compaction is enabled for the `events` table.
    pub events_enabled: bool,
    /// Whether compaction is enabled for the `metrics` table.
    pub metrics_enabled: bool,
    /// Jobs-state storage (S3), the same shape ingest's shift uses.
    pub jobs_storage: JobsStorageConfig,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            target_file_size_bytes: 128 * 1024 * 1024,
            max_group_input_bytes: 256 * 1024 * 1024,
            min_input_files: 4,
            max_skippable_tail_files: 0,
            max_merge_size_ratio: 2,
            scan_interval_secs: 300,
            rewrite_timeout_secs: 3_600,
            worker_count: default_worker_count(),
            poll_interval_ms: 1_000,
            row_group_size: 20_000,
            data_page_size_limit_bytes: 2 * 1024 * 1024,
            logs_enabled: true,
            spans_enabled: true,
            events_enabled: true,
            metrics_enabled: true,
            jobs_storage: JobsStorageConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_sane() {
        let c = CompactionConfig::default();
        assert_eq!(c.target_file_size_bytes, 128 * 1024 * 1024);
        assert!(c.max_group_input_bytes >= c.target_file_size_bytes);
        assert_eq!(c.min_input_files, 4);
        assert_eq!(c.max_skippable_tail_files, 0);
        assert_eq!(c.max_merge_size_ratio, 2);
        assert!(c.logs_enabled && c.spans_enabled && c.events_enabled && c.metrics_enabled);
    }

    #[test]
    fn default_worker_count_matches_available_parallelism() {
        let c = CompactionConfig::default();
        assert_eq!(c.worker_count, default_worker_count());
    }
}
