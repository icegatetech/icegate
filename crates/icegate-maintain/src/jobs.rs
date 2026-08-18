//! Worker pool and job-state storage configuration, shared by every maintain
//! service that runs on a jobmanager pool.
//!
//! Compaction, the orphan sweep, WAL cleanup, and the pricing crawler each own a
//! pool, and every one of them is configured through the same
//! `jobsmanager.{worker_count, poll_interval_ms, scan_interval_secs, storage}`
//! shape — the shape ingest's `shift::config` exposes too. The types live here
//! rather than in one service's module so no service owns another's knobs, and
//! so a validation message can name the block it came from.

use std::time::Duration;

use jobmanager::{JobStateCodecKind, S3StorageConfig};
use serde::{Deserialize, Serialize};

use crate::error::MaintainError;

/// Job state serialization format.
///
/// Mirrors ingest's shift codec selection so the on-disk job-state encoding is
/// configured identically for every background loop.
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

/// Job-state storage (S3) of one maintain service.
///
/// Maintain-local mirror of ingest's `shift::config::JobsStorageConfig`. It is
/// duplicated rather than imported because `icegate-maintain` depends only on
/// `icegate-common` and `jobmanager`; pulling in `icegate-ingest` solely for
/// this struct would couple maintenance to the ingest/WAL crate.
///
/// Every service points its own `prefix` at the same bucket, so one service's
/// job state never collides with another's.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JobsStorageConfig {
    /// S3 endpoint URL. Its scheme selects TLS: `https://` connects over TLS, `http://` does not.
    pub endpoint: String,
    /// Bucket name for job state.
    pub bucket: String,
    /// Prefix for job state objects.
    pub prefix: String,
    /// AWS region name.
    pub region: String,
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
    /// `config_block` is the config section these values were read from
    /// (`"compaction"`, `"gc"`, `"pricing"`, `"wal_cleanup"`); it prefixes every message
    /// so an operator is told which block to fix, not which validator ran.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if any required field is empty or the
    /// request timeout is zero.
    pub fn validate(&self, caller: &str) -> Result<(), MaintainError> {
        if self.endpoint.trim().is_empty() {
            return Err(MaintainError::Config(format!(
                "{caller}.jobsmanager.storage.endpoint cannot be empty"
            )));
        }
        if self.bucket.trim().is_empty() {
            return Err(MaintainError::Config(format!(
                "{caller}.jobsmanager.storage.bucket cannot be empty"
            )));
        }
        if self.prefix.trim().is_empty() {
            return Err(MaintainError::Config(format!(
                "{caller}.jobsmanager.storage.prefix cannot be empty"
            )));
        }
        if self.region.trim().is_empty() {
            return Err(MaintainError::Config(format!(
                "{caller}.jobsmanager.storage.region cannot be empty"
            )));
        }
        if self.request_timeout_secs == 0 {
            return Err(MaintainError::Config(format!(
                "{caller}.jobsmanager.storage.request_timeout_secs must be greater than zero"
            )));
        }
        Ok(())
    }

    /// Convert to a jobmanager [`S3StorageConfig`].
    ///
    /// Credentials are taken from the explicit fields when set, otherwise from
    /// the `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables.
    /// `config_block` names the section for error messages, as in
    /// [`Self::validate`].
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if credentials are configured as empty
    /// strings or cannot be resolved from the environment.
    pub fn to_s3_storage_config(&self, config_block: &str) -> Result<S3StorageConfig, MaintainError> {
        let access_key_id = self.resolve_access_key_id(config_block)?;
        let secret_access_key = self.resolve_secret_access_key(config_block)?;

        Ok(S3StorageConfig::new(
            self.endpoint.as_str(),
            access_key_id,
            secret_access_key,
            self.bucket.as_str(),
            self.region.as_str(),
        )
        .with_bucket_prefix(self.prefix.as_str())
        .with_job_state_codec(self.job_state_codec.into())
        .with_request_timeout(Duration::from_secs(self.request_timeout_secs)))
    }

    fn resolve_access_key_id(&self, config_block: &str) -> Result<String, MaintainError> {
        Self::resolve_credential(
            self.access_key_id.as_deref(),
            "AWS_ACCESS_KEY_ID",
            &format!("{config_block}.jobsmanager.storage.access_key_id"),
        )
    }

    fn resolve_secret_access_key(&self, config_block: &str) -> Result<String, MaintainError> {
        Self::resolve_credential(
            self.secret_access_key.as_deref(),
            "AWS_SECRET_ACCESS_KEY",
            &format!("{config_block}.jobsmanager.storage.secret_access_key"),
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

/// Default number of concurrent workers in a pool.
///
/// Mirrors ingest's `default_jobs_manager_worker_count`: half of the available
/// CPU parallelism (rounded up), leaving headroom for other work on the node.
fn default_worker_count() -> usize {
    std::thread::available_parallelism().map_or(1, |parallelism| parallelism.get().div_ceil(2))
}

/// Worker pool, discovery cadence, and job-state storage of one service.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JobsManagerConfig {
    /// Number of concurrent tasks the pool runs (the jobmanager's own worker count).
    pub worker_count: usize,
    /// Jobmanager worker poll interval, in milliseconds.
    pub poll_interval_ms: u64,
    /// Period of the discovery loop, in seconds (maps to the jobmanager
    /// iteration interval).
    pub scan_interval_secs: u64,
    /// Jobs-state storage (S3), the same shape ingest's shift uses.
    pub storage: JobsStorageConfig,
}

impl Default for JobsManagerConfig {
    fn default() -> Self {
        Self {
            worker_count: default_worker_count(),
            poll_interval_ms: 1_000,
            scan_interval_secs: 300,
            storage: JobsStorageConfig::default(),
        }
    }
}

impl JobsManagerConfig {
    /// Validate the jobs-manager tunables and the job-state storage.
    ///
    /// `config_block` names the section these values were read from; see
    /// [`JobsStorageConfig::validate`].
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] if a tunable is zero or the
    /// [`JobsStorageConfig`] is invalid.
    pub fn validate(&self, caller: &str) -> Result<(), MaintainError> {
        if self.worker_count == 0 {
            return Err(MaintainError::Config(format!(
                "{caller}.jobsmanager.worker_count must be greater than zero"
            )));
        }
        if self.poll_interval_ms == 0 {
            return Err(MaintainError::Config(format!(
                "{caller}.jobsmanager.poll_interval_ms must be greater than zero"
            )));
        }
        if self.scan_interval_secs == 0 {
            return Err(MaintainError::Config(format!(
                "{caller}.jobsmanager.scan_interval_secs must be greater than zero"
            )));
        }
        self.storage.validate(caller)
    }
}

#[cfg(test)]
mod tests {
    use super::{JobsManagerConfig, JobsStorageConfig};

    /// A storage config populated enough to pass validation.
    fn valid_storage() -> JobsStorageConfig {
        JobsStorageConfig {
            endpoint: "http://localhost:9000".to_string(),
            bucket: "jobs".to_string(),
            ..JobsStorageConfig::default()
        }
    }

    /// The block path is the machine-readable half of a config error: it is what
    /// tells an operator which of the four `jobsmanager` blocks to fix, and the
    /// shared validator used to name `compaction` for all of them.
    #[test]
    fn a_validation_error_names_the_block_it_came_from() {
        let config = JobsManagerConfig {
            worker_count: 0,
            ..JobsManagerConfig::default()
        };

        let error = config.validate("wal_cleanup").expect_err("zero workers must be rejected");

        assert!(
            error.to_string().contains("wal_cleanup.jobsmanager.worker_count"),
            "the error must name the block that carries the value, got: {error}"
        );
    }

    /// The same for the storage half, which is validated through the manager.
    #[test]
    fn a_storage_error_names_the_block_it_came_from() {
        let config = JobsManagerConfig {
            storage: JobsStorageConfig::default(),
            ..JobsManagerConfig::default()
        };

        let error = config.validate("gc").expect_err("an empty endpoint must be rejected");

        assert!(
            error.to_string().contains("gc.jobsmanager.storage.endpoint"),
            "the error must name the block that carries the value, got: {error}"
        );
    }

    /// The pool sizes itself from the node it runs on; a default that stopped
    /// doing so would silently under-use every maintain deployment.
    #[test]
    fn the_default_worker_count_follows_available_parallelism() {
        assert_eq!(JobsManagerConfig::default().worker_count, super::default_worker_count());
    }

    #[test]
    fn a_populated_config_validates() {
        let config = JobsManagerConfig {
            storage: valid_storage(),
            ..JobsManagerConfig::default()
        };

        config.validate("compaction").expect("a populated config validates");
    }
}
