//! Worker pool and job-state storage configuration, shared by every maintain
//! service that runs on a jobmanager pool.
//!
//! Compaction, the orphan sweep, WAL cleanup, and the pricing crawler each own a
//! pool, and every one of them is configured through the same
//! `jobsmanager.{worker_count, poll_interval_ms, scan_interval_secs, storage}`
//! shape — the shape ingest's `shift::config` exposes too. The types live here
//! rather than in one service's module so no service owns another's knobs, and
//! so a validation message can name the block it came from.

use std::fmt;
use std::time::Duration;

use icegate_common::storage::{REDACTED, StaticCredentials};
use jobmanager::{JobStateCodecKind, S3Config};
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
///
/// `Debug` redacts `secret_access_key`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct JobsStorageConfig {
    /// S3 endpoint URL, for an S3-compatible store or an AWS endpoint the SDK would not pick on
    /// its own. Its scheme selects TLS: `https://` connects over TLS, `http://` does not.
    /// Absent (or blank) means the SDK resolves the regional AWS endpoint.
    pub endpoint: Option<String>,
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
    /// Static access key ID for S3. Set together with `secret_access_key`, or leave both unset to
    /// sign with the AWS SDK's default credential chain. Blank counts as unset.
    pub access_key_id: Option<String>,
    /// Static secret access key for S3. Set together with `access_key_id`; see there.
    pub secret_access_key: Option<String>,
}

// Manual `Debug` so an accidental log of a config holding the storage can never leak the
// secret key; the access key id is an identifier, not a secret.
impl fmt::Debug for JobsStorageConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JobsStorageConfig")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("job_state_codec", &self.job_state_codec)
            .field("request_timeout_secs", &self.request_timeout_secs)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &self.secret_access_key.as_ref().map(|_| REDACTED))
            .finish()
    }
}

impl Default for JobsStorageConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
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
    /// Returns [`MaintainError::Config`] if the bucket, prefix or region is
    /// empty or the request timeout is zero.
    ///
    /// An absent endpoint and absent keys are valid: they select the AWS
    /// regional endpoint and the default credential chain. Half a key pair is
    /// rejected by [`Self::to_s3_storage_config`], where the keys are read.
    pub fn validate(&self, caller: &str) -> Result<(), MaintainError> {
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

    /// Convert to a jobmanager [`S3Config`].
    ///
    /// Static keys, when configured, sign every request; otherwise the AWS
    /// SDK's default credential chain does. No environment variable is read
    /// here. `config_block` names the section for error messages, as in
    /// [`Self::validate`].
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] naming the
    /// `{config_block}.jobsmanager.storage` block when only one of the two keys
    /// is set.
    pub fn to_s3_storage_config(&self, config_block: &str) -> Result<S3Config, MaintainError> {
        let credentials =
            StaticCredentials::from_parts(self.access_key_id.clone(), self.secret_access_key.clone(), None)
                .map_err(|error| MaintainError::Config(format!("{config_block}.jobsmanager.storage: {error}")))?;

        let mut s3_config = S3Config::new(self.bucket.as_str(), self.region.as_str());
        if let Some(endpoint) = self.resolve_endpoint() {
            s3_config = s3_config.with_endpoint(endpoint);
        }
        if let Some(credentials) = credentials {
            s3_config = s3_config.with_static_credentials(
                credentials.access_key_id,
                credentials.secret_access_key,
                credentials.session_token,
            );
        }

        Ok(s3_config
            .with_state_prefix(self.prefix.as_str())
            .with_job_state_codec(self.job_state_codec.into())
            .with_request_timeout(Duration::from_secs(self.request_timeout_secs)))
    }

    /// The endpoint to connect to, or `None` when the SDK should resolve the
    /// regional AWS one.
    ///
    /// A blank value counts as absent: a deployment that renders `endpoint: ""`
    /// for "no endpoint" must not hand the SDK an endpoint named by the empty
    /// string.
    fn resolve_endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref().filter(|endpoint| !endpoint.trim().is_empty())
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
    use crate::error::MaintainError;

    /// A storage config populated enough to pass validation: the bucket alone,
    /// with no endpoint and no keys.
    fn valid_storage() -> JobsStorageConfig {
        JobsStorageConfig {
            bucket: "jobs".to_string(),
            ..JobsStorageConfig::default()
        }
    }

    #[test]
    fn validate_accepts_a_storage_without_an_endpoint() {
        valid_storage()
            .validate("gc")
            .expect("an absent endpoint is the AWS regional endpoint, not a mistake");
    }

    /// No environment variable is read, so the outcome cannot depend on the
    /// `AWS_*` variables of the process running the test.
    #[test]
    fn to_s3_config_succeeds_without_keys_or_endpoint() {
        assert!(valid_storage().to_s3_storage_config("gc").is_ok());
    }

    /// A deployment may render `endpoint: ""` for "no endpoint". The S3 config
    /// cannot be inspected, so the rule is checked where it is decided.
    #[test]
    fn to_s3_config_treats_a_blank_endpoint_as_none() {
        let cases = [
            (None, None),
            (Some(""), None),
            (Some("  "), None),
            (Some("http://localhost:9000"), Some("http://localhost:9000")),
        ];

        for (configured, resolved) in cases {
            let storage = JobsStorageConfig {
                endpoint: configured.map(str::to_string),
                ..valid_storage()
            };

            assert_eq!(storage.resolve_endpoint(), resolved, "endpoint {configured:?}");
            assert!(
                storage.validate("gc").is_ok(),
                "endpoint {configured:?} must pass validation"
            );
            assert!(
                storage.to_s3_storage_config("gc").is_ok(),
                "endpoint {configured:?} must convert to an S3 config"
            );
        }
    }

    #[test]
    fn to_s3_config_accepts_a_complete_key_pair() {
        let storage = JobsStorageConfig {
            access_key_id: Some("access-key".to_string()),
            secret_access_key: Some("secret-key".to_string()),
            ..valid_storage()
        };

        assert!(storage.to_s3_storage_config("gc").is_ok());
    }

    /// A deployment that renders `access_key_id: ""` for "no static key" means
    /// no static key, not a key named by the empty string.
    #[test]
    fn to_s3_config_treats_blank_keys_as_absent() {
        let storage = JobsStorageConfig {
            access_key_id: Some(String::new()),
            secret_access_key: Some("  ".to_string()),
            ..valid_storage()
        };

        assert!(storage.to_s3_storage_config("gc").is_ok());
    }

    /// The secret reaches logs through any `{:?}` of a config that holds the storage,
    /// so the formatted storage itself must not carry it.
    #[test]
    fn debug_output_does_not_contain_the_secret_access_key() {
        let storage = JobsStorageConfig {
            access_key_id: Some("access-key".to_string()),
            secret_access_key: Some("secret-key-must-not-leak".to_string()),
            ..valid_storage()
        };

        let formatted = format!("{storage:?}");

        assert!(!formatted.contains("secret-key-must-not-leak"), "got {formatted}");
    }

    #[test]
    fn half_a_key_pair_names_the_config_block() {
        let halves = [(Some("access-key"), None), (None, Some("secret-key"))];

        for (access_key_id, secret_access_key) in halves {
            let storage = JobsStorageConfig {
                access_key_id: access_key_id.map(str::to_string),
                secret_access_key: secret_access_key.map(str::to_string),
                ..valid_storage()
            };

            assert!(
                matches!(
                    storage.to_s3_storage_config("gc"),
                    Err(MaintainError::Config(ref message)) if message.contains("gc.jobsmanager.storage")
                ),
                "half a key pair ({access_key_id:?}, {secret_access_key:?}) must be a config error naming the block"
            );
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

        let error = config.validate("gc").expect_err("an empty bucket must be rejected");

        assert!(
            error.to_string().contains("gc.jobsmanager.storage.bucket"),
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
