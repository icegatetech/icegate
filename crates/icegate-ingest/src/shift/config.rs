//! Shift configuration.

use std::fmt;
use std::time::Duration;

use icegate_common::storage::{REDACTED, StaticCredentials};
use jobmanager::{JobStateCodecKind, S3Config};
use serde::{Deserialize, Serialize};

use super::timeout::validate_timeout_ms;
use crate::error::IngestError;

/// Job state serialization format.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStateCodec {
    /// JSON-encoded job state.
    #[default]
    Json,
    /// CBOR-encoded job state.
    Cbor,
}

/// Job storage configuration for shift operations.
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
            prefix: "shifter".to_string(),
            region: "us-east-1".to_string(),
            job_state_codec: JobStateCodec::default(),
            request_timeout_secs: 5,
            access_key_id: None,
            secret_access_key: None,
        }
    }
}

impl From<JobStateCodec> for JobStateCodecKind {
    fn from(codec: JobStateCodec) -> Self {
        match codec {
            JobStateCodec::Json => Self::Json,
            JobStateCodec::Cbor => Self::Cbor,
        }
    }
}

impl JobsStorageConfig {
    /// Validate job storage configuration values.
    ///
    /// An absent endpoint and absent keys are valid: they select the AWS regional endpoint and the
    /// default credential chain. Half a key pair is rejected by [`Self::to_s3_config`], where the
    /// keys are read.
    pub fn validate(&self) -> Result<(), IngestError> {
        if self.bucket.trim().is_empty() {
            return Err(IngestError::Config(
                "jobsmanager.storage.bucket cannot be empty".to_string(),
            ));
        }
        if self.prefix.trim().is_empty() {
            return Err(IngestError::Config(
                "jobsmanager.storage.prefix cannot be empty".to_string(),
            ));
        }
        if self.region.trim().is_empty() {
            return Err(IngestError::Config(
                "jobsmanager.storage.region cannot be empty".to_string(),
            ));
        }
        if self.request_timeout_secs == 0 {
            return Err(IngestError::Config(
                "jobsmanager.storage.request_timeout_secs must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    /// Convert to jobmanager S3 storage configuration.
    ///
    /// Static keys, when configured, sign every request; otherwise the AWS SDK's default
    /// credential chain does. No environment variable is read here.
    ///
    /// # Errors
    ///
    /// Returns [`IngestError::Config`] naming the `shift.jobsmanager.storage` block when only one
    /// of the two keys is set.
    pub fn to_s3_config(&self) -> Result<S3Config, IngestError> {
        let credentials =
            StaticCredentials::from_parts(self.access_key_id.clone(), self.secret_access_key.clone(), None)
                .map_err(|error| IngestError::Config(format!("shift.jobsmanager.storage: {error}")))?;

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

    /// The endpoint to connect to, or `None` when the SDK should resolve the regional AWS one.
    ///
    /// A blank value counts as absent: a deployment that renders `endpoint: ""` for "no endpoint"
    /// must not hand the SDK an endpoint named by the empty string.
    fn resolve_endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref().filter(|endpoint| !endpoint.trim().is_empty())
    }
}

/// WAL read settings for shift.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShiftReadConfig {
    /// Maximum number of row groups to process per shift task. The restriction is necessary because very small row groups can be stored in the WAL, which can reduce performance.
    pub max_record_batches_per_task: usize,
    /// Soft per-task input target in megabytes. Planner flushes a chunk when adding the next
    /// cluster would push it past this size; chunks below this size are
    /// candidates for tail-merge with their predecessor. Acts as the lower
    /// bound of the desired output parquet file size range.
    /// Synchronize with `QueueWriteConfig::max_bytes_per_flush`.
    pub lower_bound_input_mb_per_task: u64,
    /// Hard per-task input cap in megabytes. Clusters above this are split into oversized
    /// sub-chunks (disjointness lost). Acts as the upper bound of the desired
    /// output parquet file size range. Must be `>= lower_bound`.
    pub upper_bound_input_mb_per_task: u64,
    /// Maximum number of WAL segments to read in parallel in plan stage.
    pub plan_segment_read_parallelism: usize,
    /// Maximum number of WAL segments to read in parallel in shift stage.
    pub shift_segment_read_parallelism: usize,
}

impl Default for ShiftReadConfig {
    fn default() -> Self {
        Self {
            max_record_batches_per_task: 1024,
            lower_bound_input_mb_per_task: 64,
            upper_bound_input_mb_per_task: 128,
            plan_segment_read_parallelism: 8,
            shift_segment_read_parallelism: 8,
        }
    }
}

/// Iceberg write settings for shift.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShiftWriteConfig {
    /// Parquet row group size (number of rows per row group).
    /// It is better to synchronize with `QueueCommonConfig::max_row_group_size`.
    pub row_group_size: usize,
    /// Maximum Parquet data page size, in bytes.
    ///
    /// Forwarded to `WriterProperties::set_data_page_size_limit`. Smaller pages
    /// improve page-index predicate pushdown at the cost of metadata overhead;
    /// larger pages reduce overhead but coarsen statistics granularity.
    pub data_page_size_limit_bytes: usize,
    /// Time-to-live for cached Iceberg table metadata, in seconds.
    pub table_cache_ttl_secs: u64,
}

impl Default for ShiftWriteConfig {
    fn default() -> Self {
        Self {
            row_group_size: 20_000,
            data_page_size_limit_bytes: 2 * 1024 * 1024,
            table_cache_ttl_secs: 60,
        }
    }
}

impl Default for ShiftTimeoutsConfig {
    fn default() -> Self {
        Self {
            plan_base_ms: 10_000,
            shift_base_ms: 10_000,
            shift_per_record_batch_ms: 50,
            shift_per_segment_ms: 300,
            commit_base_ms: 30_000,
            commit_per_parquet_file_ms: 100,
        }
    }
}

/// Jobs manager settings for shift.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShiftJobsManagerConfig {
    /// Number of job manager workers.
    pub worker_count: usize,
    /// Polling interval in milliseconds for job manager workers.
    pub poll_interval_ms: u64,
    /// Interval between job iterations, in milliseconds.
    pub iteration_interval_millisecs: u64,
    /// Job storage configuration.
    pub storage: JobsStorageConfig,
}

impl Default for ShiftJobsManagerConfig {
    fn default() -> Self {
        Self {
            worker_count: default_jobs_manager_worker_count(),
            poll_interval_ms: 1_000,              // 1 sec
            iteration_interval_millisecs: 30_000, // 30 sec
            storage: JobsStorageConfig::default(),
        }
    }
}

fn default_jobs_manager_worker_count() -> usize {
    // we'll leave half of threads for processing API requests
    std::thread::available_parallelism().map_or(1, |parallelism| parallelism.get().div_ceil(2))
}

/// Configuration for the shift process.
///
/// Controls how data is moved from the queue to Iceberg tables.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ShiftConfig {
    /// WAL read settings.
    pub read: ShiftReadConfig,
    /// Iceberg write settings.
    pub write: ShiftWriteConfig,
    /// Task timeout estimation settings.
    pub timeouts: ShiftTimeoutsConfig,
    /// Jobs manager settings.
    pub jobsmanager: ShiftJobsManagerConfig,
}

impl ShiftConfig {
    /// Validates configuration values.
    pub fn validate(&self) -> Result<(), IngestError> {
        if self.write.row_group_size < 128 {
            return Err(IngestError::Config(
                "row_group_size must be greater than zero".to_string(),
            ));
        }
        if self.write.data_page_size_limit_bytes == 0 {
            return Err(IngestError::Config(
                "data_page_size_limit_bytes must be greater than zero".to_string(),
            ));
        }
        if self.read.max_record_batches_per_task == 0 {
            return Err(IngestError::Config(
                "max_record_batches_per_task must be greater than zero".to_string(),
            ));
        }
        if self.read.lower_bound_input_mb_per_task == 0 {
            return Err(IngestError::Config(
                "lower_bound_input_mb_per_task must be greater than zero".to_string(),
            ));
        }
        if self.read.upper_bound_input_mb_per_task == 0 {
            return Err(IngestError::Config(
                "upper_bound_input_mb_per_task must be greater than zero".to_string(),
            ));
        }
        if self.read.upper_bound_input_mb_per_task < self.read.lower_bound_input_mb_per_task {
            return Err(IngestError::Config(
                "upper_bound_input_mb_per_task must be >= lower_bound_input_mb_per_task".to_string(),
            ));
        }
        if self.read.plan_segment_read_parallelism == 0 {
            return Err(IngestError::Config(
                "plan_segment_read_parallelism must be greater than zero".to_string(),
            ));
        }
        if self.read.shift_segment_read_parallelism == 0 {
            return Err(IngestError::Config(
                "shift_segment_read_parallelism must be greater than zero".to_string(),
            ));
        }
        if self.write.table_cache_ttl_secs == 0 {
            return Err(IngestError::Config(
                "table_cache_ttl_secs must be greater than zero".to_string(),
            ));
        }
        if self.jobsmanager.poll_interval_ms == 0 {
            return Err(IngestError::Config(
                "jobsmanager.poll_interval_ms must be greater than zero".to_string(),
            ));
        }
        if self.jobsmanager.worker_count == 0 {
            return Err(IngestError::Config(
                "jobsmanager.worker_count must be greater than zero".to_string(),
            ));
        }
        if self.jobsmanager.iteration_interval_millisecs == 0 {
            return Err(IngestError::Config(
                "jobsmanager.iteration_interval_millisecs must be greater than zero".to_string(),
            ));
        }
        self.timeouts.validate()?;
        self.jobsmanager.storage.validate()?;

        Ok(())
    }
}

/// Timeout settings for shift-related tasks.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
#[allow(clippy::struct_field_names)]
pub struct ShiftTimeoutsConfig {
    /// Base timeout for plan task in milliseconds.
    pub plan_base_ms: u64,
    /// Base timeout for shift task in milliseconds.
    pub shift_base_ms: u64,
    /// Additional timeout per record batch in milliseconds.
    pub shift_per_record_batch_ms: u64,
    /// Additional timeout per segment in milliseconds.
    pub shift_per_segment_ms: u64,
    /// Base timeout for commit task in milliseconds.
    pub commit_base_ms: u64,
    /// Additional timeout per parquet file in milliseconds.
    pub commit_per_parquet_file_ms: u64,
}

impl ShiftTimeoutsConfig {
    /// Validates timeout configuration values.
    pub fn validate(&self) -> Result<(), IngestError> {
        validate_timeout_ms("timeouts.plan_base_ms", self.plan_base_ms)?;
        validate_timeout_ms("timeouts.shift_base_ms", self.shift_base_ms)?;
        validate_timeout_ms("timeouts.shift_per_record_batch_ms", self.shift_per_record_batch_ms)?;
        validate_timeout_ms("timeouts.shift_per_segment_ms", self.shift_per_segment_ms)?;
        validate_timeout_ms("timeouts.commit_base_ms", self.commit_base_ms)?;
        validate_timeout_ms("timeouts.commit_per_parquet_file_ms", self.commit_per_parquet_file_ms)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        JobsStorageConfig, ShiftConfig, ShiftJobsManagerConfig, ShiftTimeoutsConfig, default_jobs_manager_worker_count,
    };
    use crate::error::IngestError;

    /// A job storage that names only the bucket: no endpoint, no keys.
    fn storage_with_bucket_only() -> JobsStorageConfig {
        JobsStorageConfig {
            bucket: "jobs".to_string(),
            ..JobsStorageConfig::default()
        }
    }

    #[test]
    fn validate_accepts_a_storage_without_an_endpoint() {
        storage_with_bucket_only()
            .validate()
            .expect("an absent endpoint is the AWS regional endpoint, not a mistake");
    }

    /// No environment variable is read, so the outcome cannot depend on the
    /// `AWS_*` variables of the process running the test.
    #[test]
    fn to_s3_config_succeeds_without_keys_or_endpoint() {
        assert!(storage_with_bucket_only().to_s3_config().is_ok());
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
                ..storage_with_bucket_only()
            };

            assert_eq!(storage.resolve_endpoint(), resolved, "endpoint {configured:?}");
            assert!(
                storage.validate().is_ok(),
                "endpoint {configured:?} must pass validation"
            );
            assert!(
                storage.to_s3_config().is_ok(),
                "endpoint {configured:?} must convert to an S3 config"
            );
        }
    }

    #[test]
    fn to_s3_config_accepts_a_complete_key_pair() {
        let storage = JobsStorageConfig {
            access_key_id: Some("access-key".to_string()),
            secret_access_key: Some("secret-key".to_string()),
            ..storage_with_bucket_only()
        };

        assert!(storage.to_s3_config().is_ok());
    }

    /// A deployment that renders `access_key_id: ""` for "no static key" means
    /// no static key, not a key named by the empty string.
    #[test]
    fn to_s3_config_treats_blank_keys_as_absent() {
        let storage = JobsStorageConfig {
            access_key_id: Some(String::new()),
            secret_access_key: Some("  ".to_string()),
            ..storage_with_bucket_only()
        };

        assert!(storage.to_s3_config().is_ok());
    }

    #[test]
    fn half_a_key_pair_names_the_config_block() {
        let halves = [(Some("access-key"), None), (None, Some("secret-key"))];

        for (access_key_id, secret_access_key) in halves {
            let storage = JobsStorageConfig {
                access_key_id: access_key_id.map(str::to_string),
                secret_access_key: secret_access_key.map(str::to_string),
                ..storage_with_bucket_only()
            };

            assert!(
                matches!(
                    storage.to_s3_config(),
                    Err(IngestError::Config(ref message)) if message.contains("shift.jobsmanager.storage")
                ),
                "half a key pair ({access_key_id:?}, {secret_access_key:?}) must be a config error naming the block"
            );
        }
    }

    /// The secret reaches logs through any `{:?}` of a config that holds the storage,
    /// so the formatted storage itself must not carry it.
    #[test]
    fn debug_output_does_not_contain_the_secret_access_key() {
        let storage = JobsStorageConfig {
            access_key_id: Some("access-key".to_string()),
            secret_access_key: Some("secret-key-must-not-leak".to_string()),
            ..storage_with_bucket_only()
        };

        let formatted = format!("{storage:?}");

        assert!(!formatted.contains("secret-key-must-not-leak"), "got {formatted}");
    }

    #[test]
    fn shift_read_default_input_bytes_bounds() {
        let config = ShiftConfig::default();
        assert_eq!(config.read.lower_bound_input_mb_per_task, 64);
        assert_eq!(config.read.upper_bound_input_mb_per_task, 128);
    }

    #[test]
    fn shift_validate_rejects_zero_lower_bound() {
        let mut config = ShiftConfig::default();
        config.read.lower_bound_input_mb_per_task = 0;
        let err = config.validate().expect_err("config must be invalid");
        assert!(
            matches!(err, crate::error::IngestError::Config(_)),
            "expected config error, got: {err}"
        );
    }

    #[test]
    fn shift_validate_rejects_zero_upper_bound() {
        let mut config = ShiftConfig::default();
        config.read.upper_bound_input_mb_per_task = 0;
        let err = config.validate().expect_err("config must be invalid");
        assert!(
            matches!(err, crate::error::IngestError::Config(_)),
            "expected config error, got: {err}"
        );
    }

    #[test]
    fn shift_validate_rejects_upper_below_lower() {
        let mut config = ShiftConfig::default();
        config.read.lower_bound_input_mb_per_task = 200;
        config.read.upper_bound_input_mb_per_task = 100;
        let err = config.validate().expect_err("config must be invalid");
        assert!(
            matches!(err, crate::error::IngestError::Config(_)),
            "expected config error, got: {err}"
        );
    }

    #[test]
    fn shift_validate_rejects_zero_plan_segment_read_parallelism() {
        let mut config = ShiftConfig::default();
        config.read.plan_segment_read_parallelism = 0;
        let err = config.validate().expect_err("config must be invalid");
        assert!(
            matches!(err, crate::error::IngestError::Config(_)),
            "expected config error, got: {err}"
        );
    }

    #[test]
    fn shift_validate_rejects_zero_shift_segment_read_parallelism() {
        let mut config = ShiftConfig::default();
        config.read.shift_segment_read_parallelism = 0;
        let err = config.validate().expect_err("config must be invalid");
        assert!(
            matches!(err, crate::error::IngestError::Config(_)),
            "expected config error, got: {err}"
        );
    }

    /// A zero deadline expires the task the moment a worker picks it up, so the task is
    /// re-picked forever and never completes — the configuration must be rejected at load
    /// instead. The table lists every field so a timeout added to [`ShiftTimeoutsConfig`] but
    /// left out of its `validate` fails here rather than in production.
    #[test]
    fn shift_validate_rejects_a_zero_timeout_in_every_field() {
        let zero_setters: [(&str, fn(&mut ShiftTimeoutsConfig)); 6] = [
            ("plan_base_ms", |timeouts| timeouts.plan_base_ms = 0),
            ("shift_base_ms", |timeouts| timeouts.shift_base_ms = 0),
            ("shift_per_record_batch_ms", |timeouts| {
                timeouts.shift_per_record_batch_ms = 0;
            }),
            ("shift_per_segment_ms", |timeouts| timeouts.shift_per_segment_ms = 0),
            ("commit_base_ms", |timeouts| timeouts.commit_base_ms = 0),
            ("commit_per_parquet_file_ms", |timeouts| {
                timeouts.commit_per_parquet_file_ms = 0;
            }),
        ];

        for (field, set_zero) in zero_setters {
            let mut config = ShiftConfig::default();
            set_zero(&mut config.timeouts);

            let err = config.validate().expect_err(&format!("a zero {field} must be invalid"));

            assert!(
                matches!(err, crate::error::IngestError::Config(_)),
                "expected config error for a zero {field}, got: {err}"
            );
        }
    }

    #[test]
    fn shift_validate_rejects_timeout_above_jobmanager_range() {
        let mut config = ShiftConfig::default();
        config.timeouts.plan_base_ms = u64::MAX;

        let err = config.validate().expect_err("timeout must be invalid");

        assert!(matches!(err, crate::error::IngestError::Config(_)));
    }

    #[test]
    fn shift_jobsmanager_default_worker_count_uses_available_parallelism() {
        let config = ShiftJobsManagerConfig::default();
        assert_eq!(
            config.worker_count,
            default_jobs_manager_worker_count(),
            "worker_count must follow available CPU parallelism"
        );
    }

    #[test]
    fn shift_validate_rejects_zero_data_page_size_limit_bytes() {
        // Guards the Helm chart / config field from defaulting to zero —
        // a zero data page size limit silently disables Parquet page
        // splitting, which would tank stats pruning at query time.
        let mut config = ShiftConfig::default();
        config.write.data_page_size_limit_bytes = 0;
        let err = config.validate().expect_err("config must be invalid");
        assert!(matches!(err, crate::error::IngestError::Config(_)));
    }
}
