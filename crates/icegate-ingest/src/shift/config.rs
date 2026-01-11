//! Shift configuration.

use std::time::Duration;

use icegate_jobmanager::s3_storage::{JobStateCodecKind, S3StorageConfig};
use serde::{Deserialize, Serialize};

use crate::error::IngestError;

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
    /// Jobs manager settings.
    pub jobsmanager: ShiftJobsManagerConfig,
}

/// WAL read settings for shift.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShiftReadConfig {
    /// Maximum number of row groups to process per shift task.
    pub max_record_batches_per_task: usize,
}

/// Iceberg write settings for shift.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShiftWriteConfig {
    /// Parquet row group size (number of rows per row group).
    pub row_group_size: usize,
    /// Maximum file size in MB before rolling to a new file.
    pub max_file_size_mb: usize,
    /// Time-to-live for cached Iceberg table metadata, in seconds.
    pub table_cache_ttl_secs: u64,
}

/// Jobs manager settings for shift.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShiftJobsManagerConfig {
    /// Polling interval in milliseconds for job manager workers.
    pub poll_interval_ms: u64,
    /// Interval between job iterations, in milliseconds.
    pub iteration_interval_millisecs: u64,
    /// Job storage configuration.
    pub storage: JobsStorageConfig,
}

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

impl Default for ShiftReadConfig {
    fn default() -> Self {
        Self {
            max_record_batches_per_task: 128,
        }
    }
}

impl Default for ShiftWriteConfig {
    fn default() -> Self {
        Self {
            row_group_size: 10_000,
            max_file_size_mb: 100,
            table_cache_ttl_secs: 60,
        }
    }
}

impl Default for ShiftJobsManagerConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 1000,
            iteration_interval_millisecs: 300,
            storage: JobsStorageConfig::default(),
        }
    }
}

impl Default for JobsStorageConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            bucket: String::new(),
            prefix: "shifter".to_string(),
            region: "us-east-1".to_string(),
            use_ssl: false,
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
    pub fn validate(&self) -> Result<(), IngestError> {
        if self.endpoint.trim().is_empty() {
            return Err(IngestError::Config(
                "jobsmanager.storage.endpoint cannot be empty".to_string(),
            ));
        }
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
    pub fn to_s3_config(&self) -> Result<S3StorageConfig, IngestError> {
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

    fn resolve_access_key_id(&self) -> Result<String, IngestError> {
        if let Some(access_key_id) = &self.access_key_id {
            if access_key_id.trim().is_empty() {
                return Err(IngestError::Config(
                    "jobsmanager.storage.access_key_id cannot be empty".to_string(),
                ));
            }
            return Ok(access_key_id.clone());
        }

        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| IngestError::Config("AWS_ACCESS_KEY_ID environment variable is not set".to_string()))?;
        if access_key_id.trim().is_empty() {
            return Err(IngestError::Config(
                "AWS_ACCESS_KEY_ID environment variable is empty".to_string(),
            ));
        }
        Ok(access_key_id)
    }

    fn resolve_secret_access_key(&self) -> Result<String, IngestError> {
        if let Some(secret_access_key) = &self.secret_access_key {
            if secret_access_key.trim().is_empty() {
                return Err(IngestError::Config(
                    "jobsmanager.storage.secret_access_key cannot be empty".to_string(),
                ));
            }
            return Ok(secret_access_key.clone());
        }

        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| IngestError::Config("AWS_SECRET_ACCESS_KEY environment variable is not set".to_string()))?;
        if secret_access_key.trim().is_empty() {
            return Err(IngestError::Config(
                "AWS_SECRET_ACCESS_KEY environment variable is empty".to_string(),
            ));
        }
        Ok(secret_access_key)
    }
}

impl ShiftConfig {
    /// Validates configuration values.
    pub fn validate(&self) -> Result<(), IngestError> {
        if self.write.row_group_size == 0 {
            return Err(IngestError::Config(
                "row_group_size must be greater than zero".to_string(),
            ));
        }
        if self.write.max_file_size_mb == 0 {
            return Err(IngestError::Config(
                "max_file_size_mb must be greater than zero".to_string(),
            ));
        }
        if self.read.max_record_batches_per_task == 0 {
            return Err(IngestError::Config(
                "max_row_groups_per_task must be greater than zero".to_string(),
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
        if self.jobsmanager.iteration_interval_millisecs == 0 {
            return Err(IngestError::Config(
                "jobsmanager.iteration_interval_millisecs must be greater than zero".to_string(),
            ));
        }
        self.jobsmanager.storage.validate()?;

        Ok(())
    }
}
