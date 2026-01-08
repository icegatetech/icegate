use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use aws_config::timeout::TimeoutConfig;
use aws_sdk_s3::{Client, primitives::ByteStream};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    Error, Job, JobCode, JobDefinitionRegistry, JobMeta, JobStatus, Metrics, Retrier, RetrierConfig, Storage,
    StorageError, StorageResult, Task, TaskCode, TaskStatus,
};

// TODO(low): need mechanism to clean up old job states. Required for iter num restart and reduced
// storage load TODO(high): add test s3 storage with Toxiproxy for testing network problems

const JOB_STATE_FILE_PREFIX: &str = "state-";
const JOB_STATE_FILE_EXTENSION: &str = ".json";

#[derive(Debug, Serialize, Deserialize)]
struct TaskJson {
    id: String,
    code: String,
    status: TaskStatus,
    created_by_worker: String,
    #[serde(default)]
    timeout_ms: i64,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    processing_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deadline_at: Option<DateTime<Utc>>,
    attempt: u32,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    input: Vec<u8>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    output: Vec<u8>,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    error: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct JobJson {
    id: String,
    code: String,
    iter_num: u64,
    status: JobStatus,
    tasks: Vec<TaskJson>,
    updated_by: String,
    started_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    running_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completed_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_start_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<std::collections::HashMap<String, serde_json::Value>>,
}

pub struct S3StorageConfig {
    /// S3 endpoint URL.
    pub endpoint: String,
    /// Access key ID for S3.
    pub access_key_id: String,
    /// Secret access key for S3.
    pub secret_access_key: String,
    /// Bucket name for job state.
    pub bucket_name: String,
    /// Whether to use HTTPS for the endpoint.
    pub use_ssl: bool,
    /// AWS region name.
    pub region: String,
    /// Prefix for job state objects.
    pub bucket_prefix: String,
    /// Request timeout for S3 operations.
    pub request_timeout: Duration,
    /// Retry policy configuration.
    pub retrier_config: RetrierConfig,
}

pub struct S3Storage {
    client: Client,
    bucket_name: String,
    bucket_prefix: String,
    registry: Arc<dyn JobDefinitionRegistry>, /* TODO(med): save job settings separately and provide an API for
                                               * changing settings */
    retrier: Retrier,
    metrics: Metrics,
}

impl S3Storage {
    pub async fn new(
        config: S3StorageConfig,
        registry: Arc<dyn JobDefinitionRegistry>,
        metrics: Metrics,
    ) -> Result<Self, Error> {
        let credentials = aws_sdk_s3::config::Credentials::new(
            config.access_key_id.clone(),
            config.secret_access_key.clone(),
            None,
            None,
            "static",
        );

        // Initialize AWS SDK config
        let mut sdk_config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()))
            .credentials_provider(credentials);

        let timeout_config = TimeoutConfig::builder()
            .operation_timeout(config.request_timeout)
            .operation_attempt_timeout(config.request_timeout)
            .build();
        sdk_config_loader = sdk_config_loader.timeout_config(timeout_config);

        let sdk_config = sdk_config_loader.load().await;

        // Build S3 client config
        let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

        s3_config_builder = s3_config_builder.endpoint_url(config.endpoint).force_path_style(true);

        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        // TODO(med): add check that conditional requests work for specific S3. For example, old Minio
        // versions ignore the header and atomicity breaks

        // Check if bucket exists, create if needed
        match client.head_bucket().bucket(&config.bucket_name).send().await {
            Ok(_) => info!("Bucket {} exists", config.bucket_name),
            Err(aws_sdk_s3::error::SdkError::ServiceError(se)) if se.raw().status().as_u16() == 404 => {
                client
                    .create_bucket()
                    .bucket(&config.bucket_name)
                    .send()
                    .await
                    .map_err(|e| Error::Other(format!("Failed to create bucket: {e}")))?;
                info!("Created bucket {}", config.bucket_name);
            }
            Err(e) => return Err(Error::Other(format!("Failed to check bucket: {e}"))),
        }

        let retrier = Retrier::new(config.retrier_config.clone());

        Ok(Self {
            client,
            bucket_name: config.bucket_name,
            bucket_prefix: config.bucket_prefix,
            registry,
            retrier,
            metrics,
        })
    }

    fn record_s3_ok(&self, operation: &str, start: Instant) {
        self.metrics.record_s3_operation(operation, "OK", start.elapsed());
    }

    fn record_s3_err<E: std::fmt::Debug>(&self, operation: &str, err: &aws_sdk_s3::error::SdkError<E>, start: Instant) {
        let status = if let aws_sdk_s3::error::SdkError::ServiceError(service_err) = err {
            service_err.raw().status().as_u16().to_string()
        } else {
            "ERR".to_string()
        };
        self.metrics.record_s3_operation(operation, &status, start.elapsed());
    }

    fn map_s3_error<E: std::fmt::Debug>(err: &aws_sdk_s3::error::SdkError<E>) -> StorageError {
        // TODO(med): add job context to errors
        match err {
            aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
                let status = service_err.raw().status().as_u16();
                match status {
                    401 | 403 => StorageError::Auth,
                    404 => StorageError::NotFound,
                    408 => StorageError::Timeout,
                    412 => StorageError::ConcurrentModification,
                    429 => StorageError::RateLimited,
                    500 | 502 | 503 | 504 => StorageError::ServiceUnavailable,
                    _ => StorageError::S3(format!("S3 SDK error: {err:?}")),
                }
            }
            aws_sdk_s3::error::SdkError::TimeoutError(_) => StorageError::Timeout,
            aws_sdk_s3::error::SdkError::DispatchFailure(e) => {
                // Network/connection errors - should be retryable
                StorageError::S3(format!("Network error: {e:?}"))
            }
            _ => StorageError::S3(format!("S3 SDK error: {err:?}")),
        }
    }

    fn build_job_path(&self, job_code: &JobCode) -> String {
        format!("{}/{}/", self.bucket_prefix, job_code.as_str())
    }

    // buildStatePath builds path to state file with inverted iterNum for S3 sorting
    fn build_state_path(&self, job_code: &JobCode, iter_num: u64) -> String {
        // Invert iterNum so new files are at the beginning of list (S3 sorts by name) and LIST request is
        // fast
        let inv_iter_num = u64::MAX - iter_num;
        format!(
            "{}{}{:020}{}",
            self.build_job_path(job_code),
            JOB_STATE_FILE_PREFIX,
            inv_iter_num,
            JOB_STATE_FILE_EXTENSION
        )
    }

    // parseIterNumFromFilePath parses iterNum from file path
    fn parse_iter_num_from_path(file_path: &str) -> StorageResult<u64> {
        // Expected format: {prefix}/{jobCode}/state-00001.json
        let parts: Vec<&str> = file_path.split('/').collect();
        if parts.len() < 2 {
            return Err(StorageError::Other(format!("Cannot split file path {file_path}")));
        }

        let filename = parts[parts.len() - 1];
        if !filename.starts_with(JOB_STATE_FILE_PREFIX) || !filename.ends_with(JOB_STATE_FILE_EXTENSION) {
            return Err(StorageError::Other(format!("Invalid filename format {filename}")));
        }

        let iter_num_str = filename
            .trim_start_matches(JOB_STATE_FILE_PREFIX)
            .trim_end_matches(JOB_STATE_FILE_EXTENSION);

        let inv_iter_num: u64 = iter_num_str
            .parse()
            .map_err(|e| StorageError::Other(format!("Failed to parse iter_num: {e}")))?;

        // Restore original iterNum
        Ok(u64::MAX - inv_iter_num)
    }

    fn serialize_job(job: &Job) -> StorageResult<Vec<u8>> {
        let job_json = Self::job_to_json(job);
        serde_json::to_vec_pretty(&job_json).map_err(|e| StorageError::Serialization(e.to_string()))
    }

    fn deserialize_job(&self, data: &[u8], version: &str) -> StorageResult<Job> {
        // TODO(med): replace native json format with binary format like MessagePack or CBOR for best
        // performance.
        let job_json: JobJson = serde_json::from_slice(data).map_err(|e| StorageError::Serialization(e.to_string()))?;
        let job_def = self
            .registry
            .get_job(&JobCode::new(job_json.code.as_str()))
            .map_err(|e| StorageError::Serialization(e.to_string()))?;
        Ok(Self::job_from_json(job_json, job_def.max_iterations(), version))
    }

    fn task_to_json(task: &Task) -> TaskJson {
        TaskJson {
            id: task.id().to_string(),
            code: task.code().to_string(),
            status: task.status().clone(),
            timeout_ms: task.timeout().num_milliseconds(),
            created_by_worker: task.created_by_worker().to_string(),
            processing_by: task.processing_by_worker().to_string(),
            started_at: task.started_at(),
            completed_at: task.completed_at(),
            deadline_at: task.deadline_at(),
            attempt: task.attempt(),
            input: task.input().to_vec(),
            output: task.output().to_vec(),
            error: task.error_msg().to_string(),
        }
    }

    fn job_to_json(job: &Job) -> JobJson {
        let tasks: Vec<TaskJson> = job.tasks_as_iter().map(Self::task_to_json).collect();

        JobJson {
            id: job.id().to_string(),
            code: job.code().to_string(),
            iter_num: job.iter_num(),
            status: job.status().clone(),
            tasks,
            updated_by: job.updated_by_worker_id().to_string(),
            started_at: job.started_at(),
            running_at: job.running_at(),
            completed_at: job.completed_at(),
            next_start_at: job.next_start_at(),
            metadata: if job.metadata().is_empty() {
                None
            } else {
                Some(job.metadata().clone())
            },
        }
    }

    fn task_from_json(json: TaskJson) -> Task {
        Task::restore(
            json.id,
            TaskCode::new(json.code),
            json.status,
            json.processing_by,
            json.created_by_worker,
            chrono::Duration::milliseconds(json.timeout_ms),
            json.started_at,
            json.completed_at,
            json.deadline_at,
            json.attempt,
            json.input,
            json.output,
            json.error,
        )
    }

    fn job_from_json(json: JobJson, max_iterations: u64, version: &str) -> Job {
        let tasks: Vec<Task> = json.tasks.into_iter().map(Self::task_from_json).collect();

        Job::restore(
            json.id,
            JobCode::new(json.code),
            version.to_string(),
            json.iter_num,
            json.status,
            tasks,
            json.updated_by,
            json.started_at,
            json.running_at,
            json.completed_at,
            json.next_start_at,
            json.metadata.unwrap_or_default(),
            max_iterations,
        )
    }

    async fn put_next_iteration(&self, key: &str, job_serialized: Vec<u8>) -> StorageResult<Option<String>> {
        // Use if-none-match="*" for new iteration
        let start = Instant::now();
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(ByteStream::from(job_serialized))
            .content_type("application/json")
            .if_none_match("*")
            .send()
            .await;

        match result {
            Ok(output) => {
                self.record_s3_ok("PUT", start);
                Ok(output.e_tag().map(std::string::ToString::to_string))
            }
            Err(e) => {
                self.record_s3_err("PUT", &e, start);
                Err(Self::map_s3_error(&e))
            }
        }
    }

    async fn put_current_iteration(
        &self,
        key: &str,
        job_serialized: Vec<u8>,
        version: &str,
    ) -> StorageResult<Option<String>> {
        // TODO(med): to check - set {PutObjectOptions{DisableMultipart: true} and send correct file. When
        // multipart is on there may be problems with the etag. Atomic write with If-Match
        let start = Instant::now();
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(ByteStream::from(job_serialized))
            .content_type("application/json")
            .if_match(version)
            .send()
            .await;

        match result {
            Ok(output) => {
                self.record_s3_ok("PUT", start);
                Ok(output.e_tag().map(std::string::ToString::to_string))
            }
            Err(e) => {
                self.record_s3_err("PUT", &e, start);
                Err(Self::map_s3_error(&e))
            }
        }
    }
}

#[async_trait::async_trait]
#[allow(private_interfaces)]
impl Storage for S3Storage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        // TODO(low): perhaps should try to get current job iteration first and read new file on miss. But
        // if job has few tasks, we'll miss often and make extra requests

        let job_code_for_retry = job_code.clone();
        let job_opt = self
            .retrier
            .retry(
                move || {
                    let job_code = job_code_for_retry.clone();
                    async move {
                        let job_meta = self.find_job_meta(&job_code, cancel_token).await?;
                        match self.get_job_by_meta(&job_meta, cancel_token).await {
                            Ok(job) => Ok((false, Some(job))),
                            Err(e) if e.is_retryable() || e.is_conflict() => Ok((true, None)),
                            Err(e) => Err(e),
                        }
                    }
                },
                cancel_token,
            )
            .await?;

        job_opt.ok_or_else(|| StorageError::Other("retry finished without job".into()))
    }

    #[tracing::instrument(skip(self, cancel_token), fields(job_version = %job_meta.version))]
    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let key = self.build_state_path(&job_meta.code, job_meta.iter_num);

        let start = Instant::now();
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .if_match(&job_meta.version)
            .send()
            .await;

        let output = match result {
            Ok(output) => output,
            Err(e) => {
                self.record_s3_err("GET", &e, start);
                return Err(Self::map_s3_error(&e));
            }
        };

        let data = output
            .body
            .collect()
            .await
            .map_err(|e| {
                self.metrics.record_s3_operation("GET", "ERR", start.elapsed());
                StorageError::S3(format!("Failed to read job body: {e}"))
            })?
            .into_bytes();
        self.record_s3_ok("GET", start);

        let job = self.deserialize_job(&data, job_meta.version.as_str())?;

        Ok(job)
    }

    #[tracing::instrument(skip(self, cancel_token), fields(job_code = %job_code))]
    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let prefix = self.build_job_path(job_code);

        let start = Instant::now();
        let result = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(&prefix)
            .max_keys(1) // Take only first file since inverted name order is used (DESC)
            .send()
            .await;

        let result = match result {
            Ok(output) => {
                self.record_s3_ok("LIST", start);
                output
            }
            Err(e) => {
                self.record_s3_err("LIST", &e, start);
                return Err(Self::map_s3_error(&e));
            }
        };

        let contents = result.contents();
        if let Some(object) = contents.first() {
            if let (Some(key), Some(etag)) = (object.key(), object.e_tag()) {
                let iter_num = Self::parse_iter_num_from_path(key)?;
                return Ok(JobMeta {
                    code: job_code.clone(),
                    iter_num,
                    version: etag.to_string(),
                });
            }
        }

        Err(StorageError::NotFound)
    }

    // SaveJob saves job atomically with Version check
    #[tracing::instrument(skip(self, cancel_token, job), fields(job_version = %job.version()))]
    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let is_new_iter = job.is_started();
        let version = job.version().to_string();
        let data = Arc::new(Self::serialize_job(job)?);
        let key = self.build_state_path(job.code(), job.iter_num());

        if job.is_started() {
            debug!(
                "Saving next job iteration (id: {}, code: {}, iter: {}, status: {:?})",
                job.id().to_string(),
                job.code().clone(),
                job.iter_num(),
                job.status().clone()
            );
        } else {
            debug!(
                "Saving current job iteration (id: {}, code: {}, iter: {}, status: {:?})",
                job.id().to_string(),
                job.code().clone(),
                job.iter_num(),
                job.status().clone()
            );
        }

        let etag_opt = self
            .retrier
            .retry(
                move || {
                    let key = key.clone();
                    let data = Arc::clone(&data);
                    let version = version.clone();
                    async move {
                        let result = if is_new_iter {
                            self.put_next_iteration(&key, data.as_ref().clone()).await
                        } else {
                            self.put_current_iteration(&key, data.as_ref().clone(), &version).await
                        };

                        match result {
                            Ok(etag) => Ok((false, etag)),
                            Err(e) if e.is_retryable() => Ok((true, None)),
                            Err(e) => Err(e),
                        }
                    }
                },
                cancel_token,
            )
            .await?;

        match etag_opt {
            Some(etag) => job.update_version(etag),
            None => {
                return Err(StorageError::Other(format!(
                    "missing etag after save_job for job {} (iter: {})",
                    job.code(),
                    job.iter_num()
                )));
            }
        }

        debug!(
            "Job iteration saved (id: {}, code: {}, iter: {}, status: {:?})",
            job.id().to_string(),
            job.code().clone(),
            job.iter_num(),
            job.status().clone()
        );

        Ok(())
    }
}
