//! S3-backed catalog storage.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::spec::TableMetadata;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, PutMode, PutOptions, UpdateVersion};
use tokio_util::sync::CancellationToken;

use crate::codec::CatalogCodec;
use crate::config::S3CatalogConfig;
use crate::error::{Error, Result, StorageError};
use crate::infra::retrier::Retrier;
use crate::root::CatalogRoot;
use crate::storage::{CatalogStorage, LoadOutcome, Version};

const CATALOG_SEGMENT: &str = "catalog";

/// S3 catalog storage that owns object-store access and serialization.
pub(crate) struct S3CatalogStorage {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    root_path: Path,
    root_codec: Arc<dyn CatalogCodec>,
    retrier: Retrier,
    cancel_token: CancellationToken,
}

impl S3CatalogStorage {
    /// Create a new S3-backed storage from catalog config.
    pub(crate) fn new(config: &S3CatalogConfig, cancel_token: CancellationToken) -> Result<Self> {
        validate_config(config)?;
        let retrier = Retrier::new(config.storage_retrier_config.clone());

        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(config.bucket.clone())
            .with_region(config.region.clone());

        if let Some(endpoint) = config.endpoint.as_ref() {
            builder = builder.with_endpoint(endpoint.clone());
            if endpoint.starts_with("http://") {
                builder = builder.with_allow_http(true);
            }
        }
        if let Some(access_key_id) = config.access_key_id.as_ref() {
            builder = builder.with_access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = config.secret_access_key.as_ref() {
            builder = builder.with_secret_access_key(secret_access_key);
        }

        let store: Arc<dyn ObjectStore> = Arc::new(
            builder
                .build()
                .map_err(|error| Error::Storage(StorageError::Io(error.to_string())))?,
        );

        let warehouse = Self::normalize_prefix(&config.warehouse);
        let root_codec = config.codec.into_codec();
        let catalog_prefix = if warehouse.is_empty() {
            Path::from(CATALOG_SEGMENT)
        } else {
            Path::from_iter([warehouse.as_str(), CATALOG_SEGMENT])
        };

        let root_path = catalog_prefix.child(root_codec.root_filename());

        Ok(Self {
            store,
            bucket: config.bucket.clone(),
            root_path,
            root_codec,
            retrier,
            cancel_token,
        })
    }

    /// Run a storage closure under the configured retry policy.
    ///
    /// Transient errors (timeouts, 5xx, throttling) trigger a backoff and a
    /// retry. CAS conflicts (precondition failed, create-if-absent collisions)
    /// and `NotModified` bypass the retry layer and propagate immediately —
    /// the upper layer decides whether to rebuild state and try again.
    async fn run_with_retry<F, Fut, T>(&self, op: F) -> Result<T>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send,
    {
        let retry_result = self
            .retrier
            .retry(
                || async {
                    match op().await {
                        Ok(value) => Ok((false, Some(value))),
                        Err(error) if error.is_retryable() => Ok((true, None)),
                        Err(error) => Err(error),
                    }
                },
                &self.cancel_token,
            )
            .await;

        match retry_result {
            Ok(Some(value)) => Ok(value),
            Ok(None) | Err(Error::CasMaxAttempts) => Err(Error::StorageRetryExhausted),
            Err(error) => Err(error),
        }
    }

    /// Outcome of a single conditional GET. `None` means the upstream
    /// short-circuited with 304 Not Modified.
    async fn get_with_etag_inner(&self, path: &Path, if_none_match: Option<&str>) -> Result<Option<(Bytes, String)>> {
        let options = GetOptions {
            if_none_match: if_none_match.map(ToString::to_string),
            ..GetOptions::default()
        };
        match self.store.get_opts(path, options).await {
            Ok(result) => {
                let etag = result
                    .meta
                    .e_tag
                    .clone()
                    .ok_or_else(|| StorageError::Io(format!("Missing ETag for {path}")))?;
                let bytes = result.bytes().await.map_err(StorageError::from)?;
                Ok(Some((bytes, etag)))
            }
            // `If-None-Match` matched — caller's cached copy is still current.
            // Translate to `None` here so the conditional-read signal never
            // leaks out as an `Error`.
            Err(object_store::Error::NotModified { .. }) => Ok(None),
            Err(error) => Err(StorageError::from(error).into()),
        }
    }

    async fn get_with_etag(&self, path: &Path, if_none_match: Option<&str>) -> Result<Option<(Bytes, String)>> {
        self.run_with_retry(|| self.get_with_etag_inner(path, if_none_match)).await
    }

    async fn put_if_match_inner(&self, path: &Path, data: Bytes, etag: &str) -> Result<Option<String>> {
        let options = PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            }),
            ..PutOptions::default()
        };
        let result = self
            .store
            .put_opts(path, data.into(), options)
            .await
            .map_err(StorageError::from)?;
        Ok(result.e_tag)
    }

    async fn put_if_match(&self, path: &Path, data: Bytes, etag: &str) -> Result<Option<String>> {
        self.run_with_retry(|| self.put_if_match_inner(path, data.clone(), etag)).await
    }

    async fn put_if_none_match_inner(&self, path: &Path, data: Bytes) -> Result<Option<String>> {
        let result = self
            .store
            .put_opts(path, data.into(), PutMode::Create.into())
            .await
            .map_err(StorageError::from)?;
        Ok(result.e_tag)
    }

    async fn put_if_none_match(&self, path: &Path, data: Bytes) -> Result<Option<String>> {
        self.run_with_retry(|| self.put_if_none_match_inner(path, data.clone())).await
    }

    async fn put_unconditional_inner(&self, path: &Path, data: Bytes) -> Result<()> {
        self.store
            .put_opts(path, data.into(), PutMode::Overwrite.into())
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn put_unconditional(&self, path: &Path, data: Bytes) -> Result<()> {
        self.run_with_retry(|| self.put_unconditional_inner(path, data.clone())).await
    }

    fn parse_s3_location(location: &str, expected_bucket: &str) -> Result<Path> {
        let without_scheme = location
            .strip_prefix("s3://")
            .ok_or_else(|| Error::InvalidMetadata(format!("Unsupported metadata location: {location}")))?;
        let (bucket, key) = without_scheme
            .split_once('/')
            .ok_or_else(|| Error::InvalidMetadata(format!("Invalid metadata location: {location}")))?;
        if bucket != expected_bucket {
            return Err(Error::InvalidMetadata(format!(
                "Metadata bucket mismatch: expected {expected_bucket}, got {bucket}"
            )));
        }
        if key.is_empty() {
            return Err(Error::InvalidMetadata(format!(
                "Invalid metadata location: object key is empty in {location}"
            )));
        }
        Ok(Path::from(key))
    }

    fn normalize_prefix(prefix: &str) -> String {
        prefix.trim_matches('/').to_string()
    }
}

#[async_trait]
impl CatalogStorage for S3CatalogStorage {
    async fn load_root(&self, known: Option<&Version>) -> Result<LoadOutcome> {
        let known_etag = known.and_then(|version| match version {
            Version::Etag(etag) => Some(etag.as_str()),
            Version::Absent => None,
        });
        match self.get_with_etag(&self.root_path, known_etag).await {
            Ok(Some((bytes, etag))) => Ok(LoadOutcome::Loaded {
                root: Arc::new(self.root_codec.decode_root(&bytes)?),
                version: Version::Etag(etag),
            }),
            Ok(None) => Ok(LoadOutcome::NotModified),
            Err(Error::Storage(StorageError::NotFound(_))) => Ok(LoadOutcome::Absent),
            Err(error) => Err(error),
        }
    }

    async fn save_root(&self, root: Arc<CatalogRoot>, expected: &Version) -> Result<Version> {
        let payload = self.root_codec.encode_root(&root)?;
        let write_result = match expected {
            Version::Absent => self.put_if_none_match(&self.root_path, payload).await,
            Version::Etag(etag) => self.put_if_match(&self.root_path, payload, etag).await,
        };

        match write_result {
            Ok(Some(etag)) => Ok(Version::Etag(etag)),
            // Some S3 implementations omit ETag on conditional PUT responses; surface this as
            // a non-retryable I/O error rather than silently breaking CAS for the next commit.
            Ok(None) => Err(Error::Storage(StorageError::Io(
                "missing ETag in S3 PUT response for root object".to_string(),
            ))),
            Err(Error::Storage(StorageError::PreconditionFailed | StorageError::AlreadyExists(_))) => {
                Err(Error::CommitConflict)
            }
            Err(error) => Err(error),
        }
    }

    async fn read_table_metadata(&self, location: &str) -> Result<Arc<TableMetadata>> {
        let path = Self::parse_s3_location(location, &self.bucket)?;
        let (payload, _) = self
            .get_with_etag(&path, None)
            .await?
            // `if_none_match: None` cannot produce a 304 — defensive guard
            // against an unexpected upstream behaviour change.
            .ok_or_else(|| Error::Storage(StorageError::Io("unconditional GET returned NotModified".to_string())))?;
        let metadata: TableMetadata = serde_json::from_slice(&payload)
            .map_err(|error| Error::InvalidMetadata(format!("Invalid table metadata: {error}")))?;
        Ok(Arc::new(metadata))
    }

    async fn write_table_metadata(&self, location: &str, metadata: &TableMetadata) -> Result<()> {
        let object_path = Self::parse_s3_location(location, &self.bucket)?;
        let payload = serde_json::to_vec(metadata)
            .map(Bytes::from)
            .map_err(|error| Error::InvalidMetadata(format!("Failed to serialize metadata: {error}")))?;
        // UUID in the filename guarantees unique paths; CAS on root.json decides the winner.
        self.put_unconditional(&object_path, payload).await
    }
}

fn validate_config(config: &S3CatalogConfig) -> Result<()> {
    if config.bucket.trim().is_empty() {
        return Err(Error::InvalidMetadata("bucket cannot be empty".to_string()));
    }
    if config.region.trim().is_empty() {
        return Err(Error::InvalidMetadata("region cannot be empty".to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use iceberg::ErrorKind;

    use super::*;
    use crate::CatalogCodecKind;
    use crate::infra::retrier::RetrierConfig;
    use crate::storage::CatalogStorage;

    fn config() -> S3CatalogConfig {
        S3CatalogConfig {
            bucket: "bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            warehouse: "/warehouse/".to_string(),
            codec: CatalogCodecKind::Json,
            ..S3CatalogConfig::default()
        }
    }

    #[test]
    fn new_fails_on_empty_bucket() {
        let mut config = config();
        config.bucket = "   ".to_string();

        let Err(error) = S3CatalogStorage::new(&config, CancellationToken::new()) else {
            panic!("empty bucket must fail");
        };

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[test]
    fn new_fails_on_empty_region() {
        let mut config = config();
        config.region = String::new();

        let Err(error) = S3CatalogStorage::new(&config, CancellationToken::new()) else {
            panic!("empty region must fail");
        };

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[tokio::test]
    async fn read_table_metadata_fails_on_wrong_scheme() {
        let storage = S3CatalogStorage::new(&config(), CancellationToken::new()).expect("storage");
        let error = storage
            .read_table_metadata("memory://bucket/warehouse/catalog/tables/a/metadata/00001-uuid.json")
            .await
            .expect_err("wrong scheme must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[tokio::test]
    async fn read_table_metadata_fails_on_wrong_bucket() {
        let storage = S3CatalogStorage::new(&config(), CancellationToken::new()).expect("storage");
        let error = storage
            .read_table_metadata("s3://other/warehouse/catalog/tables/a/metadata/00001-uuid.json")
            .await
            .expect_err("wrong bucket must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[tokio::test]
    async fn read_table_metadata_fails_on_empty_object_key() {
        // `s3://bucket/` parses to an empty object key; reject it before any GET.
        let storage = S3CatalogStorage::new(&config(), CancellationToken::new()).expect("storage");
        let error = storage
            .read_table_metadata("s3://bucket/")
            .await
            .expect_err("empty object key must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[tokio::test]
    async fn read_table_metadata_fails_on_missing_key_separator() {
        // `s3://bucket` has no `/` after the bucket, so there is no object key.
        let storage = S3CatalogStorage::new(&config(), CancellationToken::new()).expect("storage");
        let error = storage
            .read_table_metadata("s3://bucket")
            .await
            .expect_err("missing key separator must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[tokio::test]
    async fn storage_retry_exhaustion_is_not_cas_exhaustion() {
        let mut config = config();
        config.storage_retrier_config = RetrierConfig {
            max_attempts: 1,
            rand_delay: Duration::ZERO,
            delays: Vec::new(),
        };
        let storage = S3CatalogStorage::new(&config, CancellationToken::new()).expect("storage");

        let error = storage
            .run_with_retry(|| async {
                let result: Result<()> = Err(Error::Storage(StorageError::Transient("timeout".to_string())));
                result
            })
            .await
            .expect_err("transient retry exhaustion must be a storage exhaustion");

        assert!(matches!(error, Error::StorageRetryExhausted));
        let iceberg_error: iceberg::Error = error.into();
        assert_eq!(iceberg_error.kind(), ErrorKind::Unexpected);
        assert!(iceberg_error.retryable());
    }

    #[tokio::test]
    async fn run_with_retry_propagates_non_retryable_immediately() {
        // A non-retryable error must short-circuit: it is neither retried nor
        // remapped to `StorageRetryExhausted`. A budget of 3 attempts proves the
        // single invocation is the propagation, not exhaustion.
        let mut config = config();
        config.storage_retrier_config = RetrierConfig {
            max_attempts: 3,
            rand_delay: Duration::ZERO,
            delays: vec![Duration::ZERO],
        };
        let storage = S3CatalogStorage::new(&config, CancellationToken::new()).expect("storage");
        let calls = AtomicUsize::new(0);

        let error = storage
            .run_with_retry(|| async {
                calls.fetch_add(1, Ordering::SeqCst);
                let result: Result<()> = Err(Error::Storage(StorageError::Io("disk".to_string())));
                result
            })
            .await
            .expect_err("non-retryable error must propagate");

        assert!(matches!(error, Error::Storage(StorageError::Io(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "non-retryable error must not retry");
    }

    #[tokio::test]
    async fn run_with_retry_returns_value_after_transient() {
        // A transient fault on the first attempt backs off and retries; the
        // second attempt succeeds and its value is returned.
        let mut config = config();
        config.storage_retrier_config = RetrierConfig {
            max_attempts: 5,
            rand_delay: Duration::ZERO,
            delays: vec![Duration::ZERO],
        };
        let storage = S3CatalogStorage::new(&config, CancellationToken::new()).expect("storage");
        let calls = AtomicUsize::new(0);

        let value = storage
            .run_with_retry(|| async {
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    Err(Error::Storage(StorageError::Transient("timeout".to_string())))
                } else {
                    Ok(7_u32)
                }
            })
            .await
            .expect("transient retry must succeed");

        assert_eq!(value, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 2, "first transient then success");
    }
}
