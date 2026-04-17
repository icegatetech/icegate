//! S3-backed catalog storage.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::spec::TableMetadata;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};
use uuid::Uuid;

use crate::codec::CatalogCodec;
use crate::config::S3CatalogConfig;
use crate::error::{Error, Result, StorageError};
use crate::model::CatalogRoot;
use crate::storage::{CatalogStorage, Version};

const ABSENT_ETAG: &str = "absent"; // TODO(crit): убрать

/// S3 catalog storage that owns object-store access and serialization.
pub(crate) struct S3CatalogStorage {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    root_path: Path,
    tables_prefix: Path,
    tables_uri_prefix: String,
    codec: Arc<dyn CatalogCodec>,
}

impl S3CatalogStorage {
    /// Create a new S3-backed storage from catalog config.
    pub(crate) fn new(config: &S3CatalogConfig) -> Result<Self> {
        validate_config(config)?;

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

        let warehouse = normalize_prefix(&config.warehouse);
        let catalog_prefix = if warehouse.is_empty() {
            Path::from("catalog")
        } else {
            Path::from_iter([warehouse.as_str(), "catalog"]) // TODO(crit): константа
        };

        let tables_prefix = catalog_prefix.child("tables"); // TODO(crit): константа
        let root_path = catalog_prefix.child("root.json"); // TODO(crit): брать из кодека
        let tables_uri_prefix = format!("s3://{}/{}", config.bucket, tables_prefix);

        Ok(Self {
            store,
            bucket: config.bucket.clone(),
            root_path,
            tables_prefix,
            tables_uri_prefix,
            codec: config.codec.into_codec(),
        })
    }

    async fn get_with_etag(&self, path: &Path) -> Result<(Bytes, String)> {
        let result = self.store.get(path).await.map_err(map_object_store_error)?;
        let etag = result
            .meta
            .e_tag
            .clone()
            .ok_or_else(|| StorageError::Io(format!("Missing ETag for {path}")))?;
        let bytes = result.bytes().await.map_err(map_object_store_error)?;
        Ok((bytes, etag))
    }

    async fn put_if_match(&self, path: &Path, data: Bytes, etag: &str) -> Result<()> {
        let options = PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            }),
            ..PutOptions::default()
        };
        self.store
            .put_opts(path, data.into(), options)
            .await
            .map_err(map_object_store_error)?;
        Ok(())
    }

    async fn put_if_none_match(&self, path: &Path, data: Bytes) -> Result<()> {
        self.store
            .put_opts(path, data.into(), PutMode::Create.into())
            .await
            .map_err(map_object_store_error)?;
        Ok(())
    }

    async fn put_unconditional(&self, path: &Path, data: Bytes) -> Result<()> {
        self.store
            .put_opts(path, data.into(), PutMode::Overwrite.into())
            .await
            .map_err(map_object_store_error)?;
        Ok(())
    }
}

#[async_trait]
impl CatalogStorage for S3CatalogStorage {
    async fn load_root(&self) -> Result<(CatalogRoot, Version)> {
        match self.get_with_etag(&self.root_path).await {
            Ok((bytes, etag)) => Ok((self.codec.decode_root(&bytes)?, Version(etag))),
            Err(Error::Storage(StorageError::NotFound(_))) => {
                // TODO(crit): когда версию засунем в CatalogRoot, то будем судить, что root.js нет по пустой версии.
                Ok((CatalogRoot::default(), Version(ABSENT_ETAG.to_string())))
            }
            Err(error) => Err(error),
        }
    }

    async fn save_root(&self, root: CatalogRoot, expected: &Version) -> Result<()> {
        let payload = self.codec.encode_root(&root)?;
        // TODO(crit): когда версию засунем в CatalogRoot, то будем судить, что root.js нет по пустой версии.
        let write_result = if expected.0 == ABSENT_ETAG {
            self.put_if_none_match(&self.root_path, payload).await
        } else {
            self.put_if_match(&self.root_path, payload, &expected.0).await
        };

        match write_result {
            Ok(()) => Ok(()),
            Err(Error::Storage(StorageError::PreconditionFailed | StorageError::AlreadyExists(_))) => {
                Err(Error::CommitConflict)
            }
            Err(error) => Err(error),
        }
    }

    async fn read_table_metadata(&self, location: &str) -> Result<TableMetadata> {
        let path = parse_s3_metadata_path(location, &self.bucket)?;
        let (payload, _) = self.get_with_etag(&path).await?;
        self.codec.decode_metadata(&payload)
    }

    async fn write_table_metadata(&self, table_id: &str, sequence_number: i64, metadata: &TableMetadata) -> Result<String> {
        let metadata_path = metadata_path(
            &self.tables_prefix,
            table_id,
            sequence_number,
            self.codec.file_extension(),
        );
        let metadata_location = metadata_uri(&self.bucket, &metadata_path);
        let payload = self.codec.encode_metadata(metadata)?;
        self.put_unconditional(&metadata_path, payload).await?;
        Ok(metadata_location)
    }

    fn default_table_location(&self, table_id: &str) -> String {
        format!("{}/{}", self.tables_uri_prefix, table_id)
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

fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
}

fn metadata_path(tables_prefix: &Path, table_id: &str, sequence_number: i64, extension: &str) -> Path {
    let file = format!("{sequence_number:05}-{}.{}", Uuid::new_v4(), extension);
    tables_prefix.child(table_id.to_string()).child("metadata").child(file)
}

fn metadata_uri(bucket: &str, path: &Path) -> String {
    format!("s3://{bucket}/{path}")
}

fn parse_s3_metadata_path(metadata_location: &str, expected_bucket: &str) -> Result<Path> {
    let without_scheme = metadata_location
        .strip_prefix("s3://")
        .ok_or_else(|| Error::InvalidMetadata(format!("Unsupported metadata location: {metadata_location}")))?;
    let (bucket, key) = without_scheme
        .split_once('/')
        .ok_or_else(|| Error::InvalidMetadata(format!("Invalid metadata location: {metadata_location}")))?;
    if bucket != expected_bucket {
        return Err(Error::InvalidMetadata(format!(
            "Metadata bucket mismatch: expected {expected_bucket}, got {bucket}"
        )));
    }
    Ok(Path::from(key))
}

// TODO(crit): маппинг должен быть в StorageError
fn map_object_store_error(error: object_store::Error) -> StorageError {
    match error {
        object_store::Error::NotFound { path, .. } => StorageError::NotFound(path),
        object_store::Error::Precondition { .. } => StorageError::PreconditionFailed,
        object_store::Error::AlreadyExists { path, .. } => StorageError::AlreadyExists(path),
        other => StorageError::Io(other.to_string()),
    }
}
