//! S3-backed catalog storage.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::spec::TableMetadata;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion};

use crate::codec::CatalogCodec;
use crate::config::S3CatalogConfig;
use crate::error::{Error, Result, StorageError};
use crate::model::CatalogRoot;
use crate::storage::{CatalogStorage, Version};

const CATALOG_SEGMENT: &str = "catalog";

/// S3 catalog storage that owns object-store access and serialization.
pub(crate) struct S3CatalogStorage {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    root_path: Path,
    root_codec: Arc<dyn CatalogCodec>,
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
        })
    }

    async fn get_with_etag(&self, path: &Path) -> Result<(Bytes, String)> {
        let result = self.store.get(path).await.map_err(StorageError::from)?;
        let etag = result
            .meta
            .e_tag
            .clone()
            .ok_or_else(|| StorageError::Io(format!("Missing ETag for {path}")))?;
        let bytes = result.bytes().await.map_err(StorageError::from)?;
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
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn put_if_none_match(&self, path: &Path, data: Bytes) -> Result<()> {
        self.store
            .put_opts(path, data.into(), PutMode::Create.into())
            .await
            .map_err(StorageError::from)?;
        Ok(())
    }

    async fn put_unconditional(&self, path: &Path, data: Bytes) -> Result<()> {
        self.store
            .put_opts(path, data.into(), PutMode::Overwrite.into())
            .await
            .map_err(StorageError::from)?;
        Ok(())
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
        Ok(Path::from(key))
    }

    fn normalize_prefix(prefix: &str) -> String {
        prefix.trim_matches('/').to_string()
    }
}

#[async_trait]
impl CatalogStorage for S3CatalogStorage {
    async fn load_root(&self) -> Result<(CatalogRoot, Version)> {
        match self.get_with_etag(&self.root_path).await {
            Ok((bytes, etag)) => Ok((self.root_codec.decode_root(&bytes)?, Version::Etag(etag))),
            Err(Error::Storage(StorageError::NotFound(_))) => Ok((CatalogRoot::default(), Version::Absent)),
            Err(error) => Err(error),
        }
    }

    async fn save_root(&self, root: CatalogRoot, expected: &Version) -> Result<()> {
        let payload = self.root_codec.encode_root(&root)?;
        let write_result = match expected {
            Version::Absent => self.put_if_none_match(&self.root_path, payload).await,
            Version::Etag(etag) => self.put_if_match(&self.root_path, payload, etag).await,
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
        let path = Self::parse_s3_location(location, &self.bucket)?;
        let (payload, _) = self.get_with_etag(&path).await?;
        serde_json::from_slice(&payload)
            .map_err(|error| Error::InvalidMetadata(format!("Invalid table metadata: {error}")))
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

    use super::*;
    use crate::CatalogCodecKind;
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
        }
    }

    #[test]
    fn new_fails_on_empty_bucket() {
        let mut config = config();
        config.bucket = "   ".to_string();

        let Err(error) = S3CatalogStorage::new(&config) else {
            panic!("empty bucket must fail");
        };

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[test]
    fn new_fails_on_empty_region() {
        let mut config = config();
        config.region = String::new();

        let Err(error) = S3CatalogStorage::new(&config) else {
            panic!("empty region must fail");
        };

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[tokio::test]
    async fn read_table_metadata_fails_on_wrong_scheme() {
        let storage = S3CatalogStorage::new(&config()).expect("storage");
        let error = storage
            .read_table_metadata("memory://bucket/warehouse/catalog/tables/a/metadata/00001-uuid.json")
            .await
            .expect_err("wrong scheme must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }

    #[tokio::test]
    async fn read_table_metadata_fails_on_wrong_bucket() {
        let storage = S3CatalogStorage::new(&config()).expect("storage");
        let error = storage
            .read_table_metadata("s3://other/warehouse/catalog/tables/a/metadata/00001-uuid.json")
            .await
            .expect_err("wrong bucket must fail");

        assert!(matches!(error, Error::InvalidMetadata(_)));
    }
}
