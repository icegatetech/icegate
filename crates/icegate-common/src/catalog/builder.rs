//! Catalog builder factory

use std::{collections::HashMap, sync::Arc, time::Duration};

use iceberg::{
    Catalog, CatalogBuilder as IcebergCatalogBuilder,
    io::FileIOBuilder,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
};
use iceberg_catalog_glue::GlueCatalogBuilder;
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_catalog_s3tables::S3TablesCatalogBuilder;
use icegate_catalog_s3::{
    CatalogCodecKind, S3Catalog, S3CatalogConfig, format_warehouse_uri, resolve_path_style_access,
};
use tokio_util::sync::CancellationToken;

use super::{CacheConfig, CatalogBackend, CatalogConfig};
use crate::error::{CommonError, Result};
use crate::storage::PrefetchConfig;
use crate::storage::cache::{StorageCache, build_storage_cache};
use crate::storage::icegate_storage::IceGateStorageFactory;

/// Handle for gracefully closing the foyer IO cache on shutdown.
///
/// Foyer's `HybridCache` runs background flusher tasks that send work through
/// internal channels. Dropping the cache without closing it first causes
/// "sending on a closed channel" errors as the flusher tasks race against
/// runtime teardown. Calling [`close()`](Self::close) drains the flushers
/// cleanly before the cache is dropped.
///
/// The cache is created **outside** the catalog builder via
/// [`from_config()`](Self::from_config) so that the same cache can be shared
/// with other components (e.g., the WAL object store) without coupling its
/// lifecycle to catalog construction.
///
/// # Usage
///
/// ```ignore
/// let io_cache = IoHandle::from_config(
///     config.catalog.cache.as_ref(),
/// ).await?;
/// let catalog = CatalogBuilder::from_config(&config.catalog, &io_cache, shutdown_token).await?;
/// // ... run servers ...
/// // On shutdown:
/// io_cache.close().await;
/// ```
pub struct IoHandle {
    cache: Option<StorageCache>,
    prefetch: Option<PrefetchConfig>,
    stat_ttl: Option<Duration>,
    max_write_cache_size: Option<usize>,
}

impl IoHandle {
    /// Create a no-op handle (no cache configured).
    ///
    /// Useful in tests and tools that don't need IO caching.
    #[must_use]
    pub const fn noop() -> Self {
        Self {
            cache: None,
            prefetch: None,
            stat_ttl: None,
            max_write_cache_size: None,
        }
    }

    /// Create a handle from optional cache configuration.
    ///
    /// When `config` is `None`, creates a no-op handle whose
    /// [`close()`](Self::close) is a no-op and whose [`cache()`](Self::cache)
    /// returns `None`.
    ///
    /// # Errors
    ///
    /// Returns an error if the foyer cache cannot be built (e.g., memory/disk
    /// size overflow or filesystem error).
    pub async fn from_config(cache_config: Option<&CacheConfig>) -> Result<Self> {
        let stat_ttl = cache_config.and_then(|cc| cc.stat_ttl_secs).map(Duration::from_secs);
        let max_write_cache_size = cache_config
            .and_then(|cc| cc.max_write_cache_size_mb)
            .map(|mb| mb.saturating_mul(1024 * 1024));
        let cache = match cache_config {
            Some(cc) => Some(build_storage_cache(cc).await?),
            None => None,
        };
        let prefetch = cache_config.and_then(|cc| cc.prefetch.clone());
        Ok(Self {
            cache,
            prefetch,
            stat_ttl,
            max_write_cache_size,
        })
    }

    /// Returns a reference to the underlying foyer cache, if configured.
    ///
    /// Clone the returned cache to share it with other components (e.g., WAL
    /// object store). `StorageCache` is `Arc`-based, so clones share the same
    /// cache instance.
    pub const fn cache(&self) -> Option<&StorageCache> {
        self.cache.as_ref()
    }

    /// Returns a reference to the prefetch configuration, if configured.
    pub const fn prefetch(&self) -> Option<&PrefetchConfig> {
        self.prefetch.as_ref()
    }

    /// Returns the stat metadata cache TTL, if configured.
    pub const fn stat_ttl(&self) -> Option<Duration> {
        self.stat_ttl
    }

    /// Returns the max value size (bytes) to cache on writes, if configured.
    pub const fn max_write_cache_size(&self) -> Option<usize> {
        self.max_write_cache_size
    }

    /// Gracefully close the IO cache, draining background flusher tasks.
    ///
    /// No-op when no cache was configured.
    pub async fn close(self) {
        if let Some(cache) = self.cache {
            tracing::info!("Closing IO cache...");
            if let Err(e) = cache.close().await {
                tracing::warn!("IO cache close returned error: {e}");
            }
            tracing::info!("IO cache closed");
        }
    }

    /// Build an [`IceGateStorageFactory`] that injects the shared cache
    /// and an `OpenTelemetry` meter into every `FileIO` created by catalogs.
    ///
    /// The returned factory auto-detects the storage scheme from
    /// [`StorageConfig`] properties, so callers don't need to specify it.
    pub fn storage_factory(&self) -> Arc<dyn iceberg::io::StorageFactory> {
        let meter = opentelemetry::global::meter("iceberg-storage");
        Arc::new(IceGateStorageFactory::new(
            self.cache.clone(),
            Some(meter),
            self.prefetch.clone(),
            self.stat_ttl,
            self.max_write_cache_size,
        ))
    }
}

/// Factory for creating catalog instances from configuration
pub struct CatalogBuilder;

impl CatalogBuilder {
    /// Create a catalog from configuration.
    ///
    /// The `io_cache` handle is created **before** calling this method (via
    /// [`IoHandle::from_config`]) so the same cache can be shared with
    /// other components (e.g., WAL object store) without coupling its
    /// lifecycle to catalog construction.
    ///
    /// `cancel_token` is plumbed into retry-aware catalog implementations
    /// (currently the S3 catalog) so that long-running CAS/transient retry loops
    /// abort promptly when the process is shutting down. Pass the same token your
    /// main loop uses to coordinate shutdown; one-shot callers and tests pass
    /// [`CancellationToken::new()`]. Backends that do not honour cancellation
    /// (Memory, REST, Glue, `S3Tables`) ignore the token today.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be created.
    pub async fn from_config(
        config: &CatalogConfig,
        io_cache: &IoHandle,
        cancel_token: CancellationToken,
    ) -> Result<Arc<dyn Catalog>> {
        match &config.backend {
            CatalogBackend::Memory => Self::create_memory_catalog(config).await,
            CatalogBackend::Rest { uri } => Self::create_rest_catalog(config, uri, io_cache).await,
            CatalogBackend::S3Tables { table_bucket_arn } => {
                Self::create_s3tables_catalog(config, table_bucket_arn, io_cache).await
            }
            CatalogBackend::Glue { catalog_id } => {
                Self::create_glue_catalog(config, catalog_id.as_deref(), io_cache).await
            }
            CatalogBackend::S3 { warehouse } => {
                Self::create_s3_catalog(config, warehouse, io_cache, cancel_token).await
            }
        }
    }

    /// Create a memory catalog.
    ///
    /// The Memory catalog stores everything in memory and does not need
    /// the storage factory (no cache, metrics, or prefetch layers).
    async fn create_memory_catalog(config: &CatalogConfig) -> Result<Arc<dyn Catalog>> {
        let mut properties = HashMap::new();
        properties.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), config.warehouse.clone());

        // Add any additional properties from config
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        let catalog = MemoryCatalogBuilder::default().load("icegate", properties).await?;

        tracing::info!(
            warehouse = %config.warehouse,
            "Created Memory catalog"
        );

        Ok(Arc::new(catalog))
    }

    /// Create an S3 Tables catalog with `IceGateStorageFactory`.
    async fn create_s3tables_catalog(
        config: &CatalogConfig,
        table_bucket_arn: &str,
        io_cache: &IoHandle,
    ) -> Result<Arc<dyn Catalog>> {
        let mut properties = HashMap::new();
        properties.insert("table_bucket_arn".to_string(), table_bucket_arn.to_string());
        properties.insert("warehouse".to_string(), config.warehouse.clone());

        // Add any additional properties from config
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        let catalog = S3TablesCatalogBuilder::default()
            .with_storage_factory(io_cache.storage_factory())
            .load("s3tables", properties)
            .await?;

        tracing::info!(
            table_bucket_arn = %table_bucket_arn,
            warehouse = %config.warehouse,
            "Created S3 Tables catalog"
        );

        Ok(Arc::new(catalog))
    }

    /// Create an AWS Glue catalog with `IceGateStorageFactory`.
    async fn create_glue_catalog(
        config: &CatalogConfig,
        catalog_id: Option<&str>,
        io_cache: &IoHandle,
    ) -> Result<Arc<dyn Catalog>> {
        let mut properties = HashMap::new();
        properties.insert("warehouse".to_string(), config.warehouse.clone());

        if let Some(id) = catalog_id {
            properties.insert("catalog_id".to_string(), id.to_string());
        }

        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        let catalog = GlueCatalogBuilder::default()
            .with_storage_factory(io_cache.storage_factory())
            .load("glue", properties)
            .await?;

        tracing::info!(
            catalog_id = ?catalog_id,
            warehouse = %config.warehouse,
            "Created Glue catalog"
        );

        Ok(Arc::new(catalog))
    }

    /// Create a REST catalog with `IceGateStorageFactory`.
    ///
    /// The REST catalog **requires** a `StorageFactory` — it will error
    /// at table-load time without one.
    async fn create_rest_catalog(config: &CatalogConfig, uri: &str, io_cache: &IoHandle) -> Result<Arc<dyn Catalog>> {
        // Build REST catalog properties
        let mut properties = HashMap::new();
        properties.insert("uri".to_string(), uri.to_string());
        properties.insert("warehouse".to_string(), config.warehouse.clone());

        // Add any additional properties from config
        for (key, value) in &config.properties {
            properties.insert(key.clone(), value.clone());
        }

        // Create REST catalog with IceGateStorageFactory
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(io_cache.storage_factory())
            .load("rest", properties)
            .await?;

        tracing::info!(
            uri = %uri,
            warehouse = %config.warehouse,
            "Created REST catalog"
        );

        Ok(Arc::new(catalog))
    }

    /// Create an S3-backed catalog.
    async fn create_s3_catalog(
        config: &CatalogConfig,
        warehouse: &str,
        io_cache: &IoHandle,
        cancel_token: CancellationToken,
    ) -> Result<Arc<dyn Catalog>> {
        let bucket = config
            .properties
            .get("bucket")
            .cloned()
            .ok_or_else(|| CommonError::Config("S3 catalog requires `catalog.properties.bucket`".to_string()))?;
        let region = config
            .properties
            .get("region")
            .cloned()
            .unwrap_or_else(|| "us-east-1".to_string());
        let endpoint = config.properties.get("endpoint").cloned();
        let access_key_id = config.properties.get("access_key_id").cloned();
        let secret_access_key = config.properties.get("secret_access_key").cloned();

        match (&access_key_id, &secret_access_key) {
            (Some(_), None) => {
                return Err(CommonError::Config(
                    "S3 catalog: `access_key_id` is set but `secret_access_key` is missing".to_string(),
                ));
            }
            (None, Some(_)) => {
                return Err(CommonError::Config(
                    "S3 catalog: `secret_access_key` is set but `access_key_id` is missing".to_string(),
                ));
            }
            _ => {}
        }

        let path_style_access = config
            .properties
            .get("s3.path-style-access")
            .map(|value| {
                value.parse::<bool>().map_err(|error| {
                    CommonError::Config(format!("S3 catalog: invalid `s3.path-style-access`: {error}"))
                })
            })
            .transpose()?;
        let codec = config
            .properties
            .get("codec")
            .map_or(Ok(CatalogCodecKind::Json), |value| value.parse())
            .map_err(|e| CommonError::Config(format!("invalid s3 catalog codec: {e}")))?;

        // TODO(high): move to custom config and build fileio from custom config
        let s3_config = S3CatalogConfig {
            bucket: bucket.clone(),
            region: region.clone(),
            endpoint: endpoint.clone(),
            path_style_access,
            access_key_id,
            secret_access_key,
            warehouse: warehouse.to_string(),
            codec,
            ..S3CatalogConfig::default()
        };
        let file_io = FileIOBuilder::new(io_cache.storage_factory())
            .with_props(build_file_io_props(&config.properties, &s3_config))
            .build();

        let catalog = S3Catalog::new(s3_config, file_io, cancel_token)
            .await
            .map_err(|e| CommonError::Config(format!("failed to create s3 catalog: {e}")))?;

        if let Some(endpoint) = endpoint.as_deref() {
            tracing::info!(
                bucket = %bucket,
                region = %region,
                warehouse = %warehouse,
                endpoint = %endpoint,
                "Created S3 catalog"
            );
        } else {
            tracing::info!(
                bucket = %bucket,
                region = %region,
                warehouse = %warehouse,
                "Created S3 catalog"
            );
        }

        Ok(Arc::new(catalog))
    }
}

/// Assemble the `FileIO` properties for tables the S3 catalog returns.
///
/// Every key `s3_config` also owns is written from it rather than from
/// `properties`, so table data cannot end up addressed differently from the
/// catalog root it was found through; the remaining user properties pass through
/// untouched. The `s3.*` forms a user set directly still win, because
/// `s3_config` was itself built from them.
fn build_file_io_props(properties: &HashMap<String, String>, s3_config: &S3CatalogConfig) -> HashMap<String, String> {
    let mut file_io_props = properties.clone();
    file_io_props.insert(
        "warehouse".to_string(),
        format_warehouse_uri(&s3_config.bucket, &s3_config.warehouse),
    );
    file_io_props.insert(
        "s3.path-style-access".to_string(),
        resolve_path_style_access(s3_config.path_style_access, s3_config.endpoint.as_deref()).to_string(),
    );
    if let Some(endpoint) = s3_config.endpoint.as_ref() {
        file_io_props
            .entry("s3.endpoint".to_string())
            .or_insert_with(|| endpoint.clone());
    }
    if let (Some(key), Some(secret)) = (&s3_config.access_key_id, &s3_config.secret_access_key) {
        file_io_props
            .entry("s3.access-key-id".to_string())
            .or_insert_with(|| key.clone());
        file_io_props
            .entry("s3.secret-access-key".to_string())
            .or_insert_with(|| secret.clone());
    }
    file_io_props
        .entry("s3.region".to_string())
        .or_insert_with(|| s3_config.region.clone());
    file_io_props
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use bytes::Bytes;
    use iceberg::{NamespaceIdent, TableCreation};

    use super::*;
    use crate::error::CommonError;
    use crate::testing::container::{STORAGE_ACCESS_KEY, STORAGE_SECRET_KEY};
    use crate::testing::{S3TestContainer, create_s3_bucket};

    fn test_catalog_config(endpoint: &str, bucket: &str) -> CatalogConfig {
        CatalogConfig {
            backend: CatalogBackend::S3 {
                warehouse: "catalog".to_string(),
            },
            warehouse: "warehouse".to_string(),
            properties: HashMap::from([
                ("bucket".to_string(), bucket.to_string()),
                ("region".to_string(), "us-east-1".to_string()),
                ("endpoint".to_string(), endpoint.to_string()),
                ("access_key_id".to_string(), STORAGE_ACCESS_KEY.to_string()),
                ("secret_access_key".to_string(), STORAGE_SECRET_KEY.to_string()),
            ]),
            cache: None,
        }
    }

    fn test_schema() -> iceberg::spec::Schema {
        use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};

        Schema::builder()
            .with_schema_id(0)
            .with_identifier_field_ids(vec![1])
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema")
    }

    #[tokio::test]
    async fn s3_catalog_file_io_reads_with_explicit_credentials() {
        let store = S3TestContainer::start().await.expect("start object storage");
        let bucket = format!("catalog-{}", rand::random::<u64>());
        create_s3_bucket(store.endpoint(), &bucket).await.expect("create bucket");

        let catalog = CatalogBuilder::from_config(
            &test_catalog_config(store.endpoint(), &bucket),
            &IoHandle::noop(),
            CancellationToken::new(),
        )
        .await
        .expect("build catalog");
        let namespace = NamespaceIdent::new("ns1".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
        let table = catalog
            .create_table(
                &namespace,
                TableCreation::builder().name("tbl".to_string()).schema(test_schema()).build(),
            )
            .await
            .expect("create table");

        let data_path = format!("s3://{bucket}/warehouse/data/smoke.txt");
        let output = table.file_io().new_output(&data_path).expect("create output");
        output.write(Bytes::from_static(b"ok")).await.expect("write data file");

        let input = table.file_io().new_input(&data_path).expect("create input");
        let metadata = input.metadata().await.expect("stat data file");
        let bytes = input.read().await.expect("read data file");

        assert_eq!(metadata.size, 2);
        assert_eq!(bytes, Bytes::from_static(b"ok"));
    }

    /// The `FileIO` that reads table data and the storage that reads the catalog
    /// root are configured from one [`S3CatalogConfig`], so they must not be able
    /// to disagree about how a bucket is addressed. Without an endpoint that
    /// means virtual-hosted: path-style is what AWS deprecated, not its default.
    #[test]
    fn file_io_addresses_the_bucket_the_way_the_catalog_root_will() {
        for (endpoint, path_style_access, expected) in [
            (None, None, "false"),
            (Some("http://127.0.0.1:9000"), None, "true"),
            (None, Some(true), "true"),
            (Some("http://127.0.0.1:9000"), Some(false), "false"),
        ] {
            let s3_config = S3CatalogConfig {
                bucket: "catalog".to_string(),
                region: "us-east-1".to_string(),
                endpoint: endpoint.map(ToString::to_string),
                path_style_access,
                warehouse: "catalog".to_string(),
                ..S3CatalogConfig::default()
            };
            let properties = path_style_access
                .map(|value| HashMap::from([("s3.path-style-access".to_string(), value.to_string())]))
                .unwrap_or_default();

            let file_io_props = build_file_io_props(&properties, &s3_config);

            assert_eq!(
                file_io_props.get("s3.path-style-access").map(String::as_str),
                Some(expected),
                "endpoint {endpoint:?}, configured {path_style_access:?}",
            );
        }
    }

    /// The parse guard on `s3.path-style-access` must reject the value before
    /// any S3 client exists. The control run pins the proof: the identical
    /// configuration with a parsable value builds a catalog end to end (client
    /// construction is lazy, nothing dials S3), so swapping only that value
    /// leaves the parse guard as the sole source of the `Config` error.
    #[tokio::test]
    async fn s3_catalog_rejects_an_unparsable_path_style_access_before_touching_s3() {
        let config_with = |path_style_access: &str| CatalogConfig {
            backend: CatalogBackend::S3 {
                warehouse: "catalog".to_string(),
            },
            warehouse: "warehouse".to_string(),
            properties: HashMap::from([
                ("bucket".to_string(), "catalog".to_string()),
                ("s3.path-style-access".to_string(), path_style_access.to_string()),
            ]),
            cache: None,
        };

        CatalogBuilder::from_config(&config_with("true"), &IoHandle::noop(), CancellationToken::new())
            .await
            .expect("a parsable s3.path-style-access must build the catalog");

        let error = CatalogBuilder::from_config(&config_with("invalid"), &IoHandle::noop(), CancellationToken::new())
            .await
            .expect_err("unparsable s3.path-style-access must fail");

        assert!(matches!(error, CommonError::Config(_)), "{error}");
    }

    #[test]
    fn s3_catalog_creation_requires_bucket_at_validation_time() {
        let config = CatalogConfig {
            backend: CatalogBackend::S3 {
                warehouse: "catalog".to_string(),
            },
            warehouse: "warehouse".to_string(),
            properties: HashMap::new(),
            cache: None,
        };

        let error = config.validate().expect_err("missing bucket must fail validation");
        let CommonError::Config(message) = error else {
            panic!("unexpected error type");
        };
        assert_eq!(message, "S3 catalog requires `catalog.properties.bucket`");
    }
}
