//! IceGate's forked storage implementation with foyer caching and
//! `OpenTelemetry` observability.
//!
//! Forked from `iceberg-storage-opendal` (commit `335961ae`) to inject
//! custom `OpenDAL` layers between operator creation and use.
//!
//! # Layer stack (outermost → innermost)
//!
//! ```text
//! Prefetch → FoyerCache → OtelMetrics → OtelTrace → S3
//! ```
//!
//! - **`Prefetch`** triggers background reads for Parquet metadata/column chunks.
//! - **`FoyerCache`** short-circuits reads on cache hits.
//! - **`OtelMetrics`** records metrics for S3 round-trips (cache misses only).
//! - **`OtelTrace`** adds per-request tracing spans for S3 calls (cache misses only).

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig, StorageFactory};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::layers::{OtelMetricsLayer, OtelTraceLayer};
use opendal::services::S3Config;
use opentelemetry::metrics::Meter;
use serde::{Deserialize, Serialize};

use super::cache::{CacheLayer, CacheMetrics, StorageCache};
use super::icegate_s3::{from_opendal_error, s3_config_build, s3_config_parse};
use super::prefetch::{PrefetchConfig, PrefetchLayer, PrefetchMetrics};

// ---------------------------------------------------------------------------
// IceGateStorage
// ---------------------------------------------------------------------------

/// IceGate storage backend with observability and caching layers.
///
/// Only S3 and Memory backends are supported — IceGate does not need
/// local-filesystem, GCS, OSS, or Azure DLS variants.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IceGateStorage {
    /// In-memory storage (testing / ephemeral).
    Memory(#[serde(skip, default = "default_memory_operator")] Operator),

    /// S3-compatible storage with optional caching and metrics.
    S3 {
        /// URL scheme (`s3` or `s3a`).
        configured_scheme: String,
        /// Parsed S3 configuration.
        config: Arc<S3Config>,
        /// Shared foyer hybrid cache (if configured).
        #[serde(skip)]
        cache: Option<StorageCache>,
        /// `OpenTelemetry` meter for storage metrics (if configured).
        #[serde(skip)]
        meter: Option<Meter>,
        /// Parquet column-chunk prefetch configuration (if configured).
        #[serde(skip)]
        prefetch: Option<PrefetchConfig>,
        /// Stat metadata cache TTL (if configured).
        #[serde(skip)]
        stat_ttl: Option<Duration>,
    },
}

/// Build a default memory operator for serde deserialization fallback.
fn default_memory_operator() -> Operator {
    use opendal::services::MemoryConfig;
    #[allow(clippy::expect_used)]
    Operator::from_config(MemoryConfig::default())
        .expect("memory operator must build")
        .finish()
}

impl IceGateStorage {
    /// Build an [`Operator`] for the given absolute path, returning
    /// the operator and the relative path within the operator's root.
    ///
    /// For S3, the operator is wrapped with the full layer stack.
    /// For Memory, the bare operator is returned unchanged.
    fn create_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
        match self {
            Self::Memory(op) => {
                let relative = if let Some(rest) = path.strip_prefix("memory:/") {
                    rest.trim_start_matches('/')
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid memory URL: {path}, expected prefix memory:/"),
                    ));
                };
                Ok((op.clone(), relative))
            }
            Self::S3 {
                configured_scheme,
                config,
                cache,
                meter,
                prefetch,
                stat_ttl,
            } => {
                // Build a bare operator from the S3 config + path.
                let bare = s3_config_build(config, path)?;
                let op_info = bare.info();
                let prefix = format!("{configured_scheme}://{}/", op_info.name());

                let relative = if path.starts_with(&prefix) {
                    &path[prefix.len()..]
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid S3 URL: {path}, expected prefix {prefix}"),
                    ));
                };

                // Layer stack (applied bottom-up, executed top-down):
                //   OtelTrace → OtelMetrics → [FoyerCache] → [Prefetch]
                //
                // OtelTrace and OtelMetrics sit below the cache so they only
                // observe actual S3 round-trips (cache misses).
                //
                // `Operator::layer()` returns `Operator` directly (type-erased),
                // so no `.finish()` is needed.
                let mut operator = bare.layer(OtelTraceLayer::default());

                if let Some(m) = meter {
                    operator = operator.layer(OtelMetricsLayer::builder().register(m));
                }

                if let Some(fc) = cache {
                    let cache_metrics = meter.as_ref().map_or_else(CacheMetrics::new_disabled, CacheMetrics::new);
                    operator = operator.layer(CacheLayer::new(fc.clone(), cache_metrics, *stat_ttl));
                }

                // Prefetch layer sits outermost so background reads flow
                // through the cache layer and land in the foyer cache.
                if let Some(pf) = prefetch {
                    if pf.enabled {
                        let pf_metrics =
                            meter.as_ref().map_or_else(PrefetchMetrics::new_disabled, PrefetchMetrics::new);
                        operator = operator.layer(PrefetchLayer::new(pf.clone(), pf_metrics));
                    }
                }

                Ok((operator, relative))
            }
        }
    }
}

#[typetag::serde(name = "IceGateStorage")]
#[async_trait]
impl Storage for IceGateStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative) = self.create_operator(path)?;
        op.exists(relative).await.map_err(from_opendal_error)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, relative) = self.create_operator(path)?;
        let meta = op.stat(relative).await.map_err(from_opendal_error)?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, relative) = self.create_operator(path)?;
        let data = op.read(relative).await.map_err(from_opendal_error)?.to_bytes();
        Ok(data)
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, relative) = self.create_operator(path)?;
        let reader = op.reader(relative).await.map_err(from_opendal_error)?;
        Ok(Box::new(IceGateFileRead(reader)))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (op, relative) = self.create_operator(path)?;
        op.write(relative, bs).await.map_err(from_opendal_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, relative) = self.create_operator(path)?;
        let writer = op.writer(relative).await.map_err(from_opendal_error)?;
        Ok(Box::new(IceGateFileWrite(writer)))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative) = self.create_operator(path)?;
        op.delete(relative).await.map_err(from_opendal_error)?;
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, relative) = self.create_operator(path)?;
        let dir_path = if relative.ends_with('/') {
            relative.to_string()
        } else {
            format!("{relative}/")
        };
        op.delete_with(&dir_path).recursive(true).await.map_err(from_opendal_error)?;
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Newtype wrappers for FileRead / FileWrite
// ---------------------------------------------------------------------------

/// Wrapper around [`opendal::Reader`] implementing [`FileRead`].
struct IceGateFileRead(opendal::Reader);

#[async_trait]
impl FileRead for IceGateFileRead {
    async fn read(&self, range: std::ops::Range<u64>) -> Result<Bytes> {
        Ok(opendal::Reader::read(&self.0, range)
            .await
            .map_err(from_opendal_error)?
            .to_bytes())
    }
}

/// Wrapper around [`opendal::Writer`] implementing [`FileWrite`].
struct IceGateFileWrite(opendal::Writer);

#[async_trait]
impl FileWrite for IceGateFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        opendal::Writer::write(&mut self.0, bs).await.map_err(from_opendal_error)?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        opendal::Writer::close(&mut self.0).await.map_err(from_opendal_error)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// IceGateStorageFactory
// ---------------------------------------------------------------------------

/// Factory that produces [`IceGateStorage`] instances from Iceberg
/// [`StorageConfig`] properties.
///
/// Injected into catalog builders via
/// [`CatalogBuilder::with_storage_factory`](iceberg::CatalogBuilder::with_storage_factory)
/// so every `FileIO` created by the catalog uses IceGate's layered operator.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IceGateStorageFactory {
    configured_scheme: String,
    #[serde(skip)]
    cache: Option<StorageCache>,
    #[serde(skip)]
    meter: Option<Meter>,
    #[serde(skip)]
    prefetch: Option<PrefetchConfig>,
    #[serde(skip)]
    stat_ttl: Option<Duration>,
}

impl IceGateStorageFactory {
    /// Create a new factory.
    ///
    /// # Arguments
    ///
    /// * `scheme` — URL scheme (typically `"s3"` or `"s3a"`)
    /// * `cache` — optional shared foyer cache
    /// * `meter` — optional `OpenTelemetry` meter for storage metrics
    /// * `prefetch` — optional Parquet column-chunk prefetch configuration
    /// * `stat_ttl` — optional TTL for stat metadata caching
    pub const fn new(
        scheme: String,
        cache: Option<StorageCache>,
        meter: Option<Meter>,
        prefetch: Option<PrefetchConfig>,
        stat_ttl: Option<Duration>,
    ) -> Self {
        Self {
            configured_scheme: scheme,
            cache,
            meter,
            prefetch,
            stat_ttl,
        }
    }
}

#[typetag::serde(name = "IceGateStorageFactory")]
impl StorageFactory for IceGateStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(IceGateStorage::S3 {
            configured_scheme: self.configured_scheme.clone(),
            config: s3_config_parse(config.props().clone())?.into(),
            cache: self.cache.clone(),
            meter: self.meter.clone(),
            prefetch: self.prefetch.clone(),
            stat_ttl: self.stat_ttl,
        }))
    }
}
