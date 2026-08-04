//! IceGate's forked storage implementation with foyer caching and
//! `OpenTelemetry` observability.
//!
//! Forked from `iceberg-storage-opendal` (commit `335961ae`) to inject
//! custom `OpenDAL` layers between operator creation and use. The layer stack
//! itself lives in the `layers` module and the operators carrying it in the
//! `registry` module, both shared with the WAL object store.
//!
//! Operators are built **once** per configuration and bucket and reused by
//! every storage operation afterwards (see [`OperatorRegistry`]): an operator
//! that carries the metrics layer is retained for the lifetime of the process,
//! so building one per request grows memory without bound.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig, StorageFactory};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use opendal::services::S3Config;
use serde::{Deserialize, Serialize};

use super::icegate_s3::{from_opendal_error, parse_s3_bucket, s3_config_parse};
use super::layers::StorageLayersConfig;
use super::registry::{OperatorRegistry, S3ConfigKey};

/// Build an empty `OperatorRegistry` for serde deserialization fallback.
///
/// Like [`default_memory_operator`], this yields a storage that carries no
/// credentials — a deserialized [`IceGateStorage`] cannot reach S3 and is
/// only meaningful once a factory rebuilds it.
fn default_operator_registry() -> Arc<OperatorRegistry> {
    Arc::new(OperatorRegistry::new(&StorageLayersConfig::default(), None))
}

/// Build an empty S3 configuration key for serde deserialization fallback.
fn default_s3_config_key() -> S3ConfigKey {
    S3ConfigKey::new(S3Config::default())
}

/// Build a default memory operator for serde deserialization fallback.
fn default_memory_operator() -> Operator {
    use opendal::services::MemoryConfig;
    #[allow(clippy::expect_used)]
    Operator::from_config(MemoryConfig::default())
        .expect("memory operator must build")
        .finish()
}

// ---------------------------------------------------------------------------
// IceGateStorage
// ---------------------------------------------------------------------------

/// IceGate storage backend with observability and caching layers.
///
/// Supports S3, Memory, and local Filesystem backends.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum IceGateStorage {
    /// In-memory storage (testing / ephemeral).
    Memory(#[serde(skip, default = "default_memory_operator")] Operator),

    /// Local filesystem storage (development / testing).
    ///
    /// Uses a bare `OpenDAL` `Fs` operator without cache, metrics, or
    /// prefetch layers — local disk doesn't need them.
    Fs {
        /// Root directory for the filesystem operator.
        root: String,
    },

    /// S3-compatible storage with optional caching and metrics.
    S3 {
        /// URL scheme (`s3` or `s3a`).
        configured_scheme: String,
        /// S3 configuration this storage signs with. Belongs to the storage
        /// and not to the registry: a catalog that vends per-table
        /// credentials builds one `FileIO`, and one storage, per set of them
        /// (skipped during serialization: it holds credentials).
        #[serde(skip, default = "default_s3_config_key")]
        #[allow(private_interfaces)]
        config: S3ConfigKey,
        /// Layers and operators shared with every other storage the same
        /// factory produced (skipped during serialization: it holds
        /// credentials and live layer state).
        #[serde(skip, default = "default_operator_registry")]
        registry: Arc<OperatorRegistry>,
    },
}

impl IceGateStorage {
    /// Return the [`Operator`] serving `path` together with the path
    /// relative to that operator's root.
    ///
    /// For S3 the operator comes from the [`OperatorRegistry`] with the
    /// full layer stack already applied. For Memory the bare operator is
    /// returned unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::DataInvalid`] if `path` does not belong to
    /// this storage backend.
    fn resolve_operator<'a>(&self, path: &'a str) -> Result<(Operator, &'a str)> {
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
            Self::Fs { root } => {
                // Accept both `file:///path/to/file` and bare `/path/to/file`.
                let absolute = path.strip_prefix("file://").unwrap_or(path);
                // Use Path::strip_prefix for component-boundary-safe matching
                // (prevents `/tmp/warehouse-backup` matching root `/tmp/warehouse`).
                let abs_path = std::path::Path::new(absolute);
                let root_path = std::path::Path::new(root.trim_end_matches('/'));
                let relative = match abs_path.strip_prefix(root_path) {
                    Ok(p) => p.to_str().unwrap_or(absolute),
                    Err(_) => {
                        return Err(Error::new(
                            ErrorKind::DataInvalid,
                            format!("Path {absolute} is outside root {root}"),
                        ));
                    }
                };
                let mut fs_config = opendal::services::FsConfig::default();
                fs_config.root = Some(root.clone());
                let op = Operator::from_config(fs_config)
                    .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?
                    .finish();
                Ok((op, relative))
            }
            Self::S3 {
                configured_scheme,
                config,
                registry,
            } => {
                let bucket = parse_s3_bucket(path)?;
                let prefix = format!("{configured_scheme}://{bucket}/");

                // Accept both s3:// and s3a:// regardless of which scheme was
                // used when the storage was configured.
                let alt_scheme = match configured_scheme.as_str() {
                    "s3" => "s3a",
                    "s3a" => "s3",
                    other => other,
                };
                let alt_prefix = format!("{alt_scheme}://{bucket}/");

                let relative = if path.starts_with(&prefix) {
                    &path[prefix.len()..]
                } else if path.starts_with(&alt_prefix) {
                    &path[alt_prefix.len()..]
                } else if path == prefix.trim_end_matches('/') || path == alt_prefix.trim_end_matches('/') {
                    // Exact bucket root (no trailing slash) → empty relative path.
                    ""
                } else {
                    return Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid S3 URL: {path}, expected prefix {prefix} or {alt_prefix}"),
                    ));
                };

                Ok((registry.resolve_operator(config, &bucket)?, relative))
            }
        }
    }
}

#[typetag::serde(name = "IceGateStorage")]
#[async_trait]
impl Storage for IceGateStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (op, relative) = self.resolve_operator(path)?;
        op.exists(relative).await.map_err(from_opendal_error)
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (op, relative) = self.resolve_operator(path)?;
        let meta = op.stat(relative).await.map_err(from_opendal_error)?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (op, relative) = self.resolve_operator(path)?;
        let data = op.read(relative).await.map_err(from_opendal_error)?.to_bytes();
        Ok(data)
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (op, relative) = self.resolve_operator(path)?;
        let reader = op.reader(relative).await.map_err(from_opendal_error)?;
        Ok(Box::new(IceGateFileRead(reader)))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (op, relative) = self.resolve_operator(path)?;
        op.write(relative, bs).await.map_err(from_opendal_error)?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (op, relative) = self.resolve_operator(path)?;
        let writer = op.writer(relative).await.map_err(from_opendal_error)?;
        Ok(Box::new(IceGateFileWrite(writer)))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (op, relative) = self.resolve_operator(path)?;
        op.delete(relative).await.map_err(from_opendal_error)?;
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (op, relative) = self.resolve_operator(path)?;
        let dir_path = if relative.ends_with('/') {
            relative.to_string()
        } else {
            format!("{relative}/")
        };
        op.remove_all(&dir_path).await.map_err(from_opendal_error)?;
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
///
/// The factory auto-detects the storage scheme from [`StorageConfig`]
/// properties (warehouse URI), producing `Memory`, `Fs`, or `S3` variants
/// accordingly.
///
/// A factory outlives every `FileIO` it serves (it is created once when the
/// catalog is built), which is why it — and not the storage — owns the
/// [`OperatorRegistry`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IceGateStorageFactory {
    #[serde(skip)]
    layers: StorageLayersConfig,
    /// Registry shared by every S3 storage this factory builds, and by
    /// every clone of the factory. Built on the first S3 `build()` rather
    /// than in [`Self::new`] so that a factory serving only Memory or Fs
    /// warehouses registers no instruments and spawns no sweep task.
    #[serde(skip)]
    registry: Arc<OnceLock<Arc<OperatorRegistry>>>,
}

impl IceGateStorageFactory {
    /// Create a factory whose storages carry the layers `layers` describes.
    #[must_use]
    pub fn new(layers: StorageLayersConfig) -> Self {
        Self {
            layers,
            registry: Arc::new(OnceLock::new()),
        }
    }

    /// Return this factory's operator registry, building it on first use.
    ///
    /// [`OnceLock::get_or_init`] runs the closure exactly once, so every
    /// storage this factory produces shares one set of layers however many
    /// threads ask for it at the same time.
    fn resolve_registry(&self) -> Arc<OperatorRegistry> {
        self.registry
            .get_or_init(|| {
                // No backend: an Iceberg storage carries the S3 configuration
                // its `FileIO` was built with, and never resolves by path.
                Arc::new(OperatorRegistry::new(&self.layers, None))
            })
            .clone()
    }

    /// Detect the storage scheme from config properties.
    ///
    /// Inspects the `warehouse` property to determine the URI scheme.
    /// Falls back to `"s3"` if no warehouse is set.
    fn detect_scheme(config: &StorageConfig) -> &str {
        let props = config.props();
        let warehouse = props.get("warehouse").map_or("", String::as_str);
        if warehouse.starts_with("memory://") || warehouse.starts_with("memory:/") {
            "memory"
        } else if warehouse.starts_with("file://") || warehouse.starts_with('/') {
            // Bare absolute paths (e.g. `/tmp/warehouse`) are treated as
            // local filesystem — common in tests and the Memory catalog.
            "file"
        } else if warehouse.starts_with("s3a://") {
            "s3a"
        } else {
            // Default to S3 for s3://, empty, or unknown schemes.
            "s3"
        }
    }
}

#[typetag::serde(name = "IceGateStorageFactory")]
impl StorageFactory for IceGateStorageFactory {
    fn build(&self, config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        match Self::detect_scheme(config) {
            "memory" => Ok(Arc::new(IceGateStorage::Memory(default_memory_operator()))),
            "file" => {
                let warehouse = config.props().get("warehouse").cloned().unwrap_or_default();
                let root = warehouse.strip_prefix("file://").unwrap_or(&warehouse).to_string();
                Ok(Arc::new(IceGateStorage::Fs { root }))
            }
            scheme => Ok(Arc::new(IceGateStorage::S3 {
                configured_scheme: scheme.to_string(),
                config: S3ConfigKey::new(s3_config_parse(config.props().clone())?),
                registry: self.resolve_registry(),
            })),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::io::{
        S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT, S3_REGION,
        S3_SECRET_ACCESS_KEY,
    };
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::super::cache::{CacheKey, CacheValue};
    use super::super::prefetch::PrefetchConfig;
    use super::*;
    use crate::storage::StorageCache;

    /// Storage properties for a local S3-compatible endpoint.
    ///
    /// Credential and metadata lookups are switched off so operator
    /// construction stays hermetic — no test here performs any I/O.
    fn s3_props() -> HashMap<String, String> {
        HashMap::from([
            ("warehouse".to_string(), "s3://bucket/wh".to_string()),
            (S3_ENDPOINT.to_string(), "http://localhost:9000".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
            (S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string()),
            (S3_DISABLE_EC2_METADATA.to_string(), "true".to_string()),
            (S3_ALLOW_ANONYMOUS.to_string(), "true".to_string()),
        ])
    }

    /// The same endpoint addressed with the credentials a catalog would vend
    /// for one table.
    fn vended_s3_props(access_key_id: &str) -> HashMap<String, String> {
        let mut props = s3_props();
        props.remove(S3_ALLOW_ANONYMOUS);
        props.insert(S3_ACCESS_KEY_ID.to_string(), access_key_id.to_string());
        props.insert(S3_SECRET_ACCESS_KEY.to_string(), "secret".to_string());
        props
    }

    fn s3_factory(prefetch: Option<PrefetchConfig>) -> IceGateStorageFactory {
        IceGateStorageFactory::new(StorageLayersConfig {
            prefetch,
            ..StorageLayersConfig::default()
        })
    }

    /// A factory wired the way production wires it: a meter to register
    /// instruments against and a foyer cache to share.
    async fn observed_s3_factory() -> IceGateStorageFactory {
        let meter = SdkMeterProvider::builder().build().meter("icegate-storage-test");
        // No test here reads or writes, so entry weight is irrelevant: the
        // cache exists to make the factory build the cache layer.
        let cache: StorageCache = foyer::HybridCacheBuilder::new()
            .memory(64 * 1024)
            .with_weighter(|_key: &CacheKey, _value: &CacheValue| 1)
            .storage()
            .build()
            .await
            .expect("test cache");

        IceGateStorageFactory::new(StorageLayersConfig {
            cache: Some(cache),
            meter: Some(meter),
            ..StorageLayersConfig::default()
        })
    }

    /// Build an S3 storage the way [`StorageFactory::build`] does, so the
    /// wiring under test is the production one.
    fn s3_storage(
        factory: &IceGateStorageFactory,
        configured_scheme: &str,
        props: HashMap<String, String>,
    ) -> IceGateStorage {
        IceGateStorage::S3 {
            configured_scheme: configured_scheme.to_string(),
            config: S3ConfigKey::new(s3_config_parse(props).expect("s3 configuration must parse")),
            registry: factory.resolve_registry(),
        }
    }

    fn storage_registry(storage: &IceGateStorage) -> &Arc<OperatorRegistry> {
        match storage {
            IceGateStorage::S3 { registry, .. } => registry,
            IceGateStorage::Memory(_) | IceGateStorage::Fs { .. } => panic!("expected an S3 storage"),
        }
    }

    /// How many operators the registry has built and kept.
    fn cached_operator_count(storage: &IceGateStorage) -> usize {
        storage_registry(storage).operator_count()
    }

    /// Two operators are the same instance when they share one accessor;
    /// a freshly built operator always carries its own.
    fn is_same_operator(left: &Operator, right: &Operator) -> bool {
        Arc::ptr_eq(left.inner(), right.inner())
    }

    #[tokio::test]
    async fn repeated_paths_reuse_a_single_operator() {
        let factory = s3_factory(None);
        let storage = s3_storage(&factory, "s3", s3_props());

        let (first, _) = storage.resolve_operator("s3://bucket/a/b").expect("first operator");
        for _ in 0..100 {
            let (operator, relative) = storage.resolve_operator("s3://bucket/a/b").expect("operator");
            assert_eq!(relative, "a/b");
            assert!(is_same_operator(&first, &operator), "operator was rebuilt");
        }

        assert_eq!(cached_operator_count(&storage), 1);
    }

    /// The regression test for the leak this module exists to avoid: an
    /// operator carrying the metrics layer is never freed (see
    /// the `layers` module), so the hot path must build one per
    /// configuration and bucket and no more. Exercises the branches a factory
    /// without a meter and cache leaves out.
    #[tokio::test]
    async fn a_configured_meter_and_cache_do_not_make_the_hot_path_rebuild() {
        let factory = observed_s3_factory().await;
        let storage = s3_storage(&factory, "s3", s3_props());

        let registry = storage_registry(&storage);
        assert!(registry.layers().has_metrics(), "metrics layer must be built");
        assert!(registry.layers().cache().is_some(), "cache layer must be built");

        let (first, _) = storage.resolve_operator("s3://bucket/a/b").expect("first operator");
        for _ in 0..100 {
            let (operator, _) = storage.resolve_operator("s3://bucket/a/b").expect("operator");
            assert!(is_same_operator(&first, &operator), "operator was rebuilt");
        }

        assert_eq!(cached_operator_count(&storage), 1);
    }

    /// A catalog that vends per-table credentials builds one storage per set
    /// of them: the credentials a storage carries reach the registry, so no
    /// storage is served an operator built from another's.
    #[tokio::test]
    async fn a_storage_is_served_the_operator_of_its_own_credentials() {
        let factory = s3_factory(None);
        let first = s3_storage(&factory, "s3", vended_s3_props("key-one"));
        let second = s3_storage(&factory, "s3", vended_s3_props("key-two"));

        let (first_operator, _) = first.resolve_operator("s3://bucket/a").expect("first operator");
        let (second_operator, _) = second.resolve_operator("s3://bucket/a").expect("second operator");

        assert!(
            !is_same_operator(&first_operator, &second_operator),
            "one bucket addressed with two configurations must not share an operator"
        );
        assert!(
            Arc::ptr_eq(storage_registry(&first), storage_registry(&second)),
            "the layers stay shared: only the operators are per configuration"
        );
        assert_eq!(cached_operator_count(&first), 2);
    }

    #[tokio::test]
    async fn s3_paths_map_to_bucket_relative_paths() {
        let factory = s3_factory(None);

        // (configured scheme, path, expected relative path)
        let cases = [
            ("s3", "s3://bucket/a/b", "a/b"),
            ("s3", "s3a://bucket/a/b", "a/b"),
            ("s3a", "s3://bucket/a/b", "a/b"),
            ("s3a", "s3a://bucket/a/b", "a/b"),
            // Exact bucket root, no trailing slash.
            ("s3", "s3://bucket", ""),
            ("s3", "s3a://bucket", ""),
            ("s3", "s3://bucket/", ""),
        ];

        for (configured_scheme, path, expected) in cases {
            let storage = s3_storage(&factory, configured_scheme, s3_props());
            let (_, relative) = storage
                .resolve_operator(path)
                .unwrap_or_else(|e| panic!("{configured_scheme} + {path}: {e}"));
            assert_eq!(relative, expected, "{configured_scheme} + {path}");
        }
    }

    #[tokio::test]
    async fn non_s3_paths_are_rejected() {
        let factory = s3_factory(None);
        let storage = s3_storage(&factory, "s3", s3_props());

        // (path, why it is rejected)
        let cases = [
            ("gs://bucket/a/b", "foreign scheme"),
            ("file:///a/b", "no bucket in the URL"),
            ("/a/b", "not a URL at all"),
        ];

        for (path, reason) in cases {
            let error = storage.resolve_operator(path).expect_err(reason);
            assert_eq!(error.kind(), ErrorKind::DataInvalid, "{reason}");
        }
        assert_eq!(cached_operator_count(&storage), 0);
    }

    /// Prefetch deduplicates across storages only if they share the layer,
    /// which they do by sharing the registry. That the layer itself keeps
    /// its state when cloned into each operator is covered by
    /// `prefetch::tests::cloned_layer_shares_deduplication_state`.
    #[tokio::test]
    async fn storages_of_one_factory_share_the_registry() {
        let prefetch = PrefetchConfig {
            enabled: true,
            ..Default::default()
        };
        let factory = s3_factory(Some(prefetch));

        let first = s3_storage(&factory, "s3", s3_props());
        let second = s3_storage(&factory, "s3", s3_props());

        let registry = storage_registry(&first);
        assert!(Arc::ptr_eq(registry, storage_registry(&second)));
        assert!(registry.layers().has_prefetch(), "prefetch layer must be built once");
    }

    #[tokio::test]
    async fn disabled_prefetch_builds_no_layer() {
        let factory = s3_factory(Some(PrefetchConfig::default()));
        let storage = s3_storage(&factory, "s3", s3_props());

        assert!(!storage_registry(&storage).layers().has_prefetch());
    }
}
