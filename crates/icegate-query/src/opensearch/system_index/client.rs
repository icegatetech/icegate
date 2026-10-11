//! The object-store client of the system-index store, and the checks that gate its use.
//!
//! The store builds its own `object_store` client from the storage backend's S3 settings instead
//! of resolving one through the shared IO handle: that handle's read cache keys entries by path
//! and forwards neither a read's nor a HEAD's conditions, so a replica reading the pointer through
//! it would keep serving the first pointer it cached.

use std::sync::Arc;

use bytes::Bytes;
use iceberg::{Catalog, NamespaceIdent};
use icegate_common::{ICEGATE_NAMESPACE, StorageBackend, strip_s3_scheme};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, ObjectStoreExt as _, PutMode, PutOptions, PutResult, UpdateVersion};
use uuid::Uuid;

use crate::QueryConfig;
use crate::error::{QueryError, Result};
use crate::opensearch::SystemIndicesConfig;

/// Path segment holding the startup probe objects. A tenant identifier never contains `.`, so no
/// tenant's objects share it.
const PROBE_SEGMENT: &str = ".startup-probe";
/// The probe's create-if-absent, as a startup refusal names it.
const PROBE_CREATE: &str = "conditional create (PutMode::Create)";
/// The probe's update conditioned on the created object's `ETag`, as a startup refusal names it.
const PROBE_UPDATE: &str = "ETag compare-and-swap (PutMode::Update)";
/// The probe's read conditioned on the current `ETag`, as a startup refusal names it.
const PROBE_CONDITIONAL_GET: &str = "conditional GET (If-None-Match)";

/// The object store and location the system-index store reads and writes.
///
/// Cloning shares the client, so every listener of one process built from one value addresses the
/// same objects.
#[derive(Debug, Clone)]
pub struct SystemObjectStore {
    object_store: Arc<dyn ObjectStore>,
    location: String,
    base_path: Path,
}

impl SystemObjectStore {
    /// Wrap `object_store` as the client of the store at `location`.
    ///
    /// `object_store` must already address the bucket `location` names; the location's key prefix
    /// becomes the base every tenant's objects live under.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Config`] when `location` is not an `s3://` or `s3a://` URL naming a
    /// bucket, or its key prefix holds an empty or invalid segment.
    pub fn new(object_store: Arc<dyn ObjectStore>, location: &str) -> Result<Self> {
        let s3_location = parse_s3_location(location)?;
        Ok(Self {
            object_store,
            location: location.to_string(),
            base_path: s3_location.prefix,
        })
    }

    /// The location as configured, or as resolved under the storage bucket when none is.
    #[must_use]
    pub fn location(&self) -> &str {
        &self.location
    }
}

/// A system-indices location split into the bucket a client addresses and the key prefix under it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct S3Location {
    /// The bucket the URL names.
    pub(crate) bucket: String,
    /// The key prefix under the bucket; the root when the URL names the bucket alone.
    pub(crate) prefix: Path,
}

/// Split an `s3://` or `s3a://` URL into its bucket and key prefix. A trailing `/` is ignored.
///
/// # Errors
///
/// Returns [`QueryError::Config`] when no S3 scheme matches, the bucket is empty, or the prefix
/// holds an empty or invalid segment.
pub(crate) fn parse_s3_location(location: &str) -> Result<S3Location> {
    let bucket_and_prefix = strip_s3_scheme(location).ok_or_else(|| build_non_s3_location_refusal(location))?;
    let (bucket, key_prefix) = bucket_and_prefix.split_once('/').unwrap_or((bucket_and_prefix, ""));
    if bucket.is_empty() {
        return Err(QueryError::Config(format!(
            "opensearch.system_indices.location `{location}` names no bucket"
        )));
    }
    // `Path::parse` strips a leading `/`, so without this check the store would address a
    // different prefix from the one the overlap rule compares as text.
    if key_prefix.starts_with('/') {
        return Err(QueryError::Config(format!(
            "opensearch.system_indices.location `{location}` has an empty segment in its key prefix"
        )));
    }
    let prefix = Path::parse(key_prefix.trim_end_matches('/')).map_err(|error| {
        QueryError::Config(format!(
            "opensearch.system_indices.location `{location}` has an invalid key prefix: {error}"
        ))
    })?;
    Ok(S3Location {
        bucket: bucket.to_string(),
        prefix,
    })
}

/// The refusal of a system-indices `location` that is not an `s3://` or `s3a://` URL.
fn build_non_s3_location_refusal(location: &str) -> QueryError {
    QueryError::Config(format!(
        "opensearch.system_indices.location `{location}` must be an s3:// or s3a:// URL"
    ))
}

/// The location the system-index store uses: the configured one, else
/// [`SystemIndicesConfig::DEFAULT_LOCATION_PREFIX`] under the storage bucket.
///
/// # Errors
///
/// Returns [`QueryError::Config`] when no location is configured and the storage backend is not
/// S3: the store needs the conditional writes only an S3 location provides here.
pub(crate) fn resolve_system_indices_location(config: &QueryConfig) -> Result<String> {
    if let Some(location) = &config.opensearch.system_indices.location {
        return Ok(location.clone());
    }
    match &config.storage.backend {
        StorageBackend::S3(s3) => Ok(format!(
            "s3://{}/{}",
            s3.bucket,
            SystemIndicesConfig::DEFAULT_LOCATION_PREFIX
        )),
        StorageBackend::Memory | StorageBackend::FileSystem { .. } => Err(QueryError::Config(
            "opensearch.system_indices.location must be set when storage.backend is not s3".to_string(),
        )),
    }
}

/// Build the system-index store's client from `config`.
///
/// The client addresses the resolved location's bucket with the storage backend's S3 settings:
/// its region and endpoint (plain HTTP allowed for an `http://` endpoint, path-style addressing
/// whenever an endpoint is set, as the S3 catalog addresses one), and its access keys when both
/// are set; the ambient `AWS_*` environment otherwise. Building sends no request: the listener's
/// startup checks are the first.
///
/// # Errors
///
/// Returns [`QueryError::Config`] when the location cannot be resolved or parsed, or the client
/// cannot be built from the settings.
pub fn build_system_object_store(config: &QueryConfig) -> Result<SystemObjectStore> {
    let location = resolve_system_indices_location(config)?;
    let s3_location = parse_s3_location(&location)?;
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(s3_location.bucket);
    if let StorageBackend::S3(s3) = &config.storage.backend {
        builder = builder
            .with_region(&s3.region)
            .with_virtual_hosted_style_request(s3.endpoint.is_none());
        if let Some(endpoint) = &s3.endpoint {
            builder = builder.with_endpoint(endpoint).with_allow_http(endpoint.starts_with("http://"));
        }
        if let (Some(access_key_id), Some(secret_access_key)) = (&s3.access_key_id, &s3.secret_access_key) {
            builder = builder
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
        }
    }
    let object_store = builder.build().map_err(|error| {
        QueryError::Config(format!(
            "opensearch.system_indices: cannot build the S3 client for `{location}`: {error}"
        ))
    })?;
    // `new` parses the location a second time: the client needed the bucket before it existed,
    // and `new` stays the one constructor that derives the base path from a location.
    SystemObjectStore::new(Arc::new(object_store), &location)
}

/// Refuse a system-indices `location` equal to, inside or containing one of `protected_locations`.
///
/// Locations compare as `bucket/key` with the scheme (`s3://` or `s3a://`) and trailing `/`
/// removed, by whole path segments: `s3://b/opensearch` does not contain `s3://b/opensearch-system`.
/// A protected location that is not an S3 URL addresses no object the store could touch and is
/// skipped.
///
/// # Errors
///
/// Returns [`QueryError::Config`] naming the protected location `location` overlaps, or naming
/// `location` itself when it is not an S3 URL.
pub(crate) fn refuse_overlapping_locations(location: &str, protected_locations: &[&str]) -> Result<()> {
    let system_key = normalize_s3_location(location).ok_or_else(|| build_non_s3_location_refusal(location))?;
    for protected_location in protected_locations {
        let Some(protected_key) = normalize_s3_location(protected_location) else {
            continue;
        };
        if is_same_or_nested(system_key, protected_key) || is_same_or_nested(protected_key, system_key) {
            return Err(QueryError::Config(format!(
                "opensearch.system_indices.location `{location}` overlaps `{protected_location}`: objects under \
                 one would be read, listed or deleted as the other's"
            )));
        }
    }
    Ok(())
}

/// `location` as `bucket/key` without its S3 scheme and trailing `/`; `None` when it is not an S3
/// URL.
fn normalize_s3_location(location: &str) -> Option<&str> {
    strip_s3_scheme(location).map(|bucket_and_prefix| bucket_and_prefix.trim_end_matches('/'))
}

/// Whether `candidate_key` equals `container_key` or lies under it, by whole path segments.
fn is_same_or_nested(candidate_key: &str, container_key: &str) -> bool {
    candidate_key == container_key
        || candidate_key
            .strip_prefix(container_key)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// Refuse a store whose location overlaps the location of a table in the IceGate namespace.
///
/// Runs once, at listener startup, through the catalog the engine reads: the configuration knows
/// no table location. A namespace that does not exist yet holds no table to overlap.
///
/// # Errors
///
/// Returns [`QueryError::Config`] naming the overlapped table location, and the catalog's error
/// when listing or loading a table fails.
pub(crate) async fn refuse_table_location_overlap(store: &SystemObjectStore, catalog: &dyn Catalog) -> Result<()> {
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    if !catalog.namespace_exists(&namespace).await? {
        return Ok(());
    }
    for table_ident in catalog.list_tables(&namespace).await? {
        let table = catalog.load_table(&table_ident).await?;
        refuse_overlapping_locations(&store.location, &[table.metadata().location()])?;
    }
    Ok(())
}

/// Probe, under the store's location, the three operations the store relies on: a create-if-absent,
/// an update conditioned on the `ETag` the create returned, and a GET conditioned on
/// `If-None-Match` with the current `ETag`, which must answer not-modified.
///
/// The probe writes a fresh key per call, so replicas starting together never race on one object.
/// Once the create succeeds, the probe object is deleted whatever the later steps answer; a failed
/// delete only leaves that one small object behind and is logged.
///
/// # Errors
///
/// Returns [`QueryError::Config`] naming the operation that failed or answered outside its
/// contract.
pub(crate) async fn probe_conditional_operations(store: &SystemObjectStore) -> Result<()> {
    let probe_path = store.base_path.clone().join(PROBE_SEGMENT).join(Uuid::new_v4().to_string());
    let create_result = store
        .object_store
        .put_opts(
            &probe_path,
            Bytes::from_static(b"{\"probe\":1}").into(),
            PutMode::Create.into(),
        )
        .await
        .map_err(|error| build_probe_refusal(store, PROBE_CREATE, &error.to_string()))?;
    // The object exists from here on. Returning before the delete would leave one more object
    // behind on every restart against a store that refuses a later step.
    let probe_result = probe_update_and_conditional_get(store, &probe_path, create_result).await;
    delete_probe_object(store, &probe_path).await;
    probe_result
}

/// Probe the `ETag` compare-and-swap of the object at `probe_path`, whose create answered
/// `create_result`, then the conditional GET with the `ETag` the update answered.
///
/// # Errors
///
/// Returns [`QueryError::Config`] naming the operation that failed or answered outside its
/// contract, the create included when its answer carried no `ETag`.
async fn probe_update_and_conditional_get(
    store: &SystemObjectStore,
    probe_path: &Path,
    create_result: PutResult,
) -> Result<()> {
    let created_etag = create_result
        .e_tag
        .ok_or_else(|| build_probe_refusal(store, PROBE_CREATE, "the answer carried no ETag"))?;
    let update_options = PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: Some(created_etag),
            version: None,
        }),
        ..PutOptions::default()
    };
    let update_result = store
        .object_store
        .put_opts(probe_path, Bytes::from_static(b"{\"probe\":2}").into(), update_options)
        .await
        .map_err(|error| build_probe_refusal(store, PROBE_UPDATE, &error.to_string()))?;
    let updated_etag = update_result
        .e_tag
        .ok_or_else(|| build_probe_refusal(store, PROBE_UPDATE, "the answer carried no ETag"))?;
    let conditional_get_options = GetOptions {
        if_none_match: Some(updated_etag),
        ..GetOptions::default()
    };
    match store.object_store.get_opts(probe_path, conditional_get_options).await {
        Err(object_store::Error::NotModified { .. }) => Ok(()),
        Ok(_) => Err(build_probe_refusal(
            store,
            PROBE_CONDITIONAL_GET,
            "the current ETag answered the object instead of not-modified",
        )),
        Err(error) => Err(build_probe_refusal(store, PROBE_CONDITIONAL_GET, &error.to_string())),
    }
}

/// Delete the probe object at `probe_path`. A refusal is logged and not returned: the three
/// operations the store relies on are probed either way, and the cost is one small object.
async fn delete_probe_object(store: &SystemObjectStore, probe_path: &Path) {
    match store.object_store.delete(probe_path).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
        Err(error) => {
            tracing::warn!(location = %store.location, %error, "the startup probe object could not be deleted");
        }
    }
}

/// The startup refusal for an `operation` the store at `store`'s location does not support.
fn build_probe_refusal(store: &SystemObjectStore, operation: &str, cause: &str) -> QueryError {
    QueryError::Config(format!(
        "opensearch.system_indices: the object store at `{}` does not support {operation}: {cause}",
        store.location
    ))
}

#[cfg(test)]
mod tests {
    use std::fmt;

    use async_trait::async_trait;
    use futures::stream::{BoxStream, StreamExt as _, TryStreamExt as _};
    use icegate_common::{LOGS_TABLE, S3Config, SPANS_TABLE, StorageConfig};
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, PutMultipartOptions, PutPayload,
    };

    use super::*;
    use crate::test_support::{
        build_catalog_with_logs_and_spans_tables_at, build_catalog_with_logs_table_at, build_catalog_without_namespace,
    };

    const SYSTEM_LOCATION: &str = "s3://warehouse/opensearch-system";
    const LOGS_LOCATION: &str = "s3://warehouse/tables/logs";
    const SPANS_LOCATION: &str = "s3://warehouse/tables/spans";

    fn build_s3_query_config(bucket: &str) -> QueryConfig {
        QueryConfig {
            storage: StorageConfig {
                backend: StorageBackend::S3(S3Config {
                    bucket: bucket.to_string(),
                    region: "us-east-1".to_string(),
                    endpoint: None,
                    access_key_id: None,
                    secret_access_key: None,
                }),
            },
            ..QueryConfig::default()
        }
    }

    #[test]
    fn a_location_inside_containing_or_equal_to_a_protected_location_is_refused() {
        let refused_locations = [
            "s3://warehouse/opensearch-system",
            "s3a://warehouse/opensearch-system/",
            "s3://warehouse/opensearch-system/tables",
            "s3://warehouse",
            "s3://warehouse/",
        ];
        for protected_location in refused_locations {
            let result = refuse_overlapping_locations(SYSTEM_LOCATION, &[protected_location]);

            assert!(
                matches!(&result, Err(QueryError::Config(message)) if message.contains(protected_location)),
                "{protected_location}: {result:?}"
            );
        }
        let accepted_locations = [
            "s3://warehouse/opensearch",
            "s3://warehouse/opensearch-system-2",
            "s3://other/opensearch-system",
            "/var/data/opensearch-system",
            "memory://warehouse",
        ];
        for protected_location in accepted_locations {
            assert!(
                refuse_overlapping_locations(SYSTEM_LOCATION, &[protected_location]).is_ok(),
                "{protected_location} does not overlap {SYSTEM_LOCATION}"
            );
        }
    }

    /// A system location that is not an S3 URL is refused before any comparison, so a protected
    /// location the text happens not to prefix cannot let it through.
    #[test]
    fn a_non_s3_system_location_is_refused_before_any_comparison() {
        for location in ["/var/data/opensearch-system", "memory://warehouse/opensearch-system"] {
            let result = refuse_overlapping_locations(location, &[]);

            assert!(
                matches!(&result, Err(QueryError::Config(message)) if message.contains(location)),
                "{location}: {result:?}"
            );
        }
    }

    #[test]
    fn the_default_location_is_the_storage_bucket_opensearch_system_prefix() {
        let config = build_s3_query_config("warehouse");

        assert_eq!(
            resolve_system_indices_location(&config).expect("an S3 storage resolves a default"),
            "s3://warehouse/opensearch-system"
        );

        let mut configured_location_config = build_s3_query_config("warehouse");
        configured_location_config.opensearch.system_indices.location = Some("s3://saved-objects/tenants".to_string());

        assert_eq!(
            resolve_system_indices_location(&configured_location_config).expect("a configured location is used as is"),
            "s3://saved-objects/tenants"
        );
    }

    #[test]
    fn a_non_s3_storage_without_a_location_is_refused() {
        let memory = QueryConfig::default();
        let filesystem = QueryConfig {
            storage: StorageConfig {
                backend: StorageBackend::FileSystem {
                    root_path: "/var/data".to_string(),
                },
            },
            ..QueryConfig::default()
        };

        assert!(matches!(
            resolve_system_indices_location(&memory),
            Err(QueryError::Config(_))
        ));
        assert!(matches!(
            resolve_system_indices_location(&filesystem),
            Err(QueryError::Config(_))
        ));
    }

    /// A configured location builds the client whatever the storage backend, since only an S3
    /// storage lends it settings; and S3 storage without an endpoint builds the client of the
    /// default location. Each store reports its location and keeps that location's key prefix.
    #[test]
    fn the_system_client_builds_from_a_configured_location_over_any_storage() {
        let mut memory_config = QueryConfig::default();
        memory_config.opensearch.system_indices.location = Some("s3://saved-objects/tenants".to_string());
        let mut filesystem_config = QueryConfig {
            storage: StorageConfig {
                backend: StorageBackend::FileSystem {
                    root_path: "/var/data".to_string(),
                },
            },
            ..QueryConfig::default()
        };
        filesystem_config.opensearch.system_indices.location = Some("s3://saved-objects/tenants".to_string());
        let cases = [
            ("memory", memory_config, "s3://saved-objects/tenants", "tenants"),
            ("filesystem", filesystem_config, "s3://saved-objects/tenants", "tenants"),
            (
                "s3 without an endpoint",
                build_s3_query_config("warehouse"),
                "s3://warehouse/opensearch-system",
                "opensearch-system",
            ),
        ];
        for (storage, config, location, base_path) in cases {
            let store = build_system_object_store(&config).expect("a client over the stated settings");

            assert_eq!(store.location(), location, "{storage}");
            assert_eq!(store.base_path.as_ref(), base_path, "{storage}");
        }
    }

    /// The storage keys reach the client only as a pair. The client builder refuses either key
    /// alone, so a lone key handed on would stop the listener instead of leaving it on the ambient
    /// credentials, as no key at all does.
    #[test]
    fn a_lone_access_key_or_secret_does_not_fail_the_client_build() {
        let lone_keys = [(Some("test-access-key"), None), (None, Some("test-secret-key"))];
        for (access_key_id, secret_access_key) in lone_keys {
            let mut lone_key_builder = AmazonS3Builder::new().with_bucket_name("warehouse");
            if let Some(access_key_id) = access_key_id {
                lone_key_builder = lone_key_builder.with_access_key_id(access_key_id);
            }
            if let Some(secret_access_key) = secret_access_key {
                lone_key_builder = lone_key_builder.with_secret_access_key(secret_access_key);
            }
            assert!(
                matches!(lone_key_builder.build(), Err(object_store::Error::Generic { .. })),
                "the client builder must refuse {access_key_id:?}/{secret_access_key:?} alone"
            );
            let config = QueryConfig {
                storage: StorageConfig {
                    backend: StorageBackend::S3(S3Config {
                        bucket: "warehouse".to_string(),
                        region: "us-east-1".to_string(),
                        endpoint: Some("http://127.0.0.1:9000".to_string()),
                        access_key_id: access_key_id.map(str::to_string),
                        secret_access_key: secret_access_key.map(str::to_string),
                    }),
                },
                ..QueryConfig::default()
            };

            let result = build_system_object_store(&config);

            assert!(
                result.is_ok(),
                "{access_key_id:?}/{secret_access_key:?}: {:?}",
                result.err()
            );
        }
    }

    /// The one way a [`FaultyObjectStore`] departs from the conditional operations of `InMemory`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ProbeFault {
        /// Every `PutMode::Create` is refused.
        RefuseCreate,
        /// A `PutMode::Create` is applied, and its answer carries no `ETag`.
        OmitCreateEtag,
        /// Every `PutMode::Update` is refused.
        RefuseUpdate,
        /// A `PutMode::Update` is applied, and its answer carries no `ETag`.
        OmitUpdateEtag,
        /// `If-None-Match` is dropped, so the current `ETag` answers the object.
        IgnoreIfNoneMatch,
        /// A GET carrying `If-None-Match` fails with a transport error.
        FailConditionalGet,
        /// Every delete fails.
        RefuseDelete,
    }

    /// `InMemory` with one [`ProbeFault`]. Its own refusals name no probe operation, so a test
    /// reading the operation in the startup error reads the probe's label, not the double's.
    #[derive(Debug)]
    struct FaultyObjectStore {
        memory_store: InMemory,
        fault: ProbeFault,
    }

    impl FaultyObjectStore {
        /// The name the double gives itself, in its `Display` and in every error it answers.
        const STORE_NAME: &str = "FaultyObjectStore";

        fn new(fault: ProbeFault) -> Self {
            Self {
                memory_store: InMemory::new(),
                fault,
            }
        }

        /// The refusal the double answers for a faulted operation.
        fn build_refusal() -> object_store::Error {
            object_store::Error::NotImplemented {
                operation: "this request".to_string(),
                implementer: Self::STORE_NAME.to_string(),
            }
        }
    }

    impl fmt::Display for FaultyObjectStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(Self::STORE_NAME)
        }
    }

    #[async_trait]
    impl ObjectStore for FaultyObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            let is_create = matches!(opts.mode, PutMode::Create);
            let is_update = matches!(opts.mode, PutMode::Update(_));
            let is_refused = (self.fault == ProbeFault::RefuseCreate && is_create)
                || (self.fault == ProbeFault::RefuseUpdate && is_update);
            if is_refused {
                return Err(Self::build_refusal());
            }
            let put_result = self.memory_store.put_opts(location, payload, opts).await?;
            let omits_etag = (self.fault == ProbeFault::OmitCreateEtag && is_create)
                || (self.fault == ProbeFault::OmitUpdateEtag && is_update);
            Ok(if omits_etag {
                PutResult {
                    e_tag: None,
                    ..put_result
                }
            } else {
                put_result
            })
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.memory_store.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
            match self.fault {
                ProbeFault::IgnoreIfNoneMatch => {
                    let unconditional_options = GetOptions {
                        if_none_match: None,
                        ..options
                    };
                    self.memory_store.get_opts(location, unconditional_options).await
                }
                ProbeFault::FailConditionalGet if options.if_none_match.is_some() => {
                    Err(object_store::Error::Generic {
                        store: Self::STORE_NAME,
                        source: "the connection was reset".into(),
                    })
                }
                ProbeFault::FailConditionalGet
                | ProbeFault::RefuseCreate
                | ProbeFault::OmitCreateEtag
                | ProbeFault::RefuseUpdate
                | ProbeFault::OmitUpdateEtag
                | ProbeFault::RefuseDelete => self.memory_store.get_opts(location, options).await,
            }
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            if self.fault == ProbeFault::RefuseDelete {
                return locations.map(|_| Err(Self::build_refusal())).boxed();
            }
            self.memory_store.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.memory_store.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.memory_store.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
            self.memory_store.copy_opts(from, to, options).await
        }
    }

    fn build_faulty_store(fault: ProbeFault) -> SystemObjectStore {
        SystemObjectStore::new(Arc::new(FaultyObjectStore::new(fault)), SYSTEM_LOCATION).expect("an S3 location")
    }

    /// The paths of every object the store holds under its location.
    async fn list_object_paths(store: &SystemObjectStore) -> Vec<String> {
        store
            .object_store
            .list(Some(&store.base_path))
            .map_ok(|meta| meta.location.to_string())
            .try_collect()
            .await
            .expect("a listing")
    }

    #[tokio::test]
    async fn startup_refuses_a_store_without_conditional_updates_naming_the_operation() {
        let store = build_faulty_store(ProbeFault::RefuseUpdate);

        let result = probe_conditional_operations(&store).await;

        assert!(
            matches!(&result, Err(QueryError::Config(message)) if message.contains("ETag compare-and-swap (PutMode::Update)")),
            "{result:?}"
        );
    }

    /// Each other step of the probe that fails or answers outside its contract names the operation
    /// it probed. The answer carrying the object for the current `ETag` is the failure that would
    /// leave a replica serving a stale pointer.
    #[tokio::test]
    async fn startup_refuses_a_store_failing_any_other_probe_step_naming_the_operation() {
        let cases = [
            (ProbeFault::RefuseCreate, "conditional create (PutMode::Create)"),
            (ProbeFault::OmitCreateEtag, "conditional create (PutMode::Create)"),
            (ProbeFault::OmitUpdateEtag, "ETag compare-and-swap (PutMode::Update)"),
            (ProbeFault::IgnoreIfNoneMatch, "conditional GET (If-None-Match)"),
            (ProbeFault::FailConditionalGet, "conditional GET (If-None-Match)"),
        ];
        for (fault, operation) in cases {
            let store = build_faulty_store(fault);

            let result = probe_conditional_operations(&store).await;

            assert!(
                matches!(&result, Err(QueryError::Config(message)) if message.contains(operation)),
                "{fault:?}: {result:?}"
            );
        }
    }

    /// Each of these faults lets the create through, so the probe object exists when the later
    /// step is refused. Left behind, a crash-looping listener would add one such object on every
    /// restart against the very store the probe exists to refuse.
    #[tokio::test]
    async fn a_probe_step_refused_after_the_create_leaves_no_probe_object() {
        for fault in [
            ProbeFault::OmitCreateEtag,
            ProbeFault::RefuseUpdate,
            ProbeFault::OmitUpdateEtag,
            ProbeFault::IgnoreIfNoneMatch,
            ProbeFault::FailConditionalGet,
        ] {
            let store = build_faulty_store(fault);

            let result = probe_conditional_operations(&store).await;

            assert!(matches!(result, Err(QueryError::Config(_))), "{fault:?}: {result:?}");
            assert_eq!(list_object_paths(&store).await, Vec::<String>::new(), "{fault:?}");
        }
    }

    /// The probe removes its object, and a delete the store refuses leaves that one object behind
    /// without stopping startup: the three operations the store relies on all passed.
    #[tokio::test]
    async fn the_probe_removes_its_object_and_a_refused_delete_does_not_stop_startup() {
        let store = SystemObjectStore::new(Arc::new(InMemory::new()), SYSTEM_LOCATION).expect("an S3 location");
        let undeletable_store = build_faulty_store(ProbeFault::RefuseDelete);

        probe_conditional_operations(&store)
            .await
            .expect("InMemory supports the three conditional operations");
        probe_conditional_operations(&undeletable_store)
            .await
            .expect("a refused delete does not stop startup");

        assert_eq!(list_object_paths(&store).await, Vec::<String>::new());
        let leftover_paths = list_object_paths(&undeletable_store).await;
        assert_eq!(leftover_paths.len(), 1, "{leftover_paths:?}");
        assert!(
            leftover_paths[0].starts_with("opensearch-system/.startup-probe/"),
            "{leftover_paths:?}"
        );
    }

    #[tokio::test]
    async fn startup_refuses_a_location_equal_to_a_table_location_naming_it() {
        let catalog = build_catalog_with_logs_table_at(LOGS_LOCATION).await;
        let store = SystemObjectStore::new(Arc::new(InMemory::new()), LOGS_LOCATION).expect("an S3 location");

        let result = refuse_table_location_overlap(&store, catalog.as_ref()).await;

        assert!(
            matches!(&result, Err(QueryError::Config(message)) if message.contains("overlaps `s3://warehouse/tables/logs`")),
            "{result:?}"
        );
    }

    /// The names of the tables `catalog` lists in the IceGate namespace, in its listing order.
    async fn list_table_names(catalog: &dyn Catalog) -> Vec<String> {
        catalog
            .list_tables(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()))
            .await
            .expect("a listing")
            .into_iter()
            .map(|table_ident| table_ident.name().to_string())
            .collect()
    }

    /// The catalog lists its two tables in an order of its own, the same on every listing, so one
    /// of the two stores below overlaps the table it lists second. A check that stopped at the
    /// first listed table would accept that store.
    #[tokio::test]
    async fn startup_refuses_a_location_equal_to_any_listed_table_location_naming_it() {
        let catalog = build_catalog_with_logs_and_spans_tables_at(LOGS_LOCATION, SPANS_LOCATION).await;
        let listed_names = list_table_names(catalog.as_ref()).await;
        assert_eq!(
            list_table_names(catalog.as_ref()).await,
            listed_names,
            "the catalog lists in one order"
        );
        let mut table_names = listed_names;
        table_names.sort_unstable();
        assert_eq!(table_names, [LOGS_TABLE, SPANS_TABLE]);

        for table_location in [LOGS_LOCATION, SPANS_LOCATION] {
            let store = SystemObjectStore::new(Arc::new(InMemory::new()), table_location).expect("an S3 location");
            let expected_overlap = format!("overlaps `{table_location}`");

            let result = refuse_table_location_overlap(&store, catalog.as_ref()).await;

            assert!(
                matches!(&result, Err(QueryError::Config(message)) if message.contains(&expected_overlap)),
                "{table_location}: {result:?}"
            );
        }
    }

    #[tokio::test]
    async fn startup_accepts_a_location_beside_every_table_location() {
        let catalog = build_catalog_with_logs_and_spans_tables_at(LOGS_LOCATION, SPANS_LOCATION).await;
        let store = SystemObjectStore::new(Arc::new(InMemory::new()), SYSTEM_LOCATION).expect("an S3 location");

        refuse_table_location_overlap(&store, catalog.as_ref()).await.expect(
            "s3://warehouse/opensearch-system is beside s3://warehouse/tables/logs and s3://warehouse/tables/spans",
        );
        probe_conditional_operations(&store)
            .await
            .expect("InMemory supports the three conditional operations");
    }

    /// A deployment whose ingest has not created the namespace yet holds no table, so the check
    /// passes without listing; listing the tables of a missing namespace fails, which would stop
    /// startup instead.
    #[tokio::test]
    async fn startup_skips_the_table_check_while_the_icegate_namespace_is_absent() {
        let catalog = build_catalog_without_namespace().await;
        let store = SystemObjectStore::new(Arc::new(InMemory::new()), SYSTEM_LOCATION).expect("an S3 location");
        let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
        assert!(
            catalog.list_tables(&namespace).await.is_err(),
            "the fixture must lack the namespace the check skips"
        );

        refuse_table_location_overlap(&store, catalog.as_ref())
            .await
            .expect("a missing namespace holds no table location");
    }
}
