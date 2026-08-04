//! The operator registry: one `OpenDAL` operator per storage identity, kept
//! for the lifetime of the component.
//!
//! An operator carrying the metrics layer is never freed (see the
//! [`layers`](super::layers) module), so both operator paths — the Iceberg
//! storage and the WAL object store — resolve through a registry instead of
//! building their own operator. Everything that decides *which* operator a
//! request gets lives here; the layer stack such an operator carries lives in
//! `layers`.
//!
//! # Limits of the fix
//!
//! The registry removes repeated construction for an identity that has been
//! seen before. It does not remove the `Arc` cycle inside
//! `opendal`'s `observe::MetricsLayer`, so a **stream of identities that never
//! repeat** still grows memory: the REST catalog builds a `FileIO` per table
//! load from the properties the server returns, and a server that vends
//! rotating credentials produces a new identity every time.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::Ref;
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use opendal::services::S3Config;

use super::builder::{ObjectStoreWithPath, create_local_store, create_memory_store};
use super::config::StorageBackend;
use super::icegate_s3::build_s3_operator;
use super::layers::{StorageLayers, StorageLayersConfig};
use crate::error::{CommonError, Result};

/// Region assumed when the storage backend does not name one.
const DEFAULT_S3_REGION: &str = "us-east-1";

/// Number of operators past which a registry is rebuilding rather than reusing.
///
/// The count is per registry and runs from process start, not over a window:
/// nothing is ever evicted, so the number that says whether operators are being
/// reused is the total one registry has ever built.
///
/// A component addresses one bucket with one set of credentials, so the number
/// a deployment reaches is one. A catalog that vends credentials per table load
/// (the REST catalog does) legitimately reaches one operator per table — five,
/// for IceGate's tables — across the warehouse bucket and the catalog bucket,
/// which puts the ceiling of a healthy registry at ten. Past it the registry is
/// building operators it cannot free, and it says so once. A component runs two
/// registries (the Iceberg storage and the object-store path, split in
/// `catalog::builder`), so a process may report the warning twice.
const OPERATOR_WARN_THRESHOLD: usize = 10;

/// An [`S3Config`] together with the digest used to hash it.
///
/// The configuration is part of the operator cache key, and hashing it on
/// every storage operation would mean serializing it again; the digest is
/// therefore computed once, when a storage is built.
#[derive(Clone)]
pub(crate) struct S3ConfigKey {
    config: Arc<S3Config>,
    hash: u64,
}

impl std::fmt::Debug for S3ConfigKey {
    /// Prints the digest only: the configuration holds credentials.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ConfigKey")
            .field("digest", &self.hash)
            .finish_non_exhaustive()
    }
}

impl S3ConfigKey {
    /// Take ownership of `config` and derive its digest.
    ///
    /// Equal configurations must hash equally, so the digest comes from the
    /// serialized form rather than from a hand-picked subset of fields, which
    /// would silently stop covering a field `OpenDAL` adds later. A
    /// serialization failure only costs collisions — [`PartialEq`] still
    /// decides which entry a lookup finds — so it degrades to a fixed digest
    /// instead of failing the operation.
    pub(crate) fn new(config: S3Config) -> Self {
        let hash = serde_json::to_string(&config).map_or(0, |serialized| {
            let mut hasher = DefaultHasher::new();
            serialized.hash(&mut hasher);
            hasher.finish()
        });
        Self {
            config: Arc::new(config),
            hash,
        }
    }
}

impl PartialEq for S3ConfigKey {
    fn eq(&self, other: &Self) -> bool {
        // The pointer comparison is the fast path for clones of one storage:
        // `new_input`/`new_output` clone the storage per file, and the clones
        // share this `Arc`. Two storages built from separate `FileIO`
        // properties never do, so their lookups compare the fields.
        Arc::ptr_eq(&self.config, &other.config) || self.config == other.config
    }
}

impl Eq for S3ConfigKey {}

impl Hash for S3ConfigKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

/// Identity of a cached operator: what it signs with, and what it addresses.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct OperatorKey {
    config: S3ConfigKey,
    bucket: String,
}

/// One built operator and the object store wrapping it.
///
/// The store is built on the first caller that asks for one, so the Iceberg
/// path — which takes the operator itself — pays nothing for it.
struct CachedOperator {
    operator: Operator,
    store: OnceLock<Arc<dyn ObjectStore>>,
}

/// The operators of one component, built once per identity and shared by every
/// caller afterwards.
///
/// An operator is identified by the configuration it signs with plus the bucket
/// it addresses, so two catalogs' credentials never serve each other's tables.
/// Nothing is ever evicted: an operator carrying the metrics layer is retained
/// by `OpenDAL` whether the registry holds it or not (see the `layers`
/// module), so dropping the entry would only make the next request build a
/// second one.
///
/// One registry is one `OpenTelemetry` scope. See the module docs for what the
/// registry does **not** fix.
pub struct OperatorRegistry {
    /// Backend of the object-store paths ([`Self::resolve_object_store`]); the
    /// Iceberg path carries its own configuration in the storage.
    backend: Option<StorageBackend>,
    layers: StorageLayers,
    operators: DashMap<OperatorKey, CachedOperator>,
    /// Drives the one-shot warning; the counter below reports the same number.
    operators_built: AtomicUsize,
    growth_warned: AtomicBool,
}

impl std::fmt::Debug for OperatorRegistry {
    /// Reports the layers and how many operators are cached, never the keys:
    /// a key holds credentials.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorRegistry")
            .field("backend", &self.backend)
            .field("layers", &self.layers)
            .field("operators", &self.operators.len())
            .finish_non_exhaustive()
    }
}

impl OperatorRegistry {
    /// Build a registry whose operators carry the layers `config` describes and
    /// address `backend`.
    ///
    /// Call once per `OpenTelemetry` scope: a second registry means a second
    /// set of instrument registrations, a second cache-sweep task, and — the
    /// reason it matters — a second copy of every operator.
    #[must_use]
    pub fn new(config: &StorageLayersConfig, backend: Option<&StorageBackend>) -> Self {
        Self {
            backend: backend.cloned(),
            layers: StorageLayers::new(config),
            operators: DashMap::new(),
            operators_built: AtomicUsize::new(0),
            growth_warned: AtomicBool::new(false),
        }
    }

    /// Return the object store serving `base_path` together with the path
    /// within that store.
    ///
    /// `s3://` and `s3a://` paths share one operator per bucket, so two paths
    /// under one bucket return the same store and differ only in the returned
    /// prefix. Local and in-memory paths carry no operator and are built per
    /// call.
    ///
    /// # Errors
    ///
    /// Returns [`CommonError::Config`] if the AWS credentials are only
    /// partially configured or `OpenDAL` rejects the S3 configuration, and an
    /// I/O error if a local store cannot be opened.
    pub fn resolve_object_store(&self, base_path: &str) -> Result<ObjectStoreWithPath> {
        let Some(bucket_and_prefix) = base_path.strip_prefix("s3://").or_else(|| base_path.strip_prefix("s3a://"))
        else {
            return if base_path.starts_with("file://") || base_path.starts_with('/') {
                create_local_store(base_path)
            } else {
                Ok(create_memory_store(base_path))
            };
        };

        let (bucket, prefix) = bucket_and_prefix
            .split_once('/')
            .map_or_else(|| (bucket_and_prefix, ""), |(bucket, prefix)| (bucket, prefix));

        let config = S3ConfigKey::new(self.s3_config()?);
        // Scoped so the cache entry is released before the caller gets the
        // store: holding it blocks every other resolution of the same bucket.
        let store = {
            let cached = self.resolve_cached_operator(&config, bucket)?;
            Arc::clone(
                cached
                    .store
                    .get_or_init(|| Arc::new(OpendalStore::new(cached.operator.clone()))),
            )
        };

        Ok((store, prefix.to_string()))
    }

    /// Return the operator addressing `bucket` with `config`, building it on
    /// first use.
    ///
    /// # Errors
    ///
    /// Returns an error if `OpenDAL` rejects the S3 configuration.
    pub(crate) fn resolve_operator(&self, config: &S3ConfigKey, bucket: &str) -> iceberg::Result<Operator> {
        Ok(self.resolve_cached_operator(config, bucket)?.operator.clone())
    }

    /// Return the cache entry for `(config, bucket)`, building it on first use.
    ///
    /// Building happens while the entry is held, which costs a few microseconds
    /// of contention and buys the guarantee that concurrent misses build one
    /// operator, not one each: every operator ever built is retained.
    fn resolve_cached_operator(
        &self,
        config: &S3ConfigKey,
        bucket: &str,
    ) -> iceberg::Result<Ref<'_, OperatorKey, CachedOperator>> {
        let key = OperatorKey {
            config: config.clone(),
            bucket: bucket.to_string(),
        };
        match self.operators.entry(key) {
            Entry::Occupied(occupied) => Ok(occupied.into_ref().downgrade()),
            Entry::Vacant(vacant) => {
                let built = self.build_operator(&config.config, bucket)?;
                Ok(vacant.insert(built).downgrade())
            }
        }
    }

    /// Build an operator for `bucket` with the full layer stack, and report it.
    fn build_operator(&self, config: &S3Config, bucket: &str) -> iceberg::Result<CachedOperator> {
        let operator = self.layers.wrap_operator(build_s3_operator(config, bucket)?);

        // Counted here rather than read off the map: the entry that triggered
        // this build is held, and asking the map for its length would wait on
        // that same entry.
        let built = self.operators_built.fetch_add(1, Ordering::Relaxed) + 1;
        // TODO(med): the problem is with Opendal observe::MetricsLayer::layer. In layer() method, Arc<AccessorInfo> is cloned inside HttpClient and AccessorInfo is never released. When creating operators periodically, we get a memory leak.
        if built > OPERATOR_WARN_THRESHOLD && !self.growth_warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                operators = built,
                threshold = OPERATOR_WARN_THRESHOLD,
                "built more operators than a stable configuration needs; an operator is retained \
                 for the lifetime of the process, so a configuration that keeps changing (vended \
                 or rotating credentials) grows memory. Reported once"
            );
        }

        Ok(CachedOperator {
            operator,
            store: OnceLock::new(),
        })
    }

    /// The S3 configuration the object-store paths sign with.
    ///
    /// Credentials configured for the backend win; otherwise the ambient AWS
    /// environment is used, which is how a deployment authenticates through an
    /// IAM role. Both halves of a key pair must resolve or neither — a half
    /// configured pair signs nothing and would fail per request instead.
    ///
    /// # Errors
    ///
    /// Returns [`CommonError::Config`] when only one half of the key pair, or a
    /// session token without a key pair, is configured.
    fn s3_config(&self) -> Result<S3Config> {
        let (endpoint, region, configured_key_id, configured_secret_key) = match &self.backend {
            Some(StorageBackend::S3(s3)) => (
                s3.endpoint.clone(),
                s3.region.clone(),
                s3.access_key_id.clone(),
                s3.secret_access_key.clone(),
            ),
            _ => (None, DEFAULT_S3_REGION.to_string(), None, None),
        };

        let access_key_id = configured_key_id
            .filter(|value| !value.is_empty())
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok().filter(|value| !value.is_empty()));
        let secret_access_key = configured_secret_key
            .filter(|value| !value.is_empty())
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok().filter(|value| !value.is_empty()));
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok().filter(|value| !value.is_empty());

        if access_key_id.is_some() != secret_access_key.is_some() {
            return Err(CommonError::Config(
                "AWS credentials partially configured; set both AWS_ACCESS_KEY_ID and \
                 AWS_SECRET_ACCESS_KEY, or neither"
                    .to_string(),
            ));
        }
        if session_token.is_some() && access_key_id.is_none() {
            return Err(CommonError::Config(
                "AWS_SESSION_TOKEN requires both AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY".to_string(),
            ));
        }

        // The bucket is left unset: it is the other half of the cache key, and
        // `build_s3_operator` puts it on the builder.
        let mut config = S3Config::default();
        config.endpoint = endpoint;
        config.region = Some(region);
        config.access_key_id = access_key_id;
        config.secret_access_key = secret_access_key;
        config.session_token = session_token;
        Ok(config)
    }

    /// How many operators this registry has built, for tests and diagnostics.
    #[cfg(test)]
    pub(crate) fn operator_count(&self) -> usize {
        self.operators.len()
    }

    /// The layers every operator of this registry carries, so tests can prove
    /// they are built once.
    #[cfg(test)]
    pub(crate) const fn layers(&self) -> &StorageLayers {
        &self.layers
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::io::{S3_ALLOW_ANONYMOUS, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT, S3_REGION};

    use super::super::icegate_s3::s3_config_parse;
    use super::*;
    use crate::storage::S3Config as BackendS3Config;

    /// An S3 configuration for a local endpoint with credential and metadata
    /// lookups switched off, so operator construction stays hermetic — no test
    /// here performs any I/O.
    fn hermetic_config(access_key_id: &str) -> S3ConfigKey {
        let props = HashMap::from([
            (S3_ENDPOINT.to_string(), "http://localhost:9000".to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
            (S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string()),
            (S3_DISABLE_EC2_METADATA.to_string(), "true".to_string()),
            (S3_ALLOW_ANONYMOUS.to_string(), "true".to_string()),
            (iceberg::io::S3_ACCESS_KEY_ID.to_string(), access_key_id.to_string()),
            (iceberg::io::S3_SECRET_ACCESS_KEY.to_string(), "secret".to_string()),
        ]);
        S3ConfigKey::new(s3_config_parse(props).expect("s3 configuration must parse"))
    }

    fn registry() -> OperatorRegistry {
        OperatorRegistry::new(&StorageLayersConfig::default(), None)
    }

    /// Two operators are the same instance when they share one accessor;
    /// a freshly built operator always carries its own.
    fn is_same_operator(left: &Operator, right: &Operator) -> bool {
        Arc::ptr_eq(left.inner(), right.inner())
    }

    /// The regression test for the leak this module exists to avoid: an
    /// operator carrying the metrics layer is never freed, so one identity must
    /// build one operator however many times it is resolved.
    #[test]
    fn a_repeated_identity_builds_one_operator() {
        let registry = registry();
        let config = hermetic_config("key-one");

        let first = registry.resolve_operator(&config, "bucket").expect("first operator");
        for _ in 0..100 {
            let operator = registry.resolve_operator(&config, "bucket").expect("operator");
            assert!(is_same_operator(&first, &operator), "the operator was rebuilt");
        }

        assert_eq!(registry.operator_count(), 1);
    }

    /// A catalog that vends per-table credentials resolves one storage per set
    /// of them: none may be served an operator built from another's, and a set
    /// seen before must not build a second one.
    #[test]
    fn distinct_identities_build_one_operator_each() {
        let registry = registry();
        let first_config = hermetic_config("key-one");
        let second_config = hermetic_config("key-two");

        let first = registry.resolve_operator(&first_config, "bucket").expect("first operator");
        let second = registry.resolve_operator(&second_config, "bucket").expect("second operator");
        let other_bucket = registry
            .resolve_operator(&first_config, "other")
            .expect("other bucket operator");

        assert!(
            !is_same_operator(&first, &second),
            "one bucket addressed with two configurations must not share an operator"
        );
        assert!(
            !is_same_operator(&first, &other_bucket),
            "two buckets must not share an operator"
        );
        assert_eq!(registry.operator_count(), 3);

        // A configuration equal to one already resolved is the same identity,
        // even though the caller built a separate value for it.
        let repeated = registry
            .resolve_operator(&hermetic_config("key-one"), "bucket")
            .expect("repeated operator");
        assert!(is_same_operator(&first, &repeated));
        assert_eq!(registry.operator_count(), 3);
    }

    /// Nothing is evicted, so the count only grows: an identity resolved long
    /// ago still answers from the cache instead of building a second operator
    /// that could never be freed.
    #[test]
    fn no_identity_is_evicted_by_later_ones() {
        let registry = registry();
        let oldest = hermetic_config("key-0");
        let first = registry.resolve_operator(&oldest, "bucket").expect("first operator");

        for rotation in 1..=OPERATOR_WARN_THRESHOLD + 2 {
            registry
                .resolve_operator(&hermetic_config(&format!("key-{rotation}")), "bucket")
                .expect("operator");
        }

        let again = registry.resolve_operator(&oldest, "bucket").expect("cached operator");
        assert!(is_same_operator(&first, &again), "the oldest identity was evicted");
        assert_eq!(registry.operator_count(), OPERATOR_WARN_THRESHOLD + 3);
    }

    /// Partially configured credentials must fail before an operator exists:
    /// an access key without its secret signs nothing, and the request-time
    /// failure it would otherwise cause is far from its cause.
    #[test]
    fn half_a_key_pair_is_rejected() {
        let registry = OperatorRegistry::new(
            &StorageLayersConfig::default(),
            Some(&StorageBackend::S3(BackendS3Config {
                bucket: "bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some("http://localhost:9000".to_string()),
                access_key_id: Some("key".to_string()),
                secret_access_key: None,
            })),
        );

        let error = registry
            .resolve_object_store("s3://bucket/prefix")
            .expect_err("a half-configured key pair must fail");

        assert!(matches!(error, CommonError::Config(_)), "{error}");
        assert_eq!(registry.operator_count(), 0);
    }

    /// Non-S3 paths carry no operator: the sweep and the WAL both accept a
    /// local or in-memory base path in development, and neither retains
    /// anything, so they never reach the cache.
    #[test]
    fn paths_without_an_operator_are_not_cached() {
        let registry = registry();

        let (_, memory_prefix) = registry.resolve_object_store("memory://table").expect("memory store");

        assert_eq!(memory_prefix, "memory://table");
        assert_eq!(registry.operator_count(), 0);
    }
}
