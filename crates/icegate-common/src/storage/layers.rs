//! The `OpenDAL` layer stack every IceGate operator carries.
//!
//! Both operator paths — the Iceberg storage
//! ([`icegate_storage`](super::icegate_storage)) and the WAL object store —
//! run through [`OperatorRegistry`](super::registry::OperatorRegistry), which
//! wraps every operator it builds in this stack, so the stack is built and
//! documented here once.
//!
//! # An operator that carries the metrics layer is never freed
//!
//! Applying [`OtelMetricsLayer`] wraps the accessor's HTTP client and stores
//! the wrapper back into the accessor's own `AccessorInfo`, while the wrapper
//! holds an `Arc` to that same `AccessorInfo` — `opendal` 0.55,
//! `src/layers/observe/metrics.rs`, `MetricsLayer::layer`:
//!
//! ```text
//! let info = inner.info();
//! info.update_http_client(|client| {
//!     HttpClient::with(MetricsHttpFetcher { inner: client.into_inner(), info: info.clone(), .. })
//! });
//! ```
//!
//! The `Arc` cycle means dropping such an operator frees nothing: measured at
//! ~1-3 `KiB` retained per operator built, with or without a fresh instrument
//! registration. Registering instruments is not what accumulates — the
//! `OpenTelemetry` SDK deduplicates identical instruments within a meter — so
//! reusing the layer alone does not help.
//!
//! Layers themselves are safe to build and drop, since the cycle needs an
//! accessor, but **operators must be built once and reused**. Building one per
//! request is what made `icegate-ingest` grow to its 2 `GiB` container limit in
//! ~16 hours.

use std::time::Duration;

use opendal::Operator;
use opendal::layers::{OtelMetricsLayer, OtelTraceLayer};
use opentelemetry::metrics::Meter;

use super::cache::{CacheLayer, CacheMetrics, StorageCache};
use super::prefetch::{PrefetchConfig, PrefetchLayer, PrefetchMetrics};

/// The inputs a component's operators are built with.
///
/// One value is threaded from [`IoHandle`](crate::IoHandle) into every
/// [`OperatorRegistry`](super::registry::OperatorRegistry) it owns, so the
/// caching and the metrics scope of an operator cannot depend on which code
/// path built it. The meter is stamped per registry, because a registry is
/// exactly one `OpenTelemetry` scope.
#[derive(Clone, Debug, Default)]
pub struct StorageLayersConfig {
    /// Shared foyer read cache; `None` leaves the cache layer out.
    pub cache: Option<StorageCache>,
    /// Meter the layers and the registry register instruments against; `None`
    /// leaves them instrument-free.
    pub meter: Option<Meter>,
    /// Parquet column-chunk prefetch; `None` or disabled leaves the layer out.
    pub prefetch: Option<PrefetchConfig>,
    /// TTL for cached stat (HEAD) responses.
    pub stat_ttl: Option<Duration>,
    /// Largest value (bytes) written through the cache layer that is kept.
    pub max_write_cache_size: Option<usize>,
}

/// The optional layers shared by every operator one component builds.
///
/// Each layer is built once: the metrics layers register `OpenTelemetry`
/// instruments, and the cache and prefetch layers own state (key locks, stat
/// cache, in-flight deduplication) that only works when every operator shares
/// it.
pub(crate) struct StorageLayers {
    metrics: Option<OtelMetricsLayer>,
    cache: Option<CacheLayer>,
    prefetch: Option<PrefetchLayer>,
}

impl std::fmt::Debug for StorageLayers {
    /// Omits the layers themselves: they carry cache state, not information.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageLayers")
            .field("has_metrics", &self.metrics.is_some())
            .field("has_cache", &self.cache.is_some())
            .field("has_prefetch", &self.prefetch.is_some())
            .finish()
    }
}

impl StorageLayers {
    /// Build the layer stack, starting the cache layer's background sweep.
    ///
    /// A layer is left out when its input is absent: no `meter` means no
    /// metrics layer and instrument-free cache metrics, no `cache` means no
    /// cache layer, and a `prefetch` config that is disabled means no prefetch
    /// layer.
    ///
    /// Call once per registry: each call registers another sweep task, and
    /// operators built from different instances no longer share cache state.
    pub(crate) fn new(config: &StorageLayersConfig) -> Self {
        let meter = config.meter.as_ref();
        let cache_layer = config.cache.as_ref().map(|storage_cache| {
            let metrics = meter.map_or_else(CacheMetrics::new_disabled, CacheMetrics::new);
            let layer = CacheLayer::new(
                storage_cache.clone(),
                metrics,
                config.stat_ttl,
                config.max_write_cache_size,
            );
            layer.spawn_sweep();
            layer
        });

        let prefetch_layer = config.prefetch.as_ref().filter(|prefetch| prefetch.enabled).map(|prefetch| {
            let metrics = meter.map_or_else(PrefetchMetrics::new_disabled, PrefetchMetrics::new);
            PrefetchLayer::new(prefetch.clone(), metrics)
        });

        Self {
            metrics: meter.map(|meter| OtelMetricsLayer::builder().register(meter)),
            cache: cache_layer,
            prefetch: prefetch_layer,
        }
    }

    /// Wrap `operator` in the stack.
    ///
    /// Layers are applied bottom-up and execute top-down:
    /// `[Prefetch] -> [FoyerCache] -> [OtelMetrics] -> OtelTrace -> backend`.
    /// Trace and metrics sit below the cache so they only observe actual
    /// backend round-trips (cache misses); prefetch sits outermost so its
    /// background reads land in the cache.
    ///
    /// Because of the retention described in the module docs, call this on an
    /// operator that is then kept and reused, never on one built per request.
    pub(crate) fn wrap_operator(&self, operator: Operator) -> Operator {
        // `Operator::layer()` returns `Operator` directly (type-erased), so no
        // `.finish()` is needed between the calls.
        let mut operator = operator.layer(OtelTraceLayer::new());
        if let Some(layer) = &self.metrics {
            operator = operator.layer(layer.clone());
        }
        if let Some(layer) = &self.cache {
            operator = operator.layer(layer.clone());
        }
        if let Some(layer) = &self.prefetch {
            operator = operator.layer(layer.clone());
        }
        operator
    }

    /// Whether the prefetch layer was built, for tests and diagnostics.
    #[cfg(test)]
    pub(crate) const fn has_prefetch(&self) -> bool {
        self.prefetch.is_some()
    }

    /// Whether the metrics layer was built, for tests and diagnostics.
    #[cfg(test)]
    pub(crate) const fn has_metrics(&self) -> bool {
        self.metrics.is_some()
    }

    /// The cache layer, so tests can prove every operator shares one.
    #[cfg(test)]
    pub(crate) const fn cache(&self) -> Option<&CacheLayer> {
        self.cache.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use foyer::HybridCacheBuilder;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;

    use super::super::cache::{CacheKey, CacheValue};
    use super::*;

    /// A cache that exists only so a stack has one to build its layer from: no
    /// test here reads or writes, so entry weight is irrelevant.
    async fn storage_cache() -> StorageCache {
        HybridCacheBuilder::new()
            .memory(64 * 1024)
            .with_weighter(|_key: &CacheKey, _value: &CacheValue| 1)
            .storage()
            .build()
            .await
            .expect("test cache")
    }

    /// Each layer is independent of the others: a component configured without
    /// a meter must still cache, and one without a cache must still report
    /// metrics. Building a layer whose input is absent costs either instruments
    /// registered against a meter nobody exports, or — for the cache — a
    /// background sweep task with nothing to sweep.
    ///
    /// A prefetch config that is present but disabled is the same case as an
    /// absent one, and is the shipped default.
    #[tokio::test]
    async fn an_absent_input_leaves_its_layer_out() {
        let cache = storage_cache().await;
        let enabled_prefetch = PrefetchConfig {
            enabled: true,
            ..PrefetchConfig::default()
        };

        // (case, meter configured, cache configured, prefetch config,
        //  expected metrics/cache/prefetch layers)
        let cases = [
            (
                "every input configured",
                true,
                true,
                Some(enabled_prefetch.clone()),
                (true, true, true),
            ),
            (
                "no meter",
                false,
                true,
                Some(enabled_prefetch.clone()),
                (false, true, true),
            ),
            ("no cache", true, false, Some(enabled_prefetch), (true, false, true)),
            (
                "prefetch disabled",
                true,
                true,
                Some(PrefetchConfig::default()),
                (true, true, false),
            ),
            ("no prefetch config", true, true, None, (true, true, false)),
        ];

        for (case, has_meter, has_cache, prefetch, expected) in cases {
            let layers = StorageLayers::new(&StorageLayersConfig {
                cache: has_cache.then(|| cache.clone()),
                meter: has_meter.then(|| SdkMeterProvider::builder().build().meter("storage-layers-test")),
                prefetch,
                ..StorageLayersConfig::default()
            });

            let built = (layers.has_metrics(), layers.cache().is_some(), layers.has_prefetch());

            assert_eq!(built, expected, "{case}");
        }
    }
}
