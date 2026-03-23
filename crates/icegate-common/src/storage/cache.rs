//! Sparse range cache layer for `OpenDAL` with foyer 0.22.
//!
//! Instead of caching entire files, this layer caches individual byte
//! ranges as segments within a single [`CacheValue`]. On read, only the
//! uncached gaps are fetched from the backend and merged into the entry.
//! This is ideal for Parquet access patterns (footer read, then specific
//! column chunks).
//!
//! foyer's eviction handles capacity management — we impose no size limit.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use foyer::{DeviceBuilder as _, HybridCache};
use futures::future::try_join_all;
use opendal::raw::oio::{self, Read};
use opendal::raw::{
    Access, AccessorInfo, BytesContentRange, BytesRange, Layer, LayeredAccess, MaybeSend, OpDelete, OpList, OpRead,
    OpStat, OpWrite, RpDelete, RpList, RpRead, RpStat, RpWrite,
};
use opendal::{Buffer, Error, ErrorKind, Metadata, Result};
use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::{KeyValue, metrics::MeterProvider as _};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use serde::{Deserialize, Serialize};
use tracing::Instrument;

use crate::catalog::CacheConfig;
use crate::error::{CommonError, Result as CommonResult};

// ---------------------------------------------------------------------------
// CacheMetrics
// ---------------------------------------------------------------------------

/// Histogram bucket boundaries (in seconds) for cache read durations.
const CACHE_DURATION_BOUNDARIES: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
];

/// Histogram bucket boundaries (in bytes) for cache byte metrics.
const CACHE_BYTES_BOUNDARIES: &[f64] = &[
    512.0,
    1_024.0,
    4_096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
];

/// Histogram bucket boundaries for gap counts per cache miss.
const CACHE_GAPS_BOUNDARIES: &[f64] = &[0.0, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 50.0];

/// `OpenTelemetry` metrics for the sparse range cache layer.
///
/// Tracks cache hit/miss rates, read latencies, byte volumes, writes,
/// and evictions. All metric names are prefixed with
/// `icegate_storage_cache_`.
#[derive(Clone)]
pub(crate) struct CacheMetrics {
    enabled: bool,
    /// Cache hits by path type (`fast_path` or `lock_path`).
    hits_total: Counter<u64>,
    /// Cache misses (backend fetch required).
    misses_total: Counter<u64>,
    /// End-to-end cache read duration (seconds).
    read_duration: Histogram<f64>,
    /// Number of gaps per cache miss.
    gaps_per_miss: Histogram<f64>,
    /// Bytes fetched from the backend on cache miss.
    backend_fetch_bytes: Histogram<f64>,
    /// Bytes served from cache on cache hit.
    cache_served_bytes: Histogram<f64>,
    /// Total bytes written through the cache writer.
    write_bytes_total: Counter<u64>,
    /// Total cache entries evicted via delete.
    evictions_total: Counter<u64>,
    /// Stat cache hits.
    stat_hits_total: Counter<u64>,
    /// Stat cache misses.
    stat_misses_total: Counter<u64>,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for CacheMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheMetrics").field("enabled", &self.enabled).finish()
    }
}

impl CacheMetrics {
    /// Build a metrics recorder using the provided meter.
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            enabled: true,
            hits_total: meter
                .u64_counter("icegate_storage_cache_hits")
                .with_description("Cache hits by path type")
                .build(),
            misses_total: meter
                .u64_counter("icegate_storage_cache_misses")
                .with_description("Cache misses requiring backend fetch")
                .build(),
            read_duration: meter
                .f64_histogram("icegate_storage_cache_read_duration")
                .with_description("Cache read duration")
                .with_unit("s")
                .with_boundaries(CACHE_DURATION_BOUNDARIES.to_vec())
                .build(),
            gaps_per_miss: meter
                .f64_histogram("icegate_storage_cache_gaps_per_miss")
                .with_description("Number of uncached gaps per cache miss")
                .with_boundaries(CACHE_GAPS_BOUNDARIES.to_vec())
                .build(),
            backend_fetch_bytes: meter
                .f64_histogram("icegate_storage_cache_backend_fetch_bytes")
                .with_description("Bytes fetched from backend on cache miss")
                .with_boundaries(CACHE_BYTES_BOUNDARIES.to_vec())
                .build(),
            cache_served_bytes: meter
                .f64_histogram("icegate_storage_cache_served_bytes")
                .with_description("Bytes served from cache on hit")
                .with_boundaries(CACHE_BYTES_BOUNDARIES.to_vec())
                .build(),
            write_bytes_total: meter
                .u64_counter("icegate_storage_cache_write_bytes")
                .with_description("Total bytes written through cache")
                .build(),
            evictions_total: meter
                .u64_counter("icegate_storage_cache_evictions")
                .with_description("Cache entries evicted via delete")
                .build(),
            stat_hits_total: meter
                .u64_counter("icegate_storage_cache_stat_hits")
                .with_description("Stat cache hits")
                .build(),
            stat_misses_total: meter
                .u64_counter("icegate_storage_cache_stat_misses")
                .with_description("Stat cache misses")
                .build(),
        }
    }

    /// Build a no-op metrics recorder for tests or when metrics are disabled.
    pub(crate) fn new_disabled() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("cache-disabled");
        Self {
            enabled: false,
            hits_total: meter.u64_counter("icegate_storage_cache_hits").build(),
            misses_total: meter.u64_counter("icegate_storage_cache_misses").build(),
            read_duration: meter.f64_histogram("icegate_storage_cache_read_duration").build(),
            gaps_per_miss: meter.f64_histogram("icegate_storage_cache_gaps_per_miss").build(),
            backend_fetch_bytes: meter.f64_histogram("icegate_storage_cache_backend_fetch_bytes").build(),
            cache_served_bytes: meter.f64_histogram("icegate_storage_cache_served_bytes").build(),
            write_bytes_total: meter.u64_counter("icegate_storage_cache_write_bytes").build(),
            evictions_total: meter.u64_counter("icegate_storage_cache_evictions").build(),
            stat_hits_total: meter.u64_counter("icegate_storage_cache_stat_hits").build(),
            stat_misses_total: meter.u64_counter("icegate_storage_cache_stat_misses").build(),
        }
    }

    /// Record a cache hit.
    fn record_hit(&self, path_type: &str, served_bytes: u64) {
        if !self.enabled {
            return;
        }
        self.hits_total.add(1, &[KeyValue::new("path_type", path_type.to_string())]);
        #[allow(clippy::cast_precision_loss)]
        self.cache_served_bytes.record(served_bytes as f64, &[]);
    }

    /// Record a cache miss.
    #[allow(clippy::cast_precision_loss)]
    fn record_miss(&self, gap_count: usize, fetch_bytes: u64) {
        if !self.enabled {
            return;
        }
        self.misses_total.add(1, &[]);
        self.gaps_per_miss.record(gap_count as f64, &[]);
        self.backend_fetch_bytes.record(fetch_bytes as f64, &[]);
    }

    /// Record a cache read duration.
    fn record_read_duration(&self, start: Instant) {
        if !self.enabled {
            return;
        }
        self.read_duration.record(start.elapsed().as_secs_f64(), &[]);
    }

    /// Record bytes written through the cache writer.
    fn record_write_bytes(&self, bytes: u64) {
        if !self.enabled {
            return;
        }
        self.write_bytes_total.add(bytes, &[]);
    }

    /// Record a cache eviction.
    fn record_eviction(&self) {
        if !self.enabled {
            return;
        }
        self.evictions_total.add(1, &[]);
    }

    /// Record a stat cache hit.
    fn record_stat_hit(&self) {
        if !self.enabled {
            return;
        }
        self.stat_hits_total.add(1, &[]);
    }

    /// Record a stat cache miss.
    fn record_stat_miss(&self) {
        if !self.enabled {
            return;
        }
        self.stat_misses_total.add(1, &[]);
    }
}

// ---------------------------------------------------------------------------
// Cache key / value types
// ---------------------------------------------------------------------------

/// Cache key: file path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CacheKey {
    path: String,
}

/// Sparse range cache value.
///
/// Stores non-overlapping byte segments of a single object, keyed by
/// their start offset. Segments are always kept sorted and
/// non-overlapping; adjacent/overlapping segments are merged on
/// insertion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheValue {
    /// Sorted, non-overlapping segments: start-offset -> bytes.
    segments: BTreeMap<u64, Vec<u8>>,
}

/// Helper to convert `u64` to `usize` safely.
///
/// On 64-bit targets this is a no-op. On 32-bit targets it saturates
/// to `usize::MAX` (which is fine — files >4 `GiB` are not expected there).
#[allow(clippy::cast_possible_truncation)]
const fn u64_to_usize(v: u64) -> usize {
    if v > usize::MAX as u64 { usize::MAX } else { v as usize }
}

impl CacheValue {
    /// Create an empty sparse value.
    const fn new() -> Self {
        Self {
            segments: BTreeMap::new(),
        }
    }

    /// Insert a byte range, merging with any overlapping or adjacent
    /// segments.
    fn insert_range(&mut self, offset: u64, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let new_end = offset + data.len() as u64;

        // Collect all segments that overlap or are adjacent to [offset, new_end).
        // A segment at `seg_start` with length `seg_len` overlaps/touches if:
        //   seg_start <= new_end  AND  seg_start + seg_len >= offset
        let overlapping: Vec<(u64, Vec<u8>)> = self
            .segments
            .range(..=new_end)
            .rev()
            .take_while(|(seg_start, _)| **seg_start <= new_end)
            .filter(|(seg_start, seg_data)| **seg_start + seg_data.len() as u64 >= offset)
            .map(|(k, v)| (*k, v.clone()))
            .collect();

        // Compute the union range.
        let mut union_start = offset;
        let mut union_end = new_end;
        for (seg_start, seg_data) in &overlapping {
            union_start = union_start.min(*seg_start);
            union_end = union_end.max(*seg_start + seg_data.len() as u64);
        }

        // Build merged buffer.
        let merged_len = u64_to_usize(union_end - union_start);
        let mut merged = vec![0u8; merged_len];

        // Write existing segments first.
        for (seg_start, seg_data) in &overlapping {
            let dst_offset = u64_to_usize(*seg_start - union_start);
            merged[dst_offset..dst_offset + seg_data.len()].copy_from_slice(seg_data);
        }
        // Write new data on top (overwrites overlapping regions).
        let dst_offset = u64_to_usize(offset - union_start);
        merged[dst_offset..dst_offset + data.len()].copy_from_slice(data);

        // Remove old overlapping segments and insert merged.
        for (seg_start, _) in &overlapping {
            self.segments.remove(seg_start);
        }
        self.segments.insert(union_start, merged);
    }

    /// Find gaps within `[start, end)` that are not covered by segments.
    ///
    /// Uses bounded `BTreeMap::range` to skip segments outside the
    /// query window.
    fn find_gaps(&self, start: u64, end: u64) -> Vec<Range<u64>> {
        let mut gaps = Vec::new();
        let mut pos = start;

        // Find the first segment that could overlap [start, end).
        // A segment starting before `start` could still cover it if its
        // data extends past `start`. We look back one entry via
        // `range(..=start).next_back()`.
        let iter = if let Some((&prev_start, _)) = self.segments.range(..=start).next_back() {
            // Start iterating from the segment at or before `start`.
            self.segments.range(prev_start..end)
        } else {
            // No segment starts at or before `start`; start from `start`.
            self.segments.range(start..end)
        };

        for (&seg_start, seg_data) in iter {
            let seg_end = seg_start + seg_data.len() as u64;
            // Skip segments entirely before our position.
            if seg_end <= pos {
                continue;
            }
            // Gap before this segment?
            if seg_start > pos {
                gaps.push(pos..seg_start.min(end));
            }
            pos = pos.max(seg_end);
            if pos >= end {
                break;
            }
        }
        // Trailing gap?
        if pos < end {
            gaps.push(pos..end);
        }
        gaps
    }

    /// Extract bytes for `[start, end)`, returning `None` if not fully
    /// covered.
    ///
    /// Uses bounded `BTreeMap::range` to skip irrelevant segments.
    fn read_range(&self, start: u64, end: u64) -> Option<Vec<u8>> {
        let len = u64_to_usize(end - start);
        let mut result = Vec::with_capacity(len);
        let mut pos = start;

        // Same bounded-range strategy as `find_gaps`.
        let iter = if let Some((&prev_start, _)) = self.segments.range(..=start).next_back() {
            self.segments.range(prev_start..end)
        } else {
            self.segments.range(start..end)
        };

        for (&seg_start, seg_data) in iter {
            let seg_end = seg_start + seg_data.len() as u64;
            if seg_end <= pos {
                continue;
            }
            if seg_start > pos {
                return None; // gap
            }
            let copy_start = u64_to_usize(pos - seg_start);
            let copy_end = u64_to_usize(end.min(seg_end) - seg_start);
            result.extend_from_slice(&seg_data[copy_start..copy_end]);
            pos = seg_end.min(end);
            if pos >= end {
                break;
            }
        }
        if result.len() == len { Some(result) } else { None }
    }

    /// Check if `[start, end)` is fully covered.
    ///
    /// Short-circuits on the first gap found, avoiding allocation.
    fn covers(&self, start: u64, end: u64) -> bool {
        if start >= end {
            return true;
        }
        let mut pos = start;

        let iter = if let Some((&prev_start, _)) = self.segments.range(..=start).next_back() {
            self.segments.range(prev_start..end)
        } else {
            self.segments.range(start..end)
        };

        for (&seg_start, seg_data) in iter {
            let seg_end = seg_start + seg_data.len() as u64;
            if seg_end <= pos {
                continue;
            }
            if seg_start > pos {
                return false; // gap found
            }
            pos = seg_end;
            if pos >= end {
                return true;
            }
        }
        pos >= end
    }

    /// Total number of cached bytes across all segments.
    fn size_bytes(&self) -> usize {
        self.segments.values().map(Vec::len).sum()
    }
}

/// Shared hybrid cache for storage I/O.
pub type StorageCache = HybridCache<CacheKey, CacheValue>;

// ---------------------------------------------------------------------------
// Per-key locking
// ---------------------------------------------------------------------------

/// Per-key mutex map to serialize cache updates for the same object.
///
/// Prevents two concurrent readers from both fetching the same gap and
/// one overwriting the other's merge.
struct KeyLocks(DashMap<CacheKey, Arc<tokio::sync::Mutex<()>>>);

impl KeyLocks {
    fn new() -> Self {
        Self(DashMap::new())
    }

    /// Get (or create) the lock for `key`.
    fn lock(&self, key: &CacheKey) -> Arc<tokio::sync::Mutex<()>> {
        self.0
            .entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Remove the lock entry for `key`.
    fn remove(&self, key: &CacheKey) {
        self.0.remove(key);
    }
}

// ---------------------------------------------------------------------------
// CacheLayer (opendal Layer impl)
// ---------------------------------------------------------------------------

/// In-memory cache for stat (HEAD) metadata, keyed by path.
pub(crate) type StatCache = DashMap<CacheKey, (Instant, Metadata)>;

/// Custom sparse-range cache layer for `OpenDAL`.
///
/// Wraps a foyer [`HybridCache`] and implements [`Layer`] so it can be
/// inserted into an operator's layer stack. Reads cache individual byte
/// ranges; writes and deletes keep the cache consistent.
#[derive(Clone, Debug)]
pub(crate) struct CacheLayer {
    cache: StorageCache,
    locks: Arc<KeyLocks>,
    metrics: CacheMetrics,
    stat_cache: Arc<StatCache>,
    stat_ttl: Option<Duration>,
    max_write_cache_size: Option<usize>,
}

impl std::fmt::Debug for KeyLocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyLocks").field("entries", &self.0.len()).finish()
    }
}

impl CacheLayer {
    /// Create a new sparse-range cache layer with metrics.
    ///
    /// When `stat_ttl` is `Some`, stat (HEAD) responses are cached in
    /// memory for the given duration, avoiding redundant S3 round-trips.
    ///
    /// When `max_write_cache_size` is `Some`, writes larger than the
    /// threshold (in bytes) bypass the cache to prevent large data files
    /// from evicting smaller WAL segments.
    pub(crate) fn new(
        cache: StorageCache,
        metrics: CacheMetrics,
        stat_ttl: Option<Duration>,
        max_write_cache_size: Option<usize>,
    ) -> Self {
        Self {
            cache,
            locks: Arc::new(KeyLocks::new()),
            metrics,
            stat_cache: Arc::new(DashMap::new()),
            stat_ttl,
            max_write_cache_size,
        }
    }
}

impl<A: Access> Layer<A> for CacheLayer {
    type LayeredAccess = CacheAccessor<A>;

    fn layer(&self, accessor: A) -> Self::LayeredAccess {
        CacheAccessor {
            inner: Arc::new(CacheInner {
                accessor,
                cache: self.cache.clone(),
                locks: self.locks.clone(),
                metrics: self.metrics.clone(),
                stat_cache: self.stat_cache.clone(),
                stat_ttl: self.stat_ttl,
                max_write_cache_size: self.max_write_cache_size,
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// CacheAccessor
// ---------------------------------------------------------------------------

struct CacheInner<A: Access> {
    accessor: A,
    cache: StorageCache,
    locks: Arc<KeyLocks>,
    metrics: CacheMetrics,
    stat_cache: Arc<StatCache>,
    stat_ttl: Option<Duration>,
    max_write_cache_size: Option<usize>,
}

#[allow(clippy::missing_fields_in_debug)]
impl<A: Access> std::fmt::Debug for CacheInner<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheInner")
            .field("locks", &self.locks)
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl<A: Access> CacheInner<A> {
    /// Return stat metadata, using the stat cache when `stat_ttl` is set.
    ///
    /// On cache miss the underlying accessor is queried and the result is
    /// stored so subsequent calls within the TTL window avoid an S3
    /// round-trip.
    async fn cached_stat(&self, path: &str) -> Result<Metadata> {
        if let Some(ttl) = self.stat_ttl {
            let key = CacheKey { path: path.to_string() };
            if let Some(entry) = self.stat_cache.get(&key) {
                let (inserted_at, metadata) = entry.value();
                if inserted_at.elapsed() < ttl {
                    self.metrics.record_stat_hit();
                    return Ok(metadata.clone());
                }
                drop(entry);
                self.stat_cache.remove(&key);
            }
            self.metrics.record_stat_miss();
            let rp = self.accessor.stat(path, OpStat::default()).await?;
            let metadata = rp.into_metadata();
            self.stat_cache.insert(key, (Instant::now(), metadata.clone()));
            Ok(metadata)
        } else {
            let rp = self.accessor.stat(path, OpStat::default()).await?;
            Ok(rp.into_metadata())
        }
    }
}

/// The accessor produced by [`CacheLayer`].
pub(crate) struct CacheAccessor<A: Access> {
    inner: Arc<CacheInner<A>>,
}

impl<A: Access> std::fmt::Debug for CacheAccessor<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheAccessor").field("inner", &self.inner).finish()
    }
}

/// Build the response pair from extracted bytes.
fn make_read_response(data: Vec<u8>, range_start: u64, range_end: u64) -> (RpRead, Buffer) {
    let buf = Buffer::from(data);
    let content_range = BytesContentRange::default()
        .with_range(range_start, range_end - 1)
        .with_size(range_end);
    let rp = RpRead::new().with_size(Some(buf.len() as u64)).with_range(Some(content_range));
    (rp, buf)
}

impl<A: Access> LayeredAccess for CacheAccessor<A> {
    type Inner = A;
    type Reader = Buffer;
    type Writer = CacheWriter<A>;
    type Lister = A::Lister;
    type Deleter = CacheDeleter<A>;

    fn inner(&self) -> &Self::Inner {
        &self.inner.accessor
    }

    fn info(&self) -> Arc<AccessorInfo> {
        self.inner.accessor.info()
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let span = tracing::debug_span!("cache_read", path = path);
        let inner = self.inner.clone();
        let path = path.to_string();

        async move {
            let start = Instant::now();
            let key = CacheKey { path: path.clone() };

            let range = args.range();
            let range_start = range.offset();
            let range_size = range.size();

            // --- Fast path: lock-free cache hit ---
            // Try to serve directly from the cached entry without cloning
            // the entire CacheValue. The foyer CacheEntry holds a
            // ref-counted handle, so `.value()` is a cheap borrow.
            if let Some(size) = range_size {
                let range_end = range_start + size;
                if range_start < range_end {
                    if let Some(entry) = inner
                        .cache
                        .get(&key)
                        .await
                        .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?
                    {
                        let cached = entry.value();
                        if cached.covers(range_start, range_end) {
                            tracing::debug!(
                                offset = range_start,
                                length = range_end - range_start,
                                "Cache hit (fast path)"
                            );
                            let data = cached
                                .read_range(range_start, range_end)
                                .ok_or_else(|| Error::new(ErrorKind::Unexpected, "cache hit but read_range failed"))?;
                            inner.metrics.record_hit("fast_path", range_end - range_start);
                            inner.metrics.record_read_duration(start);
                            return Ok(make_read_response(data, range_start, range_end));
                        }
                    }
                }
            }

            // --- Slow path: need to fetch gaps, requires per-key lock ---

            // Determine the end of the requested range.
            let range_end = if let Some(size) = range_size {
                range_start + size
            } else {
                // Full-file read: stat to get content length (via stat cache).
                inner.cached_stat(&path).await?.content_length()
            };

            if range_start >= range_end {
                inner.metrics.record_read_duration(start);
                return Ok((RpRead::new(), Buffer::new()));
            }

            // Per-key lock to serialize cache updates.
            let lock = inner.locks.lock(&key);
            let _guard = lock.lock().await;

            // Re-check cache under lock — another task may have filled
            // the gaps while we waited.
            if let Some(entry) = inner
                .cache
                .get(&key)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?
            {
                let cached = entry.value();
                if cached.covers(range_start, range_end) {
                    tracing::debug!(
                        offset = range_start,
                        length = range_end - range_start,
                        "Cache hit (after lock)"
                    );
                    let data = cached
                        .read_range(range_start, range_end)
                        .ok_or_else(|| Error::new(ErrorKind::Unexpected, "cache hit but read_range failed"))?;
                    inner.metrics.record_hit("lock_path", range_end - range_start);
                    inner.metrics.record_read_duration(start);
                    return Ok(make_read_response(data, range_start, range_end));
                }
            }

            // Find uncached gaps. We need to clone the value here since
            // we must mutate it to merge fetched data.
            let existing: Option<CacheValue> = inner
                .cache
                .get(&key)
                .await
                .map_err(|e| Error::new(ErrorKind::Unexpected, e.to_string()))?
                .map(|entry| entry.value().clone());

            #[allow(clippy::single_range_in_vec_init)]
            let gaps = existing
                .as_ref()
                .map_or_else(|| vec![range_start..range_end], |v| v.find_gaps(range_start, range_end));

            let gap_bytes: u64 = gaps.iter().map(|g| g.end - g.start).sum();
            tracing::debug!(
                offset = range_start,
                length = range_end - range_start,
                gaps = gaps.len(),
                gap_bytes = gap_bytes,
                partial = existing.is_some(),
                "Cache miss"
            );

            inner.metrics.record_miss(gaps.len(), gap_bytes);

            // Fetch all gaps concurrently from the backend.
            let fetch_futures = gaps.iter().map(|gap| {
                let gap_range = BytesRange::new(gap.start, Some(gap.end - gap.start));
                let gap_start = gap.start;
                let path = path.clone();
                let inner = inner.clone();
                async move {
                    let (_, mut reader) = inner.accessor.read(&path, OpRead::default().with_range(gap_range)).await?;
                    let data = reader.read_all().await?;
                    Ok::<_, Error>((gap_start, data))
                }
            });
            let fetched = try_join_all(fetch_futures).await?;

            // Merge into cache value.
            let mut merged = existing.unwrap_or_else(CacheValue::new);
            for (offset, data) in &fetched {
                merged.insert_range(*offset, &data.to_bytes());
            }

            // Extract requested range before inserting into cache (avoids
            // cloning `merged` — we move it into the cache after).
            let data = merged
                .read_range(range_start, range_end)
                .ok_or_else(|| Error::new(ErrorKind::Unexpected, "cache merge failed to cover range"))?;
            let response = make_read_response(data, range_start, range_end);

            // Update cache.
            inner.cache.insert(key, merged);

            inner.metrics.record_read_duration(start);
            Ok(response)
        }
        .instrument(span)
        .await
    }

    fn stat(&self, path: &str, _args: OpStat) -> impl Future<Output = Result<RpStat>> + MaybeSend {
        let span = tracing::debug_span!("cache_stat", path = path);
        let inner = self.inner.clone();
        let path = path.to_string();
        async move {
            let metadata = inner.cached_stat(&path).await?;
            Ok(RpStat::new(metadata))
        }
        .instrument(span)
    }

    fn write(&self, path: &str, args: OpWrite) -> impl Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend {
        let inner = self.inner.clone();
        let path = path.to_string();
        async move {
            let (rp, w) = inner.accessor.write(&path, args).await?;
            Ok((rp, CacheWriter::new(w, path, inner)))
        }
    }

    fn delete(&self) -> impl Future<Output = Result<(RpDelete, Self::Deleter)>> + MaybeSend {
        let inner = self.inner.clone();
        async move {
            let (rp, d) = inner.accessor.delete().await?;
            Ok((rp, CacheDeleter::new(d, inner)))
        }
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.accessor.list(path, args).await
    }
}

// ---------------------------------------------------------------------------
// CacheWriter
// ---------------------------------------------------------------------------

/// Writer that buffers data and populates the cache on close.
///
/// Tracks total bytes written so that chunks are only buffered while
/// below `max_write_cache_size`. Once exceeded, further chunks are
/// passed through to the inner writer without buffering to avoid
/// holding large payloads in memory.
pub(crate) struct CacheWriter<A: Access> {
    w: A::Writer,
    buf: oio::QueueBuf,
    /// Running total of bytes passed to `write()`.
    buffered_bytes: usize,
    /// Set to `true` once `buffered_bytes` exceeds the configured limit,
    /// signalling that no further chunks should be buffered.
    exceeded: bool,
    path: String,
    inner: Arc<CacheInner<A>>,
}

impl<A: Access> CacheWriter<A> {
    fn new(w: A::Writer, path: String, inner: Arc<CacheInner<A>>) -> Self {
        Self {
            w,
            buf: oio::QueueBuf::new(),
            buffered_bytes: 0,
            exceeded: false,
            path,
            inner,
        }
    }
}

impl<A: Access> oio::Write for CacheWriter<A> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let chunk_len = bs.len();
        self.buffered_bytes += chunk_len;

        // Only buffer while below the write-cache size limit.
        if !self.exceeded {
            if self.inner.max_write_cache_size.is_some_and(|limit| self.buffered_bytes > limit) {
                // Crossed the threshold — drop what we've buffered so far
                // and stop buffering future chunks.
                self.exceeded = true;
                self.buf.clear();
            } else {
                self.buf.push(bs.clone());
            }
        }

        self.w.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let metadata = self.w.close().await?;

        let key = CacheKey {
            path: self.path.clone(),
        };

        if self.exceeded {
            tracing::trace!(
                path = self.path,
                bytes = self.buffered_bytes,
                "Write exceeds max_write_cache_size, skipping cache"
            );
            // Evict any stale entry from a previous, smaller write to this path.
            self.inner.cache.remove(&key);
            self.inner.stat_cache.remove(&key);
        } else {
            let buffer = self.buf.clone().collect();
            let mut value = CacheValue::new();
            value.insert_range(0, &buffer.to_bytes());
            self.inner.cache.insert(key.clone(), value);
            self.inner.stat_cache.remove(&key);
            tracing::trace!(
                path = self.path,
                bytes = self.buffered_bytes,
                "Cache populated on write"
            );
        }

        #[allow(clippy::cast_possible_truncation)]
        self.inner.metrics.record_write_bytes(self.buffered_bytes as u64);

        Ok(metadata)
    }

    async fn abort(&mut self) -> Result<()> {
        self.buf.clear();
        self.buffered_bytes = 0;
        self.exceeded = false;
        self.w.abort().await
    }
}

// ---------------------------------------------------------------------------
// CacheDeleter
// ---------------------------------------------------------------------------

/// Deleter that evicts cache entries on delete.
pub(crate) struct CacheDeleter<A: Access> {
    deleter: A::Deleter,
    keys: Vec<CacheKey>,
    inner: Arc<CacheInner<A>>,
}

impl<A: Access> CacheDeleter<A> {
    const fn new(deleter: A::Deleter, inner: Arc<CacheInner<A>>) -> Self {
        Self {
            deleter,
            keys: Vec::new(),
            inner,
        }
    }
}

impl<A: Access> oio::Delete for CacheDeleter<A> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.deleter.delete(path, args)?;
        self.keys.push(CacheKey { path: path.to_string() });
        Ok(())
    }

    async fn flush(&mut self) -> Result<usize> {
        let count = self.deleter.flush().await?;
        for key in &self.keys {
            tracing::trace!(path = key.path, "Cache evicted on delete");
            self.inner.cache.remove(key);
            self.inner.stat_cache.remove(key);
            self.inner.locks.remove(key);
            self.inner.metrics.record_eviction();
        }
        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// build_storage_cache
// ---------------------------------------------------------------------------

/// Build a foyer hybrid cache from [`CacheConfig`].
///
/// The returned cache uses a memory tier with LRU eviction and a
/// disk tier backed by a direct-filesystem device.
///
/// # Errors
///
/// Returns an error if:
/// - `memory_size_mb` or `disk_size_mb` overflows when converted to bytes
/// - The foyer cache fails to initialize (e.g., filesystem error)
pub async fn build_storage_cache(config: &CacheConfig) -> CommonResult<StorageCache> {
    let memory_bytes = config.memory_size_mb.checked_mul(1024 * 1024).ok_or_else(|| {
        CommonError::Config(format!(
            "memory_size_mb ({}) overflows when converted to bytes",
            config.memory_size_mb
        ))
    })?;
    let disk_bytes = config.disk_size_mb.checked_mul(1024 * 1024).ok_or_else(|| {
        CommonError::Config(format!(
            "disk_size_mb ({}) overflows when converted to bytes",
            config.disk_size_mb
        ))
    })?;

    let device = foyer::FsDeviceBuilder::new(&config.disk_dir)
        .with_capacity(disk_bytes)
        .build()
        .map_err(|e| CommonError::Config(format!("Failed to build storage cache: {e}")))?;

    foyer::HybridCacheBuilder::new()
        .memory(memory_bytes)
        .with_weighter(|_key: &CacheKey, value: &CacheValue| value.size_bytes())
        .storage()
        .with_engine_config(foyer::BlockEngineConfig::new(device))
        .build()
        .await
        .map_err(|e| CommonError::Config(format!("Failed to build storage cache: {e}")))
}

/// Register `OpenTelemetry` observable instruments that expose foyer
/// [`HybridCache`] internals as Prometheus-scrapeable gauges and counters.
///
/// The instruments are sampled lazily on each Prometheus scrape (via the
/// `OTel` callback mechanism), so they add zero overhead between scrapes.
///
/// # Metrics
///
/// | Name | Type | Unit | Description |
/// |------|------|------|-------------|
/// | `icegate_foyer_memory_usage_bytes` | Gauge | `By` | Current memory tier usage |
/// | `icegate_foyer_memory_capacity_bytes` | Gauge | `By` | Memory tier capacity |
/// | `icegate_foyer_entries` | Gauge | — | Number of cached entries |
/// | `icegate_foyer_disk_write_bytes_total` | Counter | `By` | Cumulative disk writes |
/// | `icegate_foyer_disk_read_bytes_total` | Counter | `By` | Cumulative disk reads |
/// | `icegate_foyer_disk_write_ios_total` | Counter | — | Cumulative disk write IOs |
/// | `icegate_foyer_disk_read_ios_total` | Counter | — | Cumulative disk read IOs |
/// | `icegate_foyer_read_throttled` | Gauge | — | 1 if reads are throttled |
/// | `icegate_foyer_write_throttled` | Gauge | — | 1 if writes are throttled |
pub fn register_foyer_metrics(cache: &StorageCache, meter: &Meter) {
    let c = cache.clone();
    meter
        .u64_observable_gauge("icegate_foyer_memory_usage_bytes")
        .with_description("Current foyer memory tier usage")
        .with_unit("By")
        .with_callback(move |gauge| {
            #[allow(clippy::cast_possible_truncation)]
            gauge.observe(c.memory().usage() as u64, &[]);
        })
        .build();

    let c = cache.clone();
    meter
        .u64_observable_gauge("icegate_foyer_memory_capacity_bytes")
        .with_description("Foyer memory tier capacity")
        .with_unit("By")
        .with_callback(move |gauge| {
            #[allow(clippy::cast_possible_truncation)]
            gauge.observe(c.memory().capacity() as u64, &[]);
        })
        .build();

    let c = cache.clone();
    meter
        .u64_observable_gauge("icegate_foyer_entries")
        .with_description("Number of entries in foyer memory tier")
        .with_callback(move |gauge| {
            #[allow(clippy::cast_possible_truncation)]
            gauge.observe(c.memory().entries() as u64, &[]);
        })
        .build();

    let c = cache.clone();
    meter
        .u64_observable_gauge("icegate_foyer_disk_write_bytes_total")
        .with_description("Cumulative bytes written to foyer disk tier")
        .with_unit("By")
        .with_callback(move |gauge| {
            #[allow(clippy::cast_possible_truncation)]
            gauge.observe(c.statistics().disk_write_bytes() as u64, &[]);
        })
        .build();

    let c = cache.clone();
    meter
        .u64_observable_gauge("icegate_foyer_disk_read_bytes_total")
        .with_description("Cumulative bytes read from foyer disk tier")
        .with_unit("By")
        .with_callback(move |gauge| {
            #[allow(clippy::cast_possible_truncation)]
            gauge.observe(c.statistics().disk_read_bytes() as u64, &[]);
        })
        .build();

    let c = cache.clone();
    meter
        .u64_observable_gauge("icegate_foyer_disk_write_ios_total")
        .with_description("Cumulative foyer disk write IO operations")
        .with_callback(move |gauge| {
            #[allow(clippy::cast_possible_truncation)]
            gauge.observe(c.statistics().disk_write_ios() as u64, &[]);
        })
        .build();

    let c = cache.clone();
    meter
        .u64_observable_gauge("icegate_foyer_disk_read_ios_total")
        .with_description("Cumulative foyer disk read IO operations")
        .with_callback(move |gauge| {
            #[allow(clippy::cast_possible_truncation)]
            gauge.observe(c.statistics().disk_read_ios() as u64, &[]);
        })
        .build();

    let c = cache.clone();
    meter
        .u64_observable_gauge("icegate_foyer_read_throttled")
        .with_description("Whether foyer read operations are throttled (1=yes, 0=no)")
        .with_callback(move |gauge| {
            gauge.observe(u64::from(c.statistics().is_read_throttled()), &[]);
        })
        .build();

    let c = cache.clone();
    meter
        .u64_observable_gauge("icegate_foyer_write_throttled")
        .with_description("Whether foyer write operations are throttled (1=yes, 0=no)")
        .with_callback(move |gauge| {
            gauge.observe(u64::from(c.statistics().is_write_throttled()), &[]);
        })
        .build();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_read_single_segment() {
        let mut v = CacheValue::new();
        v.insert_range(10, &[1, 2, 3, 4, 5]);
        assert_eq!(v.read_range(10, 15), Some(vec![1, 2, 3, 4, 5]));
        assert_eq!(v.read_range(11, 14), Some(vec![2, 3, 4]));
        assert!(v.covers(10, 15));
        assert!(!v.covers(9, 15));
        assert!(!v.covers(10, 16));
    }

    #[test]
    fn test_merge_overlapping_segments() {
        let mut v = CacheValue::new();
        v.insert_range(0, &[1, 2, 3, 4, 5]);
        v.insert_range(3, &[10, 11, 12, 13]);
        // Merged: [1, 2, 3, 10, 11, 12, 13] at offset 0
        assert_eq!(v.read_range(0, 7), Some(vec![1, 2, 3, 10, 11, 12, 13]));
        assert_eq!(v.segments.len(), 1);
    }

    #[test]
    fn test_merge_adjacent_segments() {
        let mut v = CacheValue::new();
        v.insert_range(0, &[1, 2, 3]);
        v.insert_range(3, &[4, 5, 6]);
        assert_eq!(v.read_range(0, 6), Some(vec![1, 2, 3, 4, 5, 6]));
        assert_eq!(v.segments.len(), 1);
    }

    #[test]
    fn test_find_gaps() {
        let mut v = CacheValue::new();
        v.insert_range(10, &[0; 5]); // [10..15)
        v.insert_range(20, &[0; 5]); // [20..25)

        let gaps = v.find_gaps(0, 30);
        assert_eq!(gaps, vec![0..10, 15..20, 25..30]);

        let gaps = v.find_gaps(10, 25);
        assert_eq!(gaps, vec![15..20]);

        let gaps = v.find_gaps(12, 22);
        assert_eq!(gaps, vec![15..20]);
    }

    #[test]
    fn test_read_range_with_gap() {
        let mut v = CacheValue::new();
        v.insert_range(0, &[1, 2, 3]);
        v.insert_range(5, &[6, 7, 8]);
        // Gap at [3..5), so read_range should return None.
        assert_eq!(v.read_range(0, 8), None);
    }

    #[test]
    fn test_insert_empty_data() {
        let mut v = CacheValue::new();
        v.insert_range(10, &[]);
        assert!(v.segments.is_empty());
    }

    #[test]
    fn test_covers_empty_range() {
        let v = CacheValue::new();
        assert!(v.covers(5, 5));
    }

    /// Build an in-memory `OpenDAL` operator with the `CacheLayer` applied.
    ///
    /// The `stat_ttl` controls the stat cache TTL; `None` disables it.
    async fn build_test_operator(stat_ttl: Option<Duration>) -> opendal::Operator {
        let metrics = CacheMetrics::new_disabled();
        let cache: StorageCache = foyer::HybridCacheBuilder::new()
            .memory(64 * 1024)
            .with_weighter(|_key: &CacheKey, value: &CacheValue| value.size_bytes())
            .storage()
            .build()
            .await
            .expect("test cache");

        opendal::Operator::from_config(opendal::services::MemoryConfig::default())
            .expect("memory operator")
            .layer(CacheLayer::new(cache, metrics, stat_ttl, None))
            .finish()
    }

    #[tokio::test]
    async fn test_stat_cache_hit_returns_cached_metadata() {
        let op = build_test_operator(Some(Duration::from_secs(60))).await;

        // Write a file so stat has something to return.
        op.write("test.txt", "hello").await.expect("write");

        // First stat — cache miss, fetches from backend.
        let meta1 = op.stat("test.txt").await.expect("stat1");

        // Second stat — should be served from the stat cache.
        let meta2 = op.stat("test.txt").await.expect("stat2");

        assert_eq!(meta1.content_length(), meta2.content_length());
    }

    #[tokio::test]
    async fn test_stat_cache_expires_after_ttl() {
        let op = build_test_operator(Some(Duration::from_millis(50))).await;

        op.write("expire.txt", "data").await.expect("write");

        // Populate the stat cache.
        let _ = op.stat("expire.txt").await.expect("stat1");

        // Wait for TTL to expire.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // After TTL, the stat should fall through to the backend again
        // (no error means it fetched successfully).
        let meta = op.stat("expire.txt").await.expect("stat after expiry");
        assert_eq!(meta.content_length(), 4);
    }

    #[tokio::test]
    async fn test_stat_cache_invalidated_on_delete() {
        let op = build_test_operator(Some(Duration::from_secs(60))).await;

        op.write("del.txt", "data").await.expect("write");

        // Populate stat cache.
        let _ = op.stat("del.txt").await.expect("stat");

        // Delete the file — should invalidate stat cache.
        op.delete("del.txt").await.expect("delete");

        // Stat after delete should return NotFound, not stale cached data.
        let result: opendal::Result<opendal::Metadata> = op.stat("del.txt").await;
        assert!(result.is_err());
    }
}
