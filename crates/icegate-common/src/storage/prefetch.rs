//! Parquet prefetch layer for `OpenDAL`.
//!
//! Intercepts reads on `.parquet` files, detects footer reads by checking
//! for the `PAR1` magic suffix, parses the Parquet metadata, and
//! proactively issues background reads for column metadata (and
//! optionally dictionary pages) that will be needed next. Background reads flow through the
//! inner layer stack
//! (typically the foyer cache layer) so their results land in cache
//! before the query engine asks for them.
//!
//! # Layer stack position
//!
//! ```text
//! PrefetchLayer -> CacheLayer -> OtelMetrics -> OtelTrace -> S3
//! ```
//!
//! The prefetch layer sits outermost so background reads warm the cache.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::{DashMap, DashSet};
use opendal::raw::oio::Read;
use opendal::raw::{
    Access, AccessorInfo, Layer, LayeredAccess, MaybeSend, OpList, OpRead, OpWrite, RpDelete, RpList, RpRead, RpWrite,
};
use opendal::{Buffer, Result};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, MeterProvider as _};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::Instrument;

// ---------------------------------------------------------------------------
// PrefetchMetrics
// ---------------------------------------------------------------------------

/// Maximum time to wait for an in-flight prefetch before falling back to
/// a direct read. Guards against lost `Notify` signals (race between
/// `find_overlap` and `.notified().await`) and abandoned prefetch tasks.
const INFLIGHT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Histogram bucket boundaries (in seconds) for prefetch task/wait durations.
const PREFETCH_DURATION_BOUNDARIES: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0];

/// Histogram bucket boundaries (in bytes) for prefetch byte metrics.
const PREFETCH_BYTES_BOUNDARIES: &[f64] = &[
    1_024.0,
    4_096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
];

/// Histogram bucket boundaries for ranges-per-file counts.
const PREFETCH_RANGES_BOUNDARIES: &[f64] = &[0.0, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0];

/// `OpenTelemetry` metrics for the Parquet prefetch layer.
///
/// Tracks footer detection, prefetch scheduling, task outcomes, and
/// in-flight waits. All metric names are prefixed with
/// `icegate_storage_prefetch_`.
#[derive(Clone)]
pub(crate) struct PrefetchMetrics {
    enabled: bool,
    /// Parquet footers successfully detected.
    footers_detected_total: Counter<u64>,
    /// Parquet footer/metadata decode failures.
    footer_parse_failures_total: Counter<u64>,
    /// Number of prefetch ranges scheduled per file.
    ranges_per_file: Histogram<f64>,
    /// Total bytes scheduled for prefetch per file.
    bytes_scheduled: Histogram<f64>,
    /// Completed prefetch tasks by outcome (`success` or `failure`).
    tasks_completed_total: Counter<u64>,
    /// Duration of individual prefetch tasks.
    task_duration: Histogram<f64>,
    /// Prefetch ranges scheduled by metadata type.
    ranges_by_type_total: Counter<u64>,
    /// Number of reads that waited for in-flight prefetches.
    inflight_waits_total: Counter<u64>,
    /// Duration spent waiting for in-flight prefetches.
    inflight_wait_duration: Histogram<f64>,
}

#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for PrefetchMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefetchMetrics").field("enabled", &self.enabled).finish()
    }
}

impl PrefetchMetrics {
    /// Build a metrics recorder using the provided meter.
    pub(crate) fn new(meter: &Meter) -> Self {
        Self {
            enabled: true,
            footers_detected_total: meter
                .u64_counter("icegate_storage_prefetch_footers_detected")
                .with_description("Parquet footers detected")
                .build(),
            footer_parse_failures_total: meter
                .u64_counter("icegate_storage_prefetch_footer_parse_failures")
                .with_description("Parquet footer/metadata decode failures")
                .build(),
            ranges_per_file: meter
                .f64_histogram("icegate_storage_prefetch_ranges_per_file")
                .with_description("Prefetch ranges scheduled per file")
                .with_boundaries(PREFETCH_RANGES_BOUNDARIES.to_vec())
                .build(),
            bytes_scheduled: meter
                .f64_histogram("icegate_storage_prefetch_bytes_scheduled")
                .with_description("Bytes scheduled for prefetch per file")
                .with_boundaries(PREFETCH_BYTES_BOUNDARIES.to_vec())
                .build(),
            tasks_completed_total: meter
                .u64_counter("icegate_storage_prefetch_tasks_completed")
                .with_description("Prefetch tasks completed by outcome")
                .build(),
            task_duration: meter
                .f64_histogram("icegate_storage_prefetch_task_duration")
                .with_description("Prefetch task duration")
                .with_unit("s")
                .with_boundaries(PREFETCH_DURATION_BOUNDARIES.to_vec())
                .build(),
            ranges_by_type_total: meter
                .u64_counter("icegate_storage_prefetch_ranges_by_type")
                .with_description("Prefetch ranges scheduled by metadata type")
                .build(),
            inflight_waits_total: meter
                .u64_counter("icegate_storage_prefetch_inflight_waits")
                .with_description("Reads that waited for in-flight prefetches")
                .build(),
            inflight_wait_duration: meter
                .f64_histogram("icegate_storage_prefetch_inflight_wait_duration")
                .with_description("Duration waiting for in-flight prefetches")
                .with_unit("s")
                .with_boundaries(PREFETCH_DURATION_BOUNDARIES.to_vec())
                .build(),
        }
    }

    /// Build a no-op metrics recorder for tests or when metrics are disabled.
    pub(crate) fn new_disabled() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("prefetch-disabled");
        Self {
            enabled: false,
            footers_detected_total: meter.u64_counter("icegate_storage_prefetch_footers_detected").build(),
            footer_parse_failures_total: meter.u64_counter("icegate_storage_prefetch_footer_parse_failures").build(),
            ranges_per_file: meter.f64_histogram("icegate_storage_prefetch_ranges_per_file").build(),
            bytes_scheduled: meter.f64_histogram("icegate_storage_prefetch_bytes_scheduled").build(),
            tasks_completed_total: meter.u64_counter("icegate_storage_prefetch_tasks_completed").build(),
            task_duration: meter.f64_histogram("icegate_storage_prefetch_task_duration").build(),
            ranges_by_type_total: meter.u64_counter("icegate_storage_prefetch_ranges_by_type").build(),
            inflight_waits_total: meter.u64_counter("icegate_storage_prefetch_inflight_waits").build(),
            inflight_wait_duration: meter.f64_histogram("icegate_storage_prefetch_inflight_wait_duration").build(),
        }
    }

    /// Record a detected Parquet footer.
    fn record_footer_detected(&self) {
        if !self.enabled {
            return;
        }
        self.footers_detected_total.add(1, &[]);
    }

    /// Record a footer/metadata decode failure.
    fn record_footer_parse_failure(&self) {
        if !self.enabled {
            return;
        }
        self.footer_parse_failures_total.add(1, &[]);
    }

    /// Record prefetch ranges and bytes scheduled for a file.
    #[allow(clippy::cast_precision_loss)]
    fn record_ranges_scheduled(&self, range_count: usize, total_bytes: u64) {
        if !self.enabled {
            return;
        }
        self.ranges_per_file.record(range_count as f64, &[]);
        self.bytes_scheduled.record(total_bytes as f64, &[]);
    }

    /// Record prefetch ranges by metadata type.
    #[allow(clippy::cast_possible_truncation)]
    fn record_ranges_by_type(&self, summary: &PrefetchRangeSummary) {
        if !self.enabled {
            return;
        }
        if summary.dictionary_pages > 0 {
            self.ranges_by_type_total.add(
                summary.dictionary_pages as u64,
                &[KeyValue::new("type", "dictionary_page")],
            );
        }
        if summary.column_indexes > 0 {
            self.ranges_by_type_total
                .add(summary.column_indexes as u64, &[KeyValue::new("type", "column_index")]);
        }
        if summary.offset_indexes > 0 {
            self.ranges_by_type_total
                .add(summary.offset_indexes as u64, &[KeyValue::new("type", "offset_index")]);
        }
        if summary.bloom_filters > 0 {
            self.ranges_by_type_total
                .add(summary.bloom_filters as u64, &[KeyValue::new("type", "bloom_filter")]);
        }
    }

    /// Record a completed prefetch task.
    fn record_task_completed(&self, outcome: &str, duration: Instant) {
        if !self.enabled {
            return;
        }
        self.tasks_completed_total
            .add(1, &[KeyValue::new("outcome", outcome.to_string())]);
        self.task_duration.record(duration.elapsed().as_secs_f64(), &[]);
    }

    /// Record a read that waited for an in-flight prefetch.
    fn record_inflight_wait(&self, duration: Instant) {
        if !self.enabled {
            return;
        }
        self.inflight_waits_total.add(1, &[]);
        self.inflight_wait_duration.record(duration.elapsed().as_secs_f64(), &[]);
    }
}

// ---------------------------------------------------------------------------
// PrefetchRangeSummary
// ---------------------------------------------------------------------------

/// Counts of prefetch ranges by metadata type, for instrumentation.
#[derive(Debug, Default)]
struct PrefetchRangeSummary {
    /// Dictionary page ranges collected.
    dictionary_pages: usize,
    /// Column index ranges collected.
    column_indexes: usize,
    /// Offset index ranges collected.
    offset_indexes: usize,
    /// Bloom filter ranges collected.
    bloom_filters: usize,
}

// ---------------------------------------------------------------------------
// PrefetchConfig
// ---------------------------------------------------------------------------

/// Configuration knobs for Parquet metadata prefetching.
///
/// When enabled, the prefetch layer detects footer reads on `.parquet`
/// files and proactively issues background reads for **metadata-only**
/// structures referenced by the footer: column indexes, offset indexes,
/// and bloom filters. Dictionary pages are **opt-in** via
/// [`prefetch_dictionary_pages`](Self::prefetch_dictionary_pages) because
/// they can be significantly larger than other metadata. Data pages are
/// never prefetched — only the metadata that the query engine reads
/// before deciding which pages to fetch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefetchConfig {
    /// Whether prefetching is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Maximum total bytes to prefetch per file (budget).
    /// Default: 4 `MiB` (metadata is typically small).
    #[serde(default = "default_max_prefetch_bytes")]
    pub max_prefetch_bytes: usize,
    /// If `Some`, only prefetch metadata for columns whose name matches
    /// one of these entries. If `None`, all columns are eligible.
    #[serde(default)]
    pub prefetch_columns: Option<Vec<String>>,
    /// Whether to prefetch dictionary pages for dictionary-encoded columns.
    /// Default: `false`.
    #[serde(default)]
    pub prefetch_dictionary_pages: bool,
    /// Maximum number of concurrent prefetch tasks.
    /// Default: 32.
    #[serde(default = "default_max_concurrent_prefetches")]
    pub max_concurrent_prefetches: usize,
}

const fn default_max_concurrent_prefetches() -> usize {
    32
}

const fn default_enabled() -> bool {
    false
}
const fn default_max_prefetch_bytes() -> usize {
    4 * 1024 * 1024 // 4 MiB — metadata is much smaller than data
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            max_prefetch_bytes: default_max_prefetch_bytes(),
            prefetch_columns: None,
            prefetch_dictionary_pages: false,
            max_concurrent_prefetches: default_max_concurrent_prefetches(),
        }
    }
}

// ---------------------------------------------------------------------------
// InFlightTracker
// ---------------------------------------------------------------------------

/// Coordinates prefetch tasks with real reads to avoid duplicate S3 requests.
///
/// Before a prefetch task issues a read, it registers the byte range in
/// `pending`. When the prefetch completes (success or failure), it removes
/// the entry and notifies all waiters. A real `read()` that overlaps an
/// in-flight prefetch awaits the notification instead of issuing a
/// duplicate request.
///
/// Uses [`tokio::sync::watch`] instead of [`tokio::sync::Notify`] so that
/// completions are never lost: a `watch::Receiver` observes the latest
/// value regardless of when it subscribes, eliminating the race where
/// `notify_waiters()` fires between `find_overlap` returning and the
/// caller awaiting.
///
/// Uses a two-level index: path → `Vec<(offset, length, watch)>` to
/// avoid scanning unrelated paths on overlap checks.
struct InFlightTracker {
    /// Maps path → list of in-flight `(offset, length, watch_tx)` entries.
    pending: DashMap<Arc<str>, Vec<InFlightEntry>>,
}

/// A single in-flight prefetch range.
///
/// The `watch` sender is held by the prefetch task. When the task
/// completes it sends `true`, waking all receivers. Receivers created
/// after completion see `true` immediately.
struct InFlightEntry {
    offset: u64,
    length: u64,
    /// Sends `true` on completion. Initial value is `false`.
    done_tx: tokio::sync::watch::Sender<bool>,
}

impl InFlightTracker {
    fn new() -> Self {
        Self {
            pending: DashMap::new(),
        }
    }

    /// Register a byte range as in-flight.
    ///
    /// The entry stays in the tracker until [`complete`] is called, which
    /// sends `true` on the watch channel and removes the entry. If the
    /// entry is never completed (e.g. task panic), the `Sender` is dropped
    /// when the tracker entry is eventually cleaned up, causing all
    /// receivers to get a `RecvError`.
    fn register(&self, path: Arc<str>, offset: u64, length: u64) {
        let (tx, _rx) = tokio::sync::watch::channel(false);
        self.pending.entry(path).or_default().push(InFlightEntry {
            offset,
            length,
            done_tx: tx,
        });
    }

    /// Mark a byte range as complete — removes the entry and wakes all
    /// receivers by sending `true`.
    fn complete(&self, path: &str, offset: u64, length: u64) {
        if let Some(mut entries) = self.pending.get_mut(path) {
            if let Some(pos) = entries.iter().position(|e| e.offset == offset && e.length == length) {
                let entry = entries.swap_remove(pos);
                // Ignoring send error: receivers may have been dropped.
                let _ = entry.done_tx.send(true);
            }
            // Clean up empty entry lists.
            if entries.is_empty() {
                drop(entries);
                self.pending.remove(path);
            }
        }
    }

    /// If any in-flight prefetch overlaps `[offset, offset+length)` for the
    /// given path, return a [`watch::Receiver`] that resolves when the
    /// prefetch completes.
    ///
    /// Because `watch` preserves the last sent value, the receiver sees
    /// `true` immediately if the prefetch already completed — no race.
    fn find_overlap(&self, path: &str, offset: u64, length: u64) -> Option<tokio::sync::watch::Receiver<bool>> {
        let end = offset + length;
        let entries = self.pending.get(path)?;
        let rx = entries
            .iter()
            .find(|entry| offset < entry.offset + entry.length && entry.offset < end)
            .map(|entry| entry.done_tx.subscribe());
        drop(entries);
        rx
    }
}

// ---------------------------------------------------------------------------
// PrefetchLayer
// ---------------------------------------------------------------------------

/// `OpenDAL` layer that prefetches Parquet column chunks after detecting
/// footer reads.
///
/// Implements [`Layer`] so it can be inserted into an operator's layer
/// stack. The layer itself stores no data — all prefetched bytes land in
/// the cache layer below.
pub(crate) struct PrefetchLayer {
    config: Arc<PrefetchConfig>,
    seen: Arc<DashSet<Arc<str>>>,
    tracker: Arc<InFlightTracker>,
    metrics: PrefetchMetrics,
}

impl PrefetchLayer {
    /// Create a new prefetch layer from configuration with metrics.
    pub(crate) fn new(config: PrefetchConfig, metrics: PrefetchMetrics) -> Self {
        Self {
            config: Arc::new(config),
            seen: Arc::new(DashSet::new()),
            tracker: Arc::new(InFlightTracker::new()),
            metrics,
        }
    }
}

impl<A: Access> Layer<A> for PrefetchLayer {
    type LayeredAccess = PrefetchAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        PrefetchAccessor {
            inner: Arc::new(PrefetchInner {
                accessor: inner,
                prefetch_semaphore: Arc::new(Semaphore::new(self.config.max_concurrent_prefetches.max(1))),
                config: self.config.clone(),
                seen: self.seen.clone(),
                tracker: self.tracker.clone(),
                pending_footers: DashMap::new(),
                metrics: self.metrics.clone(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// PrefetchAccessor
// ---------------------------------------------------------------------------

/// Maximum number of entries in the `seen` set before it is cleared.
/// Prevents unbounded memory growth for long-running processes.
const MAX_SEEN_ENTRIES: usize = 100_000;

/// Shared state between the accessor and spawned prefetch tasks.
struct PrefetchInner<A: Access> {
    accessor: A,
    config: Arc<PrefetchConfig>,
    seen: Arc<DashSet<Arc<str>>>,
    tracker: Arc<InFlightTracker>,
    /// Tracks files where we saw a PAR1 footer suffix but the buffer was
    /// too small to contain the full metadata. Stores the expected
    /// `metadata_len` so the subsequent metadata read (which does NOT end
    /// with PAR1) can be detected and decoded.
    pending_footers: DashMap<Arc<str>, usize>,
    metrics: PrefetchMetrics,
    /// Bounds the number of concurrent prefetch tasks to avoid
    /// overwhelming the backend with parallel reads.
    prefetch_semaphore: Arc<Semaphore>,
}

/// The accessor produced by [`PrefetchLayer`].
///
/// On every read:
/// 1. Checks for an in-flight prefetch overlapping the requested range
///    and waits if found.
/// 2. Delegates to the inner accessor.
/// 3. If the response looks like a Parquet footer (`.parquet` extension,
///    small size, `PAR1` magic), parses the metadata and spawns background
///    reads for column chunks.
pub(crate) struct PrefetchAccessor<A: Access> {
    inner: Arc<PrefetchInner<A>>,
}

/// Insert a path into the `seen` set, clearing it first if it has grown
/// beyond [`MAX_SEEN_ENTRIES`] to prevent unbounded memory growth.
fn bounded_seen_insert(seen: &DashSet<Arc<str>>, path: Arc<str>) {
    if seen.len() > MAX_SEEN_ENTRIES {
        seen.clear();
    }
    seen.insert(path);
}

/// Maximum read size (in bytes) considered for footer detection.
/// Reads larger than this are assumed to be column-chunk or full-file
/// reads and are not inspected.
const MAX_FOOTER_READ_SIZE: usize = 512 * 1024;

/// Minimum read size that could contain the 8-byte Parquet footer suffix
/// (4-byte metadata length + 4-byte `PAR1` magic).
const MIN_FOOTER_SUFFIX_SIZE: usize = 8;

impl<A: Access> LayeredAccess for PrefetchAccessor<A> {
    type Inner = A;
    type Reader = Buffer;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner.accessor
    }

    fn info(&self) -> Arc<AccessorInfo> {
        self.inner.accessor.info()
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let span = tracing::debug_span!("prefetch_read", path = path);
        let inner = self.inner.clone();
        let path = path.to_string();

        async move {
            // 1. If an in-flight prefetch overlaps this range, wait for it
            //    so the data is served from cache instead of a duplicate S3 hit.
            let range = args.range();
            let req_offset = range.offset();
            if let Some(req_size) = range.size() {
                if let Some(mut rx) = inner.tracker.find_overlap(&path, req_offset, req_size) {
                    tracing::debug!(offset = req_offset, length = req_size, "Waiting for in-flight prefetch");
                    let wait_start = Instant::now();
                    // `watch` preserves state: if the prefetch already
                    // completed, `wait_for` returns immediately.
                    // Timeout guards against a prefetch task that panics or
                    // is cancelled without sending completion.
                    match tokio::time::timeout(INFLIGHT_WAIT_TIMEOUT, rx.wait_for(|&done| done)).await {
                        Ok(Ok(_)) => {
                            inner.metrics.record_inflight_wait(wait_start);
                            tracing::debug!("In-flight prefetch complete, reading from cache");
                        }
                        Ok(Err(_)) | Err(_) => {
                            // Sender dropped (task cancelled) or timeout.
                            inner.metrics.record_inflight_wait(wait_start);
                            tracing::warn!(
                                offset = req_offset,
                                length = req_size,
                                elapsed_s = wait_start.elapsed().as_secs_f64(),
                                "In-flight prefetch wait failed or timed out, proceeding with read"
                            );
                        }
                    }
                }
            }

            // 2. Delegate to inner (cache layer).
            let (rp, mut reader) = inner.accessor.read(&path, args).await?;
            let buf = reader.read_all().await?;

            // 3. Footer detection + background prefetch spawning.
            //
            //    The parquet reader typically reads the footer in two steps:
            //      (a) Read the last 8 bytes: [4-byte metadata_len LE][PAR1]
            //      (b) Read the Thrift metadata bytes (no PAR1 at end)
            //
            //    We detect (a) by the PAR1 magic suffix. If the buffer is
            //    too small for the full metadata, we store the expected
            //    metadata_len in `pending_footers` so we can recognize
            //    read (b) when it arrives.
            if inner.config.enabled && path.ends_with(".parquet") && !inner.seen.contains(path.as_str()) {
                let path_arc: Arc<str> = Arc::from(path.as_str());

                // Check for a pending follow-up metadata read BEFORE
                // applying the size gate — the Thrift metadata blob can
                // exceed MAX_FOOTER_READ_SIZE for wide tables.
                if let Some(entry) = inner.pending_footers.get(path_arc.as_ref()) {
                    let expected_len = *entry;
                    drop(entry); // release DashMap read guard
                    let bytes = buf.to_bytes();
                    if bytes.len() >= expected_len {
                        // Case (b): follow-up metadata read after a small
                        // suffix probe. The buffer contains raw Thrift bytes
                        // without the PAR1 suffix. Only remove the pending
                        // entry now that we've matched the expected size.
                        inner.pending_footers.remove(path_arc.as_ref());
                        let meta_request_range = range.size().map(|s| (req_offset, s));
                        tracing::debug!(
                            footer_bytes = bytes.len(),
                            expected_len,
                            "Parquet metadata read detected (follow-up)"
                        );
                        try_decode_and_prefetch(&inner, path_arc, &bytes, meta_request_range);
                    }
                    // If bytes.len() < expected_len, this is a short
                    // intermediate read — leave the pending entry for
                    // the real metadata read.
                } else if should_check_for_footer(&path, buf.len()) {
                    // Case (a): initial read — check for PAR1 magic suffix.
                    let bytes = buf.to_bytes();
                    if is_parquet_footer(&bytes) {
                        let meta_request_range = range.size().map(|s| (req_offset, s));
                        tracing::debug!(footer_bytes = bytes.len(), "Parquet footer detected");
                        inner.metrics.record_footer_detected();
                        spawn_prefetches_if_needed(&inner, path_arc, &bytes, meta_request_range);
                    }
                }
            }

            // 4. Return original response unchanged.
            Ok((rp, buf))
        }
        .instrument(span)
        .await
    }

    fn write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> impl std::future::Future<Output = Result<(RpWrite, Self::Writer)>> + MaybeSend {
        self.inner.accessor.write(path, args)
    }

    fn delete(&self) -> impl std::future::Future<Output = Result<(RpDelete, Self::Deleter)>> + MaybeSend {
        self.inner.accessor.delete()
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.accessor.list(path, args).await
    }
}

/// Parse footer metadata from the read buffer and spawn background
/// reads for eligible column chunks.
///
/// `request_range` is the `(offset, length)` of the read that triggered
/// this call. Prefetch ranges fully contained within it are skipped
/// because the inner layer (cache) already has that data.
#[tracing::instrument(level = "debug", name = "spawn_prefetches_if_needed", skip(inner, bytes))]
fn spawn_prefetches_if_needed<A: Access>(
    inner: &Arc<PrefetchInner<A>>,
    path: Arc<str>,
    bytes: &[u8],
    request_range: Option<(u64, u64)>,
) {
    let metadata = match parse_footer_metadata(bytes) {
        FooterParseResult::Ok(m) => m,
        FooterParseResult::BufferTooSmall { metadata_len } => {
            // Expected for initial suffix reads (e.g., 8-byte footer
            // probe). Store the metadata length so we can detect the
            // follow-up metadata read that won't end with PAR1.
            tracing::debug!(
                %path,
                buffer_len = bytes.len(),
                metadata_len,
                "Footer suffix detected, waiting for metadata read"
            );
            inner.pending_footers.insert(path, metadata_len);
            return;
        }
        FooterParseResult::DecodeError(e) => {
            tracing::warn!(%path, error = %e, "Parquet footer metadata decode failed, skipping prefetch");
            inner.metrics.record_footer_parse_failure();
            // Mark as seen to avoid retrying decode on every read.
            bounded_seen_insert(&inner.seen, path);
            return;
        }
    };

    // Mark as seen only after successful parse — the initial small suffix
    // read must not prevent the subsequent full-metadata read from
    // triggering prefetch.
    bounded_seen_insert(&inner.seen, path.clone());

    let (raw_ranges, summary) = compute_prefetch_ranges(&metadata, &inner.config);
    if raw_ranges.is_empty() {
        return;
    }

    // Pass ownership to merge_ranges — avoids cloning the vec.
    let raw_count = raw_ranges.len();
    let merged = merge_ranges(raw_ranges);

    // Filter out ranges already covered by the metadata request itself.
    let to_prefetch = filter_covered_ranges(&merged, request_range);

    let total_bytes: u64 = to_prefetch.iter().map(|(_, l)| l).sum();
    tracing::debug!(
        %path,
        raw_ranges = raw_count,
        merged_ranges = merged.len(),
        prefetch_ranges = to_prefetch.len(),
        total_bytes,
        dictionary_pages = summary.dictionary_pages,
        column_indexes = summary.column_indexes,
        offset_indexes = summary.offset_indexes,
        bloom_filters = summary.bloom_filters,
        "Prefetching Parquet metadata"
    );

    inner.metrics.record_ranges_scheduled(to_prefetch.len(), total_bytes);
    inner.metrics.record_ranges_by_type(&summary);
    schedule_prefetch_tasks(inner, &path, &to_prefetch);
}

/// Spawn background read tasks for the given byte ranges.
///
/// Each task acquires a permit from the prefetch semaphore before reading,
/// bounding the number of concurrent prefetch tasks. The task reads a
/// merged range through the inner layer stack (so the result lands in the
/// cache) and notifies waiters on completion.
fn schedule_prefetch_tasks<A: Access>(inner: &Arc<PrefetchInner<A>>, path: &Arc<str>, ranges: &[(u64, u64)]) {
    for &(offset, length) in ranges {
        let inner = inner.clone();
        let path = path.clone();
        inner.tracker.register(path.clone(), offset, length);
        let task_span = tracing::debug_span!("prefetch_task", %path, offset, length);
        let semaphore = inner.prefetch_semaphore.clone();

        tokio::spawn(
            async move {
                // Acquire a permit to bound concurrent prefetch tasks.
                // If the semaphore is closed (shouldn't happen), skip gracefully.
                let Ok(_permit) = semaphore.acquire().await else {
                    inner.tracker.complete(&path, offset, length);
                    return;
                };

                let task_start = Instant::now();
                let range = opendal::raw::BytesRange::new(offset, Some(length));
                let result = inner.accessor.read(&path, OpRead::default().with_range(range)).await;

                // Whether success or failure, complete the tracker entry
                // so waiters are released. On failure, waiters will fetch
                // from S3 themselves (graceful degradation).
                inner.tracker.complete(&path, offset, length);

                match &result {
                    Ok(_) => {
                        inner.metrics.record_task_completed("success", task_start);
                        tracing::debug!(offset, length, "Prefetch task complete");
                    }
                    Err(e) => {
                        inner.metrics.record_task_completed("failure", task_start);
                        tracing::debug!(
                            offset,
                            length,
                            error = %e,
                            "Prefetch task failed (graceful degradation)"
                        );
                    }
                }
            }
            .instrument(task_span),
        );
    }
}

/// Decode raw Thrift metadata bytes (from a follow-up read after the
/// initial PAR1 suffix probe) and spawn prefetches on success.
///
/// Unlike [`spawn_prefetches_if_needed`], this function receives bytes that do NOT
/// include the 8-byte Parquet footer suffix — they are the raw Thrift
/// `FileMetaData` bytes only.
///
/// `request_range` is the `(offset, length)` of the read that triggered
/// this call. Prefetch ranges fully contained within it are skipped.
#[tracing::instrument(level = "debug", name = "try_decode_and_prefetch", skip(inner, bytes))]
fn try_decode_and_prefetch<A: Access>(
    inner: &Arc<PrefetchInner<A>>,
    path: Arc<str>,
    bytes: &[u8],
    request_range: Option<(u64, u64)>,
) {
    match ParquetMetaDataReader::decode_metadata(bytes) {
        Ok(metadata) => {
            bounded_seen_insert(&inner.seen, path.clone());

            let (raw_ranges, summary) = compute_prefetch_ranges(&metadata, &inner.config);
            if raw_ranges.is_empty() {
                return;
            }

            let raw_count = raw_ranges.len();
            let merged = merge_ranges(raw_ranges);

            // Filter out ranges already covered by the metadata request itself.
            let to_prefetch = filter_covered_ranges(&merged, request_range);

            let total_bytes: u64 = to_prefetch.iter().map(|(_, l)| l).sum();
            tracing::debug!(
                %path,
                raw_ranges = raw_count,
                merged_ranges = merged.len(),
                prefetch_ranges = to_prefetch.len(),
                total_bytes,
                dictionary_pages = summary.dictionary_pages,
                column_indexes = summary.column_indexes,
                offset_indexes = summary.offset_indexes,
                bloom_filters = summary.bloom_filters,
                "Prefetching Parquet metadata (from follow-up read)"
            );

            inner.metrics.record_ranges_scheduled(to_prefetch.len(), total_bytes);
            inner.metrics.record_ranges_by_type(&summary);
            schedule_prefetch_tasks(inner, &path, &to_prefetch);
        }
        Err(e) => {
            tracing::warn!(%path, error = %e, "Parquet metadata decode failed on follow-up read");
            inner.metrics.record_footer_parse_failure();
            bounded_seen_insert(&inner.seen, path);
        }
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Returns `true` if the path and read size suggest this could be a
/// Parquet footer read worth inspecting.
fn should_check_for_footer(path: &str, read_size: usize) -> bool {
    path.ends_with(".parquet") && (MIN_FOOTER_SUFFIX_SIZE..=MAX_FOOTER_READ_SIZE).contains(&read_size)
}

/// Returns `true` if the last 4 bytes of `data` are the Parquet magic
/// bytes `PAR1`.
fn is_parquet_footer(data: &[u8]) -> bool {
    data.len() >= MIN_FOOTER_SUFFIX_SIZE && data[data.len() - 4..] == *b"PAR1"
}

/// Result of attempting to parse Parquet footer metadata from a buffer.
enum FooterParseResult {
    /// Buffer is too small to contain the full footer metadata.
    /// This is expected for initial suffix reads (e.g., 8-byte footer
    /// probe) and should not be logged as an error. Carries the
    /// `metadata_len` extracted from the suffix so the caller can
    /// recognize the follow-up metadata read.
    BufferTooSmall {
        /// Expected Thrift metadata length (from the 4-byte LE field
        /// before the PAR1 magic).
        metadata_len: usize,
    },
    /// Buffer contains the full footer but Thrift decoding failed.
    DecodeError(parquet::errors::ParquetError),
    /// Successfully decoded Parquet metadata.
    Ok(ParquetMetaData),
}

/// Extract and decode Parquet metadata from a buffer that ends with the
/// Parquet footer suffix (`[metadata][4-byte length LE][PAR1]`).
///
/// Returns [`FooterParseResult`] to distinguish between expected partial
/// reads and actual decode failures.
fn parse_footer_metadata(data: &[u8]) -> FooterParseResult {
    if data.len() < MIN_FOOTER_SUFFIX_SIZE {
        return FooterParseResult::BufferTooSmall { metadata_len: 0 };
    }

    // Extract the metadata length from bytes [len-8..len-4].
    let len = data.len();
    let metadata_len = u32::from_le_bytes([data[len - 8], data[len - 7], data[len - 6], data[len - 5]]) as usize;

    // Verify the buffer contains the full metadata.
    if metadata_len + 8 > len {
        return FooterParseResult::BufferTooSmall { metadata_len };
    }

    let metadata_bytes = &data[len - 8 - metadata_len..len - 8];
    match ParquetMetaDataReader::decode_metadata(metadata_bytes) {
        Ok(metadata) => FooterParseResult::Ok(metadata),
        Err(e) => FooterParseResult::DecodeError(e),
    }
}

/// Maximum gap (in bytes) between two ranges that will be merged into a
/// single read. Reduces S3 round-trips at the cost of reading small
/// gaps between metadata structures.
const MERGE_GAP: u64 = 1024;

/// Merge sorted, non-overlapping ranges that are within `MERGE_GAP` of
/// each other into larger contiguous reads. Reduces S3 round-trips at
/// the cost of reading small gaps between metadata structures.
fn merge_ranges(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_unstable_by_key(|&(offset, _)| offset);

    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    let (mut cur_off, mut cur_len) = ranges[0];

    for &(off, len) in &ranges[1..] {
        let cur_end = cur_off + cur_len;
        if off <= cur_end + MERGE_GAP {
            // Overlapping or close enough — extend.
            cur_len = (off + len - cur_off).max(cur_len);
        } else {
            merged.push((cur_off, cur_len));
            cur_off = off;
            cur_len = len;
        }
    }
    merged.push((cur_off, cur_len));
    merged
}

/// Filter out ranges fully contained within the triggering read's range.
///
/// When the metadata request itself covers a byte range, any prefetch
/// range that falls entirely within it is redundant — the inner layer
/// (cache) already has the data from the metadata read.
fn filter_covered_ranges(ranges: &[(u64, u64)], request_range: Option<(u64, u64)>) -> Vec<(u64, u64)> {
    let Some((req_off, req_len)) = request_range else {
        return ranges.to_vec();
    };
    let req_end = req_off + req_len;
    ranges
        .iter()
        .copied()
        .filter(|&(off, len)| {
            // Keep ranges that are NOT fully covered by the request.
            !(off >= req_off && off + len <= req_end)
        })
        .collect()
}

/// Collect metadata-only byte ranges from Parquet footer for prefetching.
///
/// Extracts ranges for dictionary pages, column indexes, offset indexes,
/// and bloom filters from each column chunk — these are the metadata
/// structures the query engine reads after the footer and before issuing
/// data page reads. Data pages are **never** included.
///
/// Returns `(offset, length)` pairs suitable for issuing range reads,
/// alongside a [`PrefetchRangeSummary`] counting ranges by type.
fn compute_prefetch_ranges(
    metadata: &ParquetMetaData,
    config: &PrefetchConfig,
) -> (Vec<(u64, u64)>, PrefetchRangeSummary) {
    let mut ranges = Vec::new();
    let mut summary = PrefetchRangeSummary::default();
    let mut total_bytes: usize = 0;

    for rg in metadata.row_groups() {
        for col in rg.columns() {
            // Column allowlist filter.
            if let Some(ref allowlist) = config.prefetch_columns {
                let col_name = col.column_descr().name();
                if !allowlist.iter().any(|name| name == col_name) {
                    continue;
                }
            }

            // Dictionary page: offset is optional, length = data_page_offset - dict_offset.
            if config.prefetch_dictionary_pages {
                if let Some(dict_off) = col.dictionary_page_offset() {
                    let data_off = col.data_page_offset();
                    if dict_off > 0 && dict_off < data_off {
                        #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
                        let len = (data_off - dict_off) as usize;
                        if total_bytes + len > config.max_prefetch_bytes {
                            return (ranges, summary);
                        }
                        #[allow(clippy::cast_sign_loss)]
                        {
                            ranges.push((dict_off as u64, len as u64));
                        }
                        total_bytes += len;
                        summary.dictionary_pages += 1;
                    }
                }
            }

            // Collect metadata ranges for this column chunk.
            // Each is (offset: i64, length: i32, kind) — offset and length are optional.
            let metadata_ranges = [
                (col.column_index_offset(), col.column_index_length(), "column_index"),
                (col.offset_index_offset(), col.offset_index_length(), "offset_index"),
                (col.bloom_filter_offset(), col.bloom_filter_length(), "bloom_filter"),
            ];

            for (offset, length, kind) in metadata_ranges {
                let (Some(off), Some(len)) = (offset, length) else {
                    continue;
                };
                if off <= 0 || len <= 0 {
                    continue;
                }

                #[allow(clippy::cast_sign_loss)]
                let len_usize = len as usize;

                // Budget check.
                if total_bytes + len_usize > config.max_prefetch_bytes {
                    return (ranges, summary);
                }

                #[allow(clippy::cast_sign_loss)]
                let off_u64 = off as u64;
                #[allow(clippy::cast_sign_loss)]
                let len_u64 = len as u64;

                ranges.push((off_u64, len_u64));
                total_bytes += len_usize;
                match kind {
                    "column_index" => summary.column_indexes += 1,
                    "offset_index" => summary.offset_indexes += 1,
                    "bloom_filter" => summary.bloom_filters += 1,
                    _ => {}
                }
            }
        }
    }

    (ranges, summary)
}

// ---------------------------------------------------------------------------
// Debug impls (required by LayeredAccess: Access: Debug)
// ---------------------------------------------------------------------------

#[allow(clippy::missing_fields_in_debug)]
impl<A: Access> std::fmt::Debug for PrefetchInner<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefetchInner")
            .field("config", &self.config)
            .field("seen_count", &self.seen.len())
            .field("in_flight_count", &self.tracker.pending.len())
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl<A: Access> std::fmt::Debug for PrefetchAccessor<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefetchAccessor").field("inner", &self.inner).finish()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use parquet::basic::Type as PhysicalType;
    use parquet::file::metadata::RowGroupMetaData;
    use parquet::schema::types::{SchemaDescriptor, Type};

    use super::*;

    /// Build a minimal [`SchemaDescriptor`] with the given column names.
    fn test_schema(columns: &[&str]) -> Arc<SchemaDescriptor> {
        let fields: Vec<Arc<Type>> = columns
            .iter()
            .map(|name| {
                Arc::new(
                    Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                        .build()
                        .expect("test type"),
                )
            })
            .collect();
        let root = Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .expect("test schema");
        Arc::new(SchemaDescriptor::new(Arc::new(root)))
    }

    /// Per-column metadata offsets for building test row groups.
    struct TestColumnMeta {
        data_page_offset: i64,
        compressed_size: i64,
        dictionary_page_offset: Option<i64>,
        column_index: Option<(i64, i32)>,
        offset_index: Option<(i64, i32)>,
        bloom_filter: Option<(i64, i32)>,
    }

    impl TestColumnMeta {
        /// Column with only data pages, no metadata indexes.
        const fn data_only(data_page_offset: i64, compressed_size: i64) -> Self {
            Self {
                data_page_offset,
                compressed_size,
                dictionary_page_offset: None,
                column_index: None,
                offset_index: None,
                bloom_filter: None,
            }
        }
    }

    /// Build a [`RowGroupMetaData`] with column chunks and optional metadata indexes.
    fn test_row_group(schema: &Arc<SchemaDescriptor>, chunks: &[TestColumnMeta]) -> RowGroupMetaData {
        use parquet::file::metadata::ColumnChunkMetaData;
        let mut columns = Vec::new();
        for (i, chunk) in chunks.iter().enumerate() {
            let col_desc = schema.column(i);
            let mut builder = ColumnChunkMetaData::builder(col_desc)
                .set_data_page_offset(chunk.data_page_offset)
                .set_total_compressed_size(chunk.compressed_size)
                .set_total_uncompressed_size(chunk.compressed_size)
                .set_num_values(100)
                .set_encodings(vec![parquet::basic::Encoding::PLAIN]);
            if let Some(dict_off) = chunk.dictionary_page_offset {
                builder = builder.set_dictionary_page_offset(Some(dict_off));
            }
            if let Some((off, len)) = chunk.column_index {
                builder = builder.set_column_index_offset(Some(off)).set_column_index_length(Some(len));
            }
            if let Some((off, len)) = chunk.offset_index {
                builder = builder.set_offset_index_offset(Some(off)).set_offset_index_length(Some(len));
            }
            if let Some((off, len)) = chunk.bloom_filter {
                builder = builder.set_bloom_filter_offset(Some(off)).set_bloom_filter_length(Some(len));
            }
            columns.push(builder.build().expect("test column chunk"));
        }
        RowGroupMetaData::builder(schema.clone())
            .set_column_metadata(columns)
            .set_num_rows(100)
            .set_total_byte_size(chunks.iter().map(|c| c.compressed_size).sum())
            .build()
            .expect("test row group")
    }

    fn test_metadata(schema: &Arc<SchemaDescriptor>, row_groups: Vec<RowGroupMetaData>) -> ParquetMetaData {
        ParquetMetaData::new(
            parquet::file::metadata::FileMetaData::new(
                1,              // version
                100,            // num_rows
                None,           // created_by
                None,           // key_value_metadata
                schema.clone(), // schema
                None,           // column_orders
            ),
            row_groups,
        )
    }

    #[test]
    fn test_should_check_for_footer() {
        // Correct extension + reasonable size
        assert!(should_check_for_footer("data/file.parquet", 4096));
        // Too small
        assert!(!should_check_for_footer("data/file.parquet", 4));
        // Too large
        assert!(!should_check_for_footer("data/file.parquet", MAX_FOOTER_READ_SIZE + 1));
        // Wrong extension
        assert!(!should_check_for_footer("data/file.json", 4096));
        // Exact boundary sizes
        assert!(should_check_for_footer("f.parquet", MIN_FOOTER_SUFFIX_SIZE));
        assert!(should_check_for_footer("f.parquet", MAX_FOOTER_READ_SIZE));
    }

    #[test]
    fn test_footer_magic_detection() {
        // Valid PAR1 suffix
        let mut data = vec![0u8; 100];
        data[96..100].copy_from_slice(b"PAR1");
        assert!(is_parquet_footer(&data));

        // Invalid magic
        let mut data = vec![0u8; 100];
        data[96..100].copy_from_slice(b"PAR2");
        assert!(!is_parquet_footer(&data));

        // Too small
        assert!(!is_parquet_footer(&[0u8; 4]));
    }

    #[test]
    fn test_compute_prefetch_ranges_collects_metadata_only() {
        let schema = test_schema(&["col_a", "col_b"]);
        let rg = test_row_group(
            &schema,
            &[
                TestColumnMeta {
                    data_page_offset: 1000,
                    compressed_size: 50_000,
                    dictionary_page_offset: None,
                    column_index: Some((100, 200)),
                    offset_index: Some((300, 150)),
                    bloom_filter: Some((500, 64)),
                },
                TestColumnMeta {
                    data_page_offset: 60_000,
                    compressed_size: 80_000,
                    dictionary_page_offset: None,
                    column_index: Some((450, 180)),
                    offset_index: None,
                    bloom_filter: None,
                },
            ],
        );
        let metadata = test_metadata(&schema, vec![rg]);
        let config = PrefetchConfig::default();
        let (ranges, summary) = compute_prefetch_ranges(&metadata, &config);

        // col_a: column_index(100,200) + offset_index(300,150) + bloom(500,64)
        // col_b: column_index(450,180) only (no offset_index, no bloom)
        // Data page offsets (1000, 60000) must NOT appear.
        assert_eq!(ranges.len(), 4);
        assert_eq!(ranges[0], (100, 200));
        assert_eq!(ranges[1], (300, 150));
        assert_eq!(ranges[2], (500, 64));
        assert_eq!(ranges[3], (450, 180));
        assert_eq!(summary.dictionary_pages, 0);
        assert_eq!(summary.column_indexes, 2);
        assert_eq!(summary.offset_indexes, 1);
        assert_eq!(summary.bloom_filters, 1);
    }

    #[test]
    fn test_compute_prefetch_ranges_no_metadata_indexes() {
        let schema = test_schema(&["col_a"]);
        let rg = test_row_group(&schema, &[TestColumnMeta::data_only(1000, 50_000)]);
        let metadata = test_metadata(&schema, vec![rg]);
        let config = PrefetchConfig::default();
        let (ranges, _summary) = compute_prefetch_ranges(&metadata, &config);

        // No column_index, offset_index, or bloom_filter → nothing to prefetch.
        assert!(ranges.is_empty());
    }

    #[test]
    fn test_compute_prefetch_ranges_with_allowlist() {
        let schema = test_schema(&["col_a", "col_b", "col_c"]);
        let rg = test_row_group(
            &schema,
            &[
                TestColumnMeta {
                    data_page_offset: 1000,
                    compressed_size: 5000,
                    dictionary_page_offset: None,
                    column_index: Some((100, 50)),
                    offset_index: Some((200, 40)),
                    bloom_filter: None,
                },
                TestColumnMeta::data_only(2000, 6000), // col_b: no metadata
                TestColumnMeta {
                    data_page_offset: 3000,
                    compressed_size: 7000,
                    dictionary_page_offset: None,
                    column_index: Some((300, 60)),
                    offset_index: None,
                    bloom_filter: None,
                },
            ],
        );
        let metadata = test_metadata(&schema, vec![rg]);

        let config = PrefetchConfig {
            prefetch_columns: Some(vec!["col_a".to_string(), "col_c".to_string()]),
            ..Default::default()
        };
        let (ranges, _summary) = compute_prefetch_ranges(&metadata, &config);

        // col_a: column_index(100,50) + offset_index(200,40)
        // col_b: filtered out by allowlist
        // col_c: column_index(300,60)
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (100, 50));
        assert_eq!(ranges[1], (200, 40));
        assert_eq!(ranges[2], (300, 60));
    }

    #[test]
    fn test_compute_prefetch_ranges_budget_cap() {
        let schema = test_schema(&["col_a"]);
        let rg = test_row_group(
            &schema,
            &[TestColumnMeta {
                data_page_offset: 1000,
                compressed_size: 50_000,
                dictionary_page_offset: None,
                column_index: Some((100, 200)),
                offset_index: Some((300, 150)),
                bloom_filter: Some((500, 64)),
            }],
        );
        let metadata = test_metadata(&schema, vec![rg]);

        let config = PrefetchConfig {
            max_prefetch_bytes: 300, // Room for column_index(200) but not offset_index(+150)
            ..Default::default()
        };
        let (ranges, _summary) = compute_prefetch_ranges(&metadata, &config);

        // column_index(200) fits, offset_index(200+150=350) exceeds budget → stop
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (100, 200));
    }

    #[test]
    fn test_compute_prefetch_ranges_includes_dictionary_pages() {
        let schema = test_schema(&["col_a"]);
        // Dictionary at offset 800, data page at 1000 → dict length = 200.
        let rg = test_row_group(
            &schema,
            &[TestColumnMeta {
                data_page_offset: 1000,
                compressed_size: 50_000,
                dictionary_page_offset: Some(800),
                column_index: Some((100, 50)),
                offset_index: None,
                bloom_filter: None,
            }],
        );
        let metadata = test_metadata(&schema, vec![rg]);
        let config = PrefetchConfig {
            prefetch_dictionary_pages: true,
            ..Default::default()
        };
        let (ranges, summary) = compute_prefetch_ranges(&metadata, &config);

        // Dictionary page (800, 200) + column_index (100, 50)
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0], (800, 200));
        assert_eq!(ranges[1], (100, 50));
        assert_eq!(summary.dictionary_pages, 1);
        assert_eq!(summary.column_indexes, 1);
    }

    #[test]
    fn test_compute_prefetch_ranges_dictionary_budget_cap() {
        let schema = test_schema(&["col_a"]);
        // Dictionary at offset 800, data page at 1000 → dict length = 200.
        let rg = test_row_group(
            &schema,
            &[TestColumnMeta {
                data_page_offset: 1000,
                compressed_size: 50_000,
                dictionary_page_offset: Some(800),
                column_index: Some((100, 50)),
                offset_index: None,
                bloom_filter: None,
            }],
        );
        let metadata = test_metadata(&schema, vec![rg]);

        // Budget only allows the dictionary page (200 bytes), not column_index (+50).
        let config = PrefetchConfig {
            max_prefetch_bytes: 200,
            prefetch_dictionary_pages: true,
            ..Default::default()
        };
        let (ranges, summary) = compute_prefetch_ranges(&metadata, &config);

        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (800, 200));
        assert_eq!(summary.dictionary_pages, 1);
        assert_eq!(summary.column_indexes, 0);
    }

    #[test]
    fn test_compute_prefetch_ranges_dictionary_invalid_offset() {
        let schema = test_schema(&["col_a", "col_b"]);
        let rg = test_row_group(
            &schema,
            &[
                // dict_off >= data_off → should be skipped
                TestColumnMeta {
                    data_page_offset: 1000,
                    compressed_size: 50_000,
                    dictionary_page_offset: Some(1000),
                    column_index: Some((100, 50)),
                    offset_index: None,
                    bloom_filter: None,
                },
                // dict_off == 0 → should be skipped
                TestColumnMeta {
                    data_page_offset: 2000,
                    compressed_size: 50_000,
                    dictionary_page_offset: Some(0),
                    column_index: None,
                    offset_index: None,
                    bloom_filter: None,
                },
            ],
        );
        let metadata = test_metadata(&schema, vec![rg]);
        let config = PrefetchConfig {
            prefetch_dictionary_pages: true,
            ..Default::default()
        };
        let (ranges, summary) = compute_prefetch_ranges(&metadata, &config);

        // Only col_a's column_index should be collected; dict pages are invalid.
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (100, 50));
        assert_eq!(summary.dictionary_pages, 0);
    }

    #[test]
    fn test_compute_prefetch_ranges_summary_counts() {
        let schema = test_schema(&["col_a", "col_b"]);
        let rg = test_row_group(
            &schema,
            &[
                TestColumnMeta {
                    data_page_offset: 1000,
                    compressed_size: 50_000,
                    dictionary_page_offset: Some(800),
                    column_index: Some((100, 50)),
                    offset_index: Some((200, 40)),
                    bloom_filter: Some((300, 30)),
                },
                TestColumnMeta {
                    data_page_offset: 5000,
                    compressed_size: 50_000,
                    dictionary_page_offset: Some(4500),
                    column_index: Some((400, 60)),
                    offset_index: None,
                    bloom_filter: None,
                },
            ],
        );
        let metadata = test_metadata(&schema, vec![rg]);
        let config = PrefetchConfig {
            prefetch_dictionary_pages: true,
            ..Default::default()
        };
        let (ranges, summary) = compute_prefetch_ranges(&metadata, &config);

        // col_a: dict(800,200) + ci(100,50) + oi(200,40) + bf(300,30)
        // col_b: dict(4500,500) + ci(400,60)
        assert_eq!(ranges.len(), 6);
        assert_eq!(summary.dictionary_pages, 2);
        assert_eq!(summary.column_indexes, 2);
        assert_eq!(summary.offset_indexes, 1);
        assert_eq!(summary.bloom_filters, 1);
    }

    #[test]
    fn test_compute_prefetch_ranges_dictionary_pages_disabled_by_default() {
        let schema = test_schema(&["col_a"]);
        // Dictionary at offset 800, data page at 1000 → dict length = 200.
        let rg = test_row_group(
            &schema,
            &[TestColumnMeta {
                data_page_offset: 1000,
                compressed_size: 50_000,
                dictionary_page_offset: Some(800),
                column_index: Some((100, 50)),
                offset_index: None,
                bloom_filter: None,
            }],
        );
        let metadata = test_metadata(&schema, vec![rg]);
        let config = PrefetchConfig::default();
        let (ranges, summary) = compute_prefetch_ranges(&metadata, &config);

        // Only column_index collected; dictionary pages skipped by default.
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0], (100, 50));
        assert_eq!(summary.dictionary_pages, 0);
        assert_eq!(summary.column_indexes, 1);
    }

    #[test]
    fn test_merge_ranges_adjacent() {
        let ranges = vec![(100, 50), (200, 60)];
        let merged = merge_ranges(ranges);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], (100, 160)); // 100..(200+60)
    }

    #[test]
    fn test_merge_ranges_distant() {
        // Two ranges separated by more than MERGE_GAP stay separate.
        #[allow(clippy::cast_possible_truncation)]
        let far = 100 + 50 + MERGE_GAP + 1;
        let ranges = vec![(100, 50), (far, 60)];
        let merged = merge_ranges(ranges);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn test_merge_ranges_overlapping() {
        let ranges = vec![(100, 200), (150, 100)];
        let merged = merge_ranges(ranges);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], (100, 200));
    }

    #[test]
    fn test_merge_ranges_empty() {
        let merged = merge_ranges(vec![]);
        assert!(merged.is_empty());
    }

    #[test]
    fn test_merge_ranges_unsorted() {
        let ranges = vec![(300, 50), (100, 50), (200, 50)];
        let merged = merge_ranges(ranges);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], (100, 250)); // 100..(300+50)
    }

    #[test]
    fn test_filter_covered_ranges_skips_contained() {
        let ranges = vec![(100, 50), (200, 60), (500, 80)];

        // Request covers [100, 300) — first two ranges are contained.
        let filtered = filter_covered_ranges(&ranges, Some((100, 200)));
        assert_eq!(filtered, vec![(500, 80)]);
    }

    #[test]
    fn test_filter_covered_ranges_keeps_partial_overlap() {
        let ranges = vec![(100, 50), (140, 60)];

        // Request covers [100, 160) — second range extends to 200, not fully covered.
        let filtered = filter_covered_ranges(&ranges, Some((100, 60)));
        assert_eq!(filtered, vec![(140, 60)]);
    }

    #[test]
    fn test_filter_covered_ranges_none_request() {
        let ranges = vec![(100, 50), (200, 60)];

        // No request range — all ranges kept.
        let filtered = filter_covered_ranges(&ranges, None);
        assert_eq!(filtered, ranges);
    }

    #[test]
    fn test_seen_dedup() {
        let seen: DashSet<Arc<str>> = DashSet::new();
        let path: Arc<str> = Arc::from("data/file.parquet");

        assert!(!seen.contains(&path));
        seen.insert(path.clone());
        assert!(seen.contains(&path));
    }

    #[test]
    fn test_inflight_wait() {
        let tracker = InFlightTracker::new();
        tracker.register(Arc::from("file.parquet"), 100, 500);

        // Overlapping range should find the in-flight entry.
        let found = tracker.find_overlap("file.parquet", 200, 100);
        assert!(found.is_some());

        // Non-overlapping range should not find it.
        let found = tracker.find_overlap("file.parquet", 700, 100);
        assert!(found.is_none());

        // Different path should not find it.
        let found = tracker.find_overlap("other.parquet", 100, 500);
        assert!(found.is_none());
    }

    #[test]
    fn test_inflight_notify_on_complete() {
        let tracker = InFlightTracker::new();
        tracker.register(Arc::from("file.parquet"), 100, 500);

        assert!(tracker.find_overlap("file.parquet", 100, 500).is_some());

        // After completion, the entry should be removed.
        tracker.complete("file.parquet", 100, 500);
        assert!(tracker.find_overlap("file.parquet", 100, 500).is_none());
    }

    #[tokio::test]
    async fn test_inflight_watch_on_complete() {
        let tracker = Arc::new(InFlightTracker::new());
        tracker.register(Arc::from("file.parquet"), 100, 500);

        // Simulate a waiter.
        let tracker_clone = tracker.clone();
        let waiter = tokio::spawn(async move {
            if let Some(mut rx) = tracker_clone.find_overlap("file.parquet", 100, 500) {
                rx.wait_for(|&done| done).await.is_ok()
            } else {
                false
            }
        });

        // Give the waiter time to start.
        tokio::task::yield_now().await;

        // Complete (simulating error path — complete is called regardless).
        tracker.complete("file.parquet", 100, 500);

        // Waiter should complete successfully.
        let result = waiter.await.expect("waiter task should not panic");
        assert!(result);
    }

    /// Regression test: if `complete()` fires *before* the waiter calls
    /// `find_overlap`, the entry is removed so `find_overlap` returns
    /// `None` — the fast path. No lost signal, no hang.
    #[tokio::test]
    async fn test_inflight_watch_no_race() {
        let tracker = InFlightTracker::new();
        tracker.register(Arc::from("file.parquet"), 100, 500);

        // Complete BEFORE anyone subscribes.
        tracker.complete("file.parquet", 100, 500);

        assert!(tracker.find_overlap("file.parquet", 100, 500).is_none());
    }

    /// If a waiter subscribes and then `complete()` is called from
    /// another task, the waiter must wake up — even if subscribe happened
    /// first (the original `Notify` race scenario).
    #[tokio::test]
    async fn test_inflight_watch_subscribe_then_complete() {
        let tracker = Arc::new(InFlightTracker::new());
        tracker.register(Arc::from("file.parquet"), 100, 500);

        // Subscribe first.
        let mut rx = tracker.find_overlap("file.parquet", 100, 500).unwrap();

        // Complete from another "task".
        tracker.complete("file.parquet", 100, 500);

        // Receiver should see done == true immediately.
        assert!(rx.wait_for(|&done| done).await.is_ok());
    }

    /// If the tracker entry is removed without sending `true` (simulating
    /// a leaked entry cleanup), receivers should get `RecvError` because
    /// the sole `Sender` is dropped.
    #[tokio::test]
    async fn test_inflight_watch_sender_dropped() {
        let tracker = InFlightTracker::new();
        tracker.register(Arc::from("file.parquet"), 100, 500);

        // Subscribe before removing.
        let mut rx = tracker.find_overlap("file.parquet", 100, 500).unwrap();

        // Simulate leaked entry cleanup: remove without sending `true`.
        tracker.pending.remove("file.parquet");

        // Sender was inside the entry — now dropped. `wait_for` should
        // return Err, not hang.
        assert!(rx.wait_for(|&done| done).await.is_err());
    }
}
