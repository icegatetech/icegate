//! Queue reader for reading Parquet segments from object storage.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use icegate_common::retrier::{Retrier, RetrierConfig};
use lru::LruCache;
use object_store::{ObjectStore, path::Path};
use parquet::{
    arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder},
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    Topic,
    error::{QueueError, Result},
    extract::{ExtractField, ExtractedValue, ResolvedField},
    segment::SegmentId,
};

const PLAN_ROW_GROUPS_TO_BLOCKING_THREAD: usize = 64;
const DEFAULT_PLAN_SEGMENT_READ_PARALLELISM: usize = 8;

/// A segment returned by the object store listing, carrying both its identity
/// and the file size reported by the listing (no extra HEAD request needed).
#[derive(Debug, Clone)]
pub struct ListedSegment {
    /// Parsed segment identity (topic + offset).
    pub id: SegmentId,
    /// File size in bytes, as reported by the object store listing.
    pub size: u64,
}

/// Resolved segment file with its object store path and size.
#[derive(Debug, Clone)]
pub struct SegmentFile {
    /// Full object store path (including `base_path` prefix).
    pub path: String,
    /// File size in bytes.
    pub size: u64,
}

/// Result of planning segments record batches for processing.
///
/// The plan is a flat list of row-group entries, ordered deterministically
/// by `(segment_idx, row_group_idx)`. Higher-level grouping (per-tenant
/// bucketing, sort-key clustering, bin-packing) is the responsibility of the
/// caller (see `icegate-ingest::shift::planner_algorithm`).
#[derive(Debug, Clone)]
pub struct SegmentsPlan {
    /// Row-group entries across all planned segments, deterministically
    /// ordered by `(segment_idx, row_group_idx)`.
    pub entries: Vec<RowGroupPlanEntry>,
    /// Last segment offset observed in the planned segments.
    pub last_segment_offset: Option<u64>,
    /// Number of segments scanned in the plan.
    pub segments_count: usize,
    /// Total number of row groups across all planned segments.
    pub row_groups_total: usize,
    /// Total planned input size (compressed bytes) across all entries.
    pub input_bytes_total: u64,
}

/// One row-group entry in a flat segments plan.
#[derive(Debug, Clone)]
pub struct RowGroupPlanEntry {
    /// WAL segment offset.
    pub wal_offset: u64,
    /// Row group index inside the segment.
    pub row_group_idx: usize,
    /// Compressed row-group size in bytes.
    pub row_group_bytes: u64,
    /// Caller-requested field extractions, keyed by [`ExtractField::name`].
    ///
    /// Only fields whose extractor produced a value for this row group are
    /// present (e.g. `FileKeyValueRowGroupPayload` is absent when the WAL
    /// footer carries no entry for that row group).
    pub extracted: HashMap<String, ExtractedValue>,
}

/// Buffered row-group entries for a single WAL segment, used while reading
/// segment metadata in parallel.
struct SegmentRowGroups {
    entries: Vec<RowGroupPlanEntry>,
}

/// Queue reader dependency surface for shift executors.
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Queue reader dependency surface for shift executors.
#[async_trait]
pub trait QueueReader: Send + Sync {
    /// Build a flat plan of row-group entries to process.
    ///
    /// `fields` describes which per-row-group facts should be extracted from
    /// the parquet footer; values land in [`RowGroupPlanEntry::extracted`]
    /// keyed by [`ExtractField::name`].
    ///
    /// The returned `SegmentsPlan` does not perform any grouping or
    /// bin-packing: callers are expected to implement their own
    /// scheduling/clustering on top of the flat list.
    async fn plan_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        fields: &[ExtractField],
        cancel_token: &CancellationToken,
    ) -> Result<SegmentsPlan>;

    /// Open a streaming reader for a specific segment.
    async fn read_segment(
        &self,
        topic: &Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        cancel_token: &CancellationToken,
    ) -> Result<RecordBatchStream>;
}

/// Queue reader for reading Parquet segments from object storage.
///
/// Provides methods to list segments and read record batches. Optionally
/// caches parsed Parquet metadata in a bounded LRU cache. WAL files are
/// immutable once written, so cached metadata never goes stale.
pub struct ParquetQueueReader {
    /// Base path for queue segments.
    base_path: String,

    /// Object store backend abstraction.
    store: Arc<dyn ObjectStore>,

    /// Maximum number of rows per emitted [`RecordBatch`] when reading a segment.
    record_batch_size_rows: usize,
    /// Maximum number of WAL segments to read in parallel while building the plan.
    plan_segment_read_parallelism: usize,

    retrier: Retrier,

    /// LRU cache for parsed Parquet metadata, keyed by segment file path.
    /// `None` when caching is disabled (capacity = 0).
    metadata_cache: Option<Arc<Mutex<LruCache<String, Arc<ParquetMetaData>>>>>,
}

impl ParquetQueueReader {
    /// Creates a new queue reader.
    ///
    /// # Arguments
    ///
    /// * `base_path` - Base path for queue segments in object storage
    /// * `store` - Object store backend
    /// * `record_batch_size_rows` - Maximum rows per emitted `RecordBatch`
    ///
    /// # Errors
    ///
    /// Returns an error if `record_batch_size_rows` is zero.
    pub fn new(
        base_path: impl Into<String>,
        store: Arc<dyn ObjectStore>,
        record_batch_size_rows: usize,
    ) -> Result<Self> {
        Self::with_metadata_entries_cache_capacity(base_path, store, record_batch_size_rows, 0)
    }

    /// Creates a new queue reader with a Parquet metadata cache.
    ///
    /// When `metadata_entries_cache_capacity` is non-zero, parsed
    /// `ParquetMetaData` from WAL files is cached in a bounded LRU cache.
    /// Since WAL files are immutable, cached entries never go stale.
    ///
    /// # Arguments
    ///
    /// * `base_path` - Base path for queue segments in object storage
    /// * `store` - Object store backend
    /// * `record_batch_size_rows` - Maximum rows per emitted `RecordBatch`
    /// * `metadata_entries_cache_capacity` - LRU cache capacity (0 to disable)
    ///
    /// # Errors
    ///
    /// Returns an error if `record_batch_size_rows` is zero.
    pub fn with_metadata_entries_cache_capacity(
        base_path: impl Into<String>,
        store: Arc<dyn ObjectStore>,
        record_batch_size_rows: usize,
        metadata_entries_cache_capacity: usize,
    ) -> Result<Self> {
        if record_batch_size_rows == 0 {
            return Err(QueueError::Config(
                "record_batch_size_rows must be greater than zero".to_string(),
            ));
        }
        let metadata_cache =
            NonZeroUsize::new(metadata_entries_cache_capacity).map(|cap| Arc::new(Mutex::new(LruCache::new(cap))));
        Ok(Self {
            base_path: base_path.into(),
            store,
            record_batch_size_rows,
            plan_segment_read_parallelism: DEFAULT_PLAN_SEGMENT_READ_PARALLELISM,
            retrier: Retrier::new(RetrierConfig::default()),
            metadata_cache,
        })
    }

    /// Sets the WAL segment read parallelism used by the plan stage.
    ///
    /// # Errors
    ///
    /// Returns an error if `parallelism` is zero.
    pub fn with_plan_segment_read_parallelism(mut self, parallelism: usize) -> Result<Self> {
        if parallelism == 0 {
            return Err(QueueError::Config(
                "plan_segment_read_parallelism must be greater than zero".to_string(),
            ));
        }
        self.plan_segment_read_parallelism = parallelism;
        Ok(self)
    }

    /// Lists segments for a topic starting from a given offset.
    ///
    /// Returns listed segments (with file sizes) sorted by offset.
    pub async fn list_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<ListedSegment>> {
        // TODO(high): if a lot of files have accumulated, we need to somehow batch them.
        // Build prefix path - handle empty base_path
        let prefix = if self.base_path.is_empty() {
            Path::from(topic.as_str())
        } else {
            Path::from(format!("{}/{}", self.base_path, topic))
        };

        // Build base prefix for stripping - empty means no prefix to strip
        let base_prefix = if self.base_path.is_empty() {
            String::new()
        } else {
            format!("{}/", self.base_path)
        };

        // list_with_offset returns objects where path > offset_path.
        // To include start_offset, we need a path that comes before it.
        // For offset 0, use the prefix; otherwise use (start_offset - 1)'s path.
        let offset_path = if start_offset == 0 {
            // All the .parquet under the prefix will be returned. This is the expected behavior for “zero offset".
            prefix.clone()
        } else {
            let segment_path = SegmentId::new(topic, start_offset - 1).to_relative_path();
            if self.base_path.is_empty() {
                segment_path
            } else {
                Path::from(format!("{}/{}", self.base_path, segment_path))
            }
        };

        debug!(
            "list_segments_from: base_path={:?}, topic={}, start_offset={}, prefix={:?}, offset_path={:?}",
            self.base_path, topic, start_offset, prefix, offset_path
        );

        let store = Arc::clone(&self.store);
        let items: Vec<_> = self
            .retry(cancel_token, move || {
                let store = Arc::clone(&store);
                let prefix = prefix.clone();
                let offset_path = offset_path.clone();
                async move { Ok(store.list_with_offset(Some(&prefix), &offset_path).try_collect().await?) }
            })
            .await?;

        debug!("list_segments_from: found {} items", items.len());

        let mut segments: Vec<ListedSegment> = items
            .into_iter()
            .filter_map(|meta| {
                let path_str = meta.location.as_ref();
                debug!("list_segments_from: checking path={}", path_str);
                // Only include .parquet files
                if path_str.ends_with(".parquet") {
                    // Strip base_path from the full path before parsing
                    let relative_path = if base_prefix.is_empty() {
                        path_str
                    } else if let Some(p) = path_str.strip_prefix(&base_prefix) {
                        p
                    } else {
                        debug!(
                            "list_segments_from: failed to strip prefix {:?} from {}",
                            base_prefix, path_str
                        );
                        return None;
                    };
                    let relative_path_obj = Path::from(relative_path);
                    match SegmentId::from_relative_path(&relative_path_obj) {
                        Ok(id) => {
                            debug!("list_segments_from: parsed segment {:?}", id);
                            Some(ListedSegment { id, size: meta.size })
                        }
                        Err(e) => {
                            debug!(
                                "list_segments_from: failed to parse segment from {:?}: {}",
                                relative_path_obj, e
                            );
                            None
                        }
                    }
                } else {
                    None
                }
            })
            .collect();

        // Sort by offset
        segments.sort_by_key(|s| s.id.offset);

        Ok(segments)
    }

    /// Lists segment files for a topic with their sizes.
    ///
    /// File sizes are obtained directly from the object store listing,
    /// avoiding extra HEAD requests.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment listing fails.
    pub async fn list_segment_files(
        &self,
        topic: &Topic,
        start_offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<SegmentFile>> {
        let segments = self.list_segments(topic, start_offset, cancel_token).await?;
        let files = segments
            .into_iter()
            .map(|seg| {
                let path = self.segment_path(&seg.id);
                SegmentFile {
                    path: path.to_string(),
                    size: seg.size,
                }
            })
            .collect();
        Ok(files)
    }

    /// Lists segments and returns a flat plan of row-group entries.
    ///
    /// Entries are ordered deterministically by `(segment_idx, row_group_idx)`
    /// where `segment_idx` matches the listing order (sorted by WAL offset).
    pub async fn plan_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        fields: &[ExtractField],
        cancel_token: &CancellationToken,
    ) -> Result<SegmentsPlan> {
        let mut seen_names = HashSet::with_capacity(fields.len());
        for field in fields {
            if !seen_names.insert(field.name.as_str()) {
                return Err(QueueError::Config(format!(
                    "duplicate ExtractField name: '{}'",
                    field.name
                )));
            }
        }

        let listed = self.list_segments(topic, start_offset, cancel_token).await?;
        if listed.is_empty() {
            return Ok(SegmentsPlan {
                entries: Vec::new(),
                last_segment_offset: None,
                segments_count: 0,
                row_groups_total: 0,
                input_bytes_total: 0,
            });
        }

        let last_offset = listed.last().map(|s| s.id.offset);
        let segments: Vec<SegmentId> = listed.into_iter().map(|s| s.id).collect();
        let (entries, row_groups_total, input_bytes_total) =
            self.plan_record_batches(segments.as_slice(), fields, cancel_token).await?;

        Ok(SegmentsPlan {
            entries,
            last_segment_offset: last_offset,
            segments_count: segments.len(),
            row_groups_total,
            input_bytes_total,
        })
    }

    /// Opens a streaming reader for specific record batches (by index) from a segment by topic and offset.
    pub async fn read_segment(
        &self,
        topic: &Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        cancel_token: &CancellationToken,
    ) -> Result<RecordBatchStream> {
        if record_batch_idxs.is_empty() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let segment_id = SegmentId::new(topic, offset);
        let path = self.segment_path(&segment_id);
        let store = Arc::clone(&self.store);
        let path_for_head = path.clone();
        let object_meta = self
            .retry(cancel_token, move || {
                let store = Arc::clone(&store);
                let path = path_for_head.clone();
                async move { Ok(store.head(&path).await?) }
            })
            .await?;

        let reader = ParquetObjectReader::new(Arc::clone(&self.store), path).with_file_size(object_meta.size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
        let stream = builder
            .with_batch_size(self.record_batch_size_rows)
            .with_row_groups(record_batch_idxs.to_vec())
            .build()?;

        Ok(Box::pin(stream.map_err(QueueError::from)))
    }

    /// Read row-group metadata for each segment in parallel and assemble a
    /// flat list of `RowGroupPlanEntry` ordered by `(segment_idx, row_group_idx)`.
    async fn plan_record_batches(
        &self,
        segments: &[SegmentId],
        fields: &[ExtractField],
        cancel_token: &CancellationToken,
    ) -> Result<(Vec<RowGroupPlanEntry>, usize, u64)> {
        // Clone once per call so async closures can move into spawn_blocking
        // without sharing the caller-owned slice.
        let fields: Arc<[ExtractField]> = Arc::from(fields.to_vec());
        let mut inline_segments = 0usize;
        let mut blocking_segments = 0usize;
        let mut stream = futures::stream::iter(segments.iter().cloned().enumerate().map(|(segment_idx, segment)| {
            let segment_topic = segment.topic;
            let wal_offset = segment.offset;
            let fields = Arc::clone(&fields);
            let cancel_token = cancel_token.clone();
            async move {
                let parquet_meta = self.read_parquet_metadata(&segment_topic, wal_offset, &cancel_token).await?;
                let row_groups = parquet_meta.row_groups().len();
                let uses_blocking = row_groups >= PLAN_ROW_GROUPS_TO_BLOCKING_THREAD;
                let segment_entries = if uses_blocking {
                    let span = tracing::Span::current();
                    tokio::task::spawn_blocking(move || {
                        span.in_scope(|| Self::collect_segment_plan_entries(&parquet_meta, wal_offset, &fields))
                    })
                    .await??
                } else {
                    Self::collect_segment_plan_entries(&parquet_meta, wal_offset, &fields)?
                };
                Ok::<(usize, bool, SegmentRowGroups), QueueError>((segment_idx, uses_blocking, segment_entries))
            }
        }))
        .buffer_unordered(self.plan_segment_read_parallelism);

        // Reassemble entries in deterministic `(segment_idx, row_group_idx)` order.
        let mut next_expected_idx = 0usize;
        let mut pending_segment_plans: BTreeMap<usize, (bool, SegmentRowGroups)> = BTreeMap::new();
        let mut entries: Vec<RowGroupPlanEntry> = Vec::new();
        let mut row_groups_total = 0usize;
        let mut input_bytes_total = 0u64;
        while let Some(result) = stream.next().await {
            let (segment_idx, uses_blocking, segment_entries) = result?;
            pending_segment_plans.insert(segment_idx, (uses_blocking, segment_entries));
            while let Some((uses_blocking, segment_entries)) = pending_segment_plans.remove(&next_expected_idx) {
                if uses_blocking {
                    blocking_segments += 1;
                } else {
                    inline_segments += 1;
                }
                let SegmentRowGroups {
                    entries: segment_entries,
                } = segment_entries;
                for entry in segment_entries {
                    row_groups_total = row_groups_total
                        .checked_add(1)
                        .ok_or_else(|| QueueError::Metadata("row group total overflow".to_string()))?;
                    input_bytes_total = input_bytes_total
                        .checked_add(entry.row_group_bytes)
                        .ok_or_else(|| QueueError::Metadata("input bytes total overflow".to_string()))?;
                    entries.push(entry);
                }
                next_expected_idx = next_expected_idx.checked_add(1).ok_or_else(|| {
                    QueueError::Metadata("segment index overflow while assembling plan entries".to_string())
                })?;
            }
        }

        debug!(
            "plan_record_batches: segments={} inline={} blocking={}",
            segments.len(),
            inline_segments,
            blocking_segments
        );

        Ok((entries, row_groups_total, input_bytes_total))
    }

    #[tracing::instrument(level = "debug", skip(parquet_meta, fields), fields(wal_offset, n_fields = fields.len()))]
    fn collect_segment_plan_entries(
        parquet_meta: &ParquetMetaData,
        wal_offset: u64,
        fields: &[ExtractField],
    ) -> Result<SegmentRowGroups> {
        // The temp buffer (`SegmentRowGroups`) is returned because the function
        // runs inside `spawn_blocking` for large segments.

        // Pre-resolve per-field state once per segment (column indices, parsed
        // KV-metadata payloads), so the per-row-group loop is O(R*F) without
        // repeated parquet schema scans or JSON parses.
        let resolved_fields: Vec<ResolvedField<'_>> = fields
            .iter()
            .map(|field| ResolvedField::resolve(field, parquet_meta, wal_offset))
            .collect::<Result<_>>()?;

        let mut entries = Vec::with_capacity(parquet_meta.row_groups().len());
        for (row_group_idx, row_group) in parquet_meta.row_groups().iter().enumerate() {
            let row_group_bytes = Self::row_group_compressed_bytes(row_group, row_group_idx)?;
            let mut extracted = HashMap::with_capacity(resolved_fields.len());
            for resolved in &resolved_fields {
                if let Some(value) = resolved.extract(row_group, row_group_idx, wal_offset)? {
                    extracted.insert(resolved.name().to_string(), value);
                }
            }
            entries.push(RowGroupPlanEntry {
                wal_offset,
                row_group_idx,
                row_group_bytes,
                extracted,
            });
        }

        if entries.is_empty() {
            return Err(QueueError::Metadata(format!(
                "no row groups found in WAL segment {wal_offset}"
            )));
        }

        Ok(SegmentRowGroups { entries })
    }

    fn row_group_compressed_bytes(
        row_group: &parquet::file::metadata::RowGroupMetaData,
        row_group_idx: usize,
    ) -> Result<u64> {
        let mut total_bytes: u64 = 0;
        for column in row_group.columns() {
            let compressed_size = column.compressed_size();
            let compressed_size = u64::try_from(compressed_size).map_err(|_| {
                QueueError::Metadata(format!(
                    "negative compressed size in row group {row_group_idx}: {compressed_size}"
                ))
            })?;
            total_bytes = total_bytes
                .checked_add(compressed_size)
                .ok_or_else(|| QueueError::Metadata("row group compressed bytes overflow".to_string()))?;
        }
        Ok(total_bytes)
    }

    /// Reads Parquet footer metadata, using the LRU cache when available.
    ///
    /// WAL files are immutable, so cached metadata never goes stale.
    async fn read_parquet_metadata(
        &self,
        topic: &Topic,
        offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<Arc<ParquetMetaData>> {
        let segment_id = SegmentId::new(topic, offset);
        let cache_key = self.segment_path(&segment_id).to_string();

        // Check cache first
        if let Some(cache) = &self.metadata_cache {
            let mut guard = cache.lock().await;
            if let Some(cached) = guard.get(&cache_key) {
                return Ok(Arc::clone(cached));
            }
        }

        // Cache miss: read from S3
        let metadata = Arc::new(self.read_parquet_metadata_from_store(&cache_key, cancel_token).await?);

        // Insert into cache
        if let Some(cache) = &self.metadata_cache {
            let mut guard = cache.lock().await;
            guard.put(cache_key, Arc::clone(&metadata));
        }

        Ok(metadata)
    }

    /// Reads Parquet footer metadata from object storage using range requests.
    async fn read_parquet_metadata_from_store(
        &self,
        path_str: &str,
        cancel_token: &CancellationToken,
    ) -> Result<ParquetMetaData> {
        let path = Path::from(path_str);
        let store = Arc::clone(&self.store);
        let path_for_head = path.clone();
        let object_meta = self
            .retry(cancel_token, move || {
                let store = Arc::clone(&store);
                let path = path_for_head.clone();
                async move { Ok(store.head(&path).await?) }
            })
            .await?;
        let file_size = object_meta.size;

        if file_size < 8 {
            return Err(QueueError::Parquet(ParquetError::EOF(
                "file size is smaller than parquet footer".to_string(),
            )));
        }

        let file_size_usize = usize::try_from(file_size)
            .map_err(|_| QueueError::Metadata(format!("file size {file_size} exceeds addressable size")))?;
        let mut tail_len = std::cmp::min(file_size_usize, 64 * 1024);

        // TODO(high): need to use retrier
        // The footer size is taken from the end of the parquet file: the last 8 bytes are footer_len (4 bytes LE) + "PAR1".
        // Therefore, the code first reads the tail to 64 KB (or less if the file is small) in the hope that it will be enough right away.
        // If there is not enough, try_parse_sized returns NeedMoreData(need), and the code reads the larger tail.
        loop {
            let start = file_size - tail_len as u64;
            let store = Arc::clone(&self.store);
            let path = path.clone();
            let data = self
                .retry(cancel_token, move || {
                    let store = Arc::clone(&store);
                    let path = path.clone();
                    async move { Ok(store.get_range(&path, start..file_size).await?) }
                })
                .await?;
            let bytes = data;
            let mut reader = ParquetMetaDataReader::new();

            match reader.try_parse_sized(&bytes, file_size) {
                Ok(()) => return reader.finish().map_err(QueueError::Parquet),
                Err(ParquetError::NeedMoreData(need)) => {
                    if need as u64 > file_size {
                        return Err(QueueError::Parquet(ParquetError::NeedMoreData(need)));
                    }
                    if need <= tail_len {
                        return Err(QueueError::Parquet(ParquetError::NeedMoreData(need)));
                    }
                    tail_len = need;
                }
                Err(e) => return Err(QueueError::Parquet(e)),
            }
        }
    }

    /// Builds the full path for a segment, handling empty `base_path`.
    fn segment_path(&self, segment_id: &SegmentId) -> Path {
        let relative = format!("{}/{:0>20}.parquet", segment_id.topic, segment_id.offset);
        if self.base_path.is_empty() {
            Path::from(relative)
        } else {
            Path::from(format!("{}/{}", self.base_path, relative))
        }
    }

    async fn retry<T, Fut, F>(&self, cancel_token: &CancellationToken, mut op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let result = self
            .retrier
            .retry::<_, _, Result<T>, QueueError>(
                || {
                    // TODO(high): add metric
                    let fut = op();
                    async move {
                        match fut.await {
                            Ok(value) => Ok((false, Ok(value))),
                            Err(err) => Ok((err.is_retryable(), Err(err))),
                        }
                    }
                },
                cancel_token,
            )
            .await?;

        match result {
            Ok(value) => Ok(value),
            Err(err) => Err(err),
        }
    }
}

#[async_trait]
impl QueueReader for ParquetQueueReader {
    async fn plan_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        fields: &[ExtractField],
        cancel_token: &CancellationToken,
    ) -> Result<SegmentsPlan> {
        Self::plan_segments(self, topic, start_offset, fields, cancel_token).await
    }

    async fn read_segment(
        &self,
        topic: &Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        cancel_token: &CancellationToken,
    ) -> Result<RecordBatchStream> {
        Self::read_segment(self, topic, offset, record_batch_idxs, cancel_token).await
    }
}
