use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
};

use arrow::{
    array::{Array, StringArray, TimestampMicrosecondArray},
    compute::interleave_record_batch,
    datatypes::{DataType, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt};
use icegate_common::{RowGroupBoundaryRange, compare_option_ord};
use icegate_queue::{QueueReader, RecordBatchStream, Topic};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    error::{IngestError, Result},
    shift::executor::PlannedRowGroup,
};

// TODO(crit): from scheme
const LOGS_MERGE_SORT_COLUMNS: [&str; 3] = ["cloud_account_id", "service_name", "timestamp"];

enum CachedSortColumn {
    Utf8 {
        values: StringArray,
        descending: bool,
        nulls_first: bool,
    },
    TimestampMicrosecond {
        values: TimestampMicrosecondArray,
        descending: bool,
        nulls_first: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MergeCursor {
    stream_idx: usize,
    batch_generation: u64,
    row_idx: usize,
    segment_row_idx: u64,
}

/// /// Cluster is a set of Row Groups that are involved in sorting together at a given time
struct ActiveClusterState {
    source: PlannedRowGroupRead,
    stream: Option<RecordBatchStream>,
    current_batch: Option<RecordBatch>,
    current_batch_start_row: u64,
    batch_generation: u64,
    cache: Option<SortColumnCache>,
    rows_consumed_total: u64,
}

struct MergeHeap {
    cursors: Vec<MergeCursor>,
}

impl MergeHeap {
    fn new() -> Self {
        Self { cursors: Vec::new() }
    }

    fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }

    fn push(&mut self, cursor: MergeCursor, comparator: &RowComparator<'_>) -> Result<()> {
        self.cursors.push(cursor);
        let mut idx = self.cursors.len() - 1;
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if comparator.compare(&self.cursors[idx], &self.cursors[parent])? != Ordering::Less {
                break;
            }
            self.cursors.swap(idx, parent);
            idx = parent;
        }
        Ok(())
    }

    fn pop(&mut self, comparator: &RowComparator<'_>) -> Result<Option<MergeCursor>> {
        let Some(last) = self.cursors.pop() else {
            return Ok(None);
        };
        if self.cursors.is_empty() {
            return Ok(Some(last));
        }

        let result = std::mem::replace(&mut self.cursors[0], last);
        let mut idx = 0usize;
        loop {
            let left = idx * 2 + 1;
            let right = left + 1;
            if left >= self.cursors.len() {
                break;
            }

            let mut smallest = left;
            if right < self.cursors.len()
                && comparator.compare(&self.cursors[right], &self.cursors[left])? == Ordering::Less
            {
                smallest = right;
            }

            if comparator.compare(&self.cursors[smallest], &self.cursors[idx])? != Ordering::Less {
                break;
            }
            self.cursors.swap(idx, smallest);
            idx = smallest;
        }

        Ok(Some(result))
    }
}

struct RowComparator<'a> {
    streams: &'a [ActiveClusterState],
}

impl<'a> RowComparator<'a> {
    fn new(streams: &'a [ActiveClusterState]) -> Self {
        Self { streams }
    }

    fn compare(&self, left: &MergeCursor, right: &MergeCursor) -> Result<Ordering> {
        let left_cache = self
            .streams
            .get(left.stream_idx)
            .and_then(|state| state.cache.as_ref())
            .ok_or_else(|| IngestError::Shift("left merge cursor cache is missing".to_string()))?;
        let right_cache = self
            .streams
            .get(right.stream_idx)
            .and_then(|state| state.cache.as_ref())
            .ok_or_else(|| IngestError::Shift("right merge cursor cache is missing".to_string()))?;

        let ordering = left_cache.compare_row(left.row_idx, right_cache, right.row_idx)?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }

        let ordering = left.stream_idx.cmp(&right.stream_idx);
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }

        Ok(left.segment_row_idx.cmp(&right.segment_row_idx))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedRowGroupRead {
    pub segment_offset: u64,
    pub row_group_idx: usize,
    pub boundary_range: RowGroupBoundaryRange,
    pub plan_idx: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRowGroupEntry {
    source: PlannedRowGroupRead,
}

impl PendingRowGroupEntry {
    fn new(source: PlannedRowGroupRead) -> Self {
        Self { source }
    }
}

impl Ord for PendingRowGroupEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source
            .boundary_range
            .min_key
            .compare(&other.source.boundary_range.min_key)
            .then_with(|| self.source.plan_idx.cmp(&other.source.plan_idx))
    }
}

impl PartialOrd for PendingRowGroupEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Cluster is a set of Row Groups that are involved in sorting together at a given time
#[derive(Debug, Clone, Copy, Default)]
struct ActiveClusterStats {
    row_groups: usize,
    segments: usize,
    frontier_extensions: usize,
}

/// Immutable parameters that define how the merger reads and emits data.
#[derive(Debug, Clone)]
pub struct SortedBatchMergerConfig {
    /// Maximum number of rows in a merged output batch.
    pub row_group_size: usize,
    /// Limit for opening row groups in parallel while building a cluster.
    pub read_parallelism: usize,
    /// Topic from which row groups are read.
    pub topic: Topic,
    /// Shared cancellation token for all queue reads.
    pub cancel_token: CancellationToken,
}

/// Streams sorted row groups from WAL and merges them into one sorted output stream.
///
/// The merger only keeps one overlap cluster in memory at a time: it opens the
/// row groups whose key ranges intersect, performs k-way merge across them, and
/// then moves to the next cluster.
pub struct SortedBatchMerger<Q: QueueReader + ?Sized> {
    /// Immutable read/output settings.
    config: SortedBatchMergerConfig,
    /// Schema shared by all opened batches. Filled from the first batch.
    schema: SchemaRef,
    /// Queue reader used to open WAL row groups as Arrow streams.
    queue_reader: Arc<Q>,
    /// Row groups that are not opened yet, ordered by their lower boundary.
    pending_row_groups: BTreeSet<PendingRowGroupEntry>,
    /// Streams that belong to the current overlap cluster.
    active_cluster_streams: Vec<ActiveClusterState>,
    /// Min-heap of next candidate rows from active streams.
    heap: MergeHeap,
    /// Debug stats for the cluster currently being merged.
    active_cluster: Option<ActiveClusterStats>,
    /// Prefetched merged batches, used to fail fast before storage starts writing.
    prefetched_batches: VecDeque<RecordBatch>,
}

impl<Q> SortedBatchMerger<Q>
where
    Q: QueueReader + 'static + ?Sized,
{
    pub async fn try_new(
        queue_reader: Arc<Q>,
        read_plan: Vec<PlannedRowGroupRead>,
        config: SortedBatchMergerConfig,
    ) -> Result<Self> {
        if config.row_group_size == 0 {
            return Err(IngestError::Config(
                "row_group_size must be greater than zero".to_string(),
            ));
        }
        if config.read_parallelism == 0 {
            return Err(IngestError::Config(
                "shift_segment_read_parallelism must be greater than zero".to_string(),
            ));
        }

        Ok(Self {
            config,
            schema: SchemaRef::new(arrow::datatypes::Schema::empty()),
            queue_reader,
            pending_row_groups: read_plan.into_iter().map(PendingRowGroupEntry::new).collect(),
            active_cluster_streams: Vec::new(),
            heap: MergeHeap::new(),
            active_cluster: None,
            prefetched_batches: VecDeque::new(),
        })
    }

    /// Preload the first merged output batch so QueueRead failures happen
    /// before the storage write pipeline is started.
    pub async fn prefetch_first_batch(&mut self) -> Result<()> {
        if self.prefetched_batches.is_empty() {
            if let Some(batch) = self.next_batch_inner().await? {
                self.prefetched_batches.push_back(batch);
            }
        }
        Ok(())
    }

    pub fn into_stream(self) -> super::iceberg_storage::BoxRecordBatchStream {
        futures::stream::try_unfold(self, |mut merger| async move {
            match merger.next_batch().await? {
                Some(batch) => Ok(Some((batch, merger))),
                None => Ok(None),
            }
        })
        .boxed()
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(batch) = self.prefetched_batches.pop_front() {
            return Ok(Some(batch));
        }
        self.next_batch_inner().await
    }

    async fn next_batch_inner(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            // Open the next group of overlapping ranges only when the current cluster is exhausted.
            if self.heap.is_empty() {
                if !self.open_next_overlap_cluster().await? {
                    return Ok(None);
                }
            }

            let mut source_batches = Vec::new();
            let mut source_batch_idxs = HashMap::new();
            let mut interleave_indices = Vec::with_capacity(self.config.row_group_size);

            // Repeatedly take the smallest next row across active streams.
            while interleave_indices.len() < self.config.row_group_size {
                let Some(cursor) = ({
                    let comparator = RowComparator::new(&self.active_cluster_streams);
                    self.heap.pop(&comparator)?
                }) else {
                    break;
                };
                let stream_state = self
                    .active_cluster_streams
                    .get(cursor.stream_idx)
                    .ok_or_else(|| IngestError::Shift("merge cursor stream index out of bounds".to_string()))?;
                let current_batch = stream_state
                    .current_batch
                    .as_ref()
                    .ok_or_else(|| IngestError::Shift("merge cursor points to missing input batch".to_string()))?;
                if stream_state.batch_generation != cursor.batch_generation {
                    return Err(IngestError::Shift("stale merge cursor generation".to_string()));
                }

                let source_idx =
                    if let Some(source_idx) = source_batch_idxs.get(&(cursor.stream_idx, cursor.batch_generation)) {
                        *source_idx
                    } else {
                        let source_idx = source_batches.len();
                        source_batches.push(current_batch.clone());
                        source_batch_idxs.insert((cursor.stream_idx, cursor.batch_generation), source_idx);
                        source_idx
                    };
                interleave_indices.push((source_idx, cursor.row_idx));

                let next_cursor = self.advance_cursor(cursor).await?;
                if let Some(next_cursor) = next_cursor {
                    let comparator = RowComparator::new(&self.active_cluster_streams);
                    self.heap.push(next_cursor, &comparator)?;
                }
            }

            if interleave_indices.is_empty() {
                self.complete_active_cluster();
                continue;
            }

            let source_refs = source_batches.iter().collect::<Vec<_>>();
            let merged = interleave_record_batch(&source_refs, &interleave_indices)?;
            if self.heap.is_empty() {
                self.complete_active_cluster();
            }
            return Ok(Some(merged));
        }
    }

    async fn open_next_overlap_cluster(&mut self) -> Result<bool> {
        while !self.pending_row_groups.is_empty() {
            // Row groups with intersecting boundary ranges must be merged together.
            let (cluster_sources, cluster_stats) = self.pop_next_cluster_sources()?;
            debug!(
                cluster_row_groups = cluster_stats.row_groups,
                cluster_segments = cluster_stats.segments,
                cluster_frontier_extensions = cluster_stats.frontier_extensions,
                "shift merger cluster started"
            );

            let open_parallelism = cluster_sources.len().min(self.config.read_parallelism);
            let queue_reader = Arc::clone(&self.queue_reader);
            let topic = self.config.topic.clone();
            let cancel_token = self.config.cancel_token.clone();
            let mut opened = futures::stream::iter(cluster_sources.into_iter().enumerate().map(|(idx, source)| {
                let queue_reader = Arc::clone(&queue_reader);
                let topic = topic.clone();
                let cancel_token = cancel_token.clone();
                async move {
                    let opened = open_source(queue_reader, topic, source, cancel_token).await?;
                    Ok::<_, IngestError>((idx, opened))
                }
            }))
            .buffer_unordered(open_parallelism)
            .try_collect::<Vec<_>>()
            .await?;
            opened.sort_by_key(|(idx, _)| *idx);
            for (_idx, opened) in opened {
                if let Some(opened) = opened {
                    self.push_opened_source(opened)?;
                }
            }

            self.active_cluster = Some(cluster_stats);
            if !self.heap.is_empty() {
                return Ok(true);
            }
            self.complete_active_cluster();
        }
        Ok(false)
    }

    fn pop_next_cluster_sources(&mut self) -> Result<(Vec<PlannedRowGroupRead>, ActiveClusterStats)> {
        let first = self.pop_pending_source()?.ok_or_else(|| {
            IngestError::Shift("pending row groups disappeared while building overlap cluster".to_string())
        })?;

        // Extend the cluster while the next row group's lower bound still overlaps the frontier.
        let mut cluster_sources = Vec::new();
        let mut frontier = first.boundary_range.max_key.clone();
        let mut frontier_extensions = 0usize;
        cluster_sources.push(first);

        while let Some(next_pending) = self.peek_pending_source().cloned() {
            if next_pending.boundary_range.min_key.compare(&frontier) == Ordering::Greater {
                break;
            }
            let next = self.pop_pending_source()?.ok_or_else(|| {
                IngestError::Shift("pending row groups disappeared while extending overlap cluster".to_string())
            })?;
            if next.boundary_range.max_key.compare(&frontier) == Ordering::Greater {
                frontier = next.boundary_range.max_key.clone();
                frontier_extensions = frontier_extensions
                    .checked_add(1)
                    .ok_or_else(|| IngestError::Shift("cluster frontier extensions overflow".to_string()))?;
            }
            cluster_sources.push(next);
        }

        let cluster_segments = cluster_sources
            .iter()
            .map(|source| source.segment_offset)
            .collect::<HashSet<_>>()
            .len();
        let stats = ActiveClusterStats {
            row_groups: cluster_sources.len(),
            segments: cluster_segments,
            frontier_extensions,
        };

        Ok((cluster_sources, stats))
    }

    fn complete_active_cluster(&mut self) {
        if let Some(stats) = self.active_cluster.take() {
            debug!(
                cluster_row_groups = stats.row_groups,
                cluster_segments = stats.segments,
                cluster_frontier_extensions = stats.frontier_extensions,
                "shift merger cluster completed"
            );
        }
        self.active_cluster_streams.clear();
        self.heap = MergeHeap::new();
    }

    fn peek_pending_source(&self) -> Option<&PlannedRowGroupRead> {
        self.pending_row_groups.iter().next().map(|entry| &entry.source)
    }

    fn pop_pending_source(&mut self) -> Result<Option<PlannedRowGroupRead>> {
        let Some(entry) = self.pending_row_groups.iter().next().cloned() else {
            return Ok(None);
        };
        let removed = self.pending_row_groups.take(&entry).ok_or_else(|| {
            IngestError::Shift("pending row groups disappeared while selecting boundary minimum".to_string())
        })?;
        Ok(Some(removed.source))
    }

    fn push_opened_source(&mut self, opened: OpenedSource) -> Result<()> {
        if self.schema.fields().is_empty() {
            self.schema = opened.batch.schema();
        } else if opened.batch.schema() != self.schema {
            return Err(IngestError::Shift(format!(
                "schema mismatch in WAL segment {} row group {}",
                opened.source.segment_offset, opened.source.row_group_idx
            )));
        }

        let cache = SortColumnCache::try_new(&opened.batch)?;
        let stream_idx = self.active_cluster_streams.len();
        self.active_cluster_streams.push(ActiveClusterState {
            source: opened.source,
            stream: Some(opened.stream),
            current_batch: Some(opened.batch),
            current_batch_start_row: 0,
            batch_generation: 1,
            cache: Some(cache),
            rows_consumed_total: 0,
        });
        let comparator = RowComparator::new(&self.active_cluster_streams);
        self.heap.push(
            MergeCursor {
                stream_idx,
                batch_generation: 1,
                row_idx: 0,
                segment_row_idx: 0,
            },
            &comparator,
        )?;
        Ok(())
    }

    async fn advance_cursor(&mut self, cursor: MergeCursor) -> Result<Option<MergeCursor>> {
        let stream_state = self
            .active_cluster_streams
            .get_mut(cursor.stream_idx)
            .ok_or_else(|| IngestError::Shift("merge cursor stream index out of bounds".to_string()))?;
        let current_batch = stream_state
            .current_batch
            .as_ref()
            .ok_or_else(|| IngestError::Shift("merge cursor points to missing input batch".to_string()))?;
        let next_row_idx = cursor
            .row_idx
            .checked_add(1)
            .ok_or_else(|| IngestError::Shift("merge row index overflow".to_string()))?;

        if next_row_idx < current_batch.num_rows() {
            let next_segment_row_idx = cursor
                .segment_row_idx
                .checked_add(1)
                .ok_or_else(|| IngestError::Shift("segment row index overflow".to_string()))?;
            return Ok(Some(MergeCursor {
                stream_idx: cursor.stream_idx,
                batch_generation: cursor.batch_generation,
                row_idx: next_row_idx,
                segment_row_idx: next_segment_row_idx,
            }));
        }

        let consumed_rows = u64::try_from(current_batch.num_rows())
            .map_err(|_| IngestError::Shift("batch row count exceeds u64".to_string()))?;
        stream_state.rows_consumed_total = stream_state
            .rows_consumed_total
            .checked_add(consumed_rows)
            .ok_or_else(|| IngestError::Shift("rows consumed overflow".to_string()))?;

        let Some(stream) = stream_state.stream.as_mut() else {
            return Err(IngestError::Shift(format!(
                "missing stream handle for WAL segment {} row group {}",
                stream_state.source.segment_offset, stream_state.source.row_group_idx
            )));
        };
        let Some(next_batch) = next_non_empty_batch(stream).await.map_err(|err| {
            queue_read_error(
                err,
                format!(
                    "failed to advance WAL segment {} row group {}",
                    stream_state.source.segment_offset, stream_state.source.row_group_idx
                ),
            )
        })?
        else {
            stream_state.current_batch = None;
            stream_state.cache = None;
            stream_state.stream = None;
            return Ok(None);
        };
        if next_batch.schema() != self.schema {
            return Err(IngestError::Shift(format!(
                "schema mismatch while advancing WAL segment {} row group {}",
                stream_state.source.segment_offset, stream_state.source.row_group_idx
            )));
        }

        stream_state.current_batch_start_row = stream_state.rows_consumed_total;
        stream_state.batch_generation = stream_state
            .batch_generation
            .checked_add(1)
            .ok_or_else(|| IngestError::Shift("batch generation overflow".to_string()))?;
        stream_state.cache = Some(SortColumnCache::try_new(&next_batch)?);
        stream_state.current_batch = Some(next_batch);

        Ok(Some(MergeCursor {
            stream_idx: cursor.stream_idx,
            batch_generation: stream_state.batch_generation,
            row_idx: 0,
            segment_row_idx: stream_state.current_batch_start_row,
        }))
    }
}

struct OpenedSource {
    source: PlannedRowGroupRead,
    stream: RecordBatchStream,
    batch: RecordBatch,
}

async fn open_source<Q>(
    queue_reader: Arc<Q>,
    topic: Topic,
    source: PlannedRowGroupRead,
    cancel_token: CancellationToken,
) -> Result<Option<OpenedSource>>
where
    Q: QueueReader + 'static + ?Sized,
{
    let mut stream = queue_reader
        .read_segment(&topic, source.segment_offset, &[source.row_group_idx], &cancel_token)
        .await
        .map_err(|err| {
            queue_read_error(
                err.into(),
                format!(
                    "failed to open WAL segment {} row group {}",
                    source.segment_offset, source.row_group_idx
                ),
            )
        })?;
    let Some(batch) = next_non_empty_batch(&mut stream).await.map_err(|err| {
        queue_read_error(
            err,
            format!(
                "failed to read WAL segment {} row group {}",
                source.segment_offset, source.row_group_idx
            ),
        )
    })?
    else {
        return Ok(None);
    };
    Ok(Some(OpenedSource { source, stream, batch }))
}

fn queue_read_error(err: IngestError, context: String) -> IngestError {
    if matches!(err, IngestError::Cancelled) {
        IngestError::Cancelled
    } else {
        IngestError::ShiftQueueRead(format!("{context}: {err}"))
    }
}

struct SortColumnCache {
    columns: Vec<CachedSortColumn>,
}

impl SortColumnCache {
    fn try_new(batch: &RecordBatch) -> Result<Self> {
        let mut columns = Vec::with_capacity(LOGS_MERGE_SORT_COLUMNS.len());
        for column_name in LOGS_MERGE_SORT_COLUMNS {
            let column_idx = batch
                .schema()
                .index_of(column_name)
                .map_err(|err| IngestError::Shift(format!("merge input is missing {column_name}: {err}")))?;
            let column = batch.column(column_idx);
            let cached = match column.data_type() {
                DataType::Utf8 => CachedSortColumn::Utf8 {
                    values: column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| IngestError::Shift(format!("{column_name} must be Utf8")))?
                        .clone(),
                    descending: false,
                    nulls_first: true,
                },
                DataType::Timestamp(TimeUnit::Microsecond, None) => CachedSortColumn::TimestampMicrosecond {
                    values: column
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| IngestError::Shift(format!("{column_name} must be Timestamp(Microsecond)")))?
                        .clone(),
                    descending: true,
                    nulls_first: true,
                },
                other => {
                    return Err(IngestError::Shift(format!(
                        "unsupported merge sort column type for {column_name}: {other:?}"
                    )));
                }
            };
            columns.push(cached);
        }
        Ok(Self { columns })
    }

    fn compare_row(&self, left_row_idx: usize, right: &Self, right_row_idx: usize) -> Result<Ordering> {
        for (left, right) in self.columns.iter().zip(right.columns.iter()) {
            let ordering = match (left, right) {
                (
                    CachedSortColumn::Utf8 {
                        values: left,
                        descending,
                        nulls_first,
                    },
                    CachedSortColumn::Utf8 { values: right, .. },
                ) => compare_utf8(left, left_row_idx, right, right_row_idx, *descending, *nulls_first),
                (
                    CachedSortColumn::TimestampMicrosecond {
                        values: left,
                        descending,
                        nulls_first,
                    },
                    CachedSortColumn::TimestampMicrosecond { values: right, .. },
                ) => compare_timestamp(left, left_row_idx, right, right_row_idx, *descending, *nulls_first),
                _ => {
                    return Err(IngestError::Shift(
                        "sort column cache types do not match across streams".to_string(),
                    ));
                }
            };
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }

        Ok(Ordering::Equal)
    }
}

async fn next_non_empty_batch(stream: &mut RecordBatchStream) -> Result<Option<RecordBatch>> {
    loop {
        match stream.try_next().await? {
            Some(batch) if batch.num_rows() > 0 => return Ok(Some(batch)),
            Some(_) => {}
            None => return Ok(None),
        }
    }
}

fn compare_utf8(
    left: &StringArray,
    left_row_idx: usize,
    right: &StringArray,
    right_row_idx: usize,
    descending: bool,
    nulls_first: bool,
) -> Ordering {
    compare_option_ord(
        (!left.is_null(left_row_idx)).then(|| left.value(left_row_idx)),
        (!right.is_null(right_row_idx)).then(|| right.value(right_row_idx)),
        descending,
        nulls_first,
    )
}

fn compare_timestamp(
    left: &TimestampMicrosecondArray,
    left_row_idx: usize,
    right: &TimestampMicrosecondArray,
    right_row_idx: usize,
    descending: bool,
    nulls_first: bool,
) -> Ordering {
    compare_option_ord(
        (!left.is_null(left_row_idx)).then(|| left.value(left_row_idx)),
        (!right.is_null(right_row_idx)).then(|| right.value(right_row_idx)),
        descending,
        nulls_first,
    )
}

impl PlannedRowGroupRead {
    pub fn from_segments(segments: &[super::SegmentToRead]) -> Vec<Self> {
        let mut plan = Vec::new();
        let mut plan_idx = 0usize;
        for segment in segments {
            for row_group in &segment.row_groups {
                plan.push(Self::from_segment_row_group(
                    segment.segment_offset,
                    row_group,
                    plan_idx,
                ));
                plan_idx += 1;
            }
        }
        plan
    }

    fn from_segment_row_group(segment_offset: u64, row_group: &PlannedRowGroup, plan_idx: usize) -> Self {
        Self {
            segment_offset,
            row_group_idx: row_group.row_group_idx,
            boundary_range: row_group.boundary_range.clone(),
            plan_idx,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use async_trait::async_trait;
    use futures::TryStreamExt;
    use icegate_common::{RowGroupBoundaryKey, RowGroupBoundaryRange};
    use tokio_util::sync::CancellationToken;

    use super::{PlannedRowGroupRead, SortedBatchMerger, SortedBatchMergerConfig};
    use crate::{
        shift::executor::{PlannedRowGroup, SegmentToRead},
        wal_sort::logs_row_group_boundary_range_from_batch,
    };

    #[derive(Debug)]
    struct FakeQueueReader {
        batches: HashMap<(u64, usize), Vec<RecordBatch>>,
    }

    #[async_trait]
    impl icegate_queue::QueueReader for FakeQueueReader {
        async fn plan_segments(
            &self,
            _topic: &icegate_queue::Topic,
            _start_offset: u64,
            _group_by_column_name: &str,
            _max_record_batches_per_task: usize,
            _max_input_bytes_per_task: u64,
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::SegmentsPlan> {
            panic!("unused in merger tests");
        }

        async fn read_segment(
            &self,
            _topic: &icegate_queue::Topic,
            offset: u64,
            record_batch_idxs: &[usize],
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::RecordBatchStream> {
            let row_group_idx = *record_batch_idxs.first().expect("row group idx");
            let batches = self.batches.get(&(offset, row_group_idx)).cloned().unwrap_or_default();
            Ok(Box::pin(futures::stream::iter(batches.into_iter().map(Ok))))
        }

        async fn read_segment_row_group_metadata(
            &self,
            _topic: &icegate_queue::Topic,
            _offset: u64,
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<std::collections::HashMap<usize, String>> {
            panic!("unused in merger tests");
        }
    }

    fn logs_batch(rows: Vec<(Option<&str>, Option<&str>, Option<i64>, i64)>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("cloud_account_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter()
                        .map(|(cloud_account_id, _, _, _)| *cloud_account_id)
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, service_name, _, _)| *service_name).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, _, timestamp, _)| *timestamp).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, _, value)| *value).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .expect("logs batch")
    }

    fn plan(segment_offset: u64, row_group_idx: usize, batch: &RecordBatch) -> SegmentToRead {
        SegmentToRead {
            segment_offset,
            row_groups: vec![PlannedRowGroup {
                row_group_idx,
                row_group_bytes: 1,
                boundary_range: logs_row_group_boundary_range_from_batch(batch).expect("boundary range"),
            }],
        }
    }

    fn plan_with_range(
        segment_offset: u64,
        row_group_idx: usize,
        min_key: RowGroupBoundaryKey,
        max_key: RowGroupBoundaryKey,
    ) -> SegmentToRead {
        SegmentToRead {
            segment_offset,
            row_groups: vec![PlannedRowGroup {
                row_group_idx,
                row_group_bytes: 1,
                boundary_range: RowGroupBoundaryRange { min_key, max_key },
            }],
        }
    }

    fn cluster_sizes_from_metadata(segments: Vec<SegmentToRead>) -> Vec<usize> {
        let mut merger = SortedBatchMerger::<FakeQueueReader> {
            config: SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
            },
            schema: Arc::new(Schema::empty()),
            queue_reader: Arc::new(FakeQueueReader {
                batches: HashMap::new(),
            }),
            pending_row_groups: PlannedRowGroupRead::from_segments(&segments)
                .into_iter()
                .map(super::PendingRowGroupEntry::new)
                .collect(),
            active_cluster_streams: Vec::new(),
            heap: super::MergeHeap::new(),
            active_cluster: None,
            prefetched_batches: std::collections::VecDeque::new(),
        };

        let mut sizes = Vec::new();
        while !merger.pending_row_groups.is_empty() {
            let (cluster, _) = merger.pop_next_cluster_sources().expect("cluster");
            sizes.push(cluster.len());
        }
        sizes
    }

    #[tokio::test]
    async fn merger_preserves_global_order_across_non_overlapping_clusters() {
        let batch_1 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 1)]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(20), 2)]);
        let batch_3 = logs_batch(vec![(Some("acc-2"), Some("svc-1"), Some(10), 3)]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([
                ((1, 0), vec![batch_1.clone()]),
                ((2, 0), vec![batch_2.clone()]),
                ((3, 0), vec![batch_3.clone()]),
            ]),
        });
        let segments = vec![plan(1, 0, &batch_1), plan(2, 0, &batch_2), plan(3, 0, &batch_3)];
        let merger = SortedBatchMerger::try_new(
            queue_reader,
            PlannedRowGroupRead::from_segments(&segments),
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 2,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
            },
        )
        .await
        .expect("merger");

        let batches = merger.into_stream().try_collect::<Vec<_>>().await.expect("merged");
        let values = batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("values");
                (0..batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn merger_merges_overlapping_row_groups_in_single_cluster() {
        let batch_1 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 1)]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(20), 2)]);
        let batch_3 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(40), 3)]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([
                ((1, 0), vec![batch_1.clone()]),
                ((2, 0), vec![batch_2.clone()]),
                ((3, 0), vec![batch_3.clone()]),
            ]),
        });
        let segments = vec![plan(1, 0, &batch_1), plan(2, 0, &batch_2), plan(3, 0, &batch_3)];

        let merger = SortedBatchMerger::try_new(
            queue_reader,
            PlannedRowGroupRead::from_segments(&segments),
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
            },
        )
        .await
        .expect("merger");

        let batches = merger.into_stream().try_collect::<Vec<_>>().await.expect("merged");
        let values = batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("values");
                (0..batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![3, 1, 2]);
    }

    #[test]
    fn cluster_builder_forms_overlap_chain() {
        let segments = vec![
            plan_with_range(
                1,
                0,
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(50),
                },
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(30),
                },
            ),
            plan_with_range(
                2,
                0,
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(40),
                },
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(20),
                },
            ),
            plan_with_range(
                3,
                0,
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(25),
                },
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(10),
                },
            ),
        ];

        assert_eq!(cluster_sizes_from_metadata(segments), vec![3]);
    }

    #[test]
    fn cluster_builder_splits_non_overlapping_row_groups() {
        let segments = vec![
            plan_with_range(
                1,
                0,
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(50),
                },
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(40),
                },
            ),
            plan_with_range(
                2,
                0,
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-b".to_string()),
                    timestamp_micros: Some(30),
                },
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-b".to_string()),
                    timestamp_micros: Some(20),
                },
            ),
        ];

        assert_eq!(cluster_sizes_from_metadata(segments), vec![1, 1]);
    }

    #[test]
    fn cluster_builder_tracks_data_driven_width_pattern() {
        let mut segments = Vec::new();

        for idx in 0..4 {
            let start = 100 - idx as i64;
            segments.push(plan_with_range(
                u64::try_from(idx + 1).expect("segment offset"),
                0,
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(start),
                },
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-a".to_string()),
                    timestamp_micros: Some(80),
                },
            ));
        }
        for idx in 0..2 {
            let start = 70 - idx as i64;
            segments.push(plan_with_range(
                u64::try_from(idx + 10).expect("segment offset"),
                0,
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-b".to_string()),
                    timestamp_micros: Some(start),
                },
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-b".to_string()),
                    timestamp_micros: Some(60),
                },
            ));
        }
        for idx in 0..10 {
            let start = 50 - idx as i64;
            segments.push(plan_with_range(
                u64::try_from(idx + 20).expect("segment offset"),
                0,
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-2".to_string()),
                    service_name: Some("svc-c".to_string()),
                    timestamp_micros: Some(start),
                },
                RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-2".to_string()),
                    service_name: Some("svc-c".to_string()),
                    timestamp_micros: Some(30),
                },
            ));
        }

        assert_eq!(cluster_sizes_from_metadata(segments), vec![4, 2, 10]);
    }
}
