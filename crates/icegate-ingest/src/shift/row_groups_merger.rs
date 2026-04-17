use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, VecDeque},
    sync::Arc,
};

use arrow::{compute::interleave_record_batch, datatypes::SchemaRef, record_batch::RecordBatch};
use futures::{StreamExt, TryStreamExt};
use icegate_queue::{QueueReader, RecordBatchStream, Topic};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    error::{IngestError, Result},
    shift::executor::PlannedRowGroup,
    wal::{RowGroupBoundaryRange, SortColumnCache, SortColumnsDescriptor},
};

const MERGE_CANCEL_CHECK_INTERVAL_ROWS: usize = 1024;

/// Observer for merger lifecycle events.
pub(crate) trait RowGroupsMergerObserver: Send + Sync {
    /// Called when a row group starts participating in an active merge cluster.
    fn on_row_group_opened(&self, segment_offset: u64, row_group_idx: usize, bytes: u64);

    /// Called when a previously opened row group is fully drained or cleaned up.
    fn on_row_group_closed(&self, segment_offset: u64, row_group_idx: usize, bytes: u64);
}

/// No-op merger observer used by default.
#[derive(Default)]
pub(crate) struct NoopRowGroupsMergerObserver;

impl RowGroupsMergerObserver for NoopRowGroupsMergerObserver {
    fn on_row_group_opened(&self, _segment_offset: u64, _row_group_idx: usize, _bytes: u64) {}

    fn on_row_group_closed(&self, _segment_offset: u64, _row_group_idx: usize, _bytes: u64) {}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BatchScratch {
    batch_generation: u64,
    source_idx: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MergeCursor {
    stream_idx: usize,
    batch_generation: u64,
    row_idx: usize,
    /// Absolute row position inside one WAL row group across all streamed batches.
    segment_row_idx: u64,
}

struct MergeHeap {
    cursors: Vec<MergeCursor>,
}

impl MergeHeap {
    const fn new() -> Self {
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

            let smallest = if right < self.cursors.len()
                && comparator.compare(&self.cursors[right], &self.cursors[left])? == Ordering::Less
            {
                right
            } else {
                left
            };

            if comparator.compare(&self.cursors[smallest], &self.cursors[idx])? != Ordering::Less {
                break;
            }
            self.cursors.swap(idx, smallest);
            idx = smallest;
        }

        Ok(Some(result))
    }
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
    open_in_observer: bool,
}

struct RowComparator<'a> {
    streams: &'a [ActiveClusterState],
}

impl<'a> RowComparator<'a> {
    const fn new(streams: &'a [ActiveClusterState]) -> Self {
        Self { streams }
    }

    fn compare(&self, left: &MergeCursor, right: &MergeCursor) -> Result<Ordering> {
        let left_state = self
            .streams
            .get(left.stream_idx)
            .ok_or_else(|| IngestError::Shift("left merge cursor stream is missing".to_string()))?;
        let right_state = self
            .streams
            .get(right.stream_idx)
            .ok_or_else(|| IngestError::Shift("right merge cursor stream is missing".to_string()))?;
        let left_cache = left_state
            .cache
            .as_ref()
            .ok_or_else(|| IngestError::Shift("left merge cursor cache is missing".to_string()))?;
        let right_cache = right_state
            .cache
            .as_ref()
            .ok_or_else(|| IngestError::Shift("right merge cursor cache is missing".to_string()))?;

        let ordering = left_cache.compare_row(left.row_idx, right_cache, right.row_idx)?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }

        // Equal-key rows must keep their original WAL order inside the output parquet file:
        // segment_offset -> row_group_idx -> row position inside that row group.
        // Do not use internal merge mechanics like stream_idx as a business tie-breaker.
        let ordering = left_state.source.compare_wal_position(&right_state.source);
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }

        Ok(left.segment_row_idx.cmp(&right.segment_row_idx))
    }
}

/// Read plan entry normalized for the merger.
///
/// The shift executor produces row groups grouped by segment, but the merger
/// operates on one flat stream of row groups and repeatedly moves them between
/// pending and active states. This type stores the minimal immutable identity
/// the merger needs for that lifecycle: WAL position, validated boundary range,
/// and `plan_idx` preserving the original plan order when a stable tie-breaker
/// is required.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedRowGroupRead {
    plan_idx: usize,
    segment_offset: u64,
    row_group_idx: usize,
    row_group_bytes: u64,
    boundary_range: RowGroupBoundaryRange,
}

impl PlannedRowGroupRead {
    pub fn from_segments(segments: &[super::SegmentToRead]) -> Vec<Self> {
        segments
            .iter()
            .flat_map(|segment| {
                segment
                    .row_groups
                    .iter()
                    .map(move |row_group| (segment.segment_offset, row_group))
            })
            .enumerate()
            .map(|(plan_idx, (segment_offset, row_group))| {
                Self::from_segment_row_group(plan_idx, segment_offset, row_group)
            })
            .collect()
    }

    fn from_segment_row_group(plan_idx: usize, segment_offset: u64, row_group: &PlannedRowGroup) -> Self {
        Self {
            plan_idx,
            segment_offset,
            row_group_idx: row_group.row_group_idx,
            row_group_bytes: row_group.row_group_bytes,
            boundary_range: row_group.boundary_range.clone(),
        }
    }

    fn compare_wal_position(&self, other: &Self) -> Ordering {
        self.segment_offset
            .cmp(&other.segment_offset)
            .then_with(|| self.row_group_idx.cmp(&other.row_group_idx))
    }
}

fn validate_boundary_ranges(read_plan: &[PlannedRowGroupRead]) -> Result<()> {
    let Some(reference) = read_plan.first().map(|row_group| &row_group.boundary_range.min_key) else {
        return Ok(());
    };

    for row_group in read_plan {
        row_group.boundary_range.validate()?;
        row_group.boundary_range.min_key.validate_compatible_structure(reference)?;
        row_group.boundary_range.max_key.validate_compatible_structure(reference)?;
    }

    Ok(())
}

fn validate_unique_row_group_positions(read_plan: &[PlannedRowGroupRead]) -> Result<()> {
    let mut seen_positions: HashMap<(u64, usize), usize> = HashMap::new();
    for row_group in read_plan {
        let position = (row_group.segment_offset, row_group.row_group_idx);
        if let Some(first_plan_idx) = seen_positions.insert(position, row_group.plan_idx) {
            return Err(IngestError::Shift(format!(
                "duplicate row group in read plan: segment_offset={}, row_group_idx={}, first_plan_idx={}, duplicate_plan_idx={}",
                row_group.segment_offset, row_group.row_group_idx, first_plan_idx, row_group.plan_idx
            )));
        }
    }
    Ok(())
}

/// Wrapper for pending row groups stored in `BTreeSet`.
///
/// `PlannedRowGroupRead` intentionally does not define a global `Ord`, because
/// its natural ordering depends on the use site. The merger needs a specific
/// pending-queue order: first by the lower merge-key boundary, then by WAL
/// position, then by `plan_idx`. This wrapper keeps that ordering local to the
/// pending set and avoids leaking it into the base planning type.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingRowGroupWithOrder {
    source: PlannedRowGroupRead,
}

impl PendingRowGroupWithOrder {
    const fn new(source: PlannedRowGroupRead) -> Self {
        Self { source }
    }
}

impl Ord for PendingRowGroupWithOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source
            .boundary_range
            .min_key
            .compare(&other.source.boundary_range.min_key)
            .then_with(|| self.source.compare_wal_position(&other.source))
            .then_with(|| self.source.plan_idx.cmp(&other.source.plan_idx))
    }
}

impl PartialOrd for PendingRowGroupWithOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Cluster is a set of Row Groups that are involved in sorting together at a given time
#[derive(Debug, Clone, Copy, Default)]
struct ActiveClusterStats {
    row_groups: usize,
    row_group_bytes: u64,
    frontier_extensions: usize,
    open_parallelism: usize,
    active_streams: usize,
    empty_streams: usize,
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
    /// Sort descriptor used to compare rows across WAL row groups.
    pub sort_descriptor: &'static SortColumnsDescriptor,
}

/// Streams sorted row groups from WAL and merges them into one sorted output stream.
///
/// The merger only keeps one overlap cluster in memory at a time: it opens the
/// row groups whose key ranges intersect, performs k-way merge across them, and
/// then moves to the next cluster.
pub struct RowGroupsMerger<Q: QueueReader + ?Sized + 'static> {
    /// Immutable read/output settings.
    config: SortedBatchMergerConfig,
    /// Immutable logs sort descriptor shared by all per-batch column caches.
    sort_descriptor: &'static SortColumnsDescriptor,
    /// Schema shared by all opened batches. Filled from the first batch.
    schema: SchemaRef,
    /// Queue reader used to open WAL row groups as Arrow streams.
    queue_reader: Arc<Q>,
    /// Optional observer for merger lifecycle and timings.
    observer: Arc<dyn RowGroupsMergerObserver>,
    /// Row groups that are not opened yet, ordered by their lower boundary.
    pending_row_groups: BTreeSet<PendingRowGroupWithOrder>,
    /// Streams that belong to the current overlap cluster.
    active_cluster_streams: Vec<ActiveClusterState>,
    /// Min-heap of next candidate rows from active streams.
    heap: MergeHeap,
    /// Debug stats for the cluster currently being merged.
    active_cluster: Option<ActiveClusterStats>,
    /// Prefetched merged batches, used to fail fast before storage starts writing.
    prefetched_batches: VecDeque<RecordBatch>,
    /// Reused source batches for one merged output batch.
    merge_source_batches: Vec<RecordBatch>,
    /// Mapping from stream index to source-batch slot for the current output batch.
    merge_source_batch_by_stream: Vec<Option<BatchScratch>>,
    /// Reused interleave indices for one merged output batch.
    merge_interleave_indices: Vec<(usize, usize)>,
}

impl<Q> RowGroupsMerger<Q>
where
    Q: QueueReader + 'static + ?Sized,
{
    /// Build merger state from shift segments by flattening their row groups into one validated read plan.
    pub fn new(
        queue_reader: Arc<Q>,
        segments: &[super::SegmentToRead],
        config: SortedBatchMergerConfig,
    ) -> Result<Self> {
        let read_plan = PlannedRowGroupRead::from_segments(segments);
        Self::new_with_plan(queue_reader, read_plan, config, Arc::new(NoopRowGroupsMergerObserver))
    }

    /// Attach merger lifecycle observer.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn RowGroupsMergerObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Create a merger from an already flattened read plan.
    ///
    /// At this point no WAL data is opened yet. The method only validates the
    /// plan and initializes the pending set ordered by lower boundary.
    fn new_with_plan(
        queue_reader: Arc<Q>,
        read_plan: Vec<PlannedRowGroupRead>,
        config: SortedBatchMergerConfig,
        observer: Arc<dyn RowGroupsMergerObserver>,
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
        validate_boundary_ranges(&read_plan)?;
        validate_unique_row_group_positions(&read_plan)?;

        let sort_descriptor = config.sort_descriptor;
        Ok(Self {
            config,
            sort_descriptor,
            schema: SchemaRef::new(arrow::datatypes::Schema::empty()),
            queue_reader,
            observer,
            pending_row_groups: read_plan.into_iter().map(PendingRowGroupWithOrder::new).collect(),
            active_cluster_streams: Vec::new(),
            heap: MergeHeap::new(),
            active_cluster: None,
            prefetched_batches: VecDeque::new(),
            merge_source_batches: Vec::new(),
            merge_source_batch_by_stream: Vec::new(),
            merge_interleave_indices: Vec::new(),
        })
    }

    /// Preload the first merged output batch so `QueueRead` failures happen
    /// before the storage write pipeline is started.
    pub async fn prefetch_first_group(&mut self) -> Result<()> {
        if self.prefetched_batches.is_empty() {
            if let Some(batch) = self.next_group_inner().await? {
                self.prefetched_batches.push_back(batch);
            }
        }
        Ok(())
    }

    pub fn into_stream(self) -> super::iceberg_storage::BoxRecordBatchStream {
        futures::stream::try_unfold(self, |mut merger| async move {
            (merger.next_group().await?).map_or_else(|| Ok(None), |batch| Ok(Some((batch, merger))))
        })
        .boxed()
    }

    /// Return the next merged batch, serving a prefetched batch first when
    /// `prefetch_first_batch` already opened the pipeline.
    async fn next_group(&mut self) -> Result<Option<RecordBatch>> {
        if let Some(batch) = self.prefetched_batches.pop_front() {
            return Ok(Some(batch));
        }
        self.next_group_inner().await
    }

    /// Core merge loop.
    ///
    /// The method ensures that exactly one overlap cluster is active, repeatedly
    /// extracts the smallest next row from its heap, and emits up to
    /// `row_group_size` merged rows as one output batch.
    async fn next_group_inner(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            self.check_cancelled()?;

            // Open the next group of overlapping ranges only when the current cluster is exhausted.
            if self.heap.is_empty() && !self.open_next_overlap_cluster().await? {
                return Ok(None);
            }

            if self.active_cluster_streams.len() == 1 {
                if let Some(batch) = self.next_single_stream_group().await? {
                    return Ok(Some(batch));
                }
                self.complete_active_cluster();
                continue;
            }

            self.merge_source_batches.clear();
            self.merge_source_batches.reserve(self.active_cluster_streams.len());
            if self.merge_source_batch_by_stream.len() < self.active_cluster_streams.len() {
                self.merge_source_batch_by_stream
                    .resize(self.active_cluster_streams.len(), None);
            }
            for source_slot in &mut self.merge_source_batch_by_stream[..self.active_cluster_streams.len()] {
                *source_slot = None;
            }
            self.merge_interleave_indices.clear();
            self.merge_interleave_indices.reserve(self.config.row_group_size);

            // Repeatedly take the smallest next row across active streams.
            while self.merge_interleave_indices.len() < self.config.row_group_size {
                if self.merge_interleave_indices.len() % MERGE_CANCEL_CHECK_INTERVAL_ROWS == 0 {
                    self.check_cancelled()?;
                }
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

                let source_idx = match self.merge_source_batch_by_stream.get(cursor.stream_idx).and_then(|slot| *slot) {
                    Some(scratch) if scratch.batch_generation == cursor.batch_generation => scratch.source_idx,
                    _ => {
                        let source_idx = self.merge_source_batches.len();
                        self.merge_source_batches.push(current_batch.clone());
                        self.merge_source_batch_by_stream[cursor.stream_idx] = Some(BatchScratch {
                            batch_generation: cursor.batch_generation,
                            source_idx,
                        });
                        source_idx
                    }
                };
                self.merge_interleave_indices.push((source_idx, cursor.row_idx));

                let next_cursor = self.advance_cursor(cursor).await?;
                if let Some(next_cursor) = next_cursor {
                    let comparator = RowComparator::new(&self.active_cluster_streams);
                    self.heap.push(next_cursor, &comparator)?;
                }
            }

            if self.merge_interleave_indices.is_empty() {
                self.complete_active_cluster();
                continue;
            }

            self.check_cancelled()?;
            let merged = self.build_output_batch()?;
            self.clear_merge_scratch();
            if self.heap.is_empty() {
                self.complete_active_cluster();
            }
            return Ok(Some(merged));
        }
    }

    /// Emit the next output for a one-source cluster without heap/interleave work.
    async fn next_single_stream_group(&mut self) -> Result<Option<RecordBatch>> {
        self.check_cancelled()?;

        let Some(cursor) = ({
            let comparator = RowComparator::new(&self.active_cluster_streams);
            self.heap.pop(&comparator)?
        }) else {
            return Ok(None);
        };
        let row_group_size = self.config.row_group_size;
        let (batch, rows_to_emit, current_batch_rows) = {
            let stream_state = self
                .active_cluster_streams
                .get(cursor.stream_idx)
                .ok_or_else(|| IngestError::Shift("merge cursor stream index out of bounds".to_string()))?;
            if stream_state.batch_generation != cursor.batch_generation {
                return Err(IngestError::Shift("stale merge cursor generation".to_string()));
            }
            let current_batch = stream_state
                .current_batch
                .as_ref()
                .ok_or_else(|| IngestError::Shift("merge cursor points to missing input batch".to_string()))?;
            let remaining_rows = current_batch
                .num_rows()
                .checked_sub(cursor.row_idx)
                .ok_or_else(|| IngestError::Shift("merge cursor row index out of bounds".to_string()))?;
            let rows_to_emit = remaining_rows.min(row_group_size);
            (
                current_batch.slice(cursor.row_idx, rows_to_emit),
                rows_to_emit,
                current_batch.num_rows(),
            )
        };

        if rows_to_emit == 0 {
            return Err(IngestError::Shift(
                "single-source merge cursor produced an empty output slice".to_string(),
            ));
        }
        let next_row_idx = cursor
            .row_idx
            .checked_add(rows_to_emit)
            .ok_or_else(|| IngestError::Shift("merge row index overflow".to_string()))?;
        if next_row_idx < current_batch_rows {
            let rows_to_emit = u64::try_from(rows_to_emit)
                .map_err(|_| IngestError::Shift("emitted row count exceeds u64".to_string()))?;
            let next_segment_row_idx = cursor
                .segment_row_idx
                .checked_add(rows_to_emit)
                .ok_or_else(|| IngestError::Shift("segment row index overflow".to_string()))?;
            let comparator = RowComparator::new(&self.active_cluster_streams);
            self.heap.push(
                MergeCursor {
                    stream_idx: cursor.stream_idx,
                    batch_generation: cursor.batch_generation,
                    row_idx: next_row_idx,
                    segment_row_idx: next_segment_row_idx,
                },
                &comparator,
            )?;
        } else {
            let rows_to_emit = u64::try_from(rows_to_emit)
                .map_err(|_| IngestError::Shift("emitted row count exceeds u64".to_string()))?;
            let last_segment_row_idx = cursor
                .segment_row_idx
                .checked_add(rows_to_emit)
                .and_then(|idx| idx.checked_sub(1))
                .ok_or_else(|| IngestError::Shift("segment row index overflow".to_string()))?;
            let last_cursor = MergeCursor {
                stream_idx: cursor.stream_idx,
                batch_generation: cursor.batch_generation,
                row_idx: next_row_idx - 1,
                segment_row_idx: last_segment_row_idx,
            };
            if let Some(next_cursor) = self.advance_cursor(last_cursor).await? {
                let comparator = RowComparator::new(&self.active_cluster_streams);
                self.heap.push(next_cursor, &comparator)?;
            }
        }

        if self.heap.is_empty() {
            self.complete_active_cluster();
        }
        self.check_cancelled()?;
        Ok(Some(batch))
    }

    /// Build an output batch, avoiding interleave when it is a contiguous slice
    /// of one source batch.
    fn build_output_batch(&self) -> Result<RecordBatch> {
        if let Some(batch) = self.contiguous_single_source_output()? {
            return Ok(batch);
        }

        let source_refs = self.merge_source_batches.iter().collect::<Vec<_>>();
        Ok(interleave_record_batch(&source_refs, &self.merge_interleave_indices)?)
    }

    fn contiguous_single_source_output(&self) -> Result<Option<RecordBatch>> {
        let Some(&(source_idx, start_row_idx)) = self.merge_interleave_indices.first() else {
            return Ok(None);
        };
        let source_batch = self
            .merge_source_batches
            .get(source_idx)
            .ok_or_else(|| IngestError::Shift("interleave source batch index out of bounds".to_string()))?;

        for (offset, &(candidate_source_idx, row_idx)) in self.merge_interleave_indices.iter().enumerate() {
            let expected_row_idx = start_row_idx
                .checked_add(offset)
                .ok_or_else(|| IngestError::Shift("contiguous source row index overflow".to_string()))?;
            if candidate_source_idx != source_idx || row_idx != expected_row_idx {
                return Ok(None);
            }
        }

        Ok(Some(
            source_batch.slice(start_row_idx, self.merge_interleave_indices.len()),
        ))
    }

    fn clear_merge_scratch(&mut self) {
        self.merge_source_batches.clear();
        self.merge_interleave_indices.clear();
        for source_slot in self
            .merge_source_batch_by_stream
            .iter_mut()
            .take(self.active_cluster_streams.len())
        {
            *source_slot = None;
        }
    }

    fn check_cancelled(&self) -> Result<()> {
        if self.config.cancel_token.is_cancelled() {
            Err(IngestError::Cancelled)
        } else {
            Ok(())
        }
    }

    /// Open the next non-empty overlap cluster from the pending set.
    ///
    /// Pending row groups are partitioned by boundary-range intersection. All
    /// row groups that can overlap in sort order must be opened together and
    /// merged through one heap; disjoint groups can be processed later.
    async fn open_next_overlap_cluster(&mut self) -> Result<bool> {
        // TODO(high): Consider replacing eager overlap-cluster bootstrap with a lazy safe-to-emit
        // merge strategy. The current implementation opens every row group in the
        // transitive overlap cluster before emitting the first output row, which can
        // increase peak memory for long overlap chains.
        //
        // A lazy version must not simply cap opened row groups by read_parallelism:
        // that would break global ordering. It must only emit a heap minimum after all
        // pending row groups with boundary_range.min_key <= current heap minimum key
        // have been opened and added to the heap. If that barrier includes the whole
        // cluster, opening the whole cluster is required for correctness.

        while !self.pending_row_groups.is_empty() {
            // Row groups with intersecting boundary ranges must be merged together.
            let (cluster_sources, mut cluster_stats) = self.pop_next_cluster_sources()?;
            let open_parallelism = cluster_sources.len().min(self.config.read_parallelism);
            cluster_stats.open_parallelism = open_parallelism;
            debug!(
                cluster_row_groups = cluster_stats.row_groups,
                cluster_row_group_bytes = cluster_stats.row_group_bytes,
                cluster_frontier_extensions = cluster_stats.frontier_extensions,
                cluster_open_parallelism = cluster_stats.open_parallelism,
                "shift merger cluster started"
            );
            let queue_reader = Arc::clone(&self.queue_reader);
            let topic = self.config.topic.clone();
            let cancel_token = self.config.cancel_token.clone();
            let mut opened_stream = futures::stream::iter(cluster_sources.into_iter().map(|source| {
                let queue_reader = Arc::clone(&queue_reader);
                let topic = topic.clone();
                let cancel_token = cancel_token.clone();
                async move { open_source(queue_reader, topic, source, cancel_token).await }
            }))
            .buffer_unordered(open_parallelism);

            while let Some(opened) = opened_stream.try_next().await? {
                match opened {
                    Some(opened) => {
                        self.push_opened_source(opened)?;
                        cluster_stats.active_streams = cluster_stats
                            .active_streams
                            .checked_add(1)
                            .ok_or_else(|| IngestError::Shift("active cluster stream counter overflow".to_string()))?;
                    }
                    None => {
                        cluster_stats.empty_streams = cluster_stats
                            .empty_streams
                            .checked_add(1)
                            .ok_or_else(|| IngestError::Shift("empty cluster stream counter overflow".to_string()))?;
                    }
                }
            }

            debug!(
                cluster_row_groups = cluster_stats.row_groups,
                cluster_row_group_bytes = cluster_stats.row_group_bytes,
                cluster_frontier_extensions = cluster_stats.frontier_extensions,
                cluster_open_parallelism = cluster_stats.open_parallelism,
                cluster_active_streams = cluster_stats.active_streams,
                cluster_empty_streams = cluster_stats.empty_streams,
                "shift merger cluster bootstrap completed"
            );

            self.active_cluster = Some(cluster_stats);
            if !self.heap.is_empty() {
                return Ok(true);
            }
            self.complete_active_cluster();
        }
        Ok(false)
    }

    /// Remove the next overlap cluster from the pending set and return its
    /// sources together with debug stats.
    ///
    /// The first pending row group starts the cluster. Then the cluster frontier
    /// is extended while the next row group's lower bound is still inside the
    /// current frontier.
    fn pop_next_cluster_sources(&mut self) -> Result<(Vec<PlannedRowGroupRead>, ActiveClusterStats)> {
        let first = self.pop_pending_source().ok_or_else(|| {
            IngestError::Shift("pending row groups disappeared while building overlap cluster".to_string())
        })?;

        // Extend the cluster while the next row group's lower bound still overlaps the frontier.
        let mut cluster_sources = Vec::new();
        let mut frontier = first.boundary_range.max_key.clone();
        let mut frontier_extensions = 0usize;
        cluster_sources.push(first);

        while let Some(next_pending) = self.peek_pending_source() {
            if next_pending.boundary_range.min_key.compare(&frontier) == Ordering::Greater {
                break;
            }
            let next = self.pop_pending_source().ok_or_else(|| {
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

        let cluster_row_group_bytes = cluster_sources.iter().try_fold(0_u64, |total, source| {
            total
                .checked_add(source.row_group_bytes)
                .ok_or_else(|| IngestError::Shift("cluster row group bytes overflow".to_string()))
        })?;
        let stats = ActiveClusterStats {
            row_groups: cluster_sources.len(),
            row_group_bytes: cluster_row_group_bytes,
            frontier_extensions,
            open_parallelism: 0,
            active_streams: 0,
            empty_streams: 0,
        };

        Ok((cluster_sources, stats))
    }

    /// Drop all state associated with the current cluster after it has been
    /// fully drained or found empty.
    fn complete_active_cluster(&mut self) {
        for stream_state in &mut self.active_cluster_streams {
            if stream_state.open_in_observer {
                self.observer.on_row_group_closed(
                    stream_state.source.segment_offset,
                    stream_state.source.row_group_idx,
                    stream_state.source.row_group_bytes,
                );
                stream_state.open_in_observer = false;
            }
        }
        if let Some(stats) = self.active_cluster.take() {
            debug!(
                cluster_row_groups = stats.row_groups,
                cluster_row_group_bytes = stats.row_group_bytes,
                cluster_frontier_extensions = stats.frontier_extensions,
                cluster_open_parallelism = stats.open_parallelism,
                cluster_active_streams = stats.active_streams,
                cluster_empty_streams = stats.empty_streams,
                "shift merger cluster completed"
            );
        }
        self.active_cluster_streams.clear();
        self.heap = MergeHeap::new();
    }

    /// Inspect the smallest pending row group without removing it.
    fn peek_pending_source(&self) -> Option<&PlannedRowGroupRead> {
        self.pending_row_groups.iter().next().map(|entry| &entry.source)
    }

    /// Remove the smallest pending row group according to pending-set ordering.
    fn pop_pending_source(&mut self) -> Option<PlannedRowGroupRead> {
        self.pending_row_groups.pop_first().map(|entry| entry.source)
    }

    /// Register one opened WAL source in the active cluster and seed the heap
    /// with the first row from its first non-empty batch.
    fn push_opened_source(&mut self, opened: OpenedSource) -> Result<()> {
        let opened_bytes = opened.source.row_group_bytes;
        if self.schema.fields().is_empty() {
            self.schema = opened.row_group.schema();
        } else if opened.row_group.schema() != self.schema {
            return Err(IngestError::Shift(format!(
                "schema mismatch in WAL segment {} row group {}",
                opened.source.segment_offset, opened.source.row_group_idx
            )));
        }

        let cache = SortColumnCache::try_new(&opened.row_group, self.sort_descriptor, "merge input")?;
        let stream_idx = self.active_cluster_streams.len();
        self.active_cluster_streams.push(ActiveClusterState {
            source: opened.source,
            stream: Some(opened.stream),
            current_batch: Some(opened.row_group),
            current_batch_start_row: 0,
            batch_generation: 1,
            cache: Some(cache),
            rows_consumed_total: 0,
            open_in_observer: false,
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
        // Set the flag first so Drop can always balance opened/closed callbacks
        // even if observer implementation panics.
        let stream_state = self
            .active_cluster_streams
            .get_mut(stream_idx)
            .ok_or_else(|| IngestError::Shift("cannot access merger stream state after push".to_string()))?;
        stream_state.open_in_observer = true;
        self.observer.on_row_group_opened(
            stream_state.source.segment_offset,
            stream_state.source.row_group_idx,
            opened_bytes,
        );
        Ok(())
    }

    /// Advance one merge cursor within its source.
    ///
    /// If the current Arrow batch still has rows, the cursor moves locally.
    /// Otherwise the method pulls the next non-empty batch from the same WAL row
    /// group, refreshes sort caches, and returns a cursor pointing at the new
    /// batch start. When the source is exhausted, it returns `None`.
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
                &err,
                &format!(
                    "failed to advance WAL segment {} row group {}",
                    stream_state.source.segment_offset, stream_state.source.row_group_idx
                ),
            )
        })?
        else {
            if stream_state.open_in_observer {
                self.observer.on_row_group_closed(
                    stream_state.source.segment_offset,
                    stream_state.source.row_group_idx,
                    stream_state.source.row_group_bytes,
                );
                stream_state.open_in_observer = false;
            }
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
        stream_state.cache = Some(SortColumnCache::try_new(
            &next_batch,
            self.sort_descriptor,
            "merge input",
        )?);
        stream_state.current_batch = Some(next_batch);

        Ok(Some(MergeCursor {
            stream_idx: cursor.stream_idx,
            batch_generation: stream_state.batch_generation,
            row_idx: 0,
            segment_row_idx: stream_state.current_batch_start_row,
        }))
    }
}

impl<Q> Drop for RowGroupsMerger<Q>
where
    Q: QueueReader + ?Sized + 'static,
{
    fn drop(&mut self) {
        self.complete_active_cluster();
    }
}

struct OpenedSource {
    source: PlannedRowGroupRead,
    stream: RecordBatchStream,
    row_group: RecordBatch,
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
                &err.into(),
                &format!(
                    "failed to open WAL segment {} row group {}",
                    source.segment_offset, source.row_group_idx
                ),
            )
        })?;
    let Some(batch) = next_non_empty_batch(&mut stream).await.map_err(|err| {
        queue_read_error(
            &err,
            &format!(
                "failed to read WAL segment {} row group {}",
                source.segment_offset, source.row_group_idx
            ),
        )
    })?
    else {
        return Ok(None);
    };
    Ok(Some(OpenedSource {
        source,
        stream,
        row_group: batch,
    }))
}

fn queue_read_error(err: &IngestError, context: &str) -> IngestError {
    if matches!(err, IngestError::Cancelled) {
        IngestError::Cancelled
    } else {
        IngestError::ShiftQueueRead(format!("{context}: {err}"))
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

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicI64, AtomicUsize, Ordering},
        },
    };

    use arrow::{
        array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use async_trait::async_trait;
    use futures::TryStreamExt;
    use tokio::time::{Duration, sleep};
    use tokio_util::sync::CancellationToken;

    use super::{
        NoopRowGroupsMergerObserver, PlannedRowGroupRead, RowGroupsMerger, RowGroupsMergerObserver,
        SortedBatchMergerConfig,
    };
    use crate::{
        error::IngestError,
        shift::executor::{PlannedRowGroup, SegmentToRead},
        wal::{
            RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, SortColumnsDescriptor,
            logs_row_group_boundary_range_from_batch,
        },
    };

    #[derive(Debug, Default)]
    struct FakeQueueReader {
        batches: HashMap<(u64, usize), Vec<RecordBatch>>,
        open_delays: HashMap<(u64, usize), Duration>,
        error_after_n_reads: Option<usize>,
        read_segment_calls: AtomicUsize,
    }

    #[derive(Default)]
    struct FakeObserver {
        open_row_groups: AtomicI64,
        open_row_group_bytes: AtomicI64,
        opened_calls: AtomicUsize,
        closed_calls: AtomicUsize,
        lifetime_calls: AtomicUsize,
        opened_at: Mutex<HashMap<(u64, usize), std::time::Instant>>,
        lifetimes: Mutex<Vec<Duration>>,
    }

    impl FakeObserver {
        fn open_row_groups(&self) -> i64 {
            self.open_row_groups.load(Ordering::SeqCst)
        }

        fn open_row_group_bytes(&self) -> i64 {
            self.open_row_group_bytes.load(Ordering::SeqCst)
        }

        fn lifetime_calls(&self) -> usize {
            self.lifetime_calls.load(Ordering::SeqCst)
        }

        fn lifetimes(&self) -> Vec<Duration> {
            self.lifetimes.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
        }

        fn opened_calls(&self) -> usize {
            self.opened_calls.load(Ordering::SeqCst)
        }

        fn closed_calls(&self) -> usize {
            self.closed_calls.load(Ordering::SeqCst)
        }
    }

    impl RowGroupsMergerObserver for FakeObserver {
        fn on_row_group_opened(&self, segment_offset: u64, row_group_idx: usize, bytes: u64) {
            let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
            self.opened_calls.fetch_add(1, Ordering::SeqCst);
            self.open_row_groups.fetch_add(1, Ordering::SeqCst);
            self.open_row_group_bytes.fetch_add(bytes, Ordering::SeqCst);
            self.opened_at
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert((segment_offset, row_group_idx), std::time::Instant::now());
        }

        fn on_row_group_closed(&self, segment_offset: u64, row_group_idx: usize, bytes: u64) {
            let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
            self.closed_calls.fetch_add(1, Ordering::SeqCst);
            self.open_row_groups.fetch_sub(1, Ordering::SeqCst);
            self.open_row_group_bytes.fetch_sub(bytes, Ordering::SeqCst);
            let opened_at = self
                .opened_at
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&(segment_offset, row_group_idx));
            if let Some(opened_at) = opened_at {
                self.lifetime_calls.fetch_add(1, Ordering::SeqCst);
                self.lifetimes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(opened_at.elapsed());
            }
        }
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
            topic: &icegate_queue::Topic,
            offset: u64,
            record_batch_idxs: &[usize],
            _cancel_token: &CancellationToken,
        ) -> icegate_queue::Result<icegate_queue::RecordBatchStream> {
            let read_segment_call = self.read_segment_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.error_after_n_reads.is_some_and(|limit| read_segment_call >= limit) {
                return Err(icegate_queue::QueueError::Read {
                    topic: topic.as_str().to_string(),
                    offset,
                    source: Box::new(std::io::Error::other("injected read_segment error in tests")),
                });
            }
            let Some(&row_group_idx) = record_batch_idxs.first() else {
                return Err(icegate_queue::QueueError::Read {
                    topic: topic.as_str().to_string(),
                    offset,
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "record_batch_idxs is empty",
                    )),
                });
            };
            if let Some(delay) = self.open_delays.get(&(offset, row_group_idx)) {
                sleep(*delay).await;
            }
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

    #[allow(clippy::needless_pass_by_value)]
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
        plan_with_bytes(segment_offset, row_group_idx, batch, 1)
    }

    fn plan_with_bytes(
        segment_offset: u64,
        row_group_idx: usize,
        batch: &RecordBatch,
        row_group_bytes: u64,
    ) -> SegmentToRead {
        SegmentToRead {
            segment_offset,
            row_groups: vec![PlannedRowGroup {
                row_group_idx,
                row_group_bytes,
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

    fn key(
        cloud_account_id: Option<&str>,
        service_name: Option<&str>,
        timestamp_micros: Option<i64>,
    ) -> RowGroupBoundaryKey {
        RowGroupBoundaryKey {
            components: vec![
                RowGroupBoundaryComponent::string(cloud_account_id.map(str::to_string), false, true),
                RowGroupBoundaryComponent::string(service_name.map(str::to_string), false, true),
                RowGroupBoundaryComponent::timestamp_micros(timestamp_micros, true, true),
            ],
        }
    }

    fn cluster_sizes_from_metadata(segments: impl AsRef<[SegmentToRead]>) -> Vec<usize> {
        let segments = segments.as_ref();
        let sort_descriptor = SortColumnsDescriptor::logs().expect("sort descriptor");
        let mut merger = RowGroupsMerger::<FakeQueueReader> {
            config: SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
                sort_descriptor,
            },
            sort_descriptor,
            schema: Arc::new(Schema::empty()),
            queue_reader: Arc::new(FakeQueueReader {
                batches: HashMap::new(),
                open_delays: HashMap::new(),
                ..FakeQueueReader::default()
            }),
            observer: Arc::new(NoopRowGroupsMergerObserver),
            pending_row_groups: PlannedRowGroupRead::from_segments(segments)
                .into_iter()
                .map(super::PendingRowGroupWithOrder::new)
                .collect(),
            active_cluster_streams: Vec::new(),
            heap: super::MergeHeap::new(),
            active_cluster: None,
            prefetched_batches: std::collections::VecDeque::new(),
            merge_source_batches: Vec::new(),
            merge_source_batch_by_stream: Vec::new(),
            merge_interleave_indices: Vec::new(),
        };

        let mut sizes = Vec::new();
        while !merger.pending_row_groups.is_empty() {
            let (cluster, _) = merger.pop_next_cluster_sources().expect("cluster");
            sizes.push(cluster.len());
        }
        sizes
    }

    async fn merged_values(queue_reader: Arc<FakeQueueReader>, segments: Vec<SegmentToRead>) -> Vec<i64> {
        merged_values_with_row_group_size(queue_reader, segments, 8).await
    }

    async fn merged_values_with_row_group_size(
        queue_reader: Arc<FakeQueueReader>,
        segments: Vec<SegmentToRead>,
        row_group_size: usize,
    ) -> Vec<i64> {
        let merger = RowGroupsMerger::new(
            queue_reader,
            &segments,
            SortedBatchMergerConfig {
                row_group_size,
                read_parallelism: 2,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger");

        merger
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .expect("merged")
            .into_iter()
            .flat_map(|batch| {
                let values = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("values");
                (0..batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect()
    }

    async fn merged_batches_with_row_group_size(
        queue_reader: Arc<FakeQueueReader>,
        segments: Vec<SegmentToRead>,
        row_group_size: usize,
    ) -> Vec<RecordBatch> {
        let merger = RowGroupsMerger::new(
            queue_reader,
            &segments,
            SortedBatchMergerConfig {
                row_group_size,
                read_parallelism: 2,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger");

        merger.into_stream().try_collect::<Vec<_>>().await.expect("merged")
    }

    async fn merged_values_with_observer(
        queue_reader: Arc<FakeQueueReader>,
        segments: Vec<SegmentToRead>,
        observer: Arc<dyn RowGroupsMergerObserver>,
    ) -> Vec<i64> {
        let merger = RowGroupsMerger::new(
            queue_reader,
            &segments,
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 2,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
            },
        )
        .expect("merger")
        .with_observer(observer);

        merger
            .into_stream()
            .try_collect::<Vec<_>>()
            .await
            .expect("merged")
            .into_iter()
            .flat_map(|batch| {
                let values = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("values");
                (0..batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect()
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
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let segments = vec![plan(1, 0, &batch_1), plan(2, 0, &batch_2), plan(3, 0, &batch_3)];
        let merger = RowGroupsMerger::new(
            queue_reader,
            &segments,
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 2,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
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
    async fn merger_slices_single_source_cluster_without_reordering() {
        let batch = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 1),
            (Some("acc-1"), Some("svc-1"), Some(20), 2),
            (Some("acc-1"), Some("svc-1"), Some(10), 3),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });

        let batches = merged_batches_with_row_group_size(queue_reader, vec![plan(1, 0, &batch)], 2).await;
        let values = batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("values");
                (0..batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn merger_slices_contiguous_single_source_output_inside_mixed_cluster() {
        let batch_1 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(40), 1),
            (Some("acc-1"), Some("svc-1"), Some(30), 2),
        ]);
        let batch_2 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 3),
            (Some("acc-1"), Some("svc-1"), Some(10), 4),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch_1.clone()]), ((2, 0), vec![batch_2.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });

        let values =
            merged_values_with_row_group_size(queue_reader, vec![plan(1, 0, &batch_1), plan(2, 0, &batch_2)], 2).await;

        assert_eq!(values, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn merger_releases_interleave_sources_before_yielding_output() {
        let batch_1 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(40), 1),
            (Some("acc-1"), Some("svc-1"), Some(20), 3),
        ]);
        let batch_2 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 2),
            (Some("acc-1"), Some("svc-1"), Some(10), 4),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch_1.clone()]), ((2, 0), vec![batch_2.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let batch_1_value_column = Arc::clone(batch_1.column(3));
        let batch_2_value_column = Arc::clone(batch_2.column(3));

        let merger = RowGroupsMerger::new(
            queue_reader,
            &[plan(1, 0, &batch_1), plan(2, 0, &batch_2)],
            SortedBatchMergerConfig {
                row_group_size: 4,
                read_parallelism: 2,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger");

        let mut stream = merger.into_stream();
        let merged_batch = stream.try_next().await.expect("next merged batch").expect("merged batch");
        let values = merged_batch.column(3).as_any().downcast_ref::<Int64Array>().expect("values");
        assert_eq!(
            (0..merged_batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(Arc::strong_count(&batch_1_value_column), 3);
        assert_eq!(Arc::strong_count(&batch_2_value_column), 3);
    }

    #[tokio::test]
    async fn merger_returns_cancelled_before_emitting_next_batch() {
        let batch_1 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(60), 1),
            (Some("acc-1"), Some("svc-1"), Some(40), 3),
            (Some("acc-1"), Some("svc-1"), Some(20), 5),
        ]);
        let batch_2 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(50), 2),
            (Some("acc-1"), Some("svc-1"), Some(30), 4),
            (Some("acc-1"), Some("svc-1"), Some(10), 6),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch_1.clone()]), ((2, 0), vec![batch_2.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let cancel_token = CancellationToken::new();
        let merger = RowGroupsMerger::new(
            queue_reader,
            &[plan(1, 0, &batch_1), plan(2, 0, &batch_2)],
            SortedBatchMergerConfig {
                row_group_size: 2,
                read_parallelism: 2,
                topic: "logs".to_string(),
                cancel_token: cancel_token.clone(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger");

        let mut stream = merger.into_stream();
        let first_batch = stream.try_next().await.expect("first batch").expect("first batch");
        let values = first_batch.column(3).as_any().downcast_ref::<Int64Array>().expect("values");
        assert_eq!(
            (0..first_batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>(),
            vec![1, 2]
        );

        cancel_token.cancel();
        let err = stream.try_next().await.expect_err("next batch must be cancelled");
        assert!(matches!(err, IngestError::Cancelled));
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
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let segments = vec![plan(1, 0, &batch_1), plan(2, 0, &batch_2), plan(3, 0, &batch_3)];

        let merger = RowGroupsMerger::new(
            queue_reader,
            &segments,
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
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

    #[tokio::test]
    async fn merger_preserves_wal_order_for_equal_keys_across_segments() {
        let batch_1 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 11),
            (Some("acc-1"), Some("svc-1"), Some(30), 12),
        ]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 21)]);
        let batch_3 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 31),
            (Some("acc-1"), Some("svc-1"), Some(30), 32),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([
                ((10, 0), vec![batch_1.clone()]),
                ((11, 0), vec![batch_2.clone()]),
                ((12, 0), vec![batch_3.clone()]),
            ]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let values = merged_values(
            queue_reader,
            vec![plan(10, 0, &batch_1), plan(11, 0, &batch_2), plan(12, 0, &batch_3)],
        )
        .await;

        assert_eq!(values, vec![11, 12, 21, 31, 32]);
    }

    #[tokio::test]
    async fn merger_preserves_wal_order_for_equal_keys_across_row_groups_in_segment() {
        let row_group_0 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 101),
            (Some("acc-1"), Some("svc-1"), Some(30), 102),
        ]);
        let row_group_1 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 201),
            (Some("acc-1"), Some("svc-1"), Some(30), 202),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((7, 0), vec![row_group_0.clone()]), ((7, 1), vec![row_group_1.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let values = merged_values(queue_reader, vec![plan(7, 0, &row_group_0), plan(7, 1, &row_group_1)]).await;

        assert_eq!(values, vec![101, 102, 201, 202]);
    }

    #[tokio::test]
    async fn merger_preserves_wal_order_when_cluster_opens_finish_out_of_order() {
        let batch_1 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 11)]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 21)]);
        let batch_3 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 31)]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([
                ((10, 0), vec![batch_1.clone()]),
                ((11, 0), vec![batch_2.clone()]),
                ((12, 0), vec![batch_3.clone()]),
            ]),
            open_delays: HashMap::from([
                ((10, 0), Duration::from_millis(30)),
                ((11, 0), Duration::from_millis(10)),
                ((12, 0), Duration::from_millis(0)),
            ]),
            ..FakeQueueReader::default()
        });
        let values = merged_values(
            queue_reader,
            vec![plan(10, 0, &batch_1), plan(11, 0, &batch_2), plan(12, 0, &batch_3)],
        )
        .await;

        assert_eq!(values, vec![11, 21, 31]);
    }

    #[tokio::test]
    async fn merger_observer_tracks_opened_and_closed_row_groups() {
        let batch_1 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(40), 1)]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 2)]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch_1.clone()]), ((2, 0), vec![batch_2.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let observer = Arc::new(FakeObserver::default());
        let merger = RowGroupsMerger::new(
            queue_reader,
            &[plan_with_bytes(1, 0, &batch_1, 10), plan_with_bytes(2, 0, &batch_2, 20)],
            SortedBatchMergerConfig {
                row_group_size: 1,
                read_parallelism: 2,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
            },
        )
        .expect("merger")
        .with_observer(Arc::clone(&observer) as Arc<dyn RowGroupsMergerObserver>);
        let mut stream = merger.into_stream();

        let _ = stream.try_next().await.expect("first batch").expect("first batch");
        assert!(observer.opened_calls() >= 1);
        assert!(observer.closed_calls() <= observer.opened_calls());

        let _remaining = stream.try_collect::<Vec<_>>().await.expect("remaining batches");
        assert_eq!(observer.opened_calls(), 2);
        assert_eq!(observer.closed_calls(), 2);
        assert_eq!(observer.open_row_groups(), 0);
        assert_eq!(observer.open_row_group_bytes(), 0);
    }

    #[tokio::test]
    async fn merger_observer_resets_open_counts_on_cancel() {
        let batch_1 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(60), 1),
            (Some("acc-1"), Some("svc-1"), Some(40), 3),
            (Some("acc-1"), Some("svc-1"), Some(20), 5),
        ]);
        let batch_2 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(50), 2),
            (Some("acc-1"), Some("svc-1"), Some(30), 4),
            (Some("acc-1"), Some("svc-1"), Some(10), 6),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch_1.clone()]), ((2, 0), vec![batch_2.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let observer = Arc::new(FakeObserver::default());
        let cancel_token = CancellationToken::new();
        let merger = RowGroupsMerger::new(
            queue_reader,
            &[plan_with_bytes(1, 0, &batch_1, 10), plan_with_bytes(2, 0, &batch_2, 20)],
            SortedBatchMergerConfig {
                row_group_size: 2,
                read_parallelism: 2,
                topic: "logs".to_string(),
                cancel_token: cancel_token.clone(),
            },
        )
        .expect("merger")
        .with_observer(Arc::clone(&observer) as Arc<dyn RowGroupsMergerObserver>);

        let mut stream = merger.into_stream();
        let _ = stream.try_next().await.expect("first batch").expect("first batch");
        assert!(observer.open_row_groups() > 0);
        cancel_token.cancel();
        let _ = stream.try_next().await.expect_err("cancelled");
        drop(stream);

        assert_eq!(observer.open_row_groups(), 0);
        assert_eq!(observer.open_row_group_bytes(), 0);
    }

    #[tokio::test]
    async fn merger_observer_keeps_balanced_counts_when_bootstrap_fails() {
        let batch = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(40), 1)]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(40), 2)]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch.clone()]), ((2, 0), vec![batch_2.clone()])]),
            open_delays: HashMap::new(),
            error_after_n_reads: Some(2),
            ..FakeQueueReader::default()
        });
        let observer = Arc::new(FakeObserver::default());
        let merger = RowGroupsMerger::new(
            queue_reader,
            &[plan_with_bytes(1, 0, &batch, 10), plan_with_bytes(2, 0, &batch_2, 20)],
            SortedBatchMergerConfig {
                row_group_size: 1,
                read_parallelism: 1,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
            },
        )
        .expect("merger")
        .with_observer(Arc::clone(&observer) as Arc<dyn RowGroupsMergerObserver>);

        let mut stream = merger.into_stream();
        let err = stream.try_next().await.expect_err("bootstrap must fail");
        assert!(matches!(err, IngestError::ShiftQueueRead(_)));
        drop(stream);

        assert_eq!(observer.opened_calls(), 1);
        assert_eq!(observer.closed_calls(), 1);
        assert_eq!(observer.open_row_groups(), 0);
        assert_eq!(observer.open_row_group_bytes(), 0);
    }

    #[tokio::test]
    async fn merger_output_is_unchanged_with_observer_enabled() {
        let batch_1 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(40), 1),
            (Some("acc-1"), Some("svc-1"), Some(20), 3),
        ]);
        let batch_2 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 2),
            (Some("acc-1"), Some("svc-1"), Some(10), 4),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch_1.clone()]), ((2, 0), vec![batch_2.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let expected = merged_values(
            Arc::clone(&queue_reader),
            vec![plan(1, 0, &batch_1), plan(2, 0, &batch_2)],
        )
        .await;
        let actual = merged_values_with_observer(
            queue_reader,
            vec![plan(1, 0, &batch_1), plan(2, 0, &batch_2)],
            Arc::new(FakeObserver::default()) as Arc<dyn RowGroupsMergerObserver>,
        )
        .await;

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn merger_observer_records_row_group_lifetime_for_multi_stream_merge() {
        let batch_1 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(40), 1),
            (Some("acc-1"), Some("svc-1"), Some(20), 3),
        ]);
        let batch_2 = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 2),
            (Some("acc-1"), Some("svc-1"), Some(10), 4),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch_1.clone()]), ((2, 0), vec![batch_2.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let observer = Arc::new(FakeObserver::default());
        let _values = merged_values_with_observer(
            queue_reader,
            vec![plan(1, 0, &batch_1), plan(2, 0, &batch_2)],
            Arc::clone(&observer) as Arc<dyn RowGroupsMergerObserver>,
        )
        .await;

        assert_eq!(observer.lifetime_calls(), 2);
        assert_eq!(observer.lifetimes().len(), 2);
    }

    #[tokio::test]
    async fn merger_observer_records_row_group_lifetime_for_single_stream_merge() {
        let batch = logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 1),
            (Some("acc-1"), Some("svc-1"), Some(20), 2),
            (Some("acc-1"), Some("svc-1"), Some(10), 3),
        ]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch.clone()])]),
            open_delays: HashMap::new(),
            ..FakeQueueReader::default()
        });
        let observer = Arc::new(FakeObserver::default());
        let merger = RowGroupsMerger::new(
            queue_reader,
            &[plan(1, 0, &batch)],
            SortedBatchMergerConfig {
                row_group_size: 1,
                read_parallelism: 1,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
            },
        )
        .expect("merger")
        .with_observer(Arc::clone(&observer) as Arc<dyn RowGroupsMergerObserver>);

        let _batches = merger.into_stream().try_collect::<Vec<_>>().await.expect("merged");

        assert_eq!(observer.lifetime_calls(), 1);
        assert_eq!(observer.lifetimes().len(), 1);
    }

    #[test]
    fn cluster_builder_forms_overlap_chain() {
        let segments = vec![
            plan_with_range(
                1,
                0,
                key(Some("acc-1"), Some("svc-a"), Some(50)),
                key(Some("acc-1"), Some("svc-a"), Some(30)),
            ),
            plan_with_range(
                2,
                0,
                key(Some("acc-1"), Some("svc-a"), Some(40)),
                key(Some("acc-1"), Some("svc-a"), Some(20)),
            ),
            plan_with_range(
                3,
                0,
                key(Some("acc-1"), Some("svc-a"), Some(25)),
                key(Some("acc-1"), Some("svc-a"), Some(10)),
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
                key(Some("acc-1"), Some("svc-a"), Some(50)),
                key(Some("acc-1"), Some("svc-a"), Some(40)),
            ),
            plan_with_range(
                2,
                0,
                key(Some("acc-1"), Some("svc-b"), Some(30)),
                key(Some("acc-1"), Some("svc-b"), Some(20)),
            ),
        ];

        assert_eq!(cluster_sizes_from_metadata(segments), vec![1, 1]);
    }

    #[test]
    fn cluster_builder_tracks_data_driven_width_pattern() {
        let mut segments = Vec::new();

        for idx in 0..4 {
            let start = 100 - i64::from(idx);
            segments.push(plan_with_range(
                u64::try_from(idx + 1).expect("segment offset"),
                0,
                key(Some("acc-1"), Some("svc-a"), Some(start)),
                key(Some("acc-1"), Some("svc-a"), Some(80)),
            ));
        }
        for idx in 0..2 {
            let start = 70 - i64::from(idx);
            segments.push(plan_with_range(
                u64::try_from(idx + 10).expect("segment offset"),
                0,
                key(Some("acc-1"), Some("svc-b"), Some(start)),
                key(Some("acc-1"), Some("svc-b"), Some(60)),
            ));
        }
        for idx in 0..10 {
            let start = 50 - i64::from(idx);
            segments.push(plan_with_range(
                u64::try_from(idx + 20).expect("segment offset"),
                0,
                key(Some("acc-2"), Some("svc-c"), Some(start)),
                key(Some("acc-2"), Some("svc-c"), Some(30)),
            ));
        }

        assert_eq!(cluster_sizes_from_metadata(segments), vec![4, 2, 10]);
    }

    #[tokio::test]
    async fn merger_rejects_incompatible_boundary_key_structure() {
        let plan = PlannedRowGroupRead::from_segments(&[
            plan_with_range(
                1,
                0,
                key(Some("acc-1"), Some("svc-1"), Some(20)),
                key(Some("acc-1"), Some("svc-1"), Some(10)),
            ),
            plan_with_range(
                2,
                0,
                RowGroupBoundaryKey {
                    components: vec![
                        RowGroupBoundaryComponent::string(Some("acc-2".to_string()), false, true),
                        RowGroupBoundaryComponent::string(Some("svc-2".to_string()), false, true),
                    ],
                },
                RowGroupBoundaryKey {
                    components: vec![
                        RowGroupBoundaryComponent::string(Some("acc-2".to_string()), false, true),
                        RowGroupBoundaryComponent::string(Some("svc-2".to_string()), false, true),
                    ],
                },
            ),
        ]);

        let Err(err) = RowGroupsMerger::new_with_plan(
            Arc::new(FakeQueueReader {
                batches: HashMap::new(),
                open_delays: HashMap::new(),
                ..FakeQueueReader::default()
            }),
            plan,
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
            Arc::new(NoopRowGroupsMergerObserver),
        ) else {
            panic!("incompatible boundary keys must be rejected");
        };

        assert!(err.to_string().contains("component count differs"));
    }

    #[tokio::test]
    async fn merger_rejects_duplicate_row_group_positions_in_read_plan() {
        let plan = PlannedRowGroupRead::from_segments(&[
            plan_with_range(
                42,
                0,
                key(Some("acc-1"), Some("svc-1"), Some(50)),
                key(Some("acc-1"), Some("svc-1"), Some(40)),
            ),
            plan_with_range(
                42,
                0,
                key(Some("acc-1"), Some("svc-1"), Some(30)),
                key(Some("acc-1"), Some("svc-1"), Some(20)),
            ),
        ]);

        let Err(err) = RowGroupsMerger::new_with_plan(
            Arc::new(FakeQueueReader {
                batches: HashMap::new(),
                open_delays: HashMap::new(),
                ..FakeQueueReader::default()
            }),
            plan,
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                topic: "logs".to_string(),
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
            Arc::new(NoopRowGroupsMergerObserver),
        ) else {
            panic!("duplicate row groups must be rejected");
        };

        assert!(
            err.to_string()
                .contains("duplicate row group in read plan: segment_offset=42, row_group_idx=0")
        );
    }

    #[test]
    fn cluster_builder_keeps_distinct_row_groups_with_equal_min_key() {
        let same_min = key(Some("acc-1"), Some("svc-a"), Some(50));
        let segments = vec![
            plan_with_range(7, 0, same_min.clone(), key(Some("acc-1"), Some("svc-a"), Some(40))),
            plan_with_range(7, 1, same_min, key(Some("acc-1"), Some("svc-a"), Some(30))),
        ];

        assert_eq!(cluster_sizes_from_metadata(segments), vec![2]);
    }
}
