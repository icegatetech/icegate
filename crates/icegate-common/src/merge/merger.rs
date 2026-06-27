//! The k-way sort-merge engine.
//!
//! [`RowGroupsMerger`] opens an already-sorted set of inputs through a pluggable
//! [`MergeSource`](super::MergeSource), clusters the inputs whose sort-key ranges
//! overlap, and streams one globally sorted output with a stable equal-key
//! tie-break on [`MergePosition`](super::MergePosition). The shared contracts it
//! builds on — [`MergeInput`](super::MergeInput), the source and observer traits
//! — live in the parent [`super`](crate::merge) module; this file holds only the
//! engine and its private machinery so the module entry point stays small.

use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, VecDeque},
    sync::Arc,
};

use arrow::{compute::interleave_record_batch, datatypes::SchemaRef, record_batch::RecordBatch};
use futures::{StreamExt, TryStreamExt};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::{MergeInput, MergePosition, MergeSource, NoopRowGroupsMergerObserver, RowGroupsMergerObserver};
use crate::{
    error::{CommonError, Result},
    iceberg_write::CommonRecordBatchStream,
    merge::sort_key::{RowGroupBoundaryRange, SortColumnCache, SortColumnsDescriptor},
};

/// Check the cancellation token every this many merged rows to keep
/// cancellation responsive without paying the cost on every single row.
const MERGE_CANCEL_CHECK_INTERVAL_ROWS: usize = 1024;

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
    /// Absolute row position inside one input across all streamed batches.
    input_row_idx: u64,
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

/// Cluster is a set of inputs that are involved in sorting together at a given time.
struct ActiveClusterState {
    source: PlannedInputRead,
    stream: Option<CommonRecordBatchStream>,
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
            .ok_or_else(|| CommonError::Write("left merge cursor stream is missing".to_string()))?;
        let right_state = self
            .streams
            .get(right.stream_idx)
            .ok_or_else(|| CommonError::Write("right merge cursor stream is missing".to_string()))?;
        let left_cache = left_state
            .cache
            .as_ref()
            .ok_or_else(|| CommonError::Write("left merge cursor cache is missing".to_string()))?;
        let right_cache = right_state
            .cache
            .as_ref()
            .ok_or_else(|| CommonError::Write("right merge cursor cache is missing".to_string()))?;

        let ordering = left_cache.compare_row(left.row_idx, right_cache, right.row_idx)?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }

        // Equal-key rows must keep their original source order inside the output:
        // the source-assigned `position` first, then the row position inside that
        // input. Do not use internal merge mechanics like stream_idx as a business
        // tie-breaker.
        let ordering = left_state.source.position.cmp(&right_state.source.position);
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }

        Ok(left.input_row_idx.cmp(&right.input_row_idx))
    }
}

/// Read plan entry normalized for the merger.
///
/// The caller produces a flat list of [`MergeInput`]s, but the merger
/// repeatedly moves them between pending and active states. This type stores
/// the minimal immutable identity the merger needs for that lifecycle: the
/// opaque tie-break `position`, the validated boundary range, and `plan_idx`
/// preserving the original input order when a stable tie-breaker is required.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedInputRead {
    plan_idx: usize,
    position: MergePosition,
    bytes: u64,
    boundary_range: RowGroupBoundaryRange,
}

impl PlannedInputRead {
    fn from_inputs(inputs: Vec<MergeInput>) -> Vec<Self> {
        inputs
            .into_iter()
            .enumerate()
            .map(|(plan_idx, input)| Self {
                plan_idx,
                position: input.position,
                bytes: input.bytes,
                boundary_range: input.boundary_range,
            })
            .collect()
    }
}

fn validate_boundary_ranges(read_plan: &[PlannedInputRead]) -> Result<()> {
    let Some(reference) = read_plan.first().map(|input| &input.boundary_range.min_key) else {
        return Ok(());
    };

    for input in read_plan {
        input.boundary_range.validate()?;
        input.boundary_range.min_key.validate_compatible_structure(reference)?;
        input.boundary_range.max_key.validate_compatible_structure(reference)?;
    }

    Ok(())
}

fn validate_unique_input_positions(read_plan: &[PlannedInputRead]) -> Result<()> {
    let mut seen_positions: HashMap<MergePosition, usize> = HashMap::new();
    for input in read_plan {
        if let Some(first_plan_idx) = seen_positions.insert(input.position, input.plan_idx) {
            return Err(CommonError::Write(format!(
                "duplicate input position in merge plan: position={}, first_plan_idx={}, duplicate_plan_idx={}",
                input.position, first_plan_idx, input.plan_idx
            )));
        }
    }
    Ok(())
}

/// Wrapper for pending inputs stored in `BTreeSet`.
///
/// [`PlannedInputRead`] intentionally does not define a global `Ord`, because
/// its natural ordering depends on the use site. The merger needs a specific
/// pending-queue order: first by the lower merge-key boundary, then by
/// `position`, then by `plan_idx`. This wrapper keeps that ordering local to
/// the pending set and avoids leaking it into the base planning type.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingInputWithOrder {
    source: PlannedInputRead,
}

impl PendingInputWithOrder {
    const fn new(source: PlannedInputRead) -> Self {
        Self { source }
    }
}

impl Ord for PendingInputWithOrder {
    fn cmp(&self, other: &Self) -> Ordering {
        self.source
            .boundary_range
            .min_key
            .compare(&other.source.boundary_range.min_key)
            .then_with(|| self.source.position.cmp(&other.source.position))
            .then_with(|| self.source.plan_idx.cmp(&other.source.plan_idx))
    }
}

impl PartialOrd for PendingInputWithOrder {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Cluster is a set of inputs that are involved in sorting together at a given time.
#[derive(Debug, Clone, Copy, Default)]
struct ActiveClusterStats {
    inputs: usize,
    input_bytes: u64,
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
    /// Limit for opening inputs in parallel while building a cluster.
    pub read_parallelism: usize,
    /// Shared cancellation token for all source reads.
    pub cancel_token: CancellationToken,
    /// Sort descriptor used to compare rows across inputs.
    pub sort_descriptor: &'static SortColumnsDescriptor,
}

/// Streams sorted inputs from a [`MergeSource`] and merges them into one sorted
/// output stream.
///
/// The merger only keeps one overlap cluster in memory at a time: it opens the
/// inputs whose key ranges intersect, performs k-way merge across them, and
/// then moves to the next cluster.
pub struct RowGroupsMerger<S: MergeSource + ?Sized + 'static> {
    /// Immutable read/output settings. The sort descriptor lives inside
    /// `config.sort_descriptor`; there is no separate cached copy on the
    /// merger itself — every read goes through the config to keep a single
    /// source of truth.
    config: SortedBatchMergerConfig,
    /// Schema shared by all opened batches. Filled from the first batch.
    schema: SchemaRef,
    /// Source used to open inputs as Arrow streams.
    source: Arc<S>,
    /// Optional observer for merger lifecycle and timings.
    observer: Arc<dyn RowGroupsMergerObserver>,
    /// Inputs that are not opened yet, ordered by their lower boundary.
    pending_inputs: BTreeSet<PendingInputWithOrder>,
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

impl<S> RowGroupsMerger<S>
where
    S: MergeSource + 'static + ?Sized,
{
    /// Build merger state from a flat list of already-sorted inputs.
    ///
    /// At this point no input data is opened yet. The method only validates the
    /// plan and initializes the pending set ordered by lower boundary.
    ///
    /// # Errors
    ///
    /// Returns an error if `row_group_size` or `read_parallelism` is zero, if
    /// the boundary ranges are invalid or structurally incompatible, or if two
    /// inputs share the same `position`.
    pub fn new(source: Arc<S>, inputs: Vec<MergeInput>, config: SortedBatchMergerConfig) -> Result<Self> {
        let read_plan = PlannedInputRead::from_inputs(inputs);
        Self::new_with_plan(source, read_plan, config, Arc::new(NoopRowGroupsMergerObserver))
    }

    /// Attach merger lifecycle observer.
    #[must_use]
    pub fn with_observer(mut self, observer: Arc<dyn RowGroupsMergerObserver>) -> Self {
        self.observer = observer;
        self
    }

    /// Create a merger from an already validated read plan.
    fn new_with_plan(
        source: Arc<S>,
        read_plan: Vec<PlannedInputRead>,
        config: SortedBatchMergerConfig,
        observer: Arc<dyn RowGroupsMergerObserver>,
    ) -> Result<Self> {
        if config.row_group_size == 0 {
            return Err(CommonError::Config(
                "row_group_size must be greater than zero".to_string(),
            ));
        }
        if config.read_parallelism == 0 {
            return Err(CommonError::Config(
                "read_parallelism must be greater than zero".to_string(),
            ));
        }
        validate_boundary_ranges(&read_plan)?;
        validate_unique_input_positions(&read_plan)?;

        Ok(Self {
            config,
            schema: SchemaRef::new(arrow::datatypes::Schema::empty()),
            source,
            observer,
            pending_inputs: read_plan.into_iter().map(PendingInputWithOrder::new).collect(),
            active_cluster_streams: Vec::new(),
            heap: MergeHeap::new(),
            active_cluster: None,
            prefetched_batches: VecDeque::new(),
            merge_source_batches: Vec::new(),
            merge_source_batch_by_stream: Vec::new(),
            merge_interleave_indices: Vec::new(),
        })
    }

    /// Preload the first merged output batch so source read failures happen
    /// before the storage write pipeline is started.
    ///
    /// # Errors
    ///
    /// Returns an error if opening or reading the first cluster fails.
    pub async fn prefetch_first_group(&mut self) -> Result<()> {
        if self.prefetched_batches.is_empty() {
            if let Some(batch) = self.next_group_inner().await? {
                self.prefetched_batches.push_back(batch);
            }
        }
        Ok(())
    }

    /// Consume the merger and return the merged output as one sorted stream.
    #[must_use]
    pub fn into_stream(self) -> CommonRecordBatchStream {
        futures::stream::try_unfold(self, |mut merger| async move {
            (merger.next_group().await?).map_or_else(|| Ok(None), |batch| Ok(Some((batch, merger))))
        })
        .boxed()
    }

    /// Return the next merged batch, serving a prefetched batch first when
    /// `prefetch_first_group` already opened the pipeline.
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
                    .ok_or_else(|| CommonError::Write("merge cursor stream index out of bounds".to_string()))?;
                let current_batch = stream_state
                    .current_batch
                    .as_ref()
                    .ok_or_else(|| CommonError::Write("merge cursor points to missing input batch".to_string()))?;
                if stream_state.batch_generation != cursor.batch_generation {
                    return Err(CommonError::Write("stale merge cursor generation".to_string()));
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
                .ok_or_else(|| CommonError::Write("merge cursor stream index out of bounds".to_string()))?;
            if stream_state.batch_generation != cursor.batch_generation {
                return Err(CommonError::Write("stale merge cursor generation".to_string()));
            }
            let current_batch = stream_state
                .current_batch
                .as_ref()
                .ok_or_else(|| CommonError::Write("merge cursor points to missing input batch".to_string()))?;
            let remaining_rows = current_batch
                .num_rows()
                .checked_sub(cursor.row_idx)
                .ok_or_else(|| CommonError::Write("merge cursor row index out of bounds".to_string()))?;
            let rows_to_emit = remaining_rows.min(row_group_size);
            (
                current_batch.slice(cursor.row_idx, rows_to_emit),
                rows_to_emit,
                current_batch.num_rows(),
            )
        };

        if rows_to_emit == 0 {
            return Err(CommonError::Write(
                "single-source merge cursor produced an empty output slice".to_string(),
            ));
        }
        let next_row_idx = cursor
            .row_idx
            .checked_add(rows_to_emit)
            .ok_or_else(|| CommonError::Write("merge row index overflow".to_string()))?;
        if next_row_idx < current_batch_rows {
            let rows_to_emit = u64::try_from(rows_to_emit)
                .map_err(|_| CommonError::Write("emitted row count exceeds u64".to_string()))?;
            let next_input_row_idx = cursor
                .input_row_idx
                .checked_add(rows_to_emit)
                .ok_or_else(|| CommonError::Write("input row index overflow".to_string()))?;
            let comparator = RowComparator::new(&self.active_cluster_streams);
            self.heap.push(
                MergeCursor {
                    stream_idx: cursor.stream_idx,
                    batch_generation: cursor.batch_generation,
                    row_idx: next_row_idx,
                    input_row_idx: next_input_row_idx,
                },
                &comparator,
            )?;
        } else {
            let rows_to_emit = u64::try_from(rows_to_emit)
                .map_err(|_| CommonError::Write("emitted row count exceeds u64".to_string()))?;
            let last_input_row_idx = cursor
                .input_row_idx
                .checked_add(rows_to_emit)
                .and_then(|idx| idx.checked_sub(1))
                .ok_or_else(|| CommonError::Write("input row index overflow".to_string()))?;
            let last_cursor = MergeCursor {
                stream_idx: cursor.stream_idx,
                batch_generation: cursor.batch_generation,
                row_idx: next_row_idx - 1,
                input_row_idx: last_input_row_idx,
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
        interleave_record_batch(&source_refs, &self.merge_interleave_indices)
            .map_err(|err| CommonError::Write(format!("failed to interleave merged batch: {err}")))
    }

    fn contiguous_single_source_output(&self) -> Result<Option<RecordBatch>> {
        let Some(&(source_idx, start_row_idx)) = self.merge_interleave_indices.first() else {
            return Ok(None);
        };
        let source_batch = self
            .merge_source_batches
            .get(source_idx)
            .ok_or_else(|| CommonError::Write("interleave source batch index out of bounds".to_string()))?;

        for (offset, &(candidate_source_idx, row_idx)) in self.merge_interleave_indices.iter().enumerate() {
            let expected_row_idx = start_row_idx
                .checked_add(offset)
                .ok_or_else(|| CommonError::Write("contiguous source row index overflow".to_string()))?;
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
            Err(CommonError::Write("merge cancelled".to_string()))
        } else {
            Ok(())
        }
    }

    /// Open the next non-empty overlap cluster from the pending set.
    ///
    /// Pending inputs are partitioned by boundary-range intersection. All
    /// inputs that can overlap in sort order must be opened together and
    /// merged through one heap; disjoint groups can be processed later.
    async fn open_next_overlap_cluster(&mut self) -> Result<bool> {
        // TODO(high): Consider replacing eager overlap-cluster bootstrap with a lazy safe-to-emit
        // merge strategy. The current implementation opens every input in the
        // transitive overlap cluster before emitting the first output row, which can
        // increase peak memory for long overlap chains.
        //
        // A lazy version must not simply cap opened inputs by read_parallelism:
        // that would break global ordering. It must only emit a heap minimum after all
        // pending inputs with boundary_range.min_key <= current heap minimum key
        // have been opened and added to the heap. If that barrier includes the whole
        // cluster, opening the whole cluster is required for correctness.

        while !self.pending_inputs.is_empty() {
            // Inputs with intersecting boundary ranges must be merged together.
            let (cluster_sources, mut cluster_stats) = self.pop_next_cluster_sources()?;
            let open_parallelism = cluster_sources.len().min(self.config.read_parallelism);
            cluster_stats.open_parallelism = open_parallelism;
            debug!(
                cluster_inputs = cluster_stats.inputs,
                cluster_input_bytes = cluster_stats.input_bytes,
                cluster_frontier_extensions = cluster_stats.frontier_extensions,
                cluster_open_parallelism = cluster_stats.open_parallelism,
                "merger cluster started"
            );
            let source = Arc::clone(&self.source);
            let cancel_token = self.config.cancel_token.clone();
            let mut opened_stream = futures::stream::iter(cluster_sources.into_iter().map(|planned| {
                let source = Arc::clone(&source);
                let cancel_token = cancel_token.clone();
                async move { open_source(source.as_ref(), planned, &cancel_token).await }
            }))
            .buffer_unordered(open_parallelism);

            while let Some(opened) = opened_stream.try_next().await? {
                match opened {
                    Some(opened) => {
                        self.push_opened_source(opened)?;
                        cluster_stats.active_streams = cluster_stats
                            .active_streams
                            .checked_add(1)
                            .ok_or_else(|| CommonError::Write("active cluster stream counter overflow".to_string()))?;
                    }
                    None => {
                        cluster_stats.empty_streams = cluster_stats
                            .empty_streams
                            .checked_add(1)
                            .ok_or_else(|| CommonError::Write("empty cluster stream counter overflow".to_string()))?;
                    }
                }
            }

            debug!(
                cluster_inputs = cluster_stats.inputs,
                cluster_input_bytes = cluster_stats.input_bytes,
                cluster_frontier_extensions = cluster_stats.frontier_extensions,
                cluster_open_parallelism = cluster_stats.open_parallelism,
                cluster_active_streams = cluster_stats.active_streams,
                cluster_empty_streams = cluster_stats.empty_streams,
                "merger cluster bootstrap completed"
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
    /// The first pending input starts the cluster. Then the cluster frontier
    /// is extended while the next input's lower bound is still inside the
    /// current frontier.
    fn pop_next_cluster_sources(&mut self) -> Result<(Vec<PlannedInputRead>, ActiveClusterStats)> {
        let first = self.pop_pending_source().ok_or_else(|| {
            CommonError::Write("pending inputs disappeared while building overlap cluster".to_string())
        })?;

        // Extend the cluster while the next input's lower bound still overlaps the frontier.
        let mut cluster_sources = Vec::new();
        let mut frontier = first.boundary_range.max_key.clone();
        let mut frontier_extensions = 0usize;
        cluster_sources.push(first);

        while let Some(next_pending) = self.peek_pending_source() {
            if next_pending.boundary_range.min_key.compare(&frontier) == Ordering::Greater {
                break;
            }
            let next = self.pop_pending_source().ok_or_else(|| {
                CommonError::Write("pending inputs disappeared while extending overlap cluster".to_string())
            })?;
            if next.boundary_range.max_key.compare(&frontier) == Ordering::Greater {
                frontier = next.boundary_range.max_key.clone();
                frontier_extensions = frontier_extensions
                    .checked_add(1)
                    .ok_or_else(|| CommonError::Write("cluster frontier extensions overflow".to_string()))?;
            }
            cluster_sources.push(next);
        }

        let cluster_input_bytes = cluster_sources.iter().try_fold(0_u64, |total, source| {
            total
                .checked_add(source.bytes)
                .ok_or_else(|| CommonError::Write("cluster input bytes overflow".to_string()))
        })?;
        let stats = ActiveClusterStats {
            inputs: cluster_sources.len(),
            input_bytes: cluster_input_bytes,
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
                self.observer
                    .on_input_closed(stream_state.source.position, stream_state.source.bytes);
                stream_state.open_in_observer = false;
            }
        }
        if let Some(stats) = self.active_cluster.take() {
            debug!(
                cluster_inputs = stats.inputs,
                cluster_input_bytes = stats.input_bytes,
                cluster_frontier_extensions = stats.frontier_extensions,
                cluster_open_parallelism = stats.open_parallelism,
                cluster_active_streams = stats.active_streams,
                cluster_empty_streams = stats.empty_streams,
                "merger cluster completed"
            );
        }
        self.active_cluster_streams.clear();
        self.heap = MergeHeap::new();
    }

    /// Inspect the smallest pending input without removing it.
    fn peek_pending_source(&self) -> Option<&PlannedInputRead> {
        self.pending_inputs.iter().next().map(|entry| &entry.source)
    }

    /// Remove the smallest pending input according to pending-set ordering.
    fn pop_pending_source(&mut self) -> Option<PlannedInputRead> {
        self.pending_inputs.pop_first().map(|entry| entry.source)
    }

    /// Register one opened source in the active cluster and seed the heap
    /// with the first row from its first non-empty batch.
    fn push_opened_source(&mut self, opened: OpenedSource) -> Result<()> {
        let opened_bytes = opened.source.bytes;
        if self.schema.fields().is_empty() {
            self.schema = opened.batch.schema();
        } else if opened.batch.schema() != self.schema {
            return Err(CommonError::Write(format!(
                "schema mismatch in merge input at position {}",
                opened.source.position
            )));
        }

        let cache = SortColumnCache::try_new(&opened.batch, self.config.sort_descriptor, "merge input")?;
        let stream_idx = self.active_cluster_streams.len();
        self.active_cluster_streams.push(ActiveClusterState {
            source: opened.source,
            stream: Some(opened.stream),
            current_batch: Some(opened.batch),
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
                input_row_idx: 0,
            },
            &comparator,
        )?;
        // Set the flag first so Drop can always balance opened/closed callbacks
        // even if observer implementation panics.
        let stream_state = self
            .active_cluster_streams
            .get_mut(stream_idx)
            .ok_or_else(|| CommonError::Write("cannot access merger stream state after push".to_string()))?;
        stream_state.open_in_observer = true;
        self.observer.on_input_opened(stream_state.source.position, opened_bytes);
        Ok(())
    }

    /// Advance one merge cursor within its source.
    ///
    /// If the current Arrow batch still has rows, the cursor moves locally.
    /// Otherwise the method pulls the next non-empty batch from the same input,
    /// refreshes sort caches, and returns a cursor pointing at the new batch
    /// start. When the source is exhausted, it returns `None`.
    async fn advance_cursor(&mut self, cursor: MergeCursor) -> Result<Option<MergeCursor>> {
        let stream_state = self
            .active_cluster_streams
            .get_mut(cursor.stream_idx)
            .ok_or_else(|| CommonError::Write("merge cursor stream index out of bounds".to_string()))?;
        let current_batch = stream_state
            .current_batch
            .as_ref()
            .ok_or_else(|| CommonError::Write("merge cursor points to missing input batch".to_string()))?;
        let next_row_idx = cursor
            .row_idx
            .checked_add(1)
            .ok_or_else(|| CommonError::Write("merge row index overflow".to_string()))?;

        if next_row_idx < current_batch.num_rows() {
            let next_input_row_idx = cursor
                .input_row_idx
                .checked_add(1)
                .ok_or_else(|| CommonError::Write("input row index overflow".to_string()))?;
            return Ok(Some(MergeCursor {
                stream_idx: cursor.stream_idx,
                batch_generation: cursor.batch_generation,
                row_idx: next_row_idx,
                input_row_idx: next_input_row_idx,
            }));
        }

        let consumed_rows = u64::try_from(current_batch.num_rows())
            .map_err(|_| CommonError::Write("batch row count exceeds u64".to_string()))?;
        stream_state.rows_consumed_total = stream_state
            .rows_consumed_total
            .checked_add(consumed_rows)
            .ok_or_else(|| CommonError::Write("rows consumed overflow".to_string()))?;

        let Some(stream) = stream_state.stream.as_mut() else {
            return Err(CommonError::Write(format!(
                "missing stream handle for merge input at position {}",
                stream_state.source.position
            )));
        };
        let Some(next_batch) = next_non_empty_batch(stream).await? else {
            if stream_state.open_in_observer {
                self.observer
                    .on_input_closed(stream_state.source.position, stream_state.source.bytes);
                stream_state.open_in_observer = false;
            }
            stream_state.current_batch = None;
            stream_state.cache = None;
            stream_state.stream = None;
            return Ok(None);
        };
        if next_batch.schema() != self.schema {
            return Err(CommonError::Write(format!(
                "schema mismatch while advancing merge input at position {}",
                stream_state.source.position
            )));
        }

        stream_state.current_batch_start_row = stream_state.rows_consumed_total;
        stream_state.batch_generation = stream_state
            .batch_generation
            .checked_add(1)
            .ok_or_else(|| CommonError::Write("batch generation overflow".to_string()))?;
        stream_state.cache = Some(SortColumnCache::try_new(
            &next_batch,
            self.config.sort_descriptor,
            "merge input",
        )?);
        stream_state.current_batch = Some(next_batch);

        Ok(Some(MergeCursor {
            stream_idx: cursor.stream_idx,
            batch_generation: stream_state.batch_generation,
            row_idx: 0,
            input_row_idx: stream_state.current_batch_start_row,
        }))
    }
}

impl<S> Drop for RowGroupsMerger<S>
where
    S: MergeSource + ?Sized + 'static,
{
    fn drop(&mut self) {
        self.complete_active_cluster();
    }
}

struct OpenedSource {
    source: PlannedInputRead,
    stream: CommonRecordBatchStream,
    batch: RecordBatch,
}

async fn open_source<S>(
    source: &S,
    planned: PlannedInputRead,
    cancel_token: &CancellationToken,
) -> Result<Option<OpenedSource>>
where
    S: MergeSource + ?Sized,
{
    let input = MergeInput {
        position: planned.position,
        bytes: planned.bytes,
        boundary_range: planned.boundary_range.clone(),
    };
    let mut stream = source.open(&input, cancel_token).await?;
    let Some(batch) = next_non_empty_batch(&mut stream).await? else {
        return Ok(None);
    };
    Ok(Some(OpenedSource {
        source: planned,
        stream,
        batch,
    }))
}

async fn next_non_empty_batch(stream: &mut CommonRecordBatchStream) -> Result<Option<RecordBatch>> {
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
        MergeInput, MergePosition, NoopRowGroupsMergerObserver, PlannedInputRead, RowGroupsMerger,
        RowGroupsMergerObserver, SortedBatchMergerConfig,
    };
    use crate::{
        error::{CommonError, Result},
        iceberg_write::CommonRecordBatchStream,
        merge::sort_key::{
            RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, RowGroupBoundaryValue,
            SortColumnCache, SortColumnsDescriptor,
        },
    };

    /// Pack a WAL `(segment_offset, row_group_idx)` pair into the opaque merge
    /// `position` exactly as the ingest WAL adapter does:
    /// `(segment_offset as u128) << 32 | (row_group_idx as u128)`.
    ///
    /// These tests reproduce the old `(segment_offset, row_group_idx)`
    /// lexicographic tie-break order, so every input position is built through
    /// this packing and the equal-key ordering assertions stay verbatim.
    const fn position(segment_offset: u64, row_group_idx: usize) -> MergePosition {
        MergePosition::new(((segment_offset as u128) << 32) | (row_group_idx as u128))
    }

    /// Test source keyed by packed `position`, replacing the old
    /// `FakeQueueReader`. `open(input)` returns the batches registered for
    /// `input.position`.
    #[derive(Debug, Default)]
    struct FakeMergeSource {
        batches: HashMap<MergePosition, Vec<RecordBatch>>,
        open_delays: HashMap<MergePosition, Duration>,
        error_after_n_reads: Option<usize>,
        open_calls: AtomicUsize,
    }

    #[async_trait]
    impl super::MergeSource for FakeMergeSource {
        async fn open(&self, input: &MergeInput, _cancel: &CancellationToken) -> Result<CommonRecordBatchStream> {
            let open_call = self.open_calls.fetch_add(1, Ordering::SeqCst) + 1;
            if self.error_after_n_reads.is_some_and(|limit| open_call >= limit) {
                return Err(CommonError::Write("injected open error in tests".to_string()));
            }
            if let Some(delay) = self.open_delays.get(&input.position) {
                sleep(*delay).await;
            }
            let batches = self.batches.get(&input.position).cloned().unwrap_or_default();
            Ok(Box::pin(futures::stream::iter(batches.into_iter().map(Ok))))
        }
    }

    #[derive(Default)]
    struct FakeObserver {
        open_inputs: AtomicI64,
        open_input_bytes: AtomicI64,
        opened_calls: AtomicUsize,
        closed_calls: AtomicUsize,
        opened_at: Mutex<HashMap<MergePosition, std::time::Instant>>,
    }

    impl FakeObserver {
        fn open_inputs(&self) -> i64 {
            self.open_inputs.load(Ordering::SeqCst)
        }

        fn open_input_bytes(&self) -> i64 {
            self.open_input_bytes.load(Ordering::SeqCst)
        }

        fn opened_calls(&self) -> usize {
            self.opened_calls.load(Ordering::SeqCst)
        }

        fn closed_calls(&self) -> usize {
            self.closed_calls.load(Ordering::SeqCst)
        }
    }

    impl RowGroupsMergerObserver for FakeObserver {
        fn on_input_opened(&self, position: MergePosition, bytes: u64) {
            let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
            self.opened_calls.fetch_add(1, Ordering::SeqCst);
            self.open_inputs.fetch_add(1, Ordering::SeqCst);
            self.open_input_bytes.fetch_add(bytes, Ordering::SeqCst);
            self.opened_at
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(position, std::time::Instant::now());
        }

        fn on_input_closed(&self, position: MergePosition, bytes: u64) {
            let bytes = i64::try_from(bytes).unwrap_or(i64::MAX);
            self.closed_calls.fetch_add(1, Ordering::SeqCst);
            self.open_inputs.fetch_sub(1, Ordering::SeqCst);
            self.open_input_bytes.fetch_sub(bytes, Ordering::SeqCst);
            let _ = self
                .opened_at
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&position);
        }
    }

    fn boundary_component_string(
        value: Option<String>,
        descending: bool,
        nulls_first: bool,
    ) -> RowGroupBoundaryComponent {
        RowGroupBoundaryComponent {
            value: value.map(RowGroupBoundaryValue::String),
            descending,
            nulls_first,
        }
    }

    fn boundary_component_timestamp_micros(
        value: Option<i64>,
        descending: bool,
        nulls_first: bool,
    ) -> RowGroupBoundaryComponent {
        RowGroupBoundaryComponent {
            value: value.map(RowGroupBoundaryValue::TimestampMicros),
            descending,
            nulls_first,
        }
    }

    /// Build a [`RowGroupBoundaryRange`] from the first and last row of a sorted
    /// logs batch, mirroring the ingest WAL `test_utils` helper.
    fn logs_row_group_boundary_range_from_batch(batch: &RecordBatch) -> RowGroupBoundaryRange {
        let sort_columns = SortColumnCache::try_new(
            batch,
            SortColumnsDescriptor::logs().expect("logs descriptor"),
            "WAL sorting",
        )
        .expect("sort columns");
        let last_row_idx = batch.num_rows() - 1;
        let range = RowGroupBoundaryRange {
            names: sort_columns.column_names(),
            min_key: sort_columns.boundary_key(0),
            max_key: sort_columns.boundary_key(last_row_idx),
        };
        range.validate().expect("boundary range");
        range
    }

    #[allow(clippy::needless_pass_by_value)]
    fn logs_batch(rows: Vec<(Option<&str>, Option<i64>, i64)>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("value", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(service_name, _, _)| *service_name).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|(_, timestamp, _)| *timestamp).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .expect("logs batch")
    }

    /// Build a [`MergeInput`] from a sorted logs batch, packing the WAL
    /// `(segment_offset, row_group_idx)` pair into `position` and defaulting the
    /// byte size to `1` (matching the old `plan` helper).
    fn input(segment_offset: u64, row_group_idx: usize, batch: &RecordBatch) -> MergeInput {
        input_with_bytes(segment_offset, row_group_idx, batch, 1)
    }

    fn input_with_bytes(segment_offset: u64, row_group_idx: usize, batch: &RecordBatch, bytes: u64) -> MergeInput {
        MergeInput {
            position: position(segment_offset, row_group_idx),
            bytes,
            boundary_range: logs_row_group_boundary_range_from_batch(batch),
        }
    }

    fn input_with_range(
        segment_offset: u64,
        row_group_idx: usize,
        min_key: RowGroupBoundaryKey,
        max_key: RowGroupBoundaryKey,
    ) -> MergeInput {
        MergeInput {
            position: position(segment_offset, row_group_idx),
            bytes: 1,
            boundary_range: RowGroupBoundaryRange {
                names: Arc::from(["service_name".to_string(), "timestamp".to_string()]),
                min_key,
                max_key,
            },
        }
    }

    fn key(service_name: Option<&str>, timestamp_micros: Option<i64>) -> RowGroupBoundaryKey {
        RowGroupBoundaryKey::new(vec![
            boundary_component_string(service_name.map(str::to_string), false, true),
            boundary_component_timestamp_micros(timestamp_micros, true, true),
        ])
    }

    fn cluster_sizes_from_metadata(inputs: Vec<MergeInput>) -> Vec<usize> {
        let sort_descriptor = SortColumnsDescriptor::logs().expect("sort descriptor");
        let mut merger = RowGroupsMerger::<FakeMergeSource> {
            config: SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                cancel_token: CancellationToken::new(),
                sort_descriptor,
            },
            schema: Arc::new(Schema::empty()),
            source: Arc::new(FakeMergeSource::default()),
            observer: Arc::new(NoopRowGroupsMergerObserver),
            pending_inputs: PlannedInputRead::from_inputs(inputs)
                .into_iter()
                .map(super::PendingInputWithOrder::new)
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
        while !merger.pending_inputs.is_empty() {
            let (cluster, _) = merger.pop_next_cluster_sources().expect("cluster");
            sizes.push(cluster.len());
        }
        sizes
    }

    async fn merged_values(source: Arc<FakeMergeSource>, inputs: Vec<MergeInput>) -> Vec<i64> {
        merged_values_with_row_group_size(source, inputs, 8).await
    }

    async fn merged_values_with_row_group_size(
        source: Arc<FakeMergeSource>,
        inputs: Vec<MergeInput>,
        row_group_size: usize,
    ) -> Vec<i64> {
        let merger = RowGroupsMerger::new(
            source,
            inputs,
            SortedBatchMergerConfig {
                row_group_size,
                read_parallelism: 2,
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
                let values = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("values");
                (0..batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect()
    }

    async fn merged_batches_with_row_group_size(
        source: Arc<FakeMergeSource>,
        inputs: Vec<MergeInput>,
        row_group_size: usize,
    ) -> Vec<RecordBatch> {
        let merger = RowGroupsMerger::new(
            source,
            inputs,
            SortedBatchMergerConfig {
                row_group_size,
                read_parallelism: 2,
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger");

        merger.into_stream().try_collect::<Vec<_>>().await.expect("merged")
    }

    #[tokio::test]
    async fn merger_preserves_global_order_across_non_overlapping_clusters() {
        let batch_1 = logs_batch(vec![(Some("svc-1"), Some(30), 1)]);
        let batch_2 = logs_batch(vec![(Some("svc-1"), Some(20), 2)]);
        let batch_3 = logs_batch(vec![(Some("svc-1"), Some(10), 3)]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(1, 0), vec![batch_1.clone()]),
                (position(2, 0), vec![batch_2.clone()]),
                (position(3, 0), vec![batch_3.clone()]),
            ]),
            ..FakeMergeSource::default()
        });
        let inputs = vec![input(1, 0, &batch_1), input(2, 0, &batch_2), input(3, 0, &batch_3)];
        let merger = RowGroupsMerger::new(
            source,
            inputs,
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 2,
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger");

        let batches = merger.into_stream().try_collect::<Vec<_>>().await.expect("merged");
        let values = batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("values");
                (0..batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn merger_slices_single_source_cluster_without_reordering() {
        let batch = logs_batch(vec![
            (Some("svc-1"), Some(30), 1),
            (Some("svc-1"), Some(20), 2),
            (Some("svc-1"), Some(10), 3),
        ]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([(position(1, 0), vec![batch.clone()])]),
            ..FakeMergeSource::default()
        });

        let batches = merged_batches_with_row_group_size(source, vec![input(1, 0, &batch)], 2).await;
        let values = batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("values");
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
        let batch_1 = logs_batch(vec![(Some("svc-1"), Some(40), 1), (Some("svc-1"), Some(30), 2)]);
        let batch_2 = logs_batch(vec![(Some("svc-1"), Some(30), 3), (Some("svc-1"), Some(10), 4)]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(1, 0), vec![batch_1.clone()]),
                (position(2, 0), vec![batch_2.clone()]),
            ]),
            ..FakeMergeSource::default()
        });

        let values =
            merged_values_with_row_group_size(source, vec![input(1, 0, &batch_1), input(2, 0, &batch_2)], 2).await;

        assert_eq!(values, vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn merger_releases_interleave_sources_before_yielding_output() {
        let batch_1 = logs_batch(vec![(Some("svc-1"), Some(40), 1), (Some("svc-1"), Some(20), 3)]);
        let batch_2 = logs_batch(vec![(Some("svc-1"), Some(30), 2), (Some("svc-1"), Some(10), 4)]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(1, 0), vec![batch_1.clone()]),
                (position(2, 0), vec![batch_2.clone()]),
            ]),
            ..FakeMergeSource::default()
        });
        let batch_1_value_column = Arc::clone(batch_1.column(2));
        let batch_2_value_column = Arc::clone(batch_2.column(2));

        let merger = RowGroupsMerger::new(
            source,
            vec![input(1, 0, &batch_1), input(2, 0, &batch_2)],
            SortedBatchMergerConfig {
                row_group_size: 4,
                read_parallelism: 2,
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger");

        let mut stream = merger.into_stream();
        let merged_batch = stream.try_next().await.expect("next merged batch").expect("merged batch");
        let values = merged_batch.column(2).as_any().downcast_ref::<Int64Array>().expect("values");
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
            (Some("svc-1"), Some(60), 1),
            (Some("svc-1"), Some(40), 3),
            (Some("svc-1"), Some(20), 5),
        ]);
        let batch_2 = logs_batch(vec![
            (Some("svc-1"), Some(50), 2),
            (Some("svc-1"), Some(30), 4),
            (Some("svc-1"), Some(10), 6),
        ]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(1, 0), vec![batch_1.clone()]),
                (position(2, 0), vec![batch_2.clone()]),
            ]),
            ..FakeMergeSource::default()
        });
        let cancel_token = CancellationToken::new();
        let merger = RowGroupsMerger::new(
            source,
            vec![input(1, 0, &batch_1), input(2, 0, &batch_2)],
            SortedBatchMergerConfig {
                row_group_size: 2,
                read_parallelism: 2,
                cancel_token: cancel_token.clone(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger");

        let mut stream = merger.into_stream();
        let first_batch = stream.try_next().await.expect("first batch").expect("first batch");
        let values = first_batch.column(2).as_any().downcast_ref::<Int64Array>().expect("values");
        assert_eq!(
            (0..first_batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>(),
            vec![1, 2]
        );

        cancel_token.cancel();
        let err = stream.try_next().await.expect_err("next batch must be cancelled");
        assert!(matches!(err, CommonError::Write(_)));
    }

    #[tokio::test]
    async fn merger_merges_overlapping_row_groups_in_single_cluster() {
        let batch_1 = logs_batch(vec![(Some("svc-1"), Some(30), 1)]);
        let batch_2 = logs_batch(vec![(Some("svc-1"), Some(20), 2)]);
        let batch_3 = logs_batch(vec![(Some("svc-1"), Some(40), 3)]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(1, 0), vec![batch_1.clone()]),
                (position(2, 0), vec![batch_2.clone()]),
                (position(3, 0), vec![batch_3.clone()]),
            ]),
            ..FakeMergeSource::default()
        });
        let inputs = vec![input(1, 0, &batch_1), input(2, 0, &batch_2), input(3, 0, &batch_3)];

        let merger = RowGroupsMerger::new(
            source,
            inputs,
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger");

        let batches = merger.into_stream().try_collect::<Vec<_>>().await.expect("merged");
        let values = batches
            .iter()
            .flat_map(|batch| {
                let values = batch.column(2).as_any().downcast_ref::<Int64Array>().expect("values");
                (0..batch.num_rows()).map(|idx| values.value(idx)).collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![3, 1, 2]);
    }

    #[tokio::test]
    async fn merger_preserves_wal_order_for_equal_keys_across_segments() {
        let batch_1 = logs_batch(vec![(Some("svc-1"), Some(30), 11), (Some("svc-1"), Some(30), 12)]);
        let batch_2 = logs_batch(vec![(Some("svc-1"), Some(30), 21)]);
        let batch_3 = logs_batch(vec![(Some("svc-1"), Some(30), 31), (Some("svc-1"), Some(30), 32)]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(10, 0), vec![batch_1.clone()]),
                (position(11, 0), vec![batch_2.clone()]),
                (position(12, 0), vec![batch_3.clone()]),
            ]),
            ..FakeMergeSource::default()
        });
        let values = merged_values(
            source,
            vec![input(10, 0, &batch_1), input(11, 0, &batch_2), input(12, 0, &batch_3)],
        )
        .await;

        assert_eq!(values, vec![11, 12, 21, 31, 32]);
    }

    #[tokio::test]
    async fn merger_preserves_wal_order_for_equal_keys_across_row_groups_in_segment() {
        let row_group_0 = logs_batch(vec![(Some("svc-1"), Some(30), 101), (Some("svc-1"), Some(30), 102)]);
        let row_group_1 = logs_batch(vec![(Some("svc-1"), Some(30), 201), (Some("svc-1"), Some(30), 202)]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(7, 0), vec![row_group_0.clone()]),
                (position(7, 1), vec![row_group_1.clone()]),
            ]),
            ..FakeMergeSource::default()
        });
        let values = merged_values(source, vec![input(7, 0, &row_group_0), input(7, 1, &row_group_1)]).await;

        assert_eq!(values, vec![101, 102, 201, 202]);
    }

    #[tokio::test]
    async fn merger_preserves_wal_order_when_cluster_opens_finish_out_of_order() {
        let batch_1 = logs_batch(vec![(Some("svc-1"), Some(30), 11)]);
        let batch_2 = logs_batch(vec![(Some("svc-1"), Some(30), 21)]);
        let batch_3 = logs_batch(vec![(Some("svc-1"), Some(30), 31)]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(10, 0), vec![batch_1.clone()]),
                (position(11, 0), vec![batch_2.clone()]),
                (position(12, 0), vec![batch_3.clone()]),
            ]),
            open_delays: HashMap::from([
                (position(10, 0), Duration::from_millis(30)),
                (position(11, 0), Duration::from_millis(10)),
                (position(12, 0), Duration::from_millis(0)),
            ]),
            ..FakeMergeSource::default()
        });
        let values = merged_values(
            source,
            vec![input(10, 0, &batch_1), input(11, 0, &batch_2), input(12, 0, &batch_3)],
        )
        .await;

        assert_eq!(values, vec![11, 21, 31]);
    }

    #[tokio::test]
    async fn merger_observer_tracks_opened_and_closed_row_groups() {
        let batch_1 = logs_batch(vec![(Some("svc-1"), Some(40), 1)]);
        let batch_2 = logs_batch(vec![(Some("svc-1"), Some(30), 2)]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(1, 0), vec![batch_1.clone()]),
                (position(2, 0), vec![batch_2.clone()]),
            ]),
            ..FakeMergeSource::default()
        });
        let observer = Arc::new(FakeObserver::default());
        let merger = RowGroupsMerger::new(
            source,
            vec![
                input_with_bytes(1, 0, &batch_1, 10),
                input_with_bytes(2, 0, &batch_2, 20),
            ],
            SortedBatchMergerConfig {
                row_group_size: 1,
                read_parallelism: 2,
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
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
        assert_eq!(observer.open_inputs(), 0);
        assert_eq!(observer.open_input_bytes(), 0);
    }

    #[tokio::test]
    async fn merger_observer_resets_open_counts_on_cancel() {
        let batch_1 = logs_batch(vec![
            (Some("svc-1"), Some(60), 1),
            (Some("svc-1"), Some(40), 3),
            (Some("svc-1"), Some(20), 5),
        ]);
        let batch_2 = logs_batch(vec![
            (Some("svc-1"), Some(50), 2),
            (Some("svc-1"), Some(30), 4),
            (Some("svc-1"), Some(10), 6),
        ]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(1, 0), vec![batch_1.clone()]),
                (position(2, 0), vec![batch_2.clone()]),
            ]),
            ..FakeMergeSource::default()
        });
        let observer = Arc::new(FakeObserver::default());
        let cancel_token = CancellationToken::new();
        let merger = RowGroupsMerger::new(
            source,
            vec![
                input_with_bytes(1, 0, &batch_1, 10),
                input_with_bytes(2, 0, &batch_2, 20),
            ],
            SortedBatchMergerConfig {
                row_group_size: 2,
                read_parallelism: 2,
                cancel_token: cancel_token.clone(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
        )
        .expect("merger")
        .with_observer(Arc::clone(&observer) as Arc<dyn RowGroupsMergerObserver>);

        let mut stream = merger.into_stream();
        let _ = stream.try_next().await.expect("first batch").expect("first batch");
        assert!(observer.open_inputs() > 0);
        cancel_token.cancel();
        let _ = stream.try_next().await.expect_err("cancelled");
        drop(stream);

        assert_eq!(observer.open_inputs(), 0);
        assert_eq!(observer.open_input_bytes(), 0);
    }

    /// Two disjoint time clusters of the same service packed into a single
    /// chunk must keep their row-group-level boundaries intact.  The merger
    /// emits batches in sort-key order and never invents timestamps in the
    /// temporal gap between the two clusters — the downstream parquet writer
    /// therefore preserves per-row-group min/max bounds (its `ColumnIndex` is
    /// empty for the gap).
    #[tokio::test]
    async fn merger_preserves_row_group_level_bounds_with_temporal_gap() {
        // Cluster A: ts in [10..40]; Cluster B: ts in [200..230].  Same service
        // → swept-line clusters disjoint in sort-key space, packed in one
        // chunk by `bin_pack`.  Sort key uses ts DESC, so each cluster emits
        // values from max ts to min ts; cluster B (later in time) precedes
        // cluster A under the (account, service, ts DESC) ordering, but they
        // share account/service so cluster ordering follows ts DESC anyway.
        let cluster_a = logs_batch(vec![
            (Some("svc-1"), Some(40), 1),
            (Some("svc-1"), Some(30), 2),
            (Some("svc-1"), Some(20), 3),
            (Some("svc-1"), Some(10), 4),
        ]);
        let cluster_b = logs_batch(vec![
            (Some("svc-1"), Some(230), 5),
            (Some("svc-1"), Some(220), 6),
            (Some("svc-1"), Some(210), 7),
            (Some("svc-1"), Some(200), 8),
        ]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(1, 0), vec![cluster_a.clone()]),
                (position(2, 0), vec![cluster_b.clone()]),
            ]),
            ..FakeMergeSource::default()
        });

        let batches =
            merged_batches_with_row_group_size(source, vec![input(1, 0, &cluster_a), input(2, 0, &cluster_b)], 8).await;

        // 1. The two disjoint clusters arrive as separate batch slices: the
        //    merger never blends rows from cluster A and cluster B inside the
        //    same emitted batch (cluster ordering is preserved).
        // 2. No timestamp falls into the temporal gap (40, 200): if a future
        //    refactor accidentally reorders or fabricates rows, this guard
        //    fails immediately.
        let timestamps: Vec<i64> = batches
            .iter()
            .flat_map(|batch| {
                let col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("timestamp column");
                (0..batch.num_rows()).map(|idx| col.value(idx)).collect::<Vec<_>>()
            })
            .collect();

        // Cluster B precedes cluster A under ts DESC; both clusters appear in
        // their natural descending order.
        assert_eq!(timestamps, vec![230, 220, 210, 200, 40, 30, 20, 10]);
        for ts in &timestamps {
            assert!(
                !(40 < *ts && *ts < 200),
                "merger must not surface a timestamp inside the temporal gap (40, 200): got {ts}"
            );
        }

        // No batch may straddle the cluster boundary: every emitted batch's
        // timestamp range must fit inside one of the two source clusters.
        for batch in &batches {
            let col = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp column");
            let mins = (0..batch.num_rows()).map(|i| col.value(i)).min().expect("min");
            let maxs = (0..batch.num_rows()).map(|i| col.value(i)).max().expect("max");
            let in_a = mins >= 10 && maxs <= 40;
            let in_b = mins >= 200 && maxs <= 230;
            assert!(
                in_a || in_b,
                "batch range [{mins}, {maxs}] must stay within one cluster"
            );
        }
    }

    /// When a single bin-packed chunk spans the UTC midnight day boundary,
    /// the merger must keep the row order monotonic under the composite sort
    /// key (account, service, ts DESC) — the day boundary is a purely
    /// semantic line and must not perturb the merge.
    #[tokio::test]
    async fn merger_handles_chunk_spanning_day_boundary() {
        // MICROS_PER_DAY duplicated as a local constant to avoid plumbing the
        // planner-side constant through the merger's test scope.
        const MICROS_PER_DAY: i64 = 86_400 * 1_000_000;
        let before = logs_batch(vec![
            (Some("svc-1"), Some(MICROS_PER_DAY - 10), 1),
            (Some("svc-1"), Some(MICROS_PER_DAY - 20), 2),
        ]);
        let after = logs_batch(vec![
            (Some("svc-1"), Some(MICROS_PER_DAY + 20), 3),
            (Some("svc-1"), Some(MICROS_PER_DAY + 10), 4),
        ]);
        let source = Arc::new(FakeMergeSource {
            batches: HashMap::from([
                (position(1, 0), vec![before.clone()]),
                (position(2, 0), vec![after.clone()]),
            ]),
            ..FakeMergeSource::default()
        });

        let values = merged_values(source, vec![input(1, 0, &before), input(2, 0, &after)]).await;
        // ts DESC → after-midnight rows first, then before-midnight rows.
        assert_eq!(values, vec![3, 4, 1, 2]);
    }

    #[test]
    fn cluster_builder_forms_overlap_chain() {
        let inputs = vec![
            input_with_range(1, 0, key(Some("svc-a"), Some(50)), key(Some("svc-a"), Some(30))),
            input_with_range(2, 0, key(Some("svc-a"), Some(40)), key(Some("svc-a"), Some(20))),
            input_with_range(3, 0, key(Some("svc-a"), Some(25)), key(Some("svc-a"), Some(10))),
        ];

        assert_eq!(cluster_sizes_from_metadata(inputs), vec![3]);
    }

    #[test]
    fn cluster_builder_splits_non_overlapping_row_groups() {
        let inputs = vec![
            input_with_range(1, 0, key(Some("svc-a"), Some(50)), key(Some("svc-a"), Some(40))),
            input_with_range(2, 0, key(Some("svc-b"), Some(30)), key(Some("svc-b"), Some(20))),
        ];

        assert_eq!(cluster_sizes_from_metadata(inputs), vec![1, 1]);
    }

    #[test]
    fn cluster_builder_tracks_data_driven_width_pattern() {
        let mut inputs = Vec::new();

        for idx in 0..4 {
            let start = 100 - i64::from(idx);
            inputs.push(input_with_range(
                u64::try_from(idx + 1).expect("segment offset"),
                0,
                key(Some("svc-a"), Some(start)),
                key(Some("svc-a"), Some(80)),
            ));
        }
        for idx in 0..2 {
            let start = 70 - i64::from(idx);
            inputs.push(input_with_range(
                u64::try_from(idx + 10).expect("segment offset"),
                0,
                key(Some("svc-b"), Some(start)),
                key(Some("svc-b"), Some(60)),
            ));
        }
        for idx in 0..10 {
            let start = 50 - i64::from(idx);
            inputs.push(input_with_range(
                u64::try_from(idx + 20).expect("segment offset"),
                0,
                key(Some("svc-c"), Some(start)),
                key(Some("svc-c"), Some(30)),
            ));
        }

        assert_eq!(cluster_sizes_from_metadata(inputs), vec![4, 2, 10]);
    }

    #[tokio::test]
    async fn merger_rejects_incompatible_boundary_key_structure() {
        let plan = PlannedInputRead::from_inputs(vec![
            input_with_range(1, 0, key(Some("svc-1"), Some(20)), key(Some("svc-1"), Some(10))),
            input_with_range(
                2,
                0,
                RowGroupBoundaryKey::new(vec![boundary_component_string(Some("svc-2".to_string()), false, true)]),
                RowGroupBoundaryKey::new(vec![boundary_component_string(Some("svc-2".to_string()), false, true)]),
            ),
        ]);

        let Err(err) = RowGroupsMerger::new_with_plan(
            Arc::new(FakeMergeSource::default()),
            plan,
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
            Arc::new(NoopRowGroupsMergerObserver),
        ) else {
            panic!("incompatible boundary keys must be rejected");
        };

        assert!(err.to_string().contains("arity mismatch"));
    }

    #[tokio::test]
    async fn merger_rejects_duplicate_input_positions_in_read_plan() {
        let plan = PlannedInputRead::from_inputs(vec![
            input_with_range(42, 0, key(Some("svc-1"), Some(50)), key(Some("svc-1"), Some(40))),
            input_with_range(42, 0, key(Some("svc-1"), Some(30)), key(Some("svc-1"), Some(20))),
        ]);

        let Err(err) = RowGroupsMerger::new_with_plan(
            Arc::new(FakeMergeSource::default()),
            plan,
            SortedBatchMergerConfig {
                row_group_size: 8,
                read_parallelism: 1,
                cancel_token: CancellationToken::new(),
                sort_descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
            },
            Arc::new(NoopRowGroupsMergerObserver),
        ) else {
            panic!("duplicate input positions must be rejected");
        };

        assert!(err.to_string().contains(&format!(
            "duplicate input position in merge plan: position={}",
            position(42, 0)
        )));
    }

    #[test]
    fn cluster_builder_keeps_distinct_row_groups_with_equal_min_key() {
        let same_min = key(Some("svc-a"), Some(50));
        let inputs = vec![
            input_with_range(7, 0, same_min.clone(), key(Some("svc-a"), Some(40))),
            input_with_range(7, 1, same_min, key(Some("svc-a"), Some(30))),
        ];

        assert_eq!(cluster_sizes_from_metadata(inputs), vec![2]);
    }
}
