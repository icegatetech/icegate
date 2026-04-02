use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use arrow::{
    array::{Array, StringArray, TimestampMicrosecondArray},
    compute::interleave_record_batch,
    datatypes::{DataType, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt};
use icegate_common::{RowGroupBoundaryKey, compare_option_ord};
use icegate_queue::{QueueReader, RecordBatchStream, Topic};
use tokio_util::sync::CancellationToken;

use crate::{
    error::{IngestError, Result},
    shift::executor::PlannedRowGroup,
};

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

struct InputStreamState {
    source: PlannedRowGroupRead,
    stream: Option<RecordBatchStream>,
    current_batch: Option<RecordBatch>,
    current_batch_start_row: u64,
    batch_generation: u64,
    cache: Option<SortColumnCache>,
    rows_consumed_total: u64,
    open: bool,
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

    fn peek(&self) -> Option<&MergeCursor> {
        self.cursors.first()
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
    streams: &'a [InputStreamState],
}

impl<'a> RowComparator<'a> {
    fn new(streams: &'a [InputStreamState]) -> Self {
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
    pub boundary_key: RowGroupBoundaryKey,
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
            .boundary_key
            .compare(&other.source.boundary_key)
            .then_with(|| self.source.plan_idx.cmp(&other.source.plan_idx))
    }
}

impl PartialOrd for PendingRowGroupEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct SortedBatchMerger<Q: QueueReader + ?Sized> {
    schema: SchemaRef,
    row_group_size: usize,
    queue_reader: Arc<Q>,
    topic: Topic,
    pending_row_groups: BTreeSet<PendingRowGroupEntry>,
    active_streams: Vec<InputStreamState>,
    active_stream_count: usize,
    read_parallelism: usize,
    max_active_row_group_streams: usize,
    heap: MergeHeap,
    cancel_token: CancellationToken,
}

impl<Q> SortedBatchMerger<Q>
where
    Q: QueueReader + 'static + ?Sized,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new(
        queue_reader: Arc<Q>,
        topic: Topic,
        read_plan: Vec<PlannedRowGroupRead>,
        row_group_size: usize,
        read_parallelism: usize,
        max_active_row_group_streams: usize,
        cancel_token: CancellationToken,
    ) -> Result<Self> {
        if row_group_size == 0 {
            return Err(IngestError::Config(
                "row_group_size must be greater than zero".to_string(),
            ));
        }
        if max_active_row_group_streams == 0 {
            return Err(IngestError::Config(
                "max_active_row_group_streams must be greater than zero".to_string(),
            ));
        }
        if read_parallelism == 0 {
            return Err(IngestError::Config(
                "shift_segment_read_parallelism must be greater than zero".to_string(),
            ));
        }

        let mut merger = Self {
            schema: SchemaRef::new(arrow::datatypes::Schema::empty()),
            row_group_size,
            queue_reader,
            topic,
            pending_row_groups: read_plan.into_iter().map(PendingRowGroupEntry::new).collect(),
            active_streams: Vec::new(),
            active_stream_count: 0,
            read_parallelism,
            max_active_row_group_streams,
            heap: MergeHeap::new(),
            cancel_token,
        };
        merger.ensure_order_window().await?;
        Ok(merger)
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
        self.ensure_order_window().await?;
        if self.heap.is_empty() {
            return Ok(None);
        }

        let mut source_batches = Vec::new();
        let mut source_batch_idxs = HashMap::new();
        let mut interleave_indices = Vec::with_capacity(self.row_group_size);

        while interleave_indices.len() < self.row_group_size {
            self.ensure_order_window().await?;
            let Some(cursor) = ({
                let comparator = RowComparator::new(&self.active_streams);
                self.heap.pop(&comparator)?
            }) else {
                break;
            };
            let stream_state = self
                .active_streams
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
                let comparator = RowComparator::new(&self.active_streams);
                self.heap.push(next_cursor, &comparator)?;
            }
        }

        if interleave_indices.is_empty() {
            return Ok(None);
        }

        let source_refs = source_batches.iter().collect::<Vec<_>>();
        Ok(Some(interleave_record_batch(&source_refs, &interleave_indices)?))
    }

    async fn ensure_order_window(&mut self) -> Result<()> {
        loop {
            if self.heap.is_empty() && self.active_stream_count == 0 {
                self.preopen_initial_window().await?;
                if self.heap.is_empty() && self.pending_row_groups.is_empty() {
                    return Ok(());
                }
                if self.heap.is_empty() {
                    continue;
                }
            }

            let Some(next_pending) = self.peek_pending_source() else {
                return Ok(());
            };
            let Some(current_min) = self.current_heap_min_boundary_key()? else {
                return Ok(());
            };
            if next_pending.boundary_key.compare(&current_min) == Ordering::Greater {
                return Ok(());
            }
            if self.active_stream_count >= self.max_active_row_group_streams {
                return Err(IngestError::Shift(format!(
                    "configured max_active_row_group_streams={} is too small to preserve merge order for segment {} row group {}",
                    self.max_active_row_group_streams, next_pending.segment_offset, next_pending.row_group_idx
                )));
            }
            let next_pending = self.pop_pending_source()?.ok_or_else(|| {
                IngestError::Shift("pending row groups disappeared during lazy activation".to_string())
            })?;
            self.activate_source(next_pending).await?;
        }
    }

    async fn preopen_initial_window(&mut self) -> Result<()> {
        let open_count = self
            .pending_row_groups
            .len()
            .min(self.read_parallelism)
            .min(self.max_active_row_group_streams);
        if open_count == 0 {
            return Ok(());
        }

        let mut sources = Vec::with_capacity(open_count);
        for source_order in 0..open_count {
            let source = self.pop_pending_source()?.ok_or_else(|| {
                IngestError::Shift("pending row groups disappeared during initial window activation".to_string())
            })?;
            sources.push((source_order, source));
        }
        let queue_reader = Arc::clone(&self.queue_reader);
        let topic = self.topic.clone();
        let cancel_token = self.cancel_token.clone();
        let mut opened = futures::stream::iter(sources.into_iter().map(|(source_order, source)| {
            let queue_reader = Arc::clone(&queue_reader);
            let topic = topic.clone();
            let cancel_token = cancel_token.clone();
            async move {
                let opened = open_source(queue_reader, topic, source, cancel_token).await?;
                Ok::<_, IngestError>((source_order, opened))
            }
        }))
        .buffer_unordered(open_count)
        .try_collect::<Vec<_>>()
        .await?;
        opened.sort_by_key(|(source_order, _)| *source_order);
        for (_source_order, opened) in opened {
            if let Some(opened) = opened {
                self.push_opened_source(opened)?;
            }
        }
        Ok(())
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

    async fn activate_source(&mut self, source: PlannedRowGroupRead) -> Result<()> {
        let Some(opened) = open_source(
            Arc::clone(&self.queue_reader),
            self.topic.clone(),
            source,
            self.cancel_token.clone(),
        )
        .await?
        else {
            return Ok(());
        };
        self.push_opened_source(opened)?;
        Ok(())
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
        let stream_idx = self.active_streams.len();
        self.active_streams.push(InputStreamState {
            source: opened.source,
            stream: Some(opened.stream),
            current_batch: Some(opened.batch),
            current_batch_start_row: 0,
            batch_generation: 1,
            cache: Some(cache),
            rows_consumed_total: 0,
            open: true,
        });
        self.active_stream_count += 1;
        let comparator = RowComparator::new(&self.active_streams);
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

    fn current_heap_min_boundary_key(&self) -> Result<Option<RowGroupBoundaryKey>> {
        let Some(cursor) = self.heap.peek() else {
            return Ok(None);
        };
        let state = self
            .active_streams
            .get(cursor.stream_idx)
            .ok_or_else(|| IngestError::Shift("heap cursor stream index out of bounds".to_string()))?;
        let cache = state
            .cache
            .as_ref()
            .ok_or_else(|| IngestError::Shift("heap cursor cache is missing".to_string()))?;
        Ok(Some(cache.row_boundary_key(cursor.row_idx)?))
    }

    async fn advance_cursor(&mut self, cursor: MergeCursor) -> Result<Option<MergeCursor>> {
        let stream_state = self
            .active_streams
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
            IngestError::Shift(format!(
                "failed to advance WAL segment {} row group {}: {err}",
                stream_state.source.segment_offset, stream_state.source.row_group_idx
            ))
        })?
        else {
            stream_state.current_batch = None;
            stream_state.cache = None;
            stream_state.stream = None;
            if stream_state.open {
                stream_state.open = false;
                self.active_stream_count = self.active_stream_count.saturating_sub(1);
            }
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
            IngestError::Shift(format!(
                "failed to open WAL segment {} row group {}: {err}",
                source.segment_offset, source.row_group_idx
            ))
        })?;
    let Some(batch) = next_non_empty_batch(&mut stream).await.map_err(|err| {
        IngestError::Shift(format!(
            "failed to read WAL segment {} row group {}: {err}",
            source.segment_offset, source.row_group_idx
        ))
    })?
    else {
        return Ok(None);
    };
    Ok(Some(OpenedSource { source, stream, batch }))
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

    fn row_boundary_key(&self, row_idx: usize) -> Result<RowGroupBoundaryKey> {
        let CachedSortColumn::Utf8 {
            values: cloud_account_id,
            ..
        } = &self.columns[0]
        else {
            return Err(IngestError::Shift("cloud_account_id cache type is invalid".to_string()));
        };
        let CachedSortColumn::Utf8 {
            values: service_name, ..
        } = &self.columns[1]
        else {
            return Err(IngestError::Shift("service_name cache type is invalid".to_string()));
        };
        let CachedSortColumn::TimestampMicrosecond { values: timestamp, .. } = &self.columns[2] else {
            return Err(IngestError::Shift("timestamp cache type is invalid".to_string()));
        };
        Ok(RowGroupBoundaryKey {
            cloud_account_id: (!cloud_account_id.is_null(row_idx)).then(|| cloud_account_id.value(row_idx).to_string()),
            service_name: (!service_name.is_null(row_idx)).then(|| service_name.value(row_idx).to_string()),
            timestamp_micros: (!timestamp.is_null(row_idx)).then(|| timestamp.value(row_idx)),
        })
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
            boundary_key: row_group.boundary_key.clone(),
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
    use tokio_util::sync::CancellationToken;

    use super::{PlannedRowGroupRead, SortedBatchMerger};
    use crate::{
        shift::executor::{PlannedRowGroup, SegmentToRead},
        wal_sort::logs_row_group_boundary_key_from_batch,
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
                boundary_key: logs_row_group_boundary_key_from_batch(batch).expect("boundary key"),
            }],
        }
    }

    #[tokio::test]
    async fn lazy_merger_preserves_global_order() {
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
            "logs".to_string(),
            PlannedRowGroupRead::from_segments(&segments),
            8,
            2,
            2,
            CancellationToken::new(),
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
    async fn lazy_merger_fails_when_order_requires_more_active_sources() {
        let batch_1 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(30), 1)]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(30), 2)]);
        let queue_reader = Arc::new(FakeQueueReader {
            batches: HashMap::from([((1, 0), vec![batch_1.clone()]), ((2, 0), vec![batch_2.clone()])]),
        });
        let segments = vec![plan(1, 0, &batch_1), plan(2, 0, &batch_2)];
        let err = match SortedBatchMerger::try_new(
            queue_reader,
            "logs".to_string(),
            PlannedRowGroupRead::from_segments(&segments),
            8,
            1,
            1,
            CancellationToken::new(),
        )
        .await
        {
            Ok(_) => panic!("must fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("max_active_row_group_streams"));
    }

    #[tokio::test]
    async fn lazy_merger_activates_global_min_pending_source() {
        let batch_1 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(30), 1)]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(20), 2)]);
        let batch_3 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(40), 3)]);
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
            "logs".to_string(),
            PlannedRowGroupRead::from_segments(&segments),
            8,
            1,
            2,
            CancellationToken::new(),
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

    #[tokio::test]
    async fn lazy_merger_preopens_initial_window_by_boundary_order() {
        let batch_1 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(30), 1)]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(20), 2)]);
        let batch_3 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(40), 3)]);
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
            "logs".to_string(),
            PlannedRowGroupRead::from_segments(&segments),
            8,
            2,
            2,
            CancellationToken::new(),
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

    #[tokio::test]
    async fn lazy_merger_breaks_equal_boundary_ties_by_plan_order() {
        let batch_1 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(30), 1)]);
        let batch_2 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(40), 2)]);
        let batch_3 = logs_batch(vec![(Some("acc-1"), Some("svc"), Some(40), 3)]);
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
            "logs".to_string(),
            PlannedRowGroupRead::from_segments(&segments),
            8,
            1,
            3,
            CancellationToken::new(),
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
        assert_eq!(values, vec![2, 3, 1]);
    }

    #[tokio::test]
    async fn lazy_merger_uses_full_composite_boundary_key_for_lazy_activation() {
        let batch_1 = logs_batch(vec![(Some("acc-1"), Some("svc-z"), Some(10), 1)]);
        let batch_2 = logs_batch(vec![(Some("acc-2"), Some("svc-a"), Some(100), 2)]);
        let batch_3 = logs_batch(vec![(Some("acc-2"), Some("svc-b"), Some(90), 3)]);
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
            "logs".to_string(),
            PlannedRowGroupRead::from_segments(&segments),
            8,
            1,
            1,
            CancellationToken::new(),
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
        assert_eq!(
            values,
            vec![1, 2, 3],
            "a boundary key that looked only at timestamp would activate the wrong source here"
        );
    }
}
