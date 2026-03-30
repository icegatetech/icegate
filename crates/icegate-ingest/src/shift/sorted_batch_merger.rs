use std::{cmp::Ordering, collections::HashMap};

use arrow::{
    array::{Array, StringArray, TimestampMicrosecondArray},
    compute::interleave_record_batch,
    datatypes::{DataType, SchemaRef, TimeUnit},
    record_batch::RecordBatch,
};
use futures::{StreamExt, TryStreamExt};
use icegate_queue::RecordBatchStream;

use crate::error::{IngestError, Result};

// TODO(crit): кажется, текущий компонент жестко привязан к логам

// TODO(crit): нужно брать из схемы, а не хардкодить
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

/// Lightweight cursor for the merge heap.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeCursor {
    stream_idx: usize,
    batch_generation: u64,
    row_idx: usize,
    segment_row_idx: u64,
}

struct InputStreamState {
    stream: RecordBatchStream,
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

/// K-way merger for sorted input streams.
pub struct SortedBatchMerger {
    schema: SchemaRef,
    row_group_size: usize,
    streams: Vec<InputStreamState>,
    heap: MergeHeap,
}

impl SortedBatchMerger {
    /// Create a merger for logs batches.
    pub async fn try_new(streams: Vec<RecordBatchStream>, row_group_size: usize) -> Result<Self> {
        if row_group_size == 0 {
            return Err(IngestError::Config(
                "row_group_size must be greater than zero".to_string(),
            ));
        }

        let streams = streams
            .into_iter()
            .map(|stream| InputStreamState {
                stream,
                current_batch: None,
                current_batch_start_row: 0,
                batch_generation: 0,
                cache: None,
                rows_consumed_total: 0,
            })
            .collect::<Vec<_>>();

        // TODO(crit): ненадежный резолв схемы
        let mut schema = None;
        let mut merger = Self {
            schema: SchemaRef::new(arrow::datatypes::Schema::empty()),
            row_group_size,
            streams,
            heap: MergeHeap::new(),
        };

        for stream_idx in 0..merger.streams.len() {
            let maybe_cursor = {
                let stream_state = merger
                    .streams
                    .get_mut(stream_idx)
                    .ok_or_else(|| IngestError::Shift("stream index out of bounds".to_string()))?;
                if let Some(batch) = next_non_empty_batch(&mut stream_state.stream).await? {
                    if let Some(expected_schema) = schema.as_ref() {
                        if batch.schema() != *expected_schema {
                            return Err(IngestError::Shift(
                                "merge input batches must have identical schemas".to_string(),
                            ));
                        }
                    } else {
                        schema = Some(batch.schema());
                    }
                    stream_state.cache = Some(SortColumnCache::try_new(&batch)?);
                    stream_state.batch_generation = 1;
                    stream_state.current_batch = Some(batch);
                    Some(MergeCursor {
                        stream_idx,
                        batch_generation: stream_state.batch_generation,
                        row_idx: 0,
                        segment_row_idx: 0,
                    })
                } else {
                    None
                }
            };
            if let Some(cursor) = maybe_cursor {
                let comparator = RowComparator::new(&merger.streams);
                merger.heap.push(cursor, &comparator)?;
            }
        }

        merger.schema = schema.unwrap_or_else(|| arrow::datatypes::SchemaRef::new(arrow::datatypes::Schema::empty()));
        if merger.heap.is_empty() && merger.schema.fields().is_empty() {
            merger.schema = arrow::datatypes::SchemaRef::new(arrow::datatypes::Schema::empty());
        }

        Ok(merger)
    }

    /// Convert the merger into a streaming output.
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
        if self.heap.is_empty() {
            return Ok(None);
        }

        let mut source_batches = Vec::new();
        let mut source_batch_idxs = HashMap::new();
        let mut interleave_indices = Vec::with_capacity(self.row_group_size);

        while interleave_indices.len() < self.row_group_size {
            let Some(cursor) = ({
                let comparator = RowComparator::new(&self.streams);
                self.heap.pop(&comparator)?
            }) else {
                break;
            };
            let stream_state = self
                .streams
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
                let comparator = RowComparator::new(&self.streams);
                self.heap.push(next_cursor, &comparator)?;
            }
        }

        if interleave_indices.is_empty() {
            return Ok(None);
        }

        let source_refs = source_batches.iter().collect::<Vec<_>>();
        Ok(Some(interleave_record_batch(&source_refs, &interleave_indices)?))
    }

    async fn advance_cursor(&mut self, cursor: MergeCursor) -> Result<Option<MergeCursor>> {
        let stream_state = self
            .streams
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

        let Some(next_batch) = next_non_empty_batch(&mut stream_state.stream).await? else {
            stream_state.current_batch = None;
            stream_state.cache = None;
            return Ok(None);
        };
        if next_batch.schema() != self.schema {
            return Err(IngestError::Shift(
                "merge input batches must keep the same schema".to_string(),
            ));
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

/// Typed cache for sort columns of the current input batch.
pub struct SortColumnCache {
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
                        .ok_or_else(|| IngestError::Shift(format!("{column_name} must be Utf8")))? // typed cache
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

fn compare_option_ord<T: Ord>(left: Option<T>, right: Option<T>, descending: bool, nulls_first: bool) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let ordering = left.cmp(&right);
            if descending { ordering.reverse() } else { ordering }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use futures::{StreamExt, TryStreamExt};

    use super::{
        InputStreamState, MergeCursor, MergeHeap, RowComparator, SortColumnCache, SortedBatchMerger, compare_option_ord,
    };
    use crate::error::IngestError;

    fn logs_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("cloud_account_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn logs_batch(rows: Vec<(Option<&str>, Option<&str>, Option<i64>, i64)>) -> RecordBatch {
        let schema = logs_schema();
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

    fn values(batch: &RecordBatch) -> Vec<i64> {
        let values = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("value");
        (0..batch.num_rows()).map(|row_idx| values.value(row_idx)).collect()
    }

    fn sort_keys(batch: &RecordBatch) -> Vec<(Option<String>, Option<String>, Option<i64>, i64)> {
        let cloud_account_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cloud_account_id");
        let service_name = batch.column(1).as_any().downcast_ref::<StringArray>().expect("service_name");
        let timestamp = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");
        let value = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("value");

        (0..batch.num_rows())
            .map(|row_idx| {
                (
                    (!cloud_account_id.is_null(row_idx)).then(|| cloud_account_id.value(row_idx).to_string()),
                    (!service_name.is_null(row_idx)).then(|| service_name.value(row_idx).to_string()),
                    (!timestamp.is_null(row_idx)).then(|| timestamp.value(row_idx)),
                    value.value(row_idx),
                )
            })
            .collect()
    }

    fn make_stream_state(batch: RecordBatch) -> InputStreamState {
        InputStreamState {
            stream: futures::stream::empty::<std::result::Result<RecordBatch, icegate_queue::QueueError>>().boxed(),
            current_batch: Some(batch.clone()),
            current_batch_start_row: 0,
            batch_generation: 1,
            cache: Some(SortColumnCache::try_new(&batch).expect("cache")),
            rows_consumed_total: 0,
        }
    }

    fn make_cursor(stream_idx: usize, row_idx: usize, segment_row_idx: u64) -> MergeCursor {
        MergeCursor {
            stream_idx,
            batch_generation: 1,
            row_idx,
            segment_row_idx,
        }
    }

    #[tokio::test]
    async fn sorted_batch_merger_merges_multiple_sorted_streams() {
        let stream_a = futures::stream::iter(vec![Ok(logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 1),
            (Some("acc-2"), Some("svc-1"), Some(20), 2),
        ]))])
        .boxed();
        let stream_b = futures::stream::iter(vec![Ok(logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(25), 3),
            (Some("acc-2"), Some("svc-2"), Some(10), 4),
        ]))])
        .boxed();

        let merger = SortedBatchMerger::try_new(vec![Box::pin(stream_a), Box::pin(stream_b)], 8)
            .await
            .expect("merger");
        let output = merger.into_stream().try_collect::<Vec<_>>().await.expect("merged batches");

        assert_eq!(output.len(), 1);
        assert_eq!(values(&output[0]), vec![1, 3, 2, 4]);
    }

    #[tokio::test]
    async fn sorted_batch_merger_returns_error_on_segment_row_idx_overflow() {
        let stream = futures::stream::iter(vec![Ok(logs_batch(vec![
            (Some("acc-1"), Some("svc-1"), Some(30), 1),
            (Some("acc-1"), Some("svc-1"), Some(20), 2),
        ]))])
        .boxed();

        let mut merger = SortedBatchMerger::try_new(vec![Box::pin(stream)], 8).await.expect("merger");
        merger.heap = MergeHeap {
            cursors: vec![MergeCursor {
                stream_idx: 0,
                batch_generation: 1,
                row_idx: 0,
                segment_row_idx: u64::MAX,
            }],
        };

        let err = merger.next_batch().await.expect_err("segment_row_idx overflow must fail");

        assert!(matches!(
            err,
            IngestError::Shift(message) if message == "segment row index overflow"
        ));
    }

    #[test]
    fn comparator_handles_nulls_desc_and_tie_break_columns() {
        let batch = logs_batch(vec![
            (None, Some("svc-0"), Some(50), 1),
            (Some("acc-1"), Some("svc-1"), Some(40), 2),
            (Some("acc-1"), Some("svc-1"), Some(20), 3),
        ]);
        let cache = SortColumnCache::try_new(&batch).expect("cache");

        assert_eq!(cache.compare_row(0, &cache, 1).expect("compare"), Ordering::Less,);
        assert_eq!(cache.compare_row(1, &cache, 2).expect("compare"), Ordering::Less,);
    }

    #[test]
    fn row_comparator_compares_by_first_distinct_sort_column() {
        let streams = vec![
            make_stream_state(logs_batch(vec![(Some("acc-1"), Some("svc-2"), Some(30), 1)])),
            make_stream_state(logs_batch(vec![(Some("acc-2"), Some("svc-0"), Some(90), 2)])),
        ];
        let comparator = RowComparator::new(&streams);

        let ordering = comparator
            .compare(&make_cursor(0, 0, 0), &make_cursor(1, 0, 0))
            .expect("compare");

        assert_eq!(ordering, Ordering::Less);
    }

    #[test]
    fn row_comparator_uses_stream_idx_as_tie_break() {
        let batch = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 1)]);
        let streams = vec![make_stream_state(batch.clone()), make_stream_state(batch)];
        let comparator = RowComparator::new(&streams);

        let ordering = comparator
            .compare(&make_cursor(0, 0, 0), &make_cursor(1, 0, 0))
            .expect("compare");

        assert_eq!(ordering, Ordering::Less);
    }

    #[test]
    fn row_comparator_uses_segment_row_idx_as_final_tie_break() {
        let batch = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 1)]);
        let streams = vec![make_stream_state(batch)];
        let comparator = RowComparator::new(&streams);

        let ordering = comparator
            .compare(&make_cursor(0, 0, 1), &make_cursor(0, 0, 2))
            .expect("compare");

        assert_eq!(ordering, Ordering::Less);
    }

    #[test]
    fn row_comparator_errors_when_cache_is_missing() {
        let batch = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 1)]);
        let mut streams = vec![make_stream_state(batch.clone()), make_stream_state(batch)];
        streams[0].cache = None;
        let comparator = RowComparator::new(&streams);

        let err = comparator
            .compare(&make_cursor(0, 0, 0), &make_cursor(1, 0, 0))
            .expect_err("missing cache must fail");

        assert!(matches!(
            err,
            IngestError::Shift(message) if message == "left merge cursor cache is missing"
        ));
    }

    #[test]
    fn row_comparator_errors_on_incompatible_cache_types() {
        let left_batch = logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 1)]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("cloud_account_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));
        let right_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("acc-1")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("not-a-ts")])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            ],
        )
        .expect("batch");

        let left_state = make_stream_state(left_batch);
        let right_state = InputStreamState {
            stream: futures::stream::empty::<std::result::Result<RecordBatch, icegate_queue::QueueError>>().boxed(),
            current_batch: Some(right_batch),
            current_batch_start_row: 0,
            batch_generation: 1,
            cache: Some(SortColumnCache {
                columns: vec![
                    super::CachedSortColumn::Utf8 {
                        values: StringArray::from(vec![Some("acc-1")]),
                        descending: false,
                        nulls_first: true,
                    },
                    super::CachedSortColumn::Utf8 {
                        values: StringArray::from(vec![Some("svc-1")]),
                        descending: false,
                        nulls_first: true,
                    },
                    super::CachedSortColumn::Utf8 {
                        values: StringArray::from(vec![Some("not-a-ts")]),
                        descending: false,
                        nulls_first: true,
                    },
                ],
            }),
            rows_consumed_total: 0,
        };
        let streams = vec![left_state, right_state];
        let comparator = RowComparator::new(&streams);

        let err = comparator
            .compare(&make_cursor(0, 0, 0), &make_cursor(1, 0, 0))
            .expect_err("incompatible cache must fail");

        assert!(matches!(
            err,
            IngestError::Shift(message) if message == "sort column cache types do not match across streams"
        ));
    }

    #[test]
    fn merge_heap_push_and_pop_work_for_empty_and_single_item() {
        let streams = vec![make_stream_state(logs_batch(vec![(
            Some("acc-1"),
            Some("svc-1"),
            Some(30),
            1,
        )]))];
        let comparator = RowComparator::new(&streams);
        let mut heap = MergeHeap::new();

        assert!(heap.pop(&comparator).expect("pop").is_none());

        let cursor = make_cursor(0, 0, 0);
        heap.push(cursor.clone(), &comparator).expect("push");

        assert_eq!(heap.pop(&comparator).expect("pop"), Some(cursor));
        assert!(heap.pop(&comparator).expect("pop").is_none());
    }

    #[test]
    fn merge_heap_returns_items_in_min_order() {
        let streams = vec![
            make_stream_state(logs_batch(vec![(Some("acc-2"), Some("svc-1"), Some(20), 1)])),
            make_stream_state(logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(10), 2)])),
            make_stream_state(logs_batch(vec![(Some("acc-3"), Some("svc-1"), Some(30), 3)])),
        ];
        let comparator = RowComparator::new(&streams);
        let mut heap = MergeHeap::new();

        heap.push(make_cursor(0, 0, 0), &comparator).expect("push");
        heap.push(make_cursor(1, 0, 0), &comparator).expect("push");
        heap.push(make_cursor(2, 0, 0), &comparator).expect("push");

        assert_eq!(heap.pop(&comparator).expect("pop").expect("cursor").stream_idx, 1);
        assert_eq!(heap.pop(&comparator).expect("pop").expect("cursor").stream_idx, 0);
        assert_eq!(heap.pop(&comparator).expect("pop").expect("cursor").stream_idx, 2);
    }

    #[test]
    fn merge_heap_restores_heap_property_after_mixed_operations() {
        let streams = vec![
            make_stream_state(logs_batch(vec![
                (Some("acc-3"), Some("svc-1"), Some(10), 1),
                (Some("acc-3"), Some("svc-1"), Some(5), 2),
            ])),
            make_stream_state(logs_batch(vec![
                (Some("acc-1"), Some("svc-1"), Some(10), 3),
                (Some("acc-4"), Some("svc-1"), Some(5), 4),
            ])),
            make_stream_state(logs_batch(vec![
                (Some("acc-2"), Some("svc-1"), Some(10), 5),
                (Some("acc-5"), Some("svc-1"), Some(5), 6),
            ])),
        ];
        let comparator = RowComparator::new(&streams);
        let mut heap = MergeHeap::new();

        heap.push(make_cursor(0, 0, 0), &comparator).expect("push");
        heap.push(make_cursor(1, 0, 0), &comparator).expect("push");
        heap.push(make_cursor(2, 0, 0), &comparator).expect("push");
        assert_eq!(heap.pop(&comparator).expect("pop").expect("cursor").stream_idx, 1);

        heap.push(make_cursor(1, 1, 1), &comparator).expect("push");

        assert_eq!(heap.pop(&comparator).expect("pop").expect("cursor").stream_idx, 2);
        assert_eq!(heap.pop(&comparator).expect("pop").expect("cursor").stream_idx, 0);
        assert_eq!(heap.pop(&comparator).expect("pop").expect("cursor").stream_idx, 1);
    }

    #[test]
    fn compare_option_ord_ascending_nulls_first() {
        assert_eq!(compare_option_ord(None::<i64>, Some(1), false, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(1), Some(2), false, true), Ordering::Less);
    }

    #[test]
    fn compare_option_ord_ascending_nulls_last() {
        assert_eq!(
            compare_option_ord(None::<i64>, Some(1), false, false),
            Ordering::Greater
        );
        assert_eq!(compare_option_ord(Some(1), Some(2), false, false), Ordering::Less);
    }

    #[test]
    fn compare_option_ord_descending_nulls_first() {
        assert_eq!(compare_option_ord(None::<i64>, Some(1), true, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(1), Some(2), true, true), Ordering::Greater);
    }

    #[test]
    fn compare_option_ord_descending_nulls_last() {
        assert_eq!(compare_option_ord(None::<i64>, Some(1), true, false), Ordering::Greater);
        assert_eq!(compare_option_ord(Some(1), Some(2), true, false), Ordering::Greater);
    }

    #[tokio::test]
    async fn sorted_batch_merger_preserves_logs_key_with_nulls() {
        let stream_a = futures::stream::iter(vec![Ok(logs_batch(vec![
            (None, Some("svc-1"), Some(50), 1),
            (Some("acc-1"), Some("svc-1"), None, 2),
            (Some("acc-1"), Some("svc-1"), Some(80), 3),
        ]))])
        .boxed();
        let stream_b = futures::stream::iter(vec![Ok(logs_batch(vec![
            (None, Some("svc-1"), Some(40), 4),
            (Some("acc-1"), Some("svc-1"), Some(90), 5),
            (Some("acc-1"), Some("svc-1"), Some(70), 6),
        ]))])
        .boxed();

        let merger = SortedBatchMerger::try_new(vec![Box::pin(stream_a), Box::pin(stream_b)], 8)
            .await
            .expect("merger");
        let output = merger.into_stream().try_collect::<Vec<_>>().await.expect("merged batches");

        assert_eq!(output.len(), 1);
        assert_eq!(
            sort_keys(&output[0]),
            vec![
                (None, Some("svc-1".to_string()), Some(50), 1),
                (None, Some("svc-1".to_string()), Some(40), 4),
                (Some("acc-1".to_string()), Some("svc-1".to_string()), None, 2),
                (Some("acc-1".to_string()), Some("svc-1".to_string()), Some(90), 5),
                (Some("acc-1".to_string()), Some("svc-1".to_string()), Some(80), 3),
                (Some("acc-1".to_string()), Some("svc-1".to_string()), Some(70), 6),
            ]
        );
    }

    #[tokio::test]
    async fn sorted_batch_merger_propagates_heap_comparison_errors() {
        let stream_a =
            futures::stream::iter(vec![Ok(logs_batch(vec![(Some("acc-1"), Some("svc-1"), Some(30), 1)]))]).boxed();
        let stream_b =
            futures::stream::iter(vec![Ok(logs_batch(vec![(Some("acc-2"), Some("svc-1"), Some(20), 2)]))]).boxed();
        let stream_c =
            futures::stream::iter(vec![Ok(logs_batch(vec![(Some("acc-3"), Some("svc-1"), Some(10), 3)]))]).boxed();

        let mut merger =
            SortedBatchMerger::try_new(vec![Box::pin(stream_a), Box::pin(stream_b), Box::pin(stream_c)], 8)
                .await
                .expect("merger");
        merger.streams[1].cache = None;

        let err = merger.next_batch().await.expect_err("heap comparison should return an error");

        assert!(matches!(
            err,
            IngestError::Shift(message) if message == "left merge cursor cache is missing"
        ));
    }
}
