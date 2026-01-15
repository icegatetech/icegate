//! Queue reader for reading Parquet segments from object storage.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::TryStreamExt;
use icegate_common::retrier::{Retrier, RetrierConfig};
use object_store::{ObjectStore, path::Path};
use parquet::{
    arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder},
    errors::ParquetError,
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
    file::statistics::Statistics,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    Topic,
    error::{QueueError, Result},
    segment::SegmentId,
};

// Reference to record batches (row groups) inside a WAL segment.
//
// Schema:
// `SegmentsPlan`
//   groups: [`GroupedSegmentsPlan`]
//   row_groups_total: usize
//     segments: [`SegmentRecordBatchIdxs`]
//     row_groups_total: usize
//       `record_batch_idxs`: [usize]

/// Result of planning segments record batches for processing.
#[derive(Debug, Clone)]
pub struct SegmentsPlan {
    /// Grouped record batches in segments keyed by a column value.
    pub groups: Vec<GroupedSegmentsPlan>,
    /// Last segment offset observed in the planned segments.
    pub last_segment_offset: Option<u64>,
    /// Number of segments scanned in the plan.
    pub segments_count: usize,
    /// Total number of row groups across all planned segments.
    pub record_batches_total: usize,
}

/// Grouped record batches in segments keyed by a column value.
#[derive(Debug, Clone)]
pub struct GroupedSegmentsPlan {
    /// Grouping key from the requested column.
    pub group_col_val: String,
    /// WAL segment references for the group.
    pub segments: Vec<SegmentRecordBatchIdxs>,
    /// Number of segments in the group.
    pub segments_count: usize,
    /// Total number of row groups in the grouped segments.
    pub record_batches_total: usize,
}

/// Record batches indexes (row group) in segment (WAL file).
#[derive(Debug, Clone)]
pub struct SegmentRecordBatchIdxs {
    /// WAL segment offset.
    pub segment_offset: u64,
    /// Row group indices inside the segment.
    pub record_batch_idxs: Vec<usize>,
}

struct RowGroupsInSegments {
    segments: BTreeMap<u64, Vec<usize>>,
    row_group_count: usize,
}

impl RowGroupsInSegments {
    const fn new() -> Self {
        Self {
            segments: BTreeMap::new(),
            row_group_count: 0,
        }
    }

    fn push(&mut self, offset: u64, row_groups: Vec<usize>) -> Result<()> {
        if row_groups.is_empty() {
            return Ok(());
        }
        self.row_group_count = self
            .row_group_count
            .checked_add(row_groups.len())
            .ok_or_else(|| QueueError::Metadata("row group count overflow".to_string()))?;
        self.segments.entry(offset).or_default().extend(row_groups);
        Ok(())
    }

    fn take(&mut self) -> (Vec<SegmentRecordBatchIdxs>, usize) {
        let row_group_count = self.row_group_count;
        self.row_group_count = 0;
        let segments = std::mem::take(&mut self.segments);
        let segments = segments
            .into_iter()
            .map(|(offset, row_groups)| SegmentRecordBatchIdxs {
                segment_offset: offset,
                record_batch_idxs: row_groups,
            })
            .collect();
        (segments, row_group_count)
    }
}

/// Queue reader dependency surface for shift executors.
#[async_trait]
pub trait QueueReader: Send + Sync {
    /// Build a plan of record batches to process.
    async fn plan_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        group_by_column_name: &str,
        max_record_batches_per_task: usize,
        cancel_token: &CancellationToken,
    ) -> Result<SegmentsPlan>;

    /// Read record batches for a specific segment.
    async fn read_segment(
        &self,
        topic: &Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>>;
}

/// Queue reader for reading Parquet segments from object storage.
///
/// Provides methods to list segments and read record batches.
pub struct ParquetQueueReader {
    /// Base path for queue segments.
    base_path: String,

    /// Object store backend.
    store: Arc<dyn ObjectStore>,

    retrier: Retrier,
}

impl ParquetQueueReader {
    /// Creates a new queue reader.
    pub fn new(base_path: impl Into<String>, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            base_path: base_path.into(),
            store,
            retrier: Retrier::new(RetrierConfig::default()),
        }
    }

    /// Lists segments for a topic starting from a given offset.
    ///
    /// Returns segment IDs sorted by offset.
    pub async fn list_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<Vec<SegmentId>> {
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
                async move {
                    let list_stream = store.list_with_offset(Some(&prefix), &offset_path);
                    let items: Vec<_> = list_stream.try_collect().await?;
                    Ok(items)
                }
            })
            .await?;

        debug!("list_segments_from: found {} items", items.len());

        let mut segments: Vec<SegmentId> = items
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
                        Ok(seg) => {
                            debug!("list_segments_from: parsed segment {:?}", seg);
                            Some(seg)
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
        segments.sort_by_key(|s| s.offset);

        Ok(segments)
    }

    /// Lists segments and plans row groups grouped by column value.
    pub async fn plan_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        group_by_column_name: &str,
        max_row_groups_per_group: usize,
        cancel_token: &CancellationToken,
    ) -> Result<SegmentsPlan> {
        let segments = self.list_segments(topic, start_offset, cancel_token).await?;
        if segments.is_empty() {
            return Ok(SegmentsPlan {
                groups: Vec::new(),
                last_segment_offset: None,
                segments_count: 0,
                record_batches_total: 0,
            });
        }

        let last_offset = segments.last().map(|segment| segment.offset);
        let (groups, row_groups_total) = self
            .plan_record_batches(
                segments.as_slice(),
                group_by_column_name,
                max_row_groups_per_group,
                cancel_token,
            )
            .await?;

        Ok(SegmentsPlan {
            groups,
            last_segment_offset: last_offset,
            segments_count: segments.len(),
            record_batches_total: row_groups_total,
        })
    }

    /// Reads specific record batches (by index) from a segment by topic and offset.
    pub async fn read_segment(
        &self,
        topic: &Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        if record_batch_idxs.is_empty() {
            return Ok(Vec::new());
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
            .with_batch_size(8192)
            .with_row_groups(record_batch_idxs.to_vec())
            .build()?;

        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        Ok(batches)
    }

    /// Plan to read record batches grouped by column value across a list of segments.
    async fn plan_record_batches(
        &self,
        segments: &[SegmentId],
        group_by_column_name: &str,
        max_row_groups_per_group: usize,
        cancel_token: &CancellationToken,
    ) -> Result<(Vec<GroupedSegmentsPlan>, usize)> {
        if max_row_groups_per_group == 0 {
            return Err(QueueError::Config(
                "max_row_groups_per_group must be greater than zero".to_string(),
            ));
        }

        let mut grouped_chunks: HashMap<String, RowGroupsInSegments> = HashMap::new();
        let mut grouped = Vec::new();
        let mut row_groups_total = 0usize;

        for segment in segments {
            let wal_offset = segment.offset;
            let parquet_meta = self.read_parquet_metadata(&segment.topic, wal_offset, cancel_token).await?;
            let schema = parquet_meta.file_metadata().schema_descr();
            let column_idx = schema
                .columns()
                .iter()
                .position(|col| col.name() == group_by_column_name)
                .ok_or_else(|| {
                    QueueError::Metadata(format!(
                        "column '{group_by_column_name}' not found in parquet schema for segment {wal_offset}"
                    ))
                })?;

            let mut saw_row_groups = false;
            for (row_group_idx, row_group) in parquet_meta.row_groups().iter().enumerate() {
                let group_key =
                    Self::group_key_from_row_group(row_group, column_idx, group_by_column_name, row_group_idx)?;
                saw_row_groups = true;

                let chunk = grouped_chunks.entry(group_key.clone()).or_insert_with(RowGroupsInSegments::new);
                let row_groups = vec![row_group_idx];
                row_groups_total = row_groups_total
                    .checked_add(row_groups.len())
                    .ok_or_else(|| QueueError::Metadata("row group total overflow".to_string()))?;
                chunk.push(wal_offset, row_groups)?;
                if chunk.row_group_count == max_row_groups_per_group {
                    let (segments, row_groups_total) = chunk.take();
                    let segments_count = segments.len();
                    grouped.push(GroupedSegmentsPlan {
                        group_col_val: group_key,
                        segments,
                        segments_count,
                        record_batches_total: row_groups_total,
                    });
                }
            }

            if !saw_row_groups {
                return Err(QueueError::Metadata(format!(
                    "no row groups found in WAL segment {wal_offset}"
                )));
            }
        }

        for (group_key, mut chunk) in grouped_chunks {
            let (segments, row_groups_total) = chunk.take();
            if segments.is_empty() {
                continue;
            }
            let segments_count = segments.len();
            grouped.push(GroupedSegmentsPlan {
                group_col_val: group_key,
                segments,
                segments_count,
                record_batches_total: row_groups_total,
            });
        }

        Ok((grouped, row_groups_total))
    }

    /// Reads Parquet footer metadata using range requests (no full file download).
    async fn read_parquet_metadata(
        &self,
        topic: &Topic,
        offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<ParquetMetaData> {
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
        let file_size = object_meta.size;

        if file_size < 8 {
            return Err(QueueError::Parquet(ParquetError::EOF(
                "file size is smaller than parquet footer".to_string(),
            )));
        }

        let file_size_usize = usize::try_from(file_size)
            .map_err(|_| QueueError::Metadata(format!("file size {file_size} exceeds addressable size")))?;
        let mut tail_len = std::cmp::min(file_size_usize, 64 * 1024);

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
        Fut: std::future::Future<Output = Result<T>>,
    {
        let result = self
            .retrier
            .retry::<_, _, Result<T>, QueueError>(
                || {
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

fn group_key_from_row_group(
    row_group: &parquet::file::metadata::RowGroupMetaData,
    column_idx: usize,
    column_name: &str,
    row_group_idx: usize,
) -> Result<String> {
    let column = row_group.column(column_idx);
    let stats = column.statistics().ok_or_else(|| {
        QueueError::Metadata(format!(
            "missing statistics for '{column_name}' in row group {row_group_idx}"
        ))
    })?;

    match stats {
        Statistics::ByteArray(byte_stats) => {
            let min = byte_stats.min_bytes_opt().ok_or_else(|| {
                QueueError::Metadata(format!(
                    "missing min statistic for '{column_name}' in row group {row_group_idx}"
                ))
            })?;
            let max = byte_stats.max_bytes_opt().ok_or_else(|| {
                QueueError::Metadata(format!(
                    "missing max statistic for '{column_name}' in row group {row_group_idx}"
                ))
            })?;

            if min != max {
                return Err(QueueError::Metadata(format!(
                    "row group {row_group_idx} contains multiple values for '{column_name}'"
                )));
            }

            let value = std::str::from_utf8(min).map_err(|e| {
                QueueError::Metadata(format!(
                    "invalid utf8 in '{column_name}' stats for row group {row_group_idx}: {e}"
                ))
            })?;

            if value.is_empty() {
                return Err(QueueError::Metadata(format!(
                    "empty value in stats for '{column_name}' row group {row_group_idx}"
                )));
            }

                Ok(value.to_string())
            }
            _ => Err(QueueError::Metadata(format!(
                "unsupported stats type for '{column_name}' in row group {row_group_idx}"
            ))),
        }
    }
}

#[async_trait]
impl QueueReader for ParquetQueueReader {
    async fn plan_segments(
        &self,
        topic: &Topic,
        start_offset: u64,
        group_by_column_name: &str,
        max_record_batches_per_task: usize,
        cancel_token: &CancellationToken,
    ) -> Result<SegmentsPlan> {
        ParquetQueueReader::plan_segments(
            self,
            topic,
            start_offset,
            group_by_column_name,
            max_record_batches_per_task,
            cancel_token,
        )
        .await
    }

    async fn read_segment(
        &self,
        topic: &Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        cancel_token: &CancellationToken,
    ) -> Result<Vec<RecordBatch>> {
        ParquetQueueReader::read_segment(self, topic, offset, record_batch_idxs, cancel_token).await
    }
}
