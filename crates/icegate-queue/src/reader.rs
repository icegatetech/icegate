//! Queue reader for reading Parquet segments from object storage.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    errors::ParquetError,
    file::reader::{FileReader, SerializedFileReader},
    file::metadata::{ParquetMetaData, ParquetMetaDataReader},
};
use tracing::debug;

use crate::{
    error::Result,
    segment::{SegmentId, SegmentMetadata, SegmentStatus},
    Topic,
};

// TODO(crit): check
// TODO(crit): тут же не нужен recover?

/// Queue reader for reading Parquet segments from object storage.
///
/// Provides methods to read segments by topic and offset range, with optional row group filtering.
pub struct QueueReader<S: ObjectStore> {
    /// Base path for queue segments.
    base_path: String,

    /// Object store backend.
    store: Arc<S>,

    /// Batch size for reading.
    batch_size: usize,
}

impl<S: ObjectStore> QueueReader<S> {
    /// Creates a new queue reader.
    pub fn new(base_path: impl Into<String>, store: Arc<S>) -> Self {
        Self {
            base_path: base_path.into(),
            store,
            batch_size: 8192,
        }
    }

    /// Sets the batch size for reading.
    #[must_use]
    pub const fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Reads a single segment by topic and offset.
    ///
    /// Returns all record batches from the segment.
    pub async fn read_segment(&self, topic: &Topic, offset: u64) -> Result<Vec<RecordBatch>> {
        let segment_id = SegmentId::new(topic, offset);
        let data = self.fetch_segment_data(&segment_id).await?;
        self.parse_parquet(&data)
    }

    /// Reads raw parquet bytes for a segment by topic and offset.
    pub async fn read_segment_data(&self, topic: &Topic, offset: u64) -> Result<Bytes> {
        let segment_id = SegmentId::new(topic, offset);
        self.fetch_segment_data(&segment_id).await
    }

    /// Reads specific row groups from a segment by topic and offset.
    ///
    /// Returns record batches only for the requested row groups.
    pub async fn read_segment_row_groups(
        &self,
        topic: &Topic,
        offset: u64,
        row_groups: &[usize],
    ) -> Result<Vec<RecordBatch>> {
        // TODO(crit): сейчас читаем весь файл. Нужно строить ChunkReader поверх ObjectStore (range fetch), передавать его в parquet reader/metadata reader.

        if row_groups.is_empty() {
            return Ok(Vec::new());
        }

        let segment_id = SegmentId::new(topic, offset);
        let data = self.fetch_segment_data(&segment_id).await?;
        self.parse_parquet_with_row_groups(&data, row_groups.to_vec())
    }

    /// Lists segments for a topic starting from a given offset.
    ///
    /// Returns segment IDs sorted by offset.
    pub async fn list_segments_from(&self, topic: &Topic, start_offset: u64) -> Result<Vec<SegmentId>> {
        // TODO(crit): что если много файлов скопилось - надо как-то разделять
        // TODO(crit): посмотреть тут про порядок
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
            let segment_path = SegmentId::new(topic, start_offset - 1).to_path();
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

        // TODO(crit): если возвращается стрим, может прокидывать наверх тоже стрим? И сверху решать когда закончить?
        let list_stream = self.store.list_with_offset(Some(&prefix), &offset_path);
        let items: Vec<_> = list_stream.try_collect().await?;

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
                    match SegmentId::from_path(&relative_path_obj) {
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

    /// Reads segment metadata directly from parquet file.
    pub async fn read_metadata(&self, topic: &Topic, offset: u64) -> Result<SegmentMetadata> {
        // TODO(crit): если разбивать shift таски по партицям, то нужно читать только мету, кажется сейчас читается весь файл
        let segment_id = SegmentId::new(topic, offset);
        let path = self.segment_path(&segment_id);

        // Get object metadata (size, last_modified)
        let object_meta = self.store.head(&path).await?;

        // Get parquet data and parse metadata
        let data = self.store.get(&path).await?.bytes().await?;
        let parquet_reader = SerializedFileReader::new(data)?;
        let parquet_meta = parquet_reader.metadata();

        let record_count = parquet_meta.file_metadata().num_rows();
        let row_group_count = parquet_meta.num_row_groups();

        #[allow(clippy::cast_sign_loss)]
        let created_at = object_meta.last_modified.timestamp_millis() as u128;

        Ok(SegmentMetadata {
            topic: topic.clone(),
            offset,
            record_count,
            size_bytes: object_meta.size,
            row_group_count,
            status: SegmentStatus::Complete,
            schema_fingerprint: None,
            created_at,
        })
    }

    /// Reads Parquet footer metadata using range requests (no full file download).
    pub async fn read_parquet_metadata(&self, topic: &Topic, offset: u64) -> Result<ParquetMetaData> {
        // TODO(crit): этим методом мы нарушаем концепцию queue, т.к. сейчас она не зависит от данных. С другой стороны мы в Shift знаем, что это parquet файлы.
        // TODO(crit): проверить на оптимальность чтения

        let segment_id = SegmentId::new(topic, offset);
        let path = self.segment_path(&segment_id);
        let object_meta = self.store.head(&path).await?;
        let file_size = object_meta.size;

        if file_size < 8 {
            return Err(crate::error::QueueError::Parquet(ParquetError::EOF(
                "file size is smaller than parquet footer".to_string(),
            )));
        }

        let mut tail_len = std::cmp::min(file_size as usize, 64 * 1024);

        loop {
            let start = file_size - tail_len as u64;
            let data = self.store.get_range(&path, start..file_size).await?;
            let bytes = Bytes::from(data);
            let mut reader = ParquetMetaDataReader::new();

            match reader.try_parse_sized(&bytes, file_size) {
                Ok(()) => return reader.finish().map_err(crate::error::QueueError::Parquet),
                Err(ParquetError::NeedMoreData(need)) => {
                    if need as u64 > file_size {
                        return Err(crate::error::QueueError::Parquet(ParquetError::NeedMoreData(need)));
                    }
                    if need <= tail_len {
                        return Err(crate::error::QueueError::Parquet(ParquetError::NeedMoreData(need)));
                    }
                    tail_len = need;
                }
                Err(e) => return Err(crate::error::QueueError::Parquet(e)),
            }
        }
    }

    /// Fetches raw segment data from object storage.
    async fn fetch_segment_data(&self, segment_id: &SegmentId) -> Result<Bytes> {
        let path = self.segment_path(segment_id);
        let result = self.store.get(&path).await?;
        let data = result.bytes().await?;
        Ok(data)
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

    /// Parses Parquet data into record batches.
    fn parse_parquet(&self, data: &Bytes) -> Result<Vec<RecordBatch>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())?
            .with_batch_size(self.batch_size)
            .build()?;

        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(batches)
    }

    /// Parses Parquet data into record batches with row group filtering.
    fn parse_parquet_with_row_groups(&self, data: &Bytes, row_groups: Vec<usize>) -> Result<Vec<RecordBatch>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())?
            .with_batch_size(self.batch_size)
            .with_row_groups(row_groups)
            .build()?;

        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(batches)
    }
}
