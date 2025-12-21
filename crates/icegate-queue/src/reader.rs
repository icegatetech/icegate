//! Queue reader for reading Parquet segments from object storage.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    file::reader::{FileReader, SerializedFileReader},
};
use tracing::debug;

use crate::{
    error::Result,
    segment::{SegmentId, SegmentMetadata, SegmentStatus},
    Topic,
};

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

    /// Lists segments for a topic starting from a given offset.
    ///
    /// Returns segment IDs sorted by offset.
    pub async fn list_segments_from(&self, topic: &Topic, start_offset: u64) -> Result<Vec<SegmentId>> {
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
            prefix.clone()
        } else {
            let segment_path = format!(
                "{}/{}.parquet",
                topic,
                SegmentId::new(topic, start_offset - 1).offset_string()
            );
            if self.base_path.is_empty() {
                Path::from(segment_path)
            } else {
                Path::from(format!("{}/{}", self.base_path, segment_path))
            }
        };

        debug!(
            "list_segments_from: base_path={:?}, topic={}, start_offset={}, prefix={:?}, offset_path={:?}",
            self.base_path, topic, start_offset, prefix, offset_path
        );

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
                        },
                        Err(e) => {
                            debug!(
                                "list_segments_from: failed to parse segment from {:?}: {}",
                                relative_path_obj, e
                            );
                            None
                        },
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

        Ok(SegmentMetadata {
            topic: topic.clone(),
            offset,
            record_count,
            size_bytes: object_meta.size,
            row_group_count,
            status: SegmentStatus::Complete,
            schema_fingerprint: None,
        })
    }

    /// Reads all segments from `start_offset` to the latest.
    ///
    /// Returns segments in offset order with their record batches.
    pub async fn read_from_offset(&self, topic: &Topic, start_offset: u64) -> Result<Vec<(u64, Vec<RecordBatch>)>> {
        let segments = self.list_segments_from(topic, start_offset).await?;

        let mut results = Vec::with_capacity(segments.len());
        for segment_id in segments {
            let batches = self.read_segment(topic, segment_id.offset).await?;
            results.push((segment_id.offset, batches));
        }

        Ok(results)
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
        let relative = format!("{}/{}.parquet", segment_id.topic, segment_id.offset_string());
        if self.base_path.is_empty() {
            Path::from(relative)
        } else {
            Path::from(format!("{}/{}", self.base_path, relative))
        }
    }

    /// Parses Parquet data into record batches.
    fn parse_parquet(&self, data: &Bytes) -> Result<Vec<RecordBatch>> {
        let reader =
            ParquetRecordBatchReaderBuilder::try_new(data.clone())?.with_batch_size(self.batch_size).build()?;

        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(batches)
    }

    /// Gets a reference to the object store.
    pub const fn store(&self) -> &Arc<S> {
        &self.store
    }

    /// Gets the base path.
    pub fn base_path(&self) -> &str {
        &self.base_path
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use object_store::memory::InMemory;

    use super::*;
    use crate::{QueueConfig, QueueWriter};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn test_batch(values: &[i32]) -> RecordBatch {
        let schema = test_schema();
        let tenants: Vec<&str> = values.iter().map(|_| "acme").collect();
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(tenants)),
            Arc::new(Int32Array::from(values.to_vec())),
        ])
        .unwrap()
    }

    async fn write_test_segment(writer: &QueueWriter, topic: &str, values: &[i32]) -> u64 {
        let batch = test_batch(values);
        let (offset, _count) = writer.write_batch(&topic.to_string(), batch, None).await.unwrap();
        offset
    }

    #[tokio::test]
    async fn test_read_segment() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue");
        let writer = QueueWriter::new(config, store.clone());

        // Write a segment
        let _offset = write_test_segment(&writer, "logs", &[1, 2, 3, 4]).await;

        // Read it back
        let reader = QueueReader::new("queue", store);
        let batches = reader.read_segment(&"logs".to_string(), 0).await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 4);
    }

    #[tokio::test]
    async fn test_list_segments() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue");
        let writer = QueueWriter::new(config, store.clone());

        // Write segments
        write_test_segment(&writer, "logs", &[1]).await;
        write_test_segment(&writer, "logs", &[2]).await;
        write_test_segment(&writer, "events", &[3]).await;

        let reader = QueueReader::new("queue", store);

        // List logs segments
        let segments = reader.list_segments_from(&"logs".to_string(), 0).await.unwrap();
        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].offset, 0);
        assert_eq!(segments[1].offset, 1);

        // List events segments
        let segments = reader.list_segments_from(&"events".to_string(), 0).await.unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].offset, 0);
    }

    #[tokio::test]
    async fn test_read_metadata() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue");
        let writer = QueueWriter::new(config, store.clone());

        write_test_segment(&writer, "logs", &[1, 2, 3, 4]).await;

        let reader = QueueReader::new("queue", store);
        let metadata = reader.read_metadata(&"logs".to_string(), 0).await.unwrap();

        assert_eq!(metadata.topic, "logs");
        assert_eq!(metadata.offset, 0);
        assert_eq!(metadata.record_count, 4);
    }
}
