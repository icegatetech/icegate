//! # icegate-queue
//!
//! A generic WAL-based data queue with Parquet on object storage.
//!
//! This crate provides a durable, sequential queue backed by Parquet files on
//! S3-compatible object storage. It is designed for high-throughput data
//! ingestion with exactly-once semantics.
//!
//! ## Features
//!
//! - **Durable writes**: Data is persisted to object storage before acknowledgment
//! - **Exactly-once semantics**: Uses `If-None-Match` for atomic writes
//! - **Sequential ordering**: Monotonically increasing offsets per topic
//! - **Row group partitioning**: Optional grouping by column for efficient reads
//! - **Backpressure**: Bounded channels prevent memory overflow
//! - **Recovery**: Offset recovery is part of starting the writer, and fails the
//!   start when the queue's history cannot be resumed
//! - **Retention**: [`QueueCleaner`] deletes a topic's tail below a bound the
//!   caller derives
//!
//! ## Example
//!
//! ```ignore
//! use icegate_queue::{PreparedWalRowGroup, QueueConfig, QueueWriter, WriteRequest, Topic, channel};
//! use arrow::record_batch::RecordBatch;
//! use tokio::sync::oneshot;
//!
//! // Create queue writer and start it: `start` recovers the per-topic offset
//! // counters first and fails instead of starting on a queue whose history
//! // cannot be resumed.
//! let config = QueueConfig::new("s3://bucket/queue");
//! let (tx, rx) = channel(config.common.channel_capacity);
//! let writer = QueueWriter::new(config, object_store);
//! let handle = writer.start(rx).await?;
//!
//! // Send write request
//! let (response_tx, response_rx) = oneshot::channel();
//! tx.send(WriteRequest {
//!     topic: "logs".to_string(),
//!     row_groups: vec![PreparedWalRowGroup::new(record_batch)],
//!     response_tx,
//!     trace_context: None,
//! }).await?;
//!
//! // Wait for result
//! let result = response_rx.await?;
//! ```

mod accumulator;
mod channel;
mod cleaner;
mod config;
mod error;
mod extract;
mod reader;
mod segment;
mod writer;

pub use channel::{PreparedWalRowGroup, Topic, WriteChannel, WriteReceiver, WriteRequest, WriteResult, channel};
pub use cleaner::{DeleteLimits, DeleteSummary, QueueCleaner};
pub use config::{CompressionCodec, QueueConfig};
pub use error::{QueueError, Result};
pub use extract::{ExtractField, ExtractedValue, FieldExtractor};
pub use reader::{
    ListedSegment, ParquetQueueReader, QueueReader, RecordBatchStream, RowGroupPlanEntry, SegmentFile, SegmentsPlan,
};
pub use segment::SegmentId;
pub use writer::{CommittedOffsetsByTopic, NoopQueueWriterEvents, QueueWriter, QueueWriterEvents, WriteBatchOutcome};

/// WAL footer key for per-row-group opaque payloads.
///
/// Used by the queue writer to store boundary ranges and similar payloads.
/// Callers wiring [`FieldExtractor::FileKeyValueRowGroupPayload`] should pass
/// this constant rather than hard-coding the string.
pub const WAL_ROW_GROUP_METADATA_KEY: &str = "icegate.queue.row_group_metadata.v1";
