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
//! - **Recovery**: Automatic offset recovery on restart
//!
//! ## Example
//!
//! ```ignore
//! use icegate_queue::{QueueConfig, QueueWriter, WriteRequest, Topic};
//! use arrow::record_batch::RecordBatch;
//! use tokio::sync::oneshot;
//!
//! // Create queue writer
//! let config = QueueConfig::new("s3://bucket/queue");
//! let (tx, writer) = QueueWriter::new(config, object_store).await?;
//!
//! // Send write request
//! let (response_tx, response_rx) = oneshot::channel();
//! tx.send(WriteRequest {
//!     topic: "logs".to_string(),
//!     batch: record_batch,
//!     group_by_column: Some("tenant_id".to_string()),
//!     response_tx,
//! }).await?;
//!
//! // Wait for result
//! let result = response_rx.await?;
//! ```

mod accumulator;
mod channel;
mod config;
mod error;
mod reader;
mod segment;
mod writer;

pub use channel::{channel, Topic, WriteChannel, WriteReceiver, WriteRequest, WriteResult};
pub use config::{CompressionCodec, QueueConfig};
pub use error::{QueueError, Result};
pub use reader::QueueReader;
pub use segment::{SegmentId, SegmentMetadata, SegmentStatus};
pub use writer::QueueWriter;
