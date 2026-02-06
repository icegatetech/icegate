//! Batch accumulator for efficient WAL writes.
//!
//! Accumulates `RecordBatches` per topic until a flush threshold is reached,
//! then concatenates them for a single write operation.

use std::time::Instant;

use arrow::{compute::concat_batches, record_batch::RecordBatch};
use tokio::sync::oneshot;

use crate::{channel::WriteResult, config::QueueConfig, error::Result};

/// A pending batch with its response channel and optional trace context.
#[derive(Debug)]
pub struct PendingBatch {
    /// The record batch to be written.
    pub(crate) batch: RecordBatch,
    /// Response channel for this batch.
    pub(crate) response_tx: oneshot::Sender<WriteResult>,
    /// Optional W3C trace context (traceparent format).
    pub(crate) trace_context: Option<String>,
}

/// Accumulator for a single topic.
///
/// Collects `RecordBatches` and their response channels until a flush
/// threshold is reached.
#[derive(Debug)]
pub struct TopicAccumulator {
    /// Pending batches with their response channels and trace contexts.
    pending: Vec<PendingBatch>,

    /// Total record count across all batches.
    total_records: usize,

    /// Estimated total bytes across all batches.
    total_bytes: usize,

    /// Time when accumulator was created or last flushed.
    last_flush: Instant,
}

impl Default for TopicAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicAccumulator {
    /// Creates a new empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: Vec::new(),
            total_records: 0,
            total_bytes: 0,
            last_flush: Instant::now(),
        }
    }

    /// Adds a batch and its response channel to the accumulator.
    pub fn add(
        &mut self,
        batch: RecordBatch,
        response_tx: oneshot::Sender<WriteResult>,
        trace_context: Option<String>,
    ) {
        self.total_records += batch.num_rows();
        self.total_bytes += Self::estimate_batch_size(&batch);
        self.pending.push(PendingBatch {
            batch,
            response_tx,
            trace_context,
        });
    }

    /// Checks if the accumulator should be flushed based on config thresholds.
    #[must_use]
    pub fn should_flush(&self, config: &QueueConfig) -> bool {
        if self.pending.is_empty() {
            return false;
        }

        // Check record count threshold
        let row_flush_limit = config.flush_record_limit();
        if self.total_records >= row_flush_limit {
            return true;
        }

        // Check byte size threshold
        if self.total_bytes >= config.write.max_bytes_per_flush {
            return true;
        }

        // Check time threshold
        #[allow(clippy::cast_possible_truncation)] // Duration will not exceed u64 in practice
        let elapsed_ms = self.last_flush.elapsed().as_millis() as u64;
        if elapsed_ms >= config.write.flush_interval_ms {
            return true;
        }

        false
    }

    /// Returns true if the accumulator has pending batches.
    #[cfg(test)]
    #[must_use]
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Returns the number of pending batches.
    #[cfg(test)]
    #[must_use]
    pub fn batch_count(&self) -> usize {
        self.pending.len()
    }

    /// Returns the number of pending batches.
    #[must_use]
    pub fn pending_batches(&self) -> usize {
        self.pending.len()
    }

    /// Returns the total record count.
    #[cfg(test)]
    #[must_use]
    pub const fn total_records(&self) -> usize {
        self.total_records
    }

    /// Returns the total pending record count.
    #[must_use]
    pub const fn pending_records(&self) -> usize {
        self.total_records
    }

    /// Returns the estimated pending bytes.
    #[must_use]
    pub const fn pending_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Takes all accumulated batches and responses, resetting the accumulator.
    ///
    /// Returns all pending batches with their response channels and trace contexts.
    pub fn take(&mut self) -> Vec<PendingBatch> {
        self.total_records = 0;
        self.total_bytes = 0;
        self.last_flush = Instant::now();
        std::mem::take(&mut self.pending)
    }

    /// Concatenates multiple `RecordBatches` into a single batch.
    ///
    /// All batches must have the same schema.
    pub fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Err(crate::error::QueueError::Config(
                "No batches to concatenate".to_string(),
            ));
        }

        let schema = batches[0].schema();
        let concatenated = concat_batches(&schema, batches)?;
        Ok(concatenated)
    }

    /// Estimates the size of a `RecordBatch` in bytes.
    ///
    /// This is an approximation based on array memory sizes.
    fn estimate_batch_size(batch: &RecordBatch) -> usize {
        batch.columns().iter().map(|col| col.get_array_memory_size()).sum()
    }

    /// Sends success results to all pending response channels.
    ///
    /// All responses share the same `trace_context`, which should be extracted
    /// from the flush operation span that performed the actual write.
    pub fn send_success(pending: Vec<PendingBatch>, offset: u64, total_records: usize, trace_context: Option<&String>) {
        for p in pending {
            let _ = p
                .response_tx
                .send(WriteResult::success(offset, total_records, trace_context.cloned()));
        }
    }

    /// Sends failure results to all pending response channels.
    ///
    /// All responses share the same `trace_context`, which should be extracted
    /// from the flush operation span that encountered the error.
    pub fn send_failure(pending: Vec<PendingBatch>, reason: &str, trace_context: Option<&String>) {
        for p in pending {
            let _ = p.response_tx.send(WriteResult::failed(reason, trace_context.cloned()));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    fn test_batch(rows: usize) -> RecordBatch {
        let schema = test_schema();
        let tenant_ids: Vec<&str> = (0..rows).map(|_| "acme").collect();
        let values: Vec<i32> = (0..rows).map(|i| i as i32).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(tenant_ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .expect("batch creation")
    }

    #[test]
    fn test_accumulator_add() {
        let mut acc = TopicAccumulator::new();
        let (tx, _rx) = oneshot::channel();

        acc.add(test_batch(100), tx, None);

        assert_eq!(acc.batch_count(), 1);
        assert_eq!(acc.total_records(), 100);
        assert!(acc.has_pending());
    }

    #[test]
    fn test_accumulator_take() {
        let mut acc = TopicAccumulator::new();
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();

        acc.add(test_batch(100), tx1, None);
        acc.add(test_batch(50), tx2, None);

        assert_eq!(acc.total_records(), 150);

        let pending = acc.take();

        assert_eq!(pending.len(), 2);
        assert_eq!(acc.batch_count(), 0);
        assert_eq!(acc.total_records(), 0);
        assert!(!acc.has_pending());
    }

    #[test]
    fn test_should_flush_records() {
        let mut acc = TopicAccumulator::new();
        let config = QueueConfig::new("test")
            .with_max_row_group_size(50)
            .with_records_per_flush_multiplier(2);

        let (tx, _rx) = oneshot::channel();
        acc.add(test_batch(50), tx, None);
        assert!(!acc.should_flush(&config));

        let (tx, _rx) = oneshot::channel();
        acc.add(test_batch(60), tx, None);
        assert!(acc.should_flush(&config)); // 110 >= (50 * 2)
    }

    #[test]
    fn test_should_flush_empty() {
        let acc = TopicAccumulator::new();
        let config = QueueConfig::new("test");

        assert!(!acc.should_flush(&config));
    }

    #[test]
    fn test_concat_batches() {
        let batch1 = test_batch(100);
        let batch2 = test_batch(50);

        let concatenated = TopicAccumulator::concat_batches(&[batch1, batch2]).expect("concat");

        assert_eq!(concatenated.num_rows(), 150);
    }

    #[tokio::test]
    async fn test_send_success() {
        let (tx1, rx1) = oneshot::channel();
        let (tx2, rx2) = oneshot::channel();

        let pending = vec![
            PendingBatch {
                batch: test_batch(10),
                response_tx: tx1,
                trace_context: None,
            },
            PendingBatch {
                batch: test_batch(10),
                response_tx: tx2,
                trace_context: None,
            },
        ];

        let trace_ctx = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());
        TopicAccumulator::send_success(pending, 42, 100, trace_ctx.as_ref());

        let result1 = rx1.await.expect("recv");
        let result2 = rx2.await.expect("recv");

        assert!(result1.is_success());
        assert_eq!(result1.offset(), Some(42));
        assert_eq!(result1.records(), Some(100));
        assert_eq!(
            result1.trace_context(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );

        assert!(result2.is_success());
        assert_eq!(
            result2.trace_context(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
    }

    #[tokio::test]
    async fn test_send_failure() {
        let (tx, rx) = oneshot::channel();

        let pending = vec![PendingBatch {
            batch: test_batch(10),
            response_tx: tx,
            trace_context: None,
        }];

        let trace_ctx = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());
        TopicAccumulator::send_failure(pending, "test error", trace_ctx.as_ref());

        let result = rx.await.expect("recv");
        assert!(result.is_failed());
        assert_eq!(result.reason(), Some("test error"));
        assert_eq!(
            result.trace_context(),
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
        );
    }
}
