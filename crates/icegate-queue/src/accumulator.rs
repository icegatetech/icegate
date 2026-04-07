//! Batch accumulator for efficient WAL writes.
//!
//! Accumulates logical write requests per topic until a flush threshold is
//! reached, then passes their batches directly to the Parquet writer as
//! individual row groups.

use std::time::Instant;

use arrow::record_batch::RecordBatch;
use tokio::sync::oneshot;
use tracing::trace;

use crate::{
    channel::{PreparedWalRowGroup, WriteResult},
    config::QueueConfig,
};

/// A pending logical write request with its response channel and trace context.
#[derive(Debug)]
struct PendingRequest {
    /// The record batches to be written.
    row_groups: Vec<PreparedWalRowGroup>,
    /// Total rows across all batches in this request.
    records: usize,
    /// Response channel for this request.
    response_tx: oneshot::Sender<WriteResult>,
    /// Optional W3C trace context (traceparent format).
    trace_context: Option<String>,
}

/// Frozen snapshot of requests accumulated for a single flush operation.
#[derive(Debug, Default)]
pub struct PendingFlush {
    requests: Vec<PendingRequest>,
}

impl PendingFlush {
    /// Returns true when there are no requests to flush.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    /// Returns the number of logical requests in this flush.
    #[must_use]
    pub fn request_count(&self) -> usize {
        self.requests.len()
    }

    /// Returns the number of physical row groups in this flush.
    #[must_use]
    pub fn row_group_count(&self) -> usize {
        self.requests.iter().map(|request| request.row_groups.len()).sum()
    }

    /// Returns all batches in flush order.
    #[must_use]
    pub fn row_groups(&self) -> Vec<PreparedWalRowGroup> {
        // TODO(low): remove copy
        let mut row_groups = Vec::with_capacity(self.row_group_count());
        for request in &self.requests {
            row_groups.extend(request.row_groups.iter().cloned());
        }
        row_groups
    }

    /// Returns request trace contexts without exposing internal request layout.
    #[must_use]
    pub fn trace_contexts(&self) -> Vec<&str> {
        self.requests
            .iter()
            .filter_map(|request| request.trace_context.as_deref())
            .collect()
    }

    /// Sends success result to all requests in this flush.
    pub fn send_success(self, offset: u64, trace_context: Option<&str>) {
        for request in self.requests {
            let _ = request.response_tx.send(WriteResult::success(
                offset,
                request.records,
                trace_context.map(std::borrow::ToOwned::to_owned),
            ));
        }
    }

    /// Sends failure result to all requests in this flush.
    pub fn send_failure(self, reason: impl Into<String>, trace_context: Option<&str>) {
        let reason = reason.into();
        for request in self.requests {
            let _ = request.response_tx.send(WriteResult::failed(
                reason.clone(),
                trace_context.map(std::borrow::ToOwned::to_owned),
            ));
        }
    }
}

/// Accumulator for a single topic.
///
/// Collects logical write requests and their response channels until a flush
/// threshold is reached.
#[derive(Debug)]
pub struct TopicAccumulator {
    /// Pending requests with their response channels and trace contexts.
    pending: Vec<PendingRequest>,

    /// Total record count across all batches.
    total_records: usize,

    /// Estimated total bytes across all batches.
    total_bytes: usize,

    /// Number of physical batches across all pending requests.
    total_batches: usize,

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
            total_batches: 0,
            last_flush: Instant::now(),
        }
    }

    /// Adds a request and its response channel to the accumulator.
    pub fn add(
        &mut self,
        row_groups: Vec<PreparedWalRowGroup>,
        response_tx: oneshot::Sender<WriteResult>,
        trace_context: Option<String>,
    ) {
        let records = row_groups.iter().map(|row_group| row_group.batch.num_rows()).sum::<usize>();
        let bytes = row_groups
            .iter()
            .map(|row_group| Self::estimate_batch_size(&row_group.batch))
            .sum::<usize>();

        self.total_records += records;
        self.total_bytes += bytes;
        self.total_batches += row_groups.len();
        self.pending.push(PendingRequest {
            row_groups,
            records,
            response_tx,
            trace_context,
        });
    }

    /// Checks if the accumulator should be flushed based on config thresholds.
    #[must_use]
    pub fn should_flush(&self, config: &QueueConfig) -> bool {
        if self.pending.is_empty() {
            trace!("Accumulator is empty, no flush needed");
            return false;
        }

        // Check record count threshold
        let row_flush_limit = config.flush_record_limit();
        if self.total_records >= row_flush_limit {
            trace!("Record count threshold reached, flushing accumulator");
            return true;
        }

        // Check byte size threshold
        if self.total_bytes >= config.write.max_bytes_per_flush {
            trace!("Byte size threshold reached, flushing accumulator");
            return true;
        }
        trace!(
            "Byte size threshold not reached, skipping flush, current: {} bytes, threshold: {} bytes",
            self.total_bytes, config.write.max_bytes_per_flush
        );

        // Check time threshold
        #[allow(clippy::cast_possible_truncation)] // Duration will not exceed u64 in practice
        let elapsed_ms = self.last_flush.elapsed().as_millis() as u64;
        if elapsed_ms >= config.write.flush_interval_ms {
            trace!("Time threshold reached, flushing accumulator");
            return true;
        }
        trace!(
            "Time threshold not reached, skipping flush, elapsed: {}ms, threshold: {}ms",
            elapsed_ms, config.write.flush_interval_ms
        );

        false
    }

    /// Returns the number of pending physical batches.
    #[must_use]
    pub const fn pending_batches(&self) -> usize {
        self.total_batches
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

    /// Takes all accumulated requests for a single flush, resetting the accumulator.
    pub fn take_pending_flush(&mut self) -> PendingFlush {
        self.total_records = 0;
        self.total_bytes = 0;
        self.total_batches = 0;
        self.last_flush = Instant::now();
        PendingFlush {
            requests: std::mem::take(&mut self.pending),
        }
    }

    /// Estimates the size of a `RecordBatch` in bytes.
    ///
    /// This is an approximation based on array memory sizes.
    fn estimate_batch_size(batch: &RecordBatch) -> usize {
        batch.columns().iter().map(|col| col.get_array_memory_size()).sum()
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

    fn prepared_batch(rows: usize) -> PreparedWalRowGroup {
        PreparedWalRowGroup {
            batch: test_batch(rows),
            metadata: None,
        }
    }

    #[test]
    fn test_accumulator_add() {
        let mut acc = TopicAccumulator::new();
        let (tx, _rx) = oneshot::channel();

        acc.add(vec![prepared_batch(100)], tx, None);

        assert_eq!(acc.pending_batches(), 1);
        assert_eq!(acc.pending_records(), 100);
        assert!(acc.pending_batches() > 0);
    }

    #[test]
    fn test_accumulator_take_pending_flush() {
        let mut acc = TopicAccumulator::new();
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();

        acc.add(vec![prepared_batch(100)], tx1, None);
        acc.add(vec![prepared_batch(50)], tx2, None);

        assert_eq!(acc.pending_records(), 150);

        let pending = acc.take_pending_flush();

        assert_eq!(pending.request_count(), 2);
        assert_eq!(pending.row_group_count(), 2);
        assert_eq!(acc.pending_batches(), 0);
        assert_eq!(acc.pending_records(), 0);
    }

    #[test]
    fn test_pending_flush_tracks_multi_batch_request() {
        let mut acc = TopicAccumulator::new();
        let (tx, _rx) = oneshot::channel();

        acc.add(
            vec![prepared_batch(2), prepared_batch(3)],
            tx,
            Some("trace-a".to_string()),
        );

        let pending = acc.take_pending_flush();

        assert_eq!(pending.request_count(), 1);
        assert_eq!(pending.row_group_count(), 2);
        assert_eq!(
            pending
                .row_groups()
                .iter()
                .map(|row_group| row_group.batch.num_rows())
                .sum::<usize>(),
            5
        );
        assert_eq!(pending.trace_contexts(), vec!["trace-a"]);
    }

    #[test]
    fn test_should_flush_records() {
        let mut acc = TopicAccumulator::new();
        let config = QueueConfig::new("test")
            .with_max_row_group_size(50)
            .with_records_per_flush_multiplier(2);

        let (tx, _rx) = oneshot::channel();
        acc.add(vec![prepared_batch(50)], tx, None);
        assert!(!acc.should_flush(&config));

        let (tx, _rx) = oneshot::channel();
        acc.add(vec![prepared_batch(60)], tx, None);
        assert!(acc.should_flush(&config)); // 110 >= (50 * 2)
    }

    #[test]
    fn test_should_flush_empty() {
        let acc = TopicAccumulator::new();
        let config = QueueConfig::new("test");

        assert!(!acc.should_flush(&config));
    }

    #[test]
    fn test_accumulator_tracks_multi_batch_request() {
        let mut acc = TopicAccumulator::new();
        let (tx, _rx) = oneshot::channel();

        acc.add(vec![prepared_batch(2), prepared_batch(3)], tx, None);

        assert_eq!(acc.pending_batches(), 2);
        assert_eq!(acc.pending_records(), 5);

        let pending = acc.take_pending_flush();
        assert_eq!(pending.request_count(), 1);
    }
}
