//! Queue writer for durable Parquet segments on object storage. A common component for storage, it doesn't know what data it uses.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{
    array::ArrayRef,
    compute::{lexsort_to_indices, take, SortColumn, SortOptions},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use futures::{future::join_all, TryStreamExt};
use object_store::{path::Path, ObjectStore, PutMode, PutOptions, PutPayload};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::{
    accumulator::TopicAccumulator,
    channel::{WriteReceiver, WriteRequest},
    config::QueueConfig,
    error::{QueueError, Result},
    segment::SegmentId,
    Topic,
};

/// Queue writer that persists Arrow `RecordBatches` to Parquet on object
/// storage.
///
/// The writer receives batches via a channel and accumulates them until a flush
/// threshold is reached. Each flush writes accumulated batches as a single
/// Parquet segment. Writes are atomic using `If-None-Match` to prevent
/// duplicate segments.
pub struct QueueWriter {
    /// Configuration for the queue.
    config: QueueConfig,

    /// Object store backend.
    store: Arc<dyn ObjectStore>,

    /// Current offset per topic.
    offsets: Arc<RwLock<HashMap<Topic, u64>>>,

    /// Per-topic batch accumulators.
    accumulators: Arc<RwLock<HashMap<Topic, TopicAccumulator>>>,

    /// Events handler for queue writer operations.
    events: Arc<dyn QueueWriterEvents>,
}

impl QueueWriter {
    /// Creates a new queue writer.
    pub fn new(config: QueueConfig, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            config,
            store,
            offsets: Arc::new(RwLock::new(HashMap::new())),
            accumulators: Arc::new(RwLock::new(HashMap::new())),
            events: Arc::new(NoopQueueWriterEvents),
        }
    }

    /// Set a custom events handler.
    #[must_use]
    pub fn with_events(mut self, events: Arc<dyn QueueWriterEvents>) -> Self {
        self.events = events;
        self
    }

    /// Starts the writer, consuming from the provided receiver.
    ///
    /// This method spawns background tasks:
    /// 1. Main task that processes write requests and accumulates batches
    /// 2. Flush ticker that periodically checks for time-based flushes
    ///
    /// Returns a handle to the main task.
    pub fn start(self, mut receiver: WriteReceiver) -> tokio::task::JoinHandle<Result<()>> {
        let writer = Arc::new(self);
        let flush_writer = Arc::clone(&writer);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        // Spawn flush ticker task
        // To make a flush interval is more precise make the check interval is smaller than the one
        let check_flush_interval = Duration::from_millis(writer.config.write.flush_interval_ms);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_flush_interval);
            // Use Delay mode: wait for full interval AFTER each flush completes
            // This prevents tick accumulation when flush_due_topics takes longer than interval
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = flush_writer.flush_due_topics().await {
                            warn!("Flush ticker error: {}", e);
                        }
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        // Main request processing task
        tokio::spawn(async move {
            // Recover offsets from existing segments before processing
            writer.recover().await?;

            info!("Queue writer started");

            while let Some(request) = receiver.recv().await {
                // Extract remote parent context from W3C trace context
                let parent_cx = request
                    .trace_context
                    .as_ref()
                    .map_or_else(opentelemetry::Context::current, |tc| {
                        icegate_common::traceparent_to_context(tc)
                    });

                // Create a span with the remote parent context
                let span = tracing::info_span!("process_request");
                let _ = span.set_parent(parent_cx);
                let _guard = span.enter();

                let writer = Arc::clone(&writer);
                let result = writer.handle_request(request).await;
                if let Err(e) = result {
                    error!("Failed to handle write request: {}", e);
                }
            }

            // Flush all pending batches on shutdown
            info!("Queue writer shutting down, flushing pending batches...");
            writer.flush_all().await?;

            // Signal the flush ticker task to stop
            let _ = shutdown_tx.send(());

            info!("Queue writer stopped (channel closed)");
            Ok(())
        })
    }

    /// Handles a single write request by accumulating the batch.
    ///
    /// The batch is added to the topic's accumulator. If thresholds are met,
    /// a flush is triggered automatically.
    #[allow(clippy::significant_drop_tightening)] // Lock must be held while checking flush condition
    #[tracing::instrument(skip(self, request), fields(topic = %request.topic))]
    async fn handle_request(&self, request: WriteRequest) -> Result<()> {
        debug!("Start handle request to WAL");
        let topic = request.topic.clone();
        let trace_context = request.trace_context.clone();
        let (pending_batches, pending_records, pending_bytes) = {
            let mut accumulators = self.accumulators.write().await;
            let accumulator = accumulators.entry(topic.clone()).or_insert_with(TopicAccumulator::new);
            accumulator.add(request.batch, request.response_tx, trace_context);
            (
                accumulator.pending_batches(),
                accumulator.pending_records(),
                accumulator.pending_bytes(),
            )
        };

        self.events.on_accumulator_state_update(
            topic.as_str(),
            saturating_u64(pending_batches),
            saturating_u64(pending_records),
            saturating_u64(pending_bytes),
        );

        Ok(())
    }

    /// Flushes all topics that have exceeded their time threshold.
    #[tracing::instrument(skip(self))]
    async fn flush_due_topics(&self) -> Result<()> {
        let topics_to_flush: Vec<Topic> = {
            let accumulators = self.accumulators.read().await;
            accumulators
                .iter()
                .filter(|(_, acc)| acc.should_flush(&self.config))
                .map(|(topic, _)| topic.clone())
                .collect()
        };

        let errors: Vec<_> = join_all(
            topics_to_flush
                .into_iter()
                .map(|topic| async move { self.flush_topic(&topic).await }),
        )
        .await
        .into_iter()
        .filter_map(Result::err)
        .collect();
        if !errors.is_empty() {
            return Err(QueueError::Multiple(errors));
        }
        Ok(())
    }

    /// Flushes all accumulated batches for a topic.
    #[tracing::instrument(skip(self), fields(topic = %topic))]
    async fn flush_topic(&self, topic: &Topic) -> Result<()> {
        let flush_start = Instant::now();
        let pending = {
            let mut accumulators = self.accumulators.write().await;
            if let Some(accumulator) = accumulators.get_mut(topic) {
                accumulator.take()
            } else {
                return Ok(());
            }
        };

        debug!(batches = pending.len(), "Flushing topic batches");
        self.events.on_accumulator_state_update(topic.as_str(), 0, 0, 0);

        if pending.is_empty() {
            debug!("No batches to flush");
            return Ok(());
        }

        // Add span links for all trace contexts from batched requests
        let trace_contexts: Vec<&str> = pending.iter().filter_map(|p| p.trace_context.as_deref()).collect();
        icegate_common::add_span_links(trace_contexts);

        // Extract trace context from current flush_topic span
        let flush_trace_context = icegate_common::extract_current_trace_context();

        // Extract batches for concatenation
        let batches: Vec<RecordBatch> = pending.iter().map(|p| p.batch.clone()).collect();
        let total_records: usize = batches.iter().map(RecordBatch::num_rows).sum();

        // Concatenate all batches into one
        let concatenated = match TopicAccumulator::concat_batches(&batches) {
            Ok(batch) => batch,
            Err(e) => {
                self.events.on_flush_finish(topic.as_str(), "error", flush_start.elapsed());
                self.events.on_write_error(topic.as_str(), write_error_reason(&e));
                TopicAccumulator::send_failure(pending, &e.to_string(), flush_trace_context.as_ref());
                return Err(e);
            }
        };

        // Write the concatenated batch
        match self.write_batch(topic, concatenated, None).await {
            Ok(offset) => {
                self.events.on_flush_finish(topic.as_str(), "ok", flush_start.elapsed());
                debug!(offset, "Flush completed");
                TopicAccumulator::send_success(pending, offset, total_records, flush_trace_context.as_ref());
                Ok(())
            }
            Err(e) => {
                self.events.on_flush_finish(topic.as_str(), "error", flush_start.elapsed());
                self.events.on_write_error(topic.as_str(), write_error_reason(&e));
                TopicAccumulator::send_failure(pending, &e.to_string(), flush_trace_context.as_ref());
                Err(e)
            }
        }
    }

    /// Flushes all topics (used for graceful shutdown).
    async fn flush_all(&self) -> Result<()> {
        let topics: Vec<Topic> = {
            let accumulators = self.accumulators.read().await;
            accumulators.keys().cloned().collect()
        };

        for topic in topics {
            if let Err(e) = self.flush_topic(&topic).await {
                error!("Failed to flush topic {} on shutdown: {}", topic, e);
            }
        }

        Ok(())
    }

    /// Writes a batch to object storage, returning the offset.
    ///
    /// This is the core write method used internally. For normal usage, prefer using the channel-based approach via `start()`.
    /// We flush after each prepared batch, but large batches can still be split by parquet max row group size.
    #[tracing::instrument(
        skip(self, batch),
        fields(topic = %topic)
    )]
    async fn write_batch(&self, topic: &Topic, batch: RecordBatch, group_by_column: Option<String>) -> Result<u64> {
        let record_count = batch.num_rows();
        if record_count == 0 {
            debug!("Empty batch, skipping write");
            return Ok(self.get_current_offset(topic).await);
        }

        // Prepare batches (optionally grouped by column)
        let batches = match group_by_column {
            Some(ref col) => self.group_by_column(&batch, col)?,
            None => vec![batch],
        };

        // Convert to Parquet bytes
        let parquet_bytes = self.batches_to_parquet(&batches)?;
        let row_group_count = batches.len();
        let size_bytes = parquet_bytes.len() as u64;
        debug!(
            records = record_count,
            size_bytes,
            row_groups = row_group_count,
            "Parquet payload prepared"
        );

        // Write with retry on conflict
        let offset = self.write_with_retry(topic, parquet_bytes).await?;

        self.events.on_write_complete(
            topic.as_str(),
            WriteBatchOutcome {
                offset,
                records: saturating_u64(record_count),
                size_bytes,
                row_groups: saturating_u64(row_group_count),
            },
        );

        Ok(offset)
    }

    /// Groups a record batch by the values in a column.
    ///
    /// Returns separate batches, one per unique value in the column.
    /// Each batch becomes a separate row group in the Parquet file.
    #[allow(clippy::unused_self)] // May need self for future configuration access
    fn group_by_column(&self, batch: &RecordBatch, column_name: &str) -> Result<Vec<RecordBatch>> {
        let col_idx = batch
            .schema()
            .index_of(column_name)
            .map_err(|e| QueueError::Config(format!("group_by column '{column_name}' not found: {e}")))?;

        let column = batch.column(col_idx);

        // Sort by the grouping column
        let sort_column = SortColumn {
            values: Arc::clone(column),
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        };

        let indices = lexsort_to_indices(&[sort_column], None)?;

        // Take sorted values
        let sorted_columns: Vec<ArrayRef> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let sorted_batch = RecordBatch::try_new(batch.schema(), sorted_columns)?;
        let sorted_group_col = sorted_batch.column(col_idx);

        // Find group boundaries
        let mut boundaries = vec![0usize];
        for i in 1..sorted_batch.num_rows() {
            // Compare adjacent values to find group changes
            let prev_null = sorted_group_col.is_null(i - 1);
            let curr_null = sorted_group_col.is_null(i);

            let is_boundary = match (prev_null, curr_null) {
                (true, true) => false,                 // Both null, same group
                (true, false) | (false, true) => true, // Null/non-null boundary
                (false, false) => {
                    // Compare actual values using array equality
                    let prev_slice = sorted_group_col.slice(i - 1, 1);
                    let curr_slice = sorted_group_col.slice(i, 1);
                    prev_slice.as_ref() != curr_slice.as_ref()
                }
            };

            if is_boundary {
                boundaries.push(i);
            }
        }
        boundaries.push(sorted_batch.num_rows());

        // Create batches for each group
        let mut result = Vec::with_capacity(boundaries.len() - 1);
        for window in boundaries.windows(2) {
            let start = window[0];
            let length = window[1] - start;
            result.push(sorted_batch.slice(start, length));
        }

        Ok(result)
    }

    /// Converts record batches to Parquet bytes.
    fn batches_to_parquet(&self, batches: &[RecordBatch]) -> Result<Bytes> {
        if batches.is_empty() {
            return Err(QueueError::Config("No batches to write".to_string()));
        }

        let schema = batches[0].schema();
        let props = self.writer_properties();

        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;

            for batch in batches {
                writer.write(batch)?;
                // Flush after each batch to create separate row groups
                writer.flush()?;
            }

            writer.close()?;
        }

        Ok(Bytes::from(buffer))
    }

    /// Creates Parquet writer properties from config.
    fn writer_properties(&self) -> WriterProperties {
        let compression = self.config.write.compression.to_parquet_compression();

        WriterProperties::builder()
            .set_compression(compression)
            .set_max_row_group_size(self.config.common.max_row_group_size)
            .build()
    }

    /// Writes Parquet bytes to object storage with retry on conflict.
    #[tracing::instrument(skip(self, data), fields(topic = %topic))]
    async fn write_with_retry(&self, topic: &Topic, data: Bytes) -> Result<u64> {
        let mut attempts = 0;
        let max_attempts = self.config.write.write_retries;

        loop {
            let offset = self.next_offset(topic).await;
            let segment_id = SegmentId::new(topic, offset);

            match self.try_write(&segment_id, data.clone()).await {
                Ok(()) => {
                    debug!("Wrote segment {}/{}", topic, offset);

                    return Ok(offset);
                }
                Err(QueueError::AlreadyExists { .. }) => {
                    attempts += 1;
                    self.events.on_write_retry(topic.as_str(), "already_exists");
                    if attempts >= max_attempts {
                        return Err(QueueError::Write {
                            topic: topic.clone(),
                            offset,
                            source: "max retries exceeded on conflict".into(),
                        });
                    }
                    warn!(
                        "Segment {}/{} already exists, retrying (attempt {})",
                        topic, offset, attempts
                    );
                    // Increment offset and retry (loop continues)
                    // SAFETY: check the interval is within the range of u64
                    if self.config.write.flush_interval_ms >= i64::MAX as u64 {
                        return Err(QueueError::Config("flush_interval_ms too large".to_string()));
                    }
                    #[allow(clippy::cast_possible_wrap)]
                    let interval = self.config.write.flush_interval_ms as i64 / 4; // 25% delta
                    #[allow(clippy::cast_sign_loss)]
                    // SAFETY: the `interval` can't be negative
                    let delay = self.config.write.flush_interval_ms + rand::random_range(-interval..=interval) as u64;
                    debug!(offset, attempt = attempts, delay_ms = delay, "Retrying segment write");
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Attempts to write a segment to object storage.
    ///
    /// Uses `If-None-Match: *` for atomic write (fails if file exists).
    #[tracing::instrument(
        skip(self, data),
        fields(topic = %segment_id.topic, offset = segment_id.offset)
    )]
    async fn try_write(&self, segment_id: &SegmentId, data: Bytes) -> Result<()> {
        debug!("Try to save segments in store");
        let full_path = if self.config.common.base_path.is_empty() {
            segment_id.to_relative_path()
        } else {
            Path::from(format!(
                "{}/{}",
                self.config.common.base_path,
                segment_id.to_relative_path()
            ))
        };

        let opts = PutOptions {
            mode: PutMode::Create, // If-None-Match: *
            ..Default::default()
        };

        let payload = PutPayload::from_bytes(data);

        self.store.put_opts(&full_path, payload, opts).await.map_err(|e| {
            if matches!(e, object_store::Error::AlreadyExists { .. }) {
                QueueError::AlreadyExists {
                    topic: segment_id.topic.clone(),
                    offset: segment_id.offset,
                }
            } else {
                QueueError::Write {
                    topic: segment_id.topic.clone(),
                    offset: segment_id.offset,
                    source: Box::new(e),
                }
            }
        })?;

        Ok(())
    }

    /// Gets the current offset for a topic (without incrementing).
    async fn get_current_offset(&self, topic: &Topic) -> u64 {
        let offsets = self.offsets.read().await;
        offsets.get(topic).copied().unwrap_or(0)
    }

    /// Gets the next offset for a topic and increments the counter.
    #[allow(clippy::significant_drop_tightening)]
    async fn next_offset(&self, topic: &Topic) -> u64 {
        let mut offsets = self.offsets.write().await;
        let offset = offsets.entry(topic.clone()).or_insert(0);
        let current = *offset;
        *offset += 1;
        current
    }

    /// Sets the offset for a topic (used during recovery).
    pub async fn set_offset(&self, topic: &Topic, offset: u64) {
        let mut offsets = self.offsets.write().await;
        offsets.insert(topic.clone(), offset);
    }

    /// Recovers offset state from existing segments on startup.
    ///
    /// Scans the object store for existing parquet segments and sets
    /// the next offset for each topic to continue from where we left off.
    async fn recover(&self) -> Result<()> {
        let base_path = if self.config.common.base_path.is_empty() {
            None
        } else {
            Some(Path::from(self.config.common.base_path.as_str()))
        };
        let list_stream = self.store.list(base_path.as_ref());
        let items: Vec<_> = list_stream.try_collect().await?;

        // Build prefix for stripping - empty string means no prefix to strip
        let base_prefix = if self.config.common.base_path.is_empty() {
            String::new()
        } else {
            format!("{}/", self.config.common.base_path)
        };

        // Map: topic -> next offset (max found + 1)
        let mut topic_offsets: HashMap<Topic, u64> = HashMap::new();

        for item in items {
            let path_str = item.location.as_ref();
            if path_str.ends_with(".parquet") {
                // Get relative path by stripping base prefix (if any)
                let relative = if base_prefix.is_empty() {
                    path_str
                } else {
                    match path_str.strip_prefix(&base_prefix) {
                        Some(r) => r,
                        None => continue,
                    }
                };

                // Parse topic and offset from path: "topic/00000000000000000000.parquet"
                if let Ok(segment_id) = SegmentId::from_relative_path(&Path::from(relative)) {
                    let entry = topic_offsets.entry(segment_id.topic.clone()).or_insert(0);
                    let next = segment_id.offset + 1;
                    if next > *entry {
                        *entry = next;
                    }
                }
            }
        }

        // Set recovered offsets
        for (topic, next_offset) in &topic_offsets {
            self.set_offset(topic, *next_offset).await;
            info!("Recovered topic '{}': next offset = {}", topic, next_offset);
        }

        if !topic_offsets.is_empty() {
            info!("Recovery complete: {} topics", topic_offsets.len());
        }

        Ok(())
    }

    /// Gets a reference to the object store.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// Gets a reference to the config.
    pub const fn config(&self) -> &QueueConfig {
        &self.config
    }
}

/// Write outcome details for a single WAL segment.
#[derive(Debug, Clone, Copy)]
pub struct WriteBatchOutcome {
    /// Offset of the written segment.
    pub offset: u64,
    /// Total records written in the segment.
    pub records: u64,
    /// Segment size in bytes.
    pub size_bytes: u64,
    /// Row groups per segment.
    pub row_groups: u64,
}

/// Events interface for queue writer instrumentation.
pub trait QueueWriterEvents: Send + Sync {
    /// Record accumulator state for a topic.
    fn on_accumulator_state_update(&self, _topic: &str, _batches: u64, _records: u64, _bytes: u64) {}

    /// Record flush completion for a topic.
    fn on_flush_finish(&self, _topic: &str, _status: &str, _duration: Duration) {}

    /// Record a successful write outcome for a topic.
    fn on_write_complete(&self, _topic: &str, _outcome: WriteBatchOutcome) {}

    /// Record write retry attempt.
    fn on_write_retry(&self, _topic: &str, _reason: &str) {}

    /// Record write error.
    fn on_write_error(&self, _topic: &str, _reason: &str) {}
}

/// No-op events implementation.
#[derive(Debug, Default)]
pub struct NoopQueueWriterEvents;

impl QueueWriterEvents for NoopQueueWriterEvents {}

fn write_error_reason(error: &QueueError) -> &'static str {
    match error {
        QueueError::ObjectStore(_) => "object_store",
        QueueError::Parquet(_) => "parquet",
        QueueError::Arrow(_) => "arrow",
        QueueError::Config(_) => "config",
        QueueError::Write { source, .. } => {
            if source.downcast_ref::<object_store::Error>().is_some() {
                "object_store"
            } else if source.downcast_ref::<parquet::errors::ParquetError>().is_some() {
                "parquet"
            } else if source.downcast_ref::<arrow::error::ArrowError>().is_some() {
                "arrow"
            } else {
                "other"
            }
        }
        _ => "other",
    }
}

fn saturating_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use object_store::memory::InMemory;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{channel::channel, ParquetQueueReader};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn test_batch() -> RecordBatch {
        let schema = test_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["acme", "globex", "acme", "globex"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_batch_success() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue");
        let writer = QueueWriter::new(config, store.clone());

        let batch = test_batch();
        let offset = writer.write_batch(&"logs".to_string(), batch, None).await.unwrap();

        assert_eq!(offset, 0);

        // Verify file exists
        let path = Path::from("queue/logs/00000000000000000000.parquet");
        let result = store.head(&path).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_write_batch_with_grouping() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue");
        let writer = QueueWriter::new(config, store.clone());

        let batch = test_batch();
        let offset = writer
            .write_batch(&"logs".to_string(), batch, Some("tenant_id".to_string()))
            .await
            .unwrap();

        assert_eq!(offset, 0);
    }

    #[tokio::test]
    async fn test_sequential_offsets() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue");
        let writer = QueueWriter::new(config, store);

        let batch = test_batch();

        let offset1 = writer.write_batch(&"logs".to_string(), batch.clone(), None).await.unwrap();

        let offset2 = writer.write_batch(&"logs".to_string(), batch.clone(), None).await.unwrap();

        let offset3 = writer.write_batch(&"logs".to_string(), batch, None).await.unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 2);
    }

    #[tokio::test]
    async fn test_channel_write() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue");
        let writer = QueueWriter::new(config, store);

        let (tx, rx) = channel(10);
        let _handle = writer.start(rx);

        let batch = test_batch();
        let (response_tx, response_rx) = oneshot::channel();

        tx.send(WriteRequest {
            topic: "logs".to_string(),
            batch,
            group_by_column: None,
            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();

        let result = response_rx.await.unwrap();
        assert!(result.is_success());
        assert_eq!(result.offset(), Some(0));
        assert_eq!(result.records(), Some(4));
    }

    #[tokio::test]
    async fn test_group_by_column() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue");
        let writer = QueueWriter::new(config, store);

        let batch = test_batch();
        let grouped = writer.group_by_column(&batch, "tenant_id").unwrap();

        // Should have 2 groups: acme and globex
        assert_eq!(grouped.len(), 2);

        // Each group should have 2 records
        assert_eq!(grouped[0].num_rows(), 2);
        assert_eq!(grouped[1].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_max_row_group_size_limits_row_group_size() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue").with_max_row_group_size(2);
        let writer = QueueWriter::new(config, store.clone());

        writer.write_batch(&"logs".to_string(), test_batch(), None).await.unwrap();

        let reader = ParquetQueueReader::new("queue", store, 2).unwrap();
        let cancel = CancellationToken::new();
        let batches = reader.read_segment(&"logs".to_string(), 0, &[0, 1], &cancel).await.unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_recover_empty() {
        let store = Arc::new(InMemory::new());
        let config = QueueConfig::new("queue");
        let writer = QueueWriter::new(config, store);

        // Recovery on empty store should succeed
        writer.recover().await.unwrap();

        // First write should start at offset 0
        let batch = test_batch();
        let offset = writer.write_batch(&"logs".to_string(), batch, None).await.unwrap();
        assert_eq!(offset, 0);
    }

    #[tokio::test]
    async fn test_recover_with_existing_segments() {
        let store = Arc::new(InMemory::new());

        // Write some segments with first writer
        let config1 = QueueConfig::new("queue");
        let writer1 = QueueWriter::new(config1, store.clone());
        writer1.write_batch(&"logs".to_string(), test_batch(), None).await.unwrap();
        writer1.write_batch(&"logs".to_string(), test_batch(), None).await.unwrap();
        writer1.write_batch(&"logs".to_string(), test_batch(), None).await.unwrap();

        // Create new writer (simulating restart) and recover
        let config2 = QueueConfig::new("queue");
        let writer2 = QueueWriter::new(config2, store);
        writer2.recover().await.unwrap();

        // New write should continue from offset 3
        let offset = writer2.write_batch(&"logs".to_string(), test_batch(), None).await.unwrap();
        assert_eq!(offset, 3);
    }

    #[tokio::test]
    async fn test_recover_multiple_topics() {
        let store = Arc::new(InMemory::new());

        // Write segments to different topics
        let config1 = QueueConfig::new("queue");
        let writer1 = QueueWriter::new(config1, store.clone());
        writer1.write_batch(&"logs".to_string(), test_batch(), None).await.unwrap();
        writer1.write_batch(&"logs".to_string(), test_batch(), None).await.unwrap();
        writer1.write_batch(&"events".to_string(), test_batch(), None).await.unwrap();

        // Create new writer and recover
        let config2 = QueueConfig::new("queue");
        let writer2 = QueueWriter::new(config2, store);
        writer2.recover().await.unwrap();

        // Writes should continue from recovered offsets
        let logs_offset = writer2.write_batch(&"logs".to_string(), test_batch(), None).await.unwrap();
        let events_offset = writer2.write_batch(&"events".to_string(), test_batch(), None).await.unwrap();

        assert_eq!(logs_offset, 2);
        assert_eq!(events_offset, 1);
    }
}
