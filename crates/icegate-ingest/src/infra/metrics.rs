use std::{
    fmt, io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use icegate_queue::{QueueWriterEvents, WriteBatchOutcome};
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult, path::Path,
};
use opentelemetry::{
    KeyValue,
    metrics::{Counter, Gauge, Histogram, Meter, MeterProvider as _},
};
use opentelemetry_sdk::metrics::SdkMeterProvider;

use crate::error::IngestError;

// TODO(low): make interface (trate) to simplify disabled metrics implementation

/// Metrics collected during shift processing.
#[derive(Clone)]
pub struct ShiftMetrics {
    enabled: bool,
    plan_duration: Histogram<f64>,
    queue_plan_duration: Histogram<f64>,
    queue_read_segment_duration: Histogram<f64>,
    planned_segments_total: Counter<u64>,
    planned_record_batches_total: Counter<u64>,
    planned_tasks_total: Counter<u64>,
    backlog_segments: Gauge<u64>,
    planned_input_bytes_per_task: Histogram<f64>,
    segment_record_batches_per_task: Histogram<f64>,
    segment_rows_per_task: Histogram<f64>,
    parquet_write_duration: Histogram<f64>,
    parquet_files_total: Counter<u64>,
    bytes_written_total: Counter<u64>,
    get_data_files_duration: Histogram<f64>,
    commit_duration: Histogram<f64>,
    get_last_offset_duration: Histogram<f64>,
    already_committed_total: Counter<u64>,
    task_failures_total: Counter<u64>,
    task_success_total: Counter<u64>,
    backpressure_rejection_probability: Gauge<f64>,
    backpressure_load_ratio: Gauge<f64>,
    backpressure_error: Gauge<f64>,
    backpressure_integral: Gauge<f64>,
    shift_iteration_duration_ms: Gauge<f64>,
}

impl ShiftMetrics {
    /// Build a metrics recorder that performs no-ops.
    pub fn new_disabled() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("ingest_shift");

        Self {
            enabled: false,
            plan_duration: meter.f64_histogram("icegate_ingest_shift_plan_duration").build(),
            queue_plan_duration: meter.f64_histogram("icegate_ingest_shift_queue_plan_duration").build(),
            queue_read_segment_duration: meter
                .f64_histogram("icegate_ingest_shift_queue_read_segment_duration")
                .build(),
            planned_segments_total: meter.u64_counter("icegate_ingest_shift_planned_segments").build(),
            planned_record_batches_total: meter.u64_counter("icegate_ingest_shift_planned_record_batches").build(),
            planned_tasks_total: meter.u64_counter("icegate_ingest_shift_planned_tasks").build(),
            backlog_segments: meter.u64_gauge("icegate_ingest_shift_backlog_segments").build(),
            planned_input_bytes_per_task: meter
                .f64_histogram("icegate_ingest_shift_planned_input_bytes_per_task")
                .build(),
            segment_record_batches_per_task: meter
                .f64_histogram("icegate_ingest_shift_segment_record_batches_per_task")
                .build(),
            segment_rows_per_task: meter.f64_histogram("icegate_ingest_shift_segment_rows_per_task").build(),
            parquet_write_duration: meter.f64_histogram("icegate_ingest_shift_parquet_write_duration").build(),
            parquet_files_total: meter.u64_counter("icegate_ingest_shift_parquet_files").build(),
            bytes_written_total: meter.u64_counter("icegate_ingest_shift_bytes_written").build(),
            get_data_files_duration: meter.f64_histogram("icegate_ingest_shift_get_data_files_duration").build(),
            commit_duration: meter.f64_histogram("icegate_ingest_shift_commit_duration").build(),
            get_last_offset_duration: meter.f64_histogram("icegate_ingest_shift_get_last_offset_duration").build(),
            already_committed_total: meter.u64_counter("icegate_ingest_shift_already_committed").build(),
            task_failures_total: meter.u64_counter("icegate_ingest_shift_task_failures").build(),
            task_success_total: meter.u64_counter("icegate_ingest_shift_task_success").build(),
            backpressure_rejection_probability: meter
                .f64_gauge("icegate_ingest_shift_backpressure_rejection_probability")
                .build(),
            backpressure_load_ratio: meter.f64_gauge("icegate_ingest_shift_backpressure_load_ratio").build(),
            backpressure_error: meter.f64_gauge("icegate_ingest_shift_backpressure_error").build(),
            backpressure_integral: meter.f64_gauge("icegate_ingest_shift_backpressure_integral").build(),
            shift_iteration_duration_ms: meter.f64_gauge("icegate_ingest_shift_iteration_duration_ms").build(),
        }
    }

    /// Build a metrics recorder using the provided meter.
    pub fn new(meter: &Meter) -> Self {
        let plan_duration = meter
            .f64_histogram("icegate_ingest_shift_plan_duration")
            .with_description("Duration of shift plan task execution")
            .with_unit("s")
            .build();
        let queue_plan_duration = meter
            .f64_histogram("icegate_ingest_shift_queue_plan_duration")
            .with_description("Duration of queue_reader.plan_segments execution")
            .with_unit("s")
            .build();
        let queue_read_segment_duration = meter
            .f64_histogram("icegate_ingest_shift_queue_read_segment_duration")
            .with_description("Duration of queue_reader.read_segment execution")
            .with_unit("s")
            .build();
        let planned_segments_total = meter
            .u64_counter("icegate_ingest_shift_planned_segments")
            .with_description("Number of planned segments")
            .build();
        let planned_record_batches_total = meter
            .u64_counter("icegate_ingest_shift_planned_record_batches")
            .with_description("Number of planned record batches")
            .build();
        let planned_tasks_total = meter
            .u64_counter("icegate_ingest_shift_planned_tasks")
            .with_description("Number of planned shift tasks")
            .build();
        let backlog_segments = meter
            .u64_gauge("icegate_ingest_shift_backlog_segments")
            .with_description("Number of segments waiting to be processed")
            .build();
        let planned_input_bytes_per_task = meter
            .f64_histogram("icegate_ingest_shift_planned_input_bytes_per_task")
            .with_description("Planned input bytes per shift task")
            .with_unit("By")
            .build();
        let segment_record_batches_per_task = meter
            .f64_histogram("icegate_ingest_shift_segment_record_batches_per_task")
            .with_description("Record batches per shift task")
            .build();
        let segment_rows_per_task = meter
            .f64_histogram("icegate_ingest_shift_segment_rows_per_task")
            .with_description("Rows per shift task")
            .build();
        let parquet_write_duration = meter
            .f64_histogram("icegate_ingest_shift_parquet_write_duration")
            .with_description("Duration of storage.write_record_batches execution")
            .with_unit("s")
            .build();
        let parquet_files_total = meter
            .u64_counter("icegate_ingest_shift_parquet_files")
            .with_description("Number of parquet files written by shift tasks")
            .build();
        let bytes_written_total = meter
            .u64_counter("icegate_ingest_shift_bytes_written")
            .with_description("Bytes written by shift tasks")
            .with_unit("By")
            .build();
        let get_data_files_duration = meter
            .f64_histogram("icegate_ingest_shift_get_data_files_duration")
            .with_description("Duration of storage.get_data_files execution")
            .with_unit("s")
            .build();
        let commit_duration = meter
            .f64_histogram("icegate_ingest_shift_commit_duration")
            .with_description("Duration of storage.commit execution")
            .with_unit("s")
            .build();
        let get_last_offset_duration = meter
            .f64_histogram("icegate_ingest_shift_get_last_offset_duration")
            .with_description("Duration of storage.get_last_offset execution")
            .with_unit("s")
            .build();
        let already_committed_total = meter
            .u64_counter("icegate_ingest_shift_already_committed")
            .with_description("Number of commit tasks resolved as already committed")
            .build();
        let task_failures_total = meter
            .u64_counter("icegate_ingest_shift_task_failures")
            .with_description("Number of shift task failures")
            .build();
        let task_success_total = meter
            .u64_counter("icegate_ingest_shift_task_success")
            .with_description("Number of shift task successes")
            .build();
        let backpressure_rejection_probability = meter
            .f64_gauge("icegate_ingest_shift_backpressure_rejection_probability")
            .with_description("Current backpressure rejection probability (0.0 - 1.0)")
            .build();
        let backpressure_load_ratio = meter
            .f64_gauge("icegate_ingest_shift_backpressure_load_ratio")
            .with_description("Last shift iteration load ratio (duration / interval)")
            .build();
        let backpressure_error = meter
            .f64_gauge("icegate_ingest_shift_backpressure_error")
            .with_description("Current PI error signal")
            .build();
        let backpressure_integral = meter
            .f64_gauge("icegate_ingest_shift_backpressure_integral")
            .with_description("Current PI integral accumulator value")
            .build();
        let shift_iteration_duration_ms = meter
            .f64_gauge("icegate_ingest_shift_iteration_duration_ms")
            .with_description("Last shift iteration wall-clock time in milliseconds")
            .build();

        Self {
            enabled: true,
            plan_duration,
            queue_plan_duration,
            queue_read_segment_duration,
            planned_segments_total,
            planned_record_batches_total,
            planned_tasks_total,
            backlog_segments,
            planned_input_bytes_per_task,
            segment_record_batches_per_task,
            segment_rows_per_task,
            parquet_write_duration,
            parquet_files_total,
            bytes_written_total,
            get_data_files_duration,
            commit_duration,
            get_last_offset_duration,
            already_committed_total,
            task_failures_total,
            task_success_total,
            backpressure_rejection_probability,
            backpressure_load_ratio,
            backpressure_error,
            backpressure_integral,
            shift_iteration_duration_ms,
        }
    }

    /// Record the plan task duration.
    pub fn record_plan_duration(&self, duration: Duration, topic: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.plan_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record queue plan duration.
    pub fn record_queue_plan_duration(&self, duration: Duration, topic: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.queue_plan_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record queue read segment duration.
    pub fn record_queue_read_segment_duration(&self, duration: Duration, topic: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.queue_read_segment_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record the number of planned segments.
    pub fn add_planned_segments(&self, count: usize, topic: &str) {
        if !self.enabled {
            return;
        }
        self.planned_segments_total
            .add(count as u64, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record the number of planned record batches.
    pub fn add_planned_record_batches(&self, count: usize, topic: &str) {
        if !self.enabled {
            return;
        }
        self.planned_record_batches_total
            .add(count as u64, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record the number of planned shift tasks.
    pub fn add_planned_tasks(&self, count: usize, topic: &str) {
        if !self.enabled {
            return;
        }
        self.planned_tasks_total
            .add(count as u64, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record planned input bytes per shift task.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_planned_input_bytes_per_task(&self, count: u64, topic: &str) {
        if !self.enabled {
            return;
        }
        self.planned_input_bytes_per_task
            .record(count as f64, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record backlog segments count.
    pub fn record_backlog_segments(&self, backlog: u64, topic: &str) {
        if !self.enabled {
            return;
        }
        self.backlog_segments
            .record(backlog, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record record batches per shift task.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_segment_record_batches_per_task(&self, count: usize, topic: &str) {
        if !self.enabled {
            return;
        }
        self.segment_record_batches_per_task
            .record(count as f64, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record rows per shift task.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_segment_rows_per_task(&self, count: usize, topic: &str) {
        if !self.enabled {
            return;
        }
        self.segment_rows_per_task
            .record(count as f64, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record parquet write duration.
    pub fn record_parquet_write_duration(&self, duration: Duration, topic: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.parquet_write_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record parquet files written.
    pub fn add_parquet_files(&self, count: usize, topic: &str) {
        if !self.enabled {
            return;
        }
        self.parquet_files_total
            .add(count as u64, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record bytes written by shift tasks.
    pub fn add_bytes_written(&self, count: u64, topic: &str) {
        if !self.enabled {
            return;
        }
        self.bytes_written_total
            .add(count, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record data files lookup duration.
    pub fn record_get_data_files_duration(&self, duration: Duration, topic: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.get_data_files_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record commit duration.
    pub fn record_commit_duration(&self, duration: Duration, topic: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.commit_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record `get_last_offset` duration.
    pub fn record_get_last_offset_duration(&self, duration: Duration, topic: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.get_last_offset_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record already committed path.
    pub fn add_already_committed(&self, topic: &str) {
        if !self.enabled {
            return;
        }
        self.already_committed_total
            .add(1, &[KeyValue::new("topic", topic.to_string())]);
    }

    /// Record shift task failures.
    pub fn record_task_failure(&self, task: &str, reason: &str, topic: &str) {
        if !self.enabled {
            return;
        }
        self.task_failures_total.add(
            1,
            &[
                KeyValue::new("task", task.to_string()),
                KeyValue::new("reason", reason.to_string()),
                KeyValue::new("topic", topic.to_string()),
            ],
        );
    }

    /// Record shift task successes.
    pub fn record_task_success(&self, task: &str, topic: &str) {
        if !self.enabled {
            return;
        }
        self.task_success_total.add(
            1,
            &[
                KeyValue::new("task", task.to_string()),
                KeyValue::new("topic", topic.to_string()),
            ],
        );
    }

    /// Record backpressure controller state after a shift iteration.
    pub fn record_backpressure(
        &self,
        rejection_probability: f64,
        load_ratio: f64,
        error: f64,
        integral: f64,
        iteration_duration_ms: f64,
    ) {
        if !self.enabled {
            return;
        }
        self.backpressure_rejection_probability.record(rejection_probability, &[]);
        self.backpressure_load_ratio.record(load_ratio, &[]);
        self.backpressure_error.record(error, &[]);
        self.backpressure_integral.record(integral, &[]);
        self.shift_iteration_duration_ms.record(iteration_duration_ms, &[]);
    }
}

/// Metrics collected for S3 calls made by `ParquetQueueReader`.
#[derive(Clone)]
pub struct QueueReaderS3Metrics {
    enabled: bool,
    request_duration: Histogram<f64>,
}

impl QueueReaderS3Metrics {
    /// Build a metrics recorder that performs no-ops.
    pub fn new_disabled() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("ingest_shift_queue_reader_s3");

        Self {
            enabled: false,
            request_duration: meter
                .f64_histogram("icegate_ingest_shift_queue_reader_s3_request_duration")
                .build(),
        }
    }

    /// Build a metrics recorder using the provided meter.
    pub fn new(meter: &Meter) -> Self {
        let request_duration = meter
            .f64_histogram("icegate_ingest_shift_queue_reader_s3_request_duration")
            .with_description("Queue reader S3 request duration")
            .with_unit("s")
            .build();

        Self {
            enabled: true,
            request_duration,
        }
    }
}

/// Metrics collected for S3 calls made by `QueueWriter`.
#[derive(Clone)]
pub struct QueueWriterS3Metrics {
    enabled: bool,
    request_duration: Histogram<f64>,
}

impl QueueWriterS3Metrics {
    /// Build a metrics recorder that performs no-ops.
    pub fn new_disabled() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("ingest_wal_queue_writer_s3");

        Self {
            enabled: false,
            request_duration: meter
                .f64_histogram("icegate_ingest_shift_queue_writer_s3_request_duration")
                .build(),
        }
    }

    /// Build a metrics recorder using the provided meter.
    pub fn new(meter: &Meter) -> Self {
        let request_duration = meter
            .f64_histogram("icegate_ingest_shift_queue_writer_s3_request_duration")
            .with_description("Queue writer S3 request duration")
            .with_unit("s")
            .build();

        Self {
            enabled: true,
            request_duration,
        }
    }
}

/// Generic S3 request metrics sink used by `ObjectStoreMetricsDecorator`.
pub trait S3RequestMetrics: Clone + Send + Sync + 'static {
    /// Record one object-store request observation.
    fn record(&self, operation: &str, status: &str, error_kind: &str, duration: Duration);
}

impl S3RequestMetrics for QueueReaderS3Metrics {
    fn record(&self, operation: &str, status: &str, error_kind: &str, duration: Duration) {
        if !self.enabled {
            return;
        }
        let labels = &[
            KeyValue::new("operation", operation.to_string()),
            KeyValue::new("status", status.to_string()),
            KeyValue::new("error_kind", error_kind.to_string()),
            KeyValue::new("component", "queue_reader"),
        ];
        self.request_duration.record(duration.as_secs_f64(), labels);
    }
}

impl S3RequestMetrics for QueueWriterS3Metrics {
    fn record(&self, operation: &str, status: &str, error_kind: &str, duration: Duration) {
        if !self.enabled {
            return;
        }
        let labels = &[
            KeyValue::new("operation", operation.to_string()),
            KeyValue::new("status", status.to_string()),
            KeyValue::new("error_kind", error_kind.to_string()),
            KeyValue::new("component", "queue_writer"),
        ];
        self.request_duration.record(duration.as_secs_f64(), labels);
    }
}

struct InstrumentedListStream<M: S3RequestMetrics> {
    inner: BoxStream<'static, object_store::Result<ObjectMeta>>,
    metrics: Arc<M>,
    operation: &'static str,
    started_at: Instant,
    completed: bool,
    yielded_item: bool,
}

impl<M: S3RequestMetrics> InstrumentedListStream<M> {
    fn new(
        inner: BoxStream<'static, object_store::Result<ObjectMeta>>,
        metrics: Arc<M>,
        operation: &'static str,
    ) -> Self {
        Self {
            inner,
            metrics,
            operation,
            started_at: Instant::now(),
            completed: false,
            yielded_item: false,
        }
    }
}

impl<M: S3RequestMetrics> Stream for InstrumentedListStream<M> {
    type Item = object_store::Result<ObjectMeta>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.completed {
            return Poll::Ready(None);
        }
        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(meta))) => {
                this.yielded_item = true;
                Poll::Ready(Some(Ok(meta)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.metrics.record(
                    this.operation,
                    "error",
                    ObjectStoreMetricsDecorator::<M>::object_store_error_kind(&error),
                    this.started_at.elapsed(),
                );
                this.completed = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.metrics.record(this.operation, "ok", "none", this.started_at.elapsed());
                this.completed = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<M: S3RequestMetrics> Drop for InstrumentedListStream<M> {
    fn drop(&mut self) {
        if self.completed || !self.yielded_item {
            return;
        }
        self.metrics.record(self.operation, "ok", "none", self.started_at.elapsed());
        self.completed = true;
    }
}

/// Object store decorator that records queue S3 call metrics.
pub struct ObjectStoreMetricsDecorator<M> {
    inner: Arc<dyn ObjectStore>,
    metrics: M,
}

impl<M> fmt::Debug for ObjectStoreMetricsDecorator<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ObjectStoreMetricsDecorator").finish()
    }
}

impl<M> fmt::Display for ObjectStoreMetricsDecorator<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "ObjectStoreMetricsDecorator")
    }
}

impl<M: S3RequestMetrics> ObjectStoreMetricsDecorator<M> {
    /// Create a new object-store metrics decorator.
    pub const fn new(inner: Arc<dyn ObjectStore>, metrics: M) -> Self {
        Self { inner, metrics }
    }

    fn record_object_store_outcome<T>(&self, operation: &str, started_at: Instant, outcome: &object_store::Result<T>) {
        let duration = started_at.elapsed();
        match outcome {
            Ok(_) => self.metrics.record(operation, "ok", "none", duration),
            Err(error) => self
                .metrics
                .record(operation, "error", Self::object_store_error_kind(error), duration),
        }
    }

    fn instrument_list_stream(
        &self,
        operation: &'static str,
        inner_stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        InstrumentedListStream::new(inner_stream, Arc::new(self.metrics.clone()), operation).boxed()
    }

    fn object_store_error_kind(error: &object_store::Error) -> &'static str {
        match error {
            object_store::Error::AlreadyExists { .. } => "already_exists",
            object_store::Error::NotFound { .. } => "not_found",
            object_store::Error::PermissionDenied { .. } | object_store::Error::Unauthenticated { .. } => {
                "permission_denied"
            }
            object_store::Error::Generic { source, .. } => {
                source.downcast_ref::<io::Error>().map_or("other", |io_error| {
                    if Self::is_timeout_io(io_error) {
                        "timeout"
                    } else {
                        "other"
                    }
                })
            }
            _ => "other",
        }
    }

    fn is_timeout_io(error: &io::Error) -> bool {
        matches!(
            error.kind(),
            io::ErrorKind::TimedOut
                | io::ErrorKind::Interrupted
                | io::ErrorKind::WouldBlock
                | io::ErrorKind::ConnectionReset
                | io::ErrorKind::ConnectionAborted
                | io::ErrorKind::ConnectionRefused
                | io::ErrorKind::NotConnected
                | io::ErrorKind::BrokenPipe
                | io::ErrorKind::NetworkUnreachable
                | io::ErrorKind::HostUnreachable
        )
    }
}

#[async_trait]
impl<M: S3RequestMetrics> ObjectStore for ObjectStoreMetricsDecorator<M> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let started_at = Instant::now();
        let outcome = self.inner.put_opts(location, payload, opts).await;
        self.record_object_store_outcome("put", started_at, &outcome);
        outcome
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        let operation = if options.head {
            "head"
        } else if options.range.is_some() {
            "get_range"
        } else {
            "get"
        };
        let started_at = Instant::now();
        let outcome = self.inner.get_opts(location, options).await;
        self.record_object_store_outcome(operation, started_at, &outcome);
        outcome
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner_stream = self.inner.list(prefix);
        self.instrument_list_stream("list", inner_stream)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        let inner_stream = self.inner.list_with_offset(prefix, offset);
        self.instrument_list_stream("list_with_offset", inner_stream)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let started_at = Instant::now();
        let outcome = self.inner.list_with_delimiter(prefix).await;
        self.record_object_store_outcome("list_with_delimiter", started_at, &outcome);
        outcome
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// Metrics collected during OTLP intake (HTTP/gRPC).
#[derive(Clone)]
pub struct OtlpMetrics {
    enabled: bool,
    requests_total: Counter<u64>,
    request_duration: Histogram<f64>,
    request_size_bytes: Histogram<f64>,
    decode_duration: Histogram<f64>,
    decode_errors_total: Counter<u64>,
    transform_duration: Histogram<f64>,
    records_per_request: Histogram<f64>,
    wal_sorting_duration: Histogram<f64>,
    wal_enqueue_duration: Histogram<f64>,
    wal_ack_duration: Histogram<f64>,
    wal_queue_unavailable_total: Counter<u64>,
}

impl OtlpMetrics {
    /// Build a metrics recorder that performs no-ops.
    pub fn new_disabled() -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("ingest_otlp");

        Self {
            enabled: false,
            requests_total: meter.u64_counter("icegate_ingest_otlp_requests").build(),
            request_duration: meter.f64_histogram("icegate_ingest_otlp_request_duration").build(),
            request_size_bytes: meter.f64_histogram("icegate_ingest_otlp_request_size").build(),
            decode_duration: meter.f64_histogram("icegate_ingest_otlp_decode_duration").build(),
            decode_errors_total: meter.u64_counter("icegate_ingest_otlp_decode_errors").build(),
            transform_duration: meter.f64_histogram("icegate_ingest_otlp_transform_duration").build(),
            wal_sorting_duration: meter.f64_histogram("icegate_ingest_otlp_wal_sorting_duration").build(),
            records_per_request: meter.f64_histogram("icegate_ingest_otlp_records_per_request").build(),
            wal_enqueue_duration: meter.f64_histogram("icegate_ingest_wal_enqueue_duration").build(),
            wal_ack_duration: meter.f64_histogram("icegate_ingest_wal_ack_duration").build(),
            wal_queue_unavailable_total: meter.u64_counter("icegate_ingest_wal_queue_unavailable").build(),
        }
    }

    /// Build a metrics recorder using the provided meter.
    pub fn new(meter: &Meter) -> Self {
        let requests_total = meter
            .u64_counter("icegate_ingest_otlp_requests")
            .with_description("Total number of OTLP intake requests")
            .build();
        let request_duration = meter
            .f64_histogram("icegate_ingest_otlp_request_duration")
            .with_description("End-to-end OTLP request duration")
            .with_unit("s")
            .build();
        let request_size_bytes = meter
            .f64_histogram("icegate_ingest_otlp_request_size")
            .with_description("OTLP request body size")
            .with_unit("By")
            .build();
        let decode_duration = meter
            .f64_histogram("icegate_ingest_otlp_decode_duration")
            .with_description("OTLP decode duration")
            .with_unit("s")
            .build();
        let decode_errors_total = meter
            .u64_counter("icegate_ingest_otlp_decode_errors")
            .with_description("OTLP decode/validation errors")
            .build();
        let transform_duration = meter
            .f64_histogram("icegate_ingest_otlp_transform_duration")
            .with_description("OTLP transform duration")
            .with_unit("s")
            .build();
        let pre_wal_prepare_duration = meter
            .f64_histogram("icegate_ingest_otlp_wal_sorting_duration")
            .with_description("OTLP WAL sorting duration")
            .with_unit("s")
            .build();
        let records_per_request = meter
            .f64_histogram("icegate_ingest_otlp_records_per_request")
            .with_description("Records per OTLP request")
            .build();
        let wal_enqueue_duration = meter
            .f64_histogram("icegate_ingest_wal_enqueue_duration")
            .with_description("Time to enqueue WAL write request")
            .with_unit("s")
            .build();
        let wal_ack_duration = meter
            .f64_histogram("icegate_ingest_wal_ack_duration")
            .with_description("Time to wait for WAL write acknowledgment")
            .with_unit("s")
            .build();
        let wal_queue_unavailable_total = meter
            .u64_counter("icegate_ingest_wal_queue_unavailable")
            .with_description("Failed WAL enqueue attempts")
            .build();

        Self {
            enabled: true,
            requests_total,
            request_duration,
            request_size_bytes,
            decode_duration,
            decode_errors_total,
            transform_duration,
            wal_sorting_duration: pre_wal_prepare_duration,
            records_per_request,
            wal_enqueue_duration,
            wal_ack_duration,
            wal_queue_unavailable_total,
        }
    }

    /// Record a total request count.
    pub fn add_request(&self, protocol: &str, signal: &str, status: &str, encoding: &str) {
        if !self.enabled {
            return;
        }
        self.requests_total.add(
            1,
            &[
                KeyValue::new("protocol", protocol.to_string()),
                KeyValue::new("signal", signal.to_string()),
                KeyValue::new("status", status.to_string()),
                KeyValue::new("encoding", encoding.to_string()),
            ],
        );
    }

    /// Record end-to-end request duration.
    pub fn record_request_duration(&self, duration: Duration, protocol: &str, signal: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.request_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("protocol", protocol.to_string()),
                KeyValue::new("signal", signal.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record request size in bytes.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_request_size(&self, bytes: usize, protocol: &str, signal: &str, encoding: &str) {
        if !self.enabled {
            return;
        }
        self.request_size_bytes.record(
            bytes as f64,
            &[
                KeyValue::new("protocol", protocol.to_string()),
                KeyValue::new("signal", signal.to_string()),
                KeyValue::new("encoding", encoding.to_string()),
            ],
        );
    }

    /// Record decode duration.
    pub fn record_decode_duration(&self, duration: Duration, protocol: &str, signal: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.decode_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("protocol", protocol.to_string()),
                KeyValue::new("signal", signal.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record decode error count.
    pub fn add_decode_error(&self, protocol: &str, signal: &str, reason: &str) {
        if !self.enabled {
            return;
        }
        self.decode_errors_total.add(
            1,
            &[
                KeyValue::new("protocol", protocol.to_string()),
                KeyValue::new("signal", signal.to_string()),
                KeyValue::new("reason", reason.to_string()),
            ],
        );
    }

    /// Record transform duration.
    pub fn record_transform_duration(&self, duration: Duration, signal: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.transform_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("signal", signal.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record WAL sorting duration.
    pub fn record_wal_sorting_duration(&self, duration: Duration, signal: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.wal_sorting_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("signal", signal.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record records per request.
    #[allow(clippy::cast_precision_loss)]
    pub fn record_records_per_request(&self, count: usize, signal: &str) {
        if !self.enabled {
            return;
        }
        self.records_per_request
            .record(count as f64, &[KeyValue::new("signal", signal.to_string())]);
    }

    /// Record WAL enqueue duration.
    pub fn record_wal_enqueue_duration(&self, duration: Duration, topic: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.wal_enqueue_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record WAL ack wait duration.
    pub fn record_wal_ack_duration(&self, duration: Duration, topic: &str, status: &str) {
        if !self.enabled {
            return;
        }
        self.wal_ack_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    /// Record WAL queue unavailable.
    pub fn add_wal_queue_unavailable(&self, topic: &str, reason: &str) {
        if !self.enabled {
            return;
        }
        self.wal_queue_unavailable_total.add(
            1,
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("reason", reason.to_string()),
            ],
        );
    }
}

/// Helper that records OTLP request metrics for a single intake request.
pub struct OtlpRequestRecorder<'a> {
    metrics: &'a OtlpMetrics,
    protocol: &'a str,
    signal: &'a str,
    encoding: &'a str,
    request_start: Instant,
}

impl<'a> OtlpRequestRecorder<'a> {
    /// Create a new recorder for a single OTLP request.
    pub fn new(metrics: &'a OtlpMetrics, protocol: &'a str, signal: &'a str, encoding: &'a str) -> Self {
        Self {
            metrics,
            protocol,
            signal,
            encoding,
            request_start: Instant::now(),
        }
    }

    /// Record request size in bytes.
    pub fn record_request_size(&self, bytes: usize) {
        self.metrics
            .record_request_size(bytes, self.protocol, self.signal, self.encoding);
    }

    /// Record decode duration and return the decode result.
    pub fn record_decode<F, T>(&self, content_type: &str, decode: F) -> Result<T, IngestError>
    where
        F: FnOnce() -> Result<T, IngestError>,
    {
        // metric uses only in http hanler (TODO: otlp_grpc/services.rs:56)
        let start = Instant::now();
        match decode() {
            Ok(value) => {
                self.metrics
                    .record_decode_duration(start.elapsed(), self.protocol, self.signal, "ok");
                Ok(value)
            }
            Err(err) => {
                self.metrics
                    .record_decode_duration(start.elapsed(), self.protocol, self.signal, "error");
                if let Some(reason) = Self::otlp_decode_error_reason(&err, content_type) {
                    self.metrics.add_decode_error(self.protocol, self.signal, reason);
                }
                self.finish_with_status("error");
                Err(err)
            }
        }
    }

    /// Record decode duration when decoding is external.
    pub fn record_decode_duration(&self, duration: Duration, status: &str) {
        self.metrics
            .record_decode_duration(duration, self.protocol, self.signal, status);
    }

    /// Record transform duration for the provided function.
    pub fn record_transform<F, T>(&self, transform: F) -> T
    where
        F: FnOnce() -> T,
    {
        let start = Instant::now();
        let result = transform();
        self.metrics.record_transform_duration(start.elapsed(), self.signal, "ok");
        result
    }

    /// Record records per request.
    pub fn record_records_per_request(&self, count: usize) {
        self.metrics.record_records_per_request(count, self.signal);
    }

    /// Record transform duration.
    pub fn record_transform_duration(&self, duration: Duration, signal: &str, status: &str) {
        self.metrics.record_transform_duration(duration, signal, status);
    }

    /// Record WAL sorting duration.
    pub fn record_wal_sorting_duration(&self, duration: Duration, signal: &str, status: &str) {
        self.metrics.record_wal_sorting_duration(duration, signal, status);
    }

    /// Record WAL enqueue duration.
    pub fn record_wal_enqueue_duration(&self, duration: Duration, topic: &str, status: &str) {
        self.metrics.record_wal_enqueue_duration(duration, topic, status);
    }

    /// Record WAL ack wait duration.
    pub fn record_wal_ack_duration(&self, duration: Duration, topic: &str, status: &str) {
        self.metrics.record_wal_ack_duration(duration, topic, status);
    }

    /// Record WAL queue unavailable.
    pub fn add_wal_queue_unavailable(&self, topic: &str, reason: &str) {
        self.metrics.add_wal_queue_unavailable(topic, reason);
    }

    /// Finish request with status.
    pub fn finish_with_status(&self, status: &str) {
        self.metrics.add_request(self.protocol, self.signal, status, self.encoding);
        self.metrics
            .record_request_duration(self.request_start.elapsed(), self.protocol, self.signal, status);
    }

    /// Finish request with ok status.
    pub fn finish_ok(&self) {
        self.finish_with_status("ok");
    }

    /// Finish request with partial status.
    pub fn finish_partial(&self) {
        self.finish_with_status("partial");
    }

    /// Finish request with error status.
    pub fn finish_error(&self) {
        self.finish_with_status("error");
    }

    fn otlp_decode_error_reason(error: &IngestError, content_type: &str) -> Option<&'static str> {
        match error {
            IngestError::Validation(message) => {
                if message.starts_with("Unsupported Content-Type") {
                    Some("invalid_content_type")
                } else {
                    None
                }
            }
            IngestError::Decode(_) => {
                if content_type.starts_with("application/json") {
                    Some("json_decode")
                } else {
                    Some("protobuf_decode")
                }
            }
            _ => None,
        }
    }
}

/// Metrics collected during WAL queue writes.
#[derive(Clone)]
pub struct WalWriterMetrics {
    enabled: bool,
    inner: Arc<dyn QueueWriterEvents>,
    pending_batches: Gauge<u64>,
    pending_records: Gauge<u64>,
    pending_bytes: Gauge<u64>,
    flush_duration: Histogram<f64>,
    segments_total: Counter<u64>,
    segment_bytes: Histogram<f64>,
    row_groups_per_segment: Histogram<f64>,
    records_per_segment: Histogram<f64>,
    write_retries_total: Counter<u64>,
    write_errors_total: Counter<u64>,
}

impl WalWriterMetrics {
    /// Build a metrics recorder that performs no-ops.
    pub fn new_disabled(inner: Arc<dyn QueueWriterEvents>) -> Self {
        let provider = SdkMeterProvider::builder().build();
        let meter = provider.meter("ingest_wal");

        Self {
            enabled: false,
            inner,
            pending_batches: meter.u64_gauge("icegate_ingest_wal_pending_batches").build(),
            pending_records: meter.u64_gauge("icegate_ingest_wal_pending_records").build(),
            pending_bytes: meter.u64_gauge("icegate_ingest_wal_pending").build(),
            flush_duration: meter.f64_histogram("icegate_ingest_wal_flush_duration").build(),
            segments_total: meter.u64_counter("icegate_ingest_wal_segments").build(),
            segment_bytes: meter.f64_histogram("icegate_ingest_wal_segment").build(),
            row_groups_per_segment: meter.f64_histogram("icegate_ingest_wal_row_groups_per_segment").build(),
            records_per_segment: meter.f64_histogram("icegate_ingest_wal_records_per_segment").build(),
            write_retries_total: meter.u64_counter("icegate_ingest_wal_write_retries").build(),
            write_errors_total: meter.u64_counter("icegate_ingest_wal_write_errors").build(),
        }
    }

    /// Build a metrics recorder using the provided meter.
    pub fn new(meter: &Meter, inner: Arc<dyn QueueWriterEvents>) -> Self {
        let pending_batches = meter
            .u64_gauge("icegate_ingest_wal_pending_batches")
            .with_description("Number of batches in WAL accumulator")
            .build();
        let pending_records = meter
            .u64_gauge("icegate_ingest_wal_pending_records")
            .with_description("Number of records in WAL accumulator")
            .build();
        let pending_bytes = meter
            .u64_gauge("icegate_ingest_wal_pending")
            .with_description("Estimated bytes in WAL accumulator")
            .with_unit("By")
            .build();
        let flush_duration = meter
            .f64_histogram("icegate_ingest_wal_flush_duration")
            .with_description("Flush duration (concat + write)")
            .with_unit("s")
            .build();
        let segments_total = meter
            .u64_counter("icegate_ingest_wal_segments")
            .with_description("WAL segments written")
            .build();
        let segment_bytes = meter
            .f64_histogram("icegate_ingest_wal_segment")
            .with_description("WAL segment size in bytes")
            .with_unit("By")
            .build();
        let row_groups_per_segment = meter
            .f64_histogram("icegate_ingest_wal_row_groups_per_segment")
            .with_description("Row groups per WAL segment")
            .build();
        let records_per_segment = meter
            .f64_histogram("icegate_ingest_wal_records_per_segment")
            .with_description("Records per WAL segment")
            .build();
        let write_retries_total = meter
            .u64_counter("icegate_ingest_wal_write_retries")
            .with_description("WAL write retries")
            .build();
        let write_errors_total = meter
            .u64_counter("icegate_ingest_wal_write_errors")
            .with_description("WAL write errors")
            .build();

        Self {
            enabled: true,
            inner,
            pending_batches,
            pending_records,
            pending_bytes,
            flush_duration,
            segments_total,
            segment_bytes,
            row_groups_per_segment,
            records_per_segment,
            write_retries_total,
            write_errors_total,
        }
    }
}

impl QueueWriterEvents for WalWriterMetrics {
    fn on_accumulator_state_update(&self, topic: &str, batches: u64, records: u64, bytes: u64) {
        self.inner.on_accumulator_state_update(topic, batches, records, bytes);
        if !self.enabled {
            return;
        }
        self.pending_batches
            .record(batches, &[KeyValue::new("topic", topic.to_string())]);
        self.pending_records
            .record(records, &[KeyValue::new("topic", topic.to_string())]);
        self.pending_bytes.record(bytes, &[KeyValue::new("topic", topic.to_string())]);
    }

    fn on_flush_finish(&self, topic: &str, status: &str, duration: Duration) {
        self.inner.on_flush_finish(topic, status, duration);
        if !self.enabled {
            return;
        }
        self.flush_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("status", status.to_string()),
            ],
        );
    }

    #[allow(clippy::cast_precision_loss)]
    fn on_write_complete(&self, topic: &str, outcome: WriteBatchOutcome) {
        self.inner.on_write_complete(topic, outcome);
        if !self.enabled {
            return;
        }
        let labels = &[KeyValue::new("topic", topic.to_string())];
        self.segments_total.add(1, labels);
        self.segment_bytes.record(outcome.size_bytes as f64, labels);
        self.row_groups_per_segment.record(outcome.row_groups as f64, labels);
        self.records_per_segment.record(outcome.records as f64, labels);
    }

    fn on_write_retry(&self, topic: &str, reason: &str) {
        self.inner.on_write_retry(topic, reason);
        if !self.enabled {
            return;
        }
        self.write_retries_total.add(
            1,
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("reason", reason.to_string()),
            ],
        );
    }

    fn on_write_error(&self, topic: &str, reason: &str) {
        self.inner.on_write_error(topic, reason);
        if !self.enabled {
            return;
        }
        self.write_errors_total.add(
            1,
            &[
                KeyValue::new("topic", topic.to_string()),
                KeyValue::new("reason", reason.to_string()),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use bytes::Bytes;
    use futures::StreamExt;
    use icegate_queue::{ParquetQueueReader, Topic};
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
    };
    use opentelemetry::{KeyValue, metrics::MeterProvider as _};
    use opentelemetry_sdk::metrics::{
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
        data::{AggregatedMetrics, MetricData},
    };
    use tokio_util::sync::CancellationToken;

    use super::*;

    struct FlakyListStore {
        inner: Arc<dyn ObjectStore>,
        failures_left: Mutex<usize>,
        attempts: AtomicUsize,
    }

    impl FlakyListStore {
        fn new(inner: Arc<dyn ObjectStore>, failures: usize) -> Self {
            Self {
                inner,
                failures_left: Mutex::new(failures),
                attempts: AtomicUsize::new(0),
            }
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }
    }

    impl std::fmt::Debug for FlakyListStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.debug_struct("FlakyListStore").finish()
        }
    }

    impl std::fmt::Display for FlakyListStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "FlakyListStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FlakyListStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn delete(&self, location: &Path) -> object_store::Result<()> {
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let should_fail = {
                let mut failures_left = self.failures_left.lock().expect("poisoned lock");
                if *failures_left == 0 {
                    false
                } else {
                    *failures_left -= 1;
                    true
                }
            };
            if should_fail {
                let error = object_store::Error::Generic {
                    store: "FlakyListStore",
                    source: Box::new(io::Error::new(io::ErrorKind::TimedOut, "temporary timeout")),
                };
                return futures::stream::once(async move { Err(error) }).boxed();
            }
            self.inner.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn metric_labels(data_point: &[KeyValue]) -> Vec<(String, String)> {
        let mut labels = data_point
            .iter()
            .map(|kv| (kv.key.as_str().to_string(), kv.value.as_str().into_owned()))
            .collect::<Vec<_>>();
        labels.sort();
        labels
    }

    fn labels_match(labels: &[(String, String)], expected: &[(&str, &str)]) -> bool {
        if labels.len() != expected.len() {
            return false;
        }
        expected.iter().all(|(key, value)| {
            labels
                .iter()
                .any(|(label_key, label_value)| label_key == key && label_value == value)
        })
    }

    fn build_meter_provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        (provider, exporter)
    }

    fn find_histogram_count(
        exporter: &InMemoryMetricExporter,
        metric_name: &str,
        expected_labels: &[(&str, &str)],
    ) -> u64 {
        let mut total = 0_u64;
        let resource_metrics = exporter.get_finished_metrics().expect("failed to read metrics");
        for rm in resource_metrics {
            for sm in rm.scope_metrics() {
                for metric in sm.metrics() {
                    if metric.name() != metric_name {
                        continue;
                    }
                    if let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() {
                        for point in histogram.data_points() {
                            let labels = metric_labels(&point.attributes().cloned().collect::<Vec<_>>());
                            if labels_match(&labels, expected_labels) {
                                total = total.saturating_add(point.count());
                            }
                        }
                    }
                }
            }
        }
        total
    }

    #[tokio::test]
    async fn queue_reader_object_store_metrics_decorator_records_success_and_error() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("queue/logs/00000000000000000000.parquet");
        store
            .put(&path, PutPayload::from_bytes(Bytes::from_static(b"segment")))
            .await
            .expect("failed to put object");

        let (provider, exporter) = build_meter_provider();
        let meter = provider.meter("test_queue_reader_object_store_metrics");
        let metrics = QueueReaderS3Metrics::new(&meter);
        let decorated: Arc<dyn ObjectStore> = Arc::new(ObjectStoreMetricsDecorator::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            metrics,
        ));

        decorated.head(&path).await.expect("head should succeed");
        let missing_path = Path::from("queue/logs/missing.parquet");
        let _ = decorated.head(&missing_path).await;
        let range_bytes = decorated.get_range(&path, 0..3).await.expect("range read should succeed");
        assert_eq!(range_bytes, Bytes::from_static(b"seg"));
        provider.force_flush().expect("failed to flush metrics");

        let ok_labels = &[
            ("operation", "head"),
            ("status", "ok"),
            ("error_kind", "none"),
            ("component", "queue_reader"),
        ];
        let error_labels = &[
            ("operation", "head"),
            ("status", "error"),
            ("error_kind", "not_found"),
            ("component", "queue_reader"),
        ];
        let get_range_labels = &[
            ("operation", "get_range"),
            ("status", "ok"),
            ("error_kind", "none"),
            ("component", "queue_reader"),
        ];

        assert!(
            find_histogram_count(
                &exporter,
                "icegate_ingest_shift_queue_reader_s3_request_duration",
                ok_labels,
            ) >= 1
        );
        assert!(
            find_histogram_count(
                &exporter,
                "icegate_ingest_shift_queue_reader_s3_request_duration",
                error_labels,
            ) >= 1
        );
        assert_eq!(
            find_histogram_count(
                &exporter,
                "icegate_ingest_shift_queue_reader_s3_request_duration",
                get_range_labels,
            ),
            1
        );
    }

    #[test]
    fn otlp_request_recorder_tracks_transform_and_pre_wal_prepare_separately() {
        let (provider, exporter) = build_meter_provider();
        let meter = provider.meter("test_otlp_metrics");
        let metrics = OtlpMetrics::new(&meter);
        let recorder = OtlpRequestRecorder::new(&metrics, "http", "logs", "protobuf");

        recorder.record_transform_duration(Duration::from_millis(2), "logs", "ok");
        recorder.record_wal_sorting_duration(Duration::from_millis(3), "logs", "ok");

        provider.force_flush().expect("failed to flush metrics");

        assert_eq!(
            find_histogram_count(
                &exporter,
                "icegate_ingest_otlp_transform_duration",
                &[("signal", "logs"), ("status", "ok")],
            ),
            1
        );
        assert_eq!(
            find_histogram_count(
                &exporter,
                "icegate_ingest_otlp_wal_sorting_duration",
                &[("signal", "logs"), ("status", "ok")],
            ),
            1
        );
    }

    #[tokio::test]
    async fn queue_reader_retries_are_counted_as_multiple_s3_attempts() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("queue/logs/00000000000000000000.parquet");
        store
            .put(&path, PutPayload::from_bytes(Bytes::from_static(b"segment")))
            .await
            .expect("failed to put object");

        let base_store: Arc<dyn ObjectStore> = Arc::clone(&store) as Arc<dyn ObjectStore>;
        let flaky_store = Arc::new(FlakyListStore::new(base_store, 1));

        let (provider, exporter) = build_meter_provider();
        let meter = provider.meter("test_queue_reader_retry_metrics");
        let metrics = QueueReaderS3Metrics::new(&meter);
        let decorated_store: Arc<dyn ObjectStore> = Arc::new(ObjectStoreMetricsDecorator::new(
            Arc::clone(&flaky_store) as Arc<dyn ObjectStore>,
            metrics,
        ));
        let reader = ParquetQueueReader::new("queue", decorated_store, 128).expect("reader init failed");

        let topic: Topic = "logs".to_string();
        let cancel = CancellationToken::new();
        let segments = reader
            .list_segments(&topic, 0, &cancel)
            .await
            .expect("list_segments should succeed after retry");

        assert_eq!(segments.len(), 1);
        assert!(flaky_store.attempts() > 1, "expected retries for list_with_offset");

        provider.force_flush().expect("failed to flush metrics");
        let operation_labels = &[
            ("operation", "list_with_offset"),
            ("component", "queue_reader"),
            ("status", "error"),
            ("error_kind", "timeout"),
        ];
        let success_labels = &[
            ("operation", "list_with_offset"),
            ("component", "queue_reader"),
            ("status", "ok"),
            ("error_kind", "none"),
        ];
        let error_attempts = find_histogram_count(
            &exporter,
            "icegate_ingest_shift_queue_reader_s3_request_duration",
            operation_labels,
        );
        let success_attempts = find_histogram_count(
            &exporter,
            "icegate_ingest_shift_queue_reader_s3_request_duration",
            success_labels,
        );
        assert!(error_attempts >= 1, "retry error attempt must be counted");
        assert!(success_attempts >= 1, "successful retry attempt must be counted");
        assert!(
            error_attempts + success_attempts > 1,
            "all retry attempts must contribute to request rate metric"
        );
    }

    #[tokio::test]
    async fn queue_writer_object_store_metrics_decorator_records_put_conflict_and_head() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("queue/logs/00000000000000000000.parquet");

        let (provider, exporter) = build_meter_provider();
        let meter = provider.meter("test_queue_writer_object_store_metrics");
        let metrics = QueueWriterS3Metrics::new(&meter);
        let decorated: Arc<dyn ObjectStore> = Arc::new(ObjectStoreMetricsDecorator::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            metrics,
        ));

        let create_opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        decorated
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"segment")),
                create_opts.clone(),
            )
            .await
            .expect("first put should succeed");
        let _ = decorated
            .put_opts(
                &path,
                PutPayload::from_bytes(Bytes::from_static(b"segment")),
                create_opts,
            )
            .await;
        decorated.head(&path).await.expect("head should succeed");

        provider.force_flush().expect("failed to flush metrics");

        let put_ok_labels = &[
            ("operation", "put"),
            ("status", "ok"),
            ("error_kind", "none"),
            ("component", "queue_writer"),
        ];
        let put_conflict_labels = &[
            ("operation", "put"),
            ("status", "error"),
            ("error_kind", "already_exists"),
            ("component", "queue_writer"),
        ];
        let head_ok_labels = &[
            ("operation", "head"),
            ("status", "ok"),
            ("error_kind", "none"),
            ("component", "queue_writer"),
        ];

        assert_eq!(
            find_histogram_count(
                &exporter,
                "icegate_ingest_shift_queue_writer_s3_request_duration",
                put_ok_labels,
            ),
            1
        );
        assert!(
            find_histogram_count(
                &exporter,
                "icegate_ingest_shift_queue_writer_s3_request_duration",
                put_conflict_labels,
            ) >= 1
        );
        assert_eq!(
            find_histogram_count(
                &exporter,
                "icegate_ingest_shift_queue_writer_s3_request_duration",
                head_ok_labels,
            ),
            1
        );
    }

    #[tokio::test]
    async fn queue_writer_object_store_metrics_decorator_records_list_on_first_item() {
        let store = Arc::new(InMemory::new());
        let path_1 = Path::from("queue/logs/00000000000000000000.parquet");
        let path_2 = Path::from("queue/logs/00000000000000000001.parquet");
        store
            .put(&path_1, PutPayload::from_bytes(Bytes::from_static(b"segment-1")))
            .await
            .expect("failed to put object");
        store
            .put(&path_2, PutPayload::from_bytes(Bytes::from_static(b"segment-2")))
            .await
            .expect("failed to put object");

        let (provider, exporter) = build_meter_provider();
        let meter = provider.meter("test_queue_writer_list_metrics");
        let metrics = QueueWriterS3Metrics::new(&meter);
        let decorated: Arc<dyn ObjectStore> = Arc::new(ObjectStoreMetricsDecorator::new(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            metrics,
        ));

        let prefix = Path::from("queue/logs/");
        let mut stream = decorated.list(Some(&prefix));
        let first = stream.next().await.expect("expected at least one list item");
        assert!(first.is_ok(), "first list item should be ok");
        drop(stream);

        provider.force_flush().expect("failed to flush metrics");

        let list_ok_labels = &[
            ("operation", "list"),
            ("status", "ok"),
            ("error_kind", "none"),
            ("component", "queue_writer"),
        ];

        assert_eq!(
            find_histogram_count(
                &exporter,
                "icegate_ingest_shift_queue_writer_s3_request_duration",
                list_ok_labels,
            ),
            1
        );
    }
}
