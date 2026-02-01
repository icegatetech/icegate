use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use icegate_queue::{QueueWriterEvents, WriteBatchOutcome};
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
    segment_record_batches_per_task: Histogram<f64>,
    segment_rows_per_task: Histogram<f64>,
    parquet_write_duration: Histogram<f64>,
    parquet_files_total: Counter<u64>,
    bytes_written_total: Counter<u64>,
    get_data_files_duration: Histogram<f64>,
    commit_duration: Histogram<f64>,
    already_committed_total: Counter<u64>,
    task_failures_total: Counter<u64>,
    task_success_total: Counter<u64>,
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
            segment_record_batches_per_task: meter
                .f64_histogram("icegate_ingest_shift_segment_record_batches_per_task")
                .build(),
            segment_rows_per_task: meter.f64_histogram("icegate_ingest_shift_segment_rows_per_task").build(),
            parquet_write_duration: meter.f64_histogram("icegate_ingest_shift_parquet_write_duration").build(),
            parquet_files_total: meter.u64_counter("icegate_ingest_shift_parquet_files").build(),
            bytes_written_total: meter.u64_counter("icegate_ingest_shift_bytes_written").build(),
            get_data_files_duration: meter.f64_histogram("icegate_ingest_shift_get_data_files_duration").build(),
            commit_duration: meter.f64_histogram("icegate_ingest_shift_commit_duration").build(),
            already_committed_total: meter.u64_counter("icegate_ingest_shift_already_committed").build(),
            task_failures_total: meter.u64_counter("icegate_ingest_shift_task_failures").build(),
            task_success_total: meter.u64_counter("icegate_ingest_shift_task_success").build(),
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

        Self {
            enabled: true,
            plan_duration,
            queue_plan_duration,
            queue_read_segment_duration,
            planned_segments_total,
            planned_record_batches_total,
            planned_tasks_total,
            backlog_segments,
            segment_record_batches_per_task,
            segment_rows_per_task,
            parquet_write_duration,
            parquet_files_total,
            bytes_written_total,
            get_data_files_duration,
            commit_duration,
            already_committed_total,
            task_failures_total,
            task_success_total,
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
