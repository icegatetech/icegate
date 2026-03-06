//! OTLP gRPC service implementations.
//!
//! Implements the `OpenTelemetry` Protocol service traits for logs, traces, and
//! metrics ingestion via gRPC.

use std::time::Instant;

use icegate_common::{LOGS_TOPIC, TENANT_ID_HEADER, is_valid_tenant_id};
use icegate_queue::WriteChannel;
use opentelemetry_proto::tonic::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse, metrics_service_server::MetricsService},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService},
};
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::infra::metrics::{OtlpMetrics, OtlpRequestRecorder};
use crate::transform;

const SIGNAL_LOGS: &str = "logs";
const PROTOCOL_GRPC: &str = "grpc";
const ENCODING_PROTOBUF: &str = "protobuf";
const STATUS_OK: &str = "ok";
const STATUS_ERROR: &str = "error";
const WAL_REASON_CHANNEL_CLOSED: &str = "channel_closed";

/// Extract tenant ID from gRPC request metadata.
///
/// Returns `Some(tenant_id)` if the `x-scope-orgid` metadata key is present and
/// contains a valid value (non-empty, ASCII alphanumeric/hyphens/underscores).
/// Returns `None` otherwise, which falls back to `DEFAULT_TENANT_ID` downstream.
fn extract_tenant_id<T>(request: &Request<T>) -> Option<String> {
    request
        .metadata()
        .get(TENANT_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| is_valid_tenant_id(s))
        .map(String::from)
}

/// OTLP gRPC service implementation.
///
/// Handles `OpenTelemetry` Protocol requests for logs, traces, and metrics.
/// Writes data to the WAL queue for durable storage.
#[derive(Clone)]
pub struct OtlpGrpcService {
    /// Write channel for sending batches to the WAL queue.
    write_channel: WriteChannel,
    /// Metrics recorder for OTLP intake.
    metrics: OtlpMetrics,
}

impl OtlpGrpcService {
    /// Create a new OTLP gRPC service.
    pub const fn new(write_channel: WriteChannel, metrics: OtlpMetrics) -> Self {
        Self { write_channel, metrics }
    }
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcService {
    /// Handle OTLP logs export request.
    ///
    /// Transforms OTLP log records to Arrow `RecordBatch` and writes to the WAL
    /// queue. Returns partial success response with count of rejected
    /// records.
    #[allow(clippy::cast_possible_wrap)]
    #[tracing::instrument(name = "export_logs", skip(self, request))]
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        debug!("Start handle OTLP GRPC request");
        let request_size = request.get_ref().encoded_len();
        let request_metrics = OtlpRequestRecorder::new(&self.metrics, PROTOCOL_GRPC, SIGNAL_LOGS, ENCODING_PROTOBUF);
        request_metrics.record_request_size(request_size);

        let tenant_id = extract_tenant_id(&request);

        // TODO(med): instrument gRPC decoding time by wrapping tonic/prost codec; handler receives decoded payload.
        let export_request = request.into_inner();

        // Transform OTLP logs to Arrow RecordBatch (offload to blocking thread)
        let span = tracing::Span::current();
        let batch = tokio::task::spawn_blocking(move || {
            span.in_scope(|| transform::logs_to_record_batch(&export_request, tenant_id.as_deref()))
        })
        .await
        .map_err(|e| Status::internal(format!("Transform task panicked: {e}")))?
        .map_err(|e| Status::internal(format!("Failed to transform logs: {e}")))?;
        let Some(batch) = batch else {
            // No records to process - return success with 0 rejected
            request_metrics.record_records_per_request(0);
            request_metrics.finish_ok();
            return Ok(Response::new(ExportLogsServiceResponse { partial_success: None }));
        };

        let record_count = batch.num_rows();
        debug!(records = record_count, "Transformed OTLP logs to RecordBatch");
        request_metrics.record_records_per_request(record_count);

        // Create write request for the WAL queue
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let write_request = icegate_queue::WriteRequest {
            topic: LOGS_TOPIC.to_string(),
            batch,
            group_by_column: Some("tenant_id".to_string()),
            response_tx,
            trace_context: icegate_common::extract_current_trace_context(),
        };

        // Send to WAL queue
        let enqueue_start = Instant::now();
        self.write_channel.send(write_request).await.map_err(|e| {
            request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
            request_metrics.add_wal_queue_unavailable(LOGS_TOPIC, WAL_REASON_CHANNEL_CLOSED);
            request_metrics.finish_error();
            Status::unavailable(format!("WAL queue unavailable: {e}"))
        })?;
        request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), LOGS_TOPIC, STATUS_OK);

        // Wait for write result
        let ack_start = Instant::now();
        let result = response_rx.await.map_err(|e| {
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
            request_metrics.finish_error();
            Status::internal(format!("Failed to receive write result: {e}"))
        })?;

        // Add link to flush operation if trace context is available
        if let Some(tc) = result.trace_context() {
            icegate_common::add_span_link(tc);
        }

        match result {
            icegate_queue::WriteResult::Success { offset, records, .. } => {
                debug!(offset, records, "OTLP GRPC request ended successfully");
                request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_OK);
                request_metrics.finish_ok();
                Ok(Response::new(ExportLogsServiceResponse { partial_success: None }))
            }
            icegate_queue::WriteResult::Failed { reason, .. } => {
                // Return partial success with all records rejected
                request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
                request_metrics.finish_partial();
                Ok(Response::new(ExportLogsServiceResponse {
                    partial_success: Some(
                        opentelemetry_proto::tonic::collector::logs::v1::ExportLogsPartialSuccess {
                            rejected_log_records: record_count as i64,
                            error_message: reason,
                        },
                    ),
                }))
            }
        }
    }
}

#[tonic::async_trait]
impl TraceService for OtlpGrpcService {
    /// Handle OTLP traces export request.
    ///
    /// # TODO
    /// - Parse OTLP spans from request
    /// - Transform to Iceberg schema format (from schema.rs)
    /// - Write spans to Iceberg spans table via catalog
    /// - Handle batching and backpressure
    /// - Return partial success for rejected spans
    #[tracing::instrument(
        skip(self, _request),
        fields(method = "/opentelemetry.proto.collector.trace.v1.TraceService/Export")
    )]
    async fn export(
        &self,
        _request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        Err(Status::unimplemented(
            "OTLP traces ingestion: Parse OTLP → transform → write to Iceberg",
        ))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpGrpcService {
    /// Handle OTLP metrics export request.
    ///
    /// # TODO
    /// - Parse OTLP metrics from request
    /// - Handle different metric types (gauge, sum, histogram, summary)
    /// - Transform to Iceberg schema format (from schema.rs)
    /// - Write metrics to Iceberg metrics table via catalog
    /// - Handle batching and backpressure
    /// - Return partial success for rejected metrics
    #[tracing::instrument(
        skip(self, _request),
        fields(method = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
    )]
    async fn export(
        &self,
        _request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        Err(Status::unimplemented(
            "OTLP metrics ingestion: Parse OTLP → transform → write to Iceberg",
        ))
    }
}
