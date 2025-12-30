//! OTLP gRPC service implementations.
//!
//! Implements the `OpenTelemetry` Protocol service traits for logs, traces, and
//! metrics ingestion via gRPC.

use icegate_common::LOGS_TOPIC;
use icegate_queue::WriteChannel;
use opentelemetry_proto::tonic::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse, metrics_service_server::MetricsService},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService},
};
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::transform;

/// OTLP gRPC service implementation.
///
/// Handles `OpenTelemetry` Protocol requests for logs, traces, and metrics.
/// Writes data to the WAL queue for durable storage.
#[derive(Clone)]
pub struct OtlpGrpcService {
    /// Write channel for sending batches to the WAL queue.
    write_channel: WriteChannel,
}

impl OtlpGrpcService {
    /// Create a new OTLP gRPC service.
    pub const fn new(write_channel: WriteChannel) -> Self {
        Self { write_channel }
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
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let export_request = request.into_inner();

        // Extract tenant_id from metadata (TODO: implement proper tenant extraction)
        let tenant_id = None; // Will use default

        // Transform OTLP logs to Arrow RecordBatch
        let Some(batch) = transform::logs_to_record_batch(&export_request, tenant_id) else {
            // No records to process - return success with 0 rejected
            return Ok(Response::new(ExportLogsServiceResponse { partial_success: None }));
        };

        let record_count = batch.num_rows();
        debug!(records = record_count, "Transformed OTLP logs to RecordBatch");

        // Create write request for the WAL queue
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let write_request = icegate_queue::WriteRequest {
            topic: LOGS_TOPIC.to_string(),
            batch,
            group_by_column: Some("tenant_id".to_string()),
            response_tx,
        };

        // Send to WAL queue
        self.write_channel
            .send(write_request)
            .await
            .map_err(|e| Status::unavailable(format!("WAL queue unavailable: {e}")))?;

        // Wait for write result
        let result = response_rx
            .await
            .map_err(|e| Status::internal(format!("Failed to receive write result: {e}")))?;

        match result {
            icegate_queue::WriteResult::Success { offset, records } => {
                debug!(offset, records, "Logs written to WAL");
                Ok(Response::new(ExportLogsServiceResponse { partial_success: None }))
            }
            icegate_queue::WriteResult::Failed { reason } => {
                // Return partial success with all records rejected
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
    async fn export(
        &self,
        _request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        Err(Status::unimplemented(
            "OTLP metrics ingestion: Parse OTLP → transform → write to Iceberg",
        ))
    }
}
