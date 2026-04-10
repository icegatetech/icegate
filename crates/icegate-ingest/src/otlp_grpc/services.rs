//! OTLP gRPC service implementations.
//!
//! Implements the `OpenTelemetry` Protocol service traits for logs, traces, and
//! metrics ingestion via gRPC.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use icegate_common::{LOGS_TOPIC, TENANT_ID_HEADER, is_valid_tenant_id};
use icegate_queue::WriteChannel;
use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
    },
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse, metrics_service_server::MetricsService},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService},
};
use prost::Message;
use rand::Rng;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::shift::backpressure::decode_probability;
use crate::transform;
use crate::{
    infra::metrics::{OtlpMetrics, OtlpRequestRecorder},
    otlp_grpc::error::GrpcError,
};

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
    /// Maximum number of rows per WAL row group.
    wal_row_group_size: usize,
    /// Metrics recorder for OTLP intake.
    metrics: OtlpMetrics,
    /// Shared rejection probability from the backpressure controller.
    rejection_probability: Arc<AtomicU32>,
}

impl OtlpGrpcService {
    /// Create a new OTLP gRPC service.
    #[allow(clippy::missing_const_for_fn)]
    pub fn new(
        write_channel: WriteChannel,
        wal_row_group_size: usize,
        metrics: OtlpMetrics,
        rejection_probability: Arc<AtomicU32>,
    ) -> Self {
        Self {
            write_channel,
            wal_row_group_size,
            metrics,
            rejection_probability,
        }
    }

    /// Check backpressure and return gRPC `RESOURCE_EXHAUSTED` if rejected.
    fn check_backpressure(&self) -> Result<(), Status> {
        let encoded = self.rejection_probability.load(Ordering::Relaxed);
        if encoded == 0 {
            return Ok(());
        }
        let p = decode_probability(encoded);
        #[allow(clippy::cast_possible_truncation)]
        if rand::rng().random::<f32>() < p as f32 {
            return Err(Status::resource_exhausted("backpressure: shift overloaded"));
        }
        Ok(())
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
        self.check_backpressure()?;
        debug!("Start handle OTLP GRPC request");
        let request_size = request.get_ref().encoded_len();
        let request_metrics = OtlpRequestRecorder::new(&self.metrics, PROTOCOL_GRPC, SIGNAL_LOGS, ENCODING_PROTOBUF);
        request_metrics.record_request_size(request_size);

        let tenant_id = extract_tenant_id(&request);

        // TODO(med): instrument gRPC decoding time by wrapping tonic/prost codec; handler receives decoded payload.
        let export_request = request.into_inner();

        // Transform OTLP logs to Arrow RecordBatch (offload to blocking thread)
        let span = tracing::Span::current();
        let transform_start = Instant::now();
        let batch = tokio::task::spawn_blocking(move || {
            // TODO(med): Add a check - if the request size is not large, then we do not go into a separate thread. With small volumes, the overhead on the stream will not cover the costs.
            span.in_scope(|| transform::logs_to_record_batch(&export_request, tenant_id.as_deref()))
        })
        .await
        .map_err(|e| Status::internal(format!("Transform task panicked: {e}")))?
        .map_err(|e| Status::from(GrpcError(e)))?;
        request_metrics.record_transform_duration(transform_start.elapsed(), SIGNAL_LOGS, STATUS_OK);
        let Some(batch) = batch else {
            // No records to process - return success with 0 rejected
            request_metrics.record_records_per_request(0);
            request_metrics.finish_ok();
            return Ok(Response::new(ExportLogsServiceResponse { partial_success: None }));
        };

        let record_count = batch.num_rows();
        debug!(records = record_count, "Transformed OTLP logs to RecordBatch");
        request_metrics.record_records_per_request(record_count);

        let wal_row_group_size = self.wal_row_group_size;
        let trace_context = icegate_common::extract_current_trace_context();
        let prepare_start = Instant::now();
        let prepared = crate::wal::sort_logs(&batch, wal_row_group_size, trace_context).map_err(|err| {
            request_metrics.finish_error();
            Status::from(GrpcError(err))
        })?;
        request_metrics.record_wal_sorting_duration(prepare_start.elapsed(), SIGNAL_LOGS, STATUS_OK);
        let Some(prepared) = prepared else {
            request_metrics.finish_ok();
            return Ok(Response::new(ExportLogsServiceResponse { partial_success: None }));
        };

        let enqueue_start = Instant::now();
        let pending = crate::wal::submit_sorted_logs_to_wal(&self.write_channel, prepared)
            .await
            .map_err(|err| {
                request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
                request_metrics.add_wal_queue_unavailable(LOGS_TOPIC, WAL_REASON_CHANNEL_CLOSED);
                request_metrics.finish_error();
                Status::from(GrpcError(err))
            })?;
        request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), LOGS_TOPIC, STATUS_OK);

        let ack_start = Instant::now();
        let ack_outcome = pending.wait_for_ack().await.map_err(|err| {
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
            request_metrics.finish_error();
            Status::from(GrpcError(err))
        })?;

        match ack_outcome {
            crate::wal::WalAckOutcome::Success(write_result) => {
                if let Some(trace_context) = write_result.trace_context.as_deref() {
                    icegate_common::add_span_link(trace_context);
                }
                debug!(
                    offset = write_result.offset.unwrap_or_default(),
                    records = write_result.records,
                    "OTLP GRPC request ended successfully"
                );
                request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_OK);
                request_metrics.finish_ok();
                Ok(Response::new(ExportLogsServiceResponse { partial_success: None }))
            }
            crate::wal::WalAckOutcome::Partial(partial) => {
                if let Some(trace_context) = partial.trace_context.as_deref() {
                    icegate_common::add_span_link(trace_context);
                }
                request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
                request_metrics.finish_partial();
                Ok(Response::new(ExportLogsServiceResponse {
                    partial_success: Some(ExportLogsPartialSuccess {
                        rejected_log_records: i64::try_from(partial.rejected_records)
                            .map_err(|_| Status::internal("Rejected logs count exceeds i64"))?,
                        error_message: partial.reason,
                    }),
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
        self.check_backpressure()?;
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
        self.check_backpressure()?;
        Err(Status::unimplemented(
            "OTLP metrics ingestion: Parse OTLP → transform → write to Iceberg",
        ))
    }
}

#[cfg(test)]
mod tests {
    use icegate_queue::{WriteResult, channel};
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };

    use super::*;

    fn create_test_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 1_700_000_000_000_000_000,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("Test message".to_string())),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0; 16],
                        span_id: vec![0; 8],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn test_service(write_channel: icegate_queue::WriteChannel) -> OtlpGrpcService {
        OtlpGrpcService::new(
            write_channel,
            4,
            OtlpMetrics::new_disabled(),
            Arc::new(AtomicU32::new(0)),
        )
    }

    #[tokio::test]
    async fn export_logs_returns_success_on_full_wal_ack() {
        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            let total_rows = request
                .row_groups
                .iter()
                .map(|row_group| row_group.batch.num_rows())
                .sum::<usize>();
            request
                .response_tx
                .send(WriteResult::success(17, total_rows, None))
                .expect("send wal ack");
        });

        let service = test_service(tx);
        let response = LogsService::export(&service, Request::new(create_test_request()))
            .await
            .expect("grpc response")
            .into_inner();
        writer.await.expect("writer task");

        assert!(response.partial_success.is_none());
    }

    #[tokio::test]
    async fn export_logs_returns_partial_success_on_wal_partial_failure() {
        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            request
                .response_tx
                .send(WriteResult::failed("wal partial failure", None))
                .expect("send wal ack");
        });

        let service = test_service(tx);
        let response = LogsService::export(&service, Request::new(create_test_request()))
            .await
            .expect("grpc response")
            .into_inner();
        writer.await.expect("writer task");

        let partial = response.partial_success.expect("partial success");
        assert_eq!(partial.rejected_log_records, 1);
        assert_eq!(partial.error_message, "wal partial failure");
    }

    #[tokio::test]
    async fn export_logs_returns_internal_when_write_channel_is_closed() {
        let (tx, rx) = channel(1);
        drop(rx);

        let service = test_service(tx);
        let status = LogsService::export(&service, Request::new(create_test_request()))
            .await
            .expect_err("grpc status");
        assert_eq!(status.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn export_logs_returns_internal_when_wal_prepare_fails_in_blocking_worker() {
        let (tx, mut rx) = channel(1);
        let service = OtlpGrpcService::new(tx, 0, OtlpMetrics::new_disabled(), Arc::new(AtomicU32::new(0)));

        let status = LogsService::export(&service, Request::new(create_test_request()))
            .await
            .expect_err("grpc status");

        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(rx.try_recv().is_err());
    }
}
