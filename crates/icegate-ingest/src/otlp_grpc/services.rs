//! OTLP gRPC service implementations.
//!
//! Implements the `OpenTelemetry` Protocol service traits for logs, traces, and
//! metrics ingestion via gRPC.

use std::time::Instant;

use icegate_common::{TENANT_ID_HEADER, is_valid_tenant_id};
use icegate_queue::WriteChannel;
use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
    },
    metrics::v1::{
        ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::MetricsService,
    },
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse, trace_service_server::TraceService},
};
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::debug;

use crate::transform;
use crate::{
    infra::metrics::{OtlpMetrics, OtlpRequestRecorder},
    otlp_grpc::error::GrpcError,
};

const SIGNAL_LOGS: &str = "logs";
const SIGNAL_TRACES: &str = "traces";
const SIGNAL_METRICS: &str = "metrics";
const PROTOCOL_GRPC: &str = "grpc";
const ENCODING_PROTOBUF: &str = "protobuf";
const STATUS_OK: &str = "ok";

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
}

impl OtlpGrpcService {
    /// Create a new OTLP gRPC service.
    pub const fn new(write_channel: WriteChannel, wal_row_group_size: usize, metrics: OtlpMetrics) -> Self {
        Self {
            write_channel,
            wal_row_group_size,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcService {
    /// Handle OTLP logs export request.
    ///
    /// Transforms OTLP log records to Arrow `RecordBatch` and writes to the WAL
    /// queue. Returns partial success response with count of rejected
    /// records.
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
        let transform_start = Instant::now();
        let batch = tokio::task::spawn_blocking(move || {
            // TODO(med): Add a check - if the request size is not large, then we do not go into a separate thread. With small volumes, the overhead on the stream will not cover the costs.
            span.in_scope(|| transform::logs_to_record_batch(&export_request, tenant_id.as_deref()))
        })
        .await
        .map_err(|e| Status::internal(format!("Transform task panicked: {e}")))?
        .map_err(|e| Status::from(GrpcError(e)))?;
        request_metrics.record_transform_duration(transform_start.elapsed(), SIGNAL_LOGS, STATUS_OK);
        let payload =
            crate::wal::write_logs_batch_to_wal(&self.write_channel, &request_metrics, batch, self.wal_row_group_size)
                .await
                .map_err(|err| Status::from(GrpcError(err)))?;

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: payload.map(|(rejected_log_records, error_message)| ExportLogsPartialSuccess {
                rejected_log_records,
                error_message,
            }),
        }))
    }
}

#[tonic::async_trait]
impl TraceService for OtlpGrpcService {
    /// Handle OTLP traces export request.
    ///
    /// Transforms OTLP spans to Arrow `RecordBatch` and writes to the WAL
    /// queue. Transform-time drops (invalid `trace_id`/`span_id`) surface as
    /// `ExportTracePartialSuccess.rejected_spans`.
    #[tracing::instrument(name = "export_traces", skip(self, request))]
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTracePartialSuccess;

        debug!("Start handle OTLP GRPC traces request");
        let request_size = request.get_ref().encoded_len();
        let request_metrics = OtlpRequestRecorder::new(&self.metrics, PROTOCOL_GRPC, SIGNAL_TRACES, ENCODING_PROTOBUF);
        request_metrics.record_request_size(request_size);

        let tenant_id = extract_tenant_id(&request);
        let export_request = request.into_inner();

        // Transform OTLP spans to Arrow RecordBatch (offload to blocking thread).
        let span = tracing::Span::current();
        let transform_start = Instant::now();
        let (batch_opt, drops) = tokio::task::spawn_blocking(move || {
            span.in_scope(|| transform::spans_to_record_batch(&export_request, tenant_id.as_deref()))
        })
        .await
        .map_err(|e| Status::internal(format!("Transform task panicked: {e}")))?
        .map_err(|e| Status::from(GrpcError(e)))?;
        request_metrics.record_transform_duration(transform_start.elapsed(), SIGNAL_TRACES, STATUS_OK);

        let payload = crate::wal::write_traces_batch_to_wal(
            &self.write_channel,
            &request_metrics,
            batch_opt,
            drops,
            self.wal_row_group_size,
        )
        .await
        .map_err(|err| Status::from(GrpcError(err)))?;

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: payload.map(|(rejected_spans, error_message)| ExportTracePartialSuccess {
                rejected_spans,
                error_message,
            }),
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpGrpcService {
    /// Handle OTLP metrics export request.
    ///
    /// Transforms OTLP metric data points to an Arrow `RecordBatch` and writes
    /// to the WAL queue. Strict-conformance transform drops surface as
    /// `ExportMetricsPartialSuccess.rejected_data_points`.
    #[tracing::instrument(name = "export_metrics", skip(self, request))]
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        debug!("Start handle OTLP GRPC metrics request");
        let request_size = request.get_ref().encoded_len();
        let request_metrics = OtlpRequestRecorder::new(&self.metrics, PROTOCOL_GRPC, SIGNAL_METRICS, ENCODING_PROTOBUF);
        request_metrics.record_request_size(request_size);

        let tenant_id = extract_tenant_id(&request);
        let export_request = request.into_inner();

        let span = tracing::Span::current();
        let transform_start = Instant::now();
        let (batch_opt, drops) = tokio::task::spawn_blocking(move || {
            span.in_scope(|| transform::metrics_to_record_batch(&export_request, tenant_id.as_deref()))
        })
        .await
        .map_err(|e| Status::internal(format!("Transform task panicked: {e}")))?
        .map_err(|e| Status::from(GrpcError(e)))?;
        request_metrics.record_transform_duration(transform_start.elapsed(), SIGNAL_METRICS, STATUS_OK);

        let payload = crate::wal::write_metrics_batch_to_wal(
            &self.write_channel,
            &request_metrics,
            batch_opt,
            drops,
            self.wal_row_group_size,
        )
        .await
        .map_err(|err| Status::from(GrpcError(err)))?;

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: payload.map(|(rejected_data_points, error_message)| ExportMetricsPartialSuccess {
                rejected_data_points,
                error_message,
            }),
        }))
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
        OtlpGrpcService::new(write_channel, 4, OtlpMetrics::new_disabled())
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
        let service = OtlpGrpcService::new(tx, 0, OtlpMetrics::new_disabled());

        let status = LogsService::export(&service, Request::new(create_test_request()))
            .await
            .expect_err("grpc status");

        assert_eq!(status.code(), tonic::Code::Internal);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn export_traces_returns_success_on_wal_ack() {
        use opentelemetry_proto::tonic::{
            collector::trace::v1::{ExportTraceServiceRequest, trace_service_server::TraceService},
            trace::v1::{ResourceSpans, ScopeSpans, Span},
        };

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        parent_span_id: vec![],
                        trace_state: String::new(),
                        name: "op".to_string(),
                        kind: 0,
                        start_time_unix_nano: 1,
                        end_time_unix_nano: 2,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: None,
                        flags: 0,
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let req = rx.recv().await.expect("write request");
            assert_eq!(req.topic, icegate_common::SPANS_TOPIC);
            let total = req.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            req.response_tx.send(WriteResult::success(1, total, None)).expect("ack");
        });

        let service = test_service(tx);
        let response = TraceService::export(&service, Request::new(request))
            .await
            .expect("grpc ok")
            .into_inner();
        writer.await.expect("writer");
        assert!(response.partial_success.is_none());
    }

    #[tokio::test]
    async fn export_metrics_returns_success_on_wal_ack() {
        use opentelemetry_proto::tonic::collector::metrics::v1::{
            ExportMetricsServiceRequest, metrics_service_server::MetricsService,
        };
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "cpu".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_700_000_000_000_000_000,
                                value: Some(number_data_point::Value::AsDouble(1.0)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let req = rx.recv().await.expect("write request");
            assert_eq!(req.topic, icegate_common::METRICS_TOPIC);
            let total = req.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            req.response_tx.send(WriteResult::success(1, total, None)).expect("ack");
        });

        let service = test_service(tx);
        let response = MetricsService::export(&service, Request::new(request))
            .await
            .expect("grpc ok")
            .into_inner();
        writer.await.expect("writer");
        assert!(response.partial_success.is_none());
    }
}
