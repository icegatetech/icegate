//! OTLP HTTP request handlers

use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, header::CONTENT_TYPE},
    response::IntoResponse,
};
use icegate_common::{TENANT_ID_HEADER, is_valid_tenant_id};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

use super::{
    error::{OtlpError, OtlpResult},
    models::{
        ExportLogsResponse, ExportMetricsResponse, ExportTracesResponse, HealthResponse, HealthStatus,
        LogsPartialSuccess, MetricsPartialSuccess, TracesPartialSuccess,
    },
    server::OtlpHttpState,
};
use crate::{error::IngestError, infra::metrics::OtlpRequestRecorder, transform};

/// Content type for protobuf.
const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// Content type for JSON.
const CONTENT_TYPE_JSON: &str = "application/json";

const SIGNAL_LOGS: &str = "logs";
const SIGNAL_TRACES: &str = "traces";
const SIGNAL_OPERATIONS: &str = "operations";
const PROTOCOL_HTTP: &str = "http";
const STATUS_OK: &str = "ok";

/// Extract tenant ID from HTTP headers.
///
/// Returns `Some(tenant_id)` if the `x-scope-orgid` header is present and
/// contains a valid value (non-empty, ASCII alphanumeric/hyphens/underscores).
/// Returns `None` otherwise, which falls back to `DEFAULT_TENANT_ID` downstream.
fn extract_tenant_id(headers: &HeaderMap) -> Option<String> {
    headers
        .get(TENANT_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| is_valid_tenant_id(s))
        .map(String::from)
}

/// Handle OTLP logs ingestion.
///
/// Supports both Content-Types:
/// - `application/x-protobuf` (binary protobuf)
/// - `application/json` (JSON encoding)
///
/// Transforms OTLP log records to Arrow `RecordBatch` and writes to the WAL
/// queue.
#[tracing::instrument(skip(state, headers, body), fields(protocol = PROTOCOL_HTTP, signal = SIGNAL_LOGS))]
pub async fn ingest_logs(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> OtlpResult<Json<ExportLogsResponse>> {
    // Determine content type (default to protobuf per OTLP spec)
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(CONTENT_TYPE_PROTOBUF);
    let encoding = otlp_encoding_from_content_type(content_type);
    let request_metrics = OtlpRequestRecorder::new(&state.metrics, PROTOCOL_HTTP, SIGNAL_LOGS, encoding);
    request_metrics.record_request_size(body.len());

    // Parse the request based on content type
    let export_request = request_metrics
        // TODO(med): Add a check - if the request size is not large, then we do not go into a separate thread. With small volumes, the overhead on the stream will not cover the costs.
        .record_decode(content_type, || {
            parse_otlp_request::<ExportLogsServiceRequest>(content_type, &body)
        })
        .map_err(OtlpError::from)?;

    let tenant_id = extract_tenant_id(&headers);

    // Transform OTLP logs to Arrow RecordBatch (offload to blocking thread)
    let span = tracing::Span::current();
    let transform_start = Instant::now();
    let batch = tokio::task::spawn_blocking(move || {
        // TODO(med): Add a check - if the request size is not large, then we do not go into a separate thread. With small volumes, the overhead on the stream will not cover the costs.
        span.in_scope(|| transform::logs_to_record_batch(&export_request, tenant_id.as_deref()))
    })
    .await??;
    request_metrics.record_transform_duration(transform_start.elapsed(), SIGNAL_LOGS, STATUS_OK);
    let payload =
        crate::wal::write_logs_batch_to_wal(&state.write_channel, &request_metrics, batch, state.wal_row_group_size)
            .await
            .map_err(OtlpError)?;

    Ok(Json(ExportLogsResponse {
        partial_success: payload.map(|(rejected_log_records, message)| LogsPartialSuccess {
            rejected_log_records,
            error_message: Some(message),
        }),
    }))
}

/// Decode an OTLP export request body as protobuf or JSON, selected by the
/// `Content-Type`. Shared by the logs, traces, and metrics handlers; the only
/// per-signal difference is the request type `T`.
fn parse_otlp_request<T>(content_type: &str, body: &Bytes) -> Result<T, IngestError>
where
    T: prost::Message + Default + serde::de::DeserializeOwned,
{
    if content_type.starts_with(CONTENT_TYPE_PROTOBUF) {
        T::decode(body.as_ref()).map_err(|e| IngestError::Decode(format!("Failed to decode protobuf: {e}")))
    } else if content_type.starts_with(CONTENT_TYPE_JSON) {
        serde_json::from_slice(body.as_ref()).map_err(|e| IngestError::Decode(format!("Failed to decode JSON: {e}")))
    } else {
        Err(IngestError::Validation(format!(
            "Unsupported Content-Type: {content_type}. Expected application/x-protobuf or application/json"
        )))
    }
}

/// Handle OTLP traces ingestion.
///
/// Supports both Content-Types:
/// - `application/x-protobuf` (binary protobuf)
/// - `application/json` (JSON encoding)
///
/// Transforms OTLP spans to Arrow `RecordBatch` and writes to the WAL queue.
/// Transform-time drops (invalid `trace_id`/`span_id`) surface as
/// `partial_success.rejected_spans`.
#[tracing::instrument(skip(state, headers, body), fields(protocol = PROTOCOL_HTTP, signal = SIGNAL_TRACES))]
pub async fn ingest_traces(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> OtlpResult<Json<ExportTracesResponse>> {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

    // Determine content type (default to protobuf per OTLP spec)
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(CONTENT_TYPE_PROTOBUF);
    let encoding = otlp_encoding_from_content_type(content_type);
    let request_metrics = OtlpRequestRecorder::new(&state.metrics, PROTOCOL_HTTP, SIGNAL_TRACES, encoding);
    request_metrics.record_request_size(body.len());

    // Parse the request based on content type.
    let export_request: ExportTraceServiceRequest = request_metrics
        .record_decode(content_type, || {
            parse_otlp_request::<ExportTraceServiceRequest>(content_type, &body)
        })
        .map_err(OtlpError::from)?;

    let tenant_id = extract_tenant_id(&headers);
    // Shared across the two parallel transform tasks; the decoded request is owned
    // plain data (`Send + Sync`), so the `Arc` clone is cheap (no deep copy).
    let export_request = Arc::new(export_request);

    // Transform spans (authoritative) and derived operations (best-effort) on
    // separate blocking-pool threads so the two independent CPU passes over the
    // same request run in parallel instead of back-to-back.
    let span = tracing::Span::current();
    let transform_start = Instant::now();
    let spans_task = tokio::task::spawn_blocking({
        let export_request = Arc::clone(&export_request);
        let tenant_id = tenant_id.clone();
        let span = span.clone();
        move || span.in_scope(|| transform::spans_to_record_batch(&export_request, tenant_id.as_deref()))
    });
    let operations_task = tokio::task::spawn_blocking({
        let export_request = Arc::clone(&export_request);
        let span = span.clone();
        move || span.in_scope(|| transform::operations_to_record_batch(&export_request, tenant_id.as_deref()))
    });
    let (spans_join, operations_join) = tokio::join!(spans_task, operations_task);

    // Spans transform error (or task panic) fails the whole request (authoritative).
    let (batch_opt, drops) = spans_join?.map_err(OtlpError)?;
    request_metrics.record_transform_duration(transform_start.elapsed(), SIGNAL_TRACES, STATUS_OK);

    // Operations transform is best-effort: a panic degrades to a best-effort
    // failure (logged downstream) rather than failing the traces request.
    let operations_result = operations_join.unwrap_or_else(|join_err| Err(IngestError::Join(join_err)));

    // Spans (authoritative) and operations (best-effort) are written with
    // overlapping WAL acknowledgements; the operations leg can never fail or
    // alter the traces response.
    let operations_metrics = OtlpRequestRecorder::new(&state.metrics, PROTOCOL_HTTP, SIGNAL_OPERATIONS, encoding);
    let payload = crate::wal::write_traces_with_operations_to_wal(
        &state.write_channel,
        &request_metrics,
        &operations_metrics,
        batch_opt,
        drops,
        operations_result,
        state.wal_row_group_size,
    )
    .await
    .map_err(OtlpError)?;

    Ok(Json(ExportTracesResponse {
        partial_success: payload.map(|(rejected_spans, message)| TracesPartialSuccess {
            rejected_spans,
            error_message: Some(message),
        }),
    }))
}

const SIGNAL_METRICS: &str = "metrics";

/// Handle OTLP metrics ingestion.
///
/// Supports `application/x-protobuf` and `application/json`. Transforms OTLP
/// metric data points to an Arrow `RecordBatch` and writes to the WAL queue.
/// Strict-conformance transform drops surface as
/// `partial_success.rejected_data_points`.
#[tracing::instrument(skip(state, headers, body), fields(protocol = PROTOCOL_HTTP, signal = SIGNAL_METRICS))]
pub async fn ingest_metrics(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> OtlpResult<Json<ExportMetricsResponse>> {
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;

    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(CONTENT_TYPE_PROTOBUF);
    let encoding = otlp_encoding_from_content_type(content_type);
    let request_metrics = OtlpRequestRecorder::new(&state.metrics, PROTOCOL_HTTP, SIGNAL_METRICS, encoding);
    request_metrics.record_request_size(body.len());

    let export_request: ExportMetricsServiceRequest = request_metrics
        .record_decode(content_type, || {
            parse_otlp_request::<ExportMetricsServiceRequest>(content_type, &body)
        })
        .map_err(OtlpError::from)?;

    let tenant_id = extract_tenant_id(&headers);

    let span = tracing::Span::current();
    let transform_start = Instant::now();
    let (batch_opt, drops) = tokio::task::spawn_blocking(move || {
        span.in_scope(|| transform::metrics_to_record_batch(&export_request, tenant_id.as_deref()))
    })
    .await??;
    request_metrics.record_transform_duration(transform_start.elapsed(), SIGNAL_METRICS, STATUS_OK);

    let payload = crate::wal::write_metrics_batch_to_wal(
        &state.write_channel,
        &request_metrics,
        batch_opt,
        drops,
        state.wal_row_group_size,
    )
    .await
    .map_err(OtlpError)?;

    Ok(Json(ExportMetricsResponse {
        partial_success: payload.map(|(rejected_data_points, message)| MetricsPartialSuccess {
            rejected_data_points,
            error_message: Some(message),
        }),
    }))
}

/// Health check endpoint.
pub async fn health() -> impl IntoResponse {
    Json(HealthResponse {
        status: HealthStatus::Healthy,
    })
}

/// Resolve OTLP encoding from Content-Type.
fn otlp_encoding_from_content_type(content_type: &str) -> &'static str {
    if content_type.starts_with("application/x-protobuf") {
        "protobuf"
    } else if content_type.starts_with("application/json") {
        "json"
    } else {
        "unknown"
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Bytes;
    use icegate_queue::{WriteResult, channel};
    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    use prost::Message;

    use super::*;
    use crate::{infra::metrics::OtlpMetrics, otlp_http::server::OtlpHttpState};

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

    fn encode_protobuf_request() -> Bytes {
        Bytes::from(create_test_request().encode_to_vec())
    }

    fn test_state(write_channel: icegate_queue::WriteChannel) -> OtlpHttpState {
        OtlpHttpState {
            write_channel,
            wal_row_group_size: 4,
            metrics: OtlpMetrics::new_disabled(),
        }
    }

    #[test]
    fn test_parse_protobuf_request() {
        let request = create_test_request();
        let bytes = request.encode_to_vec();

        let parsed = parse_otlp_request::<ExportLogsServiceRequest>(CONTENT_TYPE_PROTOBUF, &Bytes::from(bytes));
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_parse_json_request() {
        let request = create_test_request();
        let json = serde_json::to_vec(&request).unwrap();

        let parsed = parse_otlp_request::<ExportLogsServiceRequest>(CONTENT_TYPE_JSON, &Bytes::from(json));
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_unsupported_content_type() {
        let result = parse_otlp_request::<ExportLogsServiceRequest>("text/plain", &Bytes::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_tenant_id_present() {
        let mut headers = HeaderMap::new();
        headers.insert(TENANT_ID_HEADER, "my-tenant".parse().unwrap());
        assert_eq!(extract_tenant_id(&headers), Some("my-tenant".to_string()));
    }

    #[test]
    fn test_extract_tenant_id_missing() {
        let headers = HeaderMap::new();
        assert_eq!(extract_tenant_id(&headers), None);
    }

    #[test]
    fn test_extract_tenant_id_empty() {
        let mut headers = HeaderMap::new();
        headers.insert(TENANT_ID_HEADER, "".parse().unwrap());
        assert_eq!(extract_tenant_id(&headers), None);
    }

    #[test]
    fn test_extract_tenant_id_invalid_chars() {
        let mut headers = HeaderMap::new();
        headers.insert(TENANT_ID_HEADER, "bad/tenant".parse().unwrap());
        assert_eq!(extract_tenant_id(&headers), None);
    }

    #[tokio::test]
    async fn ingest_logs_returns_success_on_full_wal_ack() {
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
                .send(WriteResult::success(11, total_rows, None))
                .expect("send wal ack");
        });

        let response = ingest_logs(State(test_state(tx)), HeaderMap::new(), encode_protobuf_request())
            .await
            .expect("http response");
        writer.await.expect("writer task");

        assert!(response.0.partial_success.is_none());
    }

    #[tokio::test]
    async fn ingest_logs_returns_partial_success_on_wal_partial_failure() {
        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            request
                .response_tx
                .send(WriteResult::failed("wal partial failure", None))
                .expect("send wal ack");
        });

        let response = ingest_logs(State(test_state(tx)), HeaderMap::new(), encode_protobuf_request())
            .await
            .expect("http response");
        writer.await.expect("writer task");

        let partial = response.0.partial_success.expect("partial success");
        assert_eq!(partial.rejected_log_records, 1);
        assert_eq!(partial.error_message.as_deref(), Some("wal partial failure"));
    }

    #[tokio::test]
    async fn ingest_logs_returns_error_before_send_on_invalid_request() {
        let (tx, mut rx) = channel(1);
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "text/plain".parse().expect("content type"));

        let result = ingest_logs(State(test_state(tx)), headers, Bytes::from_static(b"invalid")).await;
        assert!(result.is_err());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn ingest_logs_returns_error_when_write_channel_is_closed() {
        let (tx, rx) = channel(1);
        drop(rx);

        let result = ingest_logs(State(test_state(tx)), HeaderMap::new(), encode_protobuf_request()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn ingest_logs_returns_error_when_wal_prepare_fails_in_blocking_worker() {
        let (tx, mut rx) = channel(1);
        let state = OtlpHttpState {
            write_channel: tx,
            wal_row_group_size: 0,
            metrics: OtlpMetrics::new_disabled(),
        };

        let result = ingest_logs(State(state), HeaderMap::new(), encode_protobuf_request()).await;
        assert!(result.is_err());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn ingest_traces_returns_success_on_wal_ack() {
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

        let body = Bytes::from(request.encode_to_vec());
        let response = ingest_traces(State(test_state(tx)), HeaderMap::new(), body)
            .await
            .expect("http ok");
        writer.await.expect("writer");
        assert!(response.0.partial_success.is_none());
    }

    #[tokio::test]
    async fn ingest_traces_returns_partial_success_for_transform_drops() {
        // Two spans: one valid, one with zero trace_id (dropped).
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![
                        Span {
                            trace_id: vec![1u8; 16],
                            span_id: vec![2u8; 8],
                            parent_span_id: vec![],
                            trace_state: String::new(),
                            name: "ok".to_string(),
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
                        },
                        Span {
                            trace_id: vec![0u8; 16], // invalid
                            span_id: vec![2u8; 8],
                            parent_span_id: vec![],
                            trace_state: String::new(),
                            name: "bad".to_string(),
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
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let req = rx.recv().await.expect("write request");
            let total = req.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            req.response_tx.send(WriteResult::success(1, total, None)).expect("ack");
        });

        let body = Bytes::from(request.encode_to_vec());
        let response = ingest_traces(State(test_state(tx)), HeaderMap::new(), body)
            .await
            .expect("http ok");
        writer.await.expect("writer");
        let partial = response.0.partial_success.expect("partial");
        assert_eq!(partial.rejected_spans, 1);
    }

    fn metrics_gauge_request() -> opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
        use opentelemetry_proto::tonic::metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
        };
        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
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
        }
    }

    /// Gauge (valid) + Sum with unspecified temporality (dropped) in one request.
    fn metrics_mixed_request() -> opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
        use opentelemetry_proto::tonic::metrics::v1::{
            AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric,
            number_data_point,
        };
        let dp = |v: f64| NumberDataPoint {
            time_unix_nano: 1_700_000_000_000_000_000,
            value: Some(number_data_point::Value::AsDouble(v)),
            ..Default::default()
        };
        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            name: "ok".to_string(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![dp(1.0)],
                            })),
                            ..Default::default()
                        },
                        Metric {
                            name: "bad".to_string(),
                            data: Some(metric::Data::Sum(Sum {
                                data_points: vec![dp(2.0)],
                                aggregation_temporality: AggregationTemporality::Unspecified as i32,
                                is_monotonic: true,
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[tokio::test]
    async fn ingest_metrics_returns_success_on_full_wal_ack() {
        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            assert_eq!(request.topic, icegate_common::METRICS_TOPIC);
            let total = request.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            request.response_tx.send(WriteResult::success(1, total, None)).expect("ack");
        });

        let body = Bytes::from(metrics_gauge_request().encode_to_vec());
        let response = ingest_metrics(State(test_state(tx)), HeaderMap::new(), body)
            .await
            .expect("http response");
        writer.await.expect("writer task");
        assert!(response.0.partial_success.is_none());
    }

    #[tokio::test]
    async fn ingest_metrics_returns_partial_success_for_transform_drops() {
        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            let total = request.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            request.response_tx.send(WriteResult::success(1, total, None)).expect("ack");
        });

        let body = Bytes::from(metrics_mixed_request().encode_to_vec());
        let response = ingest_metrics(State(test_state(tx)), HeaderMap::new(), body)
            .await
            .expect("http response");
        writer.await.expect("writer task");
        let partial = response.0.partial_success.expect("partial");
        assert_eq!(partial.rejected_data_points, 1);
    }

    #[tokio::test]
    async fn ingest_metrics_returns_error_when_write_channel_is_closed() {
        let (tx, rx) = channel(1);
        drop(rx);
        let body = Bytes::from(metrics_gauge_request().encode_to_vec());
        let result = ingest_metrics(State(test_state(tx)), HeaderMap::new(), body).await;
        assert!(result.is_err());
    }

    fn traces_request_one_llm_one_plain() -> ExportTraceServiceRequest {
        let llm_span = Span {
            trace_id: vec![1u8; 16],
            span_id: vec![1u8; 8],
            parent_span_id: vec![],
            trace_state: String::new(),
            name: "chat".to_string(),
            kind: 0,
            start_time_unix_nano: 1_000_000,
            end_time_unix_nano: 2_000_000,
            attributes: vec![KeyValue {
                key: "gen_ai.operation.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("chat".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: None,
            flags: 0,
        };
        let plain_span = Span {
            trace_id: vec![1u8; 16],
            span_id: vec![2u8; 8],
            parent_span_id: vec![1u8; 8],
            trace_state: String::new(),
            name: "db.query".to_string(),
            kind: 0,
            start_time_unix_nano: 1_000_000,
            end_time_unix_nano: 2_000_000,
            attributes: vec![],
            dropped_attributes_count: 0,
            events: vec![],
            dropped_events_count: 0,
            links: vec![],
            dropped_links_count: 0,
            status: None,
            flags: 0,
        };
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![llm_span, plain_span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[tokio::test]
    async fn ingest_traces_succeeds_when_operations_write_fails() {
        // Spans (authoritative, first write) ack success; operations (second
        // write) hits a closed receiver. The HTTP response must still be Ok and
        // carry no partial_success (best-effort operations, D8).
        let (tx, mut rx) = channel(2);
        let writer = tokio::spawn(async move {
            let spans_req = rx.recv().await.expect("spans write request");
            assert_eq!(spans_req.topic, icegate_common::SPANS_TOPIC);
            let total = spans_req.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            spans_req
                .response_tx
                .send(WriteResult::success(1, total, None))
                .expect("spans ack");
            let ops_req = rx.recv().await.expect("operations write request");
            assert_eq!(ops_req.topic, icegate_common::OPERATIONS_TOPIC);
            drop(ops_req);
        });

        let state = OtlpHttpState {
            write_channel: tx,
            wal_row_group_size: 4,
            metrics: OtlpMetrics::new_disabled(),
        };
        let mut body = Vec::new();
        traces_request_one_llm_one_plain().encode(&mut body).expect("encode request");

        let mut headers = axum::http::HeaderMap::new();
        headers.insert(CONTENT_TYPE, CONTENT_TYPE_PROTOBUF.parse().expect("content type"));

        let response = ingest_traces(State(state), headers, axum::body::Bytes::from(body))
            .await
            .expect("traces response must be OK even when operations write fails");
        writer.await.expect("writer task");

        assert!(response.0.partial_success.is_none());
    }

    #[tokio::test]
    async fn ingest_metrics_accepts_json_body() {
        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            assert_eq!(request.topic, icegate_common::METRICS_TOPIC);
            let total = request.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            request.response_tx.send(WriteResult::success(1, total, None)).expect("ack");
        });

        // Exercise the JSON decode path (spec §9 requires protobuf AND JSON bodies).
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        );
        let body = Bytes::from(serde_json::to_vec(&metrics_gauge_request()).expect("encode json"));
        let response = ingest_metrics(State(test_state(tx)), headers, body)
            .await
            .expect("http response");
        writer.await.expect("writer task");
        assert!(response.0.partial_success.is_none());
    }
}
