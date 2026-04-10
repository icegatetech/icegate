//! OTLP HTTP request handlers

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, header::CONTENT_TYPE},
    response::IntoResponse,
};
use icegate_common::{LOGS_TOPIC, TENANT_ID_HEADER, is_valid_tenant_id};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use rand::Rng;
use tracing::debug;

use super::{
    error::{OtlpError, OtlpResult},
    models::{
        ExportLogsResponse, ExportMetricsResponse, ExportTracesResponse, HealthResponse, HealthStatus,
        LogsPartialSuccess,
    },
    server::OtlpHttpState,
};
use crate::{
    error::IngestError, infra::metrics::OtlpRequestRecorder, shift::backpressure::decode_probability, transform,
};

/// Content type for protobuf.
const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// Content type for JSON.
const CONTENT_TYPE_JSON: &str = "application/json";

const SIGNAL_LOGS: &str = "logs";
const PROTOCOL_HTTP: &str = "http";
const WAL_REASON_CHANNEL_CLOSED: &str = "channel_closed";
const STATUS_OK: &str = "ok";
const STATUS_ERROR: &str = "error";

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

/// Check backpressure and return an error if the request should be rejected.
///
/// Reads the current rejection probability from the shared atomic.
/// When probability is zero (common case), only performs a single atomic load.
fn check_backpressure(rejection_probability: &AtomicU32, retry_after_secs: u64) -> Result<(), IngestError> {
    let encoded = rejection_probability.load(Ordering::Relaxed);
    if encoded == 0 {
        return Ok(());
    }
    let p = decode_probability(encoded);
    #[allow(clippy::cast_possible_truncation)]
    if rand::rng().random::<f32>() < p as f32 {
        return Err(IngestError::Backpressure(retry_after_secs));
    }
    Ok(())
}

/// Handle OTLP logs ingestion.
///
/// Supports both Content-Types:
/// - `application/x-protobuf` (binary protobuf)
/// - `application/json` (JSON encoding)
///
/// Transforms OTLP log records to Arrow `RecordBatch` and writes to the WAL
/// queue.
#[allow(clippy::cast_possible_wrap)]
#[tracing::instrument(skip(state, headers, body), fields(protocol = PROTOCOL_HTTP, signal = SIGNAL_LOGS))]
pub async fn ingest_logs(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> OtlpResult<Json<ExportLogsResponse>> {
    check_backpressure(&state.rejection_probability, 5).map_err(OtlpError::from)?;

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
        .record_decode(content_type, || parse_logs_request(content_type, &body))
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
    let Some(batch) = batch else {
        // No records to process - return success with 0 rejected
        request_metrics.record_records_per_request(0);
        request_metrics.finish_ok();
        return Ok(Json(ExportLogsResponse::default()));
    };

    let record_count = batch.num_rows();
    debug!(records = record_count, "Transformed OTLP logs to RecordBatch");
    request_metrics.record_records_per_request(record_count);

    let trace_context = icegate_common::extract_current_trace_context();
    let prepare_start = Instant::now();
    let prepared = crate::wal::sort_logs(&batch, state.wal_row_group_size, trace_context).map_err(|e| {
        request_metrics.finish_error();
        OtlpError(e)
    })?;
    request_metrics.record_wal_sorting_duration(prepare_start.elapsed(), SIGNAL_LOGS, STATUS_OK);
    let Some(prepared) = prepared else {
        request_metrics.finish_ok();
        return Ok(Json(ExportLogsResponse::default()));
    };

    let enqueue_start = Instant::now();
    let pending = crate::wal::submit_sorted_logs_to_wal(&state.write_channel, prepared)
        .await
        .map_err(|e| {
            request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
            request_metrics.add_wal_queue_unavailable(LOGS_TOPIC, WAL_REASON_CHANNEL_CLOSED);
            request_metrics.finish_error();
            OtlpError(e)
        })?;
    request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), LOGS_TOPIC, STATUS_OK);

    let ack_start = Instant::now();
    let ack_outcome = pending.wait_for_ack().await.map_err(|e| {
        request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
        request_metrics.finish_error();
        OtlpError(e)
    })?;

    match ack_outcome {
        crate::wal::WalAckOutcome::Success(write_result) => {
            if let Some(trace_context) = write_result.trace_context.as_deref() {
                icegate_common::add_span_link(trace_context);
            }
            debug!(
                offset = write_result.offset.unwrap_or_default(),
                records = write_result.records,
                "Logs written to WAL"
            );
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_OK);
            request_metrics.finish_ok();
            Ok(Json(ExportLogsResponse::default()))
        }
        crate::wal::WalAckOutcome::Partial(partial) => {
            if let Some(trace_context) = partial.trace_context.as_deref() {
                icegate_common::add_span_link(trace_context);
            }
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
            request_metrics.finish_partial();
            Ok(Json(ExportLogsResponse {
                partial_success: Some(LogsPartialSuccess {
                    rejected_log_records: i64::try_from(partial.rejected_records).map_err(|_| {
                        OtlpError(IngestError::Validation("Rejected logs count exceeds i64".to_string()))
                    })?,
                    error_message: Some(partial.reason),
                }),
            }))
        }
    }
}

/// Parse logs request from either protobuf or JSON encoding.
fn parse_logs_request(content_type: &str, body: &Bytes) -> Result<ExportLogsServiceRequest, IngestError> {
    if content_type.starts_with(CONTENT_TYPE_PROTOBUF) {
        // Decode protobuf
        ExportLogsServiceRequest::decode(body.as_ref())
            .map_err(|e| IngestError::Decode(format!("Failed to decode protobuf: {e}")))
    } else if content_type.starts_with(CONTENT_TYPE_JSON) {
        // Decode JSON using serde
        serde_json::from_slice(body.as_ref()).map_err(|e| IngestError::Decode(format!("Failed to decode JSON: {e}")))
    } else {
        Err(IngestError::Validation(format!(
            "Unsupported Content-Type: {content_type}. Expected application/x-protobuf or application/json"
        )))
    }
}

/// Handle OTLP traces ingestion.
///
/// # TODO
/// - Parse Content-Type header (application/x-protobuf or application/json)
/// - Deserialize OTLP `TracesData` from request body
/// - Transform OTLP spans to Iceberg schema format (from schema.rs)
/// - Write spans to Iceberg spans table via catalog
/// - Handle batching and backpressure
/// - Return OTLP `ExportTracesServiceResponse`
pub async fn ingest_traces(State(state): State<OtlpHttpState>) -> OtlpResult<Json<ExportTracesResponse>> {
    check_backpressure(&state.rejection_probability, 5).map_err(OtlpError::from)?;

    Err(OtlpError(IngestError::NotImplemented(
        "OTLP traces ingestion: Parse OTLP → transform → write to Iceberg".to_string(),
    )))
}

/// Handle OTLP metrics ingestion.
///
/// # TODO
/// - Parse Content-Type header (application/x-protobuf or application/json)
/// - Deserialize OTLP `MetricsData` from request body
/// - Transform OTLP metrics to Iceberg schema format (from schema.rs)
/// - Handle different metric types (gauge, sum, histogram, summary)
/// - Write metrics to Iceberg metrics table via catalog
/// - Handle batching and backpressure
/// - Return OTLP `ExportMetricsServiceResponse`
pub async fn ingest_metrics(State(state): State<OtlpHttpState>) -> OtlpResult<Json<ExportMetricsResponse>> {
    check_backpressure(&state.rejection_probability, 5).map_err(OtlpError::from)?;

    Err(OtlpError(IngestError::NotImplemented(
        "OTLP metrics ingestion: Parse OTLP → transform → write to Iceberg".to_string(),
    )))
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
    use std::sync::Arc;

    use axum::body::Bytes;
    use icegate_queue::{WriteResult, channel};
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
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
            rejection_probability: Arc::new(AtomicU32::new(0)),
        }
    }

    #[test]
    fn test_parse_protobuf_request() {
        let request = create_test_request();
        let bytes = request.encode_to_vec();

        let parsed = parse_logs_request(CONTENT_TYPE_PROTOBUF, &Bytes::from(bytes));
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_parse_json_request() {
        let request = create_test_request();
        let json = serde_json::to_vec(&request).unwrap();

        let parsed = parse_logs_request(CONTENT_TYPE_JSON, &Bytes::from(json));
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_unsupported_content_type() {
        let result = parse_logs_request("text/plain", &Bytes::new());
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
            rejection_probability: Arc::new(AtomicU32::new(0)),
        };

        let result = ingest_logs(State(state), HeaderMap::new(), encode_protobuf_request()).await;
        assert!(result.is_err());
        assert!(rx.try_recv().is_err());
    }
}
