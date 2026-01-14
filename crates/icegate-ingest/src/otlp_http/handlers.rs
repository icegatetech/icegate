//! OTLP HTTP request handlers

use axum::{
    Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, header::CONTENT_TYPE},
    response::IntoResponse,
};
use icegate_common::LOGS_TOPIC;
use icegate_queue::{WriteRequest, WriteResult};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use prost::Message;
use tracing::debug;

use super::{
    error::{OtlpError, OtlpResult},
    models::{
        ExportLogsResponse, ExportMetricsResponse, ExportTracesResponse, HealthResponse, HealthStatus,
        LogsPartialSuccess,
    },
    server::OtlpHttpState,
};
use crate::{error::IngestError, transform};

/// Content type for protobuf.
const CONTENT_TYPE_PROTOBUF: &str = "application/x-protobuf";

/// Content type for JSON.
const CONTENT_TYPE_JSON: &str = "application/json";

/// Handle OTLP logs ingestion.
///
/// Supports both Content-Types:
/// - `application/x-protobuf` (binary protobuf)
/// - `application/json` (JSON encoding)
///
/// Transforms OTLP log records to Arrow `RecordBatch` and writes to the WAL
/// queue.
#[allow(clippy::cast_possible_wrap)]
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

    // Parse the request based on content type
    let export_request = parse_logs_request(content_type, &body)?;

    // TODO: Extract tenant_id from authentication
    let tenant_id = None;

    // Transform OTLP logs to Arrow RecordBatch
    let Some(batch) = transform::logs_to_record_batch(&export_request, tenant_id)? else {
        // No records to process - return success with 0 rejected
        return Ok(Json(ExportLogsResponse::default()));
    };

    let record_count = batch.num_rows();
    debug!(records = record_count, "Transformed OTLP logs to RecordBatch");

    // Create write request for the WAL queue
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let write_request = WriteRequest {
        topic: LOGS_TOPIC.to_string(),
        batch,
        group_by_column: Some("tenant_id".to_string()),
        response_tx,
    };

    // Send to WAL queue
    state.write_channel.send(write_request).await.map_err(|e| {
        OtlpError(IngestError::Io(std::io::Error::other(format!(
            "WAL queue unavailable: {e}"
        ))))
    })?;

    // Wait for write result
    let result = response_rx.await.map_err(|e| {
        OtlpError(IngestError::Io(std::io::Error::other(format!(
            "Failed to receive write result: {e}"
        ))))
    })?;

    match result {
        WriteResult::Success { offset, records } => {
            debug!(offset, records, "Logs written to WAL");
            Ok(Json(ExportLogsResponse::default()))
        }
        WriteResult::Failed { reason } => {
            // Return partial success with all records rejected
            Ok(Json(ExportLogsResponse {
                partial_success: Some(LogsPartialSuccess {
                    rejected_log_records: record_count as i64,
                    error_message: Some(reason),
                }),
            }))
        }
    }
}

/// Parse logs request from either protobuf or JSON encoding.
fn parse_logs_request(content_type: &str, body: &Bytes) -> OtlpResult<ExportLogsServiceRequest> {
    if content_type.starts_with(CONTENT_TYPE_PROTOBUF) {
        // Decode protobuf
        ExportLogsServiceRequest::decode(body.as_ref())
            .map_err(|e| OtlpError(IngestError::Decode(format!("Failed to decode protobuf: {e}"))))
    } else if content_type.starts_with(CONTENT_TYPE_JSON) {
        // Decode JSON using serde
        serde_json::from_slice(body.as_ref())
            .map_err(|e| OtlpError(IngestError::Decode(format!("Failed to decode JSON: {e}"))))
    } else {
        Err(OtlpError(IngestError::Validation(format!(
            "Unsupported Content-Type: {content_type}. Expected application/x-protobuf or application/json"
        ))))
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
pub async fn ingest_traces(State(_state): State<OtlpHttpState>) -> OtlpResult<Json<ExportTracesResponse>> {
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
pub async fn ingest_metrics(State(_state): State<OtlpHttpState>) -> OtlpResult<Json<ExportMetricsResponse>> {
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

#[cfg(test)]
mod tests {
    use axum::body::Bytes;
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    use prost::Message;

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
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
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
}
