//! OTLP HTTP request handlers

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use super::server::OtlpHttpState;

/// Handle OTLP logs ingestion
///
/// # TODO
/// - Parse Content-Type header (application/x-protobuf or application/json)
/// - Deserialize OTLP `LogsData` from request body
/// - Transform OTLP log records to Iceberg schema format (from schema.rs)
/// - Write records to Iceberg logs table via catalog
/// - Handle batching and backpressure
/// - Return OTLP `ExportLogsServiceResponse`
pub async fn ingest_logs(State(_state): State<OtlpHttpState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "OTLP logs ingestion not yet implemented. TODO: Parse OTLP → transform → write to Iceberg"
        })),
    )
        .into_response()
}

/// Handle OTLP traces ingestion
///
/// # TODO
/// - Parse Content-Type header (application/x-protobuf or application/json)
/// - Deserialize OTLP `TracesData` from request body
/// - Transform OTLP spans to Iceberg schema format (from schema.rs)
/// - Write spans to Iceberg spans table via catalog
/// - Handle batching and backpressure
/// - Return OTLP `ExportTracesServiceResponse`
pub async fn ingest_traces(State(_state): State<OtlpHttpState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "OTLP traces ingestion not yet implemented. TODO: Parse OTLP → transform → write to Iceberg"
        })),
    )
        .into_response()
}

/// Handle OTLP metrics ingestion
///
/// # TODO
/// - Parse Content-Type header (application/x-protobuf or application/json)
/// - Deserialize OTLP `MetricsData` from request body
/// - Transform OTLP metrics to Iceberg schema format (from schema.rs)
/// - Handle different metric types (gauge, sum, histogram, summary)
/// - Write metrics to Iceberg metrics table via catalog
/// - Handle batching and backpressure
/// - Return OTLP `ExportMetricsServiceResponse`
pub async fn ingest_metrics(State(_state): State<OtlpHttpState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "OTLP metrics ingestion not yet implemented. TODO: Parse OTLP → transform → write to Iceberg"
        })),
    )
        .into_response()
}

/// Health check endpoint
pub async fn health() -> Response {
    (StatusCode::OK, "healthy").into_response()
}
