//! OTLP HTTP response and request models.
//!
//! Typed models for OTLP-compatible API responses, providing compile-time
//! safety and clear API contracts instead of dynamic JSON construction.

use serde::{Deserialize, Serialize};

// ============================================================================
// Export Response Models (per OpenTelemetry spec)
// ============================================================================

/// Response for logs export endpoint.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ExportLogsResponse {
    /// Details about partial success (if any records were rejected).
    #[serde(rename = "partialSuccess", skip_serializing_if = "Option::is_none")]
    pub partial_success: Option<LogsPartialSuccess>,
}

/// Partial success details for logs export.
#[derive(Debug, Serialize, Deserialize)]
pub struct LogsPartialSuccess {
    /// Number of log records that were rejected.
    #[serde(rename = "rejectedLogRecords")]
    pub rejected_log_records: i64,
    /// Human-readable error message.
    #[serde(rename = "errorMessage", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Response for traces export endpoint.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ExportTracesResponse {
    /// Details about partial success (if any spans were rejected).
    #[serde(rename = "partialSuccess", skip_serializing_if = "Option::is_none")]
    pub partial_success: Option<TracesPartialSuccess>,
}

/// Partial success details for traces export.
#[derive(Debug, Serialize, Deserialize)]
pub struct TracesPartialSuccess {
    /// Number of spans that were rejected.
    #[serde(rename = "rejectedSpans")]
    pub rejected_spans: i64,
    /// Human-readable error message.
    #[serde(rename = "errorMessage", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

/// Response for metrics export endpoint.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ExportMetricsResponse {
    /// Details about partial success (if any data points were rejected).
    #[serde(rename = "partialSuccess", skip_serializing_if = "Option::is_none")]
    pub partial_success: Option<MetricsPartialSuccess>,
}

/// Partial success details for metrics export.
#[derive(Debug, Serialize, Deserialize)]
pub struct MetricsPartialSuccess {
    /// Number of data points that were rejected.
    #[serde(rename = "rejectedDataPoints")]
    pub rejected_data_points: i64,
    /// Human-readable error message.
    #[serde(rename = "errorMessage", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

// ============================================================================
// Health Check Models
// ============================================================================

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    /// Health status.
    pub status: HealthStatus,
}

/// Health status enum.
#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    /// Service is healthy.
    Healthy,
    /// Service is unhealthy.
    Unhealthy,
}

// ============================================================================
// Error Response Models
// ============================================================================

/// Generic error response for OTLP endpoints.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Error message.
    pub error: String,
    /// Error type classification.
    #[serde(rename = "errorType")]
    pub error_type: ErrorType,
}

impl ErrorResponse {
    /// Create a new error response.
    pub fn new(error_type: ErrorType, message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            error_type,
        }
    }
}

/// Error type classification.
#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ErrorType {
    /// Invalid request data.
    BadData,
    /// Feature not implemented.
    NotImplemented,
    /// Internal server error.
    Internal,
}
