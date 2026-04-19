//! Tempo HTTP request and response models.
//!
//! Typed models for Tempo-compatible API responses, providing compile-time
//! safety and clear API contracts instead of dynamic JSON construction.

use serde::{Deserialize, Serialize};

// ============================================================================
// Request Models
// ============================================================================

/// Query parameters for `GET /api/traces/{traceID}` and v2 variant.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TraceLookupParams {
    /// Optional Unix epoch seconds — narrows the time range scanned.
    pub start: Option<i64>,
    /// Optional Unix epoch seconds.
    pub end: Option<i64>,
}

/// Query parameters for `GET /api/search`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SearchParams {
    /// `TraceQL` query string. Optional — when absent, returns recent traces.
    pub q: Option<String>,
    /// Start time (Unix epoch nanoseconds OR seconds — accept both).
    pub start: Option<String>,
    /// End time (Unix epoch nanoseconds OR seconds).
    pub end: Option<String>,
    /// Maximum traces to return.
    pub limit: Option<usize>,
    /// Minimum trace duration filter (e.g., "100ms").
    #[serde(rename = "minDuration")]
    pub min_duration: Option<String>,
    /// Maximum trace duration filter.
    #[serde(rename = "maxDuration")]
    pub max_duration: Option<String>,
}

// ============================================================================
// Response Models
// ============================================================================

/// Search response (`{ traces: [...], metrics: { totalBlocks: N } }`).
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    /// Trace summaries.
    pub traces: Vec<TraceSummary>,
    /// Search-time metrics surfaced to clients.
    pub metrics: SearchMetrics,
}

/// One trace summary in a search response.
#[derive(Debug, Serialize)]
pub struct TraceSummary {
    /// Trace identifier (hex-encoded).
    #[serde(rename = "traceID")]
    pub trace_id: String,
    /// Service of the root span, if known.
    #[serde(rename = "rootServiceName", skip_serializing_if = "Option::is_none")]
    pub root_service_name: Option<String>,
    /// Name of the root span, if known.
    #[serde(rename = "rootTraceName", skip_serializing_if = "Option::is_none")]
    pub root_trace_name: Option<String>,
    /// Trace start time, Unix epoch nanoseconds, as a string.
    #[serde(rename = "startTimeUnixNano")]
    pub start_time_unix_nano: String,
    /// Total trace duration in milliseconds.
    #[serde(rename = "durationMs")]
    pub duration_ms: u64,
}

/// Metrics summary returned with search results.
#[derive(Debug, Serialize, Default)]
pub struct SearchMetrics {
    /// Total Iceberg row groups (approximation of "blocks" in Tempo terminology).
    #[serde(rename = "totalBlocks")]
    pub total_blocks: u64,
}
