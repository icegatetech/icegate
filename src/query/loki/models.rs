//! Loki API response and request models.
//!
//! Typed models for Loki-compatible API responses, providing compile-time
//! safety and clear API contracts instead of dynamic JSON construction.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ============================================================================
// Request Models
// ============================================================================

/// Query parameters for range query.
#[derive(Debug, Deserialize, Serialize)]
pub struct RangeQueryParams {
    /// `LogQL` query string.
    pub query: String,
    /// Start time (nanoseconds or RFC3339).
    pub start: Option<String>,
    /// End time (nanoseconds or RFC3339).
    pub end: Option<String>,
    /// Step (duration string, e.g. "15s").
    pub step: Option<String>,
    /// Limit.
    pub limit: Option<u32>,
    /// Direction (forward/backward).
    pub direction: Option<String>,
}

/// Query parameters for `/loki/api/v1/labels`.
#[derive(Debug, Deserialize)]
pub struct LabelsQueryParams {
    /// Start time (nanoseconds or RFC3339).
    pub start: Option<String>,
    /// End time (nanoseconds or RFC3339).
    pub end: Option<String>,
    /// Duration string (e.g. "1h") - alternative to start/end.
    pub since: Option<String>,
    /// `LogQL` selector to filter streams (e.g. `{service_name="foo"}`).
    pub query: Option<String>,
}

/// Query parameters for `/loki/api/v1/label/:name/values`.
#[derive(Debug, Deserialize)]
pub struct LabelValuesQueryParams {
    /// Start time (nanoseconds or RFC3339).
    pub start: Option<String>,
    /// End time (nanoseconds or RFC3339).
    pub end: Option<String>,
    /// Duration string (e.g. "1h") - alternative to start/end.
    pub since: Option<String>,
    /// `LogQL` selector to filter streams (e.g. `{service_name="foo"}`).
    pub query: Option<String>,
}

/// Query parameters for `/loki/api/v1/series`.
#[derive(Debug, Deserialize)]
pub struct SeriesQueryParams {
    /// Required `LogQL` selectors (at least one required).
    #[serde(rename = "match[]", default)]
    pub matchers: Vec<String>,
    /// Start time (nanoseconds or RFC3339).
    pub start: Option<String>,
    /// End time (nanoseconds or RFC3339).
    pub end: Option<String>,
}

// ============================================================================
// Response Models
// ============================================================================

/// Generic Loki API response envelope.
#[derive(Debug, Serialize)]
#[serde(bound = "T: Serialize")]
pub struct LokiResponse<T> {
    /// Response status ("success" or "error").
    pub status: ResponseStatus,
    /// Response data (present on success).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    /// Error message (present on error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Error type classification (present on error).
    #[serde(rename = "errorType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<ErrorType>,
}

impl<T: Serialize> LokiResponse<T> {
    /// Create a success response with data.
    pub const fn success(data: T) -> Self {
        Self {
            status: ResponseStatus::Success,
            data: Some(data),
            error: None,
            error_type: None,
        }
    }
}

impl LokiResponse<()> {
    /// Create an error response.
    pub fn error(error_type: ErrorType, message: impl Into<String>) -> Self {
        Self {
            status: ResponseStatus::Error,
            data: None,
            error: Some(message.into()),
            error_type: Some(error_type),
        }
    }
}

/// Response status enum.
#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ResponseStatus {
    /// Successful response.
    Success,
    /// Error response.
    Error,
}

/// Error type classification (Loki-compatible).
#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum ErrorType {
    /// Invalid request data.
    BadData,
    /// Query planning failed.
    PlanningError,
    /// Query execution failed.
    ExecutionError,
    /// Feature not implemented.
    NotImplemented,
    /// Internal server error.
    Internal,
}

// ============================================================================
// Query Result Models
// ============================================================================

/// Query result data with type and statistics.
#[derive(Debug, Serialize)]
pub struct QueryResultData {
    /// Result type ("streams" or "matrix").
    #[serde(rename = "resultType")]
    pub result_type: ResultType,
    /// Query result (streams or matrix).
    pub result: QueryResult,
    /// Query execution statistics.
    pub stats: QueryStats,
}

/// Result type enum.
#[derive(Debug, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ResultType {
    /// Log streams result.
    Streams,
    /// Metric matrix result.
    Matrix,
}

/// Query result (either streams or matrix).
#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum QueryResult {
    /// Log streams result.
    Streams(Vec<Stream>),
    /// Metric matrix result.
    Matrix(Vec<MetricSeries>),
}

/// Log stream with labels and values.
#[derive(Debug, Serialize)]
pub struct Stream {
    /// Stream labels.
    pub stream: HashMap<String, String>,
    /// Log entries as [`timestamp_nanos`, line] pairs.
    pub values: Vec<(String, String)>,
}

/// Metric series with labels and values.
#[derive(Debug, Serialize)]
pub struct MetricSeries {
    /// Metric labels.
    pub metric: HashMap<String, String>,
    /// Metric samples as [`timestamp_seconds`, value] pairs.
    pub values: Vec<(i64, String)>,
}

// ============================================================================
// Statistics Models
// ============================================================================

/// Query execution statistics (Loki-compatible format).
#[derive(Debug, Serialize)]
pub struct QueryStats {
    /// Ingester statistics.
    pub ingester: IngesterStats,
    /// Store statistics.
    pub store: StoreStats,
    /// Summary statistics.
    pub summary: SummaryStats,
}

/// Ingester statistics.
#[derive(Debug, Serialize)]
pub struct IngesterStats {
    /// Compressed bytes processed.
    #[serde(rename = "compressedBytes")]
    pub compressed_bytes: u64,
    /// Decompressed bytes processed.
    #[serde(rename = "decompressedBytes")]
    pub decompressed_bytes: u64,
    /// Decompressed lines processed.
    #[serde(rename = "decompressedLines")]
    pub decompressed_lines: u64,
    /// Head chunk bytes.
    #[serde(rename = "headChunkBytes")]
    pub head_chunk_bytes: u64,
    /// Head chunk lines.
    #[serde(rename = "headChunkLines")]
    pub head_chunk_lines: u64,
    /// Total batches processed.
    #[serde(rename = "totalBatches")]
    pub total_batches: usize,
    /// Total chunks matched.
    #[serde(rename = "totalChunksMatched")]
    pub total_chunks_matched: u64,
    /// Total duplicates found.
    #[serde(rename = "totalDuplicates")]
    pub total_duplicates: u64,
    /// Total lines sent.
    #[serde(rename = "totalLinesSent")]
    pub total_lines_sent: u64,
    /// Total ingesters reached.
    #[serde(rename = "totalReached")]
    pub total_reached: u64,
}

impl IngesterStats {
    /// Create ingester stats from query metrics.
    pub const fn from_metrics(bytes: usize, lines: usize, batches: usize) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        Self {
            compressed_bytes: 0,
            decompressed_bytes: bytes as u64,
            decompressed_lines: lines as u64,
            head_chunk_bytes: 0,
            head_chunk_lines: 0,
            total_batches: batches,
            total_chunks_matched: 0,
            total_duplicates: 0,
            total_lines_sent: lines as u64,
            total_reached: 1,
        }
    }
}

/// Store statistics.
#[derive(Debug, Serialize)]
pub struct StoreStats {
    /// Compressed bytes processed.
    #[serde(rename = "compressedBytes")]
    pub compressed_bytes: u64,
    /// Decompressed bytes processed.
    #[serde(rename = "decompressedBytes")]
    pub decompressed_bytes: u64,
    /// Decompressed lines processed.
    #[serde(rename = "decompressedLines")]
    pub decompressed_lines: u64,
    /// Chunks download time.
    #[serde(rename = "chunksDownloadTime")]
    pub chunks_download_time: u64,
    /// Total chunks referenced.
    #[serde(rename = "totalChunksRef")]
    pub total_chunks_ref: u64,
    /// Total chunks downloaded.
    #[serde(rename = "totalChunksDownloaded")]
    pub total_chunks_downloaded: u64,
    /// Total duplicates found.
    #[serde(rename = "totalDuplicates")]
    pub total_duplicates: u64,
}

impl StoreStats {
    /// Create store stats from query metrics.
    pub const fn from_metrics(bytes: usize, lines: usize) -> Self {
        #[allow(clippy::cast_possible_truncation)]
        Self {
            compressed_bytes: 0,
            decompressed_bytes: bytes as u64,
            decompressed_lines: lines as u64,
            chunks_download_time: 0,
            total_chunks_ref: 0,
            total_chunks_downloaded: 0,
            total_duplicates: 0,
        }
    }
}

/// Summary statistics.
#[derive(Debug, Serialize)]
pub struct SummaryStats {
    /// Bytes processed per second.
    #[serde(rename = "bytesProcessedPerSecond")]
    pub bytes_per_second: u64,
    /// Lines processed per second.
    #[serde(rename = "linesProcessedPerSecond")]
    pub lines_per_second: u64,
    /// Total bytes processed.
    #[serde(rename = "totalBytesProcessed")]
    pub total_bytes: u64,
    /// Total lines processed.
    #[serde(rename = "totalLinesProcessed")]
    pub total_lines: u64,
    /// Execution time in seconds.
    #[serde(rename = "execTime")]
    pub exec_time: f64,
    /// Queue time (not used, always 0).
    #[serde(rename = "queueTime")]
    pub queue_time: u64,
}

impl SummaryStats {
    /// Create summary stats from query metrics.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub fn from_metrics(bytes: usize, lines: usize, exec_time: f64) -> Self {
        let bytes_per_sec = if exec_time > 0.0 { (bytes as f64 / exec_time) as u64 } else { 0 };
        let lines_per_sec = if exec_time > 0.0 { (lines as f64 / exec_time) as u64 } else { 0 };

        Self {
            bytes_per_second: bytes_per_sec,
            lines_per_second: lines_per_sec,
            total_bytes: bytes as u64,
            total_lines: lines as u64,
            exec_time,
            queue_time: 0,
        }
    }
}

impl QueryStats {
    /// Create query stats from execution metrics.
    pub fn from_metrics(bytes: usize, lines: usize, batches: usize, exec_time: f64) -> Self {
        Self {
            ingester: IngesterStats::from_metrics(bytes, lines, batches),
            store: StoreStats::from_metrics(bytes, lines),
            summary: SummaryStats::from_metrics(bytes, lines, exec_time),
        }
    }
}
