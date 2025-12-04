//! Loki API request handlers
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, Duration, Utc};
use datafusion::arrow::array::{Array, MapArray, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use super::server::LokiState;
use crate::query::logql::antlr::AntlrParser;
use crate::query::logql::datafusion::DataFusionPlanner;
use crate::query::logql::planner::QueryContext;
use crate::query::logql::{Parser, Planner};

/// HTTP header for tenant identification (Grafana/Loki standard).
const TENANT_HEADER: &str = "x-scope-orgid";

/// Default tenant ID when header is not provided.
const DEFAULT_TENANT: &str = "anonymous";

/// Extract tenant ID from HTTP headers.
///
/// Uses the `X-Scope-OrgID` header (Grafana/Loki standard for multi-tenancy).
/// Returns "anonymous" if the header is not present or invalid.
fn extract_tenant_id(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map_or_else(|| DEFAULT_TENANT.to_string(), String::from)
}

/// Query parameters for explain endpoint
#[derive(Debug, Deserialize, Serialize)]
pub struct ExplainQueryParams {
    /// `LogQL` query string
    pub query: String,
    /// Optional start time (for context, not used in explain)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    /// Optional end time (for context, not used in explain)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    /// Optional limit (for context, not used in explain)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Optional step (for context, not used in explain)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

/// Handle explain query requests
///
/// This endpoint accepts `query_range` parameters and returns the full execution plan
/// including logical, optimized logical, and physical plans after `LogQL` transpilation.
///
/// # Returns
/// JSON response with:
/// - `query`: Original `LogQL` query
/// - `plans.logical`: Unoptimized logical plan
/// - `plans.optimized`: Optimized logical plan
/// - `plans.physical`: Physical execution plan
pub async fn explain_query(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Query(params): Query<ExplainQueryParams>,
) -> Response {
    let tenant_id = extract_tenant_id(&headers);

    // Create SessionContext from QueryEngine (uses cached IcebergCatalogProvider)
    let ctx = match loki_state.engine.create_session().await {
        Ok(ctx) => ctx,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "errorType": "session_error",
                    "error": format!("Failed to create session: {}", e),
                    "query": params.query
                })),
            )
                .into_response();
        },
    };

    // 1. Parse the LogQL query
    let parser = AntlrParser::new();
    let expr = match parser.parse(&params.query) {
        Ok(expr) => expr,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "status": "error",
                    "errorType": "bad_data",
                    "error": format!("Parse error: {}", e),
                    "query": params.query
                })),
            )
                .into_response();
        },
    };

    // 2. Create query context with default time range for explain
    let now = Utc::now();
    let context = QueryContext {
        tenant_id,
        start: params.start.as_ref().map_or(now - Duration::hours(1), |s| parse_time(s)),
        end: params.end.as_ref().map_or(now, |s| parse_time(s)),
        limit: params.limit.map(|l| l as usize),
        step: params.step.as_ref().and_then(|s| parse_duration(s)),
    };

    // 3. Plan the query
    let planner = DataFusionPlanner::new(ctx.clone(), context);
    let logical_plan = match planner.plan(expr).await {
        Ok(plan) => plan,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "status": "error",
                    "errorType": "planning_error",
                    "error": format!("Planning error: {}", e),
                    "query": params.query
                })),
            )
                .into_response();
        },
    };

    // 4. Get optimized logical plan
    let optimized_plan = match ctx.state().optimize(&logical_plan) {
        Ok(plan) => plan,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "errorType": "optimization_error",
                    "error": format!("Optimization error: {}", e),
                    "query": params.query
                })),
            )
                .into_response();
        },
    };

    // 5. Get physical plan
    let physical_plan = match ctx.state().create_physical_plan(&optimized_plan).await {
        Ok(plan) => plan,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "errorType": "physical_planning_error",
                    "error": format!("Physical planning error: {}", e),
                    "query": params.query
                })),
            )
                .into_response();
        },
    };

    // 6. Format plans as strings
    let logical_str = format!("{}", logical_plan.display_indent());
    let optimized_str = format!("{}", optimized_plan.display_indent());
    let physical_str = format!("{}", datafusion::physical_plan::displayable(physical_plan.as_ref()).indent(true));

    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "query": params.query,
            "plans": {
                "logical": logical_str,
                "optimized": optimized_str,
                "physical": physical_str
            }
        })),
    )
        .into_response()
}

/// Query parameters for range query
#[derive(Debug, Deserialize, Serialize)]
pub struct RangeQueryParams {
    /// `LogQL` query string
    pub query: String,
    /// Start time (nanoseconds or RFC3339)
    pub start: Option<String>,
    /// End time (nanoseconds or RFC3339)
    pub end: Option<String>,
    /// Step (duration string, e.g. "15s")
    pub step: Option<String>,
    /// Limit
    pub limit: Option<u32>,
    /// Direction (forward/backward)
    pub direction: Option<String>,
}

/// Handle instant query requests
pub async fn query(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Query(params): Query<ExplainQueryParams>,
) -> Response {
    // Instant query is treated as a range query with start=end=now if not provided
    let now = Utc::now();

    let time = params.start.as_ref().map_or(now, |s| parse_time(s));

    let range_params = RangeQueryParams {
        query: params.query,
        start: Some(time.timestamp_nanos_opt().unwrap_or(0).to_string()),
        end: Some(time.timestamp_nanos_opt().unwrap_or(0).to_string()),
        step: None,
        limit: params.limit,
        direction: None,
    };

    execute_query(loki_state, headers, range_params).await
}

/// Handle range query requests
pub async fn query_range(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Query(params): Query<RangeQueryParams>,
) -> Response {
    execute_query(loki_state, headers, params).await
}

async fn execute_query(loki_state: LokiState, headers: HeaderMap, params: RangeQueryParams) -> Response {
    let exec_start = Instant::now();
    let tenant_id = extract_tenant_id(&headers);

    let now = Utc::now();
    let start = params.start.as_ref().map_or(DateTime::UNIX_EPOCH, |s| parse_time(s));
    let end = params.end.as_ref().map_or(now, |s| parse_time(s));

    let context = QueryContext {
        tenant_id,
        start,
        end,
        limit: params.limit.map(|l| l as usize),
        step: params.step.as_ref().and_then(|s| parse_duration(s)),
    };

    // 1. Parse LogQL
    let parser = AntlrParser::new();
    let expr = match parser.parse(&params.query) {
        Ok(expr) => expr,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "status": "error",
                    "errorType": "bad_data",
                    "error": format!("Parse error: {}", e)
                })),
            )
                .into_response();
        },
    };

    // 2. Create SessionContext from QueryEngine (uses cached IcebergCatalogProvider)
    let ctx = match loki_state.engine.create_session().await {
        Ok(ctx) => ctx,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Failed to create session: {}", e)
                })),
            )
                .into_response();
        },
    };

    // 3. Plan Query
    let planner = DataFusionPlanner::new(ctx.clone(), context);
    let plan = match planner.plan(expr).await {
        Ok(plan) => plan,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "status": "error",
                    "error": format!("Planning error: {}", e)
                })),
            )
                .into_response();
        },
    };

    // 4. Execute Query
    let df = match ctx.execute_logical_plan(plan).await {
        Ok(df) => df,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Execution error: {}", e)
                })),
            )
                .into_response();
        },
    };

    let batches = match df.collect().await {
        Ok(batches) => batches,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "status": "error",
                    "error": format!("Collection error: {}", e)
                })),
            )
                .into_response();
        },
    };

    // 5. Format Response
    let (streams, stats) = batches_to_loki_streams(&batches);
    let exec_time = exec_start.elapsed().as_secs_f64();

    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let bytes_per_sec = if exec_time > 0.0 { (stats.total_bytes as f64 / exec_time) as u64 } else { 0 };
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let lines_per_sec = if exec_time > 0.0 { (stats.total_lines as f64 / exec_time) as u64 } else { 0 };

    (
        StatusCode::OK,
        Json(json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": streams,
                "stats": {
                    "ingester": {
                        "compressedBytes": 0,
                        "decompressedBytes": stats.total_bytes,
                        "decompressedLines": stats.total_lines,
                        "headChunkBytes": 0,
                        "headChunkLines": 0,
                        "totalBatches": batches.len(),
                        "totalChunksMatched": 0,
                        "totalDuplicates": 0,
                        "totalLinesSent": stats.total_lines,
                        "totalReached": 1
                    },
                    "store": {
                        "compressedBytes": 0,
                        "decompressedBytes": stats.total_bytes,
                        "decompressedLines": stats.total_lines,
                        "chunksDownloadTime": 0,
                        "totalChunksRef": 0,
                        "totalChunksDownloaded": 0,
                        "totalDuplicates": 0
                    },
                    "summary": {
                        "bytesProcessedPerSecond": bytes_per_sec,
                        "linesProcessedPerSecond": lines_per_sec,
                        "totalBytesProcessed": stats.total_bytes,
                        "totalLinesProcessed": stats.total_lines,
                        "execTime": exec_time,
                        "queueTime": 0
                    }
                }
            }
        })),
    )
        .into_response()
}

/// Statistics collected during result formatting
struct QueryStats {
    total_bytes: usize,
    total_lines: usize,
}

/// Simple string interner for deduplicating label strings.
///
/// Reduces allocations by reusing `Arc<str>` for repeated label names/values
/// (e.g., `service_name`, `level` appear on every row).
struct StringInterner {
    strings: HashMap<String, Arc<str>>,
}

impl StringInterner {
    /// Create a new interner with pre-allocated capacity for typical label sets.
    fn new() -> Self {
        Self {
            strings: HashMap::with_capacity(256),
        }
    }

    /// Intern a string, returning an `Arc<str>` that can be cheaply cloned.
    ///
    /// If the string was seen before, returns the existing `Arc`.
    /// Otherwise, creates a new `Arc` and stores it for future lookups.
    fn intern(&mut self, s: &str) -> Arc<str> {
        if let Some(interned) = self.strings.get(s) {
            Arc::clone(interned)
        } else {
            let arc: Arc<str> = Arc::from(s);
            self.strings.insert(s.to_string(), Arc::clone(&arc));
            arc
        }
    }
}

/// Column indices cached once per batch for efficient row processing
struct BatchColumns {
    timestamp: Option<usize>,
    body: Option<usize>,
    service_name: Option<usize>,
    severity_text: Option<usize>,
    attributes: Option<usize>,
}

impl BatchColumns {
    /// Extract column indices from schema
    fn from_schema(schema: &Schema) -> Self {
        Self {
            timestamp: schema.index_of("timestamp").ok(),
            body: schema.index_of("body").ok(),
            service_name: schema.index_of("service_name").ok(),
            severity_text: schema.index_of("severity_text").ok(),
            attributes: schema.index_of("attributes").ok(),
        }
    }
}

/// Extract timestamp from row (microseconds -> nanoseconds)
fn extract_timestamp(batch: &RecordBatch, cols: &BatchColumns, row: usize) -> i64 {
    cols.timestamp
        .and_then(|idx| {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .and_then(|arr| {
                    if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row) * 1000) // microseconds to nanoseconds
                    }
                })
        })
        .unwrap_or(0)
}

/// Extract body string from row
fn extract_body(batch: &RecordBatch, cols: &BatchColumns, row: usize) -> String {
    cols.body
        .and_then(|idx| {
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .and_then(|arr| if arr.is_null(row) { None } else { Some(arr.value(row).to_string()) })
        })
        .unwrap_or_default()
}

/// Extract labels from `service_name`, `severity_text`, and `attributes` columns.
///
/// Uses string interning to deduplicate repeated label names/values across rows.
fn extract_labels(
    batch: &RecordBatch,
    cols: &BatchColumns,
    row: usize,
    interner: &mut StringInterner,
) -> HashMap<Arc<str>, Arc<str>> {
    // Pre-allocate for typical label count (service_name + level + ~6 attributes)
    let mut labels: HashMap<Arc<str>, Arc<str>> = HashMap::with_capacity(8);

    // Add service_name as label
    if let Some(idx) = cols.service_name {
        if let Some(arr) = batch.column(idx).as_any().downcast_ref::<StringArray>() {
            if !arr.is_null(row) {
                labels.insert(interner.intern("service_name"), interner.intern(arr.value(row)));
            }
        }
    }

    // Add severity_text as "level" label
    if let Some(idx) = cols.severity_text {
        if let Some(arr) = batch.column(idx).as_any().downcast_ref::<StringArray>() {
            if !arr.is_null(row) {
                labels.insert(interner.intern("level"), interner.intern(arr.value(row)));
            }
        }
    }

    // Extract attributes from Map column
    if let Some(idx) = cols.attributes {
        if let Some(map_arr) = batch.column(idx).as_any().downcast_ref::<MapArray>() {
            if !map_arr.is_null(row) {
                let offsets = map_arr.offsets();
                #[allow(clippy::cast_sign_loss)]
                let start = offsets[row] as usize;
                #[allow(clippy::cast_sign_loss)]
                let end = offsets[row + 1] as usize;

                let keys = map_arr.keys().as_any().downcast_ref::<StringArray>();
                let values = map_arr.values().as_any().downcast_ref::<StringArray>();

                if let (Some(keys), Some(values)) = (keys, values) {
                    for i in start..end {
                        if !keys.is_null(i) && !values.is_null(i) {
                            labels.insert(interner.intern(keys.value(i)), interner.intern(values.value(i)));
                        }
                    }
                }
            }
        }
    }

    labels
}

/// Create a deterministic sorted key for stream grouping.
///
/// Writes the key into a pre-allocated buffer to avoid per-call allocations.
fn make_stream_key_into(labels: &HashMap<Arc<str>, Arc<str>>, buffer: &mut String) {
    buffer.clear();
    let mut label_pairs: Vec<_> = labels.iter().collect();
    label_pairs.sort_by(|(k1, _), (k2, _)| k1.as_ref().cmp(k2.as_ref()));
    for (i, (k, v)) in label_pairs.iter().enumerate() {
        if i > 0 {
            buffer.push(',');
        }
        buffer.push_str(k);
        buffer.push('=');
        buffer.push_str(v);
    }
}

/// Convert grouped streams to Loki JSON format.
///
/// Accepts `Arc<str>` labels and converts them to owned strings for JSON serialization.
fn streams_to_json(
    streams: HashMap<String, (HashMap<Arc<str>, Arc<str>>, Vec<(String, String)>)>,
) -> JsonValue {
    let result: Vec<JsonValue> = streams
        .into_values()
        .map(|(labels, values)| {
            // Convert Arc<str> labels to HashMap<String, String> for JSON
            let labels_map: HashMap<String, String> = labels
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            json!({
                "stream": labels_map,
                "values": values.into_iter().map(|(ts, line)| json!([ts, line])).collect::<Vec<_>>()
            })
        })
        .collect();

    json!(result)
}

/// Converts `DataFusion` `RecordBatches` to Loki streams format.
///
/// Loki streams format:
/// ```json
/// [
///   {
///     "stream": {"label1": "value1", "label2": "value2"},
///     "values": [["<timestamp_nanos>", "<log_line>"], ...]
///   }
/// ]
/// ```
///
/// Groups log entries by their labels (`service_name`, `severity_text`, and attributes).
///
/// # Performance
///
/// Uses string interning and buffer reuse to minimize allocations:
/// - `StringInterner` deduplicates repeated label names/values across rows
/// - Pre-allocated key buffer avoids per-row string allocations
/// - `itoa` for fast integer-to-string conversion
fn batches_to_loki_streams(batches: &[RecordBatch]) -> (JsonValue, QueryStats) {
    // Pre-allocate collections for typical query results
    let mut streams: HashMap<String, (HashMap<Arc<str>, Arc<str>>, Vec<(String, String)>)> =
        HashMap::with_capacity(64);
    let mut interner = StringInterner::new();
    let mut key_buffer = String::with_capacity(256);
    let mut total_bytes: usize = 0;
    let mut total_lines: usize = 0;

    for batch in batches {
        let cols = BatchColumns::from_schema(batch.schema().as_ref());

        for row in 0..batch.num_rows() {
            let timestamp_nanos = extract_timestamp(batch, &cols, row);
            let body = extract_body(batch, &cols, row);
            let labels = extract_labels(batch, &cols, row, &mut interner);

            // Reuse buffer for stream key construction
            make_stream_key_into(&labels, &mut key_buffer);

            total_bytes += body.len();
            total_lines += 1;

            // Use itoa for fast integer formatting
            let ts_str = itoa::Buffer::new().format(timestamp_nanos).to_string();

            streams
                .entry(key_buffer.clone())
                .or_insert_with(|| (labels, Vec::new()))
                .1
                .push((ts_str, body));
        }
    }

    (streams_to_json(streams), QueryStats { total_bytes, total_lines })
}

fn parse_time(s: &str) -> DateTime<Utc> {
    // Simple parsing, assumes nanoseconds if digits, otherwise try RFC3339
    s.parse::<i64>().map_or_else(
        |_| {
            // Try RFC3339 parsing
            DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or(DateTime::UNIX_EPOCH)
        },
        DateTime::from_timestamp_nanos,
    )
}

fn parse_duration(s: &str) -> Option<i64> {
    // TODO: Implement proper duration parsing (e.g. "15s", "1m")
    // For now, assume it's seconds if it ends with 's', or raw nanoseconds
    s.strip_suffix('s').map_or_else(
        || s.parse::<i64>().ok(),
        |stripped| stripped.parse::<i64>().ok().map(|v| v * 1_000_000_000),
    )
}

/// Handle label names request
///
/// # TODO
/// - Query Iceberg logs table for distinct label/attribute names
/// - Filter by time range if provided
/// - Return list of label names in Loki format
pub async fn labels(State(_state): State<LokiState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "status": "error",
            "error": "Labels endpoint not yet implemented. TODO: Query distinct labels from Iceberg"
        })),
    )
        .into_response()
}

/// Handle label values request
///
/// # TODO
/// - Query Iceberg logs table for distinct values of specified label
/// - Filter by time range if provided
/// - Return list of label values in Loki format
pub async fn label_values(State(_state): State<LokiState>, Path(_label_name): Path<String>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "status": "error",
            "error": "Label values endpoint not yet implemented. TODO: Query label values from Iceberg"
        })),
    )
        .into_response()
}

/// Health/ready check endpoint
pub async fn ready() -> Response {
    (StatusCode::OK, "ready").into_response()
}
