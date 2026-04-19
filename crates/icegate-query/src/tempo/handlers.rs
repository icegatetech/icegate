//! Tempo API request handlers.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use chrono::{DateTime, TimeZone, Utc};
use icegate_common::{DEFAULT_TENANT_ID, TENANT_ID_HEADER, is_valid_tenant_id};
use serde_json::json;

use super::{
    error::TempoResult,
    executor,
    formatters::{spans_to_otlp_json, spans_to_otlp_proto},
    models::{SearchParams, TraceLookupParams},
    server::TempoState,
    trace_by_id::{default_window, fetch},
};

// ============================================================================
// Helpers
// ============================================================================

/// Extract tenant ID from HTTP headers.
///
/// Returns the header value if present and valid (ASCII alphanumeric, hyphens,
/// underscores). Falls back to `DEFAULT_TENANT_ID` otherwise — matching the
/// ingest-path behaviour so that data is always queryable under the same
/// tenant that was used during ingestion.
fn extract_tenant_id(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| is_valid_tenant_id(s))
        .map_or_else(|| DEFAULT_TENANT_ID.to_string(), String::from)
}

/// Convert a Unix epoch second value into a UTC `DateTime`.
///
/// Falls back to [`DateTime::UNIX_EPOCH`] for ambiguous/out-of-range inputs.
fn parse_epoch_seconds(secs: i64) -> DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).single().unwrap_or(DateTime::UNIX_EPOCH)
}

/// Decide whether the caller wants OTLP protobuf or JSON.
///
/// Grafana's Tempo data source sends `Accept: application/protobuf` (or
/// `application/x-protobuf`). CLI clients typically use `application/json`
/// or `*/*` / no header. Protobuf wins when the Accept header explicitly
/// names it; JSON wins when `application/json` is named; otherwise we
/// default to protobuf to match Tempo's behaviour (Grafana is the primary
/// consumer and any generic `*/*` client is served what Tempo would have
/// served).
fn wants_protobuf(headers: &HeaderMap) -> bool {
    let Some(accept) = headers.get(header::ACCEPT).and_then(|v| v.to_str().ok()) else {
        return true;
    };
    let lower = accept.to_ascii_lowercase();
    if lower.contains("application/protobuf") || lower.contains("application/x-protobuf") {
        return true;
    }
    if lower.contains("application/json") {
        return false;
    }
    // `*/*` or unknown → protobuf (Tempo default).
    true
}

// ============================================================================
// Trace-by-ID
// ============================================================================

/// Handle `GET /api/traces/{trace_id}` (and the `/api/v2/...` alias).
///
/// Returns the matching spans encoded as OTLP `resourceSpans` JSON, or 404
/// when no spans match the trace ID under the requesting tenant.
///
/// # Errors
///
/// Returns a [`super::error::TempoError`] (rendered as JSON) if the engine
/// session cannot be created, the planner fails, or execution errors out.
#[tracing::instrument(skip_all, fields(tenant_id, trace_id = %trace_id))]
pub async fn get_trace(
    State(state): State<TempoState>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    Query(params): Query<TraceLookupParams>,
) -> TempoResult<Response> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", tenant_id.as_str());

    let now = Utc::now();
    let (default_start, default_end) = default_window(now);
    let start = params.start.map_or(default_start, parse_epoch_seconds);
    let end = params.end.map_or(default_end, parse_epoch_seconds);

    let batches = fetch(state.engine, &tenant_id, &trace_id, start, end).await?;
    if batches.iter().all(|b| b.num_rows() == 0) {
        return Ok((StatusCode::NOT_FOUND, Json(json!({"error": "trace not found"}))).into_response());
    }

    if wants_protobuf(&headers) {
        let bytes = spans_to_otlp_proto(&batches)?;
        let content_type = axum::http::HeaderValue::from_static("application/protobuf");
        Ok(([(header::CONTENT_TYPE, content_type)], bytes).into_response())
    } else {
        let body = spans_to_otlp_json(&batches)?;
        Ok((StatusCode::OK, Json(body)).into_response())
    }
}

// ============================================================================
// Stubs (Phase 7 / out-of-scope per spec)
// ============================================================================

/// Handle `GET`/`POST /api/search` (and the `/api/v2/...` alias).
///
/// Parses the `TraceQL` query in `params.q`, plans it, executes against
/// the spans table, and returns a Tempo `SearchResponse`.
///
/// # Errors
///
/// Returns a [`super::error::TempoError`] (rendered as JSON) for
/// parser, planner, or execution failures. Unsupported `TraceQL`
/// features surface as HTTP 501.
#[tracing::instrument(skip_all, fields(tenant_id, q = ?params.q))]
pub async fn search_traces(
    State(state): State<TempoState>,
    headers: HeaderMap,
    Query(params): Query<SearchParams>,
) -> TempoResult<Response> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", tenant_id.as_str());
    let resp = executor::execute(state.engine, tenant_id, &params).await?;
    Ok((StatusCode::OK, Json(resp)).into_response())
}

/// Handle search tags request (tag names/keys).
///
/// # TODO
/// - Query Iceberg spans table for distinct tag/attribute names
/// - Filter by time range if provided
/// - Return list of tag names in Tempo format
pub async fn search_tags(State(_state): State<TempoState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "Search tags endpoint not yet implemented. TODO: Query distinct tags from Iceberg"
        })),
    )
        .into_response()
}

/// Handle tag values request.
///
/// # TODO
/// - Query Iceberg spans table for distinct values of specified tag
/// - Filter by time range if provided
/// - Return list of tag values in Tempo format
pub async fn tag_values(State(_state): State<TempoState>, Path(_tag_name): Path<String>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "Tag values endpoint not yet implemented. TODO: Query tag values from Iceberg"
        })),
    )
        .into_response()
}

/// Health/ready check endpoint.
pub async fn ready() -> Response {
    (StatusCode::OK, "ready").into_response()
}
