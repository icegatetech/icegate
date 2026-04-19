//! Tempo API request handlers.
//!
//! Thin route handlers that extract the tenant from the `x-scope-orgid`
//! header, parse query parameters and delegate to [`super::executor`]
//! for `TraceQL` search, [`super::trace_by_id`] for trace lookup, and
//! [`super::metadata`] for tag/tag-value discovery.

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
    metadata,
    models::{
        Scope, SearchParams, TagValuesQueryParams, TagValuesResponse, TagsQueryParams, TagsV1Response, TagsV2Response,
        TraceLookupParams,
    },
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
// TraceQL search
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

// ============================================================================
// Tag discovery
// ============================================================================

/// Handle `GET /api/search/tags` — flat list of all distinct tag names.
#[tracing::instrument(skip_all, fields(tenant_id, error = tracing::field::Empty))]
pub async fn search_tags_v1(
    State(state): State<TempoState>,
    headers: HeaderMap,
    Query(params): Query<TagsQueryParams>,
) -> TempoResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", tenant_id.as_str());

    let tag_names = metadata::list_tags_v1(&state, &tenant_id, params.start, params.end).await?;
    Ok((StatusCode::OK, Json(TagsV1Response { tag_names })))
}

/// Handle `GET /api/v2/search/tags` — scoped tag list used by Grafana's
/// query builder.
#[tracing::instrument(skip_all, fields(tenant_id, scope = tracing::field::Empty, error = tracing::field::Empty))]
pub async fn search_tags_v2(
    State(state): State<TempoState>,
    headers: HeaderMap,
    Query(params): Query<TagsQueryParams>,
) -> TempoResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", tenant_id.as_str());
    let scope_filter = params.scope.as_deref().and_then(Scope::parse);
    if let Some(s) = scope_filter {
        tracing::Span::current().record("scope", s.as_str());
    }

    let response: TagsV2Response =
        metadata::list_tags_v2(&state, &tenant_id, params.start, params.end, scope_filter).await?;
    Ok((StatusCode::OK, Json(response)))
}

/// Handle `GET /api/search/tag/{name}/values` — distinct values for a
/// single tag.
#[tracing::instrument(skip_all, fields(tenant_id, tag_name = %tag_name, error = tracing::field::Empty))]
pub async fn tag_values(
    State(state): State<TempoState>,
    headers: HeaderMap,
    Path(tag_name): Path<String>,
    Query(params): Query<TagValuesQueryParams>,
) -> TempoResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", tenant_id.as_str());
    let limit = params.limit.unwrap_or(TagValuesQueryParams::DEFAULT_LIMIT);

    let tag_values = metadata::list_tag_values(&state, &tenant_id, &tag_name, params.start, params.end, limit).await?;
    Ok((StatusCode::OK, Json(TagValuesResponse { tag_values })))
}

// ============================================================================
// Health
// ============================================================================

/// Health/ready check endpoint.
pub async fn ready() -> Response {
    (StatusCode::OK, "ready").into_response()
}
