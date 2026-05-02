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
use iceberg::expr::Predicate;
use icegate_common::{DEFAULT_TENANT_ID, TENANT_ID_HEADER, is_valid_tenant_id};
use serde_json::json;

use super::{
    error::{TempoError, TempoResult},
    executor,
    formatters::{spans_to_otlp_json, spans_to_otlp_proto},
    metadata,
    models::{
        Scope, SearchParams, TagValuesQueryParams, TagValuesResponse, TagValuesV2Metrics, TagValuesV2Response,
        TagsQueryParams, TagsV1Response, TagsV2Response, TraceLookupParams,
    },
    server::TempoState,
    trace_by_id::{default_window, fetch},
    validation,
};
use crate::{error::QueryError, traceql::iceberg_predicate::translate_query_to_predicate_excluding};

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
/// Returns [`QueryError::Validation`] for ambiguous/out-of-range inputs —
/// silently clamping to `UNIX_EPOCH` would mask client bugs and produce
/// surprise wide-window scans starting in 1970.
///
/// # Errors
///
/// Returns [`TempoError`] (HTTP 400) when `secs` is outside chrono's
/// representable range or otherwise non-single.
fn parse_epoch_seconds(secs: i64) -> TempoResult<DateTime<Utc>> {
    Utc.timestamp_opt(secs, 0).single().ok_or_else(|| {
        TempoError::new(QueryError::Validation(format!(
            "invalid epoch-seconds timestamp: {secs} (out of representable range)"
        )))
    })
}

/// Parse the optional `?q=` `TraceQL` query and translate it to an
/// iceberg [`Predicate`] for metadata-scan pushdown.
///
/// `None`, `Some("")`, and `Some("{}")` collapse to
/// [`Predicate::AlwaysTrue`] without touching the parser. Anything else
/// is length-validated, parsed in `spawn_blocking` (ANTLR uses `Rc` and
/// is therefore not `Send`), and translated via
/// [`translate_query_to_predicate_excluding`]. Non-pushdownable clauses
/// are silently dropped per the metadata-scan over-approximation
/// contract.
///
/// `exclude_column` is the underlying physical column the caller is
/// enumerating distinct values of, when applicable. Any conjunct in
/// `q` that targets the same column is dropped from the pushdown so
/// the dropdown still shows every value satisfying the *other*
/// clauses. See
/// [`super::metadata::target_column_for_tag`] for the resolution.
/// Pass `None` for endpoints that aren't enumerating a specific tag's
/// values (e.g. `/api/v2/search/tags`).
///
/// Malformed queries surface as HTTP 400 (`Parse`), matching
/// [`super::executor::execute`]'s behaviour on `/api/search`.
async fn parse_q_to_predicate(q: Option<&str>, exclude_column: Option<&str>) -> TempoResult<Predicate> {
    let Some(q) = q else {
        return Ok(Predicate::AlwaysTrue);
    };
    if q.is_empty() || q == "{}" {
        return Ok(Predicate::AlwaysTrue);
    }
    validation::validate_query_length(q).map_err(TempoError::new)?;
    let expr = super::executor::parse_query_blocking(q.to_string()).await?;
    Ok(translate_query_to_predicate_excluding(&expr, exclude_column))
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
    let start = params.start.map(parse_epoch_seconds).transpose()?.unwrap_or(default_start);
    let end = params.end.map(parse_epoch_seconds).transpose()?.unwrap_or(default_end);
    validation::validate_query_window(start, end).map_err(TempoError::new)?;

    let result = fetch(state.engine, &tenant_id, &trace_id, start, end).await?;
    if result.batches.iter().all(|b| b.num_rows() == 0) {
        return Ok((StatusCode::NOT_FOUND, Json(json!({"error": "trace not found"}))).into_response());
    }

    // Surface truncation to the caller via response header — the body
    // payload itself stays valid OTLP so existing decoders keep working.
    let truncated_header_value = axum::http::HeaderValue::from_static("true");
    let truncated_pair = result.truncated.then_some(("x-icegate-truncated", truncated_header_value));

    if wants_protobuf(&headers) {
        let bytes = spans_to_otlp_proto(&result.batches)?;
        let content_type = axum::http::HeaderValue::from_static("application/protobuf");
        let mut resp = ([(header::CONTENT_TYPE, content_type)], bytes).into_response();
        if let Some((name, value)) = truncated_pair {
            resp.headers_mut().insert(name, value);
        }
        Ok(resp)
    } else {
        let body = spans_to_otlp_json(&result.batches)?;
        let mut resp = (StatusCode::OK, Json(body)).into_response();
        if let Some((name, value)) = truncated_pair {
            resp.headers_mut().insert(name, value);
        }
        Ok(resp)
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
#[tracing::instrument(
    skip_all,
    fields(
        endpoint = "tempo.search_tags_v1",
        tenant_id = tracing::field::Empty,
        q = ?params.q,
        start = ?params.start,
        end = ?params.end,
        effective_start = tracing::field::Empty,
        effective_end = tracing::field::Empty,
        result_count = tracing::field::Empty,
        error = tracing::field::Empty,
    ),
)]
pub async fn search_tags_v1(
    State(state): State<TempoState>,
    headers: HeaderMap,
    Query(params): Query<TagsQueryParams>,
) -> TempoResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", tenant_id.as_str());
    // Tag-list endpoints don't enumerate a specific column, so no
    // self-exclusion applies.
    let extra_predicate = parse_q_to_predicate(params.q.as_deref(), None).await?;

    let tag_names = metadata::list_tags_v1(&state, &tenant_id, params.start, params.end, extra_predicate).await?;
    tracing::Span::current().record("result_count", tag_names.len());
    Ok((StatusCode::OK, Json(TagsV1Response { tag_names })))
}

/// Handle `GET /api/v2/search/tags` — scoped tag list used by Grafana's
/// query builder.
#[tracing::instrument(
    skip_all,
    fields(
        endpoint = "tempo.search_tags_v2",
        tenant_id = tracing::field::Empty,
        scope = tracing::field::Empty,
        q = ?params.q,
        start = ?params.start,
        end = ?params.end,
        effective_start = tracing::field::Empty,
        effective_end = tracing::field::Empty,
        result_count = tracing::field::Empty,
        error = tracing::field::Empty,
    ),
)]
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
    // Tag-list endpoints don't enumerate a specific column, so no
    // self-exclusion applies.
    let extra_predicate = parse_q_to_predicate(params.q.as_deref(), None).await?;

    let response: TagsV2Response = metadata::list_tags_v2(
        &state,
        &tenant_id,
        params.start,
        params.end,
        scope_filter,
        extra_predicate,
    )
    .await?;
    tracing::Span::current().record(
        "result_count",
        response.scopes.iter().map(|s| s.tags.len()).sum::<usize>(),
    );
    Ok((StatusCode::OK, Json(response)))
}

/// Handle `GET /api/search/tag/{name}/values` — distinct values for a
/// single tag.
#[tracing::instrument(
    skip_all,
    fields(
        endpoint = "tempo.tag_values",
        tenant_id = tracing::field::Empty,
        tag_name = %tag_name,
        target_column = tracing::field::Empty,
        q = ?params.q,
        start = ?params.start,
        end = ?params.end,
        effective_start = tracing::field::Empty,
        effective_end = tracing::field::Empty,
        limit = tracing::field::Empty,
        result_count = tracing::field::Empty,
        error = tracing::field::Empty,
    ),
)]
pub async fn tag_values(
    State(state): State<TempoState>,
    headers: HeaderMap,
    Path(tag_name): Path<String>,
    Query(params): Query<TagValuesQueryParams>,
) -> TempoResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", tenant_id.as_str());
    validation::validate_tag_name(&tag_name).map_err(TempoError::new)?;
    let limit = params.limit.unwrap_or(TagValuesQueryParams::DEFAULT_LIMIT);
    tracing::Span::current().record("limit", limit);
    // Drop self-referential clauses from `q` so e.g. asking for
    // distinct `service.name` with `q={ resource.service.name = "x" }`
    // still returns every service rather than collapsing to `["x"]`.
    let exclude_column = metadata::target_column_for_tag(&tag_name);
    if let Some(col) = exclude_column {
        tracing::Span::current().record("target_column", col);
    }
    let extra_predicate = parse_q_to_predicate(params.q.as_deref(), exclude_column).await?;

    let tag_values = metadata::list_tag_values(
        &state,
        &tenant_id,
        &tag_name,
        params.start,
        params.end,
        limit,
        extra_predicate,
    )
    .await?;
    tracing::Span::current().record("result_count", tag_values.len());
    Ok((StatusCode::OK, Json(TagValuesResponse { tag_values })))
}

/// Handle `GET /api/v2/search/tag/{name}/values` — typed distinct values
/// for a single `TraceQL` identifier. Mirrors [`tag_values`] but wraps
/// each value with its `TraceQL` value type and includes a `metrics`
/// block so Grafana's query-builder UI can render the right input
/// widget (free-text vs. dropdown vs. duration picker).
///
/// Without this v2 route Grafana renders a 404 in the explore tab and
/// the value pickers for `name`, `status`, `kind`, `resource.service.name`,
/// etc. all fail with no completion suggestions, even though the v1
/// endpoint serves the same data.
#[tracing::instrument(
    skip_all,
    fields(
        endpoint = "tempo.tag_values_v2",
        tenant_id = tracing::field::Empty,
        tag_name = %tag_name,
        target_column = tracing::field::Empty,
        q = ?params.q,
        start = ?params.start,
        end = ?params.end,
        effective_start = tracing::field::Empty,
        effective_end = tracing::field::Empty,
        limit = tracing::field::Empty,
        result_count = tracing::field::Empty,
        error = tracing::field::Empty,
    ),
)]
pub async fn tag_values_v2(
    State(state): State<TempoState>,
    headers: HeaderMap,
    Path(tag_name): Path<String>,
    Query(params): Query<TagValuesQueryParams>,
) -> TempoResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", tenant_id.as_str());
    validation::validate_tag_name(&tag_name).map_err(TempoError::new)?;
    let limit = params.limit.unwrap_or(TagValuesQueryParams::DEFAULT_LIMIT);
    tracing::Span::current().record("limit", limit);
    // Drop self-referential clauses from `q` so e.g. asking for
    // distinct `status` with `q={ status = error }` still returns
    // every status rather than collapsing to `["error"]`.
    let exclude_column = metadata::target_column_for_tag(&tag_name);
    if let Some(col) = exclude_column {
        tracing::Span::current().record("target_column", col);
    }
    let extra_predicate = parse_q_to_predicate(params.q.as_deref(), exclude_column).await?;

    let tag_values = metadata::list_tag_values_v2(
        &state,
        &tenant_id,
        &tag_name,
        params.start,
        params.end,
        limit,
        extra_predicate,
    )
    .await?;
    tracing::Span::current().record("result_count", tag_values.len());
    Ok((
        StatusCode::OK,
        Json(TagValuesV2Response {
            tag_values,
            metrics: TagValuesV2Metrics {
                inspected_bytes: "0".to_string(),
            },
        }),
    ))
}

// ============================================================================
// Health
// ============================================================================

/// Health/ready check endpoint.
pub async fn ready() -> Response {
    (StatusCode::OK, "ready").into_response()
}

/// Tempo `/api/echo` query-frontend liveness probe.
///
/// Grafana's Tempo data source uses this endpoint to verify that the
/// search backend is reachable. When it is missing, Grafana renders the
/// banner *"Unable to connect to Tempo search. Please ensure that Tempo
/// is configured with search enabled."* even though the search endpoints
/// themselves work — disabling the in-builder hints, value pickers, and
/// streaming progress UI in the explore view.
///
/// Per the Tempo API spec the response must be HTTP 200 with the literal
/// body `echo`.
pub async fn echo() -> Response {
    (StatusCode::OK, "echo").into_response()
}
