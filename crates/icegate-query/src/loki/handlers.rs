//! Loki API request handlers.
//!
//! Thin route handlers that delegate to executor for query execution
//! and use typed models for responses.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use axum_extra::extract::Query as QueryExtra;
use icegate_common::errors::IceGateError;

use super::{
    error::{LokiError, LokiResult},
    executor::QueryExecutor,
    models::{LabelValuesQueryParams, LabelsQueryParams, LokiResponse, RangeQueryParams, SeriesQueryParams},
    server::LokiState,
};

// ============================================================================
// Constants
// ============================================================================

/// HTTP header for tenant identification (Grafana/Loki standard).
const TENANT_HEADER: &str = "x-scope-orgid";

/// Default tenant ID when header is not provided.
const DEFAULT_TENANT: &str = "anonymous";

// ============================================================================
// Helpers
// ============================================================================

/// Extract tenant ID from HTTP headers.
fn extract_tenant_id(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map_or_else(|| DEFAULT_TENANT.to_string(), String::from)
}

// ============================================================================
// Query Handlers
// ============================================================================

/// Handle instant query requests.
///
/// Per Loki API spec, instant queries (`/query`) use `time` parameter (not
/// start/end) and only support metric queries (returns 400 for log queries).
pub async fn query(
    State(_loki_state): State<LokiState>,
    _headers: HeaderMap,
    Query(_params): Query<RangeQueryParams>,
) -> Result<StatusCode, LokiError> {
    Err(LokiError(IceGateError::NotImplemented(
        "Instant query endpoint not yet implemented. Use /loki/api/v1/query_range instead.".to_string(),
    )))
}

/// Handle range query requests.
pub async fn query_range(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Query(params): Query<RangeQueryParams>,
) -> LokiResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    let executor = QueryExecutor::new(loki_state.engine);
    let data = executor.execute_range_query(tenant_id, &params).await?;

    Ok((StatusCode::OK, Json(LokiResponse::success(data))))
}

// ============================================================================
// Metadata Handlers
// ============================================================================

/// Handle label names request.
///
/// Loki API: `GET /loki/api/v1/labels`
pub async fn labels(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Query(params): Query<LabelsQueryParams>,
) -> LokiResult<impl IntoResponse> {
    let executor = QueryExecutor::new(loki_state.engine);
    let data = executor.execute_labels(extract_tenant_id(&headers), &params).await?;

    Ok((StatusCode::OK, Json(LokiResponse::success(data))))
}

/// Handle label values request.
///
/// Loki API: `GET /loki/api/v1/label/:name/values`
pub async fn label_values(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Path(label_name): Path<String>,
    Query(params): Query<LabelValuesQueryParams>,
) -> LokiResult<impl IntoResponse> {
    let executor = QueryExecutor::new(loki_state.engine);
    let data = executor
        .execute_label_values(extract_tenant_id(&headers), &label_name, &params)
        .await?;

    Ok((StatusCode::OK, Json(LokiResponse::success(data))))
}

/// Handle series request.
///
/// Loki API: `GET /loki/api/v1/series`
pub async fn series(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    QueryExtra(params): QueryExtra<SeriesQueryParams>,
) -> LokiResult<impl IntoResponse> {
    let executor = QueryExecutor::new(loki_state.engine);
    let data = executor.execute_series(extract_tenant_id(&headers), &params).await?;

    Ok((StatusCode::OK, Json(LokiResponse::success(data))))
}

// ============================================================================
// Health Handlers
// ============================================================================

/// Health/ready check endpoint.
pub async fn ready() -> Response {
    (StatusCode::OK, "ready").into_response()
}
