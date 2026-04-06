//! Prometheus API request handlers.
//!
//! Thin route handlers that delegate to [`QueryExecutor`] for query
//! execution and use typed models for responses. Each handler records
//! request metrics via [`QueryRequestRecorder`].

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use icegate_common::{DEFAULT_TENANT_ID, TENANT_ID_HEADER, is_valid_tenant_id};

use super::{
    error::{PrometheusError, PrometheusResult},
    executor::QueryExecutor,
    models::{InstantQueryParams, LabelValuesParams, LabelsParams, PrometheusResponse, RangeQueryParams, SeriesParams},
    server::PrometheusState,
};
use crate::infra::metrics::QueryRequestRecorder;

// ============================================================================
// Helpers
// ============================================================================

/// Extract tenant ID from HTTP headers.
///
/// Returns the header value if present and valid (ASCII alphanumeric,
/// hyphens, underscores). Falls back to `DEFAULT_TENANT_ID` otherwise --
/// matching the ingest-path behaviour so that data is always queryable
/// under the same tenant that was used during ingestion.
fn extract_tenant_id(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|s| is_valid_tenant_id(s))
        .map_or_else(|| DEFAULT_TENANT_ID.to_string(), String::from)
}

// ============================================================================
// Query Handlers
// ============================================================================

/// Handle instant query requests.
///
/// Prometheus API: `POST /api/v1/query`
///
/// Parses the `PromQL` expression, evaluates it at a single point in time,
/// and returns results as an instant vector.
#[tracing::instrument(skip_all, fields(tenant_id))]
pub async fn query(
    State(state): State<PrometheusState>,
    headers: HeaderMap,
    axum::extract::Form(params): axum::extract::Form<InstantQueryParams>,
) -> PrometheusResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let mut recorder = QueryRequestRecorder::new(&state.metrics, "prometheus", "query");
    let executor = QueryExecutor::new(state.engine, std::sync::Arc::clone(&state.metrics));
    match executor.execute_instant_query(tenant_id, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(PrometheusResponse::success(data))))
        }
        Err(e) => {
            recorder.finish("error");
            Err(e)
        }
    }
}

/// Handle range query requests.
///
/// Prometheus API: `POST /api/v1/query_range`
///
/// Parses the `PromQL` expression, evaluates it over a time range with
/// the given step interval, and returns results as a range vector
/// (matrix).
#[tracing::instrument(skip_all, fields(tenant_id))]
pub async fn query_range(
    State(state): State<PrometheusState>,
    headers: HeaderMap,
    axum::extract::Form(params): axum::extract::Form<RangeQueryParams>,
) -> PrometheusResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let mut recorder = QueryRequestRecorder::new(&state.metrics, "prometheus", "query_range");
    let executor = QueryExecutor::new(state.engine, std::sync::Arc::clone(&state.metrics));
    match executor.execute_range_query(tenant_id, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(PrometheusResponse::success(data))))
        }
        Err(e) => {
            recorder.finish("error");
            Err(e)
        }
    }
}

// ============================================================================
// Metadata Handlers
// ============================================================================

/// Handle series metadata request.
///
/// Prometheus API: `GET /api/v1/series`
///
/// # TODO
/// - Implement series matching once labels planner is available
#[tracing::instrument(skip_all, fields(tenant_id))]
pub async fn series(
    State(state): State<PrometheusState>,
    headers: HeaderMap,
    Query(_params): Query<SeriesParams>,
) -> Result<StatusCode, PrometheusError> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let mut recorder = QueryRequestRecorder::new(&state.metrics, "prometheus", "series");
    let err: PrometheusError =
        crate::error::QueryError::NotImplemented("series endpoint (requires labels planner)".to_string()).into();
    recorder.finish("error");
    Err(err)
}

/// Handle label names request.
///
/// Prometheus API: `GET /api/v1/labels`
///
/// # TODO
/// - Implement label names query once labels planner is available
#[tracing::instrument(skip_all, fields(tenant_id))]
pub async fn labels(
    State(state): State<PrometheusState>,
    headers: HeaderMap,
    Query(params): Query<LabelsParams>,
) -> PrometheusResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let mut recorder = QueryRequestRecorder::new(&state.metrics, "prometheus", "labels");
    let executor = QueryExecutor::new(state.engine, std::sync::Arc::clone(&state.metrics));
    match executor.execute_labels(tenant_id, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((
                StatusCode::OK,
                Json(PrometheusResponse::success(serde_json::json!(data))),
            ))
        }
        Err(e) => {
            recorder.finish("error");
            Err(e)
        }
    }
}

/// Handle label values request.
///
/// Prometheus API: `GET /api/v1/label/{name}/values`
///
/// # TODO
/// - Implement label values query once labels planner is available
#[tracing::instrument(skip_all, fields(tenant_id, label_name = %label_name))]
pub async fn label_values(
    State(state): State<PrometheusState>,
    headers: HeaderMap,
    Path(label_name): Path<String>,
    Query(params): Query<LabelValuesParams>,
) -> PrometheusResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let mut recorder = QueryRequestRecorder::new(&state.metrics, "prometheus", "label_values");
    let executor = QueryExecutor::new(state.engine, std::sync::Arc::clone(&state.metrics));
    match executor.execute_label_values(tenant_id, &label_name, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((
                StatusCode::OK,
                Json(PrometheusResponse::success(serde_json::json!(data))),
            ))
        }
        Err(e) => {
            recorder.finish("error");
            Err(e)
        }
    }
}

// ============================================================================
// Health Handlers
// ============================================================================

/// Health/ready check endpoint.
pub async fn ready() -> Response {
    (StatusCode::OK, "ready").into_response()
}
