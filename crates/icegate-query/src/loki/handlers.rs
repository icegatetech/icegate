//! Loki API request handlers.
//!
//! Thin route handlers that delegate to executor for query execution
//! and use typed models for responses. Each handler records request
//! metrics via [`QueryRequestRecorder`].

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use axum_extra::extract::Query as QueryExtra;
use icegate_common::{DEFAULT_TENANT_ID, TENANT_ID_HEADER, is_valid_tenant_id};

use super::{
    error::{LokiError, LokiResult},
    executor::QueryExecutor,
    models::{LabelValuesQueryParams, LabelsQueryParams, LokiResponse, RangeQueryParams, SeriesQueryParams},
    server::LokiState,
};
use crate::{error::QueryError, infra::metrics::QueryRequestRecorder};

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

// ============================================================================
// Query Handlers
// ============================================================================

/// Handle instant query requests.
///
/// Per Loki API spec, instant queries (`/query`) use `time` parameter (not
/// start/end) and only support metric queries (returns 400 for log queries).
#[tracing::instrument(skip_all, fields(tenant_id))]
pub async fn query(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Query(_params): Query<RangeQueryParams>,
) -> Result<StatusCode, LokiError> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let mut recorder = QueryRequestRecorder::new(&loki_state.metrics, "loki", "query");
    let err = LokiError(QueryError::NotImplemented(
        "Instant query endpoint not yet implemented. Use /loki/api/v1/query_range instead.".to_string(),
    ));
    recorder.finish("error");
    Err(err)
}

/// Handle range query requests.
#[tracing::instrument(skip_all, fields(tenant_id, query = %params.query))]
pub async fn query_range(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Query(params): Query<RangeQueryParams>,
) -> LokiResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let engine = loki_state.engine;
    let metrics = loki_state.metrics;
    let mut recorder = QueryRequestRecorder::new(&metrics, "loki", "query_range");
    let executor = QueryExecutor::new(engine, std::sync::Arc::clone(&metrics));
    match executor.execute_range_query(tenant_id, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(LokiResponse::success(data))))
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

/// Handle label names request.
///
/// Loki API: `GET /loki/api/v1/labels`
#[tracing::instrument(skip_all, fields(tenant_id))]
pub async fn labels(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Query(params): Query<LabelsQueryParams>,
) -> LokiResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let engine = loki_state.engine;
    let metrics = loki_state.metrics;
    let mut recorder = QueryRequestRecorder::new(&metrics, "loki", "labels");
    let executor = QueryExecutor::new(engine, std::sync::Arc::clone(&metrics));
    match executor.execute_labels(tenant_id, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(LokiResponse::success(data))))
        }
        Err(e) => {
            recorder.finish("error");
            Err(e)
        }
    }
}

/// Handle label values request.
///
/// Loki API: `GET /loki/api/v1/label/:name/values`
#[tracing::instrument(skip_all, fields(tenant_id, label_name = %label_name))]
pub async fn label_values(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    Path(label_name): Path<String>,
    Query(params): Query<LabelValuesQueryParams>,
) -> LokiResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let engine = loki_state.engine;
    let metrics = loki_state.metrics;
    let mut recorder = QueryRequestRecorder::new(&metrics, "loki", "label_values");
    let executor = QueryExecutor::new(engine, std::sync::Arc::clone(&metrics));
    match executor.execute_label_values(tenant_id, &label_name, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(LokiResponse::success(data))))
        }
        Err(e) => {
            recorder.finish("error");
            Err(e)
        }
    }
}

/// Handle series request.
///
/// Loki API: `GET /loki/api/v1/series`
#[tracing::instrument(skip_all, fields(tenant_id))]
pub async fn series(
    State(loki_state): State<LokiState>,
    headers: HeaderMap,
    QueryExtra(params): QueryExtra<SeriesQueryParams>,
) -> LokiResult<impl IntoResponse> {
    let tenant_id = extract_tenant_id(&headers);
    tracing::Span::current().record("tenant_id", &tenant_id);
    let engine = loki_state.engine;
    let metrics = loki_state.metrics;
    let mut recorder = QueryRequestRecorder::new(&metrics, "loki", "series");
    let executor = QueryExecutor::new(engine, std::sync::Arc::clone(&metrics));
    match executor.execute_series(tenant_id, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(LokiResponse::success(data))))
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
