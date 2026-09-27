//! Loki API request handlers.
//!
//! Thin route handlers that delegate to executor for query execution
//! and use typed models for responses. Each handler records request
//! metrics via [`QueryRequestRecorder`].

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use axum_extra::extract::Query as QueryExtra;
use icegate_common::TenantId;

use super::{
    error::{LokiError, LokiResult},
    executor::QueryExecutor,
    models::{LabelValuesQueryParams, LabelsQueryParams, LokiResponse, RangeQueryParams, SeriesQueryParams},
    server::LokiState,
};
use crate::{error::QueryError, infra::metrics::QueryRequestRecorder};

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
    Extension(tenant_id): Extension<TenantId>,
    Query(_params): Query<RangeQueryParams>,
) -> Result<StatusCode, LokiError> {
    tracing::Span::current().record("tenant_id", tenant_id.as_ref());
    let mut recorder = QueryRequestRecorder::new(&loki_state.metrics, "loki", "query");
    let err = LokiError(QueryError::NotImplemented(
        "Instant query endpoint not yet implemented. Use /loki/api/v1/query_range instead.".to_string(),
    ));
    recorder.finish("error");
    Err(err)
}

/// Handle range query requests.
#[tracing::instrument(skip_all, fields(tenant_id, query = %params.query, error = tracing::field::Empty))]
pub async fn query_range(
    State(loki_state): State<LokiState>,
    Extension(tenant_id): Extension<TenantId>,
    Query(params): Query<RangeQueryParams>,
) -> LokiResult<impl IntoResponse> {
    tracing::Span::current().record("tenant_id", tenant_id.as_ref());
    let engine = loki_state.engine;
    let metrics = loki_state.metrics;
    let mut recorder = QueryRequestRecorder::new(&metrics, "loki", "query_range");
    let executor = QueryExecutor::new(engine, std::sync::Arc::clone(&metrics));
    match executor.execute_range_query(&tenant_id, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(LokiResponse::success(data))))
        }
        Err(e) => {
            recorder.finish("error");
            record_handler_error("query_range", &e);
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
#[tracing::instrument(skip_all, fields(tenant_id, error = tracing::field::Empty))]
pub async fn labels(
    State(loki_state): State<LokiState>,
    Extension(tenant_id): Extension<TenantId>,
    Query(params): Query<LabelsQueryParams>,
) -> LokiResult<impl IntoResponse> {
    tracing::Span::current().record("tenant_id", tenant_id.as_ref());
    let engine = loki_state.engine;
    let metrics = loki_state.metrics;
    let mut recorder = QueryRequestRecorder::new(&metrics, "loki", "labels");
    let executor = QueryExecutor::new(engine, std::sync::Arc::clone(&metrics));
    match executor.execute_labels(&tenant_id, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(LokiResponse::success(data))))
        }
        Err(e) => {
            recorder.finish("error");
            record_handler_error("labels", &e);
            Err(e)
        }
    }
}

/// Handle label values request.
///
/// Loki API: `GET /loki/api/v1/label/:name/values`
#[tracing::instrument(skip_all, fields(tenant_id, label_name = %label_name, error = tracing::field::Empty))]
pub async fn label_values(
    State(loki_state): State<LokiState>,
    Extension(tenant_id): Extension<TenantId>,
    Path(label_name): Path<String>,
    Query(params): Query<LabelValuesQueryParams>,
) -> LokiResult<impl IntoResponse> {
    tracing::Span::current().record("tenant_id", tenant_id.as_ref());
    let engine = loki_state.engine;
    let metrics = loki_state.metrics;
    let mut recorder = QueryRequestRecorder::new(&metrics, "loki", "label_values");
    let executor = QueryExecutor::new(engine, std::sync::Arc::clone(&metrics));
    match executor.execute_label_values(&tenant_id, &label_name, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(LokiResponse::success(data))))
        }
        Err(e) => {
            recorder.finish("error");
            record_handler_error("label_values", &e);
            Err(e)
        }
    }
}

/// Handle series request.
///
/// Loki API: `GET /loki/api/v1/series`
#[tracing::instrument(skip_all, fields(tenant_id, error = tracing::field::Empty))]
pub async fn series(
    State(loki_state): State<LokiState>,
    Extension(tenant_id): Extension<TenantId>,
    QueryExtra(params): QueryExtra<SeriesQueryParams>,
) -> LokiResult<impl IntoResponse> {
    tracing::Span::current().record("tenant_id", tenant_id.as_ref());
    let engine = loki_state.engine;
    let metrics = loki_state.metrics;
    let mut recorder = QueryRequestRecorder::new(&metrics, "loki", "series");
    let executor = QueryExecutor::new(engine, std::sync::Arc::clone(&metrics));
    match executor.execute_series(&tenant_id, &params).await {
        Ok(data) => {
            recorder.finish("ok");
            Ok((StatusCode::OK, Json(LokiResponse::success(data))))
        }
        Err(e) => {
            recorder.finish("error");
            record_handler_error("series", &e);
            Err(e)
        }
    }
}

/// Record a handler error onto the current tracing span and emit an error event.
fn record_handler_error(handler: &'static str, err: &LokiError) {
    tracing::Span::current().record("error", tracing::field::display(&err.0));
    tracing::error!(handler, error = %err.0, error.debug = ?err.0, "loki handler failed");
}

// ============================================================================
// Health Handlers
// ============================================================================

/// Health/ready check endpoint.
pub async fn ready() -> Response {
    (StatusCode::OK, "ready").into_response()
}
