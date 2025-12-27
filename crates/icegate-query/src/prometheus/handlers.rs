//! Prometheus API request handlers

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

use super::server::PrometheusState;

/// Handle instant query requests
///
/// # TODO
/// - Parse `PromQL` query from request parameters
/// - Translate `PromQL` to Iceberg SQL for metrics table
/// - Execute query via `DataFusion` against Iceberg metrics table
/// - Format response in Prometheus JSON format (instant vector)
pub async fn query(State(_state): State<PrometheusState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "status": "error",
            "error": "Query execution not yet implemented. TODO: PromQL → SQL translation and execution"
        })),
    )
        .into_response()
}

/// Handle range query requests
///
/// # TODO
/// - Parse `PromQL` range query from request parameters (start, end, step)
/// - Translate `PromQL` to Iceberg SQL
/// - Execute query via `DataFusion` against Iceberg metrics table
/// - Format response in Prometheus JSON format (range vector/matrix)
pub async fn query_range(State(_state): State<PrometheusState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "status": "error",
            "error": "Range query execution not yet implemented. TODO: PromQL range → SQL translation"
        })),
    )
        .into_response()
}

/// Handle series metadata request
///
/// # TODO
/// - Parse match[] selectors from request parameters
/// - Query Iceberg metrics table for matching time series
/// - Return series labels in Prometheus format
pub async fn series(State(_state): State<PrometheusState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "status": "error",
            "error": "Series endpoint not yet implemented. TODO: Query series metadata from Iceberg"
        })),
    )
        .into_response()
}

/// Handle label names request
///
/// # TODO
/// - Query Iceberg metrics table for distinct label names
/// - Filter by time range and series matchers if provided
/// - Return list of label names in Prometheus format
pub async fn labels(State(_state): State<PrometheusState>) -> Response {
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
/// - Query Iceberg metrics table for distinct values of specified label
/// - Filter by time range and series matchers if provided
/// - Return list of label values in Prometheus format
pub async fn label_values(State(_state): State<PrometheusState>, Path(_label_name): Path<String>) -> Response {
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
