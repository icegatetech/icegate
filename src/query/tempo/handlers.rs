//! Tempo API request handlers

use super::server::TempoState;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Handle get trace by ID request
///
/// # TODO
/// - Parse trace ID from path parameter
/// - Query Iceberg spans table for all spans with matching `trace_id`
/// - Reconstruct trace structure from spans
/// - Format response in Tempo/Jaeger JSON format
pub async fn get_trace(
    State(_state): State<TempoState>,
    Path(_trace_id): Path<String>,
) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "Trace lookup not yet implemented. TODO: Query spans by trace_id from Iceberg"
        })),
    )
        .into_response()
}

/// Handle trace search request
///
/// # TODO
/// - Parse `TraceQL` query or search parameters from request body
/// - Translate `TraceQL` to Iceberg SQL for spans table
/// - Execute query via `DataFusion` against Iceberg spans table
/// - Group results by `trace_id`
/// - Format response in Tempo search format
pub async fn search_traces(State(_state): State<TempoState>) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "Trace search not yet implemented. TODO: TraceQL → SQL translation and execution"
        })),
    )
        .into_response()
}

/// Handle search tags request (tag names/keys)
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

/// Handle tag values request
///
/// # TODO
/// - Query Iceberg spans table for distinct values of specified tag
/// - Filter by time range if provided
/// - Return list of tag values in Tempo format
pub async fn tag_values(
    State(_state): State<TempoState>,
    Path(_tag_name): Path<String>,
) -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({
            "error": "Tag values endpoint not yet implemented. TODO: Query tag values from Iceberg"
        })),
    )
        .into_response()
}

/// Health/ready check endpoint
pub async fn ready() -> Response {
    (StatusCode::OK, "ready").into_response()
}
