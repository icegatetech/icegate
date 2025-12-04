//! Loki API routes

use super::handlers;
use super::server::LokiState;
use axum::{routing::get, Router};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

/// Create Loki API router
pub fn routes(state: LokiState) -> Router {
    Router::new()
        // Query endpoints (Loki API supports both GET and POST)
        .route("/loki/api/v1/query", get(handlers::query).post(handlers::query))
        .route("/loki/api/v1/query_range", get(handlers::query_range).post(handlers::query_range))
        .route("/loki/api/v1/explain", get(handlers::explain_query))
        // Label endpoints
        .route("/loki/api/v1/labels", get(handlers::labels))
        .route("/loki/api/v1/label/:name/values", get(handlers::label_values))
        // Health check
        .route("/ready", get(handlers::ready))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(state)
}
