//! Tempo API routes

use super::handlers;
use super::server::TempoState;
use axum::{routing::{get, post}, Router};

/// Create Tempo API router
pub fn routes(state: TempoState) -> Router {
    Router::new()
        // Trace endpoints
        .route("/api/traces/:trace_id", get(handlers::get_trace))
        .route("/api/search", post(handlers::search_traces))
        // Metadata endpoints
        .route("/api/search/tags", get(handlers::search_tags))
        .route("/api/search/tag/:name/values", get(handlers::tag_values))
        // Health check
        .route("/ready", get(handlers::ready))
        .with_state(state)
}
