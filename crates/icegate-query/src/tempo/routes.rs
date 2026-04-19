//! Tempo API routes

use axum::{Router, routing::get};

use super::{handlers, server::TempoState};

/// Build the Tempo HTTP router.
pub fn routes(state: TempoState) -> Router {
    Router::new()
        .route("/api/traces/{trace_id}", get(handlers::get_trace))
        .route("/api/v2/traces/{trace_id}", get(handlers::get_trace))
        .route(
            "/api/search",
            get(handlers::search_traces).post(handlers::search_traces),
        )
        .route("/api/search/tags", get(handlers::search_tags))
        .route("/api/v2/search/tags", get(handlers::search_tags))
        .route("/api/search/tag/{name}/values", get(handlers::tag_values))
        .route("/ready", get(handlers::ready))
        .with_state(state)
}
