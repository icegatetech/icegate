//! Prometheus API routes

use axum::{
    routing::{get, post},
    Router,
};

use super::{handlers, server::PrometheusState};

/// Create Prometheus API router
pub fn routes(state: PrometheusState) -> Router {
    Router::new()
        // Query endpoints
        .route("/api/v1/query", post(handlers::query))
        .route("/api/v1/query_range", post(handlers::query_range))
        // Metadata endpoints
        .route("/api/v1/series", get(handlers::series))
        .route("/api/v1/labels", get(handlers::labels))
        .route("/api/v1/label/:name/values", get(handlers::label_values))
        // Health check
        .route("/-/ready", get(handlers::ready))
        .with_state(state)
}
