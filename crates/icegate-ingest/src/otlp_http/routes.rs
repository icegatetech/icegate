//! OTLP HTTP API routes

use axum::{
    Router,
    routing::{get, post},
};

use super::{handlers, server::OtlpHttpState};

/// Create OTLP HTTP API router
pub fn routes(state: OtlpHttpState) -> Router {
    Router::new()
        // OTLP endpoints (support both protobuf and JSON)
        .route("/v1/logs", post(handlers::ingest_logs))
        .route("/v1/traces", post(handlers::ingest_traces))
        .route("/v1/metrics", post(handlers::ingest_metrics))
        // Health check
        .route("/health", get(handlers::health))
        .with_state(state)
}
