//! Tempo API routes

use axum::{Router, extract::DefaultBodyLimit, http::StatusCode, routing::get};
use tower_http::timeout::TimeoutLayer;

/// HTTP status returned when a request exceeds [`REQUEST_TIMEOUT`].
/// `503 Service Unavailable` matches axum's recommendation for an
/// upstream timeout; Grafana surfaces it as a transient error rather
/// than blaming the query for being malformed.
const TIMEOUT_STATUS: StatusCode = StatusCode::SERVICE_UNAVAILABLE;

use super::{
    handlers,
    server::TempoState,
    validation::{MAX_BODY_BYTES, REQUEST_TIMEOUT},
};

/// Build the Tempo HTTP router.
///
/// Endpoint mapping:
/// - `GET  /api/traces/{trace_id}`          — trace lookup by id.
/// - `GET  /api/v2/traces/{trace_id}`       — v2 alias of trace lookup.
/// - `GET  / POST /api/search`              — `TraceQL` search.
/// - `GET  /api/search/tags`                — v1 flat tag list.
/// - `GET  /api/v2/search/tags`             — v2 scoped tag list (Grafana).
/// - `GET  /api/search/tag/{name}/values`   — v1 tag-value enumeration.
/// - `GET  /api/v2/search/tag/{name}/values`— v2 typed tag-value enumeration
///   (Grafana's query builder uses the `{type, value}` payload to render
///   enum dropdowns and value pickers).
/// - `GET  /api/echo`                       — Grafana search-tab liveness
///   probe; must return 200 with body `echo` or Grafana hides the search
///   builder behind an "Unable to connect to Tempo search" banner.
/// - `GET  /ready`                          — health check.
///
/// # Middleware
///
/// Two layers are applied to every route:
/// - [`DefaultBodyLimit`] caps incoming POST bodies at
///   [`MAX_BODY_BYTES`]. Axum's default of 2 `MiB` is far larger than any
///   legitimate Tempo request body and would let an attacker drive the
///   lexer / parser with megabyte-sized `q=` parameters.
/// - [`TimeoutLayer`] with [`REQUEST_TIMEOUT`] guarantees the server
///   never holds a request open indefinitely on a downstream catalog
///   hang or runaway scan.
pub fn routes(state: TempoState) -> Router {
    Router::new()
        .route("/api/traces/{trace_id}", get(handlers::get_trace))
        .route("/api/v2/traces/{trace_id}", get(handlers::get_trace))
        .route(
            "/api/search",
            get(handlers::search_traces).post(handlers::search_traces),
        )
        // Metadata endpoints — v1 and v2 have different response shapes.
        .route("/api/search/tags", get(handlers::search_tags_v1))
        .route("/api/v2/search/tags", get(handlers::search_tags_v2))
        .route("/api/search/tag/{name}/values", get(handlers::tag_values))
        .route("/api/v2/search/tag/{name}/values", get(handlers::tag_values_v2))
        .route("/api/echo", get(handlers::echo))
        .route("/ready", get(handlers::ready))
        .layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
        .layer(TimeoutLayer::with_status_code(TIMEOUT_STATUS, REQUEST_TIMEOUT))
        .with_state(state)
}
