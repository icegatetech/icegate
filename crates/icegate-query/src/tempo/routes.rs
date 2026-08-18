//! Tempo API routes

use std::time::Duration;

use axum::{Router, extract::DefaultBodyLimit, http::StatusCode, routing::get};
use icegate_common::{MemoryPressure, ShedPolicy, default_shed_response, shed_when_pressured};
use tower_http::timeout::TimeoutLayer;

/// HTTP status returned when a request exceeds the configured query duration.
/// `503 Service Unavailable` matches axum's recommendation for an
/// upstream timeout; Grafana surfaces it as a transient error rather
/// than blaming the query for being malformed.
const TIMEOUT_STATUS: StatusCode = StatusCode::SERVICE_UNAVAILABLE;

/// Readiness/liveness paths exempt from memory-pressure shedding. `/api/echo`
/// is Grafana's search-tab liveness probe and must keep answering `200`.
const TEMPO_SHED_BYPASS: &[&str] = &["/ready", "/api/echo"];

use super::{handlers, server::TempoState, validation::MAX_BODY_BYTES};

/// Build the Tempo HTTP router.
///
/// Endpoint mapping:
/// - `GET  /api/traces/{trace_id}`          — trace lookup by id (bare OTLP).
/// - `GET  /api/v2/traces/{trace_id}`       — same lookup in the `TraceByIDResponse` envelope.
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
/// - [`TimeoutLayer`] with `engine.max_query_duration_secs` guarantees
///   the server never holds a request open indefinitely on a downstream
///   catalog hang or runaway scan. The value comes from the engine
///   config rather than a constant because it is one side of the WAL
///   retention contract (see [`crate::engine::QueryEngineConfig`]).
pub fn routes(state: TempoState, pressure: MemoryPressure) -> Router {
    let query_timeout = Duration::from_secs(state.engine.config().max_query_duration_secs);
    Router::new()
        .route("/api/traces/{trace_id}", get(handlers::get_trace))
        .route("/api/v2/traces/{trace_id}", get(handlers::get_trace_v2))
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
        .layer(TimeoutLayer::with_status_code(TIMEOUT_STATUS, query_timeout))
        .layer(axum::middleware::from_fn(move |req, next| {
            shed_when_pressured(
                ShedPolicy::new(pressure.clone(), "tempo", TEMPO_SHED_BYPASS, false),
                default_shed_response,
                req,
                next,
            )
        }))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use icegate_common::{
        CatalogBackend, CatalogConfig, IoHandle, MemoryPressure, MemoryPressureConfig, MemoryPressureSampler,
        UsageReader, catalog::CatalogBuilder,
    };
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;

    use super::*;
    use crate::engine::{QueryEngine, QueryEngineConfig};

    /// Deterministic pressured handle: 95 of 100 bytes crosses the 0.90
    /// high-watermark on the first `sample_once`.
    fn pressured_guard() -> MemoryPressure {
        struct FullReader;
        impl UsageReader for FullReader {
            fn limit_bytes(&self) -> u64 {
                100
            }
            fn read_working_set_bytes(&self) -> icegate_common::error::Result<u64> {
                Ok(95)
            }
        }
        let config = MemoryPressureConfig::default();
        let sampler = MemoryPressureSampler::with_reader(&config, Arc::new(FullReader));
        let guard = sampler.handle();
        sampler.sample_once().expect("sample_once");
        guard
    }

    /// Builds the router state over a fresh temp-dir warehouse, returning the
    /// directory guard alongside it. The caller MUST hold the guard for as long
    /// as it uses the state: dropping it removes the directory the catalog reads.
    async fn build_state() -> (TempoState, tempfile::TempDir) {
        let warehouse = tempfile::tempdir().expect("tempdir");
        let catalog_config = CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: warehouse.path().to_str().expect("path").to_string(),
            properties: std::collections::HashMap::new(),
            cache: None,
        };
        let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new())
            .await
            .expect("catalog");
        let wal_store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let wal_reader =
            Arc::new(icegate_queue::ParquetQueueReader::new("", Arc::clone(&wal_store), 8192).expect("reader"));
        let engine = Arc::new(QueryEngine::new(
            catalog,
            QueryEngineConfig::default(),
            wal_store,
            wal_reader,
        ));
        (TempoState { engine }, warehouse)
    }

    fn get_request(uri: &str) -> Request<Body> {
        Request::builder().method("GET").uri(uri).body(Body::empty()).expect("request")
    }

    #[tokio::test]
    async fn inert_guard_allows_requests() {
        let (state, _warehouse) = build_state().await;
        let app = routes(state, MemoryPressure::inert());
        let response = app.oneshot(get_request("/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn pressured_guard_sheds_work_path() {
        let (state, _warehouse) = build_state().await;
        let app = routes(state, pressured_guard());
        let response = app.oneshot(get_request("/api/search")).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn pressured_guard_bypasses_ready() {
        let (state, _warehouse) = build_state().await;
        let app = routes(state, pressured_guard());
        let response = app.oneshot(get_request("/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn pressured_guard_bypasses_echo() {
        let (state, _warehouse) = build_state().await;
        let app = routes(state, pressured_guard());
        let response = app.oneshot(get_request("/api/echo")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Driven over real HTTP: the timeout now comes from the engine config
    /// rather than a constant, so the wiring — not just the layer — is what is
    /// under test. The catalog behind the engine never answers.
    #[tokio::test]
    async fn a_search_exceeding_the_deadline_answers_503() {
        let state = TempoState {
            engine: crate::test_support::build_stalling_engine(1),
        };
        let (base_url, server) = crate::test_support::serve_router(routes(state, MemoryPressure::inert())).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/api/search?q=%7B%7D"))
            .send()
            .await
            .expect("the server must answer, not hang");

        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        server.abort();
    }
}
