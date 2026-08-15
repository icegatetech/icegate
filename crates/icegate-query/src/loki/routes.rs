//! Loki API routes

use std::time::Duration;

use axum::{Router, http::StatusCode, routing::get};
use icegate_common::{MemoryPressure, ShedPolicy, default_shed_response, shed_when_pressured};
use tower_http::{
    timeout::TimeoutLayer,
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing::Level;

use super::{handlers, server::LokiState};

/// Readiness paths exempt from memory-pressure shedding. `/ready` must keep
/// answering while the node sheds so the kubelet does not kill the pod exactly
/// when it is recovering.
const LOKI_SHED_BYPASS: &[&str] = &["/ready"];

/// HTTP status returned when a request exceeds the configured query duration.
/// Matches the Tempo router: Grafana reads `503` as a transient upstream
/// failure rather than a malformed query.
const TIMEOUT_STATUS: StatusCode = StatusCode::SERVICE_UNAVAILABLE;

/// Create Loki API router.
///
/// Every route carries a [`TimeoutLayer`] set to
/// `engine.max_query_duration_secs`. The handlers build their whole JSON
/// response before returning, so the layer bounds execution and not just
/// response construction — which is what lets WAL retention be derived from
/// the same number (see [`crate::engine::QueryEngineConfig`]).
pub fn routes(state: LokiState, pressure: MemoryPressure) -> Router {
    let query_timeout = Duration::from_secs(state.engine.config().max_query_duration_secs);
    Router::new()
        // Query endpoints (Loki API supports both GET and POST)
        .route("/loki/api/v1/query", get(handlers::query).post(handlers::query))
        .route(
            "/loki/api/v1/query_range",
            get(handlers::query_range).post(handlers::query_range),
        )
        // Label endpoints
        .route("/loki/api/v1/labels", get(handlers::labels))
        .route("/loki/api/v1/label/{name}/values", get(handlers::label_values))
        // Series endpoint (Loki API supports both GET and POST)
        .route("/loki/api/v1/series", get(handlers::series).post(handlers::series))
        // Health check
        .route("/ready", get(handlers::ready))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(TimeoutLayer::with_status_code(TIMEOUT_STATUS, query_timeout))
        // Outermost layer: shed new requests before any handler work while the
        // process is under memory pressure (health probes bypass).
        .layer(axum::middleware::from_fn(move |req, next| {
            shed_when_pressured(
                ShedPolicy::new(pressure.clone(), "loki", LOKI_SHED_BYPASS, false),
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
        CatalogBackend, CatalogConfig, IoHandle, MemoryPressureConfig, MemoryPressureSampler, UsageReader,
        catalog::CatalogBuilder,
    };
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        engine::{QueryEngine, QueryEngineConfig},
        infra::metrics::QueryMetrics,
    };

    /// Deterministic pressured handle: a reader reporting 95 of 100 bytes
    /// crosses the 0.90 high-watermark on the first `sample_once`, so the
    /// returned handle reports pressure without touching a real cgroup.
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

    async fn build_state() -> LokiState {
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
        // Keep the temp warehouse alive for the lifetime of the engine.
        Box::leak(Box::new(warehouse));
        LokiState {
            engine,
            metrics: Arc::new(QueryMetrics::new_disabled()),
        }
    }

    fn get_request(uri: &str) -> Request<Body> {
        Request::builder().method("GET").uri(uri).body(Body::empty()).expect("request")
    }

    #[tokio::test]
    async fn inert_guard_allows_requests() {
        let app = routes(build_state().await, MemoryPressure::inert());
        let response = app.oneshot(get_request("/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn pressured_guard_sheds_work_path() {
        let app = routes(build_state().await, pressured_guard());
        let response = app.oneshot(get_request("/loki/api/v1/query")).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn pressured_guard_bypasses_ready() {
        let app = routes(build_state().await, pressured_guard());
        let response = app.oneshot(get_request("/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Over real HTTP, not `oneshot`: the timeout is part of the Loki protocol
    /// contract now, and a client has to see `503` rather than an open socket.
    /// The catalog behind the engine never answers, so the request can only end
    /// at the deadline.
    #[tokio::test]
    async fn a_query_exceeding_the_deadline_answers_503() {
        let state = LokiState {
            engine: crate::test_support::build_stalling_engine(1),
            metrics: Arc::new(QueryMetrics::new_disabled()),
        };
        let (base_url, server) = crate::test_support::serve_router(routes(state, MemoryPressure::inert())).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/loki/api/v1/query_range"))
            .query(&[
                ("query", "{service_name=\"svc\"}"),
                // Fixed nanosecond bounds: the window only has to parse, since
                // the catalog never answers the scan behind it.
                ("start", "1700000000000000000"),
                ("end", "1700000060000000000"),
            ])
            .send()
            .await
            .expect("the server must answer, not hang");

        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        server.abort();
    }
}
