//! Loki API routes

use axum::{Router, routing::get};
use icegate_common::{MemoryPressure, ShedPolicy, default_shed_response, shed_when_pressured};
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use super::{handlers, server::LokiState};

/// Readiness paths exempt from memory-pressure shedding. `/ready` must keep
/// answering while the node sheds so the kubelet does not kill the pod exactly
/// when it is recovering.
const LOKI_SHED_BYPASS: &[&str] = &["/ready"];

/// Create Loki API router
pub fn routes(state: LokiState, pressure: MemoryPressure) -> Router {
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

    /// Builds the router state over a fresh temp-dir warehouse, returning the
    /// directory guard alongside it. The caller MUST hold the guard for as long
    /// as it uses the state: dropping it removes the directory the catalog reads.
    async fn build_state() -> (LokiState, tempfile::TempDir) {
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
        (
            LokiState {
                engine,
                metrics: Arc::new(QueryMetrics::new_disabled()),
            },
            warehouse,
        )
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
        let response = app.oneshot(get_request("/loki/api/v1/query")).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn pressured_guard_bypasses_ready() {
        let (state, _warehouse) = build_state().await;
        let app = routes(state, pressured_guard());
        let response = app.oneshot(get_request("/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }
}
