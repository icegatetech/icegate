//! Prometheus API routes

use std::time::Duration;

use axum::{
    Router,
    http::StatusCode,
    routing::{get, post},
};
use icegate_common::{MemoryPressure, ShedPolicy, default_shed_response, shed_when_pressured};
use tower_http::timeout::TimeoutLayer;

use super::{handlers, server::PrometheusState};

/// Readiness path exempt from memory-pressure shedding.
const PROMETHEUS_SHED_BYPASS: &[&str] = &["/-/ready"];

/// HTTP status returned when a request exceeds the configured query duration,
/// matching the Loki and Tempo routers.
const TIMEOUT_STATUS: StatusCode = StatusCode::SERVICE_UNAVAILABLE;

/// Create Prometheus API router.
///
/// Carries the same `engine.max_query_duration_secs` [`TimeoutLayer`] as the
/// other HTTP APIs; see [`crate::engine::QueryEngineConfig`] for why that number
/// is also what WAL retention is derived from.
pub fn routes(state: PrometheusState, pressure: MemoryPressure) -> Router {
    let query_timeout = Duration::from_secs(state.engine.config().max_query_duration_secs);
    Router::new()
        // Query endpoints
        .route("/api/v1/query", post(handlers::query))
        .route("/api/v1/query_range", post(handlers::query_range))
        // Metadata endpoints
        .route("/api/v1/series", get(handlers::series))
        .route("/api/v1/labels", get(handlers::labels))
        .route("/api/v1/label/{name}/values", get(handlers::label_values))
        // Health check
        .route("/-/ready", get(handlers::ready))
        .layer(TimeoutLayer::with_status_code(TIMEOUT_STATUS, query_timeout))
        .layer(axum::middleware::from_fn(move |req, next| {
            shed_when_pressured(
                ShedPolicy::new(pressure.clone(), "prometheus", PROMETHEUS_SHED_BYPASS, false),
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

    async fn build_state() -> PrometheusState {
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
        Box::leak(Box::new(warehouse));
        PrometheusState { engine }
    }

    fn get_request(uri: &str) -> Request<Body> {
        Request::builder().method("GET").uri(uri).body(Body::empty()).expect("request")
    }

    #[tokio::test]
    async fn inert_guard_allows_requests() {
        let app = routes(build_state().await, MemoryPressure::inert());
        let response = app.oneshot(get_request("/-/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn pressured_guard_sheds_work_path() {
        let app = routes(build_state().await, pressured_guard());
        let response = app.oneshot(get_request("/api/v1/query")).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn pressured_guard_bypasses_ready() {
        let app = routes(build_state().await, pressured_guard());
        let response = app.oneshot(get_request("/-/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    // A "query exceeds the deadline" case cannot be driven through this router
    // yet: every Prometheus handler is still a `501` stub (`handlers.rs`), so no
    // request reaches the engine and the timeout can never fire. The layer is
    // wired here the same way Loki and Tempo wire theirs (both covered by a
    // transport test); add the case with the first handler that executes a query.
}
