//! Tempo API routes

use std::time::Duration;

use axum::{Router, extract::DefaultBodyLimit, http::StatusCode, routing::get};
use icegate_common::{ShedPolicy, default_shed_response, shed_when_pressured};
use tower_http::timeout::TimeoutLayer;

use crate::infra::{
    metrics::{PROTOCOL_TEMPO, QueryTenantRejectionRecorder},
    runtime::QueryRuntime,
    tenant::{RequestTenantPolicy, build_rejection_response, resolve_request_tenant},
};

/// HTTP status returned when a request exceeds the configured query duration.
/// `503 Service Unavailable` matches axum's recommendation for an
/// upstream timeout; Grafana surfaces it as a transient error rather
/// than blaming the query for being malformed.
const TIMEOUT_STATUS: StatusCode = StatusCode::SERVICE_UNAVAILABLE;

/// Readiness/liveness paths exempt from memory-pressure shedding and from
/// tenant resolution. `/api/echo` is Grafana's search-tab liveness probe and
/// must keep answering `200` — under memory pressure, and under a `multi`
/// policy, which neither probe carries a tenant for.
const TEMPO_SHED_BYPASS: &[&str] = &["/ready", "/api/echo"];

use super::{error::TempoError, handlers, server::TempoState, validation::MAX_BODY_BYTES};

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
pub fn routes(state: TempoState, runtime: &QueryRuntime) -> Router {
    let query_timeout = Duration::from_secs(state.engine.config().max_query_duration_secs);
    let tenant_policy = RequestTenantPolicy::new(
        runtime.tenant_resolver.clone(),
        QueryTenantRejectionRecorder::new(std::sync::Arc::clone(&runtime.metrics)),
        PROTOCOL_TEMPO,
        TEMPO_SHED_BYPASS,
    );
    let pressure = runtime.pressure.clone();
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
        // Directly under the shed layer, for the reason stated on the Loki
        // router: a request carrying no usable tenant takes none of the query
        // budget, while a node under memory pressure still sheds first.
        .layer(axum::middleware::from_fn(move |req, next| {
            resolve_request_tenant(tenant_policy.clone(), build_rejection_response::<TempoError>, req, next)
        }))
        .layer(axum::middleware::from_fn(move |req, next| {
            shed_when_pressured(
                ShedPolicy::new(pressure.clone(), PROTOCOL_TEMPO, TEMPO_SHED_BYPASS, None),
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
        CatalogBackend, CatalogConfig, DEFAULT_TENANT_ID, IoHandle, MemoryPressure, TenantId, TenantResolver,
        catalog::CatalogBuilder,
    };
    use opentelemetry::metrics::MeterProvider as _;
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        engine::{QueryEngine, QueryEngineConfig},
        infra::metrics::{
            QueryMetrics,
            test_support::{build_meter_provider, find_counter_total},
        },
        test_support::build_pressured_memory,
    };

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

    /// The runtime the router is built from. `tenant_resolver` defaults to the
    /// deployment's own tenant, which is what every case that is not about
    /// tenancy needs: under it a request with no header resolves and reaches
    /// the handler, exactly as it did before the policy existed.
    fn build_runtime(state: &TempoState, pressure: MemoryPressure, tenant_resolver: TenantResolver) -> QueryRuntime {
        QueryRuntime {
            engine: Arc::clone(&state.engine),
            metrics: Arc::new(QueryMetrics::new_disabled()),
            pressure,
            tenant_resolver,
        }
    }

    fn single_default_tenant() -> TenantResolver {
        TenantResolver::Single(TenantId::new(DEFAULT_TENANT_ID).expect("the default tenant id is valid"))
    }

    fn get_request(uri: &str) -> Request<Body> {
        Request::builder().method("GET").uri(uri).body(Body::empty()).expect("request")
    }

    #[tokio::test]
    async fn inert_guard_allows_requests() {
        let (state, _warehouse) = build_state().await;
        let runtime = build_runtime(&state, MemoryPressure::inert(), single_default_tenant());
        let app = routes(state, &runtime);
        let response = app.oneshot(get_request("/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn pressured_guard_sheds_work_path() {
        let (state, _warehouse) = build_state().await;
        let runtime = build_runtime(&state, build_pressured_memory(), single_default_tenant());
        let app = routes(state, &runtime);
        let response = app.oneshot(get_request("/api/search")).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn pressured_guard_bypasses_ready() {
        let (state, _warehouse) = build_state().await;
        let runtime = build_runtime(&state, build_pressured_memory(), single_default_tenant());
        let app = routes(state, &runtime);
        let response = app.oneshot(get_request("/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn pressured_guard_bypasses_echo() {
        let (state, _warehouse) = build_state().await;
        let runtime = build_runtime(&state, build_pressured_memory(), single_default_tenant());
        let app = routes(state, &runtime);
        let response = app.oneshot(get_request("/api/echo")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Refused, and counted under this surface's own protocol label. Driven
    /// through the router because the constant and the recorder are what each
    /// router hands the shared layer on its own.
    #[tokio::test]
    async fn a_refused_request_counts_a_rejection_for_this_protocol() {
        let (provider, exporter) = build_meter_provider();
        let (state, _warehouse) = build_state().await;
        let runtime = QueryRuntime {
            engine: Arc::clone(&state.engine),
            metrics: Arc::new(QueryMetrics::new(&provider.meter("tempo_tenant_rejections"))),
            pressure: MemoryPressure::inert(),
            tenant_resolver: TenantResolver::Multi,
        };
        let app = routes(state, &runtime);

        let response = app.oneshot(get_request("/api/search/tags")).await.expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        provider.force_flush().expect("failed to flush metrics");
        assert_eq!(
            find_counter_total(
                &exporter,
                "icegate_query_tenant_rejections",
                &[("protocol", "tempo"), ("reason", "missing")],
            ),
            1
        );
    }

    /// Both probes must answer under `multi`, where nothing carries a tenant:
    /// a `400` on either is a restart of a pod that is serving correctly, or
    /// Grafana hiding its search builder behind a connection banner.
    #[tokio::test]
    async fn multi_leaves_the_probe_paths_alone() {
        for path in TEMPO_SHED_BYPASS {
            let (state, _warehouse) = build_state().await;
            let runtime = build_runtime(&state, MemoryPressure::inert(), TenantResolver::Multi);
            let app = routes(state, &runtime);
            let response = app.oneshot(get_request(path)).await.expect("response");
            assert_eq!(response.status(), StatusCode::OK, "{path} must answer under multi");
        }
    }

    /// Under memory pressure the shed layer answers first, whatever the tenant
    /// policy would have said: it is the outer of the two, and Flight SQL nests
    /// its interceptors the same way round.
    #[tokio::test]
    async fn a_request_without_a_tenant_is_shed_before_the_tenant_layer() {
        let (state, _warehouse) = build_state().await;
        let runtime = build_runtime(&state, build_pressured_memory(), TenantResolver::Multi);
        let app = routes(state, &runtime);
        let response = app.oneshot(get_request("/api/search/tags")).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Driven over real HTTP: the timeout now comes from the engine config
    /// rather than a constant, so the wiring — not just the layer — is what is
    /// under test. The catalog behind the engine never answers.
    #[tokio::test]
    async fn a_search_exceeding_the_deadline_answers_503() {
        let state = TempoState {
            engine: crate::test_support::build_stalling_engine(1),
        };
        let runtime = build_runtime(&state, MemoryPressure::inert(), single_default_tenant());
        let (base_url, server) = crate::test_support::serve_router(routes(state, &runtime)).await;

        let response = reqwest::Client::new()
            .get(format!("{base_url}/api/search?q=%7B%7D"))
            .send()
            .await
            .expect("the server must answer, not hang");

        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
        server.abort();
    }
}
