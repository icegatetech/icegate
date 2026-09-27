//! Loki API routes

use std::time::Duration;

use axum::{Router, http::StatusCode, routing::get};
use icegate_common::{ShedPolicy, default_shed_response, shed_when_pressured};
use tower_http::{
    timeout::TimeoutLayer,
    trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer},
};
use tracing::Level;

use super::{error::LokiError, handlers, server::LokiState};
use crate::infra::{
    metrics::{PROTOCOL_LOKI, QueryTenantRejectionRecorder},
    runtime::QueryRuntime,
    tenant::{RequestTenantPolicy, build_rejection_response, resolve_request_tenant},
};

/// Readiness paths exempt from memory-pressure shedding and from tenant
/// resolution. `/ready` must keep answering while the node sheds so the kubelet
/// does not kill the pod exactly when it is recovering — and must keep
/// answering under a `multi` policy, which no probe carries a tenant for.
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
pub fn routes(state: LokiState, runtime: &QueryRuntime) -> Router {
    let query_timeout = Duration::from_secs(state.engine.config().max_query_duration_secs);
    let tenant_policy = RequestTenantPolicy::new(
        runtime.tenant_resolver.clone(),
        QueryTenantRejectionRecorder::new(std::sync::Arc::clone(&runtime.metrics)),
        PROTOCOL_LOKI,
        LOKI_SHED_BYPASS,
    );
    let pressure = runtime.pressure.clone();
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
        // Directly under the shed layer: a request carrying no usable tenant is
        // refused before it takes any of the query budget, while a node under
        // memory pressure still sheds first — one atomic read against a header
        // read and an allocation. Flight SQL nests its two interceptors in the
        // same order, so both surfaces answer the same pair of conditions the
        // same way.
        .layer(axum::middleware::from_fn(move |req, next| {
            resolve_request_tenant(tenant_policy.clone(), build_rejection_response::<LokiError>, req, next)
        }))
        // Outermost layer: shed new requests before any handler work while the
        // process is under memory pressure (health probes bypass).
        .layer(axum::middleware::from_fn(move |req, next| {
            shed_when_pressured(
                ShedPolicy::new(pressure.clone(), PROTOCOL_LOKI, LOKI_SHED_BYPASS, None),
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
        http::{HeaderValue, Request, StatusCode},
    };
    use icegate_common::{
        CatalogBackend, CatalogConfig, DEFAULT_TENANT_ID, IoHandle, MemoryPressure, TENANT_ID_HEADER, TenantId,
        TenantPolicy, TenantResolver, catalog::CatalogBuilder,
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

    /// The runtime the router is built from. `tenant_resolver` defaults to the
    /// deployment's own tenant, which is what every case that is not about
    /// tenancy needs: under it a request with no header resolves and reaches
    /// the handler, exactly as it did before the policy existed.
    fn build_runtime(state: &LokiState, pressure: MemoryPressure, tenant_resolver: TenantResolver) -> QueryRuntime {
        QueryRuntime {
            engine: Arc::clone(&state.engine),
            metrics: Arc::clone(&state.metrics),
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
        let response = app.oneshot(get_request("/loki/api/v1/query")).await.expect("response");
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

    /// The layer counts what it refused, under this surface's own protocol
    /// label. Driven through the router rather than against the recorder,
    /// because what could break is the constant the router hands the layer.
    #[tokio::test]
    async fn a_refused_request_counts_a_rejection_for_this_protocol() {
        let (provider, exporter) = build_meter_provider();
        let (state, _warehouse) = build_state().await;
        let runtime = QueryRuntime {
            engine: Arc::clone(&state.engine),
            metrics: Arc::new(QueryMetrics::new(&provider.meter("loki_tenant_rejections"))),
            pressure: MemoryPressure::inert(),
            tenant_resolver: TenantResolver::Multi,
        };
        let app = routes(state, &runtime);

        let response = app.oneshot(get_request("/loki/api/v1/labels")).await.expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        // The Loki error kind, not only the status: `planning_error` also
        // answers `400`, and a client tells the two apart by this field.
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("a JSON error body");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "bad_data");

        provider.force_flush().expect("failed to flush metrics");
        assert_eq!(
            find_counter_total(
                &exporter,
                "icegate_query_tenant_rejections",
                &[("protocol", "loki"), ("reason", "missing")],
            ),
            1
        );
    }

    /// The two header shapes on which reading the first value, with an
    /// unreadable one read as none, differs from `TenantHeader::from_header_map`:
    /// a duplicate would pass as one tenant, and an unreadable value would pass
    /// as no header — which `single` serves as its own tenant. One router is
    /// enough, the layer being shared with Tempo.
    /// An admitted request would reach the unimplemented `/query` and answer
    /// `501`, so `400` can only be the layer's refusal.
    #[tokio::test]
    async fn a_duplicated_or_unreadable_tenant_header_is_refused_and_counted() {
        let single_acme = TenantPolicy::Single { id: "acme".to_string() }
            .into_resolver()
            .expect("valid single policy");
        let cases: [(&str, TenantResolver, Vec<HeaderValue>, &str); 2] = [
            (
                "a duplicated header in multi",
                TenantResolver::Multi,
                vec![HeaderValue::from_static("acme"), HeaderValue::from_static("acme")],
                "duplicate_header",
            ),
            (
                "an unreadable header in single",
                single_acme,
                vec![HeaderValue::from_bytes(&[0xff, 0xfe]).expect("opaque header value")],
                "invalid",
            ),
        ];

        for (case, tenant_resolver, header_values, reason) in cases {
            let (provider, exporter) = build_meter_provider();
            let (state, _warehouse) = build_state().await;
            let runtime = QueryRuntime {
                engine: Arc::clone(&state.engine),
                metrics: Arc::new(QueryMetrics::new(&provider.meter("loki_tenant_header_anomalies"))),
                pressure: MemoryPressure::inert(),
                tenant_resolver,
            };
            let mut builder = Request::builder()
                .method("GET")
                .uri("/loki/api/v1/query?query=%7Bservice_name%3D%22svc%22%7D");
            for value in header_values {
                // `header` appends, so two calls carry the header twice.
                builder = builder.header(TENANT_ID_HEADER, value);
            }
            let request = builder.body(Body::empty()).expect("request");

            let response = routes(state, &runtime).oneshot(request).await.expect("response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{case} must be refused");

            provider.force_flush().expect("failed to flush metrics");
            assert_eq!(
                find_counter_total(
                    &exporter,
                    "icegate_query_tenant_rejections",
                    &[("protocol", "loki"), ("reason", reason)],
                ),
                1,
                "{case} must be counted once as {reason}"
            );
        }
    }

    /// The probe is the one path that must answer under `multi`, where nothing
    /// carries a tenant: a `400` here is a kubelet restarting a healthy pod.
    #[tokio::test]
    async fn multi_leaves_the_readiness_path_alone() {
        let (state, _warehouse) = build_state().await;
        let runtime = build_runtime(&state, MemoryPressure::inert(), TenantResolver::Multi);
        let app = routes(state, &runtime);
        let response = app.oneshot(get_request("/ready")).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// A named tenant passes the layer and reaches the handler. `/query` is the
    /// unimplemented endpoint, so its `501` is the cheapest proof the request
    /// got past the tenant layer rather than being refused by it — and it is a
    /// different status from the `400` the layer answers with, which is what
    /// makes the two cases distinguishable. The `query` parameter is present
    /// because `RangeQueryParams` requires it, and its absence would itself
    /// answer `400`.
    #[tokio::test]
    async fn multi_admits_a_request_naming_a_tenant() {
        let (state, _warehouse) = build_state().await;
        let runtime = build_runtime(&state, MemoryPressure::inert(), TenantResolver::Multi);
        let app = routes(state, &runtime);
        let request = Request::builder()
            .method("GET")
            .uri("/loki/api/v1/query?query=%7Bservice_name%3D%22svc%22%7D")
            .header(TENANT_ID_HEADER, "acme")
            .body(Body::empty())
            .expect("request");
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }

    /// Under memory pressure the shed layer answers first, whatever the tenant
    /// policy would have said: it is the outer of the two, and Flight SQL nests
    /// its interceptors the same way round.
    #[tokio::test]
    async fn a_request_without_a_tenant_is_shed_before_the_tenant_layer() {
        let (state, _warehouse) = build_state().await;
        let runtime = build_runtime(&state, build_pressured_memory(), TenantResolver::Multi);
        let app = routes(state, &runtime);
        let response = app.oneshot(get_request("/loki/api/v1/labels")).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
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
        let runtime = build_runtime(&state, MemoryPressure::inert(), single_default_tenant());
        let (base_url, server) = crate::test_support::serve_router(routes(state, &runtime)).await;

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
