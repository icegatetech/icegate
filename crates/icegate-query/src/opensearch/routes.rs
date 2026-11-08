//! The `OpenSearch` router and its middleware.
//!
//! Layers, outermost first: memory-pressure shed → tenant resolution → deadline → request
//! metrics → body limit → `filter_path` → handler. Shed, tenant and deadline come in the order of
//! the Loki and Tempo routers, and each answers in the `OpenSearch` envelope. There are no bypass
//! paths: every request carries the tenant header, and the kubelet probes the operational
//! listener instead.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{DefaultBodyLimit, MatchedPath, Request};
use axum::http::{Method, Uri};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Extension, Router};
use icegate_common::{ShedPolicy, shed_when_pressured};

use super::cluster::{cat, datasources, health, info, resolve, settings};
use super::config::OpenSearchConfig;
use super::error::OpenSearchError;
use super::fields;
use super::filter_path::apply_filter_path;
use super::ppl;
use super::search;
use super::state::OpenSearchState;
use super::system_index;
use super::system_index::scroll::{clear_scroll, continue_scroll};
use super::system_index::{
    apply_aliases, bulk, create_system_index, delete_index_template, describe_system_index, documents, get_alias,
    get_index_aliases, refresh_index,
};
use crate::infra::metrics::{PROTOCOL_OPENSEARCH, QueryMetrics, QueryRequestRecorder, QueryTenantRejectionRecorder};
use crate::infra::runtime::QueryRuntime;
use crate::infra::tenant::{RequestTenantPolicy, build_rejection_response, resolve_request_tenant};

/// Paths exempt from shedding and from tenant resolution: none, because no probe addresses this
/// listener.
const OPENSEARCH_BYPASS_PATHS: &[&str] = &[];

/// The `endpoint` label of a request no route matched: one fixed value, so a client probing
/// arbitrary paths cannot grow the label set.
const UNMATCHED_ENDPOINT: &str = "unmatched";

/// Build the router serving `state` under the middleware stack of the module doc.
pub(super) fn build_router(state: OpenSearchState, runtime: &QueryRuntime, config: &OpenSearchConfig) -> Router {
    let tenant_policy = RequestTenantPolicy::new(
        runtime.tenant_resolver.clone(),
        QueryTenantRejectionRecorder::new(Arc::clone(&runtime.metrics)),
        PROTOCOL_OPENSEARCH,
        OPENSEARCH_BYPASS_PATHS,
        Some(config.max_request_body_bytes),
    );
    let shed_policy = ShedPolicy::new(
        runtime.pressure.clone(),
        PROTOCOL_OPENSEARCH,
        OPENSEARCH_BYPASS_PATHS,
        Some(config.max_request_body_bytes),
    );
    let deadline = Duration::from_secs(runtime.engine.config().max_query_duration_secs);
    let metrics = Arc::clone(&runtime.metrics);

    Router::new()
        .route("/", get(info::describe_cluster))
        .route("/_nodes", get(info::list_nodes))
        .route("/_cluster/health", get(health::report_cluster_health))
        .route(
            "/_plugins/_query/_datasources",
            get(datasources::list_query_datasources),
        )
        .route("/_cat/indices", get(cat::list_cat_indices))
        // The trailing-slash form names no index and answers like `/_cat/indices`. It is routed
        // here alone: no router-wide trailing-slash rule makes any other route answer one.
        .route("/_cat/indices/", get(cat::list_cat_indices))
        .route("/_cat/indices/{index}", get(cat::list_cat_indices))
        .route("/_cat/plugins", get(cat::list_cat_plugins))
        .route("/_resolve/index/{expression}", get(resolve::resolve_index))
        .route("/{index}/_settings/{setting}", get(settings::read_index_setting))
        .route(
            "/_field_caps",
            get(fields::describe_field_capabilities).post(fields::describe_field_capabilities),
        )
        .route(
            "/{index}/_field_caps",
            get(fields::describe_field_capabilities).post(fields::describe_field_capabilities),
        )
        .route("/_mapping", get(fields::describe_mappings))
        .route("/{index}/_mapping", get(fields::describe_mappings))
        .route("/{index}/_mapping/field/{fields}", get(fields::describe_field_mappings))
        .route("/{index}", get(describe_system_index).put(create_system_index))
        .route("/_alias/{name}", get(get_alias))
        .route("/{index}/_alias", get(get_index_aliases))
        .route("/_aliases", post(apply_aliases))
        .route("/{index}/_refresh", get(refresh_index).post(refresh_index))
        .route("/{index}/_create/{id}", put(documents::create_document))
        .route(
            "/{index}/_doc/{id}",
            put(documents::index_document)
                .post(documents::index_document)
                .get(documents::get_document)
                .delete(documents::delete_document),
        )
        .route("/{index}/_update/{id}", post(documents::update_document))
        .route("/_bulk", post(bulk::write_documents).put(bulk::write_documents))
        .route("/{index}/_bulk", post(bulk::write_documents).put(bulk::write_documents))
        .route("/_mget", get(bulk::get_documents).post(bulk::get_documents))
        .route("/{index}/_mget", get(bulk::get_documents).post(bulk::get_documents))
        .route("/_reindex", post(bulk::refuse_document_task_endpoint))
        .route("/{index}/_update_by_query", post(bulk::refuse_document_task_endpoint))
        .route("/{index}/_delete_by_query", post(bulk::refuse_document_task_endpoint))
        .route("/_tasks", get(bulk::refuse_document_task_endpoint))
        .route("/_tasks/{task_id}", get(bulk::refuse_document_task_endpoint))
        .route(
            "/{index}/_count",
            get(system_index::search::count_documents).post(system_index::search::count_documents),
        )
        .route("/_search", get(search::search_indices).post(search::search_indices))
        .route(
            "/{index}/_search",
            get(search::search_indices).post(search::search_indices),
        )
        .route("/_msearch", post(search::run_multi_search))
        .route(
            "/_plugins/_ppl",
            post(ppl::execute_ppl_query).layer(Extension(config.ppl.clone())),
        )
        .route("/_plugins/_ppl/_explain", post(ppl::explain_ppl_query))
        .route("/{index}/_msearch", post(search::run_multi_search_on_index))
        .route(
            "/_search/scroll",
            get(continue_scroll).post(continue_scroll).delete(clear_scroll),
        )
        .route(
            "/_search/scroll/{scroll_id}",
            get(continue_scroll).post(continue_scroll).delete(clear_scroll),
        )
        .route("/_template/{name}", delete(delete_index_template))
        .route("/_cat/templates/{pattern}", get(cat::list_index_templates))
        .fallback(answer_unmatched_route)
        .method_not_allowed_fallback(answer_unmatched_route)
        .layer(middleware::from_fn(apply_filter_path))
        .layer(DefaultBodyLimit::max(config.max_request_body_bytes))
        .layer(middleware::from_fn(move |request, next| {
            record_request(Arc::clone(&metrics), request, next)
        }))
        .layer(middleware::from_fn(move |request, next| {
            enforce_deadline(deadline, request, next)
        }))
        .layer(middleware::from_fn(move |request, next| {
            resolve_request_tenant(
                tenant_policy.clone(),
                build_rejection_response::<OpenSearchError>,
                request,
                next,
            )
        }))
        // Outermost: a pressured process sheds before any other work is done.
        .layer(middleware::from_fn(move |request, next| {
            shed_when_pressured(shed_policy.clone(), render_shed_envelope, request, next)
        }))
        .with_state(state)
}

/// Fallback of an unknown path and of a known path with an unserved method.
async fn answer_unmatched_route(method: Method, uri: Uri) -> OpenSearchError {
    OpenSearchError::no_handler(&method, uri.path())
}

/// Bound the whole request by the engine's query ceiling. Every response is built in full before
/// it is returned, so bounding the handler bounds the query it runs.
async fn enforce_deadline(deadline: Duration, request: Request, next: Next) -> Response {
    tokio::time::timeout(deadline, next.run(request))
        .await
        .unwrap_or_else(|_elapsed| OpenSearchError::deadline_exceeded().into_response())
}

/// Count the request and its duration under `api = opensearch` and the matched route template.
///
/// A request the deadline cuts off drops the recorder unfinished, and its `Drop` counts the
/// request as an error, so timed-out requests are counted too.
async fn record_request(metrics: Arc<QueryMetrics>, request: Request, next: Next) -> Response {
    let matched_path = request.extensions().get::<MatchedPath>().cloned();
    let endpoint = matched_path.as_ref().map_or(UNMATCHED_ENDPOINT, MatchedPath::as_str);
    let mut recorder = QueryRequestRecorder::new(&metrics, PROTOCOL_OPENSEARCH, endpoint);
    let response = next.run(request).await;
    recorder.finish(if response.status().is_success() { "ok" } else { "error" });
    response
}

/// The shed answer: the `503` envelope of [`OpenSearchError::memory_pressure`].
fn render_shed_envelope() -> Response {
    OpenSearchError::memory_pressure().into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use futures::StreamExt as _;
    use icegate_common::{MemoryPressure, TENANT_ID_HEADER, TenantResolver};
    use opentelemetry::metrics::MeterProvider as _;
    use serde_json::{Value, json};
    use tower::ServiceExt as _;

    use super::*;
    use crate::engine::QueryEngine;
    use crate::infra::metrics::test_support::{build_meter_provider, find_counter_total};
    use crate::opensearch::catalog::{CatalogSettings, IndexCatalog};
    use crate::opensearch::config::EmulatedVersion;
    use crate::opensearch::error::read_envelope;
    use crate::opensearch::node::NodeIdentity;
    use crate::test_support::{
        build_in_memory_system_index_store, build_pressured_memory, build_stalling_engine, serve_router,
    };

    /// The router over an engine whose catalog never answers: a route that answers at all read
    /// no table.
    fn build_test_router(pressure: MemoryPressure, tenant_resolver: TenantResolver, metrics: QueryMetrics) -> Router {
        build_test_router_with_engine(build_stalling_engine(30), pressure, tenant_resolver, metrics)
    }

    /// The router over `engine`; the runtime and the index catalog read through the same engine,
    /// so its deadline and its catalog are the ones every layer sees.
    fn build_test_router_with_engine(
        engine: Arc<QueryEngine>,
        pressure: MemoryPressure,
        tenant_resolver: TenantResolver,
        metrics: QueryMetrics,
    ) -> Router {
        let config = OpenSearchConfig {
            enabled: true,
            ..OpenSearchConfig::default()
        };
        let version = EmulatedVersion::parse(&config.version).expect("the default version parses");
        let runtime = QueryRuntime {
            engine: Arc::clone(&engine),
            metrics: Arc::new(metrics),
            pressure,
            tenant_resolver,
        };
        let system_store = build_in_memory_system_index_store();
        let state = OpenSearchState {
            node: Arc::new(NodeIdentity::new(
                &config.cluster_name,
                version,
                "127.0.0.1:9200".parse().expect("socket address"),
            )),
            catalog: IndexCatalog::new(
                engine,
                Arc::clone(&runtime.metrics),
                Arc::clone(&system_store),
                CatalogSettings {
                    field_discovery_window: Duration::from_secs(60),
                    max_query_memory_bytes: 1_073_741_824,
                },
            ),
            system_store,
            max_answer_bytes: config.system_indices.max_answer_bytes,
            search_limits: config.search,
        };
        build_router(state, &runtime, &config)
    }

    fn build_request(method: &str, uri: &str, tenant: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(tenant) = tenant {
            builder = builder.header(TENANT_ID_HEADER, tenant);
        }
        builder.body(Body::empty()).expect("request")
    }

    /// A `POST /_bulk` whose streamed body is `chunk_count` equal chunks, and the number of those
    /// chunks the server has read so far: a layer that answers without draining reads none.
    fn build_counted_bulk_request(tenant: Option<&str>, chunk_count: usize) -> (Request<Body>, Arc<AtomicUsize>) {
        let consumed_chunk_count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&consumed_chunk_count);
        let chunks = futures::stream::iter(0..chunk_count).map(move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::io::Error>(vec![b'x'; 1024])
        });
        let request = build_request("POST", "/_bulk", tenant).map(|_| Body::from_stream(chunks));
        (request, consumed_chunk_count)
    }

    /// The shed answers the 503 envelope and is counted on `icegate_memory_shed_requests` under
    /// the listener's own surface. A shed `_bulk` body is drained first, so a client still sending
    /// it reads the 503 instead of a connection reset.
    ///
    /// The counter is built from the process-global meter provider when a pressure handle is
    /// created, so the test installs its provider before building the handle, and both shed
    /// requests go through this one test. Every other test of this binary that sheds does so
    /// through another listener's router (another surface), so the `opensearch` surface counts
    /// exactly these two requests.
    #[tokio::test]
    async fn a_pressured_process_sheds_in_the_503_envelope_with_retry_after() {
        let (provider, exporter) = build_meter_provider();
        opentelemetry::global::set_meter_provider(provider.clone());
        let app = build_test_router(
            build_pressured_memory(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );
        let (bulk_request, consumed_chunk_count) = build_counted_bulk_request(Some("tenant-a"), 4);

        let root_response = app
            .clone()
            .oneshot(build_request("GET", "/", Some("tenant-a")))
            .await
            .expect("response");
        let bulk_response = app.oneshot(bulk_request).await.expect("response");

        assert!(root_response.headers().contains_key(header::RETRY_AFTER));
        let (root_status, root_body) = read_envelope(root_response).await;
        assert_eq!(root_status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(root_body["status"], 503);
        assert_eq!(root_body["error"]["type"], "memory_pressure_exception");
        let (bulk_status, bulk_body) = read_envelope(bulk_response).await;
        assert_eq!(bulk_status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(bulk_body["error"]["type"], "memory_pressure_exception");
        assert_eq!(consumed_chunk_count.load(Ordering::SeqCst), 4);
        provider.force_flush().expect("flush metrics");
        assert_eq!(
            find_counter_total(&exporter, "icegate_memory_shed_requests", &[("surface", "opensearch")]),
            2
        );
    }

    #[tokio::test]
    async fn a_request_without_a_tenant_under_multi_is_refused_and_counted_as_opensearch() {
        let (provider, exporter) = build_meter_provider();
        let metrics = QueryMetrics::new(&provider.meter("opensearch_tenant_rejections"));
        let app = build_test_router(MemoryPressure::inert(), TenantResolver::Multi, metrics);

        let response = app.oneshot(build_request("GET", "/", None)).await.expect("response");

        let (status, body) = read_envelope(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        provider.force_flush().expect("flush metrics");
        assert_eq!(
            find_counter_total(
                &exporter,
                "icegate_query_tenant_rejections",
                &[("protocol", "opensearch"), ("reason", "missing")]
            ),
            1
        );
    }

    /// A refused `_bulk` must reach the client as the refusal, not as a connection reset, so the
    /// body is drained first. Sent over real TCP: an in-process `oneshot` shows no reset.
    #[tokio::test]
    async fn a_refused_request_with_a_large_body_still_reads_the_refusal() {
        let app = build_test_router(
            MemoryPressure::inert(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );
        let (base_url, server) = serve_router(app).await;

        let response = reqwest::Client::new()
            .post(format!("{base_url}/_bulk"))
            .body(vec![b'x'; 4 * 1024 * 1024])
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .expect("the refusal arrives instead of a reset");

        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: Value = response.json().await.expect("JSON body");
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        server.abort();
    }

    /// Whether a reset reaches the client over TCP depends on the platform's timing, so the drain
    /// itself is pinned here, by the chunks the server read.
    #[tokio::test]
    async fn a_refused_request_body_is_drained_before_the_refusal_is_answered() {
        let app = build_test_router(
            MemoryPressure::inert(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );
        let (request, consumed_chunk_count) = build_counted_bulk_request(None, 4);

        let response = app.oneshot(request).await.expect("response");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(consumed_chunk_count.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn an_unmatched_route_or_method_is_a_400_envelope() {
        for (method, uri) in [("GET", "/_no_such_api/x"), ("DELETE", "/"), ("POST", "/")] {
            let app = build_test_router(
                MemoryPressure::inert(),
                TenantResolver::Multi,
                QueryMetrics::new_disabled(),
            );

            let response = app
                .oneshot(build_request(method, uri, Some("tenant-a")))
                .await
                .expect("response");

            let (status, body) = read_envelope(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {uri}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{method} {uri}");
        }
    }

    /// The paused clock advances to the deadline as soon as the handler is idle, so the case costs
    /// no wall-clock time and cannot race.
    #[tokio::test(start_paused = true)]
    async fn a_handler_outliving_the_deadline_is_a_503_envelope() {
        let deadline = Duration::from_secs(30);
        let app = Router::new()
            .route("/slow", get(std::future::pending::<&'static str>))
            .layer(middleware::from_fn(move |request, next| {
                enforce_deadline(deadline, request, next)
            }));

        let response = app.oneshot(build_request("GET", "/slow", None)).await.expect("response");

        assert!(response.headers().contains_key(header::RETRY_AFTER));
        let (status, body) = read_envelope(response).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["type"], "query_deadline_exceeded_exception");
    }

    /// The label set is what the dashboards are keyed on: `api`, the route template (never the
    /// raw path, whose values a client controls) and `ok` / `error`.
    #[tokio::test]
    async fn requests_are_counted_under_the_opensearch_api_label() {
        let (provider, exporter) = build_meter_provider();
        let metrics = QueryMetrics::new(&provider.meter("opensearch_requests"));
        let app = build_test_router(MemoryPressure::inert(), TenantResolver::Multi, metrics);

        let root_response = app
            .clone()
            .oneshot(build_request("GET", "/", Some("tenant-a")))
            .await
            .expect("response");
        let unmatched_response = app
            .oneshot(build_request("GET", "/_no_such_api/x", Some("tenant-a")))
            .await
            .expect("response");

        assert_eq!(root_response.status(), StatusCode::OK);
        assert_eq!(unmatched_response.status(), StatusCode::BAD_REQUEST);
        provider.force_flush().expect("flush metrics");
        assert_eq!(
            find_counter_total(
                &exporter,
                "icegate_query_requests",
                &[("api", "opensearch"), ("endpoint", "/"), ("status", "ok")]
            ),
            1
        );
        assert_eq!(
            find_counter_total(
                &exporter,
                "icegate_query_requests",
                &[("api", "opensearch"), ("endpoint", "unmatched"), ("status", "error")]
            ),
            1
        );
    }

    /// The router's catalog never answers, so an answer within the bound proves the poll reads no
    /// table. The node id is `derive_identifier("node", "icegate")`, computed independently in
    /// `node.rs`'s tests.
    #[tokio::test]
    async fn nodes_answers_without_a_catalog_and_honours_filter_path() {
        let app = build_test_router(
            MemoryPressure::inert(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );
        let uri = "/_nodes?filter_path=nodes.*.version%2Cnodes.*.http.publish_address%2Cnodes.*.ip";

        let response = tokio::time::timeout(
            Duration::from_secs(5),
            app.oneshot(build_request("GET", uri, Some("tenant-a"))),
        )
        .await
        .expect("answered without waiting on the catalog")
        .expect("response");

        let (status, body) = read_envelope(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({"nodes": {"Qd9sEClXGyasivmx7Po0bQ": {
                "version": "3.9.0",
                "ip": "127.0.0.1",
                "http": {"publish_address": "127.0.0.1:9200"}
            }}})
        );
    }

    #[tokio::test]
    async fn query_datasources_answers_an_empty_list_without_reading_a_table() {
        let app = build_test_router(
            MemoryPressure::inert(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );

        let response = tokio::time::timeout(
            Duration::from_secs(5),
            app.oneshot(build_request("GET", "/_plugins/_query/_datasources", Some("tenant-a"))),
        )
        .await
        .expect("answered without waiting on the catalog")
        .expect("response");

        let (status, body) = read_envelope(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!([]));
    }

    /// The health poll is answered from the node identity alone; the catalog that never answers
    /// proves it, as for the other polls.
    #[tokio::test]
    async fn cluster_health_answers_green_without_reading_a_table() {
        let app = build_test_router(
            MemoryPressure::inert(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );

        let response = tokio::time::timeout(
            Duration::from_secs(5),
            app.oneshot(build_request("GET", "/_cluster/health", Some("tenant-a"))),
        )
        .await
        .expect("answered without waiting on the catalog")
        .expect("response");

        let (status, body) = read_envelope(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "green");
    }

    /// A refusal under `filter_path` is still the whole envelope: one refused by the handler's
    /// parameters, one refused by the fallback.
    #[tokio::test]
    async fn an_error_answer_keeps_its_envelope_under_filter_path() {
        for uri in [
            "/_nodes?filter_path=nodes.*.version&unknown_knob=1",
            "/_no_such_api?filter_path=nodes",
        ] {
            let app = build_test_router(
                MemoryPressure::inert(),
                TenantResolver::Multi,
                QueryMetrics::new_disabled(),
            );

            let response = app
                .oneshot(build_request("GET", uri, Some("tenant-a")))
                .await
                .expect("response");

            let (status, body) = read_envelope(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
            assert_eq!(body["status"], 400, "{uri}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{uri}");
            assert_eq!(
                body["error"]["root_cause"][0]["type"], "illegal_argument_exception",
                "{uri}"
            );
        }
    }

    #[tokio::test]
    async fn an_unparsable_filter_path_is_a_400_envelope() {
        let app = build_test_router(
            MemoryPressure::inert(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );

        let response = app
            .oneshot(build_request(
                "GET",
                "/_nodes?filter_path=nodes..version",
                Some("tenant-a"),
            ))
            .await
            .expect("response");

        let (status, body) = read_envelope(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
    }

    /// Repeating `filter_path` adds paths: the values are applied as one comma list.
    #[tokio::test]
    async fn a_repeated_filter_path_keeps_the_paths_of_every_value() {
        let app = build_test_router(
            MemoryPressure::inert(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );

        let response = app
            .oneshot(build_request(
                "GET",
                "/_nodes?filter_path=nodes.*.version&filter_path=nodes.*.ip",
                Some("tenant-a"),
            ))
            .await
            .expect("response");

        let (status, body) = read_envelope(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({"nodes": {"Qd9sEClXGyasivmx7Po0bQ": {"version": "3.9.0", "ip": "127.0.0.1"}}})
        );
    }

    /// A `HEAD` request under `filter_path` answers like the filtered `GET`: no body, and the
    /// `Content-Length` of the filtered body, `{}` under a filter that matches nothing. A filter
    /// that does not parse is refused with `400` as on `GET`.
    #[tokio::test]
    async fn a_head_request_under_filter_path_answers_like_the_filtered_get() {
        let app = build_test_router(
            MemoryPressure::inert(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );

        let filtered_response = app
            .clone()
            .oneshot(build_request("HEAD", "/?filter_path=x", Some("tenant-a")))
            .await
            .expect("response");
        let refused_response = app
            .oneshot(build_request("HEAD", "/?filter_path=a..b", Some("tenant-a")))
            .await
            .expect("response");

        assert_eq!(filtered_response.status(), StatusCode::OK);
        assert_eq!(
            filtered_response
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some("2")
        );
        let filtered_body = axum::body::to_bytes(filtered_response.into_body(), usize::MAX)
            .await
            .expect("HEAD body");
        assert!(filtered_body.is_empty());
        assert_eq!(refused_response.status(), StatusCode::BAD_REQUEST);
    }

    /// A catalog that accepted the request and went silent is cut off at the engine's query
    /// ceiling, and the client reads the 503 envelope instead of waiting. The paused clock
    /// advances to the one-second ceiling as soon as nothing else can run.
    #[tokio::test(start_paused = true)]
    async fn a_catalog_that_never_answers_is_cut_off_at_the_deadline_in_the_envelope() {
        let app = build_test_router_with_engine(
            build_stalling_engine(1),
            MemoryPressure::inert(),
            TenantResolver::Multi,
            QueryMetrics::new_disabled(),
        );

        let response = tokio::time::timeout(
            Duration::from_secs(10),
            app.oneshot(build_request("GET", "/_cat/indices", Some("tenant-a"))),
        )
        .await
        .expect("answered within the bound")
        .expect("a response");

        let (status, body) = read_envelope(response).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["type"], "query_deadline_exceeded_exception");
    }
}
