//! Router construction for the Catalog REST API.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::middleware::{Next, from_fn};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tower_http::trace::TraceLayer;

use super::config::CatalogApiConfig;
use super::error::ApiError;
use super::handlers;
use super::state::CatalogApiState;
use crate::S3Catalog;

const CONFIG_RESOURCE_PATH: &str = "/v1/config";
const NAMESPACES_RESOURCE_PATH: &str = "/v1/{prefix}/namespaces";
const NAMESPACE_RESOURCE_PATH: &str = "/v1/{prefix}/namespaces/{namespace}";
const TABLES_RESOURCE_PATH: &str = "/v1/{prefix}/namespaces/{namespace}/tables";
const TABLE_RESOURCE_PATH: &str = "/v1/{prefix}/namespaces/{namespace}/tables/{table}";

const CATALOG_ROUTE_ROOT: &str = "/v1";
const NAMESPACES_ROUTE: &str = "/namespaces";
const NAMESPACE_ROUTE: &str = "/namespaces/{namespace}";
const TABLES_ROUTE: &str = "/namespaces/{namespace}/tables";
const TABLE_ROUTE: &str = "/namespaces/{namespace}/tables/{table}";

/// One operation this server implements, named by the resource path of the
/// pinned `OpenAPI` revision rather than by the path it is routed at: a client
/// matches the advertised identifier verbatim before substituting the
/// configured catalog prefix into its own request.
struct CatalogEndpoint {
    method: &'static str,
    resource_path: &'static str,
}

const GET_CONFIG: CatalogEndpoint = CatalogEndpoint {
    method: "GET",
    resource_path: CONFIG_RESOURCE_PATH,
};
const GET_NAMESPACES: CatalogEndpoint = CatalogEndpoint {
    method: "GET",
    resource_path: NAMESPACES_RESOURCE_PATH,
};
const GET_NAMESPACE: CatalogEndpoint = CatalogEndpoint {
    method: "GET",
    resource_path: NAMESPACE_RESOURCE_PATH,
};
const HEAD_NAMESPACE: CatalogEndpoint = CatalogEndpoint {
    method: "HEAD",
    resource_path: NAMESPACE_RESOURCE_PATH,
};
const GET_TABLES: CatalogEndpoint = CatalogEndpoint {
    method: "GET",
    resource_path: TABLES_RESOURCE_PATH,
};
const GET_TABLE: CatalogEndpoint = CatalogEndpoint {
    method: "GET",
    resource_path: TABLE_RESOURCE_PATH,
};
const HEAD_TABLE: CatalogEndpoint = CatalogEndpoint {
    method: "HEAD",
    resource_path: TABLE_RESOURCE_PATH,
};

const SUPPORTED_ENDPOINTS: [CatalogEndpoint; 7] = [
    GET_CONFIG,
    GET_NAMESPACES,
    GET_NAMESPACE,
    HEAD_NAMESPACE,
    GET_TABLES,
    GET_TABLE,
    HEAD_TABLE,
];

/// Render the advertised operation list `GET /v1/config` carries.
fn supported_endpoints() -> Vec<String> {
    SUPPORTED_ENDPOINTS
        .iter()
        .map(|endpoint| format!("{} {}", endpoint.method, endpoint.resource_path))
        .collect()
}

/// Build the read-only Iceberg REST catalog router over `catalog`.
pub fn router(catalog: Arc<S3Catalog>, config: CatalogApiConfig) -> Router {
    // The prefix is a literal path, not a route parameter: the catalog resources
    // are mounted once under the configured mount point, so a namespace named
    // `namespaces` can never be mistaken for the prefix segment.
    let mount_path = config.catalog_prefix().map_or_else(
        || CATALOG_ROUTE_ROOT.to_string(),
        |prefix| format!("{CATALOG_ROUTE_ROOT}/{}", prefix.as_str()),
    );
    let catalog_router = with_iceberg_fallbacks(
        Router::new()
            .route(NAMESPACES_ROUTE, get(handlers::list_namespaces))
            .route(
                NAMESPACE_ROUTE,
                get(handlers::get_namespace).head(handlers::head_namespace),
            )
            .route(TABLES_ROUTE, get(handlers::list_tables))
            .route(TABLE_ROUTE, get(handlers::load_table).head(handlers::head_table)),
    );

    with_iceberg_fallbacks(Router::new().route(CONFIG_RESOURCE_PATH, get(handlers::get_config)))
        .nest(&mount_path, catalog_router)
        .with_state(CatalogApiState::new(catalog, config, supported_endpoints()))
        .layer(from_fn(request_timeout))
        .layer(TraceLayer::new_for_http())
}

/// Answer unrouted paths and unsupported methods with the Iceberg error
/// envelope instead of Axum's bare status responses.
///
/// `method_not_allowed_fallback` only rewrites the method routers already
/// registered on the router it is called on, so each router carries its own —
/// a nested router does not inherit it from the outer one.
fn with_iceberg_fallbacks(router: Router<CatalogApiState>) -> Router<CatalogApiState> {
    router
        .fallback(|| async { ApiError::ResourceNotFound })
        .method_not_allowed_fallback(|| async { ApiError::MethodNotAllowed })
}

async fn request_timeout(request: Request, next: Next) -> Response {
    tokio::time::timeout(Duration::from_secs(30), next.run(request))
        .await
        .unwrap_or_else(|_| ApiError::RequestTimedOut.into_response())
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use axum::body::{Body, to_bytes};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::*;

    async fn never_responds() -> Response {
        pending().await
    }

    #[tokio::test(start_paused = true)]
    async fn request_timeout_returns_iceberg_service_unavailable_error() {
        let app = Router::new()
            .route("/slow", get(never_responds))
            .layer(from_fn(request_timeout));
        let request = Request::builder().uri("/slow").body(Body::empty()).expect("request");
        let response = tokio::spawn(app.oneshot(request));

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(30)).await;
        let response = response.await.expect("request task").expect("response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.expect("response body");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).expect("Iceberg error response"),
            serde_json::json!({
                "error": {
                    "message": "Catalog request timed out",
                    "type": "ServiceUnavailableException",
                    "code": 503,
                }
            })
        );
    }
}
