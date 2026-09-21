//! Tenant resolution for the OTLP/HTTP surface.
//!
//! Runs as a middleware layer rather than inside the handlers so a request that
//! carries no usable tenant is refused before decompression, body decode, and
//! transform — none of that work is done for data that will not be written.

use std::sync::Arc;

use axum::{
    Json,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use icegate_common::{TenantHeader, TenantRejection, TenantResolver, drain_request_body};

use super::{
    handlers::{PROTOCOL_HTTP, SIGNAL_LOGS, SIGNAL_METRICS, SIGNAL_TRACES},
    models::{ErrorResponse, ErrorType},
};
use crate::infra::metrics::OtlpMetrics;

/// Signal label for a request path this surface does not serve.
const SIGNAL_UNKNOWN: &str = "unknown";

/// Resolve the tenant of one request, or refuse the request.
///
/// On success the resolved `TenantId` is placed in the request extensions, where
/// the handlers read it through their `Extension` extractor. On refusal the body
/// is drained through [`drain_request_body`] before the `400` is written, bounded
/// by `drain_limit_bytes`: this layer runs above the surface's body limit, so
/// nothing else caps what a refused request can make it read.
pub(super) async fn resolve_request_tenant(
    resolver: TenantResolver,
    metrics: Arc<OtlpMetrics>,
    drain_limit_bytes: usize,
    mut request: Request,
    next: Next,
) -> Response {
    let outcome = resolver.resolve_tenant(TenantHeader::from_header_map(request.headers()));

    let rejection = match outcome {
        Ok(tenant) => {
            request.extensions_mut().insert(tenant);
            return next.run(request).await;
        }
        Err(rejection) => rejection,
    };

    metrics.add_tenant_rejection(
        PROTOCOL_HTTP,
        signal_kind_from_path(request.uri().path()),
        rejection.reason(),
    );
    drain_request_body(request.into_body(), drain_limit_bytes).await;

    reject_request(rejection)
}

/// The signal a request path belongs to, for the rejection counter's label.
fn signal_kind_from_path(path: &str) -> &'static str {
    match path {
        "/v1/logs" => SIGNAL_LOGS,
        "/v1/traces" => SIGNAL_TRACES,
        "/v1/metrics" => SIGNAL_METRICS,
        _ => SIGNAL_UNKNOWN,
    }
}

/// The `400` a request carrying no usable tenant is refused with.
///
/// `bad_data` rather than `internal`: the request is malformed as far as this
/// deployment is concerned, and a stock OTLP collector treats a 4xx as permanent
/// and drops the batch instead of retrying a request that can never succeed.
fn reject_request(rejection: TenantRejection) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse::new(ErrorType::BadData, rejection.message())),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_labels_follow_the_otlp_paths() {
        assert_eq!(signal_kind_from_path("/v1/logs"), SIGNAL_LOGS);
        assert_eq!(signal_kind_from_path("/v1/traces"), SIGNAL_TRACES);
        assert_eq!(signal_kind_from_path("/v1/metrics"), SIGNAL_METRICS);
        assert_eq!(signal_kind_from_path("/v1/anything-else"), SIGNAL_UNKNOWN);
    }
}
