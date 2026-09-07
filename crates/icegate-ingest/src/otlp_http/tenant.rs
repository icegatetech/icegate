//! Tenant resolution for the OTLP/HTTP surface.
//!
//! Runs as a middleware layer rather than inside the handlers so a request that
//! carries no usable tenant is refused before decompression, body decode, and
//! transform — none of that work is done for data that will not be written.

use std::sync::Arc;

use axum::{
    Json,
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use icegate_common::{TENANT_ID_HEADER, TenantHeader, TenantRejection, TenantResolver, drain_request_body};

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
    let outcome = resolver.resolve_tenant(read_tenant_header(request.headers()));

    let rejection = match outcome {
        Ok(tenant) => {
            request.extensions_mut().insert(tenant);
            return next.run(request).await;
        }
        Err(rejection) => rejection,
    };

    metrics.add_tenant_rejection(PROTOCOL_HTTP, signal_kind_from_path(request.uri().path()), rejection.reason());
    drain_request_body(request.into_body(), drain_limit_bytes).await;

    reject_request(rejection)
}

/// Read [`TENANT_ID_HEADER`] as the policy input.
///
/// Only the mapping from this surface's header map; what the values mean is
/// [`TenantHeader::from_values`], which the OTLP/gRPC surface reaches the same
/// way.
fn read_tenant_header(headers: &HeaderMap) -> TenantHeader<'_> {
    TenantHeader::from_values(headers.get_all(TENANT_ID_HEADER).iter().map(|value| value.to_str().ok()))
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
    use axum::http::HeaderValue;

    use super::*;

    #[test]
    fn an_absent_header_reads_as_absent() {
        assert_eq!(read_tenant_header(&HeaderMap::new()), TenantHeader::Absent);
    }

    #[test]
    fn a_single_header_reads_as_its_value() {
        let mut headers = HeaderMap::new();
        headers.insert(TENANT_ID_HEADER, HeaderValue::from_static("acme"));
        assert_eq!(read_tenant_header(&headers), TenantHeader::Once("acme"));
    }

    #[test]
    fn two_headers_read_as_duplicated_even_when_they_agree() {
        let mut headers = HeaderMap::new();
        headers.append(TENANT_ID_HEADER, HeaderValue::from_static("acme"));
        headers.append(TENANT_ID_HEADER, HeaderValue::from_static("acme"));
        assert_eq!(read_tenant_header(&headers), TenantHeader::Duplicated);
    }

    #[test]
    fn a_non_ascii_header_reads_as_an_unusable_value() {
        let mut headers = HeaderMap::new();
        headers.insert(
            TENANT_ID_HEADER,
            HeaderValue::from_bytes(&[0xff, 0xfe]).expect("byte header value"),
        );
        // Empty is rejected by `TenantId::is_valid`, so this reaches the policy
        // as a named-but-invalid tenant rather than as an absent header.
        assert_eq!(read_tenant_header(&headers), TenantHeader::Once(""));
    }

    #[test]
    fn signal_labels_follow_the_otlp_paths() {
        assert_eq!(signal_kind_from_path("/v1/logs"), SIGNAL_LOGS);
        assert_eq!(signal_kind_from_path("/v1/traces"), SIGNAL_TRACES);
        assert_eq!(signal_kind_from_path("/v1/metrics"), SIGNAL_METRICS);
        assert_eq!(signal_kind_from_path("/v1/anything-else"), SIGNAL_UNKNOWN);
    }
}
