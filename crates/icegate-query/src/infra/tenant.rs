//! Tenant resolution for the HTTP read surfaces (Loki, Tempo).
//!
//! Runs as a middleware layer rather than inside the handlers so a request that
//! carries no usable tenant is refused before parameter parsing, planning and
//! the catalog scan — none of that work is done for a request that will not be
//! answered.
//!
//! Shaped after [`shed_when_pressured`](icegate_common::shed_when_pressured),
//! which the same routers already carry: the policy travels as one value and
//! the surface's own body comes from a plain `fn` pointer, so the bypass,
//! resolve and count logic stays shared while each protocol answers in its own
//! error shape. A refused request is answered without draining its body, for
//! the reason [`ShedPolicy`](icegate_common::ShedPolicy) passes no drain limit
//! on these surfaces: their `POST` bodies carry a query string, not a payload.

use axum::{
    extract::Request,
    middleware::Next,
    response::{IntoResponse, Response},
};
use icegate_common::{TenantHeader, TenantRejection, TenantRejectionRecorder as _, TenantResolver};

use crate::infra::metrics::QueryTenantRejectionRecorder;

/// Per-surface tenant policy captured by the `from_fn` middleware closure.
///
/// `bypass_paths` MUST list every readiness/liveness probe path of the surface:
/// a probe answered `400` under a `multi` policy would make the kubelet kill a
/// pod that is serving correctly. It is the same list the surface excludes from
/// shedding, for the same reason.
#[derive(Clone)]
pub struct RequestTenantPolicy {
    resolver: TenantResolver,
    recorder: QueryTenantRejectionRecorder,
    protocol: &'static str,
    bypass_paths: &'static [&'static str],
}

impl RequestTenantPolicy {
    /// Build a policy for one surface. `protocol` is the metric attribute value
    /// (`PROTOCOL_LOKI` / `PROTOCOL_TEMPO` of [`crate::infra::metrics`]).
    #[must_use]
    pub const fn new(
        resolver: TenantResolver,
        recorder: QueryTenantRejectionRecorder,
        protocol: &'static str,
        bypass_paths: &'static [&'static str],
    ) -> Self {
        Self {
            resolver,
            recorder,
            protocol,
            bypass_paths,
        }
    }
}

/// Resolve the tenant of one request, or refuse the request with `build_response`.
///
/// On success the resolved [`TenantId`](icegate_common::TenantId) is placed in
/// the request extensions, where the handlers read it through their `Extension`
/// extractor. On refusal the counter of `policy.protocol` is incremented with
/// [`TenantRejection::reason`] and `build_response` renders the body.
pub async fn resolve_request_tenant(
    policy: RequestTenantPolicy,
    build_response: fn(TenantRejection) -> Response,
    mut request: Request,
    next: Next,
) -> Response {
    if policy.bypass_paths.contains(&request.uri().path()) {
        return next.run(request).await;
    }

    match policy.resolver.resolve_tenant(TenantHeader::from_header_map(request.headers())) {
        Ok(tenant) => {
            request.extensions_mut().insert(tenant);
            next.run(request).await
        }
        Err(rejection) => {
            policy.recorder.add_tenant_rejection(policy.protocol, rejection.reason());
            build_response(rejection)
        }
    }
}

/// Render `rejection` as the `400` of a surface whose error type wraps
/// [`QueryError`](crate::error::QueryError).
///
/// Both Loki and Tempo answer a malformed request `400` through
/// `QueryError::Validation`, so the conversion is stated once here and each
/// surface supplies only its own wrapper.
pub(crate) fn build_rejection_response<E>(rejection: TenantRejection) -> Response
where
    E: From<crate::error::QueryError> + IntoResponse,
{
    E::from(crate::error::QueryError::Validation(rejection.message().to_string())).into_response()
}
