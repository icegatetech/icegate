//! The per-request context every `OpenSearch` evaluator shares.

use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use chrono::{DateTime, Utc};
use icegate_common::TenantId;

use super::error::OpenSearchError;

/// Whose data a request reads, and the instant it is evaluated at.
///
/// `now` is read once, when the request is extracted, so every `now` the request evaluates
/// (date math in several clauses, in the query and later in its aggregations) names the same
/// instant.
#[derive(Debug, Clone)]
pub(crate) struct RequestContext {
    /// The tenant the tenant middleware resolved.
    pub(crate) tenant: TenantId,
    /// The request's instant.
    pub(crate) now: DateTime<Utc>,
}

impl<S: Send + Sync> FromRequestParts<S> for RequestContext {
    type Rejection = OpenSearchError;

    fn from_request_parts(parts: &mut Parts, _state: &S) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        // The tenant middleware runs before every route and either inserts the tenant or answers
        // the refusal itself, so a missing tenant here is a wiring fault, not a client error.
        let tenant = parts
            .extensions
            .get::<TenantId>()
            .cloned()
            .ok_or_else(|| OpenSearchError::internal("the request's tenant was not resolved"));
        // Reading the extensions needs no I/O, so the future is ready at once.
        std::future::ready(tenant.map(|tenant| Self {
            tenant,
            now: Utc::now(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use axum::http::{Request, StatusCode};

    use super::*;
    use crate::opensearch::error::render_envelope;

    #[tokio::test]
    async fn the_context_carries_the_tenant_the_middleware_resolved() {
        let (mut parts, ()) = Request::new(()).into_parts();
        let tenant = TenantId::new("tenant-a").expect("a valid tenant");
        parts.extensions.insert(tenant.clone());

        let context = RequestContext::from_request_parts(&mut parts, &()).await.expect("the context");

        assert_eq!(context.tenant, tenant);
    }

    #[tokio::test]
    async fn a_request_without_a_resolved_tenant_is_an_internal_error() {
        let (mut parts, ()) = Request::new(()).into_parts();

        let error = RequestContext::from_request_parts(&mut parts, &()).await.expect_err("refused");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["error"]["type"], "exception");
    }
}
