//! Axum HTTP adapter for the memory-pressure guard.
//!
//! Provides [`ShedPolicy`], the [`shed_when_pressured`] middleware, and the
//! [`default_shed_response`] builder used by the Loki/Tempo/Prometheus query
//! surfaces and the OTLP/HTTP ingest surface to reject new requests with a
//! `503 Service Unavailable` while the process is under memory pressure.

use axum::{
    Json,
    extract::Request,
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures::StreamExt;

use super::guard::MemoryPressure;

/// Recommended `Retry-After` value (seconds) sent on a memory-pressure 503.
///
/// Single source of truth for the wire value so every surface's response builder
/// derives the header from it rather than hardcoding a literal that could drift.
pub const SHED_RETRY_AFTER_SECS: u32 = 1;

/// The `Retry-After` header pair built from [`SHED_RETRY_AFTER_SECS`].
///
/// `u32 -> HeaderValue` is infallible, so this cannot fail.
fn shed_retry_after() -> (header::HeaderName, HeaderValue) {
    (header::RETRY_AFTER, HeaderValue::from(SHED_RETRY_AFTER_SECS))
}

/// Per-surface shedding policy captured by the `from_fn` middleware closure.
///
/// `bypass_paths` MUST list every health/readiness probe path for the surface: a
/// probe that received a 503 while shedding would make the kubelet kill the pod
/// exactly when it is trying to recover. `drain_body` is `true` only for large-body
/// surfaces (OTLP/HTTP) that must drain in constant memory so the client reads the
/// 503 instead of a connection reset (GH-158).
#[derive(Clone)]
pub struct ShedPolicy {
    guard: MemoryPressure,
    surface: &'static str,
    bypass_paths: &'static [&'static str],
    drain_body: bool,
}

impl ShedPolicy {
    /// Build a policy for one surface. `surface` is the metric attribute value
    /// (`"loki" | "tempo" | "prometheus" | "otlp_http"`).
    #[must_use]
    pub const fn new(
        guard: MemoryPressure,
        surface: &'static str,
        bypass_paths: &'static [&'static str],
        drain_body: bool,
    ) -> Self {
        Self {
            guard,
            surface,
            bypass_paths,
            drain_body,
        }
    }
}

/// Shed the request with `build_response` when the guard reports pressure, unless the
/// exact request path is a bypass path.
///
/// `build_response` is a plain `fn` pointer so each surface returns a body matching its
/// own error shape while the bypass/pressure/metric/drain logic stays shared. When
/// `drain_body` is set, the request body is drained in constant memory before the 503
/// is returned so the client reads the status rather than seeing a reset; the expensive
/// decompress/decode/transform work is still skipped.
pub async fn shed_when_pressured(
    policy: ShedPolicy,
    build_response: fn() -> Response,
    request: Request,
    next: Next,
) -> Response {
    if policy.bypass_paths.contains(&request.uri().path()) || !policy.guard.is_under_pressure() {
        return next.run(request).await;
    }
    policy.guard.record_shed(policy.surface);
    if policy.drain_body {
        // Discard each chunk so memory stays bounded regardless of body size; stop on
        // the first read error (the client hung up mid-upload).
        let mut body = request.into_body().into_data_stream();
        while let Some(chunk) = body.next().await {
            if chunk.is_err() {
                break;
            }
        }
    }
    build_response()
}

/// Generic JSON `503` shared by the query HTTP surfaces (Loki/Tempo/Prometheus).
#[must_use]
pub fn default_shed_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [shed_retry_after()],
        Json(serde_json::json!({
            "status": "error",
            "error": "service under memory pressure, retry later"
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode, header},
        routing::{get, post},
    };
    use tower::ServiceExt;

    use super::*;
    use crate::memory::guard::{MemoryPressure, MemoryPressureConfig, MemoryPressureSampler, UsageReader};

    /// Deterministic `UsageReader` returning a fixed working-set / limit. Defined
    /// locally because a peer module's `#[cfg(test)]` doubles are not importable.
    struct FixedReader {
        limit: u64,
        working_set: u64,
    }

    impl UsageReader for FixedReader {
        fn limit_bytes(&self) -> u64 {
            self.limit
        }

        fn read_working_set_bytes(&self) -> crate::error::Result<u64> {
            Ok(self.working_set)
        }
    }

    /// A `MemoryPressure` handle deterministically driven into the pressured state
    /// (ratio 0.99 >= default high-watermark 0.90) via one `sample_once`.
    fn pressured_handle() -> MemoryPressure {
        let cfg = MemoryPressureConfig::default();
        let sampler = MemoryPressureSampler::with_reader(
            &cfg,
            Arc::new(FixedReader {
                limit: 100,
                working_set: 99,
            }),
        );
        let handle = sampler.handle();
        sampler.sample_once().expect("sample_once must succeed for a fixed reader");
        assert!(handle.is_under_pressure(), "fixture must be under pressure");
        handle
    }

    /// Router with a shed layer over `/work` (guarded) and `/ready` (bypass).
    fn guarded_router(policy: ShedPolicy) -> Router {
        Router::new()
            .route("/work", get(|| async { StatusCode::OK }))
            .route("/ready", get(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn(move |req, next| {
                shed_when_pressured(policy.clone(), default_shed_response, req, next)
            }))
    }

    #[tokio::test]
    async fn pressured_work_path_sheds_503_with_retry_after() {
        let policy = ShedPolicy::new(pressured_handle(), "loki", &["/ready"], false);
        let response = guarded_router(policy)
            .oneshot(Request::builder().uri("/work").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().contains_key(header::RETRY_AFTER));
    }

    #[tokio::test]
    async fn pressured_bypass_path_passes_through() {
        let policy = ShedPolicy::new(pressured_handle(), "loki", &["/ready"], false);
        let response = guarded_router(policy)
            .oneshot(Request::builder().uri("/ready").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn inert_guard_passes_through() {
        let policy = ShedPolicy::new(MemoryPressure::inert(), "loki", &["/ready"], false);
        let response = guarded_router(policy)
            .oneshot(Request::builder().uri("/work").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn pressured_drain_body_completes_with_503() {
        // drain_body = true + an oversized body must complete cleanly with a 503
        // (body drained in constant memory), never hang or reset the connection.
        let policy = ShedPolicy::new(pressured_handle(), "otlp_http", &[], true);
        let app = Router::new()
            .route("/v1/logs", post(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn(move |req, next| {
                shed_when_pressured(policy.clone(), default_shed_response, req, next)
            }));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/logs")
                    .body(Body::from(vec![0_u8; 1024 * 1024]))
                    .expect("request"),
            )
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
