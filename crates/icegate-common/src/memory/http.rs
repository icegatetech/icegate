//! Axum HTTP adapter for the memory-pressure guard.
//!
//! Provides [`ShedPolicy`], the [`shed_when_pressured`] middleware, and the
//! [`default_shed_response`] builder used by the Loki/Tempo/Prometheus query
//! surfaces and the OTLP/HTTP ingest surface to reject new requests with a
//! `503 Service Unavailable` while the process is under memory pressure.

use axum::{
    Json,
    body::Body,
    extract::Request,
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures::StreamExt;

use super::guard::MemoryPressure;

/// Read and discard a request body a middleware layer has decided to refuse.
///
/// A layer that answers before the body is read leaves the client writing into a
/// socket nobody reads, and the client sees a connection reset instead of the
/// status; draining first is what lets it read the answer (GH-158). Memory stays
/// bounded whatever the body's size, because each chunk is dropped as it
/// arrives.
///
/// Reading stops at the first read error — the client hung up mid-upload, and
/// there is nothing left to drain — and at `limit_bytes`, which is what bounds
/// the work one refused request can extract: these layers run above the body
/// limit of their surface, so nothing else caps them. A sender that exceeds
/// `limit_bytes` gets the reset the drain exists to avoid, which is the correct
/// trade: it is already sending more than the surface would ever accept.
pub async fn drain_request_body(body: Body, limit_bytes: usize) {
    let mut stream = body.into_data_stream();
    let mut drained_bytes = 0_usize;
    while drained_bytes < limit_bytes {
        let Some(Ok(chunk)) = stream.next().await else {
            break;
        };
        drained_bytes = drained_bytes.saturating_add(chunk.len());
    }
}

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
/// exactly when it is trying to recover. `drain_limit_bytes` is `Some` only for
/// large-body surfaces (OTLP/HTTP), which must drain before answering so the
/// client reads the 503 instead of a connection reset; the surfaces that carry
/// query strings rather than payloads pass `None` and answer immediately.
#[derive(Clone)]
pub struct ShedPolicy {
    guard: MemoryPressure,
    surface: &'static str,
    bypass_paths: &'static [&'static str],
    drain_limit_bytes: Option<usize>,
}

impl ShedPolicy {
    /// Build a policy for one surface. `surface` is the metric attribute value
    /// (`"loki" | "tempo" | "prometheus" | "otlp_http"`); `drain_limit_bytes` is
    /// the bound handed to [`drain_request_body`] when the request is shed.
    #[must_use]
    pub const fn new(
        guard: MemoryPressure,
        surface: &'static str,
        bypass_paths: &'static [&'static str],
        drain_limit_bytes: Option<usize>,
    ) -> Self {
        Self {
            guard,
            surface,
            bypass_paths,
            drain_limit_bytes,
        }
    }
}

/// Shed the request with `build_response` when the guard reports pressure, unless the
/// exact request path is a bypass path.
///
/// `build_response` is a plain `fn` pointer so each surface returns a body matching its
/// own error shape while the bypass/pressure/metric/drain logic stays shared. With
/// `drain_limit_bytes` set, the request body is drained through [`drain_request_body`]
/// before the 503 is returned; the expensive decompress/decode/transform work is still
/// skipped.
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
    if let Some(limit_bytes) = policy.drain_limit_bytes {
        drain_request_body(request.into_body(), limit_bytes).await;
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
        let policy = ShedPolicy::new(pressured_handle(), "loki", &["/ready"], None);
        let response = guarded_router(policy)
            .oneshot(Request::builder().uri("/work").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(response.headers().contains_key(header::RETRY_AFTER));
    }

    #[tokio::test]
    async fn pressured_bypass_path_passes_through() {
        let policy = ShedPolicy::new(pressured_handle(), "loki", &["/ready"], None);
        let response = guarded_router(policy)
            .oneshot(Request::builder().uri("/ready").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn inert_guard_passes_through() {
        let policy = ShedPolicy::new(MemoryPressure::inert(), "loki", &["/ready"], None);
        let response = guarded_router(policy)
            .oneshot(Request::builder().uri("/work").body(Body::empty()).expect("request"))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::OK);
    }

    /// Router shedding every path, draining up to `drain_limit_bytes`.
    fn draining_router(drain_limit_bytes: usize) -> Router {
        let policy = ShedPolicy::new(pressured_handle(), "otlp_http", &[], Some(drain_limit_bytes));
        Router::new()
            .route("/v1/logs", post(|| async { StatusCode::OK }))
            .layer(axum::middleware::from_fn(move |req, next| {
                shed_when_pressured(policy.clone(), default_shed_response, req, next)
            }))
    }

    /// A `POST /v1/logs` carrying `len` bytes of body.
    fn logs_request(len: usize) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .body(Body::from(vec![0_u8; len]))
            .expect("request")
    }

    #[tokio::test]
    async fn a_body_within_the_drain_limit_is_shed_with_503() {
        const BODY_BYTES: usize = 1024 * 1024;

        // The oversized body must complete cleanly with a 503 (drained in
        // constant memory), never hang or reset the connection.
        let response = draining_router(BODY_BYTES)
            .oneshot(logs_request(BODY_BYTES))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn a_body_past_the_drain_limit_is_still_shed_with_503() {
        const DRAIN_LIMIT_BYTES: usize = 1024;

        // The drain stops at the limit rather than following the sender for as
        // long as it keeps writing; the status is still what the sender gets.
        let response = draining_router(DRAIN_LIMIT_BYTES)
            .oneshot(logs_request(DRAIN_LIMIT_BYTES * 16))
            .await
            .expect("response");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn draining_stops_at_the_limit() {
        const DRAIN_LIMIT_BYTES: usize = 1024;
        const CHUNK_BYTES: usize = 256;

        // A stream that counts what was pulled out of it: the oracle is the
        // number of chunks the drain consumed, which a limitless drain would
        // push to the full 16.
        let polled_chunks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&polled_chunks);
        let stream = futures::stream::iter(0..16).map(move |_| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, std::io::Error>(vec![0_u8; CHUNK_BYTES])
        });

        drain_request_body(Body::from_stream(stream), DRAIN_LIMIT_BYTES).await;

        assert_eq!(
            polled_chunks.load(std::sync::atomic::Ordering::SeqCst),
            DRAIN_LIMIT_BYTES / CHUNK_BYTES,
            "the drain must stop once it has read the limit"
        );
    }

    #[tokio::test]
    async fn draining_stops_at_the_first_read_error() {
        const CHUNK_BYTES: usize = 256;
        const STREAM_ITEMS: usize = 16;
        // Above everything the stream can yield, so stopping cannot be explained
        // by the limit: the error is the only thing that ends the drain.
        const DRAIN_LIMIT_BYTES: usize = CHUNK_BYTES * STREAM_ITEMS * 2;

        let polled_items = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&polled_items);
        let stream = futures::stream::iter(0..STREAM_ITEMS).map(move |index| {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if index == 0 {
                // The sender hung up mid-upload: there is nothing left to drain.
                Err(std::io::Error::other("read failed"))
            } else {
                Ok(vec![0_u8; CHUNK_BYTES])
            }
        });

        drain_request_body(Body::from_stream(stream), DRAIN_LIMIT_BYTES).await;

        assert_eq!(
            polled_items.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the drain must not read past the first error"
        );
    }
}
