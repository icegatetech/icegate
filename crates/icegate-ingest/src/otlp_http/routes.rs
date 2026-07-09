//! OTLP HTTP API routes

use axum::{
    Json, Router,
    extract::DefaultBodyLimit,
    http::StatusCode,
    middleware,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use icegate_common::{MemoryPressure, ShedPolicy, shed_when_pressured};
use tower_http::decompression::RequestDecompressionLayer;

use super::{
    handlers,
    models::{ErrorResponse, ErrorType},
    server::OtlpHttpState,
};

/// Create OTLP HTTP API router.
///
/// Includes a [`RequestDecompressionLayer`] that transparently decompresses
/// request bodies when a `Content-Encoding` header is present (gzip, zstd).
/// This matches the `OpenTelemetry` Collector's default behaviour of sending
/// gzip-compressed payloads.
///
/// `max_body_bytes` caps the size of the decompressed request body via
/// [`DefaultBodyLimit`]; requests larger than this limit are rejected with HTTP 413.
pub fn routes(state: OtlpHttpState, max_body_bytes: usize, memory_pressure: MemoryPressure) -> Router {
    Router::new()
        // OTLP endpoints (support both protobuf and JSON)
        .route("/v1/logs", post(handlers::ingest_logs))
        .route("/v1/traces", post(handlers::ingest_traces))
        .route("/v1/metrics", post(handlers::ingest_metrics))
        // Health check
        .route("/health", get(handlers::health))
        // Decompress gzip / zstd request bodies (OTLP spec requirement).
        // The body-limit layer is added after decompression so the cap applies to the
        // *decompressed* payload size (the figure that drives parser memory usage).
        .layer(RequestDecompressionLayer::new())
        .layer(DefaultBodyLimit::max(max_body_bytes))
        // Outermost: shed with a drained 503 while under memory pressure, before
        // decompress / decode. Runs first of all layers; `/health` bypasses so the
        // kubelet never kills a recovering pod. `drain_body=true` lets the client
        // finish its upload and read the 503 instead of seeing a connection reset.
        .layer(middleware::from_fn(move |req, next| {
            shed_when_pressured(
                ShedPolicy::new(memory_pressure.clone(), "otlp_http", OTLP_HTTP_SHED_BYPASS, true),
                otlp_http_shed_response,
                req,
                next,
            )
        }))
        .with_state(state)
}

/// Paths exempt from memory-pressure shedding: the health probe must stay reachable.
const OTLP_HTTP_SHED_BYPASS: &[&str] = &["/health"];

/// 503 shed by the memory-pressure guard on the OTLP/HTTP surface.
///
/// Uses the crate-local [`ErrorResponse`]/[`ErrorType`] so the body matches every other
/// OTLP/HTTP error (`errorType: "internal"`). `Retry-After` is built inline from
/// [`icegate_common::SHED_RETRY_AFTER_SECS`] because the header helper is private to
/// `icegate_common::memory::http`.
fn otlp_http_shed_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from(icegate_common::SHED_RETRY_AFTER_SECS),
        )],
        Json(ErrorResponse::new(
            ErrorType::Internal,
            "ingest under memory pressure, retry later",
        )),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use axum::{
        body::Body,
        http::{Request, StatusCode, header::CONTENT_TYPE},
    };
    use flate2::write::GzEncoder;
    use http_body_util::BodyExt;
    use icegate_common::MemoryPressure;
    use icegate_queue::{WriteResult, channel};
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, KeyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        resource::v1::Resource,
    };
    use prost::Message;
    use tower::ServiceExt;

    use super::*;
    use crate::{infra::metrics::OtlpMetrics, otlp_http::server::OtlpHttpState};

    /// Fixed-ratio `UsageReader` for deterministic pressure in tests (no cgroup).
    struct FixedReader {
        limit: u64,
        working_set: u64,
    }

    impl icegate_common::UsageReader for FixedReader {
        fn limit_bytes(&self) -> u64 {
            self.limit
        }

        fn read_working_set_bytes(&self) -> icegate_common::Result<u64> {
            Ok(self.working_set)
        }
    }

    /// A `MemoryPressure` handle that reports "under pressure" after one sample:
    /// 95/100 = 0.95 >= the default 0.90 high-watermark. No background sampler.
    fn pressured_guard() -> MemoryPressure {
        let config = icegate_common::MemoryPressureConfig::default();
        let sampler = icegate_common::MemoryPressureSampler::with_reader(
            &config,
            std::sync::Arc::new(FixedReader {
                limit: 100,
                working_set: 95,
            }),
        );
        let handle = sampler.handle();
        sampler.sample_once().expect("sample_once succeeds with a fixed reader");
        handle
    }

    #[tokio::test]
    async fn sheds_with_503_under_memory_pressure() {
        let (tx, _rx) = channel(1);
        let app = routes(test_state(tx), TEST_MAX_BODY_BYTES, pressured_guard());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(encode_protobuf()))
            .expect("build request");

        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        // Own the header value before consuming the body (ends the headers borrow).
        let retry_after = response
            .headers()
            .get(axum::http::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .expect("retry-after header present");
        assert_eq!(retry_after, icegate_common::SHED_RETRY_AFTER_SECS.to_string());

        let body = response.into_body().collect().await.expect("body").to_bytes();
        let value: serde_json::Value = serde_json::from_slice(&body).expect("json body");
        // Assert on the error-type classification, not the human message.
        assert_eq!(value["errorType"], "internal");
    }

    #[tokio::test]
    async fn health_probe_bypasses_shedding() {
        let (tx, _rx) = channel(1);
        let app = routes(test_state(tx), TEST_MAX_BODY_BYTES, pressured_guard());
        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .expect("build request");

        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn sheds_oversized_body_under_pressure_without_hang() {
        const TINY_LIMIT_BYTES: usize = 1024;

        let (tx, _rx) = channel(1);
        let app = routes(test_state(tx), TINY_LIMIT_BYTES, pressured_guard());
        let oversized_body = vec![0_u8; TINY_LIMIT_BYTES * 2];
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(oversized_body))
            .expect("build request");

        // Shed layer is outermost with drain_body=true: it drains the oversized body in
        // constant memory and returns 503 before the 413 body-limit layer runs. `oneshot`
        // completing (not hanging / resetting) is the assertion the drain preserves.
        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Body limit used by tests that expect to *succeed*. Plenty of headroom for the small
    /// fixtures built below.
    const TEST_MAX_BODY_BYTES: usize = 16 * 1024 * 1024;

    fn test_state(write_channel: icegate_queue::WriteChannel) -> OtlpHttpState {
        OtlpHttpState {
            write_channel,
            wal_row_group_size: 4,
            operations_enabled: true,
            metrics: OtlpMetrics::new_disabled(),
        }
    }

    fn create_test_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        observed_time_unix_nano: 1_700_000_000_000_000_000,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(Value::StringValue("Test message".to_string())),
                        }),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![0; 16],
                        span_id: vec![0; 8],
                        event_name: String::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;

    fn encode_protobuf() -> Vec<u8> {
        create_test_request().encode_to_vec()
    }

    fn gzip_compress(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data).expect("gzip write");
        encoder.finish().expect("gzip finish")
    }

    fn zstd_compress(data: &[u8]) -> Vec<u8> {
        zstd::encode_all(data, 3).expect("zstd encode")
    }

    /// Spawn a WAL writer that acknowledges the first write request.
    fn spawn_ack_writer(mut rx: icegate_queue::WriteReceiver) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            let total_rows = request.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            request
                .response_tx
                .send(WriteResult::success(1, total_rows, None))
                .expect("send wal ack");
        })
    }

    #[tokio::test]
    async fn ingest_gzip_compressed_protobuf() {
        let (tx, rx) = channel(1);
        let writer = spawn_ack_writer(rx);

        let compressed = gzip_compress(&encode_protobuf());
        let app = routes(test_state(tx), TEST_MAX_BODY_BYTES, MemoryPressure::inert());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header("content-encoding", "gzip")
            .body(Body::from(compressed))
            .expect("build request");

        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.expect("body").to_bytes();
        assert!(!body.is_empty());

        writer.await.expect("writer task");
    }

    #[tokio::test]
    async fn ingest_zstd_compressed_protobuf() {
        let (tx, rx) = channel(1);
        let writer = spawn_ack_writer(rx);

        let compressed = zstd_compress(&encode_protobuf());
        let app = routes(test_state(tx), TEST_MAX_BODY_BYTES, MemoryPressure::inert());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header("content-encoding", "zstd")
            .body(Body::from(compressed))
            .expect("build request");

        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.expect("body").to_bytes();
        assert!(!body.is_empty());

        writer.await.expect("writer task");
    }

    #[tokio::test]
    async fn ingest_gzip_compressed_json() {
        let (tx, rx) = channel(1);
        let writer = spawn_ack_writer(rx);

        let json = serde_json::to_vec(&create_test_request()).expect("json encode");
        let compressed = gzip_compress(&json);
        let app = routes(test_state(tx), TEST_MAX_BODY_BYTES, MemoryPressure::inert());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/json")
            .header("content-encoding", "gzip")
            .body(Body::from(compressed))
            .expect("build request");

        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        writer.await.expect("writer task");
    }

    #[tokio::test]
    async fn ingest_uncompressed_still_works() {
        let (tx, rx) = channel(1);
        let writer = spawn_ack_writer(rx);

        let app = routes(test_state(tx), TEST_MAX_BODY_BYTES, MemoryPressure::inert());
        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(encode_protobuf()))
            .expect("build request");

        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        writer.await.expect("writer task");
    }

    /// Verifies that bodies exceeding `max_body_bytes` are rejected with HTTP 413.
    /// This guards against regressions of the bug where the framework's 2 `MiB` default
    /// silently dropped real OTLP batches.
    #[tokio::test]
    async fn rejects_payloads_exceeding_max_body_bytes() {
        // 1 KiB cap; send a 2 KiB body — well under any realistic payload but above the cap.
        const TINY_LIMIT_BYTES: usize = 1024;

        // Channel intentionally sized with capacity but never read from: if the body
        // limit fails open, the handler would attempt to write to it and the test would
        // hang on `oneshot`, surfacing the regression.
        let (tx, _rx) = channel(1);

        let app = routes(test_state(tx), TINY_LIMIT_BYTES, MemoryPressure::inert());
        let oversized_body = vec![0_u8; TINY_LIMIT_BYTES * 2];
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(oversized_body))
            .expect("build request");

        let response = app.oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
