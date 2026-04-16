//! OTLP HTTP API routes

use axum::{
    Router,
    routing::{get, post},
};
use tower_http::decompression::RequestDecompressionLayer;

use super::{handlers, server::OtlpHttpState};

/// Create OTLP HTTP API router.
///
/// Includes a [`RequestDecompressionLayer`] that transparently decompresses
/// request bodies when a `Content-Encoding` header is present (gzip, zstd).
/// This matches the `OpenTelemetry` Collector's default behaviour of sending
/// gzip-compressed payloads.
pub fn routes(state: OtlpHttpState) -> Router {
    Router::new()
        // OTLP endpoints (support both protobuf and JSON)
        .route("/v1/logs", post(handlers::ingest_logs))
        .route("/v1/traces", post(handlers::ingest_traces))
        .route("/v1/metrics", post(handlers::ingest_metrics))
        // Health check
        .route("/health", get(handlers::health))
        // Decompress gzip / zstd request bodies (OTLP spec requirement).
        .layer(RequestDecompressionLayer::new())
        .with_state(state)
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

    fn test_state(write_channel: icegate_queue::WriteChannel) -> OtlpHttpState {
        OtlpHttpState {
            write_channel,
            wal_row_group_size: 4,
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
        let app = routes(test_state(tx));
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
        let app = routes(test_state(tx));
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
        let app = routes(test_state(tx));
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

        let app = routes(test_state(tx));
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
}
