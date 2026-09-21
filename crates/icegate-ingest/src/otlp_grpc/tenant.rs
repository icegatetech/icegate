//! Tenant-rejection recording for the OTLP/gRPC surface.
//!
//! The policy itself, the refusal status and the extension the handler reads are
//! [`TenantPolicyInterceptor`](icegate_common::TenantPolicyInterceptor) in
//! `icegate-common`. What stays here is the one thing that surface owns: the
//! `signal` label its counter carries.

use icegate_common::TenantRejectionRecorder;

use crate::infra::metrics::OtlpMetrics;

/// Records the refusals of one OTLP/gRPC service: adds to the
/// `(protocol, reason)` pair the `signal` label
/// `icegate_ingest_otlp_tenant_rejections` carries and the interceptor in
/// `icegate-common` knows nothing about.
///
/// One instance per wrapped server: the signal is a property of the service, so
/// it is captured at construction rather than parsed out of the request path on
/// every call.
#[derive(Clone)]
pub struct OtlpTenantRejectionRecorder {
    metrics: OtlpMetrics,
    signal: &'static str,
}

impl OtlpTenantRejectionRecorder {
    /// Build a recorder for one signal. `signal` is the metric attribute value
    /// (`"logs" | "traces" | "metrics"`).
    #[must_use]
    pub const fn new(metrics: OtlpMetrics, signal: &'static str) -> Self {
        Self { metrics, signal }
    }
}

impl TenantRejectionRecorder for OtlpTenantRejectionRecorder {
    fn add_tenant_rejection(&self, protocol: &str, reason: &str) {
        self.metrics.add_tenant_rejection(protocol, self.signal, reason);
    }
}

#[cfg(test)]
mod tests {
    use icegate_common::{TENANT_ID_HEADER, TenantPolicyInterceptor, TenantResolver};
    use opentelemetry::metrics::MeterProvider as _;
    use tonic::{Request, metadata::MetadataValue, service::Interceptor as _};

    use super::*;
    use crate::{
        infra::metrics::test_support::{build_meter_provider, find_counter_total},
        otlp_grpc::services::PROTOCOL_GRPC,
    };

    /// The interceptor is the only place the gRPC surface reports a refused RPC
    /// from, so the labels are read back through the exporter rather than
    /// asserted on the recorder. What the interceptor itself does with the port
    /// is covered in `icegate-common`; this is the label set the dashboards are
    /// keyed on.
    #[test]
    fn a_refused_rpc_counts_a_tenant_rejection_labelled_by_protocol_signal_and_reason() {
        let (provider, exporter) = build_meter_provider();
        let metrics = OtlpMetrics::new(&provider.meter("test_grpc_tenant_rejections"));

        let mut request = Request::new(());
        for _ in 0..2 {
            request.metadata_mut().append(
                TENANT_ID_HEADER,
                MetadataValue::try_from("acme").expect("ascii metadata value"),
            );
        }
        TenantPolicyInterceptor::new(
            TenantResolver::Multi,
            OtlpTenantRejectionRecorder::new(metrics, "traces"),
            PROTOCOL_GRPC,
        )
        .call(request)
        .expect_err("a duplicated header is refused");

        provider.force_flush().expect("failed to flush metrics");
        assert_eq!(
            find_counter_total(
                &exporter,
                "icegate_ingest_otlp_tenant_rejections",
                &[
                    ("protocol", "grpc"),
                    ("signal", "traces"),
                    ("reason", "duplicate_header"),
                ],
            ),
            1
        );
    }
}

/// Service-level coverage of the interceptor over a real `LogsServiceServer`.
///
/// Separate module because it drives the generated tonic service rather than the
/// interceptor alone. It is the test that fails if tonic stops carrying the
/// extensions an interceptor sets through to the handler — the one assumption
/// this design makes about someone else's library.
#[cfg(test)]
mod service_tests {
    use arrow::array::StringArray;
    // The generated tonic service is an `http::Service`; `axum` re-exports the
    // `http` crate, so no direct dependency on it is added for this test.
    use axum::http::{Request as HttpRequest, StatusCode};
    use bytes::Bytes;
    use http_body_util::Full;
    use icegate_common::schema::COL_TENANT_ID;
    use icegate_common::{TENANT_ID_HEADER, TenantPolicyInterceptor, TenantResolver};
    use icegate_queue::{WriteResult, channel};
    use opentelemetry_proto::tonic::{
        collector::logs::v1::{ExportLogsServiceRequest, logs_service_server::LogsServiceServer},
        common::v1::{AnyValue, any_value::Value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    };
    use prost::Message;
    use tonic::service::interceptor::InterceptedService;
    use tower::ServiceExt;

    use super::*;
    use crate::otlp_grpc::services::{OtlpGrpcService, PROTOCOL_GRPC};

    /// The gRPC length-prefixed framing of one unary message: a compression flag
    /// byte followed by a big-endian `u32` length.
    fn frame_message(message: &impl Message) -> Bytes {
        let payload = message.encode_to_vec();
        let mut framed = Vec::with_capacity(payload.len() + 5);
        framed.push(0);
        framed.extend_from_slice(&u32::try_from(payload.len()).expect("fixture fits in u32").to_be_bytes());
        framed.extend_from_slice(&payload);
        Bytes::from(framed)
    }

    fn one_log_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
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

    #[tokio::test]
    async fn the_handler_writes_the_tenant_the_interceptor_resolved() {
        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            let tenants = request
                .row_groups
                .iter()
                .map(|row_group| {
                    let column = row_group
                        .batch
                        .column_by_name(COL_TENANT_ID)
                        .expect("the logs batch carries a tenant_id column")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("tenant_id is a string column");
                    column.value(0).to_string()
                })
                .collect::<Vec<_>>();
            let rows = request.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            request
                .response_tx
                .send(WriteResult::success(1, rows, None))
                .expect("send wal ack");
            tenants
        });

        let service = OtlpGrpcService::new(tx, 4, false, OtlpMetrics::new_disabled());
        let server = InterceptedService::new(
            LogsServiceServer::new(service),
            TenantPolicyInterceptor::new(
                TenantResolver::Multi,
                OtlpTenantRejectionRecorder::new(OtlpMetrics::new_disabled(), "logs"),
                PROTOCOL_GRPC,
            ),
        );

        let request = HttpRequest::builder()
            .method("POST")
            .uri("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
            .header("content-type", "application/grpc")
            .header(TENANT_ID_HEADER, "acme")
            .body(Full::new(frame_message(&one_log_request())))
            .expect("build grpc request");

        let response = server.oneshot(request).await.expect("the service answers");
        assert_eq!(response.status(), StatusCode::OK);

        let tenants = writer.await.expect("writer task");
        assert_eq!(
            tenants,
            vec!["acme".to_string()],
            "the handler must write the tenant the interceptor resolved from the header"
        );
    }
}
