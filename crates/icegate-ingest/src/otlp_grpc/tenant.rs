//! Tenant resolution for the OTLP/gRPC surface.
//!
//! Runs as a tonic interceptor on `Request<()>` — metadata only, before the
//! protobuf decode and the handler — so an RPC carrying no usable tenant costs
//! nothing beyond reading its headers.

use icegate_common::{TENANT_ID_HEADER, TenantHeader, TenantRejection, TenantResolver};
use tonic::{Request, Status, metadata::MetadataMap, service::Interceptor};

use crate::infra::metrics::OtlpMetrics;

/// Rejects RPCs that carry no usable tenant, and hands the resolved
/// [`TenantId`](icegate_common::TenantId) to the handler through the request
/// extensions.
///
/// One instance per wrapped server: the signal is a property of the service, so
/// it is captured at construction rather than parsed out of the request path on
/// every call.
///
/// Wrap the configured server with `InterceptedService::new(server, interceptor)`
/// (NOT codegen `with_interceptor`, which would rebuild the service and revert
/// any `max_decoding_message_size`).
#[derive(Clone)]
pub struct TenantPolicyInterceptor {
    resolver: TenantResolver,
    metrics: OtlpMetrics,
    signal: &'static str,
}

impl TenantPolicyInterceptor {
    /// Build an interceptor for one signal. `signal` is the metric attribute
    /// value (`"logs" | "traces" | "metrics"`).
    #[must_use]
    pub const fn new(resolver: TenantResolver, metrics: OtlpMetrics, signal: &'static str) -> Self {
        Self {
            resolver,
            metrics,
            signal,
        }
    }
}

impl Interceptor for TenantPolicyInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let outcome = self.resolver.resolve_tenant(read_tenant_metadata(request.metadata()));
        match outcome {
            Ok(tenant) => {
                request.extensions_mut().insert(tenant);
                Ok(request)
            }
            Err(rejection) => {
                self.metrics
                    .add_tenant_rejection(super::services::PROTOCOL_GRPC, self.signal, rejection.reason());
                Err(reject_request(rejection))
            }
        }
    }
}

/// Read [`TENANT_ID_HEADER`] from the request metadata as the policy input.
///
/// Only the mapping from this surface's metadata map; what the values mean is
/// [`TenantHeader::from_values`], which the OTLP/HTTP surface reaches the same
/// way.
fn read_tenant_metadata(metadata: &MetadataMap) -> TenantHeader<'_> {
    TenantHeader::from_values(metadata.get_all(TENANT_ID_HEADER).iter().map(|value| value.to_str().ok()))
}

/// The status an RPC carrying no usable tenant is refused with.
///
/// `INVALID_ARGUMENT` rather than `UNAUTHENTICATED`: a stock OTLP exporter
/// treats it as permanent and drops the batch instead of retrying a request that
/// can never succeed as sent.
fn reject_request(rejection: TenantRejection) -> Status {
    Status::invalid_argument(rejection.message())
}

#[cfg(test)]
mod tests {
    use icegate_common::{TenantId, TenantPolicy};
    use opentelemetry::metrics::MeterProvider as _;
    use tonic::{Code, metadata::MetadataValue};

    use super::*;
    use crate::infra::metrics::test_support::{build_meter_provider, find_counter_total};

    fn multi_interceptor() -> TenantPolicyInterceptor {
        TenantPolicyInterceptor::new(TenantResolver::Multi, OtlpMetrics::new_disabled(), "logs")
    }

    fn request_with_tenants(values: &[&str]) -> Request<()> {
        let mut request = Request::new(());
        for value in values {
            request.metadata_mut().append(
                TENANT_ID_HEADER,
                MetadataValue::try_from(*value).expect("ascii metadata value"),
            );
        }
        request
    }

    #[test]
    fn rejects_with_invalid_argument_when_the_header_is_absent_in_multi() {
        let status = multi_interceptor()
            .call(Request::new(()))
            .expect_err("multi must refuse an RPC with no tenant header");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn rejects_a_duplicated_header() {
        let status = multi_interceptor()
            .call(request_with_tenants(&["acme", "acme"]))
            .expect_err("a duplicated header is refused even when the values agree");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn rejects_a_header_naming_another_tenant_in_single() {
        let resolver = TenantPolicy::Single { id: "acme".to_string() }
            .into_resolver()
            .expect("valid single policy");
        let status = TenantPolicyInterceptor::new(resolver, OtlpMetrics::new_disabled(), "logs")
            .call(request_with_tenants(&["victim"]))
            .expect_err("single must refuse a header naming another tenant");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    /// The interceptor is the only place the gRPC surface reports a refused RPC
    /// from, so the labels are read back through the exporter rather than
    /// asserted on the recorder.
    #[test]
    fn a_refused_rpc_counts_a_tenant_rejection_labelled_by_protocol_signal_and_reason() {
        let (provider, exporter) = build_meter_provider();
        let metrics = OtlpMetrics::new(&provider.meter("test_grpc_tenant_rejections"));

        TenantPolicyInterceptor::new(TenantResolver::Multi, metrics, "traces")
            .call(request_with_tenants(&["acme", "acme"]))
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

    #[test]
    fn puts_the_resolved_tenant_into_extensions() {
        let request = multi_interceptor()
            .call(request_with_tenants(&["tenant-b"]))
            .expect("a valid header resolves");
        let tenant = request
            .extensions()
            .get::<TenantId>()
            .expect("the interceptor must place the tenant in the extensions");
        assert_eq!(tenant.as_ref(), "tenant-b");
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
    use crate::otlp_grpc::services::OtlpGrpcService;

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
            TenantPolicyInterceptor::new(TenantResolver::Multi, OtlpMetrics::new_disabled(), "logs"),
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
