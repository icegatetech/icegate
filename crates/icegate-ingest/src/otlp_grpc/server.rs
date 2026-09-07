//! OTLP gRPC server implementation.

use icegate_common::{MemoryPressure, MemoryShedInterceptor, TenantResolver};
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer, metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use tokio_util::sync::CancellationToken;
use tonic::{service::interceptor::InterceptedService, transport::Server};

use super::{
    OtlpGrpcConfig,
    services::{OtlpGrpcService, SIGNAL_LOGS, SIGNAL_METRICS, SIGNAL_TRACES},
    tenant::TenantPolicyInterceptor,
};

/// Run the OTLP gRPC server.
///
/// Starts a tonic gRPC server that handles `OpenTelemetry` Protocol requests
/// for logs, traces, and metrics ingestion.
///
/// # Errors
///
/// Returns an error if:
/// - The socket address cannot be parsed
/// - The server fails to bind to the address
/// - The server encounters a fatal error during operation
pub async fn run(
    service: OtlpGrpcService,
    config: OtlpGrpcConfig,
    cancel_token: CancellationToken,
    memory_pressure: MemoryPressure,
    tenant_resolver: TenantResolver,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", config.host, config.port).parse()?;
    let metrics = service.metrics();

    // Reject before protobuf decode / session build while under memory pressure. Wrapping
    // the *configured* server with `InterceptedService::new` (uniform with Flight SQL)
    // preserves `NamedService` routing and any future `max_*_message_size` limits, which
    // `with_interceptor` would silently reset to tonic's 4 MiB default.
    let shed = MemoryShedInterceptor::new(memory_pressure, "otlp_grpc");

    // One tenant interceptor per server: the signal it labels its rejections with
    // is the service's own, known statically, so no request path is parsed.
    let make_tenant_interceptor = |signal| TenantPolicyInterceptor::new(tenant_resolver.clone(), metrics.clone(), signal);

    tracing::info!("Starting OTLP gRPC server on {}", addr);

    // Nested wrapping, shed outermost: the outer interceptor runs first, and
    // shedding under memory pressure must precede reading the tenant headers.
    // `NamedService` passes through both layers, so routing is unaffected.
    Server::builder()
        .add_service(InterceptedService::new(
            InterceptedService::new(LogsServiceServer::new(service.clone()), make_tenant_interceptor(SIGNAL_LOGS)),
            shed.clone(),
        ))
        .add_service(InterceptedService::new(
            InterceptedService::new(
                TraceServiceServer::new(service.clone()),
                make_tenant_interceptor(SIGNAL_TRACES),
            ),
            shed.clone(),
        ))
        .add_service(InterceptedService::new(
            InterceptedService::new(MetricsServiceServer::new(service), make_tenant_interceptor(SIGNAL_METRICS)),
            shed,
        ))
        .serve_with_shutdown(addr, async move {
            cancel_token.cancelled().await;
            tracing::info!("OTLP gRPC server shutting down gracefully");
        })
        .await?;

    tracing::info!("OTLP gRPC server stopped");

    Ok(())
}
