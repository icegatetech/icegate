//! OTLP gRPC server implementation.

use icegate_common::{MemoryPressure, MemoryShedInterceptor};
use icegate_queue::WriteChannel;
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer, metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use tokio_util::sync::CancellationToken;
use tonic::{service::interceptor::InterceptedService, transport::Server};

use super::{OtlpGrpcConfig, services::OtlpGrpcService};
use crate::infra::metrics::OtlpMetrics;

/// Run the OTLP gRPC server.
///
/// Starts a tonic gRPC server that handles `OpenTelemetry` Protocol requests
/// for logs, traces, and metrics ingestion.
///
/// # Arguments
///
/// * `write_channel` - Channel for sending batches to the WAL queue
/// * `config` - Server configuration (host, port)
/// * `cancel_token` - Token for graceful shutdown
///
/// # Errors
///
/// Returns an error if:
/// - The socket address cannot be parsed
/// - The server fails to bind to the address
/// - The server encounters a fatal error during operation
pub async fn run(
    write_channel: WriteChannel,
    wal_row_group_size: usize,
    operations_enabled: bool,
    metrics: OtlpMetrics,
    config: OtlpGrpcConfig,
    cancel_token: CancellationToken,
    memory_pressure: MemoryPressure,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", config.host, config.port).parse()?;
    let service = OtlpGrpcService::new(write_channel, wal_row_group_size, operations_enabled, metrics);

    // Reject before protobuf decode / session build while under memory pressure. Wrapping
    // the *configured* server with `InterceptedService::new` (uniform with Flight SQL)
    // preserves `NamedService` routing and any future `max_*_message_size` limits, which
    // `with_interceptor` would silently reset to tonic's 4 MiB default.
    let shed = MemoryShedInterceptor::new(memory_pressure, "otlp_grpc");

    tracing::info!("Starting OTLP gRPC server on {}", addr);

    Server::builder()
        .add_service(InterceptedService::new(
            LogsServiceServer::new(service.clone()),
            shed.clone(),
        ))
        .add_service(InterceptedService::new(
            TraceServiceServer::new(service.clone()),
            shed.clone(),
        ))
        .add_service(InterceptedService::new(MetricsServiceServer::new(service), shed))
        .serve_with_shutdown(addr, async move {
            cancel_token.cancelled().await;
            tracing::info!("OTLP gRPC server shutting down gracefully");
        })
        .await?;

    tracing::info!("OTLP gRPC server stopped");

    Ok(())
}
