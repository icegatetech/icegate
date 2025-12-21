//! OTLP gRPC server implementation.

use icegate_queue::WriteChannel;
use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer, metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;

use super::{OtlpGrpcConfig, services::OtlpGrpcService};

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
    config: OtlpGrpcConfig,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", config.host, config.port).parse()?;
    let service = OtlpGrpcService::new(write_channel);

    tracing::info!("Starting OTLP gRPC server on {}", addr);

    Server::builder()
        .add_service(LogsServiceServer::new(service.clone()))
        .add_service(TraceServiceServer::new(service.clone()))
        .add_service(MetricsServiceServer::new(service))
        .serve_with_shutdown(addr, async move {
            cancel_token.cancelled().await;
            tracing::info!("OTLP gRPC server shutting down gracefully");
        })
        .await?;

    tracing::info!("OTLP gRPC server stopped");

    Ok(())
}
