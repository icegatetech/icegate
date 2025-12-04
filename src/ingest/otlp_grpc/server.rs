//! OTLP gRPC server implementation

use std::sync::Arc;

use iceberg::Catalog;
use tokio_util::sync::CancellationToken;

use super::OtlpGrpcConfig;

/// Run the OTLP gRPC server
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
///
/// # TODO
/// - Import OTLP service definitions from opentelemetry-proto
/// - Implement `LogsService` for logs ingestion
///   ```rust,ignore
///   impl LogsService for OtlpService {
///       async fn export(&self, request: ExportLogsServiceRequest) -> Result<ExportLogsServiceResponse> {
///           // Parse OTLP logs
///           // Transform to Iceberg schema
///           // Write to logs table
///       }
///   }
///   ```
/// - Implement `TracesService` for traces ingestion
///   ```rust,ignore
///   impl TracesService for OtlpService {
///       async fn export(&self, request: ExportTracesServiceRequest) -> Result<ExportTracesServiceResponse> {
///           // Parse OTLP spans
///           // Transform to Iceberg schema
///           // Write to spans table
///       }
///   }
///   ```
/// - Implement `MetricsService` for metrics ingestion
///   ```rust,ignore
///   impl MetricsService for OtlpService {
///       async fn export(&self, request: ExportMetricsServiceRequest) -> Result<ExportMetricsServiceResponse> {
///           // Parse OTLP metrics
///           // Transform to Iceberg schema
///           // Write to metrics table
///       }
///   }
///   ```
/// - Create tonic server with all three services
/// - Configure TLS if needed
/// - Handle graceful shutdown
pub async fn run(
    _catalog: Arc<dyn Catalog>,
    config: OtlpGrpcConfig,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::warn!(
        "OTLP gRPC server is not yet implemented (would listen on {}:{})",
        config.host,
        config.port
    );

    // TODO: Replace this with actual gRPC server implementation
    // Wait for cancellation signal
    cancel_token.cancelled().await;

    tracing::info!("OTLP gRPC server (stub) stopped");

    Ok(())
}
