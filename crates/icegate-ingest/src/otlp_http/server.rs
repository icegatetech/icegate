//! OTLP HTTP server implementation

use std::net::SocketAddr;

use icegate_queue::WriteChannel;
use tokio_util::sync::CancellationToken;

use super::OtlpHttpConfig;

/// Shared application state for OTLP HTTP server
#[derive(Clone)]
pub struct OtlpHttpState {
    /// Write channel for sending batches to the WAL queue.
    pub write_channel: WriteChannel,
}

/// Run the OTLP HTTP server
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
pub async fn run(
    write_channel: WriteChannel,
    config: OtlpHttpConfig,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    let state = OtlpHttpState { write_channel };

    let app = super::routes::routes(state);

    tracing::info!("OTLP HTTP server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("OTLP HTTP server shutting down gracefully");
        })
        .await?;

    tracing::info!("OTLP HTTP server stopped");

    Ok(())
}
