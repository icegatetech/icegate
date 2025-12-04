//! OTLP HTTP server implementation

use std::{net::SocketAddr, sync::Arc};

use iceberg::Catalog;
use tokio_util::sync::CancellationToken;

use super::OtlpHttpConfig;

/// Shared application state for OTLP HTTP server
#[derive(Clone)]
pub struct OtlpHttpState {
    /// Iceberg catalog for accessing tables
    #[allow(dead_code)]
    pub catalog: Arc<dyn Catalog>,
}

/// Run the OTLP HTTP server
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
pub async fn run(
    catalog: Arc<dyn Catalog>,
    config: OtlpHttpConfig,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    let state = OtlpHttpState {
        catalog,
    };

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
