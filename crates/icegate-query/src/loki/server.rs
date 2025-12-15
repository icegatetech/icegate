//! Loki HTTP server implementation

use std::{net::SocketAddr, sync::Arc};

use tokio_util::sync::CancellationToken;

use super::LokiConfig;
use crate::engine::QueryEngine;

/// Shared application state for Loki server
#[derive(Clone)]
pub struct LokiState {
    /// Query engine for creating `DataFusion` sessions
    pub engine: Arc<QueryEngine>,
}

/// Run the Loki HTTP server
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
pub async fn run(
    engine: Arc<QueryEngine>,
    config: LokiConfig,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    let state = LokiState {
        engine,
    };

    let app = super::routes::routes(state);

    tracing::info!("Loki API server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("Loki server shutting down gracefully...");
        })
        .await?;

    tracing::info!("Loki server stopped");

    Ok(())
}
