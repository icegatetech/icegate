//! Tempo HTTP server implementation

use std::net::SocketAddr;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::TempoConfig;
use crate::query::engine::QueryEngine;

/// Shared application state for Tempo server
#[derive(Clone)]
pub struct TempoState {
    /// Query engine for creating `DataFusion` sessions
    #[allow(dead_code)]
    pub engine: Arc<QueryEngine>,
}

/// Run the Tempo HTTP server
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
pub async fn run(
    engine: Arc<QueryEngine>,
    config: TempoConfig,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    let state = TempoState { engine };

    let app = super::routes::routes(state);

    tracing::info!("Tempo API server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("Tempo server shutting down gracefully");
        })
        .await?;

    tracing::info!("Tempo server stopped");

    Ok(())
}
