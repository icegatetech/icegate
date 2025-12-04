//! Prometheus HTTP server implementation

use std::net::SocketAddr;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use super::PrometheusConfig;
use crate::query::engine::QueryEngine;

/// Shared application state for Prometheus server
#[derive(Clone)]
pub struct PrometheusState {
    /// Query engine for creating `DataFusion` sessions
    #[allow(dead_code)]
    pub engine: Arc<QueryEngine>,
}

/// Run the Prometheus HTTP server
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
pub async fn run(
    engine: Arc<QueryEngine>,
    config: PrometheusConfig,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    let state = PrometheusState { engine };

    let app = super::routes::routes(state);

    tracing::info!("Prometheus API server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("Prometheus server shutting down gracefully");
        })
        .await?;

    tracing::info!("Prometheus server stopped");

    Ok(())
}
