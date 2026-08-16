//! Tempo HTTP server implementation

use std::{net::SocketAddr, sync::Arc};

use icegate_common::MemoryPressure;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::TempoConfig;
use crate::engine::QueryEngine;

/// Shared application state for Tempo server
#[derive(Clone)]
pub struct TempoState {
    /// Query engine — primarily used to access the iceberg catalog for
    /// span table loads.
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
    pressure: MemoryPressure,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with_port_tx(engine, config, cancel_token, None, pressure).await
}

/// Run the Tempo HTTP server with optional port notification
///
/// When `port_tx` is provided, the actual bound port is sent after the listener binds.
/// This is useful for ephemeral port assignment (port 0) in tests.
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
pub async fn run_with_port_tx(
    engine: Arc<QueryEngine>,
    config: TempoConfig,
    cancel_token: CancellationToken,
    port_tx: Option<oneshot::Sender<u16>>,
    pressure: MemoryPressure,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = format!("{}:{}", config.host, config.port).parse()?;

    let state = TempoState { engine };

    let app = super::routes::routes(state, pressure);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    tracing::info!("Tempo API server listening on {}", local_addr);

    // Notify the caller of the actual bound port (useful for ephemeral ports)
    if let Some(tx) = port_tx {
        let _ = tx.send(local_addr.port());
    }

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("Tempo server shutting down gracefully");
        })
        .await?;

    tracing::info!("Tempo server stopped");

    Ok(())
}
