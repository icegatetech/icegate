//! Loki HTTP server implementation

use std::sync::Arc;

use icegate_common::{ApplicationProtocol, ListenerRuntime, ServerListener};
use tokio::sync::oneshot;

use super::LokiConfig;
use crate::{engine::QueryEngine, infra::metrics::QueryMetrics, infra::runtime::QueryRuntime};

/// Shared application state for Loki server
#[derive(Clone)]
pub struct LokiState {
    /// Query engine for creating `DataFusion` sessions
    pub engine: Arc<QueryEngine>,
    /// Query metrics recorder
    pub metrics: Arc<QueryMetrics>,
}

/// Run the Loki HTTP server
///
/// Binds `config.host:config.port` and serves over TLS, offering ALPN `http/1.1`, when
/// `listener_runtime` carries a certificate, and plaintext otherwise. Returns once the
/// runtime's token is cancelled and the server has drained.
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
pub async fn run(
    runtime: QueryRuntime,
    config: LokiConfig,
    listener_runtime: ListenerRuntime,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with_port_tx(runtime, config, listener_runtime, None).await
}

/// Run the Loki HTTP server with optional port notification
///
/// When `port_tx` is provided, the actual bound port is sent after the listener binds.
/// This is useful for ephemeral port assignment (port 0) in tests.
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
pub async fn run_with_port_tx(
    runtime: QueryRuntime,
    config: LokiConfig,
    listener_runtime: ListenerRuntime,
    port_tx: Option<oneshot::Sender<u16>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = LokiState {
        engine: Arc::clone(&runtime.engine),
        metrics: Arc::clone(&runtime.metrics),
    };

    let app = super::routes::routes(state, &runtime);

    let listener =
        ServerListener::bind(&config.host, config.port, ApplicationProtocol::Http1, &listener_runtime).await?;
    let local_addr = listener.local_addr();

    tracing::info!("Loki API server listening on {}", local_addr);

    // Notify the caller of the actual bound port (useful for ephemeral ports)
    if let Some(tx) = port_tx {
        let _ = tx.send(local_addr.port());
    }

    let cancel_token = listener_runtime.cancel_token().clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("Loki server shutting down gracefully...");
        })
        .await?;

    tracing::info!("Loki server stopped");

    Ok(())
}
