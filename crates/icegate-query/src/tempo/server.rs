//! Tempo HTTP server implementation

use std::sync::Arc;

use icegate_common::{ApplicationProtocol, ListenerRuntime, ServerListener};
use tokio::sync::oneshot;

use super::TempoConfig;
use crate::{engine::QueryEngine, infra::runtime::QueryRuntime};

/// Shared application state for Tempo server
#[derive(Clone)]
pub struct TempoState {
    /// Query engine — primarily used to access the iceberg catalog for
    /// span table loads.
    pub engine: Arc<QueryEngine>,
}

/// Run the Tempo HTTP server
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
    config: TempoConfig,
    listener_runtime: ListenerRuntime,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with_port_tx(runtime, config, listener_runtime, None).await
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
    runtime: QueryRuntime,
    config: TempoConfig,
    listener_runtime: ListenerRuntime,
    port_tx: Option<oneshot::Sender<u16>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = TempoState {
        engine: Arc::clone(&runtime.engine),
    };

    let app = super::routes::routes(state, &runtime);

    let listener =
        ServerListener::bind(&config.host, config.port, ApplicationProtocol::Http1, &listener_runtime).await?;
    let local_addr = listener.local_addr();

    tracing::info!("Tempo API server listening on {}", local_addr);

    // Notify the caller of the actual bound port (useful for ephemeral ports)
    if let Some(tx) = port_tx {
        let _ = tx.send(local_addr.port());
    }

    let cancel_token = listener_runtime.cancel_token().clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("Tempo server shutting down gracefully");
        })
        .await?;

    tracing::info!("Tempo server stopped");

    Ok(())
}
