//! `OpenSearch` listener: bind, serve, graceful shutdown.

use std::sync::Arc;

use icegate_common::{ApplicationProtocol, ListenerRuntime, ServerListener};
use tokio::sync::oneshot;

use super::config::{EmulatedVersion, OpenSearchConfig};
use super::node::NodeIdentity;
use super::state::OpenSearchState;
use crate::infra::runtime::QueryRuntime;

/// Run the listener until the token of `listener_runtime` is cancelled.
///
/// Binds `config.host:config.port` and serves over TLS, offering ALPN `http/1.1`, when
/// `listener_runtime` carries a certificate, and plaintext otherwise.
///
/// # Errors
///
/// Returns an error when the configured version does not parse (configuration validation
/// refuses it earlier), when the address cannot be bound, or when serving fails.
pub async fn run(
    runtime: QueryRuntime,
    config: OpenSearchConfig,
    listener_runtime: ListenerRuntime,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with_port_tx(runtime, config, listener_runtime, None).await
}

/// [`run`], reporting the bound port on `port_tx` once the listener is bound, so a caller can bind
/// port 0 and learn the port.
///
/// # Errors
///
/// As [`run`].
pub async fn run_with_port_tx(
    runtime: QueryRuntime,
    config: OpenSearchConfig,
    listener_runtime: ListenerRuntime,
    port_tx: Option<oneshot::Sender<u16>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let version = EmulatedVersion::parse(&config.version)?;
    let listener =
        ServerListener::bind(&config.host, config.port, ApplicationProtocol::Http1, &listener_runtime).await?;
    let local_address = listener.local_addr();
    let state = OpenSearchState {
        node: Arc::new(NodeIdentity::new(&config.cluster_name, version)),
    };
    let app = super::routes::build_router(state, &runtime, &config);
    tracing::info!("OpenSearch API server listening on {}", local_address);
    if let Some(sender) = port_tx {
        // A dropped receiver means the caller stopped waiting; serving continues regardless.
        let _ = sender.send(local_address.port());
    }
    let cancel_token = listener_runtime.cancel_token().clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("OpenSearch server shutting down gracefully...");
        })
        .await?;
    tracing::info!("OpenSearch server stopped");
    Ok(())
}
