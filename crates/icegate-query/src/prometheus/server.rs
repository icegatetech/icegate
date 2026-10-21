//! Prometheus HTTP server implementation

use std::sync::Arc;

use icegate_common::{ApplicationProtocol, ListenerRuntime, ServerListener};

use super::PrometheusConfig;
use crate::{engine::QueryEngine, infra::runtime::QueryRuntime};

/// Shared application state for Prometheus server
#[derive(Clone)]
pub struct PrometheusState {
    /// Query engine for creating `DataFusion` sessions
    #[allow(dead_code)]
    pub engine: Arc<QueryEngine>,
}

/// Run the Prometheus HTTP server
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
    config: PrometheusConfig,
    listener_runtime: ListenerRuntime,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = PrometheusState {
        engine: Arc::clone(&runtime.engine),
    };

    // No tenant layer: every handler of this surface is a `501` stub that reads
    // no tenant (`prometheus/handlers.rs`). It goes on when PromQL does, and
    // until then a layer here would refuse requests the surface never serves.
    let app = super::routes::routes(state, runtime.pressure.clone());

    let listener =
        ServerListener::bind(&config.host, config.port, ApplicationProtocol::Http1, &listener_runtime).await?;
    tracing::info!("Prometheus API server listening on {}", listener.local_addr());

    let cancel_token = listener_runtime.cancel_token().clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("Prometheus server shutting down gracefully");
        })
        .await?;

    tracing::info!("Prometheus server stopped");

    Ok(())
}
