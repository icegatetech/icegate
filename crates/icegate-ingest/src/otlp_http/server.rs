//! OTLP HTTP server implementation

use icegate_common::{ApplicationProtocol, ListenerRuntime, MemoryPressure, ServerListener, TenantResolver};
use icegate_queue::WriteChannel;

use super::OtlpHttpConfig;
use crate::infra::metrics::OtlpMetrics;

/// Shared application state for OTLP HTTP server
#[derive(Clone)]
pub struct OtlpHttpState {
    /// Write channel for sending batches to the WAL queue.
    pub write_channel: WriteChannel,
    /// Maximum number of rows per WAL row group.
    pub wal_row_group_size: usize,
    /// Whether to fork the best-effort `operations` projection from traces.
    pub operations_enabled: bool,
    /// Metrics recorder for OTLP intake.
    pub metrics: OtlpMetrics,
}

/// Run the OTLP HTTP server
///
/// Binds `config.host:config.port` and serves over TLS, offering ALPN `http/1.1`, when
/// `runtime` carries a certificate, and plaintext otherwise. Returns once the runtime's token
/// is cancelled and the server has drained.
///
/// # Errors
///
/// Returns an error if the server cannot be started or encounters a fatal error
pub async fn run(
    state: OtlpHttpState,
    config: OtlpHttpConfig,
    runtime: ListenerRuntime,
    memory_pressure: MemoryPressure,
    tenant_resolver: TenantResolver,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = super::routes::routes(state, config.max_body_bytes, memory_pressure, tenant_resolver);

    let listener = ServerListener::bind(&config.host, config.port, ApplicationProtocol::Http1, &runtime).await?;
    tracing::info!("OTLP HTTP server listening on {}", listener.local_addr());

    let cancel_token = runtime.cancel_token().clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            tracing::info!("OTLP HTTP server shutting down gracefully");
        })
        .await?;

    tracing::info!("OTLP HTTP server stopped");

    Ok(())
}
