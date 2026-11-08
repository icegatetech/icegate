//! `OpenSearch` listener: bind, serve, graceful shutdown.

use std::sync::Arc;
use std::time::Duration;

use icegate_common::{ApplicationProtocol, ListenerRuntime, ServerListener};
use tokio::sync::oneshot;

use super::catalog::{CatalogSettings, IndexCatalog};
use super::config::{EmulatedVersion, OpenSearchConfig};
use super::node::NodeIdentity;
use super::state::OpenSearchState;
use super::system_index::{
    SystemIndexLimits, SystemIndexStore, SystemObjectStore, probe_conditional_operations, refuse_table_location_overlap,
};
use crate::infra::runtime::QueryRuntime;

/// Serve the `OpenSearch`-compatible API until the token of `listener_runtime` is cancelled.
///
/// Before binding, the system-index store's location is checked against every table location the
/// catalog lists, and its conditional operations are probed; either failure stops startup with an
/// error naming the cause. The listener binds `config.host:config.port` and serves over TLS,
/// offering ALPN `http/1.1`, when `listener_runtime` carries a certificate, and plaintext otherwise.
///
/// # Errors
///
/// Returns the startup check's error; an error when the configured version does not parse
/// (configuration validation refuses it earlier); or the error binding or serving the listener
/// fails with.
pub async fn run(
    runtime: QueryRuntime,
    config: OpenSearchConfig,
    system_object_store: SystemObjectStore,
    listener_runtime: ListenerRuntime,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with_port_tx(runtime, config, system_object_store, listener_runtime, None).await
}

/// [`run`], reporting the bound port on `port_tx` once the listener is bound, so a caller can bind
/// port 0 and learn the port.
///
/// Before binding, the system-index store's location is checked against every table location the
/// catalog lists, and its conditional operations are probed; either failure stops startup with an
/// error naming the cause, and `port_tx` is dropped without a port.
///
/// # Errors
///
/// As [`run`].
pub async fn run_with_port_tx(
    runtime: QueryRuntime,
    config: OpenSearchConfig,
    system_object_store: SystemObjectStore,
    listener_runtime: ListenerRuntime,
    port_tx: Option<oneshot::Sender<u16>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    refuse_table_location_overlap(&system_object_store, runtime.engine.catalog().as_ref()).await?;
    probe_conditional_operations(&system_object_store).await?;
    tracing::info!(
        location = system_object_store.location(),
        "OpenSearch system-index store passed its startup checks"
    );
    let system_index_store = Arc::new(SystemIndexStore::new(
        system_object_store,
        SystemIndexLimits {
            max_snapshot_bytes: config.system_indices.max_snapshot_bytes,
            cache_max_bytes: config.system_indices.cache_max_bytes,
            write_deadline: Duration::from_secs(runtime.engine.config().max_query_duration_secs),
            max_scroll_lifetime: Duration::from_secs(config.system_indices.max_scroll_lifetime_secs),
        },
    ));
    let version = EmulatedVersion::parse(&config.version)?;
    let listener =
        ServerListener::bind(&config.host, config.port, ApplicationProtocol::Http1, &listener_runtime).await?;
    let local_address = listener.local_addr();
    let state = OpenSearchState {
        node: Arc::new(NodeIdentity::new(&config.cluster_name, version, local_address)),
        catalog: IndexCatalog::new(
            Arc::clone(&runtime.engine),
            Arc::clone(&runtime.metrics),
            Arc::clone(&system_index_store),
            CatalogSettings {
                field_discovery_window: Duration::from_secs(config.field_discovery_window_secs),
                max_query_memory_bytes: config.max_query_memory_bytes,
            },
        ),
        system_store: system_index_store,
        max_answer_bytes: config.system_indices.max_answer_bytes,
        search_limits: config.search,
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
