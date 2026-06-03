//! Flight SQL gRPC server bootstrap.
//!
//! Spins up a tonic server that hosts the upstream
//! `datafusion_flight_sql_server::service::FlightSqlService` with our
//! tenant-aware [`IceGateSessionStateProvider`]. Wiring mirrors the HTTP
//! servers ([`crate::loki::server::run`]) so the orchestration logic in
//! `cli::commands::run` can drive every server with one pattern.
//!
//! Unlike the HTTP servers, no [`crate::infra::metrics::QueryMetrics`] is
//! threaded in: the upstream `FlightSqlService` owns the request and
//! query-execution loop and exposes no hook to record the per-query
//! metrics (parse / plan / execute / rows / bytes) the HTTP handlers
//! emit. Wiring meaningful Flight SQL metrics needs an upstream hook or a
//! dedicated gRPC middleware layer and is tracked as a follow-up; an
//! unused `QueryMetrics` argument is deliberately not carried here so the
//! signature doesn't imply observability that isn't wired.

use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::execution::context::SQLOptions;
use datafusion_flight_sql_server::service::FlightSqlService;
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;

use super::FlightSqlConfig;
use super::provider::IceGateSessionStateProvider;
use crate::engine::QueryEngine;

/// Build the SQL execution options enforced on every client query.
///
/// We disable DDL (`CREATE`/`DROP`/`ALTER`) and DML
/// (`INSERT`/`UPDATE`/`DELETE`) — observability data is append-only via
/// the ingest path, so any write attempt from the query side is a bug.
///
/// `allow_statements` stays at its default (`true`) so analytics tooling
/// can issue `EXPLAIN`, `SHOW`, and `SET`. These remain read-only and
/// scoped to the per-request session.
fn read_only_sql_options() -> SQLOptions {
    SQLOptions::default().with_allow_ddl(false).with_allow_dml(false)
}

/// Start the Flight SQL gRPC server.
///
/// Mirrors [`crate::loki::server::run`] so the spawn site in
/// `cli::commands::run` does not need server-specific knowledge.
///
/// # Errors
///
/// Returns an error if the listener fails to bind or the underlying
/// tonic transport reports a fatal error.
pub async fn run(
    engine: Arc<QueryEngine>,
    config: FlightSqlConfig,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with_port_tx(engine, config, cancel_token, None).await
}

/// Variant of [`run`] that publishes the actually bound port on a
/// `oneshot` channel. Required for integration tests that bind to port 0
/// to avoid port-collision flakes in CI.
///
/// # Errors
///
/// Returns an error if the listener fails to bind or the underlying
/// tonic transport reports a fatal error.
pub async fn run_with_port_tx(
    engine: Arc<QueryEngine>,
    config: FlightSqlConfig,
    cancel_token: CancellationToken,
    port_tx: Option<oneshot::Sender<u16>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Bind via the `(host, port)` tuple so tonic resolves hostnames and
    // IPv6 literals through `ToSocketAddrs`. Parsing a `"host:port"`
    // string into a `SocketAddr` only accepts numeric IPs and would
    // reject `localhost` or a bracketless IPv6 host.
    let listener = TcpListener::bind((config.host.as_str(), config.port)).await?;
    let local_addr = listener.local_addr()?;
    tracing::info!(addr = %local_addr, "Flight SQL gRPC server listening");
    if let Some(tx) = port_tx {
        // Receiver gone is benign — the test simply isn't waiting on the
        // port any more.
        let _ = tx.send(local_addr.port());
    }

    let provider = Box::new(IceGateSessionStateProvider::new(engine));
    let service = FlightSqlService::new_with_provider(provider).with_sql_options(read_only_sql_options());
    let svc = FlightServiceServer::new(service)
        .max_decoding_message_size(config.max_message_size)
        .max_encoding_message_size(config.max_message_size);

    tonic::transport::Server::builder()
        .add_service(svc)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            cancel_token.cancelled().await;
            tracing::info!("Flight SQL server shutting down gracefully...");
        })
        .await?;

    tracing::info!("Flight SQL server stopped");
    Ok(())
}
