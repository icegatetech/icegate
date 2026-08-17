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

use std::{sync::Arc, time::Duration};

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::execution::context::SQLOptions;
use datafusion_flight_sql_server::service::FlightSqlService;
use icegate_common::{MemoryPressure, MemoryShedInterceptor};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;

use super::FlightSqlConfig;
use super::provider::IceGateSessionStateProvider;
use crate::engine::QueryEngine;
use crate::infra::deadline::ResponseDeadlineLayer;

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
    pressure: MemoryPressure,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with_port_tx(engine, config, cancel_token, None, pressure).await
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
    pressure: MemoryPressure,
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

    let query_deadline_secs = engine.config().max_query_duration_secs;
    let provider = Box::new(IceGateSessionStateProvider::new(engine));
    let service = FlightSqlService::new_with_provider(provider).with_sql_options(read_only_sql_options());
    let svc = FlightServiceServer::new(service)
        .max_decoding_message_size(config.max_message_size)
        .max_encoding_message_size(config.max_message_size);
    // Wrap the already-configured server so `InterceptedService` preserves the
    // codec size limits and `NamedService::NAME`; rejecting here happens at
    // HTTP/2 HEADERS time, before protobuf decode / session build.
    let intercepted = InterceptedService::new(svc, MemoryShedInterceptor::new(pressure, "flight_sql"));

    // `DoGet` streams, so the response future resolves long before the query
    // does: only a deadline that spans the BODY bounds how long a Flight SQL
    // query may hold a catalog provider (see `crate::infra::deadline`).
    let query_deadline = Duration::from_secs(query_deadline_secs);

    tonic::transport::Server::builder()
        // The layer covers both halves of a call — the response phase (planning,
        // and every unary RPC) and the stream — out of ONE budget, and answers
        // both with the same gRPC status. Deliberately no `Server::timeout`
        // alongside it: a second bound on the response phase would answer that
        // half with a status of its own, so the setting behind the deadline
        // would report differently depending on which half was running.
        .layer(ResponseDeadlineLayer::new(query_deadline))
        .add_service(intercepted)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            cancel_token.cancelled().await;
            tracing::info!("Flight SQL server shutting down gracefully...");
        })
        .await?;

    tracing::info!("Flight SQL server stopped");
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_flight::error::FlightError;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use icegate_common::testing::server_task::{DrainOutcome, PORT_BIND_TIMEOUT, SHUTDOWN_TIMEOUT, drain_server_task};
    use tonic::transport::Endpoint;

    use super::*;
    use crate::test_support::build_stalling_engine;

    /// Query deadline the test server runs with: the narrowest the engine config
    /// accepts, so the call ends in a second rather than in the default 30.
    const DEADLINE_SECS: u64 = 1;

    /// Outer bound on the RPC, an order of magnitude above the server's own
    /// deadline. It exists so a layer that stops answering fails the test
    /// instead of hanging the suite until CI kills the job.
    const RPC_TIMEOUT: Duration = Duration::from_secs(30);

    /// The query deadline over the RESPONSE phase, at the real gRPC boundary.
    /// The catalog behind the engine never answers, so `GetFlightInfo` can only
    /// end at the deadline — and it has to end with the status the layer
    /// defines, the same one a cut stream carries, rather than whatever a
    /// transport-level bound would report.
    #[tokio::test]
    async fn a_planning_phase_outliving_the_deadline_fails_with_deadline_exceeded() {
        let cancel_token = CancellationToken::new();
        let (port_tx, port_rx) = oneshot::channel();
        let server_token = cancel_token.clone();
        let mut server = tokio::spawn(async move {
            run_with_port_tx(
                build_stalling_engine(DEADLINE_SECS),
                FlightSqlConfig {
                    enabled: true,
                    host: "127.0.0.1".to_string(),
                    port: 0,
                    max_message_size: 16 * 1024 * 1024,
                },
                server_token,
                Some(port_tx),
                MemoryPressure::inert(),
            )
            .await
            .expect("the Flight SQL server runs until it is cancelled");
        });

        // Carried out as a value rather than asserted here: an unwind before the
        // drain below would leave the server task and its listener running for
        // the rest of the test binary.
        let outcome = plan_one_query(port_rx).await;

        cancel_token.cancel();
        assert_eq!(
            drain_server_task(&mut server, "Flight SQL").await,
            DrainOutcome::Finished,
            "the server must stop within {}s of the cancel",
            SHUTDOWN_TIMEOUT.as_secs()
        );

        let status = outcome.unwrap_or_else(|failure| panic!("{failure}"));
        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    }

    /// Plan one query against the server that reports its port on `port_rx`, and
    /// return the gRPC status the call fails with.
    ///
    /// Every failure is returned rather than asserted, so the caller can drain
    /// the server before failing the test. Both waits are bounded: the bind by
    /// [`PORT_BIND_TIMEOUT`], the call by [`RPC_TIMEOUT`].
    async fn plan_one_query(port_rx: oneshot::Receiver<u16>) -> Result<tonic::Status, String> {
        let port = tokio::time::timeout(PORT_BIND_TIMEOUT, port_rx)
            .await
            .map_err(|_elapsed| format!("the server did not bind within {}s", PORT_BIND_TIMEOUT.as_secs()))?
            .map_err(|_recv_error| "the server dropped the port channel before reporting a bound port".to_string())?;
        let channel = Endpoint::from_shared(format!("http://127.0.0.1:{port}"))
            .map_err(|error| format!("the bound port must form a valid endpoint URI: {error}"))?
            .connect()
            .await
            .map_err(|error| format!("the server must accept connections: {error}"))?;

        let planned = tokio::time::timeout(
            RPC_TIMEOUT,
            FlightSqlServiceClient::new(channel).execute("SELECT * FROM iceberg.icegate.logs".to_string(), None),
        )
        .await
        .map_err(|_elapsed| {
            format!(
                "the deadline must end planning well before the test's own {}s bound",
                RPC_TIMEOUT.as_secs()
            )
        })?;

        match planned {
            Ok(_info) => Err("planning past the deadline must not return flight info".to_string()),
            Err(FlightError::Tonic(status)) => Ok(*status),
            Err(other) => Err(format!("the deadline must arrive as a gRPC status, got: {other}")),
        }
    }
}
