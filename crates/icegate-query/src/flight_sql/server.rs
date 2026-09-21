//! Flight SQL gRPC server bootstrap.
//!
//! Spins up a tonic server that hosts the upstream
//! `datafusion_flight_sql_server::service::FlightSqlService` with our
//! tenant-aware [`IceGateSessionStateProvider`]. Wiring mirrors the HTTP
//! servers ([`crate::loki::server::run`]) so the orchestration logic in
//! `cli::commands::run` can drive every server with one pattern.
//!
//! The [`QueryRuntime`] this server is started with carries
//! [`QueryMetrics`](crate::infra::metrics::QueryMetrics), and the tenant
//! interceptor records its refusals through them. Per-query metrics (parse /
//! plan / execute / rows / bytes) are still absent: the upstream
//! `FlightSqlService` owns the request and query-execution loop and exposes no
//! hook to record them. Wiring those needs an upstream hook or a dedicated gRPC
//! middleware layer and is tracked as a follow-up.

use std::{sync::Arc, time::Duration};

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::execution::context::SQLOptions;
use datafusion_flight_sql_server::service::FlightSqlService;
use icegate_common::{MemoryShedInterceptor, TenantPolicyInterceptor};
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;

use super::FlightSqlConfig;
use super::provider::IceGateSessionStateProvider;
use crate::infra::deadline::ResponseDeadlineLayer;
use crate::infra::{
    metrics::{PROTOCOL_FLIGHT_SQL, QueryTenantRejectionRecorder},
    runtime::QueryRuntime,
};

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
    runtime: QueryRuntime,
    config: FlightSqlConfig,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    run_with_port_tx(runtime, config, cancel_token, None).await
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
    runtime: QueryRuntime,
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

    let query_deadline_secs = runtime.engine.config().max_query_duration_secs;
    let provider = Box::new(IceGateSessionStateProvider::new(Arc::clone(&runtime.engine)));
    let service = FlightSqlService::new_with_provider(provider).with_sql_options(read_only_sql_options());
    let svc = FlightServiceServer::new(service)
        .max_decoding_message_size(config.max_message_size)
        .max_encoding_message_size(config.max_message_size);
    // Wrap the already-configured server so `InterceptedService` preserves the
    // codec size limits and `NamedService::NAME`; rejecting here happens at
    // HTTP/2 HEADERS time, before protobuf decode / session build. NOT codegen
    // `with_interceptor`, which rebuilds the service and reverts those limits.
    //
    // Nested, shed outermost: the outer interceptor runs first, and one atomic
    // read of the pressure flag is cheaper than reading the tenant metadata and
    // allocating the identifier. The OTLP/gRPC server and both HTTP routers
    // order their two layers the same way.
    let with_tenant = InterceptedService::new(
        svc,
        TenantPolicyInterceptor::new(
            runtime.tenant_resolver.clone(),
            QueryTenantRejectionRecorder::new(Arc::clone(&runtime.metrics)),
            PROTOCOL_FLIGHT_SQL,
        ),
    );
    let intercepted = InterceptedService::new(
        with_tenant,
        MemoryShedInterceptor::new(runtime.pressure.clone(), PROTOCOL_FLIGHT_SQL),
    );

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
    use icegate_common::{MemoryPressure, TenantPolicy, TenantResolver};
    use opentelemetry::metrics::MeterProvider as _;
    use tonic::transport::Endpoint;

    use super::*;
    use crate::{
        infra::metrics::{
            QueryMetrics,
            test_support::{build_meter_provider, find_counter_total},
        },
        test_support::{build_pressured_memory, build_stalling_engine},
    };

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
        let status = serve_and_plan_one_query(QueryRuntime {
            engine: build_stalling_engine(DEADLINE_SECS),
            metrics: Arc::new(QueryMetrics::new_disabled()),
            pressure: MemoryPressure::inert(),
            // The deployment's own tenant: this case is about the deadline, and
            // its client names no tenant.
            tenant_resolver: TenantPolicy::default().into_resolver().expect("the default policy resolves"),
        })
        .await;

        assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    }

    /// Under memory pressure the shed interceptor answers first, whatever the
    /// tenant policy would have said: `run_with_port_tx` nests it outside the
    /// tenant interceptor, and a client sees that order only through which
    /// status arrives — `RESOURCE_EXHAUSTED` from `MemoryShedInterceptor`
    /// rather than `INVALID_ARGUMENT` from the tenant refusal. The client names
    /// no tenant, so under `multi` the tenant interceptor would refuse it too.
    #[tokio::test]
    async fn a_request_without_a_tenant_is_shed_before_the_tenant_interceptor() {
        let status = serve_and_plan_one_query(QueryRuntime {
            engine: build_stalling_engine(DEADLINE_SECS),
            metrics: Arc::new(QueryMetrics::new_disabled()),
            pressure: build_pressured_memory(),
            tenant_resolver: TenantResolver::Multi,
        })
        .await;

        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    }

    /// The refusal is counted under this surface's own protocol label, through
    /// the metrics of the runtime the server was started with. Driven through
    /// the server because what could break is the constant and the recorder
    /// this call site hands the interceptor.
    #[tokio::test]
    async fn a_refused_rpc_counts_a_rejection_for_this_protocol() {
        let (provider, exporter) = build_meter_provider();

        let status = serve_and_plan_one_query(QueryRuntime {
            engine: build_stalling_engine(DEADLINE_SECS),
            metrics: Arc::new(QueryMetrics::new(&provider.meter("flight_sql_tenant_rejections"))),
            pressure: MemoryPressure::inert(),
            tenant_resolver: TenantResolver::Multi,
        })
        .await;

        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        provider.force_flush().expect("failed to flush metrics");
        assert_eq!(
            find_counter_total(
                &exporter,
                "icegate_query_tenant_rejections",
                &[("protocol", "flight_sql"), ("reason", "missing")],
            ),
            1
        );
    }

    /// Start a server over `runtime` on port `0`, plan one query naming no
    /// tenant, then cancel the server and wait for it to stop.
    ///
    /// Returns the gRPC status the call failed with. Panics, only once the
    /// server and its listener are gone, when the server did not stop within
    /// [`SHUTDOWN_TIMEOUT`] or the call did not end in a gRPC status.
    async fn serve_and_plan_one_query(runtime: QueryRuntime) -> tonic::Status {
        let cancel_token = CancellationToken::new();
        let (port_tx, port_rx) = oneshot::channel();
        let server_token = cancel_token.clone();
        let mut server = tokio::spawn(async move {
            run_with_port_tx(
                runtime,
                FlightSqlConfig {
                    enabled: true,
                    host: "127.0.0.1".to_string(),
                    port: 0,
                    max_message_size: 16 * 1024 * 1024,
                },
                server_token,
                Some(port_tx),
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

        outcome.unwrap_or_else(|failure| panic!("{failure}"))
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
                "the call must end within the test's own {}s bound",
                RPC_TIMEOUT.as_secs()
            )
        })?;

        match planned {
            Ok(_info) => Err("the call must fail, not return flight info".to_string()),
            Err(FlightError::Tonic(status)) => Ok(*status),
            Err(other) => Err(format!("the call must fail with a gRPC status, got: {other}")),
        }
    }
}
