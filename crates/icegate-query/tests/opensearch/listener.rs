//! The listener's start-up over real sockets.

use std::io::ErrorKind;

use icegate_common::ListenerRuntime;
use icegate_common::testing::server_task::PORT_BIND_TIMEOUT;
use icegate_query::infra::runtime::QueryRuntime;
use icegate_query::opensearch::{OpenSearchConfig, SystemObjectStore};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::harness::{build_in_memory_system_object_store, build_runtime};

/// The outcome of one start of the listener.
type StartOutcome = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Start the listener over `runtime` at `host:port` with its token already cancelled, so a start
/// that wrongly succeeds returns at once instead of serving forever. A start that blocks before
/// it binds fails the test after [`PORT_BIND_TIMEOUT`] instead of hanging it. Returns the outcome
/// and the port the listener reported, if any. `system_object_store` passes the startup checks, which
/// run before the bind, so the start fails at the address step under test.
async fn start_cancelled_listener(
    runtime: QueryRuntime,
    system_object_store: SystemObjectStore,
    host: &str,
    port: u16,
) -> (StartOutcome, Option<u16>) {
    let config = OpenSearchConfig {
        enabled: true,
        host: host.to_string(),
        port,
        ..OpenSearchConfig::default()
    };
    let cancel_token = CancellationToken::new();
    cancel_token.cancel();
    let (port_tx, port_rx) = oneshot::channel::<u16>();

    let listener_runtime = ListenerRuntime::plaintext(cancel_token);
    let start = icegate_query::opensearch::run_with_port_tx(
        runtime,
        config,
        system_object_store,
        listener_runtime,
        Some(port_tx),
    );
    let outcome = tokio::time::timeout(PORT_BIND_TIMEOUT, start).await.unwrap_or_else(|_elapsed| {
        panic!(
            "the start at {host}:{port} did not return within {}s",
            PORT_BIND_TIMEOUT.as_secs()
        )
    });

    // The start owned the sender and has returned, so this wait ends at once.
    (outcome, port_rx.await.ok())
}

/// A port another socket holds fails the start with `AddrInUse` and reports no port, so the
/// caller learns of it instead of waiting on a listener that never serves.
#[tokio::test]
async fn a_port_already_bound_fails_the_start_without_reporting_a_port() {
    let held_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("hold a port");
    let port = held_listener.local_addr().expect("held address").port();
    let (runtime, _catalog, _warehouse) = build_runtime().await.expect("the listener's runtime");
    let system_object_store = build_in_memory_system_object_store().expect("the in-memory system store");

    let (outcome, reported_port) = start_cancelled_listener(runtime, system_object_store, "127.0.0.1", port).await;

    let error = outcome.expect_err("the held port is refused");
    let io_kind = match error.downcast_ref::<icegate_common::Error>() {
        Some(icegate_common::Error::Io(io_error)) => Some(io_error.kind()),
        _ => None,
    };
    assert_eq!(io_kind, Some(ErrorKind::AddrInUse), "{error}");
    assert_eq!(reported_port, None);
}

/// Configuration validation accepts any non-blank host, so a host no listener can bind to is
/// refused only here, at start-up.
#[tokio::test]
async fn a_host_that_is_not_an_address_fails_the_start_without_reporting_a_port() {
    let (runtime, _catalog, _warehouse) = build_runtime().await.expect("the listener's runtime");
    let system_object_store = build_in_memory_system_object_store().expect("the in-memory system store");

    let (outcome, reported_port) = start_cancelled_listener(runtime, system_object_store, "not an address", 0).await;

    let error = outcome.expect_err("the host is refused");
    assert!(
        matches!(
            error.downcast_ref::<icegate_common::Error>(),
            Some(icegate_common::Error::Io(_))
        ),
        "{error}"
    );
    assert_eq!(reported_port, None);
}
