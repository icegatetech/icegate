//! Serving plaintext and TLS through one listener, with the certificate replaced under
//! open connections.
//!
//! An integration test because every case binds a real TCP port, and the TLS cases load
//! their certificate from PEM files in a temporary directory.
//!
//! ```text
//! cargo test -p icegate-common --features grpc --test tls_listener
//! ```
#![allow(clippy::expect_used)]

use std::fs;
use std::io::{ErrorKind, Read};
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use futures::StreamExt;
use icegate_common::testing::server_task::{DrainOutcome, PORT_BIND_TIMEOUT, SHUTDOWN_TIMEOUT, drain_server_task};
use icegate_common::{
    ApplicationProtocol, CancellationToken, CertificateReload, Connection, DEFAULT_TLS_RELOAD_INTERVAL_SECS,
    HEALTH_PATH, ListenerRuntime, OperationalConfig, ServerCertificate, ServerListener, TlsConfig,
    run_operational_server, start_listener_runtime,
};
use rcgen::{CertificateParams, KeyPair};
use rustls::RootCertStore;
use rustls::crypto::aws_lc_rs;
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tonic::transport::server::Connected;

const CERTIFICATE_FILE: &str = "tls.crt";
const PRIVATE_KEY_FILE: &str = "tls.key";

/// Bound on every client step. Generous, so only a server that never answers reaches it.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a replaced pair may take to be served, with a reload interval of one second.
/// Several intervals, so only a watcher that never ran reaches it.
const RELOAD_TIMEOUT: Duration = Duration::from_secs(10);

/// The status line of a successful `GET` (RFC 9112), as the server must write it.
const OK_STATUS_LINE: &str = "HTTP/1.1 200 OK";

const ALPN_H2: &[u8] = b"h2";
const ALPN_HTTP_1_1: &[u8] = b"http/1.1";

/// Write a fresh self-signed pair for `localhost` into `dir`, replacing any pair there.
///
/// Returns the leaf as the generator emitted it, so a handshake is checked against bytes
/// that never went through the PEM decoding of the code under test.
fn write_pair(dir: &Path) -> CertificateDer<'static> {
    let key_pair = KeyPair::generate().expect("generate key pair");
    let certificate = CertificateParams::new(vec!["localhost".to_string()])
        .expect("certificate params")
        .self_signed(&key_pair)
        .expect("self-sign certificate");
    fs::write(dir.join(CERTIFICATE_FILE), certificate.pem()).expect("write certificate");
    fs::write(dir.join(PRIVATE_KEY_FILE), key_pair.serialize_pem()).expect("write private key");
    certificate.der().clone()
}

fn load_certificate(dir: &Path) -> Arc<ServerCertificate> {
    let config = TlsConfig {
        certificate_path: dir.join(CERTIFICATE_FILE),
        private_key_path: dir.join(PRIVATE_KEY_FILE),
        reload_interval_secs: DEFAULT_TLS_RELOAD_INTERVAL_SECS,
    };
    Arc::new(ServerCertificate::load(&config).expect("load the pair"))
}

/// A client trusting exactly `trusted_leaves` and offering `alpn_protocols`, most preferred
/// first.
fn build_connector(trusted_leaves: &[&CertificateDer<'static>], alpn_protocols: &[&[u8]]) -> TlsConnector {
    let mut roots = RootCertStore::empty();
    for leaf in trusted_leaves {
        roots.add((*leaf).clone()).expect("trust the certificate");
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = alpn_protocols.iter().map(|protocol| protocol.to_vec()).collect();
    TlsConnector::from(Arc::new(config))
}

async fn connect_tcp(addr: SocketAddr) -> TcpStream {
    tokio::time::timeout(CLIENT_TIMEOUT, TcpStream::connect(addr))
        .await
        .expect("the connection opens in time")
        .expect("connect")
}

async fn connect_tls(connector: &TlsConnector, addr: SocketAddr) -> TlsStream<TcpStream> {
    let tcp_stream = connect_tcp(addr).await;
    let server_name = ServerName::try_from("localhost").expect("server name");
    tokio::time::timeout(CLIENT_TIMEOUT, connector.connect(server_name, tcp_stream))
        .await
        .expect("the handshake completes in time")
        .expect("TLS handshake")
}

fn read_presented_leaf(stream: &TlsStream<TcpStream>) -> CertificateDer<'static> {
    let (_, connection) = stream.get_ref();
    connection
        .peer_certificates()
        .and_then(|chain| chain.first())
        .expect("the server presents a certificate")
        .clone()
}

fn read_negotiated_protocol(stream: &TlsStream<TcpStream>) -> Option<Vec<u8>> {
    let (_, connection) = stream.get_ref();
    connection.alpn_protocol().map(<[u8]>::to_vec)
}

fn build_health_request() -> String {
    format!("GET {HEALTH_PATH} HTTP/1.1\r\nHost: localhost\r\n\r\n")
}

/// Send one `GET` of the health path on `stream` and return the status line of the reply.
///
/// Reads exactly one response, headers and `content-length` body, so the connection is left
/// ready to carry another request.
async fn request_health<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S) -> String {
    let exchange = async {
        stream
            .write_all(build_health_request().as_bytes())
            .await
            .expect("send the request");
        let mut response = Vec::new();
        let mut chunk = [0_u8; 1024];
        let head_length = loop {
            if let Some(position) = response.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
            let read = stream.read(&mut chunk).await.expect("read the response head");
            assert_ne!(
                read, 0,
                "the server closed the connection before the response head ended"
            );
            response.extend_from_slice(&chunk[..read]);
        };
        let head = std::str::from_utf8(&response[..head_length])
            .expect("an ASCII response head")
            .to_string();
        let body_length: usize = head
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().expect("a numeric content-length"))
            })
            .expect("a content-length header");
        while response.len() < head_length + body_length {
            let read = stream.read(&mut chunk).await.expect("read the response body");
            assert_ne!(read, 0, "the server closed the connection before the body ended");
            response.extend_from_slice(&chunk[..read]);
        }
        head.lines().next().expect("a status line").to_string()
    };
    tokio::time::timeout(CLIENT_TIMEOUT, exchange)
        .await
        .expect("the response arrives in time")
}

/// A health route served through a [`ServerListener`] on a port of its own.
///
/// The token is cancelled when this is dropped, so a failing test does not leave the
/// server running; [`Self::stop`] also drains the task.
struct HealthServer {
    addr: SocketAddr,
    cancel_token: CancellationToken,
    task: JoinHandle<()>,
}

impl HealthServer {
    async fn start(protocol: ApplicationProtocol, certificate: Option<Arc<ServerCertificate>>) -> Self {
        Self::start_with_runtime(protocol, ListenerRuntime::new(CancellationToken::new(), certificate)).await
    }

    async fn start_with_runtime(protocol: ApplicationProtocol, runtime: ListenerRuntime) -> Self {
        let cancel_token = runtime.cancel_token().clone();
        let listener = ServerListener::bind("127.0.0.1", 0, protocol, &runtime)
            .await
            .expect("bind the listener");
        let addr = listener.local_addr();
        let router = Router::new().route(HEALTH_PATH, get(|| async { "ok" }));
        let shutdown_token = cancel_token.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move { shutdown_token.cancelled().await })
                .await
                .expect("the server stops without an error");
        });
        Self {
            addr,
            cancel_token,
            task,
        }
    }

    async fn stop(mut self) -> DrainOutcome {
        self.cancel_token.cancel();
        drain_server_task(&mut self.task, "health").await
    }
}

impl Drop for HealthServer {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

/// Wait until connecting to `addr` is refused, which is what a released port answers.
async fn wait_for_released_port(addr: SocketAddr) {
    let wait = async {
        while TcpStream::connect(addr).await.is_ok() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(SHUTDOWN_TIMEOUT, wait)
        .await
        .expect("the port is released once the runtime is cancelled");
}

#[tokio::test]
async fn a_plain_listener_serves_health_without_tls() {
    let server = HealthServer::start(ApplicationProtocol::Http1, None).await;
    let mut stream = connect_tcp(server.addr).await;

    let status_line = request_health(&mut stream).await;

    assert_eq!(status_line, OK_STATUS_LINE);
    drop(stream);
    assert_eq!(server.stop().await, DrainOutcome::Finished);
}

/// Drives `run_operational_server` itself, which reports no port: the port is reserved by
/// binding and releasing it, and readiness is the first connection that succeeds.
#[tokio::test]
async fn a_tls_listener_serves_health_with_the_loaded_certificate() {
    let dir = tempfile::tempdir().expect("temp dir");
    let leaf = write_pair(dir.path());
    let certificate = load_certificate(dir.path());
    let port = {
        let reserved = TcpListener::bind("127.0.0.1:0").await.expect("reserve a port");
        reserved.local_addr().expect("the reserved address").port()
    };
    let config = OperationalConfig {
        host: "127.0.0.1".to_string(),
        port,
        ..OperationalConfig::default()
    };
    let cancel_token = CancellationToken::new();
    let _cancel_on_drop = cancel_token.clone().drop_guard();
    let runtime = ListenerRuntime::new(cancel_token.clone(), Some(certificate));
    let mut task = tokio::spawn(async move {
        run_operational_server(config, None, runtime)
            .await
            .expect("the operational server stops without an error");
    });
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let ready = async {
        loop {
            if let Ok(tcp_stream) = TcpStream::connect(addr).await {
                return tcp_stream;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    let tcp_stream = tokio::time::timeout(PORT_BIND_TIMEOUT, ready)
        .await
        .expect("the operational server binds its port in time");
    let server_name = ServerName::try_from("localhost").expect("server name");
    let mut stream = tokio::time::timeout(
        CLIENT_TIMEOUT,
        build_connector(&[&leaf], &[]).connect(server_name, tcp_stream),
    )
    .await
    .expect("the handshake completes in time")
    .expect("TLS handshake");

    let status_line = request_health(&mut stream).await;

    assert_eq!(read_presented_leaf(&stream), leaf);
    assert_eq!(status_line, OK_STATUS_LINE);
    drop(stream);
    cancel_token.cancel();
    assert_eq!(
        drain_server_task(&mut task, "operational").await,
        DrainOutcome::Finished
    );
}

#[tokio::test]
async fn a_new_connection_sees_the_replaced_certificate_while_an_open_one_keeps_working() {
    let dir = tempfile::tempdir().expect("temp dir");
    let first_leaf = write_pair(dir.path());
    let certificate = load_certificate(dir.path());
    let server = HealthServer::start(ApplicationProtocol::Http1, Some(Arc::clone(&certificate))).await;
    let mut open_connection = connect_tls(&build_connector(&[&first_leaf], &[]), server.addr).await;
    assert_eq!(request_health(&mut open_connection).await, OK_STATUS_LINE);

    let second_leaf = write_pair(dir.path());
    assert_eq!(certificate.reload().expect("reload"), CertificateReload::Replaced);
    // Trusting only the second leaf makes the handshake itself fail if the first is served.
    let mut new_connection = connect_tls(&build_connector(&[&second_leaf], &[]), server.addr).await;

    assert_eq!(read_presented_leaf(&open_connection), first_leaf);
    assert_eq!(read_presented_leaf(&new_connection), second_leaf);
    assert_eq!(request_health(&mut new_connection).await, OK_STATUS_LINE);
    assert_eq!(request_health(&mut open_connection).await, OK_STATUS_LINE);
    drop((open_connection, new_connection));
    assert_eq!(server.stop().await, DrainOutcome::Finished);
}

/// Through the stream tonic serves from, so the yielded connection and the addresses it
/// reports are checked too.
#[tokio::test]
async fn a_grpc_listener_negotiates_h2() {
    let dir = tempfile::tempdir().expect("temp dir");
    let leaf = write_pair(dir.path());
    let cancel_token = CancellationToken::new();
    let _cancel_on_drop = cancel_token.clone().drop_guard();
    let runtime = ListenerRuntime::new(cancel_token, Some(load_certificate(dir.path())));
    let listener = ServerListener::bind("127.0.0.1", 0, ApplicationProtocol::Grpc, &runtime)
        .await
        .expect("bind the listener");
    let addr = listener.local_addr();
    let mut incoming = pin!(listener.into_incoming());

    let client = connect_tls(&build_connector(&[&leaf], &[ALPN_H2, ALPN_HTTP_1_1]), addr).await;
    let served = tokio::time::timeout(CLIENT_TIMEOUT, incoming.next())
        .await
        .expect("the connection is handed over in time")
        .expect("the stream is open")
        .expect("an accepted connection");

    assert_eq!(read_negotiated_protocol(&client), Some(ALPN_H2.to_vec()));
    assert!(matches!(served, Connection::Tls(_)), "got {served:?}");
    let client_addr = client.get_ref().0.local_addr().expect("the client address");
    assert_eq!(served.connect_info().remote_addr(), Some(client_addr));
}

#[tokio::test]
async fn a_plain_grpc_listener_yields_a_plain_connection_with_its_peer_address() {
    let cancel_token = CancellationToken::new();
    let _cancel_on_drop = cancel_token.clone().drop_guard();
    let runtime = ListenerRuntime::plaintext(cancel_token);
    let listener = ServerListener::bind("127.0.0.1", 0, ApplicationProtocol::Grpc, &runtime)
        .await
        .expect("bind the listener");
    let addr = listener.local_addr();
    let mut incoming = pin!(listener.into_incoming());

    let client = connect_tcp(addr).await;
    let served = tokio::time::timeout(CLIENT_TIMEOUT, incoming.next())
        .await
        .expect("the connection is handed over in time")
        .expect("the stream is open")
        .expect("an accepted connection");

    assert!(matches!(served, Connection::Plain(_)), "got {served:?}");
    let client_addr = client.local_addr().expect("the client address");
    assert_eq!(served.connect_info().remote_addr(), Some(client_addr));
}

#[tokio::test]
async fn an_http_listener_negotiates_http_1_1() {
    let dir = tempfile::tempdir().expect("temp dir");
    let leaf = write_pair(dir.path());
    let server = HealthServer::start(ApplicationProtocol::Http1, Some(load_certificate(dir.path()))).await;
    let mut stream = connect_tls(&build_connector(&[&leaf], &[ALPN_H2, ALPN_HTTP_1_1]), server.addr).await;

    let status_line = request_health(&mut stream).await;

    assert_eq!(read_negotiated_protocol(&stream), Some(ALPN_HTTP_1_1.to_vec()));
    assert_eq!(status_line, OK_STATUS_LINE);
    drop(stream);
    assert_eq!(server.stop().await, DrainOutcome::Finished);
}

#[tokio::test]
async fn a_stalled_handshake_does_not_block_other_connections() {
    let dir = tempfile::tempdir().expect("temp dir");
    let leaf = write_pair(dir.path());
    let server = HealthServer::start(ApplicationProtocol::Http1, Some(load_certificate(dir.path()))).await;
    // Opened first and never sends a ClientHello, so it is ahead of the second client in
    // the accept queue and its handshake cannot finish.
    let stalled = connect_tcp(server.addr).await;

    let mut stream = tokio::time::timeout(
        Duration::from_secs(1),
        connect_tls(&build_connector(&[&leaf], &[]), server.addr),
    )
    .await
    .expect("a second client completes its handshake while the first one stalls");

    // A read straight on the socket answers "would block" only while the server still
    // holds the stalled connection open, so its handshake was still pending throughout.
    let mut stalled = stalled.into_std().expect("a std socket");
    let stalled_read = stalled.read(&mut [0_u8; 1]);
    assert!(
        matches!(&stalled_read, Err(error) if error.kind() == ErrorKind::WouldBlock),
        "got {stalled_read:?}"
    );
    assert_eq!(request_health(&mut stream).await, OK_STATUS_LINE);
    drop((stalled, stream));
    assert_eq!(server.stop().await, DrainOutcome::Finished);
}

/// The timeout is reached on the paused clock, which jumps to the next deadline whenever
/// every task waits, so the test does not sit through the real handshake budget.
#[tokio::test]
async fn a_stalled_handshake_is_dropped_once_it_times_out() {
    let dir = tempfile::tempdir().expect("temp dir");
    let leaf = write_pair(dir.path());
    let server = HealthServer::start(ApplicationProtocol::Http1, Some(load_certificate(dir.path()))).await;
    let mut stalled = connect_tcp(server.addr).await;
    // Accepted after the stalled connection, and handshake tasks run in the order they
    // were spawned on this single-threaded runtime: once this one has completed, the
    // stalled handshake has started and its deadline is set. Only then may the clock jump.
    let mut stream = connect_tls(&build_connector(&[&leaf], &[]), server.addr).await;
    assert_eq!(request_health(&mut stream).await, OK_STATUS_LINE);
    tokio::time::pause();
    let paused_at = tokio::time::Instant::now();

    let read = tokio::time::timeout(Duration::from_secs(60), stalled.read(&mut [0_u8; 1]))
        .await
        .expect("the server closes the stalled connection before the test's own bound");

    assert_eq!(read.expect("read the stalled connection"), 0, "the server closed it");
    assert!(
        paused_at.elapsed() >= Duration::from_secs(1),
        "closed by a deadline, not at once"
    );
    drop((stalled, stream));
    assert_eq!(server.stop().await, DrainOutcome::Finished);
}

#[tokio::test]
async fn a_plaintext_request_to_a_tls_listener_is_dropped_and_serving_continues() {
    let dir = tempfile::tempdir().expect("temp dir");
    let leaf = write_pair(dir.path());
    let server = HealthServer::start(ApplicationProtocol::Http1, Some(load_certificate(dir.path()))).await;
    let mut plain = connect_tcp(server.addr).await;
    plain
        .write_all(build_health_request().as_bytes())
        .await
        .expect("send the request");

    let mut reply = Vec::new();
    let closed = tokio::time::timeout(CLIENT_TIMEOUT, plain.read_to_end(&mut reply))
        .await
        .expect("the server closes the connection in time");

    // The server may close with the rest of the request unread, which resets the
    // connection instead of ending it; either way no HTTP response comes back.
    match closed {
        Ok(_) => assert!(!reply.starts_with(b"HTTP/"), "got an HTTP response: {reply:?}"),
        Err(error) => assert_eq!(error.kind(), ErrorKind::ConnectionReset, "got {error:?}"),
    }
    let mut stream = connect_tls(&build_connector(&[&leaf], &[]), server.addr).await;
    assert_eq!(request_health(&mut stream).await, OK_STATUS_LINE);
    drop(stream);
    assert_eq!(server.stop().await, DrainOutcome::Finished);
}

#[tokio::test]
async fn cancelling_the_runtime_stops_accepting() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_pair(dir.path());
    let server = HealthServer::start(ApplicationProtocol::Http1, Some(load_certificate(dir.path()))).await;
    let addr = server.addr;

    let outcome = server.stop().await;

    assert_eq!(outcome, DrainOutcome::Finished);
    wait_for_released_port(addr).await;
}

/// The token alone stops a TLS listener: the listener is still held, so the port can only
/// be released by the accept task ending on cancellation.
#[tokio::test]
async fn cancelling_the_runtime_releases_the_port_of_a_tls_listener_still_held() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_pair(dir.path());
    let cancel_token = CancellationToken::new();
    let runtime = ListenerRuntime::new(cancel_token.clone(), Some(load_certificate(dir.path())));
    let listener = ServerListener::bind("127.0.0.1", 0, ApplicationProtocol::Http1, &runtime)
        .await
        .expect("bind the listener");
    let addr = listener.local_addr();

    cancel_token.cancel();

    wait_for_released_port(addr).await;
    drop(listener);
}

/// The bracketed form is how `::1` is written inside a `host:port` string, which an operator
/// may carry over into the `host` key.
#[tokio::test]
async fn an_ipv6_host_binds_both_bare_and_in_brackets() {
    let cancel_token = CancellationToken::new();
    let _cancel_on_drop = cancel_token.clone().drop_guard();
    let runtime = ListenerRuntime::plaintext(cancel_token);

    for host in ["::1", "[::1]"] {
        let listener = ServerListener::bind(host, 0, ApplicationProtocol::Http1, &runtime)
            .await
            .unwrap_or_else(|error| panic!("bind {host}: {error}"));

        assert_eq!(
            listener.local_addr().ip(),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            "host {host}"
        );
    }
}

fn read_nodelay(connection: &Connection) -> bool {
    let tcp_stream = match connection {
        Connection::Plain(stream) => stream,
        Connection::Tls(stream) => stream.get_ref().0,
    };
    tcp_stream.nodelay().expect("read TCP_NODELAY")
}

/// `TCP_NODELAY` of a connection accepted by a bare socket, which no listener of ours
/// touched: what "unchanged" means for this platform.
async fn read_default_nodelay() -> bool {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind a bare socket");
    let addr = listener.local_addr().expect("the bare socket's address");
    let (client, accepted) = tokio::join!(connect_tcp(addr), listener.accept());
    let (tcp_stream, _) = accepted.expect("accept on the bare socket");
    drop(client);
    tcp_stream.nodelay().expect("read TCP_NODELAY")
}

/// One server-side connection from a listener bound with `certificate`, taken the way the
/// servers of `protocol` take theirs: tonic's stream for gRPC, axum's `accept` for HTTP/1.1.
/// The client is returned too, so the connection stays open while it is inspected.
async fn accept_one_connection(
    protocol: ApplicationProtocol,
    certificate: Option<(Arc<ServerCertificate>, &CertificateDer<'static>)>,
) -> (Connection, Box<dyn std::any::Any + Send>) {
    let cancel_token = CancellationToken::new();
    let runtime = ListenerRuntime::new(
        cancel_token.clone(),
        certificate.as_ref().map(|(served, _)| Arc::clone(served)),
    );
    let mut listener = ServerListener::bind("127.0.0.1", 0, protocol, &runtime)
        .await
        .expect("bind the listener");
    let addr = listener.local_addr();
    let client: Box<dyn std::any::Any + Send> = match &certificate {
        Some((_, leaf)) => Box::new(connect_tls(&build_connector(&[leaf], &[]), addr).await),
        None => Box::new(connect_tcp(addr).await),
    };
    let served = match protocol {
        ApplicationProtocol::Grpc => tokio::time::timeout(CLIENT_TIMEOUT, pin!(listener.into_incoming()).next())
            .await
            .expect("the connection is handed over in time")
            .expect("the stream is open")
            .expect("an accepted connection"),
        ApplicationProtocol::Http1 => {
            tokio::time::timeout(CLIENT_TIMEOUT, axum::serve::Listener::accept(&mut listener))
                .await
                .expect("the connection is handed over in time")
                .0
        }
    };
    cancel_token.cancel();
    (served, client)
}

#[tokio::test]
async fn a_grpc_listener_sets_nodelay_on_plain_and_tls_connections() {
    let dir = tempfile::tempdir().expect("temp dir");
    let leaf = write_pair(dir.path());
    let certificate = load_certificate(dir.path());

    let (plain, _plain_client) = accept_one_connection(ApplicationProtocol::Grpc, None).await;
    let (tls, _tls_client) = accept_one_connection(ApplicationProtocol::Grpc, Some((certificate, &leaf))).await;

    assert!(matches!(plain, Connection::Plain(_)), "got {plain:?}");
    assert!(read_nodelay(&plain), "plain gRPC connection");
    assert!(matches!(tls, Connection::Tls(_)), "got {tls:?}");
    assert!(read_nodelay(&tls), "TLS gRPC connection");
}

#[tokio::test]
async fn an_http_listener_leaves_nodelay_as_accepted() {
    let dir = tempfile::tempdir().expect("temp dir");
    let leaf = write_pair(dir.path());
    let certificate = load_certificate(dir.path());
    let default_nodelay = read_default_nodelay().await;

    let (plain, _plain_client) = accept_one_connection(ApplicationProtocol::Http1, None).await;
    let (tls, _tls_client) = accept_one_connection(ApplicationProtocol::Http1, Some((certificate, &leaf))).await;

    assert!(matches!(plain, Connection::Plain(_)), "got {plain:?}");
    assert_eq!(read_nodelay(&plain), default_nodelay, "plain HTTP/1.1 connection");
    assert!(matches!(tls, Connection::Tls(_)), "got {tls:?}");
    assert_eq!(read_nodelay(&tls), default_nodelay, "TLS HTTP/1.1 connection");
}

/// Without cancellation, dropping a TLS listener stops its accept task: nothing would be
/// left to take the connections it accepts.
#[tokio::test]
async fn dropping_a_tls_listener_releases_its_port() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_pair(dir.path());
    let cancel_token = CancellationToken::new();
    let _cancel_on_drop = cancel_token.clone().drop_guard();
    let runtime = ListenerRuntime::new(cancel_token, Some(load_certificate(dir.path())));
    let listener = ServerListener::bind("127.0.0.1", 0, ApplicationProtocol::Http1, &runtime)
        .await
        .expect("bind the listener");
    let addr = listener.local_addr();

    drop(listener);

    wait_for_released_port(addr).await;
}

/// Startup stops on a pair that cannot be loaded, here a Secret that is not mounted, rather
/// than falling back to the plaintext listeners that an absent `tls` block means.
#[tokio::test]
async fn start_listener_runtime_rejects_a_tls_block_naming_missing_files() {
    let dir = tempfile::tempdir().expect("temp dir");
    let tls = TlsConfig {
        certificate_path: dir.path().join("missing.crt"),
        private_key_path: dir.path().join("missing.key"),
        reload_interval_secs: DEFAULT_TLS_RELOAD_INTERVAL_SECS,
    };

    let result = start_listener_runtime(Some(&tls), &CancellationToken::new());

    assert!(matches!(result, Err(icegate_common::Error::Tls(_))), "got {result:?}");
}

/// The reload task needs a Tokio runtime to run on. Called from a plain thread, with a pair
/// that loads, the missing runtime is the only thing that can fail, and it must come back as
/// an error the caller can act on rather than as a panic.
#[test]
fn start_listener_runtime_outside_a_tokio_runtime_returns_an_error() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_pair(dir.path());
    let tls = TlsConfig {
        certificate_path: dir.path().join(CERTIFICATE_FILE),
        private_key_path: dir.path().join(PRIVATE_KEY_FILE),
        reload_interval_secs: DEFAULT_TLS_RELOAD_INTERVAL_SECS,
    };

    let result = start_listener_runtime(Some(&tls), &CancellationToken::new());

    assert!(matches!(result, Err(icegate_common::Error::Tls(_))), "got {result:?}");
}

#[tokio::test]
async fn start_listener_runtime_without_a_tls_block_serves_plaintext() {
    let runtime = start_listener_runtime(None, &CancellationToken::new()).expect("a plaintext runtime");
    let server = HealthServer::start_with_runtime(ApplicationProtocol::Http1, runtime).await;
    let mut stream = connect_tcp(server.addr).await;

    let status_line = request_health(&mut stream).await;

    assert_eq!(status_line, OK_STATUS_LINE);
    drop(stream);
    assert_eq!(server.stop().await, DrainOutcome::Finished);
}

/// One handshake that trusts only `leaf`, answering whether it completed. A failed handshake
/// is an answer here, where `connect_tls` would panic.
async fn handshake_trusting_succeeds(leaf: &CertificateDer<'static>, addr: SocketAddr) -> bool {
    let tcp_stream = connect_tcp(addr).await;
    let server_name = ServerName::try_from("localhost").expect("server name");
    let handshake = build_connector(&[leaf], &[]).connect(server_name, tcp_stream);
    matches!(tokio::time::timeout(CLIENT_TIMEOUT, handshake).await, Ok(Ok(_)))
}

/// The files are rewritten after the runtime started and nothing calls `reload`, so only the
/// watcher the runtime started can make the new pair visible to the listener bound with it.
#[tokio::test]
async fn start_listener_runtime_serves_the_loaded_pair_and_the_pair_that_replaces_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    let first_leaf = write_pair(dir.path());
    let tls = TlsConfig {
        certificate_path: dir.path().join(CERTIFICATE_FILE),
        private_key_path: dir.path().join(PRIVATE_KEY_FILE),
        reload_interval_secs: 1,
    };
    let runtime = start_listener_runtime(Some(&tls), &CancellationToken::new()).expect("a TLS runtime");
    let server = HealthServer::start_with_runtime(ApplicationProtocol::Http1, runtime).await;
    let mut first_connection = connect_tls(&build_connector(&[&first_leaf], &[]), server.addr).await;
    assert_eq!(read_presented_leaf(&first_connection), first_leaf);
    assert_eq!(request_health(&mut first_connection).await, OK_STATUS_LINE);

    let second_leaf = write_pair(dir.path());
    let replaced = async {
        // A read between the two file writes sees a mismatched pair and keeps the old one,
        // so the new pair appears on a later interval.
        while !handshake_trusting_succeeds(&second_leaf, server.addr).await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };
    tokio::time::timeout(RELOAD_TIMEOUT, replaced)
        .await
        .expect("the watcher serves the replaced pair within a few reload intervals");

    drop(first_connection);
    assert_eq!(server.stop().await, DrainOutcome::Finished);
}
