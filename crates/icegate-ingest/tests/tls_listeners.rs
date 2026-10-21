//! The OTLP listeners over TLS and over plaintext, started through the same `run` functions the
//! binary spawns, so every request crosses a real socket into the router or the gRPC service.
//!
//! The tenant policy is `multi` and no request names a tenant, so the answer under test is the
//! tenant refusal. It is produced behind the transport, by the router or the interceptor, which
//! shows the request got through; and it needs no WAL write, so the write channel's receiver is
//! dropped.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{error::Error, fs, future::Future, net::Ipv4Addr, sync::Arc, time::Duration};

use icegate_common::{
    DEFAULT_TLS_RELOAD_INTERVAL_SECS, ListenerRuntime, MemoryPressure, ServerCertificate, TenantResolver, TlsConfig,
};
use icegate_ingest::{
    infra::metrics::OtlpMetrics,
    otlp_grpc::{self, OtlpGrpcConfig, OtlpGrpcService},
    otlp_http::{self, OtlpHttpConfig, OtlpHttpState},
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use rcgen::{CertificateParams, KeyPair};
use tempfile::TempDir;
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
    net::TcpStream,
    task::JoinHandle,
};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{
        ClientConfig, RootCertStore,
        crypto::aws_lc_rs,
        pki_types::{CertificateDer, ServerName},
    },
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::{
    Code,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

/// The upper bound of every wait, so a hang fails the test instead of the run.
const STEP_TIMEOUT: Duration = Duration::from_secs(10);

/// The name the test certificate is issued for, which the clients verify the server against.
const CERTIFICATE_HOST: &str = "localhost";

type ServerOutcome = Result<(), Box<dyn Error + Send + Sync>>;

/// A self-signed certificate for [`CERTIFICATE_HOST`], written to disk and loaded the way the
/// binary loads one, together with the bytes the clients trust.
struct TestCertificate {
    /// Owns the files the certificate was loaded from.
    _directory: TempDir,
    server_certificate: Arc<ServerCertificate>,
    pem: String,
    der: Vec<u8>,
}

impl TestCertificate {
    fn generate() -> Self {
        let key_pair = KeyPair::generate().expect("generate key pair");
        let certificate = CertificateParams::new(vec![CERTIFICATE_HOST.to_string()])
            .expect("certificate params")
            .self_signed(&key_pair)
            .expect("self-sign certificate");

        let directory = TempDir::new().expect("temp directory");
        let certificate_path = directory.path().join("tls.crt");
        let private_key_path = directory.path().join("tls.key");
        fs::write(&certificate_path, certificate.pem()).expect("write certificate");
        fs::write(&private_key_path, key_pair.serialize_pem()).expect("write private key");

        let server_certificate = ServerCertificate::load(&TlsConfig {
            certificate_path,
            private_key_path,
            reload_interval_secs: DEFAULT_TLS_RELOAD_INTERVAL_SECS,
        })
        .expect("load the generated pair");

        Self {
            _directory: directory,
            server_certificate: Arc::new(server_certificate),
            pem: certificate.pem(),
            der: certificate.der().as_ref().to_vec(),
        }
    }
}

/// A port nothing is listening on. Another process may take it before the server binds, which
/// the readiness wait reports as a server that exited.
fn reserve_port() -> u16 {
    std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .expect("bind a free port")
        .local_addr()
        .expect("local address")
        .port()
}

/// An ingest server task running on [`Self::port`].
struct RunningServer {
    port: u16,
    cancel_token: CancellationToken,
    handle: JoinHandle<ServerOutcome>,
    /// Cancels the server when the test ends, including by panic.
    _cancel_on_drop: DropGuard,
}

impl RunningServer {
    /// Spawn `server` and return once its port accepts connections.
    async fn start(
        port: u16,
        cancel_token: CancellationToken,
        server: impl Future<Output = ServerOutcome> + Send + 'static,
    ) -> Self {
        let mut handle = tokio::spawn(server);
        let started = tokio::time::timeout(STEP_TIMEOUT, async {
            loop {
                if handle.is_finished() {
                    // Carries the server's own error, usually a failed bind.
                    let outcome = (&mut handle).await;
                    panic!("the server exited before it accepted a connection: {outcome:?}");
                }
                if TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await.is_ok() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        started.expect("the server accepts connections in time");

        Self {
            port,
            _cancel_on_drop: cancel_token.clone().drop_guard(),
            cancel_token,
            handle,
        }
    }

    /// Cancel the server and require it to stop cleanly, as the binary's shutdown does.
    async fn stop(self) {
        self.cancel_token.cancel();
        tokio::time::timeout(STEP_TIMEOUT, self.handle)
            .await
            .expect("the server stops in time after cancellation")
            .expect("the server task does not panic")
            .expect("the server stops without an error");
    }
}

/// The listeners of one process: shut down by a fresh token, and serving TLS when a
/// certificate is given.
fn build_runtime(certificate: Option<&TestCertificate>) -> (ListenerRuntime, CancellationToken) {
    let cancel_token = CancellationToken::new();
    let runtime = ListenerRuntime::new(
        cancel_token.clone(),
        certificate.map(|certificate| Arc::clone(&certificate.server_certificate)),
    );
    (runtime, cancel_token)
}

async fn start_http_server(certificate: Option<&TestCertificate>) -> RunningServer {
    let (runtime, cancel_token) = build_runtime(certificate);
    let port = reserve_port();
    let (write_channel, write_receiver) = icegate_queue::channel(1);
    drop(write_receiver);
    let state = OtlpHttpState {
        write_channel,
        wal_row_group_size: 4,
        operations_enabled: true,
        metrics: OtlpMetrics::new_disabled(),
    };
    let config = OtlpHttpConfig {
        host: Ipv4Addr::LOCALHOST.to_string(),
        port,
        ..OtlpHttpConfig::default()
    };

    let server = otlp_http::run(state, config, runtime, MemoryPressure::inert(), TenantResolver::Multi);
    RunningServer::start(port, cancel_token, server).await
}

async fn start_grpc_server(certificate: Option<&TestCertificate>) -> RunningServer {
    let (runtime, cancel_token) = build_runtime(certificate);
    let port = reserve_port();
    let (write_channel, write_receiver) = icegate_queue::channel(1);
    drop(write_receiver);
    let service = OtlpGrpcService::new(write_channel, 4, true, OtlpMetrics::new_disabled());
    let config = OtlpGrpcConfig {
        host: Ipv4Addr::LOCALHOST.to_string(),
        port,
        ..OtlpGrpcConfig::default()
    };

    let server = otlp_grpc::run(service, config, runtime, MemoryPressure::inert(), TenantResolver::Multi);
    RunningServer::start(port, cancel_token, server).await
}

/// Handshake with the TLS listener on `port`, trusting only `certificate` and offering only
/// `http/1.1`, the protocol the HTTP listeners serve.
async fn connect_http_over_tls(port: u16, certificate: &TestCertificate) -> TlsStream<TcpStream> {
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(certificate.der.clone()))
        .expect("trust the test certificate");
    // The provider is passed explicitly so the client does not depend on which crate installed
    // a process-wide default.
    let mut config = ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];

    let tcp_stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).await.expect("connect");
    let server_name = ServerName::try_from(CERTIFICATE_HOST).expect("server name");
    tokio::time::timeout(
        STEP_TIMEOUT,
        TlsConnector::from(Arc::new(config)).connect(server_name, tcp_stream),
    )
    .await
    .expect("the handshake finishes in time")
    .expect("the handshake succeeds")
}

/// POST an empty OTLP logs export that names no tenant over `stream` and return the status code
/// of the answer.
async fn post_logs_without_tenant(stream: &mut (impl AsyncRead + AsyncWrite + Unpin)) -> u16 {
    const REQUEST: &[u8] = b"POST /v1/logs HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/x-protobuf\r\n\
                             Content-Length: 0\r\nConnection: close\r\n\r\n";
    stream.write_all(REQUEST).await.expect("send the request");

    let mut response = Vec::new();
    let mut chunk = [0_u8; 256];
    let status_line_end = loop {
        if let Some(position) = response.windows(2).position(|pair| pair == b"\r\n") {
            break position;
        }
        let read = tokio::time::timeout(STEP_TIMEOUT, stream.read(&mut chunk))
            .await
            .expect("the answer arrives in time")
            .expect("read the answer");
        assert_ne!(read, 0, "the connection closed before a status line arrived");
        response.extend_from_slice(&chunk[..read]);
    };

    // "HTTP/1.1 400 Bad Request": the code is the second field.
    let status_line = std::str::from_utf8(&response[..status_line_end]).expect("an ASCII status line");
    status_line
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a numeric status code")
}

/// Export an empty logs request that names no tenant over `channel` and return the code of the
/// refusal.
async fn export_logs_without_tenant(channel: Channel) -> Code {
    LogsServiceClient::new(channel)
        .export(ExportLogsServiceRequest::default())
        .await
        .expect_err("an export without a tenant is refused")
        .code()
}

fn build_grpc_endpoint(scheme: &str, port: u16) -> Endpoint {
    Endpoint::from_shared(format!("{scheme}://{}:{port}", Ipv4Addr::LOCALHOST))
        .expect("endpoint")
        .connect_timeout(STEP_TIMEOUT)
        .timeout(STEP_TIMEOUT)
}

#[tokio::test]
async fn otlp_http_answers_over_tls() {
    let certificate = TestCertificate::generate();
    let server = start_http_server(Some(&certificate)).await;

    let mut stream = connect_http_over_tls(server.port, &certificate).await;
    let negotiated_protocol = stream.get_ref().1.alpn_protocol().map(<[u8]>::to_vec);
    let status = post_logs_without_tenant(&mut stream).await;
    drop(stream);
    server.stop().await;

    // 400 is the tenant refusal of the router (`otlp_http/tenant.rs`).
    assert_eq!(status, 400);
    assert_eq!(negotiated_protocol, Some(b"http/1.1".to_vec()));
}

#[tokio::test]
async fn otlp_http_answers_in_plaintext_without_a_certificate() {
    let server = start_http_server(None).await;

    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, server.port)).await.expect("connect");
    let status = post_logs_without_tenant(&mut stream).await;
    drop(stream);
    server.stop().await;

    assert_eq!(status, 400);
}

#[tokio::test]
async fn otlp_grpc_answers_over_tls() {
    let certificate = TestCertificate::generate();
    let server = start_grpc_server(Some(&certificate)).await;
    // The client offers `h2` alone, so a server that did not offer it would fail the handshake.
    let tls_config = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&certificate.pem))
        .domain_name(CERTIFICATE_HOST);
    let channel = build_grpc_endpoint("https", server.port)
        .tls_config(tls_config)
        .expect("client TLS config")
        .connect()
        .await
        .expect("connect over TLS");

    let code = export_logs_without_tenant(channel).await;
    server.stop().await;

    // The tenant interceptor refuses with `InvalidArgument`.
    assert_eq!(code, Code::InvalidArgument);
}

#[tokio::test]
async fn otlp_grpc_answers_in_plaintext_without_a_certificate() {
    let server = start_grpc_server(None).await;
    let channel = build_grpc_endpoint("http", server.port)
        .connect()
        .await
        .expect("connect in plaintext");

    let code = export_logs_without_tenant(channel).await;
    server.stop().await;

    assert_eq!(code, Code::InvalidArgument);
}
