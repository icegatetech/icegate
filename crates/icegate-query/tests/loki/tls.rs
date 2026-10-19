//! The Loki listener over TLS, at the real transport boundary.
#![allow(clippy::expect_used)]

use reqwest::{StatusCode, Version};

use super::harness::TestServer;
use crate::tls_certificate::TestCertificate;

/// `/ready` is the one route that needs neither a tenant nor data, so the answer shows that the
/// handshake completed and the request reached the router. The client trusts only the test
/// certificate, so the server presented that certificate, and `HTTP/1.1` is what the listener
/// offers although the client also offers `h2`.
#[tokio::test]
async fn loki_answers_ready_over_tls() {
    let certificate = TestCertificate::generate();
    let (server, _catalog) = TestServer::start_with_certificate(Some(&certificate))
        .await
        .expect("start the Loki test server over TLS");

    let response = server
        .client
        .get(format!("{}/ready", server.base_url))
        .send()
        .await
        .expect("GET /ready over TLS");
    let status = response.status();
    let version = response.version();
    server.shutdown().await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(version, Version::HTTP_11);
}
