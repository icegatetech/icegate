//! Tests for `GET /api/echo` — Grafana's Tempo data source uses this
//! endpoint to verify that search is reachable. The response contract
//! (HTTP 200, body `echo`) is fixed by the upstream Tempo API spec.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::uninlined_format_args)]

use super::harness::TestServer;

#[tokio::test]
async fn echo_returns_200_and_echo_body() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;

    let resp = server.client.get(format!("{}/api/echo", server.base_url)).send().await?;
    let status = resp.status();
    let body = resp.text().await?;

    assert_eq!(status, 200, "Response body: {}", body);
    assert_eq!(body, "echo", "Tempo /api/echo must return literal 'echo'");

    server.shutdown().await;
    Ok(())
}
