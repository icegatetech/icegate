//! Request helpers shared by the system-index test modules: one JSON request, one NDJSON request,
//! each answering its status and parsed body, and the result type a test returns.

use std::error::Error;

use reqwest::{Method, StatusCode};
use serde_json::Value;

use super::harness::TestServer;

/// What a test returns: a failed request or body ends it with the cause.
pub type TestResult = Result<(), Box<dyn Error>>;

/// Send `body` (or no body) and return the status and the parsed answer (`null` when empty).
pub async fn send_request(
    server: &TestServer,
    method: Method,
    tenant: &str,
    path: &str,
    body: Option<&Value>,
) -> Result<(StatusCode, Value), Box<dyn Error>> {
    let request = server.request(method, Some(tenant), path);
    let request = match body {
        Some(body) => request.json(body),
        None => request,
    };
    let response = request.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    let answer = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)?
    };
    Ok((status, answer))
}

/// Send an NDJSON body with `method` and an `application/x-ndjson` content type, and return the
/// status and the parsed answer.
pub async fn send_ndjson_with_method(
    server: &TestServer,
    method: Method,
    tenant: &str,
    path: &str,
    body: String,
) -> Result<(StatusCode, Value), reqwest::Error> {
    let response = server
        .request(method, Some(tenant), path)
        .header("content-type", "application/x-ndjson")
        .body(body)
        .send()
        .await?;
    let status = response.status();
    Ok((status, response.json::<Value>().await?))
}

/// [`send_ndjson_with_method`] with `POST`.
pub async fn send_ndjson(
    server: &TestServer,
    tenant: &str,
    path: &str,
    body: String,
) -> Result<(StatusCode, Value), reqwest::Error> {
    send_ndjson_with_method(server, Method::POST, tenant, path, body).await
}
