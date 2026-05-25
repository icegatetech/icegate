//! Transaction stub tests.
//!
//! IceGate's Flight SQL endpoint is strictly read-only, so transaction
//! semantics are not actually engaged. The server still accepts the
//! `BeginTransaction` and `EndTransaction` standard actions so that
//! BI / SQL clients (`DuckDB` `duckhog`, JDBC drivers, ADBC) which issue
//! them unconditionally on connect can proceed to run queries.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args
)]

use arrow_flight::sql::EndTransaction;

use super::harness::{TestServer, execute_sql};

#[tokio::test]
async fn begin_transaction_returns_noop_handle() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    let handle = client.begin_transaction().await?;
    assert!(
        !handle.is_empty(),
        "begin_transaction must return a non-empty handle for client compatibility"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn end_transaction_accepts_any_handle() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    let handle = client.begin_transaction().await?;
    client.end_transaction(handle.clone(), EndTransaction::Commit).await?;
    // Rollback variant must also succeed.
    client.end_transaction(handle, EndTransaction::Rollback).await?;

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn query_works_inside_a_synthetic_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    let _handle = client.begin_transaction().await?;
    // Issuing a SELECT between begin/end must still execute — the
    // transaction handle is opaque and ignored server-side.
    let batches = execute_sql(&mut client, "SELECT 1 AS one").await?;
    assert_eq!(batches.len(), 1);

    server.shutdown().await;
    Ok(())
}
