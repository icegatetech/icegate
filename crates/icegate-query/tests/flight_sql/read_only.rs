//! Read-only enforcement: DDL/DML must be rejected.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args
)]

use super::harness::{TestServer, execute_sql};

#[tokio::test]
async fn insert_into_logs_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    // Target the qualified path so the SQL planner can resolve the
    // table and we exercise the actual read-only `SQLOptions` gate
    // rather than a "table not found" error.
    let result = execute_sql(
        &mut client,
        "INSERT INTO iceberg.icegate.logs (body) VALUES ('should fail')",
    )
    .await;

    assert!(result.is_err(), "INSERT must be rejected; got: {:?}", result.ok());
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn create_table_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    let result = execute_sql(&mut client, "CREATE TABLE foo (a INT)").await;
    assert!(result.is_err(), "CREATE TABLE must be rejected; got: {:?}", result.ok());
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn drop_table_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    // `IF EXISTS` keeps the planner from short-circuiting on
    // "table not found"; the read-only `SQLOptions` gate must be the
    // thing that rejects DDL.
    let result = execute_sql(&mut client, "DROP TABLE IF EXISTS iceberg.icegate.logs").await;
    assert!(result.is_err(), "DROP TABLE must be rejected; got: {:?}", result.ok());
    server.shutdown().await;
    Ok(())
}
