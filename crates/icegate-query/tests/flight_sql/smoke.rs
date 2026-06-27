//! Smoke tests: server starts and round-trips a trivial query.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args
)]

use super::harness::{TestServer, execute_sql};

#[tokio::test]
async fn select_one_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    // NB: `datafusion-flight-sql-server` rejects `do_handshake` with
    // `Unimplemented` by design, recommending middleware-style auth
    // (metadata headers) instead. Clients that require handshake (some
    // BI tools, older JDBC drivers) need to disable it or fall back to
    // metadata-based auth.
    let batches = execute_sql(&mut client, "SELECT 1 AS one").await?;
    assert_eq!(batches.len(), 1, "expected exactly one batch");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
    // Assert the scalar value round-trips, not just the batch shape.
    let value = batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("SELECT 1 should produce an Int64 column")
        .value(0);
    assert_eq!(value, 1, "SELECT 1 must round-trip the literal value 1");

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn select_count_on_empty_logs_returns_zero() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    let batches = execute_sql(&mut client, "SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert_eq!(super::harness::count_from_batches(&batches), 0);

    server.shutdown().await;
    Ok(())
}
