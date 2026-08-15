//! The query deadline, observed at the Flight SQL wire boundary.
//!
//! `DoGet` streams, so its response future resolves while the query is still
//! running: only a deadline covering the response BODY can end a runaway query,
//! and that is what `icegate_query::infra::deadline` adds. Here the whole path
//! is real — tonic transport, Flight SQL client, an actual long-running plan.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::uninlined_format_args)]

use std::time::{Duration, Instant};

use icegate_query::engine::QueryEngineConfig;

use super::harness::{TestServer, execute_sql};

/// The narrowest deadline the config accepts. The query below cannot finish
/// inside it, and the test's own timeout is far wider, so the outcome is
/// decided by the deadline and not by how fast the machine is.
const DEADLINE_SECS: u64 = 1;

/// Row count no machine gets through in a second; the plan streams for as long
/// as it is allowed to.
const ENDLESS_ROWS: u64 = 100_000_000_000;

#[tokio::test]
async fn a_query_outliving_the_deadline_fails_the_stream() -> Result<(), Box<dyn std::error::Error>> {
    let engine_config = QueryEngineConfig {
        max_query_duration_secs: DEADLINE_SECS,
        ..QueryEngineConfig::default()
    };
    let (server, _catalog) = TestServer::start_with_engine_config(engine_config).await?;
    let mut client = server.client(Some("default"));

    let started = Instant::now();
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        execute_sql(
            &mut client,
            &format!("SELECT count(*) FROM generate_series(1, {ENDLESS_ROWS})"),
        ),
    )
    .await
    .expect("the deadline must end the query well before the test's own timeout");

    let error = result.expect_err("a query past its deadline must not return rows");
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "the stream ended at the deadline, not by exhausting the plan: {error}"
    );

    server.shutdown().await;
    Ok(())
}
