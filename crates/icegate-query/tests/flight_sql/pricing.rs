//! `icegate.prices` is a global (non-tenant) table that must remain visible and
//! joinable through the tenant-scoped Flight SQL endpoint. It carries no
//! `tenant_id`, so the tenant decorator has to pass it through unwrapped — the
//! guarantee the whole conversation-cost feature depends on.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE};

use super::harness::{TestServer, count_from_batches, execute_sql, write_test_logs_for_tenant};

#[tokio::test]
async fn prices_is_selectable_through_the_tenant_endpoint() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("tenant-alpha"));
    // Empty until the crawler runs — the assertion is that it resolves at all,
    // i.e. the decorator did not reject it for the missing tenant_id.
    let batches = execute_sql(&mut client, "SELECT count(*) FROM iceberg.icegate.prices").await?;
    assert_eq!(count_from_batches(&batches), 0);
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn prices_effective_view_is_selectable() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("tenant-alpha"));
    let batches = execute_sql(&mut client, "SELECT count(*) FROM iceberg.icegate.prices_effective").await?;
    assert_eq!(count_from_batches(&batches), 0);
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn prices_joins_tenant_scoped_tables_in_one_plan() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?;
    let table = catalog.load_table(&ident).await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-alpha", "claude-opus-4", "Alpha").await?;

    let mut client = server.client(Some("tenant-alpha"));

    // Execute, don't just EXPLAIN: planning alone would not exercise the tenant
    // `FilterExec`/`ProjectionExec` rewrite running alongside the unwrapped
    // global view, which is the half of this that can only fail at run time.
    // The seeded logs give the tenant-wrapped side real rows to probe with;
    // `prices` is empty, so the inner join is correctly empty.
    let batches = execute_sql(
        &mut client,
        "SELECT count(*) FROM iceberg.icegate.logs l \
         JOIN iceberg.icegate.prices_effective p ON l.service_name = p.model",
    )
    .await?;
    assert_eq!(count_from_batches(&batches), 0);

    let explain = execute_sql(
        &mut client,
        "EXPLAIN SELECT l.body, p.input_usd_per_1m \
         FROM iceberg.icegate.logs l \
         JOIN iceberg.icegate.prices_effective p ON l.service_name = p.model",
    )
    .await?;
    assert!(!explain.is_empty());

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn prices_exposes_no_tenant_id_column() -> Result<(), Box<dyn std::error::Error>> {
    // Guards the allowlist semantics from the other direction: `prices` is
    // global, so no tenant scoping should have been applied or stripped.
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("tenant-alpha"));
    let result = execute_sql(&mut client, "SELECT tenant_id FROM iceberg.icegate.prices").await;
    assert!(result.is_err(), "prices must not expose a tenant_id column");
    server.shutdown().await;
    Ok(())
}
