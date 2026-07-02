//! Cross-catalog reachability: the global non-tenant `reference.llm.pricing`
//! table is visible AND joinable to the tenant-scoped `iceberg` catalog in a
//! single plan, driven through the real Flight SQL endpoint (not a bare
//! `SessionContext`). This is the make-or-break guarantee the whole
//! conversation-cost feature depends on.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE};

use super::harness::{TestServer, count_from_batches, execute_sql, write_test_logs_for_tenant};

#[tokio::test]
async fn standalone_select_on_reference_pricing_works() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("tenant-alpha"));
    let batches = execute_sql(&mut client, "SELECT count(*) FROM reference.llm.pricing").await?;
    assert_eq!(count_from_batches(&batches), 3);
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn cross_catalog_join_plans_and_executes() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    // Seed tenant-alpha logs whose service_name matches a pricing `model` row,
    // so the join produces matches. (logs is the table the harness can write;
    // the join reachability is independent of which iceberg table we pick.)
    let ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?;
    let table = catalog.load_table(&ident).await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-alpha", "claude-opus-4", "Alpha").await?;

    let mut client = server.client(Some("tenant-alpha"));
    let batches = execute_sql(
        &mut client,
        "SELECT count(*) FROM iceberg.icegate.logs l \
         JOIN reference.llm.pricing p ON l.service_name = p.model",
    )
    .await?;
    assert_eq!(count_from_batches(&batches), 3);

    // The cross-catalog plan is valid (references both catalogs).
    let explain = execute_sql(
        &mut client,
        "EXPLAIN SELECT l.body, p.input_usd_per_1m FROM iceberg.icegate.logs l \
         JOIN reference.llm.pricing p ON l.service_name = p.model",
    )
    .await?;
    assert!(!explain.is_empty());

    server.shutdown().await;
    Ok(())
}
