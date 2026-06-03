//! Cross-tenant isolation enforced by the session provider.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args
)]

use icegate_common::{DEFAULT_TENANT_ID, ICEGATE_NAMESPACE, LOGS_TABLE};

use super::harness::{TestServer, count_from_batches, execute_sql, write_logs_file, write_test_logs_for_tenant};

#[tokio::test]
async fn each_tenant_sees_only_their_own_rows() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    let table_ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?;
    let table = catalog.load_table(&table_ident).await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-alpha", "alpha-service", "Alpha").await?;

    let table = catalog.load_table(&table_ident).await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-beta", "beta-service", "Beta").await?;

    let mut client_alpha = server.client(Some("tenant-alpha"));
    let batches = execute_sql(&mut client_alpha, "SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert_eq!(count_from_batches(&batches), 3, "tenant-alpha should see 3 rows");

    let mut client_beta = server.client(Some("tenant-beta"));
    let batches = execute_sql(&mut client_beta, "SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert_eq!(count_from_batches(&batches), 3, "tenant-beta should see 3 rows");

    let mut client_other = server.client(Some("tenant-charlie"));
    let batches = execute_sql(&mut client_other, "SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert_eq!(
        count_from_batches(&batches),
        0,
        "tenant-charlie has no data and should see 0 rows"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn missing_tenant_header_falls_back_to_default_tenant() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    // Seed rows under the default tenant AND a non-default tenant. A table
    // holding only default-tenant rows can't distinguish correct scoping
    // from an unscoped leak — both return 3. With a second tenant present,
    // a broken fallback that returned unscoped results would see all 6
    // rows; only a fallback that resolves to DEFAULT_TENANT_ID sees exactly
    // the 3 default rows.
    let table_ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?;
    let table = catalog.load_table(&table_ident).await?;
    write_test_logs_for_tenant(&table, &catalog, DEFAULT_TENANT_ID, "default-service", "Default").await?;

    let table = catalog.load_table(&table_ident).await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-other", "other-service", "Other").await?;

    // No tenant header — provider falls back to DEFAULT_TENANT_ID ("default").
    let mut client = server.client(None);
    let batches = execute_sql(&mut client, "SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert_eq!(
        count_from_batches(&batches),
        3,
        "headerless client must fall back to the default tenant and see only its 3 rows"
    );
    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn direct_query_against_raw_catalog_is_tenant_filtered() -> Result<(), Box<dyn std::error::Error>> {
    // Defence-in-depth: the qualified path `iceberg.icegate.logs`
    // stays reachable (so DuckDB / JDBC clients that walk the catalog
    // tree still work), but every scan goes through
    // `TenantScopedTableProvider`, which AND's `tenant_id = '<t>'`
    // into the filter list before delegating. The result: tenant-beta
    // querying tenant-alpha's qualified path returns zero rows, never
    // a leak.
    let (server, catalog) = TestServer::start().await?;

    let table_ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?;
    let table = catalog.load_table(&table_ident).await?;
    write_test_logs_for_tenant(&table, &catalog, "tenant-alpha", "alpha-service", "Alpha").await?;

    // tenant-alpha sees its three rows through the qualified path.
    let mut client_alpha = server.client(Some("tenant-alpha"));
    let batches = execute_sql(&mut client_alpha, "SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert_eq!(
        count_from_batches(&batches),
        3,
        "tenant-alpha sees its own rows via qualified path"
    );

    // tenant-beta sees zero rows for the same qualified path.
    let mut client_beta = server.client(Some("tenant-beta"));
    let batches = execute_sql(&mut client_beta, "SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert_eq!(
        count_from_batches(&batches),
        0,
        "tenant-beta must NOT see tenant-alpha's rows through the qualified iceberg path"
    );

    // The `tenant_id` column is hidden from the schema the wrapper
    // advertises, so a user can't even reference it in a WHERE clause
    // — let alone override the injected predicate. The query fails
    // at planning time with "column not found", which is a stronger
    // outcome than silently returning zero rows.
    let result = execute_sql(
        &mut client_beta,
        "SELECT count(*) FROM iceberg.icegate.logs WHERE tenant_id = 'tenant-alpha'",
    )
    .await;
    assert!(
        result.is_err(),
        "referring to the hidden tenant_id column must fail at plan time; got: {:?}",
        result.ok()
    );
    // Same for projection: `SELECT tenant_id` should also be rejected.
    let result = execute_sql(&mut client_beta, "SELECT tenant_id FROM iceberg.icegate.logs").await;
    assert!(
        result.is_err(),
        "projecting tenant_id must fail at plan time; got: {:?}",
        result.ok()
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tenant_id_is_hidden_from_schema_introspection() -> Result<(), Box<dyn std::error::Error>> {
    // `DESCRIBE` and `information_schema.columns` are the canonical
    // ways a client lists a table's columns. The hidden column must
    // not appear in either.
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    let describe = execute_sql(&mut client, "DESCRIBE iceberg.icegate.logs").await?;
    let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&describe)?.to_string();
    assert!(
        !pretty.contains("tenant_id"),
        "DESCRIBE must hide tenant_id, got:\n{pretty}"
    );

    let info_schema = execute_sql(
        &mut client,
        "SELECT column_name FROM information_schema.columns \
         WHERE table_catalog = 'iceberg' AND table_schema = 'icegate' AND table_name = 'logs'",
    )
    .await?;
    let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&info_schema)?.to_string();
    assert!(
        !pretty.contains("tenant_id"),
        "information_schema.columns must hide tenant_id, got:\n{pretty}"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn co_located_tenants_in_one_file_stay_isolated() -> Result<(), Box<dyn std::error::Error>> {
    // The strongest isolation check: two tenants' rows live in the SAME
    // data file, so the file's `tenant_id` statistics span both tenants
    // and no file/row-group pruning can separate them. Isolation here can
    // only come from the wrapper's row-level `FilterExec` — the exact
    // guarantee that protects un-partitioned WAL hot segments in
    // production. This runs through the full SQL planner + optimizer, so
    // it also proves the optimizer doesn't drop the injected filter.
    let (server, catalog) = TestServer::start().await?;

    let table_ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?;
    let table = catalog.load_table(&table_ident).await?;
    write_logs_file(
        &table,
        &catalog,
        &["tenant-alpha", "tenant-alpha", "tenant-beta"],
        "shared-service",
        "Mixed",
    )
    .await?;

    let mut client_alpha = server.client(Some("tenant-alpha"));
    let batches = execute_sql(&mut client_alpha, "SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert_eq!(
        count_from_batches(&batches),
        2,
        "tenant-alpha must see only its two rows from the shared file"
    );

    let mut client_beta = server.client(Some("tenant-beta"));
    let batches = execute_sql(&mut client_beta, "SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert_eq!(
        count_from_batches(&batches),
        1,
        "tenant-beta must see only its one row, never tenant-alpha's co-located rows"
    );

    server.shutdown().await;
    Ok(())
}
