//! Metadata RPCs (catalogs / schemas / tables).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::uninlined_format_args
)]

use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::CommandGetTables;
use datafusion::arrow::array::RecordBatch;
use futures::TryStreamExt;

use super::harness::{TestServer, execute_sql};

#[tokio::test]
async fn explain_is_allowed() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    // EXPLAIN is gated by `SQLOptions::allow_statements` which we keep
    // at its default (`true`), so this should succeed.
    let batches = execute_sql(&mut client, "EXPLAIN SELECT count(*) FROM iceberg.icegate.logs").await?;
    assert!(!batches.is_empty(), "EXPLAIN should return at least one batch");

    server.shutdown().await;
    Ok(())
}

/// Drive the Flight SQL `get_tables` RPC end-to-end against a given
/// catalog filter, returning the rows that the client would render.
async fn fetch_tables(
    client: &mut arrow_flight::sql::client::FlightSqlServiceClient<tonic::transport::Channel>,
    catalog: Option<&str>,
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error>> {
    let cmd = CommandGetTables {
        catalog: catalog.map(str::to_owned),
        db_schema_filter_pattern: None,
        table_name_filter_pattern: None,
        table_types: Vec::new(),
        include_schema: false,
    };
    let info = client.get_tables(cmd).await?;
    let mut out = Vec::new();
    for endpoint in info.endpoint {
        let ticket = endpoint.ticket.ok_or("missing ticket")?;
        let stream: FlightRecordBatchStream = client.do_get(ticket).await?;
        let mut batches: Vec<RecordBatch> = stream.try_collect().await?;
        out.append(&mut batches);
    }
    Ok(out)
}

#[tokio::test]
async fn flight_sql_get_tables_exposes_iceberg_paths() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    // The iceberg catalog is the sole entry point for clients. Tenant
    // isolation is enforced one layer down by the `TableProvider`
    // wrapper (see `direct_query_against_raw_catalog_is_tenant_filtered`).
    let iceberg_batches = fetch_tables(&mut client, Some("iceberg")).await?;
    let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&iceberg_batches)?.to_string();
    for name in ["logs", "spans", "events", "metrics"] {
        assert!(
            pretty.contains(name),
            "get_tables(catalog=iceberg) should surface `{name}`, got:\n{pretty}"
        );
    }

    // No convenience views exist any more — the default `datafusion`
    // catalog is empty so clients have a single place to look.
    let df_batches = fetch_tables(&mut client, Some("datafusion")).await?;
    let df_rows = df_batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(
        df_rows, 0,
        "default datafusion catalog must be empty: clients should only see iceberg.icegate.*"
    );

    server.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn show_tables_lists_iceberg_paths() -> Result<(), Box<dyn std::error::Error>> {
    let (server, _catalog) = TestServer::start().await?;
    let mut client = server.client(Some("default"));

    let batches = execute_sql(&mut client, "SHOW TABLES").await?;
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&batches)?.to_string();
    // The four observability tables are visible only under the
    // `iceberg.icegate.*` path now that we no longer register
    // convenience views in the default catalog.
    for name in ["logs", "spans", "events", "metrics"] {
        assert!(
            formatted.contains(name),
            "SHOW TABLES output should mention `{name}`, got:\n{formatted}"
        );
    }
    server.shutdown().await;
    Ok(())
}
