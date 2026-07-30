//! Column projection through the tenant-scoped table provider.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use datafusion::arrow::array::{Array, RecordBatch, StringArray};
use icegate_common::{ICEGATE_NAMESPACE, SPANS_TABLE};

use super::harness::{TestServer, count_from_batches, execute_sql, write_spans_file};

/// Every value of one string column, sorted — the projection contract is the
/// row multiset, not its order (no `ORDER BY` is issued).
fn sorted_strings(batches: &[RecordBatch], column: usize) -> Vec<String> {
    let mut values: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string column");
            (0..array.len())
                .map(|i| {
                    assert!(array.is_valid(i), "spans.name is non-nullable");
                    array.value(i).to_string()
                })
                .collect::<Vec<_>>()
        })
        .collect();
    values.sort();
    values
}

/// Projecting `spans.name` on its own must return the same rows `count(*)`
/// reports.
///
/// `name` and the hidden `tenant_id` are both non-nullable strings. When the
/// wrapper appended `tenant_id` after the visible columns, the Iceberg reader
/// received `[name, tenant_id]` — an order it does not honour — and relabelled
/// the batch instead of reordering it, so the injected tenant predicate was
/// evaluated against span names. Every row was discarded: `SELECT name`
/// returned nothing while `count(*)` returned the full table.
#[tokio::test]
async fn projecting_name_alone_returns_the_same_rows_as_count() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;

    // All three rows land in ONE data file, so the file's `tenant_id`
    // statistics span both tenants and isolation can only come from the
    // wrapper's row-level filter — the same layout as a WAL hot segment.
    let table_ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, SPANS_TABLE])?;
    let table = catalog.load_table(&table_ident).await?;
    write_spans_file(
        &table,
        &catalog,
        &[
            ("tenant-alpha", "GET /health"),
            ("tenant-beta", "POST /orders"),
            ("tenant-alpha", "db query"),
        ],
    )
    .await?;

    let mut client_alpha = server.client(Some("tenant-alpha"));
    let counted =
        count_from_batches(&execute_sql(&mut client_alpha, "SELECT count(*) FROM iceberg.icegate.spans").await?);
    assert_eq!(counted, 2, "tenant-alpha wrote two spans");

    // Expected values are listed in ASCII order ('G' < 'd') to match the
    // canonicalising sort in `sorted_strings`.
    let expected = vec!["GET /health".to_string(), "db query".to_string()];

    let batches = execute_sql(&mut client_alpha, "SELECT name FROM iceberg.icegate.spans").await?;
    assert_eq!(
        sorted_strings(&batches, 0),
        expected,
        "a lone `name` projection must return every row `count(*)` counted"
    );

    // Adding a second column always worked, even while the lone projection
    // was broken; it must keep returning the same names.
    let batches = execute_sql(&mut client_alpha, "SELECT span_id, name FROM iceberg.icegate.spans").await?;
    assert_eq!(sorted_strings(&batches, 1), expected);

    // Cross-tenant negative: tenant-beta sees only its own span name, never
    // tenant-alpha's rows from the same file.
    let mut client_beta = server.client(Some("tenant-beta"));
    let batches = execute_sql(&mut client_beta, "SELECT name FROM iceberg.icegate.spans").await?;
    assert_eq!(sorted_strings(&batches, 0), vec!["POST /orders".to_string()]);

    server.shutdown().await;
    Ok(())
}
