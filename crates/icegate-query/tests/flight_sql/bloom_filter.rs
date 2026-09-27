//! Bloom filter row group pruning of the Iceberg scan.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use datafusion::arrow::array::{Array, FixedSizeBinaryArray, RecordBatch, StringArray};
use datafusion::parquet::file::properties::DEFAULT_PAGE_SIZE;
use icegate_common::parquet_encoding::{LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS};
use icegate_common::parquet_writer::build_writer_properties;
use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE};

use super::harness::{LogRow, TestServer, build_logs_batch, commit_data_file, execute_sql};

const TENANT: &str = "tenant-bloom";

/// Rows per row group: the six fixture rows land in three row groups.
const ROWS_PER_ROW_GROUP: usize = 2;

/// Trace id `Tk`: 16 bytes `[k, 0, …]`.
const fn build_trace_id(k: u8) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0] = k;
    id
}

/// Span id `Sk`: 8 bytes `[k, 0, …]`.
const fn build_span_id(k: u8) -> [u8; 8] {
    let mut id = [0u8; 8];
    id[0] = k;
    id
}

/// SQL literal of `Tk` typed as the `trace_id` column.
fn format_trace_id_literal(k: u8) -> String {
    format!(
        "arrow_cast(decode('{k:02x}{}', 'hex'), 'FixedSizeBinary(16)')",
        "00".repeat(15)
    )
}

/// SQL literal of `Sk` typed as the `span_id` column.
fn format_span_id_literal(k: u8) -> String {
    format!(
        "arrow_cast(decode('{k:02x}{}', 'hex'), 'FixedSizeBinary(8)')",
        "00".repeat(7)
    )
}

/// Every `span_id` of the result, sorted — no `ORDER BY` is issued.
fn collect_sorted_span_ids(batches: &[RecordBatch]) -> Vec<Vec<u8>> {
    let mut ids: Vec<Vec<u8>> = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("span_id is FixedSizeBinary(8)");
            (0..array.len()).map(|i| array.value(i).to_vec()).collect::<Vec<_>>()
        })
        .collect();
    ids.sort();
    ids
}

/// Value of metric `name` on the `IcegateIcebergScan` line of an
/// `EXPLAIN ANALYZE` result.
fn extract_iceberg_scan_metric(batches: &[RecordBatch], name: &str) -> String {
    let scan_lines: Vec<&str> = batches
        .iter()
        .flat_map(|batch| {
            let plans = batch
                .column_by_name("plan")
                .expect("EXPLAIN ANALYZE returns a `plan` column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("`plan` is Utf8");
            (0..plans.len()).flat_map(move |i| plans.value(i).lines())
        })
        .filter(|line| line.contains("IcegateIcebergScan"))
        .collect();
    assert_eq!(scan_lines.len(), 1, "expected one Iceberg scan in {scan_lines:?}");

    let prefix = format!("{name}=");
    let start = scan_lines[0]
        .find(&prefix)
        .unwrap_or_else(|| panic!("metric `{name}` missing from {}", scan_lines[0]))
        + prefix.len();
    let value = &scan_lines[0][start..];
    let end = value.find([',', ']']).unwrap_or(value.len());
    value[..end].to_string()
}

/// One predicate on the fixture and the outcome it must produce.
struct BloomFilterCase {
    predicate: String,
    span_ids: Vec<Vec<u8>>,
    row_groups_pruned_bloom_filter: &'static str,
}

/// Row groups whose bloom filters prove the predicate value absent are
/// skipped, and the rows returned are exactly the matching ones.
///
/// Every row group's min/max range contains `T5`, `T7`, and `S5`, so min/max
/// pruning keeps all three row groups and only the bloom filter phase can
/// drop them; the `3 total` in every expected metric proves all three
/// reached that phase.
#[tokio::test]
async fn bloom_filter_prunes_row_groups_without_the_value() -> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;
    let table_ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, LOGS_TABLE])?;
    let table = catalog.load_table(&table_ident).await?;

    // Row groups in write order: (T1,S1) (T9,S9) | (T2,S2) (T8,S8) | (T3,S3) (T7,S7).
    let rows: Vec<LogRow<'_>> = [1u8, 9, 2, 8, 3, 7]
        .into_iter()
        .map(|k| LogRow {
            tenant_id: TENANT,
            trace_id: build_trace_id(k),
            span_id: build_span_id(k),
        })
        .collect();
    let batch = build_logs_batch(&table, &rows, "svc", "bloom")?;
    let writer_properties = build_writer_properties(
        ROWS_PER_ROW_GROUP,
        DEFAULT_PAGE_SIZE,
        LOGS_BLOOM_COLUMNS,
        LOGS_COLUMN_ENCODINGS,
    );
    commit_data_file(&table, &catalog, batch, "bloom-filter", writer_properties).await?;

    let cases = [
        BloomFilterCase {
            predicate: format!("trace_id = {}", format_trace_id_literal(7)),
            span_ids: vec![build_span_id(7).to_vec()],
            row_groups_pruned_bloom_filter: "3 total → 1 matched",
        },
        BloomFilterCase {
            predicate: format!("trace_id = {}", format_trace_id_literal(5)),
            span_ids: vec![],
            row_groups_pruned_bloom_filter: "3 total → 0 matched",
        },
        BloomFilterCase {
            predicate: format!(
                "trace_id IN ({}, {})",
                format_trace_id_literal(2),
                format_trace_id_literal(7)
            ),
            span_ids: vec![build_span_id(2).to_vec(), build_span_id(7).to_vec()],
            row_groups_pruned_bloom_filter: "3 total → 2 matched",
        },
        // The row group holding `T7` is dropped by the `span_id` bloom filter:
        // either column proving absence is enough.
        BloomFilterCase {
            predicate: format!(
                "trace_id = {} AND span_id = {}",
                format_trace_id_literal(7),
                format_span_id_literal(5)
            ),
            span_ids: vec![],
            row_groups_pruned_bloom_filter: "3 total → 0 matched",
        },
    ];

    let mut client = server.client(Some(TENANT));
    for case in cases {
        let query = format!("SELECT span_id FROM iceberg.icegate.logs WHERE {}", case.predicate);

        let batches = execute_sql(&mut client, &query).await?;
        assert_eq!(
            collect_sorted_span_ids(&batches),
            case.span_ids,
            "rows of `{}`",
            case.predicate
        );

        let explained = execute_sql(&mut client, &format!("EXPLAIN ANALYZE {query}")).await?;
        assert_eq!(
            extract_iceberg_scan_metric(&explained, "row_groups_pruned_bloom_filter"),
            case.row_groups_pruned_bloom_filter,
            "bloom filter pruning of `{}`",
            case.predicate
        );
        assert_eq!(
            extract_iceberg_scan_metric(&explained, "bloom_filter_read_errors"),
            "0",
            "bloom filter read errors of `{}`",
            case.predicate
        );
    }

    server.shutdown().await;
    Ok(())
}
