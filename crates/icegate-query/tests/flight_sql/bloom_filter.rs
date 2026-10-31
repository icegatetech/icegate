//! Bloom filter row group pruning of the Iceberg scan.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    new_null_array,
};
use datafusion::parquet::file::properties::DEFAULT_PAGE_SIZE;
use iceberg::table::Table;
use icegate_common::parquet_encoding::{
    LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS, OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS,
};
use icegate_common::parquet_writer::build_writer_properties;
use icegate_common::schema::{
    COL_CONVERSATION_ID, COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_SPAN_ID, COL_TENANT_ID,
    COL_TIMESTAMP, COL_TRACE_ID,
};
use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE, OPERATIONS_TABLE};
use tonic::transport::Channel;

use super::harness::{LogRow, TestServer, build_logs_batch, commit_data_file, execute_sql};

const TENANT: &str = "tenant-bloom";

/// Rows per `logs` row group: the six fixture rows land in three row groups.
const ROWS_PER_ROW_GROUP: usize = 2;

/// Rows per `operations` row group: two operations of different conversations
/// and one operation outside any conversation.
const OPERATIONS_PER_ROW_GROUP: usize = 3;

/// Start, end, and ingestion time of every fixture operation.
const FIXTURE_TIMESTAMP_MICROS: i64 = 1_700_000_000_000_000;

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

/// Runs every case as `SELECT span_id FROM iceberg.icegate.<table> WHERE <predicate>`
/// and checks both the rows it returns and how its bloom filter phase pruned.
async fn assert_bloom_filter_cases(
    client: &mut FlightSqlServiceClient<Channel>,
    table: &str,
    cases: Vec<BloomFilterCase>,
) -> Result<(), Box<dyn std::error::Error>> {
    for case in cases {
        let query = format!("SELECT span_id FROM iceberg.icegate.{table} WHERE {}", case.predicate);

        let batches = execute_sql(client, &query).await?;
        assert_eq!(
            collect_sorted_span_ids(&batches),
            case.span_ids,
            "rows of `{}`",
            case.predicate
        );

        let explained = execute_sql(client, &format!("EXPLAIN ANALYZE {query}")).await?;
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
    Ok(())
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

    let cases = vec![
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
    assert_bloom_filter_cases(&mut client, LOGS_TABLE, cases).await?;

    server.shutdown().await;
    Ok(())
}

/// One fixture `operations` row.
struct OperationRow {
    /// The `k` of the row's `Tk` trace id and `Sk` span id.
    id: u8,
    /// `None` for an operation outside any conversation.
    conversation_id: Option<&'static str>,
}

/// Build one `operations` batch with a row per entry of `rows`, in order.
///
/// Only the required columns and `conversation_id` carry values; every other
/// column is NULL. A required column the fixture does not fill fails the build,
/// so the fixture breaks loudly when the schema gains one.
fn build_operations_batch(table: &Table, rows: &[OperationRow]) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let row_count = rows.len();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);
    let timestamps = || TimestampMicrosecondArray::from(vec![FIXTURE_TIMESTAMP_MICROS; row_count]);

    let mut by_name: HashMap<&str, ArrayRef> = HashMap::new();
    by_name.insert(COL_TENANT_ID, Arc::new(StringArray::from(vec![TENANT; row_count])));
    by_name.insert(
        COL_CONVERSATION_ID,
        Arc::new(StringArray::from(
            rows.iter().map(|row| row.conversation_id).collect::<Vec<_>>(),
        )),
    );
    by_name.insert(
        COL_TRACE_ID,
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            rows.iter().map(|row| build_trace_id(row.id)),
        )?),
    );
    by_name.insert(
        COL_SPAN_ID,
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            rows.iter().map(|row| build_span_id(row.id)),
        )?),
    );
    by_name.insert(COL_TIMESTAMP, Arc::new(timestamps()));
    by_name.insert(COL_END_TIMESTAMP, Arc::new(timestamps()));
    by_name.insert(COL_INGESTED_TIMESTAMP, Arc::new(timestamps()));
    by_name.insert(COL_DURATION_MICROS, Arc::new(Int64Array::from(vec![0_i64; row_count])));
    // `icegate_common::schema` exports no `COL_*` constant for it, so it stays
    // a literal; a misspelling leaves it unconsumed and fails the check below.
    by_name.insert("operation_name", Arc::new(StringArray::from(vec!["chat"; row_count])));

    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| match by_name.remove(field.name().as_str()) {
            Some(column) => Ok(column),
            None if field.is_nullable() => Ok(new_null_array(field.data_type(), row_count)),
            None => Err(format!(
                "operations fixture must fill required column `{}`",
                field.name()
            )),
        })
        .collect::<Result<Vec<ArrayRef>, String>>()?;
    assert!(
        by_name.is_empty(),
        "operations fixture builds columns the schema does not have: {:?}",
        by_name.keys()
    );

    Ok(RecordBatch::try_new(arrow_schema, columns)?)
}

/// A conversation lookup on `operations` skips the row groups whose
/// `conversation_id` bloom filter proves the conversation absent, and keeps
/// every row of the operations outside any conversation.
///
/// `conversation_id` is neither a partition nor a sort column, so a row group
/// holds several conversations and its min/max range spans them: every range
/// of the first three row groups contains `conv-5` and `conv-7`, and only the
/// bloom filter phase can drop one of them. Each of the three also holds an
/// operation outside any conversation, so its filter is built over a column
/// with NULLs; the fourth row group holds only such operations.
///
/// The queries project `span_id` and filter on `conversation_id`: the
/// predicate column is not read back, which is the shape of a conversation's
/// trace lookup.
#[tokio::test]
async fn conversation_id_bloom_filter_prunes_operations_row_groups_without_the_conversation()
-> Result<(), Box<dyn std::error::Error>> {
    let (server, catalog) = TestServer::start().await?;
    let table_ident = iceberg::TableIdent::from_strs([ICEGATE_NAMESPACE, OPERATIONS_TABLE])?;
    let table = catalog.load_table(&table_ident).await?;

    // Row groups in write order:
    // (S1 conv-1, S9 conv-9, S11) | (S2 conv-2, S8 conv-8, S12) | (S3 conv-3, S7 conv-7, S13) | (S14, S15, S16).
    let rows = [
        (1, Some("conv-1")),
        (9, Some("conv-9")),
        (11, None),
        (2, Some("conv-2")),
        (8, Some("conv-8")),
        (12, None),
        (3, Some("conv-3")),
        (7, Some("conv-7")),
        (13, None),
        (14, None),
        (15, None),
        (16, None),
    ]
    .map(|(id, conversation_id)| OperationRow { id, conversation_id });
    let unstamped_span_ids: Vec<Vec<u8>> = [11u8, 12, 13, 14, 15, 16].map(|k| build_span_id(k).to_vec()).to_vec();
    let batch = build_operations_batch(&table, &rows)?;
    let writer_properties = build_writer_properties(
        OPERATIONS_PER_ROW_GROUP,
        DEFAULT_PAGE_SIZE,
        OPERATIONS_BLOOM_COLUMNS,
        OPERATIONS_COLUMN_ENCODINGS,
    );
    commit_data_file(&table, &catalog, batch, "bloom-filter-operations", writer_properties).await?;

    // The all-NULL fourth row group never reaches the bloom filter phase of an
    // equality lookup: its statistics already prove that no row can match, so
    // these metrics count three row groups.
    let cases = vec![
        BloomFilterCase {
            predicate: "conversation_id = 'conv-7'".to_owned(),
            span_ids: vec![build_span_id(7).to_vec()],
            row_groups_pruned_bloom_filter: "3 total → 1 matched",
        },
        BloomFilterCase {
            predicate: "conversation_id = 'conv-5'".to_owned(),
            span_ids: vec![],
            row_groups_pruned_bloom_filter: "3 total → 0 matched",
        },
        BloomFilterCase {
            predicate: "conversation_id IN ('conv-2', 'conv-7')".to_owned(),
            span_ids: vec![build_span_id(2).to_vec(), build_span_id(7).to_vec()],
            row_groups_pruned_bloom_filter: "3 total → 2 matched",
        },
        // A bloom filter holds no NULLs, so it must not prune a NULL lookup:
        // every row group reaches the phase and every one is kept.
        BloomFilterCase {
            predicate: "conversation_id IS NULL".to_owned(),
            span_ids: unstamped_span_ids,
            row_groups_pruned_bloom_filter: "4 total → 4 matched",
        },
    ];

    let mut client = server.client(Some(TENANT));
    assert_bloom_filter_cases(&mut client, OPERATIONS_TABLE, cases).await?;

    server.shutdown().await;
    Ok(())
}
