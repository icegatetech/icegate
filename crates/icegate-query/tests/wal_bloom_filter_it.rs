//! Bloom filter row group pruning of the WAL scan.
//!
//! The query engine reads uncommitted rows straight from WAL segments, through a
//! different reader than the Iceberg scan, so the bloom filters the WAL writer
//! puts into a segment are only worth their write cost if that reader uses them.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    new_null_array,
};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::prelude::SessionContext;
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use icegate_common::parquet_encoding::{OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS};
use icegate_common::schema::{
    COL_CONVERSATION_ID, COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_SPAN_ID, COL_TENANT_ID,
    COL_TIMESTAMP, COL_TRACE_ID,
};
use icegate_common::{
    CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, OPERATIONS_TABLE, OPERATIONS_TOPIC,
    catalog::CatalogBuilder, icegate_table_ident, schema,
};
use icegate_query::engine::{QueryEngine, QueryEngineConfig};
use icegate_queue::{ParquetQueueReader, PreparedWalRowGroup, QueueConfig, QueueWriter, WriteRequest, channel};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Tenant every fixture operation belongs to; nothing here tests tenant scoping.
const TENANT_ID: &str = "tenant-bloom";

/// Start, end, and ingestion time of every fixture operation.
const FIXTURE_TIMESTAMP_MICROS: i64 = 1_700_000_000_000_000;

/// The query engine and what it reads: a catalog whose `operations` table has
/// no snapshot, so every row is served from the WAL.
struct Fixture {
    engine: Arc<QueryEngine>,
    _warehouse: tempfile::TempDir,
}

/// A memory catalog holding an empty `operations` table.
async fn build_catalog() -> (Arc<dyn Catalog>, tempfile::TempDir) {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let catalog_config = CatalogConfig {
        backend: CatalogBackend::Memory,
        warehouse: warehouse.path().to_str().expect("warehouse path").to_string(),
        properties: HashMap::new(),
        cache: None,
    };
    let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("build catalog");
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let creation = TableCreation::builder()
        .name(OPERATIONS_TABLE.to_string())
        .schema(schema::operations_schema().expect("operations schema"))
        .build();
    catalog.create_table(&namespace, creation).await.expect("create table");
    (catalog, warehouse)
}

/// Span id `Sk`: 8 bytes `[k, 0, …]`.
const fn build_span_id(k: u8) -> [u8; 8] {
    let mut id = [0u8; 8];
    id[0] = k;
    id
}

/// One `operations` row group: a row per `(k, conversation_id)` entry, with
/// span id `Sk`, trace id `[k, 0, …]`, and `None` for an operation outside any
/// conversation.
///
/// Only the required columns and `conversation_id` carry values; every other
/// column is NULL. A required column the fixture does not fill panics, so the
/// fixture breaks loudly when the schema gains one.
fn build_operations_row_group(arrow_schema: &ArrowSchemaRef, rows: &[(u8, Option<&str>)]) -> PreparedWalRowGroup {
    let row_count = rows.len();
    let timestamps = || TimestampMicrosecondArray::from(vec![FIXTURE_TIMESTAMP_MICROS; row_count]);
    let trace_ids = rows.iter().map(|(k, _)| {
        let mut id = [0u8; 16];
        id[0] = *k;
        id
    });

    let mut by_name: HashMap<&str, ArrayRef> = HashMap::new();
    by_name.insert(COL_TENANT_ID, Arc::new(StringArray::from(vec![TENANT_ID; row_count])));
    by_name.insert(
        COL_CONVERSATION_ID,
        Arc::new(StringArray::from(rows.iter().map(|(_, id)| *id).collect::<Vec<_>>())),
    );
    by_name.insert(
        COL_TRACE_ID,
        Arc::new(FixedSizeBinaryArray::try_from_iter(trace_ids).expect("trace ids")),
    );
    by_name.insert(
        COL_SPAN_ID,
        Arc::new(FixedSizeBinaryArray::try_from_iter(rows.iter().map(|(k, _)| build_span_id(*k))).expect("span ids")),
    );
    by_name.insert(COL_TIMESTAMP, Arc::new(timestamps()));
    by_name.insert(COL_END_TIMESTAMP, Arc::new(timestamps()));
    by_name.insert(COL_INGESTED_TIMESTAMP, Arc::new(timestamps()));
    by_name.insert(COL_DURATION_MICROS, Arc::new(Int64Array::from(vec![0_i64; row_count])));
    // `icegate_common::schema` exports no `COL_*` constant for it, so it stays
    // a literal; a misspelling leaves it unconsumed and fails the check below.
    by_name.insert("operation_name", Arc::new(StringArray::from(vec!["chat"; row_count])));

    let columns: Vec<ArrayRef> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            by_name.remove(field.name().as_str()).unwrap_or_else(|| {
                assert!(
                    field.is_nullable(),
                    "operations fixture must fill required column `{}`",
                    field.name()
                );
                new_null_array(field.data_type(), row_count)
            })
        })
        .collect();
    assert!(
        by_name.is_empty(),
        "operations fixture builds columns the schema does not have: {:?}",
        by_name.keys()
    );
    PreparedWalRowGroup::new(RecordBatch::try_new(Arc::clone(arrow_schema), columns).expect("operations batch"))
}

impl Fixture {
    /// Write one WAL segment through the production writer, with the
    /// production `operations` bloom and encoding lists, one row group per
    /// entry of `row_groups`.
    async fn seed(row_groups: &[&[(u8, Option<&str>)]]) -> Self {
        let (catalog, warehouse) = build_catalog().await;
        let table = catalog
            .load_table(&icegate_table_ident(OPERATIONS_TABLE))
            .await
            .expect("load operations table");
        let arrow_schema =
            Arc::new(iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).expect("arrow schema"));

        let wal_store = Arc::new(InMemory::new());
        let queue_config = QueueConfig::new("");
        let (write_tx, write_rx) = channel(queue_config.common.channel_capacity);
        let writer = QueueWriter::new(queue_config, Arc::clone(&wal_store) as Arc<dyn ObjectStore>)
            .with_bloom_filter_columns(HashMap::from([(
                OPERATIONS_TOPIC.to_string(),
                OPERATIONS_BLOOM_COLUMNS,
            )]))
            .with_column_encodings(HashMap::from([(
                OPERATIONS_TOPIC.to_string(),
                OPERATIONS_COLUMN_ENCODINGS,
            )]));
        let writer_handle = writer.start(write_rx).await.expect("start WAL writer");
        let (response_tx, response_rx) = oneshot::channel();
        write_tx
            .send(WriteRequest {
                topic: OPERATIONS_TOPIC.to_string(),
                row_groups: row_groups
                    .iter()
                    .map(|rows| build_operations_row_group(&arrow_schema, rows))
                    .collect(),
                response_tx,
                trace_context: None,
            })
            .await
            .expect("enqueue WAL write");
        let result = response_rx.await.expect("WAL write result");
        assert_eq!(result.offset(), Some(0), "the fixture writes exactly one segment");
        drop(write_tx);
        writer_handle.await.expect("WAL writer task").expect("WAL writer shutdown");

        let engine_config = QueryEngineConfig {
            wal_query_enabled: true,
            ..QueryEngineConfig::default()
        };
        let wal_reader = Arc::new(
            ParquetQueueReader::new("", Arc::clone(&wal_store) as Arc<dyn ObjectStore>, 8_192).expect("wal reader"),
        );
        let engine = Arc::new(QueryEngine::new(
            catalog,
            engine_config,
            wal_store as Arc<dyn ObjectStore>,
            wal_reader,
        ));
        Self {
            engine,
            _warehouse: warehouse,
        }
    }
}

/// Every `span_id` the query returns, sorted — no `ORDER BY` is issued.
async fn query_span_ids(session: &SessionContext, query: &str) -> Vec<Vec<u8>> {
    let batches = session.sql(query).await.expect("plan").collect().await.expect("execute");
    let mut ids: Vec<Vec<u8>> = batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name(COL_SPAN_ID)
                .expect("span_id column")
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("span_id is FixedSizeBinary(8)");
            (0..array.len()).map(|i| array.value(i).to_vec()).collect::<Vec<_>>()
        })
        .collect();
    ids.sort();
    ids
}

/// Value of metric `name` on the WAL scan (`DataSourceExec`) line of an
/// `EXPLAIN ANALYZE` of `query`.
async fn explain_wal_scan_metric(session: &SessionContext, query: &str, name: &str) -> String {
    let batches = session
        .sql(&format!("EXPLAIN ANALYZE {query}"))
        .await
        .expect("plan explain")
        .collect()
        .await
        .expect("execute explain");
    let scan_lines: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let plans = batch
                .column_by_name("plan")
                .expect("EXPLAIN ANALYZE returns a `plan` column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("`plan` is Utf8");
            (0..plans.len())
                .flat_map(|i| plans.value(i).lines().map(str::to_owned).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        })
        .filter(|line| line.contains("DataSourceExec"))
        .collect();
    assert_eq!(scan_lines.len(), 1, "expected one WAL scan in {scan_lines:?}");

    let prefix = format!("{name}=");
    let start = scan_lines[0]
        .find(&prefix)
        .unwrap_or_else(|| panic!("metric `{name}` missing from {}", scan_lines[0]))
        + prefix.len();
    let value = &scan_lines[0][start..];
    let end = value.find([',', ']']).unwrap_or(value.len());
    value[..end].to_string()
}

/// A conversation lookup served from the WAL skips the segment row groups whose
/// `conversation_id` bloom filter proves the conversation absent.
///
/// Every row group's min/max range contains `conv-5` and `conv-7`, so
/// statistics keep all three and only the bloom filter phase can drop one.
/// Each row group also holds an operation outside any conversation, the shape
/// ingest produces.
#[tokio::test]
async fn conversation_id_bloom_filter_prunes_wal_row_groups_without_the_conversation() {
    let fixture = Fixture::seed(&[
        &[(1, Some("conv-1")), (9, Some("conv-9")), (11, None)],
        &[(2, Some("conv-2")), (8, Some("conv-8")), (12, None)],
        &[(3, Some("conv-3")), (7, Some("conv-7")), (13, None)],
    ])
    .await;
    let session = fixture.engine.create_session().await.expect("session");

    let cases = [
        (
            "conversation_id = 'conv-7'",
            vec![build_span_id(7).to_vec()],
            "3 total → 1 matched",
        ),
        ("conversation_id = 'conv-5'", vec![], "3 total → 0 matched"),
        (
            "conversation_id IN ('conv-2', 'conv-7')",
            vec![build_span_id(2).to_vec(), build_span_id(7).to_vec()],
            "3 total → 2 matched",
        ),
        (
            "conversation_id IS NULL",
            [11u8, 12, 13].map(|k| build_span_id(k).to_vec()).to_vec(),
            "3 total → 3 matched",
        ),
    ];
    for (predicate, span_ids, row_groups_pruned_bloom_filter) in cases {
        let query = format!("SELECT span_id FROM iceberg.{ICEGATE_NAMESPACE}.{OPERATIONS_TABLE} WHERE {predicate}");
        assert_eq!(
            query_span_ids(&session, &query).await,
            span_ids,
            "rows of `{predicate}`"
        );
        assert_eq!(
            explain_wal_scan_metric(&session, &query, "row_groups_pruned_bloom_filter").await,
            row_groups_pruned_bloom_filter,
            "bloom filter pruning of `{predicate}`"
        );
    }
}
