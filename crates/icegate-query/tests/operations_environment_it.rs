//! Reading `operations.environment` where the storage and the reader's schema
//! disagree about the column.
//!
//! After `migrate create` adds the column, data files and WAL segments written
//! before it stay as they are. Both readers of the table, the Iceberg scan and
//! the WAL scan, must then answer NULL for the column they lack, with and without
//! a filter on it. A provider built before the migration must in turn read a WAL
//! segment that already carries the column, without exposing it. The WAL scan
//! leaves both to DataFusion, so these tests are what pin the behaviour.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    new_null_array,
};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use icegate_common::iceberg_write::{WriteConfig, write_record_batches_to_parquet};
use icegate_common::parquet_encoding::{OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS};
use icegate_common::schema::{
    COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_ENVIRONMENT, COL_INGESTED_TIMESTAMP, COL_SPAN_ID, COL_TENANT_ID,
    COL_TIMESTAMP, COL_TRACE_ID,
};
use icegate_common::{
    CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, OPERATIONS_TABLE, OPERATIONS_TOPIC,
    WAL_OFFSET_PROPERTY, catalog::CatalogBuilder, icegate_table_ident, schema,
};
use icegate_query::engine::{QueryEngine, QueryEngineConfig};
use icegate_queue::{ParquetQueueReader, PreparedWalRowGroup, QueueConfig, QueueWriter, WriteRequest, channel};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

/// Tenant every fixture operation belongs to; nothing here tests tenant scoping.
const TENANT_ID: &str = "tenant-environment";

/// Start, end, and ingestion time of every fixture operation.
const FIXTURE_TIMESTAMP_MICROS: i64 = 1_700_000_000_000_000;

/// WAL offset the data file's snapshot claims: the segment at this offset is
/// already in Iceberg and must not be read again.
const COMMITTED_OFFSET: u64 = 0;

/// Span in a WAL segment the committed offset covers.
const S0: u8 = 10;
/// Span in the data file written before the column existed.
const S1: u8 = 1;
/// Span in a WAL segment of the previous layout, above the committed offset.
const S2: u8 = 2;
/// Span in a WAL segment of the current layout, with a value in the column.
const S3: u8 = 3;
/// Span in a WAL segment of the current layout, with NULL in the column.
const S4: u8 = 4;

/// Span id `Sk`: 8 bytes `[k, 0, …]`.
const fn build_span_id(k: u8) -> [u8; 8] {
    let mut id = [0u8; 8];
    id[0] = k;
    id
}

/// A memory catalog holding an `operations` table without `environment`.
async fn create_previous_operations_table() -> (Arc<dyn Catalog>, Table, tempfile::TempDir) {
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
    let code_schema = schema::operations_schema().expect("operations schema");
    let previous_schema = Schema::builder()
        .with_fields(
            code_schema
                .as_struct()
                .fields()
                .iter()
                .filter(|field| field.name != COL_ENVIRONMENT)
                .cloned(),
        )
        .build()
        .expect("previous schema");
    let creation = TableCreation::builder()
        .name(OPERATIONS_TABLE.to_string())
        .schema(previous_schema)
        .partition_spec(
            schema::operations_partition_spec(&code_schema)
                .expect("operations partition spec")
                .into_unbound(),
        )
        .sort_order(schema::operations_sort_order(&code_schema).expect("operations sort order"))
        .build();
    let table = catalog.create_table(&namespace, creation).await.expect("create table");
    (catalog, table, warehouse)
}

/// One `operations` batch in `arrow_schema`, a row per `(k, environment)` entry,
/// with span id `Sk`.
///
/// Only the required columns and, when the schema has it, `environment` carry
/// values. A required column the fixture does not fill panics, so the fixture
/// breaks loudly when the schema gains one.
fn build_operations_batch(arrow_schema: &ArrowSchemaRef, rows: &[(u8, Option<&str>)]) -> RecordBatch {
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
    // `icegate_common::schema` exports no `COL_*` constant for it, so it stays a literal.
    by_name.insert("operation_name", Arc::new(StringArray::from(vec!["chat"; row_count])));
    if arrow_schema.field_with_name(COL_ENVIRONMENT).is_ok() {
        by_name.insert(
            COL_ENVIRONMENT,
            Arc::new(StringArray::from(
                rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
            )),
        );
    }

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
    RecordBatch::try_new(Arc::clone(arrow_schema), columns).expect("operations batch")
}

/// Commit `batch` as one data file, with [`COMMITTED_OFFSET`] on the snapshot.
async fn commit_data_file(catalog: &Arc<dyn Catalog>, table: &Table, batch: RecordBatch) {
    let write_config = WriteConfig {
        row_group_size: 1_000,
        data_page_size_limit_bytes: 1024 * 1024,
        max_file_size_bytes: 16 * 1024 * 1024,
        bloom_filter_columns: OPERATIONS_BLOOM_COLUMNS,
        column_encodings: OPERATIONS_COLUMN_ENCODINGS,
    };
    let data_files = write_record_batches_to_parquet(
        table.clone(),
        write_config,
        futures::stream::once(async { Ok(batch) }).boxed(),
        &CancellationToken::new(),
    )
    .await
    .expect("write data file")
    .data_files;
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(data_files)
        .set_snapshot_properties(HashMap::from([(
            WAL_OFFSET_PROPERTY.to_string(),
            COMMITTED_OFFSET.to_string(),
        )]))
        .apply(tx)
        .expect("apply fast append");
    tx.commit(catalog.as_ref()).await.expect("commit data file");
}

/// Add `environment` the way `migrate create` does: the column and a snapshot
/// that keeps the committed offset, in one commit. Answers the evolved table.
async fn add_environment_column(catalog: &Arc<dyn Catalog>) -> Table {
    let table = catalog
        .load_table(&icegate_table_ident(OPERATIONS_TABLE))
        .await
        .expect("load operations table");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_schema()
        .add_column(AddColumn::optional(
            COL_ENVIRONMENT,
            Type::Primitive(PrimitiveType::String),
        ))
        .apply(tx)
        .expect("add column applies");
    // A snapshot with no data files commits only when it states at least one property.
    let stamp = tx
        .fast_append()
        .set_snapshot_properties(HashMap::from([(
            "fixture.added-columns".to_string(),
            COL_ENVIRONMENT.to_string(),
        )]))
        .inherit_summary_property(WAL_OFFSET_PROPERTY);
    let tx = stamp.apply(tx).expect("stamp applies");
    tx.commit(catalog.as_ref()).await.expect("add column commits")
}

/// Write one WAL segment per entry of `segments`, through the production writer
/// with the production `operations` bloom and encoding lists.
async fn write_wal_segments(wal_store: &Arc<InMemory>, segments: Vec<RecordBatch>) {
    let queue_config = QueueConfig::new("");
    let (write_tx, write_rx) = channel(queue_config.common.channel_capacity);
    let writer = QueueWriter::new(queue_config, Arc::clone(wal_store) as Arc<dyn ObjectStore>)
        .with_bloom_filter_columns(HashMap::from([(
            OPERATIONS_TOPIC.to_string(),
            OPERATIONS_BLOOM_COLUMNS,
        )]))
        .with_column_encodings(HashMap::from([(
            OPERATIONS_TOPIC.to_string(),
            OPERATIONS_COLUMN_ENCODINGS,
        )]));
    let writer_handle = writer.start(write_rx).await.expect("start WAL writer");
    for (segment_idx, batch) in segments.into_iter().enumerate() {
        let (response_tx, response_rx) = oneshot::channel();
        write_tx
            .send(WriteRequest {
                topic: OPERATIONS_TOPIC.to_string(),
                row_groups: vec![PreparedWalRowGroup::new(batch)],
                response_tx,
                trace_context: None,
            })
            .await
            .expect("enqueue WAL write");
        let result = response_rx.await.expect("WAL write result");
        assert_eq!(
            result.offset(),
            Some(segment_idx as u64),
            "the fixture depends on the segment offsets the queue assigns"
        );
    }
    drop(write_tx);
    writer_handle.await.expect("WAL writer task").expect("WAL writer shutdown");
}

/// Every `(span_id, environment)` the query returns, sorted: no `ORDER BY` is issued.
async fn query_environments(session: &SessionContext, query: &str) -> Vec<(Vec<u8>, Option<String>)> {
    let batches = session.sql(query).await.expect("plan").collect().await.expect("execute");
    let mut rows: Vec<(Vec<u8>, Option<String>)> = batches
        .iter()
        .flat_map(|batch| {
            let span_ids = batch
                .column_by_name(COL_SPAN_ID)
                .expect("span_id column")
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("span_id is FixedSizeBinary(8)");
            let environments = batch
                .column_by_name(COL_ENVIRONMENT)
                .expect("environment column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("environment is Utf8");
            (0..batch.num_rows())
                .map(|row| {
                    let environment = (!environments.is_null(row)).then(|| environments.value(row).to_string());
                    (span_ids.value(row).to_vec(), environment)
                })
                .collect::<Vec<_>>()
        })
        .collect();
    rows.sort();
    rows
}

/// A query engine with the WAL scan on, reading the segments of `wal_store`.
fn build_query_engine(catalog: Arc<dyn Catalog>, wal_store: Arc<InMemory>) -> Arc<QueryEngine> {
    let engine_config = QueryEngineConfig {
        wal_query_enabled: true,
        ..QueryEngineConfig::default()
    };
    let wal_reader = Arc::new(
        ParquetQueueReader::new("", Arc::clone(&wal_store) as Arc<dyn ObjectStore>, 8_192).expect("wal reader"),
    );
    Arc::new(QueryEngine::new(
        catalog,
        engine_config,
        wal_store as Arc<dyn ObjectStore>,
        wal_reader,
    ))
}

/// The name a query uses for the `operations` table.
fn build_operations_source() -> String {
    format!("iceberg.{ICEGATE_NAMESPACE}.{OPERATIONS_TABLE}")
}

/// A session over storage of both layouts, with [`COMMITTED_OFFSET`] committed:
/// [`S1`] in a data file without the column, [`S0`] and [`S2`] in WAL segments
/// without it, [`S3`] and [`S4`] in a WAL segment with it.
async fn seed_storage_of_both_layouts() -> (SessionContext, tempfile::TempDir) {
    let (catalog, table, warehouse) = create_previous_operations_table().await;
    let previous_schema =
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).expect("previous arrow schema"));
    assert!(
        previous_schema.field_with_name(COL_ENVIRONMENT).is_err(),
        "the fixture must write storage lacking the column"
    );
    commit_data_file(
        &catalog,
        &table,
        build_operations_batch(&previous_schema, &[(S1, None)]),
    )
    .await;

    let table = add_environment_column(&catalog).await;
    let current_schema =
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).expect("current arrow schema"));

    let wal_store = Arc::new(InMemory::new());
    write_wal_segments(
        &wal_store,
        vec![
            build_operations_batch(&previous_schema, &[(S0, None)]),
            build_operations_batch(&previous_schema, &[(S2, None)]),
            build_operations_batch(&current_schema, &[(S3, Some("prod")), (S4, None)]),
        ],
    )
    .await;

    let engine = build_query_engine(catalog, wal_store);
    let session = engine.create_session().await.expect("session");
    (session, warehouse)
}

/// The nodes of `plan` that have no children: the scans of its data sources.
fn collect_plan_leaves(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    if children.is_empty() {
        return vec![Arc::clone(plan)];
    }
    children.into_iter().flat_map(collect_plan_leaves).collect()
}

#[tokio::test]
async fn queries_read_environment_as_null_where_a_wal_segment_or_data_file_lacks_it() {
    let (session, _warehouse) = seed_storage_of_both_layouts().await;

    let source = build_operations_source();
    let row = |k: u8, environment: Option<&str>| (build_span_id(k).to_vec(), environment.map(str::to_string));
    let cases = [
        (
            format!("SELECT span_id, environment FROM {source}"),
            vec![row(S1, None), row(S2, None), row(S3, Some("prod")), row(S4, None)],
        ),
        (
            format!("SELECT span_id, environment FROM {source} WHERE environment = 'prod'"),
            vec![row(S3, Some("prod"))],
        ),
        (
            format!("SELECT span_id, environment FROM {source} WHERE environment IS NULL"),
            vec![row(S1, None), row(S2, None), row(S4, None)],
        ),
        (
            format!("SELECT span_id, environment FROM {source} WHERE environment = 'absent'"),
            vec![],
        ),
        (
            format!("SELECT * FROM {source}"),
            vec![row(S1, None), row(S2, None), row(S3, Some("prod")), row(S4, None)],
        ),
    ];
    for (query, expected) in cases {
        assert_eq!(query_environments(&session, &query).await, expected, "{query}");
    }
}

/// Every other query also reads a column all the files hold; this one projects a
/// file lacking the column down to nothing, the case a reader may take for a file
/// with nothing to give.
#[tokio::test]
async fn a_query_reading_only_environment_keeps_the_rows_of_storage_lacking_the_column() {
    let (session, _warehouse) = seed_storage_of_both_layouts().await;
    let source = build_operations_source();

    let frame = session
        .sql(&format!(
            "SELECT environment, COUNT(*) AS row_count FROM {source} GROUP BY environment"
        ))
        .await
        .expect("plan");

    let plan = frame.clone().create_physical_plan().await.expect("physical plan");
    let leaves = collect_plan_leaves(&plan);
    assert_eq!(leaves.len(), 2, "the plan must scan both the Iceberg table and the WAL");
    for leaf in &leaves {
        let leaf_schema = leaf.schema();
        let column_names: Vec<&str> = leaf_schema.fields().iter().map(|field| field.name().as_str()).collect();
        assert_eq!(column_names, ["environment"], "{}", leaf.name());
    }

    let batches = frame.collect().await.expect("execute");
    let mut groups: Vec<(Option<String>, i64)> = batches
        .iter()
        .flat_map(|batch| {
            let environments = batch
                .column_by_name(COL_ENVIRONMENT)
                .expect("environment column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("environment is Utf8");
            let row_counts = batch
                .column_by_name("row_count")
                .expect("row_count column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("row_count is Int64");
            (0..batch.num_rows())
                .map(|row| {
                    let environment = (!environments.is_null(row)).then(|| environments.value(row).to_string());
                    (environment, row_counts.value(row))
                })
                .collect::<Vec<_>>()
        })
        .collect();
    groups.sort();
    assert_eq!(groups, [(None, 3), (Some("prod".to_string()), 1)]);
}

/// `migrate create` can run while a provider built before it still serves queries,
/// and a new ingest pod can write a segment with the column in that window. Only
/// one session is created, before the migration: a later one may take a rebuilt
/// provider once the cached one exceeds its maximum age.
#[tokio::test]
async fn a_provider_built_before_the_migration_reads_a_wal_segment_wider_than_its_schema() {
    let (catalog, table, _warehouse) = create_previous_operations_table().await;
    let previous_schema =
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).expect("previous arrow schema"));
    commit_data_file(
        &catalog,
        &table,
        build_operations_batch(&previous_schema, &[(S1, None)]),
    )
    .await;

    let wal_store = Arc::new(InMemory::new());
    let engine = build_query_engine(Arc::clone(&catalog), Arc::clone(&wal_store));
    let session = engine.create_session().await.expect("session");

    let table = add_environment_column(&catalog).await;
    let current_schema =
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).expect("current arrow schema"));
    assert!(
        current_schema.field_with_name(COL_ENVIRONMENT).is_ok(),
        "the fixture must write a segment wider than the provider's schema"
    );
    write_wal_segments(
        &wal_store,
        vec![
            build_operations_batch(&previous_schema, &[(S0, None)]),
            build_operations_batch(&current_schema, &[(S3, Some("prod")), (S4, None)]),
        ],
    )
    .await;

    let source = build_operations_source();
    let batches = session
        .sql(&format!("SELECT * FROM {source}"))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("execute");

    let mut span_ids: Vec<Vec<u8>> = batches
        .iter()
        .flat_map(|batch| {
            assert!(
                batch.column_by_name(COL_ENVIRONMENT).is_none(),
                "the provider must still answer with the layout it was built on"
            );
            batch
                .column_by_name(COL_SPAN_ID)
                .expect("span_id column")
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("span_id is FixedSizeBinary(8)")
                .iter()
                .map(|span_id| span_id.expect("span_id is required").to_vec())
                .collect::<Vec<_>>()
        })
        .collect();
    span_ids.sort();
    assert_eq!(
        span_ids,
        [
            build_span_id(S1).to_vec(),
            build_span_id(S3).to_vec(),
            build_span_id(S4).to_vec()
        ]
    );
}
