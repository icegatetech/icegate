//! Integration test for [`icegate_common::list_data_files_with_stats`].
//!
//! Stands up an object store and the owned `S3Catalog`, creates the `logs` table, seeds
//! TWO data files in one `(tenant_id, day)` partition via parquet-write +
//! `Transaction::fast_append`, then enumerates them with decoded sort-key
//! bounds and asserts the per-file envelope shape and ordering.
//!
//! The object store + S3 catalog + writer harness is cribbed from the Phase-0
//! compaction spike (`icegate-maintain`'s `compaction_replace_spike_test`),
//! trimmed to seeding and reading. Requires Docker (a prerequisite) to run the
//! object-storage container.
// Test-only seed harness: small row-index casts to fixed-width byte/timestamp
// fillers are inherent to building the Arrow batch and cannot overflow at the
// row counts used here.
#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::doc_markdown
)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use iceberg::io::FileIOBuilder;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use icegate_catalog_s3::{CatalogCodecKind, S3Catalog, S3CatalogConfig};
use icegate_common::catalog::IoHandle;
use icegate_common::list_data_files_with_stats;
use icegate_common::merge::sort_key::SortColumnsDescriptor;
use icegate_common::schema::{
    COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, logs_partition_spec, logs_schema,
    logs_sort_order,
};
use icegate_common::testing::{S3TestContainer, create_s3_bucket};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

const BUCKET_NAME: &str = "warehouse";
const NAMESPACE: &str = "icegate";
const TABLE: &str = "logs";
const TENANT: &str = "tenant-a";

/// Connection parameters for a running object store.
struct StorageConn {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

/// Stand up object storage and capture its connection parameters. No Nessie: the
/// `S3Catalog` stores `root.json` in the same bucket via compare-and-swap.
async fn setup_object_store() -> (S3TestContainer, StorageConn) {
    let store = S3TestContainer::start().await.expect("start object storage");
    create_s3_bucket(store.endpoint(), BUCKET_NAME).await.expect("create bucket");
    let conn = StorageConn {
        endpoint: store.endpoint().to_string(),
        access_key: store.username().to_string(),
        secret_key: store.password().to_string(),
    };
    (store, conn)
}

/// Build a concrete [`S3Catalog`] against the object store with a `FileIO` over the same
/// OpenDAL S3 backend the production builder uses.
async fn build_s3_catalog(conn: &StorageConn) -> S3Catalog {
    let io = IoHandle::noop();
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert("warehouse".to_string(), format!("s3://{BUCKET_NAME}"));
    props.insert("s3.endpoint".to_string(), conn.endpoint.clone());
    props.insert("s3.path-style-access".to_string(), "true".to_string());
    props.insert("s3.access-key-id".to_string(), conn.access_key.clone());
    props.insert("s3.secret-access-key".to_string(), conn.secret_key.clone());
    props.insert("s3.region".to_string(), "us-east-1".to_string());
    let file_io = FileIOBuilder::new(io.storage_factory()).with_props(props).build();

    S3Catalog::new(
        S3CatalogConfig {
            bucket: BUCKET_NAME.to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some(conn.endpoint.clone()),
            access_key_id: Some(conn.access_key.clone()),
            secret_access_key: Some(conn.secret_key.clone()),
            warehouse: BUCKET_NAME.to_string(),
            codec: CatalogCodecKind::Json,
            ..S3CatalogConfig::default()
        },
        file_io,
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .expect("build S3 catalog")
}

/// Build one Arrow `RecordBatch` for the `logs` table containing `rows` rows in
/// a single `(tenant_id, day)` partition. The schema is derived from the iceberg
/// `logs` schema so field names, order, nullability, and field-id metadata match
/// exactly what the writer expects.
///
/// Deriving the schema independently of the created table is safe here because this suite
/// drives the real `S3Catalog`, which preserves the caller's field ids verbatim on
/// `create_table` (see `IcebergTableMetadata::create` in
/// `icegate-catalog-s3/src/domain/root.rs`, pinned by its `create_preserves_caller_field_ids`
/// regression test). That is not true of every catalog backend: the `Memory` backend
/// (plain upstream `MemoryCatalogBuilder`, used by `icegate-ingest`'s shift-pipeline tests)
/// reassigns nested map field ids on `create_table`, so fixtures that exercise it must build
/// from the created table's own schema instead — see `icegate-ingest`'s
/// `shift::test_utils::logs_ingest_batch_with_schema`.
fn logs_batch(service: &str, base_micros: i64, rows: usize) -> RecordBatch {
    let iceberg_schema = logs_schema().unwrap();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).unwrap());

    let tenant = StringArray::from(vec![TENANT; rows]);
    let service_name = StringArray::from(vec![Some(service); rows]);
    let ts: Vec<i64> = (0..rows as i64).map(|i| base_micros + i).collect();

    let trace_vals: Vec<[u8; 16]> = (0..rows).map(|i| [i as u8; 16]).collect();
    let trace_id = FixedSizeBinaryArray::try_from_iter(trace_vals.iter().map(|v| v.to_vec())).unwrap();
    let span_vals: Vec<[u8; 8]> = (0..rows).map(|i| [i as u8; 8]).collect();
    let span_id = FixedSizeBinaryArray::try_from_iter(span_vals.iter().map(|v| v.to_vec())).unwrap();

    let severity = StringArray::from(vec![Some("INFO"); rows]);
    let body = StringArray::from((0..rows).map(|i| Some(format!("msg-{service}-{i}"))).collect::<Vec<_>>());
    let resource_attributes = build_empty_string_map(&arrow_schema, COL_RESOURCE_ATTRIBUTES, rows);
    let scope_attributes = build_empty_string_map(&arrow_schema, COL_SCOPE_ATTRIBUTES, rows);
    let log_attributes = build_empty_string_map(&arrow_schema, COL_LOG_ATTRIBUTES, rows);

    RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(tenant),
            Arc::new(service_name),
            Arc::new(TimestampMicrosecondArray::from(ts.clone())),
            Arc::new(TimestampMicrosecondArray::from(ts.clone())),
            Arc::new(TimestampMicrosecondArray::from(ts)),
            Arc::new(trace_id),
            Arc::new(span_id),
            Arc::new(severity),
            Arc::new(body),
            resource_attributes,
            scope_attributes,
            log_attributes,
        ],
    )
    .expect("record batch")
}

/// Build an empty `MAP<Utf8,Utf8>` column of length `rows` whose type is exactly
/// the named attribute-map field of the target arrow schema (including the
/// `PARQUET:field_id` metadata on the key/value sub-fields and value
/// nullability), so `RecordBatch::try_new` accepts it.
fn build_empty_string_map(arrow_schema: &ArrowSchema, column: &str, rows: usize) -> Arc<dyn Array> {
    use arrow::array::{MapArray, StructArray};
    use arrow::buffer::OffsetBuffer;

    let attr_field = arrow_schema
        .field_with_name(column)
        .unwrap_or_else(|_| panic!("{column} field"));
    let DataType::Map(entry_field, ordered) = attr_field.data_type() else {
        panic!("{column} must be a Map");
    };
    let DataType::Struct(kv_fields) = entry_field.data_type() else {
        panic!("map entry must be a Struct");
    };
    let empty_key: Arc<dyn Array> = Arc::new(StringArray::from(Vec::<&str>::new()));
    let empty_value: Arc<dyn Array> = Arc::new(StringArray::from(Vec::<&str>::new()));
    let entries = StructArray::new(kv_fields.clone(), vec![empty_key, empty_value], None);
    let offsets = OffsetBuffer::new(vec![0_i32; rows + 1].into());
    Arc::new(MapArray::new(entry_field.clone(), offsets, entries, None, *ordered))
}

/// Write one `RecordBatch` into the table as parquet data files and return them,
/// routing by partition so all rows land in one `(tenant, day)` partition file.
async fn write_data_files(table: &Table, batch: RecordBatch) -> Vec<DataFile> {
    let metadata = table.metadata().clone();
    let file_io = table.file_io().clone();

    let location_generator = DefaultLocationGenerator::new(&metadata).unwrap();
    let file_name_generator =
        DefaultFileNameGenerator::new(Uuid::now_v7().to_string(), None, iceberg::spec::DataFileFormat::Parquet);

    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), metadata.current_schema().clone());
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        1024 * 1024 * 1024,
        file_io,
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_builder);
    let mut fanout = FanoutWriter::new(data_file_writer_builder);

    let splitter = iceberg::arrow::RecordBatchPartitionSplitter::try_new_with_computed_values(
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
    )
    .unwrap();
    for (partition_key, partition_batch) in splitter.split(&batch).unwrap() {
        fanout.write(partition_key, partition_batch).await.unwrap();
    }
    fanout.close().await.unwrap()
}

/// Commit data files with a public `fast_append`.
async fn commit_fast_append(catalog: &S3Catalog, ident: &TableIdent, data_files: Vec<DataFile>) {
    let table = catalog.load_table(ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).unwrap();
    tx.commit(catalog).await.unwrap();
}

#[tokio::test]
async fn list_data_files_with_stats_enumerates_two_seeded_files() {
    let (_store, conn) = setup_object_store().await;
    let catalog = build_s3_catalog(&conn).await;

    // --- create namespace + logs table -------------------------------------
    catalog
        .create_namespace(&NamespaceIdent::new(NAMESPACE.to_string()), HashMap::new())
        .await
        .expect("create namespace");

    let schema = logs_schema().unwrap();
    let partition_spec = logs_partition_spec(&schema).unwrap();
    let sort_order = logs_sort_order(&schema).unwrap();
    let creation = TableCreation::builder()
        .name(TABLE.to_string())
        .schema(schema.clone())
        .partition_spec(partition_spec.into_unbound())
        .sort_order(sort_order)
        .build();
    let table = catalog
        .create_table(&NamespaceIdent::new(NAMESPACE.to_string()), creation)
        .await
        .expect("create logs table");
    let table_ident = TableIdent::new(NamespaceIdent::new(NAMESPACE.to_string()), TABLE.to_string());

    // --- seed TWO data files in one partition via two fast appends ----------
    let day_micros: i64 = 1_749_600_000_000_000; // 2026-06-11T00:00:00Z in micros
    let files_a = write_data_files(&table, logs_batch("svc-a", day_micros, 3)).await;
    commit_fast_append(&catalog, &table_ident, files_a).await;

    let table = catalog.load_table(&table_ident).await.unwrap();
    let files_b = write_data_files(&table, logs_batch("svc-b", day_micros + 1000, 4)).await;
    commit_fast_append(&catalog, &table_ident, files_b).await;

    // --- enumerate with sort-key bounds ------------------------------------
    let table = catalog.load_table(&table_ident).await.unwrap();
    let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");
    let stats = list_data_files_with_stats(&table, descriptor)
        .await
        .expect("list data files with stats");

    // Exactly the two seeded files.
    assert_eq!(stats.len(), 2, "expected exactly two data files at current snapshot");

    for file_stats in &stats {
        // Each file holds rows.
        assert!(
            file_stats.data_file.record_count() > 0,
            "data file must report a positive record count"
        );

        // The logs sort key is (service_name, timestamp): arity 2 on both keys.
        assert_eq!(
            file_stats.min_key().components().len(),
            2,
            "min_key must carry both sort columns"
        );
        assert_eq!(
            file_stats.max_key().components().len(),
            2,
            "max_key must carry both sort columns"
        );

        // Both files set non-null service_name and timestamp, so neither
        // component decodes to None.
        for component in file_stats.min_key().components() {
            assert!(component.value.is_some(), "min_key component must be non-None");
        }
        for component in file_stats.max_key().components() {
            assert!(component.value.is_some(), "max_key component must be non-None");
        }

        // The envelope must be ordered under the sort-aware comparison.
        assert_ne!(
            file_stats.min_key().compare(file_stats.max_key()),
            std::cmp::Ordering::Greater,
            "min_key must not sort after max_key"
        );

        // A positive size feeds the planner's bin-packing budget.
        assert!(file_stats.size_bytes() > 0, "data file must report a positive size");
    }

    // Both seeded files share the single (tenant-a, 2026-06-11) partition, so
    // their derived partition keys must be identical and non-empty.
    let partition_keys: std::collections::BTreeSet<&str> =
        stats.iter().map(icegate_common::DataFileStats::partition_key).collect();
    assert_eq!(
        partition_keys.len(),
        1,
        "both files live in one partition, so their partition keys must match"
    );
    let only_key = partition_keys.iter().next().expect("one partition key");
    assert!(!only_key.is_empty(), "partition key must not be empty");
}
