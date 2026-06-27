//! Integration test for the compaction REWRITE executor
//! ([`RewriteExecutor`]) against `MinIO`.
//!
//! Seeds a `logs` table with several small data files in ONE `(tenant, day)`
//! partition, each internally sorted by the logs sort order `(service_name ASC,
//! timestamp DESC)`, and commits them in a single `fast_append` so they share
//! one snapshot. A [`RewriteExecutor`] built for `logs` then runs one
//! [`RewriteInput`] listing those files (base = the table's current `main`
//! snapshot id) and the test asserts:
//!
//! * the outcome is [`RewriteOutcome::Committed`];
//! * after reload the partition has STRICTLY FEWER data files;
//! * the total row count is unchanged;
//! * the full row set and the per-key order are identical before vs after the
//!   rewrite (both read back through the table's parquet and compared).
//!
//! Marked `#[ignore]`; run with Docker available:
//!
//! ```text
//! cargo test -p icegate-maintain --test rewrite_it -- --ignored --nocapture
//! ```

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, MapArray, StringArray, StructArray, TimestampMicrosecondArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use iceberg::arrow::ArrowFileReader;
use iceberg::io::FileIOBuilder;
use iceberg::spec::{DataFile, DataFileFormat};
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
use icegate_common::WAL_OFFSET_PROPERTY;
use icegate_common::catalog::IoHandle;
use icegate_common::iceberg_write::WriteConfig;
use icegate_common::manifest_scan::{DataFileStats, list_data_files_with_stats};
use icegate_common::merge::sort_key::SortColumnsDescriptor;
use icegate_common::parquet_encoding::{LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS};
use icegate_common::schema::{logs_partition_spec, logs_schema, logs_sort_order};
use icegate_common::testing::{MinIOContainer, create_s3_bucket};
use icegate_maintain::compact::metrics::CompactMetrics;
use icegate_maintain::compact::rewrite::{RewriteExecutor, RewriteInput, RewriteOutcome};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const BUCKET_NAME: &str = "warehouse";
const NAMESPACE: &str = "icegate";
const TABLE: &str = "logs";
const TENANT: &str = "tenant-a";
/// 2026-06-11T00:00:00Z in microseconds. Every seeded file shares this day so
/// they all land in the same `(tenant_id, day)` partition.
const DAY_MICROS: i64 = 1_749_600_000_000_000;
/// Output Parquet target size in bytes (128 mebibytes). It is far larger than
/// the tiny seeded data, so the whole rewrite lands in one output file,
/// exercising the many-to-one happy path.
const TARGET_FILE_SIZE_BYTES: u64 = 128 * 1024 * 1024;

/// Connection parameters for a running `MinIO`.
#[derive(Clone)]
struct MinioConn {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

/// Stand up `MinIO` and capture its connection parameters.
async fn setup_minio() -> (MinIOContainer, MinioConn) {
    let minio = MinIOContainer::builder().start().await.expect("start MinIO");
    create_s3_bucket(minio.endpoint(), BUCKET_NAME).await.expect("create bucket");
    let conn = MinioConn {
        endpoint: minio.endpoint().to_string(),
        access_key: minio.username().to_string(),
        secret_key: minio.password().to_string(),
    };
    (minio, conn)
}

/// Build a concrete [`S3Catalog`] against `MinIO`.
async fn build_s3_catalog(conn: &MinioConn) -> S3Catalog {
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

/// Create the namespace and `logs` table, returning the table identifier.
async fn create_logs_table(catalog: &S3Catalog) -> TableIdent {
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
    catalog
        .create_table(&NamespaceIdent::new(NAMESPACE.to_string()), creation)
        .await
        .expect("create logs table");
    TableIdent::new(NamespaceIdent::new(NAMESPACE.to_string()), TABLE.to_string())
}

/// Build one `logs` Arrow batch from `(service_name, timestamp_micros)` rows,
/// laid out in the caller's order (the writer does not re-sort, so the caller
/// must emit rows already in `(service_name ASC, timestamp DESC)` order to
/// produce an internally sorted data file). The `body` column is `msg-<unique>`
/// so every row is globally distinguishable when comparing the set before vs
/// after.
fn logs_batch(rows: &[(&str, i64)], unique_offset: usize) -> RecordBatch {
    let iceberg_schema = logs_schema().unwrap();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).unwrap());
    let n = rows.len();

    let tenant = StringArray::from(vec![TENANT; n]);
    let service_name = StringArray::from(rows.iter().map(|(s, _)| Some(*s)).collect::<Vec<_>>());
    let ts: Vec<i64> = rows.iter().map(|(_, t)| *t).collect();
    let trace_vals: Vec<[u8; 16]> = (0..n).map(|i| [u8::try_from((unique_offset + i) % 256).unwrap(); 16]).collect();
    let trace_id = FixedSizeBinaryArray::try_from_iter(trace_vals.iter().map(|v| v.to_vec())).unwrap();
    let span_vals: Vec<[u8; 8]> = (0..n).map(|i| [u8::try_from((unique_offset + i) % 256).unwrap(); 8]).collect();
    let span_id = FixedSizeBinaryArray::try_from_iter(span_vals.iter().map(|v| v.to_vec())).unwrap();
    let attributes = empty_string_map(&arrow_schema, n);

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
            Arc::new(StringArray::from(vec![Some("INFO"); n])),
            Arc::new(StringArray::from(
                (0..n).map(|i| Some(format!("msg-{}", unique_offset + i))).collect::<Vec<_>>(),
            )),
            attributes,
        ],
    )
    .expect("record batch")
}

/// Build an empty `MAP<Utf8,Utf8>` column of length `rows` typed exactly like
/// the `attributes` field of the iceberg-derived arrow schema.
fn empty_string_map(arrow_schema: &ArrowSchema, rows: usize) -> Arc<dyn Array> {
    let attr_field = arrow_schema.field_with_name("attributes").unwrap();
    let DataType::Map(entry_field, ordered) = attr_field.data_type() else {
        panic!("attributes must be a Map");
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

/// Write a batch into the table as parquet (one file per partition); a single
/// `(tenant, day)` batch yields exactly one [`DataFile`].
async fn write_one_file(table: &Table, batch: RecordBatch) -> DataFile {
    let metadata = table.metadata().clone();
    let file_io = table.file_io().clone();
    let location_generator = DefaultLocationGenerator::new(metadata.clone()).unwrap();
    let file_name_generator = DefaultFileNameGenerator::new(Uuid::now_v7().to_string(), None, DataFileFormat::Parquet);
    let parquet_builder =
        ParquetWriterBuilder::new(WriterProperties::builder().build(), metadata.current_schema().clone());
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        1024 * 1024 * 1024,
        file_io,
        location_generator,
        file_name_generator,
    );
    let mut fanout = FanoutWriter::new(DataFileWriterBuilder::new(rolling_builder));
    let splitter = iceberg::arrow::RecordBatchPartitionSplitter::try_new_with_computed_values(
        metadata.current_schema().clone(),
        metadata.default_partition_spec().clone(),
    )
    .unwrap();
    for (partition_key, partition_batch) in splitter.split(&batch).unwrap() {
        fanout.write(partition_key, partition_batch).await.unwrap();
    }
    let mut files = fanout.close().await.unwrap();
    assert_eq!(files.len(), 1, "single-partition batch must yield one data file");
    files.pop().unwrap()
}

/// Commit data files with a public `fast_append` (all files in one snapshot).
async fn fast_append(catalog: &S3Catalog, ident: &TableIdent, data_files: Vec<DataFile>) {
    let table = catalog.load_table(ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).unwrap();
    tx.commit(catalog).await.unwrap();
}

/// Commit data files with a `fast_append` that ALSO stamps the WAL offset onto
/// the snapshot summary, mirroring the Shifter's commit. Used to seed the base
/// snapshot a compaction must carry the offset forward from.
async fn fast_append_with_offset(catalog: &S3Catalog, ident: &TableIdent, data_files: Vec<DataFile>, offset: u64) {
    let table = catalog.load_table(ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx
        .fast_append()
        .add_data_files(data_files)
        .set_snapshot_properties(HashMap::from([(WAL_OFFSET_PROPERTY.to_string(), offset.to_string())]));
    let tx = action.apply(tx).unwrap();
    tx.commit(catalog).await.unwrap();
}

/// A flattened logs row used for the before/after set comparison: the body
/// (a globally unique `msg-<n>` tag) plus the two sort-key columns.
type LogRow = (String, String, i64); // (body, service_name, timestamp)

/// Read every row of the table's data files back through the table's `FileIO`,
/// flattened into `(body, service_name, timestamp)` in physical order.
async fn read_all_rows(table: &Table, descriptor: &SortColumnsDescriptor) -> Vec<LogRow> {
    let stats = list_data_files_with_stats(table, descriptor).await.expect("list files");
    let file_io = table.file_io();
    let mut rows: Vec<LogRow> = Vec::new();
    for stat in &stats {
        let path = stat.data_file.file_path();
        let input = file_io.new_input(path).expect("open data file");
        let meta = input.metadata().await.expect("stat data file");
        let reader = input.reader().await.expect("data file reader");
        let arrow_reader = ArrowFileReader::new(meta, reader);
        let mut stream = ParquetRecordBatchStreamBuilder::new(arrow_reader)
            .await
            .expect("parquet builder")
            .build()
            .expect("parquet stream");
        while let Some(batch) = stream.try_next().await.expect("read batch") {
            append_rows(&batch, &mut rows);
        }
    }
    rows
}

/// Flatten one logs batch's `body`/`service_name`/`timestamp` into `rows`.
fn append_rows(batch: &RecordBatch, rows: &mut Vec<LogRow>) {
    let messages = batch
        .column_by_name("body")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let services = batch
        .column_by_name("service_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let timestamps = batch
        .column_by_name("timestamp")
        .unwrap()
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    for i in 0..batch.num_rows() {
        rows.push((
            messages.value(i).to_string(),
            services.value(i).to_string(),
            timestamps.value(i),
        ));
    }
}

/// Count the live data files in the table's current snapshot.
async fn data_file_count(table: &Table, descriptor: &SortColumnsDescriptor) -> usize {
    list_data_files_with_stats(table, descriptor).await.expect("list files").len()
}

#[tokio::test]
#[ignore = "requires Docker (MinIO); run with --ignored"]
async fn rewrite_executor_compacts_partition_preserving_rows_and_order() {
    let (_minio, conn) = setup_minio().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;
    let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");

    // Seed FOUR small, internally-sorted files in one (tenant, day) partition.
    // Each file is sorted by (service_name ASC, timestamp DESC); the service
    // ranges overlap across files so the merge genuinely interleaves them.
    let table = catalog.load_table(&ident).await.unwrap();
    let files_rows: Vec<Vec<(&str, i64)>> = vec![
        vec![
            ("svc-a", DAY_MICROS + 30),
            ("svc-a", DAY_MICROS + 10),
            ("svc-c", DAY_MICROS + 50),
        ],
        vec![
            ("svc-b", DAY_MICROS + 40),
            ("svc-b", DAY_MICROS + 5),
            ("svc-d", DAY_MICROS + 60),
        ],
        vec![
            ("svc-a", DAY_MICROS + 20),
            ("svc-c", DAY_MICROS + 25),
            ("svc-c", DAY_MICROS + 15),
        ],
        vec![
            ("svc-b", DAY_MICROS + 35),
            ("svc-d", DAY_MICROS + 70),
            ("svc-d", DAY_MICROS + 12),
        ],
    ];

    let mut seeded_files: Vec<DataFile> = Vec::new();
    let mut unique_offset = 0usize;
    for rows in &files_rows {
        let file = write_one_file(&table, logs_batch(rows, unique_offset)).await;
        unique_offset += rows.len();
        seeded_files.push(file);
    }
    let seeded_rows_total: u64 = seeded_files.iter().map(DataFile::record_count).sum();

    // One append => one snapshot => all four files enumerable from the manifest.
    fast_append(&catalog, &ident, seeded_files.clone()).await;

    // Capture the pre-rewrite state: file count and full row set.
    let before = catalog.load_table(&ident).await.unwrap();
    let files_before = data_file_count(&before, descriptor).await;
    assert_eq!(files_before, files_rows.len(), "all seeded files must be enumerable");
    let mut rows_before = read_all_rows(&before, descriptor).await;
    assert_eq!(
        rows_before.len() as u64,
        seeded_rows_total,
        "all seeded rows present before"
    );

    // Enumerate the partition's files to build the rewrite input (sorted by
    // min_key, which is the executor's position contract).
    let mut stats: Vec<DataFileStats> = list_data_files_with_stats(&before, descriptor).await.expect("list stats");
    stats.sort_by(|left, right| left.min_key().compare(right.min_key()));
    let partition_key = stats[0].partition_key().to_string();
    let input_file_paths: Vec<String> = stats.iter().map(|s| s.data_file.file_path().to_string()).collect();

    // Build the executor for logs with the real logs encoding policy.
    let write_cfg = WriteConfig {
        row_group_size: 20_000,
        data_page_size_limit_bytes: 2 * 1024 * 1024,
        max_file_size_bytes: TARGET_FILE_SIZE_BYTES,
        bloom_filter_columns: LOGS_BLOOM_COLUMNS,
        column_encodings: LOGS_COLUMN_ENCODINGS,
    };
    // The executor commits over `dyn Catalog`; coerce the concrete S3 catalog
    // used for seeding into the generic trait object.
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let executor = RewriteExecutor::new(dyn_catalog, write_cfg, descriptor, CompactMetrics::new());

    let rewrite_input = RewriteInput {
        table: TABLE.to_string(),
        partition_key,
        input_file_paths,
    };

    let cancel = CancellationToken::new();
    let outcome = executor.execute(&rewrite_input, &cancel).await.expect("execute rewrite");

    // 1. The rewrite committed: many inputs -> at least one (here one) output.
    match outcome {
        RewriteOutcome::Committed {
            input_files,
            output_files,
            rows,
            ..
        } => {
            assert_eq!(input_files, files_rows.len(), "all inputs removed");
            assert!(output_files >= 1, "at least one output file written");
            assert!(output_files < input_files, "rewrite must reduce the file count");
            assert_eq!(rows as u64, seeded_rows_total, "output rows == input rows");
        }
        RewriteOutcome::Aborted => panic!("rewrite must commit, not abort"),
    }

    // 2. After reload the partition has STRICTLY FEWER data files.
    let after = catalog.load_table(&ident).await.unwrap();
    let files_after = data_file_count(&after, descriptor).await;
    assert!(
        files_after < files_before,
        "compaction must reduce file count: before={files_before}, after={files_after}"
    );

    // 3. Total row count is unchanged.
    let mut rows_after = read_all_rows(&after, descriptor).await;
    assert_eq!(
        rows_after.len() as u64,
        seeded_rows_total,
        "row count must be preserved across the rewrite"
    );

    // 4a. The full row SET is identical before vs after (sort both by the unique
    //     message tag and compare element-by-element).
    rows_before.sort();
    rows_after.sort();
    assert_eq!(rows_before, rows_after, "row set must be identical before vs after");

    // 4b. The PER-KEY order is identical: reading the post-rewrite files in
    //     physical order yields rows in (service_name ASC, timestamp DESC)
    //     order, i.e. the merged output is globally sorted by the logs sort key.
    let physical_after = read_all_rows(&after, descriptor).await;
    let mut expected_sorted = physical_after.clone();
    // service_name ASC, then timestamp DESC.
    expected_sorted.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| b.2.cmp(&a.2)));
    assert_eq!(
        physical_after, expected_sorted,
        "post-rewrite physical order must be globally sorted by (service_name ASC, timestamp DESC)"
    );
}

/// The compaction `replace` snapshot MUST carry the Shifter's WAL offset forward
/// (via `inherit_summary_property`). Without it, after a compaction the current
/// snapshot would carry no offset and — under Nessie's severed parent chain —
/// the Shifter would resume from 0 and re-commit the entire WAL, duplicating
/// every row. This guards that the upstream-fork `inherit_summary_property`
/// behaviour the fix relies on does not silently regress on a fork bump.
#[tokio::test]
#[ignore = "requires Docker (MinIO); run with --ignored"]
async fn rewrite_carries_wal_offset_property_forward() {
    const SEED_OFFSET: u64 = 4242;

    let (_minio, conn) = setup_minio().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;
    let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");

    // Seed several small files in one partition (enough for the planner to
    // compact), committed in one snapshot stamped with the WAL offset.
    let table = catalog.load_table(&ident).await.unwrap();
    let files_rows: Vec<Vec<(&str, i64)>> = vec![
        vec![("svc-a", DAY_MICROS + 30), ("svc-c", DAY_MICROS + 50)],
        vec![("svc-b", DAY_MICROS + 40), ("svc-d", DAY_MICROS + 60)],
        vec![("svc-a", DAY_MICROS + 20), ("svc-c", DAY_MICROS + 15)],
    ];
    let mut seeded_files: Vec<DataFile> = Vec::new();
    let mut unique_offset = 0usize;
    for rows in &files_rows {
        let file = write_one_file(&table, logs_batch(rows, unique_offset)).await;
        unique_offset += rows.len();
        seeded_files.push(file);
    }
    fast_append_with_offset(&catalog, &ident, seeded_files, SEED_OFFSET).await;

    // Precondition: the base snapshot carries the offset.
    let before = catalog.load_table(&ident).await.unwrap();
    assert_eq!(
        before
            .metadata()
            .current_snapshot()
            .unwrap()
            .summary()
            .additional_properties
            .get(WAL_OFFSET_PROPERTY)
            .map(String::as_str),
        Some(SEED_OFFSET.to_string().as_str()),
        "base snapshot must carry the seeded offset",
    );

    // Build and run one rewrite over the seeded partition.
    let mut stats: Vec<DataFileStats> = list_data_files_with_stats(&before, descriptor).await.expect("list stats");
    stats.sort_by(|left, right| left.min_key().compare(right.min_key()));
    let partition_key = stats[0].partition_key().to_string();
    let input_file_paths: Vec<String> = stats.iter().map(|s| s.data_file.file_path().to_string()).collect();

    let write_cfg = WriteConfig {
        row_group_size: 20_000,
        data_page_size_limit_bytes: 2 * 1024 * 1024,
        max_file_size_bytes: TARGET_FILE_SIZE_BYTES,
        bloom_filter_columns: LOGS_BLOOM_COLUMNS,
        column_encodings: LOGS_COLUMN_ENCODINGS,
    };
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let executor = RewriteExecutor::new(dyn_catalog, write_cfg, descriptor, CompactMetrics::new());
    let rewrite_input = RewriteInput {
        table: TABLE.to_string(),
        partition_key,
        input_file_paths,
    };
    let cancel = CancellationToken::new();
    let outcome = executor.execute(&rewrite_input, &cancel).await.expect("execute rewrite");
    assert!(
        matches!(outcome, RewriteOutcome::Committed { .. }),
        "rewrite must commit, not abort",
    );

    // THE ASSERTION: the new `replace` snapshot carries the SAME offset forward.
    let after = catalog.load_table(&ident).await.unwrap();
    let carried = after
        .metadata()
        .current_snapshot()
        .unwrap()
        .summary()
        .additional_properties
        .get(WAL_OFFSET_PROPERTY)
        .cloned();
    assert_eq!(
        carried.as_deref(),
        Some(SEED_OFFSET.to_string().as_str()),
        "compaction replace snapshot must carry the WAL offset forward",
    );
}
