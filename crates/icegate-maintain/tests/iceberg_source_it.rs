//! Integration test for the Iceberg [`IcebergMergeSource`] compaction adapter
//! against `MinIO`.
//!
//! Seeds a `logs` table with TWO data files in one `(tenant, day)` partition,
//! each internally sorted by the logs sort order `(service_name ASC, timestamp
//! DESC)` and with OVERLAPPING `service_name` ranges so the k-way merge actually
//! interleaves them. The files are enumerated through
//! [`list_data_files_with_stats`], wrapped in [`MergeInput`]s (position = index
//! in sorted-by-`min_key` order, boundary range + size from the stats), and fed
//! to a [`RowGroupsMerger`] backed by [`IcebergMergeSource`]. The test asserts
//! the merged output preserves the row count and is globally sorted by the logs
//! sort order.
//!
//! Marked `#[ignore]`; run with Docker available:
//!
//! ```text
//! cargo test -p icegate-maintain --test iceberg_source_it -- --ignored --nocapture
//! ```

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_lines)]

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, MapArray, StringArray, StructArray, TimestampMicrosecondArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
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
use icegate_common::catalog::IoHandle;
use icegate_common::manifest_scan::{DataFileStats, list_data_files_with_stats};
use icegate_common::merge::sort_key::{SortColumnCache, SortColumnsDescriptor};
use icegate_common::merge::{MergeInput, RowGroupsMerger, SortedBatchMergerConfig};
use icegate_common::schema::{logs_partition_spec, logs_schema, logs_sort_order};
use icegate_common::testing::{MinIOContainer, create_s3_bucket};
use icegate_maintain::compact::iceberg_source::IcebergMergeSource;
use parquet::file::properties::WriterProperties;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const BUCKET_NAME: &str = "warehouse";
const NAMESPACE: &str = "icegate";
const TABLE: &str = "logs";
const TENANT: &str = "tenant-a";
/// 2026-06-11T00:00:00Z in microseconds. Both seeded files share this day so
/// they land in the same `(tenant_id, day)` partition.
const DAY_MICROS: i64 = 1_749_600_000_000_000;

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
        },
        file_io,
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
/// already laid out in the caller's order (the writer does not re-sort, so the
/// caller is responsible for emitting rows in `(service_name ASC, timestamp
/// DESC)` order to produce an internally sorted data file).
fn logs_batch(rows: &[(&str, i64)]) -> RecordBatch {
    let iceberg_schema = logs_schema().unwrap();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).unwrap());
    let n = rows.len();

    let tenant = StringArray::from(vec![TENANT; n]);
    let service_name = StringArray::from(rows.iter().map(|(s, _)| Some(*s)).collect::<Vec<_>>());
    let ts: Vec<i64> = rows.iter().map(|(_, t)| *t).collect();
    let trace_vals: Vec<[u8; 16]> = (0..n).map(|i| [u8::try_from(i % 256).unwrap(); 16]).collect();
    let trace_id = FixedSizeBinaryArray::try_from_iter(trace_vals.iter().map(|v| v.to_vec())).unwrap();
    let span_vals: Vec<[u8; 8]> = (0..n).map(|i| [u8::try_from(i % 256).unwrap(); 8]).collect();
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
                (0..n).map(|i| Some(format!("msg-{i}"))).collect::<Vec<_>>(),
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

/// `(service_name, timestamp)` pairs from a logs batch's first two sort columns,
/// flattened across the merged output stream, used to assert ordering and the
/// exact interleaved sequence.
fn service_and_timestamp_rows(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
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
            rows.push((services.value(i).to_string(), timestamps.value(i)));
        }
    }
    rows
}

#[tokio::test]
#[ignore = "requires Docker (MinIO); run with --ignored"]
async fn iceberg_merge_source_interleaves_two_overlapping_sorted_files() {
    let (_minio, conn) = setup_minio().await;
    let catalog = build_s3_catalog(&conn).await;
    let ident = create_logs_table(&catalog).await;

    // Two files in one partition, each internally sorted by
    // (service_name ASC, timestamp DESC), with OVERLAPPING service ranges
    // (svc-b and svc-c appear in both) so the merge must interleave them.
    let table = catalog.load_table(&ident).await.unwrap();
    let rows_f1: Vec<(&str, i64)> = vec![
        ("svc-a", DAY_MICROS + 30),
        ("svc-a", DAY_MICROS + 10),
        ("svc-b", DAY_MICROS + 40),
        ("svc-c", DAY_MICROS + 50),
    ];
    let rows_f2: Vec<(&str, i64)> = vec![
        ("svc-b", DAY_MICROS + 35),
        ("svc-b", DAY_MICROS + 5),
        ("svc-c", DAY_MICROS + 25),
        ("svc-d", DAY_MICROS + 60),
    ];
    let file_1 = write_one_file(&table, logs_batch(&rows_f1)).await;
    let file_2 = write_one_file(&table, logs_batch(&rows_f2)).await;
    let count_f1 = file_1.record_count();
    let count_f2 = file_2.record_count();
    assert_eq!(count_f1, rows_f1.len() as u64);
    assert_eq!(count_f2, rows_f2.len() as u64);

    // One append => one snapshot => both files enumerable from the manifest.
    fast_append(&catalog, &ident, vec![file_1.clone(), file_2.clone()]).await;

    let base = catalog.load_table(&ident).await.unwrap();
    let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");

    // Enumerate the two data files with their decoded sort-key envelopes.
    let mut stats: Vec<DataFileStats> = list_data_files_with_stats(&base, descriptor)
        .await
        .expect("list data files with stats");
    assert_eq!(stats.len(), 2, "two seeded data files must be enumerated");

    // Assign positions in sorted-by-min_key order (the rewrite task's contract).
    stats.sort_by(|a, b| a.min_key().compare(b.min_key()));

    let mut paths_by_position: HashMap<u128, String> = HashMap::new();
    let mut inputs: Vec<MergeInput> = Vec::with_capacity(stats.len());
    for (position, stat) in stats.iter().enumerate() {
        let position = position as u128;
        paths_by_position.insert(position, stat.data_file.file_path().to_string());
        inputs.push(MergeInput::new(
            position,
            stat.size_bytes(),
            stat.boundary_range().clone(),
        ));
    }

    // Build the source over the table's FileIO + the position→path map, and run
    // the k-way merger.
    let cancel_token = CancellationToken::new();
    let source = IcebergMergeSource::new(base.file_io().clone(), paths_by_position);
    let merger = RowGroupsMerger::new(
        Arc::new(source),
        inputs,
        SortedBatchMergerConfig {
            row_group_size: 1024,
            read_parallelism: 2,
            cancel_token,
            sort_descriptor: descriptor,
        },
    )
    .expect("build merger");

    let merged_batches: Vec<RecordBatch> = merger.into_stream().try_collect().await.expect("collect merged stream");

    // 1. Row count is preserved: total == rows(f1) + rows(f2).
    let total_merged: usize = merged_batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        total_merged as u64,
        count_f1 + count_f2,
        "merged rows must equal rows(f1) + rows(f2)"
    );

    // 2. The output is globally sorted by (service_name ASC, timestamp DESC).
    //    Compare every adjacent pair of rows across the whole stream using the
    //    descriptor's own ordering, including across batch boundaries.
    let caches: Vec<SortColumnCache> = merged_batches
        .iter()
        .map(|batch| SortColumnCache::try_new(batch, descriptor, "merged output").expect("sort cache"))
        .collect();
    let mut prev: Option<(usize, usize)> = None; // (batch_idx, row_idx)
    for (batch_idx, batch) in merged_batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            if let Some((prev_batch, prev_row)) = prev {
                let ordering = caches[prev_batch]
                    .compare_row(prev_row, &caches[batch_idx], row_idx)
                    .expect("compare adjacent rows");
                assert_ne!(
                    ordering,
                    Ordering::Greater,
                    "merged output must be globally non-decreasing in sort order \
                     (violation between batch {prev_batch} row {prev_row} and batch {batch_idx} row {row_idx})"
                );
            }
            prev = Some((batch_idx, row_idx));
        }
    }

    // 3. The merged sequence is exactly the globally sorted interleave of both
    //    files' (service_name ASC, timestamp DESC) rows.
    let mut expected: Vec<(String, i64)> = rows_f1
        .iter()
        .chain(rows_f2.iter())
        .map(|(s, t)| ((*s).to_string(), *t))
        .collect();
    // service_name ASC, then timestamp DESC.
    expected.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
    let actual = service_and_timestamp_rows(&merged_batches);
    assert_eq!(
        actual, expected,
        "merged output must be the globally sorted interleave of both files"
    );
}
