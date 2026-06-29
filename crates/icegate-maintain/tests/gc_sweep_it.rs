//! Integration tests for the orphan-file garbage-collection sweep against `MinIO`.
//!
//! Drives `run_sweep` directly (deterministic, fast) for five targeted tests,
//! and one end-to-end test that drives the full [`GcRunner`] background loop.
//! All tests are `#[ignore]`d; run with Docker available:
//!
//! ```text
//! cargo test -p icegate-maintain --test gc_sweep_it -- --ignored --nocapture
//! ```
//!
//! If `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` are unset, the sweep's object
//! store can't authenticate and tests panic with an `InvalidAccessKeyId` list error.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, FixedSizeBinaryArray, MapArray, StringArray, StructArray, TimestampMicrosecondArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use futures::StreamExt;
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
use icegate_common::catalog::IoHandle;
use icegate_common::manifest_scan::list_data_files_with_stats;
use icegate_common::merge::sort_key::SortColumnsDescriptor;
use icegate_common::schema::{logs_partition_spec, logs_schema, logs_sort_order};
use icegate_common::storage::{S3Config, StorageBackend, StorageConfig};
use icegate_common::testing::{MinIOContainer, create_s3_bucket, create_s3_object_store};
use icegate_maintain::gc::config::GcOrphansConfig;
use icegate_maintain::gc::metrics::GcMetrics;
use icegate_maintain::gc::sweep::run_sweep;
use object_store::ObjectStore;
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
/// laid out in the caller's order. The `body` column is `msg-<unique>` so every
/// row is globally distinguishable.
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

/// Commit a single data file in its own `fast_append` (its own snapshot).
async fn fast_append_one(catalog: &S3Catalog, ident: &TableIdent, data_file: DataFile) {
    let table = catalog.load_table(ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(vec![data_file]);
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

// ── GC-specific helpers ────────────────────────────────────────────────────────

/// Build the `StorageConfig` that `run_sweep` uses to construct a raw object
/// store.
///
/// The caller must have `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` set in
/// the environment before invoking `run_sweep`; `MinIO` always uses
/// `minioadmin`/`minioadmin`, so run these tests as:
/// ```text
/// AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
///   cargo test -p icegate-maintain --test gc_sweep_it -- --ignored
/// ```
fn gc_storage_config(conn: &MinioConn) -> StorageConfig {
    StorageConfig {
        backend: StorageBackend::S3(S3Config {
            bucket: BUCKET_NAME.to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some(conn.endpoint.clone()),
        }),
    }
}

/// A `GcOrphansConfig` with a zero grace period (everything unreferenced is
/// eligible) unless overridden by the caller.
const fn orphans_config(min_age_secs: u64, dry_run: bool, include_metadata: bool) -> GcOrphansConfig {
    GcOrphansConfig {
        enabled: true,
        dry_run,
        min_age_secs,
        include_metadata,
        delete_concurrency: 8,
        sweep_timeout_secs: 600,
    }
}

/// List every object key in the bucket (sorted) via a direct S3 object store.
async fn list_all_object_keys(conn: &MinioConn) -> Vec<String> {
    let store: Arc<dyn ObjectStore> =
        create_s3_object_store(&conn.endpoint, BUCKET_NAME).expect("build test object store");
    let mut stream = store.list(None);
    let mut keys = Vec::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.expect("list object");
        keys.push(meta.location.as_ref().to_string());
    }
    keys.sort();
    keys
}

/// Count object keys that include `/<segment>/` anywhere in the path.
///
/// The S3 catalog stores tables under `catalog/tables/<uuid>/`, so files are
/// at `catalog/tables/<uuid>/data/` and `catalog/tables/<uuid>/metadata/` —
/// not under `<namespace>/<table>/`. Searching for `/<segment>/` is sufficient
/// because each integration test creates exactly one table.
fn count_under_segment(keys: &[String], segment: &str) -> usize {
    let needle = format!("/{segment}/");
    keys.iter().filter(|k| k.contains(&needle)).count()
}

/// Write a leaked data file: physically write a parquet file into the table's
/// `data/` prefix WITHOUT committing it to any snapshot, so it is referenced by
/// nothing. This is the deterministic Phase-2 orphan under a history-retaining
/// catalog (the S3 catalog used here keeps every snapshot, so a *compacted-away*
/// file would stay referenced — only a never-committed file is a true orphan).
async fn write_leaked_data_file(catalog: &S3Catalog, ident: &TableIdent, unique: usize) {
    let table = catalog.load_table(ident).await.unwrap();
    let _uncommitted = write_one_file(&table, logs_batch(&[("svc", DAY_MICROS)], unique)).await;
}

/// Put a dummy leaked object under the table's `metadata/` prefix via a direct
/// object store (no catalog involvement), so it is an orphan metadata file.
///
/// Looks up the table's actual location (e.g. `s3://warehouse/warehouse/catalog/tables/<uuid>`)
/// to write the leaked file into the correct S3 prefix that `run_sweep` will scan.
async fn put_leaked_metadata(conn: &MinioConn, catalog: &S3Catalog, ident: &TableIdent, name: &str) {
    let table = catalog.load_table(ident).await.unwrap();
    // This catalog always lays tables out at s3://<bucket>/...; assert it loudly.
    let location = table.metadata().location();
    let bucket_prefix = location
        .strip_prefix(&format!("s3://{BUCKET_NAME}/"))
        .expect("table location must be s3://<bucket>/... under the S3 catalog");
    let store: Arc<dyn ObjectStore> = create_s3_object_store(&conn.endpoint, BUCKET_NAME).expect("test store");
    let key = format!("{bucket_prefix}/metadata/{name}");
    store
        .put(
            &object_store::path::Path::from(key),
            object_store::PutPayload::from_static(b"leaked"),
        )
        .await
        .expect("put leaked metadata");
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires Docker (MinIO); run with --ignored"]
async fn gc_reclaims_unreferenced_files_and_keeps_live_ones() {
    let (_minio, conn) = setup_minio().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;

    // One committed (referenced) data file.
    let live = write_one_file(
        &catalog.load_table(&ident).await.unwrap(),
        logs_batch(&[("svc", DAY_MICROS)], 0),
    )
    .await;
    fast_append_one(&catalog, &ident, live.clone()).await;
    // One leaked (never-committed) data file — the orphan.
    write_leaked_data_file(&catalog, &ident, 1).await;

    let data_before = count_under_segment(&list_all_object_keys(&conn).await, "data");
    assert_eq!(data_before, 2, "expected 1 live + 1 leaked data file before sweep");

    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let storage = gc_storage_config(&conn);
    let summary = run_sweep(
        &dyn_catalog,
        &storage,
        TABLE,
        &orphans_config(0, false, true),
        Utc::now(),
        &GcMetrics::new(),
        &CancellationToken::new(),
    )
    .await
    .expect("sweep succeeds");

    // `>= 1` (not `== 1`): under `include_metadata=true` a superseded metadata
    // object may also be reclaimed. The precise invariant is on the data segment.
    assert!(summary.deleted >= 1, "the leaked file should be reclaimed: {summary:?}");
    assert!(
        summary.found_data >= 1,
        "the leaked data file is an orphan: {summary:?}"
    );
    let data_after = count_under_segment(&list_all_object_keys(&conn).await, "data");
    assert_eq!(data_after, 1, "the referenced data file must remain");

    // The referenced file is still readable through the table (would panic if gone).
    let live_table = catalog.load_table(&ident).await.unwrap();
    let descriptor = icegate_common::merge::sort_key::SortColumnsDescriptor::logs().expect("logs descriptor");
    let rows = read_all_rows(&live_table, descriptor).await;
    assert!(!rows.is_empty(), "live rows must be intact");
}

#[tokio::test]
#[ignore = "requires Docker (MinIO); run with --ignored"]
async fn gc_preserves_everything_inside_the_grace_period() {
    let (_minio, conn) = setup_minio().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;

    let live = write_one_file(
        &catalog.load_table(&ident).await.unwrap(),
        logs_batch(&[("svc", DAY_MICROS)], 0),
    )
    .await;
    fast_append_one(&catalog, &ident, live.clone()).await;
    write_leaked_data_file(&catalog, &ident, 1).await; // an orphan, but fresh

    let before = list_all_object_keys(&conn).await;
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let storage = gc_storage_config(&conn);
    // 1 hour grace: everything was written seconds ago, so nothing is eligible.
    let summary = run_sweep(
        &dyn_catalog,
        &storage,
        TABLE,
        &orphans_config(3_600, false, true),
        Utc::now(),
        &GcMetrics::new(),
        &CancellationToken::new(),
    )
    .await
    .expect("sweep succeeds");

    assert_eq!(
        summary.deleted, 0,
        "nothing should be deleted inside the grace period: {summary:?}"
    );
    let after = list_all_object_keys(&conn).await;
    assert_eq!(before.len(), after.len(), "object count must be unchanged");
}

#[tokio::test]
#[ignore = "requires Docker (MinIO); run with --ignored"]
async fn gc_dry_run_finds_orphans_but_deletes_nothing() {
    let (_minio, conn) = setup_minio().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;

    let live = write_one_file(
        &catalog.load_table(&ident).await.unwrap(),
        logs_batch(&[("svc", DAY_MICROS)], 0),
    )
    .await;
    fast_append_one(&catalog, &ident, live.clone()).await;
    write_leaked_data_file(&catalog, &ident, 1).await; // an orphan

    let before = list_all_object_keys(&conn).await;
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let storage = gc_storage_config(&conn);
    let summary = run_sweep(
        &dyn_catalog,
        &storage,
        TABLE,
        &orphans_config(0, true, true), // dry_run = true
        Utc::now(),
        &GcMetrics::new(),
        &CancellationToken::new(),
    )
    .await
    .expect("sweep succeeds");

    assert!(
        summary.found_data >= 1,
        "dry-run must still identify orphans: {summary:?}"
    );
    assert_eq!(summary.deleted, 0, "dry-run must delete nothing");
    let after = list_all_object_keys(&conn).await;
    assert_eq!(before.len(), after.len(), "dry-run must not change object count");
}

#[tokio::test]
#[ignore = "requires Docker (MinIO); run with --ignored"]
async fn gc_leaves_metadata_when_metadata_sweeping_is_disabled() {
    let (_minio, conn) = setup_minio().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;

    let live = write_one_file(
        &catalog.load_table(&ident).await.unwrap(),
        logs_batch(&[("svc", DAY_MICROS)], 0),
    )
    .await;
    fast_append_one(&catalog, &ident, live.clone()).await;
    write_leaked_data_file(&catalog, &ident, 1).await; // orphan data
    put_leaked_metadata(&conn, &catalog, &ident, "leaked-orphan.avro").await; // orphan metadata

    let metadata_before = count_under_segment(&list_all_object_keys(&conn).await, "metadata");
    let data_before = count_under_segment(&list_all_object_keys(&conn).await, "data");
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let storage = gc_storage_config(&conn);
    let summary = run_sweep(
        &dyn_catalog,
        &storage,
        TABLE,
        &orphans_config(0, false, false), // include_metadata = false
        Utc::now(),
        &GcMetrics::new(),
        &CancellationToken::new(),
    )
    .await
    .expect("sweep succeeds");

    assert_eq!(
        summary.found_metadata, 0,
        "metadata must not be considered an orphan when excluded"
    );
    assert!(summary.found_data >= 1, "data orphans are still swept");
    let metadata_after = count_under_segment(&list_all_object_keys(&conn).await, "metadata");
    let data_after = count_under_segment(&list_all_object_keys(&conn).await, "data");
    assert_eq!(metadata_before, metadata_after, "metadata objects must be untouched");
    assert!(
        data_after < data_before,
        "the leaked data orphan must still be reclaimed"
    );
}

#[tokio::test]
#[ignore = "requires Docker (MinIO); run with --ignored"]
async fn gc_fails_closed_and_deletes_nothing_when_a_manifest_is_unreadable() {
    let (_minio, conn) = setup_minio().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;
    let live = write_one_file(
        &catalog.load_table(&ident).await.unwrap(),
        logs_batch(&[("svc", DAY_MICROS)], 0),
    )
    .await;
    fast_append_one(&catalog, &ident, live.clone()).await;
    // A leaked orphan that WOULD be deleted on a successful sweep — proves the
    // fail-closed path deletes nothing, not merely that there were no orphans.
    write_leaked_data_file(&catalog, &ident, 1).await;

    // Corrupt the referenced set: delete one manifest object out-of-band so
    // `collect_referenced_paths` errors. The sweep must then delete nothing.
    let store: Arc<dyn ObjectStore> = create_s3_object_store(&conn.endpoint, BUCKET_NAME).expect("test store");
    // Find any Avro manifest under the table's metadata/ prefix (actual path is
    // `catalog/tables/<uuid>/metadata/*.avro`, not `icegate/logs/metadata/`).
    let manifest_key = list_all_object_keys(&conn)
        .await
        .into_iter()
        .find(|k| {
            k.contains("/metadata/")
                && std::path::Path::new(k)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("avro"))
        })
        .expect("a manifest/list .avro object exists");
    store
        .delete(&object_store::path::Path::from(manifest_key))
        .await
        .expect("delete a manifest out-of-band");

    let before = list_all_object_keys(&conn).await;
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let storage = gc_storage_config(&conn);
    let result = run_sweep(
        &dyn_catalog,
        &storage,
        TABLE,
        &orphans_config(0, false, true),
        Utc::now(),
        &GcMetrics::new(),
        &CancellationToken::new(),
    )
    .await;

    assert!(
        result.is_err(),
        "sweep must fail closed when the referenced set can't be built"
    );
    let after = list_all_object_keys(&conn).await;
    assert_eq!(before.len(), after.len(), "fail-closed: no objects may be deleted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker (MinIO); run with --ignored"]
async fn gc_runner_reclaims_in_the_background() {
    use icegate_maintain::compact::config::{CompactionJobsManagerConfig, JobStateCodec, JobsStorageConfig};
    use icegate_maintain::gc::GcRunner;
    use icegate_maintain::gc::config::GcConfig;

    let (_minio, conn) = setup_minio().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;

    let live = write_one_file(
        &catalog.load_table(&ident).await.unwrap(),
        logs_batch(&[("svc", DAY_MICROS)], 0),
    )
    .await;
    fast_append_one(&catalog, &ident, live.clone()).await;
    write_leaked_data_file(&catalog, &ident, 1).await; // the orphan the background sweep should reclaim

    let storage = gc_storage_config(&conn);
    let gc = GcConfig {
        enabled: true,
        spans_enabled: false,
        events_enabled: false,
        metrics_enabled: false,
        operations_enabled: false,
        orphans: GcOrphansConfig {
            min_age_secs: 0,
            ..GcOrphansConfig::default()
        },
        jobsmanager: CompactionJobsManagerConfig {
            worker_count: 1,
            poll_interval_ms: 100,
            scan_interval_secs: 1,
            storage: JobsStorageConfig {
                endpoint: conn.endpoint.clone(),
                bucket: BUCKET_NAME.to_string(),
                prefix: "gc".to_string(),
                region: "us-east-1".to_string(),
                use_ssl: false,
                job_state_codec: JobStateCodec::Json,
                request_timeout_secs: 5,
                access_key_id: Some(conn.access_key.clone()),
                secret_access_key: Some(conn.secret_key.clone()),
            },
        },
        ..GcConfig::default()
    };

    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let runner = GcRunner::new_with_max_iterations(dyn_catalog, &storage, &gc, Some(1))
        .await
        .expect("build runner");
    let data_before = count_under_segment(&list_all_object_keys(&conn).await, "data");
    let handle = runner.start().expect("start runner");

    // Poll until the background sweep reduces the on-disk data-file count.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let data_now = count_under_segment(&list_all_object_keys(&conn).await, "data");
        if data_now < data_before {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background gc did not reclaim within 60s (still {data_now})"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    handle.shutdown().await.expect("shutdown runner");
}
