//! Integration tests for the orphan-file garbage-collection sweep against the object store.
//!
//! Drives `run_sweep` directly (deterministic, fast) for five targeted tests,
//! and one end-to-end test that drives the full [`GcRunner`] background loop.
//! Each test starts its own object-storage container via testcontainers and threads the
//! container credentials through the storage config, so the suite is
//! self-contained and needs only a running Docker daemon:
//!
//! ```text
//! cargo test -p icegate-maintain --test gc_sweep_it
//! ```

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_lines)]

mod common;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use common::{
    BUCKET_NAME, DAY_MICROS, StorageConn, build_operator_registry, build_s3_catalog, list_all_object_keys, logs_batch,
    setup_object_store, write_one_file,
};
use futures::TryStreamExt;
use iceberg::arrow::ArrowFileReader;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use icegate_catalog_s3::S3Catalog;
use icegate_common::manifest_scan::list_data_files_with_stats;
use icegate_common::merge::sort_key::SortColumnsDescriptor;
use icegate_common::schema::{logs_partition_spec, logs_schema, logs_sort_order};
use icegate_common::testing::create_s3_object_store;
use icegate_maintain::gc::config::GcOrphansConfig;
use icegate_maintain::gc::metrics::GcMetrics;
use icegate_maintain::gc::sweep::run_sweep;
use object_store::{ObjectStore, ObjectStoreExt};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use tokio_util::sync::CancellationToken;

const NAMESPACE: &str = "icegate";
const TABLE: &str = "logs";

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
async fn put_leaked_metadata(conn: &StorageConn, catalog: &S3Catalog, ident: &TableIdent, name: &str) {
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
async fn gc_reclaims_unreferenced_files_and_keeps_live_ones() {
    let (_store, conn) = setup_object_store().await;
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
    let operator_registry = build_operator_registry(&conn).await;
    let summary = run_sweep(
        &dyn_catalog,
        &operator_registry,
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
async fn gc_preserves_everything_inside_the_grace_period() {
    let (_store, conn) = setup_object_store().await;
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
    let operator_registry = build_operator_registry(&conn).await;
    // 1 hour grace: everything was written seconds ago, so nothing is eligible.
    let summary = run_sweep(
        &dyn_catalog,
        &operator_registry,
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
async fn gc_dry_run_finds_orphans_but_deletes_nothing() {
    let (_store, conn) = setup_object_store().await;
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
    let operator_registry = build_operator_registry(&conn).await;
    let summary = run_sweep(
        &dyn_catalog,
        &operator_registry,
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
async fn gc_leaves_metadata_when_metadata_sweeping_is_disabled() {
    let (_store, conn) = setup_object_store().await;
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
    let operator_registry = build_operator_registry(&conn).await;
    let summary = run_sweep(
        &dyn_catalog,
        &operator_registry,
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
async fn gc_fails_closed_and_deletes_nothing_when_a_manifest_is_unreadable() {
    let (_store, conn) = setup_object_store().await;
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
    let operator_registry = build_operator_registry(&conn).await;
    let result = run_sweep(
        &dyn_catalog,
        &operator_registry,
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
async fn gc_runner_reclaims_in_the_background() {
    use icegate_maintain::gc::GcRunner;
    use icegate_maintain::gc::config::GcConfig;
    use icegate_maintain::jobs::{JobStateCodec, JobsManagerConfig, JobsStorageConfig};

    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;

    let live = write_one_file(
        &catalog.load_table(&ident).await.unwrap(),
        logs_batch(&[("svc", DAY_MICROS)], 0),
    )
    .await;
    fast_append_one(&catalog, &ident, live.clone()).await;
    write_leaked_data_file(&catalog, &ident, 1).await; // the orphan the background sweep should reclaim

    let gc = GcConfig {
        enabled: true,
        spans_enabled: false,
        events_enabled: false,
        metrics_enabled: false,
        operations_enabled: false,
        orphans: GcOrphansConfig {
            enabled: true,
            min_age_secs: 0,
            ..GcOrphansConfig::default()
        },
        jobsmanager: JobsManagerConfig {
            worker_count: 1,
            poll_interval_ms: 100,
            scan_interval_secs: 1,
            storage: JobsStorageConfig {
                endpoint: conn.endpoint.clone(),
                bucket: BUCKET_NAME.to_string(),
                prefix: "gc".to_string(),
                region: "us-east-1".to_string(),
                job_state_codec: JobStateCodec::Json,
                request_timeout_secs: 5,
                access_key_id: Some(conn.access_key.clone()),
                secret_access_key: Some(conn.secret_key.clone()),
            },
        },
        ..GcConfig::default()
    };

    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let spec = icegate_maintain::gc::GcRunnerSpec {
        operator_registry: build_operator_registry(&conn).await,
        config: gc,
    };
    let runner = GcRunner::new_with_max_iterations(dyn_catalog, spec, Some(1))
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

    // Count dropping by one proves *an* object went away, not that the orphan
    // (and not the committed file) was the one deleted. Reload and read back to
    // confirm the live data survived.
    let live_table = catalog.load_table(&ident).await.unwrap();
    let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");
    let rows = read_all_rows(&live_table, descriptor).await;
    assert!(!rows.is_empty(), "committed rows must remain after background gc");

    handle.shutdown().await.expect("shutdown runner");
}
