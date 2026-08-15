//! End-to-end integration test for the compaction [`Compactor`] service against
//! the object store (§13).
//!
//! Seeds a `logs` table with SIX small, internally-sorted data files in ONE
//! `(tenant, day)` partition, each committed in its own `fast_append` so they
//! land in six successive snapshots. A [`Compactor`] configured for `logs` only,
//! capped at a single discovery iteration, is then started against a concrete
//! [`S3Catalog`]; the test polls until the partition's data-file count drops,
//! then shuts the compactor down and asserts:
//!
//! * the partition has STRICTLY FEWER data files than before;
//! * the total row count is unchanged;
//! * the full row set is identical before vs after (compared on a globally
//!   unique per-row body tag);
//! * the post-compaction physical row order is globally sorted by the logs sort
//!   key `(service_name ASC, timestamp DESC)`;
//! * the post-compaction files' `[min_key, max_key]` sort-key ranges are
//!   pairwise NON-overlapping.
//!
//! Requires Docker (a prerequisite):
//!
//! ```text
//! cargo test -p icegate-maintain --test compaction_e2e_it -- --nocapture
//! ```

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::too_many_lines)]

mod common;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, FixedSizeBinaryArray, MapArray, StringArray, StructArray, TimestampMicrosecondArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use common::{BUCKET_NAME, StorageConn, build_s3_catalog, setup_object_store};
use futures::TryStreamExt;
use iceberg::arrow::ArrowFileReader;
use iceberg::spec::{DataFile, DataFileFormat, Datum, FieldSummary, ManifestContentType, Operation, PrimitiveType};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use icegate_catalog_s3::S3Catalog;
use icegate_common::manifest_scan::{DataFileStats, list_data_files_with_stats, list_manifest_entries};
use icegate_common::merge::sort_key::SortColumnsDescriptor;
use icegate_common::schema::{logs_partition_spec, logs_schema, logs_sort_order};
use icegate_common::{WAL_OFFSET_PROPERTY, resolve_committed_offset};
use icegate_maintain::compact::config::{CompactionConfig, DataCompactionConfig, ManifestCompactionConfig};
use icegate_maintain::compact::manifest::rewrite::{
    MANIFEST_REWRITE_MARKER_KEY, ManifestCompactExecutor, ManifestCompactInput, ManifestCompactOutcome,
};
use icegate_maintain::compact::metrics::CompactMetrics;
use icegate_maintain::compact::{Compactor, CompactorHandle};
use icegate_maintain::jobs::{JobsManagerConfig, JobsStorageConfig};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::properties::WriterProperties;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const NAMESPACE: &str = "icegate";
const TABLE: &str = "logs";
const TENANT: &str = "tenant-a";
/// Second tenant, used where a test needs more than one partition value.
const OTHER_TENANT: &str = "tenant-b";
/// The `timestamp_day` partition value of every seeded file: the day ordinal
/// (days since the epoch) the Iceberg `day` transform stores. 20250 is
/// 2025-06-11.
const DAY_ORDINAL: i32 = 20_250;
/// Midnight of [`DAY_ORDINAL`] in microseconds, so every seeded file lands in
/// the same `(tenant_id, timestamp_day)` partition for its tenant.
const DAY_MICROS: i64 = 20_250 * 86_400_000_000;
/// Output Parquet target size (128 mebibytes): far larger than the tiny seeded
/// data, so the rewrite collapses the partition into a single output file.
const TARGET_FILE_SIZE_BYTES: u64 = 128 * 1024 * 1024;
/// Bound the poll-until-compacted wait so a stuck cycle fails fast rather than
/// hanging the suite.
const POLL_TIMEOUT: Duration = Duration::from_secs(60);
/// Interval between data-file-count polls while waiting for the cycle to land.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

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

/// Build one `logs` Arrow batch for `tenant` from `(service_name,
/// timestamp_micros)` rows in the caller's order. The writer does not re-sort,
/// so the caller must emit rows already in `(service_name ASC, timestamp DESC)`
/// order to produce an internally sorted file. The `body` column is
/// `msg-<unique>` so every row is globally distinguishable when comparing the
/// set before vs after.
fn logs_batch(tenant_id: &str, rows: &[(&str, i64)], unique_offset: usize) -> RecordBatch {
    let iceberg_schema = logs_schema().unwrap();
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).unwrap());
    let n = rows.len();

    let tenant = StringArray::from(vec![tenant_id; n]);
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
    let location_generator = DefaultLocationGenerator::new(&metadata).unwrap();
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

/// Count the live data files in the table's current snapshot.
async fn data_file_count(table: &Table, descriptor: &SortColumnsDescriptor) -> usize {
    list_data_files_with_stats(table, descriptor).await.expect("list files").len()
}

/// Build a compaction config that compacts ONLY `logs`, eagerly (so the small
/// seeded partition is never skipped as healthy), packs the whole partition into
/// a single rewrite group, and points its job-state storage at the object store.
fn compaction_config(conn: &StorageConn) -> CompactionConfig {
    let jobs_storage = JobsStorageConfig {
        endpoint: conn.endpoint.clone(),
        bucket: BUCKET_NAME.to_string(),
        prefix: "compactor".to_string(),
        region: "us-east-1".to_string(),
        job_state_codec: icegate_maintain::jobs::JobStateCodec::Json,
        request_timeout_secs: 30,
        access_key_id: Some(conn.access_key.clone()),
        secret_access_key: Some(conn.secret_key.clone()),
    };

    CompactionConfig {
        logs_enabled: true,
        spans_enabled: false,
        events_enabled: false,
        metrics_enabled: false,
        operations_enabled: false,
        data: DataCompactionConfig {
            target_file_size_bytes: TARGET_FILE_SIZE_BYTES,
            // Generous group budget so all six tiny files pack into one group.
            max_group_input_bytes: TARGET_FILE_SIZE_BYTES,
            // A 2-file partition is the healthy ceiling; six files is well past it,
            // so the partition is always selected for rewrite.
            min_input_files: 2,
            max_skippable_tail_files: 0,
            // Equal-size seed files, so the default 2x ratio keeps them in one group.
            max_merge_size_ratio: 2,
            // Generous rewrite deadline so the merge is never declared expired.
            rewrite_timeout_secs: 600,
            row_group_size: 20_000,
            data_page_size_limit_bytes: 2 * 1024 * 1024,
        },
        manifest: ManifestCompactionConfig {
            // Manifest compaction runs for every enabled table; these are just its
            // default tunables. The DATA-compaction test asserts only on data files,
            // which manifest repacking never touches, so it stays deterministic.
            target_size_bytes: 8 * 1024 * 1024,
            candidate_size_ratio: 0.75,
            max_manifests_per_commit: 64,
            rewrite_timeout_secs: 600,
        },
        jobsmanager: JobsManagerConfig {
            // Tight loop so the single iteration runs promptly.
            scan_interval_secs: 1,
            worker_count: 1,
            poll_interval_ms: 100,
            storage: jobs_storage,
        },
    }
}

/// Poll the partition's live data-file count until it drops below `baseline`,
/// returning the new count. Fails the test if the drop does not happen within
/// [`POLL_TIMEOUT`].
async fn wait_for_file_count_drop(
    catalog: &S3Catalog,
    ident: &TableIdent,
    descriptor: &SortColumnsDescriptor,
    baseline: usize,
) -> usize {
    let deadline = Instant::now() + POLL_TIMEOUT;
    loop {
        let table = catalog.load_table(ident).await.expect("reload table");
        let count = data_file_count(&table, descriptor).await;
        if count < baseline {
            return count;
        }
        assert!(
            Instant::now() < deadline,
            "compaction did not reduce the file count within {POLL_TIMEOUT:?} (still {count} files)"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compactor_compacts_partition_end_to_end() {
    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;
    let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");

    // Seed SIX small, internally-sorted files in one (tenant, day) partition.
    // Each file is sorted by (service_name ASC, timestamp DESC); the service
    // ranges overlap across files so the merge genuinely interleaves them.
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
        vec![
            ("svc-a", DAY_MICROS + 45),
            ("svc-c", DAY_MICROS + 33),
            ("svc-e", DAY_MICROS + 22),
        ],
        vec![
            ("svc-b", DAY_MICROS + 18),
            ("svc-d", DAY_MICROS + 55),
            ("svc-e", DAY_MICROS + 8),
        ],
    ];

    let mut unique_offset = 0usize;
    let mut seeded_rows_total: u64 = 0;
    for rows in &files_rows {
        // Each file is committed in its own fast_append => its own snapshot.
        let table = catalog.load_table(&ident).await.unwrap();
        let file = write_one_file(&table, logs_batch(TENANT, rows, unique_offset)).await;
        seeded_rows_total += file.record_count();
        unique_offset += rows.len();
        fast_append_one(&catalog, &ident, file).await;
    }

    // Capture the pre-compaction state: file count and full row set.
    let before = catalog.load_table(&ident).await.unwrap();
    let files_before = data_file_count(&before, descriptor).await;
    assert_eq!(files_before, files_rows.len(), "all seeded files must be enumerable");
    let mut rows_before = read_all_rows(&before, descriptor).await;
    assert_eq!(
        rows_before.len() as u64,
        seeded_rows_total,
        "all seeded rows present before"
    );

    // Run EXACTLY ONE compaction cycle: `max_iterations = Some(1)` makes each
    // job run its initial PLAN iteration (fan out + drain its REWRITE tasks) and
    // never start a second one. We still poll for the observable effect so the
    // assertions only run once the replace has actually landed.
    let config = compaction_config(&conn);
    // The compactor commits over `dyn Catalog`; coerce the concrete S3 catalog
    // used for seeding into the generic trait object.
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let compactor = Compactor::new_with_max_iterations(dyn_catalog, &config, Some(1))
        .await
        .expect("build compactor");
    let handle: CompactorHandle = compactor.start().expect("start compactor");

    let files_after = wait_for_file_count_drop(&catalog, &ident, descriptor, files_before).await;
    handle.shutdown().await.expect("shutdown compactor");

    // 1. The partition has STRICTLY FEWER data files than before.
    assert!(
        files_after < files_before,
        "compaction must reduce file count: before={files_before}, after={files_after}"
    );

    // 2. Total row count is unchanged.
    let after = catalog.load_table(&ident).await.unwrap();
    let mut rows_after = read_all_rows(&after, descriptor).await;
    assert_eq!(
        rows_after.len() as u64,
        seeded_rows_total,
        "row count must be preserved across the compaction"
    );

    // 3. The full row SET is identical before vs after (sort both by the unique
    //    message tag and compare element-by-element).
    rows_before.sort();
    rows_after.sort();
    assert_eq!(rows_before, rows_after, "row set must be identical before vs after");

    // 4. The post-compaction physical order is globally sorted by the logs sort
    //    key (service_name ASC, timestamp DESC).
    let physical_after = read_all_rows(&after, descriptor).await;
    let mut expected_sorted = physical_after.clone();
    expected_sorted.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| b.2.cmp(&a.2)));
    assert_eq!(
        physical_after, expected_sorted,
        "post-compaction physical order must be globally sorted by (service_name ASC, timestamp DESC)"
    );

    // 5. The post-compaction files' [min_key, max_key] sort-key ranges are
    //    pairwise NON-overlapping: sort the files by min_key and assert each
    //    file's max_key is strictly less than the next file's min_key.
    let mut stats_after: Vec<DataFileStats> =
        list_data_files_with_stats(&after, descriptor).await.expect("list stats after");
    stats_after.sort_by(|left, right| left.min_key().compare(right.min_key()));
    for window in stats_after.windows(2) {
        let [lower, upper] = window else { unreachable!() };
        assert_eq!(
            lower.max_key().compare(upper.min_key()),
            std::cmp::Ordering::Less,
            "post-compaction files must have pairwise non-overlapping sort-key ranges: \
             file max_key {:?} is not strictly less than next file min_key {:?}",
            lower.max_key(),
            upper.min_key(),
        );
    }
}

/// Count the DATA manifests in the table's current snapshot's manifest list.
async fn data_manifest_count(table: &Table) -> usize {
    list_manifest_entries(table)
        .await
        .expect("list manifest entries")
        .into_iter()
        .filter(|entry| entry.content == ManifestContentType::Data)
        .count()
}

/// What a `(tenant_id, timestamp_day)` predicate still has to open: the number
/// of DATA manifests manifest pruning cannot discard for that partition value,
/// and their total `manifest_length`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PruningFootprint {
    manifests: usize,
    bytes: i64,
}

/// Measure the pruning footprint of one `(tenant_id, timestamp_day)` value.
///
/// Reads the same source manifest pruning uses — the manifest-list entries'
/// partition summaries (`ManifestFile.partitions`, what
/// `iceberg::expr::visitors::manifest_evaluator` evaluates) — and keeps every
/// DATA manifest whose summaries admit the value. A manifest with no summaries
/// (or no bound for a field) can never be pruned away, so it always counts:
/// the result is the upper bound on what a scan for that partition must open.
async fn measure_pruning_footprint(table: &Table, tenant_id: &str, timestamp_day: i32) -> PruningFootprint {
    let snapshot = table.metadata().current_snapshot().expect("current snapshot");
    let manifest_list = table.manifest_list_reader(snapshot).load().await.expect("load manifest list");

    let tenant_value = Datum::string(tenant_id);
    let day_value = Datum::int(timestamp_day);

    manifest_list
        .entries()
        .iter()
        .filter(|manifest| manifest.content == ManifestContentType::Data)
        .filter(|manifest| {
            // The logs spec is (tenant_id identity, timestamp_day day), so the
            // summaries are positional: index 0 is the tenant, index 1 the day.
            manifest.partitions.as_ref().is_none_or(|summaries| {
                summary_admits(summaries.first(), &tenant_value, &PrimitiveType::String)
                    && summary_admits(summaries.get(1), &day_value, &PrimitiveType::Int)
            })
        })
        .fold(PruningFootprint { manifests: 0, bytes: 0 }, |footprint, manifest| {
            PruningFootprint {
                manifests: footprint.manifests + 1,
                bytes: footprint.bytes + manifest.manifest_length,
            }
        })
}

/// Whether one partition-field summary admits `value`, i.e. cannot prove the
/// manifest holds no row with it. A missing summary or bound admits everything,
/// and a bound that fails to decode is treated as admitting rather than silently
/// pruning the manifest out of the measurement.
fn summary_admits(summary: Option<&FieldSummary>, value: &Datum, primitive: &PrimitiveType) -> bool {
    let Some(summary) = summary else { return true };

    let below_lower = summary
        .lower_bound
        .as_ref()
        .and_then(|bytes| Datum::try_from_bytes(bytes, primitive.clone()).ok())
        .is_some_and(|lower| lower > *value);
    let above_upper = summary
        .upper_bound
        .as_ref()
        .and_then(|bytes| Datum::try_from_bytes(bytes, primitive.clone()).ok())
        .is_some_and(|upper| upper < *value);

    !below_lower && !above_upper
}

/// Poll the current snapshot's DATA-manifest count until it drops below
/// `baseline`, returning the new count. Fails the test if the drop does not
/// happen within [`POLL_TIMEOUT`].
async fn wait_for_manifest_count_drop(catalog: &S3Catalog, ident: &TableIdent, baseline: usize) -> usize {
    let deadline = Instant::now() + POLL_TIMEOUT;
    loop {
        let table = catalog.load_table(ident).await.expect("reload table");
        let count = data_manifest_count(&table).await;
        if count < baseline {
            return count;
        }
        assert!(
            Instant::now() < deadline,
            "manifest compaction did not reduce the manifest count within {POLL_TIMEOUT:?} (still {count})"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Commit a single data file in its own `fast_append`, stamping `props` on the
/// snapshot summary (used to seed a base WAL offset before manifest compaction).
async fn fast_append_one_with_props(
    catalog: &S3Catalog,
    ident: &TableIdent,
    data_file: DataFile,
    props: HashMap<String, String>,
) {
    let table = catalog.load_table(ident).await.unwrap();
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(vec![data_file]).set_snapshot_properties(props);
    let tx = action.apply(tx).unwrap();
    tx.commit(catalog).await.unwrap();
}

/// A config that DISABLES data-file compaction (both healthiness thresholds far
/// above the seeded file count, so the partition is always skipped and the PLAN
/// fans out zero REWRITE tasks). Manifest compaction runs for every enabled
/// table, so this isolates manifest repacking and exercises the `N == 0`
/// independent trigger.
fn manifest_compaction_config(conn: &StorageConn) -> CompactionConfig {
    let mut config = compaction_config(conn);
    config.data.min_input_files = 1_000;
    config.data.max_skippable_tail_files = 1_000;
    config
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compactor_repacks_manifests_without_touching_data() {
    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;
    let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");

    // Seed SIX small files, each in its own fast_append => six snapshots, each
    // adding one DATA manifest and carrying the earlier ones forward unmerged.
    // Three files per tenant, so the seeded partition values are
    // (tenant-a, day) and (tenant-b, day) and the pruning check below is not
    // degenerate: a repack that widened every output manifest's tenant range
    // would show up as a larger per-value footprint.
    let files_rows: Vec<(&str, Vec<(&str, i64)>)> = vec![
        (TENANT, vec![("svc-a", DAY_MICROS + 30)]),
        (TENANT, vec![("svc-b", DAY_MICROS + 40)]),
        (TENANT, vec![("svc-c", DAY_MICROS + 50)]),
        (OTHER_TENANT, vec![("svc-d", DAY_MICROS + 60)]),
        (OTHER_TENANT, vec![("svc-e", DAY_MICROS + 70)]),
        (OTHER_TENANT, vec![("svc-f", DAY_MICROS + 80)]),
    ];
    let mut unique_offset = 0usize;
    let mut seeded_rows_total: u64 = 0;
    for (tenant_id, rows) in &files_rows {
        let table = catalog.load_table(&ident).await.unwrap();
        let file = write_one_file(&table, logs_batch(tenant_id, rows, unique_offset)).await;
        seeded_rows_total += file.record_count();
        unique_offset += rows.len();
        fast_append_one(&catalog, &ident, file).await;
    }

    let before = catalog.load_table(&ident).await.unwrap();
    let manifests_before = data_manifest_count(&before).await;
    let files_before = data_file_count(&before, descriptor).await;
    assert_eq!(
        manifests_before,
        files_rows.len(),
        "each fast_append adds one data manifest"
    );
    assert_eq!(files_before, files_rows.len(), "all seeded files must be enumerable");
    let mut rows_before = read_all_rows(&before, descriptor).await;

    // Pruning baseline per seeded partition value, re-checked after the repack.
    let footprints_before: Vec<PruningFootprint> = futures::future::join_all(
        [TENANT, OTHER_TENANT]
            .iter()
            .map(|tenant_id| measure_pruning_footprint(&before, tenant_id, DAY_ORDINAL)),
    )
    .await;

    // One cycle. Data compaction skips the (healthy) partition, so the PLAN fans
    // out zero REWRITE tasks and the compact_manifest task runs immediately (the
    // independent `N == 0` trigger), repacking the five manifests.
    let config = manifest_compaction_config(&conn);
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let compactor = Compactor::new_with_max_iterations(dyn_catalog.clone(), &config, Some(1))
        .await
        .expect("build compactor");
    let handle: CompactorHandle = compactor.start().expect("start compactor");
    let manifests_after = wait_for_manifest_count_drop(&catalog, &ident, manifests_before).await;
    handle.shutdown().await.expect("shutdown compactor");

    // 1. The current snapshot has STRICTLY FEWER data manifests than before.
    assert!(
        manifests_after < manifests_before,
        "manifest count must drop: before={manifests_before}, after={manifests_after}"
    );

    let after = catalog.load_table(&ident).await.unwrap();

    // 2. Data files are untouched: the data-file count is unchanged and the full
    //    row set is identical before vs after.
    assert_eq!(
        data_file_count(&after, descriptor).await,
        files_before,
        "manifest compaction must not change the data-file count"
    );
    let mut rows_after = read_all_rows(&after, descriptor).await;
    assert_eq!(rows_after.len() as u64, seeded_rows_total, "row count preserved");
    rows_before.sort();
    rows_after.sort();
    assert_eq!(
        rows_before, rows_after,
        "row set must be identical (data files untouched)"
    );

    // 3. The new snapshot is a Replace carrying the manifest-rewrite marker.
    let snapshot = after.metadata().current_snapshot().expect("current snapshot");
    assert_eq!(snapshot.summary().operation, Operation::Replace);
    assert_eq!(
        snapshot
            .summary()
            .additional_properties
            .get(MANIFEST_REWRITE_MARKER_KEY)
            .map(String::as_str),
        Some("true"),
        "the replace snapshot must carry the manifest-rewrite marker"
    );

    // 3b. Pruning metadata survives the rewrite: every repacked output DATA
    //     manifest carries partition summaries, one per partition-spec field
    //     (tenant_id, timestamp_day), so manifest pruning stays effective.
    let manifest_list = after.manifest_list_reader(snapshot).load().await.expect("load manifest list");
    let output_data_manifests: Vec<_> = manifest_list
        .entries()
        .iter()
        .filter(|manifest| manifest.content == ManifestContentType::Data)
        .collect();
    assert_eq!(
        output_data_manifests.len(),
        manifests_after,
        "data-manifest count is consistent"
    );
    for manifest in &output_data_manifests {
        let summaries = manifest
            .partitions
            .as_ref()
            .expect("output manifest must carry partition summaries");
        assert_eq!(
            summaries.len(),
            2,
            "the logs partition spec has two fields (tenant_id, timestamp_day)"
        );
    }

    // 3c. Pruning stays at least as effective: for every seeded partition value,
    //     neither the number of DATA manifests a scan must open nor their total
    //     bytes grew. Repacking trades object count for wider summaries, so this
    //     is the check that the trade did not backfire on the query side.
    for (tenant_id, before_footprint) in [TENANT, OTHER_TENANT].iter().zip(footprints_before) {
        let after_footprint = measure_pruning_footprint(&after, tenant_id, DAY_ORDINAL).await;
        assert!(
            after_footprint.manifests <= before_footprint.manifests,
            "manifests surviving pruning for ({tenant_id}, {DAY_ORDINAL}) grew: \
             before={before_footprint:?}, after={after_footprint:?}"
        );
        assert!(
            after_footprint.bytes <= before_footprint.bytes,
            "manifest bytes surviving pruning for ({tenant_id}, {DAY_ORDINAL}) grew: \
             before={before_footprint:?}, after={after_footprint:?}"
        );
    }

    // 4. Idempotent no-op: a second run against the now-compact snapshot finds
    //    fewer than two candidates, opens no transaction, and leaves the current
    //    snapshot id unchanged.
    let snapshot_id_after = after.metadata().current_snapshot_id().expect("snapshot id");
    let executor = ManifestCompactExecutor::new(
        dyn_catalog.clone(),
        config.manifest.target_size_bytes,
        config.manifest.candidate_size_ratio,
        config.manifest.max_manifests_per_commit,
        CompactMetrics::new(),
    );
    let outcome = executor
        .execute(
            &ManifestCompactInput {
                table: TABLE.to_string(),
                trace_context: None,
            },
            &CancellationToken::new(),
        )
        .await
        .expect("idempotent run");
    assert_eq!(
        outcome,
        ManifestCompactOutcome::Skipped,
        "a second run on a compact snapshot must be a no-op"
    );
    let reloaded = catalog.load_table(&ident).await.unwrap();
    assert_eq!(
        reloaded.metadata().current_snapshot_id(),
        Some(snapshot_id_after),
        "the no-op run must not create a new snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manifest_compaction_carries_wal_offset_forward() {
    const WAL_OFFSET: u64 = 4242;

    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;

    // Seed four files; the LAST fast_append stamps the WAL offset on its snapshot,
    // so the base the manifest rewrite supersedes carries an offset to inherit.
    let files_rows: Vec<Vec<(&str, i64)>> = vec![
        vec![("svc-a", DAY_MICROS + 30)],
        vec![("svc-b", DAY_MICROS + 40)],
        vec![("svc-c", DAY_MICROS + 50)],
        vec![("svc-d", DAY_MICROS + 60)],
    ];
    let mut unique_offset = 0usize;
    for (index, rows) in files_rows.iter().enumerate() {
        let table = catalog.load_table(&ident).await.unwrap();
        let file = write_one_file(&table, logs_batch(TENANT, rows, unique_offset)).await;
        unique_offset += rows.len();
        let props = if index + 1 == files_rows.len() {
            HashMap::from([(WAL_OFFSET_PROPERTY.to_string(), WAL_OFFSET.to_string())])
        } else {
            HashMap::new()
        };
        fast_append_one_with_props(&catalog, &ident, file, props).await;
    }

    let before = catalog.load_table(&ident).await.unwrap();
    assert_eq!(
        resolve_committed_offset(before.metadata()).expect("resolve before"),
        Some(WAL_OFFSET),
        "the seeded base snapshot must carry the WAL offset"
    );
    let manifests_before = data_manifest_count(&before).await;

    let config = manifest_compaction_config(&conn);
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let compactor = Compactor::new_with_max_iterations(dyn_catalog, &config, Some(1))
        .await
        .expect("build compactor");
    let handle = compactor.start().expect("start compactor");
    wait_for_manifest_count_drop(&catalog, &ident, manifests_before).await;
    handle.shutdown().await.expect("shutdown compactor");

    // The replace snapshot inherited the WAL offset, so resolution still finds it
    // after manifest compaction.
    let after = catalog.load_table(&ident).await.unwrap();
    assert_eq!(
        resolve_committed_offset(after.metadata()).expect("resolve after"),
        Some(WAL_OFFSET),
        "manifest compaction must carry the WAL offset forward onto the replace snapshot"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manifest_compaction_reports_actual_output_count() {
    // The committed outcome's `output_manifests` must be the ACTUAL number the
    // repack produced (re-read from the manifest list), not the pre-commit
    // estimate. Drive the executor directly so the `Committed` outcome is
    // observable, then cross-check it against the table's real post-commit
    // DATA-manifest count.
    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let ident = create_logs_table(&catalog).await;

    // Four tiny single-partition files, one manifest each, all well below the
    // candidate threshold so all four are selected and pack into one manifest.
    let files_rows: Vec<Vec<(&str, i64)>> = vec![
        vec![("svc-a", DAY_MICROS + 30)],
        vec![("svc-b", DAY_MICROS + 40)],
        vec![("svc-c", DAY_MICROS + 50)],
        vec![("svc-d", DAY_MICROS + 60)],
    ];
    let mut unique_offset = 0usize;
    for rows in &files_rows {
        let table = catalog.load_table(&ident).await.unwrap();
        let file = write_one_file(&table, logs_batch(TENANT, rows, unique_offset)).await;
        unique_offset += rows.len();
        fast_append_one(&catalog, &ident, file).await;
    }

    let before = catalog.load_table(&ident).await.unwrap();
    let manifests_before = data_manifest_count(&before).await;
    assert_eq!(
        manifests_before,
        files_rows.len(),
        "each fast_append adds one data manifest"
    );

    let config = manifest_compaction_config(&conn);
    let dyn_catalog: Arc<dyn Catalog> = catalog.clone();
    let executor = ManifestCompactExecutor::new(
        dyn_catalog,
        config.manifest.target_size_bytes,
        config.manifest.candidate_size_ratio,
        config.manifest.max_manifests_per_commit,
        CompactMetrics::new(),
    );
    let outcome = executor
        .execute(
            &ManifestCompactInput {
                table: TABLE.to_string(),
                trace_context: None,
            },
            &CancellationToken::new(),
        )
        .await
        .expect("repack run");

    let after = catalog.load_table(&ident).await.unwrap();
    let manifests_after = data_manifest_count(&after).await;

    match outcome {
        ManifestCompactOutcome::Committed {
            input_manifests,
            output_manifests,
        } => {
            assert_eq!(input_manifests, manifests_before, "all seeded manifests were repacked");
            // The reported output count must equal the manifests the snapshot now
            // actually carries (all repacked into one group, no DELETE manifests).
            assert_eq!(
                output_manifests, manifests_after,
                "reported output count must match the actual post-commit manifest count"
            );
        }
        ManifestCompactOutcome::Skipped => panic!("expected a committed repack, got a skip"),
    }
}
