//! End-to-end test of the shift job as production assembles it: [`Shifter`] over a real WAL on
//! object storage, a real S3-backed Iceberg catalog, and S3-backed job state.
//!
//! What only this layer covers is the assembly itself - the configuration validation, the
//! `IcebergStorage` construction with the production encodings, the metrics decorators, and the
//! `plan -> shift -> commit` topology - plus the persistence the pool depends on: job state that
//! travels through object storage rather than through the in-memory backend the component tests in
//! [`super::pipeline_tests`] use. Removing a task registration from [`Shifter`] fails here, because
//! nothing in this file rebuilds that wiring.
//!
//! Requires Docker (a prerequisite):
//!
//! ```text
//! cargo test -p icegate-ingest shift::pipeline_it_tests
//! ```

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent, table::Table};
use icegate_common::{
    ICEGATE_NAMESPACE, LOGS_TABLE, LOGS_TOPIC,
    catalog::{CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle},
    list_data_files_with_stats,
    parquet_encoding::{LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS},
    resolve_committed_offset,
    schema::{logs_partition_spec, logs_schema, logs_sort_order},
    testing::{S3TestContainer, create_s3_bucket, create_s3_object_store},
};
use icegate_queue::{ParquetQueueReader, QueueConfig, QueueWriter, WriteRequest, channel};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    CURRENT_PLANNER_PARTITION_SPEC, ShiftConfig, ShiftJobSpec, Shifter,
    config::JobsStorageConfig,
    test_utils::{expected_row_bodies, log_sort_key_cmp, read_parquet_output_rows, two_tenant_ingest_batches},
};
use crate::{
    infra::metrics::ShiftMetrics,
    wal::{SortColumnsDescriptor, sort_logs},
};

/// Bucket the test provisions for the WAL, the catalog, the table data, and the job state.
const BUCKET_NAME: &str = "warehouse";

/// Highest WAL offset the two-segment fixture covers: the queue numbers segments from zero.
const FIXTURE_LAST_OFFSET: u64 = 1;

/// Bound the wait for the iteration to land, so a stuck pool fails the test instead of hanging it.
const COMMIT_TIMEOUT: Duration = Duration::from_secs(90);

/// Interval between snapshot polls while waiting for the commit task to publish its snapshot.
const POLL_INTERVAL: Duration = Duration::from_millis(200);

/// The shifted `logs` rows of one parquet file, in the physical order the file stores them.
struct CommittedFile {
    tenant_id: String,
    row_bodies: Vec<String>,
}

/// Object storage, plus the credentials every component in this test authenticates with.
struct ObjectStorage {
    _container: S3TestContainer,
    endpoint: String,
    access_key: String,
    secret_key: String,
}

/// Start object storage and create the bucket the run works in.
async fn start_object_storage() -> ObjectStorage {
    let container = S3TestContainer::start().await.expect("start object storage");
    create_s3_bucket(container.endpoint(), BUCKET_NAME)
        .await
        .expect("create bucket");
    ObjectStorage {
        endpoint: container.endpoint().to_string(),
        access_key: container.username().to_string(),
        secret_key: container.password().to_string(),
        _container: container,
    }
}

/// Write the fixture batches into the WAL through the production writer, one segment per batch,
/// and answer the queue reader the shifter consumes them with.
///
/// The writer runs with the production bloom-filter and encoding lists for `logs`, so the segments
/// the shift task reads back are encoded the way the ingest binary encodes them.
async fn write_wal_segments(storage: &ObjectStorage, base_path: &str) -> Arc<ParquetQueueReader> {
    let store = create_s3_object_store(&storage.endpoint, BUCKET_NAME).expect("wal object store");
    let queue_config = QueueConfig::new(base_path);
    let max_row_group_size = queue_config.common.max_row_group_size;
    let (write_tx, write_rx) = channel(queue_config.common.channel_capacity);
    let writer = QueueWriter::new(queue_config, Arc::clone(&store))
        .with_bloom_filter_columns(HashMap::from([(LOGS_TOPIC.to_string(), LOGS_BLOOM_COLUMNS)]))
        .with_column_encodings(HashMap::from([(LOGS_TOPIC.to_string(), LOGS_COLUMN_ENCODINGS)]));
    let writer_handle = writer.start(write_rx).await.expect("start WAL writer");

    for (segment_idx, batch) in two_tenant_ingest_batches().into_iter().enumerate() {
        // Two row groups per segment, so a shift task has something to merge across row groups
        // rather than copying a single sorted block through.
        let prepared = sort_logs(&batch, 2, None)
            .expect("prepare WAL segment")
            .expect("segment row groups");
        let (response_tx, response_rx) = oneshot::channel();
        write_tx
            .send(WriteRequest {
                topic: LOGS_TOPIC.to_string(),
                row_groups: prepared.write_request.row_groups,
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

    Arc::new(ParquetQueueReader::new(base_path.to_string(), store, max_row_group_size).expect("queue reader"))
}

/// Build the S3-backed catalog the ingest binary builds, and create the `logs` table in it.
async fn create_logs_table(storage: &ObjectStorage, catalog_prefix: &str) -> (Arc<dyn Catalog>, TableIdent) {
    let catalog_config = CatalogConfig {
        backend: CatalogBackend::S3 {
            warehouse: catalog_prefix.to_string(),
        },
        warehouse: BUCKET_NAME.to_string(),
        properties: HashMap::from([
            ("bucket".to_string(), BUCKET_NAME.to_string()),
            ("region".to_string(), "us-east-1".to_string()),
            ("endpoint".to_string(), storage.endpoint.clone()),
            ("access_key_id".to_string(), storage.access_key.clone()),
            ("secret_access_key".to_string(), storage.secret_key.clone()),
        ]),
        cache: None,
    };
    let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("S3 catalog");

    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let schema = logs_schema().expect("logs schema");
    let partition_spec = logs_partition_spec(&schema).expect("logs partition spec");
    let sort_order = logs_sort_order(&schema).expect("logs sort order");
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(LOGS_TABLE.to_string())
                .schema(schema)
                .partition_spec(partition_spec.into_unbound())
                .sort_order(sort_order)
                .build(),
        )
        .await
        .expect("create logs table");

    (catalog, TableIdent::new(namespace, LOGS_TABLE.to_string()))
}

/// The shift configuration the run uses: production defaults, with the job state pointed at this
/// run's own prefix in the bucket and a poll interval short enough to keep the test brief.
fn shift_config(storage: &ObjectStorage, jobs_prefix: &str) -> ShiftConfig {
    let mut config = ShiftConfig::default();
    config.jobsmanager.worker_count = 4;
    config.jobsmanager.poll_interval_ms = 50;
    config.jobsmanager.storage = JobsStorageConfig {
        endpoint: storage.endpoint.clone(),
        bucket: BUCKET_NAME.to_string(),
        prefix: jobs_prefix.to_string(),
        access_key_id: Some(storage.access_key.clone()),
        secret_access_key: Some(storage.secret_key.clone()),
        ..JobsStorageConfig::default()
    };
    config
}

/// Poll the table until a snapshot carries a committed WAL offset, and answer it.
///
/// The pool publishes the snapshot from a worker, so the test cannot observe the commit directly;
/// what it can observe is the offset the snapshot claims, which no earlier step writes.
async fn wait_for_committed_offset(catalog: &Arc<dyn Catalog>, ident: &TableIdent) -> u64 {
    let deadline = Instant::now() + COMMIT_TIMEOUT;
    loop {
        let table = catalog.load_table(ident).await.expect("load logs table");
        if let Some(offset) = resolve_committed_offset(table.metadata()).expect("read committed WAL offset") {
            return offset;
        }
        assert!(
            Instant::now() < deadline,
            "the shift iteration did not commit a snapshot within {COMMIT_TIMEOUT:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Read every parquet file the current snapshot references, decoded into its tenant and its rows
/// in physical order.
async fn read_committed_files(table: &Table) -> Vec<CommittedFile> {
    let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");
    let data_files = list_data_files_with_stats(table, descriptor)
        .await
        .expect("current snapshot data files");

    let mut committed = Vec::with_capacity(data_files.len());
    for data_file in data_files {
        let path = data_file.data_file.file_path().to_string();
        let rows = read_parquet_output_rows(table.file_io(), &path).await;
        let tenant_id = rows.first().expect("parquet rows").tenant_id.clone();
        assert!(
            rows.iter().all(|row| row.tenant_id == tenant_id),
            "one parquet file must hold one tenant partition, got {path}"
        );
        for window in rows.windows(2) {
            assert_ne!(
                log_sort_key_cmp(&window[0], &window[1]),
                std::cmp::Ordering::Greater,
                "parquet rows of {path} must be monotonic by the logs sort order"
            );
        }
        committed.push(CommittedFile {
            tenant_id,
            row_bodies: rows.into_iter().filter_map(|row| row.body).collect(),
        });
    }
    committed.sort_by(|left, right| left.tenant_id.cmp(&right.tenant_id));
    committed
}

/// One iteration of the production shifter over a two-segment WAL: every tenant of the fan-out
/// reaches Iceberg exactly once, in sort-key order, under a snapshot claiming the WAL offset the
/// plan saw.
///
/// Multi-threaded, with as many runtime threads as [`shift_config`] gives the pool workers: a shift
/// task merges row groups and encodes parquet synchronously, so on a single-threaded runtime it
/// would hold the thread and stall the other workers and the timers this test waits on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_production_shifter_commits_every_tenant_of_the_wal_in_sort_key_order() {
    let storage = start_object_storage().await;
    let run_id = Uuid::new_v4().simple().to_string();
    let queue_reader = write_wal_segments(&storage, &format!("wal-{run_id}")).await;
    let (catalog, ident) = create_logs_table(&storage, &format!("catalog-{run_id}")).await;

    let config = shift_config(&storage, &format!("jobs-{run_id}"));
    let jobs_storage = config.jobsmanager.storage.to_s3_config().expect("job state storage");
    let jobs = &[ShiftJobSpec {
        job_name: "shift_logs",
        topic: LOGS_TOPIC,
        table: LOGS_TABLE,
        descriptor: SortColumnsDescriptor::logs().expect("logs descriptor"),
        planner_partition_spec: &CURRENT_PLANNER_PARTITION_SPEC,
        bloom_filter_columns: LOGS_BLOOM_COLUMNS,
        column_encodings: LOGS_COLUMN_ENCODINGS,
    }];
    let shifter = Shifter::new_with_max_iterations(
        Arc::clone(&catalog),
        queue_reader,
        Arc::new(config),
        jobs_storage,
        ShiftMetrics::new_disabled(),
        Arc::new(jobmanager::NoopMetrics),
        jobs,
        Some(1),
    )
    .await
    .expect("build the production shifter");

    let handle = shifter.start().expect("start the shifter");
    let committed_offset = wait_for_committed_offset(&catalog, &ident).await;
    handle.shutdown().await.expect("shut the shifter down");

    assert_eq!(
        committed_offset, FIXTURE_LAST_OFFSET,
        "the snapshot must claim the highest WAL offset the plan task saw"
    );

    let table = catalog.load_table(&ident).await.expect("load committed logs table");
    let committed = read_committed_files(&table).await;
    assert_eq!(
        committed.iter().map(|file| file.tenant_id.as_str()).collect::<Vec<_>>(),
        vec!["tenant-a", "tenant-b"],
        "the fan-out must commit one file per tenant"
    );
    for file in &committed {
        assert_eq!(
            file.row_bodies,
            expected_row_bodies(&file.tenant_id),
            "tenant '{}' must be stored in sort-key order with WAL-stable tie-breakers",
            file.tenant_id
        );
    }
}
