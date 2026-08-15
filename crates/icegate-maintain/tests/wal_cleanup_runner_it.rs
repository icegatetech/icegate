//! The WAL-cleanup runner driven end to end: jobs registered on its own pool and
//! its own job-state prefix, one cycle executed, the handle drained.
//!
//! What `wal_cleanup_it` covers with the executor alone, this covers through the
//! jobmanager — the pool the runner owns is what the split from the orphan sweep
//! actually changed, and nothing below the runner can prove it was built.
//! Job state lives in object storage, so this test needs a running Docker daemon:
//!
//! ```text
//! cargo test -p icegate-maintain --test wal_cleanup_runner_it
//! ```

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use common::{BUCKET_NAME, StorageConn, setup_object_store};
use futures::StreamExt;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use icegate_common::{
    CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, LOGS_TABLE, LOGS_TOPIC, WAL_OFFSET_PROPERTY,
    catalog::CatalogBuilder, icegate_table_ident, schema,
};
use icegate_maintain::jobs::{JobStateCodec, JobsManagerConfig, JobsStorageConfig};
use icegate_maintain::wal_cleanup::config::WalCleanupConfig;
use icegate_maintain::wal_cleanup::runner::WalCleanupRunner;
use icegate_queue::QueueCleaner;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory};
use tokio_util::sync::CancellationToken;

/// Base path the WAL queue lives under in the test store.
const BASE_PATH: &str = "queue";
/// Segments seeded for the `logs` topic: offsets `0..SEGMENT_COUNT`.
const SEGMENT_COUNT: u64 = 16;
/// Offset the seeded Iceberg commit records as consumed.
const COMMITTED_OFFSET: u64 = 10;
/// Segments below the committed offset cleanup must leave alone.
const KEEP_SEGMENTS: u64 = 5;
/// Job-state prefix the cleanup pool must use, distinct from every other
/// service's.
const JOB_STATE_PREFIX: &str = "wal_cleanup";

/// Cleanup config for the runner: only the `logs` topic, one worker, a tight
/// discovery loop, and job state in the test bucket.
///
/// `enabled: false`, as in `wal_cleanup_it`: the switch is the caller's business
/// (the `run` command reads it to decide whether to build a runner at all), the
/// jobs themselves never read it, and a disabled block lets the test keep a
/// `keep_segments_count` below the floor an enabled one would have to clear.
fn cleanup_config(conn: &StorageConn) -> WalCleanupConfig {
    WalCleanupConfig {
        enabled: false,
        logs_enabled: true,
        spans_enabled: false,
        metrics_enabled: false,
        operations_enabled: false,
        keep_segments_count: KEEP_SEGMENTS,
        max_deletes_per_cycle: 100,
        delete_concurrency: 4,
        cleanup_timeout_secs: 60,
        jobsmanager: JobsManagerConfig {
            worker_count: 1,
            poll_interval_ms: 100,
            scan_interval_secs: 1,
            storage: JobsStorageConfig {
                endpoint: conn.endpoint.clone(),
                bucket: BUCKET_NAME.to_string(),
                prefix: JOB_STATE_PREFIX.to_string(),
                region: "us-east-1".to_string(),
                job_state_codec: JobStateCodec::Json,
                request_timeout_secs: 5,
                access_key_id: Some(conn.access_key.clone()),
                secret_access_key: Some(conn.secret_key.clone()),
            },
        },
        ..WalCleanupConfig::default()
    }
}

/// Build a memory catalog holding a `logs` table with one shift commit on it.
///
/// The temp warehouse is returned so the caller keeps it alive for the test.
async fn build_catalog_with_commit() -> (Arc<dyn Catalog>, tempfile::TempDir) {
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
        .name(LOGS_TABLE.to_string())
        .schema(schema::logs_schema().expect("logs schema"))
        .build();
    catalog.create_table(&namespace, creation).await.expect("create table");

    let table = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .set_snapshot_properties(HashMap::from([(
            WAL_OFFSET_PROPERTY.to_string(),
            COMMITTED_OFFSET.to_string(),
        )]))
        .apply(tx)
        .expect("apply fast append");
    tx.commit(catalog.as_ref()).await.expect("commit snapshot");

    (catalog, warehouse)
}

async fn seed_segments(store: &InMemory) {
    for offset in 0..SEGMENT_COUNT {
        let path = Path::from(format!("{BASE_PATH}/{LOGS_TOPIC}/{offset:0>20}.parquet"));
        store
            .put(&path, PutPayload::from_bytes(Bytes::from_static(b"segment")))
            .await
            .expect("seed segment");
    }
}

/// Number of segments still under the topic prefix.
async fn surviving_segment_count(store: &InMemory) -> usize {
    let prefix = Path::from(format!("{BASE_PATH}/{LOGS_TOPIC}/"));
    store.list(Some(&prefix)).collect::<Vec<_>>().await.len()
}

/// The runner registers its jobs, runs a cycle that reclaims the tail, writes
/// its job state under its own prefix, and drains cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_runner_cleans_on_its_own_pool_and_drains() {
    let (_store, conn) = setup_object_store().await;
    let (catalog, _warehouse) = build_catalog_with_commit().await;
    let queue_store = Arc::new(InMemory::new());
    seed_segments(&queue_store).await;
    let cleaner = Arc::new(QueueCleaner::new(BASE_PATH, Arc::clone(&queue_store) as _));

    let runner = WalCleanupRunner::new_with_max_iterations(catalog, cleaner, cleanup_config(&conn), Some(1))
        .await
        .expect("build cleanup runner");
    let handle = runner.start().expect("start cleanup runner");

    // Poll until the cycle has reclaimed the tail below the bound.
    let expected = usize::try_from(SEGMENT_COUNT - (COMMITTED_OFFSET - KEEP_SEGMENTS + 1)).expect("count fits usize");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if surviving_segment_count(&queue_store).await == expected {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the cleanup runner did not reclaim within 60s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    handle.shutdown().await.expect("drain cleanup runner");
}

/// A cleanup block switched on with every topic switched off is a
/// configuration nobody meant to write: the runner would raise a worker pool
/// that holds no job, and cleanup would report itself as running while the queue
/// grows. It must be refused at startup, naming the four keys to set.
#[tokio::test]
async fn the_runner_rejects_a_config_without_a_single_enabled_topic() {
    let (catalog, _warehouse) = build_catalog_with_commit().await;
    let cleaner = Arc::new(QueueCleaner::new(BASE_PATH, Arc::new(InMemory::new()) as _));
    let config = WalCleanupConfig {
        logs_enabled: false,
        spans_enabled: false,
        metrics_enabled: false,
        operations_enabled: false,
        jobsmanager: JobsManagerConfig {
            // A usable job-state storage, so the topic check is what the runner
            // reaches: an unset endpoint is rejected before it.
            storage: JobsStorageConfig {
                endpoint: "http://127.0.0.1:9000".to_string(),
                bucket: BUCKET_NAME.to_string(),
                prefix: JOB_STATE_PREFIX.to_string(),
                region: "us-east-1".to_string(),
                ..JobsStorageConfig::default()
            },
            ..JobsManagerConfig::default()
        },
        ..WalCleanupConfig::default()
    };

    let Err(error) = WalCleanupRunner::new(catalog, cleaner, config).await else {
        panic!("a cleanup runner with no topic to clean must not be built");
    };

    assert!(
        error
            .to_string()
            .contains("wal_cleanup.{logs,spans,metrics,operations}_enabled"),
        "the failure must name the keys to set, got: {error}"
    );
}

/// The job-state storage is validated where the pool is built, not at config
/// load: a `migrate` config carries no `wal_cleanup` block at all, so an empty endpoint
/// has to reach the loader intact and be rejected here instead.
#[tokio::test]
async fn the_runner_rejects_an_unset_job_state_storage() {
    let (catalog, _warehouse) = build_catalog_with_commit().await;
    let cleaner = Arc::new(QueueCleaner::new(BASE_PATH, Arc::new(InMemory::new()) as _));
    let config = WalCleanupConfig::default();
    config.validate().expect("the tunables themselves are valid");

    let Err(error) = WalCleanupRunner::new(catalog, cleaner, config).await else {
        panic!("an unset job-state storage must fail the runner");
    };

    assert!(
        error.to_string().contains("wal_cleanup.jobsmanager.storage.endpoint"),
        "the failure must name the block to fix, got: {error}"
    );
}
