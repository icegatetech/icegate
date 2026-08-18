//! One WAL-cleanup cycle, driven end to end over an in-memory catalog and an
//! in-memory object store.
//!
//! The bound is derived from real Iceberg snapshot summaries — the same
//! `icegate.queue.offset` the Shifter stamps — and the deletes go through the
//! queue crate's own cleaner, so the segment key format has one owner here too.
//!
//! The bound itself — `committed - keep_segments_count` and the cases where it
//! resolves to nothing — is covered by the unit tests of
//! `wal_cleanup::resolve_delete_bound`, which assemble metadata directly. What is here is
//! everything below that: a real commit, a real cleaner, real deletes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use icegate_common::{
    CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, LOGS_TABLE, LOGS_TOPIC, WAL_OFFSET_PROPERTY,
    catalog::CatalogBuilder, icegate_table_ident, schema,
};
use icegate_maintain::wal_cleanup::WalCleanupExecutor;
use icegate_maintain::wal_cleanup::config::WalCleanupConfig;
use icegate_maintain::wal_cleanup::metrics::WalCleanupMetrics;
use icegate_queue::QueueCleaner;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, memory::InMemory};
use tokio_util::sync::CancellationToken;

/// Base path the WAL queue lives under in the test store.
const BASE_PATH: &str = "queue";

/// Segments seeded per topic: offsets `0..SEGMENT_COUNT`.
const SEGMENT_COUNT: u64 = 16;

/// Offset the seeded Iceberg commit records as consumed.
const COMMITTED_OFFSET: u64 = 10;

/// Segments below the committed offset cleanup must leave alone.
const KEEP_SEGMENTS: u64 = 5;

/// Build a memory catalog holding an empty `logs` table.
///
/// The temp warehouse is returned so the caller keeps it alive for the test.
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
    if !catalog.namespace_exists(&namespace).await.expect("namespace exists") {
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");
    }
    let creation = TableCreation::builder()
        .name(LOGS_TABLE.to_string())
        .schema(schema::logs_schema().expect("logs schema"))
        .build();
    catalog.create_table(&namespace, creation).await.expect("create table");

    (catalog, warehouse)
}

/// Commit a snapshot carrying `offset` in its summary, the way the Shifter does.
///
/// Data files are beside the point: the delete bound is read from the summary,
/// and an append with no files still produces a well-formed snapshot.
async fn commit_offset(catalog: &Arc<dyn Catalog>, offset: Option<u64>) {
    let table = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");
    let mut properties = HashMap::from([("icegate.test.commit".to_string(), "1".to_string())]);
    if let Some(offset) = offset {
        properties.insert(WAL_OFFSET_PROPERTY.to_string(), offset.to_string());
    }
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .set_snapshot_properties(properties)
        .apply(tx)
        .expect("apply fast append");
    tx.commit(catalog.as_ref()).await.expect("commit snapshot");
}

async fn seed_segments(store: &InMemory, offsets: impl IntoIterator<Item = u64>) {
    for offset in offsets {
        let path = Path::from(format!("{BASE_PATH}/{LOGS_TOPIC}/{offset:0>20}.parquet"));
        store
            .put(&path, PutPayload::from_bytes(Bytes::from_static(b"segment")))
            .await
            .expect("seed segment");
    }
}

/// Offsets still present under the topic prefix, ascending.
async fn surviving_offsets(store: &InMemory) -> Vec<u64> {
    let prefix = Path::from(format!("{BASE_PATH}/{LOGS_TOPIC}/"));
    let mut offsets: Vec<u64> = store
        .list(Some(&prefix))
        .map(|meta| {
            let location = meta.expect("list segment").location;
            let name = location.as_ref().rsplit('/').next().expect("file name").to_string();
            name.trim_end_matches(".parquet").parse::<u64>().expect("segment offset")
        })
        .collect::<Vec<_>>()
        .await;
    offsets.sort_unstable();
    offsets
}

/// Cleanup config for the tests.
///
/// `enabled: false` deliberately: the executor never reads it (the runner does,
/// when it decides to register jobs at all), and a disabled block lets these
/// tests keep a `keep_segments_count` far below the floor an enabled one would
/// have to clear — five segments make the arithmetic readable.
fn cleanup_config() -> WalCleanupConfig {
    let config = WalCleanupConfig {
        enabled: false,
        keep_segments_count: KEEP_SEGMENTS,
        max_deletes_per_cycle: 100,
        delete_concurrency: 4,
        ..WalCleanupConfig::default()
    };
    config.validate().expect("the test config must be one the binary loads");
    config
}

fn build_executor(
    catalog: &Arc<dyn Catalog>,
    store: Arc<dyn ObjectStore>,
    config: WalCleanupConfig,
) -> WalCleanupExecutor {
    build_executor_for_table(catalog, store, config, LOGS_TABLE)
}

/// The same executor over a caller-chosen table, so a cycle can be pointed at
/// one the catalog does not hold.
fn build_executor_for_table(
    catalog: &Arc<dyn Catalog>,
    store: Arc<dyn ObjectStore>,
    config: WalCleanupConfig,
    table: &'static str,
) -> WalCleanupExecutor {
    let cleaner = Arc::new(QueueCleaner::new(BASE_PATH, store));
    WalCleanupExecutor::new(
        Arc::clone(catalog),
        cleaner,
        LOGS_TOPIC,
        table,
        config,
        WalCleanupMetrics::new(),
    )
}

/// The bound is `committed - keep_segments_count`, inclusive: with 10 committed
/// and 5 kept, segments `0..=5` go and `6..` stay.
/// A second cycle over the same state must be a no-op — cleanup runs on a
/// schedule, so idempotency is the normal case, not an edge one.
#[tokio::test]
async fn one_cycle_deletes_up_to_the_bound_and_repeats_clean() {
    let (catalog, _warehouse) = build_catalog().await;
    commit_offset(&catalog, Some(COMMITTED_OFFSET)).await;
    let store = Arc::new(InMemory::new());
    seed_segments(&store, 0..SEGMENT_COUNT).await;
    let executor = build_executor(&catalog, Arc::clone(&store) as _, cleanup_config());
    let cancel = CancellationToken::new();

    executor.clean_topic(&cancel).await.expect("first cleanup cycle");

    let expected: Vec<u64> = (COMMITTED_OFFSET - KEEP_SEGMENTS + 1..SEGMENT_COUNT).collect();
    assert_eq!(surviving_offsets(&store).await, expected);

    executor.clean_topic(&cancel).await.expect("second cleanup cycle");
    assert_eq!(
        surviving_offsets(&store).await,
        expected,
        "a repeated cycle over unchanged state must delete nothing further"
    );
}

/// A dry run reports what it would take and takes nothing.
#[tokio::test]
async fn a_dry_run_cycle_deletes_nothing() {
    let (catalog, _warehouse) = build_catalog().await;
    commit_offset(&catalog, Some(COMMITTED_OFFSET)).await;
    let store = Arc::new(InMemory::new());
    seed_segments(&store, 0..SEGMENT_COUNT).await;
    let config = WalCleanupConfig {
        dry_run: true,
        ..cleanup_config()
    };
    let executor = build_executor(&catalog, Arc::clone(&store) as _, config);

    executor.clean_topic(&CancellationToken::new()).await.expect("dry-run cycle");

    assert_eq!(surviving_offsets(&store).await, (0..SEGMENT_COUNT).collect::<Vec<_>>());
}

/// A table whose snapshots record no offset anywhere cannot be resolved, and a
/// destructive operation must fail CLOSED on that: the cycle errors and the
/// queue is untouched.
#[tokio::test]
async fn an_unresolvable_offset_deletes_nothing() {
    let (catalog, _warehouse) = build_catalog().await;
    commit_offset(&catalog, None).await;
    let store = Arc::new(InMemory::new());
    seed_segments(&store, 0..SEGMENT_COUNT).await;
    let executor = build_executor(&catalog, Arc::clone(&store) as _, cleanup_config());

    let error = executor
        .clean_topic(&CancellationToken::new())
        .await
        .expect_err("an unresolvable committed offset must fail the cycle");

    assert!(
        error.to_string().contains("committed offset"),
        "the failure must name the unresolved offset, got: {error}"
    );
    assert_eq!(surviving_offsets(&store).await, (0..SEGMENT_COUNT).collect::<Vec<_>>());
}

/// Fewer segments committed than `keep_segments_count` protects: the cycle
/// resolves no bound, succeeds, and touches nothing. This is the freshly created
/// queue, and the shape every cycle takes until the Shifter has moved enough.
#[tokio::test]
async fn a_committed_offset_below_the_keep_count_deletes_nothing() {
    let (catalog, _warehouse) = build_catalog().await;
    commit_offset(&catalog, Some(KEEP_SEGMENTS - 1)).await;
    let store = Arc::new(InMemory::new());
    seed_segments(&store, 0..SEGMENT_COUNT).await;
    let executor = build_executor(&catalog, Arc::clone(&store) as _, cleanup_config());

    executor
        .clean_topic(&CancellationToken::new())
        .await
        .expect("a cycle without a bound is not an error");

    assert_eq!(surviving_offsets(&store).await, (0..SEGMENT_COUNT).collect::<Vec<_>>());
}

/// A store that refuses every deletion — expired credentials, a bucket policy —
/// is not a failed cycle: the bound was established and the segments simply did
/// not go. The cycle succeeds and leaves the queue exactly as it found it, which
/// is what this case pins down. That such a cycle is also REPORTED is not
/// observable here — `clean_topic` returns the same empty outcome either way,
/// and the signal is a log line plus `wal_cleanup.cycles.stalled`; the rule
/// deciding it is covered by `is_cycle_stalled`'s own test.
#[tokio::test]
async fn a_store_refusing_every_delete_leaves_the_queue_intact() {
    let (catalog, _warehouse) = build_catalog().await;
    commit_offset(&catalog, Some(COMMITTED_OFFSET)).await;
    let inner = Arc::new(InMemory::new());
    seed_segments(&inner, 0..SEGMENT_COUNT).await;
    let store = Arc::new(RefusingDeleteStore {
        inner: Arc::clone(&inner),
    });
    let executor = build_executor(&catalog, store as Arc<dyn ObjectStore>, cleanup_config());

    executor
        .clean_topic(&CancellationToken::new())
        .await
        .expect("a store refusing the deletes is not a failed cycle");

    assert_eq!(surviving_offsets(&inner).await, (0..SEGMENT_COUNT).collect::<Vec<_>>());
}

/// Store double whose every deletion fails and whose other operations pass
/// through, so a cycle over it reaches a bound and reclaims nothing.
#[derive(Debug)]
struct RefusingDeleteStore {
    inner: Arc<InMemory>,
}

impl std::fmt::Display for RefusingDeleteStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RefusingDeleteStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RefusingDeleteStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
        // `ObjectStoreExt::delete` routes through this, so refusing here refuses
        // every segment the cleaner hands over.
        locations
            .map(|_| {
                Err(object_store::Error::Generic {
                    store: "RefusingDeleteStore",
                    source: "deletion refused".into(),
                })
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: object_store::CopyOptions) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, opts).await
    }
}

/// A table the catalog cannot hand over — dropped, renamed, a catalog that is
/// down — leaves the bound underived, and a destructive operation must fail
/// CLOSED on that too. Treating the failure as "no snapshots" would read as "no
/// bound" and hide a broken catalog behind quiet, harmless-looking cycles.
#[tokio::test]
async fn a_missing_table_fails_the_cycle_and_deletes_nothing() {
    const MISSING_TABLE: &str = "no_such_table";

    let (catalog, _warehouse) = build_catalog().await;
    commit_offset(&catalog, Some(COMMITTED_OFFSET)).await;
    let store = Arc::new(InMemory::new());
    seed_segments(&store, 0..SEGMENT_COUNT).await;
    let executor = build_executor_for_table(&catalog, Arc::clone(&store) as _, cleanup_config(), MISSING_TABLE);

    let error = executor
        .clean_topic(&CancellationToken::new())
        .await
        .expect_err("a table that cannot be loaded must fail the cycle");

    assert!(
        error.to_string().contains(MISSING_TABLE),
        "the failure must name the table to fix, got: {error}"
    );
    assert_eq!(surviving_offsets(&store).await, (0..SEGMENT_COUNT).collect::<Vec<_>>());
}

/// A table that was never shifted to has no committed offset at all: every
/// segment is still the only copy of its rows, so nothing may go.
#[tokio::test]
async fn a_table_without_snapshots_deletes_nothing() {
    let (catalog, _warehouse) = build_catalog().await;
    let store = Arc::new(InMemory::new());
    seed_segments(&store, 0..SEGMENT_COUNT).await;
    let executor = build_executor(&catalog, Arc::clone(&store) as _, cleanup_config());

    executor
        .clean_topic(&CancellationToken::new())
        .await
        .expect("a table with no snapshot is not an error");

    assert_eq!(surviving_offsets(&store).await, (0..SEGMENT_COUNT).collect::<Vec<_>>());
}
