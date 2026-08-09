//! Snapshot retention, end to end against a real catalog.
//!
//! What `migrate create` stamps onto a table is the whole of IceGate's side of
//! expiration: the plan is computed and applied upstream, by every commit, off
//! those properties. These tests therefore drive the real path — create the
//! tables through [`create_tables`], commit through a `Transaction` the way the
//! Shifter does — and assert what the stamp is responsible for: the history
//! stays inside the configured window, the WAL offset stays resolvable across
//! the snapshots expiration removes, and the storage those snapshots held is
//! actually reclaimed by the GC sweep, which is the reason the policy exists.
//!
//! Requires Docker:
//!
//! ```text
//! cargo test -p icegate-maintain --test snapshot_expiration_it
//! ```

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::Utc;
use common::{
    BUCKET_NAME, DAY_MICROS, StorageConn, build_operator_registry, build_s3_catalog, list_all_object_keys, logs_batch,
    setup_object_store, write_one_file,
};
use iceberg::Catalog;
use iceberg::spec::DataFile;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use icegate_catalog_s3::S3Catalog;
use icegate_common::{LOGS_TABLE, WAL_OFFSET_PROPERTY, icegate_table_ident, resolve_wal_offset};
use icegate_maintain::gc::config::GcOrphansConfig;
use icegate_maintain::gc::metrics::GcMetrics;
use icegate_maintain::gc::sweep::run_sweep;
use icegate_maintain::migrate::config::SnapshotExpirationConfig;
use icegate_maintain::migrate::operations::create_tables;
use tokio_util::sync::CancellationToken;

/// Ancestors kept regardless of age. Two rather than one so a test can tell
/// "the window held it" from "nothing was expired at all".
const MIN_SNAPSHOTS_TO_KEEP: usize = 2;

/// Age window of a single millisecond: a commit against the object store takes
/// far longer than that, so by the time the next commit plans, every earlier
/// snapshot is past the window and only the explicit protections remain.
const MAX_SNAPSHOT_AGE_MS: i64 = 1;

/// Summary key every commit below carries, so an append with no data files is
/// still a well-formed snapshot (an append with neither files nor summary
/// properties is rejected upstream). Its value is never read; what matters is
/// that a snapshot can be built without the WAL offset in its summary.
const COMMIT_MARKER_KEY: &str = "icegate.test.commit";

/// A policy that expires aggressively, so the protections are what is under
/// test rather than the width of the window.
const fn narrow_window() -> SnapshotExpirationConfig {
    SnapshotExpirationConfig {
        enabled: true,
        min_snapshots_to_keep: MIN_SNAPSHOTS_TO_KEEP,
        max_snapshot_age_ms: MAX_SNAPSHOT_AGE_MS,
        metadata_previous_versions_max: MIN_SNAPSHOTS_TO_KEEP,
    }
}

/// Create the IceGate tables under `expiration`.
async fn create_logs_table(catalog: &Arc<S3Catalog>, expiration: &SnapshotExpirationConfig) {
    let dyn_catalog: Arc<dyn Catalog> = Arc::clone(catalog) as Arc<dyn Catalog>;
    create_tables(&dyn_catalog, expiration, false).await.expect("create tables");
}

/// Commit an empty append, optionally stamping the WAL offset onto the snapshot
/// summary the way the Shifter does. Data files are beside the point here — a
/// snapshot counts towards the retention window whether or not it added rows,
/// and the offset lives in the summary either way.
async fn commit_snapshot(catalog: &S3Catalog, offset: Option<u64>) {
    let table = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");

    let mut properties = HashMap::from([(COMMIT_MARKER_KEY.to_string(), "1".to_string())]);
    if let Some(offset) = offset {
        properties.insert(WAL_OFFSET_PROPERTY.to_string(), offset.to_string());
    }

    let tx = Transaction::new(&table);
    let action = tx.fast_append().set_snapshot_properties(properties);
    let tx = action.apply(tx).expect("apply fast append");
    tx.commit(catalog).await.expect("commit snapshot");
}

/// Appends committed before the rewrite; each lands in its own snapshot and
/// contributes one data file the rewrite then replaces.
const APPENDS_BEFORE_REWRITE: usize = 3;

/// Appends committed after the rewrite, enough to push the rewrite snapshot and
/// everything before it out of a [`MIN_SNAPSHOTS_TO_KEEP`]-wide window.
const APPENDS_AFTER_REWRITE: usize = 2;

/// WAL offset the last seeded append stamps: the appends carry `0..n`, so the
/// newest is one below their total.
const LAST_COMMITTED_OFFSET: u64 = (APPENDS_BEFORE_REWRITE + APPENDS_AFTER_REWRITE - 1) as u64;

/// The files a seeded history left in object storage, captured as it was built
/// rather than read back from the table, so the assertions never depend on the
/// reachability walk the sweep itself uses.
struct SeededHistory {
    /// Data files the rewrite replaced. Referenced only by the snapshots that
    /// preceded it — the storage a bounded history is supposed to free.
    replaced_data_files: Vec<String>,
    /// Data files the current snapshot still names: the rewrite's output and
    /// every append after it.
    live_data_files: Vec<String>,
    /// Manifest-list path of each snapshot, in commit order. A manifest list
    /// belongs to exactly one snapshot and is never inherited, so its presence
    /// is a direct statement about that snapshot's own files.
    manifest_lists: Vec<String>,
}

/// Commit one data file in its own `fast_append`, stamping the WAL offset onto
/// the snapshot summary the way the Shifter does, and returning the committed
/// file. The row tag doubles as the offset, so each is unique per commit.
async fn append_one_file(catalog: &S3Catalog, unique_offset: usize) -> DataFile {
    let table = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");
    let data_file = write_one_file(&table, logs_batch(&[("svc", DAY_MICROS)], unique_offset)).await;

    let tx = Transaction::new(&table);
    let action = tx
        .fast_append()
        .add_data_files(vec![data_file.clone()])
        .set_snapshot_properties(HashMap::from([(
            WAL_OFFSET_PROPERTY.to_string(),
            unique_offset.to_string(),
        )]));
    let tx = action.apply(tx).expect("apply fast append");
    tx.commit(catalog).await.expect("commit append");
    data_file
}

/// Replace `replaced` with a single file in one `replace` snapshot, the way
/// data compaction commits its rewrites — WAL offset carried forward from the
/// snapshot the rewrite supersedes. The output carries the same rows, so the
/// rewrite loses no data on its way through the sweep.
async fn rewrite_into_one_file(catalog: &S3Catalog, replaced: Vec<DataFile>) -> DataFile {
    let table = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");
    let rows = vec![("svc", DAY_MICROS); replaced.len()];
    let compacted = write_one_file(&table, logs_batch(&rows, 0)).await;

    let tx = Transaction::new(&table);
    let tx = tx
        .rewrite_files()
        .add_data_files(vec![compacted.clone()])
        .delete_files(replaced)
        .inherit_summary_property(WAL_OFFSET_PROPERTY)
        .apply(tx)
        .expect("apply rewrite");
    tx.commit(catalog).await.expect("commit rewrite");
    compacted
}

/// The manifest-list path of the table's current snapshot.
async fn read_head_manifest_list(catalog: &S3Catalog) -> String {
    let table = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");
    table
        .metadata()
        .current_snapshot()
        .expect("a committed snapshot")
        .manifest_list()
        .to_string()
}

/// Seed the history compaction leaves behind: appends, a rewrite that replaces
/// all of them with one file, then further appends. Without expiration every
/// replaced file stays referenced by the snapshots that preceded the rewrite,
/// which is precisely why compaction alone never frees storage.
async fn seed_rewritten_history(catalog: &S3Catalog) -> SeededHistory {
    let mut manifest_lists = Vec::with_capacity(APPENDS_BEFORE_REWRITE + 1 + APPENDS_AFTER_REWRITE);
    let mut replaced = Vec::with_capacity(APPENDS_BEFORE_REWRITE);
    for unique_offset in 0..APPENDS_BEFORE_REWRITE {
        replaced.push(append_one_file(catalog, unique_offset).await);
        manifest_lists.push(read_head_manifest_list(catalog).await);
    }
    let replaced_data_files = replaced.iter().map(|file| file.file_path().to_string()).collect();

    let compacted = rewrite_into_one_file(catalog, replaced).await;
    manifest_lists.push(read_head_manifest_list(catalog).await);

    let mut live_data_files = vec![compacted.file_path().to_string()];
    for index in 0..APPENDS_AFTER_REWRITE {
        let appended = append_one_file(catalog, APPENDS_BEFORE_REWRITE + index).await;
        live_data_files.push(appended.file_path().to_string());
        manifest_lists.push(read_head_manifest_list(catalog).await);
    }

    SeededHistory {
        replaced_data_files,
        live_data_files,
        manifest_lists,
    }
}

/// Run one orphan sweep of `logs` with no grace period, so the assertion is
/// about reachability alone rather than about how long ago a file was written.
async fn sweep_logs_orphans(catalog: &Arc<S3Catalog>, conn: &StorageConn) {
    let dyn_catalog: Arc<dyn Catalog> = Arc::clone(catalog) as Arc<dyn Catalog>;
    let orphans = GcOrphansConfig {
        enabled: true,
        dry_run: false,
        min_age_secs: 0,
        include_metadata: true,
        delete_concurrency: 8,
        sweep_timeout_secs: 600,
    };
    let operator_registry = build_operator_registry(conn).await;
    run_sweep(
        &dyn_catalog,
        &operator_registry,
        LOGS_TABLE,
        &orphans,
        Utc::now(),
        &GcMetrics::new(),
        &CancellationToken::new(),
    )
    .await
    .expect("sweep succeeds");
}

/// The bucket-relative key of an absolute table URI, which is how the object
/// store lists it.
fn to_object_key(uri: &str) -> String {
    uri.strip_prefix(&format!("s3://{BUCKET_NAME}/"))
        .expect("table files live under s3://<bucket>/ with this catalog")
        .to_string()
}

/// Every object key currently in the bucket.
async fn read_object_keys(conn: &StorageConn) -> HashSet<String> {
    list_all_object_keys(conn).await.into_iter().collect()
}

/// Assert each URI in `uris` is (or is not) backed by an object right now.
fn assert_objects_exist(keys: &HashSet<String>, uris: &[String], expected: bool, reason: &str) {
    for uri in uris {
        let key = to_object_key(uri);
        assert_eq!(keys.contains(&key), expected, "{reason}: {key}");
    }
}

/// The current state of `logs` as `(snapshot count, resolved WAL offset)`.
async fn read_history(catalog: &S3Catalog) -> (usize, Option<u64>) {
    let table = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");
    let metadata = table.metadata();
    let offset = resolve_wal_offset(metadata).expect("the WAL offset must stay resolvable");
    (metadata.snapshots().len(), offset)
}

/// Every commit carries the offset, so the carrier is the current snapshot and
/// nothing beyond the window needs protecting: the history collapses to the
/// window itself, and the offset is still the one the last commit recorded.
#[tokio::test]
#[ignore = "expiration have to merged with new implementation"]
async fn expiration_holds_the_history_at_the_configured_window() {
    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    create_logs_table(&catalog, &narrow_window()).await;

    for offset in 1..=6_u64 {
        commit_snapshot(&catalog, Some(offset)).await;
    }

    let (snapshot_count, offset) = read_history(&catalog).await;
    assert_eq!(
        snapshot_count, MIN_SNAPSHOTS_TO_KEEP,
        "six commits, a two-snapshot window: only the current snapshot and its parent survive"
    );
    assert_eq!(offset, Some(6), "the newest commit's offset must survive");
}

/// The case the preserved-summary property exists for: compaction and any other
/// writer commit snapshots that carry no offset, so the carrier sinks out of the
/// retention window. Expiring it — or any link between it and the current
/// snapshot — would send the Shifter back to offset 0 and re-commit the queue.
#[tokio::test]
async fn expiration_keeps_a_wal_offset_carrier_outside_the_window() {
    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    create_logs_table(&catalog, &narrow_window()).await;

    commit_snapshot(&catalog, Some(42)).await;
    // Five offset-less commits push the carrier well past a two-snapshot window.
    for _ in 0..5 {
        commit_snapshot(&catalog, None).await;
    }

    let (snapshot_count, offset) = read_history(&catalog).await;
    assert_eq!(
        offset,
        Some(42),
        "the offset must still resolve by walking the ancestor chain to its carrier"
    );
    assert_eq!(
        snapshot_count, 6,
        "the carrier and every link from the current snapshot down to it must survive"
    );
}

/// A table created with the policy switched off keeps every snapshot: the
/// property is the only thing that makes a writer expire anything, and `false`
/// must read as "off", not as "use the spec default".
#[tokio::test]
async fn a_table_created_with_expiration_disabled_keeps_every_snapshot() {
    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let disabled = SnapshotExpirationConfig {
        enabled: false,
        ..narrow_window()
    };
    create_logs_table(&catalog, &disabled).await;

    for offset in 1..=4_u64 {
        commit_snapshot(&catalog, Some(offset)).await;
    }

    let (snapshot_count, offset) = read_history(&catalog).await;
    assert_eq!(snapshot_count, 4, "expiration is off: no snapshot may be removed");
    assert_eq!(offset, Some(4));
}

/// The point of the policy: storage a compaction rewrote is actually reclaimed.
/// Trimming the history only moves the files out of the referenced set — the GC
/// sweep is what deletes them, and it reads that set from the current metadata
/// (`gc/reachable.rs`). This drives the whole chain on one table: rewrite the
/// data the way compaction does, let the window drop the snapshots that still
/// named the originals, then sweep and check the objects themselves.
#[tokio::test]
#[ignore = "expiration have to merged with new implementation"]
async fn the_sweep_reclaims_the_data_and_manifests_of_expired_snapshots() {
    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    create_logs_table(&catalog, &narrow_window()).await;

    let history = seed_rewritten_history(&catalog).await;

    let (snapshot_count, offset) = read_history(&catalog).await;
    assert_eq!(
        snapshot_count, MIN_SNAPSHOTS_TO_KEEP,
        "the fixture must reach the condition under test: every snapshot but the newest two gone"
    );
    assert_eq!(
        offset,
        Some(LAST_COMMITTED_OFFSET),
        "the newest commit's offset survives"
    );
    let before = read_object_keys(&conn).await;
    assert_objects_exist(
        &before,
        &history.replaced_data_files,
        true,
        "a rewritten file is unreferenced but still on disk until the sweep runs",
    );

    sweep_logs_orphans(&catalog, &conn).await;

    let after = read_object_keys(&conn).await;
    assert_objects_exist(
        &after,
        &history.replaced_data_files,
        false,
        "the data files of the expired snapshots must be reclaimed",
    );
    let expired_count = history.manifest_lists.len() - MIN_SNAPSHOTS_TO_KEEP;
    assert_objects_exist(
        &after,
        &history.manifest_lists[..expired_count],
        false,
        "the manifest list of an expired snapshot is referenced by nothing",
    );
    assert_objects_exist(
        &after,
        &history.manifest_lists[expired_count..],
        true,
        "the retained snapshots' manifest lists must survive",
    );
    assert_objects_exist(
        &after,
        &history.live_data_files,
        true,
        "a file the current snapshot names must survive",
    );

    // The metadata the table still points at, and the metadata-log the S3
    // catalog resolves a lost commit ack against, are referenced by definition.
    let table = catalog
        .load_table(&icegate_table_ident(LOGS_TABLE))
        .await
        .expect("load logs table");
    let metadata_files: Vec<String> =
        std::iter::once(table.metadata_location().expect("a metadata pointer").to_string())
            .chain(table.metadata().metadata_log().iter().map(|entry| entry.metadata_file.clone()))
            .collect();
    assert_objects_exist(
        &after,
        &metadata_files,
        true,
        "the current metadata and every retained metadata-log entry must survive",
    );
}

/// The same history without the policy, which is what makes the test above a
/// statement about expiration rather than about GC: with every snapshot kept,
/// the pre-rewrite snapshots still name the replaced files, the sweep finds no
/// orphan among them, and compaction has freed nothing.
#[tokio::test]
async fn the_sweep_keeps_rewritten_files_while_expiration_is_disabled() {
    let (_store, conn) = setup_object_store().await;
    let catalog = Arc::new(build_s3_catalog(&conn).await);
    let disabled = SnapshotExpirationConfig {
        enabled: false,
        ..narrow_window()
    };
    create_logs_table(&catalog, &disabled).await;

    let history = seed_rewritten_history(&catalog).await;

    let (snapshot_count, offset) = read_history(&catalog).await;
    assert_eq!(
        snapshot_count,
        APPENDS_BEFORE_REWRITE + 1 + APPENDS_AFTER_REWRITE,
        "expiration is off: the rewrite and every append keep their snapshot"
    );
    assert_eq!(offset, Some(LAST_COMMITTED_OFFSET));

    sweep_logs_orphans(&catalog, &conn).await;

    let after = read_object_keys(&conn).await;
    assert_objects_exist(
        &after,
        &history.replaced_data_files,
        true,
        "an unbounded history keeps every rewritten file referenced, so the sweep must not take it",
    );
    assert_objects_exist(
        &after,
        &history.manifest_lists,
        true,
        "every snapshot survives, so does its manifest list",
    );
}
