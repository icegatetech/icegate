//! Garbage collection of superseded snapshot objects.
//!
//! A snapshot object may be deleted once no reader can reach it any more: the pointer no longer
//! names it, every scroll that read it has expired, and every write attempt that read it has
//! ended. A scroll opens on the snapshot object that is current and expires at most
//! `max_scroll_lifetime_secs` after it opened; a write attempt ends within the write deadline
//! (`engine.max_query_duration_secs`). A snapshot object stops being current at the pointer swap
//! to a successor (a snapshot object whose lineage names it as predecessor), and that swap comes
//! within the write deadline of the successor's write. So a snapshot object whose newest successor
//! is older than `max_scroll_lifetime_secs + engine.max_query_duration_secs` is unreachable: the
//! write deadline in that sum absorbs the lag between writing a successor and swapping to it.
//! Every age is measured on the object store's clock: a pass writes a probe object and reads its
//! `Last-Modified`, and compares it with the `Last-Modified` the listing reports, so no replica
//! clock enters the decision. When an age cannot be shown to exceed the retention (no successor, a
//! successor stamped after the probe, a tie), the object stays; when the probe, the pointer, the
//! listing or a lineage frame cannot be read, the pass deletes nothing.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use futures::{StreamExt as _, TryStreamExt as _};
use icegate_common::TenantId;
use object_store::path::Path;
use object_store::{ObjectStoreExt as _, PutPayload};

use super::client::SystemObjectStore;
use super::store::{SNAPSHOT_NAME_PREFIX, read_tenant_pointer};
use crate::opensearch::error::OpenSearchError;

/// The per-tenant object whose `Last-Modified` is the store's clock for one pass.
const CLOCK_PROBE_NAME: &str = "gc-probe";

/// How many lineage frames one pass reads at once.
const LINEAGE_READ_CONCURRENCY: usize = 16;

/// One listed snapshot object with the predecessor its lineage frame records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotListing {
    /// The object's snapshot name, `snap-<uuid>`.
    pub(crate) snapshot_name: String,
    /// `Last-Modified` as the object store's listing reported it.
    pub(crate) last_modified: DateTime<Utc>,
    /// The snapshot object this one superseded.
    pub(crate) predecessor: Option<String>,
}

/// The snapshot objects garbage collection may delete, in name order.
///
/// A snapshot object qualifies when it is not `current_snapshot_name` and the newest listed
/// snapshot object naming it as predecessor was written more than `retention` before
/// `store_now`, both instants read from the object store's clock. No successor, a successor
/// exactly `retention` old, or one stamped after `store_now` keeps it, and so does a `retention`
/// no time span can hold.
pub(crate) fn choose_superseded_snapshots(
    listings: &[SnapshotListing],
    current_snapshot_name: &str,
    store_now: DateTime<Utc>,
    retention: Duration,
) -> Vec<String> {
    let Ok(retention) = TimeDelta::from_std(retention) else {
        return Vec::new();
    };
    let mut latest_successor_times: BTreeMap<&str, DateTime<Utc>> = BTreeMap::new();
    for listing in listings {
        if let Some(predecessor) = listing.predecessor.as_deref() {
            latest_successor_times
                .entry(predecessor)
                .and_modify(|latest_time| *latest_time = (*latest_time).max(listing.last_modified))
                .or_insert(listing.last_modified);
        }
    }
    let mut superseded_names: Vec<String> = listings
        .iter()
        .filter(|listing| listing.snapshot_name != current_snapshot_name)
        .filter(|listing| {
            latest_successor_times
                .get(listing.snapshot_name.as_str())
                .is_some_and(|&successor_time| store_now.signed_duration_since(successor_time) > retention)
        })
        .map(|listing| listing.snapshot_name.clone())
        .collect();
    superseded_names.sort();
    superseded_names.dedup();
    superseded_names
}

/// Deletes the superseded snapshot objects of `tenant` and returns how many are gone.
///
/// `current_snapshot_name` is the snapshot the pointer named when the pass was scheduled, never
/// deleted. One pass writes the tenant's clock probe, reads the pointer, lists the tenant's objects
/// once, reads the lineage frame of each snapshot object with ranged reads (never a snapshot
/// frame), and deletes what [`choose_superseded_snapshots`] picks except the object the pointer
/// named. The pointer is read before the listing, so the retention bound holds for a pass that
/// starts late too: the object current at that read stays, and an object superseded before it had
/// the successor it was swapped to written before the listing, so the listing shows that successor
/// and its age. A snapshot object that disappears between the listing and its lineage read is
/// skipped; one whose delete answers not-found was collected by a concurrent pass and counts as
/// collected.
///
/// # Errors
///
/// An internal error when the probe cannot be written or read back, the pointer cannot be read or
/// decoded, the listing fails or a lineage frame cannot be read or decoded (nothing is deleted
/// then), or a delete fails.
pub(crate) async fn collect_superseded_snapshots(
    system_object_store: &SystemObjectStore,
    tenant: &TenantId,
    current_snapshot_name: &str,
    retention: Duration,
) -> Result<usize, OpenSearchError> {
    let object_store = system_object_store.object_store();
    let tenant_path = system_object_store.tenant_path(tenant);
    let probe_path = tenant_path.clone().join(CLOCK_PROBE_NAME);
    object_store
        .put(&probe_path, PutPayload::from_static(b"{}"))
        .await
        .map_err(|error| build_collection_failure(tenant, "write the clock probe", &error))?;
    let store_now = object_store
        .head(&probe_path)
        .await
        .map_err(|error| build_collection_failure(tenant, "read the clock probe", &error))?
        .last_modified;
    let pointer_target = read_tenant_pointer(system_object_store, tenant)
        .await?
        .map(|(_, snapshot_name)| snapshot_name);
    let candidates: Vec<(String, DateTime<Utc>)> = object_store
        .list(Some(&tenant_path))
        .try_filter_map(|metadata| {
            // Only direct children named like snapshot objects: the pointer and the probe are
            // never candidates.
            let candidate = metadata.location.prefix_match(&tenant_path).and_then(|mut child_parts| {
                let name_part = child_parts.next()?;
                let snapshot_name = name_part.as_ref();
                (child_parts.next().is_none() && snapshot_name.starts_with(SNAPSHOT_NAME_PREFIX))
                    .then(|| (snapshot_name.to_owned(), metadata.last_modified))
            });
            futures::future::ready(Ok(candidate))
        })
        .try_collect()
        .await
        .map_err(|error| build_collection_failure(tenant, "list snapshot objects", &error))?;
    let listings: Vec<SnapshotListing> = futures::stream::iter(candidates)
        .map(|(snapshot_name, last_modified)| async move {
            let lineage = system_object_store.read_lineage_frame(tenant, &snapshot_name).await?;
            Ok::<_, OpenSearchError>(lineage.map(|lineage| SnapshotListing {
                snapshot_name,
                last_modified,
                predecessor: lineage.predecessor,
            }))
        })
        .buffer_unordered(LINEAGE_READ_CONCURRENCY)
        .try_filter_map(futures::future::ok)
        .try_collect()
        .await?;
    let superseded_paths: Vec<object_store::Result<Path>> =
        choose_superseded_snapshots(&listings, current_snapshot_name, store_now, retention)
            .into_iter()
            .filter(|snapshot_name| pointer_target.as_deref() != Some(snapshot_name.as_str()))
            .map(|snapshot_name| Ok(system_object_store.snapshot_path(tenant, &snapshot_name)))
            .collect();
    let collected_count = superseded_paths.len();
    // One delete stream for the pass: `delete_stream` deletes in bulk where the store supports it.
    let mut delete_outcomes = object_store.delete_stream(futures::stream::iter(superseded_paths).boxed());
    while let Some(delete_outcome) = delete_outcomes.next().await {
        match delete_outcome {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                return Err(build_collection_failure(
                    tenant,
                    "delete a superseded snapshot object",
                    &error,
                ));
            }
        }
    }
    Ok(collected_count)
}

/// The `500` of a pass that could not do `step` for `tenant`.
fn build_collection_failure(tenant: &TenantId, step: &str, error: &object_store::Error) -> OpenSearchError {
    OpenSearchError::internal(format!(
        "system-index garbage collection for tenant [{tenant}] could not {step}: {error}"
    ))
}

#[cfg(test)]
mod tests {
    use std::slice;
    use std::sync::Arc;
    use std::time::Duration;

    use axum::http::StatusCode;
    use chrono::{DateTime, TimeDelta, Utc};
    use icegate_common::TenantId;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
    use serde_json::{Value, json};

    use super::{SnapshotListing, choose_superseded_snapshots, collect_superseded_snapshots};
    use crate::opensearch::context::RequestContext;
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::opensearch::extract::parse_parameters;
    use crate::opensearch::search::request::{SearchParameters, parse_search_value};
    use crate::opensearch::system_index::bulk::BulkItem;
    use crate::opensearch::system_index::client::SystemObjectStore;
    use crate::opensearch::system_index::codec::{SnapshotLineage, encode_snapshot_object};
    use crate::opensearch::system_index::scroll::{ScrollCursor, ScrollOpening, open_system_scroll, resume_scroll};
    use crate::opensearch::system_index::search::search_system_indices;
    use crate::opensearch::system_index::snapshot::{
        DocumentWrite, OpType, SystemOperation, SystemSnapshot, WriteConcurrency,
    };
    use crate::opensearch::system_index::store::{SystemIndexLimits, SystemIndexStore, read_tenant_pointer};
    use crate::test_support::{ClockedObjectStore, build_tenant, wait_for_condition, yield_to_background_passes};

    const LOCATION: &str = "s3://opensearch-test/opensearch-system";
    /// The longest scroll keep-alive `build_index_store` allows; every scroll below opens with it.
    const MAX_SCROLL_LIFETIME: Duration = Duration::from_mins(15);
    /// The query string of every scroll below: a keep-alive of [`MAX_SCROLL_LIFETIME`].
    const SCROLL_QUERY_STRING: &str = "scroll=15m";
    /// The retention the literal instants below are stated against: the sum of the scroll
    /// lifetime and the write deadline that `build_index_store` configures.
    const RETENTION: Duration = Duration::from_secs(930);

    fn build_instant(minute: i64, second: i64) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-10-08T12:00:00Z")
            .expect("literal instant")
            .with_timezone(&Utc)
            + TimeDelta::minutes(minute)
            + TimeDelta::seconds(second)
    }

    fn build_listing(name: &str, last_modified: DateTime<Utc>, predecessor: Option<&str>) -> SnapshotListing {
        SnapshotListing {
            snapshot_name: name.to_owned(),
            last_modified,
            predecessor: predecessor.map(str::to_owned),
        }
    }

    #[test]
    fn a_snapshot_object_is_chosen_only_when_unreferenced_and_its_latest_successor_is_older_than_the_retention() {
        let listings = [
            build_listing("snap-a", build_instant(0, 0), None),
            build_listing("snap-b", build_instant(1, 0), Some("snap-a")),
            build_listing("snap-c", build_instant(30, 0), Some("snap-b")),
            build_listing("snap-orphan", build_instant(2, 0), Some("snap-b")),
        ];

        let superseded_names = choose_superseded_snapshots(&listings, "snap-c", build_instant(40, 0), RETENTION);

        // snap-a: its successor snap-b is 39 minutes old. snap-b: its newest successor snap-c is
        // 10 minutes old; the older orphan successor does not count. snap-orphan has no
        // successor; snap-c is current.
        assert_eq!(superseded_names, ["snap-a"]);
    }

    #[test]
    fn a_snapshot_object_without_a_successor_is_kept() {
        let listings = [
            build_listing("snap-a", build_instant(0, 0), None),
            build_listing("snap-lost", build_instant(0, 1), Some("snap-a")),
        ];

        let superseded_names = choose_superseded_snapshots(&listings, "snap-a", build_instant(600, 0), RETENTION);

        assert!(superseded_names.is_empty(), "{superseded_names:?}");
    }

    #[test]
    fn a_successor_at_the_retention_boundary_keeps_the_snapshot_object() {
        let listings = [
            build_listing("snap-a", build_instant(0, 0), None),
            build_listing("snap-b", build_instant(1, 0), Some("snap-a")),
        ];

        let below_boundary = choose_superseded_snapshots(&listings, "snap-b", build_instant(1, 929), RETENTION);
        let at_boundary = choose_superseded_snapshots(&listings, "snap-b", build_instant(1, 930), RETENTION);
        let above_boundary = choose_superseded_snapshots(&listings, "snap-b", build_instant(1, 931), RETENTION);

        assert!(below_boundary.is_empty(), "{below_boundary:?}");
        assert!(at_boundary.is_empty(), "{at_boundary:?}");
        assert_eq!(above_boundary, ["snap-a"]);
    }

    #[test]
    fn the_current_snapshot_object_is_never_chosen() {
        // A successor whose swap failed names the current snapshot object as its predecessor;
        // however old that successor is, the current one stays.
        let listings = [
            build_listing("snap-a", build_instant(0, 0), None),
            build_listing("snap-failed", build_instant(1, 0), Some("snap-a")),
        ];

        let superseded_names = choose_superseded_snapshots(&listings, "snap-a", build_instant(600, 0), RETENTION);

        assert!(superseded_names.is_empty(), "{superseded_names:?}");
    }

    #[test]
    fn a_successor_stamped_after_the_store_clock_keeps_the_snapshot_object() {
        let listings = [
            build_listing("snap-a", build_instant(0, 0), None),
            build_listing("snap-b", build_instant(30, 0), Some("snap-a")),
        ];

        let superseded_names = choose_superseded_snapshots(&listings, "snap-b", build_instant(10, 0), RETENTION);

        assert!(superseded_names.is_empty(), "{superseded_names:?}");
    }

    fn build_clocked_system_object_store(clocked_store: &Arc<ClockedObjectStore>) -> SystemObjectStore {
        SystemObjectStore::new(Arc::clone(clocked_store) as Arc<dyn ObjectStore>, LOCATION).expect("an S3 location")
    }

    fn build_index_store(system_object_store: SystemObjectStore) -> SystemIndexStore {
        SystemIndexStore::new(
            system_object_store,
            SystemIndexLimits {
                max_snapshot_bytes: 4 << 20,
                cache_max_bytes: 8 << 20,
                write_deadline: Duration::from_secs(30),
                max_scroll_lifetime: MAX_SCROLL_LIFETIME,
            },
        )
    }

    /// An empty snapshot object whose lineage names `predecessor`.
    fn encode_empty_snapshot_object(predecessor: Option<&str>) -> PutPayload {
        let lineage = SnapshotLineage {
            predecessor: predecessor.map(str::to_owned),
            operation_ids: Vec::new(),
        };
        let encoded_object = encode_snapshot_object(&lineage, &SystemSnapshot::default()).expect("encodes");
        encoded_object.bytes.into()
    }

    /// The pointer naming `snapshot_name`, in the form the store writes it.
    fn encode_pointer(snapshot_name: &str) -> PutPayload {
        json!({"snapshot_name": snapshot_name}).to_string().into()
    }

    /// Writes an empty snapshot object named `name` whose lineage names `predecessor`, stamped with
    /// the double's current time.
    async fn put_snapshot_object(
        system_object_store: &SystemObjectStore,
        tenant: &TenantId,
        name: &str,
        predecessor: Option<&str>,
    ) {
        system_object_store
            .object_store()
            .put(
                &system_object_store.snapshot_path(tenant, name),
                encode_empty_snapshot_object(predecessor),
            )
            .await
            .expect("stored");
    }

    /// Writes `snap-a` at minute 0 and its successor `snap-b` at minute 1, then sets the store
    /// clock to minute 60: `snap-a` is eligible (its successor is 59 minutes old) unless the pass
    /// fails, and `snap-b` is the current one.
    async fn put_eligible_snapshot_pair(
        clocked_store: &ClockedObjectStore,
        system_object_store: &SystemObjectStore,
        tenant: &TenantId,
    ) {
        clocked_store.set_time(build_instant(0, 0));
        put_snapshot_object(system_object_store, tenant, "snap-a", None).await;
        clocked_store.set_time(build_instant(1, 0));
        put_snapshot_object(system_object_store, tenant, "snap-b", Some("snap-a")).await;
        clocked_store.set_time(build_instant(60, 0));
    }

    async fn has_object(system_object_store: &SystemObjectStore, path: &Path) -> bool {
        match system_object_store.object_store().head(path).await {
            Ok(_) => true,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(error) => panic!("head {path}: {error}"),
        }
    }

    async fn has_snapshot_object(system_object_store: &SystemObjectStore, tenant: &TenantId, name: &str) -> bool {
        has_object(system_object_store, &system_object_store.snapshot_path(tenant, name)).await
    }

    fn build_probe_path(system_object_store: &SystemObjectStore, tenant: &TenantId) -> Path {
        system_object_store.tenant_path(tenant).join("gc-probe")
    }

    /// The `Last-Modified` of the tenant's clock probe: the store time at which the latest pass
    /// started; `None` before any pass.
    async fn read_probe_time(system_object_store: &SystemObjectStore, tenant: &TenantId) -> Option<DateTime<Utc>> {
        match system_object_store
            .object_store()
            .head(&build_probe_path(system_object_store, tenant))
            .await
        {
            Ok(metadata) => Some(metadata.last_modified),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(error) => panic!("head the clock probe: {error}"),
        }
    }

    /// Yields until a background pass has deleted `path`, failing after the yield budget.
    async fn wait_for_deletion(system_object_store: &SystemObjectStore, path: &Path) {
        wait_for_condition(
            move || async move { !has_object(system_object_store, path).await },
            &format!("the background pass never deleted {path}"),
        )
        .await;
    }

    /// Yields until a background pass started at the store time `probe_time`, failing after the
    /// yield budget.
    async fn wait_for_probe_time(
        system_object_store: &SystemObjectStore,
        tenant: &TenantId,
        probe_time: DateTime<Utc>,
    ) {
        wait_for_condition(
            move || async move { read_probe_time(system_object_store, tenant).await == Some(probe_time) },
            &format!("no pass started at {probe_time}"),
        )
        .await;
    }

    /// A failed pass answers the error envelope's `500` with type `exception`.
    async fn assert_internal_error(error: OpenSearchError) {
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        assert_eq!(body["error"]["type"], "exception", "{body}");
    }

    fn build_index_creation(name: &str) -> SystemOperation {
        SystemOperation::CreateIndex {
            name: name.to_owned(),
            mappings: json!({"dynamic": "strict", "properties": {"type": {"type": "keyword"}}}),
            settings: json!({"number_of_shards": 1}),
        }
    }

    fn build_document_write(index: &str, id: &str) -> DocumentWrite {
        DocumentWrite {
            index: index.to_owned(),
            id: id.to_owned(),
            source: json!({"type": "visualization"}),
            op_type: OpType::Index,
            concurrency: WriteConcurrency {
                if_seq_no: None,
                if_primary_term: None,
            },
        }
    }

    fn build_indexing_operation(index: &str, id: &str) -> SystemOperation {
        SystemOperation::IndexDocument(build_document_write(index, id))
    }

    async fn read_current_snapshot_name(index_store: &SystemIndexStore, tenant: &TenantId) -> String {
        index_store
            .read(tenant)
            .await
            .expect("read")
            .snapshot_name
            .as_deref()
            .expect("a pointer exists")
            .to_owned()
    }

    #[tokio::test]
    async fn a_snapshot_object_already_deleted_counts_as_collected() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_snapshot_object(&system_object_store, &tenant, "snap-a", None).await;
        clocked_store.set_time(build_instant(1, 0));
        put_snapshot_object(&system_object_store, &tenant, "snap-b", Some("snap-a")).await;
        clocked_store.set_time(build_instant(1, 931));
        // Another pass deletes snap-a between this pass's listing and its delete.
        clocked_store.report_next_delete_as_missing(system_object_store.snapshot_path(&tenant, "snap-a"));

        let collected_count = collect_superseded_snapshots(&system_object_store, &tenant, "snap-b", RETENTION)
            .await
            .expect("the pass succeeds");

        assert_eq!(collected_count, 1);
        assert!(!has_snapshot_object(&system_object_store, &tenant, "snap-a").await);
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-b").await);
    }

    #[tokio::test]
    async fn a_snapshot_object_gone_before_its_lineage_read_is_skipped_and_the_pass_collects_the_rest() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_snapshot_object(&system_object_store, &tenant, "snap-a", None).await;
        clocked_store.set_time(build_instant(1, 0));
        put_snapshot_object(&system_object_store, &tenant, "snap-b", Some("snap-a")).await;
        clocked_store.set_time(build_instant(2, 0));
        put_snapshot_object(&system_object_store, &tenant, "snap-c", Some("snap-b")).await;
        clocked_store.set_time(build_instant(60, 0));
        // Another pass deletes snap-a between this pass's listing and its lineage read.
        clocked_store.report_next_get_as_missing(system_object_store.snapshot_path(&tenant, "snap-a"));

        let collected_count = collect_superseded_snapshots(&system_object_store, &tenant, "snap-c", RETENTION)
            .await
            .expect("the pass succeeds");

        // snap-a is skipped and not counted; snap-b is chosen through snap-c's lineage.
        assert_eq!(collected_count, 1);
        assert!(!has_snapshot_object(&system_object_store, &tenant, "snap-a").await);
        assert!(!has_snapshot_object(&system_object_store, &tenant, "snap-b").await);
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-c").await);
    }

    #[tokio::test]
    async fn a_failed_store_clock_probe_deletes_nothing() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_eligible_snapshot_pair(&clocked_store, &system_object_store, &tenant).await;
        clocked_store.refuse_puts_to(build_probe_path(&system_object_store, &tenant));

        let error = collect_superseded_snapshots(&system_object_store, &tenant, "snap-b", RETENTION)
            .await
            .expect_err("the pass fails");

        assert_internal_error(error).await;
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-a").await);
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-b").await);
    }

    /// The probe was written, but its `Last-Modified` cannot be read back: there is no store
    /// clock to measure ages with.
    #[tokio::test]
    async fn an_unreadable_clock_probe_deletes_nothing() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_eligible_snapshot_pair(&clocked_store, &system_object_store, &tenant).await;
        clocked_store.report_next_get_as_missing(build_probe_path(&system_object_store, &tenant));

        let error = collect_superseded_snapshots(&system_object_store, &tenant, "snap-b", RETENTION)
            .await
            .expect_err("the pass fails");

        assert_internal_error(error).await;
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-a").await);
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-b").await);
    }

    /// Without the pointer the pass cannot tell which object is current now, so it keeps all.
    #[tokio::test]
    async fn an_unreadable_pointer_deletes_nothing() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_eligible_snapshot_pair(&clocked_store, &system_object_store, &tenant).await;
        system_object_store
            .object_store()
            .put(
                &system_object_store.pointer_path(&tenant),
                PutPayload::from_static(b"not json"),
            )
            .await
            .expect("a corrupt pointer");

        let error = collect_superseded_snapshots(&system_object_store, &tenant, "snap-b", RETENTION)
            .await
            .expect_err("the pass fails");

        assert_internal_error(error).await;
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-a").await);
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-b").await);
    }

    #[tokio::test]
    async fn an_unlistable_tenant_directory_deletes_nothing() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_eligible_snapshot_pair(&clocked_store, &system_object_store, &tenant).await;
        clocked_store.refuse_listings_of(system_object_store.tenant_path(&tenant));

        let error = collect_superseded_snapshots(&system_object_store, &tenant, "snap-b", RETENTION)
            .await
            .expect_err("the pass fails");

        assert_internal_error(error).await;
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-a").await);
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-b").await);
    }

    /// The bytes are no lineage frame, so whether `snap-corrupt` names `snap-a` (and keeps it) is
    /// unknown: skipping it could drop the newest successor of an object a scroll still reads.
    #[tokio::test]
    async fn an_undecodable_lineage_frame_deletes_nothing() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_eligible_snapshot_pair(&clocked_store, &system_object_store, &tenant).await;
        system_object_store
            .object_store()
            .put(
                &system_object_store.snapshot_path(&tenant, "snap-corrupt"),
                PutPayload::from_static(b"these bytes are not a snapshot object"),
            )
            .await
            .expect("a corrupt snapshot object");

        let error = collect_superseded_snapshots(&system_object_store, &tenant, "snap-b", RETENTION)
            .await
            .expect_err("the pass fails");

        assert_internal_error(error).await;
        for name in ["snap-a", "snap-b", "snap-corrupt"] {
            assert!(has_snapshot_object(&system_object_store, &tenant, name).await, "{name}");
        }
    }

    #[tokio::test]
    async fn a_refused_delete_fails_the_pass_and_keeps_the_object() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_eligible_snapshot_pair(&clocked_store, &system_object_store, &tenant).await;
        clocked_store.refuse_deletes_of(system_object_store.snapshot_path(&tenant, "snap-a"));

        let error = collect_superseded_snapshots(&system_object_store, &tenant, "snap-b", RETENTION)
            .await
            .expect_err("the pass fails");

        assert_internal_error(error).await;
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-a").await);
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-b").await);
    }

    // Which pass deletes which object is a scheduling outcome and is not asserted: both passes
    // succeed, together they collect both superseded objects, and the current one stays.
    #[tokio::test]
    async fn two_concurrent_collectors_both_succeed() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_snapshot_object(&system_object_store, &tenant, "snap-a", None).await;
        clocked_store.set_time(build_instant(1, 0));
        put_snapshot_object(&system_object_store, &tenant, "snap-b", Some("snap-a")).await;
        clocked_store.set_time(build_instant(2, 0));
        put_snapshot_object(&system_object_store, &tenant, "snap-c", Some("snap-b")).await;
        clocked_store.set_time(build_instant(60, 0));

        let (first_outcome, second_outcome) = tokio::join!(
            collect_superseded_snapshots(&system_object_store, &tenant, "snap-c", RETENTION),
            collect_superseded_snapshots(&system_object_store, &tenant, "snap-c", RETENTION),
        );

        let first_count = first_outcome.expect("the first pass succeeds");
        let second_count = second_outcome.expect("the second pass succeeds");
        assert!(first_count + second_count >= 2, "{first_count} + {second_count}");
        assert!(first_count <= 2 && second_count <= 2, "{first_count}, {second_count}");
        assert!(!has_snapshot_object(&system_object_store, &tenant, "snap-a").await);
        assert!(!has_snapshot_object(&system_object_store, &tenant, "snap-b").await);
        assert!(has_snapshot_object(&system_object_store, &tenant, "snap-c").await);
    }

    // `tenant-ab` starts with the id `tenant-a`: a listing that matched characters instead of whole
    // path segments would reach its objects.
    #[tokio::test]
    async fn a_pass_for_one_tenant_leaves_another_tenants_superseded_objects() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant_a = build_tenant("tenant-a");
        let tenant_ab = build_tenant("tenant-ab");
        put_snapshot_object(&system_object_store, &tenant_a, "snap-a1", None).await;
        put_snapshot_object(&system_object_store, &tenant_ab, "snap-ab1", None).await;
        clocked_store.set_time(build_instant(1, 0));
        put_snapshot_object(&system_object_store, &tenant_a, "snap-a2", Some("snap-a1")).await;
        put_snapshot_object(&system_object_store, &tenant_ab, "snap-ab2", Some("snap-ab1")).await;
        clocked_store.set_time(build_instant(60, 0));

        let collected_count = collect_superseded_snapshots(&system_object_store, &tenant_a, "snap-a2", RETENTION)
            .await
            .expect("the pass succeeds");

        assert_eq!(collected_count, 1);
        assert!(!has_snapshot_object(&system_object_store, &tenant_a, "snap-a1").await);
        assert!(has_snapshot_object(&system_object_store, &tenant_a, "snap-a2").await);
        assert!(has_snapshot_object(&system_object_store, &tenant_ab, "snap-ab1").await);
        assert!(has_snapshot_object(&system_object_store, &tenant_ab, "snap-ab2").await);
    }

    #[tokio::test]
    async fn a_replica_with_a_cached_snapshot_never_loses_another_replicas_write_across_gc() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let replica_a = build_index_store(build_clocked_system_object_store(&clocked_store));
        let replica_b = build_index_store(build_clocked_system_object_store(&clocked_store));
        let tenant = build_tenant("tenant-a");
        replica_a
            .write(&tenant, build_index_creation(".kibana_1"))
            .await
            .expect("A creates the index");
        let name_read_by_a = read_current_snapshot_name(&replica_a, &tenant).await;
        clocked_store.set_time(build_instant(1, 0));
        replica_b
            .write(&tenant, build_indexing_operation(".kibana_1", "written-by-b"))
            .await
            .expect("B writes");
        let current_name = read_current_snapshot_name(&replica_b, &tenant).await;
        clocked_store.set_time(build_instant(60, 0));
        let observer = build_clocked_system_object_store(&clocked_store);
        let collected_count = collect_superseded_snapshots(&observer, &tenant, &current_name, RETENTION)
            .await
            .expect("the pass succeeds");
        assert_eq!(collected_count, 1, "the snapshot object replica A cached is collected");
        assert!(!has_snapshot_object(&observer, &tenant, &name_read_by_a).await);

        // A's cache still names the collected snapshot object; its write must start from B's.
        replica_a
            .write(&tenant, build_indexing_operation(".kibana_1", "written-by-a"))
            .await
            .expect("A writes after the pass");

        let snapshot_after = replica_b.read(&tenant).await.expect("read").snapshot;
        let index = snapshot_after.find_index(".kibana_1").expect("the index exists");
        assert!(index.find_document("written-by-b").is_some(), "B's write survives");
        assert!(index.find_document("written-by-a").is_some(), "A's write lands");
    }

    #[tokio::test]
    async fn a_pass_given_a_stale_current_keeps_the_object_the_pointer_names_now() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let index_store = build_index_store(system_object_store.clone());
        let tenant = build_tenant("tenant-a");
        index_store
            .write(&tenant, build_index_creation(".kibana_1"))
            .await
            .expect("create");
        // The pass was scheduled while the pointer named `stale_name`.
        let stale_name = read_current_snapshot_name(&index_store, &tenant).await;
        clocked_store.set_time(build_instant(1, 0));
        index_store
            .write(&tenant, build_indexing_operation(".kibana_1", "a"))
            .await
            .expect("a successor");
        let live_name = read_current_snapshot_name(&index_store, &tenant).await;
        // A failed swap's object, whose cleanup delete was lost, names the live object.
        clocked_store.set_time(build_instant(2, 0));
        put_snapshot_object(&system_object_store, &tenant, "snap-orphan", Some(live_name.as_str())).await;
        clocked_store.set_time(build_instant(2, 931));

        let collected_count = collect_superseded_snapshots(&system_object_store, &tenant, &stale_name, RETENTION)
            .await
            .expect("the pass succeeds");

        assert_eq!(
            collected_count, 0,
            "`stale_name` is protected as the given current one, `live_name` by the pointer"
        );
        assert!(has_snapshot_object(&system_object_store, &tenant, &live_name).await);
        assert!(has_snapshot_object(&system_object_store, &tenant, &stale_name).await);
        let snapshot = index_store.read(&tenant).await.expect("the live object still reads").snapshot;
        let index = snapshot.find_index(".kibana_1").expect("the index exists");
        assert!(index.find_document("a").is_some());
    }

    /// A delayed pass given the stale `snap-q` lists while the pointer names `snap-p`; while it
    /// reads lineage frames, a writer stores `snap-r` (unlisted) and swaps the pointer to it.
    /// `snap-p`'s only listed successor is an old orphan, so only the pointer read made before the
    /// listing shows that `snap-p` was current moments ago: a scroll opened on it still reads it.
    #[tokio::test]
    async fn a_pointer_swapped_during_the_lineage_reads_keeps_the_object_it_named_at_the_listing() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let tenant = build_tenant("tenant-a");
        put_snapshot_object(&system_object_store, &tenant, "snap-q", None).await;
        clocked_store.set_time(build_instant(1, 0));
        put_snapshot_object(&system_object_store, &tenant, "snap-p", Some("snap-q")).await;
        let pointer_path = system_object_store.pointer_path(&tenant);
        system_object_store
            .object_store()
            .put(&pointer_path, encode_pointer("snap-p"))
            .await
            .expect("the pointer names snap-p");
        // A failed swap's object, whose cleanup delete was lost, names snap-p.
        clocked_store.set_time(build_instant(2, 0));
        put_snapshot_object(&system_object_store, &tenant, "snap-orphan", Some("snap-p")).await;
        clocked_store.set_time(build_instant(2, 931));
        let lineage_read_path = system_object_store.snapshot_path(&tenant, "snap-q");
        clocked_store.put_on_next_get(
            lineage_read_path.clone(),
            system_object_store.snapshot_path(&tenant, "snap-r"),
            encode_empty_snapshot_object(Some("snap-p")),
        );
        clocked_store.put_on_next_get(lineage_read_path, pointer_path, encode_pointer("snap-r"));

        let collected_count = collect_superseded_snapshots(&system_object_store, &tenant, "snap-q", RETENTION)
            .await
            .expect("the pass succeeds");

        let (_, pointer_target) = read_tenant_pointer(&system_object_store, &tenant)
            .await
            .expect("the pointer reads")
            .expect("a pointer exists");
        assert_eq!(pointer_target, "snap-r", "the writer swapped during the pass");
        assert_eq!(collected_count, 0);
        for name in ["snap-q", "snap-p", "snap-orphan", "snap-r"] {
            assert!(has_snapshot_object(&system_object_store, &tenant, name).await, "{name}");
        }
    }

    fn read_hit_ids(page: &Value) -> Vec<String> {
        page["hits"]["hits"]
            .as_array()
            .expect("hits")
            .iter()
            .map(|hit| hit["_id"].as_str().expect("_id").to_owned())
            .collect()
    }

    /// Opens a scroll over `index` of the tenant's current snapshot at `opened_at`, as the initial
    /// search does, with `size` documents a page: the `_id`s of the first page and the scroll id of
    /// the next.
    async fn open_scroll(
        index_store: &SystemIndexStore,
        tenant: &TenantId,
        index: &str,
        size: usize,
        opened_at: DateTime<Utc>,
    ) -> (Vec<String>, String) {
        let current_snapshot = index_store.read(tenant).await.expect("read");
        let parameters = parse_parameters::<SearchParameters>(Some(SCROLL_QUERY_STRING)).expect("parameters");
        let body = json!({"size": size});
        let request = parse_search_value(&body, &parameters).expect("a search request");
        let opening = ScrollOpening {
            lifetime: parameters.scroll.as_deref().expect("a keep-alive"),
            maximum_lifetime: MAX_SCROLL_LIFETIME,
            query_string: SCROLL_QUERY_STRING,
            body,
        };
        let context = RequestContext {
            tenant: tenant.clone(),
            now: opened_at,
        };
        let page = open_system_scroll(&current_snapshot, &[index.to_owned()], &request, opening, &context)
            .expect("the scroll opens");
        let scroll_id = page["_scroll_id"].as_str().expect("a scroll id").to_owned();
        (read_hit_ids(&page), scroll_id)
    }

    /// Continues the scroll `scroll_id` at `now` with the keep-alive `requested_lifetime`, as a
    /// continuation does: the cursor is validated against its fixed expiry, its snapshot object is
    /// read by name, and the page is read from it. The `_id`s of the page and the scroll id of the
    /// next.
    async fn continue_scroll_at(
        index_store: &SystemIndexStore,
        tenant: &TenantId,
        scroll_id: &str,
        requested_lifetime: &str,
        now: DateTime<Utc>,
    ) -> (Vec<String>, String) {
        let cursor = resume_scroll(scroll_id, Some(requested_lifetime), MAX_SCROLL_LIFETIME, now)
            .expect("the scroll has not expired");
        let snapshot = index_store
            .read_snapshot_by_name(tenant, &cursor.snapshot_name)
            .await
            .expect("read")
            .expect("the scrolled snapshot object still exists");
        let page_ids = read_snapshot_page(&snapshot, &cursor, tenant);
        let next_scroll_id = cursor.advance(page_ids.len()).encode().expect("encodes");
        (page_ids, next_scroll_id)
    }

    /// The `_id`s of the page of `snapshot` at `cursor`: the scroll's own request parsed again,
    /// from the cursor's offset, evaluated at the opening instant as every page of a scroll is.
    fn read_snapshot_page(snapshot: &SystemSnapshot, cursor: &ScrollCursor, tenant: &TenantId) -> Vec<String> {
        let parameters = parse_parameters::<SearchParameters>(Some(&cursor.query_string)).expect("parameters");
        let mut request = parse_search_value(&cursor.body, &parameters).expect("a search request");
        request.from = cursor.offset;
        let context = RequestContext {
            tenant: tenant.clone(),
            now: DateTime::from_timestamp_millis(cursor.opened_at_ms).expect("an opening instant"),
        };
        let page = search_system_indices(snapshot, slice::from_ref(&cursor.index), &request, &context).expect("a page");
        read_hit_ids(&page)
    }

    fn build_document_ids(count: usize) -> Vec<String> {
        (0..count).map(|number| format!("doc-{number:03}")).collect()
    }

    #[tokio::test]
    async fn scrolling_kibana_1_while_bulking_into_kibana_2_with_gc_between_batches_returns_every_document_once() {
        // Seeded an hour before the scroll opens, so the seed-era objects are collectable while
        // the scroll runs.
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(-60, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let index_store = build_index_store(system_object_store.clone());
        let tenant = build_tenant("tenant-a");
        index_store
            .write(&tenant, build_index_creation(".kibana_1"))
            .await
            .expect("create .kibana_1");
        for id in build_document_ids(250) {
            index_store
                .write(&tenant, build_indexing_operation(".kibana_1", &id))
                .await
                .expect("seed");
        }
        clocked_store.set_time(build_instant(0, 0));
        index_store
            .write(&tenant, build_index_creation(".kibana_2"))
            .await
            .expect("create .kibana_2");
        let scrolled_name = read_current_snapshot_name(&index_store, &tenant).await;

        let mut returned_ids = Vec::new();
        let mut collected_count = 0;
        let mut minute = 0;
        let (mut page_ids, mut scroll_id) =
            open_scroll(&index_store, &tenant, ".kibana_1", 100, build_instant(minute, 0)).await;
        while !page_ids.is_empty() {
            // Copy the page into `.kibana_2` with one `_bulk`, then let a minute pass, collect, and
            // continue the scroll.
            let items = page_ids
                .iter()
                .map(|id| BulkItem::Write(build_document_write(".kibana_2", id)))
                .collect();
            index_store
                .write(
                    &tenant,
                    SystemOperation::Bulk {
                        items,
                        returned_sources: None,
                    },
                )
                .await
                .expect("copy");
            returned_ids.extend(page_ids);
            minute += 1;
            clocked_store.set_time(build_instant(minute, 0));
            let current_name = read_current_snapshot_name(&index_store, &tenant).await;
            collected_count += collect_superseded_snapshots(&system_object_store, &tenant, &current_name, RETENTION)
                .await
                .expect("the pass succeeds");
            (page_ids, scroll_id) =
                continue_scroll_at(&index_store, &tenant, &scroll_id, "15m", build_instant(minute, 0)).await;
        }

        assert_eq!(
            returned_ids,
            build_document_ids(250),
            "every document once, in _id order"
        );
        // The seed's create and 250 writes left 251 objects. The first 250 were superseded at
        // minute -60 and are collected; the last one was superseded by the scrolled object at
        // minute 0 and stays, as does every object written since.
        assert_eq!(
            collected_count, 250,
            "the passes between batches collect the seed-era objects"
        );
        assert!(has_snapshot_object(&system_object_store, &tenant, &scrolled_name).await);
        let copied_snapshot = index_store.read(&tenant).await.expect("read").snapshot;
        let target = copied_snapshot.find_index(".kibana_2").expect(".kibana_2");
        assert!(build_document_ids(250).iter().all(|id| target.find_document(id).is_some()));
    }

    #[tokio::test]
    async fn a_scroll_continued_up_to_its_fixed_expiry_while_successors_are_written_reads_every_page() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let index_store = build_index_store(system_object_store.clone());
        let tenant = build_tenant("tenant-a");
        index_store
            .write(&tenant, build_index_creation(".kibana_1"))
            .await
            .expect("create");
        for id in build_document_ids(16) {
            index_store
                .write(&tenant, build_indexing_operation(".kibana_1", &id))
                .await
                .expect("seed");
        }
        let scrolled_name = read_current_snapshot_name(&index_store, &tenant).await;

        // Opened at minute 0 with `scroll=15m`, one document a page: each later minute continues
        // it, the last continuation at the expiry itself. Each continuation asks for `1m` except
        // the one at minute 14, which asks for `15m`; neither moves the expiry the opening fixed.
        // Every minute another write supersedes the current snapshot object and a pass collects
        // with the store clock at that minute.
        let (mut returned_ids, mut scroll_id) =
            open_scroll(&index_store, &tenant, ".kibana_1", 1, build_instant(0, 0)).await;
        assert_eq!(returned_ids.len(), 1, "minute 0");
        for minute in 0..=15 {
            if minute > 0 {
                clocked_store.set_time(build_instant(minute, 0));
                let requested_lifetime = if minute == 14 { "15m" } else { "1m" };
                let (page_ids, next_scroll_id) = continue_scroll_at(
                    &index_store,
                    &tenant,
                    &scroll_id,
                    requested_lifetime,
                    build_instant(minute, 0),
                )
                .await;
                assert_eq!(page_ids.len(), 1, "minute {minute}");
                returned_ids.extend(page_ids);
                scroll_id = next_scroll_id;
            }
            index_store
                .write(
                    &tenant,
                    build_indexing_operation(".kibana_1", &format!("later-{minute:02}")),
                )
                .await
                .expect("a successor");
            let current_name = read_current_snapshot_name(&index_store, &tenant).await;
            collect_superseded_snapshots(&system_object_store, &tenant, &current_name, RETENTION)
                .await
                .expect("the pass succeeds");
        }

        assert_eq!(returned_ids, build_document_ids(16));
        // The `15m` asked for at minute 14 did not extend the scroll: a millisecond past minute 15
        // it is gone.
        let expired_at = build_instant(15, 0) + TimeDelta::milliseconds(1);
        let error =
            resume_scroll(&scroll_id, Some("15m"), MAX_SCROLL_LIFETIME, expired_at).expect_err("the scroll expired");
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["error"]["type"], "search_context_missing_exception", "{body}");
        // The scrolled snapshot object's first successor was written at minute 0, so it outlives
        // the expiry by the query deadline: collected only once that successor is older than the
        // retention.
        clocked_store.set_time(build_instant(15, 30));
        let current_name = read_current_snapshot_name(&index_store, &tenant).await;
        collect_superseded_snapshots(&system_object_store, &tenant, &current_name, RETENTION)
            .await
            .expect("pass at the boundary");
        assert!(has_snapshot_object(&system_object_store, &tenant, &scrolled_name).await);
        clocked_store.set_time(build_instant(15, 31));
        collect_superseded_snapshots(&system_object_store, &tenant, &current_name, RETENTION)
            .await
            .expect("pass past the boundary");
        assert!(!has_snapshot_object(&system_object_store, &tenant, &scrolled_name).await);
    }

    /// The write loop starts a tenant's pass after a successful swap once that tenant's previous
    /// pass on the replica, or the store's creation, is at least the scroll lifetime old. Tokio's
    /// paused clock is the replica's clock; the double's clock is the store's.
    #[tokio::test(start_paused = true)]
    async fn the_write_loop_starts_a_tenants_pass_once_the_scroll_lifetime_has_passed_since_its_last_one() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let index_store = build_index_store(system_object_store.clone());
        let tenant_a = build_tenant("tenant-a");
        let tenant_b = build_tenant("tenant-b");
        for tenant in [&tenant_a, &tenant_b] {
            index_store
                .write(tenant, build_index_creation(".kibana_1"))
                .await
                .expect("create");
        }
        let first_name_a = read_current_snapshot_name(&index_store, &tenant_a).await;
        let first_name_b = read_current_snapshot_name(&index_store, &tenant_b).await;
        clocked_store.set_time(build_instant(1, 0));
        for tenant in [&tenant_a, &tenant_b] {
            index_store
                .write(tenant, build_indexing_operation(".kibana_1", "a"))
                .await
                .expect("second");
        }
        let second_name_a = read_current_snapshot_name(&index_store, &tenant_a).await;
        // On the store's clock every first object is collectable from here on: its successor was
        // written 29 minutes ago.
        clocked_store.set_time(build_instant(30, 0));

        tokio::time::advance(Duration::from_mins(1)).await;
        index_store
            .write(&tenant_a, build_indexing_operation(".kibana_1", "b"))
            .await
            .expect("third");
        yield_to_background_passes().await;
        assert_eq!(
            read_probe_time(&system_object_store, &tenant_a).await,
            None,
            "no pass before the scroll lifetime has passed since the store started"
        );
        assert!(has_snapshot_object(&system_object_store, &tenant_a, &first_name_a).await);

        // 16 minutes after the store started: tenant-a's first pass.
        tokio::time::advance(Duration::from_mins(15)).await;
        index_store
            .write(&tenant_a, build_indexing_operation(".kibana_1", "c"))
            .await
            .expect("fourth");
        wait_for_deletion(
            &system_object_store,
            &system_object_store.snapshot_path(&tenant_a, &first_name_a),
        )
        .await;

        // Tenant-a's pass a minute ago does not hold back tenant-b's first one.
        tokio::time::advance(Duration::from_mins(1)).await;
        index_store
            .write(&tenant_b, build_indexing_operation(".kibana_1", "b"))
            .await
            .expect("tenant-b's third");
        wait_for_deletion(
            &system_object_store,
            &system_object_store.snapshot_path(&tenant_b, &first_name_b),
        )
        .await;

        // Tenant-a's second object becomes collectable on the store's clock (its successor was
        // written 20 minutes ago), but tenant-a's next pass waits the scroll lifetime from its
        // last one: none 14 minutes 59 seconds after it.
        clocked_store.set_time(build_instant(50, 0));
        tokio::time::advance(Duration::from_mins(14) - Duration::from_secs(1)).await;
        index_store
            .write(&tenant_a, build_indexing_operation(".kibana_1", "d"))
            .await
            .expect("fifth");
        yield_to_background_passes().await;
        assert_eq!(
            read_probe_time(&system_object_store, &tenant_a).await,
            Some(build_instant(30, 0)),
            "the probe still carries the first pass's store time"
        );
        assert!(has_snapshot_object(&system_object_store, &tenant_a, &second_name_a).await);

        // Exactly the scroll lifetime after tenant-a's last pass.
        tokio::time::advance(Duration::from_secs(1)).await;
        index_store
            .write(&tenant_a, build_indexing_operation(".kibana_1", "e"))
            .await
            .expect("sixth");
        wait_for_deletion(
            &system_object_store,
            &system_object_store.snapshot_path(&tenant_a, &second_name_a),
        )
        .await;
    }

    /// The retention a pass the write loop starts applies is the scroll lifetime plus the write
    /// deadline ([`RETENTION`] with `build_index_store`'s limits): a successor exactly that old
    /// keeps its predecessor, one second older lets the next due pass collect it.
    #[tokio::test(start_paused = true)]
    async fn the_write_loop_retains_a_superseded_snapshot_object_for_the_scroll_lifetime_plus_the_write_deadline() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let index_store = build_index_store(system_object_store.clone());
        let tenant = build_tenant("tenant-a");
        index_store
            .write(&tenant, build_index_creation(".kibana_1"))
            .await
            .expect("create");
        let first_snapshot_name = read_current_snapshot_name(&index_store, &tenant).await;
        clocked_store.set_time(build_instant(1, 0));
        index_store
            .write(&tenant, build_indexing_operation(".kibana_1", "a"))
            .await
            .expect("the successor");

        clocked_store.set_time(build_instant(1, 930));
        tokio::time::advance(Duration::from_mins(15)).await;
        index_store
            .write(&tenant, build_indexing_operation(".kibana_1", "b"))
            .await
            .expect("third");
        wait_for_probe_time(&system_object_store, &tenant, build_instant(1, 930)).await;
        yield_to_background_passes().await;
        assert!(
            has_snapshot_object(&system_object_store, &tenant, &first_snapshot_name).await,
            "a successor 930 s old keeps its predecessor"
        );

        clocked_store.set_time(build_instant(1, 931));
        tokio::time::advance(Duration::from_mins(15)).await;
        index_store
            .write(&tenant, build_indexing_operation(".kibana_1", "c"))
            .await
            .expect("fourth");
        wait_for_deletion(
            &system_object_store,
            &system_object_store.snapshot_path(&tenant, &first_snapshot_name),
        )
        .await;
    }

    /// The retention of a pass the write loop starts is the scroll lifetime plus the write
    /// deadline ([`RETENTION`] with `build_index_store`'s limits), pinned at its boundary within
    /// one pass: the predecessor of a successor written one second more than the retention
    /// before the pass is collected, the predecessor of one written exactly the retention before
    /// it stays.
    #[tokio::test(start_paused = true)]
    async fn a_due_pass_from_the_write_loop_collects_only_the_object_superseded_more_than_the_retention_ago() {
        let clocked_store = Arc::new(ClockedObjectStore::new(build_instant(0, 0)));
        let system_object_store = build_clocked_system_object_store(&clocked_store);
        let index_store = build_index_store(system_object_store.clone());
        let tenant = build_tenant("tenant-a");
        index_store
            .write(&tenant, build_index_creation(".kibana_1"))
            .await
            .expect("create");
        let first_snapshot_name = read_current_snapshot_name(&index_store, &tenant).await;
        clocked_store.set_time(build_instant(1, 0));
        index_store
            .write(&tenant, build_indexing_operation(".kibana_1", "a"))
            .await
            .expect("the first object's successor");
        let second_snapshot_name = read_current_snapshot_name(&index_store, &tenant).await;
        clocked_store.set_time(build_instant(1, 1));
        index_store
            .write(&tenant, build_indexing_operation(".kibana_1", "b"))
            .await
            .expect("the second object's successor");

        // The first due pass, on the store clock 931 s after the second object was written and
        // exactly 930 s after the third.
        clocked_store.set_time(build_instant(1, 931));
        tokio::time::advance(MAX_SCROLL_LIFETIME).await;
        index_store
            .write(&tenant, build_indexing_operation(".kibana_1", "c"))
            .await
            .expect("the write that starts the pass");
        wait_for_deletion(
            &system_object_store,
            &system_object_store.snapshot_path(&tenant, &first_snapshot_name),
        )
        .await;
        yield_to_background_passes().await;

        assert!(
            has_snapshot_object(&system_object_store, &tenant, &second_snapshot_name).await,
            "a successor exactly 930 s old keeps its predecessor"
        );
    }
}
