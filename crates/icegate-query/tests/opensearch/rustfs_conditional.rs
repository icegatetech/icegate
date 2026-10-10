//! The object-store behaviours the system-index store depends on, against real RustFS:
//! create-if-absent, compare-and-swap on `ETag` (sequentially and under contention), conditional
//! GET in both arms (not modified for the current `ETag`, the changed object for an outdated one),
//! and a last-modified time on objects written by plain put, create, and update (garbage collection
//! measures supersession age from it).
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::{TimeDelta, Utc};
use icegate_common::testing::{S3TestContainer, create_s3_bucket, create_s3_object_store};
use object_store::{GetOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions, UpdateVersion, path::Path};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const BUCKET: &str = "opensearch-conditional";

/// Number of writers released at once in the compare-and-swap race.
const CONTENDERS: usize = 8;

/// Upper bound for the whole race, so a hung request fails the test instead of the run.
const RACE_TIMEOUT: Duration = Duration::from_secs(30);

/// Allowance for container or VM clock skew and the whole-second granularity of `Last-Modified`.
const CLOCK_TOLERANCE: TimeDelta = TimeDelta::minutes(5);

/// Starts a RustFS container with an empty bucket; the container guard must outlive the store.
async fn start_store() -> (S3TestContainer, Arc<dyn ObjectStore>) {
    let container = S3TestContainer::start().await.expect("rustfs container starts");
    create_s3_bucket(container.endpoint(), BUCKET).await.expect("bucket created");
    let store = create_s3_object_store(container.endpoint(), BUCKET).expect("store built");
    (container, store)
}

/// Builds put options that apply only while the object's current `ETag` equals `etag`.
fn build_etag_match_options(etag: &str) -> PutOptions {
    PutOptions {
        mode: PutMode::Update(UpdateVersion {
            e_tag: Some(etag.to_string()),
            version: None,
        }),
        ..PutOptions::default()
    }
}

#[tokio::test]
async fn create_if_absent_refuses_an_existing_pointer_and_keeps_its_content() {
    let (_container, store) = start_store().await;
    let pointer = Path::from("tenant-a/current");

    store
        .put_opts(&pointer, Bytes::from_static(b"snap-1").into(), PutMode::Create.into())
        .await
        .expect("first create succeeds");
    let second = store
        .put_opts(&pointer, Bytes::from_static(b"snap-2").into(), PutMode::Create.into())
        .await;

    assert!(
        matches!(second, Err(object_store::Error::AlreadyExists { .. })),
        "{second:?}"
    );
    let current = store.get(&pointer).await.expect("get").bytes().await.expect("bytes");
    assert_eq!(current, Bytes::from_static(b"snap-1"));
}

#[tokio::test]
async fn update_with_a_stale_etag_is_a_precondition_failure_and_keeps_the_winner() {
    let (_container, store) = start_store().await;
    let pointer = Path::from("tenant-a/current");
    let first_etag = store
        .put_opts(&pointer, Bytes::from_static(b"snap-1").into(), PutMode::Create.into())
        .await
        .expect("create")
        .e_tag
        .expect("RustFS returns an ETag on create");
    store
        .put_opts(
            &pointer,
            Bytes::from_static(b"snap-2").into(),
            build_etag_match_options(&first_etag),
        )
        .await
        .expect("update with the current etag succeeds");

    let stale = store
        .put_opts(
            &pointer,
            Bytes::from_static(b"snap-3").into(),
            build_etag_match_options(&first_etag),
        )
        .await;

    assert!(
        matches!(stale, Err(object_store::Error::Precondition { .. })),
        "{stale:?}"
    );
    let current = store.get(&pointer).await.expect("get").bytes().await.expect("bytes");
    assert_eq!(current, Bytes::from_static(b"snap-2"));
}

// Which writer wins is a scheduling outcome and is deliberately not asserted; the contract is that
// exactly one does and that the stored object is that writer's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_updates_with_the_same_etag_admit_one_winner_and_store_its_content() {
    let (_container, store) = start_store().await;
    let pointer = Path::from("tenant-a/current");
    let initial_etag = store
        .put_opts(&pointer, Bytes::from_static(b"snap-0").into(), PutMode::Create.into())
        .await
        .expect("create")
        .e_tag
        .expect("RustFS returns an ETag on create");
    let barrier = Arc::new(Barrier::new(CONTENDERS));

    let mut writers = JoinSet::new();
    for writer in 0..CONTENDERS {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let pointer = pointer.clone();
        let options = build_etag_match_options(&initial_etag);
        writers.spawn(async move {
            let body = Bytes::from(format!("snap-writer-{writer}"));
            barrier.wait().await;
            let outcome = store.put_opts(&pointer, body.clone().into(), options).await;
            (body, outcome)
        });
    }
    let outcomes = tokio::time::timeout(RACE_TIMEOUT, async {
        let mut outcomes = Vec::with_capacity(CONTENDERS);
        while let Some(joined) = writers.join_next().await {
            outcomes.push(joined.expect("writer task completes without panicking"));
        }
        outcomes
    })
    .await
    .expect("every writer finishes within the bound");

    assert_eq!(outcomes.len(), CONTENDERS);
    let (winners, losers): (Vec<_>, Vec<_>) = outcomes.into_iter().partition(|(_, outcome)| outcome.is_ok());
    assert_eq!(winners.len(), 1, "exactly one writer wins; losers: {losers:?}");
    for (_, outcome) in &losers {
        assert!(
            matches!(outcome, Err(object_store::Error::Precondition { .. })),
            "{outcome:?}"
        );
    }
    let (winner_body, winner_outcome) = winners.into_iter().next().expect("one winner");
    let winner_etag = winner_outcome
        .expect("the winner succeeded")
        .e_tag
        .expect("RustFS returns an ETag on update");
    assert_ne!(winner_etag, initial_etag);
    let stored = store.get(&pointer).await.expect("get");
    assert_eq!(stored.meta.e_tag.as_deref(), Some(winner_etag.as_str()));
    assert_eq!(stored.bytes().await.expect("bytes"), winner_body);
}

#[tokio::test]
async fn conditional_get_with_the_current_etag_answers_not_modified() {
    let (_container, store) = start_store().await;
    let pointer = Path::from("tenant-a/current");
    let etag = store
        .put_opts(&pointer, Bytes::from_static(b"snap-1").into(), PutMode::Create.into())
        .await
        .expect("create")
        .e_tag
        .expect("etag");

    let unchanged = store
        .get_opts(
            &pointer,
            GetOptions {
                if_none_match: Some(etag),
                ..GetOptions::default()
            },
        )
        .await;

    assert!(
        matches!(unchanged, Err(object_store::Error::NotModified { .. })),
        "{unchanged:?}"
    );
}

#[tokio::test]
async fn conditional_get_with_an_outdated_etag_returns_the_updated_object_and_its_new_etag() {
    let (_container, store) = start_store().await;
    let pointer = Path::from("tenant-a/current");
    let outdated_etag = store
        .put_opts(&pointer, Bytes::from_static(b"snap-1").into(), PutMode::Create.into())
        .await
        .expect("create")
        .e_tag
        .expect("etag");
    let updated_etag = store
        .put_opts(
            &pointer,
            Bytes::from_static(b"snap-2").into(),
            build_etag_match_options(&outdated_etag),
        )
        .await
        .expect("update with the current etag succeeds")
        .e_tag
        .expect("RustFS returns an ETag on update");
    assert_ne!(updated_etag, outdated_etag);

    let changed = store
        .get_opts(
            &pointer,
            GetOptions {
                if_none_match: Some(outdated_etag),
                ..GetOptions::default()
            },
        )
        .await
        .expect("an outdated etag does not answer not-modified");

    assert_eq!(changed.meta.e_tag.as_deref(), Some(updated_etag.as_str()));
    assert_eq!(changed.bytes().await.expect("bytes"), Bytes::from_static(b"snap-2"));
}

// The real clock is the contract here: garbage collection ages a superseded body by this timestamp.
#[tokio::test]
async fn plain_put_create_and_update_writes_each_carry_a_last_modified_near_the_local_clock() {
    let (_container, store) = start_store().await;
    let plain = Path::from("tenant-a/plain");
    let created = Path::from("tenant-a/created");
    let updated = Path::from("tenant-a/updated");
    let before = Utc::now();

    store.put(&plain, Bytes::from_static(b"{}").into()).await.expect("put");
    store
        .put_opts(&created, Bytes::from_static(b"{}").into(), PutMode::Create.into())
        .await
        .expect("create");
    let etag = store
        .put_opts(&updated, Bytes::from_static(b"{}").into(), PutMode::Create.into())
        .await
        .expect("create before update")
        .e_tag
        .expect("etag");
    store
        .put_opts(
            &updated,
            Bytes::from_static(b"{\"v\":2}").into(),
            build_etag_match_options(&etag),
        )
        .await
        .expect("update");
    let after = Utc::now();

    for path in [&plain, &created, &updated] {
        let last_modified = store.head(path).await.expect("head").last_modified;
        assert!(
            last_modified >= before - CLOCK_TOLERANCE && last_modified <= after + CLOCK_TOLERANCE,
            "{path}: {last_modified} is outside [{before}, {after}] widened by {CLOCK_TOLERANCE:?}"
        );
    }
}
