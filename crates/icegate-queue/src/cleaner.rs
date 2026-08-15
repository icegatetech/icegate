//! Retention cleanup for WAL segments.
//!
//! Segment keys are this crate's format, so deleting one is this crate's job:
//! [`QueueCleaner`] is the entry point a retention service (in IceGate, the
//! maintain binary) drives. It decides NOTHING about safety — the bound below
//! which segments may go is the caller's, derived from what has been committed
//! downstream and how old that commit is. This module only turns that bound into
//! deletes.

use std::sync::Arc;

use futures::StreamExt;
use icegate_common::{Retrier, RetrierConfig};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    Topic,
    error::{QueueError, Result},
    segment::{parse_segment_offset, topic_prefix},
};

/// Floor under the per-call failure threshold derived from the batch size.
///
/// The threshold is one fully failed batch (see [`delete_failure_limit`]), which
/// at a concurrency of one degenerates into a single key: one object the store
/// will never delete would then abandon the topic on every cycle, which is the
/// fault this whole mechanism exists to survive.
const MIN_DELETE_FAILURE_LIMIT: u64 = 4;

/// Attempts one segment deletion gets, the first one included.
///
/// With [`delete_retrier_config`]'s delays, one key that keeps failing costs at
/// most ~0.7 s of waiting, so even a run of stuck keys cannot eat the cleanup
/// timeout the caller runs the cycle under. Nothing is lost by giving up early:
/// deletion is idempotent and the next cycle lists the same key again.
const DELETE_ATTEMPTS: usize = 4;

/// Backoff for [`DELETE_ATTEMPTS`], deliberately shorter than
/// [`RetrierConfig::default`], whose 20 attempts back off up to a minute.
fn delete_retrier_config() -> RetrierConfig {
    RetrierConfig {
        max_attempts: DELETE_ATTEMPTS,
        rand_delay: Duration::from_millis(50),
        delays: vec![
            Duration::from_millis(100),
            Duration::from_millis(100),
            Duration::from_millis(200),
        ],
    }
}

/// Consecutive segments that exhausted their retries or met a terminal error,
/// with no success in between, past which the call gives up on the topic.
///
/// A store refusing the operation as a whole — expired credentials, throttling,
/// a bucket policy — fails every key it is handed from the moment it breaks, and
/// pressing on would work through up to [`DeleteLimits::max_deletes`] remaining
/// segments, retrying each to exhaustion. A per-key fault (an object lock, a
/// corrupt entry) fails a handful
/// and lets the rest through, so it must be skipped instead: the next call lists
/// the same prefix from the same start, meets the same key first, and a call
/// that stopped there would never delete anything again. The length of the
/// UNBROKEN run is what separates the two, which is why the counter resets on
/// every success: a store that breaks halfway through a call is the same fault
/// as one that was broken when the call started, and a threshold on the call's
/// total failures would miss it.
///
/// The threshold is one batch of *segments*, not of requests, and it is checked
/// only after a whole batch has completed. So the segments a broken store fails
/// before the call gives up are the threshold ROUNDED UP to a full batch, where
/// the batch is [`DeleteLimits::concurrency`] floored at one and the threshold
/// is that batch size raised to [`MIN_DELETE_FAILURE_LIMIT`]. At a concurrency
/// below that floor the rounding leaves MORE failed segments than the
/// concurrency, not fewer — a smaller concurrency cannot stop the call any
/// sooner than a completed batch.
///
/// A refusal the store may answer differently next time — a 429 `SlowDown`, a 503,
/// a dropped connection, which is exactly how a store throttling every caller
/// fails — is retried, so each refused segment costs up to [`DELETE_ATTEMPTS`]
/// requests and the whole run costs at most that per rounded-up segment. Those
/// retries do reach an already-overloaded store, so the pauses between them are
/// deliberately short ([`delete_retrier_config`]).
///
/// Deletions inside one batch complete in whatever order the store answers in,
/// so a run is counted over completions, not over offsets. That costs nothing
/// for either fault: a broken store fails every completion whatever their order,
/// and a handful of locked keys cannot reach the limit however they interleave.
const fn delete_failure_limit(batch_size: usize) -> u64 {
    let batch_size = batch_size as u64;
    if batch_size > MIN_DELETE_FAILURE_LIMIT {
        batch_size
    } else {
        MIN_DELETE_FAILURE_LIMIT
    }
}

/// Bounds on the work a single cleanup call may do.
#[derive(Debug, Clone, Copy)]
pub struct DeleteLimits {
    /// Maximum number of segments a single call may delete. A call that meets a
    /// further candidate past it ends with [`DeleteSummary::truncated`] set and
    /// leaves the rest for the next one; a call whose candidates merely fill it
    /// exactly is complete, not truncated.
    pub max_deletes: usize,
    /// Maximum number of deletions in flight at once.
    pub concurrency: usize,
    /// When `true`, count what would be deleted and delete nothing.
    pub dry_run: bool,
}

/// Outcome of a single cleanup call for one topic.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DeleteSummary {
    /// Segments identified as being at or below the bound.
    pub found: u64,
    /// Segments deleted successfully. Zero on a dry run.
    pub deleted: u64,
    /// Segments whose deletion failed.
    pub failed: u64,
    /// Bytes freed by the successful deletions, as reported by the listing.
    pub bytes_reclaimed: u64,
    /// `true` when [`DeleteLimits::max_deletes`] cut the batch short, so
    /// segments at or below the bound remain. The caller MUST NOT read a
    /// truncated run as "the tail is gone".
    pub truncated: bool,
    /// `true` when the call gave up on the topic after the run of consecutive
    /// refused deletions [`delete_failure_limit`] allows, so segments at or
    /// below the bound remain.
    ///
    /// Carried as a flag rather than left to be inferred from the counters: once
    /// the run may start after a success, `deleted` and `failed` no longer tell
    /// an abandoned call apart from one that stepped over a few locked keys, and
    /// the caller has to know which of the two it is reporting.
    pub is_abandoned: bool,
    /// Lowest segment offset known to remain in the topic after this call: the
    /// first one above the bound, or — when the call was truncated, ran dry, or
    /// a delete failed — the lowest one it left behind. `None` means the call
    /// saw nothing left.
    ///
    /// "Lowest" holds under the lexicographic listing order the scan already
    /// depends on (see [`QueueCleaner::delete_segments_upto`]): a store that
    /// answered unordered would report the first key seen above the bound, which
    /// may not be the smallest. The field is diagnostic — nothing decides a
    /// delete from it — so the same store that makes the call delete less than
    /// it could also makes this number an upper estimate.
    pub lowest_remaining: Option<u64>,
}

/// Deletes WAL segments at or below a caller-supplied offset bound.
///
/// Holds no state of its own beyond where the queue lives; every call is
/// self-contained and idempotent — a segment already gone is simply not listed
/// the next time.
pub struct QueueCleaner {
    /// Base path the queue's topics live under (may be empty for a store root).
    base_path: String,
    /// Object store holding the segments.
    store: Arc<dyn ObjectStore>,
    /// Retries one segment deletion gets, built once per cleaner.
    retrier: Retrier,
}

impl QueueCleaner {
    /// Create a cleaner for the queue rooted at `base_path` in `store`.
    #[must_use]
    pub fn new(base_path: impl Into<String>, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            base_path: base_path.into(),
            store,
            retrier: Retrier::new(delete_retrier_config()),
        }
    }

    /// Delete every segment of `topic` whose offset is at or below
    /// `delete_bound`, oldest first.
    ///
    /// The topic prefix is listed from its start and the scan STOPS at the first
    /// key above the bound: the object stores IceGate runs on (S3 and
    /// S3-compatible) return keys in lexicographic order, which for zero-padded
    /// segment names is offset order, so everything at or below the bound has
    /// already been seen by then. `ObjectStore` itself promises no order, so a
    /// store that returns keys unordered would make this call delete less than
    /// it could — never more than the bound allows, which is the property that
    /// matters for a destructive operation.
    ///
    /// Deletion runs in batches of [`DeleteLimits::concurrency`], each batch
    /// awaited in full before the next starts, so a cancellation stops the call
    /// with the remaining (higher) segments untouched. Deleting oldest first
    /// also means a cancelled call leaves a contiguous tail, not a hole.
    ///
    /// A segment the store refuses to delete is retried ([`DELETE_ATTEMPTS`])
    /// and, once those are spent or the refusal is terminal, SKIPPED rather than
    /// made fatal: the call goes on to the higher ones and reports the refusal
    /// in [`DeleteSummary::failed`]. A key that turns out to be gone already
    /// counts as deleted — see [`Self::delete_segment`]. Only an unbroken run of
    /// refusals ends the call early, with [`DeleteSummary::is_abandoned`] set —
    /// see [`delete_failure_limit`]. A refused segment is therefore the one case
    /// that leaves a hole below the tail: the writer's recovery tolerates it
    /// (`exists_above` in [`crate::writer`]), while a reader that spans the hole
    /// is told about it rather than handed the rest
    /// ([`QueueError::SegmentMissing`], raised in [`crate::reader`]).
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::ObjectStore`] if the listing fails and
    /// [`QueueError::Cancelled`] if `cancel` fires. Individual delete failures
    /// are reported in [`DeleteSummary::failed`], not propagated.
    pub async fn delete_segments_upto(
        &self,
        topic: &Topic,
        delete_bound: u64,
        limits: &DeleteLimits,
        cancel: &CancellationToken,
    ) -> Result<DeleteSummary> {
        let mut summary = DeleteSummary::default();
        let SegmentScan {
            mut candidates,
            boundary_offset,
            is_budget_exhausted,
        } = self.collect_deletable_segments(topic, delete_bound, limits, cancel).await?;
        summary.found = candidates.len() as u64;
        summary.truncated = is_budget_exhausted;
        summary.lowest_remaining = boundary_offset;

        // Ordered here rather than relied upon from the listing: the batches
        // below must take the oldest segments first whatever order the store
        // answered in.
        candidates.sort_unstable_by_key(|candidate| candidate.offset);

        if limits.dry_run {
            summary.lowest_remaining = lower_of(candidates.first().map(|c| c.offset), summary.lowest_remaining);
            debug!(topic = %topic, delete_bound, found = summary.found, "WAL cleanup dry run");
            return Ok(summary);
        }

        let batch_size = limits.concurrency.max(1);
        let failure_limit = delete_failure_limit(batch_size);
        let mut consecutive_failures: u64 = 0;
        for (batch_idx, batch) in candidates.chunks(batch_size).enumerate() {
            if cancel.is_cancelled() {
                return Err(QueueError::Cancelled);
            }
            let outcomes = futures::stream::iter(batch.iter().cloned().map(|candidate| async move {
                self.delete_segment(&candidate, cancel)
                    .await
                    .map_err(|error| (candidate.offset, error))
            }))
            .buffer_unordered(batch_size)
            .collect::<Vec<_>>()
            .await;

            for outcome in outcomes {
                match outcome {
                    Ok(size) => {
                        consecutive_failures = 0;
                        summary.deleted += 1;
                        summary.bytes_reclaimed += size;
                    }
                    Err((offset, error)) => {
                        // A stop, not a fault of the store: it must not count
                        // towards abandoning the topic, and the segments this
                        // batch did reach stay deleted either way.
                        if matches!(error, QueueError::Cancelled) {
                            return Err(QueueError::Cancelled);
                        }
                        consecutive_failures += 1;
                        summary.failed += 1;
                        summary.lowest_remaining = lower_of(Some(offset), summary.lowest_remaining);
                        warn!(topic = %topic, offset, %error, "failed to delete WAL segment");
                    }
                }
            }
            // Nothing has gone through since the run began and the refusals have
            // added up: the store is refusing the operation, not one key.
            // Everything from the next batch on is untouched, so the first of
            // those is what remains lowest — beside any offset that already
            // failed.
            if consecutive_failures >= failure_limit {
                summary.is_abandoned = true;
                let next_untouched = candidates.get((batch_idx + 1) * batch_size).map(|candidate| candidate.offset);
                summary.lowest_remaining = lower_of(next_untouched, summary.lowest_remaining);
                warn!(
                    topic = %topic,
                    deleted = summary.deleted,
                    failed = summary.failed,
                    consecutive_failures,
                    "abandoning WAL cleanup of this topic: the object store refused an unbroken run of deletions"
                );
                break;
            }
        }

        Ok(summary)
    }

    /// Delete one segment, retrying a transient refusal, and report the bytes
    /// the listing said it held.
    ///
    /// A key the store answers `NotFound` for is a SUCCESS: the delete is
    /// idempotent, and a segment that vanished between the listing and this
    /// request (a concurrent cycle, a manual sweep) is not a fault to count
    /// against the topic. Its bytes are still reported — the size comes from the
    /// listing of this very cycle, so it is what the topic stopped holding.
    ///
    /// # Errors
    ///
    /// Returns [`QueueError::ObjectStore`] for a terminal refusal,
    /// [`QueueError::MaxAttemptsReached`] once [`DELETE_ATTEMPTS`] transient
    /// ones are spent, and [`QueueError::Cancelled`] if `cancel` fires. The
    /// attempts themselves are logged at debug level, because the exhaustion
    /// error carries no cause of its own.
    async fn delete_segment(&self, candidate: &DeletableSegment, cancel: &CancellationToken) -> Result<u64> {
        self.retrier
            .retry::<_, _, Result<u64>, QueueError>(
                || {
                    let store = Arc::clone(&self.store);
                    let path = candidate.path.clone();
                    async move {
                        match store.delete(&path).await {
                            Ok(()) => Ok((false, Ok(candidate.size))),
                            Err(object_store::Error::NotFound { .. }) => {
                                debug!(offset = candidate.offset, "WAL segment was already gone");
                                Ok((false, Ok(candidate.size)))
                            }
                            Err(error) => {
                                debug!(offset = candidate.offset, %error, "WAL segment deletion attempt failed");
                                let error = QueueError::ObjectStore(error);
                                Ok((error.is_retryable(), Err(error)))
                            }
                        }
                    }
                },
                cancel,
            )
            .await?
    }

    /// List the topic prefix and collect the segments at or below the bound.
    async fn collect_deletable_segments(
        &self,
        topic: &Topic,
        delete_bound: u64,
        limits: &DeleteLimits,
        cancel: &CancellationToken,
    ) -> Result<SegmentScan> {
        let prefix = topic_prefix(&self.base_path, topic);
        // The listing is not a lookup the caller could spare us: cleanup keeps no
        // state, so the low end of the range exists nowhere else, and the pass
        // also yields the object sizes `bytes_reclaimed` reports and the first
        // surviving key that makes `truncated` / `lowest_remaining` honest rather
        // than assumed. Deleting blind from the bound downwards would instead cost
        // one doomed request per already-deleted offset, every cycle.
        //
        // TODO(med): delete in batches. `DeleteObjects` lives in opendal's service
        // layer and is only reachable through an `Operator`, while
        // `OperatorRegistry::resolve_operator` is `pub(crate)` and
        // `OpendalStore::delete_stream` issues strictly one request per key.
        //
        // TODO(low): cut the LIST requests one cycle costs. The scan walks the
        // prefix from its start every time, so a topic whose surviving tail sits
        // far above the first key pages through the gap on each cycle. Resuming
        // from `DeleteSummary::lowest_remaining` of the previous cycle
        // (`ObjectStore::list_with_offset`) would start the scan at the tail
        // instead, but that number lives nowhere between cycles today: cleanup
        // keeps no state, and giving it one means a per-topic record that
        // survives a restart and stays correct when segments below it are
        // refused, so the saving has to be worth that.
        let mut stream = self.store.list(Some(&prefix));
        // The array accommodates all candidates at once up to `max_deletes`.
        let mut candidates: Vec<DeletableSegment> = Vec::new();
        let mut boundary_offset = None;
        let mut is_budget_exhausted = false;

        loop {
            // Race the listing against cancellation so a slow store cannot hold
            // shutdown until the page resolves.
            let item = tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(QueueError::Cancelled),
                item = stream.next() => item,
            };
            let Some(item) = item else { break };
            let meta = item?;
            let Some(offset) = parse_segment_offset(&self.base_path, &meta.location) else {
                continue;
            };
            if offset > delete_bound {
                boundary_offset = lower_of(Some(offset), boundary_offset);
                break;
            }
            // Reached only on a key the budget has no room for, so the budget —
            // not the end of the prefix — is what stopped the scan. Deciding
            // this from `candidates.len() == max_deletes` after the fact cannot
            // tell the two apart, and reports a full cycle as a partial one.
            if candidates.len() >= limits.max_deletes {
                boundary_offset = lower_of(Some(offset), boundary_offset);
                is_budget_exhausted = true;
                break;
            }
            candidates.push(DeletableSegment {
                path: meta.location,
                offset,
                size: meta.size,
            });
        }

        Ok(SegmentScan {
            candidates,
            boundary_offset,
            is_budget_exhausted,
        })
    }
}

/// What one listing pass over a topic prefix found.
struct SegmentScan {
    /// Segments at or below the bound, in the order the store listed them.
    candidates: Vec<DeletableSegment>,
    /// Lowest offset the scan left behind: the first key above the bound, or the
    /// one the [`DeleteLimits::max_deletes`] cut stopped at.
    boundary_offset: Option<u64>,
    /// Whether [`DeleteLimits::max_deletes`] stopped the scan, so segments at or
    /// below the bound remain for the next call.
    is_budget_exhausted: bool,
}

/// Written out rather than derived: `ObjectStore` requires `Debug`, so a derive
/// would compile and would print the store's own configuration — endpoint,
/// bucket, and whatever a backend chooses to include — into every log line and
/// error that formats a cleaner. Only where the queue lives belongs there.
impl std::fmt::Debug for QueueCleaner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueCleaner")
            .field("base_path", &self.base_path)
            .finish_non_exhaustive()
    }
}

/// One segment selected for deletion, with what the listing already told us
/// about it (no extra HEAD request needed for the reclaimed size).
///
/// `Clone` so a delete batch can own its entries: a batch built from borrows
/// would need a higher-ranked closure, which `buffer_unordered` does not accept.
#[derive(Clone)]
struct DeletableSegment {
    /// Full object key.
    path: Path,
    /// Segment offset parsed from the key.
    offset: u64,
    /// Object size in bytes, as reported by the listing.
    size: u64,
}

/// The lower of two optional offsets, treating `None` as "no offset yet".
fn lower_of(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (value, None) | (None, value) => value,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use bytes::Bytes;
    use object_store::{PutPayload, memory::InMemory};

    use super::*;
    use crate::segment::SegmentId;

    const BASE_PATH: &str = "queue";
    const TOPIC: &str = "logs";

    /// Payload of every seeded segment; cleanup reads keys and sizes only.
    const SEGMENT_BODY: &[u8] = b"segment";

    fn limits(max_deletes: usize, concurrency: usize, dry_run: bool) -> DeleteLimits {
        DeleteLimits {
            max_deletes,
            concurrency,
            dry_run,
        }
    }

    async fn seed_segments(store: &InMemory, topic: &str, offsets: impl IntoIterator<Item = u64>) {
        for offset in offsets {
            let path = Path::from(format!(
                "{BASE_PATH}/{}",
                SegmentId::new(topic, offset).to_relative_path()
            ));
            store
                .put(&path, PutPayload::from_bytes(Bytes::from_static(SEGMENT_BODY)))
                .await
                .expect("seed segment");
        }
    }

    /// Offsets of the segments still present under the topic prefix, ascending.
    ///
    /// Panics on a key that is not a segment, so a test that puts a foreign
    /// object under the prefix reads the raw keys with [`surviving_keys`].
    async fn surviving_offsets(store: &InMemory, topic: &str) -> Vec<u64> {
        let prefix = topic_prefix(BASE_PATH, &topic.to_string());
        let mut offsets: Vec<u64> = store
            .list(Some(&prefix))
            .map(|meta| {
                let meta = meta.expect("list segment");
                parse_segment_offset(BASE_PATH, &meta.location).expect("segment key")
            })
            .collect::<Vec<_>>()
            .await;
        offsets.sort_unstable();
        offsets
    }

    /// Every key still present under the topic prefix, segment or not, sorted.
    async fn surviving_keys(store: &InMemory, topic: &str) -> Vec<String> {
        let prefix = topic_prefix(BASE_PATH, &topic.to_string());
        let mut keys: Vec<String> = store
            .list(Some(&prefix))
            .map(|meta| meta.expect("list object").location.to_string())
            .collect::<Vec<_>>()
            .await;
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn deletes_exactly_the_segments_at_or_below_the_bound() {
        let store = Arc::new(InMemory::new());
        seed_segments(&store, TOPIC, 0..6).await;
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 3, &limits(100, 4, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(summary.found, 4);
        assert_eq!(summary.deleted, 4);
        assert_eq!(summary.failed, 0);
        assert!(!summary.truncated);
        assert_eq!(summary.lowest_remaining, Some(4));
        // The only report of how much the topic stopped holding, and the sole
        // input to `wal_cleanup.bytes.reclaimed`: four seeded bodies, counted
        // from the seed rather than from the cleaner's own accounting.
        assert_eq!(summary.bytes_reclaimed, 4 * SEGMENT_BODY.len() as u64);
        assert_eq!(surviving_offsets(&store, TOPIC).await, vec![4, 5]);
    }

    /// A bucket is shared: a stray upload, another tool's output, an operator's
    /// note can all land under the topic prefix. Cleanup lists that prefix, so
    /// anything it cannot parse as a segment of THIS crate's format must survive
    /// a cycle that reclaims everything around it — deleting a foreign object is
    /// data loss outside the queue's own contract.
    #[tokio::test]
    async fn a_foreign_object_under_the_topic_prefix_survives_the_cycle() {
        /// Both shapes a foreign key takes: one that looks like a segment file
        /// and one that does not. Listed in sorted order, which is what
        /// [`surviving_keys`] returns.
        const FOREIGN_KEYS: [&str; 2] = ["queue/logs/README.txt", "queue/logs/notes.parquet"];

        let store = Arc::new(InMemory::new());
        seed_segments(&store, TOPIC, 0..4).await;
        for key in FOREIGN_KEYS {
            store
                .put(
                    &Path::from(key),
                    PutPayload::from_bytes(Bytes::from_static(SEGMENT_BODY)),
                )
                .await
                .expect("seed foreign object");
        }
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 3, &limits(100, 4, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(summary.found, 4, "the foreign keys are not candidates");
        assert_eq!(summary.deleted, 4);
        assert_eq!(
            surviving_keys(&store, TOPIC).await,
            FOREIGN_KEYS.to_vec(),
            "every segment goes and both foreign objects stay"
        );
    }

    /// The bound is inclusive, and the segment one above it is exactly what a
    /// live reader may still be reading — it must survive.
    #[tokio::test]
    async fn keeps_the_segment_immediately_above_the_bound() {
        let store = Arc::new(InMemory::new());
        seed_segments(&store, TOPIC, 0..3).await;
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        cleaner
            .delete_segments_upto(&TOPIC.to_string(), 0, &limits(100, 4, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(surviving_offsets(&store, TOPIC).await, vec![1, 2]);
    }

    /// A cycle that hits its budget must SAY so: a caller reading `truncated`
    /// as "all done" would report a tail as reclaimed while it is still there.
    #[tokio::test]
    async fn reports_truncation_when_the_delete_budget_is_reached() {
        let store = Arc::new(InMemory::new());
        seed_segments(&store, TOPIC, 0..10).await;
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 9, &limits(3, 2, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert!(summary.truncated);
        assert_eq!(summary.found, 3);
        assert_eq!(summary.deleted, 3);
        assert_eq!(summary.lowest_remaining, Some(3));
        assert_eq!(surviving_offsets(&store, TOPIC).await, (3..10).collect::<Vec<_>>());
    }

    /// The opposite of the case above and the one that used to be misreported:
    /// the candidates fill the budget exactly and the prefix ends there, so the
    /// cycle IS complete. Claiming truncation here makes the operator's only
    /// honesty signal cry wolf on every steady-state cycle whose queue happens
    /// to match the budget.
    #[tokio::test]
    async fn a_budget_filled_exactly_is_a_complete_cycle() {
        let store = Arc::new(InMemory::new());
        seed_segments(&store, TOPIC, 0..3).await;
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 2, &limits(3, 2, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(summary.found, 3);
        assert_eq!(summary.deleted, 3);
        assert!(!summary.truncated, "nothing at or below the bound was left behind");
        assert_eq!(summary.lowest_remaining, None);
        assert!(surviving_offsets(&store, TOPIC).await.is_empty());
    }

    /// The same for a budget filled exactly with segments left ABOVE the bound:
    /// the scan stops on the first of those, not on the budget.
    #[tokio::test]
    async fn a_budget_filled_exactly_below_a_surviving_tail_is_complete() {
        let store = Arc::new(InMemory::new());
        seed_segments(&store, TOPIC, 0..6).await;
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 2, &limits(3, 2, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(summary.deleted, 3);
        assert!(!summary.truncated);
        assert_eq!(summary.lowest_remaining, Some(3));
        assert_eq!(surviving_offsets(&store, TOPIC).await, vec![3, 4, 5]);
    }

    /// One cleaner serves every topic (in IceGate, four jobs share it), so a
    /// cycle asked for one topic must not reach the segments of another — and
    /// the bound it was given means nothing there, because each topic's bound is
    /// derived from its own table.
    #[tokio::test]
    async fn a_cycle_leaves_the_segments_of_another_topic_alone() {
        const OTHER_TOPIC: &str = "spans";

        let store = Arc::new(InMemory::new());
        seed_segments(&store, TOPIC, 0..6).await;
        seed_segments(&store, OTHER_TOPIC, 0..6).await;
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 5, &limits(100, 4, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(summary.found, 6, "only one topic's segments are candidates");
        assert!(surviving_offsets(&store, TOPIC).await.is_empty());
        assert_eq!(
            surviving_offsets(&store, OTHER_TOPIC).await,
            (0..6).collect::<Vec<_>>(),
            "the neighbouring topic is untouched"
        );
    }

    /// `ObjectStore` promises no listing order, and the cleaner must take the
    /// oldest segments first regardless: a cycle stopped partway leaves a
    /// contiguous tail rather than a hole, which is the difference between a
    /// reader resuming and a reader failing on a missing segment. The store here
    /// answers the listing backwards and cancels once one segment is gone, so
    /// the assertion reads which segment that was.
    #[tokio::test]
    async fn an_interrupted_cycle_deletes_the_oldest_segment_first_on_an_unordered_listing() {
        let inner = Arc::new(InMemory::new());
        seed_segments(&inner, TOPIC, 0..6).await;
        let cancel = CancellationToken::new();
        let store = Arc::new(ListingStore {
            inner: Arc::new(CancelOnDeleteStore {
                inner: Arc::clone(&inner),
                cancel: cancel.clone(),
            }),
            order: ListingOrder::Reversed,
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let error = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 5, &limits(100, 1, false), &cancel)
            .await
            .expect_err("cancelled cleanup must not report success");

        assert!(matches!(error, QueueError::Cancelled));
        assert_eq!(
            surviving_offsets(&inner, TOPIC).await,
            (1..6).collect::<Vec<_>>(),
            "the first batch must take offset 0, not whichever key the listing answered first"
        );
    }

    /// The other half of the same guarantee: on an unordered listing the scan
    /// stops at the first key above the bound and so may take LESS than it
    /// could, but it never takes more. Deleting a segment above the bound is the
    /// one outcome that costs rows nothing else can restore.
    ///
    /// The assertions pin the early stop itself, not merely "nothing above the
    /// bound went": the reversed listing hands the scan a key above the bound
    /// first, so a correct cycle here deletes NOTHING. A scan taught to read
    /// past that key and take everything below the bound would be safe and
    /// would fail this test — which is why the stop is stated in the name.
    #[tokio::test]
    async fn an_unordered_listing_stops_the_scan_at_the_first_key_above_the_bound() {
        let inner = Arc::new(InMemory::new());
        seed_segments(&inner, TOPIC, 0..6).await;
        let store = Arc::new(ListingStore {
            inner: Arc::clone(&inner) as _,
            order: ListingOrder::Reversed,
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 2, &limits(100, 4, false), &CancellationToken::new())
            .await
            .expect("an unordered listing is not a failed cycle");

        assert_eq!(
            summary.deleted, 0,
            "the reversed listing hands the scan a key above the bound first, and it stops there"
        );
        assert_eq!(surviving_offsets(&inner, TOPIC).await, (0..6).collect::<Vec<_>>());
    }

    /// A segment the store will never delete — an object lock, a bucket policy —
    /// sits at the head of the prefix, so every cycle meets it first. Stopping
    /// there would freeze the topic for good; the cycle must step over it and
    /// reclaim everything else below the bound.
    #[tokio::test]
    async fn a_locked_segment_does_not_stop_the_rest_of_the_cycle() {
        let inner = Arc::new(InMemory::new());
        seed_segments(&inner, TOPIC, 0..6).await;
        let store = Arc::new(RefusingDeleteStore {
            inner: Arc::clone(&inner),
            refusal: Refusal::Offset(0),
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);
        let topic = TOPIC.to_string();
        let cancel = CancellationToken::new();

        let first = cleaner
            .delete_segments_upto(&topic, 5, &limits(100, 2, false), &cancel)
            .await
            .expect("cleanup");

        assert_eq!(first.deleted, 5);
        assert_eq!(first.failed, 1);
        assert!(
            !first.is_abandoned,
            "one locked key is not a store refusing the operation"
        );
        assert_eq!(first.lowest_remaining, Some(0));
        assert_eq!(surviving_offsets(&inner, TOPIC).await, vec![0]);

        let second = cleaner
            .delete_segments_upto(&topic, 5, &limits(100, 2, false), &cancel)
            .await
            .expect("cleanup");

        assert_eq!(second.found, 1, "only the locked segment is left to try");
        assert_eq!(second.failed, 1);
        assert_eq!(
            surviving_offsets(&inner, TOPIC).await,
            vec![0],
            "the cycle cannot go backwards"
        );
    }

    /// A store refusing EVERY deletion is the other fault, and it must not cost
    /// one doomed request per candidate: the call gives up once the failures add
    /// up with nothing having succeeded. The run it takes is one delete batch,
    /// so a concurrency ABOVE the floor is what this case pins down.
    #[tokio::test]
    async fn a_store_refusing_every_delete_gives_up_after_one_batch() {
        const CONCURRENCY: usize = 8;
        /// One full batch of [`CONCURRENCY`] refusals, which is the threshold.
        const EXPECTED_FAILURES: u64 = 8;

        let inner = Arc::new(InMemory::new());
        let segment_count = EXPECTED_FAILURES * 3;
        seed_segments(&inner, TOPIC, 0..segment_count).await;
        let store = Arc::new(RefusingDeleteStore {
            inner: Arc::clone(&inner),
            refusal: Refusal::Every,
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(
                &TOPIC.to_string(),
                segment_count - 1,
                &limits(
                    usize::try_from(segment_count).expect("budget fits usize"),
                    CONCURRENCY,
                    false,
                ),
                &CancellationToken::new(),
            )
            .await
            .expect("cleanup");

        assert_eq!(summary.deleted, 0);
        assert_eq!(
            summary.failed, EXPECTED_FAILURES,
            "the call must stop probing a dead store, not walk the whole budget"
        );
        assert!(summary.is_abandoned);
        assert_eq!(
            surviving_offsets(&inner, TOPIC).await,
            (0..segment_count).collect::<Vec<_>>()
        );
    }

    /// The same fault arriving MID-CYCLE — credentials that expire, throttling
    /// that starts, a policy that changes while the call runs. The successes
    /// before it must not buy the dead store a walk through the whole budget,
    /// which is what a threshold on the call's total failures did.
    #[tokio::test]
    async fn a_store_that_starts_refusing_mid_cycle_gives_up_at_the_same_limit() {
        const CONCURRENCY: usize = 6;
        /// One full batch of [`CONCURRENCY`] refusals, as above.
        const EXPECTED_FAILURES: u64 = 6;
        /// Segments deleted before the store breaks; a whole number of batches,
        /// so the break lands between batches rather than inside one.
        const DELETES_BEFORE_REFUSAL: u64 = 12;

        let inner = Arc::new(InMemory::new());
        let segment_count = DELETES_BEFORE_REFUSAL + EXPECTED_FAILURES * 3;
        seed_segments(&inner, TOPIC, 0..segment_count).await;
        let store = Arc::new(RefusingDeleteStore {
            inner: Arc::clone(&inner),
            refusal: Refusal::FromOffset(DELETES_BEFORE_REFUSAL),
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(
                &TOPIC.to_string(),
                segment_count - 1,
                &limits(
                    usize::try_from(segment_count).expect("budget fits usize"),
                    CONCURRENCY,
                    false,
                ),
                &CancellationToken::new(),
            )
            .await
            .expect("cleanup");

        assert_eq!(summary.deleted, DELETES_BEFORE_REFUSAL);
        assert_eq!(
            summary.failed, EXPECTED_FAILURES,
            "a successful start must not extend how long the call probes a dead store"
        );
        assert!(summary.is_abandoned);
        assert_eq!(
            surviving_offsets(&inner, TOPIC).await,
            (DELETES_BEFORE_REFUSAL..segment_count).collect::<Vec<_>>()
        );
    }

    /// The floor: at a concurrency of one, "a fully failed batch" is a single
    /// key, so a threshold taken from the batch size alone would let one locked
    /// object abandon the topic on every cycle — the very fault the mechanism
    /// exists to step over.
    #[tokio::test]
    async fn a_single_locked_key_cannot_abandon_a_topic_cleaned_one_at_a_time() {
        let inner = Arc::new(InMemory::new());
        seed_segments(&inner, TOPIC, 0..6).await;
        let store = Arc::new(RefusingDeleteStore {
            inner: Arc::clone(&inner),
            refusal: Refusal::Offset(0),
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 5, &limits(100, 1, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(summary.deleted, 5);
        assert_eq!(summary.failed, 1);
        assert!(!summary.is_abandoned);
        assert_eq!(surviving_offsets(&inner, TOPIC).await, vec![0]);
    }

    /// A refusal the store will answer differently next time — a reset
    /// connection, a 5xx, throttling — must cost the cycle a repeat, not a
    /// segment: the tail is reclaimed and nothing is reported as failed.
    #[tokio::test]
    async fn a_transient_refusal_is_retried_rather_than_counted_as_a_failure() {
        let inner = Arc::new(InMemory::new());
        seed_segments(&inner, TOPIC, 0..4).await;
        let store = Arc::new(RefusingDeleteStore {
            inner: Arc::clone(&inner),
            refusal: Refusal::OnceTransiently(Arc::new(Mutex::new(HashSet::new()))),
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 3, &limits(100, 2, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(summary.deleted, 4);
        assert_eq!(summary.failed, 0);
        assert!(!summary.is_abandoned);
        assert!(surviving_offsets(&inner, TOPIC).await.is_empty());
    }

    /// The DELETE requests a store refusing every deletion costs before the call
    /// abandons the topic, across concurrencies on both sides of
    /// [`MIN_DELETE_FAILURE_LIMIT`]. The run is checked only after a whole batch,
    /// so the failed segments round UP to a full batch: below the floor a run
    /// spans several single-or-small batches (more failures than the
    /// concurrency), at or above it one batch already trips the limit. Each
    /// refused segment is retried to [`DELETE_ATTEMPTS`] exhaustion, so the
    /// request count is the failed segments times that. Expected values are
    /// stated per case, not derived from the production helpers.
    #[tokio::test]
    async fn a_broken_store_costs_at_most_one_rounded_up_batch_of_delete_retries() {
        // (concurrency, expected failed segments, expected DELETE requests).
        // Concurrency 0 folds onto a batch of one (`concurrency.max(1)`).
        let cases: [(usize, u64, u64); 5] = [(0, 4, 16), (1, 4, 16), (3, 6, 24), (4, 4, 16), (8, 8, 32)];

        for (concurrency, expected_failed, expected_attempts) in cases {
            let inner = Arc::new(InMemory::new());
            // Three batches' worth of candidates, so a call that wrongly walked
            // the whole budget would fail far more than one rounded-up batch.
            let segment_count = expected_failed * 3;
            seed_segments(&inner, TOPIC, 0..segment_count).await;
            let attempts = Arc::new(AtomicU64::new(0));
            let store = Arc::new(AttemptCountingStore {
                inner: Arc::clone(&inner),
                attempts: Arc::clone(&attempts),
            });
            let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

            let summary = cleaner
                .delete_segments_upto(
                    &TOPIC.to_string(),
                    segment_count - 1,
                    &limits(
                        usize::try_from(segment_count).expect("budget fits usize"),
                        concurrency,
                        false,
                    ),
                    &CancellationToken::new(),
                )
                .await
                .expect("exhausted retries must not surface as a cancelled cleanup");

            assert_eq!(summary.deleted, 0, "concurrency {concurrency}");
            assert_eq!(
                summary.failed, expected_failed,
                "concurrency {concurrency}: failures round up to a full batch"
            );
            assert!(summary.is_abandoned, "concurrency {concurrency}");
            assert_eq!(
                attempts.load(Ordering::Relaxed),
                expected_attempts,
                "concurrency {concurrency}: each refused segment costs DELETE_ATTEMPTS requests"
            );
            assert_eq!(
                surviving_offsets(&inner, TOPIC).await,
                (0..segment_count).collect::<Vec<_>>(),
                "concurrency {concurrency}"
            );
        }
    }

    /// A segment that vanished between the listing and the delete — a manual
    /// sweep, a concurrent cycle — is not a fault: the topic no longer holds it,
    /// which is exactly what the call was asked to achieve.
    #[tokio::test]
    async fn a_segment_already_gone_counts_as_deleted() {
        let inner = Arc::new(InMemory::new());
        seed_segments(&inner, TOPIC, 0..4).await;
        let store = Arc::new(RefusingDeleteStore {
            inner: Arc::clone(&inner),
            refusal: Refusal::NotFound(1),
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 3, &limits(100, 2, false), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(summary.deleted, 4);
        assert_eq!(summary.failed, 0);
        assert!(!summary.is_abandoned);
        assert_eq!(
            surviving_offsets(&inner, TOPIC).await,
            vec![1],
            "the double answered NotFound without touching the object"
        );
    }

    /// Which deletions [`RefusingDeleteStore`] turns down.
    #[derive(Debug, Clone)]
    enum Refusal {
        /// One key is permanently undeletable, everything else goes through.
        Offset(u64),
        /// Every key at or above an offset is refused: the store breaks partway
        /// through the cycle and stays broken. Deterministic because the cleaner
        /// deletes oldest first.
        FromOffset(u64),
        /// Every key is refused: the store, not the key, is the fault.
        Every,
        /// One key answers `NotFound` and is left in place, so the assertion can
        /// tell "reported gone" apart from "actually deleted".
        NotFound(u64),
        /// Every key is refused ONCE with a transient fault and goes through on
        /// the next attempt. The set holds the offsets already refused.
        OnceTransiently(Arc<Mutex<HashSet<u64>>>),
    }

    impl Refusal {
        /// The error a deletion of `location` is answered with, or `None` when
        /// it goes through to the inner store.
        fn answer(&self, location: &Path) -> Option<object_store::Error> {
            let offset = parse_segment_offset(BASE_PATH, location)?;
            match self {
                Self::Offset(refused) => (offset == *refused).then(terminal_refusal),
                Self::FromOffset(refused) => (offset >= *refused).then(terminal_refusal),
                Self::Every => Some(terminal_refusal()),
                Self::NotFound(missing) => (offset == *missing).then(|| object_store::Error::NotFound {
                    path: location.to_string(),
                    source: "no such key".into(),
                }),
                // The lock is released within this expression, so no guard
                // crosses the caller's await.
                Self::OnceTransiently(refused_offsets) => refused_offsets
                    .lock()
                    .expect("refusal bookkeeping is never poisoned")
                    .insert(offset)
                    .then(transient_refusal),
            }
        }
    }

    /// A refusal no repeat will get past: an untyped source, which is what
    /// [`icegate_common::is_retryable_object_store_error`] calls terminal.
    fn terminal_refusal() -> object_store::Error {
        object_store::Error::Generic {
            store: "RefusingDeleteStore",
            source: "deletion refused".into(),
        }
    }

    /// A refusal the classifier calls transient, in the shape a real backend
    /// produces it: a network fault wrapped in `Generic`.
    fn transient_refusal() -> object_store::Error {
        object_store::Error::Generic {
            store: "RefusingDeleteStore",
            source: std::io::Error::from(std::io::ErrorKind::ConnectionReset).into(),
        }
    }

    /// Store double that answers the refused deletions with an error and passes
    /// the rest through to `inner`.
    #[derive(Debug)]
    struct RefusingDeleteStore {
        inner: Arc<InMemory>,
        refusal: Refusal,
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
            let refusal = self.refusal.clone();
            let inner = Arc::clone(&self.inner);
            locations
                .then(move |location| {
                    let inner = Arc::clone(&inner);
                    let refusal = refusal.clone();
                    async move {
                        let location = location?;
                        if let Some(error) = refusal.answer(&location) {
                            return Err(error);
                        }
                        inner.delete(&location).await?;
                        Ok(location)
                    }
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

    /// Store double that refuses EVERY deletion transiently and counts the
    /// delete requests it received, so a test can pin down how many DELETE calls
    /// a broken store costs before the cleanup gives up. Reads pass through to
    /// `inner` so the listing still sees the seeded segments.
    #[derive(Debug)]
    struct AttemptCountingStore {
        inner: Arc<InMemory>,
        attempts: Arc<AtomicU64>,
    }

    impl std::fmt::Display for AttemptCountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "AttemptCountingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for AttemptCountingStore {
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
            // `ObjectStoreExt::delete` routes one path through here per call, so
            // counting yielded locations counts delete attempts one to one.
            let attempts = Arc::clone(&self.attempts);
            locations
                .then(move |location| {
                    let attempts = Arc::clone(&attempts);
                    async move {
                        let _location = location?;
                        attempts.fetch_add(1, Ordering::Relaxed);
                        Err(transient_refusal())
                    }
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

    #[tokio::test]
    async fn dry_run_counts_without_deleting() {
        let store = Arc::new(InMemory::new());
        seed_segments(&store, TOPIC, 0..5).await;
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 2, &limits(100, 4, true), &CancellationToken::new())
            .await
            .expect("cleanup");

        assert_eq!(summary.found, 3);
        assert_eq!(summary.deleted, 0);
        assert_eq!(summary.bytes_reclaimed, 0);
        assert_eq!(summary.lowest_remaining, Some(0));
        assert_eq!(surviving_offsets(&store, TOPIC).await, (0..5).collect::<Vec<_>>());
    }

    /// Cleanup runs on a schedule, so the same bound arrives again and again.
    /// The second call must be a no-op rather than an error over missing keys.
    #[tokio::test]
    async fn repeating_the_same_cleanup_changes_nothing() {
        let store = Arc::new(InMemory::new());
        seed_segments(&store, TOPIC, 0..5).await;
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);
        let topic = TOPIC.to_string();
        let cancel = CancellationToken::new();

        cleaner
            .delete_segments_upto(&topic, 2, &limits(100, 4, false), &cancel)
            .await
            .expect("first cleanup");
        let survivors_after_first = surviving_offsets(&store, TOPIC).await;

        let second = cleaner
            .delete_segments_upto(&topic, 2, &limits(100, 4, false), &cancel)
            .await
            .expect("second cleanup");

        assert_eq!(second.found, 0);
        assert_eq!(second.deleted, 0);
        assert_eq!(surviving_offsets(&store, TOPIC).await, survivors_after_first);
    }

    /// Cancellation during the delete phase stops the call and leaves the
    /// segments it had not reached in place. The token fires from inside the
    /// store on the first delete, so the stop is observed BETWEEN batches rather
    /// than being pre-cancelled at the listing.
    #[tokio::test]
    async fn cancellation_between_batches_stops_the_deletes() {
        let inner = Arc::new(InMemory::new());
        seed_segments(&inner, TOPIC, 0..6).await;
        let cancel = CancellationToken::new();
        let store = Arc::new(CancelOnDeleteStore {
            inner: Arc::clone(&inner),
            cancel: cancel.clone(),
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let error = cleaner
            .delete_segments_upto(&TOPIC.to_string(), 5, &limits(100, 1, false), &cancel)
            .await
            .expect_err("cancelled cleanup must not report success");

        assert!(matches!(error, QueueError::Cancelled));
        let survivors = surviving_offsets(&inner, TOPIC).await;
        assert_eq!(
            survivors,
            (1..6).collect::<Vec<_>>(),
            "only the first batch (one segment) may have been deleted"
        );
    }

    /// A store that accepted the LIST and then went quiet must not hold the
    /// cycle: the cleanup task is stopped by cancelling its token (that is what
    /// `cleanup_timeout_secs` turns into), and a cycle parked on a listing would
    /// never observe it, so the maintain worker never comes back.
    #[tokio::test]
    async fn cancellation_during_the_listing_stops_the_cycle() {
        let cancel = CancellationToken::new();
        let store = Arc::new(ListingStore {
            inner: Arc::new(InMemory::new()),
            order: ListingOrder::Stalling,
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let cancelling = cancel.clone();
        let canceller = tokio::spawn(async move {
            // Yields until the cleanup task has parked on the listing, so the
            // token fires while the call is inside `list`, not before it.
            tokio::task::yield_now().await;
            cancelling.cancel();
        });

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            cleaner.delete_segments_upto(&TOPIC.to_string(), 5, &limits(100, 4, false), &cancel),
        )
        .await
        .expect("a cancelled cycle must not wait on the listing")
        .expect_err("cancelled cleanup must not report success");

        assert!(matches!(error, QueueError::Cancelled));
        canceller.await.expect("the cancelling task must not panic");
    }

    /// Cancellation caught INSIDE a delete batch is a stop, not a fault of the
    /// store: reported as one it would count towards abandoning the topic and
    /// leave the caller recording store failures in
    /// `wal_cleanup.segments.delete_failed` on every ordinary shutdown.
    ///
    /// The candidates are exactly ONE batch, so nothing follows the cancelled
    /// one: with a second batch the check at the top of the loop would raise the
    /// same `Cancelled` whatever the batch's own outcomes were, and the case
    /// would pass with the raise inside the batch removed.
    #[tokio::test]
    async fn cancellation_inside_a_batch_is_raised_as_cancelled_not_as_a_failure() {
        let inner = Arc::new(InMemory::new());
        seed_segments(&inner, TOPIC, 0..2).await;
        let cancel = CancellationToken::new();
        let store = Arc::new(HangingDeleteStore {
            inner: Arc::clone(&inner),
            cancel_at: 0,
            cancel: cancel.clone(),
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let error = tokio::time::timeout(
            Duration::from_secs(5),
            cleaner.delete_segments_upto(&TOPIC.to_string(), 1, &limits(100, 2, false), &cancel),
        )
        .await
        .expect("a cancelled batch must not wait on the hanging delete")
        .expect_err("cancelled cleanup must not report success");

        assert!(matches!(error, QueueError::Cancelled));
        assert_eq!(
            surviving_offsets(&inner, TOPIC).await,
            vec![1],
            "the delete that did go through stays done"
        );
    }

    /// What one cycle costs in billed requests: one listing pass for the whole
    /// topic and no HEAD at all, because the sizes `bytes_reclaimed` reports come
    /// from that same listing. A HEAD per segment would double the request count
    /// of every cycle on a queue whose tail is large.
    #[tokio::test]
    async fn a_cycle_costs_one_listing_and_no_head_requests() {
        const SEGMENT_COUNT: u64 = 10;

        let inner = Arc::new(InMemory::new());
        seed_segments(&inner, TOPIC, 0..SEGMENT_COUNT).await;
        let counts = Arc::new(RequestCounts::default());
        let store = Arc::new(RequestCountingStore {
            inner: Arc::clone(&inner),
            counts: Arc::clone(&counts),
        });
        let cleaner = QueueCleaner::new(BASE_PATH, Arc::clone(&store) as _);

        let summary = cleaner
            .delete_segments_upto(
                &TOPIC.to_string(),
                SEGMENT_COUNT - 1,
                &limits(100, 4, false),
                &CancellationToken::new(),
            )
            .await
            .expect("cleanup");

        assert_eq!(summary.deleted, SEGMENT_COUNT);
        assert_eq!(counts.lists.load(Ordering::Relaxed), 1, "one pass over the prefix");
        assert_eq!(
            counts.reads.load(Ordering::Relaxed),
            0,
            "the listing already carries every size and path the cycle needs"
        );
        assert_eq!(counts.deletes.load(Ordering::Relaxed), SEGMENT_COUNT);
    }

    /// Order [`ListingStore`] answers a listing in.
    #[derive(Debug, Clone, Copy)]
    enum ListingOrder {
        /// Every key of the inner store, highest first — `ObjectStore` promises
        /// no order, and this is the order that breaks a scan relying on one.
        Reversed,
        /// Never yields an item, modelling a store that accepted the request and
        /// went quiet. Without a duration for the test to guess at.
        Stalling,
    }

    /// Store double that bends the LISTING and passes everything else through,
    /// so it can be layered over another double that bends the deletes.
    #[derive(Debug)]
    struct ListingStore {
        inner: Arc<dyn ObjectStore>,
        order: ListingOrder,
    }

    impl std::fmt::Display for ListingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ListingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ListingStore {
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
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            match self.order {
                ListingOrder::Reversed => {
                    let listing = self.inner.list(prefix);
                    futures::stream::once(async move {
                        let mut items = listing.collect::<Vec<_>>().await;
                        items.reverse();
                        futures::stream::iter(items)
                    })
                    .flatten()
                    .boxed()
                }
                ListingOrder::Stalling => futures::stream::pending().boxed(),
            }
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, opts: object_store::CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    /// Store double that cancels the caller's token on ONE segment's deletion,
    /// lets that one through, and never answers any other — the shape a store
    /// takes when the process is shutting down while a batch is in flight.
    #[derive(Debug)]
    struct HangingDeleteStore {
        inner: Arc<InMemory>,
        /// Offset whose deletion fires the token and goes through.
        cancel_at: u64,
        cancel: CancellationToken,
    }

    impl std::fmt::Display for HangingDeleteStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "HangingDeleteStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for HangingDeleteStore {
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
            let inner = Arc::clone(&self.inner);
            let cancel = self.cancel.clone();
            let cancel_at = self.cancel_at;
            locations
                .then(move |location| {
                    let inner = Arc::clone(&inner);
                    let cancel = cancel.clone();
                    async move {
                        let location = location?;
                        if parse_segment_offset(BASE_PATH, &location) == Some(cancel_at) {
                            inner.delete(&location).await?;
                            cancel.cancel();
                            return Ok(location);
                        }
                        std::future::pending().await
                    }
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

    /// Billed requests one cycle made, by kind.
    #[derive(Debug, Default)]
    struct RequestCounts {
        lists: AtomicU64,
        /// GET and HEAD alike: `ObjectStore::head` is a `get_opts` with the head
        /// flag, so both land here and a HEAD per segment cannot hide.
        reads: AtomicU64,
        deletes: AtomicU64,
    }

    /// Store double that counts the requests it is handed and serves them all
    /// from `inner`, so a cycle over it both succeeds and states its cost.
    #[derive(Debug)]
    struct RequestCountingStore {
        inner: Arc<InMemory>,
        counts: Arc<RequestCounts>,
    }

    impl std::fmt::Display for RequestCountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "RequestCountingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for RequestCountingStore {
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
            self.counts.reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            let counts = Arc::clone(&self.counts);
            let inner = Arc::clone(&self.inner);
            locations
                .then(move |location| {
                    let counts = Arc::clone(&counts);
                    let inner = Arc::clone(&inner);
                    async move {
                        let location = location?;
                        counts.deletes.fetch_add(1, Ordering::Relaxed);
                        inner.delete(&location).await?;
                        Ok(location)
                    }
                })
                .boxed()
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            self.counts.lists.fetch_add(1, Ordering::Relaxed);
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<object_store::ListResult> {
            self.counts.lists.fetch_add(1, Ordering::Relaxed);
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, opts: object_store::CopyOptions) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    /// Store double that cancels the caller's token as soon as one delete has
    /// gone through, so the cleanup loop meets a cancelled token at the start of
    /// its second batch.
    #[derive(Debug)]
    struct CancelOnDeleteStore {
        inner: Arc<InMemory>,
        cancel: CancellationToken,
    }

    impl std::fmt::Display for CancelOnDeleteStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CancelOnDeleteStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CancelOnDeleteStore {
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
            // `ObjectStoreExt::delete` routes through this, so cancelling on the
            // first yielded deletion cancels as soon as one segment is gone.
            let cancel = self.cancel.clone();
            self.inner.delete_stream(locations).inspect(move |_| cancel.cancel()).boxed()
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
}
