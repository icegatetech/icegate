//! The per-tenant system-index store of one replica.
//!
//! Layout under the store's location, per tenant: `<tenant>/current` is the pointer, a small JSON
//! object naming the current snapshot object; `<tenant>/snap-<uuid>` are the immutable snapshot
//! objects, each a lineage frame and the complete snapshot (see the codec module). A tenant whose
//! pointer does not exist has no system index.
//!
//! Reads send a conditional GET of the pointer with the cached `ETag`; not-modified serves the
//! cached snapshot, so a write on any replica is visible to the next read on every replica. An LRU
//! over decoded snapshots, bounded by their total decoded length, holds the cache.
//!
//! Writes go through one write loop per tenant and replica. The loop coalesces the operations
//! queued while it was busy into one snapshot object and one compare-and-swap of the pointer
//! (`PutMode::Update` on the `ETag` the attempt read, or `PutMode::Create` on an absent pointer).
//! The snapshot object is stored before the pointer is swapped, so a pointer never names a missing
//! object. A loop ends once its queue is empty after a batch, so a replica holds no task and no
//! queue for a tenant that stopped writing; the tenant's next write starts a new loop.
//!
//! Landed detection. Neither a conflict answer (a stale `ETag`, or a create on an existing
//! pointer) nor a lost answer (any other error) proves that a swap did not land: when a request
//! is sent again after a first try that landed but whose answer was an error, the second try meets
//! the pointer the first one installed and answers a conflict. After either answer the
//! loop reloads the pointer and walks the lineage from the current snapshot object back to the one
//! its attempts read. Finding one of its own snapshot objects on that path means that attempt
//! landed, and the loop answers with the outcomes computed for it instead of applying the
//! operations again; not finding one means none landed, and the loop re-applies on the current
//! state. An attempt answered with a conflict and not on the path is deleted at once: its request
//! completed, so it can never land later. The check is exact, with no window of recent operations:
//! every swap installs a fresh snapshot name, so a pointer `ETag` never recurs, and an attempt
//! conditioned on an `ETag` the pointer has moved past can never land later. While the pointer has
//! not moved, the lost attempts and the next one are all conditioned on the same `ETag`, so at most
//! one of them lands. The path stays readable because garbage collection keeps a superseded
//! snapshot object until its successor is older than the longest scroll lifetime plus the query
//! deadline, longer than one write retries. A lineage frame missing on the path anyway ends the
//! write with `503`, nothing re-applied.
//!
//! Conflicts are retried with capped, jittered exponential backoff until the write deadline; the
//! answer is then `503`, never a version conflict, because the operation itself was never refused.
//!
//! Decoding a snapshot object, and copying, applying and encoding a snapshot for an attempt, run
//! on the blocking pool: each is linear in the snapshot, which `max_snapshot_bytes` lets grow
//! large, and on an async worker it would stall every other request that worker serves.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use icegate_common::TenantId;
use lru::LruCache;
use object_store::{GetOptions, GetResult, ObjectStoreExt as _, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use uuid::Uuid;

use super::client::SystemObjectStore;
use super::codec::{
    DecodedSnapshotObject, EncodedSnapshotObject, SnapshotLineage, decode_snapshot_object, encode_snapshot_object,
};
use super::snapshot::{SystemOperation, SystemOutcome, SystemSnapshot};
use crate::opensearch::error::OpenSearchError;

/// Prefix of every snapshot name: snapshot objects are `snap-<uuid>`.
const SNAPSHOT_NAME_PREFIX: &str = "snap-";
/// Upper bound of the first retry's sleep; each later retry doubles it, up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_millis(10);
/// Largest upper bound of one retry's sleep.
const MAX_BACKOFF: Duration = Duration::from_millis(500);

/// The bounds the store works within.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SystemIndexLimits {
    /// Largest decoded snapshot one tenant may hold; a write past it is refused and stores nothing.
    pub(crate) max_snapshot_bytes: usize,
    /// Bound of the decoded-snapshot cache, summed over the cached snapshots' decoded lengths.
    pub(crate) cache_max_bytes: usize,
    /// How long one batch of writes may retry before answering `503`: the query deadline.
    pub(crate) write_deadline: Duration,
}

/// A tenant's system-index state as one read saw it.
#[derive(Debug, Clone)]
pub(crate) struct CurrentSnapshot {
    /// Snapshot name of the snapshot object the pointer named; `None` for the empty state of an
    /// absent pointer. A write records it as the next snapshot object's predecessor.
    pub(crate) snapshot_name: Option<Arc<str>>,
    /// The snapshot, shared with the cache: a read never copies it.
    pub(crate) snapshot: Arc<SystemSnapshot>,
}

/// The pointer object's content.
#[derive(Debug, Serialize, Deserialize)]
struct Pointer {
    /// Snapshot name of the current snapshot object.
    snapshot_name: String,
}

/// What one replica knows of a tenant's pointer.
#[derive(Debug, Clone)]
struct PointerState {
    /// The pointer's `ETag`; `None` while the pointer is absent, so the next swap creates it.
    etag: Option<String>,
    /// The snapshot the pointer names.
    current_snapshot: CurrentSnapshot,
    /// Decoded length of `current_snapshot.snapshot`, which the cache is bounded by.
    decoded_len: usize,
}

impl PointerState {
    /// The state of a tenant without a pointer: no system index.
    fn from_absent_pointer() -> Self {
        Self {
            etag: None,
            current_snapshot: CurrentSnapshot {
                snapshot_name: None,
                snapshot: Arc::new(SystemSnapshot::default()),
            },
            decoded_len: 0,
        }
    }
}

/// The pointer states of the most recently used tenants, bounded by their total decoded length.
/// An absent pointer is never cached: there is nothing to revalidate.
struct SnapshotCache {
    /// The cached states, by tenant, in recency order.
    entries: LruCache<String, PointerState>,
    /// Sum of the cached states' decoded lengths.
    total_bytes: usize,
    /// Bound of `total_bytes` once a store has evicted what it must.
    max_bytes: usize,
}

impl SnapshotCache {
    /// An empty cache bounded by `max_bytes`.
    fn new(max_bytes: usize) -> Self {
        Self {
            entries: LruCache::unbounded(),
            total_bytes: 0,
            max_bytes,
        }
    }

    /// The cached state of `tenant`, marked most recently used.
    fn find_state(&mut self, tenant: &TenantId) -> Option<PointerState> {
        self.entries.get(tenant.as_ref()).cloned()
    }

    /// Cache `state` for `tenant`, then evict the least recently used tenants until the total fits.
    fn store_state(&mut self, tenant: &TenantId, state: PointerState) {
        let added_len = state.decoded_len;
        if let Some(replaced_state) = self.entries.put(tenant.as_ref().to_string(), state) {
            self.total_bytes = self.total_bytes.saturating_sub(replaced_state.decoded_len);
        }
        self.total_bytes = self.total_bytes.saturating_add(added_len);
        while self.total_bytes > self.max_bytes {
            let Some((_, evicted_state)) = self.entries.pop_lru() else {
                break;
            };
            self.total_bytes = self.total_bytes.saturating_sub(evicted_state.decoded_len);
        }
    }

    /// Forget `tenant`, so its next read reloads the pointer.
    fn remove_state(&mut self, tenant: &TenantId) {
        if let Some(removed_state) = self.entries.pop(tenant.as_ref()) {
            self.total_bytes = self.total_bytes.saturating_sub(removed_state.decoded_len);
        }
    }
}

/// An operation and the id it was given when queued; the id goes into the lineage of the snapshot
/// object that applies it.
#[derive(Debug, Clone)]
struct PendingOperation {
    /// Identifies the operation in lineages, across every attempt that applies it.
    id: Uuid,
    /// The operation to apply.
    operation: SystemOperation,
}

/// An operation queued to a tenant's write loop, and where its outcome goes.
struct QueuedOperation {
    /// The operation with its id.
    pending_operation: PendingOperation,
    /// Receives the operation's outcome.
    reply: oneshot::Sender<Result<SystemOutcome, OpenSearchError>>,
}

/// A snapshot object written for one swap, with the outcomes computed for it.
struct Attempt {
    /// The snapshot object's name.
    snapshot_name: Arc<str>,
    /// The snapshot the object holds.
    snapshot: Arc<SystemSnapshot>,
    /// Decoded length of `snapshot`.
    decoded_len: usize,
    /// One outcome per operation of the batch, in queue order.
    outcomes: Vec<Result<SystemOutcome, OpenSearchError>>,
}

impl Attempt {
    /// The pointer state once the pointer names this attempt under `etag`.
    fn to_pointer_state(&self, etag: String) -> PointerState {
        PointerState {
            etag: Some(etag),
            current_snapshot: CurrentSnapshot {
                snapshot_name: Some(Arc::clone(&self.snapshot_name)),
                snapshot: Arc::clone(&self.snapshot),
            },
            decoded_len: self.decoded_len,
        }
    }
}

/// The answer to one compare-and-swap of a pointer.
enum SwapAnswer {
    /// The pointer names the attempt's snapshot object; the new `ETag`, if one was returned.
    Swapped(Option<String>),
    /// Refused: the pointer was not the one the request expected. A request sent again after its
    /// first try landed is answered this way too, so only the lineage walk decides whether it
    /// landed.
    Conflict,
    /// Any other error: the swap may or may not have landed.
    Lost(object_store::Error),
}

/// A batch applied to a copy of the snapshot an attempt starts from.
enum PreparedBatch {
    /// Every operation was refused and no lost attempt may still land: the refusals stand on the
    /// snapshot as it is, without a swap.
    Refused(Vec<Result<SystemOutcome, OpenSearchError>>),
    /// The snapshot to swap in, with its outcomes and its encoded snapshot object.
    Encoded {
        /// The snapshot with the batch applied.
        snapshot: SystemSnapshot,
        /// One outcome per operation of the batch, in queue order.
        outcomes: Vec<Result<SystemOutcome, OpenSearchError>>,
        /// `snapshot` with its lineage, ready to store.
        encoded_object: EncodedSnapshotObject,
    },
}

/// Why a batch could not be swapped in as a whole.
enum BatchFailure {
    /// The batch would grow the snapshot past `max_snapshot_bytes`; nothing was stored.
    TooLarge,
    /// The write failed for every operation of the batch.
    Failed(OpenSearchError),
}

/// The per-tenant system-index store of one replica (see the module doc).
pub(crate) struct SystemIndexStore {
    /// State the store shares with its write loops.
    shared: Arc<StoreShared>,
}

/// What the store's handlers and its write loops share.
struct StoreShared {
    /// The object-store client and the tenants' paths.
    system_object_store: SystemObjectStore,
    /// The bounds the store works within.
    limits: SystemIndexLimits,
    /// The decoded snapshots of the most recently used tenants.
    cache: Mutex<SnapshotCache>,
    /// The sender of the write queue of each tenant whose write loop runs. A loop removes its own
    /// entry under this lock when it goes idle, so the map holds only tenants with a running loop.
    queue_senders: Mutex<HashMap<String, mpsc::UnboundedSender<QueuedOperation>>>,
}

impl SystemIndexStore {
    /// A store over `system_object_store` within `limits`. A tenant's write loop starts on its
    /// first write and ends once its queue is idle.
    pub(crate) fn new(system_object_store: SystemObjectStore, limits: SystemIndexLimits) -> Self {
        Self {
            shared: Arc::new(StoreShared {
                system_object_store,
                limits,
                cache: Mutex::new(SnapshotCache::new(limits.cache_max_bytes)),
                queue_senders: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// The tenant's current system-index state: the cached one when the pointer has not changed,
    /// the pointer's current snapshot otherwise, the empty state when it does not exist.
    ///
    /// # Errors
    ///
    /// Returns `500` when the object store fails, the pointer or a snapshot object is corrupt, or
    /// the pointer names a snapshot object that does not exist.
    pub(crate) async fn read(&self, tenant: &TenantId) -> Result<CurrentSnapshot, OpenSearchError> {
        Ok(self.shared.refresh_pointer_state(tenant).await?.current_snapshot)
    }

    /// Apply `operation` to the tenant's system indices and answer its outcome.
    ///
    /// The operation joins the tenant's write queue and is applied exactly once or not at all, also
    /// when a compare-and-swap answer is lost or another replica writes concurrently. Dropping the
    /// returned future does not withdraw a queued operation: it is still applied, and the tenant's
    /// later writes are served as if its caller had waited.
    ///
    /// # Errors
    ///
    /// Returns the operation's own refusal (such as `resource_already_exists_exception`); `413`
    /// when it would grow the decoded snapshot past `max_snapshot_bytes`; `503` when it could not
    /// be confirmed, or its snapshot object could not be stored, within the write deadline, or a
    /// lineage it needed was missing (its outcome is then unknown); `500` when the object store
    /// refused the snapshot object, answered a swap without an `ETag` or failed otherwise.
    pub(crate) async fn write(
        &self,
        tenant: &TenantId,
        operation: SystemOperation,
    ) -> Result<SystemOutcome, OpenSearchError> {
        let (reply, answer) = oneshot::channel();
        let mut queued_operation = QueuedOperation {
            pending_operation: PendingOperation {
                id: Uuid::new_v4(),
                operation,
            },
            reply,
        };
        // A loop that went idle between the lookup and the send has closed its queue and handed
        // the operation back; the next lookup finds no open queue and starts a new loop.
        while let Err(mpsc::error::SendError(returned_operation)) =
            self.find_queue_sender(tenant).send(queued_operation)
        {
            queued_operation = returned_operation;
        }
        answer
            .await
            .map_err(|_| OpenSearchError::internal("the system-index write loop dropped an operation"))?
    }

    /// The sender of `tenant`'s open write queue, starting a write loop when it has none.
    fn find_queue_sender(&self, tenant: &TenantId) -> mpsc::UnboundedSender<QueuedOperation> {
        let mut queue_senders = lock_mutex(&self.shared.queue_senders);
        if let Some(queue_sender) = queue_senders.get(tenant.as_ref())
            && !queue_sender.is_closed()
        {
            return queue_sender.clone();
        }
        let (queue_sender, queue_receiver) = mpsc::unbounded_channel();
        tokio::spawn(run_write_loop(
            Arc::clone(&self.shared),
            tenant.clone(),
            queue_sender.downgrade(),
            queue_receiver,
        ));
        queue_senders.insert(tenant.as_ref().to_string(), queue_sender.clone());
        queue_sender
    }
}

/// Lock `mutex`, recovering the data of a poisoned lock: no critical section of this module can
/// panic between two updates of the guarded data, so a poisoned lock still holds consistent data.
fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Drain the queue `queue_receiver` reads, swapping each run of queued operations in as one batch,
/// until the queue is empty after a batch. `own_queue_sender` is a weak handle on the queue's
/// sender, so the loop can tell its own entry in the sender map from a successor's without keeping
/// the queue open.
async fn run_write_loop(
    shared: Arc<StoreShared>,
    tenant: TenantId,
    own_queue_sender: mpsc::WeakUnboundedSender<QueuedOperation>,
    mut queue_receiver: mpsc::UnboundedReceiver<QueuedOperation>,
) {
    // The first `recv` waits only until the send of the lookup that spawned this loop arrives:
    // that lookup sends right after it. After `retire_idle_queue` closes the queue, `recv` yields
    // what was sent before the close and then `None`.
    while let Some(first_operation) = queue_receiver.recv().await {
        let mut batch = vec![first_operation];
        while let Ok(next_operation) = queue_receiver.try_recv() {
            batch.push(next_operation);
        }
        shared.write_batch(&tenant, batch).await;
        shared.retire_idle_queue(&tenant, &own_queue_sender, &mut queue_receiver);
    }
}

/// Send each operation's outcome. A caller that stopped waiting has nothing to be told: its
/// operation stands, and the loop goes on with the next one.
fn send_outcomes(
    replies: Vec<oneshot::Sender<Result<SystemOutcome, OpenSearchError>>>,
    outcomes: Vec<Result<SystemOutcome, OpenSearchError>>,
) {
    for (reply, outcome) in replies.into_iter().zip(outcomes) {
        let _ = reply.send(outcome);
    }
}

/// Send `error` to every operation of a batch whose caller still waits.
fn send_error_to_all(replies: Vec<oneshot::Sender<Result<SystemOutcome, OpenSearchError>>>, error: &OpenSearchError) {
    for reply in replies {
        let _ = reply.send(Err(error.clone()));
    }
}

/// Run `work` on the blocking pool (see the module doc). `task` names the work in the `500` a
/// failed join answers: the work panicked, or the runtime is shutting down.
async fn run_snapshot_work<T: Send + 'static>(
    task: &str,
    work: impl FnOnce() -> Result<T, OpenSearchError> + Send + 'static,
) -> Result<T, OpenSearchError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| OpenSearchError::internal(format!("{task} did not finish: {error}")))?
}

/// Apply `operations` to a copy of `base_snapshot` and encode the result with its lineage:
/// `predecessor` and `operation_ids`. When every operation is refused and `has_lost_attempts` is
/// false, nothing needs a swap, and the refusals are answered without encoding or building the
/// lineage.
///
/// # Errors
///
/// Returns `500` when the snapshot cannot be encoded.
fn prepare_batch(
    base_snapshot: &SystemSnapshot,
    operations: &[PendingOperation],
    predecessor: Option<&str>,
    operation_ids: &[Uuid],
    has_lost_attempts: bool,
) -> Result<PreparedBatch, OpenSearchError> {
    let mut snapshot = base_snapshot.clone();
    let outcomes: Vec<_> = operations
        .iter()
        .map(|pending_operation| snapshot.apply(&pending_operation.operation))
        .collect();
    if !has_lost_attempts && outcomes.iter().all(Result::is_err) {
        return Ok(PreparedBatch::Refused(outcomes));
    }
    let lineage = SnapshotLineage {
        predecessor: predecessor.map(str::to_string),
        operation_ids: operation_ids.to_vec(),
    };
    let encoded_object = encode_snapshot_object(&lineage, &snapshot)?;
    Ok(PreparedBatch::Encoded {
        snapshot,
        outcomes,
        encoded_object,
    })
}

/// Whether a failure to store an object may pass on a retry. `Generic` is the object store's
/// fallback for a failure it does not classify, a transport error among them; every classified
/// kind (a permission or authentication refusal, an unsupported operation, an invalid path) names
/// a condition the retry would meet again.
const fn is_transient_store_error(error: &object_store::Error) -> bool {
    matches!(error, object_store::Error::Generic { .. })
}

/// The bound below which retry `retry` (counted from 0) sleeps: [`FIRST_BACKOFF`], doubled per
/// retry, capped at [`MAX_BACKOFF`].
fn compute_backoff_bound(retry: u32) -> Duration {
    FIRST_BACKOFF.saturating_mul(2_u32.saturating_pow(retry)).min(MAX_BACKOFF)
}

/// Sleep before retry `retry`: uniform below [`compute_backoff_bound`], so replicas contending
/// for one pointer spread apart.
fn choose_backoff(retry: u32) -> Duration {
    let bound_micros = u64::try_from(compute_backoff_bound(retry).as_micros()).unwrap_or(u64::MAX);
    Duration::from_micros(rand::random_range(0..=bound_micros))
}

/// The `ETag` and snapshot name of a pointer GET's answer.
async fn decode_pointer_answer(result: GetResult) -> Result<(String, String), OpenSearchError> {
    let etag = result
        .meta
        .e_tag
        .clone()
        .ok_or_else(|| OpenSearchError::internal("the pointer was read without an ETag"))?;
    let bytes = result
        .bytes()
        .await
        .map_err(|error| OpenSearchError::internal(format!("cannot read the pointer: {error}")))?;
    let pointer: Pointer = serde_json::from_slice(&bytes)
        .map_err(|error| OpenSearchError::internal(format!("the pointer is corrupt: {error}")))?;
    Ok((etag, pointer.snapshot_name))
}

impl StoreShared {
    /// The cache, locked.
    fn lock_cache(&self) -> MutexGuard<'_, SnapshotCache> {
        lock_mutex(&self.cache)
    }

    /// When the queue `queue_receiver` reads is empty, close it, and remove the tenant's entry
    /// from the sender map if it is still `own_queue_sender`. Both happen under the map's lock,
    /// which every lookup takes, so no lookup hands out this queue afterwards. An operation sent
    /// between the emptiness check and the close stays buffered and is still received; a send
    /// after the close fails, and the writer's next lookup starts a new loop.
    fn retire_idle_queue(
        &self,
        tenant: &TenantId,
        own_queue_sender: &mpsc::WeakUnboundedSender<QueuedOperation>,
        queue_receiver: &mut mpsc::UnboundedReceiver<QueuedOperation>,
    ) {
        let mut queue_senders = lock_mutex(&self.queue_senders);
        if !queue_receiver.is_empty() {
            return;
        }
        queue_receiver.close();
        let is_own_entry = queue_senders
            .get(tenant.as_ref())
            .zip(own_queue_sender.upgrade())
            .is_some_and(|(entry_sender, own_sender)| entry_sender.same_channel(&own_sender));
        if is_own_entry {
            queue_senders.remove(tenant.as_ref());
        }
    }

    /// The tenant's pointer state, revalidating the cached one with a conditional GET.
    async fn refresh_pointer_state(&self, tenant: &TenantId) -> Result<PointerState, OpenSearchError> {
        let cached_state = self.lock_cache().find_state(tenant);
        let options = GetOptions {
            if_none_match: cached_state.as_ref().and_then(|state| state.etag.clone()),
            ..GetOptions::default()
        };
        let answer = self
            .system_object_store
            .object_store()
            .get_opts(&self.system_object_store.pointer_path(tenant), options)
            .await;
        let (etag, snapshot_name) = match answer {
            Err(object_store::Error::NotModified { .. }) => {
                return cached_state
                    .ok_or_else(|| OpenSearchError::internal("an unconditional pointer read answered not-modified"));
            }
            Err(object_store::Error::NotFound { .. }) => {
                self.lock_cache().remove_state(tenant);
                return Ok(PointerState::from_absent_pointer());
            }
            Err(error) => return Err(OpenSearchError::internal(format!("cannot read the pointer: {error}"))),
            Ok(result) => decode_pointer_answer(result).await?,
        };
        if let Some(state) =
            cached_state.filter(|state| state.current_snapshot.snapshot_name.as_deref() == Some(snapshot_name.as_str()))
        {
            let state = PointerState {
                etag: Some(etag),
                ..state
            };
            self.lock_cache().store_state(tenant, state.clone());
            return Ok(state);
        }
        self.load_pointer_state(tenant, Some((etag, snapshot_name))).await
    }

    /// The pointer as the store holds it now, without revalidation: its `ETag` and the snapshot
    /// name it names; `None` when it is absent.
    async fn read_pointer(&self, tenant: &TenantId) -> Result<Option<(String, String)>, OpenSearchError> {
        match self
            .system_object_store
            .object_store()
            .get(&self.system_object_store.pointer_path(tenant))
            .await
        {
            Ok(result) => decode_pointer_answer(result).await.map(Some),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(OpenSearchError::internal(format!("cannot read the pointer: {error}"))),
        }
    }

    /// The state `pointer` (its `ETag` and snapshot name, `None` when absent) names, loading its
    /// snapshot object; cached on success.
    async fn load_pointer_state(
        &self,
        tenant: &TenantId,
        pointer: Option<(String, String)>,
    ) -> Result<PointerState, OpenSearchError> {
        let Some((etag, snapshot_name)) = pointer else {
            self.lock_cache().remove_state(tenant);
            return Ok(PointerState::from_absent_pointer());
        };
        let decoded_object = self.load_snapshot_object(tenant, &snapshot_name).await?;
        let state = PointerState {
            etag: Some(etag),
            current_snapshot: CurrentSnapshot {
                snapshot_name: Some(Arc::from(snapshot_name)),
                snapshot: Arc::new(decoded_object.snapshot),
            },
            decoded_len: decoded_object.decoded_len,
        };
        self.lock_cache().store_state(tenant, state.clone());
        Ok(state)
    }

    /// The snapshot object `snapshot_name` of `tenant`, downloaded and decoded on the blocking
    /// pool.
    ///
    /// # Errors
    ///
    /// Returns `500` when the object is missing, cannot be read or is corrupt.
    async fn load_snapshot_object(
        &self,
        tenant: &TenantId,
        snapshot_name: &str,
    ) -> Result<DecodedSnapshotObject, OpenSearchError> {
        let path = self.system_object_store.snapshot_path(tenant, snapshot_name);
        let result = self.system_object_store.object_store().get(&path).await.map_err(|error| {
            OpenSearchError::internal(format!(
                "cannot read the snapshot object {path} the pointer names: {error}"
            ))
        })?;
        let bytes = result
            .bytes()
            .await
            .map_err(|error| OpenSearchError::internal(format!("cannot read the snapshot object {path}: {error}")))?;
        run_snapshot_work("decoding a system-index snapshot object", move || {
            decode_snapshot_object(&bytes)
        })
        .await
    }

    /// Swap the batch in, answering every operation. A batch too large as a whole is split, so
    /// only the operations that do not fit are refused.
    async fn write_batch(&self, tenant: &TenantId, batch: Vec<QueuedOperation>) {
        let (operations, replies): (Vec<PendingOperation>, Vec<_>) = batch
            .into_iter()
            .map(|queued_operation| (queued_operation.pending_operation, queued_operation.reply))
            .unzip();
        // Shared with the blocking task of every attempt, which therefore copies no operation.
        let operations: Arc<[PendingOperation]> = Arc::from(operations);
        match self.swap_in_operations(tenant, &operations).await {
            Ok(outcomes) => send_outcomes(replies, outcomes),
            Err(BatchFailure::TooLarge) if operations.len() > 1 => {
                for (pending_operation, reply) in operations.iter().zip(replies) {
                    // Copied out of the batch: only a batch refused as a whole is split, so the
                    // writes that fit never pay for the copy.
                    let single_operation: Arc<[PendingOperation]> = Arc::from([pending_operation.clone()]);
                    let outcome = match self.swap_in_operations(tenant, &single_operation).await {
                        Ok(mut outcomes) => outcomes.pop().unwrap_or_else(|| {
                            Err(OpenSearchError::internal("a one-operation batch produced no outcome"))
                        }),
                        Err(BatchFailure::TooLarge) => {
                            Err(OpenSearchError::snapshot_too_large(self.limits.max_snapshot_bytes))
                        }
                        Err(BatchFailure::Failed(error)) => Err(error),
                    };
                    let _ = reply.send(outcome);
                }
            }
            Err(BatchFailure::TooLarge) => {
                send_error_to_all(
                    replies,
                    &OpenSearchError::snapshot_too_large(self.limits.max_snapshot_bytes),
                );
            }
            Err(BatchFailure::Failed(error)) => send_error_to_all(replies, &error),
        }
    }

    /// Swap `operations` in as one snapshot object (see the module doc), re-applying them after
    /// every conflict or lost answer that did not land, until the write deadline.
    async fn swap_in_operations(
        &self,
        tenant: &TenantId,
        operations: &Arc<[PendingOperation]>,
    ) -> Result<Vec<Result<SystemOutcome, OpenSearchError>>, BatchFailure> {
        let deadline = Instant::now() + self.limits.write_deadline;
        // Shared with the blocking task of every attempt; only an attempt that encodes copies it.
        let operation_ids: Arc<[Uuid]> = operations.iter().map(|pending_operation| pending_operation.id).collect();
        let mut base_state = self.refresh_pointer_state(tenant).await.map_err(BatchFailure::Failed)?;
        // Attempts conditioned on `base_state` whose swap answer was lost: any of them may still
        // land.
        let mut lost_attempts: Vec<Attempt> = Vec::new();
        let mut retry: u32 = 0;
        loop {
            let predecessor = base_state.current_snapshot.snapshot_name.clone();
            let base_snapshot = Arc::clone(&base_state.current_snapshot.snapshot);
            let batch_operations = Arc::clone(operations);
            let batch_operation_ids = Arc::clone(&operation_ids);
            let has_lost_attempts = !lost_attempts.is_empty();
            let prepared_batch = run_snapshot_work("applying a system-index batch", move || {
                prepare_batch(
                    &base_snapshot,
                    &batch_operations,
                    predecessor.as_deref(),
                    &batch_operation_ids,
                    has_lost_attempts,
                )
            })
            .await
            .map_err(BatchFailure::Failed)?;
            let (working_snapshot, outcomes, encoded_object) = match prepared_batch {
                // Nothing changes: the refusals stand on the state just read, without a swap.
                PreparedBatch::Refused(outcomes) => return Ok(outcomes),
                PreparedBatch::Encoded {
                    snapshot,
                    outcomes,
                    encoded_object,
                } => (snapshot, outcomes, encoded_object),
            };
            if encoded_object.decoded_len > self.limits.max_snapshot_bytes {
                return Err(BatchFailure::TooLarge);
            }
            let snapshot_name: Arc<str> = Arc::from(format!("{SNAPSHOT_NAME_PREFIX}{}", Uuid::new_v4()));
            self.put_snapshot_object(tenant, &snapshot_name, encoded_object.bytes, deadline)
                .await?;
            let attempt = Attempt {
                snapshot_name,
                snapshot: Arc::new(working_snapshot),
                decoded_len: encoded_object.decoded_len,
                outcomes,
            };
            let answer = self
                .swap_pointer(tenant, base_state.etag.as_deref(), &attempt.snapshot_name)
                .await
                .map_err(BatchFailure::Failed)?;
            let conflicted_attempt = match answer {
                SwapAnswer::Swapped(etag) => return self.finish_swap(tenant, attempt, etag, &lost_attempts).await,
                // Not deleted yet: the conflict may answer a retry of a try that landed.
                SwapAnswer::Conflict => Some(attempt),
                SwapAnswer::Lost(error) => {
                    tracing::warn!(%tenant, %error, "a system-index pointer swap lost its answer; checking whether it landed");
                    lost_attempts.push(attempt);
                    None
                }
            };
            if let Some(outcomes) = self
                .reconcile_attempts(
                    tenant,
                    &mut base_state,
                    &mut lost_attempts,
                    conflicted_attempt,
                    &operation_ids,
                )
                .await?
            {
                return Ok(outcomes);
            }
            if Instant::now() >= deadline {
                return Err(BatchFailure::Failed(OpenSearchError::write_contention()));
            }
            tokio::time::sleep(choose_backoff(retry)).await;
            retry = retry.saturating_add(1);
        }
    }

    /// Settle a swap that landed. The lost attempts were conditioned on the `ETag` it replaced, so
    /// none of them can land any more and their objects are deleted. The new state is cached; a
    /// swap answered without an `ETag` cannot be revalidated, so it fails the write instead.
    async fn finish_swap(
        &self,
        tenant: &TenantId,
        attempt: Attempt,
        etag: Option<String>,
        lost_attempts: &[Attempt],
    ) -> Result<Vec<Result<SystemOutcome, OpenSearchError>>, BatchFailure> {
        self.delete_attempts(tenant, lost_attempts).await;
        let Some(etag) = etag else {
            self.lock_cache().remove_state(tenant);
            return Err(BatchFailure::Failed(OpenSearchError::internal(
                "the object store answered a pointer swap without an ETag",
            )));
        };
        self.lock_cache().store_state(tenant, attempt.to_pointer_state(etag));
        Ok(attempt.outcomes)
    }

    /// After a conflict or a lost answer: reload the pointer and walk its lineage back to
    /// `base_state`. An attempt found on the way, lost or `conflicted_attempt`, landed, and its
    /// outcomes are the answer. Otherwise `conflicted_attempt` is deleted (its request completed,
    /// so it can never land later), and when the pointer moved the lost attempts can no longer
    /// land either: they are deleted and `base_state` becomes the current state. `None` tells the
    /// caller to re-apply. When the pointer cannot be reloaded, `conflicted_attempt` is kept with
    /// the lost ones (whether it landed is still unknown) and the write is retried.
    async fn reconcile_attempts(
        &self,
        tenant: &TenantId,
        base_state: &mut PointerState,
        lost_attempts: &mut Vec<Attempt>,
        conflicted_attempt: Option<Attempt>,
        operation_ids: &[Uuid],
    ) -> Result<Option<Vec<Result<SystemOutcome, OpenSearchError>>>, BatchFailure> {
        self.lock_cache().remove_state(tenant);
        // The walk looks for the conflicted attempt among the candidates; it is the last one.
        let has_conflicted_attempt = conflicted_attempt.is_some();
        lost_attempts.extend(conflicted_attempt);
        let current_pointer = match self.read_pointer(tenant).await {
            Ok(current_pointer) => current_pointer,
            Err(error) => {
                tracing::warn!(%tenant, ?error, "the system-index pointer could not be reloaded; retrying");
                return Ok(None);
            }
        };
        let current_name = current_pointer.as_ref().map(|(_, snapshot_name)| snapshot_name.as_str());
        let read_name = base_state.current_snapshot.snapshot_name.as_deref();
        let has_moved = current_name != read_name;
        let landed_position = self
            .find_landed_attempt(tenant, current_name, read_name, lost_attempts, operation_ids)
            .await
            .map_err(BatchFailure::Failed)?;
        if let Some(position) = landed_position {
            let landed_attempt = lost_attempts.swap_remove(position);
            self.delete_attempts(tenant, lost_attempts).await;
            if let Some((etag, snapshot_name)) = current_pointer
                && *snapshot_name == *landed_attempt.snapshot_name
            {
                self.lock_cache().store_state(tenant, landed_attempt.to_pointer_state(etag));
            }
            return Ok(Some(landed_attempt.outcomes));
        }
        if has_moved {
            self.delete_attempts(tenant, lost_attempts).await;
            lost_attempts.clear();
            *base_state = self
                .load_pointer_state(tenant, current_pointer)
                .await
                .map_err(BatchFailure::Failed)?;
        } else if has_conflicted_attempt && let Some(refused_attempt) = lost_attempts.pop() {
            self.delete_snapshot_object(tenant, &refused_attempt.snapshot_name).await;
        }
        Ok(None)
    }

    /// Store a snapshot object, retrying transient failures (see [`is_transient_store_error`])
    /// until `deadline`. Its name is fresh, so a retry overwrites nothing anyone reads.
    ///
    /// # Errors
    ///
    /// Returns `503` when a transient failure lasts until `deadline`, and `500` at once on any
    /// other failure; the pointer is not swapped for this attempt in either case.
    async fn put_snapshot_object(
        &self,
        tenant: &TenantId,
        snapshot_name: &str,
        bytes: Bytes,
        deadline: Instant,
    ) -> Result<(), BatchFailure> {
        let path = self.system_object_store.snapshot_path(tenant, snapshot_name);
        let mut retry: u32 = 0;
        loop {
            match self.system_object_store.object_store().put(&path, bytes.clone().into()).await {
                Ok(_) => return Ok(()),
                Err(error) if !is_transient_store_error(&error) => {
                    return Err(BatchFailure::Failed(OpenSearchError::internal(format!(
                        "the object store refused the system-index snapshot object {path}: {error}"
                    ))));
                }
                Err(error) if Instant::now() < deadline => {
                    tracing::warn!(%tenant, %error, "a system-index snapshot object could not be stored; retrying");
                    tokio::time::sleep(choose_backoff(retry)).await;
                    retry = retry.saturating_add(1);
                }
                Err(error) => {
                    tracing::warn!(%tenant, %error, "a system-index snapshot object could not be stored before the deadline");
                    return Err(BatchFailure::Failed(OpenSearchError::write_contention()));
                }
            }
        }
    }

    /// Compare-and-swap the tenant's pointer to `snapshot_name`, conditioned on `expected_etag`
    /// (`None`: the pointer must not exist). A stale update and a create on an existing pointer
    /// are both conflicts, the classification the S3 catalog uses.
    async fn swap_pointer(
        &self,
        tenant: &TenantId,
        expected_etag: Option<&str>,
        snapshot_name: &str,
    ) -> Result<SwapAnswer, OpenSearchError> {
        let pointer = serde_json::to_vec(&Pointer {
            snapshot_name: snapshot_name.to_string(),
        })
        .map_err(|error| OpenSearchError::internal(format!("cannot encode a pointer: {error}")))?;
        let mode = expected_etag.map_or(PutMode::Create, |etag| {
            PutMode::Update(UpdateVersion {
                e_tag: Some(etag.to_string()),
                version: None,
            })
        });
        let options = PutOptions {
            mode,
            ..PutOptions::default()
        };
        let answer = self
            .system_object_store
            .object_store()
            .put_opts(
                &self.system_object_store.pointer_path(tenant),
                PutPayload::from(pointer),
                options,
            )
            .await;
        Ok(match answer {
            Ok(result) => SwapAnswer::Swapped(result.e_tag),
            Err(object_store::Error::Precondition { .. } | object_store::Error::AlreadyExists { .. }) => {
                SwapAnswer::Conflict
            }
            Err(error) => SwapAnswer::Lost(error),
        })
    }

    /// Walk the lineage from `current_name` back to `read_name`, the snapshot the lost attempts
    /// were applied to; the position of the lost attempt found on the way, if any.
    ///
    /// # Errors
    ///
    /// Returns `503` when a lineage frame on the path is missing, and `500` when the store fails,
    /// a frame is corrupt, or this batch's operations appear in a snapshot object no lost attempt
    /// wrote.
    async fn find_landed_attempt(
        &self,
        tenant: &TenantId,
        current_name: Option<&str>,
        read_name: Option<&str>,
        lost_attempts: &[Attempt],
        operation_ids: &[Uuid],
    ) -> Result<Option<usize>, OpenSearchError> {
        let mut cursor = current_name.map(str::to_string);
        while let Some(snapshot_name) = cursor {
            if Some(snapshot_name.as_str()) == read_name {
                return Ok(None);
            }
            if let Some(position) = lost_attempts
                .iter()
                .position(|attempt| *attempt.snapshot_name == *snapshot_name)
            {
                return Ok(Some(position));
            }
            let lineage = self
                .system_object_store
                .read_lineage_frame(tenant, &snapshot_name)
                .await?
                .ok_or_else(OpenSearchError::write_contention)?;
            if lineage.operation_ids.iter().any(|id| operation_ids.contains(id)) {
                return Err(OpenSearchError::internal(format!(
                    "snapshot object {snapshot_name} applied operations of a write that did not store it"
                )));
            }
            cursor = lineage.predecessor;
        }
        Ok(None)
    }

    /// Delete the snapshot objects of attempts that can no longer land.
    async fn delete_attempts(&self, tenant: &TenantId, attempts: &[Attempt]) {
        for attempt in attempts {
            self.delete_snapshot_object(tenant, &attempt.snapshot_name).await;
        }
    }

    /// Delete one snapshot object. Not-found counts as deleted. Any other failure is logged and
    /// leaves the object behind: nothing names it, so nothing reads it.
    async fn delete_snapshot_object(&self, tenant: &TenantId, snapshot_name: &str) {
        let path = self.system_object_store.snapshot_path(tenant, snapshot_name);
        match self.system_object_store.object_store().delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => {
                tracing::warn!(%tenant, %error, %path, "an unused system-index snapshot object could not be deleted");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fmt;

    use async_trait::async_trait;
    use axum::http::StatusCode;
    use futures::TryStreamExt as _;
    use futures::stream::BoxStream;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutResult,
    };
    use serde_json::json;
    use tokio::sync::Notify;

    use super::*;
    use crate::opensearch::error::render_envelope;

    const LOCATION: &str = "s3://opensearch-test/opensearch-system";
    const TENANT_A: &str = "tenant-a";
    const TENANT_B: &str = "tenant-b";
    /// Bound on every wait for the store or the double.
    const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

    /// A point the double holds one request at until the test releases it.
    #[derive(Debug, Clone, Default)]
    struct Gate {
        arrival: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl Gate {
        /// Wait until the double holds a request here.
        async fn wait_for_request(&self) {
            tokio::time::timeout(WAIT_TIMEOUT, self.arrival.notified())
                .await
                .expect("the double reaches the gate");
        }

        /// Let the held request continue.
        fn release_request(&self) {
            self.release.notify_one();
        }

        /// Hold the calling request until the test releases it.
        async fn hold_request(&self) {
            self.arrival.notify_one();
            self.release.notified().await;
        }
    }

    /// What the double does with the next compare-and-swap of a pointer.
    #[derive(Debug)]
    enum PointerSwapFault {
        /// Refuse it as the store refuses a stale update (`Precondition`) or a create on an
        /// existing pointer (`AlreadyExists`), changing nothing.
        Conflict,
        /// Apply it, then refuse it as [`Self::Conflict`] does: a request sent again after its
        /// first try landed with an error answer, whose second try met the pointer the first one
        /// installed.
        ApplyThenConflict,
        /// Apply it, then answer with a transient error, after the gate releases it when there is
        /// one: a lost acknowledgement.
        LoseAnswer(Option<Gate>),
        /// Answer with a transient error after the gate releases it, without applying it: a
        /// request lost on its way.
        LoseRequest(Gate),
        /// Answer with a transient error without applying it, and keep the request to apply later:
        /// a request that arrives late.
        Delay,
        /// Apply the request [`Self::Delay`] kept, then this one: the late request lands first.
        ApplyDelayedFirst,
        /// Apply it and answer without an `ETag`.
        OmitEtag,
        /// Hold it at the gate, then apply it.
        Pause(Gate),
    }

    /// What the double does with the next read of a pointer.
    #[derive(Debug, Clone, Copy)]
    enum PointerReadFault {
        /// Answer with a transient error without reading.
        Fail,
        /// Serve it without the pointer's `ETag`.
        OmitEtag,
    }

    /// How the double fails a snapshot-object put.
    #[derive(Debug, Clone, Copy)]
    enum SnapshotPutFailure {
        /// A transient error (`Generic`), as a lost connection surfaces.
        Transient,
        /// A permission refusal, which every retry would meet again.
        Refused,
    }

    /// One read the double served.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedRead {
        path: String,
        is_conditional: bool,
        is_range: bool,
        is_not_modified: bool,
    }

    /// An object store over `InMemory` that injects pointer faults and snapshot-object put
    /// failures, and records reads.
    #[derive(Debug)]
    struct ScriptedObjectStore {
        memory_store: InMemory,
        swap_faults: Mutex<VecDeque<PointerSwapFault>>,
        /// One entry per upcoming pointer read: `None` serves it.
        read_faults: Mutex<VecDeque<Option<PointerReadFault>>>,
        /// How the upcoming snapshot-object puts fail, and how many of them.
        snapshot_put_failures: Mutex<(SnapshotPutFailure, usize)>,
        /// How many snapshot-object puts the double received.
        snapshot_put_count: Mutex<usize>,
        delayed_request: Mutex<Option<(Path, PutPayload, PutOptions)>>,
        reads: Mutex<Vec<RecordedRead>>,
        pointer_swaps: Mutex<Vec<&'static str>>,
    }

    impl ScriptedObjectStore {
        const fn new(memory_store: InMemory) -> Self {
            Self {
                memory_store,
                swap_faults: Mutex::new(VecDeque::new()),
                read_faults: Mutex::new(VecDeque::new()),
                snapshot_put_failures: Mutex::new((SnapshotPutFailure::Transient, 0)),
                snapshot_put_count: Mutex::new(0),
                delayed_request: Mutex::new(None),
                reads: Mutex::new(Vec::new()),
                pointer_swaps: Mutex::new(Vec::new()),
            }
        }

        fn push_swap_faults(&self, faults: impl IntoIterator<Item = PointerSwapFault>) {
            self.swap_faults.lock().expect("swap faults").extend(faults);
        }

        fn push_read_faults(&self, faults: impl IntoIterator<Item = Option<PointerReadFault>>) {
            self.read_faults.lock().expect("read faults").extend(faults);
        }

        fn fail_snapshot_puts(&self, failure: SnapshotPutFailure, failure_count: usize) {
            *self.snapshot_put_failures.lock().expect("put failures") = (failure, failure_count);
        }

        fn count_snapshot_puts(&self) -> usize {
            *self.snapshot_put_count.lock().expect("put count")
        }

        /// The mode of every pointer swap the double received, in order.
        fn list_pointer_swaps(&self) -> Vec<&'static str> {
            self.pointer_swaps.lock().expect("swap log").clone()
        }

        fn list_reads(&self) -> Vec<RecordedRead> {
            self.reads.lock().expect("read log").clone()
        }

        fn clear_reads(&self) {
            self.reads.lock().expect("read log").clear();
        }

        /// Apply the request [`PointerSwapFault::Delay`] kept, as it would land arriving late.
        async fn apply_delayed_request(&self) -> object_store::Result<PutResult> {
            let (location, payload, options) = self
                .delayed_request
                .lock()
                .expect("delayed request")
                .take()
                .expect("a delayed request is kept");
            self.memory_store.put_opts(&location, payload, options).await
        }

        /// How the next snapshot-object put fails, if it does, consuming one scripted failure.
        fn take_snapshot_put_failure(&self) -> Option<SnapshotPutFailure> {
            *self.snapshot_put_count.lock().expect("put count") += 1;
            let mut snapshot_put_failures = self.snapshot_put_failures.lock().expect("put failures");
            let (failure, failure_count) = *snapshot_put_failures;
            *snapshot_put_failures = (failure, failure_count.saturating_sub(1));
            drop(snapshot_put_failures);
            (failure_count > 0).then_some(failure)
        }

        /// Write `body` as the pointer of `tenant`, bypassing the store.
        async fn put_raw_pointer(&self, tenant: &str, body: &'static [u8]) {
            self.memory_store
                .put_opts(
                    &Path::from(format!("opensearch-system/{tenant}/current")),
                    PutPayload::from_static(body),
                    PutOptions::default(),
                )
                .await
                .expect("a raw pointer");
        }

        /// The snapshot names of every snapshot object stored for `tenant`, sorted.
        async fn list_snapshot_names(&self, tenant: &str) -> Vec<String> {
            let prefix = Path::from(format!("opensearch-system/{tenant}"));
            let mut names: Vec<String> = self
                .memory_store
                .list(Some(&prefix))
                .map_ok(|meta| meta.location.filename().unwrap_or_default().to_string())
                .try_collect::<Vec<String>>()
                .await
                .expect("list")
                .into_iter()
                .filter(|name| name.starts_with(SNAPSHOT_NAME_PREFIX))
                .collect();
            names.sort();
            names
        }
    }

    impl fmt::Display for ScriptedObjectStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("ScriptedObjectStore")
        }
    }

    /// The refusal the store answers a conflicting swap with.
    fn build_swap_refusal(location: &Path, mode: &PutMode) -> object_store::Error {
        let path = location.to_string();
        let source = "the pointer is not the one the swap expected".into();
        match mode {
            PutMode::Create => object_store::Error::AlreadyExists { path, source },
            PutMode::Overwrite | PutMode::Update(_) => object_store::Error::Precondition { path, source },
        }
    }

    /// The transient error a lost answer surfaces as.
    fn build_lost_answer() -> object_store::Error {
        object_store::Error::Generic {
            store: "ScriptedObjectStore",
            source: "the answer was lost".into(),
        }
    }

    /// The refusal of a write the credentials do not allow.
    fn build_permission_refusal(location: &Path) -> object_store::Error {
        object_store::Error::PermissionDenied {
            path: location.to_string(),
            source: "the credentials may not write here".into(),
        }
    }

    #[async_trait]
    impl ObjectStore for ScriptedObjectStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<PutResult> {
            if location.filename() != Some("current") {
                return match self.take_snapshot_put_failure() {
                    None => self.memory_store.put_opts(location, payload, options).await,
                    Some(SnapshotPutFailure::Transient) => Err(build_lost_answer()),
                    Some(SnapshotPutFailure::Refused) => Err(build_permission_refusal(location)),
                };
            }
            let mode = if matches!(options.mode, PutMode::Create) {
                "create"
            } else {
                "update"
            };
            self.pointer_swaps.lock().expect("swap log").push(mode);
            let fault = self.swap_faults.lock().expect("swap faults").pop_front();
            match fault {
                None => self.memory_store.put_opts(location, payload, options).await,
                Some(PointerSwapFault::Conflict) => Err(build_swap_refusal(location, &options.mode)),
                Some(PointerSwapFault::ApplyThenConflict) => {
                    let mode = options.mode.clone();
                    self.memory_store.put_opts(location, payload, options).await?;
                    Err(build_swap_refusal(location, &mode))
                }
                Some(PointerSwapFault::LoseAnswer(gate)) => {
                    self.memory_store.put_opts(location, payload, options).await?;
                    if let Some(gate) = gate {
                        gate.hold_request().await;
                    }
                    Err(build_lost_answer())
                }
                Some(PointerSwapFault::LoseRequest(gate)) => {
                    gate.hold_request().await;
                    Err(build_lost_answer())
                }
                Some(PointerSwapFault::Delay) => {
                    *self.delayed_request.lock().expect("delayed request") = Some((location.clone(), payload, options));
                    Err(build_lost_answer())
                }
                Some(PointerSwapFault::ApplyDelayedFirst) => {
                    self.apply_delayed_request().await?;
                    self.memory_store.put_opts(location, payload, options).await
                }
                Some(PointerSwapFault::OmitEtag) => {
                    let put_result = self.memory_store.put_opts(location, payload, options).await?;
                    Ok(PutResult {
                        e_tag: None,
                        version: put_result.version,
                    })
                }
                Some(PointerSwapFault::Pause(gate)) => {
                    gate.hold_request().await;
                    self.memory_store.put_opts(location, payload, options).await
                }
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.memory_store.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
            let is_conditional = options.if_none_match.is_some();
            let is_range = options.range.is_some();
            let read_fault = if location.filename() == Some("current") {
                self.read_faults.lock().expect("read faults").pop_front().flatten()
            } else {
                None
            };
            let result = match read_fault {
                None => self.memory_store.get_opts(location, options).await,
                Some(PointerReadFault::Fail) => Err(build_lost_answer()),
                Some(PointerReadFault::OmitEtag) => {
                    self.memory_store.get_opts(location, options).await.map(|mut get_result| {
                        get_result.meta.e_tag = None;
                        get_result
                    })
                }
            };
            self.reads.lock().expect("read log").push(RecordedRead {
                path: location.to_string(),
                is_conditional,
                is_range,
                is_not_modified: matches!(result, Err(object_store::Error::NotModified { .. })),
            });
            result
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.memory_store.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.memory_store.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
            self.memory_store.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> object_store::Result<()> {
            self.memory_store.copy_opts(from, to, options).await
        }
    }

    const fn build_limits() -> SystemIndexLimits {
        SystemIndexLimits {
            max_snapshot_bytes: 1 << 20,
            cache_max_bytes: 4 << 20,
            write_deadline: Duration::from_secs(30),
        }
    }

    fn build_tenant(id: &str) -> TenantId {
        TenantId::new(id).expect("a valid tenant")
    }

    fn build_index_creation(name: &str) -> SystemOperation {
        SystemOperation::CreateIndex {
            name: name.to_string(),
            mappings: json!({"dynamic": "strict"}),
            settings: json!({"number_of_shards": 1}),
        }
    }

    fn build_creation_outcome(name: &str) -> SystemOutcome {
        SystemOutcome::IndexCreated { name: name.to_string() }
    }

    /// A replica whose object store is the double, and a second replica over the same objects
    /// without faults.
    fn build_replicas(
        limits: SystemIndexLimits,
    ) -> (Arc<ScriptedObjectStore>, Arc<SystemIndexStore>, Arc<SystemIndexStore>) {
        let memory_store = InMemory::new();
        let double = Arc::new(ScriptedObjectStore::new(memory_store.clone()));
        let scripted_objects: Arc<dyn ObjectStore> = Arc::clone(&double) as Arc<dyn ObjectStore>;
        let scripted_replica = SystemIndexStore::new(
            SystemObjectStore::new(scripted_objects, LOCATION).expect("an S3 location"),
            limits,
        );
        let other_replica = SystemIndexStore::new(
            SystemObjectStore::new(Arc::new(memory_store), LOCATION).expect("an S3 location"),
            limits,
        );
        (double, Arc::new(scripted_replica), Arc::new(other_replica))
    }

    /// The decoded length of the snapshot holding the indices `names`, computed by the codec.
    fn measure_snapshot_with_indices(names: &[&str]) -> usize {
        let mut snapshot = SystemSnapshot::default();
        for name in names {
            snapshot.apply(&build_index_creation(name)).expect("create");
        }
        let lineage = SnapshotLineage {
            predecessor: None,
            operation_ids: Vec::new(),
        };
        encode_snapshot_object(&lineage, &snapshot).expect("encode").decoded_len
    }

    async fn assert_error(error: OpenSearchError, status: StatusCode, error_type: &str) {
        let (rendered_status, body) = render_envelope(error).await;
        assert_eq!(rendered_status, status, "{body}");
        assert_eq!(body["error"]["type"], error_type, "{body}");
    }

    /// Spawn `operation` on `store` for `tenant`, so the test can act while the write is in flight.
    fn spawn_write(
        store: &Arc<SystemIndexStore>,
        tenant: &TenantId,
        operation: SystemOperation,
    ) -> tokio::task::JoinHandle<Result<SystemOutcome, OpenSearchError>> {
        let store = Arc::clone(store);
        let tenant = tenant.clone();
        tokio::spawn(async move { store.write(&tenant, operation).await })
    }

    async fn join_write(
        pending_write: tokio::task::JoinHandle<Result<SystemOutcome, OpenSearchError>>,
    ) -> Result<SystemOutcome, OpenSearchError> {
        tokio::time::timeout(WAIT_TIMEOUT, pending_write)
            .await
            .expect("the write finishes")
            .expect("the write task does not panic")
    }

    #[tokio::test(start_paused = true)]
    async fn a_create_on_an_existing_pointer_is_a_conflict_like_a_stale_update() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);

        double.push_swap_faults([PointerSwapFault::Conflict]);
        let create_outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_1")).await;
        double.push_swap_faults([PointerSwapFault::Conflict]);
        let update_outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_2")).await;

        assert_eq!(
            create_outcome.expect("created after one refused create"),
            build_creation_outcome(".kibana_1")
        );
        assert_eq!(
            update_outcome.expect("created after one refused update"),
            build_creation_outcome(".kibana_2")
        );
        assert_eq!(double.list_pointer_swaps(), ["create", "create", "update", "update"]);
        let current_snapshot = scripted_replica.read(&tenant).await.expect("read");
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
        assert!(current_snapshot.snapshot.find_index(".kibana_2").is_some());
        assert_eq!(
            double.list_snapshot_names(TENANT_A).await.len(),
            2,
            "refused attempts delete their objects"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_stale_update_is_reloaded_and_reapplied_until_it_lands() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        scripted_replica
            .write(&tenant, build_index_creation(".kibana_1"))
            .await
            .expect("first write");

        double.push_swap_faults([
            PointerSwapFault::Conflict,
            PointerSwapFault::Conflict,
            PointerSwapFault::Conflict,
        ]);
        let outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_2")).await;

        assert_eq!(
            outcome.expect("lands after three conflicts"),
            build_creation_outcome(".kibana_2")
        );
        assert_eq!(
            double.list_pointer_swaps(),
            ["create", "update", "update", "update", "update"]
        );
        assert_eq!(double.list_snapshot_names(TENANT_A).await.len(), 2);
    }

    /// A re-application would answer `resource_already_exists_exception`; answering the outcome
    /// computed for the landed attempt proves the operation was applied once.
    #[tokio::test]
    async fn an_applied_write_whose_answer_was_lost_is_not_applied_twice() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        double.push_swap_faults([PointerSwapFault::LoseAnswer(None)]);

        let outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_1")).await;

        assert_eq!(
            outcome.expect("detected as landed"),
            build_creation_outcome(".kibana_1")
        );
        assert_eq!(double.list_pointer_swaps(), ["create"]);
        assert_eq!(double.list_snapshot_names(TENANT_A).await.len(), 1);
    }

    /// The conflict answers a request sent again after its first try installed the attempt, on a
    /// create (`AlreadyExists`) and on an update (`Precondition`). A re-application would answer
    /// `resource_already_exists_exception`; deleting the attempt's object would leave the pointer
    /// naming a missing object, which the final read would answer with a `500`.
    #[tokio::test]
    async fn a_conflict_answered_for_a_swap_that_landed_is_detected_as_landed() {
        let (double, scripted_replica, other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);

        double.push_swap_faults([PointerSwapFault::ApplyThenConflict]);
        let create_outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_1")).await;

        assert_eq!(
            create_outcome.expect("the landed create is detected"),
            build_creation_outcome(".kibana_1")
        );
        assert_eq!(double.list_pointer_swaps(), ["create"]);
        assert_eq!(double.list_snapshot_names(TENANT_A).await.len(), 1);

        double.push_swap_faults([PointerSwapFault::ApplyThenConflict]);
        let update_outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_2")).await;

        assert_eq!(
            update_outcome.expect("the landed update is detected"),
            build_creation_outcome(".kibana_2")
        );
        assert_eq!(double.list_pointer_swaps(), ["create", "update"]);
        assert_eq!(double.list_snapshot_names(TENANT_A).await.len(), 2);
        // The other replica has cached nothing, so it loads the snapshot object the pointer names.
        let current_snapshot = other_replica.read(&tenant).await.expect("the pointer names an existing object");
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
        assert!(current_snapshot.snapshot.find_index(".kibana_2").is_some());
    }

    #[tokio::test]
    async fn a_lost_acknowledgement_while_another_replica_advances_is_detected_as_landed() {
        let (double, scripted_replica, other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        let gate = Gate::default();
        double.push_swap_faults([PointerSwapFault::LoseAnswer(Some(gate.clone()))]);

        let pending_write = spawn_write(&scripted_replica, &tenant, build_index_creation(".kibana_1"));
        gate.wait_for_request().await;
        other_replica
            .write(&tenant, build_index_creation(".other"))
            .await
            .expect("the other replica writes after the swap landed");
        gate.release_request();
        let outcome = join_write(pending_write).await;

        assert_eq!(
            outcome.expect("detected as landed"),
            build_creation_outcome(".kibana_1")
        );
        assert_eq!(double.list_pointer_swaps(), ["create"]);
        let current_snapshot = other_replica.read(&tenant).await.expect("read");
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
        assert!(current_snapshot.snapshot.find_index(".other").is_some());
    }

    /// No window of recent operations could hold the attempt after 200 later swaps; the walk
    /// reads each of their lineage frames with two ranged GETs and no snapshot frame at all.
    #[tokio::test]
    async fn a_lost_acknowledgement_followed_by_two_hundred_other_swaps_is_detected_as_landed_by_its_lineage() {
        let (double, scripted_replica, other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        let gate = Gate::default();
        double.push_swap_faults([PointerSwapFault::LoseAnswer(Some(gate.clone()))]);

        let pending_write = spawn_write(&scripted_replica, &tenant, build_index_creation(".kibana_1"));
        gate.wait_for_request().await;
        for swap in 0..200 {
            other_replica
                .write(&tenant, build_index_creation(&format!(".other-{swap:03}")))
                .await
                .expect("the other replica writes");
        }
        double.clear_reads();
        gate.release_request();
        let outcome = join_write(pending_write).await;

        assert_eq!(
            outcome.expect("detected as landed"),
            build_creation_outcome(".kibana_1")
        );
        assert_eq!(double.list_pointer_swaps(), ["create"]);
        let reads = double.list_reads();
        assert_eq!(
            reads.iter().filter(|read| read.is_range).count(),
            400,
            "two ranged GETs per frame"
        );
        assert!(
            reads
                .iter()
                .all(|read| read.is_range || read.path == "opensearch-system/tenant-a/current"),
            "no snapshot frame is read: {reads:?}"
        );
        let current_snapshot = other_replica.read(&tenant).await.expect("read");
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
        assert!(current_snapshot.snapshot.find_index(".other-000").is_some());
        assert!(current_snapshot.snapshot.find_index(".other-199").is_some());
    }

    #[tokio::test]
    async fn an_attempt_that_had_not_landed_before_two_hundred_other_swaps_is_applied_exactly_once() {
        let (double, scripted_replica, other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        let gate = Gate::default();
        double.push_swap_faults([PointerSwapFault::LoseRequest(gate.clone())]);

        let pending_write = spawn_write(&scripted_replica, &tenant, build_index_creation(".kibana_1"));
        gate.wait_for_request().await;
        for swap in 0..200 {
            other_replica
                .write(&tenant, build_index_creation(&format!(".other-{swap:03}")))
                .await
                .expect("the other replica writes");
        }
        gate.release_request();
        let outcome = join_write(pending_write).await;

        assert_eq!(outcome.expect("re-applied once"), build_creation_outcome(".kibana_1"));
        assert_eq!(double.list_pointer_swaps(), ["create", "update"]);
        let current_snapshot = other_replica.read(&tenant).await.expect("read");
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
        assert!(current_snapshot.snapshot.find_index(".other-199").is_some());
    }

    #[tokio::test]
    async fn a_lineage_frame_missing_on_the_walk_answers_503_without_reapplying() {
        let (double, scripted_replica, other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        let gate = Gate::default();
        double.push_swap_faults([PointerSwapFault::LoseRequest(gate.clone())]);

        let pending_write = spawn_write(&scripted_replica, &tenant, build_index_creation(".kibana_1"));
        gate.wait_for_request().await;
        for swap in 0..3 {
            other_replica
                .write(&tenant, build_index_creation(&format!(".other-{swap}")))
                .await
                .expect("the other replica writes");
        }
        let system_object_store =
            SystemObjectStore::new(Arc::new(double.memory_store.clone()), LOCATION).expect("an S3 location");
        let newest_name = other_replica
            .read(&tenant)
            .await
            .expect("read")
            .snapshot_name
            .expect("a pointer");
        let middle_name = system_object_store
            .read_lineage_frame(&tenant, &newest_name)
            .await
            .expect("the newest lineage")
            .expect("the newest object exists")
            .predecessor
            .expect("a predecessor");
        system_object_store
            .object_store()
            .delete(&system_object_store.snapshot_path(&tenant, &middle_name))
            .await
            .expect("delete the middle snapshot object");
        gate.release_request();
        let outcome = join_write(pending_write).await;

        assert_error(
            outcome.expect_err("the walk cannot finish"),
            StatusCode::SERVICE_UNAVAILABLE,
            "write_contention_exception",
        )
        .await;
        assert_eq!(double.list_pointer_swaps(), ["create"], "nothing is re-applied");
        let current_snapshot = other_replica.read(&tenant).await.expect("read");
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_none());
    }

    /// Both orders of a late request and a re-applied one, each conditioned on the same (absent)
    /// pointer: exactly one lands, and the answer is that one's outcome.
    #[tokio::test(start_paused = true)]
    async fn a_late_answer_and_a_reapplied_attempt_on_an_unmoved_pointer_land_at_most_once() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());

        let late_first_tenant = build_tenant(TENANT_A);
        double.push_swap_faults([PointerSwapFault::Delay, PointerSwapFault::ApplyDelayedFirst]);
        let outcome = scripted_replica
            .write(&late_first_tenant, build_index_creation(".kibana_1"))
            .await;

        assert_eq!(
            outcome.expect("the late attempt landed"),
            build_creation_outcome(".kibana_1")
        );
        let landed_names = double.list_snapshot_names(TENANT_A).await;
        assert_eq!(landed_names.len(), 1, "the refused re-application deleted its object");
        let current_snapshot = scripted_replica.read(&late_first_tenant).await.expect("read");
        assert_eq!(
            current_snapshot.snapshot_name.as_deref(),
            Some(landed_names[0].as_str())
        );

        let reapplied_first_tenant = build_tenant(TENANT_B);
        double.push_swap_faults([PointerSwapFault::Delay]);
        let outcome = scripted_replica
            .write(&reapplied_first_tenant, build_index_creation(".kibana_1"))
            .await;
        let late_answer = double.apply_delayed_request().await;

        assert_eq!(
            outcome.expect("the re-applied attempt landed"),
            build_creation_outcome(".kibana_1")
        );
        assert!(
            matches!(late_answer, Err(object_store::Error::AlreadyExists { .. })),
            "the late create finds the pointer: {late_answer:?}"
        );
        let landed_names = double.list_snapshot_names(TENANT_B).await;
        assert_eq!(
            landed_names.len(),
            1,
            "the late attempt's object was deleted once it could not land"
        );
        let current_snapshot = scripted_replica.read(&reapplied_first_tenant).await.expect("read");
        assert_eq!(
            current_snapshot.snapshot_name.as_deref(),
            Some(landed_names[0].as_str())
        );
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn contention_past_the_write_deadline_answers_503_never_409() {
        let limits = SystemIndexLimits {
            write_deadline: Duration::from_secs(2),
            ..build_limits()
        };
        let (double, scripted_replica, _other_replica) = build_replicas(limits);
        let tenant = build_tenant(TENANT_A);
        double.push_swap_faults((0..10_000).map(|_| PointerSwapFault::Conflict));

        let outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_1")).await;

        assert_error(
            outcome.expect_err("never lands"),
            StatusCode::SERVICE_UNAVAILABLE,
            "write_contention_exception",
        )
        .await;
        assert!(
            double.list_pointer_swaps().len() > 1,
            "the write retried before giving up"
        );
        assert!(double.list_snapshot_names(TENANT_A).await.is_empty());
    }

    /// A transient failure storing the snapshot object is retried before the pointer is
    /// swapped; the object's name is fresh, so the retry overwrites nothing anyone reads.
    #[tokio::test(start_paused = true)]
    async fn a_snapshot_object_put_failing_transiently_is_retried_until_it_is_stored() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        double.fail_snapshot_puts(SnapshotPutFailure::Transient, 2);

        let outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_1")).await;

        assert_eq!(
            outcome.expect("stored on the third put"),
            build_creation_outcome(".kibana_1")
        );
        assert_eq!(double.count_snapshot_puts(), 3);
        assert_eq!(double.list_pointer_swaps(), ["create"]);
        assert_eq!(double.list_snapshot_names(TENANT_A).await.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_snapshot_object_put_failing_past_the_write_deadline_answers_503_with_the_pointer_unchanged() {
        let limits = SystemIndexLimits {
            write_deadline: Duration::from_secs(2),
            ..build_limits()
        };
        let (double, scripted_replica, _other_replica) = build_replicas(limits);
        let tenant = build_tenant(TENANT_A);
        double.fail_snapshot_puts(SnapshotPutFailure::Transient, usize::MAX);

        let outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_1")).await;

        assert_error(
            outcome.expect_err("never stored"),
            StatusCode::SERVICE_UNAVAILABLE,
            "write_contention_exception",
        )
        .await;
        assert!(double.count_snapshot_puts() > 1, "the put retried before giving up");
        assert!(double.list_pointer_swaps().is_empty());
        assert_eq!(scripted_replica.read(&tenant).await.expect("read").snapshot_name, None);
    }

    /// Only one put is refused, so a retry would store the object and land the write: the `500`
    /// shows the refusal was not retried.
    #[tokio::test(start_paused = true)]
    async fn a_snapshot_object_put_refused_for_permission_answers_500_at_once_with_the_pointer_unchanged() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        double.fail_snapshot_puts(SnapshotPutFailure::Refused, 1);

        let outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_1")).await;

        assert_error(
            outcome.expect_err("a refusal no retry changes"),
            StatusCode::INTERNAL_SERVER_ERROR,
            "exception",
        )
        .await;
        assert_eq!(double.count_snapshot_puts(), 1);
        assert!(double.list_pointer_swaps().is_empty());
        assert_eq!(scripted_replica.read(&tenant).await.expect("read").snapshot_name, None);
    }

    /// After a conflict or a lost answer the pointer reload fails once: the attempt is kept, the
    /// write retries on the state it read, and the next swap settles the kept attempt (deleted
    /// once the retry lands, or detected as landed when the retry meets the pointer it installed).
    #[tokio::test(start_paused = true)]
    async fn a_pointer_reload_failing_after_a_swap_keeps_the_attempt_and_the_next_swap_settles_it() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let refused_tenant = build_tenant(TENANT_A);
        let lost_tenant = build_tenant(TENANT_B);

        double.push_swap_faults([PointerSwapFault::Conflict]);
        double.push_read_faults([None, Some(PointerReadFault::Fail)]);
        let refused_outcome = scripted_replica.write(&refused_tenant, build_index_creation(".kibana_1")).await;
        double.push_swap_faults([PointerSwapFault::LoseAnswer(None)]);
        double.push_read_faults([None, Some(PointerReadFault::Fail)]);
        let lost_outcome = scripted_replica.write(&lost_tenant, build_index_creation(".kibana_1")).await;

        assert_eq!(
            refused_outcome.expect("lands on the retry"),
            build_creation_outcome(".kibana_1")
        );
        assert_eq!(
            lost_outcome.expect("detected as landed on the retry"),
            build_creation_outcome(".kibana_1")
        );
        assert_eq!(double.list_pointer_swaps(), ["create", "create", "create", "create"]);
        assert_eq!(double.list_snapshot_names(TENANT_A).await.len(), 1);
        assert_eq!(double.list_snapshot_names(TENANT_B).await.len(), 1);
        for tenant in [&refused_tenant, &lost_tenant] {
            let current_snapshot = scripted_replica.read(tenant).await.expect("read");
            assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
        }
    }

    #[tokio::test]
    async fn a_put_result_without_an_etag_fails_the_write() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        double.push_swap_faults([PointerSwapFault::OmitEtag]);

        let outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_1")).await;

        assert_error(
            outcome.expect_err("an unrevalidatable swap"),
            StatusCode::INTERNAL_SERVER_ERROR,
            "exception",
        )
        .await;
        // The swap landed; the failed write dropped its cache entry, so the read reloads it.
        let current_snapshot = scripted_replica.read(&tenant).await.expect("read");
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
    }

    #[tokio::test]
    async fn a_snapshot_at_max_snapshot_bytes_is_written_and_one_byte_more_is_413_with_the_pointer_unchanged() {
        let one_index_size = measure_snapshot_with_indices(&[".kibana_1"]);
        let tenant = build_tenant(TENANT_A);
        let write_with_bound = |max_snapshot_bytes: usize| {
            let tenant = tenant.clone();
            async move {
                let limits = SystemIndexLimits {
                    max_snapshot_bytes,
                    ..build_limits()
                };
                let (double, scripted_replica, _other_replica) = build_replicas(limits);
                let outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_1")).await;
                let current_snapshot = scripted_replica.read(&tenant).await.expect("read");
                (outcome, current_snapshot, double)
            }
        };

        let (below_outcome, below_snapshot, _) = write_with_bound(one_index_size + 1).await;
        let (at_outcome, at_snapshot, _) = write_with_bound(one_index_size).await;
        let (above_outcome, above_snapshot, above_double) = write_with_bound(one_index_size - 1).await;

        assert_eq!(
            below_outcome.expect("below the bound"),
            build_creation_outcome(".kibana_1")
        );
        assert!(below_snapshot.snapshot.find_index(".kibana_1").is_some());
        assert_eq!(at_outcome.expect("at the bound"), build_creation_outcome(".kibana_1"));
        assert!(at_snapshot.snapshot.find_index(".kibana_1").is_some());
        assert_error(
            above_outcome.expect_err("one byte over the bound"),
            StatusCode::PAYLOAD_TOO_LARGE,
            "illegal_argument_exception",
        )
        .await;
        assert_eq!(above_snapshot.snapshot_name, None, "the pointer is unchanged");
        assert!(above_double.list_pointer_swaps().is_empty());
        assert!(above_double.list_snapshot_names(TENANT_A).await.is_empty());
    }

    /// The first write is held mid-swap while two more are queued; together they would grow the
    /// snapshot past the bound, so the batch is split and only the one that does not fit is
    /// refused.
    #[tokio::test]
    async fn a_coalesced_batch_too_large_as_a_whole_refuses_only_the_operation_that_does_not_fit() {
        let two_index_size = measure_snapshot_with_indices(&[".kibana_1", ".kibana_2"]);
        assert!(
            measure_snapshot_with_indices(&[".kibana_1", ".kibana_2", ".kibana_3"]) > two_index_size,
            "the coalesced batch must exceed the bound"
        );
        let limits = SystemIndexLimits {
            max_snapshot_bytes: two_index_size,
            ..build_limits()
        };
        let (double, scripted_replica, _other_replica) = build_replicas(limits);
        let tenant = build_tenant(TENANT_A);
        let gate = Gate::default();
        double.push_swap_faults([PointerSwapFault::Pause(gate.clone())]);

        let mut first_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_1")));
        assert!(futures::poll!(first_write.as_mut()).is_pending());
        gate.wait_for_request().await;
        let mut second_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_2")));
        let mut third_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_3")));
        assert!(futures::poll!(second_write.as_mut()).is_pending());
        assert!(futures::poll!(third_write.as_mut()).is_pending());
        gate.release_request();
        let outcomes = tokio::time::timeout(WAIT_TIMEOUT, async {
            tokio::join!(first_write, second_write, third_write)
        })
        .await
        .expect("the writes finish");

        assert_eq!(outcomes.0.expect("first"), build_creation_outcome(".kibana_1"));
        assert_eq!(outcomes.1.expect("second"), build_creation_outcome(".kibana_2"));
        assert_error(
            outcomes.2.expect_err("the third does not fit"),
            StatusCode::PAYLOAD_TOO_LARGE,
            "illegal_argument_exception",
        )
        .await;
        assert_eq!(double.list_pointer_swaps(), ["create", "update"]);
        assert_eq!(double.list_snapshot_names(TENANT_A).await.len(), 2);
        let current_snapshot = scripted_replica.read(&tenant).await.expect("read");
        assert!(current_snapshot.snapshot.find_index(".kibana_2").is_some());
        assert!(current_snapshot.snapshot.find_index(".kibana_3").is_none());
    }

    #[tokio::test]
    async fn a_cold_read_of_an_absent_pointer_is_the_empty_state() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());

        let current_snapshot = scripted_replica.read(&build_tenant(TENANT_A)).await.expect("read");

        assert_eq!(current_snapshot.snapshot_name, None);
        assert_eq!(*current_snapshot.snapshot, SystemSnapshot::default());
        assert_eq!(
            double.list_reads(),
            [RecordedRead {
                path: "opensearch-system/tenant-a/current".to_string(),
                is_conditional: false,
                is_range: false,
                is_not_modified: false,
            }]
        );
    }

    /// A pointer that does not parse, one read without an `ETag`, and one naming a snapshot
    /// object that does not exist: the read answers `500` instead of an empty or stale state.
    #[tokio::test]
    async fn a_corrupt_etagless_or_dangling_pointer_is_a_500_read() {
        let (double, scripted_replica, other_replica) = build_replicas(build_limits());
        let [corrupt_tenant, etagless_tenant, dangling_tenant] = [TENANT_A, TENANT_B, "tenant-c"].map(build_tenant);
        double.put_raw_pointer(TENANT_A, b"not a pointer").await;
        other_replica
            .write(&etagless_tenant, build_index_creation(".kibana_1"))
            .await
            .expect("tenant b writes");
        double
            .put_raw_pointer(
                "tenant-c",
                br#"{"snapshot_name":"snap-00000000-0000-4000-8000-000000000000"}"#,
            )
            .await;
        double.push_read_faults([None, Some(PointerReadFault::OmitEtag), None]);

        for tenant in [&corrupt_tenant, &etagless_tenant, &dangling_tenant] {
            let error = scripted_replica.read(tenant).await.expect_err("an unusable pointer");

            assert_error(error, StatusCode::INTERNAL_SERVER_ERROR, "exception").await;
        }
    }

    #[tokio::test]
    async fn a_current_cached_snapshot_is_served_from_a_not_modified_answer() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        scripted_replica
            .write(&tenant, build_index_creation(".kibana_1"))
            .await
            .expect("write");
        double.clear_reads();

        let first_snapshot = scripted_replica.read(&tenant).await.expect("read");
        let second_snapshot = scripted_replica.read(&tenant).await.expect("read");

        assert!(Arc::ptr_eq(&first_snapshot.snapshot, &second_snapshot.snapshot));
        let not_modified_read = RecordedRead {
            path: "opensearch-system/tenant-a/current".to_string(),
            is_conditional: true,
            is_range: false,
            is_not_modified: true,
        };
        assert_eq!(double.list_reads(), [not_modified_read.clone(), not_modified_read]);
    }

    /// The cached pointer is revalidated on every read, so a write of another replica is seen by
    /// the next read: the conditional GET returns the moved pointer, and its snapshot is loaded.
    #[tokio::test]
    async fn a_read_after_another_replica_wrote_loads_the_snapshot_the_pointer_now_names() {
        let (double, scripted_replica, other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        scripted_replica
            .write(&tenant, build_index_creation(".kibana_1"))
            .await
            .expect("this replica writes");
        other_replica
            .write(&tenant, build_index_creation(".kibana_2"))
            .await
            .expect("the other replica writes");
        double.clear_reads();

        let current_snapshot = scripted_replica.read(&tenant).await.expect("read");

        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
        assert!(current_snapshot.snapshot.find_index(".kibana_2").is_some());
        let snapshot_name = current_snapshot.snapshot_name.expect("a pointer");
        assert_eq!(
            double.list_reads(),
            [
                RecordedRead {
                    path: "opensearch-system/tenant-a/current".to_string(),
                    is_conditional: true,
                    is_range: false,
                    is_not_modified: false,
                },
                RecordedRead {
                    path: format!("opensearch-system/tenant-a/{snapshot_name}"),
                    is_conditional: false,
                    is_range: false,
                    is_not_modified: false,
                },
            ]
        );
    }

    #[tokio::test]
    async fn the_cache_evicts_the_least_recently_used_snapshot_above_cache_max_bytes() {
        let one_index_size = measure_snapshot_with_indices(&[".kibana_1"]);
        let limits = SystemIndexLimits {
            max_snapshot_bytes: one_index_size,
            cache_max_bytes: 2 * one_index_size,
            ..build_limits()
        };
        let (double, scripted_replica, _other_replica) = build_replicas(limits);
        let [tenant_a, tenant_b, tenant_c] = ["tenant-a", "tenant-b", "tenant-c"].map(build_tenant);
        scripted_replica
            .write(&tenant_a, build_index_creation(".kibana_1"))
            .await
            .expect("write a");
        scripted_replica
            .write(&tenant_b, build_index_creation(".kibana_1"))
            .await
            .expect("write b");
        // Reading a makes b the least recently used of the two cached snapshots.
        scripted_replica.read(&tenant_a).await.expect("read a");
        // Caching c's snapshot exceeds two snapshots' worth and evicts b.
        scripted_replica
            .write(&tenant_c, build_index_creation(".kibana_1"))
            .await
            .expect("write c");
        double.clear_reads();

        scripted_replica.read(&tenant_a).await.expect("read a");
        let tenant_b_snapshot = scripted_replica.read(&tenant_b).await.expect("read b");

        let snapshot_name = tenant_b_snapshot.snapshot_name.expect("b has a pointer");
        assert_eq!(
            double.list_reads(),
            [
                RecordedRead {
                    path: "opensearch-system/tenant-a/current".to_string(),
                    is_conditional: true,
                    is_range: false,
                    is_not_modified: true,
                },
                RecordedRead {
                    path: "opensearch-system/tenant-b/current".to_string(),
                    is_conditional: false,
                    is_range: false,
                    is_not_modified: false,
                },
                RecordedRead {
                    path: format!("opensearch-system/tenant-b/{snapshot_name}"),
                    is_conditional: false,
                    is_range: false,
                    is_not_modified: false,
                },
            ]
        );
    }

    /// The first write is held mid-swap while three more are queued; once it finishes, the three
    /// share one snapshot object and one swap.
    #[tokio::test]
    async fn queued_operations_of_one_tenant_are_coalesced_into_one_pointer_swap() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        let gate = Gate::default();
        double.push_swap_faults([PointerSwapFault::Pause(gate.clone())]);

        let mut first_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_1")));
        // One poll queues the operation and starts the tenant's write loop.
        assert!(futures::poll!(first_write.as_mut()).is_pending());
        gate.wait_for_request().await;
        let mut second_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_2")));
        let mut third_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_3")));
        let mut fourth_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_4")));
        assert!(futures::poll!(second_write.as_mut()).is_pending());
        assert!(futures::poll!(third_write.as_mut()).is_pending());
        assert!(futures::poll!(fourth_write.as_mut()).is_pending());
        gate.release_request();
        let outcomes = tokio::time::timeout(WAIT_TIMEOUT, async {
            tokio::join!(first_write, second_write, third_write, fourth_write)
        })
        .await
        .expect("the writes finish");

        assert_eq!(outcomes.0.expect("first"), build_creation_outcome(".kibana_1"));
        assert_eq!(outcomes.1.expect("second"), build_creation_outcome(".kibana_2"));
        assert_eq!(outcomes.2.expect("third"), build_creation_outcome(".kibana_3"));
        assert_eq!(outcomes.3.expect("fourth"), build_creation_outcome(".kibana_4"));
        assert_eq!(double.list_pointer_swaps(), ["create", "update"]);
    }

    /// Wait until no spawned task is alive; in these tests the only spawned tasks are write loops.
    async fn wait_until_no_write_loop_runs() {
        tokio::time::timeout(WAIT_TIMEOUT, async {
            while tokio::runtime::Handle::current().metrics().num_alive_tasks() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("every write loop ends once its queue is idle");
    }

    /// One write loop per tenant that ever wrote would grow without bound with the tenants a
    /// replica serves: each loop ends once its queue is idle, and the tenant's next write starts
    /// a new one that lands.
    #[tokio::test]
    async fn an_idle_write_loop_ends_and_the_next_write_of_its_tenant_starts_a_new_one() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let [tenant_a, tenant_b, tenant_c] = [TENANT_A, TENANT_B, "tenant-c"].map(build_tenant);
        for tenant in [&tenant_a, &tenant_b, &tenant_c] {
            scripted_replica
                .write(tenant, build_index_creation(".kibana_1"))
                .await
                .expect("first write");
        }

        wait_until_no_write_loop_runs().await;
        let outcome = scripted_replica.write(&tenant_a, build_index_creation(".kibana_2")).await;
        wait_until_no_write_loop_runs().await;

        assert_eq!(
            outcome.expect("a new write loop lands it"),
            build_creation_outcome(".kibana_2")
        );
        assert_eq!(double.list_pointer_swaps(), ["create", "create", "create", "update"]);
        let current_snapshot = scripted_replica.read(&tenant_a).await.expect("read");
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_some());
        assert!(current_snapshot.snapshot.find_index(".kibana_2").is_some());
    }

    /// The caller of the second write stops waiting while it is queued behind a held swap, as a
    /// request does when the deadline layer answers it first. Its operation still lands, in one
    /// batch with the third write, whose caller still gets its answer; the loop then ends, and the
    /// tenant's next write lands on a new one.
    #[tokio::test]
    async fn a_write_whose_caller_stopped_waiting_still_lands_and_the_loop_serves_the_next_write() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        let tenant = build_tenant(TENANT_A);
        let gate = Gate::default();
        double.push_swap_faults([PointerSwapFault::Pause(gate.clone())]);

        let mut first_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_1")));
        assert!(futures::poll!(first_write.as_mut()).is_pending());
        gate.wait_for_request().await;
        let mut abandoned_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_2")));
        let mut third_write = Box::pin(scripted_replica.write(&tenant, build_index_creation(".kibana_3")));
        assert!(futures::poll!(abandoned_write.as_mut()).is_pending());
        assert!(futures::poll!(third_write.as_mut()).is_pending());
        drop(abandoned_write);
        gate.release_request();
        let outcomes = tokio::time::timeout(WAIT_TIMEOUT, async { tokio::join!(first_write, third_write) })
            .await
            .expect("the writes finish");
        wait_until_no_write_loop_runs().await;
        let swaps_before_next_write = double.list_pointer_swaps();
        let snapshot_before_next_write = scripted_replica.read(&tenant).await.expect("read");
        let next_outcome = scripted_replica.write(&tenant, build_index_creation(".kibana_4")).await;

        assert_eq!(outcomes.0.expect("first"), build_creation_outcome(".kibana_1"));
        assert_eq!(outcomes.1.expect("third"), build_creation_outcome(".kibana_3"));
        assert_eq!(swaps_before_next_write, ["create", "update"]);
        assert!(
            snapshot_before_next_write.snapshot.find_index(".kibana_2").is_some(),
            "the abandoned write landed"
        );
        assert!(snapshot_before_next_write.snapshot.find_index(".kibana_3").is_some());
        assert_eq!(
            next_outcome.expect("the next write lands"),
            build_creation_outcome(".kibana_4")
        );
        assert_eq!(double.list_pointer_swaps(), ["create", "update", "update"]);
    }

    #[tokio::test]
    async fn tenant_b_never_reads_tenant_a_pointer() {
        let (double, scripted_replica, _other_replica) = build_replicas(build_limits());
        scripted_replica
            .write(&build_tenant(TENANT_A), build_index_creation(".kibana_1"))
            .await
            .expect("tenant a writes");
        double.clear_reads();

        let current_snapshot = scripted_replica.read(&build_tenant(TENANT_B)).await.expect("read");

        assert_eq!(current_snapshot.snapshot_name, None);
        assert!(current_snapshot.snapshot.find_index(".kibana_1").is_none());
        let reads = double.list_reads();
        assert!(!reads.is_empty());
        assert!(
            reads.iter().all(|read| read.path.starts_with("opensearch-system/tenant-b/")),
            "{reads:?}"
        );
    }

    /// The bound below which each retry's sleep is drawn: the first, its doubling, and the cap
    /// that holds from the first retry whose doubled bound would exceed it, up to the largest
    /// retry count.
    #[test]
    fn the_backoff_bound_starts_at_the_first_backoff_doubles_per_retry_and_is_capped() {
        let cases = [
            (0, Duration::from_millis(10)),
            (1, Duration::from_millis(20)),
            (5, Duration::from_millis(320)),
            (6, Duration::from_millis(500)),
            (40, Duration::from_millis(500)),
            (u32::MAX, Duration::from_millis(500)),
        ];
        for (retry, bound) in cases {
            assert_eq!(compute_backoff_bound(retry), bound, "retry {retry}");
        }
    }
}
