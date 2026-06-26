//! Sort-merge primitives shared across ingest, the Shifter, and compaction.
//!
//! These types are shared by the ingest WAL sorter, the Shifter's row-group
//! merger, and the Parquet compaction manifest scan.
//!
//! The types here describe a table's Iceberg sort order as a runtime
//! comparison plan ([`sort_key::SortColumnsDescriptor`] /
//! [`sort_key::SortColumnCache`]) and capture the inclusive min/max sort key
//! of a sorted row group ([`sort_key::RowGroupBoundaryRange`]). Keeping them in
//! `icegate-common` lets the compaction reader construct boundary keys from
//! Iceberg `Datum` bounds using exactly the same comparison semantics the
//! writer used to produce the sorted data.
//!
//! The k-way [`RowGroupsMerger`] sits on top of those primitives. It opens an
//! already-sorted set of [`MergeInput`]s through a pluggable [`MergeSource`]
//! and streams one globally sorted output. The Shifter plugs in a WAL-backed
//! source and the compactor plugs in an Iceberg-data-file source; neither the
//! overlap clustering nor the equal-key tie-break logic is duplicated.

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{error::Result, iceberg_write::CommonRecordBatchStream, merge::sort_key::RowGroupBoundaryRange};

/// Swept-line clustering of items by their sort-key boundary range.
pub mod cluster;
/// The k-way sort-merge engine ([`RowGroupsMerger`]).
pub mod merger;
/// Sort-key descriptors, the per-batch comparison cache, and row-group
/// boundary key types.
pub mod sort_key;

pub use merger::{RowGroupsMerger, SortedBatchMergerConfig};

/// One already-sorted mergeable input: a WAL row group, or an Iceberg data
/// file.
///
/// The merger treats inputs opaquely. It only needs three things from each
/// one: a stable tie-break [`position`](Self::position), the compressed
/// [`bytes`](Self::bytes) size used for cluster stats and observer accounting,
/// and the inclusive sort-key [`boundary_range`](Self::boundary_range) used to
/// decide which inputs overlap and must therefore be merged together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeInput {
    /// Opaque, source-assigned tie-break position. On EQUAL sort keys, rows are
    /// emitted in ascending [`MergePosition`] order. Sources MUST assign
    /// positions whose ascending order reproduces the desired stable order.
    pub position: MergePosition,
    /// Compressed size of this input in bytes, surfaced verbatim to the
    /// [`RowGroupsMergerObserver`] and summed into per-cluster stats. Sources
    /// that have no meaningful size may pass `0`.
    pub bytes: u64,
    /// Inclusive sort-key boundary range for overlap clustering.
    pub boundary_range: RowGroupBoundaryRange,
}

impl MergeInput {
    /// Create a new merge input from a tie-break position, byte size, and
    /// boundary range.
    #[must_use]
    pub const fn new(position: MergePosition, bytes: u64, boundary_range: RowGroupBoundaryRange) -> Self {
        Self {
            position,
            bytes,
            boundary_range,
        }
    }
}

/// A merge input's stable tie-break position, in planned order.
///
/// A newtype over `u128` so a merge position cannot be accidentally swapped with
/// an unrelated integer, and so source-specific position logic has a single
/// typed value to hang behavior on. On EQUAL sort keys the merger emits rows in
/// ascending `MergePosition` order, so each source MUST assign positions whose
/// ascending order reproduces its desired stable order.
///
/// `icegate-common` keeps the value opaque. Source crates layer their own
/// encoding on top without `common` knowing the physical coordinates: the WAL
/// source in `icegate-ingest` packs `(segment_offset, row_group_idx)` into the
/// `u128` via its own extension trait, and the compaction source uses the input's
/// index in sorted order. `u128` is wide enough for that WAL packing
/// (`(segment_offset as u128) << 32 | row_group_idx`) without overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MergePosition(u128);

impl MergePosition {
    /// Wrap a raw position value.
    #[must_use]
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// The underlying position value, for sources that need to (de)code it.
    #[must_use]
    pub const fn get(self) -> u128 {
        self.0
    }
}

impl std::fmt::Display for MergePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opens a [`MergeInput`] as an Arrow record-batch stream of already-sorted
/// rows.
///
/// Implementations resolve the opaque [`MergeInput::position`] back to whatever
/// physical location they own (a WAL segment + row group, an Iceberg data
/// file) and return its rows in sort order. The returned stream's error type
/// is [`crate::error::CommonError`]; sources whose underlying read yields a
/// different error must bridge it at the boundary.
#[async_trait]
pub trait MergeSource: Send + Sync {
    /// Open `input` as a stream of already-sorted record batches.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying input cannot be opened or read, or if
    /// the read is cancelled via `cancel`.
    async fn open(&self, input: &MergeInput, cancel: &CancellationToken) -> Result<CommonRecordBatchStream>;
}

/// Observer for merger lifecycle events, keyed by [`MergeInput::position`].
///
/// The Shifter uses this to track how many WAL row groups are concurrently
/// open and for how long. The merger guarantees every `on_input_opened` is
/// balanced by exactly one `on_input_closed`, including on cancellation and
/// `Drop`.
pub trait RowGroupsMergerObserver: Send + Sync {
    /// Called when an input starts participating in an active merge cluster.
    fn on_input_opened(&self, position: MergePosition, bytes: u64);

    /// Called when a previously opened input is fully drained or cleaned up.
    fn on_input_closed(&self, position: MergePosition, bytes: u64);
}

/// No-op merger observer used by default.
#[derive(Default)]
pub struct NoopRowGroupsMergerObserver;

impl RowGroupsMergerObserver for NoopRowGroupsMergerObserver {
    fn on_input_opened(&self, _position: MergePosition, _bytes: u64) {}

    fn on_input_closed(&self, _position: MergePosition, _bytes: u64) {}
}
