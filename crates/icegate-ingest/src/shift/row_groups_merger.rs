//! WAL adapter over the shared k-way merger.
//!
//! The generic sort-merge engine lives in
//! [`icegate_common::merge`]; it opens an already-sorted set of
//! [`MergeInput`]s through a pluggable [`icegate_common::merge::MergeSource`]
//! and streams one globally sorted output. This module wires that engine to the
//! WAL: it maps the Shifter's `(segment_offset, row_group_idx)` row groups onto
//! the merger's opaque `position`/`bytes`/`boundary_range` inputs and opens each
//! one by reading the matching WAL segment row group.
//!
//! The merger types are re-exported here under the names the shift path already
//! uses, so the rest of ingest is unaffected by the move into `icegate-common`.

use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use icegate_common::merge::{MergeInput, MergeSource};
pub use icegate_common::merge::{
    NoopRowGroupsMergerObserver, RowGroupsMerger, RowGroupsMergerObserver, SortedBatchMergerConfig,
};
use icegate_queue::{QueueReader, Topic};
use tokio_util::sync::CancellationToken;

use crate::error::IngestError;

/// Message prefix the WAL source stamps on every open/read failure.
///
/// The shared merger collapses every failure into
/// `icegate_common::Error::Write`, erasing the typed `IngestError` variant. The
/// Shifter still needs to tell a WAL read failure apart from a merger-internal
/// error so it can preserve the [`super::shift_runner`] `QueueRead` failure
/// reason. Stamping this stable prefix lets the bridge classify the error on
/// the way back without leaking WAL semantics into `icegate-common`.
pub(crate) const WAL_SOURCE_ERROR_PREFIX: &str = "wal merge source:";

/// Number of low bits in a merge `position` reserved for the row-group index.
///
/// The WAL packs `(segment_offset, row_group_idx)` into the merger's opaque
/// `u128` position so that ascending position order reproduces the old
/// `(segment_offset, row_group_idx)` lexicographic tie-break exactly: the
/// `segment_offset` occupies the high bits and the `row_group_idx` the low
/// [`WAL_ROW_GROUP_IDX_BITS`] bits.
///
/// 32 bits is ample: it caps `row_group_idx` at ~4.3 billion, and a single WAL
/// segment never holds anywhere near that many row groups, so the pack never
/// truncates a real index.
const WAL_ROW_GROUP_IDX_BITS: u32 = 32;

/// Low-bit mask isolating the row-group index inside a packed merge `position`.
const WAL_ROW_GROUP_IDX_MASK: u128 = (1 << WAL_ROW_GROUP_IDX_BITS) - 1;

/// Pack a WAL `(segment_offset, row_group_idx)` pair into the opaque merge
/// `position`.
///
/// `position = (segment_offset as u128) << 32 | (row_group_idx as u128)`.
///
/// Because `segment_offset` (a `u64`) lands entirely above bit 32 and
/// `row_group_idx` entirely below it, comparing two packed positions yields the
/// same order as comparing `(segment_offset, row_group_idx)` lexicographically:
/// any difference in `segment_offset` dominates the low 32 bits, and equal
/// `segment_offset`s fall back to comparing `row_group_idx`. This reproduces the
/// merger's previous `compare_wal_position` tie-break for equal sort keys
/// exactly, with no overflow (`u64` shifted into bits 32..96 of a `u128`).
#[must_use]
pub const fn wal_merge_position(segment_offset: u64, row_group_idx: usize) -> u128 {
    ((segment_offset as u128) << WAL_ROW_GROUP_IDX_BITS) | (row_group_idx as u128 & WAL_ROW_GROUP_IDX_MASK)
}

/// Decode a packed merge `position` back into its WAL coordinates.
///
/// Inverse of [`wal_merge_position`]: returns `(segment_offset, row_group_idx)`.
///
/// The two casts are provably lossless for positions produced by
/// [`wal_merge_position`]: the high part is first masked to 64 bits (so it fits
/// `u64`) and `row_group_idx` is masked to [`WAL_ROW_GROUP_IDX_BITS`] bits (so
/// it fits `usize` on every supported 32-/64-bit target). The masks make the
/// truncation a no-op, hence the scoped allow.
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub const fn wal_decode_position(position: u128) -> (u64, usize) {
    let segment_offset = ((position >> WAL_ROW_GROUP_IDX_BITS) & (u64::MAX as u128)) as u64;
    let row_group_idx = (position & WAL_ROW_GROUP_IDX_MASK) as usize;
    (segment_offset, row_group_idx)
}

/// Build the merger's flat input list from the Shifter's segment plan.
///
/// Each WAL row group becomes one [`MergeInput`] whose `position` packs the
/// `(segment_offset, row_group_idx)` pair (see [`wal_merge_position`]), whose
/// `bytes` is the compressed row-group size, and whose `boundary_range` is the
/// row group's validated sort-key range. The packing reproduces the old
/// `compare_wal_position` ordering, so equal-key rows keep their original WAL
/// order in the merged output.
#[must_use]
pub fn wal_inputs_from_segments(segments: &[super::SegmentToRead]) -> Vec<MergeInput> {
    segments
        .iter()
        .flat_map(|segment| {
            segment.row_groups.iter().map(move |row_group| MergeInput {
                position: wal_merge_position(segment.segment_offset, row_group.row_group_idx),
                bytes: row_group.row_group_bytes,
                boundary_range: row_group.boundary_range.clone(),
            })
        })
        .collect()
}

/// [`MergeSource`] that opens merge inputs by reading WAL segment row groups.
///
/// The opaque `position` carried by each [`MergeInput`] is decoded back to a
/// `(segment_offset, row_group_idx)` pair and used to read exactly that row
/// group from the WAL topic.
pub struct WalMergeSource {
    queue_reader: Arc<dyn QueueReader>,
    topic: Topic,
}

impl WalMergeSource {
    /// Create a WAL merge source for `topic`, reading through `queue_reader`.
    #[must_use]
    pub fn new(queue_reader: Arc<dyn QueueReader>, topic: Topic) -> Self {
        Self { queue_reader, topic }
    }
}

#[async_trait]
impl MergeSource for WalMergeSource {
    async fn open(
        &self,
        input: &MergeInput,
        cancel: &CancellationToken,
    ) -> icegate_common::error::Result<icegate_common::iceberg_write::CommonRecordBatchStream> {
        let (segment_offset, row_group_idx) = wal_decode_position(input.position);
        let stream = self
            .queue_reader
            .read_segment(&self.topic, segment_offset, &[row_group_idx], cancel)
            .await
            .map_err(|err| {
                wal_stream_error(&err.into(), segment_offset, row_group_idx, "failed to open WAL segment")
            })?;
        // Bridge the queue stream's error type (`QueueError`) into the merger's
        // `CommonError`. The merger has no knowledge of WAL specifics, so the
        // mapping happens here at the source boundary. Cancellation is
        // re-detected at the shift bridge via the shared cancel token; encoding
        // it in the message here is sufficient because the merger surfaces the
        // first stream error verbatim.
        let bridged = stream.map_err(move |err| {
            wal_stream_error(
                &IngestError::from(err),
                segment_offset,
                row_group_idx,
                "failed to read WAL segment",
            )
        });
        Ok(Box::pin(bridged))
    }
}

/// Map a WAL open/read failure into the merger's `CommonError`, preserving the
/// `segment N row group M` context the Shifter logs on and stamping
/// [`WAL_SOURCE_ERROR_PREFIX`] so the bridge can recover the `QueueRead`
/// failure reason.
fn wal_stream_error(
    err: &IngestError,
    segment_offset: u64,
    row_group_idx: usize,
    context: &str,
) -> icegate_common::error::CommonError {
    icegate_common::error::CommonError::Write(format!(
        "{WAL_SOURCE_ERROR_PREFIX} {context} {segment_offset} row group {row_group_idx}: {err}"
    ))
}

/// Whether `err` was produced by [`WalMergeSource`] (vs. a merger-internal
/// failure), used by the Shifter bridge to decide the failure reason.
#[must_use]
pub(crate) fn is_wal_source_error(err: &icegate_common::error::CommonError) -> bool {
    matches!(err, icegate_common::error::CommonError::Write(message) if message.starts_with(WAL_SOURCE_ERROR_PREFIX))
}

#[cfg(test)]
mod tests {
    use super::{wal_decode_position, wal_merge_position};

    /// The packed `position` ordering must reproduce the old
    /// `(segment_offset, row_group_idx)` lexicographic tie-break exactly. This
    /// is the correctness contract the equal-key WAL-order merger tests depend
    /// on, verified here directly on the packing function.
    #[test]
    fn wal_position_packing_preserves_lexicographic_order() {
        let cases = [
            (10_u64, 0_usize),
            (10, 1),
            (11, 0),
            (12, 0),
            (7, 0),
            (7, 1),
            (0, 0),
            (1, 0),
            (u64::MAX, 0),
            (u64::MAX, 7),
        ];

        for &(left_offset, left_idx) in &cases {
            for &(right_offset, right_idx) in &cases {
                let left = wal_merge_position(left_offset, left_idx);
                let right = wal_merge_position(right_offset, right_idx);
                let expected = (left_offset, left_idx).cmp(&(right_offset, right_idx));
                assert_eq!(
                    left.cmp(&right),
                    expected,
                    "packed order for ({left_offset},{left_idx}) vs ({right_offset},{right_idx}) \
                     must match lexicographic (segment_offset, row_group_idx)"
                );
            }
        }
    }

    /// `wal_decode_position` is the exact inverse of `wal_merge_position`.
    #[test]
    fn wal_position_round_trips() {
        for &(offset, idx) in &[(0_u64, 0_usize), (10, 3), (u64::from(u32::MAX), 5), (u64::MAX, 1)] {
            let packed = wal_merge_position(offset, idx);
            assert_eq!(wal_decode_position(packed), (offset, idx));
        }
    }
}
