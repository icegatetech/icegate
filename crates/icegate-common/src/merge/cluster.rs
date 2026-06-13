//! Swept-line clustering of items by their sort-key boundary range.
//!
//! This module generalises the ingest Shifter's row-group clustering so that
//! both the WAL shift planner and the Parquet compaction planner can group
//! transitively-overlapping items (WAL row groups or Iceberg data files) by
//! their [`RowGroupBoundaryRange`] under the table's composite sort order.

use std::cmp::Ordering;

use crate::merge::sort_key::RowGroupBoundaryRange;

/// Internal accumulator for one in-progress swept-line cluster.
///
/// `running_max_key_idx` is an index into `items` pointing at the element whose
/// `max_key` is the current cluster maximum. Storing an index instead of a
/// cloned boundary key keeps the sweep allocation-free on every extension —
/// only the `usize` is updated.
struct ClusterAcc<T> {
    items: Vec<T>,
    running_max_key_idx: usize,
}

/// Group transitively-overlapping items into clusters by sort-key boundary range.
///
/// Input is sorted by each item's `min_key`; two items overlap when
/// `b.min_key <= running_max_key` (the max `max_key` seen so far in the cluster),
/// so A–B and B–C overlaps transitively flush into one cluster. Guarantees the
/// strict between-cluster invariant `clusters[i].running_max_key < clusters[i+1].min_key`.
///
/// `range_of` projects each item to its [`RowGroupBoundaryRange`]; the algorithm
/// never inspects the rest of the item, so callers compute per-cluster byte sums
/// (or any other aggregate) themselves over the returned groups.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use icegate_common::merge::cluster::swept_line_cluster;
/// use icegate_common::merge::sort_key::{
///     RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange,
///     RowGroupBoundaryValue,
/// };
///
/// fn range(lo: i64, hi: i64) -> RowGroupBoundaryRange {
///     let comp = |v: i64| RowGroupBoundaryComponent {
///         value: Some(RowGroupBoundaryValue::TimestampMicros(v)),
///         descending: false,
///         nulls_first: true,
///     };
///     RowGroupBoundaryRange {
///         names: Arc::from(vec!["timestamp".to_string()]),
///         min_key: RowGroupBoundaryKey::new(vec![comp(lo)]),
///         max_key: RowGroupBoundaryKey::new(vec![comp(hi)]),
///     }
/// }
///
/// // [10,20] and [15,25] overlap into one cluster; [40,50] is disjoint.
/// let items = vec![
///     (range(10, 20), "a"),
///     (range(15, 25), "b"),
///     (range(40, 50), "c"),
/// ];
/// let clusters = swept_line_cluster(items, |(r, _)| r);
/// assert_eq!(clusters.len(), 2);
/// assert_eq!(clusters[0].len(), 2);
/// ```
pub fn swept_line_cluster<T>(mut items: Vec<T>, range_of: impl Fn(&T) -> &RowGroupBoundaryRange) -> Vec<Vec<T>> {
    // Phase 1: stable-sort by `min_key` under the composite sort order. Ties on
    // `min_key` keep their input order, matching the Shifter's deterministic
    // surface.
    items.sort_by(|left, right| range_of(left).min_key.compare(&range_of(right).min_key));

    // Phase 2: sweep, growing one cluster until the incoming item's `min_key`
    // sorts strictly after the running maximum `max_key`.
    let mut clusters: Vec<Vec<T>> = Vec::new();
    let mut current: Option<ClusterAcc<T>> = None;
    for item in items {
        match current.as_mut() {
            None => {
                current = Some(ClusterAcc {
                    items: vec![item],
                    running_max_key_idx: 0,
                });
            }
            Some(acc) => {
                // Compute the overlap order; the borrow of `acc.items` ends at
                // the block boundary so we can mutate it afterwards.
                let order = {
                    let running_max = &range_of(&acc.items[acc.running_max_key_idx]).max_key;
                    range_of(&item).min_key.compare(running_max)
                };
                if matches!(order, Ordering::Less | Ordering::Equal) {
                    // Determine whether the incoming item extends the cluster
                    // maximum; the borrow ends before the push below.
                    let extends_max = {
                        let running_max = &range_of(&acc.items[acc.running_max_key_idx]).max_key;
                        range_of(&item).max_key.compare(running_max) == Ordering::Greater
                    };
                    if extends_max {
                        // After the push, `item` will sit at the current length.
                        acc.running_max_key_idx = acc.items.len();
                    }
                    acc.items.push(item);
                } else {
                    // Invariant: we are inside the `Some(acc)` arm above, so
                    // `current` must still be `Some`. Making the `else` explicit
                    // keeps the intent obvious — silently dropping `item` would
                    // lose data and is unreachable on this branch.
                    let Some(finished) = current.take() else {
                        unreachable!("current is Some on the Greater branch");
                    };
                    clusters.push(finished.items);
                    current = Some(ClusterAcc {
                        items: vec![item],
                        running_max_key_idx: 0,
                    });
                }
            }
        }
    }
    if let Some(acc) = current {
        clusters.push(acc.items);
    }
    clusters
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::merge::sort_key::{
        RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, RowGroupBoundaryValue,
    };

    // Build a single-timestamp-column boundary range [lo, hi].
    fn range(lo: i64, hi: i64) -> RowGroupBoundaryRange {
        let comp = |v: i64| RowGroupBoundaryComponent {
            value: Some(RowGroupBoundaryValue::TimestampMicros(v)),
            descending: false,
            nulls_first: true,
        };
        RowGroupBoundaryRange {
            names: Arc::from(vec!["timestamp".to_string()]),
            min_key: RowGroupBoundaryKey::new(vec![comp(lo)]),
            max_key: RowGroupBoundaryKey::new(vec![comp(hi)]),
        }
    }

    #[test]
    fn disjoint_clusters_have_strictly_increasing_min_keys() {
        // [10,20] and [15,25] overlap -> one cluster; [40,50] -> separate.
        let items = vec![(range(10, 20), 1u32), (range(15, 25), 2), (range(40, 50), 3)];
        let clusters = swept_line_cluster(items, |(r, _)| r);
        assert_eq!(clusters.len(), 2);
        assert_eq!(clusters[0].len(), 2);
        assert_eq!(clusters[1].len(), 1);
    }

    #[test]
    fn transitive_overlap_merges_chain() {
        // [10,20],[18,30],[28,40] chain transitively into one cluster.
        let items = vec![(range(10, 20), 1u32), (range(18, 30), 2), (range(28, 40), 3)];
        let clusters = swept_line_cluster(items, |(r, _)| r);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].len(), 3);
    }
}
