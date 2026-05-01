//! Sort-aware shift planner. Planner only works with data.
//!
//! Given a flat list of WAL row-group entries enriched with sort-key
//! boundaries from the WAL footer, this module groups them into shift-task
//! chunks that minimise sort-key overlap between output parquet files:
//!
//! 1. **Sort** by `min_key` under the table's composite sort order.
//! 2. **Cluster** transitively overlapping row groups (swept-line) — each
//!    cluster is an atomic unit during bin-packing so the boundary between
//!    chunks always falls on a non-overlap point.
//! 3. **Bin-pack** clusters into chunks. Normal disjoint clusters are appended
//!    until the chunk reaches `lower_bound_bytes`; the next disjoint cluster
//!    starts a new chunk unless row-group or `upper_bound_bytes` limits force
//!    an earlier flush. Clusters that exceed `upper_bound_bytes` are split
//!    linearly (disjointness loss is accepted as the failover path).
//! 4. **Tail-merge**: if the final chunk is below `lower_bound_bytes` and the
//!    previous chunk is not the result of an oversized split, merge them
//!    when the combined size still fits inside `upper_bound_bytes`.
//!
//! Outputs of two adjacent non-oversized chunks are pairwise disjoint by
//! construction; outputs of oversized splits may overlap inside the cluster
//! that produced them.

use std::{collections::HashMap, sync::Arc};

use icegate_queue::{ExtractedValue, RowGroupPlanEntry};
use tokio_util::sync::CancellationToken;

use crate::{
    error::{IngestError, Result},
    wal::{RowGroupBoundaryRange, deserialize_row_group_boundary_range},
};

/// Field name under which the planner expects the per-row-group boundary
/// payload in [`RowGroupPlanEntry::extracted`].
pub(super) const PLAN_FIELD_BOUNDARY_RANGE: &str = "boundary_range";

/// One field of a planner partition spec.
///
/// The variants describe how to materialize a partition value from a row
/// group's extracted statistics or from its sort-key boundary range; they do
/// not encode any specific column name themselves.
pub(super) enum PlannerPartitionField {
    /// Identity partition sourced from UTF-8 singleton column statistics.
    IdentityUtf8ColumnStats {
        /// Field name used as the key inside [`RowGroupPlanEntry::extracted`].
        plan_field: &'static str,
        /// Source column name in the WAL row-group statistics.
        column_name: &'static str,
    },
    /// Iceberg day transform sourced from the row-group's physical timestamp
    /// column statistics (min/max). Required instead of reading the boundary
    /// range under a composite sort key: with a heterogeneous key prefix the
    /// boundary timestamps reflect the *boundary rows* and can hide interior
    /// timestamps that fall in a different day.
    DayTimestampColumnStats {
        /// Field name used as the key inside [`RowGroupPlanEntry::extracted`].
        plan_field: &'static str,
    },
}

/// Convert queue plan entries into schema-agnostic planner row groups using the
/// supplied partition fields.
///
/// # Errors
///
/// Returns an error if required extracted fields or boundary components are
/// missing, invalid, empty, or use unsupported value types.
pub(super) fn plan_entries_to_row_groups(
    fields: &[PlannerPartitionField],
    entries: Vec<RowGroupPlanEntry>,
) -> Result<Vec<PlanRowGroup>> {
    // One intern table per batch: identity partition strings (e.g. tenant_id)
    // are shared via Arc<str> so that N row groups with the same tenant produce
    // only one heap allocation for the string data instead of N copies.
    let mut interner: HashMap<String, Arc<str>> = HashMap::new();
    entries
        .into_iter()
        .map(|entry| plan_entry_to_row_group(fields, entry, &mut interner))
        .collect()
}

fn plan_entry_to_row_group(
    fields: &[PlannerPartitionField],
    entry: RowGroupPlanEntry,
    interner: &mut HashMap<String, Arc<str>>,
) -> Result<PlanRowGroup> {
    let RowGroupPlanEntry {
        wal_offset,
        row_group_idx,
        row_group_bytes,
        mut extracted,
    } = entry;

    let metadata = take_required_string(
        &mut extracted,
        PLAN_FIELD_BOUNDARY_RANGE,
        wal_offset,
        row_group_idx,
        RequiredFieldKind::BoundaryPayload,
    )?;
    let boundary_range =
        deserialize_row_group_boundary_range(&metadata).map_err(|e| IngestError::Shift(e.to_string()))?;
    let partition = row_group_partition(fields, &mut extracted, wal_offset, row_group_idx, interner)?;

    Ok(PlanRowGroup {
        wal_offset,
        row_group_idx,
        row_group_bytes,
        boundary_range,
        partition,
    })
}

fn row_group_partition(
    fields: &[PlannerPartitionField],
    extracted: &mut HashMap<String, ExtractedValue>,
    wal_offset: u64,
    row_group_idx: usize,
    interner: &mut HashMap<String, Arc<str>>,
) -> Result<RowGroupPartition> {
    let mut values = Vec::with_capacity(fields.len());
    for field in fields {
        match field {
            PlannerPartitionField::IdentityUtf8ColumnStats { plan_field, .. } => {
                let value = take_required_string(
                    extracted,
                    plan_field,
                    wal_offset,
                    row_group_idx,
                    RequiredFieldKind::IdentityValue,
                )?;
                if value.is_empty() {
                    return Err(IngestError::Shift(format!(
                        "empty {plan_field} in plan entry (WAL segment {wal_offset} row group {row_group_idx})"
                    )));
                }
                // Intern the string so all row groups sharing the same identity
                // value (e.g. same tenant_id) reuse one Arc<str> allocation.
                let arc_str = interner.entry(value).or_insert_with_key(|k| Arc::from(k.as_str())).clone();
                values.push(PartitionValue::String(arc_str));
            }
            PlannerPartitionField::DayTimestampColumnStats { plan_field } => {
                let Some(value) = extracted.get(*plan_field) else {
                    return Err(IngestError::Shift(format!(
                        "missing required timestamp stats ({plan_field}) in plan entry \
                         (WAL segment {wal_offset} row group {row_group_idx}): \
                         timestamp column must be non-null for day partitioning"
                    )));
                };
                let &ExtractedValue::TimestampMicrosRange(min_ts, max_ts) = value else {
                    return Err(IngestError::Shift(format!(
                        "{plan_field} field has unexpected type for day partition \
                         (WAL segment {wal_offset} row group {row_group_idx})"
                    )));
                };
                let min_day = micros_to_day(min_ts);
                let max_day = micros_to_day(max_ts);
                if min_day != max_day {
                    // Row group straddles a day boundary: include the day range
                    // in the bucket key so cross-day row groups from different
                    // day pairs are never packed into the same shift task.
                    values.push(PartitionValue::Day(min_day));
                    values.push(PartitionValue::Day(max_day));
                    return Ok(RowGroupPartition::CrossPartition(PartitionBucket(values)));
                }
                values.push(PartitionValue::Day(min_day));
            }
        }
    }
    let bucket = PartitionBucket(values);
    Ok(RowGroupPartition::Single(bucket))
}

#[derive(Copy, Clone)]
enum RequiredFieldKind {
    IdentityValue,
    BoundaryPayload,
}

fn take_required_string(
    extracted: &mut HashMap<String, ExtractedValue>,
    field: &str,
    wal_offset: u64,
    row_group_idx: usize,
    kind: RequiredFieldKind,
) -> Result<String> {
    let value = extracted.remove(field).ok_or_else(|| match kind {
        RequiredFieldKind::IdentityValue => IngestError::Shift(format!(
            "missing {field} field in plan entry (WAL segment {wal_offset} row group {row_group_idx})"
        )),
        RequiredFieldKind::BoundaryPayload => IngestError::Shift(format!(
            "missing {field} payload for WAL segment {wal_offset} row group {row_group_idx}"
        )),
    })?;
    value.into_utf8().ok_or_else(|| {
        IngestError::Shift(format!(
            "{field} field has unexpected type (expected utf8) \
             in plan entry (WAL segment {wal_offset} row group {row_group_idx})"
        ))
    })
}

/// Microseconds in one UTC day, the granularity of Iceberg day partitions.
pub(super) const MICROS_PER_DAY: i64 = 86_400 * 1_000_000;

/// Convert a microsecond timestamp to its Iceberg `day` partition value.
#[allow(clippy::cast_possible_truncation)]
const fn micros_to_day(ts_micros: i64) -> i32 {
    (ts_micros.div_euclid(MICROS_PER_DAY)) as i32
}

/// One WAL row group augmented with its parsed sort-key boundary.
///
/// `row_group_metadata` is the original opaque payload from the WAL footer;
/// it is preserved verbatim so that downstream shift tasks do not re-serialize
/// the boundary.
#[derive(Debug, Clone)]
pub(super) struct PlanRowGroup {
    /// WAL segment offset.
    pub(super) wal_offset: u64,
    /// Row-group index inside the segment.
    pub(super) row_group_idx: usize,
    /// Compressed row-group size in bytes.
    pub(super) row_group_bytes: u64,
    /// Parsed boundary range (min/max key under the table's sort order).
    pub(super) boundary_range: RowGroupBoundaryRange,
    /// Whether this row group maps to exactly one bucket or crosses at least one
    /// supported partition boundary.
    pub(super) partition: RowGroupPartition,
}

/// Schema-agnostic partition bucket used by the planner.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct PartitionBucket(pub(super) Vec<PartitionValue>);

/// One supported value inside a planner partition bucket.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) enum PartitionValue {
    /// Identity UTF-8 partition value.
    ///
    /// Uses `Arc<str>` so that cloning a bucket across row groups for the same
    /// tenant shares the underlying string allocation rather than copying it.
    String(Arc<str>),
    /// Iceberg day transform value.
    Day(i32),
}

/// Partition classification for one row group.
#[derive(Debug, Clone)]
pub(super) enum RowGroupPartition {
    /// The row group belongs to exactly one partition bucket.
    Single(PartitionBucket),
    /// The row group crosses at least one partition transform boundary.
    CrossPartition(PartitionBucket),
}

/// One swept-line cluster: row groups whose sort-key ranges transitively overlap.
#[derive(Debug, Clone)]
pub(super) struct Cluster {
    /// Row groups in min-key order.
    pub(super) row_groups: Vec<PlanRowGroup>,
    /// Sum of `row_group_bytes` across `row_groups`.
    pub(super) total_bytes: u64,
}

/// One bin-packed chunk = the input for a single shift task.
#[derive(Debug, Clone)]
pub(super) struct PlannedChunk {
    /// Row groups in clustered, then bin-packed order.
    pub(super) row_groups: Vec<PlanRowGroup>,
    /// Sum of `row_group_bytes` across `row_groups`.
    pub(super) total_bytes: u64,
    /// `true` if the chunk is the result of splitting an oversized cluster
    /// (i.e., disjointness with neighbouring chunks is not guaranteed).
    pub(super) is_oversized_split: bool,
}

/// Limits driving the planner. Bounds come from
/// `ShiftReadConfig::{lower,upper}_bound_input_bytes_per_task`; the row-group
/// cap from `max_record_batches_per_task`.
#[derive(Debug, Clone, Copy)]
pub(super) struct PlannerConfig {
    /// Soft per-chunk budget. Once the current chunk reaches this size, the
    /// next disjoint cluster starts a new chunk. Also acts as the tail-merge
    /// eligibility cutoff: a final chunk smaller than this is a candidate for
    /// merge into its predecessor.
    pub(super) lower_bound_bytes: u64,
    /// Hard per-chunk cap. Clusters above this are split into oversized
    /// sub-chunks. Tail-merge bails when combined size would exceed it.
    pub(super) upper_bound_bytes: u64,
    /// Per-chunk row-group cap (defence against pathological fan-out).
    pub(super) max_row_groups: usize,
}

impl PlannerConfig {
    /// Build planner limits from explicit byte bounds.
    ///
    /// Caller must ensure `upper >= lower > 0` (validated by config layer).
    pub(super) const fn from_bounds(lower: u64, upper: u64, max_row_groups: usize) -> Self {
        Self {
            lower_bound_bytes: lower,
            upper_bound_bytes: upper,
            max_row_groups,
        }
    }
}

/// End-to-end planner pipeline for row groups with precomputed partition buckets.
///
/// Pipeline: `partition_by_bucket` → per-partition-bucket(`sort_by_min_key` →
/// `swept_line_cluster` → `bin_pack` → `tail_merge`).
///
/// Row groups are first split by abstract [`PartitionBucket`] so that the
/// algorithm never bin-packs or tail-merges row groups that belong to different
/// Iceberg partitions.
///
/// Row groups classified as [`RowGroupPartition::CrossPartition`] are processed
/// in bucketed overflow groups.  The adapter decides the overflow bucket key;
/// the planner only guarantees that different keys are never merged together.
///
/// `cancel_token` is checked before each phase so that long-running plan tasks
/// abort promptly on shutdown.
///
/// # Errors
///
/// Returns an error if `cancel_token` is cancelled before any phase runs.
pub(super) fn plan_row_groups(
    row_groups: Vec<PlanRowGroup>,
    limits: &PlannerConfig,
    cancel_token: &CancellationToken,
) -> Result<(Vec<PlannedChunk>, PlannerStats)> {
    check_cancelled(cancel_token, "partition_by_bucket")?;
    let (partition_buckets, cross_partition_buckets) = partition_by_bucket(row_groups);
    let cross_partition_row_groups = cross_partition_buckets.values().map(Vec::len).sum();

    let mut all_chunks: Vec<PlannedChunk> = Vec::new();
    let mut stats = PlannerStats {
        cross_partition_row_groups,
        ..Default::default()
    };

    for (_bucket, bucket_rgs) in partition_buckets {
        let (mut chunks, bucket_stats) = plan_bucket(bucket_rgs, limits, cancel_token)?;
        all_chunks.append(&mut chunks);
        stats.clusters = stats.clusters.saturating_add(bucket_stats.clusters);
        stats.oversized_clusters = stats.oversized_clusters.saturating_add(bucket_stats.oversized_clusters);
        stats.tail_merged |= bucket_stats.tail_merged;
        stats.max_cluster_bytes = stats.max_cluster_bytes.max(bucket_stats.max_cluster_bytes);
    }

    for (_bucket, bucket_rgs) in cross_partition_buckets {
        let (mut chunks, bucket_stats) = plan_bucket(bucket_rgs, limits, cancel_token)?;
        all_chunks.append(&mut chunks);
        stats.clusters = stats.clusters.saturating_add(bucket_stats.clusters);
        stats.oversized_clusters = stats.oversized_clusters.saturating_add(bucket_stats.oversized_clusters);
        stats.tail_merged |= bucket_stats.tail_merged;
        stats.max_cluster_bytes = stats.max_cluster_bytes.max(bucket_stats.max_cluster_bytes);
    }

    Ok((all_chunks, stats))
}

/// Split row groups into exact partition buckets and cross-partition overflow
/// buckets.  The planner treats the bucket value as opaque.
fn partition_by_bucket(
    row_groups: Vec<PlanRowGroup>,
) -> (
    HashMap<PartitionBucket, Vec<PlanRowGroup>>,
    HashMap<PartitionBucket, Vec<PlanRowGroup>>,
) {
    let mut partition_buckets: HashMap<PartitionBucket, Vec<PlanRowGroup>> = HashMap::new();
    let mut cross_partition_buckets: HashMap<PartitionBucket, Vec<PlanRowGroup>> = HashMap::new();

    // Bucket lookup via `get_mut`; `clone` only on first insertion per bucket.
    // This keeps the number of `PartitionBucket` clones equal to the number of
    // unique buckets rather than the number of row groups.
    for rg in row_groups {
        let (map, bucket) = match &rg.partition {
            RowGroupPartition::Single(bucket) => (&mut partition_buckets, bucket),
            RowGroupPartition::CrossPartition(bucket) => (&mut cross_partition_buckets, bucket),
        };
        if let Some(existing) = map.get_mut(bucket) {
            existing.push(rg);
        } else {
            let key = bucket.clone();
            map.insert(key, vec![rg]);
        }
    }

    (partition_buckets, cross_partition_buckets)
}

fn check_cancelled(cancel_token: &CancellationToken, phase: &'static str) -> Result<()> {
    if cancel_token.is_cancelled() {
        return Err(IngestError::Shift(format!("plan_row_groups cancelled before {phase}")));
    }
    Ok(())
}

/// Telemetry returned alongside the chunk list for one planner run.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct PlannerStats {
    /// Number of clusters formed during the swept-line pass (across all partition buckets).
    pub(super) clusters: usize,
    /// Number of clusters that exceeded `upper_bound_bytes` and had to be split.
    pub(super) oversized_clusters: usize,
    /// `true` if any partition bucket's tail-merge pass merged the last chunk into the previous one.
    pub(super) tail_merged: bool,
    /// Largest cluster (bytes) seen during clustering (across all partition buckets).
    pub(super) max_cluster_bytes: u64,
    /// Number of WAL row groups that span more than one supported partition bucket.
    ///
    /// These row groups cannot be atomically split by the planner; each may cause Iceberg's
    /// partition fanout to produce more than one Parquet file from its shift task.
    pub(super) cross_partition_row_groups: usize,
}

/// Aggregated output of planning one partition bucket (or the cross-partition bucket).
struct BucketStats {
    clusters: usize,
    oversized_clusters: usize,
    tail_merged: bool,
    max_cluster_bytes: u64,
}

/// Run the inner planning pipeline (sort → cluster → bin-pack → tail-merge) for one homogeneous
/// bucket of row groups.
///
/// # Errors
///
/// Returns an error if `cancel_token` is cancelled before any phase.
fn plan_bucket(
    mut row_groups: Vec<PlanRowGroup>,
    limits: &PlannerConfig,
    cancel_token: &CancellationToken,
) -> Result<(Vec<PlannedChunk>, BucketStats)> {
    // TODO(high): try to reduce row group iterations

    check_cancelled(cancel_token, "sort")?;
    sort_by_min_key(&mut row_groups);
    check_cancelled(cancel_token, "swept_line_cluster")?;
    let clusters = swept_line_cluster(row_groups);
    let n_clusters = clusters.len();
    let max_cluster_bytes = clusters.iter().map(|c| c.total_bytes).max().unwrap_or_default();
    check_cancelled(cancel_token, "bin_pack")?;
    let (mut chunks, oversized_clusters) = bin_pack(clusters, limits)?;
    check_cancelled(cancel_token, "tail_merge")?;
    let tail_merged = tail_merge(&mut chunks, limits);
    Ok((
        chunks,
        BucketStats {
            clusters: n_clusters,
            oversized_clusters,
            tail_merged,
            max_cluster_bytes,
        },
    ))
}

/// Stable-sort row groups by `min_key` under the table's sort order.
///
/// Stability matters: ties on `min_key` (e.g., two row groups starting at the
/// same `(account, service)` prefix) must keep their input order so the test
/// surface is deterministic.
fn sort_by_min_key(row_groups: &mut [PlanRowGroup]) {
    row_groups.sort_by(|left, right| left.boundary_range.min_key.compare(&right.boundary_range.min_key));
}

/// Internal accumulator for a swept-line cluster.
struct ClusterAcc {
    row_groups: Vec<PlanRowGroup>,
    /// Index into `row_groups` of the element whose `max_key` is the current
    /// cluster maximum. Storing an index avoids cloning the boundary key on
    /// every extension — only the usize is updated.
    running_max_key_idx: usize,
    total_bytes: u64,
}

/// Group transitively overlapping row groups into clusters using a swept-line
/// pass over `min_key`-sorted input.
///
/// Two row groups overlap when `b.min_key <= cluster.running_max_key`. The
/// `running_max_key` is the maximum `max_key` observed so far inside the
/// current cluster — ensuring that A overlaps B and B overlaps C transitively
/// flushes both into the same cluster.
///
/// Invariant: between any two consecutive output clusters
/// `clusters[i].running_max_key < clusters[i+1].min_key` (strict).
fn swept_line_cluster(row_groups: Vec<PlanRowGroup>) -> Vec<Cluster> {
    use std::cmp::Ordering;

    let mut clusters: Vec<Cluster> = Vec::new();
    let mut current: Option<ClusterAcc> = None;
    for rg in row_groups {
        match current.as_mut() {
            None => {
                current = Some(ClusterAcc {
                    running_max_key_idx: 0,
                    total_bytes: rg.row_group_bytes,
                    row_groups: vec![rg],
                });
            }
            Some(acc) => {
                // Compute the overlap order; borrow ends at block boundary so
                // we can mutate `acc.row_groups` afterwards.
                let order = {
                    let running_max = &acc.row_groups[acc.running_max_key_idx].boundary_range.max_key;
                    rg.boundary_range.min_key.compare(running_max)
                };
                if matches!(order, Ordering::Less | Ordering::Equal) {
                    // Determine whether the incoming RG extends the cluster
                    // maximum; borrow ends before the push below.
                    let extends_max = {
                        let running_max = &acc.row_groups[acc.running_max_key_idx].boundary_range.max_key;
                        rg.boundary_range.max_key.compare(running_max) == Ordering::Greater
                    };
                    if extends_max {
                        // After push, `rg` will sit at the current length.
                        acc.running_max_key_idx = acc.row_groups.len();
                    }
                    acc.total_bytes = acc.total_bytes.saturating_add(rg.row_group_bytes);
                    acc.row_groups.push(rg);
                } else {
                    // Invariant: we are inside the `Some(acc)` arm above, so
                    // `current` must still be `Some`. The `else` arm makes
                    // the invariant explicit — silently dropping `rg` would
                    // lose data and is unreachable on this branch.
                    let Some(finished) = current.take() else {
                        unreachable!("current is Some on the Greater branch");
                    };
                    clusters.push(Cluster {
                        row_groups: finished.row_groups,
                        total_bytes: finished.total_bytes,
                    });
                    current = Some(ClusterAcc {
                        running_max_key_idx: 0,
                        total_bytes: rg.row_group_bytes,
                        row_groups: vec![rg],
                    });
                }
            }
        }
    }
    if let Some(acc) = current {
        clusters.push(Cluster {
            row_groups: acc.row_groups,
            total_bytes: acc.total_bytes,
        });
    }
    clusters
}

/// Bin-pack clusters greedily into shift-task chunks.
///
/// - A cluster exceeding `limits.upper_bound_bytes` is split linearly into
///   sub-chunks of at most `upper_bound_bytes` (each marked `is_oversized_split`).
/// - Normal clusters are added whole until the current chunk reaches
///   `lower_bound_bytes`; after that, the next disjoint cluster starts a new
///   chunk.
/// - `upper_bound_bytes` and `max_row_groups` remain hard caps and flush the
///   current chunk before adding the next normal cluster.
/// - A small final chunk is handled by `tail_merge`.
///
/// # Errors
///
/// Returns an error if any individual WAL row group inside an oversized cluster
/// is itself larger than `limits.upper_bound_bytes`.  The planner cannot split
/// a single WAL row group; such a row group is an operator misconfiguration
/// (the WAL writer produced a row group larger than the planner's hard cap).
fn bin_pack(clusters: Vec<Cluster>, limits: &PlannerConfig) -> Result<(Vec<PlannedChunk>, usize)> {
    let mut chunks: Vec<PlannedChunk> = Vec::new();
    let mut current: Option<PlannedChunk> = None;
    let mut oversized_count = 0usize;

    for cluster in clusters {
        if cluster.total_bytes > limits.upper_bound_bytes {
            // Flush any in-progress chunk before emitting oversized splits.
            if let Some(chunk) = current.take() {
                chunks.push(chunk);
            }
            oversized_count += 1;
            // All sub-chunks except the last go straight to `chunks`.  The
            // last sub-chunk is kept in `current` so that subsequent disjoint
            // normal clusters can be packed into it up to `upper_bound_bytes`
            // (ТЗ Case 4: fat-service trailing sub-chunk + skinny services →
            // one output file instead of two).
            let mut sub_iter = split_oversized_cluster(cluster, limits)?.into_iter().peekable();
            while let Some(sub) = sub_iter.next() {
                if sub_iter.peek().is_none() {
                    current = Some(sub);
                } else {
                    chunks.push(sub);
                }
            }
            continue;
        }

        let cluster_count = cluster.row_groups.len();
        let must_flush = current.as_ref().is_some_and(|chunk| {
            // When `current` is the trailing sub-chunk of an oversized split,
            // skip the lower-bound flush so disjoint normal clusters can pack
            // into it.  Hard caps (`upper_bound_bytes`, `max_row_groups`)
            // still apply.
            let over_lower = !chunk.is_oversized_split && chunk.total_bytes >= limits.lower_bound_bytes;
            over_lower
                || chunk.total_bytes.saturating_add(cluster.total_bytes) > limits.upper_bound_bytes
                || chunk.row_groups.len().saturating_add(cluster_count) > limits.max_row_groups
        });
        if must_flush {
            if let Some(chunk) = current.take() {
                chunks.push(chunk);
            }
        }

        let chunk = current.get_or_insert_with(|| PlannedChunk {
            row_groups: Vec::new(),
            total_bytes: 0,
            is_oversized_split: false,
        });
        chunk.total_bytes = chunk.total_bytes.saturating_add(cluster.total_bytes);
        chunk.row_groups.extend(cluster.row_groups);
    }

    if let Some(chunk) = current {
        chunks.push(chunk);
    }
    Ok((chunks, oversized_count))
}

/// Linearly split an oversized cluster into chunks of at most
/// `limits.upper_bound_bytes`, also respecting `limits.max_row_groups`. The
/// resulting sub-chunks preserve internal sort-key order but are NOT pairwise
/// disjoint — the cluster they came from already contained overlapping row
/// groups by definition.
///
/// # Errors
///
/// Returns an error if any individual row group inside the cluster is itself
/// larger than `limits.upper_bound_bytes`.  The planner cannot split a single
/// WAL row group; callers must treat this as an operator misconfiguration and
/// surface the error rather than letting the writer failover mask it.
fn split_oversized_cluster(cluster: Cluster, limits: &PlannerConfig) -> Result<Vec<PlannedChunk>> {
    let mut sub_chunks: Vec<PlannedChunk> = Vec::new();
    let mut current = PlannedChunk {
        row_groups: Vec::new(),
        total_bytes: 0,
        is_oversized_split: true,
    };
    for rg in cluster.row_groups {
        if rg.row_group_bytes > limits.upper_bound_bytes {
            return Err(IngestError::Shift(format!(
                "WAL row group (segment {} index {}) is {} bytes which exceeds the planner \
                 upper bound of {} bytes; reduce the WAL row group size or increase \
                 upper_bound_input_bytes_per_task",
                rg.wal_offset, rg.row_group_idx, rg.row_group_bytes, limits.upper_bound_bytes,
            )));
        }
        let next_bytes = current.total_bytes.saturating_add(rg.row_group_bytes);
        let next_count = current.row_groups.len().saturating_add(1);
        if !current.row_groups.is_empty()
            && (next_bytes > limits.upper_bound_bytes || next_count > limits.max_row_groups)
        {
            sub_chunks.push(std::mem::replace(
                &mut current,
                PlannedChunk {
                    row_groups: Vec::new(),
                    total_bytes: 0,
                    is_oversized_split: true,
                },
            ));
        }
        current.total_bytes = current.total_bytes.saturating_add(rg.row_group_bytes);
        current.row_groups.push(rg);
    }
    if !current.row_groups.is_empty() {
        sub_chunks.push(current);
    }
    Ok(sub_chunks)
}

/// If the final chunk is below `lower_bound_bytes` (i.e., outside the desired
/// output size range) and the previous chunk is not an oversized split, merge
/// them when their combined size still fits inside `upper_bound_bytes`.
///
/// A tail at or above `lower_bound_bytes` is already a legitimately-sized file
/// — leave it alone.
///
/// # Partition invariant
///
/// This function is called only from [`plan_bucket`], after `plan_row_groups`
/// has split inputs by opaque [`PartitionBucket`].  The caller guarantees that
/// every chunk in `chunks` belongs to the same planner partition bucket.  If
/// `plan_row_groups` is ever restructured to flatten buckets before calling
/// `tail_merge`, an explicit bucket equality guard must be added here.
///
/// Returns `true` when a merge happens.
fn tail_merge(chunks: &mut Vec<PlannedChunk>, limits: &PlannerConfig) -> bool {
    if chunks.len() < 2 {
        return false;
    }
    let last_idx = chunks.len() - 1;
    let prev_idx = last_idx - 1;
    let last_bytes = chunks[last_idx].total_bytes;
    let prev_bytes = chunks[prev_idx].total_bytes;
    let prev_is_oversized_split = chunks[prev_idx].is_oversized_split;
    let last_is_oversized_split = chunks[last_idx].is_oversized_split;
    if prev_is_oversized_split || last_is_oversized_split {
        return false;
    }
    if last_bytes >= limits.lower_bound_bytes {
        return false;
    }
    if prev_bytes.saturating_add(last_bytes) > limits.upper_bound_bytes {
        return false;
    }
    let prev_rg_count = chunks[prev_idx].row_groups.len();
    let last_rg_count = chunks[last_idx].row_groups.len();
    if prev_rg_count.saturating_add(last_rg_count) > limits.max_row_groups {
        return false;
    }

    // Bounds checked above (`chunks.len() >= 2`); `pop`/`last_mut` cannot return
    // None.
    let Some(last) = chunks.pop() else {
        return false;
    };
    let Some(prev) = chunks.last_mut() else {
        // Should be unreachable; restore the popped value to keep state consistent.
        chunks.push(last);
        return false;
    };
    prev.total_bytes = prev.total_bytes.saturating_add(last.total_bytes);
    prev.row_groups.extend(last.row_groups);
    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        Cluster, PartitionBucket, PartitionValue, PlanRowGroup, PlannedChunk, PlannerConfig, RowGroupPartition,
        bin_pack, partition_by_bucket, sort_by_min_key, swept_line_cluster, tail_merge,
    };
    use crate::{
        error::IngestError,
        wal::{RowGroupBoundaryKey, RowGroupBoundaryRange, test_utils::boundary_component_timestamp_micros},
    };

    /// Build a row group with a single timestamp-only sort key
    /// (`min_ts`, `max_ts`) for tests; `descending=false` so larger values
    /// are "later" in sort order — this keeps the test setup intuitive.
    fn rg(wal_offset: u64, row_group_idx: usize, bytes: u64, min_ts: i64, max_ts: i64) -> PlanRowGroup {
        PlanRowGroup {
            wal_offset,
            row_group_idx,
            row_group_bytes: bytes,
            boundary_range: RowGroupBoundaryRange {
                names: Arc::from(["sort_key".to_string()]),
                min_key: RowGroupBoundaryKey::new(vec![boundary_component_timestamp_micros(Some(min_ts), false, true)]),
                max_key: RowGroupBoundaryKey::new(vec![boundary_component_timestamp_micros(Some(max_ts), false, true)]),
            },
            partition: RowGroupPartition::Single(test_bucket("bucket-a", 0)),
        }
    }

    fn rg_with_bucket(
        wal_offset: u64,
        row_group_idx: usize,
        bytes: u64,
        min_ts: i64,
        max_ts: i64,
        bucket: PartitionBucket,
    ) -> PlanRowGroup {
        PlanRowGroup {
            partition: RowGroupPartition::Single(bucket),
            ..rg(wal_offset, row_group_idx, bytes, min_ts, max_ts)
        }
    }

    fn cross_partition_rg(
        wal_offset: u64,
        row_group_idx: usize,
        bytes: u64,
        min_ts: i64,
        max_ts: i64,
        bucket: PartitionBucket,
    ) -> PlanRowGroup {
        PlanRowGroup {
            partition: RowGroupPartition::CrossPartition(bucket),
            ..rg(wal_offset, row_group_idx, bytes, min_ts, max_ts)
        }
    }

    fn test_bucket(label: &str, day: i32) -> PartitionBucket {
        PartitionBucket(vec![PartitionValue::String(Arc::from(label)), PartitionValue::Day(day)])
    }

    /// Build planner limits with `upper = 2 * lower` — preserves the previous
    /// `from_target` test surface while exercising the new bounds API.
    fn limits(lower: u64, max_rg: usize) -> PlannerConfig {
        PlannerConfig::from_bounds(lower, lower.saturating_mul(2), max_rg)
    }

    fn limits_with_upper(lower: u64, upper: u64, max_rg: usize) -> PlannerConfig {
        PlannerConfig::from_bounds(lower, upper, max_rg)
    }

    // ---- sort_by_min_key ----

    #[test]
    fn sort_orders_by_min_key_ascending() {
        let mut rgs = vec![rg(0, 2, 1, 30, 40), rg(0, 0, 1, 10, 20), rg(0, 1, 1, 20, 30)];
        sort_by_min_key(&mut rgs);
        assert_eq!(rg_idxs_vec(&rgs), vec![(0, 0), (0, 1), (0, 2)]);
    }

    #[test]
    fn sort_is_stable_on_equal_min_keys() {
        let mut rgs = vec![rg(0, 0, 1, 10, 20), rg(1, 5, 1, 10, 50), rg(2, 7, 1, 10, 30)];
        sort_by_min_key(&mut rgs);
        // Inputs all share min_key=10 — stable sort must preserve the input order.
        assert_eq!(rg_idxs_vec(&rgs), vec![(0, 0), (1, 5), (2, 7)]);
    }

    fn rg_idxs_vec(rgs: &[PlanRowGroup]) -> Vec<(u64, usize)> {
        rgs.iter().map(|r| (r.wal_offset, r.row_group_idx)).collect()
    }

    fn cluster(row_groups: Vec<PlanRowGroup>) -> Cluster {
        let total_bytes = row_groups.iter().map(|rg| rg.row_group_bytes).sum();
        Cluster {
            row_groups,
            total_bytes,
        }
    }

    // ---- swept_line_cluster ----

    #[test]
    fn cluster_disjoint_inputs_yields_separate_clusters() {
        let rgs = vec![rg(0, 0, 1, 0, 9), rg(0, 1, 1, 10, 19), rg(0, 2, 1, 20, 29)];
        let clusters = swept_line_cluster(rgs);
        assert_eq!(clusters.len(), 3);
    }

    #[test]
    fn cluster_overlapping_inputs_collapse_into_one() {
        let rgs = vec![rg(0, 0, 1, 0, 50), rg(0, 1, 1, 10, 30), rg(0, 2, 1, 25, 40)];
        let clusters = swept_line_cluster(rgs);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].row_groups.len(), 3);
        assert_eq!(clusters[0].total_bytes, 3);
    }

    #[test]
    fn cluster_transitive_overlap_is_one_cluster() {
        // A overlaps B (B.min=20 <= A.max=25), B overlaps C (C.min=40 <= B.max=45),
        // A does NOT directly overlap C (C.min=40 > A.max=25). Transitive overlap
        // must still collapse into a single cluster.
        let rgs = vec![rg(0, 0, 1, 0, 25), rg(0, 1, 1, 20, 45), rg(0, 2, 1, 40, 60)];
        let clusters = swept_line_cluster(rgs);
        assert_eq!(clusters.len(), 1);
        assert_eq!(clusters[0].row_groups.len(), 3);
        assert_eq!(clusters[0].total_bytes, 3);
    }

    #[test]
    fn cluster_mix_disjoint_and_overlapping() {
        // [0,10] | [20,30]+[25,35] | [50,60]
        let rgs = vec![
            rg(0, 0, 1, 0, 10),
            rg(0, 1, 1, 20, 30),
            rg(0, 2, 1, 25, 35),
            rg(0, 3, 1, 50, 60),
        ];
        let clusters = swept_line_cluster(rgs);
        assert_eq!(clusters.len(), 3);
        assert_eq!(clusters[0].row_groups.len(), 1);
        assert_eq!(clusters[0].total_bytes, 1);
        assert_eq!(clusters[1].row_groups.len(), 2);
        assert_eq!(clusters[1].total_bytes, 2);
        assert_eq!(clusters[2].row_groups.len(), 1);
        assert_eq!(clusters[2].total_bytes, 1);
    }

    // ---- bin_pack ----

    #[test]
    fn bin_pack_empty_input_yields_empty() {
        let (chunks, oversized) = bin_pack(Vec::new(), &limits(100, 100)).expect("ok");
        assert!(chunks.is_empty());
        assert_eq!(oversized, 0);
    }

    #[test]
    fn bin_pack_single_cluster_under_target() {
        let cluster = cluster(vec![rg(0, 0, 30, 0, 10)]);
        let (chunks, oversized) = bin_pack(vec![cluster], &limits(100, 100)).expect("ok");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].total_bytes, 30);
        assert_eq!(oversized, 0);
        assert!(!chunks[0].is_oversized_split);
    }

    #[test]
    fn bin_pack_two_clusters_exceed_upper_bound_split_into_two_chunks() {
        // Combined size 160 > upper_bound 128 → flush first, then accumulate second.
        let c1 = cluster(vec![rg(0, 0, 80, 0, 10)]);
        let c2 = cluster(vec![rg(0, 1, 80, 20, 30)]);
        let (chunks, _) = bin_pack(vec![c1, c2], &limits_with_upper(64, 128, 100)).expect("ok");
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].total_bytes, 80);
        assert_eq!(chunks[1].total_bytes, 80);
    }

    #[test]
    fn bin_pack_cluster_between_bounds_is_not_split() {
        // lower=100, upper=200: a 150-byte cluster must not trigger split_oversized.
        // c1(50)+big(150)=200 ≤ upper → packed together; then c3(40) overflows → own chunk.
        let c1 = cluster(vec![rg(0, 0, 50, 0, 10)]);
        let big = cluster(vec![rg(0, 1, 150, 20, 80)]);
        let c3 = cluster(vec![rg(0, 2, 40, 90, 100)]);
        let (chunks, oversized) = bin_pack(vec![c1, big, c3], &limits(100, 100)).expect("ok");
        assert_eq!(oversized, 0);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].total_bytes, 200);
        assert!(!chunks[0].is_oversized_split);
        assert_eq!(chunks[1].total_bytes, 40);
    }

    #[test]
    fn bin_pack_oversized_cluster_split_increments_counter() {
        // Cluster of three 100-byte row groups overlap each other → one cluster
        // of 300 bytes; hard_cap=200 forces a split into two sub-chunks.
        let cluster = cluster(vec![rg(0, 0, 100, 0, 50), rg(0, 1, 100, 10, 60), rg(0, 2, 100, 20, 70)]);
        let (chunks, oversized) = bin_pack(vec![cluster], &limits(100, 100)).expect("ok");
        assert_eq!(oversized, 1);
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|c| c.is_oversized_split));
        assert_eq!(chunks[0].total_bytes, 200);
        assert_eq!(chunks[1].total_bytes, 100);
    }

    #[test]
    fn bin_pack_small_tail_can_be_remerged_by_tail_merge() {
        let c1 = cluster(vec![rg(0, 0, 80, 0, 10)]);
        let c2 = cluster(vec![rg(0, 1, 30, 20, 30)]);
        let mut chunks = bin_pack(vec![c1, c2], &limits_with_upper(64, 128, 100)).expect("ok").0;
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].total_bytes, 80);
        assert_eq!(chunks[1].total_bytes, 30);
        assert!(tail_merge(&mut chunks, &limits_with_upper(64, 128, 100)));
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].total_bytes, 110);
    }

    #[test]
    fn bin_pack_max_row_groups_limit_triggers_flush_before_bytes_limit() {
        // target_bytes=1_000 (way above the actual sizes) but max_row_groups=2.
        let c1 = cluster(vec![rg(0, 0, 1, 0, 10)]);
        let c2 = cluster(vec![rg(0, 1, 1, 20, 30)]);
        let c3 = cluster(vec![rg(0, 2, 1, 40, 50)]);
        let (chunks, _) = bin_pack(vec![c1, c2, c3], &limits(1_000, 2)).expect("ok");
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].row_groups.len(), 2);
        assert_eq!(chunks[1].row_groups.len(), 1);
    }

    /// ТЗ Case 4: oversized cluster (fat service) followed by disjoint normal
    /// clusters (skinny services).  The last sub-chunk of the fat split must
    /// absorb the skinny clusters → 2 output files, not 3.
    #[test]
    fn bin_pack_oversized_trailing_sub_chunk_absorbs_subsequent_disjoint_clusters() {
        // fat: 3 overlapping RGs × 50 B = 150 B > upper(128) → split into
        //   sub1 = RG0+RG1 = 100 B, sub2 = RG2 = 50 B (left in `current`).
        // skinny: 2 disjoint clusters × 20 B each.
        // Expected: chunk1=100 B (oversized_split), chunk2=90 B (50+40, oversized_split).
        let fat = cluster(vec![rg(0, 0, 50, 0, 50), rg(0, 1, 50, 10, 60), rg(0, 2, 50, 20, 70)]);
        let skinny1 = cluster(vec![rg(1, 0, 20, 200, 210)]);
        let skinny2 = cluster(vec![rg(2, 0, 20, 220, 230)]);
        let lim = limits_with_upper(64, 128, 100);
        let (chunks, oversized) = bin_pack(vec![fat, skinny1, skinny2], &lim).expect("ok");
        assert_eq!(oversized, 1);
        assert_eq!(
            chunks.len(),
            2,
            "fat trailing sub-chunk + skinny must merge into one file"
        );
        assert_eq!(chunks[0].total_bytes, 100);
        assert!(chunks[0].is_oversized_split);
        assert_eq!(chunks[1].total_bytes, 90);
        assert!(chunks[1].is_oversized_split);
    }

    /// ТЗ Case 2 regression: oversized cluster with no subsequent clusters must
    /// still produce exactly 2 sub-chunks, both marked `is_oversized_split`.
    #[test]
    fn bin_pack_oversized_without_disjoint_tail_stays_two_sub_chunks() {
        let cluster = cluster(vec![rg(0, 0, 100, 0, 50), rg(0, 1, 100, 10, 60), rg(0, 2, 100, 20, 70)]);
        let (chunks, oversized) = bin_pack(vec![cluster], &limits(100, 100)).expect("ok");
        assert_eq!(oversized, 1);
        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|c| c.is_oversized_split));
        assert_eq!(chunks[0].total_bytes, 200);
        assert_eq!(chunks[1].total_bytes, 100);
    }

    #[test]
    fn bin_pack_single_row_group_exceeding_upper_bound_is_error() {
        // A WAL row group that is itself larger than upper_bound_bytes cannot be
        // split by the planner — this is a misconfiguration and must fail-fast.
        // upper=100, single overlapping cluster whose lone RG is 150 bytes.
        let cluster = cluster(vec![rg(0, 0, 150, 0, 50), rg(0, 1, 10, 10, 60)]);
        let lim = limits_with_upper(64, 100, 100);
        let err = bin_pack(vec![cluster], &lim).expect_err("must fail when single rg > upper_bound");
        assert!(matches!(err, IngestError::Shift(_)));
    }

    // ---- tail_merge ----

    #[test]
    fn tail_merge_single_chunk_is_noop() {
        let mut chunks = vec![PlannedChunk {
            row_groups: vec![rg(0, 0, 30, 0, 10)],
            total_bytes: 30,
            is_oversized_split: false,
        }];
        assert!(!tail_merge(&mut chunks, &limits(100, 100)));
        assert_eq!(chunks.len(), 1);
    }

    #[test]
    fn tail_merge_skips_when_tail_at_or_above_lower_bound() {
        // Tail already inside the desired output range — leave it alone.
        let mut chunks = vec![
            PlannedChunk {
                row_groups: vec![rg(0, 0, 80, 0, 10)],
                total_bytes: 80,
                is_oversized_split: false,
            },
            PlannedChunk {
                row_groups: vec![rg(0, 1, 70, 20, 30)],
                total_bytes: 70,
                is_oversized_split: false,
            },
        ];
        // lower=64, upper=128; tail=70 >= 64 -> no merge (regardless of room).
        assert!(!tail_merge(&mut chunks, &limits_with_upper(64, 128, 100)));
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn tail_merge_merges_when_tail_below_lower_bound_and_combined_within_upper() {
        // Case 10 from sort_in_planner.md: chunk1=80, chunk2=30, lower=64,
        // upper=128 -> single 110-byte chunk.
        let mut chunks = vec![
            PlannedChunk {
                row_groups: vec![rg(0, 0, 80, 0, 10)],
                total_bytes: 80,
                is_oversized_split: false,
            },
            PlannedChunk {
                row_groups: vec![rg(0, 1, 30, 20, 30)],
                total_bytes: 30,
                is_oversized_split: false,
            },
        ];
        assert!(tail_merge(&mut chunks, &limits_with_upper(64, 128, 100)));
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].total_bytes, 110);
    }

    #[test]
    fn tail_merge_skips_when_combined_exceeds_upper_bound() {
        // Case 11: chunk1=120, chunk2=10, upper=128 -> combined=130 > 128 ->
        // keep two chunks even though tail=10 < lower=64.
        let mut chunks = vec![
            PlannedChunk {
                row_groups: vec![rg(0, 0, 120, 0, 10)],
                total_bytes: 120,
                is_oversized_split: false,
            },
            PlannedChunk {
                row_groups: vec![rg(0, 1, 10, 20, 30)],
                total_bytes: 10,
                is_oversized_split: false,
            },
        ];
        assert!(!tail_merge(&mut chunks, &limits_with_upper(64, 128, 100)));
        assert_eq!(chunks.len(), 2);
    }

    #[test]
    fn tail_merge_skips_when_previous_is_oversized_split() {
        let mut chunks = vec![
            PlannedChunk {
                row_groups: vec![rg(0, 0, 80, 0, 10)],
                total_bytes: 80,
                is_oversized_split: true,
            },
            PlannedChunk {
                row_groups: vec![rg(0, 1, 10, 20, 30)],
                total_bytes: 10,
                is_oversized_split: false,
            },
        ];
        assert!(!tail_merge(&mut chunks, &limits(100, 100)));
        assert_eq!(chunks.len(), 2);
    }

    // ---- partition_by_bucket ----

    #[test]
    fn partition_by_bucket_groups_matching_buckets() {
        let bucket = test_bucket("bucket-a", 0);
        let (buckets, cross_partition) = partition_by_bucket(vec![
            rg_with_bucket(0, 0, 100, 100, 200, bucket.clone()),
            rg_with_bucket(0, 1, 100, 300, 400, bucket.clone()),
        ]);
        assert_eq!(buckets.len(), 1);
        assert!(cross_partition.is_empty());
        assert_eq!(buckets[&bucket].len(), 2);
    }

    #[test]
    fn partition_by_bucket_separates_different_buckets() {
        let bucket0 = test_bucket("bucket-a", 0);
        let bucket1 = test_bucket("bucket-a", 1);
        let (by_bucket, cross_partition) = partition_by_bucket(vec![
            rg_with_bucket(0, 0, 100, 100, 200, bucket0.clone()),
            rg_with_bucket(1, 0, 100, 300, 400, bucket1.clone()),
        ]);
        assert_eq!(by_bucket.len(), 2);
        assert!(cross_partition.is_empty());
        assert_eq!(by_bucket[&bucket0].len(), 1);
        assert_eq!(by_bucket[&bucket1].len(), 1);
    }

    #[test]
    fn partition_by_bucket_sends_cross_partition_row_group_to_overflow_bucket() {
        let bucket = PartitionBucket(vec![PartitionValue::String(Arc::from("bucket-a"))]);
        let (buckets, cross_partition) =
            partition_by_bucket(vec![cross_partition_rg(0, 0, 100, 100, 200, bucket.clone())]);
        assert!(buckets.is_empty());
        assert_eq!(cross_partition[&bucket].len(), 1);
    }

    // ---- plan_row_groups with partitioning ----

    #[test]
    fn plan_row_groups_single_day_workload_behavior_unchanged() {
        // All row groups in day 0 → identical behaviour to the pre-partitioning code.
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        let rgs = vec![rg(0, 0, 30, 0, 10), rg(0, 1, 30, 20, 30)];
        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(rgs, &limits(100, 100), &cancel_token).expect("ok");
        assert_eq!(chunks.len(), 1);
        assert_eq!(stats.cross_partition_row_groups, 0);
    }

    #[test]
    fn plan_row_groups_different_buckets_create_two_chunks() {
        // Row groups in different partition buckets must produce separate chunks.
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        let rg_before = rg_with_bucket(0, 0, 30, 0, 10, test_bucket("bucket-a", 0));
        let rg_after = rg_with_bucket(1, 0, 30, 20, 30, test_bucket("bucket-a", 1));
        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(vec![rg_before, rg_after], &limits(100, 100), &cancel_token).expect("ok");
        assert_eq!(chunks.len(), 2);
        assert_eq!(stats.cross_partition_row_groups, 0);
    }

    #[test]
    fn plan_row_groups_cross_partition_rg_counted_in_stats() {
        // A cross-partition row group ends up in an overflow bucket.
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        let rg_cross = cross_partition_rg(
            0,
            0,
            30,
            0,
            10,
            PartitionBucket(vec![PartitionValue::String(Arc::from("bucket-a"))]),
        );
        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(vec![rg_cross], &limits(100, 100), &cancel_token).expect("ok");
        assert_eq!(chunks.len(), 1);
        assert_eq!(stats.cross_partition_row_groups, 1);
    }

    // ---- plan_row_groups partition isolation ----

    /// `tail_merge` must skip when the previous and tail chunks live in
    /// different partition buckets — even when their byte sums fit inside
    /// `upper_bound_bytes`.  The numbers (chunk1=120 MB, chunk2=10 MB,
    /// sum=130 MB > upper=128 MB) deliberately *also* fail the upper-bound
    /// guard, so the assertion proves both invariants hold simultaneously.
    /// Partition isolation is structural here: `plan_row_groups` calls
    /// `tail_merge` once per bucket, so cross-bucket merging is impossible to
    /// express through the public API regardless of the byte budget.
    #[test]
    fn plan_row_groups_skips_tail_merge_when_partition_tuple_differs_case11() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        // 120 MB cluster in bucket day0 + 10 MB cluster in bucket day1.
        let rg_day0 = rg_with_bucket(0, 0, 120 * 1024 * 1024, 0, 10, test_bucket("bucket-a", 0));
        let rg_day1 = rg_with_bucket(0, 1, 10 * 1024 * 1024, 20, 30, test_bucket("bucket-a", 1));
        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(
            vec![rg_day0, rg_day1],
            &limits_with_upper(64 * 1024 * 1024, 128 * 1024 * 1024, 100),
            &cancel_token,
        )
        .expect("ok");
        assert_eq!(chunks.len(), 2, "different partition buckets must not tail-merge");
        assert!(!stats.tail_merged, "tail_merge must not have triggered across buckets");
        let mut sizes: Vec<u64> = chunks.iter().map(|c| c.total_bytes).collect();
        sizes.sort_unstable();
        assert_eq!(sizes, vec![10 * 1024 * 1024, 120 * 1024 * 1024]);
    }

    /// Guard that cross-day row groups whose timestamps straddle different day
    /// pairs are never packed into one chunk.  Before the fix, both row groups
    /// would land in the same `CrossPartition` bucket keyed only by tenant, so
    /// `bin_pack` could merge them into a single shift task, causing Iceberg's
    /// partition fanout to write multiple day-partition files per task.
    #[test]
    fn plan_row_groups_cross_day_rgs_with_different_day_pairs_produce_separate_chunks() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        // Two cross-day row groups for the same tenant but straddling different
        // day boundaries: (day0, day1) and (day1, day2).  Each must go into its
        // own chunk so the writer never fans out across multiple day partitions.
        let bucket_day0_1 = PartitionBucket(vec![
            PartitionValue::String(Arc::from("tenant-a")),
            PartitionValue::Day(0),
            PartitionValue::Day(1),
        ]);
        let bucket_day1_2 = PartitionBucket(vec![
            PartitionValue::String(Arc::from("tenant-a")),
            PartitionValue::Day(1),
            PartitionValue::Day(2),
        ]);
        let rg_cross_0_1 = cross_partition_rg(0, 0, 30, 0, 10, bucket_day0_1);
        let rg_cross_1_2 = cross_partition_rg(1, 0, 30, 20, 30, bucket_day1_2);
        let cancel_token = CancellationToken::new();
        let (chunks, stats) =
            plan_row_groups(vec![rg_cross_0_1, rg_cross_1_2], &limits(100, 100), &cancel_token).expect("ok");
        assert_eq!(
            chunks.len(),
            2,
            "cross-day rgs from different day pairs must not share a chunk"
        );
        assert_eq!(stats.cross_partition_row_groups, 2);
    }

    // ---- plan_row_groups domain end-to-end (ТЗ Cases 1, 2, 3, 4, 12) ----

    /// ТЗ Case 1: a single fat service producing monotonic, disjoint row groups
    /// must be split into ~`upper_bound`-sized chunks. None of the chunks can
    /// be marked `is_oversized_split`, and `oversized_clusters` must stay at 0
    /// because every cluster is below the upper bound.
    #[test]
    fn plan_row_groups_fat_monotonic_service_yields_three_lower_bound_chunks() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        // Thirty disjoint row groups of ~6.7 MB → 200 MB total. lower=64,
        // upper=128 MB.  The bin-packer flushes every chunk once it crosses
        // lower_bound, producing three chunks of ~67 MB each.
        let rg_size: u64 = 6_700_000;
        let rgs: Vec<PlanRowGroup> = (0..30)
            .map(|i| {
                #[allow(clippy::cast_possible_wrap)]
                let i_i64 = i as i64;
                rg(0, i, rg_size, i_i64 * 100, i_i64 * 100 + 50)
            })
            .collect();
        let lim = limits_with_upper(64_000_000, 128_000_000, 1_000);
        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(rgs, &lim, &cancel_token).expect("ok");

        // 30 disjoint clusters of 6.7 MB; bin_pack flushes once the chunk
        // crosses lower_bound (64 MB), so each chunk gets exactly 10 row
        // groups (10 * 6_700_000 = 67_000_000) — strictly inside
        // [lower_bound, upper_bound].  Pinning the sizes is what guards
        // against the small-file regression: any drift to chunks below
        // lower_bound would fail the lower-bound assertion.
        assert_eq!(chunks.len(), 3, "200 MB / 64 MB lower bound => 3 chunks");
        assert_eq!(stats.oversized_clusters, 0);
        assert!(!stats.tail_merged, "all chunks already >= lower_bound");
        for chunk in &chunks {
            assert!(!chunk.is_oversized_split);
            assert_eq!(chunk.row_groups.len(), 10, "each chunk must hold 10 row groups");
            assert_eq!(chunk.total_bytes, 67_000_000);
            assert!(
                chunk.total_bytes >= lim.lower_bound_bytes,
                "chunk must reach lower bound"
            );
            assert!(chunk.total_bytes <= lim.upper_bound_bytes);
        }
    }

    /// ТЗ Case 2: an overlapping cluster larger than `upper_bound` must be
    /// linearly split.  Both sub-chunks are marked `is_oversized_split`, the
    /// counter increments, and neither sub-chunk exceeds `upper_bound`.
    #[test]
    fn plan_row_groups_overlapping_oversized_cluster_yields_two_oversized_split_chunks() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        // Four overlapping 50-byte row groups => one cluster of 200 bytes.
        // upper=128 forces the splitter into two sub-chunks of 100 bytes each
        // (50+50 fits, +50 overflows, so flush; remaining 50+50 forms sub2).
        let rgs = vec![
            rg(0, 0, 50, 0, 100),
            rg(0, 1, 50, 50, 150),
            rg(0, 2, 50, 90, 200),
            rg(0, 3, 50, 120, 220),
        ];
        let lim = limits_with_upper(64, 128, 100);
        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(rgs, &lim, &cancel_token).expect("ok");

        assert_eq!(chunks.len(), 2);
        assert_eq!(stats.oversized_clusters, 1);
        for chunk in &chunks {
            assert!(chunk.is_oversized_split);
            assert!(chunk.total_bytes <= lim.upper_bound_bytes);
        }
    }

    /// ТЗ Case 3: many thin disjoint clusters with combined size below
    /// `upper_bound` must collapse into a single chunk that sits below
    /// `lower_bound` — there is simply no more data to fill the chunk and the
    /// last chunk is the only one, so tail-merge has nothing to merge.
    #[test]
    fn plan_row_groups_many_thin_disjoint_services_collapse_into_one_chunk() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        let rgs: Vec<PlanRowGroup> = (0..20)
            .map(|i| {
                #[allow(clippy::cast_possible_wrap)]
                let i_i64 = i as i64;
                rg(0, i, 3, i_i64 * 100, i_i64 * 100 + 50)
            })
            .collect();
        let lim = limits_with_upper(64, 128, 1_000);
        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(rgs, &lim, &cancel_token).expect("ok");

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].total_bytes, 60);
        assert!(chunks[0].total_bytes < lim.lower_bound_bytes);
        assert_eq!(stats.oversized_clusters, 0);
        assert!(!stats.tail_merged, "single-chunk run cannot tail-merge");
    }

    /// ТЗ Case 4: fat service + skinny tail.  After bin-packing the planner
    /// emits a small last chunk; tail-merge folds it into the previous chunk
    /// because the combined size still fits inside `upper_bound`.
    #[test]
    fn plan_row_groups_fat_then_skinny_tail_merged_into_previous_chunk() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        // First two disjoint clusters of 70 bytes each cross lower_bound and
        // flush, then a 10-byte skinny tail.  Tail-merge folds the 10-byte
        // tail into the second 70-byte chunk (combined 80 ≤ upper=128).
        let rgs = vec![rg(0, 0, 70, 0, 10), rg(0, 1, 70, 20, 30), rg(0, 2, 10, 40, 50)];
        let lim = limits_with_upper(64, 128, 100);
        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(rgs, &lim, &cancel_token).expect("ok");

        assert_eq!(chunks.len(), 2, "skinny tail must merge into previous chunk");
        assert!(stats.tail_merged);
        // Sort sizes because partition_by_bucket is HashMap-ordered.
        let mut sizes: Vec<u64> = chunks.iter().map(|c| c.total_bytes).collect();
        sizes.sort_unstable();
        assert_eq!(sizes, vec![70, 80]);
    }

    /// ТЗ Case 12 fail-fast: a single WAL row group inside an oversized
    /// cluster cannot exceed `upper_bound` — the planner cannot split a row
    /// group, so this is an operator misconfiguration and must surface as
    /// `IngestError::Shift`.
    #[test]
    fn plan_row_groups_oversized_cluster_with_single_rg_above_upper_is_error() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        // Two overlapping row groups: one 150 B (above upper=100), one 10 B.
        // The cluster total 160 > upper triggers split_oversized; the 150 B
        // row group then trips the per-row-group guard.
        let rgs = vec![rg(0, 0, 150, 0, 50), rg(0, 1, 10, 10, 60)];
        let lim = limits_with_upper(64, 100, 100);
        let cancel_token = CancellationToken::new();
        let err = plan_row_groups(rgs, &lim, &cancel_token).expect_err("rg > upper must error");
        assert!(matches!(err, IngestError::Shift(_)));
    }

    // ---- split_oversized boundaries ----

    /// A cluster sized *exactly* `upper_bound` must not trip `split_oversized`.
    /// The strict `>` comparison in `bin_pack` is what guarantees this; if it
    /// ever drifts to `>=`, an at-cap cluster would silently split and burn
    /// the oversized counter.
    #[test]
    fn split_oversized_at_exact_upper_bound_does_not_split() {
        // Two overlapping row groups of 64 bytes each = 128-byte cluster,
        // upper_bound = 128.
        let cluster = cluster(vec![rg(0, 0, 64, 0, 50), rg(0, 1, 64, 10, 60)]);
        let lim = limits_with_upper(64, 128, 100);
        let (chunks, oversized) = bin_pack(vec![cluster], &lim).expect("ok");
        assert_eq!(chunks.len(), 1, "cluster at exact upper bound stays as one chunk");
        assert_eq!(
            oversized, 0,
            "split_oversized counter must not increment at the boundary"
        );
        assert!(!chunks[0].is_oversized_split);
        assert_eq!(chunks[0].total_bytes, 128);
    }

    /// Sanity counterpart to [`split_oversized_at_exact_upper_bound_does_not_split`]:
    /// one byte over the boundary must trigger the split.
    #[test]
    fn split_oversized_at_upper_bound_plus_one_splits() {
        // 65 + 64 = 129 bytes, upper_bound = 128 → must split.
        let cluster = cluster(vec![rg(0, 0, 65, 0, 50), rg(0, 1, 64, 10, 60)]);
        let lim = limits_with_upper(64, 128, 100);
        let (chunks, oversized) = bin_pack(vec![cluster], &lim).expect("ok");
        assert_eq!(chunks.len(), 2);
        assert_eq!(oversized, 1);
        assert!(chunks.iter().all(|c| c.is_oversized_split));
    }

    // ---- pairwise overlap identical bounds ----

    /// A degenerate oversized cluster where every row group has identical
    /// `[min, max]` bounds.  After `split_oversized` the resulting sub-chunks
    /// must inherit those identical bounds — file-level pruning is fully
    /// degraded for this window, but the planner must still produce the
    /// expected `(127 MB, 74 MB)` shape with both chunks marked
    /// `is_oversized_split`.  Regression guard against silently dropping the
    /// oversized counter or smearing bounds.
    #[test]
    fn plan_row_groups_pairwise_overlap_yields_split_with_identical_bounds() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        // 30 row groups × 6.7 MB = 201 MB total. All share bounds [10, 80].
        let rg_size: u64 = 6_700_000;
        let rgs: Vec<PlanRowGroup> = (0..30).map(|i| rg(0, i, rg_size, 10, 80)).collect();
        let lim = limits_with_upper(64_000_000, 128_000_000, 1_000);
        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(rgs, &lim, &cancel_token).expect("ok");

        assert_eq!(chunks.len(), 2, "200 MB / 128 MB upper bound => 2 oversized sub-chunks");
        assert_eq!(stats.oversized_clusters, 1);
        for chunk in &chunks {
            assert!(chunk.is_oversized_split);
            assert!(chunk.total_bytes <= lim.upper_bound_bytes);
        }
        // Greedy split: first chunk packs as many RGs as fit ≤ upper_bound; the
        // remainder forms the second chunk.  19 × 6.7 MB = 127.3 MB ≤ 128 MB,
        // 20 × 6.7 MB = 134 MB > 128 MB → split point is 19.
        assert_eq!(chunks[0].row_groups.len(), 19);
        assert_eq!(chunks[1].row_groups.len(), 11);
        assert_eq!(chunks[0].total_bytes, 19 * rg_size);
        assert_eq!(chunks[1].total_bytes, 11 * rg_size);

        // All row groups share bounds [10, 80] → both sub-chunks must expose
        // identical min/max boundary keys (file-level pruning fully degraded).
        for chunk in &chunks {
            for plan_rg in &chunk.row_groups {
                assert_eq!(plan_rg.boundary_range, chunks[0].row_groups[0].boundary_range);
            }
        }
    }

    // ---- tail_merge above-lower-bound boundary ----

    /// Boundary refinement of [`tail_merge_skips_when_tail_at_or_above_lower_bound`]:
    /// the existing test pins `tail=70 ≥ lower=64`.  This one nails the exact
    /// off-by-one cliff: `tail=lower` (must skip) and `tail=lower+1` (must
    /// skip).  The under-cliff case `tail < lower → merges` is covered by
    /// [`tail_merge_merges_when_tail_below_lower_bound_and_combined_within_upper`].
    #[test]
    fn tail_merge_skips_when_tail_above_lower_bound() {
        let lim = limits_with_upper(64, 128, 100);
        for tail_bytes in [64u64, 65u64] {
            let mut chunks = vec![
                PlannedChunk {
                    row_groups: vec![rg(0, 0, 32, 0, 10)],
                    total_bytes: 32,
                    is_oversized_split: false,
                },
                PlannedChunk {
                    row_groups: vec![rg(0, 1, tail_bytes, 20, 30)],
                    total_bytes: tail_bytes,
                    is_oversized_split: false,
                },
            ];
            assert!(
                !tail_merge(&mut chunks, &lim),
                "tail={tail_bytes} >= lower=64 must not trigger tail_merge"
            );
            assert_eq!(chunks.len(), 2);
        }
    }

    // ---- plan_row_groups empty input ----

    /// An empty input must produce an empty plan without panicking or
    /// invoking any of the algorithm phases.
    #[test]
    fn plan_empty_row_groups_returns_empty_plan() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        let cancel_token = CancellationToken::new();
        let (chunks, stats) = plan_row_groups(Vec::new(), &limits(100, 100), &cancel_token).expect("ok");
        assert!(chunks.is_empty());
        assert_eq!(stats.clusters, 0);
        assert_eq!(stats.oversized_clusters, 0);
        assert_eq!(stats.cross_partition_row_groups, 0);
        assert!(!stats.tail_merged);
        assert_eq!(stats.max_cluster_bytes, 0);
    }

    // ---- plan_row_groups cancellation ----

    #[test]
    fn plan_row_groups_cancels_between_phases() {
        use tokio_util::sync::CancellationToken;

        use super::plan_row_groups;

        let row_groups = vec![rg(0, 0, 30, 0, 10), rg(0, 1, 30, 20, 30)];
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();
        let err = plan_row_groups(row_groups, &limits(100, 100), &cancel_token).expect_err("must abort on cancel");
        assert!(matches!(err, IngestError::Shift(_)));
    }
}
