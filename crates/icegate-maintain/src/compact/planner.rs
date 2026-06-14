//! Compaction planner: group, skip, cluster, size-split, and bin-pack data files.
//!
//! Given a table's data files (each carrying its `(tenant, day)` partition key,
//! byte size, and decoded sort-key envelope), [`plan_rewrite_groups`] produces
//! *rewrite groups*. Each group is a set of input files in one partition that a
//! later REWRITE task rewrites into approximately one output file.
//!
//! The algorithm is six stages:
//! 1. **Group** files by partition key, so a rewrite never spans partitions.
//! 2. **Skip healthy partitions.** A partition with few files and no
//!    significant tail of sub-target files is left untouched.
//! 3. **Cluster** the remaining files by transitive sort-key overlap
//!    ([`swept_line_cluster`](icegate_common::merge::cluster::swept_line_cluster)),
//!    so each cluster spans one contiguous sort-key range.
//! 4. **Split each cluster by size** (`split_cluster_by_size`) so a small file is
//!    not merged into a much larger, *near-target* one — which would re-read the
//!    large file for little gain. The gate engages only while a tier's largest
//!    file is at or above [`PlannerLimits::target_file_size_bytes`]; once the
//!    largest remaining file is itself sub-target, re-reading it is cheap and the
//!    whole remainder is merged. Above target, files within a
//!    [`PlannerLimits::max_merge_size_ratio`] factor of the largest stay together
//!    and smaller files split into lower size tiers, unless they collectively
//!    reach at least half the largest file *and* the cluster fits one rewrite
//!    group (then absorbing them is worthwhile). Excluding an overlapping large
//!    file trades some sort-key pruning for much lower read/write amplification.
//! 5. **Bin-pack** each size group by input-byte budget into one or more groups,
//!    so no single rewrite reads more than the configured cap.
//! 6. **Drop non-beneficial groups.** A group of a single file rewrites it
//!    1-to-1 with no file-count reduction; emitting it would make every scan
//!    re-rewrite the same file forever (a partition of disjoint, already-large
//!    files would never converge). Only groups of two or more files — which
//!    actually merge — are kept; a partition left with no such group is counted
//!    as skipped, not compacted.
//!
//! The planner is generic over [`PlannableFile`] so its logic is unit-testable
//! against a fake file without constructing an [`iceberg::spec::DataFile`].

use icegate_common::manifest_scan::DataFileStats;
use icegate_common::merge::cluster::swept_line_cluster;
use icegate_common::merge::sort_key::RowGroupBoundaryRange;

/// A file the planner can reason about: its partition, byte size, and sort-key
/// envelope.
///
/// Implemented by [`icegate_common::manifest_scan::DataFileStats`] in
/// production and by a fake in unit tests. The planner inspects only these
/// three projections; everything else about a file (path, record count) is the
/// caller's concern when it maps a returned group to a rewrite input.
pub trait PlannableFile {
    /// Stable `(tenant, day)` partition key. Files sharing this key may be
    /// grouped together; files with different keys never share a group.
    fn partition_key(&self) -> &str;

    /// On-disk size of the file in bytes, used for the bin-packing budget.
    fn size_bytes(&self) -> u64;

    /// Inclusive sort-key envelope used to cluster transitively-overlapping
    /// files.
    fn boundary_range(&self) -> &RowGroupBoundaryRange;
}

/// Plan over real Iceberg data files enumerated by the manifest scan. Each
/// projection delegates to the matching [`DataFileStats`] accessor, so the
/// production planner runs the exact same logic exercised by the unit-test
/// fake.
impl PlannableFile for DataFileStats {
    fn partition_key(&self) -> &str {
        self.partition_key()
    }

    fn size_bytes(&self) -> u64 {
        self.size_bytes()
    }

    fn boundary_range(&self) -> &RowGroupBoundaryRange {
        self.boundary_range()
    }
}

/// Tunable limits that decide which partitions to skip and how large a rewrite
/// group may grow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerLimits {
    /// A file at or above this size is considered already "large enough"; files
    /// below it are sub-target candidates for compaction.
    pub target_file_size_bytes: u64,
    /// Maximum summed input bytes a single rewrite group may read. A cluster
    /// larger than this is split into multiple groups.
    pub max_group_input_bytes: u64,
    /// A partition with at most this many files is a skip candidate (subject to
    /// the sub-target tail check below).
    pub min_input_files: usize,
    /// A skip-candidate partition is left untouched only when its number of
    /// sub-target files does not exceed this many.
    pub max_skippable_tail_files: usize,
    /// Largest-to-smallest size ratio allowed within one rewrite group.
    ///
    /// A file joins the cluster's largest only when its size times this ratio is
    /// at least the largest (i.e. it is no more than `ratio` times smaller);
    /// smaller files form their own size tier. Must be at least 1: a value of 0
    /// is rejected at startup (`Compactor::new_with_max_iterations`) and, as a
    /// defensive backstop, clamped to 1 by the planner.
    pub max_merge_size_ratio: u64,
}

/// Outcome of planning one table scan: the rewrite groups to fan out plus the
/// partition-level skip accounting the PLAN telemetry records.
///
/// The two counters are complementary over the table's partitions: every
/// partition is either *skipped* (contributing no group) or *compacted* (it
/// produced at least one rewrite group). A partition is skipped both when it is
/// healthy and when clustering leaves only non-beneficial single-file groups, so
/// `partitions_compacted` equals the number of distinct partition keys appearing
/// across [`groups`](Self::groups).
///
/// `Default` is hand-written (not derived) so it never requires `F: Default`:
/// `F` is a [`PlannableFile`] data-file type that has no meaningful default.
pub struct PlanOutcome<F> {
    /// Rewrite groups, one per output file. Every group is non-empty and all its
    /// files share one [`PlannableFile::partition_key`]. See
    /// [`plan_rewrite_groups`] for how the caller maps a group to a rewrite.
    pub groups: Vec<Vec<F>>,
    /// Partitions that produced at least one rewrite group.
    pub partitions_compacted: usize,
    /// Partitions skipped as healthy (no rewrite group emitted).
    pub partitions_skipped: usize,
}

impl<F> Default for PlanOutcome<F> {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            partitions_compacted: 0,
            partitions_skipped: 0,
        }
    }
}

/// Group, select, and bin-pack files into rewrite groups, one per output file.
///
/// Returns a [`PlanOutcome`]: the rewrite groups plus how many partitions were
/// compacted versus skipped. Every group holds at least two files (single-file
/// groups are dropped — see the module docs), and all files in a group share the
/// same [`PlannableFile::partition_key`]. A partition may yield zero groups (it
/// was skipped as healthy, or clustering left only single-file groups) or several
/// (a large or wide-overlap partition split by the byte budget). The caller maps
/// each returned group to a rewrite input from the underlying files' paths.
///
/// Generic over [`PlannableFile`] so the grouping, skip, clustering, and
/// bin-packing logic is exercised in unit tests without building real Iceberg
/// data files. See the module docs for the staged algorithm.
#[must_use]
pub fn plan_rewrite_groups<F: PlannableFile>(files: Vec<F>, cfg: &PlannerLimits) -> PlanOutcome<F> {
    let mut outcome = PlanOutcome::default();
    for partition_files in group_by_partition(files) {
        if is_healthy(&partition_files, cfg) {
            // Few files and a negligible sub-target tail: leave the partition
            // untouched so we never rewrite data that is already well-shaped.
            outcome.partitions_skipped += 1;
            continue;
        }
        // Cluster transitively-overlapping files, split each cluster into
        // size-similar tiers so a small file is not merged into a much larger
        // one, then bin-pack each tier by the input-byte budget.
        // `swept_line_cluster` stable-sorts by `min_key`, so the clustering and
        // the resulting groups are deterministic.
        let mut partition_groups: Vec<Vec<F>> = Vec::new();
        for cluster in swept_line_cluster(partition_files, PlannableFile::boundary_range) {
            for size_group in split_cluster_by_size(cluster, cfg) {
                bin_pack_into(size_group, cfg.max_group_input_bytes, &mut partition_groups);
            }
        }
        // Keep only groups that actually merge (>= 2 files). A single-file group
        // rewrites one file 1-to-1 with no benefit, so emitting it would make
        // every scan re-rewrite the same file forever — the re-compaction loop a
        // partition of disjoint, already-large files would otherwise spin in.
        // Dropping single-file groups leaves such files untouched.
        partition_groups.retain(|group| group.len() >= 2);
        if partition_groups.is_empty() {
            // Nothing beneficial to do (only lone/disjoint files survived):
            // report the partition as skipped so the telemetry matches the
            // "no rewrite scheduled" outcome.
            outcome.partitions_skipped += 1;
        } else {
            outcome.partitions_compacted += 1;
            outcome.groups.append(&mut partition_groups);
        }
    }
    outcome
}

/// Partition `files` by [`PlannableFile::partition_key`], preserving the order
/// in which each distinct partition was first seen so the output is
/// deterministic regardless of the hash map's internal ordering.
fn group_by_partition<F: PlannableFile>(files: Vec<F>) -> Vec<Vec<F>> {
    use std::collections::HashMap;

    let mut index_of: HashMap<String, usize> = HashMap::new();
    let mut partitions: Vec<Vec<F>> = Vec::new();
    for file in files {
        let next_index = partitions.len();
        // `entry` borrows the key, so clone only on first insertion.
        let index = *index_of.entry(file.partition_key().to_string()).or_insert(next_index);
        if index == next_index {
            partitions.push(Vec::new());
        }
        partitions[index].push(file);
    }
    partitions
}

/// Decide whether a single partition's files are healthy enough to skip.
///
/// A partition is healthy when it has at most `min_input_files` files AND its
/// number of sub-target files (smaller than `target_file_size_bytes`) does not
/// exceed `max_skippable_tail_files`. Such a partition is neither fragmented
/// nor dominated by a small-file tail, so compacting it would not pay off.
fn is_healthy<F: PlannableFile>(partition_files: &[F], cfg: &PlannerLimits) -> bool {
    if partition_files.len() > cfg.min_input_files {
        return false;
    }
    let sub_target = partition_files
        .iter()
        .filter(|file| file.size_bytes() < cfg.target_file_size_bytes)
        .count();
    sub_target <= cfg.max_skippable_tail_files
}

/// Pack `cluster` files (kept in cluster order) into rewrite groups whose summed
/// [`PlannableFile::size_bytes`] stays at or below `max_group_input_bytes`,
/// appending each finished group to `out`.
///
/// A new group is started when adding the next file would exceed the cap; a
/// single file larger than the cap forms its own group rather than being
/// dropped. The cluster is assumed non-empty (`swept_line_cluster` never emits
/// empty clusters), so at least one group is produced.
fn bin_pack_into<F: PlannableFile>(cluster: Vec<F>, max_group_input_bytes: u64, out: &mut Vec<Vec<F>>) {
    let mut current: Vec<F> = Vec::new();
    let mut current_bytes: u64 = 0;
    for file in cluster {
        let file_bytes = file.size_bytes();
        // Start a new group when the running group is non-empty and appending
        // this file would push it over the cap. `saturating_add` keeps the
        // comparison well-defined even for pathologically large sizes.
        if !current.is_empty() && current_bytes.saturating_add(file_bytes) > max_group_input_bytes {
            out.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(file_bytes);
        current.push(file);
    }
    if !current.is_empty() {
        out.push(current);
    }
}

/// Absorb a cluster's smaller files into its largest only when they collectively
/// reach `1 / LARGE_FILE_ABSORB_DENOMINATOR` (one half, e.g.) of the largest file's
/// size.
///
/// The threshold is applied as a multiplier on the *small* side —
/// `sum_small * LARGE_FILE_ABSORB_DENOMINATOR >= largest` — so the comparison
/// stays integer-exact. Do NOT rewrite it as `largest / DENOMINATOR`, which
/// truncates (and would risk a divide-by-zero if the constant ever became 0).
/// Hard-coded this iteration by design (see the size-aware-grouping spec);
/// promote it to config only if a workload needs a tunable absorb point.
const LARGE_FILE_ABSORB_DENOMINATOR: u64 = 2;

/// Split one overlap `cluster` into size-similar tiers so a small file is never
/// merged into a much larger, *near-target* one (which would re-read the large
/// file for little gain).
///
/// The gate only protects files that are expensive to re-read: it engages a tier
/// only while that tier's largest file is at or above
/// [`PlannerLimits::target_file_size_bytes`]. Once the largest remaining file is
/// itself sub-target, re-reading it is cheap and merging shrinks the file count,
/// so the whole remainder is kept together for `bin_pack_into` to size by byte
/// budget — this is what keeps a partition of skewed-but-all-small files
/// converging to a single output instead of staying fragmented.
///
/// Above target, a file stays in the current tier when its size is within a
/// `ratio` factor of the tier's largest (`size * ratio >= largest`); smaller
/// files drop into lower tiers. The whole remainder is absorbed into the large
/// file instead — bypassing the gate — when the smaller files collectively reach
/// at least half the largest AND the cluster fits one rewrite group
/// (`total <= max_group_input_bytes`). The fit check matters: without it the
/// byte-budget bin-packer could re-split an "absorbed" cluster and strand a file
/// in a single-file group that is then dropped, so the absorb would silently
/// fail to merge anything.
///
/// Each returned tier preserves the input's (sort-key) order, so a later
/// `bin_pack_into` still splits along sort order. A tier holding a single file is
/// emitted as-is and dropped downstream by the single-file-group filter. `ratio`
/// is clamped to at least 1 (it is also rejected at startup; see
/// `Compactor::new_with_max_iterations`). The split is iterative, so stack use is
/// constant regardless of how many size tiers a cluster spans.
fn split_cluster_by_size<F: PlannableFile>(cluster: Vec<F>, cfg: &PlannerLimits) -> Vec<Vec<F>> {
    // A ratio below 1 is meaningless (a file can never be `>= ratio`x the
    // largest); clamp so the function stays total even if startup validation is
    // bypassed (e.g. a future direct caller).
    let ratio = cfg.max_merge_size_ratio.max(1);
    let mut remaining = cluster;
    let mut tiers: Vec<Vec<F>> = Vec::new();
    loop {
        // A lone file is its own tier (dropped downstream as a single-file
        // group); nothing left to split.
        if remaining.len() < 2 {
            tiers.push(remaining);
            return tiers;
        }
        // The largest file anchors both the similarity gate and the absorb
        // override; `total` feeds the absorb fit test. Both are computed in one
        // pass with a plain comparison, so no `Option` and no file-count
        // invariant has to be re-asserted (`remaining` is non-empty here).
        let mut largest: u64 = 0;
        let mut total: u64 = 0;
        for file in &remaining {
            let size = file.size_bytes();
            if size > largest {
                largest = size;
            }
            total = total.saturating_add(size);
        }
        // TODO(closed-partition): when this partition is "closed" (its day is far
        // enough in the past that no further writes will land), bypass the size
        // gate so a final partition is fully compacted regardless of size
        // differences even when the largest file is over target. No closure
        // detection exists yet.
        if largest < cfg.target_file_size_bytes {
            // Even the largest remaining file is sub-target: re-reading it is
            // cheap, so keep the whole remainder as one tier and let the byte
            // budget size it.
            tiers.push(remaining);
            return tiers;
        }

        // A file is "small" when it is more than `ratio`x smaller than the
        // largest. Sum the small bytes for the absorb test in a second pass (it
        // needs `largest`).
        let is_small = |size: u64| size.saturating_mul(ratio) < largest;
        let mut sum_small: u64 = 0;
        let mut has_small = false;
        for file in &remaining {
            let size = file.size_bytes();
            if is_small(size) {
                sum_small = sum_small.saturating_add(size);
                has_small = true;
            }
        }
        if !has_small {
            // Every remaining file is within `ratio` of the largest: one tier.
            tiers.push(remaining);
            return tiers;
        }
        if total <= cfg.max_group_input_bytes && sum_small.saturating_mul(LARGE_FILE_ABSORB_DENOMINATOR) >= largest {
            // The small files reach half the largest and the whole cluster fits
            // one rewrite group, so absorbing them is worthwhile and bin-packing
            // will keep them together (nothing stranded): keep the cluster whole.
            tiers.push(remaining);
            return tiers;
        }

        // Peel the comparable tier off and continue on the smaller files.
        // `partition` preserves each side's relative (sort) order; the largest is
        // never small (`largest * ratio >= largest` for `ratio >= 1`), so
        // `comparable` is non-empty and `small` strictly shrinks — the loop
        // terminates.
        let (comparable, small): (Vec<F>, Vec<F>) =
            remaining.into_iter().partition(|file| !is_small(file.size_bytes()));
        tiers.push(comparable);
        remaining = small;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use icegate_common::merge::sort_key::{
        RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, RowGroupBoundaryValue,
    };

    use super::{PlannableFile, PlannerLimits, plan_rewrite_groups};

    /// A unit-test stand-in for a planner input, carrying just the three
    /// projections [`PlannableFile`] exposes. Building these avoids constructing
    /// real [`iceberg::spec::DataFile`]s in planner tests.
    struct FakeFile {
        partition: String,
        size: u64,
        range: RowGroupBoundaryRange,
    }

    impl PlannableFile for FakeFile {
        fn partition_key(&self) -> &str {
            &self.partition
        }

        fn size_bytes(&self) -> u64 {
            self.size
        }

        fn boundary_range(&self) -> &RowGroupBoundaryRange {
            &self.range
        }
    }

    /// Build a single-timestamp-column boundary range `[lo, hi]`, mirroring the
    /// pattern used by the `cluster` module tests.
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

    fn file(partition: &str, size: u64, lo: i64, hi: i64) -> FakeFile {
        FakeFile {
            partition: partition.to_string(),
            size,
            range: range(lo, hi),
        }
    }

    /// Limits with a 100 `KiB` target / 250 `KiB` group cap,
    /// `min_input_files = 4`, a sub-target tail tolerance of 2, and a 2x
    /// size-merge ratio.
    fn limits() -> PlannerLimits {
        PlannerLimits {
            target_file_size_bytes: 100_000,
            max_group_input_bytes: 250_000,
            min_input_files: 4,
            max_skippable_tail_files: 2,
            max_merge_size_ratio: 2,
        }
    }

    #[test]
    fn skips_healthy_partition() {
        // 3 files (<= min_input_files = 4), all at/above target, so there is no
        // sub-target tail. The partition is healthy and yields no groups.
        let files = vec![
            file("t/1", 120_000, 10, 20),
            file("t/1", 130_000, 30, 40),
            file("t/1", 110_000, 50, 60),
        ];
        let outcome = plan_rewrite_groups(files, &limits());
        assert!(
            outcome.groups.is_empty(),
            "healthy partition must produce no rewrite groups"
        );
        assert_eq!(
            outcome.partitions_skipped, 1,
            "the one healthy partition must be skipped"
        );
        assert_eq!(
            outcome.partitions_compacted, 0,
            "a skipped partition must not count as compacted"
        );
    }

    #[test]
    fn size_gate_leaves_oversized_file_untouched() {
        // A 200 KiB file overlapping four 20 KiB files. The smalls sum to 80 KiB,
        // well under half the 200 KiB large file. A 300 KiB budget keeps the whole
        // cluster within one rewrite group, so the absorb *mass* rule alone
        // (not the fit guard) is what leaves the large file out: it is dropped as
        // a lone group and only the smalls merge.
        let files = vec![
            file("t/1", 200_000, 10, 40),
            file("t/1", 20_000, 11, 39),
            file("t/1", 20_000, 12, 38),
            file("t/1", 20_000, 13, 37),
            file("t/1", 20_000, 14, 36),
        ];
        let outcome = plan_rewrite_groups(
            files,
            &PlannerLimits {
                max_group_input_bytes: 300_000,
                ..limits()
            },
        );
        assert_eq!(outcome.groups.len(), 1, "only the small files should form a group");
        let group = &outcome.groups[0];
        assert_eq!(group.len(), 4, "all four small files merge together");
        assert!(
            group.iter().all(|f| f.size_bytes() == 20_000),
            "the oversized file must not be pulled into the small-file group"
        );
        assert_eq!(outcome.partitions_compacted, 1);
    }

    #[test]
    fn size_override_absorbs_small_files_reaching_half() {
        // When the overlapping small files collectively reach at least half the
        // large file's size, absorbing them is worthwhile: the whole cluster
        // merges (120 KiB + four 20 KiB = 200 KiB, smalls 80 KiB >= 60 KiB).
        let files = vec![
            file("t/1", 120_000, 10, 40),
            file("t/1", 20_000, 11, 39),
            file("t/1", 20_000, 12, 38),
            file("t/1", 20_000, 13, 37),
            file("t/1", 20_000, 14, 36),
        ];
        let outcome = plan_rewrite_groups(files, &limits());
        assert_eq!(outcome.groups.len(), 1);
        let group = &outcome.groups[0];
        assert_eq!(group.len(), 5, "smalls summing to >= half the large are absorbed");
        assert!(
            group.iter().any(|f| f.size_bytes() == 120_000),
            "the large file must be part of the absorbing group"
        );
    }

    #[test]
    fn size_gate_splits_cluster_into_size_tiers() {
        // One overlap cluster spanning three size tiers must split into one
        // group per tier, never mixing far-apart sizes. A 1 KiB target keeps
        // every file over target so the size gate (not the sub-target shortcut)
        // governs, isolating pure size-tiering.
        let files = vec![
            file("t/1", 120_000, 10, 60),
            file("t/1", 60_000, 11, 59),
            file("t/1", 20_000, 12, 58),
            file("t/1", 20_000, 13, 57),
            file("t/1", 4_000, 14, 56),
            file("t/1", 4_000, 15, 55),
        ];
        let outcome = plan_rewrite_groups(
            files,
            &PlannerLimits {
                target_file_size_bytes: 1_000,
                ..limits()
            },
        );
        assert_eq!(outcome.groups.len(), 3, "three size tiers yield three groups");

        let mut tier_sizes: Vec<Vec<u64>> = outcome
            .groups
            .iter()
            .map(|group| {
                let mut sizes: Vec<u64> = group.iter().map(PlannableFile::size_bytes).collect();
                sizes.sort_unstable();
                sizes
            })
            .collect();
        tier_sizes.sort();
        assert_eq!(
            tier_sizes,
            vec![vec![4_000, 4_000], vec![20_000, 20_000], vec![60_000, 120_000],],
            "each group holds exactly one size tier"
        );
    }

    #[test]
    fn size_ratio_controls_comparability() {
        // A 50 KiB file overlapping a 120 KiB file is below 120/2, so at the
        // default 2x ratio neither file merges (both dropped as lone groups);
        // at a 3x ratio 50 KiB is comparable and the pair merges.
        let make = || vec![file("t/1", 120_000, 10, 40), file("t/1", 50_000, 12, 38)];
        let base = PlannerLimits {
            target_file_size_bytes: 100_000,
            max_group_input_bytes: 250_000,
            min_input_files: 1,
            max_skippable_tail_files: 0,
            max_merge_size_ratio: 2,
        };

        let strict = plan_rewrite_groups(make(), &base);
        assert!(
            strict.groups.is_empty(),
            "at ratio 2 the 50 KiB file is too small to join the 120 KiB file"
        );

        let loose = PlannerLimits {
            max_merge_size_ratio: 3,
            ..base
        };
        let merged = plan_rewrite_groups(make(), &loose);
        assert_eq!(merged.groups.len(), 1, "at ratio 3 the two files merge");
        assert_eq!(merged.groups[0].len(), 2);
    }

    #[test]
    fn size_ratio_zero_is_treated_as_one() {
        // A misconfigured ratio of 0 is clamped to 1 (only equal-or-larger files
        // share a tier) rather than panicking or merging everything: the two
        // equal 100 KiB files merge and the 20 KiB file is left out.
        let files = vec![
            file("t/1", 100_000, 10, 40),
            file("t/1", 100_000, 11, 39),
            file("t/1", 20_000, 12, 38),
        ];
        let limits = PlannerLimits {
            target_file_size_bytes: 100_000,
            max_group_input_bytes: 250_000,
            min_input_files: 1,
            max_skippable_tail_files: 0,
            max_merge_size_ratio: 0,
        };
        let outcome = plan_rewrite_groups(files, &limits);
        assert_eq!(
            outcome.groups.len(),
            1,
            "the two equal files merge, the small one is left out"
        );
        assert_eq!(outcome.groups[0].len(), 2);
        assert!(outcome.groups[0].iter().all(|f| f.size_bytes() == 100_000));
    }

    #[test]
    fn sub_target_files_merge_regardless_of_size_skew() {
        // Every file is below the 100 KiB target, so the "large" 90 KiB file is
        // cheap to re-read and the size gate must NOT protect it: the whole
        // overlapping cluster merges into one group instead of splitting by
        // relative size (which would leave the partition permanently fragmented).
        let files = vec![
            file("t/1", 90_000, 10, 40),
            file("t/1", 20_000, 11, 39),
            file("t/1", 20_000, 12, 38),
            file("t/1", 4_000, 13, 37),
            file("t/1", 4_000, 14, 36),
        ];
        let outcome = plan_rewrite_groups(files, &limits());
        assert_eq!(outcome.groups.len(), 1, "all sub-target files merge into one group");
        assert_eq!(outcome.groups[0].len(), 5, "no sub-target file is left out");
        assert_eq!(outcome.partitions_compacted, 1);
    }

    #[test]
    fn absorb_falls_back_to_tiers_when_cluster_exceeds_budget() {
        // The three 60 KiB smalls reach half the 200 KiB large file
        // (180 KiB >= 100 KiB), so the absorb override is tempted to keep the
        // whole 380 KiB cluster. But it exceeds the 250 KiB group budget, and the
        // large file sits between smalls in sort order, so byte-budget packing
        // would strand a small file in a single-file group and drop it. The fit
        // guard instead tiers, so all three similar 60 KiB files merge and none
        // is stranded; the 200 KiB file is left alone.
        let files = vec![
            file("t/1", 60_000, 10, 40),
            file("t/1", 200_000, 11, 39),
            file("t/1", 60_000, 12, 38),
            file("t/1", 60_000, 13, 37),
        ];
        let outcome = plan_rewrite_groups(files, &limits());
        assert_eq!(outcome.groups.len(), 1, "the three similar 60 KiB files merge");
        let group = &outcome.groups[0];
        assert_eq!(group.len(), 3, "no small file is stranded by byte-budget packing");
        assert!(
            group.iter().all(|f| f.size_bytes() == 60_000),
            "only the similar-size files merge; the 200 KiB file is left alone"
        );
        assert_eq!(
            outcome.partitions_compacted, 1,
            "the partition is compacted, not skipped"
        );
    }

    #[test]
    fn over_target_files_exceeding_budget_are_left_alone_while_smalls_merge() {
        // Two over-target files (200 KiB, 160 KiB) are size-similar but together
        // exceed the 250 KiB budget, so they cannot share one rewrite and are each
        // left alone — the skew goal of never re-reading a large file for little
        // gain. The three overlapping 60 KiB sub-target files still merge among
        // themselves. (Three smalls make the partition unhealthy: 3 > the tail
        // tolerance of 2.)
        let files = vec![
            file("t/1", 200_000, 10, 40),
            file("t/1", 160_000, 11, 39),
            file("t/1", 60_000, 12, 38),
            file("t/1", 60_000, 13, 37),
            file("t/1", 60_000, 14, 36),
        ];
        let outcome = plan_rewrite_groups(files, &limits());
        assert_eq!(outcome.groups.len(), 1, "only the three similar small files merge");
        let group = &outcome.groups[0];
        assert_eq!(group.len(), 3, "the two over-budget large files are left alone");
        assert!(group.iter().all(|f| f.size_bytes() == 60_000));
        assert_eq!(outcome.partitions_compacted, 1);
    }

    #[test]
    fn absorb_fires_exactly_at_half_the_largest() {
        // Pin the absorb threshold boundary independently of the fit guard. Both
        // clusters fit the budget; only the half-mass rule differs.
        let base = PlannerLimits {
            target_file_size_bytes: 100_000,
            max_group_input_bytes: 250_000,
            min_input_files: 1,
            max_skippable_tail_files: 0,
            max_merge_size_ratio: 2,
        };

        // Smalls sum to exactly half the largest (2 * 30 KiB = 60 KiB == 120/2),
        // tripping the `>=` threshold: the whole cluster is absorbed.
        let at_half = plan_rewrite_groups(
            vec![
                file("t/1", 120_000, 10, 40),
                file("t/1", 30_000, 12, 38),
                file("t/1", 30_000, 13, 37),
            ],
            &base,
        );
        assert_eq!(at_half.groups.len(), 1, "smalls at exactly half are absorbed");
        assert_eq!(
            at_half.groups[0].len(),
            3,
            "the large file is part of the absorbing group"
        );

        // One KiB less of smalls (59 KiB, 59 * 2 = 118 < 120) misses the
        // threshold, so the large file is left out and only the smalls merge.
        let below_half = plan_rewrite_groups(
            vec![
                file("t/1", 120_000, 10, 40),
                file("t/1", 29_000, 12, 38),
                file("t/1", 30_000, 13, 37),
            ],
            &base,
        );
        assert_eq!(below_half.groups.len(), 1, "below half, only the smalls merge");
        assert_eq!(below_half.groups[0].len(), 2, "the 120 KiB file is left out");
    }

    #[test]
    fn deeply_skewed_cluster_tiers_iteratively_without_overflow() {
        // A super-increasing size ladder (each file larger than the sum of all
        // smaller ones) defeats the absorb override at every level and peels one
        // singleton tier per file — the deepest split shape, and the case that
        // would blow the stack under recursion at a tight ratio. The iterative
        // planner handles 16 tiers with constant stack: every tier is a lone file
        // and is dropped, so the partition yields no group.
        let mut files = Vec::new();
        let mut size = 1_u64;
        for i in 0_i64..16 {
            size = size.saturating_mul(4); // 4, 16, 64, ... strictly super-increasing
            files.push(file("t/1", size, 10 + i, 100 - i)); // all overlapping
        }
        let limits = PlannerLimits {
            target_file_size_bytes: 1,
            max_group_input_bytes: u64::MAX,
            min_input_files: 1,
            max_skippable_tail_files: 0,
            max_merge_size_ratio: 1,
        };
        let outcome = plan_rewrite_groups(files, &limits);
        assert!(
            outcome.groups.is_empty(),
            "every size tier is a lone file and is dropped"
        );
        assert_eq!(outcome.partitions_skipped, 1);
    }

    #[test]
    fn bin_packs_by_input_budget_into_multiple_groups() {
        // Six transitively-overlapping 60 KiB files = 360 KiB > 250 KiB cap, all
        // in one partition. They cluster into one overlap chain that the 250 KiB
        // budget must split into more than one group, each <= the cap. Every kept
        // group still merges two or more files, so none is dropped as
        // non-beneficial.
        let files = vec![
            file("t/1", 60_000, 10, 100),
            file("t/1", 60_000, 20, 110),
            file("t/1", 60_000, 30, 120),
            file("t/1", 60_000, 40, 130),
            file("t/1", 60_000, 50, 140),
            file("t/1", 60_000, 60, 150),
        ];
        let cfg = limits();
        let groups = plan_rewrite_groups(files, &cfg).groups;

        assert!(
            groups.len() > 1,
            "input exceeding the group budget must split into multiple groups"
        );
        for group in &groups {
            assert!(group.len() >= 2, "a kept group must merge at least two files");
            let summed: u64 = group.iter().map(PlannableFile::size_bytes).sum();
            assert!(
                summed <= cfg.max_group_input_bytes,
                "each group's summed input bytes ({summed}) must stay within the budget"
            );
        }
        // The chain is one cluster that bin-packs without dropping any file.
        let total: usize = groups.iter().map(Vec::len).sum();
        assert_eq!(total, 6, "every input file must appear in exactly one group");
    }

    #[test]
    fn disjoint_partition_is_skipped_to_avoid_recompaction_loop() {
        // A partition with more than `min_input_files` files — so the count check
        // marks it unhealthy — whose files are all disjoint AND already at or
        // above target. Each file forms its own single-file cluster, so there is
        // nothing to merge. The planner must emit NO rewrite group and count the
        // partition as skipped; otherwise every scan would rewrite these files
        // 1-to-1 forever (the re-compaction loop this guards against).
        let files = vec![
            file("t/1", 120_000, 10, 20),
            file("t/1", 130_000, 30, 40),
            file("t/1", 140_000, 50, 60),
            file("t/1", 150_000, 70, 80),
            file("t/1", 160_000, 90, 100),
        ];
        let outcome = plan_rewrite_groups(files, &limits());
        assert!(
            outcome.groups.is_empty(),
            "a partition of disjoint, already-large files must produce no rewrite groups"
        );
        assert_eq!(
            outcome.partitions_skipped, 1,
            "such a partition is skipped, not compacted"
        );
        assert_eq!(
            outcome.partitions_compacted, 0,
            "no partition is compacted when nothing can be merged"
        );
    }

    #[test]
    fn respects_partition_boundaries() {
        // Two partitions, each unhealthy on its own (5 overlapping sub-target
        // files). Files from different partitions must never share a group, even
        // when their sort-key ranges overlap across partitions.
        let mut files = Vec::new();
        for _ in 0..5 {
            // Identical, fully-overlapping ranges so each partition's five files
            // cluster into one mergeable group; the same ranges appear in both
            // partitions to prove cross-partition files never share a group.
            files.push(file("t/1", 40_000, 0, 50));
            files.push(file("t/2", 40_000, 0, 50));
        }
        let outcome = plan_rewrite_groups(files, &limits());
        assert_eq!(
            outcome.partitions_compacted, 2,
            "both unhealthy partitions must count as compacted"
        );
        assert_eq!(
            outcome.partitions_skipped, 0,
            "neither partition is healthy enough to skip"
        );

        for group in &outcome.groups {
            let partitions: std::collections::BTreeSet<&str> = group.iter().map(PlannableFile::partition_key).collect();
            assert_eq!(
                partitions.len(),
                1,
                "a rewrite group must contain files from exactly one partition"
            );
        }
        // Both partitions produced at least one group.
        let seen: std::collections::BTreeSet<String> = outcome
            .groups
            .iter()
            .filter_map(|group| group.first().map(|f| f.partition_key().to_string()))
            .collect();
        assert_eq!(seen.len(), 2, "both partitions must yield rewrite groups");
    }
}
