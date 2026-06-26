//! Compaction planner: group, skip, size-split, and bin-pack data files.
//!
//! Given a table's data files (each carrying its `(tenant, day)` partition key,
//! byte size, and decoded sort-key envelope), [`plan_rewrite_groups`] produces
//! *rewrite groups*. Each group is a set of input files in one partition that a
//! later REWRITE task rewrites into approximately one output file.
//!
//! The algorithm is four stages:
//! 1. **Group** files by partition key, so a rewrite never spans partitions.
//! 2. **Skip healthy partitions.** A partition with few files and no
//!    significant tail of sub-target files is left untouched.
//! 3. **Split each partition's files by size** (`split_by_size`) so a
//!    small file is not merged into a much larger, *near-target* one — which
//!    would re-read the large file for little gain. The gate engages only while a
//!    tier's largest file is at or above [`PlannerLimits::target_file_size_bytes`];
//!    once the largest remaining file is itself sub-target, re-reading it is cheap
//!    and the whole remainder is merged. Above target, files within a
//!    [`PlannerLimits::max_merge_size_ratio`] factor of the largest stay together
//!    and smaller files split into lower size tiers, unless they collectively
//!    reach at least half the largest file *and* the partition fits one rewrite
//!    group (then absorbing them is worthwhile). The planner does NOT cluster by
//!    sort-key overlap, so files with disjoint sort-key ranges may share a group;
//!    the REWRITE k-way merge still produces a sorted output.
//! 4. **Bin-pack** each size group by input-byte budget into one or more groups,
//!    so no single rewrite reads more than the configured cap, then **drop
//!    non-beneficial groups**. A group is beneficial only if it both merges two
//!    or more files AND reduces the file count (`ceil(sum_bytes / target) < len`).
//!    A single-file group, or a group of files each already at or above target
//!    (`N` in → `N` out), reduces nothing; emitting it would make every scan
//!    re-rewrite the same files forever. Such groups are dropped; a partition left
//!    with no beneficial group is counted as skipped, not compacted.
//!
//! The planner is generic over [`PlannableFile`] so its logic is unit-testable
//! against a fake file without constructing an [`iceberg::spec::DataFile`].

use icegate_common::manifest_scan::DataFileStats;

/// A file the planner can reason about: its partition, byte size, and sort-key
/// envelope.
///
/// Implemented by [`icegate_common::manifest_scan::DataFileStats`] in
/// production and by a fake in unit tests. The planner inspects only these
/// two projections; everything else about a file (path, record count, sort-key
/// envelope) is the caller's concern when it maps a returned group to a rewrite
/// input.
pub trait PlannableFile {
    /// Stable `(tenant, day)` partition key. Files sharing this key may be
    /// grouped together; files with different keys never share a group.
    fn partition_key(&self) -> &str;

    /// On-disk size of the file in bytes, used for the bin-packing budget.
    fn size_bytes(&self) -> u64;
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
}

/// Tunable limits that decide which partitions to skip and how large a rewrite
/// group may grow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannerLimits {
    /// A file at or above this size is considered already "large enough"; files
    /// below it are sub-target candidates for compaction.
    pub target_file_size_bytes: u64,
    /// Maximum summed input bytes a single rewrite group may read. A size tier
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
    /// A file joins the largest file's tier only when its size times this ratio is
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
/// healthy and when size-splitting leaves only non-beneficial single-file groups,
/// so `partitions_compacted` equals the number of distinct partition keys
/// appearing across [`groups`](Self::groups).
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
/// compacted versus skipped. Every group holds at least two files AND reduces the
/// partition's file count (non-reducing groups are dropped — see the module docs),
/// and all files in a group share the same [`PlannableFile::partition_key`]. A
/// partition may yield zero groups (it was skipped as healthy, or size-splitting
/// left only non-reducing groups) or several (a large or skewed partition split by
/// the byte budget). The caller maps each returned group to a rewrite input from
/// the underlying files' paths.
///
/// Generic over [`PlannableFile`] so the grouping, skip, size-split, and
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
        // Split the partition's files into size-similar tiers so a small file is
        // not merged into a much larger one, then bin-pack each tier by the
        // input-byte budget. `group_by_partition` preserves first-seen order, so
        // the resulting groups are deterministic. There is no sort-key clustering:
        // files with disjoint ranges may share a group, and the REWRITE k-way
        // merge still produces a sorted output.
        let mut partition_groups: Vec<Vec<F>> = Vec::new();
        for size_group in split_by_size(partition_files, cfg) {
            bin_pack_into(size_group, cfg.max_group_input_bytes, &mut partition_groups);
        }
        // Keep only groups that actually MERGE (>= 2 files) AND REDUCE the file
        // count. A single-file group rewrites 1-to-1; a group whose inputs are
        // each already at or above target re-rolls N input files into N
        // target-sized outputs (no reduction). Either way, emitting it would make
        // every scan re-select and re-rewrite the same files forever — an
        // infinite loop that burns CPU/IO and never converges. `reduces_file_count`
        // predicts the output count from the re-roll at `target_file_size_bytes`.
        partition_groups.retain(|group| group.len() >= 2 && reduces_file_count(group, cfg.target_file_size_bytes));
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

/// Pack a size tier's `files` (kept in input order) into rewrite groups whose
/// summed [`PlannableFile::size_bytes`] stays at or below `max_group_input_bytes`,
/// appending each finished group to `out`.
///
/// A new group is started when adding the next file would exceed the cap; a
/// single file larger than the cap forms its own group rather than being
/// dropped. The input (a size tier from `split_by_size`) is assumed
/// non-empty, so at least one group is produced.
fn bin_pack_into<F: PlannableFile>(files: Vec<F>, max_group_input_bytes: u64, out: &mut Vec<Vec<F>>) {
    let mut current: Vec<F> = Vec::new();
    let mut current_bytes: u64 = 0;
    for file in files {
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

/// Whether rewriting `group` would actually reduce the partition's file count.
///
/// A REWRITE re-rolls the group's summed bytes into `target_file_size_bytes`-sized
/// outputs, so it emits roughly `ceil(sum_bytes / target)` files. Merging is only
/// worthwhile when that is strictly fewer than the inputs. The canonical
/// non-reducing case is a group of files each already at or above target: `N`
/// inputs re-roll into `N` outputs, the planner re-selects them on the next scan,
/// and compaction loops forever without converging. Two sub-target files whose
/// sum still exceeds one target file (e.g. two 60% files → 120% → two outputs)
/// are the same no-reduction trap. This predicate drops both.
///
/// `target_file_size_bytes` is validated `> 0` at startup
/// ([`crate::compact::config::CompactionConfig::validate`]); it is clamped to 1
/// here so the function stays total if a direct caller bypasses that.
fn reduces_file_count<F: PlannableFile>(group: &[F], target_file_size_bytes: u64) -> bool {
    let target = target_file_size_bytes.max(1);
    let sum_bytes = group.iter().map(PlannableFile::size_bytes).fold(0u64, u64::saturating_add);
    // Predicted outputs after re-rolling at the target size. `usize -> u64` for
    // the count comparison is lossless on every supported (<= 64-bit) target.
    let predicted_outputs = sum_bytes.div_ceil(target);
    predicted_outputs < group.len() as u64
}

/// Absorb the partition's smaller files into the largest only when they
/// collectively reach `1 / LARGE_FILE_ABSORB_DENOMINATOR` (one half) of the
/// largest file's size.
///
/// The threshold is applied as a multiplier on the *small* side —
/// `sum_small * LARGE_FILE_ABSORB_DENOMINATOR >= largest` — so the comparison
/// stays integer-exact. Do NOT rewrite it as `largest / DENOMINATOR`, which
/// truncates (and would risk a divide-by-zero if the constant ever became 0).
/// Hard-coded this iteration by design (see the size-aware-grouping spec);
/// promote it to config only if a workload needs a tunable absorb point.
const LARGE_FILE_ABSORB_DENOMINATOR: u64 = 2;

/// Split a partition's `files` into size-similar tiers so a small file is never
/// merged into a much larger, *near-target* one (which would re-read the large
/// file for little gain). The planner no longer clusters by sort-key overlap, so
/// these are all of a partition's files; the REWRITE k-way merge sorts the output
/// regardless of how they are grouped here.
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
/// at least half the largest AND the partition fits one rewrite group
/// (`total <= max_group_input_bytes`). The fit check matters: without it the
/// byte-budget bin-packer could re-split an "absorbed" group and strand a file
/// in a single-file group that is then dropped, so the absorb would silently
/// fail to merge anything.
///
/// Each returned tier preserves the input order, so a later `bin_pack_into`
/// splits along that order. A tier holding a single file is emitted as-is and
/// dropped downstream by the single-file-group filter. `ratio` is clamped to at
/// least 1 (it is also rejected at startup; see
/// `Compactor::new_with_max_iterations`). The split is iterative, so stack use is
/// constant regardless of how many size tiers the partition spans.
fn split_by_size<F: PlannableFile>(files: Vec<F>, cfg: &PlannerLimits) -> Vec<Vec<F>> {
    // A ratio below 1 is meaningless (a file can never be `>= ratio`x the
    // largest); clamp so the function stays total even if startup validation is
    // bypassed (e.g. a future direct caller).
    let ratio = cfg.max_merge_size_ratio.max(1);
    let mut remaining = files;
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
            // The small files reach half the largest and the whole set fits one
            // rewrite group, so absorbing them is worthwhile and bin-packing will
            // keep them together (nothing stranded): keep the set whole.
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
    use super::{PlannableFile, PlannerLimits, plan_rewrite_groups};

    /// A unit-test stand-in for a planner input, carrying just the two
    /// projections [`PlannableFile`] exposes. Building these avoids constructing
    /// real [`iceberg::spec::DataFile`]s in planner tests.
    struct FakeFile {
        partition: String,
        size: u64,
    }

    impl PlannableFile for FakeFile {
        fn partition_key(&self) -> &str {
            &self.partition
        }

        fn size_bytes(&self) -> u64 {
            self.size
        }
    }

    fn file(partition: &str, size: u64) -> FakeFile {
        FakeFile {
            partition: partition.to_string(),
            size,
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
        let files = vec![file("t/1", 120_000), file("t/1", 130_000), file("t/1", 110_000)];
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
            file("t/1", 200_000),
            file("t/1", 20_000),
            file("t/1", 20_000),
            file("t/1", 20_000),
            file("t/1", 20_000),
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
            file("t/1", 120_000),
            file("t/1", 20_000),
            file("t/1", 20_000),
            file("t/1", 20_000),
            file("t/1", 20_000),
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
    fn over_target_files_of_distinct_sizes_are_left_untouched() {
        // One partition's files span several distinct OVER-target size tiers. The
        // size gate splits them so a small file is never merged into a much larger
        // one, but every resulting tier is made of files that are each already at
        // or above target — merging them re-rolls N inputs into N target-sized
        // outputs (no reduction). So the file-count-reduction filter drops every
        // group and the partition is skipped. A 1 KiB target keeps every file over
        // target so the size gate (not the sub-target shortcut) governs.
        let files = vec![
            file("t/1", 120_000),
            file("t/1", 60_000),
            file("t/1", 20_000),
            file("t/1", 20_000),
            file("t/1", 4_000),
            file("t/1", 4_000),
        ];
        let outcome = plan_rewrite_groups(
            files,
            &PlannerLimits {
                target_file_size_bytes: 1_000,
                ..limits()
            },
        );
        assert!(
            outcome.groups.is_empty(),
            "merging over-target files yields no file-count reduction, so none is rewritten"
        );
        assert_eq!(outcome.partitions_skipped, 1);
        assert_eq!(outcome.partitions_compacted, 0);
    }

    #[test]
    fn size_ratio_controls_comparability() {
        // A 150 KiB over-target file alongside two 20 KiB sub-target files. The
        // ratio decides whether the small files are "comparable" enough to merge
        // INTO the large one (a reducing group), versus being peeled off and
        // merged on their own while the large file is left untouched:
        //  - at ratio 2 the 20 KiB files are too small (20 * 2 = 40 < 150), so they
        //    peel into their own tier and merge alone (the absorb mass rule also
        //    fails: 2 * 20 = 40 < 150/2), leaving the 150 KiB file out.
        //  - at ratio 8 the 20 KiB files are comparable (20 * 8 = 160 >= 150), so
        //    all three share one tier and merge into a single group that re-rolls
        //    190 KiB -> ceil(190/100) = 2 files (3 -> 2, a reduction).
        let make = || vec![file("t/1", 150_000), file("t/1", 20_000), file("t/1", 20_000)];
        let base = PlannerLimits {
            target_file_size_bytes: 100_000,
            max_group_input_bytes: 250_000,
            min_input_files: 1,
            max_skippable_tail_files: 0,
            max_merge_size_ratio: 2,
        };

        let strict = plan_rewrite_groups(make(), &base);
        assert_eq!(strict.groups.len(), 1, "at ratio 2 only the two small files merge");
        assert_eq!(strict.groups[0].len(), 2, "the 150 KiB file is left out at ratio 2");
        assert!(
            strict.groups[0].iter().all(|f| f.size_bytes() == 20_000),
            "the over-target file is not pulled into the small-file group at ratio 2"
        );

        let loose = PlannerLimits {
            max_merge_size_ratio: 8,
            ..base
        };
        let merged = plan_rewrite_groups(make(), &loose);
        assert_eq!(merged.groups.len(), 1, "at ratio 8 all three files share a tier");
        assert_eq!(
            merged.groups[0].len(),
            3,
            "at ratio 8 the small files are comparable enough to absorb the large one"
        );
    }

    #[test]
    fn size_ratio_zero_is_treated_as_one() {
        // A misconfigured ratio of 0 is clamped to 1 rather than making every file
        // (including the largest) "small" — which would peel an empty comparable
        // tier and loop forever. With the clamp, a 120 KiB over-target file and two
        // 30 KiB files behave exactly as at ratio 1: the smalls reach half the
        // largest (2 * 30 = 60 == 120/2), so the absorb fires and all three merge
        // into one reducing group (180 KiB -> ceil(180/100) = 2 files, 3 -> 2).
        let make = || vec![file("t/1", 120_000), file("t/1", 30_000), file("t/1", 30_000)];
        let zero = PlannerLimits {
            target_file_size_bytes: 100_000,
            max_group_input_bytes: 250_000,
            min_input_files: 1,
            max_skippable_tail_files: 0,
            max_merge_size_ratio: 0,
        };
        let one = PlannerLimits {
            max_merge_size_ratio: 1,
            ..zero
        };

        let zero_outcome = plan_rewrite_groups(make(), &zero);
        assert_eq!(
            zero_outcome.groups.len(),
            1,
            "ratio 0 must not hang or silently merge nothing"
        );
        assert_eq!(zero_outcome.groups[0].len(), 3, "all three files merge as at ratio 1");

        // Clamping to 1 means ratio 0 produces exactly the ratio-1 plan.
        let one_outcome = plan_rewrite_groups(make(), &one);
        assert_eq!(one_outcome.groups.len(), zero_outcome.groups.len());
        assert_eq!(one_outcome.groups[0].len(), zero_outcome.groups[0].len());
    }

    #[test]
    fn sub_target_files_merge_regardless_of_size_skew() {
        // Every file is below the 100 KiB target, so the "large" 90 KiB file is
        // cheap to re-read and the size gate must NOT protect it: the whole
        // overlapping cluster merges into one group instead of splitting by
        // relative size (which would leave the partition permanently fragmented).
        let files = vec![
            file("t/1", 90_000),
            file("t/1", 20_000),
            file("t/1", 20_000),
            file("t/1", 4_000),
            file("t/1", 4_000),
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
            file("t/1", 60_000),
            file("t/1", 200_000),
            file("t/1", 60_000),
            file("t/1", 60_000),
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
            file("t/1", 200_000),
            file("t/1", 160_000),
            file("t/1", 60_000),
            file("t/1", 60_000),
            file("t/1", 60_000),
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
            vec![file("t/1", 120_000), file("t/1", 30_000), file("t/1", 30_000)],
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
            vec![file("t/1", 120_000), file("t/1", 29_000), file("t/1", 30_000)],
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
        for _ in 0..16 {
            size = size.saturating_mul(4); // 4, 16, 64, ... strictly super-increasing
            files.push(file("t/1", size));
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
        // Eight 60 KiB files = 480 KiB > 250 KiB cap, all in one partition and all
        // sub-target, so they form a single size tier that the 250 KiB budget must
        // split into more than one group (four files each, 240 KiB <= cap). Each
        // four-file group re-rolls to ceil(240/100) = 3 target-sized outputs
        // (4 -> 3), so it reduces the file count and is kept — none is dropped as
        // non-beneficial.
        let files = vec![
            file("t/1", 60_000),
            file("t/1", 60_000),
            file("t/1", 60_000),
            file("t/1", 60_000),
            file("t/1", 60_000),
            file("t/1", 60_000),
            file("t/1", 60_000),
            file("t/1", 60_000),
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
        // Two reducing sub-target tiers that bin-pack without dropping any file.
        let total: usize = groups.iter().map(Vec::len).sum();
        assert_eq!(total, 8, "every input file must appear in exactly one group");
    }

    #[test]
    fn at_target_files_are_not_rewritten_without_reduction() {
        // The infinite-loop guard Sergey flagged. A partition with more than
        // `min_input_files` files that are each already AT target. They bin-pack
        // into a group of two, but that group re-rolls 2 x 128 KiB into 2
        // target-sized outputs (no reduction). Without the file-count-reduction
        // filter the planner would emit the group, the REWRITE would replace 2
        // files with 2 files, and the next scan would re-select the same shape —
        // forever. The filter drops every non-reducing group, so the partition is
        // skipped and the loop never starts.
        let files = vec![file("t/1", 128_000), file("t/1", 128_000), file("t/1", 128_000)];
        let outcome = plan_rewrite_groups(
            files,
            &PlannerLimits {
                target_file_size_bytes: 128_000,
                max_group_input_bytes: 256_000,
                min_input_files: 1,
                max_skippable_tail_files: 0,
                max_merge_size_ratio: 2,
            },
        );
        assert!(
            outcome.groups.is_empty(),
            "at-target files must not be rewritten: merging them does not reduce the file count"
        );
        assert_eq!(
            outcome.partitions_skipped, 1,
            "the partition is skipped, breaking the loop"
        );
        assert_eq!(outcome.partitions_compacted, 0);
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
            files.push(file("t/1", 40_000));
            files.push(file("t/2", 40_000));
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
