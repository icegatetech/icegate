//! Compaction planner: group, skip, cluster, and bin-pack data files.
//!
//! Given a table's data files (each carrying its `(tenant, day)` partition key,
//! byte size, and decoded sort-key envelope), [`plan_rewrite_groups`] produces
//! *rewrite groups*. Each group is a set of input files in one partition that a
//! later PLAN executor (Task 4.4) rewrites into approximately one output file.
//!
//! The algorithm is five stages:
//! 1. **Group** files by partition key, so a rewrite never spans partitions.
//! 2. **Skip healthy partitions.** A partition with few files and no
//!    significant tail of sub-target files is left untouched.
//! 3. **Cluster** the remaining files by transitive sort-key overlap
//!    ([`swept_line_cluster`](icegate_common::merge::cluster::swept_line_cluster)),
//!    so each cluster's output occupies a disjoint sort-key range. An
//!    over-target file overlapping the tail is intentionally pulled in so the
//!    rewrite produces non-overlapping outputs.
//! 4. **Bin-pack** each cluster by input-byte budget into one or more groups, so
//!    no single rewrite reads more than the configured cap.
//! 5. **Drop non-beneficial groups.** A group of a single file rewrites it
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
        // Cluster transitively-overlapping files so each cluster's rewrite
        // output occupies a disjoint sort-key range, then split each cluster by
        // the input-byte budget. `swept_line_cluster` stable-sorts by `min_key`,
        // so both the clustering and the resulting groups are deterministic.
        let mut partition_groups: Vec<Vec<F>> = Vec::new();
        for cluster in swept_line_cluster(partition_files, PlannableFile::boundary_range) {
            bin_pack_into(cluster, cfg.max_group_input_bytes, &mut partition_groups);
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
    /// `min_input_files = 4`, and a sub-target tail tolerance of 2.
    fn limits() -> PlannerLimits {
        PlannerLimits {
            target_file_size_bytes: 100_000,
            max_group_input_bytes: 250_000,
            min_input_files: 4,
            max_skippable_tail_files: 2,
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
    fn pulls_overlapping_oversized_neighbor_into_group() {
        // A partition with a small sub-target file whose range overlaps an
        // over-target file. The over-target file (180 KiB) is above the 100 KiB
        // target yet below the 250 KiB group budget, so once clustered with the
        // 10 KiB small file the pair (190 KiB) still fits in a single rewrite
        // group. The partition is made unhealthy by three extra disjoint small
        // files pushing file_count past min_input_files.
        let files = vec![
            // small + over-target overlap on [10,30] / [25,40].
            file("t/1", 10_000, 10, 30),
            file("t/1", 180_000, 25, 40),
            // extra small files to push file_count past min_input_files so the
            // partition is not skipped.
            file("t/1", 10_000, 100, 110),
            file("t/1", 10_000, 200, 210),
            file("t/1", 10_000, 300, 310),
        ];
        let groups = plan_rewrite_groups(files, &limits()).groups;

        // The cluster over [10,40] must yield a single group holding the small
        // and the over-target file together.
        let overlap_group = groups
            .iter()
            .find(|group| group.iter().any(|f| f.size_bytes() == 180_000))
            .expect("a group must contain the over-target file");
        assert!(
            overlap_group.iter().any(|f| f.size_bytes() == 10_000),
            "the over-target file's group must also contain the overlapping small file"
        );
        assert_eq!(
            overlap_group.len(),
            2,
            "exactly the overlapping small + over-target pair should cluster together"
        );
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
