//! Manifest-compaction planner: pick one repack group from a snapshot's
//! manifest list.
//!
//! Pure selection over [`ManifestFile`] entries — no I/O, no transaction. The
//! executor calls [`plan_manifest_compaction`] BEFORE opening a transaction, so a
//! run with nothing worth repacking commits nothing (an empty `replace` snapshot
//! would only grow `metadata.json`).
//!
//! Candidate selection, the packing estimate, and the rewrite action's actual
//! packing all work in MANIFEST bytes (a manifest's byte size tracks its entry
//! count, not the referenced data-file sizes), so the estimate here matches the
//! byte-level count the action produces.
//!
//! This gate is necessary but not sufficient: a manifest-list entry carries no
//! schema id, so this planner groups only by partition spec, while the rewrite
//! action additionally splits each group by schema id and re-gates every
//! sub-group. After a schema evolution the selected manifests can therefore be
//! singletons per schema and reduce nothing, in which case the action returns
//! `ErrorKind::NoReduction`; the executor maps that to a normal skip.

use std::collections::HashMap;

use iceberg::spec::{ManifestContentType, ManifestFile};

/// The plan for one manifest-compaction run.
///
/// Borrows the selected manifests from the entry slice passed to
/// [`plan_manifest_compaction`], so the plan never copies a [`ManifestFile`].
#[derive(Debug, PartialEq, Eq)]
pub enum ManifestCompactPlan<'entries> {
    /// Repack these manifests (at least two, all sharing `partition_spec_id`, and
    /// estimated to pack into strictly fewer output manifests).
    Repack {
        /// Partition spec every selected manifest shares.
        partition_spec_id: i32,
        /// The manifests to repack, smallest first.
        manifests: Vec<&'entries ManifestFile>,
    },
    /// Nothing worth repacking this run, with the counts that ruled it out.
    Skip(ManifestSurvey),
}

/// What the planner saw in a snapshot's manifest list when it found nothing to
/// repack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestSurvey {
    /// Manifest-list entries in the current snapshot, DATA and DELETE alike.
    pub total_manifests: usize,
    /// Of those, DATA manifests — the only repack candidates by content type.
    pub data_manifests: usize,
    /// DATA manifests also below the candidate size threshold.
    pub candidate_manifests: usize,
    /// Candidates in the largest single partition-spec group, before the
    /// per-commit cap. Below two, no group could ever reduce.
    pub largest_group_manifests: usize,
    /// Summed manifest bytes of that largest group. Against the packing target
    /// this shows whether the group could have reduced at all.
    pub largest_group_bytes: u64,
}

/// Plan one manifest-compaction run from the current snapshot's manifest-list
/// entries.
#[must_use]
pub fn plan_manifest_compaction(
    entries: &[ManifestFile],
    target_manifest_size_bytes: u64,
    candidate_size_ratio: f64,
    max_manifests_per_commit: usize,
) -> ManifestCompactPlan<'_> {
    // Group the sub-target DATA manifests by partition spec. Delete manifests and
    // already-large manifests are never candidates.
    let mut by_spec: HashMap<i32, Vec<&ManifestFile>> = HashMap::new();
    let mut data_manifests = 0;
    for entry in entries {
        if entry.content != ManifestContentType::Data {
            continue;
        }
        data_manifests += 1;
        if is_candidate_size(entry.manifest_length, target_manifest_size_bytes, candidate_size_ratio) {
            by_spec.entry(entry.partition_spec_id).or_default().push(entry);
        }
    }

    // Survey the groups before they are consumed below, so a skip can report why.
    let survey = survey_candidates(entries.len(), data_manifests, &by_spec);

    // Of the spec groups that would strictly reduce, keep the one with the most
    // candidates; on a tie prefer the lowest spec id so selection is
    // deterministic regardless of hash-map iteration order. `candidate_count` is
    // the group's full size (before the per-commit cap), preserving the original
    // "largest group first" ranking.
    let best = by_spec
        .into_iter()
        .filter_map(|(spec_id, group)| {
            let candidate_count = group.len();
            select_repack_group(group, target_manifest_size_bytes, max_manifests_per_commit)
                .map(|manifests| (spec_id, candidate_count, manifests))
        })
        .max_by(|(left_id, left_count, _), (right_id, right_count, _)| {
            left_count.cmp(right_count).then(right_id.cmp(left_id))
        });

    match best {
        Some((partition_spec_id, _, manifests)) => ManifestCompactPlan::Repack {
            partition_spec_id,
            manifests,
        },
        None => ManifestCompactPlan::Skip(survey),
    }
}

/// Summarize the candidate groups for a skip's log line.
fn survey_candidates(
    total_manifests: usize,
    data_manifests: usize,
    manifest_by_spec: &HashMap<i32, Vec<&ManifestFile>>,
) -> ManifestSurvey {
    let largest = manifest_by_spec.values().max_by_key(|group| group.len());
    ManifestSurvey {
        total_manifests,
        data_manifests,
        candidate_manifests: manifest_by_spec.values().map(Vec::len).sum(),
        largest_group_manifests: largest.map_or(0, Vec::len),
        largest_group_bytes: largest.map_or(0, |group| {
            group
                .iter()
                .map(|manifest| u64::try_from(manifest.manifest_length).unwrap_or(0))
                .sum()
        }),
    }
}

fn select_repack_group(
    mut group: Vec<&ManifestFile>,
    target_manifest_size_bytes: u64,
    max_manifests_per_commit: usize,
) -> Option<Vec<&ManifestFile>> {
    // Keep the smallest `max_manifests_per_commit` manifests, bounding one
    // commit's work.
    group.sort_by(|left, right| {
        left.manifest_length
            .cmp(&right.manifest_length)
            .then_with(|| left.manifest_path.cmp(&right.manifest_path))
    });
    group.truncate(max_manifests_per_commit);

    // A group of fewer than two manifests can never repack into fewer.
    if group.len() < 2 {
        return None;
    }

    // Reject when the group would not strictly reduce the manifest count.
    if estimate_output_manifests(&group, target_manifest_size_bytes) >= group.len() as u64 {
        return None;
    }

    Some(group)
}

/// Whether a manifest of `manifest_length` bytes is small enough to repack.
///
/// A manifest at or above `candidate_size_ratio * target_manifest_size_bytes`
/// bytes is already large enough and is left alone.
#[allow(clippy::cast_precision_loss)]
fn is_candidate_size(manifest_length: i64, target_manifest_size_bytes: u64, candidate_size_ratio: f64) -> bool {
    let threshold = candidate_size_ratio * target_manifest_size_bytes as f64;
    (manifest_length.max(0) as f64) < threshold
}

/// Estimate the number of output manifests packing `selected` at
/// `target_manifest_size_bytes` produces: `ceil(sum(manifest_length) / target)`.
///
/// Matches the output count the rewrite action produces (both work in manifest
/// bytes), so it is a sound reduction gate. `target_manifest_size_bytes` is
/// guaranteed positive by config validation.
#[must_use]
pub fn estimate_output_manifests(selected: &[&ManifestFile], target_manifest_size_bytes: u64) -> u64 {
    let total_bytes: u64 = selected
        .iter()
        .map(|manifest| u64::try_from(manifest.manifest_length).unwrap_or(0))
        .sum();
    total_bytes.div_ceil(target_manifest_size_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 8 `MiB` target with a 0.75 ratio, matching the config defaults: a manifest
    /// is a candidate below 6 `MiB`.
    const TARGET: u64 = 8 * 1024 * 1024;
    const RATIO: f64 = 0.75;
    const MIB: i64 = 1024 * 1024;

    /// Build a `ManifestFile` fixture carrying only the four fields the planner
    /// reads; every other field is a benign zero/`None` the planner ignores.
    fn entry(path: &str, length_bytes: i64, content: ManifestContentType, spec_id: i32) -> ManifestFile {
        ManifestFile {
            manifest_path: path.to_string(),
            manifest_length: length_bytes,
            partition_spec_id: spec_id,
            content,
            sequence_number: 0,
            min_sequence_number: 0,
            added_snapshot_id: 0,
            added_files_count: None,
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    fn data(path: &str, length_bytes: i64, spec_id: i32) -> ManifestFile {
        entry(path, length_bytes, ManifestContentType::Data, spec_id)
    }

    #[test]
    fn skips_when_fewer_than_two_candidates() {
        // One small DATA manifest plus two small DELETE manifests: only DATA
        // manifests are candidates, so the largest group has a single manifest.
        let entries = vec![
            data("a", MIB, 0),
            entry("d1", MIB, ManifestContentType::Deletes, 0),
            entry("d2", MIB, ManifestContentType::Deletes, 0),
        ];
        assert_eq!(
            plan_manifest_compaction(&entries, TARGET, RATIO, 64),
            ManifestCompactPlan::Skip(ManifestSurvey {
                total_manifests: 3,
                data_manifests: 1,
                candidate_manifests: 1,
                largest_group_manifests: 1,
                largest_group_bytes: 1024 * 1024,
            })
        );
    }

    #[test]
    fn skips_large_manifests() {
        // Both manifests are at or above the 6 MiB candidate threshold, so neither
        // is selected and there is nothing to repack. The survey separates this
        // from the too-few-manifests case: two DATA manifests, zero candidates.
        let entries = vec![data("a", 6 * MIB, 0), data("b", 7 * MIB, 0)];
        assert_eq!(
            plan_manifest_compaction(&entries, TARGET, RATIO, 64),
            ManifestCompactPlan::Skip(ManifestSurvey {
                total_manifests: 2,
                data_manifests: 2,
                candidate_manifests: 0,
                largest_group_manifests: 0,
                largest_group_bytes: 0,
            })
        );
    }

    #[test]
    fn skips_when_no_count_reduction() {
        // Two 5 MiB candidates (below the 6 MiB threshold) sum to 10 MiB, which
        // packs into ceil(10/8) = 2 manifests — no reduction, so skip. The survey
        // reports a full group, so the summed bytes against the target are what
        // explain the skip.
        let entries = vec![data("a", 5 * MIB, 0), data("b", 5 * MIB, 0)];
        assert_eq!(
            plan_manifest_compaction(&entries, TARGET, RATIO, 64),
            ManifestCompactPlan::Skip(ManifestSurvey {
                total_manifests: 2,
                data_manifests: 2,
                candidate_manifests: 2,
                largest_group_manifests: 2,
                largest_group_bytes: 10 * 1024 * 1024,
            })
        );
    }

    #[test]
    fn repacks_group_with_most_candidates() {
        // Spec 0 has two candidates, spec 1 has three: the larger group (spec 1)
        // is chosen, and its 3 MiB total packs into a single manifest.
        let entries = vec![
            data("s0a", MIB, 0),
            data("s0b", MIB, 0),
            data("s1a", MIB, 1),
            data("s1b", MIB, 1),
            data("s1c", MIB, 1),
        ];
        match plan_manifest_compaction(&entries, TARGET, RATIO, 64) {
            ManifestCompactPlan::Repack {
                partition_spec_id,
                manifests,
            } => {
                assert_eq!(partition_spec_id, 1);
                assert_eq!(manifests.len(), 3);
                assert!(manifests.iter().all(|manifest| manifest.partition_spec_id == 1));
            }
            ManifestCompactPlan::Skip(_) => panic!("expected a repack of the larger group"),
        }
    }

    #[test]
    fn repacks_reducible_group_when_largest_group_is_irreducible() {
        // Spec 0 has the most candidates (three near-target manifests), but they
        // already pack into three outputs (3 * ~6 MiB -> ceil(~17.99/8) = 3), so
        // that group is irreducible. Spec 1's two small manifests pack into one.
        // The larger irreducible group must not starve the smaller reducible one.
        let entries = vec![
            data("s0a", 6 * MIB - 1, 0),
            data("s0b", 6 * MIB - 1, 0),
            data("s0c", 6 * MIB - 1, 0),
            data("s1a", MIB, 1),
            data("s1b", MIB, 1),
        ];
        match plan_manifest_compaction(&entries, TARGET, RATIO, 64) {
            ManifestCompactPlan::Repack {
                partition_spec_id,
                manifests,
            } => {
                assert_eq!(partition_spec_id, 1);
                assert_eq!(manifests.len(), 2);
                assert!(manifests.iter().all(|manifest| manifest.partition_spec_id == 1));
            }
            ManifestCompactPlan::Skip(_) => panic!("expected the smaller reducible group to be repacked"),
        }
    }

    #[test]
    fn caps_to_max_manifests_per_commit_taking_smallest() {
        // Five candidates but a cap of three: the three SMALLEST are taken.
        let entries = vec![
            data("big1", 3 * MIB, 0),
            data("big2", 3 * MIB, 0),
            data("small1", MIB, 0),
            data("small2", MIB, 0),
            data("small3", MIB, 0),
        ];
        match plan_manifest_compaction(&entries, TARGET, RATIO, 3) {
            ManifestCompactPlan::Repack { manifests, .. } => {
                assert_eq!(manifests.len(), 3);
                let paths: Vec<&str> = manifests.iter().map(|manifest| manifest.manifest_path.as_str()).collect();
                assert_eq!(paths, vec!["small1", "small2", "small3"]);
            }
            ManifestCompactPlan::Skip(_) => panic!("expected a capped repack"),
        }
    }

    #[test]
    fn estimate_matches_ceiling_of_total_over_target() {
        let owned = [data("a", 3 * MIB, 0), data("b", 3 * MIB, 0), data("c", 3 * MIB, 0)];
        let selected: Vec<&ManifestFile> = owned.iter().collect();
        // 9 MiB over an 8 MiB target -> ceil(9/8) = 2.
        assert_eq!(estimate_output_manifests(&selected, TARGET), 2);
    }

    #[test]
    fn is_candidate_size_uses_ratio_of_target() {
        // Threshold is 0.75 * 8 MiB = 6 MiB.
        assert!(is_candidate_size(6 * MIB - 1, TARGET, RATIO));
        assert!(!is_candidate_size(6 * MIB, TARGET, RATIO));
        // A negative length (never expected) is treated as zero, hence a candidate.
        assert!(is_candidate_size(-1, TARGET, RATIO));
    }
}
