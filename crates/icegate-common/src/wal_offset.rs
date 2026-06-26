//! Resolution of the last committed WAL offset from Iceberg snapshot history.
//!
//! The Shifter records the last committed WAL queue offset in each
//! WAL-to-Iceberg commit's snapshot summary under
//! [`WAL_OFFSET_PROPERTY`](crate::WAL_OFFSET_PROPERTY). Two independent readers
//! need it back: the query engine (to find the WAL/Iceberg boundary) and the
//! Shifter itself (to resume shifting where it left off).
//!
//! Compaction commits `replace` snapshots that do NOT carry the offset
//! property, so the offset MUST be resolved by walking the snapshot parent
//! chain back to the most recent commit that recorded it. Reading only the
//! current snapshot loses the offset after any compaction; the Shifter would
//! then resume from offset 0 and re-commit the entire WAL, duplicating every
//! already-committed row. Centralising the walk here keeps both readers on the
//! identical, compaction-safe resolution.

use std::collections::HashSet;

use iceberg::spec::TableMetadata;

use crate::WAL_OFFSET_PROPERTY;
use crate::error::{CommonError, Result};

/// Safety cap on the snapshot parent-chain walk. Cyclic parent references are
/// caught separately by the visited-set on the first repeat (see the loop in
/// [`resolve_wal_offset`]); this bound guards only against an unexpectedly deep
/// but acyclic history.
const MAX_SNAPSHOT_WALK: u32 = 1000;

/// Resolve the last committed WAL offset by walking the snapshot parent chain,
/// gating against a silent "no offset" that would re-shift the entire WAL.
///
/// Starts at the current snapshot and follows `parent_snapshot_id` links,
/// returning the first [`WAL_OFFSET_PROPERTY`](crate::WAL_OFFSET_PROPERTY)
/// found. The approach is catalog-agnostic:
/// * compaction propagates the offset onto its `replace` snapshots (the
///   maintain crate's rewrite carries it forward via the Iceberg
///   `inherit_summary_property` action), so the CURRENT snapshot already carries
///   it under catalogs that flatten history and return only the current snapshot
///   with a severed parent chain (Nessie's Iceberg REST API);
/// * the walk additionally recovers the offset from an ANCESTOR under catalogs
///   that expose full snapshot history (and may run their own optimizers),
///   without depending on compaction having propagated it.
///
/// # Returns
///
/// * `Ok(None)` — the table has NO current snapshot: a freshly created table
///   never shifted to. Callers resume from WAL offset 0 (segment 0), the correct
///   "start from the beginning" behaviour. This is the only legitimate "no
///   offset" case and is NOT a gate failure.
/// * `Ok(Some(offset))` — the offset found on the current snapshot or an
///   ancestor.
///
/// # Errors
///
/// Returns [`CommonError::WalOffset`] — the GATE — when the table HAS at least
/// one snapshot but the offset is found NOWHERE in the reachable chain (the
/// chain ends at the root, a referenced parent is absent from the metadata, or
/// the [`MAX_SNAPSHOT_WALK`] cap is hit), or when a recorded value cannot be
/// parsed. Defaulting to 0 in any of those cases would re-commit the whole WAL
/// and duplicate every row, so the operation fails loudly instead. A missing
/// offset means a snapshot bypassed the Shifter-sets / compaction-propagates
/// invariant and must be repaired — recreating the table resets it to the fresh
/// (offset-0) state.
pub fn resolve_wal_offset(metadata: &TableMetadata) -> Result<Option<u64>> {
    // No snapshot at all: a freshly created table. Resuming from offset 0 is
    // correct (nothing committed yet), so this is NOT a gate failure.
    let Some(mut snapshot) = metadata.current_snapshot() else {
        return Ok(None);
    };
    // Snapshot ids already visited. Serves two purposes with one allocation:
    // detecting a cyclic `parent_snapshot_id` (corrupt metadata) on the first
    // repeat, and bounding the walk — `visited.len()` is the number of distinct
    // snapshots seen, so a single `len() >= MAX_SNAPSHOT_WALK` check caps an
    // unexpectedly deep history. The common Nessie case finds the offset on the
    // current snapshot and returns before the first insert, so this set never
    // allocates on that hot path.
    let mut visited: HashSet<i64> = HashSet::new();
    loop {
        if let Some(raw) = snapshot.summary().additional_properties.get(WAL_OFFSET_PROPERTY) {
            let offset = raw.parse::<u64>().map_err(|e| {
                CommonError::WalOffset(format!(
                    "malformed {WAL_OFFSET_PROPERTY} value {raw:?} in snapshot {}: {e}",
                    snapshot.snapshot_id()
                ))
            })?;
            return Ok(Some(offset));
        }
        if !visited.insert(snapshot.snapshot_id()) {
            return Err(CommonError::WalOffset(format!(
                "cyclic parent_snapshot_id chain detected at snapshot {} while resolving \
                 {WAL_OFFSET_PROPERTY}: refusing to resume the WAL from offset 0",
                snapshot.snapshot_id()
            )));
        }
        if visited.len() >= MAX_SNAPSHOT_WALK as usize {
            return Err(CommonError::WalOffset(format!(
                "walked {MAX_SNAPSHOT_WALK} snapshots from {} without finding {WAL_OFFSET_PROPERTY}: \
                 refusing to resume the WAL from offset 0 (would re-commit the entire queue)",
                snapshot.snapshot_id()
            )));
        }
        // GATE: the chain reaches the root and the table HAS snapshots, yet no
        // offset was found anywhere. A freshly created table has no snapshot and
        // returned `Ok(None)` above, so reaching here means a snapshot bypassed
        // the Shifter-sets / compaction-propagates invariant. Refuse to default
        // to 0 (it would re-shift the whole WAL); fail so the state is repaired.
        let Some(parent_id) = snapshot.parent_snapshot_id() else {
            return Err(CommonError::WalOffset(format!(
                "table has snapshot(s) but none in the chain from {} carry {WAL_OFFSET_PROPERTY}: \
                 refusing to resume the WAL from offset 0 (would re-commit the entire queue and \
                 duplicate every row). The Shifter sets this on every commit and compaction propagates \
                 it; a missing value means a snapshot bypassed that invariant. A freshly created table \
                 (no snapshot) legitimately starts at 0.",
                snapshot.snapshot_id()
            )));
        };
        let Some(parent) = metadata.snapshot_by_id(parent_id) else {
            return Err(CommonError::WalOffset(format!(
                "snapshot {} references parent {parent_id} absent from table metadata: cannot \
                 determine the WAL offset; refusing to resume from 0",
                snapshot.snapshot_id()
            )));
        };
        snapshot = parent;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{
        FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SortOrder, Summary, TableMetadata,
        TableMetadataBuilder, Type, UnboundPartitionSpec,
    };

    use super::resolve_wal_offset;
    use crate::WAL_OFFSET_PROPERTY;

    const TEST_LOCATION: &str = "s3://bucket/test/location";
    const MAIN_BRANCH: &str = "main";

    /// Minimal single-column schema; the walk never inspects column data.
    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap()
    }

    /// Build a snapshot carrying an explicit `(id, parent, sequence)` and an
    /// optional recorded WAL offset in its summary. A `None` offset models a
    /// compaction `replace` snapshot (operation aside), which omits the
    /// property entirely.
    fn snapshot(snapshot_id: i64, parent: Option<i64>, sequence_number: i64, offset: Option<u64>) -> Snapshot {
        let mut additional_properties = HashMap::new();
        if let Some(offset) = offset {
            additional_properties.insert(WAL_OFFSET_PROPERTY.to_string(), offset.to_string());
        }
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(sequence_number)
            // Monotonic, well clear of any clock-skew tolerance.
            .with_timestamp_ms(1_700_000_000_000 + sequence_number * 1000)
            .with_schema_id(0)
            .with_manifest_list(format!("/snap-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties,
            })
            .build()
    }

    /// Assemble a `TableMetadata` whose `main` branch points at the snapshots in
    /// `chain` order (last entry becomes the current snapshot).
    fn metadata_with_chain(chain: Vec<Snapshot>) -> TableMetadata {
        let mut builder = TableMetadataBuilder::new(
            schema(),
            UnboundPartitionSpec::builder().with_spec_id(0).build(),
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap();
        for snap in chain {
            builder = builder.set_branch_snapshot(snap, MAIN_BRANCH).unwrap();
        }
        builder.build().unwrap().metadata
    }

    /// THE GATE: a table whose ENTIRE reachable chain lacks the offset must fail,
    /// never silently resolve to `None`/0 (which would re-shift the entire WAL).
    /// This is the broken-invariant case (e.g. pre-fix or un-propagated
    /// `replace` snapshots) where even the walk finds nothing.
    #[test]
    fn gate_errors_when_no_snapshot_in_chain_has_offset() {
        let metadata = metadata_with_chain(vec![snapshot(1, None, 1, None), snapshot(2, Some(1), 2, None)]);

        assert!(resolve_wal_offset(&metadata).is_err());
    }

    /// A single offset-less snapshot (chain of length one, parent at root) also
    /// trips the gate — the walk reaches the root without finding the offset.
    #[test]
    fn gate_errors_for_lone_snapshot_without_offset() {
        let metadata = metadata_with_chain(vec![snapshot(1, None, 1, None)]);

        assert!(resolve_wal_offset(&metadata).is_err());
    }

    /// A cyclic `parent_snapshot_id` chain (corrupt metadata: a snapshot whose
    /// parent is itself) terminates with an error on the first repeat instead of
    /// spinning to the depth cap — never an infinite loop, never a resume-from-0.
    #[test]
    fn gate_errors_on_cyclic_parent_chain() {
        let metadata = metadata_with_chain(vec![snapshot(1, Some(1), 1, None)]);

        assert!(resolve_wal_offset(&metadata).is_err());
    }

    /// When the current snapshot carries the offset, it is returned directly.
    #[test]
    fn resolves_offset_from_current_snapshot() {
        let metadata = metadata_with_chain(vec![snapshot(1, None, 1, Some(7))]);

        assert_eq!(resolve_wal_offset(&metadata).expect("ok"), Some(7));
    }

    /// The walk is preserved for catalogs that expose full snapshot history: when
    /// the current (e.g. compaction `replace`) snapshot lacks the offset but an
    /// ANCESTOR recorded it, the walk recovers it rather than tripping the gate.
    #[test]
    fn resolves_offset_from_ancestor_when_current_snapshot_lacks_it() {
        let metadata = metadata_with_chain(vec![
            snapshot(1, None, 1, Some(42)), // shift commit (offset)
            snapshot(2, Some(1), 2, None),  // compaction replace (no offset)
        ]);

        assert_eq!(resolve_wal_offset(&metadata).expect("walks to ancestor"), Some(42));
    }

    /// A freshly created table has NO snapshot, so it resolves to `None` — the
    /// one legitimate "no offset" case. Callers start the WAL from offset 0
    /// (segment 0), and the gate does NOT fire.
    #[test]
    fn resolves_none_for_freshly_created_table() {
        let metadata = metadata_with_chain(vec![]);

        assert_eq!(resolve_wal_offset(&metadata).expect("ok"), None);
    }

    /// A recorded offset that is not a valid `u64` is a hard error, never a
    /// silent `None` (which would duplicate the WAL).
    #[test]
    fn errors_on_malformed_offset_value() {
        let mut additional_properties = HashMap::new();
        additional_properties.insert(WAL_OFFSET_PROPERTY.to_string(), "not-a-number".to_string());
        let bad = Snapshot::builder()
            .with_snapshot_id(1)
            .with_sequence_number(1)
            .with_timestamp_ms(1_700_000_001_000)
            .with_schema_id(0)
            .with_manifest_list("/snap-1.avro")
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties,
            })
            .build();
        let metadata = metadata_with_chain(vec![bad]);

        assert!(resolve_wal_offset(&metadata).is_err());
    }

    /// A snapshot whose `parent_snapshot_id` references an id ABSENT from the
    /// metadata (corrupt history, or an ancestor expired out from under the
    /// chain) cannot resolve the offset and must error on the `snapshot_by_id`
    /// miss — never resume from 0.
    #[test]
    fn gate_errors_when_parent_snapshot_absent_from_metadata() {
        // Single snapshot pointing at a parent id that was never added.
        let metadata = metadata_with_chain(vec![snapshot(1, Some(999), 1, None)]);

        assert!(resolve_wal_offset(&metadata).is_err());
    }

    /// A linear chain of offset-less snapshots longer than `MAX_SNAPSHOT_WALK`
    /// trips the depth cap (the `visited.len()` bound), refusing to resume from 0
    /// rather than walking unbounded.
    #[test]
    fn gate_errors_when_walk_exceeds_depth_cap() {
        let chain: Vec<Snapshot> = (1..=(super::MAX_SNAPSHOT_WALK + 1))
            .map(|i| {
                let id = i64::from(i);
                let parent = (i > 1).then(|| id - 1);
                snapshot(id, parent, id, None)
            })
            .collect();
        let metadata = metadata_with_chain(chain);

        assert!(resolve_wal_offset(&metadata).is_err());
    }
}
