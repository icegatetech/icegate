//! Content invariants a data-file rewrite must preserve.
//!
//! A rewrite is only allowed to change how rows are PACKED into files, never
//! which rows exist. [`verify_content_invariants`] checks that BEFORE the
//! replace transaction is committed, so a violation rejects the rewrite while
//! the freshly written outputs are still unreferenced and safe to delete.
//!
//! Two invariants:
//!
//! 1. **Row count is preserved** — the outputs' record counts sum to the
//!    inputs'.
//! 2. **Sort-key envelope is preserved** — the column-wise union of the outputs'
//!    Iceberg manifest bounds equals that of the inputs'.
//!
//! Invariant 2 is the subtle one, and everything below the entry point exists to
//! make it sound rather than merely plausible. Iceberg manifest bounds are
//! COLUMN-WISE, OPTIONAL, and lossy, which forces two design choices:
//!
//! * The fold is **per column**, not per whole key (`union_envelope`). A file's
//!   `min_key` pairs one column's literal minimum with another column's
//!   unrelated value, so unioning whole keys does not reconstruct the merged
//!   file's column-wise bounds and would report false violations.
//! * An **absent bound annihilates** the fold (`keep_literal_extreme`) and is
//!   **skipped** by the comparison (`first_envelope_conflict`). Iceberg omits a
//!   column's bound both for an all-null column and when parquet marks the stat
//!   non-exact (routine for fixed/binary columns like `trace_id`), so absence
//!   means "nothing recorded", never "no such value".

use iceberg::spec::DataFile;
use iceberg::table::Table;
use icegate_common::manifest_scan::decode_data_file_envelope;
use icegate_common::merge::sort_key::{
    RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, SortColumnsDescriptor,
};

use crate::error::{MaintainError, Result};

/// Verify the two REWRITE content invariants against the inputs.
///
/// 1. **Row count is preserved:** the sum of the output files' record counts
///    equals the sum of the input files' record counts.
/// 2. **Sort-key envelope is preserved:** the output's overall inclusive
///    sort-order envelope equals the union of the inputs' envelopes — i.e. the
///    minimum `min_key` and maximum `max_key` (under the direction-aware
///    [`RowGroupBoundaryKey::compare`]) are identical across the input set and
///    the output set.
///
/// The output bounds are decoded from each written [`DataFile`]'s
/// `lower_bounds`/`upper_bounds` through
/// [`decode_data_file_envelope`], the SAME direction-aware code path the
/// manifest scan uses for the inputs, so the two sides are compared on equal
/// footing. A merge that dropped, duplicated, or reordered rows such that an
/// extreme sort key changed fails invariant 2; a merge that changed the row
/// total fails invariant 1.
///
/// # Errors
///
/// Returns [`MaintainError::InvariantViolation`] if either invariant is violated
/// (a data-corruption signal kept distinct from configuration errors), or a
/// [`MaintainError::Config`] enumeration/decode error if an output file's bounds
/// cannot be read.
pub fn verify_content_invariants(
    table: &Table,
    descriptor: &SortColumnsDescriptor,
    removed: &[DataFile],
    added: &[DataFile],
) -> Result<()> {
    // Invariant 1: total row count is preserved.
    let removed_rows: u64 = removed.iter().map(DataFile::record_count).sum();
    let added_rows: u64 = added.iter().map(DataFile::record_count).sum();
    if removed_rows != added_rows {
        return Err(MaintainError::InvariantViolation(format!(
            "rewrite row-count invariant violated: inputs hold {removed_rows} rows but outputs hold {added_rows}"
        )));
    }

    // Invariant 2: the per-column sort-key bounds are preserved. A lossless
    // merge keeps, for every sort column, the literal minimum and literal
    // maximum value across the whole input set — so the column-wise union of the
    // outputs' manifest bounds must equal that of the inputs'. Comparing the
    // *whole* per-file key min/max would be WRONG: Iceberg manifest bounds are
    // column-wise, so a file's `min_key` can pair one column's literal min with
    // another column's unrelated value, and unioning such synthetic keys does
    // not reconstruct the merged file's column-wise bounds. Folding per column
    // first is what makes this a sound, false-positive-free invariant.
    let input_ranges = decode_envelopes(table, descriptor, removed)?;
    let output_ranges = decode_envelopes(table, descriptor, added)?;

    let Some(inputs_union) = union_envelope(&input_ranges).map_err(MaintainError::from)? else {
        // Empty input set: with an equal row count the output is empty too, so
        // there is nothing to compare.
        return Ok(());
    };
    let Some(outputs_union) = union_envelope(&output_ranges).map_err(MaintainError::from)? else {
        // Non-empty inputs but empty outputs with an equal row count is
        // impossible (it would imply 0 rows on both sides, handled above), so an
        // empty output here is a genuine invariant violation.
        return Err(MaintainError::InvariantViolation(
            "rewrite envelope invariant violated: inputs are non-empty but outputs produced no files".to_string(),
        ));
    };

    // Compare the column-wise union envelopes column by column, TOLERATING a
    // bound that is absent on either side. Iceberg manifest lower/upper bounds
    // are a lossy, OPTIONAL summary: parquet can mark a row-group statistic
    // non-exact (it does this for fixed/byte columns under truncation), and
    // iceberg-rust then OMITS that column's bound from the file's manifest
    // entirely — `MinMaxColAggregator` only records a bound when the parquet
    // stat reports `*_is_exact()`. The very same column can therefore be ABSENT
    // in one file's recorded bounds yet PRESENT in another's for IDENTICAL
    // underlying data (e.g. inputs written by ingest / an older binary, outputs
    // by this compactor). An absent bound means "this side recorded no bound",
    // NOT "the value is null/missing", so it cannot witness an envelope change:
    // a column only proves a violation when BOTH sides recorded a bound and the
    // recorded values differ. Comparing the whole key with `compare_checked`
    // (which treats absent-vs-present as a hard inequality) is what tripped the
    // false positive this guards against. The error still pinpoints the first
    // genuinely conflicting column and its values for fast diagnosis.
    if let Some(idx) = first_envelope_conflict(&inputs_union.min_key, &outputs_union.min_key)? {
        return Err(MaintainError::InvariantViolation(format!(
            "rewrite envelope invariant violated: output minimum sort key differs from input minimum \
             ({} input file(s) -> {} output file(s); {})",
            removed.len(),
            added.len(),
            describe_conflict(&inputs_union.names, idx, &inputs_union.min_key, &outputs_union.min_key),
        )));
    }
    if let Some(idx) = first_envelope_conflict(&inputs_union.max_key, &outputs_union.max_key)? {
        return Err(MaintainError::InvariantViolation(format!(
            "rewrite envelope invariant violated: output maximum sort key differs from input maximum \
             ({} input file(s) -> {} output file(s); {})",
            removed.len(),
            added.len(),
            describe_conflict(&inputs_union.names, idx, &inputs_union.max_key, &outputs_union.max_key),
        )));
    }

    Ok(())
}

/// Find the first sort column that genuinely conflicts between the input and
/// output union boundary keys, tolerating bounds that are ABSENT on either side.
///
/// A column conflicts only when BOTH `input_key` and `output_key` recorded a
/// bound (`value: Some`) for it and the two recorded values differ. A column
/// absent on either side (`value: None`) carries no constraint — Iceberg
/// manifest bounds are optional and lossy (see the module docs) — so it is
/// skipped rather than treated as a mismatch.
///
/// # Errors
///
/// Returns [`MaintainError::Config`] (via [`RowGroupBoundaryKey::validate_compatible_structure`])
/// if the two keys are structurally incompatible (differing arity, direction,
/// null order, value type, or fixed-bytes width).
fn first_envelope_conflict(input_key: &RowGroupBoundaryKey, output_key: &RowGroupBoundaryKey) -> Result<Option<usize>> {
    input_key
        .validate_compatible_structure(output_key)
        .map_err(MaintainError::from)?;
    Ok(input_key
        .components()
        .iter()
        .zip(output_key.components())
        .position(|(input, output)| input.value.is_some() && output.value.is_some() && input.value != output.value))
}

/// Render the conflicting column at `idx` (name + input/output values) for the
/// envelope-invariant error message.
fn describe_conflict(
    names: &[String],
    idx: usize,
    input_key: &RowGroupBoundaryKey,
    output_key: &RowGroupBoundaryKey,
) -> String {
    let column = names.get(idx).map_or("<unknown>", String::as_str);
    let input = input_key.components().get(idx).map(|component| &component.value);
    let output = output_key.components().get(idx).map(|component| &component.value);
    format!("column '{column}' (index {idx}): input={input:?}, output={output:?}")
}

/// Decode every data file's sort-order envelope through the table's schema.
///
/// A thin wrapper over [`decode_data_file_envelope`] that collects the per-file
/// envelopes and bridges the common-crate error into [`MaintainError`].
fn decode_envelopes(
    table: &Table,
    descriptor: &SortColumnsDescriptor,
    files: &[DataFile],
) -> Result<Vec<RowGroupBoundaryRange>> {
    files
        .iter()
        .map(|file| decode_data_file_envelope(table, descriptor, file).map_err(MaintainError::from))
        .collect()
}

/// Fold a set of per-file envelopes into ONE column-wise union envelope.
///
/// For every sort column the union takes the literal minimum across all files
/// (the value that sorts first in raw, ascending-ignoring-direction order) and
/// the literal maximum (the value that sorts last), then re-assembles the
/// direction-aware `min_key`/`max_key` from those per-column extremes. This is
/// the column-wise union of the files' Iceberg manifest bounds — the quantity a
/// lossless merge provably preserves.
///
/// Iceberg manifest bounds are OPTIONAL and lossy: a file omits a column's bound
/// both when the column is entirely null and when parquet marks the stat
/// non-exact (common for fixed/binary columns like `trace_id`). An absent bound
/// thus carries no constraint on the column's literal extreme, so it ANNIHILATES
/// the per-column fold (see [`keep_literal_extreme`]): a column's union extreme
/// resolves to `None` whenever ANY file omitted that bound. A `None` union
/// extreme is skipped by [`first_envelope_conflict`], which is precisely what
/// keeps the equality invariant from firing when the inputs simply did not record
/// a usable bound for the value the lossless merge later writes out exactly.
///
/// Returns `None` for an empty input (no files ⇒ no envelope). All files must
/// share the descriptor's column structure (guaranteed: every range is decoded
/// from the same descriptor).
///
/// # Errors
///
/// Returns an error if two files' envelopes have incompatible component
/// structure (different arity, direction, null order, or value type), which a
/// single descriptor should never produce.
fn union_envelope(ranges: &[RowGroupBoundaryRange]) -> icegate_common::error::Result<Option<RowGroupBoundaryRange>> {
    let Some(first) = ranges.first() else {
        return Ok(None);
    };

    let column_count = first.min_key.components().len();
    // Per column, accumulate the literal-min and literal-max component. Seed
    // from the first file's direction-aware key by recovering each column's
    // literal extremes.
    let mut literal_min: Vec<RowGroupBoundaryComponent> = Vec::with_capacity(column_count);
    let mut literal_max: Vec<RowGroupBoundaryComponent> = Vec::with_capacity(column_count);
    for column_idx in 0..column_count {
        let (min_component, max_component) = column_literal_bounds(first, column_idx)?;
        literal_min.push(min_component);
        literal_max.push(max_component);
    }

    for range in &ranges[1..] {
        range.min_key.validate_compatible_structure(&first.min_key)?;
        range.max_key.validate_compatible_structure(&first.max_key)?;
        for column_idx in 0..column_count {
            let (candidate_min, candidate_max) = column_literal_bounds(range, column_idx)?;
            literal_min[column_idx] = keep_literal_extreme(&literal_min[column_idx], &candidate_min, Extreme::Min)?;
            literal_max[column_idx] = keep_literal_extreme(&literal_max[column_idx], &candidate_max, Extreme::Max)?;
        }
    }

    // Re-assemble the direction-aware keys from the per-column literal extremes:
    // ascending columns put the literal minimum in `min_key`, descending columns
    // put the literal maximum there (mirroring the manifest-scan decode).
    let mut min_components = Vec::with_capacity(column_count);
    let mut max_components = Vec::with_capacity(column_count);
    for column_idx in 0..column_count {
        let min_lit = &literal_min[column_idx];
        let max_lit = &literal_max[column_idx];
        if min_lit.descending {
            min_components.push(max_lit.clone());
            max_components.push(min_lit.clone());
        } else {
            min_components.push(min_lit.clone());
            max_components.push(max_lit.clone());
        }
    }

    Ok(Some(RowGroupBoundaryRange {
        names: first.names.clone(),
        min_key: RowGroupBoundaryKey::new(min_components),
        max_key: RowGroupBoundaryKey::new(max_components),
    }))
}

/// Which literal extreme is being accumulated.
#[derive(Clone, Copy)]
enum Extreme {
    /// The literal minimum (sorts first in raw ascending order).
    Min,
    /// The literal maximum (sorts last in raw ascending order).
    Max,
}

/// Recover one sort column's literal `(min, max)` components from a file's
/// direction-aware envelope, preserving each component's ORIGINAL direction and
/// null order.
///
/// For an ascending column the literal minimum is in `min_key[column_idx]` and
/// the literal maximum in `max_key[column_idx]`; for a descending column they
/// are swapped (the manifest-scan decode put the literal maximum in `min_key`).
/// The components are returned verbatim — direction-aware re-assembly and raw
/// literal comparison both rely on the unmodified flags.
fn column_literal_bounds(
    range: &RowGroupBoundaryRange,
    column_idx: usize,
) -> icegate_common::error::Result<(RowGroupBoundaryComponent, RowGroupBoundaryComponent)> {
    let min_key_component = range.min_key.components().get(column_idx).ok_or_else(|| {
        icegate_common::error::CommonError::Write("envelope min_key column index out of bounds".to_string())
    })?;
    let max_key_component = range.max_key.components().get(column_idx).ok_or_else(|| {
        icegate_common::error::CommonError::Write("envelope max_key column index out of bounds".to_string())
    })?;

    let (literal_min, literal_max) = if min_key_component.descending {
        (max_key_component, min_key_component)
    } else {
        (min_key_component, max_key_component)
    };
    Ok((literal_min.clone(), literal_max.clone()))
}

/// Project a component to raw-literal comparison form: same value and null
/// order, but `descending: false` so [`RowGroupBoundaryKey::compare`] orders it
/// by literal value regardless of the column's sort direction.
fn as_literal_component(component: &RowGroupBoundaryComponent) -> RowGroupBoundaryComponent {
    RowGroupBoundaryComponent {
        value: component.value.clone(),
        descending: false,
        nulls_first: component.nulls_first,
    }
}

/// Keep whichever of `current`/`candidate` is the requested literal extreme,
/// comparing them in raw literal order (direction-ignoring) while returning the
/// chosen component VERBATIM so its original direction/null flags survive into
/// the re-assembled union key.
fn keep_literal_extreme(
    current: &RowGroupBoundaryComponent,
    candidate: &RowGroupBoundaryComponent,
    extreme: Extreme,
) -> icegate_common::error::Result<RowGroupBoundaryComponent> {
    // An ABSENT bound (`value: None`) means the file recorded NO bound for this
    // column: Iceberg omits a column from the manifest `lower_bounds`/`upper_bounds`
    // maps both for an all-null column AND when parquet marks the stat non-exact
    // (which it routinely does for fixed/binary columns such as `trace_id`). An
    // absent bound therefore carries no information about the column's literal
    // extreme — the file's true extreme could exceed EVERY present bound. For the
    // equality-based envelope invariant the union extreme must consequently become
    // UNCONSTRAINED (absent) whenever ANY contributing file omitted the bound:
    // absence ANNIHILATES the fold, it is not the identity. Keeping the present
    // sibling instead understates the union extreme — when one input file omits
    // its `trace_id` upper bound but holds the group's true maximum, the input
    // union max collapses to a smaller present bound while the losslessly-merged
    // output records the true (exact) maximum, tripping a FALSE envelope-invariant
    // violation that wedges every Fixed(16)-sorted table's compaction. A union
    // extreme that is absent is simply skipped by `first_envelope_conflict`
    // (which only flags a column when BOTH sides recorded a bound), so this
    // relaxes the check exactly where the inputs carry no usable bound; the
    // row-count invariant still independently guards against a lossy merge.
    match (&current.value, &candidate.value) {
        (None, _) => return Ok(current.clone()),
        (Some(_), None) => return Ok(candidate.clone()),
        (Some(_), Some(_)) => {}
    }

    // Both bounds are present: compare by literal value only — wrap each in a
    // one-element key normalized to `descending: false` (the structure-checked
    // comparison also validates the value types match before ordering them).
    let current_key = RowGroupBoundaryKey::new(vec![as_literal_component(current)]);
    let candidate_key = RowGroupBoundaryKey::new(vec![as_literal_component(candidate)]);
    let ordering = current_key.compare_checked(&candidate_key)?;
    let keep_candidate = match extreme {
        Extreme::Min => ordering == std::cmp::Ordering::Greater,
        Extreme::Max => ordering == std::cmp::Ordering::Less,
    };
    Ok(if keep_candidate {
        candidate.clone()
    } else {
        current.clone()
    })
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::sync::Arc;

    use icegate_common::merge::sort_key::{
        RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, RowGroupBoundaryValue,
    };

    use super::{first_envelope_conflict, union_envelope};

    /// Build a logs-shaped envelope for one data file from its per-column literal
    /// bounds: `service_name` (ASC, nulls-first) and `timestamp` (DESC,
    /// nulls-first), mirroring the logs sort order. The arguments are the file's
    /// LITERAL min/max per column; this helper assembles the direction-aware
    /// `min_key`/`max_key` exactly like the manifest-scan decode.
    fn logs_envelope(
        service_min: &str,
        service_max: &str,
        ts_literal_min: i64,
        ts_literal_max: i64,
    ) -> RowGroupBoundaryRange {
        logs_envelope_opt(Some(service_min), Some(service_max), ts_literal_min, ts_literal_max)
    }

    /// Build a logs-shaped envelope whose `service_name` bound may be ABSENT
    /// (`None`), modelling an optional sort column that is entirely null in a
    /// data file: Iceberg omits an all-null column from the manifest bounds
    /// map, so the manifest-scan decode yields a `None`-valued component. The
    /// `timestamp` column is always present (it is a required column). The
    /// direction-aware assembly mirrors [`logs_envelope`].
    fn logs_envelope_opt(
        service_min: Option<&str>,
        service_max: Option<&str>,
        ts_literal_min: i64,
        ts_literal_max: i64,
    ) -> RowGroupBoundaryRange {
        let names: Arc<[String]> = Arc::from(["service_name".to_string(), "timestamp".to_string()]);
        // service_name ASC: literal min in min_key, literal max in max_key.
        // timestamp DESC: literal max in min_key, literal min in max_key.
        let min_key = RowGroupBoundaryKey::new(vec![
            RowGroupBoundaryComponent {
                value: service_min.map(|service| RowGroupBoundaryValue::String(service.to_string())),
                descending: false,
                nulls_first: true,
            },
            RowGroupBoundaryComponent {
                value: Some(RowGroupBoundaryValue::TimestampMicros(ts_literal_max)),
                descending: true,
                nulls_first: true,
            },
        ]);
        let max_key = RowGroupBoundaryKey::new(vec![
            RowGroupBoundaryComponent {
                value: service_max.map(|service| RowGroupBoundaryValue::String(service.to_string())),
                descending: false,
                nulls_first: true,
            },
            RowGroupBoundaryComponent {
                value: Some(RowGroupBoundaryValue::TimestampMicros(ts_literal_min)),
                descending: true,
                nulls_first: true,
            },
        ]);
        let range = RowGroupBoundaryRange {
            names,
            min_key,
            max_key,
        };
        range.validate().expect("valid logs envelope");
        range
    }

    /// The column-wise union of several files' envelopes must equal a single
    /// file's envelope built from the per-column literal extremes — even when no
    /// individual input file holds the (service, timestamp) tuple that the merged
    /// file's synthetic column-wise bound pairs together. This is the property a
    /// whole-key min/max fold gets WRONG and the per-column fold gets right.
    #[test]
    fn union_envelope_folds_per_column_literal_extremes() {
        // File A: services [svc-a, svc-c], timestamps [10, 50].
        // File B: services [svc-b, svc-d], timestamps [5, 70].
        let ranges = vec![
            logs_envelope("svc-a", "svc-c", 10, 50),
            logs_envelope("svc-b", "svc-d", 5, 70),
        ];

        let union = union_envelope(&ranges).expect("union ok").expect("non-empty union");

        // The merged file spans services [svc-a, svc-d] and timestamps [5, 70];
        // its direction-aware envelope is exactly `logs_envelope("svc-a",
        // "svc-d", 5, 70)`, pairing svc-a with the global max timestamp (70) in
        // min_key — a tuple no input row holds, yet the column-wise union must
        // reproduce it.
        let expected = logs_envelope("svc-a", "svc-d", 5, 70);
        assert_eq!(
            union.min_key.compare_checked(&expected.min_key).expect("compatible"),
            Ordering::Equal,
            "union min_key must equal the column-wise literal-extreme min_key"
        );
        assert_eq!(
            union.max_key.compare_checked(&expected.max_key).expect("compatible"),
            Ordering::Equal,
            "union max_key must equal the column-wise literal-extreme max_key"
        );
    }

    /// A merge that drops the row carrying a sort column's literal extreme
    /// changes that column's union bound, so the output union envelope must
    /// differ from the input union envelope. This is the bad-merge case the
    /// invariant exists to catch.
    #[test]
    fn union_envelope_detects_dropped_column_extreme() {
        // Inputs span timestamps [5, 70].
        let inputs = vec![
            logs_envelope("svc-a", "svc-c", 10, 50),
            logs_envelope("svc-b", "svc-d", 5, 70),
        ];
        // A faulty merge dropped every row with timestamp 70: output max ts = 50.
        let outputs = vec![logs_envelope("svc-a", "svc-d", 5, 50)];

        let inputs_union = union_envelope(&inputs).expect("ok").expect("non-empty");
        let outputs_union = union_envelope(&outputs).expect("ok").expect("non-empty");

        // The descending timestamp's literal max lives in `min_key`, so the
        // dropped 70 makes the two min_keys differ — the invariant fires.
        assert_ne!(
            inputs_union
                .min_key
                .compare_checked(&outputs_union.min_key)
                .expect("compatible"),
            Ordering::Equal,
            "dropping a column's literal extreme must change the union envelope"
        );
    }

    /// A lossless merge preserves every row — including those whose optional
    /// sort column is NULL — but Iceberg manifest bounds describe only NON-NULL
    /// values: a data file whose `service_name` is entirely null is omitted from
    /// the bounds map and decodes to a `None`-valued component. Such an absent
    /// bound carries no constraint, so it ANNIHILATES the column-wise union:
    /// `service_name`'s union extreme becomes absent because one input file
    /// omitted it. An absent union extreme is skipped by `first_envelope_conflict`,
    /// so the invariant does not fire even though the merged output records a
    /// concrete non-null `service_name` bound — the perfectly-correct merge that
    /// the production `spans` failure regressed must pass.
    #[test]
    fn union_envelope_tolerates_absent_optional_bound() {
        // Input file A: service_name entirely NULL (bound absent); ts [10, 50].
        // Input file B: service_name [svc-a, svc-c]; ts [5, 70].
        let inputs = vec![
            logs_envelope_opt(None, None, 10, 50),
            logs_envelope_opt(Some("svc-a"), Some("svc-c"), 5, 70),
        ];
        // Merged output: the null rows and valued rows now share one file, whose
        // service_name lower/upper bounds are the non-null [svc-a, svc-c] and
        // whose timestamps span the full [5, 70].
        let outputs = vec![logs_envelope_opt(Some("svc-a"), Some("svc-c"), 5, 70)];

        let inputs_union = union_envelope(&inputs).expect("ok").expect("non-empty");
        let outputs_union = union_envelope(&outputs).expect("ok").expect("non-empty");

        assert_eq!(
            first_envelope_conflict(&inputs_union.min_key, &outputs_union.min_key).expect("compatible"),
            None,
            "an all-null optional sort column must not trip the envelope invariant on the min side"
        );
        assert_eq!(
            first_envelope_conflict(&inputs_union.max_key, &outputs_union.max_key).expect("compatible"),
            None,
            "the timestamp column (present on both sides) must still agree, and absent service_name is skipped"
        );
    }

    /// An empty set of files has no envelope.
    #[test]
    fn union_envelope_empty_is_none() {
        assert!(union_envelope(&[]).expect("ok").is_none());
    }

    /// Build a one-column `trace_id`-shaped key (`Fixed(16)` ASC, nulls-first)
    /// whose single bound may be ABSENT (`None`), modelling a manifest that did
    /// or did not record the column's bound.
    fn trace_id_key(value: Option<[u8; 16]>) -> RowGroupBoundaryKey {
        RowGroupBoundaryKey::new(vec![RowGroupBoundaryComponent {
            value: value.map(|bytes| RowGroupBoundaryValue::FixedBytes(bytes.to_vec())),
            descending: false,
            nulls_first: true,
        }])
    }

    /// The exact production failure: a REQUIRED `trace_id` whose bound is ABSENT
    /// in the input manifest (parquet marked the stat non-exact, so iceberg-rust
    /// omitted it) but PRESENT in the freshly written output. An absent bound
    /// records no constraint, so the comparison must tolerate it rather than
    /// reject the rewrite — in BOTH directions (input-absent and output-absent).
    #[test]
    fn first_envelope_conflict_tolerates_bound_absent_on_either_side() {
        let present = trace_id_key(Some([7u8; 16]));
        let absent = trace_id_key(None);

        assert_eq!(
            first_envelope_conflict(&absent, &present).expect("compatible"),
            None,
            "an absent input bound must not conflict with a present output bound"
        );
        assert_eq!(
            first_envelope_conflict(&present, &absent).expect("compatible"),
            None,
            "a present input bound must not conflict with an absent output bound"
        );
        assert_eq!(
            first_envelope_conflict(&absent, &absent).expect("compatible"),
            None,
            "two absent bounds do not conflict"
        );
    }

    /// When BOTH sides recorded a bound, the comparison still flags a genuine
    /// difference (the real-corruption case the invariant exists to catch) and
    /// passes identical bounds.
    #[test]
    fn first_envelope_conflict_flags_differing_present_bounds() {
        let a = trace_id_key(Some([1u8; 16]));
        let b = trace_id_key(Some([2u8; 16]));

        assert_eq!(
            first_envelope_conflict(&a, &b).expect("compatible"),
            Some(0),
            "two present-but-different bounds must conflict at column 0"
        );
        assert_eq!(
            first_envelope_conflict(&a, &a).expect("compatible"),
            None,
            "two identical present bounds do not conflict"
        );
    }

    /// Build a one-column `trace_id`-shaped envelope (`Fixed(16)` ASC,
    /// nulls-first) from its literal `(min, max)` bounds, either of which may be
    /// ABSENT (`None`) to model a data file whose manifest omitted that bound
    /// (parquet marked the fixed-binary stat non-exact, so iceberg-rust dropped
    /// it). Mirrors the manifest-scan decode: ASC ⇒ literal min in `min_key`,
    /// literal max in `max_key`. `validate` is intentionally not called so an
    /// absent-upper / present-lower file can be represented.
    fn trace_id_envelope(min: Option<[u8; 16]>, max: Option<[u8; 16]>) -> RowGroupBoundaryRange {
        let component = |value: Option<[u8; 16]>| RowGroupBoundaryComponent {
            value: value.map(|bytes| RowGroupBoundaryValue::FixedBytes(bytes.to_vec())),
            descending: false,
            nulls_first: true,
        };
        RowGroupBoundaryRange {
            names: Arc::from(["trace_id".to_string()]),
            min_key: RowGroupBoundaryKey::new(vec![component(min)]),
            max_key: RowGroupBoundaryKey::new(vec![component(max)]),
        }
    }

    /// The production `spans`/`operations` failure: a rewrite group whose leading
    /// `trace_id` (`Fixed(16)`) sort key is recorded EXACTLY by one input file
    /// but OMITTED by another that actually holds the group's true maximum.
    /// Folding the absent bound as the identity understates the input-union max
    /// to the smaller present bound, while the losslessly-merged output records
    /// the true (exact) max — a FALSE envelope-invariant violation that wedges
    /// every Fixed(16)-sorted table's compaction. Because at least one input file
    /// omitted the bound, the column's input-union extreme is UNCONSTRAINED, so
    /// the invariant must NOT fire. Uses the byte values from the live failure.
    #[test]
    fn union_envelope_tolerates_input_max_understated_by_omitted_binary_bound() {
        // input=255,144,... in the production error; the file that recorded it.
        let recorded_max = [255, 144, 0, 138, 103, 237, 33, 106, 50, 153, 156, 110, 138, 241, 3, 58];
        // output=255,240,... in the production error; the true max, held by the
        // input file that OMITTED its trace_id bound and surfaced only after the
        // lossless merge wrote an exact output bound.
        let true_max = [255, 240, 168, 237, 155, 158, 11, 211, 225, 18, 82, 40, 58, 13, 212, 90];
        let group_min = [0u8; 16];

        let inputs = vec![
            // File A: records an exact (but not group-global) trace_id range.
            trace_id_envelope(Some(group_min), Some(recorded_max)),
            // File B: trace_id bound omitted entirely (non-exact ⇒ dropped),
            // yet its rows carry `true_max`.
            trace_id_envelope(None, None),
        ];
        // The merge fuses both files; the output records an exact bound covering
        // the true group max.
        let outputs = vec![trace_id_envelope(Some(group_min), Some(true_max))];

        let inputs_union = union_envelope(&inputs).expect("ok").expect("non-empty");
        let outputs_union = union_envelope(&outputs).expect("ok").expect("non-empty");

        assert_eq!(
            first_envelope_conflict(&inputs_union.max_key, &outputs_union.max_key).expect("compatible"),
            None,
            "an input max understated by an omitted binary bound must not trip the envelope invariant"
        );
        assert_eq!(
            first_envelope_conflict(&inputs_union.min_key, &outputs_union.min_key).expect("compatible"),
            None,
            "the min side must likewise tolerate the omitted bound"
        );
    }
}
