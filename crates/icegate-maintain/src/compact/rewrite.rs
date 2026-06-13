//! Compaction REWRITE task executor: merge → write → atomic replace.
//!
//! A REWRITE task takes one planner-produced rewrite group (the input data-file
//! paths of one `(tenant_id, day)` partition, already in sorted order) and:
//!
//! 1. loads the table fresh and re-enumerates the partition's data files,
//! 2. k-way-merges the already-sorted inputs into target-sized Parquet through
//!    the shared [`write_record_batches_to_parquet`] pipeline,
//! 3. verifies content invariants (row count + sort-key envelope are
//!    preserved), and
//! 4. atomically swaps the inputs for the outputs in a single Iceberg `replace`
//!    snapshot via the generic [`Transaction::rewrite_files`] action committed
//!    over an `Arc<dyn iceberg::Catalog>`.
//!
//! The merge output is written exactly **once** ([`RewriteExecutor::execute`]
//! step 4). [`Transaction::commit`] performs the optimistic-concurrency retry
//! internally: on a retryable commit conflict it reloads the table and replays
//! the SAME already-written `added`/`removed` sets against the fresh base, so the
//! expensive read+merge+encode work is never repeated here.
//!
//! If any input file the planner listed has vanished from the partition by the
//! time the executor runs (a sibling compactor already rewrote it),
//! [`RewriteExecutor::execute`] aborts cleanly with [`RewriteOutcome::Aborted`]
//! BEFORE writing or committing: the next planning scan will re-derive a group
//! from whatever files remain.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use icegate_common::ICEGATE_NAMESPACE;
use icegate_common::iceberg_write::{WriteConfig, write_record_batches_to_parquet};
use icegate_common::manifest_scan::{DataFileStats, decode_data_file_envelope, list_data_files_with_stats};
use icegate_common::merge::sort_key::{
    RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, SortColumnsDescriptor,
};
use icegate_common::merge::{MergeInput, RowGroupsMerger, SortedBatchMergerConfig};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::compact::iceberg_source::IcebergMergeSource;
use crate::compact::metrics::CompactMetrics;
use crate::error::{MaintainError, Result};

/// Upper bound on how many data files the merger opens concurrently while
/// building one overlap cluster. The rewrite group is small (a handful of
/// sub-target files), so the input count caps it; this constant only prevents a
/// pathologically wide cluster from opening an unbounded number of object-store
/// reads at once.
const MAX_READ_PARALLELISM: usize = 8;

/// A serialized PLAN → REWRITE message: one rewrite group to execute.
///
/// Produced by the planner (Task 4.4 wiring) for one partition and persisted in
/// the job queue, so it must round-trip through serde. The
/// [`input_file_paths`](Self::input_file_paths) are listed in sorted order
/// (position = index), matching the order [`RewriteExecutor::execute`] assigns
/// to the merger.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RewriteInput {
    /// Table name within the `icegate` namespace (e.g. `logs`, `spans`).
    pub table: String,
    /// Stable `(tenant_id, day)` partition key the rewrite group belongs to.
    pub partition_key: String,
    /// Input data-file paths, in sorted order (position = index).
    pub input_file_paths: Vec<String>,
}

/// Outcome of executing one REWRITE task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewriteOutcome {
    /// The rewrite committed: `input_files` files were replaced by
    /// `output_files` files holding `rows` rows.
    Committed {
        /// Number of input data files removed from the partition.
        input_files: usize,
        /// Number of output data files added to the partition.
        output_files: usize,
        /// Total rows in the output (equal to the input row total).
        rows: usize,
        /// Total compressed bytes across the input data files.
        input_bytes: u64,
        /// Total compressed bytes across the output data files.
        output_bytes: u64,
    },
    /// The rewrite was abandoned because at least one input file had vanished
    /// from the partition (a sibling compactor took it); nothing was committed.
    Aborted,
}

/// Executes one REWRITE task: merge a rewrite group's inputs into target-sized
/// Parquet and atomically replace them via the generic
/// [`Transaction::rewrite_files`] action.
///
/// One executor is built per table from that table's encoding policy and sort
/// descriptor; it is cheap to clone-free reuse across many rewrite groups of the
/// same table.
pub struct RewriteExecutor {
    /// Generic Iceberg catalog used to load the table and commit the replace.
    ///
    /// The commit goes through [`Transaction::commit`], which is generic over
    /// `&dyn Catalog` and performs the optimistic-concurrency retry internally.
    catalog: Arc<dyn Catalog>,
    /// Per-table Parquet write policy (encodings, bloom filters, row-group / page
    /// / target-file sizes).
    write_cfg: WriteConfig,
    /// Table sort descriptor, used both to enumerate input bounds and to compare
    /// rows during the merge.
    descriptor: &'static SortColumnsDescriptor,
    /// Compaction instruments recorded from the rewrite/commit path.
    metrics: CompactMetrics,
}

impl RewriteExecutor {
    /// Build a rewrite executor for one table.
    ///
    /// # Arguments
    ///
    /// * `catalog` - The generic Iceberg catalog used to load the table and
    ///   commit the replace.
    /// * `write_cfg` - The table's Parquet write policy.
    /// * `descriptor` - The table's sort descriptor.
    /// * `metrics` - The compaction instruments, shared with the job's PLAN
    ///   executor so both paths record to the same instruments. Build one with
    ///   [`CompactMetrics::new`] (a no-op until a meter provider is installed).
    #[must_use]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        write_cfg: WriteConfig,
        descriptor: &'static SortColumnsDescriptor,
        metrics: CompactMetrics,
    ) -> Self {
        Self {
            catalog,
            write_cfg,
            descriptor,
            metrics,
        }
    }

    /// Execute one rewrite group end-to-end.
    ///
    /// Loads the table fresh, matches the input paths to live data files,
    /// k-way-merges them into target-sized Parquet, checks the row-count and
    /// sort-key-envelope invariants against the inputs, and atomically replaces
    /// the inputs with the outputs via [`Transaction::rewrite_files`]. Returns
    /// [`RewriteOutcome::Aborted`] without writing anything if any input path is
    /// no longer present in the partition.
    ///
    /// The replace is committed through [`Transaction::commit`], which reloads
    /// the table and replays the same `added`/`removed` sets on a retryable
    /// commit conflict — the optimistic-concurrency retry happens inside the
    /// transaction, never re-running the merge.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be loaded or enumerated, if the
    /// merge/write fails, if a content invariant is violated (row count or
    /// sort-key envelope changed — the rewrite is rejected and NOT committed),
    /// or if the replace commit fails (including a removed input that is no
    /// longer live, which the transaction surfaces as a `DataInvalid` error).
    pub async fn execute(&self, input: &RewriteInput, cancel: &CancellationToken) -> Result<RewriteOutcome> {
        let table_ident = table_ident(&input.table);

        // Step 1: load the table FRESH. The transaction commit reloads it again
        // and guards concurrency itself, so this load only needs to be recent
        // enough to plan the merge against the live data files.
        let table = self.catalog.load_table(&table_ident).await?;

        // Step 2: enumerate the partition's live data files and match the planned
        // input paths. A missing input means a sibling compactor already took it;
        // abort before doing any merge/write work.
        let Some(matched) = self.match_inputs(&table, &input.input_file_paths).await? else {
            self.metrics.record_commit_aborted(&input.table);
            return Ok(RewriteOutcome::Aborted);
        };

        // The files to remove on commit are exactly the matched inputs; they must
        // be live in the table the transaction commits against (the transaction
        // rejects a stale removed file with a `DataInvalid` error).
        let removed: Vec<DataFile> = matched.iter().map(|stats| stats.data_file.clone()).collect();
        // Total compressed input bytes, captured before the merge consumes the
        // stats, for the compaction byte/ratio metrics.
        let input_bytes: u64 = matched.iter().map(DataFileStats::size_bytes).sum();

        // Steps 3+4: build the merged stream and write it ONCE into target-sized
        // parquet. `added` is committed verbatim (the transaction's internal
        // retry replays it without re-merging).
        let merged_stream = self.build_merge_stream(&table, &matched, cancel)?;
        let written = write_record_batches_to_parquet(table.clone(), self.write_cfg, merged_stream, cancel).await?;
        let output_files = written.data_files.len();
        let rows = written.rows_written;
        let added = written.data_files;
        let output_bytes: u64 = added.iter().map(DataFile::file_size_in_bytes).sum();

        // Step 5: content invariants. A violation rejects the rewrite BEFORE any
        // commit; the freshly written orphan output files are cleaned up by the
        // writer's orphan cleanup on a later failure path / GC.
        verify_content_invariants(&table, self.descriptor, &removed, &added)?;

        // Step 6: atomically swap the inputs for the outputs in one `replace`
        // snapshot. `Transaction::commit` does the optimistic retry internally.
        let input_files = removed.len();
        let tx = Transaction::new(&table);
        let tx = tx
            .rewrite_files()
            .add_data_files(added)
            .delete_files(removed)
            .apply(tx)
            .map_err(MaintainError::from)?;
        // The `compact_commit` span (a child of `compact_rewrite`) covers the
        // commit's internal optimistic-concurrency retry loop. Instrument the
        // commit future rather than entering a guard, which would otherwise be
        // held across the `.await` and make the future `!Send`.
        let commit_span = info_span!(
            "compact_commit",
            table = input.table.as_str(),
            partition = input.partition_key.as_str()
        );
        tx.commit(self.catalog.as_ref())
            .instrument(commit_span)
            .await
            .map_err(MaintainError::from)?;

        self.metrics.record_rewrite_committed(
            &input.table,
            input_files as u64,
            output_files as u64,
            rows as u64,
            input_bytes,
            output_bytes,
        );
        Ok(RewriteOutcome::Committed {
            input_files,
            output_files,
            rows,
            input_bytes,
            output_bytes,
        })
    }

    /// Enumerate the table's live data files and return the ones matching
    /// `input_file_paths`, ordered by sort-key `min_key` (the merge/position
    /// order). Returns `None` if ANY requested path is absent (a sibling
    /// compactor already rewrote it).
    async fn match_inputs(&self, table: &Table, input_file_paths: &[String]) -> Result<Option<Vec<DataFileStats>>> {
        let all_stats = list_data_files_with_stats(table, self.descriptor).await?;

        // Index live files by path so each requested input is a single lookup.
        // Keyed by an owned `String` because the value owns the path the key
        // would otherwise borrow from.
        let mut by_path: HashMap<String, DataFileStats> = all_stats
            .into_iter()
            .map(|stats| (path_key(&stats).to_string(), stats))
            .collect();

        let mut matched: Vec<DataFileStats> = Vec::with_capacity(input_file_paths.len());
        for wanted in input_file_paths {
            match by_path.remove(wanted) {
                Some(stats) => matched.push(stats),
                // A planned input is gone: abandon the whole group.
                None => return Ok(None),
            }
        }

        // Position order is sort-key order: sort the matched stats by min_key so
        // `position = index` reproduces the merger's intended stable order.
        matched.sort_by(|left, right| left.min_key().compare(right.min_key()));
        Ok(Some(matched))
    }

    /// Build the k-way-merged record-batch stream over the matched inputs.
    ///
    /// Each input gets `position = index` (after the `min_key` sort in
    /// [`Self::match_inputs`]), its compressed size, and its decoded boundary
    /// range; the position→path map drives the [`IcebergMergeSource`].
    fn build_merge_stream(
        &self,
        table: &Table,
        matched: &[DataFileStats],
        cancel: &CancellationToken,
    ) -> Result<icegate_common::iceberg_write::CommonRecordBatchStream> {
        let mut paths_by_position: HashMap<u128, String> = HashMap::with_capacity(matched.len());
        let mut inputs: Vec<MergeInput> = Vec::with_capacity(matched.len());
        for (index, stats) in matched.iter().enumerate() {
            // `position = index` is safe: `matched` is already in `min_key`
            // order, and the index is far below `u128::MAX`.
            let position = index as u128;
            paths_by_position.insert(position, stats.data_file.file_path().to_string());
            inputs.push(MergeInput::new(
                position,
                stats.size_bytes(),
                stats.boundary_range().clone(),
            ));
        }

        let source = IcebergMergeSource::new(table.file_io().clone(), paths_by_position);
        let merger = RowGroupsMerger::new(
            Arc::new(source),
            inputs,
            SortedBatchMergerConfig {
                row_group_size: self.write_cfg.row_group_size,
                read_parallelism: matched.len().clamp(1, MAX_READ_PARALLELISM),
                cancel_token: cancel.clone(),
                sort_descriptor: self.descriptor,
            },
        )
        .map_err(MaintainError::from)?;
        Ok(merger.into_stream())
    }
}

/// Build the [`TableIdent`] for a table name inside the `icegate` namespace.
fn table_ident(table: &str) -> TableIdent {
    TableIdent::new(NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), table.to_string())
}

/// Borrow a data file's path as the map key.
fn path_key(stats: &DataFileStats) -> &str {
    stats.data_file.file_path()
}

/// Verify the two REWRITE content invariants (§9) against the inputs.
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
/// Returns [`MaintainError::Config`] if either invariant is violated, or an
/// enumeration/decode error if an output file's bounds cannot be read.
fn verify_content_invariants(
    table: &Table,
    descriptor: &SortColumnsDescriptor,
    removed: &[DataFile],
    added: &[DataFile],
) -> Result<()> {
    // Invariant 1: total row count is preserved.
    let removed_rows: u64 = removed.iter().map(DataFile::record_count).sum();
    let added_rows: u64 = added.iter().map(DataFile::record_count).sum();
    if removed_rows != added_rows {
        return Err(MaintainError::Config(format!(
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
        return Err(MaintainError::Config(
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
        return Err(MaintainError::Config(format!(
            "rewrite envelope invariant violated: output minimum sort key differs from input minimum \
             ({} input file(s) -> {} output file(s); {})",
            removed.len(),
            added.len(),
            describe_conflict(&inputs_union.names, idx, &inputs_union.min_key, &outputs_union.min_key),
        )));
    }
    if let Some(idx) = first_envelope_conflict(&inputs_union.max_key, &outputs_union.max_key)? {
        return Err(MaintainError::Config(format!(
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
/// manifest bounds are optional and lossy (see the call site) — so it is
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
/// Iceberg manifest bounds cover only NON-NULL values, so a file whose optional
/// sort column is entirely null contributes an absent (`None`) bound. Such an
/// absent bound is the IDENTITY of the per-column fold (see
/// [`keep_literal_extreme`]): it never becomes a literal extreme, so a column
/// resolves to `None` only when EVERY file is null for it. This keeps the union
/// equal to the non-null value range a lossless merge preserves, rather than
/// letting one all-null input file pull the extreme to null.
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
    // An ABSENT bound (`value: None`) means the file held no non-null value for
    // this column: Iceberg omits an all-null column from the manifest
    // `lower_bounds`/`upper_bounds` maps. A non-existent value is neither a
    // literal minimum nor a literal maximum, so an absent bound is the IDENTITY
    // of the fold — when exactly one side is absent keep the present one, and
    // when both are absent the extreme stays absent. Comparing it as a null that
    // sorts first/last would instead let a single all-null input file drag the
    // whole group's union extreme to null, even though the merged output (which
    // mixes those null rows with valued rows from sibling files) reports a
    // concrete non-null bound — a FALSE envelope-invariant violation that blocks
    // a perfectly lossless compaction.
    match (&current.value, &candidate.value) {
        (None, _) => return Ok(candidate.clone()),
        (Some(_), None) => return Ok(current.clone()),
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
    /// bound carries no literal extreme, so it must act as the IDENTITY of the
    /// column-wise union: the union of a null-only file and a valued file equals
    /// the valued file's envelope. The merged output mixes the null rows with
    /// the valued rows, so Iceberg records the non-null minimum as its lower
    /// bound; the input-union and output-union `min_key`s must therefore agree.
    /// Treating the absent bound as a null that sorts first would wrongly make
    /// the input-union minimum null and trip the rewrite envelope invariant on a
    /// perfectly correct merge (the production `spans` failure this regresses).
    #[test]
    fn union_envelope_treats_absent_optional_bound_as_identity() {
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
            inputs_union
                .min_key
                .compare_checked(&outputs_union.min_key)
                .expect("compatible"),
            Ordering::Equal,
            "an all-null optional sort column must not make the union minimum null"
        );
        assert_eq!(
            inputs_union
                .max_key
                .compare_checked(&outputs_union.max_key)
                .expect("compatible"),
            Ordering::Equal,
            "the union maximum must equal the valued file's maximum"
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
}
