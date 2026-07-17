//! Compaction REWRITE task executor: merge → write → atomic replace.
//!
//! A REWRITE task takes one planner-produced rewrite group (the input data-file
//! paths of one `(tenant_id, day)` partition, already in sorted order) and:
//!
//! 1. loads the table fresh and re-enumerates the partition's data files,
//! 2. k-way-merges the already-sorted inputs into target-sized Parquet through
//!    the shared [`write_record_batches_to_parquet`] pipeline,
//! 3. verifies the content invariants of
//!    [`crate::compact::data::envelope`] (row count + sort-key envelope are
//!    preserved), and
//! 4. atomically swaps the inputs for the outputs in a single Iceberg `replace`
//!    snapshot via the generic [`Transaction::rewrite_files`] action committed
//!    over an `Arc<dyn iceberg::Catalog>`.
//!
//! The merge output is written exactly **once** ([`CompactFilesExecutor::execute`]
//! step 4). [`Transaction::commit`] performs the optimistic-concurrency retry
//! internally: on a retryable commit conflict it reloads the table and replays
//! the SAME already-written `added`/`removed` sets against the fresh base, so the
//! expensive read+merge+encode work is never repeated here.
//!
//! If any input file the planner listed has vanished from the partition by the
//! time the executor runs (a sibling compactor already rewrote it),
//! [`CompactFilesExecutor::execute`] aborts cleanly with [`RewriteOutcome::Aborted`]
//! BEFORE writing or committing: the next planning scan will re-derive a group
//! from whatever files remain.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use iceberg::Catalog;
use iceberg::io::FileIO;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use icegate_common::iceberg_write::{WriteConfig, write_record_batches_to_parquet};
use icegate_common::manifest_scan::{DataFileStats, list_data_files_in_partition};
use icegate_common::merge::sort_key::SortColumnsDescriptor;
use icegate_common::merge::{MergeInput, MergePosition, RowGroupsMerger, SortedBatchMergerConfig};
use icegate_common::{WAL_OFFSET_PROPERTY, icegate_table_ident};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::compact::data::envelope::verify_content_invariants;
use crate::compact::data::merge_source::IcebergMergeSource;
use crate::compact::metrics::CompactMetrics;
use crate::error::{MaintainError, Result};

/// Upper bound on how many data files the merger opens concurrently while
/// rewriting one group. The rewrite group is small (a handful of sub-target
/// files), so the input count caps it; this constant only prevents a
/// pathologically wide group from opening an unbounded number of object-store
/// reads at once.
const MAX_READ_PARALLELISM: usize = 8;

/// A serialized PLAN → REWRITE message: one rewrite group to execute.
///
/// Produced by the planner (Task 4.4 wiring) for one partition and persisted in
/// the job queue, so it must round-trip through serde. The
/// [`input_file_paths`](Self::input_file_paths) are listed in sorted order
/// (position = index), matching the order [`CompactFilesExecutor::execute`] assigns
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
pub struct CompactFilesExecutor {
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

impl CompactFilesExecutor {
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
        let table_ident = icegate_table_ident(&input.table);

        // Step 1: load the table FRESH. The transaction commit reloads it again
        // and guards concurrency itself, so this load only needs to be recent
        // enough to plan the merge against the live data files.
        let table = self.catalog.load_table(&table_ident).await?;

        // Step 2: enumerate the partition's live data files and match the planned
        // input paths. A missing input means a sibling compactor already took it;
        // abort before doing any merge/write work.
        let Some(matched) = self.match_inputs(&table, &input.partition_key, &input.input_file_paths).await? else {
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

        // If the matched inputs held zero rows the merge produces no output
        // files. The replace transaction rejects an empty added-file set, so
        // committing would fail the task on a no-op; treat it as a clean abort
        // instead (the next scan re-derives a group from whatever remains).
        if added.is_empty() {
            self.metrics.record_commit_aborted(&input.table);
            return Ok(RewriteOutcome::Aborted);
        }

        // Step 5: content invariants. A violation rejects the rewrite BEFORE any
        // commit, so the freshly written output files are definitely unreferenced
        // and safe to delete. The shared writer only cleans up failures that occur
        // inside the write pipeline; after it returns successfully ownership of
        // pre-commit cleanup belongs to this executor.
        if let Err(error) = verify_content_invariants(&table, self.descriptor, &removed, &added) {
            cleanup_uncommitted_data_files(table.file_io(), &added).await;
            return Err(error);
        }

        // Observability for the carry-forward invariant: `inherit_summary_property`
        // is a SILENT no-op when the base snapshot carries no offset, which (under
        // Nessie's severed parent chain) produces an unrecoverable offset-less
        // replace. Warn so the breakage is visible at its origin rather than only
        // later as a Shifter/query WAL-offset gate failure. The value actually
        // committed is still resolved against the FRESH base under the commit's
        // optimistic retry, so this is a heads-up, not the committed source of truth.
        if !table
            .metadata()
            .current_snapshot()
            .is_some_and(|snapshot| snapshot.summary().additional_properties.contains_key(WAL_OFFSET_PROPERTY))
        {
            tracing::warn!(
                table = input.table.as_str(),
                partition = input.partition_key.as_str(),
                "compaction base snapshot carries no {WAL_OFFSET_PROPERTY}; the replace snapshot will \
                 inherit none and the Shifter/query WAL-offset gate may fail until a shift commit \
                 restamps it"
            );
        }

        // Step 6: atomically swap the inputs for the outputs in one `replace`
        // snapshot. `Transaction::commit` does the optimistic retry internally.
        //
        // Carry the Shifter's WAL offset forward onto the new `replace` snapshot,
        // UNCHANGED. A `replace` snapshot otherwise drops the property, and the
        // Nessie REST catalog returns only the current snapshot with no parent
        // chain — so an offset missing from the current snapshot is unrecoverable
        // and the Shifter would resume from 0, re-committing the whole WAL.
        // `inherit_summary_property` resolves the value at snapshot-production
        // time from the snapshot this rewrite actually supersedes, so the
        // commit's internal optimistic retry uses the FRESH base after any racing
        // shift commit — never a value frozen here before that race.
        let input_files = removed.len();
        // The output files are already written to object storage. If the replace
        // is rejected (apply) or fails to commit, they leak as orphans unless we
        // delete them, so capture their paths before `add_data_files` consumes
        // `added`.
        let output_paths: Vec<String> = added.iter().map(|file| file.file_path().to_string()).collect();
        let tx = Transaction::new(&table);
        let pending = tx
            .rewrite_files()
            .add_data_files(added)
            .delete_files(removed)
            .inherit_summary_property(WAL_OFFSET_PROPERTY)
            .apply(tx);
        let tx = match pending {
            Ok(tx) => tx,
            Err(error) => {
                // `apply` only assembles the replace action locally, so a failure
                // here means nothing was committed and the outputs are
                // unambiguously unreferenced — safe to delete.
                cleanup_orphaned_outputs(table.file_io(), &output_paths).await;
                return Err(MaintainError::from(error));
            }
        };
        let commit_span = info_span!(
            "compact_commit",
            table = input.table.as_str(),
            partition = input.partition_key.as_str()
        );
        if let Err(error) = tx.commit(self.catalog.as_ref()).instrument(commit_span).await {
            // A commit failure is AMBIGUOUS: the catalog may have applied the
            // replace remotely before the error surfaced, so the outputs cannot be
            // deleted blindly (that could remove files the new snapshot
            // references). Reload and delete only the outputs the table does NOT
            // reference.
            self.cleanup_orphans_after_failed_commit(&input.table, &input.partition_key, &output_paths)
                .await;
            return Err(MaintainError::from(error));
        }

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

    /// After an ambiguous commit failure, best-effort delete the freshly written
    /// outputs that the table does NOT reference.
    ///
    /// The commit may have been applied remotely before the error surfaced, so
    /// blindly deleting `output_paths` could remove files the new snapshot
    /// references. Reload the table and keep any output that is now live (its
    /// file names are unique to this rewrite, so a live output means the commit
    /// took effect); delete only the rest. If the reload or enumeration fails,
    /// leave everything in place — a leftover orphan is reclaimable by a later
    /// orphan-file sweep, but deleting referenced data is not recoverable.
    async fn cleanup_orphans_after_failed_commit(
        &self,
        table_name: &str,
        partition_key: &str,
        output_paths: &[String],
    ) {
        let table = match self.catalog.load_table(&icegate_table_ident(table_name)).await {
            Ok(table) => table,
            Err(error) => {
                tracing::warn!(
                    table = table_name,
                    error = %error,
                    "could not reload table to clean up compaction outputs after a failed commit; leaving them in place"
                );
                return;
            }
        };
        let live: HashSet<String> = match list_data_files_in_partition(&table, self.descriptor, partition_key).await {
            Ok(stats) => stats.iter().map(|stat| stat.data_file.file_path().to_string()).collect(),
            Err(error) => {
                tracing::warn!(
                    table = table_name,
                    error = %error,
                    "could not enumerate partition to clean up compaction outputs after a failed commit; leaving them in place"
                );
                return;
            }
        };
        let orphans: Vec<String> = output_paths
            .iter()
            .filter(|path| !live.contains(path.as_str()))
            .cloned()
            .collect();
        cleanup_orphaned_outputs(table.file_io(), &orphans).await;
    }

    /// Enumerate the table's live data files and return the ones matching
    /// `input_file_paths`, ordered by sort-key `min_key` (the merge/position
    /// order). Returns `None` if ANY requested path is absent (a sibling
    /// compactor already rewrote it).
    async fn match_inputs(
        &self,
        table: &Table,
        partition_key: &str,
        input_file_paths: &[String],
    ) -> Result<Option<Vec<DataFileStats>>> {
        // Enumerate only this group's partition: every planned input belongs to
        // it, so a table-wide scan would re-decode the bounds of every other
        // partition's files on every fanned-out REWRITE task for nothing.
        let all_stats = list_data_files_in_partition(table, self.descriptor, partition_key).await?;

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
        let mut paths_by_position: HashMap<MergePosition, String> = HashMap::with_capacity(matched.len());
        let mut inputs: Vec<MergeInput> = Vec::with_capacity(matched.len());
        for (index, stats) in matched.iter().enumerate() {
            // `position = index` is safe: `matched` is already in `min_key`
            // order, and the index is far below `u128::MAX`.
            let position = MergePosition::new(index as u128);
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

/// Borrow a data file's path as the map key.
fn path_key(stats: &DataFileStats) -> &str {
    stats.data_file.file_path()
}

/// Best-effort deletion of successfully written data files that were rejected
/// before the Iceberg transaction was committed.
///
/// Cleanup failures are logged without replacing the validation error that
/// caused the rewrite to abort. This helper must not be used after an ambiguous
/// catalog commit failure because the commit may have succeeded remotely.
async fn cleanup_uncommitted_data_files(file_io: &FileIO, data_files: &[DataFile]) {
    for data_file in data_files {
        let path = data_file.file_path();
        if let Err(error) = file_io.delete(path).await {
            tracing::warn!(
                path,
                error = %error,
                "failed to cleanup compaction output rejected before commit"
            );
        }
    }
}

/// Best-effort deletion of orphaned compaction output files by path.
///
/// Used on the apply/commit failure paths where the outputs are already written
/// but `added` has been consumed by the transaction, so only paths remain.
/// Cleanup failures are logged and otherwise ignored so they never mask the
/// original error.
async fn cleanup_orphaned_outputs(file_io: &FileIO, paths: &[String]) {
    for path in paths {
        if let Err(error) = file_io.delete(path).await {
            tracing::warn!(
                path,
                error = %error,
                "failed to delete orphaned compaction output after a failed rewrite"
            );
        }
    }
}
