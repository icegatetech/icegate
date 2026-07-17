//! Manifest-compaction task executor.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind};
use icegate_common::manifest_scan::list_manifest_entries;
use icegate_common::{WAL_OFFSET_PROPERTY, icegate_table_ident};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span, warn};

use crate::compact::manifest::planner::{ManifestCompactPlan, estimate_output_manifests, plan_manifest_compaction};
use crate::compact::metrics::CompactMetrics;
use crate::error::{MaintainError, Result};

/// Snapshot-summary property key marking a snapshot as a manifest rewrite.
///
/// A manifest rewrite adds no data files, and the snapshot producer rejects a
/// commit with neither added files nor summary properties, so this marker is
/// always set on the `replace` snapshot: it both records why the snapshot exists
/// and satisfies that empty-commit guard.
pub const MANIFEST_REWRITE_MARKER_KEY: &str = "icegate.manifest-rewrite";

/// Value stored under [`MANIFEST_REWRITE_MARKER_KEY`].
const MANIFEST_REWRITE_MARKER_VALUE: &str = "true";

/// Serialized message for one `compact_manifest` task.
///
/// Deliberately minimal: the executor re-derives the repack group from the
/// table's live manifest list at run time, so the payload carries only the
/// table name and never a stale manifest selection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestCompactInput {
    /// Table name within the `icegate` namespace (e.g. `logs`, `spans`).
    pub table: String,
}

/// Outcome of executing one `compact_manifest` task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestCompactOutcome {
    /// A manifest group was repacked and committed: `input_manifests` manifests
    /// were replaced by `output_manifests` manifests (strictly fewer).
    Committed {
        /// Number of input manifests removed from the current snapshot.
        input_manifests: usize,
        /// Number of output manifests the repack packed them into.
        output_manifests: usize,
    },
    /// No transaction was opened: the table has no snapshot, no group had two or
    /// more candidates, or the chosen group would not pack into fewer manifests.
    Skipped,
}

/// Executes one `compact_manifest` task: repack one group of small DATA manifests
/// via the generic [`Transaction::rewrite_manifests`] action.
///
/// One executor is built per table from that table's manifest tunables; it is
/// cheap to reuse across many runs of the same table.
pub struct ManifestCompactExecutor {
    /// Generic Iceberg catalog used to load the table and commit the rewrite.
    ///
    /// The commit goes through [`Transaction::commit`], which is generic over
    /// `&dyn Catalog` and performs the optimistic-concurrency retry internally.
    catalog: Arc<dyn Catalog>,
    /// Target byte size of each output manifest (packing target).
    target_manifest_size_bytes: u64,
    /// Fraction of the target below which a manifest is a repack candidate.
    candidate_size_ratio: f64,
    /// Upper bound on input manifests repacked per commit (incrementality).
    max_manifests_per_commit: usize,
    /// Compaction instruments, shared with the job's PLAN/REWRITE executors.
    metrics: CompactMetrics,
}

impl ManifestCompactExecutor {
    /// Build a manifest-compaction executor for one table.
    #[must_use]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        target_manifest_size_bytes: u64,
        candidate_size_ratio: f64,
        max_manifests_per_commit: usize,
        metrics: CompactMetrics,
    ) -> Self {
        Self {
            catalog,
            target_manifest_size_bytes,
            candidate_size_ratio,
            max_manifests_per_commit,
            metrics,
        }
    }

    /// Execute one manifest-compaction run end-to-end.
    pub async fn execute(
        &self,
        input: &ManifestCompactInput,
        cancel: &CancellationToken,
    ) -> Result<ManifestCompactOutcome> {
        let table_ident = icegate_table_ident(&input.table);

        // Load the table FRESH. The transaction commit reloads it again and guards
        // concurrency itself, so this load only needs to be recent enough to plan
        // the repack against the live manifest list.
        let table = self.catalog.load_table(&table_ident).await?;

        // Without a current snapshot there are no manifests to repack.
        if table.metadata().current_snapshot_id().is_none() {
            info!(
                "compact manifest: table '{}' has no snapshot, nothing to do",
                input.table
            );
            return Ok(ManifestCompactOutcome::Skipped);
        }

        let entries = list_manifest_entries(&table).await?;
        let (partition_spec_id, selected) = match plan_manifest_compaction(
            &entries,
            self.target_manifest_size_bytes,
            self.candidate_size_ratio,
            self.max_manifests_per_commit,
        ) {
            ManifestCompactPlan::Repack {
                partition_spec_id,
                manifests,
            } => (partition_spec_id, manifests),
            ManifestCompactPlan::Skip(survey) => {
                self.metrics.record_manifest_skipped(&input.table);
                info!(
                    table = input.table.as_str(),
                    total_manifests = survey.total_manifests,
                    data_manifests = survey.data_manifests,
                    candidate_manifests = survey.candidate_manifests,
                    largest_group_manifests = survey.largest_group_manifests,
                    largest_group_bytes = survey.largest_group_bytes,
                    target_manifest_size_bytes = self.target_manifest_size_bytes,
                    "compact manifest: found no manifest group worth repacking"
                );
                return Ok(ManifestCompactOutcome::Skipped);
            }
        };

        // Observability for the WAL-offset carry-forward invariant, mirroring the
        // data-rewrite path: `inherit_summary_property` is a SILENT no-op when the
        // base snapshot carries no offset (which, under Nessie's severed parent
        // chain, yields an offset-less replace). Warn so the breakage is visible at
        // its origin rather than only later as a Shifter/query WAL-offset gate
        // failure. The value actually committed is still resolved against the FRESH
        // base under the commit's optimistic retry, so this is a heads-up.
        if !table
            .metadata()
            .current_snapshot()
            .is_some_and(|snapshot| snapshot.summary().additional_properties.contains_key(WAL_OFFSET_PROPERTY))
        {
            warn!(
                table = input.table.as_str(),
                spec_id = partition_spec_id,
                "manifest compaction base snapshot carries no {WAL_OFFSET_PROPERTY}; the replace snapshot \
                 will inherit none and the Shifter/query WAL-offset gate may fail until a shift commit \
                 restamps it"
            );
        }

        // Cancellation checkpoint before the one irreversible network step. There
        // is no interruptible CPU work inside the commit (it repacks metadata, not
        // data), so a shutdown that arrives mid-commit lets it finish; re-running
        // the immutable task next cycle is idempotent.
        if cancel.is_cancelled() {
            return Err(MaintainError::Storage(
                "manifest compaction cancelled before commit".to_string(),
            ));
        }

        let input_manifests = selected.len();
        let manifests_before = entries.len();
        let estimated_output = usize::try_from(estimate_output_manifests(&selected, self.target_manifest_size_bytes))
            .unwrap_or(usize::MAX);
        let input_paths: Vec<String> = selected.into_iter().map(|manifest| manifest.manifest_path.clone()).collect();

        let mut snapshot_properties = HashMap::new();
        snapshot_properties.insert(
            MANIFEST_REWRITE_MARKER_KEY.to_string(),
            MANIFEST_REWRITE_MARKER_VALUE.to_string(),
        );

        let transaction = Transaction::new(&table);
        let pending = transaction
            .rewrite_manifests()
            .add_input_manifests(input_paths)
            .target_manifest_size_bytes(self.target_manifest_size_bytes)
            .set_snapshot_properties(snapshot_properties)
            .inherit_summary_property(WAL_OFFSET_PROPERTY)
            .apply(transaction)?;

        // The `compact_manifest_commit` span (a child of `compact_manifest`) covers
        // the commit's internal optimistic-concurrency retry loop. Instrument the
        // future rather than entering a guard, which would be held across the
        // `.await` and make the future `!Send`.
        let commit_span = info_span!("compact_manifest_commit", table = input.table.as_str());
        if let Err(error) = pending.commit(self.catalog.as_ref()).instrument(commit_span).await {
            if error.kind() == ErrorKind::NoReduction {
                self.metrics.record_manifest_skipped(&input.table);
                info!(
                    "compact manifest: table '{}' selected {input_manifests} manifest(s) but the rewrite \
                     action found no per-schema reduction, skipping",
                    input.table
                );
                return Ok(ManifestCompactOutcome::Skipped);
            }
            return Err(error.into());
        }

        // Record the ACTUAL output count, re-derived from the committed manifest
        // list, because the packer's per-schema split can diverge from the
        // pre-commit estimate. A re-read failure is non-fatal (the commit is
        // already durable), so fall back to the estimate with a warn.
        let output_manifests = match self
            .count_output_manifests(&table_ident, manifests_before, input_manifests)
            .await
        {
            Ok(actual) => actual,
            Err(error) => {
                warn!(
                    table = input.table.as_str(),
                    %error,
                    "manifest compaction committed but re-reading the manifest list for the output count \
                     failed; reporting the pre-commit estimate instead"
                );
                estimated_output
            }
        };

        self.metrics
            .record_manifest_compacted(&input.table, input_manifests as u64, output_manifests as u64);
        Ok(ManifestCompactOutcome::Committed {
            input_manifests,
            output_manifests,
        })
    }

    /// Actual number of output manifests a committed repack produced, re-derived
    /// from the table's post-commit manifest list.
    ///
    /// The repack removed `input_manifests` from a list of `manifests_before`
    /// total and added the outputs, so
    /// `output = total_after - (manifests_before - input_manifests)`. The reload
    /// races with any concurrent commit (e.g. a Shifter append), which would
    /// perturb the count by the manifests it added; the window is small and this
    /// feeds an observability metric only.
    async fn count_output_manifests(
        &self,
        table_ident: &iceberg::TableIdent,
        manifests_before: usize,
        input_manifests: usize,
    ) -> Result<usize> {
        let table = self.catalog.load_table(table_ident).await?;
        let manifests_after = list_manifest_entries(&table).await?.len();
        let carried = manifests_before.saturating_sub(input_manifests);
        Ok(manifests_after.saturating_sub(carried))
    }
}
