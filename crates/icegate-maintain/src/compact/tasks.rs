//! Jobmanager task layer for compaction.
//!
//! One runner per task code. Each runner is the ONLY place that knows about the
//! jobmanager: it opens the task's tracing span, parses the serialized payload,
//! drives the matching domain executor, and completes the task. The domain
//! modules ([`crate::compact::data`], [`crate::compact::manifest`]) stay free of
//! jobmanager dependencies and are unit-testable without one.
//!
//! PLAN is the exception that proves the rule: fanning out REWRITE and MANIFEST
//! tasks IS a jobmanager operation, so [`PlanTaskRunner`] holds the planning
//! I/O itself and delegates only the pure bin-packing to
//! [`crate::compact::data::planner`].

use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use iceberg::Catalog;
use icegate_common::icegate_table_ident;
use icegate_common::manifest_scan::list_data_files_with_stats;
use jobmanager::{Error as JobError, ImmutableTask, JobManager, TaskCode, TaskDefinition, TaskExecutorFn};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info, info_span};

use crate::compact::compactor::CompactJobSpec;
use crate::compact::data::planner::{PlannerLimits, plan_rewrite_groups};
use crate::compact::data::rewrite::{CompactFilesExecutor, RewriteInput, RewriteOutcome};
use crate::compact::manifest::rewrite::{ManifestCompactExecutor, ManifestCompactInput, ManifestCompactOutcome};
use crate::compact::metrics::CompactMetrics;

/// Task code for the compaction PLAN task (enumerate + fan-out REWRITEs).
pub const PLAN_TASK_CODE: &str = "compact_plan";
/// Task code for the compaction REWRITE task (merge one group + atomic replace).
pub const FILES_TASK_CODE: &str = "compact_files";
/// Task code for the manifest-compaction task (repack one manifest group).
pub const MANIFEST_TASK_CODE: &str = "compact_manifest";

/// Per-table PLAN runner: enumerate the table's data files, bin-pack them into
/// rewrite groups, and fan out one REWRITE task per group.
///
/// Holds everything one job's PLAN task needs by value/`Arc` so the executor
/// closure is `'static`. Constructed by [`crate::compact::compactor`], which is
/// why the fields are crate-visible rather than behind a wide constructor.
pub struct PlanTaskRunner {
    /// Generic Iceberg catalog used to load the table fresh each iteration.
    pub(super) catalog: Arc<dyn Catalog>,
    /// The table this runner plans for, with its sort/encoding policy.
    pub(super) spec: CompactJobSpec,
    /// Tunables deciding which partitions to skip and how large a group may grow.
    pub(super) planner_limits: PlannerLimits,
    pub(super) compact_files_timeout: ChronoDuration,
    pub(super) compact_manifest_timeout: ChronoDuration,
    /// Compaction instruments shared with this job's other runners; the PLAN
    /// path records the partition compacted/skipped and group fan-out counts.
    pub(super) metrics: CompactMetrics,
}

impl PlanTaskRunner {
    /// Run one PLAN task: load the table fresh, plan rewrite groups against its
    /// current `main` snapshot, submit a REWRITE task per group, and complete
    /// the PLAN task.
    ///
    /// If the table has no current snapshot (freshly created, never committed
    /// to) or the planner finds no work, the task completes with no REWRITE
    /// tasks submitted.
    async fn run(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), JobError> {
        let span = info_span!("compact_plan", table = self.spec.table);
        self.run_plan(task, manager, cancel).instrument(span).await
    }

    /// The instrumented body of [`Self::run`], split out so the `compact_plan`
    /// span wraps the whole async future (an [`tracing::Span::entered`] guard
    /// cannot be held across the `.await` points without making the future
    /// `!Send`).
    ///
    /// `cancel` is checked before each potentially slow step (table load, data
    /// file enumeration) and before fanning out each REWRITE task, so a shutdown
    /// stops the PLAN promptly. A cancelled PLAN returns an error rather than
    /// completing, leaving the immutable PLAN task to be re-run on the next
    /// discovery cycle — planning is idempotent, so re-enumerating is safe.
    async fn run_plan(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), JobError> {
        check_cancellation(cancel)?;
        let table_ident = icegate_table_ident(self.spec.table);
        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| JobError::TaskExecution(format!("failed to load table '{}': {e}", self.spec.table)))?;

        // Without a current snapshot there is nothing to compact. The rewrite
        // transaction guards its own concurrency, so the planner no longer needs
        // to capture a base snapshot id for the REWRITE tasks.
        if table.metadata().current_snapshot_id().is_none() {
            info!(
                "compact plan: table '{}' has no snapshot, nothing to do",
                self.spec.table
            );
            return manager.complete_task(task.id(), Vec::new());
        }

        check_cancellation(cancel)?;
        let stats = list_data_files_with_stats(&table, self.spec.descriptor)
            .await
            .map_err(|e| JobError::TaskExecution(format!("failed to enumerate data files: {e}")))?;

        let outcome = plan_rewrite_groups(stats, &self.planner_limits);
        let group_count = outcome.groups.len();
        // PLAN telemetry: groups fanned out, plus the partition compacted/skipped
        // split. `usize -> u64` is lossless on every supported (<= 64-bit) target.
        self.metrics.record_plan(
            self.spec.table,
            group_count as u64,
            outcome.partitions_compacted as u64,
            outcome.partitions_skipped as u64,
        );

        // Ids of the REWRITE tasks fanned out this iteration; a `compact_manifest`
        // task depends on them so it runs only after data compaction settles.
        let mut rewrite_ids = Vec::new();
        for group in outcome.groups {
            // Stop fanning out promptly on shutdown. Tasks already submitted stay
            // queued; the immutable PLAN task re-runs next cycle and re-derives the
            // remaining groups.
            check_cancellation(cancel)?;
            // Every file in a planner group shares a partition key; an empty
            // group never occurs (`plan_rewrite_groups` only emits non-empty
            // groups), but guard defensively rather than index blindly.
            let Some(first) = group.first() else { continue };
            let partition_key = first.partition_key().to_string();
            // Input paths in sort-key (`min_key`) order: that is the position
            // contract the REWRITE executor relies on. The planner preserves
            // cluster order, but re-sorting here keeps the contract explicit and
            // independent of upstream ordering guarantees.
            let mut ordered = group;
            ordered.sort_by(|left, right| left.min_key().compare(right.min_key()));
            let input_file_paths: Vec<String> =
                ordered.iter().map(|stats| stats.data_file.file_path().to_string()).collect();

            let rewrite_input = RewriteInput {
                table: self.spec.table.to_string(),
                partition_key,
                input_file_paths,
            };
            let payload = serde_json::to_vec(&rewrite_input)
                .map_err(|e| JobError::TaskExecution(format!("failed to serialize rewrite input: {e}")))?;
            // TODO(med): the serialized payload embeds every input file path, so a
            // group with very many files can exceed the jobmanager's per-task
            // `max_input_bytes` and fail `add_task` (failing the whole PLAN task).
            // The byte-budget bin-packer bounds a group's summed *bytes*, not its
            // file *count*, so a partition of many tiny files can still produce a
            // large path list. Cap files-per-group (or chunk the payload) so a
            // pathological partition degrades gracefully instead of erroring.
            let files_task = TaskDefinition::new(TaskCode::new(FILES_TASK_CODE), payload, self.compact_files_timeout)?;
            rewrite_ids.push(manager.add_task(files_task)?);
        }

        let manifest_input = ManifestCompactInput {
            table: self.spec.table.to_string(),
        };
        let payload = serde_json::to_vec(&manifest_input)
            .map_err(|e| JobError::TaskExecution(format!("failed to serialize manifest compact input: {e}")))?;
        let manifest_task = TaskDefinition::new(
            TaskCode::new(MANIFEST_TASK_CODE),
            payload,
            self.compact_manifest_timeout,
        )?
        .with_dependencies(rewrite_ids);
        manager.add_task(manifest_task)?;

        info!(
            "compact plan: table '{}' scheduled {} rewrite task(s)",
            self.spec.table, group_count
        );
        manager.complete_task(task.id(), Vec::new())
    }
}

/// Runs data-file compaction for a single Iceberg table.
pub struct CompactFilesRunner {
    executor: CompactFilesExecutor,
    table: &'static str,
}

impl CompactFilesRunner {
    /// Creates a runner bound to `table`, driven by `executor`.
    #[must_use]
    pub const fn new(executor: CompactFilesExecutor, table: &'static str) -> Self {
        Self { executor, table }
    }

    async fn run(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> Result<(), JobError> {
        // The `(tenant, day)` partition is only known after parsing the input, so
        // declare the field empty and fill it in below via `Span::record`.
        let span = info_span!("compact_files", table = self.table, partition = tracing::field::Empty);
        self.run_compact(task, manager, cancel).instrument(span).await
    }

    async fn run_compact(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> Result<(), JobError> {
        let input: RewriteInput = serde_json::from_slice(task.get_input())
            .map_err(|e| JobError::TaskExecution(format!("failed to parse rewrite input: {e}")))?;
        tracing::Span::current().record("partition", input.partition_key.as_str());

        let outcome = self
            .executor
            .execute(&input, cancel)
            .await
            .map_err(|e| JobError::TaskExecution(format!("rewrite of table '{}' failed: {e}", self.table)))?;

        match outcome {
            RewriteOutcome::Committed {
                input_files,
                output_files,
                rows,
                ..
            } => {
                info!(
                    "compact files: table '{}' replaced {} file(s) with {} file(s) holding {} row(s)",
                    self.table, input_files, output_files, rows
                );
            }
            RewriteOutcome::Aborted => {
                info!(
                    "compact files: table '{}' aborted (an input was taken by a sibling compactor)",
                    self.table
                );
            }
        }

        manager.complete_task(task.id(), Vec::new())
    }
}

/// Per-table MANIFEST runner: deserialize one `compact_manifest` input and
/// delegate to the table's [`ManifestCompactExecutor`].
pub struct CompactManifestRunner {
    executor: ManifestCompactExecutor,
    table: &'static str,
}

impl CompactManifestRunner {
    /// Wrap `executor` as the MANIFEST task runner for `table`.
    #[must_use]
    pub const fn new(executor: ManifestCompactExecutor, table: &'static str) -> Self {
        Self { executor, table }
    }

    /// Run one MANIFEST task: parse its [`ManifestCompactInput`], repack one
    /// manifest group (or skip when nothing is worth repacking), and complete the
    /// task. A skip is a successful completion, not a failure: the group either
    /// had too few candidates or would not reduce the manifest count.
    async fn run(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), JobError> {
        let span = info_span!("compact_manifest", table = self.table);
        self.run_compact(task, manager, cancel).instrument(span).await
    }

    /// The instrumented body of [`Self::run`], split out so the `compact_manifest`
    /// span wraps the whole async future (the span guard cannot cross the `.await`
    /// points without making the future `!Send`).
    async fn run_compact(
        &self,
        task: &dyn ImmutableTask,
        manager: &dyn JobManager,
        cancel: &CancellationToken,
    ) -> std::result::Result<(), JobError> {
        check_cancellation(cancel)?;
        let input: ManifestCompactInput = serde_json::from_slice(task.get_input())
            .map_err(|e| JobError::TaskExecution(format!("failed to parse manifest compact input: {e}")))?;

        let outcome = self.executor.execute(&input, cancel).await.map_err(|e| {
            JobError::TaskExecution(format!("manifest compaction of table '{}' failed: {e}", self.table))
        })?;

        match outcome {
            ManifestCompactOutcome::Committed {
                input_manifests,
                output_manifests,
            } => {
                info!(
                    "compact manifest: table '{}' repacked {} manifest(s) into {}",
                    self.table, input_manifests, output_manifests
                );
            }
            ManifestCompactOutcome::Skipped => {
                info!(
                    "compact manifest: table '{}' skipped (no group worth repacking)",
                    self.table
                );
            }
        }

        manager.complete_task(task.id(), Vec::new())
    }
}

/// Build the [`TaskExecutorFn`] for a job's PLAN task from a [`PlanTaskRunner`].
///
/// The jobmanager calls executors with a borrowed `&dyn JobManager` it owns, so
/// the closure clones the `Arc<PlanTaskRunner>` and reads the task input via the
/// [`ImmutableTask`] trait, exactly like ingest's shift executor.
#[must_use]
pub fn plan_executor_fn(runner: Arc<PlanTaskRunner>) -> TaskExecutorFn {
    Arc::new(move |task, manager, cancel| {
        let runner = Arc::clone(&runner);
        Box::pin(async move { runner.run(task.as_ref(), manager, &cancel).await })
    })
}

/// Build the [`TaskExecutorFn`] for a job's REWRITE task from a
/// [`CompactFilesRunner`]. The cancellation token is forwarded so an in-flight
/// merge stops promptly on shutdown.
#[must_use]
pub fn rewrite_executor_fn(runner: Arc<CompactFilesRunner>) -> TaskExecutorFn {
    Arc::new(move |task, manager, cancel| {
        let runner = Arc::clone(&runner);
        Box::pin(async move { runner.run(task.as_ref(), manager, &cancel).await })
    })
}

/// Build the [`TaskExecutorFn`] for a job's MANIFEST task from a
/// [`CompactManifestRunner`]. The cancellation token is forwarded so a shutdown
/// stops the run before its commit.
#[must_use]
pub fn manifest_executor_fn(runner: Arc<CompactManifestRunner>) -> TaskExecutorFn {
    Arc::new(move |task, manager, cancel| {
        let runner = Arc::clone(&runner);
        Box::pin(async move { runner.run(task.as_ref(), manager, &cancel).await })
    })
}

/// Return a task error if `cancel` has been triggered, so a task stops at the
/// next checkpoint on shutdown.
///
/// The jobmanager has no cancellation-specific outcome, so a cancelled task
/// surfaces as a [`JobError::TaskExecution`] (the same channel the REWRITE path
/// uses); the immutable task is simply re-run on the next discovery cycle.
fn check_cancellation(cancel: &CancellationToken) -> std::result::Result<(), JobError> {
    if cancel.is_cancelled() {
        return Err(JobError::TaskExecution("compaction task cancelled".to_string()));
    }
    Ok(())
}
