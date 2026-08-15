//! WAL segment cleanup: delete the queue tail that Iceberg no longer needs.
//!
//! One jobmanager job per enabled topic, on the cleanup runner's own pool
//! ([`runner`]). Each cycle derives a delete bound from the table the topic
//! shifts into and hands it to [`icegate_queue::QueueCleaner`]; how the bound is
//! derived and what cleanup deliberately leaves alone is in
//! [`README.md`](./README.md).

/// Cleanup configuration: retention floors, per-cycle budgets, cadence.
pub mod config;
/// Cleanup instruments recorded to the `OpenTelemetry` global meter.
pub mod metrics;
/// The cleanup runner: one job per enabled topic on its own worker pool.
pub mod runner;

use std::sync::Arc;

use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::spec::TableMetadata;
use icegate_common::{icegate_table_ident, resolve_committed_offset};
use icegate_queue::{DeleteLimits, QueueCleaner, Topic};
use jobmanager::{Error as JobError, TaskContext, TaskExecutor, TaskOutcome, TaskResult};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, info_span};

use crate::wal_cleanup::config::WalCleanupConfig;
use crate::wal_cleanup::metrics::WalCleanupMetrics;

/// Highest WAL offset that may be deleted, or `None` when nothing may be.
///
/// The bound is `committed - keep_segments_count`, and both terms are offsets:
/// cleanup reads no clock, of its own or of anyone else's.
///
/// * `committed` — the offset the table's current snapshot records. `None` means
///   nothing has been shifted into Iceberg yet, so every segment is still the
///   only copy of its data, and nothing may go.
/// * `keep_segments_count` — how far the bound stays behind that boundary, which
///   is the margin for readers that are behind it. With fewer segments committed
///   than the count protects, the answer is again `None`.
///
/// A query planned against a cached catalog provider reads the WAL from the
/// boundary THAT provider recorded, so it can still name a segment at or below
/// the current `committed`. The count is what keeps such a segment in place;
/// when it is not enough, the query FAILS rather than answering without those
/// rows (`icegate-query`'s `provider/table.rs`).
///
/// # Errors
///
/// Returns [`JobError`] when the committed offset cannot be resolved from the
/// snapshot chain. Fails CLOSED — nothing is deleted.
fn resolve_delete_bound(
    metadata: &TableMetadata,
    config: &WalCleanupConfig,
) -> std::result::Result<Option<u64>, JobError> {
    let committed = resolve_committed_offset(metadata)
        .map_err(|e| JobError::Other(format!("wal cleanup could not resolve the committed offset: {e}")))?;
    Ok(committed.and_then(|committed| committed.checked_sub(config.keep_segments_count)))
}

/// Whether a finished cycle is one an operator has to look at.
const fn is_cycle_stalled(dry_run: bool, summary: &icegate_queue::DeleteSummary) -> bool {
    if dry_run || summary.found == 0 {
        return false;
    }
    summary.deleted == 0 || summary.is_abandoned
}

/// Per-topic cleanup task executor.
pub struct WalCleanupExecutor {
    /// Catalog the topic's table is loaded from.
    catalog: Arc<dyn Catalog>,
    /// Cleaner addressing the WAL queue; shared by every topic's executor.
    cleaner: Arc<QueueCleaner>,
    /// WAL topic this executor cleans, in the form the cleaner addresses it by.
    topic: Topic,
    /// Iceberg table the topic shifts into.
    table: &'static str,
    /// Cleanup tunables.
    config: WalCleanupConfig,
    /// Instruments for this executor's cycles.
    metrics: WalCleanupMetrics,
}

impl WalCleanupExecutor {
    /// Build an executor for one topic.
    #[must_use]
    pub fn new(
        catalog: Arc<dyn Catalog>,
        cleaner: Arc<QueueCleaner>,
        topic: &str,
        table: &'static str,
        config: WalCleanupConfig,
        metrics: WalCleanupMetrics,
    ) -> Self {
        Self {
            catalog,
            cleaner,
            topic: topic.to_string(),
            table,
            config,
            metrics,
        }
    }

    /// Run one cleanup cycle for this topic.
    ///
    /// Derives the delete bound from the topic's table and deletes up to it,
    /// honouring `cancel` between delete batches. Both inputs are offsets — the
    /// cycle reads no clock at all (see [`resolve_delete_bound`]).
    ///
    /// # Errors
    ///
    /// Returns [`JobError`] if the table cannot be loaded, the delete bound
    /// cannot be derived, or the delete pass fails. All three fail CLOSED:
    /// nothing is deleted when the bound is not established.
    pub async fn clean_topic(&self, cancel: &CancellationToken) -> std::result::Result<TaskOutcome, JobError> {
        let start = std::time::Instant::now();
        let table = self
            .catalog
            .load_table(&icegate_table_ident(self.table))
            .await
            .map_err(|e| JobError::Other(format!("wal cleanup failed to load table '{}': {e}", self.table)))?;

        let Some(delete_bound) = resolve_delete_bound(table.metadata(), &self.config)? else {
            self.metrics.record_cycle_without_bound(&self.topic);
            tracing::info!(
                topic = %self.topic,
                table = self.table,
                keep_segments_count = self.config.keep_segments_count,
                "wal cleanup found no delete bound, deleting nothing: nothing has been shifted into \
                 the table yet, or fewer segments are committed than keep_segments_count protects"
            );
            return Ok(TaskOutcome::empty());
        };
        self.metrics.record_delete_bound(&self.topic, delete_bound);

        let limits = DeleteLimits {
            max_deletes: self.config.max_deletes_per_cycle,
            concurrency: self.config.delete_concurrency,
            dry_run: self.config.dry_run,
        };
        let summary = self
            .cleaner
            .delete_segments_upto(&self.topic, delete_bound, &limits, cancel)
            .await
            .map_err(|e| JobError::Other(format!("wal cleanup of topic '{}' failed: {e}", self.topic)))?;

        self.metrics.record_cycle(&self.topic, &summary);
        self.metrics.record_duration(&self.topic, start.elapsed().as_secs_f64());

        if summary.truncated {
            // Reported separately from the cycle line below: a truncated cycle
            // reclaimed a tail but did NOT reach the bound, and reading it as
            // "the tail is gone" is how a queue quietly keeps growing.
            tracing::warn!(
                topic = %self.topic,
                delete_bound,
                deleted = summary.deleted,
                max_deletes_per_cycle = self.config.max_deletes_per_cycle,
                lowest_remaining = ?summary.lowest_remaining,
                "wal cleanup hit its per-cycle delete budget, segments at or below the bound remain"
            );
        }
        // Not the same as the per-segment `warn!` the cleaner already emits:
        // that one fires on a single locked key the cycle steps over, this one
        // on a cycle that leaves the tail below the bound in place — see
        // [`is_cycle_stalled`].
        if is_cycle_stalled(self.config.dry_run, &summary) {
            self.metrics.record_stalled_cycle(&self.topic);
            tracing::error!(
                topic = %self.topic,
                table = self.table,
                delete_bound,
                found = summary.found,
                deleted = summary.deleted,
                failed = summary.failed,
                is_abandoned = summary.is_abandoned,
                "wal cleanup left the tail below the bound in place"
            );
        }
        tracing::info!(
            topic = %self.topic,
            table = self.table,
            delete_bound,
            found = summary.found,
            deleted = summary.deleted,
            failed = summary.failed,
            bytes_reclaimed = summary.bytes_reclaimed,
            dry_run = self.config.dry_run,
            "wal cleanup complete"
        );
        Ok(TaskOutcome::empty())
    }
}

#[async_trait]
impl TaskExecutor for WalCleanupExecutor {
    async fn execute(&self, ctx: TaskContext) -> TaskResult {
        let span = info_span!("wal_cleanup", topic = %self.topic);
        Ok(self.clean_topic(ctx.cancel_token()).instrument(span).await?)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{
        FormatVersion, NestedField, Operation, PrimitiveType, Schema, Snapshot, SortOrder, Summary, TableMetadata,
        TableMetadataBuilder, Type, UnboundPartitionSpec,
    };
    use icegate_common::WAL_OFFSET_PROPERTY;
    use icegate_queue::DeleteSummary;

    use super::{is_cycle_stalled, resolve_delete_bound};
    use crate::wal_cleanup::config::WalCleanupConfig;

    const TEST_LOCATION: &str = "s3://bucket/test/location";
    const MAIN_BRANCH: &str = "main";
    /// Timestamp of the oldest snapshot the helpers below build. Nothing here
    /// depends on it — the bound is derived from offsets alone — but a snapshot
    /// has to carry one.
    const BASE_TIMESTAMP_MS: i64 = 1_700_000_000_000;
    /// Segments the test configs keep below the committed offset.
    const KEEP_COUNT: u64 = 100;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "x", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("schema")
    }

    /// A snapshot at `timestamp_ms` carrying `offset`, chained to `parent`.
    fn snapshot(snapshot_id: i64, parent: Option<i64>, timestamp_ms: i64, offset: Option<u64>) -> Snapshot {
        let mut additional_properties = HashMap::new();
        if let Some(offset) = offset {
            additional_properties.insert(WAL_OFFSET_PROPERTY.to_string(), offset.to_string());
        }
        Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent)
            .with_sequence_number(snapshot_id)
            .with_timestamp_ms(timestamp_ms)
            .with_schema_id(0)
            .with_manifest_list(format!("/snap-{snapshot_id}.avro"))
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties,
            })
            .build()
    }

    fn metadata_with_chain(chain: Vec<Snapshot>) -> TableMetadata {
        let mut builder = TableMetadataBuilder::new(
            schema(),
            UnboundPartitionSpec::builder().with_spec_id(0).build(),
            SortOrder::unsorted_order(),
            TEST_LOCATION.to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder");
        for snap in chain {
            builder = builder.set_branch_snapshot(snap, MAIN_BRANCH).expect("branch snapshot");
        }
        builder.build().expect("metadata").metadata
    }

    /// `enabled: false` so the keep count can stay small enough to read the
    /// arithmetic off the assertions: the switch decides whether the runner
    /// registers jobs at all, never what a bound resolves to, and only an
    /// enabled block has to clear the floor.
    fn config() -> WalCleanupConfig {
        let config = WalCleanupConfig {
            enabled: false,
            keep_segments_count: KEEP_COUNT,
            ..WalCleanupConfig::default()
        };
        config.validate().expect("the test config must be one the binary loads");
        config
    }

    /// The rule: the bound trails the committed offset by `keep_segments_count`,
    /// and the committed offset is the one the CURRENT snapshot records.
    #[test]
    fn the_bound_trails_the_committed_offset_by_the_keep_count() {
        let metadata = metadata_with_chain(vec![
            snapshot(1, None, BASE_TIMESTAMP_MS, Some(1_000)),
            snapshot(2, Some(1), BASE_TIMESTAMP_MS + 1_000, Some(5_000)),
        ]);

        assert_eq!(
            resolve_delete_bound(&metadata, &config()).expect("ok"),
            Some(5_000 - KEEP_COUNT)
        );
    }

    /// A compaction `replace` snapshot carries no offset of its own, so the
    /// bound is resolved from its ancestor rather than read as "nothing
    /// committed".
    #[test]
    fn the_bound_resolves_through_a_compaction_snapshot() {
        let metadata = metadata_with_chain(vec![
            snapshot(1, None, BASE_TIMESTAMP_MS, Some(5_000)),
            snapshot(2, Some(1), BASE_TIMESTAMP_MS + 1_000, None),
        ]);

        assert_eq!(
            resolve_delete_bound(&metadata, &config()).expect("ok"),
            Some(5_000 - KEEP_COUNT)
        );
    }

    /// Fewer segments committed than the keep count protects: nothing may go.
    #[test]
    fn a_committed_offset_below_the_keep_count_has_no_bound() {
        let metadata = metadata_with_chain(vec![snapshot(1, None, BASE_TIMESTAMP_MS, Some(50))]);

        assert_eq!(resolve_delete_bound(&metadata, &config()).expect("ok"), None);
    }

    /// Exactly the keep count committed: the bound lands on offset 0, which is
    /// still a delete of segment 0 — the boundary case either side of the
    /// `checked_sub` above.
    #[test]
    fn a_committed_offset_equal_to_the_keep_count_bounds_at_zero() {
        let metadata = metadata_with_chain(vec![snapshot(1, None, BASE_TIMESTAMP_MS, Some(KEEP_COUNT))]);

        assert_eq!(resolve_delete_bound(&metadata, &config()).expect("ok"), Some(0));
    }

    /// A table nothing was ever shifted into: every segment is the only copy of
    /// its rows.
    #[test]
    fn a_table_without_snapshots_has_no_bound() {
        let metadata = metadata_with_chain(vec![]);

        assert_eq!(resolve_delete_bound(&metadata, &config()).expect("ok"), None);
    }

    /// THE EXPIRATION CASE: the ancestors were expired away, so the oldest
    /// surviving snapshot points at a parent the metadata no longer holds. The
    /// current snapshot still carries the offset, so the cycle resolves a bound
    /// as usual — retention of snapshots and retention of WAL segments are
    /// independent.
    #[test]
    fn a_chain_truncated_by_expiration_still_resolves_a_bound() {
        let metadata = metadata_with_chain(vec![
            snapshot(1, Some(999), BASE_TIMESTAMP_MS, Some(1_000)),
            snapshot(2, Some(1), BASE_TIMESTAMP_MS + 1_000, Some(5_000)),
        ]);

        assert_eq!(
            resolve_delete_bound(&metadata, &config()).expect("a truncated chain is not a failed cycle"),
            Some(5_000 - KEEP_COUNT)
        );
    }

    /// A table whose snapshots record no offset anywhere cannot be resolved, and
    /// a destructive operation fails CLOSED on that.
    #[test]
    fn an_unresolvable_committed_offset_fails_the_cycle() {
        let metadata = metadata_with_chain(vec![snapshot(1, None, BASE_TIMESTAMP_MS, None)]);

        assert!(resolve_delete_bound(&metadata, &config()).is_err());
    }

    /// Which finished cycles the operator is told about. The case the counters
    /// alone cannot express is the last one: a cleaner that reclaimed part of
    /// the tail and then met a store refusing the rest leaves the queue growing
    /// just as surely as one that reclaimed nothing.
    #[test]
    fn a_cycle_that_leaves_the_tail_in_place_is_stalled() {
        let healthy = DeleteSummary {
            found: 10,
            deleted: 10,
            ..DeleteSummary::default()
        };
        let reclaimed_nothing = DeleteSummary {
            found: 10,
            failed: 10,
            ..DeleteSummary::default()
        };
        let abandoned_after_reclaiming = DeleteSummary {
            found: 100,
            deleted: 8,
            failed: 16,
            is_abandoned: true,
            ..DeleteSummary::default()
        };
        let nothing_to_do = DeleteSummary::default();

        assert!(!is_cycle_stalled(false, &healthy));
        assert!(!is_cycle_stalled(false, &nothing_to_do));
        assert!(is_cycle_stalled(false, &reclaimed_nothing));
        assert!(is_cycle_stalled(false, &abandoned_after_reclaiming));

        assert!(
            !is_cycle_stalled(true, &reclaimed_nothing),
            "a dry run deletes nothing by definition"
        );
        assert!(
            !is_cycle_stalled(true, &abandoned_after_reclaiming),
            "and reaches no store to be refused by"
        );
    }
}
