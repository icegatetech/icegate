//! The orphan-file sweep: list, diff, and delete.
//!
//! Lists a table's object-store prefix, diffs against the referenced set, and
//! deletes unreferenced objects past the grace period.

use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures::StreamExt;
use iceberg::Catalog;
use icegate_common::{StorageConfig, create_object_store, icegate_table_ident};
use object_store::path::Path as ObjectPath;
use tokio_util::sync::CancellationToken;

use crate::error::MaintainError;
use crate::gc::config::GcOrphansConfig;
use crate::gc::decide::{Decision, ObjectClass, classify};
use crate::gc::metrics::GcMetrics;
use crate::gc::reachable::collect_referenced_paths;

/// Per-table outcome counters for one sweep.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepSummary {
    /// Objects listed under the table prefix.
    pub scanned: u64,
    /// Objects kept because they are referenced.
    pub referenced: u64,
    /// Unreferenced objects kept because they are inside the grace period.
    pub too_young: u64,
    /// Orphan data files identified.
    pub found_data: u64,
    /// Orphan metadata files identified.
    pub found_metadata: u64,
    /// Orphan objects successfully deleted.
    pub deleted: u64,
    /// Orphan objects whose deletion failed.
    pub delete_failed: u64,
    /// Bytes reclaimed by successful deletions.
    pub bytes_reclaimed: u64,
}

/// Run one orphan sweep for a single table.
///
/// Loads the table, builds the referenced set (failing closed on error), lists
/// the table's object-store prefix, and deletes unreferenced objects older than
/// `cfg.min_age_secs` relative to `now`. When `cfg.dry_run` is set, orphans are
/// counted and metered but not deleted.
///
/// # Errors
///
/// Returns an error if the table cannot be loaded, the referenced set cannot be
/// built (fail-closed: nothing is deleted), the object store cannot be built, a
/// list page fails, or the sweep is cancelled. Individual delete failures are
/// recorded in the summary, not propagated.
pub async fn run_sweep(
    catalog: &Arc<dyn Catalog>,
    storage: &StorageConfig,
    table: &str,
    cfg: &GcOrphansConfig,
    now: DateTime<Utc>,
    metrics: &GcMetrics,
    cancel: &CancellationToken,
) -> Result<SweepSummary, MaintainError> {
    let ident = icegate_table_ident(table);
    let loaded = catalog.load_table(&ident).await?;

    // FAIL-CLOSED: a partial referenced set must never drive a delete.
    let referenced = match collect_referenced_paths(&loaded).await {
        Ok(set) => set,
        Err(error) => {
            metrics.record_reference_set_build_failed(table);
            return Err(error);
        }
    };

    let location = loaded.metadata().location();
    let (store, prefix) = create_object_store(location, Some(&storage.backend), None, None, None, None)?;

    let grace = i64::try_from(cfg.min_age_secs)
        .map_err(|_| MaintainError::Config("gc.orphans.min_age_secs is too large".to_string()))?;
    let cutoff = now - ChronoDuration::seconds(grace);

    let list_prefix = ObjectPath::from(prefix.as_str());
    let mut stream = store.list(Some(&list_prefix));

    let mut summary = SweepSummary::default();
    // Hold only orphan paths (bounded by orphan count); the referenced set, the
    // larger structure, is the live set.
    let mut orphans: Vec<(ObjectPath, u64, ObjectClass)> = Vec::new();

    loop {
        // Race the list page against cancellation so a slow or stuck storage
        // call cannot keep shutdown blocked until the future resolves.
        let item = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(MaintainError::Storage(format!("gc sweep of table '{table}' cancelled")));
            }
            item = stream.next() => item,
        };
        let Some(item) = item else { break };
        let meta = item.map_err(|e| MaintainError::Storage(format!("gc list error for table '{table}': {e}")))?;
        summary.scanned += 1;
        // LIST keys are already bucket-relative canonical keys (unlike the full
        // URIs in reachable.rs), so they compare directly against the referenced
        // set; see decide::canonicalize_key.
        match classify(
            meta.location.as_ref(),
            &prefix,
            &referenced,
            meta.last_modified,
            cutoff,
            cfg.include_metadata,
        ) {
            Decision::Referenced => summary.referenced += 1,
            Decision::TooYoung => summary.too_young += 1,
            Decision::SkipMetadataDisabled | Decision::SkipUnknownLayout => {}
            Decision::Delete(class) => {
                match class {
                    ObjectClass::Data => summary.found_data += 1,
                    ObjectClass::Metadata => summary.found_metadata += 1,
                }
                metrics.record_orphan_found(table, class);
                // A dry run returns before deletion, so retaining the path would
                // only grow memory across a large backlog scan without use.
                if !cfg.dry_run {
                    orphans.push((meta.location, meta.size, class));
                }
            }
        }
    }

    if cfg.dry_run {
        metrics.record_scan(table, summary.scanned, summary.too_young);
        return Ok(summary);
    }

    let mut deletions = futures::stream::iter(orphans.into_iter().map(|(path, size, class)| {
        let store = Arc::clone(&store);
        async move {
            match store.delete(&path).await {
                Ok(()) => Ok((size, class)),
                Err(error) => Err((path, class, error)),
            }
        }
    }))
    .buffer_unordered(cfg.delete_concurrency.max(1));

    loop {
        // Keep the delete drain cancellation-aware too, so shutdown short-circuits
        // promptly instead of waiting on the next in-flight deletion.
        let result = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(MaintainError::Storage(format!("gc sweep of table '{table}' cancelled")));
            }
            result = deletions.next() => result,
        };
        let Some(result) = result else { break };
        match result {
            Ok((size, class)) => {
                summary.deleted += 1;
                summary.bytes_reclaimed += size;
                metrics.record_orphan_deleted(table, class, size);
            }
            Err((path, class, error)) => {
                summary.delete_failed += 1;
                metrics.record_orphan_delete_failed(table, class);
                tracing::warn!(table, path = %path, kind = ?class, error = %error, "failed to delete orphan object");
            }
        }
    }

    metrics.record_scan(table, summary.scanned, summary.too_young);
    Ok(summary)
}
