//! Configuration for WAL segment cleanup.

use serde::{Deserialize, Serialize};

use crate::error::MaintainError;
use crate::jobs::{JobsManagerConfig, JobsStorageConfig};

/// Config block the cleanup tunables are read from, used in validation
/// messages. It is the `wal_cleanup` field name of `MaintainConfig`, which is
/// the key the chart and Compose render.
pub(crate) const WAL_CLEANUP_CONFIG_BLOCK: &str = "wal_cleanup";

/// Floor on [`WalCleanupConfig::keep_segments_count`] while cleanup is enabled.
///
/// The count is the whole safety margin cleanup has: it holds the delete bound
/// this many segments below the boundary the Shifter last recorded, which is
/// where the readers that are behind — above all a query planned against a
/// cached catalog provider — still are. It is a margin in SEGMENTS against a
/// window in TIME, so the value that covers the window is
///
/// ```text
/// keep_segments_count >= (max_age_secs + max_query_duration_secs) * segments_per_second
/// ```
///
/// where the two terms of the window are query's
/// (`icegate-query/src/engine/config.rs`) and the rate ceiling is one segment
/// per flush tick of the queue writer (`icegate-queue/src/writer.rs`). This
/// floor is that product at the values both ship today; the arithmetic, and what
/// an operator does when their own values differ, is in
/// [`README.md`](./README.md). Below the window a query does not answer from
/// what is left — it fails, because the rows in the gap are in neither the WAL
/// nor the snapshot the stale provider reads.
///
/// Applied only while cleanup is enabled: a disabled block deletes nothing, so
/// an operator staging a value for a later rollout is not blocked by it.
const MIN_KEEP_SEGMENTS_COUNT: u64 = 1_200;

/// Configuration for deleting WAL segments already shifted into Iceberg.
// Six independent switches, four of them per-topic toggles mirroring `GcConfig`;
// none is a state machine that would read better as an enum.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalCleanupConfig {
    /// Master switch: when `false`, the `run` service starts no cleanup runner
    /// at all (so an upgrade never silently begins deleting).
    pub enabled: bool,
    /// When `true`, compute and report what would be deleted but delete nothing.
    pub dry_run: bool,
    /// Whether cleanup runs for the `logs` topic.
    pub logs_enabled: bool,
    /// Whether cleanup runs for the `spans` topic.
    pub spans_enabled: bool,
    /// Whether cleanup runs for the `metrics` topic.
    pub metrics_enabled: bool,
    /// Whether cleanup runs for the `operations` topic.
    pub operations_enabled: bool,
    /// Number of segments below the committed offset that are never deleted.
    ///
    /// The single retention knob: the delete bound is `committed -
    /// keep_segments_count`, so this is how far cleanup stays behind the
    /// boundary the Shifter last recorded, and it is what a reader that is
    /// behind — above all a query planned against a cached catalog provider —
    /// has to fit inside. Sizing it against the ingest rate, and the floor
    /// enforced while cleanup is enabled: [`MIN_KEEP_SEGMENTS_COUNT`].
    pub keep_segments_count: u64,
    /// Maximum number of segments one cleanup cycle may delete per topic.
    pub max_deletes_per_cycle: usize,
    /// Maximum number of segment deletions in flight at once.
    pub delete_concurrency: usize,
    /// Per-topic cleanup task deadline, in seconds.
    pub cleanup_timeout_secs: u64,
    /// Worker pool, cleanup cadence, and job-state storage of the cleanup
    /// runner. The `Default` below sets a distinct `"wal_cleanup"` storage
    /// prefix, so cleanup's job state never collides with another service's.
    ///
    /// The cadence of a cleanup cycle is this block's `scan_interval_secs`, the
    /// same knob compaction, gc, and pricing take it from.
    pub jobsmanager: JobsManagerConfig,
}

impl Default for WalCleanupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dry_run: false,
            logs_enabled: true,
            spans_enabled: true,
            metrics_enabled: true,
            operations_enabled: true,
            keep_segments_count: MIN_KEEP_SEGMENTS_COUNT,
            max_deletes_per_cycle: 50_000,
            delete_concurrency: 16,
            cleanup_timeout_secs: 600,
            jobsmanager: JobsManagerConfig {
                scan_interval_secs: 600,
                storage: JobsStorageConfig {
                    prefix: "wal_cleanup".to_string(),
                    ..JobsStorageConfig::default()
                },
                ..JobsManagerConfig::default()
            },
        }
    }
}

impl WalCleanupConfig {
    /// Validate the cleanup tunables.
    ///
    /// The [`jobsmanager`](Self::jobsmanager) block is deliberately NOT checked
    /// here. This method runs on every config load, including the one-shot
    /// `migrate` commands, which share `MaintainConfig` and carry no
    /// `wal_cleanup` block at all — the default job-state storage has an empty
    /// endpoint and bucket, so validating it here would fail those commands on a
    /// service they never start. `WalCleanupRunner::new` validates it instead,
    /// at the one point where the pool is actually built.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] when `keep_segments_count`,
    /// `max_deletes_per_cycle`, `delete_concurrency` or `cleanup_timeout_secs`
    /// is zero, or when cleanup is enabled with `keep_segments_count` below
    /// [`MIN_KEEP_SEGMENTS_COUNT`].
    pub fn validate(&self) -> Result<(), MaintainError> {
        if self.keep_segments_count == 0 {
            return Err(MaintainError::Config(
                "wal_cleanup.keep_segments_count must be greater than zero".to_string(),
            ));
        }
        if self.max_deletes_per_cycle == 0 {
            return Err(MaintainError::Config(
                "wal_cleanup.max_deletes_per_cycle must be greater than zero".to_string(),
            ));
        }
        if self.delete_concurrency == 0 {
            return Err(MaintainError::Config(
                "wal_cleanup.delete_concurrency must be greater than zero".to_string(),
            ));
        }
        if self.cleanup_timeout_secs == 0 {
            return Err(MaintainError::Config(
                "wal_cleanup.cleanup_timeout_secs must be greater than zero".to_string(),
            ));
        }
        if self.enabled && self.keep_segments_count < MIN_KEEP_SEGMENTS_COUNT {
            return Err(MaintainError::Config(format!(
                "wal_cleanup.keep_segments_count must be at least {MIN_KEEP_SEGMENTS_COUNT} while cleanup \
                 is enabled: it is the only margin cleanup keeps below the committed offset, it has to \
                 cover the segments a query planned on a cached catalog provider still reads, and the \
                 segments it holds cost far less than queries failing on a queue tail that is gone"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{MIN_KEEP_SEGMENTS_COUNT, WAL_CLEANUP_CONFIG_BLOCK, WalCleanupConfig};

    #[test]
    fn defaults_are_off_with_the_floor_of_segments_kept() {
        let config = WalCleanupConfig::default();
        assert!(!config.enabled);
        assert!(!config.dry_run);
        assert!(config.logs_enabled);
        assert!(config.spans_enabled);
        assert!(config.metrics_enabled);
        assert!(config.operations_enabled);
        assert_eq!(config.keep_segments_count, MIN_KEEP_SEGMENTS_COUNT);
        assert_eq!(config.max_deletes_per_cycle, 50_000);
        assert_eq!(config.delete_concurrency, 16);
        assert_eq!(config.cleanup_timeout_secs, 600);
        assert_eq!(config.jobsmanager.scan_interval_secs, 600);
        assert_eq!(config.jobsmanager.storage.prefix, "wal_cleanup");
        config.validate().expect("the defaults validate");
    }

    /// The count floor: rejected while enabled, accepted while disabled (an
    /// operator may stage a value for a later rollout), and the floor value
    /// itself is valid.
    #[test]
    fn the_keep_count_floor_applies_only_while_cleanup_is_enabled() {
        let below_floor = WalCleanupConfig {
            enabled: true,
            keep_segments_count: MIN_KEEP_SEGMENTS_COUNT - 1,
            ..WalCleanupConfig::default()
        };
        assert!(below_floor.validate().is_err());

        let staged = WalCleanupConfig {
            enabled: false,
            ..below_floor
        };
        staged.validate().expect("a disabled block deletes nothing");

        let at_floor = WalCleanupConfig {
            enabled: true,
            keep_segments_count: MIN_KEEP_SEGMENTS_COUNT,
            ..WalCleanupConfig::default()
        };
        at_floor.validate().expect("the floor itself is a valid setting");
    }

    #[test]
    fn zero_limits_are_rejected() {
        let cases: [(&str, WalCleanupConfig); 4] = [
            (
                "keep_segments_count",
                WalCleanupConfig {
                    keep_segments_count: 0,
                    ..WalCleanupConfig::default()
                },
            ),
            (
                "max_deletes_per_cycle",
                WalCleanupConfig {
                    max_deletes_per_cycle: 0,
                    ..WalCleanupConfig::default()
                },
            ),
            (
                "delete_concurrency",
                WalCleanupConfig {
                    delete_concurrency: 0,
                    ..WalCleanupConfig::default()
                },
            ),
            (
                "cleanup_timeout_secs",
                WalCleanupConfig {
                    cleanup_timeout_secs: 0,
                    ..WalCleanupConfig::default()
                },
            ),
        ];

        for (field, config) in cases {
            assert!(config.validate().is_err(), "{field} of zero must be rejected");
        }
    }

    /// The job-state storage is the runner's business, not the loader's: a
    /// `migrate` config carries no `wal_cleanup` block, so the default empty
    /// storage reaches this validator on every one of those commands and must pass.
    /// `WalCleanupRunner::new` is where the same block is rejected.
    #[test]
    fn validate_leaves_the_job_state_storage_to_the_runner() {
        let config = WalCleanupConfig::default();
        assert!(config.jobsmanager.storage.endpoint.is_empty());

        config.validate().expect("an unset job-state storage must still load");
        assert!(
            config.jobsmanager.validate(WAL_CLEANUP_CONFIG_BLOCK).is_err(),
            "the runner's own check must be the one that rejects it"
        );
    }
}
