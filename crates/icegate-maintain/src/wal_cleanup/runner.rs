//! The WAL-cleanup runner: one job per enabled topic, on its own worker pool.
//!
//! Cleanup runs on a pool of its own rather than sharing the orphan sweep's, so
//! a sweep holding a worker for up to `gc.orphans.sweep_timeout_secs` cannot
//! stand between two cleanup cycles. An idle pool is close to free — a worker
//! that has nothing due does not reach the job-state store — while a pool shared
//! with a daily sweep turns a ten-minute cleanup interval into a lower bound
//! nobody can predict.

use std::sync::Arc;
use std::time::Duration;

use iceberg::Catalog;
use icegate_common::{
    LOGS_TABLE, LOGS_TOPIC, METRICS_TABLE, METRICS_TOPIC, OPERATIONS_TABLE, OPERATIONS_TOPIC, SPANS_TABLE, SPANS_TOPIC,
};
use icegate_queue::QueueCleaner;
use jobmanager::{JobsManager, JobsManagerHandle, OtelMetrics, TaskCode, TaskDefinition};

use crate::error::{MaintainError, Result};
use crate::wal_cleanup::WalCleanupExecutor;
use crate::wal_cleanup::config::{WAL_CLEANUP_CONFIG_BLOCK, WalCleanupConfig};
use crate::wal_cleanup::metrics::WalCleanupMetrics;

/// Task code for the single per-topic cleanup task.
pub const CLEAN_WAL_TASK_CODE: &str = "clean_wal";

/// Static identity of one topic's cleanup job.
#[derive(Clone, Copy)]
struct WalTopicSpec {
    /// Stable job name used in the registry and logs (e.g. `clean_wal_logs`).
    job_name: &'static str,
    /// WAL topic to clean.
    topic: &'static str,
    /// Iceberg table the topic shifts into; its snapshots carry the offsets the
    /// delete bound is derived from.
    table: &'static str,
}

/// Build the per-topic specs for the enabled WAL topics.
///
/// There is no `events` entry: `events` is an Iceberg table with no WAL topic of
/// its own (the four topics are declared in `icegate_common`), so it is swept for
/// orphans but has nothing to clean up.
fn enabled_wal_topics(config: &WalCleanupConfig) -> Vec<WalTopicSpec> {
    let mut specs = Vec::new();
    if config.logs_enabled {
        specs.push(WalTopicSpec {
            job_name: "clean_wal_logs",
            topic: LOGS_TOPIC,
            table: LOGS_TABLE,
        });
    }
    if config.spans_enabled {
        specs.push(WalTopicSpec {
            job_name: "clean_wal_spans",
            topic: SPANS_TOPIC,
            table: SPANS_TABLE,
        });
    }
    if config.metrics_enabled {
        specs.push(WalTopicSpec {
            job_name: "clean_wal_metrics",
            topic: METRICS_TOPIC,
            table: METRICS_TABLE,
        });
    }
    if config.operations_enabled {
        specs.push(WalTopicSpec {
            job_name: "clean_wal_operations",
            topic: OPERATIONS_TOPIC,
            table: OPERATIONS_TABLE,
        });
    }
    specs
}

/// Runs WAL segment cleanup inside the maintain process.
pub struct WalCleanupRunner {
    manager: JobsManager,
}

/// Handle for draining a running [`WalCleanupRunner`].
pub struct WalCleanupRunnerHandle {
    handle: JobsManagerHandle,
}

impl WalCleanupRunner {
    /// Build a runner with one cleanup job per enabled WAL topic.
    ///
    /// `cleaner` addresses the queue every topic's job deletes from; the caller
    /// resolves it from `queue.common.base_path` before any job exists, so a
    /// misconfigured queue location is a failed startup rather than a job that
    /// fails every cycle.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if the cleanup config is invalid, its job-state
    /// storage is unusable (this is the one place that block is validated — see
    /// [`WalCleanupConfig::validate`]), no topic is enabled, or the jobmanager
    /// storage cannot be constructed.
    pub async fn new(catalog: Arc<dyn Catalog>, cleaner: Arc<QueueCleaner>, config: WalCleanupConfig) -> Result<Self> {
        Self::new_with_max_iterations(catalog, cleaner, config, None).await
    }

    /// Test seam: like [`Self::new`] but caps each job at `max_iterations`
    /// discovery cycles.
    ///
    /// # Errors
    ///
    /// See [`Self::new`].
    #[doc(hidden)]
    pub async fn new_with_max_iterations(
        catalog: Arc<dyn Catalog>,
        cleaner: Arc<QueueCleaner>,
        config: WalCleanupConfig,
        max_iterations: Option<u64>,
    ) -> Result<Self> {
        config.validate()?;
        config.jobsmanager.validate(WAL_CLEANUP_CONFIG_BLOCK)?;
        let specs = enabled_wal_topics(&config);
        if specs.is_empty() {
            return Err(MaintainError::Config(
                "wal cleanup is enabled but no topic is: enable at least one of \
                 wal_cleanup.{logs,spans,metrics,operations}_enabled"
                    .to_string(),
            ));
        }

        let interval = Duration::from_secs(config.jobsmanager.scan_interval_secs);
        let timeout = Duration::from_secs(config.cleanup_timeout_secs);
        let metrics = WalCleanupMetrics::new();

        // Enable the jobmanager's own metrics (job/task durations and statuses,
        // task-steal events, optimistic-concurrency save retries, and job-state
        // storage S3 latency / cache hits), mirroring the other runners: binds to
        // the global meter installed by `MetricsRuntime`; inert when metrics are
        // disabled (no provider set).
        let mut builder = JobsManager::builder()
            .s3(config.jobsmanager.storage.to_s3_storage_config(WAL_CLEANUP_CONFIG_BLOCK)?)
            .workers(config.jobsmanager.worker_count)
            .poll_interval(Duration::from_millis(config.jobsmanager.poll_interval_ms))
            .metrics(Arc::new(OtelMetrics::new(&opentelemetry::global::meter(
                "icegate-maintain",
            ))));

        for spec in specs {
            let executor = Arc::new(WalCleanupExecutor::new(
                Arc::clone(&catalog),
                Arc::clone(&cleaner),
                spec.topic,
                spec.table,
                config.clone(),
                metrics.clone(),
            ));
            builder = builder.job(spec.job_name, move |job| {
                job.every(interval);
                if let Some(max) = max_iterations {
                    job.max_iterations(max);
                }
                job.add_task(
                    TaskDefinition::new(TaskCode::new(CLEAN_WAL_TASK_CODE), timeout),
                    executor,
                );
            });
        }

        let manager = builder.build().await.map_err(map_job_error)?;
        Ok(Self { manager })
    }

    /// Start the cleanup workers. Returns a handle that drains them on shutdown.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if the jobmanager fails to start.
    pub fn start(&self) -> Result<WalCleanupRunnerHandle> {
        let handle = self.manager.start().map_err(map_job_error)?;
        Ok(WalCleanupRunnerHandle { handle })
    }
}

impl WalCleanupRunnerHandle {
    /// Cancel the workers and wait for them to drain.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError`] if a worker stopped with an error.
    pub async fn shutdown(self) -> Result<()> {
        self.handle.shutdown().await.map_err(map_job_error)
    }
}

/// Map any [`Display`](std::fmt::Display) error (jobmanager constructors,
/// duration overflow) into [`MaintainError::Config`].
fn map_job_error<E: std::fmt::Display>(error: E) -> MaintainError {
    MaintainError::Config(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::enabled_wal_topics;
    use crate::wal_cleanup::config::WalCleanupConfig;

    /// The four WAL topics, and only those: `events` is a table without a WAL
    /// topic, so a fifth job here would clean a prefix that never exists.
    #[test]
    fn every_enabled_topic_gets_a_job() {
        let specs = enabled_wal_topics(&WalCleanupConfig::default());

        let topics: Vec<&str> = specs.iter().map(|spec| spec.topic).collect();
        assert_eq!(topics, vec!["logs", "spans", "metrics", "operations"]);
    }

    /// A per-topic toggle is what an operator uses to roll cleanup out one topic
    /// at a time, so a disabled one must register no job at all.
    #[test]
    fn a_disabled_topic_gets_no_job() {
        let config = WalCleanupConfig {
            logs_enabled: false,
            operations_enabled: false,
            ..WalCleanupConfig::default()
        };

        let specs = enabled_wal_topics(&config);

        let topics: Vec<&str> = specs.iter().map(|spec| spec.topic).collect();
        assert_eq!(topics, vec!["spans", "metrics"]);
    }
}
