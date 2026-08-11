//! Task timeout estimation utilities for shift operations.

use std::time::Duration;

use super::config::ShiftTimeoutsConfig;
use crate::error::IngestError;

/// Reject a timeout that the job manager cannot persist in task state.
pub(super) fn validate_timeout_ms(field_name: &str, timeout_ms: u64) -> Result<(), IngestError> {
    if timeout_ms == 0 {
        return Err(IngestError::Config(format!("{field_name} must be greater than zero")));
    }

    if chrono::Duration::from_std(Duration::from_millis(timeout_ms)).is_err() {
        return Err(IngestError::Config(format!(
            "{field_name} exceeds the jobmanager task timeout range"
        )));
    }

    Ok(())
}

#[allow(clippy::struct_field_names)]
#[derive(Clone)]
pub struct TimeoutEstimator {
    plan_base: Duration,
    shift_base: Duration,
    shift_per_record_batch: Duration,
    shift_per_segment: Duration,
    commit_base: Duration,
    commit_per_parquet_file: Duration,
}

impl TimeoutEstimator {
    pub(crate) const fn new(config: &ShiftTimeoutsConfig) -> Self {
        Self {
            plan_base: Duration::from_millis(config.plan_base_ms),
            shift_base: Duration::from_millis(config.shift_base_ms),
            shift_per_record_batch: Duration::from_millis(config.shift_per_record_batch_ms),
            shift_per_segment: Duration::from_millis(config.shift_per_segment_ms),
            commit_base: Duration::from_millis(config.commit_base_ms),
            commit_per_parquet_file: Duration::from_millis(config.commit_per_parquet_file_ms),
        }
    }

    pub(crate) const fn plan_timeout(&self) -> Duration {
        self.plan_base
    }

    pub(crate) fn shift_timeout(
        &self,
        segments_count: usize,
        record_batch_total: usize,
    ) -> Result<Duration, IngestError> {
        let estimate_ms = estimate_shift_timeout_ms(
            "shift",
            self.shift_base.as_millis(),
            self.shift_per_record_batch.as_millis(),
            self.shift_per_segment.as_millis(),
            segments_count,
            record_batch_total,
        )?;
        duration_from_ms(estimate_ms, "shift")
    }

    pub(crate) fn commit_timeout(&self, row_groups_total: usize) -> Result<Duration, IngestError> {
        let parquet_files = estimate_parquet_files(row_groups_total)?;
        let estimate_ms = estimate_commit_timeout_ms(
            "commit",
            self.commit_base.as_millis(),
            self.commit_per_parquet_file.as_millis(),
            parquet_files,
        )?;
        duration_from_ms(estimate_ms, "commit")
    }
}

fn estimate_shift_timeout_ms(
    label: &str,
    base_ms: u128,
    per_record_batch_ms: u128,
    per_segment_ms: u128,
    segments_count: usize,
    row_groups_total: usize,
) -> Result<u128, IngestError> {
    let segments_count = usize_to_u128(segments_count, label)?;
    let row_groups_total = usize_to_u128(row_groups_total, label)?;

    let mut total = base_ms;
    total = total
        .checked_add(
            per_record_batch_ms
                .checked_mul(row_groups_total)
                .ok_or_else(|| timeout_overflow(label))?,
        )
        .ok_or_else(|| timeout_overflow(label))?;
    total = total
        .checked_add(
            per_segment_ms
                .checked_mul(segments_count)
                .ok_or_else(|| timeout_overflow(label))?,
        )
        .ok_or_else(|| timeout_overflow(label))?;

    Ok(total)
}

fn estimate_commit_timeout_ms(
    label: &str,
    base_ms: u128,
    per_parquet_file_ms: u128,
    parquet_files: u64,
) -> Result<u128, IngestError> {
    let parquet_files = u128::from(parquet_files);
    let mut total = base_ms;
    total = total
        .checked_add(
            per_parquet_file_ms
                .checked_mul(parquet_files)
                .ok_or_else(|| timeout_overflow(label))?,
        )
        .ok_or_else(|| timeout_overflow(label))?;
    Ok(total)
}

fn estimate_parquet_files(row_groups_total: usize) -> Result<u64, IngestError> {
    let row_groups_total = usize_to_u64(row_groups_total, "commit")?;
    Ok(std::cmp::max(1, row_groups_total))
}

fn duration_from_ms(estimate_ms: u128, label: &str) -> Result<Duration, IngestError> {
    let estimate_ms =
        u64::try_from(estimate_ms).map_err(|_| IngestError::Config(format!("{label} timeout exceeds u64")))?;
    validate_timeout_ms(label, estimate_ms)?;
    Ok(Duration::from_millis(estimate_ms))
}

fn usize_to_u64(value: usize, label: &str) -> Result<u64, IngestError> {
    u64::try_from(value).map_err(|_| IngestError::Config(format!("{label} count exceeds u64")))
}

fn usize_to_u128(value: usize, label: &str) -> Result<u128, IngestError> {
    u128::try_from(value).map_err(|_| IngestError::Config(format!("{label} count exceeds u128")))
}

fn timeout_overflow(label: &str) -> IngestError {
    IngestError::Config(format!("{label} timeout calculation overflowed"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ShiftTimeoutsConfig {
        ShiftTimeoutsConfig {
            plan_base_ms: 1_000,
            shift_base_ms: 1_000,
            shift_per_record_batch_ms: 1_000,
            shift_per_segment_ms: 2_000,
            commit_base_ms: 1_000,
            commit_per_parquet_file_ms: 500,
        }
    }

    /// Highest per-item cost `ShiftTimeoutsConfig::validate` accepts, so an overflow case below
    /// starts from a configuration the process would really have started with.
    fn max_accepted_timeout_ms() -> u64 {
        u64::try_from(i64::MAX).expect("the jobmanager timeout bound fits u64")
    }

    /// Every estimate is its base plus the per-item costs of what the task was given, and a commit
    /// is billed for at least one parquet file. Expectations are written out rather than recomputed
    /// with the formula under test.
    #[test]
    fn timeout_estimates_follow_the_configured_base_and_per_item_costs() {
        let estimator = TimeoutEstimator::new(&test_config());

        assert_eq!(estimator.plan_timeout(), Duration::from_secs(1));

        for (segments_count, record_batch_total, expected_ms) in
            [(0, 0, 1_000), (1, 0, 3_000), (0, 1, 2_000), (3, 5, 12_000)]
        {
            assert_eq!(
                estimator
                    .shift_timeout(segments_count, record_batch_total)
                    .expect("shift timeout"),
                Duration::from_millis(expected_ms),
                "shift timeout for {segments_count} segments and {record_batch_total} record batches"
            );
        }

        for (row_groups_total, expected_ms) in [(0, 1_500), (1, 1_500), (3, 2_500)] {
            assert_eq!(
                estimator.commit_timeout(row_groups_total).expect("commit timeout"),
                Duration::from_millis(expected_ms),
                "commit timeout for {row_groups_total} row groups"
            );
        }
    }

    /// A per-item cost the configuration accepts still multiplies out past what a task deadline can
    /// hold. Failing the plan task is the only correct answer: a silently truncated estimate would
    /// hand the shift task a deadline shorter than the work it was planned for.
    #[test]
    fn shift_timeout_fails_when_the_estimate_exceeds_u64_milliseconds() {
        let config = ShiftTimeoutsConfig {
            shift_per_record_batch_ms: max_accepted_timeout_ms(),
            ..test_config()
        };
        config
            .validate()
            .expect("the per-record-batch cost must be one the configuration accepts");

        let err = TimeoutEstimator::new(&config)
            .shift_timeout(0, 3)
            .expect_err("an estimate above u64 milliseconds must fail the plan task");

        assert!(
            matches!(err, IngestError::Config(_)),
            "expected config error, got: {err}"
        );
    }

    /// Same rule on the commit side, where the per-item count is the number of parquet files the
    /// fan-out will produce.
    #[test]
    fn commit_timeout_fails_when_the_estimate_exceeds_u64_milliseconds() {
        let config = ShiftTimeoutsConfig {
            commit_per_parquet_file_ms: max_accepted_timeout_ms(),
            ..test_config()
        };
        config
            .validate()
            .expect("the per-parquet-file cost must be one the configuration accepts");

        let err = TimeoutEstimator::new(&config)
            .commit_timeout(3)
            .expect_err("an estimate above u64 milliseconds must fail the plan task");

        assert!(
            matches!(err, IngestError::Config(_)),
            "expected config error, got: {err}"
        );
    }
}
