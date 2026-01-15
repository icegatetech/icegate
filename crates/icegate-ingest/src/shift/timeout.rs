//! Task timeout estimation utilities for shift operations.

use chrono::Duration as ChronoDuration;

use super::config::ShiftTimeoutsConfig;
use crate::error::IngestError;

#[allow(clippy::struct_field_names)]
pub struct TimeoutEstimator {
    plan_base: ChronoDuration,
    shift_base: ChronoDuration,
    shift_per_record_batch: ChronoDuration,
    shift_per_segment: ChronoDuration,
    commit_base: ChronoDuration,
    commit_per_parquet_file: ChronoDuration,
}

impl TimeoutEstimator {
    pub(crate) fn new(config: &ShiftTimeoutsConfig) -> Result<Self, IngestError> {
        Ok(Self {
            plan_base: duration_from_u64(config.plan_base_ms, "plan_base_ms")?,
            shift_base: duration_from_u64(config.shift_base_ms, "shift_base_ms")?,
            shift_per_record_batch: duration_from_u64(config.shift_per_record_batch_ms, "shift_per_record_batch_ms")?,
            shift_per_segment: duration_from_u64(config.shift_per_segment_ms, "shift_per_segment_ms")?,
            commit_base: duration_from_u64(config.commit_base_ms, "commit_base_ms")?,
            commit_per_parquet_file: duration_from_u64(
                config.commit_per_parquet_file_ms,
                "commit_per_parquet_file_ms",
            )?,
        })
    }

    pub(crate) const fn plan_timeout(&self) -> ChronoDuration {
        self.plan_base
    }

    pub(crate) fn shift_timeout(
        &self,
        segments_count: usize,
        record_batch_total: usize,
    ) -> Result<ChronoDuration, IngestError> {
        let estimate_ms = estimate_shift_timeout_ms(
            "shift",
            duration_to_ms(self.shift_base),
            duration_to_ms(self.shift_per_record_batch),
            duration_to_ms(self.shift_per_segment),
            segments_count,
            record_batch_total,
        )?;
        duration_from_ms(estimate_ms, "shift")
    }

    pub(crate) fn commit_timeout(&self, row_groups_total: usize) -> Result<ChronoDuration, IngestError> {
        let parquet_files = estimate_parquet_files(row_groups_total)?;
        let estimate_ms = estimate_commit_timeout_ms(
            "commit",
            duration_to_ms(self.commit_base),
            duration_to_ms(self.commit_per_parquet_file),
            parquet_files,
        )?;
        duration_from_ms(estimate_ms, "commit")
    }
}

fn estimate_shift_timeout_ms(
    label: &str,
    base_ms: i128,
    per_record_batch_ms: i128,
    per_segment_ms: i128,
    segments_count: usize,
    row_groups_total: usize,
) -> Result<i128, IngestError> {
    let segments_count = usize_to_i128(segments_count, label)?;
    let row_groups_total = usize_to_i128(row_groups_total, label)?;

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
    base_ms: i128,
    per_parquet_file_ms: i128,
    parquet_files: u64,
) -> Result<i128, IngestError> {
    let parquet_files = i128::from(parquet_files);
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

fn duration_from_ms(estimate_ms: i128, label: &str) -> Result<ChronoDuration, IngestError> {
    let estimate_ms =
        i64::try_from(estimate_ms).map_err(|_| IngestError::Config(format!("{label} timeout exceeds i64")))?;
    Ok(ChronoDuration::milliseconds(estimate_ms))
}

fn duration_from_u64(value: u64, label: &str) -> Result<ChronoDuration, IngestError> {
    let value = i64::try_from(value).map_err(|_| IngestError::Config(format!("{label} exceeds i64 milliseconds")))?;
    Ok(ChronoDuration::milliseconds(value))
}

fn duration_to_ms(duration: ChronoDuration) -> i128 {
    i128::from(duration.num_milliseconds())
}

fn usize_to_u64(value: usize, label: &str) -> Result<u64, IngestError> {
    u64::try_from(value).map_err(|_| IngestError::Config(format!("{label} count exceeds u64")))
}

fn usize_to_i128(value: usize, label: &str) -> Result<i128, IngestError> {
    i128::try_from(value).map_err(|_| IngestError::Config(format!("{label} count exceeds i128")))
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

    #[test]
    fn plan_timeout_uses_base() {
        let estimator = TimeoutEstimator::new(&test_config()).expect("estimator");
        let timeout = estimator.plan_timeout();
        assert_eq!(timeout, ChronoDuration::milliseconds(1_000));
    }

    #[test]
    fn shift_timeout() {
        let estimator = TimeoutEstimator::new(&test_config()).expect("estimator");
        let timeout = estimator.shift_timeout(3, 5).expect("shift timeout");
        assert_eq!(timeout, ChronoDuration::milliseconds(12_000));
    }

    #[test]
    fn commit_timeout_uses_parquet_file_estimate() {
        let estimator = TimeoutEstimator::new(&test_config()).expect("estimator");
        let timeout = estimator.commit_timeout(3).expect("commit timeout");
        assert_eq!(timeout, ChronoDuration::milliseconds(2_500));
    }

    #[test]
    fn commit_timeout_uses_min_parquet_files() {
        let estimator = TimeoutEstimator::new(&test_config()).expect("estimator");
        let timeout = estimator.commit_timeout(0).expect("commit timeout");
        assert_eq!(timeout, ChronoDuration::milliseconds(1_500));
    }
}
