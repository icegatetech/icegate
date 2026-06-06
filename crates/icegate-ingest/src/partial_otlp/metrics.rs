//! Helpers for composing OTLP metrics partial-success responses.
//!
//! Transform-time drops (strict OTLP-conformance rejections) and WAL-side
//! partial failures both contribute to `rejectedDataPoints`. The HTTP and gRPC
//! handlers share these so the two protocols stay in lockstep; the WAL-write
//! orchestration that consumes them lives in [`crate::wal`].

use crate::error::IngestError;
use crate::infra::metrics::OtlpRequestRecorder;

/// Message reported when data points are dropped at transform time for failing
/// strict OTLP validation (unset value, unspecified aggregation temporality,
/// out-of-range count/flags, non-finite value, inconsistent histogram buckets,
/// quantile outside [0, 1], or out-of-range exponential-histogram scale).
pub const INVALID_METRIC_MSG: &str = concat!(
    "data point rejected: unset value, unspecified aggregation temporality, ",
    "out-of-range count/flags, non-finite value, inconsistent histogram buckets, ",
    "quantile outside [0, 1], or out-of-range exponential-histogram scale"
);

/// Noun used in metrics partial-success error messages.
const COUNT_NOUN: &str = "data points";
/// Noun used in the combined transform/WAL partial-success message.
const DROP_NOUN: &str = "data point(s)";

/// Narrow a `usize` drop count to `i64` for OTLP partial-success reporting.
///
/// Returns `Ok(None)` when `drops == 0` so callers can skip emitting a
/// `partial_success` payload. Returns `Err` only on the unreachable i64
/// overflow path.
pub fn rejected_data_points_from_drops(drops: usize) -> Result<Option<i64>, IngestError> {
    super::rejected_from_drops(drops, COUNT_NOUN)
}

/// Finish request metrics with status "partial" when there are transform-time
/// drops, otherwise "ok".
pub fn finish_metrics_with_drops(request_metrics: &OtlpRequestRecorder, drops: usize) {
    super::finish_with_drops(request_metrics, drops);
}

/// Compose a combined error message when both transform-time drops and a
/// WAL-side partial failure contributed to rejections.
///
/// When `drops == 0` the WAL reason is returned verbatim.
pub fn compose_partial_reason(wal_reason: &str, drops: usize) -> String {
    super::compose_partial_reason(wal_reason, drops, DROP_NOUN, INVALID_METRIC_MSG)
}

/// Partial-success payload from transform-time drops alone: `Some((rejected,
/// message))` when `drops > 0`, otherwise `None`.
pub(crate) fn partial_from_drops(drops: usize) -> Result<Option<(i64, String)>, IngestError> {
    Ok(rejected_data_points_from_drops(drops)?.map(|rejected| (rejected, INVALID_METRIC_MSG.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_data_points_from_drops_zero_returns_none() {
        assert_eq!(rejected_data_points_from_drops(0).expect("ok"), None);
    }

    #[test]
    fn rejected_data_points_from_drops_small_value_passes_through() {
        assert_eq!(rejected_data_points_from_drops(5).expect("ok"), Some(5));
    }

    #[test]
    fn compose_partial_reason_without_drops_returns_wal_verbatim() {
        assert_eq!(compose_partial_reason("wal-err", 0), "wal-err".to_string());
    }

    #[test]
    fn compose_partial_reason_with_drops_combines_causes() {
        let msg = compose_partial_reason("wal-err", 3);
        assert!(msg.starts_with("wal-err"));
        assert!(msg.contains("3 data point(s)"));
        assert!(msg.contains(INVALID_METRIC_MSG));
    }
}
