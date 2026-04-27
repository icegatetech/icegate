//! Shared helpers for composing OTLP traces partial-success responses.
//!
//! Transform-time drops (invalid `trace_id` / `span_id`) and WAL-side
//! partial failures both contribute to `rejected_spans`. The HTTP and gRPC
//! handlers share the conversion and message formatting so the two
//! protocols stay in lockstep.

use crate::error::IngestError;
use crate::infra::metrics::OtlpRequestRecorder;

/// Message reported when spans are dropped at transform time because their
/// `trace_id` or `span_id` failed validation.
pub const INVALID_TRACE_MSG: &str = "invalid trace_id or span_id";

/// Narrow a `usize` drop count to `i64` for OTLP partial-success reporting.
///
/// Returns `Ok(None)` when `drops == 0` so callers can skip emitting a
/// `partial_success` payload entirely. Returns `Err` only on the
/// mathematically-unreachable i64 overflow path (a 64-bit platform with
/// more than `i64::MAX` dropped spans), surfacing the cause to the caller.
pub fn rejected_spans_from_drops(drops: usize) -> Result<Option<i64>, IngestError> {
    if drops == 0 {
        return Ok(None);
    }
    i64::try_from(drops)
        .map(Some)
        .map_err(|_| IngestError::Validation("Rejected spans count exceeds i64".to_string()))
}

/// Finish request metrics with status "partial" when there are transform-time
/// drops, otherwise with status "ok".
///
/// Shared by the HTTP and gRPC trace handlers: both report partial success
/// on any drop count > 0 so Grafana panels for `status="partial"` reflect
/// every path where some spans failed validation.
pub fn finish_metrics_with_drops(request_metrics: &OtlpRequestRecorder, drops: usize) {
    if drops > 0 {
        request_metrics.finish_partial();
    } else {
        request_metrics.finish_ok();
    }
}

/// Compose a combined error message when both transform-time drops and a
/// WAL-side partial failure contributed to rejections.
///
/// * `wal_reason` — reason reported by the WAL writer.
/// * `drops` — spans dropped at transform time (invalid IDs).
///
/// When `drops == 0` the WAL reason is returned verbatim so existing
/// WAL-only failures keep their original wording.
pub fn compose_partial_reason(wal_reason: &str, drops: usize) -> String {
    if drops == 0 {
        return wal_reason.to_string();
    }
    format!("{wal_reason}; also {drops} span(s) rejected at transform: {INVALID_TRACE_MSG}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejected_spans_from_drops_zero_returns_none() {
        assert_eq!(rejected_spans_from_drops(0).expect("ok"), None);
    }

    #[test]
    fn rejected_spans_from_drops_small_value_passes_through() {
        assert_eq!(rejected_spans_from_drops(5).expect("ok"), Some(5));
    }

    #[test]
    fn compose_partial_reason_without_drops_returns_wal_verbatim() {
        assert_eq!(compose_partial_reason("wal-err", 0), "wal-err".to_string());
    }

    #[test]
    fn compose_partial_reason_with_drops_combines_causes() {
        let msg = compose_partial_reason("wal-err", 3);
        assert!(msg.starts_with("wal-err"));
        assert!(msg.contains("3 span(s)"));
        assert!(msg.contains(INVALID_TRACE_MSG));
    }
}
