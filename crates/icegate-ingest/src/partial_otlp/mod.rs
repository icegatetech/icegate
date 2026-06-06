//! Signal-agnostic helpers for composing OTLP partial-success responses.
//!
//! Transform-time drops (strict-conformance rejections) and WAL-side partial
//! failures both contribute to the per-signal `rejected_*` count. The per-signal
//! submodules ([`metrics`], [`traces`]) wrap these primitives with their own
//! message constants and nouns so the HTTP and gRPC handlers stay in lockstep
//! across signals. The WAL-write orchestration that consumes these helpers lives
//! in [`crate::wal`].

pub(crate) mod metrics;
pub(crate) mod traces;

use crate::error::IngestError;
use crate::infra::metrics::OtlpRequestRecorder;

/// Narrow a `usize` drop count to `i64` for OTLP partial-success reporting.
///
/// Returns `Ok(None)` when `drops == 0` so callers can skip emitting a
/// `partial_success` payload entirely. Returns `Err` only on the
/// mathematically-unreachable i64 overflow path; `count_noun` names the
/// rejected unit (e.g. `"spans"`, `"data points"`) in that error message.
pub fn rejected_from_drops(drops: usize, count_noun: &str) -> Result<Option<i64>, IngestError> {
    if drops == 0 {
        return Ok(None);
    }
    i64::try_from(drops)
        .map(Some)
        .map_err(|_| IngestError::Validation(format!("Rejected {count_noun} count exceeds i64")))
}

/// Finish request metrics with status "partial" when there are transform-time
/// drops, otherwise with status "ok".
pub fn finish_with_drops(request_metrics: &OtlpRequestRecorder, drops: usize) {
    if drops > 0 {
        request_metrics.finish_partial();
    } else {
        request_metrics.finish_ok();
    }
}

/// Compose a combined error message when both transform-time drops and a
/// WAL-side partial failure contributed to rejections.
///
/// When `drops == 0` the WAL reason is returned verbatim so existing WAL-only
/// failures keep their original wording. `drop_noun` names the rejected unit
/// (e.g. `"span(s)"`, `"data point(s)"`) and `transform_msg` is the signal's
/// strict-conformance message.
pub fn compose_partial_reason(wal_reason: &str, drops: usize, drop_noun: &str, transform_msg: &str) -> String {
    if drops == 0 {
        return wal_reason.to_string();
    }
    format!("{wal_reason}; also {drops} {drop_noun} rejected at transform: {transform_msg}")
}
