//! Shared helpers for composing OTLP metrics partial-success responses.
//!
//! Transform-time drops (strict OTLP-conformance rejections) and WAL-side
//! partial failures both contribute to `rejectedDataPoints`. The HTTP and gRPC
//! handlers share the conversion and message formatting so the two protocols
//! stay in lockstep.

use std::time::Instant;

use arrow::record_batch::RecordBatch;
use icegate_common::METRICS_TOPIC;
use icegate_queue::WriteChannel;
use tracing::debug;

use crate::error::IngestError;
use crate::infra::metrics::OtlpRequestRecorder;
use crate::wal::WalAckOutcome;

/// Signal label for the metrics WAL pipeline timing metrics.
const SIGNAL_METRICS: &str = "metrics";
/// WAL-unavailable reason recorded when the write channel is closed.
const WAL_REASON_CHANNEL_CLOSED: &str = "channel_closed";
/// Status label for successful timing metrics.
const STATUS_OK: &str = "ok";
/// Status label for failed timing metrics.
const STATUS_ERROR: &str = "error";

/// Message reported when data points are dropped at transform time for failing
/// strict OTLP validation (unset value, unspecified aggregation temporality, or
/// an out-of-range count/flag value).
pub const INVALID_METRIC_MSG: &str =
    "data point rejected: unset value, unspecified aggregation temporality, or out-of-range count/flags";

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
    crate::otlp_partial::rejected_from_drops(drops, COUNT_NOUN)
}

/// Finish request metrics with status "partial" when there are transform-time
/// drops, otherwise "ok".
pub fn finish_metrics_with_drops(request_metrics: &OtlpRequestRecorder, drops: usize) {
    crate::otlp_partial::finish_with_drops(request_metrics, drops);
}

/// Compose a combined error message when both transform-time drops and a
/// WAL-side partial failure contributed to rejections.
///
/// When `drops == 0` the WAL reason is returned verbatim.
pub fn compose_partial_reason(wal_reason: &str, drops: usize) -> String {
    crate::otlp_partial::compose_partial_reason(wal_reason, drops, DROP_NOUN, INVALID_METRIC_MSG)
}

/// Partial-success payload from transform-time drops alone: `Some((rejected,
/// message))` when `drops > 0`, otherwise `None`.
fn partial_from_drops(drops: usize) -> Result<Option<(i64, String)>, IngestError> {
    Ok(rejected_data_points_from_drops(drops)?.map(|rejected| (rejected, INVALID_METRIC_MSG.to_string())))
}

/// Sort a metrics `RecordBatch`, submit it to the WAL, await the ack, and
/// compose the partial-success payload. Shared by the HTTP and gRPC metrics
/// handlers so the transform → sort → submit → ack → partial pipeline stays in
/// lockstep across protocols; each handler only adapts the request decode and
/// the protocol-specific response/error type.
///
/// `batch_opt` is `None` when the transform dropped every data point; `drops` is
/// the transform-time drop count. Returns `Some((rejected_data_points,
/// error_message))` when a partial-success payload must be reported, or `None`
/// on a fully-accepted request. All failures propagate as [`IngestError`] for
/// each protocol to map to its own error type.
///
/// # Errors
///
/// Returns `IngestError` if WAL sorting, submission, or the ack fails, or if the
/// combined rejected-data-point count overflows.
pub async fn write_metrics_batch_to_wal(
    write_channel: &WriteChannel,
    request_metrics: &OtlpRequestRecorder<'_>,
    batch_opt: Option<RecordBatch>,
    drops: usize,
    wal_row_group_size: usize,
) -> Result<Option<(i64, String)>, IngestError> {
    let Some(batch) = batch_opt else {
        request_metrics.record_records_per_request(0);
        finish_metrics_with_drops(request_metrics, drops);
        return partial_from_drops(drops);
    };

    let record_count = batch.num_rows();
    debug!(records = record_count, "Transformed OTLP metrics to RecordBatch");
    request_metrics.record_records_per_request(record_count);

    let trace_context = icegate_common::extract_current_trace_context();
    let prepare_start = Instant::now();
    let prepared = crate::wal::sort_metrics(&batch, wal_row_group_size, trace_context).inspect_err(|_| {
        request_metrics.finish_error();
    })?;
    request_metrics.record_wal_sorting_duration(prepare_start.elapsed(), SIGNAL_METRICS, STATUS_OK);
    // A non-empty batch always yields at least one row group; treat its absence
    // as an internal invariant violation instead of silently succeeding.
    let prepared = prepared.ok_or_else(|| {
        request_metrics.finish_error();
        IngestError::Validation("metrics WAL preparation produced no row groups for a non-empty batch".to_string())
    })?;

    let enqueue_start = Instant::now();
    let pending = crate::wal::submit_sorted_rows_to_wal(write_channel, prepared)
        .await
        .inspect_err(|_| {
            request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), METRICS_TOPIC, STATUS_ERROR);
            request_metrics.add_wal_queue_unavailable(METRICS_TOPIC, WAL_REASON_CHANNEL_CLOSED);
            request_metrics.finish_error();
        })?;
    request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), METRICS_TOPIC, STATUS_OK);

    let ack_start = Instant::now();
    let ack_outcome = pending.wait_for_ack().await.inspect_err(|_| {
        request_metrics.record_wal_ack_duration(ack_start.elapsed(), METRICS_TOPIC, STATUS_ERROR);
        request_metrics.finish_error();
    })?;

    match ack_outcome {
        WalAckOutcome::Success(write_result) => {
            if let Some(ctx) = write_result.trace_context.as_deref() {
                icegate_common::add_span_link(ctx);
            }
            debug!(
                offset = write_result.offset.unwrap_or_default(),
                records = write_result.records,
                "Metrics written to WAL"
            );
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), METRICS_TOPIC, STATUS_OK);
            finish_metrics_with_drops(request_metrics, drops);
            partial_from_drops(drops)
        }
        WalAckOutcome::Partial(partial) => {
            if let Some(ctx) = partial.trace_context.as_deref() {
                icegate_common::add_span_link(ctx);
            }
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), METRICS_TOPIC, STATUS_ERROR);
            request_metrics.finish_partial();
            let combined = drops
                .checked_add(partial.rejected_records)
                .ok_or_else(|| IngestError::Validation("Rejected data points count exceeds usize::MAX".to_string()))?;
            let rejected = i64::try_from(combined)
                .map_err(|_| IngestError::Validation("Rejected data points count exceeds i64".to_string()))?;
            Ok(Some((rejected, compose_partial_reason(&partial.reason, drops))))
        }
    }
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
