//! Per-signal WAL-write orchestration: sort a transformed `RecordBatch`, submit
//! it to the WAL, await the ack, and compose the OTLP partial-success payload.
//!
//! One `write_<signal>_batch_to_wal` per signal keeps the sort -> submit -> ack
//! -> partial pipeline in lockstep across the HTTP and gRPC handlers; each
//! handler only adapts the request decode and the protocol-specific
//! response/error type. The partial-success composition primitives live in
//! [`crate::partial_otlp`]; logs carry no transform-time drops, so their writer
//! needs none.

use std::time::Instant;

use arrow::record_batch::RecordBatch;
use icegate_common::{LOGS_TOPIC, METRICS_TOPIC, OPERATIONS_TOPIC, SPANS_TOPIC};
use icegate_queue::WriteChannel;
use tracing::debug;

use super::{WalAckOutcome, sort_logs, sort_metrics, sort_operations, sort_spans, submit_sorted_rows_to_wal};
use crate::error::IngestError;
use crate::infra::metrics::OtlpRequestRecorder;
use crate::partial_otlp;

/// Signal label for the logs WAL pipeline timing metrics.
const SIGNAL_LOGS: &str = "logs";
/// Signal label for the traces WAL pipeline timing metrics.
const SIGNAL_TRACES: &str = "traces";
/// Signal label for the metrics WAL pipeline timing metrics.
const SIGNAL_METRICS: &str = "metrics";
/// Signal label for the operations WAL pipeline timing metrics.
const SIGNAL_OPERATIONS: &str = "operations";
/// WAL-unavailable reason recorded when the write channel is closed.
const WAL_REASON_CHANNEL_CLOSED: &str = "channel_closed";
/// Status label for successful timing metrics.
const STATUS_OK: &str = "ok";
/// Status label for failed timing metrics.
const STATUS_ERROR: &str = "error";

/// Sort a logs `RecordBatch`, submit it to the WAL, await the ack, and return the
/// partial-success payload. Shared by the HTTP and gRPC logs handlers.
///
/// Logs have no transform-time drops, so `batch_opt` is `None` only when the
/// transform produced no records. Returns `Some((rejected_log_records,
/// error_message))` when the WAL reports a partial write, or `None` on a
/// fully-accepted (or empty) request. All failures propagate as [`IngestError`].
///
/// # Errors
///
/// Returns `IngestError` if WAL sorting, submission, or the ack fails, or if the
/// rejected-record count overflows `i64`.
pub async fn write_logs_batch_to_wal(
    write_channel: &WriteChannel,
    request_metrics: &OtlpRequestRecorder<'_>,
    batch_opt: Option<RecordBatch>,
    wal_row_group_size: usize,
) -> Result<Option<(i64, String)>, IngestError> {
    let Some(batch) = batch_opt else {
        request_metrics.record_records_per_request(0);
        request_metrics.finish_ok();
        return Ok(None);
    };

    let record_count = batch.num_rows();
    debug!(records = record_count, "Transformed OTLP logs to RecordBatch");
    request_metrics.record_records_per_request(record_count);

    let trace_context = icegate_common::extract_current_trace_context();
    let prepare_start = Instant::now();
    let prepared = sort_logs(&batch, wal_row_group_size, trace_context).inspect_err(|_| {
        request_metrics.finish_error();
    })?;
    request_metrics.record_wal_sorting_duration(prepare_start.elapsed(), SIGNAL_LOGS, STATUS_OK);
    let Some(prepared) = prepared else {
        request_metrics.finish_ok();
        return Ok(None);
    };

    let enqueue_start = Instant::now();
    let pending = submit_sorted_rows_to_wal(write_channel, prepared).await.inspect_err(|_| {
        request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
        request_metrics.add_wal_queue_unavailable(LOGS_TOPIC, WAL_REASON_CHANNEL_CLOSED);
        request_metrics.finish_error();
    })?;
    request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), LOGS_TOPIC, STATUS_OK);

    let ack_start = Instant::now();
    let ack_outcome = pending.wait_for_ack().await.inspect_err(|_| {
        request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
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
                "Logs written to WAL"
            );
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_OK);
            request_metrics.finish_ok();
            Ok(None)
        }
        WalAckOutcome::Partial(partial) => {
            if let Some(ctx) = partial.trace_context.as_deref() {
                icegate_common::add_span_link(ctx);
            }
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), LOGS_TOPIC, STATUS_ERROR);
            request_metrics.finish_partial();
            let rejected = i64::try_from(partial.rejected_records)
                .map_err(|_| IngestError::Validation("Rejected logs count exceeds i64".to_string()))?;
            Ok(Some((rejected, partial.reason)))
        }
    }
}

/// Sort a traces `RecordBatch`, submit it to the WAL, await the ack, and compose
/// the partial-success payload. Shared by the HTTP and gRPC traces handlers.
///
/// `batch_opt` is `None` when the transform dropped every span; `drops` is the
/// transform-time drop count (invalid `trace_id`/`span_id`). Returns
/// `Some((rejected_spans, error_message))` when a partial-success payload must be
/// reported, or `None` on a fully-accepted request. All failures propagate as
/// [`IngestError`].
///
/// # Errors
///
/// Returns `IngestError` if WAL sorting, submission, or the ack fails, or if the
/// combined rejected-span count overflows.
pub async fn write_traces_batch_to_wal(
    write_channel: &WriteChannel,
    request_metrics: &OtlpRequestRecorder<'_>,
    batch_opt: Option<RecordBatch>,
    drops: usize,
    wal_row_group_size: usize,
) -> Result<Option<(i64, String)>, IngestError> {
    let Some(batch) = batch_opt else {
        request_metrics.record_records_per_request(0);
        partial_otlp::traces::finish_metrics_with_drops(request_metrics, drops);
        return partial_otlp::traces::partial_from_drops(drops);
    };

    let record_count = batch.num_rows();
    debug!(records = record_count, "Transformed OTLP spans to RecordBatch");
    request_metrics.record_records_per_request(record_count);

    let trace_context = icegate_common::extract_current_trace_context();
    let prepare_start = Instant::now();
    let prepared = sort_spans(&batch, wal_row_group_size, trace_context).inspect_err(|_| {
        request_metrics.finish_error();
    })?;
    request_metrics.record_wal_sorting_duration(prepare_start.elapsed(), SIGNAL_TRACES, STATUS_OK);
    let Some(prepared) = prepared else {
        partial_otlp::traces::finish_metrics_with_drops(request_metrics, drops);
        return partial_otlp::traces::partial_from_drops(drops);
    };

    let enqueue_start = Instant::now();
    let pending = submit_sorted_rows_to_wal(write_channel, prepared).await.inspect_err(|_| {
        request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), SPANS_TOPIC, STATUS_ERROR);
        request_metrics.add_wal_queue_unavailable(SPANS_TOPIC, WAL_REASON_CHANNEL_CLOSED);
        request_metrics.finish_error();
    })?;
    request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), SPANS_TOPIC, STATUS_OK);

    let ack_start = Instant::now();
    let ack_outcome = pending.wait_for_ack().await.inspect_err(|_| {
        request_metrics.record_wal_ack_duration(ack_start.elapsed(), SPANS_TOPIC, STATUS_ERROR);
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
                "Spans written to WAL"
            );
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), SPANS_TOPIC, STATUS_OK);
            partial_otlp::traces::finish_metrics_with_drops(request_metrics, drops);
            partial_otlp::traces::partial_from_drops(drops)
        }
        WalAckOutcome::Partial(partial) => {
            if let Some(ctx) = partial.trace_context.as_deref() {
                icegate_common::add_span_link(ctx);
            }
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), SPANS_TOPIC, STATUS_ERROR);
            request_metrics.finish_partial();
            let combined = drops
                .checked_add(partial.rejected_records)
                .ok_or_else(|| IngestError::Validation("Rejected spans count exceeds usize::MAX".to_string()))?;
            let rejected = i64::try_from(combined)
                .map_err(|_| IngestError::Validation("Rejected spans count exceeds i64".to_string()))?;
            Ok(Some((
                rejected,
                partial_otlp::traces::compose_partial_reason(&partial.reason, drops),
            )))
        }
    }
}

/// Sort a metrics `RecordBatch`, submit it to the WAL, await the ack, and compose
/// the partial-success payload. Shared by the HTTP and gRPC metrics handlers.
///
/// `batch_opt` is `None` when the transform dropped every data point; `drops` is
/// the transform-time drop count. Returns `Some((rejected_data_points,
/// error_message))` when a partial-success payload must be reported, or `None`
/// on a fully-accepted request. All failures propagate as [`IngestError`].
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
        partial_otlp::metrics::finish_metrics_with_drops(request_metrics, drops);
        return partial_otlp::metrics::partial_from_drops(drops);
    };

    let record_count = batch.num_rows();
    debug!(records = record_count, "Transformed OTLP metrics to RecordBatch");
    request_metrics.record_records_per_request(record_count);

    let trace_context = icegate_common::extract_current_trace_context();
    let prepare_start = Instant::now();
    let prepared = sort_metrics(&batch, wal_row_group_size, trace_context).inspect_err(|_| {
        request_metrics.finish_error();
    })?;
    // A non-empty batch always yields at least one row group; treat its absence
    // as an internal invariant violation instead of silently succeeding. Record
    // the OK sort timing only after this check passes, so the invariant-violation
    // path emits `finish_error()` alone rather than a conflicting `status="ok"`
    // sort sample (mirrors the `sort_metrics` error branch above).
    let prepared = prepared.ok_or_else(|| {
        request_metrics.finish_error();
        IngestError::Validation("metrics WAL preparation produced no row groups for a non-empty batch".to_string())
    })?;
    request_metrics.record_wal_sorting_duration(prepare_start.elapsed(), SIGNAL_METRICS, STATUS_OK);

    let enqueue_start = Instant::now();
    let pending = submit_sorted_rows_to_wal(write_channel, prepared).await.inspect_err(|_| {
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
            partial_otlp::metrics::finish_metrics_with_drops(request_metrics, drops);
            partial_otlp::metrics::partial_from_drops(drops)
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
            Ok(Some((
                rejected,
                partial_otlp::metrics::compose_partial_reason(&partial.reason, drops),
            )))
        }
    }
}

/// Sort an operations `RecordBatch`, submit it to the WAL, await the ack, and
/// compose the partial-success payload. Shared by the HTTP and gRPC traces
/// handlers, which derive operations from the same OTLP traces request.
///
/// `batch_opt` is `None` when the transform produced no operation rows; `drops`
/// is the transform-time drop count (spans that failed strict typed projection).
/// Returns `Some((rejected_operations, error_message))` when a partial-success
/// payload must be reported, or `None` on a fully-accepted request. All failures
/// propagate as [`IngestError`].
///
/// # Errors
///
/// Returns `IngestError` if WAL sorting, submission, or the ack fails, or if the
/// combined rejected-operation count overflows.
pub async fn write_operations_batch_to_wal(
    write_channel: &WriteChannel,
    request_metrics: &OtlpRequestRecorder<'_>,
    batch_opt: Option<RecordBatch>,
    drops: usize,
    wal_row_group_size: usize,
) -> Result<Option<(i64, String)>, IngestError> {
    let Some(batch) = batch_opt else {
        request_metrics.record_records_per_request(0);
        partial_otlp::traces::finish_metrics_with_drops(request_metrics, drops);
        return partial_otlp::traces::partial_from_drops(drops);
    };

    let record_count = batch.num_rows();
    debug!(
        records = record_count,
        "Transformed OTLP spans to operations RecordBatch"
    );
    request_metrics.record_records_per_request(record_count);

    let trace_context = icegate_common::extract_current_trace_context();
    let prepare_start = Instant::now();
    let prepared = sort_operations(&batch, wal_row_group_size, trace_context).inspect_err(|_| {
        request_metrics.finish_error();
    })?;
    request_metrics.record_wal_sorting_duration(prepare_start.elapsed(), SIGNAL_OPERATIONS, STATUS_OK);
    let Some(prepared) = prepared else {
        partial_otlp::traces::finish_metrics_with_drops(request_metrics, drops);
        return partial_otlp::traces::partial_from_drops(drops);
    };

    let enqueue_start = Instant::now();
    let pending = submit_sorted_rows_to_wal(write_channel, prepared).await.inspect_err(|_| {
        request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), OPERATIONS_TOPIC, STATUS_ERROR);
        request_metrics.add_wal_queue_unavailable(OPERATIONS_TOPIC, WAL_REASON_CHANNEL_CLOSED);
        request_metrics.finish_error();
    })?;
    request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), OPERATIONS_TOPIC, STATUS_OK);

    let ack_start = Instant::now();
    let ack_outcome = pending.wait_for_ack().await.inspect_err(|_| {
        request_metrics.record_wal_ack_duration(ack_start.elapsed(), OPERATIONS_TOPIC, STATUS_ERROR);
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
                "Operations written to WAL"
            );
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), OPERATIONS_TOPIC, STATUS_OK);
            partial_otlp::traces::finish_metrics_with_drops(request_metrics, drops);
            partial_otlp::traces::partial_from_drops(drops)
        }
        WalAckOutcome::Partial(partial) => {
            if let Some(ctx) = partial.trace_context.as_deref() {
                icegate_common::add_span_link(ctx);
            }
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), OPERATIONS_TOPIC, STATUS_ERROR);
            request_metrics.finish_partial();
            let combined = drops
                .checked_add(partial.rejected_records)
                .ok_or_else(|| IngestError::Validation("Rejected operations count exceeds usize::MAX".to_string()))?;
            let rejected = i64::try_from(combined)
                .map_err(|_| IngestError::Validation("Rejected operations count exceeds i64".to_string()))?;
            Ok(Some((
                rejected,
                partial_otlp::traces::compose_partial_reason(&partial.reason, drops),
            )))
        }
    }
}
