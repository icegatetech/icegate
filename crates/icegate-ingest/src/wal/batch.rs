//! Per-signal WAL-write orchestration: sort a transformed `RecordBatch`, submit
//! it to the WAL, await the ack, and compose the OTLP partial-success payload.
//!
//! Logs and metrics each keep one `write_<signal>_batch_to_wal` that runs the
//! sort -> submit -> ack -> partial pipeline in lockstep. Traces are different:
//! every traces request also derives a best-effort `operations` projection, so
//! [`write_traces_with_operations_to_wal`] drives both signals with overlapping
//! acknowledgements. It splits the pipeline into a [`submit_signal`] phase (sort
//! off the reactor, then enqueue) and an [`await_signal`] phase (await ack, then
//! compose partial), submits spans first, then awaits the authoritative spans ack
//! concurrently with the entire best-effort operations leg. Each handler only
//! adapts the request decode and the protocol-specific response/error type. The
//! partial-success composition primitives live in [`crate::partial_otlp`]; logs
//! carry no transform-time drops, so their writer needs none.

use std::time::Instant;

use arrow::record_batch::RecordBatch;
use icegate_common::{LOGS_TOPIC, METRICS_TOPIC, OPERATIONS_TOPIC, SPANS_TOPIC};
use icegate_queue::WriteChannel;
use tracing::debug;

use super::writer::{PendingWalWrite, PreparedWalWrite};
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
    // A non-empty batch always yields at least one row group; treat its absence
    // as an internal invariant violation instead of silently succeeding, so valid
    // log records cannot disappear without an error. Record the OK sort timing
    // only after this check passes, so the invariant-violation path emits
    // `finish_error()` alone rather than a conflicting `status="ok"` sort sample
    // (mirrors the metrics WAL path).
    let prepared = prepared.ok_or_else(|| {
        request_metrics.finish_error();
        IngestError::Validation("logs WAL preparation produced no row groups for a non-empty batch".to_string())
    })?;
    request_metrics.record_wal_sorting_duration(prepare_start.elapsed(), SIGNAL_LOGS, STATUS_OK);

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

/// A submitted-but-not-yet-acknowledged WAL write together with its
/// transform-time drop count, or an already-finished empty/all-dropped
/// short-circuit whose metrics were recorded at submit time.
enum SignalSubmit {
    /// Nothing was enqueued (empty transform output or all rows dropped); the
    /// partial-success payload is final and request metrics are already
    /// finished, so [`await_signal`] must not touch metrics again.
    Empty(Option<(i64, String)>),
    /// A WAL write was enqueued; await its ack to compose the payload.
    Pending {
        /// Pending WAL acknowledgement handle.
        pending: PendingWalWrite,
        /// Transform-time drop count to fold into the partial-success payload.
        drops: usize,
    },
}

/// Sort one signal's `RecordBatch` off the async reactor and enqueue it to the
/// WAL, returning a pending-ack handle. Shared by the authoritative spans leg and
/// the best-effort operations leg of [`write_traces_with_operations_to_wal`].
///
/// The CPU-bound sort (and its Arrow `take` column copy) runs inside
/// [`tokio::task::spawn_blocking`] so it never blocks the runtime's worker
/// threads. Metrics are recorded on the async side because [`OtlpRequestRecorder`]
/// borrows non-`'static` state and cannot cross the blocking boundary.
///
/// # Errors
///
/// Returns `IngestError` if the sort task panics or fails, if a non-empty batch
/// produces no row groups (an invariant violation), or if the WAL channel is
/// unavailable.
async fn submit_signal(
    write_channel: &WriteChannel,
    request_metrics: &OtlpRequestRecorder<'_>,
    batch_opt: Option<RecordBatch>,
    drops: usize,
    wal_row_group_size: usize,
    signal: &str,
    topic: &str,
    sort: fn(&RecordBatch, usize, Option<String>) -> Result<Option<PreparedWalWrite>, IngestError>,
) -> Result<SignalSubmit, IngestError> {
    let Some(batch) = batch_opt else {
        request_metrics.record_records_per_request(0);
        partial_otlp::traces::finish_metrics_with_drops(request_metrics, drops);
        return Ok(SignalSubmit::Empty(partial_otlp::traces::partial_from_drops(drops)?));
    };

    let record_count = batch.num_rows();
    debug!(records = record_count, signal, "Transformed OTLP signal to RecordBatch");
    request_metrics.record_records_per_request(record_count);

    let trace_context = icegate_common::extract_current_trace_context();
    let prepare_start = Instant::now();
    // Offload the CPU-bound sort/`take` to the blocking pool so it does not stall
    // the reactor worker threads (the transform was already offloaded upstream).
    let prepared = match tokio::task::spawn_blocking(move || sort(&batch, wal_row_group_size, trace_context)).await {
        Ok(result) => result,
        Err(join_err) => Err(IngestError::Join(join_err)),
    }
    .inspect_err(|_| request_metrics.finish_error())?;
    // A non-empty batch always yields at least one row group; treat its absence
    // as an internal invariant violation instead of silently succeeding, so valid
    // rows cannot disappear without an error. Record the OK sort timing only after
    // this check passes, so the invariant-violation path emits `finish_error()`
    // alone rather than a conflicting `status="ok"` sort sample.
    let prepared = prepared.ok_or_else(|| {
        request_metrics.finish_error();
        IngestError::Validation(format!(
            "{signal} WAL preparation produced no row groups for a non-empty batch"
        ))
    })?;
    request_metrics.record_wal_sorting_duration(prepare_start.elapsed(), signal, STATUS_OK);

    let enqueue_start = Instant::now();
    let pending = submit_sorted_rows_to_wal(write_channel, prepared).await.inspect_err(|_| {
        request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), topic, STATUS_ERROR);
        request_metrics.add_wal_queue_unavailable(topic, WAL_REASON_CHANNEL_CLOSED);
        request_metrics.finish_error();
    })?;
    request_metrics.record_wal_enqueue_duration(enqueue_start.elapsed(), topic, STATUS_OK);

    Ok(SignalSubmit::Pending { pending, drops })
}

/// Await a previously [`submit_signal`]-ed WAL write and compose its OTLP
/// partial-success payload. The [`SignalSubmit::Empty`] arm short-circuits
/// without touching metrics (already finished at submit), keeping every
/// `OtlpRequestRecorder` finished exactly once.
///
/// # Errors
///
/// Returns `IngestError` if the WAL ack channel fails or the rejected-record
/// count overflows.
async fn await_signal(
    request_metrics: &OtlpRequestRecorder<'_>,
    submit: SignalSubmit,
    topic: &str,
) -> Result<Option<(i64, String)>, IngestError> {
    let (pending, drops) = match submit {
        SignalSubmit::Empty(payload) => return Ok(payload),
        SignalSubmit::Pending { pending, drops } => (pending, drops),
    };

    let ack_start = Instant::now();
    let ack_outcome = pending.wait_for_ack().await.inspect_err(|_| {
        request_metrics.record_wal_ack_duration(ack_start.elapsed(), topic, STATUS_ERROR);
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
                topic,
                "Signal written to WAL"
            );
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), topic, STATUS_OK);
            partial_otlp::traces::finish_metrics_with_drops(request_metrics, drops);
            partial_otlp::traces::partial_from_drops(drops)
        }
        WalAckOutcome::Partial(partial) => {
            if let Some(ctx) = partial.trace_context.as_deref() {
                icegate_common::add_span_link(ctx);
            }
            request_metrics.record_wal_ack_duration(ack_start.elapsed(), topic, STATUS_ERROR);
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

/// Run the best-effort operations WAL leg to completion, swallowing every error.
///
/// Operations is a best-effort projection of spans (TRI-72): a transform, sort,
/// enqueue, or ack failure must never fail or alter the authoritative traces
/// response. Each failure path logs once, and `submit_signal`/`await_signal`
/// record their own terminal metric, so this returns `()` and is safe to overlap
/// with the spans ack via `join!`.
async fn run_operations_best_effort(
    write_channel: &WriteChannel,
    operations_metrics: &OtlpRequestRecorder<'_>,
    operations_result: Result<(Option<RecordBatch>, usize), IngestError>,
    wal_row_group_size: usize,
) {
    let (batch_opt, drops) = match operations_result {
        Ok(value) => value,
        Err(err) => {
            tracing::error!(error = %err, "Failed to transform operations from spans (best-effort)");
            operations_metrics.finish_with_status(STATUS_ERROR);
            return;
        }
    };

    let submit = match submit_signal(
        write_channel,
        operations_metrics,
        batch_opt,
        drops,
        wal_row_group_size,
        SIGNAL_OPERATIONS,
        OPERATIONS_TOPIC,
        sort_operations,
    )
    .await
    {
        Ok(submit) => submit,
        Err(err) => {
            // `submit_signal` already recorded the terminal error status.
            tracing::error!(error = %err, "Failed to submit operations batch to WAL (best-effort)");
            return;
        }
    };

    if let Err(err) = await_signal(operations_metrics, submit, OPERATIONS_TOPIC).await {
        // `await_signal` already recorded the terminal error status.
        tracing::error!(error = %err, "Failed to await operations WAL ack (best-effort)");
    }
}

/// Write the authoritative spans batch and the best-effort operations batch to
/// the WAL with overlapping acknowledgements. Shared by the HTTP and gRPC traces
/// handlers, which derive both batches from the same OTLP traces request.
///
/// Spans are submitted first (preserving WAL channel ordering), then the spans
/// ack is awaited concurrently with the entire operations leg via `join!`. This
/// collapses the two serial WAL flush round-trips into one: the response-critical
/// latency drops from `spans_ack + operations_ack` to `max(spans_ack,
/// operations_ack)`. Spans are authoritative — a spans transform, sort, enqueue,
/// or ack failure fails the whole request. Operations are best-effort and can
/// never fail or alter the traces response (see [`run_operations_best_effort`]).
///
/// `operations_result` is the operations transform outcome (`Err` when the
/// transform itself failed, kept best-effort). The returned payload is derived
/// solely from the spans leg.
///
/// # Errors
///
/// Returns `IngestError` if the authoritative spans WAL write fails (sort,
/// enqueue, or ack) or the combined rejected-span count overflows.
pub async fn write_traces_with_operations_to_wal(
    write_channel: &WriteChannel,
    traces_metrics: &OtlpRequestRecorder<'_>,
    operations_metrics: &OtlpRequestRecorder<'_>,
    spans_batch_opt: Option<RecordBatch>,
    spans_drops: usize,
    operations_result: Result<(Option<RecordBatch>, usize), IngestError>,
    wal_row_group_size: usize,
) -> Result<Option<(i64, String)>, IngestError> {
    // Submit spans FIRST so the spans WriteRequest reaches the WAL channel before
    // the operations one (preserves topic ordering) and so a full bounded channel
    // can never gate the authoritative path behind the best-effort leg. A spans
    // submit failure (sort/enqueue) is authoritative and fails the request.
    let spans_submit = submit_signal(
        write_channel,
        traces_metrics,
        spans_batch_opt,
        spans_drops,
        wal_row_group_size,
        SIGNAL_TRACES,
        SPANS_TOPIC,
        sort_spans,
    )
    .await?;

    // Overlap: await the authoritative spans ack concurrently with the entire
    // best-effort operations leg (sort + enqueue + ack). `join!` runs both on the
    // current task, so no spawn/`'static` bound is needed and the tracing span is
    // retained; the operations leg never returns an error to gate the response.
    let (spans_payload, ()) = tokio::join!(
        await_signal(traces_metrics, spans_submit, SPANS_TOPIC),
        run_operations_best_effort(write_channel, operations_metrics, operations_result, wal_row_group_size),
    );

    spans_payload
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

#[cfg(test)]
mod tests {
    use icegate_common::{OPERATIONS_TOPIC, SPANS_TOPIC};
    use icegate_queue::{WriteResult, channel};
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use super::*;
    use crate::error::IngestError;
    use crate::infra::metrics::OtlpMetrics;

    /// One LLM span (carries `gen_ai.operation.name`) so the spans transform
    /// yields one row AND the operations projection yields one row.
    fn one_llm_span_request() -> ExportTraceServiceRequest {
        let span = Span {
            trace_id: vec![1u8; 16],
            span_id: vec![2u8; 8],
            parent_span_id: Vec::new(),
            trace_state: String::new(),
            name: "chat".to_string(),
            kind: 0,
            start_time_unix_nano: 1_000_000,
            end_time_unix_nano: 2_000_000,
            attributes: vec![KeyValue {
                key: "gen_ai.operation.name".to_string(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("chat".to_string())),
                }),
            }],
            dropped_attributes_count: 0,
            events: Vec::new(),
            dropped_events_count: 0,
            links: Vec::new(),
            dropped_links_count: 0,
            status: None,
            flags: 0,
        };
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    /// Build the spans and operations transform outputs from one LLM-span request.
    fn spans_and_operations() -> ((Option<RecordBatch>, usize), (Option<RecordBatch>, usize)) {
        let request = one_llm_span_request();
        let spans = crate::transform::spans_to_record_batch(&request, Some("tenant-a")).expect("spans transform");
        let operations =
            crate::transform::operations_to_record_batch(&request, Some("tenant-a")).expect("operations transform");
        (spans, operations)
    }

    fn recorder<'a>(metrics: &'a OtlpMetrics, signal: &'a str) -> OtlpRequestRecorder<'a> {
        OtlpRequestRecorder::new(metrics, "grpc", signal, "protobuf")
    }

    #[tokio::test]
    async fn driver_submits_spans_before_operations_and_overlaps_acks() {
        let (tx, mut rx) = channel(2);
        let writer = tokio::spawn(async move {
            let mut topics = Vec::new();
            for _ in 0..2 {
                let req = rx.recv().await.expect("write request");
                topics.push(req.topic.clone());
                let rows = req.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
                req.response_tx.send(WriteResult::success(1, rows, None)).expect("ack");
            }
            topics
        });

        let metrics = OtlpMetrics::new_disabled();
        let ((spans_batch, spans_drops), (ops_batch, ops_drops)) = spans_and_operations();
        let payload = write_traces_with_operations_to_wal(
            &tx,
            &recorder(&metrics, "traces"),
            &recorder(&metrics, "operations"),
            spans_batch,
            spans_drops,
            Ok((ops_batch, ops_drops)),
            4,
        )
        .await
        .expect("driver ok");

        let topics = writer.await.expect("writer task");
        assert!(payload.is_none(), "fully-accepted request carries no partial success");
        assert_eq!(topics, vec![SPANS_TOPIC.to_string(), OPERATIONS_TOPIC.to_string()]);
    }

    #[tokio::test]
    async fn driver_is_ok_when_operations_wal_fails() {
        // Spans (authoritative) acks success; the operations write hits a closed
        // receiver. The traces result must still be Ok with no partial success.
        let (tx, mut rx) = channel(2);
        let writer = tokio::spawn(async move {
            let spans_req = rx.recv().await.expect("spans request");
            assert_eq!(spans_req.topic, SPANS_TOPIC);
            let rows = spans_req.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            spans_req.response_tx.send(WriteResult::success(1, rows, None)).expect("ack");
            let ops_req = rx.recv().await.expect("operations request");
            assert_eq!(ops_req.topic, OPERATIONS_TOPIC);
            drop(ops_req); // closes the operations oneshot -> best-effort failure
        });

        let metrics = OtlpMetrics::new_disabled();
        let ((spans_batch, spans_drops), (ops_batch, ops_drops)) = spans_and_operations();
        let payload = write_traces_with_operations_to_wal(
            &tx,
            &recorder(&metrics, "traces"),
            &recorder(&metrics, "operations"),
            spans_batch,
            spans_drops,
            Ok((ops_batch, ops_drops)),
            4,
        )
        .await
        .expect("traces result must be Ok even when operations write fails");

        writer.await.expect("writer task");
        assert!(payload.is_none());
    }

    #[tokio::test]
    async fn driver_propagates_spans_wal_failure() {
        // Closed channel: the authoritative spans submit fails, which must fail
        // the whole traces request.
        let (tx, rx) = channel(1);
        drop(rx);

        let metrics = OtlpMetrics::new_disabled();
        let ((spans_batch, spans_drops), (ops_batch, ops_drops)) = spans_and_operations();
        let result = write_traces_with_operations_to_wal(
            &tx,
            &recorder(&metrics, "traces"),
            &recorder(&metrics, "operations"),
            spans_batch,
            spans_drops,
            Ok((ops_batch, ops_drops)),
            4,
        )
        .await;

        assert!(
            result.is_err(),
            "a spans WAL failure is authoritative and must fail the request"
        );
    }

    #[tokio::test]
    async fn driver_swallows_operations_transform_error() {
        // Operations transform failed upstream (best-effort): the driver must
        // still write spans and return the spans payload without erroring.
        let (tx, mut rx) = channel(1);
        let writer = tokio::spawn(async move {
            let spans_req = rx.recv().await.expect("spans request");
            assert_eq!(spans_req.topic, SPANS_TOPIC);
            let rows = spans_req.row_groups.iter().map(|rg| rg.batch.num_rows()).sum::<usize>();
            spans_req.response_tx.send(WriteResult::success(1, rows, None)).expect("ack");
            assert!(
                rx.recv().await.is_none(),
                "no operations write is submitted on transform error"
            );
        });

        let metrics = OtlpMetrics::new_disabled();
        let ((spans_batch, spans_drops), _ops) = spans_and_operations();
        let payload = write_traces_with_operations_to_wal(
            &tx,
            &recorder(&metrics, "traces"),
            &recorder(&metrics, "operations"),
            spans_batch,
            spans_drops,
            Err(IngestError::Validation("operations transform failed".to_string())),
            4,
        )
        .await
        .expect("traces result must be Ok when operations transform fails");

        drop(tx);
        writer.await.expect("writer task");
        assert!(payload.is_none());
    }
}
