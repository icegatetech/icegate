//! Trace-by-ID query: fetch every span for a given `trace_id`.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use datafusion::{
    arrow::array::RecordBatch,
    logical_expr::{col, lit},
    scalar::ScalarValue,
};
use icegate_common::{
    SPANS_TABLE_FQN,
    schema::{COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID},
};

use crate::{
    engine::QueryEngine,
    error::QueryError,
    tempo::{error::TempoResult, validation::MAX_TRACE_SPANS},
};

/// Outcome of a trace-by-id fetch.
///
/// `truncated` signals that the trace contained more than
/// [`MAX_TRACE_SPANS`] spans and the response is partial; callers
/// should propagate the signal to the client (e.g. via the
/// `x-icegate-truncated` HTTP response header) so consumers know the
/// trace view is incomplete.
#[derive(Debug)]
pub struct FetchResult {
    /// Span record batches, capped to [`MAX_TRACE_SPANS`] rows total.
    pub batches: Vec<RecordBatch>,
    /// `true` when the underlying scan returned more rows than the cap
    /// allowed and the trailing rows were dropped.
    pub truncated: bool,
}

/// Fetch all spans for a given `trace_id` under a tenant.
///
/// Returns a [`FetchResult`] whose `batches` may be empty if the trace
/// is not found. The result is hard-capped at [`MAX_TRACE_SPANS`]
/// rows; if the underlying trace is larger, the function returns the
/// first `MAX_TRACE_SPANS` rows (in scan order) and sets
/// `truncated = true` so the caller can warn the consumer.
///
/// Time range narrows the scan; pass a wide window (e.g., 7 days) by
/// default if the request did not specify `start` / `end`.
///
/// Tenant filter is applied **before** the trace-id filter so a
/// caller cannot probe the existence of a specific trace under
/// another tenant by timing the response.
///
/// # Errors
///
/// Returns [`crate::error::QueryError::Validation`] if `trace_id` is not
/// decodable hex, and [`crate::error::QueryError`] for engine, planning, or
/// execution failures.
pub async fn fetch(
    engine: Arc<QueryEngine>,
    tenant_id: &str,
    trace_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> TempoResult<FetchResult> {
    // `trace_id` is validated upstream by `validation::validate_trace_id`
    // (exactly 32 hex chars), so it decodes to exactly 16 raw bytes — the
    // Fixed(16) column width that keeps the pushed-down literal comparable
    // against the column's Fixed(16) bounds. Map any residual decode error to
    // a validation error (HTTP 400) rather than panicking or returning a
    // misleading empty trace.
    let trace_id_bytes = hex::decode(trace_id).map_err(|e| QueryError::Validation(format!("invalid trace_id: {e}")))?;
    let trace_id_lit = lit(ScalarValue::FixedSizeBinary(16, Some(trace_id_bytes)));

    let session = engine.create_session().await?;
    let df = session.table(SPANS_TABLE_FQN).await.map_err(QueryError::from)?;
    let df = df.filter(col(COL_TENANT_ID).eq(lit(tenant_id))).map_err(QueryError::from)?;
    let df = df.filter(col(COL_TRACE_ID).eq(trace_id_lit)).map_err(QueryError::from)?;
    let df = df
        .filter(
            col(COL_TIMESTAMP)
                .gt_eq(lit(ScalarValue::TimestampMicrosecond(
                    Some(start.timestamp_micros()),
                    None,
                )))
                .and(col(COL_TIMESTAMP).lt_eq(lit(ScalarValue::TimestampMicrosecond(
                    Some(end.timestamp_micros()),
                    None,
                )))),
        )
        .map_err(QueryError::from)?;
    // Apply the cap with one extra slot so we can detect truncation
    // without needing a second query — if the scan returns
    // exactly `MAX_TRACE_SPANS + 1` rows we know there's more.
    let probe_cap = MAX_TRACE_SPANS.saturating_add(1);
    let df = df.limit(0, Some(probe_cap)).map_err(QueryError::from)?;
    let batches = df.collect().await.map_err(QueryError::from)?;

    let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
    if total > MAX_TRACE_SPANS {
        // Drop the trailing row(s) so the caller never sees more than
        // the documented cap. Iceberg / Arrow batches can be
        // multi-row, so trim from the tail one batch at a time.
        let trimmed = trim_to_cap(batches, MAX_TRACE_SPANS);
        return Ok(FetchResult {
            batches: trimmed,
            truncated: true,
        });
    }
    Ok(FetchResult {
        batches,
        truncated: false,
    })
}

/// Trim a sequence of [`RecordBatch`]es to at most `cap` total rows,
/// dropping or slicing trailing batches as needed.
fn trim_to_cap(batches: Vec<RecordBatch>, cap: usize) -> Vec<RecordBatch> {
    let mut remaining = cap;
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        if remaining == 0 {
            break;
        }
        let n = batch.num_rows();
        if n <= remaining {
            remaining -= n;
            out.push(batch);
        } else {
            // `RecordBatch::slice` is a zero-copy view; safe to keep.
            out.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }
    out
}

/// Default lookup window when the caller did not specify one.
#[must_use]
pub fn default_window(now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    (now - Duration::days(7), now + Duration::minutes(5))
}
