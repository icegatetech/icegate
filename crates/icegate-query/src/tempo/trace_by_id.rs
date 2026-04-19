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

use crate::{engine::QueryEngine, error::QueryError, tempo::error::TempoResult};

/// Fetch all spans for a given `trace_id` under a tenant.
///
/// Returns an empty `Vec` if the trace is not found. Time range narrows
/// the scan; pass a wide window (e.g., 7 days) by default if the request
/// did not specify `start` / `end`.
///
/// # Errors
///
/// Returns [`crate::error::QueryError`] for engine, planning, or
/// execution failures.
pub async fn fetch(
    engine: Arc<QueryEngine>,
    tenant_id: &str,
    trace_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> TempoResult<Vec<RecordBatch>> {
    let session = engine.create_session().await?;
    let df = session.table(SPANS_TABLE_FQN).await.map_err(QueryError::from)?;
    let df = df.filter(col(COL_TENANT_ID).eq(lit(tenant_id))).map_err(QueryError::from)?;
    let df = df.filter(col(COL_TRACE_ID).eq(lit(trace_id))).map_err(QueryError::from)?;
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
    let batches = df.collect().await.map_err(QueryError::from)?;
    Ok(batches)
}

/// Default lookup window when the caller did not specify one.
#[must_use]
pub fn default_window(now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    (now - Duration::days(7), now + Duration::minutes(5))
}
