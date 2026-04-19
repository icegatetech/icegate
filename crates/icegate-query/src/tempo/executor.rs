//! Search-mode executor: parses `TraceQL`, plans against spans, formats results.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use datafusion::arrow::array::RecordBatch;

use super::{
    error::{TempoError, TempoResult},
    formatters::spansets_to_search_response,
    models::{SearchParams, SearchResponse},
};
use crate::{
    engine::QueryEngine,
    error::QueryError,
    traceql::{
        DEFAULT_SPANS_PER_SPANSET,
        antlr::AntlrParser,
        datafusion::DataFusionPlanner,
        duration::parse_duration_opt,
        parser::Parser,
        planner::{Planner, QueryContext},
    },
};

/// Execute a Tempo `/api/search` request.
///
/// Parses the `TraceQL` query, plans it against the spans table, collects
/// the resulting spanset record batches, and formats them as a Tempo
/// `SearchResponse`.
///
/// When `params.q` is absent, a default empty-selector (`{}`) is used so
/// that callers see recent traces.
///
/// # Errors
///
/// - 400 (`Parse` / `Validation` / `Plan`) when the query is malformed.
/// - 501 (`NotImplemented`) when the query uses unsupported features.
/// - 500 for engine / `DataFusion` failures.
pub async fn execute(
    engine: Arc<QueryEngine>,
    tenant_id: String,
    params: &SearchParams,
) -> TempoResult<SearchResponse> {
    let now = Utc::now();
    let start = params.start.as_deref().map_or(now - Duration::hours(1), parse_time);
    let end = params.end.as_deref().map_or(now, parse_time);

    // `spss=0` disables the per-trace span cap (matches upstream Tempo).
    // Anything else — including the absent case — clamps to the
    // server-side default so we never let a single pathological trace
    // blow up stage 2's working set.
    let spans_per_spanset = match params.spss {
        Some(0) => None,
        Some(n) => Some(n),
        None => Some(DEFAULT_SPANS_PER_SPANSET),
    };

    let qctx = QueryContext {
        tenant_id,
        start,
        end,
        limit: params.limit,
        spans_per_spanset,
        min_duration: params.min_duration.as_deref().and_then(parse_duration_opt),
        max_duration: params.max_duration.as_deref().and_then(parse_duration_opt),
        step: None,
        max_grid_points: QueryContext::DEFAULT_MAX_GRID_POINTS,
    };

    // Default query when q is omitted: empty selector returns recent traces.
    let q = params.q.clone().unwrap_or_else(|| "{}".to_string());

    let parser = AntlrParser::new();
    let expr = parser.parse(&q).map_err(TempoError::from)?;

    let session = engine.create_session().await.map_err(TempoError)?;
    let planner = DataFusionPlanner::new(session, qctx);
    let df = planner.plan(expr).await.map_err(TempoError::from)?;
    let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| TempoError(QueryError::from(e)))?;

    Ok(spansets_to_search_response(&batches))
}

/// Parse a Tempo time parameter.
///
/// Tempo's `/api/search` spec defines `start` / `end` as Unix epoch seconds
/// (integer or fractional). We also accept millisecond / microsecond /
/// nanosecond encodings by magnitude so clients that reuse a Loki-style
/// nanosecond convention still work. RFC3339 is accepted as a fallback.
/// Malformed input returns [`DateTime::UNIX_EPOCH`].
fn parse_time(s: &str) -> DateTime<Utc> {
    // Fractional seconds (e.g. "1776611234.567") — handled first so we don't
    // truncate the sub-second component when the integer path succeeds.
    if s.contains('.') {
        if let Ok(secs) = s.parse::<f64>() {
            return from_fractional_seconds(secs);
        }
    }

    if let Ok(n) = s.parse::<i64>() {
        return from_epoch_with_magnitude(n);
    }

    DateTime::parse_from_rfc3339(s).map_or(DateTime::UNIX_EPOCH, |dt| dt.with_timezone(&Utc))
}

/// Convert a numeric timestamp to [`DateTime<Utc>`] based on magnitude.
///
/// Thresholds assume a timestamp in the last ~100 years (post-1970):
/// - `< 1e12`        → seconds       (10 digits today)
/// - `< 1e15`        → milliseconds  (13 digits today)
/// - `< 1e18`        → microseconds  (16 digits today)
/// - otherwise       → nanoseconds   (19 digits today)
fn from_epoch_with_magnitude(n: i64) -> DateTime<Utc> {
    let abs = n.unsigned_abs();
    if abs < 1_000_000_000_000 {
        DateTime::from_timestamp(n, 0).unwrap_or(DateTime::UNIX_EPOCH)
    } else if abs < 1_000_000_000_000_000 {
        DateTime::from_timestamp_millis(n).unwrap_or(DateTime::UNIX_EPOCH)
    } else if abs < 1_000_000_000_000_000_000 {
        DateTime::from_timestamp_micros(n).unwrap_or(DateTime::UNIX_EPOCH)
    } else {
        DateTime::from_timestamp_nanos(n)
    }
}

/// Convert a fractional-seconds timestamp (`1776611234.567`) to a
/// nanosecond-precise [`DateTime<Utc>`].
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn from_fractional_seconds(secs: f64) -> DateTime<Utc> {
    if !secs.is_finite() {
        return DateTime::UNIX_EPOCH;
    }
    let nanos = (secs * 1_000_000_000.0).round();
    if nanos > i64::MAX as f64 || nanos < i64::MIN as f64 {
        return DateTime::UNIX_EPOCH;
    }
    DateTime::from_timestamp_nanos(nanos as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_unix_epoch_seconds_not_nanos() {
        // 10-digit "now"-ish second value as Grafana sends it.
        let t = parse_time("1776611234");
        assert_eq!(t.timestamp(), 1_776_611_234);
    }

    #[test]
    fn parses_fractional_seconds() {
        let t = parse_time("1776611234.5");
        assert_eq!(t.timestamp(), 1_776_611_234);
        assert_eq!(t.timestamp_subsec_nanos(), 500_000_000);
    }

    #[test]
    fn parses_millisecond_magnitude() {
        // 13-digit value looks like ms.
        let t = parse_time("1776611234567");
        assert_eq!(t.timestamp_millis(), 1_776_611_234_567);
    }

    #[test]
    fn parses_nanosecond_magnitude() {
        // 19-digit value looks like ns.
        let t = parse_time("1776611234567890000");
        assert_eq!(t.timestamp_nanos_opt(), Some(1_776_611_234_567_890_000));
    }

    #[test]
    fn parses_rfc3339_fallback() {
        let t = parse_time("2026-04-19T00:00:00Z");
        assert_eq!(t.timestamp(), 1_776_556_800);
    }

    #[test]
    fn malformed_input_yields_unix_epoch() {
        assert_eq!(parse_time("not-a-time"), DateTime::<Utc>::UNIX_EPOCH);
    }
}
