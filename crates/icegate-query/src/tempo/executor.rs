//! Search-mode executor: parses `TraceQL`, plans against spans, formats results.

use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use datafusion::arrow::array::RecordBatch;

use super::{
    error::{TempoError, TempoResult},
    formatters::spansets_to_search_response,
    models::{SearchParams, SearchResponse},
    validation,
};
use crate::{
    engine::QueryEngine,
    error::{ParseError, QueryError},
    traceql::{
        DEFAULT_SPANS_PER_SPANSET, TraceQLExpr,
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
/// - 400 (`Parse` / `Validation` / `Plan`) when the query is malformed,
///   too long, or its time window is inverted / oversize.
/// - 501 (`NotImplemented`) when the query uses unsupported features —
///   currently includes the `minDuration` / `maxDuration` parameters,
///   which are accepted by the wire format but not yet pushed into the
///   planner. Returning 501 (rather than silently ignoring) prevents a
///   caller from believing their filter applied.
/// - 500 for engine / `DataFusion` failures.
pub async fn execute(
    engine: Arc<QueryEngine>,
    tenant_id: String,
    params: &SearchParams,
) -> TempoResult<SearchResponse> {
    // ----- time window -----
    let now = Utc::now();
    let start = params
        .start
        .as_deref()
        .map_or(Ok(now - Duration::hours(1)), parse_time)
        .map_err(TempoError::new)?;
    let end = params.end.as_deref().map_or(Ok(now), parse_time).map_err(TempoError::new)?;
    validation::validate_query_window(start, end).map_err(TempoError::new)?;

    // ----- duration filters: validate format first, then reject as not-yet-implemented -----
    // Validate before the NotImplemented short-circuit so malformed values
    // surface as 400 (caller error) rather than being masked by the 501.
    validate_duration_param("minDuration", params.min_duration.as_deref())?;
    validate_duration_param("maxDuration", params.max_duration.as_deref())?;
    if params.min_duration.is_some() || params.max_duration.is_some() {
        return Err(TempoError::new(QueryError::NotImplemented(
            "minDuration / maxDuration are accepted by the API but not yet \
             applied by the planner; remove the parameter or use a span-level \
             `{ duration > Xs }` filter in the TraceQL query"
                .to_string(),
        )));
    }

    // ----- spans-per-spanset cap -----
    // `spss=0` disables the per-trace span cap (matches upstream Tempo).
    // Anything else — including the absent case — clamps to the
    // server-side default so we never let a single pathological trace
    // blow up stage 2's working set.
    let spans_per_spanset = match params.spss {
        Some(0) => None,
        Some(n) => Some(n),
        None => Some(DEFAULT_SPANS_PER_SPANSET),
    };

    // ----- search limit (clamped to MAX_SEARCH_LIMIT) -----
    let limit = validation::clamp_search_limit(params.limit);

    let qctx = QueryContext {
        tenant_id,
        start,
        end,
        limit,
        spans_per_spanset,
        // Always None until the planner consumes them; we already returned
        // 501 above when the caller supplied a value.
        min_duration: None,
        max_duration: None,
        step: None,
        max_grid_points: QueryContext::DEFAULT_MAX_GRID_POINTS,
    };

    // ----- query string -----
    // Default query when q is omitted: empty selector returns recent traces.
    let q = params.q.as_deref().unwrap_or("{}");
    validation::validate_query_length(q).map_err(TempoError::new)?;

    let expr = parse_query_blocking(q.to_string()).await?;

    // Reject metrics-mode queries on the search endpoint: the planner accepts them but
    // `spansets_to_search_response` only knows how to format spanset RecordBatches.
    if expr.is_metrics() {
        return Err(TempoError::new(QueryError::Validation(
            "metrics-mode queries are not supported on /api/search; use the metrics endpoint".to_string(),
        )));
    }

    let session = engine.create_session().await.map_err(TempoError::new)?;
    let planner = DataFusionPlanner::new(session, qctx);
    let df = planner.plan(expr).await.map_err(TempoError::new)?;

    // Safety-net total-spans ceiling. The per-trace `spss` cap already
    // bounds memory in the common case; this limit is the emergency
    // brake for `spss=0` (Tempo's "unlimited" opt-out) so a single
    // pathological multi-trace response cannot OOM the formatter.
    // Applied unconditionally — if the cap fires when `spss` is set,
    // the user has asked for far more spans than any real visualisation
    // can render anyway.
    let df = df
        .limit(0, Some(validation::MAX_TOTAL_SPANS))
        .map_err(|e| TempoError::new(QueryError::from(e)))?;

    let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| TempoError::new(QueryError::from(e)))?;

    Ok(spansets_to_search_response(&batches))
}

/// Parse a `TraceQL` query string on the blocking pool.
///
/// ANTLR parsing is CPU-bound and the parser internally uses [`std::rc::Rc`]
/// (so `ANTLRError` is not [`Send`]). This helper centralises the
/// `spawn_blocking + span.in_scope + make_query_error_send + join_error`
/// dance so both [`execute`] and [`super::handlers::parse_q_to_predicate`]
/// share a single parsing path.
///
/// `q` is taken by value because [`tokio::task::spawn_blocking`] needs a
/// `'static` closure; callers should pre-validate length via
/// [`super::validation::validate_query_length`].
pub(super) async fn parse_query_blocking(q: String) -> TempoResult<TraceQLExpr> {
    let span = tracing::Span::current();
    let parse_result = tokio::task::spawn_blocking(move || {
        span.in_scope(|| {
            let parser = AntlrParser::new();
            parser.parse(&q).map_err(make_query_error_send)
        })
    })
    .await
    .map_err(join_error)?;
    parse_result.map_err(TempoError::new)
}

/// Strip non-Send `ANTLRError` details from [`QueryError`] so it can
/// cross a `spawn_blocking` boundary. `ANTLRError` carries `Rc` and is
/// therefore not `Send`; keeping only the human-readable message
/// preserves the diagnostic without violating the trait bound.
pub(super) fn make_query_error_send(err: QueryError) -> QueryError {
    match err {
        QueryError::Parse(errors) => {
            QueryError::Parse(errors.into_iter().map(|e| ParseError { antlr_error: None, ..e }).collect())
        }
        other => other,
    }
}

/// Map a [`tokio::task::JoinError`] from `spawn_blocking` into a [`TempoError`].
///
/// Takes by value because `map_err` passes ownership.
#[allow(clippy::needless_pass_by_value)]
pub(super) fn join_error(e: tokio::task::JoinError) -> TempoError {
    TempoError::new(QueryError::Internal(format!("task panicked: {e}")))
}

/// Validate the format of an optional duration query parameter without
/// consuming it. Returns an HTTP 400 (`Validation`) when the value is
/// present but cannot be parsed as a `TraceQL` duration literal.
///
/// `None` and well-formed inputs both succeed silently. Used to prove
/// that malformed `minDuration` / `maxDuration` values fail loudly even
/// while the planner still rejects the *feature* with a 501 below.
fn validate_duration_param(name: &str, value: Option<&str>) -> TempoResult<()> {
    let Some(text) = value else {
        return Ok(());
    };
    if parse_duration_opt(text).is_some() {
        return Ok(());
    }
    Err(TempoError::new(QueryError::Validation(format!(
        "invalid {name} format: {text:?} (expected TraceQL duration literal, e.g. `5s`, `100ms`)"
    ))))
}

/// Parse a Tempo time parameter.
///
/// Tempo's `/api/search` spec defines `start` / `end` as Unix epoch seconds
/// (integer or fractional). We also accept millisecond / microsecond /
/// nanosecond encodings by magnitude so clients that reuse a Loki-style
/// nanosecond convention still work. RFC3339 is accepted as a fallback.
///
/// # Errors
///
/// Returns [`QueryError::Validation`] when the input does not parse as
/// any of the accepted forms — silently returning [`DateTime::UNIX_EPOCH`]
/// would obscure caller bugs and produce surprise wide-window scans.
fn parse_time(s: &str) -> Result<DateTime<Utc>, QueryError> {
    // Fractional seconds (e.g. "1776611234.567") — handled first so we don't
    // truncate the sub-second component when the integer path succeeds.
    if s.contains('.') {
        if let Ok(secs) = s.parse::<f64>() {
            return from_fractional_seconds(secs);
        }
    }

    if let Ok(n) = s.parse::<i64>() {
        return Ok(from_epoch_with_magnitude(n));
    }

    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| QueryError::Validation(format!("could not parse time '{s}': {e}")))
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
///
/// # Errors
///
/// Returns [`QueryError::Validation`] for non-finite values
/// (`NaN` / +/- inf) and values that overflow `i64` nanoseconds.
#[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
fn from_fractional_seconds(secs: f64) -> Result<DateTime<Utc>, QueryError> {
    if !secs.is_finite() {
        return Err(QueryError::Validation(format!("non-finite timestamp '{secs}'")));
    }
    let nanos = (secs * 1_000_000_000.0).round();
    if nanos > i64::MAX as f64 || nanos < i64::MIN as f64 {
        return Err(QueryError::Validation(format!(
            "timestamp '{secs}' overflows i64 nanoseconds",
        )));
    }
    Ok(DateTime::from_timestamp_nanos(nanos as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_unix_epoch_seconds_not_nanos() {
        // 10-digit "now"-ish second value as Grafana sends it.
        let t = parse_time("1776611234").expect("seconds parse");
        assert_eq!(t.timestamp(), 1_776_611_234);
    }

    #[test]
    fn parses_fractional_seconds() {
        let t = parse_time("1776611234.5").expect("fractional parse");
        assert_eq!(t.timestamp(), 1_776_611_234);
        assert_eq!(t.timestamp_subsec_nanos(), 500_000_000);
    }

    #[test]
    fn parses_millisecond_magnitude() {
        // 13-digit value looks like ms.
        let t = parse_time("1776611234567").expect("ms parse");
        assert_eq!(t.timestamp_millis(), 1_776_611_234_567);
    }

    #[test]
    fn parses_nanosecond_magnitude() {
        // 19-digit value looks like ns.
        let t = parse_time("1776611234567890000").expect("ns parse");
        assert_eq!(t.timestamp_nanos_opt(), Some(1_776_611_234_567_890_000));
    }

    #[test]
    fn parses_rfc3339_fallback() {
        let t = parse_time("2026-04-19T00:00:00Z").expect("rfc3339 parse");
        assert_eq!(t.timestamp(), 1_776_556_800);
    }

    #[test]
    fn malformed_input_yields_validation_error() {
        let err = parse_time("not-a-time").expect_err("must reject");
        assert!(matches!(err, QueryError::Validation(_)));
    }

    #[test]
    fn nan_seconds_yields_validation_error() {
        let err = parse_time("NaN").expect_err("must reject");
        assert!(matches!(err, QueryError::Validation(_)));
    }
}
