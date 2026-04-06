//! Query execution logic for Prometheus API.
//!
//! Extracts the common query execution flow from handlers, including
//! parsing, planning, and execution of `PromQL` queries. Mirrors the
//! Loki executor pattern with Prometheus-specific time parsing and
//! response formatting.

use std::{sync::Arc, time::Instant};

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::{
    arrow::{
        array::{Array, Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray},
        datatypes::DataType,
    },
    prelude::SessionContext,
};

use super::{
    error::{PrometheusError, PrometheusResult},
    models::{InstantQueryParams, LabelValuesParams, LabelsParams, RangeQueryParams},
};
use crate::{
    engine::QueryEngine,
    error::{ParseError, QueryError},
    infra::metrics::QueryMetrics,
    promql::{Parser, Planner, antlr::AntlrParser, datafusion::DataFusionPlanner, planner::PromQLQueryContext},
};

// ============================================================================
// Time Parsing
// ============================================================================

/// Parse a Prometheus time string (seconds or RFC3339).
///
/// Prometheus uses **seconds** (with optional decimal fraction) for Unix
/// timestamps, unlike Loki which uses nanoseconds. Falls back to
/// `DateTime::UNIX_EPOCH` if parsing fails.
///
/// # Arguments
///
/// * `s` - Unix timestamp in seconds (e.g. "1609459200", "1609459200.123")
///   or RFC3339 string (e.g. "2021-01-01T00:00:00Z")
fn parse_time(s: &str) -> DateTime<Utc> {
    // Try parsing as float seconds first (Prometheus convention)
    if let Ok(secs) = s.parse::<f64>() {
        #[allow(clippy::cast_possible_truncation)]
        let whole_secs = secs.trunc() as i64;
        // Convert fractional seconds to nanoseconds for sub-second precision
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let nanos = ((secs.fract().abs()) * 1_000_000_000.0) as u32;
        return DateTime::from_timestamp(whole_secs, nanos).unwrap_or(DateTime::UNIX_EPOCH);
    }

    // Try RFC3339
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or(DateTime::UNIX_EPOCH)
}

/// Parse a Prometheus step parameter.
///
/// Accepts either a `PromQL` duration string (e.g. "15s", "1m") or a
/// plain float representing seconds (e.g. "15", "60.5").
///
/// # Errors
///
/// Returns `QueryError::Validation` if the step cannot be parsed or
/// results in a non-positive duration.
fn parse_step(s: &str) -> crate::error::Result<TimeDelta> {
    // Try as PromQL duration first
    if let Ok(d) = crate::promql::duration::parse_duration(s) {
        if d <= TimeDelta::zero() {
            return Err(QueryError::Validation("step must be positive".to_string()));
        }
        return Ok(d);
    }

    // Try as float seconds
    let secs: f64 = s.parse().map_err(|_| QueryError::Validation(format!("invalid step: {s}")))?;

    if secs <= 0.0 {
        return Err(QueryError::Validation("step must be positive".to_string()));
    }

    #[allow(clippy::cast_possible_truncation)]
    let millis = (secs * 1000.0) as i64;
    Ok(TimeDelta::milliseconds(millis))
}

// ============================================================================
// spawn_blocking helpers
// ============================================================================

/// Strip non-Send `ANTLRError` details from [`QueryError`], keeping
/// human-readable messages.
///
/// Required for crossing `spawn_blocking` boundaries since `ANTLRError`
/// contains `Rc`.
fn make_query_error_send(err: QueryError) -> QueryError {
    match err {
        QueryError::Parse(errors) => {
            QueryError::Parse(errors.into_iter().map(|e| ParseError { antlr_error: None, ..e }).collect())
        }
        other => other,
    }
}

/// Map a [`tokio::task::JoinError`] from `spawn_blocking` into a
/// [`PrometheusError`].
#[allow(clippy::needless_pass_by_value)]
fn join_error(e: tokio::task::JoinError) -> PrometheusError {
    PrometheusError(QueryError::Internal(format!("task panicked: {e}")))
}

// ============================================================================
// Result Formatting
// ============================================================================

/// Format `RecordBatch` results as a Prometheus instant vector JSON value.
///
/// Expected batch schema: `timestamp`, `metric_name`, `attributes`, `value`.
/// Produces: `{"resultType": "vector", "result": [{"metric": {...}, "value":
/// [ts, "val"]}, ...]}`
fn format_instant_vector(batches: &[RecordBatch]) -> serde_json::Value {
    let mut results = Vec::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        let timestamps = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>();
        let metric_names = batch.column(1).as_any().downcast_ref::<StringArray>();
        let values = batch.column(3).as_any().downcast_ref::<Float64Array>();

        // If columns are not the expected types, skip this batch
        let (Some(timestamps), Some(metric_names), Some(values)) = (timestamps, metric_names, values) else {
            continue;
        };

        // Attempt to read attributes as a string column (cast from MAP)
        let attrs_as_string = try_extract_attributes_as_string(batch, 2);

        for row in 0..num_rows {
            if timestamps.is_null(row) || values.is_null(row) {
                continue;
            }

            let mut metric = serde_json::Map::new();
            if !metric_names.is_null(row) {
                metric.insert(
                    "__name__".to_string(),
                    serde_json::Value::String(metric_names.value(row).to_string()),
                );
            }

            // Include attributes if available
            if let Some(ref attrs) = attrs_as_string {
                if !attrs.is_null(row) {
                    // Attributes are cast to string representation; parse if
                    // possible, otherwise include as-is
                    let attr_str = attrs.value(row);
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(attr_str) {
                        for (k, v) in parsed {
                            metric.insert(k, v);
                        }
                    }
                }
            }

            // Prometheus timestamps are in seconds (float)
            #[allow(clippy::cast_precision_loss)]
            let ts_secs = timestamps.value(row) as f64 / 1_000_000.0;
            let val_str = format_float(values.value(row));

            results.push(serde_json::json!({
                "metric": metric,
                "value": [ts_secs, val_str]
            }));
        }
    }

    serde_json::json!({
        "resultType": "vector",
        "result": results
    })
}

/// Format `RecordBatch` results as a Prometheus range vector (matrix) JSON
/// value.
///
/// Groups samples by series identity (`metric_name` + `attributes`) and
/// produces: `{"resultType": "matrix", "result": [{"metric": {...},
/// "values": [[ts, "val"], ...]}, ...]}`
fn format_range_vector(batches: &[RecordBatch]) -> serde_json::Value {
    use std::collections::BTreeMap;

    // Group samples by series key (metric_name + attributes string)
    let mut series_map: BTreeMap<String, (serde_json::Map<String, serde_json::Value>, Vec<serde_json::Value>)> =
        BTreeMap::new();

    for batch in batches {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        let timestamps = batch.column(0).as_any().downcast_ref::<TimestampMicrosecondArray>();
        let metric_names = batch.column(1).as_any().downcast_ref::<StringArray>();
        let values = batch.column(3).as_any().downcast_ref::<Float64Array>();

        let (Some(timestamps), Some(metric_names), Some(values)) = (timestamps, metric_names, values) else {
            continue;
        };

        let attrs_as_string = try_extract_attributes_as_string(batch, 2);

        for row in 0..num_rows {
            if timestamps.is_null(row) || values.is_null(row) {
                continue;
            }

            let name = if metric_names.is_null(row) {
                String::new()
            } else {
                metric_names.value(row).to_string()
            };

            let attr_str = attrs_as_string
                .as_ref()
                .filter(|a| !a.is_null(row))
                .map(|a| a.value(row).to_string())
                .unwrap_or_default();

            let series_key = format!("{name}\x00{attr_str}");

            let entry = series_map.entry(series_key).or_insert_with(|| {
                let mut metric = serde_json::Map::new();
                if !name.is_empty() {
                    metric.insert("__name__".to_string(), serde_json::Value::String(name.clone()));
                }
                if let Ok(parsed) = serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&attr_str) {
                    for (k, v) in parsed {
                        metric.insert(k, v);
                    }
                }
                (metric, Vec::new())
            });

            #[allow(clippy::cast_precision_loss)]
            let ts_secs = timestamps.value(row) as f64 / 1_000_000.0;
            let val_str = format_float(values.value(row));

            entry.1.push(serde_json::json!([ts_secs, val_str]));
        }
    }

    let results: Vec<serde_json::Value> = series_map
        .into_values()
        .map(|(metric, values)| {
            serde_json::json!({
                "metric": metric,
                "values": values
            })
        })
        .collect();

    serde_json::json!({
        "resultType": "matrix",
        "result": results
    })
}

/// Format a float value for Prometheus JSON output.
///
/// Handles special cases: `NaN`, `+Inf`, `-Inf`. Otherwise uses standard
/// display formatting.
fn format_float(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() { "+Inf" } else { "-Inf" }.to_string()
    } else {
        v.to_string()
    }
}

/// Try to extract attributes column as a `StringArray`.
///
/// The planner outputs attributes as a MAP column. For JSON formatting,
/// we attempt to read it as a string (which DataFusion may have cast).
/// Returns `None` if the column is not a string type.
fn try_extract_attributes_as_string(batch: &RecordBatch, col_idx: usize) -> Option<StringArray> {
    if col_idx >= batch.num_columns() {
        return None;
    }

    let col = batch.column(col_idx);

    // Direct string column
    if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
        return Some(arr.clone());
    }

    // Try casting MAP to string via Arrow cast
    datafusion::arrow::compute::cast(col, &DataType::Utf8)
        .ok()
        .and_then(|arr| arr.as_any().downcast_ref::<StringArray>().cloned())
}

// ============================================================================
// Query Executor
// ============================================================================

/// Query executor for Prometheus API operations.
///
/// Encapsulates the common flow of creating sessions, parsing queries,
/// planning, and executing against the query engine. Each phase is timed
/// and recorded via [`QueryMetrics`].
pub struct QueryExecutor {
    engine: Arc<QueryEngine>,
    metrics: Arc<QueryMetrics>,
}

impl QueryExecutor {
    /// Create a new query executor with metrics.
    pub const fn new(engine: Arc<QueryEngine>, metrics: Arc<QueryMetrics>) -> Self {
        Self { engine, metrics }
    }

    /// Create a DataFusion session context (timed), counting errors via
    /// metrics.
    async fn create_session(&self) -> PrometheusResult<SessionContext> {
        let start = Instant::now();
        let session = self.engine.create_session().await.map_err(|e| {
            self.metrics.record_session_create_duration(start.elapsed());
            self.metrics.add_error("prometheus", "session");
            e
        })?;
        self.metrics.record_session_create_duration(start.elapsed());
        Ok(session)
    }

    /// Execute an instant query and return formatted Prometheus JSON.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - Tenant identifier for multi-tenancy isolation.
    /// * `params` - Instant query parameters (query, time, timeout).
    ///
    /// # Errors
    ///
    /// Returns `PrometheusError` on parse, plan, or execution failure.
    #[tracing::instrument(skip(self, params), fields(tenant_id, query = %params.query))]
    pub async fn execute_instant_query(
        &self,
        tenant_id: String,
        params: &InstantQueryParams,
    ) -> PrometheusResult<serde_json::Value> {
        let exec_start = Instant::now();

        let eval_time = params.time.as_ref().map_or_else(Utc::now, |s| parse_time(s));

        let query_ctx = PromQLQueryContext::instant(tenant_id, eval_time);

        // Parse PromQL (CPU-bound ANTLR parse offloaded to blocking thread)
        let parse_start = Instant::now();
        let query_str = params.query.clone();
        let span = tracing::Span::current();
        let parse_result = tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                let parser = AntlrParser::new();
                parser.parse(&query_str).map_err(make_query_error_send)
            })
        })
        .await
        .map_err(join_error)?;

        let expr = match parse_result {
            Ok(expr) => {
                self.metrics.record_parse_duration(parse_start.elapsed(), "prometheus");
                expr
            }
            Err(e) => {
                self.metrics.record_parse_duration(parse_start.elapsed(), "prometheus");
                self.metrics.add_error("prometheus", "parse");
                return Err(e.into());
            }
        };

        // Create session, plan, and execute
        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx.clone(), query_ctx);

        let plan_start = Instant::now();
        let df = match planner.plan(expr).await {
            Ok(df) => {
                self.metrics.record_plan_duration(plan_start.elapsed(), "prometheus", "instant");
                df
            }
            Err(e) => {
                self.metrics.record_plan_duration(plan_start.elapsed(), "prometheus", "instant");
                self.metrics.add_error("prometheus", "plan");
                return Err(e.into());
            }
        };

        let execute_start = Instant::now();
        let batches = match df.collect().await {
            Ok(b) => {
                self.metrics
                    .record_execute_duration(execute_start.elapsed(), "prometheus", "instant");
                b
            }
            Err(e) => {
                self.metrics
                    .record_execute_duration(execute_start.elapsed(), "prometheus", "instant");
                self.metrics.add_error("prometheus", "execute");
                return Err(QueryError::from(e).into());
            }
        };

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        self.metrics.record_result_rows(total_rows, "prometheus", "instant");

        // Format results (CPU-bound, offloaded to blocking thread)
        let format_start = Instant::now();
        let span = tracing::Span::current();
        let formatted = tokio::task::spawn_blocking(move || span.in_scope(|| format_instant_vector(&batches)))
            .await
            .map_err(join_error)?;
        self.metrics
            .record_format_duration(format_start.elapsed(), "prometheus", "vector");

        tracing::debug!(
            elapsed_ms = exec_start.elapsed().as_millis(),
            rows = total_rows,
            "instant query complete"
        );

        Ok(formatted)
    }

    /// Execute a range query and return formatted Prometheus JSON.
    ///
    /// # Arguments
    ///
    /// * `tenant_id` - Tenant identifier for multi-tenancy isolation.
    /// * `params` - Range query parameters (query, start, end, step, timeout).
    ///
    /// # Errors
    ///
    /// Returns `PrometheusError` on parse, plan, or execution failure.
    #[tracing::instrument(skip(self, params), fields(tenant_id, query = %params.query))]
    pub async fn execute_range_query(
        &self,
        tenant_id: String,
        params: &RangeQueryParams,
    ) -> PrometheusResult<serde_json::Value> {
        let exec_start = Instant::now();

        let start = parse_time(&params.start);
        let end = parse_time(&params.end);
        let step = parse_step(&params.step)?;

        let query_ctx = PromQLQueryContext::range(tenant_id, start, end, step);

        // Parse PromQL (CPU-bound ANTLR parse offloaded to blocking thread)
        let parse_start = Instant::now();
        let query_str = params.query.clone();
        let span = tracing::Span::current();
        let parse_result = tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                let parser = AntlrParser::new();
                parser.parse(&query_str).map_err(make_query_error_send)
            })
        })
        .await
        .map_err(join_error)?;

        let expr = match parse_result {
            Ok(expr) => {
                self.metrics.record_parse_duration(parse_start.elapsed(), "prometheus");
                expr
            }
            Err(e) => {
                self.metrics.record_parse_duration(parse_start.elapsed(), "prometheus");
                self.metrics.add_error("prometheus", "parse");
                return Err(e.into());
            }
        };

        // Create session, plan, and execute
        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx.clone(), query_ctx);

        let plan_start = Instant::now();
        let df = match planner.plan(expr).await {
            Ok(df) => {
                self.metrics.record_plan_duration(plan_start.elapsed(), "prometheus", "range");
                df
            }
            Err(e) => {
                self.metrics.record_plan_duration(plan_start.elapsed(), "prometheus", "range");
                self.metrics.add_error("prometheus", "plan");
                return Err(e.into());
            }
        };

        let execute_start = Instant::now();
        let batches = match df.collect().await {
            Ok(b) => {
                self.metrics
                    .record_execute_duration(execute_start.elapsed(), "prometheus", "range");
                b
            }
            Err(e) => {
                self.metrics
                    .record_execute_duration(execute_start.elapsed(), "prometheus", "range");
                self.metrics.add_error("prometheus", "execute");
                return Err(QueryError::from(e).into());
            }
        };

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        self.metrics.record_result_rows(total_rows, "prometheus", "range");

        // Format results (CPU-bound, offloaded to blocking thread)
        let format_start = Instant::now();
        let span = tracing::Span::current();
        let formatted = tokio::task::spawn_blocking(move || span.in_scope(|| format_range_vector(&batches)))
            .await
            .map_err(join_error)?;
        self.metrics
            .record_format_duration(format_start.elapsed(), "prometheus", "matrix");

        tracing::debug!(
            elapsed_ms = exec_start.elapsed().as_millis(),
            rows = total_rows,
            "range query complete"
        );

        Ok(formatted)
    }

    /// Execute a labels metadata query.
    ///
    /// Returns distinct label names from the metrics table.
    ///
    /// # Errors
    ///
    /// Returns `PrometheusError` on session creation or query failure.
    #[tracing::instrument(skip(self, params), fields(tenant_id))]
    pub async fn execute_labels(&self, tenant_id: String, params: &LabelsParams) -> PrometheusResult<Vec<String>> {
        let now = Utc::now();
        let start = params.start.as_deref().map_or(now - chrono::Duration::hours(6), |s| parse_time(s));
        let end = params.end.as_deref().map_or(now, |s| parse_time(s));
        let query_ctx = PromQLQueryContext::range(tenant_id, start, end, TimeDelta::minutes(1));

        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);

        let df = planner.plan_labels().await?;
        let batches = df.collect().await.map_err(QueryError::from)?;

        let mut labels = std::collections::BTreeSet::new();
        for batch in &batches {
            if let Some(col) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        labels.insert(col.value(i).to_string());
                    }
                }
            }
        }

        Ok(labels.into_iter().collect())
    }

    /// Execute a label values metadata query.
    ///
    /// Returns distinct values for a specific label from the metrics table.
    ///
    /// # Errors
    ///
    /// Returns `PrometheusError` on session creation or query failure.
    #[tracing::instrument(skip(self, params), fields(tenant_id, label_name))]
    pub async fn execute_label_values(
        &self,
        tenant_id: String,
        label_name: &str,
        params: &LabelValuesParams,
    ) -> PrometheusResult<Vec<String>> {
        let now = Utc::now();
        let start = params.start.as_deref().map_or(now - chrono::Duration::hours(6), |s| parse_time(s));
        let end = params.end.as_deref().map_or(now, |s| parse_time(s));
        let query_ctx = PromQLQueryContext::range(tenant_id, start, end, TimeDelta::minutes(1));

        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);

        let df = planner.plan_label_values(label_name).await?;
        let batches = df.collect().await.map_err(QueryError::from)?;

        let mut values = std::collections::BTreeSet::new();
        for batch in &batches {
            if let Some(col) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        values.insert(col.value(i).to_string());
                    }
                }
            }
        }

        Ok(values.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_time_unix_seconds() {
        let dt = parse_time("1609459200");
        assert_eq!(dt, DateTime::from_timestamp(1_609_459_200, 0).expect("valid timestamp"));
    }

    #[test]
    fn test_parse_time_unix_float() {
        let dt = parse_time("1609459200.5");
        assert_eq!(dt.timestamp(), 1_609_459_200);
        assert_eq!(dt.timestamp_subsec_millis(), 500);
    }

    #[test]
    fn test_parse_time_rfc3339() {
        let dt = parse_time("2021-01-01T00:00:00Z");
        assert_eq!(dt, DateTime::from_timestamp(1_609_459_200, 0).expect("valid timestamp"));
    }

    #[test]
    fn test_parse_time_invalid_fallback() {
        let dt = parse_time("not-a-time");
        assert_eq!(dt, DateTime::UNIX_EPOCH);
    }

    #[test]
    fn test_parse_step_duration() {
        let step = parse_step("15s").expect("valid step");
        assert_eq!(step, TimeDelta::seconds(15));
    }

    #[test]
    fn test_parse_step_float_seconds() {
        let step = parse_step("60").expect("valid step");
        assert_eq!(step, TimeDelta::seconds(60));
    }

    #[test]
    fn test_parse_step_zero_rejected() {
        assert!(parse_step("0").is_err());
    }

    #[test]
    fn test_parse_step_negative_rejected() {
        assert!(parse_step("-5").is_err());
    }

    #[test]
    fn test_format_float_normal() {
        assert_eq!(format_float(42.5), "42.5");
    }

    #[test]
    fn test_format_float_nan() {
        assert_eq!(format_float(f64::NAN), "NaN");
    }

    #[test]
    fn test_format_float_inf() {
        assert_eq!(format_float(f64::INFINITY), "+Inf");
        assert_eq!(format_float(f64::NEG_INFINITY), "-Inf");
    }
}
