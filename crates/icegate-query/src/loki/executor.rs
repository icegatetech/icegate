//! Query execution logic for Loki API.
//!
//! Extracts the common query execution flow from handlers, including
//! parsing, planning, and execution of `LogQL` queries.

use std::{sync::Arc, time::Instant};

use chrono::{DateTime, Duration, Utc};
use datafusion::{
    arrow::array::RecordBatch,
    prelude::{DataFrame, SessionContext},
};

use super::{
    error::{LokiError, LokiResult},
    formatters::{batches_to_loki_matrix, batches_to_loki_streams, batches_to_series_list, extract_string_column},
    models::{QueryResultData, RangeQueryParams},
};
use crate::{
    engine::QueryEngine,
    error::{ParseError, QueryError},
    infra::metrics::QueryMetrics,
    logql::{
        Parser, Planner,
        antlr::AntlrParser,
        datafusion::DataFusionPlanner,
        duration::parse_duration_opt,
        expr::LogQLExpr,
        log::Selector,
        planner::{QueryContext, SortDirection},
    },
};

// ============================================================================
// Time Parsing
// ============================================================================

/// Parse time string (nanoseconds or RFC3339).
pub fn parse_time(s: &str) -> DateTime<Utc> {
    s.parse::<i64>().map_or_else(
        |_| {
            DateTime::parse_from_rfc3339(s)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or(DateTime::UNIX_EPOCH)
        },
        DateTime::from_timestamp_nanos,
    )
}

/// Build `QueryContext` from request parameters.
///
/// Handles time range parsing with support for `start`, `end`, and `since`
/// parameters. Defaults to 6 hours if no time range is specified.
pub fn build_query_context(
    tenant_id: String,
    start: Option<&String>,
    end: Option<&String>,
    since: Option<&String>,
) -> QueryContext {
    let now = Utc::now();
    let end_time = end.map_or(now, |s| parse_time(s));
    let start_time = since
        .and_then(|s| parse_duration_opt(s))
        .map(|d| end_time - d)
        .or_else(|| start.map(|s| parse_time(s)))
        .unwrap_or(now - Duration::hours(6));

    QueryContext {
        tenant_id,
        start: start_time,
        end: end_time,
        limit: None,
        step: None,
        direction: SortDirection::default(),
    }
}

// ============================================================================
// Selector Parsing
// ============================================================================

/// Parse a `LogQL` selector string into a `Selector`.
pub fn parse_selector(query: &str) -> LokiResult<Selector> {
    let parser = AntlrParser::new();
    match parser.parse(query) {
        Ok(LogQLExpr::Log(log_expr)) => Ok(log_expr.selector),
        Ok(_) => Err(QueryError::Validation("Expected log selector, got metric query".to_string()).into()),
        Err(e) => Err(e.into()),
    }
}

// ============================================================================
// spawn_blocking helpers
// ============================================================================

/// Strip non-Send `ANTLRError` details from [`QueryError`], keeping human-readable messages.
///
/// Required for crossing `spawn_blocking` boundaries since `ANTLRError` contains `Rc`.
fn make_query_error_send(err: QueryError) -> QueryError {
    match err {
        QueryError::Parse(errors) => {
            QueryError::Parse(errors.into_iter().map(|e| ParseError { antlr_error: None, ..e }).collect())
        }
        other => other,
    }
}

/// Map a [`tokio::task::JoinError`] from `spawn_blocking` into a [`LokiError`].
///
/// Takes by value because `map_err` passes ownership.
#[allow(clippy::needless_pass_by_value)]
fn join_error(e: tokio::task::JoinError) -> LokiError {
    LokiError(QueryError::Internal(format!("task panicked: {e}")))
}

// ============================================================================
// Query Executor
// ============================================================================

/// Query executor for Loki API operations.
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

    /// Create a DataFusion session context (timed), counting errors via metrics.
    async fn create_session(&self) -> LokiResult<SessionContext> {
        let start = Instant::now();
        let session = self.engine.create_session().await.map_err(|e| {
            self.metrics.record_session_create_duration(start.elapsed());
            self.metrics.add_error("loki", "session");
            e
        })?;
        self.metrics.record_session_create_duration(start.elapsed());
        Ok(session)
    }

    /// Plan a query and execute it, recording metrics for both phases.
    ///
    /// Times the plan future and `df.collect()` execution, recording durations,
    /// errors, and result row counts via [`QueryMetrics`].
    async fn plan_and_execute<F>(&self, plan_type: &str, plan_future: F) -> LokiResult<Vec<RecordBatch>>
    where
        F: std::future::Future<Output = crate::error::Result<DataFrame>>,
    {
        let plan_start = Instant::now();
        let df = match plan_future.await {
            Ok(df) => {
                self.metrics.record_plan_duration(plan_start.elapsed(), "loki", plan_type);
                df
            }
            Err(e) => {
                self.metrics.record_plan_duration(plan_start.elapsed(), "loki", plan_type);
                self.metrics.add_error("loki", "plan");
                return Err(e.into());
            }
        };

        let execute_start = Instant::now();
        let batches = match df.collect().await {
            Ok(batches) => {
                self.metrics.record_execute_duration(execute_start.elapsed(), "loki", plan_type);
                batches
            }
            Err(e) => {
                self.metrics.record_execute_duration(execute_start.elapsed(), "loki", plan_type);
                self.metrics.add_error("loki", "execute");
                return Err(QueryError::from(e).into());
            }
        };

        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        self.metrics.record_result_rows(total_rows, "loki", plan_type);

        Ok(batches)
    }

    /// Parse an optional selector string on a blocking thread.
    ///
    /// Returns [`Selector::empty()`] when `query` is `None`.
    async fn parse_selector_opt(&self, query: Option<String>) -> LokiResult<Selector> {
        match query {
            Some(q) => {
                let span = tracing::Span::current();
                tokio::task::spawn_blocking(move || {
                    span.in_scope(|| parse_selector(&q).map_err(|e| LokiError(make_query_error_send(e.0))))
                })
                .await
                .map_err(join_error)?
            }
            None => Ok(Selector::empty()),
        }
    }

    /// Execute a range query and return formatted results.
    #[tracing::instrument(skip(self, params), fields(tenant_id, query = %params.query))]
    pub async fn execute_range_query(
        &self,
        tenant_id: String,
        params: &RangeQueryParams,
    ) -> LokiResult<QueryResultData> {
        let exec_start = Instant::now();

        let now = Utc::now();
        let start = params.start.as_ref().map_or(now - Duration::hours(1), |s| parse_time(s));
        let end = params.end.as_ref().map_or(now, |s| parse_time(s));

        let query_ctx = QueryContext {
            tenant_id,
            start,
            end,
            limit: params.limit.map(|l| l as usize),
            step: params.step.as_ref().and_then(|s| parse_duration_opt(s)),
            direction: match params.direction.as_deref() {
                Some("forward") => SortDirection::Forward,
                _ => SortDirection::Backward, // Default per Loki spec
            },
        };

        // Parse LogQL (CPU-bound ANTLR parse offloaded to blocking thread)
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
                self.metrics.record_parse_duration(parse_start.elapsed(), "loki");
                expr
            }
            Err(e) => {
                self.metrics.record_parse_duration(parse_start.elapsed(), "loki");
                self.metrics.add_error("loki", "parse");
                return Err(e.into());
            }
        };

        // Track query type before planning consumes the expression
        let is_metric_query = expr.is_metric();
        let plan_type = if is_metric_query { "metric" } else { "log" };

        // Create session, plan, and execute
        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);
        let batches = self.plan_and_execute(plan_type, planner.plan(expr)).await?;

        let exec_time = exec_start.elapsed().as_secs_f64();

        // Format results based on query type (CPU-bound, offloaded to blocking thread)
        let format_start = Instant::now();
        let span = tracing::Span::current();
        let formatted = tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                if is_metric_query {
                    batches_to_loki_matrix(&batches)
                } else {
                    batches_to_loki_streams(&batches)
                }
            })
        })
        .await
        .map_err(join_error)?;
        let result_type_str = if is_metric_query { "matrix" } else { "streams" };
        self.metrics
            .record_format_duration(format_start.elapsed(), "loki", result_type_str);
        self.metrics.record_result_bytes(formatted.total_bytes, "loki", plan_type);

        let stats = formatted.to_stats(exec_time);
        let result_data = QueryResultData {
            result_type: formatted.result_type,
            result: formatted.result,
            stats,
        };

        Ok(result_data)
    }

    /// Execute a labels metadata query.
    #[tracing::instrument(skip(self, params), fields(tenant_id))]
    pub async fn execute_labels(
        &self,
        tenant_id: String,
        params: &super::models::LabelsQueryParams,
    ) -> LokiResult<Vec<String>> {
        let query_ctx = build_query_context(
            tenant_id,
            params.start.as_ref(),
            params.end.as_ref(),
            params.since.as_ref(),
        );

        // Parse optional selector (CPU-bound, offloaded to blocking thread)
        let selector = self.parse_selector_opt(params.query.clone()).await?;

        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);
        let batches = self.plan_and_execute("labels", planner.plan_labels(selector)).await?;

        // Dedup and sort labels via BTreeSet (CPU-bound, offloaded to blocking thread)
        let span = tracing::Span::current();
        let result = tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                let labels: std::collections::BTreeSet<String> =
                    extract_string_column(&batches, 0).into_iter().collect();
                labels.into_iter().collect()
            })
        })
        .await
        .map_err(join_error)?;

        Ok(result)
    }

    /// Execute a label values metadata query.
    #[tracing::instrument(skip(self, params), fields(tenant_id, label_name))]
    pub async fn execute_label_values(
        &self,
        tenant_id: String,
        label_name: &str,
        params: &super::models::LabelValuesQueryParams,
    ) -> LokiResult<Vec<String>> {
        let query_ctx = build_query_context(
            tenant_id,
            params.start.as_ref(),
            params.end.as_ref(),
            params.since.as_ref(),
        );

        // Parse optional selector (CPU-bound, offloaded to blocking thread)
        let selector = self.parse_selector_opt(params.query.clone()).await?;

        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);
        let batches = self
            .plan_and_execute("label_values", planner.plan_label_values(selector, label_name))
            .await?;

        // CPU-bound column extraction offloaded to blocking thread
        let span = tracing::Span::current();
        let result = tokio::task::spawn_blocking(move || span.in_scope(|| extract_string_column(&batches, 0)))
            .await
            .map_err(join_error)?;

        Ok(result)
    }

    /// Execute a series metadata query.
    #[tracing::instrument(skip(self, params), fields(tenant_id))]
    pub async fn execute_series(
        &self,
        tenant_id: String,
        params: &super::models::SeriesQueryParams,
    ) -> LokiResult<Vec<std::collections::HashMap<String, String>>> {
        if params.matchers.is_empty() {
            return Err(QueryError::Validation("match[] parameter required".to_string()).into());
        }

        let query_ctx = build_query_context(tenant_id, params.start.as_ref(), params.end.as_ref(), None);

        // Parse all matchers (CPU-bound, offloaded to blocking thread)
        let matchers = params.matchers.clone();
        let span = tracing::Span::current();
        let selectors: Vec<Selector> = tokio::task::spawn_blocking(move || {
            span.in_scope(|| {
                matchers
                    .iter()
                    .map(|m| parse_selector(m).map_err(|e| LokiError(make_query_error_send(e.0))))
                    .collect::<LokiResult<Vec<_>>>()
            })
        })
        .await
        .map_err(join_error)??;

        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);
        let batches = self.plan_and_execute("series", planner.plan_series(&selectors)).await?;

        // Format series (CPU-bound, offloaded to blocking thread)
        let format_start = Instant::now();
        let span = tracing::Span::current();
        let result = tokio::task::spawn_blocking(move || span.in_scope(|| batches_to_series_list(&batches)))
            .await
            .map_err(join_error)?;
        self.metrics.record_format_duration(format_start.elapsed(), "loki", "series");

        Ok(result)
    }
}
