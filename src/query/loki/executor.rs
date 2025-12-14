//! Query execution logic for Loki API.
//!
//! Extracts the common query execution flow from handlers, including
//! parsing, planning, and execution of `LogQL` queries.

use std::{sync::Arc, time::Instant};

use chrono::{DateTime, Duration, Utc};
use datafusion::prelude::SessionContext;

use super::{
    error::LokiResult,
    formatters::{batches_to_loki_matrix, batches_to_loki_streams, batches_to_series_list, extract_string_column},
    models::{QueryResultData, RangeQueryParams},
};
use crate::{
    common::errors::IceGateError,
    query::{
        engine::QueryEngine,
        logql::{
            antlr::AntlrParser, datafusion::DataFusionPlanner, duration::parse_duration_opt, expr::LogQLExpr,
            log::Selector, planner::QueryContext, Parser, Planner,
        },
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
        Ok(_) => Err(IceGateError::Validation(
            "Expected log selector, got metric query".to_string(),
        )),
        Err(e) => Err(e),
    }
}

// ============================================================================
// Query Executor
// ============================================================================

/// Query executor for Loki API operations.
///
/// Encapsulates the common flow of creating sessions, parsing queries,
/// planning, and executing against the query engine.
pub struct QueryExecutor {
    engine: Arc<QueryEngine>,
}

impl QueryExecutor {
    /// Create a new query executor.
    pub const fn new(engine: Arc<QueryEngine>) -> Self {
        Self {
            engine,
        }
    }

    /// Create a DataFusion session context.
    pub async fn create_session(&self) -> LokiResult<SessionContext> {
        self.engine.create_session().await
    }

    /// Execute a range query and return formatted results.
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
        };

        // Parse LogQL
        let parser = AntlrParser::new();
        let expr = parser.parse(&params.query)?;

        // Track query type before planning consumes the expression
        let is_metric_query = expr.is_metric();

        // Create session and plan
        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);
        let df = planner.plan(expr).await?;

        // Execute
        let batches = df.collect().await?;

        let exec_time = exec_start.elapsed().as_secs_f64();

        // Format results based on query type
        let formatted = if is_metric_query {
            batches_to_loki_matrix(&batches)
        } else {
            batches_to_loki_streams(&batches)
        };

        let stats = formatted.to_stats(exec_time);
        let result_data = QueryResultData {
            result_type: formatted.result_type,
            result: formatted.result,
            stats,
        };

        Ok(result_data)
    }

    /// Execute a labels metadata query.
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

        // Parse optional selector
        let selector = match params.query.as_ref() {
            Some(q) => parse_selector(q)?,
            None => Selector::empty(),
        };

        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);

        let df = planner.plan_labels(selector).await?;
        let batches = df.collect().await?;

        let mut labels: std::collections::HashSet<String> = std::collections::HashSet::new();
        for value in extract_string_column(&batches, 0) {
            labels.insert(value);
        }

        let mut result: Vec<String> = labels.into_iter().collect();
        result.sort();

        Ok(result)
    }

    /// Execute a label values metadata query.
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

        // Parse optional selector
        let selector = match params.query.as_ref() {
            Some(q) => parse_selector(q)?,
            None => Selector::empty(),
        };

        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);

        let df = planner.plan_label_values(selector, label_name).await?;
        let batches = df.collect().await?;

        Ok(extract_string_column(&batches, 0))
    }

    /// Execute a series metadata query.
    pub async fn execute_series(
        &self,
        tenant_id: String,
        params: &super::models::SeriesQueryParams,
    ) -> LokiResult<Vec<std::collections::HashMap<String, String>>> {
        if params.matchers.is_empty() {
            return Err(IceGateError::Validation("match[] parameter required".to_string()));
        }

        let query_ctx = build_query_context(tenant_id, params.start.as_ref(), params.end.as_ref(), None);

        // Parse all matchers
        let selectors: Vec<Selector> = params
            .matchers
            .iter()
            .map(|m| parse_selector(m))
            .collect::<LokiResult<Vec<_>>>()?;

        let session_ctx = self.create_session().await?;
        let planner = DataFusionPlanner::new(session_ctx, query_ctx);

        let df = planner.plan_series(&selectors).await?;
        let batches = df.collect().await?;

        Ok(batches_to_series_list(&batches))
    }
}
