//! DataFusion-based LogQL query planner.

// Allow warnings for stub implementations - these will be fixed as features are implemented
#![allow(
    clippy::unused_self,
    clippy::unused_async,
    clippy::unnecessary_wraps,
    clippy::match_same_arms,
    clippy::let_and_return,
    clippy::option_if_let_else,
    clippy::items_after_statements
)]

use std::{future::Future, pin::Pin};

use datafusion::{
    logical_expr::{col, lit, Expr, LogicalPlan, LogicalPlanBuilder},
    prelude::*,
};

use crate::{
    common::{errors::IceGateError, Result, LOGS_TABLE_FQN},
    query::logql::{
        common::MatchOp,
        expr::LogQLExpr,
        log::{LabelMatcher, LogExpr, Selector},
        metric::MetricExpr,
        planner::{Planner, QueryContext, DEFAULT_LOG_LIMIT},
    },
};

/// A planner that converts `LogQL` expressions into `DataFusion`
/// `LogicalPlans`.
pub struct DataFusionPlanner {
    ctx: SessionContext,
    context: QueryContext,
}

impl DataFusionPlanner {
    /// Creates a new `DataFusionPlanner`.
    pub const fn new(ctx: SessionContext, context: QueryContext) -> Self {
        Self {
            ctx,
            context,
        }
    }
}

impl Planner for DataFusionPlanner {
    type Plan = LogicalPlan;

    async fn plan(&self, expr: LogQLExpr) -> Result<Self::Plan> {
        match expr {
            LogQLExpr::Log(log_expr) => {
                let plan = self.plan_log(log_expr).await?;
                // Apply limit only for log queries (not metrics)
                // Uses context limit or Loki default of 100 entries
                let limit = self.context.limit.unwrap_or(DEFAULT_LOG_LIMIT);
                LogicalPlanBuilder::from(plan)
                    .limit(0, Some(limit))?
                    .build()
                    .map_err(IceGateError::from)
            },
            LogQLExpr::Metric(metric_expr) => self.plan_metric(metric_expr).await,
        }
    }
}

impl DataFusionPlanner {
    async fn plan_log(&self, expr: LogExpr) -> Result<LogicalPlan> {
        // 1. Scan the logs table from iceberg.icegate namespace
        let df = self.ctx.table(LOGS_TABLE_FQN).await?;

        // 2. Apply MANDATORY tenant filter (multi-tenancy isolation)
        // This filter is applied FIRST and cannot be bypassed by user queries.
        // Since tenant_id is the leading partition key, Iceberg will prune
        // non-matching partitions for efficient query execution.
        let df = df.filter(col("tenant_id").eq(lit(&self.context.tenant_id)))?;

        // 3. Apply time range filter
        // The timestamp column is Timestamp(Microsecond)
        // Convert DateTime<Utc> to microseconds for comparison
        let ts_col = col("timestamp");
        let start_micros = self.context.start.timestamp_micros();
        let end_micros = self.context.end.timestamp_micros();
        let start_literal = lit(datafusion::scalar::ScalarValue::TimestampMicrosecond(
            Some(start_micros),
            None,
        ));
        let end_literal = lit(datafusion::scalar::ScalarValue::TimestampMicrosecond(
            Some(end_micros),
            None,
        ));
        let df = df.filter(ts_col.clone().gt_eq(start_literal).and(ts_col.lt_eq(end_literal)))?;

        // 4. Apply Selector matchers
        let df = self.apply_selector(df, expr.selector)?;

        // 5. Apply Pipeline stages
        let df = self.apply_pipeline(df, expr.pipeline)?;

        Ok(df.into_unoptimized_plan())
    }

    fn plan_metric<'a>(&'a self, expr: MetricExpr) -> Pin<Box<dyn Future<Output = Result<LogicalPlan>> + Send + 'a>> {
        Box::pin(async move {
            match expr {
                MetricExpr::RangeAggregation(agg) => self.plan_range_aggregation(agg).await,
                MetricExpr::VectorAggregation(agg) => self.plan_vector_aggregation(agg).await,
                MetricExpr::BinaryOp {
                    op: _op,
                    left,
                    right,
                    modifier: _modifier,
                } => {
                    let _left_plan = self.plan_metric(*left).await?;
                    let _right_plan = self.plan_metric(*right).await?;

                    // TODO: Implement binary operations (vector matching)
                    // This requires joining left and right plans based on labels and timestamp,
                    // applying the operation, and handling the modifier (on/ignoring,
                    // group_left/right).
                    Err(IceGateError::NotImplemented(
                        "Binary operations not yet implemented".to_string(),
                    ))
                },
                MetricExpr::Literal(val) => {
                    // Return a plan with a single row containing the literal value
                    let df = self.ctx.read_empty()?;
                    let df = df.select(vec![lit(val).alias("value")])?;
                    Ok(df.into_unoptimized_plan())
                },
                MetricExpr::Vector(_vals) => {
                    // TODO: Implement vector literal
                    Err(IceGateError::NotImplemented(
                        "Vector literal not yet implemented".to_string(),
                    ))
                },
                MetricExpr::LabelReplace {
                    ..
                } => {
                    // TODO: Implement label replace
                    Err(IceGateError::NotImplemented(
                        "Label replace not yet implemented".to_string(),
                    ))
                },
                MetricExpr::Variable(_) => {
                    Err(IceGateError::NotImplemented("Variable not yet implemented".to_string()))
                },
                MetricExpr::Parens(inner) => self.plan_metric(*inner).await,
            }
        })
    }

    async fn plan_range_aggregation(&self, agg: crate::query::logql::metric::RangeAggregation) -> Result<LogicalPlan> {
        // 1. Plan the inner LogExpr
        let df = self.plan_log(agg.range_expr.log_expr).await?;

        // 2. Apply aggregation
        // For range aggregations like rate({}[5m]), we typically need to:
        // a. Bucketize by time (if step is provided)
        // b. Apply the aggregation function over the window

        // TODO: This is a complex mapping that requires window functions or specific
        // UDAFs. For now, we will return the underlying log stream plan, but we
        // should eventually map `agg.op` (Rate, CountOverTime, etc.) to
        // DataFusion operations.

        // Example placeholder for count_over_time:
        // df.aggregate(vec![col("service_name")], vec![count(col("timestamp"))])?

        Ok(df)
    }

    async fn plan_vector_aggregation(
        &self,
        agg: crate::query::logql::metric::VectorAggregation,
    ) -> Result<LogicalPlan> {
        // 1. Plan the inner MetricExpr
        let df = self.plan_metric(*agg.expr).await?;

        // 2. Identify grouping columns
        // LogQL: sum by (label1, label2) (...)
        // DataFusion: group_expr = [col("label1"), col("label2")]
        let group_exprs = if let Some(grouping) = agg.grouping {
            let labels = match grouping {
                crate::query::logql::common::Grouping::By(labels) => labels,
                crate::query::logql::common::Grouping::Without(labels) => labels, // TODO: Handle 'without' correctly
            };

            labels
                .iter()
                .map(|l| {
                    if self.is_top_level_field(&l.name) {
                        col(&l.name)
                    } else {
                        datafusion::functions::core::get_field().call(vec![col("attributes"), lit(l.name.as_str())])
                    }
                })
                .collect::<Vec<_>>()
        } else {
            vec![]
        };

        // 3. Identify aggregation function
        // LogQL: sum, avg, min, max, count, stddev, stdvar, bottomk, topk
        // We need to map agg.op to DataFusion aggregate functions.
        // For now, we'll implement a few common ones.

        // Note: We assume the inner plan produces a "value" column or similar that we
        // aggregate. Since we don't have a strict schema for the inner metric
        // plan yet, we'll assume a column named "value".
        let value_col = col("value");

        let aggr_expr = match agg.op {
            crate::query::logql::metric::VectorAggregationOp::Sum => {
                datafusion::functions_aggregate::expr_fn::sum(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Avg => {
                datafusion::functions_aggregate::expr_fn::avg(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Min => {
                datafusion::functions_aggregate::expr_fn::min(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Max => {
                datafusion::functions_aggregate::expr_fn::max(value_col)
            },
            crate::query::logql::metric::VectorAggregationOp::Count => {
                datafusion::functions_aggregate::expr_fn::count(value_col)
            },
            // TODO: Implement other operators
            _ => {
                return Err(IceGateError::NotImplemented(format!(
                    "Vector aggregation op {:?} not supported",
                    agg.op
                )));
            },
        };

        // 4. Apply aggregation
        // We need to use the `aggregate` method on the LogicalPlan builder (DataFrame).
        // Since `df` is a LogicalPlan, we might need to wrap it in a DataFrame or use
        // LogicalPlanBuilder. However, `DataFusionPlanner` doesn't hold the
        // full context to create a DataFrame easily without a SessionContext reference
        // that is fully valid. But we can use `LogicalPlanBuilder::from(df)`.

        let builder = LogicalPlanBuilder::from(df);
        let plan = builder
            .aggregate(group_exprs, vec![aggr_expr])
            .map_err(IceGateError::from)?
            .build()
            .map_err(IceGateError::from)?;

        Ok(plan)
    }

    fn apply_selector(&self, df: DataFrame, selector: Selector) -> Result<DataFrame> {
        let mut df = df;
        for matcher in selector.matchers {
            let expr = self.matcher_to_expr(matcher)?;
            df = df.filter(expr)?;
        }
        Ok(df)
    }

    fn matcher_to_expr(&self, matcher: LabelMatcher) -> Result<Expr> {
        let col_expr = if self.is_top_level_field(&matcher.label) {
            col(matcher.label)
        } else {
            // Access attribute from the "attributes" map
            // using get_field for map access
            datafusion::functions::core::get_field().call(vec![col("attributes"), lit(matcher.label)])
        };

        let val = lit(matcher.value);

        match matcher.op {
            MatchOp::Eq => Ok(col_expr.eq(val)),
            MatchOp::Neq => Ok(col_expr.not_eq(val)),
            // Use regex_match function from datafusion-functions
            MatchOp::Re => Ok(datafusion::functions::regex::regexp_match().call(vec![col_expr, val])),
            MatchOp::Nre => Ok(datafusion::functions::regex::regexp_match().call(vec![col_expr, val]).not()),
        }
    }

    fn is_top_level_field(&self, name: &str) -> bool {
        matches!(
            name,
            "tenant_id"
                | "account_id"
                | "service_name"
                | "timestamp"
                | "observed_timestamp"
                | "ingested_timestamp"
                | "trace_id"
                | "span_id"
                | "severity_number"
                | "severity_text"
                | "body"
                | "flags"
                | "dropped_attributes_count"
        )
    }

    fn apply_pipeline(
        &self,
        mut df: DataFrame,
        pipeline: Vec<crate::query::logql::log::PipelineStage>,
    ) -> Result<DataFrame> {
        use crate::query::logql::log::PipelineStage;

        for stage in pipeline {
            df = match stage {
                PipelineStage::LineFilter(filter) => self.apply_line_filter(df, filter)?,
                PipelineStage::LogParser(parser) => self.apply_parser(df, parser)?,
                PipelineStage::LabelFormat(ops) => self.apply_label_format(df, ops)?,
                PipelineStage::LineFormat(_template) => {
                    // TODO: Implement line_format using template engine
                    df
                },
                PipelineStage::Decolorize => self.apply_decolorize(df)?,
                PipelineStage::Drop(labels) => self.apply_drop(df, labels)?,
                PipelineStage::Keep(labels) => self.apply_keep(df, labels)?,
                PipelineStage::LabelFilter(filter_expr) => self.apply_label_filter(df, filter_expr)?,
            };
        }

        Ok(df)
    }

    fn apply_line_filter(&self, df: DataFrame, filter: crate::query::logql::log::LineFilter) -> Result<DataFrame> {
        use crate::query::logql::log::{LineFilterOp, LineFilterValue};

        let body_col = col("body");
        let mut combined_expr: Option<Expr> = None;

        for filter_value in filter.filters {
            let filter_str = match filter_value {
                LineFilterValue::String(s) => s,
                LineFilterValue::Ip(_cidr) => {
                    return Err(IceGateError::NotImplemented("IP CIDR filtering".into()));
                },
            };

            let expr = match filter.op {
                LineFilterOp::Contains => {
                    datafusion::functions::string::contains().call(vec![body_col.clone(), lit(filter_str)])
                },
                LineFilterOp::NotContains => datafusion::functions::string::contains()
                    .call(vec![body_col.clone(), lit(filter_str)])
                    .not(),
                LineFilterOp::Match => {
                    datafusion::functions::regex::regexp_like().call(vec![body_col.clone(), lit(filter_str)])
                },
                LineFilterOp::NotMatch => datafusion::functions::regex::regexp_like()
                    .call(vec![body_col.clone(), lit(filter_str)])
                    .not(),
                LineFilterOp::NotPattern => {
                    return Err(IceGateError::NotImplemented("pattern matching filter".into()));
                },
            };

            combined_expr = Some(match combined_expr {
                Some(existing) => existing.and(expr),
                None => expr,
            });
        }

        match combined_expr {
            Some(expr) => df.filter(expr).map_err(IceGateError::from),
            None => Ok(df),
        }
    }

    fn apply_parser(&self, df: DataFrame, parser: crate::query::logql::log::LogParser) -> Result<DataFrame> {
        use crate::query::logql::log::LogParser;

        // For parsers, we typically invoke a UDF that extracts attributes from the log
        // body and merges them into the attributes map.
        // Since DataFusion doesn't support "merge into map" easily in a single
        // expression without complex UDFs, we'll assume the UDF returns a
        // Map/Struct and we might need to project it. For now, we'll just
        // invoke the UDF and project the result as "attributes" (merging is complex).
        // A real implementation would likely use a specific "extract_and_merge" UDF.

        let _body_col = col("body");

        match parser {
            LogParser::Json(_fields) => {
                // Call json_parser UDF
                // let udf = self.ctx.udf("json_parser")?;
                // let args = vec![body_col];
                // let expr = udf.call(args);
                // For now, we'll just return df as we don't have the UDF registered
                // TODO: Implement JSON parsing
                Ok(df)
            },
            LogParser::Logfmt {
                ..
            } => {
                // TODO: Implement Logfmt parsing
                Ok(df)
            },
            LogParser::Regexp(_pattern) => {
                // TODO: Implement Regexp parsing
                Ok(df)
            },
            LogParser::Pattern(_pattern) => {
                // TODO: Implement Pattern parsing
                Ok(df)
            },
            LogParser::Unpack => {
                // TODO: Implement Unpack
                Ok(df)
            },
        }
    }

    fn apply_label_format(
        &self,
        df: DataFrame,
        ops: Vec<crate::query::logql::common::LabelFormatOp>,
    ) -> Result<DataFrame> {
        use crate::query::logql::common::LabelFormatOp;

        for op in ops {
            match op {
                LabelFormatOp::Rename {
                    ..
                } => {
                    // TODO: Implement label rename
                    // Rename is essentially projecting the src column as dst
                },
                LabelFormatOp::Template {
                    ..
                } => {
                    // TODO: Implement label template
                },
            }
        }
        Ok(df)
    }

    const fn apply_decolorize(&self, df: DataFrame) -> Result<DataFrame> {
        // Call decolorize UDF on body
        // let udf = self.ctx.udf("decolorize")?;
        // let expr = udf.call(vec![col("body")]);
        // df.select(vec![expr.alias("body"), col("timestamp"), ...])
        // TODO: Implement decolorize
        Ok(df)
    }

    fn apply_drop(
        &self,
        df: DataFrame,
        _labels: Vec<crate::query::logql::common::LabelExtraction>,
    ) -> Result<DataFrame> {
        // Drop columns. In DataFusion, we select all EXCEPT the dropped ones.
        // But we can only drop top-level columns easily. Attributes map modification is
        // harder. TODO: Implement drop
        Ok(df)
    }

    fn apply_keep(
        &self,
        df: DataFrame,
        _labels: Vec<crate::query::logql::common::LabelExtraction>,
    ) -> Result<DataFrame> {
        // Keep only specified columns.
        // TODO: Implement keep
        Ok(df)
    }

    fn apply_label_filter(
        &self,
        df: DataFrame,
        filter_expr: crate::query::logql::log::LabelFilterExpr,
    ) -> Result<DataFrame> {
        let expr = self.label_filter_to_expr(filter_expr)?;
        df.filter(expr).map_err(IceGateError::from)
    }

    fn label_filter_to_expr(&self, filter: crate::query::logql::log::LabelFilterExpr) -> Result<Expr> {
        use crate::query::logql::log::LabelFilterExpr;

        match filter {
            LabelFilterExpr::And(left, right) => {
                let left_expr = self.label_filter_to_expr(*left)?;
                let right_expr = self.label_filter_to_expr(*right)?;
                Ok(left_expr.and(right_expr))
            },
            LabelFilterExpr::Or(left, right) => {
                let left_expr = self.label_filter_to_expr(*left)?;
                let right_expr = self.label_filter_to_expr(*right)?;
                Ok(left_expr.or(right_expr))
            },
            LabelFilterExpr::Parens(inner) => self.label_filter_to_expr(*inner),
            LabelFilterExpr::Matcher(matcher) => self.matcher_to_expr(matcher),
            LabelFilterExpr::Number {
                label,
                op,
                value,
            } => {
                let col_expr = if self.is_top_level_field(&label) {
                    col(label)
                } else {
                    datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
                };

                use crate::query::logql::common::ComparisonOp;
                let expr = match op {
                    ComparisonOp::Gt => col_expr.gt(lit(value)),
                    ComparisonOp::Ge => col_expr.gt_eq(lit(value)),
                    ComparisonOp::Lt => col_expr.lt(lit(value)),
                    ComparisonOp::Le => col_expr.lt_eq(lit(value)),
                    ComparisonOp::Eq => col_expr.eq(lit(value)),
                    ComparisonOp::Neq => col_expr.not_eq(lit(value)),
                };
                Ok(expr)
            },
            LabelFilterExpr::Duration {
                label,
                op,
                value,
            } => {
                // Convert duration to nanoseconds and compare
                let col_expr = if self.is_top_level_field(&label) {
                    col(label)
                } else {
                    datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
                };

                use crate::query::logql::common::ComparisonOp;
                let nanos = value.as_nanos();
                let expr = match op {
                    ComparisonOp::Gt => col_expr.gt(lit(nanos)),
                    ComparisonOp::Ge => col_expr.gt_eq(lit(nanos)),
                    ComparisonOp::Lt => col_expr.lt(lit(nanos)),
                    ComparisonOp::Le => col_expr.lt_eq(lit(nanos)),
                    ComparisonOp::Eq => col_expr.eq(lit(nanos)),
                    ComparisonOp::Neq => col_expr.not_eq(lit(nanos)),
                };
                Ok(expr)
            },
            LabelFilterExpr::Bytes {
                label,
                op,
                value,
            } => {
                // Compare byte values as u64
                let col_expr = if self.is_top_level_field(&label) {
                    col(label)
                } else {
                    datafusion::functions::core::get_field().call(vec![col("attributes"), lit(label)])
                };

                use crate::query::logql::common::ComparisonOp;
                let expr = match op {
                    ComparisonOp::Gt => col_expr.gt(lit(value)),
                    ComparisonOp::Ge => col_expr.gt_eq(lit(value)),
                    ComparisonOp::Lt => col_expr.lt(lit(value)),
                    ComparisonOp::Le => col_expr.lt_eq(lit(value)),
                    ComparisonOp::Eq => col_expr.eq(lit(value)),
                    ComparisonOp::Neq => col_expr.not_eq(lit(value)),
                };
                Ok(expr)
            },
            LabelFilterExpr::Ip {
                ..
            } => {
                // TODO: Implement IP filtering using ip_match UDF
                Err(IceGateError::NotImplemented(
                    "IP filtering not yet implemented".to_string(),
                ))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use datafusion::prelude::SessionContext;
    use iceberg_datafusion::IcebergCatalogProvider;

    use super::*;
    use crate::{
        common::{
            catalog::CatalogBuilder, schema::logs_schema, CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, LOGS_TABLE,
        },
        query::logql::{
            common::MatchOp,
            log::{LabelMatcher, LineFilter, LogExpr, PipelineStage, Selector},
        },
    };

    async fn create_test_context() -> (SessionContext, QueryContext) {
        let ctx = SessionContext::new();

        // Create a memory Iceberg catalog for testing with a temporary warehouse path
        let warehouse_path = tempfile::tempdir().expect("Failed to create temp dir");
        let warehouse_str = warehouse_path.path().to_str().unwrap().to_string();

        let config = CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: warehouse_str,
            properties: std::collections::HashMap::new(),
        };

        let iceberg_catalog = CatalogBuilder::from_config(&config)
            .await
            .expect("Failed to create test catalog");

        // Create the namespace and table
        let namespace = iceberg::NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
        if !iceberg_catalog.namespace_exists(&namespace).await.unwrap_or(false) {
            iceberg_catalog
                .create_namespace(&namespace, std::collections::HashMap::new())
                .await
                .expect("Failed to create namespace");
        }

        // Create logs table using the common schema
        let schema = logs_schema().expect("Failed to get logs schema");
        let table_creation = iceberg::TableCreation::builder()
            .name(LOGS_TABLE.to_string())
            .schema(schema)
            .build();

        let _ = iceberg_catalog.create_table(&namespace, table_creation).await;

        // Register Iceberg catalog with DataFusion
        let iceberg_provider = IcebergCatalogProvider::try_new(iceberg_catalog)
            .await
            .expect("Failed to create IcebergCatalogProvider");
        ctx.register_catalog("iceberg", Arc::new(iceberg_provider));

        let context = QueryContext {
            tenant_id: "test-tenant".to_string(),
            start: Utc.timestamp_opt(0, 0).unwrap(),
            end: Utc.timestamp_opt(100, 0).unwrap(), // 100 seconds from epoch
            limit: None,
            step: None,
        };

        (ctx, context)
    }

    #[tokio::test]
    async fn test_selector_planning() {
        let (ctx, context) = create_test_context().await;
        let planner = DataFusionPlanner::new(ctx, context);

        let selector = Selector::new(vec![
            LabelMatcher::new("service_name", MatchOp::Eq, "frontend"),
            LabelMatcher::new("severity_text", MatchOp::Neq, "error"),
        ]);
        let expr = LogExpr::new(selector);

        let plan = planner.plan_log(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        // Verify filter expressions are present
        // Display format: Filter: iceberg.icegate.logs.service_name = Utf8("frontend")
        assert!(
            display.contains("iceberg.icegate.logs.service_name = Utf8(\"frontend\")")
                && display.contains("iceberg.icegate.logs.severity_text != Utf8(\"error\")"),
            "Plan does not contain expected filters:\n{display}"
        );
    }

    #[tokio::test]
    async fn test_selector_attribute_access() {
        let (ctx, context) = create_test_context().await;
        let planner = DataFusionPlanner::new(ctx, context);

        let selector = Selector::new(vec![LabelMatcher::new("custom_attr", MatchOp::Eq, "value")]);
        let expr = LogExpr::new(selector);

        let plan = planner.plan_log(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        assert!(
            display.contains("get_field(iceberg.icegate.logs.attributes, Utf8(\"custom_attr\"))"),
            "Plan does not contain expected attribute access:\n{display}"
        );
    }

    #[tokio::test]
    async fn test_line_filter_contains() {
        let (ctx, context) = create_test_context().await;
        let planner = DataFusionPlanner::new(ctx, context);

        let selector = Selector::new(vec![]);
        let mut expr = LogExpr::new(selector);
        expr.pipeline.push(PipelineStage::LineFilter(LineFilter::contains("error")));

        let plan = planner.plan_log(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        assert!(
            display.contains("contains(iceberg.icegate.logs.body, Utf8(\"error\"))"),
            "Plan does not contain expected contains filter:\n{display}"
        );
    }

    #[tokio::test]
    async fn test_line_filter_not_contains() {
        let (ctx, context) = create_test_context().await;
        let planner = DataFusionPlanner::new(ctx, context);

        let selector = Selector::new(vec![]);
        let mut expr = LogExpr::new(selector);
        expr.pipeline.push(PipelineStage::LineFilter(LineFilter::not_contains("info")));

        let plan = planner.plan_log(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        assert!(
            display.contains("NOT contains(iceberg.icegate.logs.body, Utf8(\"info\"))"),
            "Plan does not contain expected NOT contains filter:\n{display}"
        );
    }

    #[tokio::test]
    async fn test_line_filter_regex() {
        let (ctx, context) = create_test_context().await;
        let planner = DataFusionPlanner::new(ctx, context);

        let selector = Selector::new(vec![]);
        let mut expr = LogExpr::new(selector);
        expr.pipeline.push(PipelineStage::LineFilter(LineFilter::matches("error.*")));

        let plan = planner.plan_log(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        assert!(
            display.contains("regexp_like(iceberg.icegate.logs.body, Utf8(\"error.*\"))"),
            "Plan does not contain expected regexp_like filter:\n{display}"
        );
    }

    #[tokio::test]
    async fn test_line_filter_not_regex() {
        let (ctx, context) = create_test_context().await;
        let planner = DataFusionPlanner::new(ctx, context);

        let selector = Selector::new(vec![]);
        let mut expr = LogExpr::new(selector);
        expr.pipeline
            .push(PipelineStage::LineFilter(LineFilter::not_matches("debug.*")));

        let plan = planner.plan_log(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        assert!(
            display.contains("NOT regexp_like(iceberg.icegate.logs.body, Utf8(\"debug.*\"))"),
            "Plan does not contain expected NOT regexp_like filter:\n{display}"
        );
    }

    #[tokio::test]
    async fn test_metric_literal_planning() {
        let (ctx, context) = create_test_context().await;
        let planner = DataFusionPlanner::new(ctx, context);

        let expr = MetricExpr::Literal(42.0);
        let plan = planner.plan_metric(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        // Verify literal projection
        // Should contain a projection with alias "value" and the literal value
        assert!(
            display.contains("42 AS value") || display.contains("Float64(42) AS value"),
            "Plan does not contain expected literal projection:\n{display}"
        );
    }

    #[tokio::test]
    async fn test_log_query_default_limit() {
        use crate::query::logql::planner::DEFAULT_LOG_LIMIT;

        let (ctx, context) = create_test_context().await;
        let planner = DataFusionPlanner::new(ctx, context);

        let selector = Selector::new(vec![]);
        let log_expr = LogExpr::new(selector);
        let expr = LogQLExpr::Log(log_expr);

        let plan = planner.plan(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        // Verify limit node is present with default value (100)
        let expected_limit = format!("Limit: skip=0, fetch={DEFAULT_LOG_LIMIT}");
        assert!(
            display.contains(&expected_limit),
            "Plan does not contain expected limit node '{expected_limit}':\n{display}"
        );
    }

    #[tokio::test]
    async fn test_log_query_custom_limit() {
        let (ctx, mut context) = create_test_context().await;
        context.limit = Some(50);
        let planner = DataFusionPlanner::new(ctx, context);

        let selector = Selector::new(vec![]);
        let log_expr = LogExpr::new(selector);
        let expr = LogQLExpr::Log(log_expr);

        let plan = planner.plan(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        // Verify limit node uses custom value
        assert!(
            display.contains("Limit: skip=0, fetch=50"),
            "Plan does not contain expected limit 'Limit: skip=0, fetch=50':\n{display}"
        );
    }

    #[tokio::test]
    async fn test_metric_query_no_limit() {
        let (ctx, context) = create_test_context().await;
        let planner = DataFusionPlanner::new(ctx, context);

        let expr = LogQLExpr::Metric(MetricExpr::Literal(42.0));
        let plan = planner.plan(expr).await.expect("Planning failed");
        let display = plan.display_indent().to_string();

        // Verify no limit node for metric queries
        assert!(
            !display.contains("Limit:"),
            "Metric query should not have limit node:\n{display}"
        );
    }
}
