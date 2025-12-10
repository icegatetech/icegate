//! Tests for DataFusion-based `LogQL` query planner.

use std::sync::Arc;

use chrono::{TimeDelta, TimeZone, Utc};
use datafusion::{
    logical_expr::{
        logical_plan::{Filter, Limit, Projection},
        BinaryExpr, Expr, JoinType, LogicalPlan, Operator,
    },
    prelude::{DataFrame, SessionContext},
    scalar::ScalarValue,
};
use iceberg_datafusion::IcebergCatalogProvider;

use super::planner::DataFusionPlanner;

/// Extract `LogicalPlan` from `DataFrame` for test assertions.
fn get_logical_plan(df: &DataFrame) -> &LogicalPlan {
    df.logical_plan()
}
use crate::{
    common::{
        catalog::CatalogBuilder, schema::logs_schema, CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, LOGS_TABLE,
    },
    query::logql::{
        common::MatchOp,
        expr::LogQLExpr,
        log::{LabelMatcher, LineFilter, LogExpr, PipelineStage, Selector},
        metric::MetricExpr,
        planner::{Planner, QueryContext, DEFAULT_LOG_LIMIT},
    },
};

// ============================================================================
// Plan Node Helpers
// ============================================================================

/// Extract Filter, panic with context on mismatch.
#[allow(dead_code)]
fn unwrap_filter(plan: &LogicalPlan) -> &Filter {
    match plan {
        LogicalPlan::Filter(f) => f,
        other => panic!("Expected Filter, got: {}", other.display_indent()),
    }
}

/// Extract Limit, panic with context on mismatch.
fn unwrap_limit(plan: &LogicalPlan) -> &Limit {
    match plan {
        LogicalPlan::Limit(l) => l,
        other => panic!("Expected Limit, got: {}", other.display_indent()),
    }
}

/// Extract Projection, panic with context on mismatch.
#[allow(dead_code)]
fn unwrap_projection(plan: &LogicalPlan) -> &Projection {
    match plan {
        LogicalPlan::Projection(p) => p,
        other => panic!("Expected Projection, got: {}", other.display_indent()),
    }
}

/// Collect all Filter nodes from plan tree.
fn collect_filters(plan: &LogicalPlan) -> Vec<&Filter> {
    let mut filters = Vec::new();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if let LogicalPlan::Filter(f) = node {
            filters.push(f);
        }
        stack.extend(node.inputs());
    }
    filters
}

/// Find first Limit in plan tree.
fn find_limit(plan: &LogicalPlan) -> Option<&Limit> {
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if let LogicalPlan::Limit(l) = node {
            return Some(l);
        }
        stack.extend(node.inputs());
    }
    None
}

/// Find first Projection in plan tree.
fn find_projection(plan: &LogicalPlan) -> Option<&Projection> {
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if let LogicalPlan::Projection(p) = node {
            return Some(p);
        }
        stack.extend(node.inputs());
    }
    None
}

/// Collect all Projection nodes from plan tree.
fn collect_projections(plan: &LogicalPlan) -> Vec<&Projection> {
    let mut projections = Vec::new();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if let LogicalPlan::Projection(p) = node {
            projections.push(p);
        }
        stack.extend(node.inputs());
    }
    projections
}

// ============================================================================
// Expr Pattern Matching Helpers
// ============================================================================

/// Check column name (handles qualified: `iceberg.icegate.logs.X`).
fn is_column_named(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Column(col) => col.name == name || col.name.ends_with(&format!(".{name}")),
        _ => false,
    }
}

/// Check string literal value.
fn is_literal_str(expr: &Expr, value: &str) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => s == value,
        _ => false,
    }
}

/// Check f64 literal value.
fn is_literal_f64(expr: &Expr, value: f64) -> bool {
    match expr {
        Expr::Literal(ScalarValue::Float64(Some(v)), _) => (*v - value).abs() < f64::EPSILON,
        _ => false,
    }
}

/// Extract usize from literal (for Limit fetch/skip).
fn get_literal_usize(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => usize::try_from(*v).ok(),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => usize::try_from(*v).ok(),
        _ => None,
    }
}

/// Check if `BinaryExpr` with given operator.
fn is_binary_op(expr: &Expr, expected_op: Operator) -> Option<(&Expr, &Expr)> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op,
            right,
        }) if *op == expected_op => Some((left.as_ref(), right.as_ref())),
        _ => None,
    }
}

/// Check if `ScalarFunction` with given name.
fn is_scalar_function<'a>(expr: &'a Expr, fn_name: &str) -> Option<&'a Vec<Expr>> {
    match expr {
        Expr::ScalarFunction(sf) if sf.func.name() == fn_name => Some(&sf.args),
        _ => None,
    }
}

/// Check if NOT expression.
fn is_negated(expr: &Expr) -> Option<&Expr> {
    match expr {
        Expr::Not(inner) => Some(inner.as_ref()),
        _ => None,
    }
}

/// Check if Alias with given name.
fn is_alias_named<'a>(expr: &'a Expr, name: &str) -> Option<&'a Expr> {
    match expr {
        Expr::Alias(alias) if alias.name == name => Some(alias.expr.as_ref()),
        _ => None,
    }
}


// ============================================================================
// Test Setup
// ============================================================================

async fn create_test_context() -> (SessionContext, QueryContext) {
    let session_ctx = SessionContext::new();

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
    session_ctx.register_catalog("iceberg", Arc::new(iceberg_provider));

    let query_ctx = QueryContext {
        tenant_id: "test-tenant".to_string(),
        start: Utc.timestamp_opt(0, 0).unwrap(),
        end: Utc.timestamp_opt(100, 0).unwrap(), // 100 seconds from epoch
        limit: None,
        step: Some(TimeDelta::seconds(15)), // 15-second step for metric queries
    };

    (session_ctx, query_ctx)
}

// ============================================================================
// Selector Tests
// ============================================================================

#[tokio::test]
async fn test_selector_planning() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![
        LabelMatcher::new("service_name", MatchOp::Eq, "frontend"),
        LabelMatcher::new("severity_text", MatchOp::Neq, "error"),
    ]);
    let log_expr = LogExpr::new(selector);

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let filters = collect_filters(plan);

    // Check: service_name = "frontend"
    let has_service_filter = filters.iter().any(|f| {
        is_binary_op(&f.predicate, Operator::Eq)
            .is_some_and(|(left, right)| is_column_named(left, "service_name") && is_literal_str(right, "frontend"))
    });
    assert!(has_service_filter, "Missing service_name = 'frontend' filter");

    // Check: severity_text != "error"
    let has_severity_filter = filters.iter().any(|f| {
        is_binary_op(&f.predicate, Operator::NotEq)
            .is_some_and(|(left, right)| is_column_named(left, "severity_text") && is_literal_str(right, "error"))
    });
    assert!(has_severity_filter, "Missing severity_text != 'error' filter");
}

#[tokio::test]
async fn test_selector_attribute_access() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("custom_attr", MatchOp::Eq, "value")]);
    let log_expr = LogExpr::new(selector);

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let filters = collect_filters(plan);

    // Check: get_field(attributes, "custom_attr") = "value"
    let has_attr_filter = filters.iter().any(|f| {
        is_binary_op(&f.predicate, Operator::Eq).is_some_and(|(left, right)| {
            is_scalar_function(left, "get_field").is_some_and(|args| {
                args.len() == 2
                    && is_column_named(&args[0], "attributes")
                    && is_literal_str(&args[1], "custom_attr")
                    && is_literal_str(right, "value")
            })
        })
    });
    assert!(
        has_attr_filter,
        "Missing get_field(attributes, 'custom_attr') = 'value' filter"
    );
}

// ============================================================================
// Line Filter Tests
// ============================================================================

#[tokio::test]
async fn test_line_filter_contains() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);
    log_expr.pipeline.push(PipelineStage::LineFilter(LineFilter::contains("error")));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let filters = collect_filters(plan);

    // Check: contains(body, "error")
    let has_contains = filters.iter().any(|f| {
        is_scalar_function(&f.predicate, "contains").is_some_and(|args| {
            args.len() == 2 && is_column_named(&args[0], "body") && is_literal_str(&args[1], "error")
        })
    });
    assert!(has_contains, "Missing contains(body, 'error') filter");
}

#[tokio::test]
async fn test_line_filter_not_contains() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);
    log_expr
        .pipeline
        .push(PipelineStage::LineFilter(LineFilter::not_contains("info")));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let filters = collect_filters(plan);

    // Check: NOT contains(body, "info")
    let has_not_contains = filters.iter().any(|f| {
        is_negated(&f.predicate).is_some_and(|inner| {
            is_scalar_function(inner, "contains").is_some_and(|args| {
                args.len() == 2 && is_column_named(&args[0], "body") && is_literal_str(&args[1], "info")
            })
        })
    });
    assert!(has_not_contains, "Missing NOT contains(body, 'info') filter");
}

#[tokio::test]
async fn test_line_filter_regex() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);
    log_expr
        .pipeline
        .push(PipelineStage::LineFilter(LineFilter::matches("error.*")));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let filters = collect_filters(plan);

    // Check: regexp_like(body, "error.*")
    let has_regex = filters.iter().any(|f| {
        is_scalar_function(&f.predicate, "regexp_like").is_some_and(|args| {
            args.len() == 2 && is_column_named(&args[0], "body") && is_literal_str(&args[1], "error.*")
        })
    });
    assert!(has_regex, "Missing regexp_like(body, 'error.*') filter");
}

#[tokio::test]
async fn test_line_filter_not_regex() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);
    log_expr
        .pipeline
        .push(PipelineStage::LineFilter(LineFilter::not_matches("debug.*")));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let filters = collect_filters(plan);

    // Check: NOT regexp_like(body, "debug.*")
    let has_not_regex = filters.iter().any(|f| {
        is_negated(&f.predicate).is_some_and(|inner| {
            is_scalar_function(inner, "regexp_like").is_some_and(|args| {
                args.len() == 2 && is_column_named(&args[0], "body") && is_literal_str(&args[1], "debug.*")
            })
        })
    });
    assert!(has_not_regex, "Missing NOT regexp_like(body, 'debug.*') filter");
}

// ============================================================================
// Metric Literal Tests
// ============================================================================

#[tokio::test]
async fn test_metric_literal_planning() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let metric_expr = MetricExpr::Literal(42.0);
    let df = planner.plan(LogQLExpr::Metric(metric_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Find projection with "value" alias containing literal 42.0
    let proj = find_projection(plan).expect("Missing Projection node");

    let has_value_alias = proj
        .expr
        .iter()
        .any(|e| is_alias_named(e, "value").is_some_and(|inner| is_literal_f64(inner, 42.0)));
    assert!(has_value_alias, "Missing 'value' alias with literal 42.0");
}

// ============================================================================
// Limit Tests
// ============================================================================

#[tokio::test]
async fn test_log_query_default_limit() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let log_expr = LogExpr::new(selector);
    let expr = LogQLExpr::Log(log_expr);

    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let limit = unwrap_limit(plan);

    // Check skip = 0 (can be None or Some(0))
    match &limit.skip {
        None => {}, // None means skip=0, which is OK
        Some(skip_expr) => {
            let skip_val = get_literal_usize(skip_expr).expect("skip should be a literal");
            assert_eq!(skip_val, 0, "Expected skip=0");
        },
    }

    // Check fetch = DEFAULT_LOG_LIMIT (100)
    match &limit.fetch {
        Some(fetch) => {
            let fetch_val = get_literal_usize(fetch).expect("fetch should be a literal");
            assert_eq!(fetch_val, DEFAULT_LOG_LIMIT, "Expected fetch={DEFAULT_LOG_LIMIT}");
        },
        None => panic!("Expected fetch limit"),
    }
}

#[tokio::test]
async fn test_log_query_custom_limit() {
    let (session_ctx, mut query_ctx) = create_test_context().await;
    query_ctx.limit = Some(50);
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let log_expr = LogExpr::new(selector);
    let expr = LogQLExpr::Log(log_expr);

    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let limit = unwrap_limit(plan);

    // Check fetch = 50
    match &limit.fetch {
        Some(fetch) => {
            let fetch_val = get_literal_usize(fetch).expect("fetch should be a literal");
            assert_eq!(fetch_val, 50, "Expected fetch=50");
        },
        None => panic!("Expected fetch limit"),
    }
}

#[tokio::test]
async fn test_metric_query_no_limit() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let expr = LogQLExpr::Metric(MetricExpr::Literal(42.0));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Verify no Limit node for metric queries
    let limit = find_limit(plan);
    assert!(limit.is_none(), "Metric query should not have Limit node");
}

// ============================================================================
// Range Aggregation Tests (Window Function Based)
// ============================================================================

/// Check if plan contains window expressions.
fn plan_contains_window(plan: &LogicalPlan) -> bool {
    let debug_str = format!("{plan:?}").to_lowercase();
    debug_str.contains("windowfunction") || debug_str.contains("window")
}

/// Check if plan contains time bucketing (`date_bin` or `time_bucket` alias).
fn plan_contains_time_bucketing(plan: &LogicalPlan) -> bool {
    let debug_str = format!("{plan:?}").to_lowercase();
    debug_str.contains("date_bin")
        || debug_str.contains("datebin")
        || debug_str.contains("time_bucket")
        || debug_str.contains("timebucket")
}

#[tokio::test]
async fn test_count_over_time_planning() {
    use crate::query::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Window-based implementation: check for window function and time bucketing
    assert!(
        plan_contains_window(plan),
        "Plan should contain window function for count_over_time"
    );
    assert!(plan_contains_time_bucketing(plan), "Plan should contain time bucketing");

    // Check for "value" alias in projections
    let projections = collect_projections(plan);
    let has_value = projections
        .iter()
        .any(|p| p.expr.iter().any(|e| is_alias_named(e, "value").is_some()));
    assert!(has_value, "Plan should have 'value' alias");
}

#[tokio::test]
async fn test_rate_planning() {
    use crate::query::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::Rate, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Window-based implementation: rate = window_count / range_seconds
    // Check for window function and division by range (300 seconds = 5 minutes)
    assert!(
        plan_contains_window(plan),
        "Plan should contain window function for rate"
    );

    // Check for division operation (rate divides by range seconds)
    let plan_str = format!("{plan:?}");
    assert!(
        plan_str.contains("Divide") || plan_str.contains("300"),
        "Rate plan should contain division by range seconds (300)"
    );
}

#[tokio::test]
async fn test_bytes_over_time_planning() {
    use crate::query::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::BytesOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Window-based implementation: bytes_over_time uses window function with
    // octet_length
    assert!(
        plan_contains_window(plan),
        "Plan should contain window function for bytes_over_time"
    );

    // Check for octet_length function (measures byte length)
    let plan_str = format!("{plan:?}");
    assert!(
        plan_str.contains("octet_length") || plan_str.contains("OctetLength"),
        "Plan should contain octet_length for byte measurement"
    );
}

#[tokio::test]
async fn test_bytes_rate_planning() {
    use crate::query::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::BytesRate, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Window-based implementation: bytes_rate = window_sum(octet_length) /
    // range_seconds
    assert!(
        plan_contains_window(plan),
        "Plan should contain window function for bytes_rate"
    );

    let plan_str = format!("{plan:?}");
    // Check for octet_length and division
    assert!(
        plan_str.contains("octet_length") || plan_str.contains("OctetLength"),
        "Plan should contain octet_length for byte measurement"
    );
    assert!(
        plan_str.contains("Divide") || plan_str.contains("300"),
        "Bytes rate plan should contain division by range seconds (300)"
    );
}

#[tokio::test]
async fn test_range_aggregation_with_grouping() {
    use crate::query::logql::{
        common::{Grouping, GroupingLabel},
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let grouping = Grouping::By(vec![GroupingLabel::new("severity_text")]);
    let agg = RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr).with_grouping(grouping);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Window-based implementation: grouping is handled via partition_by in window
    // function Check plan contains window function
    assert!(plan_contains_window(plan), "Plan should contain window function");

    // Check that severity_text appears in the plan (used in window partition or
    // aggregation)
    let plan_str = format!("{plan:?}");
    assert!(
        plan_str.contains("severity_text"),
        "Plan should reference severity_text for grouping"
    );
}

#[tokio::test]
async fn test_step_based_bucketing() {
    use crate::query::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, mut query_ctx) = create_test_context().await;
    // Set step to 60 seconds
    query_ctx.step = Some(TimeDelta::seconds(60));
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    // Range is 5 minutes, but step is 60 seconds - step should be used for
    // bucketing
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Window-based implementation: date_bin is used for time bucketing in
    // projection
    assert!(plan_contains_time_bucketing(plan), "Plan should contain time bucketing");
}

#[tokio::test]
async fn test_offset_modifier() {
    use chrono::TimeDelta;

    use crate::query::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    // rate({job="mysql"}[5m] offset 1h)
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_offset(TimeDelta::hours(1));
    let agg = RangeAggregation::new(RangeAggregationOp::Rate, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Verify there are multiple Filter nodes (tenant + time range filters)
    let filters = collect_filters(plan);
    assert!(
        filters.len() >= 2,
        "Plan should contain at least 2 Filter nodes (tenant + time range), found {}",
        filters.len()
    );
}

#[tokio::test]
async fn test_unwrap_required_error() {
    use chrono::TimeDelta;

    use crate::query::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    // sum_over_time requires unwrap expression
    let agg = RangeAggregation::new(RangeAggregationOp::SumOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let result = planner.plan(expr).await;

    // Should error because unwrap is required
    assert!(result.is_err(), "sum_over_time without unwrap should error");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("requires an unwrap expression"),
        "Error should mention unwrap requirement: {err}"
    );
}

#[tokio::test]
async fn test_time_grid_gap_filling() {
    use chrono::TimeDelta;
    use datafusion::logical_expr::logical_plan::Join;

    use crate::query::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    // Helper to find Join nodes (for RIGHT JOIN with time grid)
    fn find_join(plan: &LogicalPlan) -> Option<&Join> {
        let mut stack = vec![plan];
        while let Some(node) = stack.pop() {
            if let LogicalPlan::Join(j) = node {
                return Some(j);
            }
            stack.extend(node.inputs());
        }
        None
    }

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // For non-grouped queries, we should have a RIGHT JOIN with time grid
    let join = find_join(plan);
    assert!(join.is_some(), "Plan should contain a Join node for time grid filling");

    // Verify the Join is LEFT/RIGHT/FULL (outer join for gap filling)
    let join_node = join.unwrap();
    assert!(
        matches!(join_node.join_type, JoinType::Left | JoinType::Right | JoinType::Full),
        "Join should be an outer join (LEFT/RIGHT/FULL) for gap filling, got {:?}",
        join_node.join_type
    );
}
