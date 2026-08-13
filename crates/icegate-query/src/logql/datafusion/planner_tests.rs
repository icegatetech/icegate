//! Tests for DataFusion-based `LogQL` query planner.

use std::{collections::BTreeMap, sync::Arc};

use chrono::{TimeDelta, TimeZone, Utc};
use datafusion::{
    arrow::{
        array::{
            Array, ArrayRef, FixedSizeBinaryBuilder, Float64Array, MapArray, MapBuilder, StringArray, StringBuilder,
            TimestampMicrosecondArray,
        },
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    },
    catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider},
    datasource::MemTable,
    logical_expr::{
        BinaryExpr, Expr, LogicalPlan, Operator,
        logical_plan::{Filter, Limit, Projection},
    },
    prelude::{DataFrame, SessionContext},
    scalar::ScalarValue,
};
use iceberg_datafusion::IcebergCatalogProvider;

use super::planner::{DataFusionPlanner, MERGED_ATTRIBUTES_COLUMN};

/// Extract `LogicalPlan` from `DataFrame` for test assertions.
fn get_logical_plan(df: &DataFrame) -> &LogicalPlan {
    df.logical_plan()
}
use icegate_common::{
    CancellationToken, CatalogBackend, CatalogBuilder, CatalogConfig, ICEBERG_CATALOG, ICEGATE_NAMESPACE, IoHandle,
    LOGS_TABLE, schema::logs_schema,
};

use crate::logql::{
    common::MatchOp,
    expr::LogQLExpr,
    log::{DropKeepLabel, LabelMatcher, LineFilter, LogExpr, PipelineStage, Selector},
    metric::MetricExpr,
    planner::{DEFAULT_LOG_LIMIT, Planner, QueryContext, SortDirection},
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
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == expected_op => Some((left.as_ref(), right.as_ref())),
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

/// Check `map_get_by_normalized_key(<column>, <key>)` shape, as produced by
/// the planner's `attribute_lookup` helper for one of the three MAP levels.
fn looks_up_map(expr: &Expr, column: &str, key: &str) -> bool {
    is_scalar_function(expr, "map_get_by_normalized_key")
        .is_some_and(|args| args.len() == 2 && is_column_named(&args[0], column) && is_literal_str(&args[1], key))
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
        cache: None,
    };

    let iceberg_catalog = CatalogBuilder::from_config(&config, &IoHandle::noop(), CancellationToken::new())
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
        direction: SortDirection::default(),
        max_grid_points: QueryContext::DEFAULT_MAX_GRID_POINTS,
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

    // Check: coalesce(map_get_by_normalized_key(log_attributes, "custom_attr"),
    //                  map_get_by_normalized_key(scope_attributes, "custom_attr"),
    //                  map_get_by_normalized_key(resource_attributes, "custom_attr")) = "value"
    // Argument order encodes label precedence: log, then scope, then resource.
    let has_attr_filter = filters.iter().any(|f| {
        is_binary_op(&f.predicate, Operator::Eq).is_some_and(|(left, right)| {
            is_literal_str(right, "value")
                && is_scalar_function(left, "coalesce").is_some_and(|args| {
                    args.len() == 3
                        && looks_up_map(&args[0], "log_attributes", "custom_attr")
                        && looks_up_map(&args[1], "scope_attributes", "custom_attr")
                        && looks_up_map(&args[2], "resource_attributes", "custom_attr")
                })
        })
    });
    assert!(
        has_attr_filter,
        "Missing coalesce(log/scope/resource attribute lookups) = 'value' filter"
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
    let result = planner.plan(LogQLExpr::Metric(metric_expr)).await;

    // Literal values are not yet implemented
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("not yet implemented"),
        "Expected NotImplemented error, got: {err}"
    );
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
    let limit = find_limit(plan).expect("Expected Limit in plan tree");

    // Check skip = 0 (can be None or Some(0))
    match &limit.skip {
        None => {} // None means skip=0, which is OK
        Some(skip_expr) => {
            let skip_val = get_literal_usize(skip_expr).expect("skip should be a literal");
            assert_eq!(skip_val, 0, "Expected skip=0");
        }
    }

    // Check fetch = DEFAULT_LOG_LIMIT (100)
    match &limit.fetch {
        Some(fetch) => {
            let fetch_val = get_literal_usize(fetch).expect("fetch should be a literal");
            assert_eq!(fetch_val, DEFAULT_LOG_LIMIT, "Expected fetch={DEFAULT_LOG_LIMIT}");
        }
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
    let limit = find_limit(plan).expect("Expected Limit in plan tree");

    // Check fetch = 50
    match &limit.fetch {
        Some(fetch) => {
            let fetch_val = get_literal_usize(fetch).expect("fetch should be a literal");
            assert_eq!(fetch_val, 50, "Expected fetch=50");
        }
        None => panic!("Expected fetch limit"),
    }
}

#[tokio::test]
async fn test_metric_query_no_limit() {
    use crate::logql::metric::{RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    // Use a RangeAggregation instead of Literal (which is not implemented)
    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Verify no Limit node for metric queries
    let limit = find_limit(plan);
    assert!(limit.is_none(), "Metric query should not have Limit node");
}

// ============================================================================
// Range Aggregation Tests (UDAF Based)
// ============================================================================

/// Check if plan contains aggregate expressions.
fn plan_contains_aggregate(plan: &LogicalPlan) -> bool {
    let debug_str = format!("{plan:?}").to_lowercase();
    debug_str.contains("aggregate")
}

#[tokio::test]
async fn test_count_over_time_planning() {
    use crate::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // UDAF-based implementation: count_over_time uses aggregate function
    assert!(
        plan_contains_aggregate(plan),
        "Plan should contain aggregate for count_over_time"
    );

    // Check for "value" alias in projections
    let projections = collect_projections(plan);
    let has_value = projections
        .iter()
        .any(|p| p.expr.iter().any(|e| is_alias_named(e, "value").is_some()));
    assert!(has_value, "Plan should have 'value' alias");
}

#[tokio::test]
async fn test_rate_planning() {
    use crate::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::Rate, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // UDAF-based implementation: rate_over_time divides count by range_seconds
    // internally
    assert!(plan_contains_aggregate(plan), "Plan should contain aggregate for rate");

    // Check for "value" alias in projections
    let projections = collect_projections(plan);
    let has_value = projections
        .iter()
        .any(|p| p.expr.iter().any(|e| is_alias_named(e, "value").is_some()));
    assert!(has_value, "Plan should have 'value' alias");
}

#[tokio::test]
async fn test_bytes_over_time_planning() {
    use crate::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::BytesOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // UDAF-based implementation: bytes_over_time accepts body column and calculates
    // byte length internally
    assert!(
        plan_contains_aggregate(plan),
        "Plan should contain aggregate for bytes_over_time"
    );

    // Check for "value" alias in projections
    let projections = collect_projections(plan);
    let has_value = projections
        .iter()
        .any(|p| p.expr.iter().any(|e| is_alias_named(e, "value").is_some()));
    assert!(has_value, "Plan should have 'value' alias");
}

#[tokio::test]
async fn test_bytes_rate_planning() {
    use crate::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::BytesRate, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // UDAF-based implementation: bytes_rate accepts body column, calculates bytes
    // internally, and divides by range_seconds
    assert!(
        plan_contains_aggregate(plan),
        "Plan should contain aggregate for bytes_rate"
    );

    // Check for "value" alias in projections
    let projections = collect_projections(plan);
    let has_value = projections
        .iter()
        .any(|p| p.expr.iter().any(|e| is_alias_named(e, "value").is_some()));
    assert!(has_value, "Plan should have 'value' alias");
}

#[tokio::test]
async fn test_range_aggregation_with_grouping() {
    use crate::logql::{
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

    // UDAF-based implementation: count_over_time uses aggregate function with
    // grouping Check plan contains aggregate
    assert!(plan_contains_aggregate(plan), "Plan should contain aggregate");

    // Check that severity_text appears in the plan (used in grouping)
    let plan_str = format!("{plan:?}");
    assert!(
        plan_str.contains("severity_text"),
        "Plan should reference severity_text for grouping"
    );
}

#[tokio::test]
async fn test_step_based_bucketing() {
    use crate::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

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

    // UDAF-based implementation: step is passed to count_over_time UDAF
    // Check plan contains aggregate with step parameter
    assert!(
        plan_contains_aggregate(plan),
        "Plan should contain aggregate for count_over_time"
    );

    // GridAgg UDAF encodes the step in its grid points
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("gridagg"),
        "Plan should contain gridagg UDAF with step encoded in grid points"
    );
}

#[tokio::test]
async fn test_offset_modifier() {
    use chrono::TimeDelta;

    use crate::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

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

    use crate::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

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
    use crate::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "mysql")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // UDAF-based implementation: gridagg UDAF generates per-bucket values,
    // then we unnest and select. Gap filling happens naturally because
    // non-matching grid points produce NULL values (sparse representation).
    assert!(
        plan_contains_aggregate(plan),
        "Plan should contain aggregate for counting"
    );

    // Check plan contains gridagg UDAF with count operation
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("gridagg"),
        "Plan should reference gridagg UDAF: {plan_str}"
    );
    assert!(
        plan_str.contains("op: count"),
        "Plan should use count operation in gridagg: {plan_str}"
    );
}

// ============================================================================
// Unwrap Range Aggregation Tests
// ============================================================================

#[tokio::test]
async fn test_sum_over_time_with_numeric_unwrap() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service_name", MatchOp::Eq, "api")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("response_time"));
    let agg = RangeAggregation::new(RangeAggregationOp::SumOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    // Verify plan contains expected operations
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("parse_numeric") || plan_str.contains("parsenumeric"),
        "Plan should use parse_numeric UDF for default conversion"
    );
    assert!(
        plan_str.contains("gridagg"),
        "Plan should contain gridagg UDAF for time bucketing"
    );
    assert!(plan_str.contains("op: sum"), "Plan should use sum operation in gridagg");
}

#[tokio::test]
async fn test_avg_over_time_with_duration_conversion() {
    use crate::logql::{
        log::{UnwrapConversion, UnwrapExpr},
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("job", MatchOp::Eq, "worker")]);
    let log_expr = LogExpr::new(selector);
    let unwrap = UnwrapExpr::with_conversion("processing_time", UnwrapConversion::Duration);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(10)).with_unwrap(unwrap);
    let agg = RangeAggregation::new(RangeAggregationOp::AvgOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("parse_duration") || plan_str.contains("parseduration"),
        "Plan should use parse_duration UDF for duration conversion"
    );
    assert!(plan_str.contains("avg"), "Plan should contain avg aggregation");
}

#[tokio::test]
async fn test_min_max_over_time() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    // Test min_over_time
    let selector = Selector::new(vec![LabelMatcher::new("app", MatchOp::Eq, "db")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr.clone(), TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("latency"));
    let min_agg = RangeAggregation::new(RangeAggregationOp::MinOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(min_agg));
    let df = planner.plan(expr).await.expect("min_over_time planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(plan_str.contains("min"), "Plan should contain min aggregation");

    // Test max_over_time
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("latency"));
    let max_agg = RangeAggregation::new(RangeAggregationOp::MaxOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(max_agg));
    let df = planner.plan(expr).await.expect("max_over_time planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(plan_str.contains("max"), "Plan should contain max aggregation");
}

#[tokio::test]
async fn test_first_last_over_time() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("container", MatchOp::Eq, "nginx")]);
    let log_expr = LogExpr::new(selector);

    // Test first_over_time
    let range_expr = RangeExpr::new(log_expr.clone(), TimeDelta::hours(1)).with_unwrap(UnwrapExpr::new("request_size"));
    let first_agg = RangeAggregation::new(RangeAggregationOp::FirstOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(first_agg));
    let df = planner.plan(expr).await.expect("first_over_time planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("op: first"),
        "Plan should contain gridagg UDAF with first operation"
    );
    assert!(plan_str.contains("timestamp"), "Plan should reference timestamp column");

    // Test last_over_time
    let range_expr = RangeExpr::new(log_expr, TimeDelta::hours(1)).with_unwrap(UnwrapExpr::new("request_size"));
    let last_agg = RangeAggregation::new(RangeAggregationOp::LastOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(last_agg));
    let df = planner.plan(expr).await.expect("last_over_time planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("op: last"),
        "Plan should contain gridagg UDAF with last operation"
    );
}

#[tokio::test]
async fn test_quantile_over_time() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service", MatchOp::Eq, "payment")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(15)).with_unwrap(UnwrapExpr::new("amount"));

    // quantile_over_time requires a parameter (phi)
    let agg = RangeAggregation::new(RangeAggregationOp::QuantileOverTime, range_expr).with_param(0.95);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("quantile_over_time planning failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("quantile"),
        "Plan should contain gridagg UDAF with quantile operation"
    );
    assert!(plan_str.contains("0.95"), "Plan should contain the quantile parameter");
}

#[tokio::test]
async fn test_quantile_over_time_missing_param_error() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service", MatchOp::Eq, "test")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("value"));

    // quantile_over_time WITHOUT parameter should error
    let agg = RangeAggregation::new(RangeAggregationOp::QuantileOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let result = planner.plan(expr).await;

    assert!(result.is_err(), "quantile_over_time without parameter should error");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("parameter"),
        "Error should mention missing parameter: {err}"
    );
}

#[tokio::test]
async fn test_quantile_over_time_param_out_of_range() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service", MatchOp::Eq, "test")]);
    let log_expr = LogExpr::new(selector);

    // Test parameter > 1.0
    let range_expr = RangeExpr::new(log_expr.clone(), TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("value"));
    let agg = RangeAggregation::new(RangeAggregationOp::QuantileOverTime, range_expr).with_param(1.5);
    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let result = planner.plan(expr).await;
    assert!(result.is_err(), "quantile_over_time with parameter > 1.0 should error");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("between 0.0 and 1.0"),
        "Error should mention valid range: {err}"
    );

    // Test parameter < 0.0
    let range_expr = RangeExpr::new(log_expr.clone(), TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("value"));
    let agg = RangeAggregation::new(RangeAggregationOp::QuantileOverTime, range_expr).with_param(-0.1);
    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let result = planner.plan(expr).await;
    assert!(result.is_err(), "quantile_over_time with parameter < 0.0 should error");
    let err = result.unwrap_err();
    assert!(
        err.to_string().contains("between 0.0 and 1.0"),
        "Error should mention valid range: {err}"
    );

    // Test boundary values (should succeed)
    for phi in [0.0, 0.5, 1.0] {
        let range_expr = RangeExpr::new(log_expr.clone(), TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("value"));
        let agg = RangeAggregation::new(RangeAggregationOp::QuantileOverTime, range_expr).with_param(phi);
        let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
        let result = planner.plan(expr).await;
        assert!(result.is_ok(), "quantile_over_time with parameter {phi} should succeed");
    }
}

#[tokio::test]
async fn test_stddev_stdvar_over_time() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("sensor", MatchOp::Eq, "temp")]);
    let log_expr = LogExpr::new(selector);

    // Test stddev_over_time
    let range_expr = RangeExpr::new(log_expr.clone(), TimeDelta::minutes(30)).with_unwrap(UnwrapExpr::new("reading"));
    let stddev_agg = RangeAggregation::new(RangeAggregationOp::StddevOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(stddev_agg));
    let df = planner.plan(expr).await.expect("stddev_over_time planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("op: stddev"),
        "Plan should contain gridagg UDAF with stddev operation"
    );

    // Test stdvar_over_time
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(30)).with_unwrap(UnwrapExpr::new("reading"));
    let stdvar_agg = RangeAggregation::new(RangeAggregationOp::StdvarOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(stdvar_agg));
    let df = planner.plan(expr).await.expect("stdvar_over_time planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("op: stdvar"),
        "Plan should contain gridagg UDAF with stdvar operation"
    );
}

#[tokio::test]
async fn test_rate_counter() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("metric", MatchOp::Eq, "requests")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("count"));
    let agg = RangeAggregation::new(RangeAggregationOp::RateCounter, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("rate_counter planning failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    // rate_counter is handled by gridagg UDAF with RateCounter op
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("ratecounter"),
        "Plan should contain gridagg UDAF with ratecounter operation"
    );
}

#[tokio::test]
async fn test_rate_counter_with_single_reset() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("metric", MatchOp::Eq, "requests")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("count"));
    let agg = RangeAggregation::new(RangeAggregationOp::RateCounter, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("rate_counter planning failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    // Counter reset detection is handled inside gridagg UDAF with RateCounter op
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("ratecounter"),
        "Plan should contain gridagg UDAF with ratecounter operation for reset detection"
    );
}

#[tokio::test]
async fn test_rate_counter_multiple_resets() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("counter", MatchOp::Eq, "total")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(10)).with_unwrap(UnwrapExpr::new("value"));
    let agg = RangeAggregation::new(RangeAggregationOp::RateCounter, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("rate_counter planning failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    // Counter reset detection for multiple resets is handled inside gridagg UDAF
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("ratecounter"),
        "Plan should contain gridagg UDAF with ratecounter operation for multiple resets"
    );
}

#[tokio::test]
async fn test_rate_counter_no_reset() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("monotonic", MatchOp::Eq, "true")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("counter"));
    let agg = RangeAggregation::new(RangeAggregationOp::RateCounter, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("rate_counter planning failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    // Monotonic counter handling is inside gridagg UDAF with RateCounter op
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("ratecounter"),
        "Plan should contain gridagg UDAF with ratecounter operation for monotonic counters"
    );
}

#[tokio::test]
async fn test_rate_counter_single_value() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("single", MatchOp::Eq, "sample")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::seconds(30)).with_unwrap(UnwrapExpr::new("value"));
    let agg = RangeAggregation::new(RangeAggregationOp::RateCounter, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("rate_counter planning failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    // Single value and NULL handling is inside gridagg UDAF with RateCounter op
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("ratecounter"),
        "Plan should contain gridagg UDAF with ratecounter operation for single value case"
    );
}

#[tokio::test]
async fn test_rate_counter_label_grouping() {
    use crate::logql::{
        common::{Grouping, GroupingLabel},
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr, VectorAggregation, VectorAggregationOp},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    // Create rate_counter with grouping by service_name
    let selector = Selector::new(vec![LabelMatcher::new("app", MatchOp::Eq, "backend")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("requests"));
    let range_agg = RangeAggregation::new(RangeAggregationOp::RateCounter, range_expr);

    // Wrap in vector aggregation with grouping
    let grouping = Grouping::By(vec![GroupingLabel::new("service_name")]);
    let vector_agg = VectorAggregation::new(VectorAggregationOp::Sum, MetricExpr::RangeAggregation(range_agg))
        .with_grouping(grouping);

    let expr = LogQLExpr::Metric(MetricExpr::VectorAggregation(vector_agg));
    let df = planner.plan(expr).await.expect("rate_counter with grouping planning failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    // Counter reset detection per label group is handled inside gridagg UDAF
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("ratecounter"),
        "Plan should contain gridagg UDAF with ratecounter operation"
    );
    // Verify service_name appears in the plan for grouping
    assert!(
        plan_str.contains("service_name"),
        "Plan should reference service_name for label grouping"
    );
}

#[tokio::test]
async fn test_bytes_conversion() {
    use crate::logql::{
        log::{UnwrapConversion, UnwrapExpr},
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("app", MatchOp::Eq, "storage")]);
    let log_expr = LogExpr::new(selector);
    let unwrap = UnwrapExpr::with_conversion("disk_usage", UnwrapConversion::Bytes);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::hours(1)).with_unwrap(unwrap);
    let agg = RangeAggregation::new(RangeAggregationOp::SumOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning with bytes conversion failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("parse_bytes") || plan_str.contains("parsebytes"),
        "Plan should use parse_bytes UDF for bytes conversion"
    );
}

#[tokio::test]
async fn test_duration_seconds_conversion() {
    use crate::logql::{
        log::{UnwrapConversion, UnwrapExpr},
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("type", MatchOp::Eq, "batch")]);
    let log_expr = LogExpr::new(selector);
    let unwrap = UnwrapExpr::with_conversion("execution_time", UnwrapConversion::DurationSeconds);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(20)).with_unwrap(unwrap);
    let agg = RangeAggregation::new(RangeAggregationOp::MaxOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner
        .plan(expr)
        .await
        .expect("Planning with duration_seconds conversion failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("parse_duration") || plan_str.contains("parseduration"),
        "Plan should use parse_duration UDF for duration_seconds conversion"
    );
}

#[tokio::test]
async fn test_unwrap_error_handling_in_plan() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("service", MatchOp::Eq, "test")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("value"));
    let agg = RangeAggregation::new(RangeAggregationOp::SumOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");
    let plan = get_logical_plan(&df);

    let plan_str = format!("{plan:?}").to_lowercase();

    // Verify error tracking logic is present in plan
    assert!(
        plan_str.contains("is_null") || plan_str.contains("isnull"),
        "Plan should check for NULL values (conversion errors)"
    );
    assert!(
        plan_str.contains("bool_or") || plan_str.contains("boolor"),
        "Plan should aggregate error flags with bool_or"
    );
    assert!(
        plan_str.contains("map_insert") || plan_str.contains("mapinsert"),
        "Plan should use map_insert to add __error__ label"
    );
}

#[tokio::test]
async fn test_unwrap_with_offset() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![LabelMatcher::new("env", MatchOp::Eq, "prod")]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5))
        .with_offset(TimeDelta::hours(1))
        .with_unwrap(UnwrapExpr::new("metric"));
    let agg = RangeAggregation::new(RangeAggregationOp::AvgOverTime, range_expr);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning with offset failed");
    let plan = get_logical_plan(&df);

    // Offset shifts the time window but shouldn't change the core aggregation logic
    let plan_str = format!("{plan:?}").to_lowercase();
    assert!(
        plan_str.contains("gridagg") && plan_str.contains("op: avg"),
        "Plan should contain gridagg UDAF with avg operation even with offset"
    );
}

/// `MAP<Utf8, Utf8>` field shape produced by [`MapBuilder`]'s default
/// element names (`entries`/`keys`/`values`) — mirrors the fixture
/// convention already used by `attribute_lookup_tests` in `planner.rs` and
/// the `traceql` planner's `MemTable` tests.
fn unwrap_fixture_attribute_map_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("keys", DataType::Utf8, false)),
                        Arc::new(Field::new("values", DataType::Utf8, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        false,
    )
}

/// Build a `MAP<Utf8, Utf8>` column with one row per entry of `rows`. An
/// empty slice for a row produces a present-but-empty map — a genuinely
/// absent attribute, as opposed to a SQL NULL map.
fn unwrap_fixture_attribute_map_column(rows: &[&[(&str, &str)]]) -> ArrayRef {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for pairs in rows {
        for (key, value) in *pairs {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true).expect("map row");
    }
    Arc::new(builder.finish())
}

/// Minimal logs-table fixture schema: tenant/time filtering, the `by
/// (service_name)` grouping key, and the three per-level attribute maps
/// `attribute_lookup` reads.
fn unwrap_fixture_schema() -> Arc<Schema> {
    use icegate_common::schema::{
        COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_TENANT_ID,
        COL_TIMESTAMP,
    };

    Arc::new(Schema::new(vec![
        Field::new(COL_TENANT_ID, DataType::Utf8, false),
        Field::new(COL_TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new(COL_SERVICE_NAME, DataType::Utf8, true),
        unwrap_fixture_attribute_map_field(COL_LOG_ATTRIBUTES),
        unwrap_fixture_attribute_map_field(COL_SCOPE_ATTRIBUTES),
        unwrap_fixture_attribute_map_field(COL_RESOURCE_ATTRIBUTES),
    ]))
}

/// Mounts `table` at the logs table's fully-qualified name behind a bare
/// in-memory catalog, so `session_ctx.table(LOGS_TABLE_FQN)` resolves
/// without a real Iceberg catalog. Mirrors the `traceql` planner's
/// `MemTable` fixture convention (see `traceql/datafusion/planner_tests.rs`).
fn register_logs_fixture(ctx: &SessionContext, table: Arc<MemTable>) {
    let catalog_provider: Arc<dyn CatalogProvider> = Arc::new(MemoryCatalogProvider::new());
    ctx.register_catalog(ICEBERG_CATALOG, catalog_provider);
    let catalog = ctx.catalog(ICEBERG_CATALOG).expect("catalog just registered");

    let schema_provider: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
    let _ = catalog
        .register_schema(ICEGATE_NAMESPACE, schema_provider)
        .expect("register schema");
    let schema = catalog.schema(ICEGATE_NAMESPACE).expect("schema just registered");

    schema.register_table(LOGS_TABLE.to_string(), table).expect("register table");
}

/// Build a `SessionContext` + `QueryContext` isolated from
/// [`create_test_context`]: two fixture rows sharing `service_name`, one
/// with `value = "2.0"`, the other missing the `value` attribute entirely.
/// `start == end` produces exactly one `GridAgg` grid point, and both rows'
/// timestamps fall inside that point's 5-minute lookback window, so the
/// aggregate output is pinned to a single, unambiguous row.
fn unwrap_null_passthrough_fixture() -> (SessionContext, QueryContext) {
    const TENANT: &str = "unwrap-fixture-tenant";
    const SERVICE: &str = "unwrap-fixture-service";

    let grid_point = Utc.timestamp_opt(1_000, 0).unwrap();
    let row_timestamp = Utc.timestamp_opt(900, 0).unwrap().timestamp_micros();

    let tenant_ids = StringArray::from(vec![TENANT, TENANT]);
    let timestamps = TimestampMicrosecondArray::from(vec![row_timestamp, row_timestamp]);
    let service_names = StringArray::from(vec![Some(SERVICE), Some(SERVICE)]);
    // Row 0 carries a parseable `value`; row 1 omits the key entirely, so
    // `attribute_lookup` (and therefore the unwrapped value) resolves NULL.
    let log_attrs = unwrap_fixture_attribute_map_column(&[&[("value", "2.0")], &[]]);
    let scope_attrs = unwrap_fixture_attribute_map_column(&[&[], &[]]);
    let resource_attrs = unwrap_fixture_attribute_map_column(&[&[], &[]]);

    let batch = RecordBatch::try_new(
        unwrap_fixture_schema(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(timestamps),
            Arc::new(service_names),
            log_attrs,
            scope_attrs,
            resource_attrs,
        ],
    )
    .expect("record batch");

    let ctx = SessionContext::new();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
    register_logs_fixture(&ctx, Arc::new(table));

    let query_ctx = QueryContext {
        tenant_id: TENANT.to_string(),
        start: grid_point,
        end: grid_point,
        limit: None,
        step: Some(TimeDelta::seconds(15)),
        direction: SortDirection::default(),
        max_grid_points: QueryContext::DEFAULT_MAX_GRID_POINTS,
    };

    (ctx, query_ctx)
}

#[tokio::test]
async fn test_unwrap_null_passthrough_to_udaf() {
    use crate::logql::{
        common::{Grouping, GroupingLabel},
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    let (session_ctx, query_ctx) = unwrap_null_passthrough_fixture();
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("value"));
    // `avg_over_time` (unlike `sum_over_time`) supports grouping, and
    // grouping `by (service_name)` is required here: without an explicit
    // `by`, the planner groups by the *entire* merged attribute map —
    // including `value` itself — which would put our two fixture rows in
    // separate single-row groups and hide the NULL-handling difference this
    // test exists to catch.
    let agg = RangeAggregation::new(RangeAggregationOp::AvgOverTime, range_expr)
        .with_grouping(Grouping::By(vec![GroupingLabel::new("service_name")]));

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");

    // Error tracking via _has_unwrap_error should still be present.
    let plan_str = format!("{:?}", get_logical_plan(&df)).to_lowercase();
    assert!(
        plan_str.contains("_has_unwrap_error"),
        "Plan should still track unwrap errors via _has_unwrap_error column"
    );

    let batches = df.collect().await.expect("collect failed");
    let mut values = Vec::new();
    for batch in &batches {
        let value_col = batch
            .column_by_name("value")
            .expect("value column present")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value column is Float64");
        for value in value_col {
            values.push(value.expect("grid point should be non-empty: both fixture rows fall in its lookback window"));
        }
    }

    // NULL unwrapped values must NOT be coalesced to 0.0 — GridAgg
    // accumulators skip NULL rows natively (see SumGridAccumulator /
    // AvgGridAccumulator::update_batch in udaf/grid_agg.rs), so this
    // bucket's average is 2.0 / 1 (only the parseable row counts).
    // Coalescing the missing row's value to 0.0 before aggregation would
    // instead average (0.0 + 2.0) / 2 = 1.0 — silently distorting sum, avg,
    // min, stddev, and quantile aggregates alike.
    assert_eq!(
        values.len(),
        1,
        "one grid point, one `by (service_name)` group -> one output row"
    );
    assert!(
        (values[0] - 2.0).abs() < f64::EPSILON,
        "expected avg 2.0 from skipping the NULL row, got {}",
        values[0]
    );
}

// ============================================================================
// Grouped Range Aggregation - Preserved Attributes Regression Tests
//
// docs/tests.md requires planner/executor changes to be tested by executing
// representative data; a plan-shape substring check (as
// test_range_aggregation_with_grouping and test_rate_counter_label_grouping
// use) cannot see whether the *values* in the output attributes map are
// right. plan_log_range_aggregation and plan_unwrap_range_aggregation each
// have a by/without branch that must read back the just-filtered
// `attributes` column (`attrs_for_preserve = col(MERGED_ATTRIBUTES_COLUMN)`) rather
// than recompute merged_attributes() fresh from the still-present raw
// per-level columns — the latter would silently discard the grouping filter.
// ============================================================================

/// Build a `SessionContext` + `QueryContext` for the `attrs_for_preserve`
/// regression tests below: one row per entry of `log_rows`, all sharing
/// `region = "us"` but differing in `pod` (and, for the unwrap variant, in a
/// parseable `value`), all falling inside one grid point's lookback window —
/// see [`unwrap_null_passthrough_fixture`] for why `start == end` and
/// `row_timestamp` pin a single, unambiguous grid bucket. A grouped
/// aggregation `by (region)` must collapse the rows into one group whose
/// preserved `attributes` map reflects ONLY the by-filtered label set: `pod`
/// (and `value`) must not leak through from the raw per-level columns.
fn grouping_filter_fixture(log_rows: &[&[(&str, &str)]]) -> (SessionContext, QueryContext) {
    const TENANT: &str = "grouping-filter-fixture-tenant";
    const SERVICE: &str = "grouping-filter-fixture-service";

    let grid_point = Utc.timestamp_opt(1_000, 0).unwrap();
    let row_timestamp = Utc.timestamp_opt(900, 0).unwrap().timestamp_micros();
    let row_count = log_rows.len();

    let tenant_ids = StringArray::from(vec![TENANT; row_count]);
    let timestamps = TimestampMicrosecondArray::from(vec![row_timestamp; row_count]);
    let service_names = StringArray::from(vec![Some(SERVICE); row_count]);
    let log_attrs = unwrap_fixture_attribute_map_column(log_rows);
    let empty_rows: Vec<&[(&str, &str)]> = vec![&[]; row_count];
    let scope_attrs = unwrap_fixture_attribute_map_column(&empty_rows);
    let resource_attrs = unwrap_fixture_attribute_map_column(&empty_rows);

    let batch = RecordBatch::try_new(
        unwrap_fixture_schema(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(timestamps),
            Arc::new(service_names),
            log_attrs,
            scope_attrs,
            resource_attrs,
        ],
    )
    .expect("record batch");

    let ctx = SessionContext::new();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
    register_logs_fixture(&ctx, Arc::new(table));

    let query_ctx = QueryContext {
        tenant_id: TENANT.to_string(),
        start: grid_point,
        end: grid_point,
        limit: None,
        step: Some(TimeDelta::seconds(15)),
        direction: SortDirection::default(),
        max_grid_points: QueryContext::DEFAULT_MAX_GRID_POINTS,
    };

    (ctx, query_ctx)
}

/// Collect `df` and assert every row's `attributes` output column equals
/// exactly `expected` — the by/without-filtered label set a grouped
/// aggregation must preserve. Also asserts at least one row was produced, so
/// an empty result can't vacuously pass.
async fn assert_all_rows_have_attributes(df: DataFrame, expected: &BTreeMap<&str, &str>) {
    let batches = df.collect().await.expect("collect failed");
    let mut saw_row = false;
    for batch in &batches {
        let attrs_col = batch
            .column_by_name(MERGED_ATTRIBUTES_COLUMN)
            .expect("attributes column present")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("attributes column is MAP");
        for row in 0..attrs_col.len() {
            saw_row = true;
            let entries = attrs_col.value(row);
            let keys = entries.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
            let values = entries.column(1).as_any().downcast_ref::<StringArray>().expect("values");
            let attrs: BTreeMap<&str, &str> = (0..keys.len()).map(|i| (keys.value(i), values.value(i))).collect();
            assert_eq!(
                &attrs, expected,
                "grouped aggregation's preserved attributes must equal exactly the by/without-filtered set"
            );
        }
    }
    assert!(saw_row, "expected at least one output row from the grid point");
}

#[tokio::test]
async fn test_count_over_time_grouping_preserves_only_the_by_filtered_attributes() {
    use crate::logql::{
        common::{Grouping, GroupingLabel},
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    // Two rows share `region`, the by-label, but differ in `pod`, which is
    // not in the by-clause and must be filtered out of the preserved labels.
    let (session_ctx, query_ctx) = grouping_filter_fixture(&[
        &[("region", "us"), ("pod", "pod-a")],
        &[("region", "us"), ("pod", "pod-b")],
    ]);
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let grouping = Grouping::By(vec![GroupingLabel::new("region")]);
    let agg = RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr).with_grouping(grouping);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");

    // `pod` is excluded by `by (region)`. If plan_log_range_aggregation's
    // attrs_for_preserve recomputed an unfiltered merged_attributes() instead
    // of reading back the just-filtered `attributes` column, `pod` would leak
    // through from the still-present raw per-level columns.
    let expected: BTreeMap<&str, &str> = BTreeMap::from([("region", "us")]);
    assert_all_rows_have_attributes(df, &expected).await;
}

#[tokio::test]
async fn test_avg_over_time_grouping_preserves_only_the_by_filtered_attributes() {
    use crate::logql::{
        common::{Grouping, GroupingLabel},
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    // Same shape as the count_over_time case above, exercising the unwrap
    // sibling plan_unwrap_range_aggregation instead: two rows share `region`
    // but differ in `pod` and in the unwrapped `value` itself, neither of
    // which is in the by-clause.
    let (session_ctx, query_ctx) = grouping_filter_fixture(&[
        &[("region", "us"), ("pod", "pod-a"), ("value", "2.0")],
        &[("region", "us"), ("pod", "pod-b"), ("value", "4.0")],
    ]);
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let log_expr = LogExpr::new(selector);
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("value"));
    let grouping = Grouping::By(vec![GroupingLabel::new("region")]);
    let agg = RangeAggregation::new(RangeAggregationOp::AvgOverTime, range_expr).with_grouping(grouping);

    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));
    let df = planner.plan(expr).await.expect("Planning failed");

    // Same contract as plan_log_range_aggregation, at the unwrap-based
    // sibling: `pod` and the unwrap source label `value` are both excluded by
    // `by (region)`, and must not leak through if attrs_for_preserve is
    // reverted to an unconditional merged_attributes().
    let expected: BTreeMap<&str, &str> = BTreeMap::from([("region", "us")]);
    assert_all_rows_have_attributes(df, &expected).await;
}

// ============================================================================
// Drop/Keep Pipeline Stage Tests
// ============================================================================

#[tokio::test]
async fn test_drop_with_equals_matcher() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);

    // Create: drop level="debug"
    let matcher = LabelMatcher::new("level", MatchOp::Eq, "debug");
    log_expr
        .pipeline
        .push(PipelineStage::Drop(vec![DropKeepLabel::with_matcher(matcher)]));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();

    // Verify map_drop_keys UDF is used
    assert!(
        plan_str.contains("mapdropkeys"),
        "Plan should use map_drop_keys UDF for drop operation"
    );

    // Verify the plan contains the key "level"
    assert!(plan_str.contains("level"), "Plan should contain the label 'level'");

    // Verify the plan contains the value "debug"
    assert!(plan_str.contains("debug"), "Plan should contain the value 'debug'");
}

#[tokio::test]
async fn test_drop_with_regex_matcher() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);

    // Create: drop level=~"debug|info"
    let matcher = LabelMatcher::new("level", MatchOp::Re, "debug|info");
    log_expr
        .pipeline
        .push(PipelineStage::Drop(vec![DropKeepLabel::with_matcher(matcher)]));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();

    // Verify map_drop_keys UDF is used
    assert!(
        plan_str.contains("mapdropkeys"),
        "Plan should use map_drop_keys UDF for drop operation"
    );

    // Verify the plan contains the regex pattern
    assert!(
        plan_str.contains("debug|info"),
        "Plan should contain the regex pattern 'debug|info'"
    );

    // Verify the plan contains the =~ operator
    assert!(
        plan_str.contains("=~"),
        "Plan should contain the regex match operator '=~'"
    );
}

#[tokio::test]
async fn test_drop_mixed_simple_and_matchers() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);

    // Create: drop method, level="debug"
    let matcher = LabelMatcher::new("level", MatchOp::Eq, "debug");
    log_expr.pipeline.push(PipelineStage::Drop(vec![
        DropKeepLabel::new("method"),         // Simple name
        DropKeepLabel::with_matcher(matcher), // With matcher
    ]));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();

    // Verify map_drop_keys UDF is used
    assert!(
        plan_str.contains("mapdropkeys"),
        "Plan should use map_drop_keys UDF for drop operation"
    );

    // Verify both keys are present
    assert!(plan_str.contains("method"), "Plan should contain the label 'method'");
    assert!(plan_str.contains("level"), "Plan should contain the label 'level'");

    // Verify the matcher value is present
    assert!(plan_str.contains("debug"), "Plan should contain the value 'debug'");
}

#[tokio::test]
async fn test_keep_with_equals_matcher() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);

    // Create: keep level="info"
    let matcher = LabelMatcher::new("level", MatchOp::Eq, "info");
    log_expr
        .pipeline
        .push(PipelineStage::Keep(vec![DropKeepLabel::with_matcher(matcher)]));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();

    // Verify map_keep_keys UDF is used
    assert!(
        plan_str.contains("mapkeepkeys"),
        "Plan should use map_keep_keys UDF for keep operation"
    );

    // Verify the plan contains the key "level"
    assert!(plan_str.contains("level"), "Plan should contain the label 'level'");

    // Verify the plan contains the value "info"
    assert!(plan_str.contains("info"), "Plan should contain the value 'info'");
}

#[tokio::test]
async fn test_keep_with_not_equals_matcher() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);

    // Create: keep level!="error"
    let matcher = LabelMatcher::new("level", MatchOp::Neq, "error");
    log_expr
        .pipeline
        .push(PipelineStage::Keep(vec![DropKeepLabel::with_matcher(matcher)]));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();

    // Verify map_keep_keys UDF is used
    assert!(
        plan_str.contains("mapkeepkeys"),
        "Plan should use map_keep_keys UDF for keep operation"
    );

    // Verify the plan contains the key "level"
    assert!(plan_str.contains("level"), "Plan should contain the label 'level'");

    // Verify the plan contains the value "error"
    assert!(plan_str.contains("error"), "Plan should contain the value 'error'");

    // Verify the plan contains the != operator
    assert!(
        plan_str.contains("!="),
        "Plan should contain the not-equals operator '!='"
    );
}

#[tokio::test]
async fn test_keep_mixed_simple_and_matchers() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);

    // Create: keep level, service="api"
    let matcher = LabelMatcher::new("service", MatchOp::Eq, "api");
    log_expr.pipeline.push(PipelineStage::Keep(vec![
        DropKeepLabel::new("level"),          // Simple name
        DropKeepLabel::with_matcher(matcher), // With matcher
    ]));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();

    // Verify map_keep_keys UDF is used
    assert!(
        plan_str.contains("mapkeepkeys"),
        "Plan should use map_keep_keys UDF for keep operation"
    );

    // Verify both keys are present
    assert!(plan_str.contains("level"), "Plan should contain the label 'level'");
    assert!(plan_str.contains("service"), "Plan should contain the label 'service'");

    // Verify the matcher value is present
    assert!(plan_str.contains("api"), "Plan should contain the value 'api'");
}

#[tokio::test]
async fn test_drop_simple_names_backward_compat() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);

    // Create: drop method, level (simple names only - backward compatibility test)
    log_expr.pipeline.push(PipelineStage::Drop(vec![
        DropKeepLabel::new("method"),
        DropKeepLabel::new("level"),
    ]));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();

    // Verify map_drop_keys UDF is used
    assert!(
        plan_str.contains("mapdropkeys"),
        "Plan should use map_drop_keys UDF for drop operation"
    );

    // Verify both keys are present
    assert!(plan_str.contains("method"), "Plan should contain the label 'method'");
    assert!(plan_str.contains("level"), "Plan should contain the label 'level'");
}

#[tokio::test]
async fn test_keep_simple_names_backward_compat() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);

    // Create: keep service, app (simple names only - backward compatibility test)
    log_expr.pipeline.push(PipelineStage::Keep(vec![
        DropKeepLabel::new("service"),
        DropKeepLabel::new("app"),
    ]));

    let df = planner.plan(LogQLExpr::Log(log_expr)).await.expect("Planning failed");
    let plan = get_logical_plan(&df);
    let plan_str = format!("{plan:?}").to_lowercase();

    // Verify map_keep_keys UDF is used
    assert!(
        plan_str.contains("mapkeepkeys"),
        "Plan should use map_keep_keys UDF for keep operation"
    );

    // Verify both keys are present
    assert!(plan_str.contains("service"), "Plan should contain the label 'service'");
    assert!(plan_str.contains("app"), "Plan should contain the label 'app'");
}

// ============================================================================
// C1 regression: aggregations over a `drop`/`keep`-carrying inner pipeline
//
// plan_log_range_aggregation, plan_vector_aggregation (via grouping pushed
// down into its inner range aggregation), and plan_unwrap_range_aggregation
// each read attributes again *after* the inner LogExpr's own pipeline may
// have already run `drop`/`keep` — which collapses the three per-level
// columns into one `attributes` column. Before the fix, merged_attributes()/
// attribute_lookup() ignored that and unconditionally referenced the three
// per-level columns, so any of `count_over_time`, `sum(...) by (...)`, or
// `avg_over_time(... | unwrap ...)` wrapped around a `drop`/`keep` stage
// failed at plan construction (`No field named ...log_attributes`). These
// tests execute the full planner end to end and assert on the resulting
// values and attributes, not on plan shape.
// ============================================================================

/// Schema for [`ungrouped_aggregation_fixture`]: like [`unwrap_fixture_schema`],
/// but additionally carries every column `build_label_grouping_exprs`/
/// `build_default_label_columns` reference unconditionally when a range
/// aggregation has NO `by`/`without` clause (`trace_id`, `span_id`,
/// `severity_text` — the rest of `LOG_INDEXED_ATTRIBUTE_COLUMNS` beyond
/// `service_name`). [`grouping_filter_fixture`]'s narrower schema is
/// sufficient only for a *grouped* aggregation, whose grouping/select
/// expressions reference solely the labels named in the `by`/`without`
/// clause (see `test_count_over_time_grouping_preserves_only_the_by_filtered_attributes`
/// above) — `region` there is not a top-level field, so those columns are
/// never touched.
fn ungrouped_aggregation_fixture_schema() -> Arc<Schema> {
    use icegate_common::schema::{
        COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT,
        COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID,
    };

    Arc::new(Schema::new(vec![
        Field::new(COL_TENANT_ID, DataType::Utf8, false),
        Field::new(COL_TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new(COL_SERVICE_NAME, DataType::Utf8, true),
        Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), true),
        Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), true),
        Field::new(COL_SEVERITY_TEXT, DataType::Utf8, true),
        unwrap_fixture_attribute_map_field(COL_LOG_ATTRIBUTES),
        unwrap_fixture_attribute_map_field(COL_SCOPE_ATTRIBUTES),
        unwrap_fixture_attribute_map_field(COL_RESOURCE_ATTRIBUTES),
    ]))
}

/// Build a `SessionContext` + `QueryContext` for testing an *ungrouped*
/// range aggregation (no `by`/`without` clause) over a `drop`/`keep`-carrying
/// inner pipeline: same grid-point/lookback-window shape as
/// [`grouping_filter_fixture`] (one row per entry of `log_rows`, all falling
/// inside a single grid point's lookback window), but on
/// [`ungrouped_aggregation_fixture_schema`] so the aggregation's
/// unconditional `LOG_INDEXED_ATTRIBUTE_COLUMNS` references resolve.
/// `trace_id`/`span_id`/`severity_text` are present but NULL on every row —
/// nothing under test reads their values, only their existence as columns.
fn ungrouped_aggregation_fixture(log_rows: &[&[(&str, &str)]]) -> (SessionContext, QueryContext) {
    const TENANT: &str = "ungrouped-aggregation-fixture-tenant";
    const SERVICE: &str = "ungrouped-aggregation-fixture-service";

    let grid_point = Utc.timestamp_opt(1_000, 0).unwrap();
    let row_timestamp = Utc.timestamp_opt(900, 0).unwrap().timestamp_micros();
    let row_count = log_rows.len();

    let tenant_ids = StringArray::from(vec![TENANT; row_count]);
    let timestamps = TimestampMicrosecondArray::from(vec![row_timestamp; row_count]);
    let service_names = StringArray::from(vec![Some(SERVICE); row_count]);
    let mut trace_id_builder = FixedSizeBinaryBuilder::new(16);
    trace_id_builder.append_nulls(row_count);
    let mut span_id_builder = FixedSizeBinaryBuilder::new(8);
    span_id_builder.append_nulls(row_count);
    let severity_texts = StringArray::from(vec![None::<&str>; row_count]);
    let log_attrs = unwrap_fixture_attribute_map_column(log_rows);
    let empty_rows: Vec<&[(&str, &str)]> = vec![&[]; row_count];
    let scope_attrs = unwrap_fixture_attribute_map_column(&empty_rows);
    let resource_attrs = unwrap_fixture_attribute_map_column(&empty_rows);

    let batch = RecordBatch::try_new(
        ungrouped_aggregation_fixture_schema(),
        vec![
            Arc::new(tenant_ids),
            Arc::new(timestamps),
            Arc::new(service_names),
            Arc::new(trace_id_builder.finish()),
            Arc::new(span_id_builder.finish()),
            Arc::new(severity_texts),
            log_attrs,
            scope_attrs,
            resource_attrs,
        ],
    )
    .expect("record batch");

    let ctx = SessionContext::new();
    let table = MemTable::try_new(batch.schema(), vec![vec![batch]]).expect("memtable");
    register_logs_fixture(&ctx, Arc::new(table));

    let query_ctx = QueryContext {
        tenant_id: TENANT.to_string(),
        start: grid_point,
        end: grid_point,
        limit: None,
        step: Some(TimeDelta::seconds(15)),
        direction: SortDirection::default(),
        max_grid_points: QueryContext::DEFAULT_MAX_GRID_POINTS,
    };

    (ctx, query_ctx)
}

/// Collect `df` and return each row's `value` alongside its `attributes` map
/// — the combined oracle the tests below need: proving both the aggregate
/// result and the surviving attribute set are correct, not merely that
/// planning didn't error.
async fn collect_value_and_attributes(df: DataFrame) -> Vec<(f64, BTreeMap<String, String>)> {
    let batches = df.collect().await.expect("collect failed");
    let mut rows = Vec::new();
    for batch in &batches {
        let value_col = batch
            .column_by_name("value")
            .expect("value column present")
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value column is Float64");
        let attrs_col = batch
            .column_by_name(MERGED_ATTRIBUTES_COLUMN)
            .expect("attributes column present")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("attributes column is MAP");
        for row in 0..value_col.len() {
            let entries = attrs_col.value(row);
            let keys = entries.column(0).as_any().downcast_ref::<StringArray>().expect("keys");
            let values = entries.column(1).as_any().downcast_ref::<StringArray>().expect("values");
            let attrs = (0..keys.len())
                .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
                .collect();
            rows.push((value_col.value(row), attrs));
        }
    }
    rows
}

#[tokio::test]
async fn test_count_over_time_over_a_drop_without_grouping_reads_the_merged_attributes_column() {
    use crate::logql::metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr};

    // count_over_time({...} | drop user_id [5m]) — no by/without clause.
    let (session_ctx, query_ctx) = ungrouped_aggregation_fixture(&[
        &[("user_id", "u1"), ("region", "us")],
        &[("user_id", "u2"), ("region", "us")],
    ]);
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);
    log_expr.pipeline.push(PipelineStage::Drop(vec![DropKeepLabel::new("user_id")]));
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let agg = RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr);
    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));

    let df = planner.plan(expr).await.expect("Planning failed");
    let rows = collect_value_and_attributes(df).await;

    // Both rows share region=us (identical post-drop attribute sets), so the
    // ungrouped path's "group by every remaining label" collapses them into
    // a single count-2 bucket with user_id gone.
    assert_eq!(
        rows.len(),
        1,
        "one grid point, one merged-attribute group -> one output row"
    );
    let (value, attrs) = &rows[0];
    assert!((value - 2.0).abs() < f64::EPSILON, "expected count 2, got {value}");
    assert_eq!(attrs.get("region").map(String::as_str), Some("us"));
    assert!(
        !attrs.contains_key("user_id"),
        "dropped label user_id must be absent from the output"
    );
}

#[tokio::test]
async fn test_vector_aggregation_by_over_a_drop_reads_the_merged_attributes_column() {
    use crate::logql::{
        common::{Grouping, GroupingLabel},
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr, VectorAggregation, VectorAggregationOp},
    };

    // sum(count_over_time({...} | drop user_id [5m])) by (region): the outer
    // vector aggregation's grouping is pushed down into the inner range
    // aggregation (count_over_time supports grouping), which is where C1's
    // unconditional merged_attributes() call actually lived.
    let (session_ctx, query_ctx) = grouping_filter_fixture(&[
        &[("user_id", "u1"), ("region", "us")],
        &[("user_id", "u2"), ("region", "us")],
        &[("user_id", "u3"), ("region", "eu")],
    ]);
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);
    log_expr.pipeline.push(PipelineStage::Drop(vec![DropKeepLabel::new("user_id")]));
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5));
    let inner = MetricExpr::RangeAggregation(RangeAggregation::new(RangeAggregationOp::CountOverTime, range_expr));
    let grouping = Grouping::By(vec![GroupingLabel::new("region")]);
    let outer = VectorAggregation::new(VectorAggregationOp::Sum, inner).with_grouping(grouping);
    let expr = LogQLExpr::Metric(MetricExpr::VectorAggregation(outer));

    let df = planner.plan(expr).await.expect("Planning failed");
    let rows = collect_value_and_attributes(df).await;

    assert_eq!(rows.len(), 2, "two distinct region groups");
    let by_region: BTreeMap<String, f64> = rows
        .iter()
        .map(|(value, attrs)| (attrs.get("region").expect("region kept by by (region)").clone(), *value))
        .collect();
    assert!(
        (by_region["us"] - 2.0).abs() < f64::EPSILON,
        "region=us: two dropped-user_id rows should sum to 2, got {by_region:?}"
    );
    assert!(
        (by_region["eu"] - 1.0).abs() < f64::EPSILON,
        "region=eu: one row should sum to 1, got {by_region:?}"
    );
    for (_, attrs) in &rows {
        assert!(
            !attrs.contains_key("user_id"),
            "dropped label user_id must be absent from the output"
        );
        assert_eq!(attrs.len(), 1, "by (region) must keep exactly region, nothing else");
    }
}

#[tokio::test]
async fn test_avg_over_time_unwrap_over_a_drop_reads_the_merged_attributes_column() {
    use crate::logql::{
        log::UnwrapExpr,
        metric::{MetricExpr, RangeAggregation, RangeAggregationOp, RangeExpr},
    };

    // avg_over_time({...} | drop user_id | unwrap http_duration_ms [5m]):
    // extract_unwrapped_value's attribute_lookup call is the one C1 left
    // unconditional here, reached before this range aggregation even gets to
    // its own (also-fixed) grouping/merge logic.
    let (session_ctx, query_ctx) =
        ungrouped_aggregation_fixture(&[&[("user_id", "u1"), ("http_duration_ms", "150"), ("region", "us")]]);
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let selector = Selector::new(vec![]);
    let mut log_expr = LogExpr::new(selector);
    log_expr.pipeline.push(PipelineStage::Drop(vec![DropKeepLabel::new("user_id")]));
    let range_expr = RangeExpr::new(log_expr, TimeDelta::minutes(5)).with_unwrap(UnwrapExpr::new("http_duration_ms"));
    let agg = RangeAggregation::new(RangeAggregationOp::AvgOverTime, range_expr);
    let expr = LogQLExpr::Metric(MetricExpr::RangeAggregation(agg));

    let df = planner.plan(expr).await.expect("Planning failed");
    let rows = collect_value_and_attributes(df).await;

    assert_eq!(rows.len(), 1, "one fixture row, one grid point -> one output row");
    let (value, attrs) = &rows[0];
    assert!(
        (value - 150.0).abs() < f64::EPSILON,
        "expected avg 150.0 from the single unwrapped sample, got {value}"
    );
    assert_eq!(
        attrs.get("region").map(String::as_str),
        Some("us"),
        "a non-dropped, non-unwrapped attribute must survive"
    );
    assert!(
        !attrs.contains_key("user_id"),
        "dropped label user_id must be absent from the output"
    );
}
