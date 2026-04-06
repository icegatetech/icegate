//! Tests for the DataFusion-based `PromQL` query planner.

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion::{
    logical_expr::{LogicalPlan, logical_plan::Filter},
    prelude::{DataFrame, SessionContext},
};
use iceberg_datafusion::IcebergCatalogProvider;
use icegate_common::{
    CatalogBackend, CatalogBuilder, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, METRICS_TABLE, schema::metrics_schema,
};

use super::planner::DataFusionPlanner;
use crate::promql::{
    AggregationOp, BinaryOperator, Grouping, MatchOp, PromQLExpr,
    aggregation::Aggregation,
    binary::{BinaryOp, UnaryOp, UnaryOperator},
    planner::{Planner, PromQLQueryContext},
    selector::{InstantSelector, LabelMatcher, MatrixSelector},
};

// ============================================================================
// Plan Node Helpers
// ============================================================================

/// Extract the `LogicalPlan` from a `DataFrame` for test assertions.
fn get_logical_plan(df: &DataFrame) -> &LogicalPlan {
    df.logical_plan()
}

/// Collect all `Filter` nodes from a plan tree.
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

// ============================================================================
// Test Setup
// ============================================================================

/// Creates a `SessionContext` with an in-memory Iceberg catalog containing
/// a metrics table, plus a `PromQLQueryContext` for testing.
async fn create_test_context() -> (SessionContext, PromQLQueryContext) {
    let session_ctx = SessionContext::new();

    let warehouse_path = tempfile::tempdir().expect("Failed to create temp dir");
    let warehouse_str = warehouse_path.path().to_str().unwrap().to_string();

    let config = CatalogConfig {
        backend: CatalogBackend::Memory,
        warehouse: warehouse_str,
        properties: std::collections::HashMap::new(),
        cache: None,
    };

    let iceberg_catalog = CatalogBuilder::from_config(&config, &IoHandle::noop())
        .await
        .expect("Failed to create test catalog");

    let namespace = iceberg::NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    if !iceberg_catalog.namespace_exists(&namespace).await.unwrap_or(false) {
        iceberg_catalog
            .create_namespace(&namespace, std::collections::HashMap::new())
            .await
            .expect("Failed to create namespace");
    }

    let schema = metrics_schema().expect("Failed to get metrics schema");
    let table_creation = iceberg::TableCreation::builder()
        .name(METRICS_TABLE.to_string())
        .schema(schema)
        .build();

    let _ = iceberg_catalog.create_table(&namespace, table_creation).await;

    let iceberg_provider = IcebergCatalogProvider::try_new(iceberg_catalog)
        .await
        .expect("Failed to create IcebergCatalogProvider");
    session_ctx.register_catalog("iceberg", Arc::new(iceberg_provider));

    let query_ctx = PromQLQueryContext::instant("test-tenant", Utc.timestamp_opt(1000, 0).unwrap());

    (session_ctx, query_ctx)
}

// ============================================================================
// Instant Selector Tests
// ============================================================================

#[tokio::test]
async fn test_instant_selector_basic() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let sel = InstantSelector::new("http_requests_total");
    let df = planner.plan(PromQLExpr::InstantSelector(sel)).await.expect("Planning failed");

    let plan = get_logical_plan(&df);
    let filters = collect_filters(plan);

    // Must have at least tenant_id, time range, and metric_name filters
    assert!(
        filters.len() >= 3,
        "Expected at least 3 filters (tenant, time, metric), got {}",
        filters.len()
    );
}

#[tokio::test]
async fn test_instant_selector_with_matchers() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let sel = InstantSelector::with_matchers(
        "http_requests_total",
        vec![
            LabelMatcher::new("job", MatchOp::Eq, "api"),
            LabelMatcher::new("status", MatchOp::Neq, "500"),
        ],
    );

    let df = planner.plan(PromQLExpr::InstantSelector(sel)).await.expect("Planning failed");

    let plan = get_logical_plan(&df);
    let plan_str = format!("{}", plan.display_indent());

    // Verify the plan contains references to our label matchers
    assert!(
        plan_str.contains("job") && plan_str.contains("api"),
        "Plan should reference job=api matcher"
    );
}

// ============================================================================
// Matrix Selector Tests
// ============================================================================

#[tokio::test]
async fn test_matrix_selector_basic() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let instant = InstantSelector::new("http_requests_total");
    let matrix = MatrixSelector::new(instant, chrono::TimeDelta::minutes(5));

    let df = planner.plan(PromQLExpr::MatrixSelector(matrix)).await.expect("Planning failed");

    let plan = get_logical_plan(&df);
    let filters = collect_filters(plan);

    // Must have tenant_id, time range, and metric_name filters
    assert!(filters.len() >= 3, "Expected at least 3 filters, got {}", filters.len());
}

// ============================================================================
// Aggregation Tests
// ============================================================================

#[tokio::test]
async fn test_aggregation_sum() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let sel = InstantSelector::new("http_requests_total");
    let agg = Aggregation::new(AggregationOp::Sum, PromQLExpr::InstantSelector(sel));

    let df = planner.plan(PromQLExpr::Aggregation(agg)).await.expect("Planning failed");

    let plan = get_logical_plan(&df);
    let plan_str = format!("{}", plan.display_indent());
    assert!(plan_str.contains("sum"), "Plan should contain sum aggregate");
}

#[tokio::test]
async fn test_aggregation_sum_by() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let sel = InstantSelector::new("http_requests_total");
    let agg = Aggregation::new(AggregationOp::Sum, PromQLExpr::InstantSelector(sel))
        .with_grouping(Grouping::By(vec!["job".to_string()]));

    let df = planner.plan(PromQLExpr::Aggregation(agg)).await.expect("Planning failed");

    let plan = get_logical_plan(&df);
    let plan_str = format!("{}", plan.display_indent());
    assert!(plan_str.contains("sum"), "Plan should contain sum aggregate");
    assert!(plan_str.contains("job"), "Plan should reference 'job' grouping label");
}

#[tokio::test]
async fn test_aggregation_unsupported() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let sel = InstantSelector::new("http_requests_total");
    let agg = Aggregation::new(AggregationOp::Topk, PromQLExpr::InstantSelector(sel));

    let result = planner.plan(PromQLExpr::Aggregation(agg)).await;
    assert!(result.is_err(), "topk should return NotImplemented");
}

// ============================================================================
// Binary Op Tests
// ============================================================================

#[tokio::test]
async fn test_binary_op_scalar_right() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let sel = InstantSelector::new("http_requests_total");
    let binop = BinaryOp::new(
        BinaryOperator::Mul,
        PromQLExpr::InstantSelector(sel),
        PromQLExpr::NumberLiteral(100.0),
    );

    let df = planner.plan(PromQLExpr::BinaryOp(binop)).await.expect("Planning failed");

    // Verify the plan was created without error
    let _plan = get_logical_plan(&df);
}

#[tokio::test]
async fn test_binary_op_vector_vector_not_implemented() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let lhs = InstantSelector::new("metric_a");
    let rhs = InstantSelector::new("metric_b");
    let binop = BinaryOp::new(
        BinaryOperator::Add,
        PromQLExpr::InstantSelector(lhs),
        PromQLExpr::InstantSelector(rhs),
    );

    let result = planner.plan(PromQLExpr::BinaryOp(binop)).await;
    assert!(result.is_err(), "vector-vector should return NotImplemented");
}

// ============================================================================
// Unary Op Tests
// ============================================================================

#[tokio::test]
async fn test_unary_neg() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let sel = InstantSelector::new("http_requests_total");
    let unary = UnaryOp::new(UnaryOperator::Neg, PromQLExpr::InstantSelector(sel));

    let df = planner.plan(PromQLExpr::UnaryOp(unary)).await.expect("Planning failed");

    let _plan = get_logical_plan(&df);
}

// ============================================================================
// Literal Tests
// ============================================================================

#[tokio::test]
async fn test_number_literal() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let df = planner.plan(PromQLExpr::NumberLiteral(42.0)).await.expect("Planning failed");

    let _plan = get_logical_plan(&df);
}

#[tokio::test]
async fn test_string_literal() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let df = planner
        .plan(PromQLExpr::StringLiteral("hello".to_string()))
        .await
        .expect("Planning failed");

    let _plan = get_logical_plan(&df);
}

// ============================================================================
// Parens Test
// ============================================================================

#[tokio::test]
async fn test_parens_delegates_to_inner() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let inner = PromQLExpr::NumberLiteral(99.0);
    let df = planner
        .plan(PromQLExpr::Parens(Box::new(inner)))
        .await
        .expect("Planning failed");

    let _plan = get_logical_plan(&df);
}

// ============================================================================
// NotImplemented Tests
// ============================================================================

#[tokio::test]
async fn test_subquery_not_implemented() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let sel = InstantSelector::new("http_requests_total");
    let subquery =
        crate::promql::binary::Subquery::new(PromQLExpr::InstantSelector(sel), chrono::TimeDelta::minutes(30));

    let result = planner.plan(PromQLExpr::Subquery(subquery)).await;
    assert!(result.is_err(), "subquery should return NotImplemented");
}

#[tokio::test]
async fn test_function_call_not_implemented() {
    let (session_ctx, query_ctx) = create_test_context().await;
    let planner = DataFusionPlanner::new(session_ctx, query_ctx);

    let sel = InstantSelector::new("http_requests_total");
    let matrix = MatrixSelector::new(sel, chrono::TimeDelta::minutes(5));
    let func = crate::promql::function::FunctionCall::new("rate", vec![PromQLExpr::MatrixSelector(matrix)]);

    let result = planner.plan(PromQLExpr::FunctionCall(func)).await;
    assert!(result.is_err(), "rate() should return NotImplemented");
}
