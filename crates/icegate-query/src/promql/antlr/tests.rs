//! Tests for the ANTLR-based `PromQL` parser.

use super::AntlrParser;
use crate::promql::{AggregationOp, BinaryOperator, Grouping, MatchOp, PromQLExpr, UnaryOperator, parser::Parser};

/// Helper to assert a query parses successfully and return the expression.
fn parse(query: &str) -> PromQLExpr {
    let parser = AntlrParser::new();
    parser.parse(query).unwrap_or_else(|e| panic!("Failed to parse '{query}': {e}"))
}

/// Helper to assert a query fails to parse.
fn assert_fails(query: &str) {
    let parser = AntlrParser::new();
    assert!(parser.parse(query).is_err(), "Expected parse failure for: {query}");
}

// =========================================================================
// Instant selectors
// =========================================================================

#[test]
fn test_instant_selector_metric_only() {
    let expr = parse("up");
    match &expr {
        PromQLExpr::InstantSelector(s) => {
            assert_eq!(s.metric_name.as_deref(), Some("up"));
            assert!(s.matchers.is_empty());
        }
        other => panic!("Expected InstantSelector, got: {other:?}"),
    }
}

#[test]
fn test_instant_selector_with_matchers() {
    let expr = parse(r#"http_requests_total{job="api", status="200"}"#);
    match &expr {
        PromQLExpr::InstantSelector(s) => {
            assert_eq!(s.metric_name.as_deref(), Some("http_requests_total"));
            assert_eq!(s.matchers.len(), 2);
            assert_eq!(s.matchers[0].name, "job");
            assert_eq!(s.matchers[0].op, MatchOp::Eq);
            assert_eq!(s.matchers[0].value, "api");
            assert_eq!(s.matchers[1].name, "status");
            assert_eq!(s.matchers[1].value, "200");
        }
        other => panic!("Expected InstantSelector, got: {other:?}"),
    }
}

#[test]
fn test_instant_selector_matchers_only() {
    let expr = parse(r#"{__name__="up"}"#);
    match &expr {
        PromQLExpr::InstantSelector(s) => {
            assert!(s.metric_name.is_none());
            assert_eq!(s.matchers.len(), 1);
            assert_eq!(s.matchers[0].name, "__name__");
        }
        other => panic!("Expected InstantSelector, got: {other:?}"),
    }
}

#[test]
fn test_instant_selector_regex_matcher() {
    let expr = parse(r#"http_requests_total{status=~"5.."}"#);
    match &expr {
        PromQLExpr::InstantSelector(s) => {
            assert_eq!(s.matchers[0].op, MatchOp::Re);
            assert_eq!(s.matchers[0].value, "5..");
        }
        other => panic!("Expected InstantSelector, got: {other:?}"),
    }
}

#[test]
fn test_instant_selector_nre_matcher() {
    let expr = parse(r#"http_requests_total{status!~"2.."}"#);
    match &expr {
        PromQLExpr::InstantSelector(s) => {
            assert_eq!(s.matchers[0].op, MatchOp::Nre);
        }
        other => panic!("Expected InstantSelector, got: {other:?}"),
    }
}

#[test]
fn test_instant_selector_ne_matcher() {
    let expr = parse(r#"http_requests_total{job!="api"}"#);
    match &expr {
        PromQLExpr::InstantSelector(s) => {
            assert_eq!(s.matchers[0].op, MatchOp::Neq);
        }
        other => panic!("Expected InstantSelector, got: {other:?}"),
    }
}

#[test]
fn test_instant_selector_empty_braces() {
    let expr = parse("up{}");
    match &expr {
        PromQLExpr::InstantSelector(s) => {
            assert_eq!(s.metric_name.as_deref(), Some("up"));
            assert!(s.matchers.is_empty());
        }
        other => panic!("Expected InstantSelector, got: {other:?}"),
    }
}

// =========================================================================
// Matrix selectors
// =========================================================================

#[test]
fn test_matrix_selector_simple() {
    let expr = parse("http_requests_total[5m]");
    match &expr {
        PromQLExpr::MatrixSelector(ms) => {
            assert_eq!(ms.selector.metric_name.as_deref(), Some("http_requests_total"));
            assert_eq!(ms.range.num_minutes(), 5);
        }
        other => panic!("Expected MatrixSelector, got: {other:?}"),
    }
}

#[test]
fn test_matrix_selector_with_matchers() {
    let expr = parse(r#"http_requests_total{job="api"}[5m]"#);
    match &expr {
        PromQLExpr::MatrixSelector(ms) => {
            assert_eq!(ms.selector.metric_name.as_deref(), Some("http_requests_total"));
            assert_eq!(ms.selector.matchers.len(), 1);
            assert_eq!(ms.range.num_minutes(), 5);
        }
        other => panic!("Expected MatrixSelector, got: {other:?}"),
    }
}

#[test]
fn test_matrix_selector_hours() {
    let expr = parse("metric[1h]");
    match &expr {
        PromQLExpr::MatrixSelector(ms) => {
            assert_eq!(ms.range.num_hours(), 1);
        }
        other => panic!("Expected MatrixSelector, got: {other:?}"),
    }
}

// =========================================================================
// Function calls
// =========================================================================

#[test]
fn test_function_rate() {
    let expr = parse("rate(http_requests_total[5m])");
    match &expr {
        PromQLExpr::FunctionCall(fc) => {
            assert_eq!(fc.name, "rate");
            assert_eq!(fc.args.len(), 1);
            assert!(matches!(&fc.args[0], PromQLExpr::MatrixSelector(_)));
        }
        other => panic!("Expected FunctionCall, got: {other:?}"),
    }
}

#[test]
fn test_function_increase() {
    let expr = parse("increase(http_requests_total[1h])");
    match &expr {
        PromQLExpr::FunctionCall(fc) => {
            assert_eq!(fc.name, "increase");
            assert_eq!(fc.args.len(), 1);
        }
        other => panic!("Expected FunctionCall, got: {other:?}"),
    }
}

#[test]
fn test_function_irate() {
    let expr = parse("irate(http_requests_total[5m])");
    match &expr {
        PromQLExpr::FunctionCall(fc) => {
            assert_eq!(fc.name, "irate");
        }
        other => panic!("Expected FunctionCall, got: {other:?}"),
    }
}

#[test]
fn test_function_histogram_quantile() {
    let expr = parse("histogram_quantile(0.95, rate(http_request_duration_bucket[5m]))");
    match &expr {
        PromQLExpr::FunctionCall(fc) => {
            assert_eq!(fc.name, "histogram_quantile");
            assert_eq!(fc.args.len(), 2);
            assert!(matches!(&fc.args[0], PromQLExpr::NumberLiteral(v) if (*v - 0.95).abs() < f64::EPSILON));
        }
        other => panic!("Expected FunctionCall, got: {other:?}"),
    }
}

#[test]
fn test_function_abs() {
    let expr = parse("abs(up)");
    match &expr {
        PromQLExpr::FunctionCall(fc) => {
            assert_eq!(fc.name, "abs");
            assert_eq!(fc.args.len(), 1);
        }
        other => panic!("Expected FunctionCall, got: {other:?}"),
    }
}

#[test]
fn test_function_no_args() {
    let expr = parse("time()");
    match &expr {
        PromQLExpr::FunctionCall(fc) => {
            assert_eq!(fc.name, "time");
            assert!(fc.args.is_empty());
        }
        other => panic!("Expected FunctionCall, got: {other:?}"),
    }
}

// =========================================================================
// Aggregations
// =========================================================================

#[test]
fn test_aggregation_sum() {
    let expr = parse("sum(rate(http_requests_total[5m]))");
    match &expr {
        PromQLExpr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Sum);
            assert!(agg.grouping.is_none());
            assert!(agg.param.is_none());
        }
        other => panic!("Expected Aggregation, got: {other:?}"),
    }
}

#[test]
fn test_aggregation_sum_by() {
    let expr = parse("sum by (job) (rate(http_requests_total[5m]))");
    match &expr {
        PromQLExpr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Sum);
            match &agg.grouping {
                Some(Grouping::By(labels)) => {
                    assert_eq!(labels, &["job"]);
                }
                other => panic!("Expected By grouping, got: {other:?}"),
            }
        }
        other => panic!("Expected Aggregation, got: {other:?}"),
    }
}

#[test]
fn test_aggregation_sum_by_postfix() {
    let expr = parse("sum(rate(http_requests_total[5m])) by (job)");
    match &expr {
        PromQLExpr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Sum);
            match &agg.grouping {
                Some(Grouping::By(labels)) => {
                    assert_eq!(labels, &["job"]);
                }
                other => panic!("Expected By grouping, got: {other:?}"),
            }
        }
        other => panic!("Expected Aggregation, got: {other:?}"),
    }
}

#[test]
fn test_aggregation_avg_without() {
    let expr = parse("avg without (instance) (rate(cpu_usage[5m]))");
    match &expr {
        PromQLExpr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Avg);
            match &agg.grouping {
                Some(Grouping::Without(labels)) => {
                    assert_eq!(labels, &["instance"]);
                }
                other => panic!("Expected Without grouping, got: {other:?}"),
            }
        }
        other => panic!("Expected Aggregation, got: {other:?}"),
    }
}

#[test]
fn test_aggregation_topk() {
    let expr = parse("topk(5, http_requests_total)");
    match &expr {
        PromQLExpr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Topk);
            assert!(agg.param.is_some());
        }
        other => panic!("Expected Aggregation, got: {other:?}"),
    }
}

#[test]
fn test_aggregation_count() {
    let expr = parse("count(up)");
    match &expr {
        PromQLExpr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Count);
        }
        other => panic!("Expected Aggregation, got: {other:?}"),
    }
}

// =========================================================================
// Binary operations
// =========================================================================

#[test]
fn test_binary_add() {
    let expr = parse("up + up");
    match &expr {
        PromQLExpr::BinaryOp(binop) => {
            assert_eq!(binop.op, BinaryOperator::Add);
        }
        other => panic!("Expected BinaryOp, got: {other:?}"),
    }
}

#[test]
fn test_binary_comparison_bool() {
    let expr = parse("up > bool 0.5");
    match &expr {
        PromQLExpr::BinaryOp(binop) => {
            assert_eq!(binop.op, BinaryOperator::Gt);
            assert!(binop.modifier.as_ref().is_some_and(|m| m.return_bool));
        }
        other => panic!("Expected BinaryOp, got: {other:?}"),
    }
}

#[test]
fn test_binary_comparison() {
    let expr = parse("http_requests_total > 100");
    match &expr {
        PromQLExpr::BinaryOp(binop) => {
            assert_eq!(binop.op, BinaryOperator::Gt);
        }
        other => panic!("Expected BinaryOp, got: {other:?}"),
    }
}

// =========================================================================
// Operator precedence
// =========================================================================

#[test]
fn test_precedence_mul_over_add() {
    // a + b * c should parse as a + (b * c)
    let expr = parse("up + up * up");
    match &expr {
        PromQLExpr::BinaryOp(binop) => {
            assert_eq!(binop.op, BinaryOperator::Add);
            assert!(matches!(binop.rhs.as_ref(), PromQLExpr::BinaryOp(inner) if inner.op == BinaryOperator::Mul));
        }
        other => panic!("Expected BinaryOp, got: {other:?}"),
    }
}

#[test]
fn test_precedence_pow_right_assoc() {
    // a ^ b ^ c should parse as a ^ (b ^ c)
    let expr = parse("up ^ up ^ up");
    match &expr {
        PromQLExpr::BinaryOp(binop) => {
            assert_eq!(binop.op, BinaryOperator::Pow);
            assert!(matches!(binop.rhs.as_ref(), PromQLExpr::BinaryOp(inner) if inner.op == BinaryOperator::Pow));
        }
        other => panic!("Expected BinaryOp, got: {other:?}"),
    }
}

// =========================================================================
// Unary operations
// =========================================================================

#[test]
fn test_unary_neg() {
    let expr = parse("-up");
    match &expr {
        PromQLExpr::UnaryOp(unary) => {
            assert_eq!(unary.op, UnaryOperator::Neg);
        }
        other => panic!("Expected UnaryOp, got: {other:?}"),
    }
}

// =========================================================================
// Offset modifier
// =========================================================================

#[test]
fn test_offset_instant_selector() {
    let expr = parse("http_requests_total offset 5m");
    match &expr {
        PromQLExpr::InstantSelector(s) => {
            assert!(s.offset.is_some());
            assert_eq!(s.offset.unwrap().num_minutes(), 5);
        }
        other => panic!("Expected InstantSelector, got: {other:?}"),
    }
}

#[test]
fn test_offset_matrix_selector() {
    let expr = parse("http_requests_total[5m] offset 1h");
    match &expr {
        PromQLExpr::MatrixSelector(ms) => {
            assert!(ms.selector.offset.is_some());
            assert_eq!(ms.selector.offset.unwrap().num_hours(), 1);
        }
        other => panic!("Expected MatrixSelector, got: {other:?}"),
    }
}

// =========================================================================
// @ modifier
// =========================================================================

#[test]
fn test_at_modifier_timestamp() {
    let expr = parse("http_requests_total @ 1609459200");
    match &expr {
        PromQLExpr::InstantSelector(s) => {
            assert!(s.at.is_some());
        }
        other => panic!("Expected InstantSelector, got: {other:?}"),
    }
}

// =========================================================================
// Subqueries
// =========================================================================

#[test]
fn test_subquery_with_step() {
    let expr = parse("rate(http_requests_total[5m])[30m:1m]");
    match &expr {
        PromQLExpr::Subquery(sq) => {
            assert_eq!(sq.range.num_minutes(), 30);
            assert_eq!(sq.step.map(|s| s.num_minutes()), Some(1));
        }
        other => panic!("Expected Subquery, got: {other:?}"),
    }
}

#[test]
fn test_subquery_without_step() {
    let expr = parse("rate(http_requests_total[5m])[30m:]");
    match &expr {
        PromQLExpr::Subquery(sq) => {
            assert_eq!(sq.range.num_minutes(), 30);
            assert!(sq.step.is_none());
        }
        other => panic!("Expected Subquery, got: {other:?}"),
    }
}

// =========================================================================
// Number and string literals
// =========================================================================

#[test]
fn test_number_literal() {
    let expr = parse("42");
    assert!(matches!(expr, PromQLExpr::NumberLiteral(v) if (v - 42.0).abs() < f64::EPSILON));
}

#[test]
fn test_negative_number_literal() {
    let expr = parse("-2.75");
    match &expr {
        PromQLExpr::UnaryOp(u) => {
            assert_eq!(u.op, UnaryOperator::Neg);
            assert!(matches!(u.expr.as_ref(), PromQLExpr::NumberLiteral(v) if (*v - 2.75).abs() < f64::EPSILON));
        }
        other => panic!("Expected UnaryOp, got: {other:?}"),
    }
}

#[test]
fn test_string_literal() {
    let expr = parse(r#""hello""#);
    assert!(matches!(expr, PromQLExpr::StringLiteral(ref s) if s == "hello"));
}

// =========================================================================
// Parenthesized expressions
// =========================================================================

#[test]
fn test_parens() {
    let expr = parse("(up)");
    assert!(matches!(expr, PromQLExpr::Parens(_)));
}

// =========================================================================
// Complex queries
// =========================================================================

#[test]
fn test_complex_rate_sum_by() {
    let expr = parse(r#"sum by (job) (rate(http_requests_total{job="api"}[5m]))"#);
    match &expr {
        PromQLExpr::Aggregation(agg) => {
            assert_eq!(agg.op, AggregationOp::Sum);
            assert!(matches!(&agg.grouping, Some(Grouping::By(labels)) if labels == &["job"]));
            assert!(matches!(agg.expr.as_ref(), PromQLExpr::FunctionCall(fc) if fc.name == "rate"));
        }
        other => panic!("Expected Aggregation, got: {other:?}"),
    }
}

#[test]
fn test_complex_histogram_quantile() {
    let expr =
        parse(r#"histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{job="api"}[5m])))"#);
    match &expr {
        PromQLExpr::FunctionCall(fc) => {
            assert_eq!(fc.name, "histogram_quantile");
            assert_eq!(fc.args.len(), 2);
        }
        other => panic!("Expected FunctionCall, got: {other:?}"),
    }
}

// =========================================================================
// Error cases
// =========================================================================

#[test]
fn test_error_empty_query() {
    assert_fails("");
}

#[test]
fn test_error_unmatched_bracket() {
    assert_fails("http_requests_total[5m");
}

#[test]
fn test_error_unmatched_brace() {
    assert_fails(r#"http_requests_total{job="api""#);
}

#[test]
fn test_error_invalid_operator() {
    assert_fails("up >< up");
}
