//! `TraceQL` parser unit tests.
//!
//! Asserts on AST shapes produced by [`AntlrParser::parse`]. Tests follow
//! the suite layout from the implementation plan (Phase 4 Task 4.2).

use super::AntlrParser;
use crate::traceql::{
    common::{ComparisonOp, FieldRef, IntrinsicField, KindValue, LiteralValue, Scope, StatusValue},
    expr::TraceQLExpr,
    metric::{AggregationOp, MetricsFunction, PipelineExpr, PipelineStage},
    parser::Parser,
    spanset::{SpanFilter, SpanSelector, SpansetExpr, SpansetOp},
};

fn parse(q: &str) -> TraceQLExpr {
    AntlrParser::new()
        .parse(q)
        .unwrap_or_else(|e| panic!("parse failed for {q:?}: {e}"))
}

fn parse_err(q: &str) {
    let r = AntlrParser::new().parse(q);
    assert!(r.is_err(), "expected parse failure for {q:?}, got {r:?}");
}

// =========================================================================
// Empty / minimal selectors
// =========================================================================

#[test]
fn parses_empty_selector() {
    let e = parse("{}");
    assert_eq!(e, TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector::all())));
}

#[test]
fn parses_intrinsic_status_eq_error() {
    let e = parse("{ status = error }");
    let expected = SpanFilter::Compare {
        field: FieldRef::Intrinsic(IntrinsicField::Status),
        op: ComparisonOp::Eq,
        value: LiteralValue::Status(StatusValue::Error),
    };
    assert_eq!(
        e,
        TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector::new(expected)))
    );
}

#[test]
fn parses_intrinsic_kind_eq_server() {
    let e = parse("{ kind = server }");
    let expected = SpanFilter::Compare {
        field: FieldRef::Intrinsic(IntrinsicField::Kind),
        op: ComparisonOp::Eq,
        value: LiteralValue::Kind(KindValue::Server),
    };
    assert_eq!(
        e,
        TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector::new(expected)))
    );
}

#[test]
fn parses_duration_gt_literal() {
    let e = parse("{ duration > 1s }");
    let expected = SpanFilter::Compare {
        field: FieldRef::Intrinsic(IntrinsicField::Duration),
        op: ComparisonOp::Gt,
        value: LiteralValue::Duration(1_000_000_000),
    };
    assert_eq!(
        e,
        TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector::new(expected)))
    );
}

#[test]
fn parses_intrinsic_name_eq_string() {
    let e = parse(r#"{ name = "GET /api" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { field, value, .. }),
    })) = e
    {
        assert_eq!(field, FieldRef::Intrinsic(IntrinsicField::Name));
        assert_eq!(value, LiteralValue::String("GET /api".to_string()));
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_string_literal_with_double_quotes() {
    let e = parse(r#"{ span.http.method = "GET" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { value, .. }),
    })) = e
    {
        assert_eq!(value, LiteralValue::String("GET".to_string()));
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_int_literal() {
    let e = parse("{ span.http.status_code = 200 }");
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { value, .. }),
    })) = e
    {
        assert_eq!(value, LiteralValue::Int(200));
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_nil_literal() {
    let e = parse("{ span.db.system != nil }");
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { op, value, .. }),
    })) = e
    {
        assert_eq!(op, ComparisonOp::Neq);
        assert_eq!(value, LiteralValue::Nil);
    } else {
        panic!("unexpected AST shape");
    }
}

// =========================================================================
// Scoped attribute references
// =========================================================================

#[test]
fn parses_span_dot_attribute() {
    let e = parse(r#"{ span.http.method = "GET" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { field, .. }),
    })) = e
    {
        assert_eq!(
            field,
            FieldRef::Attribute {
                scope: Scope::Span,
                name: "http.method".to_string()
            }
        );
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_resource_dot_attribute() {
    let e = parse(r#"{ resource.service.name = "frontend" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { field, .. }),
    })) = e
    {
        assert_eq!(
            field,
            FieldRef::Attribute {
                scope: Scope::Resource,
                name: "service.name".to_string()
            }
        );
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_dot_shorthand_any_scope() {
    let e = parse(r#"{ .cluster = "prod" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { field, .. }),
    })) = e
    {
        assert_eq!(
            field,
            FieldRef::Attribute {
                scope: Scope::Any,
                name: "cluster".to_string()
            }
        );
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_event_scoped_attribute() {
    let e = parse(r#"{ event.exception.type = "ValueError" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { field, .. }),
    })) = e
    {
        assert_eq!(
            field,
            FieldRef::Attribute {
                scope: Scope::Event,
                name: "exception.type".to_string()
            }
        );
    } else {
        panic!("unexpected AST shape");
    }
}

// =========================================================================
// Boolean composition inside selector
// =========================================================================

#[test]
fn parses_and_inside_selector() {
    let e = parse("{ kind = server && duration > 1s }");
    assert!(matches!(
        e,
        TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
            filter: Some(SpanFilter::And(_, _))
        }))
    ));
}

#[test]
fn parses_or_inside_selector() {
    let e = parse(r"{ status = error || status = unset }");
    assert!(matches!(
        e,
        TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
            filter: Some(SpanFilter::Or(_, _))
        }))
    ));
}

#[test]
fn parses_not_inside_selector() {
    let e = parse("{ ! status = ok }");
    assert!(matches!(
        e,
        TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
            filter: Some(SpanFilter::Not(_))
        }))
    ));
}

#[test]
fn parses_paren_inside_selector() {
    let e = parse("{ (status = error) }");
    assert!(matches!(
        e,
        TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
            filter: Some(SpanFilter::Paren(_))
        }))
    ));
}

// =========================================================================
// Regex match
// =========================================================================

#[test]
fn parses_regex_match() {
    let e = parse(r#"{ span.http.method =~ "DELETE|GET" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { op, .. }),
    })) = e
    {
        assert_eq!(op, ComparisonOp::Re);
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_regex_not_match() {
    let e = parse(r#"{ span.http.method !~ "POST" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { op, .. }),
    })) = e
    {
        assert_eq!(op, ComparisonOp::Nre);
    } else {
        panic!("unexpected AST shape");
    }
}

// =========================================================================
// Spanset operators (parsed; planner rejects in v1)
// =========================================================================

#[test]
fn parses_descendant_operator() {
    let e = parse(r#"{ resource.service.name = "frontend" } >> { span.db.system = "postgresql" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Op { op, .. }) = e {
        assert_eq!(op, SpansetOp::Descendant);
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_child_operator() {
    let e = parse("{ span.http.status_code = 200 } > { span.db.system != nil }");
    if let TraceQLExpr::Spanset(SpansetExpr::Op { op, .. }) = e {
        assert_eq!(op, SpansetOp::Child);
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_ancestor_operator() {
    let e = parse(r"{ status = error } << { kind = client }");
    if let TraceQLExpr::Spanset(SpansetExpr::Op { op, .. }) = e {
        assert_eq!(op, SpansetOp::Ancestor);
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_sibling_operator() {
    let e = parse("{ kind = server } ~ { kind = client }");
    if let TraceQLExpr::Spanset(SpansetExpr::Op { op, .. }) = e {
        assert_eq!(op, SpansetOp::Sibling);
    } else {
        panic!("unexpected AST shape");
    }
}

// =========================================================================
// Pipeline aggregations (search mode)
// =========================================================================

#[test]
fn parses_pipeline_count_filter() {
    let e = parse("{} | count() > 3");
    if let TraceQLExpr::Pipeline(PipelineExpr::Search { stages, .. }) = e {
        assert_eq!(stages.len(), 1);
        assert!(matches!(
            stages[0],
            PipelineStage::AggregateFilter {
                op: AggregationOp::Count,
                ..
            }
        ));
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_pipeline_by_then_count() {
    let e = parse("{ status = error } | by(resource.service.name) | count() > 1");
    if let TraceQLExpr::Pipeline(PipelineExpr::Search { stages, .. }) = e {
        assert_eq!(stages.len(), 2);
        assert!(matches!(stages[0], PipelineStage::By(_)));
        assert!(matches!(stages[1], PipelineStage::AggregateFilter { .. }));
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_pipeline_avg_with_field() {
    let e = parse("{} | avg(duration)");
    if let TraceQLExpr::Pipeline(PipelineExpr::Search { stages, .. }) = e {
        assert_eq!(stages.len(), 1);
        if let PipelineStage::Aggregate { op, arg } = &stages[0] {
            assert_eq!(*op, AggregationOp::Avg);
            assert_eq!(*arg, Some(FieldRef::Intrinsic(IntrinsicField::Duration)));
        } else {
            panic!("expected Aggregate stage");
        }
    } else {
        panic!("unexpected AST shape");
    }
}

// =========================================================================
// Metrics mode
// =========================================================================

#[test]
fn parses_metrics_rate() {
    let e = parse(r#"{ resource.service.name = "frontend" } | rate()"#);
    assert!(e.is_metrics());
}

#[test]
fn parses_metrics_count_over_time_with_by() {
    let e = parse("{} | count_over_time() by (resource.service.name)");
    if let TraceQLExpr::Pipeline(PipelineExpr::Metrics { function, group_by, .. }) = e {
        assert!(matches!(function, MetricsFunction::CountOverTime));
        assert!(group_by.is_some());
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_metrics_histogram_over_time_with_field() {
    let e = parse("{} | histogram_over_time(duration)");
    if let TraceQLExpr::Pipeline(PipelineExpr::Metrics { function, .. }) = e {
        if let MetricsFunction::HistogramOverTime { field } = function {
            assert_eq!(field, FieldRef::Intrinsic(IntrinsicField::Duration));
        } else {
            panic!("expected HistogramOverTime");
        }
    } else {
        panic!("unexpected AST shape");
    }
}

// =========================================================================
// Negative cases
// =========================================================================

#[test]
fn rejects_unbalanced_braces() {
    parse_err("{ status = error");
}

#[test]
fn rejects_invalid_intrinsic() {
    // `bogus` is not an intrinsic; without a scope it should fail unless the
    // grammar promotes it to ".bogus" automatically (it doesn't — leading
    // `.` is required for any-scope shorthand).
    parse_err("{ bogus = 1 }");
}

#[test]
fn rejects_dangling_pipe() {
    parse_err("{} |");
}

#[test]
fn rejects_metrics_function_in_middle_of_pipeline() {
    // metrics functions must be terminal.
    parse_err("{} | rate() | count() > 1");
}

// =========================================================================
// Grafana-compat: bare (unquoted) string values
// =========================================================================
//
// Grafana's Tempo data source sends attribute filters with unquoted values
// like `{resource.service.name=icegate}`. Match that tolerance by treating
// a bare IDENT in literal position as a string.

#[test]
fn parses_bare_word_rhs_as_string_literal() {
    let e = parse("{resource.service.name=icegate}");
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { value, .. }),
    })) = e
    {
        assert_eq!(value, LiteralValue::String("icegate".to_string()));
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn bare_and_quoted_string_rhs_produce_equivalent_ast() {
    let bare = parse("{resource.service.name=icegate}");
    let quoted = parse(r#"{resource.service.name="icegate"}"#);
    assert_eq!(bare, quoted);
}

#[test]
fn enum_literals_still_take_precedence_over_bare_identifiers() {
    // `error` must lex as STATUS_ERROR, not IDENT, so `status = error`
    // stays semantically typed.
    let e = parse("{ status = error }");
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { value, .. }),
    })) = e
    {
        assert_eq!(value, LiteralValue::Status(StatusValue::Error));
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_hyphenated_bare_word_rhs() {
    // Kubernetes-style service names contain hyphens; Grafana sends them bare.
    let e = parse("{resource.service.name=icegate-query}");
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { value, .. }),
    })) = e
    {
        assert_eq!(value, LiteralValue::String("icegate-query".to_string()));
    } else {
        panic!("unexpected AST shape");
    }
}

#[test]
fn parses_hyphenated_attribute_key() {
    // Attribute names themselves can contain hyphens (e.g. `k8s.node-name`).
    let e = parse(r#"{ resource.k8s.node-name = "worker-01" }"#);
    if let TraceQLExpr::Spanset(SpansetExpr::Selector(SpanSelector {
        filter: Some(SpanFilter::Compare { field, .. }),
    })) = e
    {
        assert_eq!(
            field,
            FieldRef::Attribute {
                scope: Scope::Resource,
                name: "k8s.node-name".to_string()
            }
        );
    } else {
        panic!("unexpected AST shape");
    }
}
