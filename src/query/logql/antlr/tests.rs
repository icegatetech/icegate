//! Tests for ANTLR-based LogQL parser.

use super::*;
use antlr4rust::Parser as AntlrParserTrait;
use antlr4rust::common_token_stream::CommonTokenStream;
use antlr4rust::input_stream::InputStream;
use antlr4rust::tree::ParseTree;

use crate::query::logql::common::*;
use crate::query::logql::expr::*;
use crate::query::logql::log::*;
use crate::query::logql::metric::*;
use crate::query::logql::parser::Parser; // Import the Parser trait

/// Helper that expects parsing to succeed and match the expected expression
fn assert_parses_to(query: &str, expected: LogQLExpr) {
    let parser = AntlrParser::new();
    match parser.parse(query) {
        Ok(expr) => {
            assert_eq!(
                expr, expected,
                "Parsed expression does not match expected for query: '{}'",
                query
            );
        },
        Err(e) => {
            panic!("Query should parse successfully: '{}'\nError: {:?}", query, e);
        },
    }
}

/// Helper function to parse a LogQL query with error listeners on both lexer and parser
fn parse_query(query: &str) -> std::result::Result<(), Vec<String>> {
    let input = InputStream::new(query);
    let mut lexer = LogQLLexer::new(input);

    // Add error listener to lexer
    let lexer_error_listener = CollectingErrorListener::new();
    lexer.remove_error_listeners();
    lexer.add_error_listener(Box::new(lexer_error_listener.clone()));

    let token_source = CommonTokenStream::new(lexer);
    let mut parser = LogQLParser::new(token_source);

    // Add error listener to parser
    let parser_error_listener = CollectingErrorListener::new();
    parser.remove_error_listeners();
    parser.add_error_listener(Box::new(parser_error_listener.clone()));

    // Parse the query
    match parser.root() {
        Ok(_) => {
            // Check both lexer and parser errors
            let mut all_errors: Vec<String> = lexer_error_listener.get_errors().iter().map(|e| e.to_string()).collect();
            all_errors.extend(parser_error_listener.get_errors().iter().map(|e| e.to_string()));

            if all_errors.is_empty() { Ok(()) } else { Err(all_errors) }
        },
        Err(_) => {
            // Collect all errors from both lexer and parser
            let mut all_errors: Vec<String> = lexer_error_listener.get_errors().iter().map(|e| e.to_string()).collect();
            all_errors.extend(parser_error_listener.get_errors().iter().map(|e| e.to_string()));
            Err(all_errors)
        },
    }
}

/// Helper that expects parsing to succeed
fn assert_parses(query: &str) {
    match parse_query(query) {
        Ok(_) => {},
        Err(errors) => {
            panic!(
                "Query should parse successfully: '{}'\nErrors:\n{}",
                query,
                errors.join("\n")
            );
        },
    }
}

/// Helper that expects parsing to fail
fn assert_fails(query: &str) {
    match parse_query(query) {
        Ok(_) => panic!("Query should fail to parse: '{}'", query),
        Err(_) => {},
    }
}

/// Get parse tree as string for inspection using Debug format
fn get_parse_tree(query: &str) -> std::result::Result<String, Vec<String>> {
    let input = InputStream::new(query);
    let mut lexer = LogQLLexer::new(input);

    // Add error listener to lexer
    let lexer_error_listener = CollectingErrorListener::new();
    lexer.remove_error_listeners();
    lexer.add_error_listener(Box::new(lexer_error_listener.clone()));

    let token_source = CommonTokenStream::new(lexer);
    let mut parser = LogQLParser::new(token_source);

    // Add error listener to parser
    let parser_error_listener = CollectingErrorListener::new();
    parser.remove_error_listeners();
    parser.add_error_listener(Box::new(parser_error_listener.clone()));

    // Parse the query
    let parse_result = parser.root();

    match parse_result {
        Ok(tree) => {
            // Check both lexer and parser errors
            let mut all_errors: Vec<String> = lexer_error_listener.get_errors().iter().map(|e| e.to_string()).collect();
            all_errors.extend(parser_error_listener.get_errors().iter().map(|e| e.to_string()));

            if all_errors.is_empty() {
                // Use Debug format to get string representation of parse tree
                Ok(tree.to_string_tree(&*parser))
            } else {
                Err(all_errors)
            }
        },
        Err(_) => {
            // Collect all errors from both lexer and parser
            let mut all_errors: Vec<String> = lexer_error_listener.get_errors().iter().map(|e| e.to_string()).collect();
            all_errors.extend(parser_error_listener.get_errors().iter().map(|e| e.to_string()));
            Err(all_errors)
        },
    }
}

/// Parse and validate that tree contains expected rule names
fn assert_tree_contains(query: &str, expected_rules: &[&str]) {
    match get_parse_tree(query) {
        Ok(tree_str) => {
            for rule in expected_rules {
                if !tree_str.contains(rule) {
                    panic!(
                        "Parse tree validation failed for query: '{}'\nExpected to contain rule: '{}'\nParse tree:\n{}",
                        query, rule, tree_str
                    );
                }
            }
        },
        Err(errors) => {
            panic!(
                "Query should parse successfully: '{}'\nErrors:\n{}",
                query,
                errors.join("\n")
            );
        },
    }
}

// ==================== Basic Selector Tests ====================

#[test]
fn test_empty_selector() {
    assert_parses("{}");
}

#[test]
fn test_basic_selector() {
    assert_parses_to(
        r#"{job="mysql"}"#,
        LogQLExpr::Log(LogExpr::new(Selector::new(vec![LabelMatcher::eq("job", "mysql")]))),
    );
    assert_parses_to(
        r#"{namespace="prod", pod="app-123"}"#,
        LogQLExpr::Log(LogExpr::new(Selector::new(vec![
            LabelMatcher::eq("namespace", "prod"),
            LabelMatcher::eq("pod", "app-123"),
        ]))),
    );
}

#[test]
fn test_selector_operators() {
    assert_parses_to(
        r#"{job="mysql"}"#,
        LogQLExpr::Log(LogExpr::new(Selector::new(vec![LabelMatcher::eq("job", "mysql")]))),
    );
    assert_parses_to(
        r#"{job!="mysql"}"#,
        LogQLExpr::Log(LogExpr::new(Selector::new(vec![LabelMatcher::neq("job", "mysql")]))),
    );
    assert_parses_to(
        r#"{job=~"mysql.*"}"#,
        LogQLExpr::Log(LogExpr::new(Selector::new(vec![LabelMatcher::re("job", "mysql.*")]))),
    );
    assert_parses_to(
        r#"{job!~"mysql.*"}"#,
        LogQLExpr::Log(LogExpr::new(Selector::new(vec![LabelMatcher::nre("job", "mysql.*")]))),
    );
}

#[test]
fn test_selector_with_prefix() {
    assert_parses(r#"{resource:job="mysql"}"#);
    assert_parses(r#"{log:level="error"}"#);
    assert_parses(r#"{scope:name="api"}"#);
}

// ==================== Line Filter Tests ====================

#[test]
fn test_line_filter_contains() {
    assert_parses_to(
        r#"{job="mysql"} |= "error""#,
        LogQLExpr::Log(LogExpr::with_pipeline(
            Selector::new(vec![LabelMatcher::eq("job", "mysql")]),
            vec![PipelineStage::LineFilter(LineFilter::contains("error"))],
        )),
    );
    assert_parses(r#"{job="mysql"} |= "error" or "warning""#);
}

#[test]
fn test_line_filter_not_contains() {
    assert_parses(r#"{job="mysql"} != "debug""#);
    assert_parses(r#"{job="mysql"} != "info" or "trace""#);
}

#[test]
fn test_line_filter_regex() {
    assert_parses(r#"{job="mysql"} |~ "error.*""#);
    assert_parses(r#"{job="mysql"} !~ "debug|trace""#);
}

#[test]
fn test_line_filter_pattern() {
    assert_parses(r#"{job="mysql"} !> "pattern""#);
}

#[test]
fn test_line_filter_ip_function() {
    assert_parses(r#"{job="access"} |= ip("192.168.1.0/24")"#);
    assert_parses(r#"{job="access"} |= ip("::1")"#);
    assert_parses(r#"{job="access"} |= ip("192.168.0.1-192.168.0.255")"#);
}

// ==================== Parser Tests ====================

#[test]
fn test_json_parser() {
    assert_parses_to(
        r#"{job="app"} | json"#,
        LogQLExpr::Log(LogExpr::with_pipeline(
            Selector::new(vec![LabelMatcher::eq("job", "app")]),
            vec![PipelineStage::LogParser(LogParser::json())],
        )),
    );
    assert_parses(r#"{job="app"} | json field1, field2"#);
    assert_parses(r#"{job="app"} | json nested="$.path.to.field""#);
}

#[test]
fn test_logfmt_parser() {
    assert_parses(r#"{job="app"} | logfmt"#);
    assert_parses(r#"{job="app"} | logfmt --strict"#);
    assert_parses(r#"{job="app"} | logfmt --keep-empty"#);
    assert_parses(r#"{job="app"} | logfmt --strict --keep-empty"#);
    assert_parses(r#"{job="app"} | logfmt field1, field2"#);
}

#[test]
fn test_pattern_parser() {
    assert_parses(r#"{job="app"} | pattern "<ip> - <user>""#);
    assert_parses(r#"{job="nginx"} | pattern "<_> <method> <path> <status>""#);
}

#[test]
fn test_regexp_parser() {
    assert_parses(r#"{job="app"} | regexp "(?P<user>\w+)""#);
}

#[test]
fn test_unpack_parser() {
    assert_parses(r#"{job="app"} | unpack"#);
}

// ==================== Label Format Tests ====================

#[test]
fn test_label_format_rename() {
    assert_parses(r#"{job="app"} | label_format dst=src"#);
    assert_parses(r#"{job="app"} | label_format foo=bar, baz=qux"#);
}

#[test]
fn test_label_format_template() {
    assert_parses(r#"{job="app"} | label_format dst="{{.status}} {{.query}}""#);
    assert_parses(r#"{job="app"} | label_format foo="static", bar="{{.value}}""#);
}

#[test]
fn test_label_format_mixed() {
    assert_parses(r#"{job="app"} | label_format dst=src, formatted="{{.field}}""#);
}

// ==================== Line Format Tests ====================

#[test]
fn test_line_format() {
    assert_parses(r#"{job="app"} | line_format "{{.field}}""#);
    assert_parses(r#"{job="app"} | line_format "{{.timestamp}} {{.message}}""#);
    assert_parses(r#"{job="app"} | json | line_format "{{.status}} - {{.query}}""#);
}

#[test]
fn test_line_format_with_functions() {
    assert_parses(r#"{job="app"} | line_format "{{.field | upper}}""#);
    assert_parses(r#"{job="app"} | line_format "{{printf \"%-40.40s\" .request_uri}}""#);
}

// ==================== Label Management Tests ====================

#[test]
fn test_drop_labels() {
    assert_parses(r#"{job="app"} | json | drop field1, field2"#);
}

#[test]
fn test_keep_labels() {
    assert_parses(r#"{job="app"} | json | keep field1, field2"#);
}

#[test]
fn test_decolorize() {
    assert_parses(r#"{job="app"} | decolorize"#);
}

// ==================== Label Filter Tests ====================

#[test]
fn test_label_filter_after_parser() {
    assert_parses(r#"{job="app"} | json | status="200""#);
    assert_parses(r#"{job="app"} | logfmt | level!="debug""#);
    assert_parses(r#"{job="app"} | json | request_time > 10s"#);
    assert_parses(r#"{job="app"} | logfmt | status_code >= 200"#);
}

#[test]
fn test_label_filter_ip_function() {
    assert_parses(r#"{job="app"} | json | client_ip = ip("192.168.0.0/16")"#);
    assert_parses(r#"{job="app"} | logfmt | remote_ip != ip("10.0.0.0/8")"#);
}

// ==================== Complex Pipeline Tests ====================

#[test]
fn test_complex_pipeline() {
    assert_parses(r#"{job="mysql"} |= "error" | json | request_time > 10s | line_format "{{.query}}""#);
}

#[test]
fn test_multi_stage_pipeline() {
    assert_parses(
        r#"{container="query-frontend"}
        |= "metrics.go"
        | logfmt
        | request_time > 10s
        | line_format "{{.ts}}\t{{.duration}}""#,
    );
}

// ==================== Range Aggregation Tests ====================

#[test]
fn test_count_over_time() {
    assert_parses_to(
        r#"count_over_time({job="mysql"}[5m])"#,
        LogQLExpr::Metric(MetricExpr::RangeAggregation(RangeAggregation::new(
            RangeAggregationOp::CountOverTime,
            RangeExpr::new(
                LogExpr::new(Selector::new(vec![LabelMatcher::eq("job", "mysql")])),
                Duration::from_mins(5),
            ),
        ))),
    );
    assert_parses(r#"count_over_time({job="mysql"} |= "error" [1h])"#);
}

#[test]
fn test_rate() {
    assert_parses(r#"rate({job="mysql"}[5m])"#);
    assert_parses(r#"rate({job="mysql"} | json | unwrap request_bytes [1m])"#);
}

#[test]
fn test_bytes_over_time() {
    assert_parses(r#"bytes_over_time({job="mysql"}[5m])"#);
    assert_parses(r#"bytes_rate({job="mysql"}[1m])"#);
}

#[test]
fn test_unwrap_aggregations() {
    assert_parses(r#"sum_over_time({job="app"} | json | unwrap request_time [5m])"#);
    assert_parses(r#"avg_over_time({job="app"} | json | unwrap response_bytes [1m])"#);
    assert_parses(r#"max_over_time({job="app"} | json | unwrap latency [5m])"#);
    assert_parses(r#"min_over_time({job="app"} | json | unwrap size [1m])"#);
    assert_parses(r#"stddev_over_time({job="app"} | json | unwrap value [5m])"#);
    assert_parses(r#"quantile_over_time(0.99, {job="app"} | json | unwrap request_time [5m])"#);
}

#[test]
fn test_unwrap_with_conversion() {
    assert_parses(r#"sum_over_time({job="app"} | json | unwrap bytes(size) [5m])"#);
    assert_parses(r#"avg_over_time({job="app"} | json | unwrap duration(latency) [1m])"#);
    assert_parses(r#"max_over_time({job="app"} | json | unwrap duration_seconds(time) [5m])"#);
}

#[test]
fn test_rate_counter() {
    assert_parses(r#"rate_counter({job="app"} | json | unwrap counter [1m])"#);
}

#[test]
fn test_absent_over_time() {
    assert_parses(r#"absent_over_time({job="mysql"}[5m])"#);
}

// ==================== Vector Aggregation Tests ====================

#[test]
fn test_vector_aggregations() {
    assert_parses_to(
        r#"sum(rate({job="mysql"}[5m]))"#,
        LogQLExpr::Metric(MetricExpr::VectorAggregation(VectorAggregation::new(
            VectorAggregationOp::Sum,
            MetricExpr::RangeAggregation(RangeAggregation::new(
                RangeAggregationOp::Rate,
                RangeExpr::new(
                    LogExpr::new(Selector::new(vec![LabelMatcher::eq("job", "mysql")])),
                    Duration::from_mins(5),
                ),
            )),
        ))),
    );
    assert_parses(r#"avg(count_over_time({job="app"}[1m]))"#);
    assert_parses(r#"max(rate({job="api"}[5m]))"#);
    assert_parses(r#"min(bytes_rate({job="app"}[1m]))"#);
    assert_parses(r#"count(rate({job="mysql"}[5m]))"#);
    assert_parses(r#"stddev(rate({job="app"}[5m]))"#);
    assert_parses(r#"stdvar(rate({job="app"}[5m]))"#);
}

#[test]
fn test_topk_bottomk() {
    assert_parses(r#"topk(10, rate({job="mysql"}[5m]))"#);
    assert_parses(r#"bottomk(5, count_over_time({job="app"}[1m]))"#);
}

#[test]
fn test_approx_topk() {
    assert_parses(r#"approx_topk(10, sum(rate({job="mysql"}[5m])) by (pod))"#);
    assert_parses(r#"approx_topk(100, count_over_time({job="app"}[5m]))"#);
}

#[test]
fn test_sort() {
    assert_parses(r#"sort(rate({job="mysql"}[5m]))"#);
    assert_parses(r#"sort_desc(count_over_time({job="app"}[1m]))"#);
}

#[test]
fn test_grouping_by() {
    assert_parses(r#"sum by (pod) (rate({job="mysql"}[5m]))"#);
    assert_parses(r#"avg by (namespace, pod) (count_over_time({job="app"}[1m]))"#);
    assert_parses(r#"max by (region) (bytes_rate({job="api"}[5m]))"#);
}

#[test]
fn test_grouping_without() {
    assert_parses(r#"sum without (pod) (rate({job="mysql"}[5m]))"#);
    assert_parses(r#"avg without (instance) (count_over_time({job="app"}[1m]))"#);
}

#[test]
fn test_grouping_with_prefix() {
    assert_parses(r#"sum by (resource:namespace) (rate({job="mysql"}[5m]))"#);
    assert_parses(r#"avg by (log:level, scope:name) (count_over_time({job="app"}[1m]))"#);
}

#[test]
fn test_grouping_empty() {
    assert_parses(r#"sum by () (rate({job="mysql"}[5m]))"#);
    assert_parses(r#"avg without () (count_over_time({job="app"}[1m]))"#);
}

#[test]
fn test_topk_with_grouping() {
    assert_parses(r#"topk(10, sum(rate({job="mysql"}[5m])) by (pod))"#);
    assert_parses(r#"topk(5, rate({job="app"}[1m])) by (namespace)"#);
}

// ==================== Binary Operator Tests ====================

#[test]
fn test_arithmetic_operators() {
    assert_parses(r#"rate({job="a"}[5m]) + rate({job="b"}[5m])"#);
    assert_parses(r#"rate({job="a"}[5m]) - rate({job="b"}[5m])"#);
    assert_parses_to(
        r#"rate({job="a"}[5m]) * 100"#,
        LogQLExpr::Metric(MetricExpr::BinaryOp {
            left: Box::new(MetricExpr::RangeAggregation(RangeAggregation::new(
                RangeAggregationOp::Rate,
                RangeExpr::new(
                    LogExpr::new(Selector::new(vec![LabelMatcher::eq("job", "a")])),
                    Duration::from_mins(5),
                ),
            ))),
            op: BinaryOp::Mul,
            modifier: Some(BinaryOpModifier {
                return_bool: false,
                vector_matching: None,
            }),
            right: Box::new(MetricExpr::Literal(100.0)),
        }),
    );
    assert_parses(r#"rate({job="a"}[5m]) / rate({job="b"}[5m])"#);
    assert_parses(r#"rate({job="a"}[5m]) % 10"#);
    assert_parses(r#"2 ^ 3"#);
}

#[test]
fn test_comparison_operators() {
    assert_parses(r#"rate({job="a"}[5m]) == 100"#);
    assert_parses(r#"rate({job="a"}[5m]) != 0"#);
    assert_parses(r#"rate({job="a"}[5m]) > 100"#);
    assert_parses(r#"rate({job="a"}[5m]) >= 100"#);
    assert_parses(r#"rate({job="a"}[5m]) < 100"#);
    assert_parses(r#"rate({job="a"}[5m]) <= 100"#);
}

#[test]
fn test_logical_operators() {
    assert_parses(r#"rate({job="a"}[5m]) and rate({job="b"}[5m])"#);
    assert_parses(r#"rate({job="a"}[5m]) or rate({job="b"}[5m])"#);
    assert_parses(r#"rate({job="a"}[5m]) unless rate({job="b"}[5m])"#);
}

#[test]
fn test_bool_modifier() {
    assert_parses(r#"rate({job="a"}[5m]) == bool 100"#);
    assert_parses(r#"rate({job="a"}[5m]) > bool rate({job="b"}[5m])"#);
}

#[test]
fn test_on_ignoring_modifiers() {
    assert_parses(r#"rate({job="a"}[5m]) + on(pod) rate({job="b"}[5m])"#);
    assert_parses(r#"rate({job="a"}[5m]) / ignoring(instance) rate({job="b"}[5m])"#);
    assert_parses(r#"sum by(app) (rate({job="foo"}[1m])) / on() sum(rate({job="foo"}[1m]))"#);
}

#[test]
fn test_group_left_right() {
    assert_parses(r#"rate({job="a"}[5m]) / on(pod) group_left rate({job="b"}[5m])"#);
    assert_parses(r#"rate({job="a"}[5m]) * on(namespace) group_right(pod) rate({job="b"}[5m])"#);
}

#[test]
fn test_operator_precedence() {
    assert_parses(r#"1 + 2 * 3"#);
    assert_parses(r#"2 ^ 3 ^ 4"#);
    assert_parses(r#"(1 + 2) * 3"#);
}

// ==================== Offset and @ Modifier Tests ====================

#[test]
fn test_offset() {
    assert_parses(r#"rate({job="mysql"}[5m] offset 1h)"#);
    assert_parses(r#"count_over_time({job="app"}[1m] offset 30m)"#);
}

#[test]
fn test_at_modifier() {
    assert_parses(r#"rate({job="mysql"}[5m] @ 1234567890)"#);
    assert_parses(r#"count_over_time({job="app"}[1m] @ 1609746000)"#);
}

#[test]
fn test_offset_and_at_combined() {
    assert_parses(r#"rate({job="mysql"}[5m] offset 1h @ 1234567890)"#);
}

// ==================== Special Function Tests ====================

#[test]
fn test_vector_function() {
    assert_parses(r#"vector(0)"#);
    assert_parses(r#"vector(100)"#);
    assert_parses(r#"sum(count_over_time({job="foo"}[5m])) or vector(0)"#);
}

#[test]
fn test_label_replace() {
    assert_parses(r#"label_replace(rate({job="api"}[1m]), "foo", "$1", "service", "(.*):.*")"#);
}

// ==================== Literal Expression Tests ====================

#[test]
fn test_number_literals() {
    assert_parses(r#"42"#);
    assert_parses(r#"3.14"#);
    assert_parses(r#".5"#);
    assert_parses(r#"1e10"#);
    assert_parses(r#"1.5e-3"#);
    assert_parses(r#"0x1234"#);
}

#[test]
fn test_signed_numbers() {
    assert_parses(r#"+42"#);
    assert_parses(r#"-3.14"#);
    assert_parses(r#"-1e10"#);
}

// ==================== Variable Expression Tests ====================

#[test]
fn test_variable_expressions() {
    assert_parses(r#"my_metric"#);
    assert_parses(r#"metric_name"#);
    assert_parses(r#"my_metric + 10"#);
}

// ==================== Complex Query Tests ====================

#[test]
fn test_grafana_docs_examples() {
    // Example from Grafana Loki docs
    assert_parses(r#"sum by(machine) (count_over_time({app="foo"}[1m])) / on() sum(count_over_time({app="foo"}[1m]))"#);

    assert_parses(
        r#"sum without(app) (count_over_time({app="foo"}[1m])) > sum without(app) (count_over_time({app="bar"}[1m]))"#,
    );

    assert_parses(
        r#"{cluster="ops-tools1", name="querier"}
        |= "metrics.go"
        != "loki-canary"
        | logfmt
        | query != ""
        | line_format "{{.ts}}\t{{.duration}}\ttraceID = {{.traceID}}""#,
    );
}

#[test]
fn test_real_world_queries() {
    // Real-world query examples
    assert_parses(r#"sum(rate({namespace="production"} |= "error" [5m])) by (pod)"#);

    assert_parses(r#"topk(10, sum by (path) (rate({job="nginx"} | json | status >= 500 [5m])))"#);

    assert_parses(
        r#"avg_over_time({job="app"}
        | json
        | unwrap request_time
        | status="200" [5m])
        by (endpoint)"#,
    );

    assert_parses(
        r#"count_over_time({namespace="prod"}
        |= "OutOfMemory"
        | pattern "<_> pod=<pod> <_>" [1h])"#,
    );
}

#[test]
fn test_approx_topk_real_world() {
    assert_parses(r#"approx_topk(100, sum by (endpoint) (rate({job="api"}[5m])))"#);

    assert_parses(r#"approx_topk(50, count_over_time({namespace="prod"} |= "error" [1h]))"#);
}

#[test]
fn test_all_new_features_combined() {
    // Test combining all newly added features
    assert_parses(
        r#"approx_topk(10,
            sum by (endpoint) (
                rate({job="api"}
                |= ip("192.168.0.0/16")
                | pattern "<method> <path> <status>"
                | unpack
                | json
                | line_format "{{.method}} {{.path}}"
                | label_format clean_path="{{.path | trimPrefix \"/\"}}"
                | status >= 200
                [5m] @ 1234567890)
            )
        )"#,
    );
}

// ==================== Comment Tests ====================

#[test]
fn test_comments() {
    assert_parses(r###"{job="mysql"} # this is a comment"###);
    assert_parses(
        r###"{job="mysql"}
        | json # parse JSON
        | request_time > 10s # filter slow queries
        "###,
    );
}

// ==================== Edge Cases ====================

#[test]
fn test_nested_parentheses() {
    assert_parses(r#"((rate({job="a"}[5m])))"#);
    assert_parses(r#"(1 + (2 * (3 + 4)))"#);
}

#[test]
fn test_multiline_queries() {
    assert_parses(
        r#"
        sum by (namespace) (
            rate(
                {job="mysql"}
                |= "error"
                | json
                | request_time > 100ms
                [5m]
            )
        )
        "#,
    );
}

#[test]
fn test_string_escaping() {
    assert_parses(r#"{job="my\"job"}"#);
    assert_parses(r#"{job='my\'job'}"#);
    assert_parses(r#"{job=`my\`job`}"#);
}

// ==================== Error Case Tests ====================

#[test]
fn test_invalid_queries_fail() {
    // Invalid selector syntax
    assert_fails("{job}"); // Missing operator and value
    assert_fails("{job=}"); // Missing value
    assert_fails("{=bar}"); // Missing label name

    // Invalid line filter
    assert_fails(r#"{job="app"} | "error""#); // Missing operator

    // Invalid aggregation
    assert_fails(r#"sum({job="app"})"#); // Missing range
    assert_fails(r#"rate({job="app"})"#); // Missing range

    // Invalid binary operation
    assert_fails(r#"1 +"#); // Missing right operand
    // Note: "+ 1" is actually valid (unary plus), so it's not an error case

    // Invalid grouping
    assert_fails(r#"sum by ()"#); // Missing expression
    assert_fails(r#"sum by () "#); // Missing expression
}

#[test]
fn test_incomplete_queries_fail() {
    assert_fails(r#"{job="app"} |"#); // Incomplete pipe
    assert_fails(r#"{job="app"} | json |"#); // Incomplete pipe chain
    assert_fails(r#"rate({job="app"}[5m]"#); // Missing closing bracket
    assert_fails(r#"rate({job="app"}[5m) "#); // Missing closing paren
}

#[test]
fn test_malformed_ranges_fail() {
    assert_fails(r#"{job="app"}[]"#); // Empty range
    assert_fails(r#"{job="app"}[5]"#); // Missing duration unit
    assert_fails(r#"rate({job="app"})"#); // Missing range for rate function
}

// ==================== Parse Tree Validation Tests ====================

#[test]
fn test_parse_tree_json_parser() {
    // Verify that JSON parser is correctly identified in parse tree
    assert_tree_contains(r#"{job="app"} | json"#, &["jsonParser"]);
}

#[test]
fn test_parse_tree_pattern_parser() {
    // Verify that pattern parser is correctly identified in parse tree
    assert_tree_contains(r#"{job="app"} | pattern "<ip>""#, &["patternParser"]);
}

#[test]
fn test_parse_tree_unpack_parser() {
    // Verify that unpack parser is correctly identified in parse tree
    assert_tree_contains(r#"{job="app"} | unpack"#, &["unpackParser"]);
}

#[test]
fn test_parse_tree_label_format() {
    // Verify that label format is correctly identified in parse tree
    assert_tree_contains(r#"{job="app"} | label_format dst=src"#, &["labelFormatExpr"]);
}

#[test]
fn test_parse_tree_line_format() {
    // Verify that line format is correctly identified in parse tree
    assert_tree_contains(r#"{job="app"} | line_format "{{.field}}""#, &["lineFormatExpr"]);
}

#[test]
fn test_parse_tree_selector_matchers() {
    // Verify different matcher types are identified
    assert_tree_contains(r#"{job="mysql"}"#, &["selector"]);
    assert_tree_contains(r#"{job="mysql", level="error"}"#, &["matcher", "selector"]);
}

// ==================== Field Name Ambiguity Tests ====================

#[test]
fn test_field_names_not_keywords() {
    // Test that field names "duration", "bytes", "duration_seconds" work correctly
    // These used to be keywords for conversion functions but are now parsed as ATTRIBUTE

    // Using "duration" as a field name in filters
    assert_parses(r#"{job="app"} | json | duration > 10s"#);
    assert_parses(r#"{job="app"} | logfmt | duration >= 100ms"#);
    assert_parses(r#"{job="app"} | json | duration = 5s"#);

    // Using "bytes" as a field name in filters
    assert_parses(r#"{job="app"} | json | bytes > 1KB"#);
    assert_parses(r#"{job="app"} | logfmt | bytes >= 10MB"#);

    // Using "duration_seconds" as a field name
    assert_parses(r#"{job="app"} | json | duration_seconds > 10"#);

    // Conversion functions still work (parsed as ATTRIBUTE in unwrap context)
    assert_parses(r#"sum_over_time({job="app"} | json | unwrap duration(latency) [5m])"#);
    assert_parses(r#"avg_over_time({job="app"} | json | unwrap bytes(size) [1m])"#);
    assert_parses(r#"max_over_time({job="app"} | json | unwrap duration_seconds(time) [5m])"#);

    // Complex: using both field name and conversion function
    assert_parses(r#"sum_over_time({job="app"} | json | duration > 10s | unwrap bytes(response_size) [5m])"#);
}
