//! Visitor implementations for converting ANTLR parse tree to `LogQL` AST.
//!
//! This module provides visitor structs that implement
//! `LogQLParserVisitorCompat` to transform the ANTLR parse tree into the typed
//! `LogQL` AST representation.

use std::rc::Rc;

use antlr4rust::tree::{ParseTree, ParseTreeVisitorCompat};
use chrono::TimeDelta;

#[allow(clippy::wildcard_imports)]
use super::*;
use super::{super::duration::parse_duration, LogQLParserVisitorCompat};
use crate::{
    error::{QueryError, Result},
    logql::{
        common::{ComparisonOp, Grouping, GroupingLabel, LabelExtraction, LabelFormatOp, MatchOp, parse_error},
        expr::LogQLExpr,
        log::{
            LabelFilterExpr, LabelMatcher, LineFilter, LineFilterOp, LineFilterValue, LogExpr, LogParser,
            PipelineStage, Selector, UnwrapConversion, UnwrapExpr,
        },
        metric::{
            AtModifier, BinaryOp, BinaryOpModifier, MatchingLabels, MetricExpr, RangeAggregation, RangeAggregationOp,
            RangeExpr, VectorAggregation, VectorAggregationOp, VectorMatchCardinality, VectorMatching,
        },
    },
};

// ============================================================================
// Wrapper type for visitor results (implements Default for ANTLR compatibility)
// ============================================================================

/// Wrapper for visitor results that implements Default.
///
/// ANTLR's `ParseTreeVisitorCompat::Return` requires `Default`, but `Result<T>`
/// doesn't implement it. This wrapper provides a default error state.
pub struct VisitorResult<T>(Result<T>);

impl<T> Default for VisitorResult<T> {
    fn default() -> Self {
        Self(Err(parse_error("Not yet visited")))
    }
}

impl<T> VisitorResult<T> {
    pub const fn ok(value: T) -> Self {
        Self(Ok(value))
    }

    pub const fn err(error: QueryError) -> Self {
        Self(Err(error))
    }

    pub fn into_result(self) -> Result<T> {
        self.0
    }
}

// ============================================================================
// Pure Utility Functions
// ============================================================================

/// Convert f64 to u64 with range checking.
#[allow(clippy::cast_precision_loss)] // Precision loss in bounds check is acceptable
fn f64_to_u64_checked(value: f64) -> Result<u64> {
    if value.is_nan() || value.is_infinite() {
        return Err(parse_error("Bytes value is not finite"));
    }
    if value < 0.0 {
        return Err(parse_error("Bytes value cannot be negative"));
    }
    if value > u64::MAX as f64 {
        return Err(parse_error("Bytes value out of range"));
    }
    // SAFETY: Value is non-negative and within u64 range after bounds checks above
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(value.trunc() as u64)
}

/// Remove surrounding quotes from a string literal.
///
/// Note: Escape sequences (e.g., `\n`, `\\`) are NOT processed here.
/// ANTLR's lexer handles escape sequences during tokenization.
fn clean_string(text: &str) -> String {
    let text = text.trim();
    if (text.starts_with('"') && text.ends_with('"'))
        || (text.starts_with('\'') && text.ends_with('\''))
        || (text.starts_with('`') && text.ends_with('`'))
    {
        text[1..text.len() - 1].to_string()
    } else {
        text.to_string()
    }
}

/// Parse a bytes string like `1KB`, `10MiB`, `1e3GB`, `0x10KiB` into bytes.
///
/// Supports decimal, scientific notation (`1e3`), and hexadecimal (`0x10`)
/// number formats.
///
/// Suffixes (case-insensitive):
/// - Binary (IEC, base 1024): `KiB`, `MiB`, `GiB`, `TiB`, `PiB`, `EiB`
/// - Decimal (SI, base 1000): `KB`, `MB`, `GB`, `TB`, `PB`, `EB`
/// - Bytes: `B`
fn parse_bytes(text: &str) -> Result<u64> {
    // Known suffixes ordered by length (longest first to avoid partial matches)
    // Binary units (IEC): base 1024
    // Decimal units (SI): base 1000
    const SUFFIXES: &[(&str, f64)] = &[
        // Binary units (IEC) - base 1024
        ("EIB", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ("PIB", 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ("TIB", 1024.0 * 1024.0 * 1024.0 * 1024.0),
        ("GIB", 1024.0 * 1024.0 * 1024.0),
        ("MIB", 1024.0 * 1024.0),
        ("KIB", 1024.0),
        // Decimal units (SI) - base 1000
        ("EB", 1000.0 * 1000.0 * 1000.0 * 1000.0 * 1000.0 * 1000.0),
        ("PB", 1000.0 * 1000.0 * 1000.0 * 1000.0 * 1000.0),
        ("TB", 1000.0 * 1000.0 * 1000.0 * 1000.0),
        ("GB", 1000.0 * 1000.0 * 1000.0),
        ("MB", 1000.0 * 1000.0),
        ("KB", 1000.0),
        // Bytes
        ("B", 1.0),
    ];

    let text = text.trim();
    if text.is_empty() {
        return Err(parse_error("Empty bytes value"));
    }

    let text_upper = text.to_uppercase();

    // Strip known suffix from the end and parse the numeric prefix
    for &(suffix, multiplier) in SUFFIXES {
        if text_upper.ends_with(suffix) {
            let prefix_len = text.len() - suffix.len();
            let num_str = text[..prefix_len].trim();
            if num_str.is_empty() {
                return Err(parse_error(format!("Missing number in bytes: {text}")));
            }
            let num = parse_number(num_str)?;
            return f64_to_u64_checked(num * multiplier);
        }
    }

    // No recognized suffix - try parsing as raw bytes
    if let Ok(num) = parse_number(text) {
        return f64_to_u64_checked(num);
    }

    // Could not parse - extract trailing alphabetic characters as unknown suffix
    let suffix_start = text.rfind(|c: char| !c.is_ascii_alphabetic()).map_or(0, |i| i + 1);
    if suffix_start < text.len() {
        let unknown_suffix = &text[suffix_start..];
        Err(parse_error(format!("Unknown bytes unit: {unknown_suffix}")))
    } else {
        Err(parse_error(format!("Invalid number in bytes: {text}")))
    }
}

/// Parse a number string into f64.
fn parse_number(text: &str) -> Result<f64> {
    let text = text.trim();

    // Handle hex numbers
    if text.starts_with("0x") || text.starts_with("0X") {
        let hex_str = &text[2..];
        let value = u64::from_str_radix(hex_str, 16).map_err(|_| parse_error(format!("Invalid hex number: {text}")))?;
        // Check if conversion would lose precision (f64 mantissa is 52 bits)
        if value > (1u64 << 52) {
            return Err(parse_error(format!(
                "Hex number too large for precise float representation: {text}"
            )));
        }
        // SAFETY: Value is <= 2^52 after bounds check, so no precision loss
        #[allow(clippy::cast_precision_loss)]
        return Ok(value as f64);
    }

    text.parse().map_err(|_| parse_error(format!("Invalid number: {text}")))
}

// ============================================================================
// Shared Helper Functions (used across log and metric contexts)
// ============================================================================

/// Visit a selector context and return a Selector.
fn visit_selector(ctx: &SelectorContextAll) -> Result<Selector> {
    if let Some(matchers_ctx) = ctx.matchers() {
        let matchers = visit_matchers(&matchers_ctx)?;
        Ok(Selector::new(matchers))
    } else {
        // Empty selector: {}
        Ok(Selector::empty())
    }
}

/// Visit matchers and collect all label matchers.
fn visit_matchers(ctx: &MatchersContextAll) -> Result<Vec<LabelMatcher>> {
    let mut matchers = Vec::new();

    // Get all matcher children
    for matcher_ctx in ctx.matcher_all() {
        let matcher = visit_matcher(&matcher_ctx)?;
        matchers.push(matcher);
    }

    Ok(matchers)
}

/// Visit a single matcher context.
fn visit_matcher(ctx: &MatcherContextAll) -> Result<LabelMatcher> {
    match ctx {
        MatcherContextAll::MatcherEqContext(c) => {
            let label = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
            let value = c.STRING().ok_or_else(|| parse_error("Missing value"))?.get_text();
            Ok(LabelMatcher::new(label, MatchOp::Eq, clean_string(&value)))
        }
        MatcherContextAll::MatcherNeqContext(c) => {
            let label = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
            let value = c.STRING().ok_or_else(|| parse_error("Missing value"))?.get_text();
            Ok(LabelMatcher::new(label, MatchOp::Neq, clean_string(&value)))
        }
        MatcherContextAll::MatcherReContext(c) => {
            let label = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
            let value = c.STRING().ok_or_else(|| parse_error("Missing value"))?.get_text();
            Ok(LabelMatcher::new(label, MatchOp::Re, clean_string(&value)))
        }
        MatcherContextAll::MatcherNreContext(c) => {
            let label = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
            let value = c.STRING().ok_or_else(|| parse_error("Missing value"))?.get_text();
            Ok(LabelMatcher::new(label, MatchOp::Nre, clean_string(&value)))
        }
        MatcherContextAll::Error(_) => Err(parse_error("Error in matcher")),
    }
}

/// Visit label extractions list.
fn visit_label_extractions(ctx: &LabelExtractionsContextAll) -> Result<Vec<LabelExtraction>> {
    let mut extractions = Vec::new();

    for extraction_ctx in ctx.labelExtractionExpr_all() {
        match extraction_ctx.as_ref() {
            LabelExtractionExprContextAll::LabelExtractionSimpleContext(c) => {
                let name = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
                extractions.push(LabelExtraction::new(name));
            }
            LabelExtractionExprContextAll::LabelExtractionWithPathContext(c) => {
                let name = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
                let path = c.STRING().ok_or_else(|| parse_error("Missing path"))?.get_text();
                extractions.push(LabelExtraction::with_path(name, clean_string(&path)));
            }
            LabelExtractionExprContextAll::Error(_) => {
                return Err(parse_error("Error in label extraction"));
            }
        }
    }

    Ok(extractions)
}

/// Visit grouping clause.
fn visit_grouping(ctx: &GroupingContextAll) -> Result<Grouping> {
    match ctx {
        GroupingContextAll::GroupingByContext(c) => {
            let labels = c
                .groupingLabels()
                .map_or_else(Vec::new, |labels_ctx| collect_grouping_labels(&labels_ctx));
            Ok(Grouping::By(labels))
        }
        GroupingContextAll::GroupingWithoutContext(c) => {
            let labels = c
                .groupingLabels()
                .map_or_else(Vec::new, |labels_ctx| collect_grouping_labels(&labels_ctx));
            Ok(Grouping::Without(labels))
        }
        GroupingContextAll::GroupingByEmptyContext(_) => Ok(Grouping::By(Vec::new())),
        GroupingContextAll::GroupingWithoutEmptyContext(_) => Ok(Grouping::Without(Vec::new())),
        GroupingContextAll::Error(_) => Err(parse_error("Error in grouping")),
    }
}

/// Collect grouping labels.
fn collect_grouping_labels(ctx: &GroupingLabelsContextAll) -> Vec<GroupingLabel> {
    let mut labels = Vec::new();

    // groupingLabels: groupingLabel (COMMA groupingLabel)*
    for label_ctx in ctx.groupingLabel_all() {
        let mut label = String::new();
        if let Some(prefix) = label_ctx.PREFIX() {
            label.push_str(&prefix.get_text());
        }
        if let Some(attr) = label_ctx.ATTRIBUTE() {
            label.push_str(&attr.get_text());
        }
        if !label.is_empty() {
            labels.push(GroupingLabel::new(label));
        }
    }

    labels
}

/// Collect line filter values.
fn collect_line_filter_values(filters: &[Rc<LineFilterContextAll>]) -> Result<Vec<LineFilterValue>> {
    let mut values = Vec::new();
    for filter_ctx in filters {
        match filter_ctx.as_ref() {
            LineFilterContextAll::LineFilterStringContext(c) => {
                let s = c.STRING().ok_or_else(|| parse_error("Missing string in line filter"))?;
                values.push(LineFilterValue::String(clean_string(&s.get_text())));
            }
            LineFilterContextAll::LineFilterIpContext(c) => {
                if let Some(ip_fn) = c.ipFn() {
                    let s = ip_fn.STRING().ok_or_else(|| parse_error("Missing IP address in ip()"))?;
                    values.push(LineFilterValue::Ip(clean_string(&s.get_text())));
                }
            }
            LineFilterContextAll::Error(_) => {
                return Err(parse_error("Error in line filter value"));
            }
        }
    }
    Ok(values)
}

/// Collect grouping label strings for binary op modifiers.
fn collect_grouping_label_strings(ctx: &BinOpGroupingLabelsContextAll) -> Result<Vec<String>> {
    let mut labels = Vec::new();

    // binOpGroupingLabels: LPAREN groupingLabelList? RPAREN
    // groupingLabelList: groupingLabelList COMMA groupingLabel | groupingLabel
    // Use recursive traversal of the label list
    if let Some(list_ctx) = ctx.groupingLabelList() {
        collect_labels_from_list(&list_ctx, &mut labels)?;
    }

    Ok(labels)
}

/// Recursively collect labels from a grouping label list.
fn collect_labels_from_list(ctx: &GroupingLabelListContextAll, labels: &mut Vec<String>) -> Result<()> {
    // Handle recursive structure: groupingLabelList COMMA groupingLabel |
    // groupingLabel
    if let Some(nested_list) = ctx.groupingLabelList() {
        collect_labels_from_list(&nested_list, labels)?;
    }

    if let Some(label_ctx) = ctx.groupingLabel() {
        let mut label = String::new();
        if let Some(prefix) = label_ctx.PREFIX() {
            label.push_str(&prefix.get_text());
        }
        if let Some(attr) = label_ctx.ATTRIBUTE() {
            label.push_str(&attr.get_text());
        }
        if !label.is_empty() {
            labels.push(label);
        }
    }

    Ok(())
}

// ============================================================================
// Pure Enum Mappers (context → AST enum)
// ============================================================================

/// Visit comparison operator.
fn visit_comparison_op(ctx: &ComparisonOpContextAll) -> Result<ComparisonOp> {
    if ctx.GT().is_some() {
        Ok(ComparisonOp::Gt)
    } else if ctx.GE().is_some() {
        Ok(ComparisonOp::Ge)
    } else if ctx.LT().is_some() {
        Ok(ComparisonOp::Lt)
    } else if ctx.LE().is_some() {
        Ok(ComparisonOp::Le)
    } else if ctx.EQL().is_some() || ctx.EQ().is_some() {
        Ok(ComparisonOp::Eq)
    } else if ctx.NE().is_some() {
        Ok(ComparisonOp::Neq)
    } else {
        Err(parse_error("Unknown comparison operator"))
    }
}

/// Visit literal expression and extract numeric value.
fn visit_literal_value(ctx: &LiteralExprContextAll) -> Result<f64> {
    match ctx {
        LiteralExprContextAll::LiteralNumberContext(c) => {
            let num = c.NUMBER().ok_or_else(|| parse_error("Missing number"))?;
            parse_number(&num.get_text())
        }
        LiteralExprContextAll::LiteralPositiveNumberContext(c) => {
            let num = c.NUMBER().ok_or_else(|| parse_error("Missing number"))?;
            parse_number(&num.get_text())
        }
        LiteralExprContextAll::LiteralNegativeNumberContext(c) => {
            let num = c.NUMBER().ok_or_else(|| parse_error("Missing number"))?;
            let value = parse_number(&num.get_text())?;
            Ok(-value)
        }
        LiteralExprContextAll::Error(_) => Err(parse_error("Error in literal expression")),
    }
}

/// Visit range log operation.
fn visit_range_log_op(ctx: &RangeLogOpContextAll) -> Result<RangeAggregationOp> {
    match ctx {
        RangeLogOpContextAll::RangeLogOpCountContext(_) => Ok(RangeAggregationOp::CountOverTime),
        RangeLogOpContextAll::RangeLogOpRateContext(_) => Ok(RangeAggregationOp::Rate),
        RangeLogOpContextAll::RangeLogOpBytesContext(_) => Ok(RangeAggregationOp::BytesOverTime),
        RangeLogOpContextAll::RangeLogOpBytesRateContext(_) => Ok(RangeAggregationOp::BytesRate),
        RangeLogOpContextAll::RangeLogOpAbsentContext(_) => Ok(RangeAggregationOp::AbsentOverTime),
        RangeLogOpContextAll::Error(_) => Err(parse_error("Error in range log operation")),
    }
}

/// Visit range unwrap operation without grouping.
fn visit_range_unwrap_op_no_group(ctx: &RangeUnwrapOpNoGroupingContextAll) -> Result<RangeAggregationOp> {
    match ctx {
        RangeUnwrapOpNoGroupingContextAll::RangeUnwrapOpNoGroupSumContext(_) => Ok(RangeAggregationOp::SumOverTime),
        RangeUnwrapOpNoGroupingContextAll::RangeUnwrapOpNoGroupRateContext(_) => Ok(RangeAggregationOp::Rate),
        RangeUnwrapOpNoGroupingContextAll::RangeUnwrapOpNoGroupRateCounterContext(_) => {
            Ok(RangeAggregationOp::RateCounter)
        }
        RangeUnwrapOpNoGroupingContextAll::Error(_) => Err(parse_error("Error in range unwrap operation")),
    }
}

/// Visit range unwrap operation with grouping.
fn visit_range_unwrap_op_with_group(
    ctx: &RangeUnwrapOpWithGroupingContextAll,
) -> Result<(RangeAggregationOp, Option<f64>)> {
    match ctx {
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpAvgContext(_) => Ok((RangeAggregationOp::AvgOverTime, None)),
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpMinContext(_) => Ok((RangeAggregationOp::MinOverTime, None)),
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpMaxContext(_) => Ok((RangeAggregationOp::MaxOverTime, None)),
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpStddevContext(_) => {
            Ok((RangeAggregationOp::StddevOverTime, None))
        }
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpStdvarContext(_) => {
            Ok((RangeAggregationOp::StdvarOverTime, None))
        }
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpQuantileContext(_) => {
            // Note: quantile parameter would need to come from parent context
            Ok((RangeAggregationOp::QuantileOverTime, None))
        }
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpFirstContext(_) => {
            Ok((RangeAggregationOp::FirstOverTime, None))
        }
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpLastContext(_) => {
            Ok((RangeAggregationOp::LastOverTime, None))
        }
        RangeUnwrapOpWithGroupingContextAll::Error(_) => Err(parse_error("Error in range unwrap operation")),
    }
}

/// Visit vector operation.
fn visit_vector_op(ctx: &VectorOpContextAll) -> Result<VectorAggregationOp> {
    let text = ctx.get_text().to_uppercase();
    match text.as_str() {
        "SUM" => Ok(VectorAggregationOp::Sum),
        "AVG" => Ok(VectorAggregationOp::Avg),
        "COUNT" => Ok(VectorAggregationOp::Count),
        "MAX" => Ok(VectorAggregationOp::Max),
        "MIN" => Ok(VectorAggregationOp::Min),
        "STDDEV" => Ok(VectorAggregationOp::Stddev),
        "STDVAR" => Ok(VectorAggregationOp::Stdvar),
        "TOPK" => Ok(VectorAggregationOp::Topk),
        "BOTTOMK" => Ok(VectorAggregationOp::Bottomk),
        "APPROX_TOPK" => Ok(VectorAggregationOp::ApproxTopk),
        "SORT" => Ok(VectorAggregationOp::Sort),
        "SORT_DESC" => Ok(VectorAggregationOp::SortDesc),
        _ => Err(parse_error(format!("Unknown vector operation: {text}"))),
    }
}

/// Visit range context.
fn visit_range(ctx: &RangeContextAll) -> Result<TimeDelta> {
    // range: LBRACK duration RBRACK
    let duration_ctx = ctx.duration().ok_or_else(|| parse_error("Missing duration in range"))?;
    // duration: DURATION
    let duration_token = duration_ctx.DURATION().ok_or_else(|| parse_error("Missing duration token"))?;
    parse_duration(&duration_token.get_text())
}

/// Visit @ modifier.
fn visit_at_modifier(ctx: &AtModifierContextAll) -> Result<AtModifier> {
    if let Some(num) = ctx.NUMBER() {
        let text = num.get_text();
        let timestamp: i64 = text.parse().map_err(|_| parse_error(format!("Invalid timestamp: {text}")))?;

        if ctx.SUB().is_some() {
            Ok(AtModifier::Timestamp(-timestamp))
        } else {
            Ok(AtModifier::Timestamp(timestamp))
        }
    } else {
        Err(parse_error("Invalid @ modifier"))
    }
}

// ============================================================================
// LogQLExprVisitor - Main Visitor
// ============================================================================

/// Main visitor for converting ANTLR parse tree to `LogQLExpr` AST.
pub struct LogQLExprVisitor {
    temp_result: VisitorResult<LogQLExpr>,
}

impl LogQLExprVisitor {
    // ========================================================================
    // Internal Pipeline/Log Methods
    // ========================================================================

    /// Maximum pipeline nesting depth to prevent stack overflow.
    const MAX_PIPELINE_DEPTH: usize = 100;

    /// Create a new visitor.
    pub fn new() -> Self {
        Self {
            temp_result: VisitorResult::default(),
        }
    }

    // ========================================================================
    // Internal Metric Expression Methods
    // ========================================================================

    /// Visit a metric expression context.
    fn visit_metric_expr(&self, ctx: &MetricExprContextAll) -> Result<MetricExpr> {
        match ctx {
            MetricExprContextAll::MetricExprRangeAggContext(c) => self.visit_metric_expr_range_agg(c),
            MetricExprContextAll::MetricExprVectorAggContext(c) => self.visit_metric_expr_vector_agg(c),
            MetricExprContextAll::MetricExprLiteralContext(c) => self.visit_metric_expr_literal(c),
            MetricExprContextAll::MetricExprParensContext(c) => {
                let inner = c.metricExpr().ok_or_else(|| parse_error("Missing inner expression"))?;
                let inner_expr = self.visit_metric_expr(&inner)?;
                Ok(MetricExpr::Parens(Box::new(inner_expr)))
            }
            MetricExprContextAll::MetricExprLabelReplaceContext(c) => self.visit_metric_expr_label_replace(c),
            MetricExprContextAll::MetricExprVectorContext(c) => {
                let vector_ctx = c.vectorExpr().ok_or_else(|| parse_error("Missing vector expression"))?;
                let num = vector_ctx.NUMBER().ok_or_else(|| parse_error("Missing number in vector()"))?;
                let value = parse_number(&num.get_text())?;
                Ok(MetricExpr::Vector(value))
            }
            MetricExprContextAll::MetricExprVariableContext(c) => {
                let var_ctx = c.variableExpr().ok_or_else(|| parse_error("Missing variable expression"))?;
                let name = var_ctx
                    .ATTRIBUTE()
                    .ok_or_else(|| parse_error("Missing variable name"))?
                    .get_text();
                Ok(MetricExpr::Variable(name))
            }
            // Binary operations
            MetricExprContextAll::BinaryOpPowContext(c) => self.visit_binary_op(c, BinaryOp::Pow),
            MetricExprContextAll::BinaryOpMulContext(c) => self.visit_binary_op(c, BinaryOp::Mul),
            MetricExprContextAll::BinaryOpDivContext(c) => self.visit_binary_op(c, BinaryOp::Div),
            MetricExprContextAll::BinaryOpModContext(c) => self.visit_binary_op(c, BinaryOp::Mod),
            MetricExprContextAll::BinaryOpAddContext(c) => self.visit_binary_op(c, BinaryOp::Add),
            MetricExprContextAll::BinaryOpSubContext(c) => self.visit_binary_op(c, BinaryOp::Sub),
            MetricExprContextAll::BinaryOpEqlContext(c) => self.visit_binary_op(c, BinaryOp::Eq),
            MetricExprContextAll::BinaryOpNeqContext(c) => self.visit_binary_op(c, BinaryOp::Neq),
            MetricExprContextAll::BinaryOpGtContext(c) => self.visit_binary_op(c, BinaryOp::Gt),
            MetricExprContextAll::BinaryOpGeContext(c) => self.visit_binary_op(c, BinaryOp::Ge),
            MetricExprContextAll::BinaryOpLtContext(c) => self.visit_binary_op(c, BinaryOp::Lt),
            MetricExprContextAll::BinaryOpLeContext(c) => self.visit_binary_op(c, BinaryOp::Le),
            MetricExprContextAll::BinaryOpAndContext(c) => self.visit_binary_op(c, BinaryOp::And),
            MetricExprContextAll::BinaryOpOrContext(c) => self.visit_binary_op(c, BinaryOp::Or),
            MetricExprContextAll::BinaryOpUnlessContext(c) => self.visit_binary_op(c, BinaryOp::Unless),
            MetricExprContextAll::Error(_) => Err(parse_error("Error in metric expression")),
        }
    }

    /// Visit metric expression with range aggregation.
    fn visit_metric_expr_range_agg(&self, ctx: &MetricExprRangeAggContext) -> Result<MetricExpr> {
        let agg_ctx = ctx
            .rangeAggregationExpr()
            .ok_or_else(|| parse_error("Missing range aggregation"))?;
        let agg = self.visit_range_aggregation(&agg_ctx)?;
        Ok(MetricExpr::RangeAggregation(agg))
    }

    /// Visit metric expression with vector aggregation.
    fn visit_metric_expr_vector_agg(&self, ctx: &MetricExprVectorAggContext) -> Result<MetricExpr> {
        let agg_ctx = ctx
            .vectorAggregationExpr()
            .ok_or_else(|| parse_error("Missing vector aggregation"))?;
        let agg = self.visit_vector_aggregation(&agg_ctx)?;
        Ok(MetricExpr::VectorAggregation(agg))
    }

    /// Visit metric expression literal.
    #[allow(clippy::unused_self)]
    fn visit_metric_expr_literal(&self, ctx: &MetricExprLiteralContext) -> Result<MetricExpr> {
        let literal = ctx.literalExpr().ok_or_else(|| parse_error("Missing literal"))?;
        let value = visit_literal_value(&literal)?;
        Ok(MetricExpr::Literal(value))
    }

    /// Visit metric expression with `label_replace`.
    fn visit_metric_expr_label_replace(&self, ctx: &MetricExprLabelReplaceContext) -> Result<MetricExpr> {
        let label_replace = ctx.labelReplaceExpr().ok_or_else(|| parse_error("Missing label_replace"))?;

        let metric_expr = label_replace
            .metricExpr()
            .ok_or_else(|| parse_error("Missing metric expression in label_replace"))?;
        let expr = self.visit_metric_expr(&metric_expr)?;

        let strings: Vec<_> = label_replace.STRING_all();
        if strings.len() < 4 {
            return Err(parse_error("label_replace requires 4 string arguments"));
        }

        Ok(MetricExpr::LabelReplace {
            expr: Box::new(expr),
            dst_label: clean_string(&strings[0].get_text()),
            replacement: clean_string(&strings[1].get_text()),
            src_label: clean_string(&strings[2].get_text()),
            regex: clean_string(&strings[3].get_text()),
        })
    }

    /// Visit binary operation.
    fn visit_binary_op<'input, T: BinaryOpContextHelper<'input>>(&self, ctx: &T, op: BinaryOp) -> Result<MetricExpr> {
        let exprs = ctx.get_metric_exprs();
        if exprs.len() < 2 {
            return Err(parse_error("Binary operation requires two operands"));
        }

        let left = self.visit_metric_expr(&exprs[0])?;
        let right = self.visit_metric_expr(&exprs[1])?;

        let modifier = if let Some(mod_ctx) = ctx.get_bin_op_modifier() {
            Some(self.visit_bin_op_modifier(&mod_ctx)?)
        } else {
            None
        };

        Ok(MetricExpr::BinaryOp {
            left: Box::new(left),
            op,
            modifier,
            right: Box::new(right),
        })
    }

    /// Visit binary operation modifier.
    fn visit_bin_op_modifier(&self, ctx: &BinOpModifierContextAll) -> Result<BinaryOpModifier> {
        let return_bool = ctx.BOOL().is_some();

        let vector_matching = if let Some(matching_ctx) = ctx.onOrIgnoringModifier() {
            Some(self.visit_vector_matching(&matching_ctx, ctx)?)
        } else {
            None
        };

        Ok(BinaryOpModifier {
            return_bool,
            vector_matching,
        })
    }

    /// Visit vector matching clause.
    #[allow(clippy::unused_self)]
    fn visit_vector_matching(
        &self,
        matching_ctx: &OnOrIgnoringModifierContextAll,
        mod_ctx: &BinOpModifierContextAll,
    ) -> Result<VectorMatching> {
        let matching = if matching_ctx.ON().is_some() {
            let labels = if let Some(labels_ctx) = matching_ctx.binOpGroupingLabels() {
                collect_grouping_label_strings(&labels_ctx)?
            } else {
                Vec::new()
            };
            MatchingLabels::On(labels)
        } else if matching_ctx.IGNORING().is_some() {
            let labels = if let Some(labels_ctx) = matching_ctx.binOpGroupingLabels() {
                collect_grouping_label_strings(&labels_ctx)?
            } else {
                Vec::new()
            };
            MatchingLabels::Ignoring(labels)
        } else {
            return Err(parse_error("Expected ON or IGNORING"));
        };

        let (card, include) = if mod_ctx.GROUP_LEFT().is_some() {
            let include = if let Some(labels_ctx) = mod_ctx.binOpGroupingLabels() {
                collect_grouping_label_strings(&labels_ctx)?
            } else {
                Vec::new()
            };
            (VectorMatchCardinality::ManyToOne, include)
        } else if mod_ctx.GROUP_RIGHT().is_some() {
            let include = if let Some(labels_ctx) = mod_ctx.binOpGroupingLabels() {
                collect_grouping_label_strings(&labels_ctx)?
            } else {
                Vec::new()
            };
            (VectorMatchCardinality::OneToMany, include)
        } else {
            (VectorMatchCardinality::OneToOne, Vec::new())
        };

        Ok(VectorMatching {
            card,
            matching,
            include,
        })
    }

    /// Visit range aggregation expression.
    fn visit_range_aggregation(&self, ctx: &RangeAggregationExprContextAll) -> Result<RangeAggregation> {
        // Get the operation type and optional parameter
        let (op, param) = if let Some(range_log_op) = ctx.rangeLogOp() {
            (visit_range_log_op(&range_log_op)?, None)
        } else if let Some(range_unwrap_no_group) = ctx.rangeUnwrapOpNoGrouping() {
            (visit_range_unwrap_op_no_group(&range_unwrap_no_group)?, None)
        } else if let Some(range_unwrap_with_group) = ctx.rangeUnwrapOpWithGrouping() {
            visit_range_unwrap_op_with_group(&range_unwrap_with_group)?
        } else {
            return Err(parse_error("Unknown range aggregation operation"));
        };

        // Get the range expression
        let range_expr = if let Some(log_range) = ctx.logRangeExpr() {
            self.visit_log_range_expr(&log_range)?
        } else if let Some(unwrapped_range) = ctx.unwrappedRangeExpr() {
            self.visit_unwrapped_range_expr(&unwrapped_range)?
        } else {
            return Err(parse_error("Missing range expression"));
        };

        // Get optional grouping
        let grouping = if let Some(grouping_ctx) = ctx.grouping() {
            Some(visit_grouping(&grouping_ctx)?)
        } else {
            None
        };

        Ok(RangeAggregation {
            op,
            range_expr,
            grouping,
            param,
        })
    }

    /// Visit vector aggregation expression.
    fn visit_vector_aggregation(&self, ctx: &VectorAggregationExprContextAll) -> Result<VectorAggregation> {
        let op_ctx = ctx.vectorOp().ok_or_else(|| parse_error("Missing vector operation"))?;
        let op = visit_vector_op(&op_ctx)?;

        let metric_ctx = ctx
            .metricExpr()
            .ok_or_else(|| parse_error("Missing metric expression in vector aggregation"))?;
        let expr = self.visit_metric_expr(&metric_ctx)?;

        let grouping = if let Some(grouping_ctx) = ctx.grouping() {
            Some(visit_grouping(&grouping_ctx)?)
        } else {
            None
        };

        let param = if let Some(num) = ctx.NUMBER() {
            let value = parse_number(&num.get_text())?;
            if value < 0.0 || value > f64::from(u32::MAX) || value.fract() != 0.0 {
                return Err(parse_error("Parameter must be a non-negative integer"));
            }
            // SAFETY: Value is validated to be in u32 range above
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            Some(value.trunc() as u32)
        } else {
            None
        };

        Ok(VectorAggregation {
            op,
            expr: Box::new(expr),
            grouping,
            param,
        })
    }

    /// Visit log range expression.
    fn visit_log_range_expr(&self, ctx: &LogRangeExprContextAll) -> Result<RangeExpr> {
        let selector_ctx = ctx
            .selector()
            .ok_or_else(|| parse_error("Missing selector in range expression"))?;
        let selector = visit_selector(&selector_ctx)?;

        let range_ctx = ctx.range().ok_or_else(|| parse_error("Missing range"))?;
        let range = visit_range(&range_ctx)?;

        let pipeline = if let Some(pipeline_ctx) = ctx.pipelineExpr() {
            self.collect_pipeline_stages(&pipeline_ctx)?
        } else {
            Vec::new()
        };

        let log_expr = LogExpr::with_pipeline(selector, pipeline);

        let offset = if let Some(offset_ctx) = ctx.offsetExpr() {
            let duration_ctx = offset_ctx.duration().ok_or_else(|| parse_error("Missing duration in offset"))?;
            Some(parse_duration(&duration_ctx.get_text())?)
        } else {
            None
        };

        let at = if let Some(at_ctx) = ctx.atModifier() {
            Some(visit_at_modifier(&at_ctx)?)
        } else {
            None
        };

        Ok(RangeExpr {
            log_expr,
            range,
            offset,
            at,
            unwrap: None,
        })
    }

    /// Visit unwrapped range expression.
    fn visit_unwrapped_range_expr(&self, ctx: &UnwrappedRangeExprContextAll) -> Result<RangeExpr> {
        let selector_ctx = ctx
            .selector()
            .ok_or_else(|| parse_error("Missing selector in unwrapped range"))?;
        let selector = visit_selector(&selector_ctx)?;

        let range_ctx = ctx.range().ok_or_else(|| parse_error("Missing range"))?;
        let range = visit_range(&range_ctx)?;

        let pipeline = if let Some(pipeline_ctx) = ctx.pipelineExpr() {
            self.collect_pipeline_stages(&pipeline_ctx)?
        } else {
            Vec::new()
        };

        let log_expr = LogExpr::with_pipeline(selector, pipeline);

        let unwrap_ctx = ctx.unwrapExpr().ok_or_else(|| parse_error("Missing unwrap expression"))?;
        let unwrap = self.visit_unwrap_expr(&unwrap_ctx)?;

        let offset = if let Some(offset_ctx) = ctx.offsetExpr() {
            let duration_ctx = offset_ctx.duration().ok_or_else(|| parse_error("Missing duration in offset"))?;
            Some(parse_duration(&duration_ctx.get_text())?)
        } else {
            None
        };

        let at = if let Some(at_ctx) = ctx.atModifier() {
            Some(visit_at_modifier(&at_ctx)?)
        } else {
            None
        };

        Ok(RangeExpr {
            log_expr,
            range,
            offset,
            at,
            unwrap: Some(unwrap),
        })
    }

    /// Visit unwrap expression.
    fn visit_unwrap_expr(&self, ctx: &UnwrapExprContextAll) -> Result<UnwrapExpr> {
        match ctx {
            UnwrapExprContextAll::UnwrapBasicContext(c) => {
                let label = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label in unwrap"))?.get_text();
                Ok(UnwrapExpr::new(label))
            }
            UnwrapExprContextAll::UnwrapWithConversionContext(c) => {
                let attrs: Vec<_> = c.ATTRIBUTE_all();
                if attrs.len() < 2 {
                    return Err(parse_error("Unwrap with conversion requires function and label"));
                }
                let conversion_fn = attrs[0].get_text();
                let label = attrs[1].get_text();

                let conversion = match conversion_fn.as_str() {
                    "duration" => UnwrapConversion::Duration,
                    "duration_seconds" => UnwrapConversion::DurationSeconds,
                    "bytes" => UnwrapConversion::Bytes,
                    _ => return Err(parse_error(format!("Unknown unwrap conversion: {conversion_fn}"))),
                };

                Ok(UnwrapExpr::with_conversion(label, conversion))
            }
            UnwrapExprContextAll::UnwrapWithFilterContext(c) => {
                // Get base unwrap from inner unwrapExpr
                let inner = c.unwrapExpr().ok_or_else(|| parse_error("Missing unwrap expression"))?;
                let mut unwrap = self.visit_unwrap_expr(&inner)?;

                // Add filter if present
                if let Some(filter_ctx) = c.labelFilter() {
                    let filter = self.visit_label_filter(&filter_ctx)?;
                    unwrap.post_filter = Some(Box::new(filter));
                }

                Ok(unwrap)
            }
            UnwrapExprContextAll::Error(_) => Err(parse_error("Error in unwrap expression")),
        }
    }

    /// Collect pipeline stages from a recursive pipelineExpr.
    fn collect_pipeline_stages(&self, ctx: &PipelineExprContextAll) -> Result<Vec<PipelineStage>> {
        let mut stages = Vec::new();
        self.collect_pipeline_stages_recursive(ctx, &mut stages, 0)?;
        Ok(stages)
    }

    /// Recursively collect pipeline stages.
    fn collect_pipeline_stages_recursive(
        &self,
        ctx: &PipelineExprContextAll,
        stages: &mut Vec<PipelineStage>,
        depth: usize,
    ) -> Result<()> {
        if depth > Self::MAX_PIPELINE_DEPTH {
            return Err(parse_error("Pipeline exceeds maximum nesting depth"));
        }

        // If there's a nested pipelineExpr, process it first (left recursion)
        if let Some(nested) = ctx.pipelineExpr() {
            self.collect_pipeline_stages_recursive(&nested, stages, depth + 1)?;
        }

        // Then process this stage
        if let Some(stage_ctx) = ctx.pipelineStage() {
            let stage = self.visit_pipeline_stage(&stage_ctx)?;
            stages.push(stage);
        }

        Ok(())
    }

    /// Visit a single pipeline stage.
    fn visit_pipeline_stage(&self, ctx: &PipelineStageContextAll) -> Result<PipelineStage> {
        // Line filters
        if let Some(line_filters) = ctx.lineFilters() {
            return self.visit_line_filters(&line_filters);
        }

        // JSON parser
        if let Some(json_parser) = ctx.jsonParser() {
            return self.visit_json_parser(&json_parser);
        }

        // Logfmt parser
        if let Some(logfmt_parser) = ctx.logfmtParser() {
            return self.visit_logfmt_parser(&logfmt_parser);
        }

        // Regexp parser
        if let Some(regexp_parser) = ctx.regexpParser() {
            let pattern = regexp_parser.STRING().ok_or_else(|| parse_error("Missing regexp pattern"))?;
            return Ok(PipelineStage::LogParser(LogParser::Regexp(clean_string(
                &pattern.get_text(),
            ))));
        }

        // Pattern parser
        if let Some(pattern_parser) = ctx.patternParser() {
            let pattern = pattern_parser.STRING().ok_or_else(|| parse_error("Missing pattern template"))?;
            return Ok(PipelineStage::LogParser(LogParser::Pattern(clean_string(
                &pattern.get_text(),
            ))));
        }

        // Unpack parser
        if ctx.unpackParser().is_some() {
            return Ok(PipelineStage::LogParser(LogParser::Unpack));
        }

        // Label format
        if let Some(label_format) = ctx.labelFormatExpr() {
            return self.visit_label_format_expr(&label_format);
        }

        // Line format
        if let Some(line_format) = ctx.lineFormatExpr() {
            let template = line_format
                .STRING()
                .ok_or_else(|| parse_error("Missing line format template"))?;
            return Ok(PipelineStage::LineFormat(clean_string(&template.get_text())));
        }

        // Decolorize
        if ctx.decolorizeExpr().is_some() {
            return Ok(PipelineStage::Decolorize);
        }

        // Drop
        if let Some(drop_expr) = ctx.dropExpr() {
            if let Some(extractions) = drop_expr.labelExtractions() {
                let fields = visit_label_extractions(&extractions)?;
                return Ok(PipelineStage::Drop(fields));
            }
            return Ok(PipelineStage::Drop(Vec::new()));
        }

        // Keep
        if let Some(keep_expr) = ctx.keepExpr() {
            if let Some(extractions) = keep_expr.labelExtractions() {
                let fields = visit_label_extractions(&extractions)?;
                return Ok(PipelineStage::Keep(fields));
            }
            return Ok(PipelineStage::Keep(Vec::new()));
        }

        // Label filter
        if let Some(label_filter) = ctx.labelFilter() {
            let filter = self.visit_label_filter(&label_filter)?;
            return Ok(PipelineStage::LabelFilter(filter));
        }

        Err(parse_error("Unknown pipeline stage"))
    }

    /// Visit line filters.
    #[allow(clippy::unused_self)]
    fn visit_line_filters(&self, ctx: &LineFiltersContextAll) -> Result<PipelineStage> {
        let (op, filters) = match ctx {
            LineFiltersContextAll::LineFiltersContainsContext(c) => {
                let filters = collect_line_filter_values(&c.lineFilter_all())?;
                (LineFilterOp::Contains, filters)
            }
            LineFiltersContextAll::LineFiltersNotContainsContext(c) => {
                let filters = collect_line_filter_values(&c.lineFilter_all())?;
                (LineFilterOp::NotContains, filters)
            }
            LineFiltersContextAll::LineFiltersMatchContext(c) => {
                let filters = collect_line_filter_values(&c.lineFilter_all())?;
                (LineFilterOp::Match, filters)
            }
            LineFiltersContextAll::LineFiltersNotMatchContext(c) => {
                let filters = collect_line_filter_values(&c.lineFilter_all())?;
                (LineFilterOp::NotMatch, filters)
            }
            LineFiltersContextAll::LineFiltersPatternContext(_)
            | LineFiltersContextAll::LineFiltersNotPatternContext(_) => {
                return Err(QueryError::NotImplemented(
                    "Pattern matching doesn't supported yet.".to_string(),
                ));
            }
            LineFiltersContextAll::Error(_) => {
                return Err(parse_error("Error in line filter"));
            }
        };

        Ok(PipelineStage::LineFilter(LineFilter::new(op, filters)))
    }

    /// Visit JSON parser.
    #[allow(clippy::unused_self)]
    fn visit_json_parser(&self, ctx: &JsonParserContext) -> Result<PipelineStage> {
        if let Some(extractions) = ctx.labelExtractions() {
            let fields = visit_label_extractions(&extractions)?;
            Ok(PipelineStage::LogParser(LogParser::Json(Some(fields))))
        } else {
            Ok(PipelineStage::LogParser(LogParser::Json(None)))
        }
    }

    /// Visit logfmt parser.
    #[allow(clippy::unused_self)]
    fn visit_logfmt_parser(&self, ctx: &LogfmtParserContext) -> Result<PipelineStage> {
        // Check for flags using LOGFMT_FLAG_all()
        let flags: Vec<_> = ctx.LOGFMT_FLAG_all();
        let strict = flags.iter().any(|f| f.get_text() == "--strict");
        let keep_empty = flags.iter().any(|f| f.get_text() == "--keep-empty");

        let fields = if let Some(extractions) = ctx.labelExtractions() {
            Some(visit_label_extractions(&extractions)?)
        } else {
            None
        };

        Ok(PipelineStage::LogParser(LogParser::Logfmt {
            strict,
            keep_empty,
            fields,
        }))
    }

    /// Visit label format expression.
    #[allow(clippy::unused_self)]
    fn visit_label_format_expr(&self, ctx: &LabelFormatExprContext) -> Result<PipelineStage> {
        let ops_ctx = ctx
            .labelFormatOps()
            .ok_or_else(|| parse_error("Missing label format operations"))?;

        let mut ops = Vec::new();
        for op_ctx in ops_ctx.labelFormatOp_all() {
            match op_ctx.as_ref() {
                LabelFormatOpContextAll::LabelFormatRenameContext(c) => {
                    let attrs: Vec<_> = c.ATTRIBUTE_all().iter().map(|a| a.get_text()).collect();
                    if attrs.len() >= 2 {
                        ops.push(LabelFormatOp::Rename {
                            dst: attrs[0].clone(),
                            src: attrs[1].clone(),
                        });
                    }
                }
                LabelFormatOpContextAll::LabelFormatTemplateContext(c) => {
                    let dst = c
                        .ATTRIBUTE()
                        .ok_or_else(|| parse_error("Missing destination label"))?
                        .get_text();
                    let template = c.STRING().ok_or_else(|| parse_error("Missing template"))?.get_text();
                    ops.push(LabelFormatOp::Template {
                        dst,
                        template: clean_string(&template),
                    });
                }
                LabelFormatOpContextAll::Error(_) => {
                    return Err(parse_error("Error in label format operation"));
                }
            }
        }

        Ok(PipelineStage::LabelFormat(ops))
    }

    /// Visit label filter expression.
    fn visit_label_filter(&self, ctx: &LabelFilterContextAll) -> Result<LabelFilterExpr> {
        match ctx {
            LabelFilterContextAll::LabelFilterAndContext(c) => {
                let filters = c.labelFilter_all();
                if filters.len() < 2 {
                    return Err(parse_error("AND filter requires two operands"));
                }
                let left = self.visit_label_filter(&filters[0])?;
                let right = self.visit_label_filter(&filters[1])?;
                Ok(LabelFilterExpr::And(Box::new(left), Box::new(right)))
            }
            LabelFilterContextAll::LabelFilterOrContext(c) => {
                let filters = c.labelFilter_all();
                if filters.len() < 2 {
                    return Err(parse_error("OR filter requires two operands"));
                }
                let left = self.visit_label_filter(&filters[0])?;
                let right = self.visit_label_filter(&filters[1])?;
                Ok(LabelFilterExpr::Or(Box::new(left), Box::new(right)))
            }
            LabelFilterContextAll::LabelFilterParensContext(c) => {
                let inner = c.labelFilter().ok_or_else(|| parse_error("Missing inner filter"))?;
                let inner_expr = self.visit_label_filter(&inner)?;
                Ok(LabelFilterExpr::Parens(Box::new(inner_expr)))
            }
            LabelFilterContextAll::LabelFilterMatcherContext(c) => {
                let matcher_ctx = c.matcher().ok_or_else(|| parse_error("Missing matcher in label filter"))?;
                let matcher = visit_matcher(&matcher_ctx)?;
                Ok(LabelFilterExpr::Matcher(matcher))
            }
            LabelFilterContextAll::LabelFilterNumberContext(c) => {
                let filter = c.numberFilter().ok_or_else(|| parse_error("Missing number filter"))?;
                self.visit_number_filter(&filter)
            }
            LabelFilterContextAll::LabelFilterDurationContext(c) => {
                let filter = c.durationFilter().ok_or_else(|| parse_error("Missing duration filter"))?;
                self.visit_duration_filter(&filter)
            }
            LabelFilterContextAll::LabelFilterBytesContext(c) => {
                let filter = c.bytesFilter().ok_or_else(|| parse_error("Missing bytes filter"))?;
                self.visit_bytes_filter(&filter)
            }
            LabelFilterContextAll::LabelFilterIpContext(c) => {
                let filter = c.ipLabelFilter().ok_or_else(|| parse_error("Missing IP filter"))?;
                self.visit_ip_label_filter(&filter)
            }
            LabelFilterContextAll::Error(_) => Err(parse_error("Error in label filter")),
        }
    }

    /// Visit number filter.
    #[allow(clippy::unused_self)]
    fn visit_number_filter(&self, ctx: &NumberFilterContextAll) -> Result<LabelFilterExpr> {
        let label = ctx
            .ATTRIBUTE()
            .ok_or_else(|| parse_error("Missing label in number filter"))?
            .get_text();

        let op = ctx.comparisonOp().ok_or_else(|| parse_error("Missing comparison operator"))?;
        let comparison_op = visit_comparison_op(&op)?;

        let literal = ctx.literalExpr().ok_or_else(|| parse_error("Missing literal expression"))?;
        let value = visit_literal_value(&literal)?;

        Ok(LabelFilterExpr::Number {
            label,
            op: comparison_op,
            value,
        })
    }

    /// Visit duration filter.
    #[allow(clippy::unused_self)]
    fn visit_duration_filter(&self, ctx: &DurationFilterContextAll) -> Result<LabelFilterExpr> {
        let label = ctx
            .ATTRIBUTE()
            .ok_or_else(|| parse_error("Missing label in duration filter"))?
            .get_text();

        let op = ctx.comparisonOp().ok_or_else(|| parse_error("Missing comparison operator"))?;
        let comparison_op = visit_comparison_op(&op)?;

        let duration_ctx = ctx.duration().ok_or_else(|| parse_error("Missing duration value"))?;
        let duration = parse_duration(&duration_ctx.get_text())?;

        Ok(LabelFilterExpr::Duration {
            label,
            op: comparison_op,
            value: duration,
        })
    }

    /// Visit bytes filter.
    #[allow(clippy::unused_self)]
    fn visit_bytes_filter(&self, ctx: &BytesFilterContextAll) -> Result<LabelFilterExpr> {
        let label = ctx
            .ATTRIBUTE()
            .ok_or_else(|| parse_error("Missing label in bytes filter"))?
            .get_text();

        let op = ctx.comparisonOp().ok_or_else(|| parse_error("Missing comparison operator"))?;
        let comparison_op = visit_comparison_op(&op)?;

        let bytes = ctx.BYTES().ok_or_else(|| parse_error("Missing bytes value"))?.get_text();
        let value = parse_bytes(&bytes)?;

        Ok(LabelFilterExpr::Bytes {
            label,
            op: comparison_op,
            value,
        })
    }

    /// Visit IP label filter.
    #[allow(clippy::unused_self)]
    fn visit_ip_label_filter(&self, ctx: &IpLabelFilterContextAll) -> Result<LabelFilterExpr> {
        let label = ctx
            .ATTRIBUTE()
            .ok_or_else(|| parse_error("Missing label in IP filter"))?
            .get_text();

        let negated = ctx.NE().is_some();

        let ip_fn = ctx.ipFn().ok_or_else(|| parse_error("Missing ip() function"))?;
        let cidr = ip_fn.STRING().ok_or_else(|| parse_error("Missing IP address"))?.get_text();

        Ok(LabelFilterExpr::Ip {
            label,
            negated,
            cidr: clean_string(&cidr),
        })
    }
}

impl Default for LogQLExprVisitor {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Helper trait for binary operation contexts
// ============================================================================

/// Helper trait for binary operation contexts.
trait BinaryOpContextHelper<'input> {
    fn get_metric_exprs(&self) -> Vec<Rc<MetricExprContextAll<'input>>>;
    fn get_bin_op_modifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>>;
}

macro_rules! impl_binary_op_helper {
    ($($ctx:ty),*) => {
        $(
            impl<'input> BinaryOpContextHelper<'input> for $ctx {
                fn get_metric_exprs(&self) -> Vec<Rc<MetricExprContextAll<'input>>> {
                    self.metricExpr_all()
                }
                fn get_bin_op_modifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> {
                    self.binOpModifier()
                }
            }
        )*
    };
}

impl_binary_op_helper!(
    BinaryOpPowContext<'input>,
    BinaryOpMulContext<'input>,
    BinaryOpDivContext<'input>,
    BinaryOpModContext<'input>,
    BinaryOpAddContext<'input>,
    BinaryOpSubContext<'input>,
    BinaryOpEqlContext<'input>,
    BinaryOpNeqContext<'input>,
    BinaryOpGtContext<'input>,
    BinaryOpGeContext<'input>,
    BinaryOpLtContext<'input>,
    BinaryOpLeContext<'input>,
    BinaryOpAndContext<'input>,
    BinaryOpOrContext<'input>,
    BinaryOpUnlessContext<'input>
);

// ============================================================================
// ANTLR Visitor Trait Implementation
// ============================================================================

impl ParseTreeVisitorCompat<'_> for LogQLExprVisitor {
    type Node = LogQLParserContextType;
    type Return = VisitorResult<LogQLExpr>;

    fn temp_result(&mut self) -> &mut Self::Return {
        &mut self.temp_result
    }
}

impl<'input> LogQLParserVisitorCompat<'input> for LogQLExprVisitor {
    fn visit_root(&mut self, ctx: &RootContext<'input>) -> Self::Return {
        let Some(expr_ctx) = ctx.expr() else {
            return VisitorResult::err(parse_error("Empty query"));
        };
        self.visit(&*expr_ctx)
    }

    fn visit_logExprWithSelectorOnly(&mut self, ctx: &LogExprWithSelectorOnlyContext<'input>) -> Self::Return {
        let Some(selector_ctx) = ctx.selector() else {
            return VisitorResult::err(parse_error("Missing selector"));
        };

        match visit_selector(&selector_ctx) {
            Ok(selector) => VisitorResult::ok(LogQLExpr::Log(LogExpr::new(selector))),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_logExprWithPipeline(&mut self, ctx: &LogExprWithPipelineContext<'input>) -> Self::Return {
        let Some(selector_ctx) = ctx.selector() else {
            return VisitorResult::err(parse_error("Missing selector"));
        };

        let selector = match visit_selector(&selector_ctx) {
            Ok(s) => s,
            Err(e) => return VisitorResult::err(e),
        };

        let Some(pipeline_ctx) = ctx.pipelineExpr() else {
            return VisitorResult::err(parse_error("Missing pipeline"));
        };

        match self.collect_pipeline_stages(&pipeline_ctx) {
            Ok(pipeline) => VisitorResult::ok(LogQLExpr::Log(LogExpr::with_pipeline(selector, pipeline))),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_metricExprRangeAgg(&mut self, ctx: &MetricExprRangeAggContext<'input>) -> Self::Return {
        match self.visit_metric_expr_range_agg(ctx) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_metricExprVectorAgg(&mut self, ctx: &MetricExprVectorAggContext<'input>) -> Self::Return {
        match self.visit_metric_expr_vector_agg(ctx) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_metricExprLiteral(&mut self, ctx: &MetricExprLiteralContext<'input>) -> Self::Return {
        match self.visit_metric_expr_literal(ctx) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_metricExprParens(&mut self, ctx: &MetricExprParensContext<'input>) -> Self::Return {
        let Some(inner) = ctx.metricExpr() else {
            return VisitorResult::err(parse_error("Missing inner expression"));
        };
        match self.visit_metric_expr(&inner) {
            Ok(inner_expr) => VisitorResult::ok(LogQLExpr::Metric(MetricExpr::Parens(Box::new(inner_expr)))),
            Err(e) => VisitorResult::err(e),
        }
    }

    // Binary operations
    fn visit_binaryOpPow(&mut self, ctx: &BinaryOpPowContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Pow) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpMul(&mut self, ctx: &BinaryOpMulContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Mul) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpDiv(&mut self, ctx: &BinaryOpDivContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Div) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpMod(&mut self, ctx: &BinaryOpModContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Mod) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpAdd(&mut self, ctx: &BinaryOpAddContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Add) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpSub(&mut self, ctx: &BinaryOpSubContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Sub) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpEql(&mut self, ctx: &BinaryOpEqlContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Eq) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpNeq(&mut self, ctx: &BinaryOpNeqContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Neq) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpGt(&mut self, ctx: &BinaryOpGtContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Gt) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpGe(&mut self, ctx: &BinaryOpGeContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Ge) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpLt(&mut self, ctx: &BinaryOpLtContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Lt) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpLe(&mut self, ctx: &BinaryOpLeContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Le) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpAnd(&mut self, ctx: &BinaryOpAndContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::And) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpOr(&mut self, ctx: &BinaryOpOrContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Or) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpUnless(&mut self, ctx: &BinaryOpUnlessContext<'input>) -> Self::Return {
        match self.visit_binary_op(ctx, BinaryOp::Unless) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }
}

#[cfg(test)]
mod parse_bytes_tests {
    use super::*;

    // ==================== Binary Units (IEC, base 1024) ====================

    #[test]
    fn test_binary_kib() {
        assert_eq!(parse_bytes("1KiB").unwrap(), 1024);
        assert_eq!(parse_bytes("2KiB").unwrap(), 2048);
    }

    #[test]
    fn test_binary_mib() {
        assert_eq!(parse_bytes("1MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes("2MiB").unwrap(), 2 * 1024 * 1024);
    }

    #[test]
    fn test_binary_gib() {
        assert_eq!(parse_bytes("1GiB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn test_binary_tib() {
        assert_eq!(parse_bytes("1TiB").unwrap(), 1024_u64.pow(4));
    }

    #[test]
    fn test_binary_pib() {
        assert_eq!(parse_bytes("1PiB").unwrap(), 1024_u64.pow(5));
    }

    #[test]
    fn test_binary_eib() {
        assert_eq!(parse_bytes("1EiB").unwrap(), 1024_u64.pow(6));
    }

    // ==================== Decimal Units (SI, base 1000) ====================

    #[test]
    fn test_decimal_kb() {
        assert_eq!(parse_bytes("1KB").unwrap(), 1000);
        assert_eq!(parse_bytes("2KB").unwrap(), 2000);
    }

    #[test]
    fn test_decimal_mb() {
        assert_eq!(parse_bytes("1MB").unwrap(), 1_000_000);
        assert_eq!(parse_bytes("2MB").unwrap(), 2_000_000);
    }

    #[test]
    fn test_decimal_gb() {
        assert_eq!(parse_bytes("1GB").unwrap(), 1_000_000_000);
    }

    #[test]
    fn test_decimal_tb() {
        assert_eq!(parse_bytes("1TB").unwrap(), 1_000_000_000_000);
    }

    #[test]
    fn test_decimal_pb() {
        assert_eq!(parse_bytes("1PB").unwrap(), 1_000_000_000_000_000);
    }

    #[test]
    fn test_decimal_eb() {
        assert_eq!(parse_bytes("1EB").unwrap(), 1_000_000_000_000_000_000);
    }

    // ==================== Binary vs Decimal Differentiation ====================

    #[test]
    fn test_kib_vs_kb() {
        // 1 KiB = 1024 bytes, 1 KB = 1000 bytes
        assert_eq!(parse_bytes("1KiB").unwrap(), 1024);
        assert_eq!(parse_bytes("1KB").unwrap(), 1000);
        assert_ne!(parse_bytes("1KiB").unwrap(), parse_bytes("1KB").unwrap());
    }

    #[test]
    fn test_mib_vs_mb() {
        // 1 MiB = 1048576, 1 MB = 1000000
        assert_eq!(parse_bytes("1MiB").unwrap(), 1_048_576);
        assert_eq!(parse_bytes("1MB").unwrap(), 1_000_000);
    }

    #[test]
    fn test_gib_vs_gb() {
        // 1 GiB = 1073741824, 1 GB = 1000000000
        assert_eq!(parse_bytes("1GiB").unwrap(), 1_073_741_824);
        assert_eq!(parse_bytes("1GB").unwrap(), 1_000_000_000);
    }

    // ==================== Scientific Notation ====================

    #[test]
    fn test_scientific_notation_decimal_units() {
        // 1e3 KB = 1000 * 1000 = 1,000,000
        assert_eq!(parse_bytes("1e3KB").unwrap(), 1_000_000);
        // 1E3 MB = 1000 * 1,000,000 = 1,000,000,000
        assert_eq!(parse_bytes("1E3MB").unwrap(), 1_000_000_000);
    }

    #[test]
    fn test_scientific_notation_binary_units() {
        // 1e3 KiB = 1000 * 1024 = 1,024,000
        assert_eq!(parse_bytes("1e3KiB").unwrap(), 1_024_000);
        // 2e2 MiB = 200 * 1048576 = 209,715,200
        assert_eq!(parse_bytes("2e2MiB").unwrap(), 209_715_200);
    }

    #[test]
    fn test_scientific_notation_raw_bytes() {
        assert_eq!(parse_bytes("1e3").unwrap(), 1000);
        assert_eq!(parse_bytes("1e6").unwrap(), 1_000_000);
    }

    // ==================== Hexadecimal Numbers ====================

    #[test]
    fn test_hex_decimal_units() {
        // 0x10 = 16, 16 KB = 16,000
        assert_eq!(parse_bytes("0x10KB").unwrap(), 16_000);
        // 0xFF = 255, 255 MB = 255,000,000
        assert_eq!(parse_bytes("0xFFMB").unwrap(), 255_000_000);
    }

    #[test]
    fn test_hex_binary_units() {
        // 0x10 = 16, 16 KiB = 16,384
        assert_eq!(parse_bytes("0x10KiB").unwrap(), 16_384);
        // 0xFF = 255, 255 MiB = 267,386,880
        assert_eq!(parse_bytes("0xFFMiB").unwrap(), 267_386_880);
    }

    #[test]
    fn test_hex_raw_bytes() {
        assert_eq!(parse_bytes("0x10").unwrap(), 16);
        assert_eq!(parse_bytes("0xFF").unwrap(), 255);
        assert_eq!(parse_bytes("0x100").unwrap(), 256);
    }

    #[test]
    fn test_hex_uppercase_prefix() {
        assert_eq!(parse_bytes("0X10KB").unwrap(), 16_000);
        assert_eq!(parse_bytes("0X10KiB").unwrap(), 16_384);
    }

    // ==================== Case Insensitivity ====================

    #[test]
    fn test_suffix_case_insensitivity() {
        // All variations should work
        assert_eq!(parse_bytes("1kb").unwrap(), 1000);
        assert_eq!(parse_bytes("1Kb").unwrap(), 1000);
        assert_eq!(parse_bytes("1KB").unwrap(), 1000);
        assert_eq!(parse_bytes("1kB").unwrap(), 1000);
    }

    #[test]
    fn test_binary_suffix_case_insensitivity() {
        assert_eq!(parse_bytes("1kib").unwrap(), 1024);
        assert_eq!(parse_bytes("1Kib").unwrap(), 1024);
        assert_eq!(parse_bytes("1KIB").unwrap(), 1024);
        assert_eq!(parse_bytes("1KiB").unwrap(), 1024);
    }

    #[test]
    fn test_bytes_suffix_case_insensitivity() {
        assert_eq!(parse_bytes("100b").unwrap(), 100);
        assert_eq!(parse_bytes("100B").unwrap(), 100);
    }

    // ==================== Raw Bytes (No Suffix) ====================

    #[test]
    fn test_raw_bytes_integer() {
        assert_eq!(parse_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_bytes("100").unwrap(), 100);
    }

    #[test]
    fn test_raw_bytes_decimal() {
        assert_eq!(parse_bytes("1.5").unwrap(), 1); // truncated
        assert_eq!(parse_bytes("2.9").unwrap(), 2); // truncated
    }

    // ==================== Decimal Numbers with Units ====================

    #[test]
    fn test_decimal_number_with_unit() {
        // 1.5 KB = 1500
        assert_eq!(parse_bytes("1.5KB").unwrap(), 1500);
        // 1.5 KiB = 1536
        assert_eq!(parse_bytes("1.5KiB").unwrap(), 1536);
        // 2.5 MB = 2,500,000
        assert_eq!(parse_bytes("2.5MB").unwrap(), 2_500_000);
    }

    // ==================== Whitespace Handling ====================

    #[test]
    fn test_whitespace_trimming() {
        assert_eq!(parse_bytes("  1KB  ").unwrap(), 1000);
        assert_eq!(parse_bytes("\t1MiB\n").unwrap(), 1_048_576);
    }

    // ==================== Error Cases ====================

    #[test]
    fn test_empty_input() {
        assert!(parse_bytes("").is_err());
        assert!(parse_bytes("   ").is_err());
    }

    #[test]
    fn test_missing_number() {
        assert!(parse_bytes("KB").is_err());
        assert!(parse_bytes("MiB").is_err());
    }

    #[test]
    fn test_unknown_suffix() {
        // Use "ZZ" which doesn't match any valid suffix (not ending with B)
        let err = parse_bytes("100ZZ").unwrap_err();
        let err_str = err.to_string();
        assert!(
            err_str.contains("Unknown bytes unit"),
            "Expected 'Unknown bytes unit' but got: {err_str}"
        );
    }

    #[test]
    fn test_invalid_number() {
        let err = parse_bytes("abcKB").unwrap_err();
        assert!(err.to_string().contains("Invalid"));
    }
}
