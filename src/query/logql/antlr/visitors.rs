//! Visitor implementations for converting ANTLR parse tree to `LogQL` AST.
//!
//! This module provides visitor structs that implement
//! `LogQLParserVisitorCompat` to transform the ANTLR parse tree into the typed
//! `LogQL` AST representation.

use std::rc::Rc;

use antlr4rust::tree::{ParseTree, ParseTreeVisitorCompat};

use super::LogQLParserVisitorCompat;
#[allow(clippy::wildcard_imports)]
use super::*;
use crate::{
    common::{
        errors::{IceGateError, ParseError},
        Result,
    },
    query::logql::{
        common::{ComparisonOp, Duration, Grouping, GroupingLabel, LabelExtraction, LabelFormatOp, MatchOp},
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

    pub const fn err(error: IceGateError) -> Self {
        Self(Err(error))
    }

    pub fn into_result(self) -> Result<T> {
        self.0
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Convert f64 to i64 with overflow checking.
#[allow(clippy::cast_precision_loss)] // Precision loss in bounds check is acceptable
fn f64_to_i64_checked(value: f64) -> Result<i64> {
    if value.is_nan() || value.is_infinite() {
        return Err(parse_error("Duration value is not finite"));
    }
    if value > i64::MAX as f64 || value < i64::MIN as f64 {
        return Err(parse_error("Duration value out of range"));
    }
    // SAFETY: Value is within i64 range after bounds check above
    #[allow(clippy::cast_possible_truncation)]
    Ok(value.trunc() as i64)
}

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

/// Parse a duration string like "5m", "1h", "30s" into Duration.
fn parse_duration(text: &str) -> Result<Duration> {
    let text = text.trim();
    let is_negative = text.starts_with('-');
    let text = text.trim_start_matches('-').trim_start_matches('+');

    // Parse numeric part and unit
    let mut num_end = 0;
    for (i, c) in text.char_indices() {
        if c.is_ascii_digit() || c == '.' {
            num_end = i + c.len_utf8();
        } else {
            break;
        }
    }

    if num_end == 0 {
        return Err(parse_error(format!("Invalid duration: {text}")));
    }

    let num_str = &text[..num_end];
    let unit = &text[num_end..];

    let num: f64 = num_str
        .parse()
        .map_err(|_| parse_error(format!("Invalid number in duration: {num_str}")))?;

    let nanos = match unit {
        "ns" => f64_to_i64_checked(num)?,
        "us" | "µs" => f64_to_i64_checked(num * 1_000.0)?,
        "ms" => f64_to_i64_checked(num * 1_000_000.0)?,
        "s" => f64_to_i64_checked(num * 1_000_000_000.0)?,
        "m" => f64_to_i64_checked(num * 60.0 * 1_000_000_000.0)?,
        "h" => f64_to_i64_checked(num * 3600.0 * 1_000_000_000.0)?,
        "d" => f64_to_i64_checked(num * 86400.0 * 1_000_000_000.0)?,
        "w" => f64_to_i64_checked(num * 7.0 * 86400.0 * 1_000_000_000.0)?,
        "y" => f64_to_i64_checked(num * 365.0 * 86400.0 * 1_000_000_000.0)?,
        _ => return Err(parse_error(format!("Unknown duration unit: {unit}"))),
    };

    let nanos = if is_negative { -nanos } else { nanos };
    Ok(Duration::from_nanos(nanos))
}

/// Parse a bytes string like "1KB", "10MB" into bytes.
fn parse_bytes(text: &str) -> Result<u64> {
    let text = text.trim();

    // Find where the number ends and unit begins
    let mut num_end = 0;
    for (i, c) in text.char_indices() {
        if c.is_ascii_digit() || c == '.' {
            num_end = i + c.len_utf8();
        } else {
            break;
        }
    }

    let num_str = &text[..num_end];
    let unit = text[num_end..].trim().to_uppercase();

    let num: f64 = num_str
        .parse()
        .map_err(|_| parse_error(format!("Invalid number in bytes: {num_str}")))?;

    let multiplier: f64 = match unit.as_str() {
        "" | "B" => 1.0,
        "KB" | "KIB" => 1024.0,
        "MB" | "MIB" => 1024.0 * 1024.0,
        "GB" | "GIB" => 1024.0 * 1024.0 * 1024.0,
        "TB" | "TIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        "PB" | "PIB" => 1024.0 * 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return Err(parse_error(format!("Unknown bytes unit: {unit}"))),
    };

    f64_to_u64_checked(num * multiplier)
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

/// Create a parse error with location info.
fn parse_error(message: impl Into<String>) -> IceGateError {
    IceGateError::Parse(vec![ParseError {
        line: 0,
        column: 0,
        message: message.into(),
        antlr_error: None,
    }])
}

// ============================================================================
// LogQLExprVisitor - Top-level visitor
// ============================================================================

/// Main visitor for converting ANTLR parse tree to `LogQLExpr` AST.
pub struct LogQLExprVisitor {
    temp_result: VisitorResult<LogQLExpr>,
}

impl LogQLExprVisitor {
    pub fn new() -> Self {
        Self {
            temp_result: VisitorResult::default(),
        }
    }
}

impl Default for LogQLExprVisitor {
    fn default() -> Self {
        Self::new()
    }
}

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

        let selector = match visit_selector(&selector_ctx) {
            Ok(s) => s,
            Err(e) => return VisitorResult::err(e),
        };
        VisitorResult::ok(LogQLExpr::Log(LogExpr::new(selector)))
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

        let pipeline = match collect_pipeline_stages(&pipeline_ctx) {
            Ok(p) => p,
            Err(e) => return VisitorResult::err(e),
        };
        VisitorResult::ok(LogQLExpr::Log(LogExpr::with_pipeline(selector, pipeline)))
    }

    // Metric expression visitors - delegate to visit_metric_expr helper
    fn visit_metricExprRangeAgg(&mut self, ctx: &MetricExprRangeAggContext<'input>) -> Self::Return {
        match visit_metric_expr_range_agg(ctx) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_metricExprVectorAgg(&mut self, ctx: &MetricExprVectorAggContext<'input>) -> Self::Return {
        match visit_metric_expr_vector_agg(ctx) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_metricExprLiteral(&mut self, ctx: &MetricExprLiteralContext<'input>) -> Self::Return {
        match visit_metric_expr_literal(ctx) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_metricExprParens(&mut self, ctx: &MetricExprParensContext<'input>) -> Self::Return {
        let Some(inner) = ctx.metricExpr() else {
            return VisitorResult::err(parse_error("Missing inner expression"));
        };
        let inner_expr = match visit_metric_expr(&inner) {
            Ok(e) => e,
            Err(e) => return VisitorResult::err(e),
        };
        VisitorResult::ok(LogQLExpr::Metric(MetricExpr::Parens(Box::new(inner_expr))))
    }

    // Binary operations - these need to return LogQLExpr::Metric
    fn visit_binaryOpPow(&mut self, ctx: &BinaryOpPowContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Pow) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpMul(&mut self, ctx: &BinaryOpMulContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Mul) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpDiv(&mut self, ctx: &BinaryOpDivContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Div) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpMod(&mut self, ctx: &BinaryOpModContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Mod) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpAdd(&mut self, ctx: &BinaryOpAddContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Add) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpSub(&mut self, ctx: &BinaryOpSubContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Sub) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpEql(&mut self, ctx: &BinaryOpEqlContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Eq) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpNeq(&mut self, ctx: &BinaryOpNeqContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Neq) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpGt(&mut self, ctx: &BinaryOpGtContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Gt) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpGe(&mut self, ctx: &BinaryOpGeContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Ge) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpLt(&mut self, ctx: &BinaryOpLtContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Lt) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpLe(&mut self, ctx: &BinaryOpLeContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Le) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpAnd(&mut self, ctx: &BinaryOpAndContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::And) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpOr(&mut self, ctx: &BinaryOpOrContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Or) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_binaryOpUnless(&mut self, ctx: &BinaryOpUnlessContext<'input>) -> Self::Return {
        match visit_binary_op_generic(ctx, BinaryOp::Unless) {
            Ok(expr) => VisitorResult::ok(LogQLExpr::Metric(expr)),
            Err(e) => VisitorResult::err(e),
        }
    }
}

// ============================================================================
// Helper visitor functions
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
        },
        MatcherContextAll::MatcherNeqContext(c) => {
            let label = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
            let value = c.STRING().ok_or_else(|| parse_error("Missing value"))?.get_text();
            Ok(LabelMatcher::new(label, MatchOp::Neq, clean_string(&value)))
        },
        MatcherContextAll::MatcherReContext(c) => {
            let label = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
            let value = c.STRING().ok_or_else(|| parse_error("Missing value"))?.get_text();
            Ok(LabelMatcher::new(label, MatchOp::Re, clean_string(&value)))
        },
        MatcherContextAll::MatcherNreContext(c) => {
            let label = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
            let value = c.STRING().ok_or_else(|| parse_error("Missing value"))?.get_text();
            Ok(LabelMatcher::new(label, MatchOp::Nre, clean_string(&value)))
        },
        MatcherContextAll::Error(_) => Err(parse_error("Error in matcher")),
    }
}

/// Collect pipeline stages from a recursive pipelineExpr.
fn collect_pipeline_stages(ctx: &PipelineExprContextAll) -> Result<Vec<PipelineStage>> {
    // Handle the recursive structure: pipelineExpr : pipelineStage | pipelineExpr
    // pipelineStage
    fn collect_recursive(ctx: &PipelineExprContextAll, stages: &mut Vec<PipelineStage>) -> Result<()> {
        // If there's a nested pipelineExpr, process it first (left recursion)
        if let Some(nested) = ctx.pipelineExpr() {
            collect_recursive(&nested, stages)?;
        }

        // Then process this stage
        if let Some(stage_ctx) = ctx.pipelineStage() {
            let stage = visit_pipeline_stage(&stage_ctx)?;
            stages.push(stage);
        }

        Ok(())
    }

    let mut stages = Vec::new();
    collect_recursive(ctx, &mut stages)?;
    Ok(stages)
}

/// Visit a single pipeline stage.
fn visit_pipeline_stage(ctx: &PipelineStageContextAll) -> Result<PipelineStage> {
    // Line filters
    if let Some(line_filters) = ctx.lineFilters() {
        return visit_line_filters(&line_filters);
    }

    // JSON parser
    if let Some(json_parser) = ctx.jsonParser() {
        return visit_json_parser(&json_parser);
    }

    // Logfmt parser
    if let Some(logfmt_parser) = ctx.logfmtParser() {
        return visit_logfmt_parser(&logfmt_parser);
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
        return visit_label_format_expr(&label_format);
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
        let filter = visit_label_filter(&label_filter)?;
        return Ok(PipelineStage::LabelFilter(filter));
    }

    Err(parse_error("Unknown pipeline stage"))
}

/// Visit line filters.
fn visit_line_filters(ctx: &LineFiltersContextAll) -> Result<PipelineStage> {
    let (op, filters) = match ctx {
        LineFiltersContextAll::LineFiltersContainsContext(c) => {
            let filters = collect_line_filter_values(&c.lineFilter_all())?;
            (LineFilterOp::Contains, filters)
        },
        LineFiltersContextAll::LineFiltersNotContainsContext(c) => {
            let filters = collect_line_filter_values(&c.lineFilter_all())?;
            (LineFilterOp::NotContains, filters)
        },
        LineFiltersContextAll::LineFiltersMatchContext(c) => {
            let filters = collect_line_filter_values(&c.lineFilter_all())?;
            (LineFilterOp::Match, filters)
        },
        LineFiltersContextAll::LineFiltersNotMatchContext(c) => {
            let filters = collect_line_filter_values(&c.lineFilter_all())?;
            (LineFilterOp::NotMatch, filters)
        },
        LineFiltersContextAll::LineFiltersNotPatternContext(c) => {
            let filters = collect_line_filter_values(&c.lineFilter_all())?;
            (LineFilterOp::NotPattern, filters)
        },
        LineFiltersContextAll::Error(_) => {
            return Err(parse_error("Error in line filter"));
        },
    };

    Ok(PipelineStage::LineFilter(LineFilter::new(op, filters)))
}

/// Collect line filter values.
fn collect_line_filter_values(filters: &[Rc<LineFilterContextAll>]) -> Result<Vec<LineFilterValue>> {
    let mut values = Vec::new();
    for filter_ctx in filters {
        match filter_ctx.as_ref() {
            LineFilterContextAll::LineFilterStringContext(c) => {
                let s = c.STRING().ok_or_else(|| parse_error("Missing string in line filter"))?;
                values.push(LineFilterValue::String(clean_string(&s.get_text())));
            },
            LineFilterContextAll::LineFilterIpContext(c) => {
                if let Some(ip_fn) = c.ipFn() {
                    let s = ip_fn.STRING().ok_or_else(|| parse_error("Missing IP address in ip()"))?;
                    values.push(LineFilterValue::Ip(clean_string(&s.get_text())));
                }
            },
            LineFilterContextAll::Error(_) => {
                return Err(parse_error("Error in line filter value"));
            },
        }
    }
    Ok(values)
}

/// Visit JSON parser.
fn visit_json_parser(ctx: &JsonParserContext) -> Result<PipelineStage> {
    if let Some(extractions) = ctx.labelExtractions() {
        let fields = visit_label_extractions(&extractions)?;
        Ok(PipelineStage::LogParser(LogParser::Json(Some(fields))))
    } else {
        Ok(PipelineStage::LogParser(LogParser::Json(None)))
    }
}

/// Visit logfmt parser.
fn visit_logfmt_parser(ctx: &LogfmtParserContext) -> Result<PipelineStage> {
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
fn visit_label_format_expr(ctx: &LabelFormatExprContext) -> Result<PipelineStage> {
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
            },
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
            },
            LabelFormatOpContextAll::Error(_) => {
                return Err(parse_error("Error in label format operation"));
            },
        }
    }

    Ok(PipelineStage::LabelFormat(ops))
}

/// Visit label extractions list.
fn visit_label_extractions(ctx: &LabelExtractionsContextAll) -> Result<Vec<LabelExtraction>> {
    let mut extractions = Vec::new();

    for extraction_ctx in ctx.labelExtractionExpr_all() {
        match extraction_ctx.as_ref() {
            LabelExtractionExprContextAll::LabelExtractionSimpleContext(c) => {
                let name = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
                extractions.push(LabelExtraction::new(name));
            },
            LabelExtractionExprContextAll::LabelExtractionWithPathContext(c) => {
                let name = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label name"))?.get_text();
                let path = c.STRING().ok_or_else(|| parse_error("Missing path"))?.get_text();
                extractions.push(LabelExtraction::with_path(name, clean_string(&path)));
            },
            LabelExtractionExprContextAll::Error(_) => {
                return Err(parse_error("Error in label extraction"));
            },
        }
    }

    Ok(extractions)
}

/// Visit label filter expression.
fn visit_label_filter(ctx: &LabelFilterContextAll) -> Result<LabelFilterExpr> {
    match ctx {
        LabelFilterContextAll::LabelFilterAndContext(c) => {
            let filters = c.labelFilter_all();
            if filters.len() < 2 {
                return Err(parse_error("AND filter requires two operands"));
            }
            let left = visit_label_filter(&filters[0])?;
            let right = visit_label_filter(&filters[1])?;
            Ok(LabelFilterExpr::And(Box::new(left), Box::new(right)))
        },
        LabelFilterContextAll::LabelFilterOrContext(c) => {
            let filters = c.labelFilter_all();
            if filters.len() < 2 {
                return Err(parse_error("OR filter requires two operands"));
            }
            let left = visit_label_filter(&filters[0])?;
            let right = visit_label_filter(&filters[1])?;
            Ok(LabelFilterExpr::Or(Box::new(left), Box::new(right)))
        },
        LabelFilterContextAll::LabelFilterParensContext(c) => {
            let inner = c.labelFilter().ok_or_else(|| parse_error("Missing inner filter"))?;
            let inner_expr = visit_label_filter(&inner)?;
            Ok(LabelFilterExpr::Parens(Box::new(inner_expr)))
        },
        LabelFilterContextAll::LabelFilterMatcherContext(c) => {
            let matcher_ctx = c.matcher().ok_or_else(|| parse_error("Missing matcher in label filter"))?;
            let matcher = visit_matcher(&matcher_ctx)?;
            Ok(LabelFilterExpr::Matcher(matcher))
        },
        LabelFilterContextAll::LabelFilterNumberContext(c) => {
            let filter = c.numberFilter().ok_or_else(|| parse_error("Missing number filter"))?;
            visit_number_filter(&filter)
        },
        LabelFilterContextAll::LabelFilterDurationContext(c) => {
            let filter = c.durationFilter().ok_or_else(|| parse_error("Missing duration filter"))?;
            visit_duration_filter(&filter)
        },
        LabelFilterContextAll::LabelFilterBytesContext(c) => {
            let filter = c.bytesFilter().ok_or_else(|| parse_error("Missing bytes filter"))?;
            visit_bytes_filter(&filter)
        },
        LabelFilterContextAll::LabelFilterIpContext(c) => {
            let filter = c.ipLabelFilter().ok_or_else(|| parse_error("Missing IP filter"))?;
            visit_ip_label_filter(&filter)
        },
        LabelFilterContextAll::Error(_) => Err(parse_error("Error in label filter")),
    }
}

/// Visit number filter.
fn visit_number_filter(ctx: &NumberFilterContextAll) -> Result<LabelFilterExpr> {
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
fn visit_duration_filter(ctx: &DurationFilterContextAll) -> Result<LabelFilterExpr> {
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
fn visit_bytes_filter(ctx: &BytesFilterContextAll) -> Result<LabelFilterExpr> {
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
fn visit_ip_label_filter(ctx: &IpLabelFilterContextAll) -> Result<LabelFilterExpr> {
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
        },
        LiteralExprContextAll::LiteralPositiveNumberContext(c) => {
            let num = c.NUMBER().ok_or_else(|| parse_error("Missing number"))?;
            parse_number(&num.get_text())
        },
        LiteralExprContextAll::LiteralNegativeNumberContext(c) => {
            let num = c.NUMBER().ok_or_else(|| parse_error("Missing number"))?;
            let value = parse_number(&num.get_text())?;
            Ok(-value)
        },
        LiteralExprContextAll::Error(_) => Err(parse_error("Error in literal expression")),
    }
}

// ============================================================================
// Metric Expression Helpers
// ============================================================================

/// Visit a metric expression context.
fn visit_metric_expr(ctx: &MetricExprContextAll) -> Result<MetricExpr> {
    match ctx {
        MetricExprContextAll::MetricExprRangeAggContext(c) => visit_metric_expr_range_agg(c),
        MetricExprContextAll::MetricExprVectorAggContext(c) => visit_metric_expr_vector_agg(c),
        MetricExprContextAll::MetricExprLiteralContext(c) => visit_metric_expr_literal(c),
        MetricExprContextAll::MetricExprParensContext(c) => {
            let inner = c.metricExpr().ok_or_else(|| parse_error("Missing inner expression"))?;
            let inner_expr = visit_metric_expr(&inner)?;
            Ok(MetricExpr::Parens(Box::new(inner_expr)))
        },
        MetricExprContextAll::MetricExprLabelReplaceContext(c) => visit_metric_expr_label_replace(c),
        MetricExprContextAll::MetricExprVectorContext(c) => {
            let vector_ctx = c.vectorExpr().ok_or_else(|| parse_error("Missing vector expression"))?;
            let num = vector_ctx.NUMBER().ok_or_else(|| parse_error("Missing number in vector()"))?;
            let value = parse_number(&num.get_text())?;
            Ok(MetricExpr::Vector(value))
        },
        MetricExprContextAll::MetricExprVariableContext(c) => {
            let var_ctx = c.variableExpr().ok_or_else(|| parse_error("Missing variable expression"))?;
            let name = var_ctx
                .ATTRIBUTE()
                .ok_or_else(|| parse_error("Missing variable name"))?
                .get_text();
            Ok(MetricExpr::Variable(name))
        },
        // Binary operations
        MetricExprContextAll::BinaryOpPowContext(c) => visit_binary_op_generic(c, BinaryOp::Pow),
        MetricExprContextAll::BinaryOpMulContext(c) => visit_binary_op_generic(c, BinaryOp::Mul),
        MetricExprContextAll::BinaryOpDivContext(c) => visit_binary_op_generic(c, BinaryOp::Div),
        MetricExprContextAll::BinaryOpModContext(c) => visit_binary_op_generic(c, BinaryOp::Mod),
        MetricExprContextAll::BinaryOpAddContext(c) => visit_binary_op_generic(c, BinaryOp::Add),
        MetricExprContextAll::BinaryOpSubContext(c) => visit_binary_op_generic(c, BinaryOp::Sub),
        MetricExprContextAll::BinaryOpEqlContext(c) => visit_binary_op_generic(c, BinaryOp::Eq),
        MetricExprContextAll::BinaryOpNeqContext(c) => visit_binary_op_generic(c, BinaryOp::Neq),
        MetricExprContextAll::BinaryOpGtContext(c) => visit_binary_op_generic(c, BinaryOp::Gt),
        MetricExprContextAll::BinaryOpGeContext(c) => visit_binary_op_generic(c, BinaryOp::Ge),
        MetricExprContextAll::BinaryOpLtContext(c) => visit_binary_op_generic(c, BinaryOp::Lt),
        MetricExprContextAll::BinaryOpLeContext(c) => visit_binary_op_generic(c, BinaryOp::Le),
        MetricExprContextAll::BinaryOpAndContext(c) => visit_binary_op_generic(c, BinaryOp::And),
        MetricExprContextAll::BinaryOpOrContext(c) => visit_binary_op_generic(c, BinaryOp::Or),
        MetricExprContextAll::BinaryOpUnlessContext(c) => visit_binary_op_generic(c, BinaryOp::Unless),
        MetricExprContextAll::Error(_) => Err(parse_error("Error in metric expression")),
    }
}

fn visit_metric_expr_range_agg(ctx: &MetricExprRangeAggContext) -> Result<MetricExpr> {
    let agg_ctx = ctx
        .rangeAggregationExpr()
        .ok_or_else(|| parse_error("Missing range aggregation"))?;
    let agg = visit_range_aggregation(&agg_ctx)?;
    Ok(MetricExpr::RangeAggregation(agg))
}

fn visit_metric_expr_vector_agg(ctx: &MetricExprVectorAggContext) -> Result<MetricExpr> {
    let agg_ctx = ctx
        .vectorAggregationExpr()
        .ok_or_else(|| parse_error("Missing vector aggregation"))?;
    let agg = visit_vector_aggregation(&agg_ctx)?;
    Ok(MetricExpr::VectorAggregation(agg))
}

fn visit_metric_expr_literal(ctx: &MetricExprLiteralContext) -> Result<MetricExpr> {
    let literal = ctx.literalExpr().ok_or_else(|| parse_error("Missing literal"))?;
    let value = visit_literal_value(&literal)?;
    Ok(MetricExpr::Literal(value))
}

fn visit_metric_expr_label_replace(ctx: &MetricExprLabelReplaceContext) -> Result<MetricExpr> {
    let label_replace = ctx.labelReplaceExpr().ok_or_else(|| parse_error("Missing label_replace"))?;

    let metric_expr = label_replace
        .metricExpr()
        .ok_or_else(|| parse_error("Missing metric expression in label_replace"))?;
    let expr = visit_metric_expr(&metric_expr)?;

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

fn visit_binary_op_generic<'input, T: BinaryOpContextHelper<'input>>(ctx: &T, op: BinaryOp) -> Result<MetricExpr> {
    let exprs = ctx.get_metric_exprs();
    if exprs.len() < 2 {
        return Err(parse_error("Binary operation requires two operands"));
    }

    let left = visit_metric_expr(&exprs[0])?;
    let right = visit_metric_expr(&exprs[1])?;

    let modifier = if let Some(mod_ctx) = ctx.get_bin_op_modifier() {
        Some(visit_bin_op_modifier(&mod_ctx)?)
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

fn visit_bin_op_modifier(ctx: &BinOpModifierContextAll) -> Result<BinaryOpModifier> {
    let return_bool = ctx.BOOL().is_some();

    let vector_matching = if let Some(matching_ctx) = ctx.onOrIgnoringModifier() {
        Some(visit_vector_matching(&matching_ctx, ctx)?)
    } else {
        None
    };

    Ok(BinaryOpModifier {
        return_bool,
        vector_matching,
    })
}

fn visit_vector_matching(
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
// Range Aggregation
// ============================================================================

fn visit_range_aggregation(ctx: &RangeAggregationExprContextAll) -> Result<RangeAggregation> {
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
        visit_log_range_expr(&log_range)?
    } else if let Some(unwrapped_range) = ctx.unwrappedRangeExpr() {
        visit_unwrapped_range_expr(&unwrapped_range)?
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

fn visit_range_unwrap_op_no_group(ctx: &RangeUnwrapOpNoGroupingContextAll) -> Result<RangeAggregationOp> {
    match ctx {
        RangeUnwrapOpNoGroupingContextAll::RangeUnwrapOpNoGroupSumContext(_) => Ok(RangeAggregationOp::SumOverTime),
        RangeUnwrapOpNoGroupingContextAll::RangeUnwrapOpNoGroupRateContext(_) => Ok(RangeAggregationOp::Rate),
        RangeUnwrapOpNoGroupingContextAll::RangeUnwrapOpNoGroupRateCounterContext(_) => {
            Ok(RangeAggregationOp::RateCounter)
        },
        RangeUnwrapOpNoGroupingContextAll::Error(_) => Err(parse_error("Error in range unwrap operation")),
    }
}

fn visit_range_unwrap_op_with_group(
    ctx: &RangeUnwrapOpWithGroupingContextAll,
) -> Result<(RangeAggregationOp, Option<f64>)> {
    match ctx {
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpAvgContext(_) => Ok((RangeAggregationOp::AvgOverTime, None)),
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpMinContext(_) => Ok((RangeAggregationOp::MinOverTime, None)),
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpMaxContext(_) => Ok((RangeAggregationOp::MaxOverTime, None)),
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpStddevContext(_) => {
            Ok((RangeAggregationOp::StddevOverTime, None))
        },
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpStdvarContext(_) => {
            Ok((RangeAggregationOp::StdvarOverTime, None))
        },
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpQuantileContext(_) => {
            // Note: quantile parameter would need to come from parent context
            Ok((RangeAggregationOp::QuantileOverTime, None))
        },
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpFirstContext(_) => {
            Ok((RangeAggregationOp::FirstOverTime, None))
        },
        RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpLastContext(_) => {
            Ok((RangeAggregationOp::LastOverTime, None))
        },
        RangeUnwrapOpWithGroupingContextAll::Error(_) => Err(parse_error("Error in range unwrap operation")),
    }
}

fn visit_log_range_expr(ctx: &LogRangeExprContextAll) -> Result<RangeExpr> {
    let selector_ctx = ctx
        .selector()
        .ok_or_else(|| parse_error("Missing selector in range expression"))?;
    let selector = visit_selector(&selector_ctx)?;

    let range_ctx = ctx.range().ok_or_else(|| parse_error("Missing range"))?;
    let range = visit_range(&range_ctx)?;

    let pipeline = if let Some(pipeline_ctx) = ctx.pipelineExpr() {
        collect_pipeline_stages(&pipeline_ctx)?
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

fn visit_unwrapped_range_expr(ctx: &UnwrappedRangeExprContextAll) -> Result<RangeExpr> {
    let selector_ctx = ctx
        .selector()
        .ok_or_else(|| parse_error("Missing selector in unwrapped range"))?;
    let selector = visit_selector(&selector_ctx)?;

    let range_ctx = ctx.range().ok_or_else(|| parse_error("Missing range"))?;
    let range = visit_range(&range_ctx)?;

    let pipeline = if let Some(pipeline_ctx) = ctx.pipelineExpr() {
        collect_pipeline_stages(&pipeline_ctx)?
    } else {
        Vec::new()
    };

    let log_expr = LogExpr::with_pipeline(selector, pipeline);

    let unwrap_ctx = ctx.unwrapExpr().ok_or_else(|| parse_error("Missing unwrap expression"))?;
    let unwrap = visit_unwrap_expr(&unwrap_ctx)?;

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

fn visit_range(ctx: &RangeContextAll) -> Result<Duration> {
    // range: LBRACK duration RBRACK
    let duration_ctx = ctx.duration().ok_or_else(|| parse_error("Missing duration in range"))?;
    // duration: DURATION
    let duration_token = duration_ctx.DURATION().ok_or_else(|| parse_error("Missing duration token"))?;
    parse_duration(&duration_token.get_text())
}

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

fn visit_unwrap_expr(ctx: &UnwrapExprContextAll) -> Result<UnwrapExpr> {
    match ctx {
        UnwrapExprContextAll::UnwrapBasicContext(c) => {
            let label = c.ATTRIBUTE().ok_or_else(|| parse_error("Missing label in unwrap"))?.get_text();
            Ok(UnwrapExpr::new(label))
        },
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
        },
        UnwrapExprContextAll::UnwrapWithFilterContext(c) => {
            // Get base unwrap from inner unwrapExpr
            let inner = c.unwrapExpr().ok_or_else(|| parse_error("Missing unwrap expression"))?;
            let mut unwrap = visit_unwrap_expr(&inner)?;

            // Add filter if present
            if let Some(filter_ctx) = c.labelFilter() {
                let filter = visit_label_filter(&filter_ctx)?;
                unwrap.post_filter = Some(Box::new(filter));
            }

            Ok(unwrap)
        },
        UnwrapExprContextAll::Error(_) => Err(parse_error("Error in unwrap expression")),
    }
}

// ============================================================================
// Vector Aggregation
// ============================================================================

fn visit_vector_aggregation(ctx: &VectorAggregationExprContextAll) -> Result<VectorAggregation> {
    let op_ctx = ctx.vectorOp().ok_or_else(|| parse_error("Missing vector operation"))?;
    let op = visit_vector_op(&op_ctx)?;

    let metric_ctx = ctx
        .metricExpr()
        .ok_or_else(|| parse_error("Missing metric expression in vector aggregation"))?;
    let expr = visit_metric_expr(&metric_ctx)?;

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

fn visit_grouping(ctx: &GroupingContextAll) -> Result<Grouping> {
    match ctx {
        GroupingContextAll::GroupingByContext(c) => {
            let labels = c
                .groupingLabels()
                .map_or_else(Vec::new, |labels_ctx| collect_grouping_labels(&labels_ctx));
            Ok(Grouping::By(labels))
        },
        GroupingContextAll::GroupingWithoutContext(c) => {
            let labels = c
                .groupingLabels()
                .map_or_else(Vec::new, |labels_ctx| collect_grouping_labels(&labels_ctx));
            Ok(Grouping::Without(labels))
        },
        GroupingContextAll::GroupingByEmptyContext(_) => Ok(Grouping::By(Vec::new())),
        GroupingContextAll::GroupingWithoutEmptyContext(_) => Ok(Grouping::Without(Vec::new())),
        GroupingContextAll::Error(_) => Err(parse_error("Error in grouping")),
    }
}

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
