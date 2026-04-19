//! AST visitor that converts ANTLR parse trees into [`TraceQLExpr`].
//!
//! Mirrors the `LogQL` visitor pattern: a single
//! [`ParseTreeVisitorCompat`] impl with `Self::Return = VisitorResult<TraceQLExpr>`
//! plus a collection of free helper functions that walk the inner contexts
//! and return typed `Result<T>` values directly. Most of the recursion is
//! done via plain function calls (not via `self.visit(...)`); only the
//! top-level `visit_root` produces the `Self::Return`.

use std::rc::Rc;

use antlr4rust::tree::{ParseTree, ParseTreeVisitorCompat};

#[allow(clippy::wildcard_imports)]
use super::traceqlparser::*;
use super::traceqlparservisitor::TraceQLParserVisitorCompat;
use crate::{
    error::{QueryError, Result, parse_error},
    traceql::{
        common::{ComparisonOp, FieldRef, IntrinsicField, KindValue, LiteralValue, ParentScope, Scope, StatusValue},
        duration::parse_duration_nanos,
        expr::TraceQLExpr,
        metric::{AggregationOp, GroupingKeys, MetricsFunction, PipelineExpr, PipelineStage},
        spanset::{SpanFilter, SpanSelector, SpansetExpr, SpansetOp},
    },
};

// ============================================================================
// VisitorResult: ANTLR's Return type requires Default; Result does not.
// ============================================================================

/// Wrapper for visitor results that implements `Default`.
///
/// The antlr4rust runtime requires `Self::Return: Default`. Wrap the
/// fallible result in a struct so we can give it a meaningful default
/// (an "unvisited" error) without polluting the AST type.
pub struct VisitorResult<T>(Result<T>);

impl<T> Default for VisitorResult<T> {
    fn default() -> Self {
        Self(Err(parse_error("not yet visited")))
    }
}

impl<T> VisitorResult<T> {
    /// Wrap a successful value.
    pub const fn ok(value: T) -> Self {
        Self(Ok(value))
    }

    /// Wrap an error.
    pub const fn err(error: QueryError) -> Self {
        Self(Err(error))
    }

    /// Unwrap into a `Result`.
    pub fn into_result(self) -> Result<T> {
        self.0
    }
}

// ============================================================================
// Pure helpers — string utilities, literals, enum mappers
// ============================================================================

/// Strip surrounding `"` or `'` quotes from a string literal token.
fn clean_string(text: &str) -> String {
    let trimmed = text.trim();
    if (trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2)
        || (trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2)
    {
        trimmed[1..trimmed.len() - 1].to_string()
    } else {
        trimmed.to_string()
    }
}

/// Parse a `TraceQL` bytes literal (e.g., `1KiB`, `10MB`) into raw bytes.
fn parse_bytes(text: &str) -> Result<i64> {
    const SUFFIXES: &[(&str, i64)] = &[
        ("PiB", 1024_i64.pow(5)),
        ("TiB", 1024_i64.pow(4)),
        ("GiB", 1024_i64.pow(3)),
        ("MiB", 1024 * 1024),
        ("KiB", 1024),
        ("PB", 1000_i64.pow(5)),
        ("TB", 1000_i64.pow(4)),
        ("GB", 1000_i64.pow(3)),
        ("MB", 1000 * 1000),
        ("KB", 1000),
        ("B", 1),
    ];
    let trimmed = text.trim();
    for (suffix, multiplier) in SUFFIXES {
        if let Some(num_str) = trimmed.strip_suffix(suffix) {
            let num: f64 = num_str
                .parse()
                .map_err(|_| parse_error(format!("invalid bytes literal: {text}")))?;
            // Multiplier values are bounded constants (largest = 1024^5 ≈
            // 1.13e15) which fit comfortably in f64's 2^53 mantissa range,
            // so the cast is exact for every value in the table.
            #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
            let bytes = (num * (*multiplier as f64)).trunc() as i64;
            return Ok(bytes);
        }
    }
    Err(parse_error(format!("invalid bytes literal: {text}")))
}

/// Parse a `TraceQL` duration literal (e.g., `1s`, `100ms`) into nanoseconds.
fn parse_duration_literal(text: &str) -> Result<i64> {
    parse_duration_nanos(text).ok_or_else(|| parse_error(format!("invalid duration: {text}")))
}

/// Parse an integer literal token (e.g., `42`, `-7`).
fn parse_int_literal(text: &str) -> Result<i64> {
    text.trim()
        .parse::<i64>()
        .map_err(|_| parse_error(format!("invalid integer literal: {text}")))
}

/// Parse a float literal token (e.g., `3.14`).
fn parse_float_literal(text: &str) -> Result<f64> {
    text.trim()
        .parse::<f64>()
        .map_err(|_| parse_error(format!("invalid float literal: {text}")))
}

// ----------------------------------------------------------------------------
// Comparison / spanset / aggregate / metrics enum mappers
// ----------------------------------------------------------------------------

fn visit_comparison_op(ctx: &ComparisonOpContext<'_>) -> Result<ComparisonOp> {
    if ctx.EQ_RE().is_some() {
        Ok(ComparisonOp::Re)
    } else if ctx.NEQ_RE().is_some() {
        // NEQ_RE token also stands for the spanset NOT_SIBLING glyph at the
        // lexer level (longest-match wins), but inside a comparisonOp context
        // it can only mean regex-not-match.
        Ok(ComparisonOp::Nre)
    } else if ctx.GE().is_some() {
        Ok(ComparisonOp::Ge)
    } else if ctx.LE().is_some() {
        Ok(ComparisonOp::Le)
    } else if ctx.NEQ().is_some() {
        Ok(ComparisonOp::Neq)
    } else if ctx.EQ().is_some() {
        Ok(ComparisonOp::Eq)
    } else if ctx.GT().is_some() {
        Ok(ComparisonOp::Gt)
    } else if ctx.LT().is_some() {
        Ok(ComparisonOp::Lt)
    } else {
        Err(parse_error("unknown comparison operator"))
    }
}

fn visit_spanset_op(ctx: &SpansetOpContext<'_>) -> Result<SpansetOp> {
    // Order matters: longer/more-specific tokens first to avoid masking.
    if ctx.AND().is_some() {
        Ok(SpansetOp::And)
    } else if ctx.OR().is_some() {
        Ok(SpansetOp::Or)
    } else if ctx.DESC().is_some() {
        Ok(SpansetOp::Descendant)
    } else if ctx.ANC().is_some() {
        Ok(SpansetOp::Ancestor)
    } else if ctx.NOT_DESC().is_some() {
        Ok(SpansetOp::NotDescendant)
    } else if ctx.NOT_ANC().is_some() {
        Ok(SpansetOp::NotAncestor)
    } else if ctx.NOT_CHILD().is_some() {
        Ok(SpansetOp::NotChild)
    } else if ctx.NOT_PARENT().is_some() {
        Ok(SpansetOp::NotParent)
    } else if ctx.NOT_SIBLING().is_some() {
        // The lexer prefers NEQ_RE, but the grammar also accepts NEQ_RE here
        // — check both via the text fallback below if the dedicated accessor
        // returned None.
        Ok(SpansetOp::NotSibling)
    } else if ctx.SIBLING().is_some() {
        Ok(SpansetOp::Sibling)
    } else if ctx.GT().is_some() {
        Ok(SpansetOp::Child)
    } else if ctx.LT().is_some() {
        Ok(SpansetOp::Parent)
    } else {
        // Fallback: inspect raw text to handle the `!~` collision (lexer
        // emits NEQ_RE; not exposed by the spansetOp context accessors).
        match ctx.get_text().as_str() {
            "!~" => Ok(SpansetOp::NotSibling),
            other => Err(parse_error(format!("unknown spanset operator: {other}"))),
        }
    }
}

fn visit_aggregate_op(ctx: &AggregateOpContext<'_>) -> Result<AggregationOp> {
    if ctx.FN_COUNT().is_some() {
        Ok(AggregationOp::Count)
    } else if ctx.FN_SUM().is_some() {
        Ok(AggregationOp::Sum)
    } else if ctx.FN_AVG().is_some() {
        Ok(AggregationOp::Avg)
    } else if ctx.FN_MIN().is_some() {
        Ok(AggregationOp::Min)
    } else if ctx.FN_MAX().is_some() {
        Ok(AggregationOp::Max)
    } else if ctx.FN_QUANTILE().is_some() {
        Ok(AggregationOp::Quantile)
    } else {
        Err(parse_error("unknown aggregate operator"))
    }
}

// ----------------------------------------------------------------------------
// Literal / intrinsic
// ----------------------------------------------------------------------------

fn visit_literal(ctx: &LiteralContext<'_>) -> Result<LiteralValue> {
    if let Some(tok) = ctx.DURATION() {
        return Ok(LiteralValue::Duration(parse_duration_literal(&tok.get_text())?));
    }
    if let Some(tok) = ctx.BYTES() {
        return Ok(LiteralValue::Bytes(parse_bytes(&tok.get_text())?));
    }
    if let Some(tok) = ctx.FLOAT() {
        return Ok(LiteralValue::Float(parse_float_literal(&tok.get_text())?));
    }
    if let Some(tok) = ctx.INT() {
        return Ok(LiteralValue::Int(parse_int_literal(&tok.get_text())?));
    }
    if let Some(tok) = ctx.STRING() {
        return Ok(LiteralValue::String(clean_string(&tok.get_text())));
    }
    if ctx.KW_TRUE().is_some() {
        return Ok(LiteralValue::Bool(true));
    }
    if ctx.KW_FALSE().is_some() {
        return Ok(LiteralValue::Bool(false));
    }
    if ctx.KW_NIL().is_some() {
        return Ok(LiteralValue::Nil);
    }
    if ctx.STATUS_OK().is_some() {
        return Ok(LiteralValue::Status(StatusValue::Ok));
    }
    if ctx.STATUS_ERROR().is_some() {
        return Ok(LiteralValue::Status(StatusValue::Error));
    }
    if ctx.STATUS_UNSET().is_some() {
        return Ok(LiteralValue::Status(StatusValue::Unset));
    }
    if ctx.KIND_SERVER().is_some() {
        return Ok(LiteralValue::Kind(KindValue::Server));
    }
    if ctx.KIND_CLIENT().is_some() {
        return Ok(LiteralValue::Kind(KindValue::Client));
    }
    if ctx.KIND_PRODUCER().is_some() {
        return Ok(LiteralValue::Kind(KindValue::Producer));
    }
    if ctx.KIND_CONSUMER().is_some() {
        return Ok(LiteralValue::Kind(KindValue::Consumer));
    }
    if ctx.KIND_INTERNAL().is_some() {
        return Ok(LiteralValue::Kind(KindValue::Internal));
    }
    // Bare-word string: Grafana's Tempo data source emits unquoted values
    // for attribute filters like `{.svc = frontend}`. Treat any IDENT here
    // as a string literal to match that tolerance.
    if let Some(tok) = ctx.IDENT() {
        return Ok(LiteralValue::String(tok.get_text()));
    }
    Err(parse_error("unrecognized literal"))
}

fn visit_intrinsic(ctx: &IntrinsicContext<'_>) -> Result<IntrinsicField> {
    if ctx.INTR_NAME().is_some() {
        Ok(IntrinsicField::Name)
    } else if ctx.INTR_STATUS_MESSAGE().is_some() {
        // Check status_message before status because the longer match would
        // otherwise be missed if both accessors return Some.
        Ok(IntrinsicField::StatusMessage)
    } else if ctx.INTR_STATUS().is_some() {
        Ok(IntrinsicField::Status)
    } else if ctx.INTR_KIND().is_some() {
        Ok(IntrinsicField::Kind)
    } else if ctx.INTR_TRACE_DURATION().is_some() {
        Ok(IntrinsicField::TraceDuration)
    } else if ctx.INTR_DURATION().is_some() {
        Ok(IntrinsicField::Duration)
    } else if ctx.INTR_ROOT_NAME().is_some() {
        Ok(IntrinsicField::RootName)
    } else if ctx.INTR_ROOT_SVC().is_some() {
        Ok(IntrinsicField::RootServiceName)
    } else if ctx.INTR_TRACE_ID().is_some() {
        Ok(IntrinsicField::TraceID)
    } else if ctx.INTR_SPAN_ID().is_some() {
        Ok(IntrinsicField::SpanID)
    } else {
        Err(parse_error("unrecognized intrinsic field"))
    }
}

// ----------------------------------------------------------------------------
// identChain / scopedAttribute / fieldRef
// ----------------------------------------------------------------------------

fn visit_ident_chain(ctx: &IdentChainContext<'_>) -> Result<String> {
    let parts = ctx.identPart_all();
    if parts.is_empty() {
        return Err(parse_error("empty identifier chain"));
    }
    let mut out = String::new();
    for (i, part) in parts.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        // identPart matches a single token; its text is exactly that token.
        out.push_str(part.get_text().trim());
    }
    Ok(out)
}

fn visit_scoped_attribute(ctx: &ScopedAttributeContext<'_>) -> Result<FieldRef> {
    let ident_ctx = ctx.identChain().ok_or_else(|| parse_error("missing identifier chain"))?;
    let name = visit_ident_chain(&ident_ctx)?;
    let scope = if ctx.SCOPE_SPAN().is_some() && ctx.SCOPE_PARENT().is_none() {
        Scope::Span
    } else if ctx.SCOPE_RESOURCE().is_some() && ctx.SCOPE_PARENT().is_none() {
        Scope::Resource
    } else if ctx.SCOPE_EVENT().is_some() {
        Scope::Event
    } else if ctx.SCOPE_LINK().is_some() {
        Scope::Link
    } else if ctx.SCOPE_PARENT().is_some() {
        // parent.span.foo or parent.resource.foo — the inner SCOPE_SPAN /
        // SCOPE_RESOURCE accessor will match the second token.
        let inner = if ctx.SCOPE_SPAN().is_some() {
            ParentScope::Span
        } else if ctx.SCOPE_RESOURCE().is_some() {
            ParentScope::Resource
        } else {
            return Err(parse_error("parent scope must be parent.span or parent.resource"));
        };
        Scope::Parent(inner)
    } else {
        return Err(parse_error("unknown attribute scope"));
    };
    Ok(FieldRef::Attribute { scope, name })
}

fn visit_field_ref(ctx: &FieldRefContextAll<'_>) -> Result<FieldRef> {
    match ctx {
        FieldRefContextAll::FieldIntrinsicContext(c) => {
            let intr = c.intrinsic().ok_or_else(|| parse_error("missing intrinsic"))?;
            Ok(FieldRef::Intrinsic(visit_intrinsic(&intr)?))
        }
        FieldRefContextAll::FieldScopedContext(c) => {
            let scoped = c.scopedAttribute().ok_or_else(|| parse_error("missing scoped attribute"))?;
            visit_scoped_attribute(&scoped)
        }
        FieldRefContextAll::FieldAnyScopeContext(c) => {
            let chain = c.identChain().ok_or_else(|| parse_error("missing identifier chain"))?;
            let name = visit_ident_chain(&chain)?;
            Ok(FieldRef::Attribute {
                scope: Scope::Any,
                name,
            })
        }
        FieldRefContextAll::Error(_) => Err(parse_error("error in field reference")),
    }
}

// ----------------------------------------------------------------------------
// spanFilter (recursive)
// ----------------------------------------------------------------------------

/// Maximum filter nesting depth — guards against pathological inputs that
/// would otherwise blow the stack.
const MAX_FILTER_DEPTH: usize = 100;

fn visit_span_filter(ctx: &SpanFilterContextAll<'_>, depth: usize) -> Result<SpanFilter> {
    if depth > MAX_FILTER_DEPTH {
        return Err(parse_error("span filter exceeds maximum nesting depth"));
    }
    match ctx {
        SpanFilterContextAll::FilterCompareContext(c) => {
            let field_ctx = c.fieldRef().ok_or_else(|| parse_error("missing field reference"))?;
            let cmp_ctx = c.comparisonOp().ok_or_else(|| parse_error("missing comparison op"))?;
            let lit_ctx = c.literal().ok_or_else(|| parse_error("missing literal"))?;
            let field = visit_field_ref(&field_ctx)?;
            let op = visit_comparison_op(&cmp_ctx)?;
            let value = visit_literal(&lit_ctx)?;
            Ok(SpanFilter::Compare { field, op, value })
        }
        SpanFilterContextAll::FilterParenContext(c) => {
            let inner = c.spanFilter().ok_or_else(|| parse_error("missing inner filter"))?;
            let inner_filter = visit_span_filter(&inner, depth + 1)?;
            Ok(SpanFilter::Paren(Box::new(inner_filter)))
        }
        SpanFilterContextAll::FilterNotContext(c) => {
            let inner = c.spanFilter().ok_or_else(|| parse_error("missing operand for !"))?;
            let inner_filter = visit_span_filter(&inner, depth + 1)?;
            Ok(SpanFilter::Not(Box::new(inner_filter)))
        }
        SpanFilterContextAll::FilterAndContext(c) => {
            let (lhs, rhs) = pop_two_filters(&c.spanFilter_all(), depth)?;
            Ok(SpanFilter::And(Box::new(lhs), Box::new(rhs)))
        }
        SpanFilterContextAll::FilterOrContext(c) => {
            let (lhs, rhs) = pop_two_filters(&c.spanFilter_all(), depth)?;
            Ok(SpanFilter::Or(Box::new(lhs), Box::new(rhs)))
        }
        SpanFilterContextAll::Error(_) => Err(parse_error("error in span filter")),
    }
}

fn pop_two_filters(filters: &[Rc<SpanFilterContextAll<'_>>], depth: usize) -> Result<(SpanFilter, SpanFilter)> {
    if filters.len() < 2 {
        return Err(parse_error("binary filter requires two operands"));
    }
    let lhs = visit_span_filter(&filters[0], depth + 1)?;
    let rhs = visit_span_filter(&filters[1], depth + 1)?;
    Ok((lhs, rhs))
}

// ----------------------------------------------------------------------------
// spanSelector / spansetExpr
// ----------------------------------------------------------------------------

fn visit_span_selector(ctx: &SpanSelectorContext<'_>) -> Result<SpanSelector> {
    if let Some(filter_ctx) = ctx.spanFilter() {
        let filter = visit_span_filter(&filter_ctx, 0)?;
        Ok(SpanSelector::new(filter))
    } else {
        Ok(SpanSelector::all())
    }
}

fn visit_spanset_expr(ctx: &SpansetExprContextAll<'_>) -> Result<SpansetExpr> {
    match ctx {
        SpansetExprContextAll::SpansetLeafContext(c) => {
            let sel_ctx = c.spanSelector().ok_or_else(|| parse_error("missing span selector"))?;
            Ok(SpansetExpr::Selector(visit_span_selector(&sel_ctx)?))
        }
        SpansetExprContextAll::SpansetParenContext(c) => {
            // Parens don't add an AST node; pass through.
            let inner = c.spansetExpr().ok_or_else(|| parse_error("missing inner spanset"))?;
            visit_spanset_expr(&inner)
        }
        SpansetExprContextAll::SpansetBinaryContext(c) => {
            let exprs = c.spansetExpr_all();
            if exprs.len() < 2 {
                return Err(parse_error("spanset binary op requires two operands"));
            }
            let lhs = visit_spanset_expr(&exprs[0])?;
            let rhs = visit_spanset_expr(&exprs[1])?;
            let op_ctx = c.spansetOp().ok_or_else(|| parse_error("missing spanset operator"))?;
            let op = visit_spanset_op(&op_ctx)?;
            Ok(SpansetExpr::Op {
                lhs: Box::new(lhs),
                op,
                rhs: Box::new(rhs),
            })
        }
        SpansetExprContextAll::Error(_) => Err(parse_error("error in spanset expression")),
    }
}

// ----------------------------------------------------------------------------
// Pipeline — byClause, aggregate, aggregateFilter, metricsFunction, stages
// ----------------------------------------------------------------------------

fn visit_by_clause(ctx: &ByClauseContext<'_>) -> Result<GroupingKeys> {
    let mut keys = Vec::new();
    for field_ctx in ctx.fieldRef_all() {
        keys.push(visit_field_ref(&field_ctx)?);
    }
    if keys.is_empty() {
        return Err(parse_error("by(...) requires at least one field reference"));
    }
    Ok(GroupingKeys { keys })
}

fn visit_aggregate(ctx: &AggregateContext<'_>) -> Result<PipelineStage> {
    let op_ctx = ctx.aggregateOp().ok_or_else(|| parse_error("missing aggregate operator"))?;
    let op = visit_aggregate_op(&op_ctx)?;
    let arg = if let Some(field_ctx) = ctx.fieldRef() {
        Some(visit_field_ref(&field_ctx)?)
    } else {
        None
    };
    if op.requires_arg() && arg.is_none() {
        return Err(parse_error(format!("{} requires a field argument", op.as_str())));
    }
    Ok(PipelineStage::Aggregate { op, arg })
}

fn visit_aggregate_filter(ctx: &AggregateFilterContext<'_>) -> Result<PipelineStage> {
    let op_ctx = ctx.aggregateOp().ok_or_else(|| parse_error("missing aggregate operator"))?;
    let op = visit_aggregate_op(&op_ctx)?;
    let arg = if let Some(field_ctx) = ctx.fieldRef() {
        Some(visit_field_ref(&field_ctx)?)
    } else {
        None
    };
    if op.requires_arg() && arg.is_none() {
        return Err(parse_error(format!("{} requires a field argument", op.as_str())));
    }
    let cmp_ctx = ctx.comparisonOp().ok_or_else(|| parse_error("missing comparison op"))?;
    let cmp = visit_comparison_op(&cmp_ctx)?;
    let lit_ctx = ctx.literal().ok_or_else(|| parse_error("missing literal"))?;
    let value = visit_literal(&lit_ctx)?;
    Ok(PipelineStage::AggregateFilter { op, arg, cmp, value })
}

fn visit_metrics_function(ctx: &MetricsFunctionContext<'_>) -> Result<MetricsFunction> {
    if ctx.FN_RATE().is_some() {
        return Ok(MetricsFunction::Rate);
    }
    if ctx.FN_COUNT_OVER_TIME().is_some() {
        return Ok(MetricsFunction::CountOverTime);
    }
    if ctx.FN_HISTOGRAM_OVER_TIME().is_some() {
        let field_ctx = ctx
            .fieldRef()
            .ok_or_else(|| parse_error("histogram_over_time requires a field argument"))?;
        let field = visit_field_ref(&field_ctx)?;
        return Ok(MetricsFunction::HistogramOverTime { field });
    }
    Err(parse_error("unrecognized metrics function"))
}

/// Result of visiting a single pipeline stage. The variant tells the
/// pipeline-level dispatcher whether the stage is search-only, terminal
/// (metrics), or grouping-only.
enum StageOutcome {
    /// A search-mode stage (`by(...)`, `count()`, `count() > N`, …).
    Search(PipelineStage),
    /// A terminal metrics function (`rate()`, `count_over_time()`, …) with
    /// optional `by(...)` grouping attached.
    Metrics {
        function: MetricsFunction,
        group_by: Option<GroupingKeys>,
    },
}

fn visit_pipeline_stage(ctx: &PipelineStageContext<'_>) -> Result<StageOutcome> {
    if let Some(by_ctx) = ctx.byClause()
        && ctx.metricsFunction().is_none()
    {
        let keys = visit_by_clause(&by_ctx)?;
        return Ok(StageOutcome::Search(PipelineStage::By(keys)));
    }
    if let Some(filter_ctx) = ctx.aggregateFilter() {
        return Ok(StageOutcome::Search(visit_aggregate_filter(&filter_ctx)?));
    }
    if let Some(agg_ctx) = ctx.aggregate() {
        return Ok(StageOutcome::Search(visit_aggregate(&agg_ctx)?));
    }
    if let Some(metrics_ctx) = ctx.metricsFunction() {
        let function = visit_metrics_function(&metrics_ctx)?;
        let group_by = if let Some(by_ctx) = ctx.byClause() {
            Some(visit_by_clause(&by_ctx)?)
        } else {
            None
        };
        return Ok(StageOutcome::Metrics { function, group_by });
    }
    Err(parse_error("unknown pipeline stage"))
}

fn visit_pipeline_expr(ctx: &PipelineExprContext<'_>) -> Result<PipelineExpr> {
    let source_ctx = ctx.spansetExpr().ok_or_else(|| parse_error("missing spanset in pipeline"))?;
    let source = visit_spanset_expr(&source_ctx)?;

    let stage_ctxs = ctx.pipelineStage_all();
    if stage_ctxs.is_empty() {
        return Err(parse_error("pipeline requires at least one stage"));
    }

    let mut search_stages: Vec<PipelineStage> = Vec::with_capacity(stage_ctxs.len());
    for (i, stage_ctx) in stage_ctxs.iter().enumerate() {
        match visit_pipeline_stage(stage_ctx)? {
            StageOutcome::Search(stage) => search_stages.push(stage),
            StageOutcome::Metrics { function, group_by } => {
                if i + 1 != stage_ctxs.len() {
                    return Err(parse_error("metrics function must be the terminal pipeline stage"));
                }
                return Ok(PipelineExpr::Metrics {
                    source,
                    prelude: search_stages,
                    function,
                    group_by,
                });
            }
        }
    }
    Ok(PipelineExpr::Search {
        source,
        stages: search_stages,
    })
}

// ============================================================================
// TraceQLExprVisitor — the actual ANTLR visitor.
// ============================================================================

/// Main visitor for converting ANTLR parse trees into [`TraceQLExpr`].
pub struct TraceQLExprVisitor {
    temp_result: VisitorResult<TraceQLExpr>,
}

impl TraceQLExprVisitor {
    /// Create a new visitor.
    #[must_use]
    pub fn new() -> Self {
        Self {
            temp_result: VisitorResult::default(),
        }
    }
}

impl Default for TraceQLExprVisitor {
    fn default() -> Self {
        Self::new()
    }
}

impl ParseTreeVisitorCompat<'_> for TraceQLExprVisitor {
    type Node = TraceQLParserContextType;
    type Return = VisitorResult<TraceQLExpr>;

    fn temp_result(&mut self) -> &mut Self::Return {
        &mut self.temp_result
    }
}

impl<'input> TraceQLParserVisitorCompat<'input> for TraceQLExprVisitor {
    fn visit_root(&mut self, ctx: &RootContext<'input>) -> Self::Return {
        // root: pipelineExpr EOF | spansetExpr EOF
        if let Some(pipeline_ctx) = ctx.pipelineExpr() {
            return match visit_pipeline_expr(&pipeline_ctx) {
                Ok(p) => VisitorResult::ok(TraceQLExpr::Pipeline(p)),
                Err(e) => VisitorResult::err(e),
            };
        }
        if let Some(spanset_ctx) = ctx.spansetExpr() {
            return match visit_spanset_expr(&spanset_ctx) {
                Ok(s) => VisitorResult::ok(TraceQLExpr::Spanset(s)),
                Err(e) => VisitorResult::err(e),
            };
        }
        VisitorResult::err(parse_error("empty query"))
    }
}
