//! Visitor implementations for converting ANTLR parse tree to `PromQL` AST.
//!
//! This module provides visitor structs that implement
//! `PromQLParserVisitorCompat` to transform the ANTLR parse tree into the typed
//! `PromQL` AST representation.

use std::rc::Rc;

use antlr4rust::tree::{ParseTree, ParseTreeVisitorCompat};
use chrono::TimeDelta;

use super::PromQLParserVisitorCompat;
#[allow(clippy::wildcard_imports)]
use super::*;
use crate::{
    error::{QueryError, Result},
    promql::{
        aggregation::{Aggregation, AggregationOp},
        binary::{
            BinaryOp, BinaryOpModifier, BinaryOperator, GroupModifier, Subquery, UnaryOp, UnaryOperator, VectorMatching,
        },
        common::{AtModifier, Grouping, MatchOp, parse_error},
        duration::parse_duration,
        expr::PromQLExpr,
        function::FunctionCall,
        selector::{InstantSelector, LabelMatcher, MatrixSelector},
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

/// Parse a number string into f64.
fn parse_number(text: &str) -> Result<f64> {
    let text = text.trim();

    // Handle special float values
    match text.to_lowercase().as_str() {
        "inf" | "+inf" => return Ok(f64::INFINITY),
        "-inf" => return Ok(f64::NEG_INFINITY),
        "nan" => return Ok(f64::NAN),
        _ => {}
    }

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

/// Get a `TimeDelta` from a duration context.
fn get_duration(ctx: &DurationContextAll) -> Result<TimeDelta> {
    let text = ctx.get_text();
    parse_duration(&text)
}

// ============================================================================
// Main Visitor
// ============================================================================

/// Visitor that converts ANTLR parse tree into `PromQLExpr` AST.
pub struct PromQLExprVisitor {
    temp_result: VisitorResult<PromQLExpr>,
}

impl PromQLExprVisitor {
    /// Creates a new visitor instance.
    pub fn new() -> Self {
        Self {
            temp_result: VisitorResult::default(),
        }
    }

    // ========================================================================
    // Private helper methods that return Result<T> directly
    // ========================================================================

    /// Dispatch an expr context to the appropriate handler.
    fn visit_expr_ctx(&self, ctx: &ExprContextAll) -> Result<PromQLExpr> {
        match ctx {
            ExprContextAll::ExprPowContext(c) => {
                let exprs = c.expr_all();
                let left = exprs.first().ok_or_else(|| parse_error("Missing left operand in pow"))?;
                let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand in pow"))?;
                self.visit_binary_op(left, right, BinaryOperator::Pow, c.binOpModifier())
            }
            ExprContextAll::ExprMulDivModContext(c) => {
                let exprs = c.expr_all();
                let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
                let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
                let op_ctx = c.mulDivModOp().ok_or_else(|| parse_error("Missing operator"))?;
                let op = if op_ctx.MUL().is_some() {
                    BinaryOperator::Mul
                } else if op_ctx.DIV().is_some() {
                    BinaryOperator::Div
                } else if op_ctx.MOD().is_some() {
                    BinaryOperator::Mod
                } else {
                    return Err(parse_error("Unknown mul/div/mod operator"));
                };
                self.visit_binary_op(left, right, op, c.binOpModifier())
            }
            ExprContextAll::ExprAddSubContext(c) => {
                let exprs = c.expr_all();
                let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
                let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
                let op_ctx = c.addSubOp().ok_or_else(|| parse_error("Missing operator"))?;
                let op = if op_ctx.ADD().is_some() {
                    BinaryOperator::Add
                } else if op_ctx.SUB().is_some() {
                    BinaryOperator::Sub
                } else {
                    return Err(parse_error("Unknown add/sub operator"));
                };
                self.visit_binary_op(left, right, op, c.binOpModifier())
            }
            ExprContextAll::ExprCompareContext(c) => {
                let exprs = c.expr_all();
                let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
                let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
                let op_ctx = c.compareOp().ok_or_else(|| parse_error("Missing compare operator"))?;
                let op = if op_ctx.EQL().is_some() {
                    BinaryOperator::Eq
                } else if op_ctx.NE().is_some() {
                    BinaryOperator::Neq
                } else if op_ctx.GT().is_some() {
                    BinaryOperator::Gt
                } else if op_ctx.LT().is_some() {
                    BinaryOperator::Lt
                } else if op_ctx.GE().is_some() {
                    BinaryOperator::Ge
                } else if op_ctx.LE().is_some() {
                    BinaryOperator::Le
                } else {
                    return Err(parse_error("Unknown compare operator"));
                };
                self.visit_binary_op(left, right, op, c.binOpModifier())
            }
            ExprContextAll::ExprAndUnlessContext(c) => {
                let exprs = c.expr_all();
                let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
                let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
                let op_ctx = c.andUnlessOp().ok_or_else(|| parse_error("Missing and/unless operator"))?;
                let op = if op_ctx.AND().is_some() {
                    BinaryOperator::And
                } else if op_ctx.UNLESS().is_some() {
                    BinaryOperator::Unless
                } else {
                    return Err(parse_error("Unknown and/unless operator"));
                };
                self.visit_binary_op(left, right, op, c.binOpModifier())
            }
            ExprContextAll::ExprOrContext(c) => {
                let exprs = c.expr_all();
                let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
                let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
                self.visit_binary_op(left, right, BinaryOperator::Or, c.binOpModifier())
            }
            ExprContextAll::ExprSubqueryContext(c) => {
                let inner_expr_ctx = c.expr().ok_or_else(|| parse_error("Missing subquery inner expression"))?;
                let inner_expr = self.visit_expr_ctx(&inner_expr_ctx)?;

                let durations = c.duration_all();
                // First duration is the range
                let range = durations
                    .first()
                    .ok_or_else(|| parse_error("Missing subquery range"))
                    .and_then(|d| get_duration(d))?;
                // Second duration (optional) is the step
                let step = durations.get(1).map(|d| get_duration(d)).transpose()?;

                let mut subquery = Subquery::new(inner_expr, range);
                if let Some(s) = step {
                    subquery = subquery.with_step(s);
                }
                if let Some(offset_ctx) = c.offsetModifier() {
                    let dur_ctx = offset_ctx.duration().ok_or_else(|| parse_error("Missing offset duration"))?;
                    subquery = subquery.with_offset(get_duration(&dur_ctx)?);
                }
                if let Some(at_ctx) = c.atModifier() {
                    subquery = subquery.with_at(visit_at_modifier(&at_ctx)?);
                }

                Ok(PromQLExpr::Subquery(subquery))
            }
            ExprContextAll::ExprUnaryContext(c) => {
                let unary_ctx = c.unaryExpr().ok_or_else(|| parse_error("Missing unary expression"))?;
                let op_ctx = unary_ctx.unaryOp().ok_or_else(|| parse_error("Missing unary operator"))?;
                let op = if op_ctx.SUB().is_some() {
                    UnaryOperator::Neg
                } else {
                    UnaryOperator::Pos
                };
                let inner_ctx = unary_ctx.expr().ok_or_else(|| parse_error("Missing unary operand"))?;
                let inner = self.visit_expr_ctx(&inner_ctx)?;
                Ok(PromQLExpr::UnaryOp(UnaryOp::new(op, inner)))
            }
            ExprContextAll::ExprVectorContext(c) => {
                let vector_ctx = c.vector().ok_or_else(|| parse_error("Missing vector in expression"))?;
                self.visit_vector_ctx(&vector_ctx)
            }
            ExprContextAll::Error(_) => Err(parse_error("Error in expression")),
        }
    }

    /// Dispatch a vector context to the appropriate handler.
    fn visit_vector_ctx(&self, ctx: &VectorContextAll) -> Result<PromQLExpr> {
        match ctx {
            VectorContextAll::VectorFunctionContext(c) => {
                let fc_ctx = c.functionCall().ok_or_else(|| parse_error("Missing function call"))?;
                let fc = self.visit_function_call_ctx(&fc_ctx)?;
                Ok(PromQLExpr::FunctionCall(fc))
            }
            VectorContextAll::VectorAggregationContext(c) => {
                let agg_ctx = c.aggregation().ok_or_else(|| parse_error("Missing aggregation"))?;
                let agg = self.visit_aggregation_ctx(&agg_ctx)?;
                Ok(PromQLExpr::Aggregation(agg))
            }
            VectorContextAll::VectorMatrixContext(c) => {
                let ms_ctx = c.matrixSelector().ok_or_else(|| parse_error("Missing matrix selector"))?;
                let ms = self.visit_matrix_selector_ctx(&ms_ctx)?;
                Ok(PromQLExpr::MatrixSelector(ms))
            }
            VectorContextAll::VectorInstantContext(c) => {
                let is_ctx = c.instantSelector().ok_or_else(|| parse_error("Missing instant selector"))?;
                let is = self.visit_instant_selector_ctx(&is_ctx)?;
                Ok(PromQLExpr::InstantSelector(is))
            }
            VectorContextAll::VectorNumberContext(c) => {
                let nl_ctx = c.numberLiteral().ok_or_else(|| parse_error("Missing number literal"))?;
                let num = nl_ctx.NUMBER().ok_or_else(|| parse_error("Missing NUMBER token"))?;
                let text = num.get_text();
                // Check for sign prefix
                let value = if nl_ctx.SUB().is_some() {
                    -parse_number(&text)?
                } else {
                    parse_number(&text)?
                };
                Ok(PromQLExpr::NumberLiteral(value))
            }
            VectorContextAll::VectorStringContext(c) => {
                let sl_ctx = c.stringLiteral().ok_or_else(|| parse_error("Missing string literal"))?;
                let s = sl_ctx.STRING().ok_or_else(|| parse_error("Missing STRING token"))?;
                Ok(PromQLExpr::StringLiteral(clean_string(&s.get_text())))
            }
            VectorContextAll::VectorParenContext(c) => {
                let paren_ctx = c.parenExpr().ok_or_else(|| parse_error("Missing paren expression"))?;
                let inner_ctx = paren_ctx.expr().ok_or_else(|| parse_error("Missing expression in parens"))?;
                let inner = self.visit_expr_ctx(&inner_ctx)?;
                Ok(PromQLExpr::Parens(Box::new(inner)))
            }
            VectorContextAll::Error(_) => Err(parse_error("Error in vector")),
        }
    }

    /// Build a binary operation expression.
    fn visit_binary_op(
        &self,
        left_ctx: &Rc<ExprContextAll<'_>>,
        right_ctx: &Rc<ExprContextAll<'_>>,
        op: BinaryOperator,
        modifier_ctx: Option<Rc<BinOpModifierContextAll<'_>>>,
    ) -> Result<PromQLExpr> {
        let lhs = self.visit_expr_ctx(left_ctx)?;
        let rhs = self.visit_expr_ctx(right_ctx)?;

        let modifier = modifier_ctx.and_then(|m| visit_bin_op_modifier(&m));

        if let Some(m) = modifier {
            Ok(PromQLExpr::BinaryOp(BinaryOp::with_modifier(op, lhs, rhs, m)))
        } else {
            Ok(PromQLExpr::BinaryOp(BinaryOp::new(op, lhs, rhs)))
        }
    }

    /// Visit a function call context.
    fn visit_function_call_ctx(&self, ctx: &FunctionCallContextAll<'_>) -> Result<FunctionCall> {
        let name_ctx = ctx.functionName().ok_or_else(|| parse_error("Missing function name"))?;
        let name = name_ctx.get_text();

        let mut args = Vec::new();
        for expr_ctx in ctx.expr_all() {
            args.push(self.visit_expr_ctx(&expr_ctx)?);
        }

        Ok(FunctionCall::new(name, args))
    }

    /// Visit an aggregation context.
    fn visit_aggregation_ctx(&self, ctx: &AggregationContextAll<'_>) -> Result<Aggregation> {
        let op_ctx = ctx.aggregationOp().ok_or_else(|| parse_error("Missing aggregation operator"))?;
        let op_text = op_ctx.get_text().to_lowercase();
        let op = match op_text.as_str() {
            "sum" => AggregationOp::Sum,
            "min" => AggregationOp::Min,
            "max" => AggregationOp::Max,
            "avg" => AggregationOp::Avg,
            "group" => AggregationOp::Group,
            "stddev" => AggregationOp::Stddev,
            "stdvar" => AggregationOp::Stdvar,
            "count" => AggregationOp::Count,
            "count_values" => AggregationOp::CountValues,
            "bottomk" => AggregationOp::Bottomk,
            "topk" => AggregationOp::Topk,
            "quantile" => AggregationOp::Quantile,
            _ => return Err(parse_error(format!("Unknown aggregation: {op_text}"))),
        };

        // Aggregation has expr_all() which gives us the expressions inside
        // parens. For parameterized aggs like topk(5, expr), the first expr is
        // the parameter and the second is the main expression.
        let exprs = ctx.expr_all();

        let (param, expr) = if op.requires_param() {
            if exprs.len() < 2 {
                return Err(parse_error(format!(
                    "Aggregation '{op_text}' requires a parameter and an expression"
                )));
            }
            let param_expr = self.visit_expr_ctx(&exprs[0])?;
            let main_expr = self.visit_expr_ctx(&exprs[1])?;
            (Some(param_expr), main_expr)
        } else {
            let main_expr = exprs
                .first()
                .ok_or_else(|| parse_error("Missing aggregation expression"))
                .and_then(|e| self.visit_expr_ctx(e))?;
            (None, main_expr)
        };

        let mut agg = Aggregation::new(op, expr);
        if let Some(p) = param {
            agg = agg.with_param(p);
        }
        if let Some(g_ctx) = ctx.grouping() {
            agg = agg.with_grouping(visit_grouping_ctx(&g_ctx)?);
        }

        Ok(agg)
    }

    /// Visit an instant selector context.
    #[allow(clippy::unused_self)]
    fn visit_instant_selector_ctx(&self, ctx: &InstantSelectorContextAll<'_>) -> Result<InstantSelector> {
        let metric_name = ctx.IDENTIFIER().map(|id| id.get_text());

        let matchers = ctx
            .labelMatchers()
            .map(|lm| visit_label_matchers(&lm))
            .transpose()?
            .unwrap_or_default();

        let mut selector = match metric_name {
            Some(name) => {
                if matchers.is_empty() {
                    InstantSelector::new(name)
                } else {
                    InstantSelector::with_matchers(name, matchers)
                }
            }
            None => InstantSelector::from_matchers(matchers),
        };

        if let Some(offset_ctx) = ctx.offsetModifier() {
            let dur_ctx = offset_ctx.duration().ok_or_else(|| parse_error("Missing offset duration"))?;
            selector = selector.with_offset(get_duration(&dur_ctx)?);
        }

        if let Some(at_ctx) = ctx.atModifier() {
            selector = selector.with_at(visit_at_modifier(&at_ctx)?);
        }

        Ok(selector)
    }

    /// Visit a matrix selector context.
    #[allow(clippy::unused_self)]
    fn visit_matrix_selector_ctx(&self, ctx: &MatrixSelectorContextAll<'_>) -> Result<MatrixSelector> {
        let metric_name = ctx.IDENTIFIER().map(|id| id.get_text());

        let matchers = ctx
            .labelMatchers()
            .map(|lm| visit_label_matchers(&lm))
            .transpose()?
            .unwrap_or_default();

        let mut selector = match metric_name {
            Some(name) => {
                if matchers.is_empty() {
                    InstantSelector::new(name)
                } else {
                    InstantSelector::with_matchers(name, matchers)
                }
            }
            None => InstantSelector::from_matchers(matchers),
        };

        if let Some(offset_ctx) = ctx.offsetModifier() {
            let dur_ctx = offset_ctx.duration().ok_or_else(|| parse_error("Missing offset duration"))?;
            selector = selector.with_offset(get_duration(&dur_ctx)?);
        }

        if let Some(at_ctx) = ctx.atModifier() {
            selector = selector.with_at(visit_at_modifier(&at_ctx)?);
        }

        let time_range = ctx
            .timeRange()
            .ok_or_else(|| parse_error("Missing time range in matrix selector"))?;
        let range_dur = time_range
            .duration()
            .ok_or_else(|| parse_error("Missing duration in time range"))?;
        let range = get_duration(&range_dur)?;

        Ok(MatrixSelector::new(selector, range))
    }
}

// ============================================================================
// Standalone helper functions
// ============================================================================

/// Visit a `binOpModifier` context and return an optional `BinaryOpModifier`.
///
/// Returns `None` if the modifier context contains neither `bool` nor
/// `on`/`ignoring` (which can happen when the grammar allows an empty
/// modifier).
fn visit_bin_op_modifier(ctx: &BinOpModifierContextAll<'_>) -> Option<BinaryOpModifier> {
    let return_bool = ctx.BOOL().is_some();

    let matching = ctx.onOrIgnoring().map(|oi_ctx| {
        let labels = oi_ctx.labelNameList().map(|ll| visit_label_name_list(&ll)).unwrap_or_default();

        let (on_labels, ignoring_labels) = if oi_ctx.ON().is_some() {
            (Some(labels), None)
        } else {
            (None, Some(labels))
        };

        let group = ctx.groupModifier().map(|gm_ctx| {
            let group_labels = gm_ctx.labelNameList().map(|ll| visit_label_name_list(&ll)).unwrap_or_default();
            if gm_ctx.GROUP_LEFT().is_some() {
                GroupModifier::Left(group_labels)
            } else {
                GroupModifier::Right(group_labels)
            }
        });

        VectorMatching {
            on_labels,
            ignoring_labels,
            group,
        }
    });

    if !return_bool && matching.is_none() {
        return None;
    }

    Some(BinaryOpModifier { return_bool, matching })
}

/// Visit a grouping context (`by (labels)` or `without (labels)`).
fn visit_grouping_ctx(ctx: &GroupingContextAll<'_>) -> Result<Grouping> {
    let labels = ctx.labelNameList().map(|ll| visit_label_name_list(&ll)).unwrap_or_default();

    if ctx.BY().is_some() {
        Ok(Grouping::By(labels))
    } else if ctx.WITHOUT().is_some() {
        Ok(Grouping::Without(labels))
    } else {
        Err(parse_error("Grouping must be 'by' or 'without'"))
    }
}

/// Visit a label name list and collect label names.
fn visit_label_name_list(ctx: &LabelNameListContextAll<'_>) -> Vec<String> {
    ctx.labelName_all().iter().map(|ln| ln.get_text()).collect()
}

/// Visit label matchers and collect all label matchers.
fn visit_label_matchers(ctx: &LabelMatchersContextAll<'_>) -> Result<Vec<LabelMatcher>> {
    let mut matchers = Vec::new();
    for lm_ctx in ctx.labelMatcher_all() {
        matchers.push(visit_label_matcher(&lm_ctx)?);
    }
    Ok(matchers)
}

/// Visit a single label matcher context.
fn visit_label_matcher(ctx: &LabelMatcherContextAll<'_>) -> Result<LabelMatcher> {
    let name_ctx = ctx.labelName().ok_or_else(|| parse_error("Missing label name in matcher"))?;
    let name = name_ctx.get_text();

    let op_ctx = ctx.labelMatcherOp().ok_or_else(|| parse_error("Missing match operator"))?;
    let op = if op_ctx.EQ().is_some() {
        MatchOp::Eq
    } else if op_ctx.NE().is_some() {
        MatchOp::Neq
    } else if op_ctx.RE().is_some() {
        MatchOp::Re
    } else if op_ctx.NRE().is_some() {
        MatchOp::Nre
    } else {
        return Err(parse_error("Unknown label match operator"));
    };

    let value = ctx
        .STRING()
        .ok_or_else(|| parse_error("Missing value in label matcher"))?
        .get_text();

    Ok(LabelMatcher::new(name, op, clean_string(&value)))
}

/// Visit an `@` modifier context.
fn visit_at_modifier(ctx: &AtModifierContextAll<'_>) -> Result<AtModifier> {
    if ctx.START().is_some() {
        Ok(AtModifier::Start)
    } else if ctx.END().is_some() {
        Ok(AtModifier::End)
    } else if let Some(num) = ctx.NUMBER() {
        let text = num.get_text();
        let value = parse_number(&text)?;
        let value = if ctx.SUB().is_some() { -value } else { value };
        Ok(AtModifier::Timestamp(value))
    } else {
        Err(parse_error("Invalid @ modifier"))
    }
}

// ============================================================================
// PromQLParserVisitorCompat implementation
// ============================================================================

impl ParseTreeVisitorCompat<'_> for PromQLExprVisitor {
    type Node = PromQLParserContextType;
    type Return = VisitorResult<PromQLExpr>;

    fn temp_result(&mut self) -> &mut Self::Return {
        &mut self.temp_result
    }
}

impl<'input> PromQLParserVisitorCompat<'input> for PromQLExprVisitor {
    fn visit_root(&mut self, ctx: &RootContext<'input>) -> Self::Return {
        let Some(expr_ctx) = ctx.expr() else {
            return VisitorResult::err(parse_error("Empty query"));
        };
        match self.visit_expr_ctx(&expr_ctx) {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_exprPow(&mut self, ctx: &ExprPowContext<'input>) -> Self::Return {
        let exprs = ctx.expr_all();
        let result = (|| {
            let left = exprs.first().ok_or_else(|| parse_error("Missing left operand in pow"))?;
            let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand in pow"))?;
            self.visit_binary_op(left, right, BinaryOperator::Pow, ctx.binOpModifier())
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_exprMulDivMod(&mut self, ctx: &ExprMulDivModContext<'input>) -> Self::Return {
        let result = (|| {
            let exprs = ctx.expr_all();
            let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
            let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
            let op_ctx = ctx.mulDivModOp().ok_or_else(|| parse_error("Missing operator"))?;
            let op = if op_ctx.MUL().is_some() {
                BinaryOperator::Mul
            } else if op_ctx.DIV().is_some() {
                BinaryOperator::Div
            } else if op_ctx.MOD().is_some() {
                BinaryOperator::Mod
            } else {
                return Err(parse_error("Unknown mul/div/mod operator"));
            };
            self.visit_binary_op(left, right, op, ctx.binOpModifier())
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_exprAddSub(&mut self, ctx: &ExprAddSubContext<'input>) -> Self::Return {
        let result = (|| {
            let exprs = ctx.expr_all();
            let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
            let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
            let op_ctx = ctx.addSubOp().ok_or_else(|| parse_error("Missing operator"))?;
            let op = if op_ctx.ADD().is_some() {
                BinaryOperator::Add
            } else if op_ctx.SUB().is_some() {
                BinaryOperator::Sub
            } else {
                return Err(parse_error("Unknown add/sub operator"));
            };
            self.visit_binary_op(left, right, op, ctx.binOpModifier())
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_exprCompare(&mut self, ctx: &ExprCompareContext<'input>) -> Self::Return {
        let result = (|| {
            let exprs = ctx.expr_all();
            let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
            let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
            let op_ctx = ctx.compareOp().ok_or_else(|| parse_error("Missing compare operator"))?;
            let op = if op_ctx.EQL().is_some() {
                BinaryOperator::Eq
            } else if op_ctx.NE().is_some() {
                BinaryOperator::Neq
            } else if op_ctx.GT().is_some() {
                BinaryOperator::Gt
            } else if op_ctx.LT().is_some() {
                BinaryOperator::Lt
            } else if op_ctx.GE().is_some() {
                BinaryOperator::Ge
            } else if op_ctx.LE().is_some() {
                BinaryOperator::Le
            } else {
                return Err(parse_error("Unknown compare operator"));
            };
            self.visit_binary_op(left, right, op, ctx.binOpModifier())
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_exprAndUnless(&mut self, ctx: &ExprAndUnlessContext<'input>) -> Self::Return {
        let result = (|| {
            let exprs = ctx.expr_all();
            let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
            let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
            let op_ctx = ctx.andUnlessOp().ok_or_else(|| parse_error("Missing and/unless operator"))?;
            let op = if op_ctx.AND().is_some() {
                BinaryOperator::And
            } else if op_ctx.UNLESS().is_some() {
                BinaryOperator::Unless
            } else {
                return Err(parse_error("Unknown and/unless operator"));
            };
            self.visit_binary_op(left, right, op, ctx.binOpModifier())
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_exprOr(&mut self, ctx: &ExprOrContext<'input>) -> Self::Return {
        let result = (|| {
            let exprs = ctx.expr_all();
            let left = exprs.first().ok_or_else(|| parse_error("Missing left operand"))?;
            let right = exprs.get(1).ok_or_else(|| parse_error("Missing right operand"))?;
            self.visit_binary_op(left, right, BinaryOperator::Or, ctx.binOpModifier())
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_exprSubquery(&mut self, ctx: &ExprSubqueryContext<'input>) -> Self::Return {
        let result = (|| {
            let inner_expr_ctx = ctx.expr().ok_or_else(|| parse_error("Missing subquery inner expression"))?;
            let inner_expr = self.visit_expr_ctx(&inner_expr_ctx)?;

            let durations = ctx.duration_all();
            let range = durations
                .first()
                .ok_or_else(|| parse_error("Missing subquery range"))
                .and_then(|d| get_duration(d))?;
            let step = durations.get(1).map(|d| get_duration(d)).transpose()?;

            let mut subquery = Subquery::new(inner_expr, range);
            if let Some(s) = step {
                subquery = subquery.with_step(s);
            }
            if let Some(offset_ctx) = ctx.offsetModifier() {
                let dur_ctx = offset_ctx.duration().ok_or_else(|| parse_error("Missing offset duration"))?;
                subquery = subquery.with_offset(get_duration(&dur_ctx)?);
            }
            if let Some(at_ctx) = ctx.atModifier() {
                subquery = subquery.with_at(visit_at_modifier(&at_ctx)?);
            }

            Ok(PromQLExpr::Subquery(subquery))
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_exprUnary(&mut self, ctx: &ExprUnaryContext<'input>) -> Self::Return {
        let result = (|| {
            let unary_ctx = ctx.unaryExpr().ok_or_else(|| parse_error("Missing unary expression"))?;
            let op_ctx = unary_ctx.unaryOp().ok_or_else(|| parse_error("Missing unary operator"))?;
            let op = if op_ctx.SUB().is_some() {
                UnaryOperator::Neg
            } else {
                UnaryOperator::Pos
            };
            let inner_ctx = unary_ctx.expr().ok_or_else(|| parse_error("Missing unary operand"))?;
            let inner = self.visit_expr_ctx(&inner_ctx)?;
            Ok(PromQLExpr::UnaryOp(UnaryOp::new(op, inner)))
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_exprVector(&mut self, ctx: &ExprVectorContext<'input>) -> Self::Return {
        let result = (|| {
            let vector_ctx = ctx.vector().ok_or_else(|| parse_error("Missing vector in expression"))?;
            self.visit_vector_ctx(&vector_ctx)
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_vectorFunction(&mut self, ctx: &VectorFunctionContext<'input>) -> Self::Return {
        let result = (|| {
            let fc_ctx = ctx.functionCall().ok_or_else(|| parse_error("Missing function call"))?;
            let fc = self.visit_function_call_ctx(&fc_ctx)?;
            Ok(PromQLExpr::FunctionCall(fc))
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_vectorAggregation(&mut self, ctx: &VectorAggregationContext<'input>) -> Self::Return {
        let result = (|| {
            let agg_ctx = ctx.aggregation().ok_or_else(|| parse_error("Missing aggregation"))?;
            let agg = self.visit_aggregation_ctx(&agg_ctx)?;
            Ok(PromQLExpr::Aggregation(agg))
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_vectorMatrix(&mut self, ctx: &VectorMatrixContext<'input>) -> Self::Return {
        let Some(ms_ctx) = ctx.matrixSelector() else {
            return VisitorResult::err(parse_error("Missing matrix selector"));
        };
        match self.visit_matrix_selector_ctx(&ms_ctx) {
            Ok(ms) => VisitorResult::ok(PromQLExpr::MatrixSelector(ms)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_vectorInstant(&mut self, ctx: &VectorInstantContext<'input>) -> Self::Return {
        let Some(is_ctx) = ctx.instantSelector() else {
            return VisitorResult::err(parse_error("Missing instant selector"));
        };
        match self.visit_instant_selector_ctx(&is_ctx) {
            Ok(is) => VisitorResult::ok(PromQLExpr::InstantSelector(is)),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_vectorNumber(&mut self, ctx: &VectorNumberContext<'input>) -> Self::Return {
        let result = (|| {
            let nl_ctx = ctx.numberLiteral().ok_or_else(|| parse_error("Missing number literal"))?;
            let num = nl_ctx.NUMBER().ok_or_else(|| parse_error("Missing NUMBER token"))?;
            let text = num.get_text();
            let value = if nl_ctx.SUB().is_some() {
                -parse_number(&text)?
            } else {
                parse_number(&text)?
            };
            Ok(PromQLExpr::NumberLiteral(value))
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_vectorString(&mut self, ctx: &VectorStringContext<'input>) -> Self::Return {
        let result = (|| {
            let sl_ctx = ctx.stringLiteral().ok_or_else(|| parse_error("Missing string literal"))?;
            let s = sl_ctx.STRING().ok_or_else(|| parse_error("Missing STRING token"))?;
            Ok(PromQLExpr::StringLiteral(clean_string(&s.get_text())))
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }

    fn visit_vectorParen(&mut self, ctx: &VectorParenContext<'input>) -> Self::Return {
        let result = (|| {
            let paren_ctx = ctx.parenExpr().ok_or_else(|| parse_error("Missing paren expression"))?;
            let inner_ctx = paren_ctx.expr().ok_or_else(|| parse_error("Missing expression in parens"))?;
            let inner = self.visit_expr_ctx(&inner_ctx)?;
            Ok(PromQLExpr::Parens(Box::new(inner)))
        })();
        match result {
            Ok(expr) => VisitorResult::ok(expr),
            Err(e) => VisitorResult::err(e),
        }
    }
}
