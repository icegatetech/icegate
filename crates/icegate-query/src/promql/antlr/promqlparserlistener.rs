#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#![allow(nonstandard_style)]
// Generated from antlr/PromQLParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::ParseTreeListener;
use super::promqlparser::*;

pub trait PromQLParserListener<'input> : ParseTreeListener<'input,PromQLParserContextType>{
/**
 * Enter a parse tree produced by {@link PromQLParser#root}.
 * @param ctx the parse tree
 */
fn enter_root(&mut self, _ctx: &RootContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#root}.
 * @param ctx the parse tree
 */
fn exit_root(&mut self, _ctx: &RootContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exprMulDivMod}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_exprMulDivMod(&mut self, _ctx: &ExprMulDivModContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exprMulDivMod}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_exprMulDivMod(&mut self, _ctx: &ExprMulDivModContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exprOr}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_exprOr(&mut self, _ctx: &ExprOrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exprOr}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_exprOr(&mut self, _ctx: &ExprOrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exprAndUnless}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_exprAndUnless(&mut self, _ctx: &ExprAndUnlessContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exprAndUnless}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_exprAndUnless(&mut self, _ctx: &ExprAndUnlessContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exprSubquery}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_exprSubquery(&mut self, _ctx: &ExprSubqueryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exprSubquery}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_exprSubquery(&mut self, _ctx: &ExprSubqueryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exprAddSub}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_exprAddSub(&mut self, _ctx: &ExprAddSubContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exprAddSub}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_exprAddSub(&mut self, _ctx: &ExprAddSubContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exprPow}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_exprPow(&mut self, _ctx: &ExprPowContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exprPow}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_exprPow(&mut self, _ctx: &ExprPowContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exprUnary}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_exprUnary(&mut self, _ctx: &ExprUnaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exprUnary}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_exprUnary(&mut self, _ctx: &ExprUnaryContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exprCompare}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_exprCompare(&mut self, _ctx: &ExprCompareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exprCompare}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_exprCompare(&mut self, _ctx: &ExprCompareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code exprVector}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_exprVector(&mut self, _ctx: &ExprVectorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code exprVector}
 * labeled alternative in {@link PromQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_exprVector(&mut self, _ctx: &ExprVectorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code vectorFunction}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn enter_vectorFunction(&mut self, _ctx: &VectorFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code vectorFunction}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn exit_vectorFunction(&mut self, _ctx: &VectorFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code vectorAggregation}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn enter_vectorAggregation(&mut self, _ctx: &VectorAggregationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code vectorAggregation}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn exit_vectorAggregation(&mut self, _ctx: &VectorAggregationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code vectorMatrix}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn enter_vectorMatrix(&mut self, _ctx: &VectorMatrixContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code vectorMatrix}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn exit_vectorMatrix(&mut self, _ctx: &VectorMatrixContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code vectorInstant}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn enter_vectorInstant(&mut self, _ctx: &VectorInstantContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code vectorInstant}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn exit_vectorInstant(&mut self, _ctx: &VectorInstantContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code vectorNumber}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn enter_vectorNumber(&mut self, _ctx: &VectorNumberContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code vectorNumber}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn exit_vectorNumber(&mut self, _ctx: &VectorNumberContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code vectorString}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn enter_vectorString(&mut self, _ctx: &VectorStringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code vectorString}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn exit_vectorString(&mut self, _ctx: &VectorStringContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code vectorParen}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn enter_vectorParen(&mut self, _ctx: &VectorParenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code vectorParen}
 * labeled alternative in {@link PromQLParser#vector}.
 * @param ctx the parse tree
 */
fn exit_vectorParen(&mut self, _ctx: &VectorParenContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#parenExpr}.
 * @param ctx the parse tree
 */
fn enter_parenExpr(&mut self, _ctx: &ParenExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#parenExpr}.
 * @param ctx the parse tree
 */
fn exit_parenExpr(&mut self, _ctx: &ParenExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#unaryExpr}.
 * @param ctx the parse tree
 */
fn enter_unaryExpr(&mut self, _ctx: &UnaryExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#unaryExpr}.
 * @param ctx the parse tree
 */
fn exit_unaryExpr(&mut self, _ctx: &UnaryExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#unaryOp}.
 * @param ctx the parse tree
 */
fn enter_unaryOp(&mut self, _ctx: &UnaryOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#unaryOp}.
 * @param ctx the parse tree
 */
fn exit_unaryOp(&mut self, _ctx: &UnaryOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#mulDivModOp}.
 * @param ctx the parse tree
 */
fn enter_mulDivModOp(&mut self, _ctx: &MulDivModOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#mulDivModOp}.
 * @param ctx the parse tree
 */
fn exit_mulDivModOp(&mut self, _ctx: &MulDivModOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#addSubOp}.
 * @param ctx the parse tree
 */
fn enter_addSubOp(&mut self, _ctx: &AddSubOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#addSubOp}.
 * @param ctx the parse tree
 */
fn exit_addSubOp(&mut self, _ctx: &AddSubOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#compareOp}.
 * @param ctx the parse tree
 */
fn enter_compareOp(&mut self, _ctx: &CompareOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#compareOp}.
 * @param ctx the parse tree
 */
fn exit_compareOp(&mut self, _ctx: &CompareOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#andUnlessOp}.
 * @param ctx the parse tree
 */
fn enter_andUnlessOp(&mut self, _ctx: &AndUnlessOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#andUnlessOp}.
 * @param ctx the parse tree
 */
fn exit_andUnlessOp(&mut self, _ctx: &AndUnlessOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#binOpModifier}.
 * @param ctx the parse tree
 */
fn enter_binOpModifier(&mut self, _ctx: &BinOpModifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#binOpModifier}.
 * @param ctx the parse tree
 */
fn exit_binOpModifier(&mut self, _ctx: &BinOpModifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#onOrIgnoring}.
 * @param ctx the parse tree
 */
fn enter_onOrIgnoring(&mut self, _ctx: &OnOrIgnoringContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#onOrIgnoring}.
 * @param ctx the parse tree
 */
fn exit_onOrIgnoring(&mut self, _ctx: &OnOrIgnoringContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#groupModifier}.
 * @param ctx the parse tree
 */
fn enter_groupModifier(&mut self, _ctx: &GroupModifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#groupModifier}.
 * @param ctx the parse tree
 */
fn exit_groupModifier(&mut self, _ctx: &GroupModifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#instantSelector}.
 * @param ctx the parse tree
 */
fn enter_instantSelector(&mut self, _ctx: &InstantSelectorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#instantSelector}.
 * @param ctx the parse tree
 */
fn exit_instantSelector(&mut self, _ctx: &InstantSelectorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#matrixSelector}.
 * @param ctx the parse tree
 */
fn enter_matrixSelector(&mut self, _ctx: &MatrixSelectorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#matrixSelector}.
 * @param ctx the parse tree
 */
fn exit_matrixSelector(&mut self, _ctx: &MatrixSelectorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#labelMatchers}.
 * @param ctx the parse tree
 */
fn enter_labelMatchers(&mut self, _ctx: &LabelMatchersContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#labelMatchers}.
 * @param ctx the parse tree
 */
fn exit_labelMatchers(&mut self, _ctx: &LabelMatchersContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#labelMatcher}.
 * @param ctx the parse tree
 */
fn enter_labelMatcher(&mut self, _ctx: &LabelMatcherContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#labelMatcher}.
 * @param ctx the parse tree
 */
fn exit_labelMatcher(&mut self, _ctx: &LabelMatcherContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#labelMatcherOp}.
 * @param ctx the parse tree
 */
fn enter_labelMatcherOp(&mut self, _ctx: &LabelMatcherOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#labelMatcherOp}.
 * @param ctx the parse tree
 */
fn exit_labelMatcherOp(&mut self, _ctx: &LabelMatcherOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#functionCall}.
 * @param ctx the parse tree
 */
fn enter_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#functionCall}.
 * @param ctx the parse tree
 */
fn exit_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#functionName}.
 * @param ctx the parse tree
 */
fn enter_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#functionName}.
 * @param ctx the parse tree
 */
fn exit_functionName(&mut self, _ctx: &FunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#rangeFunctionName}.
 * @param ctx the parse tree
 */
fn enter_rangeFunctionName(&mut self, _ctx: &RangeFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#rangeFunctionName}.
 * @param ctx the parse tree
 */
fn exit_rangeFunctionName(&mut self, _ctx: &RangeFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#mathFunctionName}.
 * @param ctx the parse tree
 */
fn enter_mathFunctionName(&mut self, _ctx: &MathFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#mathFunctionName}.
 * @param ctx the parse tree
 */
fn exit_mathFunctionName(&mut self, _ctx: &MathFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#trigFunctionName}.
 * @param ctx the parse tree
 */
fn enter_trigFunctionName(&mut self, _ctx: &TrigFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#trigFunctionName}.
 * @param ctx the parse tree
 */
fn exit_trigFunctionName(&mut self, _ctx: &TrigFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#labelFunctionName}.
 * @param ctx the parse tree
 */
fn enter_labelFunctionName(&mut self, _ctx: &LabelFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#labelFunctionName}.
 * @param ctx the parse tree
 */
fn exit_labelFunctionName(&mut self, _ctx: &LabelFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#timeFunctionName}.
 * @param ctx the parse tree
 */
fn enter_timeFunctionName(&mut self, _ctx: &TimeFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#timeFunctionName}.
 * @param ctx the parse tree
 */
fn exit_timeFunctionName(&mut self, _ctx: &TimeFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#histogramFunctionName}.
 * @param ctx the parse tree
 */
fn enter_histogramFunctionName(&mut self, _ctx: &HistogramFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#histogramFunctionName}.
 * @param ctx the parse tree
 */
fn exit_histogramFunctionName(&mut self, _ctx: &HistogramFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#aggregation}.
 * @param ctx the parse tree
 */
fn enter_aggregation(&mut self, _ctx: &AggregationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#aggregation}.
 * @param ctx the parse tree
 */
fn exit_aggregation(&mut self, _ctx: &AggregationContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#aggregationOp}.
 * @param ctx the parse tree
 */
fn enter_aggregationOp(&mut self, _ctx: &AggregationOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#aggregationOp}.
 * @param ctx the parse tree
 */
fn exit_aggregationOp(&mut self, _ctx: &AggregationOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#grouping}.
 * @param ctx the parse tree
 */
fn enter_grouping(&mut self, _ctx: &GroupingContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#grouping}.
 * @param ctx the parse tree
 */
fn exit_grouping(&mut self, _ctx: &GroupingContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#timeRange}.
 * @param ctx the parse tree
 */
fn enter_timeRange(&mut self, _ctx: &TimeRangeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#timeRange}.
 * @param ctx the parse tree
 */
fn exit_timeRange(&mut self, _ctx: &TimeRangeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#offsetModifier}.
 * @param ctx the parse tree
 */
fn enter_offsetModifier(&mut self, _ctx: &OffsetModifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#offsetModifier}.
 * @param ctx the parse tree
 */
fn exit_offsetModifier(&mut self, _ctx: &OffsetModifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#atModifier}.
 * @param ctx the parse tree
 */
fn enter_atModifier(&mut self, _ctx: &AtModifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#atModifier}.
 * @param ctx the parse tree
 */
fn exit_atModifier(&mut self, _ctx: &AtModifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#labelName}.
 * @param ctx the parse tree
 */
fn enter_labelName(&mut self, _ctx: &LabelNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#labelName}.
 * @param ctx the parse tree
 */
fn exit_labelName(&mut self, _ctx: &LabelNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#labelNameList}.
 * @param ctx the parse tree
 */
fn enter_labelNameList(&mut self, _ctx: &LabelNameListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#labelNameList}.
 * @param ctx the parse tree
 */
fn exit_labelNameList(&mut self, _ctx: &LabelNameListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#keyword}.
 * @param ctx the parse tree
 */
fn enter_keyword(&mut self, _ctx: &KeywordContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#keyword}.
 * @param ctx the parse tree
 */
fn exit_keyword(&mut self, _ctx: &KeywordContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#numberLiteral}.
 * @param ctx the parse tree
 */
fn enter_numberLiteral(&mut self, _ctx: &NumberLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#numberLiteral}.
 * @param ctx the parse tree
 */
fn exit_numberLiteral(&mut self, _ctx: &NumberLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#stringLiteral}.
 * @param ctx the parse tree
 */
fn enter_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#stringLiteral}.
 * @param ctx the parse tree
 */
fn exit_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link PromQLParser#duration}.
 * @param ctx the parse tree
 */
fn enter_duration(&mut self, _ctx: &DurationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link PromQLParser#duration}.
 * @param ctx the parse tree
 */
fn exit_duration(&mut self, _ctx: &DurationContext<'input>) { }

}

antlr4rust::coerce_from!{ 'input : PromQLParserListener<'input> }


