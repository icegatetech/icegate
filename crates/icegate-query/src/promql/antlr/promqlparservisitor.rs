#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#![allow(nonstandard_style)]
// Generated from antlr/PromQLParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::{ParseTreeVisitor,ParseTreeVisitorCompat};
use super::promqlparser::*;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PromQLParser}.
 */
pub trait PromQLParserVisitor<'input>: ParseTreeVisitor<'input,PromQLParserContextType>{
	/**
	 * Visit a parse tree produced by {@link PromQLParser#root}.
	 * @param ctx the parse tree
	 */
	fn visit_root(&mut self, ctx: &RootContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exprMulDivMod}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_exprMulDivMod(&mut self, ctx: &ExprMulDivModContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exprOr}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_exprOr(&mut self, ctx: &ExprOrContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exprAndUnless}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_exprAndUnless(&mut self, ctx: &ExprAndUnlessContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exprSubquery}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_exprSubquery(&mut self, ctx: &ExprSubqueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exprAddSub}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_exprAddSub(&mut self, ctx: &ExprAddSubContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exprPow}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_exprPow(&mut self, ctx: &ExprPowContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exprUnary}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_exprUnary(&mut self, ctx: &ExprUnaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exprCompare}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_exprCompare(&mut self, ctx: &ExprCompareContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code exprVector}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_exprVector(&mut self, ctx: &ExprVectorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code vectorFunction}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorFunction(&mut self, ctx: &VectorFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code vectorAggregation}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorAggregation(&mut self, ctx: &VectorAggregationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code vectorMatrix}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorMatrix(&mut self, ctx: &VectorMatrixContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code vectorInstant}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorInstant(&mut self, ctx: &VectorInstantContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code vectorNumber}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorNumber(&mut self, ctx: &VectorNumberContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code vectorString}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorString(&mut self, ctx: &VectorStringContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code vectorParen}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorParen(&mut self, ctx: &VectorParenContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#parenExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_parenExpr(&mut self, ctx: &ParenExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#unaryExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_unaryExpr(&mut self, ctx: &UnaryExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#unaryOp}.
	 * @param ctx the parse tree
	 */
	fn visit_unaryOp(&mut self, ctx: &UnaryOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#mulDivModOp}.
	 * @param ctx the parse tree
	 */
	fn visit_mulDivModOp(&mut self, ctx: &MulDivModOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#addSubOp}.
	 * @param ctx the parse tree
	 */
	fn visit_addSubOp(&mut self, ctx: &AddSubOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#compareOp}.
	 * @param ctx the parse tree
	 */
	fn visit_compareOp(&mut self, ctx: &CompareOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#andUnlessOp}.
	 * @param ctx the parse tree
	 */
	fn visit_andUnlessOp(&mut self, ctx: &AndUnlessOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#binOpModifier}.
	 * @param ctx the parse tree
	 */
	fn visit_binOpModifier(&mut self, ctx: &BinOpModifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#onOrIgnoring}.
	 * @param ctx the parse tree
	 */
	fn visit_onOrIgnoring(&mut self, ctx: &OnOrIgnoringContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#groupModifier}.
	 * @param ctx the parse tree
	 */
	fn visit_groupModifier(&mut self, ctx: &GroupModifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#instantSelector}.
	 * @param ctx the parse tree
	 */
	fn visit_instantSelector(&mut self, ctx: &InstantSelectorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#matrixSelector}.
	 * @param ctx the parse tree
	 */
	fn visit_matrixSelector(&mut self, ctx: &MatrixSelectorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelMatchers}.
	 * @param ctx the parse tree
	 */
	fn visit_labelMatchers(&mut self, ctx: &LabelMatchersContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelMatcher}.
	 * @param ctx the parse tree
	 */
	fn visit_labelMatcher(&mut self, ctx: &LabelMatcherContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelMatcherOp}.
	 * @param ctx the parse tree
	 */
	fn visit_labelMatcherOp(&mut self, ctx: &LabelMatcherOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#functionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#functionName}.
	 * @param ctx the parse tree
	 */
	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#rangeFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeFunctionName(&mut self, ctx: &RangeFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#mathFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_mathFunctionName(&mut self, ctx: &MathFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#trigFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_trigFunctionName(&mut self, ctx: &TrigFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFunctionName(&mut self, ctx: &LabelFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#timeFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_timeFunctionName(&mut self, ctx: &TimeFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#histogramFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_histogramFunctionName(&mut self, ctx: &HistogramFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#aggregation}.
	 * @param ctx the parse tree
	 */
	fn visit_aggregation(&mut self, ctx: &AggregationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#aggregationOp}.
	 * @param ctx the parse tree
	 */
	fn visit_aggregationOp(&mut self, ctx: &AggregationOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#grouping}.
	 * @param ctx the parse tree
	 */
	fn visit_grouping(&mut self, ctx: &GroupingContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#timeRange}.
	 * @param ctx the parse tree
	 */
	fn visit_timeRange(&mut self, ctx: &TimeRangeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#offsetModifier}.
	 * @param ctx the parse tree
	 */
	fn visit_offsetModifier(&mut self, ctx: &OffsetModifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#atModifier}.
	 * @param ctx the parse tree
	 */
	fn visit_atModifier(&mut self, ctx: &AtModifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelName}.
	 * @param ctx the parse tree
	 */
	fn visit_labelName(&mut self, ctx: &LabelNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelNameList}.
	 * @param ctx the parse tree
	 */
	fn visit_labelNameList(&mut self, ctx: &LabelNameListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#keyword}.
	 * @param ctx the parse tree
	 */
	fn visit_keyword(&mut self, ctx: &KeywordContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#numberLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_numberLiteral(&mut self, ctx: &NumberLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link PromQLParser#duration}.
	 * @param ctx the parse tree
	 */
	fn visit_duration(&mut self, ctx: &DurationContext<'input>) { self.visit_children(ctx) }

}

pub trait PromQLParserVisitorCompat<'input>:ParseTreeVisitorCompat<'input, Node= PromQLParserContextType>{
	/**
	 * Visit a parse tree produced by {@link PromQLParser#root}.
	 * @param ctx the parse tree
	 */
		fn visit_root(&mut self, ctx: &RootContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exprMulDivMod}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_exprMulDivMod(&mut self, ctx: &ExprMulDivModContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exprOr}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_exprOr(&mut self, ctx: &ExprOrContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exprAndUnless}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_exprAndUnless(&mut self, ctx: &ExprAndUnlessContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exprSubquery}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_exprSubquery(&mut self, ctx: &ExprSubqueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exprAddSub}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_exprAddSub(&mut self, ctx: &ExprAddSubContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exprPow}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_exprPow(&mut self, ctx: &ExprPowContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exprUnary}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_exprUnary(&mut self, ctx: &ExprUnaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exprCompare}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_exprCompare(&mut self, ctx: &ExprCompareContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code exprVector}
	 * labeled alternative in {@link PromQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_exprVector(&mut self, ctx: &ExprVectorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code vectorFunction}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorFunction(&mut self, ctx: &VectorFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code vectorAggregation}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorAggregation(&mut self, ctx: &VectorAggregationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code vectorMatrix}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorMatrix(&mut self, ctx: &VectorMatrixContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code vectorInstant}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorInstant(&mut self, ctx: &VectorInstantContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code vectorNumber}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorNumber(&mut self, ctx: &VectorNumberContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code vectorString}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorString(&mut self, ctx: &VectorStringContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code vectorParen}
	 * labeled alternative in {@link PromQLParser#vector}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorParen(&mut self, ctx: &VectorParenContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#parenExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_parenExpr(&mut self, ctx: &ParenExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#unaryExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_unaryExpr(&mut self, ctx: &UnaryExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#unaryOp}.
	 * @param ctx the parse tree
	 */
		fn visit_unaryOp(&mut self, ctx: &UnaryOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#mulDivModOp}.
	 * @param ctx the parse tree
	 */
		fn visit_mulDivModOp(&mut self, ctx: &MulDivModOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#addSubOp}.
	 * @param ctx the parse tree
	 */
		fn visit_addSubOp(&mut self, ctx: &AddSubOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#compareOp}.
	 * @param ctx the parse tree
	 */
		fn visit_compareOp(&mut self, ctx: &CompareOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#andUnlessOp}.
	 * @param ctx the parse tree
	 */
		fn visit_andUnlessOp(&mut self, ctx: &AndUnlessOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#binOpModifier}.
	 * @param ctx the parse tree
	 */
		fn visit_binOpModifier(&mut self, ctx: &BinOpModifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#onOrIgnoring}.
	 * @param ctx the parse tree
	 */
		fn visit_onOrIgnoring(&mut self, ctx: &OnOrIgnoringContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#groupModifier}.
	 * @param ctx the parse tree
	 */
		fn visit_groupModifier(&mut self, ctx: &GroupModifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#instantSelector}.
	 * @param ctx the parse tree
	 */
		fn visit_instantSelector(&mut self, ctx: &InstantSelectorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#matrixSelector}.
	 * @param ctx the parse tree
	 */
		fn visit_matrixSelector(&mut self, ctx: &MatrixSelectorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelMatchers}.
	 * @param ctx the parse tree
	 */
		fn visit_labelMatchers(&mut self, ctx: &LabelMatchersContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelMatcher}.
	 * @param ctx the parse tree
	 */
		fn visit_labelMatcher(&mut self, ctx: &LabelMatcherContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelMatcherOp}.
	 * @param ctx the parse tree
	 */
		fn visit_labelMatcherOp(&mut self, ctx: &LabelMatcherOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#functionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#functionName}.
	 * @param ctx the parse tree
	 */
		fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#rangeFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeFunctionName(&mut self, ctx: &RangeFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#mathFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_mathFunctionName(&mut self, ctx: &MathFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#trigFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_trigFunctionName(&mut self, ctx: &TrigFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFunctionName(&mut self, ctx: &LabelFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#timeFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_timeFunctionName(&mut self, ctx: &TimeFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#histogramFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_histogramFunctionName(&mut self, ctx: &HistogramFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#aggregation}.
	 * @param ctx the parse tree
	 */
		fn visit_aggregation(&mut self, ctx: &AggregationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#aggregationOp}.
	 * @param ctx the parse tree
	 */
		fn visit_aggregationOp(&mut self, ctx: &AggregationOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#grouping}.
	 * @param ctx the parse tree
	 */
		fn visit_grouping(&mut self, ctx: &GroupingContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#timeRange}.
	 * @param ctx the parse tree
	 */
		fn visit_timeRange(&mut self, ctx: &TimeRangeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#offsetModifier}.
	 * @param ctx the parse tree
	 */
		fn visit_offsetModifier(&mut self, ctx: &OffsetModifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#atModifier}.
	 * @param ctx the parse tree
	 */
		fn visit_atModifier(&mut self, ctx: &AtModifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelName}.
	 * @param ctx the parse tree
	 */
		fn visit_labelName(&mut self, ctx: &LabelNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#labelNameList}.
	 * @param ctx the parse tree
	 */
		fn visit_labelNameList(&mut self, ctx: &LabelNameListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#keyword}.
	 * @param ctx the parse tree
	 */
		fn visit_keyword(&mut self, ctx: &KeywordContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#numberLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_numberLiteral(&mut self, ctx: &NumberLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link PromQLParser#duration}.
	 * @param ctx the parse tree
	 */
		fn visit_duration(&mut self, ctx: &DurationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

}

impl<'input,T> PromQLParserVisitor<'input> for T
where
	T: PromQLParserVisitorCompat<'input>
{
	fn visit_root(&mut self, ctx: &RootContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_root(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exprMulDivMod(&mut self, ctx: &ExprMulDivModContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_exprMulDivMod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exprOr(&mut self, ctx: &ExprOrContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_exprOr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exprAndUnless(&mut self, ctx: &ExprAndUnlessContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_exprAndUnless(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exprSubquery(&mut self, ctx: &ExprSubqueryContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_exprSubquery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exprAddSub(&mut self, ctx: &ExprAddSubContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_exprAddSub(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exprPow(&mut self, ctx: &ExprPowContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_exprPow(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exprUnary(&mut self, ctx: &ExprUnaryContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_exprUnary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exprCompare(&mut self, ctx: &ExprCompareContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_exprCompare(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_exprVector(&mut self, ctx: &ExprVectorContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_exprVector(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorFunction(&mut self, ctx: &VectorFunctionContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_vectorFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorAggregation(&mut self, ctx: &VectorAggregationContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_vectorAggregation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorMatrix(&mut self, ctx: &VectorMatrixContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_vectorMatrix(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorInstant(&mut self, ctx: &VectorInstantContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_vectorInstant(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorNumber(&mut self, ctx: &VectorNumberContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_vectorNumber(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorString(&mut self, ctx: &VectorStringContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_vectorString(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorParen(&mut self, ctx: &VectorParenContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_vectorParen(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_parenExpr(&mut self, ctx: &ParenExprContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_parenExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unaryExpr(&mut self, ctx: &UnaryExprContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_unaryExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unaryOp(&mut self, ctx: &UnaryOpContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_unaryOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mulDivModOp(&mut self, ctx: &MulDivModOpContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_mulDivModOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_addSubOp(&mut self, ctx: &AddSubOpContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_addSubOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_compareOp(&mut self, ctx: &CompareOpContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_compareOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_andUnlessOp(&mut self, ctx: &AndUnlessOpContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_andUnlessOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binOpModifier(&mut self, ctx: &BinOpModifierContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_binOpModifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_onOrIgnoring(&mut self, ctx: &OnOrIgnoringContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_onOrIgnoring(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupModifier(&mut self, ctx: &GroupModifierContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_groupModifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_instantSelector(&mut self, ctx: &InstantSelectorContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_instantSelector(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_matrixSelector(&mut self, ctx: &MatrixSelectorContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_matrixSelector(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelMatchers(&mut self, ctx: &LabelMatchersContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_labelMatchers(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelMatcher(&mut self, ctx: &LabelMatcherContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_labelMatcher(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelMatcherOp(&mut self, ctx: &LabelMatcherOpContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_labelMatcherOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_functionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionName(&mut self, ctx: &FunctionNameContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_functionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeFunctionName(&mut self, ctx: &RangeFunctionNameContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_rangeFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mathFunctionName(&mut self, ctx: &MathFunctionNameContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_mathFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trigFunctionName(&mut self, ctx: &TrigFunctionNameContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_trigFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFunctionName(&mut self, ctx: &LabelFunctionNameContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_labelFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timeFunctionName(&mut self, ctx: &TimeFunctionNameContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_timeFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_histogramFunctionName(&mut self, ctx: &HistogramFunctionNameContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_histogramFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aggregation(&mut self, ctx: &AggregationContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_aggregation(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aggregationOp(&mut self, ctx: &AggregationOpContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_aggregationOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grouping(&mut self, ctx: &GroupingContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_grouping(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timeRange(&mut self, ctx: &TimeRangeContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_timeRange(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_offsetModifier(&mut self, ctx: &OffsetModifierContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_offsetModifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_atModifier(&mut self, ctx: &AtModifierContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_atModifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelName(&mut self, ctx: &LabelNameContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_labelName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelNameList(&mut self, ctx: &LabelNameListContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_labelNameList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_keyword(&mut self, ctx: &KeywordContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_keyword(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_numberLiteral(&mut self, ctx: &NumberLiteralContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_numberLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_stringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_duration(&mut self, ctx: &DurationContext<'input>){
		let result = <Self as PromQLParserVisitorCompat>::visit_duration(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

}