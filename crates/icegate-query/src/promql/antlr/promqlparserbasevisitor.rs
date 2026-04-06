#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]

// Generated from antlr/PromQLParser.g4 by ANTLR 4.13.2

use antlr4rust::tree::ParseTreeVisitor;
use super::promqlparser::*;

// A complete Visitor for a parse tree produced by PromQLParser.

pub trait PromQLParserBaseVisitor<'input>:
    ParseTreeVisitor<'input, PromQLParserContextType> {
	// Visit a parse tree produced by PromQLParser#root.
	fn visit_root(&mut self, ctx: &RootContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#exprMulDivMod.
	fn visit_exprmuldivmod(&mut self, ctx: &ExprMulDivModContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#exprOr.
	fn visit_expror(&mut self, ctx: &ExprOrContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#exprAndUnless.
	fn visit_exprandunless(&mut self, ctx: &ExprAndUnlessContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#exprSubquery.
	fn visit_exprsubquery(&mut self, ctx: &ExprSubqueryContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#exprAddSub.
	fn visit_expraddsub(&mut self, ctx: &ExprAddSubContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#exprPow.
	fn visit_exprpow(&mut self, ctx: &ExprPowContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#exprUnary.
	fn visit_exprunary(&mut self, ctx: &ExprUnaryContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#exprCompare.
	fn visit_exprcompare(&mut self, ctx: &ExprCompareContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#exprVector.
	fn visit_exprvector(&mut self, ctx: &ExprVectorContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#vectorFunction.
	fn visit_vectorfunction(&mut self, ctx: &VectorFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#vectorAggregation.
	fn visit_vectoraggregation(&mut self, ctx: &VectorAggregationContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#vectorMatrix.
	fn visit_vectormatrix(&mut self, ctx: &VectorMatrixContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#vectorInstant.
	fn visit_vectorinstant(&mut self, ctx: &VectorInstantContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#vectorNumber.
	fn visit_vectornumber(&mut self, ctx: &VectorNumberContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#vectorString.
	fn visit_vectorstring(&mut self, ctx: &VectorStringContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#vectorParen.
	fn visit_vectorparen(&mut self, ctx: &VectorParenContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#parenExpr.
	fn visit_parenexpr(&mut self, ctx: &ParenExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#unaryExpr.
	fn visit_unaryexpr(&mut self, ctx: &UnaryExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#unaryOp.
	fn visit_unaryop(&mut self, ctx: &UnaryOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#mulDivModOp.
	fn visit_muldivmodop(&mut self, ctx: &MulDivModOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#addSubOp.
	fn visit_addsubop(&mut self, ctx: &AddSubOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#compareOp.
	fn visit_compareop(&mut self, ctx: &CompareOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#andUnlessOp.
	fn visit_andunlessop(&mut self, ctx: &AndUnlessOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#binOpModifier.
	fn visit_binopmodifier(&mut self, ctx: &BinOpModifierContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#onOrIgnoring.
	fn visit_onorignoring(&mut self, ctx: &OnOrIgnoringContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#groupModifier.
	fn visit_groupmodifier(&mut self, ctx: &GroupModifierContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#instantSelector.
	fn visit_instantselector(&mut self, ctx: &InstantSelectorContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#matrixSelector.
	fn visit_matrixselector(&mut self, ctx: &MatrixSelectorContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#labelMatchers.
	fn visit_labelmatchers(&mut self, ctx: &LabelMatchersContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#labelMatcher.
	fn visit_labelmatcher(&mut self, ctx: &LabelMatcherContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#labelMatcherOp.
	fn visit_labelmatcherop(&mut self, ctx: &LabelMatcherOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#functionCall.
	fn visit_functioncall(&mut self, ctx: &FunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#functionName.
	fn visit_functionname(&mut self, ctx: &FunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#rangeFunctionName.
	fn visit_rangefunctionname(&mut self, ctx: &RangeFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#mathFunctionName.
	fn visit_mathfunctionname(&mut self, ctx: &MathFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#trigFunctionName.
	fn visit_trigfunctionname(&mut self, ctx: &TrigFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#labelFunctionName.
	fn visit_labelfunctionname(&mut self, ctx: &LabelFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#timeFunctionName.
	fn visit_timefunctionname(&mut self, ctx: &TimeFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#histogramFunctionName.
	fn visit_histogramfunctionname(&mut self, ctx: &HistogramFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#aggregation.
	fn visit_aggregation(&mut self, ctx: &AggregationContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#aggregationOp.
	fn visit_aggregationop(&mut self, ctx: &AggregationOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#grouping.
	fn visit_grouping(&mut self, ctx: &GroupingContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#timeRange.
	fn visit_timerange(&mut self, ctx: &TimeRangeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#offsetModifier.
	fn visit_offsetmodifier(&mut self, ctx: &OffsetModifierContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#atModifier.
	fn visit_atmodifier(&mut self, ctx: &AtModifierContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#labelName.
	fn visit_labelname(&mut self, ctx: &LabelNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#labelNameList.
	fn visit_labelnamelist(&mut self, ctx: &LabelNameListContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#keyword.
	fn visit_keyword(&mut self, ctx: &KeywordContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#numberLiteral.
	fn visit_numberliteral(&mut self, ctx: &NumberLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#stringLiteral.
	fn visit_stringliteral(&mut self, ctx: &StringLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by PromQLParser#duration.
	fn visit_duration(&mut self, ctx: &DurationContext<'input>) {
            self.visit_children(ctx)
        }

}