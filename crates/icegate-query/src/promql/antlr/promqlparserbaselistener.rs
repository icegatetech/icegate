#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
// Generated from antlr/PromQLParser.g4 by ANTLR 4.13.2

use super::promqlparser::*;
use antlr4rust::tree::ParseTreeListener;

// A complete Visitor for a parse tree produced by PromQLParser.

pub trait PromQLParserBaseListener<'input>:
    ParseTreeListener<'input, PromQLParserContextType> {

    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_root(&mut self, _ctx: &RootContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_root(&mut self, _ctx: &RootContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_exprmuldivmod(&mut self, _ctx: &ExprMulDivModContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_exprmuldivmod(&mut self, _ctx: &ExprMulDivModContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_expror(&mut self, _ctx: &ExprOrContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_expror(&mut self, _ctx: &ExprOrContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_exprandunless(&mut self, _ctx: &ExprAndUnlessContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_exprandunless(&mut self, _ctx: &ExprAndUnlessContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_exprsubquery(&mut self, _ctx: &ExprSubqueryContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_exprsubquery(&mut self, _ctx: &ExprSubqueryContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_expraddsub(&mut self, _ctx: &ExprAddSubContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_expraddsub(&mut self, _ctx: &ExprAddSubContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_exprpow(&mut self, _ctx: &ExprPowContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_exprpow(&mut self, _ctx: &ExprPowContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_exprunary(&mut self, _ctx: &ExprUnaryContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_exprunary(&mut self, _ctx: &ExprUnaryContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_exprcompare(&mut self, _ctx: &ExprCompareContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_exprcompare(&mut self, _ctx: &ExprCompareContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_exprvector(&mut self, _ctx: &ExprVectorContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_exprvector(&mut self, _ctx: &ExprVectorContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectorfunction(&mut self, _ctx: &VectorFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectorfunction(&mut self, _ctx: &VectorFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectoraggregation(&mut self, _ctx: &VectorAggregationContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectoraggregation(&mut self, _ctx: &VectorAggregationContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectormatrix(&mut self, _ctx: &VectorMatrixContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectormatrix(&mut self, _ctx: &VectorMatrixContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectorinstant(&mut self, _ctx: &VectorInstantContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectorinstant(&mut self, _ctx: &VectorInstantContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectornumber(&mut self, _ctx: &VectorNumberContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectornumber(&mut self, _ctx: &VectorNumberContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectorstring(&mut self, _ctx: &VectorStringContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectorstring(&mut self, _ctx: &VectorStringContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectorparen(&mut self, _ctx: &VectorParenContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectorparen(&mut self, _ctx: &VectorParenContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_parenexpr(&mut self, _ctx: &ParenExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_parenexpr(&mut self, _ctx: &ParenExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_unaryexpr(&mut self, _ctx: &UnaryExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_unaryexpr(&mut self, _ctx: &UnaryExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_unaryop(&mut self, _ctx: &UnaryOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_unaryop(&mut self, _ctx: &UnaryOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_muldivmodop(&mut self, _ctx: &MulDivModOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_muldivmodop(&mut self, _ctx: &MulDivModOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_addsubop(&mut self, _ctx: &AddSubOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_addsubop(&mut self, _ctx: &AddSubOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_compareop(&mut self, _ctx: &CompareOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_compareop(&mut self, _ctx: &CompareOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_andunlessop(&mut self, _ctx: &AndUnlessOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_andunlessop(&mut self, _ctx: &AndUnlessOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binopmodifier(&mut self, _ctx: &BinOpModifierContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binopmodifier(&mut self, _ctx: &BinOpModifierContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_onorignoring(&mut self, _ctx: &OnOrIgnoringContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_onorignoring(&mut self, _ctx: &OnOrIgnoringContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_groupmodifier(&mut self, _ctx: &GroupModifierContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_groupmodifier(&mut self, _ctx: &GroupModifierContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_instantselector(&mut self, _ctx: &InstantSelectorContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_instantselector(&mut self, _ctx: &InstantSelectorContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_matrixselector(&mut self, _ctx: &MatrixSelectorContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_matrixselector(&mut self, _ctx: &MatrixSelectorContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelmatchers(&mut self, _ctx: &LabelMatchersContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelmatchers(&mut self, _ctx: &LabelMatchersContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelmatcher(&mut self, _ctx: &LabelMatcherContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelmatcher(&mut self, _ctx: &LabelMatcherContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelmatcherop(&mut self, _ctx: &LabelMatcherOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelmatcherop(&mut self, _ctx: &LabelMatcherOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_functioncall(&mut self, _ctx: &FunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_functioncall(&mut self, _ctx: &FunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_functionname(&mut self, _ctx: &FunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_functionname(&mut self, _ctx: &FunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangefunctionname(&mut self, _ctx: &RangeFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangefunctionname(&mut self, _ctx: &RangeFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_mathfunctionname(&mut self, _ctx: &MathFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_mathfunctionname(&mut self, _ctx: &MathFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_trigfunctionname(&mut self, _ctx: &TrigFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_trigfunctionname(&mut self, _ctx: &TrigFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelfunctionname(&mut self, _ctx: &LabelFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelfunctionname(&mut self, _ctx: &LabelFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timefunctionname(&mut self, _ctx: &TimeFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timefunctionname(&mut self, _ctx: &TimeFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_histogramfunctionname(&mut self, _ctx: &HistogramFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_histogramfunctionname(&mut self, _ctx: &HistogramFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_aggregation(&mut self, _ctx: &AggregationContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_aggregation(&mut self, _ctx: &AggregationContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_aggregationop(&mut self, _ctx: &AggregationOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_aggregationop(&mut self, _ctx: &AggregationOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_grouping(&mut self, _ctx: &GroupingContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_grouping(&mut self, _ctx: &GroupingContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timerange(&mut self, _ctx: &TimeRangeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timerange(&mut self, _ctx: &TimeRangeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_offsetmodifier(&mut self, _ctx: &OffsetModifierContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_offsetmodifier(&mut self, _ctx: &OffsetModifierContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_atmodifier(&mut self, _ctx: &AtModifierContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_atmodifier(&mut self, _ctx: &AtModifierContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelname(&mut self, _ctx: &LabelNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelname(&mut self, _ctx: &LabelNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelnamelist(&mut self, _ctx: &LabelNameListContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelnamelist(&mut self, _ctx: &LabelNameListContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_keyword(&mut self, _ctx: &KeywordContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_keyword(&mut self, _ctx: &KeywordContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_numberliteral(&mut self, _ctx: &NumberLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_numberliteral(&mut self, _ctx: &NumberLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_stringliteral(&mut self, _ctx: &StringLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_stringliteral(&mut self, _ctx: &StringLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_duration(&mut self, _ctx: &DurationContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  PromQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_duration(&mut self, _ctx: &DurationContext<'input>) {}


}