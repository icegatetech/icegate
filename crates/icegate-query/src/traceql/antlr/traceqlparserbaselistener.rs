#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
// Generated from antlr/TraceQLParser.g4 by ANTLR 4.13.2

use super::traceqlparser::*;
use antlr4rust::tree::ParseTreeListener;

// A complete Visitor for a parse tree produced by TraceQLParser.

pub trait TraceQLParserBaseListener<'input>:
    ParseTreeListener<'input, TraceQLParserContextType> {

    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_root(&mut self, _ctx: &RootContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_root(&mut self, _ctx: &RootContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_pipelineexpr(&mut self, _ctx: &PipelineExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_pipelineexpr(&mut self, _ctx: &PipelineExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_pipelinestage(&mut self, _ctx: &PipelineStageContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_pipelinestage(&mut self, _ctx: &PipelineStageContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_byclause(&mut self, _ctx: &ByClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_byclause(&mut self, _ctx: &ByClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_aggregate(&mut self, _ctx: &AggregateContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_aggregate(&mut self, _ctx: &AggregateContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_aggregatefilter(&mut self, _ctx: &AggregateFilterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_aggregatefilter(&mut self, _ctx: &AggregateFilterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_aggregateop(&mut self, _ctx: &AggregateOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_aggregateop(&mut self, _ctx: &AggregateOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_metricsfunction(&mut self, _ctx: &MetricsFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_metricsfunction(&mut self, _ctx: &MetricsFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_spansetparen(&mut self, _ctx: &SpansetParenContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_spansetparen(&mut self, _ctx: &SpansetParenContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_spansetleaf(&mut self, _ctx: &SpansetLeafContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_spansetleaf(&mut self, _ctx: &SpansetLeafContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_spansetbinary(&mut self, _ctx: &SpansetBinaryContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_spansetbinary(&mut self, _ctx: &SpansetBinaryContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_spansetop(&mut self, _ctx: &SpansetOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_spansetop(&mut self, _ctx: &SpansetOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_spanselector(&mut self, _ctx: &SpanSelectorContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_spanselector(&mut self, _ctx: &SpanSelectorContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_filterparen(&mut self, _ctx: &FilterParenContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_filterparen(&mut self, _ctx: &FilterParenContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_filternot(&mut self, _ctx: &FilterNotContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_filternot(&mut self, _ctx: &FilterNotContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_filtercompare(&mut self, _ctx: &FilterCompareContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_filtercompare(&mut self, _ctx: &FilterCompareContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_filteror(&mut self, _ctx: &FilterOrContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_filteror(&mut self, _ctx: &FilterOrContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_filterand(&mut self, _ctx: &FilterAndContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_filterand(&mut self, _ctx: &FilterAndContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_comparisonop(&mut self, _ctx: &ComparisonOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_comparisonop(&mut self, _ctx: &ComparisonOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldintrinsic(&mut self, _ctx: &FieldIntrinsicContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldintrinsic(&mut self, _ctx: &FieldIntrinsicContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldscoped(&mut self, _ctx: &FieldScopedContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldscoped(&mut self, _ctx: &FieldScopedContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldanyscope(&mut self, _ctx: &FieldAnyScopeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldanyscope(&mut self, _ctx: &FieldAnyScopeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_scopedattribute(&mut self, _ctx: &ScopedAttributeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_scopedattribute(&mut self, _ctx: &ScopedAttributeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_identchain(&mut self, _ctx: &IdentChainContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_identchain(&mut self, _ctx: &IdentChainContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_identpart(&mut self, _ctx: &IdentPartContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_identpart(&mut self, _ctx: &IdentPartContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_intrinsic(&mut self, _ctx: &IntrinsicContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_intrinsic(&mut self, _ctx: &IntrinsicContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_literal(&mut self, _ctx: &LiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  TraceQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_literal(&mut self, _ctx: &LiteralContext<'input>) {}


}