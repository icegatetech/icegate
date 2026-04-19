#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#![allow(nonstandard_style)]
// Generated from antlr/TraceQLParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::ParseTreeListener;
use super::traceqlparser::*;

pub trait TraceQLParserListener<'input> : ParseTreeListener<'input,TraceQLParserContextType>{
/**
 * Enter a parse tree produced by {@link TraceQLParser#root}.
 * @param ctx the parse tree
 */
fn enter_root(&mut self, _ctx: &RootContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#root}.
 * @param ctx the parse tree
 */
fn exit_root(&mut self, _ctx: &RootContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#pipelineExpr}.
 * @param ctx the parse tree
 */
fn enter_pipelineExpr(&mut self, _ctx: &PipelineExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#pipelineExpr}.
 * @param ctx the parse tree
 */
fn exit_pipelineExpr(&mut self, _ctx: &PipelineExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#pipelineStage}.
 * @param ctx the parse tree
 */
fn enter_pipelineStage(&mut self, _ctx: &PipelineStageContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#pipelineStage}.
 * @param ctx the parse tree
 */
fn exit_pipelineStage(&mut self, _ctx: &PipelineStageContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#byClause}.
 * @param ctx the parse tree
 */
fn enter_byClause(&mut self, _ctx: &ByClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#byClause}.
 * @param ctx the parse tree
 */
fn exit_byClause(&mut self, _ctx: &ByClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#aggregate}.
 * @param ctx the parse tree
 */
fn enter_aggregate(&mut self, _ctx: &AggregateContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#aggregate}.
 * @param ctx the parse tree
 */
fn exit_aggregate(&mut self, _ctx: &AggregateContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#aggregateFilter}.
 * @param ctx the parse tree
 */
fn enter_aggregateFilter(&mut self, _ctx: &AggregateFilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#aggregateFilter}.
 * @param ctx the parse tree
 */
fn exit_aggregateFilter(&mut self, _ctx: &AggregateFilterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#aggregateOp}.
 * @param ctx the parse tree
 */
fn enter_aggregateOp(&mut self, _ctx: &AggregateOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#aggregateOp}.
 * @param ctx the parse tree
 */
fn exit_aggregateOp(&mut self, _ctx: &AggregateOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#metricsFunction}.
 * @param ctx the parse tree
 */
fn enter_metricsFunction(&mut self, _ctx: &MetricsFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#metricsFunction}.
 * @param ctx the parse tree
 */
fn exit_metricsFunction(&mut self, _ctx: &MetricsFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code SpansetParen}
 * labeled alternative in {@link TraceQLParser#spansetExpr}.
 * @param ctx the parse tree
 */
fn enter_SpansetParen(&mut self, _ctx: &SpansetParenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code SpansetParen}
 * labeled alternative in {@link TraceQLParser#spansetExpr}.
 * @param ctx the parse tree
 */
fn exit_SpansetParen(&mut self, _ctx: &SpansetParenContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code SpansetLeaf}
 * labeled alternative in {@link TraceQLParser#spansetExpr}.
 * @param ctx the parse tree
 */
fn enter_SpansetLeaf(&mut self, _ctx: &SpansetLeafContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code SpansetLeaf}
 * labeled alternative in {@link TraceQLParser#spansetExpr}.
 * @param ctx the parse tree
 */
fn exit_SpansetLeaf(&mut self, _ctx: &SpansetLeafContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code SpansetBinary}
 * labeled alternative in {@link TraceQLParser#spansetExpr}.
 * @param ctx the parse tree
 */
fn enter_SpansetBinary(&mut self, _ctx: &SpansetBinaryContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code SpansetBinary}
 * labeled alternative in {@link TraceQLParser#spansetExpr}.
 * @param ctx the parse tree
 */
fn exit_SpansetBinary(&mut self, _ctx: &SpansetBinaryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#spansetOp}.
 * @param ctx the parse tree
 */
fn enter_spansetOp(&mut self, _ctx: &SpansetOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#spansetOp}.
 * @param ctx the parse tree
 */
fn exit_spansetOp(&mut self, _ctx: &SpansetOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#spanSelector}.
 * @param ctx the parse tree
 */
fn enter_spanSelector(&mut self, _ctx: &SpanSelectorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#spanSelector}.
 * @param ctx the parse tree
 */
fn exit_spanSelector(&mut self, _ctx: &SpanSelectorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code FilterParen}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn enter_FilterParen(&mut self, _ctx: &FilterParenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code FilterParen}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn exit_FilterParen(&mut self, _ctx: &FilterParenContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code FilterNot}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn enter_FilterNot(&mut self, _ctx: &FilterNotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code FilterNot}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn exit_FilterNot(&mut self, _ctx: &FilterNotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code FilterCompare}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn enter_FilterCompare(&mut self, _ctx: &FilterCompareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code FilterCompare}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn exit_FilterCompare(&mut self, _ctx: &FilterCompareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code FilterOr}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn enter_FilterOr(&mut self, _ctx: &FilterOrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code FilterOr}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn exit_FilterOr(&mut self, _ctx: &FilterOrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code FilterAnd}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn enter_FilterAnd(&mut self, _ctx: &FilterAndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code FilterAnd}
 * labeled alternative in {@link TraceQLParser#spanFilter}.
 * @param ctx the parse tree
 */
fn exit_FilterAnd(&mut self, _ctx: &FilterAndContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#comparisonOp}.
 * @param ctx the parse tree
 */
fn enter_comparisonOp(&mut self, _ctx: &ComparisonOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#comparisonOp}.
 * @param ctx the parse tree
 */
fn exit_comparisonOp(&mut self, _ctx: &ComparisonOpContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code FieldIntrinsic}
 * labeled alternative in {@link TraceQLParser#fieldRef}.
 * @param ctx the parse tree
 */
fn enter_FieldIntrinsic(&mut self, _ctx: &FieldIntrinsicContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code FieldIntrinsic}
 * labeled alternative in {@link TraceQLParser#fieldRef}.
 * @param ctx the parse tree
 */
fn exit_FieldIntrinsic(&mut self, _ctx: &FieldIntrinsicContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code FieldScoped}
 * labeled alternative in {@link TraceQLParser#fieldRef}.
 * @param ctx the parse tree
 */
fn enter_FieldScoped(&mut self, _ctx: &FieldScopedContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code FieldScoped}
 * labeled alternative in {@link TraceQLParser#fieldRef}.
 * @param ctx the parse tree
 */
fn exit_FieldScoped(&mut self, _ctx: &FieldScopedContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code FieldAnyScope}
 * labeled alternative in {@link TraceQLParser#fieldRef}.
 * @param ctx the parse tree
 */
fn enter_FieldAnyScope(&mut self, _ctx: &FieldAnyScopeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code FieldAnyScope}
 * labeled alternative in {@link TraceQLParser#fieldRef}.
 * @param ctx the parse tree
 */
fn exit_FieldAnyScope(&mut self, _ctx: &FieldAnyScopeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#scopedAttribute}.
 * @param ctx the parse tree
 */
fn enter_scopedAttribute(&mut self, _ctx: &ScopedAttributeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#scopedAttribute}.
 * @param ctx the parse tree
 */
fn exit_scopedAttribute(&mut self, _ctx: &ScopedAttributeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#identChain}.
 * @param ctx the parse tree
 */
fn enter_identChain(&mut self, _ctx: &IdentChainContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#identChain}.
 * @param ctx the parse tree
 */
fn exit_identChain(&mut self, _ctx: &IdentChainContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#identPart}.
 * @param ctx the parse tree
 */
fn enter_identPart(&mut self, _ctx: &IdentPartContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#identPart}.
 * @param ctx the parse tree
 */
fn exit_identPart(&mut self, _ctx: &IdentPartContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#intrinsic}.
 * @param ctx the parse tree
 */
fn enter_intrinsic(&mut self, _ctx: &IntrinsicContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#intrinsic}.
 * @param ctx the parse tree
 */
fn exit_intrinsic(&mut self, _ctx: &IntrinsicContext<'input>) { }
/**
 * Enter a parse tree produced by {@link TraceQLParser#literal}.
 * @param ctx the parse tree
 */
fn enter_literal(&mut self, _ctx: &LiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link TraceQLParser#literal}.
 * @param ctx the parse tree
 */
fn exit_literal(&mut self, _ctx: &LiteralContext<'input>) { }

}

antlr4rust::coerce_from!{ 'input : TraceQLParserListener<'input> }


