#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#![allow(nonstandard_style)]
// Generated from antlr/TraceQLParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::{ParseTreeVisitor,ParseTreeVisitorCompat};
use super::traceqlparser::*;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link TraceQLParser}.
 */
pub trait TraceQLParserVisitor<'input>: ParseTreeVisitor<'input,TraceQLParserContextType>{
	/**
	 * Visit a parse tree produced by {@link TraceQLParser#root}.
	 * @param ctx the parse tree
	 */
	fn visit_root(&mut self, ctx: &RootContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#pipelineExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_pipelineExpr(&mut self, ctx: &PipelineExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#pipelineStage}.
	 * @param ctx the parse tree
	 */
	fn visit_pipelineStage(&mut self, ctx: &PipelineStageContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#byClause}.
	 * @param ctx the parse tree
	 */
	fn visit_byClause(&mut self, ctx: &ByClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#aggregate}.
	 * @param ctx the parse tree
	 */
	fn visit_aggregate(&mut self, ctx: &AggregateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#aggregateFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_aggregateFilter(&mut self, ctx: &AggregateFilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#aggregateOp}.
	 * @param ctx the parse tree
	 */
	fn visit_aggregateOp(&mut self, ctx: &AggregateOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#metricsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_metricsFunction(&mut self, ctx: &MetricsFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code SpansetParen}
	 * labeled alternative in {@link TraceQLParser#spansetExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_SpansetParen(&mut self, ctx: &SpansetParenContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code SpansetLeaf}
	 * labeled alternative in {@link TraceQLParser#spansetExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_SpansetLeaf(&mut self, ctx: &SpansetLeafContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code SpansetBinary}
	 * labeled alternative in {@link TraceQLParser#spansetExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_SpansetBinary(&mut self, ctx: &SpansetBinaryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#spansetOp}.
	 * @param ctx the parse tree
	 */
	fn visit_spansetOp(&mut self, ctx: &SpansetOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#spanSelector}.
	 * @param ctx the parse tree
	 */
	fn visit_spanSelector(&mut self, ctx: &SpanSelectorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code FilterParen}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_FilterParen(&mut self, ctx: &FilterParenContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code FilterNot}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_FilterNot(&mut self, ctx: &FilterNotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code FilterCompare}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_FilterCompare(&mut self, ctx: &FilterCompareContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code FilterOr}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_FilterOr(&mut self, ctx: &FilterOrContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code FilterAnd}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_FilterAnd(&mut self, ctx: &FilterAndContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#comparisonOp}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonOp(&mut self, ctx: &ComparisonOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code FieldIntrinsic}
	 * labeled alternative in {@link TraceQLParser#fieldRef}.
	 * @param ctx the parse tree
	 */
	fn visit_FieldIntrinsic(&mut self, ctx: &FieldIntrinsicContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code FieldScoped}
	 * labeled alternative in {@link TraceQLParser#fieldRef}.
	 * @param ctx the parse tree
	 */
	fn visit_FieldScoped(&mut self, ctx: &FieldScopedContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code FieldAnyScope}
	 * labeled alternative in {@link TraceQLParser#fieldRef}.
	 * @param ctx the parse tree
	 */
	fn visit_FieldAnyScope(&mut self, ctx: &FieldAnyScopeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#scopedAttribute}.
	 * @param ctx the parse tree
	 */
	fn visit_scopedAttribute(&mut self, ctx: &ScopedAttributeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#identChain}.
	 * @param ctx the parse tree
	 */
	fn visit_identChain(&mut self, ctx: &IdentChainContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#identPart}.
	 * @param ctx the parse tree
	 */
	fn visit_identPart(&mut self, ctx: &IdentPartContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#intrinsic}.
	 * @param ctx the parse tree
	 */
	fn visit_intrinsic(&mut self, ctx: &IntrinsicContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#literal}.
	 * @param ctx the parse tree
	 */
	fn visit_literal(&mut self, ctx: &LiteralContext<'input>) { self.visit_children(ctx) }

}

pub trait TraceQLParserVisitorCompat<'input>:ParseTreeVisitorCompat<'input, Node= TraceQLParserContextType>{
	/**
	 * Visit a parse tree produced by {@link TraceQLParser#root}.
	 * @param ctx the parse tree
	 */
		fn visit_root(&mut self, ctx: &RootContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#pipelineExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_pipelineExpr(&mut self, ctx: &PipelineExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#pipelineStage}.
	 * @param ctx the parse tree
	 */
		fn visit_pipelineStage(&mut self, ctx: &PipelineStageContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#byClause}.
	 * @param ctx the parse tree
	 */
		fn visit_byClause(&mut self, ctx: &ByClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#aggregate}.
	 * @param ctx the parse tree
	 */
		fn visit_aggregate(&mut self, ctx: &AggregateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#aggregateFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_aggregateFilter(&mut self, ctx: &AggregateFilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#aggregateOp}.
	 * @param ctx the parse tree
	 */
		fn visit_aggregateOp(&mut self, ctx: &AggregateOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#metricsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_metricsFunction(&mut self, ctx: &MetricsFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code SpansetParen}
	 * labeled alternative in {@link TraceQLParser#spansetExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_SpansetParen(&mut self, ctx: &SpansetParenContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code SpansetLeaf}
	 * labeled alternative in {@link TraceQLParser#spansetExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_SpansetLeaf(&mut self, ctx: &SpansetLeafContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code SpansetBinary}
	 * labeled alternative in {@link TraceQLParser#spansetExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_SpansetBinary(&mut self, ctx: &SpansetBinaryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#spansetOp}.
	 * @param ctx the parse tree
	 */
		fn visit_spansetOp(&mut self, ctx: &SpansetOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#spanSelector}.
	 * @param ctx the parse tree
	 */
		fn visit_spanSelector(&mut self, ctx: &SpanSelectorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code FilterParen}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_FilterParen(&mut self, ctx: &FilterParenContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code FilterNot}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_FilterNot(&mut self, ctx: &FilterNotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code FilterCompare}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_FilterCompare(&mut self, ctx: &FilterCompareContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code FilterOr}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_FilterOr(&mut self, ctx: &FilterOrContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code FilterAnd}
	 * labeled alternative in {@link TraceQLParser#spanFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_FilterAnd(&mut self, ctx: &FilterAndContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#comparisonOp}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonOp(&mut self, ctx: &ComparisonOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code FieldIntrinsic}
	 * labeled alternative in {@link TraceQLParser#fieldRef}.
	 * @param ctx the parse tree
	 */
		fn visit_FieldIntrinsic(&mut self, ctx: &FieldIntrinsicContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code FieldScoped}
	 * labeled alternative in {@link TraceQLParser#fieldRef}.
	 * @param ctx the parse tree
	 */
		fn visit_FieldScoped(&mut self, ctx: &FieldScopedContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code FieldAnyScope}
	 * labeled alternative in {@link TraceQLParser#fieldRef}.
	 * @param ctx the parse tree
	 */
		fn visit_FieldAnyScope(&mut self, ctx: &FieldAnyScopeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#scopedAttribute}.
	 * @param ctx the parse tree
	 */
		fn visit_scopedAttribute(&mut self, ctx: &ScopedAttributeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#identChain}.
	 * @param ctx the parse tree
	 */
		fn visit_identChain(&mut self, ctx: &IdentChainContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#identPart}.
	 * @param ctx the parse tree
	 */
		fn visit_identPart(&mut self, ctx: &IdentPartContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#intrinsic}.
	 * @param ctx the parse tree
	 */
		fn visit_intrinsic(&mut self, ctx: &IntrinsicContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link TraceQLParser#literal}.
	 * @param ctx the parse tree
	 */
		fn visit_literal(&mut self, ctx: &LiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

}

impl<'input,T> TraceQLParserVisitor<'input> for T
where
	T: TraceQLParserVisitorCompat<'input>
{
	fn visit_root(&mut self, ctx: &RootContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_root(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pipelineExpr(&mut self, ctx: &PipelineExprContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_pipelineExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pipelineStage(&mut self, ctx: &PipelineStageContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_pipelineStage(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_byClause(&mut self, ctx: &ByClauseContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_byClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aggregate(&mut self, ctx: &AggregateContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_aggregate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aggregateFilter(&mut self, ctx: &AggregateFilterContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_aggregateFilter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aggregateOp(&mut self, ctx: &AggregateOpContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_aggregateOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_metricsFunction(&mut self, ctx: &MetricsFunctionContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_metricsFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_SpansetParen(&mut self, ctx: &SpansetParenContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_SpansetParen(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_SpansetLeaf(&mut self, ctx: &SpansetLeafContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_SpansetLeaf(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_SpansetBinary(&mut self, ctx: &SpansetBinaryContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_SpansetBinary(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_spansetOp(&mut self, ctx: &SpansetOpContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_spansetOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_spanSelector(&mut self, ctx: &SpanSelectorContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_spanSelector(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_FilterParen(&mut self, ctx: &FilterParenContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_FilterParen(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_FilterNot(&mut self, ctx: &FilterNotContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_FilterNot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_FilterCompare(&mut self, ctx: &FilterCompareContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_FilterCompare(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_FilterOr(&mut self, ctx: &FilterOrContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_FilterOr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_FilterAnd(&mut self, ctx: &FilterAndContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_FilterAnd(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonOp(&mut self, ctx: &ComparisonOpContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_comparisonOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_FieldIntrinsic(&mut self, ctx: &FieldIntrinsicContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_FieldIntrinsic(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_FieldScoped(&mut self, ctx: &FieldScopedContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_FieldScoped(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_FieldAnyScope(&mut self, ctx: &FieldAnyScopeContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_FieldAnyScope(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_scopedAttribute(&mut self, ctx: &ScopedAttributeContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_scopedAttribute(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identChain(&mut self, ctx: &IdentChainContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_identChain(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identPart(&mut self, ctx: &IdentPartContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_identPart(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intrinsic(&mut self, ctx: &IntrinsicContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_intrinsic(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_literal(&mut self, ctx: &LiteralContext<'input>){
		let result = <Self as TraceQLParserVisitorCompat>::visit_literal(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

}