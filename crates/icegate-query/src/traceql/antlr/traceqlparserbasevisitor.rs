#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]

// Generated from antlr/TraceQLParser.g4 by ANTLR 4.13.2

use antlr4rust::tree::ParseTreeVisitor;
use super::traceqlparser::*;

// A complete Visitor for a parse tree produced by TraceQLParser.

pub trait TraceQLParserBaseVisitor<'input>:
    ParseTreeVisitor<'input, TraceQLParserContextType> {
	// Visit a parse tree produced by TraceQLParser#root.
	fn visit_root(&mut self, ctx: &RootContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#pipelineExpr.
	fn visit_pipelineexpr(&mut self, ctx: &PipelineExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#pipelineStage.
	fn visit_pipelinestage(&mut self, ctx: &PipelineStageContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#byClause.
	fn visit_byclause(&mut self, ctx: &ByClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#aggregate.
	fn visit_aggregate(&mut self, ctx: &AggregateContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#aggregateFilter.
	fn visit_aggregatefilter(&mut self, ctx: &AggregateFilterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#aggregateOp.
	fn visit_aggregateop(&mut self, ctx: &AggregateOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#metricsFunction.
	fn visit_metricsfunction(&mut self, ctx: &MetricsFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#SpansetParen.
	fn visit_spansetparen(&mut self, ctx: &SpansetParenContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#SpansetLeaf.
	fn visit_spansetleaf(&mut self, ctx: &SpansetLeafContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#SpansetBinary.
	fn visit_spansetbinary(&mut self, ctx: &SpansetBinaryContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#spansetOp.
	fn visit_spansetop(&mut self, ctx: &SpansetOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#spanSelector.
	fn visit_spanselector(&mut self, ctx: &SpanSelectorContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#FilterParen.
	fn visit_filterparen(&mut self, ctx: &FilterParenContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#FilterNot.
	fn visit_filternot(&mut self, ctx: &FilterNotContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#FilterCompare.
	fn visit_filtercompare(&mut self, ctx: &FilterCompareContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#FilterOr.
	fn visit_filteror(&mut self, ctx: &FilterOrContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#FilterAnd.
	fn visit_filterand(&mut self, ctx: &FilterAndContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#comparisonOp.
	fn visit_comparisonop(&mut self, ctx: &ComparisonOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#FieldIntrinsic.
	fn visit_fieldintrinsic(&mut self, ctx: &FieldIntrinsicContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#FieldScoped.
	fn visit_fieldscoped(&mut self, ctx: &FieldScopedContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#FieldAnyScope.
	fn visit_fieldanyscope(&mut self, ctx: &FieldAnyScopeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#scopedAttribute.
	fn visit_scopedattribute(&mut self, ctx: &ScopedAttributeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#identChain.
	fn visit_identchain(&mut self, ctx: &IdentChainContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#identPart.
	fn visit_identpart(&mut self, ctx: &IdentPartContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#intrinsic.
	fn visit_intrinsic(&mut self, ctx: &IntrinsicContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by TraceQLParser#literal.
	fn visit_literal(&mut self, ctx: &LiteralContext<'input>) {
            self.visit_children(ctx)
        }

}