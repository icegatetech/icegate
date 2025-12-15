#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#![allow(nonstandard_style)]
// Generated from antlr/LogQLParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::{ParseTreeVisitor,ParseTreeVisitorCompat};
use super::logqlparser::*;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link LogQLParser}.
 */
pub trait LogQLParserVisitor<'input>: ParseTreeVisitor<'input,LogQLParserContextType>{
	/**
	 * Visit a parse tree produced by {@link LogQLParser#root}.
	 * @param ctx the parse tree
	 */
	fn visit_root(&mut self, ctx: &RootContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#expr}.
	 * @param ctx the parse tree
	 */
	fn visit_expr(&mut self, ctx: &ExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logExprWithSelectorOnly}
	 * labeled alternative in {@link LogQLParser#logExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_logExprWithSelectorOnly(&mut self, ctx: &LogExprWithSelectorOnlyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logExprWithPipeline}
	 * labeled alternative in {@link LogQLParser#logExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_logExprWithPipeline(&mut self, ctx: &LogExprWithPipelineContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#selector}.
	 * @param ctx the parse tree
	 */
	fn visit_selector(&mut self, ctx: &SelectorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#matchers}.
	 * @param ctx the parse tree
	 */
	fn visit_matchers(&mut self, ctx: &MatchersContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code matcherEq}
	 * labeled alternative in {@link LogQLParser#matcher}.
	 * @param ctx the parse tree
	 */
	fn visit_matcherEq(&mut self, ctx: &MatcherEqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code matcherNeq}
	 * labeled alternative in {@link LogQLParser#matcher}.
	 * @param ctx the parse tree
	 */
	fn visit_matcherNeq(&mut self, ctx: &MatcherNeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code matcherRe}
	 * labeled alternative in {@link LogQLParser#matcher}.
	 * @param ctx the parse tree
	 */
	fn visit_matcherRe(&mut self, ctx: &MatcherReContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code matcherNre}
	 * labeled alternative in {@link LogQLParser#matcher}.
	 * @param ctx the parse tree
	 */
	fn visit_matcherNre(&mut self, ctx: &MatcherNreContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#pipelineExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_pipelineExpr(&mut self, ctx: &PipelineExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#pipelineStage}.
	 * @param ctx the parse tree
	 */
	fn visit_pipelineStage(&mut self, ctx: &PipelineStageContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lineFiltersContains}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
	fn visit_lineFiltersContains(&mut self, ctx: &LineFiltersContainsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lineFiltersNotContains}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
	fn visit_lineFiltersNotContains(&mut self, ctx: &LineFiltersNotContainsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lineFiltersMatch}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
	fn visit_lineFiltersMatch(&mut self, ctx: &LineFiltersMatchContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lineFiltersNotMatch}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
	fn visit_lineFiltersNotMatch(&mut self, ctx: &LineFiltersNotMatchContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lineFiltersPattern}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
	fn visit_lineFiltersPattern(&mut self, ctx: &LineFiltersPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lineFiltersNotPattern}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
	fn visit_lineFiltersNotPattern(&mut self, ctx: &LineFiltersNotPatternContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lineFilterString}
	 * labeled alternative in {@link LogQLParser#lineFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_lineFilterString(&mut self, ctx: &LineFilterStringContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lineFilterIp}
	 * labeled alternative in {@link LogQLParser#lineFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_lineFilterIp(&mut self, ctx: &LineFilterIpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#ipFn}.
	 * @param ctx the parse tree
	 */
	fn visit_ipFn(&mut self, ctx: &IpFnContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#regexpParser}.
	 * @param ctx the parse tree
	 */
	fn visit_regexpParser(&mut self, ctx: &RegexpParserContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#patternParser}.
	 * @param ctx the parse tree
	 */
	fn visit_patternParser(&mut self, ctx: &PatternParserContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#unpackParser}.
	 * @param ctx the parse tree
	 */
	fn visit_unpackParser(&mut self, ctx: &UnpackParserContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#logfmtParser}.
	 * @param ctx the parse tree
	 */
	fn visit_logfmtParser(&mut self, ctx: &LogfmtParserContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#labelFormatExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFormatExpr(&mut self, ctx: &LabelFormatExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#labelFormatOps}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFormatOps(&mut self, ctx: &LabelFormatOpsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFormatRename}
	 * labeled alternative in {@link LogQLParser#labelFormatOp}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFormatRename(&mut self, ctx: &LabelFormatRenameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFormatTemplate}
	 * labeled alternative in {@link LogQLParser#labelFormatOp}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFormatTemplate(&mut self, ctx: &LabelFormatTemplateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#lineFormatExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_lineFormatExpr(&mut self, ctx: &LineFormatExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#decolorizeExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_decolorizeExpr(&mut self, ctx: &DecolorizeExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#dropExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_dropExpr(&mut self, ctx: &DropExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#keepExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_keepExpr(&mut self, ctx: &KeepExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#jsonParser}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonParser(&mut self, ctx: &JsonParserContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelExtractionWithPath}
	 * labeled alternative in {@link LogQLParser#labelExtractionExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_labelExtractionWithPath(&mut self, ctx: &LabelExtractionWithPathContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelExtractionSimple}
	 * labeled alternative in {@link LogQLParser#labelExtractionExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_labelExtractionSimple(&mut self, ctx: &LabelExtractionSimpleContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#labelExtractions}.
	 * @param ctx the parse tree
	 */
	fn visit_labelExtractions(&mut self, ctx: &LabelExtractionsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFilterBytes}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFilterBytes(&mut self, ctx: &LabelFilterBytesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFilterDuration}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFilterDuration(&mut self, ctx: &LabelFilterDurationContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFilterParens}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFilterParens(&mut self, ctx: &LabelFilterParensContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFilterOr}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFilterOr(&mut self, ctx: &LabelFilterOrContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFilterMatcher}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFilterMatcher(&mut self, ctx: &LabelFilterMatcherContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFilterIp}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFilterIp(&mut self, ctx: &LabelFilterIpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFilterNumber}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFilterNumber(&mut self, ctx: &LabelFilterNumberContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code labelFilterAnd}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_labelFilterAnd(&mut self, ctx: &LabelFilterAndContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#numberFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_numberFilter(&mut self, ctx: &NumberFilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#durationFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_durationFilter(&mut self, ctx: &DurationFilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#bytesFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_bytesFilter(&mut self, ctx: &BytesFilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#ipLabelFilter}.
	 * @param ctx the parse tree
	 */
	fn visit_ipLabelFilter(&mut self, ctx: &IpLabelFilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#comparisonOp}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonOp(&mut self, ctx: &ComparisonOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code metricExprParens}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_metricExprParens(&mut self, ctx: &MetricExprParensContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code metricExprLabelReplace}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_metricExprLabelReplace(&mut self, ctx: &MetricExprLabelReplaceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code metricExprVariable}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_metricExprVariable(&mut self, ctx: &MetricExprVariableContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpGe}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpGe(&mut self, ctx: &BinaryOpGeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpPow}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpPow(&mut self, ctx: &BinaryOpPowContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpUnless}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpUnless(&mut self, ctx: &BinaryOpUnlessContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code metricExprRangeAgg}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_metricExprRangeAgg(&mut self, ctx: &MetricExprRangeAggContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpAnd}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpAnd(&mut self, ctx: &BinaryOpAndContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpOr}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpOr(&mut self, ctx: &BinaryOpOrContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpLt}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpLt(&mut self, ctx: &BinaryOpLtContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code metricExprLiteral}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_metricExprLiteral(&mut self, ctx: &MetricExprLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpNeq}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpNeq(&mut self, ctx: &BinaryOpNeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpAdd}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpAdd(&mut self, ctx: &BinaryOpAddContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpGt}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpGt(&mut self, ctx: &BinaryOpGtContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpSub}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpSub(&mut self, ctx: &BinaryOpSubContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpEql}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpEql(&mut self, ctx: &BinaryOpEqlContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpMul}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpMul(&mut self, ctx: &BinaryOpMulContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code metricExprVector}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_metricExprVector(&mut self, ctx: &MetricExprVectorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpMod}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpMod(&mut self, ctx: &BinaryOpModContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpLe}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpLe(&mut self, ctx: &BinaryOpLeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code metricExprVectorAgg}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_metricExprVectorAgg(&mut self, ctx: &MetricExprVectorAggContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryOpDiv}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryOpDiv(&mut self, ctx: &BinaryOpDivContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#rangeAggregationExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeAggregationExpr(&mut self, ctx: &RangeAggregationExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpCount}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeLogOpCount(&mut self, ctx: &RangeLogOpCountContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpRate}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeLogOpRate(&mut self, ctx: &RangeLogOpRateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpBytes}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeLogOpBytes(&mut self, ctx: &RangeLogOpBytesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpBytesRate}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeLogOpBytesRate(&mut self, ctx: &RangeLogOpBytesRateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpAbsent}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeLogOpAbsent(&mut self, ctx: &RangeLogOpAbsentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpNoGroupSum}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpNoGroupSum(&mut self, ctx: &RangeUnwrapOpNoGroupSumContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpNoGroupRate}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpNoGroupRate(&mut self, ctx: &RangeUnwrapOpNoGroupRateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpNoGroupRateCounter}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpNoGroupRateCounter(&mut self, ctx: &RangeUnwrapOpNoGroupRateCounterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpAvg}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpAvg(&mut self, ctx: &RangeUnwrapOpAvgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpMin}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpMin(&mut self, ctx: &RangeUnwrapOpMinContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpMax}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpMax(&mut self, ctx: &RangeUnwrapOpMaxContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpStddev}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpStddev(&mut self, ctx: &RangeUnwrapOpStddevContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpStdvar}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpStdvar(&mut self, ctx: &RangeUnwrapOpStdvarContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpQuantile}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpQuantile(&mut self, ctx: &RangeUnwrapOpQuantileContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpFirst}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpFirst(&mut self, ctx: &RangeUnwrapOpFirstContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpLast}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeUnwrapOpLast(&mut self, ctx: &RangeUnwrapOpLastContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#vectorAggregationExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorAggregationExpr(&mut self, ctx: &VectorAggregationExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#vectorOp}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorOp(&mut self, ctx: &VectorOpContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#binOpModifier}.
	 * @param ctx the parse tree
	 */
	fn visit_binOpModifier(&mut self, ctx: &BinOpModifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#onOrIgnoringModifier}.
	 * @param ctx the parse tree
	 */
	fn visit_onOrIgnoringModifier(&mut self, ctx: &OnOrIgnoringModifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupingBy}
	 * labeled alternative in {@link LogQLParser#grouping}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingBy(&mut self, ctx: &GroupingByContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupingWithout}
	 * labeled alternative in {@link LogQLParser#grouping}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingWithout(&mut self, ctx: &GroupingWithoutContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupingByEmpty}
	 * labeled alternative in {@link LogQLParser#grouping}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingByEmpty(&mut self, ctx: &GroupingByEmptyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupingWithoutEmpty}
	 * labeled alternative in {@link LogQLParser#grouping}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingWithoutEmpty(&mut self, ctx: &GroupingWithoutEmptyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#binOpGroupingLabels}.
	 * @param ctx the parse tree
	 */
	fn visit_binOpGroupingLabels(&mut self, ctx: &BinOpGroupingLabelsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#groupingLabelList}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingLabelList(&mut self, ctx: &GroupingLabelListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#groupingLabel}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingLabel(&mut self, ctx: &GroupingLabelContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#groupingLabels}.
	 * @param ctx the parse tree
	 */
	fn visit_groupingLabels(&mut self, ctx: &GroupingLabelsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#logRangeExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_logRangeExpr(&mut self, ctx: &LogRangeExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#unwrappedRangeExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_unwrappedRangeExpr(&mut self, ctx: &UnwrappedRangeExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#range}.
	 * @param ctx the parse tree
	 */
	fn visit_range(&mut self, ctx: &RangeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#offsetExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_offsetExpr(&mut self, ctx: &OffsetExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#atModifier}.
	 * @param ctx the parse tree
	 */
	fn visit_atModifier(&mut self, ctx: &AtModifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unwrapWithFilter}
	 * labeled alternative in {@link LogQLParser#unwrapExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_unwrapWithFilter(&mut self, ctx: &UnwrapWithFilterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unwrapWithConversion}
	 * labeled alternative in {@link LogQLParser#unwrapExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_unwrapWithConversion(&mut self, ctx: &UnwrapWithConversionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code unwrapBasic}
	 * labeled alternative in {@link LogQLParser#unwrapExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_unwrapBasic(&mut self, ctx: &UnwrapBasicContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code literalNumber}
	 * labeled alternative in {@link LogQLParser#literalExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_literalNumber(&mut self, ctx: &LiteralNumberContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code literalPositiveNumber}
	 * labeled alternative in {@link LogQLParser#literalExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_literalPositiveNumber(&mut self, ctx: &LiteralPositiveNumberContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code literalNegativeNumber}
	 * labeled alternative in {@link LogQLParser#literalExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_literalNegativeNumber(&mut self, ctx: &LiteralNegativeNumberContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#labelReplaceExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_labelReplaceExpr(&mut self, ctx: &LabelReplaceExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#vectorExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_vectorExpr(&mut self, ctx: &VectorExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#variableExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_variableExpr(&mut self, ctx: &VariableExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LogQLParser#duration}.
	 * @param ctx the parse tree
	 */
	fn visit_duration(&mut self, ctx: &DurationContext<'input>) { self.visit_children(ctx) }

}

pub trait LogQLParserVisitorCompat<'input>:ParseTreeVisitorCompat<'input, Node= LogQLParserContextType>{
	/**
	 * Visit a parse tree produced by {@link LogQLParser#root}.
	 * @param ctx the parse tree
	 */
		fn visit_root(&mut self, ctx: &RootContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#expr}.
	 * @param ctx the parse tree
	 */
		fn visit_expr(&mut self, ctx: &ExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logExprWithSelectorOnly}
	 * labeled alternative in {@link LogQLParser#logExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_logExprWithSelectorOnly(&mut self, ctx: &LogExprWithSelectorOnlyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logExprWithPipeline}
	 * labeled alternative in {@link LogQLParser#logExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_logExprWithPipeline(&mut self, ctx: &LogExprWithPipelineContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#selector}.
	 * @param ctx the parse tree
	 */
		fn visit_selector(&mut self, ctx: &SelectorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#matchers}.
	 * @param ctx the parse tree
	 */
		fn visit_matchers(&mut self, ctx: &MatchersContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code matcherEq}
	 * labeled alternative in {@link LogQLParser#matcher}.
	 * @param ctx the parse tree
	 */
		fn visit_matcherEq(&mut self, ctx: &MatcherEqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code matcherNeq}
	 * labeled alternative in {@link LogQLParser#matcher}.
	 * @param ctx the parse tree
	 */
		fn visit_matcherNeq(&mut self, ctx: &MatcherNeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code matcherRe}
	 * labeled alternative in {@link LogQLParser#matcher}.
	 * @param ctx the parse tree
	 */
		fn visit_matcherRe(&mut self, ctx: &MatcherReContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code matcherNre}
	 * labeled alternative in {@link LogQLParser#matcher}.
	 * @param ctx the parse tree
	 */
		fn visit_matcherNre(&mut self, ctx: &MatcherNreContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#pipelineExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_pipelineExpr(&mut self, ctx: &PipelineExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#pipelineStage}.
	 * @param ctx the parse tree
	 */
		fn visit_pipelineStage(&mut self, ctx: &PipelineStageContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lineFiltersContains}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
		fn visit_lineFiltersContains(&mut self, ctx: &LineFiltersContainsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lineFiltersNotContains}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
		fn visit_lineFiltersNotContains(&mut self, ctx: &LineFiltersNotContainsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lineFiltersMatch}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
		fn visit_lineFiltersMatch(&mut self, ctx: &LineFiltersMatchContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lineFiltersNotMatch}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
		fn visit_lineFiltersNotMatch(&mut self, ctx: &LineFiltersNotMatchContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lineFiltersPattern}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
		fn visit_lineFiltersPattern(&mut self, ctx: &LineFiltersPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lineFiltersNotPattern}
	 * labeled alternative in {@link LogQLParser#lineFilters}.
	 * @param ctx the parse tree
	 */
		fn visit_lineFiltersNotPattern(&mut self, ctx: &LineFiltersNotPatternContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lineFilterString}
	 * labeled alternative in {@link LogQLParser#lineFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_lineFilterString(&mut self, ctx: &LineFilterStringContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lineFilterIp}
	 * labeled alternative in {@link LogQLParser#lineFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_lineFilterIp(&mut self, ctx: &LineFilterIpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#ipFn}.
	 * @param ctx the parse tree
	 */
		fn visit_ipFn(&mut self, ctx: &IpFnContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#regexpParser}.
	 * @param ctx the parse tree
	 */
		fn visit_regexpParser(&mut self, ctx: &RegexpParserContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#patternParser}.
	 * @param ctx the parse tree
	 */
		fn visit_patternParser(&mut self, ctx: &PatternParserContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#unpackParser}.
	 * @param ctx the parse tree
	 */
		fn visit_unpackParser(&mut self, ctx: &UnpackParserContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#logfmtParser}.
	 * @param ctx the parse tree
	 */
		fn visit_logfmtParser(&mut self, ctx: &LogfmtParserContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#labelFormatExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFormatExpr(&mut self, ctx: &LabelFormatExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#labelFormatOps}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFormatOps(&mut self, ctx: &LabelFormatOpsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFormatRename}
	 * labeled alternative in {@link LogQLParser#labelFormatOp}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFormatRename(&mut self, ctx: &LabelFormatRenameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFormatTemplate}
	 * labeled alternative in {@link LogQLParser#labelFormatOp}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFormatTemplate(&mut self, ctx: &LabelFormatTemplateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#lineFormatExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_lineFormatExpr(&mut self, ctx: &LineFormatExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#decolorizeExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_decolorizeExpr(&mut self, ctx: &DecolorizeExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#dropExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_dropExpr(&mut self, ctx: &DropExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#keepExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_keepExpr(&mut self, ctx: &KeepExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#jsonParser}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonParser(&mut self, ctx: &JsonParserContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelExtractionWithPath}
	 * labeled alternative in {@link LogQLParser#labelExtractionExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_labelExtractionWithPath(&mut self, ctx: &LabelExtractionWithPathContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelExtractionSimple}
	 * labeled alternative in {@link LogQLParser#labelExtractionExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_labelExtractionSimple(&mut self, ctx: &LabelExtractionSimpleContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#labelExtractions}.
	 * @param ctx the parse tree
	 */
		fn visit_labelExtractions(&mut self, ctx: &LabelExtractionsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFilterBytes}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFilterBytes(&mut self, ctx: &LabelFilterBytesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFilterDuration}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFilterDuration(&mut self, ctx: &LabelFilterDurationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFilterParens}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFilterParens(&mut self, ctx: &LabelFilterParensContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFilterOr}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFilterOr(&mut self, ctx: &LabelFilterOrContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFilterMatcher}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFilterMatcher(&mut self, ctx: &LabelFilterMatcherContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFilterIp}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFilterIp(&mut self, ctx: &LabelFilterIpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFilterNumber}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFilterNumber(&mut self, ctx: &LabelFilterNumberContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code labelFilterAnd}
	 * labeled alternative in {@link LogQLParser#labelFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_labelFilterAnd(&mut self, ctx: &LabelFilterAndContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#numberFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_numberFilter(&mut self, ctx: &NumberFilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#durationFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_durationFilter(&mut self, ctx: &DurationFilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#bytesFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_bytesFilter(&mut self, ctx: &BytesFilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#ipLabelFilter}.
	 * @param ctx the parse tree
	 */
		fn visit_ipLabelFilter(&mut self, ctx: &IpLabelFilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#comparisonOp}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonOp(&mut self, ctx: &ComparisonOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code metricExprParens}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_metricExprParens(&mut self, ctx: &MetricExprParensContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code metricExprLabelReplace}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_metricExprLabelReplace(&mut self, ctx: &MetricExprLabelReplaceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code metricExprVariable}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_metricExprVariable(&mut self, ctx: &MetricExprVariableContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpGe}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpGe(&mut self, ctx: &BinaryOpGeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpPow}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpPow(&mut self, ctx: &BinaryOpPowContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpUnless}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpUnless(&mut self, ctx: &BinaryOpUnlessContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code metricExprRangeAgg}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_metricExprRangeAgg(&mut self, ctx: &MetricExprRangeAggContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpAnd}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpAnd(&mut self, ctx: &BinaryOpAndContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpOr}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpOr(&mut self, ctx: &BinaryOpOrContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpLt}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpLt(&mut self, ctx: &BinaryOpLtContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code metricExprLiteral}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_metricExprLiteral(&mut self, ctx: &MetricExprLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpNeq}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpNeq(&mut self, ctx: &BinaryOpNeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpAdd}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpAdd(&mut self, ctx: &BinaryOpAddContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpGt}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpGt(&mut self, ctx: &BinaryOpGtContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpSub}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpSub(&mut self, ctx: &BinaryOpSubContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpEql}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpEql(&mut self, ctx: &BinaryOpEqlContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpMul}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpMul(&mut self, ctx: &BinaryOpMulContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code metricExprVector}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_metricExprVector(&mut self, ctx: &MetricExprVectorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpMod}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpMod(&mut self, ctx: &BinaryOpModContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpLe}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpLe(&mut self, ctx: &BinaryOpLeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code metricExprVectorAgg}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_metricExprVectorAgg(&mut self, ctx: &MetricExprVectorAggContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryOpDiv}
	 * labeled alternative in {@link LogQLParser#metricExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryOpDiv(&mut self, ctx: &BinaryOpDivContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#rangeAggregationExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeAggregationExpr(&mut self, ctx: &RangeAggregationExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpCount}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeLogOpCount(&mut self, ctx: &RangeLogOpCountContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpRate}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeLogOpRate(&mut self, ctx: &RangeLogOpRateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpBytes}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeLogOpBytes(&mut self, ctx: &RangeLogOpBytesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpBytesRate}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeLogOpBytesRate(&mut self, ctx: &RangeLogOpBytesRateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeLogOpAbsent}
	 * labeled alternative in {@link LogQLParser#rangeLogOp}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeLogOpAbsent(&mut self, ctx: &RangeLogOpAbsentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpNoGroupSum}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpNoGroupSum(&mut self, ctx: &RangeUnwrapOpNoGroupSumContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpNoGroupRate}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpNoGroupRate(&mut self, ctx: &RangeUnwrapOpNoGroupRateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpNoGroupRateCounter}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpNoGroupRateCounter(&mut self, ctx: &RangeUnwrapOpNoGroupRateCounterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpAvg}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpAvg(&mut self, ctx: &RangeUnwrapOpAvgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpMin}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpMin(&mut self, ctx: &RangeUnwrapOpMinContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpMax}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpMax(&mut self, ctx: &RangeUnwrapOpMaxContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpStddev}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpStddev(&mut self, ctx: &RangeUnwrapOpStddevContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpStdvar}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpStdvar(&mut self, ctx: &RangeUnwrapOpStdvarContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpQuantile}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpQuantile(&mut self, ctx: &RangeUnwrapOpQuantileContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpFirst}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpFirst(&mut self, ctx: &RangeUnwrapOpFirstContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeUnwrapOpLast}
	 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeUnwrapOpLast(&mut self, ctx: &RangeUnwrapOpLastContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#vectorAggregationExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorAggregationExpr(&mut self, ctx: &VectorAggregationExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#vectorOp}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorOp(&mut self, ctx: &VectorOpContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#binOpModifier}.
	 * @param ctx the parse tree
	 */
		fn visit_binOpModifier(&mut self, ctx: &BinOpModifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#onOrIgnoringModifier}.
	 * @param ctx the parse tree
	 */
		fn visit_onOrIgnoringModifier(&mut self, ctx: &OnOrIgnoringModifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupingBy}
	 * labeled alternative in {@link LogQLParser#grouping}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingBy(&mut self, ctx: &GroupingByContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupingWithout}
	 * labeled alternative in {@link LogQLParser#grouping}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingWithout(&mut self, ctx: &GroupingWithoutContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupingByEmpty}
	 * labeled alternative in {@link LogQLParser#grouping}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingByEmpty(&mut self, ctx: &GroupingByEmptyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupingWithoutEmpty}
	 * labeled alternative in {@link LogQLParser#grouping}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingWithoutEmpty(&mut self, ctx: &GroupingWithoutEmptyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#binOpGroupingLabels}.
	 * @param ctx the parse tree
	 */
		fn visit_binOpGroupingLabels(&mut self, ctx: &BinOpGroupingLabelsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#groupingLabelList}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingLabelList(&mut self, ctx: &GroupingLabelListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#groupingLabel}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingLabel(&mut self, ctx: &GroupingLabelContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#groupingLabels}.
	 * @param ctx the parse tree
	 */
		fn visit_groupingLabels(&mut self, ctx: &GroupingLabelsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#logRangeExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_logRangeExpr(&mut self, ctx: &LogRangeExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#unwrappedRangeExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_unwrappedRangeExpr(&mut self, ctx: &UnwrappedRangeExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#range}.
	 * @param ctx the parse tree
	 */
		fn visit_range(&mut self, ctx: &RangeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#offsetExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_offsetExpr(&mut self, ctx: &OffsetExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#atModifier}.
	 * @param ctx the parse tree
	 */
		fn visit_atModifier(&mut self, ctx: &AtModifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unwrapWithFilter}
	 * labeled alternative in {@link LogQLParser#unwrapExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_unwrapWithFilter(&mut self, ctx: &UnwrapWithFilterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unwrapWithConversion}
	 * labeled alternative in {@link LogQLParser#unwrapExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_unwrapWithConversion(&mut self, ctx: &UnwrapWithConversionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code unwrapBasic}
	 * labeled alternative in {@link LogQLParser#unwrapExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_unwrapBasic(&mut self, ctx: &UnwrapBasicContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code literalNumber}
	 * labeled alternative in {@link LogQLParser#literalExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_literalNumber(&mut self, ctx: &LiteralNumberContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code literalPositiveNumber}
	 * labeled alternative in {@link LogQLParser#literalExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_literalPositiveNumber(&mut self, ctx: &LiteralPositiveNumberContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code literalNegativeNumber}
	 * labeled alternative in {@link LogQLParser#literalExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_literalNegativeNumber(&mut self, ctx: &LiteralNegativeNumberContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#labelReplaceExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_labelReplaceExpr(&mut self, ctx: &LabelReplaceExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#vectorExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_vectorExpr(&mut self, ctx: &VectorExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#variableExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_variableExpr(&mut self, ctx: &VariableExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LogQLParser#duration}.
	 * @param ctx the parse tree
	 */
		fn visit_duration(&mut self, ctx: &DurationContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

}

impl<'input,T> LogQLParserVisitor<'input> for T
where
	T: LogQLParserVisitorCompat<'input>
{
	fn visit_root(&mut self, ctx: &RootContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_root(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expr(&mut self, ctx: &ExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_expr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logExprWithSelectorOnly(&mut self, ctx: &LogExprWithSelectorOnlyContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_logExprWithSelectorOnly(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logExprWithPipeline(&mut self, ctx: &LogExprWithPipelineContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_logExprWithPipeline(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selector(&mut self, ctx: &SelectorContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_selector(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_matchers(&mut self, ctx: &MatchersContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_matchers(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_matcherEq(&mut self, ctx: &MatcherEqContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_matcherEq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_matcherNeq(&mut self, ctx: &MatcherNeqContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_matcherNeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_matcherRe(&mut self, ctx: &MatcherReContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_matcherRe(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_matcherNre(&mut self, ctx: &MatcherNreContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_matcherNre(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pipelineExpr(&mut self, ctx: &PipelineExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_pipelineExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pipelineStage(&mut self, ctx: &PipelineStageContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_pipelineStage(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lineFiltersContains(&mut self, ctx: &LineFiltersContainsContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_lineFiltersContains(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lineFiltersNotContains(&mut self, ctx: &LineFiltersNotContainsContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_lineFiltersNotContains(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lineFiltersMatch(&mut self, ctx: &LineFiltersMatchContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_lineFiltersMatch(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lineFiltersNotMatch(&mut self, ctx: &LineFiltersNotMatchContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_lineFiltersNotMatch(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lineFiltersPattern(&mut self, ctx: &LineFiltersPatternContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_lineFiltersPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lineFiltersNotPattern(&mut self, ctx: &LineFiltersNotPatternContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_lineFiltersNotPattern(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lineFilterString(&mut self, ctx: &LineFilterStringContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_lineFilterString(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lineFilterIp(&mut self, ctx: &LineFilterIpContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_lineFilterIp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_ipFn(&mut self, ctx: &IpFnContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_ipFn(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_regexpParser(&mut self, ctx: &RegexpParserContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_regexpParser(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternParser(&mut self, ctx: &PatternParserContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_patternParser(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unpackParser(&mut self, ctx: &UnpackParserContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_unpackParser(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logfmtParser(&mut self, ctx: &LogfmtParserContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_logfmtParser(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFormatExpr(&mut self, ctx: &LabelFormatExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFormatExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFormatOps(&mut self, ctx: &LabelFormatOpsContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFormatOps(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFormatRename(&mut self, ctx: &LabelFormatRenameContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFormatRename(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFormatTemplate(&mut self, ctx: &LabelFormatTemplateContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFormatTemplate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lineFormatExpr(&mut self, ctx: &LineFormatExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_lineFormatExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_decolorizeExpr(&mut self, ctx: &DecolorizeExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_decolorizeExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dropExpr(&mut self, ctx: &DropExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_dropExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_keepExpr(&mut self, ctx: &KeepExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_keepExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonParser(&mut self, ctx: &JsonParserContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_jsonParser(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelExtractionWithPath(&mut self, ctx: &LabelExtractionWithPathContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelExtractionWithPath(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelExtractionSimple(&mut self, ctx: &LabelExtractionSimpleContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelExtractionSimple(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelExtractions(&mut self, ctx: &LabelExtractionsContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelExtractions(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFilterBytes(&mut self, ctx: &LabelFilterBytesContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFilterBytes(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFilterDuration(&mut self, ctx: &LabelFilterDurationContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFilterDuration(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFilterParens(&mut self, ctx: &LabelFilterParensContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFilterParens(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFilterOr(&mut self, ctx: &LabelFilterOrContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFilterOr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFilterMatcher(&mut self, ctx: &LabelFilterMatcherContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFilterMatcher(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFilterIp(&mut self, ctx: &LabelFilterIpContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFilterIp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFilterNumber(&mut self, ctx: &LabelFilterNumberContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFilterNumber(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelFilterAnd(&mut self, ctx: &LabelFilterAndContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelFilterAnd(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_numberFilter(&mut self, ctx: &NumberFilterContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_numberFilter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_durationFilter(&mut self, ctx: &DurationFilterContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_durationFilter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bytesFilter(&mut self, ctx: &BytesFilterContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_bytesFilter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_ipLabelFilter(&mut self, ctx: &IpLabelFilterContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_ipLabelFilter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonOp(&mut self, ctx: &ComparisonOpContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_comparisonOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_metricExprParens(&mut self, ctx: &MetricExprParensContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_metricExprParens(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_metricExprLabelReplace(&mut self, ctx: &MetricExprLabelReplaceContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_metricExprLabelReplace(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_metricExprVariable(&mut self, ctx: &MetricExprVariableContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_metricExprVariable(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpGe(&mut self, ctx: &BinaryOpGeContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpGe(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpPow(&mut self, ctx: &BinaryOpPowContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpPow(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpUnless(&mut self, ctx: &BinaryOpUnlessContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpUnless(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_metricExprRangeAgg(&mut self, ctx: &MetricExprRangeAggContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_metricExprRangeAgg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpAnd(&mut self, ctx: &BinaryOpAndContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpAnd(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpOr(&mut self, ctx: &BinaryOpOrContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpOr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpLt(&mut self, ctx: &BinaryOpLtContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpLt(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_metricExprLiteral(&mut self, ctx: &MetricExprLiteralContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_metricExprLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpNeq(&mut self, ctx: &BinaryOpNeqContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpNeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpAdd(&mut self, ctx: &BinaryOpAddContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpAdd(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpGt(&mut self, ctx: &BinaryOpGtContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpGt(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpSub(&mut self, ctx: &BinaryOpSubContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpSub(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpEql(&mut self, ctx: &BinaryOpEqlContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpEql(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpMul(&mut self, ctx: &BinaryOpMulContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpMul(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_metricExprVector(&mut self, ctx: &MetricExprVectorContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_metricExprVector(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpMod(&mut self, ctx: &BinaryOpModContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpMod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpLe(&mut self, ctx: &BinaryOpLeContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpLe(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_metricExprVectorAgg(&mut self, ctx: &MetricExprVectorAggContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_metricExprVectorAgg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryOpDiv(&mut self, ctx: &BinaryOpDivContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binaryOpDiv(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeAggregationExpr(&mut self, ctx: &RangeAggregationExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeAggregationExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeLogOpCount(&mut self, ctx: &RangeLogOpCountContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeLogOpCount(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeLogOpRate(&mut self, ctx: &RangeLogOpRateContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeLogOpRate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeLogOpBytes(&mut self, ctx: &RangeLogOpBytesContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeLogOpBytes(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeLogOpBytesRate(&mut self, ctx: &RangeLogOpBytesRateContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeLogOpBytesRate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeLogOpAbsent(&mut self, ctx: &RangeLogOpAbsentContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeLogOpAbsent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpNoGroupSum(&mut self, ctx: &RangeUnwrapOpNoGroupSumContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpNoGroupSum(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpNoGroupRate(&mut self, ctx: &RangeUnwrapOpNoGroupRateContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpNoGroupRate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpNoGroupRateCounter(&mut self, ctx: &RangeUnwrapOpNoGroupRateCounterContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpNoGroupRateCounter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpAvg(&mut self, ctx: &RangeUnwrapOpAvgContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpAvg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpMin(&mut self, ctx: &RangeUnwrapOpMinContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpMin(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpMax(&mut self, ctx: &RangeUnwrapOpMaxContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpMax(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpStddev(&mut self, ctx: &RangeUnwrapOpStddevContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpStddev(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpStdvar(&mut self, ctx: &RangeUnwrapOpStdvarContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpStdvar(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpQuantile(&mut self, ctx: &RangeUnwrapOpQuantileContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpQuantile(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpFirst(&mut self, ctx: &RangeUnwrapOpFirstContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpFirst(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeUnwrapOpLast(&mut self, ctx: &RangeUnwrapOpLastContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_rangeUnwrapOpLast(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorAggregationExpr(&mut self, ctx: &VectorAggregationExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_vectorAggregationExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorOp(&mut self, ctx: &VectorOpContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_vectorOp(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binOpModifier(&mut self, ctx: &BinOpModifierContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binOpModifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_onOrIgnoringModifier(&mut self, ctx: &OnOrIgnoringModifierContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_onOrIgnoringModifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingBy(&mut self, ctx: &GroupingByContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_groupingBy(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingWithout(&mut self, ctx: &GroupingWithoutContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_groupingWithout(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingByEmpty(&mut self, ctx: &GroupingByEmptyContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_groupingByEmpty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingWithoutEmpty(&mut self, ctx: &GroupingWithoutEmptyContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_groupingWithoutEmpty(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binOpGroupingLabels(&mut self, ctx: &BinOpGroupingLabelsContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_binOpGroupingLabels(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingLabelList(&mut self, ctx: &GroupingLabelListContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_groupingLabelList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingLabel(&mut self, ctx: &GroupingLabelContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_groupingLabel(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupingLabels(&mut self, ctx: &GroupingLabelsContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_groupingLabels(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logRangeExpr(&mut self, ctx: &LogRangeExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_logRangeExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unwrappedRangeExpr(&mut self, ctx: &UnwrappedRangeExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_unwrappedRangeExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_range(&mut self, ctx: &RangeContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_range(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_offsetExpr(&mut self, ctx: &OffsetExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_offsetExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_atModifier(&mut self, ctx: &AtModifierContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_atModifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unwrapWithFilter(&mut self, ctx: &UnwrapWithFilterContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_unwrapWithFilter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unwrapWithConversion(&mut self, ctx: &UnwrapWithConversionContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_unwrapWithConversion(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unwrapBasic(&mut self, ctx: &UnwrapBasicContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_unwrapBasic(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_literalNumber(&mut self, ctx: &LiteralNumberContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_literalNumber(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_literalPositiveNumber(&mut self, ctx: &LiteralPositiveNumberContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_literalPositiveNumber(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_literalNegativeNumber(&mut self, ctx: &LiteralNegativeNumberContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_literalNegativeNumber(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_labelReplaceExpr(&mut self, ctx: &LabelReplaceExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_labelReplaceExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_vectorExpr(&mut self, ctx: &VectorExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_vectorExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_variableExpr(&mut self, ctx: &VariableExprContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_variableExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_duration(&mut self, ctx: &DurationContext<'input>){
		let result = <Self as LogQLParserVisitorCompat>::visit_duration(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

}