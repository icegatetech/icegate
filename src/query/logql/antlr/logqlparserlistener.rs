#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
#![allow(nonstandard_style)]
// Generated from antlr/LogQLParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::ParseTreeListener;
use super::logqlparser::*;

pub trait LogQLParserListener<'input> : ParseTreeListener<'input,LogQLParserContextType>{
/**
 * Enter a parse tree produced by {@link LogQLParser#root}.
 * @param ctx the parse tree
 */
fn enter_root(&mut self, _ctx: &RootContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#root}.
 * @param ctx the parse tree
 */
fn exit_root(&mut self, _ctx: &RootContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#expr}.
 * @param ctx the parse tree
 */
fn enter_expr(&mut self, _ctx: &ExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#expr}.
 * @param ctx the parse tree
 */
fn exit_expr(&mut self, _ctx: &ExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logExprWithSelectorOnly}
 * labeled alternative in {@link LogQLParser#logExpr}.
 * @param ctx the parse tree
 */
fn enter_logExprWithSelectorOnly(&mut self, _ctx: &LogExprWithSelectorOnlyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logExprWithSelectorOnly}
 * labeled alternative in {@link LogQLParser#logExpr}.
 * @param ctx the parse tree
 */
fn exit_logExprWithSelectorOnly(&mut self, _ctx: &LogExprWithSelectorOnlyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logExprWithPipeline}
 * labeled alternative in {@link LogQLParser#logExpr}.
 * @param ctx the parse tree
 */
fn enter_logExprWithPipeline(&mut self, _ctx: &LogExprWithPipelineContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logExprWithPipeline}
 * labeled alternative in {@link LogQLParser#logExpr}.
 * @param ctx the parse tree
 */
fn exit_logExprWithPipeline(&mut self, _ctx: &LogExprWithPipelineContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#selector}.
 * @param ctx the parse tree
 */
fn enter_selector(&mut self, _ctx: &SelectorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#selector}.
 * @param ctx the parse tree
 */
fn exit_selector(&mut self, _ctx: &SelectorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#matchers}.
 * @param ctx the parse tree
 */
fn enter_matchers(&mut self, _ctx: &MatchersContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#matchers}.
 * @param ctx the parse tree
 */
fn exit_matchers(&mut self, _ctx: &MatchersContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code matcherEq}
 * labeled alternative in {@link LogQLParser#matcher}.
 * @param ctx the parse tree
 */
fn enter_matcherEq(&mut self, _ctx: &MatcherEqContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code matcherEq}
 * labeled alternative in {@link LogQLParser#matcher}.
 * @param ctx the parse tree
 */
fn exit_matcherEq(&mut self, _ctx: &MatcherEqContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code matcherNeq}
 * labeled alternative in {@link LogQLParser#matcher}.
 * @param ctx the parse tree
 */
fn enter_matcherNeq(&mut self, _ctx: &MatcherNeqContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code matcherNeq}
 * labeled alternative in {@link LogQLParser#matcher}.
 * @param ctx the parse tree
 */
fn exit_matcherNeq(&mut self, _ctx: &MatcherNeqContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code matcherRe}
 * labeled alternative in {@link LogQLParser#matcher}.
 * @param ctx the parse tree
 */
fn enter_matcherRe(&mut self, _ctx: &MatcherReContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code matcherRe}
 * labeled alternative in {@link LogQLParser#matcher}.
 * @param ctx the parse tree
 */
fn exit_matcherRe(&mut self, _ctx: &MatcherReContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code matcherNre}
 * labeled alternative in {@link LogQLParser#matcher}.
 * @param ctx the parse tree
 */
fn enter_matcherNre(&mut self, _ctx: &MatcherNreContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code matcherNre}
 * labeled alternative in {@link LogQLParser#matcher}.
 * @param ctx the parse tree
 */
fn exit_matcherNre(&mut self, _ctx: &MatcherNreContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#pipelineExpr}.
 * @param ctx the parse tree
 */
fn enter_pipelineExpr(&mut self, _ctx: &PipelineExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#pipelineExpr}.
 * @param ctx the parse tree
 */
fn exit_pipelineExpr(&mut self, _ctx: &PipelineExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#pipelineStage}.
 * @param ctx the parse tree
 */
fn enter_pipelineStage(&mut self, _ctx: &PipelineStageContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#pipelineStage}.
 * @param ctx the parse tree
 */
fn exit_pipelineStage(&mut self, _ctx: &PipelineStageContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lineFiltersContains}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn enter_lineFiltersContains(&mut self, _ctx: &LineFiltersContainsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lineFiltersContains}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn exit_lineFiltersContains(&mut self, _ctx: &LineFiltersContainsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lineFiltersNotContains}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn enter_lineFiltersNotContains(&mut self, _ctx: &LineFiltersNotContainsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lineFiltersNotContains}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn exit_lineFiltersNotContains(&mut self, _ctx: &LineFiltersNotContainsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lineFiltersMatch}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn enter_lineFiltersMatch(&mut self, _ctx: &LineFiltersMatchContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lineFiltersMatch}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn exit_lineFiltersMatch(&mut self, _ctx: &LineFiltersMatchContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lineFiltersNotMatch}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn enter_lineFiltersNotMatch(&mut self, _ctx: &LineFiltersNotMatchContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lineFiltersNotMatch}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn exit_lineFiltersNotMatch(&mut self, _ctx: &LineFiltersNotMatchContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lineFiltersPattern}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn enter_lineFiltersPattern(&mut self, _ctx: &LineFiltersPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lineFiltersPattern}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn exit_lineFiltersPattern(&mut self, _ctx: &LineFiltersPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lineFiltersNotPattern}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn enter_lineFiltersNotPattern(&mut self, _ctx: &LineFiltersNotPatternContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lineFiltersNotPattern}
 * labeled alternative in {@link LogQLParser#lineFilters}.
 * @param ctx the parse tree
 */
fn exit_lineFiltersNotPattern(&mut self, _ctx: &LineFiltersNotPatternContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lineFilterString}
 * labeled alternative in {@link LogQLParser#lineFilter}.
 * @param ctx the parse tree
 */
fn enter_lineFilterString(&mut self, _ctx: &LineFilterStringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lineFilterString}
 * labeled alternative in {@link LogQLParser#lineFilter}.
 * @param ctx the parse tree
 */
fn exit_lineFilterString(&mut self, _ctx: &LineFilterStringContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lineFilterIp}
 * labeled alternative in {@link LogQLParser#lineFilter}.
 * @param ctx the parse tree
 */
fn enter_lineFilterIp(&mut self, _ctx: &LineFilterIpContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lineFilterIp}
 * labeled alternative in {@link LogQLParser#lineFilter}.
 * @param ctx the parse tree
 */
fn exit_lineFilterIp(&mut self, _ctx: &LineFilterIpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#ipFn}.
 * @param ctx the parse tree
 */
fn enter_ipFn(&mut self, _ctx: &IpFnContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#ipFn}.
 * @param ctx the parse tree
 */
fn exit_ipFn(&mut self, _ctx: &IpFnContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#regexpParser}.
 * @param ctx the parse tree
 */
fn enter_regexpParser(&mut self, _ctx: &RegexpParserContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#regexpParser}.
 * @param ctx the parse tree
 */
fn exit_regexpParser(&mut self, _ctx: &RegexpParserContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#patternParser}.
 * @param ctx the parse tree
 */
fn enter_patternParser(&mut self, _ctx: &PatternParserContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#patternParser}.
 * @param ctx the parse tree
 */
fn exit_patternParser(&mut self, _ctx: &PatternParserContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#unpackParser}.
 * @param ctx the parse tree
 */
fn enter_unpackParser(&mut self, _ctx: &UnpackParserContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#unpackParser}.
 * @param ctx the parse tree
 */
fn exit_unpackParser(&mut self, _ctx: &UnpackParserContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#logfmtParser}.
 * @param ctx the parse tree
 */
fn enter_logfmtParser(&mut self, _ctx: &LogfmtParserContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#logfmtParser}.
 * @param ctx the parse tree
 */
fn exit_logfmtParser(&mut self, _ctx: &LogfmtParserContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#labelFormatExpr}.
 * @param ctx the parse tree
 */
fn enter_labelFormatExpr(&mut self, _ctx: &LabelFormatExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#labelFormatExpr}.
 * @param ctx the parse tree
 */
fn exit_labelFormatExpr(&mut self, _ctx: &LabelFormatExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#labelFormatOps}.
 * @param ctx the parse tree
 */
fn enter_labelFormatOps(&mut self, _ctx: &LabelFormatOpsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#labelFormatOps}.
 * @param ctx the parse tree
 */
fn exit_labelFormatOps(&mut self, _ctx: &LabelFormatOpsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFormatRename}
 * labeled alternative in {@link LogQLParser#labelFormatOp}.
 * @param ctx the parse tree
 */
fn enter_labelFormatRename(&mut self, _ctx: &LabelFormatRenameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFormatRename}
 * labeled alternative in {@link LogQLParser#labelFormatOp}.
 * @param ctx the parse tree
 */
fn exit_labelFormatRename(&mut self, _ctx: &LabelFormatRenameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFormatTemplate}
 * labeled alternative in {@link LogQLParser#labelFormatOp}.
 * @param ctx the parse tree
 */
fn enter_labelFormatTemplate(&mut self, _ctx: &LabelFormatTemplateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFormatTemplate}
 * labeled alternative in {@link LogQLParser#labelFormatOp}.
 * @param ctx the parse tree
 */
fn exit_labelFormatTemplate(&mut self, _ctx: &LabelFormatTemplateContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#lineFormatExpr}.
 * @param ctx the parse tree
 */
fn enter_lineFormatExpr(&mut self, _ctx: &LineFormatExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#lineFormatExpr}.
 * @param ctx the parse tree
 */
fn exit_lineFormatExpr(&mut self, _ctx: &LineFormatExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#decolorizeExpr}.
 * @param ctx the parse tree
 */
fn enter_decolorizeExpr(&mut self, _ctx: &DecolorizeExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#decolorizeExpr}.
 * @param ctx the parse tree
 */
fn exit_decolorizeExpr(&mut self, _ctx: &DecolorizeExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#dropExpr}.
 * @param ctx the parse tree
 */
fn enter_dropExpr(&mut self, _ctx: &DropExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#dropExpr}.
 * @param ctx the parse tree
 */
fn exit_dropExpr(&mut self, _ctx: &DropExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#keepExpr}.
 * @param ctx the parse tree
 */
fn enter_keepExpr(&mut self, _ctx: &KeepExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#keepExpr}.
 * @param ctx the parse tree
 */
fn exit_keepExpr(&mut self, _ctx: &KeepExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#jsonParser}.
 * @param ctx the parse tree
 */
fn enter_jsonParser(&mut self, _ctx: &JsonParserContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#jsonParser}.
 * @param ctx the parse tree
 */
fn exit_jsonParser(&mut self, _ctx: &JsonParserContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelExtractionWithPath}
 * labeled alternative in {@link LogQLParser#labelExtractionExpr}.
 * @param ctx the parse tree
 */
fn enter_labelExtractionWithPath(&mut self, _ctx: &LabelExtractionWithPathContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelExtractionWithPath}
 * labeled alternative in {@link LogQLParser#labelExtractionExpr}.
 * @param ctx the parse tree
 */
fn exit_labelExtractionWithPath(&mut self, _ctx: &LabelExtractionWithPathContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelExtractionSimple}
 * labeled alternative in {@link LogQLParser#labelExtractionExpr}.
 * @param ctx the parse tree
 */
fn enter_labelExtractionSimple(&mut self, _ctx: &LabelExtractionSimpleContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelExtractionSimple}
 * labeled alternative in {@link LogQLParser#labelExtractionExpr}.
 * @param ctx the parse tree
 */
fn exit_labelExtractionSimple(&mut self, _ctx: &LabelExtractionSimpleContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#labelExtractions}.
 * @param ctx the parse tree
 */
fn enter_labelExtractions(&mut self, _ctx: &LabelExtractionsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#labelExtractions}.
 * @param ctx the parse tree
 */
fn exit_labelExtractions(&mut self, _ctx: &LabelExtractionsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFilterBytes}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn enter_labelFilterBytes(&mut self, _ctx: &LabelFilterBytesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFilterBytes}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn exit_labelFilterBytes(&mut self, _ctx: &LabelFilterBytesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFilterDuration}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn enter_labelFilterDuration(&mut self, _ctx: &LabelFilterDurationContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFilterDuration}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn exit_labelFilterDuration(&mut self, _ctx: &LabelFilterDurationContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFilterParens}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn enter_labelFilterParens(&mut self, _ctx: &LabelFilterParensContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFilterParens}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn exit_labelFilterParens(&mut self, _ctx: &LabelFilterParensContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFilterOr}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn enter_labelFilterOr(&mut self, _ctx: &LabelFilterOrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFilterOr}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn exit_labelFilterOr(&mut self, _ctx: &LabelFilterOrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFilterMatcher}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn enter_labelFilterMatcher(&mut self, _ctx: &LabelFilterMatcherContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFilterMatcher}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn exit_labelFilterMatcher(&mut self, _ctx: &LabelFilterMatcherContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFilterIp}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn enter_labelFilterIp(&mut self, _ctx: &LabelFilterIpContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFilterIp}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn exit_labelFilterIp(&mut self, _ctx: &LabelFilterIpContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFilterNumber}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn enter_labelFilterNumber(&mut self, _ctx: &LabelFilterNumberContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFilterNumber}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn exit_labelFilterNumber(&mut self, _ctx: &LabelFilterNumberContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code labelFilterAnd}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn enter_labelFilterAnd(&mut self, _ctx: &LabelFilterAndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code labelFilterAnd}
 * labeled alternative in {@link LogQLParser#labelFilter}.
 * @param ctx the parse tree
 */
fn exit_labelFilterAnd(&mut self, _ctx: &LabelFilterAndContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#numberFilter}.
 * @param ctx the parse tree
 */
fn enter_numberFilter(&mut self, _ctx: &NumberFilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#numberFilter}.
 * @param ctx the parse tree
 */
fn exit_numberFilter(&mut self, _ctx: &NumberFilterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#durationFilter}.
 * @param ctx the parse tree
 */
fn enter_durationFilter(&mut self, _ctx: &DurationFilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#durationFilter}.
 * @param ctx the parse tree
 */
fn exit_durationFilter(&mut self, _ctx: &DurationFilterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#bytesFilter}.
 * @param ctx the parse tree
 */
fn enter_bytesFilter(&mut self, _ctx: &BytesFilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#bytesFilter}.
 * @param ctx the parse tree
 */
fn exit_bytesFilter(&mut self, _ctx: &BytesFilterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#ipLabelFilter}.
 * @param ctx the parse tree
 */
fn enter_ipLabelFilter(&mut self, _ctx: &IpLabelFilterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#ipLabelFilter}.
 * @param ctx the parse tree
 */
fn exit_ipLabelFilter(&mut self, _ctx: &IpLabelFilterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#comparisonOp}.
 * @param ctx the parse tree
 */
fn enter_comparisonOp(&mut self, _ctx: &ComparisonOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#comparisonOp}.
 * @param ctx the parse tree
 */
fn exit_comparisonOp(&mut self, _ctx: &ComparisonOpContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code metricExprParens}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_metricExprParens(&mut self, _ctx: &MetricExprParensContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code metricExprParens}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_metricExprParens(&mut self, _ctx: &MetricExprParensContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code metricExprLabelReplace}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_metricExprLabelReplace(&mut self, _ctx: &MetricExprLabelReplaceContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code metricExprLabelReplace}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_metricExprLabelReplace(&mut self, _ctx: &MetricExprLabelReplaceContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code metricExprVariable}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_metricExprVariable(&mut self, _ctx: &MetricExprVariableContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code metricExprVariable}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_metricExprVariable(&mut self, _ctx: &MetricExprVariableContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpGe}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpGe(&mut self, _ctx: &BinaryOpGeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpGe}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpGe(&mut self, _ctx: &BinaryOpGeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpPow}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpPow(&mut self, _ctx: &BinaryOpPowContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpPow}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpPow(&mut self, _ctx: &BinaryOpPowContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpUnless}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpUnless(&mut self, _ctx: &BinaryOpUnlessContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpUnless}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpUnless(&mut self, _ctx: &BinaryOpUnlessContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code metricExprRangeAgg}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_metricExprRangeAgg(&mut self, _ctx: &MetricExprRangeAggContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code metricExprRangeAgg}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_metricExprRangeAgg(&mut self, _ctx: &MetricExprRangeAggContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpAnd}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpAnd(&mut self, _ctx: &BinaryOpAndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpAnd}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpAnd(&mut self, _ctx: &BinaryOpAndContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpOr}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpOr(&mut self, _ctx: &BinaryOpOrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpOr}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpOr(&mut self, _ctx: &BinaryOpOrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpLt}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpLt(&mut self, _ctx: &BinaryOpLtContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpLt}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpLt(&mut self, _ctx: &BinaryOpLtContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code metricExprLiteral}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_metricExprLiteral(&mut self, _ctx: &MetricExprLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code metricExprLiteral}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_metricExprLiteral(&mut self, _ctx: &MetricExprLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpNeq}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpNeq(&mut self, _ctx: &BinaryOpNeqContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpNeq}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpNeq(&mut self, _ctx: &BinaryOpNeqContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpAdd}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpAdd(&mut self, _ctx: &BinaryOpAddContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpAdd}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpAdd(&mut self, _ctx: &BinaryOpAddContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpGt}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpGt(&mut self, _ctx: &BinaryOpGtContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpGt}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpGt(&mut self, _ctx: &BinaryOpGtContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpSub}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpSub(&mut self, _ctx: &BinaryOpSubContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpSub}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpSub(&mut self, _ctx: &BinaryOpSubContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpEql}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpEql(&mut self, _ctx: &BinaryOpEqlContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpEql}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpEql(&mut self, _ctx: &BinaryOpEqlContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpMul}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpMul(&mut self, _ctx: &BinaryOpMulContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpMul}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpMul(&mut self, _ctx: &BinaryOpMulContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code metricExprVector}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_metricExprVector(&mut self, _ctx: &MetricExprVectorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code metricExprVector}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_metricExprVector(&mut self, _ctx: &MetricExprVectorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpMod}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpMod(&mut self, _ctx: &BinaryOpModContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpMod}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpMod(&mut self, _ctx: &BinaryOpModContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpLe}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpLe(&mut self, _ctx: &BinaryOpLeContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpLe}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpLe(&mut self, _ctx: &BinaryOpLeContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code metricExprVectorAgg}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_metricExprVectorAgg(&mut self, _ctx: &MetricExprVectorAggContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code metricExprVectorAgg}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_metricExprVectorAgg(&mut self, _ctx: &MetricExprVectorAggContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryOpDiv}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn enter_binaryOpDiv(&mut self, _ctx: &BinaryOpDivContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryOpDiv}
 * labeled alternative in {@link LogQLParser#metricExpr}.
 * @param ctx the parse tree
 */
fn exit_binaryOpDiv(&mut self, _ctx: &BinaryOpDivContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#rangeAggregationExpr}.
 * @param ctx the parse tree
 */
fn enter_rangeAggregationExpr(&mut self, _ctx: &RangeAggregationExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#rangeAggregationExpr}.
 * @param ctx the parse tree
 */
fn exit_rangeAggregationExpr(&mut self, _ctx: &RangeAggregationExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeLogOpCount}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn enter_rangeLogOpCount(&mut self, _ctx: &RangeLogOpCountContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeLogOpCount}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn exit_rangeLogOpCount(&mut self, _ctx: &RangeLogOpCountContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeLogOpRate}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn enter_rangeLogOpRate(&mut self, _ctx: &RangeLogOpRateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeLogOpRate}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn exit_rangeLogOpRate(&mut self, _ctx: &RangeLogOpRateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeLogOpBytes}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn enter_rangeLogOpBytes(&mut self, _ctx: &RangeLogOpBytesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeLogOpBytes}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn exit_rangeLogOpBytes(&mut self, _ctx: &RangeLogOpBytesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeLogOpBytesRate}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn enter_rangeLogOpBytesRate(&mut self, _ctx: &RangeLogOpBytesRateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeLogOpBytesRate}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn exit_rangeLogOpBytesRate(&mut self, _ctx: &RangeLogOpBytesRateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeLogOpAbsent}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn enter_rangeLogOpAbsent(&mut self, _ctx: &RangeLogOpAbsentContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeLogOpAbsent}
 * labeled alternative in {@link LogQLParser#rangeLogOp}.
 * @param ctx the parse tree
 */
fn exit_rangeLogOpAbsent(&mut self, _ctx: &RangeLogOpAbsentContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpNoGroupSum}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpNoGroupSum(&mut self, _ctx: &RangeUnwrapOpNoGroupSumContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpNoGroupSum}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpNoGroupSum(&mut self, _ctx: &RangeUnwrapOpNoGroupSumContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpNoGroupRate}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpNoGroupRate(&mut self, _ctx: &RangeUnwrapOpNoGroupRateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpNoGroupRate}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpNoGroupRate(&mut self, _ctx: &RangeUnwrapOpNoGroupRateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpNoGroupRateCounter}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpNoGroupRateCounter(&mut self, _ctx: &RangeUnwrapOpNoGroupRateCounterContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpNoGroupRateCounter}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpNoGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpNoGroupRateCounter(&mut self, _ctx: &RangeUnwrapOpNoGroupRateCounterContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpAvg}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpAvg(&mut self, _ctx: &RangeUnwrapOpAvgContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpAvg}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpAvg(&mut self, _ctx: &RangeUnwrapOpAvgContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpMin}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpMin(&mut self, _ctx: &RangeUnwrapOpMinContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpMin}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpMin(&mut self, _ctx: &RangeUnwrapOpMinContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpMax}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpMax(&mut self, _ctx: &RangeUnwrapOpMaxContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpMax}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpMax(&mut self, _ctx: &RangeUnwrapOpMaxContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpStddev}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpStddev(&mut self, _ctx: &RangeUnwrapOpStddevContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpStddev}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpStddev(&mut self, _ctx: &RangeUnwrapOpStddevContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpStdvar}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpStdvar(&mut self, _ctx: &RangeUnwrapOpStdvarContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpStdvar}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpStdvar(&mut self, _ctx: &RangeUnwrapOpStdvarContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpQuantile}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpQuantile(&mut self, _ctx: &RangeUnwrapOpQuantileContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpQuantile}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpQuantile(&mut self, _ctx: &RangeUnwrapOpQuantileContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpFirst}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpFirst(&mut self, _ctx: &RangeUnwrapOpFirstContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpFirst}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpFirst(&mut self, _ctx: &RangeUnwrapOpFirstContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeUnwrapOpLast}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn enter_rangeUnwrapOpLast(&mut self, _ctx: &RangeUnwrapOpLastContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeUnwrapOpLast}
 * labeled alternative in {@link LogQLParser#rangeUnwrapOpWithGrouping}.
 * @param ctx the parse tree
 */
fn exit_rangeUnwrapOpLast(&mut self, _ctx: &RangeUnwrapOpLastContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#vectorAggregationExpr}.
 * @param ctx the parse tree
 */
fn enter_vectorAggregationExpr(&mut self, _ctx: &VectorAggregationExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#vectorAggregationExpr}.
 * @param ctx the parse tree
 */
fn exit_vectorAggregationExpr(&mut self, _ctx: &VectorAggregationExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#vectorOp}.
 * @param ctx the parse tree
 */
fn enter_vectorOp(&mut self, _ctx: &VectorOpContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#vectorOp}.
 * @param ctx the parse tree
 */
fn exit_vectorOp(&mut self, _ctx: &VectorOpContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#binOpModifier}.
 * @param ctx the parse tree
 */
fn enter_binOpModifier(&mut self, _ctx: &BinOpModifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#binOpModifier}.
 * @param ctx the parse tree
 */
fn exit_binOpModifier(&mut self, _ctx: &BinOpModifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#onOrIgnoringModifier}.
 * @param ctx the parse tree
 */
fn enter_onOrIgnoringModifier(&mut self, _ctx: &OnOrIgnoringModifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#onOrIgnoringModifier}.
 * @param ctx the parse tree
 */
fn exit_onOrIgnoringModifier(&mut self, _ctx: &OnOrIgnoringModifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupingBy}
 * labeled alternative in {@link LogQLParser#grouping}.
 * @param ctx the parse tree
 */
fn enter_groupingBy(&mut self, _ctx: &GroupingByContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupingBy}
 * labeled alternative in {@link LogQLParser#grouping}.
 * @param ctx the parse tree
 */
fn exit_groupingBy(&mut self, _ctx: &GroupingByContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupingWithout}
 * labeled alternative in {@link LogQLParser#grouping}.
 * @param ctx the parse tree
 */
fn enter_groupingWithout(&mut self, _ctx: &GroupingWithoutContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupingWithout}
 * labeled alternative in {@link LogQLParser#grouping}.
 * @param ctx the parse tree
 */
fn exit_groupingWithout(&mut self, _ctx: &GroupingWithoutContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupingByEmpty}
 * labeled alternative in {@link LogQLParser#grouping}.
 * @param ctx the parse tree
 */
fn enter_groupingByEmpty(&mut self, _ctx: &GroupingByEmptyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupingByEmpty}
 * labeled alternative in {@link LogQLParser#grouping}.
 * @param ctx the parse tree
 */
fn exit_groupingByEmpty(&mut self, _ctx: &GroupingByEmptyContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupingWithoutEmpty}
 * labeled alternative in {@link LogQLParser#grouping}.
 * @param ctx the parse tree
 */
fn enter_groupingWithoutEmpty(&mut self, _ctx: &GroupingWithoutEmptyContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupingWithoutEmpty}
 * labeled alternative in {@link LogQLParser#grouping}.
 * @param ctx the parse tree
 */
fn exit_groupingWithoutEmpty(&mut self, _ctx: &GroupingWithoutEmptyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#binOpGroupingLabels}.
 * @param ctx the parse tree
 */
fn enter_binOpGroupingLabels(&mut self, _ctx: &BinOpGroupingLabelsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#binOpGroupingLabels}.
 * @param ctx the parse tree
 */
fn exit_binOpGroupingLabels(&mut self, _ctx: &BinOpGroupingLabelsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#groupingLabelList}.
 * @param ctx the parse tree
 */
fn enter_groupingLabelList(&mut self, _ctx: &GroupingLabelListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#groupingLabelList}.
 * @param ctx the parse tree
 */
fn exit_groupingLabelList(&mut self, _ctx: &GroupingLabelListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#groupingLabel}.
 * @param ctx the parse tree
 */
fn enter_groupingLabel(&mut self, _ctx: &GroupingLabelContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#groupingLabel}.
 * @param ctx the parse tree
 */
fn exit_groupingLabel(&mut self, _ctx: &GroupingLabelContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#groupingLabels}.
 * @param ctx the parse tree
 */
fn enter_groupingLabels(&mut self, _ctx: &GroupingLabelsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#groupingLabels}.
 * @param ctx the parse tree
 */
fn exit_groupingLabels(&mut self, _ctx: &GroupingLabelsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#logRangeExpr}.
 * @param ctx the parse tree
 */
fn enter_logRangeExpr(&mut self, _ctx: &LogRangeExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#logRangeExpr}.
 * @param ctx the parse tree
 */
fn exit_logRangeExpr(&mut self, _ctx: &LogRangeExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#unwrappedRangeExpr}.
 * @param ctx the parse tree
 */
fn enter_unwrappedRangeExpr(&mut self, _ctx: &UnwrappedRangeExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#unwrappedRangeExpr}.
 * @param ctx the parse tree
 */
fn exit_unwrappedRangeExpr(&mut self, _ctx: &UnwrappedRangeExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#range}.
 * @param ctx the parse tree
 */
fn enter_range(&mut self, _ctx: &RangeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#range}.
 * @param ctx the parse tree
 */
fn exit_range(&mut self, _ctx: &RangeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#offsetExpr}.
 * @param ctx the parse tree
 */
fn enter_offsetExpr(&mut self, _ctx: &OffsetExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#offsetExpr}.
 * @param ctx the parse tree
 */
fn exit_offsetExpr(&mut self, _ctx: &OffsetExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#atModifier}.
 * @param ctx the parse tree
 */
fn enter_atModifier(&mut self, _ctx: &AtModifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#atModifier}.
 * @param ctx the parse tree
 */
fn exit_atModifier(&mut self, _ctx: &AtModifierContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unwrapWithFilter}
 * labeled alternative in {@link LogQLParser#unwrapExpr}.
 * @param ctx the parse tree
 */
fn enter_unwrapWithFilter(&mut self, _ctx: &UnwrapWithFilterContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unwrapWithFilter}
 * labeled alternative in {@link LogQLParser#unwrapExpr}.
 * @param ctx the parse tree
 */
fn exit_unwrapWithFilter(&mut self, _ctx: &UnwrapWithFilterContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unwrapWithConversion}
 * labeled alternative in {@link LogQLParser#unwrapExpr}.
 * @param ctx the parse tree
 */
fn enter_unwrapWithConversion(&mut self, _ctx: &UnwrapWithConversionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unwrapWithConversion}
 * labeled alternative in {@link LogQLParser#unwrapExpr}.
 * @param ctx the parse tree
 */
fn exit_unwrapWithConversion(&mut self, _ctx: &UnwrapWithConversionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code unwrapBasic}
 * labeled alternative in {@link LogQLParser#unwrapExpr}.
 * @param ctx the parse tree
 */
fn enter_unwrapBasic(&mut self, _ctx: &UnwrapBasicContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code unwrapBasic}
 * labeled alternative in {@link LogQLParser#unwrapExpr}.
 * @param ctx the parse tree
 */
fn exit_unwrapBasic(&mut self, _ctx: &UnwrapBasicContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code literalNumber}
 * labeled alternative in {@link LogQLParser#literalExpr}.
 * @param ctx the parse tree
 */
fn enter_literalNumber(&mut self, _ctx: &LiteralNumberContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code literalNumber}
 * labeled alternative in {@link LogQLParser#literalExpr}.
 * @param ctx the parse tree
 */
fn exit_literalNumber(&mut self, _ctx: &LiteralNumberContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code literalPositiveNumber}
 * labeled alternative in {@link LogQLParser#literalExpr}.
 * @param ctx the parse tree
 */
fn enter_literalPositiveNumber(&mut self, _ctx: &LiteralPositiveNumberContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code literalPositiveNumber}
 * labeled alternative in {@link LogQLParser#literalExpr}.
 * @param ctx the parse tree
 */
fn exit_literalPositiveNumber(&mut self, _ctx: &LiteralPositiveNumberContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code literalNegativeNumber}
 * labeled alternative in {@link LogQLParser#literalExpr}.
 * @param ctx the parse tree
 */
fn enter_literalNegativeNumber(&mut self, _ctx: &LiteralNegativeNumberContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code literalNegativeNumber}
 * labeled alternative in {@link LogQLParser#literalExpr}.
 * @param ctx the parse tree
 */
fn exit_literalNegativeNumber(&mut self, _ctx: &LiteralNegativeNumberContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#labelReplaceExpr}.
 * @param ctx the parse tree
 */
fn enter_labelReplaceExpr(&mut self, _ctx: &LabelReplaceExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#labelReplaceExpr}.
 * @param ctx the parse tree
 */
fn exit_labelReplaceExpr(&mut self, _ctx: &LabelReplaceExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#vectorExpr}.
 * @param ctx the parse tree
 */
fn enter_vectorExpr(&mut self, _ctx: &VectorExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#vectorExpr}.
 * @param ctx the parse tree
 */
fn exit_vectorExpr(&mut self, _ctx: &VectorExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#variableExpr}.
 * @param ctx the parse tree
 */
fn enter_variableExpr(&mut self, _ctx: &VariableExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#variableExpr}.
 * @param ctx the parse tree
 */
fn exit_variableExpr(&mut self, _ctx: &VariableExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LogQLParser#duration}.
 * @param ctx the parse tree
 */
fn enter_duration(&mut self, _ctx: &DurationContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LogQLParser#duration}.
 * @param ctx the parse tree
 */
fn exit_duration(&mut self, _ctx: &DurationContext<'input>) { }

}

antlr4rust::coerce_from!{ 'input : LogQLParserListener<'input> }


