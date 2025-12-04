#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]

// Generated from antlr/LogQLParser.g4 by ANTLR 4.13.2

use antlr4rust::tree::ParseTreeVisitor;
use super::logqlparser::*;

// A complete Visitor for a parse tree produced by LogQLParser.

pub trait LogQLParserBaseVisitor<'input>:
    ParseTreeVisitor<'input, LogQLParserContextType> {
	// Visit a parse tree produced by LogQLParser#root.
	fn visit_root(&mut self, ctx: &RootContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#expr.
	fn visit_expr(&mut self, ctx: &ExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#logExprWithSelectorOnly.
	fn visit_logexprwithselectoronly(&mut self, ctx: &LogExprWithSelectorOnlyContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#logExprWithPipeline.
	fn visit_logexprwithpipeline(&mut self, ctx: &LogExprWithPipelineContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#selector.
	fn visit_selector(&mut self, ctx: &SelectorContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#matchers.
	fn visit_matchers(&mut self, ctx: &MatchersContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#matcherEq.
	fn visit_matchereq(&mut self, ctx: &MatcherEqContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#matcherNeq.
	fn visit_matcherneq(&mut self, ctx: &MatcherNeqContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#matcherRe.
	fn visit_matcherre(&mut self, ctx: &MatcherReContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#matcherNre.
	fn visit_matchernre(&mut self, ctx: &MatcherNreContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#pipelineExpr.
	fn visit_pipelineexpr(&mut self, ctx: &PipelineExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#pipelineStage.
	fn visit_pipelinestage(&mut self, ctx: &PipelineStageContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#lineFiltersContains.
	fn visit_linefilterscontains(&mut self, ctx: &LineFiltersContainsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#lineFiltersNotContains.
	fn visit_linefiltersnotcontains(&mut self, ctx: &LineFiltersNotContainsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#lineFiltersMatch.
	fn visit_linefiltersmatch(&mut self, ctx: &LineFiltersMatchContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#lineFiltersNotMatch.
	fn visit_linefiltersnotmatch(&mut self, ctx: &LineFiltersNotMatchContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#lineFiltersNotPattern.
	fn visit_linefiltersnotpattern(&mut self, ctx: &LineFiltersNotPatternContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#lineFilterString.
	fn visit_linefilterstring(&mut self, ctx: &LineFilterStringContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#lineFilterIp.
	fn visit_linefilterip(&mut self, ctx: &LineFilterIpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#ipFn.
	fn visit_ipfn(&mut self, ctx: &IpFnContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#regexpParser.
	fn visit_regexpparser(&mut self, ctx: &RegexpParserContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#patternParser.
	fn visit_patternparser(&mut self, ctx: &PatternParserContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#unpackParser.
	fn visit_unpackparser(&mut self, ctx: &UnpackParserContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#logfmtParser.
	fn visit_logfmtparser(&mut self, ctx: &LogfmtParserContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFormatExpr.
	fn visit_labelformatexpr(&mut self, ctx: &LabelFormatExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFormatOps.
	fn visit_labelformatops(&mut self, ctx: &LabelFormatOpsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFormatRename.
	fn visit_labelformatrename(&mut self, ctx: &LabelFormatRenameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFormatTemplate.
	fn visit_labelformattemplate(&mut self, ctx: &LabelFormatTemplateContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#lineFormatExpr.
	fn visit_lineformatexpr(&mut self, ctx: &LineFormatExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#decolorizeExpr.
	fn visit_decolorizeexpr(&mut self, ctx: &DecolorizeExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#dropExpr.
	fn visit_dropexpr(&mut self, ctx: &DropExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#keepExpr.
	fn visit_keepexpr(&mut self, ctx: &KeepExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#jsonParser.
	fn visit_jsonparser(&mut self, ctx: &JsonParserContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelExtractionWithPath.
	fn visit_labelextractionwithpath(&mut self, ctx: &LabelExtractionWithPathContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelExtractionSimple.
	fn visit_labelextractionsimple(&mut self, ctx: &LabelExtractionSimpleContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelExtractions.
	fn visit_labelextractions(&mut self, ctx: &LabelExtractionsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFilterBytes.
	fn visit_labelfilterbytes(&mut self, ctx: &LabelFilterBytesContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFilterDuration.
	fn visit_labelfilterduration(&mut self, ctx: &LabelFilterDurationContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFilterParens.
	fn visit_labelfilterparens(&mut self, ctx: &LabelFilterParensContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFilterOr.
	fn visit_labelfilteror(&mut self, ctx: &LabelFilterOrContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFilterMatcher.
	fn visit_labelfiltermatcher(&mut self, ctx: &LabelFilterMatcherContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFilterIp.
	fn visit_labelfilterip(&mut self, ctx: &LabelFilterIpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFilterNumber.
	fn visit_labelfilternumber(&mut self, ctx: &LabelFilterNumberContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelFilterAnd.
	fn visit_labelfilterand(&mut self, ctx: &LabelFilterAndContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#numberFilter.
	fn visit_numberfilter(&mut self, ctx: &NumberFilterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#durationFilter.
	fn visit_durationfilter(&mut self, ctx: &DurationFilterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#bytesFilter.
	fn visit_bytesfilter(&mut self, ctx: &BytesFilterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#ipLabelFilter.
	fn visit_iplabelfilter(&mut self, ctx: &IpLabelFilterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#comparisonOp.
	fn visit_comparisonop(&mut self, ctx: &ComparisonOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#metricExprParens.
	fn visit_metricexprparens(&mut self, ctx: &MetricExprParensContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#metricExprLabelReplace.
	fn visit_metricexprlabelreplace(&mut self, ctx: &MetricExprLabelReplaceContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#metricExprVariable.
	fn visit_metricexprvariable(&mut self, ctx: &MetricExprVariableContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpGe.
	fn visit_binaryopge(&mut self, ctx: &BinaryOpGeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpPow.
	fn visit_binaryoppow(&mut self, ctx: &BinaryOpPowContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpUnless.
	fn visit_binaryopunless(&mut self, ctx: &BinaryOpUnlessContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#metricExprRangeAgg.
	fn visit_metricexprrangeagg(&mut self, ctx: &MetricExprRangeAggContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpAnd.
	fn visit_binaryopand(&mut self, ctx: &BinaryOpAndContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpOr.
	fn visit_binaryopor(&mut self, ctx: &BinaryOpOrContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpLt.
	fn visit_binaryoplt(&mut self, ctx: &BinaryOpLtContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#metricExprLiteral.
	fn visit_metricexprliteral(&mut self, ctx: &MetricExprLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpNeq.
	fn visit_binaryopneq(&mut self, ctx: &BinaryOpNeqContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpAdd.
	fn visit_binaryopadd(&mut self, ctx: &BinaryOpAddContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpGt.
	fn visit_binaryopgt(&mut self, ctx: &BinaryOpGtContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpSub.
	fn visit_binaryopsub(&mut self, ctx: &BinaryOpSubContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpEql.
	fn visit_binaryopeql(&mut self, ctx: &BinaryOpEqlContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpMul.
	fn visit_binaryopmul(&mut self, ctx: &BinaryOpMulContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#metricExprVector.
	fn visit_metricexprvector(&mut self, ctx: &MetricExprVectorContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpMod.
	fn visit_binaryopmod(&mut self, ctx: &BinaryOpModContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpLe.
	fn visit_binaryople(&mut self, ctx: &BinaryOpLeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#metricExprVectorAgg.
	fn visit_metricexprvectoragg(&mut self, ctx: &MetricExprVectorAggContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binaryOpDiv.
	fn visit_binaryopdiv(&mut self, ctx: &BinaryOpDivContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeAggregationExpr.
	fn visit_rangeaggregationexpr(&mut self, ctx: &RangeAggregationExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeLogOpCount.
	fn visit_rangelogopcount(&mut self, ctx: &RangeLogOpCountContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeLogOpRate.
	fn visit_rangelogoprate(&mut self, ctx: &RangeLogOpRateContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeLogOpBytes.
	fn visit_rangelogopbytes(&mut self, ctx: &RangeLogOpBytesContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeLogOpBytesRate.
	fn visit_rangelogopbytesrate(&mut self, ctx: &RangeLogOpBytesRateContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeLogOpAbsent.
	fn visit_rangelogopabsent(&mut self, ctx: &RangeLogOpAbsentContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpNoGroupSum.
	fn visit_rangeunwrapopnogroupsum(&mut self, ctx: &RangeUnwrapOpNoGroupSumContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpNoGroupRate.
	fn visit_rangeunwrapopnogrouprate(&mut self, ctx: &RangeUnwrapOpNoGroupRateContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpNoGroupRateCounter.
	fn visit_rangeunwrapopnogroupratecounter(&mut self, ctx: &RangeUnwrapOpNoGroupRateCounterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpAvg.
	fn visit_rangeunwrapopavg(&mut self, ctx: &RangeUnwrapOpAvgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpMin.
	fn visit_rangeunwrapopmin(&mut self, ctx: &RangeUnwrapOpMinContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpMax.
	fn visit_rangeunwrapopmax(&mut self, ctx: &RangeUnwrapOpMaxContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpStddev.
	fn visit_rangeunwrapopstddev(&mut self, ctx: &RangeUnwrapOpStddevContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpStdvar.
	fn visit_rangeunwrapopstdvar(&mut self, ctx: &RangeUnwrapOpStdvarContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpQuantile.
	fn visit_rangeunwrapopquantile(&mut self, ctx: &RangeUnwrapOpQuantileContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpFirst.
	fn visit_rangeunwrapopfirst(&mut self, ctx: &RangeUnwrapOpFirstContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#rangeUnwrapOpLast.
	fn visit_rangeunwrapoplast(&mut self, ctx: &RangeUnwrapOpLastContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#vectorAggregationExpr.
	fn visit_vectoraggregationexpr(&mut self, ctx: &VectorAggregationExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#vectorOp.
	fn visit_vectorop(&mut self, ctx: &VectorOpContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binOpModifier.
	fn visit_binopmodifier(&mut self, ctx: &BinOpModifierContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#onOrIgnoringModifier.
	fn visit_onorignoringmodifier(&mut self, ctx: &OnOrIgnoringModifierContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#groupingBy.
	fn visit_groupingby(&mut self, ctx: &GroupingByContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#groupingWithout.
	fn visit_groupingwithout(&mut self, ctx: &GroupingWithoutContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#groupingByEmpty.
	fn visit_groupingbyempty(&mut self, ctx: &GroupingByEmptyContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#groupingWithoutEmpty.
	fn visit_groupingwithoutempty(&mut self, ctx: &GroupingWithoutEmptyContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#binOpGroupingLabels.
	fn visit_binopgroupinglabels(&mut self, ctx: &BinOpGroupingLabelsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#groupingLabelList.
	fn visit_groupinglabellist(&mut self, ctx: &GroupingLabelListContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#groupingLabel.
	fn visit_groupinglabel(&mut self, ctx: &GroupingLabelContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#groupingLabels.
	fn visit_groupinglabels(&mut self, ctx: &GroupingLabelsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#logRangeExpr.
	fn visit_lograngeexpr(&mut self, ctx: &LogRangeExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#unwrappedRangeExpr.
	fn visit_unwrappedrangeexpr(&mut self, ctx: &UnwrappedRangeExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#range.
	fn visit_range(&mut self, ctx: &RangeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#offsetExpr.
	fn visit_offsetexpr(&mut self, ctx: &OffsetExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#atModifier.
	fn visit_atmodifier(&mut self, ctx: &AtModifierContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#unwrapWithFilter.
	fn visit_unwrapwithfilter(&mut self, ctx: &UnwrapWithFilterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#unwrapWithConversion.
	fn visit_unwrapwithconversion(&mut self, ctx: &UnwrapWithConversionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#unwrapBasic.
	fn visit_unwrapbasic(&mut self, ctx: &UnwrapBasicContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#literalNumber.
	fn visit_literalnumber(&mut self, ctx: &LiteralNumberContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#literalPositiveNumber.
	fn visit_literalpositivenumber(&mut self, ctx: &LiteralPositiveNumberContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#literalNegativeNumber.
	fn visit_literalnegativenumber(&mut self, ctx: &LiteralNegativeNumberContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#labelReplaceExpr.
	fn visit_labelreplaceexpr(&mut self, ctx: &LabelReplaceExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#vectorExpr.
	fn visit_vectorexpr(&mut self, ctx: &VectorExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#variableExpr.
	fn visit_variableexpr(&mut self, ctx: &VariableExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LogQLParser#duration.
	fn visit_duration(&mut self, ctx: &DurationContext<'input>) {
            self.visit_children(ctx)
        }

}