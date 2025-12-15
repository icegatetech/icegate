#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
// Generated from antlr/LogQLParser.g4 by ANTLR 4.13.2

use super::logqlparser::*;
use antlr4rust::tree::ParseTreeListener;

// A complete Visitor for a parse tree produced by LogQLParser.

pub trait LogQLParserBaseListener<'input>:
    ParseTreeListener<'input, LogQLParserContextType> {

    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_root(&mut self, _ctx: &RootContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_root(&mut self, _ctx: &RootContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_expr(&mut self, _ctx: &ExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_expr(&mut self, _ctx: &ExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logexprwithselectoronly(&mut self, _ctx: &LogExprWithSelectorOnlyContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logexprwithselectoronly(&mut self, _ctx: &LogExprWithSelectorOnlyContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logexprwithpipeline(&mut self, _ctx: &LogExprWithPipelineContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logexprwithpipeline(&mut self, _ctx: &LogExprWithPipelineContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_selector(&mut self, _ctx: &SelectorContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_selector(&mut self, _ctx: &SelectorContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_matchers(&mut self, _ctx: &MatchersContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_matchers(&mut self, _ctx: &MatchersContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_matchereq(&mut self, _ctx: &MatcherEqContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_matchereq(&mut self, _ctx: &MatcherEqContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_matcherneq(&mut self, _ctx: &MatcherNeqContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_matcherneq(&mut self, _ctx: &MatcherNeqContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_matcherre(&mut self, _ctx: &MatcherReContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_matcherre(&mut self, _ctx: &MatcherReContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_matchernre(&mut self, _ctx: &MatcherNreContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_matchernre(&mut self, _ctx: &MatcherNreContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_pipelineexpr(&mut self, _ctx: &PipelineExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_pipelineexpr(&mut self, _ctx: &PipelineExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_pipelinestage(&mut self, _ctx: &PipelineStageContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_pipelinestage(&mut self, _ctx: &PipelineStageContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_linefilterscontains(&mut self, _ctx: &LineFiltersContainsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_linefilterscontains(&mut self, _ctx: &LineFiltersContainsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_linefiltersnotcontains(&mut self, _ctx: &LineFiltersNotContainsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_linefiltersnotcontains(&mut self, _ctx: &LineFiltersNotContainsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_linefiltersmatch(&mut self, _ctx: &LineFiltersMatchContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_linefiltersmatch(&mut self, _ctx: &LineFiltersMatchContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_linefiltersnotmatch(&mut self, _ctx: &LineFiltersNotMatchContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_linefiltersnotmatch(&mut self, _ctx: &LineFiltersNotMatchContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_linefilterspattern(&mut self, _ctx: &LineFiltersPatternContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_linefilterspattern(&mut self, _ctx: &LineFiltersPatternContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_linefiltersnotpattern(&mut self, _ctx: &LineFiltersNotPatternContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_linefiltersnotpattern(&mut self, _ctx: &LineFiltersNotPatternContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_linefilterstring(&mut self, _ctx: &LineFilterStringContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_linefilterstring(&mut self, _ctx: &LineFilterStringContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_linefilterip(&mut self, _ctx: &LineFilterIpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_linefilterip(&mut self, _ctx: &LineFilterIpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_ipfn(&mut self, _ctx: &IpFnContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_ipfn(&mut self, _ctx: &IpFnContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_regexpparser(&mut self, _ctx: &RegexpParserContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_regexpparser(&mut self, _ctx: &RegexpParserContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_patternparser(&mut self, _ctx: &PatternParserContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_patternparser(&mut self, _ctx: &PatternParserContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_unpackparser(&mut self, _ctx: &UnpackParserContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_unpackparser(&mut self, _ctx: &UnpackParserContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logfmtparser(&mut self, _ctx: &LogfmtParserContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logfmtparser(&mut self, _ctx: &LogfmtParserContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelformatexpr(&mut self, _ctx: &LabelFormatExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelformatexpr(&mut self, _ctx: &LabelFormatExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelformatops(&mut self, _ctx: &LabelFormatOpsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelformatops(&mut self, _ctx: &LabelFormatOpsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelformatrename(&mut self, _ctx: &LabelFormatRenameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelformatrename(&mut self, _ctx: &LabelFormatRenameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelformattemplate(&mut self, _ctx: &LabelFormatTemplateContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelformattemplate(&mut self, _ctx: &LabelFormatTemplateContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lineformatexpr(&mut self, _ctx: &LineFormatExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lineformatexpr(&mut self, _ctx: &LineFormatExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_decolorizeexpr(&mut self, _ctx: &DecolorizeExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_decolorizeexpr(&mut self, _ctx: &DecolorizeExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_dropexpr(&mut self, _ctx: &DropExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_dropexpr(&mut self, _ctx: &DropExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_keepexpr(&mut self, _ctx: &KeepExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_keepexpr(&mut self, _ctx: &KeepExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_jsonparser(&mut self, _ctx: &JsonParserContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_jsonparser(&mut self, _ctx: &JsonParserContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelextractionwithpath(&mut self, _ctx: &LabelExtractionWithPathContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelextractionwithpath(&mut self, _ctx: &LabelExtractionWithPathContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelextractionsimple(&mut self, _ctx: &LabelExtractionSimpleContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelextractionsimple(&mut self, _ctx: &LabelExtractionSimpleContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelextractions(&mut self, _ctx: &LabelExtractionsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelextractions(&mut self, _ctx: &LabelExtractionsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelfilterbytes(&mut self, _ctx: &LabelFilterBytesContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelfilterbytes(&mut self, _ctx: &LabelFilterBytesContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelfilterduration(&mut self, _ctx: &LabelFilterDurationContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelfilterduration(&mut self, _ctx: &LabelFilterDurationContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelfilterparens(&mut self, _ctx: &LabelFilterParensContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelfilterparens(&mut self, _ctx: &LabelFilterParensContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelfilteror(&mut self, _ctx: &LabelFilterOrContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelfilteror(&mut self, _ctx: &LabelFilterOrContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelfiltermatcher(&mut self, _ctx: &LabelFilterMatcherContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelfiltermatcher(&mut self, _ctx: &LabelFilterMatcherContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelfilterip(&mut self, _ctx: &LabelFilterIpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelfilterip(&mut self, _ctx: &LabelFilterIpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelfilternumber(&mut self, _ctx: &LabelFilterNumberContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelfilternumber(&mut self, _ctx: &LabelFilterNumberContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelfilterand(&mut self, _ctx: &LabelFilterAndContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelfilterand(&mut self, _ctx: &LabelFilterAndContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_numberfilter(&mut self, _ctx: &NumberFilterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_numberfilter(&mut self, _ctx: &NumberFilterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_durationfilter(&mut self, _ctx: &DurationFilterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_durationfilter(&mut self, _ctx: &DurationFilterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_bytesfilter(&mut self, _ctx: &BytesFilterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_bytesfilter(&mut self, _ctx: &BytesFilterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_iplabelfilter(&mut self, _ctx: &IpLabelFilterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_iplabelfilter(&mut self, _ctx: &IpLabelFilterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_comparisonop(&mut self, _ctx: &ComparisonOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_comparisonop(&mut self, _ctx: &ComparisonOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_metricexprparens(&mut self, _ctx: &MetricExprParensContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_metricexprparens(&mut self, _ctx: &MetricExprParensContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_metricexprlabelreplace(&mut self, _ctx: &MetricExprLabelReplaceContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_metricexprlabelreplace(&mut self, _ctx: &MetricExprLabelReplaceContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_metricexprvariable(&mut self, _ctx: &MetricExprVariableContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_metricexprvariable(&mut self, _ctx: &MetricExprVariableContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopge(&mut self, _ctx: &BinaryOpGeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopge(&mut self, _ctx: &BinaryOpGeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryoppow(&mut self, _ctx: &BinaryOpPowContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryoppow(&mut self, _ctx: &BinaryOpPowContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopunless(&mut self, _ctx: &BinaryOpUnlessContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopunless(&mut self, _ctx: &BinaryOpUnlessContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_metricexprrangeagg(&mut self, _ctx: &MetricExprRangeAggContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_metricexprrangeagg(&mut self, _ctx: &MetricExprRangeAggContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopand(&mut self, _ctx: &BinaryOpAndContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopand(&mut self, _ctx: &BinaryOpAndContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopor(&mut self, _ctx: &BinaryOpOrContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopor(&mut self, _ctx: &BinaryOpOrContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryoplt(&mut self, _ctx: &BinaryOpLtContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryoplt(&mut self, _ctx: &BinaryOpLtContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_metricexprliteral(&mut self, _ctx: &MetricExprLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_metricexprliteral(&mut self, _ctx: &MetricExprLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopneq(&mut self, _ctx: &BinaryOpNeqContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopneq(&mut self, _ctx: &BinaryOpNeqContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopadd(&mut self, _ctx: &BinaryOpAddContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopadd(&mut self, _ctx: &BinaryOpAddContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopgt(&mut self, _ctx: &BinaryOpGtContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopgt(&mut self, _ctx: &BinaryOpGtContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopsub(&mut self, _ctx: &BinaryOpSubContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopsub(&mut self, _ctx: &BinaryOpSubContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopeql(&mut self, _ctx: &BinaryOpEqlContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopeql(&mut self, _ctx: &BinaryOpEqlContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopmul(&mut self, _ctx: &BinaryOpMulContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopmul(&mut self, _ctx: &BinaryOpMulContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_metricexprvector(&mut self, _ctx: &MetricExprVectorContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_metricexprvector(&mut self, _ctx: &MetricExprVectorContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopmod(&mut self, _ctx: &BinaryOpModContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopmod(&mut self, _ctx: &BinaryOpModContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryople(&mut self, _ctx: &BinaryOpLeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryople(&mut self, _ctx: &BinaryOpLeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_metricexprvectoragg(&mut self, _ctx: &MetricExprVectorAggContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_metricexprvectoragg(&mut self, _ctx: &MetricExprVectorAggContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryopdiv(&mut self, _ctx: &BinaryOpDivContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryopdiv(&mut self, _ctx: &BinaryOpDivContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeaggregationexpr(&mut self, _ctx: &RangeAggregationExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeaggregationexpr(&mut self, _ctx: &RangeAggregationExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangelogopcount(&mut self, _ctx: &RangeLogOpCountContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangelogopcount(&mut self, _ctx: &RangeLogOpCountContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangelogoprate(&mut self, _ctx: &RangeLogOpRateContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangelogoprate(&mut self, _ctx: &RangeLogOpRateContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangelogopbytes(&mut self, _ctx: &RangeLogOpBytesContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangelogopbytes(&mut self, _ctx: &RangeLogOpBytesContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangelogopbytesrate(&mut self, _ctx: &RangeLogOpBytesRateContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangelogopbytesrate(&mut self, _ctx: &RangeLogOpBytesRateContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangelogopabsent(&mut self, _ctx: &RangeLogOpAbsentContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangelogopabsent(&mut self, _ctx: &RangeLogOpAbsentContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopnogroupsum(&mut self, _ctx: &RangeUnwrapOpNoGroupSumContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopnogroupsum(&mut self, _ctx: &RangeUnwrapOpNoGroupSumContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopnogrouprate(&mut self, _ctx: &RangeUnwrapOpNoGroupRateContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopnogrouprate(&mut self, _ctx: &RangeUnwrapOpNoGroupRateContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopnogroupratecounter(&mut self, _ctx: &RangeUnwrapOpNoGroupRateCounterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopnogroupratecounter(&mut self, _ctx: &RangeUnwrapOpNoGroupRateCounterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopavg(&mut self, _ctx: &RangeUnwrapOpAvgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopavg(&mut self, _ctx: &RangeUnwrapOpAvgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopmin(&mut self, _ctx: &RangeUnwrapOpMinContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopmin(&mut self, _ctx: &RangeUnwrapOpMinContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopmax(&mut self, _ctx: &RangeUnwrapOpMaxContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopmax(&mut self, _ctx: &RangeUnwrapOpMaxContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopstddev(&mut self, _ctx: &RangeUnwrapOpStddevContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopstddev(&mut self, _ctx: &RangeUnwrapOpStddevContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopstdvar(&mut self, _ctx: &RangeUnwrapOpStdvarContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopstdvar(&mut self, _ctx: &RangeUnwrapOpStdvarContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopquantile(&mut self, _ctx: &RangeUnwrapOpQuantileContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopquantile(&mut self, _ctx: &RangeUnwrapOpQuantileContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapopfirst(&mut self, _ctx: &RangeUnwrapOpFirstContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapopfirst(&mut self, _ctx: &RangeUnwrapOpFirstContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeunwrapoplast(&mut self, _ctx: &RangeUnwrapOpLastContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeunwrapoplast(&mut self, _ctx: &RangeUnwrapOpLastContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectoraggregationexpr(&mut self, _ctx: &VectorAggregationExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectoraggregationexpr(&mut self, _ctx: &VectorAggregationExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectorop(&mut self, _ctx: &VectorOpContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectorop(&mut self, _ctx: &VectorOpContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binopmodifier(&mut self, _ctx: &BinOpModifierContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binopmodifier(&mut self, _ctx: &BinOpModifierContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_onorignoringmodifier(&mut self, _ctx: &OnOrIgnoringModifierContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_onorignoringmodifier(&mut self, _ctx: &OnOrIgnoringModifierContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_groupingby(&mut self, _ctx: &GroupingByContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_groupingby(&mut self, _ctx: &GroupingByContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_groupingwithout(&mut self, _ctx: &GroupingWithoutContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_groupingwithout(&mut self, _ctx: &GroupingWithoutContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_groupingbyempty(&mut self, _ctx: &GroupingByEmptyContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_groupingbyempty(&mut self, _ctx: &GroupingByEmptyContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_groupingwithoutempty(&mut self, _ctx: &GroupingWithoutEmptyContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_groupingwithoutempty(&mut self, _ctx: &GroupingWithoutEmptyContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binopgroupinglabels(&mut self, _ctx: &BinOpGroupingLabelsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binopgroupinglabels(&mut self, _ctx: &BinOpGroupingLabelsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_groupinglabellist(&mut self, _ctx: &GroupingLabelListContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_groupinglabellist(&mut self, _ctx: &GroupingLabelListContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_groupinglabel(&mut self, _ctx: &GroupingLabelContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_groupinglabel(&mut self, _ctx: &GroupingLabelContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_groupinglabels(&mut self, _ctx: &GroupingLabelsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_groupinglabels(&mut self, _ctx: &GroupingLabelsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lograngeexpr(&mut self, _ctx: &LogRangeExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lograngeexpr(&mut self, _ctx: &LogRangeExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_unwrappedrangeexpr(&mut self, _ctx: &UnwrappedRangeExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_unwrappedrangeexpr(&mut self, _ctx: &UnwrappedRangeExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_range(&mut self, _ctx: &RangeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_range(&mut self, _ctx: &RangeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_offsetexpr(&mut self, _ctx: &OffsetExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_offsetexpr(&mut self, _ctx: &OffsetExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_atmodifier(&mut self, _ctx: &AtModifierContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_atmodifier(&mut self, _ctx: &AtModifierContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_unwrapwithfilter(&mut self, _ctx: &UnwrapWithFilterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_unwrapwithfilter(&mut self, _ctx: &UnwrapWithFilterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_unwrapwithconversion(&mut self, _ctx: &UnwrapWithConversionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_unwrapwithconversion(&mut self, _ctx: &UnwrapWithConversionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_unwrapbasic(&mut self, _ctx: &UnwrapBasicContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_unwrapbasic(&mut self, _ctx: &UnwrapBasicContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_literalnumber(&mut self, _ctx: &LiteralNumberContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_literalnumber(&mut self, _ctx: &LiteralNumberContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_literalpositivenumber(&mut self, _ctx: &LiteralPositiveNumberContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_literalpositivenumber(&mut self, _ctx: &LiteralPositiveNumberContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_literalnegativenumber(&mut self, _ctx: &LiteralNegativeNumberContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_literalnegativenumber(&mut self, _ctx: &LiteralNegativeNumberContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_labelreplaceexpr(&mut self, _ctx: &LabelReplaceExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_labelreplaceexpr(&mut self, _ctx: &LabelReplaceExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_vectorexpr(&mut self, _ctx: &VectorExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_vectorexpr(&mut self, _ctx: &VectorExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_variableexpr(&mut self, _ctx: &VariableExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_variableexpr(&mut self, _ctx: &VariableExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_duration(&mut self, _ctx: &DurationContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LogQLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_duration(&mut self, _ctx: &DurationContext<'input>) {}


}