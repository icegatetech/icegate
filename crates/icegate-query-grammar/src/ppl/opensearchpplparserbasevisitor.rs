#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]

// Generated from OpenSearchPPLParser.g4 by ANTLR 4.13.2

use antlr4rust::tree::ParseTreeVisitor;
use super::opensearchpplparser::*;

// A complete Visitor for a parse tree produced by OpenSearchPPLParser.

pub trait OpenSearchPPLParserBaseVisitor<'input>:
    ParseTreeVisitor<'input, OpenSearchPPLParserContextType> {
	// Visit a parse tree produced by OpenSearchPPLParser#root.
	fn visit_root(&mut self, ctx: &RootContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#pplStatement.
	fn visit_pplstatement(&mut self, ctx: &PplStatementContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#subPipeline.
	fn visit_subpipeline(&mut self, ctx: &SubPipelineContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#queryStatement.
	fn visit_querystatement(&mut self, ctx: &QueryStatementContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#explainStatement.
	fn visit_explainstatement(&mut self, ctx: &ExplainStatementContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#explainMode.
	fn visit_explainmode(&mut self, ctx: &ExplainModeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#subSearch.
	fn visit_subsearch(&mut self, ctx: &SubSearchContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#pplCommands.
	fn visit_pplcommands(&mut self, ctx: &PplCommandsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#commands.
	fn visit_commands(&mut self, ctx: &CommandsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#commandName.
	fn visit_commandname(&mut self, ctx: &CommandNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#searchFrom.
	fn visit_searchfrom(&mut self, ctx: &SearchFromContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#orExpression.
	fn visit_orexpression(&mut self, ctx: &OrExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timeModifierExpression.
	fn visit_timemodifierexpression(&mut self, ctx: &TimeModifierExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#andExpression.
	fn visit_andexpression(&mut self, ctx: &AndExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#notExpression.
	fn visit_notexpression(&mut self, ctx: &NotExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#termExpression.
	fn visit_termexpression(&mut self, ctx: &TermExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#groupedExpression.
	fn visit_groupedexpression(&mut self, ctx: &GroupedExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#searchComparisonTerm.
	fn visit_searchcomparisonterm(&mut self, ctx: &SearchComparisonTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#searchInListTerm.
	fn visit_searchinlistterm(&mut self, ctx: &SearchInListTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#searchLiteralTerm.
	fn visit_searchliteralterm(&mut self, ctx: &SearchLiteralTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#searchLiteral.
	fn visit_searchliteral(&mut self, ctx: &SearchLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#searchFieldCompare.
	fn visit_searchfieldcompare(&mut self, ctx: &SearchFieldCompareContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#searchFieldInValues.
	fn visit_searchfieldinvalues(&mut self, ctx: &SearchFieldInValuesContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#searchLiterals.
	fn visit_searchliterals(&mut self, ctx: &SearchLiteralsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#equals.
	fn visit_equals(&mut self, ctx: &EqualsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#notEquals.
	fn visit_notequals(&mut self, ctx: &NotEqualsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#lessThan.
	fn visit_lessthan(&mut self, ctx: &LessThanContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#lessOrEqual.
	fn visit_lessorequal(&mut self, ctx: &LessOrEqualContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#greaterThan.
	fn visit_greaterthan(&mut self, ctx: &GreaterThanContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#greaterOrEqual.
	fn visit_greaterorequal(&mut self, ctx: &GreaterOrEqualContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#describeCommand.
	fn visit_describecommand(&mut self, ctx: &DescribeCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#restCommand.
	fn visit_restcommand(&mut self, ctx: &RestCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#restArgument.
	fn visit_restargument(&mut self, ctx: &RestArgumentContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#showDataSourcesCommand.
	fn visit_showdatasourcescommand(&mut self, ctx: &ShowDataSourcesCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#makeresultsCommand.
	fn visit_makeresultscommand(&mut self, ctx: &MakeresultsCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#makeresultsArg.
	fn visit_makeresultsarg(&mut self, ctx: &MakeresultsArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#whereCommand.
	fn visit_wherecommand(&mut self, ctx: &WhereCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fieldsCommand.
	fn visit_fieldscommand(&mut self, ctx: &FieldsCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#tableCommand.
	fn visit_tablecommand(&mut self, ctx: &TableCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fieldsCommandBody.
	fn visit_fieldscommandbody(&mut self, ctx: &FieldsCommandBodyContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#wcFieldList.
	fn visit_wcfieldlist(&mut self, ctx: &WcFieldListContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#renameCommand.
	fn visit_renamecommand(&mut self, ctx: &RenameCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#replaceCommand.
	fn visit_replacecommand(&mut self, ctx: &ReplaceCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#replacePair.
	fn visit_replacepair(&mut self, ctx: &ReplacePairContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#statsCommand.
	fn visit_statscommand(&mut self, ctx: &StatsCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#appendPipeCommand.
	fn visit_appendpipecommand(&mut self, ctx: &AppendPipeCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#statsArgs.
	fn visit_statsargs(&mut self, ctx: &StatsArgsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#partitionsArg.
	fn visit_partitionsarg(&mut self, ctx: &PartitionsArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#allnumArg.
	fn visit_allnumarg(&mut self, ctx: &AllnumArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#delimArg.
	fn visit_delimarg(&mut self, ctx: &DelimArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#bucketNullableArg.
	fn visit_bucketnullablearg(&mut self, ctx: &BucketNullableArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#dedupSplitArg.
	fn visit_dedupsplitarg(&mut self, ctx: &DedupSplitArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#eventstatsCommand.
	fn visit_eventstatscommand(&mut self, ctx: &EventstatsCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#streamstatsCommand.
	fn visit_streamstatscommand(&mut self, ctx: &StreamstatsCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#streamstatsArgs.
	fn visit_streamstatsargs(&mut self, ctx: &StreamstatsArgsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#currentArg.
	fn visit_currentarg(&mut self, ctx: &CurrentArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#windowArg.
	fn visit_windowarg(&mut self, ctx: &WindowArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#globalArg.
	fn visit_globalarg(&mut self, ctx: &GlobalArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#resetBeforeArg.
	fn visit_resetbeforearg(&mut self, ctx: &ResetBeforeArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#resetAfterArg.
	fn visit_resetafterarg(&mut self, ctx: &ResetAfterArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#dedupCommand.
	fn visit_dedupcommand(&mut self, ctx: &DedupCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#sortCommand.
	fn visit_sortcommand(&mut self, ctx: &SortCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#reverseCommand.
	fn visit_reversecommand(&mut self, ctx: &ReverseCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#chartCommand.
	fn visit_chartcommand(&mut self, ctx: &ChartCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#chartOptions.
	fn visit_chartoptions(&mut self, ctx: &ChartOptionsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#rowSplit.
	fn visit_rowsplit(&mut self, ctx: &RowSplitContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#columnSplit.
	fn visit_columnsplit(&mut self, ctx: &ColumnSplitContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timechartCommand.
	fn visit_timechartcommand(&mut self, ctx: &TimechartCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#transposeCommand.
	fn visit_transposecommand(&mut self, ctx: &TransposeCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#transposeParameter.
	fn visit_transposeparameter(&mut self, ctx: &TransposeParameterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timewrapCommand.
	fn visit_timewrapcommand(&mut self, ctx: &TimewrapCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timewrapParameter.
	fn visit_timewrapparameter(&mut self, ctx: &TimewrapParameterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timewrapAlign.
	fn visit_timewrapalign(&mut self, ctx: &TimewrapAlignContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timewrapSeries.
	fn visit_timewrapseries(&mut self, ctx: &TimewrapSeriesContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timechartParameter.
	fn visit_timechartparameter(&mut self, ctx: &TimechartParameterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#spanLiteral.
	fn visit_spanliteral(&mut self, ctx: &SpanLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#evalCommand.
	fn visit_evalcommand(&mut self, ctx: &EvalCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachCommand.
	fn visit_foreachcommand(&mut self, ctx: &ForeachCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachArgument.
	fn visit_foreachargument(&mut self, ctx: &ForeachArgumentContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachTarget.
	fn visit_foreachtarget(&mut self, ctx: &ForeachTargetContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachOption.
	fn visit_foreachoption(&mut self, ctx: &ForeachOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachEvalCommand.
	fn visit_foreachevalcommand(&mut self, ctx: &ForeachEvalCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachEvalClause.
	fn visit_foreachevalclause(&mut self, ctx: &ForeachEvalClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachEvalTarget.
	fn visit_foreachevaltarget(&mut self, ctx: &ForeachEvalTargetContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fieldformatCommand.
	fn visit_fieldformatcommand(&mut self, ctx: &FieldformatCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#headCommand.
	fn visit_headcommand(&mut self, ctx: &HeadCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#binCommand.
	fn visit_bincommand(&mut self, ctx: &BinCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#binOption.
	fn visit_binoption(&mut self, ctx: &BinOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#aligntimeValue.
	fn visit_aligntimevalue(&mut self, ctx: &AligntimeValueContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#numericSpanValue.
	fn visit_numericspanvalue(&mut self, ctx: &NumericSpanValueContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#logBasedSpanValue.
	fn visit_logbasedspanvalue(&mut self, ctx: &LogBasedSpanValueContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#logWithBaseSpan.
	fn visit_logwithbasespan(&mut self, ctx: &LogWithBaseSpanContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#rareTopCommand.
	fn visit_raretopcommand(&mut self, ctx: &RareTopCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#rareTopOption.
	fn visit_raretopoption(&mut self, ctx: &RareTopOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#grokCommand.
	fn visit_grokcommand(&mut self, ctx: &GrokCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#parseCommand.
	fn visit_parsecommand(&mut self, ctx: &ParseCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#spathCommand.
	fn visit_spathcommand(&mut self, ctx: &SpathCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#spathParameter.
	fn visit_spathparameter(&mut self, ctx: &SpathParameterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#indexablePath.
	fn visit_indexablepath(&mut self, ctx: &IndexablePathContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#pathElement.
	fn visit_pathelement(&mut self, ctx: &PathElementContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#pathArrayAccess.
	fn visit_patharrayaccess(&mut self, ctx: &PathArrayAccessContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#regexCommand.
	fn visit_regexcommand(&mut self, ctx: &RegexCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#regexExpr.
	fn visit_regexexpr(&mut self, ctx: &RegexExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#rexCommand.
	fn visit_rexcommand(&mut self, ctx: &RexCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#rexExpr.
	fn visit_rexexpr(&mut self, ctx: &RexExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#rexOption.
	fn visit_rexoption(&mut self, ctx: &RexOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#patternsMethod.
	fn visit_patternsmethod(&mut self, ctx: &PatternsMethodContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#patternsCommand.
	fn visit_patternscommand(&mut self, ctx: &PatternsCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#patternsCommandOption.
	fn visit_patternscommandoption(&mut self, ctx: &PatternsCommandOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#patternsParameter.
	fn visit_patternsparameter(&mut self, ctx: &PatternsParameterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#patternMethod.
	fn visit_patternmethod(&mut self, ctx: &PatternMethodContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#patternMode.
	fn visit_patternmode(&mut self, ctx: &PatternModeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#lookupCommand.
	fn visit_lookupcommand(&mut self, ctx: &LookupCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#lookupMappingList.
	fn visit_lookupmappinglist(&mut self, ctx: &LookupMappingListContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#outputCandidateList.
	fn visit_outputcandidatelist(&mut self, ctx: &OutputCandidateListContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#lookupPair.
	fn visit_lookuppair(&mut self, ctx: &LookupPairContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fillNullWithClause.
	fn visit_fillnullwithclause(&mut self, ctx: &FillNullWithClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fillNullUsingClause.
	fn visit_fillnullusingclause(&mut self, ctx: &FillNullUsingClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fillNullValueWithFields.
	fn visit_fillnullvaluewithfields(&mut self, ctx: &FillNullValueWithFieldsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fillNullValueAllFields.
	fn visit_fillnullvalueallfields(&mut self, ctx: &FillNullValueAllFieldsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fillNullWith.
	fn visit_fillnullwith(&mut self, ctx: &FillNullWithContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fillNullUsing.
	fn visit_fillnullusing(&mut self, ctx: &FillNullUsingContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#replacementPair.
	fn visit_replacementpair(&mut self, ctx: &ReplacementPairContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#convertCommand.
	fn visit_convertcommand(&mut self, ctx: &ConvertCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#convertFunction.
	fn visit_convertfunction(&mut self, ctx: &ConvertFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#trendlineCommand.
	fn visit_trendlinecommand(&mut self, ctx: &TrendlineCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#trendlineClause.
	fn visit_trendlineclause(&mut self, ctx: &TrendlineClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#trendlineType.
	fn visit_trendlinetype(&mut self, ctx: &TrendlineTypeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#expandCommand.
	fn visit_expandcommand(&mut self, ctx: &ExpandCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#mvcombineCommand.
	fn visit_mvcombinecommand(&mut self, ctx: &MvcombineCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#nomvCommand.
	fn visit_nomvcommand(&mut self, ctx: &NomvCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#mvexpandCommand.
	fn visit_mvexpandcommand(&mut self, ctx: &MvexpandCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#flattenCommand.
	fn visit_flattencommand(&mut self, ctx: &FlattenCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#appendcolCommand.
	fn visit_appendcolcommand(&mut self, ctx: &AppendcolCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#appendCommand.
	fn visit_appendcommand(&mut self, ctx: &AppendCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#multisearchCommand.
	fn visit_multisearchcommand(&mut self, ctx: &MultisearchCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#unionCommand.
	fn visit_unioncommand(&mut self, ctx: &UnionCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#subsearchOptions.
	fn visit_subsearchoptions(&mut self, ctx: &SubsearchOptionsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#unionDataset.
	fn visit_uniondataset(&mut self, ctx: &UnionDatasetContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#kmeansCommand.
	fn visit_kmeanscommand(&mut self, ctx: &KmeansCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#kmeansParameter.
	fn visit_kmeansparameter(&mut self, ctx: &KmeansParameterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#adCommand.
	fn visit_adcommand(&mut self, ctx: &AdCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#adParameter.
	fn visit_adparameter(&mut self, ctx: &AdParameterContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#mlCommand.
	fn visit_mlcommand(&mut self, ctx: &MlCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#mlArg.
	fn visit_mlarg(&mut self, ctx: &MlArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#addtotalsCommand.
	fn visit_addtotalscommand(&mut self, ctx: &AddtotalsCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#addtotalsOption.
	fn visit_addtotalsoption(&mut self, ctx: &AddtotalsOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#addcoltotalsCommand.
	fn visit_addcoltotalscommand(&mut self, ctx: &AddcoltotalsCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#addcoltotalsOption.
	fn visit_addcoltotalsoption(&mut self, ctx: &AddcoltotalsOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#graphLookupCommand.
	fn visit_graphlookupcommand(&mut self, ctx: &GraphLookupCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#startClause.
	fn visit_startclause(&mut self, ctx: &StartClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#edgeClause.
	fn visit_edgeclause(&mut self, ctx: &EdgeClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#graphLookupArgs.
	fn visit_graphlookupargs(&mut self, ctx: &GraphLookupArgsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#xyseriesCommand.
	fn visit_xyseriescommand(&mut self, ctx: &XyseriesCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#xyseriesOption.
	fn visit_xyseriesoption(&mut self, ctx: &XyseriesOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#xyseriesPivotValues.
	fn visit_xyseriespivotvalues(&mut self, ctx: &XyseriesPivotValuesContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fromClause.
	fn visit_fromclause(&mut self, ctx: &FromClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#tableOrSubqueryClause.
	fn visit_tableorsubqueryclause(&mut self, ctx: &TableOrSubqueryClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#tableSourceClause.
	fn visit_tablesourceclause(&mut self, ctx: &TableSourceClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#dynamicSourceClause.
	fn visit_dynamicsourceclause(&mut self, ctx: &DynamicSourceClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#sourceReference.
	fn visit_sourcereference(&mut self, ctx: &SourceReferenceContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#sourceFilterArg.
	fn visit_sourcefilterarg(&mut self, ctx: &SourceFilterArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#joinCommand.
	fn visit_joincommand(&mut self, ctx: &JoinCommandContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#sqlLikeJoinType.
	fn visit_sqllikejointype(&mut self, ctx: &SqlLikeJoinTypeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#joinType.
	fn visit_jointype(&mut self, ctx: &JoinTypeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#sideAlias.
	fn visit_sidealias(&mut self, ctx: &SideAliasContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#joinCriteria.
	fn visit_joincriteria(&mut self, ctx: &JoinCriteriaContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#joinHintList.
	fn visit_joinhintlist(&mut self, ctx: &JoinHintListContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#leftHint.
	fn visit_lefthint(&mut self, ctx: &LeftHintContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#rightHint.
	fn visit_righthint(&mut self, ctx: &RightHintContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#overwriteOption.
	fn visit_overwriteoption(&mut self, ctx: &OverwriteOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#typeOption.
	fn visit_typeoption(&mut self, ctx: &TypeOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#maxOption.
	fn visit_maxoption(&mut self, ctx: &MaxOptionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#renameClause.
	fn visit_renameclause(&mut self, ctx: &RenameClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#byClause.
	fn visit_byclause(&mut self, ctx: &ByClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#statsByClause.
	fn visit_statsbyclause(&mut self, ctx: &StatsByClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#bySpanClause.
	fn visit_byspanclause(&mut self, ctx: &BySpanClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#spanClause.
	fn visit_spanclause(&mut self, ctx: &SpanClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#sortbyClause.
	fn visit_sortbyclause(&mut self, ctx: &SortbyClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#evalClause.
	fn visit_evalclause(&mut self, ctx: &EvalClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fieldFormatEvalClause.
	fn visit_fieldformatevalclause(&mut self, ctx: &FieldFormatEvalClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#eventstatsAggTerm.
	fn visit_eventstatsaggterm(&mut self, ctx: &EventstatsAggTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#streamstatsAggTerm.
	fn visit_streamstatsaggterm(&mut self, ctx: &StreamstatsAggTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#windowFunction.
	fn visit_windowfunction(&mut self, ctx: &WindowFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#windowFunctionName.
	fn visit_windowfunctionname(&mut self, ctx: &WindowFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#scalarWindowFunctionName.
	fn visit_scalarwindowfunctionname(&mut self, ctx: &ScalarWindowFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#statsAggTerm.
	fn visit_statsaggterm(&mut self, ctx: &StatsAggTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#countEvalFunctionCall.
	fn visit_countevalfunctioncall(&mut self, ctx: &CountEvalFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#countAllFunctionCall.
	fn visit_countallfunctioncall(&mut self, ctx: &CountAllFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#percentileShortcutFunctionCall.
	fn visit_percentileshortcutfunctioncall(&mut self, ctx: &PercentileShortcutFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#distinctCountFunctionCall.
	fn visit_distinctcountfunctioncall(&mut self, ctx: &DistinctCountFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#takeAggFunctionCall.
	fn visit_takeaggfunctioncall(&mut self, ctx: &TakeAggFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#valuesAggFunctionCall.
	fn visit_valuesaggfunctioncall(&mut self, ctx: &ValuesAggFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#percentileApproxFunctionCall.
	fn visit_percentileapproxfunctioncall(&mut self, ctx: &PercentileApproxFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#perFunctionCall.
	fn visit_perfunctioncall(&mut self, ctx: &PerFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#statsFunctionCall.
	fn visit_statsfunctioncall(&mut self, ctx: &StatsFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#statsFunctionName.
	fn visit_statsfunctionname(&mut self, ctx: &StatsFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#takeAggFunction.
	fn visit_takeaggfunction(&mut self, ctx: &TakeAggFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#valuesAggFunction.
	fn visit_valuesaggfunction(&mut self, ctx: &ValuesAggFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#percentileApproxFunction.
	fn visit_percentileapproxfunction(&mut self, ctx: &PercentileApproxFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#perFunction.
	fn visit_perfunction(&mut self, ctx: &PerFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#numericLiteral.
	fn visit_numericliteral(&mut self, ctx: &NumericLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#stringDotlogicalExpression.
	fn visit_stringdotlogicalexpression(&mut self, ctx: &StringDotlogicalExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#stringDotlogicalExpressionDotString.
	fn visit_stringdotlogicalexpressiondotstring(&mut self, ctx: &StringDotlogicalExpressionDotStringContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#logicalExpressionDotString.
	fn visit_logicalexpressiondotstring(&mut self, ctx: &LogicalExpressionDotStringContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#ffStandardLogicalExpression.
	fn visit_ffstandardlogicalexpression(&mut self, ctx: &FfStandardLogicalExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#logicalNot.
	fn visit_logicalnot(&mut self, ctx: &LogicalNotContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#logicalExpr.
	fn visit_logicalexpr(&mut self, ctx: &LogicalExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#logicalAnd.
	fn visit_logicaland(&mut self, ctx: &LogicalAndContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#logicalXor.
	fn visit_logicalxor(&mut self, ctx: &LogicalXorContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#logicalOr.
	fn visit_logicalor(&mut self, ctx: &LogicalOrContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#relevanceExpr.
	fn visit_relevanceexpr(&mut self, ctx: &RelevanceExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#valueExpr.
	fn visit_valueexpr(&mut self, ctx: &ValueExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#inExpr.
	fn visit_inexpr(&mut self, ctx: &InExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#isNullPredicate.
	fn visit_isnullpredicate(&mut self, ctx: &IsNullPredicateContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#between.
	fn visit_between(&mut self, ctx: &BetweenContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#compareExpr.
	fn visit_compareexpr(&mut self, ctx: &CompareExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#nullNotnull.
	fn visit_nullnotnull(&mut self, ctx: &NullNotnullContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#inSubqueryExpr.
	fn visit_insubqueryexpr(&mut self, ctx: &InSubqueryExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#lambdaExpr.
	fn visit_lambdaexpr(&mut self, ctx: &LambdaExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#literalValueExpr.
	fn visit_literalvalueexpr(&mut self, ctx: &LiteralValueExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#functionCallExpr.
	fn visit_functioncallexpr(&mut self, ctx: &FunctionCallExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#existsSubqueryExpr.
	fn visit_existssubqueryexpr(&mut self, ctx: &ExistsSubqueryExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachPlaceholderExpr.
	fn visit_foreachplaceholderexpr(&mut self, ctx: &ForeachPlaceholderExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#scalarSubqueryExpr.
	fn visit_scalarsubqueryexpr(&mut self, ctx: &ScalarSubqueryExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#nestedValueExpr.
	fn visit_nestedvalueexpr(&mut self, ctx: &NestedValueExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#binaryArithmetic.
	fn visit_binaryarithmetic(&mut self, ctx: &BinaryArithmeticContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fieldExpr.
	fn visit_fieldexpr(&mut self, ctx: &FieldExprContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#evalExpression.
	fn visit_evalexpression(&mut self, ctx: &EvalExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#functionCall.
	fn visit_functioncall(&mut self, ctx: &FunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#mvmapFunctionCall.
	fn visit_mvmapfunctioncall(&mut self, ctx: &MvmapFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#positionFunctionCall.
	fn visit_positionfunctioncall(&mut self, ctx: &PositionFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#caseFunctionCall.
	fn visit_casefunctioncall(&mut self, ctx: &CaseFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#relevanceExpression.
	fn visit_relevanceexpression(&mut self, ctx: &RelevanceExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#singleFieldRelevanceFunction.
	fn visit_singlefieldrelevancefunction(&mut self, ctx: &SingleFieldRelevanceFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#multiFieldRelevanceFunction.
	fn visit_multifieldrelevancefunction(&mut self, ctx: &MultiFieldRelevanceFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timeModifier.
	fn visit_timemodifier(&mut self, ctx: &TimeModifierContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timeModifierValue.
	fn visit_timemodifiervalue(&mut self, ctx: &TimeModifierValueContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#tableSource.
	fn visit_tablesource(&mut self, ctx: &TableSourceContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#tableFunction.
	fn visit_tablefunction(&mut self, ctx: &TableFunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fieldList.
	fn visit_fieldlist(&mut self, ctx: &FieldListContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#invalidMixedSortField.
	fn visit_invalidmixedsortfield(&mut self, ctx: &InvalidMixedSortFieldContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#prefixSortField.
	fn visit_prefixsortfield(&mut self, ctx: &PrefixSortFieldContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#suffixSortField.
	fn visit_suffixsortfield(&mut self, ctx: &SuffixSortFieldContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#defaultSortField.
	fn visit_defaultsortfield(&mut self, ctx: &DefaultSortFieldContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#sortFieldExpression.
	fn visit_sortfieldexpression(&mut self, ctx: &SortFieldExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#fieldExpression.
	fn visit_fieldexpression(&mut self, ctx: &FieldExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#wcFieldExpression.
	fn visit_wcfieldexpression(&mut self, ctx: &WcFieldExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachPlaceholder.
	fn visit_foreachplaceholder(&mut self, ctx: &ForeachPlaceholderContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#foreachPlaceholderName.
	fn visit_foreachplaceholdername(&mut self, ctx: &ForeachPlaceholderNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#selectFieldExpression.
	fn visit_selectfieldexpression(&mut self, ctx: &SelectFieldExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#renameFieldExpression.
	fn visit_renamefieldexpression(&mut self, ctx: &RenameFieldExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#evalFunctionCall.
	fn visit_evalfunctioncall(&mut self, ctx: &EvalFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#dataTypeFunctionCall.
	fn visit_datatypefunctioncall(&mut self, ctx: &DataTypeFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#convertedDataType.
	fn visit_converteddatatype(&mut self, ctx: &ConvertedDataTypeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#evalFunctionName.
	fn visit_evalfunctionname(&mut self, ctx: &EvalFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#functionArgs.
	fn visit_functionargs(&mut self, ctx: &FunctionArgsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#namedFunctionArgs.
	fn visit_namedfunctionargs(&mut self, ctx: &NamedFunctionArgsContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#functionArg.
	fn visit_functionarg(&mut self, ctx: &FunctionArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#namedFunctionArg.
	fn visit_namedfunctionarg(&mut self, ctx: &NamedFunctionArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#functionArgExpression.
	fn visit_functionargexpression(&mut self, ctx: &FunctionArgExpressionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#lambda.
	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#relevanceArg.
	fn visit_relevancearg(&mut self, ctx: &RelevanceArgContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#relevanceArgName.
	fn visit_relevanceargname(&mut self, ctx: &RelevanceArgNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#relevanceFieldAndWeight.
	fn visit_relevancefieldandweight(&mut self, ctx: &RelevanceFieldAndWeightContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#relevanceFieldWeight.
	fn visit_relevancefieldweight(&mut self, ctx: &RelevanceFieldWeightContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#relevanceField.
	fn visit_relevancefield(&mut self, ctx: &RelevanceFieldContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#relevanceQuery.
	fn visit_relevancequery(&mut self, ctx: &RelevanceQueryContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#relevanceArgValue.
	fn visit_relevanceargvalue(&mut self, ctx: &RelevanceArgValueContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#mathematicalFunctionName.
	fn visit_mathematicalfunctionname(&mut self, ctx: &MathematicalFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#geoipFunctionName.
	fn visit_geoipfunctionname(&mut self, ctx: &GeoipFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#collectionFunctionName.
	fn visit_collectionfunctionname(&mut self, ctx: &CollectionFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#trigonometricFunctionName.
	fn visit_trigonometricfunctionname(&mut self, ctx: &TrigonometricFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#jsonFunctionName.
	fn visit_jsonfunctionname(&mut self, ctx: &JsonFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#cryptographicFunctionName.
	fn visit_cryptographicfunctionname(&mut self, ctx: &CryptographicFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#dateTimeFunctionName.
	fn visit_datetimefunctionname(&mut self, ctx: &DateTimeFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#getFormatFunctionCall.
	fn visit_getformatfunctioncall(&mut self, ctx: &GetFormatFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#getFormatType.
	fn visit_getformattype(&mut self, ctx: &GetFormatTypeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#extractFunctionCall.
	fn visit_extractfunctioncall(&mut self, ctx: &ExtractFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#simpleDateTimePart.
	fn visit_simpledatetimepart(&mut self, ctx: &SimpleDateTimePartContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#complexDateTimePart.
	fn visit_complexdatetimepart(&mut self, ctx: &ComplexDateTimePartContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#datetimePart.
	fn visit_datetimepart(&mut self, ctx: &DatetimePartContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timestampFunctionCall.
	fn visit_timestampfunctioncall(&mut self, ctx: &TimestampFunctionCallContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timestampFunctionName.
	fn visit_timestampfunctionname(&mut self, ctx: &TimestampFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#conditionFunctionName.
	fn visit_conditionfunctionname(&mut self, ctx: &ConditionFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#flowControlFunctionName.
	fn visit_flowcontrolfunctionname(&mut self, ctx: &FlowControlFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#systemFunctionName.
	fn visit_systemfunctionname(&mut self, ctx: &SystemFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#textFunctionName.
	fn visit_textfunctionname(&mut self, ctx: &TextFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#positionFunctionName.
	fn visit_positionfunctionname(&mut self, ctx: &PositionFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#comparisonOperator.
	fn visit_comparisonoperator(&mut self, ctx: &ComparisonOperatorContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#singleFieldRelevanceFunctionName.
	fn visit_singlefieldrelevancefunctionname(&mut self, ctx: &SingleFieldRelevanceFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#multiFieldRelevanceFunctionName.
	fn visit_multifieldrelevancefunctionname(&mut self, ctx: &MultiFieldRelevanceFunctionNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#literalValue.
	fn visit_literalvalue(&mut self, ctx: &LiteralValueContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#intervalLiteral.
	fn visit_intervalliteral(&mut self, ctx: &IntervalLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#stringLiteral.
	fn visit_stringliteral(&mut self, ctx: &StringLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#integerLiteral.
	fn visit_integerliteral(&mut self, ctx: &IntegerLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#decimalLiteral.
	fn visit_decimalliteral(&mut self, ctx: &DecimalLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#doubleLiteral.
	fn visit_doubleliteral(&mut self, ctx: &DoubleLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#floatLiteral.
	fn visit_floatliteral(&mut self, ctx: &FloatLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#booleanLiteral.
	fn visit_booleanliteral(&mut self, ctx: &BooleanLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#datetimeLiteral.
	fn visit_datetimeliteral(&mut self, ctx: &DatetimeLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#dateLiteral.
	fn visit_dateliteral(&mut self, ctx: &DateLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timeLiteral.
	fn visit_timeliteral(&mut self, ctx: &TimeLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#timestampLiteral.
	fn visit_timestampliteral(&mut self, ctx: &TimestampLiteralContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#intervalUnit.
	fn visit_intervalunit(&mut self, ctx: &IntervalUnitContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#valueList.
	fn visit_valuelist(&mut self, ctx: &ValueListContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#identsAsQualifiedName.
	fn visit_identsasqualifiedname(&mut self, ctx: &IdentsAsQualifiedNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#identsAsTableQualifiedName.
	fn visit_identsastablequalifiedname(&mut self, ctx: &IdentsAsTableQualifiedNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#identsAsWildcardQualifiedName.
	fn visit_identsaswildcardqualifiedname(&mut self, ctx: &IdentsAsWildcardQualifiedNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#identsAsQualifiedNameSeq.
	fn visit_identsasqualifiednameseq(&mut self, ctx: &IdentsAsQualifiedNameSeqContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#ident.
	fn visit_ident(&mut self, ctx: &IdentContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#tableIdent.
	fn visit_tableident(&mut self, ctx: &TableIdentContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#wildcard.
	fn visit_wildcard(&mut self, ctx: &WildcardContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#keywordsCanBeId.
	fn visit_keywordscanbeid(&mut self, ctx: &KeywordsCanBeIdContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by OpenSearchPPLParser#searchableKeyWord.
	fn visit_searchablekeyword(&mut self, ctx: &SearchableKeyWordContext<'input>) {
            self.visit_children(ctx)
        }

}
