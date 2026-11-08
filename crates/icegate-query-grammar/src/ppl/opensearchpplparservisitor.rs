#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]
#![allow(nonstandard_style)]
// Generated from OpenSearchPPLParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::{ParseTreeVisitor,ParseTreeVisitorCompat};
use super::opensearchpplparser::*;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link OpenSearchPPLParser}.
 */
pub trait OpenSearchPPLParserVisitor<'input>: ParseTreeVisitor<'input,OpenSearchPPLParserContextType>{
	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#root}.
	 * @param ctx the parse tree
	 */
	fn visit_root(&mut self, ctx: &RootContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#pplStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_pplStatement(&mut self, ctx: &PplStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#subPipeline}.
	 * @param ctx the parse tree
	 */
	fn visit_subPipeline(&mut self, ctx: &SubPipelineContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#queryStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_queryStatement(&mut self, ctx: &QueryStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#explainStatement}.
	 * @param ctx the parse tree
	 */
	fn visit_explainStatement(&mut self, ctx: &ExplainStatementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#explainMode}.
	 * @param ctx the parse tree
	 */
	fn visit_explainMode(&mut self, ctx: &ExplainModeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#subSearch}.
	 * @param ctx the parse tree
	 */
	fn visit_subSearch(&mut self, ctx: &SubSearchContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#pplCommands}.
	 * @param ctx the parse tree
	 */
	fn visit_pplCommands(&mut self, ctx: &PplCommandsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#commands}.
	 * @param ctx the parse tree
	 */
	fn visit_commands(&mut self, ctx: &CommandsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#commandName}.
	 * @param ctx the parse tree
	 */
	fn visit_commandName(&mut self, ctx: &CommandNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchFrom}
	 * labeled alternative in {@link OpenSearchPPLParser#searchCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_searchFrom(&mut self, ctx: &SearchFromContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code orExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_orExpression(&mut self, ctx: &OrExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code timeModifierExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_timeModifierExpression(&mut self, ctx: &TimeModifierExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code andExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_andExpression(&mut self, ctx: &AndExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code notExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_notExpression(&mut self, ctx: &NotExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code termExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_termExpression(&mut self, ctx: &TermExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code groupedExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_groupedExpression(&mut self, ctx: &GroupedExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchComparisonTerm}
	 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_searchComparisonTerm(&mut self, ctx: &SearchComparisonTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchInListTerm}
	 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_searchInListTerm(&mut self, ctx: &SearchInListTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchLiteralTerm}
	 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_searchLiteralTerm(&mut self, ctx: &SearchLiteralTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#searchLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_searchLiteral(&mut self, ctx: &SearchLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchFieldCompare}
	 * labeled alternative in {@link OpenSearchPPLParser#searchFieldComparison}.
	 * @param ctx the parse tree
	 */
	fn visit_searchFieldCompare(&mut self, ctx: &SearchFieldCompareContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchFieldInValues}
	 * labeled alternative in {@link OpenSearchPPLParser#searchFieldInList}.
	 * @param ctx the parse tree
	 */
	fn visit_searchFieldInValues(&mut self, ctx: &SearchFieldInValuesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code searchLiterals}
	 * labeled alternative in {@link OpenSearchPPLParser#searchLiteralList}.
	 * @param ctx the parse tree
	 */
	fn visit_searchLiterals(&mut self, ctx: &SearchLiteralsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code equals}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_equals(&mut self, ctx: &EqualsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code notEquals}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_notEquals(&mut self, ctx: &NotEqualsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lessThan}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_lessThan(&mut self, ctx: &LessThanContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lessOrEqual}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_lessOrEqual(&mut self, ctx: &LessOrEqualContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code greaterThan}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_greaterThan(&mut self, ctx: &GreaterThanContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code greaterOrEqual}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_greaterOrEqual(&mut self, ctx: &GreaterOrEqualContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#describeCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_describeCommand(&mut self, ctx: &DescribeCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#restCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_restCommand(&mut self, ctx: &RestCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#restArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_restArgument(&mut self, ctx: &RestArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#showDataSourcesCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_showDataSourcesCommand(&mut self, ctx: &ShowDataSourcesCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#makeresultsCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_makeresultsCommand(&mut self, ctx: &MakeresultsCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#makeresultsArg}.
	 * @param ctx the parse tree
	 */
	fn visit_makeresultsArg(&mut self, ctx: &MakeresultsArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#whereCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_whereCommand(&mut self, ctx: &WhereCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldsCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldsCommand(&mut self, ctx: &FieldsCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_tableCommand(&mut self, ctx: &TableCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldsCommandBody}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldsCommandBody(&mut self, ctx: &FieldsCommandBodyContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#wcFieldList}.
	 * @param ctx the parse tree
	 */
	fn visit_wcFieldList(&mut self, ctx: &WcFieldListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#renameCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_renameCommand(&mut self, ctx: &RenameCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#replaceCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_replaceCommand(&mut self, ctx: &ReplaceCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#replacePair}.
	 * @param ctx the parse tree
	 */
	fn visit_replacePair(&mut self, ctx: &ReplacePairContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_statsCommand(&mut self, ctx: &StatsCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#appendPipeCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_appendPipeCommand(&mut self, ctx: &AppendPipeCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsArgs}.
	 * @param ctx the parse tree
	 */
	fn visit_statsArgs(&mut self, ctx: &StatsArgsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#partitionsArg}.
	 * @param ctx the parse tree
	 */
	fn visit_partitionsArg(&mut self, ctx: &PartitionsArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#allnumArg}.
	 * @param ctx the parse tree
	 */
	fn visit_allnumArg(&mut self, ctx: &AllnumArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#delimArg}.
	 * @param ctx the parse tree
	 */
	fn visit_delimArg(&mut self, ctx: &DelimArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#bucketNullableArg}.
	 * @param ctx the parse tree
	 */
	fn visit_bucketNullableArg(&mut self, ctx: &BucketNullableArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dedupSplitArg}.
	 * @param ctx the parse tree
	 */
	fn visit_dedupSplitArg(&mut self, ctx: &DedupSplitArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#eventstatsCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_eventstatsCommand(&mut self, ctx: &EventstatsCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#streamstatsCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_streamstatsCommand(&mut self, ctx: &StreamstatsCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#streamstatsArgs}.
	 * @param ctx the parse tree
	 */
	fn visit_streamstatsArgs(&mut self, ctx: &StreamstatsArgsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#currentArg}.
	 * @param ctx the parse tree
	 */
	fn visit_currentArg(&mut self, ctx: &CurrentArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#windowArg}.
	 * @param ctx the parse tree
	 */
	fn visit_windowArg(&mut self, ctx: &WindowArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#globalArg}.
	 * @param ctx the parse tree
	 */
	fn visit_globalArg(&mut self, ctx: &GlobalArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#resetBeforeArg}.
	 * @param ctx the parse tree
	 */
	fn visit_resetBeforeArg(&mut self, ctx: &ResetBeforeArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#resetAfterArg}.
	 * @param ctx the parse tree
	 */
	fn visit_resetAfterArg(&mut self, ctx: &ResetAfterArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dedupCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_dedupCommand(&mut self, ctx: &DedupCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sortCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_sortCommand(&mut self, ctx: &SortCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#reverseCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_reverseCommand(&mut self, ctx: &ReverseCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#chartCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_chartCommand(&mut self, ctx: &ChartCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#chartOptions}.
	 * @param ctx the parse tree
	 */
	fn visit_chartOptions(&mut self, ctx: &ChartOptionsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rowSplit}.
	 * @param ctx the parse tree
	 */
	fn visit_rowSplit(&mut self, ctx: &RowSplitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#columnSplit}.
	 * @param ctx the parse tree
	 */
	fn visit_columnSplit(&mut self, ctx: &ColumnSplitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timechartCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_timechartCommand(&mut self, ctx: &TimechartCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#transposeCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_transposeCommand(&mut self, ctx: &TransposeCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#transposeParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_transposeParameter(&mut self, ctx: &TransposeParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timewrapCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_timewrapCommand(&mut self, ctx: &TimewrapCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timewrapParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_timewrapParameter(&mut self, ctx: &TimewrapParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timewrapAlign}.
	 * @param ctx the parse tree
	 */
	fn visit_timewrapAlign(&mut self, ctx: &TimewrapAlignContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timewrapSeries}.
	 * @param ctx the parse tree
	 */
	fn visit_timewrapSeries(&mut self, ctx: &TimewrapSeriesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timechartParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_timechartParameter(&mut self, ctx: &TimechartParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#spanLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_spanLiteral(&mut self, ctx: &SpanLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_evalCommand(&mut self, ctx: &EvalCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachCommand(&mut self, ctx: &ForeachCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachArgument}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachArgument(&mut self, ctx: &ForeachArgumentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachTarget(&mut self, ctx: &ForeachTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachOption}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachOption(&mut self, ctx: &ForeachOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachEvalCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachEvalCommand(&mut self, ctx: &ForeachEvalCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachEvalClause}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachEvalClause(&mut self, ctx: &ForeachEvalClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachEvalTarget}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachEvalTarget(&mut self, ctx: &ForeachEvalTargetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldformatCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldformatCommand(&mut self, ctx: &FieldformatCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#headCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_headCommand(&mut self, ctx: &HeadCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#binCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_binCommand(&mut self, ctx: &BinCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#binOption}.
	 * @param ctx the parse tree
	 */
	fn visit_binOption(&mut self, ctx: &BinOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#aligntimeValue}.
	 * @param ctx the parse tree
	 */
	fn visit_aligntimeValue(&mut self, ctx: &AligntimeValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code numericSpanValue}
	 * labeled alternative in {@link OpenSearchPPLParser#binSpanValue}.
	 * @param ctx the parse tree
	 */
	fn visit_numericSpanValue(&mut self, ctx: &NumericSpanValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logBasedSpanValue}
	 * labeled alternative in {@link OpenSearchPPLParser#binSpanValue}.
	 * @param ctx the parse tree
	 */
	fn visit_logBasedSpanValue(&mut self, ctx: &LogBasedSpanValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logWithBaseSpan}
	 * labeled alternative in {@link OpenSearchPPLParser#logSpanValue}.
	 * @param ctx the parse tree
	 */
	fn visit_logWithBaseSpan(&mut self, ctx: &LogWithBaseSpanContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rareTopCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_rareTopCommand(&mut self, ctx: &RareTopCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rareTopOption}.
	 * @param ctx the parse tree
	 */
	fn visit_rareTopOption(&mut self, ctx: &RareTopOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#grokCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_grokCommand(&mut self, ctx: &GrokCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#parseCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_parseCommand(&mut self, ctx: &ParseCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#spathCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_spathCommand(&mut self, ctx: &SpathCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#spathParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_spathParameter(&mut self, ctx: &SpathParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#indexablePath}.
	 * @param ctx the parse tree
	 */
	fn visit_indexablePath(&mut self, ctx: &IndexablePathContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#pathElement}.
	 * @param ctx the parse tree
	 */
	fn visit_pathElement(&mut self, ctx: &PathElementContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#pathArrayAccess}.
	 * @param ctx the parse tree
	 */
	fn visit_pathArrayAccess(&mut self, ctx: &PathArrayAccessContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#regexCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_regexCommand(&mut self, ctx: &RegexCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#regexExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_regexExpr(&mut self, ctx: &RegexExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rexCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_rexCommand(&mut self, ctx: &RexCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rexExpr}.
	 * @param ctx the parse tree
	 */
	fn visit_rexExpr(&mut self, ctx: &RexExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rexOption}.
	 * @param ctx the parse tree
	 */
	fn visit_rexOption(&mut self, ctx: &RexOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternsMethod}.
	 * @param ctx the parse tree
	 */
	fn visit_patternsMethod(&mut self, ctx: &PatternsMethodContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternsCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_patternsCommand(&mut self, ctx: &PatternsCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternsCommandOption}.
	 * @param ctx the parse tree
	 */
	fn visit_patternsCommandOption(&mut self, ctx: &PatternsCommandOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternsParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_patternsParameter(&mut self, ctx: &PatternsParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternMethod}.
	 * @param ctx the parse tree
	 */
	fn visit_patternMethod(&mut self, ctx: &PatternMethodContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternMode}.
	 * @param ctx the parse tree
	 */
	fn visit_patternMode(&mut self, ctx: &PatternModeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#lookupCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_lookupCommand(&mut self, ctx: &LookupCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#lookupMappingList}.
	 * @param ctx the parse tree
	 */
	fn visit_lookupMappingList(&mut self, ctx: &LookupMappingListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#outputCandidateList}.
	 * @param ctx the parse tree
	 */
	fn visit_outputCandidateList(&mut self, ctx: &OutputCandidateListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#lookupPair}.
	 * @param ctx the parse tree
	 */
	fn visit_lookupPair(&mut self, ctx: &LookupPairContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code fillNullWithClause}
	 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_fillNullWithClause(&mut self, ctx: &FillNullWithClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code fillNullUsingClause}
	 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_fillNullUsingClause(&mut self, ctx: &FillNullUsingClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code fillNullValueWithFields}
	 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_fillNullValueWithFields(&mut self, ctx: &FillNullValueWithFieldsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code fillNullValueAllFields}
	 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_fillNullValueAllFields(&mut self, ctx: &FillNullValueAllFieldsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fillNullWith}.
	 * @param ctx the parse tree
	 */
	fn visit_fillNullWith(&mut self, ctx: &FillNullWithContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fillNullUsing}.
	 * @param ctx the parse tree
	 */
	fn visit_fillNullUsing(&mut self, ctx: &FillNullUsingContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#replacementPair}.
	 * @param ctx the parse tree
	 */
	fn visit_replacementPair(&mut self, ctx: &ReplacementPairContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#convertCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_convertCommand(&mut self, ctx: &ConvertCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#convertFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_convertFunction(&mut self, ctx: &ConvertFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#trendlineCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_trendlineCommand(&mut self, ctx: &TrendlineCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#trendlineClause}.
	 * @param ctx the parse tree
	 */
	fn visit_trendlineClause(&mut self, ctx: &TrendlineClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#trendlineType}.
	 * @param ctx the parse tree
	 */
	fn visit_trendlineType(&mut self, ctx: &TrendlineTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#expandCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_expandCommand(&mut self, ctx: &ExpandCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mvcombineCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_mvcombineCommand(&mut self, ctx: &MvcombineCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#nomvCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_nomvCommand(&mut self, ctx: &NomvCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mvexpandCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_mvexpandCommand(&mut self, ctx: &MvexpandCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#flattenCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_flattenCommand(&mut self, ctx: &FlattenCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#appendcolCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_appendcolCommand(&mut self, ctx: &AppendcolCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#appendCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_appendCommand(&mut self, ctx: &AppendCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#multisearchCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_multisearchCommand(&mut self, ctx: &MultisearchCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#unionCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_unionCommand(&mut self, ctx: &UnionCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#subsearchOptions}.
	 * @param ctx the parse tree
	 */
	fn visit_subsearchOptions(&mut self, ctx: &SubsearchOptionsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#unionDataset}.
	 * @param ctx the parse tree
	 */
	fn visit_unionDataset(&mut self, ctx: &UnionDatasetContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#kmeansCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_kmeansCommand(&mut self, ctx: &KmeansCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#kmeansParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_kmeansParameter(&mut self, ctx: &KmeansParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#adCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_adCommand(&mut self, ctx: &AdCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#adParameter}.
	 * @param ctx the parse tree
	 */
	fn visit_adParameter(&mut self, ctx: &AdParameterContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mlCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_mlCommand(&mut self, ctx: &MlCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mlArg}.
	 * @param ctx the parse tree
	 */
	fn visit_mlArg(&mut self, ctx: &MlArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#addtotalsCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_addtotalsCommand(&mut self, ctx: &AddtotalsCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#addtotalsOption}.
	 * @param ctx the parse tree
	 */
	fn visit_addtotalsOption(&mut self, ctx: &AddtotalsOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#addcoltotalsCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_addcoltotalsCommand(&mut self, ctx: &AddcoltotalsCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#addcoltotalsOption}.
	 * @param ctx the parse tree
	 */
	fn visit_addcoltotalsOption(&mut self, ctx: &AddcoltotalsOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#graphLookupCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_graphLookupCommand(&mut self, ctx: &GraphLookupCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#startClause}.
	 * @param ctx the parse tree
	 */
	fn visit_startClause(&mut self, ctx: &StartClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#edgeClause}.
	 * @param ctx the parse tree
	 */
	fn visit_edgeClause(&mut self, ctx: &EdgeClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#graphLookupArgs}.
	 * @param ctx the parse tree
	 */
	fn visit_graphLookupArgs(&mut self, ctx: &GraphLookupArgsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#xyseriesCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_xyseriesCommand(&mut self, ctx: &XyseriesCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#xyseriesOption}.
	 * @param ctx the parse tree
	 */
	fn visit_xyseriesOption(&mut self, ctx: &XyseriesOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#xyseriesPivotValues}.
	 * @param ctx the parse tree
	 */
	fn visit_xyseriesPivotValues(&mut self, ctx: &XyseriesPivotValuesContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fromClause}.
	 * @param ctx the parse tree
	 */
	fn visit_fromClause(&mut self, ctx: &FromClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableOrSubqueryClause}.
	 * @param ctx the parse tree
	 */
	fn visit_tableOrSubqueryClause(&mut self, ctx: &TableOrSubqueryClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableSourceClause}.
	 * @param ctx the parse tree
	 */
	fn visit_tableSourceClause(&mut self, ctx: &TableSourceClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dynamicSourceClause}.
	 * @param ctx the parse tree
	 */
	fn visit_dynamicSourceClause(&mut self, ctx: &DynamicSourceClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sourceReference}.
	 * @param ctx the parse tree
	 */
	fn visit_sourceReference(&mut self, ctx: &SourceReferenceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sourceFilterArg}.
	 * @param ctx the parse tree
	 */
	fn visit_sourceFilterArg(&mut self, ctx: &SourceFilterArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#joinCommand}.
	 * @param ctx the parse tree
	 */
	fn visit_joinCommand(&mut self, ctx: &JoinCommandContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sqlLikeJoinType}.
	 * @param ctx the parse tree
	 */
	fn visit_sqlLikeJoinType(&mut self, ctx: &SqlLikeJoinTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#joinType}.
	 * @param ctx the parse tree
	 */
	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sideAlias}.
	 * @param ctx the parse tree
	 */
	fn visit_sideAlias(&mut self, ctx: &SideAliasContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#joinHintList}.
	 * @param ctx the parse tree
	 */
	fn visit_joinHintList(&mut self, ctx: &JoinHintListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code leftHint}
	 * labeled alternative in {@link OpenSearchPPLParser#hintPair}.
	 * @param ctx the parse tree
	 */
	fn visit_leftHint(&mut self, ctx: &LeftHintContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rightHint}
	 * labeled alternative in {@link OpenSearchPPLParser#hintPair}.
	 * @param ctx the parse tree
	 */
	fn visit_rightHint(&mut self, ctx: &RightHintContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code overwriteOption}
	 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
	 * @param ctx the parse tree
	 */
	fn visit_overwriteOption(&mut self, ctx: &OverwriteOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code typeOption}
	 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
	 * @param ctx the parse tree
	 */
	fn visit_typeOption(&mut self, ctx: &TypeOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code maxOption}
	 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
	 * @param ctx the parse tree
	 */
	fn visit_maxOption(&mut self, ctx: &MaxOptionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#renameClause}.
	 * @param ctx the parse tree
	 */
	fn visit_renameClause(&mut self, ctx: &RenameClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#byClause}.
	 * @param ctx the parse tree
	 */
	fn visit_byClause(&mut self, ctx: &ByClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsByClause}.
	 * @param ctx the parse tree
	 */
	fn visit_statsByClause(&mut self, ctx: &StatsByClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#bySpanClause}.
	 * @param ctx the parse tree
	 */
	fn visit_bySpanClause(&mut self, ctx: &BySpanClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#spanClause}.
	 * @param ctx the parse tree
	 */
	fn visit_spanClause(&mut self, ctx: &SpanClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sortbyClause}.
	 * @param ctx the parse tree
	 */
	fn visit_sortbyClause(&mut self, ctx: &SortbyClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalClause}.
	 * @param ctx the parse tree
	 */
	fn visit_evalClause(&mut self, ctx: &EvalClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldFormatEvalClause}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldFormatEvalClause(&mut self, ctx: &FieldFormatEvalClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#eventstatsAggTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_eventstatsAggTerm(&mut self, ctx: &EventstatsAggTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#streamstatsAggTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_streamstatsAggTerm(&mut self, ctx: &StreamstatsAggTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#windowFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_windowFunction(&mut self, ctx: &WindowFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#windowFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_windowFunctionName(&mut self, ctx: &WindowFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#scalarWindowFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_scalarWindowFunctionName(&mut self, ctx: &ScalarWindowFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsAggTerm}.
	 * @param ctx the parse tree
	 */
	fn visit_statsAggTerm(&mut self, ctx: &StatsAggTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code countEvalFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_countEvalFunctionCall(&mut self, ctx: &CountEvalFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code countAllFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_countAllFunctionCall(&mut self, ctx: &CountAllFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code percentileShortcutFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_percentileShortcutFunctionCall(&mut self, ctx: &PercentileShortcutFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code distinctCountFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_distinctCountFunctionCall(&mut self, ctx: &DistinctCountFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code takeAggFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_takeAggFunctionCall(&mut self, ctx: &TakeAggFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code valuesAggFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_valuesAggFunctionCall(&mut self, ctx: &ValuesAggFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code percentileApproxFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_percentileApproxFunctionCall(&mut self, ctx: &PercentileApproxFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code perFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_perFunctionCall(&mut self, ctx: &PerFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code statsFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_statsFunctionCall(&mut self, ctx: &StatsFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_statsFunctionName(&mut self, ctx: &StatsFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#takeAggFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_takeAggFunction(&mut self, ctx: &TakeAggFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#valuesAggFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_valuesAggFunction(&mut self, ctx: &ValuesAggFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#percentileApproxFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_percentileApproxFunction(&mut self, ctx: &PercentileApproxFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#perFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_perFunction(&mut self, ctx: &PerFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code stringDotlogicalExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_stringDotlogicalExpression(&mut self, ctx: &StringDotlogicalExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code stringDotlogicalExpressionDotString}
	 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_stringDotlogicalExpressionDotString(&mut self, ctx: &StringDotlogicalExpressionDotStringContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalExpressionDotString}
	 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalExpressionDotString(&mut self, ctx: &LogicalExpressionDotStringContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code ffStandardLogicalExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_ffStandardLogicalExpression(&mut self, ctx: &FfStandardLogicalExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalExpr(&mut self, ctx: &LogicalExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalAnd}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalAnd(&mut self, ctx: &LogicalAndContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalXor}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalXor(&mut self, ctx: &LogicalXorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code logicalOr}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_logicalOr(&mut self, ctx: &LogicalOrContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code relevanceExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_relevanceExpr(&mut self, ctx: &RelevanceExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code valueExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_valueExpr(&mut self, ctx: &ValueExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_inExpr(&mut self, ctx: &InExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code isNullPredicate}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_isNullPredicate(&mut self, ctx: &IsNullPredicateContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_between(&mut self, ctx: &BetweenContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code compareExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
	fn visit_compareExpr(&mut self, ctx: &CompareExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#nullNotnull}.
	 * @param ctx the parse tree
	 */
	fn visit_nullNotnull(&mut self, ctx: &NullNotnullContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code inSubqueryExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_inSubqueryExpr(&mut self, ctx: &InSubqueryExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code lambdaExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_lambdaExpr(&mut self, ctx: &LambdaExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code literalValueExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_literalValueExpr(&mut self, ctx: &LiteralValueExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code functionCallExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCallExpr(&mut self, ctx: &FunctionCallExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code existsSubqueryExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_existsSubqueryExpr(&mut self, ctx: &ExistsSubqueryExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code foreachPlaceholderExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachPlaceholderExpr(&mut self, ctx: &ForeachPlaceholderExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code scalarSubqueryExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_scalarSubqueryExpr(&mut self, ctx: &ScalarSubqueryExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code nestedValueExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_nestedValueExpr(&mut self, ctx: &NestedValueExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code binaryArithmetic}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_binaryArithmetic(&mut self, ctx: &BinaryArithmeticContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code fieldExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldExpr(&mut self, ctx: &FieldExprContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_evalExpression(&mut self, ctx: &EvalExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#functionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mvmapFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_mvmapFunctionCall(&mut self, ctx: &MvmapFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#positionFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_positionFunctionCall(&mut self, ctx: &PositionFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#caseFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_caseFunctionCall(&mut self, ctx: &CaseFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_relevanceExpression(&mut self, ctx: &RelevanceExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#singleFieldRelevanceFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_singleFieldRelevanceFunction(&mut self, ctx: &SingleFieldRelevanceFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#multiFieldRelevanceFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_multiFieldRelevanceFunction(&mut self, ctx: &MultiFieldRelevanceFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timeModifier}.
	 * @param ctx the parse tree
	 */
	fn visit_timeModifier(&mut self, ctx: &TimeModifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timeModifierValue}.
	 * @param ctx the parse tree
	 */
	fn visit_timeModifierValue(&mut self, ctx: &TimeModifierValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableSource}.
	 * @param ctx the parse tree
	 */
	fn visit_tableSource(&mut self, ctx: &TableSourceContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableFunction}.
	 * @param ctx the parse tree
	 */
	fn visit_tableFunction(&mut self, ctx: &TableFunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldList}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldList(&mut self, ctx: &FieldListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code invalidMixedSortField}
	 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
	 * @param ctx the parse tree
	 */
	fn visit_invalidMixedSortField(&mut self, ctx: &InvalidMixedSortFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prefixSortField}
	 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
	 * @param ctx the parse tree
	 */
	fn visit_prefixSortField(&mut self, ctx: &PrefixSortFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code suffixSortField}
	 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
	 * @param ctx the parse tree
	 */
	fn visit_suffixSortField(&mut self, ctx: &SuffixSortFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code defaultSortField}
	 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
	 * @param ctx the parse tree
	 */
	fn visit_defaultSortField(&mut self, ctx: &DefaultSortFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sortFieldExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_sortFieldExpression(&mut self, ctx: &SortFieldExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldExpression(&mut self, ctx: &FieldExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#wcFieldExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_wcFieldExpression(&mut self, ctx: &WcFieldExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachPlaceholder}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachPlaceholder(&mut self, ctx: &ForeachPlaceholderContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachPlaceholderName}.
	 * @param ctx the parse tree
	 */
	fn visit_foreachPlaceholderName(&mut self, ctx: &ForeachPlaceholderNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#selectFieldExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_selectFieldExpression(&mut self, ctx: &SelectFieldExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#renameFieldExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_renameFieldExpression(&mut self, ctx: &RenameFieldExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_evalFunctionCall(&mut self, ctx: &EvalFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dataTypeFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_dataTypeFunctionCall(&mut self, ctx: &DataTypeFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#convertedDataType}.
	 * @param ctx the parse tree
	 */
	fn visit_convertedDataType(&mut self, ctx: &ConvertedDataTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_evalFunctionName(&mut self, ctx: &EvalFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#functionArgs}.
	 * @param ctx the parse tree
	 */
	fn visit_functionArgs(&mut self, ctx: &FunctionArgsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#namedFunctionArgs}.
	 * @param ctx the parse tree
	 */
	fn visit_namedFunctionArgs(&mut self, ctx: &NamedFunctionArgsContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#functionArg}.
	 * @param ctx the parse tree
	 */
	fn visit_functionArg(&mut self, ctx: &FunctionArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#namedFunctionArg}.
	 * @param ctx the parse tree
	 */
	fn visit_namedFunctionArg(&mut self, ctx: &NamedFunctionArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#functionArgExpression}.
	 * @param ctx the parse tree
	 */
	fn visit_functionArgExpression(&mut self, ctx: &FunctionArgExpressionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#lambda}.
	 * @param ctx the parse tree
	 */
	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceArg}.
	 * @param ctx the parse tree
	 */
	fn visit_relevanceArg(&mut self, ctx: &RelevanceArgContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceArgName}.
	 * @param ctx the parse tree
	 */
	fn visit_relevanceArgName(&mut self, ctx: &RelevanceArgNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceFieldAndWeight}.
	 * @param ctx the parse tree
	 */
	fn visit_relevanceFieldAndWeight(&mut self, ctx: &RelevanceFieldAndWeightContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceFieldWeight}.
	 * @param ctx the parse tree
	 */
	fn visit_relevanceFieldWeight(&mut self, ctx: &RelevanceFieldWeightContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceField}.
	 * @param ctx the parse tree
	 */
	fn visit_relevanceField(&mut self, ctx: &RelevanceFieldContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceQuery}.
	 * @param ctx the parse tree
	 */
	fn visit_relevanceQuery(&mut self, ctx: &RelevanceQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceArgValue}.
	 * @param ctx the parse tree
	 */
	fn visit_relevanceArgValue(&mut self, ctx: &RelevanceArgValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mathematicalFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_mathematicalFunctionName(&mut self, ctx: &MathematicalFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#geoipFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_geoipFunctionName(&mut self, ctx: &GeoipFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#collectionFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_collectionFunctionName(&mut self, ctx: &CollectionFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#trigonometricFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_trigonometricFunctionName(&mut self, ctx: &TrigonometricFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#jsonFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_jsonFunctionName(&mut self, ctx: &JsonFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#cryptographicFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_cryptographicFunctionName(&mut self, ctx: &CryptographicFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dateTimeFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_dateTimeFunctionName(&mut self, ctx: &DateTimeFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#getFormatFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_getFormatFunctionCall(&mut self, ctx: &GetFormatFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#getFormatType}.
	 * @param ctx the parse tree
	 */
	fn visit_getFormatType(&mut self, ctx: &GetFormatTypeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#extractFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_extractFunctionCall(&mut self, ctx: &ExtractFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#simpleDateTimePart}.
	 * @param ctx the parse tree
	 */
	fn visit_simpleDateTimePart(&mut self, ctx: &SimpleDateTimePartContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#complexDateTimePart}.
	 * @param ctx the parse tree
	 */
	fn visit_complexDateTimePart(&mut self, ctx: &ComplexDateTimePartContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#datetimePart}.
	 * @param ctx the parse tree
	 */
	fn visit_datetimePart(&mut self, ctx: &DatetimePartContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timestampFunctionCall}.
	 * @param ctx the parse tree
	 */
	fn visit_timestampFunctionCall(&mut self, ctx: &TimestampFunctionCallContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timestampFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_timestampFunctionName(&mut self, ctx: &TimestampFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#conditionFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_conditionFunctionName(&mut self, ctx: &ConditionFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#flowControlFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_flowControlFunctionName(&mut self, ctx: &FlowControlFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#systemFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_systemFunctionName(&mut self, ctx: &SystemFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#textFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_textFunctionName(&mut self, ctx: &TextFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#positionFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_positionFunctionName(&mut self, ctx: &PositionFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#singleFieldRelevanceFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_singleFieldRelevanceFunctionName(&mut self, ctx: &SingleFieldRelevanceFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#multiFieldRelevanceFunctionName}.
	 * @param ctx the parse tree
	 */
	fn visit_multiFieldRelevanceFunctionName(&mut self, ctx: &MultiFieldRelevanceFunctionNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#literalValue}.
	 * @param ctx the parse tree
	 */
	fn visit_literalValue(&mut self, ctx: &LiteralValueContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#intervalLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#integerLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#decimalLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#doubleLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#floatLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_floatLiteral(&mut self, ctx: &FloatLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#booleanLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#datetimeLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_datetimeLiteral(&mut self, ctx: &DatetimeLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dateLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_dateLiteral(&mut self, ctx: &DateLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timeLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_timeLiteral(&mut self, ctx: &TimeLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timestampLiteral}.
	 * @param ctx the parse tree
	 */
	fn visit_timestampLiteral(&mut self, ctx: &TimestampLiteralContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#intervalUnit}.
	 * @param ctx the parse tree
	 */
	fn visit_intervalUnit(&mut self, ctx: &IntervalUnitContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#valueList}.
	 * @param ctx the parse tree
	 */
	fn visit_valueList(&mut self, ctx: &ValueListContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identsAsQualifiedName}
	 * labeled alternative in {@link OpenSearchPPLParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_identsAsQualifiedName(&mut self, ctx: &IdentsAsQualifiedNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identsAsTableQualifiedName}
	 * labeled alternative in {@link OpenSearchPPLParser#tableQualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_identsAsTableQualifiedName(&mut self, ctx: &IdentsAsTableQualifiedNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identsAsWildcardQualifiedName}
	 * labeled alternative in {@link OpenSearchPPLParser#wcQualifiedName}.
	 * @param ctx the parse tree
	 */
	fn visit_identsAsWildcardQualifiedName(&mut self, ctx: &IdentsAsWildcardQualifiedNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code identsAsQualifiedNameSeq}
	 * labeled alternative in {@link OpenSearchPPLParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
	fn visit_identsAsQualifiedNameSeq(&mut self, ctx: &IdentsAsQualifiedNameSeqContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#ident}.
	 * @param ctx the parse tree
	 */
	fn visit_ident(&mut self, ctx: &IdentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableIdent}.
	 * @param ctx the parse tree
	 */
	fn visit_tableIdent(&mut self, ctx: &TableIdentContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#wildcard}.
	 * @param ctx the parse tree
	 */
	fn visit_wildcard(&mut self, ctx: &WildcardContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#keywordsCanBeId}.
	 * @param ctx the parse tree
	 */
	fn visit_keywordsCanBeId(&mut self, ctx: &KeywordsCanBeIdContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#searchableKeyWord}.
	 * @param ctx the parse tree
	 */
	fn visit_searchableKeyWord(&mut self, ctx: &SearchableKeyWordContext<'input>) { self.visit_children(ctx) }

}

pub trait OpenSearchPPLParserVisitorCompat<'input>:ParseTreeVisitorCompat<'input, Node= OpenSearchPPLParserContextType>{
	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#root}.
	 * @param ctx the parse tree
	 */
		fn visit_root(&mut self, ctx: &RootContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#pplStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_pplStatement(&mut self, ctx: &PplStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#subPipeline}.
	 * @param ctx the parse tree
	 */
		fn visit_subPipeline(&mut self, ctx: &SubPipelineContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#queryStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_queryStatement(&mut self, ctx: &QueryStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#explainStatement}.
	 * @param ctx the parse tree
	 */
		fn visit_explainStatement(&mut self, ctx: &ExplainStatementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#explainMode}.
	 * @param ctx the parse tree
	 */
		fn visit_explainMode(&mut self, ctx: &ExplainModeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#subSearch}.
	 * @param ctx the parse tree
	 */
		fn visit_subSearch(&mut self, ctx: &SubSearchContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#pplCommands}.
	 * @param ctx the parse tree
	 */
		fn visit_pplCommands(&mut self, ctx: &PplCommandsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#commands}.
	 * @param ctx the parse tree
	 */
		fn visit_commands(&mut self, ctx: &CommandsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#commandName}.
	 * @param ctx the parse tree
	 */
		fn visit_commandName(&mut self, ctx: &CommandNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchFrom}
	 * labeled alternative in {@link OpenSearchPPLParser#searchCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_searchFrom(&mut self, ctx: &SearchFromContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code orExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_orExpression(&mut self, ctx: &OrExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code timeModifierExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_timeModifierExpression(&mut self, ctx: &TimeModifierExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code andExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_andExpression(&mut self, ctx: &AndExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code notExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_notExpression(&mut self, ctx: &NotExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code termExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_termExpression(&mut self, ctx: &TermExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code groupedExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_groupedExpression(&mut self, ctx: &GroupedExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchComparisonTerm}
	 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_searchComparisonTerm(&mut self, ctx: &SearchComparisonTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchInListTerm}
	 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_searchInListTerm(&mut self, ctx: &SearchInListTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchLiteralTerm}
	 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_searchLiteralTerm(&mut self, ctx: &SearchLiteralTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#searchLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_searchLiteral(&mut self, ctx: &SearchLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchFieldCompare}
	 * labeled alternative in {@link OpenSearchPPLParser#searchFieldComparison}.
	 * @param ctx the parse tree
	 */
		fn visit_searchFieldCompare(&mut self, ctx: &SearchFieldCompareContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchFieldInValues}
	 * labeled alternative in {@link OpenSearchPPLParser#searchFieldInList}.
	 * @param ctx the parse tree
	 */
		fn visit_searchFieldInValues(&mut self, ctx: &SearchFieldInValuesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code searchLiterals}
	 * labeled alternative in {@link OpenSearchPPLParser#searchLiteralList}.
	 * @param ctx the parse tree
	 */
		fn visit_searchLiterals(&mut self, ctx: &SearchLiteralsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code equals}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_equals(&mut self, ctx: &EqualsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code notEquals}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_notEquals(&mut self, ctx: &NotEqualsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lessThan}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_lessThan(&mut self, ctx: &LessThanContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lessOrEqual}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_lessOrEqual(&mut self, ctx: &LessOrEqualContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code greaterThan}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_greaterThan(&mut self, ctx: &GreaterThanContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code greaterOrEqual}
	 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_greaterOrEqual(&mut self, ctx: &GreaterOrEqualContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#describeCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_describeCommand(&mut self, ctx: &DescribeCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#restCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_restCommand(&mut self, ctx: &RestCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#restArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_restArgument(&mut self, ctx: &RestArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#showDataSourcesCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_showDataSourcesCommand(&mut self, ctx: &ShowDataSourcesCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#makeresultsCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_makeresultsCommand(&mut self, ctx: &MakeresultsCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#makeresultsArg}.
	 * @param ctx the parse tree
	 */
		fn visit_makeresultsArg(&mut self, ctx: &MakeresultsArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#whereCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_whereCommand(&mut self, ctx: &WhereCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldsCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldsCommand(&mut self, ctx: &FieldsCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_tableCommand(&mut self, ctx: &TableCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldsCommandBody}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldsCommandBody(&mut self, ctx: &FieldsCommandBodyContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#wcFieldList}.
	 * @param ctx the parse tree
	 */
		fn visit_wcFieldList(&mut self, ctx: &WcFieldListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#renameCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_renameCommand(&mut self, ctx: &RenameCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#replaceCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_replaceCommand(&mut self, ctx: &ReplaceCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#replacePair}.
	 * @param ctx the parse tree
	 */
		fn visit_replacePair(&mut self, ctx: &ReplacePairContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_statsCommand(&mut self, ctx: &StatsCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#appendPipeCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_appendPipeCommand(&mut self, ctx: &AppendPipeCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsArgs}.
	 * @param ctx the parse tree
	 */
		fn visit_statsArgs(&mut self, ctx: &StatsArgsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#partitionsArg}.
	 * @param ctx the parse tree
	 */
		fn visit_partitionsArg(&mut self, ctx: &PartitionsArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#allnumArg}.
	 * @param ctx the parse tree
	 */
		fn visit_allnumArg(&mut self, ctx: &AllnumArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#delimArg}.
	 * @param ctx the parse tree
	 */
		fn visit_delimArg(&mut self, ctx: &DelimArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#bucketNullableArg}.
	 * @param ctx the parse tree
	 */
		fn visit_bucketNullableArg(&mut self, ctx: &BucketNullableArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dedupSplitArg}.
	 * @param ctx the parse tree
	 */
		fn visit_dedupSplitArg(&mut self, ctx: &DedupSplitArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#eventstatsCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_eventstatsCommand(&mut self, ctx: &EventstatsCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#streamstatsCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_streamstatsCommand(&mut self, ctx: &StreamstatsCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#streamstatsArgs}.
	 * @param ctx the parse tree
	 */
		fn visit_streamstatsArgs(&mut self, ctx: &StreamstatsArgsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#currentArg}.
	 * @param ctx the parse tree
	 */
		fn visit_currentArg(&mut self, ctx: &CurrentArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#windowArg}.
	 * @param ctx the parse tree
	 */
		fn visit_windowArg(&mut self, ctx: &WindowArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#globalArg}.
	 * @param ctx the parse tree
	 */
		fn visit_globalArg(&mut self, ctx: &GlobalArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#resetBeforeArg}.
	 * @param ctx the parse tree
	 */
		fn visit_resetBeforeArg(&mut self, ctx: &ResetBeforeArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#resetAfterArg}.
	 * @param ctx the parse tree
	 */
		fn visit_resetAfterArg(&mut self, ctx: &ResetAfterArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dedupCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_dedupCommand(&mut self, ctx: &DedupCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sortCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_sortCommand(&mut self, ctx: &SortCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#reverseCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_reverseCommand(&mut self, ctx: &ReverseCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#chartCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_chartCommand(&mut self, ctx: &ChartCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#chartOptions}.
	 * @param ctx the parse tree
	 */
		fn visit_chartOptions(&mut self, ctx: &ChartOptionsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rowSplit}.
	 * @param ctx the parse tree
	 */
		fn visit_rowSplit(&mut self, ctx: &RowSplitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#columnSplit}.
	 * @param ctx the parse tree
	 */
		fn visit_columnSplit(&mut self, ctx: &ColumnSplitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timechartCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_timechartCommand(&mut self, ctx: &TimechartCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#transposeCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_transposeCommand(&mut self, ctx: &TransposeCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#transposeParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_transposeParameter(&mut self, ctx: &TransposeParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timewrapCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_timewrapCommand(&mut self, ctx: &TimewrapCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timewrapParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_timewrapParameter(&mut self, ctx: &TimewrapParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timewrapAlign}.
	 * @param ctx the parse tree
	 */
		fn visit_timewrapAlign(&mut self, ctx: &TimewrapAlignContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timewrapSeries}.
	 * @param ctx the parse tree
	 */
		fn visit_timewrapSeries(&mut self, ctx: &TimewrapSeriesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timechartParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_timechartParameter(&mut self, ctx: &TimechartParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#spanLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_spanLiteral(&mut self, ctx: &SpanLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_evalCommand(&mut self, ctx: &EvalCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachCommand(&mut self, ctx: &ForeachCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachArgument}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachArgument(&mut self, ctx: &ForeachArgumentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachTarget(&mut self, ctx: &ForeachTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachOption}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachOption(&mut self, ctx: &ForeachOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachEvalCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachEvalCommand(&mut self, ctx: &ForeachEvalCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachEvalClause}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachEvalClause(&mut self, ctx: &ForeachEvalClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachEvalTarget}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachEvalTarget(&mut self, ctx: &ForeachEvalTargetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldformatCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldformatCommand(&mut self, ctx: &FieldformatCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#headCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_headCommand(&mut self, ctx: &HeadCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#binCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_binCommand(&mut self, ctx: &BinCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#binOption}.
	 * @param ctx the parse tree
	 */
		fn visit_binOption(&mut self, ctx: &BinOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#aligntimeValue}.
	 * @param ctx the parse tree
	 */
		fn visit_aligntimeValue(&mut self, ctx: &AligntimeValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code numericSpanValue}
	 * labeled alternative in {@link OpenSearchPPLParser#binSpanValue}.
	 * @param ctx the parse tree
	 */
		fn visit_numericSpanValue(&mut self, ctx: &NumericSpanValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logBasedSpanValue}
	 * labeled alternative in {@link OpenSearchPPLParser#binSpanValue}.
	 * @param ctx the parse tree
	 */
		fn visit_logBasedSpanValue(&mut self, ctx: &LogBasedSpanValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logWithBaseSpan}
	 * labeled alternative in {@link OpenSearchPPLParser#logSpanValue}.
	 * @param ctx the parse tree
	 */
		fn visit_logWithBaseSpan(&mut self, ctx: &LogWithBaseSpanContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rareTopCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_rareTopCommand(&mut self, ctx: &RareTopCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rareTopOption}.
	 * @param ctx the parse tree
	 */
		fn visit_rareTopOption(&mut self, ctx: &RareTopOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#grokCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_grokCommand(&mut self, ctx: &GrokCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#parseCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_parseCommand(&mut self, ctx: &ParseCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#spathCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_spathCommand(&mut self, ctx: &SpathCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#spathParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_spathParameter(&mut self, ctx: &SpathParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#indexablePath}.
	 * @param ctx the parse tree
	 */
		fn visit_indexablePath(&mut self, ctx: &IndexablePathContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#pathElement}.
	 * @param ctx the parse tree
	 */
		fn visit_pathElement(&mut self, ctx: &PathElementContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#pathArrayAccess}.
	 * @param ctx the parse tree
	 */
		fn visit_pathArrayAccess(&mut self, ctx: &PathArrayAccessContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#regexCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_regexCommand(&mut self, ctx: &RegexCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#regexExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_regexExpr(&mut self, ctx: &RegexExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rexCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_rexCommand(&mut self, ctx: &RexCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rexExpr}.
	 * @param ctx the parse tree
	 */
		fn visit_rexExpr(&mut self, ctx: &RexExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#rexOption}.
	 * @param ctx the parse tree
	 */
		fn visit_rexOption(&mut self, ctx: &RexOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternsMethod}.
	 * @param ctx the parse tree
	 */
		fn visit_patternsMethod(&mut self, ctx: &PatternsMethodContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternsCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_patternsCommand(&mut self, ctx: &PatternsCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternsCommandOption}.
	 * @param ctx the parse tree
	 */
		fn visit_patternsCommandOption(&mut self, ctx: &PatternsCommandOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternsParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_patternsParameter(&mut self, ctx: &PatternsParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternMethod}.
	 * @param ctx the parse tree
	 */
		fn visit_patternMethod(&mut self, ctx: &PatternMethodContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#patternMode}.
	 * @param ctx the parse tree
	 */
		fn visit_patternMode(&mut self, ctx: &PatternModeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#lookupCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_lookupCommand(&mut self, ctx: &LookupCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#lookupMappingList}.
	 * @param ctx the parse tree
	 */
		fn visit_lookupMappingList(&mut self, ctx: &LookupMappingListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#outputCandidateList}.
	 * @param ctx the parse tree
	 */
		fn visit_outputCandidateList(&mut self, ctx: &OutputCandidateListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#lookupPair}.
	 * @param ctx the parse tree
	 */
		fn visit_lookupPair(&mut self, ctx: &LookupPairContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code fillNullWithClause}
	 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_fillNullWithClause(&mut self, ctx: &FillNullWithClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code fillNullUsingClause}
	 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_fillNullUsingClause(&mut self, ctx: &FillNullUsingClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code fillNullValueWithFields}
	 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_fillNullValueWithFields(&mut self, ctx: &FillNullValueWithFieldsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code fillNullValueAllFields}
	 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_fillNullValueAllFields(&mut self, ctx: &FillNullValueAllFieldsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fillNullWith}.
	 * @param ctx the parse tree
	 */
		fn visit_fillNullWith(&mut self, ctx: &FillNullWithContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fillNullUsing}.
	 * @param ctx the parse tree
	 */
		fn visit_fillNullUsing(&mut self, ctx: &FillNullUsingContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#replacementPair}.
	 * @param ctx the parse tree
	 */
		fn visit_replacementPair(&mut self, ctx: &ReplacementPairContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#convertCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_convertCommand(&mut self, ctx: &ConvertCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#convertFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_convertFunction(&mut self, ctx: &ConvertFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#trendlineCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_trendlineCommand(&mut self, ctx: &TrendlineCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#trendlineClause}.
	 * @param ctx the parse tree
	 */
		fn visit_trendlineClause(&mut self, ctx: &TrendlineClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#trendlineType}.
	 * @param ctx the parse tree
	 */
		fn visit_trendlineType(&mut self, ctx: &TrendlineTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#expandCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_expandCommand(&mut self, ctx: &ExpandCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mvcombineCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_mvcombineCommand(&mut self, ctx: &MvcombineCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#nomvCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_nomvCommand(&mut self, ctx: &NomvCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mvexpandCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_mvexpandCommand(&mut self, ctx: &MvexpandCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#flattenCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_flattenCommand(&mut self, ctx: &FlattenCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#appendcolCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_appendcolCommand(&mut self, ctx: &AppendcolCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#appendCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_appendCommand(&mut self, ctx: &AppendCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#multisearchCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_multisearchCommand(&mut self, ctx: &MultisearchCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#unionCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_unionCommand(&mut self, ctx: &UnionCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#subsearchOptions}.
	 * @param ctx the parse tree
	 */
		fn visit_subsearchOptions(&mut self, ctx: &SubsearchOptionsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#unionDataset}.
	 * @param ctx the parse tree
	 */
		fn visit_unionDataset(&mut self, ctx: &UnionDatasetContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#kmeansCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_kmeansCommand(&mut self, ctx: &KmeansCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#kmeansParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_kmeansParameter(&mut self, ctx: &KmeansParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#adCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_adCommand(&mut self, ctx: &AdCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#adParameter}.
	 * @param ctx the parse tree
	 */
		fn visit_adParameter(&mut self, ctx: &AdParameterContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mlCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_mlCommand(&mut self, ctx: &MlCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mlArg}.
	 * @param ctx the parse tree
	 */
		fn visit_mlArg(&mut self, ctx: &MlArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#addtotalsCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_addtotalsCommand(&mut self, ctx: &AddtotalsCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#addtotalsOption}.
	 * @param ctx the parse tree
	 */
		fn visit_addtotalsOption(&mut self, ctx: &AddtotalsOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#addcoltotalsCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_addcoltotalsCommand(&mut self, ctx: &AddcoltotalsCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#addcoltotalsOption}.
	 * @param ctx the parse tree
	 */
		fn visit_addcoltotalsOption(&mut self, ctx: &AddcoltotalsOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#graphLookupCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_graphLookupCommand(&mut self, ctx: &GraphLookupCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#startClause}.
	 * @param ctx the parse tree
	 */
		fn visit_startClause(&mut self, ctx: &StartClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#edgeClause}.
	 * @param ctx the parse tree
	 */
		fn visit_edgeClause(&mut self, ctx: &EdgeClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#graphLookupArgs}.
	 * @param ctx the parse tree
	 */
		fn visit_graphLookupArgs(&mut self, ctx: &GraphLookupArgsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#xyseriesCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_xyseriesCommand(&mut self, ctx: &XyseriesCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#xyseriesOption}.
	 * @param ctx the parse tree
	 */
		fn visit_xyseriesOption(&mut self, ctx: &XyseriesOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#xyseriesPivotValues}.
	 * @param ctx the parse tree
	 */
		fn visit_xyseriesPivotValues(&mut self, ctx: &XyseriesPivotValuesContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fromClause}.
	 * @param ctx the parse tree
	 */
		fn visit_fromClause(&mut self, ctx: &FromClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableOrSubqueryClause}.
	 * @param ctx the parse tree
	 */
		fn visit_tableOrSubqueryClause(&mut self, ctx: &TableOrSubqueryClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableSourceClause}.
	 * @param ctx the parse tree
	 */
		fn visit_tableSourceClause(&mut self, ctx: &TableSourceClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dynamicSourceClause}.
	 * @param ctx the parse tree
	 */
		fn visit_dynamicSourceClause(&mut self, ctx: &DynamicSourceClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sourceReference}.
	 * @param ctx the parse tree
	 */
		fn visit_sourceReference(&mut self, ctx: &SourceReferenceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sourceFilterArg}.
	 * @param ctx the parse tree
	 */
		fn visit_sourceFilterArg(&mut self, ctx: &SourceFilterArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#joinCommand}.
	 * @param ctx the parse tree
	 */
		fn visit_joinCommand(&mut self, ctx: &JoinCommandContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sqlLikeJoinType}.
	 * @param ctx the parse tree
	 */
		fn visit_sqlLikeJoinType(&mut self, ctx: &SqlLikeJoinTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#joinType}.
	 * @param ctx the parse tree
	 */
		fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sideAlias}.
	 * @param ctx the parse tree
	 */
		fn visit_sideAlias(&mut self, ctx: &SideAliasContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#joinCriteria}.
	 * @param ctx the parse tree
	 */
		fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#joinHintList}.
	 * @param ctx the parse tree
	 */
		fn visit_joinHintList(&mut self, ctx: &JoinHintListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code leftHint}
	 * labeled alternative in {@link OpenSearchPPLParser#hintPair}.
	 * @param ctx the parse tree
	 */
		fn visit_leftHint(&mut self, ctx: &LeftHintContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rightHint}
	 * labeled alternative in {@link OpenSearchPPLParser#hintPair}.
	 * @param ctx the parse tree
	 */
		fn visit_rightHint(&mut self, ctx: &RightHintContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code overwriteOption}
	 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
	 * @param ctx the parse tree
	 */
		fn visit_overwriteOption(&mut self, ctx: &OverwriteOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code typeOption}
	 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
	 * @param ctx the parse tree
	 */
		fn visit_typeOption(&mut self, ctx: &TypeOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code maxOption}
	 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
	 * @param ctx the parse tree
	 */
		fn visit_maxOption(&mut self, ctx: &MaxOptionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#renameClause}.
	 * @param ctx the parse tree
	 */
		fn visit_renameClause(&mut self, ctx: &RenameClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#byClause}.
	 * @param ctx the parse tree
	 */
		fn visit_byClause(&mut self, ctx: &ByClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsByClause}.
	 * @param ctx the parse tree
	 */
		fn visit_statsByClause(&mut self, ctx: &StatsByClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#bySpanClause}.
	 * @param ctx the parse tree
	 */
		fn visit_bySpanClause(&mut self, ctx: &BySpanClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#spanClause}.
	 * @param ctx the parse tree
	 */
		fn visit_spanClause(&mut self, ctx: &SpanClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sortbyClause}.
	 * @param ctx the parse tree
	 */
		fn visit_sortbyClause(&mut self, ctx: &SortbyClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalClause}.
	 * @param ctx the parse tree
	 */
		fn visit_evalClause(&mut self, ctx: &EvalClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldFormatEvalClause}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldFormatEvalClause(&mut self, ctx: &FieldFormatEvalClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#eventstatsAggTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_eventstatsAggTerm(&mut self, ctx: &EventstatsAggTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#streamstatsAggTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_streamstatsAggTerm(&mut self, ctx: &StreamstatsAggTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#windowFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_windowFunction(&mut self, ctx: &WindowFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#windowFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_windowFunctionName(&mut self, ctx: &WindowFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#scalarWindowFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_scalarWindowFunctionName(&mut self, ctx: &ScalarWindowFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsAggTerm}.
	 * @param ctx the parse tree
	 */
		fn visit_statsAggTerm(&mut self, ctx: &StatsAggTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code countEvalFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_countEvalFunctionCall(&mut self, ctx: &CountEvalFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code countAllFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_countAllFunctionCall(&mut self, ctx: &CountAllFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code percentileShortcutFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_percentileShortcutFunctionCall(&mut self, ctx: &PercentileShortcutFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code distinctCountFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_distinctCountFunctionCall(&mut self, ctx: &DistinctCountFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code takeAggFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_takeAggFunctionCall(&mut self, ctx: &TakeAggFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code valuesAggFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_valuesAggFunctionCall(&mut self, ctx: &ValuesAggFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code percentileApproxFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_percentileApproxFunctionCall(&mut self, ctx: &PercentileApproxFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code perFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_perFunctionCall(&mut self, ctx: &PerFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code statsFunctionCall}
	 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_statsFunctionCall(&mut self, ctx: &StatsFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#statsFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_statsFunctionName(&mut self, ctx: &StatsFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#takeAggFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_takeAggFunction(&mut self, ctx: &TakeAggFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#valuesAggFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_valuesAggFunction(&mut self, ctx: &ValuesAggFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#percentileApproxFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_percentileApproxFunction(&mut self, ctx: &PercentileApproxFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#perFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_perFunction(&mut self, ctx: &PerFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#numericLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code stringDotlogicalExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_stringDotlogicalExpression(&mut self, ctx: &StringDotlogicalExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code stringDotlogicalExpressionDotString}
	 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_stringDotlogicalExpressionDotString(&mut self, ctx: &StringDotlogicalExpressionDotStringContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalExpressionDotString}
	 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalExpressionDotString(&mut self, ctx: &LogicalExpressionDotStringContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code ffStandardLogicalExpression}
	 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_ffStandardLogicalExpression(&mut self, ctx: &FfStandardLogicalExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalNot}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalExpr(&mut self, ctx: &LogicalExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalAnd}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalAnd(&mut self, ctx: &LogicalAndContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalXor}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalXor(&mut self, ctx: &LogicalXorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code logicalOr}
	 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_logicalOr(&mut self, ctx: &LogicalOrContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code relevanceExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_relevanceExpr(&mut self, ctx: &RelevanceExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code valueExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_valueExpr(&mut self, ctx: &ValueExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_inExpr(&mut self, ctx: &InExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code isNullPredicate}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_isNullPredicate(&mut self, ctx: &IsNullPredicateContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code between}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_between(&mut self, ctx: &BetweenContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code compareExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#expression}.
	 * @param ctx the parse tree
	 */
		fn visit_compareExpr(&mut self, ctx: &CompareExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#nullNotnull}.
	 * @param ctx the parse tree
	 */
		fn visit_nullNotnull(&mut self, ctx: &NullNotnullContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code inSubqueryExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_inSubqueryExpr(&mut self, ctx: &InSubqueryExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code lambdaExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_lambdaExpr(&mut self, ctx: &LambdaExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code literalValueExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_literalValueExpr(&mut self, ctx: &LiteralValueExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code functionCallExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCallExpr(&mut self, ctx: &FunctionCallExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code existsSubqueryExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_existsSubqueryExpr(&mut self, ctx: &ExistsSubqueryExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code foreachPlaceholderExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachPlaceholderExpr(&mut self, ctx: &ForeachPlaceholderExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code scalarSubqueryExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_scalarSubqueryExpr(&mut self, ctx: &ScalarSubqueryExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code nestedValueExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_nestedValueExpr(&mut self, ctx: &NestedValueExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code binaryArithmetic}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_binaryArithmetic(&mut self, ctx: &BinaryArithmeticContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code fieldExpr}
	 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldExpr(&mut self, ctx: &FieldExprContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_evalExpression(&mut self, ctx: &EvalExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#functionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mvmapFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_mvmapFunctionCall(&mut self, ctx: &MvmapFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#positionFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_positionFunctionCall(&mut self, ctx: &PositionFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#caseFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_caseFunctionCall(&mut self, ctx: &CaseFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_relevanceExpression(&mut self, ctx: &RelevanceExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#singleFieldRelevanceFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_singleFieldRelevanceFunction(&mut self, ctx: &SingleFieldRelevanceFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#multiFieldRelevanceFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_multiFieldRelevanceFunction(&mut self, ctx: &MultiFieldRelevanceFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timeModifier}.
	 * @param ctx the parse tree
	 */
		fn visit_timeModifier(&mut self, ctx: &TimeModifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timeModifierValue}.
	 * @param ctx the parse tree
	 */
		fn visit_timeModifierValue(&mut self, ctx: &TimeModifierValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableSource}.
	 * @param ctx the parse tree
	 */
		fn visit_tableSource(&mut self, ctx: &TableSourceContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableFunction}.
	 * @param ctx the parse tree
	 */
		fn visit_tableFunction(&mut self, ctx: &TableFunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldList}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldList(&mut self, ctx: &FieldListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code invalidMixedSortField}
	 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
	 * @param ctx the parse tree
	 */
		fn visit_invalidMixedSortField(&mut self, ctx: &InvalidMixedSortFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prefixSortField}
	 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
	 * @param ctx the parse tree
	 */
		fn visit_prefixSortField(&mut self, ctx: &PrefixSortFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code suffixSortField}
	 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
	 * @param ctx the parse tree
	 */
		fn visit_suffixSortField(&mut self, ctx: &SuffixSortFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code defaultSortField}
	 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
	 * @param ctx the parse tree
	 */
		fn visit_defaultSortField(&mut self, ctx: &DefaultSortFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#sortFieldExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_sortFieldExpression(&mut self, ctx: &SortFieldExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#fieldExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldExpression(&mut self, ctx: &FieldExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#wcFieldExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_wcFieldExpression(&mut self, ctx: &WcFieldExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachPlaceholder}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachPlaceholder(&mut self, ctx: &ForeachPlaceholderContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#foreachPlaceholderName}.
	 * @param ctx the parse tree
	 */
		fn visit_foreachPlaceholderName(&mut self, ctx: &ForeachPlaceholderNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#selectFieldExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_selectFieldExpression(&mut self, ctx: &SelectFieldExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#renameFieldExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_renameFieldExpression(&mut self, ctx: &RenameFieldExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_evalFunctionCall(&mut self, ctx: &EvalFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dataTypeFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_dataTypeFunctionCall(&mut self, ctx: &DataTypeFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#convertedDataType}.
	 * @param ctx the parse tree
	 */
		fn visit_convertedDataType(&mut self, ctx: &ConvertedDataTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#evalFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_evalFunctionName(&mut self, ctx: &EvalFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#functionArgs}.
	 * @param ctx the parse tree
	 */
		fn visit_functionArgs(&mut self, ctx: &FunctionArgsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#namedFunctionArgs}.
	 * @param ctx the parse tree
	 */
		fn visit_namedFunctionArgs(&mut self, ctx: &NamedFunctionArgsContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#functionArg}.
	 * @param ctx the parse tree
	 */
		fn visit_functionArg(&mut self, ctx: &FunctionArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#namedFunctionArg}.
	 * @param ctx the parse tree
	 */
		fn visit_namedFunctionArg(&mut self, ctx: &NamedFunctionArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#functionArgExpression}.
	 * @param ctx the parse tree
	 */
		fn visit_functionArgExpression(&mut self, ctx: &FunctionArgExpressionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#lambda}.
	 * @param ctx the parse tree
	 */
		fn visit_lambda(&mut self, ctx: &LambdaContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceArg}.
	 * @param ctx the parse tree
	 */
		fn visit_relevanceArg(&mut self, ctx: &RelevanceArgContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceArgName}.
	 * @param ctx the parse tree
	 */
		fn visit_relevanceArgName(&mut self, ctx: &RelevanceArgNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceFieldAndWeight}.
	 * @param ctx the parse tree
	 */
		fn visit_relevanceFieldAndWeight(&mut self, ctx: &RelevanceFieldAndWeightContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceFieldWeight}.
	 * @param ctx the parse tree
	 */
		fn visit_relevanceFieldWeight(&mut self, ctx: &RelevanceFieldWeightContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceField}.
	 * @param ctx the parse tree
	 */
		fn visit_relevanceField(&mut self, ctx: &RelevanceFieldContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceQuery}.
	 * @param ctx the parse tree
	 */
		fn visit_relevanceQuery(&mut self, ctx: &RelevanceQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#relevanceArgValue}.
	 * @param ctx the parse tree
	 */
		fn visit_relevanceArgValue(&mut self, ctx: &RelevanceArgValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#mathematicalFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_mathematicalFunctionName(&mut self, ctx: &MathematicalFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#geoipFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_geoipFunctionName(&mut self, ctx: &GeoipFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#collectionFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_collectionFunctionName(&mut self, ctx: &CollectionFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#trigonometricFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_trigonometricFunctionName(&mut self, ctx: &TrigonometricFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#jsonFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_jsonFunctionName(&mut self, ctx: &JsonFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#cryptographicFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_cryptographicFunctionName(&mut self, ctx: &CryptographicFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dateTimeFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_dateTimeFunctionName(&mut self, ctx: &DateTimeFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#getFormatFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_getFormatFunctionCall(&mut self, ctx: &GetFormatFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#getFormatType}.
	 * @param ctx the parse tree
	 */
		fn visit_getFormatType(&mut self, ctx: &GetFormatTypeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#extractFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_extractFunctionCall(&mut self, ctx: &ExtractFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#simpleDateTimePart}.
	 * @param ctx the parse tree
	 */
		fn visit_simpleDateTimePart(&mut self, ctx: &SimpleDateTimePartContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#complexDateTimePart}.
	 * @param ctx the parse tree
	 */
		fn visit_complexDateTimePart(&mut self, ctx: &ComplexDateTimePartContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#datetimePart}.
	 * @param ctx the parse tree
	 */
		fn visit_datetimePart(&mut self, ctx: &DatetimePartContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timestampFunctionCall}.
	 * @param ctx the parse tree
	 */
		fn visit_timestampFunctionCall(&mut self, ctx: &TimestampFunctionCallContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timestampFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_timestampFunctionName(&mut self, ctx: &TimestampFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#conditionFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_conditionFunctionName(&mut self, ctx: &ConditionFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#flowControlFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_flowControlFunctionName(&mut self, ctx: &FlowControlFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#systemFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_systemFunctionName(&mut self, ctx: &SystemFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#textFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_textFunctionName(&mut self, ctx: &TextFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#positionFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_positionFunctionName(&mut self, ctx: &PositionFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#comparisonOperator}.
	 * @param ctx the parse tree
	 */
		fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#singleFieldRelevanceFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_singleFieldRelevanceFunctionName(&mut self, ctx: &SingleFieldRelevanceFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#multiFieldRelevanceFunctionName}.
	 * @param ctx the parse tree
	 */
		fn visit_multiFieldRelevanceFunctionName(&mut self, ctx: &MultiFieldRelevanceFunctionNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#literalValue}.
	 * @param ctx the parse tree
	 */
		fn visit_literalValue(&mut self, ctx: &LiteralValueContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#intervalLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#stringLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#integerLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#decimalLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#doubleLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#floatLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_floatLiteral(&mut self, ctx: &FloatLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#booleanLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#datetimeLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_datetimeLiteral(&mut self, ctx: &DatetimeLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#dateLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_dateLiteral(&mut self, ctx: &DateLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timeLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_timeLiteral(&mut self, ctx: &TimeLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#timestampLiteral}.
	 * @param ctx the parse tree
	 */
		fn visit_timestampLiteral(&mut self, ctx: &TimestampLiteralContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#intervalUnit}.
	 * @param ctx the parse tree
	 */
		fn visit_intervalUnit(&mut self, ctx: &IntervalUnitContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#valueList}.
	 * @param ctx the parse tree
	 */
		fn visit_valueList(&mut self, ctx: &ValueListContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identsAsQualifiedName}
	 * labeled alternative in {@link OpenSearchPPLParser#qualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_identsAsQualifiedName(&mut self, ctx: &IdentsAsQualifiedNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identsAsTableQualifiedName}
	 * labeled alternative in {@link OpenSearchPPLParser#tableQualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_identsAsTableQualifiedName(&mut self, ctx: &IdentsAsTableQualifiedNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identsAsWildcardQualifiedName}
	 * labeled alternative in {@link OpenSearchPPLParser#wcQualifiedName}.
	 * @param ctx the parse tree
	 */
		fn visit_identsAsWildcardQualifiedName(&mut self, ctx: &IdentsAsWildcardQualifiedNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code identsAsQualifiedNameSeq}
	 * labeled alternative in {@link OpenSearchPPLParser#identifierSeq}.
	 * @param ctx the parse tree
	 */
		fn visit_identsAsQualifiedNameSeq(&mut self, ctx: &IdentsAsQualifiedNameSeqContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#ident}.
	 * @param ctx the parse tree
	 */
		fn visit_ident(&mut self, ctx: &IdentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#tableIdent}.
	 * @param ctx the parse tree
	 */
		fn visit_tableIdent(&mut self, ctx: &TableIdentContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#wildcard}.
	 * @param ctx the parse tree
	 */
		fn visit_wildcard(&mut self, ctx: &WildcardContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#keywordsCanBeId}.
	 * @param ctx the parse tree
	 */
		fn visit_keywordsCanBeId(&mut self, ctx: &KeywordsCanBeIdContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link OpenSearchPPLParser#searchableKeyWord}.
	 * @param ctx the parse tree
	 */
		fn visit_searchableKeyWord(&mut self, ctx: &SearchableKeyWordContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

}

impl<'input,T> OpenSearchPPLParserVisitor<'input> for T
where
	T: OpenSearchPPLParserVisitorCompat<'input>
{
	fn visit_root(&mut self, ctx: &RootContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_root(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pplStatement(&mut self, ctx: &PplStatementContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_pplStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subPipeline(&mut self, ctx: &SubPipelineContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_subPipeline(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_queryStatement(&mut self, ctx: &QueryStatementContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_queryStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_explainStatement(&mut self, ctx: &ExplainStatementContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_explainStatement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_explainMode(&mut self, ctx: &ExplainModeContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_explainMode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subSearch(&mut self, ctx: &SubSearchContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_subSearch(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pplCommands(&mut self, ctx: &PplCommandsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_pplCommands(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_commands(&mut self, ctx: &CommandsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_commands(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_commandName(&mut self, ctx: &CommandNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_commandName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchFrom(&mut self, ctx: &SearchFromContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_searchFrom(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_orExpression(&mut self, ctx: &OrExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_orExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timeModifierExpression(&mut self, ctx: &TimeModifierExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timeModifierExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_andExpression(&mut self, ctx: &AndExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_andExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_notExpression(&mut self, ctx: &NotExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_notExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_termExpression(&mut self, ctx: &TermExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_termExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_groupedExpression(&mut self, ctx: &GroupedExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_groupedExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchComparisonTerm(&mut self, ctx: &SearchComparisonTermContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_searchComparisonTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchInListTerm(&mut self, ctx: &SearchInListTermContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_searchInListTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchLiteralTerm(&mut self, ctx: &SearchLiteralTermContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_searchLiteralTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchLiteral(&mut self, ctx: &SearchLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_searchLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchFieldCompare(&mut self, ctx: &SearchFieldCompareContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_searchFieldCompare(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchFieldInValues(&mut self, ctx: &SearchFieldInValuesContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_searchFieldInValues(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchLiterals(&mut self, ctx: &SearchLiteralsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_searchLiterals(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_equals(&mut self, ctx: &EqualsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_equals(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_notEquals(&mut self, ctx: &NotEqualsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_notEquals(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lessThan(&mut self, ctx: &LessThanContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_lessThan(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lessOrEqual(&mut self, ctx: &LessOrEqualContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_lessOrEqual(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_greaterThan(&mut self, ctx: &GreaterThanContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_greaterThan(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_greaterOrEqual(&mut self, ctx: &GreaterOrEqualContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_greaterOrEqual(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_describeCommand(&mut self, ctx: &DescribeCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_describeCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_restCommand(&mut self, ctx: &RestCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_restCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_restArgument(&mut self, ctx: &RestArgumentContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_restArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_showDataSourcesCommand(&mut self, ctx: &ShowDataSourcesCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_showDataSourcesCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_makeresultsCommand(&mut self, ctx: &MakeresultsCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_makeresultsCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_makeresultsArg(&mut self, ctx: &MakeresultsArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_makeresultsArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_whereCommand(&mut self, ctx: &WhereCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_whereCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldsCommand(&mut self, ctx: &FieldsCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fieldsCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableCommand(&mut self, ctx: &TableCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_tableCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldsCommandBody(&mut self, ctx: &FieldsCommandBodyContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fieldsCommandBody(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_wcFieldList(&mut self, ctx: &WcFieldListContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_wcFieldList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameCommand(&mut self, ctx: &RenameCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_renameCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_replaceCommand(&mut self, ctx: &ReplaceCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_replaceCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_replacePair(&mut self, ctx: &ReplacePairContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_replacePair(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statsCommand(&mut self, ctx: &StatsCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_statsCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_appendPipeCommand(&mut self, ctx: &AppendPipeCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_appendPipeCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statsArgs(&mut self, ctx: &StatsArgsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_statsArgs(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_partitionsArg(&mut self, ctx: &PartitionsArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_partitionsArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_allnumArg(&mut self, ctx: &AllnumArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_allnumArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_delimArg(&mut self, ctx: &DelimArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_delimArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bucketNullableArg(&mut self, ctx: &BucketNullableArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_bucketNullableArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dedupSplitArg(&mut self, ctx: &DedupSplitArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_dedupSplitArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_eventstatsCommand(&mut self, ctx: &EventstatsCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_eventstatsCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_streamstatsCommand(&mut self, ctx: &StreamstatsCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_streamstatsCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_streamstatsArgs(&mut self, ctx: &StreamstatsArgsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_streamstatsArgs(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_currentArg(&mut self, ctx: &CurrentArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_currentArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowArg(&mut self, ctx: &WindowArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_windowArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_globalArg(&mut self, ctx: &GlobalArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_globalArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_resetBeforeArg(&mut self, ctx: &ResetBeforeArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_resetBeforeArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_resetAfterArg(&mut self, ctx: &ResetAfterArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_resetAfterArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dedupCommand(&mut self, ctx: &DedupCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_dedupCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sortCommand(&mut self, ctx: &SortCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_sortCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_reverseCommand(&mut self, ctx: &ReverseCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_reverseCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_chartCommand(&mut self, ctx: &ChartCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_chartCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_chartOptions(&mut self, ctx: &ChartOptionsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_chartOptions(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rowSplit(&mut self, ctx: &RowSplitContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_rowSplit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_columnSplit(&mut self, ctx: &ColumnSplitContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_columnSplit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timechartCommand(&mut self, ctx: &TimechartCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timechartCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_transposeCommand(&mut self, ctx: &TransposeCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_transposeCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_transposeParameter(&mut self, ctx: &TransposeParameterContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_transposeParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timewrapCommand(&mut self, ctx: &TimewrapCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timewrapCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timewrapParameter(&mut self, ctx: &TimewrapParameterContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timewrapParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timewrapAlign(&mut self, ctx: &TimewrapAlignContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timewrapAlign(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timewrapSeries(&mut self, ctx: &TimewrapSeriesContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timewrapSeries(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timechartParameter(&mut self, ctx: &TimechartParameterContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timechartParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_spanLiteral(&mut self, ctx: &SpanLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_spanLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_evalCommand(&mut self, ctx: &EvalCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_evalCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachCommand(&mut self, ctx: &ForeachCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachArgument(&mut self, ctx: &ForeachArgumentContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachArgument(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachTarget(&mut self, ctx: &ForeachTargetContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachOption(&mut self, ctx: &ForeachOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachEvalCommand(&mut self, ctx: &ForeachEvalCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachEvalCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachEvalClause(&mut self, ctx: &ForeachEvalClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachEvalClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachEvalTarget(&mut self, ctx: &ForeachEvalTargetContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachEvalTarget(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldformatCommand(&mut self, ctx: &FieldformatCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fieldformatCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_headCommand(&mut self, ctx: &HeadCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_headCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binCommand(&mut self, ctx: &BinCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_binCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binOption(&mut self, ctx: &BinOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_binOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_aligntimeValue(&mut self, ctx: &AligntimeValueContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_aligntimeValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_numericSpanValue(&mut self, ctx: &NumericSpanValueContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_numericSpanValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logBasedSpanValue(&mut self, ctx: &LogBasedSpanValueContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_logBasedSpanValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logWithBaseSpan(&mut self, ctx: &LogWithBaseSpanContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_logWithBaseSpan(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rareTopCommand(&mut self, ctx: &RareTopCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_rareTopCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rareTopOption(&mut self, ctx: &RareTopOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_rareTopOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_grokCommand(&mut self, ctx: &GrokCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_grokCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_parseCommand(&mut self, ctx: &ParseCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_parseCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_spathCommand(&mut self, ctx: &SpathCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_spathCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_spathParameter(&mut self, ctx: &SpathParameterContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_spathParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_indexablePath(&mut self, ctx: &IndexablePathContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_indexablePath(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathElement(&mut self, ctx: &PathElementContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_pathElement(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_pathArrayAccess(&mut self, ctx: &PathArrayAccessContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_pathArrayAccess(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_regexCommand(&mut self, ctx: &RegexCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_regexCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_regexExpr(&mut self, ctx: &RegexExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_regexExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rexCommand(&mut self, ctx: &RexCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_rexCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rexExpr(&mut self, ctx: &RexExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_rexExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rexOption(&mut self, ctx: &RexOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_rexOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternsMethod(&mut self, ctx: &PatternsMethodContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_patternsMethod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternsCommand(&mut self, ctx: &PatternsCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_patternsCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternsCommandOption(&mut self, ctx: &PatternsCommandOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_patternsCommandOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternsParameter(&mut self, ctx: &PatternsParameterContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_patternsParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternMethod(&mut self, ctx: &PatternMethodContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_patternMethod(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_patternMode(&mut self, ctx: &PatternModeContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_patternMode(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lookupCommand(&mut self, ctx: &LookupCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_lookupCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lookupMappingList(&mut self, ctx: &LookupMappingListContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_lookupMappingList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_outputCandidateList(&mut self, ctx: &OutputCandidateListContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_outputCandidateList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lookupPair(&mut self, ctx: &LookupPairContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_lookupPair(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fillNullWithClause(&mut self, ctx: &FillNullWithClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fillNullWithClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fillNullUsingClause(&mut self, ctx: &FillNullUsingClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fillNullUsingClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fillNullValueWithFields(&mut self, ctx: &FillNullValueWithFieldsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fillNullValueWithFields(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fillNullValueAllFields(&mut self, ctx: &FillNullValueAllFieldsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fillNullValueAllFields(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fillNullWith(&mut self, ctx: &FillNullWithContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fillNullWith(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fillNullUsing(&mut self, ctx: &FillNullUsingContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fillNullUsing(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_replacementPair(&mut self, ctx: &ReplacementPairContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_replacementPair(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_convertCommand(&mut self, ctx: &ConvertCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_convertCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_convertFunction(&mut self, ctx: &ConvertFunctionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_convertFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trendlineCommand(&mut self, ctx: &TrendlineCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_trendlineCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trendlineClause(&mut self, ctx: &TrendlineClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_trendlineClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trendlineType(&mut self, ctx: &TrendlineTypeContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_trendlineType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_expandCommand(&mut self, ctx: &ExpandCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_expandCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mvcombineCommand(&mut self, ctx: &MvcombineCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_mvcombineCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nomvCommand(&mut self, ctx: &NomvCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_nomvCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mvexpandCommand(&mut self, ctx: &MvexpandCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_mvexpandCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_flattenCommand(&mut self, ctx: &FlattenCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_flattenCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_appendcolCommand(&mut self, ctx: &AppendcolCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_appendcolCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_appendCommand(&mut self, ctx: &AppendCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_appendCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multisearchCommand(&mut self, ctx: &MultisearchCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_multisearchCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unionCommand(&mut self, ctx: &UnionCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_unionCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_subsearchOptions(&mut self, ctx: &SubsearchOptionsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_subsearchOptions(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_unionDataset(&mut self, ctx: &UnionDatasetContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_unionDataset(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_kmeansCommand(&mut self, ctx: &KmeansCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_kmeansCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_kmeansParameter(&mut self, ctx: &KmeansParameterContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_kmeansParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_adCommand(&mut self, ctx: &AdCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_adCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_adParameter(&mut self, ctx: &AdParameterContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_adParameter(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mlCommand(&mut self, ctx: &MlCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_mlCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mlArg(&mut self, ctx: &MlArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_mlArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_addtotalsCommand(&mut self, ctx: &AddtotalsCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_addtotalsCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_addtotalsOption(&mut self, ctx: &AddtotalsOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_addtotalsOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_addcoltotalsCommand(&mut self, ctx: &AddcoltotalsCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_addcoltotalsCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_addcoltotalsOption(&mut self, ctx: &AddcoltotalsOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_addcoltotalsOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_graphLookupCommand(&mut self, ctx: &GraphLookupCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_graphLookupCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_startClause(&mut self, ctx: &StartClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_startClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_edgeClause(&mut self, ctx: &EdgeClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_edgeClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_graphLookupArgs(&mut self, ctx: &GraphLookupArgsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_graphLookupArgs(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_xyseriesCommand(&mut self, ctx: &XyseriesCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_xyseriesCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_xyseriesOption(&mut self, ctx: &XyseriesOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_xyseriesOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_xyseriesPivotValues(&mut self, ctx: &XyseriesPivotValuesContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_xyseriesPivotValues(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fromClause(&mut self, ctx: &FromClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fromClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableOrSubqueryClause(&mut self, ctx: &TableOrSubqueryClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_tableOrSubqueryClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableSourceClause(&mut self, ctx: &TableSourceClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_tableSourceClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dynamicSourceClause(&mut self, ctx: &DynamicSourceClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_dynamicSourceClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sourceReference(&mut self, ctx: &SourceReferenceContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_sourceReference(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sourceFilterArg(&mut self, ctx: &SourceFilterArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_sourceFilterArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinCommand(&mut self, ctx: &JoinCommandContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_joinCommand(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sqlLikeJoinType(&mut self, ctx: &SqlLikeJoinTypeContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_sqlLikeJoinType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinType(&mut self, ctx: &JoinTypeContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_joinType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sideAlias(&mut self, ctx: &SideAliasContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_sideAlias(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinCriteria(&mut self, ctx: &JoinCriteriaContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_joinCriteria(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_joinHintList(&mut self, ctx: &JoinHintListContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_joinHintList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_leftHint(&mut self, ctx: &LeftHintContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_leftHint(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rightHint(&mut self, ctx: &RightHintContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_rightHint(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_overwriteOption(&mut self, ctx: &OverwriteOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_overwriteOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_typeOption(&mut self, ctx: &TypeOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_typeOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_maxOption(&mut self, ctx: &MaxOptionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_maxOption(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameClause(&mut self, ctx: &RenameClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_renameClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_byClause(&mut self, ctx: &ByClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_byClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statsByClause(&mut self, ctx: &StatsByClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_statsByClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bySpanClause(&mut self, ctx: &BySpanClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_bySpanClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_spanClause(&mut self, ctx: &SpanClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_spanClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sortbyClause(&mut self, ctx: &SortbyClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_sortbyClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_evalClause(&mut self, ctx: &EvalClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_evalClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldFormatEvalClause(&mut self, ctx: &FieldFormatEvalClauseContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fieldFormatEvalClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_eventstatsAggTerm(&mut self, ctx: &EventstatsAggTermContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_eventstatsAggTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_streamstatsAggTerm(&mut self, ctx: &StreamstatsAggTermContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_streamstatsAggTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowFunction(&mut self, ctx: &WindowFunctionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_windowFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_windowFunctionName(&mut self, ctx: &WindowFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_windowFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_scalarWindowFunctionName(&mut self, ctx: &ScalarWindowFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_scalarWindowFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statsAggTerm(&mut self, ctx: &StatsAggTermContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_statsAggTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_countEvalFunctionCall(&mut self, ctx: &CountEvalFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_countEvalFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_countAllFunctionCall(&mut self, ctx: &CountAllFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_countAllFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_percentileShortcutFunctionCall(&mut self, ctx: &PercentileShortcutFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_percentileShortcutFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_distinctCountFunctionCall(&mut self, ctx: &DistinctCountFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_distinctCountFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_takeAggFunctionCall(&mut self, ctx: &TakeAggFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_takeAggFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valuesAggFunctionCall(&mut self, ctx: &ValuesAggFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_valuesAggFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_percentileApproxFunctionCall(&mut self, ctx: &PercentileApproxFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_percentileApproxFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_perFunctionCall(&mut self, ctx: &PerFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_perFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statsFunctionCall(&mut self, ctx: &StatsFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_statsFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_statsFunctionName(&mut self, ctx: &StatsFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_statsFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_takeAggFunction(&mut self, ctx: &TakeAggFunctionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_takeAggFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valuesAggFunction(&mut self, ctx: &ValuesAggFunctionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_valuesAggFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_percentileApproxFunction(&mut self, ctx: &PercentileApproxFunctionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_percentileApproxFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_perFunction(&mut self, ctx: &PerFunctionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_perFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_numericLiteral(&mut self, ctx: &NumericLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_numericLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stringDotlogicalExpression(&mut self, ctx: &StringDotlogicalExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_stringDotlogicalExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stringDotlogicalExpressionDotString(&mut self, ctx: &StringDotlogicalExpressionDotStringContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_stringDotlogicalExpressionDotString(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalExpressionDotString(&mut self, ctx: &LogicalExpressionDotStringContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_logicalExpressionDotString(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_ffStandardLogicalExpression(&mut self, ctx: &FfStandardLogicalExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_ffStandardLogicalExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalNot(&mut self, ctx: &LogicalNotContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_logicalNot(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalExpr(&mut self, ctx: &LogicalExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_logicalExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalAnd(&mut self, ctx: &LogicalAndContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_logicalAnd(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalXor(&mut self, ctx: &LogicalXorContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_logicalXor(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_logicalOr(&mut self, ctx: &LogicalOrContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_logicalOr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relevanceExpr(&mut self, ctx: &RelevanceExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_relevanceExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valueExpr(&mut self, ctx: &ValueExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_valueExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inExpr(&mut self, ctx: &InExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_inExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_isNullPredicate(&mut self, ctx: &IsNullPredicateContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_isNullPredicate(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_between(&mut self, ctx: &BetweenContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_between(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_compareExpr(&mut self, ctx: &CompareExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_compareExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nullNotnull(&mut self, ctx: &NullNotnullContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_nullNotnull(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_inSubqueryExpr(&mut self, ctx: &InSubqueryExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_inSubqueryExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lambdaExpr(&mut self, ctx: &LambdaExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_lambdaExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_literalValueExpr(&mut self, ctx: &LiteralValueExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_literalValueExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCallExpr(&mut self, ctx: &FunctionCallExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_functionCallExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_existsSubqueryExpr(&mut self, ctx: &ExistsSubqueryExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_existsSubqueryExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachPlaceholderExpr(&mut self, ctx: &ForeachPlaceholderExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachPlaceholderExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_scalarSubqueryExpr(&mut self, ctx: &ScalarSubqueryExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_scalarSubqueryExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_nestedValueExpr(&mut self, ctx: &NestedValueExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_nestedValueExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_binaryArithmetic(&mut self, ctx: &BinaryArithmeticContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_binaryArithmetic(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldExpr(&mut self, ctx: &FieldExprContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fieldExpr(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_evalExpression(&mut self, ctx: &EvalExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_evalExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionCall(&mut self, ctx: &FunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_functionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mvmapFunctionCall(&mut self, ctx: &MvmapFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_mvmapFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_positionFunctionCall(&mut self, ctx: &PositionFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_positionFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_caseFunctionCall(&mut self, ctx: &CaseFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_caseFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relevanceExpression(&mut self, ctx: &RelevanceExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_relevanceExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleFieldRelevanceFunction(&mut self, ctx: &SingleFieldRelevanceFunctionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_singleFieldRelevanceFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiFieldRelevanceFunction(&mut self, ctx: &MultiFieldRelevanceFunctionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_multiFieldRelevanceFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timeModifier(&mut self, ctx: &TimeModifierContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timeModifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timeModifierValue(&mut self, ctx: &TimeModifierValueContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timeModifierValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableSource(&mut self, ctx: &TableSourceContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_tableSource(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableFunction(&mut self, ctx: &TableFunctionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_tableFunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldList(&mut self, ctx: &FieldListContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fieldList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_invalidMixedSortField(&mut self, ctx: &InvalidMixedSortFieldContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_invalidMixedSortField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prefixSortField(&mut self, ctx: &PrefixSortFieldContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_prefixSortField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_suffixSortField(&mut self, ctx: &SuffixSortFieldContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_suffixSortField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_defaultSortField(&mut self, ctx: &DefaultSortFieldContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_defaultSortField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_sortFieldExpression(&mut self, ctx: &SortFieldExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_sortFieldExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldExpression(&mut self, ctx: &FieldExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_fieldExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_wcFieldExpression(&mut self, ctx: &WcFieldExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_wcFieldExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachPlaceholder(&mut self, ctx: &ForeachPlaceholderContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachPlaceholder(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_foreachPlaceholderName(&mut self, ctx: &ForeachPlaceholderNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_foreachPlaceholderName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_selectFieldExpression(&mut self, ctx: &SelectFieldExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_selectFieldExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_renameFieldExpression(&mut self, ctx: &RenameFieldExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_renameFieldExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_evalFunctionCall(&mut self, ctx: &EvalFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_evalFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dataTypeFunctionCall(&mut self, ctx: &DataTypeFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_dataTypeFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_convertedDataType(&mut self, ctx: &ConvertedDataTypeContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_convertedDataType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_evalFunctionName(&mut self, ctx: &EvalFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_evalFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionArgs(&mut self, ctx: &FunctionArgsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_functionArgs(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedFunctionArgs(&mut self, ctx: &NamedFunctionArgsContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_namedFunctionArgs(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionArg(&mut self, ctx: &FunctionArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_functionArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_namedFunctionArg(&mut self, ctx: &NamedFunctionArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_namedFunctionArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_functionArgExpression(&mut self, ctx: &FunctionArgExpressionContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_functionArgExpression(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_lambda(&mut self, ctx: &LambdaContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_lambda(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relevanceArg(&mut self, ctx: &RelevanceArgContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_relevanceArg(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relevanceArgName(&mut self, ctx: &RelevanceArgNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_relevanceArgName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relevanceFieldAndWeight(&mut self, ctx: &RelevanceFieldAndWeightContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_relevanceFieldAndWeight(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relevanceFieldWeight(&mut self, ctx: &RelevanceFieldWeightContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_relevanceFieldWeight(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relevanceField(&mut self, ctx: &RelevanceFieldContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_relevanceField(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relevanceQuery(&mut self, ctx: &RelevanceQueryContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_relevanceQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_relevanceArgValue(&mut self, ctx: &RelevanceArgValueContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_relevanceArgValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_mathematicalFunctionName(&mut self, ctx: &MathematicalFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_mathematicalFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_geoipFunctionName(&mut self, ctx: &GeoipFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_geoipFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_collectionFunctionName(&mut self, ctx: &CollectionFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_collectionFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_trigonometricFunctionName(&mut self, ctx: &TrigonometricFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_trigonometricFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_jsonFunctionName(&mut self, ctx: &JsonFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_jsonFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_cryptographicFunctionName(&mut self, ctx: &CryptographicFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_cryptographicFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dateTimeFunctionName(&mut self, ctx: &DateTimeFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_dateTimeFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_getFormatFunctionCall(&mut self, ctx: &GetFormatFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_getFormatFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_getFormatType(&mut self, ctx: &GetFormatTypeContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_getFormatType(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_extractFunctionCall(&mut self, ctx: &ExtractFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_extractFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_simpleDateTimePart(&mut self, ctx: &SimpleDateTimePartContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_simpleDateTimePart(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_complexDateTimePart(&mut self, ctx: &ComplexDateTimePartContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_complexDateTimePart(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_datetimePart(&mut self, ctx: &DatetimePartContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_datetimePart(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timestampFunctionCall(&mut self, ctx: &TimestampFunctionCallContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timestampFunctionCall(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timestampFunctionName(&mut self, ctx: &TimestampFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timestampFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_conditionFunctionName(&mut self, ctx: &ConditionFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_conditionFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_flowControlFunctionName(&mut self, ctx: &FlowControlFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_flowControlFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_systemFunctionName(&mut self, ctx: &SystemFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_systemFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_textFunctionName(&mut self, ctx: &TextFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_textFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_positionFunctionName(&mut self, ctx: &PositionFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_positionFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_comparisonOperator(&mut self, ctx: &ComparisonOperatorContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_comparisonOperator(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_singleFieldRelevanceFunctionName(&mut self, ctx: &SingleFieldRelevanceFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_singleFieldRelevanceFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_multiFieldRelevanceFunctionName(&mut self, ctx: &MultiFieldRelevanceFunctionNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_multiFieldRelevanceFunctionName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_literalValue(&mut self, ctx: &LiteralValueContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_literalValue(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalLiteral(&mut self, ctx: &IntervalLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_intervalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_stringLiteral(&mut self, ctx: &StringLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_stringLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_integerLiteral(&mut self, ctx: &IntegerLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_integerLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_decimalLiteral(&mut self, ctx: &DecimalLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_decimalLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_doubleLiteral(&mut self, ctx: &DoubleLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_doubleLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_floatLiteral(&mut self, ctx: &FloatLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_floatLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_booleanLiteral(&mut self, ctx: &BooleanLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_booleanLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_datetimeLiteral(&mut self, ctx: &DatetimeLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_datetimeLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_dateLiteral(&mut self, ctx: &DateLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_dateLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timeLiteral(&mut self, ctx: &TimeLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timeLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_timestampLiteral(&mut self, ctx: &TimestampLiteralContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_timestampLiteral(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_intervalUnit(&mut self, ctx: &IntervalUnitContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_intervalUnit(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_valueList(&mut self, ctx: &ValueListContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_valueList(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identsAsQualifiedName(&mut self, ctx: &IdentsAsQualifiedNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_identsAsQualifiedName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identsAsTableQualifiedName(&mut self, ctx: &IdentsAsTableQualifiedNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_identsAsTableQualifiedName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identsAsWildcardQualifiedName(&mut self, ctx: &IdentsAsWildcardQualifiedNameContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_identsAsWildcardQualifiedName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_identsAsQualifiedNameSeq(&mut self, ctx: &IdentsAsQualifiedNameSeqContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_identsAsQualifiedNameSeq(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_ident(&mut self, ctx: &IdentContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_ident(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_tableIdent(&mut self, ctx: &TableIdentContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_tableIdent(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_wildcard(&mut self, ctx: &WildcardContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_wildcard(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_keywordsCanBeId(&mut self, ctx: &KeywordsCanBeIdContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_keywordsCanBeId(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_searchableKeyWord(&mut self, ctx: &SearchableKeyWordContext<'input>){
		let result = <Self as OpenSearchPPLParserVisitorCompat>::visit_searchableKeyWord(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

}
