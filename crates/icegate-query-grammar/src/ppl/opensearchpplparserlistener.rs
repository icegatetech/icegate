#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]
#![allow(nonstandard_style)]
// Generated from OpenSearchPPLParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::ParseTreeListener;
use super::opensearchpplparser::*;

pub trait OpenSearchPPLParserListener<'input> : ParseTreeListener<'input,OpenSearchPPLParserContextType>{
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#root}.
 * @param ctx the parse tree
 */
fn enter_root(&mut self, _ctx: &RootContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#root}.
 * @param ctx the parse tree
 */
fn exit_root(&mut self, _ctx: &RootContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#pplStatement}.
 * @param ctx the parse tree
 */
fn enter_pplStatement(&mut self, _ctx: &PplStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#pplStatement}.
 * @param ctx the parse tree
 */
fn exit_pplStatement(&mut self, _ctx: &PplStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#subPipeline}.
 * @param ctx the parse tree
 */
fn enter_subPipeline(&mut self, _ctx: &SubPipelineContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#subPipeline}.
 * @param ctx the parse tree
 */
fn exit_subPipeline(&mut self, _ctx: &SubPipelineContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#queryStatement}.
 * @param ctx the parse tree
 */
fn enter_queryStatement(&mut self, _ctx: &QueryStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#queryStatement}.
 * @param ctx the parse tree
 */
fn exit_queryStatement(&mut self, _ctx: &QueryStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#explainStatement}.
 * @param ctx the parse tree
 */
fn enter_explainStatement(&mut self, _ctx: &ExplainStatementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#explainStatement}.
 * @param ctx the parse tree
 */
fn exit_explainStatement(&mut self, _ctx: &ExplainStatementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#explainMode}.
 * @param ctx the parse tree
 */
fn enter_explainMode(&mut self, _ctx: &ExplainModeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#explainMode}.
 * @param ctx the parse tree
 */
fn exit_explainMode(&mut self, _ctx: &ExplainModeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#subSearch}.
 * @param ctx the parse tree
 */
fn enter_subSearch(&mut self, _ctx: &SubSearchContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#subSearch}.
 * @param ctx the parse tree
 */
fn exit_subSearch(&mut self, _ctx: &SubSearchContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#pplCommands}.
 * @param ctx the parse tree
 */
fn enter_pplCommands(&mut self, _ctx: &PplCommandsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#pplCommands}.
 * @param ctx the parse tree
 */
fn exit_pplCommands(&mut self, _ctx: &PplCommandsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#commands}.
 * @param ctx the parse tree
 */
fn enter_commands(&mut self, _ctx: &CommandsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#commands}.
 * @param ctx the parse tree
 */
fn exit_commands(&mut self, _ctx: &CommandsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#commandName}.
 * @param ctx the parse tree
 */
fn enter_commandName(&mut self, _ctx: &CommandNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#commandName}.
 * @param ctx the parse tree
 */
fn exit_commandName(&mut self, _ctx: &CommandNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchFrom}
 * labeled alternative in {@link OpenSearchPPLParser#searchCommand}.
 * @param ctx the parse tree
 */
fn enter_searchFrom(&mut self, _ctx: &SearchFromContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchFrom}
 * labeled alternative in {@link OpenSearchPPLParser#searchCommand}.
 * @param ctx the parse tree
 */
fn exit_searchFrom(&mut self, _ctx: &SearchFromContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code orExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn enter_orExpression(&mut self, _ctx: &OrExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code orExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn exit_orExpression(&mut self, _ctx: &OrExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code timeModifierExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn enter_timeModifierExpression(&mut self, _ctx: &TimeModifierExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code timeModifierExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn exit_timeModifierExpression(&mut self, _ctx: &TimeModifierExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code andExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn enter_andExpression(&mut self, _ctx: &AndExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code andExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn exit_andExpression(&mut self, _ctx: &AndExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code notExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn enter_notExpression(&mut self, _ctx: &NotExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code notExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn exit_notExpression(&mut self, _ctx: &NotExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code termExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn enter_termExpression(&mut self, _ctx: &TermExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code termExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn exit_termExpression(&mut self, _ctx: &TermExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code groupedExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn enter_groupedExpression(&mut self, _ctx: &GroupedExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code groupedExpression}
 * labeled alternative in {@link OpenSearchPPLParser#searchExpression}.
 * @param ctx the parse tree
 */
fn exit_groupedExpression(&mut self, _ctx: &GroupedExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchComparisonTerm}
 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
 * @param ctx the parse tree
 */
fn enter_searchComparisonTerm(&mut self, _ctx: &SearchComparisonTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchComparisonTerm}
 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
 * @param ctx the parse tree
 */
fn exit_searchComparisonTerm(&mut self, _ctx: &SearchComparisonTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchInListTerm}
 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
 * @param ctx the parse tree
 */
fn enter_searchInListTerm(&mut self, _ctx: &SearchInListTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchInListTerm}
 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
 * @param ctx the parse tree
 */
fn exit_searchInListTerm(&mut self, _ctx: &SearchInListTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchLiteralTerm}
 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
 * @param ctx the parse tree
 */
fn enter_searchLiteralTerm(&mut self, _ctx: &SearchLiteralTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchLiteralTerm}
 * labeled alternative in {@link OpenSearchPPLParser#searchTerm}.
 * @param ctx the parse tree
 */
fn exit_searchLiteralTerm(&mut self, _ctx: &SearchLiteralTermContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#searchLiteral}.
 * @param ctx the parse tree
 */
fn enter_searchLiteral(&mut self, _ctx: &SearchLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#searchLiteral}.
 * @param ctx the parse tree
 */
fn exit_searchLiteral(&mut self, _ctx: &SearchLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchFieldCompare}
 * labeled alternative in {@link OpenSearchPPLParser#searchFieldComparison}.
 * @param ctx the parse tree
 */
fn enter_searchFieldCompare(&mut self, _ctx: &SearchFieldCompareContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchFieldCompare}
 * labeled alternative in {@link OpenSearchPPLParser#searchFieldComparison}.
 * @param ctx the parse tree
 */
fn exit_searchFieldCompare(&mut self, _ctx: &SearchFieldCompareContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchFieldInValues}
 * labeled alternative in {@link OpenSearchPPLParser#searchFieldInList}.
 * @param ctx the parse tree
 */
fn enter_searchFieldInValues(&mut self, _ctx: &SearchFieldInValuesContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchFieldInValues}
 * labeled alternative in {@link OpenSearchPPLParser#searchFieldInList}.
 * @param ctx the parse tree
 */
fn exit_searchFieldInValues(&mut self, _ctx: &SearchFieldInValuesContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code searchLiterals}
 * labeled alternative in {@link OpenSearchPPLParser#searchLiteralList}.
 * @param ctx the parse tree
 */
fn enter_searchLiterals(&mut self, _ctx: &SearchLiteralsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code searchLiterals}
 * labeled alternative in {@link OpenSearchPPLParser#searchLiteralList}.
 * @param ctx the parse tree
 */
fn exit_searchLiterals(&mut self, _ctx: &SearchLiteralsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code equals}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_equals(&mut self, _ctx: &EqualsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code equals}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_equals(&mut self, _ctx: &EqualsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code notEquals}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_notEquals(&mut self, _ctx: &NotEqualsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code notEquals}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_notEquals(&mut self, _ctx: &NotEqualsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lessThan}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_lessThan(&mut self, _ctx: &LessThanContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lessThan}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_lessThan(&mut self, _ctx: &LessThanContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lessOrEqual}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_lessOrEqual(&mut self, _ctx: &LessOrEqualContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lessOrEqual}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_lessOrEqual(&mut self, _ctx: &LessOrEqualContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code greaterThan}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_greaterThan(&mut self, _ctx: &GreaterThanContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code greaterThan}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_greaterThan(&mut self, _ctx: &GreaterThanContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code greaterOrEqual}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_greaterOrEqual(&mut self, _ctx: &GreaterOrEqualContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code greaterOrEqual}
 * labeled alternative in {@link OpenSearchPPLParser#searchComparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_greaterOrEqual(&mut self, _ctx: &GreaterOrEqualContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#describeCommand}.
 * @param ctx the parse tree
 */
fn enter_describeCommand(&mut self, _ctx: &DescribeCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#describeCommand}.
 * @param ctx the parse tree
 */
fn exit_describeCommand(&mut self, _ctx: &DescribeCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#restCommand}.
 * @param ctx the parse tree
 */
fn enter_restCommand(&mut self, _ctx: &RestCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#restCommand}.
 * @param ctx the parse tree
 */
fn exit_restCommand(&mut self, _ctx: &RestCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#restArgument}.
 * @param ctx the parse tree
 */
fn enter_restArgument(&mut self, _ctx: &RestArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#restArgument}.
 * @param ctx the parse tree
 */
fn exit_restArgument(&mut self, _ctx: &RestArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#showDataSourcesCommand}.
 * @param ctx the parse tree
 */
fn enter_showDataSourcesCommand(&mut self, _ctx: &ShowDataSourcesCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#showDataSourcesCommand}.
 * @param ctx the parse tree
 */
fn exit_showDataSourcesCommand(&mut self, _ctx: &ShowDataSourcesCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#makeresultsCommand}.
 * @param ctx the parse tree
 */
fn enter_makeresultsCommand(&mut self, _ctx: &MakeresultsCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#makeresultsCommand}.
 * @param ctx the parse tree
 */
fn exit_makeresultsCommand(&mut self, _ctx: &MakeresultsCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#makeresultsArg}.
 * @param ctx the parse tree
 */
fn enter_makeresultsArg(&mut self, _ctx: &MakeresultsArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#makeresultsArg}.
 * @param ctx the parse tree
 */
fn exit_makeresultsArg(&mut self, _ctx: &MakeresultsArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#whereCommand}.
 * @param ctx the parse tree
 */
fn enter_whereCommand(&mut self, _ctx: &WhereCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#whereCommand}.
 * @param ctx the parse tree
 */
fn exit_whereCommand(&mut self, _ctx: &WhereCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#fieldsCommand}.
 * @param ctx the parse tree
 */
fn enter_fieldsCommand(&mut self, _ctx: &FieldsCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#fieldsCommand}.
 * @param ctx the parse tree
 */
fn exit_fieldsCommand(&mut self, _ctx: &FieldsCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#tableCommand}.
 * @param ctx the parse tree
 */
fn enter_tableCommand(&mut self, _ctx: &TableCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#tableCommand}.
 * @param ctx the parse tree
 */
fn exit_tableCommand(&mut self, _ctx: &TableCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#fieldsCommandBody}.
 * @param ctx the parse tree
 */
fn enter_fieldsCommandBody(&mut self, _ctx: &FieldsCommandBodyContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#fieldsCommandBody}.
 * @param ctx the parse tree
 */
fn exit_fieldsCommandBody(&mut self, _ctx: &FieldsCommandBodyContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#wcFieldList}.
 * @param ctx the parse tree
 */
fn enter_wcFieldList(&mut self, _ctx: &WcFieldListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#wcFieldList}.
 * @param ctx the parse tree
 */
fn exit_wcFieldList(&mut self, _ctx: &WcFieldListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#renameCommand}.
 * @param ctx the parse tree
 */
fn enter_renameCommand(&mut self, _ctx: &RenameCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#renameCommand}.
 * @param ctx the parse tree
 */
fn exit_renameCommand(&mut self, _ctx: &RenameCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#replaceCommand}.
 * @param ctx the parse tree
 */
fn enter_replaceCommand(&mut self, _ctx: &ReplaceCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#replaceCommand}.
 * @param ctx the parse tree
 */
fn exit_replaceCommand(&mut self, _ctx: &ReplaceCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#replacePair}.
 * @param ctx the parse tree
 */
fn enter_replacePair(&mut self, _ctx: &ReplacePairContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#replacePair}.
 * @param ctx the parse tree
 */
fn exit_replacePair(&mut self, _ctx: &ReplacePairContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#statsCommand}.
 * @param ctx the parse tree
 */
fn enter_statsCommand(&mut self, _ctx: &StatsCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#statsCommand}.
 * @param ctx the parse tree
 */
fn exit_statsCommand(&mut self, _ctx: &StatsCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#appendPipeCommand}.
 * @param ctx the parse tree
 */
fn enter_appendPipeCommand(&mut self, _ctx: &AppendPipeCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#appendPipeCommand}.
 * @param ctx the parse tree
 */
fn exit_appendPipeCommand(&mut self, _ctx: &AppendPipeCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#statsArgs}.
 * @param ctx the parse tree
 */
fn enter_statsArgs(&mut self, _ctx: &StatsArgsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#statsArgs}.
 * @param ctx the parse tree
 */
fn exit_statsArgs(&mut self, _ctx: &StatsArgsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#partitionsArg}.
 * @param ctx the parse tree
 */
fn enter_partitionsArg(&mut self, _ctx: &PartitionsArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#partitionsArg}.
 * @param ctx the parse tree
 */
fn exit_partitionsArg(&mut self, _ctx: &PartitionsArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#allnumArg}.
 * @param ctx the parse tree
 */
fn enter_allnumArg(&mut self, _ctx: &AllnumArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#allnumArg}.
 * @param ctx the parse tree
 */
fn exit_allnumArg(&mut self, _ctx: &AllnumArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#delimArg}.
 * @param ctx the parse tree
 */
fn enter_delimArg(&mut self, _ctx: &DelimArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#delimArg}.
 * @param ctx the parse tree
 */
fn exit_delimArg(&mut self, _ctx: &DelimArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#bucketNullableArg}.
 * @param ctx the parse tree
 */
fn enter_bucketNullableArg(&mut self, _ctx: &BucketNullableArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#bucketNullableArg}.
 * @param ctx the parse tree
 */
fn exit_bucketNullableArg(&mut self, _ctx: &BucketNullableArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#dedupSplitArg}.
 * @param ctx the parse tree
 */
fn enter_dedupSplitArg(&mut self, _ctx: &DedupSplitArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#dedupSplitArg}.
 * @param ctx the parse tree
 */
fn exit_dedupSplitArg(&mut self, _ctx: &DedupSplitArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#eventstatsCommand}.
 * @param ctx the parse tree
 */
fn enter_eventstatsCommand(&mut self, _ctx: &EventstatsCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#eventstatsCommand}.
 * @param ctx the parse tree
 */
fn exit_eventstatsCommand(&mut self, _ctx: &EventstatsCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#streamstatsCommand}.
 * @param ctx the parse tree
 */
fn enter_streamstatsCommand(&mut self, _ctx: &StreamstatsCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#streamstatsCommand}.
 * @param ctx the parse tree
 */
fn exit_streamstatsCommand(&mut self, _ctx: &StreamstatsCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#streamstatsArgs}.
 * @param ctx the parse tree
 */
fn enter_streamstatsArgs(&mut self, _ctx: &StreamstatsArgsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#streamstatsArgs}.
 * @param ctx the parse tree
 */
fn exit_streamstatsArgs(&mut self, _ctx: &StreamstatsArgsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#currentArg}.
 * @param ctx the parse tree
 */
fn enter_currentArg(&mut self, _ctx: &CurrentArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#currentArg}.
 * @param ctx the parse tree
 */
fn exit_currentArg(&mut self, _ctx: &CurrentArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#windowArg}.
 * @param ctx the parse tree
 */
fn enter_windowArg(&mut self, _ctx: &WindowArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#windowArg}.
 * @param ctx the parse tree
 */
fn exit_windowArg(&mut self, _ctx: &WindowArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#globalArg}.
 * @param ctx the parse tree
 */
fn enter_globalArg(&mut self, _ctx: &GlobalArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#globalArg}.
 * @param ctx the parse tree
 */
fn exit_globalArg(&mut self, _ctx: &GlobalArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#resetBeforeArg}.
 * @param ctx the parse tree
 */
fn enter_resetBeforeArg(&mut self, _ctx: &ResetBeforeArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#resetBeforeArg}.
 * @param ctx the parse tree
 */
fn exit_resetBeforeArg(&mut self, _ctx: &ResetBeforeArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#resetAfterArg}.
 * @param ctx the parse tree
 */
fn enter_resetAfterArg(&mut self, _ctx: &ResetAfterArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#resetAfterArg}.
 * @param ctx the parse tree
 */
fn exit_resetAfterArg(&mut self, _ctx: &ResetAfterArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#dedupCommand}.
 * @param ctx the parse tree
 */
fn enter_dedupCommand(&mut self, _ctx: &DedupCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#dedupCommand}.
 * @param ctx the parse tree
 */
fn exit_dedupCommand(&mut self, _ctx: &DedupCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#sortCommand}.
 * @param ctx the parse tree
 */
fn enter_sortCommand(&mut self, _ctx: &SortCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#sortCommand}.
 * @param ctx the parse tree
 */
fn exit_sortCommand(&mut self, _ctx: &SortCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#reverseCommand}.
 * @param ctx the parse tree
 */
fn enter_reverseCommand(&mut self, _ctx: &ReverseCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#reverseCommand}.
 * @param ctx the parse tree
 */
fn exit_reverseCommand(&mut self, _ctx: &ReverseCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#chartCommand}.
 * @param ctx the parse tree
 */
fn enter_chartCommand(&mut self, _ctx: &ChartCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#chartCommand}.
 * @param ctx the parse tree
 */
fn exit_chartCommand(&mut self, _ctx: &ChartCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#chartOptions}.
 * @param ctx the parse tree
 */
fn enter_chartOptions(&mut self, _ctx: &ChartOptionsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#chartOptions}.
 * @param ctx the parse tree
 */
fn exit_chartOptions(&mut self, _ctx: &ChartOptionsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#rowSplit}.
 * @param ctx the parse tree
 */
fn enter_rowSplit(&mut self, _ctx: &RowSplitContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#rowSplit}.
 * @param ctx the parse tree
 */
fn exit_rowSplit(&mut self, _ctx: &RowSplitContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#columnSplit}.
 * @param ctx the parse tree
 */
fn enter_columnSplit(&mut self, _ctx: &ColumnSplitContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#columnSplit}.
 * @param ctx the parse tree
 */
fn exit_columnSplit(&mut self, _ctx: &ColumnSplitContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timechartCommand}.
 * @param ctx the parse tree
 */
fn enter_timechartCommand(&mut self, _ctx: &TimechartCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timechartCommand}.
 * @param ctx the parse tree
 */
fn exit_timechartCommand(&mut self, _ctx: &TimechartCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#transposeCommand}.
 * @param ctx the parse tree
 */
fn enter_transposeCommand(&mut self, _ctx: &TransposeCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#transposeCommand}.
 * @param ctx the parse tree
 */
fn exit_transposeCommand(&mut self, _ctx: &TransposeCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#transposeParameter}.
 * @param ctx the parse tree
 */
fn enter_transposeParameter(&mut self, _ctx: &TransposeParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#transposeParameter}.
 * @param ctx the parse tree
 */
fn exit_transposeParameter(&mut self, _ctx: &TransposeParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timewrapCommand}.
 * @param ctx the parse tree
 */
fn enter_timewrapCommand(&mut self, _ctx: &TimewrapCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timewrapCommand}.
 * @param ctx the parse tree
 */
fn exit_timewrapCommand(&mut self, _ctx: &TimewrapCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timewrapParameter}.
 * @param ctx the parse tree
 */
fn enter_timewrapParameter(&mut self, _ctx: &TimewrapParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timewrapParameter}.
 * @param ctx the parse tree
 */
fn exit_timewrapParameter(&mut self, _ctx: &TimewrapParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timewrapAlign}.
 * @param ctx the parse tree
 */
fn enter_timewrapAlign(&mut self, _ctx: &TimewrapAlignContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timewrapAlign}.
 * @param ctx the parse tree
 */
fn exit_timewrapAlign(&mut self, _ctx: &TimewrapAlignContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timewrapSeries}.
 * @param ctx the parse tree
 */
fn enter_timewrapSeries(&mut self, _ctx: &TimewrapSeriesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timewrapSeries}.
 * @param ctx the parse tree
 */
fn exit_timewrapSeries(&mut self, _ctx: &TimewrapSeriesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timechartParameter}.
 * @param ctx the parse tree
 */
fn enter_timechartParameter(&mut self, _ctx: &TimechartParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timechartParameter}.
 * @param ctx the parse tree
 */
fn exit_timechartParameter(&mut self, _ctx: &TimechartParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#spanLiteral}.
 * @param ctx the parse tree
 */
fn enter_spanLiteral(&mut self, _ctx: &SpanLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#spanLiteral}.
 * @param ctx the parse tree
 */
fn exit_spanLiteral(&mut self, _ctx: &SpanLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#evalCommand}.
 * @param ctx the parse tree
 */
fn enter_evalCommand(&mut self, _ctx: &EvalCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#evalCommand}.
 * @param ctx the parse tree
 */
fn exit_evalCommand(&mut self, _ctx: &EvalCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#foreachCommand}.
 * @param ctx the parse tree
 */
fn enter_foreachCommand(&mut self, _ctx: &ForeachCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#foreachCommand}.
 * @param ctx the parse tree
 */
fn exit_foreachCommand(&mut self, _ctx: &ForeachCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#foreachArgument}.
 * @param ctx the parse tree
 */
fn enter_foreachArgument(&mut self, _ctx: &ForeachArgumentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#foreachArgument}.
 * @param ctx the parse tree
 */
fn exit_foreachArgument(&mut self, _ctx: &ForeachArgumentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#foreachTarget}.
 * @param ctx the parse tree
 */
fn enter_foreachTarget(&mut self, _ctx: &ForeachTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#foreachTarget}.
 * @param ctx the parse tree
 */
fn exit_foreachTarget(&mut self, _ctx: &ForeachTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#foreachOption}.
 * @param ctx the parse tree
 */
fn enter_foreachOption(&mut self, _ctx: &ForeachOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#foreachOption}.
 * @param ctx the parse tree
 */
fn exit_foreachOption(&mut self, _ctx: &ForeachOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#foreachEvalCommand}.
 * @param ctx the parse tree
 */
fn enter_foreachEvalCommand(&mut self, _ctx: &ForeachEvalCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#foreachEvalCommand}.
 * @param ctx the parse tree
 */
fn exit_foreachEvalCommand(&mut self, _ctx: &ForeachEvalCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#foreachEvalClause}.
 * @param ctx the parse tree
 */
fn enter_foreachEvalClause(&mut self, _ctx: &ForeachEvalClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#foreachEvalClause}.
 * @param ctx the parse tree
 */
fn exit_foreachEvalClause(&mut self, _ctx: &ForeachEvalClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#foreachEvalTarget}.
 * @param ctx the parse tree
 */
fn enter_foreachEvalTarget(&mut self, _ctx: &ForeachEvalTargetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#foreachEvalTarget}.
 * @param ctx the parse tree
 */
fn exit_foreachEvalTarget(&mut self, _ctx: &ForeachEvalTargetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#fieldformatCommand}.
 * @param ctx the parse tree
 */
fn enter_fieldformatCommand(&mut self, _ctx: &FieldformatCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#fieldformatCommand}.
 * @param ctx the parse tree
 */
fn exit_fieldformatCommand(&mut self, _ctx: &FieldformatCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#headCommand}.
 * @param ctx the parse tree
 */
fn enter_headCommand(&mut self, _ctx: &HeadCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#headCommand}.
 * @param ctx the parse tree
 */
fn exit_headCommand(&mut self, _ctx: &HeadCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#binCommand}.
 * @param ctx the parse tree
 */
fn enter_binCommand(&mut self, _ctx: &BinCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#binCommand}.
 * @param ctx the parse tree
 */
fn exit_binCommand(&mut self, _ctx: &BinCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#binOption}.
 * @param ctx the parse tree
 */
fn enter_binOption(&mut self, _ctx: &BinOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#binOption}.
 * @param ctx the parse tree
 */
fn exit_binOption(&mut self, _ctx: &BinOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#aligntimeValue}.
 * @param ctx the parse tree
 */
fn enter_aligntimeValue(&mut self, _ctx: &AligntimeValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#aligntimeValue}.
 * @param ctx the parse tree
 */
fn exit_aligntimeValue(&mut self, _ctx: &AligntimeValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code numericSpanValue}
 * labeled alternative in {@link OpenSearchPPLParser#binSpanValue}.
 * @param ctx the parse tree
 */
fn enter_numericSpanValue(&mut self, _ctx: &NumericSpanValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code numericSpanValue}
 * labeled alternative in {@link OpenSearchPPLParser#binSpanValue}.
 * @param ctx the parse tree
 */
fn exit_numericSpanValue(&mut self, _ctx: &NumericSpanValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logBasedSpanValue}
 * labeled alternative in {@link OpenSearchPPLParser#binSpanValue}.
 * @param ctx the parse tree
 */
fn enter_logBasedSpanValue(&mut self, _ctx: &LogBasedSpanValueContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logBasedSpanValue}
 * labeled alternative in {@link OpenSearchPPLParser#binSpanValue}.
 * @param ctx the parse tree
 */
fn exit_logBasedSpanValue(&mut self, _ctx: &LogBasedSpanValueContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logWithBaseSpan}
 * labeled alternative in {@link OpenSearchPPLParser#logSpanValue}.
 * @param ctx the parse tree
 */
fn enter_logWithBaseSpan(&mut self, _ctx: &LogWithBaseSpanContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logWithBaseSpan}
 * labeled alternative in {@link OpenSearchPPLParser#logSpanValue}.
 * @param ctx the parse tree
 */
fn exit_logWithBaseSpan(&mut self, _ctx: &LogWithBaseSpanContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#rareTopCommand}.
 * @param ctx the parse tree
 */
fn enter_rareTopCommand(&mut self, _ctx: &RareTopCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#rareTopCommand}.
 * @param ctx the parse tree
 */
fn exit_rareTopCommand(&mut self, _ctx: &RareTopCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#rareTopOption}.
 * @param ctx the parse tree
 */
fn enter_rareTopOption(&mut self, _ctx: &RareTopOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#rareTopOption}.
 * @param ctx the parse tree
 */
fn exit_rareTopOption(&mut self, _ctx: &RareTopOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#grokCommand}.
 * @param ctx the parse tree
 */
fn enter_grokCommand(&mut self, _ctx: &GrokCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#grokCommand}.
 * @param ctx the parse tree
 */
fn exit_grokCommand(&mut self, _ctx: &GrokCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#parseCommand}.
 * @param ctx the parse tree
 */
fn enter_parseCommand(&mut self, _ctx: &ParseCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#parseCommand}.
 * @param ctx the parse tree
 */
fn exit_parseCommand(&mut self, _ctx: &ParseCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#spathCommand}.
 * @param ctx the parse tree
 */
fn enter_spathCommand(&mut self, _ctx: &SpathCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#spathCommand}.
 * @param ctx the parse tree
 */
fn exit_spathCommand(&mut self, _ctx: &SpathCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#spathParameter}.
 * @param ctx the parse tree
 */
fn enter_spathParameter(&mut self, _ctx: &SpathParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#spathParameter}.
 * @param ctx the parse tree
 */
fn exit_spathParameter(&mut self, _ctx: &SpathParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#indexablePath}.
 * @param ctx the parse tree
 */
fn enter_indexablePath(&mut self, _ctx: &IndexablePathContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#indexablePath}.
 * @param ctx the parse tree
 */
fn exit_indexablePath(&mut self, _ctx: &IndexablePathContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#pathElement}.
 * @param ctx the parse tree
 */
fn enter_pathElement(&mut self, _ctx: &PathElementContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#pathElement}.
 * @param ctx the parse tree
 */
fn exit_pathElement(&mut self, _ctx: &PathElementContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#pathArrayAccess}.
 * @param ctx the parse tree
 */
fn enter_pathArrayAccess(&mut self, _ctx: &PathArrayAccessContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#pathArrayAccess}.
 * @param ctx the parse tree
 */
fn exit_pathArrayAccess(&mut self, _ctx: &PathArrayAccessContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#regexCommand}.
 * @param ctx the parse tree
 */
fn enter_regexCommand(&mut self, _ctx: &RegexCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#regexCommand}.
 * @param ctx the parse tree
 */
fn exit_regexCommand(&mut self, _ctx: &RegexCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#regexExpr}.
 * @param ctx the parse tree
 */
fn enter_regexExpr(&mut self, _ctx: &RegexExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#regexExpr}.
 * @param ctx the parse tree
 */
fn exit_regexExpr(&mut self, _ctx: &RegexExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#rexCommand}.
 * @param ctx the parse tree
 */
fn enter_rexCommand(&mut self, _ctx: &RexCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#rexCommand}.
 * @param ctx the parse tree
 */
fn exit_rexCommand(&mut self, _ctx: &RexCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#rexExpr}.
 * @param ctx the parse tree
 */
fn enter_rexExpr(&mut self, _ctx: &RexExprContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#rexExpr}.
 * @param ctx the parse tree
 */
fn exit_rexExpr(&mut self, _ctx: &RexExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#rexOption}.
 * @param ctx the parse tree
 */
fn enter_rexOption(&mut self, _ctx: &RexOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#rexOption}.
 * @param ctx the parse tree
 */
fn exit_rexOption(&mut self, _ctx: &RexOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#patternsMethod}.
 * @param ctx the parse tree
 */
fn enter_patternsMethod(&mut self, _ctx: &PatternsMethodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#patternsMethod}.
 * @param ctx the parse tree
 */
fn exit_patternsMethod(&mut self, _ctx: &PatternsMethodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#patternsCommand}.
 * @param ctx the parse tree
 */
fn enter_patternsCommand(&mut self, _ctx: &PatternsCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#patternsCommand}.
 * @param ctx the parse tree
 */
fn exit_patternsCommand(&mut self, _ctx: &PatternsCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#patternsCommandOption}.
 * @param ctx the parse tree
 */
fn enter_patternsCommandOption(&mut self, _ctx: &PatternsCommandOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#patternsCommandOption}.
 * @param ctx the parse tree
 */
fn exit_patternsCommandOption(&mut self, _ctx: &PatternsCommandOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#patternsParameter}.
 * @param ctx the parse tree
 */
fn enter_patternsParameter(&mut self, _ctx: &PatternsParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#patternsParameter}.
 * @param ctx the parse tree
 */
fn exit_patternsParameter(&mut self, _ctx: &PatternsParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#patternMethod}.
 * @param ctx the parse tree
 */
fn enter_patternMethod(&mut self, _ctx: &PatternMethodContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#patternMethod}.
 * @param ctx the parse tree
 */
fn exit_patternMethod(&mut self, _ctx: &PatternMethodContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#patternMode}.
 * @param ctx the parse tree
 */
fn enter_patternMode(&mut self, _ctx: &PatternModeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#patternMode}.
 * @param ctx the parse tree
 */
fn exit_patternMode(&mut self, _ctx: &PatternModeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#lookupCommand}.
 * @param ctx the parse tree
 */
fn enter_lookupCommand(&mut self, _ctx: &LookupCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#lookupCommand}.
 * @param ctx the parse tree
 */
fn exit_lookupCommand(&mut self, _ctx: &LookupCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#lookupMappingList}.
 * @param ctx the parse tree
 */
fn enter_lookupMappingList(&mut self, _ctx: &LookupMappingListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#lookupMappingList}.
 * @param ctx the parse tree
 */
fn exit_lookupMappingList(&mut self, _ctx: &LookupMappingListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#outputCandidateList}.
 * @param ctx the parse tree
 */
fn enter_outputCandidateList(&mut self, _ctx: &OutputCandidateListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#outputCandidateList}.
 * @param ctx the parse tree
 */
fn exit_outputCandidateList(&mut self, _ctx: &OutputCandidateListContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#lookupPair}.
 * @param ctx the parse tree
 */
fn enter_lookupPair(&mut self, _ctx: &LookupPairContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#lookupPair}.
 * @param ctx the parse tree
 */
fn exit_lookupPair(&mut self, _ctx: &LookupPairContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code fillNullWithClause}
 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
 * @param ctx the parse tree
 */
fn enter_fillNullWithClause(&mut self, _ctx: &FillNullWithClauseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code fillNullWithClause}
 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
 * @param ctx the parse tree
 */
fn exit_fillNullWithClause(&mut self, _ctx: &FillNullWithClauseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code fillNullUsingClause}
 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
 * @param ctx the parse tree
 */
fn enter_fillNullUsingClause(&mut self, _ctx: &FillNullUsingClauseContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code fillNullUsingClause}
 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
 * @param ctx the parse tree
 */
fn exit_fillNullUsingClause(&mut self, _ctx: &FillNullUsingClauseContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code fillNullValueWithFields}
 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
 * @param ctx the parse tree
 */
fn enter_fillNullValueWithFields(&mut self, _ctx: &FillNullValueWithFieldsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code fillNullValueWithFields}
 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
 * @param ctx the parse tree
 */
fn exit_fillNullValueWithFields(&mut self, _ctx: &FillNullValueWithFieldsContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code fillNullValueAllFields}
 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
 * @param ctx the parse tree
 */
fn enter_fillNullValueAllFields(&mut self, _ctx: &FillNullValueAllFieldsContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code fillNullValueAllFields}
 * labeled alternative in {@link OpenSearchPPLParser#fillnullCommand}.
 * @param ctx the parse tree
 */
fn exit_fillNullValueAllFields(&mut self, _ctx: &FillNullValueAllFieldsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#fillNullWith}.
 * @param ctx the parse tree
 */
fn enter_fillNullWith(&mut self, _ctx: &FillNullWithContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#fillNullWith}.
 * @param ctx the parse tree
 */
fn exit_fillNullWith(&mut self, _ctx: &FillNullWithContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#fillNullUsing}.
 * @param ctx the parse tree
 */
fn enter_fillNullUsing(&mut self, _ctx: &FillNullUsingContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#fillNullUsing}.
 * @param ctx the parse tree
 */
fn exit_fillNullUsing(&mut self, _ctx: &FillNullUsingContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#replacementPair}.
 * @param ctx the parse tree
 */
fn enter_replacementPair(&mut self, _ctx: &ReplacementPairContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#replacementPair}.
 * @param ctx the parse tree
 */
fn exit_replacementPair(&mut self, _ctx: &ReplacementPairContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#convertCommand}.
 * @param ctx the parse tree
 */
fn enter_convertCommand(&mut self, _ctx: &ConvertCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#convertCommand}.
 * @param ctx the parse tree
 */
fn exit_convertCommand(&mut self, _ctx: &ConvertCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#convertFunction}.
 * @param ctx the parse tree
 */
fn enter_convertFunction(&mut self, _ctx: &ConvertFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#convertFunction}.
 * @param ctx the parse tree
 */
fn exit_convertFunction(&mut self, _ctx: &ConvertFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#trendlineCommand}.
 * @param ctx the parse tree
 */
fn enter_trendlineCommand(&mut self, _ctx: &TrendlineCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#trendlineCommand}.
 * @param ctx the parse tree
 */
fn exit_trendlineCommand(&mut self, _ctx: &TrendlineCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#trendlineClause}.
 * @param ctx the parse tree
 */
fn enter_trendlineClause(&mut self, _ctx: &TrendlineClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#trendlineClause}.
 * @param ctx the parse tree
 */
fn exit_trendlineClause(&mut self, _ctx: &TrendlineClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#trendlineType}.
 * @param ctx the parse tree
 */
fn enter_trendlineType(&mut self, _ctx: &TrendlineTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#trendlineType}.
 * @param ctx the parse tree
 */
fn exit_trendlineType(&mut self, _ctx: &TrendlineTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#expandCommand}.
 * @param ctx the parse tree
 */
fn enter_expandCommand(&mut self, _ctx: &ExpandCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#expandCommand}.
 * @param ctx the parse tree
 */
fn exit_expandCommand(&mut self, _ctx: &ExpandCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#mvcombineCommand}.
 * @param ctx the parse tree
 */
fn enter_mvcombineCommand(&mut self, _ctx: &MvcombineCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#mvcombineCommand}.
 * @param ctx the parse tree
 */
fn exit_mvcombineCommand(&mut self, _ctx: &MvcombineCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#nomvCommand}.
 * @param ctx the parse tree
 */
fn enter_nomvCommand(&mut self, _ctx: &NomvCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#nomvCommand}.
 * @param ctx the parse tree
 */
fn exit_nomvCommand(&mut self, _ctx: &NomvCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#mvexpandCommand}.
 * @param ctx the parse tree
 */
fn enter_mvexpandCommand(&mut self, _ctx: &MvexpandCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#mvexpandCommand}.
 * @param ctx the parse tree
 */
fn exit_mvexpandCommand(&mut self, _ctx: &MvexpandCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#flattenCommand}.
 * @param ctx the parse tree
 */
fn enter_flattenCommand(&mut self, _ctx: &FlattenCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#flattenCommand}.
 * @param ctx the parse tree
 */
fn exit_flattenCommand(&mut self, _ctx: &FlattenCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#appendcolCommand}.
 * @param ctx the parse tree
 */
fn enter_appendcolCommand(&mut self, _ctx: &AppendcolCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#appendcolCommand}.
 * @param ctx the parse tree
 */
fn exit_appendcolCommand(&mut self, _ctx: &AppendcolCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#appendCommand}.
 * @param ctx the parse tree
 */
fn enter_appendCommand(&mut self, _ctx: &AppendCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#appendCommand}.
 * @param ctx the parse tree
 */
fn exit_appendCommand(&mut self, _ctx: &AppendCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#multisearchCommand}.
 * @param ctx the parse tree
 */
fn enter_multisearchCommand(&mut self, _ctx: &MultisearchCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#multisearchCommand}.
 * @param ctx the parse tree
 */
fn exit_multisearchCommand(&mut self, _ctx: &MultisearchCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#unionCommand}.
 * @param ctx the parse tree
 */
fn enter_unionCommand(&mut self, _ctx: &UnionCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#unionCommand}.
 * @param ctx the parse tree
 */
fn exit_unionCommand(&mut self, _ctx: &UnionCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#subsearchOptions}.
 * @param ctx the parse tree
 */
fn enter_subsearchOptions(&mut self, _ctx: &SubsearchOptionsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#subsearchOptions}.
 * @param ctx the parse tree
 */
fn exit_subsearchOptions(&mut self, _ctx: &SubsearchOptionsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#unionDataset}.
 * @param ctx the parse tree
 */
fn enter_unionDataset(&mut self, _ctx: &UnionDatasetContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#unionDataset}.
 * @param ctx the parse tree
 */
fn exit_unionDataset(&mut self, _ctx: &UnionDatasetContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#kmeansCommand}.
 * @param ctx the parse tree
 */
fn enter_kmeansCommand(&mut self, _ctx: &KmeansCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#kmeansCommand}.
 * @param ctx the parse tree
 */
fn exit_kmeansCommand(&mut self, _ctx: &KmeansCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#kmeansParameter}.
 * @param ctx the parse tree
 */
fn enter_kmeansParameter(&mut self, _ctx: &KmeansParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#kmeansParameter}.
 * @param ctx the parse tree
 */
fn exit_kmeansParameter(&mut self, _ctx: &KmeansParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#adCommand}.
 * @param ctx the parse tree
 */
fn enter_adCommand(&mut self, _ctx: &AdCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#adCommand}.
 * @param ctx the parse tree
 */
fn exit_adCommand(&mut self, _ctx: &AdCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#adParameter}.
 * @param ctx the parse tree
 */
fn enter_adParameter(&mut self, _ctx: &AdParameterContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#adParameter}.
 * @param ctx the parse tree
 */
fn exit_adParameter(&mut self, _ctx: &AdParameterContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#mlCommand}.
 * @param ctx the parse tree
 */
fn enter_mlCommand(&mut self, _ctx: &MlCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#mlCommand}.
 * @param ctx the parse tree
 */
fn exit_mlCommand(&mut self, _ctx: &MlCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#mlArg}.
 * @param ctx the parse tree
 */
fn enter_mlArg(&mut self, _ctx: &MlArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#mlArg}.
 * @param ctx the parse tree
 */
fn exit_mlArg(&mut self, _ctx: &MlArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#addtotalsCommand}.
 * @param ctx the parse tree
 */
fn enter_addtotalsCommand(&mut self, _ctx: &AddtotalsCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#addtotalsCommand}.
 * @param ctx the parse tree
 */
fn exit_addtotalsCommand(&mut self, _ctx: &AddtotalsCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#addtotalsOption}.
 * @param ctx the parse tree
 */
fn enter_addtotalsOption(&mut self, _ctx: &AddtotalsOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#addtotalsOption}.
 * @param ctx the parse tree
 */
fn exit_addtotalsOption(&mut self, _ctx: &AddtotalsOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#addcoltotalsCommand}.
 * @param ctx the parse tree
 */
fn enter_addcoltotalsCommand(&mut self, _ctx: &AddcoltotalsCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#addcoltotalsCommand}.
 * @param ctx the parse tree
 */
fn exit_addcoltotalsCommand(&mut self, _ctx: &AddcoltotalsCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#addcoltotalsOption}.
 * @param ctx the parse tree
 */
fn enter_addcoltotalsOption(&mut self, _ctx: &AddcoltotalsOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#addcoltotalsOption}.
 * @param ctx the parse tree
 */
fn exit_addcoltotalsOption(&mut self, _ctx: &AddcoltotalsOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#graphLookupCommand}.
 * @param ctx the parse tree
 */
fn enter_graphLookupCommand(&mut self, _ctx: &GraphLookupCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#graphLookupCommand}.
 * @param ctx the parse tree
 */
fn exit_graphLookupCommand(&mut self, _ctx: &GraphLookupCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#startClause}.
 * @param ctx the parse tree
 */
fn enter_startClause(&mut self, _ctx: &StartClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#startClause}.
 * @param ctx the parse tree
 */
fn exit_startClause(&mut self, _ctx: &StartClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#edgeClause}.
 * @param ctx the parse tree
 */
fn enter_edgeClause(&mut self, _ctx: &EdgeClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#edgeClause}.
 * @param ctx the parse tree
 */
fn exit_edgeClause(&mut self, _ctx: &EdgeClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#graphLookupArgs}.
 * @param ctx the parse tree
 */
fn enter_graphLookupArgs(&mut self, _ctx: &GraphLookupArgsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#graphLookupArgs}.
 * @param ctx the parse tree
 */
fn exit_graphLookupArgs(&mut self, _ctx: &GraphLookupArgsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#xyseriesCommand}.
 * @param ctx the parse tree
 */
fn enter_xyseriesCommand(&mut self, _ctx: &XyseriesCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#xyseriesCommand}.
 * @param ctx the parse tree
 */
fn exit_xyseriesCommand(&mut self, _ctx: &XyseriesCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#xyseriesOption}.
 * @param ctx the parse tree
 */
fn enter_xyseriesOption(&mut self, _ctx: &XyseriesOptionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#xyseriesOption}.
 * @param ctx the parse tree
 */
fn exit_xyseriesOption(&mut self, _ctx: &XyseriesOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#xyseriesPivotValues}.
 * @param ctx the parse tree
 */
fn enter_xyseriesPivotValues(&mut self, _ctx: &XyseriesPivotValuesContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#xyseriesPivotValues}.
 * @param ctx the parse tree
 */
fn exit_xyseriesPivotValues(&mut self, _ctx: &XyseriesPivotValuesContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#fromClause}.
 * @param ctx the parse tree
 */
fn enter_fromClause(&mut self, _ctx: &FromClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#fromClause}.
 * @param ctx the parse tree
 */
fn exit_fromClause(&mut self, _ctx: &FromClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#tableOrSubqueryClause}.
 * @param ctx the parse tree
 */
fn enter_tableOrSubqueryClause(&mut self, _ctx: &TableOrSubqueryClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#tableOrSubqueryClause}.
 * @param ctx the parse tree
 */
fn exit_tableOrSubqueryClause(&mut self, _ctx: &TableOrSubqueryClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#tableSourceClause}.
 * @param ctx the parse tree
 */
fn enter_tableSourceClause(&mut self, _ctx: &TableSourceClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#tableSourceClause}.
 * @param ctx the parse tree
 */
fn exit_tableSourceClause(&mut self, _ctx: &TableSourceClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#dynamicSourceClause}.
 * @param ctx the parse tree
 */
fn enter_dynamicSourceClause(&mut self, _ctx: &DynamicSourceClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#dynamicSourceClause}.
 * @param ctx the parse tree
 */
fn exit_dynamicSourceClause(&mut self, _ctx: &DynamicSourceClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#sourceReference}.
 * @param ctx the parse tree
 */
fn enter_sourceReference(&mut self, _ctx: &SourceReferenceContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#sourceReference}.
 * @param ctx the parse tree
 */
fn exit_sourceReference(&mut self, _ctx: &SourceReferenceContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#sourceFilterArg}.
 * @param ctx the parse tree
 */
fn enter_sourceFilterArg(&mut self, _ctx: &SourceFilterArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#sourceFilterArg}.
 * @param ctx the parse tree
 */
fn exit_sourceFilterArg(&mut self, _ctx: &SourceFilterArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#joinCommand}.
 * @param ctx the parse tree
 */
fn enter_joinCommand(&mut self, _ctx: &JoinCommandContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#joinCommand}.
 * @param ctx the parse tree
 */
fn exit_joinCommand(&mut self, _ctx: &JoinCommandContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#sqlLikeJoinType}.
 * @param ctx the parse tree
 */
fn enter_sqlLikeJoinType(&mut self, _ctx: &SqlLikeJoinTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#sqlLikeJoinType}.
 * @param ctx the parse tree
 */
fn exit_sqlLikeJoinType(&mut self, _ctx: &SqlLikeJoinTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#joinType}.
 * @param ctx the parse tree
 */
fn enter_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#joinType}.
 * @param ctx the parse tree
 */
fn exit_joinType(&mut self, _ctx: &JoinTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#sideAlias}.
 * @param ctx the parse tree
 */
fn enter_sideAlias(&mut self, _ctx: &SideAliasContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#sideAlias}.
 * @param ctx the parse tree
 */
fn exit_sideAlias(&mut self, _ctx: &SideAliasContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn enter_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#joinCriteria}.
 * @param ctx the parse tree
 */
fn exit_joinCriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#joinHintList}.
 * @param ctx the parse tree
 */
fn enter_joinHintList(&mut self, _ctx: &JoinHintListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#joinHintList}.
 * @param ctx the parse tree
 */
fn exit_joinHintList(&mut self, _ctx: &JoinHintListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code leftHint}
 * labeled alternative in {@link OpenSearchPPLParser#hintPair}.
 * @param ctx the parse tree
 */
fn enter_leftHint(&mut self, _ctx: &LeftHintContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code leftHint}
 * labeled alternative in {@link OpenSearchPPLParser#hintPair}.
 * @param ctx the parse tree
 */
fn exit_leftHint(&mut self, _ctx: &LeftHintContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rightHint}
 * labeled alternative in {@link OpenSearchPPLParser#hintPair}.
 * @param ctx the parse tree
 */
fn enter_rightHint(&mut self, _ctx: &RightHintContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rightHint}
 * labeled alternative in {@link OpenSearchPPLParser#hintPair}.
 * @param ctx the parse tree
 */
fn exit_rightHint(&mut self, _ctx: &RightHintContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code overwriteOption}
 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
 * @param ctx the parse tree
 */
fn enter_overwriteOption(&mut self, _ctx: &OverwriteOptionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code overwriteOption}
 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
 * @param ctx the parse tree
 */
fn exit_overwriteOption(&mut self, _ctx: &OverwriteOptionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code typeOption}
 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
 * @param ctx the parse tree
 */
fn enter_typeOption(&mut self, _ctx: &TypeOptionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code typeOption}
 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
 * @param ctx the parse tree
 */
fn exit_typeOption(&mut self, _ctx: &TypeOptionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code maxOption}
 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
 * @param ctx the parse tree
 */
fn enter_maxOption(&mut self, _ctx: &MaxOptionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code maxOption}
 * labeled alternative in {@link OpenSearchPPLParser#joinOption}.
 * @param ctx the parse tree
 */
fn exit_maxOption(&mut self, _ctx: &MaxOptionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#renameClause}.
 * @param ctx the parse tree
 */
fn enter_renameClause(&mut self, _ctx: &RenameClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#renameClause}.
 * @param ctx the parse tree
 */
fn exit_renameClause(&mut self, _ctx: &RenameClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#byClause}.
 * @param ctx the parse tree
 */
fn enter_byClause(&mut self, _ctx: &ByClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#byClause}.
 * @param ctx the parse tree
 */
fn exit_byClause(&mut self, _ctx: &ByClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#statsByClause}.
 * @param ctx the parse tree
 */
fn enter_statsByClause(&mut self, _ctx: &StatsByClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#statsByClause}.
 * @param ctx the parse tree
 */
fn exit_statsByClause(&mut self, _ctx: &StatsByClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#bySpanClause}.
 * @param ctx the parse tree
 */
fn enter_bySpanClause(&mut self, _ctx: &BySpanClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#bySpanClause}.
 * @param ctx the parse tree
 */
fn exit_bySpanClause(&mut self, _ctx: &BySpanClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#spanClause}.
 * @param ctx the parse tree
 */
fn enter_spanClause(&mut self, _ctx: &SpanClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#spanClause}.
 * @param ctx the parse tree
 */
fn exit_spanClause(&mut self, _ctx: &SpanClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#sortbyClause}.
 * @param ctx the parse tree
 */
fn enter_sortbyClause(&mut self, _ctx: &SortbyClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#sortbyClause}.
 * @param ctx the parse tree
 */
fn exit_sortbyClause(&mut self, _ctx: &SortbyClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#evalClause}.
 * @param ctx the parse tree
 */
fn enter_evalClause(&mut self, _ctx: &EvalClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#evalClause}.
 * @param ctx the parse tree
 */
fn exit_evalClause(&mut self, _ctx: &EvalClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#fieldFormatEvalClause}.
 * @param ctx the parse tree
 */
fn enter_fieldFormatEvalClause(&mut self, _ctx: &FieldFormatEvalClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#fieldFormatEvalClause}.
 * @param ctx the parse tree
 */
fn exit_fieldFormatEvalClause(&mut self, _ctx: &FieldFormatEvalClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#eventstatsAggTerm}.
 * @param ctx the parse tree
 */
fn enter_eventstatsAggTerm(&mut self, _ctx: &EventstatsAggTermContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#eventstatsAggTerm}.
 * @param ctx the parse tree
 */
fn exit_eventstatsAggTerm(&mut self, _ctx: &EventstatsAggTermContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#streamstatsAggTerm}.
 * @param ctx the parse tree
 */
fn enter_streamstatsAggTerm(&mut self, _ctx: &StreamstatsAggTermContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#streamstatsAggTerm}.
 * @param ctx the parse tree
 */
fn exit_streamstatsAggTerm(&mut self, _ctx: &StreamstatsAggTermContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#windowFunction}.
 * @param ctx the parse tree
 */
fn enter_windowFunction(&mut self, _ctx: &WindowFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#windowFunction}.
 * @param ctx the parse tree
 */
fn exit_windowFunction(&mut self, _ctx: &WindowFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#windowFunctionName}.
 * @param ctx the parse tree
 */
fn enter_windowFunctionName(&mut self, _ctx: &WindowFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#windowFunctionName}.
 * @param ctx the parse tree
 */
fn exit_windowFunctionName(&mut self, _ctx: &WindowFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#scalarWindowFunctionName}.
 * @param ctx the parse tree
 */
fn enter_scalarWindowFunctionName(&mut self, _ctx: &ScalarWindowFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#scalarWindowFunctionName}.
 * @param ctx the parse tree
 */
fn exit_scalarWindowFunctionName(&mut self, _ctx: &ScalarWindowFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#statsAggTerm}.
 * @param ctx the parse tree
 */
fn enter_statsAggTerm(&mut self, _ctx: &StatsAggTermContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#statsAggTerm}.
 * @param ctx the parse tree
 */
fn exit_statsAggTerm(&mut self, _ctx: &StatsAggTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code countEvalFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn enter_countEvalFunctionCall(&mut self, _ctx: &CountEvalFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code countEvalFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn exit_countEvalFunctionCall(&mut self, _ctx: &CountEvalFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code countAllFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn enter_countAllFunctionCall(&mut self, _ctx: &CountAllFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code countAllFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn exit_countAllFunctionCall(&mut self, _ctx: &CountAllFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code percentileShortcutFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn enter_percentileShortcutFunctionCall(&mut self, _ctx: &PercentileShortcutFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code percentileShortcutFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn exit_percentileShortcutFunctionCall(&mut self, _ctx: &PercentileShortcutFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code distinctCountFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn enter_distinctCountFunctionCall(&mut self, _ctx: &DistinctCountFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code distinctCountFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn exit_distinctCountFunctionCall(&mut self, _ctx: &DistinctCountFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code takeAggFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn enter_takeAggFunctionCall(&mut self, _ctx: &TakeAggFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code takeAggFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn exit_takeAggFunctionCall(&mut self, _ctx: &TakeAggFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code valuesAggFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn enter_valuesAggFunctionCall(&mut self, _ctx: &ValuesAggFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code valuesAggFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn exit_valuesAggFunctionCall(&mut self, _ctx: &ValuesAggFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code percentileApproxFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn enter_percentileApproxFunctionCall(&mut self, _ctx: &PercentileApproxFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code percentileApproxFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn exit_percentileApproxFunctionCall(&mut self, _ctx: &PercentileApproxFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code perFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn enter_perFunctionCall(&mut self, _ctx: &PerFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code perFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn exit_perFunctionCall(&mut self, _ctx: &PerFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code statsFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn enter_statsFunctionCall(&mut self, _ctx: &StatsFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code statsFunctionCall}
 * labeled alternative in {@link OpenSearchPPLParser#statsFunction}.
 * @param ctx the parse tree
 */
fn exit_statsFunctionCall(&mut self, _ctx: &StatsFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#statsFunctionName}.
 * @param ctx the parse tree
 */
fn enter_statsFunctionName(&mut self, _ctx: &StatsFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#statsFunctionName}.
 * @param ctx the parse tree
 */
fn exit_statsFunctionName(&mut self, _ctx: &StatsFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#takeAggFunction}.
 * @param ctx the parse tree
 */
fn enter_takeAggFunction(&mut self, _ctx: &TakeAggFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#takeAggFunction}.
 * @param ctx the parse tree
 */
fn exit_takeAggFunction(&mut self, _ctx: &TakeAggFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#valuesAggFunction}.
 * @param ctx the parse tree
 */
fn enter_valuesAggFunction(&mut self, _ctx: &ValuesAggFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#valuesAggFunction}.
 * @param ctx the parse tree
 */
fn exit_valuesAggFunction(&mut self, _ctx: &ValuesAggFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#percentileApproxFunction}.
 * @param ctx the parse tree
 */
fn enter_percentileApproxFunction(&mut self, _ctx: &PercentileApproxFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#percentileApproxFunction}.
 * @param ctx the parse tree
 */
fn exit_percentileApproxFunction(&mut self, _ctx: &PercentileApproxFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#perFunction}.
 * @param ctx the parse tree
 */
fn enter_perFunction(&mut self, _ctx: &PerFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#perFunction}.
 * @param ctx the parse tree
 */
fn exit_perFunction(&mut self, _ctx: &PerFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#numericLiteral}.
 * @param ctx the parse tree
 */
fn enter_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#numericLiteral}.
 * @param ctx the parse tree
 */
fn exit_numericLiteral(&mut self, _ctx: &NumericLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code stringDotlogicalExpression}
 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
 * @param ctx the parse tree
 */
fn enter_stringDotlogicalExpression(&mut self, _ctx: &StringDotlogicalExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code stringDotlogicalExpression}
 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
 * @param ctx the parse tree
 */
fn exit_stringDotlogicalExpression(&mut self, _ctx: &StringDotlogicalExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code stringDotlogicalExpressionDotString}
 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
 * @param ctx the parse tree
 */
fn enter_stringDotlogicalExpressionDotString(&mut self, _ctx: &StringDotlogicalExpressionDotStringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code stringDotlogicalExpressionDotString}
 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
 * @param ctx the parse tree
 */
fn exit_stringDotlogicalExpressionDotString(&mut self, _ctx: &StringDotlogicalExpressionDotStringContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalExpressionDotString}
 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalExpressionDotString(&mut self, _ctx: &LogicalExpressionDotStringContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalExpressionDotString}
 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalExpressionDotString(&mut self, _ctx: &LogicalExpressionDotStringContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code ffStandardLogicalExpression}
 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
 * @param ctx the parse tree
 */
fn enter_ffStandardLogicalExpression(&mut self, _ctx: &FfStandardLogicalExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code ffStandardLogicalExpression}
 * labeled alternative in {@link OpenSearchPPLParser#ffLogicalExpression}.
 * @param ctx the parse tree
 */
fn exit_ffStandardLogicalExpression(&mut self, _ctx: &FfStandardLogicalExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalNot}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalNot(&mut self, _ctx: &LogicalNotContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalExpr}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalExpr(&mut self, _ctx: &LogicalExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalExpr}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalExpr(&mut self, _ctx: &LogicalExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalAnd}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalAnd(&mut self, _ctx: &LogicalAndContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalAnd}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalAnd(&mut self, _ctx: &LogicalAndContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalXor}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalXor(&mut self, _ctx: &LogicalXorContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalXor}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalXor(&mut self, _ctx: &LogicalXorContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code logicalOr}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn enter_logicalOr(&mut self, _ctx: &LogicalOrContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code logicalOr}
 * labeled alternative in {@link OpenSearchPPLParser#logicalExpression}.
 * @param ctx the parse tree
 */
fn exit_logicalOr(&mut self, _ctx: &LogicalOrContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code relevanceExpr}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn enter_relevanceExpr(&mut self, _ctx: &RelevanceExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code relevanceExpr}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn exit_relevanceExpr(&mut self, _ctx: &RelevanceExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code valueExpr}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn enter_valueExpr(&mut self, _ctx: &ValueExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code valueExpr}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn exit_valueExpr(&mut self, _ctx: &ValueExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inExpr}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn enter_inExpr(&mut self, _ctx: &InExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inExpr}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn exit_inExpr(&mut self, _ctx: &InExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code isNullPredicate}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn enter_isNullPredicate(&mut self, _ctx: &IsNullPredicateContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code isNullPredicate}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn exit_isNullPredicate(&mut self, _ctx: &IsNullPredicateContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code between}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn enter_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code between}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn exit_between(&mut self, _ctx: &BetweenContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code compareExpr}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn enter_compareExpr(&mut self, _ctx: &CompareExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code compareExpr}
 * labeled alternative in {@link OpenSearchPPLParser#expression}.
 * @param ctx the parse tree
 */
fn exit_compareExpr(&mut self, _ctx: &CompareExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#nullNotnull}.
 * @param ctx the parse tree
 */
fn enter_nullNotnull(&mut self, _ctx: &NullNotnullContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#nullNotnull}.
 * @param ctx the parse tree
 */
fn exit_nullNotnull(&mut self, _ctx: &NullNotnullContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code inSubqueryExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_inSubqueryExpr(&mut self, _ctx: &InSubqueryExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code inSubqueryExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_inSubqueryExpr(&mut self, _ctx: &InSubqueryExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code lambdaExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_lambdaExpr(&mut self, _ctx: &LambdaExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code lambdaExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_lambdaExpr(&mut self, _ctx: &LambdaExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code literalValueExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_literalValueExpr(&mut self, _ctx: &LiteralValueExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code literalValueExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_literalValueExpr(&mut self, _ctx: &LiteralValueExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code functionCallExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_functionCallExpr(&mut self, _ctx: &FunctionCallExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code functionCallExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_functionCallExpr(&mut self, _ctx: &FunctionCallExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code existsSubqueryExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_existsSubqueryExpr(&mut self, _ctx: &ExistsSubqueryExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code existsSubqueryExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_existsSubqueryExpr(&mut self, _ctx: &ExistsSubqueryExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code foreachPlaceholderExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_foreachPlaceholderExpr(&mut self, _ctx: &ForeachPlaceholderExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code foreachPlaceholderExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_foreachPlaceholderExpr(&mut self, _ctx: &ForeachPlaceholderExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code scalarSubqueryExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_scalarSubqueryExpr(&mut self, _ctx: &ScalarSubqueryExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code scalarSubqueryExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_scalarSubqueryExpr(&mut self, _ctx: &ScalarSubqueryExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code nestedValueExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_nestedValueExpr(&mut self, _ctx: &NestedValueExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code nestedValueExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_nestedValueExpr(&mut self, _ctx: &NestedValueExprContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code binaryArithmetic}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_binaryArithmetic(&mut self, _ctx: &BinaryArithmeticContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code binaryArithmetic}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_binaryArithmetic(&mut self, _ctx: &BinaryArithmeticContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code fieldExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn enter_fieldExpr(&mut self, _ctx: &FieldExprContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code fieldExpr}
 * labeled alternative in {@link OpenSearchPPLParser#valueExpression}.
 * @param ctx the parse tree
 */
fn exit_fieldExpr(&mut self, _ctx: &FieldExprContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#evalExpression}.
 * @param ctx the parse tree
 */
fn enter_evalExpression(&mut self, _ctx: &EvalExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#evalExpression}.
 * @param ctx the parse tree
 */
fn exit_evalExpression(&mut self, _ctx: &EvalExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#functionCall}.
 * @param ctx the parse tree
 */
fn enter_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#functionCall}.
 * @param ctx the parse tree
 */
fn exit_functionCall(&mut self, _ctx: &FunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#mvmapFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_mvmapFunctionCall(&mut self, _ctx: &MvmapFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#mvmapFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_mvmapFunctionCall(&mut self, _ctx: &MvmapFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#positionFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_positionFunctionCall(&mut self, _ctx: &PositionFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#positionFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_positionFunctionCall(&mut self, _ctx: &PositionFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#caseFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_caseFunctionCall(&mut self, _ctx: &CaseFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#caseFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_caseFunctionCall(&mut self, _ctx: &CaseFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#relevanceExpression}.
 * @param ctx the parse tree
 */
fn enter_relevanceExpression(&mut self, _ctx: &RelevanceExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#relevanceExpression}.
 * @param ctx the parse tree
 */
fn exit_relevanceExpression(&mut self, _ctx: &RelevanceExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#singleFieldRelevanceFunction}.
 * @param ctx the parse tree
 */
fn enter_singleFieldRelevanceFunction(&mut self, _ctx: &SingleFieldRelevanceFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#singleFieldRelevanceFunction}.
 * @param ctx the parse tree
 */
fn exit_singleFieldRelevanceFunction(&mut self, _ctx: &SingleFieldRelevanceFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#multiFieldRelevanceFunction}.
 * @param ctx the parse tree
 */
fn enter_multiFieldRelevanceFunction(&mut self, _ctx: &MultiFieldRelevanceFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#multiFieldRelevanceFunction}.
 * @param ctx the parse tree
 */
fn exit_multiFieldRelevanceFunction(&mut self, _ctx: &MultiFieldRelevanceFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timeModifier}.
 * @param ctx the parse tree
 */
fn enter_timeModifier(&mut self, _ctx: &TimeModifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timeModifier}.
 * @param ctx the parse tree
 */
fn exit_timeModifier(&mut self, _ctx: &TimeModifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timeModifierValue}.
 * @param ctx the parse tree
 */
fn enter_timeModifierValue(&mut self, _ctx: &TimeModifierValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timeModifierValue}.
 * @param ctx the parse tree
 */
fn exit_timeModifierValue(&mut self, _ctx: &TimeModifierValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#tableSource}.
 * @param ctx the parse tree
 */
fn enter_tableSource(&mut self, _ctx: &TableSourceContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#tableSource}.
 * @param ctx the parse tree
 */
fn exit_tableSource(&mut self, _ctx: &TableSourceContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#tableFunction}.
 * @param ctx the parse tree
 */
fn enter_tableFunction(&mut self, _ctx: &TableFunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#tableFunction}.
 * @param ctx the parse tree
 */
fn exit_tableFunction(&mut self, _ctx: &TableFunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#fieldList}.
 * @param ctx the parse tree
 */
fn enter_fieldList(&mut self, _ctx: &FieldListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#fieldList}.
 * @param ctx the parse tree
 */
fn exit_fieldList(&mut self, _ctx: &FieldListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code invalidMixedSortField}
 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
 * @param ctx the parse tree
 */
fn enter_invalidMixedSortField(&mut self, _ctx: &InvalidMixedSortFieldContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code invalidMixedSortField}
 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
 * @param ctx the parse tree
 */
fn exit_invalidMixedSortField(&mut self, _ctx: &InvalidMixedSortFieldContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prefixSortField}
 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
 * @param ctx the parse tree
 */
fn enter_prefixSortField(&mut self, _ctx: &PrefixSortFieldContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prefixSortField}
 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
 * @param ctx the parse tree
 */
fn exit_prefixSortField(&mut self, _ctx: &PrefixSortFieldContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code suffixSortField}
 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
 * @param ctx the parse tree
 */
fn enter_suffixSortField(&mut self, _ctx: &SuffixSortFieldContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code suffixSortField}
 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
 * @param ctx the parse tree
 */
fn exit_suffixSortField(&mut self, _ctx: &SuffixSortFieldContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code defaultSortField}
 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
 * @param ctx the parse tree
 */
fn enter_defaultSortField(&mut self, _ctx: &DefaultSortFieldContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code defaultSortField}
 * labeled alternative in {@link OpenSearchPPLParser#sortField}.
 * @param ctx the parse tree
 */
fn exit_defaultSortField(&mut self, _ctx: &DefaultSortFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#sortFieldExpression}.
 * @param ctx the parse tree
 */
fn enter_sortFieldExpression(&mut self, _ctx: &SortFieldExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#sortFieldExpression}.
 * @param ctx the parse tree
 */
fn exit_sortFieldExpression(&mut self, _ctx: &SortFieldExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#fieldExpression}.
 * @param ctx the parse tree
 */
fn enter_fieldExpression(&mut self, _ctx: &FieldExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#fieldExpression}.
 * @param ctx the parse tree
 */
fn exit_fieldExpression(&mut self, _ctx: &FieldExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#wcFieldExpression}.
 * @param ctx the parse tree
 */
fn enter_wcFieldExpression(&mut self, _ctx: &WcFieldExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#wcFieldExpression}.
 * @param ctx the parse tree
 */
fn exit_wcFieldExpression(&mut self, _ctx: &WcFieldExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#foreachPlaceholder}.
 * @param ctx the parse tree
 */
fn enter_foreachPlaceholder(&mut self, _ctx: &ForeachPlaceholderContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#foreachPlaceholder}.
 * @param ctx the parse tree
 */
fn exit_foreachPlaceholder(&mut self, _ctx: &ForeachPlaceholderContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#foreachPlaceholderName}.
 * @param ctx the parse tree
 */
fn enter_foreachPlaceholderName(&mut self, _ctx: &ForeachPlaceholderNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#foreachPlaceholderName}.
 * @param ctx the parse tree
 */
fn exit_foreachPlaceholderName(&mut self, _ctx: &ForeachPlaceholderNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#selectFieldExpression}.
 * @param ctx the parse tree
 */
fn enter_selectFieldExpression(&mut self, _ctx: &SelectFieldExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#selectFieldExpression}.
 * @param ctx the parse tree
 */
fn exit_selectFieldExpression(&mut self, _ctx: &SelectFieldExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#renameFieldExpression}.
 * @param ctx the parse tree
 */
fn enter_renameFieldExpression(&mut self, _ctx: &RenameFieldExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#renameFieldExpression}.
 * @param ctx the parse tree
 */
fn exit_renameFieldExpression(&mut self, _ctx: &RenameFieldExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#evalFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_evalFunctionCall(&mut self, _ctx: &EvalFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#evalFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_evalFunctionCall(&mut self, _ctx: &EvalFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#dataTypeFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_dataTypeFunctionCall(&mut self, _ctx: &DataTypeFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#dataTypeFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_dataTypeFunctionCall(&mut self, _ctx: &DataTypeFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#convertedDataType}.
 * @param ctx the parse tree
 */
fn enter_convertedDataType(&mut self, _ctx: &ConvertedDataTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#convertedDataType}.
 * @param ctx the parse tree
 */
fn exit_convertedDataType(&mut self, _ctx: &ConvertedDataTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#evalFunctionName}.
 * @param ctx the parse tree
 */
fn enter_evalFunctionName(&mut self, _ctx: &EvalFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#evalFunctionName}.
 * @param ctx the parse tree
 */
fn exit_evalFunctionName(&mut self, _ctx: &EvalFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#functionArgs}.
 * @param ctx the parse tree
 */
fn enter_functionArgs(&mut self, _ctx: &FunctionArgsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#functionArgs}.
 * @param ctx the parse tree
 */
fn exit_functionArgs(&mut self, _ctx: &FunctionArgsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#namedFunctionArgs}.
 * @param ctx the parse tree
 */
fn enter_namedFunctionArgs(&mut self, _ctx: &NamedFunctionArgsContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#namedFunctionArgs}.
 * @param ctx the parse tree
 */
fn exit_namedFunctionArgs(&mut self, _ctx: &NamedFunctionArgsContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#functionArg}.
 * @param ctx the parse tree
 */
fn enter_functionArg(&mut self, _ctx: &FunctionArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#functionArg}.
 * @param ctx the parse tree
 */
fn exit_functionArg(&mut self, _ctx: &FunctionArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#namedFunctionArg}.
 * @param ctx the parse tree
 */
fn enter_namedFunctionArg(&mut self, _ctx: &NamedFunctionArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#namedFunctionArg}.
 * @param ctx the parse tree
 */
fn exit_namedFunctionArg(&mut self, _ctx: &NamedFunctionArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#functionArgExpression}.
 * @param ctx the parse tree
 */
fn enter_functionArgExpression(&mut self, _ctx: &FunctionArgExpressionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#functionArgExpression}.
 * @param ctx the parse tree
 */
fn exit_functionArgExpression(&mut self, _ctx: &FunctionArgExpressionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#lambda}.
 * @param ctx the parse tree
 */
fn enter_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#lambda}.
 * @param ctx the parse tree
 */
fn exit_lambda(&mut self, _ctx: &LambdaContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#relevanceArg}.
 * @param ctx the parse tree
 */
fn enter_relevanceArg(&mut self, _ctx: &RelevanceArgContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#relevanceArg}.
 * @param ctx the parse tree
 */
fn exit_relevanceArg(&mut self, _ctx: &RelevanceArgContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#relevanceArgName}.
 * @param ctx the parse tree
 */
fn enter_relevanceArgName(&mut self, _ctx: &RelevanceArgNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#relevanceArgName}.
 * @param ctx the parse tree
 */
fn exit_relevanceArgName(&mut self, _ctx: &RelevanceArgNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#relevanceFieldAndWeight}.
 * @param ctx the parse tree
 */
fn enter_relevanceFieldAndWeight(&mut self, _ctx: &RelevanceFieldAndWeightContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#relevanceFieldAndWeight}.
 * @param ctx the parse tree
 */
fn exit_relevanceFieldAndWeight(&mut self, _ctx: &RelevanceFieldAndWeightContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#relevanceFieldWeight}.
 * @param ctx the parse tree
 */
fn enter_relevanceFieldWeight(&mut self, _ctx: &RelevanceFieldWeightContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#relevanceFieldWeight}.
 * @param ctx the parse tree
 */
fn exit_relevanceFieldWeight(&mut self, _ctx: &RelevanceFieldWeightContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#relevanceField}.
 * @param ctx the parse tree
 */
fn enter_relevanceField(&mut self, _ctx: &RelevanceFieldContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#relevanceField}.
 * @param ctx the parse tree
 */
fn exit_relevanceField(&mut self, _ctx: &RelevanceFieldContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#relevanceQuery}.
 * @param ctx the parse tree
 */
fn enter_relevanceQuery(&mut self, _ctx: &RelevanceQueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#relevanceQuery}.
 * @param ctx the parse tree
 */
fn exit_relevanceQuery(&mut self, _ctx: &RelevanceQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#relevanceArgValue}.
 * @param ctx the parse tree
 */
fn enter_relevanceArgValue(&mut self, _ctx: &RelevanceArgValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#relevanceArgValue}.
 * @param ctx the parse tree
 */
fn exit_relevanceArgValue(&mut self, _ctx: &RelevanceArgValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#mathematicalFunctionName}.
 * @param ctx the parse tree
 */
fn enter_mathematicalFunctionName(&mut self, _ctx: &MathematicalFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#mathematicalFunctionName}.
 * @param ctx the parse tree
 */
fn exit_mathematicalFunctionName(&mut self, _ctx: &MathematicalFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#geoipFunctionName}.
 * @param ctx the parse tree
 */
fn enter_geoipFunctionName(&mut self, _ctx: &GeoipFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#geoipFunctionName}.
 * @param ctx the parse tree
 */
fn exit_geoipFunctionName(&mut self, _ctx: &GeoipFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#collectionFunctionName}.
 * @param ctx the parse tree
 */
fn enter_collectionFunctionName(&mut self, _ctx: &CollectionFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#collectionFunctionName}.
 * @param ctx the parse tree
 */
fn exit_collectionFunctionName(&mut self, _ctx: &CollectionFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#trigonometricFunctionName}.
 * @param ctx the parse tree
 */
fn enter_trigonometricFunctionName(&mut self, _ctx: &TrigonometricFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#trigonometricFunctionName}.
 * @param ctx the parse tree
 */
fn exit_trigonometricFunctionName(&mut self, _ctx: &TrigonometricFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#jsonFunctionName}.
 * @param ctx the parse tree
 */
fn enter_jsonFunctionName(&mut self, _ctx: &JsonFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#jsonFunctionName}.
 * @param ctx the parse tree
 */
fn exit_jsonFunctionName(&mut self, _ctx: &JsonFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#cryptographicFunctionName}.
 * @param ctx the parse tree
 */
fn enter_cryptographicFunctionName(&mut self, _ctx: &CryptographicFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#cryptographicFunctionName}.
 * @param ctx the parse tree
 */
fn exit_cryptographicFunctionName(&mut self, _ctx: &CryptographicFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#dateTimeFunctionName}.
 * @param ctx the parse tree
 */
fn enter_dateTimeFunctionName(&mut self, _ctx: &DateTimeFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#dateTimeFunctionName}.
 * @param ctx the parse tree
 */
fn exit_dateTimeFunctionName(&mut self, _ctx: &DateTimeFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#getFormatFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_getFormatFunctionCall(&mut self, _ctx: &GetFormatFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#getFormatFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_getFormatFunctionCall(&mut self, _ctx: &GetFormatFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#getFormatType}.
 * @param ctx the parse tree
 */
fn enter_getFormatType(&mut self, _ctx: &GetFormatTypeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#getFormatType}.
 * @param ctx the parse tree
 */
fn exit_getFormatType(&mut self, _ctx: &GetFormatTypeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#extractFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_extractFunctionCall(&mut self, _ctx: &ExtractFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#extractFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_extractFunctionCall(&mut self, _ctx: &ExtractFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#simpleDateTimePart}.
 * @param ctx the parse tree
 */
fn enter_simpleDateTimePart(&mut self, _ctx: &SimpleDateTimePartContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#simpleDateTimePart}.
 * @param ctx the parse tree
 */
fn exit_simpleDateTimePart(&mut self, _ctx: &SimpleDateTimePartContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#complexDateTimePart}.
 * @param ctx the parse tree
 */
fn enter_complexDateTimePart(&mut self, _ctx: &ComplexDateTimePartContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#complexDateTimePart}.
 * @param ctx the parse tree
 */
fn exit_complexDateTimePart(&mut self, _ctx: &ComplexDateTimePartContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#datetimePart}.
 * @param ctx the parse tree
 */
fn enter_datetimePart(&mut self, _ctx: &DatetimePartContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#datetimePart}.
 * @param ctx the parse tree
 */
fn exit_datetimePart(&mut self, _ctx: &DatetimePartContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timestampFunctionCall}.
 * @param ctx the parse tree
 */
fn enter_timestampFunctionCall(&mut self, _ctx: &TimestampFunctionCallContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timestampFunctionCall}.
 * @param ctx the parse tree
 */
fn exit_timestampFunctionCall(&mut self, _ctx: &TimestampFunctionCallContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timestampFunctionName}.
 * @param ctx the parse tree
 */
fn enter_timestampFunctionName(&mut self, _ctx: &TimestampFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timestampFunctionName}.
 * @param ctx the parse tree
 */
fn exit_timestampFunctionName(&mut self, _ctx: &TimestampFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#conditionFunctionName}.
 * @param ctx the parse tree
 */
fn enter_conditionFunctionName(&mut self, _ctx: &ConditionFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#conditionFunctionName}.
 * @param ctx the parse tree
 */
fn exit_conditionFunctionName(&mut self, _ctx: &ConditionFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#flowControlFunctionName}.
 * @param ctx the parse tree
 */
fn enter_flowControlFunctionName(&mut self, _ctx: &FlowControlFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#flowControlFunctionName}.
 * @param ctx the parse tree
 */
fn exit_flowControlFunctionName(&mut self, _ctx: &FlowControlFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#systemFunctionName}.
 * @param ctx the parse tree
 */
fn enter_systemFunctionName(&mut self, _ctx: &SystemFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#systemFunctionName}.
 * @param ctx the parse tree
 */
fn exit_systemFunctionName(&mut self, _ctx: &SystemFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#textFunctionName}.
 * @param ctx the parse tree
 */
fn enter_textFunctionName(&mut self, _ctx: &TextFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#textFunctionName}.
 * @param ctx the parse tree
 */
fn exit_textFunctionName(&mut self, _ctx: &TextFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#positionFunctionName}.
 * @param ctx the parse tree
 */
fn enter_positionFunctionName(&mut self, _ctx: &PositionFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#positionFunctionName}.
 * @param ctx the parse tree
 */
fn exit_positionFunctionName(&mut self, _ctx: &PositionFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn enter_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#comparisonOperator}.
 * @param ctx the parse tree
 */
fn exit_comparisonOperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#singleFieldRelevanceFunctionName}.
 * @param ctx the parse tree
 */
fn enter_singleFieldRelevanceFunctionName(&mut self, _ctx: &SingleFieldRelevanceFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#singleFieldRelevanceFunctionName}.
 * @param ctx the parse tree
 */
fn exit_singleFieldRelevanceFunctionName(&mut self, _ctx: &SingleFieldRelevanceFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#multiFieldRelevanceFunctionName}.
 * @param ctx the parse tree
 */
fn enter_multiFieldRelevanceFunctionName(&mut self, _ctx: &MultiFieldRelevanceFunctionNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#multiFieldRelevanceFunctionName}.
 * @param ctx the parse tree
 */
fn exit_multiFieldRelevanceFunctionName(&mut self, _ctx: &MultiFieldRelevanceFunctionNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#literalValue}.
 * @param ctx the parse tree
 */
fn enter_literalValue(&mut self, _ctx: &LiteralValueContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#literalValue}.
 * @param ctx the parse tree
 */
fn exit_literalValue(&mut self, _ctx: &LiteralValueContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#intervalLiteral}.
 * @param ctx the parse tree
 */
fn enter_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#intervalLiteral}.
 * @param ctx the parse tree
 */
fn exit_intervalLiteral(&mut self, _ctx: &IntervalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#stringLiteral}.
 * @param ctx the parse tree
 */
fn enter_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#stringLiteral}.
 * @param ctx the parse tree
 */
fn exit_stringLiteral(&mut self, _ctx: &StringLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#integerLiteral}.
 * @param ctx the parse tree
 */
fn enter_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#integerLiteral}.
 * @param ctx the parse tree
 */
fn exit_integerLiteral(&mut self, _ctx: &IntegerLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#decimalLiteral}.
 * @param ctx the parse tree
 */
fn enter_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#decimalLiteral}.
 * @param ctx the parse tree
 */
fn exit_decimalLiteral(&mut self, _ctx: &DecimalLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#doubleLiteral}.
 * @param ctx the parse tree
 */
fn enter_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#doubleLiteral}.
 * @param ctx the parse tree
 */
fn exit_doubleLiteral(&mut self, _ctx: &DoubleLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#floatLiteral}.
 * @param ctx the parse tree
 */
fn enter_floatLiteral(&mut self, _ctx: &FloatLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#floatLiteral}.
 * @param ctx the parse tree
 */
fn exit_floatLiteral(&mut self, _ctx: &FloatLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#booleanLiteral}.
 * @param ctx the parse tree
 */
fn enter_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#booleanLiteral}.
 * @param ctx the parse tree
 */
fn exit_booleanLiteral(&mut self, _ctx: &BooleanLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#datetimeLiteral}.
 * @param ctx the parse tree
 */
fn enter_datetimeLiteral(&mut self, _ctx: &DatetimeLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#datetimeLiteral}.
 * @param ctx the parse tree
 */
fn exit_datetimeLiteral(&mut self, _ctx: &DatetimeLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#dateLiteral}.
 * @param ctx the parse tree
 */
fn enter_dateLiteral(&mut self, _ctx: &DateLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#dateLiteral}.
 * @param ctx the parse tree
 */
fn exit_dateLiteral(&mut self, _ctx: &DateLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timeLiteral}.
 * @param ctx the parse tree
 */
fn enter_timeLiteral(&mut self, _ctx: &TimeLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timeLiteral}.
 * @param ctx the parse tree
 */
fn exit_timeLiteral(&mut self, _ctx: &TimeLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#timestampLiteral}.
 * @param ctx the parse tree
 */
fn enter_timestampLiteral(&mut self, _ctx: &TimestampLiteralContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#timestampLiteral}.
 * @param ctx the parse tree
 */
fn exit_timestampLiteral(&mut self, _ctx: &TimestampLiteralContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#intervalUnit}.
 * @param ctx the parse tree
 */
fn enter_intervalUnit(&mut self, _ctx: &IntervalUnitContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#intervalUnit}.
 * @param ctx the parse tree
 */
fn exit_intervalUnit(&mut self, _ctx: &IntervalUnitContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#valueList}.
 * @param ctx the parse tree
 */
fn enter_valueList(&mut self, _ctx: &ValueListContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#valueList}.
 * @param ctx the parse tree
 */
fn exit_valueList(&mut self, _ctx: &ValueListContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identsAsQualifiedName}
 * labeled alternative in {@link OpenSearchPPLParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn enter_identsAsQualifiedName(&mut self, _ctx: &IdentsAsQualifiedNameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identsAsQualifiedName}
 * labeled alternative in {@link OpenSearchPPLParser#qualifiedName}.
 * @param ctx the parse tree
 */
fn exit_identsAsQualifiedName(&mut self, _ctx: &IdentsAsQualifiedNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identsAsTableQualifiedName}
 * labeled alternative in {@link OpenSearchPPLParser#tableQualifiedName}.
 * @param ctx the parse tree
 */
fn enter_identsAsTableQualifiedName(&mut self, _ctx: &IdentsAsTableQualifiedNameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identsAsTableQualifiedName}
 * labeled alternative in {@link OpenSearchPPLParser#tableQualifiedName}.
 * @param ctx the parse tree
 */
fn exit_identsAsTableQualifiedName(&mut self, _ctx: &IdentsAsTableQualifiedNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identsAsWildcardQualifiedName}
 * labeled alternative in {@link OpenSearchPPLParser#wcQualifiedName}.
 * @param ctx the parse tree
 */
fn enter_identsAsWildcardQualifiedName(&mut self, _ctx: &IdentsAsWildcardQualifiedNameContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identsAsWildcardQualifiedName}
 * labeled alternative in {@link OpenSearchPPLParser#wcQualifiedName}.
 * @param ctx the parse tree
 */
fn exit_identsAsWildcardQualifiedName(&mut self, _ctx: &IdentsAsWildcardQualifiedNameContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code identsAsQualifiedNameSeq}
 * labeled alternative in {@link OpenSearchPPLParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn enter_identsAsQualifiedNameSeq(&mut self, _ctx: &IdentsAsQualifiedNameSeqContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code identsAsQualifiedNameSeq}
 * labeled alternative in {@link OpenSearchPPLParser#identifierSeq}.
 * @param ctx the parse tree
 */
fn exit_identsAsQualifiedNameSeq(&mut self, _ctx: &IdentsAsQualifiedNameSeqContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#ident}.
 * @param ctx the parse tree
 */
fn enter_ident(&mut self, _ctx: &IdentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#ident}.
 * @param ctx the parse tree
 */
fn exit_ident(&mut self, _ctx: &IdentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#tableIdent}.
 * @param ctx the parse tree
 */
fn enter_tableIdent(&mut self, _ctx: &TableIdentContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#tableIdent}.
 * @param ctx the parse tree
 */
fn exit_tableIdent(&mut self, _ctx: &TableIdentContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#wildcard}.
 * @param ctx the parse tree
 */
fn enter_wildcard(&mut self, _ctx: &WildcardContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#wildcard}.
 * @param ctx the parse tree
 */
fn exit_wildcard(&mut self, _ctx: &WildcardContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#keywordsCanBeId}.
 * @param ctx the parse tree
 */
fn enter_keywordsCanBeId(&mut self, _ctx: &KeywordsCanBeIdContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#keywordsCanBeId}.
 * @param ctx the parse tree
 */
fn exit_keywordsCanBeId(&mut self, _ctx: &KeywordsCanBeIdContext<'input>) { }
/**
 * Enter a parse tree produced by {@link OpenSearchPPLParser#searchableKeyWord}.
 * @param ctx the parse tree
 */
fn enter_searchableKeyWord(&mut self, _ctx: &SearchableKeyWordContext<'input>) { }
/**
 * Exit a parse tree produced by {@link OpenSearchPPLParser#searchableKeyWord}.
 * @param ctx the parse tree
 */
fn exit_searchableKeyWord(&mut self, _ctx: &SearchableKeyWordContext<'input>) { }

}

antlr4rust::coerce_from!{ 'input : OpenSearchPPLParserListener<'input> }
