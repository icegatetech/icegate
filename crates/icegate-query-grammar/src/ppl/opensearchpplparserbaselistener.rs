#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]
// Generated from OpenSearchPPLParser.g4 by ANTLR 4.13.2

use super::opensearchpplparser::*;
use antlr4rust::tree::ParseTreeListener;

// A complete Visitor for a parse tree produced by OpenSearchPPLParser.

pub trait OpenSearchPPLParserBaseListener<'input>:
    ParseTreeListener<'input, OpenSearchPPLParserContextType> {

    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_root(&mut self, _ctx: &RootContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_root(&mut self, _ctx: &RootContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_pplstatement(&mut self, _ctx: &PplStatementContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_pplstatement(&mut self, _ctx: &PplStatementContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_subpipeline(&mut self, _ctx: &SubPipelineContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_subpipeline(&mut self, _ctx: &SubPipelineContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_querystatement(&mut self, _ctx: &QueryStatementContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_querystatement(&mut self, _ctx: &QueryStatementContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_explainstatement(&mut self, _ctx: &ExplainStatementContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_explainstatement(&mut self, _ctx: &ExplainStatementContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_explainmode(&mut self, _ctx: &ExplainModeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_explainmode(&mut self, _ctx: &ExplainModeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_subsearch(&mut self, _ctx: &SubSearchContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_subsearch(&mut self, _ctx: &SubSearchContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_pplcommands(&mut self, _ctx: &PplCommandsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_pplcommands(&mut self, _ctx: &PplCommandsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_commands(&mut self, _ctx: &CommandsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_commands(&mut self, _ctx: &CommandsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_commandname(&mut self, _ctx: &CommandNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_commandname(&mut self, _ctx: &CommandNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_searchfrom(&mut self, _ctx: &SearchFromContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_searchfrom(&mut self, _ctx: &SearchFromContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_orexpression(&mut self, _ctx: &OrExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_orexpression(&mut self, _ctx: &OrExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timemodifierexpression(&mut self, _ctx: &TimeModifierExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timemodifierexpression(&mut self, _ctx: &TimeModifierExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_andexpression(&mut self, _ctx: &AndExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_andexpression(&mut self, _ctx: &AndExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_notexpression(&mut self, _ctx: &NotExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_notexpression(&mut self, _ctx: &NotExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_termexpression(&mut self, _ctx: &TermExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_termexpression(&mut self, _ctx: &TermExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_groupedexpression(&mut self, _ctx: &GroupedExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_groupedexpression(&mut self, _ctx: &GroupedExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_searchcomparisonterm(&mut self, _ctx: &SearchComparisonTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_searchcomparisonterm(&mut self, _ctx: &SearchComparisonTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_searchinlistterm(&mut self, _ctx: &SearchInListTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_searchinlistterm(&mut self, _ctx: &SearchInListTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_searchliteralterm(&mut self, _ctx: &SearchLiteralTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_searchliteralterm(&mut self, _ctx: &SearchLiteralTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_searchliteral(&mut self, _ctx: &SearchLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_searchliteral(&mut self, _ctx: &SearchLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_searchfieldcompare(&mut self, _ctx: &SearchFieldCompareContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_searchfieldcompare(&mut self, _ctx: &SearchFieldCompareContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_searchfieldinvalues(&mut self, _ctx: &SearchFieldInValuesContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_searchfieldinvalues(&mut self, _ctx: &SearchFieldInValuesContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_searchliterals(&mut self, _ctx: &SearchLiteralsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_searchliterals(&mut self, _ctx: &SearchLiteralsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_equals(&mut self, _ctx: &EqualsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_equals(&mut self, _ctx: &EqualsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_notequals(&mut self, _ctx: &NotEqualsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_notequals(&mut self, _ctx: &NotEqualsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lessthan(&mut self, _ctx: &LessThanContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lessthan(&mut self, _ctx: &LessThanContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lessorequal(&mut self, _ctx: &LessOrEqualContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lessorequal(&mut self, _ctx: &LessOrEqualContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_greaterthan(&mut self, _ctx: &GreaterThanContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_greaterthan(&mut self, _ctx: &GreaterThanContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_greaterorequal(&mut self, _ctx: &GreaterOrEqualContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_greaterorequal(&mut self, _ctx: &GreaterOrEqualContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_describecommand(&mut self, _ctx: &DescribeCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_describecommand(&mut self, _ctx: &DescribeCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_restcommand(&mut self, _ctx: &RestCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_restcommand(&mut self, _ctx: &RestCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_restargument(&mut self, _ctx: &RestArgumentContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_restargument(&mut self, _ctx: &RestArgumentContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_showdatasourcescommand(&mut self, _ctx: &ShowDataSourcesCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_showdatasourcescommand(&mut self, _ctx: &ShowDataSourcesCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_makeresultscommand(&mut self, _ctx: &MakeresultsCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_makeresultscommand(&mut self, _ctx: &MakeresultsCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_makeresultsarg(&mut self, _ctx: &MakeresultsArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_makeresultsarg(&mut self, _ctx: &MakeresultsArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_wherecommand(&mut self, _ctx: &WhereCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_wherecommand(&mut self, _ctx: &WhereCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldscommand(&mut self, _ctx: &FieldsCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldscommand(&mut self, _ctx: &FieldsCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_tablecommand(&mut self, _ctx: &TableCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_tablecommand(&mut self, _ctx: &TableCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldscommandbody(&mut self, _ctx: &FieldsCommandBodyContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldscommandbody(&mut self, _ctx: &FieldsCommandBodyContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_wcfieldlist(&mut self, _ctx: &WcFieldListContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_wcfieldlist(&mut self, _ctx: &WcFieldListContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_renamecommand(&mut self, _ctx: &RenameCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_renamecommand(&mut self, _ctx: &RenameCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_replacecommand(&mut self, _ctx: &ReplaceCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_replacecommand(&mut self, _ctx: &ReplaceCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_replacepair(&mut self, _ctx: &ReplacePairContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_replacepair(&mut self, _ctx: &ReplacePairContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_statscommand(&mut self, _ctx: &StatsCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_statscommand(&mut self, _ctx: &StatsCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_appendpipecommand(&mut self, _ctx: &AppendPipeCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_appendpipecommand(&mut self, _ctx: &AppendPipeCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_statsargs(&mut self, _ctx: &StatsArgsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_statsargs(&mut self, _ctx: &StatsArgsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_partitionsarg(&mut self, _ctx: &PartitionsArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_partitionsarg(&mut self, _ctx: &PartitionsArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_allnumarg(&mut self, _ctx: &AllnumArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_allnumarg(&mut self, _ctx: &AllnumArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_delimarg(&mut self, _ctx: &DelimArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_delimarg(&mut self, _ctx: &DelimArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_bucketnullablearg(&mut self, _ctx: &BucketNullableArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_bucketnullablearg(&mut self, _ctx: &BucketNullableArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_dedupsplitarg(&mut self, _ctx: &DedupSplitArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_dedupsplitarg(&mut self, _ctx: &DedupSplitArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_eventstatscommand(&mut self, _ctx: &EventstatsCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_eventstatscommand(&mut self, _ctx: &EventstatsCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_streamstatscommand(&mut self, _ctx: &StreamstatsCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_streamstatscommand(&mut self, _ctx: &StreamstatsCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_streamstatsargs(&mut self, _ctx: &StreamstatsArgsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_streamstatsargs(&mut self, _ctx: &StreamstatsArgsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_currentarg(&mut self, _ctx: &CurrentArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_currentarg(&mut self, _ctx: &CurrentArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_windowarg(&mut self, _ctx: &WindowArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_windowarg(&mut self, _ctx: &WindowArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_globalarg(&mut self, _ctx: &GlobalArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_globalarg(&mut self, _ctx: &GlobalArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_resetbeforearg(&mut self, _ctx: &ResetBeforeArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_resetbeforearg(&mut self, _ctx: &ResetBeforeArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_resetafterarg(&mut self, _ctx: &ResetAfterArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_resetafterarg(&mut self, _ctx: &ResetAfterArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_dedupcommand(&mut self, _ctx: &DedupCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_dedupcommand(&mut self, _ctx: &DedupCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_sortcommand(&mut self, _ctx: &SortCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_sortcommand(&mut self, _ctx: &SortCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_reversecommand(&mut self, _ctx: &ReverseCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_reversecommand(&mut self, _ctx: &ReverseCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_chartcommand(&mut self, _ctx: &ChartCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_chartcommand(&mut self, _ctx: &ChartCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_chartoptions(&mut self, _ctx: &ChartOptionsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_chartoptions(&mut self, _ctx: &ChartOptionsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rowsplit(&mut self, _ctx: &RowSplitContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rowsplit(&mut self, _ctx: &RowSplitContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_columnsplit(&mut self, _ctx: &ColumnSplitContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_columnsplit(&mut self, _ctx: &ColumnSplitContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timechartcommand(&mut self, _ctx: &TimechartCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timechartcommand(&mut self, _ctx: &TimechartCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_transposecommand(&mut self, _ctx: &TransposeCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_transposecommand(&mut self, _ctx: &TransposeCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_transposeparameter(&mut self, _ctx: &TransposeParameterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_transposeparameter(&mut self, _ctx: &TransposeParameterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timewrapcommand(&mut self, _ctx: &TimewrapCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timewrapcommand(&mut self, _ctx: &TimewrapCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timewrapparameter(&mut self, _ctx: &TimewrapParameterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timewrapparameter(&mut self, _ctx: &TimewrapParameterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timewrapalign(&mut self, _ctx: &TimewrapAlignContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timewrapalign(&mut self, _ctx: &TimewrapAlignContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timewrapseries(&mut self, _ctx: &TimewrapSeriesContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timewrapseries(&mut self, _ctx: &TimewrapSeriesContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timechartparameter(&mut self, _ctx: &TimechartParameterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timechartparameter(&mut self, _ctx: &TimechartParameterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_spanliteral(&mut self, _ctx: &SpanLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_spanliteral(&mut self, _ctx: &SpanLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_evalcommand(&mut self, _ctx: &EvalCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_evalcommand(&mut self, _ctx: &EvalCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachcommand(&mut self, _ctx: &ForeachCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachcommand(&mut self, _ctx: &ForeachCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachargument(&mut self, _ctx: &ForeachArgumentContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachargument(&mut self, _ctx: &ForeachArgumentContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachtarget(&mut self, _ctx: &ForeachTargetContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachtarget(&mut self, _ctx: &ForeachTargetContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachoption(&mut self, _ctx: &ForeachOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachoption(&mut self, _ctx: &ForeachOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachevalcommand(&mut self, _ctx: &ForeachEvalCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachevalcommand(&mut self, _ctx: &ForeachEvalCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachevalclause(&mut self, _ctx: &ForeachEvalClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachevalclause(&mut self, _ctx: &ForeachEvalClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachevaltarget(&mut self, _ctx: &ForeachEvalTargetContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachevaltarget(&mut self, _ctx: &ForeachEvalTargetContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldformatcommand(&mut self, _ctx: &FieldformatCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldformatcommand(&mut self, _ctx: &FieldformatCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_headcommand(&mut self, _ctx: &HeadCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_headcommand(&mut self, _ctx: &HeadCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_bincommand(&mut self, _ctx: &BinCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_bincommand(&mut self, _ctx: &BinCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binoption(&mut self, _ctx: &BinOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binoption(&mut self, _ctx: &BinOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_aligntimevalue(&mut self, _ctx: &AligntimeValueContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_aligntimevalue(&mut self, _ctx: &AligntimeValueContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_numericspanvalue(&mut self, _ctx: &NumericSpanValueContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_numericspanvalue(&mut self, _ctx: &NumericSpanValueContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logbasedspanvalue(&mut self, _ctx: &LogBasedSpanValueContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logbasedspanvalue(&mut self, _ctx: &LogBasedSpanValueContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logwithbasespan(&mut self, _ctx: &LogWithBaseSpanContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logwithbasespan(&mut self, _ctx: &LogWithBaseSpanContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_raretopcommand(&mut self, _ctx: &RareTopCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_raretopcommand(&mut self, _ctx: &RareTopCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_raretopoption(&mut self, _ctx: &RareTopOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_raretopoption(&mut self, _ctx: &RareTopOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_grokcommand(&mut self, _ctx: &GrokCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_grokcommand(&mut self, _ctx: &GrokCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_parsecommand(&mut self, _ctx: &ParseCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_parsecommand(&mut self, _ctx: &ParseCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_spathcommand(&mut self, _ctx: &SpathCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_spathcommand(&mut self, _ctx: &SpathCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_spathparameter(&mut self, _ctx: &SpathParameterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_spathparameter(&mut self, _ctx: &SpathParameterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_indexablepath(&mut self, _ctx: &IndexablePathContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_indexablepath(&mut self, _ctx: &IndexablePathContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_pathelement(&mut self, _ctx: &PathElementContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_pathelement(&mut self, _ctx: &PathElementContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_patharrayaccess(&mut self, _ctx: &PathArrayAccessContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_patharrayaccess(&mut self, _ctx: &PathArrayAccessContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_regexcommand(&mut self, _ctx: &RegexCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_regexcommand(&mut self, _ctx: &RegexCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_regexexpr(&mut self, _ctx: &RegexExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_regexexpr(&mut self, _ctx: &RegexExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rexcommand(&mut self, _ctx: &RexCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rexcommand(&mut self, _ctx: &RexCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rexexpr(&mut self, _ctx: &RexExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rexexpr(&mut self, _ctx: &RexExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rexoption(&mut self, _ctx: &RexOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rexoption(&mut self, _ctx: &RexOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_patternsmethod(&mut self, _ctx: &PatternsMethodContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_patternsmethod(&mut self, _ctx: &PatternsMethodContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_patternscommand(&mut self, _ctx: &PatternsCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_patternscommand(&mut self, _ctx: &PatternsCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_patternscommandoption(&mut self, _ctx: &PatternsCommandOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_patternscommandoption(&mut self, _ctx: &PatternsCommandOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_patternsparameter(&mut self, _ctx: &PatternsParameterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_patternsparameter(&mut self, _ctx: &PatternsParameterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_patternmethod(&mut self, _ctx: &PatternMethodContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_patternmethod(&mut self, _ctx: &PatternMethodContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_patternmode(&mut self, _ctx: &PatternModeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_patternmode(&mut self, _ctx: &PatternModeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lookupcommand(&mut self, _ctx: &LookupCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lookupcommand(&mut self, _ctx: &LookupCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lookupmappinglist(&mut self, _ctx: &LookupMappingListContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lookupmappinglist(&mut self, _ctx: &LookupMappingListContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_outputcandidatelist(&mut self, _ctx: &OutputCandidateListContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_outputcandidatelist(&mut self, _ctx: &OutputCandidateListContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lookuppair(&mut self, _ctx: &LookupPairContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lookuppair(&mut self, _ctx: &LookupPairContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fillnullwithclause(&mut self, _ctx: &FillNullWithClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fillnullwithclause(&mut self, _ctx: &FillNullWithClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fillnullusingclause(&mut self, _ctx: &FillNullUsingClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fillnullusingclause(&mut self, _ctx: &FillNullUsingClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fillnullvaluewithfields(&mut self, _ctx: &FillNullValueWithFieldsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fillnullvaluewithfields(&mut self, _ctx: &FillNullValueWithFieldsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fillnullvalueallfields(&mut self, _ctx: &FillNullValueAllFieldsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fillnullvalueallfields(&mut self, _ctx: &FillNullValueAllFieldsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fillnullwith(&mut self, _ctx: &FillNullWithContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fillnullwith(&mut self, _ctx: &FillNullWithContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fillnullusing(&mut self, _ctx: &FillNullUsingContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fillnullusing(&mut self, _ctx: &FillNullUsingContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_replacementpair(&mut self, _ctx: &ReplacementPairContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_replacementpair(&mut self, _ctx: &ReplacementPairContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_convertcommand(&mut self, _ctx: &ConvertCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_convertcommand(&mut self, _ctx: &ConvertCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_convertfunction(&mut self, _ctx: &ConvertFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_convertfunction(&mut self, _ctx: &ConvertFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_trendlinecommand(&mut self, _ctx: &TrendlineCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_trendlinecommand(&mut self, _ctx: &TrendlineCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_trendlineclause(&mut self, _ctx: &TrendlineClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_trendlineclause(&mut self, _ctx: &TrendlineClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_trendlinetype(&mut self, _ctx: &TrendlineTypeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_trendlinetype(&mut self, _ctx: &TrendlineTypeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_expandcommand(&mut self, _ctx: &ExpandCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_expandcommand(&mut self, _ctx: &ExpandCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_mvcombinecommand(&mut self, _ctx: &MvcombineCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_mvcombinecommand(&mut self, _ctx: &MvcombineCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_nomvcommand(&mut self, _ctx: &NomvCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_nomvcommand(&mut self, _ctx: &NomvCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_mvexpandcommand(&mut self, _ctx: &MvexpandCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_mvexpandcommand(&mut self, _ctx: &MvexpandCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_flattencommand(&mut self, _ctx: &FlattenCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_flattencommand(&mut self, _ctx: &FlattenCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_appendcolcommand(&mut self, _ctx: &AppendcolCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_appendcolcommand(&mut self, _ctx: &AppendcolCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_appendcommand(&mut self, _ctx: &AppendCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_appendcommand(&mut self, _ctx: &AppendCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_multisearchcommand(&mut self, _ctx: &MultisearchCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_multisearchcommand(&mut self, _ctx: &MultisearchCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_unioncommand(&mut self, _ctx: &UnionCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_unioncommand(&mut self, _ctx: &UnionCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_subsearchoptions(&mut self, _ctx: &SubsearchOptionsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_subsearchoptions(&mut self, _ctx: &SubsearchOptionsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_uniondataset(&mut self, _ctx: &UnionDatasetContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_uniondataset(&mut self, _ctx: &UnionDatasetContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_kmeanscommand(&mut self, _ctx: &KmeansCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_kmeanscommand(&mut self, _ctx: &KmeansCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_kmeansparameter(&mut self, _ctx: &KmeansParameterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_kmeansparameter(&mut self, _ctx: &KmeansParameterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_adcommand(&mut self, _ctx: &AdCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_adcommand(&mut self, _ctx: &AdCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_adparameter(&mut self, _ctx: &AdParameterContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_adparameter(&mut self, _ctx: &AdParameterContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_mlcommand(&mut self, _ctx: &MlCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_mlcommand(&mut self, _ctx: &MlCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_mlarg(&mut self, _ctx: &MlArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_mlarg(&mut self, _ctx: &MlArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_addtotalscommand(&mut self, _ctx: &AddtotalsCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_addtotalscommand(&mut self, _ctx: &AddtotalsCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_addtotalsoption(&mut self, _ctx: &AddtotalsOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_addtotalsoption(&mut self, _ctx: &AddtotalsOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_addcoltotalscommand(&mut self, _ctx: &AddcoltotalsCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_addcoltotalscommand(&mut self, _ctx: &AddcoltotalsCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_addcoltotalsoption(&mut self, _ctx: &AddcoltotalsOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_addcoltotalsoption(&mut self, _ctx: &AddcoltotalsOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_graphlookupcommand(&mut self, _ctx: &GraphLookupCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_graphlookupcommand(&mut self, _ctx: &GraphLookupCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_startclause(&mut self, _ctx: &StartClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_startclause(&mut self, _ctx: &StartClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_edgeclause(&mut self, _ctx: &EdgeClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_edgeclause(&mut self, _ctx: &EdgeClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_graphlookupargs(&mut self, _ctx: &GraphLookupArgsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_graphlookupargs(&mut self, _ctx: &GraphLookupArgsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_xyseriescommand(&mut self, _ctx: &XyseriesCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_xyseriescommand(&mut self, _ctx: &XyseriesCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_xyseriesoption(&mut self, _ctx: &XyseriesOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_xyseriesoption(&mut self, _ctx: &XyseriesOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_xyseriespivotvalues(&mut self, _ctx: &XyseriesPivotValuesContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_xyseriespivotvalues(&mut self, _ctx: &XyseriesPivotValuesContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fromclause(&mut self, _ctx: &FromClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fromclause(&mut self, _ctx: &FromClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_tableorsubqueryclause(&mut self, _ctx: &TableOrSubqueryClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_tableorsubqueryclause(&mut self, _ctx: &TableOrSubqueryClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_tablesourceclause(&mut self, _ctx: &TableSourceClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_tablesourceclause(&mut self, _ctx: &TableSourceClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_dynamicsourceclause(&mut self, _ctx: &DynamicSourceClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_dynamicsourceclause(&mut self, _ctx: &DynamicSourceClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_sourcereference(&mut self, _ctx: &SourceReferenceContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_sourcereference(&mut self, _ctx: &SourceReferenceContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_sourcefilterarg(&mut self, _ctx: &SourceFilterArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_sourcefilterarg(&mut self, _ctx: &SourceFilterArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_joincommand(&mut self, _ctx: &JoinCommandContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_joincommand(&mut self, _ctx: &JoinCommandContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_sqllikejointype(&mut self, _ctx: &SqlLikeJoinTypeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_sqllikejointype(&mut self, _ctx: &SqlLikeJoinTypeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_jointype(&mut self, _ctx: &JoinTypeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_jointype(&mut self, _ctx: &JoinTypeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_sidealias(&mut self, _ctx: &SideAliasContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_sidealias(&mut self, _ctx: &SideAliasContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_joincriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_joincriteria(&mut self, _ctx: &JoinCriteriaContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_joinhintlist(&mut self, _ctx: &JoinHintListContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_joinhintlist(&mut self, _ctx: &JoinHintListContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lefthint(&mut self, _ctx: &LeftHintContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lefthint(&mut self, _ctx: &LeftHintContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_righthint(&mut self, _ctx: &RightHintContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_righthint(&mut self, _ctx: &RightHintContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_overwriteoption(&mut self, _ctx: &OverwriteOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_overwriteoption(&mut self, _ctx: &OverwriteOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_typeoption(&mut self, _ctx: &TypeOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_typeoption(&mut self, _ctx: &TypeOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_maxoption(&mut self, _ctx: &MaxOptionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_maxoption(&mut self, _ctx: &MaxOptionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_renameclause(&mut self, _ctx: &RenameClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_renameclause(&mut self, _ctx: &RenameClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_byclause(&mut self, _ctx: &ByClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_byclause(&mut self, _ctx: &ByClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_statsbyclause(&mut self, _ctx: &StatsByClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_statsbyclause(&mut self, _ctx: &StatsByClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_byspanclause(&mut self, _ctx: &BySpanClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_byspanclause(&mut self, _ctx: &BySpanClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_spanclause(&mut self, _ctx: &SpanClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_spanclause(&mut self, _ctx: &SpanClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_sortbyclause(&mut self, _ctx: &SortbyClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_sortbyclause(&mut self, _ctx: &SortbyClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_evalclause(&mut self, _ctx: &EvalClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_evalclause(&mut self, _ctx: &EvalClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldformatevalclause(&mut self, _ctx: &FieldFormatEvalClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldformatevalclause(&mut self, _ctx: &FieldFormatEvalClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_eventstatsaggterm(&mut self, _ctx: &EventstatsAggTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_eventstatsaggterm(&mut self, _ctx: &EventstatsAggTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_streamstatsaggterm(&mut self, _ctx: &StreamstatsAggTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_streamstatsaggterm(&mut self, _ctx: &StreamstatsAggTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_windowfunction(&mut self, _ctx: &WindowFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_windowfunction(&mut self, _ctx: &WindowFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_windowfunctionname(&mut self, _ctx: &WindowFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_windowfunctionname(&mut self, _ctx: &WindowFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_scalarwindowfunctionname(&mut self, _ctx: &ScalarWindowFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_scalarwindowfunctionname(&mut self, _ctx: &ScalarWindowFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_statsaggterm(&mut self, _ctx: &StatsAggTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_statsaggterm(&mut self, _ctx: &StatsAggTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_countevalfunctioncall(&mut self, _ctx: &CountEvalFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_countevalfunctioncall(&mut self, _ctx: &CountEvalFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_countallfunctioncall(&mut self, _ctx: &CountAllFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_countallfunctioncall(&mut self, _ctx: &CountAllFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_percentileshortcutfunctioncall(&mut self, _ctx: &PercentileShortcutFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_percentileshortcutfunctioncall(&mut self, _ctx: &PercentileShortcutFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_distinctcountfunctioncall(&mut self, _ctx: &DistinctCountFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_distinctcountfunctioncall(&mut self, _ctx: &DistinctCountFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_takeaggfunctioncall(&mut self, _ctx: &TakeAggFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_takeaggfunctioncall(&mut self, _ctx: &TakeAggFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_valuesaggfunctioncall(&mut self, _ctx: &ValuesAggFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_valuesaggfunctioncall(&mut self, _ctx: &ValuesAggFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_percentileapproxfunctioncall(&mut self, _ctx: &PercentileApproxFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_percentileapproxfunctioncall(&mut self, _ctx: &PercentileApproxFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_perfunctioncall(&mut self, _ctx: &PerFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_perfunctioncall(&mut self, _ctx: &PerFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_statsfunctioncall(&mut self, _ctx: &StatsFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_statsfunctioncall(&mut self, _ctx: &StatsFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_statsfunctionname(&mut self, _ctx: &StatsFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_statsfunctionname(&mut self, _ctx: &StatsFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_takeaggfunction(&mut self, _ctx: &TakeAggFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_takeaggfunction(&mut self, _ctx: &TakeAggFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_valuesaggfunction(&mut self, _ctx: &ValuesAggFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_valuesaggfunction(&mut self, _ctx: &ValuesAggFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_percentileapproxfunction(&mut self, _ctx: &PercentileApproxFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_percentileapproxfunction(&mut self, _ctx: &PercentileApproxFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_perfunction(&mut self, _ctx: &PerFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_perfunction(&mut self, _ctx: &PerFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_numericliteral(&mut self, _ctx: &NumericLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_numericliteral(&mut self, _ctx: &NumericLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_stringdotlogicalexpression(&mut self, _ctx: &StringDotlogicalExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_stringdotlogicalexpression(&mut self, _ctx: &StringDotlogicalExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_stringdotlogicalexpressiondotstring(&mut self, _ctx: &StringDotlogicalExpressionDotStringContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_stringdotlogicalexpressiondotstring(&mut self, _ctx: &StringDotlogicalExpressionDotStringContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logicalexpressiondotstring(&mut self, _ctx: &LogicalExpressionDotStringContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logicalexpressiondotstring(&mut self, _ctx: &LogicalExpressionDotStringContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_ffstandardlogicalexpression(&mut self, _ctx: &FfStandardLogicalExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_ffstandardlogicalexpression(&mut self, _ctx: &FfStandardLogicalExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logicalnot(&mut self, _ctx: &LogicalNotContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logicalnot(&mut self, _ctx: &LogicalNotContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logicalexpr(&mut self, _ctx: &LogicalExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logicalexpr(&mut self, _ctx: &LogicalExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logicaland(&mut self, _ctx: &LogicalAndContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logicaland(&mut self, _ctx: &LogicalAndContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logicalxor(&mut self, _ctx: &LogicalXorContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logicalxor(&mut self, _ctx: &LogicalXorContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_logicalor(&mut self, _ctx: &LogicalOrContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_logicalor(&mut self, _ctx: &LogicalOrContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_relevanceexpr(&mut self, _ctx: &RelevanceExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_relevanceexpr(&mut self, _ctx: &RelevanceExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_valueexpr(&mut self, _ctx: &ValueExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_valueexpr(&mut self, _ctx: &ValueExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_inexpr(&mut self, _ctx: &InExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_inexpr(&mut self, _ctx: &InExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_isnullpredicate(&mut self, _ctx: &IsNullPredicateContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_isnullpredicate(&mut self, _ctx: &IsNullPredicateContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_between(&mut self, _ctx: &BetweenContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_between(&mut self, _ctx: &BetweenContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_compareexpr(&mut self, _ctx: &CompareExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_compareexpr(&mut self, _ctx: &CompareExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_nullnotnull(&mut self, _ctx: &NullNotnullContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_nullnotnull(&mut self, _ctx: &NullNotnullContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_insubqueryexpr(&mut self, _ctx: &InSubqueryExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_insubqueryexpr(&mut self, _ctx: &InSubqueryExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lambdaexpr(&mut self, _ctx: &LambdaExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lambdaexpr(&mut self, _ctx: &LambdaExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_literalvalueexpr(&mut self, _ctx: &LiteralValueExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_literalvalueexpr(&mut self, _ctx: &LiteralValueExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_functioncallexpr(&mut self, _ctx: &FunctionCallExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_functioncallexpr(&mut self, _ctx: &FunctionCallExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_existssubqueryexpr(&mut self, _ctx: &ExistsSubqueryExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_existssubqueryexpr(&mut self, _ctx: &ExistsSubqueryExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachplaceholderexpr(&mut self, _ctx: &ForeachPlaceholderExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachplaceholderexpr(&mut self, _ctx: &ForeachPlaceholderExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_scalarsubqueryexpr(&mut self, _ctx: &ScalarSubqueryExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_scalarsubqueryexpr(&mut self, _ctx: &ScalarSubqueryExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_nestedvalueexpr(&mut self, _ctx: &NestedValueExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_nestedvalueexpr(&mut self, _ctx: &NestedValueExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_binaryarithmetic(&mut self, _ctx: &BinaryArithmeticContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_binaryarithmetic(&mut self, _ctx: &BinaryArithmeticContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldexpr(&mut self, _ctx: &FieldExprContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldexpr(&mut self, _ctx: &FieldExprContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_evalexpression(&mut self, _ctx: &EvalExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_evalexpression(&mut self, _ctx: &EvalExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_functioncall(&mut self, _ctx: &FunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_functioncall(&mut self, _ctx: &FunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_mvmapfunctioncall(&mut self, _ctx: &MvmapFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_mvmapfunctioncall(&mut self, _ctx: &MvmapFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_positionfunctioncall(&mut self, _ctx: &PositionFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_positionfunctioncall(&mut self, _ctx: &PositionFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_casefunctioncall(&mut self, _ctx: &CaseFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_casefunctioncall(&mut self, _ctx: &CaseFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_relevanceexpression(&mut self, _ctx: &RelevanceExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_relevanceexpression(&mut self, _ctx: &RelevanceExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_singlefieldrelevancefunction(&mut self, _ctx: &SingleFieldRelevanceFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_singlefieldrelevancefunction(&mut self, _ctx: &SingleFieldRelevanceFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_multifieldrelevancefunction(&mut self, _ctx: &MultiFieldRelevanceFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_multifieldrelevancefunction(&mut self, _ctx: &MultiFieldRelevanceFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timemodifier(&mut self, _ctx: &TimeModifierContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timemodifier(&mut self, _ctx: &TimeModifierContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timemodifiervalue(&mut self, _ctx: &TimeModifierValueContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timemodifiervalue(&mut self, _ctx: &TimeModifierValueContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_tablesource(&mut self, _ctx: &TableSourceContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_tablesource(&mut self, _ctx: &TableSourceContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_tablefunction(&mut self, _ctx: &TableFunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_tablefunction(&mut self, _ctx: &TableFunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldlist(&mut self, _ctx: &FieldListContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldlist(&mut self, _ctx: &FieldListContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_invalidmixedsortfield(&mut self, _ctx: &InvalidMixedSortFieldContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_invalidmixedsortfield(&mut self, _ctx: &InvalidMixedSortFieldContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_prefixsortfield(&mut self, _ctx: &PrefixSortFieldContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_prefixsortfield(&mut self, _ctx: &PrefixSortFieldContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_suffixsortfield(&mut self, _ctx: &SuffixSortFieldContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_suffixsortfield(&mut self, _ctx: &SuffixSortFieldContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_defaultsortfield(&mut self, _ctx: &DefaultSortFieldContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_defaultsortfield(&mut self, _ctx: &DefaultSortFieldContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_sortfieldexpression(&mut self, _ctx: &SortFieldExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_sortfieldexpression(&mut self, _ctx: &SortFieldExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldexpression(&mut self, _ctx: &FieldExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldexpression(&mut self, _ctx: &FieldExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_wcfieldexpression(&mut self, _ctx: &WcFieldExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_wcfieldexpression(&mut self, _ctx: &WcFieldExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachplaceholder(&mut self, _ctx: &ForeachPlaceholderContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachplaceholder(&mut self, _ctx: &ForeachPlaceholderContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_foreachplaceholdername(&mut self, _ctx: &ForeachPlaceholderNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_foreachplaceholdername(&mut self, _ctx: &ForeachPlaceholderNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_selectfieldexpression(&mut self, _ctx: &SelectFieldExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_selectfieldexpression(&mut self, _ctx: &SelectFieldExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_renamefieldexpression(&mut self, _ctx: &RenameFieldExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_renamefieldexpression(&mut self, _ctx: &RenameFieldExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_evalfunctioncall(&mut self, _ctx: &EvalFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_evalfunctioncall(&mut self, _ctx: &EvalFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_datatypefunctioncall(&mut self, _ctx: &DataTypeFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_datatypefunctioncall(&mut self, _ctx: &DataTypeFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_converteddatatype(&mut self, _ctx: &ConvertedDataTypeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_converteddatatype(&mut self, _ctx: &ConvertedDataTypeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_evalfunctionname(&mut self, _ctx: &EvalFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_evalfunctionname(&mut self, _ctx: &EvalFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_functionargs(&mut self, _ctx: &FunctionArgsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_functionargs(&mut self, _ctx: &FunctionArgsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_namedfunctionargs(&mut self, _ctx: &NamedFunctionArgsContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_namedfunctionargs(&mut self, _ctx: &NamedFunctionArgsContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_functionarg(&mut self, _ctx: &FunctionArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_functionarg(&mut self, _ctx: &FunctionArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_namedfunctionarg(&mut self, _ctx: &NamedFunctionArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_namedfunctionarg(&mut self, _ctx: &NamedFunctionArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_functionargexpression(&mut self, _ctx: &FunctionArgExpressionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_functionargexpression(&mut self, _ctx: &FunctionArgExpressionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_lambda(&mut self, _ctx: &LambdaContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_lambda(&mut self, _ctx: &LambdaContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_relevancearg(&mut self, _ctx: &RelevanceArgContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_relevancearg(&mut self, _ctx: &RelevanceArgContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_relevanceargname(&mut self, _ctx: &RelevanceArgNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_relevanceargname(&mut self, _ctx: &RelevanceArgNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_relevancefieldandweight(&mut self, _ctx: &RelevanceFieldAndWeightContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_relevancefieldandweight(&mut self, _ctx: &RelevanceFieldAndWeightContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_relevancefieldweight(&mut self, _ctx: &RelevanceFieldWeightContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_relevancefieldweight(&mut self, _ctx: &RelevanceFieldWeightContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_relevancefield(&mut self, _ctx: &RelevanceFieldContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_relevancefield(&mut self, _ctx: &RelevanceFieldContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_relevancequery(&mut self, _ctx: &RelevanceQueryContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_relevancequery(&mut self, _ctx: &RelevanceQueryContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_relevanceargvalue(&mut self, _ctx: &RelevanceArgValueContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_relevanceargvalue(&mut self, _ctx: &RelevanceArgValueContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_mathematicalfunctionname(&mut self, _ctx: &MathematicalFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_mathematicalfunctionname(&mut self, _ctx: &MathematicalFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_geoipfunctionname(&mut self, _ctx: &GeoipFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_geoipfunctionname(&mut self, _ctx: &GeoipFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_collectionfunctionname(&mut self, _ctx: &CollectionFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_collectionfunctionname(&mut self, _ctx: &CollectionFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_trigonometricfunctionname(&mut self, _ctx: &TrigonometricFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_trigonometricfunctionname(&mut self, _ctx: &TrigonometricFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_jsonfunctionname(&mut self, _ctx: &JsonFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_jsonfunctionname(&mut self, _ctx: &JsonFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_cryptographicfunctionname(&mut self, _ctx: &CryptographicFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_cryptographicfunctionname(&mut self, _ctx: &CryptographicFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_datetimefunctionname(&mut self, _ctx: &DateTimeFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_datetimefunctionname(&mut self, _ctx: &DateTimeFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_getformatfunctioncall(&mut self, _ctx: &GetFormatFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_getformatfunctioncall(&mut self, _ctx: &GetFormatFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_getformattype(&mut self, _ctx: &GetFormatTypeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_getformattype(&mut self, _ctx: &GetFormatTypeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_extractfunctioncall(&mut self, _ctx: &ExtractFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_extractfunctioncall(&mut self, _ctx: &ExtractFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_simpledatetimepart(&mut self, _ctx: &SimpleDateTimePartContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_simpledatetimepart(&mut self, _ctx: &SimpleDateTimePartContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_complexdatetimepart(&mut self, _ctx: &ComplexDateTimePartContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_complexdatetimepart(&mut self, _ctx: &ComplexDateTimePartContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_datetimepart(&mut self, _ctx: &DatetimePartContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_datetimepart(&mut self, _ctx: &DatetimePartContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timestampfunctioncall(&mut self, _ctx: &TimestampFunctionCallContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timestampfunctioncall(&mut self, _ctx: &TimestampFunctionCallContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timestampfunctionname(&mut self, _ctx: &TimestampFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timestampfunctionname(&mut self, _ctx: &TimestampFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_conditionfunctionname(&mut self, _ctx: &ConditionFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_conditionfunctionname(&mut self, _ctx: &ConditionFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_flowcontrolfunctionname(&mut self, _ctx: &FlowControlFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_flowcontrolfunctionname(&mut self, _ctx: &FlowControlFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_systemfunctionname(&mut self, _ctx: &SystemFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_systemfunctionname(&mut self, _ctx: &SystemFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_textfunctionname(&mut self, _ctx: &TextFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_textfunctionname(&mut self, _ctx: &TextFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_positionfunctionname(&mut self, _ctx: &PositionFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_positionfunctionname(&mut self, _ctx: &PositionFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_comparisonoperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_comparisonoperator(&mut self, _ctx: &ComparisonOperatorContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_singlefieldrelevancefunctionname(&mut self, _ctx: &SingleFieldRelevanceFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_singlefieldrelevancefunctionname(&mut self, _ctx: &SingleFieldRelevanceFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_multifieldrelevancefunctionname(&mut self, _ctx: &MultiFieldRelevanceFunctionNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_multifieldrelevancefunctionname(&mut self, _ctx: &MultiFieldRelevanceFunctionNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_literalvalue(&mut self, _ctx: &LiteralValueContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_literalvalue(&mut self, _ctx: &LiteralValueContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_intervalliteral(&mut self, _ctx: &IntervalLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_intervalliteral(&mut self, _ctx: &IntervalLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_stringliteral(&mut self, _ctx: &StringLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_stringliteral(&mut self, _ctx: &StringLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_integerliteral(&mut self, _ctx: &IntegerLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_integerliteral(&mut self, _ctx: &IntegerLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_decimalliteral(&mut self, _ctx: &DecimalLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_decimalliteral(&mut self, _ctx: &DecimalLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_doubleliteral(&mut self, _ctx: &DoubleLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_doubleliteral(&mut self, _ctx: &DoubleLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_floatliteral(&mut self, _ctx: &FloatLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_floatliteral(&mut self, _ctx: &FloatLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_booleanliteral(&mut self, _ctx: &BooleanLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_booleanliteral(&mut self, _ctx: &BooleanLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_datetimeliteral(&mut self, _ctx: &DatetimeLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_datetimeliteral(&mut self, _ctx: &DatetimeLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_dateliteral(&mut self, _ctx: &DateLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_dateliteral(&mut self, _ctx: &DateLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timeliteral(&mut self, _ctx: &TimeLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timeliteral(&mut self, _ctx: &TimeLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_timestampliteral(&mut self, _ctx: &TimestampLiteralContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_timestampliteral(&mut self, _ctx: &TimestampLiteralContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_intervalunit(&mut self, _ctx: &IntervalUnitContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_intervalunit(&mut self, _ctx: &IntervalUnitContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_valuelist(&mut self, _ctx: &ValueListContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_valuelist(&mut self, _ctx: &ValueListContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_identsasqualifiedname(&mut self, _ctx: &IdentsAsQualifiedNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_identsasqualifiedname(&mut self, _ctx: &IdentsAsQualifiedNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_identsastablequalifiedname(&mut self, _ctx: &IdentsAsTableQualifiedNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_identsastablequalifiedname(&mut self, _ctx: &IdentsAsTableQualifiedNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_identsaswildcardqualifiedname(&mut self, _ctx: &IdentsAsWildcardQualifiedNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_identsaswildcardqualifiedname(&mut self, _ctx: &IdentsAsWildcardQualifiedNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_identsasqualifiednameseq(&mut self, _ctx: &IdentsAsQualifiedNameSeqContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_identsasqualifiednameseq(&mut self, _ctx: &IdentsAsQualifiedNameSeqContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_ident(&mut self, _ctx: &IdentContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_ident(&mut self, _ctx: &IdentContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_tableident(&mut self, _ctx: &TableIdentContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_tableident(&mut self, _ctx: &TableIdentContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_wildcard(&mut self, _ctx: &WildcardContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_wildcard(&mut self, _ctx: &WildcardContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_keywordscanbeid(&mut self, _ctx: &KeywordsCanBeIdContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_keywordscanbeid(&mut self, _ctx: &KeywordsCanBeIdContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_searchablekeyword(&mut self, _ctx: &SearchableKeyWordContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  OpenSearchPPLParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_searchablekeyword(&mut self, _ctx: &SearchableKeyWordContext<'input>) {}


}
