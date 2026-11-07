#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]
// Generated from LuceneParser.g4 by ANTLR 4.13.2

use super::luceneparser::*;
use antlr4rust::tree::ParseTreeListener;

// A complete Visitor for a parse tree produced by LuceneParser.

pub trait LuceneParserBaseListener<'input>:
    ParseTreeListener<'input, LuceneParserContextType> {

    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_toplevelquery(&mut self, _ctx: &TopLevelQueryContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_toplevelquery(&mut self, _ctx: &TopLevelQueryContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_query(&mut self, _ctx: &QueryContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_query(&mut self, _ctx: &QueryContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_conjoinedclause(&mut self, _ctx: &ConjoinedClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_conjoinedclause(&mut self, _ctx: &ConjoinedClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_conjunction(&mut self, _ctx: &ConjunctionContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_conjunction(&mut self, _ctx: &ConjunctionContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_modifier(&mut self, _ctx: &ModifierContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_modifier(&mut self, _ctx: &ModifierContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_modclause(&mut self, _ctx: &ModClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_modclause(&mut self, _ctx: &ModClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_clause(&mut self, _ctx: &ClauseContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_clause(&mut self, _ctx: &ClauseContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_fieldname(&mut self, _ctx: &FieldNameContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_fieldname(&mut self, _ctx: &FieldNameContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_group(&mut self, _ctx: &GroupContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_group(&mut self, _ctx: &GroupContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_boost(&mut self, _ctx: &BoostContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_boost(&mut self, _ctx: &BoostContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_plainterm(&mut self, _ctx: &PlainTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_plainterm(&mut self, _ctx: &PlainTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_starterm(&mut self, _ctx: &StarTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_starterm(&mut self, _ctx: &StarTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_prefixterm(&mut self, _ctx: &PrefixTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_prefixterm(&mut self, _ctx: &PrefixTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_wildcardterm(&mut self, _ctx: &WildcardTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_wildcardterm(&mut self, _ctx: &WildcardTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_regexpterm(&mut self, _ctx: &RegexpTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_regexpterm(&mut self, _ctx: &RegexpTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_bareoperatorterm(&mut self, _ctx: &BareOperatorTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_bareoperatorterm(&mut self, _ctx: &BareOperatorTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_quotedterm(&mut self, _ctx: &QuotedTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_quotedterm(&mut self, _ctx: &QuotedTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeterm(&mut self, _ctx: &RangeTermContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeterm(&mut self, _ctx: &RangeTermContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_termmodifiers(&mut self, _ctx: &TermModifiersContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_termmodifiers(&mut self, _ctx: &TermModifiersContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_range(&mut self, _ctx: &RangeContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_range(&mut self, _ctx: &RangeContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangestart(&mut self, _ctx: &RangeStartContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangestart(&mut self, _ctx: &RangeStartContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangeend(&mut self, _ctx: &RangeEndContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangeend(&mut self, _ctx: &RangeEndContext<'input>) {}


    /**
     * Enter a parse tree produced by \{@link LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn enter_rangebound(&mut self, _ctx: &RangeBoundContext<'input>) {}
    /**
     * Exit a parse tree produced by \{@link  LuceneParserBaseParser#s}.
     * @param ctx the parse tree
     */
    fn exit_rangebound(&mut self, _ctx: &RangeBoundContext<'input>) {}


}
