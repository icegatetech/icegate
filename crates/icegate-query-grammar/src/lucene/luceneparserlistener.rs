#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]
#![allow(nonstandard_style)]
// Generated from LuceneParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::ParseTreeListener;
use super::luceneparser::*;

pub trait LuceneParserListener<'input> : ParseTreeListener<'input,LuceneParserContextType>{
/**
 * Enter a parse tree produced by {@link LuceneParser#topLevelQuery}.
 * @param ctx the parse tree
 */
fn enter_topLevelQuery(&mut self, _ctx: &TopLevelQueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#topLevelQuery}.
 * @param ctx the parse tree
 */
fn exit_topLevelQuery(&mut self, _ctx: &TopLevelQueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#query}.
 * @param ctx the parse tree
 */
fn enter_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#query}.
 * @param ctx the parse tree
 */
fn exit_query(&mut self, _ctx: &QueryContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#conjoinedClause}.
 * @param ctx the parse tree
 */
fn enter_conjoinedClause(&mut self, _ctx: &ConjoinedClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#conjoinedClause}.
 * @param ctx the parse tree
 */
fn exit_conjoinedClause(&mut self, _ctx: &ConjoinedClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#conjunction}.
 * @param ctx the parse tree
 */
fn enter_conjunction(&mut self, _ctx: &ConjunctionContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#conjunction}.
 * @param ctx the parse tree
 */
fn exit_conjunction(&mut self, _ctx: &ConjunctionContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#modifier}.
 * @param ctx the parse tree
 */
fn enter_modifier(&mut self, _ctx: &ModifierContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#modifier}.
 * @param ctx the parse tree
 */
fn exit_modifier(&mut self, _ctx: &ModifierContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#modClause}.
 * @param ctx the parse tree
 */
fn enter_modClause(&mut self, _ctx: &ModClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#modClause}.
 * @param ctx the parse tree
 */
fn exit_modClause(&mut self, _ctx: &ModClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#clause}.
 * @param ctx the parse tree
 */
fn enter_clause(&mut self, _ctx: &ClauseContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#clause}.
 * @param ctx the parse tree
 */
fn exit_clause(&mut self, _ctx: &ClauseContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#fieldName}.
 * @param ctx the parse tree
 */
fn enter_fieldName(&mut self, _ctx: &FieldNameContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#fieldName}.
 * @param ctx the parse tree
 */
fn exit_fieldName(&mut self, _ctx: &FieldNameContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#group}.
 * @param ctx the parse tree
 */
fn enter_group(&mut self, _ctx: &GroupContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#group}.
 * @param ctx the parse tree
 */
fn exit_group(&mut self, _ctx: &GroupContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#boost}.
 * @param ctx the parse tree
 */
fn enter_boost(&mut self, _ctx: &BoostContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#boost}.
 * @param ctx the parse tree
 */
fn exit_boost(&mut self, _ctx: &BoostContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code plainTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn enter_plainTerm(&mut self, _ctx: &PlainTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code plainTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn exit_plainTerm(&mut self, _ctx: &PlainTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code starTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn enter_starTerm(&mut self, _ctx: &StarTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code starTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn exit_starTerm(&mut self, _ctx: &StarTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code prefixTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn enter_prefixTerm(&mut self, _ctx: &PrefixTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code prefixTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn exit_prefixTerm(&mut self, _ctx: &PrefixTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code wildcardTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn enter_wildcardTerm(&mut self, _ctx: &WildcardTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code wildcardTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn exit_wildcardTerm(&mut self, _ctx: &WildcardTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code regexpTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn enter_regexpTerm(&mut self, _ctx: &RegexpTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code regexpTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn exit_regexpTerm(&mut self, _ctx: &RegexpTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code bareOperatorTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn enter_bareOperatorTerm(&mut self, _ctx: &BareOperatorTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code bareOperatorTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn exit_bareOperatorTerm(&mut self, _ctx: &BareOperatorTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code quotedTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn enter_quotedTerm(&mut self, _ctx: &QuotedTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code quotedTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn exit_quotedTerm(&mut self, _ctx: &QuotedTermContext<'input>) { }
/**
 * Enter a parse tree produced by the {@code rangeTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn enter_rangeTerm(&mut self, _ctx: &RangeTermContext<'input>) { }
/**
 * Exit a parse tree produced by the {@code rangeTerm}
 * labeled alternative in {@link LuceneParser#term}.
 * @param ctx the parse tree
 */
fn exit_rangeTerm(&mut self, _ctx: &RangeTermContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#termModifiers}.
 * @param ctx the parse tree
 */
fn enter_termModifiers(&mut self, _ctx: &TermModifiersContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#termModifiers}.
 * @param ctx the parse tree
 */
fn exit_termModifiers(&mut self, _ctx: &TermModifiersContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#range}.
 * @param ctx the parse tree
 */
fn enter_range(&mut self, _ctx: &RangeContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#range}.
 * @param ctx the parse tree
 */
fn exit_range(&mut self, _ctx: &RangeContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#rangeStart}.
 * @param ctx the parse tree
 */
fn enter_rangeStart(&mut self, _ctx: &RangeStartContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#rangeStart}.
 * @param ctx the parse tree
 */
fn exit_rangeStart(&mut self, _ctx: &RangeStartContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#rangeEnd}.
 * @param ctx the parse tree
 */
fn enter_rangeEnd(&mut self, _ctx: &RangeEndContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#rangeEnd}.
 * @param ctx the parse tree
 */
fn exit_rangeEnd(&mut self, _ctx: &RangeEndContext<'input>) { }
/**
 * Enter a parse tree produced by {@link LuceneParser#rangeBound}.
 * @param ctx the parse tree
 */
fn enter_rangeBound(&mut self, _ctx: &RangeBoundContext<'input>) { }
/**
 * Exit a parse tree produced by {@link LuceneParser#rangeBound}.
 * @param ctx the parse tree
 */
fn exit_rangeBound(&mut self, _ctx: &RangeBoundContext<'input>) { }

}

antlr4rust::coerce_from!{ 'input : LuceneParserListener<'input> }
