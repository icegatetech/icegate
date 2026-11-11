#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]

// Generated from LuceneParser.g4 by ANTLR 4.13.2

use antlr4rust::tree::ParseTreeVisitor;
use super::luceneparser::*;

// A complete Visitor for a parse tree produced by LuceneParser.

pub trait LuceneParserBaseVisitor<'input>:
    ParseTreeVisitor<'input, LuceneParserContextType> {
	// Visit a parse tree produced by LuceneParser#topLevelQuery.
	fn visit_toplevelquery(&mut self, ctx: &TopLevelQueryContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#query.
	fn visit_query(&mut self, ctx: &QueryContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#conjoinedClause.
	fn visit_conjoinedclause(&mut self, ctx: &ConjoinedClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#conjunction.
	fn visit_conjunction(&mut self, ctx: &ConjunctionContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#modifier.
	fn visit_modifier(&mut self, ctx: &ModifierContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#modClause.
	fn visit_modclause(&mut self, ctx: &ModClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#clause.
	fn visit_clause(&mut self, ctx: &ClauseContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#fieldName.
	fn visit_fieldname(&mut self, ctx: &FieldNameContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#group.
	fn visit_group(&mut self, ctx: &GroupContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#boost.
	fn visit_boost(&mut self, ctx: &BoostContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#plainTerm.
	fn visit_plainterm(&mut self, ctx: &PlainTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#starTerm.
	fn visit_starterm(&mut self, ctx: &StarTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#prefixTerm.
	fn visit_prefixterm(&mut self, ctx: &PrefixTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#wildcardTerm.
	fn visit_wildcardterm(&mut self, ctx: &WildcardTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#regexpTerm.
	fn visit_regexpterm(&mut self, ctx: &RegexpTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#bareOperatorTerm.
	fn visit_bareoperatorterm(&mut self, ctx: &BareOperatorTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#quotedTerm.
	fn visit_quotedterm(&mut self, ctx: &QuotedTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#rangeTerm.
	fn visit_rangeterm(&mut self, ctx: &RangeTermContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#termModifiers.
	fn visit_termmodifiers(&mut self, ctx: &TermModifiersContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#range.
	fn visit_range(&mut self, ctx: &RangeContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#rangeStart.
	fn visit_rangestart(&mut self, ctx: &RangeStartContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#rangeEnd.
	fn visit_rangeend(&mut self, ctx: &RangeEndContext<'input>) {
            self.visit_children(ctx)
        }

	// Visit a parse tree produced by LuceneParser#rangeBound.
	fn visit_rangebound(&mut self, ctx: &RangeBoundContext<'input>) {
            self.visit_children(ctx)
        }

}
