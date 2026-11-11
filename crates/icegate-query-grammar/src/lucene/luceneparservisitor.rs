#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]
#![allow(nonstandard_style)]
// Generated from LuceneParser.g4 by ANTLR 4.13.2
use antlr4rust::tree::{ParseTreeVisitor,ParseTreeVisitorCompat};
use super::luceneparser::*;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link LuceneParser}.
 */
pub trait LuceneParserVisitor<'input>: ParseTreeVisitor<'input,LuceneParserContextType>{
	/**
	 * Visit a parse tree produced by {@link LuceneParser#topLevelQuery}.
	 * @param ctx the parse tree
	 */
	fn visit_topLevelQuery(&mut self, ctx: &TopLevelQueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#query}.
	 * @param ctx the parse tree
	 */
	fn visit_query(&mut self, ctx: &QueryContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#conjoinedClause}.
	 * @param ctx the parse tree
	 */
	fn visit_conjoinedClause(&mut self, ctx: &ConjoinedClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#conjunction}.
	 * @param ctx the parse tree
	 */
	fn visit_conjunction(&mut self, ctx: &ConjunctionContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#modifier}.
	 * @param ctx the parse tree
	 */
	fn visit_modifier(&mut self, ctx: &ModifierContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#modClause}.
	 * @param ctx the parse tree
	 */
	fn visit_modClause(&mut self, ctx: &ModClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#clause}.
	 * @param ctx the parse tree
	 */
	fn visit_clause(&mut self, ctx: &ClauseContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#fieldName}.
	 * @param ctx the parse tree
	 */
	fn visit_fieldName(&mut self, ctx: &FieldNameContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#group}.
	 * @param ctx the parse tree
	 */
	fn visit_group(&mut self, ctx: &GroupContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#boost}.
	 * @param ctx the parse tree
	 */
	fn visit_boost(&mut self, ctx: &BoostContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code plainTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
	fn visit_plainTerm(&mut self, ctx: &PlainTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code starTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
	fn visit_starTerm(&mut self, ctx: &StarTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code prefixTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
	fn visit_prefixTerm(&mut self, ctx: &PrefixTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code wildcardTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
	fn visit_wildcardTerm(&mut self, ctx: &WildcardTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code regexpTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
	fn visit_regexpTerm(&mut self, ctx: &RegexpTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code bareOperatorTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
	fn visit_bareOperatorTerm(&mut self, ctx: &BareOperatorTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code quotedTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
	fn visit_quotedTerm(&mut self, ctx: &QuotedTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by the {@code rangeTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeTerm(&mut self, ctx: &RangeTermContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#termModifiers}.
	 * @param ctx the parse tree
	 */
	fn visit_termModifiers(&mut self, ctx: &TermModifiersContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#range}.
	 * @param ctx the parse tree
	 */
	fn visit_range(&mut self, ctx: &RangeContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#rangeStart}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeStart(&mut self, ctx: &RangeStartContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#rangeEnd}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeEnd(&mut self, ctx: &RangeEndContext<'input>) { self.visit_children(ctx) }

	/**
	 * Visit a parse tree produced by {@link LuceneParser#rangeBound}.
	 * @param ctx the parse tree
	 */
	fn visit_rangeBound(&mut self, ctx: &RangeBoundContext<'input>) { self.visit_children(ctx) }

}

pub trait LuceneParserVisitorCompat<'input>:ParseTreeVisitorCompat<'input, Node= LuceneParserContextType>{
	/**
	 * Visit a parse tree produced by {@link LuceneParser#topLevelQuery}.
	 * @param ctx the parse tree
	 */
		fn visit_topLevelQuery(&mut self, ctx: &TopLevelQueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#query}.
	 * @param ctx the parse tree
	 */
		fn visit_query(&mut self, ctx: &QueryContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#conjoinedClause}.
	 * @param ctx the parse tree
	 */
		fn visit_conjoinedClause(&mut self, ctx: &ConjoinedClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#conjunction}.
	 * @param ctx the parse tree
	 */
		fn visit_conjunction(&mut self, ctx: &ConjunctionContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#modifier}.
	 * @param ctx the parse tree
	 */
		fn visit_modifier(&mut self, ctx: &ModifierContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#modClause}.
	 * @param ctx the parse tree
	 */
		fn visit_modClause(&mut self, ctx: &ModClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#clause}.
	 * @param ctx the parse tree
	 */
		fn visit_clause(&mut self, ctx: &ClauseContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#fieldName}.
	 * @param ctx the parse tree
	 */
		fn visit_fieldName(&mut self, ctx: &FieldNameContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#group}.
	 * @param ctx the parse tree
	 */
		fn visit_group(&mut self, ctx: &GroupContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#boost}.
	 * @param ctx the parse tree
	 */
		fn visit_boost(&mut self, ctx: &BoostContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code plainTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
		fn visit_plainTerm(&mut self, ctx: &PlainTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code starTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
		fn visit_starTerm(&mut self, ctx: &StarTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code prefixTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
		fn visit_prefixTerm(&mut self, ctx: &PrefixTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code wildcardTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
		fn visit_wildcardTerm(&mut self, ctx: &WildcardTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code regexpTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
		fn visit_regexpTerm(&mut self, ctx: &RegexpTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code bareOperatorTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
		fn visit_bareOperatorTerm(&mut self, ctx: &BareOperatorTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code quotedTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
		fn visit_quotedTerm(&mut self, ctx: &QuotedTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by the {@code rangeTerm}
	 * labeled alternative in {@link LuceneParser#term}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeTerm(&mut self, ctx: &RangeTermContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#termModifiers}.
	 * @param ctx the parse tree
	 */
		fn visit_termModifiers(&mut self, ctx: &TermModifiersContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#range}.
	 * @param ctx the parse tree
	 */
		fn visit_range(&mut self, ctx: &RangeContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#rangeStart}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeStart(&mut self, ctx: &RangeStartContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#rangeEnd}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeEnd(&mut self, ctx: &RangeEndContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

	/**
	 * Visit a parse tree produced by {@link LuceneParser#rangeBound}.
	 * @param ctx the parse tree
	 */
		fn visit_rangeBound(&mut self, ctx: &RangeBoundContext<'input>) -> Self::Return {
			self.visit_children(ctx)
		}

}

impl<'input,T> LuceneParserVisitor<'input> for T
where
	T: LuceneParserVisitorCompat<'input>
{
	fn visit_topLevelQuery(&mut self, ctx: &TopLevelQueryContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_topLevelQuery(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_query(&mut self, ctx: &QueryContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_query(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_conjoinedClause(&mut self, ctx: &ConjoinedClauseContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_conjoinedClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_conjunction(&mut self, ctx: &ConjunctionContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_conjunction(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_modifier(&mut self, ctx: &ModifierContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_modifier(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_modClause(&mut self, ctx: &ModClauseContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_modClause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_clause(&mut self, ctx: &ClauseContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_clause(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_fieldName(&mut self, ctx: &FieldNameContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_fieldName(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_group(&mut self, ctx: &GroupContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_group(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_boost(&mut self, ctx: &BoostContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_boost(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_plainTerm(&mut self, ctx: &PlainTermContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_plainTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_starTerm(&mut self, ctx: &StarTermContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_starTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_prefixTerm(&mut self, ctx: &PrefixTermContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_prefixTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_wildcardTerm(&mut self, ctx: &WildcardTermContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_wildcardTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_regexpTerm(&mut self, ctx: &RegexpTermContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_regexpTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_bareOperatorTerm(&mut self, ctx: &BareOperatorTermContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_bareOperatorTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_quotedTerm(&mut self, ctx: &QuotedTermContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_quotedTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeTerm(&mut self, ctx: &RangeTermContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_rangeTerm(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_termModifiers(&mut self, ctx: &TermModifiersContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_termModifiers(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_range(&mut self, ctx: &RangeContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_range(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeStart(&mut self, ctx: &RangeStartContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_rangeStart(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeEnd(&mut self, ctx: &RangeEndContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_rangeEnd(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

	fn visit_rangeBound(&mut self, ctx: &RangeBoundContext<'input>){
		let result = <Self as LuceneParserVisitorCompat>::visit_rangeBound(self, ctx);
        *<Self as ParseTreeVisitorCompat>::temp_result(self) = result;
	}

}
