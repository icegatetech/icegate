#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]
// Generated from LuceneParser.g4 by ANTLR 4.13.2
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(nonstandard_style)]
#![allow(unused_imports)]
#![allow(unused_mut)]
#![allow(unused_braces)]
use antlr4rust::PredictionContextCache;
use antlr4rust::parser::{Parser, BaseParser, ParserRecog, ParserNodeType};
use antlr4rust::token_stream::TokenStream;
use antlr4rust::TokenSource;
use antlr4rust::parser_atn_simulator::ParserATNSimulator;
use antlr4rust::errors::*;
use antlr4rust::rule_context::{BaseRuleContext, CustomRuleContext, RuleContext};
use antlr4rust::recognizer::{Recognizer,Actions};
use antlr4rust::atn_deserializer::ATNDeserializer;
use antlr4rust::dfa::DFA;
use antlr4rust::atn::{ATN, INVALID_ALT};
use antlr4rust::error_strategy::{ErrorStrategy, DefaultErrorStrategy};
use antlr4rust::parser_rule_context::{BaseParserRuleContext, ParserRuleContext,cast,cast_mut};
use antlr4rust::tree::*;
use antlr4rust::token::{TOKEN_EOF,OwningToken,Token};
use antlr4rust::int_stream::EOF;
use antlr4rust::vocabulary::{Vocabulary,VocabularyImpl};
use antlr4rust::token_factory::{CommonTokenFactory,TokenFactory, TokenAware};
use super::luceneparserlistener::*;
use super::luceneparservisitor::*;

use antlr4rust::lazy_static;
use antlr4rust::{TidAble,TidExt};

use std::marker::PhantomData;
use std::sync::Arc;
use std::rc::Rc;
use std::convert::TryFrom;
use std::cell::RefCell;
use std::ops::{DerefMut, Deref};
use std::borrow::{Borrow,BorrowMut};
use std::any::{Any,TypeId};

		pub const LuceneParser_DEFAULT_SKIP:i32=1; 
		pub const LuceneParser_AND:i32=2; 
		pub const LuceneParser_OR:i32=3; 
		pub const LuceneParser_NOT:i32=4; 
		pub const LuceneParser_BAREOPER:i32=5; 
		pub const LuceneParser_PLUS:i32=6; 
		pub const LuceneParser_MINUS:i32=7; 
		pub const LuceneParser_LPAREN:i32=8; 
		pub const LuceneParser_RPAREN:i32=9; 
		pub const LuceneParser_COLON:i32=10; 
		pub const LuceneParser_STAR:i32=11; 
		pub const LuceneParser_CARAT:i32=12; 
		pub const LuceneParser_QUOTED:i32=13; 
		pub const LuceneParser_TERM:i32=14; 
		pub const LuceneParser_FUZZY_SLOP:i32=15; 
		pub const LuceneParser_PREFIXTERM:i32=16; 
		pub const LuceneParser_WILDTERM:i32=17; 
		pub const LuceneParser_REGEXPTERM:i32=18; 
		pub const LuceneParser_RANGEIN_START:i32=19; 
		pub const LuceneParser_RANGEEX_START:i32=20; 
		pub const LuceneParser_NUMBER:i32=21; 
		pub const LuceneParser_RANGE_SKIP:i32=22; 
		pub const LuceneParser_RANGE_TO:i32=23; 
		pub const LuceneParser_RANGEIN_END:i32=24; 
		pub const LuceneParser_RANGEEX_END:i32=25; 
		pub const LuceneParser_RANGE_QUOTED:i32=26; 
		pub const LuceneParser_RANGE_GOOP:i32=27;
	pub const LuceneParser_EOF:i32=EOF;
	pub const RULE_topLevelQuery:usize = 0; 
	pub const RULE_query:usize = 1; 
	pub const RULE_conjoinedClause:usize = 2; 
	pub const RULE_conjunction:usize = 3; 
	pub const RULE_modifier:usize = 4; 
	pub const RULE_modClause:usize = 5; 
	pub const RULE_clause:usize = 6; 
	pub const RULE_fieldName:usize = 7; 
	pub const RULE_group:usize = 8; 
	pub const RULE_boost:usize = 9; 
	pub const RULE_term:usize = 10; 
	pub const RULE_termModifiers:usize = 11; 
	pub const RULE_range:usize = 12; 
	pub const RULE_rangeStart:usize = 13; 
	pub const RULE_rangeEnd:usize = 14; 
	pub const RULE_rangeBound:usize = 15;
	pub const ruleNames: [&'static str; 16] =  [
		"topLevelQuery", "query", "conjoinedClause", "conjunction", "modifier", 
		"modClause", "clause", "fieldName", "group", "boost", "term", "termModifiers", 
		"range", "rangeStart", "rangeEnd", "rangeBound"
	];


	pub const _LITERAL_NAMES: [Option<&'static str>;26] = [
		None, None, None, None, None, None, Some("'+'"), Some("'-'"), Some("'('"), 
		Some("')'"), Some("':'"), Some("'*'"), Some("'^'"), None, None, None, 
		None, None, None, Some("'['"), Some("'{'"), None, None, Some("'TO'"), 
		Some("']'"), Some("'}'")
	];
	pub const _SYMBOLIC_NAMES: [Option<&'static str>;28]  = [
		None, Some("DEFAULT_SKIP"), Some("AND"), Some("OR"), Some("NOT"), Some("BAREOPER"), 
		Some("PLUS"), Some("MINUS"), Some("LPAREN"), Some("RPAREN"), Some("COLON"), 
		Some("STAR"), Some("CARAT"), Some("QUOTED"), Some("TERM"), Some("FUZZY_SLOP"), 
		Some("PREFIXTERM"), Some("WILDTERM"), Some("REGEXPTERM"), Some("RANGEIN_START"), 
		Some("RANGEEX_START"), Some("NUMBER"), Some("RANGE_SKIP"), Some("RANGE_TO"), 
		Some("RANGEIN_END"), Some("RANGEEX_END"), Some("RANGE_QUOTED"), Some("RANGE_GOOP")
	];
	lazy_static!{
	    static ref _shared_context_cache: Arc<PredictionContextCache> = Arc::new(PredictionContextCache::new());
		static ref VOCABULARY: Box<dyn Vocabulary> = Box::new(VocabularyImpl::new(_LITERAL_NAMES.iter(), _SYMBOLIC_NAMES.iter(), None));
	}


type BaseParserType<'input, I> =
	BaseParser<'input,LuceneParserExt<'input>, I, LuceneParserContextType , dyn LuceneParserListener<'input> + 'input >;

type TokenType<'input> = <LocalTokenFactory<'input> as TokenFactory<'input>>::Tok;
pub type LocalTokenFactory<'input> = CommonTokenFactory;

pub type LuceneParserTreeWalker<'input,'a> =
	ParseTreeWalker<'input, 'a, LuceneParserContextType , dyn LuceneParserListener<'input> + 'a>;

/// Parser for LuceneParser grammar
pub struct LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	base:BaseParserType<'input,I>,
	interpreter:Arc<ParserATNSimulator>,
	_shared_context_cache: Box<PredictionContextCache>,
    pub err_handler: Box<dyn ErrorStrategy<'input,BaseParserType<'input,I> > >,
}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    pub fn set_error_strategy(&mut self, strategy: Box<dyn ErrorStrategy<'input,BaseParserType<'input,I> > >) {
        self.err_handler = strategy
    }

    pub fn with_strategy(input: I, strategy: Box<dyn ErrorStrategy<'input,BaseParserType<'input,I> > >) -> Self {
		antlr4rust::recognizer::check_version("0","5");
		let interpreter = Arc::new(ParserATNSimulator::new(
			_ATN.clone(),
			_decision_to_DFA.clone(),
			_shared_context_cache.clone(),
		));
		Self {
			base: BaseParser::new_base_parser(
				input,
				Arc::clone(&interpreter),
				LuceneParserExt{
					_pd: Default::default(),
				}
			),
			interpreter,
            _shared_context_cache: Box::new(PredictionContextCache::new()),
            err_handler: strategy,
        }
    }

}

type DynStrategy<'input,I> = Box<dyn ErrorStrategy<'input,BaseParserType<'input,I>> + 'input>;

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    pub fn with_dyn_strategy(input: I) -> Self{
    	Self::with_strategy(input,Box::new(DefaultErrorStrategy::new()))
    }
}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    pub fn new(input: I) -> Self{
    	Self::with_strategy(input,Box::new(DefaultErrorStrategy::new()))
    }
}

/// Trait for monomorphized trait object that corresponds to the nodes of parse tree generated for LuceneParser
pub trait LuceneParserContext<'input>:
	for<'x> Listenable<dyn LuceneParserListener<'input> + 'x > + 
	for<'x> Visitable<dyn LuceneParserVisitor<'input> + 'x > + 
	ParserRuleContext<'input, TF=LocalTokenFactory<'input>, Ctx=LuceneParserContextType>
{}

antlr4rust::coerce_from!{ 'input : LuceneParserContext<'input> }

impl<'input, 'x, T> VisitableDyn<T> for dyn LuceneParserContext<'input> + 'input
where
    T: LuceneParserVisitor<'input> + 'x,
{
    fn accept_dyn(&self, visitor: &mut T) {
        self.accept(visitor as &mut (dyn LuceneParserVisitor<'input> + 'x))
    }
}

impl<'input> LuceneParserContext<'input> for TerminalNode<'input,LuceneParserContextType> {}
impl<'input> LuceneParserContext<'input> for ErrorNode<'input,LuceneParserContextType> {}

antlr4rust::tid! { impl<'input> TidAble<'input> for dyn LuceneParserContext<'input> + 'input }

antlr4rust::tid! { impl<'input> TidAble<'input> for dyn LuceneParserListener<'input> + 'input }

pub struct LuceneParserContextType;
antlr4rust::tid!{LuceneParserContextType}

impl<'input> ParserNodeType<'input> for LuceneParserContextType{
	type TF = LocalTokenFactory<'input>;
	type Type = dyn LuceneParserContext<'input> + 'input;
}

impl<'input, I> Deref for LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    type Target = BaseParserType<'input,I>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'input, I> DerefMut for LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub struct LuceneParserExt<'input>{
	_pd: PhantomData<&'input str>,
}

impl<'input> LuceneParserExt<'input>{
}
antlr4rust::tid! { LuceneParserExt<'a> }

impl<'input> TokenAware<'input> for LuceneParserExt<'input>{
	type TF = LocalTokenFactory<'input>;
}

impl<'input,I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>> ParserRecog<'input, BaseParserType<'input,I>> for LuceneParserExt<'input>{}

impl<'input,I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>> Actions<'input, BaseParserType<'input,I>> for LuceneParserExt<'input>{
	fn get_grammar_file_name(&self) -> & str{ "LuceneParser.g4"}

   	fn get_rule_names(&self) -> &[& str] {&ruleNames}

   	fn get_vocabulary(&self) -> &dyn Vocabulary { &**VOCABULARY }
}
//------------------- topLevelQuery ----------------
pub type TopLevelQueryContextAll<'input> = TopLevelQueryContext<'input>;


pub type TopLevelQueryContext<'input> = BaseParserRuleContext<'input,TopLevelQueryContextExt<'input>>;

#[derive(Clone)]
pub struct TopLevelQueryContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for TopLevelQueryContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for TopLevelQueryContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_topLevelQuery(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_topLevelQuery(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for TopLevelQueryContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_topLevelQuery(self);
	}
}

impl<'input> CustomRuleContext<'input> for TopLevelQueryContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_topLevelQuery }
	//fn type_rule_index() -> usize where Self: Sized { RULE_topLevelQuery }
}
antlr4rust::tid!{TopLevelQueryContextExt<'a>}

impl<'input> TopLevelQueryContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<TopLevelQueryContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,TopLevelQueryContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait TopLevelQueryContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<TopLevelQueryContextExt<'input>>{

fn query(&self) -> Option<Rc<QueryContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token EOF
/// Returns `None` if there is no child corresponding to token EOF
fn EOF(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_EOF, 0)
}

}

impl<'input> TopLevelQueryContextAttrs<'input> for TopLevelQueryContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn topLevelQuery(&mut self,)
	-> Result<Rc<TopLevelQueryContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = TopLevelQueryContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 0, RULE_topLevelQuery);
        let mut _localctx: Rc<TopLevelQueryContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule query*/
			recog.base.set_state(32);
			recog.query()?;

			recog.base.set_state(33);
			recog.base.match_token(LuceneParser_EOF,&mut recog.err_handler)?;

			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- query ----------------
pub type QueryContextAll<'input> = QueryContext<'input>;


pub type QueryContext<'input> = BaseParserRuleContext<'input,QueryContextExt<'input>>;

#[derive(Clone)]
pub struct QueryContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for QueryContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for QueryContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_query(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_query(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for QueryContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_query(self);
	}
}

impl<'input> CustomRuleContext<'input> for QueryContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_query }
	//fn type_rule_index() -> usize where Self: Sized { RULE_query }
}
antlr4rust::tid!{QueryContextExt<'a>}

impl<'input> QueryContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<QueryContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,QueryContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait QueryContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<QueryContextExt<'input>>{

fn modClause(&self) -> Option<Rc<ModClauseContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn conjoinedClause_all(&self) ->  Vec<Rc<ConjoinedClauseContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn conjoinedClause(&self, i: usize) -> Option<Rc<ConjoinedClauseContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}

}

impl<'input> QueryContextAttrs<'input> for QueryContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn query(&mut self,)
	-> Result<Rc<QueryContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = QueryContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 2, RULE_query);
        let mut _localctx: Rc<QueryContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule modClause*/
			recog.base.set_state(35);
			recog.modClause()?;

			recog.base.set_state(39);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			while (((_la) & !0x3f) == 0 && ((1usize << _la) & 2058748) != 0) {
				{
				{
				/*InvokeRule conjoinedClause*/
				recog.base.set_state(36);
				recog.conjoinedClause()?;

				}
				}
				recog.base.set_state(41);
				recog.err_handler.sync(&mut recog.base)?;
				_la = recog.base.input.la(1);
			}
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- conjoinedClause ----------------
pub type ConjoinedClauseContextAll<'input> = ConjoinedClauseContext<'input>;


pub type ConjoinedClauseContext<'input> = BaseParserRuleContext<'input,ConjoinedClauseContextExt<'input>>;

#[derive(Clone)]
pub struct ConjoinedClauseContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for ConjoinedClauseContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for ConjoinedClauseContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_conjoinedClause(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_conjoinedClause(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for ConjoinedClauseContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_conjoinedClause(self);
	}
}

impl<'input> CustomRuleContext<'input> for ConjoinedClauseContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_conjoinedClause }
	//fn type_rule_index() -> usize where Self: Sized { RULE_conjoinedClause }
}
antlr4rust::tid!{ConjoinedClauseContextExt<'a>}

impl<'input> ConjoinedClauseContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ConjoinedClauseContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ConjoinedClauseContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ConjoinedClauseContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<ConjoinedClauseContextExt<'input>>{

fn modClause(&self) -> Option<Rc<ModClauseContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn conjunction(&self) -> Option<Rc<ConjunctionContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> ConjoinedClauseContextAttrs<'input> for ConjoinedClauseContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn conjoinedClause(&mut self,)
	-> Result<Rc<ConjoinedClauseContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ConjoinedClauseContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 4, RULE_conjoinedClause);
        let mut _localctx: Rc<ConjoinedClauseContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(43);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==LuceneParser_AND || _la==LuceneParser_OR {
				{
				/*InvokeRule conjunction*/
				recog.base.set_state(42);
				recog.conjunction()?;

				}
			}

			/*InvokeRule modClause*/
			recog.base.set_state(45);
			recog.modClause()?;

			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- conjunction ----------------
pub type ConjunctionContextAll<'input> = ConjunctionContext<'input>;


pub type ConjunctionContext<'input> = BaseParserRuleContext<'input,ConjunctionContextExt<'input>>;

#[derive(Clone)]
pub struct ConjunctionContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for ConjunctionContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for ConjunctionContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_conjunction(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_conjunction(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for ConjunctionContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_conjunction(self);
	}
}

impl<'input> CustomRuleContext<'input> for ConjunctionContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_conjunction }
	//fn type_rule_index() -> usize where Self: Sized { RULE_conjunction }
}
antlr4rust::tid!{ConjunctionContextExt<'a>}

impl<'input> ConjunctionContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ConjunctionContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ConjunctionContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ConjunctionContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<ConjunctionContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token AND
/// Returns `None` if there is no child corresponding to token AND
fn AND(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_AND, 0)
}
/// Retrieves first TerminalNode corresponding to token OR
/// Returns `None` if there is no child corresponding to token OR
fn OR(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_OR, 0)
}

}

impl<'input> ConjunctionContextAttrs<'input> for ConjunctionContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn conjunction(&mut self,)
	-> Result<Rc<ConjunctionContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ConjunctionContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 6, RULE_conjunction);
        let mut _localctx: Rc<ConjunctionContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(47);
			_la = recog.base.input.la(1);
			if { !(_la==LuceneParser_AND || _la==LuceneParser_OR) } {
				recog.err_handler.recover_inline(&mut recog.base)?;

			}
			else {
				if  recog.base.input.la(1)==TOKEN_EOF { recog.base.matched_eof = true };
				recog.err_handler.report_match(&mut recog.base);
				recog.base.consume(&mut recog.err_handler);
			}
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- modifier ----------------
pub type ModifierContextAll<'input> = ModifierContext<'input>;


pub type ModifierContext<'input> = BaseParserRuleContext<'input,ModifierContextExt<'input>>;

#[derive(Clone)]
pub struct ModifierContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for ModifierContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for ModifierContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_modifier(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_modifier(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for ModifierContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_modifier(self);
	}
}

impl<'input> CustomRuleContext<'input> for ModifierContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_modifier }
	//fn type_rule_index() -> usize where Self: Sized { RULE_modifier }
}
antlr4rust::tid!{ModifierContextExt<'a>}

impl<'input> ModifierContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ModifierContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ModifierContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ModifierContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<ModifierContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token PLUS
/// Returns `None` if there is no child corresponding to token PLUS
fn PLUS(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_PLUS, 0)
}
/// Retrieves first TerminalNode corresponding to token MINUS
/// Returns `None` if there is no child corresponding to token MINUS
fn MINUS(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_MINUS, 0)
}
/// Retrieves first TerminalNode corresponding to token NOT
/// Returns `None` if there is no child corresponding to token NOT
fn NOT(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_NOT, 0)
}

}

impl<'input> ModifierContextAttrs<'input> for ModifierContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn modifier(&mut self,)
	-> Result<Rc<ModifierContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ModifierContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 8, RULE_modifier);
        let mut _localctx: Rc<ModifierContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(49);
			_la = recog.base.input.la(1);
			if { !((((_la) & !0x3f) == 0 && ((1usize << _la) & 208) != 0)) } {
				recog.err_handler.recover_inline(&mut recog.base)?;

			}
			else {
				if  recog.base.input.la(1)==TOKEN_EOF { recog.base.matched_eof = true };
				recog.err_handler.report_match(&mut recog.base);
				recog.base.consume(&mut recog.err_handler);
			}
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- modClause ----------------
pub type ModClauseContextAll<'input> = ModClauseContext<'input>;


pub type ModClauseContext<'input> = BaseParserRuleContext<'input,ModClauseContextExt<'input>>;

#[derive(Clone)]
pub struct ModClauseContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for ModClauseContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for ModClauseContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_modClause(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_modClause(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for ModClauseContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_modClause(self);
	}
}

impl<'input> CustomRuleContext<'input> for ModClauseContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_modClause }
	//fn type_rule_index() -> usize where Self: Sized { RULE_modClause }
}
antlr4rust::tid!{ModClauseContextExt<'a>}

impl<'input> ModClauseContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ModClauseContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ModClauseContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ModClauseContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<ModClauseContextExt<'input>>{

fn clause(&self) -> Option<Rc<ClauseContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn modifier(&self) -> Option<Rc<ModifierContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> ModClauseContextAttrs<'input> for ModClauseContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn modClause(&mut self,)
	-> Result<Rc<ModClauseContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ModClauseContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 10, RULE_modClause);
        let mut _localctx: Rc<ModClauseContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(52);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if (((_la) & !0x3f) == 0 && ((1usize << _la) & 208) != 0) {
				{
				/*InvokeRule modifier*/
				recog.base.set_state(51);
				recog.modifier()?;

				}
			}

			/*InvokeRule clause*/
			recog.base.set_state(54);
			recog.clause()?;

			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- clause ----------------
pub type ClauseContextAll<'input> = ClauseContext<'input>;


pub type ClauseContext<'input> = BaseParserRuleContext<'input,ClauseContextExt<'input>>;

#[derive(Clone)]
pub struct ClauseContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for ClauseContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for ClauseContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_clause(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_clause(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for ClauseContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_clause(self);
	}
}

impl<'input> CustomRuleContext<'input> for ClauseContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_clause }
	//fn type_rule_index() -> usize where Self: Sized { RULE_clause }
}
antlr4rust::tid!{ClauseContextExt<'a>}

impl<'input> ClauseContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ClauseContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ClauseContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ClauseContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<ClauseContextExt<'input>>{

fn term(&self) -> Option<Rc<TermContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn group(&self) -> Option<Rc<GroupContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn fieldName(&self) -> Option<Rc<FieldNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> ClauseContextAttrs<'input> for ClauseContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn clause(&mut self,)
	-> Result<Rc<ClauseContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ClauseContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 12, RULE_clause);
        let mut _localctx: Rc<ClauseContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(57);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(3,&mut recog.base)? {
				x if x == 1=>{
					{
					/*InvokeRule fieldName*/
					recog.base.set_state(56);
					recog.fieldName()?;

					}
				}

				_ => {}
			}
			recog.base.set_state(61);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LuceneParser_BAREOPER |LuceneParser_STAR |LuceneParser_QUOTED |LuceneParser_TERM |
			LuceneParser_PREFIXTERM |LuceneParser_WILDTERM |LuceneParser_REGEXPTERM |
			LuceneParser_RANGEIN_START |LuceneParser_RANGEEX_START 
				=> {
					{
					/*InvokeRule term*/
					recog.base.set_state(59);
					recog.term()?;

					}
				}

			LuceneParser_LPAREN 
				=> {
					{
					/*InvokeRule group*/
					recog.base.set_state(60);
					recog.group()?;

					}
				}

				_ => Err(ANTLRError::NoAltError(NoViableAltError::new(&mut recog.base)))?
			}
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- fieldName ----------------
pub type FieldNameContextAll<'input> = FieldNameContext<'input>;


pub type FieldNameContext<'input> = BaseParserRuleContext<'input,FieldNameContextExt<'input>>;

#[derive(Clone)]
pub struct FieldNameContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for FieldNameContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for FieldNameContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_fieldName(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_fieldName(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for FieldNameContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_fieldName(self);
	}
}

impl<'input> CustomRuleContext<'input> for FieldNameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_fieldName }
	//fn type_rule_index() -> usize where Self: Sized { RULE_fieldName }
}
antlr4rust::tid!{FieldNameContextExt<'a>}

impl<'input> FieldNameContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<FieldNameContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,FieldNameContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait FieldNameContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<FieldNameContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token TERM
/// Returns `None` if there is no child corresponding to token TERM
fn TERM(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_TERM, 0)
}
/// Retrieves first TerminalNode corresponding to token COLON
/// Returns `None` if there is no child corresponding to token COLON
fn COLON(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_COLON, 0)
}
/// Retrieves first TerminalNode corresponding to token STAR
/// Returns `None` if there is no child corresponding to token STAR
fn STAR(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_STAR, 0)
}

}

impl<'input> FieldNameContextAttrs<'input> for FieldNameContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn fieldName(&mut self,)
	-> Result<Rc<FieldNameContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = FieldNameContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 14, RULE_fieldName);
        let mut _localctx: Rc<FieldNameContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(67);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LuceneParser_TERM 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(63);
					recog.base.match_token(LuceneParser_TERM,&mut recog.err_handler)?;

					recog.base.set_state(64);
					recog.base.match_token(LuceneParser_COLON,&mut recog.err_handler)?;

					}
				}

			LuceneParser_STAR 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(65);
					recog.base.match_token(LuceneParser_STAR,&mut recog.err_handler)?;

					recog.base.set_state(66);
					recog.base.match_token(LuceneParser_COLON,&mut recog.err_handler)?;

					}
				}

				_ => Err(ANTLRError::NoAltError(NoViableAltError::new(&mut recog.base)))?
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- group ----------------
pub type GroupContextAll<'input> = GroupContext<'input>;


pub type GroupContext<'input> = BaseParserRuleContext<'input,GroupContextExt<'input>>;

#[derive(Clone)]
pub struct GroupContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for GroupContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for GroupContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_group(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_group(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for GroupContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_group(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_group }
	//fn type_rule_index() -> usize where Self: Sized { RULE_group }
}
antlr4rust::tid!{GroupContextExt<'a>}

impl<'input> GroupContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<GroupContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,GroupContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait GroupContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<GroupContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_LPAREN, 0)
}
fn query(&self) -> Option<Rc<QueryContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_RPAREN, 0)
}
fn boost(&self) -> Option<Rc<BoostContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> GroupContextAttrs<'input> for GroupContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn group(&mut self,)
	-> Result<Rc<GroupContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = GroupContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 16, RULE_group);
        let mut _localctx: Rc<GroupContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(69);
			recog.base.match_token(LuceneParser_LPAREN,&mut recog.err_handler)?;

			/*InvokeRule query*/
			recog.base.set_state(70);
			recog.query()?;

			recog.base.set_state(71);
			recog.base.match_token(LuceneParser_RPAREN,&mut recog.err_handler)?;

			recog.base.set_state(73);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==LuceneParser_CARAT {
				{
				/*InvokeRule boost*/
				recog.base.set_state(72);
				recog.boost()?;

				}
			}

			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- boost ----------------
pub type BoostContextAll<'input> = BoostContext<'input>;


pub type BoostContext<'input> = BaseParserRuleContext<'input,BoostContextExt<'input>>;

#[derive(Clone)]
pub struct BoostContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for BoostContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for BoostContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_boost(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_boost(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for BoostContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_boost(self);
	}
}

impl<'input> CustomRuleContext<'input> for BoostContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_boost }
	//fn type_rule_index() -> usize where Self: Sized { RULE_boost }
}
antlr4rust::tid!{BoostContextExt<'a>}

impl<'input> BoostContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<BoostContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,BoostContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait BoostContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<BoostContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token CARAT
/// Returns `None` if there is no child corresponding to token CARAT
fn CARAT(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_CARAT, 0)
}
/// Retrieves first TerminalNode corresponding to token NUMBER
/// Returns `None` if there is no child corresponding to token NUMBER
fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_NUMBER, 0)
}

}

impl<'input> BoostContextAttrs<'input> for BoostContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn boost(&mut self,)
	-> Result<Rc<BoostContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = BoostContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 18, RULE_boost);
        let mut _localctx: Rc<BoostContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(75);
			recog.base.match_token(LuceneParser_CARAT,&mut recog.err_handler)?;

			recog.base.set_state(76);
			recog.base.match_token(LuceneParser_NUMBER,&mut recog.err_handler)?;

			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- term ----------------
#[derive(Debug)]
pub enum TermContextAll<'input>{
	StarTermContext(StarTermContext<'input>),
	PrefixTermContext(PrefixTermContext<'input>),
	PlainTermContext(PlainTermContext<'input>),
	RangeTermContext(RangeTermContext<'input>),
	WildcardTermContext(WildcardTermContext<'input>),
	BareOperatorTermContext(BareOperatorTermContext<'input>),
	QuotedTermContext(QuotedTermContext<'input>),
	RegexpTermContext(RegexpTermContext<'input>),
Error(TermContext<'input>)
}
antlr4rust::tid!{TermContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for TermContextAll<'input>{}

impl<'input> LuceneParserContext<'input> for TermContextAll<'input>{}

impl<'input> Deref for TermContextAll<'input>{
	type Target = dyn TermContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use TermContextAll::*;
		match self{
			StarTermContext(inner) => inner,
			PrefixTermContext(inner) => inner,
			PlainTermContext(inner) => inner,
			RangeTermContext(inner) => inner,
			WildcardTermContext(inner) => inner,
			BareOperatorTermContext(inner) => inner,
			QuotedTermContext(inner) => inner,
			RegexpTermContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for TermContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for TermContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type TermContext<'input> = BaseParserRuleContext<'input,TermContextExt<'input>>;

#[derive(Clone)]
pub struct TermContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for TermContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for TermContext<'input>{
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for TermContext<'input>{
}

impl<'input> CustomRuleContext<'input> for TermContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_term }
	//fn type_rule_index() -> usize where Self: Sized { RULE_term }
}
antlr4rust::tid!{TermContextExt<'a>}

impl<'input> TermContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<TermContextAll<'input>> {
		Rc::new(
		TermContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,TermContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait TermContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<TermContextExt<'input>>{


}

impl<'input> TermContextAttrs<'input> for TermContext<'input>{}

pub type StarTermContext<'input> = BaseParserRuleContext<'input,StarTermContextExt<'input>>;

pub trait StarTermContextAttrs<'input>: LuceneParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token STAR
	/// Returns `None` if there is no child corresponding to token STAR
	fn STAR(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
		self.get_token(LuceneParser_STAR, 0)
	}
	fn termModifiers(&self) -> Option<Rc<TermModifiersContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> StarTermContextAttrs<'input> for StarTermContext<'input>{}

pub struct StarTermContextExt<'input>{
	base:TermContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{StarTermContextExt<'a>}

impl<'input> LuceneParserContext<'input> for StarTermContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for StarTermContext<'input>{
	fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_starTerm(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_starTerm(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for StarTermContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_starTerm(self);
	}
}

impl<'input> CustomRuleContext<'input> for StarTermContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_term }
	//fn type_rule_index() -> usize where Self: Sized { RULE_term }
}

impl<'input> Borrow<TermContextExt<'input>> for StarTermContext<'input>{
	fn borrow(&self) -> &TermContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<TermContextExt<'input>> for StarTermContext<'input>{
	fn borrow_mut(&mut self) -> &mut TermContextExt<'input> { &mut self.base }
}

impl<'input> TermContextAttrs<'input> for StarTermContext<'input> {}

impl<'input> StarTermContextExt<'input>{
	fn new(ctx: &dyn TermContextAttrs<'input>) -> Rc<TermContextAll<'input>>  {
		Rc::new(
			TermContextAll::StarTermContext(
				BaseParserRuleContext::copy_from(ctx,StarTermContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type PrefixTermContext<'input> = BaseParserRuleContext<'input,PrefixTermContextExt<'input>>;

pub trait PrefixTermContextAttrs<'input>: LuceneParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token PREFIXTERM
	/// Returns `None` if there is no child corresponding to token PREFIXTERM
	fn PREFIXTERM(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
		self.get_token(LuceneParser_PREFIXTERM, 0)
	}
	fn termModifiers(&self) -> Option<Rc<TermModifiersContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> PrefixTermContextAttrs<'input> for PrefixTermContext<'input>{}

pub struct PrefixTermContextExt<'input>{
	base:TermContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{PrefixTermContextExt<'a>}

impl<'input> LuceneParserContext<'input> for PrefixTermContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for PrefixTermContext<'input>{
	fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_prefixTerm(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_prefixTerm(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for PrefixTermContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_prefixTerm(self);
	}
}

impl<'input> CustomRuleContext<'input> for PrefixTermContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_term }
	//fn type_rule_index() -> usize where Self: Sized { RULE_term }
}

impl<'input> Borrow<TermContextExt<'input>> for PrefixTermContext<'input>{
	fn borrow(&self) -> &TermContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<TermContextExt<'input>> for PrefixTermContext<'input>{
	fn borrow_mut(&mut self) -> &mut TermContextExt<'input> { &mut self.base }
}

impl<'input> TermContextAttrs<'input> for PrefixTermContext<'input> {}

impl<'input> PrefixTermContextExt<'input>{
	fn new(ctx: &dyn TermContextAttrs<'input>) -> Rc<TermContextAll<'input>>  {
		Rc::new(
			TermContextAll::PrefixTermContext(
				BaseParserRuleContext::copy_from(ctx,PrefixTermContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type PlainTermContext<'input> = BaseParserRuleContext<'input,PlainTermContextExt<'input>>;

pub trait PlainTermContextAttrs<'input>: LuceneParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token TERM
	/// Returns `None` if there is no child corresponding to token TERM
	fn TERM(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
		self.get_token(LuceneParser_TERM, 0)
	}
	fn termModifiers(&self) -> Option<Rc<TermModifiersContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> PlainTermContextAttrs<'input> for PlainTermContext<'input>{}

pub struct PlainTermContextExt<'input>{
	base:TermContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{PlainTermContextExt<'a>}

impl<'input> LuceneParserContext<'input> for PlainTermContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for PlainTermContext<'input>{
	fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_plainTerm(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_plainTerm(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for PlainTermContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_plainTerm(self);
	}
}

impl<'input> CustomRuleContext<'input> for PlainTermContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_term }
	//fn type_rule_index() -> usize where Self: Sized { RULE_term }
}

impl<'input> Borrow<TermContextExt<'input>> for PlainTermContext<'input>{
	fn borrow(&self) -> &TermContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<TermContextExt<'input>> for PlainTermContext<'input>{
	fn borrow_mut(&mut self) -> &mut TermContextExt<'input> { &mut self.base }
}

impl<'input> TermContextAttrs<'input> for PlainTermContext<'input> {}

impl<'input> PlainTermContextExt<'input>{
	fn new(ctx: &dyn TermContextAttrs<'input>) -> Rc<TermContextAll<'input>>  {
		Rc::new(
			TermContextAll::PlainTermContext(
				BaseParserRuleContext::copy_from(ctx,PlainTermContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeTermContext<'input> = BaseParserRuleContext<'input,RangeTermContextExt<'input>>;

pub trait RangeTermContextAttrs<'input>: LuceneParserContext<'input>{
	fn range(&self) -> Option<Rc<RangeContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	fn boost(&self) -> Option<Rc<BoostContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> RangeTermContextAttrs<'input> for RangeTermContext<'input>{}

pub struct RangeTermContextExt<'input>{
	base:TermContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeTermContextExt<'a>}

impl<'input> LuceneParserContext<'input> for RangeTermContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for RangeTermContext<'input>{
	fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeTerm(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeTerm(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for RangeTermContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_rangeTerm(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeTermContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_term }
	//fn type_rule_index() -> usize where Self: Sized { RULE_term }
}

impl<'input> Borrow<TermContextExt<'input>> for RangeTermContext<'input>{
	fn borrow(&self) -> &TermContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<TermContextExt<'input>> for RangeTermContext<'input>{
	fn borrow_mut(&mut self) -> &mut TermContextExt<'input> { &mut self.base }
}

impl<'input> TermContextAttrs<'input> for RangeTermContext<'input> {}

impl<'input> RangeTermContextExt<'input>{
	fn new(ctx: &dyn TermContextAttrs<'input>) -> Rc<TermContextAll<'input>>  {
		Rc::new(
			TermContextAll::RangeTermContext(
				BaseParserRuleContext::copy_from(ctx,RangeTermContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type WildcardTermContext<'input> = BaseParserRuleContext<'input,WildcardTermContextExt<'input>>;

pub trait WildcardTermContextAttrs<'input>: LuceneParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token WILDTERM
	/// Returns `None` if there is no child corresponding to token WILDTERM
	fn WILDTERM(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
		self.get_token(LuceneParser_WILDTERM, 0)
	}
	fn termModifiers(&self) -> Option<Rc<TermModifiersContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> WildcardTermContextAttrs<'input> for WildcardTermContext<'input>{}

pub struct WildcardTermContextExt<'input>{
	base:TermContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{WildcardTermContextExt<'a>}

impl<'input> LuceneParserContext<'input> for WildcardTermContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for WildcardTermContext<'input>{
	fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_wildcardTerm(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_wildcardTerm(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for WildcardTermContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_wildcardTerm(self);
	}
}

impl<'input> CustomRuleContext<'input> for WildcardTermContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_term }
	//fn type_rule_index() -> usize where Self: Sized { RULE_term }
}

impl<'input> Borrow<TermContextExt<'input>> for WildcardTermContext<'input>{
	fn borrow(&self) -> &TermContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<TermContextExt<'input>> for WildcardTermContext<'input>{
	fn borrow_mut(&mut self) -> &mut TermContextExt<'input> { &mut self.base }
}

impl<'input> TermContextAttrs<'input> for WildcardTermContext<'input> {}

impl<'input> WildcardTermContextExt<'input>{
	fn new(ctx: &dyn TermContextAttrs<'input>) -> Rc<TermContextAll<'input>>  {
		Rc::new(
			TermContextAll::WildcardTermContext(
				BaseParserRuleContext::copy_from(ctx,WildcardTermContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BareOperatorTermContext<'input> = BaseParserRuleContext<'input,BareOperatorTermContextExt<'input>>;

pub trait BareOperatorTermContextAttrs<'input>: LuceneParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token BAREOPER
	/// Returns `None` if there is no child corresponding to token BAREOPER
	fn BAREOPER(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
		self.get_token(LuceneParser_BAREOPER, 0)
	}
	fn termModifiers(&self) -> Option<Rc<TermModifiersContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BareOperatorTermContextAttrs<'input> for BareOperatorTermContext<'input>{}

pub struct BareOperatorTermContextExt<'input>{
	base:TermContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BareOperatorTermContextExt<'a>}

impl<'input> LuceneParserContext<'input> for BareOperatorTermContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for BareOperatorTermContext<'input>{
	fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_bareOperatorTerm(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_bareOperatorTerm(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for BareOperatorTermContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_bareOperatorTerm(self);
	}
}

impl<'input> CustomRuleContext<'input> for BareOperatorTermContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_term }
	//fn type_rule_index() -> usize where Self: Sized { RULE_term }
}

impl<'input> Borrow<TermContextExt<'input>> for BareOperatorTermContext<'input>{
	fn borrow(&self) -> &TermContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<TermContextExt<'input>> for BareOperatorTermContext<'input>{
	fn borrow_mut(&mut self) -> &mut TermContextExt<'input> { &mut self.base }
}

impl<'input> TermContextAttrs<'input> for BareOperatorTermContext<'input> {}

impl<'input> BareOperatorTermContextExt<'input>{
	fn new(ctx: &dyn TermContextAttrs<'input>) -> Rc<TermContextAll<'input>>  {
		Rc::new(
			TermContextAll::BareOperatorTermContext(
				BaseParserRuleContext::copy_from(ctx,BareOperatorTermContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type QuotedTermContext<'input> = BaseParserRuleContext<'input,QuotedTermContextExt<'input>>;

pub trait QuotedTermContextAttrs<'input>: LuceneParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token QUOTED
	/// Returns `None` if there is no child corresponding to token QUOTED
	fn QUOTED(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
		self.get_token(LuceneParser_QUOTED, 0)
	}
	fn termModifiers(&self) -> Option<Rc<TermModifiersContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> QuotedTermContextAttrs<'input> for QuotedTermContext<'input>{}

pub struct QuotedTermContextExt<'input>{
	base:TermContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{QuotedTermContextExt<'a>}

impl<'input> LuceneParserContext<'input> for QuotedTermContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for QuotedTermContext<'input>{
	fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_quotedTerm(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_quotedTerm(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for QuotedTermContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_quotedTerm(self);
	}
}

impl<'input> CustomRuleContext<'input> for QuotedTermContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_term }
	//fn type_rule_index() -> usize where Self: Sized { RULE_term }
}

impl<'input> Borrow<TermContextExt<'input>> for QuotedTermContext<'input>{
	fn borrow(&self) -> &TermContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<TermContextExt<'input>> for QuotedTermContext<'input>{
	fn borrow_mut(&mut self) -> &mut TermContextExt<'input> { &mut self.base }
}

impl<'input> TermContextAttrs<'input> for QuotedTermContext<'input> {}

impl<'input> QuotedTermContextExt<'input>{
	fn new(ctx: &dyn TermContextAttrs<'input>) -> Rc<TermContextAll<'input>>  {
		Rc::new(
			TermContextAll::QuotedTermContext(
				BaseParserRuleContext::copy_from(ctx,QuotedTermContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RegexpTermContext<'input> = BaseParserRuleContext<'input,RegexpTermContextExt<'input>>;

pub trait RegexpTermContextAttrs<'input>: LuceneParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token REGEXPTERM
	/// Returns `None` if there is no child corresponding to token REGEXPTERM
	fn REGEXPTERM(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
		self.get_token(LuceneParser_REGEXPTERM, 0)
	}
	fn termModifiers(&self) -> Option<Rc<TermModifiersContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> RegexpTermContextAttrs<'input> for RegexpTermContext<'input>{}

pub struct RegexpTermContextExt<'input>{
	base:TermContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RegexpTermContextExt<'a>}

impl<'input> LuceneParserContext<'input> for RegexpTermContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for RegexpTermContext<'input>{
	fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_regexpTerm(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_regexpTerm(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for RegexpTermContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_regexpTerm(self);
	}
}

impl<'input> CustomRuleContext<'input> for RegexpTermContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_term }
	//fn type_rule_index() -> usize where Self: Sized { RULE_term }
}

impl<'input> Borrow<TermContextExt<'input>> for RegexpTermContext<'input>{
	fn borrow(&self) -> &TermContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<TermContextExt<'input>> for RegexpTermContext<'input>{
	fn borrow_mut(&mut self) -> &mut TermContextExt<'input> { &mut self.base }
}

impl<'input> TermContextAttrs<'input> for RegexpTermContext<'input> {}

impl<'input> RegexpTermContextExt<'input>{
	fn new(ctx: &dyn TermContextAttrs<'input>) -> Rc<TermContextAll<'input>>  {
		Rc::new(
			TermContextAll::RegexpTermContext(
				BaseParserRuleContext::copy_from(ctx,RegexpTermContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn term(&mut self,)
	-> Result<Rc<TermContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = TermContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 20, RULE_term);
        let mut _localctx: Rc<TermContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(96);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LuceneParser_TERM 
				=> {
					let tmp = PlainTermContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(78);
					recog.base.match_token(LuceneParser_TERM,&mut recog.err_handler)?;

					/*InvokeRule termModifiers*/
					recog.base.set_state(79);
					recog.termModifiers()?;

					}
				}

			LuceneParser_STAR 
				=> {
					let tmp = StarTermContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(80);
					recog.base.match_token(LuceneParser_STAR,&mut recog.err_handler)?;

					/*InvokeRule termModifiers*/
					recog.base.set_state(81);
					recog.termModifiers()?;

					}
				}

			LuceneParser_PREFIXTERM 
				=> {
					let tmp = PrefixTermContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					recog.base.set_state(82);
					recog.base.match_token(LuceneParser_PREFIXTERM,&mut recog.err_handler)?;

					/*InvokeRule termModifiers*/
					recog.base.set_state(83);
					recog.termModifiers()?;

					}
				}

			LuceneParser_WILDTERM 
				=> {
					let tmp = WildcardTermContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 4)?;
					_localctx = tmp;
					{
					recog.base.set_state(84);
					recog.base.match_token(LuceneParser_WILDTERM,&mut recog.err_handler)?;

					/*InvokeRule termModifiers*/
					recog.base.set_state(85);
					recog.termModifiers()?;

					}
				}

			LuceneParser_REGEXPTERM 
				=> {
					let tmp = RegexpTermContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 5)?;
					_localctx = tmp;
					{
					recog.base.set_state(86);
					recog.base.match_token(LuceneParser_REGEXPTERM,&mut recog.err_handler)?;

					/*InvokeRule termModifiers*/
					recog.base.set_state(87);
					recog.termModifiers()?;

					}
				}

			LuceneParser_BAREOPER 
				=> {
					let tmp = BareOperatorTermContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 6)?;
					_localctx = tmp;
					{
					recog.base.set_state(88);
					recog.base.match_token(LuceneParser_BAREOPER,&mut recog.err_handler)?;

					/*InvokeRule termModifiers*/
					recog.base.set_state(89);
					recog.termModifiers()?;

					}
				}

			LuceneParser_QUOTED 
				=> {
					let tmp = QuotedTermContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 7)?;
					_localctx = tmp;
					{
					recog.base.set_state(90);
					recog.base.match_token(LuceneParser_QUOTED,&mut recog.err_handler)?;

					/*InvokeRule termModifiers*/
					recog.base.set_state(91);
					recog.termModifiers()?;

					}
				}

			LuceneParser_RANGEIN_START |LuceneParser_RANGEEX_START 
				=> {
					let tmp = RangeTermContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 8)?;
					_localctx = tmp;
					{
					/*InvokeRule range*/
					recog.base.set_state(92);
					recog.range()?;

					recog.base.set_state(94);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LuceneParser_CARAT {
						{
						/*InvokeRule boost*/
						recog.base.set_state(93);
						recog.boost()?;

						}
					}

					}
				}

				_ => Err(ANTLRError::NoAltError(NoViableAltError::new(&mut recog.base)))?
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- termModifiers ----------------
pub type TermModifiersContextAll<'input> = TermModifiersContext<'input>;


pub type TermModifiersContext<'input> = BaseParserRuleContext<'input,TermModifiersContextExt<'input>>;

#[derive(Clone)]
pub struct TermModifiersContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for TermModifiersContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for TermModifiersContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_termModifiers(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_termModifiers(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for TermModifiersContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_termModifiers(self);
	}
}

impl<'input> CustomRuleContext<'input> for TermModifiersContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_termModifiers }
	//fn type_rule_index() -> usize where Self: Sized { RULE_termModifiers }
}
antlr4rust::tid!{TermModifiersContextExt<'a>}

impl<'input> TermModifiersContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<TermModifiersContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,TermModifiersContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait TermModifiersContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<TermModifiersContextExt<'input>>{

fn boost(&self) -> Option<Rc<BoostContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token FUZZY_SLOP
/// Returns `None` if there is no child corresponding to token FUZZY_SLOP
fn FUZZY_SLOP(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_FUZZY_SLOP, 0)
}

}

impl<'input> TermModifiersContextAttrs<'input> for TermModifiersContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn termModifiers(&mut self,)
	-> Result<Rc<TermModifiersContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = TermModifiersContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 22, RULE_termModifiers);
        let mut _localctx: Rc<TermModifiersContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(107);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LuceneParser_CARAT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule boost*/
					recog.base.set_state(98);
					recog.boost()?;

					recog.base.set_state(100);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LuceneParser_FUZZY_SLOP {
						{
						recog.base.set_state(99);
						recog.base.match_token(LuceneParser_FUZZY_SLOP,&mut recog.err_handler)?;

						}
					}

					}
				}

			LuceneParser_FUZZY_SLOP 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(102);
					recog.base.match_token(LuceneParser_FUZZY_SLOP,&mut recog.err_handler)?;

					recog.base.set_state(104);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LuceneParser_CARAT {
						{
						/*InvokeRule boost*/
						recog.base.set_state(103);
						recog.boost()?;

						}
					}

					}
				}

			LuceneParser_EOF |LuceneParser_AND |LuceneParser_OR |LuceneParser_NOT |
			LuceneParser_BAREOPER |LuceneParser_PLUS |LuceneParser_MINUS |LuceneParser_LPAREN |
			LuceneParser_RPAREN |LuceneParser_STAR |LuceneParser_QUOTED |LuceneParser_TERM |
			LuceneParser_PREFIXTERM |LuceneParser_WILDTERM |LuceneParser_REGEXPTERM |
			LuceneParser_RANGEIN_START |LuceneParser_RANGEEX_START 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					}
				}

				_ => Err(ANTLRError::NoAltError(NoViableAltError::new(&mut recog.base)))?
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- range ----------------
pub type RangeContextAll<'input> = RangeContext<'input>;


pub type RangeContext<'input> = BaseParserRuleContext<'input,RangeContextExt<'input>>;

#[derive(Clone)]
pub struct RangeContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for RangeContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for RangeContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_range(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_range(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for RangeContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_range(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_range }
	//fn type_rule_index() -> usize where Self: Sized { RULE_range }
}
antlr4rust::tid!{RangeContextExt<'a>}

impl<'input> RangeContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RangeContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<RangeContextExt<'input>>{

fn rangeStart(&self) -> Option<Rc<RangeStartContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn rangeBound_all(&self) ->  Vec<Rc<RangeBoundContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn rangeBound(&self, i: usize) -> Option<Rc<RangeBoundContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves first TerminalNode corresponding to token RANGE_TO
/// Returns `None` if there is no child corresponding to token RANGE_TO
fn RANGE_TO(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_RANGE_TO, 0)
}
fn rangeEnd(&self) -> Option<Rc<RangeEndContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> RangeContextAttrs<'input> for RangeContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn range(&mut self,)
	-> Result<Rc<RangeContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 24, RULE_range);
        let mut _localctx: Rc<RangeContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule rangeStart*/
			recog.base.set_state(109);
			recog.rangeStart()?;

			/*InvokeRule rangeBound*/
			recog.base.set_state(110);
			recog.rangeBound()?;

			recog.base.set_state(111);
			recog.base.match_token(LuceneParser_RANGE_TO,&mut recog.err_handler)?;

			/*InvokeRule rangeBound*/
			recog.base.set_state(112);
			recog.rangeBound()?;

			/*InvokeRule rangeEnd*/
			recog.base.set_state(113);
			recog.rangeEnd()?;

			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- rangeStart ----------------
pub type RangeStartContextAll<'input> = RangeStartContext<'input>;


pub type RangeStartContext<'input> = BaseParserRuleContext<'input,RangeStartContextExt<'input>>;

#[derive(Clone)]
pub struct RangeStartContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for RangeStartContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for RangeStartContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_rangeStart(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_rangeStart(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for RangeStartContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_rangeStart(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeStartContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeStart }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeStart }
}
antlr4rust::tid!{RangeStartContextExt<'a>}

impl<'input> RangeStartContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeStartContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeStartContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RangeStartContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<RangeStartContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token RANGEIN_START
/// Returns `None` if there is no child corresponding to token RANGEIN_START
fn RANGEIN_START(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_RANGEIN_START, 0)
}
/// Retrieves first TerminalNode corresponding to token RANGEEX_START
/// Returns `None` if there is no child corresponding to token RANGEEX_START
fn RANGEEX_START(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_RANGEEX_START, 0)
}

}

impl<'input> RangeStartContextAttrs<'input> for RangeStartContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn rangeStart(&mut self,)
	-> Result<Rc<RangeStartContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeStartContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 26, RULE_rangeStart);
        let mut _localctx: Rc<RangeStartContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(115);
			_la = recog.base.input.la(1);
			if { !(_la==LuceneParser_RANGEIN_START || _la==LuceneParser_RANGEEX_START) } {
				recog.err_handler.recover_inline(&mut recog.base)?;

			}
			else {
				if  recog.base.input.la(1)==TOKEN_EOF { recog.base.matched_eof = true };
				recog.err_handler.report_match(&mut recog.base);
				recog.base.consume(&mut recog.err_handler);
			}
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- rangeEnd ----------------
pub type RangeEndContextAll<'input> = RangeEndContext<'input>;


pub type RangeEndContext<'input> = BaseParserRuleContext<'input,RangeEndContextExt<'input>>;

#[derive(Clone)]
pub struct RangeEndContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for RangeEndContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for RangeEndContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_rangeEnd(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_rangeEnd(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for RangeEndContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_rangeEnd(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeEndContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeEnd }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeEnd }
}
antlr4rust::tid!{RangeEndContextExt<'a>}

impl<'input> RangeEndContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeEndContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeEndContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RangeEndContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<RangeEndContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token RANGEIN_END
/// Returns `None` if there is no child corresponding to token RANGEIN_END
fn RANGEIN_END(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_RANGEIN_END, 0)
}
/// Retrieves first TerminalNode corresponding to token RANGEEX_END
/// Returns `None` if there is no child corresponding to token RANGEEX_END
fn RANGEEX_END(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_RANGEEX_END, 0)
}

}

impl<'input> RangeEndContextAttrs<'input> for RangeEndContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn rangeEnd(&mut self,)
	-> Result<Rc<RangeEndContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeEndContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 28, RULE_rangeEnd);
        let mut _localctx: Rc<RangeEndContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(117);
			_la = recog.base.input.la(1);
			if { !(_la==LuceneParser_RANGEIN_END || _la==LuceneParser_RANGEEX_END) } {
				recog.err_handler.recover_inline(&mut recog.base)?;

			}
			else {
				if  recog.base.input.la(1)==TOKEN_EOF { recog.base.matched_eof = true };
				recog.err_handler.report_match(&mut recog.base);
				recog.base.consume(&mut recog.err_handler);
			}
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
//------------------- rangeBound ----------------
pub type RangeBoundContextAll<'input> = RangeBoundContext<'input>;


pub type RangeBoundContext<'input> = BaseParserRuleContext<'input,RangeBoundContextExt<'input>>;

#[derive(Clone)]
pub struct RangeBoundContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LuceneParserContext<'input> for RangeBoundContext<'input>{}

impl<'input,'a> Listenable<dyn LuceneParserListener<'input> + 'a> for RangeBoundContext<'input>{
		fn enter(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_rangeBound(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LuceneParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_rangeBound(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LuceneParserVisitor<'input> + 'a> for RangeBoundContext<'input>{
	fn accept(&self,visitor: &mut (dyn LuceneParserVisitor<'input> + 'a)) {
		visitor.visit_rangeBound(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeBoundContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LuceneParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeBound }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeBound }
}
antlr4rust::tid!{RangeBoundContextExt<'a>}

impl<'input> RangeBoundContextExt<'input>{
	fn new(parent: Option<Rc<dyn LuceneParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeBoundContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeBoundContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RangeBoundContextAttrs<'input>: LuceneParserContext<'input> + BorrowMut<RangeBoundContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token RANGE_GOOP
/// Returns `None` if there is no child corresponding to token RANGE_GOOP
fn RANGE_GOOP(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_RANGE_GOOP, 0)
}
/// Retrieves first TerminalNode corresponding to token RANGE_QUOTED
/// Returns `None` if there is no child corresponding to token RANGE_QUOTED
fn RANGE_QUOTED(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_RANGE_QUOTED, 0)
}
/// Retrieves first TerminalNode corresponding to token RANGE_TO
/// Returns `None` if there is no child corresponding to token RANGE_TO
fn RANGE_TO(&self) -> Option<Rc<TerminalNode<'input,LuceneParserContextType>>> where Self:Sized{
	self.get_token(LuceneParser_RANGE_TO, 0)
}

}

impl<'input> RangeBoundContextAttrs<'input> for RangeBoundContext<'input>{}

impl<'input, I> LuceneParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn rangeBound(&mut self,)
	-> Result<Rc<RangeBoundContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeBoundContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 30, RULE_rangeBound);
        let mut _localctx: Rc<RangeBoundContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(119);
			_la = recog.base.input.la(1);
			if { !((((_la) & !0x3f) == 0 && ((1usize << _la) & 209715200) != 0)) } {
				recog.err_handler.recover_inline(&mut recog.base)?;

			}
			else {
				if  recog.base.input.la(1)==TOKEN_EOF { recog.base.matched_eof = true };
				recog.err_handler.report_match(&mut recog.base);
				recog.base.consume(&mut recog.err_handler);
			}
			}
			Ok(())
		})();
		match result {
		Ok(_)=>{},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re) => {
				//_localctx.exception = re;
				recog.err_handler.report_error(&mut recog.base, re);
				recog.err_handler.recover(&mut recog.base, re)?;
			}
		}
		recog.base.exit_rule()?;

		Ok(_localctx)
	}
}
	lazy_static!{
    static ref _ATN: Arc<ATN> =
        Arc::new(ATNDeserializer::new(None).deserialize(&mut _serializedATN.iter()));
    static ref _decision_to_DFA: Arc<Vec<antlr4rust::RwLock<DFA>>> = {
        let mut dfa = Vec::new();
        let size = _ATN.decision_to_state.len() as i32;
        for i in 0..size {
            dfa.push(DFA::new(
                _ATN.clone(),
                _ATN.get_decision_state(i),
                i,
            ).into())
        }
        Arc::new(dfa)
    };
	static ref _serializedATN: Vec<i32> = vec![
		4, 1, 27, 122, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7, 
		4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 
		7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7, 14, 2, 15, 
		7, 15, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 5, 1, 38, 8, 1, 10, 1, 12, 1, 41, 
		9, 1, 1, 2, 3, 2, 44, 8, 2, 1, 2, 1, 2, 1, 3, 1, 3, 1, 4, 1, 4, 1, 5, 
		3, 5, 53, 8, 5, 1, 5, 1, 5, 1, 6, 3, 6, 58, 8, 6, 1, 6, 1, 6, 3, 6, 62, 
		8, 6, 1, 7, 1, 7, 1, 7, 1, 7, 3, 7, 68, 8, 7, 1, 8, 1, 8, 1, 8, 1, 8, 
		3, 8, 74, 8, 8, 1, 9, 1, 9, 1, 9, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 
		10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 
		10, 3, 10, 95, 8, 10, 3, 10, 97, 8, 10, 1, 11, 1, 11, 3, 11, 101, 8, 11, 
		1, 11, 1, 11, 3, 11, 105, 8, 11, 1, 11, 3, 11, 108, 8, 11, 1, 12, 1, 12, 
		1, 12, 1, 12, 1, 12, 1, 12, 1, 13, 1, 13, 1, 14, 1, 14, 1, 15, 1, 15, 
		1, 15, 0, 0, 16, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 
		30, 0, 5, 1, 0, 2, 3, 2, 0, 4, 4, 6, 7, 1, 0, 19, 20, 1, 0, 24, 25, 2, 
		0, 23, 23, 26, 27, 124, 0, 32, 1, 0, 0, 0, 2, 35, 1, 0, 0, 0, 4, 43, 1, 
		0, 0, 0, 6, 47, 1, 0, 0, 0, 8, 49, 1, 0, 0, 0, 10, 52, 1, 0, 0, 0, 12, 
		57, 1, 0, 0, 0, 14, 67, 1, 0, 0, 0, 16, 69, 1, 0, 0, 0, 18, 75, 1, 0, 
		0, 0, 20, 96, 1, 0, 0, 0, 22, 107, 1, 0, 0, 0, 24, 109, 1, 0, 0, 0, 26, 
		115, 1, 0, 0, 0, 28, 117, 1, 0, 0, 0, 30, 119, 1, 0, 0, 0, 32, 33, 3, 
		2, 1, 0, 33, 34, 5, 0, 0, 1, 34, 1, 1, 0, 0, 0, 35, 39, 3, 10, 5, 0, 36, 
		38, 3, 4, 2, 0, 37, 36, 1, 0, 0, 0, 38, 41, 1, 0, 0, 0, 39, 37, 1, 0, 
		0, 0, 39, 40, 1, 0, 0, 0, 40, 3, 1, 0, 0, 0, 41, 39, 1, 0, 0, 0, 42, 44, 
		3, 6, 3, 0, 43, 42, 1, 0, 0, 0, 43, 44, 1, 0, 0, 0, 44, 45, 1, 0, 0, 0, 
		45, 46, 3, 10, 5, 0, 46, 5, 1, 0, 0, 0, 47, 48, 7, 0, 0, 0, 48, 7, 1, 
		0, 0, 0, 49, 50, 7, 1, 0, 0, 50, 9, 1, 0, 0, 0, 51, 53, 3, 8, 4, 0, 52, 
		51, 1, 0, 0, 0, 52, 53, 1, 0, 0, 0, 53, 54, 1, 0, 0, 0, 54, 55, 3, 12, 
		6, 0, 55, 11, 1, 0, 0, 0, 56, 58, 3, 14, 7, 0, 57, 56, 1, 0, 0, 0, 57, 
		58, 1, 0, 0, 0, 58, 61, 1, 0, 0, 0, 59, 62, 3, 20, 10, 0, 60, 62, 3, 16, 
		8, 0, 61, 59, 1, 0, 0, 0, 61, 60, 1, 0, 0, 0, 62, 13, 1, 0, 0, 0, 63, 
		64, 5, 14, 0, 0, 64, 68, 5, 10, 0, 0, 65, 66, 5, 11, 0, 0, 66, 68, 5, 
		10, 0, 0, 67, 63, 1, 0, 0, 0, 67, 65, 1, 0, 0, 0, 68, 15, 1, 0, 0, 0, 
		69, 70, 5, 8, 0, 0, 70, 71, 3, 2, 1, 0, 71, 73, 5, 9, 0, 0, 72, 74, 3, 
		18, 9, 0, 73, 72, 1, 0, 0, 0, 73, 74, 1, 0, 0, 0, 74, 17, 1, 0, 0, 0, 
		75, 76, 5, 12, 0, 0, 76, 77, 5, 21, 0, 0, 77, 19, 1, 0, 0, 0, 78, 79, 
		5, 14, 0, 0, 79, 97, 3, 22, 11, 0, 80, 81, 5, 11, 0, 0, 81, 97, 3, 22, 
		11, 0, 82, 83, 5, 16, 0, 0, 83, 97, 3, 22, 11, 0, 84, 85, 5, 17, 0, 0, 
		85, 97, 3, 22, 11, 0, 86, 87, 5, 18, 0, 0, 87, 97, 3, 22, 11, 0, 88, 89, 
		5, 5, 0, 0, 89, 97, 3, 22, 11, 0, 90, 91, 5, 13, 0, 0, 91, 97, 3, 22, 
		11, 0, 92, 94, 3, 24, 12, 0, 93, 95, 3, 18, 9, 0, 94, 93, 1, 0, 0, 0, 
		94, 95, 1, 0, 0, 0, 95, 97, 1, 0, 0, 0, 96, 78, 1, 0, 0, 0, 96, 80, 1, 
		0, 0, 0, 96, 82, 1, 0, 0, 0, 96, 84, 1, 0, 0, 0, 96, 86, 1, 0, 0, 0, 96, 
		88, 1, 0, 0, 0, 96, 90, 1, 0, 0, 0, 96, 92, 1, 0, 0, 0, 97, 21, 1, 0, 
		0, 0, 98, 100, 3, 18, 9, 0, 99, 101, 5, 15, 0, 0, 100, 99, 1, 0, 0, 0, 
		100, 101, 1, 0, 0, 0, 101, 108, 1, 0, 0, 0, 102, 104, 5, 15, 0, 0, 103, 
		105, 3, 18, 9, 0, 104, 103, 1, 0, 0, 0, 104, 105, 1, 0, 0, 0, 105, 108, 
		1, 0, 0, 0, 106, 108, 1, 0, 0, 0, 107, 98, 1, 0, 0, 0, 107, 102, 1, 0, 
		0, 0, 107, 106, 1, 0, 0, 0, 108, 23, 1, 0, 0, 0, 109, 110, 3, 26, 13, 
		0, 110, 111, 3, 30, 15, 0, 111, 112, 5, 23, 0, 0, 112, 113, 3, 30, 15, 
		0, 113, 114, 3, 28, 14, 0, 114, 25, 1, 0, 0, 0, 115, 116, 7, 2, 0, 0, 
		116, 27, 1, 0, 0, 0, 117, 118, 7, 3, 0, 0, 118, 29, 1, 0, 0, 0, 119, 120, 
		7, 4, 0, 0, 120, 31, 1, 0, 0, 0, 12, 39, 43, 52, 57, 61, 67, 73, 94, 96, 
		100, 104, 107
	];
}
