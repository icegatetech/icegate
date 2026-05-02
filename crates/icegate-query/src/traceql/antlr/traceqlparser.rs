#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]
// Generated from antlr/TraceQLParser.g4 by ANTLR 4.13.2
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
use super::traceqlparserlistener::*;
use super::traceqlparservisitor::*;

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

		pub const TraceQLParser_LBRACE:i32=1; 
		pub const TraceQLParser_RBRACE:i32=2; 
		pub const TraceQLParser_LPAREN:i32=3; 
		pub const TraceQLParser_RPAREN:i32=4; 
		pub const TraceQLParser_COMMA:i32=5; 
		pub const TraceQLParser_DOT:i32=6; 
		pub const TraceQLParser_COLON:i32=7; 
		pub const TraceQLParser_PIPE:i32=8; 
		pub const TraceQLParser_EQ_RE:i32=9; 
		pub const TraceQLParser_NEQ_RE:i32=10; 
		pub const TraceQLParser_GE:i32=11; 
		pub const TraceQLParser_LE:i32=12; 
		pub const TraceQLParser_NEQ:i32=13; 
		pub const TraceQLParser_EQ:i32=14; 
		pub const TraceQLParser_GT:i32=15; 
		pub const TraceQLParser_LT:i32=16; 
		pub const TraceQLParser_DESC:i32=17; 
		pub const TraceQLParser_ANC:i32=18; 
		pub const TraceQLParser_SIBLING:i32=19; 
		pub const TraceQLParser_NOT_DESC:i32=20; 
		pub const TraceQLParser_NOT_ANC:i32=21; 
		pub const TraceQLParser_NOT_CHILD:i32=22; 
		pub const TraceQLParser_NOT_PARENT:i32=23; 
		pub const TraceQLParser_AND:i32=24; 
		pub const TraceQLParser_OR:i32=25; 
		pub const TraceQLParser_NOT:i32=26; 
		pub const TraceQLParser_PLUS:i32=27; 
		pub const TraceQLParser_MINUS:i32=28; 
		pub const TraceQLParser_STAR:i32=29; 
		pub const TraceQLParser_SLASH:i32=30; 
		pub const TraceQLParser_PERCENT:i32=31; 
		pub const TraceQLParser_SCOPE_SPAN:i32=32; 
		pub const TraceQLParser_SCOPE_RESOURCE:i32=33; 
		pub const TraceQLParser_SCOPE_EVENT:i32=34; 
		pub const TraceQLParser_SCOPE_LINK:i32=35; 
		pub const TraceQLParser_SCOPE_PARENT:i32=36; 
		pub const TraceQLParser_SCOPE_TRACE:i32=37; 
		pub const TraceQLParser_INTR_NAME:i32=38; 
		pub const TraceQLParser_INTR_STATUS:i32=39; 
		pub const TraceQLParser_INTR_STATUS_MESSAGE:i32=40; 
		pub const TraceQLParser_INTR_KIND:i32=41; 
		pub const TraceQLParser_INTR_DURATION:i32=42; 
		pub const TraceQLParser_INTR_TRACE_DURATION:i32=43; 
		pub const TraceQLParser_INTR_ROOT_NAME:i32=44; 
		pub const TraceQLParser_INTR_ROOT_SVC:i32=45; 
		pub const TraceQLParser_INTR_TRACE_ID:i32=46; 
		pub const TraceQLParser_INTR_SPAN_ID:i32=47; 
		pub const TraceQLParser_STATUS_OK:i32=48; 
		pub const TraceQLParser_STATUS_ERROR:i32=49; 
		pub const TraceQLParser_STATUS_UNSET:i32=50; 
		pub const TraceQLParser_KIND_SERVER:i32=51; 
		pub const TraceQLParser_KIND_CLIENT:i32=52; 
		pub const TraceQLParser_KIND_PRODUCER:i32=53; 
		pub const TraceQLParser_KIND_CONSUMER:i32=54; 
		pub const TraceQLParser_KIND_INTERNAL:i32=55; 
		pub const TraceQLParser_FN_COUNT:i32=56; 
		pub const TraceQLParser_FN_SUM:i32=57; 
		pub const TraceQLParser_FN_AVG:i32=58; 
		pub const TraceQLParser_FN_MIN:i32=59; 
		pub const TraceQLParser_FN_MAX:i32=60; 
		pub const TraceQLParser_FN_QUANTILE:i32=61; 
		pub const TraceQLParser_FN_RATE:i32=62; 
		pub const TraceQLParser_FN_COUNT_OVER_TIME:i32=63; 
		pub const TraceQLParser_FN_HISTOGRAM_OVER_TIME:i32=64; 
		pub const TraceQLParser_KW_BY:i32=65; 
		pub const TraceQLParser_KW_TRUE:i32=66; 
		pub const TraceQLParser_KW_FALSE:i32=67; 
		pub const TraceQLParser_KW_NIL:i32=68; 
		pub const TraceQLParser_DURATION:i32=69; 
		pub const TraceQLParser_BYTES:i32=70; 
		pub const TraceQLParser_FLOAT:i32=71; 
		pub const TraceQLParser_INT:i32=72; 
		pub const TraceQLParser_STRING:i32=73; 
		pub const TraceQLParser_IDENT:i32=74; 
		pub const TraceQLParser_WS:i32=75; 
		pub const TraceQLParser_COMMENT:i32=76;
	pub const TraceQLParser_EOF:i32=EOF;
	pub const RULE_root:usize = 0; 
	pub const RULE_pipelineExpr:usize = 1; 
	pub const RULE_pipelineStage:usize = 2; 
	pub const RULE_byClause:usize = 3; 
	pub const RULE_aggregate:usize = 4; 
	pub const RULE_aggregateFilter:usize = 5; 
	pub const RULE_aggregateOp:usize = 6; 
	pub const RULE_metricsFunction:usize = 7; 
	pub const RULE_spansetExpr:usize = 8; 
	pub const RULE_spansetOr:usize = 9; 
	pub const RULE_spansetAnd:usize = 10; 
	pub const RULE_spansetRel:usize = 11; 
	pub const RULE_spansetPrimary:usize = 12; 
	pub const RULE_spansetRelOp:usize = 13; 
	pub const RULE_spanSelector:usize = 14; 
	pub const RULE_spanFilter:usize = 15; 
	pub const RULE_comparisonOp:usize = 16; 
	pub const RULE_fieldRef:usize = 17; 
	pub const RULE_scopedAttribute:usize = 18; 
	pub const RULE_identChain:usize = 19; 
	pub const RULE_identPart:usize = 20; 
	pub const RULE_intrinsic:usize = 21; 
	pub const RULE_literal:usize = 22;
	pub const ruleNames: [&'static str; 23] =  [
		"root", "pipelineExpr", "pipelineStage", "byClause", "aggregate", "aggregateFilter", 
		"aggregateOp", "metricsFunction", "spansetExpr", "spansetOr", "spansetAnd", 
		"spansetRel", "spansetPrimary", "spansetRelOp", "spanSelector", "spanFilter", 
		"comparisonOp", "fieldRef", "scopedAttribute", "identChain", "identPart", 
		"intrinsic", "literal"
	];


	pub const _LITERAL_NAMES: [Option<&'static str>;69] = [
		None, Some("'{'"), Some("'}'"), Some("'('"), Some("')'"), Some("','"), 
		Some("'.'"), Some("':'"), Some("'|'"), Some("'=~'"), Some("'!~'"), Some("'>='"), 
		Some("'<='"), Some("'!='"), Some("'='"), Some("'>'"), Some("'<'"), Some("'>>'"), 
		Some("'<<'"), Some("'~'"), Some("'!>>'"), Some("'!<<'"), Some("'!>'"), 
		Some("'!<'"), Some("'&&'"), Some("'||'"), Some("'!'"), Some("'+'"), Some("'-'"), 
		Some("'*'"), Some("'/'"), Some("'%'"), Some("'span'"), Some("'resource'"), 
		Some("'event'"), Some("'link'"), Some("'parent'"), Some("'trace'"), Some("'name'"), 
		Some("'status'"), Some("'statusMessage'"), Some("'kind'"), Some("'duration'"), 
		Some("'traceDuration'"), Some("'rootName'"), Some("'rootServiceName'"), 
		Some("'traceID'"), Some("'spanID'"), Some("'ok'"), Some("'error'"), Some("'unset'"), 
		Some("'server'"), Some("'client'"), Some("'producer'"), Some("'consumer'"), 
		Some("'internal'"), Some("'count'"), Some("'sum'"), Some("'avg'"), Some("'min'"), 
		Some("'max'"), Some("'quantile_over_time'"), Some("'rate'"), Some("'count_over_time'"), 
		Some("'histogram_over_time'"), Some("'by'"), Some("'true'"), Some("'false'"), 
		Some("'nil'")
	];
	pub const _SYMBOLIC_NAMES: [Option<&'static str>;77]  = [
		None, Some("LBRACE"), Some("RBRACE"), Some("LPAREN"), Some("RPAREN"), 
		Some("COMMA"), Some("DOT"), Some("COLON"), Some("PIPE"), Some("EQ_RE"), 
		Some("NEQ_RE"), Some("GE"), Some("LE"), Some("NEQ"), Some("EQ"), Some("GT"), 
		Some("LT"), Some("DESC"), Some("ANC"), Some("SIBLING"), Some("NOT_DESC"), 
		Some("NOT_ANC"), Some("NOT_CHILD"), Some("NOT_PARENT"), Some("AND"), Some("OR"), 
		Some("NOT"), Some("PLUS"), Some("MINUS"), Some("STAR"), Some("SLASH"), 
		Some("PERCENT"), Some("SCOPE_SPAN"), Some("SCOPE_RESOURCE"), Some("SCOPE_EVENT"), 
		Some("SCOPE_LINK"), Some("SCOPE_PARENT"), Some("SCOPE_TRACE"), Some("INTR_NAME"), 
		Some("INTR_STATUS"), Some("INTR_STATUS_MESSAGE"), Some("INTR_KIND"), Some("INTR_DURATION"), 
		Some("INTR_TRACE_DURATION"), Some("INTR_ROOT_NAME"), Some("INTR_ROOT_SVC"), 
		Some("INTR_TRACE_ID"), Some("INTR_SPAN_ID"), Some("STATUS_OK"), Some("STATUS_ERROR"), 
		Some("STATUS_UNSET"), Some("KIND_SERVER"), Some("KIND_CLIENT"), Some("KIND_PRODUCER"), 
		Some("KIND_CONSUMER"), Some("KIND_INTERNAL"), Some("FN_COUNT"), Some("FN_SUM"), 
		Some("FN_AVG"), Some("FN_MIN"), Some("FN_MAX"), Some("FN_QUANTILE"), Some("FN_RATE"), 
		Some("FN_COUNT_OVER_TIME"), Some("FN_HISTOGRAM_OVER_TIME"), Some("KW_BY"), 
		Some("KW_TRUE"), Some("KW_FALSE"), Some("KW_NIL"), Some("DURATION"), Some("BYTES"), 
		Some("FLOAT"), Some("INT"), Some("STRING"), Some("IDENT"), Some("WS"), 
		Some("COMMENT")
	];
	lazy_static!{
	    static ref _shared_context_cache: Arc<PredictionContextCache> = Arc::new(PredictionContextCache::new());
		static ref VOCABULARY: Box<dyn Vocabulary> = Box::new(VocabularyImpl::new(_LITERAL_NAMES.iter(), _SYMBOLIC_NAMES.iter(), None));
	}


type BaseParserType<'input, I> =
	BaseParser<'input,TraceQLParserExt<'input>, I, TraceQLParserContextType , dyn TraceQLParserListener<'input> + 'input >;

type TokenType<'input> = <LocalTokenFactory<'input> as TokenFactory<'input>>::Tok;
pub type LocalTokenFactory<'input> = CommonTokenFactory;

pub type TraceQLParserTreeWalker<'input,'a> =
	ParseTreeWalker<'input, 'a, TraceQLParserContextType , dyn TraceQLParserListener<'input> + 'a>;

/// Parser for TraceQLParser grammar
pub struct TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	base:BaseParserType<'input,I>,
	interpreter:Arc<ParserATNSimulator>,
	_shared_context_cache: Box<PredictionContextCache>,
    pub err_handler: Box<dyn ErrorStrategy<'input,BaseParserType<'input,I> > >,
}

impl<'input, I> TraceQLParser<'input, I>
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
				TraceQLParserExt{
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

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    pub fn with_dyn_strategy(input: I) -> Self{
    	Self::with_strategy(input,Box::new(DefaultErrorStrategy::new()))
    }
}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    pub fn new(input: I) -> Self{
    	Self::with_strategy(input,Box::new(DefaultErrorStrategy::new()))
    }
}

/// Trait for monomorphized trait object that corresponds to the nodes of parse tree generated for TraceQLParser
pub trait TraceQLParserContext<'input>:
	for<'x> Listenable<dyn TraceQLParserListener<'input> + 'x > + 
	for<'x> Visitable<dyn TraceQLParserVisitor<'input> + 'x > + 
	ParserRuleContext<'input, TF=LocalTokenFactory<'input>, Ctx=TraceQLParserContextType>
{}

antlr4rust::coerce_from!{ 'input : TraceQLParserContext<'input> }

impl<'input, 'x, T> VisitableDyn<T> for dyn TraceQLParserContext<'input> + 'input
where
    T: TraceQLParserVisitor<'input> + 'x,
{
    fn accept_dyn(&self, visitor: &mut T) {
        self.accept(visitor as &mut (dyn TraceQLParserVisitor<'input> + 'x))
    }
}

impl<'input> TraceQLParserContext<'input> for TerminalNode<'input,TraceQLParserContextType> {}
impl<'input> TraceQLParserContext<'input> for ErrorNode<'input,TraceQLParserContextType> {}

antlr4rust::tid! { impl<'input> TidAble<'input> for dyn TraceQLParserContext<'input> + 'input }

antlr4rust::tid! { impl<'input> TidAble<'input> for dyn TraceQLParserListener<'input> + 'input }

pub struct TraceQLParserContextType;
antlr4rust::tid!{TraceQLParserContextType}

impl<'input> ParserNodeType<'input> for TraceQLParserContextType{
	type TF = LocalTokenFactory<'input>;
	type Type = dyn TraceQLParserContext<'input> + 'input;
}

impl<'input, I> Deref for TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    type Target = BaseParserType<'input,I>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'input, I> DerefMut for TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub struct TraceQLParserExt<'input>{
	_pd: PhantomData<&'input str>,
}

impl<'input> TraceQLParserExt<'input>{
}
antlr4rust::tid! { TraceQLParserExt<'a> }

impl<'input> TokenAware<'input> for TraceQLParserExt<'input>{
	type TF = LocalTokenFactory<'input>;
}

impl<'input,I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>> ParserRecog<'input, BaseParserType<'input,I>> for TraceQLParserExt<'input>{}

impl<'input,I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>> Actions<'input, BaseParserType<'input,I>> for TraceQLParserExt<'input>{
	fn get_grammar_file_name(&self) -> & str{ "TraceQLParser.g4"}

   	fn get_rule_names(&self) -> &[& str] {&ruleNames}

   	fn get_vocabulary(&self) -> &dyn Vocabulary { &**VOCABULARY }
	fn sempred(_localctx: Option<&(dyn TraceQLParserContext<'input> + 'input)>, rule_index: i32, pred_index: i32,
			   recog:&mut BaseParserType<'input,I>
	)->bool{
		match rule_index {
					15 => TraceQLParser::<'input,I>::spanFilter_sempred(_localctx.and_then(|x|x.downcast_ref()), pred_index, recog),
			_ => true
		}
	}
}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	fn spanFilter_sempred(_localctx: Option<&SpanFilterContext<'input>>, pred_index:i32,
						recog:&mut <Self as Deref>::Target
		) -> bool {
		match pred_index {
				0=>{
					recog.precpred(None, 3)
				}
				1=>{
					recog.precpred(None, 2)
				}
			_ => true
		}
	}
}
//------------------- root ----------------
pub type RootContextAll<'input> = RootContext<'input>;


pub type RootContext<'input> = BaseParserRuleContext<'input,RootContextExt<'input>>;

#[derive(Clone)]
pub struct RootContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for RootContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for RootContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_root(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_root(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for RootContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_root(self);
	}
}

impl<'input> CustomRuleContext<'input> for RootContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_root }
	//fn type_rule_index() -> usize where Self: Sized { RULE_root }
}
antlr4rust::tid!{RootContextExt<'a>}

impl<'input> RootContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RootContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RootContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RootContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<RootContextExt<'input>>{

fn pipelineExpr(&self) -> Option<Rc<PipelineExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token EOF
/// Returns `None` if there is no child corresponding to token EOF
fn EOF(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_EOF, 0)
}
fn spansetExpr(&self) -> Option<Rc<SpansetExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> RootContextAttrs<'input> for RootContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn root(&mut self,)
	-> Result<Rc<RootContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RootContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 0, RULE_root);
        let mut _localctx: Rc<RootContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(52);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(0,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule pipelineExpr*/
					recog.base.set_state(46);
					recog.pipelineExpr()?;

					recog.base.set_state(47);
					recog.base.match_token(TraceQLParser_EOF,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule spansetExpr*/
					recog.base.set_state(49);
					recog.spansetExpr()?;

					recog.base.set_state(50);
					recog.base.match_token(TraceQLParser_EOF,&mut recog.err_handler)?;

					}
				}

				_ => {}
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
//------------------- pipelineExpr ----------------
pub type PipelineExprContextAll<'input> = PipelineExprContext<'input>;


pub type PipelineExprContext<'input> = BaseParserRuleContext<'input,PipelineExprContextExt<'input>>;

#[derive(Clone)]
pub struct PipelineExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for PipelineExprContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for PipelineExprContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_pipelineExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_pipelineExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for PipelineExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_pipelineExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for PipelineExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_pipelineExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_pipelineExpr }
}
antlr4rust::tid!{PipelineExprContextExt<'a>}

impl<'input> PipelineExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<PipelineExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,PipelineExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait PipelineExprContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<PipelineExprContextExt<'input>>{

fn spansetExpr(&self) -> Option<Rc<SpansetExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves all `TerminalNode`s corresponding to token PIPE in current rule
fn PIPE_all(&self) -> Vec<Rc<TerminalNode<'input,TraceQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token PIPE, starting from 0.
/// Returns `None` if number of children corresponding to token PIPE is less or equal than `i`.
fn PIPE(&self, i: usize) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_PIPE, i)
}
fn pipelineStage_all(&self) ->  Vec<Rc<PipelineStageContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn pipelineStage(&self, i: usize) -> Option<Rc<PipelineStageContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}

}

impl<'input> PipelineExprContextAttrs<'input> for PipelineExprContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn pipelineExpr(&mut self,)
	-> Result<Rc<PipelineExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = PipelineExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 2, RULE_pipelineExpr);
        let mut _localctx: Rc<PipelineExprContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule spansetExpr*/
			recog.base.set_state(54);
			recog.spansetExpr()?;

			recog.base.set_state(57); 
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			loop {
				{
				{
				recog.base.set_state(55);
				recog.base.match_token(TraceQLParser_PIPE,&mut recog.err_handler)?;

				/*InvokeRule pipelineStage*/
				recog.base.set_state(56);
				recog.pipelineStage()?;

				}
				}
				recog.base.set_state(59); 
				recog.err_handler.sync(&mut recog.base)?;
				_la = recog.base.input.la(1);
				if !(_la==TraceQLParser_PIPE) {break}
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
//------------------- pipelineStage ----------------
pub type PipelineStageContextAll<'input> = PipelineStageContext<'input>;


pub type PipelineStageContext<'input> = BaseParserRuleContext<'input,PipelineStageContextExt<'input>>;

#[derive(Clone)]
pub struct PipelineStageContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for PipelineStageContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for PipelineStageContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_pipelineStage(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_pipelineStage(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for PipelineStageContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_pipelineStage(self);
	}
}

impl<'input> CustomRuleContext<'input> for PipelineStageContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_pipelineStage }
	//fn type_rule_index() -> usize where Self: Sized { RULE_pipelineStage }
}
antlr4rust::tid!{PipelineStageContextExt<'a>}

impl<'input> PipelineStageContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<PipelineStageContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,PipelineStageContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait PipelineStageContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<PipelineStageContextExt<'input>>{

fn byClause(&self) -> Option<Rc<ByClauseContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn aggregateFilter(&self) -> Option<Rc<AggregateFilterContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn aggregate(&self) -> Option<Rc<AggregateContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn metricsFunction(&self) -> Option<Rc<MetricsFunctionContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> PipelineStageContextAttrs<'input> for PipelineStageContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn pipelineStage(&mut self,)
	-> Result<Rc<PipelineStageContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = PipelineStageContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 4, RULE_pipelineStage);
        let mut _localctx: Rc<PipelineStageContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(68);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(3,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule byClause*/
					recog.base.set_state(61);
					recog.byClause()?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule aggregateFilter*/
					recog.base.set_state(62);
					recog.aggregateFilter()?;

					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					/*InvokeRule aggregate*/
					recog.base.set_state(63);
					recog.aggregate()?;

					}
				}
			,
				4 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					/*InvokeRule metricsFunction*/
					recog.base.set_state(64);
					recog.metricsFunction()?;

					recog.base.set_state(66);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==TraceQLParser_KW_BY {
						{
						/*InvokeRule byClause*/
						recog.base.set_state(65);
						recog.byClause()?;

						}
					}

					}
				}

				_ => {}
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
//------------------- byClause ----------------
pub type ByClauseContextAll<'input> = ByClauseContext<'input>;


pub type ByClauseContext<'input> = BaseParserRuleContext<'input,ByClauseContextExt<'input>>;

#[derive(Clone)]
pub struct ByClauseContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for ByClauseContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for ByClauseContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_byClause(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_byClause(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for ByClauseContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_byClause(self);
	}
}

impl<'input> CustomRuleContext<'input> for ByClauseContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_byClause }
	//fn type_rule_index() -> usize where Self: Sized { RULE_byClause }
}
antlr4rust::tid!{ByClauseContextExt<'a>}

impl<'input> ByClauseContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ByClauseContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ByClauseContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ByClauseContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<ByClauseContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token KW_BY
/// Returns `None` if there is no child corresponding to token KW_BY
fn KW_BY(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KW_BY, 0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_LPAREN, 0)
}
fn fieldRef_all(&self) ->  Vec<Rc<FieldRefContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn fieldRef(&self, i: usize) -> Option<Rc<FieldRefContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_RPAREN, 0)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,TraceQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_COMMA, i)
}

}

impl<'input> ByClauseContextAttrs<'input> for ByClauseContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn byClause(&mut self,)
	-> Result<Rc<ByClauseContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ByClauseContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 6, RULE_byClause);
        let mut _localctx: Rc<ByClauseContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(70);
			recog.base.match_token(TraceQLParser_KW_BY,&mut recog.err_handler)?;

			recog.base.set_state(71);
			recog.base.match_token(TraceQLParser_LPAREN,&mut recog.err_handler)?;

			/*InvokeRule fieldRef*/
			recog.base.set_state(72);
			recog.fieldRef()?;

			recog.base.set_state(77);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			while _la==TraceQLParser_COMMA {
				{
				{
				recog.base.set_state(73);
				recog.base.match_token(TraceQLParser_COMMA,&mut recog.err_handler)?;

				/*InvokeRule fieldRef*/
				recog.base.set_state(74);
				recog.fieldRef()?;

				}
				}
				recog.base.set_state(79);
				recog.err_handler.sync(&mut recog.base)?;
				_la = recog.base.input.la(1);
			}
			recog.base.set_state(80);
			recog.base.match_token(TraceQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- aggregate ----------------
pub type AggregateContextAll<'input> = AggregateContext<'input>;


pub type AggregateContext<'input> = BaseParserRuleContext<'input,AggregateContextExt<'input>>;

#[derive(Clone)]
pub struct AggregateContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for AggregateContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for AggregateContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_aggregate(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_aggregate(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for AggregateContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_aggregate(self);
	}
}

impl<'input> CustomRuleContext<'input> for AggregateContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_aggregate }
	//fn type_rule_index() -> usize where Self: Sized { RULE_aggregate }
}
antlr4rust::tid!{AggregateContextExt<'a>}

impl<'input> AggregateContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<AggregateContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,AggregateContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait AggregateContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<AggregateContextExt<'input>>{

fn aggregateOp(&self) -> Option<Rc<AggregateOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_RPAREN, 0)
}
fn fieldRef(&self) -> Option<Rc<FieldRefContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token COMMA
/// Returns `None` if there is no child corresponding to token COMMA
fn COMMA(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_COMMA, 0)
}
fn literal(&self) -> Option<Rc<LiteralContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> AggregateContextAttrs<'input> for AggregateContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn aggregate(&mut self,)
	-> Result<Rc<AggregateContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = AggregateContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 8, RULE_aggregate);
        let mut _localctx: Rc<AggregateContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule aggregateOp*/
			recog.base.set_state(82);
			recog.aggregateOp()?;

			recog.base.set_state(83);
			recog.base.match_token(TraceQLParser_LPAREN,&mut recog.err_handler)?;

			recog.base.set_state(85);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==TraceQLParser_DOT || ((((_la - 32)) & !0x3f) == 0 && ((1usize << (_la - 32)) & 65503) != 0) {
				{
				/*InvokeRule fieldRef*/
				recog.base.set_state(84);
				recog.fieldRef()?;

				}
			}

			recog.base.set_state(89);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==TraceQLParser_COMMA {
				{
				recog.base.set_state(87);
				recog.base.match_token(TraceQLParser_COMMA,&mut recog.err_handler)?;

				/*InvokeRule literal*/
				recog.base.set_state(88);
				recog.literal()?;

				}
			}

			recog.base.set_state(91);
			recog.base.match_token(TraceQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- aggregateFilter ----------------
pub type AggregateFilterContextAll<'input> = AggregateFilterContext<'input>;


pub type AggregateFilterContext<'input> = BaseParserRuleContext<'input,AggregateFilterContextExt<'input>>;

#[derive(Clone)]
pub struct AggregateFilterContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for AggregateFilterContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for AggregateFilterContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_aggregateFilter(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_aggregateFilter(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for AggregateFilterContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_aggregateFilter(self);
	}
}

impl<'input> CustomRuleContext<'input> for AggregateFilterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_aggregateFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_aggregateFilter }
}
antlr4rust::tid!{AggregateFilterContextExt<'a>}

impl<'input> AggregateFilterContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<AggregateFilterContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,AggregateFilterContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait AggregateFilterContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<AggregateFilterContextExt<'input>>{

fn aggregateOp(&self) -> Option<Rc<AggregateOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_RPAREN, 0)
}
fn comparisonOp(&self) -> Option<Rc<ComparisonOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn literal_all(&self) ->  Vec<Rc<LiteralContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn literal(&self, i: usize) -> Option<Rc<LiteralContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
fn fieldRef(&self) -> Option<Rc<FieldRefContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token COMMA
/// Returns `None` if there is no child corresponding to token COMMA
fn COMMA(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_COMMA, 0)
}

}

impl<'input> AggregateFilterContextAttrs<'input> for AggregateFilterContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn aggregateFilter(&mut self,)
	-> Result<Rc<AggregateFilterContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = AggregateFilterContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 10, RULE_aggregateFilter);
        let mut _localctx: Rc<AggregateFilterContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule aggregateOp*/
			recog.base.set_state(93);
			recog.aggregateOp()?;

			recog.base.set_state(94);
			recog.base.match_token(TraceQLParser_LPAREN,&mut recog.err_handler)?;

			recog.base.set_state(96);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==TraceQLParser_DOT || ((((_la - 32)) & !0x3f) == 0 && ((1usize << (_la - 32)) & 65503) != 0) {
				{
				/*InvokeRule fieldRef*/
				recog.base.set_state(95);
				recog.fieldRef()?;

				}
			}

			recog.base.set_state(100);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==TraceQLParser_COMMA {
				{
				recog.base.set_state(98);
				recog.base.match_token(TraceQLParser_COMMA,&mut recog.err_handler)?;

				/*InvokeRule literal*/
				recog.base.set_state(99);
				recog.literal()?;

				}
			}

			recog.base.set_state(102);
			recog.base.match_token(TraceQLParser_RPAREN,&mut recog.err_handler)?;

			/*InvokeRule comparisonOp*/
			recog.base.set_state(103);
			recog.comparisonOp()?;

			/*InvokeRule literal*/
			recog.base.set_state(104);
			recog.literal()?;

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
//------------------- aggregateOp ----------------
pub type AggregateOpContextAll<'input> = AggregateOpContext<'input>;


pub type AggregateOpContext<'input> = BaseParserRuleContext<'input,AggregateOpContextExt<'input>>;

#[derive(Clone)]
pub struct AggregateOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for AggregateOpContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for AggregateOpContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_aggregateOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_aggregateOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for AggregateOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_aggregateOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for AggregateOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_aggregateOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_aggregateOp }
}
antlr4rust::tid!{AggregateOpContextExt<'a>}

impl<'input> AggregateOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<AggregateOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,AggregateOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait AggregateOpContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<AggregateOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token FN_COUNT
/// Returns `None` if there is no child corresponding to token FN_COUNT
fn FN_COUNT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_COUNT, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_SUM
/// Returns `None` if there is no child corresponding to token FN_SUM
fn FN_SUM(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_SUM, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_AVG
/// Returns `None` if there is no child corresponding to token FN_AVG
fn FN_AVG(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_AVG, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_MIN
/// Returns `None` if there is no child corresponding to token FN_MIN
fn FN_MIN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_MIN, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_MAX
/// Returns `None` if there is no child corresponding to token FN_MAX
fn FN_MAX(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_MAX, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_QUANTILE
/// Returns `None` if there is no child corresponding to token FN_QUANTILE
fn FN_QUANTILE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_QUANTILE, 0)
}

}

impl<'input> AggregateOpContextAttrs<'input> for AggregateOpContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn aggregateOp(&mut self,)
	-> Result<Rc<AggregateOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = AggregateOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 12, RULE_aggregateOp);
        let mut _localctx: Rc<AggregateOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(106);
			_la = recog.base.input.la(1);
			if { !(((((_la - 56)) & !0x3f) == 0 && ((1usize << (_la - 56)) & 63) != 0)) } {
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
//------------------- metricsFunction ----------------
pub type MetricsFunctionContextAll<'input> = MetricsFunctionContext<'input>;


pub type MetricsFunctionContext<'input> = BaseParserRuleContext<'input,MetricsFunctionContextExt<'input>>;

#[derive(Clone)]
pub struct MetricsFunctionContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for MetricsFunctionContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for MetricsFunctionContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_metricsFunction(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_metricsFunction(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for MetricsFunctionContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_metricsFunction(self);
	}
}

impl<'input> CustomRuleContext<'input> for MetricsFunctionContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricsFunction }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricsFunction }
}
antlr4rust::tid!{MetricsFunctionContextExt<'a>}

impl<'input> MetricsFunctionContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<MetricsFunctionContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,MetricsFunctionContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait MetricsFunctionContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<MetricsFunctionContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token FN_RATE
/// Returns `None` if there is no child corresponding to token FN_RATE
fn FN_RATE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_RATE, 0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_RPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_COUNT_OVER_TIME
/// Returns `None` if there is no child corresponding to token FN_COUNT_OVER_TIME
fn FN_COUNT_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_COUNT_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_HISTOGRAM_OVER_TIME
/// Returns `None` if there is no child corresponding to token FN_HISTOGRAM_OVER_TIME
fn FN_HISTOGRAM_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_HISTOGRAM_OVER_TIME, 0)
}
fn fieldRef(&self) -> Option<Rc<FieldRefContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> MetricsFunctionContextAttrs<'input> for MetricsFunctionContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn metricsFunction(&mut self,)
	-> Result<Rc<MetricsFunctionContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = MetricsFunctionContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 14, RULE_metricsFunction);
        let mut _localctx: Rc<MetricsFunctionContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(119);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			TraceQLParser_FN_RATE 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(108);
					recog.base.match_token(TraceQLParser_FN_RATE,&mut recog.err_handler)?;

					recog.base.set_state(109);
					recog.base.match_token(TraceQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(110);
					recog.base.match_token(TraceQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}

			TraceQLParser_FN_COUNT_OVER_TIME 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(111);
					recog.base.match_token(TraceQLParser_FN_COUNT_OVER_TIME,&mut recog.err_handler)?;

					recog.base.set_state(112);
					recog.base.match_token(TraceQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(113);
					recog.base.match_token(TraceQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}

			TraceQLParser_FN_HISTOGRAM_OVER_TIME 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(114);
					recog.base.match_token(TraceQLParser_FN_HISTOGRAM_OVER_TIME,&mut recog.err_handler)?;

					recog.base.set_state(115);
					recog.base.match_token(TraceQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule fieldRef*/
					recog.base.set_state(116);
					recog.fieldRef()?;

					recog.base.set_state(117);
					recog.base.match_token(TraceQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- spansetExpr ----------------
pub type SpansetExprContextAll<'input> = SpansetExprContext<'input>;


pub type SpansetExprContext<'input> = BaseParserRuleContext<'input,SpansetExprContextExt<'input>>;

#[derive(Clone)]
pub struct SpansetExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for SpansetExprContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for SpansetExprContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_spansetExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_spansetExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for SpansetExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_spansetExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for SpansetExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spansetExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spansetExpr }
}
antlr4rust::tid!{SpansetExprContextExt<'a>}

impl<'input> SpansetExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<SpansetExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,SpansetExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait SpansetExprContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<SpansetExprContextExt<'input>>{

fn spansetOr(&self) -> Option<Rc<SpansetOrContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> SpansetExprContextAttrs<'input> for SpansetExprContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn spansetExpr(&mut self,)
	-> Result<Rc<SpansetExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = SpansetExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 16, RULE_spansetExpr);
        let mut _localctx: Rc<SpansetExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule spansetOr*/
			recog.base.set_state(121);
			recog.spansetOr()?;

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
//------------------- spansetOr ----------------
pub type SpansetOrContextAll<'input> = SpansetOrContext<'input>;


pub type SpansetOrContext<'input> = BaseParserRuleContext<'input,SpansetOrContextExt<'input>>;

#[derive(Clone)]
pub struct SpansetOrContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for SpansetOrContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for SpansetOrContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_spansetOr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_spansetOr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for SpansetOrContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_spansetOr(self);
	}
}

impl<'input> CustomRuleContext<'input> for SpansetOrContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spansetOr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spansetOr }
}
antlr4rust::tid!{SpansetOrContextExt<'a>}

impl<'input> SpansetOrContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<SpansetOrContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,SpansetOrContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait SpansetOrContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<SpansetOrContextExt<'input>>{

fn spansetAnd_all(&self) ->  Vec<Rc<SpansetAndContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn spansetAnd(&self, i: usize) -> Option<Rc<SpansetAndContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token OR in current rule
fn OR_all(&self) -> Vec<Rc<TerminalNode<'input,TraceQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token OR, starting from 0.
/// Returns `None` if number of children corresponding to token OR is less or equal than `i`.
fn OR(&self, i: usize) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_OR, i)
}

}

impl<'input> SpansetOrContextAttrs<'input> for SpansetOrContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn spansetOr(&mut self,)
	-> Result<Rc<SpansetOrContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = SpansetOrContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 18, RULE_spansetOr);
        let mut _localctx: Rc<SpansetOrContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule spansetAnd*/
			recog.base.set_state(123);
			recog.spansetAnd()?;

			recog.base.set_state(128);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			while _la==TraceQLParser_OR {
				{
				{
				recog.base.set_state(124);
				recog.base.match_token(TraceQLParser_OR,&mut recog.err_handler)?;

				/*InvokeRule spansetAnd*/
				recog.base.set_state(125);
				recog.spansetAnd()?;

				}
				}
				recog.base.set_state(130);
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
//------------------- spansetAnd ----------------
pub type SpansetAndContextAll<'input> = SpansetAndContext<'input>;


pub type SpansetAndContext<'input> = BaseParserRuleContext<'input,SpansetAndContextExt<'input>>;

#[derive(Clone)]
pub struct SpansetAndContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for SpansetAndContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for SpansetAndContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_spansetAnd(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_spansetAnd(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for SpansetAndContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_spansetAnd(self);
	}
}

impl<'input> CustomRuleContext<'input> for SpansetAndContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spansetAnd }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spansetAnd }
}
antlr4rust::tid!{SpansetAndContextExt<'a>}

impl<'input> SpansetAndContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<SpansetAndContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,SpansetAndContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait SpansetAndContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<SpansetAndContextExt<'input>>{

fn spansetRel_all(&self) ->  Vec<Rc<SpansetRelContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn spansetRel(&self, i: usize) -> Option<Rc<SpansetRelContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token AND in current rule
fn AND_all(&self) -> Vec<Rc<TerminalNode<'input,TraceQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token AND, starting from 0.
/// Returns `None` if number of children corresponding to token AND is less or equal than `i`.
fn AND(&self, i: usize) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_AND, i)
}

}

impl<'input> SpansetAndContextAttrs<'input> for SpansetAndContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn spansetAnd(&mut self,)
	-> Result<Rc<SpansetAndContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = SpansetAndContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 20, RULE_spansetAnd);
        let mut _localctx: Rc<SpansetAndContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule spansetRel*/
			recog.base.set_state(131);
			recog.spansetRel()?;

			recog.base.set_state(136);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			while _la==TraceQLParser_AND {
				{
				{
				recog.base.set_state(132);
				recog.base.match_token(TraceQLParser_AND,&mut recog.err_handler)?;

				/*InvokeRule spansetRel*/
				recog.base.set_state(133);
				recog.spansetRel()?;

				}
				}
				recog.base.set_state(138);
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
//------------------- spansetRel ----------------
pub type SpansetRelContextAll<'input> = SpansetRelContext<'input>;


pub type SpansetRelContext<'input> = BaseParserRuleContext<'input,SpansetRelContextExt<'input>>;

#[derive(Clone)]
pub struct SpansetRelContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for SpansetRelContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for SpansetRelContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_spansetRel(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_spansetRel(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for SpansetRelContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_spansetRel(self);
	}
}

impl<'input> CustomRuleContext<'input> for SpansetRelContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spansetRel }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spansetRel }
}
antlr4rust::tid!{SpansetRelContextExt<'a>}

impl<'input> SpansetRelContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<SpansetRelContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,SpansetRelContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait SpansetRelContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<SpansetRelContextExt<'input>>{

fn spansetPrimary_all(&self) ->  Vec<Rc<SpansetPrimaryContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn spansetPrimary(&self, i: usize) -> Option<Rc<SpansetPrimaryContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
fn spansetRelOp_all(&self) ->  Vec<Rc<SpansetRelOpContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn spansetRelOp(&self, i: usize) -> Option<Rc<SpansetRelOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}

}

impl<'input> SpansetRelContextAttrs<'input> for SpansetRelContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn spansetRel(&mut self,)
	-> Result<Rc<SpansetRelContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = SpansetRelContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 22, RULE_spansetRel);
        let mut _localctx: Rc<SpansetRelContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule spansetPrimary*/
			recog.base.set_state(139);
			recog.spansetPrimary()?;

			recog.base.set_state(145);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			while (((_la) & !0x3f) == 0 && ((1usize << _la) & 16745472) != 0) {
				{
				{
				/*InvokeRule spansetRelOp*/
				recog.base.set_state(140);
				recog.spansetRelOp()?;

				/*InvokeRule spansetPrimary*/
				recog.base.set_state(141);
				recog.spansetPrimary()?;

				}
				}
				recog.base.set_state(147);
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
//------------------- spansetPrimary ----------------
pub type SpansetPrimaryContextAll<'input> = SpansetPrimaryContext<'input>;


pub type SpansetPrimaryContext<'input> = BaseParserRuleContext<'input,SpansetPrimaryContextExt<'input>>;

#[derive(Clone)]
pub struct SpansetPrimaryContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for SpansetPrimaryContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for SpansetPrimaryContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_spansetPrimary(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_spansetPrimary(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for SpansetPrimaryContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_spansetPrimary(self);
	}
}

impl<'input> CustomRuleContext<'input> for SpansetPrimaryContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spansetPrimary }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spansetPrimary }
}
antlr4rust::tid!{SpansetPrimaryContextExt<'a>}

impl<'input> SpansetPrimaryContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<SpansetPrimaryContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,SpansetPrimaryContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait SpansetPrimaryContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<SpansetPrimaryContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_LPAREN, 0)
}
fn spansetExpr(&self) -> Option<Rc<SpansetExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_RPAREN, 0)
}
fn spanSelector(&self) -> Option<Rc<SpanSelectorContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> SpansetPrimaryContextAttrs<'input> for SpansetPrimaryContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn spansetPrimary(&mut self,)
	-> Result<Rc<SpansetPrimaryContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = SpansetPrimaryContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 24, RULE_spansetPrimary);
        let mut _localctx: Rc<SpansetPrimaryContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(153);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			TraceQLParser_LPAREN 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(148);
					recog.base.match_token(TraceQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule spansetExpr*/
					recog.base.set_state(149);
					recog.spansetExpr()?;

					recog.base.set_state(150);
					recog.base.match_token(TraceQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}

			TraceQLParser_LBRACE 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule spanSelector*/
					recog.base.set_state(152);
					recog.spanSelector()?;

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
//------------------- spansetRelOp ----------------
pub type SpansetRelOpContextAll<'input> = SpansetRelOpContext<'input>;


pub type SpansetRelOpContext<'input> = BaseParserRuleContext<'input,SpansetRelOpContextExt<'input>>;

#[derive(Clone)]
pub struct SpansetRelOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for SpansetRelOpContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for SpansetRelOpContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_spansetRelOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_spansetRelOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for SpansetRelOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_spansetRelOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for SpansetRelOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spansetRelOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spansetRelOp }
}
antlr4rust::tid!{SpansetRelOpContextExt<'a>}

impl<'input> SpansetRelOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<SpansetRelOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,SpansetRelOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait SpansetRelOpContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<SpansetRelOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token DESC
/// Returns `None` if there is no child corresponding to token DESC
fn DESC(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_DESC, 0)
}
/// Retrieves first TerminalNode corresponding to token GT
/// Returns `None` if there is no child corresponding to token GT
fn GT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_GT, 0)
}
/// Retrieves first TerminalNode corresponding to token ANC
/// Returns `None` if there is no child corresponding to token ANC
fn ANC(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_ANC, 0)
}
/// Retrieves first TerminalNode corresponding to token LT
/// Returns `None` if there is no child corresponding to token LT
fn LT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_LT, 0)
}
/// Retrieves first TerminalNode corresponding to token SIBLING
/// Returns `None` if there is no child corresponding to token SIBLING
fn SIBLING(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SIBLING, 0)
}
/// Retrieves first TerminalNode corresponding to token NOT_DESC
/// Returns `None` if there is no child corresponding to token NOT_DESC
fn NOT_DESC(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_NOT_DESC, 0)
}
/// Retrieves first TerminalNode corresponding to token NOT_CHILD
/// Returns `None` if there is no child corresponding to token NOT_CHILD
fn NOT_CHILD(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_NOT_CHILD, 0)
}
/// Retrieves first TerminalNode corresponding to token NOT_ANC
/// Returns `None` if there is no child corresponding to token NOT_ANC
fn NOT_ANC(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_NOT_ANC, 0)
}
/// Retrieves first TerminalNode corresponding to token NOT_PARENT
/// Returns `None` if there is no child corresponding to token NOT_PARENT
fn NOT_PARENT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_NOT_PARENT, 0)
}
/// Retrieves first TerminalNode corresponding to token NEQ_RE
/// Returns `None` if there is no child corresponding to token NEQ_RE
fn NEQ_RE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_NEQ_RE, 0)
}

}

impl<'input> SpansetRelOpContextAttrs<'input> for SpansetRelOpContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn spansetRelOp(&mut self,)
	-> Result<Rc<SpansetRelOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = SpansetRelOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 26, RULE_spansetRelOp);
        let mut _localctx: Rc<SpansetRelOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(155);
			_la = recog.base.input.la(1);
			if { !((((_la) & !0x3f) == 0 && ((1usize << _la) & 16745472) != 0)) } {
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
//------------------- spanSelector ----------------
pub type SpanSelectorContextAll<'input> = SpanSelectorContext<'input>;


pub type SpanSelectorContext<'input> = BaseParserRuleContext<'input,SpanSelectorContextExt<'input>>;

#[derive(Clone)]
pub struct SpanSelectorContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for SpanSelectorContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for SpanSelectorContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_spanSelector(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_spanSelector(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for SpanSelectorContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_spanSelector(self);
	}
}

impl<'input> CustomRuleContext<'input> for SpanSelectorContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spanSelector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spanSelector }
}
antlr4rust::tid!{SpanSelectorContextExt<'a>}

impl<'input> SpanSelectorContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<SpanSelectorContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,SpanSelectorContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait SpanSelectorContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<SpanSelectorContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LBRACE
/// Returns `None` if there is no child corresponding to token LBRACE
fn LBRACE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_LBRACE, 0)
}
/// Retrieves first TerminalNode corresponding to token RBRACE
/// Returns `None` if there is no child corresponding to token RBRACE
fn RBRACE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_RBRACE, 0)
}
fn spanFilter(&self) -> Option<Rc<SpanFilterContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> SpanSelectorContextAttrs<'input> for SpanSelectorContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn spanSelector(&mut self,)
	-> Result<Rc<SpanSelectorContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = SpanSelectorContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 28, RULE_spanSelector);
        let mut _localctx: Rc<SpanSelectorContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(157);
			recog.base.match_token(TraceQLParser_LBRACE,&mut recog.err_handler)?;

			recog.base.set_state(159);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if (((_la) & !0x3f) == 0 && ((1usize << _la) & 67108936) != 0) || ((((_la - 32)) & !0x3f) == 0 && ((1usize << (_la - 32)) & 65503) != 0) {
				{
				/*InvokeRule spanFilter*/
				recog.base.set_state(158);
				recog.spanFilter_rec(0)?;

				}
			}

			recog.base.set_state(161);
			recog.base.match_token(TraceQLParser_RBRACE,&mut recog.err_handler)?;

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
//------------------- spanFilter ----------------
#[derive(Debug)]
pub enum SpanFilterContextAll<'input>{
	FilterParenContext(FilterParenContext<'input>),
	FilterNotContext(FilterNotContext<'input>),
	FilterCompareContext(FilterCompareContext<'input>),
	FilterOrContext(FilterOrContext<'input>),
	FilterAndContext(FilterAndContext<'input>),
Error(SpanFilterContext<'input>)
}
antlr4rust::tid!{SpanFilterContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for SpanFilterContextAll<'input>{}

impl<'input> TraceQLParserContext<'input> for SpanFilterContextAll<'input>{}

impl<'input> Deref for SpanFilterContextAll<'input>{
	type Target = dyn SpanFilterContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use SpanFilterContextAll::*;
		match self{
			FilterParenContext(inner) => inner,
			FilterNotContext(inner) => inner,
			FilterCompareContext(inner) => inner,
			FilterOrContext(inner) => inner,
			FilterAndContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for SpanFilterContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for SpanFilterContextAll<'input>{
    fn enter(&self, listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type SpanFilterContext<'input> = BaseParserRuleContext<'input,SpanFilterContextExt<'input>>;

#[derive(Clone)]
pub struct SpanFilterContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for SpanFilterContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for SpanFilterContext<'input>{
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for SpanFilterContext<'input>{
}

impl<'input> CustomRuleContext<'input> for SpanFilterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spanFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spanFilter }
}
antlr4rust::tid!{SpanFilterContextExt<'a>}

impl<'input> SpanFilterContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<SpanFilterContextAll<'input>> {
		Rc::new(
		SpanFilterContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,SpanFilterContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait SpanFilterContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<SpanFilterContextExt<'input>>{


}

impl<'input> SpanFilterContextAttrs<'input> for SpanFilterContext<'input>{}

pub type FilterParenContext<'input> = BaseParserRuleContext<'input,FilterParenContextExt<'input>>;

pub trait FilterParenContextAttrs<'input>: TraceQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token LPAREN
	/// Returns `None` if there is no child corresponding to token LPAREN
	fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
		self.get_token(TraceQLParser_LPAREN, 0)
	}
	fn spanFilter(&self) -> Option<Rc<SpanFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	/// Retrieves first TerminalNode corresponding to token RPAREN
	/// Returns `None` if there is no child corresponding to token RPAREN
	fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
		self.get_token(TraceQLParser_RPAREN, 0)
	}
}

impl<'input> FilterParenContextAttrs<'input> for FilterParenContext<'input>{}

pub struct FilterParenContextExt<'input>{
	base:SpanFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{FilterParenContextExt<'a>}

impl<'input> TraceQLParserContext<'input> for FilterParenContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FilterParenContext<'input>{
	fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_FilterParen(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_FilterParen(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FilterParenContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_FilterParen(self);
	}
}

impl<'input> CustomRuleContext<'input> for FilterParenContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spanFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spanFilter }
}

impl<'input> Borrow<SpanFilterContextExt<'input>> for FilterParenContext<'input>{
	fn borrow(&self) -> &SpanFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<SpanFilterContextExt<'input>> for FilterParenContext<'input>{
	fn borrow_mut(&mut self) -> &mut SpanFilterContextExt<'input> { &mut self.base }
}

impl<'input> SpanFilterContextAttrs<'input> for FilterParenContext<'input> {}

impl<'input> FilterParenContextExt<'input>{
	fn new(ctx: &dyn SpanFilterContextAttrs<'input>) -> Rc<SpanFilterContextAll<'input>>  {
		Rc::new(
			SpanFilterContextAll::FilterParenContext(
				BaseParserRuleContext::copy_from(ctx,FilterParenContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type FilterNotContext<'input> = BaseParserRuleContext<'input,FilterNotContextExt<'input>>;

pub trait FilterNotContextAttrs<'input>: TraceQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token NOT
	/// Returns `None` if there is no child corresponding to token NOT
	fn NOT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
		self.get_token(TraceQLParser_NOT, 0)
	}
	fn spanFilter(&self) -> Option<Rc<SpanFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> FilterNotContextAttrs<'input> for FilterNotContext<'input>{}

pub struct FilterNotContextExt<'input>{
	base:SpanFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{FilterNotContextExt<'a>}

impl<'input> TraceQLParserContext<'input> for FilterNotContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FilterNotContext<'input>{
	fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_FilterNot(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_FilterNot(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FilterNotContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_FilterNot(self);
	}
}

impl<'input> CustomRuleContext<'input> for FilterNotContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spanFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spanFilter }
}

impl<'input> Borrow<SpanFilterContextExt<'input>> for FilterNotContext<'input>{
	fn borrow(&self) -> &SpanFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<SpanFilterContextExt<'input>> for FilterNotContext<'input>{
	fn borrow_mut(&mut self) -> &mut SpanFilterContextExt<'input> { &mut self.base }
}

impl<'input> SpanFilterContextAttrs<'input> for FilterNotContext<'input> {}

impl<'input> FilterNotContextExt<'input>{
	fn new(ctx: &dyn SpanFilterContextAttrs<'input>) -> Rc<SpanFilterContextAll<'input>>  {
		Rc::new(
			SpanFilterContextAll::FilterNotContext(
				BaseParserRuleContext::copy_from(ctx,FilterNotContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type FilterCompareContext<'input> = BaseParserRuleContext<'input,FilterCompareContextExt<'input>>;

pub trait FilterCompareContextAttrs<'input>: TraceQLParserContext<'input>{
	fn fieldRef(&self) -> Option<Rc<FieldRefContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	fn comparisonOp(&self) -> Option<Rc<ComparisonOpContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	fn literal(&self) -> Option<Rc<LiteralContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> FilterCompareContextAttrs<'input> for FilterCompareContext<'input>{}

pub struct FilterCompareContextExt<'input>{
	base:SpanFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{FilterCompareContextExt<'a>}

impl<'input> TraceQLParserContext<'input> for FilterCompareContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FilterCompareContext<'input>{
	fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_FilterCompare(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_FilterCompare(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FilterCompareContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_FilterCompare(self);
	}
}

impl<'input> CustomRuleContext<'input> for FilterCompareContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spanFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spanFilter }
}

impl<'input> Borrow<SpanFilterContextExt<'input>> for FilterCompareContext<'input>{
	fn borrow(&self) -> &SpanFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<SpanFilterContextExt<'input>> for FilterCompareContext<'input>{
	fn borrow_mut(&mut self) -> &mut SpanFilterContextExt<'input> { &mut self.base }
}

impl<'input> SpanFilterContextAttrs<'input> for FilterCompareContext<'input> {}

impl<'input> FilterCompareContextExt<'input>{
	fn new(ctx: &dyn SpanFilterContextAttrs<'input>) -> Rc<SpanFilterContextAll<'input>>  {
		Rc::new(
			SpanFilterContextAll::FilterCompareContext(
				BaseParserRuleContext::copy_from(ctx,FilterCompareContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type FilterOrContext<'input> = BaseParserRuleContext<'input,FilterOrContextExt<'input>>;

pub trait FilterOrContextAttrs<'input>: TraceQLParserContext<'input>{
	fn spanFilter_all(&self) ->  Vec<Rc<SpanFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn spanFilter(&self, i: usize) -> Option<Rc<SpanFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token OR
	/// Returns `None` if there is no child corresponding to token OR
	fn OR(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
		self.get_token(TraceQLParser_OR, 0)
	}
}

impl<'input> FilterOrContextAttrs<'input> for FilterOrContext<'input>{}

pub struct FilterOrContextExt<'input>{
	base:SpanFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{FilterOrContextExt<'a>}

impl<'input> TraceQLParserContext<'input> for FilterOrContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FilterOrContext<'input>{
	fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_FilterOr(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_FilterOr(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FilterOrContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_FilterOr(self);
	}
}

impl<'input> CustomRuleContext<'input> for FilterOrContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spanFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spanFilter }
}

impl<'input> Borrow<SpanFilterContextExt<'input>> for FilterOrContext<'input>{
	fn borrow(&self) -> &SpanFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<SpanFilterContextExt<'input>> for FilterOrContext<'input>{
	fn borrow_mut(&mut self) -> &mut SpanFilterContextExt<'input> { &mut self.base }
}

impl<'input> SpanFilterContextAttrs<'input> for FilterOrContext<'input> {}

impl<'input> FilterOrContextExt<'input>{
	fn new(ctx: &dyn SpanFilterContextAttrs<'input>) -> Rc<SpanFilterContextAll<'input>>  {
		Rc::new(
			SpanFilterContextAll::FilterOrContext(
				BaseParserRuleContext::copy_from(ctx,FilterOrContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type FilterAndContext<'input> = BaseParserRuleContext<'input,FilterAndContextExt<'input>>;

pub trait FilterAndContextAttrs<'input>: TraceQLParserContext<'input>{
	fn spanFilter_all(&self) ->  Vec<Rc<SpanFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn spanFilter(&self, i: usize) -> Option<Rc<SpanFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token AND
	/// Returns `None` if there is no child corresponding to token AND
	fn AND(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
		self.get_token(TraceQLParser_AND, 0)
	}
}

impl<'input> FilterAndContextAttrs<'input> for FilterAndContext<'input>{}

pub struct FilterAndContextExt<'input>{
	base:SpanFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{FilterAndContextExt<'a>}

impl<'input> TraceQLParserContext<'input> for FilterAndContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FilterAndContext<'input>{
	fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_FilterAnd(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_FilterAnd(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FilterAndContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_FilterAnd(self);
	}
}

impl<'input> CustomRuleContext<'input> for FilterAndContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_spanFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_spanFilter }
}

impl<'input> Borrow<SpanFilterContextExt<'input>> for FilterAndContext<'input>{
	fn borrow(&self) -> &SpanFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<SpanFilterContextExt<'input>> for FilterAndContext<'input>{
	fn borrow_mut(&mut self) -> &mut SpanFilterContextExt<'input> { &mut self.base }
}

impl<'input> SpanFilterContextAttrs<'input> for FilterAndContext<'input> {}

impl<'input> FilterAndContextExt<'input>{
	fn new(ctx: &dyn SpanFilterContextAttrs<'input>) -> Rc<SpanFilterContextAll<'input>>  {
		Rc::new(
			SpanFilterContextAll::FilterAndContext(
				BaseParserRuleContext::copy_from(ctx,FilterAndContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn  spanFilter(&mut self,)
	-> Result<Rc<SpanFilterContextAll<'input>>,ANTLRError> {
		self.spanFilter_rec(0)
	}

	fn spanFilter_rec(&mut self, _p: i32)
	-> Result<Rc<SpanFilterContextAll<'input>>,ANTLRError> {
		let recog = self;
		let _parentctx = recog.ctx.take();
		let _parentState = recog.base.get_state();
		let mut _localctx = SpanFilterContextExt::new(_parentctx.clone(), recog.base.get_state());
		recog.base.enter_recursion_rule(_localctx.clone(), 30, RULE_spanFilter, _p);
	    let mut _localctx: Rc<SpanFilterContextAll> = _localctx;
        let mut _prevctx = _localctx.clone();
		let _startState = 30;
		let result: Result<(), ANTLRError> = (|| {
			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(174);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			TraceQLParser_NOT 
				=> {
					{
					let mut tmp = FilterNotContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();

					recog.base.set_state(164);
					recog.base.match_token(TraceQLParser_NOT,&mut recog.err_handler)?;

					/*InvokeRule spanFilter*/
					recog.base.set_state(165);
					recog.spanFilter_rec(5)?;

					}
				}

			TraceQLParser_LPAREN 
				=> {
					{
					let mut tmp = FilterParenContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					recog.base.set_state(166);
					recog.base.match_token(TraceQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule spanFilter*/
					recog.base.set_state(167);
					recog.spanFilter_rec(0)?;

					recog.base.set_state(168);
					recog.base.match_token(TraceQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}

			TraceQLParser_DOT |TraceQLParser_SCOPE_SPAN |TraceQLParser_SCOPE_RESOURCE |
			TraceQLParser_SCOPE_EVENT |TraceQLParser_SCOPE_LINK |TraceQLParser_SCOPE_PARENT |
			TraceQLParser_INTR_NAME |TraceQLParser_INTR_STATUS |TraceQLParser_INTR_STATUS_MESSAGE |
			TraceQLParser_INTR_KIND |TraceQLParser_INTR_DURATION |TraceQLParser_INTR_TRACE_DURATION |
			TraceQLParser_INTR_ROOT_NAME |TraceQLParser_INTR_ROOT_SVC |TraceQLParser_INTR_TRACE_ID |
			TraceQLParser_INTR_SPAN_ID 
				=> {
					{
					let mut tmp = FilterCompareContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule fieldRef*/
					recog.base.set_state(170);
					recog.fieldRef()?;

					/*InvokeRule comparisonOp*/
					recog.base.set_state(171);
					recog.comparisonOp()?;

					/*InvokeRule literal*/
					recog.base.set_state(172);
					recog.literal()?;

					}
				}

				_ => Err(ANTLRError::NoAltError(NoViableAltError::new(&mut recog.base)))?
			}
			let tmp = recog.input.lt(-1).cloned();
			recog.ctx.as_ref().unwrap().set_stop(tmp);
			recog.base.set_state(184);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(17,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					recog.trigger_exit_rule_event()?;
					_prevctx = _localctx.clone();
					{
					recog.base.set_state(182);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(16,&mut recog.base)? {
						1 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = FilterAndContextExt::new(&**SpanFilterContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_spanFilter)?;
							_localctx = tmp;
							recog.base.set_state(176);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 3)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 3)".to_owned()), None))?;
							}
							recog.base.set_state(177);
							recog.base.match_token(TraceQLParser_AND,&mut recog.err_handler)?;

							/*InvokeRule spanFilter*/
							recog.base.set_state(178);
							recog.spanFilter_rec(4)?;

							}
						}
					,
						2 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = FilterOrContextExt::new(&**SpanFilterContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_spanFilter)?;
							_localctx = tmp;
							recog.base.set_state(179);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 2)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 2)".to_owned()), None))?;
							}
							recog.base.set_state(180);
							recog.base.match_token(TraceQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule spanFilter*/
							recog.base.set_state(181);
							recog.spanFilter_rec(3)?;

							}
						}

						_ => {}
					}
					} 
				}
				recog.base.set_state(186);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(17,&mut recog.base)?;
			}
			}
			Ok(())
		})();
		match result {
		Ok(_) => {},
        Err(e @ ANTLRError::FallThrough(_)) => return Err(e),
		Err(ref re)=>{
			//_localctx.exception = re;
			recog.err_handler.report_error(&mut recog.base, re);
	        recog.err_handler.recover(&mut recog.base, re)?;}
		}
		recog.base.unroll_recursion_context(_parentctx)?;

		Ok(_localctx)
	}
}
//------------------- comparisonOp ----------------
pub type ComparisonOpContextAll<'input> = ComparisonOpContext<'input>;


pub type ComparisonOpContext<'input> = BaseParserRuleContext<'input,ComparisonOpContextExt<'input>>;

#[derive(Clone)]
pub struct ComparisonOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for ComparisonOpContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for ComparisonOpContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_comparisonOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_comparisonOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for ComparisonOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_comparisonOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for ComparisonOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_comparisonOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_comparisonOp }
}
antlr4rust::tid!{ComparisonOpContextExt<'a>}

impl<'input> ComparisonOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ComparisonOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ComparisonOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ComparisonOpContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<ComparisonOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token EQ
/// Returns `None` if there is no child corresponding to token EQ
fn EQ(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_EQ, 0)
}
/// Retrieves first TerminalNode corresponding to token NEQ
/// Returns `None` if there is no child corresponding to token NEQ
fn NEQ(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_NEQ, 0)
}
/// Retrieves first TerminalNode corresponding to token GT
/// Returns `None` if there is no child corresponding to token GT
fn GT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_GT, 0)
}
/// Retrieves first TerminalNode corresponding to token GE
/// Returns `None` if there is no child corresponding to token GE
fn GE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_GE, 0)
}
/// Retrieves first TerminalNode corresponding to token LT
/// Returns `None` if there is no child corresponding to token LT
fn LT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_LT, 0)
}
/// Retrieves first TerminalNode corresponding to token LE
/// Returns `None` if there is no child corresponding to token LE
fn LE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_LE, 0)
}
/// Retrieves first TerminalNode corresponding to token EQ_RE
/// Returns `None` if there is no child corresponding to token EQ_RE
fn EQ_RE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_EQ_RE, 0)
}
/// Retrieves first TerminalNode corresponding to token NEQ_RE
/// Returns `None` if there is no child corresponding to token NEQ_RE
fn NEQ_RE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_NEQ_RE, 0)
}

}

impl<'input> ComparisonOpContextAttrs<'input> for ComparisonOpContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn comparisonOp(&mut self,)
	-> Result<Rc<ComparisonOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ComparisonOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 32, RULE_comparisonOp);
        let mut _localctx: Rc<ComparisonOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(187);
			_la = recog.base.input.la(1);
			if { !((((_la) & !0x3f) == 0 && ((1usize << _la) & 130560) != 0)) } {
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
//------------------- fieldRef ----------------
#[derive(Debug)]
pub enum FieldRefContextAll<'input>{
	FieldAnyScopeContext(FieldAnyScopeContext<'input>),
	FieldScopedContext(FieldScopedContext<'input>),
	FieldIntrinsicContext(FieldIntrinsicContext<'input>),
Error(FieldRefContext<'input>)
}
antlr4rust::tid!{FieldRefContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for FieldRefContextAll<'input>{}

impl<'input> TraceQLParserContext<'input> for FieldRefContextAll<'input>{}

impl<'input> Deref for FieldRefContextAll<'input>{
	type Target = dyn FieldRefContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use FieldRefContextAll::*;
		match self{
			FieldAnyScopeContext(inner) => inner,
			FieldScopedContext(inner) => inner,
			FieldIntrinsicContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FieldRefContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FieldRefContextAll<'input>{
    fn enter(&self, listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type FieldRefContext<'input> = BaseParserRuleContext<'input,FieldRefContextExt<'input>>;

#[derive(Clone)]
pub struct FieldRefContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for FieldRefContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FieldRefContext<'input>{
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FieldRefContext<'input>{
}

impl<'input> CustomRuleContext<'input> for FieldRefContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_fieldRef }
	//fn type_rule_index() -> usize where Self: Sized { RULE_fieldRef }
}
antlr4rust::tid!{FieldRefContextExt<'a>}

impl<'input> FieldRefContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<FieldRefContextAll<'input>> {
		Rc::new(
		FieldRefContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,FieldRefContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait FieldRefContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<FieldRefContextExt<'input>>{


}

impl<'input> FieldRefContextAttrs<'input> for FieldRefContext<'input>{}

pub type FieldAnyScopeContext<'input> = BaseParserRuleContext<'input,FieldAnyScopeContextExt<'input>>;

pub trait FieldAnyScopeContextAttrs<'input>: TraceQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token DOT
	/// Returns `None` if there is no child corresponding to token DOT
	fn DOT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
		self.get_token(TraceQLParser_DOT, 0)
	}
	fn identChain(&self) -> Option<Rc<IdentChainContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> FieldAnyScopeContextAttrs<'input> for FieldAnyScopeContext<'input>{}

pub struct FieldAnyScopeContextExt<'input>{
	base:FieldRefContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{FieldAnyScopeContextExt<'a>}

impl<'input> TraceQLParserContext<'input> for FieldAnyScopeContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FieldAnyScopeContext<'input>{
	fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_FieldAnyScope(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_FieldAnyScope(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FieldAnyScopeContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_FieldAnyScope(self);
	}
}

impl<'input> CustomRuleContext<'input> for FieldAnyScopeContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_fieldRef }
	//fn type_rule_index() -> usize where Self: Sized { RULE_fieldRef }
}

impl<'input> Borrow<FieldRefContextExt<'input>> for FieldAnyScopeContext<'input>{
	fn borrow(&self) -> &FieldRefContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<FieldRefContextExt<'input>> for FieldAnyScopeContext<'input>{
	fn borrow_mut(&mut self) -> &mut FieldRefContextExt<'input> { &mut self.base }
}

impl<'input> FieldRefContextAttrs<'input> for FieldAnyScopeContext<'input> {}

impl<'input> FieldAnyScopeContextExt<'input>{
	fn new(ctx: &dyn FieldRefContextAttrs<'input>) -> Rc<FieldRefContextAll<'input>>  {
		Rc::new(
			FieldRefContextAll::FieldAnyScopeContext(
				BaseParserRuleContext::copy_from(ctx,FieldAnyScopeContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type FieldScopedContext<'input> = BaseParserRuleContext<'input,FieldScopedContextExt<'input>>;

pub trait FieldScopedContextAttrs<'input>: TraceQLParserContext<'input>{
	fn scopedAttribute(&self) -> Option<Rc<ScopedAttributeContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> FieldScopedContextAttrs<'input> for FieldScopedContext<'input>{}

pub struct FieldScopedContextExt<'input>{
	base:FieldRefContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{FieldScopedContextExt<'a>}

impl<'input> TraceQLParserContext<'input> for FieldScopedContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FieldScopedContext<'input>{
	fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_FieldScoped(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_FieldScoped(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FieldScopedContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_FieldScoped(self);
	}
}

impl<'input> CustomRuleContext<'input> for FieldScopedContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_fieldRef }
	//fn type_rule_index() -> usize where Self: Sized { RULE_fieldRef }
}

impl<'input> Borrow<FieldRefContextExt<'input>> for FieldScopedContext<'input>{
	fn borrow(&self) -> &FieldRefContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<FieldRefContextExt<'input>> for FieldScopedContext<'input>{
	fn borrow_mut(&mut self) -> &mut FieldRefContextExt<'input> { &mut self.base }
}

impl<'input> FieldRefContextAttrs<'input> for FieldScopedContext<'input> {}

impl<'input> FieldScopedContextExt<'input>{
	fn new(ctx: &dyn FieldRefContextAttrs<'input>) -> Rc<FieldRefContextAll<'input>>  {
		Rc::new(
			FieldRefContextAll::FieldScopedContext(
				BaseParserRuleContext::copy_from(ctx,FieldScopedContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type FieldIntrinsicContext<'input> = BaseParserRuleContext<'input,FieldIntrinsicContextExt<'input>>;

pub trait FieldIntrinsicContextAttrs<'input>: TraceQLParserContext<'input>{
	fn intrinsic(&self) -> Option<Rc<IntrinsicContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> FieldIntrinsicContextAttrs<'input> for FieldIntrinsicContext<'input>{}

pub struct FieldIntrinsicContextExt<'input>{
	base:FieldRefContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{FieldIntrinsicContextExt<'a>}

impl<'input> TraceQLParserContext<'input> for FieldIntrinsicContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for FieldIntrinsicContext<'input>{
	fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_FieldIntrinsic(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_FieldIntrinsic(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for FieldIntrinsicContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_FieldIntrinsic(self);
	}
}

impl<'input> CustomRuleContext<'input> for FieldIntrinsicContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_fieldRef }
	//fn type_rule_index() -> usize where Self: Sized { RULE_fieldRef }
}

impl<'input> Borrow<FieldRefContextExt<'input>> for FieldIntrinsicContext<'input>{
	fn borrow(&self) -> &FieldRefContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<FieldRefContextExt<'input>> for FieldIntrinsicContext<'input>{
	fn borrow_mut(&mut self) -> &mut FieldRefContextExt<'input> { &mut self.base }
}

impl<'input> FieldRefContextAttrs<'input> for FieldIntrinsicContext<'input> {}

impl<'input> FieldIntrinsicContextExt<'input>{
	fn new(ctx: &dyn FieldRefContextAttrs<'input>) -> Rc<FieldRefContextAll<'input>>  {
		Rc::new(
			FieldRefContextAll::FieldIntrinsicContext(
				BaseParserRuleContext::copy_from(ctx,FieldIntrinsicContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn fieldRef(&mut self,)
	-> Result<Rc<FieldRefContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = FieldRefContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 34, RULE_fieldRef);
        let mut _localctx: Rc<FieldRefContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(193);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			TraceQLParser_INTR_NAME |TraceQLParser_INTR_STATUS |TraceQLParser_INTR_STATUS_MESSAGE |
			TraceQLParser_INTR_KIND |TraceQLParser_INTR_DURATION |TraceQLParser_INTR_TRACE_DURATION |
			TraceQLParser_INTR_ROOT_NAME |TraceQLParser_INTR_ROOT_SVC |TraceQLParser_INTR_TRACE_ID |
			TraceQLParser_INTR_SPAN_ID 
				=> {
					let tmp = FieldIntrinsicContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					/*InvokeRule intrinsic*/
					recog.base.set_state(189);
					recog.intrinsic()?;

					}
				}

			TraceQLParser_SCOPE_SPAN |TraceQLParser_SCOPE_RESOURCE |TraceQLParser_SCOPE_EVENT |
			TraceQLParser_SCOPE_LINK |TraceQLParser_SCOPE_PARENT 
				=> {
					let tmp = FieldScopedContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					/*InvokeRule scopedAttribute*/
					recog.base.set_state(190);
					recog.scopedAttribute()?;

					}
				}

			TraceQLParser_DOT 
				=> {
					let tmp = FieldAnyScopeContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					recog.base.set_state(191);
					recog.base.match_token(TraceQLParser_DOT,&mut recog.err_handler)?;

					/*InvokeRule identChain*/
					recog.base.set_state(192);
					recog.identChain()?;

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
//------------------- scopedAttribute ----------------
pub type ScopedAttributeContextAll<'input> = ScopedAttributeContext<'input>;


pub type ScopedAttributeContext<'input> = BaseParserRuleContext<'input,ScopedAttributeContextExt<'input>>;

#[derive(Clone)]
pub struct ScopedAttributeContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for ScopedAttributeContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for ScopedAttributeContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_scopedAttribute(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_scopedAttribute(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for ScopedAttributeContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_scopedAttribute(self);
	}
}

impl<'input> CustomRuleContext<'input> for ScopedAttributeContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_scopedAttribute }
	//fn type_rule_index() -> usize where Self: Sized { RULE_scopedAttribute }
}
antlr4rust::tid!{ScopedAttributeContextExt<'a>}

impl<'input> ScopedAttributeContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ScopedAttributeContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ScopedAttributeContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ScopedAttributeContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<ScopedAttributeContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token SCOPE_SPAN
/// Returns `None` if there is no child corresponding to token SCOPE_SPAN
fn SCOPE_SPAN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_SPAN, 0)
}
/// Retrieves all `TerminalNode`s corresponding to token DOT in current rule
fn DOT_all(&self) -> Vec<Rc<TerminalNode<'input,TraceQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token DOT, starting from 0.
/// Returns `None` if number of children corresponding to token DOT is less or equal than `i`.
fn DOT(&self, i: usize) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_DOT, i)
}
fn identChain(&self) -> Option<Rc<IdentChainContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_RESOURCE
/// Returns `None` if there is no child corresponding to token SCOPE_RESOURCE
fn SCOPE_RESOURCE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_RESOURCE, 0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_EVENT
/// Returns `None` if there is no child corresponding to token SCOPE_EVENT
fn SCOPE_EVENT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_EVENT, 0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_LINK
/// Returns `None` if there is no child corresponding to token SCOPE_LINK
fn SCOPE_LINK(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_LINK, 0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_PARENT
/// Returns `None` if there is no child corresponding to token SCOPE_PARENT
fn SCOPE_PARENT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_PARENT, 0)
}

}

impl<'input> ScopedAttributeContextAttrs<'input> for ScopedAttributeContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn scopedAttribute(&mut self,)
	-> Result<Rc<ScopedAttributeContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ScopedAttributeContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 36, RULE_scopedAttribute);
        let mut _localctx: Rc<ScopedAttributeContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(212);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			TraceQLParser_SCOPE_SPAN 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(195);
					recog.base.match_token(TraceQLParser_SCOPE_SPAN,&mut recog.err_handler)?;

					recog.base.set_state(196);
					recog.base.match_token(TraceQLParser_DOT,&mut recog.err_handler)?;

					/*InvokeRule identChain*/
					recog.base.set_state(197);
					recog.identChain()?;

					}
				}

			TraceQLParser_SCOPE_RESOURCE 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(198);
					recog.base.match_token(TraceQLParser_SCOPE_RESOURCE,&mut recog.err_handler)?;

					recog.base.set_state(199);
					recog.base.match_token(TraceQLParser_DOT,&mut recog.err_handler)?;

					/*InvokeRule identChain*/
					recog.base.set_state(200);
					recog.identChain()?;

					}
				}

			TraceQLParser_SCOPE_EVENT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(201);
					recog.base.match_token(TraceQLParser_SCOPE_EVENT,&mut recog.err_handler)?;

					recog.base.set_state(202);
					recog.base.match_token(TraceQLParser_DOT,&mut recog.err_handler)?;

					/*InvokeRule identChain*/
					recog.base.set_state(203);
					recog.identChain()?;

					}
				}

			TraceQLParser_SCOPE_LINK 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					recog.base.set_state(204);
					recog.base.match_token(TraceQLParser_SCOPE_LINK,&mut recog.err_handler)?;

					recog.base.set_state(205);
					recog.base.match_token(TraceQLParser_DOT,&mut recog.err_handler)?;

					/*InvokeRule identChain*/
					recog.base.set_state(206);
					recog.identChain()?;

					}
				}

			TraceQLParser_SCOPE_PARENT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 5)?;
					recog.base.enter_outer_alt(None, 5)?;
					{
					recog.base.set_state(207);
					recog.base.match_token(TraceQLParser_SCOPE_PARENT,&mut recog.err_handler)?;

					recog.base.set_state(208);
					recog.base.match_token(TraceQLParser_DOT,&mut recog.err_handler)?;

					recog.base.set_state(209);
					_la = recog.base.input.la(1);
					if { !(_la==TraceQLParser_SCOPE_SPAN || _la==TraceQLParser_SCOPE_RESOURCE) } {
						recog.err_handler.recover_inline(&mut recog.base)?;

					}
					else {
						if  recog.base.input.la(1)==TOKEN_EOF { recog.base.matched_eof = true };
						recog.err_handler.report_match(&mut recog.base);
						recog.base.consume(&mut recog.err_handler);
					}
					recog.base.set_state(210);
					recog.base.match_token(TraceQLParser_DOT,&mut recog.err_handler)?;

					/*InvokeRule identChain*/
					recog.base.set_state(211);
					recog.identChain()?;

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
//------------------- identChain ----------------
pub type IdentChainContextAll<'input> = IdentChainContext<'input>;


pub type IdentChainContext<'input> = BaseParserRuleContext<'input,IdentChainContextExt<'input>>;

#[derive(Clone)]
pub struct IdentChainContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for IdentChainContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for IdentChainContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_identChain(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_identChain(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for IdentChainContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_identChain(self);
	}
}

impl<'input> CustomRuleContext<'input> for IdentChainContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_identChain }
	//fn type_rule_index() -> usize where Self: Sized { RULE_identChain }
}
antlr4rust::tid!{IdentChainContextExt<'a>}

impl<'input> IdentChainContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<IdentChainContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,IdentChainContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait IdentChainContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<IdentChainContextExt<'input>>{

fn identPart_all(&self) ->  Vec<Rc<IdentPartContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn identPart(&self, i: usize) -> Option<Rc<IdentPartContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token DOT in current rule
fn DOT_all(&self) -> Vec<Rc<TerminalNode<'input,TraceQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token DOT, starting from 0.
/// Returns `None` if number of children corresponding to token DOT is less or equal than `i`.
fn DOT(&self, i: usize) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_DOT, i)
}

}

impl<'input> IdentChainContextAttrs<'input> for IdentChainContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn identChain(&mut self,)
	-> Result<Rc<IdentChainContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = IdentChainContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 38, RULE_identChain);
        let mut _localctx: Rc<IdentChainContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule identPart*/
			recog.base.set_state(214);
			recog.identPart()?;

			recog.base.set_state(219);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			while _la==TraceQLParser_DOT {
				{
				{
				recog.base.set_state(215);
				recog.base.match_token(TraceQLParser_DOT,&mut recog.err_handler)?;

				/*InvokeRule identPart*/
				recog.base.set_state(216);
				recog.identPart()?;

				}
				}
				recog.base.set_state(221);
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
//------------------- identPart ----------------
pub type IdentPartContextAll<'input> = IdentPartContext<'input>;


pub type IdentPartContext<'input> = BaseParserRuleContext<'input,IdentPartContextExt<'input>>;

#[derive(Clone)]
pub struct IdentPartContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for IdentPartContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for IdentPartContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_identPart(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_identPart(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for IdentPartContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_identPart(self);
	}
}

impl<'input> CustomRuleContext<'input> for IdentPartContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_identPart }
	//fn type_rule_index() -> usize where Self: Sized { RULE_identPart }
}
antlr4rust::tid!{IdentPartContextExt<'a>}

impl<'input> IdentPartContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<IdentPartContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,IdentPartContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait IdentPartContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<IdentPartContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token IDENT
/// Returns `None` if there is no child corresponding to token IDENT
fn IDENT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_IDENT, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_NAME
/// Returns `None` if there is no child corresponding to token INTR_NAME
fn INTR_NAME(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_NAME, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_STATUS
/// Returns `None` if there is no child corresponding to token INTR_STATUS
fn INTR_STATUS(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_STATUS, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_STATUS_MESSAGE
/// Returns `None` if there is no child corresponding to token INTR_STATUS_MESSAGE
fn INTR_STATUS_MESSAGE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_STATUS_MESSAGE, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_KIND
/// Returns `None` if there is no child corresponding to token INTR_KIND
fn INTR_KIND(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_KIND, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_DURATION
/// Returns `None` if there is no child corresponding to token INTR_DURATION
fn INTR_DURATION(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_DURATION, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_TRACE_DURATION
/// Returns `None` if there is no child corresponding to token INTR_TRACE_DURATION
fn INTR_TRACE_DURATION(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_TRACE_DURATION, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_ROOT_NAME
/// Returns `None` if there is no child corresponding to token INTR_ROOT_NAME
fn INTR_ROOT_NAME(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_ROOT_NAME, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_ROOT_SVC
/// Returns `None` if there is no child corresponding to token INTR_ROOT_SVC
fn INTR_ROOT_SVC(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_ROOT_SVC, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_TRACE_ID
/// Returns `None` if there is no child corresponding to token INTR_TRACE_ID
fn INTR_TRACE_ID(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_TRACE_ID, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_SPAN_ID
/// Returns `None` if there is no child corresponding to token INTR_SPAN_ID
fn INTR_SPAN_ID(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_SPAN_ID, 0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_SPAN
/// Returns `None` if there is no child corresponding to token SCOPE_SPAN
fn SCOPE_SPAN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_SPAN, 0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_RESOURCE
/// Returns `None` if there is no child corresponding to token SCOPE_RESOURCE
fn SCOPE_RESOURCE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_RESOURCE, 0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_EVENT
/// Returns `None` if there is no child corresponding to token SCOPE_EVENT
fn SCOPE_EVENT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_EVENT, 0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_LINK
/// Returns `None` if there is no child corresponding to token SCOPE_LINK
fn SCOPE_LINK(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_LINK, 0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_PARENT
/// Returns `None` if there is no child corresponding to token SCOPE_PARENT
fn SCOPE_PARENT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_PARENT, 0)
}
/// Retrieves first TerminalNode corresponding to token SCOPE_TRACE
/// Returns `None` if there is no child corresponding to token SCOPE_TRACE
fn SCOPE_TRACE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_SCOPE_TRACE, 0)
}
/// Retrieves first TerminalNode corresponding to token STATUS_OK
/// Returns `None` if there is no child corresponding to token STATUS_OK
fn STATUS_OK(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_STATUS_OK, 0)
}
/// Retrieves first TerminalNode corresponding to token STATUS_ERROR
/// Returns `None` if there is no child corresponding to token STATUS_ERROR
fn STATUS_ERROR(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_STATUS_ERROR, 0)
}
/// Retrieves first TerminalNode corresponding to token STATUS_UNSET
/// Returns `None` if there is no child corresponding to token STATUS_UNSET
fn STATUS_UNSET(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_STATUS_UNSET, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_SERVER
/// Returns `None` if there is no child corresponding to token KIND_SERVER
fn KIND_SERVER(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_SERVER, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_CLIENT
/// Returns `None` if there is no child corresponding to token KIND_CLIENT
fn KIND_CLIENT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_CLIENT, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_PRODUCER
/// Returns `None` if there is no child corresponding to token KIND_PRODUCER
fn KIND_PRODUCER(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_PRODUCER, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_CONSUMER
/// Returns `None` if there is no child corresponding to token KIND_CONSUMER
fn KIND_CONSUMER(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_CONSUMER, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_INTERNAL
/// Returns `None` if there is no child corresponding to token KIND_INTERNAL
fn KIND_INTERNAL(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_INTERNAL, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_COUNT
/// Returns `None` if there is no child corresponding to token FN_COUNT
fn FN_COUNT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_COUNT, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_SUM
/// Returns `None` if there is no child corresponding to token FN_SUM
fn FN_SUM(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_SUM, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_AVG
/// Returns `None` if there is no child corresponding to token FN_AVG
fn FN_AVG(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_AVG, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_MIN
/// Returns `None` if there is no child corresponding to token FN_MIN
fn FN_MIN(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_MIN, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_MAX
/// Returns `None` if there is no child corresponding to token FN_MAX
fn FN_MAX(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_MAX, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_QUANTILE
/// Returns `None` if there is no child corresponding to token FN_QUANTILE
fn FN_QUANTILE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_QUANTILE, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_RATE
/// Returns `None` if there is no child corresponding to token FN_RATE
fn FN_RATE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_RATE, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_COUNT_OVER_TIME
/// Returns `None` if there is no child corresponding to token FN_COUNT_OVER_TIME
fn FN_COUNT_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_COUNT_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token FN_HISTOGRAM_OVER_TIME
/// Returns `None` if there is no child corresponding to token FN_HISTOGRAM_OVER_TIME
fn FN_HISTOGRAM_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FN_HISTOGRAM_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token KW_BY
/// Returns `None` if there is no child corresponding to token KW_BY
fn KW_BY(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KW_BY, 0)
}
/// Retrieves first TerminalNode corresponding to token KW_TRUE
/// Returns `None` if there is no child corresponding to token KW_TRUE
fn KW_TRUE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KW_TRUE, 0)
}
/// Retrieves first TerminalNode corresponding to token KW_FALSE
/// Returns `None` if there is no child corresponding to token KW_FALSE
fn KW_FALSE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KW_FALSE, 0)
}
/// Retrieves first TerminalNode corresponding to token KW_NIL
/// Returns `None` if there is no child corresponding to token KW_NIL
fn KW_NIL(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KW_NIL, 0)
}

}

impl<'input> IdentPartContextAttrs<'input> for IdentPartContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn identPart(&mut self,)
	-> Result<Rc<IdentPartContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = IdentPartContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 40, RULE_identPart);
        let mut _localctx: Rc<IdentPartContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(222);
			_la = recog.base.input.la(1);
			if { !(((((_la - 32)) & !0x3f) == 0 && ((1usize << (_la - 32)) & 4294967295) != 0) || ((((_la - 64)) & !0x3f) == 0 && ((1usize << (_la - 64)) & 1055) != 0)) } {
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
//------------------- intrinsic ----------------
pub type IntrinsicContextAll<'input> = IntrinsicContext<'input>;


pub type IntrinsicContext<'input> = BaseParserRuleContext<'input,IntrinsicContextExt<'input>>;

#[derive(Clone)]
pub struct IntrinsicContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for IntrinsicContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for IntrinsicContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_intrinsic(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_intrinsic(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for IntrinsicContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_intrinsic(self);
	}
}

impl<'input> CustomRuleContext<'input> for IntrinsicContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_intrinsic }
	//fn type_rule_index() -> usize where Self: Sized { RULE_intrinsic }
}
antlr4rust::tid!{IntrinsicContextExt<'a>}

impl<'input> IntrinsicContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<IntrinsicContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,IntrinsicContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait IntrinsicContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<IntrinsicContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token INTR_NAME
/// Returns `None` if there is no child corresponding to token INTR_NAME
fn INTR_NAME(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_NAME, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_STATUS
/// Returns `None` if there is no child corresponding to token INTR_STATUS
fn INTR_STATUS(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_STATUS, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_STATUS_MESSAGE
/// Returns `None` if there is no child corresponding to token INTR_STATUS_MESSAGE
fn INTR_STATUS_MESSAGE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_STATUS_MESSAGE, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_KIND
/// Returns `None` if there is no child corresponding to token INTR_KIND
fn INTR_KIND(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_KIND, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_DURATION
/// Returns `None` if there is no child corresponding to token INTR_DURATION
fn INTR_DURATION(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_DURATION, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_TRACE_DURATION
/// Returns `None` if there is no child corresponding to token INTR_TRACE_DURATION
fn INTR_TRACE_DURATION(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_TRACE_DURATION, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_ROOT_NAME
/// Returns `None` if there is no child corresponding to token INTR_ROOT_NAME
fn INTR_ROOT_NAME(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_ROOT_NAME, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_ROOT_SVC
/// Returns `None` if there is no child corresponding to token INTR_ROOT_SVC
fn INTR_ROOT_SVC(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_ROOT_SVC, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_TRACE_ID
/// Returns `None` if there is no child corresponding to token INTR_TRACE_ID
fn INTR_TRACE_ID(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_TRACE_ID, 0)
}
/// Retrieves first TerminalNode corresponding to token INTR_SPAN_ID
/// Returns `None` if there is no child corresponding to token INTR_SPAN_ID
fn INTR_SPAN_ID(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INTR_SPAN_ID, 0)
}

}

impl<'input> IntrinsicContextAttrs<'input> for IntrinsicContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn intrinsic(&mut self,)
	-> Result<Rc<IntrinsicContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = IntrinsicContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 42, RULE_intrinsic);
        let mut _localctx: Rc<IntrinsicContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(224);
			_la = recog.base.input.la(1);
			if { !(((((_la - 38)) & !0x3f) == 0 && ((1usize << (_la - 38)) & 1023) != 0)) } {
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
//------------------- literal ----------------
pub type LiteralContextAll<'input> = LiteralContext<'input>;


pub type LiteralContext<'input> = BaseParserRuleContext<'input,LiteralContextExt<'input>>;

#[derive(Clone)]
pub struct LiteralContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> TraceQLParserContext<'input> for LiteralContext<'input>{}

impl<'input,'a> Listenable<dyn TraceQLParserListener<'input> + 'a> for LiteralContext<'input>{
		fn enter(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_literal(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn TraceQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_literal(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn TraceQLParserVisitor<'input> + 'a> for LiteralContext<'input>{
	fn accept(&self,visitor: &mut (dyn TraceQLParserVisitor<'input> + 'a)) {
		visitor.visit_literal(self);
	}
}

impl<'input> CustomRuleContext<'input> for LiteralContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = TraceQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_literal }
	//fn type_rule_index() -> usize where Self: Sized { RULE_literal }
}
antlr4rust::tid!{LiteralContextExt<'a>}

impl<'input> LiteralContextExt<'input>{
	fn new(parent: Option<Rc<dyn TraceQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LiteralContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LiteralContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LiteralContextAttrs<'input>: TraceQLParserContext<'input> + BorrowMut<LiteralContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token DURATION
/// Returns `None` if there is no child corresponding to token DURATION
fn DURATION(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_DURATION, 0)
}
/// Retrieves first TerminalNode corresponding to token BYTES
/// Returns `None` if there is no child corresponding to token BYTES
fn BYTES(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_BYTES, 0)
}
/// Retrieves first TerminalNode corresponding to token FLOAT
/// Returns `None` if there is no child corresponding to token FLOAT
fn FLOAT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_FLOAT, 0)
}
/// Retrieves first TerminalNode corresponding to token INT
/// Returns `None` if there is no child corresponding to token INT
fn INT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_INT, 0)
}
/// Retrieves first TerminalNode corresponding to token STRING
/// Returns `None` if there is no child corresponding to token STRING
fn STRING(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_STRING, 0)
}
/// Retrieves first TerminalNode corresponding to token KW_TRUE
/// Returns `None` if there is no child corresponding to token KW_TRUE
fn KW_TRUE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KW_TRUE, 0)
}
/// Retrieves first TerminalNode corresponding to token KW_FALSE
/// Returns `None` if there is no child corresponding to token KW_FALSE
fn KW_FALSE(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KW_FALSE, 0)
}
/// Retrieves first TerminalNode corresponding to token KW_NIL
/// Returns `None` if there is no child corresponding to token KW_NIL
fn KW_NIL(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KW_NIL, 0)
}
/// Retrieves first TerminalNode corresponding to token STATUS_OK
/// Returns `None` if there is no child corresponding to token STATUS_OK
fn STATUS_OK(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_STATUS_OK, 0)
}
/// Retrieves first TerminalNode corresponding to token STATUS_ERROR
/// Returns `None` if there is no child corresponding to token STATUS_ERROR
fn STATUS_ERROR(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_STATUS_ERROR, 0)
}
/// Retrieves first TerminalNode corresponding to token STATUS_UNSET
/// Returns `None` if there is no child corresponding to token STATUS_UNSET
fn STATUS_UNSET(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_STATUS_UNSET, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_SERVER
/// Returns `None` if there is no child corresponding to token KIND_SERVER
fn KIND_SERVER(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_SERVER, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_CLIENT
/// Returns `None` if there is no child corresponding to token KIND_CLIENT
fn KIND_CLIENT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_CLIENT, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_PRODUCER
/// Returns `None` if there is no child corresponding to token KIND_PRODUCER
fn KIND_PRODUCER(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_PRODUCER, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_CONSUMER
/// Returns `None` if there is no child corresponding to token KIND_CONSUMER
fn KIND_CONSUMER(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_CONSUMER, 0)
}
/// Retrieves first TerminalNode corresponding to token KIND_INTERNAL
/// Returns `None` if there is no child corresponding to token KIND_INTERNAL
fn KIND_INTERNAL(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_KIND_INTERNAL, 0)
}
/// Retrieves first TerminalNode corresponding to token IDENT
/// Returns `None` if there is no child corresponding to token IDENT
fn IDENT(&self) -> Option<Rc<TerminalNode<'input,TraceQLParserContextType>>> where Self:Sized{
	self.get_token(TraceQLParser_IDENT, 0)
}

}

impl<'input> LiteralContextAttrs<'input> for LiteralContext<'input>{}

impl<'input, I> TraceQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn literal(&mut self,)
	-> Result<Rc<LiteralContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LiteralContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 44, RULE_literal);
        let mut _localctx: Rc<LiteralContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(226);
			_la = recog.base.input.la(1);
			if { !(((((_la - 48)) & !0x3f) == 0 && ((1usize << (_la - 48)) & 133955839) != 0)) } {
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
		4, 1, 76, 229, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 7, 
		4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 
		7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7, 14, 2, 15, 
		7, 15, 2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7, 19, 2, 20, 
		7, 20, 2, 21, 7, 21, 2, 22, 7, 22, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 
		3, 0, 53, 8, 0, 1, 1, 1, 1, 1, 1, 4, 1, 58, 8, 1, 11, 1, 12, 1, 59, 1, 
		2, 1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 67, 8, 2, 3, 2, 69, 8, 2, 1, 3, 1, 3, 
		1, 3, 1, 3, 1, 3, 5, 3, 76, 8, 3, 10, 3, 12, 3, 79, 9, 3, 1, 3, 1, 3, 
		1, 4, 1, 4, 1, 4, 3, 4, 86, 8, 4, 1, 4, 1, 4, 3, 4, 90, 8, 4, 1, 4, 1, 
		4, 1, 5, 1, 5, 1, 5, 3, 5, 97, 8, 5, 1, 5, 1, 5, 3, 5, 101, 8, 5, 1, 5, 
		1, 5, 1, 5, 1, 5, 1, 6, 1, 6, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 
		1, 7, 1, 7, 1, 7, 1, 7, 3, 7, 120, 8, 7, 1, 8, 1, 8, 1, 9, 1, 9, 1, 9, 
		5, 9, 127, 8, 9, 10, 9, 12, 9, 130, 9, 9, 1, 10, 1, 10, 1, 10, 5, 10, 
		135, 8, 10, 10, 10, 12, 10, 138, 9, 10, 1, 11, 1, 11, 1, 11, 1, 11, 5, 
		11, 144, 8, 11, 10, 11, 12, 11, 147, 9, 11, 1, 12, 1, 12, 1, 12, 1, 12, 
		1, 12, 3, 12, 154, 8, 12, 1, 13, 1, 13, 1, 14, 1, 14, 3, 14, 160, 8, 14, 
		1, 14, 1, 14, 1, 15, 1, 15, 1, 15, 1, 15, 1, 15, 1, 15, 1, 15, 1, 15, 
		1, 15, 1, 15, 1, 15, 3, 15, 175, 8, 15, 1, 15, 1, 15, 1, 15, 1, 15, 1, 
		15, 1, 15, 5, 15, 183, 8, 15, 10, 15, 12, 15, 186, 9, 15, 1, 16, 1, 16, 
		1, 17, 1, 17, 1, 17, 1, 17, 3, 17, 194, 8, 17, 1, 18, 1, 18, 1, 18, 1, 
		18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 1, 
		18, 1, 18, 1, 18, 1, 18, 3, 18, 213, 8, 18, 1, 19, 1, 19, 1, 19, 5, 19, 
		218, 8, 19, 10, 19, 12, 19, 221, 9, 19, 1, 20, 1, 20, 1, 21, 1, 21, 1, 
		22, 1, 22, 1, 22, 0, 1, 30, 23, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 
		22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 0, 7, 1, 0, 56, 61, 2, 
		0, 10, 10, 15, 23, 1, 0, 9, 16, 1, 0, 32, 33, 2, 0, 32, 68, 74, 74, 1, 
		0, 38, 47, 2, 0, 48, 55, 66, 74, 234, 0, 52, 1, 0, 0, 0, 2, 54, 1, 0, 
		0, 0, 4, 68, 1, 0, 0, 0, 6, 70, 1, 0, 0, 0, 8, 82, 1, 0, 0, 0, 10, 93, 
		1, 0, 0, 0, 12, 106, 1, 0, 0, 0, 14, 119, 1, 0, 0, 0, 16, 121, 1, 0, 0, 
		0, 18, 123, 1, 0, 0, 0, 20, 131, 1, 0, 0, 0, 22, 139, 1, 0, 0, 0, 24, 
		153, 1, 0, 0, 0, 26, 155, 1, 0, 0, 0, 28, 157, 1, 0, 0, 0, 30, 174, 1, 
		0, 0, 0, 32, 187, 1, 0, 0, 0, 34, 193, 1, 0, 0, 0, 36, 212, 1, 0, 0, 0, 
		38, 214, 1, 0, 0, 0, 40, 222, 1, 0, 0, 0, 42, 224, 1, 0, 0, 0, 44, 226, 
		1, 0, 0, 0, 46, 47, 3, 2, 1, 0, 47, 48, 5, 0, 0, 1, 48, 53, 1, 0, 0, 0, 
		49, 50, 3, 16, 8, 0, 50, 51, 5, 0, 0, 1, 51, 53, 1, 0, 0, 0, 52, 46, 1, 
		0, 0, 0, 52, 49, 1, 0, 0, 0, 53, 1, 1, 0, 0, 0, 54, 57, 3, 16, 8, 0, 55, 
		56, 5, 8, 0, 0, 56, 58, 3, 4, 2, 0, 57, 55, 1, 0, 0, 0, 58, 59, 1, 0, 
		0, 0, 59, 57, 1, 0, 0, 0, 59, 60, 1, 0, 0, 0, 60, 3, 1, 0, 0, 0, 61, 69, 
		3, 6, 3, 0, 62, 69, 3, 10, 5, 0, 63, 69, 3, 8, 4, 0, 64, 66, 3, 14, 7, 
		0, 65, 67, 3, 6, 3, 0, 66, 65, 1, 0, 0, 0, 66, 67, 1, 0, 0, 0, 67, 69, 
		1, 0, 0, 0, 68, 61, 1, 0, 0, 0, 68, 62, 1, 0, 0, 0, 68, 63, 1, 0, 0, 0, 
		68, 64, 1, 0, 0, 0, 69, 5, 1, 0, 0, 0, 70, 71, 5, 65, 0, 0, 71, 72, 5, 
		3, 0, 0, 72, 77, 3, 34, 17, 0, 73, 74, 5, 5, 0, 0, 74, 76, 3, 34, 17, 
		0, 75, 73, 1, 0, 0, 0, 76, 79, 1, 0, 0, 0, 77, 75, 1, 0, 0, 0, 77, 78, 
		1, 0, 0, 0, 78, 80, 1, 0, 0, 0, 79, 77, 1, 0, 0, 0, 80, 81, 5, 4, 0, 0, 
		81, 7, 1, 0, 0, 0, 82, 83, 3, 12, 6, 0, 83, 85, 5, 3, 0, 0, 84, 86, 3, 
		34, 17, 0, 85, 84, 1, 0, 0, 0, 85, 86, 1, 0, 0, 0, 86, 89, 1, 0, 0, 0, 
		87, 88, 5, 5, 0, 0, 88, 90, 3, 44, 22, 0, 89, 87, 1, 0, 0, 0, 89, 90, 
		1, 0, 0, 0, 90, 91, 1, 0, 0, 0, 91, 92, 5, 4, 0, 0, 92, 9, 1, 0, 0, 0, 
		93, 94, 3, 12, 6, 0, 94, 96, 5, 3, 0, 0, 95, 97, 3, 34, 17, 0, 96, 95, 
		1, 0, 0, 0, 96, 97, 1, 0, 0, 0, 97, 100, 1, 0, 0, 0, 98, 99, 5, 5, 0, 
		0, 99, 101, 3, 44, 22, 0, 100, 98, 1, 0, 0, 0, 100, 101, 1, 0, 0, 0, 101, 
		102, 1, 0, 0, 0, 102, 103, 5, 4, 0, 0, 103, 104, 3, 32, 16, 0, 104, 105, 
		3, 44, 22, 0, 105, 11, 1, 0, 0, 0, 106, 107, 7, 0, 0, 0, 107, 13, 1, 0, 
		0, 0, 108, 109, 5, 62, 0, 0, 109, 110, 5, 3, 0, 0, 110, 120, 5, 4, 0, 
		0, 111, 112, 5, 63, 0, 0, 112, 113, 5, 3, 0, 0, 113, 120, 5, 4, 0, 0, 
		114, 115, 5, 64, 0, 0, 115, 116, 5, 3, 0, 0, 116, 117, 3, 34, 17, 0, 117, 
		118, 5, 4, 0, 0, 118, 120, 1, 0, 0, 0, 119, 108, 1, 0, 0, 0, 119, 111, 
		1, 0, 0, 0, 119, 114, 1, 0, 0, 0, 120, 15, 1, 0, 0, 0, 121, 122, 3, 18, 
		9, 0, 122, 17, 1, 0, 0, 0, 123, 128, 3, 20, 10, 0, 124, 125, 5, 25, 0, 
		0, 125, 127, 3, 20, 10, 0, 126, 124, 1, 0, 0, 0, 127, 130, 1, 0, 0, 0, 
		128, 126, 1, 0, 0, 0, 128, 129, 1, 0, 0, 0, 129, 19, 1, 0, 0, 0, 130, 
		128, 1, 0, 0, 0, 131, 136, 3, 22, 11, 0, 132, 133, 5, 24, 0, 0, 133, 135, 
		3, 22, 11, 0, 134, 132, 1, 0, 0, 0, 135, 138, 1, 0, 0, 0, 136, 134, 1, 
		0, 0, 0, 136, 137, 1, 0, 0, 0, 137, 21, 1, 0, 0, 0, 138, 136, 1, 0, 0, 
		0, 139, 145, 3, 24, 12, 0, 140, 141, 3, 26, 13, 0, 141, 142, 3, 24, 12, 
		0, 142, 144, 1, 0, 0, 0, 143, 140, 1, 0, 0, 0, 144, 147, 1, 0, 0, 0, 145, 
		143, 1, 0, 0, 0, 145, 146, 1, 0, 0, 0, 146, 23, 1, 0, 0, 0, 147, 145, 
		1, 0, 0, 0, 148, 149, 5, 3, 0, 0, 149, 150, 3, 16, 8, 0, 150, 151, 5, 
		4, 0, 0, 151, 154, 1, 0, 0, 0, 152, 154, 3, 28, 14, 0, 153, 148, 1, 0, 
		0, 0, 153, 152, 1, 0, 0, 0, 154, 25, 1, 0, 0, 0, 155, 156, 7, 1, 0, 0, 
		156, 27, 1, 0, 0, 0, 157, 159, 5, 1, 0, 0, 158, 160, 3, 30, 15, 0, 159, 
		158, 1, 0, 0, 0, 159, 160, 1, 0, 0, 0, 160, 161, 1, 0, 0, 0, 161, 162, 
		5, 2, 0, 0, 162, 29, 1, 0, 0, 0, 163, 164, 6, 15, -1, 0, 164, 165, 5, 
		26, 0, 0, 165, 175, 3, 30, 15, 5, 166, 167, 5, 3, 0, 0, 167, 168, 3, 30, 
		15, 0, 168, 169, 5, 4, 0, 0, 169, 175, 1, 0, 0, 0, 170, 171, 3, 34, 17, 
		0, 171, 172, 3, 32, 16, 0, 172, 173, 3, 44, 22, 0, 173, 175, 1, 0, 0, 
		0, 174, 163, 1, 0, 0, 0, 174, 166, 1, 0, 0, 0, 174, 170, 1, 0, 0, 0, 175, 
		184, 1, 0, 0, 0, 176, 177, 10, 3, 0, 0, 177, 178, 5, 24, 0, 0, 178, 183, 
		3, 30, 15, 4, 179, 180, 10, 2, 0, 0, 180, 181, 5, 25, 0, 0, 181, 183, 
		3, 30, 15, 3, 182, 176, 1, 0, 0, 0, 182, 179, 1, 0, 0, 0, 183, 186, 1, 
		0, 0, 0, 184, 182, 1, 0, 0, 0, 184, 185, 1, 0, 0, 0, 185, 31, 1, 0, 0, 
		0, 186, 184, 1, 0, 0, 0, 187, 188, 7, 2, 0, 0, 188, 33, 1, 0, 0, 0, 189, 
		194, 3, 42, 21, 0, 190, 194, 3, 36, 18, 0, 191, 192, 5, 6, 0, 0, 192, 
		194, 3, 38, 19, 0, 193, 189, 1, 0, 0, 0, 193, 190, 1, 0, 0, 0, 193, 191, 
		1, 0, 0, 0, 194, 35, 1, 0, 0, 0, 195, 196, 5, 32, 0, 0, 196, 197, 5, 6, 
		0, 0, 197, 213, 3, 38, 19, 0, 198, 199, 5, 33, 0, 0, 199, 200, 5, 6, 0, 
		0, 200, 213, 3, 38, 19, 0, 201, 202, 5, 34, 0, 0, 202, 203, 5, 6, 0, 0, 
		203, 213, 3, 38, 19, 0, 204, 205, 5, 35, 0, 0, 205, 206, 5, 6, 0, 0, 206, 
		213, 3, 38, 19, 0, 207, 208, 5, 36, 0, 0, 208, 209, 5, 6, 0, 0, 209, 210, 
		7, 3, 0, 0, 210, 211, 5, 6, 0, 0, 211, 213, 3, 38, 19, 0, 212, 195, 1, 
		0, 0, 0, 212, 198, 1, 0, 0, 0, 212, 201, 1, 0, 0, 0, 212, 204, 1, 0, 0, 
		0, 212, 207, 1, 0, 0, 0, 213, 37, 1, 0, 0, 0, 214, 219, 3, 40, 20, 0, 
		215, 216, 5, 6, 0, 0, 216, 218, 3, 40, 20, 0, 217, 215, 1, 0, 0, 0, 218, 
		221, 1, 0, 0, 0, 219, 217, 1, 0, 0, 0, 219, 220, 1, 0, 0, 0, 220, 39, 
		1, 0, 0, 0, 221, 219, 1, 0, 0, 0, 222, 223, 7, 4, 0, 0, 223, 41, 1, 0, 
		0, 0, 224, 225, 7, 5, 0, 0, 225, 43, 1, 0, 0, 0, 226, 227, 7, 6, 0, 0, 
		227, 45, 1, 0, 0, 0, 21, 52, 59, 66, 68, 77, 85, 89, 96, 100, 119, 128, 
		136, 145, 153, 159, 174, 182, 184, 193, 212, 219
	];
}
