#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
// Generated from antlr/PromQLParser.g4 by ANTLR 4.13.2
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
use super::promqlparserlistener::*;
use super::promqlparservisitor::*;

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

		pub const PromQLParser_LPAREN:i32=1; 
		pub const PromQLParser_RPAREN:i32=2; 
		pub const PromQLParser_LBRACE:i32=3; 
		pub const PromQLParser_RBRACE:i32=4; 
		pub const PromQLParser_LBRACK:i32=5; 
		pub const PromQLParser_RBRACK:i32=6; 
		pub const PromQLParser_COLON:i32=7; 
		pub const PromQLParser_COMMA:i32=8; 
		pub const PromQLParser_ADD:i32=9; 
		pub const PromQLParser_SUB:i32=10; 
		pub const PromQLParser_MUL:i32=11; 
		pub const PromQLParser_DIV:i32=12; 
		pub const PromQLParser_POW:i32=13; 
		pub const PromQLParser_EQ:i32=14; 
		pub const PromQLParser_NE:i32=15; 
		pub const PromQLParser_RE:i32=16; 
		pub const PromQLParser_NRE:i32=17; 
		pub const PromQLParser_GT:i32=18; 
		pub const PromQLParser_LT:i32=19; 
		pub const PromQLParser_GE:i32=20; 
		pub const PromQLParser_LE:i32=21; 
		pub const PromQLParser_EQL:i32=22; 
		pub const PromQLParser_AND:i32=23; 
		pub const PromQLParser_OR:i32=24; 
		pub const PromQLParser_UNLESS:i32=25; 
		pub const PromQLParser_BOOL:i32=26; 
		pub const PromQLParser_BY:i32=27; 
		pub const PromQLParser_WITHOUT:i32=28; 
		pub const PromQLParser_ON:i32=29; 
		pub const PromQLParser_IGNORING:i32=30; 
		pub const PromQLParser_GROUP_LEFT:i32=31; 
		pub const PromQLParser_GROUP_RIGHT:i32=32; 
		pub const PromQLParser_OFFSET:i32=33; 
		pub const PromQLParser_AT:i32=34; 
		pub const PromQLParser_START:i32=35; 
		pub const PromQLParser_END:i32=36; 
		pub const PromQLParser_MOD:i32=37; 
		pub const PromQLParser_AGG_SUM:i32=38; 
		pub const PromQLParser_AGG_MIN:i32=39; 
		pub const PromQLParser_AGG_MAX:i32=40; 
		pub const PromQLParser_AGG_AVG:i32=41; 
		pub const PromQLParser_AGG_GROUP:i32=42; 
		pub const PromQLParser_AGG_STDDEV:i32=43; 
		pub const PromQLParser_AGG_STDVAR:i32=44; 
		pub const PromQLParser_AGG_COUNT:i32=45; 
		pub const PromQLParser_AGG_COUNT_VALUES:i32=46; 
		pub const PromQLParser_AGG_BOTTOMK:i32=47; 
		pub const PromQLParser_AGG_TOPK:i32=48; 
		pub const PromQLParser_AGG_QUANTILE:i32=49; 
		pub const PromQLParser_RATE:i32=50; 
		pub const PromQLParser_IRATE:i32=51; 
		pub const PromQLParser_INCREASE:i32=52; 
		pub const PromQLParser_DELTA:i32=53; 
		pub const PromQLParser_IDELTA:i32=54; 
		pub const PromQLParser_DERIV:i32=55; 
		pub const PromQLParser_CHANGES:i32=56; 
		pub const PromQLParser_RESETS:i32=57; 
		pub const PromQLParser_PREDICT_LINEAR:i32=58; 
		pub const PromQLParser_HOLT_WINTERS:i32=59; 
		pub const PromQLParser_AVG_OVER_TIME:i32=60; 
		pub const PromQLParser_MIN_OVER_TIME:i32=61; 
		pub const PromQLParser_MAX_OVER_TIME:i32=62; 
		pub const PromQLParser_SUM_OVER_TIME:i32=63; 
		pub const PromQLParser_COUNT_OVER_TIME:i32=64; 
		pub const PromQLParser_QUANTILE_OVER_TIME:i32=65; 
		pub const PromQLParser_STDDEV_OVER_TIME:i32=66; 
		pub const PromQLParser_STDVAR_OVER_TIME:i32=67; 
		pub const PromQLParser_LAST_OVER_TIME:i32=68; 
		pub const PromQLParser_PRESENT_OVER_TIME:i32=69; 
		pub const PromQLParser_ABSENT_OVER_TIME:i32=70; 
		pub const PromQLParser_ABS:i32=71; 
		pub const PromQLParser_ABSENT:i32=72; 
		pub const PromQLParser_CEIL:i32=73; 
		pub const PromQLParser_CLAMP:i32=74; 
		pub const PromQLParser_CLAMP_MAX:i32=75; 
		pub const PromQLParser_CLAMP_MIN:i32=76; 
		pub const PromQLParser_EXP:i32=77; 
		pub const PromQLParser_FLOOR:i32=78; 
		pub const PromQLParser_LN:i32=79; 
		pub const PromQLParser_LOG2:i32=80; 
		pub const PromQLParser_LOG10:i32=81; 
		pub const PromQLParser_ROUND:i32=82; 
		pub const PromQLParser_SCALAR:i32=83; 
		pub const PromQLParser_SGN:i32=84; 
		pub const PromQLParser_SORT:i32=85; 
		pub const PromQLParser_SORT_DESC:i32=86; 
		pub const PromQLParser_SQRT:i32=87; 
		pub const PromQLParser_ACOS:i32=88; 
		pub const PromQLParser_ACOSH:i32=89; 
		pub const PromQLParser_ASIN:i32=90; 
		pub const PromQLParser_ASINH:i32=91; 
		pub const PromQLParser_ATAN:i32=92; 
		pub const PromQLParser_ATANH:i32=93; 
		pub const PromQLParser_COS:i32=94; 
		pub const PromQLParser_COSH:i32=95; 
		pub const PromQLParser_SIN:i32=96; 
		pub const PromQLParser_SINH:i32=97; 
		pub const PromQLParser_TAN:i32=98; 
		pub const PromQLParser_TANH:i32=99; 
		pub const PromQLParser_DEG:i32=100; 
		pub const PromQLParser_RAD:i32=101; 
		pub const PromQLParser_PI:i32=102; 
		pub const PromQLParser_LABEL_JOIN:i32=103; 
		pub const PromQLParser_LABEL_REPLACE:i32=104; 
		pub const PromQLParser_TIME:i32=105; 
		pub const PromQLParser_TIMESTAMP:i32=106; 
		pub const PromQLParser_DAY_OF_MONTH:i32=107; 
		pub const PromQLParser_DAY_OF_WEEK:i32=108; 
		pub const PromQLParser_DAY_OF_YEAR:i32=109; 
		pub const PromQLParser_DAYS_IN_MONTH:i32=110; 
		pub const PromQLParser_HOUR:i32=111; 
		pub const PromQLParser_MINUTE:i32=112; 
		pub const PromQLParser_MONTH:i32=113; 
		pub const PromQLParser_YEAR:i32=114; 
		pub const PromQLParser_HISTOGRAM_COUNT:i32=115; 
		pub const PromQLParser_HISTOGRAM_SUM:i32=116; 
		pub const PromQLParser_HISTOGRAM_FRACTION:i32=117; 
		pub const PromQLParser_HISTOGRAM_QUANTILE:i32=118; 
		pub const PromQLParser_VECTOR:i32=119; 
		pub const PromQLParser_DURATION_TYPE_MILLISECOND:i32=120; 
		pub const PromQLParser_DURATION_TYPE_YEAR:i32=121; 
		pub const PromQLParser_DURATION_TYPE_WEEK:i32=122; 
		pub const PromQLParser_DURATION_TYPE_DAY:i32=123; 
		pub const PromQLParser_DURATION_TYPE_HOUR:i32=124; 
		pub const PromQLParser_DURATION_TYPE_MINUTE:i32=125; 
		pub const PromQLParser_DURATION_TYPE_SECOND:i32=126; 
		pub const PromQLParser_NUMBER:i32=127; 
		pub const PromQLParser_DURATION:i32=128; 
		pub const PromQLParser_STRING:i32=129; 
		pub const PromQLParser_IDENTIFIER:i32=130; 
		pub const PromQLParser_WS:i32=131; 
		pub const PromQLParser_SL_COMMENT:i32=132;
	pub const PromQLParser_EOF:i32=EOF;
	pub const RULE_root:usize = 0; 
	pub const RULE_expr:usize = 1; 
	pub const RULE_vector:usize = 2; 
	pub const RULE_parenExpr:usize = 3; 
	pub const RULE_unaryExpr:usize = 4; 
	pub const RULE_unaryOp:usize = 5; 
	pub const RULE_mulDivModOp:usize = 6; 
	pub const RULE_addSubOp:usize = 7; 
	pub const RULE_compareOp:usize = 8; 
	pub const RULE_andUnlessOp:usize = 9; 
	pub const RULE_binOpModifier:usize = 10; 
	pub const RULE_onOrIgnoring:usize = 11; 
	pub const RULE_groupModifier:usize = 12; 
	pub const RULE_instantSelector:usize = 13; 
	pub const RULE_matrixSelector:usize = 14; 
	pub const RULE_labelMatchers:usize = 15; 
	pub const RULE_labelMatcher:usize = 16; 
	pub const RULE_labelMatcherOp:usize = 17; 
	pub const RULE_functionCall:usize = 18; 
	pub const RULE_functionName:usize = 19; 
	pub const RULE_rangeFunctionName:usize = 20; 
	pub const RULE_mathFunctionName:usize = 21; 
	pub const RULE_trigFunctionName:usize = 22; 
	pub const RULE_labelFunctionName:usize = 23; 
	pub const RULE_timeFunctionName:usize = 24; 
	pub const RULE_histogramFunctionName:usize = 25; 
	pub const RULE_aggregation:usize = 26; 
	pub const RULE_aggregationOp:usize = 27; 
	pub const RULE_grouping:usize = 28; 
	pub const RULE_timeRange:usize = 29; 
	pub const RULE_offsetModifier:usize = 30; 
	pub const RULE_atModifier:usize = 31; 
	pub const RULE_labelName:usize = 32; 
	pub const RULE_labelNameList:usize = 33; 
	pub const RULE_keyword:usize = 34; 
	pub const RULE_numberLiteral:usize = 35; 
	pub const RULE_stringLiteral:usize = 36; 
	pub const RULE_duration:usize = 37;
	pub const ruleNames: [&'static str; 38] =  [
		"root", "expr", "vector", "parenExpr", "unaryExpr", "unaryOp", "mulDivModOp", 
		"addSubOp", "compareOp", "andUnlessOp", "binOpModifier", "onOrIgnoring", 
		"groupModifier", "instantSelector", "matrixSelector", "labelMatchers", 
		"labelMatcher", "labelMatcherOp", "functionCall", "functionName", "rangeFunctionName", 
		"mathFunctionName", "trigFunctionName", "labelFunctionName", "timeFunctionName", 
		"histogramFunctionName", "aggregation", "aggregationOp", "grouping", "timeRange", 
		"offsetModifier", "atModifier", "labelName", "labelNameList", "keyword", 
		"numberLiteral", "stringLiteral", "duration"
	];


	pub const _LITERAL_NAMES: [Option<&'static str>;127] = [
		None, Some("'('"), Some("')'"), Some("'{'"), Some("'}'"), Some("'['"), 
		Some("']'"), Some("':'"), Some("','"), Some("'+'"), Some("'-'"), Some("'*'"), 
		Some("'/'"), Some("'^'"), Some("'='"), Some("'!='"), Some("'=~'"), Some("'!~'"), 
		Some("'>'"), Some("'<'"), Some("'>='"), Some("'<='"), Some("'=='"), Some("'and'"), 
		Some("'or'"), Some("'unless'"), Some("'bool'"), Some("'by'"), Some("'without'"), 
		Some("'on'"), Some("'ignoring'"), Some("'group_left'"), Some("'group_right'"), 
		Some("'offset'"), Some("'@'"), Some("'start'"), Some("'end'"), Some("'%'"), 
		Some("'sum'"), Some("'min'"), Some("'max'"), Some("'avg'"), Some("'group'"), 
		Some("'stddev'"), Some("'stdvar'"), Some("'count'"), Some("'count_values'"), 
		Some("'bottomk'"), Some("'topk'"), Some("'quantile'"), Some("'rate'"), 
		Some("'irate'"), Some("'increase'"), Some("'delta'"), Some("'idelta'"), 
		Some("'deriv'"), Some("'changes'"), Some("'resets'"), Some("'predict_linear'"), 
		Some("'holt_winters'"), Some("'avg_over_time'"), Some("'min_over_time'"), 
		Some("'max_over_time'"), Some("'sum_over_time'"), Some("'count_over_time'"), 
		Some("'quantile_over_time'"), Some("'stddev_over_time'"), Some("'stdvar_over_time'"), 
		Some("'last_over_time'"), Some("'present_over_time'"), Some("'absent_over_time'"), 
		Some("'abs'"), Some("'absent'"), Some("'ceil'"), Some("'clamp'"), Some("'clamp_max'"), 
		Some("'clamp_min'"), Some("'exp'"), Some("'floor'"), Some("'ln'"), Some("'log2'"), 
		Some("'log10'"), Some("'round'"), Some("'scalar'"), Some("'sgn'"), Some("'sort'"), 
		Some("'sort_desc'"), Some("'sqrt'"), Some("'acos'"), Some("'acosh'"), 
		Some("'asin'"), Some("'asinh'"), Some("'atan'"), Some("'atanh'"), Some("'cos'"), 
		Some("'cosh'"), Some("'sin'"), Some("'sinh'"), Some("'tan'"), Some("'tanh'"), 
		Some("'deg'"), Some("'rad'"), Some("'pi'"), Some("'label_join'"), Some("'label_replace'"), 
		Some("'time'"), Some("'timestamp'"), Some("'day_of_month'"), Some("'day_of_week'"), 
		Some("'day_of_year'"), Some("'days_in_month'"), Some("'hour'"), Some("'minute'"), 
		Some("'month'"), Some("'year'"), Some("'histogram_count'"), Some("'histogram_sum'"), 
		Some("'histogram_fraction'"), Some("'histogram_quantile'"), Some("'vector'"), 
		Some("'ms'"), Some("'y'"), Some("'w'"), Some("'d'"), Some("'h'"), Some("'m'"), 
		Some("'s'")
	];
	pub const _SYMBOLIC_NAMES: [Option<&'static str>;133]  = [
		None, Some("LPAREN"), Some("RPAREN"), Some("LBRACE"), Some("RBRACE"), 
		Some("LBRACK"), Some("RBRACK"), Some("COLON"), Some("COMMA"), Some("ADD"), 
		Some("SUB"), Some("MUL"), Some("DIV"), Some("POW"), Some("EQ"), Some("NE"), 
		Some("RE"), Some("NRE"), Some("GT"), Some("LT"), Some("GE"), Some("LE"), 
		Some("EQL"), Some("AND"), Some("OR"), Some("UNLESS"), Some("BOOL"), Some("BY"), 
		Some("WITHOUT"), Some("ON"), Some("IGNORING"), Some("GROUP_LEFT"), Some("GROUP_RIGHT"), 
		Some("OFFSET"), Some("AT"), Some("START"), Some("END"), Some("MOD"), Some("AGG_SUM"), 
		Some("AGG_MIN"), Some("AGG_MAX"), Some("AGG_AVG"), Some("AGG_GROUP"), 
		Some("AGG_STDDEV"), Some("AGG_STDVAR"), Some("AGG_COUNT"), Some("AGG_COUNT_VALUES"), 
		Some("AGG_BOTTOMK"), Some("AGG_TOPK"), Some("AGG_QUANTILE"), Some("RATE"), 
		Some("IRATE"), Some("INCREASE"), Some("DELTA"), Some("IDELTA"), Some("DERIV"), 
		Some("CHANGES"), Some("RESETS"), Some("PREDICT_LINEAR"), Some("HOLT_WINTERS"), 
		Some("AVG_OVER_TIME"), Some("MIN_OVER_TIME"), Some("MAX_OVER_TIME"), Some("SUM_OVER_TIME"), 
		Some("COUNT_OVER_TIME"), Some("QUANTILE_OVER_TIME"), Some("STDDEV_OVER_TIME"), 
		Some("STDVAR_OVER_TIME"), Some("LAST_OVER_TIME"), Some("PRESENT_OVER_TIME"), 
		Some("ABSENT_OVER_TIME"), Some("ABS"), Some("ABSENT"), Some("CEIL"), Some("CLAMP"), 
		Some("CLAMP_MAX"), Some("CLAMP_MIN"), Some("EXP"), Some("FLOOR"), Some("LN"), 
		Some("LOG2"), Some("LOG10"), Some("ROUND"), Some("SCALAR"), Some("SGN"), 
		Some("SORT"), Some("SORT_DESC"), Some("SQRT"), Some("ACOS"), Some("ACOSH"), 
		Some("ASIN"), Some("ASINH"), Some("ATAN"), Some("ATANH"), Some("COS"), 
		Some("COSH"), Some("SIN"), Some("SINH"), Some("TAN"), Some("TANH"), Some("DEG"), 
		Some("RAD"), Some("PI"), Some("LABEL_JOIN"), Some("LABEL_REPLACE"), Some("TIME"), 
		Some("TIMESTAMP"), Some("DAY_OF_MONTH"), Some("DAY_OF_WEEK"), Some("DAY_OF_YEAR"), 
		Some("DAYS_IN_MONTH"), Some("HOUR"), Some("MINUTE"), Some("MONTH"), Some("YEAR"), 
		Some("HISTOGRAM_COUNT"), Some("HISTOGRAM_SUM"), Some("HISTOGRAM_FRACTION"), 
		Some("HISTOGRAM_QUANTILE"), Some("VECTOR"), Some("DURATION_TYPE_MILLISECOND"), 
		Some("DURATION_TYPE_YEAR"), Some("DURATION_TYPE_WEEK"), Some("DURATION_TYPE_DAY"), 
		Some("DURATION_TYPE_HOUR"), Some("DURATION_TYPE_MINUTE"), Some("DURATION_TYPE_SECOND"), 
		Some("NUMBER"), Some("DURATION"), Some("STRING"), Some("IDENTIFIER"), 
		Some("WS"), Some("SL_COMMENT")
	];
	lazy_static!{
	    static ref _shared_context_cache: Arc<PredictionContextCache> = Arc::new(PredictionContextCache::new());
		static ref VOCABULARY: Box<dyn Vocabulary> = Box::new(VocabularyImpl::new(_LITERAL_NAMES.iter(), _SYMBOLIC_NAMES.iter(), None));
	}


type BaseParserType<'input, I> =
	BaseParser<'input,PromQLParserExt<'input>, I, PromQLParserContextType , dyn PromQLParserListener<'input> + 'input >;

type TokenType<'input> = <LocalTokenFactory<'input> as TokenFactory<'input>>::Tok;
pub type LocalTokenFactory<'input> = CommonTokenFactory;

pub type PromQLParserTreeWalker<'input,'a> =
	ParseTreeWalker<'input, 'a, PromQLParserContextType , dyn PromQLParserListener<'input> + 'a>;

/// Parser for PromQLParser grammar
pub struct PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	base:BaseParserType<'input,I>,
	interpreter:Arc<ParserATNSimulator>,
	_shared_context_cache: Box<PredictionContextCache>,
    pub err_handler: Box<dyn ErrorStrategy<'input,BaseParserType<'input,I> > >,
}

impl<'input, I> PromQLParser<'input, I>
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
				PromQLParserExt{
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

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    pub fn with_dyn_strategy(input: I) -> Self{
    	Self::with_strategy(input,Box::new(DefaultErrorStrategy::new()))
    }
}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    pub fn new(input: I) -> Self{
    	Self::with_strategy(input,Box::new(DefaultErrorStrategy::new()))
    }
}

/// Trait for monomorphized trait object that corresponds to the nodes of parse tree generated for PromQLParser
pub trait PromQLParserContext<'input>:
	for<'x> Listenable<dyn PromQLParserListener<'input> + 'x > + 
	for<'x> Visitable<dyn PromQLParserVisitor<'input> + 'x > + 
	ParserRuleContext<'input, TF=LocalTokenFactory<'input>, Ctx=PromQLParserContextType>
{}

antlr4rust::coerce_from!{ 'input : PromQLParserContext<'input> }

impl<'input, 'x, T> VisitableDyn<T> for dyn PromQLParserContext<'input> + 'input
where
    T: PromQLParserVisitor<'input> + 'x,
{
    fn accept_dyn(&self, visitor: &mut T) {
        self.accept(visitor as &mut (dyn PromQLParserVisitor<'input> + 'x))
    }
}

impl<'input> PromQLParserContext<'input> for TerminalNode<'input,PromQLParserContextType> {}
impl<'input> PromQLParserContext<'input> for ErrorNode<'input,PromQLParserContextType> {}

antlr4rust::tid! { impl<'input> TidAble<'input> for dyn PromQLParserContext<'input> + 'input }

antlr4rust::tid! { impl<'input> TidAble<'input> for dyn PromQLParserListener<'input> + 'input }

pub struct PromQLParserContextType;
antlr4rust::tid!{PromQLParserContextType}

impl<'input> ParserNodeType<'input> for PromQLParserContextType{
	type TF = LocalTokenFactory<'input>;
	type Type = dyn PromQLParserContext<'input> + 'input;
}

impl<'input, I> Deref for PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    type Target = BaseParserType<'input,I>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'input, I> DerefMut for PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub struct PromQLParserExt<'input>{
	_pd: PhantomData<&'input str>,
}

impl<'input> PromQLParserExt<'input>{
}
antlr4rust::tid! { PromQLParserExt<'a> }

impl<'input> TokenAware<'input> for PromQLParserExt<'input>{
	type TF = LocalTokenFactory<'input>;
}

impl<'input,I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>> ParserRecog<'input, BaseParserType<'input,I>> for PromQLParserExt<'input>{}

impl<'input,I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>> Actions<'input, BaseParserType<'input,I>> for PromQLParserExt<'input>{
	fn get_grammar_file_name(&self) -> & str{ "PromQLParser.g4"}

   	fn get_rule_names(&self) -> &[& str] {&ruleNames}

   	fn get_vocabulary(&self) -> &dyn Vocabulary { &**VOCABULARY }
	fn sempred(_localctx: Option<&(dyn PromQLParserContext<'input> + 'input)>, rule_index: i32, pred_index: i32,
			   recog:&mut BaseParserType<'input,I>
	)->bool{
		match rule_index {
					1 => PromQLParser::<'input,I>::expr_sempred(_localctx.and_then(|x|x.downcast_ref()), pred_index, recog),
			_ => true
		}
	}
}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	fn expr_sempred(_localctx: Option<&ExprContext<'input>>, pred_index:i32,
						recog:&mut <Self as Deref>::Target
		) -> bool {
		match pred_index {
				0=>{
					recog.precpred(None, 9)
				}
				1=>{
					recog.precpred(None, 8)
				}
				2=>{
					recog.precpred(None, 7)
				}
				3=>{
					recog.precpred(None, 6)
				}
				4=>{
					recog.precpred(None, 5)
				}
				5=>{
					recog.precpred(None, 4)
				}
				6=>{
					recog.precpred(None, 3)
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

impl<'input> PromQLParserContext<'input> for RootContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for RootContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_root(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_root(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for RootContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_root(self);
	}
}

impl<'input> CustomRuleContext<'input> for RootContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_root }
	//fn type_rule_index() -> usize where Self: Sized { RULE_root }
}
antlr4rust::tid!{RootContextExt<'a>}

impl<'input> RootContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RootContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RootContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RootContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<RootContextExt<'input>>{

fn expr(&self) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token EOF
/// Returns `None` if there is no child corresponding to token EOF
fn EOF(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_EOF, 0)
}

}

impl<'input> RootContextAttrs<'input> for RootContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
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

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule expr*/
			recog.base.set_state(76);
			recog.expr_rec(0)?;

			recog.base.set_state(77);
			recog.base.match_token(PromQLParser_EOF,&mut recog.err_handler)?;

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
//------------------- expr ----------------
#[derive(Debug)]
pub enum ExprContextAll<'input>{
	ExprMulDivModContext(ExprMulDivModContext<'input>),
	ExprOrContext(ExprOrContext<'input>),
	ExprAndUnlessContext(ExprAndUnlessContext<'input>),
	ExprSubqueryContext(ExprSubqueryContext<'input>),
	ExprAddSubContext(ExprAddSubContext<'input>),
	ExprPowContext(ExprPowContext<'input>),
	ExprUnaryContext(ExprUnaryContext<'input>),
	ExprCompareContext(ExprCompareContext<'input>),
	ExprVectorContext(ExprVectorContext<'input>),
Error(ExprContext<'input>)
}
antlr4rust::tid!{ExprContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for ExprContextAll<'input>{}

impl<'input> PromQLParserContext<'input> for ExprContextAll<'input>{}

impl<'input> Deref for ExprContextAll<'input>{
	type Target = dyn ExprContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use ExprContextAll::*;
		match self{
			ExprMulDivModContext(inner) => inner,
			ExprOrContext(inner) => inner,
			ExprAndUnlessContext(inner) => inner,
			ExprSubqueryContext(inner) => inner,
			ExprAddSubContext(inner) => inner,
			ExprPowContext(inner) => inner,
			ExprUnaryContext(inner) => inner,
			ExprCompareContext(inner) => inner,
			ExprVectorContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprContextAll<'input>{
    fn enter(&self, listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type ExprContext<'input> = BaseParserRuleContext<'input,ExprContextExt<'input>>;

#[derive(Clone)]
pub struct ExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for ExprContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprContext<'input>{
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprContext<'input>{
}

impl<'input> CustomRuleContext<'input> for ExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}
antlr4rust::tid!{ExprContextExt<'a>}

impl<'input> ExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ExprContextAll<'input>> {
		Rc::new(
		ExprContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ExprContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait ExprContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<ExprContextExt<'input>>{


}

impl<'input> ExprContextAttrs<'input> for ExprContext<'input>{}

pub type ExprMulDivModContext<'input> = BaseParserRuleContext<'input,ExprMulDivModContextExt<'input>>;

pub trait ExprMulDivModContextAttrs<'input>: PromQLParserContext<'input>{
	fn expr_all(&self) ->  Vec<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn expr(&self, i: usize) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	fn mulDivModOp(&self) -> Option<Rc<MulDivModOpContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> ExprMulDivModContextAttrs<'input> for ExprMulDivModContext<'input>{}

pub struct ExprMulDivModContextExt<'input>{
	base:ExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{ExprMulDivModContextExt<'a>}

impl<'input> PromQLParserContext<'input> for ExprMulDivModContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprMulDivModContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_exprMulDivMod(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_exprMulDivMod(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprMulDivModContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_exprMulDivMod(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprMulDivModContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}

impl<'input> Borrow<ExprContextExt<'input>> for ExprMulDivModContext<'input>{
	fn borrow(&self) -> &ExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<ExprContextExt<'input>> for ExprMulDivModContext<'input>{
	fn borrow_mut(&mut self) -> &mut ExprContextExt<'input> { &mut self.base }
}

impl<'input> ExprContextAttrs<'input> for ExprMulDivModContext<'input> {}

impl<'input> ExprMulDivModContextExt<'input>{
	fn new(ctx: &dyn ExprContextAttrs<'input>) -> Rc<ExprContextAll<'input>>  {
		Rc::new(
			ExprContextAll::ExprMulDivModContext(
				BaseParserRuleContext::copy_from(ctx,ExprMulDivModContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type ExprOrContext<'input> = BaseParserRuleContext<'input,ExprOrContextExt<'input>>;

pub trait ExprOrContextAttrs<'input>: PromQLParserContext<'input>{
	fn expr_all(&self) ->  Vec<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn expr(&self, i: usize) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token OR
	/// Returns `None` if there is no child corresponding to token OR
	fn OR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
		self.get_token(PromQLParser_OR, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> ExprOrContextAttrs<'input> for ExprOrContext<'input>{}

pub struct ExprOrContextExt<'input>{
	base:ExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{ExprOrContextExt<'a>}

impl<'input> PromQLParserContext<'input> for ExprOrContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprOrContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_exprOr(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_exprOr(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprOrContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_exprOr(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprOrContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}

impl<'input> Borrow<ExprContextExt<'input>> for ExprOrContext<'input>{
	fn borrow(&self) -> &ExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<ExprContextExt<'input>> for ExprOrContext<'input>{
	fn borrow_mut(&mut self) -> &mut ExprContextExt<'input> { &mut self.base }
}

impl<'input> ExprContextAttrs<'input> for ExprOrContext<'input> {}

impl<'input> ExprOrContextExt<'input>{
	fn new(ctx: &dyn ExprContextAttrs<'input>) -> Rc<ExprContextAll<'input>>  {
		Rc::new(
			ExprContextAll::ExprOrContext(
				BaseParserRuleContext::copy_from(ctx,ExprOrContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type ExprAndUnlessContext<'input> = BaseParserRuleContext<'input,ExprAndUnlessContextExt<'input>>;

pub trait ExprAndUnlessContextAttrs<'input>: PromQLParserContext<'input>{
	fn expr_all(&self) ->  Vec<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn expr(&self, i: usize) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	fn andUnlessOp(&self) -> Option<Rc<AndUnlessOpContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> ExprAndUnlessContextAttrs<'input> for ExprAndUnlessContext<'input>{}

pub struct ExprAndUnlessContextExt<'input>{
	base:ExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{ExprAndUnlessContextExt<'a>}

impl<'input> PromQLParserContext<'input> for ExprAndUnlessContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprAndUnlessContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_exprAndUnless(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_exprAndUnless(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprAndUnlessContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_exprAndUnless(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprAndUnlessContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}

impl<'input> Borrow<ExprContextExt<'input>> for ExprAndUnlessContext<'input>{
	fn borrow(&self) -> &ExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<ExprContextExt<'input>> for ExprAndUnlessContext<'input>{
	fn borrow_mut(&mut self) -> &mut ExprContextExt<'input> { &mut self.base }
}

impl<'input> ExprContextAttrs<'input> for ExprAndUnlessContext<'input> {}

impl<'input> ExprAndUnlessContextExt<'input>{
	fn new(ctx: &dyn ExprContextAttrs<'input>) -> Rc<ExprContextAll<'input>>  {
		Rc::new(
			ExprContextAll::ExprAndUnlessContext(
				BaseParserRuleContext::copy_from(ctx,ExprAndUnlessContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type ExprSubqueryContext<'input> = BaseParserRuleContext<'input,ExprSubqueryContextExt<'input>>;

pub trait ExprSubqueryContextAttrs<'input>: PromQLParserContext<'input>{
	fn expr(&self) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	/// Retrieves first TerminalNode corresponding to token LBRACK
	/// Returns `None` if there is no child corresponding to token LBRACK
	fn LBRACK(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
		self.get_token(PromQLParser_LBRACK, 0)
	}
	fn duration_all(&self) ->  Vec<Rc<DurationContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn duration(&self, i: usize) -> Option<Rc<DurationContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token COLON
	/// Returns `None` if there is no child corresponding to token COLON
	fn COLON(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
		self.get_token(PromQLParser_COLON, 0)
	}
	/// Retrieves first TerminalNode corresponding to token RBRACK
	/// Returns `None` if there is no child corresponding to token RBRACK
	fn RBRACK(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
		self.get_token(PromQLParser_RBRACK, 0)
	}
	fn offsetModifier(&self) -> Option<Rc<OffsetModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	fn atModifier(&self) -> Option<Rc<AtModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> ExprSubqueryContextAttrs<'input> for ExprSubqueryContext<'input>{}

pub struct ExprSubqueryContextExt<'input>{
	base:ExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{ExprSubqueryContextExt<'a>}

impl<'input> PromQLParserContext<'input> for ExprSubqueryContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprSubqueryContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_exprSubquery(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_exprSubquery(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprSubqueryContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_exprSubquery(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprSubqueryContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}

impl<'input> Borrow<ExprContextExt<'input>> for ExprSubqueryContext<'input>{
	fn borrow(&self) -> &ExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<ExprContextExt<'input>> for ExprSubqueryContext<'input>{
	fn borrow_mut(&mut self) -> &mut ExprContextExt<'input> { &mut self.base }
}

impl<'input> ExprContextAttrs<'input> for ExprSubqueryContext<'input> {}

impl<'input> ExprSubqueryContextExt<'input>{
	fn new(ctx: &dyn ExprContextAttrs<'input>) -> Rc<ExprContextAll<'input>>  {
		Rc::new(
			ExprContextAll::ExprSubqueryContext(
				BaseParserRuleContext::copy_from(ctx,ExprSubqueryContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type ExprAddSubContext<'input> = BaseParserRuleContext<'input,ExprAddSubContextExt<'input>>;

pub trait ExprAddSubContextAttrs<'input>: PromQLParserContext<'input>{
	fn expr_all(&self) ->  Vec<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn expr(&self, i: usize) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	fn addSubOp(&self) -> Option<Rc<AddSubOpContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> ExprAddSubContextAttrs<'input> for ExprAddSubContext<'input>{}

pub struct ExprAddSubContextExt<'input>{
	base:ExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{ExprAddSubContextExt<'a>}

impl<'input> PromQLParserContext<'input> for ExprAddSubContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprAddSubContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_exprAddSub(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_exprAddSub(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprAddSubContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_exprAddSub(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprAddSubContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}

impl<'input> Borrow<ExprContextExt<'input>> for ExprAddSubContext<'input>{
	fn borrow(&self) -> &ExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<ExprContextExt<'input>> for ExprAddSubContext<'input>{
	fn borrow_mut(&mut self) -> &mut ExprContextExt<'input> { &mut self.base }
}

impl<'input> ExprContextAttrs<'input> for ExprAddSubContext<'input> {}

impl<'input> ExprAddSubContextExt<'input>{
	fn new(ctx: &dyn ExprContextAttrs<'input>) -> Rc<ExprContextAll<'input>>  {
		Rc::new(
			ExprContextAll::ExprAddSubContext(
				BaseParserRuleContext::copy_from(ctx,ExprAddSubContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type ExprPowContext<'input> = BaseParserRuleContext<'input,ExprPowContextExt<'input>>;

pub trait ExprPowContextAttrs<'input>: PromQLParserContext<'input>{
	fn expr_all(&self) ->  Vec<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn expr(&self, i: usize) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token POW
	/// Returns `None` if there is no child corresponding to token POW
	fn POW(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
		self.get_token(PromQLParser_POW, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> ExprPowContextAttrs<'input> for ExprPowContext<'input>{}

pub struct ExprPowContextExt<'input>{
	base:ExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{ExprPowContextExt<'a>}

impl<'input> PromQLParserContext<'input> for ExprPowContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprPowContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_exprPow(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_exprPow(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprPowContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_exprPow(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprPowContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}

impl<'input> Borrow<ExprContextExt<'input>> for ExprPowContext<'input>{
	fn borrow(&self) -> &ExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<ExprContextExt<'input>> for ExprPowContext<'input>{
	fn borrow_mut(&mut self) -> &mut ExprContextExt<'input> { &mut self.base }
}

impl<'input> ExprContextAttrs<'input> for ExprPowContext<'input> {}

impl<'input> ExprPowContextExt<'input>{
	fn new(ctx: &dyn ExprContextAttrs<'input>) -> Rc<ExprContextAll<'input>>  {
		Rc::new(
			ExprContextAll::ExprPowContext(
				BaseParserRuleContext::copy_from(ctx,ExprPowContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type ExprUnaryContext<'input> = BaseParserRuleContext<'input,ExprUnaryContextExt<'input>>;

pub trait ExprUnaryContextAttrs<'input>: PromQLParserContext<'input>{
	fn unaryExpr(&self) -> Option<Rc<UnaryExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> ExprUnaryContextAttrs<'input> for ExprUnaryContext<'input>{}

pub struct ExprUnaryContextExt<'input>{
	base:ExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{ExprUnaryContextExt<'a>}

impl<'input> PromQLParserContext<'input> for ExprUnaryContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprUnaryContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_exprUnary(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_exprUnary(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprUnaryContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_exprUnary(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprUnaryContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}

impl<'input> Borrow<ExprContextExt<'input>> for ExprUnaryContext<'input>{
	fn borrow(&self) -> &ExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<ExprContextExt<'input>> for ExprUnaryContext<'input>{
	fn borrow_mut(&mut self) -> &mut ExprContextExt<'input> { &mut self.base }
}

impl<'input> ExprContextAttrs<'input> for ExprUnaryContext<'input> {}

impl<'input> ExprUnaryContextExt<'input>{
	fn new(ctx: &dyn ExprContextAttrs<'input>) -> Rc<ExprContextAll<'input>>  {
		Rc::new(
			ExprContextAll::ExprUnaryContext(
				BaseParserRuleContext::copy_from(ctx,ExprUnaryContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type ExprCompareContext<'input> = BaseParserRuleContext<'input,ExprCompareContextExt<'input>>;

pub trait ExprCompareContextAttrs<'input>: PromQLParserContext<'input>{
	fn expr_all(&self) ->  Vec<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn expr(&self, i: usize) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	fn compareOp(&self) -> Option<Rc<CompareOpContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> ExprCompareContextAttrs<'input> for ExprCompareContext<'input>{}

pub struct ExprCompareContextExt<'input>{
	base:ExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{ExprCompareContextExt<'a>}

impl<'input> PromQLParserContext<'input> for ExprCompareContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprCompareContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_exprCompare(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_exprCompare(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprCompareContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_exprCompare(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprCompareContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}

impl<'input> Borrow<ExprContextExt<'input>> for ExprCompareContext<'input>{
	fn borrow(&self) -> &ExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<ExprContextExt<'input>> for ExprCompareContext<'input>{
	fn borrow_mut(&mut self) -> &mut ExprContextExt<'input> { &mut self.base }
}

impl<'input> ExprContextAttrs<'input> for ExprCompareContext<'input> {}

impl<'input> ExprCompareContextExt<'input>{
	fn new(ctx: &dyn ExprContextAttrs<'input>) -> Rc<ExprContextAll<'input>>  {
		Rc::new(
			ExprContextAll::ExprCompareContext(
				BaseParserRuleContext::copy_from(ctx,ExprCompareContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type ExprVectorContext<'input> = BaseParserRuleContext<'input,ExprVectorContextExt<'input>>;

pub trait ExprVectorContextAttrs<'input>: PromQLParserContext<'input>{
	fn vector(&self) -> Option<Rc<VectorContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> ExprVectorContextAttrs<'input> for ExprVectorContext<'input>{}

pub struct ExprVectorContextExt<'input>{
	base:ExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{ExprVectorContextExt<'a>}

impl<'input> PromQLParserContext<'input> for ExprVectorContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ExprVectorContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_exprVector(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_exprVector(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ExprVectorContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_exprVector(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprVectorContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}

impl<'input> Borrow<ExprContextExt<'input>> for ExprVectorContext<'input>{
	fn borrow(&self) -> &ExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<ExprContextExt<'input>> for ExprVectorContext<'input>{
	fn borrow_mut(&mut self) -> &mut ExprContextExt<'input> { &mut self.base }
}

impl<'input> ExprContextAttrs<'input> for ExprVectorContext<'input> {}

impl<'input> ExprVectorContextExt<'input>{
	fn new(ctx: &dyn ExprContextAttrs<'input>) -> Rc<ExprContextAll<'input>>  {
		Rc::new(
			ExprContextAll::ExprVectorContext(
				BaseParserRuleContext::copy_from(ctx,ExprVectorContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn  expr(&mut self,)
	-> Result<Rc<ExprContextAll<'input>>,ANTLRError> {
		self.expr_rec(0)
	}

	fn expr_rec(&mut self, _p: i32)
	-> Result<Rc<ExprContextAll<'input>>,ANTLRError> {
		let recog = self;
		let _parentctx = recog.ctx.take();
		let _parentState = recog.base.get_state();
		let mut _localctx = ExprContextExt::new(_parentctx.clone(), recog.base.get_state());
		recog.base.enter_recursion_rule(_localctx.clone(), 2, RULE_expr, _p);
	    let mut _localctx: Rc<ExprContextAll> = _localctx;
        let mut _prevctx = _localctx.clone();
		let _startState = 2;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {
			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(82);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(0,&mut recog.base)? {
				1 =>{
					{
					let mut tmp = ExprUnaryContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();

					/*InvokeRule unaryExpr*/
					recog.base.set_state(80);
					recog.unaryExpr()?;

					}
				}
			,
				2 =>{
					{
					let mut tmp = ExprVectorContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule vector*/
					recog.base.set_state(81);
					recog.vector()?;

					}
				}

				_ => {}
			}
			let tmp = recog.input.lt(-1).cloned();
			recog.ctx.as_ref().unwrap().set_stop(tmp);
			recog.base.set_state(130);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(5,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					recog.trigger_exit_rule_event()?;
					_prevctx = _localctx.clone();
					{
					recog.base.set_state(128);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(4,&mut recog.base)? {
						1 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = ExprPowContextExt::new(&**ExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_expr)?;
							_localctx = tmp;
							recog.base.set_state(84);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 9)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 9)".to_owned()), None))?;
							}
							recog.base.set_state(85);
							recog.base.match_token(PromQLParser_POW,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(86);
							recog.binOpModifier()?;

							/*InvokeRule expr*/
							recog.base.set_state(87);
							recog.expr_rec(9)?;

							}
						}
					,
						2 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = ExprMulDivModContextExt::new(&**ExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_expr)?;
							_localctx = tmp;
							recog.base.set_state(89);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 8)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 8)".to_owned()), None))?;
							}
							/*InvokeRule mulDivModOp*/
							recog.base.set_state(90);
							recog.mulDivModOp()?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(91);
							recog.binOpModifier()?;

							/*InvokeRule expr*/
							recog.base.set_state(92);
							recog.expr_rec(9)?;

							}
						}
					,
						3 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = ExprAddSubContextExt::new(&**ExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_expr)?;
							_localctx = tmp;
							recog.base.set_state(94);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 7)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 7)".to_owned()), None))?;
							}
							/*InvokeRule addSubOp*/
							recog.base.set_state(95);
							recog.addSubOp()?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(96);
							recog.binOpModifier()?;

							/*InvokeRule expr*/
							recog.base.set_state(97);
							recog.expr_rec(8)?;

							}
						}
					,
						4 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = ExprCompareContextExt::new(&**ExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_expr)?;
							_localctx = tmp;
							recog.base.set_state(99);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 6)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 6)".to_owned()), None))?;
							}
							/*InvokeRule compareOp*/
							recog.base.set_state(100);
							recog.compareOp()?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(101);
							recog.binOpModifier()?;

							/*InvokeRule expr*/
							recog.base.set_state(102);
							recog.expr_rec(7)?;

							}
						}
					,
						5 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = ExprAndUnlessContextExt::new(&**ExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_expr)?;
							_localctx = tmp;
							recog.base.set_state(104);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 5)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 5)".to_owned()), None))?;
							}
							/*InvokeRule andUnlessOp*/
							recog.base.set_state(105);
							recog.andUnlessOp()?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(106);
							recog.binOpModifier()?;

							/*InvokeRule expr*/
							recog.base.set_state(107);
							recog.expr_rec(6)?;

							}
						}
					,
						6 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = ExprOrContextExt::new(&**ExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_expr)?;
							_localctx = tmp;
							recog.base.set_state(109);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 4)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 4)".to_owned()), None))?;
							}
							recog.base.set_state(110);
							recog.base.match_token(PromQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(111);
							recog.binOpModifier()?;

							/*InvokeRule expr*/
							recog.base.set_state(112);
							recog.expr_rec(5)?;

							}
						}
					,
						7 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = ExprSubqueryContextExt::new(&**ExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_expr)?;
							_localctx = tmp;
							recog.base.set_state(114);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 3)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 3)".to_owned()), None))?;
							}
							recog.base.set_state(115);
							recog.base.match_token(PromQLParser_LBRACK,&mut recog.err_handler)?;

							/*InvokeRule duration*/
							recog.base.set_state(116);
							recog.duration()?;

							recog.base.set_state(117);
							recog.base.match_token(PromQLParser_COLON,&mut recog.err_handler)?;

							recog.base.set_state(119);
							recog.err_handler.sync(&mut recog.base)?;
							_la = recog.base.input.la(1);
							if _la==PromQLParser_SUB || _la==PromQLParser_DURATION {
								{
								/*InvokeRule duration*/
								recog.base.set_state(118);
								recog.duration()?;

								}
							}

							recog.base.set_state(121);
							recog.base.match_token(PromQLParser_RBRACK,&mut recog.err_handler)?;

							recog.base.set_state(123);
							recog.err_handler.sync(&mut recog.base)?;
							match  recog.interpreter.adaptive_predict(2,&mut recog.base)? {
								x if x == 1=>{
									{
									/*InvokeRule offsetModifier*/
									recog.base.set_state(122);
									recog.offsetModifier()?;

									}
								}

								_ => {}
							}
							recog.base.set_state(126);
							recog.err_handler.sync(&mut recog.base)?;
							match  recog.interpreter.adaptive_predict(3,&mut recog.base)? {
								x if x == 1=>{
									{
									/*InvokeRule atModifier*/
									recog.base.set_state(125);
									recog.atModifier()?;

									}
								}

								_ => {}
							}
							}
						}

						_ => {}
					}
					} 
				}
				recog.base.set_state(132);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(5,&mut recog.base)?;
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
//------------------- vector ----------------
#[derive(Debug)]
pub enum VectorContextAll<'input>{
	VectorFunctionContext(VectorFunctionContext<'input>),
	VectorNumberContext(VectorNumberContext<'input>),
	VectorInstantContext(VectorInstantContext<'input>),
	VectorParenContext(VectorParenContext<'input>),
	VectorMatrixContext(VectorMatrixContext<'input>),
	VectorStringContext(VectorStringContext<'input>),
	VectorAggregationContext(VectorAggregationContext<'input>),
Error(VectorContext<'input>)
}
antlr4rust::tid!{VectorContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for VectorContextAll<'input>{}

impl<'input> PromQLParserContext<'input> for VectorContextAll<'input>{}

impl<'input> Deref for VectorContextAll<'input>{
	type Target = dyn VectorContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use VectorContextAll::*;
		match self{
			VectorFunctionContext(inner) => inner,
			VectorNumberContext(inner) => inner,
			VectorInstantContext(inner) => inner,
			VectorParenContext(inner) => inner,
			VectorMatrixContext(inner) => inner,
			VectorStringContext(inner) => inner,
			VectorAggregationContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for VectorContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for VectorContextAll<'input>{
    fn enter(&self, listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type VectorContext<'input> = BaseParserRuleContext<'input,VectorContextExt<'input>>;

#[derive(Clone)]
pub struct VectorContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for VectorContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for VectorContext<'input>{
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for VectorContext<'input>{
}

impl<'input> CustomRuleContext<'input> for VectorContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vector }
}
antlr4rust::tid!{VectorContextExt<'a>}

impl<'input> VectorContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<VectorContextAll<'input>> {
		Rc::new(
		VectorContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,VectorContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait VectorContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<VectorContextExt<'input>>{


}

impl<'input> VectorContextAttrs<'input> for VectorContext<'input>{}

pub type VectorFunctionContext<'input> = BaseParserRuleContext<'input,VectorFunctionContextExt<'input>>;

pub trait VectorFunctionContextAttrs<'input>: PromQLParserContext<'input>{
	fn functionCall(&self) -> Option<Rc<FunctionCallContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> VectorFunctionContextAttrs<'input> for VectorFunctionContext<'input>{}

pub struct VectorFunctionContextExt<'input>{
	base:VectorContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{VectorFunctionContextExt<'a>}

impl<'input> PromQLParserContext<'input> for VectorFunctionContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for VectorFunctionContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_vectorFunction(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_vectorFunction(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for VectorFunctionContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorFunction(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorFunctionContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vector }
}

impl<'input> Borrow<VectorContextExt<'input>> for VectorFunctionContext<'input>{
	fn borrow(&self) -> &VectorContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<VectorContextExt<'input>> for VectorFunctionContext<'input>{
	fn borrow_mut(&mut self) -> &mut VectorContextExt<'input> { &mut self.base }
}

impl<'input> VectorContextAttrs<'input> for VectorFunctionContext<'input> {}

impl<'input> VectorFunctionContextExt<'input>{
	fn new(ctx: &dyn VectorContextAttrs<'input>) -> Rc<VectorContextAll<'input>>  {
		Rc::new(
			VectorContextAll::VectorFunctionContext(
				BaseParserRuleContext::copy_from(ctx,VectorFunctionContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type VectorNumberContext<'input> = BaseParserRuleContext<'input,VectorNumberContextExt<'input>>;

pub trait VectorNumberContextAttrs<'input>: PromQLParserContext<'input>{
	fn numberLiteral(&self) -> Option<Rc<NumberLiteralContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> VectorNumberContextAttrs<'input> for VectorNumberContext<'input>{}

pub struct VectorNumberContextExt<'input>{
	base:VectorContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{VectorNumberContextExt<'a>}

impl<'input> PromQLParserContext<'input> for VectorNumberContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for VectorNumberContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_vectorNumber(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_vectorNumber(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for VectorNumberContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorNumber(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorNumberContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vector }
}

impl<'input> Borrow<VectorContextExt<'input>> for VectorNumberContext<'input>{
	fn borrow(&self) -> &VectorContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<VectorContextExt<'input>> for VectorNumberContext<'input>{
	fn borrow_mut(&mut self) -> &mut VectorContextExt<'input> { &mut self.base }
}

impl<'input> VectorContextAttrs<'input> for VectorNumberContext<'input> {}

impl<'input> VectorNumberContextExt<'input>{
	fn new(ctx: &dyn VectorContextAttrs<'input>) -> Rc<VectorContextAll<'input>>  {
		Rc::new(
			VectorContextAll::VectorNumberContext(
				BaseParserRuleContext::copy_from(ctx,VectorNumberContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type VectorInstantContext<'input> = BaseParserRuleContext<'input,VectorInstantContextExt<'input>>;

pub trait VectorInstantContextAttrs<'input>: PromQLParserContext<'input>{
	fn instantSelector(&self) -> Option<Rc<InstantSelectorContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> VectorInstantContextAttrs<'input> for VectorInstantContext<'input>{}

pub struct VectorInstantContextExt<'input>{
	base:VectorContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{VectorInstantContextExt<'a>}

impl<'input> PromQLParserContext<'input> for VectorInstantContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for VectorInstantContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_vectorInstant(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_vectorInstant(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for VectorInstantContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorInstant(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorInstantContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vector }
}

impl<'input> Borrow<VectorContextExt<'input>> for VectorInstantContext<'input>{
	fn borrow(&self) -> &VectorContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<VectorContextExt<'input>> for VectorInstantContext<'input>{
	fn borrow_mut(&mut self) -> &mut VectorContextExt<'input> { &mut self.base }
}

impl<'input> VectorContextAttrs<'input> for VectorInstantContext<'input> {}

impl<'input> VectorInstantContextExt<'input>{
	fn new(ctx: &dyn VectorContextAttrs<'input>) -> Rc<VectorContextAll<'input>>  {
		Rc::new(
			VectorContextAll::VectorInstantContext(
				BaseParserRuleContext::copy_from(ctx,VectorInstantContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type VectorParenContext<'input> = BaseParserRuleContext<'input,VectorParenContextExt<'input>>;

pub trait VectorParenContextAttrs<'input>: PromQLParserContext<'input>{
	fn parenExpr(&self) -> Option<Rc<ParenExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> VectorParenContextAttrs<'input> for VectorParenContext<'input>{}

pub struct VectorParenContextExt<'input>{
	base:VectorContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{VectorParenContextExt<'a>}

impl<'input> PromQLParserContext<'input> for VectorParenContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for VectorParenContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_vectorParen(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_vectorParen(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for VectorParenContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorParen(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorParenContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vector }
}

impl<'input> Borrow<VectorContextExt<'input>> for VectorParenContext<'input>{
	fn borrow(&self) -> &VectorContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<VectorContextExt<'input>> for VectorParenContext<'input>{
	fn borrow_mut(&mut self) -> &mut VectorContextExt<'input> { &mut self.base }
}

impl<'input> VectorContextAttrs<'input> for VectorParenContext<'input> {}

impl<'input> VectorParenContextExt<'input>{
	fn new(ctx: &dyn VectorContextAttrs<'input>) -> Rc<VectorContextAll<'input>>  {
		Rc::new(
			VectorContextAll::VectorParenContext(
				BaseParserRuleContext::copy_from(ctx,VectorParenContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type VectorMatrixContext<'input> = BaseParserRuleContext<'input,VectorMatrixContextExt<'input>>;

pub trait VectorMatrixContextAttrs<'input>: PromQLParserContext<'input>{
	fn matrixSelector(&self) -> Option<Rc<MatrixSelectorContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> VectorMatrixContextAttrs<'input> for VectorMatrixContext<'input>{}

pub struct VectorMatrixContextExt<'input>{
	base:VectorContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{VectorMatrixContextExt<'a>}

impl<'input> PromQLParserContext<'input> for VectorMatrixContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for VectorMatrixContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_vectorMatrix(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_vectorMatrix(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for VectorMatrixContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorMatrix(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorMatrixContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vector }
}

impl<'input> Borrow<VectorContextExt<'input>> for VectorMatrixContext<'input>{
	fn borrow(&self) -> &VectorContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<VectorContextExt<'input>> for VectorMatrixContext<'input>{
	fn borrow_mut(&mut self) -> &mut VectorContextExt<'input> { &mut self.base }
}

impl<'input> VectorContextAttrs<'input> for VectorMatrixContext<'input> {}

impl<'input> VectorMatrixContextExt<'input>{
	fn new(ctx: &dyn VectorContextAttrs<'input>) -> Rc<VectorContextAll<'input>>  {
		Rc::new(
			VectorContextAll::VectorMatrixContext(
				BaseParserRuleContext::copy_from(ctx,VectorMatrixContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type VectorStringContext<'input> = BaseParserRuleContext<'input,VectorStringContextExt<'input>>;

pub trait VectorStringContextAttrs<'input>: PromQLParserContext<'input>{
	fn stringLiteral(&self) -> Option<Rc<StringLiteralContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> VectorStringContextAttrs<'input> for VectorStringContext<'input>{}

pub struct VectorStringContextExt<'input>{
	base:VectorContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{VectorStringContextExt<'a>}

impl<'input> PromQLParserContext<'input> for VectorStringContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for VectorStringContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_vectorString(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_vectorString(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for VectorStringContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorString(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorStringContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vector }
}

impl<'input> Borrow<VectorContextExt<'input>> for VectorStringContext<'input>{
	fn borrow(&self) -> &VectorContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<VectorContextExt<'input>> for VectorStringContext<'input>{
	fn borrow_mut(&mut self) -> &mut VectorContextExt<'input> { &mut self.base }
}

impl<'input> VectorContextAttrs<'input> for VectorStringContext<'input> {}

impl<'input> VectorStringContextExt<'input>{
	fn new(ctx: &dyn VectorContextAttrs<'input>) -> Rc<VectorContextAll<'input>>  {
		Rc::new(
			VectorContextAll::VectorStringContext(
				BaseParserRuleContext::copy_from(ctx,VectorStringContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type VectorAggregationContext<'input> = BaseParserRuleContext<'input,VectorAggregationContextExt<'input>>;

pub trait VectorAggregationContextAttrs<'input>: PromQLParserContext<'input>{
	fn aggregation(&self) -> Option<Rc<AggregationContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> VectorAggregationContextAttrs<'input> for VectorAggregationContext<'input>{}

pub struct VectorAggregationContextExt<'input>{
	base:VectorContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{VectorAggregationContextExt<'a>}

impl<'input> PromQLParserContext<'input> for VectorAggregationContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for VectorAggregationContext<'input>{
	fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_vectorAggregation(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_vectorAggregation(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for VectorAggregationContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorAggregation(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorAggregationContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vector }
}

impl<'input> Borrow<VectorContextExt<'input>> for VectorAggregationContext<'input>{
	fn borrow(&self) -> &VectorContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<VectorContextExt<'input>> for VectorAggregationContext<'input>{
	fn borrow_mut(&mut self) -> &mut VectorContextExt<'input> { &mut self.base }
}

impl<'input> VectorContextAttrs<'input> for VectorAggregationContext<'input> {}

impl<'input> VectorAggregationContextExt<'input>{
	fn new(ctx: &dyn VectorContextAttrs<'input>) -> Rc<VectorContextAll<'input>>  {
		Rc::new(
			VectorContextAll::VectorAggregationContext(
				BaseParserRuleContext::copy_from(ctx,VectorAggregationContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn vector(&mut self,)
	-> Result<Rc<VectorContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = VectorContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 4, RULE_vector);
        let mut _localctx: Rc<VectorContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(140);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(6,&mut recog.base)? {
				1 =>{
					let tmp = VectorFunctionContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					/*InvokeRule functionCall*/
					recog.base.set_state(133);
					recog.functionCall()?;

					}
				}
			,
				2 =>{
					let tmp = VectorAggregationContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					/*InvokeRule aggregation*/
					recog.base.set_state(134);
					recog.aggregation()?;

					}
				}
			,
				3 =>{
					let tmp = VectorMatrixContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					/*InvokeRule matrixSelector*/
					recog.base.set_state(135);
					recog.matrixSelector()?;

					}
				}
			,
				4 =>{
					let tmp = VectorInstantContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 4)?;
					_localctx = tmp;
					{
					/*InvokeRule instantSelector*/
					recog.base.set_state(136);
					recog.instantSelector()?;

					}
				}
			,
				5 =>{
					let tmp = VectorNumberContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 5)?;
					_localctx = tmp;
					{
					/*InvokeRule numberLiteral*/
					recog.base.set_state(137);
					recog.numberLiteral()?;

					}
				}
			,
				6 =>{
					let tmp = VectorStringContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 6)?;
					_localctx = tmp;
					{
					/*InvokeRule stringLiteral*/
					recog.base.set_state(138);
					recog.stringLiteral()?;

					}
				}
			,
				7 =>{
					let tmp = VectorParenContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 7)?;
					_localctx = tmp;
					{
					/*InvokeRule parenExpr*/
					recog.base.set_state(139);
					recog.parenExpr()?;

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
//------------------- parenExpr ----------------
pub type ParenExprContextAll<'input> = ParenExprContext<'input>;


pub type ParenExprContext<'input> = BaseParserRuleContext<'input,ParenExprContextExt<'input>>;

#[derive(Clone)]
pub struct ParenExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for ParenExprContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for ParenExprContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_parenExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_parenExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for ParenExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_parenExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for ParenExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_parenExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_parenExpr }
}
antlr4rust::tid!{ParenExprContextExt<'a>}

impl<'input> ParenExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ParenExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ParenExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ParenExprContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<ParenExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LPAREN, 0)
}
fn expr(&self) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RPAREN, 0)
}

}

impl<'input> ParenExprContextAttrs<'input> for ParenExprContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn parenExpr(&mut self,)
	-> Result<Rc<ParenExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ParenExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 6, RULE_parenExpr);
        let mut _localctx: Rc<ParenExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(142);
			recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

			/*InvokeRule expr*/
			recog.base.set_state(143);
			recog.expr_rec(0)?;

			recog.base.set_state(144);
			recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- unaryExpr ----------------
pub type UnaryExprContextAll<'input> = UnaryExprContext<'input>;


pub type UnaryExprContext<'input> = BaseParserRuleContext<'input,UnaryExprContextExt<'input>>;

#[derive(Clone)]
pub struct UnaryExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for UnaryExprContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for UnaryExprContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_unaryExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_unaryExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for UnaryExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_unaryExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for UnaryExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_unaryExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_unaryExpr }
}
antlr4rust::tid!{UnaryExprContextExt<'a>}

impl<'input> UnaryExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<UnaryExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,UnaryExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait UnaryExprContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<UnaryExprContextExt<'input>>{

fn unaryOp(&self) -> Option<Rc<UnaryOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn expr(&self) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> UnaryExprContextAttrs<'input> for UnaryExprContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn unaryExpr(&mut self,)
	-> Result<Rc<UnaryExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = UnaryExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 8, RULE_unaryExpr);
        let mut _localctx: Rc<UnaryExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule unaryOp*/
			recog.base.set_state(146);
			recog.unaryOp()?;

			/*InvokeRule expr*/
			recog.base.set_state(147);
			recog.expr_rec(0)?;

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
//------------------- unaryOp ----------------
pub type UnaryOpContextAll<'input> = UnaryOpContext<'input>;


pub type UnaryOpContext<'input> = BaseParserRuleContext<'input,UnaryOpContextExt<'input>>;

#[derive(Clone)]
pub struct UnaryOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for UnaryOpContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for UnaryOpContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_unaryOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_unaryOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for UnaryOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_unaryOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for UnaryOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_unaryOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_unaryOp }
}
antlr4rust::tid!{UnaryOpContextExt<'a>}

impl<'input> UnaryOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<UnaryOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,UnaryOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait UnaryOpContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<UnaryOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ADD
/// Returns `None` if there is no child corresponding to token ADD
fn ADD(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ADD, 0)
}
/// Retrieves first TerminalNode corresponding to token SUB
/// Returns `None` if there is no child corresponding to token SUB
fn SUB(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SUB, 0)
}

}

impl<'input> UnaryOpContextAttrs<'input> for UnaryOpContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn unaryOp(&mut self,)
	-> Result<Rc<UnaryOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = UnaryOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 10, RULE_unaryOp);
        let mut _localctx: Rc<UnaryOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(149);
			_la = recog.base.input.la(1);
			if { !(_la==PromQLParser_ADD || _la==PromQLParser_SUB) } {
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
//------------------- mulDivModOp ----------------
pub type MulDivModOpContextAll<'input> = MulDivModOpContext<'input>;


pub type MulDivModOpContext<'input> = BaseParserRuleContext<'input,MulDivModOpContextExt<'input>>;

#[derive(Clone)]
pub struct MulDivModOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for MulDivModOpContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for MulDivModOpContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_mulDivModOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_mulDivModOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for MulDivModOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_mulDivModOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for MulDivModOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_mulDivModOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_mulDivModOp }
}
antlr4rust::tid!{MulDivModOpContextExt<'a>}

impl<'input> MulDivModOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<MulDivModOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,MulDivModOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait MulDivModOpContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<MulDivModOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token MUL
/// Returns `None` if there is no child corresponding to token MUL
fn MUL(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_MUL, 0)
}
/// Retrieves first TerminalNode corresponding to token DIV
/// Returns `None` if there is no child corresponding to token DIV
fn DIV(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_DIV, 0)
}
/// Retrieves first TerminalNode corresponding to token MOD
/// Returns `None` if there is no child corresponding to token MOD
fn MOD(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_MOD, 0)
}

}

impl<'input> MulDivModOpContextAttrs<'input> for MulDivModOpContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn mulDivModOp(&mut self,)
	-> Result<Rc<MulDivModOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = MulDivModOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 12, RULE_mulDivModOp);
        let mut _localctx: Rc<MulDivModOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(151);
			_la = recog.base.input.la(1);
			if { !(((((_la - 11)) & !0x3f) == 0 && ((1usize << (_la - 11)) & 67108867) != 0)) } {
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
//------------------- addSubOp ----------------
pub type AddSubOpContextAll<'input> = AddSubOpContext<'input>;


pub type AddSubOpContext<'input> = BaseParserRuleContext<'input,AddSubOpContextExt<'input>>;

#[derive(Clone)]
pub struct AddSubOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for AddSubOpContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for AddSubOpContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_addSubOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_addSubOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for AddSubOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_addSubOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for AddSubOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_addSubOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_addSubOp }
}
antlr4rust::tid!{AddSubOpContextExt<'a>}

impl<'input> AddSubOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<AddSubOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,AddSubOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait AddSubOpContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<AddSubOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ADD
/// Returns `None` if there is no child corresponding to token ADD
fn ADD(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ADD, 0)
}
/// Retrieves first TerminalNode corresponding to token SUB
/// Returns `None` if there is no child corresponding to token SUB
fn SUB(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SUB, 0)
}

}

impl<'input> AddSubOpContextAttrs<'input> for AddSubOpContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn addSubOp(&mut self,)
	-> Result<Rc<AddSubOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = AddSubOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 14, RULE_addSubOp);
        let mut _localctx: Rc<AddSubOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(153);
			_la = recog.base.input.la(1);
			if { !(_la==PromQLParser_ADD || _la==PromQLParser_SUB) } {
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
//------------------- compareOp ----------------
pub type CompareOpContextAll<'input> = CompareOpContext<'input>;


pub type CompareOpContext<'input> = BaseParserRuleContext<'input,CompareOpContextExt<'input>>;

#[derive(Clone)]
pub struct CompareOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for CompareOpContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for CompareOpContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_compareOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_compareOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for CompareOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_compareOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for CompareOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_compareOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_compareOp }
}
antlr4rust::tid!{CompareOpContextExt<'a>}

impl<'input> CompareOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<CompareOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,CompareOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait CompareOpContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<CompareOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token EQL
/// Returns `None` if there is no child corresponding to token EQL
fn EQL(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_EQL, 0)
}
/// Retrieves first TerminalNode corresponding to token NE
/// Returns `None` if there is no child corresponding to token NE
fn NE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_NE, 0)
}
/// Retrieves first TerminalNode corresponding to token GT
/// Returns `None` if there is no child corresponding to token GT
fn GT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_GT, 0)
}
/// Retrieves first TerminalNode corresponding to token LT
/// Returns `None` if there is no child corresponding to token LT
fn LT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LT, 0)
}
/// Retrieves first TerminalNode corresponding to token GE
/// Returns `None` if there is no child corresponding to token GE
fn GE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_GE, 0)
}
/// Retrieves first TerminalNode corresponding to token LE
/// Returns `None` if there is no child corresponding to token LE
fn LE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LE, 0)
}

}

impl<'input> CompareOpContextAttrs<'input> for CompareOpContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn compareOp(&mut self,)
	-> Result<Rc<CompareOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = CompareOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 16, RULE_compareOp);
        let mut _localctx: Rc<CompareOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(155);
			_la = recog.base.input.la(1);
			if { !((((_la) & !0x3f) == 0 && ((1usize << _la) & 8159232) != 0)) } {
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
//------------------- andUnlessOp ----------------
pub type AndUnlessOpContextAll<'input> = AndUnlessOpContext<'input>;


pub type AndUnlessOpContext<'input> = BaseParserRuleContext<'input,AndUnlessOpContextExt<'input>>;

#[derive(Clone)]
pub struct AndUnlessOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for AndUnlessOpContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for AndUnlessOpContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_andUnlessOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_andUnlessOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for AndUnlessOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_andUnlessOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for AndUnlessOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_andUnlessOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_andUnlessOp }
}
antlr4rust::tid!{AndUnlessOpContextExt<'a>}

impl<'input> AndUnlessOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<AndUnlessOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,AndUnlessOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait AndUnlessOpContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<AndUnlessOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token AND
/// Returns `None` if there is no child corresponding to token AND
fn AND(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AND, 0)
}
/// Retrieves first TerminalNode corresponding to token UNLESS
/// Returns `None` if there is no child corresponding to token UNLESS
fn UNLESS(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_UNLESS, 0)
}

}

impl<'input> AndUnlessOpContextAttrs<'input> for AndUnlessOpContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn andUnlessOp(&mut self,)
	-> Result<Rc<AndUnlessOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = AndUnlessOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 18, RULE_andUnlessOp);
        let mut _localctx: Rc<AndUnlessOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(157);
			_la = recog.base.input.la(1);
			if { !(_la==PromQLParser_AND || _la==PromQLParser_UNLESS) } {
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
//------------------- binOpModifier ----------------
pub type BinOpModifierContextAll<'input> = BinOpModifierContext<'input>;


pub type BinOpModifierContext<'input> = BaseParserRuleContext<'input,BinOpModifierContextExt<'input>>;

#[derive(Clone)]
pub struct BinOpModifierContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for BinOpModifierContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for BinOpModifierContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_binOpModifier(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_binOpModifier(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for BinOpModifierContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_binOpModifier(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinOpModifierContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_binOpModifier }
	//fn type_rule_index() -> usize where Self: Sized { RULE_binOpModifier }
}
antlr4rust::tid!{BinOpModifierContextExt<'a>}

impl<'input> BinOpModifierContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<BinOpModifierContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,BinOpModifierContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait BinOpModifierContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<BinOpModifierContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token BOOL
/// Returns `None` if there is no child corresponding to token BOOL
fn BOOL(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_BOOL, 0)
}
fn onOrIgnoring(&self) -> Option<Rc<OnOrIgnoringContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn groupModifier(&self) -> Option<Rc<GroupModifierContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> BinOpModifierContextAttrs<'input> for BinOpModifierContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn binOpModifier(&mut self,)
	-> Result<Rc<BinOpModifierContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = BinOpModifierContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 20, RULE_binOpModifier);
        let mut _localctx: Rc<BinOpModifierContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(160);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==PromQLParser_BOOL {
				{
				recog.base.set_state(159);
				recog.base.match_token(PromQLParser_BOOL,&mut recog.err_handler)?;

				}
			}

			recog.base.set_state(166);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==PromQLParser_ON || _la==PromQLParser_IGNORING {
				{
				/*InvokeRule onOrIgnoring*/
				recog.base.set_state(162);
				recog.onOrIgnoring()?;

				recog.base.set_state(164);
				recog.err_handler.sync(&mut recog.base)?;
				_la = recog.base.input.la(1);
				if _la==PromQLParser_GROUP_LEFT || _la==PromQLParser_GROUP_RIGHT {
					{
					/*InvokeRule groupModifier*/
					recog.base.set_state(163);
					recog.groupModifier()?;

					}
				}

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
//------------------- onOrIgnoring ----------------
pub type OnOrIgnoringContextAll<'input> = OnOrIgnoringContext<'input>;


pub type OnOrIgnoringContext<'input> = BaseParserRuleContext<'input,OnOrIgnoringContextExt<'input>>;

#[derive(Clone)]
pub struct OnOrIgnoringContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for OnOrIgnoringContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for OnOrIgnoringContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_onOrIgnoring(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_onOrIgnoring(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for OnOrIgnoringContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_onOrIgnoring(self);
	}
}

impl<'input> CustomRuleContext<'input> for OnOrIgnoringContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_onOrIgnoring }
	//fn type_rule_index() -> usize where Self: Sized { RULE_onOrIgnoring }
}
antlr4rust::tid!{OnOrIgnoringContextExt<'a>}

impl<'input> OnOrIgnoringContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<OnOrIgnoringContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,OnOrIgnoringContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait OnOrIgnoringContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<OnOrIgnoringContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ON
/// Returns `None` if there is no child corresponding to token ON
fn ON(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ON, 0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RPAREN, 0)
}
fn labelNameList(&self) -> Option<Rc<LabelNameListContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token IGNORING
/// Returns `None` if there is no child corresponding to token IGNORING
fn IGNORING(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_IGNORING, 0)
}

}

impl<'input> OnOrIgnoringContextAttrs<'input> for OnOrIgnoringContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn onOrIgnoring(&mut self,)
	-> Result<Rc<OnOrIgnoringContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = OnOrIgnoringContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 22, RULE_onOrIgnoring);
        let mut _localctx: Rc<OnOrIgnoringContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(180);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			PromQLParser_ON 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(168);
					recog.base.match_token(PromQLParser_ON,&mut recog.err_handler)?;

					recog.base.set_state(169);
					recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(171);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if ((((_la - 23)) & !0x3f) == 0 && ((1usize << (_la - 23)) & 134199295) != 0) || _la==PromQLParser_IDENTIFIER {
						{
						/*InvokeRule labelNameList*/
						recog.base.set_state(170);
						recog.labelNameList()?;

						}
					}

					recog.base.set_state(173);
					recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}

			PromQLParser_IGNORING 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(174);
					recog.base.match_token(PromQLParser_IGNORING,&mut recog.err_handler)?;

					recog.base.set_state(175);
					recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(177);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if ((((_la - 23)) & !0x3f) == 0 && ((1usize << (_la - 23)) & 134199295) != 0) || _la==PromQLParser_IDENTIFIER {
						{
						/*InvokeRule labelNameList*/
						recog.base.set_state(176);
						recog.labelNameList()?;

						}
					}

					recog.base.set_state(179);
					recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- groupModifier ----------------
pub type GroupModifierContextAll<'input> = GroupModifierContext<'input>;


pub type GroupModifierContext<'input> = BaseParserRuleContext<'input,GroupModifierContextExt<'input>>;

#[derive(Clone)]
pub struct GroupModifierContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for GroupModifierContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for GroupModifierContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_groupModifier(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_groupModifier(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for GroupModifierContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_groupModifier(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupModifierContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_groupModifier }
	//fn type_rule_index() -> usize where Self: Sized { RULE_groupModifier }
}
antlr4rust::tid!{GroupModifierContextExt<'a>}

impl<'input> GroupModifierContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<GroupModifierContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,GroupModifierContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait GroupModifierContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<GroupModifierContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token GROUP_LEFT
/// Returns `None` if there is no child corresponding to token GROUP_LEFT
fn GROUP_LEFT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_GROUP_LEFT, 0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RPAREN, 0)
}
fn labelNameList(&self) -> Option<Rc<LabelNameListContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token GROUP_RIGHT
/// Returns `None` if there is no child corresponding to token GROUP_RIGHT
fn GROUP_RIGHT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_GROUP_RIGHT, 0)
}

}

impl<'input> GroupModifierContextAttrs<'input> for GroupModifierContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn groupModifier(&mut self,)
	-> Result<Rc<GroupModifierContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = GroupModifierContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 24, RULE_groupModifier);
        let mut _localctx: Rc<GroupModifierContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(198);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			PromQLParser_GROUP_LEFT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(182);
					recog.base.match_token(PromQLParser_GROUP_LEFT,&mut recog.err_handler)?;

					recog.base.set_state(188);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(14,&mut recog.base)? {
						x if x == 1=>{
							{
							recog.base.set_state(183);
							recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

							recog.base.set_state(185);
							recog.err_handler.sync(&mut recog.base)?;
							_la = recog.base.input.la(1);
							if ((((_la - 23)) & !0x3f) == 0 && ((1usize << (_la - 23)) & 134199295) != 0) || _la==PromQLParser_IDENTIFIER {
								{
								/*InvokeRule labelNameList*/
								recog.base.set_state(184);
								recog.labelNameList()?;

								}
							}

							recog.base.set_state(187);
							recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

							}
						}

						_ => {}
					}
					}
				}

			PromQLParser_GROUP_RIGHT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(190);
					recog.base.match_token(PromQLParser_GROUP_RIGHT,&mut recog.err_handler)?;

					recog.base.set_state(196);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(16,&mut recog.base)? {
						x if x == 1=>{
							{
							recog.base.set_state(191);
							recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

							recog.base.set_state(193);
							recog.err_handler.sync(&mut recog.base)?;
							_la = recog.base.input.la(1);
							if ((((_la - 23)) & !0x3f) == 0 && ((1usize << (_la - 23)) & 134199295) != 0) || _la==PromQLParser_IDENTIFIER {
								{
								/*InvokeRule labelNameList*/
								recog.base.set_state(192);
								recog.labelNameList()?;

								}
							}

							recog.base.set_state(195);
							recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

							}
						}

						_ => {}
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
//------------------- instantSelector ----------------
pub type InstantSelectorContextAll<'input> = InstantSelectorContext<'input>;


pub type InstantSelectorContext<'input> = BaseParserRuleContext<'input,InstantSelectorContextExt<'input>>;

#[derive(Clone)]
pub struct InstantSelectorContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for InstantSelectorContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for InstantSelectorContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_instantSelector(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_instantSelector(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for InstantSelectorContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_instantSelector(self);
	}
}

impl<'input> CustomRuleContext<'input> for InstantSelectorContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_instantSelector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_instantSelector }
}
antlr4rust::tid!{InstantSelectorContextExt<'a>}

impl<'input> InstantSelectorContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<InstantSelectorContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,InstantSelectorContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait InstantSelectorContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<InstantSelectorContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token IDENTIFIER
/// Returns `None` if there is no child corresponding to token IDENTIFIER
fn IDENTIFIER(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_IDENTIFIER, 0)
}
/// Retrieves first TerminalNode corresponding to token LBRACE
/// Returns `None` if there is no child corresponding to token LBRACE
fn LBRACE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LBRACE, 0)
}
fn labelMatchers(&self) -> Option<Rc<LabelMatchersContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RBRACE
/// Returns `None` if there is no child corresponding to token RBRACE
fn RBRACE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RBRACE, 0)
}
fn offsetModifier(&self) -> Option<Rc<OffsetModifierContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn atModifier(&self) -> Option<Rc<AtModifierContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> InstantSelectorContextAttrs<'input> for InstantSelectorContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn instantSelector(&mut self,)
	-> Result<Rc<InstantSelectorContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = InstantSelectorContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 26, RULE_instantSelector);
        let mut _localctx: Rc<InstantSelectorContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(235);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(26,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(200);
					recog.base.match_token(PromQLParser_IDENTIFIER,&mut recog.err_handler)?;

					recog.base.set_state(201);
					recog.base.match_token(PromQLParser_LBRACE,&mut recog.err_handler)?;

					/*InvokeRule labelMatchers*/
					recog.base.set_state(202);
					recog.labelMatchers()?;

					recog.base.set_state(203);
					recog.base.match_token(PromQLParser_RBRACE,&mut recog.err_handler)?;

					recog.base.set_state(205);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(18,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule offsetModifier*/
							recog.base.set_state(204);
							recog.offsetModifier()?;

							}
						}

						_ => {}
					}
					recog.base.set_state(208);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(19,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule atModifier*/
							recog.base.set_state(207);
							recog.atModifier()?;

							}
						}

						_ => {}
					}
					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(210);
					recog.base.match_token(PromQLParser_IDENTIFIER,&mut recog.err_handler)?;

					recog.base.set_state(211);
					recog.base.match_token(PromQLParser_LBRACE,&mut recog.err_handler)?;

					recog.base.set_state(212);
					recog.base.match_token(PromQLParser_RBRACE,&mut recog.err_handler)?;

					recog.base.set_state(214);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(20,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule offsetModifier*/
							recog.base.set_state(213);
							recog.offsetModifier()?;

							}
						}

						_ => {}
					}
					recog.base.set_state(217);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(21,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule atModifier*/
							recog.base.set_state(216);
							recog.atModifier()?;

							}
						}

						_ => {}
					}
					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(219);
					recog.base.match_token(PromQLParser_IDENTIFIER,&mut recog.err_handler)?;

					recog.base.set_state(221);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(22,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule offsetModifier*/
							recog.base.set_state(220);
							recog.offsetModifier()?;

							}
						}

						_ => {}
					}
					recog.base.set_state(224);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(23,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule atModifier*/
							recog.base.set_state(223);
							recog.atModifier()?;

							}
						}

						_ => {}
					}
					}
				}
			,
				4 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					recog.base.set_state(226);
					recog.base.match_token(PromQLParser_LBRACE,&mut recog.err_handler)?;

					/*InvokeRule labelMatchers*/
					recog.base.set_state(227);
					recog.labelMatchers()?;

					recog.base.set_state(228);
					recog.base.match_token(PromQLParser_RBRACE,&mut recog.err_handler)?;

					recog.base.set_state(230);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(24,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule offsetModifier*/
							recog.base.set_state(229);
							recog.offsetModifier()?;

							}
						}

						_ => {}
					}
					recog.base.set_state(233);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(25,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule atModifier*/
							recog.base.set_state(232);
							recog.atModifier()?;

							}
						}

						_ => {}
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
//------------------- matrixSelector ----------------
pub type MatrixSelectorContextAll<'input> = MatrixSelectorContext<'input>;


pub type MatrixSelectorContext<'input> = BaseParserRuleContext<'input,MatrixSelectorContextExt<'input>>;

#[derive(Clone)]
pub struct MatrixSelectorContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for MatrixSelectorContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for MatrixSelectorContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_matrixSelector(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_matrixSelector(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for MatrixSelectorContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_matrixSelector(self);
	}
}

impl<'input> CustomRuleContext<'input> for MatrixSelectorContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_matrixSelector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_matrixSelector }
}
antlr4rust::tid!{MatrixSelectorContextExt<'a>}

impl<'input> MatrixSelectorContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<MatrixSelectorContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,MatrixSelectorContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait MatrixSelectorContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<MatrixSelectorContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token IDENTIFIER
/// Returns `None` if there is no child corresponding to token IDENTIFIER
fn IDENTIFIER(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_IDENTIFIER, 0)
}
/// Retrieves first TerminalNode corresponding to token LBRACE
/// Returns `None` if there is no child corresponding to token LBRACE
fn LBRACE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LBRACE, 0)
}
fn labelMatchers(&self) -> Option<Rc<LabelMatchersContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RBRACE
/// Returns `None` if there is no child corresponding to token RBRACE
fn RBRACE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RBRACE, 0)
}
fn timeRange(&self) -> Option<Rc<TimeRangeContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn offsetModifier(&self) -> Option<Rc<OffsetModifierContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn atModifier(&self) -> Option<Rc<AtModifierContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> MatrixSelectorContextAttrs<'input> for MatrixSelectorContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn matrixSelector(&mut self,)
	-> Result<Rc<MatrixSelectorContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = MatrixSelectorContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 28, RULE_matrixSelector);
        let mut _localctx: Rc<MatrixSelectorContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(276);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(35,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(237);
					recog.base.match_token(PromQLParser_IDENTIFIER,&mut recog.err_handler)?;

					recog.base.set_state(238);
					recog.base.match_token(PromQLParser_LBRACE,&mut recog.err_handler)?;

					/*InvokeRule labelMatchers*/
					recog.base.set_state(239);
					recog.labelMatchers()?;

					recog.base.set_state(240);
					recog.base.match_token(PromQLParser_RBRACE,&mut recog.err_handler)?;

					/*InvokeRule timeRange*/
					recog.base.set_state(241);
					recog.timeRange()?;

					recog.base.set_state(243);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(27,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule offsetModifier*/
							recog.base.set_state(242);
							recog.offsetModifier()?;

							}
						}

						_ => {}
					}
					recog.base.set_state(246);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(28,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule atModifier*/
							recog.base.set_state(245);
							recog.atModifier()?;

							}
						}

						_ => {}
					}
					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(248);
					recog.base.match_token(PromQLParser_IDENTIFIER,&mut recog.err_handler)?;

					recog.base.set_state(249);
					recog.base.match_token(PromQLParser_LBRACE,&mut recog.err_handler)?;

					recog.base.set_state(250);
					recog.base.match_token(PromQLParser_RBRACE,&mut recog.err_handler)?;

					/*InvokeRule timeRange*/
					recog.base.set_state(251);
					recog.timeRange()?;

					recog.base.set_state(253);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(29,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule offsetModifier*/
							recog.base.set_state(252);
							recog.offsetModifier()?;

							}
						}

						_ => {}
					}
					recog.base.set_state(256);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(30,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule atModifier*/
							recog.base.set_state(255);
							recog.atModifier()?;

							}
						}

						_ => {}
					}
					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(258);
					recog.base.match_token(PromQLParser_IDENTIFIER,&mut recog.err_handler)?;

					/*InvokeRule timeRange*/
					recog.base.set_state(259);
					recog.timeRange()?;

					recog.base.set_state(261);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(31,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule offsetModifier*/
							recog.base.set_state(260);
							recog.offsetModifier()?;

							}
						}

						_ => {}
					}
					recog.base.set_state(264);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(32,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule atModifier*/
							recog.base.set_state(263);
							recog.atModifier()?;

							}
						}

						_ => {}
					}
					}
				}
			,
				4 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					recog.base.set_state(266);
					recog.base.match_token(PromQLParser_LBRACE,&mut recog.err_handler)?;

					/*InvokeRule labelMatchers*/
					recog.base.set_state(267);
					recog.labelMatchers()?;

					recog.base.set_state(268);
					recog.base.match_token(PromQLParser_RBRACE,&mut recog.err_handler)?;

					/*InvokeRule timeRange*/
					recog.base.set_state(269);
					recog.timeRange()?;

					recog.base.set_state(271);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(33,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule offsetModifier*/
							recog.base.set_state(270);
							recog.offsetModifier()?;

							}
						}

						_ => {}
					}
					recog.base.set_state(274);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(34,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule atModifier*/
							recog.base.set_state(273);
							recog.atModifier()?;

							}
						}

						_ => {}
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
//------------------- labelMatchers ----------------
pub type LabelMatchersContextAll<'input> = LabelMatchersContext<'input>;


pub type LabelMatchersContext<'input> = BaseParserRuleContext<'input,LabelMatchersContextExt<'input>>;

#[derive(Clone)]
pub struct LabelMatchersContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for LabelMatchersContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for LabelMatchersContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelMatchers(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelMatchers(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for LabelMatchersContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelMatchers(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelMatchersContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelMatchers }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelMatchers }
}
antlr4rust::tid!{LabelMatchersContextExt<'a>}

impl<'input> LabelMatchersContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelMatchersContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelMatchersContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelMatchersContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<LabelMatchersContextExt<'input>>{

fn labelMatcher_all(&self) ->  Vec<Rc<LabelMatcherContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn labelMatcher(&self, i: usize) -> Option<Rc<LabelMatcherContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,PromQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_COMMA, i)
}

}

impl<'input> LabelMatchersContextAttrs<'input> for LabelMatchersContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelMatchers(&mut self,)
	-> Result<Rc<LabelMatchersContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelMatchersContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 30, RULE_labelMatchers);
        let mut _localctx: Rc<LabelMatchersContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule labelMatcher*/
			recog.base.set_state(278);
			recog.labelMatcher()?;

			recog.base.set_state(283);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(36,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					{
					{
					recog.base.set_state(279);
					recog.base.match_token(PromQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule labelMatcher*/
					recog.base.set_state(280);
					recog.labelMatcher()?;

					}
					} 
				}
				recog.base.set_state(285);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(36,&mut recog.base)?;
			}
			recog.base.set_state(287);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==PromQLParser_COMMA {
				{
				recog.base.set_state(286);
				recog.base.match_token(PromQLParser_COMMA,&mut recog.err_handler)?;

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
//------------------- labelMatcher ----------------
pub type LabelMatcherContextAll<'input> = LabelMatcherContext<'input>;


pub type LabelMatcherContext<'input> = BaseParserRuleContext<'input,LabelMatcherContextExt<'input>>;

#[derive(Clone)]
pub struct LabelMatcherContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for LabelMatcherContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for LabelMatcherContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelMatcher(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelMatcher(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for LabelMatcherContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelMatcher(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelMatcherContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelMatcher }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelMatcher }
}
antlr4rust::tid!{LabelMatcherContextExt<'a>}

impl<'input> LabelMatcherContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelMatcherContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelMatcherContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelMatcherContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<LabelMatcherContextExt<'input>>{

fn labelName(&self) -> Option<Rc<LabelNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn labelMatcherOp(&self) -> Option<Rc<LabelMatcherOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token STRING
/// Returns `None` if there is no child corresponding to token STRING
fn STRING(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_STRING, 0)
}

}

impl<'input> LabelMatcherContextAttrs<'input> for LabelMatcherContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelMatcher(&mut self,)
	-> Result<Rc<LabelMatcherContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelMatcherContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 32, RULE_labelMatcher);
        let mut _localctx: Rc<LabelMatcherContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule labelName*/
			recog.base.set_state(289);
			recog.labelName()?;

			/*InvokeRule labelMatcherOp*/
			recog.base.set_state(290);
			recog.labelMatcherOp()?;

			recog.base.set_state(291);
			recog.base.match_token(PromQLParser_STRING,&mut recog.err_handler)?;

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
//------------------- labelMatcherOp ----------------
pub type LabelMatcherOpContextAll<'input> = LabelMatcherOpContext<'input>;


pub type LabelMatcherOpContext<'input> = BaseParserRuleContext<'input,LabelMatcherOpContextExt<'input>>;

#[derive(Clone)]
pub struct LabelMatcherOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for LabelMatcherOpContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for LabelMatcherOpContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelMatcherOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelMatcherOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for LabelMatcherOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelMatcherOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelMatcherOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelMatcherOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelMatcherOp }
}
antlr4rust::tid!{LabelMatcherOpContextExt<'a>}

impl<'input> LabelMatcherOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelMatcherOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelMatcherOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelMatcherOpContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<LabelMatcherOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token EQ
/// Returns `None` if there is no child corresponding to token EQ
fn EQ(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_EQ, 0)
}
/// Retrieves first TerminalNode corresponding to token NE
/// Returns `None` if there is no child corresponding to token NE
fn NE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_NE, 0)
}
/// Retrieves first TerminalNode corresponding to token RE
/// Returns `None` if there is no child corresponding to token RE
fn RE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RE, 0)
}
/// Retrieves first TerminalNode corresponding to token NRE
/// Returns `None` if there is no child corresponding to token NRE
fn NRE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_NRE, 0)
}

}

impl<'input> LabelMatcherOpContextAttrs<'input> for LabelMatcherOpContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelMatcherOp(&mut self,)
	-> Result<Rc<LabelMatcherOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelMatcherOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 34, RULE_labelMatcherOp);
        let mut _localctx: Rc<LabelMatcherOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(293);
			_la = recog.base.input.la(1);
			if { !((((_la) & !0x3f) == 0 && ((1usize << _la) & 245760) != 0)) } {
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
//------------------- functionCall ----------------
pub type FunctionCallContextAll<'input> = FunctionCallContext<'input>;


pub type FunctionCallContext<'input> = BaseParserRuleContext<'input,FunctionCallContextExt<'input>>;

#[derive(Clone)]
pub struct FunctionCallContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for FunctionCallContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for FunctionCallContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_functionCall(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_functionCall(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for FunctionCallContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_functionCall(self);
	}
}

impl<'input> CustomRuleContext<'input> for FunctionCallContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_functionCall }
	//fn type_rule_index() -> usize where Self: Sized { RULE_functionCall }
}
antlr4rust::tid!{FunctionCallContextExt<'a>}

impl<'input> FunctionCallContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<FunctionCallContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,FunctionCallContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait FunctionCallContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<FunctionCallContextExt<'input>>{

fn functionName(&self) -> Option<Rc<FunctionNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RPAREN, 0)
}
fn expr_all(&self) ->  Vec<Rc<ExprContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn expr(&self, i: usize) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,PromQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_COMMA, i)
}

}

impl<'input> FunctionCallContextAttrs<'input> for FunctionCallContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn functionCall(&mut self,)
	-> Result<Rc<FunctionCallContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = FunctionCallContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 36, RULE_functionCall);
        let mut _localctx: Rc<FunctionCallContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule functionName*/
			recog.base.set_state(295);
			recog.functionName()?;

			recog.base.set_state(296);
			recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

			recog.base.set_state(305);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if (((_la) & !0x3f) == 0 && ((1usize << _la) & 1546) != 0) || ((((_la - 38)) & !0x3f) == 0 && ((1usize << (_la - 38)) & 4294967295) != 0) || ((((_la - 70)) & !0x3f) == 0 && ((1usize << (_la - 70)) & 4294967295) != 0) || ((((_la - 102)) & !0x3f) == 0 && ((1usize << (_la - 102)) & 436469759) != 0) {
				{
				/*InvokeRule expr*/
				recog.base.set_state(297);
				recog.expr_rec(0)?;

				recog.base.set_state(302);
				recog.err_handler.sync(&mut recog.base)?;
				_la = recog.base.input.la(1);
				while _la==PromQLParser_COMMA {
					{
					{
					recog.base.set_state(298);
					recog.base.match_token(PromQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule expr*/
					recog.base.set_state(299);
					recog.expr_rec(0)?;

					}
					}
					recog.base.set_state(304);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
				}
				}
			}

			recog.base.set_state(307);
			recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- functionName ----------------
pub type FunctionNameContextAll<'input> = FunctionNameContext<'input>;


pub type FunctionNameContext<'input> = BaseParserRuleContext<'input,FunctionNameContextExt<'input>>;

#[derive(Clone)]
pub struct FunctionNameContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for FunctionNameContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for FunctionNameContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_functionName(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_functionName(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for FunctionNameContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_functionName(self);
	}
}

impl<'input> CustomRuleContext<'input> for FunctionNameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_functionName }
	//fn type_rule_index() -> usize where Self: Sized { RULE_functionName }
}
antlr4rust::tid!{FunctionNameContextExt<'a>}

impl<'input> FunctionNameContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<FunctionNameContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,FunctionNameContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait FunctionNameContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<FunctionNameContextExt<'input>>{

fn rangeFunctionName(&self) -> Option<Rc<RangeFunctionNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn mathFunctionName(&self) -> Option<Rc<MathFunctionNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn trigFunctionName(&self) -> Option<Rc<TrigFunctionNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn labelFunctionName(&self) -> Option<Rc<LabelFunctionNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn timeFunctionName(&self) -> Option<Rc<TimeFunctionNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn histogramFunctionName(&self) -> Option<Rc<HistogramFunctionNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token ABSENT
/// Returns `None` if there is no child corresponding to token ABSENT
fn ABSENT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ABSENT, 0)
}
/// Retrieves first TerminalNode corresponding to token SCALAR
/// Returns `None` if there is no child corresponding to token SCALAR
fn SCALAR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SCALAR, 0)
}
/// Retrieves first TerminalNode corresponding to token VECTOR
/// Returns `None` if there is no child corresponding to token VECTOR
fn VECTOR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_VECTOR, 0)
}
/// Retrieves first TerminalNode corresponding to token SORT
/// Returns `None` if there is no child corresponding to token SORT
fn SORT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SORT, 0)
}
/// Retrieves first TerminalNode corresponding to token SORT_DESC
/// Returns `None` if there is no child corresponding to token SORT_DESC
fn SORT_DESC(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SORT_DESC, 0)
}
/// Retrieves first TerminalNode corresponding to token PI
/// Returns `None` if there is no child corresponding to token PI
fn PI(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_PI, 0)
}

}

impl<'input> FunctionNameContextAttrs<'input> for FunctionNameContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn functionName(&mut self,)
	-> Result<Rc<FunctionNameContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = FunctionNameContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 38, RULE_functionName);
        let mut _localctx: Rc<FunctionNameContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(321);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			PromQLParser_RATE |PromQLParser_IRATE |PromQLParser_INCREASE |PromQLParser_DELTA |
			PromQLParser_IDELTA |PromQLParser_DERIV |PromQLParser_CHANGES |PromQLParser_RESETS |
			PromQLParser_PREDICT_LINEAR |PromQLParser_HOLT_WINTERS |PromQLParser_AVG_OVER_TIME |
			PromQLParser_MIN_OVER_TIME |PromQLParser_MAX_OVER_TIME |PromQLParser_SUM_OVER_TIME |
			PromQLParser_COUNT_OVER_TIME |PromQLParser_QUANTILE_OVER_TIME |PromQLParser_STDDEV_OVER_TIME |
			PromQLParser_STDVAR_OVER_TIME |PromQLParser_LAST_OVER_TIME |PromQLParser_PRESENT_OVER_TIME |
			PromQLParser_ABSENT_OVER_TIME 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule rangeFunctionName*/
					recog.base.set_state(309);
					recog.rangeFunctionName()?;

					}
				}

			PromQLParser_ABS |PromQLParser_CEIL |PromQLParser_CLAMP |PromQLParser_CLAMP_MAX |
			PromQLParser_CLAMP_MIN |PromQLParser_EXP |PromQLParser_FLOOR |PromQLParser_LN |
			PromQLParser_LOG2 |PromQLParser_LOG10 |PromQLParser_ROUND |PromQLParser_SGN |
			PromQLParser_SQRT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule mathFunctionName*/
					recog.base.set_state(310);
					recog.mathFunctionName()?;

					}
				}

			PromQLParser_ACOS |PromQLParser_ACOSH |PromQLParser_ASIN |PromQLParser_ASINH |
			PromQLParser_ATAN |PromQLParser_ATANH |PromQLParser_COS |PromQLParser_COSH |
			PromQLParser_SIN |PromQLParser_SINH |PromQLParser_TAN |PromQLParser_TANH |
			PromQLParser_DEG |PromQLParser_RAD 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					/*InvokeRule trigFunctionName*/
					recog.base.set_state(311);
					recog.trigFunctionName()?;

					}
				}

			PromQLParser_LABEL_JOIN |PromQLParser_LABEL_REPLACE 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					/*InvokeRule labelFunctionName*/
					recog.base.set_state(312);
					recog.labelFunctionName()?;

					}
				}

			PromQLParser_TIME |PromQLParser_TIMESTAMP |PromQLParser_DAY_OF_MONTH |
			PromQLParser_DAY_OF_WEEK |PromQLParser_DAY_OF_YEAR |PromQLParser_DAYS_IN_MONTH |
			PromQLParser_HOUR |PromQLParser_MINUTE |PromQLParser_MONTH |PromQLParser_YEAR 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 5)?;
					recog.base.enter_outer_alt(None, 5)?;
					{
					/*InvokeRule timeFunctionName*/
					recog.base.set_state(313);
					recog.timeFunctionName()?;

					}
				}

			PromQLParser_HISTOGRAM_COUNT |PromQLParser_HISTOGRAM_SUM |PromQLParser_HISTOGRAM_FRACTION |
			PromQLParser_HISTOGRAM_QUANTILE 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 6)?;
					recog.base.enter_outer_alt(None, 6)?;
					{
					/*InvokeRule histogramFunctionName*/
					recog.base.set_state(314);
					recog.histogramFunctionName()?;

					}
				}

			PromQLParser_ABSENT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 7)?;
					recog.base.enter_outer_alt(None, 7)?;
					{
					recog.base.set_state(315);
					recog.base.match_token(PromQLParser_ABSENT,&mut recog.err_handler)?;

					}
				}

			PromQLParser_SCALAR 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 8)?;
					recog.base.enter_outer_alt(None, 8)?;
					{
					recog.base.set_state(316);
					recog.base.match_token(PromQLParser_SCALAR,&mut recog.err_handler)?;

					}
				}

			PromQLParser_VECTOR 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 9)?;
					recog.base.enter_outer_alt(None, 9)?;
					{
					recog.base.set_state(317);
					recog.base.match_token(PromQLParser_VECTOR,&mut recog.err_handler)?;

					}
				}

			PromQLParser_SORT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 10)?;
					recog.base.enter_outer_alt(None, 10)?;
					{
					recog.base.set_state(318);
					recog.base.match_token(PromQLParser_SORT,&mut recog.err_handler)?;

					}
				}

			PromQLParser_SORT_DESC 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 11)?;
					recog.base.enter_outer_alt(None, 11)?;
					{
					recog.base.set_state(319);
					recog.base.match_token(PromQLParser_SORT_DESC,&mut recog.err_handler)?;

					}
				}

			PromQLParser_PI 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 12)?;
					recog.base.enter_outer_alt(None, 12)?;
					{
					recog.base.set_state(320);
					recog.base.match_token(PromQLParser_PI,&mut recog.err_handler)?;

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
//------------------- rangeFunctionName ----------------
pub type RangeFunctionNameContextAll<'input> = RangeFunctionNameContext<'input>;


pub type RangeFunctionNameContext<'input> = BaseParserRuleContext<'input,RangeFunctionNameContextExt<'input>>;

#[derive(Clone)]
pub struct RangeFunctionNameContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for RangeFunctionNameContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for RangeFunctionNameContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_rangeFunctionName(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_rangeFunctionName(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for RangeFunctionNameContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeFunctionName(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeFunctionNameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeFunctionName }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeFunctionName }
}
antlr4rust::tid!{RangeFunctionNameContextExt<'a>}

impl<'input> RangeFunctionNameContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeFunctionNameContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeFunctionNameContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RangeFunctionNameContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<RangeFunctionNameContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token RATE
/// Returns `None` if there is no child corresponding to token RATE
fn RATE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RATE, 0)
}
/// Retrieves first TerminalNode corresponding to token IRATE
/// Returns `None` if there is no child corresponding to token IRATE
fn IRATE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_IRATE, 0)
}
/// Retrieves first TerminalNode corresponding to token INCREASE
/// Returns `None` if there is no child corresponding to token INCREASE
fn INCREASE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_INCREASE, 0)
}
/// Retrieves first TerminalNode corresponding to token DELTA
/// Returns `None` if there is no child corresponding to token DELTA
fn DELTA(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_DELTA, 0)
}
/// Retrieves first TerminalNode corresponding to token IDELTA
/// Returns `None` if there is no child corresponding to token IDELTA
fn IDELTA(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_IDELTA, 0)
}
/// Retrieves first TerminalNode corresponding to token DERIV
/// Returns `None` if there is no child corresponding to token DERIV
fn DERIV(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_DERIV, 0)
}
/// Retrieves first TerminalNode corresponding to token CHANGES
/// Returns `None` if there is no child corresponding to token CHANGES
fn CHANGES(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_CHANGES, 0)
}
/// Retrieves first TerminalNode corresponding to token RESETS
/// Returns `None` if there is no child corresponding to token RESETS
fn RESETS(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RESETS, 0)
}
/// Retrieves first TerminalNode corresponding to token PREDICT_LINEAR
/// Returns `None` if there is no child corresponding to token PREDICT_LINEAR
fn PREDICT_LINEAR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_PREDICT_LINEAR, 0)
}
/// Retrieves first TerminalNode corresponding to token HOLT_WINTERS
/// Returns `None` if there is no child corresponding to token HOLT_WINTERS
fn HOLT_WINTERS(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_HOLT_WINTERS, 0)
}
/// Retrieves first TerminalNode corresponding to token AVG_OVER_TIME
/// Returns `None` if there is no child corresponding to token AVG_OVER_TIME
fn AVG_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AVG_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token MIN_OVER_TIME
/// Returns `None` if there is no child corresponding to token MIN_OVER_TIME
fn MIN_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_MIN_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token MAX_OVER_TIME
/// Returns `None` if there is no child corresponding to token MAX_OVER_TIME
fn MAX_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_MAX_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token SUM_OVER_TIME
/// Returns `None` if there is no child corresponding to token SUM_OVER_TIME
fn SUM_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SUM_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token COUNT_OVER_TIME
/// Returns `None` if there is no child corresponding to token COUNT_OVER_TIME
fn COUNT_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_COUNT_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token QUANTILE_OVER_TIME
/// Returns `None` if there is no child corresponding to token QUANTILE_OVER_TIME
fn QUANTILE_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_QUANTILE_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token STDDEV_OVER_TIME
/// Returns `None` if there is no child corresponding to token STDDEV_OVER_TIME
fn STDDEV_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_STDDEV_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token STDVAR_OVER_TIME
/// Returns `None` if there is no child corresponding to token STDVAR_OVER_TIME
fn STDVAR_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_STDVAR_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token LAST_OVER_TIME
/// Returns `None` if there is no child corresponding to token LAST_OVER_TIME
fn LAST_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LAST_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token PRESENT_OVER_TIME
/// Returns `None` if there is no child corresponding to token PRESENT_OVER_TIME
fn PRESENT_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_PRESENT_OVER_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token ABSENT_OVER_TIME
/// Returns `None` if there is no child corresponding to token ABSENT_OVER_TIME
fn ABSENT_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ABSENT_OVER_TIME, 0)
}

}

impl<'input> RangeFunctionNameContextAttrs<'input> for RangeFunctionNameContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn rangeFunctionName(&mut self,)
	-> Result<Rc<RangeFunctionNameContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeFunctionNameContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 40, RULE_rangeFunctionName);
        let mut _localctx: Rc<RangeFunctionNameContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(323);
			_la = recog.base.input.la(1);
			if { !(((((_la - 50)) & !0x3f) == 0 && ((1usize << (_la - 50)) & 2097151) != 0)) } {
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
//------------------- mathFunctionName ----------------
pub type MathFunctionNameContextAll<'input> = MathFunctionNameContext<'input>;


pub type MathFunctionNameContext<'input> = BaseParserRuleContext<'input,MathFunctionNameContextExt<'input>>;

#[derive(Clone)]
pub struct MathFunctionNameContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for MathFunctionNameContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for MathFunctionNameContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_mathFunctionName(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_mathFunctionName(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for MathFunctionNameContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_mathFunctionName(self);
	}
}

impl<'input> CustomRuleContext<'input> for MathFunctionNameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_mathFunctionName }
	//fn type_rule_index() -> usize where Self: Sized { RULE_mathFunctionName }
}
antlr4rust::tid!{MathFunctionNameContextExt<'a>}

impl<'input> MathFunctionNameContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<MathFunctionNameContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,MathFunctionNameContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait MathFunctionNameContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<MathFunctionNameContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ABS
/// Returns `None` if there is no child corresponding to token ABS
fn ABS(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ABS, 0)
}
/// Retrieves first TerminalNode corresponding to token CEIL
/// Returns `None` if there is no child corresponding to token CEIL
fn CEIL(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_CEIL, 0)
}
/// Retrieves first TerminalNode corresponding to token CLAMP
/// Returns `None` if there is no child corresponding to token CLAMP
fn CLAMP(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_CLAMP, 0)
}
/// Retrieves first TerminalNode corresponding to token CLAMP_MAX
/// Returns `None` if there is no child corresponding to token CLAMP_MAX
fn CLAMP_MAX(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_CLAMP_MAX, 0)
}
/// Retrieves first TerminalNode corresponding to token CLAMP_MIN
/// Returns `None` if there is no child corresponding to token CLAMP_MIN
fn CLAMP_MIN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_CLAMP_MIN, 0)
}
/// Retrieves first TerminalNode corresponding to token EXP
/// Returns `None` if there is no child corresponding to token EXP
fn EXP(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_EXP, 0)
}
/// Retrieves first TerminalNode corresponding to token FLOOR
/// Returns `None` if there is no child corresponding to token FLOOR
fn FLOOR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_FLOOR, 0)
}
/// Retrieves first TerminalNode corresponding to token LN
/// Returns `None` if there is no child corresponding to token LN
fn LN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LN, 0)
}
/// Retrieves first TerminalNode corresponding to token LOG2
/// Returns `None` if there is no child corresponding to token LOG2
fn LOG2(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LOG2, 0)
}
/// Retrieves first TerminalNode corresponding to token LOG10
/// Returns `None` if there is no child corresponding to token LOG10
fn LOG10(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LOG10, 0)
}
/// Retrieves first TerminalNode corresponding to token ROUND
/// Returns `None` if there is no child corresponding to token ROUND
fn ROUND(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ROUND, 0)
}
/// Retrieves first TerminalNode corresponding to token SGN
/// Returns `None` if there is no child corresponding to token SGN
fn SGN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SGN, 0)
}
/// Retrieves first TerminalNode corresponding to token SQRT
/// Returns `None` if there is no child corresponding to token SQRT
fn SQRT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SQRT, 0)
}

}

impl<'input> MathFunctionNameContextAttrs<'input> for MathFunctionNameContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn mathFunctionName(&mut self,)
	-> Result<Rc<MathFunctionNameContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = MathFunctionNameContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 42, RULE_mathFunctionName);
        let mut _localctx: Rc<MathFunctionNameContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(325);
			_la = recog.base.input.la(1);
			if { !(((((_la - 71)) & !0x3f) == 0 && ((1usize << (_la - 71)) & 77821) != 0)) } {
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
//------------------- trigFunctionName ----------------
pub type TrigFunctionNameContextAll<'input> = TrigFunctionNameContext<'input>;


pub type TrigFunctionNameContext<'input> = BaseParserRuleContext<'input,TrigFunctionNameContextExt<'input>>;

#[derive(Clone)]
pub struct TrigFunctionNameContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for TrigFunctionNameContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for TrigFunctionNameContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_trigFunctionName(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_trigFunctionName(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for TrigFunctionNameContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_trigFunctionName(self);
	}
}

impl<'input> CustomRuleContext<'input> for TrigFunctionNameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_trigFunctionName }
	//fn type_rule_index() -> usize where Self: Sized { RULE_trigFunctionName }
}
antlr4rust::tid!{TrigFunctionNameContextExt<'a>}

impl<'input> TrigFunctionNameContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<TrigFunctionNameContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,TrigFunctionNameContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait TrigFunctionNameContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<TrigFunctionNameContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ACOS
/// Returns `None` if there is no child corresponding to token ACOS
fn ACOS(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ACOS, 0)
}
/// Retrieves first TerminalNode corresponding to token ACOSH
/// Returns `None` if there is no child corresponding to token ACOSH
fn ACOSH(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ACOSH, 0)
}
/// Retrieves first TerminalNode corresponding to token ASIN
/// Returns `None` if there is no child corresponding to token ASIN
fn ASIN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ASIN, 0)
}
/// Retrieves first TerminalNode corresponding to token ASINH
/// Returns `None` if there is no child corresponding to token ASINH
fn ASINH(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ASINH, 0)
}
/// Retrieves first TerminalNode corresponding to token ATAN
/// Returns `None` if there is no child corresponding to token ATAN
fn ATAN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ATAN, 0)
}
/// Retrieves first TerminalNode corresponding to token ATANH
/// Returns `None` if there is no child corresponding to token ATANH
fn ATANH(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ATANH, 0)
}
/// Retrieves first TerminalNode corresponding to token COS
/// Returns `None` if there is no child corresponding to token COS
fn COS(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_COS, 0)
}
/// Retrieves first TerminalNode corresponding to token COSH
/// Returns `None` if there is no child corresponding to token COSH
fn COSH(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_COSH, 0)
}
/// Retrieves first TerminalNode corresponding to token SIN
/// Returns `None` if there is no child corresponding to token SIN
fn SIN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SIN, 0)
}
/// Retrieves first TerminalNode corresponding to token SINH
/// Returns `None` if there is no child corresponding to token SINH
fn SINH(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SINH, 0)
}
/// Retrieves first TerminalNode corresponding to token TAN
/// Returns `None` if there is no child corresponding to token TAN
fn TAN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_TAN, 0)
}
/// Retrieves first TerminalNode corresponding to token TANH
/// Returns `None` if there is no child corresponding to token TANH
fn TANH(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_TANH, 0)
}
/// Retrieves first TerminalNode corresponding to token DEG
/// Returns `None` if there is no child corresponding to token DEG
fn DEG(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_DEG, 0)
}
/// Retrieves first TerminalNode corresponding to token RAD
/// Returns `None` if there is no child corresponding to token RAD
fn RAD(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RAD, 0)
}

}

impl<'input> TrigFunctionNameContextAttrs<'input> for TrigFunctionNameContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn trigFunctionName(&mut self,)
	-> Result<Rc<TrigFunctionNameContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = TrigFunctionNameContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 44, RULE_trigFunctionName);
        let mut _localctx: Rc<TrigFunctionNameContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(327);
			_la = recog.base.input.la(1);
			if { !(((((_la - 88)) & !0x3f) == 0 && ((1usize << (_la - 88)) & 16383) != 0)) } {
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
//------------------- labelFunctionName ----------------
pub type LabelFunctionNameContextAll<'input> = LabelFunctionNameContext<'input>;


pub type LabelFunctionNameContext<'input> = BaseParserRuleContext<'input,LabelFunctionNameContextExt<'input>>;

#[derive(Clone)]
pub struct LabelFunctionNameContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for LabelFunctionNameContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for LabelFunctionNameContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelFunctionName(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelFunctionName(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for LabelFunctionNameContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFunctionName(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFunctionNameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFunctionName }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFunctionName }
}
antlr4rust::tid!{LabelFunctionNameContextExt<'a>}

impl<'input> LabelFunctionNameContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelFunctionNameContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelFunctionNameContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelFunctionNameContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<LabelFunctionNameContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LABEL_JOIN
/// Returns `None` if there is no child corresponding to token LABEL_JOIN
fn LABEL_JOIN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LABEL_JOIN, 0)
}
/// Retrieves first TerminalNode corresponding to token LABEL_REPLACE
/// Returns `None` if there is no child corresponding to token LABEL_REPLACE
fn LABEL_REPLACE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LABEL_REPLACE, 0)
}

}

impl<'input> LabelFunctionNameContextAttrs<'input> for LabelFunctionNameContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelFunctionName(&mut self,)
	-> Result<Rc<LabelFunctionNameContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelFunctionNameContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 46, RULE_labelFunctionName);
        let mut _localctx: Rc<LabelFunctionNameContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(329);
			_la = recog.base.input.la(1);
			if { !(_la==PromQLParser_LABEL_JOIN || _la==PromQLParser_LABEL_REPLACE) } {
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
//------------------- timeFunctionName ----------------
pub type TimeFunctionNameContextAll<'input> = TimeFunctionNameContext<'input>;


pub type TimeFunctionNameContext<'input> = BaseParserRuleContext<'input,TimeFunctionNameContextExt<'input>>;

#[derive(Clone)]
pub struct TimeFunctionNameContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for TimeFunctionNameContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for TimeFunctionNameContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_timeFunctionName(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_timeFunctionName(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for TimeFunctionNameContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_timeFunctionName(self);
	}
}

impl<'input> CustomRuleContext<'input> for TimeFunctionNameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_timeFunctionName }
	//fn type_rule_index() -> usize where Self: Sized { RULE_timeFunctionName }
}
antlr4rust::tid!{TimeFunctionNameContextExt<'a>}

impl<'input> TimeFunctionNameContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<TimeFunctionNameContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,TimeFunctionNameContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait TimeFunctionNameContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<TimeFunctionNameContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token TIME
/// Returns `None` if there is no child corresponding to token TIME
fn TIME(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_TIME, 0)
}
/// Retrieves first TerminalNode corresponding to token TIMESTAMP
/// Returns `None` if there is no child corresponding to token TIMESTAMP
fn TIMESTAMP(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_TIMESTAMP, 0)
}
/// Retrieves first TerminalNode corresponding to token DAY_OF_MONTH
/// Returns `None` if there is no child corresponding to token DAY_OF_MONTH
fn DAY_OF_MONTH(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_DAY_OF_MONTH, 0)
}
/// Retrieves first TerminalNode corresponding to token DAY_OF_WEEK
/// Returns `None` if there is no child corresponding to token DAY_OF_WEEK
fn DAY_OF_WEEK(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_DAY_OF_WEEK, 0)
}
/// Retrieves first TerminalNode corresponding to token DAY_OF_YEAR
/// Returns `None` if there is no child corresponding to token DAY_OF_YEAR
fn DAY_OF_YEAR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_DAY_OF_YEAR, 0)
}
/// Retrieves first TerminalNode corresponding to token DAYS_IN_MONTH
/// Returns `None` if there is no child corresponding to token DAYS_IN_MONTH
fn DAYS_IN_MONTH(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_DAYS_IN_MONTH, 0)
}
/// Retrieves first TerminalNode corresponding to token HOUR
/// Returns `None` if there is no child corresponding to token HOUR
fn HOUR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_HOUR, 0)
}
/// Retrieves first TerminalNode corresponding to token MINUTE
/// Returns `None` if there is no child corresponding to token MINUTE
fn MINUTE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_MINUTE, 0)
}
/// Retrieves first TerminalNode corresponding to token MONTH
/// Returns `None` if there is no child corresponding to token MONTH
fn MONTH(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_MONTH, 0)
}
/// Retrieves first TerminalNode corresponding to token YEAR
/// Returns `None` if there is no child corresponding to token YEAR
fn YEAR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_YEAR, 0)
}

}

impl<'input> TimeFunctionNameContextAttrs<'input> for TimeFunctionNameContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn timeFunctionName(&mut self,)
	-> Result<Rc<TimeFunctionNameContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = TimeFunctionNameContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 48, RULE_timeFunctionName);
        let mut _localctx: Rc<TimeFunctionNameContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(331);
			_la = recog.base.input.la(1);
			if { !(((((_la - 105)) & !0x3f) == 0 && ((1usize << (_la - 105)) & 1023) != 0)) } {
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
//------------------- histogramFunctionName ----------------
pub type HistogramFunctionNameContextAll<'input> = HistogramFunctionNameContext<'input>;


pub type HistogramFunctionNameContext<'input> = BaseParserRuleContext<'input,HistogramFunctionNameContextExt<'input>>;

#[derive(Clone)]
pub struct HistogramFunctionNameContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for HistogramFunctionNameContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for HistogramFunctionNameContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_histogramFunctionName(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_histogramFunctionName(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for HistogramFunctionNameContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_histogramFunctionName(self);
	}
}

impl<'input> CustomRuleContext<'input> for HistogramFunctionNameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_histogramFunctionName }
	//fn type_rule_index() -> usize where Self: Sized { RULE_histogramFunctionName }
}
antlr4rust::tid!{HistogramFunctionNameContextExt<'a>}

impl<'input> HistogramFunctionNameContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<HistogramFunctionNameContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,HistogramFunctionNameContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait HistogramFunctionNameContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<HistogramFunctionNameContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token HISTOGRAM_COUNT
/// Returns `None` if there is no child corresponding to token HISTOGRAM_COUNT
fn HISTOGRAM_COUNT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_HISTOGRAM_COUNT, 0)
}
/// Retrieves first TerminalNode corresponding to token HISTOGRAM_SUM
/// Returns `None` if there is no child corresponding to token HISTOGRAM_SUM
fn HISTOGRAM_SUM(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_HISTOGRAM_SUM, 0)
}
/// Retrieves first TerminalNode corresponding to token HISTOGRAM_FRACTION
/// Returns `None` if there is no child corresponding to token HISTOGRAM_FRACTION
fn HISTOGRAM_FRACTION(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_HISTOGRAM_FRACTION, 0)
}
/// Retrieves first TerminalNode corresponding to token HISTOGRAM_QUANTILE
/// Returns `None` if there is no child corresponding to token HISTOGRAM_QUANTILE
fn HISTOGRAM_QUANTILE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_HISTOGRAM_QUANTILE, 0)
}

}

impl<'input> HistogramFunctionNameContextAttrs<'input> for HistogramFunctionNameContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn histogramFunctionName(&mut self,)
	-> Result<Rc<HistogramFunctionNameContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = HistogramFunctionNameContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 50, RULE_histogramFunctionName);
        let mut _localctx: Rc<HistogramFunctionNameContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(333);
			_la = recog.base.input.la(1);
			if { !(((((_la - 115)) & !0x3f) == 0 && ((1usize << (_la - 115)) & 15) != 0)) } {
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
//------------------- aggregation ----------------
pub type AggregationContextAll<'input> = AggregationContext<'input>;


pub type AggregationContext<'input> = BaseParserRuleContext<'input,AggregationContextExt<'input>>;

#[derive(Clone)]
pub struct AggregationContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for AggregationContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for AggregationContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_aggregation(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_aggregation(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for AggregationContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_aggregation(self);
	}
}

impl<'input> CustomRuleContext<'input> for AggregationContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_aggregation }
	//fn type_rule_index() -> usize where Self: Sized { RULE_aggregation }
}
antlr4rust::tid!{AggregationContextExt<'a>}

impl<'input> AggregationContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<AggregationContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,AggregationContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait AggregationContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<AggregationContextExt<'input>>{

fn aggregationOp(&self) -> Option<Rc<AggregationOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RPAREN, 0)
}
fn expr_all(&self) ->  Vec<Rc<ExprContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn expr(&self, i: usize) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
fn grouping(&self) -> Option<Rc<GroupingContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,PromQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_COMMA, i)
}

}

impl<'input> AggregationContextAttrs<'input> for AggregationContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn aggregation(&mut self,)
	-> Result<Rc<AggregationContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = AggregationContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 52, RULE_aggregation);
        let mut _localctx: Rc<AggregationContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(366);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(46,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule aggregationOp*/
					recog.base.set_state(335);
					recog.aggregationOp()?;

					recog.base.set_state(336);
					recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(345);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if (((_la) & !0x3f) == 0 && ((1usize << _la) & 1546) != 0) || ((((_la - 38)) & !0x3f) == 0 && ((1usize << (_la - 38)) & 4294967295) != 0) || ((((_la - 70)) & !0x3f) == 0 && ((1usize << (_la - 70)) & 4294967295) != 0) || ((((_la - 102)) & !0x3f) == 0 && ((1usize << (_la - 102)) & 436469759) != 0) {
						{
						/*InvokeRule expr*/
						recog.base.set_state(337);
						recog.expr_rec(0)?;

						recog.base.set_state(342);
						recog.err_handler.sync(&mut recog.base)?;
						_la = recog.base.input.la(1);
						while _la==PromQLParser_COMMA {
							{
							{
							recog.base.set_state(338);
							recog.base.match_token(PromQLParser_COMMA,&mut recog.err_handler)?;

							/*InvokeRule expr*/
							recog.base.set_state(339);
							recog.expr_rec(0)?;

							}
							}
							recog.base.set_state(344);
							recog.err_handler.sync(&mut recog.base)?;
							_la = recog.base.input.la(1);
						}
						}
					}

					recog.base.set_state(347);
					recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

					recog.base.set_state(349);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(43,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule grouping*/
							recog.base.set_state(348);
							recog.grouping()?;

							}
						}

						_ => {}
					}
					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule aggregationOp*/
					recog.base.set_state(351);
					recog.aggregationOp()?;

					/*InvokeRule grouping*/
					recog.base.set_state(352);
					recog.grouping()?;

					recog.base.set_state(353);
					recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(362);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if (((_la) & !0x3f) == 0 && ((1usize << _la) & 1546) != 0) || ((((_la - 38)) & !0x3f) == 0 && ((1usize << (_la - 38)) & 4294967295) != 0) || ((((_la - 70)) & !0x3f) == 0 && ((1usize << (_la - 70)) & 4294967295) != 0) || ((((_la - 102)) & !0x3f) == 0 && ((1usize << (_la - 102)) & 436469759) != 0) {
						{
						/*InvokeRule expr*/
						recog.base.set_state(354);
						recog.expr_rec(0)?;

						recog.base.set_state(359);
						recog.err_handler.sync(&mut recog.base)?;
						_la = recog.base.input.la(1);
						while _la==PromQLParser_COMMA {
							{
							{
							recog.base.set_state(355);
							recog.base.match_token(PromQLParser_COMMA,&mut recog.err_handler)?;

							/*InvokeRule expr*/
							recog.base.set_state(356);
							recog.expr_rec(0)?;

							}
							}
							recog.base.set_state(361);
							recog.err_handler.sync(&mut recog.base)?;
							_la = recog.base.input.la(1);
						}
						}
					}

					recog.base.set_state(364);
					recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- aggregationOp ----------------
pub type AggregationOpContextAll<'input> = AggregationOpContext<'input>;


pub type AggregationOpContext<'input> = BaseParserRuleContext<'input,AggregationOpContextExt<'input>>;

#[derive(Clone)]
pub struct AggregationOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for AggregationOpContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for AggregationOpContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_aggregationOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_aggregationOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for AggregationOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_aggregationOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for AggregationOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_aggregationOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_aggregationOp }
}
antlr4rust::tid!{AggregationOpContextExt<'a>}

impl<'input> AggregationOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<AggregationOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,AggregationOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait AggregationOpContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<AggregationOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token AGG_SUM
/// Returns `None` if there is no child corresponding to token AGG_SUM
fn AGG_SUM(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_SUM, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_MIN
/// Returns `None` if there is no child corresponding to token AGG_MIN
fn AGG_MIN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_MIN, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_MAX
/// Returns `None` if there is no child corresponding to token AGG_MAX
fn AGG_MAX(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_MAX, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_AVG
/// Returns `None` if there is no child corresponding to token AGG_AVG
fn AGG_AVG(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_AVG, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_GROUP
/// Returns `None` if there is no child corresponding to token AGG_GROUP
fn AGG_GROUP(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_GROUP, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_STDDEV
/// Returns `None` if there is no child corresponding to token AGG_STDDEV
fn AGG_STDDEV(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_STDDEV, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_STDVAR
/// Returns `None` if there is no child corresponding to token AGG_STDVAR
fn AGG_STDVAR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_STDVAR, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_COUNT
/// Returns `None` if there is no child corresponding to token AGG_COUNT
fn AGG_COUNT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_COUNT, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_COUNT_VALUES
/// Returns `None` if there is no child corresponding to token AGG_COUNT_VALUES
fn AGG_COUNT_VALUES(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_COUNT_VALUES, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_BOTTOMK
/// Returns `None` if there is no child corresponding to token AGG_BOTTOMK
fn AGG_BOTTOMK(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_BOTTOMK, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_TOPK
/// Returns `None` if there is no child corresponding to token AGG_TOPK
fn AGG_TOPK(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_TOPK, 0)
}
/// Retrieves first TerminalNode corresponding to token AGG_QUANTILE
/// Returns `None` if there is no child corresponding to token AGG_QUANTILE
fn AGG_QUANTILE(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AGG_QUANTILE, 0)
}

}

impl<'input> AggregationOpContextAttrs<'input> for AggregationOpContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn aggregationOp(&mut self,)
	-> Result<Rc<AggregationOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = AggregationOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 54, RULE_aggregationOp);
        let mut _localctx: Rc<AggregationOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(368);
			_la = recog.base.input.la(1);
			if { !(((((_la - 38)) & !0x3f) == 0 && ((1usize << (_la - 38)) & 4095) != 0)) } {
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
//------------------- grouping ----------------
pub type GroupingContextAll<'input> = GroupingContext<'input>;


pub type GroupingContext<'input> = BaseParserRuleContext<'input,GroupingContextExt<'input>>;

#[derive(Clone)]
pub struct GroupingContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for GroupingContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for GroupingContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_grouping(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_grouping(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for GroupingContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_grouping(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupingContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_grouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_grouping }
}
antlr4rust::tid!{GroupingContextExt<'a>}

impl<'input> GroupingContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<GroupingContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,GroupingContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait GroupingContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<GroupingContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token BY
/// Returns `None` if there is no child corresponding to token BY
fn BY(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_BY, 0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RPAREN, 0)
}
fn labelNameList(&self) -> Option<Rc<LabelNameListContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token WITHOUT
/// Returns `None` if there is no child corresponding to token WITHOUT
fn WITHOUT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_WITHOUT, 0)
}

}

impl<'input> GroupingContextAttrs<'input> for GroupingContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn grouping(&mut self,)
	-> Result<Rc<GroupingContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = GroupingContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 56, RULE_grouping);
        let mut _localctx: Rc<GroupingContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(382);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			PromQLParser_BY 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(370);
					recog.base.match_token(PromQLParser_BY,&mut recog.err_handler)?;

					recog.base.set_state(371);
					recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(373);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if ((((_la - 23)) & !0x3f) == 0 && ((1usize << (_la - 23)) & 134199295) != 0) || _la==PromQLParser_IDENTIFIER {
						{
						/*InvokeRule labelNameList*/
						recog.base.set_state(372);
						recog.labelNameList()?;

						}
					}

					recog.base.set_state(375);
					recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}

			PromQLParser_WITHOUT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(376);
					recog.base.match_token(PromQLParser_WITHOUT,&mut recog.err_handler)?;

					recog.base.set_state(377);
					recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(379);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if ((((_la - 23)) & !0x3f) == 0 && ((1usize << (_la - 23)) & 134199295) != 0) || _la==PromQLParser_IDENTIFIER {
						{
						/*InvokeRule labelNameList*/
						recog.base.set_state(378);
						recog.labelNameList()?;

						}
					}

					recog.base.set_state(381);
					recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- timeRange ----------------
pub type TimeRangeContextAll<'input> = TimeRangeContext<'input>;


pub type TimeRangeContext<'input> = BaseParserRuleContext<'input,TimeRangeContextExt<'input>>;

#[derive(Clone)]
pub struct TimeRangeContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for TimeRangeContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for TimeRangeContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_timeRange(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_timeRange(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for TimeRangeContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_timeRange(self);
	}
}

impl<'input> CustomRuleContext<'input> for TimeRangeContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_timeRange }
	//fn type_rule_index() -> usize where Self: Sized { RULE_timeRange }
}
antlr4rust::tid!{TimeRangeContextExt<'a>}

impl<'input> TimeRangeContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<TimeRangeContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,TimeRangeContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait TimeRangeContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<TimeRangeContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LBRACK
/// Returns `None` if there is no child corresponding to token LBRACK
fn LBRACK(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LBRACK, 0)
}
fn duration(&self) -> Option<Rc<DurationContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RBRACK
/// Returns `None` if there is no child corresponding to token RBRACK
fn RBRACK(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RBRACK, 0)
}

}

impl<'input> TimeRangeContextAttrs<'input> for TimeRangeContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn timeRange(&mut self,)
	-> Result<Rc<TimeRangeContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = TimeRangeContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 58, RULE_timeRange);
        let mut _localctx: Rc<TimeRangeContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(384);
			recog.base.match_token(PromQLParser_LBRACK,&mut recog.err_handler)?;

			/*InvokeRule duration*/
			recog.base.set_state(385);
			recog.duration()?;

			recog.base.set_state(386);
			recog.base.match_token(PromQLParser_RBRACK,&mut recog.err_handler)?;

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
//------------------- offsetModifier ----------------
pub type OffsetModifierContextAll<'input> = OffsetModifierContext<'input>;


pub type OffsetModifierContext<'input> = BaseParserRuleContext<'input,OffsetModifierContextExt<'input>>;

#[derive(Clone)]
pub struct OffsetModifierContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for OffsetModifierContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for OffsetModifierContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_offsetModifier(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_offsetModifier(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for OffsetModifierContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_offsetModifier(self);
	}
}

impl<'input> CustomRuleContext<'input> for OffsetModifierContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_offsetModifier }
	//fn type_rule_index() -> usize where Self: Sized { RULE_offsetModifier }
}
antlr4rust::tid!{OffsetModifierContextExt<'a>}

impl<'input> OffsetModifierContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<OffsetModifierContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,OffsetModifierContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait OffsetModifierContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<OffsetModifierContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token OFFSET
/// Returns `None` if there is no child corresponding to token OFFSET
fn OFFSET(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_OFFSET, 0)
}
fn duration(&self) -> Option<Rc<DurationContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> OffsetModifierContextAttrs<'input> for OffsetModifierContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn offsetModifier(&mut self,)
	-> Result<Rc<OffsetModifierContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = OffsetModifierContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 60, RULE_offsetModifier);
        let mut _localctx: Rc<OffsetModifierContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(388);
			recog.base.match_token(PromQLParser_OFFSET,&mut recog.err_handler)?;

			/*InvokeRule duration*/
			recog.base.set_state(389);
			recog.duration()?;

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
//------------------- atModifier ----------------
pub type AtModifierContextAll<'input> = AtModifierContext<'input>;


pub type AtModifierContext<'input> = BaseParserRuleContext<'input,AtModifierContextExt<'input>>;

#[derive(Clone)]
pub struct AtModifierContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for AtModifierContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for AtModifierContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_atModifier(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_atModifier(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for AtModifierContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_atModifier(self);
	}
}

impl<'input> CustomRuleContext<'input> for AtModifierContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_atModifier }
	//fn type_rule_index() -> usize where Self: Sized { RULE_atModifier }
}
antlr4rust::tid!{AtModifierContextExt<'a>}

impl<'input> AtModifierContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<AtModifierContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,AtModifierContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait AtModifierContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<AtModifierContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token AT
/// Returns `None` if there is no child corresponding to token AT
fn AT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AT, 0)
}
/// Retrieves first TerminalNode corresponding to token NUMBER
/// Returns `None` if there is no child corresponding to token NUMBER
fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_NUMBER, 0)
}
/// Retrieves first TerminalNode corresponding to token SUB
/// Returns `None` if there is no child corresponding to token SUB
fn SUB(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SUB, 0)
}
/// Retrieves first TerminalNode corresponding to token START
/// Returns `None` if there is no child corresponding to token START
fn START(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_START, 0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_RPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token END
/// Returns `None` if there is no child corresponding to token END
fn END(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_END, 0)
}

}

impl<'input> AtModifierContextAttrs<'input> for AtModifierContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn atModifier(&mut self,)
	-> Result<Rc<AtModifierContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = AtModifierContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 62, RULE_atModifier);
        let mut _localctx: Rc<AtModifierContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(404);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(50,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(391);
					recog.base.match_token(PromQLParser_AT,&mut recog.err_handler)?;

					recog.base.set_state(392);
					recog.base.match_token(PromQLParser_NUMBER,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(393);
					recog.base.match_token(PromQLParser_AT,&mut recog.err_handler)?;

					recog.base.set_state(394);
					recog.base.match_token(PromQLParser_SUB,&mut recog.err_handler)?;

					recog.base.set_state(395);
					recog.base.match_token(PromQLParser_NUMBER,&mut recog.err_handler)?;

					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(396);
					recog.base.match_token(PromQLParser_AT,&mut recog.err_handler)?;

					recog.base.set_state(397);
					recog.base.match_token(PromQLParser_START,&mut recog.err_handler)?;

					recog.base.set_state(398);
					recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(399);
					recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				4 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					recog.base.set_state(400);
					recog.base.match_token(PromQLParser_AT,&mut recog.err_handler)?;

					recog.base.set_state(401);
					recog.base.match_token(PromQLParser_END,&mut recog.err_handler)?;

					recog.base.set_state(402);
					recog.base.match_token(PromQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(403);
					recog.base.match_token(PromQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- labelName ----------------
pub type LabelNameContextAll<'input> = LabelNameContext<'input>;


pub type LabelNameContext<'input> = BaseParserRuleContext<'input,LabelNameContextExt<'input>>;

#[derive(Clone)]
pub struct LabelNameContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for LabelNameContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for LabelNameContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelName(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelName(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for LabelNameContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelName(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelNameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelName }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelName }
}
antlr4rust::tid!{LabelNameContextExt<'a>}

impl<'input> LabelNameContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelNameContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelNameContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelNameContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<LabelNameContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token IDENTIFIER
/// Returns `None` if there is no child corresponding to token IDENTIFIER
fn IDENTIFIER(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_IDENTIFIER, 0)
}
fn keyword(&self) -> Option<Rc<KeywordContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> LabelNameContextAttrs<'input> for LabelNameContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelName(&mut self,)
	-> Result<Rc<LabelNameContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelNameContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 64, RULE_labelName);
        let mut _localctx: Rc<LabelNameContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(408);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			PromQLParser_IDENTIFIER 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(406);
					recog.base.match_token(PromQLParser_IDENTIFIER,&mut recog.err_handler)?;

					}
				}

			PromQLParser_AND |PromQLParser_OR |PromQLParser_UNLESS |PromQLParser_BOOL |
			PromQLParser_BY |PromQLParser_WITHOUT |PromQLParser_ON |PromQLParser_IGNORING |
			PromQLParser_GROUP_LEFT |PromQLParser_GROUP_RIGHT |PromQLParser_OFFSET |
			PromQLParser_START |PromQLParser_END |PromQLParser_AGG_SUM |PromQLParser_AGG_MIN |
			PromQLParser_AGG_MAX |PromQLParser_AGG_AVG |PromQLParser_AGG_GROUP |PromQLParser_AGG_STDDEV |
			PromQLParser_AGG_STDVAR |PromQLParser_AGG_COUNT |PromQLParser_AGG_COUNT_VALUES |
			PromQLParser_AGG_BOTTOMK |PromQLParser_AGG_TOPK |PromQLParser_AGG_QUANTILE 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule keyword*/
					recog.base.set_state(407);
					recog.keyword()?;

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
//------------------- labelNameList ----------------
pub type LabelNameListContextAll<'input> = LabelNameListContext<'input>;


pub type LabelNameListContext<'input> = BaseParserRuleContext<'input,LabelNameListContextExt<'input>>;

#[derive(Clone)]
pub struct LabelNameListContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for LabelNameListContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for LabelNameListContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelNameList(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelNameList(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for LabelNameListContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelNameList(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelNameListContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelNameList }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelNameList }
}
antlr4rust::tid!{LabelNameListContextExt<'a>}

impl<'input> LabelNameListContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelNameListContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelNameListContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelNameListContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<LabelNameListContextExt<'input>>{

fn labelName_all(&self) ->  Vec<Rc<LabelNameContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn labelName(&self, i: usize) -> Option<Rc<LabelNameContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,PromQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_COMMA, i)
}

}

impl<'input> LabelNameListContextAttrs<'input> for LabelNameListContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelNameList(&mut self,)
	-> Result<Rc<LabelNameListContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelNameListContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 66, RULE_labelNameList);
        let mut _localctx: Rc<LabelNameListContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule labelName*/
			recog.base.set_state(410);
			recog.labelName()?;

			recog.base.set_state(415);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			while _la==PromQLParser_COMMA {
				{
				{
				recog.base.set_state(411);
				recog.base.match_token(PromQLParser_COMMA,&mut recog.err_handler)?;

				/*InvokeRule labelName*/
				recog.base.set_state(412);
				recog.labelName()?;

				}
				}
				recog.base.set_state(417);
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
//------------------- keyword ----------------
pub type KeywordContextAll<'input> = KeywordContext<'input>;


pub type KeywordContext<'input> = BaseParserRuleContext<'input,KeywordContextExt<'input>>;

#[derive(Clone)]
pub struct KeywordContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for KeywordContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for KeywordContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_keyword(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_keyword(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for KeywordContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_keyword(self);
	}
}

impl<'input> CustomRuleContext<'input> for KeywordContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_keyword }
	//fn type_rule_index() -> usize where Self: Sized { RULE_keyword }
}
antlr4rust::tid!{KeywordContextExt<'a>}

impl<'input> KeywordContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<KeywordContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,KeywordContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait KeywordContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<KeywordContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token AND
/// Returns `None` if there is no child corresponding to token AND
fn AND(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_AND, 0)
}
/// Retrieves first TerminalNode corresponding to token OR
/// Returns `None` if there is no child corresponding to token OR
fn OR(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_OR, 0)
}
/// Retrieves first TerminalNode corresponding to token UNLESS
/// Returns `None` if there is no child corresponding to token UNLESS
fn UNLESS(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_UNLESS, 0)
}
/// Retrieves first TerminalNode corresponding to token BY
/// Returns `None` if there is no child corresponding to token BY
fn BY(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_BY, 0)
}
/// Retrieves first TerminalNode corresponding to token WITHOUT
/// Returns `None` if there is no child corresponding to token WITHOUT
fn WITHOUT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_WITHOUT, 0)
}
/// Retrieves first TerminalNode corresponding to token ON
/// Returns `None` if there is no child corresponding to token ON
fn ON(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ON, 0)
}
/// Retrieves first TerminalNode corresponding to token IGNORING
/// Returns `None` if there is no child corresponding to token IGNORING
fn IGNORING(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_IGNORING, 0)
}
/// Retrieves first TerminalNode corresponding to token GROUP_LEFT
/// Returns `None` if there is no child corresponding to token GROUP_LEFT
fn GROUP_LEFT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_GROUP_LEFT, 0)
}
/// Retrieves first TerminalNode corresponding to token GROUP_RIGHT
/// Returns `None` if there is no child corresponding to token GROUP_RIGHT
fn GROUP_RIGHT(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_GROUP_RIGHT, 0)
}
/// Retrieves first TerminalNode corresponding to token OFFSET
/// Returns `None` if there is no child corresponding to token OFFSET
fn OFFSET(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_OFFSET, 0)
}
/// Retrieves first TerminalNode corresponding to token BOOL
/// Returns `None` if there is no child corresponding to token BOOL
fn BOOL(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_BOOL, 0)
}
/// Retrieves first TerminalNode corresponding to token START
/// Returns `None` if there is no child corresponding to token START
fn START(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_START, 0)
}
/// Retrieves first TerminalNode corresponding to token END
/// Returns `None` if there is no child corresponding to token END
fn END(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_END, 0)
}
fn aggregationOp(&self) -> Option<Rc<AggregationOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> KeywordContextAttrs<'input> for KeywordContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn keyword(&mut self,)
	-> Result<Rc<KeywordContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = KeywordContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 68, RULE_keyword);
        let mut _localctx: Rc<KeywordContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(432);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			PromQLParser_AND 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(418);
					recog.base.match_token(PromQLParser_AND,&mut recog.err_handler)?;

					}
				}

			PromQLParser_OR 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(419);
					recog.base.match_token(PromQLParser_OR,&mut recog.err_handler)?;

					}
				}

			PromQLParser_UNLESS 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(420);
					recog.base.match_token(PromQLParser_UNLESS,&mut recog.err_handler)?;

					}
				}

			PromQLParser_BY 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					recog.base.set_state(421);
					recog.base.match_token(PromQLParser_BY,&mut recog.err_handler)?;

					}
				}

			PromQLParser_WITHOUT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 5)?;
					recog.base.enter_outer_alt(None, 5)?;
					{
					recog.base.set_state(422);
					recog.base.match_token(PromQLParser_WITHOUT,&mut recog.err_handler)?;

					}
				}

			PromQLParser_ON 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 6)?;
					recog.base.enter_outer_alt(None, 6)?;
					{
					recog.base.set_state(423);
					recog.base.match_token(PromQLParser_ON,&mut recog.err_handler)?;

					}
				}

			PromQLParser_IGNORING 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 7)?;
					recog.base.enter_outer_alt(None, 7)?;
					{
					recog.base.set_state(424);
					recog.base.match_token(PromQLParser_IGNORING,&mut recog.err_handler)?;

					}
				}

			PromQLParser_GROUP_LEFT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 8)?;
					recog.base.enter_outer_alt(None, 8)?;
					{
					recog.base.set_state(425);
					recog.base.match_token(PromQLParser_GROUP_LEFT,&mut recog.err_handler)?;

					}
				}

			PromQLParser_GROUP_RIGHT 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 9)?;
					recog.base.enter_outer_alt(None, 9)?;
					{
					recog.base.set_state(426);
					recog.base.match_token(PromQLParser_GROUP_RIGHT,&mut recog.err_handler)?;

					}
				}

			PromQLParser_OFFSET 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 10)?;
					recog.base.enter_outer_alt(None, 10)?;
					{
					recog.base.set_state(427);
					recog.base.match_token(PromQLParser_OFFSET,&mut recog.err_handler)?;

					}
				}

			PromQLParser_BOOL 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 11)?;
					recog.base.enter_outer_alt(None, 11)?;
					{
					recog.base.set_state(428);
					recog.base.match_token(PromQLParser_BOOL,&mut recog.err_handler)?;

					}
				}

			PromQLParser_START 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 12)?;
					recog.base.enter_outer_alt(None, 12)?;
					{
					recog.base.set_state(429);
					recog.base.match_token(PromQLParser_START,&mut recog.err_handler)?;

					}
				}

			PromQLParser_END 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 13)?;
					recog.base.enter_outer_alt(None, 13)?;
					{
					recog.base.set_state(430);
					recog.base.match_token(PromQLParser_END,&mut recog.err_handler)?;

					}
				}

			PromQLParser_AGG_SUM |PromQLParser_AGG_MIN |PromQLParser_AGG_MAX |PromQLParser_AGG_AVG |
			PromQLParser_AGG_GROUP |PromQLParser_AGG_STDDEV |PromQLParser_AGG_STDVAR |
			PromQLParser_AGG_COUNT |PromQLParser_AGG_COUNT_VALUES |PromQLParser_AGG_BOTTOMK |
			PromQLParser_AGG_TOPK |PromQLParser_AGG_QUANTILE 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 14)?;
					recog.base.enter_outer_alt(None, 14)?;
					{
					/*InvokeRule aggregationOp*/
					recog.base.set_state(431);
					recog.aggregationOp()?;

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
//------------------- numberLiteral ----------------
pub type NumberLiteralContextAll<'input> = NumberLiteralContext<'input>;


pub type NumberLiteralContext<'input> = BaseParserRuleContext<'input,NumberLiteralContextExt<'input>>;

#[derive(Clone)]
pub struct NumberLiteralContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for NumberLiteralContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for NumberLiteralContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_numberLiteral(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_numberLiteral(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for NumberLiteralContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_numberLiteral(self);
	}
}

impl<'input> CustomRuleContext<'input> for NumberLiteralContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_numberLiteral }
	//fn type_rule_index() -> usize where Self: Sized { RULE_numberLiteral }
}
antlr4rust::tid!{NumberLiteralContextExt<'a>}

impl<'input> NumberLiteralContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<NumberLiteralContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,NumberLiteralContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait NumberLiteralContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<NumberLiteralContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token NUMBER
/// Returns `None` if there is no child corresponding to token NUMBER
fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_NUMBER, 0)
}
/// Retrieves first TerminalNode corresponding to token ADD
/// Returns `None` if there is no child corresponding to token ADD
fn ADD(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_ADD, 0)
}
/// Retrieves first TerminalNode corresponding to token SUB
/// Returns `None` if there is no child corresponding to token SUB
fn SUB(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SUB, 0)
}

}

impl<'input> NumberLiteralContextAttrs<'input> for NumberLiteralContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn numberLiteral(&mut self,)
	-> Result<Rc<NumberLiteralContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = NumberLiteralContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 70, RULE_numberLiteral);
        let mut _localctx: Rc<NumberLiteralContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(439);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			PromQLParser_NUMBER 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(434);
					recog.base.match_token(PromQLParser_NUMBER,&mut recog.err_handler)?;

					}
				}

			PromQLParser_ADD 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(435);
					recog.base.match_token(PromQLParser_ADD,&mut recog.err_handler)?;

					recog.base.set_state(436);
					recog.base.match_token(PromQLParser_NUMBER,&mut recog.err_handler)?;

					}
				}

			PromQLParser_SUB 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(437);
					recog.base.match_token(PromQLParser_SUB,&mut recog.err_handler)?;

					recog.base.set_state(438);
					recog.base.match_token(PromQLParser_NUMBER,&mut recog.err_handler)?;

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
//------------------- stringLiteral ----------------
pub type StringLiteralContextAll<'input> = StringLiteralContext<'input>;


pub type StringLiteralContext<'input> = BaseParserRuleContext<'input,StringLiteralContextExt<'input>>;

#[derive(Clone)]
pub struct StringLiteralContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for StringLiteralContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for StringLiteralContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_stringLiteral(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_stringLiteral(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for StringLiteralContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_stringLiteral(self);
	}
}

impl<'input> CustomRuleContext<'input> for StringLiteralContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_stringLiteral }
	//fn type_rule_index() -> usize where Self: Sized { RULE_stringLiteral }
}
antlr4rust::tid!{StringLiteralContextExt<'a>}

impl<'input> StringLiteralContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<StringLiteralContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,StringLiteralContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait StringLiteralContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<StringLiteralContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token STRING
/// Returns `None` if there is no child corresponding to token STRING
fn STRING(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_STRING, 0)
}

}

impl<'input> StringLiteralContextAttrs<'input> for StringLiteralContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn stringLiteral(&mut self,)
	-> Result<Rc<StringLiteralContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = StringLiteralContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 72, RULE_stringLiteral);
        let mut _localctx: Rc<StringLiteralContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(441);
			recog.base.match_token(PromQLParser_STRING,&mut recog.err_handler)?;

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
//------------------- duration ----------------
pub type DurationContextAll<'input> = DurationContext<'input>;


pub type DurationContext<'input> = BaseParserRuleContext<'input,DurationContextExt<'input>>;

#[derive(Clone)]
pub struct DurationContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> PromQLParserContext<'input> for DurationContext<'input>{}

impl<'input,'a> Listenable<dyn PromQLParserListener<'input> + 'a> for DurationContext<'input>{
		fn enter(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_duration(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn PromQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_duration(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn PromQLParserVisitor<'input> + 'a> for DurationContext<'input>{
	fn accept(&self,visitor: &mut (dyn PromQLParserVisitor<'input> + 'a)) {
		visitor.visit_duration(self);
	}
}

impl<'input> CustomRuleContext<'input> for DurationContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = PromQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_duration }
	//fn type_rule_index() -> usize where Self: Sized { RULE_duration }
}
antlr4rust::tid!{DurationContextExt<'a>}

impl<'input> DurationContextExt<'input>{
	fn new(parent: Option<Rc<dyn PromQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<DurationContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,DurationContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait DurationContextAttrs<'input>: PromQLParserContext<'input> + BorrowMut<DurationContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token DURATION
/// Returns `None` if there is no child corresponding to token DURATION
fn DURATION(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_DURATION, 0)
}
/// Retrieves first TerminalNode corresponding to token SUB
/// Returns `None` if there is no child corresponding to token SUB
fn SUB(&self) -> Option<Rc<TerminalNode<'input,PromQLParserContextType>>> where Self:Sized{
	self.get_token(PromQLParser_SUB, 0)
}

}

impl<'input> DurationContextAttrs<'input> for DurationContext<'input>{}

impl<'input, I> PromQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn duration(&mut self,)
	-> Result<Rc<DurationContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = DurationContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 74, RULE_duration);
        let mut _localctx: Rc<DurationContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(444);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==PromQLParser_SUB {
				{
				recog.base.set_state(443);
				recog.base.match_token(PromQLParser_SUB,&mut recog.err_handler)?;

				}
			}

			recog.base.set_state(446);
			recog.base.match_token(PromQLParser_DURATION,&mut recog.err_handler)?;

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
		4, 1, 132, 449, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 
		7, 4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 
		7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7, 14, 2, 15, 
		7, 15, 2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7, 19, 2, 20, 
		7, 20, 2, 21, 7, 21, 2, 22, 7, 22, 2, 23, 7, 23, 2, 24, 7, 24, 2, 25, 
		7, 25, 2, 26, 7, 26, 2, 27, 7, 27, 2, 28, 7, 28, 2, 29, 7, 29, 2, 30, 
		7, 30, 2, 31, 7, 31, 2, 32, 7, 32, 2, 33, 7, 33, 2, 34, 7, 34, 2, 35, 
		7, 35, 2, 36, 7, 36, 2, 37, 7, 37, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 
		3, 1, 83, 8, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
		1, 1, 1, 1, 3, 1, 120, 8, 1, 1, 1, 1, 1, 3, 1, 124, 8, 1, 1, 1, 3, 1, 
		127, 8, 1, 5, 1, 129, 8, 1, 10, 1, 12, 1, 132, 9, 1, 1, 2, 1, 2, 1, 2, 
		1, 2, 1, 2, 1, 2, 1, 2, 3, 2, 141, 8, 2, 1, 3, 1, 3, 1, 3, 1, 3, 1, 4, 
		1, 4, 1, 4, 1, 5, 1, 5, 1, 6, 1, 6, 1, 7, 1, 7, 1, 8, 1, 8, 1, 9, 1, 9, 
		1, 10, 3, 10, 161, 8, 10, 1, 10, 1, 10, 3, 10, 165, 8, 10, 3, 10, 167, 
		8, 10, 1, 11, 1, 11, 1, 11, 3, 11, 172, 8, 11, 1, 11, 1, 11, 1, 11, 1, 
		11, 3, 11, 178, 8, 11, 1, 11, 3, 11, 181, 8, 11, 1, 12, 1, 12, 1, 12, 
		3, 12, 186, 8, 12, 1, 12, 3, 12, 189, 8, 12, 1, 12, 1, 12, 1, 12, 3, 12, 
		194, 8, 12, 1, 12, 3, 12, 197, 8, 12, 3, 12, 199, 8, 12, 1, 13, 1, 13, 
		1, 13, 1, 13, 1, 13, 3, 13, 206, 8, 13, 1, 13, 3, 13, 209, 8, 13, 1, 13, 
		1, 13, 1, 13, 1, 13, 3, 13, 215, 8, 13, 1, 13, 3, 13, 218, 8, 13, 1, 13, 
		1, 13, 3, 13, 222, 8, 13, 1, 13, 3, 13, 225, 8, 13, 1, 13, 1, 13, 1, 13, 
		1, 13, 3, 13, 231, 8, 13, 1, 13, 3, 13, 234, 8, 13, 3, 13, 236, 8, 13, 
		1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 3, 14, 244, 8, 14, 1, 14, 3, 
		14, 247, 8, 14, 1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 3, 14, 254, 8, 14, 
		1, 14, 3, 14, 257, 8, 14, 1, 14, 1, 14, 1, 14, 3, 14, 262, 8, 14, 1, 14, 
		3, 14, 265, 8, 14, 1, 14, 1, 14, 1, 14, 1, 14, 1, 14, 3, 14, 272, 8, 14, 
		1, 14, 3, 14, 275, 8, 14, 3, 14, 277, 8, 14, 1, 15, 1, 15, 1, 15, 5, 15, 
		282, 8, 15, 10, 15, 12, 15, 285, 9, 15, 1, 15, 3, 15, 288, 8, 15, 1, 16, 
		1, 16, 1, 16, 1, 16, 1, 17, 1, 17, 1, 18, 1, 18, 1, 18, 1, 18, 1, 18, 
		5, 18, 301, 8, 18, 10, 18, 12, 18, 304, 9, 18, 3, 18, 306, 8, 18, 1, 18, 
		1, 18, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 1, 19, 
		1, 19, 1, 19, 1, 19, 3, 19, 322, 8, 19, 1, 20, 1, 20, 1, 21, 1, 21, 1, 
		22, 1, 22, 1, 23, 1, 23, 1, 24, 1, 24, 1, 25, 1, 25, 1, 26, 1, 26, 1, 
		26, 1, 26, 1, 26, 5, 26, 341, 8, 26, 10, 26, 12, 26, 344, 9, 26, 3, 26, 
		346, 8, 26, 1, 26, 1, 26, 3, 26, 350, 8, 26, 1, 26, 1, 26, 1, 26, 1, 26, 
		1, 26, 1, 26, 5, 26, 358, 8, 26, 10, 26, 12, 26, 361, 9, 26, 3, 26, 363, 
		8, 26, 1, 26, 1, 26, 3, 26, 367, 8, 26, 1, 27, 1, 27, 1, 28, 1, 28, 1, 
		28, 3, 28, 374, 8, 28, 1, 28, 1, 28, 1, 28, 1, 28, 3, 28, 380, 8, 28, 
		1, 28, 3, 28, 383, 8, 28, 1, 29, 1, 29, 1, 29, 1, 29, 1, 30, 1, 30, 1, 
		30, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 
		31, 1, 31, 1, 31, 1, 31, 3, 31, 405, 8, 31, 1, 32, 1, 32, 3, 32, 409, 
		8, 32, 1, 33, 1, 33, 1, 33, 5, 33, 414, 8, 33, 10, 33, 12, 33, 417, 9, 
		33, 1, 34, 1, 34, 1, 34, 1, 34, 1, 34, 1, 34, 1, 34, 1, 34, 1, 34, 1, 
		34, 1, 34, 1, 34, 1, 34, 1, 34, 3, 34, 433, 8, 34, 1, 35, 1, 35, 1, 35, 
		1, 35, 1, 35, 3, 35, 440, 8, 35, 1, 36, 1, 36, 1, 37, 3, 37, 445, 8, 37, 
		1, 37, 1, 37, 1, 37, 0, 1, 2, 38, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 
		22, 24, 26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 
		58, 60, 62, 64, 66, 68, 70, 72, 74, 0, 12, 1, 0, 9, 10, 2, 0, 11, 12, 
		37, 37, 2, 0, 15, 15, 18, 22, 2, 0, 23, 23, 25, 25, 1, 0, 14, 17, 1, 0, 
		50, 70, 4, 0, 71, 71, 73, 82, 84, 84, 87, 87, 1, 0, 88, 101, 1, 0, 103, 
		104, 1, 0, 105, 114, 1, 0, 115, 118, 1, 0, 38, 49, 505, 0, 76, 1, 0, 0, 
		0, 2, 82, 1, 0, 0, 0, 4, 140, 1, 0, 0, 0, 6, 142, 1, 0, 0, 0, 8, 146, 
		1, 0, 0, 0, 10, 149, 1, 0, 0, 0, 12, 151, 1, 0, 0, 0, 14, 153, 1, 0, 0, 
		0, 16, 155, 1, 0, 0, 0, 18, 157, 1, 0, 0, 0, 20, 160, 1, 0, 0, 0, 22, 
		180, 1, 0, 0, 0, 24, 198, 1, 0, 0, 0, 26, 235, 1, 0, 0, 0, 28, 276, 1, 
		0, 0, 0, 30, 278, 1, 0, 0, 0, 32, 289, 1, 0, 0, 0, 34, 293, 1, 0, 0, 0, 
		36, 295, 1, 0, 0, 0, 38, 321, 1, 0, 0, 0, 40, 323, 1, 0, 0, 0, 42, 325, 
		1, 0, 0, 0, 44, 327, 1, 0, 0, 0, 46, 329, 1, 0, 0, 0, 48, 331, 1, 0, 0, 
		0, 50, 333, 1, 0, 0, 0, 52, 366, 1, 0, 0, 0, 54, 368, 1, 0, 0, 0, 56, 
		382, 1, 0, 0, 0, 58, 384, 1, 0, 0, 0, 60, 388, 1, 0, 0, 0, 62, 404, 1, 
		0, 0, 0, 64, 408, 1, 0, 0, 0, 66, 410, 1, 0, 0, 0, 68, 432, 1, 0, 0, 0, 
		70, 439, 1, 0, 0, 0, 72, 441, 1, 0, 0, 0, 74, 444, 1, 0, 0, 0, 76, 77, 
		3, 2, 1, 0, 77, 78, 5, 0, 0, 1, 78, 1, 1, 0, 0, 0, 79, 80, 6, 1, -1, 0, 
		80, 83, 3, 8, 4, 0, 81, 83, 3, 4, 2, 0, 82, 79, 1, 0, 0, 0, 82, 81, 1, 
		0, 0, 0, 83, 130, 1, 0, 0, 0, 84, 85, 10, 9, 0, 0, 85, 86, 5, 13, 0, 0, 
		86, 87, 3, 20, 10, 0, 87, 88, 3, 2, 1, 9, 88, 129, 1, 0, 0, 0, 89, 90, 
		10, 8, 0, 0, 90, 91, 3, 12, 6, 0, 91, 92, 3, 20, 10, 0, 92, 93, 3, 2, 
		1, 9, 93, 129, 1, 0, 0, 0, 94, 95, 10, 7, 0, 0, 95, 96, 3, 14, 7, 0, 96, 
		97, 3, 20, 10, 0, 97, 98, 3, 2, 1, 8, 98, 129, 1, 0, 0, 0, 99, 100, 10, 
		6, 0, 0, 100, 101, 3, 16, 8, 0, 101, 102, 3, 20, 10, 0, 102, 103, 3, 2, 
		1, 7, 103, 129, 1, 0, 0, 0, 104, 105, 10, 5, 0, 0, 105, 106, 3, 18, 9, 
		0, 106, 107, 3, 20, 10, 0, 107, 108, 3, 2, 1, 6, 108, 129, 1, 0, 0, 0, 
		109, 110, 10, 4, 0, 0, 110, 111, 5, 24, 0, 0, 111, 112, 3, 20, 10, 0, 
		112, 113, 3, 2, 1, 5, 113, 129, 1, 0, 0, 0, 114, 115, 10, 3, 0, 0, 115, 
		116, 5, 5, 0, 0, 116, 117, 3, 74, 37, 0, 117, 119, 5, 7, 0, 0, 118, 120, 
		3, 74, 37, 0, 119, 118, 1, 0, 0, 0, 119, 120, 1, 0, 0, 0, 120, 121, 1, 
		0, 0, 0, 121, 123, 5, 6, 0, 0, 122, 124, 3, 60, 30, 0, 123, 122, 1, 0, 
		0, 0, 123, 124, 1, 0, 0, 0, 124, 126, 1, 0, 0, 0, 125, 127, 3, 62, 31, 
		0, 126, 125, 1, 0, 0, 0, 126, 127, 1, 0, 0, 0, 127, 129, 1, 0, 0, 0, 128, 
		84, 1, 0, 0, 0, 128, 89, 1, 0, 0, 0, 128, 94, 1, 0, 0, 0, 128, 99, 1, 
		0, 0, 0, 128, 104, 1, 0, 0, 0, 128, 109, 1, 0, 0, 0, 128, 114, 1, 0, 0, 
		0, 129, 132, 1, 0, 0, 0, 130, 128, 1, 0, 0, 0, 130, 131, 1, 0, 0, 0, 131, 
		3, 1, 0, 0, 0, 132, 130, 1, 0, 0, 0, 133, 141, 3, 36, 18, 0, 134, 141, 
		3, 52, 26, 0, 135, 141, 3, 28, 14, 0, 136, 141, 3, 26, 13, 0, 137, 141, 
		3, 70, 35, 0, 138, 141, 3, 72, 36, 0, 139, 141, 3, 6, 3, 0, 140, 133, 
		1, 0, 0, 0, 140, 134, 1, 0, 0, 0, 140, 135, 1, 0, 0, 0, 140, 136, 1, 0, 
		0, 0, 140, 137, 1, 0, 0, 0, 140, 138, 1, 0, 0, 0, 140, 139, 1, 0, 0, 0, 
		141, 5, 1, 0, 0, 0, 142, 143, 5, 1, 0, 0, 143, 144, 3, 2, 1, 0, 144, 145, 
		5, 2, 0, 0, 145, 7, 1, 0, 0, 0, 146, 147, 3, 10, 5, 0, 147, 148, 3, 2, 
		1, 0, 148, 9, 1, 0, 0, 0, 149, 150, 7, 0, 0, 0, 150, 11, 1, 0, 0, 0, 151, 
		152, 7, 1, 0, 0, 152, 13, 1, 0, 0, 0, 153, 154, 7, 0, 0, 0, 154, 15, 1, 
		0, 0, 0, 155, 156, 7, 2, 0, 0, 156, 17, 1, 0, 0, 0, 157, 158, 7, 3, 0, 
		0, 158, 19, 1, 0, 0, 0, 159, 161, 5, 26, 0, 0, 160, 159, 1, 0, 0, 0, 160, 
		161, 1, 0, 0, 0, 161, 166, 1, 0, 0, 0, 162, 164, 3, 22, 11, 0, 163, 165, 
		3, 24, 12, 0, 164, 163, 1, 0, 0, 0, 164, 165, 1, 0, 0, 0, 165, 167, 1, 
		0, 0, 0, 166, 162, 1, 0, 0, 0, 166, 167, 1, 0, 0, 0, 167, 21, 1, 0, 0, 
		0, 168, 169, 5, 29, 0, 0, 169, 171, 5, 1, 0, 0, 170, 172, 3, 66, 33, 0, 
		171, 170, 1, 0, 0, 0, 171, 172, 1, 0, 0, 0, 172, 173, 1, 0, 0, 0, 173, 
		181, 5, 2, 0, 0, 174, 175, 5, 30, 0, 0, 175, 177, 5, 1, 0, 0, 176, 178, 
		3, 66, 33, 0, 177, 176, 1, 0, 0, 0, 177, 178, 1, 0, 0, 0, 178, 179, 1, 
		0, 0, 0, 179, 181, 5, 2, 0, 0, 180, 168, 1, 0, 0, 0, 180, 174, 1, 0, 0, 
		0, 181, 23, 1, 0, 0, 0, 182, 188, 5, 31, 0, 0, 183, 185, 5, 1, 0, 0, 184, 
		186, 3, 66, 33, 0, 185, 184, 1, 0, 0, 0, 185, 186, 1, 0, 0, 0, 186, 187, 
		1, 0, 0, 0, 187, 189, 5, 2, 0, 0, 188, 183, 1, 0, 0, 0, 188, 189, 1, 0, 
		0, 0, 189, 199, 1, 0, 0, 0, 190, 196, 5, 32, 0, 0, 191, 193, 5, 1, 0, 
		0, 192, 194, 3, 66, 33, 0, 193, 192, 1, 0, 0, 0, 193, 194, 1, 0, 0, 0, 
		194, 195, 1, 0, 0, 0, 195, 197, 5, 2, 0, 0, 196, 191, 1, 0, 0, 0, 196, 
		197, 1, 0, 0, 0, 197, 199, 1, 0, 0, 0, 198, 182, 1, 0, 0, 0, 198, 190, 
		1, 0, 0, 0, 199, 25, 1, 0, 0, 0, 200, 201, 5, 130, 0, 0, 201, 202, 5, 
		3, 0, 0, 202, 203, 3, 30, 15, 0, 203, 205, 5, 4, 0, 0, 204, 206, 3, 60, 
		30, 0, 205, 204, 1, 0, 0, 0, 205, 206, 1, 0, 0, 0, 206, 208, 1, 0, 0, 
		0, 207, 209, 3, 62, 31, 0, 208, 207, 1, 0, 0, 0, 208, 209, 1, 0, 0, 0, 
		209, 236, 1, 0, 0, 0, 210, 211, 5, 130, 0, 0, 211, 212, 5, 3, 0, 0, 212, 
		214, 5, 4, 0, 0, 213, 215, 3, 60, 30, 0, 214, 213, 1, 0, 0, 0, 214, 215, 
		1, 0, 0, 0, 215, 217, 1, 0, 0, 0, 216, 218, 3, 62, 31, 0, 217, 216, 1, 
		0, 0, 0, 217, 218, 1, 0, 0, 0, 218, 236, 1, 0, 0, 0, 219, 221, 5, 130, 
		0, 0, 220, 222, 3, 60, 30, 0, 221, 220, 1, 0, 0, 0, 221, 222, 1, 0, 0, 
		0, 222, 224, 1, 0, 0, 0, 223, 225, 3, 62, 31, 0, 224, 223, 1, 0, 0, 0, 
		224, 225, 1, 0, 0, 0, 225, 236, 1, 0, 0, 0, 226, 227, 5, 3, 0, 0, 227, 
		228, 3, 30, 15, 0, 228, 230, 5, 4, 0, 0, 229, 231, 3, 60, 30, 0, 230, 
		229, 1, 0, 0, 0, 230, 231, 1, 0, 0, 0, 231, 233, 1, 0, 0, 0, 232, 234, 
		3, 62, 31, 0, 233, 232, 1, 0, 0, 0, 233, 234, 1, 0, 0, 0, 234, 236, 1, 
		0, 0, 0, 235, 200, 1, 0, 0, 0, 235, 210, 1, 0, 0, 0, 235, 219, 1, 0, 0, 
		0, 235, 226, 1, 0, 0, 0, 236, 27, 1, 0, 0, 0, 237, 238, 5, 130, 0, 0, 
		238, 239, 5, 3, 0, 0, 239, 240, 3, 30, 15, 0, 240, 241, 5, 4, 0, 0, 241, 
		243, 3, 58, 29, 0, 242, 244, 3, 60, 30, 0, 243, 242, 1, 0, 0, 0, 243, 
		244, 1, 0, 0, 0, 244, 246, 1, 0, 0, 0, 245, 247, 3, 62, 31, 0, 246, 245, 
		1, 0, 0, 0, 246, 247, 1, 0, 0, 0, 247, 277, 1, 0, 0, 0, 248, 249, 5, 130, 
		0, 0, 249, 250, 5, 3, 0, 0, 250, 251, 5, 4, 0, 0, 251, 253, 3, 58, 29, 
		0, 252, 254, 3, 60, 30, 0, 253, 252, 1, 0, 0, 0, 253, 254, 1, 0, 0, 0, 
		254, 256, 1, 0, 0, 0, 255, 257, 3, 62, 31, 0, 256, 255, 1, 0, 0, 0, 256, 
		257, 1, 0, 0, 0, 257, 277, 1, 0, 0, 0, 258, 259, 5, 130, 0, 0, 259, 261, 
		3, 58, 29, 0, 260, 262, 3, 60, 30, 0, 261, 260, 1, 0, 0, 0, 261, 262, 
		1, 0, 0, 0, 262, 264, 1, 0, 0, 0, 263, 265, 3, 62, 31, 0, 264, 263, 1, 
		0, 0, 0, 264, 265, 1, 0, 0, 0, 265, 277, 1, 0, 0, 0, 266, 267, 5, 3, 0, 
		0, 267, 268, 3, 30, 15, 0, 268, 269, 5, 4, 0, 0, 269, 271, 3, 58, 29, 
		0, 270, 272, 3, 60, 30, 0, 271, 270, 1, 0, 0, 0, 271, 272, 1, 0, 0, 0, 
		272, 274, 1, 0, 0, 0, 273, 275, 3, 62, 31, 0, 274, 273, 1, 0, 0, 0, 274, 
		275, 1, 0, 0, 0, 275, 277, 1, 0, 0, 0, 276, 237, 1, 0, 0, 0, 276, 248, 
		1, 0, 0, 0, 276, 258, 1, 0, 0, 0, 276, 266, 1, 0, 0, 0, 277, 29, 1, 0, 
		0, 0, 278, 283, 3, 32, 16, 0, 279, 280, 5, 8, 0, 0, 280, 282, 3, 32, 16, 
		0, 281, 279, 1, 0, 0, 0, 282, 285, 1, 0, 0, 0, 283, 281, 1, 0, 0, 0, 283, 
		284, 1, 0, 0, 0, 284, 287, 1, 0, 0, 0, 285, 283, 1, 0, 0, 0, 286, 288, 
		5, 8, 0, 0, 287, 286, 1, 0, 0, 0, 287, 288, 1, 0, 0, 0, 288, 31, 1, 0, 
		0, 0, 289, 290, 3, 64, 32, 0, 290, 291, 3, 34, 17, 0, 291, 292, 5, 129, 
		0, 0, 292, 33, 1, 0, 0, 0, 293, 294, 7, 4, 0, 0, 294, 35, 1, 0, 0, 0, 
		295, 296, 3, 38, 19, 0, 296, 305, 5, 1, 0, 0, 297, 302, 3, 2, 1, 0, 298, 
		299, 5, 8, 0, 0, 299, 301, 3, 2, 1, 0, 300, 298, 1, 0, 0, 0, 301, 304, 
		1, 0, 0, 0, 302, 300, 1, 0, 0, 0, 302, 303, 1, 0, 0, 0, 303, 306, 1, 0, 
		0, 0, 304, 302, 1, 0, 0, 0, 305, 297, 1, 0, 0, 0, 305, 306, 1, 0, 0, 0, 
		306, 307, 1, 0, 0, 0, 307, 308, 5, 2, 0, 0, 308, 37, 1, 0, 0, 0, 309, 
		322, 3, 40, 20, 0, 310, 322, 3, 42, 21, 0, 311, 322, 3, 44, 22, 0, 312, 
		322, 3, 46, 23, 0, 313, 322, 3, 48, 24, 0, 314, 322, 3, 50, 25, 0, 315, 
		322, 5, 72, 0, 0, 316, 322, 5, 83, 0, 0, 317, 322, 5, 119, 0, 0, 318, 
		322, 5, 85, 0, 0, 319, 322, 5, 86, 0, 0, 320, 322, 5, 102, 0, 0, 321, 
		309, 1, 0, 0, 0, 321, 310, 1, 0, 0, 0, 321, 311, 1, 0, 0, 0, 321, 312, 
		1, 0, 0, 0, 321, 313, 1, 0, 0, 0, 321, 314, 1, 0, 0, 0, 321, 315, 1, 0, 
		0, 0, 321, 316, 1, 0, 0, 0, 321, 317, 1, 0, 0, 0, 321, 318, 1, 0, 0, 0, 
		321, 319, 1, 0, 0, 0, 321, 320, 1, 0, 0, 0, 322, 39, 1, 0, 0, 0, 323, 
		324, 7, 5, 0, 0, 324, 41, 1, 0, 0, 0, 325, 326, 7, 6, 0, 0, 326, 43, 1, 
		0, 0, 0, 327, 328, 7, 7, 0, 0, 328, 45, 1, 0, 0, 0, 329, 330, 7, 8, 0, 
		0, 330, 47, 1, 0, 0, 0, 331, 332, 7, 9, 0, 0, 332, 49, 1, 0, 0, 0, 333, 
		334, 7, 10, 0, 0, 334, 51, 1, 0, 0, 0, 335, 336, 3, 54, 27, 0, 336, 345, 
		5, 1, 0, 0, 337, 342, 3, 2, 1, 0, 338, 339, 5, 8, 0, 0, 339, 341, 3, 2, 
		1, 0, 340, 338, 1, 0, 0, 0, 341, 344, 1, 0, 0, 0, 342, 340, 1, 0, 0, 0, 
		342, 343, 1, 0, 0, 0, 343, 346, 1, 0, 0, 0, 344, 342, 1, 0, 0, 0, 345, 
		337, 1, 0, 0, 0, 345, 346, 1, 0, 0, 0, 346, 347, 1, 0, 0, 0, 347, 349, 
		5, 2, 0, 0, 348, 350, 3, 56, 28, 0, 349, 348, 1, 0, 0, 0, 349, 350, 1, 
		0, 0, 0, 350, 367, 1, 0, 0, 0, 351, 352, 3, 54, 27, 0, 352, 353, 3, 56, 
		28, 0, 353, 362, 5, 1, 0, 0, 354, 359, 3, 2, 1, 0, 355, 356, 5, 8, 0, 
		0, 356, 358, 3, 2, 1, 0, 357, 355, 1, 0, 0, 0, 358, 361, 1, 0, 0, 0, 359, 
		357, 1, 0, 0, 0, 359, 360, 1, 0, 0, 0, 360, 363, 1, 0, 0, 0, 361, 359, 
		1, 0, 0, 0, 362, 354, 1, 0, 0, 0, 362, 363, 1, 0, 0, 0, 363, 364, 1, 0, 
		0, 0, 364, 365, 5, 2, 0, 0, 365, 367, 1, 0, 0, 0, 366, 335, 1, 0, 0, 0, 
		366, 351, 1, 0, 0, 0, 367, 53, 1, 0, 0, 0, 368, 369, 7, 11, 0, 0, 369, 
		55, 1, 0, 0, 0, 370, 371, 5, 27, 0, 0, 371, 373, 5, 1, 0, 0, 372, 374, 
		3, 66, 33, 0, 373, 372, 1, 0, 0, 0, 373, 374, 1, 0, 0, 0, 374, 375, 1, 
		0, 0, 0, 375, 383, 5, 2, 0, 0, 376, 377, 5, 28, 0, 0, 377, 379, 5, 1, 
		0, 0, 378, 380, 3, 66, 33, 0, 379, 378, 1, 0, 0, 0, 379, 380, 1, 0, 0, 
		0, 380, 381, 1, 0, 0, 0, 381, 383, 5, 2, 0, 0, 382, 370, 1, 0, 0, 0, 382, 
		376, 1, 0, 0, 0, 383, 57, 1, 0, 0, 0, 384, 385, 5, 5, 0, 0, 385, 386, 
		3, 74, 37, 0, 386, 387, 5, 6, 0, 0, 387, 59, 1, 0, 0, 0, 388, 389, 5, 
		33, 0, 0, 389, 390, 3, 74, 37, 0, 390, 61, 1, 0, 0, 0, 391, 392, 5, 34, 
		0, 0, 392, 405, 5, 127, 0, 0, 393, 394, 5, 34, 0, 0, 394, 395, 5, 10, 
		0, 0, 395, 405, 5, 127, 0, 0, 396, 397, 5, 34, 0, 0, 397, 398, 5, 35, 
		0, 0, 398, 399, 5, 1, 0, 0, 399, 405, 5, 2, 0, 0, 400, 401, 5, 34, 0, 
		0, 401, 402, 5, 36, 0, 0, 402, 403, 5, 1, 0, 0, 403, 405, 5, 2, 0, 0, 
		404, 391, 1, 0, 0, 0, 404, 393, 1, 0, 0, 0, 404, 396, 1, 0, 0, 0, 404, 
		400, 1, 0, 0, 0, 405, 63, 1, 0, 0, 0, 406, 409, 5, 130, 0, 0, 407, 409, 
		3, 68, 34, 0, 408, 406, 1, 0, 0, 0, 408, 407, 1, 0, 0, 0, 409, 65, 1, 
		0, 0, 0, 410, 415, 3, 64, 32, 0, 411, 412, 5, 8, 0, 0, 412, 414, 3, 64, 
		32, 0, 413, 411, 1, 0, 0, 0, 414, 417, 1, 0, 0, 0, 415, 413, 1, 0, 0, 
		0, 415, 416, 1, 0, 0, 0, 416, 67, 1, 0, 0, 0, 417, 415, 1, 0, 0, 0, 418, 
		433, 5, 23, 0, 0, 419, 433, 5, 24, 0, 0, 420, 433, 5, 25, 0, 0, 421, 433, 
		5, 27, 0, 0, 422, 433, 5, 28, 0, 0, 423, 433, 5, 29, 0, 0, 424, 433, 5, 
		30, 0, 0, 425, 433, 5, 31, 0, 0, 426, 433, 5, 32, 0, 0, 427, 433, 5, 33, 
		0, 0, 428, 433, 5, 26, 0, 0, 429, 433, 5, 35, 0, 0, 430, 433, 5, 36, 0, 
		0, 431, 433, 3, 54, 27, 0, 432, 418, 1, 0, 0, 0, 432, 419, 1, 0, 0, 0, 
		432, 420, 1, 0, 0, 0, 432, 421, 1, 0, 0, 0, 432, 422, 1, 0, 0, 0, 432, 
		423, 1, 0, 0, 0, 432, 424, 1, 0, 0, 0, 432, 425, 1, 0, 0, 0, 432, 426, 
		1, 0, 0, 0, 432, 427, 1, 0, 0, 0, 432, 428, 1, 0, 0, 0, 432, 429, 1, 0, 
		0, 0, 432, 430, 1, 0, 0, 0, 432, 431, 1, 0, 0, 0, 433, 69, 1, 0, 0, 0, 
		434, 440, 5, 127, 0, 0, 435, 436, 5, 9, 0, 0, 436, 440, 5, 127, 0, 0, 
		437, 438, 5, 10, 0, 0, 438, 440, 5, 127, 0, 0, 439, 434, 1, 0, 0, 0, 439, 
		435, 1, 0, 0, 0, 439, 437, 1, 0, 0, 0, 440, 71, 1, 0, 0, 0, 441, 442, 
		5, 129, 0, 0, 442, 73, 1, 0, 0, 0, 443, 445, 5, 10, 0, 0, 444, 443, 1, 
		0, 0, 0, 444, 445, 1, 0, 0, 0, 445, 446, 1, 0, 0, 0, 446, 447, 5, 128, 
		0, 0, 447, 75, 1, 0, 0, 0, 56, 82, 119, 123, 126, 128, 130, 140, 160, 
		164, 166, 171, 177, 180, 185, 188, 193, 196, 198, 205, 208, 214, 217, 
		221, 224, 230, 233, 235, 243, 246, 253, 256, 261, 264, 271, 274, 276, 
		283, 287, 302, 305, 321, 342, 345, 349, 359, 362, 366, 373, 379, 382, 
		404, 408, 415, 432, 439, 444
	];
}
