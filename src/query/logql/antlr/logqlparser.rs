#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used)]
// Generated from antlr/LogQLParser.g4 by ANTLR 4.13.2
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
use super::logqlparserlistener::*;
use super::logqlparservisitor::*;

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

		pub const LogQLParser_LPAREN:i32=1; 
		pub const LogQLParser_RPAREN:i32=2; 
		pub const LogQLParser_LBRACE:i32=3; 
		pub const LogQLParser_RBRACE:i32=4; 
		pub const LogQLParser_LBRACK:i32=5; 
		pub const LogQLParser_RBRACK:i32=6; 
		pub const LogQLParser_COLON:i32=7; 
		pub const LogQLParser_COMMA:i32=8; 
		pub const LogQLParser_SEMI:i32=9; 
		pub const LogQLParser_PIPE:i32=10; 
		pub const LogQLParser_DOT:i32=11; 
		pub const LogQLParser_ADD:i32=12; 
		pub const LogQLParser_SUB:i32=13; 
		pub const LogQLParser_MUL:i32=14; 
		pub const LogQLParser_DIV:i32=15; 
		pub const LogQLParser_POW:i32=16; 
		pub const LogQLParser_EQ:i32=17; 
		pub const LogQLParser_NE:i32=18; 
		pub const LogQLParser_RE:i32=19; 
		pub const LogQLParser_NRE:i32=20; 
		pub const LogQLParser_GT:i32=21; 
		pub const LogQLParser_LT:i32=22; 
		pub const LogQLParser_GE:i32=23; 
		pub const LogQLParser_LE:i32=24; 
		pub const LogQLParser_EQL:i32=25; 
		pub const LogQLParser_PIPE_CONTAINS:i32=26; 
		pub const LogQLParser_PIPE_MATCH:i32=27; 
		pub const LogQLParser_PIPE_PATTERN:i32=28; 
		pub const LogQLParser_PIPE_NPATTERN:i32=29; 
		pub const LogQLParser_AND:i32=30; 
		pub const LogQLParser_OR:i32=31; 
		pub const LogQLParser_UNLESS:i32=32; 
		pub const LogQLParser_BOOL:i32=33; 
		pub const LogQLParser_BY:i32=34; 
		pub const LogQLParser_WITHOUT:i32=35; 
		pub const LogQLParser_KEEP:i32=36; 
		pub const LogQLParser_DROP:i32=37; 
		pub const LogQLParser_DECOLORIZE:i32=38; 
		pub const LogQLParser_LABEL_REPLACE:i32=39; 
		pub const LogQLParser_SUM:i32=40; 
		pub const LogQLParser_AVG:i32=41; 
		pub const LogQLParser_COUNT:i32=42; 
		pub const LogQLParser_MAX:i32=43; 
		pub const LogQLParser_MIN:i32=44; 
		pub const LogQLParser_STDDEV:i32=45; 
		pub const LogQLParser_STDVAR:i32=46; 
		pub const LogQLParser_TOPK:i32=47; 
		pub const LogQLParser_BOTTOMK:i32=48; 
		pub const LogQLParser_APPROX_TOPK:i32=49; 
		pub const LogQLParser_JSON:i32=50; 
		pub const LogQLParser_LOGFMT:i32=51; 
		pub const LogQLParser_UNPACK:i32=52; 
		pub const LogQLParser_PATTERN:i32=53; 
		pub const LogQLParser_REGEXP:i32=54; 
		pub const LogQLParser_LINE_FORMAT:i32=55; 
		pub const LogQLParser_LABEL_FORMAT:i32=56; 
		pub const LogQLParser_VECTOR:i32=57; 
		pub const LogQLParser_OFFSET:i32=58; 
		pub const LogQLParser_ON:i32=59; 
		pub const LogQLParser_IGNORING:i32=60; 
		pub const LogQLParser_GROUP_LEFT:i32=61; 
		pub const LogQLParser_GROUP_RIGHT:i32=62; 
		pub const LogQLParser_UNWRAP:i32=63; 
		pub const LogQLParser_SORT:i32=64; 
		pub const LogQLParser_SORT_DESC:i32=65; 
		pub const LogQLParser_AT:i32=66; 
		pub const LogQLParser_COUNT_OVER_TIME:i32=67; 
		pub const LogQLParser_RATE:i32=68; 
		pub const LogQLParser_RATE_COUNTER:i32=69; 
		pub const LogQLParser_BYTES_OVER_TIME:i32=70; 
		pub const LogQLParser_BYTES_RATE:i32=71; 
		pub const LogQLParser_AVG_OVER_TIME:i32=72; 
		pub const LogQLParser_SUM_OVER_TIME:i32=73; 
		pub const LogQLParser_MIN_OVER_TIME:i32=74; 
		pub const LogQLParser_MAX_OVER_TIME:i32=75; 
		pub const LogQLParser_STDDEV_OVER_TIME:i32=76; 
		pub const LogQLParser_STDVAR_OVER_TIME:i32=77; 
		pub const LogQLParser_QUANTILE_OVER_TIME:i32=78; 
		pub const LogQLParser_FIRST_OVER_TIME:i32=79; 
		pub const LogQLParser_LAST_OVER_TIME:i32=80; 
		pub const LogQLParser_ABSENT_OVER_TIME:i32=81; 
		pub const LogQLParser_MOD:i32=82; 
		pub const LogQLParser_LOGFMT_FLAG:i32=83; 
		pub const LogQLParser_DURATION_TYPE_MILLISECOND:i32=84; 
		pub const LogQLParser_DURATION_TYPE_MICROSECOND:i32=85; 
		pub const LogQLParser_DURATION_TYPE_NANOSECOND:i32=86; 
		pub const LogQLParser_DURATION_TYPE_YEAR:i32=87; 
		pub const LogQLParser_DURATION_TYPE_WEEK:i32=88; 
		pub const LogQLParser_DURATION_TYPE_DAY:i32=89; 
		pub const LogQLParser_DURATION_TYPE_HOUR:i32=90; 
		pub const LogQLParser_DURATION_TYPE_MINUTE:i32=91; 
		pub const LogQLParser_DURATION_TYPE_SECOND:i32=92; 
		pub const LogQLParser_NUMBER:i32=93; 
		pub const LogQLParser_DURATION:i32=94; 
		pub const LogQLParser_BYTES:i32=95; 
		pub const LogQLParser_STRING:i32=96; 
		pub const LogQLParser_PREFIX:i32=97; 
		pub const LogQLParser_IP:i32=98; 
		pub const LogQLParser_ATTRIBUTE:i32=99; 
		pub const LogQLParser_IPV4_ADDRESS:i32=100; 
		pub const LogQLParser_IPV6_ADDRESS:i32=101; 
		pub const LogQLParser_WS:i32=102; 
		pub const LogQLParser_SL_COMMENT:i32=103;
	pub const LogQLParser_EOF:i32=EOF;
	pub const RULE_root:usize = 0; 
	pub const RULE_expr:usize = 1; 
	pub const RULE_logExpr:usize = 2; 
	pub const RULE_selector:usize = 3; 
	pub const RULE_matchers:usize = 4; 
	pub const RULE_matcher:usize = 5; 
	pub const RULE_pipelineExpr:usize = 6; 
	pub const RULE_pipelineStage:usize = 7; 
	pub const RULE_lineFilters:usize = 8; 
	pub const RULE_lineFilter:usize = 9; 
	pub const RULE_ipFn:usize = 10; 
	pub const RULE_regexpParser:usize = 11; 
	pub const RULE_patternParser:usize = 12; 
	pub const RULE_unpackParser:usize = 13; 
	pub const RULE_logfmtParser:usize = 14; 
	pub const RULE_labelFormatExpr:usize = 15; 
	pub const RULE_labelFormatOps:usize = 16; 
	pub const RULE_labelFormatOp:usize = 17; 
	pub const RULE_lineFormatExpr:usize = 18; 
	pub const RULE_decolorizeExpr:usize = 19; 
	pub const RULE_dropExpr:usize = 20; 
	pub const RULE_keepExpr:usize = 21; 
	pub const RULE_jsonParser:usize = 22; 
	pub const RULE_labelExtractionExpr:usize = 23; 
	pub const RULE_labelExtractions:usize = 24; 
	pub const RULE_labelFilter:usize = 25; 
	pub const RULE_numberFilter:usize = 26; 
	pub const RULE_durationFilter:usize = 27; 
	pub const RULE_bytesFilter:usize = 28; 
	pub const RULE_ipLabelFilter:usize = 29; 
	pub const RULE_comparisonOp:usize = 30; 
	pub const RULE_metricExpr:usize = 31; 
	pub const RULE_rangeAggregationExpr:usize = 32; 
	pub const RULE_rangeLogOp:usize = 33; 
	pub const RULE_rangeUnwrapOpNoGrouping:usize = 34; 
	pub const RULE_rangeUnwrapOpWithGrouping:usize = 35; 
	pub const RULE_vectorAggregationExpr:usize = 36; 
	pub const RULE_vectorOp:usize = 37; 
	pub const RULE_binOpModifier:usize = 38; 
	pub const RULE_onOrIgnoringModifier:usize = 39; 
	pub const RULE_grouping:usize = 40; 
	pub const RULE_binOpGroupingLabels:usize = 41; 
	pub const RULE_groupingLabelList:usize = 42; 
	pub const RULE_groupingLabel:usize = 43; 
	pub const RULE_groupingLabels:usize = 44; 
	pub const RULE_logRangeExpr:usize = 45; 
	pub const RULE_unwrappedRangeExpr:usize = 46; 
	pub const RULE_range:usize = 47; 
	pub const RULE_offsetExpr:usize = 48; 
	pub const RULE_atModifier:usize = 49; 
	pub const RULE_unwrapExpr:usize = 50; 
	pub const RULE_literalExpr:usize = 51; 
	pub const RULE_labelReplaceExpr:usize = 52; 
	pub const RULE_vectorExpr:usize = 53; 
	pub const RULE_variableExpr:usize = 54; 
	pub const RULE_duration:usize = 55;
	pub const ruleNames: [&'static str; 56] =  [
		"root", "expr", "logExpr", "selector", "matchers", "matcher", "pipelineExpr", 
		"pipelineStage", "lineFilters", "lineFilter", "ipFn", "regexpParser", 
		"patternParser", "unpackParser", "logfmtParser", "labelFormatExpr", "labelFormatOps", 
		"labelFormatOp", "lineFormatExpr", "decolorizeExpr", "dropExpr", "keepExpr", 
		"jsonParser", "labelExtractionExpr", "labelExtractions", "labelFilter", 
		"numberFilter", "durationFilter", "bytesFilter", "ipLabelFilter", "comparisonOp", 
		"metricExpr", "rangeAggregationExpr", "rangeLogOp", "rangeUnwrapOpNoGrouping", 
		"rangeUnwrapOpWithGrouping", "vectorAggregationExpr", "vectorOp", "binOpModifier", 
		"onOrIgnoringModifier", "grouping", "binOpGroupingLabels", "groupingLabelList", 
		"groupingLabel", "groupingLabels", "logRangeExpr", "unwrappedRangeExpr", 
		"range", "offsetExpr", "atModifier", "unwrapExpr", "literalExpr", "labelReplaceExpr", 
		"vectorExpr", "variableExpr", "duration"
	];


	pub const _LITERAL_NAMES: [Option<&'static str>;99] = [
		None, Some("'('"), Some("')'"), Some("'{'"), Some("'}'"), Some("'['"), 
		Some("']'"), Some("':'"), Some("','"), Some("';'"), Some("'|'"), Some("'.'"), 
		Some("'+'"), Some("'-'"), Some("'*'"), Some("'/'"), Some("'^'"), Some("'='"), 
		Some("'!='"), Some("'=~'"), Some("'!~'"), Some("'>'"), Some("'<'"), Some("'>='"), 
		Some("'<='"), Some("'=='"), Some("'|='"), Some("'|~'"), Some("'|>'"), 
		Some("'!>'"), Some("'and'"), Some("'or'"), Some("'unless'"), Some("'bool'"), 
		Some("'by'"), Some("'without'"), Some("'keep'"), Some("'drop'"), Some("'decolorize'"), 
		Some("'label_replace'"), Some("'sum'"), Some("'avg'"), Some("'count'"), 
		Some("'max'"), Some("'min'"), Some("'stddev'"), Some("'stdvar'"), Some("'topk'"), 
		Some("'bottomk'"), Some("'approx_topk'"), Some("'json'"), Some("'logfmt'"), 
		Some("'unpack'"), Some("'pattern'"), Some("'regexp'"), Some("'line_format'"), 
		Some("'label_format'"), Some("'vector'"), Some("'offset'"), Some("'on'"), 
		Some("'ignoring'"), Some("'group_left'"), Some("'group_right'"), Some("'unwrap'"), 
		Some("'sort'"), Some("'sort_desc'"), Some("'@'"), Some("'count_over_time'"), 
		Some("'rate'"), Some("'rate_counter'"), Some("'bytes_over_time'"), Some("'bytes_rate'"), 
		Some("'avg_over_time'"), Some("'sum_over_time'"), Some("'min_over_time'"), 
		Some("'max_over_time'"), Some("'stddev_over_time'"), Some("'stdvar_over_time'"), 
		Some("'quantile_over_time'"), Some("'first_over_time'"), Some("'last_over_time'"), 
		Some("'absent_over_time'"), Some("'%'"), None, Some("'ms'"), None, Some("'ns'"), 
		Some("'y'"), Some("'w'"), Some("'d'"), Some("'h'"), Some("'m'"), Some("'s'"), 
		None, None, None, None, None, Some("'ip'")
	];
	pub const _SYMBOLIC_NAMES: [Option<&'static str>;104]  = [
		None, Some("LPAREN"), Some("RPAREN"), Some("LBRACE"), Some("RBRACE"), 
		Some("LBRACK"), Some("RBRACK"), Some("COLON"), Some("COMMA"), Some("SEMI"), 
		Some("PIPE"), Some("DOT"), Some("ADD"), Some("SUB"), Some("MUL"), Some("DIV"), 
		Some("POW"), Some("EQ"), Some("NE"), Some("RE"), Some("NRE"), Some("GT"), 
		Some("LT"), Some("GE"), Some("LE"), Some("EQL"), Some("PIPE_CONTAINS"), 
		Some("PIPE_MATCH"), Some("PIPE_PATTERN"), Some("PIPE_NPATTERN"), Some("AND"), 
		Some("OR"), Some("UNLESS"), Some("BOOL"), Some("BY"), Some("WITHOUT"), 
		Some("KEEP"), Some("DROP"), Some("DECOLORIZE"), Some("LABEL_REPLACE"), 
		Some("SUM"), Some("AVG"), Some("COUNT"), Some("MAX"), Some("MIN"), Some("STDDEV"), 
		Some("STDVAR"), Some("TOPK"), Some("BOTTOMK"), Some("APPROX_TOPK"), Some("JSON"), 
		Some("LOGFMT"), Some("UNPACK"), Some("PATTERN"), Some("REGEXP"), Some("LINE_FORMAT"), 
		Some("LABEL_FORMAT"), Some("VECTOR"), Some("OFFSET"), Some("ON"), Some("IGNORING"), 
		Some("GROUP_LEFT"), Some("GROUP_RIGHT"), Some("UNWRAP"), Some("SORT"), 
		Some("SORT_DESC"), Some("AT"), Some("COUNT_OVER_TIME"), Some("RATE"), 
		Some("RATE_COUNTER"), Some("BYTES_OVER_TIME"), Some("BYTES_RATE"), Some("AVG_OVER_TIME"), 
		Some("SUM_OVER_TIME"), Some("MIN_OVER_TIME"), Some("MAX_OVER_TIME"), Some("STDDEV_OVER_TIME"), 
		Some("STDVAR_OVER_TIME"), Some("QUANTILE_OVER_TIME"), Some("FIRST_OVER_TIME"), 
		Some("LAST_OVER_TIME"), Some("ABSENT_OVER_TIME"), Some("MOD"), Some("LOGFMT_FLAG"), 
		Some("DURATION_TYPE_MILLISECOND"), Some("DURATION_TYPE_MICROSECOND"), 
		Some("DURATION_TYPE_NANOSECOND"), Some("DURATION_TYPE_YEAR"), Some("DURATION_TYPE_WEEK"), 
		Some("DURATION_TYPE_DAY"), Some("DURATION_TYPE_HOUR"), Some("DURATION_TYPE_MINUTE"), 
		Some("DURATION_TYPE_SECOND"), Some("NUMBER"), Some("DURATION"), Some("BYTES"), 
		Some("STRING"), Some("PREFIX"), Some("IP"), Some("ATTRIBUTE"), Some("IPV4_ADDRESS"), 
		Some("IPV6_ADDRESS"), Some("WS"), Some("SL_COMMENT")
	];
	lazy_static!{
	    static ref _shared_context_cache: Arc<PredictionContextCache> = Arc::new(PredictionContextCache::new());
		static ref VOCABULARY: Box<dyn Vocabulary> = Box::new(VocabularyImpl::new(_LITERAL_NAMES.iter(), _SYMBOLIC_NAMES.iter(), None));
	}


type BaseParserType<'input, I> =
	BaseParser<'input,LogQLParserExt<'input>, I, LogQLParserContextType , dyn LogQLParserListener<'input> + 'input >;

type TokenType<'input> = <LocalTokenFactory<'input> as TokenFactory<'input>>::Tok;
pub type LocalTokenFactory<'input> = CommonTokenFactory;

pub type LogQLParserTreeWalker<'input,'a> =
	ParseTreeWalker<'input, 'a, LogQLParserContextType , dyn LogQLParserListener<'input> + 'a>;

/// Parser for LogQLParser grammar
pub struct LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	base:BaseParserType<'input,I>,
	interpreter:Arc<ParserATNSimulator>,
	_shared_context_cache: Box<PredictionContextCache>,
    pub err_handler: Box<dyn ErrorStrategy<'input,BaseParserType<'input,I> > >,
}

impl<'input, I> LogQLParser<'input, I>
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
				LogQLParserExt{
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

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    pub fn with_dyn_strategy(input: I) -> Self{
    	Self::with_strategy(input,Box::new(DefaultErrorStrategy::new()))
    }
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    pub fn new(input: I) -> Self{
    	Self::with_strategy(input,Box::new(DefaultErrorStrategy::new()))
    }
}

/// Trait for monomorphized trait object that corresponds to the nodes of parse tree generated for LogQLParser
pub trait LogQLParserContext<'input>:
	for<'x> Listenable<dyn LogQLParserListener<'input> + 'x > + 
	for<'x> Visitable<dyn LogQLParserVisitor<'input> + 'x > + 
	ParserRuleContext<'input, TF=LocalTokenFactory<'input>, Ctx=LogQLParserContextType>
{}

antlr4rust::coerce_from!{ 'input : LogQLParserContext<'input> }

impl<'input, 'x, T> VisitableDyn<T> for dyn LogQLParserContext<'input> + 'input
where
    T: LogQLParserVisitor<'input> + 'x,
{
    fn accept_dyn(&self, visitor: &mut T) {
        self.accept(visitor as &mut (dyn LogQLParserVisitor<'input> + 'x))
    }
}

impl<'input> LogQLParserContext<'input> for TerminalNode<'input,LogQLParserContextType> {}
impl<'input> LogQLParserContext<'input> for ErrorNode<'input,LogQLParserContextType> {}

antlr4rust::tid! { impl<'input> TidAble<'input> for dyn LogQLParserContext<'input> + 'input }

antlr4rust::tid! { impl<'input> TidAble<'input> for dyn LogQLParserListener<'input> + 'input }

pub struct LogQLParserContextType;
antlr4rust::tid!{LogQLParserContextType}

impl<'input> ParserNodeType<'input> for LogQLParserContextType{
	type TF = LocalTokenFactory<'input>;
	type Type = dyn LogQLParserContext<'input> + 'input;
}

impl<'input, I> Deref for LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    type Target = BaseParserType<'input,I>;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<'input, I> DerefMut for LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

pub struct LogQLParserExt<'input>{
	_pd: PhantomData<&'input str>,
}

impl<'input> LogQLParserExt<'input>{
}
antlr4rust::tid! { LogQLParserExt<'a> }

impl<'input> TokenAware<'input> for LogQLParserExt<'input>{
	type TF = LocalTokenFactory<'input>;
}

impl<'input,I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>> ParserRecog<'input, BaseParserType<'input,I>> for LogQLParserExt<'input>{}

impl<'input,I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>> Actions<'input, BaseParserType<'input,I>> for LogQLParserExt<'input>{
	fn get_grammar_file_name(&self) -> & str{ "LogQLParser.g4"}

   	fn get_rule_names(&self) -> &[& str] {&ruleNames}

   	fn get_vocabulary(&self) -> &dyn Vocabulary { &**VOCABULARY }
	fn sempred(_localctx: Option<&(dyn LogQLParserContext<'input> + 'input)>, rule_index: i32, pred_index: i32,
			   recog:&mut BaseParserType<'input,I>
	)->bool{
		match rule_index {
					6 => LogQLParser::<'input,I>::pipelineExpr_sempred(_localctx.and_then(|x|x.downcast_ref()), pred_index, recog),
					25 => LogQLParser::<'input,I>::labelFilter_sempred(_localctx.and_then(|x|x.downcast_ref()), pred_index, recog),
					31 => LogQLParser::<'input,I>::metricExpr_sempred(_localctx.and_then(|x|x.downcast_ref()), pred_index, recog),
					42 => LogQLParser::<'input,I>::groupingLabelList_sempred(_localctx.and_then(|x|x.downcast_ref()), pred_index, recog),
					50 => LogQLParser::<'input,I>::unwrapExpr_sempred(_localctx.and_then(|x|x.downcast_ref()), pred_index, recog),
			_ => true
		}
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	fn pipelineExpr_sempred(_localctx: Option<&PipelineExprContext<'input>>, pred_index:i32,
						recog:&mut <Self as Deref>::Target
		) -> bool {
		match pred_index {
				0=>{
					recog.precpred(None, 1)
				}
			_ => true
		}
	}
	fn labelFilter_sempred(_localctx: Option<&LabelFilterContext<'input>>, pred_index:i32,
						recog:&mut <Self as Deref>::Target
		) -> bool {
		match pred_index {
				1=>{
					recog.precpred(None, 8)
				}
				2=>{
					recog.precpred(None, 7)
				}
			_ => true
		}
	}
	fn metricExpr_sempred(_localctx: Option<&MetricExprContext<'input>>, pred_index:i32,
						recog:&mut <Self as Deref>::Target
		) -> bool {
		match pred_index {
				3=>{
					recog.precpred(None, 22)
				}
				4=>{
					recog.precpred(None, 21)
				}
				5=>{
					recog.precpred(None, 20)
				}
				6=>{
					recog.precpred(None, 19)
				}
				7=>{
					recog.precpred(None, 18)
				}
				8=>{
					recog.precpred(None, 17)
				}
				9=>{
					recog.precpred(None, 16)
				}
				10=>{
					recog.precpred(None, 15)
				}
				11=>{
					recog.precpred(None, 14)
				}
				12=>{
					recog.precpred(None, 13)
				}
				13=>{
					recog.precpred(None, 12)
				}
				14=>{
					recog.precpred(None, 11)
				}
				15=>{
					recog.precpred(None, 10)
				}
				16=>{
					recog.precpred(None, 9)
				}
				17=>{
					recog.precpred(None, 8)
				}
			_ => true
		}
	}
	fn groupingLabelList_sempred(_localctx: Option<&GroupingLabelListContext<'input>>, pred_index:i32,
						recog:&mut <Self as Deref>::Target
		) -> bool {
		match pred_index {
				18=>{
					recog.precpred(None, 2)
				}
			_ => true
		}
	}
	fn unwrapExpr_sempred(_localctx: Option<&UnwrapExprContext<'input>>, pred_index:i32,
						recog:&mut <Self as Deref>::Target
		) -> bool {
		match pred_index {
				19=>{
					recog.precpred(None, 1)
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

impl<'input> LogQLParserContext<'input> for RootContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RootContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_root(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_root(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RootContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_root(self);
	}
}

impl<'input> CustomRuleContext<'input> for RootContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_root }
	//fn type_rule_index() -> usize where Self: Sized { RULE_root }
}
antlr4rust::tid!{RootContextExt<'a>}

impl<'input> RootContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RootContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RootContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RootContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<RootContextExt<'input>>{

fn expr(&self) -> Option<Rc<ExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token EOF
/// Returns `None` if there is no child corresponding to token EOF
fn EOF(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_EOF, 0)
}

}

impl<'input> RootContextAttrs<'input> for RootContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
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
			recog.base.set_state(112);
			recog.expr()?;

			recog.base.set_state(113);
			recog.base.match_token(LogQLParser_EOF,&mut recog.err_handler)?;

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
pub type ExprContextAll<'input> = ExprContext<'input>;


pub type ExprContext<'input> = BaseParserRuleContext<'input,ExprContextExt<'input>>;

#[derive(Clone)]
pub struct ExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for ExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for ExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_expr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_expr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for ExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_expr(self);
	}
}

impl<'input> CustomRuleContext<'input> for ExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_expr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_expr }
}
antlr4rust::tid!{ExprContextExt<'a>}

impl<'input> ExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<ExprContextExt<'input>>{

fn logExpr(&self) -> Option<Rc<LogExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn metricExpr(&self) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> ExprContextAttrs<'input> for ExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn expr(&mut self,)
	-> Result<Rc<ExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 2, RULE_expr);
        let mut _localctx: Rc<ExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(117);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LogQLParser_LBRACE 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule logExpr*/
					recog.base.set_state(115);
					recog.logExpr()?;

					}
				}

			LogQLParser_LPAREN |LogQLParser_ADD |LogQLParser_SUB |LogQLParser_LABEL_REPLACE |
			LogQLParser_SUM |LogQLParser_AVG |LogQLParser_COUNT |LogQLParser_MAX |
			LogQLParser_MIN |LogQLParser_STDDEV |LogQLParser_STDVAR |LogQLParser_TOPK |
			LogQLParser_BOTTOMK |LogQLParser_APPROX_TOPK |LogQLParser_VECTOR |LogQLParser_SORT |
			LogQLParser_SORT_DESC |LogQLParser_COUNT_OVER_TIME |LogQLParser_RATE |
			LogQLParser_RATE_COUNTER |LogQLParser_BYTES_OVER_TIME |LogQLParser_BYTES_RATE |
			LogQLParser_AVG_OVER_TIME |LogQLParser_SUM_OVER_TIME |LogQLParser_MIN_OVER_TIME |
			LogQLParser_MAX_OVER_TIME |LogQLParser_STDDEV_OVER_TIME |LogQLParser_STDVAR_OVER_TIME |
			LogQLParser_QUANTILE_OVER_TIME |LogQLParser_FIRST_OVER_TIME |LogQLParser_LAST_OVER_TIME |
			LogQLParser_ABSENT_OVER_TIME |LogQLParser_NUMBER |LogQLParser_ATTRIBUTE 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule metricExpr*/
					recog.base.set_state(116);
					recog.metricExpr_rec(0)?;

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
//------------------- logExpr ----------------
#[derive(Debug)]
pub enum LogExprContextAll<'input>{
	LogExprWithSelectorOnlyContext(LogExprWithSelectorOnlyContext<'input>),
	LogExprWithPipelineContext(LogExprWithPipelineContext<'input>),
Error(LogExprContext<'input>)
}
antlr4rust::tid!{LogExprContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for LogExprContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for LogExprContextAll<'input>{}

impl<'input> Deref for LogExprContextAll<'input>{
	type Target = dyn LogExprContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use LogExprContextAll::*;
		match self{
			LogExprWithSelectorOnlyContext(inner) => inner,
			LogExprWithPipelineContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LogExprContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LogExprContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type LogExprContext<'input> = BaseParserRuleContext<'input,LogExprContextExt<'input>>;

#[derive(Clone)]
pub struct LogExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LogExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LogExprContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LogExprContext<'input>{
}

impl<'input> CustomRuleContext<'input> for LogExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_logExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_logExpr }
}
antlr4rust::tid!{LogExprContextExt<'a>}

impl<'input> LogExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LogExprContextAll<'input>> {
		Rc::new(
		LogExprContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LogExprContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait LogExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LogExprContextExt<'input>>{


}

impl<'input> LogExprContextAttrs<'input> for LogExprContext<'input>{}

pub type LogExprWithSelectorOnlyContext<'input> = BaseParserRuleContext<'input,LogExprWithSelectorOnlyContextExt<'input>>;

pub trait LogExprWithSelectorOnlyContextAttrs<'input>: LogQLParserContext<'input>{
	fn selector(&self) -> Option<Rc<SelectorContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> LogExprWithSelectorOnlyContextAttrs<'input> for LogExprWithSelectorOnlyContext<'input>{}

pub struct LogExprWithSelectorOnlyContextExt<'input>{
	base:LogExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LogExprWithSelectorOnlyContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LogExprWithSelectorOnlyContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LogExprWithSelectorOnlyContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_logExprWithSelectorOnly(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_logExprWithSelectorOnly(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LogExprWithSelectorOnlyContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_logExprWithSelectorOnly(self);
	}
}

impl<'input> CustomRuleContext<'input> for LogExprWithSelectorOnlyContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_logExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_logExpr }
}

impl<'input> Borrow<LogExprContextExt<'input>> for LogExprWithSelectorOnlyContext<'input>{
	fn borrow(&self) -> &LogExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LogExprContextExt<'input>> for LogExprWithSelectorOnlyContext<'input>{
	fn borrow_mut(&mut self) -> &mut LogExprContextExt<'input> { &mut self.base }
}

impl<'input> LogExprContextAttrs<'input> for LogExprWithSelectorOnlyContext<'input> {}

impl<'input> LogExprWithSelectorOnlyContextExt<'input>{
	fn new(ctx: &dyn LogExprContextAttrs<'input>) -> Rc<LogExprContextAll<'input>>  {
		Rc::new(
			LogExprContextAll::LogExprWithSelectorOnlyContext(
				BaseParserRuleContext::copy_from(ctx,LogExprWithSelectorOnlyContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LogExprWithPipelineContext<'input> = BaseParserRuleContext<'input,LogExprWithPipelineContextExt<'input>>;

pub trait LogExprWithPipelineContextAttrs<'input>: LogQLParserContext<'input>{
	fn selector(&self) -> Option<Rc<SelectorContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	fn pipelineExpr(&self) -> Option<Rc<PipelineExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> LogExprWithPipelineContextAttrs<'input> for LogExprWithPipelineContext<'input>{}

pub struct LogExprWithPipelineContextExt<'input>{
	base:LogExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LogExprWithPipelineContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LogExprWithPipelineContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LogExprWithPipelineContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_logExprWithPipeline(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_logExprWithPipeline(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LogExprWithPipelineContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_logExprWithPipeline(self);
	}
}

impl<'input> CustomRuleContext<'input> for LogExprWithPipelineContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_logExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_logExpr }
}

impl<'input> Borrow<LogExprContextExt<'input>> for LogExprWithPipelineContext<'input>{
	fn borrow(&self) -> &LogExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LogExprContextExt<'input>> for LogExprWithPipelineContext<'input>{
	fn borrow_mut(&mut self) -> &mut LogExprContextExt<'input> { &mut self.base }
}

impl<'input> LogExprContextAttrs<'input> for LogExprWithPipelineContext<'input> {}

impl<'input> LogExprWithPipelineContextExt<'input>{
	fn new(ctx: &dyn LogExprContextAttrs<'input>) -> Rc<LogExprContextAll<'input>>  {
		Rc::new(
			LogExprContextAll::LogExprWithPipelineContext(
				BaseParserRuleContext::copy_from(ctx,LogExprWithPipelineContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn logExpr(&mut self,)
	-> Result<Rc<LogExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LogExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 4, RULE_logExpr);
        let mut _localctx: Rc<LogExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(123);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(1,&mut recog.base)? {
				1 =>{
					let tmp = LogExprWithSelectorOnlyContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					/*InvokeRule selector*/
					recog.base.set_state(119);
					recog.selector()?;

					}
				}
			,
				2 =>{
					let tmp = LogExprWithPipelineContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					/*InvokeRule selector*/
					recog.base.set_state(120);
					recog.selector()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(121);
					recog.pipelineExpr_rec(0)?;

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
//------------------- selector ----------------
pub type SelectorContextAll<'input> = SelectorContext<'input>;


pub type SelectorContext<'input> = BaseParserRuleContext<'input,SelectorContextExt<'input>>;

#[derive(Clone)]
pub struct SelectorContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for SelectorContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for SelectorContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_selector(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_selector(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for SelectorContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_selector(self);
	}
}

impl<'input> CustomRuleContext<'input> for SelectorContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_selector }
	//fn type_rule_index() -> usize where Self: Sized { RULE_selector }
}
antlr4rust::tid!{SelectorContextExt<'a>}

impl<'input> SelectorContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<SelectorContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,SelectorContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait SelectorContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<SelectorContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LBRACE
/// Returns `None` if there is no child corresponding to token LBRACE
fn LBRACE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LBRACE, 0)
}
fn matchers(&self) -> Option<Rc<MatchersContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RBRACE
/// Returns `None` if there is no child corresponding to token RBRACE
fn RBRACE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RBRACE, 0)
}

}

impl<'input> SelectorContextAttrs<'input> for SelectorContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn selector(&mut self,)
	-> Result<Rc<SelectorContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = SelectorContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 6, RULE_selector);
        let mut _localctx: Rc<SelectorContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(131);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(2,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(125);
					recog.base.match_token(LogQLParser_LBRACE,&mut recog.err_handler)?;

					/*InvokeRule matchers*/
					recog.base.set_state(126);
					recog.matchers()?;

					recog.base.set_state(127);
					recog.base.match_token(LogQLParser_RBRACE,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(129);
					recog.base.match_token(LogQLParser_LBRACE,&mut recog.err_handler)?;

					recog.base.set_state(130);
					recog.base.match_token(LogQLParser_RBRACE,&mut recog.err_handler)?;

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
//------------------- matchers ----------------
pub type MatchersContextAll<'input> = MatchersContext<'input>;


pub type MatchersContext<'input> = BaseParserRuleContext<'input,MatchersContextExt<'input>>;

#[derive(Clone)]
pub struct MatchersContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for MatchersContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MatchersContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_matchers(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_matchers(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MatchersContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_matchers(self);
	}
}

impl<'input> CustomRuleContext<'input> for MatchersContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_matchers }
	//fn type_rule_index() -> usize where Self: Sized { RULE_matchers }
}
antlr4rust::tid!{MatchersContextExt<'a>}

impl<'input> MatchersContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<MatchersContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,MatchersContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait MatchersContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<MatchersContextExt<'input>>{

fn matcher_all(&self) ->  Vec<Rc<MatcherContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn matcher(&self, i: usize) -> Option<Rc<MatcherContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COMMA, i)
}

}

impl<'input> MatchersContextAttrs<'input> for MatchersContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn matchers(&mut self,)
	-> Result<Rc<MatchersContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = MatchersContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 8, RULE_matchers);
        let mut _localctx: Rc<MatchersContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule matcher*/
			recog.base.set_state(133);
			recog.matcher()?;

			recog.base.set_state(138);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			while _la==LogQLParser_COMMA {
				{
				{
				recog.base.set_state(134);
				recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

				/*InvokeRule matcher*/
				recog.base.set_state(135);
				recog.matcher()?;

				}
				}
				recog.base.set_state(140);
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
//------------------- matcher ----------------
#[derive(Debug)]
pub enum MatcherContextAll<'input>{
	MatcherReContext(MatcherReContext<'input>),
	MatcherNeqContext(MatcherNeqContext<'input>),
	MatcherEqContext(MatcherEqContext<'input>),
	MatcherNreContext(MatcherNreContext<'input>),
Error(MatcherContext<'input>)
}
antlr4rust::tid!{MatcherContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for MatcherContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for MatcherContextAll<'input>{}

impl<'input> Deref for MatcherContextAll<'input>{
	type Target = dyn MatcherContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use MatcherContextAll::*;
		match self{
			MatcherReContext(inner) => inner,
			MatcherNeqContext(inner) => inner,
			MatcherEqContext(inner) => inner,
			MatcherNreContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MatcherContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MatcherContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type MatcherContext<'input> = BaseParserRuleContext<'input,MatcherContextExt<'input>>;

#[derive(Clone)]
pub struct MatcherContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for MatcherContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MatcherContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MatcherContext<'input>{
}

impl<'input> CustomRuleContext<'input> for MatcherContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_matcher }
	//fn type_rule_index() -> usize where Self: Sized { RULE_matcher }
}
antlr4rust::tid!{MatcherContextExt<'a>}

impl<'input> MatcherContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<MatcherContextAll<'input>> {
		Rc::new(
		MatcherContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,MatcherContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait MatcherContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<MatcherContextExt<'input>>{


}

impl<'input> MatcherContextAttrs<'input> for MatcherContext<'input>{}

pub type MatcherReContext<'input> = BaseParserRuleContext<'input,MatcherReContextExt<'input>>;

pub trait MatcherReContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
	/// Returns `None` if there is no child corresponding to token ATTRIBUTE
	fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token RE
	/// Returns `None` if there is no child corresponding to token RE
	fn RE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token STRING
	/// Returns `None` if there is no child corresponding to token STRING
	fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_STRING, 0)
	}
	/// Retrieves first TerminalNode corresponding to token PREFIX
	/// Returns `None` if there is no child corresponding to token PREFIX
	fn PREFIX(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PREFIX, 0)
	}
}

impl<'input> MatcherReContextAttrs<'input> for MatcherReContext<'input>{}

pub struct MatcherReContextExt<'input>{
	base:MatcherContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MatcherReContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MatcherReContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MatcherReContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_matcherRe(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_matcherRe(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MatcherReContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_matcherRe(self);
	}
}

impl<'input> CustomRuleContext<'input> for MatcherReContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_matcher }
	//fn type_rule_index() -> usize where Self: Sized { RULE_matcher }
}

impl<'input> Borrow<MatcherContextExt<'input>> for MatcherReContext<'input>{
	fn borrow(&self) -> &MatcherContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MatcherContextExt<'input>> for MatcherReContext<'input>{
	fn borrow_mut(&mut self) -> &mut MatcherContextExt<'input> { &mut self.base }
}

impl<'input> MatcherContextAttrs<'input> for MatcherReContext<'input> {}

impl<'input> MatcherReContextExt<'input>{
	fn new(ctx: &dyn MatcherContextAttrs<'input>) -> Rc<MatcherContextAll<'input>>  {
		Rc::new(
			MatcherContextAll::MatcherReContext(
				BaseParserRuleContext::copy_from(ctx,MatcherReContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type MatcherNeqContext<'input> = BaseParserRuleContext<'input,MatcherNeqContextExt<'input>>;

pub trait MatcherNeqContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
	/// Returns `None` if there is no child corresponding to token ATTRIBUTE
	fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token NE
	/// Returns `None` if there is no child corresponding to token NE
	fn NE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_NE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token STRING
	/// Returns `None` if there is no child corresponding to token STRING
	fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_STRING, 0)
	}
	/// Retrieves first TerminalNode corresponding to token PREFIX
	/// Returns `None` if there is no child corresponding to token PREFIX
	fn PREFIX(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PREFIX, 0)
	}
}

impl<'input> MatcherNeqContextAttrs<'input> for MatcherNeqContext<'input>{}

pub struct MatcherNeqContextExt<'input>{
	base:MatcherContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MatcherNeqContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MatcherNeqContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MatcherNeqContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_matcherNeq(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_matcherNeq(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MatcherNeqContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_matcherNeq(self);
	}
}

impl<'input> CustomRuleContext<'input> for MatcherNeqContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_matcher }
	//fn type_rule_index() -> usize where Self: Sized { RULE_matcher }
}

impl<'input> Borrow<MatcherContextExt<'input>> for MatcherNeqContext<'input>{
	fn borrow(&self) -> &MatcherContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MatcherContextExt<'input>> for MatcherNeqContext<'input>{
	fn borrow_mut(&mut self) -> &mut MatcherContextExt<'input> { &mut self.base }
}

impl<'input> MatcherContextAttrs<'input> for MatcherNeqContext<'input> {}

impl<'input> MatcherNeqContextExt<'input>{
	fn new(ctx: &dyn MatcherContextAttrs<'input>) -> Rc<MatcherContextAll<'input>>  {
		Rc::new(
			MatcherContextAll::MatcherNeqContext(
				BaseParserRuleContext::copy_from(ctx,MatcherNeqContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type MatcherEqContext<'input> = BaseParserRuleContext<'input,MatcherEqContextExt<'input>>;

pub trait MatcherEqContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
	/// Returns `None` if there is no child corresponding to token ATTRIBUTE
	fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token EQ
	/// Returns `None` if there is no child corresponding to token EQ
	fn EQ(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_EQ, 0)
	}
	/// Retrieves first TerminalNode corresponding to token STRING
	/// Returns `None` if there is no child corresponding to token STRING
	fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_STRING, 0)
	}
	/// Retrieves first TerminalNode corresponding to token PREFIX
	/// Returns `None` if there is no child corresponding to token PREFIX
	fn PREFIX(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PREFIX, 0)
	}
}

impl<'input> MatcherEqContextAttrs<'input> for MatcherEqContext<'input>{}

pub struct MatcherEqContextExt<'input>{
	base:MatcherContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MatcherEqContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MatcherEqContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MatcherEqContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_matcherEq(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_matcherEq(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MatcherEqContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_matcherEq(self);
	}
}

impl<'input> CustomRuleContext<'input> for MatcherEqContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_matcher }
	//fn type_rule_index() -> usize where Self: Sized { RULE_matcher }
}

impl<'input> Borrow<MatcherContextExt<'input>> for MatcherEqContext<'input>{
	fn borrow(&self) -> &MatcherContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MatcherContextExt<'input>> for MatcherEqContext<'input>{
	fn borrow_mut(&mut self) -> &mut MatcherContextExt<'input> { &mut self.base }
}

impl<'input> MatcherContextAttrs<'input> for MatcherEqContext<'input> {}

impl<'input> MatcherEqContextExt<'input>{
	fn new(ctx: &dyn MatcherContextAttrs<'input>) -> Rc<MatcherContextAll<'input>>  {
		Rc::new(
			MatcherContextAll::MatcherEqContext(
				BaseParserRuleContext::copy_from(ctx,MatcherEqContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type MatcherNreContext<'input> = BaseParserRuleContext<'input,MatcherNreContextExt<'input>>;

pub trait MatcherNreContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
	/// Returns `None` if there is no child corresponding to token ATTRIBUTE
	fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token NRE
	/// Returns `None` if there is no child corresponding to token NRE
	fn NRE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_NRE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token STRING
	/// Returns `None` if there is no child corresponding to token STRING
	fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_STRING, 0)
	}
	/// Retrieves first TerminalNode corresponding to token PREFIX
	/// Returns `None` if there is no child corresponding to token PREFIX
	fn PREFIX(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PREFIX, 0)
	}
}

impl<'input> MatcherNreContextAttrs<'input> for MatcherNreContext<'input>{}

pub struct MatcherNreContextExt<'input>{
	base:MatcherContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MatcherNreContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MatcherNreContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MatcherNreContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_matcherNre(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_matcherNre(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MatcherNreContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_matcherNre(self);
	}
}

impl<'input> CustomRuleContext<'input> for MatcherNreContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_matcher }
	//fn type_rule_index() -> usize where Self: Sized { RULE_matcher }
}

impl<'input> Borrow<MatcherContextExt<'input>> for MatcherNreContext<'input>{
	fn borrow(&self) -> &MatcherContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MatcherContextExt<'input>> for MatcherNreContext<'input>{
	fn borrow_mut(&mut self) -> &mut MatcherContextExt<'input> { &mut self.base }
}

impl<'input> MatcherContextAttrs<'input> for MatcherNreContext<'input> {}

impl<'input> MatcherNreContextExt<'input>{
	fn new(ctx: &dyn MatcherContextAttrs<'input>) -> Rc<MatcherContextAll<'input>>  {
		Rc::new(
			MatcherContextAll::MatcherNreContext(
				BaseParserRuleContext::copy_from(ctx,MatcherNreContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn matcher(&mut self,)
	-> Result<Rc<MatcherContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = MatcherContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 10, RULE_matcher);
        let mut _localctx: Rc<MatcherContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(165);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(8,&mut recog.base)? {
				1 =>{
					let tmp = MatcherEqContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(142);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_PREFIX {
						{
						recog.base.set_state(141);
						recog.base.match_token(LogQLParser_PREFIX,&mut recog.err_handler)?;

						}
					}

					recog.base.set_state(144);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(145);
					recog.base.match_token(LogQLParser_EQ,&mut recog.err_handler)?;

					recog.base.set_state(146);
					recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					let tmp = MatcherNeqContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(148);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_PREFIX {
						{
						recog.base.set_state(147);
						recog.base.match_token(LogQLParser_PREFIX,&mut recog.err_handler)?;

						}
					}

					recog.base.set_state(150);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(151);
					recog.base.match_token(LogQLParser_NE,&mut recog.err_handler)?;

					recog.base.set_state(152);
					recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

					}
				}
			,
				3 =>{
					let tmp = MatcherReContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					recog.base.set_state(154);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_PREFIX {
						{
						recog.base.set_state(153);
						recog.base.match_token(LogQLParser_PREFIX,&mut recog.err_handler)?;

						}
					}

					recog.base.set_state(156);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(157);
					recog.base.match_token(LogQLParser_RE,&mut recog.err_handler)?;

					recog.base.set_state(158);
					recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

					}
				}
			,
				4 =>{
					let tmp = MatcherNreContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 4)?;
					_localctx = tmp;
					{
					recog.base.set_state(160);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_PREFIX {
						{
						recog.base.set_state(159);
						recog.base.match_token(LogQLParser_PREFIX,&mut recog.err_handler)?;

						}
					}

					recog.base.set_state(162);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(163);
					recog.base.match_token(LogQLParser_NRE,&mut recog.err_handler)?;

					recog.base.set_state(164);
					recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

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

impl<'input> LogQLParserContext<'input> for PipelineExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for PipelineExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_pipelineExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_pipelineExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for PipelineExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_pipelineExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for PipelineExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_pipelineExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_pipelineExpr }
}
antlr4rust::tid!{PipelineExprContextExt<'a>}

impl<'input> PipelineExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<PipelineExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,PipelineExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait PipelineExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<PipelineExprContextExt<'input>>{

fn pipelineStage(&self) -> Option<Rc<PipelineStageContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn pipelineExpr(&self) -> Option<Rc<PipelineExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> PipelineExprContextAttrs<'input> for PipelineExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn  pipelineExpr(&mut self,)
	-> Result<Rc<PipelineExprContextAll<'input>>,ANTLRError> {
		self.pipelineExpr_rec(0)
	}

	fn pipelineExpr_rec(&mut self, _p: i32)
	-> Result<Rc<PipelineExprContextAll<'input>>,ANTLRError> {
		let recog = self;
		let _parentctx = recog.ctx.take();
		let _parentState = recog.base.get_state();
		let mut _localctx = PipelineExprContextExt::new(_parentctx.clone(), recog.base.get_state());
		recog.base.enter_recursion_rule(_localctx.clone(), 12, RULE_pipelineExpr, _p);
	    let mut _localctx: Rc<PipelineExprContextAll> = _localctx;
        let mut _prevctx = _localctx.clone();
		let _startState = 12;
		let result: Result<(), ANTLRError> = (|| {
			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			{
			/*InvokeRule pipelineStage*/
			recog.base.set_state(168);
			recog.pipelineStage()?;

			}
			let tmp = recog.input.lt(-1).cloned();
			recog.ctx.as_ref().unwrap().set_stop(tmp);
			recog.base.set_state(174);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(9,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					recog.trigger_exit_rule_event()?;
					_prevctx = _localctx.clone();
					{
					{
					/*recRuleAltStartAction*/
					let mut tmp = PipelineExprContextExt::new(_parentctx.clone(), _parentState);
					recog.push_new_recursion_context(tmp.clone(), _startState, RULE_pipelineExpr)?;
					_localctx = tmp;
					recog.base.set_state(170);
					if !({let _localctx = Some(_localctx.clone());
					recog.precpred(None, 1)}) {
						Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 1)".to_owned()), None))?;
					}
					/*InvokeRule pipelineStage*/
					recog.base.set_state(171);
					recog.pipelineStage()?;

					}
					} 
				}
				recog.base.set_state(176);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(9,&mut recog.base)?;
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
//------------------- pipelineStage ----------------
pub type PipelineStageContextAll<'input> = PipelineStageContext<'input>;


pub type PipelineStageContext<'input> = BaseParserRuleContext<'input,PipelineStageContextExt<'input>>;

#[derive(Clone)]
pub struct PipelineStageContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for PipelineStageContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for PipelineStageContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_pipelineStage(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_pipelineStage(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for PipelineStageContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_pipelineStage(self);
	}
}

impl<'input> CustomRuleContext<'input> for PipelineStageContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_pipelineStage }
	//fn type_rule_index() -> usize where Self: Sized { RULE_pipelineStage }
}
antlr4rust::tid!{PipelineStageContextExt<'a>}

impl<'input> PipelineStageContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<PipelineStageContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,PipelineStageContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait PipelineStageContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<PipelineStageContextExt<'input>>{

fn lineFilters(&self) -> Option<Rc<LineFiltersContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token PIPE
/// Returns `None` if there is no child corresponding to token PIPE
fn PIPE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_PIPE, 0)
}
fn logfmtParser(&self) -> Option<Rc<LogfmtParserContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn regexpParser(&self) -> Option<Rc<RegexpParserContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn patternParser(&self) -> Option<Rc<PatternParserContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn unpackParser(&self) -> Option<Rc<UnpackParserContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn labelFormatExpr(&self) -> Option<Rc<LabelFormatExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn lineFormatExpr(&self) -> Option<Rc<LineFormatExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn decolorizeExpr(&self) -> Option<Rc<DecolorizeExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn dropExpr(&self) -> Option<Rc<DropExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn keepExpr(&self) -> Option<Rc<KeepExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn jsonParser(&self) -> Option<Rc<JsonParserContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn labelFilter(&self) -> Option<Rc<LabelFilterContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> PipelineStageContextAttrs<'input> for PipelineStageContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn pipelineStage(&mut self,)
	-> Result<Rc<PipelineStageContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = PipelineStageContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 14, RULE_pipelineStage);
        let mut _localctx: Rc<PipelineStageContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(200);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(10,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule lineFilters*/
					recog.base.set_state(177);
					recog.lineFilters()?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(178);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule logfmtParser*/
					recog.base.set_state(179);
					recog.logfmtParser()?;

					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(180);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule regexpParser*/
					recog.base.set_state(181);
					recog.regexpParser()?;

					}
				}
			,
				4 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					recog.base.set_state(182);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule patternParser*/
					recog.base.set_state(183);
					recog.patternParser()?;

					}
				}
			,
				5 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 5)?;
					recog.base.enter_outer_alt(None, 5)?;
					{
					recog.base.set_state(184);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule unpackParser*/
					recog.base.set_state(185);
					recog.unpackParser()?;

					}
				}
			,
				6 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 6)?;
					recog.base.enter_outer_alt(None, 6)?;
					{
					recog.base.set_state(186);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule labelFormatExpr*/
					recog.base.set_state(187);
					recog.labelFormatExpr()?;

					}
				}
			,
				7 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 7)?;
					recog.base.enter_outer_alt(None, 7)?;
					{
					recog.base.set_state(188);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule lineFormatExpr*/
					recog.base.set_state(189);
					recog.lineFormatExpr()?;

					}
				}
			,
				8 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 8)?;
					recog.base.enter_outer_alt(None, 8)?;
					{
					recog.base.set_state(190);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule decolorizeExpr*/
					recog.base.set_state(191);
					recog.decolorizeExpr()?;

					}
				}
			,
				9 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 9)?;
					recog.base.enter_outer_alt(None, 9)?;
					{
					recog.base.set_state(192);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule dropExpr*/
					recog.base.set_state(193);
					recog.dropExpr()?;

					}
				}
			,
				10 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 10)?;
					recog.base.enter_outer_alt(None, 10)?;
					{
					recog.base.set_state(194);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule keepExpr*/
					recog.base.set_state(195);
					recog.keepExpr()?;

					}
				}
			,
				11 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 11)?;
					recog.base.enter_outer_alt(None, 11)?;
					{
					recog.base.set_state(196);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule jsonParser*/
					recog.base.set_state(197);
					recog.jsonParser()?;

					}
				}
			,
				12 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 12)?;
					recog.base.enter_outer_alt(None, 12)?;
					{
					recog.base.set_state(198);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule labelFilter*/
					recog.base.set_state(199);
					recog.labelFilter_rec(0)?;

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
//------------------- lineFilters ----------------
#[derive(Debug)]
pub enum LineFiltersContextAll<'input>{
	LineFiltersMatchContext(LineFiltersMatchContext<'input>),
	LineFiltersContainsContext(LineFiltersContainsContext<'input>),
	LineFiltersNotPatternContext(LineFiltersNotPatternContext<'input>),
	LineFiltersNotContainsContext(LineFiltersNotContainsContext<'input>),
	LineFiltersPatternContext(LineFiltersPatternContext<'input>),
	LineFiltersNotMatchContext(LineFiltersNotMatchContext<'input>),
Error(LineFiltersContext<'input>)
}
antlr4rust::tid!{LineFiltersContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for LineFiltersContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for LineFiltersContextAll<'input>{}

impl<'input> Deref for LineFiltersContextAll<'input>{
	type Target = dyn LineFiltersContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use LineFiltersContextAll::*;
		match self{
			LineFiltersMatchContext(inner) => inner,
			LineFiltersContainsContext(inner) => inner,
			LineFiltersNotPatternContext(inner) => inner,
			LineFiltersNotContainsContext(inner) => inner,
			LineFiltersPatternContext(inner) => inner,
			LineFiltersNotMatchContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFiltersContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFiltersContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type LineFiltersContext<'input> = BaseParserRuleContext<'input,LineFiltersContextExt<'input>>;

#[derive(Clone)]
pub struct LineFiltersContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LineFiltersContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFiltersContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFiltersContext<'input>{
}

impl<'input> CustomRuleContext<'input> for LineFiltersContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilters }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilters }
}
antlr4rust::tid!{LineFiltersContextExt<'a>}

impl<'input> LineFiltersContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LineFiltersContextAll<'input>> {
		Rc::new(
		LineFiltersContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LineFiltersContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait LineFiltersContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LineFiltersContextExt<'input>>{


}

impl<'input> LineFiltersContextAttrs<'input> for LineFiltersContext<'input>{}

pub type LineFiltersMatchContext<'input> = BaseParserRuleContext<'input,LineFiltersMatchContextExt<'input>>;

pub trait LineFiltersMatchContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token PIPE_MATCH
	/// Returns `None` if there is no child corresponding to token PIPE_MATCH
	fn PIPE_MATCH(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PIPE_MATCH, 0)
	}
	fn lineFilter_all(&self) ->  Vec<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn lineFilter(&self, i: usize) -> Option<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves all `TerminalNode`s corresponding to token OR in current rule
	fn OR_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
		self.children_of_type()
	}
	/// Retrieves 'i's TerminalNode corresponding to token OR, starting from 0.
	/// Returns `None` if number of children corresponding to token OR is less or equal than `i`.
	fn OR(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_OR, i)
	}
}

impl<'input> LineFiltersMatchContextAttrs<'input> for LineFiltersMatchContext<'input>{}

pub struct LineFiltersMatchContextExt<'input>{
	base:LineFiltersContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LineFiltersMatchContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LineFiltersMatchContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFiltersMatchContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_lineFiltersMatch(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_lineFiltersMatch(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFiltersMatchContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_lineFiltersMatch(self);
	}
}

impl<'input> CustomRuleContext<'input> for LineFiltersMatchContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilters }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilters }
}

impl<'input> Borrow<LineFiltersContextExt<'input>> for LineFiltersMatchContext<'input>{
	fn borrow(&self) -> &LineFiltersContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LineFiltersContextExt<'input>> for LineFiltersMatchContext<'input>{
	fn borrow_mut(&mut self) -> &mut LineFiltersContextExt<'input> { &mut self.base }
}

impl<'input> LineFiltersContextAttrs<'input> for LineFiltersMatchContext<'input> {}

impl<'input> LineFiltersMatchContextExt<'input>{
	fn new(ctx: &dyn LineFiltersContextAttrs<'input>) -> Rc<LineFiltersContextAll<'input>>  {
		Rc::new(
			LineFiltersContextAll::LineFiltersMatchContext(
				BaseParserRuleContext::copy_from(ctx,LineFiltersMatchContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LineFiltersContainsContext<'input> = BaseParserRuleContext<'input,LineFiltersContainsContextExt<'input>>;

pub trait LineFiltersContainsContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token PIPE_CONTAINS
	/// Returns `None` if there is no child corresponding to token PIPE_CONTAINS
	fn PIPE_CONTAINS(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PIPE_CONTAINS, 0)
	}
	fn lineFilter_all(&self) ->  Vec<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn lineFilter(&self, i: usize) -> Option<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves all `TerminalNode`s corresponding to token OR in current rule
	fn OR_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
		self.children_of_type()
	}
	/// Retrieves 'i's TerminalNode corresponding to token OR, starting from 0.
	/// Returns `None` if number of children corresponding to token OR is less or equal than `i`.
	fn OR(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_OR, i)
	}
}

impl<'input> LineFiltersContainsContextAttrs<'input> for LineFiltersContainsContext<'input>{}

pub struct LineFiltersContainsContextExt<'input>{
	base:LineFiltersContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LineFiltersContainsContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LineFiltersContainsContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFiltersContainsContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_lineFiltersContains(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_lineFiltersContains(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFiltersContainsContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_lineFiltersContains(self);
	}
}

impl<'input> CustomRuleContext<'input> for LineFiltersContainsContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilters }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilters }
}

impl<'input> Borrow<LineFiltersContextExt<'input>> for LineFiltersContainsContext<'input>{
	fn borrow(&self) -> &LineFiltersContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LineFiltersContextExt<'input>> for LineFiltersContainsContext<'input>{
	fn borrow_mut(&mut self) -> &mut LineFiltersContextExt<'input> { &mut self.base }
}

impl<'input> LineFiltersContextAttrs<'input> for LineFiltersContainsContext<'input> {}

impl<'input> LineFiltersContainsContextExt<'input>{
	fn new(ctx: &dyn LineFiltersContextAttrs<'input>) -> Rc<LineFiltersContextAll<'input>>  {
		Rc::new(
			LineFiltersContextAll::LineFiltersContainsContext(
				BaseParserRuleContext::copy_from(ctx,LineFiltersContainsContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LineFiltersNotPatternContext<'input> = BaseParserRuleContext<'input,LineFiltersNotPatternContextExt<'input>>;

pub trait LineFiltersNotPatternContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token PIPE_NPATTERN
	/// Returns `None` if there is no child corresponding to token PIPE_NPATTERN
	fn PIPE_NPATTERN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PIPE_NPATTERN, 0)
	}
	fn lineFilter_all(&self) ->  Vec<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn lineFilter(&self, i: usize) -> Option<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves all `TerminalNode`s corresponding to token OR in current rule
	fn OR_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
		self.children_of_type()
	}
	/// Retrieves 'i's TerminalNode corresponding to token OR, starting from 0.
	/// Returns `None` if number of children corresponding to token OR is less or equal than `i`.
	fn OR(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_OR, i)
	}
}

impl<'input> LineFiltersNotPatternContextAttrs<'input> for LineFiltersNotPatternContext<'input>{}

pub struct LineFiltersNotPatternContextExt<'input>{
	base:LineFiltersContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LineFiltersNotPatternContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LineFiltersNotPatternContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFiltersNotPatternContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_lineFiltersNotPattern(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_lineFiltersNotPattern(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFiltersNotPatternContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_lineFiltersNotPattern(self);
	}
}

impl<'input> CustomRuleContext<'input> for LineFiltersNotPatternContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilters }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilters }
}

impl<'input> Borrow<LineFiltersContextExt<'input>> for LineFiltersNotPatternContext<'input>{
	fn borrow(&self) -> &LineFiltersContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LineFiltersContextExt<'input>> for LineFiltersNotPatternContext<'input>{
	fn borrow_mut(&mut self) -> &mut LineFiltersContextExt<'input> { &mut self.base }
}

impl<'input> LineFiltersContextAttrs<'input> for LineFiltersNotPatternContext<'input> {}

impl<'input> LineFiltersNotPatternContextExt<'input>{
	fn new(ctx: &dyn LineFiltersContextAttrs<'input>) -> Rc<LineFiltersContextAll<'input>>  {
		Rc::new(
			LineFiltersContextAll::LineFiltersNotPatternContext(
				BaseParserRuleContext::copy_from(ctx,LineFiltersNotPatternContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LineFiltersNotContainsContext<'input> = BaseParserRuleContext<'input,LineFiltersNotContainsContextExt<'input>>;

pub trait LineFiltersNotContainsContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token NE
	/// Returns `None` if there is no child corresponding to token NE
	fn NE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_NE, 0)
	}
	fn lineFilter_all(&self) ->  Vec<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn lineFilter(&self, i: usize) -> Option<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves all `TerminalNode`s corresponding to token OR in current rule
	fn OR_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
		self.children_of_type()
	}
	/// Retrieves 'i's TerminalNode corresponding to token OR, starting from 0.
	/// Returns `None` if number of children corresponding to token OR is less or equal than `i`.
	fn OR(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_OR, i)
	}
}

impl<'input> LineFiltersNotContainsContextAttrs<'input> for LineFiltersNotContainsContext<'input>{}

pub struct LineFiltersNotContainsContextExt<'input>{
	base:LineFiltersContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LineFiltersNotContainsContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LineFiltersNotContainsContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFiltersNotContainsContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_lineFiltersNotContains(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_lineFiltersNotContains(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFiltersNotContainsContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_lineFiltersNotContains(self);
	}
}

impl<'input> CustomRuleContext<'input> for LineFiltersNotContainsContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilters }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilters }
}

impl<'input> Borrow<LineFiltersContextExt<'input>> for LineFiltersNotContainsContext<'input>{
	fn borrow(&self) -> &LineFiltersContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LineFiltersContextExt<'input>> for LineFiltersNotContainsContext<'input>{
	fn borrow_mut(&mut self) -> &mut LineFiltersContextExt<'input> { &mut self.base }
}

impl<'input> LineFiltersContextAttrs<'input> for LineFiltersNotContainsContext<'input> {}

impl<'input> LineFiltersNotContainsContextExt<'input>{
	fn new(ctx: &dyn LineFiltersContextAttrs<'input>) -> Rc<LineFiltersContextAll<'input>>  {
		Rc::new(
			LineFiltersContextAll::LineFiltersNotContainsContext(
				BaseParserRuleContext::copy_from(ctx,LineFiltersNotContainsContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LineFiltersPatternContext<'input> = BaseParserRuleContext<'input,LineFiltersPatternContextExt<'input>>;

pub trait LineFiltersPatternContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token PIPE_PATTERN
	/// Returns `None` if there is no child corresponding to token PIPE_PATTERN
	fn PIPE_PATTERN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PIPE_PATTERN, 0)
	}
	fn lineFilter_all(&self) ->  Vec<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn lineFilter(&self, i: usize) -> Option<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves all `TerminalNode`s corresponding to token OR in current rule
	fn OR_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
		self.children_of_type()
	}
	/// Retrieves 'i's TerminalNode corresponding to token OR, starting from 0.
	/// Returns `None` if number of children corresponding to token OR is less or equal than `i`.
	fn OR(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_OR, i)
	}
}

impl<'input> LineFiltersPatternContextAttrs<'input> for LineFiltersPatternContext<'input>{}

pub struct LineFiltersPatternContextExt<'input>{
	base:LineFiltersContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LineFiltersPatternContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LineFiltersPatternContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFiltersPatternContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_lineFiltersPattern(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_lineFiltersPattern(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFiltersPatternContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_lineFiltersPattern(self);
	}
}

impl<'input> CustomRuleContext<'input> for LineFiltersPatternContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilters }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilters }
}

impl<'input> Borrow<LineFiltersContextExt<'input>> for LineFiltersPatternContext<'input>{
	fn borrow(&self) -> &LineFiltersContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LineFiltersContextExt<'input>> for LineFiltersPatternContext<'input>{
	fn borrow_mut(&mut self) -> &mut LineFiltersContextExt<'input> { &mut self.base }
}

impl<'input> LineFiltersContextAttrs<'input> for LineFiltersPatternContext<'input> {}

impl<'input> LineFiltersPatternContextExt<'input>{
	fn new(ctx: &dyn LineFiltersContextAttrs<'input>) -> Rc<LineFiltersContextAll<'input>>  {
		Rc::new(
			LineFiltersContextAll::LineFiltersPatternContext(
				BaseParserRuleContext::copy_from(ctx,LineFiltersPatternContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LineFiltersNotMatchContext<'input> = BaseParserRuleContext<'input,LineFiltersNotMatchContextExt<'input>>;

pub trait LineFiltersNotMatchContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token NRE
	/// Returns `None` if there is no child corresponding to token NRE
	fn NRE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_NRE, 0)
	}
	fn lineFilter_all(&self) ->  Vec<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn lineFilter(&self, i: usize) -> Option<Rc<LineFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves all `TerminalNode`s corresponding to token OR in current rule
	fn OR_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
		self.children_of_type()
	}
	/// Retrieves 'i's TerminalNode corresponding to token OR, starting from 0.
	/// Returns `None` if number of children corresponding to token OR is less or equal than `i`.
	fn OR(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_OR, i)
	}
}

impl<'input> LineFiltersNotMatchContextAttrs<'input> for LineFiltersNotMatchContext<'input>{}

pub struct LineFiltersNotMatchContextExt<'input>{
	base:LineFiltersContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LineFiltersNotMatchContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LineFiltersNotMatchContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFiltersNotMatchContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_lineFiltersNotMatch(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_lineFiltersNotMatch(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFiltersNotMatchContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_lineFiltersNotMatch(self);
	}
}

impl<'input> CustomRuleContext<'input> for LineFiltersNotMatchContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilters }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilters }
}

impl<'input> Borrow<LineFiltersContextExt<'input>> for LineFiltersNotMatchContext<'input>{
	fn borrow(&self) -> &LineFiltersContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LineFiltersContextExt<'input>> for LineFiltersNotMatchContext<'input>{
	fn borrow_mut(&mut self) -> &mut LineFiltersContextExt<'input> { &mut self.base }
}

impl<'input> LineFiltersContextAttrs<'input> for LineFiltersNotMatchContext<'input> {}

impl<'input> LineFiltersNotMatchContextExt<'input>{
	fn new(ctx: &dyn LineFiltersContextAttrs<'input>) -> Rc<LineFiltersContextAll<'input>>  {
		Rc::new(
			LineFiltersContextAll::LineFiltersNotMatchContext(
				BaseParserRuleContext::copy_from(ctx,LineFiltersNotMatchContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn lineFilters(&mut self,)
	-> Result<Rc<LineFiltersContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LineFiltersContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 16, RULE_lineFilters);
        let mut _localctx: Rc<LineFiltersContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			let mut _alt: i32;
			recog.base.set_state(256);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LogQLParser_PIPE_CONTAINS 
				=> {
					let tmp = LineFiltersContainsContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(202);
					recog.base.match_token(LogQLParser_PIPE_CONTAINS,&mut recog.err_handler)?;

					/*InvokeRule lineFilter*/
					recog.base.set_state(203);
					recog.lineFilter()?;

					recog.base.set_state(208);
					recog.err_handler.sync(&mut recog.base)?;
					_alt = recog.interpreter.adaptive_predict(11,&mut recog.base)?;
					while { _alt!=2 && _alt!=INVALID_ALT } {
						if _alt==1 {
							{
							{
							recog.base.set_state(204);
							recog.base.match_token(LogQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule lineFilter*/
							recog.base.set_state(205);
							recog.lineFilter()?;

							}
							} 
						}
						recog.base.set_state(210);
						recog.err_handler.sync(&mut recog.base)?;
						_alt = recog.interpreter.adaptive_predict(11,&mut recog.base)?;
					}
					}
				}

			LogQLParser_NE 
				=> {
					let tmp = LineFiltersNotContainsContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(211);
					recog.base.match_token(LogQLParser_NE,&mut recog.err_handler)?;

					/*InvokeRule lineFilter*/
					recog.base.set_state(212);
					recog.lineFilter()?;

					recog.base.set_state(217);
					recog.err_handler.sync(&mut recog.base)?;
					_alt = recog.interpreter.adaptive_predict(12,&mut recog.base)?;
					while { _alt!=2 && _alt!=INVALID_ALT } {
						if _alt==1 {
							{
							{
							recog.base.set_state(213);
							recog.base.match_token(LogQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule lineFilter*/
							recog.base.set_state(214);
							recog.lineFilter()?;

							}
							} 
						}
						recog.base.set_state(219);
						recog.err_handler.sync(&mut recog.base)?;
						_alt = recog.interpreter.adaptive_predict(12,&mut recog.base)?;
					}
					}
				}

			LogQLParser_PIPE_MATCH 
				=> {
					let tmp = LineFiltersMatchContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					recog.base.set_state(220);
					recog.base.match_token(LogQLParser_PIPE_MATCH,&mut recog.err_handler)?;

					/*InvokeRule lineFilter*/
					recog.base.set_state(221);
					recog.lineFilter()?;

					recog.base.set_state(226);
					recog.err_handler.sync(&mut recog.base)?;
					_alt = recog.interpreter.adaptive_predict(13,&mut recog.base)?;
					while { _alt!=2 && _alt!=INVALID_ALT } {
						if _alt==1 {
							{
							{
							recog.base.set_state(222);
							recog.base.match_token(LogQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule lineFilter*/
							recog.base.set_state(223);
							recog.lineFilter()?;

							}
							} 
						}
						recog.base.set_state(228);
						recog.err_handler.sync(&mut recog.base)?;
						_alt = recog.interpreter.adaptive_predict(13,&mut recog.base)?;
					}
					}
				}

			LogQLParser_NRE 
				=> {
					let tmp = LineFiltersNotMatchContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 4)?;
					_localctx = tmp;
					{
					recog.base.set_state(229);
					recog.base.match_token(LogQLParser_NRE,&mut recog.err_handler)?;

					/*InvokeRule lineFilter*/
					recog.base.set_state(230);
					recog.lineFilter()?;

					recog.base.set_state(235);
					recog.err_handler.sync(&mut recog.base)?;
					_alt = recog.interpreter.adaptive_predict(14,&mut recog.base)?;
					while { _alt!=2 && _alt!=INVALID_ALT } {
						if _alt==1 {
							{
							{
							recog.base.set_state(231);
							recog.base.match_token(LogQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule lineFilter*/
							recog.base.set_state(232);
							recog.lineFilter()?;

							}
							} 
						}
						recog.base.set_state(237);
						recog.err_handler.sync(&mut recog.base)?;
						_alt = recog.interpreter.adaptive_predict(14,&mut recog.base)?;
					}
					}
				}

			LogQLParser_PIPE_PATTERN 
				=> {
					let tmp = LineFiltersPatternContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 5)?;
					_localctx = tmp;
					{
					recog.base.set_state(238);
					recog.base.match_token(LogQLParser_PIPE_PATTERN,&mut recog.err_handler)?;

					/*InvokeRule lineFilter*/
					recog.base.set_state(239);
					recog.lineFilter()?;

					recog.base.set_state(244);
					recog.err_handler.sync(&mut recog.base)?;
					_alt = recog.interpreter.adaptive_predict(15,&mut recog.base)?;
					while { _alt!=2 && _alt!=INVALID_ALT } {
						if _alt==1 {
							{
							{
							recog.base.set_state(240);
							recog.base.match_token(LogQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule lineFilter*/
							recog.base.set_state(241);
							recog.lineFilter()?;

							}
							} 
						}
						recog.base.set_state(246);
						recog.err_handler.sync(&mut recog.base)?;
						_alt = recog.interpreter.adaptive_predict(15,&mut recog.base)?;
					}
					}
				}

			LogQLParser_PIPE_NPATTERN 
				=> {
					let tmp = LineFiltersNotPatternContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 6)?;
					_localctx = tmp;
					{
					recog.base.set_state(247);
					recog.base.match_token(LogQLParser_PIPE_NPATTERN,&mut recog.err_handler)?;

					/*InvokeRule lineFilter*/
					recog.base.set_state(248);
					recog.lineFilter()?;

					recog.base.set_state(253);
					recog.err_handler.sync(&mut recog.base)?;
					_alt = recog.interpreter.adaptive_predict(16,&mut recog.base)?;
					while { _alt!=2 && _alt!=INVALID_ALT } {
						if _alt==1 {
							{
							{
							recog.base.set_state(249);
							recog.base.match_token(LogQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule lineFilter*/
							recog.base.set_state(250);
							recog.lineFilter()?;

							}
							} 
						}
						recog.base.set_state(255);
						recog.err_handler.sync(&mut recog.base)?;
						_alt = recog.interpreter.adaptive_predict(16,&mut recog.base)?;
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
//------------------- lineFilter ----------------
#[derive(Debug)]
pub enum LineFilterContextAll<'input>{
	LineFilterStringContext(LineFilterStringContext<'input>),
	LineFilterIpContext(LineFilterIpContext<'input>),
Error(LineFilterContext<'input>)
}
antlr4rust::tid!{LineFilterContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for LineFilterContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for LineFilterContextAll<'input>{}

impl<'input> Deref for LineFilterContextAll<'input>{
	type Target = dyn LineFilterContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use LineFilterContextAll::*;
		match self{
			LineFilterStringContext(inner) => inner,
			LineFilterIpContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFilterContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFilterContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type LineFilterContext<'input> = BaseParserRuleContext<'input,LineFilterContextExt<'input>>;

#[derive(Clone)]
pub struct LineFilterContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LineFilterContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFilterContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFilterContext<'input>{
}

impl<'input> CustomRuleContext<'input> for LineFilterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilter }
}
antlr4rust::tid!{LineFilterContextExt<'a>}

impl<'input> LineFilterContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LineFilterContextAll<'input>> {
		Rc::new(
		LineFilterContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LineFilterContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait LineFilterContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LineFilterContextExt<'input>>{


}

impl<'input> LineFilterContextAttrs<'input> for LineFilterContext<'input>{}

pub type LineFilterStringContext<'input> = BaseParserRuleContext<'input,LineFilterStringContextExt<'input>>;

pub trait LineFilterStringContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token STRING
	/// Returns `None` if there is no child corresponding to token STRING
	fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_STRING, 0)
	}
}

impl<'input> LineFilterStringContextAttrs<'input> for LineFilterStringContext<'input>{}

pub struct LineFilterStringContextExt<'input>{
	base:LineFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LineFilterStringContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LineFilterStringContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFilterStringContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_lineFilterString(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_lineFilterString(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFilterStringContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_lineFilterString(self);
	}
}

impl<'input> CustomRuleContext<'input> for LineFilterStringContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilter }
}

impl<'input> Borrow<LineFilterContextExt<'input>> for LineFilterStringContext<'input>{
	fn borrow(&self) -> &LineFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LineFilterContextExt<'input>> for LineFilterStringContext<'input>{
	fn borrow_mut(&mut self) -> &mut LineFilterContextExt<'input> { &mut self.base }
}

impl<'input> LineFilterContextAttrs<'input> for LineFilterStringContext<'input> {}

impl<'input> LineFilterStringContextExt<'input>{
	fn new(ctx: &dyn LineFilterContextAttrs<'input>) -> Rc<LineFilterContextAll<'input>>  {
		Rc::new(
			LineFilterContextAll::LineFilterStringContext(
				BaseParserRuleContext::copy_from(ctx,LineFilterStringContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LineFilterIpContext<'input> = BaseParserRuleContext<'input,LineFilterIpContextExt<'input>>;

pub trait LineFilterIpContextAttrs<'input>: LogQLParserContext<'input>{
	fn ipFn(&self) -> Option<Rc<IpFnContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> LineFilterIpContextAttrs<'input> for LineFilterIpContext<'input>{}

pub struct LineFilterIpContextExt<'input>{
	base:LineFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LineFilterIpContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LineFilterIpContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFilterIpContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_lineFilterIp(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_lineFilterIp(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFilterIpContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_lineFilterIp(self);
	}
}

impl<'input> CustomRuleContext<'input> for LineFilterIpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFilter }
}

impl<'input> Borrow<LineFilterContextExt<'input>> for LineFilterIpContext<'input>{
	fn borrow(&self) -> &LineFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LineFilterContextExt<'input>> for LineFilterIpContext<'input>{
	fn borrow_mut(&mut self) -> &mut LineFilterContextExt<'input> { &mut self.base }
}

impl<'input> LineFilterContextAttrs<'input> for LineFilterIpContext<'input> {}

impl<'input> LineFilterIpContextExt<'input>{
	fn new(ctx: &dyn LineFilterContextAttrs<'input>) -> Rc<LineFilterContextAll<'input>>  {
		Rc::new(
			LineFilterContextAll::LineFilterIpContext(
				BaseParserRuleContext::copy_from(ctx,LineFilterIpContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn lineFilter(&mut self,)
	-> Result<Rc<LineFilterContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LineFilterContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 18, RULE_lineFilter);
        let mut _localctx: Rc<LineFilterContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(260);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LogQLParser_STRING 
				=> {
					let tmp = LineFilterStringContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(258);
					recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

					}
				}

			LogQLParser_IP 
				=> {
					let tmp = LineFilterIpContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					/*InvokeRule ipFn*/
					recog.base.set_state(259);
					recog.ipFn()?;

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
//------------------- ipFn ----------------
pub type IpFnContextAll<'input> = IpFnContext<'input>;


pub type IpFnContext<'input> = BaseParserRuleContext<'input,IpFnContextExt<'input>>;

#[derive(Clone)]
pub struct IpFnContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for IpFnContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for IpFnContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_ipFn(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_ipFn(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for IpFnContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_ipFn(self);
	}
}

impl<'input> CustomRuleContext<'input> for IpFnContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_ipFn }
	//fn type_rule_index() -> usize where Self: Sized { RULE_ipFn }
}
antlr4rust::tid!{IpFnContextExt<'a>}

impl<'input> IpFnContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<IpFnContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,IpFnContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait IpFnContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<IpFnContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token IP
/// Returns `None` if there is no child corresponding to token IP
fn IP(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_IP, 0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token STRING
/// Returns `None` if there is no child corresponding to token STRING
fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_STRING, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RPAREN, 0)
}

}

impl<'input> IpFnContextAttrs<'input> for IpFnContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn ipFn(&mut self,)
	-> Result<Rc<IpFnContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = IpFnContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 20, RULE_ipFn);
        let mut _localctx: Rc<IpFnContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(262);
			recog.base.match_token(LogQLParser_IP,&mut recog.err_handler)?;

			recog.base.set_state(263);
			recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

			recog.base.set_state(264);
			recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

			recog.base.set_state(265);
			recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- regexpParser ----------------
pub type RegexpParserContextAll<'input> = RegexpParserContext<'input>;


pub type RegexpParserContext<'input> = BaseParserRuleContext<'input,RegexpParserContextExt<'input>>;

#[derive(Clone)]
pub struct RegexpParserContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for RegexpParserContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RegexpParserContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_regexpParser(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_regexpParser(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RegexpParserContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_regexpParser(self);
	}
}

impl<'input> CustomRuleContext<'input> for RegexpParserContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_regexpParser }
	//fn type_rule_index() -> usize where Self: Sized { RULE_regexpParser }
}
antlr4rust::tid!{RegexpParserContextExt<'a>}

impl<'input> RegexpParserContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RegexpParserContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RegexpParserContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RegexpParserContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<RegexpParserContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token REGEXP
/// Returns `None` if there is no child corresponding to token REGEXP
fn REGEXP(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_REGEXP, 0)
}
/// Retrieves first TerminalNode corresponding to token STRING
/// Returns `None` if there is no child corresponding to token STRING
fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_STRING, 0)
}

}

impl<'input> RegexpParserContextAttrs<'input> for RegexpParserContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn regexpParser(&mut self,)
	-> Result<Rc<RegexpParserContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RegexpParserContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 22, RULE_regexpParser);
        let mut _localctx: Rc<RegexpParserContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(267);
			recog.base.match_token(LogQLParser_REGEXP,&mut recog.err_handler)?;

			recog.base.set_state(268);
			recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

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
//------------------- patternParser ----------------
pub type PatternParserContextAll<'input> = PatternParserContext<'input>;


pub type PatternParserContext<'input> = BaseParserRuleContext<'input,PatternParserContextExt<'input>>;

#[derive(Clone)]
pub struct PatternParserContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for PatternParserContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for PatternParserContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_patternParser(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_patternParser(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for PatternParserContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_patternParser(self);
	}
}

impl<'input> CustomRuleContext<'input> for PatternParserContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_patternParser }
	//fn type_rule_index() -> usize where Self: Sized { RULE_patternParser }
}
antlr4rust::tid!{PatternParserContextExt<'a>}

impl<'input> PatternParserContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<PatternParserContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,PatternParserContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait PatternParserContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<PatternParserContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token PATTERN
/// Returns `None` if there is no child corresponding to token PATTERN
fn PATTERN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_PATTERN, 0)
}
/// Retrieves first TerminalNode corresponding to token STRING
/// Returns `None` if there is no child corresponding to token STRING
fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_STRING, 0)
}

}

impl<'input> PatternParserContextAttrs<'input> for PatternParserContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn patternParser(&mut self,)
	-> Result<Rc<PatternParserContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = PatternParserContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 24, RULE_patternParser);
        let mut _localctx: Rc<PatternParserContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(270);
			recog.base.match_token(LogQLParser_PATTERN,&mut recog.err_handler)?;

			recog.base.set_state(271);
			recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

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
//------------------- unpackParser ----------------
pub type UnpackParserContextAll<'input> = UnpackParserContext<'input>;


pub type UnpackParserContext<'input> = BaseParserRuleContext<'input,UnpackParserContextExt<'input>>;

#[derive(Clone)]
pub struct UnpackParserContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for UnpackParserContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for UnpackParserContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_unpackParser(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_unpackParser(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for UnpackParserContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_unpackParser(self);
	}
}

impl<'input> CustomRuleContext<'input> for UnpackParserContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_unpackParser }
	//fn type_rule_index() -> usize where Self: Sized { RULE_unpackParser }
}
antlr4rust::tid!{UnpackParserContextExt<'a>}

impl<'input> UnpackParserContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<UnpackParserContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,UnpackParserContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait UnpackParserContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<UnpackParserContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token UNPACK
/// Returns `None` if there is no child corresponding to token UNPACK
fn UNPACK(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_UNPACK, 0)
}

}

impl<'input> UnpackParserContextAttrs<'input> for UnpackParserContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn unpackParser(&mut self,)
	-> Result<Rc<UnpackParserContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = UnpackParserContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 26, RULE_unpackParser);
        let mut _localctx: Rc<UnpackParserContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(273);
			recog.base.match_token(LogQLParser_UNPACK,&mut recog.err_handler)?;

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
//------------------- logfmtParser ----------------
pub type LogfmtParserContextAll<'input> = LogfmtParserContext<'input>;


pub type LogfmtParserContext<'input> = BaseParserRuleContext<'input,LogfmtParserContextExt<'input>>;

#[derive(Clone)]
pub struct LogfmtParserContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LogfmtParserContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LogfmtParserContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_logfmtParser(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_logfmtParser(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LogfmtParserContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_logfmtParser(self);
	}
}

impl<'input> CustomRuleContext<'input> for LogfmtParserContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_logfmtParser }
	//fn type_rule_index() -> usize where Self: Sized { RULE_logfmtParser }
}
antlr4rust::tid!{LogfmtParserContextExt<'a>}

impl<'input> LogfmtParserContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LogfmtParserContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LogfmtParserContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LogfmtParserContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LogfmtParserContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LOGFMT
/// Returns `None` if there is no child corresponding to token LOGFMT
fn LOGFMT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LOGFMT, 0)
}
/// Retrieves all `TerminalNode`s corresponding to token LOGFMT_FLAG in current rule
fn LOGFMT_FLAG_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token LOGFMT_FLAG, starting from 0.
/// Returns `None` if number of children corresponding to token LOGFMT_FLAG is less or equal than `i`.
fn LOGFMT_FLAG(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LOGFMT_FLAG, i)
}
fn labelExtractions(&self) -> Option<Rc<LabelExtractionsContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> LogfmtParserContextAttrs<'input> for LogfmtParserContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn logfmtParser(&mut self,)
	-> Result<Rc<LogfmtParserContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LogfmtParserContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 28, RULE_logfmtParser);
        let mut _localctx: Rc<LogfmtParserContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(275);
			recog.base.match_token(LogQLParser_LOGFMT,&mut recog.err_handler)?;

			recog.base.set_state(279);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(19,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					{
					{
					recog.base.set_state(276);
					recog.base.match_token(LogQLParser_LOGFMT_FLAG,&mut recog.err_handler)?;

					}
					} 
				}
				recog.base.set_state(281);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(19,&mut recog.base)?;
			}
			recog.base.set_state(283);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(20,&mut recog.base)? {
				x if x == 1=>{
					{
					/*InvokeRule labelExtractions*/
					recog.base.set_state(282);
					recog.labelExtractions()?;

					}
				}

				_ => {}
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
//------------------- labelFormatExpr ----------------
pub type LabelFormatExprContextAll<'input> = LabelFormatExprContext<'input>;


pub type LabelFormatExprContext<'input> = BaseParserRuleContext<'input,LabelFormatExprContextExt<'input>>;

#[derive(Clone)]
pub struct LabelFormatExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LabelFormatExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFormatExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelFormatExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelFormatExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFormatExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFormatExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFormatExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFormatExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFormatExpr }
}
antlr4rust::tid!{LabelFormatExprContextExt<'a>}

impl<'input> LabelFormatExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelFormatExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelFormatExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelFormatExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LabelFormatExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LABEL_FORMAT
/// Returns `None` if there is no child corresponding to token LABEL_FORMAT
fn LABEL_FORMAT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LABEL_FORMAT, 0)
}
fn labelFormatOps(&self) -> Option<Rc<LabelFormatOpsContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> LabelFormatExprContextAttrs<'input> for LabelFormatExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelFormatExpr(&mut self,)
	-> Result<Rc<LabelFormatExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelFormatExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 30, RULE_labelFormatExpr);
        let mut _localctx: Rc<LabelFormatExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(285);
			recog.base.match_token(LogQLParser_LABEL_FORMAT,&mut recog.err_handler)?;

			/*InvokeRule labelFormatOps*/
			recog.base.set_state(286);
			recog.labelFormatOps()?;

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
//------------------- labelFormatOps ----------------
pub type LabelFormatOpsContextAll<'input> = LabelFormatOpsContext<'input>;


pub type LabelFormatOpsContext<'input> = BaseParserRuleContext<'input,LabelFormatOpsContextExt<'input>>;

#[derive(Clone)]
pub struct LabelFormatOpsContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LabelFormatOpsContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFormatOpsContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelFormatOps(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelFormatOps(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFormatOpsContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFormatOps(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFormatOpsContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFormatOps }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFormatOps }
}
antlr4rust::tid!{LabelFormatOpsContextExt<'a>}

impl<'input> LabelFormatOpsContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelFormatOpsContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelFormatOpsContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelFormatOpsContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LabelFormatOpsContextExt<'input>>{

fn labelFormatOp_all(&self) ->  Vec<Rc<LabelFormatOpContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn labelFormatOp(&self, i: usize) -> Option<Rc<LabelFormatOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COMMA, i)
}

}

impl<'input> LabelFormatOpsContextAttrs<'input> for LabelFormatOpsContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelFormatOps(&mut self,)
	-> Result<Rc<LabelFormatOpsContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelFormatOpsContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 32, RULE_labelFormatOps);
        let mut _localctx: Rc<LabelFormatOpsContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule labelFormatOp*/
			recog.base.set_state(288);
			recog.labelFormatOp()?;

			recog.base.set_state(293);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(21,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					{
					{
					recog.base.set_state(289);
					recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule labelFormatOp*/
					recog.base.set_state(290);
					recog.labelFormatOp()?;

					}
					} 
				}
				recog.base.set_state(295);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(21,&mut recog.base)?;
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
//------------------- labelFormatOp ----------------
#[derive(Debug)]
pub enum LabelFormatOpContextAll<'input>{
	LabelFormatRenameContext(LabelFormatRenameContext<'input>),
	LabelFormatTemplateContext(LabelFormatTemplateContext<'input>),
Error(LabelFormatOpContext<'input>)
}
antlr4rust::tid!{LabelFormatOpContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for LabelFormatOpContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for LabelFormatOpContextAll<'input>{}

impl<'input> Deref for LabelFormatOpContextAll<'input>{
	type Target = dyn LabelFormatOpContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use LabelFormatOpContextAll::*;
		match self{
			LabelFormatRenameContext(inner) => inner,
			LabelFormatTemplateContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFormatOpContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFormatOpContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type LabelFormatOpContext<'input> = BaseParserRuleContext<'input,LabelFormatOpContextExt<'input>>;

#[derive(Clone)]
pub struct LabelFormatOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LabelFormatOpContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFormatOpContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFormatOpContext<'input>{
}

impl<'input> CustomRuleContext<'input> for LabelFormatOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFormatOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFormatOp }
}
antlr4rust::tid!{LabelFormatOpContextExt<'a>}

impl<'input> LabelFormatOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelFormatOpContextAll<'input>> {
		Rc::new(
		LabelFormatOpContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelFormatOpContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait LabelFormatOpContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LabelFormatOpContextExt<'input>>{


}

impl<'input> LabelFormatOpContextAttrs<'input> for LabelFormatOpContext<'input>{}

pub type LabelFormatRenameContext<'input> = BaseParserRuleContext<'input,LabelFormatRenameContextExt<'input>>;

pub trait LabelFormatRenameContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves all `TerminalNode`s corresponding to token ATTRIBUTE in current rule
	fn ATTRIBUTE_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
		self.children_of_type()
	}
	/// Retrieves 'i's TerminalNode corresponding to token ATTRIBUTE, starting from 0.
	/// Returns `None` if number of children corresponding to token ATTRIBUTE is less or equal than `i`.
	fn ATTRIBUTE(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, i)
	}
	/// Retrieves first TerminalNode corresponding to token EQ
	/// Returns `None` if there is no child corresponding to token EQ
	fn EQ(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_EQ, 0)
	}
}

impl<'input> LabelFormatRenameContextAttrs<'input> for LabelFormatRenameContext<'input>{}

pub struct LabelFormatRenameContextExt<'input>{
	base:LabelFormatOpContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFormatRenameContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFormatRenameContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFormatRenameContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFormatRename(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFormatRename(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFormatRenameContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFormatRename(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFormatRenameContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFormatOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFormatOp }
}

impl<'input> Borrow<LabelFormatOpContextExt<'input>> for LabelFormatRenameContext<'input>{
	fn borrow(&self) -> &LabelFormatOpContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFormatOpContextExt<'input>> for LabelFormatRenameContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFormatOpContextExt<'input> { &mut self.base }
}

impl<'input> LabelFormatOpContextAttrs<'input> for LabelFormatRenameContext<'input> {}

impl<'input> LabelFormatRenameContextExt<'input>{
	fn new(ctx: &dyn LabelFormatOpContextAttrs<'input>) -> Rc<LabelFormatOpContextAll<'input>>  {
		Rc::new(
			LabelFormatOpContextAll::LabelFormatRenameContext(
				BaseParserRuleContext::copy_from(ctx,LabelFormatRenameContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LabelFormatTemplateContext<'input> = BaseParserRuleContext<'input,LabelFormatTemplateContextExt<'input>>;

pub trait LabelFormatTemplateContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
	/// Returns `None` if there is no child corresponding to token ATTRIBUTE
	fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token EQ
	/// Returns `None` if there is no child corresponding to token EQ
	fn EQ(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_EQ, 0)
	}
	/// Retrieves first TerminalNode corresponding to token STRING
	/// Returns `None` if there is no child corresponding to token STRING
	fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_STRING, 0)
	}
}

impl<'input> LabelFormatTemplateContextAttrs<'input> for LabelFormatTemplateContext<'input>{}

pub struct LabelFormatTemplateContextExt<'input>{
	base:LabelFormatOpContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFormatTemplateContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFormatTemplateContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFormatTemplateContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFormatTemplate(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFormatTemplate(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFormatTemplateContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFormatTemplate(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFormatTemplateContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFormatOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFormatOp }
}

impl<'input> Borrow<LabelFormatOpContextExt<'input>> for LabelFormatTemplateContext<'input>{
	fn borrow(&self) -> &LabelFormatOpContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFormatOpContextExt<'input>> for LabelFormatTemplateContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFormatOpContextExt<'input> { &mut self.base }
}

impl<'input> LabelFormatOpContextAttrs<'input> for LabelFormatTemplateContext<'input> {}

impl<'input> LabelFormatTemplateContextExt<'input>{
	fn new(ctx: &dyn LabelFormatOpContextAttrs<'input>) -> Rc<LabelFormatOpContextAll<'input>>  {
		Rc::new(
			LabelFormatOpContextAll::LabelFormatTemplateContext(
				BaseParserRuleContext::copy_from(ctx,LabelFormatTemplateContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelFormatOp(&mut self,)
	-> Result<Rc<LabelFormatOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelFormatOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 34, RULE_labelFormatOp);
        let mut _localctx: Rc<LabelFormatOpContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(302);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(22,&mut recog.base)? {
				1 =>{
					let tmp = LabelFormatRenameContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(296);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(297);
					recog.base.match_token(LogQLParser_EQ,&mut recog.err_handler)?;

					recog.base.set_state(298);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					let tmp = LabelFormatTemplateContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(299);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(300);
					recog.base.match_token(LogQLParser_EQ,&mut recog.err_handler)?;

					recog.base.set_state(301);
					recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

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
//------------------- lineFormatExpr ----------------
pub type LineFormatExprContextAll<'input> = LineFormatExprContext<'input>;


pub type LineFormatExprContext<'input> = BaseParserRuleContext<'input,LineFormatExprContextExt<'input>>;

#[derive(Clone)]
pub struct LineFormatExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LineFormatExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LineFormatExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_lineFormatExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_lineFormatExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LineFormatExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_lineFormatExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for LineFormatExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_lineFormatExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_lineFormatExpr }
}
antlr4rust::tid!{LineFormatExprContextExt<'a>}

impl<'input> LineFormatExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LineFormatExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LineFormatExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LineFormatExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LineFormatExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LINE_FORMAT
/// Returns `None` if there is no child corresponding to token LINE_FORMAT
fn LINE_FORMAT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LINE_FORMAT, 0)
}
/// Retrieves first TerminalNode corresponding to token STRING
/// Returns `None` if there is no child corresponding to token STRING
fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_STRING, 0)
}

}

impl<'input> LineFormatExprContextAttrs<'input> for LineFormatExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn lineFormatExpr(&mut self,)
	-> Result<Rc<LineFormatExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LineFormatExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 36, RULE_lineFormatExpr);
        let mut _localctx: Rc<LineFormatExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(304);
			recog.base.match_token(LogQLParser_LINE_FORMAT,&mut recog.err_handler)?;

			recog.base.set_state(305);
			recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

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
//------------------- decolorizeExpr ----------------
pub type DecolorizeExprContextAll<'input> = DecolorizeExprContext<'input>;


pub type DecolorizeExprContext<'input> = BaseParserRuleContext<'input,DecolorizeExprContextExt<'input>>;

#[derive(Clone)]
pub struct DecolorizeExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for DecolorizeExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for DecolorizeExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_decolorizeExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_decolorizeExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for DecolorizeExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_decolorizeExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for DecolorizeExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_decolorizeExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_decolorizeExpr }
}
antlr4rust::tid!{DecolorizeExprContextExt<'a>}

impl<'input> DecolorizeExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<DecolorizeExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,DecolorizeExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait DecolorizeExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<DecolorizeExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token DECOLORIZE
/// Returns `None` if there is no child corresponding to token DECOLORIZE
fn DECOLORIZE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_DECOLORIZE, 0)
}

}

impl<'input> DecolorizeExprContextAttrs<'input> for DecolorizeExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn decolorizeExpr(&mut self,)
	-> Result<Rc<DecolorizeExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = DecolorizeExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 38, RULE_decolorizeExpr);
        let mut _localctx: Rc<DecolorizeExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(307);
			recog.base.match_token(LogQLParser_DECOLORIZE,&mut recog.err_handler)?;

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
//------------------- dropExpr ----------------
pub type DropExprContextAll<'input> = DropExprContext<'input>;


pub type DropExprContext<'input> = BaseParserRuleContext<'input,DropExprContextExt<'input>>;

#[derive(Clone)]
pub struct DropExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for DropExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for DropExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_dropExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_dropExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for DropExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_dropExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for DropExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_dropExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_dropExpr }
}
antlr4rust::tid!{DropExprContextExt<'a>}

impl<'input> DropExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<DropExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,DropExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait DropExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<DropExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token DROP
/// Returns `None` if there is no child corresponding to token DROP
fn DROP(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_DROP, 0)
}
fn labelExtractions(&self) -> Option<Rc<LabelExtractionsContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> DropExprContextAttrs<'input> for DropExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn dropExpr(&mut self,)
	-> Result<Rc<DropExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = DropExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 40, RULE_dropExpr);
        let mut _localctx: Rc<DropExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(309);
			recog.base.match_token(LogQLParser_DROP,&mut recog.err_handler)?;

			/*InvokeRule labelExtractions*/
			recog.base.set_state(310);
			recog.labelExtractions()?;

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
//------------------- keepExpr ----------------
pub type KeepExprContextAll<'input> = KeepExprContext<'input>;


pub type KeepExprContext<'input> = BaseParserRuleContext<'input,KeepExprContextExt<'input>>;

#[derive(Clone)]
pub struct KeepExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for KeepExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for KeepExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_keepExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_keepExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for KeepExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_keepExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for KeepExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_keepExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_keepExpr }
}
antlr4rust::tid!{KeepExprContextExt<'a>}

impl<'input> KeepExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<KeepExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,KeepExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait KeepExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<KeepExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token KEEP
/// Returns `None` if there is no child corresponding to token KEEP
fn KEEP(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_KEEP, 0)
}
fn labelExtractions(&self) -> Option<Rc<LabelExtractionsContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> KeepExprContextAttrs<'input> for KeepExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn keepExpr(&mut self,)
	-> Result<Rc<KeepExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = KeepExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 42, RULE_keepExpr);
        let mut _localctx: Rc<KeepExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(312);
			recog.base.match_token(LogQLParser_KEEP,&mut recog.err_handler)?;

			/*InvokeRule labelExtractions*/
			recog.base.set_state(313);
			recog.labelExtractions()?;

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
//------------------- jsonParser ----------------
pub type JsonParserContextAll<'input> = JsonParserContext<'input>;


pub type JsonParserContext<'input> = BaseParserRuleContext<'input,JsonParserContextExt<'input>>;

#[derive(Clone)]
pub struct JsonParserContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for JsonParserContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for JsonParserContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_jsonParser(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_jsonParser(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for JsonParserContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_jsonParser(self);
	}
}

impl<'input> CustomRuleContext<'input> for JsonParserContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_jsonParser }
	//fn type_rule_index() -> usize where Self: Sized { RULE_jsonParser }
}
antlr4rust::tid!{JsonParserContextExt<'a>}

impl<'input> JsonParserContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<JsonParserContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,JsonParserContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait JsonParserContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<JsonParserContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token JSON
/// Returns `None` if there is no child corresponding to token JSON
fn JSON(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_JSON, 0)
}
fn labelExtractions(&self) -> Option<Rc<LabelExtractionsContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> JsonParserContextAttrs<'input> for JsonParserContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn jsonParser(&mut self,)
	-> Result<Rc<JsonParserContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = JsonParserContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 44, RULE_jsonParser);
        let mut _localctx: Rc<JsonParserContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(315);
			recog.base.match_token(LogQLParser_JSON,&mut recog.err_handler)?;

			recog.base.set_state(317);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(23,&mut recog.base)? {
				x if x == 1=>{
					{
					/*InvokeRule labelExtractions*/
					recog.base.set_state(316);
					recog.labelExtractions()?;

					}
				}

				_ => {}
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
//------------------- labelExtractionExpr ----------------
#[derive(Debug)]
pub enum LabelExtractionExprContextAll<'input>{
	LabelExtractionSimpleContext(LabelExtractionSimpleContext<'input>),
	LabelExtractionWithPathContext(LabelExtractionWithPathContext<'input>),
Error(LabelExtractionExprContext<'input>)
}
antlr4rust::tid!{LabelExtractionExprContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for LabelExtractionExprContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for LabelExtractionExprContextAll<'input>{}

impl<'input> Deref for LabelExtractionExprContextAll<'input>{
	type Target = dyn LabelExtractionExprContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use LabelExtractionExprContextAll::*;
		match self{
			LabelExtractionSimpleContext(inner) => inner,
			LabelExtractionWithPathContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelExtractionExprContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelExtractionExprContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type LabelExtractionExprContext<'input> = BaseParserRuleContext<'input,LabelExtractionExprContextExt<'input>>;

#[derive(Clone)]
pub struct LabelExtractionExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LabelExtractionExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelExtractionExprContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelExtractionExprContext<'input>{
}

impl<'input> CustomRuleContext<'input> for LabelExtractionExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelExtractionExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelExtractionExpr }
}
antlr4rust::tid!{LabelExtractionExprContextExt<'a>}

impl<'input> LabelExtractionExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelExtractionExprContextAll<'input>> {
		Rc::new(
		LabelExtractionExprContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelExtractionExprContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait LabelExtractionExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LabelExtractionExprContextExt<'input>>{


}

impl<'input> LabelExtractionExprContextAttrs<'input> for LabelExtractionExprContext<'input>{}

pub type LabelExtractionSimpleContext<'input> = BaseParserRuleContext<'input,LabelExtractionSimpleContextExt<'input>>;

pub trait LabelExtractionSimpleContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
	/// Returns `None` if there is no child corresponding to token ATTRIBUTE
	fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, 0)
	}
}

impl<'input> LabelExtractionSimpleContextAttrs<'input> for LabelExtractionSimpleContext<'input>{}

pub struct LabelExtractionSimpleContextExt<'input>{
	base:LabelExtractionExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelExtractionSimpleContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelExtractionSimpleContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelExtractionSimpleContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelExtractionSimple(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelExtractionSimple(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelExtractionSimpleContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelExtractionSimple(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelExtractionSimpleContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelExtractionExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelExtractionExpr }
}

impl<'input> Borrow<LabelExtractionExprContextExt<'input>> for LabelExtractionSimpleContext<'input>{
	fn borrow(&self) -> &LabelExtractionExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelExtractionExprContextExt<'input>> for LabelExtractionSimpleContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelExtractionExprContextExt<'input> { &mut self.base }
}

impl<'input> LabelExtractionExprContextAttrs<'input> for LabelExtractionSimpleContext<'input> {}

impl<'input> LabelExtractionSimpleContextExt<'input>{
	fn new(ctx: &dyn LabelExtractionExprContextAttrs<'input>) -> Rc<LabelExtractionExprContextAll<'input>>  {
		Rc::new(
			LabelExtractionExprContextAll::LabelExtractionSimpleContext(
				BaseParserRuleContext::copy_from(ctx,LabelExtractionSimpleContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LabelExtractionWithPathContext<'input> = BaseParserRuleContext<'input,LabelExtractionWithPathContextExt<'input>>;

pub trait LabelExtractionWithPathContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
	/// Returns `None` if there is no child corresponding to token ATTRIBUTE
	fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token EQ
	/// Returns `None` if there is no child corresponding to token EQ
	fn EQ(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_EQ, 0)
	}
	/// Retrieves first TerminalNode corresponding to token STRING
	/// Returns `None` if there is no child corresponding to token STRING
	fn STRING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_STRING, 0)
	}
}

impl<'input> LabelExtractionWithPathContextAttrs<'input> for LabelExtractionWithPathContext<'input>{}

pub struct LabelExtractionWithPathContextExt<'input>{
	base:LabelExtractionExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelExtractionWithPathContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelExtractionWithPathContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelExtractionWithPathContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelExtractionWithPath(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelExtractionWithPath(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelExtractionWithPathContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelExtractionWithPath(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelExtractionWithPathContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelExtractionExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelExtractionExpr }
}

impl<'input> Borrow<LabelExtractionExprContextExt<'input>> for LabelExtractionWithPathContext<'input>{
	fn borrow(&self) -> &LabelExtractionExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelExtractionExprContextExt<'input>> for LabelExtractionWithPathContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelExtractionExprContextExt<'input> { &mut self.base }
}

impl<'input> LabelExtractionExprContextAttrs<'input> for LabelExtractionWithPathContext<'input> {}

impl<'input> LabelExtractionWithPathContextExt<'input>{
	fn new(ctx: &dyn LabelExtractionExprContextAttrs<'input>) -> Rc<LabelExtractionExprContextAll<'input>>  {
		Rc::new(
			LabelExtractionExprContextAll::LabelExtractionWithPathContext(
				BaseParserRuleContext::copy_from(ctx,LabelExtractionWithPathContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelExtractionExpr(&mut self,)
	-> Result<Rc<LabelExtractionExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelExtractionExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 46, RULE_labelExtractionExpr);
        let mut _localctx: Rc<LabelExtractionExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(323);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(24,&mut recog.base)? {
				1 =>{
					let tmp = LabelExtractionWithPathContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(319);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(320);
					recog.base.match_token(LogQLParser_EQ,&mut recog.err_handler)?;

					recog.base.set_state(321);
					recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					let tmp = LabelExtractionSimpleContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(322);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

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
//------------------- labelExtractions ----------------
pub type LabelExtractionsContextAll<'input> = LabelExtractionsContext<'input>;


pub type LabelExtractionsContext<'input> = BaseParserRuleContext<'input,LabelExtractionsContextExt<'input>>;

#[derive(Clone)]
pub struct LabelExtractionsContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LabelExtractionsContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelExtractionsContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelExtractions(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelExtractions(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelExtractionsContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelExtractions(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelExtractionsContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelExtractions }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelExtractions }
}
antlr4rust::tid!{LabelExtractionsContextExt<'a>}

impl<'input> LabelExtractionsContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelExtractionsContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelExtractionsContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelExtractionsContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LabelExtractionsContextExt<'input>>{

fn labelExtractionExpr_all(&self) ->  Vec<Rc<LabelExtractionExprContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn labelExtractionExpr(&self, i: usize) -> Option<Rc<LabelExtractionExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COMMA, i)
}

}

impl<'input> LabelExtractionsContextAttrs<'input> for LabelExtractionsContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelExtractions(&mut self,)
	-> Result<Rc<LabelExtractionsContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelExtractionsContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 48, RULE_labelExtractions);
        let mut _localctx: Rc<LabelExtractionsContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule labelExtractionExpr*/
			recog.base.set_state(325);
			recog.labelExtractionExpr()?;

			recog.base.set_state(330);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(25,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					{
					{
					recog.base.set_state(326);
					recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule labelExtractionExpr*/
					recog.base.set_state(327);
					recog.labelExtractionExpr()?;

					}
					} 
				}
				recog.base.set_state(332);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(25,&mut recog.base)?;
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
//------------------- labelFilter ----------------
#[derive(Debug)]
pub enum LabelFilterContextAll<'input>{
	LabelFilterBytesContext(LabelFilterBytesContext<'input>),
	LabelFilterDurationContext(LabelFilterDurationContext<'input>),
	LabelFilterParensContext(LabelFilterParensContext<'input>),
	LabelFilterOrContext(LabelFilterOrContext<'input>),
	LabelFilterMatcherContext(LabelFilterMatcherContext<'input>),
	LabelFilterIpContext(LabelFilterIpContext<'input>),
	LabelFilterNumberContext(LabelFilterNumberContext<'input>),
	LabelFilterAndContext(LabelFilterAndContext<'input>),
Error(LabelFilterContext<'input>)
}
antlr4rust::tid!{LabelFilterContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for LabelFilterContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for LabelFilterContextAll<'input>{}

impl<'input> Deref for LabelFilterContextAll<'input>{
	type Target = dyn LabelFilterContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use LabelFilterContextAll::*;
		match self{
			LabelFilterBytesContext(inner) => inner,
			LabelFilterDurationContext(inner) => inner,
			LabelFilterParensContext(inner) => inner,
			LabelFilterOrContext(inner) => inner,
			LabelFilterMatcherContext(inner) => inner,
			LabelFilterIpContext(inner) => inner,
			LabelFilterNumberContext(inner) => inner,
			LabelFilterAndContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type LabelFilterContext<'input> = BaseParserRuleContext<'input,LabelFilterContextExt<'input>>;

#[derive(Clone)]
pub struct LabelFilterContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LabelFilterContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterContext<'input>{
}

impl<'input> CustomRuleContext<'input> for LabelFilterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFilter }
}
antlr4rust::tid!{LabelFilterContextExt<'a>}

impl<'input> LabelFilterContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelFilterContextAll<'input>> {
		Rc::new(
		LabelFilterContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelFilterContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait LabelFilterContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LabelFilterContextExt<'input>>{


}

impl<'input> LabelFilterContextAttrs<'input> for LabelFilterContext<'input>{}

pub type LabelFilterBytesContext<'input> = BaseParserRuleContext<'input,LabelFilterBytesContextExt<'input>>;

pub trait LabelFilterBytesContextAttrs<'input>: LogQLParserContext<'input>{
	fn bytesFilter(&self) -> Option<Rc<BytesFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> LabelFilterBytesContextAttrs<'input> for LabelFilterBytesContext<'input>{}

pub struct LabelFilterBytesContextExt<'input>{
	base:LabelFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFilterBytesContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFilterBytesContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterBytesContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFilterBytes(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFilterBytes(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterBytesContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFilterBytes(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFilterBytesContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFilter }
}

impl<'input> Borrow<LabelFilterContextExt<'input>> for LabelFilterBytesContext<'input>{
	fn borrow(&self) -> &LabelFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFilterContextExt<'input>> for LabelFilterBytesContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFilterContextExt<'input> { &mut self.base }
}

impl<'input> LabelFilterContextAttrs<'input> for LabelFilterBytesContext<'input> {}

impl<'input> LabelFilterBytesContextExt<'input>{
	fn new(ctx: &dyn LabelFilterContextAttrs<'input>) -> Rc<LabelFilterContextAll<'input>>  {
		Rc::new(
			LabelFilterContextAll::LabelFilterBytesContext(
				BaseParserRuleContext::copy_from(ctx,LabelFilterBytesContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LabelFilterDurationContext<'input> = BaseParserRuleContext<'input,LabelFilterDurationContextExt<'input>>;

pub trait LabelFilterDurationContextAttrs<'input>: LogQLParserContext<'input>{
	fn durationFilter(&self) -> Option<Rc<DurationFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> LabelFilterDurationContextAttrs<'input> for LabelFilterDurationContext<'input>{}

pub struct LabelFilterDurationContextExt<'input>{
	base:LabelFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFilterDurationContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFilterDurationContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterDurationContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFilterDuration(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFilterDuration(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterDurationContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFilterDuration(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFilterDurationContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFilter }
}

impl<'input> Borrow<LabelFilterContextExt<'input>> for LabelFilterDurationContext<'input>{
	fn borrow(&self) -> &LabelFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFilterContextExt<'input>> for LabelFilterDurationContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFilterContextExt<'input> { &mut self.base }
}

impl<'input> LabelFilterContextAttrs<'input> for LabelFilterDurationContext<'input> {}

impl<'input> LabelFilterDurationContextExt<'input>{
	fn new(ctx: &dyn LabelFilterContextAttrs<'input>) -> Rc<LabelFilterContextAll<'input>>  {
		Rc::new(
			LabelFilterContextAll::LabelFilterDurationContext(
				BaseParserRuleContext::copy_from(ctx,LabelFilterDurationContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LabelFilterParensContext<'input> = BaseParserRuleContext<'input,LabelFilterParensContextExt<'input>>;

pub trait LabelFilterParensContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token LPAREN
	/// Returns `None` if there is no child corresponding to token LPAREN
	fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LPAREN, 0)
	}
	fn labelFilter(&self) -> Option<Rc<LabelFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	/// Retrieves first TerminalNode corresponding to token RPAREN
	/// Returns `None` if there is no child corresponding to token RPAREN
	fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RPAREN, 0)
	}
}

impl<'input> LabelFilterParensContextAttrs<'input> for LabelFilterParensContext<'input>{}

pub struct LabelFilterParensContextExt<'input>{
	base:LabelFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFilterParensContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFilterParensContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterParensContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFilterParens(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFilterParens(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterParensContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFilterParens(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFilterParensContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFilter }
}

impl<'input> Borrow<LabelFilterContextExt<'input>> for LabelFilterParensContext<'input>{
	fn borrow(&self) -> &LabelFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFilterContextExt<'input>> for LabelFilterParensContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFilterContextExt<'input> { &mut self.base }
}

impl<'input> LabelFilterContextAttrs<'input> for LabelFilterParensContext<'input> {}

impl<'input> LabelFilterParensContextExt<'input>{
	fn new(ctx: &dyn LabelFilterContextAttrs<'input>) -> Rc<LabelFilterContextAll<'input>>  {
		Rc::new(
			LabelFilterContextAll::LabelFilterParensContext(
				BaseParserRuleContext::copy_from(ctx,LabelFilterParensContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LabelFilterOrContext<'input> = BaseParserRuleContext<'input,LabelFilterOrContextExt<'input>>;

pub trait LabelFilterOrContextAttrs<'input>: LogQLParserContext<'input>{
	fn labelFilter_all(&self) ->  Vec<Rc<LabelFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn labelFilter(&self, i: usize) -> Option<Rc<LabelFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token OR
	/// Returns `None` if there is no child corresponding to token OR
	fn OR(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_OR, 0)
	}
}

impl<'input> LabelFilterOrContextAttrs<'input> for LabelFilterOrContext<'input>{}

pub struct LabelFilterOrContextExt<'input>{
	base:LabelFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFilterOrContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFilterOrContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterOrContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFilterOr(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFilterOr(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterOrContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFilterOr(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFilterOrContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFilter }
}

impl<'input> Borrow<LabelFilterContextExt<'input>> for LabelFilterOrContext<'input>{
	fn borrow(&self) -> &LabelFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFilterContextExt<'input>> for LabelFilterOrContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFilterContextExt<'input> { &mut self.base }
}

impl<'input> LabelFilterContextAttrs<'input> for LabelFilterOrContext<'input> {}

impl<'input> LabelFilterOrContextExt<'input>{
	fn new(ctx: &dyn LabelFilterContextAttrs<'input>) -> Rc<LabelFilterContextAll<'input>>  {
		Rc::new(
			LabelFilterContextAll::LabelFilterOrContext(
				BaseParserRuleContext::copy_from(ctx,LabelFilterOrContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LabelFilterMatcherContext<'input> = BaseParserRuleContext<'input,LabelFilterMatcherContextExt<'input>>;

pub trait LabelFilterMatcherContextAttrs<'input>: LogQLParserContext<'input>{
	fn matcher(&self) -> Option<Rc<MatcherContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> LabelFilterMatcherContextAttrs<'input> for LabelFilterMatcherContext<'input>{}

pub struct LabelFilterMatcherContextExt<'input>{
	base:LabelFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFilterMatcherContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFilterMatcherContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterMatcherContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFilterMatcher(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFilterMatcher(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterMatcherContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFilterMatcher(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFilterMatcherContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFilter }
}

impl<'input> Borrow<LabelFilterContextExt<'input>> for LabelFilterMatcherContext<'input>{
	fn borrow(&self) -> &LabelFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFilterContextExt<'input>> for LabelFilterMatcherContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFilterContextExt<'input> { &mut self.base }
}

impl<'input> LabelFilterContextAttrs<'input> for LabelFilterMatcherContext<'input> {}

impl<'input> LabelFilterMatcherContextExt<'input>{
	fn new(ctx: &dyn LabelFilterContextAttrs<'input>) -> Rc<LabelFilterContextAll<'input>>  {
		Rc::new(
			LabelFilterContextAll::LabelFilterMatcherContext(
				BaseParserRuleContext::copy_from(ctx,LabelFilterMatcherContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LabelFilterIpContext<'input> = BaseParserRuleContext<'input,LabelFilterIpContextExt<'input>>;

pub trait LabelFilterIpContextAttrs<'input>: LogQLParserContext<'input>{
	fn ipLabelFilter(&self) -> Option<Rc<IpLabelFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> LabelFilterIpContextAttrs<'input> for LabelFilterIpContext<'input>{}

pub struct LabelFilterIpContextExt<'input>{
	base:LabelFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFilterIpContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFilterIpContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterIpContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFilterIp(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFilterIp(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterIpContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFilterIp(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFilterIpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFilter }
}

impl<'input> Borrow<LabelFilterContextExt<'input>> for LabelFilterIpContext<'input>{
	fn borrow(&self) -> &LabelFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFilterContextExt<'input>> for LabelFilterIpContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFilterContextExt<'input> { &mut self.base }
}

impl<'input> LabelFilterContextAttrs<'input> for LabelFilterIpContext<'input> {}

impl<'input> LabelFilterIpContextExt<'input>{
	fn new(ctx: &dyn LabelFilterContextAttrs<'input>) -> Rc<LabelFilterContextAll<'input>>  {
		Rc::new(
			LabelFilterContextAll::LabelFilterIpContext(
				BaseParserRuleContext::copy_from(ctx,LabelFilterIpContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LabelFilterNumberContext<'input> = BaseParserRuleContext<'input,LabelFilterNumberContextExt<'input>>;

pub trait LabelFilterNumberContextAttrs<'input>: LogQLParserContext<'input>{
	fn numberFilter(&self) -> Option<Rc<NumberFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> LabelFilterNumberContextAttrs<'input> for LabelFilterNumberContext<'input>{}

pub struct LabelFilterNumberContextExt<'input>{
	base:LabelFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFilterNumberContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFilterNumberContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterNumberContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFilterNumber(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFilterNumber(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterNumberContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFilterNumber(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFilterNumberContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFilter }
}

impl<'input> Borrow<LabelFilterContextExt<'input>> for LabelFilterNumberContext<'input>{
	fn borrow(&self) -> &LabelFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFilterContextExt<'input>> for LabelFilterNumberContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFilterContextExt<'input> { &mut self.base }
}

impl<'input> LabelFilterContextAttrs<'input> for LabelFilterNumberContext<'input> {}

impl<'input> LabelFilterNumberContextExt<'input>{
	fn new(ctx: &dyn LabelFilterContextAttrs<'input>) -> Rc<LabelFilterContextAll<'input>>  {
		Rc::new(
			LabelFilterContextAll::LabelFilterNumberContext(
				BaseParserRuleContext::copy_from(ctx,LabelFilterNumberContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LabelFilterAndContext<'input> = BaseParserRuleContext<'input,LabelFilterAndContextExt<'input>>;

pub trait LabelFilterAndContextAttrs<'input>: LogQLParserContext<'input>{
	fn labelFilter_all(&self) ->  Vec<Rc<LabelFilterContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn labelFilter(&self, i: usize) -> Option<Rc<LabelFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token AND
	/// Returns `None` if there is no child corresponding to token AND
	fn AND(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_AND, 0)
	}
}

impl<'input> LabelFilterAndContextAttrs<'input> for LabelFilterAndContext<'input>{}

pub struct LabelFilterAndContextExt<'input>{
	base:LabelFilterContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LabelFilterAndContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LabelFilterAndContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelFilterAndContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_labelFilterAnd(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_labelFilterAnd(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelFilterAndContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelFilterAnd(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelFilterAndContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelFilter }
}

impl<'input> Borrow<LabelFilterContextExt<'input>> for LabelFilterAndContext<'input>{
	fn borrow(&self) -> &LabelFilterContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LabelFilterContextExt<'input>> for LabelFilterAndContext<'input>{
	fn borrow_mut(&mut self) -> &mut LabelFilterContextExt<'input> { &mut self.base }
}

impl<'input> LabelFilterContextAttrs<'input> for LabelFilterAndContext<'input> {}

impl<'input> LabelFilterAndContextExt<'input>{
	fn new(ctx: &dyn LabelFilterContextAttrs<'input>) -> Rc<LabelFilterContextAll<'input>>  {
		Rc::new(
			LabelFilterContextAll::LabelFilterAndContext(
				BaseParserRuleContext::copy_from(ctx,LabelFilterAndContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn  labelFilter(&mut self,)
	-> Result<Rc<LabelFilterContextAll<'input>>,ANTLRError> {
		self.labelFilter_rec(0)
	}

	fn labelFilter_rec(&mut self, _p: i32)
	-> Result<Rc<LabelFilterContextAll<'input>>,ANTLRError> {
		let recog = self;
		let _parentctx = recog.ctx.take();
		let _parentState = recog.base.get_state();
		let mut _localctx = LabelFilterContextExt::new(_parentctx.clone(), recog.base.get_state());
		recog.base.enter_recursion_rule(_localctx.clone(), 50, RULE_labelFilter, _p);
	    let mut _localctx: Rc<LabelFilterContextAll> = _localctx;
        let mut _prevctx = _localctx.clone();
		let _startState = 50;
		let result: Result<(), ANTLRError> = (|| {
			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(343);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(26,&mut recog.base)? {
				1 =>{
					{
					let mut tmp = LabelFilterParensContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();

					recog.base.set_state(334);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule labelFilter*/
					recog.base.set_state(335);
					recog.labelFilter_rec(0)?;

					recog.base.set_state(336);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					{
					let mut tmp = LabelFilterMatcherContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule matcher*/
					recog.base.set_state(338);
					recog.matcher()?;

					}
				}
			,
				3 =>{
					{
					let mut tmp = LabelFilterNumberContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule numberFilter*/
					recog.base.set_state(339);
					recog.numberFilter()?;

					}
				}
			,
				4 =>{
					{
					let mut tmp = LabelFilterDurationContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule durationFilter*/
					recog.base.set_state(340);
					recog.durationFilter()?;

					}
				}
			,
				5 =>{
					{
					let mut tmp = LabelFilterBytesContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule bytesFilter*/
					recog.base.set_state(341);
					recog.bytesFilter()?;

					}
				}
			,
				6 =>{
					{
					let mut tmp = LabelFilterIpContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule ipLabelFilter*/
					recog.base.set_state(342);
					recog.ipLabelFilter()?;

					}
				}

				_ => {}
			}
			let tmp = recog.input.lt(-1).cloned();
			recog.ctx.as_ref().unwrap().set_stop(tmp);
			recog.base.set_state(353);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(28,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					recog.trigger_exit_rule_event()?;
					_prevctx = _localctx.clone();
					{
					recog.base.set_state(351);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(27,&mut recog.base)? {
						1 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = LabelFilterAndContextExt::new(&**LabelFilterContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_labelFilter)?;
							_localctx = tmp;
							recog.base.set_state(345);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 8)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 8)".to_owned()), None))?;
							}
							recog.base.set_state(346);
							recog.base.match_token(LogQLParser_AND,&mut recog.err_handler)?;

							/*InvokeRule labelFilter*/
							recog.base.set_state(347);
							recog.labelFilter_rec(9)?;

							}
						}
					,
						2 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = LabelFilterOrContextExt::new(&**LabelFilterContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_labelFilter)?;
							_localctx = tmp;
							recog.base.set_state(348);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 7)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 7)".to_owned()), None))?;
							}
							recog.base.set_state(349);
							recog.base.match_token(LogQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule labelFilter*/
							recog.base.set_state(350);
							recog.labelFilter_rec(8)?;

							}
						}

						_ => {}
					}
					} 
				}
				recog.base.set_state(355);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(28,&mut recog.base)?;
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
//------------------- numberFilter ----------------
pub type NumberFilterContextAll<'input> = NumberFilterContext<'input>;


pub type NumberFilterContext<'input> = BaseParserRuleContext<'input,NumberFilterContextExt<'input>>;

#[derive(Clone)]
pub struct NumberFilterContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for NumberFilterContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for NumberFilterContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_numberFilter(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_numberFilter(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for NumberFilterContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_numberFilter(self);
	}
}

impl<'input> CustomRuleContext<'input> for NumberFilterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_numberFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_numberFilter }
}
antlr4rust::tid!{NumberFilterContextExt<'a>}

impl<'input> NumberFilterContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<NumberFilterContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,NumberFilterContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait NumberFilterContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<NumberFilterContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
/// Returns `None` if there is no child corresponding to token ATTRIBUTE
fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_ATTRIBUTE, 0)
}
fn comparisonOp(&self) -> Option<Rc<ComparisonOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn literalExpr(&self) -> Option<Rc<LiteralExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> NumberFilterContextAttrs<'input> for NumberFilterContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn numberFilter(&mut self,)
	-> Result<Rc<NumberFilterContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = NumberFilterContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 52, RULE_numberFilter);
        let mut _localctx: Rc<NumberFilterContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(356);
			recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

			/*InvokeRule comparisonOp*/
			recog.base.set_state(357);
			recog.comparisonOp()?;

			/*InvokeRule literalExpr*/
			recog.base.set_state(358);
			recog.literalExpr()?;

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
//------------------- durationFilter ----------------
pub type DurationFilterContextAll<'input> = DurationFilterContext<'input>;


pub type DurationFilterContext<'input> = BaseParserRuleContext<'input,DurationFilterContextExt<'input>>;

#[derive(Clone)]
pub struct DurationFilterContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for DurationFilterContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for DurationFilterContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_durationFilter(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_durationFilter(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for DurationFilterContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_durationFilter(self);
	}
}

impl<'input> CustomRuleContext<'input> for DurationFilterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_durationFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_durationFilter }
}
antlr4rust::tid!{DurationFilterContextExt<'a>}

impl<'input> DurationFilterContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<DurationFilterContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,DurationFilterContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait DurationFilterContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<DurationFilterContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
/// Returns `None` if there is no child corresponding to token ATTRIBUTE
fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_ATTRIBUTE, 0)
}
fn comparisonOp(&self) -> Option<Rc<ComparisonOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn duration(&self) -> Option<Rc<DurationContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> DurationFilterContextAttrs<'input> for DurationFilterContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn durationFilter(&mut self,)
	-> Result<Rc<DurationFilterContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = DurationFilterContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 54, RULE_durationFilter);
        let mut _localctx: Rc<DurationFilterContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(360);
			recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

			/*InvokeRule comparisonOp*/
			recog.base.set_state(361);
			recog.comparisonOp()?;

			/*InvokeRule duration*/
			recog.base.set_state(362);
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
//------------------- bytesFilter ----------------
pub type BytesFilterContextAll<'input> = BytesFilterContext<'input>;


pub type BytesFilterContext<'input> = BaseParserRuleContext<'input,BytesFilterContextExt<'input>>;

#[derive(Clone)]
pub struct BytesFilterContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for BytesFilterContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BytesFilterContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_bytesFilter(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_bytesFilter(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BytesFilterContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_bytesFilter(self);
	}
}

impl<'input> CustomRuleContext<'input> for BytesFilterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_bytesFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_bytesFilter }
}
antlr4rust::tid!{BytesFilterContextExt<'a>}

impl<'input> BytesFilterContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<BytesFilterContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,BytesFilterContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait BytesFilterContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<BytesFilterContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
/// Returns `None` if there is no child corresponding to token ATTRIBUTE
fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_ATTRIBUTE, 0)
}
fn comparisonOp(&self) -> Option<Rc<ComparisonOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token BYTES
/// Returns `None` if there is no child corresponding to token BYTES
fn BYTES(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_BYTES, 0)
}

}

impl<'input> BytesFilterContextAttrs<'input> for BytesFilterContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn bytesFilter(&mut self,)
	-> Result<Rc<BytesFilterContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = BytesFilterContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 56, RULE_bytesFilter);
        let mut _localctx: Rc<BytesFilterContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(364);
			recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

			/*InvokeRule comparisonOp*/
			recog.base.set_state(365);
			recog.comparisonOp()?;

			recog.base.set_state(366);
			recog.base.match_token(LogQLParser_BYTES,&mut recog.err_handler)?;

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
//------------------- ipLabelFilter ----------------
pub type IpLabelFilterContextAll<'input> = IpLabelFilterContext<'input>;


pub type IpLabelFilterContext<'input> = BaseParserRuleContext<'input,IpLabelFilterContextExt<'input>>;

#[derive(Clone)]
pub struct IpLabelFilterContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for IpLabelFilterContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for IpLabelFilterContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_ipLabelFilter(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_ipLabelFilter(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for IpLabelFilterContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_ipLabelFilter(self);
	}
}

impl<'input> CustomRuleContext<'input> for IpLabelFilterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_ipLabelFilter }
	//fn type_rule_index() -> usize where Self: Sized { RULE_ipLabelFilter }
}
antlr4rust::tid!{IpLabelFilterContextExt<'a>}

impl<'input> IpLabelFilterContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<IpLabelFilterContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,IpLabelFilterContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait IpLabelFilterContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<IpLabelFilterContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
/// Returns `None` if there is no child corresponding to token ATTRIBUTE
fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_ATTRIBUTE, 0)
}
/// Retrieves first TerminalNode corresponding to token EQ
/// Returns `None` if there is no child corresponding to token EQ
fn EQ(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_EQ, 0)
}
fn ipFn(&self) -> Option<Rc<IpFnContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token NE
/// Returns `None` if there is no child corresponding to token NE
fn NE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_NE, 0)
}

}

impl<'input> IpLabelFilterContextAttrs<'input> for IpLabelFilterContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn ipLabelFilter(&mut self,)
	-> Result<Rc<IpLabelFilterContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = IpLabelFilterContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 58, RULE_ipLabelFilter);
        let mut _localctx: Rc<IpLabelFilterContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(374);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(29,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(368);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(369);
					recog.base.match_token(LogQLParser_EQ,&mut recog.err_handler)?;

					/*InvokeRule ipFn*/
					recog.base.set_state(370);
					recog.ipFn()?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(371);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(372);
					recog.base.match_token(LogQLParser_NE,&mut recog.err_handler)?;

					/*InvokeRule ipFn*/
					recog.base.set_state(373);
					recog.ipFn()?;

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
//------------------- comparisonOp ----------------
pub type ComparisonOpContextAll<'input> = ComparisonOpContext<'input>;


pub type ComparisonOpContext<'input> = BaseParserRuleContext<'input,ComparisonOpContextExt<'input>>;

#[derive(Clone)]
pub struct ComparisonOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for ComparisonOpContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for ComparisonOpContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_comparisonOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_comparisonOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for ComparisonOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_comparisonOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for ComparisonOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_comparisonOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_comparisonOp }
}
antlr4rust::tid!{ComparisonOpContextExt<'a>}

impl<'input> ComparisonOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<ComparisonOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,ComparisonOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait ComparisonOpContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<ComparisonOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token GT
/// Returns `None` if there is no child corresponding to token GT
fn GT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_GT, 0)
}
/// Retrieves first TerminalNode corresponding to token GE
/// Returns `None` if there is no child corresponding to token GE
fn GE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_GE, 0)
}
/// Retrieves first TerminalNode corresponding to token LT
/// Returns `None` if there is no child corresponding to token LT
fn LT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LT, 0)
}
/// Retrieves first TerminalNode corresponding to token LE
/// Returns `None` if there is no child corresponding to token LE
fn LE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LE, 0)
}
/// Retrieves first TerminalNode corresponding to token EQ
/// Returns `None` if there is no child corresponding to token EQ
fn EQ(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_EQ, 0)
}
/// Retrieves first TerminalNode corresponding to token EQL
/// Returns `None` if there is no child corresponding to token EQL
fn EQL(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_EQL, 0)
}
/// Retrieves first TerminalNode corresponding to token NE
/// Returns `None` if there is no child corresponding to token NE
fn NE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_NE, 0)
}

}

impl<'input> ComparisonOpContextAttrs<'input> for ComparisonOpContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn comparisonOp(&mut self,)
	-> Result<Rc<ComparisonOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = ComparisonOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 60, RULE_comparisonOp);
        let mut _localctx: Rc<ComparisonOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(376);
			_la = recog.base.input.la(1);
			if { !((((_la) & !0x3f) == 0 && ((1usize << _la) & 65404928) != 0)) } {
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
//------------------- metricExpr ----------------
#[derive(Debug)]
pub enum MetricExprContextAll<'input>{
	MetricExprParensContext(MetricExprParensContext<'input>),
	MetricExprLabelReplaceContext(MetricExprLabelReplaceContext<'input>),
	MetricExprVariableContext(MetricExprVariableContext<'input>),
	BinaryOpGeContext(BinaryOpGeContext<'input>),
	BinaryOpPowContext(BinaryOpPowContext<'input>),
	BinaryOpUnlessContext(BinaryOpUnlessContext<'input>),
	MetricExprRangeAggContext(MetricExprRangeAggContext<'input>),
	BinaryOpAndContext(BinaryOpAndContext<'input>),
	BinaryOpOrContext(BinaryOpOrContext<'input>),
	BinaryOpLtContext(BinaryOpLtContext<'input>),
	MetricExprLiteralContext(MetricExprLiteralContext<'input>),
	BinaryOpNeqContext(BinaryOpNeqContext<'input>),
	BinaryOpAddContext(BinaryOpAddContext<'input>),
	BinaryOpGtContext(BinaryOpGtContext<'input>),
	BinaryOpSubContext(BinaryOpSubContext<'input>),
	BinaryOpEqlContext(BinaryOpEqlContext<'input>),
	BinaryOpMulContext(BinaryOpMulContext<'input>),
	MetricExprVectorContext(MetricExprVectorContext<'input>),
	BinaryOpModContext(BinaryOpModContext<'input>),
	BinaryOpLeContext(BinaryOpLeContext<'input>),
	MetricExprVectorAggContext(MetricExprVectorAggContext<'input>),
	BinaryOpDivContext(BinaryOpDivContext<'input>),
Error(MetricExprContext<'input>)
}
antlr4rust::tid!{MetricExprContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for MetricExprContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for MetricExprContextAll<'input>{}

impl<'input> Deref for MetricExprContextAll<'input>{
	type Target = dyn MetricExprContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use MetricExprContextAll::*;
		match self{
			MetricExprParensContext(inner) => inner,
			MetricExprLabelReplaceContext(inner) => inner,
			MetricExprVariableContext(inner) => inner,
			BinaryOpGeContext(inner) => inner,
			BinaryOpPowContext(inner) => inner,
			BinaryOpUnlessContext(inner) => inner,
			MetricExprRangeAggContext(inner) => inner,
			BinaryOpAndContext(inner) => inner,
			BinaryOpOrContext(inner) => inner,
			BinaryOpLtContext(inner) => inner,
			MetricExprLiteralContext(inner) => inner,
			BinaryOpNeqContext(inner) => inner,
			BinaryOpAddContext(inner) => inner,
			BinaryOpGtContext(inner) => inner,
			BinaryOpSubContext(inner) => inner,
			BinaryOpEqlContext(inner) => inner,
			BinaryOpMulContext(inner) => inner,
			MetricExprVectorContext(inner) => inner,
			BinaryOpModContext(inner) => inner,
			BinaryOpLeContext(inner) => inner,
			MetricExprVectorAggContext(inner) => inner,
			BinaryOpDivContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MetricExprContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MetricExprContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type MetricExprContext<'input> = BaseParserRuleContext<'input,MetricExprContextExt<'input>>;

#[derive(Clone)]
pub struct MetricExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for MetricExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MetricExprContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MetricExprContext<'input>{
}

impl<'input> CustomRuleContext<'input> for MetricExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}
antlr4rust::tid!{MetricExprContextExt<'a>}

impl<'input> MetricExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<MetricExprContextAll<'input>> {
		Rc::new(
		MetricExprContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,MetricExprContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait MetricExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<MetricExprContextExt<'input>>{


}

impl<'input> MetricExprContextAttrs<'input> for MetricExprContext<'input>{}

pub type MetricExprParensContext<'input> = BaseParserRuleContext<'input,MetricExprParensContextExt<'input>>;

pub trait MetricExprParensContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token LPAREN
	/// Returns `None` if there is no child corresponding to token LPAREN
	fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LPAREN, 0)
	}
	fn metricExpr(&self) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	/// Retrieves first TerminalNode corresponding to token RPAREN
	/// Returns `None` if there is no child corresponding to token RPAREN
	fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RPAREN, 0)
	}
}

impl<'input> MetricExprParensContextAttrs<'input> for MetricExprParensContext<'input>{}

pub struct MetricExprParensContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MetricExprParensContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MetricExprParensContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MetricExprParensContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_metricExprParens(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_metricExprParens(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MetricExprParensContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_metricExprParens(self);
	}
}

impl<'input> CustomRuleContext<'input> for MetricExprParensContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for MetricExprParensContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for MetricExprParensContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for MetricExprParensContext<'input> {}

impl<'input> MetricExprParensContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::MetricExprParensContext(
				BaseParserRuleContext::copy_from(ctx,MetricExprParensContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type MetricExprLabelReplaceContext<'input> = BaseParserRuleContext<'input,MetricExprLabelReplaceContextExt<'input>>;

pub trait MetricExprLabelReplaceContextAttrs<'input>: LogQLParserContext<'input>{
	fn labelReplaceExpr(&self) -> Option<Rc<LabelReplaceExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> MetricExprLabelReplaceContextAttrs<'input> for MetricExprLabelReplaceContext<'input>{}

pub struct MetricExprLabelReplaceContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MetricExprLabelReplaceContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MetricExprLabelReplaceContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MetricExprLabelReplaceContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_metricExprLabelReplace(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_metricExprLabelReplace(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MetricExprLabelReplaceContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_metricExprLabelReplace(self);
	}
}

impl<'input> CustomRuleContext<'input> for MetricExprLabelReplaceContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for MetricExprLabelReplaceContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for MetricExprLabelReplaceContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for MetricExprLabelReplaceContext<'input> {}

impl<'input> MetricExprLabelReplaceContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::MetricExprLabelReplaceContext(
				BaseParserRuleContext::copy_from(ctx,MetricExprLabelReplaceContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type MetricExprVariableContext<'input> = BaseParserRuleContext<'input,MetricExprVariableContextExt<'input>>;

pub trait MetricExprVariableContextAttrs<'input>: LogQLParserContext<'input>{
	fn variableExpr(&self) -> Option<Rc<VariableExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> MetricExprVariableContextAttrs<'input> for MetricExprVariableContext<'input>{}

pub struct MetricExprVariableContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MetricExprVariableContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MetricExprVariableContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MetricExprVariableContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_metricExprVariable(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_metricExprVariable(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MetricExprVariableContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_metricExprVariable(self);
	}
}

impl<'input> CustomRuleContext<'input> for MetricExprVariableContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for MetricExprVariableContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for MetricExprVariableContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for MetricExprVariableContext<'input> {}

impl<'input> MetricExprVariableContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::MetricExprVariableContext(
				BaseParserRuleContext::copy_from(ctx,MetricExprVariableContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpGeContext<'input> = BaseParserRuleContext<'input,BinaryOpGeContextExt<'input>>;

pub trait BinaryOpGeContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token GE
	/// Returns `None` if there is no child corresponding to token GE
	fn GE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_GE, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpGeContextAttrs<'input> for BinaryOpGeContext<'input>{}

pub struct BinaryOpGeContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpGeContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpGeContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpGeContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpGe(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpGe(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpGeContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpGe(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpGeContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpGeContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpGeContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpGeContext<'input> {}

impl<'input> BinaryOpGeContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpGeContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpGeContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpPowContext<'input> = BaseParserRuleContext<'input,BinaryOpPowContextExt<'input>>;

pub trait BinaryOpPowContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token POW
	/// Returns `None` if there is no child corresponding to token POW
	fn POW(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_POW, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpPowContextAttrs<'input> for BinaryOpPowContext<'input>{}

pub struct BinaryOpPowContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpPowContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpPowContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpPowContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpPow(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpPow(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpPowContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpPow(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpPowContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpPowContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpPowContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpPowContext<'input> {}

impl<'input> BinaryOpPowContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpPowContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpPowContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpUnlessContext<'input> = BaseParserRuleContext<'input,BinaryOpUnlessContextExt<'input>>;

pub trait BinaryOpUnlessContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token UNLESS
	/// Returns `None` if there is no child corresponding to token UNLESS
	fn UNLESS(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_UNLESS, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpUnlessContextAttrs<'input> for BinaryOpUnlessContext<'input>{}

pub struct BinaryOpUnlessContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpUnlessContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpUnlessContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpUnlessContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpUnless(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpUnless(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpUnlessContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpUnless(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpUnlessContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpUnlessContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpUnlessContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpUnlessContext<'input> {}

impl<'input> BinaryOpUnlessContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpUnlessContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpUnlessContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type MetricExprRangeAggContext<'input> = BaseParserRuleContext<'input,MetricExprRangeAggContextExt<'input>>;

pub trait MetricExprRangeAggContextAttrs<'input>: LogQLParserContext<'input>{
	fn rangeAggregationExpr(&self) -> Option<Rc<RangeAggregationExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> MetricExprRangeAggContextAttrs<'input> for MetricExprRangeAggContext<'input>{}

pub struct MetricExprRangeAggContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MetricExprRangeAggContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MetricExprRangeAggContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MetricExprRangeAggContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_metricExprRangeAgg(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_metricExprRangeAgg(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MetricExprRangeAggContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_metricExprRangeAgg(self);
	}
}

impl<'input> CustomRuleContext<'input> for MetricExprRangeAggContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for MetricExprRangeAggContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for MetricExprRangeAggContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for MetricExprRangeAggContext<'input> {}

impl<'input> MetricExprRangeAggContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::MetricExprRangeAggContext(
				BaseParserRuleContext::copy_from(ctx,MetricExprRangeAggContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpAndContext<'input> = BaseParserRuleContext<'input,BinaryOpAndContextExt<'input>>;

pub trait BinaryOpAndContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token AND
	/// Returns `None` if there is no child corresponding to token AND
	fn AND(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_AND, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpAndContextAttrs<'input> for BinaryOpAndContext<'input>{}

pub struct BinaryOpAndContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpAndContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpAndContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpAndContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpAnd(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpAnd(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpAndContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpAnd(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpAndContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpAndContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpAndContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpAndContext<'input> {}

impl<'input> BinaryOpAndContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpAndContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpAndContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpOrContext<'input> = BaseParserRuleContext<'input,BinaryOpOrContextExt<'input>>;

pub trait BinaryOpOrContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token OR
	/// Returns `None` if there is no child corresponding to token OR
	fn OR(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_OR, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpOrContextAttrs<'input> for BinaryOpOrContext<'input>{}

pub struct BinaryOpOrContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpOrContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpOrContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpOrContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpOr(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpOr(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpOrContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpOr(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpOrContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpOrContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpOrContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpOrContext<'input> {}

impl<'input> BinaryOpOrContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpOrContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpOrContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpLtContext<'input> = BaseParserRuleContext<'input,BinaryOpLtContextExt<'input>>;

pub trait BinaryOpLtContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token LT
	/// Returns `None` if there is no child corresponding to token LT
	fn LT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LT, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpLtContextAttrs<'input> for BinaryOpLtContext<'input>{}

pub struct BinaryOpLtContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpLtContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpLtContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpLtContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpLt(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpLt(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpLtContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpLt(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpLtContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpLtContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpLtContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpLtContext<'input> {}

impl<'input> BinaryOpLtContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpLtContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpLtContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type MetricExprLiteralContext<'input> = BaseParserRuleContext<'input,MetricExprLiteralContextExt<'input>>;

pub trait MetricExprLiteralContextAttrs<'input>: LogQLParserContext<'input>{
	fn literalExpr(&self) -> Option<Rc<LiteralExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> MetricExprLiteralContextAttrs<'input> for MetricExprLiteralContext<'input>{}

pub struct MetricExprLiteralContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MetricExprLiteralContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MetricExprLiteralContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MetricExprLiteralContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_metricExprLiteral(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_metricExprLiteral(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MetricExprLiteralContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_metricExprLiteral(self);
	}
}

impl<'input> CustomRuleContext<'input> for MetricExprLiteralContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for MetricExprLiteralContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for MetricExprLiteralContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for MetricExprLiteralContext<'input> {}

impl<'input> MetricExprLiteralContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::MetricExprLiteralContext(
				BaseParserRuleContext::copy_from(ctx,MetricExprLiteralContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpNeqContext<'input> = BaseParserRuleContext<'input,BinaryOpNeqContextExt<'input>>;

pub trait BinaryOpNeqContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token NE
	/// Returns `None` if there is no child corresponding to token NE
	fn NE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_NE, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpNeqContextAttrs<'input> for BinaryOpNeqContext<'input>{}

pub struct BinaryOpNeqContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpNeqContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpNeqContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpNeqContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpNeq(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpNeq(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpNeqContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpNeq(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpNeqContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpNeqContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpNeqContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpNeqContext<'input> {}

impl<'input> BinaryOpNeqContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpNeqContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpNeqContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpAddContext<'input> = BaseParserRuleContext<'input,BinaryOpAddContextExt<'input>>;

pub trait BinaryOpAddContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token ADD
	/// Returns `None` if there is no child corresponding to token ADD
	fn ADD(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ADD, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpAddContextAttrs<'input> for BinaryOpAddContext<'input>{}

pub struct BinaryOpAddContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpAddContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpAddContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpAddContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpAdd(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpAdd(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpAddContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpAdd(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpAddContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpAddContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpAddContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpAddContext<'input> {}

impl<'input> BinaryOpAddContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpAddContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpAddContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpGtContext<'input> = BaseParserRuleContext<'input,BinaryOpGtContextExt<'input>>;

pub trait BinaryOpGtContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token GT
	/// Returns `None` if there is no child corresponding to token GT
	fn GT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_GT, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpGtContextAttrs<'input> for BinaryOpGtContext<'input>{}

pub struct BinaryOpGtContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpGtContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpGtContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpGtContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpGt(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpGt(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpGtContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpGt(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpGtContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpGtContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpGtContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpGtContext<'input> {}

impl<'input> BinaryOpGtContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpGtContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpGtContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpSubContext<'input> = BaseParserRuleContext<'input,BinaryOpSubContextExt<'input>>;

pub trait BinaryOpSubContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token SUB
	/// Returns `None` if there is no child corresponding to token SUB
	fn SUB(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_SUB, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpSubContextAttrs<'input> for BinaryOpSubContext<'input>{}

pub struct BinaryOpSubContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpSubContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpSubContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpSubContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpSub(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpSub(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpSubContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpSub(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpSubContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpSubContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpSubContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpSubContext<'input> {}

impl<'input> BinaryOpSubContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpSubContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpSubContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpEqlContext<'input> = BaseParserRuleContext<'input,BinaryOpEqlContextExt<'input>>;

pub trait BinaryOpEqlContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token EQL
	/// Returns `None` if there is no child corresponding to token EQL
	fn EQL(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_EQL, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpEqlContextAttrs<'input> for BinaryOpEqlContext<'input>{}

pub struct BinaryOpEqlContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpEqlContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpEqlContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpEqlContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpEql(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpEql(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpEqlContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpEql(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpEqlContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpEqlContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpEqlContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpEqlContext<'input> {}

impl<'input> BinaryOpEqlContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpEqlContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpEqlContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpMulContext<'input> = BaseParserRuleContext<'input,BinaryOpMulContextExt<'input>>;

pub trait BinaryOpMulContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token MUL
	/// Returns `None` if there is no child corresponding to token MUL
	fn MUL(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_MUL, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpMulContextAttrs<'input> for BinaryOpMulContext<'input>{}

pub struct BinaryOpMulContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpMulContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpMulContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpMulContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpMul(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpMul(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpMulContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpMul(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpMulContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpMulContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpMulContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpMulContext<'input> {}

impl<'input> BinaryOpMulContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpMulContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpMulContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type MetricExprVectorContext<'input> = BaseParserRuleContext<'input,MetricExprVectorContextExt<'input>>;

pub trait MetricExprVectorContextAttrs<'input>: LogQLParserContext<'input>{
	fn vectorExpr(&self) -> Option<Rc<VectorExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> MetricExprVectorContextAttrs<'input> for MetricExprVectorContext<'input>{}

pub struct MetricExprVectorContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MetricExprVectorContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MetricExprVectorContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MetricExprVectorContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_metricExprVector(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_metricExprVector(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MetricExprVectorContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_metricExprVector(self);
	}
}

impl<'input> CustomRuleContext<'input> for MetricExprVectorContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for MetricExprVectorContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for MetricExprVectorContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for MetricExprVectorContext<'input> {}

impl<'input> MetricExprVectorContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::MetricExprVectorContext(
				BaseParserRuleContext::copy_from(ctx,MetricExprVectorContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpModContext<'input> = BaseParserRuleContext<'input,BinaryOpModContextExt<'input>>;

pub trait BinaryOpModContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token MOD
	/// Returns `None` if there is no child corresponding to token MOD
	fn MOD(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_MOD, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpModContextAttrs<'input> for BinaryOpModContext<'input>{}

pub struct BinaryOpModContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpModContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpModContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpModContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpMod(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpMod(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpModContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpMod(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpModContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpModContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpModContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpModContext<'input> {}

impl<'input> BinaryOpModContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpModContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpModContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpLeContext<'input> = BaseParserRuleContext<'input,BinaryOpLeContextExt<'input>>;

pub trait BinaryOpLeContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token LE
	/// Returns `None` if there is no child corresponding to token LE
	fn LE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LE, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpLeContextAttrs<'input> for BinaryOpLeContext<'input>{}

pub struct BinaryOpLeContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpLeContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpLeContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpLeContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpLe(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpLe(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpLeContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpLe(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpLeContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpLeContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpLeContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpLeContext<'input> {}

impl<'input> BinaryOpLeContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpLeContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpLeContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type MetricExprVectorAggContext<'input> = BaseParserRuleContext<'input,MetricExprVectorAggContextExt<'input>>;

pub trait MetricExprVectorAggContextAttrs<'input>: LogQLParserContext<'input>{
	fn vectorAggregationExpr(&self) -> Option<Rc<VectorAggregationExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> MetricExprVectorAggContextAttrs<'input> for MetricExprVectorAggContext<'input>{}

pub struct MetricExprVectorAggContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{MetricExprVectorAggContextExt<'a>}

impl<'input> LogQLParserContext<'input> for MetricExprVectorAggContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for MetricExprVectorAggContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_metricExprVectorAgg(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_metricExprVectorAgg(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for MetricExprVectorAggContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_metricExprVectorAgg(self);
	}
}

impl<'input> CustomRuleContext<'input> for MetricExprVectorAggContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for MetricExprVectorAggContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for MetricExprVectorAggContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for MetricExprVectorAggContext<'input> {}

impl<'input> MetricExprVectorAggContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::MetricExprVectorAggContext(
				BaseParserRuleContext::copy_from(ctx,MetricExprVectorAggContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type BinaryOpDivContext<'input> = BaseParserRuleContext<'input,BinaryOpDivContextExt<'input>>;

pub trait BinaryOpDivContextAttrs<'input>: LogQLParserContext<'input>{
	fn metricExpr_all(&self) ->  Vec<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.children_of_type()
	}
	fn metricExpr(&self, i: usize) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(i)
	}
	/// Retrieves first TerminalNode corresponding to token DIV
	/// Returns `None` if there is no child corresponding to token DIV
	fn DIV(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_DIV, 0)
	}
	fn binOpModifier(&self) -> Option<Rc<BinOpModifierContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> BinaryOpDivContextAttrs<'input> for BinaryOpDivContext<'input>{}

pub struct BinaryOpDivContextExt<'input>{
	base:MetricExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{BinaryOpDivContextExt<'a>}

impl<'input> LogQLParserContext<'input> for BinaryOpDivContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinaryOpDivContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_binaryOpDiv(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_binaryOpDiv(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinaryOpDivContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binaryOpDiv(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinaryOpDivContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_metricExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_metricExpr }
}

impl<'input> Borrow<MetricExprContextExt<'input>> for BinaryOpDivContext<'input>{
	fn borrow(&self) -> &MetricExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<MetricExprContextExt<'input>> for BinaryOpDivContext<'input>{
	fn borrow_mut(&mut self) -> &mut MetricExprContextExt<'input> { &mut self.base }
}

impl<'input> MetricExprContextAttrs<'input> for BinaryOpDivContext<'input> {}

impl<'input> BinaryOpDivContextExt<'input>{
	fn new(ctx: &dyn MetricExprContextAttrs<'input>) -> Rc<MetricExprContextAll<'input>>  {
		Rc::new(
			MetricExprContextAll::BinaryOpDivContext(
				BaseParserRuleContext::copy_from(ctx,BinaryOpDivContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn  metricExpr(&mut self,)
	-> Result<Rc<MetricExprContextAll<'input>>,ANTLRError> {
		self.metricExpr_rec(0)
	}

	fn metricExpr_rec(&mut self, _p: i32)
	-> Result<Rc<MetricExprContextAll<'input>>,ANTLRError> {
		let recog = self;
		let _parentctx = recog.ctx.take();
		let _parentState = recog.base.get_state();
		let mut _localctx = MetricExprContextExt::new(_parentctx.clone(), recog.base.get_state());
		recog.base.enter_recursion_rule(_localctx.clone(), 62, RULE_metricExpr, _p);
	    let mut _localctx: Rc<MetricExprContextAll> = _localctx;
        let mut _prevctx = _localctx.clone();
		let _startState = 62;
		let result: Result<(), ANTLRError> = (|| {
			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(389);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LogQLParser_COUNT_OVER_TIME |LogQLParser_RATE |LogQLParser_RATE_COUNTER |
			LogQLParser_BYTES_OVER_TIME |LogQLParser_BYTES_RATE |LogQLParser_AVG_OVER_TIME |
			LogQLParser_SUM_OVER_TIME |LogQLParser_MIN_OVER_TIME |LogQLParser_MAX_OVER_TIME |
			LogQLParser_STDDEV_OVER_TIME |LogQLParser_STDVAR_OVER_TIME |LogQLParser_QUANTILE_OVER_TIME |
			LogQLParser_FIRST_OVER_TIME |LogQLParser_LAST_OVER_TIME |LogQLParser_ABSENT_OVER_TIME 
				=> {
					{
					let mut tmp = MetricExprRangeAggContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();

					/*InvokeRule rangeAggregationExpr*/
					recog.base.set_state(379);
					recog.rangeAggregationExpr()?;

					}
				}

			LogQLParser_SUM |LogQLParser_AVG |LogQLParser_COUNT |LogQLParser_MAX |
			LogQLParser_MIN |LogQLParser_STDDEV |LogQLParser_STDVAR |LogQLParser_TOPK |
			LogQLParser_BOTTOMK |LogQLParser_APPROX_TOPK |LogQLParser_SORT |LogQLParser_SORT_DESC 
				=> {
					{
					let mut tmp = MetricExprVectorAggContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule vectorAggregationExpr*/
					recog.base.set_state(380);
					recog.vectorAggregationExpr()?;

					}
				}

			LogQLParser_ADD |LogQLParser_SUB |LogQLParser_NUMBER 
				=> {
					{
					let mut tmp = MetricExprLiteralContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule literalExpr*/
					recog.base.set_state(381);
					recog.literalExpr()?;

					}
				}

			LogQLParser_LABEL_REPLACE 
				=> {
					{
					let mut tmp = MetricExprLabelReplaceContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule labelReplaceExpr*/
					recog.base.set_state(382);
					recog.labelReplaceExpr()?;

					}
				}

			LogQLParser_VECTOR 
				=> {
					{
					let mut tmp = MetricExprVectorContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule vectorExpr*/
					recog.base.set_state(383);
					recog.vectorExpr()?;

					}
				}

			LogQLParser_ATTRIBUTE 
				=> {
					{
					let mut tmp = MetricExprVariableContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					/*InvokeRule variableExpr*/
					recog.base.set_state(384);
					recog.variableExpr()?;

					}
				}

			LogQLParser_LPAREN 
				=> {
					{
					let mut tmp = MetricExprParensContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					recog.base.set_state(385);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule metricExpr*/
					recog.base.set_state(386);
					recog.metricExpr_rec(0)?;

					recog.base.set_state(387);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}

				_ => Err(ANTLRError::NoAltError(NoViableAltError::new(&mut recog.base)))?
			}
			let tmp = recog.input.lt(-1).cloned();
			recog.ctx.as_ref().unwrap().set_stop(tmp);
			recog.base.set_state(468);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(32,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					recog.trigger_exit_rule_event()?;
					_prevctx = _localctx.clone();
					{
					recog.base.set_state(466);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(31,&mut recog.base)? {
						1 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpPowContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(391);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 22)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 22)".to_owned()), None))?;
							}
							recog.base.set_state(392);
							recog.base.match_token(LogQLParser_POW,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(393);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(394);
							recog.metricExpr_rec(23)?;

							}
						}
					,
						2 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpMulContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(396);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 21)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 21)".to_owned()), None))?;
							}
							recog.base.set_state(397);
							recog.base.match_token(LogQLParser_MUL,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(398);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(399);
							recog.metricExpr_rec(22)?;

							}
						}
					,
						3 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpDivContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(401);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 20)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 20)".to_owned()), None))?;
							}
							recog.base.set_state(402);
							recog.base.match_token(LogQLParser_DIV,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(403);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(404);
							recog.metricExpr_rec(21)?;

							}
						}
					,
						4 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpModContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(406);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 19)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 19)".to_owned()), None))?;
							}
							recog.base.set_state(407);
							recog.base.match_token(LogQLParser_MOD,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(408);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(409);
							recog.metricExpr_rec(20)?;

							}
						}
					,
						5 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpAddContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(411);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 18)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 18)".to_owned()), None))?;
							}
							recog.base.set_state(412);
							recog.base.match_token(LogQLParser_ADD,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(413);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(414);
							recog.metricExpr_rec(19)?;

							}
						}
					,
						6 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpSubContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(416);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 17)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 17)".to_owned()), None))?;
							}
							recog.base.set_state(417);
							recog.base.match_token(LogQLParser_SUB,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(418);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(419);
							recog.metricExpr_rec(18)?;

							}
						}
					,
						7 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpEqlContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(421);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 16)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 16)".to_owned()), None))?;
							}
							recog.base.set_state(422);
							recog.base.match_token(LogQLParser_EQL,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(423);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(424);
							recog.metricExpr_rec(17)?;

							}
						}
					,
						8 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpNeqContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(426);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 15)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 15)".to_owned()), None))?;
							}
							recog.base.set_state(427);
							recog.base.match_token(LogQLParser_NE,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(428);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(429);
							recog.metricExpr_rec(16)?;

							}
						}
					,
						9 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpGtContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(431);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 14)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 14)".to_owned()), None))?;
							}
							recog.base.set_state(432);
							recog.base.match_token(LogQLParser_GT,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(433);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(434);
							recog.metricExpr_rec(15)?;

							}
						}
					,
						10 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpGeContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(436);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 13)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 13)".to_owned()), None))?;
							}
							recog.base.set_state(437);
							recog.base.match_token(LogQLParser_GE,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(438);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(439);
							recog.metricExpr_rec(14)?;

							}
						}
					,
						11 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpLtContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(441);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 12)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 12)".to_owned()), None))?;
							}
							recog.base.set_state(442);
							recog.base.match_token(LogQLParser_LT,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(443);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(444);
							recog.metricExpr_rec(13)?;

							}
						}
					,
						12 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpLeContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(446);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 11)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 11)".to_owned()), None))?;
							}
							recog.base.set_state(447);
							recog.base.match_token(LogQLParser_LE,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(448);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(449);
							recog.metricExpr_rec(12)?;

							}
						}
					,
						13 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpAndContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(451);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 10)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 10)".to_owned()), None))?;
							}
							recog.base.set_state(452);
							recog.base.match_token(LogQLParser_AND,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(453);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(454);
							recog.metricExpr_rec(11)?;

							}
						}
					,
						14 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpOrContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(456);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 9)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 9)".to_owned()), None))?;
							}
							recog.base.set_state(457);
							recog.base.match_token(LogQLParser_OR,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(458);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(459);
							recog.metricExpr_rec(10)?;

							}
						}
					,
						15 =>{
							{
							/*recRuleLabeledAltStartAction*/
							let mut tmp = BinaryOpUnlessContextExt::new(&**MetricExprContextExt::new(_parentctx.clone(), _parentState));
							recog.push_new_recursion_context(tmp.clone(), _startState, RULE_metricExpr)?;
							_localctx = tmp;
							recog.base.set_state(461);
							if !({let _localctx = Some(_localctx.clone());
							recog.precpred(None, 8)}) {
								Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 8)".to_owned()), None))?;
							}
							recog.base.set_state(462);
							recog.base.match_token(LogQLParser_UNLESS,&mut recog.err_handler)?;

							/*InvokeRule binOpModifier*/
							recog.base.set_state(463);
							recog.binOpModifier()?;

							/*InvokeRule metricExpr*/
							recog.base.set_state(464);
							recog.metricExpr_rec(9)?;

							}
						}

						_ => {}
					}
					} 
				}
				recog.base.set_state(470);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(32,&mut recog.base)?;
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
//------------------- rangeAggregationExpr ----------------
pub type RangeAggregationExprContextAll<'input> = RangeAggregationExprContext<'input>;


pub type RangeAggregationExprContext<'input> = BaseParserRuleContext<'input,RangeAggregationExprContextExt<'input>>;

#[derive(Clone)]
pub struct RangeAggregationExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for RangeAggregationExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeAggregationExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_rangeAggregationExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_rangeAggregationExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeAggregationExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeAggregationExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeAggregationExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeAggregationExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeAggregationExpr }
}
antlr4rust::tid!{RangeAggregationExprContextExt<'a>}

impl<'input> RangeAggregationExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeAggregationExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeAggregationExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RangeAggregationExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<RangeAggregationExprContextExt<'input>>{

fn rangeLogOp(&self) -> Option<Rc<RangeLogOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LPAREN, 0)
}
fn logRangeExpr(&self) -> Option<Rc<LogRangeExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RPAREN, 0)
}
fn rangeUnwrapOpNoGrouping(&self) -> Option<Rc<RangeUnwrapOpNoGroupingContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn unwrappedRangeExpr(&self) -> Option<Rc<UnwrappedRangeExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn rangeUnwrapOpWithGrouping(&self) -> Option<Rc<RangeUnwrapOpWithGroupingContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn grouping(&self) -> Option<Rc<GroupingContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token NUMBER
/// Returns `None` if there is no child corresponding to token NUMBER
fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_NUMBER, 0)
}
/// Retrieves first TerminalNode corresponding to token COMMA
/// Returns `None` if there is no child corresponding to token COMMA
fn COMMA(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COMMA, 0)
}

}

impl<'input> RangeAggregationExprContextAttrs<'input> for RangeAggregationExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn rangeAggregationExpr(&mut self,)
	-> Result<Rc<RangeAggregationExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeAggregationExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 64, RULE_rangeAggregationExpr);
        let mut _localctx: Rc<RangeAggregationExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(507);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(33,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule rangeLogOp*/
					recog.base.set_state(471);
					recog.rangeLogOp()?;

					recog.base.set_state(472);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule logRangeExpr*/
					recog.base.set_state(473);
					recog.logRangeExpr()?;

					recog.base.set_state(474);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule rangeUnwrapOpNoGrouping*/
					recog.base.set_state(476);
					recog.rangeUnwrapOpNoGrouping()?;

					recog.base.set_state(477);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule unwrappedRangeExpr*/
					recog.base.set_state(478);
					recog.unwrappedRangeExpr()?;

					recog.base.set_state(479);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					/*InvokeRule rangeUnwrapOpWithGrouping*/
					recog.base.set_state(481);
					recog.rangeUnwrapOpWithGrouping()?;

					recog.base.set_state(482);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule unwrappedRangeExpr*/
					recog.base.set_state(483);
					recog.unwrappedRangeExpr()?;

					recog.base.set_state(484);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule grouping*/
					recog.base.set_state(485);
					recog.grouping()?;

					}
				}
			,
				4 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					/*InvokeRule rangeUnwrapOpWithGrouping*/
					recog.base.set_state(487);
					recog.rangeUnwrapOpWithGrouping()?;

					recog.base.set_state(488);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule unwrappedRangeExpr*/
					recog.base.set_state(489);
					recog.unwrappedRangeExpr()?;

					recog.base.set_state(490);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				5 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 5)?;
					recog.base.enter_outer_alt(None, 5)?;
					{
					/*InvokeRule rangeUnwrapOpWithGrouping*/
					recog.base.set_state(492);
					recog.rangeUnwrapOpWithGrouping()?;

					recog.base.set_state(493);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(494);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

					recog.base.set_state(495);
					recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule unwrappedRangeExpr*/
					recog.base.set_state(496);
					recog.unwrappedRangeExpr()?;

					recog.base.set_state(497);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule grouping*/
					recog.base.set_state(498);
					recog.grouping()?;

					}
				}
			,
				6 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 6)?;
					recog.base.enter_outer_alt(None, 6)?;
					{
					/*InvokeRule rangeUnwrapOpWithGrouping*/
					recog.base.set_state(500);
					recog.rangeUnwrapOpWithGrouping()?;

					recog.base.set_state(501);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(502);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

					recog.base.set_state(503);
					recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule unwrappedRangeExpr*/
					recog.base.set_state(504);
					recog.unwrappedRangeExpr()?;

					recog.base.set_state(505);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- rangeLogOp ----------------
#[derive(Debug)]
pub enum RangeLogOpContextAll<'input>{
	RangeLogOpAbsentContext(RangeLogOpAbsentContext<'input>),
	RangeLogOpCountContext(RangeLogOpCountContext<'input>),
	RangeLogOpBytesRateContext(RangeLogOpBytesRateContext<'input>),
	RangeLogOpRateContext(RangeLogOpRateContext<'input>),
	RangeLogOpBytesContext(RangeLogOpBytesContext<'input>),
Error(RangeLogOpContext<'input>)
}
antlr4rust::tid!{RangeLogOpContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for RangeLogOpContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for RangeLogOpContextAll<'input>{}

impl<'input> Deref for RangeLogOpContextAll<'input>{
	type Target = dyn RangeLogOpContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use RangeLogOpContextAll::*;
		match self{
			RangeLogOpAbsentContext(inner) => inner,
			RangeLogOpCountContext(inner) => inner,
			RangeLogOpBytesRateContext(inner) => inner,
			RangeLogOpRateContext(inner) => inner,
			RangeLogOpBytesContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeLogOpContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeLogOpContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type RangeLogOpContext<'input> = BaseParserRuleContext<'input,RangeLogOpContextExt<'input>>;

#[derive(Clone)]
pub struct RangeLogOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for RangeLogOpContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeLogOpContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeLogOpContext<'input>{
}

impl<'input> CustomRuleContext<'input> for RangeLogOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeLogOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeLogOp }
}
antlr4rust::tid!{RangeLogOpContextExt<'a>}

impl<'input> RangeLogOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeLogOpContextAll<'input>> {
		Rc::new(
		RangeLogOpContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeLogOpContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait RangeLogOpContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<RangeLogOpContextExt<'input>>{


}

impl<'input> RangeLogOpContextAttrs<'input> for RangeLogOpContext<'input>{}

pub type RangeLogOpAbsentContext<'input> = BaseParserRuleContext<'input,RangeLogOpAbsentContextExt<'input>>;

pub trait RangeLogOpAbsentContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token ABSENT_OVER_TIME
	/// Returns `None` if there is no child corresponding to token ABSENT_OVER_TIME
	fn ABSENT_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ABSENT_OVER_TIME, 0)
	}
}

impl<'input> RangeLogOpAbsentContextAttrs<'input> for RangeLogOpAbsentContext<'input>{}

pub struct RangeLogOpAbsentContextExt<'input>{
	base:RangeLogOpContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeLogOpAbsentContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeLogOpAbsentContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeLogOpAbsentContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeLogOpAbsent(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeLogOpAbsent(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeLogOpAbsentContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeLogOpAbsent(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeLogOpAbsentContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeLogOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeLogOp }
}

impl<'input> Borrow<RangeLogOpContextExt<'input>> for RangeLogOpAbsentContext<'input>{
	fn borrow(&self) -> &RangeLogOpContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeLogOpContextExt<'input>> for RangeLogOpAbsentContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeLogOpContextExt<'input> { &mut self.base }
}

impl<'input> RangeLogOpContextAttrs<'input> for RangeLogOpAbsentContext<'input> {}

impl<'input> RangeLogOpAbsentContextExt<'input>{
	fn new(ctx: &dyn RangeLogOpContextAttrs<'input>) -> Rc<RangeLogOpContextAll<'input>>  {
		Rc::new(
			RangeLogOpContextAll::RangeLogOpAbsentContext(
				BaseParserRuleContext::copy_from(ctx,RangeLogOpAbsentContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeLogOpCountContext<'input> = BaseParserRuleContext<'input,RangeLogOpCountContextExt<'input>>;

pub trait RangeLogOpCountContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token COUNT_OVER_TIME
	/// Returns `None` if there is no child corresponding to token COUNT_OVER_TIME
	fn COUNT_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_COUNT_OVER_TIME, 0)
	}
}

impl<'input> RangeLogOpCountContextAttrs<'input> for RangeLogOpCountContext<'input>{}

pub struct RangeLogOpCountContextExt<'input>{
	base:RangeLogOpContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeLogOpCountContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeLogOpCountContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeLogOpCountContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeLogOpCount(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeLogOpCount(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeLogOpCountContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeLogOpCount(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeLogOpCountContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeLogOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeLogOp }
}

impl<'input> Borrow<RangeLogOpContextExt<'input>> for RangeLogOpCountContext<'input>{
	fn borrow(&self) -> &RangeLogOpContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeLogOpContextExt<'input>> for RangeLogOpCountContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeLogOpContextExt<'input> { &mut self.base }
}

impl<'input> RangeLogOpContextAttrs<'input> for RangeLogOpCountContext<'input> {}

impl<'input> RangeLogOpCountContextExt<'input>{
	fn new(ctx: &dyn RangeLogOpContextAttrs<'input>) -> Rc<RangeLogOpContextAll<'input>>  {
		Rc::new(
			RangeLogOpContextAll::RangeLogOpCountContext(
				BaseParserRuleContext::copy_from(ctx,RangeLogOpCountContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeLogOpBytesRateContext<'input> = BaseParserRuleContext<'input,RangeLogOpBytesRateContextExt<'input>>;

pub trait RangeLogOpBytesRateContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token BYTES_RATE
	/// Returns `None` if there is no child corresponding to token BYTES_RATE
	fn BYTES_RATE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_BYTES_RATE, 0)
	}
}

impl<'input> RangeLogOpBytesRateContextAttrs<'input> for RangeLogOpBytesRateContext<'input>{}

pub struct RangeLogOpBytesRateContextExt<'input>{
	base:RangeLogOpContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeLogOpBytesRateContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeLogOpBytesRateContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeLogOpBytesRateContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeLogOpBytesRate(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeLogOpBytesRate(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeLogOpBytesRateContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeLogOpBytesRate(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeLogOpBytesRateContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeLogOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeLogOp }
}

impl<'input> Borrow<RangeLogOpContextExt<'input>> for RangeLogOpBytesRateContext<'input>{
	fn borrow(&self) -> &RangeLogOpContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeLogOpContextExt<'input>> for RangeLogOpBytesRateContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeLogOpContextExt<'input> { &mut self.base }
}

impl<'input> RangeLogOpContextAttrs<'input> for RangeLogOpBytesRateContext<'input> {}

impl<'input> RangeLogOpBytesRateContextExt<'input>{
	fn new(ctx: &dyn RangeLogOpContextAttrs<'input>) -> Rc<RangeLogOpContextAll<'input>>  {
		Rc::new(
			RangeLogOpContextAll::RangeLogOpBytesRateContext(
				BaseParserRuleContext::copy_from(ctx,RangeLogOpBytesRateContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeLogOpRateContext<'input> = BaseParserRuleContext<'input,RangeLogOpRateContextExt<'input>>;

pub trait RangeLogOpRateContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token RATE
	/// Returns `None` if there is no child corresponding to token RATE
	fn RATE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RATE, 0)
	}
}

impl<'input> RangeLogOpRateContextAttrs<'input> for RangeLogOpRateContext<'input>{}

pub struct RangeLogOpRateContextExt<'input>{
	base:RangeLogOpContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeLogOpRateContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeLogOpRateContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeLogOpRateContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeLogOpRate(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeLogOpRate(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeLogOpRateContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeLogOpRate(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeLogOpRateContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeLogOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeLogOp }
}

impl<'input> Borrow<RangeLogOpContextExt<'input>> for RangeLogOpRateContext<'input>{
	fn borrow(&self) -> &RangeLogOpContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeLogOpContextExt<'input>> for RangeLogOpRateContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeLogOpContextExt<'input> { &mut self.base }
}

impl<'input> RangeLogOpContextAttrs<'input> for RangeLogOpRateContext<'input> {}

impl<'input> RangeLogOpRateContextExt<'input>{
	fn new(ctx: &dyn RangeLogOpContextAttrs<'input>) -> Rc<RangeLogOpContextAll<'input>>  {
		Rc::new(
			RangeLogOpContextAll::RangeLogOpRateContext(
				BaseParserRuleContext::copy_from(ctx,RangeLogOpRateContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeLogOpBytesContext<'input> = BaseParserRuleContext<'input,RangeLogOpBytesContextExt<'input>>;

pub trait RangeLogOpBytesContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token BYTES_OVER_TIME
	/// Returns `None` if there is no child corresponding to token BYTES_OVER_TIME
	fn BYTES_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_BYTES_OVER_TIME, 0)
	}
}

impl<'input> RangeLogOpBytesContextAttrs<'input> for RangeLogOpBytesContext<'input>{}

pub struct RangeLogOpBytesContextExt<'input>{
	base:RangeLogOpContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeLogOpBytesContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeLogOpBytesContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeLogOpBytesContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeLogOpBytes(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeLogOpBytes(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeLogOpBytesContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeLogOpBytes(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeLogOpBytesContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeLogOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeLogOp }
}

impl<'input> Borrow<RangeLogOpContextExt<'input>> for RangeLogOpBytesContext<'input>{
	fn borrow(&self) -> &RangeLogOpContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeLogOpContextExt<'input>> for RangeLogOpBytesContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeLogOpContextExt<'input> { &mut self.base }
}

impl<'input> RangeLogOpContextAttrs<'input> for RangeLogOpBytesContext<'input> {}

impl<'input> RangeLogOpBytesContextExt<'input>{
	fn new(ctx: &dyn RangeLogOpContextAttrs<'input>) -> Rc<RangeLogOpContextAll<'input>>  {
		Rc::new(
			RangeLogOpContextAll::RangeLogOpBytesContext(
				BaseParserRuleContext::copy_from(ctx,RangeLogOpBytesContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn rangeLogOp(&mut self,)
	-> Result<Rc<RangeLogOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeLogOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 66, RULE_rangeLogOp);
        let mut _localctx: Rc<RangeLogOpContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(514);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LogQLParser_COUNT_OVER_TIME 
				=> {
					let tmp = RangeLogOpCountContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(509);
					recog.base.match_token(LogQLParser_COUNT_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_RATE 
				=> {
					let tmp = RangeLogOpRateContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(510);
					recog.base.match_token(LogQLParser_RATE,&mut recog.err_handler)?;

					}
				}

			LogQLParser_BYTES_OVER_TIME 
				=> {
					let tmp = RangeLogOpBytesContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					recog.base.set_state(511);
					recog.base.match_token(LogQLParser_BYTES_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_BYTES_RATE 
				=> {
					let tmp = RangeLogOpBytesRateContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 4)?;
					_localctx = tmp;
					{
					recog.base.set_state(512);
					recog.base.match_token(LogQLParser_BYTES_RATE,&mut recog.err_handler)?;

					}
				}

			LogQLParser_ABSENT_OVER_TIME 
				=> {
					let tmp = RangeLogOpAbsentContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 5)?;
					_localctx = tmp;
					{
					recog.base.set_state(513);
					recog.base.match_token(LogQLParser_ABSENT_OVER_TIME,&mut recog.err_handler)?;

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
//------------------- rangeUnwrapOpNoGrouping ----------------
#[derive(Debug)]
pub enum RangeUnwrapOpNoGroupingContextAll<'input>{
	RangeUnwrapOpNoGroupRateContext(RangeUnwrapOpNoGroupRateContext<'input>),
	RangeUnwrapOpNoGroupRateCounterContext(RangeUnwrapOpNoGroupRateCounterContext<'input>),
	RangeUnwrapOpNoGroupSumContext(RangeUnwrapOpNoGroupSumContext<'input>),
Error(RangeUnwrapOpNoGroupingContext<'input>)
}
antlr4rust::tid!{RangeUnwrapOpNoGroupingContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for RangeUnwrapOpNoGroupingContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpNoGroupingContextAll<'input>{}

impl<'input> Deref for RangeUnwrapOpNoGroupingContextAll<'input>{
	type Target = dyn RangeUnwrapOpNoGroupingContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use RangeUnwrapOpNoGroupingContextAll::*;
		match self{
			RangeUnwrapOpNoGroupRateContext(inner) => inner,
			RangeUnwrapOpNoGroupRateCounterContext(inner) => inner,
			RangeUnwrapOpNoGroupSumContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpNoGroupingContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpNoGroupingContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type RangeUnwrapOpNoGroupingContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpNoGroupingContextExt<'input>>;

#[derive(Clone)]
pub struct RangeUnwrapOpNoGroupingContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpNoGroupingContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpNoGroupingContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpNoGroupingContext<'input>{
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpNoGroupingContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpNoGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpNoGrouping }
}
antlr4rust::tid!{RangeUnwrapOpNoGroupingContextExt<'a>}

impl<'input> RangeUnwrapOpNoGroupingContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeUnwrapOpNoGroupingContextAll<'input>> {
		Rc::new(
		RangeUnwrapOpNoGroupingContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeUnwrapOpNoGroupingContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait RangeUnwrapOpNoGroupingContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<RangeUnwrapOpNoGroupingContextExt<'input>>{


}

impl<'input> RangeUnwrapOpNoGroupingContextAttrs<'input> for RangeUnwrapOpNoGroupingContext<'input>{}

pub type RangeUnwrapOpNoGroupRateContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpNoGroupRateContextExt<'input>>;

pub trait RangeUnwrapOpNoGroupRateContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token RATE
	/// Returns `None` if there is no child corresponding to token RATE
	fn RATE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RATE, 0)
	}
}

impl<'input> RangeUnwrapOpNoGroupRateContextAttrs<'input> for RangeUnwrapOpNoGroupRateContext<'input>{}

pub struct RangeUnwrapOpNoGroupRateContextExt<'input>{
	base:RangeUnwrapOpNoGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpNoGroupRateContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpNoGroupRateContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpNoGroupRateContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpNoGroupRate(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpNoGroupRate(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpNoGroupRateContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpNoGroupRate(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpNoGroupRateContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpNoGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpNoGrouping }
}

impl<'input> Borrow<RangeUnwrapOpNoGroupingContextExt<'input>> for RangeUnwrapOpNoGroupRateContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpNoGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpNoGroupingContextExt<'input>> for RangeUnwrapOpNoGroupRateContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpNoGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpNoGroupingContextAttrs<'input> for RangeUnwrapOpNoGroupRateContext<'input> {}

impl<'input> RangeUnwrapOpNoGroupRateContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpNoGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpNoGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpNoGroupingContextAll::RangeUnwrapOpNoGroupRateContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpNoGroupRateContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeUnwrapOpNoGroupRateCounterContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpNoGroupRateCounterContextExt<'input>>;

pub trait RangeUnwrapOpNoGroupRateCounterContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token RATE_COUNTER
	/// Returns `None` if there is no child corresponding to token RATE_COUNTER
	fn RATE_COUNTER(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RATE_COUNTER, 0)
	}
}

impl<'input> RangeUnwrapOpNoGroupRateCounterContextAttrs<'input> for RangeUnwrapOpNoGroupRateCounterContext<'input>{}

pub struct RangeUnwrapOpNoGroupRateCounterContextExt<'input>{
	base:RangeUnwrapOpNoGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpNoGroupRateCounterContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpNoGroupRateCounterContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpNoGroupRateCounterContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpNoGroupRateCounter(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpNoGroupRateCounter(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpNoGroupRateCounterContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpNoGroupRateCounter(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpNoGroupRateCounterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpNoGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpNoGrouping }
}

impl<'input> Borrow<RangeUnwrapOpNoGroupingContextExt<'input>> for RangeUnwrapOpNoGroupRateCounterContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpNoGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpNoGroupingContextExt<'input>> for RangeUnwrapOpNoGroupRateCounterContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpNoGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpNoGroupingContextAttrs<'input> for RangeUnwrapOpNoGroupRateCounterContext<'input> {}

impl<'input> RangeUnwrapOpNoGroupRateCounterContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpNoGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpNoGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpNoGroupingContextAll::RangeUnwrapOpNoGroupRateCounterContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpNoGroupRateCounterContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeUnwrapOpNoGroupSumContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpNoGroupSumContextExt<'input>>;

pub trait RangeUnwrapOpNoGroupSumContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token SUM_OVER_TIME
	/// Returns `None` if there is no child corresponding to token SUM_OVER_TIME
	fn SUM_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_SUM_OVER_TIME, 0)
	}
}

impl<'input> RangeUnwrapOpNoGroupSumContextAttrs<'input> for RangeUnwrapOpNoGroupSumContext<'input>{}

pub struct RangeUnwrapOpNoGroupSumContextExt<'input>{
	base:RangeUnwrapOpNoGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpNoGroupSumContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpNoGroupSumContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpNoGroupSumContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpNoGroupSum(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpNoGroupSum(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpNoGroupSumContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpNoGroupSum(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpNoGroupSumContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpNoGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpNoGrouping }
}

impl<'input> Borrow<RangeUnwrapOpNoGroupingContextExt<'input>> for RangeUnwrapOpNoGroupSumContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpNoGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpNoGroupingContextExt<'input>> for RangeUnwrapOpNoGroupSumContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpNoGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpNoGroupingContextAttrs<'input> for RangeUnwrapOpNoGroupSumContext<'input> {}

impl<'input> RangeUnwrapOpNoGroupSumContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpNoGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpNoGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpNoGroupingContextAll::RangeUnwrapOpNoGroupSumContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpNoGroupSumContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn rangeUnwrapOpNoGrouping(&mut self,)
	-> Result<Rc<RangeUnwrapOpNoGroupingContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeUnwrapOpNoGroupingContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 68, RULE_rangeUnwrapOpNoGrouping);
        let mut _localctx: Rc<RangeUnwrapOpNoGroupingContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(519);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LogQLParser_SUM_OVER_TIME 
				=> {
					let tmp = RangeUnwrapOpNoGroupSumContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(516);
					recog.base.match_token(LogQLParser_SUM_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_RATE 
				=> {
					let tmp = RangeUnwrapOpNoGroupRateContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(517);
					recog.base.match_token(LogQLParser_RATE,&mut recog.err_handler)?;

					}
				}

			LogQLParser_RATE_COUNTER 
				=> {
					let tmp = RangeUnwrapOpNoGroupRateCounterContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					recog.base.set_state(518);
					recog.base.match_token(LogQLParser_RATE_COUNTER,&mut recog.err_handler)?;

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
//------------------- rangeUnwrapOpWithGrouping ----------------
#[derive(Debug)]
pub enum RangeUnwrapOpWithGroupingContextAll<'input>{
	RangeUnwrapOpLastContext(RangeUnwrapOpLastContext<'input>),
	RangeUnwrapOpStddevContext(RangeUnwrapOpStddevContext<'input>),
	RangeUnwrapOpFirstContext(RangeUnwrapOpFirstContext<'input>),
	RangeUnwrapOpStdvarContext(RangeUnwrapOpStdvarContext<'input>),
	RangeUnwrapOpMaxContext(RangeUnwrapOpMaxContext<'input>),
	RangeUnwrapOpAvgContext(RangeUnwrapOpAvgContext<'input>),
	RangeUnwrapOpMinContext(RangeUnwrapOpMinContext<'input>),
	RangeUnwrapOpQuantileContext(RangeUnwrapOpQuantileContext<'input>),
Error(RangeUnwrapOpWithGroupingContext<'input>)
}
antlr4rust::tid!{RangeUnwrapOpWithGroupingContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for RangeUnwrapOpWithGroupingContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpWithGroupingContextAll<'input>{}

impl<'input> Deref for RangeUnwrapOpWithGroupingContextAll<'input>{
	type Target = dyn RangeUnwrapOpWithGroupingContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use RangeUnwrapOpWithGroupingContextAll::*;
		match self{
			RangeUnwrapOpLastContext(inner) => inner,
			RangeUnwrapOpStddevContext(inner) => inner,
			RangeUnwrapOpFirstContext(inner) => inner,
			RangeUnwrapOpStdvarContext(inner) => inner,
			RangeUnwrapOpMaxContext(inner) => inner,
			RangeUnwrapOpAvgContext(inner) => inner,
			RangeUnwrapOpMinContext(inner) => inner,
			RangeUnwrapOpQuantileContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpWithGroupingContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpWithGroupingContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type RangeUnwrapOpWithGroupingContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpWithGroupingContextExt<'input>>;

#[derive(Clone)]
pub struct RangeUnwrapOpWithGroupingContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpWithGroupingContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpWithGroupingContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpWithGroupingContext<'input>{
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpWithGroupingContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpWithGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpWithGrouping }
}
antlr4rust::tid!{RangeUnwrapOpWithGroupingContextExt<'a>}

impl<'input> RangeUnwrapOpWithGroupingContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeUnwrapOpWithGroupingContextAll<'input>> {
		Rc::new(
		RangeUnwrapOpWithGroupingContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeUnwrapOpWithGroupingContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait RangeUnwrapOpWithGroupingContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<RangeUnwrapOpWithGroupingContextExt<'input>>{


}

impl<'input> RangeUnwrapOpWithGroupingContextAttrs<'input> for RangeUnwrapOpWithGroupingContext<'input>{}

pub type RangeUnwrapOpLastContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpLastContextExt<'input>>;

pub trait RangeUnwrapOpLastContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token LAST_OVER_TIME
	/// Returns `None` if there is no child corresponding to token LAST_OVER_TIME
	fn LAST_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LAST_OVER_TIME, 0)
	}
}

impl<'input> RangeUnwrapOpLastContextAttrs<'input> for RangeUnwrapOpLastContext<'input>{}

pub struct RangeUnwrapOpLastContextExt<'input>{
	base:RangeUnwrapOpWithGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpLastContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpLastContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpLastContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpLast(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpLast(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpLastContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpLast(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpLastContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpWithGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpWithGrouping }
}

impl<'input> Borrow<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpLastContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpWithGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpLastContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpWithGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpWithGroupingContextAttrs<'input> for RangeUnwrapOpLastContext<'input> {}

impl<'input> RangeUnwrapOpLastContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpWithGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpWithGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpLastContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpLastContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeUnwrapOpStddevContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpStddevContextExt<'input>>;

pub trait RangeUnwrapOpStddevContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token STDDEV_OVER_TIME
	/// Returns `None` if there is no child corresponding to token STDDEV_OVER_TIME
	fn STDDEV_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_STDDEV_OVER_TIME, 0)
	}
}

impl<'input> RangeUnwrapOpStddevContextAttrs<'input> for RangeUnwrapOpStddevContext<'input>{}

pub struct RangeUnwrapOpStddevContextExt<'input>{
	base:RangeUnwrapOpWithGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpStddevContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpStddevContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpStddevContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpStddev(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpStddev(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpStddevContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpStddev(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpStddevContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpWithGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpWithGrouping }
}

impl<'input> Borrow<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpStddevContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpWithGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpStddevContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpWithGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpWithGroupingContextAttrs<'input> for RangeUnwrapOpStddevContext<'input> {}

impl<'input> RangeUnwrapOpStddevContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpWithGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpWithGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpStddevContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpStddevContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeUnwrapOpFirstContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpFirstContextExt<'input>>;

pub trait RangeUnwrapOpFirstContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token FIRST_OVER_TIME
	/// Returns `None` if there is no child corresponding to token FIRST_OVER_TIME
	fn FIRST_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_FIRST_OVER_TIME, 0)
	}
}

impl<'input> RangeUnwrapOpFirstContextAttrs<'input> for RangeUnwrapOpFirstContext<'input>{}

pub struct RangeUnwrapOpFirstContextExt<'input>{
	base:RangeUnwrapOpWithGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpFirstContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpFirstContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpFirstContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpFirst(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpFirst(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpFirstContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpFirst(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpFirstContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpWithGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpWithGrouping }
}

impl<'input> Borrow<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpFirstContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpWithGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpFirstContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpWithGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpWithGroupingContextAttrs<'input> for RangeUnwrapOpFirstContext<'input> {}

impl<'input> RangeUnwrapOpFirstContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpWithGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpWithGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpFirstContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpFirstContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeUnwrapOpStdvarContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpStdvarContextExt<'input>>;

pub trait RangeUnwrapOpStdvarContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token STDVAR_OVER_TIME
	/// Returns `None` if there is no child corresponding to token STDVAR_OVER_TIME
	fn STDVAR_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_STDVAR_OVER_TIME, 0)
	}
}

impl<'input> RangeUnwrapOpStdvarContextAttrs<'input> for RangeUnwrapOpStdvarContext<'input>{}

pub struct RangeUnwrapOpStdvarContextExt<'input>{
	base:RangeUnwrapOpWithGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpStdvarContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpStdvarContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpStdvarContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpStdvar(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpStdvar(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpStdvarContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpStdvar(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpStdvarContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpWithGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpWithGrouping }
}

impl<'input> Borrow<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpStdvarContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpWithGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpStdvarContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpWithGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpWithGroupingContextAttrs<'input> for RangeUnwrapOpStdvarContext<'input> {}

impl<'input> RangeUnwrapOpStdvarContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpWithGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpWithGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpStdvarContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpStdvarContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeUnwrapOpMaxContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpMaxContextExt<'input>>;

pub trait RangeUnwrapOpMaxContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token MAX_OVER_TIME
	/// Returns `None` if there is no child corresponding to token MAX_OVER_TIME
	fn MAX_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_MAX_OVER_TIME, 0)
	}
}

impl<'input> RangeUnwrapOpMaxContextAttrs<'input> for RangeUnwrapOpMaxContext<'input>{}

pub struct RangeUnwrapOpMaxContextExt<'input>{
	base:RangeUnwrapOpWithGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpMaxContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpMaxContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpMaxContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpMax(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpMax(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpMaxContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpMax(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpMaxContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpWithGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpWithGrouping }
}

impl<'input> Borrow<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpMaxContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpWithGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpMaxContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpWithGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpWithGroupingContextAttrs<'input> for RangeUnwrapOpMaxContext<'input> {}

impl<'input> RangeUnwrapOpMaxContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpWithGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpWithGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpMaxContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpMaxContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeUnwrapOpAvgContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpAvgContextExt<'input>>;

pub trait RangeUnwrapOpAvgContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token AVG_OVER_TIME
	/// Returns `None` if there is no child corresponding to token AVG_OVER_TIME
	fn AVG_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_AVG_OVER_TIME, 0)
	}
}

impl<'input> RangeUnwrapOpAvgContextAttrs<'input> for RangeUnwrapOpAvgContext<'input>{}

pub struct RangeUnwrapOpAvgContextExt<'input>{
	base:RangeUnwrapOpWithGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpAvgContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpAvgContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpAvgContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpAvg(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpAvg(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpAvgContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpAvg(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpAvgContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpWithGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpWithGrouping }
}

impl<'input> Borrow<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpAvgContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpWithGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpAvgContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpWithGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpWithGroupingContextAttrs<'input> for RangeUnwrapOpAvgContext<'input> {}

impl<'input> RangeUnwrapOpAvgContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpWithGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpWithGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpAvgContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpAvgContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeUnwrapOpMinContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpMinContextExt<'input>>;

pub trait RangeUnwrapOpMinContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token MIN_OVER_TIME
	/// Returns `None` if there is no child corresponding to token MIN_OVER_TIME
	fn MIN_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_MIN_OVER_TIME, 0)
	}
}

impl<'input> RangeUnwrapOpMinContextAttrs<'input> for RangeUnwrapOpMinContext<'input>{}

pub struct RangeUnwrapOpMinContextExt<'input>{
	base:RangeUnwrapOpWithGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpMinContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpMinContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpMinContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpMin(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpMin(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpMinContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpMin(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpMinContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpWithGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpWithGrouping }
}

impl<'input> Borrow<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpMinContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpWithGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpMinContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpWithGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpWithGroupingContextAttrs<'input> for RangeUnwrapOpMinContext<'input> {}

impl<'input> RangeUnwrapOpMinContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpWithGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpWithGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpMinContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpMinContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type RangeUnwrapOpQuantileContext<'input> = BaseParserRuleContext<'input,RangeUnwrapOpQuantileContextExt<'input>>;

pub trait RangeUnwrapOpQuantileContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token QUANTILE_OVER_TIME
	/// Returns `None` if there is no child corresponding to token QUANTILE_OVER_TIME
	fn QUANTILE_OVER_TIME(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_QUANTILE_OVER_TIME, 0)
	}
}

impl<'input> RangeUnwrapOpQuantileContextAttrs<'input> for RangeUnwrapOpQuantileContext<'input>{}

pub struct RangeUnwrapOpQuantileContextExt<'input>{
	base:RangeUnwrapOpWithGroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{RangeUnwrapOpQuantileContextExt<'a>}

impl<'input> LogQLParserContext<'input> for RangeUnwrapOpQuantileContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeUnwrapOpQuantileContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_rangeUnwrapOpQuantile(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_rangeUnwrapOpQuantile(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeUnwrapOpQuantileContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_rangeUnwrapOpQuantile(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeUnwrapOpQuantileContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_rangeUnwrapOpWithGrouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_rangeUnwrapOpWithGrouping }
}

impl<'input> Borrow<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpQuantileContext<'input>{
	fn borrow(&self) -> &RangeUnwrapOpWithGroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<RangeUnwrapOpWithGroupingContextExt<'input>> for RangeUnwrapOpQuantileContext<'input>{
	fn borrow_mut(&mut self) -> &mut RangeUnwrapOpWithGroupingContextExt<'input> { &mut self.base }
}

impl<'input> RangeUnwrapOpWithGroupingContextAttrs<'input> for RangeUnwrapOpQuantileContext<'input> {}

impl<'input> RangeUnwrapOpQuantileContextExt<'input>{
	fn new(ctx: &dyn RangeUnwrapOpWithGroupingContextAttrs<'input>) -> Rc<RangeUnwrapOpWithGroupingContextAll<'input>>  {
		Rc::new(
			RangeUnwrapOpWithGroupingContextAll::RangeUnwrapOpQuantileContext(
				BaseParserRuleContext::copy_from(ctx,RangeUnwrapOpQuantileContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn rangeUnwrapOpWithGrouping(&mut self,)
	-> Result<Rc<RangeUnwrapOpWithGroupingContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeUnwrapOpWithGroupingContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 70, RULE_rangeUnwrapOpWithGrouping);
        let mut _localctx: Rc<RangeUnwrapOpWithGroupingContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(529);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LogQLParser_AVG_OVER_TIME 
				=> {
					let tmp = RangeUnwrapOpAvgContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(521);
					recog.base.match_token(LogQLParser_AVG_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_MIN_OVER_TIME 
				=> {
					let tmp = RangeUnwrapOpMinContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(522);
					recog.base.match_token(LogQLParser_MIN_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_MAX_OVER_TIME 
				=> {
					let tmp = RangeUnwrapOpMaxContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					recog.base.set_state(523);
					recog.base.match_token(LogQLParser_MAX_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_STDDEV_OVER_TIME 
				=> {
					let tmp = RangeUnwrapOpStddevContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 4)?;
					_localctx = tmp;
					{
					recog.base.set_state(524);
					recog.base.match_token(LogQLParser_STDDEV_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_STDVAR_OVER_TIME 
				=> {
					let tmp = RangeUnwrapOpStdvarContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 5)?;
					_localctx = tmp;
					{
					recog.base.set_state(525);
					recog.base.match_token(LogQLParser_STDVAR_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_QUANTILE_OVER_TIME 
				=> {
					let tmp = RangeUnwrapOpQuantileContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 6)?;
					_localctx = tmp;
					{
					recog.base.set_state(526);
					recog.base.match_token(LogQLParser_QUANTILE_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_FIRST_OVER_TIME 
				=> {
					let tmp = RangeUnwrapOpFirstContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 7)?;
					_localctx = tmp;
					{
					recog.base.set_state(527);
					recog.base.match_token(LogQLParser_FIRST_OVER_TIME,&mut recog.err_handler)?;

					}
				}

			LogQLParser_LAST_OVER_TIME 
				=> {
					let tmp = RangeUnwrapOpLastContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 8)?;
					_localctx = tmp;
					{
					recog.base.set_state(528);
					recog.base.match_token(LogQLParser_LAST_OVER_TIME,&mut recog.err_handler)?;

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
//------------------- vectorAggregationExpr ----------------
pub type VectorAggregationExprContextAll<'input> = VectorAggregationExprContext<'input>;


pub type VectorAggregationExprContext<'input> = BaseParserRuleContext<'input,VectorAggregationExprContextExt<'input>>;

#[derive(Clone)]
pub struct VectorAggregationExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for VectorAggregationExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for VectorAggregationExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_vectorAggregationExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_vectorAggregationExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for VectorAggregationExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorAggregationExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorAggregationExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vectorAggregationExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vectorAggregationExpr }
}
antlr4rust::tid!{VectorAggregationExprContextExt<'a>}

impl<'input> VectorAggregationExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<VectorAggregationExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,VectorAggregationExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait VectorAggregationExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<VectorAggregationExprContextExt<'input>>{

fn vectorOp(&self) -> Option<Rc<VectorOpContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LPAREN, 0)
}
fn metricExpr(&self) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RPAREN, 0)
}
fn grouping(&self) -> Option<Rc<GroupingContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token NUMBER
/// Returns `None` if there is no child corresponding to token NUMBER
fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_NUMBER, 0)
}
/// Retrieves first TerminalNode corresponding to token COMMA
/// Returns `None` if there is no child corresponding to token COMMA
fn COMMA(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COMMA, 0)
}

}

impl<'input> VectorAggregationExprContextAttrs<'input> for VectorAggregationExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn vectorAggregationExpr(&mut self,)
	-> Result<Rc<VectorAggregationExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = VectorAggregationExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 72, RULE_vectorAggregationExpr);
        let mut _localctx: Rc<VectorAggregationExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(571);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(37,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule vectorOp*/
					recog.base.set_state(531);
					recog.vectorOp()?;

					recog.base.set_state(532);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule metricExpr*/
					recog.base.set_state(533);
					recog.metricExpr_rec(0)?;

					recog.base.set_state(534);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule vectorOp*/
					recog.base.set_state(536);
					recog.vectorOp()?;

					/*InvokeRule grouping*/
					recog.base.set_state(537);
					recog.grouping()?;

					recog.base.set_state(538);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule metricExpr*/
					recog.base.set_state(539);
					recog.metricExpr_rec(0)?;

					recog.base.set_state(540);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					/*InvokeRule vectorOp*/
					recog.base.set_state(542);
					recog.vectorOp()?;

					recog.base.set_state(543);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule metricExpr*/
					recog.base.set_state(544);
					recog.metricExpr_rec(0)?;

					recog.base.set_state(545);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule grouping*/
					recog.base.set_state(546);
					recog.grouping()?;

					}
				}
			,
				4 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					/*InvokeRule vectorOp*/
					recog.base.set_state(548);
					recog.vectorOp()?;

					recog.base.set_state(549);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(550);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

					recog.base.set_state(551);
					recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule metricExpr*/
					recog.base.set_state(552);
					recog.metricExpr_rec(0)?;

					recog.base.set_state(553);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				5 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 5)?;
					recog.base.enter_outer_alt(None, 5)?;
					{
					/*InvokeRule vectorOp*/
					recog.base.set_state(555);
					recog.vectorOp()?;

					recog.base.set_state(556);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(557);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

					recog.base.set_state(558);
					recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule metricExpr*/
					recog.base.set_state(559);
					recog.metricExpr_rec(0)?;

					recog.base.set_state(560);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule grouping*/
					recog.base.set_state(561);
					recog.grouping()?;

					}
				}
			,
				6 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 6)?;
					recog.base.enter_outer_alt(None, 6)?;
					{
					/*InvokeRule vectorOp*/
					recog.base.set_state(563);
					recog.vectorOp()?;

					/*InvokeRule grouping*/
					recog.base.set_state(564);
					recog.grouping()?;

					recog.base.set_state(565);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(566);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

					recog.base.set_state(567);
					recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule metricExpr*/
					recog.base.set_state(568);
					recog.metricExpr_rec(0)?;

					recog.base.set_state(569);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- vectorOp ----------------
pub type VectorOpContextAll<'input> = VectorOpContext<'input>;


pub type VectorOpContext<'input> = BaseParserRuleContext<'input,VectorOpContextExt<'input>>;

#[derive(Clone)]
pub struct VectorOpContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for VectorOpContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for VectorOpContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_vectorOp(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_vectorOp(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for VectorOpContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorOp(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorOpContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vectorOp }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vectorOp }
}
antlr4rust::tid!{VectorOpContextExt<'a>}

impl<'input> VectorOpContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<VectorOpContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,VectorOpContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait VectorOpContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<VectorOpContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token SUM
/// Returns `None` if there is no child corresponding to token SUM
fn SUM(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_SUM, 0)
}
/// Retrieves first TerminalNode corresponding to token AVG
/// Returns `None` if there is no child corresponding to token AVG
fn AVG(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_AVG, 0)
}
/// Retrieves first TerminalNode corresponding to token COUNT
/// Returns `None` if there is no child corresponding to token COUNT
fn COUNT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COUNT, 0)
}
/// Retrieves first TerminalNode corresponding to token MAX
/// Returns `None` if there is no child corresponding to token MAX
fn MAX(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_MAX, 0)
}
/// Retrieves first TerminalNode corresponding to token MIN
/// Returns `None` if there is no child corresponding to token MIN
fn MIN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_MIN, 0)
}
/// Retrieves first TerminalNode corresponding to token STDDEV
/// Returns `None` if there is no child corresponding to token STDDEV
fn STDDEV(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_STDDEV, 0)
}
/// Retrieves first TerminalNode corresponding to token STDVAR
/// Returns `None` if there is no child corresponding to token STDVAR
fn STDVAR(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_STDVAR, 0)
}
/// Retrieves first TerminalNode corresponding to token BOTTOMK
/// Returns `None` if there is no child corresponding to token BOTTOMK
fn BOTTOMK(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_BOTTOMK, 0)
}
/// Retrieves first TerminalNode corresponding to token TOPK
/// Returns `None` if there is no child corresponding to token TOPK
fn TOPK(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_TOPK, 0)
}
/// Retrieves first TerminalNode corresponding to token APPROX_TOPK
/// Returns `None` if there is no child corresponding to token APPROX_TOPK
fn APPROX_TOPK(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_APPROX_TOPK, 0)
}
/// Retrieves first TerminalNode corresponding to token SORT
/// Returns `None` if there is no child corresponding to token SORT
fn SORT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_SORT, 0)
}
/// Retrieves first TerminalNode corresponding to token SORT_DESC
/// Returns `None` if there is no child corresponding to token SORT_DESC
fn SORT_DESC(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_SORT_DESC, 0)
}

}

impl<'input> VectorOpContextAttrs<'input> for VectorOpContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn vectorOp(&mut self,)
	-> Result<Rc<VectorOpContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = VectorOpContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 74, RULE_vectorOp);
        let mut _localctx: Rc<VectorOpContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(573);
			_la = recog.base.input.la(1);
			if { !(((((_la - 40)) & !0x3f) == 0 && ((1usize << (_la - 40)) & 50332671) != 0)) } {
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

impl<'input> LogQLParserContext<'input> for BinOpModifierContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinOpModifierContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_binOpModifier(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_binOpModifier(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinOpModifierContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binOpModifier(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinOpModifierContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_binOpModifier }
	//fn type_rule_index() -> usize where Self: Sized { RULE_binOpModifier }
}
antlr4rust::tid!{BinOpModifierContextExt<'a>}

impl<'input> BinOpModifierContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<BinOpModifierContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,BinOpModifierContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait BinOpModifierContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<BinOpModifierContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token BOOL
/// Returns `None` if there is no child corresponding to token BOOL
fn BOOL(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_BOOL, 0)
}
fn onOrIgnoringModifier(&self) -> Option<Rc<OnOrIgnoringModifierContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token GROUP_LEFT
/// Returns `None` if there is no child corresponding to token GROUP_LEFT
fn GROUP_LEFT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_GROUP_LEFT, 0)
}
/// Retrieves first TerminalNode corresponding to token GROUP_RIGHT
/// Returns `None` if there is no child corresponding to token GROUP_RIGHT
fn GROUP_RIGHT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_GROUP_RIGHT, 0)
}
fn binOpGroupingLabels(&self) -> Option<Rc<BinOpGroupingLabelsContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> BinOpModifierContextAttrs<'input> for BinOpModifierContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn binOpModifier(&mut self,)
	-> Result<Rc<BinOpModifierContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = BinOpModifierContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 76, RULE_binOpModifier);
        let mut _localctx: Rc<BinOpModifierContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(576);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==LogQLParser_BOOL {
				{
				recog.base.set_state(575);
				recog.base.match_token(LogQLParser_BOOL,&mut recog.err_handler)?;

				}
			}

			recog.base.set_state(585);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==LogQLParser_ON || _la==LogQLParser_IGNORING {
				{
				/*InvokeRule onOrIgnoringModifier*/
				recog.base.set_state(578);
				recog.onOrIgnoringModifier()?;

				recog.base.set_state(583);
				recog.err_handler.sync(&mut recog.base)?;
				_la = recog.base.input.la(1);
				if _la==LogQLParser_GROUP_LEFT || _la==LogQLParser_GROUP_RIGHT {
					{
					recog.base.set_state(579);
					_la = recog.base.input.la(1);
					if { !(_la==LogQLParser_GROUP_LEFT || _la==LogQLParser_GROUP_RIGHT) } {
						recog.err_handler.recover_inline(&mut recog.base)?;

					}
					else {
						if  recog.base.input.la(1)==TOKEN_EOF { recog.base.matched_eof = true };
						recog.err_handler.report_match(&mut recog.base);
						recog.base.consume(&mut recog.err_handler);
					}
					recog.base.set_state(581);
					recog.err_handler.sync(&mut recog.base)?;
					match  recog.interpreter.adaptive_predict(39,&mut recog.base)? {
						x if x == 1=>{
							{
							/*InvokeRule binOpGroupingLabels*/
							recog.base.set_state(580);
							recog.binOpGroupingLabels()?;

							}
						}

						_ => {}
					}
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
//------------------- onOrIgnoringModifier ----------------
pub type OnOrIgnoringModifierContextAll<'input> = OnOrIgnoringModifierContext<'input>;


pub type OnOrIgnoringModifierContext<'input> = BaseParserRuleContext<'input,OnOrIgnoringModifierContextExt<'input>>;

#[derive(Clone)]
pub struct OnOrIgnoringModifierContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for OnOrIgnoringModifierContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for OnOrIgnoringModifierContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_onOrIgnoringModifier(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_onOrIgnoringModifier(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for OnOrIgnoringModifierContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_onOrIgnoringModifier(self);
	}
}

impl<'input> CustomRuleContext<'input> for OnOrIgnoringModifierContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_onOrIgnoringModifier }
	//fn type_rule_index() -> usize where Self: Sized { RULE_onOrIgnoringModifier }
}
antlr4rust::tid!{OnOrIgnoringModifierContextExt<'a>}

impl<'input> OnOrIgnoringModifierContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<OnOrIgnoringModifierContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,OnOrIgnoringModifierContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait OnOrIgnoringModifierContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<OnOrIgnoringModifierContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token IGNORING
/// Returns `None` if there is no child corresponding to token IGNORING
fn IGNORING(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_IGNORING, 0)
}
fn binOpGroupingLabels(&self) -> Option<Rc<BinOpGroupingLabelsContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token ON
/// Returns `None` if there is no child corresponding to token ON
fn ON(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_ON, 0)
}

}

impl<'input> OnOrIgnoringModifierContextAttrs<'input> for OnOrIgnoringModifierContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn onOrIgnoringModifier(&mut self,)
	-> Result<Rc<OnOrIgnoringModifierContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = OnOrIgnoringModifierContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 78, RULE_onOrIgnoringModifier);
        let mut _localctx: Rc<OnOrIgnoringModifierContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(591);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LogQLParser_IGNORING 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(587);
					recog.base.match_token(LogQLParser_IGNORING,&mut recog.err_handler)?;

					/*InvokeRule binOpGroupingLabels*/
					recog.base.set_state(588);
					recog.binOpGroupingLabels()?;

					}
				}

			LogQLParser_ON 
				=> {
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(589);
					recog.base.match_token(LogQLParser_ON,&mut recog.err_handler)?;

					/*InvokeRule binOpGroupingLabels*/
					recog.base.set_state(590);
					recog.binOpGroupingLabels()?;

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
//------------------- grouping ----------------
#[derive(Debug)]
pub enum GroupingContextAll<'input>{
	GroupingByContext(GroupingByContext<'input>),
	GroupingWithoutEmptyContext(GroupingWithoutEmptyContext<'input>),
	GroupingByEmptyContext(GroupingByEmptyContext<'input>),
	GroupingWithoutContext(GroupingWithoutContext<'input>),
Error(GroupingContext<'input>)
}
antlr4rust::tid!{GroupingContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for GroupingContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for GroupingContextAll<'input>{}

impl<'input> Deref for GroupingContextAll<'input>{
	type Target = dyn GroupingContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use GroupingContextAll::*;
		match self{
			GroupingByContext(inner) => inner,
			GroupingWithoutEmptyContext(inner) => inner,
			GroupingByEmptyContext(inner) => inner,
			GroupingWithoutContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for GroupingContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for GroupingContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type GroupingContext<'input> = BaseParserRuleContext<'input,GroupingContextExt<'input>>;

#[derive(Clone)]
pub struct GroupingContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for GroupingContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for GroupingContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for GroupingContext<'input>{
}

impl<'input> CustomRuleContext<'input> for GroupingContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_grouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_grouping }
}
antlr4rust::tid!{GroupingContextExt<'a>}

impl<'input> GroupingContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<GroupingContextAll<'input>> {
		Rc::new(
		GroupingContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,GroupingContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait GroupingContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<GroupingContextExt<'input>>{


}

impl<'input> GroupingContextAttrs<'input> for GroupingContext<'input>{}

pub type GroupingByContext<'input> = BaseParserRuleContext<'input,GroupingByContextExt<'input>>;

pub trait GroupingByContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token BY
	/// Returns `None` if there is no child corresponding to token BY
	fn BY(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_BY, 0)
	}
	/// Retrieves first TerminalNode corresponding to token LPAREN
	/// Returns `None` if there is no child corresponding to token LPAREN
	fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LPAREN, 0)
	}
	fn groupingLabels(&self) -> Option<Rc<GroupingLabelsContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	/// Retrieves first TerminalNode corresponding to token RPAREN
	/// Returns `None` if there is no child corresponding to token RPAREN
	fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RPAREN, 0)
	}
}

impl<'input> GroupingByContextAttrs<'input> for GroupingByContext<'input>{}

pub struct GroupingByContextExt<'input>{
	base:GroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{GroupingByContextExt<'a>}

impl<'input> LogQLParserContext<'input> for GroupingByContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for GroupingByContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_groupingBy(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_groupingBy(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for GroupingByContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_groupingBy(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupingByContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_grouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_grouping }
}

impl<'input> Borrow<GroupingContextExt<'input>> for GroupingByContext<'input>{
	fn borrow(&self) -> &GroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<GroupingContextExt<'input>> for GroupingByContext<'input>{
	fn borrow_mut(&mut self) -> &mut GroupingContextExt<'input> { &mut self.base }
}

impl<'input> GroupingContextAttrs<'input> for GroupingByContext<'input> {}

impl<'input> GroupingByContextExt<'input>{
	fn new(ctx: &dyn GroupingContextAttrs<'input>) -> Rc<GroupingContextAll<'input>>  {
		Rc::new(
			GroupingContextAll::GroupingByContext(
				BaseParserRuleContext::copy_from(ctx,GroupingByContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type GroupingWithoutEmptyContext<'input> = BaseParserRuleContext<'input,GroupingWithoutEmptyContextExt<'input>>;

pub trait GroupingWithoutEmptyContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token WITHOUT
	/// Returns `None` if there is no child corresponding to token WITHOUT
	fn WITHOUT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_WITHOUT, 0)
	}
	/// Retrieves first TerminalNode corresponding to token LPAREN
	/// Returns `None` if there is no child corresponding to token LPAREN
	fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LPAREN, 0)
	}
	/// Retrieves first TerminalNode corresponding to token RPAREN
	/// Returns `None` if there is no child corresponding to token RPAREN
	fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RPAREN, 0)
	}
}

impl<'input> GroupingWithoutEmptyContextAttrs<'input> for GroupingWithoutEmptyContext<'input>{}

pub struct GroupingWithoutEmptyContextExt<'input>{
	base:GroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{GroupingWithoutEmptyContextExt<'a>}

impl<'input> LogQLParserContext<'input> for GroupingWithoutEmptyContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for GroupingWithoutEmptyContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_groupingWithoutEmpty(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_groupingWithoutEmpty(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for GroupingWithoutEmptyContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_groupingWithoutEmpty(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupingWithoutEmptyContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_grouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_grouping }
}

impl<'input> Borrow<GroupingContextExt<'input>> for GroupingWithoutEmptyContext<'input>{
	fn borrow(&self) -> &GroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<GroupingContextExt<'input>> for GroupingWithoutEmptyContext<'input>{
	fn borrow_mut(&mut self) -> &mut GroupingContextExt<'input> { &mut self.base }
}

impl<'input> GroupingContextAttrs<'input> for GroupingWithoutEmptyContext<'input> {}

impl<'input> GroupingWithoutEmptyContextExt<'input>{
	fn new(ctx: &dyn GroupingContextAttrs<'input>) -> Rc<GroupingContextAll<'input>>  {
		Rc::new(
			GroupingContextAll::GroupingWithoutEmptyContext(
				BaseParserRuleContext::copy_from(ctx,GroupingWithoutEmptyContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type GroupingByEmptyContext<'input> = BaseParserRuleContext<'input,GroupingByEmptyContextExt<'input>>;

pub trait GroupingByEmptyContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token BY
	/// Returns `None` if there is no child corresponding to token BY
	fn BY(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_BY, 0)
	}
	/// Retrieves first TerminalNode corresponding to token LPAREN
	/// Returns `None` if there is no child corresponding to token LPAREN
	fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LPAREN, 0)
	}
	/// Retrieves first TerminalNode corresponding to token RPAREN
	/// Returns `None` if there is no child corresponding to token RPAREN
	fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RPAREN, 0)
	}
}

impl<'input> GroupingByEmptyContextAttrs<'input> for GroupingByEmptyContext<'input>{}

pub struct GroupingByEmptyContextExt<'input>{
	base:GroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{GroupingByEmptyContextExt<'a>}

impl<'input> LogQLParserContext<'input> for GroupingByEmptyContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for GroupingByEmptyContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_groupingByEmpty(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_groupingByEmpty(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for GroupingByEmptyContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_groupingByEmpty(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupingByEmptyContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_grouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_grouping }
}

impl<'input> Borrow<GroupingContextExt<'input>> for GroupingByEmptyContext<'input>{
	fn borrow(&self) -> &GroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<GroupingContextExt<'input>> for GroupingByEmptyContext<'input>{
	fn borrow_mut(&mut self) -> &mut GroupingContextExt<'input> { &mut self.base }
}

impl<'input> GroupingContextAttrs<'input> for GroupingByEmptyContext<'input> {}

impl<'input> GroupingByEmptyContextExt<'input>{
	fn new(ctx: &dyn GroupingContextAttrs<'input>) -> Rc<GroupingContextAll<'input>>  {
		Rc::new(
			GroupingContextAll::GroupingByEmptyContext(
				BaseParserRuleContext::copy_from(ctx,GroupingByEmptyContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type GroupingWithoutContext<'input> = BaseParserRuleContext<'input,GroupingWithoutContextExt<'input>>;

pub trait GroupingWithoutContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token WITHOUT
	/// Returns `None` if there is no child corresponding to token WITHOUT
	fn WITHOUT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_WITHOUT, 0)
	}
	/// Retrieves first TerminalNode corresponding to token LPAREN
	/// Returns `None` if there is no child corresponding to token LPAREN
	fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LPAREN, 0)
	}
	fn groupingLabels(&self) -> Option<Rc<GroupingLabelsContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	/// Retrieves first TerminalNode corresponding to token RPAREN
	/// Returns `None` if there is no child corresponding to token RPAREN
	fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RPAREN, 0)
	}
}

impl<'input> GroupingWithoutContextAttrs<'input> for GroupingWithoutContext<'input>{}

pub struct GroupingWithoutContextExt<'input>{
	base:GroupingContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{GroupingWithoutContextExt<'a>}

impl<'input> LogQLParserContext<'input> for GroupingWithoutContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for GroupingWithoutContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_groupingWithout(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_groupingWithout(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for GroupingWithoutContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_groupingWithout(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupingWithoutContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_grouping }
	//fn type_rule_index() -> usize where Self: Sized { RULE_grouping }
}

impl<'input> Borrow<GroupingContextExt<'input>> for GroupingWithoutContext<'input>{
	fn borrow(&self) -> &GroupingContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<GroupingContextExt<'input>> for GroupingWithoutContext<'input>{
	fn borrow_mut(&mut self) -> &mut GroupingContextExt<'input> { &mut self.base }
}

impl<'input> GroupingContextAttrs<'input> for GroupingWithoutContext<'input> {}

impl<'input> GroupingWithoutContextExt<'input>{
	fn new(ctx: &dyn GroupingContextAttrs<'input>) -> Rc<GroupingContextAll<'input>>  {
		Rc::new(
			GroupingContextAll::GroupingWithoutContext(
				BaseParserRuleContext::copy_from(ctx,GroupingWithoutContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn grouping(&mut self,)
	-> Result<Rc<GroupingContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = GroupingContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 80, RULE_grouping);
        let mut _localctx: Rc<GroupingContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(609);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(43,&mut recog.base)? {
				1 =>{
					let tmp = GroupingByContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(593);
					recog.base.match_token(LogQLParser_BY,&mut recog.err_handler)?;

					recog.base.set_state(594);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule groupingLabels*/
					recog.base.set_state(595);
					recog.groupingLabels()?;

					recog.base.set_state(596);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					let tmp = GroupingWithoutContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(598);
					recog.base.match_token(LogQLParser_WITHOUT,&mut recog.err_handler)?;

					recog.base.set_state(599);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule groupingLabels*/
					recog.base.set_state(600);
					recog.groupingLabels()?;

					recog.base.set_state(601);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				3 =>{
					let tmp = GroupingByEmptyContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					recog.base.set_state(603);
					recog.base.match_token(LogQLParser_BY,&mut recog.err_handler)?;

					recog.base.set_state(604);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(605);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				4 =>{
					let tmp = GroupingWithoutEmptyContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 4)?;
					_localctx = tmp;
					{
					recog.base.set_state(606);
					recog.base.match_token(LogQLParser_WITHOUT,&mut recog.err_handler)?;

					recog.base.set_state(607);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(608);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- binOpGroupingLabels ----------------
pub type BinOpGroupingLabelsContextAll<'input> = BinOpGroupingLabelsContext<'input>;


pub type BinOpGroupingLabelsContext<'input> = BaseParserRuleContext<'input,BinOpGroupingLabelsContextExt<'input>>;

#[derive(Clone)]
pub struct BinOpGroupingLabelsContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for BinOpGroupingLabelsContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for BinOpGroupingLabelsContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_binOpGroupingLabels(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_binOpGroupingLabels(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for BinOpGroupingLabelsContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_binOpGroupingLabels(self);
	}
}

impl<'input> CustomRuleContext<'input> for BinOpGroupingLabelsContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_binOpGroupingLabels }
	//fn type_rule_index() -> usize where Self: Sized { RULE_binOpGroupingLabels }
}
antlr4rust::tid!{BinOpGroupingLabelsContextExt<'a>}

impl<'input> BinOpGroupingLabelsContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<BinOpGroupingLabelsContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,BinOpGroupingLabelsContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait BinOpGroupingLabelsContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<BinOpGroupingLabelsContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LPAREN, 0)
}
fn groupingLabelList(&self) -> Option<Rc<GroupingLabelListContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token COMMA
/// Returns `None` if there is no child corresponding to token COMMA
fn COMMA(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COMMA, 0)
}

}

impl<'input> BinOpGroupingLabelsContextAttrs<'input> for BinOpGroupingLabelsContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn binOpGroupingLabels(&mut self,)
	-> Result<Rc<BinOpGroupingLabelsContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = BinOpGroupingLabelsContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 82, RULE_binOpGroupingLabels);
        let mut _localctx: Rc<BinOpGroupingLabelsContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(622);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(44,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(611);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule groupingLabelList*/
					recog.base.set_state(612);
					recog.groupingLabelList_rec(0)?;

					recog.base.set_state(613);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(615);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule groupingLabelList*/
					recog.base.set_state(616);
					recog.groupingLabelList_rec(0)?;

					recog.base.set_state(617);
					recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

					recog.base.set_state(618);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(620);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(621);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- groupingLabelList ----------------
pub type GroupingLabelListContextAll<'input> = GroupingLabelListContext<'input>;


pub type GroupingLabelListContext<'input> = BaseParserRuleContext<'input,GroupingLabelListContextExt<'input>>;

#[derive(Clone)]
pub struct GroupingLabelListContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for GroupingLabelListContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for GroupingLabelListContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_groupingLabelList(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_groupingLabelList(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for GroupingLabelListContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_groupingLabelList(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupingLabelListContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_groupingLabelList }
	//fn type_rule_index() -> usize where Self: Sized { RULE_groupingLabelList }
}
antlr4rust::tid!{GroupingLabelListContextExt<'a>}

impl<'input> GroupingLabelListContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<GroupingLabelListContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,GroupingLabelListContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait GroupingLabelListContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<GroupingLabelListContextExt<'input>>{

fn groupingLabel(&self) -> Option<Rc<GroupingLabelContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn groupingLabelList(&self) -> Option<Rc<GroupingLabelListContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token COMMA
/// Returns `None` if there is no child corresponding to token COMMA
fn COMMA(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COMMA, 0)
}

}

impl<'input> GroupingLabelListContextAttrs<'input> for GroupingLabelListContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn  groupingLabelList(&mut self,)
	-> Result<Rc<GroupingLabelListContextAll<'input>>,ANTLRError> {
		self.groupingLabelList_rec(0)
	}

	fn groupingLabelList_rec(&mut self, _p: i32)
	-> Result<Rc<GroupingLabelListContextAll<'input>>,ANTLRError> {
		let recog = self;
		let _parentctx = recog.ctx.take();
		let _parentState = recog.base.get_state();
		let mut _localctx = GroupingLabelListContextExt::new(_parentctx.clone(), recog.base.get_state());
		recog.base.enter_recursion_rule(_localctx.clone(), 84, RULE_groupingLabelList, _p);
	    let mut _localctx: Rc<GroupingLabelListContextAll> = _localctx;
        let mut _prevctx = _localctx.clone();
		let _startState = 84;
		let result: Result<(), ANTLRError> = (|| {
			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			{
			/*InvokeRule groupingLabel*/
			recog.base.set_state(625);
			recog.groupingLabel()?;

			}
			let tmp = recog.input.lt(-1).cloned();
			recog.ctx.as_ref().unwrap().set_stop(tmp);
			recog.base.set_state(632);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(45,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					recog.trigger_exit_rule_event()?;
					_prevctx = _localctx.clone();
					{
					{
					/*recRuleAltStartAction*/
					let mut tmp = GroupingLabelListContextExt::new(_parentctx.clone(), _parentState);
					recog.push_new_recursion_context(tmp.clone(), _startState, RULE_groupingLabelList)?;
					_localctx = tmp;
					recog.base.set_state(627);
					if !({let _localctx = Some(_localctx.clone());
					recog.precpred(None, 2)}) {
						Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 2)".to_owned()), None))?;
					}
					recog.base.set_state(628);
					recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

					/*InvokeRule groupingLabel*/
					recog.base.set_state(629);
					recog.groupingLabel()?;

					}
					} 
				}
				recog.base.set_state(634);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(45,&mut recog.base)?;
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
//------------------- groupingLabel ----------------
pub type GroupingLabelContextAll<'input> = GroupingLabelContext<'input>;


pub type GroupingLabelContext<'input> = BaseParserRuleContext<'input,GroupingLabelContextExt<'input>>;

#[derive(Clone)]
pub struct GroupingLabelContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for GroupingLabelContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for GroupingLabelContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_groupingLabel(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_groupingLabel(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for GroupingLabelContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_groupingLabel(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupingLabelContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_groupingLabel }
	//fn type_rule_index() -> usize where Self: Sized { RULE_groupingLabel }
}
antlr4rust::tid!{GroupingLabelContextExt<'a>}

impl<'input> GroupingLabelContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<GroupingLabelContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,GroupingLabelContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait GroupingLabelContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<GroupingLabelContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
/// Returns `None` if there is no child corresponding to token ATTRIBUTE
fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_ATTRIBUTE, 0)
}
/// Retrieves first TerminalNode corresponding to token PREFIX
/// Returns `None` if there is no child corresponding to token PREFIX
fn PREFIX(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_PREFIX, 0)
}

}

impl<'input> GroupingLabelContextAttrs<'input> for GroupingLabelContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn groupingLabel(&mut self,)
	-> Result<Rc<GroupingLabelContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = GroupingLabelContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 86, RULE_groupingLabel);
        let mut _localctx: Rc<GroupingLabelContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(636);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==LogQLParser_PREFIX {
				{
				recog.base.set_state(635);
				recog.base.match_token(LogQLParser_PREFIX,&mut recog.err_handler)?;

				}
			}

			recog.base.set_state(638);
			recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

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
//------------------- groupingLabels ----------------
pub type GroupingLabelsContextAll<'input> = GroupingLabelsContext<'input>;


pub type GroupingLabelsContext<'input> = BaseParserRuleContext<'input,GroupingLabelsContextExt<'input>>;

#[derive(Clone)]
pub struct GroupingLabelsContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for GroupingLabelsContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for GroupingLabelsContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_groupingLabels(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_groupingLabels(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for GroupingLabelsContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_groupingLabels(self);
	}
}

impl<'input> CustomRuleContext<'input> for GroupingLabelsContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_groupingLabels }
	//fn type_rule_index() -> usize where Self: Sized { RULE_groupingLabels }
}
antlr4rust::tid!{GroupingLabelsContextExt<'a>}

impl<'input> GroupingLabelsContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<GroupingLabelsContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,GroupingLabelsContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait GroupingLabelsContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<GroupingLabelsContextExt<'input>>{

fn groupingLabel_all(&self) ->  Vec<Rc<GroupingLabelContextAll<'input>>> where Self:Sized{
	self.children_of_type()
}
fn groupingLabel(&self, i: usize) -> Option<Rc<GroupingLabelContextAll<'input>>> where Self:Sized{
	self.child_of_type(i)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COMMA, i)
}

}

impl<'input> GroupingLabelsContextAttrs<'input> for GroupingLabelsContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn groupingLabels(&mut self,)
	-> Result<Rc<GroupingLabelsContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = GroupingLabelsContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 88, RULE_groupingLabels);
        let mut _localctx: Rc<GroupingLabelsContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			/*InvokeRule groupingLabel*/
			recog.base.set_state(640);
			recog.groupingLabel()?;

			recog.base.set_state(645);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			while _la==LogQLParser_COMMA {
				{
				{
				recog.base.set_state(641);
				recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

				/*InvokeRule groupingLabel*/
				recog.base.set_state(642);
				recog.groupingLabel()?;

				}
				}
				recog.base.set_state(647);
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
//------------------- logRangeExpr ----------------
pub type LogRangeExprContextAll<'input> = LogRangeExprContext<'input>;


pub type LogRangeExprContext<'input> = BaseParserRuleContext<'input,LogRangeExprContextExt<'input>>;

#[derive(Clone)]
pub struct LogRangeExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LogRangeExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LogRangeExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_logRangeExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_logRangeExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LogRangeExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_logRangeExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for LogRangeExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_logRangeExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_logRangeExpr }
}
antlr4rust::tid!{LogRangeExprContextExt<'a>}

impl<'input> LogRangeExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LogRangeExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LogRangeExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LogRangeExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LogRangeExprContextExt<'input>>{

fn selector(&self) -> Option<Rc<SelectorContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn range(&self) -> Option<Rc<RangeContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn atModifier(&self) -> Option<Rc<AtModifierContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn offsetExpr(&self) -> Option<Rc<OffsetExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RPAREN, 0)
}
fn pipelineExpr(&self) -> Option<Rc<PipelineExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn logRangeExpr(&self) -> Option<Rc<LogRangeExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> LogRangeExprContextAttrs<'input> for LogRangeExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn logRangeExpr(&mut self,)
	-> Result<Rc<LogRangeExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LogRangeExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 90, RULE_logRangeExpr);
        let mut _localctx: Rc<LogRangeExprContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(721);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(58,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(648);
					recog.selector()?;

					/*InvokeRule range*/
					recog.base.set_state(649);
					recog.range()?;

					recog.base.set_state(651);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(650);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(653);
					recog.selector()?;

					/*InvokeRule range*/
					recog.base.set_state(654);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(655);
					recog.offsetExpr()?;

					recog.base.set_state(657);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(656);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(659);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule selector*/
					recog.base.set_state(660);
					recog.selector()?;

					recog.base.set_state(661);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule range*/
					recog.base.set_state(662);
					recog.range()?;

					recog.base.set_state(664);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(663);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				4 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					recog.base.set_state(666);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule selector*/
					recog.base.set_state(667);
					recog.selector()?;

					recog.base.set_state(668);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule range*/
					recog.base.set_state(669);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(670);
					recog.offsetExpr()?;

					recog.base.set_state(672);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(671);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				5 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 5)?;
					recog.base.enter_outer_alt(None, 5)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(674);
					recog.selector()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(675);
					recog.pipelineExpr_rec(0)?;

					/*InvokeRule range*/
					recog.base.set_state(676);
					recog.range()?;

					recog.base.set_state(678);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(677);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				6 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 6)?;
					recog.base.enter_outer_alt(None, 6)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(680);
					recog.selector()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(681);
					recog.pipelineExpr_rec(0)?;

					/*InvokeRule range*/
					recog.base.set_state(682);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(683);
					recog.offsetExpr()?;

					recog.base.set_state(685);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(684);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				7 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 7)?;
					recog.base.enter_outer_alt(None, 7)?;
					{
					recog.base.set_state(687);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule selector*/
					recog.base.set_state(688);
					recog.selector()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(689);
					recog.pipelineExpr_rec(0)?;

					recog.base.set_state(690);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule range*/
					recog.base.set_state(691);
					recog.range()?;

					recog.base.set_state(693);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(692);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				8 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 8)?;
					recog.base.enter_outer_alt(None, 8)?;
					{
					recog.base.set_state(695);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule selector*/
					recog.base.set_state(696);
					recog.selector()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(697);
					recog.pipelineExpr_rec(0)?;

					recog.base.set_state(698);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule range*/
					recog.base.set_state(699);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(700);
					recog.offsetExpr()?;

					recog.base.set_state(702);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(701);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				9 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 9)?;
					recog.base.enter_outer_alt(None, 9)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(704);
					recog.selector()?;

					/*InvokeRule range*/
					recog.base.set_state(705);
					recog.range()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(706);
					recog.pipelineExpr_rec(0)?;

					recog.base.set_state(708);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(707);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				10 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 10)?;
					recog.base.enter_outer_alt(None, 10)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(710);
					recog.selector()?;

					/*InvokeRule range*/
					recog.base.set_state(711);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(712);
					recog.offsetExpr()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(713);
					recog.pipelineExpr_rec(0)?;

					recog.base.set_state(715);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(714);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				11 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 11)?;
					recog.base.enter_outer_alt(None, 11)?;
					{
					recog.base.set_state(717);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule logRangeExpr*/
					recog.base.set_state(718);
					recog.logRangeExpr()?;

					recog.base.set_state(719);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- unwrappedRangeExpr ----------------
pub type UnwrappedRangeExprContextAll<'input> = UnwrappedRangeExprContext<'input>;


pub type UnwrappedRangeExprContext<'input> = BaseParserRuleContext<'input,UnwrappedRangeExprContextExt<'input>>;

#[derive(Clone)]
pub struct UnwrappedRangeExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for UnwrappedRangeExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for UnwrappedRangeExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_unwrappedRangeExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_unwrappedRangeExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for UnwrappedRangeExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_unwrappedRangeExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for UnwrappedRangeExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_unwrappedRangeExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_unwrappedRangeExpr }
}
antlr4rust::tid!{UnwrappedRangeExprContextExt<'a>}

impl<'input> UnwrappedRangeExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<UnwrappedRangeExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,UnwrappedRangeExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait UnwrappedRangeExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<UnwrappedRangeExprContextExt<'input>>{

fn selector(&self) -> Option<Rc<SelectorContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn range(&self) -> Option<Rc<RangeContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn unwrapExpr(&self) -> Option<Rc<UnwrapExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn atModifier(&self) -> Option<Rc<AtModifierContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
fn offsetExpr(&self) -> Option<Rc<OffsetExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RPAREN, 0)
}
fn pipelineExpr(&self) -> Option<Rc<PipelineExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> UnwrappedRangeExprContextAttrs<'input> for UnwrappedRangeExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn unwrappedRangeExpr(&mut self,)
	-> Result<Rc<UnwrappedRangeExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = UnwrappedRangeExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 92, RULE_unwrappedRangeExpr);
        let mut _localctx: Rc<UnwrappedRangeExprContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(805);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(70,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(723);
					recog.selector()?;

					/*InvokeRule range*/
					recog.base.set_state(724);
					recog.range()?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(725);
					recog.unwrapExpr_rec(0)?;

					recog.base.set_state(727);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(726);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(729);
					recog.selector()?;

					/*InvokeRule range*/
					recog.base.set_state(730);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(731);
					recog.offsetExpr()?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(732);
					recog.unwrapExpr_rec(0)?;

					recog.base.set_state(734);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(733);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				3 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 3)?;
					recog.base.enter_outer_alt(None, 3)?;
					{
					recog.base.set_state(736);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule selector*/
					recog.base.set_state(737);
					recog.selector()?;

					recog.base.set_state(738);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule range*/
					recog.base.set_state(739);
					recog.range()?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(740);
					recog.unwrapExpr_rec(0)?;

					recog.base.set_state(742);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(741);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				4 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 4)?;
					recog.base.enter_outer_alt(None, 4)?;
					{
					recog.base.set_state(744);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule selector*/
					recog.base.set_state(745);
					recog.selector()?;

					recog.base.set_state(746);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule range*/
					recog.base.set_state(747);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(748);
					recog.offsetExpr()?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(749);
					recog.unwrapExpr_rec(0)?;

					recog.base.set_state(751);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(750);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				5 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 5)?;
					recog.base.enter_outer_alt(None, 5)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(753);
					recog.selector()?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(754);
					recog.unwrapExpr_rec(0)?;

					/*InvokeRule range*/
					recog.base.set_state(755);
					recog.range()?;

					recog.base.set_state(757);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(756);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				6 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 6)?;
					recog.base.enter_outer_alt(None, 6)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(759);
					recog.selector()?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(760);
					recog.unwrapExpr_rec(0)?;

					/*InvokeRule range*/
					recog.base.set_state(761);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(762);
					recog.offsetExpr()?;

					recog.base.set_state(764);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(763);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				7 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 7)?;
					recog.base.enter_outer_alt(None, 7)?;
					{
					recog.base.set_state(766);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule selector*/
					recog.base.set_state(767);
					recog.selector()?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(768);
					recog.unwrapExpr_rec(0)?;

					recog.base.set_state(769);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule range*/
					recog.base.set_state(770);
					recog.range()?;

					recog.base.set_state(772);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(771);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				8 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 8)?;
					recog.base.enter_outer_alt(None, 8)?;
					{
					recog.base.set_state(774);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					/*InvokeRule selector*/
					recog.base.set_state(775);
					recog.selector()?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(776);
					recog.unwrapExpr_rec(0)?;

					recog.base.set_state(777);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					/*InvokeRule range*/
					recog.base.set_state(778);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(779);
					recog.offsetExpr()?;

					recog.base.set_state(781);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(780);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				9 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 9)?;
					recog.base.enter_outer_alt(None, 9)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(783);
					recog.selector()?;

					/*InvokeRule range*/
					recog.base.set_state(784);
					recog.range()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(785);
					recog.pipelineExpr_rec(0)?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(786);
					recog.unwrapExpr_rec(0)?;

					recog.base.set_state(788);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(787);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				10 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 10)?;
					recog.base.enter_outer_alt(None, 10)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(790);
					recog.selector()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(791);
					recog.pipelineExpr_rec(0)?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(792);
					recog.unwrapExpr_rec(0)?;

					/*InvokeRule range*/
					recog.base.set_state(793);
					recog.range()?;

					recog.base.set_state(795);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(794);
						recog.atModifier()?;

						}
					}

					}
				}
			,
				11 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 11)?;
					recog.base.enter_outer_alt(None, 11)?;
					{
					/*InvokeRule selector*/
					recog.base.set_state(797);
					recog.selector()?;

					/*InvokeRule range*/
					recog.base.set_state(798);
					recog.range()?;

					/*InvokeRule offsetExpr*/
					recog.base.set_state(799);
					recog.offsetExpr()?;

					/*InvokeRule pipelineExpr*/
					recog.base.set_state(800);
					recog.pipelineExpr_rec(0)?;

					/*InvokeRule unwrapExpr*/
					recog.base.set_state(801);
					recog.unwrapExpr_rec(0)?;

					recog.base.set_state(803);
					recog.err_handler.sync(&mut recog.base)?;
					_la = recog.base.input.la(1);
					if _la==LogQLParser_AT {
						{
						/*InvokeRule atModifier*/
						recog.base.set_state(802);
						recog.atModifier()?;

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
//------------------- range ----------------
pub type RangeContextAll<'input> = RangeContext<'input>;


pub type RangeContext<'input> = BaseParserRuleContext<'input,RangeContextExt<'input>>;

#[derive(Clone)]
pub struct RangeContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for RangeContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for RangeContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_range(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_range(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for RangeContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_range(self);
	}
}

impl<'input> CustomRuleContext<'input> for RangeContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_range }
	//fn type_rule_index() -> usize where Self: Sized { RULE_range }
}
antlr4rust::tid!{RangeContextExt<'a>}

impl<'input> RangeContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<RangeContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,RangeContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait RangeContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<RangeContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LBRACK
/// Returns `None` if there is no child corresponding to token LBRACK
fn LBRACK(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LBRACK, 0)
}
fn duration(&self) -> Option<Rc<DurationContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves first TerminalNode corresponding to token RBRACK
/// Returns `None` if there is no child corresponding to token RBRACK
fn RBRACK(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RBRACK, 0)
}

}

impl<'input> RangeContextAttrs<'input> for RangeContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn range(&mut self,)
	-> Result<Rc<RangeContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = RangeContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 94, RULE_range);
        let mut _localctx: Rc<RangeContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(807);
			recog.base.match_token(LogQLParser_LBRACK,&mut recog.err_handler)?;

			/*InvokeRule duration*/
			recog.base.set_state(808);
			recog.duration()?;

			recog.base.set_state(809);
			recog.base.match_token(LogQLParser_RBRACK,&mut recog.err_handler)?;

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
//------------------- offsetExpr ----------------
pub type OffsetExprContextAll<'input> = OffsetExprContext<'input>;


pub type OffsetExprContext<'input> = BaseParserRuleContext<'input,OffsetExprContextExt<'input>>;

#[derive(Clone)]
pub struct OffsetExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for OffsetExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for OffsetExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_offsetExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_offsetExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for OffsetExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_offsetExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for OffsetExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_offsetExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_offsetExpr }
}
antlr4rust::tid!{OffsetExprContextExt<'a>}

impl<'input> OffsetExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<OffsetExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,OffsetExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait OffsetExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<OffsetExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token OFFSET
/// Returns `None` if there is no child corresponding to token OFFSET
fn OFFSET(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_OFFSET, 0)
}
fn duration(&self) -> Option<Rc<DurationContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}

}

impl<'input> OffsetExprContextAttrs<'input> for OffsetExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn offsetExpr(&mut self,)
	-> Result<Rc<OffsetExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = OffsetExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 96, RULE_offsetExpr);
        let mut _localctx: Rc<OffsetExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(811);
			recog.base.match_token(LogQLParser_OFFSET,&mut recog.err_handler)?;

			/*InvokeRule duration*/
			recog.base.set_state(812);
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

impl<'input> LogQLParserContext<'input> for AtModifierContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for AtModifierContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_atModifier(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_atModifier(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for AtModifierContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_atModifier(self);
	}
}

impl<'input> CustomRuleContext<'input> for AtModifierContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_atModifier }
	//fn type_rule_index() -> usize where Self: Sized { RULE_atModifier }
}
antlr4rust::tid!{AtModifierContextExt<'a>}

impl<'input> AtModifierContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<AtModifierContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,AtModifierContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait AtModifierContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<AtModifierContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token AT
/// Returns `None` if there is no child corresponding to token AT
fn AT(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_AT, 0)
}
/// Retrieves first TerminalNode corresponding to token NUMBER
/// Returns `None` if there is no child corresponding to token NUMBER
fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_NUMBER, 0)
}
/// Retrieves first TerminalNode corresponding to token SUB
/// Returns `None` if there is no child corresponding to token SUB
fn SUB(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_SUB, 0)
}

}

impl<'input> AtModifierContextAttrs<'input> for AtModifierContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn atModifier(&mut self,)
	-> Result<Rc<AtModifierContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = AtModifierContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 98, RULE_atModifier);
        let mut _localctx: Rc<AtModifierContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(819);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(71,&mut recog.base)? {
				1 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
					recog.base.enter_outer_alt(None, 1)?;
					{
					recog.base.set_state(814);
					recog.base.match_token(LogQLParser_AT,&mut recog.err_handler)?;

					recog.base.set_state(815);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					//recog.base.enter_outer_alt(_localctx.clone(), 2)?;
					recog.base.enter_outer_alt(None, 2)?;
					{
					recog.base.set_state(816);
					recog.base.match_token(LogQLParser_AT,&mut recog.err_handler)?;

					recog.base.set_state(817);
					recog.base.match_token(LogQLParser_SUB,&mut recog.err_handler)?;

					recog.base.set_state(818);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

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
//------------------- unwrapExpr ----------------
#[derive(Debug)]
pub enum UnwrapExprContextAll<'input>{
	UnwrapWithFilterContext(UnwrapWithFilterContext<'input>),
	UnwrapWithConversionContext(UnwrapWithConversionContext<'input>),
	UnwrapBasicContext(UnwrapBasicContext<'input>),
Error(UnwrapExprContext<'input>)
}
antlr4rust::tid!{UnwrapExprContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for UnwrapExprContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for UnwrapExprContextAll<'input>{}

impl<'input> Deref for UnwrapExprContextAll<'input>{
	type Target = dyn UnwrapExprContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use UnwrapExprContextAll::*;
		match self{
			UnwrapWithFilterContext(inner) => inner,
			UnwrapWithConversionContext(inner) => inner,
			UnwrapBasicContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for UnwrapExprContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for UnwrapExprContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type UnwrapExprContext<'input> = BaseParserRuleContext<'input,UnwrapExprContextExt<'input>>;

#[derive(Clone)]
pub struct UnwrapExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for UnwrapExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for UnwrapExprContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for UnwrapExprContext<'input>{
}

impl<'input> CustomRuleContext<'input> for UnwrapExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_unwrapExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_unwrapExpr }
}
antlr4rust::tid!{UnwrapExprContextExt<'a>}

impl<'input> UnwrapExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<UnwrapExprContextAll<'input>> {
		Rc::new(
		UnwrapExprContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,UnwrapExprContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait UnwrapExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<UnwrapExprContextExt<'input>>{


}

impl<'input> UnwrapExprContextAttrs<'input> for UnwrapExprContext<'input>{}

pub type UnwrapWithFilterContext<'input> = BaseParserRuleContext<'input,UnwrapWithFilterContextExt<'input>>;

pub trait UnwrapWithFilterContextAttrs<'input>: LogQLParserContext<'input>{
	fn unwrapExpr(&self) -> Option<Rc<UnwrapExprContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
	/// Retrieves first TerminalNode corresponding to token PIPE
	/// Returns `None` if there is no child corresponding to token PIPE
	fn PIPE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PIPE, 0)
	}
	fn labelFilter(&self) -> Option<Rc<LabelFilterContextAll<'input>>> where Self:Sized{
		self.child_of_type(0)
	}
}

impl<'input> UnwrapWithFilterContextAttrs<'input> for UnwrapWithFilterContext<'input>{}

pub struct UnwrapWithFilterContextExt<'input>{
	base:UnwrapExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{UnwrapWithFilterContextExt<'a>}

impl<'input> LogQLParserContext<'input> for UnwrapWithFilterContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for UnwrapWithFilterContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_unwrapWithFilter(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_unwrapWithFilter(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for UnwrapWithFilterContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_unwrapWithFilter(self);
	}
}

impl<'input> CustomRuleContext<'input> for UnwrapWithFilterContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_unwrapExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_unwrapExpr }
}

impl<'input> Borrow<UnwrapExprContextExt<'input>> for UnwrapWithFilterContext<'input>{
	fn borrow(&self) -> &UnwrapExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<UnwrapExprContextExt<'input>> for UnwrapWithFilterContext<'input>{
	fn borrow_mut(&mut self) -> &mut UnwrapExprContextExt<'input> { &mut self.base }
}

impl<'input> UnwrapExprContextAttrs<'input> for UnwrapWithFilterContext<'input> {}

impl<'input> UnwrapWithFilterContextExt<'input>{
	fn new(ctx: &dyn UnwrapExprContextAttrs<'input>) -> Rc<UnwrapExprContextAll<'input>>  {
		Rc::new(
			UnwrapExprContextAll::UnwrapWithFilterContext(
				BaseParserRuleContext::copy_from(ctx,UnwrapWithFilterContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type UnwrapWithConversionContext<'input> = BaseParserRuleContext<'input,UnwrapWithConversionContextExt<'input>>;

pub trait UnwrapWithConversionContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token PIPE
	/// Returns `None` if there is no child corresponding to token PIPE
	fn PIPE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PIPE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token UNWRAP
	/// Returns `None` if there is no child corresponding to token UNWRAP
	fn UNWRAP(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_UNWRAP, 0)
	}
	/// Retrieves all `TerminalNode`s corresponding to token ATTRIBUTE in current rule
	fn ATTRIBUTE_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
		self.children_of_type()
	}
	/// Retrieves 'i's TerminalNode corresponding to token ATTRIBUTE, starting from 0.
	/// Returns `None` if number of children corresponding to token ATTRIBUTE is less or equal than `i`.
	fn ATTRIBUTE(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, i)
	}
	/// Retrieves first TerminalNode corresponding to token LPAREN
	/// Returns `None` if there is no child corresponding to token LPAREN
	fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_LPAREN, 0)
	}
	/// Retrieves first TerminalNode corresponding to token RPAREN
	/// Returns `None` if there is no child corresponding to token RPAREN
	fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_RPAREN, 0)
	}
}

impl<'input> UnwrapWithConversionContextAttrs<'input> for UnwrapWithConversionContext<'input>{}

pub struct UnwrapWithConversionContextExt<'input>{
	base:UnwrapExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{UnwrapWithConversionContextExt<'a>}

impl<'input> LogQLParserContext<'input> for UnwrapWithConversionContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for UnwrapWithConversionContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_unwrapWithConversion(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_unwrapWithConversion(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for UnwrapWithConversionContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_unwrapWithConversion(self);
	}
}

impl<'input> CustomRuleContext<'input> for UnwrapWithConversionContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_unwrapExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_unwrapExpr }
}

impl<'input> Borrow<UnwrapExprContextExt<'input>> for UnwrapWithConversionContext<'input>{
	fn borrow(&self) -> &UnwrapExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<UnwrapExprContextExt<'input>> for UnwrapWithConversionContext<'input>{
	fn borrow_mut(&mut self) -> &mut UnwrapExprContextExt<'input> { &mut self.base }
}

impl<'input> UnwrapExprContextAttrs<'input> for UnwrapWithConversionContext<'input> {}

impl<'input> UnwrapWithConversionContextExt<'input>{
	fn new(ctx: &dyn UnwrapExprContextAttrs<'input>) -> Rc<UnwrapExprContextAll<'input>>  {
		Rc::new(
			UnwrapExprContextAll::UnwrapWithConversionContext(
				BaseParserRuleContext::copy_from(ctx,UnwrapWithConversionContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type UnwrapBasicContext<'input> = BaseParserRuleContext<'input,UnwrapBasicContextExt<'input>>;

pub trait UnwrapBasicContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token PIPE
	/// Returns `None` if there is no child corresponding to token PIPE
	fn PIPE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_PIPE, 0)
	}
	/// Retrieves first TerminalNode corresponding to token UNWRAP
	/// Returns `None` if there is no child corresponding to token UNWRAP
	fn UNWRAP(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_UNWRAP, 0)
	}
	/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
	/// Returns `None` if there is no child corresponding to token ATTRIBUTE
	fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ATTRIBUTE, 0)
	}
}

impl<'input> UnwrapBasicContextAttrs<'input> for UnwrapBasicContext<'input>{}

pub struct UnwrapBasicContextExt<'input>{
	base:UnwrapExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{UnwrapBasicContextExt<'a>}

impl<'input> LogQLParserContext<'input> for UnwrapBasicContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for UnwrapBasicContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_unwrapBasic(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_unwrapBasic(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for UnwrapBasicContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_unwrapBasic(self);
	}
}

impl<'input> CustomRuleContext<'input> for UnwrapBasicContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_unwrapExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_unwrapExpr }
}

impl<'input> Borrow<UnwrapExprContextExt<'input>> for UnwrapBasicContext<'input>{
	fn borrow(&self) -> &UnwrapExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<UnwrapExprContextExt<'input>> for UnwrapBasicContext<'input>{
	fn borrow_mut(&mut self) -> &mut UnwrapExprContextExt<'input> { &mut self.base }
}

impl<'input> UnwrapExprContextAttrs<'input> for UnwrapBasicContext<'input> {}

impl<'input> UnwrapBasicContextExt<'input>{
	fn new(ctx: &dyn UnwrapExprContextAttrs<'input>) -> Rc<UnwrapExprContextAll<'input>>  {
		Rc::new(
			UnwrapExprContextAll::UnwrapBasicContext(
				BaseParserRuleContext::copy_from(ctx,UnwrapBasicContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn  unwrapExpr(&mut self,)
	-> Result<Rc<UnwrapExprContextAll<'input>>,ANTLRError> {
		self.unwrapExpr_rec(0)
	}

	fn unwrapExpr_rec(&mut self, _p: i32)
	-> Result<Rc<UnwrapExprContextAll<'input>>,ANTLRError> {
		let recog = self;
		let _parentctx = recog.ctx.take();
		let _parentState = recog.base.get_state();
		let mut _localctx = UnwrapExprContextExt::new(_parentctx.clone(), recog.base.get_state());
		recog.base.enter_recursion_rule(_localctx.clone(), 100, RULE_unwrapExpr, _p);
	    let mut _localctx: Rc<UnwrapExprContextAll> = _localctx;
        let mut _prevctx = _localctx.clone();
		let _startState = 100;
		let result: Result<(), ANTLRError> = (|| {
			let mut _alt: i32;
			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(831);
			recog.err_handler.sync(&mut recog.base)?;
			match  recog.interpreter.adaptive_predict(72,&mut recog.base)? {
				1 =>{
					{
					let mut tmp = UnwrapBasicContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();

					recog.base.set_state(822);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					recog.base.set_state(823);
					recog.base.match_token(LogQLParser_UNWRAP,&mut recog.err_handler)?;

					recog.base.set_state(824);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					}
				}
			,
				2 =>{
					{
					let mut tmp = UnwrapWithConversionContextExt::new(&**_localctx);
					recog.ctx = Some(tmp.clone());
					_localctx = tmp;
					_prevctx = _localctx.clone();
					recog.base.set_state(825);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					recog.base.set_state(826);
					recog.base.match_token(LogQLParser_UNWRAP,&mut recog.err_handler)?;

					recog.base.set_state(827);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(828);
					recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

					recog.base.set_state(829);
					recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

					recog.base.set_state(830);
					recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

					}
				}

				_ => {}
			}
			let tmp = recog.input.lt(-1).cloned();
			recog.ctx.as_ref().unwrap().set_stop(tmp);
			recog.base.set_state(838);
			recog.err_handler.sync(&mut recog.base)?;
			_alt = recog.interpreter.adaptive_predict(73,&mut recog.base)?;
			while { _alt!=2 && _alt!=INVALID_ALT } {
				if _alt==1 {
					recog.trigger_exit_rule_event()?;
					_prevctx = _localctx.clone();
					{
					{
					/*recRuleLabeledAltStartAction*/
					let mut tmp = UnwrapWithFilterContextExt::new(&**UnwrapExprContextExt::new(_parentctx.clone(), _parentState));
					recog.push_new_recursion_context(tmp.clone(), _startState, RULE_unwrapExpr)?;
					_localctx = tmp;
					recog.base.set_state(833);
					if !({let _localctx = Some(_localctx.clone());
					recog.precpred(None, 1)}) {
						Err(FailedPredicateError::new(&mut recog.base, Some("recog.precpred(None, 1)".to_owned()), None))?;
					}
					recog.base.set_state(834);
					recog.base.match_token(LogQLParser_PIPE,&mut recog.err_handler)?;

					/*InvokeRule labelFilter*/
					recog.base.set_state(835);
					recog.labelFilter_rec(0)?;

					}
					} 
				}
				recog.base.set_state(840);
				recog.err_handler.sync(&mut recog.base)?;
				_alt = recog.interpreter.adaptive_predict(73,&mut recog.base)?;
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
//------------------- literalExpr ----------------
#[derive(Debug)]
pub enum LiteralExprContextAll<'input>{
	LiteralNumberContext(LiteralNumberContext<'input>),
	LiteralPositiveNumberContext(LiteralPositiveNumberContext<'input>),
	LiteralNegativeNumberContext(LiteralNegativeNumberContext<'input>),
Error(LiteralExprContext<'input>)
}
antlr4rust::tid!{LiteralExprContextAll<'a>}

impl<'input> antlr4rust::parser_rule_context::DerefSeal for LiteralExprContextAll<'input>{}

impl<'input> LogQLParserContext<'input> for LiteralExprContextAll<'input>{}

impl<'input> Deref for LiteralExprContextAll<'input>{
	type Target = dyn LiteralExprContextAttrs<'input> + 'input;
	fn deref(&self) -> &Self::Target{
		use LiteralExprContextAll::*;
		match self{
			LiteralNumberContext(inner) => inner,
			LiteralPositiveNumberContext(inner) => inner,
			LiteralNegativeNumberContext(inner) => inner,
Error(inner) => inner
		}
	}
}
impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LiteralExprContextAll<'input>{
	fn accept(&self, visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) { self.deref().accept(visitor) }
}
impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LiteralExprContextAll<'input>{
    fn enter(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().enter(listener) }
    fn exit(&self, listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> { self.deref().exit(listener) }
}



pub type LiteralExprContext<'input> = BaseParserRuleContext<'input,LiteralExprContextExt<'input>>;

#[derive(Clone)]
pub struct LiteralExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LiteralExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LiteralExprContext<'input>{
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LiteralExprContext<'input>{
}

impl<'input> CustomRuleContext<'input> for LiteralExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_literalExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_literalExpr }
}
antlr4rust::tid!{LiteralExprContextExt<'a>}

impl<'input> LiteralExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LiteralExprContextAll<'input>> {
		Rc::new(
		LiteralExprContextAll::Error(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LiteralExprContextExt{

				ph:PhantomData
			}),
		)
		)
	}
}

pub trait LiteralExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LiteralExprContextExt<'input>>{


}

impl<'input> LiteralExprContextAttrs<'input> for LiteralExprContext<'input>{}

pub type LiteralNumberContext<'input> = BaseParserRuleContext<'input,LiteralNumberContextExt<'input>>;

pub trait LiteralNumberContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token NUMBER
	/// Returns `None` if there is no child corresponding to token NUMBER
	fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_NUMBER, 0)
	}
}

impl<'input> LiteralNumberContextAttrs<'input> for LiteralNumberContext<'input>{}

pub struct LiteralNumberContextExt<'input>{
	base:LiteralExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LiteralNumberContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LiteralNumberContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LiteralNumberContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_literalNumber(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_literalNumber(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LiteralNumberContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_literalNumber(self);
	}
}

impl<'input> CustomRuleContext<'input> for LiteralNumberContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_literalExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_literalExpr }
}

impl<'input> Borrow<LiteralExprContextExt<'input>> for LiteralNumberContext<'input>{
	fn borrow(&self) -> &LiteralExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LiteralExprContextExt<'input>> for LiteralNumberContext<'input>{
	fn borrow_mut(&mut self) -> &mut LiteralExprContextExt<'input> { &mut self.base }
}

impl<'input> LiteralExprContextAttrs<'input> for LiteralNumberContext<'input> {}

impl<'input> LiteralNumberContextExt<'input>{
	fn new(ctx: &dyn LiteralExprContextAttrs<'input>) -> Rc<LiteralExprContextAll<'input>>  {
		Rc::new(
			LiteralExprContextAll::LiteralNumberContext(
				BaseParserRuleContext::copy_from(ctx,LiteralNumberContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LiteralPositiveNumberContext<'input> = BaseParserRuleContext<'input,LiteralPositiveNumberContextExt<'input>>;

pub trait LiteralPositiveNumberContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token ADD
	/// Returns `None` if there is no child corresponding to token ADD
	fn ADD(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_ADD, 0)
	}
	/// Retrieves first TerminalNode corresponding to token NUMBER
	/// Returns `None` if there is no child corresponding to token NUMBER
	fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_NUMBER, 0)
	}
}

impl<'input> LiteralPositiveNumberContextAttrs<'input> for LiteralPositiveNumberContext<'input>{}

pub struct LiteralPositiveNumberContextExt<'input>{
	base:LiteralExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LiteralPositiveNumberContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LiteralPositiveNumberContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LiteralPositiveNumberContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_literalPositiveNumber(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_literalPositiveNumber(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LiteralPositiveNumberContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_literalPositiveNumber(self);
	}
}

impl<'input> CustomRuleContext<'input> for LiteralPositiveNumberContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_literalExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_literalExpr }
}

impl<'input> Borrow<LiteralExprContextExt<'input>> for LiteralPositiveNumberContext<'input>{
	fn borrow(&self) -> &LiteralExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LiteralExprContextExt<'input>> for LiteralPositiveNumberContext<'input>{
	fn borrow_mut(&mut self) -> &mut LiteralExprContextExt<'input> { &mut self.base }
}

impl<'input> LiteralExprContextAttrs<'input> for LiteralPositiveNumberContext<'input> {}

impl<'input> LiteralPositiveNumberContextExt<'input>{
	fn new(ctx: &dyn LiteralExprContextAttrs<'input>) -> Rc<LiteralExprContextAll<'input>>  {
		Rc::new(
			LiteralExprContextAll::LiteralPositiveNumberContext(
				BaseParserRuleContext::copy_from(ctx,LiteralPositiveNumberContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

pub type LiteralNegativeNumberContext<'input> = BaseParserRuleContext<'input,LiteralNegativeNumberContextExt<'input>>;

pub trait LiteralNegativeNumberContextAttrs<'input>: LogQLParserContext<'input>{
	/// Retrieves first TerminalNode corresponding to token SUB
	/// Returns `None` if there is no child corresponding to token SUB
	fn SUB(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_SUB, 0)
	}
	/// Retrieves first TerminalNode corresponding to token NUMBER
	/// Returns `None` if there is no child corresponding to token NUMBER
	fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
		self.get_token(LogQLParser_NUMBER, 0)
	}
}

impl<'input> LiteralNegativeNumberContextAttrs<'input> for LiteralNegativeNumberContext<'input>{}

pub struct LiteralNegativeNumberContextExt<'input>{
	base:LiteralExprContextExt<'input>,
	ph:PhantomData<&'input str>
}

antlr4rust::tid!{LiteralNegativeNumberContextExt<'a>}

impl<'input> LogQLParserContext<'input> for LiteralNegativeNumberContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LiteralNegativeNumberContext<'input>{
	fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.enter_every_rule(self)?;
		listener.enter_literalNegativeNumber(self);
		Ok(())
	}
	fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
		listener.exit_literalNegativeNumber(self);
		listener.exit_every_rule(self)?;
		Ok(())
	}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LiteralNegativeNumberContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_literalNegativeNumber(self);
	}
}

impl<'input> CustomRuleContext<'input> for LiteralNegativeNumberContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_literalExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_literalExpr }
}

impl<'input> Borrow<LiteralExprContextExt<'input>> for LiteralNegativeNumberContext<'input>{
	fn borrow(&self) -> &LiteralExprContextExt<'input> { &self.base }
}
impl<'input> BorrowMut<LiteralExprContextExt<'input>> for LiteralNegativeNumberContext<'input>{
	fn borrow_mut(&mut self) -> &mut LiteralExprContextExt<'input> { &mut self.base }
}

impl<'input> LiteralExprContextAttrs<'input> for LiteralNegativeNumberContext<'input> {}

impl<'input> LiteralNegativeNumberContextExt<'input>{
	fn new(ctx: &dyn LiteralExprContextAttrs<'input>) -> Rc<LiteralExprContextAll<'input>>  {
		Rc::new(
			LiteralExprContextAll::LiteralNegativeNumberContext(
				BaseParserRuleContext::copy_from(ctx,LiteralNegativeNumberContextExt{
        			base: ctx.borrow().clone(),
        			ph:PhantomData
				})
			)
		)
	}
}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn literalExpr(&mut self,)
	-> Result<Rc<LiteralExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LiteralExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 102, RULE_literalExpr);
        let mut _localctx: Rc<LiteralExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			recog.base.set_state(846);
			recog.err_handler.sync(&mut recog.base)?;
			match recog.base.input.la(1) {
			LogQLParser_NUMBER 
				=> {
					let tmp = LiteralNumberContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 1)?;
					_localctx = tmp;
					{
					recog.base.set_state(841);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

					}
				}

			LogQLParser_ADD 
				=> {
					let tmp = LiteralPositiveNumberContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 2)?;
					_localctx = tmp;
					{
					recog.base.set_state(842);
					recog.base.match_token(LogQLParser_ADD,&mut recog.err_handler)?;

					recog.base.set_state(843);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

					}
				}

			LogQLParser_SUB 
				=> {
					let tmp = LiteralNegativeNumberContextExt::new(&**_localctx);
					recog.base.enter_outer_alt(Some(tmp.clone()), 3)?;
					_localctx = tmp;
					{
					recog.base.set_state(844);
					recog.base.match_token(LogQLParser_SUB,&mut recog.err_handler)?;

					recog.base.set_state(845);
					recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

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
//------------------- labelReplaceExpr ----------------
pub type LabelReplaceExprContextAll<'input> = LabelReplaceExprContext<'input>;


pub type LabelReplaceExprContext<'input> = BaseParserRuleContext<'input,LabelReplaceExprContextExt<'input>>;

#[derive(Clone)]
pub struct LabelReplaceExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for LabelReplaceExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for LabelReplaceExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_labelReplaceExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_labelReplaceExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for LabelReplaceExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_labelReplaceExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for LabelReplaceExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_labelReplaceExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_labelReplaceExpr }
}
antlr4rust::tid!{LabelReplaceExprContextExt<'a>}

impl<'input> LabelReplaceExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<LabelReplaceExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,LabelReplaceExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait LabelReplaceExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<LabelReplaceExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token LABEL_REPLACE
/// Returns `None` if there is no child corresponding to token LABEL_REPLACE
fn LABEL_REPLACE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LABEL_REPLACE, 0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LPAREN, 0)
}
fn metricExpr(&self) -> Option<Rc<MetricExprContextAll<'input>>> where Self:Sized{
	self.child_of_type(0)
}
/// Retrieves all `TerminalNode`s corresponding to token COMMA in current rule
fn COMMA_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token COMMA, starting from 0.
/// Returns `None` if number of children corresponding to token COMMA is less or equal than `i`.
fn COMMA(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_COMMA, i)
}
/// Retrieves all `TerminalNode`s corresponding to token STRING in current rule
fn STRING_all(&self) -> Vec<Rc<TerminalNode<'input,LogQLParserContextType>>>  where Self:Sized{
	self.children_of_type()
}
/// Retrieves 'i's TerminalNode corresponding to token STRING, starting from 0.
/// Returns `None` if number of children corresponding to token STRING is less or equal than `i`.
fn STRING(&self, i: usize) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_STRING, i)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RPAREN, 0)
}

}

impl<'input> LabelReplaceExprContextAttrs<'input> for LabelReplaceExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn labelReplaceExpr(&mut self,)
	-> Result<Rc<LabelReplaceExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = LabelReplaceExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 104, RULE_labelReplaceExpr);
        let mut _localctx: Rc<LabelReplaceExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(848);
			recog.base.match_token(LogQLParser_LABEL_REPLACE,&mut recog.err_handler)?;

			recog.base.set_state(849);
			recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

			/*InvokeRule metricExpr*/
			recog.base.set_state(850);
			recog.metricExpr_rec(0)?;

			recog.base.set_state(851);
			recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

			recog.base.set_state(852);
			recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

			recog.base.set_state(853);
			recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

			recog.base.set_state(854);
			recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

			recog.base.set_state(855);
			recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

			recog.base.set_state(856);
			recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

			recog.base.set_state(857);
			recog.base.match_token(LogQLParser_COMMA,&mut recog.err_handler)?;

			recog.base.set_state(858);
			recog.base.match_token(LogQLParser_STRING,&mut recog.err_handler)?;

			recog.base.set_state(859);
			recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- vectorExpr ----------------
pub type VectorExprContextAll<'input> = VectorExprContext<'input>;


pub type VectorExprContext<'input> = BaseParserRuleContext<'input,VectorExprContextExt<'input>>;

#[derive(Clone)]
pub struct VectorExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for VectorExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for VectorExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_vectorExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_vectorExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for VectorExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_vectorExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for VectorExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_vectorExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_vectorExpr }
}
antlr4rust::tid!{VectorExprContextExt<'a>}

impl<'input> VectorExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<VectorExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,VectorExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait VectorExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<VectorExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token VECTOR
/// Returns `None` if there is no child corresponding to token VECTOR
fn VECTOR(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_VECTOR, 0)
}
/// Retrieves first TerminalNode corresponding to token LPAREN
/// Returns `None` if there is no child corresponding to token LPAREN
fn LPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_LPAREN, 0)
}
/// Retrieves first TerminalNode corresponding to token NUMBER
/// Returns `None` if there is no child corresponding to token NUMBER
fn NUMBER(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_NUMBER, 0)
}
/// Retrieves first TerminalNode corresponding to token RPAREN
/// Returns `None` if there is no child corresponding to token RPAREN
fn RPAREN(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_RPAREN, 0)
}

}

impl<'input> VectorExprContextAttrs<'input> for VectorExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn vectorExpr(&mut self,)
	-> Result<Rc<VectorExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = VectorExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 106, RULE_vectorExpr);
        let mut _localctx: Rc<VectorExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(861);
			recog.base.match_token(LogQLParser_VECTOR,&mut recog.err_handler)?;

			recog.base.set_state(862);
			recog.base.match_token(LogQLParser_LPAREN,&mut recog.err_handler)?;

			recog.base.set_state(863);
			recog.base.match_token(LogQLParser_NUMBER,&mut recog.err_handler)?;

			recog.base.set_state(864);
			recog.base.match_token(LogQLParser_RPAREN,&mut recog.err_handler)?;

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
//------------------- variableExpr ----------------
pub type VariableExprContextAll<'input> = VariableExprContext<'input>;


pub type VariableExprContext<'input> = BaseParserRuleContext<'input,VariableExprContextExt<'input>>;

#[derive(Clone)]
pub struct VariableExprContextExt<'input>{
ph:PhantomData<&'input str>
}

impl<'input> LogQLParserContext<'input> for VariableExprContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for VariableExprContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_variableExpr(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_variableExpr(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for VariableExprContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_variableExpr(self);
	}
}

impl<'input> CustomRuleContext<'input> for VariableExprContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_variableExpr }
	//fn type_rule_index() -> usize where Self: Sized { RULE_variableExpr }
}
antlr4rust::tid!{VariableExprContextExt<'a>}

impl<'input> VariableExprContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<VariableExprContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,VariableExprContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait VariableExprContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<VariableExprContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token ATTRIBUTE
/// Returns `None` if there is no child corresponding to token ATTRIBUTE
fn ATTRIBUTE(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_ATTRIBUTE, 0)
}

}

impl<'input> VariableExprContextAttrs<'input> for VariableExprContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn variableExpr(&mut self,)
	-> Result<Rc<VariableExprContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = VariableExprContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 108, RULE_variableExpr);
        let mut _localctx: Rc<VariableExprContextAll> = _localctx;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(866);
			recog.base.match_token(LogQLParser_ATTRIBUTE,&mut recog.err_handler)?;

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

impl<'input> LogQLParserContext<'input> for DurationContext<'input>{}

impl<'input,'a> Listenable<dyn LogQLParserListener<'input> + 'a> for DurationContext<'input>{
		fn enter(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.enter_every_rule(self)?;
			listener.enter_duration(self);
			Ok(())
		}
		fn exit(&self,listener: &mut (dyn LogQLParserListener<'input> + 'a)) -> Result<(), ANTLRError> {
			listener.exit_duration(self);
			listener.exit_every_rule(self)?;
			Ok(())
		}
}

impl<'input,'a> Visitable<dyn LogQLParserVisitor<'input> + 'a> for DurationContext<'input>{
	fn accept(&self,visitor: &mut (dyn LogQLParserVisitor<'input> + 'a)) {
		visitor.visit_duration(self);
	}
}

impl<'input> CustomRuleContext<'input> for DurationContextExt<'input>{
	type TF = LocalTokenFactory<'input>;
	type Ctx = LogQLParserContextType;
	fn get_rule_index(&self) -> usize { RULE_duration }
	//fn type_rule_index() -> usize where Self: Sized { RULE_duration }
}
antlr4rust::tid!{DurationContextExt<'a>}

impl<'input> DurationContextExt<'input>{
	fn new(parent: Option<Rc<dyn LogQLParserContext<'input> + 'input > >, invoking_state: i32) -> Rc<DurationContextAll<'input>> {
		Rc::new(
			BaseParserRuleContext::new_parser_ctx(parent, invoking_state,DurationContextExt{

				ph:PhantomData
			}),
		)
	}
}

pub trait DurationContextAttrs<'input>: LogQLParserContext<'input> + BorrowMut<DurationContextExt<'input>>{

/// Retrieves first TerminalNode corresponding to token DURATION
/// Returns `None` if there is no child corresponding to token DURATION
fn DURATION(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_DURATION, 0)
}
/// Retrieves first TerminalNode corresponding to token SUB
/// Returns `None` if there is no child corresponding to token SUB
fn SUB(&self) -> Option<Rc<TerminalNode<'input,LogQLParserContextType>>> where Self:Sized{
	self.get_token(LogQLParser_SUB, 0)
}

}

impl<'input> DurationContextAttrs<'input> for DurationContext<'input>{}

impl<'input, I> LogQLParser<'input, I>
where
    I: TokenStream<'input, TF = LocalTokenFactory<'input> > + TidAble<'input>,
{
	pub fn duration(&mut self,)
	-> Result<Rc<DurationContextAll<'input>>,ANTLRError> {
		let mut recog = self;
		let _parentctx = recog.ctx.take();
		let mut _localctx = DurationContextExt::new(_parentctx.clone(), recog.base.get_state());
        recog.base.enter_rule(_localctx.clone(), 110, RULE_duration);
        let mut _localctx: Rc<DurationContextAll> = _localctx;
		let mut _la: i32 = -1;
		let result: Result<(), ANTLRError> = (|| {

			//recog.base.enter_outer_alt(_localctx.clone(), 1)?;
			recog.base.enter_outer_alt(None, 1)?;
			{
			recog.base.set_state(869);
			recog.err_handler.sync(&mut recog.base)?;
			_la = recog.base.input.la(1);
			if _la==LogQLParser_SUB {
				{
				recog.base.set_state(868);
				recog.base.match_token(LogQLParser_SUB,&mut recog.err_handler)?;

				}
			}

			recog.base.set_state(871);
			recog.base.match_token(LogQLParser_DURATION,&mut recog.err_handler)?;

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
		4, 1, 103, 874, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 2, 2, 3, 7, 3, 2, 4, 
		7, 4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 7, 8, 2, 9, 7, 9, 2, 10, 
		7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 13, 2, 14, 7, 14, 2, 15, 
		7, 15, 2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 18, 2, 19, 7, 19, 2, 20, 
		7, 20, 2, 21, 7, 21, 2, 22, 7, 22, 2, 23, 7, 23, 2, 24, 7, 24, 2, 25, 
		7, 25, 2, 26, 7, 26, 2, 27, 7, 27, 2, 28, 7, 28, 2, 29, 7, 29, 2, 30, 
		7, 30, 2, 31, 7, 31, 2, 32, 7, 32, 2, 33, 7, 33, 2, 34, 7, 34, 2, 35, 
		7, 35, 2, 36, 7, 36, 2, 37, 7, 37, 2, 38, 7, 38, 2, 39, 7, 39, 2, 40, 
		7, 40, 2, 41, 7, 41, 2, 42, 7, 42, 2, 43, 7, 43, 2, 44, 7, 44, 2, 45, 
		7, 45, 2, 46, 7, 46, 2, 47, 7, 47, 2, 48, 7, 48, 2, 49, 7, 49, 2, 50, 
		7, 50, 2, 51, 7, 51, 2, 52, 7, 52, 2, 53, 7, 53, 2, 54, 7, 54, 2, 55, 
		7, 55, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 3, 1, 118, 8, 1, 1, 2, 1, 2, 1, 2, 
		1, 2, 3, 2, 124, 8, 2, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 1, 3, 3, 3, 132, 
		8, 3, 1, 4, 1, 4, 1, 4, 5, 4, 137, 8, 4, 10, 4, 12, 4, 140, 9, 4, 1, 5, 
		3, 5, 143, 8, 5, 1, 5, 1, 5, 1, 5, 1, 5, 3, 5, 149, 8, 5, 1, 5, 1, 5, 
		1, 5, 1, 5, 3, 5, 155, 8, 5, 1, 5, 1, 5, 1, 5, 1, 5, 3, 5, 161, 8, 5, 
		1, 5, 1, 5, 1, 5, 3, 5, 166, 8, 5, 1, 6, 1, 6, 1, 6, 1, 6, 1, 6, 5, 6, 
		173, 8, 6, 10, 6, 12, 6, 176, 9, 6, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 
		1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 
		1, 7, 1, 7, 1, 7, 1, 7, 1, 7, 3, 7, 201, 8, 7, 1, 8, 1, 8, 1, 8, 1, 8, 
		5, 8, 207, 8, 8, 10, 8, 12, 8, 210, 9, 8, 1, 8, 1, 8, 1, 8, 1, 8, 5, 8, 
		216, 8, 8, 10, 8, 12, 8, 219, 9, 8, 1, 8, 1, 8, 1, 8, 1, 8, 5, 8, 225, 
		8, 8, 10, 8, 12, 8, 228, 9, 8, 1, 8, 1, 8, 1, 8, 1, 8, 5, 8, 234, 8, 8, 
		10, 8, 12, 8, 237, 9, 8, 1, 8, 1, 8, 1, 8, 1, 8, 5, 8, 243, 8, 8, 10, 
		8, 12, 8, 246, 9, 8, 1, 8, 1, 8, 1, 8, 1, 8, 5, 8, 252, 8, 8, 10, 8, 12, 
		8, 255, 9, 8, 3, 8, 257, 8, 8, 1, 9, 1, 9, 3, 9, 261, 8, 9, 1, 10, 1, 
		10, 1, 10, 1, 10, 1, 10, 1, 11, 1, 11, 1, 11, 1, 12, 1, 12, 1, 12, 1, 
		13, 1, 13, 1, 14, 1, 14, 5, 14, 278, 8, 14, 10, 14, 12, 14, 281, 9, 14, 
		1, 14, 3, 14, 284, 8, 14, 1, 15, 1, 15, 1, 15, 1, 16, 1, 16, 1, 16, 5, 
		16, 292, 8, 16, 10, 16, 12, 16, 295, 9, 16, 1, 17, 1, 17, 1, 17, 1, 17, 
		1, 17, 1, 17, 3, 17, 303, 8, 17, 1, 18, 1, 18, 1, 18, 1, 19, 1, 19, 1, 
		20, 1, 20, 1, 20, 1, 21, 1, 21, 1, 21, 1, 22, 1, 22, 3, 22, 318, 8, 22, 
		1, 23, 1, 23, 1, 23, 1, 23, 3, 23, 324, 8, 23, 1, 24, 1, 24, 1, 24, 5, 
		24, 329, 8, 24, 10, 24, 12, 24, 332, 9, 24, 1, 25, 1, 25, 1, 25, 1, 25, 
		1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 1, 25, 3, 25, 344, 8, 25, 1, 25, 1, 
		25, 1, 25, 1, 25, 1, 25, 1, 25, 5, 25, 352, 8, 25, 10, 25, 12, 25, 355, 
		9, 25, 1, 26, 1, 26, 1, 26, 1, 26, 1, 27, 1, 27, 1, 27, 1, 27, 1, 28, 
		1, 28, 1, 28, 1, 28, 1, 29, 1, 29, 1, 29, 1, 29, 1, 29, 1, 29, 3, 29, 
		375, 8, 29, 1, 30, 1, 30, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 
		31, 1, 31, 1, 31, 1, 31, 1, 31, 3, 31, 390, 8, 31, 1, 31, 1, 31, 1, 31, 
		1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 
		1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 
		1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 
		1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 
		1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 
		1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 
		1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 1, 31, 
		1, 31, 1, 31, 5, 31, 467, 8, 31, 10, 31, 12, 31, 470, 9, 31, 1, 32, 1, 
		32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 
		32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 
		32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 32, 1, 
		32, 1, 32, 1, 32, 1, 32, 1, 32, 3, 32, 508, 8, 32, 1, 33, 1, 33, 1, 33, 
		1, 33, 1, 33, 3, 33, 515, 8, 33, 1, 34, 1, 34, 1, 34, 3, 34, 520, 8, 34, 
		1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 1, 35, 3, 35, 530, 8, 
		35, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 
		36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 
		36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 
		36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 36, 1, 
		36, 3, 36, 572, 8, 36, 1, 37, 1, 37, 1, 38, 3, 38, 577, 8, 38, 1, 38, 
		1, 38, 1, 38, 3, 38, 582, 8, 38, 3, 38, 584, 8, 38, 3, 38, 586, 8, 38, 
		1, 39, 1, 39, 1, 39, 1, 39, 3, 39, 592, 8, 39, 1, 40, 1, 40, 1, 40, 1, 
		40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 40, 1, 
		40, 1, 40, 1, 40, 3, 40, 610, 8, 40, 1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 
		1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 1, 41, 3, 41, 623, 8, 41, 1, 42, 1, 
		42, 1, 42, 1, 42, 1, 42, 1, 42, 5, 42, 631, 8, 42, 10, 42, 12, 42, 634, 
		9, 42, 1, 43, 3, 43, 637, 8, 43, 1, 43, 1, 43, 1, 44, 1, 44, 1, 44, 5, 
		44, 644, 8, 44, 10, 44, 12, 44, 647, 9, 44, 1, 45, 1, 45, 1, 45, 3, 45, 
		652, 8, 45, 1, 45, 1, 45, 1, 45, 1, 45, 3, 45, 658, 8, 45, 1, 45, 1, 45, 
		1, 45, 1, 45, 1, 45, 3, 45, 665, 8, 45, 1, 45, 1, 45, 1, 45, 1, 45, 1, 
		45, 1, 45, 3, 45, 673, 8, 45, 1, 45, 1, 45, 1, 45, 1, 45, 3, 45, 679, 
		8, 45, 1, 45, 1, 45, 1, 45, 1, 45, 1, 45, 3, 45, 686, 8, 45, 1, 45, 1, 
		45, 1, 45, 1, 45, 1, 45, 1, 45, 3, 45, 694, 8, 45, 1, 45, 1, 45, 1, 45, 
		1, 45, 1, 45, 1, 45, 1, 45, 3, 45, 703, 8, 45, 1, 45, 1, 45, 1, 45, 1, 
		45, 3, 45, 709, 8, 45, 1, 45, 1, 45, 1, 45, 1, 45, 1, 45, 3, 45, 716, 
		8, 45, 1, 45, 1, 45, 1, 45, 1, 45, 3, 45, 722, 8, 45, 1, 46, 1, 46, 1, 
		46, 1, 46, 3, 46, 728, 8, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 3, 46, 
		735, 8, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 3, 46, 743, 8, 46, 
		1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 3, 46, 752, 8, 46, 1, 
		46, 1, 46, 1, 46, 1, 46, 3, 46, 758, 8, 46, 1, 46, 1, 46, 1, 46, 1, 46, 
		1, 46, 3, 46, 765, 8, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 3, 
		46, 773, 8, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 3, 46, 
		782, 8, 46, 1, 46, 1, 46, 1, 46, 1, 46, 1, 46, 3, 46, 789, 8, 46, 1, 46, 
		1, 46, 1, 46, 1, 46, 1, 46, 3, 46, 796, 8, 46, 1, 46, 1, 46, 1, 46, 1, 
		46, 1, 46, 1, 46, 3, 46, 804, 8, 46, 3, 46, 806, 8, 46, 1, 47, 1, 47, 
		1, 47, 1, 47, 1, 48, 1, 48, 1, 48, 1, 49, 1, 49, 1, 49, 1, 49, 1, 49, 
		3, 49, 820, 8, 49, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 50, 1, 
		50, 1, 50, 1, 50, 3, 50, 832, 8, 50, 1, 50, 1, 50, 1, 50, 5, 50, 837, 
		8, 50, 10, 50, 12, 50, 840, 9, 50, 1, 51, 1, 51, 1, 51, 1, 51, 1, 51, 
		3, 51, 847, 8, 51, 1, 52, 1, 52, 1, 52, 1, 52, 1, 52, 1, 52, 1, 52, 1, 
		52, 1, 52, 1, 52, 1, 52, 1, 52, 1, 52, 1, 53, 1, 53, 1, 53, 1, 53, 1, 
		53, 1, 54, 1, 54, 1, 55, 3, 55, 870, 8, 55, 1, 55, 1, 55, 1, 55, 0, 5, 
		12, 50, 62, 84, 100, 56, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 
		26, 28, 30, 32, 34, 36, 38, 40, 42, 44, 46, 48, 50, 52, 54, 56, 58, 60, 
		62, 64, 66, 68, 70, 72, 74, 76, 78, 80, 82, 84, 86, 88, 90, 92, 94, 96, 
		98, 100, 102, 104, 106, 108, 110, 0, 3, 2, 0, 17, 18, 21, 25, 2, 0, 40, 
		49, 64, 65, 1, 0, 61, 62, 971, 0, 112, 1, 0, 0, 0, 2, 117, 1, 0, 0, 0, 
		4, 123, 1, 0, 0, 0, 6, 131, 1, 0, 0, 0, 8, 133, 1, 0, 0, 0, 10, 165, 1, 
		0, 0, 0, 12, 167, 1, 0, 0, 0, 14, 200, 1, 0, 0, 0, 16, 256, 1, 0, 0, 0, 
		18, 260, 1, 0, 0, 0, 20, 262, 1, 0, 0, 0, 22, 267, 1, 0, 0, 0, 24, 270, 
		1, 0, 0, 0, 26, 273, 1, 0, 0, 0, 28, 275, 1, 0, 0, 0, 30, 285, 1, 0, 0, 
		0, 32, 288, 1, 0, 0, 0, 34, 302, 1, 0, 0, 0, 36, 304, 1, 0, 0, 0, 38, 
		307, 1, 0, 0, 0, 40, 309, 1, 0, 0, 0, 42, 312, 1, 0, 0, 0, 44, 315, 1, 
		0, 0, 0, 46, 323, 1, 0, 0, 0, 48, 325, 1, 0, 0, 0, 50, 343, 1, 0, 0, 0, 
		52, 356, 1, 0, 0, 0, 54, 360, 1, 0, 0, 0, 56, 364, 1, 0, 0, 0, 58, 374, 
		1, 0, 0, 0, 60, 376, 1, 0, 0, 0, 62, 389, 1, 0, 0, 0, 64, 507, 1, 0, 0, 
		0, 66, 514, 1, 0, 0, 0, 68, 519, 1, 0, 0, 0, 70, 529, 1, 0, 0, 0, 72, 
		571, 1, 0, 0, 0, 74, 573, 1, 0, 0, 0, 76, 576, 1, 0, 0, 0, 78, 591, 1, 
		0, 0, 0, 80, 609, 1, 0, 0, 0, 82, 622, 1, 0, 0, 0, 84, 624, 1, 0, 0, 0, 
		86, 636, 1, 0, 0, 0, 88, 640, 1, 0, 0, 0, 90, 721, 1, 0, 0, 0, 92, 805, 
		1, 0, 0, 0, 94, 807, 1, 0, 0, 0, 96, 811, 1, 0, 0, 0, 98, 819, 1, 0, 0, 
		0, 100, 831, 1, 0, 0, 0, 102, 846, 1, 0, 0, 0, 104, 848, 1, 0, 0, 0, 106, 
		861, 1, 0, 0, 0, 108, 866, 1, 0, 0, 0, 110, 869, 1, 0, 0, 0, 112, 113, 
		3, 2, 1, 0, 113, 114, 5, 0, 0, 1, 114, 1, 1, 0, 0, 0, 115, 118, 3, 4, 
		2, 0, 116, 118, 3, 62, 31, 0, 117, 115, 1, 0, 0, 0, 117, 116, 1, 0, 0, 
		0, 118, 3, 1, 0, 0, 0, 119, 124, 3, 6, 3, 0, 120, 121, 3, 6, 3, 0, 121, 
		122, 3, 12, 6, 0, 122, 124, 1, 0, 0, 0, 123, 119, 1, 0, 0, 0, 123, 120, 
		1, 0, 0, 0, 124, 5, 1, 0, 0, 0, 125, 126, 5, 3, 0, 0, 126, 127, 3, 8, 
		4, 0, 127, 128, 5, 4, 0, 0, 128, 132, 1, 0, 0, 0, 129, 130, 5, 3, 0, 0, 
		130, 132, 5, 4, 0, 0, 131, 125, 1, 0, 0, 0, 131, 129, 1, 0, 0, 0, 132, 
		7, 1, 0, 0, 0, 133, 138, 3, 10, 5, 0, 134, 135, 5, 8, 0, 0, 135, 137, 
		3, 10, 5, 0, 136, 134, 1, 0, 0, 0, 137, 140, 1, 0, 0, 0, 138, 136, 1, 
		0, 0, 0, 138, 139, 1, 0, 0, 0, 139, 9, 1, 0, 0, 0, 140, 138, 1, 0, 0, 
		0, 141, 143, 5, 97, 0, 0, 142, 141, 1, 0, 0, 0, 142, 143, 1, 0, 0, 0, 
		143, 144, 1, 0, 0, 0, 144, 145, 5, 99, 0, 0, 145, 146, 5, 17, 0, 0, 146, 
		166, 5, 96, 0, 0, 147, 149, 5, 97, 0, 0, 148, 147, 1, 0, 0, 0, 148, 149, 
		1, 0, 0, 0, 149, 150, 1, 0, 0, 0, 150, 151, 5, 99, 0, 0, 151, 152, 5, 
		18, 0, 0, 152, 166, 5, 96, 0, 0, 153, 155, 5, 97, 0, 0, 154, 153, 1, 0, 
		0, 0, 154, 155, 1, 0, 0, 0, 155, 156, 1, 0, 0, 0, 156, 157, 5, 99, 0, 
		0, 157, 158, 5, 19, 0, 0, 158, 166, 5, 96, 0, 0, 159, 161, 5, 97, 0, 0, 
		160, 159, 1, 0, 0, 0, 160, 161, 1, 0, 0, 0, 161, 162, 1, 0, 0, 0, 162, 
		163, 5, 99, 0, 0, 163, 164, 5, 20, 0, 0, 164, 166, 5, 96, 0, 0, 165, 142, 
		1, 0, 0, 0, 165, 148, 1, 0, 0, 0, 165, 154, 1, 0, 0, 0, 165, 160, 1, 0, 
		0, 0, 166, 11, 1, 0, 0, 0, 167, 168, 6, 6, -1, 0, 168, 169, 3, 14, 7, 
		0, 169, 174, 1, 0, 0, 0, 170, 171, 10, 1, 0, 0, 171, 173, 3, 14, 7, 0, 
		172, 170, 1, 0, 0, 0, 173, 176, 1, 0, 0, 0, 174, 172, 1, 0, 0, 0, 174, 
		175, 1, 0, 0, 0, 175, 13, 1, 0, 0, 0, 176, 174, 1, 0, 0, 0, 177, 201, 
		3, 16, 8, 0, 178, 179, 5, 10, 0, 0, 179, 201, 3, 28, 14, 0, 180, 181, 
		5, 10, 0, 0, 181, 201, 3, 22, 11, 0, 182, 183, 5, 10, 0, 0, 183, 201, 
		3, 24, 12, 0, 184, 185, 5, 10, 0, 0, 185, 201, 3, 26, 13, 0, 186, 187, 
		5, 10, 0, 0, 187, 201, 3, 30, 15, 0, 188, 189, 5, 10, 0, 0, 189, 201, 
		3, 36, 18, 0, 190, 191, 5, 10, 0, 0, 191, 201, 3, 38, 19, 0, 192, 193, 
		5, 10, 0, 0, 193, 201, 3, 40, 20, 0, 194, 195, 5, 10, 0, 0, 195, 201, 
		3, 42, 21, 0, 196, 197, 5, 10, 0, 0, 197, 201, 3, 44, 22, 0, 198, 199, 
		5, 10, 0, 0, 199, 201, 3, 50, 25, 0, 200, 177, 1, 0, 0, 0, 200, 178, 1, 
		0, 0, 0, 200, 180, 1, 0, 0, 0, 200, 182, 1, 0, 0, 0, 200, 184, 1, 0, 0, 
		0, 200, 186, 1, 0, 0, 0, 200, 188, 1, 0, 0, 0, 200, 190, 1, 0, 0, 0, 200, 
		192, 1, 0, 0, 0, 200, 194, 1, 0, 0, 0, 200, 196, 1, 0, 0, 0, 200, 198, 
		1, 0, 0, 0, 201, 15, 1, 0, 0, 0, 202, 203, 5, 26, 0, 0, 203, 208, 3, 18, 
		9, 0, 204, 205, 5, 31, 0, 0, 205, 207, 3, 18, 9, 0, 206, 204, 1, 0, 0, 
		0, 207, 210, 1, 0, 0, 0, 208, 206, 1, 0, 0, 0, 208, 209, 1, 0, 0, 0, 209, 
		257, 1, 0, 0, 0, 210, 208, 1, 0, 0, 0, 211, 212, 5, 18, 0, 0, 212, 217, 
		3, 18, 9, 0, 213, 214, 5, 31, 0, 0, 214, 216, 3, 18, 9, 0, 215, 213, 1, 
		0, 0, 0, 216, 219, 1, 0, 0, 0, 217, 215, 1, 0, 0, 0, 217, 218, 1, 0, 0, 
		0, 218, 257, 1, 0, 0, 0, 219, 217, 1, 0, 0, 0, 220, 221, 5, 27, 0, 0, 
		221, 226, 3, 18, 9, 0, 222, 223, 5, 31, 0, 0, 223, 225, 3, 18, 9, 0, 224, 
		222, 1, 0, 0, 0, 225, 228, 1, 0, 0, 0, 226, 224, 1, 0, 0, 0, 226, 227, 
		1, 0, 0, 0, 227, 257, 1, 0, 0, 0, 228, 226, 1, 0, 0, 0, 229, 230, 5, 20, 
		0, 0, 230, 235, 3, 18, 9, 0, 231, 232, 5, 31, 0, 0, 232, 234, 3, 18, 9, 
		0, 233, 231, 1, 0, 0, 0, 234, 237, 1, 0, 0, 0, 235, 233, 1, 0, 0, 0, 235, 
		236, 1, 0, 0, 0, 236, 257, 1, 0, 0, 0, 237, 235, 1, 0, 0, 0, 238, 239, 
		5, 28, 0, 0, 239, 244, 3, 18, 9, 0, 240, 241, 5, 31, 0, 0, 241, 243, 3, 
		18, 9, 0, 242, 240, 1, 0, 0, 0, 243, 246, 1, 0, 0, 0, 244, 242, 1, 0, 
		0, 0, 244, 245, 1, 0, 0, 0, 245, 257, 1, 0, 0, 0, 246, 244, 1, 0, 0, 0, 
		247, 248, 5, 29, 0, 0, 248, 253, 3, 18, 9, 0, 249, 250, 5, 31, 0, 0, 250, 
		252, 3, 18, 9, 0, 251, 249, 1, 0, 0, 0, 252, 255, 1, 0, 0, 0, 253, 251, 
		1, 0, 0, 0, 253, 254, 1, 0, 0, 0, 254, 257, 1, 0, 0, 0, 255, 253, 1, 0, 
		0, 0, 256, 202, 1, 0, 0, 0, 256, 211, 1, 0, 0, 0, 256, 220, 1, 0, 0, 0, 
		256, 229, 1, 0, 0, 0, 256, 238, 1, 0, 0, 0, 256, 247, 1, 0, 0, 0, 257, 
		17, 1, 0, 0, 0, 258, 261, 5, 96, 0, 0, 259, 261, 3, 20, 10, 0, 260, 258, 
		1, 0, 0, 0, 260, 259, 1, 0, 0, 0, 261, 19, 1, 0, 0, 0, 262, 263, 5, 98, 
		0, 0, 263, 264, 5, 1, 0, 0, 264, 265, 5, 96, 0, 0, 265, 266, 5, 2, 0, 
		0, 266, 21, 1, 0, 0, 0, 267, 268, 5, 54, 0, 0, 268, 269, 5, 96, 0, 0, 
		269, 23, 1, 0, 0, 0, 270, 271, 5, 53, 0, 0, 271, 272, 5, 96, 0, 0, 272, 
		25, 1, 0, 0, 0, 273, 274, 5, 52, 0, 0, 274, 27, 1, 0, 0, 0, 275, 279, 
		5, 51, 0, 0, 276, 278, 5, 83, 0, 0, 277, 276, 1, 0, 0, 0, 278, 281, 1, 
		0, 0, 0, 279, 277, 1, 0, 0, 0, 279, 280, 1, 0, 0, 0, 280, 283, 1, 0, 0, 
		0, 281, 279, 1, 0, 0, 0, 282, 284, 3, 48, 24, 0, 283, 282, 1, 0, 0, 0, 
		283, 284, 1, 0, 0, 0, 284, 29, 1, 0, 0, 0, 285, 286, 5, 56, 0, 0, 286, 
		287, 3, 32, 16, 0, 287, 31, 1, 0, 0, 0, 288, 293, 3, 34, 17, 0, 289, 290, 
		5, 8, 0, 0, 290, 292, 3, 34, 17, 0, 291, 289, 1, 0, 0, 0, 292, 295, 1, 
		0, 0, 0, 293, 291, 1, 0, 0, 0, 293, 294, 1, 0, 0, 0, 294, 33, 1, 0, 0, 
		0, 295, 293, 1, 0, 0, 0, 296, 297, 5, 99, 0, 0, 297, 298, 5, 17, 0, 0, 
		298, 303, 5, 99, 0, 0, 299, 300, 5, 99, 0, 0, 300, 301, 5, 17, 0, 0, 301, 
		303, 5, 96, 0, 0, 302, 296, 1, 0, 0, 0, 302, 299, 1, 0, 0, 0, 303, 35, 
		1, 0, 0, 0, 304, 305, 5, 55, 0, 0, 305, 306, 5, 96, 0, 0, 306, 37, 1, 
		0, 0, 0, 307, 308, 5, 38, 0, 0, 308, 39, 1, 0, 0, 0, 309, 310, 5, 37, 
		0, 0, 310, 311, 3, 48, 24, 0, 311, 41, 1, 0, 0, 0, 312, 313, 5, 36, 0, 
		0, 313, 314, 3, 48, 24, 0, 314, 43, 1, 0, 0, 0, 315, 317, 5, 50, 0, 0, 
		316, 318, 3, 48, 24, 0, 317, 316, 1, 0, 0, 0, 317, 318, 1, 0, 0, 0, 318, 
		45, 1, 0, 0, 0, 319, 320, 5, 99, 0, 0, 320, 321, 5, 17, 0, 0, 321, 324, 
		5, 96, 0, 0, 322, 324, 5, 99, 0, 0, 323, 319, 1, 0, 0, 0, 323, 322, 1, 
		0, 0, 0, 324, 47, 1, 0, 0, 0, 325, 330, 3, 46, 23, 0, 326, 327, 5, 8, 
		0, 0, 327, 329, 3, 46, 23, 0, 328, 326, 1, 0, 0, 0, 329, 332, 1, 0, 0, 
		0, 330, 328, 1, 0, 0, 0, 330, 331, 1, 0, 0, 0, 331, 49, 1, 0, 0, 0, 332, 
		330, 1, 0, 0, 0, 333, 334, 6, 25, -1, 0, 334, 335, 5, 1, 0, 0, 335, 336, 
		3, 50, 25, 0, 336, 337, 5, 2, 0, 0, 337, 344, 1, 0, 0, 0, 338, 344, 3, 
		10, 5, 0, 339, 344, 3, 52, 26, 0, 340, 344, 3, 54, 27, 0, 341, 344, 3, 
		56, 28, 0, 342, 344, 3, 58, 29, 0, 343, 333, 1, 0, 0, 0, 343, 338, 1, 
		0, 0, 0, 343, 339, 1, 0, 0, 0, 343, 340, 1, 0, 0, 0, 343, 341, 1, 0, 0, 
		0, 343, 342, 1, 0, 0, 0, 344, 353, 1, 0, 0, 0, 345, 346, 10, 8, 0, 0, 
		346, 347, 5, 30, 0, 0, 347, 352, 3, 50, 25, 9, 348, 349, 10, 7, 0, 0, 
		349, 350, 5, 31, 0, 0, 350, 352, 3, 50, 25, 8, 351, 345, 1, 0, 0, 0, 351, 
		348, 1, 0, 0, 0, 352, 355, 1, 0, 0, 0, 353, 351, 1, 0, 0, 0, 353, 354, 
		1, 0, 0, 0, 354, 51, 1, 0, 0, 0, 355, 353, 1, 0, 0, 0, 356, 357, 5, 99, 
		0, 0, 357, 358, 3, 60, 30, 0, 358, 359, 3, 102, 51, 0, 359, 53, 1, 0, 
		0, 0, 360, 361, 5, 99, 0, 0, 361, 362, 3, 60, 30, 0, 362, 363, 3, 110, 
		55, 0, 363, 55, 1, 0, 0, 0, 364, 365, 5, 99, 0, 0, 365, 366, 3, 60, 30, 
		0, 366, 367, 5, 95, 0, 0, 367, 57, 1, 0, 0, 0, 368, 369, 5, 99, 0, 0, 
		369, 370, 5, 17, 0, 0, 370, 375, 3, 20, 10, 0, 371, 372, 5, 99, 0, 0, 
		372, 373, 5, 18, 0, 0, 373, 375, 3, 20, 10, 0, 374, 368, 1, 0, 0, 0, 374, 
		371, 1, 0, 0, 0, 375, 59, 1, 0, 0, 0, 376, 377, 7, 0, 0, 0, 377, 61, 1, 
		0, 0, 0, 378, 379, 6, 31, -1, 0, 379, 390, 3, 64, 32, 0, 380, 390, 3, 
		72, 36, 0, 381, 390, 3, 102, 51, 0, 382, 390, 3, 104, 52, 0, 383, 390, 
		3, 106, 53, 0, 384, 390, 3, 108, 54, 0, 385, 386, 5, 1, 0, 0, 386, 387, 
		3, 62, 31, 0, 387, 388, 5, 2, 0, 0, 388, 390, 1, 0, 0, 0, 389, 378, 1, 
		0, 0, 0, 389, 380, 1, 0, 0, 0, 389, 381, 1, 0, 0, 0, 389, 382, 1, 0, 0, 
		0, 389, 383, 1, 0, 0, 0, 389, 384, 1, 0, 0, 0, 389, 385, 1, 0, 0, 0, 390, 
		468, 1, 0, 0, 0, 391, 392, 10, 22, 0, 0, 392, 393, 5, 16, 0, 0, 393, 394, 
		3, 76, 38, 0, 394, 395, 3, 62, 31, 23, 395, 467, 1, 0, 0, 0, 396, 397, 
		10, 21, 0, 0, 397, 398, 5, 14, 0, 0, 398, 399, 3, 76, 38, 0, 399, 400, 
		3, 62, 31, 22, 400, 467, 1, 0, 0, 0, 401, 402, 10, 20, 0, 0, 402, 403, 
		5, 15, 0, 0, 403, 404, 3, 76, 38, 0, 404, 405, 3, 62, 31, 21, 405, 467, 
		1, 0, 0, 0, 406, 407, 10, 19, 0, 0, 407, 408, 5, 82, 0, 0, 408, 409, 3, 
		76, 38, 0, 409, 410, 3, 62, 31, 20, 410, 467, 1, 0, 0, 0, 411, 412, 10, 
		18, 0, 0, 412, 413, 5, 12, 0, 0, 413, 414, 3, 76, 38, 0, 414, 415, 3, 
		62, 31, 19, 415, 467, 1, 0, 0, 0, 416, 417, 10, 17, 0, 0, 417, 418, 5, 
		13, 0, 0, 418, 419, 3, 76, 38, 0, 419, 420, 3, 62, 31, 18, 420, 467, 1, 
		0, 0, 0, 421, 422, 10, 16, 0, 0, 422, 423, 5, 25, 0, 0, 423, 424, 3, 76, 
		38, 0, 424, 425, 3, 62, 31, 17, 425, 467, 1, 0, 0, 0, 426, 427, 10, 15, 
		0, 0, 427, 428, 5, 18, 0, 0, 428, 429, 3, 76, 38, 0, 429, 430, 3, 62, 
		31, 16, 430, 467, 1, 0, 0, 0, 431, 432, 10, 14, 0, 0, 432, 433, 5, 21, 
		0, 0, 433, 434, 3, 76, 38, 0, 434, 435, 3, 62, 31, 15, 435, 467, 1, 0, 
		0, 0, 436, 437, 10, 13, 0, 0, 437, 438, 5, 23, 0, 0, 438, 439, 3, 76, 
		38, 0, 439, 440, 3, 62, 31, 14, 440, 467, 1, 0, 0, 0, 441, 442, 10, 12, 
		0, 0, 442, 443, 5, 22, 0, 0, 443, 444, 3, 76, 38, 0, 444, 445, 3, 62, 
		31, 13, 445, 467, 1, 0, 0, 0, 446, 447, 10, 11, 0, 0, 447, 448, 5, 24, 
		0, 0, 448, 449, 3, 76, 38, 0, 449, 450, 3, 62, 31, 12, 450, 467, 1, 0, 
		0, 0, 451, 452, 10, 10, 0, 0, 452, 453, 5, 30, 0, 0, 453, 454, 3, 76, 
		38, 0, 454, 455, 3, 62, 31, 11, 455, 467, 1, 0, 0, 0, 456, 457, 10, 9, 
		0, 0, 457, 458, 5, 31, 0, 0, 458, 459, 3, 76, 38, 0, 459, 460, 3, 62, 
		31, 10, 460, 467, 1, 0, 0, 0, 461, 462, 10, 8, 0, 0, 462, 463, 5, 32, 
		0, 0, 463, 464, 3, 76, 38, 0, 464, 465, 3, 62, 31, 9, 465, 467, 1, 0, 
		0, 0, 466, 391, 1, 0, 0, 0, 466, 396, 1, 0, 0, 0, 466, 401, 1, 0, 0, 0, 
		466, 406, 1, 0, 0, 0, 466, 411, 1, 0, 0, 0, 466, 416, 1, 0, 0, 0, 466, 
		421, 1, 0, 0, 0, 466, 426, 1, 0, 0, 0, 466, 431, 1, 0, 0, 0, 466, 436, 
		1, 0, 0, 0, 466, 441, 1, 0, 0, 0, 466, 446, 1, 0, 0, 0, 466, 451, 1, 0, 
		0, 0, 466, 456, 1, 0, 0, 0, 466, 461, 1, 0, 0, 0, 467, 470, 1, 0, 0, 0, 
		468, 466, 1, 0, 0, 0, 468, 469, 1, 0, 0, 0, 469, 63, 1, 0, 0, 0, 470, 
		468, 1, 0, 0, 0, 471, 472, 3, 66, 33, 0, 472, 473, 5, 1, 0, 0, 473, 474, 
		3, 90, 45, 0, 474, 475, 5, 2, 0, 0, 475, 508, 1, 0, 0, 0, 476, 477, 3, 
		68, 34, 0, 477, 478, 5, 1, 0, 0, 478, 479, 3, 92, 46, 0, 479, 480, 5, 
		2, 0, 0, 480, 508, 1, 0, 0, 0, 481, 482, 3, 70, 35, 0, 482, 483, 5, 1, 
		0, 0, 483, 484, 3, 92, 46, 0, 484, 485, 5, 2, 0, 0, 485, 486, 3, 80, 40, 
		0, 486, 508, 1, 0, 0, 0, 487, 488, 3, 70, 35, 0, 488, 489, 5, 1, 0, 0, 
		489, 490, 3, 92, 46, 0, 490, 491, 5, 2, 0, 0, 491, 508, 1, 0, 0, 0, 492, 
		493, 3, 70, 35, 0, 493, 494, 5, 1, 0, 0, 494, 495, 5, 93, 0, 0, 495, 496, 
		5, 8, 0, 0, 496, 497, 3, 92, 46, 0, 497, 498, 5, 2, 0, 0, 498, 499, 3, 
		80, 40, 0, 499, 508, 1, 0, 0, 0, 500, 501, 3, 70, 35, 0, 501, 502, 5, 
		1, 0, 0, 502, 503, 5, 93, 0, 0, 503, 504, 5, 8, 0, 0, 504, 505, 3, 92, 
		46, 0, 505, 506, 5, 2, 0, 0, 506, 508, 1, 0, 0, 0, 507, 471, 1, 0, 0, 
		0, 507, 476, 1, 0, 0, 0, 507, 481, 1, 0, 0, 0, 507, 487, 1, 0, 0, 0, 507, 
		492, 1, 0, 0, 0, 507, 500, 1, 0, 0, 0, 508, 65, 1, 0, 0, 0, 509, 515, 
		5, 67, 0, 0, 510, 515, 5, 68, 0, 0, 511, 515, 5, 70, 0, 0, 512, 515, 5, 
		71, 0, 0, 513, 515, 5, 81, 0, 0, 514, 509, 1, 0, 0, 0, 514, 510, 1, 0, 
		0, 0, 514, 511, 1, 0, 0, 0, 514, 512, 1, 0, 0, 0, 514, 513, 1, 0, 0, 0, 
		515, 67, 1, 0, 0, 0, 516, 520, 5, 73, 0, 0, 517, 520, 5, 68, 0, 0, 518, 
		520, 5, 69, 0, 0, 519, 516, 1, 0, 0, 0, 519, 517, 1, 0, 0, 0, 519, 518, 
		1, 0, 0, 0, 520, 69, 1, 0, 0, 0, 521, 530, 5, 72, 0, 0, 522, 530, 5, 74, 
		0, 0, 523, 530, 5, 75, 0, 0, 524, 530, 5, 76, 0, 0, 525, 530, 5, 77, 0, 
		0, 526, 530, 5, 78, 0, 0, 527, 530, 5, 79, 0, 0, 528, 530, 5, 80, 0, 0, 
		529, 521, 1, 0, 0, 0, 529, 522, 1, 0, 0, 0, 529, 523, 1, 0, 0, 0, 529, 
		524, 1, 0, 0, 0, 529, 525, 1, 0, 0, 0, 529, 526, 1, 0, 0, 0, 529, 527, 
		1, 0, 0, 0, 529, 528, 1, 0, 0, 0, 530, 71, 1, 0, 0, 0, 531, 532, 3, 74, 
		37, 0, 532, 533, 5, 1, 0, 0, 533, 534, 3, 62, 31, 0, 534, 535, 5, 2, 0, 
		0, 535, 572, 1, 0, 0, 0, 536, 537, 3, 74, 37, 0, 537, 538, 3, 80, 40, 
		0, 538, 539, 5, 1, 0, 0, 539, 540, 3, 62, 31, 0, 540, 541, 5, 2, 0, 0, 
		541, 572, 1, 0, 0, 0, 542, 543, 3, 74, 37, 0, 543, 544, 5, 1, 0, 0, 544, 
		545, 3, 62, 31, 0, 545, 546, 5, 2, 0, 0, 546, 547, 3, 80, 40, 0, 547, 
		572, 1, 0, 0, 0, 548, 549, 3, 74, 37, 0, 549, 550, 5, 1, 0, 0, 550, 551, 
		5, 93, 0, 0, 551, 552, 5, 8, 0, 0, 552, 553, 3, 62, 31, 0, 553, 554, 5, 
		2, 0, 0, 554, 572, 1, 0, 0, 0, 555, 556, 3, 74, 37, 0, 556, 557, 5, 1, 
		0, 0, 557, 558, 5, 93, 0, 0, 558, 559, 5, 8, 0, 0, 559, 560, 3, 62, 31, 
		0, 560, 561, 5, 2, 0, 0, 561, 562, 3, 80, 40, 0, 562, 572, 1, 0, 0, 0, 
		563, 564, 3, 74, 37, 0, 564, 565, 3, 80, 40, 0, 565, 566, 5, 1, 0, 0, 
		566, 567, 5, 93, 0, 0, 567, 568, 5, 8, 0, 0, 568, 569, 3, 62, 31, 0, 569, 
		570, 5, 2, 0, 0, 570, 572, 1, 0, 0, 0, 571, 531, 1, 0, 0, 0, 571, 536, 
		1, 0, 0, 0, 571, 542, 1, 0, 0, 0, 571, 548, 1, 0, 0, 0, 571, 555, 1, 0, 
		0, 0, 571, 563, 1, 0, 0, 0, 572, 73, 1, 0, 0, 0, 573, 574, 7, 1, 0, 0, 
		574, 75, 1, 0, 0, 0, 575, 577, 5, 33, 0, 0, 576, 575, 1, 0, 0, 0, 576, 
		577, 1, 0, 0, 0, 577, 585, 1, 0, 0, 0, 578, 583, 3, 78, 39, 0, 579, 581, 
		7, 2, 0, 0, 580, 582, 3, 82, 41, 0, 581, 580, 1, 0, 0, 0, 581, 582, 1, 
		0, 0, 0, 582, 584, 1, 0, 0, 0, 583, 579, 1, 0, 0, 0, 583, 584, 1, 0, 0, 
		0, 584, 586, 1, 0, 0, 0, 585, 578, 1, 0, 0, 0, 585, 586, 1, 0, 0, 0, 586, 
		77, 1, 0, 0, 0, 587, 588, 5, 60, 0, 0, 588, 592, 3, 82, 41, 0, 589, 590, 
		5, 59, 0, 0, 590, 592, 3, 82, 41, 0, 591, 587, 1, 0, 0, 0, 591, 589, 1, 
		0, 0, 0, 592, 79, 1, 0, 0, 0, 593, 594, 5, 34, 0, 0, 594, 595, 5, 1, 0, 
		0, 595, 596, 3, 88, 44, 0, 596, 597, 5, 2, 0, 0, 597, 610, 1, 0, 0, 0, 
		598, 599, 5, 35, 0, 0, 599, 600, 5, 1, 0, 0, 600, 601, 3, 88, 44, 0, 601, 
		602, 5, 2, 0, 0, 602, 610, 1, 0, 0, 0, 603, 604, 5, 34, 0, 0, 604, 605, 
		5, 1, 0, 0, 605, 610, 5, 2, 0, 0, 606, 607, 5, 35, 0, 0, 607, 608, 5, 
		1, 0, 0, 608, 610, 5, 2, 0, 0, 609, 593, 1, 0, 0, 0, 609, 598, 1, 0, 0, 
		0, 609, 603, 1, 0, 0, 0, 609, 606, 1, 0, 0, 0, 610, 81, 1, 0, 0, 0, 611, 
		612, 5, 1, 0, 0, 612, 613, 3, 84, 42, 0, 613, 614, 5, 2, 0, 0, 614, 623, 
		1, 0, 0, 0, 615, 616, 5, 1, 0, 0, 616, 617, 3, 84, 42, 0, 617, 618, 5, 
		8, 0, 0, 618, 619, 5, 2, 0, 0, 619, 623, 1, 0, 0, 0, 620, 621, 5, 1, 0, 
		0, 621, 623, 5, 2, 0, 0, 622, 611, 1, 0, 0, 0, 622, 615, 1, 0, 0, 0, 622, 
		620, 1, 0, 0, 0, 623, 83, 1, 0, 0, 0, 624, 625, 6, 42, -1, 0, 625, 626, 
		3, 86, 43, 0, 626, 632, 1, 0, 0, 0, 627, 628, 10, 2, 0, 0, 628, 629, 5, 
		8, 0, 0, 629, 631, 3, 86, 43, 0, 630, 627, 1, 0, 0, 0, 631, 634, 1, 0, 
		0, 0, 632, 630, 1, 0, 0, 0, 632, 633, 1, 0, 0, 0, 633, 85, 1, 0, 0, 0, 
		634, 632, 1, 0, 0, 0, 635, 637, 5, 97, 0, 0, 636, 635, 1, 0, 0, 0, 636, 
		637, 1, 0, 0, 0, 637, 638, 1, 0, 0, 0, 638, 639, 5, 99, 0, 0, 639, 87, 
		1, 0, 0, 0, 640, 645, 3, 86, 43, 0, 641, 642, 5, 8, 0, 0, 642, 644, 3, 
		86, 43, 0, 643, 641, 1, 0, 0, 0, 644, 647, 1, 0, 0, 0, 645, 643, 1, 0, 
		0, 0, 645, 646, 1, 0, 0, 0, 646, 89, 1, 0, 0, 0, 647, 645, 1, 0, 0, 0, 
		648, 649, 3, 6, 3, 0, 649, 651, 3, 94, 47, 0, 650, 652, 3, 98, 49, 0, 
		651, 650, 1, 0, 0, 0, 651, 652, 1, 0, 0, 0, 652, 722, 1, 0, 0, 0, 653, 
		654, 3, 6, 3, 0, 654, 655, 3, 94, 47, 0, 655, 657, 3, 96, 48, 0, 656, 
		658, 3, 98, 49, 0, 657, 656, 1, 0, 0, 0, 657, 658, 1, 0, 0, 0, 658, 722, 
		1, 0, 0, 0, 659, 660, 5, 1, 0, 0, 660, 661, 3, 6, 3, 0, 661, 662, 5, 2, 
		0, 0, 662, 664, 3, 94, 47, 0, 663, 665, 3, 98, 49, 0, 664, 663, 1, 0, 
		0, 0, 664, 665, 1, 0, 0, 0, 665, 722, 1, 0, 0, 0, 666, 667, 5, 1, 0, 0, 
		667, 668, 3, 6, 3, 0, 668, 669, 5, 2, 0, 0, 669, 670, 3, 94, 47, 0, 670, 
		672, 3, 96, 48, 0, 671, 673, 3, 98, 49, 0, 672, 671, 1, 0, 0, 0, 672, 
		673, 1, 0, 0, 0, 673, 722, 1, 0, 0, 0, 674, 675, 3, 6, 3, 0, 675, 676, 
		3, 12, 6, 0, 676, 678, 3, 94, 47, 0, 677, 679, 3, 98, 49, 0, 678, 677, 
		1, 0, 0, 0, 678, 679, 1, 0, 0, 0, 679, 722, 1, 0, 0, 0, 680, 681, 3, 6, 
		3, 0, 681, 682, 3, 12, 6, 0, 682, 683, 3, 94, 47, 0, 683, 685, 3, 96, 
		48, 0, 684, 686, 3, 98, 49, 0, 685, 684, 1, 0, 0, 0, 685, 686, 1, 0, 0, 
		0, 686, 722, 1, 0, 0, 0, 687, 688, 5, 1, 0, 0, 688, 689, 3, 6, 3, 0, 689, 
		690, 3, 12, 6, 0, 690, 691, 5, 2, 0, 0, 691, 693, 3, 94, 47, 0, 692, 694, 
		3, 98, 49, 0, 693, 692, 1, 0, 0, 0, 693, 694, 1, 0, 0, 0, 694, 722, 1, 
		0, 0, 0, 695, 696, 5, 1, 0, 0, 696, 697, 3, 6, 3, 0, 697, 698, 3, 12, 
		6, 0, 698, 699, 5, 2, 0, 0, 699, 700, 3, 94, 47, 0, 700, 702, 3, 96, 48, 
		0, 701, 703, 3, 98, 49, 0, 702, 701, 1, 0, 0, 0, 702, 703, 1, 0, 0, 0, 
		703, 722, 1, 0, 0, 0, 704, 705, 3, 6, 3, 0, 705, 706, 3, 94, 47, 0, 706, 
		708, 3, 12, 6, 0, 707, 709, 3, 98, 49, 0, 708, 707, 1, 0, 0, 0, 708, 709, 
		1, 0, 0, 0, 709, 722, 1, 0, 0, 0, 710, 711, 3, 6, 3, 0, 711, 712, 3, 94, 
		47, 0, 712, 713, 3, 96, 48, 0, 713, 715, 3, 12, 6, 0, 714, 716, 3, 98, 
		49, 0, 715, 714, 1, 0, 0, 0, 715, 716, 1, 0, 0, 0, 716, 722, 1, 0, 0, 
		0, 717, 718, 5, 1, 0, 0, 718, 719, 3, 90, 45, 0, 719, 720, 5, 2, 0, 0, 
		720, 722, 1, 0, 0, 0, 721, 648, 1, 0, 0, 0, 721, 653, 1, 0, 0, 0, 721, 
		659, 1, 0, 0, 0, 721, 666, 1, 0, 0, 0, 721, 674, 1, 0, 0, 0, 721, 680, 
		1, 0, 0, 0, 721, 687, 1, 0, 0, 0, 721, 695, 1, 0, 0, 0, 721, 704, 1, 0, 
		0, 0, 721, 710, 1, 0, 0, 0, 721, 717, 1, 0, 0, 0, 722, 91, 1, 0, 0, 0, 
		723, 724, 3, 6, 3, 0, 724, 725, 3, 94, 47, 0, 725, 727, 3, 100, 50, 0, 
		726, 728, 3, 98, 49, 0, 727, 726, 1, 0, 0, 0, 727, 728, 1, 0, 0, 0, 728, 
		806, 1, 0, 0, 0, 729, 730, 3, 6, 3, 0, 730, 731, 3, 94, 47, 0, 731, 732, 
		3, 96, 48, 0, 732, 734, 3, 100, 50, 0, 733, 735, 3, 98, 49, 0, 734, 733, 
		1, 0, 0, 0, 734, 735, 1, 0, 0, 0, 735, 806, 1, 0, 0, 0, 736, 737, 5, 1, 
		0, 0, 737, 738, 3, 6, 3, 0, 738, 739, 5, 2, 0, 0, 739, 740, 3, 94, 47, 
		0, 740, 742, 3, 100, 50, 0, 741, 743, 3, 98, 49, 0, 742, 741, 1, 0, 0, 
		0, 742, 743, 1, 0, 0, 0, 743, 806, 1, 0, 0, 0, 744, 745, 5, 1, 0, 0, 745, 
		746, 3, 6, 3, 0, 746, 747, 5, 2, 0, 0, 747, 748, 3, 94, 47, 0, 748, 749, 
		3, 96, 48, 0, 749, 751, 3, 100, 50, 0, 750, 752, 3, 98, 49, 0, 751, 750, 
		1, 0, 0, 0, 751, 752, 1, 0, 0, 0, 752, 806, 1, 0, 0, 0, 753, 754, 3, 6, 
		3, 0, 754, 755, 3, 100, 50, 0, 755, 757, 3, 94, 47, 0, 756, 758, 3, 98, 
		49, 0, 757, 756, 1, 0, 0, 0, 757, 758, 1, 0, 0, 0, 758, 806, 1, 0, 0, 
		0, 759, 760, 3, 6, 3, 0, 760, 761, 3, 100, 50, 0, 761, 762, 3, 94, 47, 
		0, 762, 764, 3, 96, 48, 0, 763, 765, 3, 98, 49, 0, 764, 763, 1, 0, 0, 
		0, 764, 765, 1, 0, 0, 0, 765, 806, 1, 0, 0, 0, 766, 767, 5, 1, 0, 0, 767, 
		768, 3, 6, 3, 0, 768, 769, 3, 100, 50, 0, 769, 770, 5, 2, 0, 0, 770, 772, 
		3, 94, 47, 0, 771, 773, 3, 98, 49, 0, 772, 771, 1, 0, 0, 0, 772, 773, 
		1, 0, 0, 0, 773, 806, 1, 0, 0, 0, 774, 775, 5, 1, 0, 0, 775, 776, 3, 6, 
		3, 0, 776, 777, 3, 100, 50, 0, 777, 778, 5, 2, 0, 0, 778, 779, 3, 94, 
		47, 0, 779, 781, 3, 96, 48, 0, 780, 782, 3, 98, 49, 0, 781, 780, 1, 0, 
		0, 0, 781, 782, 1, 0, 0, 0, 782, 806, 1, 0, 0, 0, 783, 784, 3, 6, 3, 0, 
		784, 785, 3, 94, 47, 0, 785, 786, 3, 12, 6, 0, 786, 788, 3, 100, 50, 0, 
		787, 789, 3, 98, 49, 0, 788, 787, 1, 0, 0, 0, 788, 789, 1, 0, 0, 0, 789, 
		806, 1, 0, 0, 0, 790, 791, 3, 6, 3, 0, 791, 792, 3, 12, 6, 0, 792, 793, 
		3, 100, 50, 0, 793, 795, 3, 94, 47, 0, 794, 796, 3, 98, 49, 0, 795, 794, 
		1, 0, 0, 0, 795, 796, 1, 0, 0, 0, 796, 806, 1, 0, 0, 0, 797, 798, 3, 6, 
		3, 0, 798, 799, 3, 94, 47, 0, 799, 800, 3, 96, 48, 0, 800, 801, 3, 12, 
		6, 0, 801, 803, 3, 100, 50, 0, 802, 804, 3, 98, 49, 0, 803, 802, 1, 0, 
		0, 0, 803, 804, 1, 0, 0, 0, 804, 806, 1, 0, 0, 0, 805, 723, 1, 0, 0, 0, 
		805, 729, 1, 0, 0, 0, 805, 736, 1, 0, 0, 0, 805, 744, 1, 0, 0, 0, 805, 
		753, 1, 0, 0, 0, 805, 759, 1, 0, 0, 0, 805, 766, 1, 0, 0, 0, 805, 774, 
		1, 0, 0, 0, 805, 783, 1, 0, 0, 0, 805, 790, 1, 0, 0, 0, 805, 797, 1, 0, 
		0, 0, 806, 93, 1, 0, 0, 0, 807, 808, 5, 5, 0, 0, 808, 809, 3, 110, 55, 
		0, 809, 810, 5, 6, 0, 0, 810, 95, 1, 0, 0, 0, 811, 812, 5, 58, 0, 0, 812, 
		813, 3, 110, 55, 0, 813, 97, 1, 0, 0, 0, 814, 815, 5, 66, 0, 0, 815, 820, 
		5, 93, 0, 0, 816, 817, 5, 66, 0, 0, 817, 818, 5, 13, 0, 0, 818, 820, 5, 
		93, 0, 0, 819, 814, 1, 0, 0, 0, 819, 816, 1, 0, 0, 0, 820, 99, 1, 0, 0, 
		0, 821, 822, 6, 50, -1, 0, 822, 823, 5, 10, 0, 0, 823, 824, 5, 63, 0, 
		0, 824, 832, 5, 99, 0, 0, 825, 826, 5, 10, 0, 0, 826, 827, 5, 63, 0, 0, 
		827, 828, 5, 99, 0, 0, 828, 829, 5, 1, 0, 0, 829, 830, 5, 99, 0, 0, 830, 
		832, 5, 2, 0, 0, 831, 821, 1, 0, 0, 0, 831, 825, 1, 0, 0, 0, 832, 838, 
		1, 0, 0, 0, 833, 834, 10, 1, 0, 0, 834, 835, 5, 10, 0, 0, 835, 837, 3, 
		50, 25, 0, 836, 833, 1, 0, 0, 0, 837, 840, 1, 0, 0, 0, 838, 836, 1, 0, 
		0, 0, 838, 839, 1, 0, 0, 0, 839, 101, 1, 0, 0, 0, 840, 838, 1, 0, 0, 0, 
		841, 847, 5, 93, 0, 0, 842, 843, 5, 12, 0, 0, 843, 847, 5, 93, 0, 0, 844, 
		845, 5, 13, 0, 0, 845, 847, 5, 93, 0, 0, 846, 841, 1, 0, 0, 0, 846, 842, 
		1, 0, 0, 0, 846, 844, 1, 0, 0, 0, 847, 103, 1, 0, 0, 0, 848, 849, 5, 39, 
		0, 0, 849, 850, 5, 1, 0, 0, 850, 851, 3, 62, 31, 0, 851, 852, 5, 8, 0, 
		0, 852, 853, 5, 96, 0, 0, 853, 854, 5, 8, 0, 0, 854, 855, 5, 96, 0, 0, 
		855, 856, 5, 8, 0, 0, 856, 857, 5, 96, 0, 0, 857, 858, 5, 8, 0, 0, 858, 
		859, 5, 96, 0, 0, 859, 860, 5, 2, 0, 0, 860, 105, 1, 0, 0, 0, 861, 862, 
		5, 57, 0, 0, 862, 863, 5, 1, 0, 0, 863, 864, 5, 93, 0, 0, 864, 865, 5, 
		2, 0, 0, 865, 107, 1, 0, 0, 0, 866, 867, 5, 99, 0, 0, 867, 109, 1, 0, 
		0, 0, 868, 870, 5, 13, 0, 0, 869, 868, 1, 0, 0, 0, 869, 870, 1, 0, 0, 
		0, 870, 871, 1, 0, 0, 0, 871, 872, 5, 94, 0, 0, 872, 111, 1, 0, 0, 0, 
		76, 117, 123, 131, 138, 142, 148, 154, 160, 165, 174, 200, 208, 217, 226, 
		235, 244, 253, 256, 260, 279, 283, 293, 302, 317, 323, 330, 343, 351, 
		353, 374, 389, 466, 468, 507, 514, 519, 529, 571, 576, 581, 583, 585, 
		591, 609, 622, 632, 636, 645, 651, 657, 664, 672, 678, 685, 693, 702, 
		708, 715, 721, 727, 734, 742, 751, 757, 764, 772, 781, 788, 795, 803, 
		805, 819, 831, 838, 846, 869
	];
}
