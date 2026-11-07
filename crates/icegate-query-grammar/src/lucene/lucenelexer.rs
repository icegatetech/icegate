#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::unwrap_used, unused_parens)]
// Generated from LuceneLexer.g4 by ANTLR 4.13.2
#![allow(dead_code)]
#![allow(nonstandard_style)]
#![allow(unused_imports)]
#![allow(unused_variables)]
use antlr4rust::atn::ATN;
use antlr4rust::char_stream::CharStream;
use antlr4rust::int_stream::IntStream;
use antlr4rust::tree::ParseTree;
use antlr4rust::lexer::{BaseLexer, Lexer, LexerRecog};
use antlr4rust::atn_deserializer::ATNDeserializer;
use antlr4rust::dfa::DFA;
use antlr4rust::lexer_atn_simulator::{LexerATNSimulator, ILexerATNSimulator};
use antlr4rust::PredictionContextCache;
use antlr4rust::recognizer::{Recognizer,Actions};
use antlr4rust::error_listener::ErrorListener;
use antlr4rust::TokenSource;
use antlr4rust::token_factory::{TokenFactory,CommonTokenFactory,TokenAware};
use antlr4rust::token::*;
use antlr4rust::rule_context::{BaseRuleContext,EmptyCustomRuleContext,EmptyContext};
use antlr4rust::parser_rule_context::{ParserRuleContext,BaseParserRuleContext,cast};
use antlr4rust::vocabulary::{Vocabulary,VocabularyImpl};

use antlr4rust::{lazy_static,Tid,TidAble,TidExt};

use std::sync::Arc;
use std::cell::RefCell;
use std::rc::Rc;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};


	pub const DEFAULT_SKIP:i32=1; 
	pub const AND:i32=2; 
	pub const OR:i32=3; 
	pub const NOT:i32=4; 
	pub const BAREOPER:i32=5; 
	pub const PLUS:i32=6; 
	pub const MINUS:i32=7; 
	pub const LPAREN:i32=8; 
	pub const RPAREN:i32=9; 
	pub const COLON:i32=10; 
	pub const STAR:i32=11; 
	pub const CARAT:i32=12; 
	pub const QUOTED:i32=13; 
	pub const TERM:i32=14; 
	pub const FUZZY_SLOP:i32=15; 
	pub const PREFIXTERM:i32=16; 
	pub const WILDTERM:i32=17; 
	pub const REGEXPTERM:i32=18; 
	pub const RANGEIN_START:i32=19; 
	pub const RANGEEX_START:i32=20; 
	pub const NUMBER:i32=21; 
	pub const RANGE_SKIP:i32=22; 
	pub const RANGE_TO:i32=23; 
	pub const RANGEIN_END:i32=24; 
	pub const RANGEEX_END:i32=25; 
	pub const RANGE_QUOTED:i32=26; 
	pub const RANGE_GOOP:i32=27;
	pub const channelNames: [&'static str;0+2] = [
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	];

	pub const modeNames: [&'static str;3] = [
		"DEFAULT_MODE", "BOOST", "RANGE"
	];

	pub const ruleNames: [&'static str;33] = [
		"DEFAULT_SKIP", "AND", "OR", "NOT", "BAREOPER", "PLUS", "MINUS", "LPAREN", 
		"RPAREN", "COLON", "STAR", "CARAT", "QUOTED", "TERM", "FUZZY_SLOP", "PREFIXTERM", 
		"WILDTERM", "REGEXPTERM", "RANGEIN_START", "RANGEEX_START", "NUM_CHAR", 
		"ESCAPED_CHAR", "TERM_START_CHAR", "TERM_CHAR", "WHITESPACE", "QUOTED_CHAR", 
		"NUMBER", "RANGE_SKIP", "RANGE_TO", "RANGEIN_END", "RANGEEX_END", "RANGE_QUOTED", 
		"RANGE_GOOP"
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


pub type LexerContext<'input> = BaseRuleContext<'input,EmptyCustomRuleContext<'input,LocalTokenFactory<'input> >>;
pub type LocalTokenFactory<'input> = CommonTokenFactory;

type From<'a> = <LocalTokenFactory<'a> as TokenFactory<'a> >::From;

pub struct LuceneLexer<'input, Input:CharStream<From<'input> >> {
	base: BaseLexer<'input,LuceneLexerActions,Input,LocalTokenFactory<'input>>,
}

antlr4rust::tid! { impl<'input,Input> TidAble<'input> for LuceneLexer<'input,Input> where Input:CharStream<From<'input> > }

impl<'input, Input:CharStream<From<'input> >> Deref for LuceneLexer<'input,Input>{
	type Target = BaseLexer<'input,LuceneLexerActions,Input,LocalTokenFactory<'input>>;

	fn deref(&self) -> &Self::Target {
		&self.base
	}
}

impl<'input, Input:CharStream<From<'input> >> DerefMut for LuceneLexer<'input,Input>{
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.base
	}
}


impl<'input, Input:CharStream<From<'input> >> LuceneLexer<'input,Input>{
    fn get_rule_names(&self) -> &'static [&'static str] {
        &ruleNames
    }
    fn get_literal_names(&self) -> &[Option<&str>] {
        &_LITERAL_NAMES
    }

    fn get_symbolic_names(&self) -> &[Option<&str>] {
        &_SYMBOLIC_NAMES
    }

    fn get_grammar_file_name(&self) -> &'static str {
        "LuceneLexer.g4"
    }

	pub fn new_with_token_factory(input: Input, tf: &'input LocalTokenFactory<'input>) -> Self {
		antlr4rust::recognizer::check_version("0","5");
    	Self {
			base: BaseLexer::new_base_lexer(
				input,
				LexerATNSimulator::new_lexer_atnsimulator(
					_ATN.clone(),
					_decision_to_DFA.clone(),
					_shared_context_cache.clone(),
				),
				LuceneLexerActions{},
				tf
			)
	    }
	}
}

impl<'input, Input:CharStream<From<'input> >> LuceneLexer<'input,Input> where &'input LocalTokenFactory<'input>:Default{
	pub fn new(input: Input) -> Self{
		LuceneLexer::new_with_token_factory(input, <&LocalTokenFactory<'input> as Default>::default())
	}
}

pub struct LuceneLexerActions {
}

impl LuceneLexerActions{
}

impl<'input, Input:CharStream<From<'input> >> Actions<'input,BaseLexer<'input,LuceneLexerActions,Input,LocalTokenFactory<'input>>> for LuceneLexerActions{
	}

	impl<'input, Input:CharStream<From<'input> >> LuceneLexer<'input,Input>{

}

impl<'input, Input:CharStream<From<'input> >> LexerRecog<'input,BaseLexer<'input,LuceneLexerActions,Input,LocalTokenFactory<'input>>> for LuceneLexerActions{
}
impl<'input> TokenAware<'input> for LuceneLexerActions{
	type TF = LocalTokenFactory<'input>;
}

impl<'input, Input:CharStream<From<'input> >> TokenSource<'input> for LuceneLexer<'input,Input>{
	type TF = LocalTokenFactory<'input>;

    fn next_token(&mut self) -> <Self::TF as TokenFactory<'input>>::Tok {
        self.base.next_token()
    }

    fn get_line(&self) -> isize {
        self.base.get_line()
    }

    fn get_char_position_in_line(&self) -> isize {
        self.base.get_char_position_in_line()
    }

    fn get_input_stream(&mut self) -> Option<&mut dyn IntStream> {
        self.base.get_input_stream()
    }

	fn get_source_name(&self) -> String {
		self.base.get_source_name()
	}

    fn get_token_factory(&self) -> &'input Self::TF {
        self.base.get_token_factory()
    }

    fn get_dfa_string(&self) -> String {
        self.base.get_dfa_string()
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
			4, 0, 27, 258, 6, -1, 6, -1, 6, -1, 2, 0, 7, 0, 2, 1, 7, 1, 2, 2, 7, 
			2, 2, 3, 7, 3, 2, 4, 7, 4, 2, 5, 7, 5, 2, 6, 7, 6, 2, 7, 7, 7, 2, 8, 
			7, 8, 2, 9, 7, 9, 2, 10, 7, 10, 2, 11, 7, 11, 2, 12, 7, 12, 2, 13, 7, 
			13, 2, 14, 7, 14, 2, 15, 7, 15, 2, 16, 7, 16, 2, 17, 7, 17, 2, 18, 7, 
			18, 2, 19, 7, 19, 2, 20, 7, 20, 2, 21, 7, 21, 2, 22, 7, 22, 2, 23, 7, 
			23, 2, 24, 7, 24, 2, 25, 7, 25, 2, 26, 7, 26, 2, 27, 7, 27, 2, 28, 7, 
			28, 2, 29, 7, 29, 2, 30, 7, 30, 2, 31, 7, 31, 2, 32, 7, 32, 1, 0, 1, 
			0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 79, 8, 1, 1, 2, 1, 
			2, 1, 2, 1, 2, 3, 2, 85, 8, 2, 1, 3, 1, 3, 1, 3, 1, 3, 3, 3, 91, 8, 3, 
			1, 4, 1, 4, 1, 4, 1, 5, 1, 5, 1, 6, 1, 6, 1, 7, 1, 7, 1, 8, 1, 8, 1, 
			9, 1, 9, 1, 10, 1, 10, 1, 11, 1, 11, 1, 11, 1, 11, 1, 12, 1, 12, 5, 12, 
			114, 8, 12, 10, 12, 12, 12, 117, 9, 12, 1, 12, 1, 12, 1, 13, 1, 13, 5, 
			13, 123, 8, 13, 10, 13, 12, 13, 126, 9, 13, 1, 14, 1, 14, 4, 14, 130, 
			8, 14, 11, 14, 12, 14, 131, 1, 14, 1, 14, 4, 14, 136, 8, 14, 11, 14, 
			12, 14, 137, 3, 14, 140, 8, 14, 1, 14, 5, 14, 143, 8, 14, 10, 14, 12, 
			14, 146, 9, 14, 1, 14, 5, 14, 149, 8, 14, 10, 14, 12, 14, 152, 9, 14, 
			3, 14, 154, 8, 14, 1, 15, 1, 15, 5, 15, 158, 8, 15, 10, 15, 12, 15, 161, 
			9, 15, 1, 15, 1, 15, 1, 16, 1, 16, 3, 16, 167, 8, 16, 1, 16, 1, 16, 5, 
			16, 171, 8, 16, 10, 16, 12, 16, 174, 9, 16, 1, 17, 1, 17, 1, 17, 1, 17, 
			5, 17, 180, 8, 17, 10, 17, 12, 17, 183, 9, 17, 1, 17, 1, 17, 1, 18, 1, 
			18, 1, 18, 1, 18, 1, 19, 1, 19, 1, 19, 1, 19, 1, 20, 1, 20, 1, 21, 1, 
			21, 1, 21, 1, 22, 1, 22, 3, 22, 202, 8, 22, 1, 23, 1, 23, 3, 23, 206, 
			8, 23, 1, 24, 1, 24, 1, 25, 1, 25, 3, 25, 212, 8, 25, 1, 26, 4, 26, 215, 
			8, 26, 11, 26, 12, 26, 216, 1, 26, 1, 26, 4, 26, 221, 8, 26, 11, 26, 
			12, 26, 222, 3, 26, 225, 8, 26, 1, 26, 1, 26, 1, 27, 1, 27, 1, 27, 1, 
			27, 1, 28, 1, 28, 1, 28, 1, 29, 1, 29, 1, 29, 1, 29, 1, 30, 1, 30, 1, 
			30, 1, 30, 1, 31, 1, 31, 1, 31, 1, 31, 4, 31, 248, 8, 31, 11, 31, 12, 
			31, 249, 1, 31, 1, 31, 1, 32, 4, 32, 255, 8, 32, 11, 32, 12, 32, 256, 
			0, 0, 33, 3, 1, 5, 2, 7, 3, 9, 4, 11, 5, 13, 6, 15, 7, 17, 8, 19, 9, 
			21, 10, 23, 11, 25, 12, 27, 13, 29, 14, 31, 15, 33, 16, 35, 17, 37, 18, 
			39, 19, 41, 20, 43, 0, 45, 0, 47, 0, 49, 0, 51, 0, 53, 0, 55, 21, 57, 
			22, 59, 23, 61, 24, 63, 25, 65, 26, 67, 27, 3, 0, 1, 2, 10, 3, 0, 33, 
			33, 43, 43, 45, 45, 2, 0, 42, 42, 63, 63, 1, 0, 47, 47, 1, 0, 48, 57, 
			12, 0, 9, 10, 13, 13, 32, 34, 40, 43, 45, 45, 47, 47, 58, 58, 63, 63, 
			91, 94, 123, 123, 125, 126, 12288, 12288, 2, 0, 43, 43, 45, 45, 4, 0, 
			9, 10, 13, 13, 32, 32, 12288, 12288, 2, 0, 34, 34, 92, 92, 1, 0, 34, 
			34, 3, 0, 32, 32, 93, 93, 125, 125, 275, 0, 3, 1, 0, 0, 0, 0, 5, 1, 0, 
			0, 0, 0, 7, 1, 0, 0, 0, 0, 9, 1, 0, 0, 0, 0, 11, 1, 0, 0, 0, 0, 13, 1, 
			0, 0, 0, 0, 15, 1, 0, 0, 0, 0, 17, 1, 0, 0, 0, 0, 19, 1, 0, 0, 0, 0, 
			21, 1, 0, 0, 0, 0, 23, 1, 0, 0, 0, 0, 25, 1, 0, 0, 0, 0, 27, 1, 0, 0, 
			0, 0, 29, 1, 0, 0, 0, 0, 31, 1, 0, 0, 0, 0, 33, 1, 0, 0, 0, 0, 35, 1, 
			0, 0, 0, 0, 37, 1, 0, 0, 0, 0, 39, 1, 0, 0, 0, 0, 41, 1, 0, 0, 0, 1, 
			55, 1, 0, 0, 0, 2, 57, 1, 0, 0, 0, 2, 59, 1, 0, 0, 0, 2, 61, 1, 0, 0, 
			0, 2, 63, 1, 0, 0, 0, 2, 65, 1, 0, 0, 0, 2, 67, 1, 0, 0, 0, 3, 69, 1, 
			0, 0, 0, 5, 78, 1, 0, 0, 0, 7, 84, 1, 0, 0, 0, 9, 90, 1, 0, 0, 0, 11, 
			92, 1, 0, 0, 0, 13, 95, 1, 0, 0, 0, 15, 97, 1, 0, 0, 0, 17, 99, 1, 0, 
			0, 0, 19, 101, 1, 0, 0, 0, 21, 103, 1, 0, 0, 0, 23, 105, 1, 0, 0, 0, 
			25, 107, 1, 0, 0, 0, 27, 111, 1, 0, 0, 0, 29, 120, 1, 0, 0, 0, 31, 127, 
			1, 0, 0, 0, 33, 155, 1, 0, 0, 0, 35, 166, 1, 0, 0, 0, 37, 175, 1, 0, 
			0, 0, 39, 186, 1, 0, 0, 0, 41, 190, 1, 0, 0, 0, 43, 194, 1, 0, 0, 0, 
			45, 196, 1, 0, 0, 0, 47, 201, 1, 0, 0, 0, 49, 205, 1, 0, 0, 0, 51, 207, 
			1, 0, 0, 0, 53, 211, 1, 0, 0, 0, 55, 214, 1, 0, 0, 0, 57, 228, 1, 0, 
			0, 0, 59, 232, 1, 0, 0, 0, 61, 235, 1, 0, 0, 0, 63, 239, 1, 0, 0, 0, 
			65, 243, 1, 0, 0, 0, 67, 254, 1, 0, 0, 0, 69, 70, 3, 51, 24, 0, 70, 71, 
			1, 0, 0, 0, 71, 72, 6, 0, 0, 0, 72, 4, 1, 0, 0, 0, 73, 74, 5, 65, 0, 
			0, 74, 75, 5, 78, 0, 0, 75, 79, 5, 68, 0, 0, 76, 77, 5, 38, 0, 0, 77, 
			79, 5, 38, 0, 0, 78, 73, 1, 0, 0, 0, 78, 76, 1, 0, 0, 0, 79, 6, 1, 0, 
			0, 0, 80, 81, 5, 79, 0, 0, 81, 85, 5, 82, 0, 0, 82, 83, 5, 124, 0, 0, 
			83, 85, 5, 124, 0, 0, 84, 80, 1, 0, 0, 0, 84, 82, 1, 0, 0, 0, 85, 8, 
			1, 0, 0, 0, 86, 87, 5, 78, 0, 0, 87, 88, 5, 79, 0, 0, 88, 91, 5, 84, 
			0, 0, 89, 91, 5, 33, 0, 0, 90, 86, 1, 0, 0, 0, 90, 89, 1, 0, 0, 0, 91, 
			10, 1, 0, 0, 0, 92, 93, 7, 0, 0, 0, 93, 94, 3, 51, 24, 0, 94, 12, 1, 
			0, 0, 0, 95, 96, 5, 43, 0, 0, 96, 14, 1, 0, 0, 0, 97, 98, 5, 45, 0, 0, 
			98, 16, 1, 0, 0, 0, 99, 100, 5, 40, 0, 0, 100, 18, 1, 0, 0, 0, 101, 102, 
			5, 41, 0, 0, 102, 20, 1, 0, 0, 0, 103, 104, 5, 58, 0, 0, 104, 22, 1, 
			0, 0, 0, 105, 106, 5, 42, 0, 0, 106, 24, 1, 0, 0, 0, 107, 108, 5, 94, 
			0, 0, 108, 109, 1, 0, 0, 0, 109, 110, 6, 11, 1, 0, 110, 26, 1, 0, 0, 
			0, 111, 115, 5, 34, 0, 0, 112, 114, 3, 53, 25, 0, 113, 112, 1, 0, 0, 
			0, 114, 117, 1, 0, 0, 0, 115, 113, 1, 0, 0, 0, 115, 116, 1, 0, 0, 0, 
			116, 118, 1, 0, 0, 0, 117, 115, 1, 0, 0, 0, 118, 119, 5, 34, 0, 0, 119, 
			28, 1, 0, 0, 0, 120, 124, 3, 47, 22, 0, 121, 123, 3, 49, 23, 0, 122, 
			121, 1, 0, 0, 0, 123, 126, 1, 0, 0, 0, 124, 122, 1, 0, 0, 0, 124, 125, 
			1, 0, 0, 0, 125, 30, 1, 0, 0, 0, 126, 124, 1, 0, 0, 0, 127, 153, 5, 126, 
			0, 0, 128, 130, 3, 43, 20, 0, 129, 128, 1, 0, 0, 0, 130, 131, 1, 0, 0, 
			0, 131, 129, 1, 0, 0, 0, 131, 132, 1, 0, 0, 0, 132, 139, 1, 0, 0, 0, 
			133, 135, 5, 46, 0, 0, 134, 136, 3, 43, 20, 0, 135, 134, 1, 0, 0, 0, 
			136, 137, 1, 0, 0, 0, 137, 135, 1, 0, 0, 0, 137, 138, 1, 0, 0, 0, 138, 
			140, 1, 0, 0, 0, 139, 133, 1, 0, 0, 0, 139, 140, 1, 0, 0, 0, 140, 144, 
			1, 0, 0, 0, 141, 143, 3, 49, 23, 0, 142, 141, 1, 0, 0, 0, 143, 146, 1, 
			0, 0, 0, 144, 142, 1, 0, 0, 0, 144, 145, 1, 0, 0, 0, 145, 154, 1, 0, 
			0, 0, 146, 144, 1, 0, 0, 0, 147, 149, 3, 49, 23, 0, 148, 147, 1, 0, 0, 
			0, 149, 152, 1, 0, 0, 0, 150, 148, 1, 0, 0, 0, 150, 151, 1, 0, 0, 0, 
			151, 154, 1, 0, 0, 0, 152, 150, 1, 0, 0, 0, 153, 129, 1, 0, 0, 0, 153, 
			150, 1, 0, 0, 0, 154, 32, 1, 0, 0, 0, 155, 159, 3, 47, 22, 0, 156, 158, 
			3, 49, 23, 0, 157, 156, 1, 0, 0, 0, 158, 161, 1, 0, 0, 0, 159, 157, 1, 
			0, 0, 0, 159, 160, 1, 0, 0, 0, 160, 162, 1, 0, 0, 0, 161, 159, 1, 0, 
			0, 0, 162, 163, 5, 42, 0, 0, 163, 34, 1, 0, 0, 0, 164, 167, 3, 47, 22, 
			0, 165, 167, 7, 1, 0, 0, 166, 164, 1, 0, 0, 0, 166, 165, 1, 0, 0, 0, 
			167, 172, 1, 0, 0, 0, 168, 171, 3, 49, 23, 0, 169, 171, 7, 1, 0, 0, 170, 
			168, 1, 0, 0, 0, 170, 169, 1, 0, 0, 0, 171, 174, 1, 0, 0, 0, 172, 170, 
			1, 0, 0, 0, 172, 173, 1, 0, 0, 0, 173, 36, 1, 0, 0, 0, 174, 172, 1, 0, 
			0, 0, 175, 181, 5, 47, 0, 0, 176, 180, 8, 2, 0, 0, 177, 178, 5, 92, 0, 
			0, 178, 180, 5, 47, 0, 0, 179, 176, 1, 0, 0, 0, 179, 177, 1, 0, 0, 0, 
			180, 183, 1, 0, 0, 0, 181, 179, 1, 0, 0, 0, 181, 182, 1, 0, 0, 0, 182, 
			184, 1, 0, 0, 0, 183, 181, 1, 0, 0, 0, 184, 185, 5, 47, 0, 0, 185, 38, 
			1, 0, 0, 0, 186, 187, 5, 91, 0, 0, 187, 188, 1, 0, 0, 0, 188, 189, 6, 
			18, 2, 0, 189, 40, 1, 0, 0, 0, 190, 191, 5, 123, 0, 0, 191, 192, 1, 0, 
			0, 0, 192, 193, 6, 19, 2, 0, 193, 42, 1, 0, 0, 0, 194, 195, 7, 3, 0, 
			0, 195, 44, 1, 0, 0, 0, 196, 197, 5, 92, 0, 0, 197, 198, 9, 0, 0, 0, 
			198, 46, 1, 0, 0, 0, 199, 202, 8, 4, 0, 0, 200, 202, 3, 45, 21, 0, 201, 
			199, 1, 0, 0, 0, 201, 200, 1, 0, 0, 0, 202, 48, 1, 0, 0, 0, 203, 206, 
			3, 47, 22, 0, 204, 206, 7, 5, 0, 0, 205, 203, 1, 0, 0, 0, 205, 204, 1, 
			0, 0, 0, 206, 50, 1, 0, 0, 0, 207, 208, 7, 6, 0, 0, 208, 52, 1, 0, 0, 
			0, 209, 212, 8, 7, 0, 0, 210, 212, 3, 45, 21, 0, 211, 209, 1, 0, 0, 0, 
			211, 210, 1, 0, 0, 0, 212, 54, 1, 0, 0, 0, 213, 215, 3, 43, 20, 0, 214, 
			213, 1, 0, 0, 0, 215, 216, 1, 0, 0, 0, 216, 214, 1, 0, 0, 0, 216, 217, 
			1, 0, 0, 0, 217, 224, 1, 0, 0, 0, 218, 220, 5, 46, 0, 0, 219, 221, 3, 
			43, 20, 0, 220, 219, 1, 0, 0, 0, 221, 222, 1, 0, 0, 0, 222, 220, 1, 0, 
			0, 0, 222, 223, 1, 0, 0, 0, 223, 225, 1, 0, 0, 0, 224, 218, 1, 0, 0, 
			0, 224, 225, 1, 0, 0, 0, 225, 226, 1, 0, 0, 0, 226, 227, 6, 26, 3, 0, 
			227, 56, 1, 0, 0, 0, 228, 229, 3, 51, 24, 0, 229, 230, 1, 0, 0, 0, 230, 
			231, 6, 27, 0, 0, 231, 58, 1, 0, 0, 0, 232, 233, 5, 84, 0, 0, 233, 234, 
			5, 79, 0, 0, 234, 60, 1, 0, 0, 0, 235, 236, 5, 93, 0, 0, 236, 237, 1, 
			0, 0, 0, 237, 238, 6, 29, 3, 0, 238, 62, 1, 0, 0, 0, 239, 240, 5, 125, 
			0, 0, 240, 241, 1, 0, 0, 0, 241, 242, 6, 30, 3, 0, 242, 64, 1, 0, 0, 
			0, 243, 247, 5, 34, 0, 0, 244, 248, 8, 8, 0, 0, 245, 246, 5, 92, 0, 0, 
			246, 248, 5, 34, 0, 0, 247, 244, 1, 0, 0, 0, 247, 245, 1, 0, 0, 0, 248, 
			249, 1, 0, 0, 0, 249, 247, 1, 0, 0, 0, 249, 250, 1, 0, 0, 0, 250, 251, 
			1, 0, 0, 0, 251, 252, 5, 34, 0, 0, 252, 66, 1, 0, 0, 0, 253, 255, 8, 
			9, 0, 0, 254, 253, 1, 0, 0, 0, 255, 256, 1, 0, 0, 0, 256, 254, 1, 0, 
			0, 0, 256, 257, 1, 0, 0, 0, 257, 68, 1, 0, 0, 0, 29, 0, 1, 2, 78, 84, 
			90, 115, 124, 131, 137, 139, 144, 150, 153, 159, 166, 170, 172, 179, 
			181, 201, 205, 211, 216, 222, 224, 247, 249, 256, 4, 6, 0, 0, 2, 1, 0, 
			2, 2, 0, 2, 0, 0
		];
	}
