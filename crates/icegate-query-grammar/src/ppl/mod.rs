//! The PPL parser generated from the vendored `OpenSearch` PPL grammar, and its entry point.
//!
//! The generated modules carry no documentation of their own, so this module allows
//! `missing_docs`; the items written by hand below are documented all the same.
#![allow(missing_docs)]

mod char_stream;
mod opensearchppllexer;
mod opensearchpplparser;
#[allow(dead_code)]
mod opensearchpplparserbaselistener;
#[allow(dead_code)]
mod opensearchpplparserbasevisitor;
mod opensearchpplparserlistener;
mod opensearchpplparservisitor;

use std::rc::Rc;

use antlr4rust::Parser as _;
use antlr4rust::common_token_stream::CommonTokenStream;
use antlr4rust::error_strategy::BailErrorStrategy;
use antlr4rust::errors::ANTLRError;
use antlr4rust::int_stream::IntStream;
use antlr4rust::token::{OwningToken, TOKEN_EOF, Token};
use antlr4rust::token_stream::TokenStream;
pub use opensearchppllexer::OpenSearchPPLLexer;
pub use opensearchpplparser::*;
pub use opensearchpplparservisitor::*;

use self::char_stream::UpperCaseCharStream;
use crate::SyntaxError;

/// The serialized lexer ATN, rule, literal, symbolic, channel and mode names of the vendored
/// grammar, exactly as the generator wrote them into `OpenSearchPPLLexer.interp`.
pub const PPL_LEXER_INTERP: &str = include_str!("OpenSearchPPLLexer.interp");

/// The serialized parser ATN, rule, literal and symbolic names of the vendored grammar, exactly
/// as the generator wrote them into `OpenSearchPPLParser.interp`.
pub const PPL_PARSER_INTERP: &str = include_str!("OpenSearchPPLParser.interp");

/// The token stream the PPL parser reads.
type PplTokenStream<'input> = CommonTokenStream<'input, OpenSearchPPLLexer<'input, UpperCaseCharStream<'input>>>;

/// Parse `text` with the `root` rule of the vendored PPL grammar.
///
/// Keywords match in any case: the lexer reads the simple upper case of each code point, while
/// token text keeps the characters as written. Parsing stops at the first error, which names the
/// offending token and its 1-based line and 0-based column.
///
/// # Errors
///
/// Returns [`SyntaxError`] for whichever comes first in the text: the parser's first error, or a
/// character that is no token of the grammar (the lexer emits it as an `ERROR_RECOGNITION` token)
/// and is not whitespace. The grammar has no whitespace rule, so whitespace between tokens also
/// reaches the lexer as such a token; Unicode `White_Space` characters are skipped. The error
/// always names a token: `offending_text` is `Some`, and the end of the query is `Some("<EOF>")`.
pub fn parse_ppl_root(text: &str) -> Result<Rc<RootContextAll<'_>>, SyntaxError> {
    let mut parser = build_ppl_parser(text);
    let outcome = parser.root();
    let unrecognized_character_error = find_first_unrecognized_character(&parser.input);
    match outcome {
        Ok(root) => unrecognized_character_error.map_or(Ok(root), Err),
        Err(error) => {
            // Every recognition error the bail strategy wraps carries its offending token. An
            // error that carries none (an illegal-state or user error of the runtime, which no
            // query of this grammar raises) is placed at the token the parser stopped on, so the
            // error always has a real position to report and to order against a character error.
            let parse_error = find_offending_token(&error).map_or_else(
                || describe_offending_token(&**parser.get_current_token()),
                describe_offending_token,
            );
            Err(match unrecognized_character_error {
                Some(character_error) if is_before(&character_error, &parse_error) => character_error,
                _ => parse_error,
            })
        }
    }
}

/// The parser [`parse_ppl_root`] runs over `text`: case-insensitive look-ahead, the bail error
/// strategy, and no error listeners, so the first error ends the parse and nothing is printed.
fn build_ppl_parser(text: &str) -> OpenSearchPPLParser<'_, PplTokenStream<'_>> {
    let mut lexer = OpenSearchPPLLexer::new(UpperCaseCharStream::new(text));
    lexer.remove_error_listeners();
    let mut parser =
        OpenSearchPPLParser::with_strategy(CommonTokenStream::new(lexer), Box::new(BailErrorStrategy::new()));
    parser.remove_error_listeners();
    parser
}

/// The first non-whitespace `ERROR_RECOGNITION` token among the tokens the parser fetched.
///
/// When the parse failed, every token up to the offending one has been fetched, so a character
/// error before the parser error is never missed.
fn find_first_unrecognized_character(tokens: &PplTokenStream<'_>) -> Option<SyntaxError> {
    (0..tokens.size()).find_map(|index| {
        let token = tokens.get(index);
        if token.get_token_type() != opensearchppllexer::ERROR_RECOGNITION {
            return None;
        }
        let text = token.get_text();
        (!text.chars().all(char::is_whitespace)).then(|| SyntaxError {
            line: token.get_line(),
            column: token.get_column(),
            message: format!("unrecognized character '{text}'"),
            offending_text: Some(text.to_string()),
        })
    })
}

/// The offending token of the recognition error that stopped a bailed-out parse. The bail
/// strategy wraps that error in `FallThrough`; `None` when the error carries no token.
fn find_offending_token(error: &ANTLRError) -> Option<&OwningToken> {
    let cause = match error {
        ANTLRError::FallThrough(inner) => inner.source().and_then(|source| source.downcast_ref::<ANTLRError>()),
        other => Some(other),
    };
    cause.and_then(ANTLRError::get_offending_token)
}

/// The error naming `token`; the end of input is named `<EOF>`.
fn describe_offending_token<T: Token<Data = str> + ?Sized>(token: &T) -> SyntaxError {
    let text = if token.get_token_type() == TOKEN_EOF {
        "<EOF>".to_string()
    } else {
        token.get_text().to_string()
    };
    SyntaxError {
        line: token.get_line(),
        column: token.get_column(),
        message: format!("unexpected '{text}'"),
        offending_text: Some(text),
    }
}

/// Whether `first` lies before `second` in the query text.
fn is_before(first: &SyntaxError, second: &SyntaxError) -> bool {
    (first.line, first.column) < (second.line, second.column)
}

#[cfg(test)]
mod tests;
