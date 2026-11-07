//! The Lucene classic query-string parser (generated) and its parse entry point.

// Generated modules carry no docs; the hand-written items below are documented regardless.
#![allow(missing_docs)]

mod lucenelexer;
mod luceneparser;
#[allow(dead_code)]
mod luceneparserbaselistener;
#[allow(dead_code)]
mod luceneparserbasevisitor;
mod luceneparserlistener;
mod luceneparservisitor;

use std::{cell::Cell, rc::Rc};

use antlr4rust::{
    Parser as _,
    common_token_stream::CommonTokenStream,
    error_listener::ErrorListener,
    error_strategy::BailErrorStrategy,
    errors::ANTLRError,
    input_stream::InputStream,
    recognizer::Recognizer,
    token::{TOKEN_EOF, Token as _},
    token_factory::TokenFactory,
};
use lucenelexer::LuceneLexer;
use luceneparser::LuceneParser;
pub use luceneparser::{
    BareOperatorTermContext, BareOperatorTermContextAttrs, BoostContextAll, BoostContextAttrs, ClauseContextAll,
    ClauseContextAttrs, ConjoinedClauseContextAll, ConjoinedClauseContextAttrs, ConjunctionContextAll,
    ConjunctionContextAttrs, FieldNameContextAll, FieldNameContextAttrs, GroupContextAll, GroupContextAttrs,
    LuceneParserContextType, ModClauseContextAll, ModClauseContextAttrs, ModifierContextAll, ModifierContextAttrs,
    PlainTermContext, PlainTermContextAttrs, PrefixTermContext, PrefixTermContextAttrs, QueryContextAll,
    QueryContextAttrs, QuotedTermContext, QuotedTermContextAttrs, RangeBoundContextAll, RangeBoundContextAttrs,
    RangeContextAll, RangeContextAttrs, RangeEndContextAll, RangeEndContextAttrs, RangeStartContextAll,
    RangeStartContextAttrs, RangeTermContext, RangeTermContextAttrs, RegexpTermContext, RegexpTermContextAttrs,
    StarTermContext, StarTermContextAttrs, TermContextAll, TermContextAttrs, TermModifiersContextAll,
    TermModifiersContextAttrs, TopLevelQueryContextAll, TopLevelQueryContextAttrs, WildcardTermContext,
    WildcardTermContextAttrs,
};
pub use luceneparservisitor::{LuceneParserVisitor, LuceneParserVisitorCompat};

use crate::SyntaxError;

/// Records where the first lexer error happened; later ones describe input after it.
struct FirstLexerErrorRecorder {
    position: Rc<Cell<Option<(isize, isize)>>>,
}

impl<'input, T: Recognizer<'input>> ErrorListener<'input, T> for FirstLexerErrorRecorder {
    fn syntax_error(
        &self,
        _recognizer: &T,
        _offending_symbol: Option<&<T::TF as TokenFactory<'input>>::Inner>,
        line: isize,
        column: isize,
        _message: &str,
        _error: Option<&ANTLRError>,
    ) {
        if self.position.get().is_none() {
            self.position.set(Some((line, column)));
        }
    }
}

/// Parses a Lucene classic query string into its parse tree.
///
/// The lexer, the token stream and the parser are built here; the parser bails at
/// its first error, and a character no lexer rule matches is an error too, so a
/// returned tree is the parse of the whole text.
///
/// # Errors
///
/// The first lexer or parser error, with its 1-based line, its 0-based column
/// counted in characters, and the offending character or token (`None` when the
/// text ended too early, or when a lexer error's position names no character of
/// the text).
pub fn parse_lucene_query(text: &str) -> Result<Rc<TopLevelQueryContextAll<'_>>, SyntaxError> {
    let lexer_error = Rc::new(Cell::new(None));
    let mut lexer = LuceneLexer::new(InputStream::new(text));
    // Errors are recorded, never printed: this recorder is the lexer's only listener.
    lexer.remove_error_listeners();
    lexer.add_error_listener(Box::new(FirstLexerErrorRecorder {
        position: Rc::clone(&lexer_error),
    }));
    let mut parser = LuceneParser::with_strategy(CommonTokenStream::new(lexer), Box::new(BailErrorStrategy::new()));
    parser.remove_error_listeners();
    let parse_result = parser.topLevelQuery();
    // A lexer error decides the result even when the tokens around the unmatched
    // character parse: the `a/b` and `a^ 2` rows of the tests pin this.
    if let Some((line, column)) = lexer_error.get() {
        let offending_text = find_character_at(text, line, column).map(str::to_owned);
        // The lexer reports at the start of a token attempt, and an attempt starts only on
        // a character of the text, so the position resolves unless the runtime and
        // `find_character_at` count lines or columns differently; the multibyte and
        // multi-line rows of the tests pin that they agree. A position that resolves to no
        // character is reported as unrecognized input; a counting mismatch that would name
        // a wrong character is caught by those rows, not here.
        let message = offending_text.as_deref().map_or_else(
            || String::from("unrecognized input"),
            |character| format!("unexpected character [{character}]"),
        );
        return Err(SyntaxError {
            line,
            column,
            offending_text,
            message,
        });
    }
    parse_result.map_err(|_| {
        // The parser has no listener; the error is read from its current token, where
        // the bail strategy stopped. The parser-error rows of the tests pin that position.
        let token = parser.get_current_token();
        let line = token.get_line();
        let column = token.get_column();
        if token.get_token_type() == TOKEN_EOF {
            return SyntaxError {
                line,
                column,
                offending_text: None,
                message: String::from("unexpected end of query"),
            };
        }
        let offending_text = token.get_text().to_owned();
        SyntaxError {
            line,
            column,
            message: format!("unexpected [{offending_text}]"),
            offending_text: Some(offending_text),
        }
    })
}

/// The character at a 1-based line and a 0-based column counted in characters.
fn find_character_at(text: &str, line: isize, column: isize) -> Option<&str> {
    let line_index = usize::try_from(line).ok()?.checked_sub(1)?;
    let column_index = usize::try_from(column).ok()?;
    let line_text = text.split('\n').nth(line_index)?;
    let (start, character) = line_text.char_indices().nth(column_index)?;
    line_text.get(start..start + character.len_utf8())
}

#[cfg(test)]
mod tests;
