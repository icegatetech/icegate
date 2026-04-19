//! ANTLR-based TraceQL parser implementation.

#![allow(missing_docs)]

mod traceqllexer;
mod traceqlparser;
#[allow(dead_code)]
mod traceqlparserbaselistener;
#[allow(dead_code)]
mod traceqlparserbasevisitor;
mod traceqlparserlistener;
mod traceqlparservisitor;
pub(crate) mod visitors;

use std::sync::{Arc, Mutex};

use antlr4rust::{
    Parser as AntlrParserTrait, common_token_stream::CommonTokenStream, error_listener::ErrorListener,
    errors::ANTLRError, input_stream::InputStream, recognizer::Recognizer, tree::ParseTreeVisitorCompat,
};
pub use traceqllexer::TraceQLLexer;
pub use traceqlparser::*;
pub use traceqlparservisitor::*;
use visitors::TraceQLExprVisitor;

use super::{expr::TraceQLExpr, parser::Parser};
use crate::error::{ParseError, QueryError, Result};

/// Error listener that collects parsing errors.
#[derive(Debug, Clone)]
struct CollectingErrorListener {
    errors: Arc<Mutex<Vec<ParseError>>>,
}

impl CollectingErrorListener {
    fn new() -> Self {
        Self {
            errors: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_errors(&self) -> Vec<ParseError> {
        self.errors.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }
}

impl<'a, T: Recognizer<'a>> ErrorListener<'a, T> for CollectingErrorListener {
    fn syntax_error(
        &self,
        _recognizer: &T,
        _offending_symbol: Option<&<T::TF as antlr4rust::token_factory::TokenFactory<'a>>::Inner>,
        line: isize,
        column: isize,
        msg: &str,
        e: Option<&ANTLRError>,
    ) {
        let error = ParseError {
            line,
            column,
            message: msg.to_string(),
            antlr_error: e.cloned(),
        };
        self.errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(error);
    }
}

/// ANTLR-based `TraceQL` parser.
#[derive(Debug, Default)]
pub struct AntlrParser;

impl AntlrParser {
    /// Create a new ANTLR parser instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Parser for AntlrParser {
    #[tracing::instrument(skip(self), fields(query))]
    fn parse(&self, query: &str) -> Result<TraceQLExpr> {
        let input = InputStream::new(query);
        let mut lexer = TraceQLLexer::new(input);

        let lexer_error_listener = CollectingErrorListener::new();
        lexer.remove_error_listeners();
        lexer.add_error_listener(Box::new(lexer_error_listener.clone()));

        let token_source = CommonTokenStream::new(lexer);
        let mut parser = TraceQLParser::new(token_source);

        let parser_error_listener = CollectingErrorListener::new();
        parser.remove_error_listeners();
        parser.add_error_listener(Box::new(parser_error_listener.clone()));

        let tree = parser.root().map_err(|_| {
            let mut all_errors = lexer_error_listener.get_errors();
            all_errors.extend(parser_error_listener.get_errors());
            QueryError::Parse(all_errors)
        })?;

        let mut all_errors = lexer_error_listener.get_errors();
        all_errors.extend(parser_error_listener.get_errors());
        if !all_errors.is_empty() {
            return Err(QueryError::Parse(all_errors));
        }

        let mut visitor = TraceQLExprVisitor::new();
        // `ParseTreeVisitorCompat::visit` returns the captured `Self::Return`
        // and resets `temp_result` to `Default::default()` via `mem::take`.
        // We must consume the value it returns directly rather than relying
        // on the visitor's stored state.
        visitor.visit(&*tree).into_result()
    }
}

#[cfg(test)]
mod tests;
