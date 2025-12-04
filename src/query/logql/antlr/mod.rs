//! ANTLR-based LogQL parser implementation.
//!
//! This module provides an ANTLR-based implementation of the [`Parser`] trait
//! for parsing LogQL query strings into AST representations.
//!
//! # Module Structure
//!
//! - Generated ANTLR code (lexer, parser, visitor traits)
//! - [`visitors`] - AST visitor implementations
//! - [`AntlrParser`] - Main parser implementation

// Allow clippy warnings for ANTLR-generated code
#![allow(missing_docs)]

mod logqllexer;
mod logqlparser;
#[allow(dead_code)]
mod logqlparserbaselistener;
#[allow(dead_code)]
mod logqlparserbasevisitor;
mod logqlparserlistener;
mod logqlparservisitor;
pub(crate) mod visitors;

use std::sync::{Arc, Mutex};

use antlr4rust::{
    common_token_stream::CommonTokenStream, error_listener::ErrorListener, errors::ANTLRError,
    input_stream::InputStream, recognizer::Recognizer, tree::ParseTreeVisitorCompat, Parser as AntlrParserTrait,
};
pub use logqllexer::LogQLLexer;
pub use logqlparser::*;
pub use logqlparservisitor::*;
use visitors::LogQLExprVisitor;

use super::{expr::LogQLExpr, parser::Parser};
use crate::common::{
    errors::{IceGateError, ParseError},
    Result,
};

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
        if let Ok(mut errors) = self.errors.lock() {
            errors.push(error);
        }
    }
}

/// ANTLR-based `LogQL` parser.
///
/// This parser uses ANTLR4 to parse `LogQL` query strings and convert them
/// into the high-level [`LogQLExpr`] AST representation.
///
/// # Examples
///
/// ```
/// use icegate::query::logql::{antlr::AntlrParser, parser::Parser, MatchOp};
///
/// let parser = AntlrParser::new();
/// let expr = parser.parse(r#"{job="mysql"} |= "error""#).unwrap();
/// ```
#[derive(Debug, Default)]
pub struct AntlrParser;

impl AntlrParser {
    /// Creates a new ANTLR parser instance.
    pub const fn new() -> Self {
        Self
    }
}

impl Parser for AntlrParser {
    fn parse(&self, query: &str) -> Result<LogQLExpr> {
        let input = InputStream::new(query);
        let mut lexer = LogQLLexer::new(input);

        // Add error listener to lexer
        let lexer_error_listener = CollectingErrorListener::new();
        lexer.remove_error_listeners();
        lexer.add_error_listener(Box::new(lexer_error_listener.clone()));

        let token_source = CommonTokenStream::new(lexer);
        let mut parser = LogQLParser::new(token_source);

        // Add error listener to parser
        let parser_error_listener = CollectingErrorListener::new();
        parser.remove_error_listeners();
        parser.add_error_listener(Box::new(parser_error_listener.clone()));

        // Parse the query
        let tree = parser.root().map_err(|_| {
            let mut all_errors = lexer_error_listener.get_errors();
            all_errors.extend(parser_error_listener.get_errors());
            IceGateError::Parse(all_errors)
        })?;

        // Collect any errors from listeners
        let mut all_errors = lexer_error_listener.get_errors();
        all_errors.extend(parser_error_listener.get_errors());

        if !all_errors.is_empty() {
            return Err(IceGateError::Parse(all_errors));
        }

        // Convert parse tree to LogQLExpr AST using visitor
        let mut visitor = LogQLExprVisitor::new();
        let result = visitor.visit(&*tree);
        result.into_result()
    }
}

#[cfg(test)]
mod tests;
