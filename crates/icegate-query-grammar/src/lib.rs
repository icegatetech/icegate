//! Generated ANTLR parsers of IceGate's query languages.
//!
//! Each language module exposes one entry point per start rule: it takes the
//! query text, runs the generated lexer and parser, and stops at the first
//! syntax error. A module may also expose lexer-only measures of the text, such
//! as the group depth its parser recurses through. Turning a parse tree into a
//! query is the caller's job; this crate holds no query semantics. The
//! grammars, their origins and the regeneration recipe are described in the
//! crate README.

pub mod lucene;

/// The first error that stopped a parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("line {line}:{column} {message}")]
pub struct SyntaxError {
    /// Line of the offending input, counted from 1.
    pub line: isize,
    /// Column of the offending input, counted in characters from 0.
    pub column: isize,
    /// The offending character or token; `None` when the text ended too early or the
    /// reported position names no character of the text.
    pub offending_text: Option<String>,
    /// What went wrong, in this crate's wording.
    pub message: String,
}
