//! The `OpenSearch` Query DSL.
//!
//! [`parse::parse_query`] turns a request's `query` JSON into the [`ast::Query`] tree; every
//! evaluator works on that tree, so a request means the same thing on system and data indices.
//! [`date_math`] resolves date bounds against the request's `now`. [`analyzer`] splits and lowercases
//! text for full-text matching.

/// The standard analyzer: tokenizer and lowercasing shared by every full-text evaluator.
pub(crate) mod analyzer;
pub(crate) mod ast;
pub(crate) mod date_math;
pub(crate) mod parse;
