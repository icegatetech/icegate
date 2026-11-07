//! The `OpenSearch` Query DSL.
//!
//! [`parse::parse_query`] turns a request's `query` JSON into the [`ast::Query`] tree; every
//! evaluator works on that tree, so a request means the same thing on system and data indices.
//! [`date_math`] resolves date bounds against the request's `now`. [`analyzer`] splits and lowercases
//! text for full-text matching. [`simple_query`] parses the text of a `simple_query_string`.
//! [`literal`] reads the typed values (numbers, booleans, dates) of term-level leaves.
//! [`lower`] lowers the tree to a DataFusion predicate over one data index, once per index.
//! [`full_text`] resolves the full-text leaves against one index's fields into plain leaves, and
//! `udf` holds the per-token scalar functions the lowering matches text columns with. [`lucene`]
//! parses a `query_string` leaf and translates its Lucene query string into the full-text tree.

/// The standard analyzer: tokenizer and lowercasing shared by every full-text evaluator.
pub(crate) mod analyzer;
pub(crate) mod ast;
pub(crate) mod date_math;
pub(crate) mod full_text;
pub(crate) mod literal;
pub(crate) mod lower;
pub(crate) mod lucene;
pub(crate) mod parse;
/// The `simple_query_string` syntax.
pub(crate) mod simple_query;
mod udf;
