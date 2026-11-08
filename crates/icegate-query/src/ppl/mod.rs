//! PPL, the piped processing language of `/_plugins/_ppl`: query text to an AST to one DataFusion
//! plan over the tenant's tables.
//!
//! [`parse_ppl`] builds the AST from the parse tree of the vendored grammar and refuses, by name,
//! every command and function IceGate does not plan; [`plan_ppl`] lowers the AST. `README.md`
//! beside this module states the supported commands and their contracts.

mod ast;
mod build;
mod error;
mod functions;
mod plan;

pub(crate) use build::parse_ppl;
pub(crate) use error::PplError;
pub(crate) use plan::{PplColumn, PplType, plan_ppl};
