//! The PPL endpoints of the listener: `POST /_plugins/_ppl`, answered in the `jdbc`, `viz`,
//! `csv` or `raw` format, `POST /_plugins/_ppl/_explain`, and the PPL error envelope; beside them,
//! `POST /_plugins/_sql` (only `SHOW TABLES LIKE`) and the grammar bundle at
//! `GET /_plugins/_ppl/_grammar`.

mod error;
mod grammar_bundle;
mod handlers;
mod render;
mod sql_show_tables;

pub(crate) use grammar_bundle::serve_ppl_grammar;
pub(crate) use handlers::{execute_ppl_query, explain_ppl_query};
pub(crate) use sql_show_tables::show_tables;
