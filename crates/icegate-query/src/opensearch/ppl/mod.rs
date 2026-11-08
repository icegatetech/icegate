//! The PPL endpoints of the listener: `POST /_plugins/_ppl`, answered in the `jdbc`, `viz`,
//! `csv` or `raw` format, `POST /_plugins/_ppl/_explain`, and the PPL error envelope.

mod error;
mod handlers;
mod render;

pub(crate) use handlers::{execute_ppl_query, explain_ppl_query};
