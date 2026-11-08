//! The PPL endpoint of the listener: `POST /_plugins/_ppl`, its jdbc rendering and the PPL error
//! envelope.

mod error;
mod handlers;
mod render;

pub(crate) use handlers::execute_ppl_query;
