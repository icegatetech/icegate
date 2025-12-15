//! Tempo API server module
//!
//! Provides Tempo-compatible HTTP API for querying traces using TraceQL.

mod config;
mod handlers;
mod routes;
mod server;

pub use config::TempoConfig;
pub use server::run;
