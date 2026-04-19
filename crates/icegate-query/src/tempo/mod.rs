//! Tempo API server module
//!
//! Provides Tempo-compatible HTTP API for querying traces using TraceQL.

mod config;
pub mod error;
pub mod executor;
pub mod formatters;
mod handlers;
mod metadata;
pub mod models;
mod routes;
mod server;
pub mod trace_by_id;

pub use config::TempoConfig;
pub use server::run;
