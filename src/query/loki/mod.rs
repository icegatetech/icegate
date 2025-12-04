//! Loki API server module
//!
//! Provides Loki-compatible HTTP API for querying logs using LogQL.

mod config;
mod handlers;
mod routes;
mod server;

pub use config::LokiConfig;
pub use server::run;
