//! OTLP HTTP server module
//!
//! Provides OpenTelemetry Protocol HTTP endpoint for ingesting logs, traces,
//! and metrics.

mod config;
mod handlers;
mod routes;
mod server;

pub use config::OtlpHttpConfig;
pub use server::run;
