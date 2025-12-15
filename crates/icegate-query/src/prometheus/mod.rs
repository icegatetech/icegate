//! Prometheus API server module
//!
//! Provides Prometheus-compatible HTTP API for querying metrics using PromQL.

mod config;
mod handlers;
mod routes;
mod server;

pub use config::PrometheusConfig;
pub use server::run;
