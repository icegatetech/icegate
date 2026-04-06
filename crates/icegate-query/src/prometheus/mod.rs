//! Prometheus API server module
//!
//! Provides Prometheus-compatible HTTP API for querying metrics using PromQL.

mod config;
mod error;
mod executor;
mod handlers;
mod models;
mod routes;
mod server;

pub use config::PrometheusConfig;
pub use server::run;
