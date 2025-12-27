//! Query component - query APIs for logs, traces, and metrics

mod config;

/// Error types for query operations.
pub mod error;

/// CLI for query binary
pub mod cli;

/// Query execution engine with cached catalog provider
pub mod engine;

/// Loki API server (LogQL)
pub mod loki;

/// LogQL query language implementation
pub mod logql;

/// Prometheus API server (PromQL)
pub mod prometheus;

/// Tempo API server (TraceQL)
pub mod tempo;

pub use config::QueryConfig;
