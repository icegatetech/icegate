//! Query component - query APIs for logs, traces, and metrics

mod config;

/// Error types for query operations.
pub mod error;

/// CLI for query binary
pub mod cli;

/// Query execution engine with cached catalog provider
pub mod engine;

/// Infrastructure: metrics, observability helpers.
pub mod infra;

/// Flight SQL gRPC server (Apache Arrow Flight SQL)
pub mod flight_sql;

/// Loki API server (LogQL)
pub mod loki;

/// LogQL query language implementation
pub mod logql;

/// Prometheus API server (PromQL)
pub mod prometheus;

/// Tempo API server (TraceQL)
pub mod tempo;

/// TraceQL query language implementation
pub mod traceql;

/// Doubles and bootstrap shared by the crate's inline tests.
#[cfg(test)]
mod test_support;

pub use config::QueryConfig;
