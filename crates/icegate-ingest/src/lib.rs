//! Ingest component - OTLP receivers for logs, traces, and metrics

mod config;

/// OTLP gRPC server
pub mod otlp_grpc;

/// OTLP HTTP server
pub mod otlp_http;

pub use config::IngestConfig;
