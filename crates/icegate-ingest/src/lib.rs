//! Ingest component - OTLP receivers for logs, traces, and metrics

mod config;

/// Error types for ingest operations.
pub mod error;

/// CLI module for the ingest binary
pub mod cli;

/// OTLP gRPC server
pub mod otlp_grpc;

/// OTLP HTTP server
pub mod otlp_http;

/// OTLP to Arrow transform utilities (shared by gRPC and HTTP)
pub mod transform;

pub use config::IngestConfig;
