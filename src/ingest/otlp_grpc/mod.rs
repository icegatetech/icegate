//! OTLP gRPC server module
//!
//! Provides OpenTelemetry Protocol gRPC endpoint for ingesting logs, traces, and metrics.
//!
//! # TODO
//! - Add opentelemetry-proto dependency for OTLP protobuf definitions
//! - Generate tonic service stubs from .proto files
//! - Implement LogsService, TracesService, MetricsService traits
//! - Parse OTLP protobuf messages
//! - Transform OTLP data to Iceberg schema format
//! - Write to Iceberg tables via catalog

mod config;
mod server;

pub use config::OtlpGrpcConfig;
pub use server::run;
