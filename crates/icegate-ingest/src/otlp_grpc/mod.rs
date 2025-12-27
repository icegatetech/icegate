//! OTLP gRPC server module.
//!
//! Provides OpenTelemetry Protocol gRPC endpoint for ingesting logs, traces,
//! and metrics.

mod config;
mod error;
mod server;
mod services;

pub use config::OtlpGrpcConfig;
pub use error::GrpcError;
pub use server::run;
pub use services::OtlpGrpcService;
