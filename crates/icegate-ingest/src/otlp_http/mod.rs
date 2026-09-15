//! OTLP HTTP server module
//!
//! Provides OpenTelemetry Protocol HTTP endpoint for ingesting logs, traces,
//! and metrics.

mod config;
mod error;
mod handlers;
mod models;
mod routes;
mod server;
mod tenant;

pub use config::OtlpHttpConfig;
pub use error::{OtlpError, OtlpResult};
pub use models::{ErrorResponse, ErrorType, ExportLogsResponse, ExportMetricsResponse, ExportTracesResponse};
pub use server::{OtlpHttpState, run};
