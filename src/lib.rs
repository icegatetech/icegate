//! `IceGate` - Observability data lake query engine using `Iceberg` and
//! `DataFusion`.
//!
//! This crate provides Iceberg schema definitions and query capabilities for
//! `OpenTelemetry` observability data (logs, traces, metrics).

/// Common utilities and error types.
pub mod common;
/// Ingest component for receiving OTLP data.
pub mod ingest;
/// Migration module for schema management.
pub mod maintain;
/// Query component for querying data via Loki, Prometheus, and Tempo APIs.
pub mod query;
