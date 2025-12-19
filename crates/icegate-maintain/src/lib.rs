//! Maintenance operations for IceGate.

/// Error types for maintenance operations.
pub mod error;

/// CLI for maintain binary.
pub mod cli;
/// Configuration for maintain binary.
pub mod config;
/// Schema migration operations.
pub mod migrate;
/// Shift operations for moving data from queue to Iceberg.
pub mod shift;
