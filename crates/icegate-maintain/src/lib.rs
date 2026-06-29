//! Maintenance operations for IceGate.

/// Error types for maintenance operations.
pub mod error;

/// CLI for maintain binary.
pub mod cli;
/// Parquet compaction operations.
pub mod compact;
/// Configuration for maintain binary.
pub mod config;
/// Background orphan-file garbage collection.
pub mod gc;
/// Schema migration operations.
pub mod migrate;
