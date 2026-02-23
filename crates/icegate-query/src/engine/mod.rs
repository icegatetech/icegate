//! Query execution engine.
//!
//! This module provides the `QueryEngine` abstraction that:
//! - Builds a fresh catalog provider per session for up-to-date metadata
//! - Provides pre-configured `SessionContext` instances for query execution

mod config;
mod core;
pub(crate) mod provider;

pub use core::QueryEngine;

pub use config::QueryEngineConfig;
pub(crate) use provider::{SourceMetrics, extract_source_metrics};
