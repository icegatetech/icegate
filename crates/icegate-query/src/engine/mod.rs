//! Query execution engine with cached catalog provider
//!
//! This module provides the `QueryEngine` abstraction that:
//! - Caches `IcebergCatalogProvider` to avoid per-query metadata fetches
//! - Provides pre-configured `SessionContext` instances for query execution
//! - Supports periodic cache refresh via background task

mod config;
mod core;
pub(crate) mod provider;

pub use core::QueryEngine;

pub use config::QueryEngineConfig;
pub(crate) use provider::{SourceMetrics, extract_source_metrics};
