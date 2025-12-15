//! Query execution engine with cached catalog provider
//!
//! This module provides the `QueryEngine` abstraction that:
//! - Caches `IcebergCatalogProvider` to avoid per-query metadata fetches
//! - Provides pre-configured `SessionContext` instances for query execution
//! - Supports periodic cache refresh via background task

mod config;
mod core;

pub use core::QueryEngine;

pub use config::QueryEngineConfig;
