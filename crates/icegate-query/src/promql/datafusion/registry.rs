//! Registry for PromQL UDFs and UDAFs in DataFusion.
//!
//! This module provides the central registry that manages all PromQL-specific
//! user-defined functions and aggregates.

use datafusion::prelude::SessionContext;

/// Registry for all `PromQL` UDFs and UDAFs.
///
/// Phase 1: No custom functions are registered. This provides the extension
/// point for future PromQL-specific UDFs (e.g., `histogram_quantile`) and
/// UDAFs (e.g., rate computation over range vectors).
#[derive(Debug, Clone, Default)]
pub struct UdfRegistry;

impl UdfRegistry {
    /// Creates a new UDF registry.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Registers all UDFs and UDAFs with a DataFusion session context.
    ///
    /// Phase 1: This is a no-op. Custom functions will be added incrementally
    /// as `PromQL` features are implemented.
    pub const fn register_all(&self, _session_ctx: &SessionContext) {
        // Phase 1: no custom UDFs yet
    }
}
