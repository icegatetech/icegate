//! User-defined functions for LogQL operations in DataFusion.
//!
//! This module provides simplified UDF placeholders. Full implementations
//! would require using the ScalarUDFImpl trait with proper type handling.

use datafusion::prelude::SessionContext;

/// Registry for all LogQL UDFs.
///
/// Currently provides placeholder registrations for LogQL-specific operations.
/// These will be implemented as proper UDFs when the pipeline stages are fully built out.
#[derive(Debug, Clone, Default)]
pub struct UdfRegistry;

impl UdfRegistry {
    /// Creates a new UDF registry.
    pub fn new() -> Self {
        Self
    }

    /// Registers all UDFs with a DataFusion session context.
    ///
    /// Currently a no-op placeholder. Future implementations will register:
    /// - `json_parser`: Parses JSON and extracts fields
    /// - `logfmt_parser`: Parses logfmt format  
    /// - `regexp_parser`: Extracts named regex groups
    /// - `pattern_parser`: Pattern-based field extraction
    /// - `decolorize`: Removes ANSI color codes
    /// - `line_format`: Template-based line formatting
    /// - `ip_match`: CIDR-based IP matching
    /// - `unpack`: Unpacks nested structures
    pub fn register_all(&self, _ctx: &SessionContext) {
        // TODO: Implement UDF registration when pipeline stages need them
        // For now, we'll use built-in DataFusion functions where possible
    }
}
