//! UDF registration for `TraceQL`.
//!
//! v1 reuses `LogQL` UDFs; nothing `TraceQL`-specific is required yet.

use datafusion::prelude::SessionContext;

/// Register `TraceQL`-specific UDFs on a session context.
///
/// Currently a no-op; reserved for future use.
#[allow(dead_code)] // Reserved for future TraceQL-specific UDFs.
pub const fn register(_ctx: &SessionContext) {}
