//! The dependency set every read-protocol server is started with.

use std::sync::Arc;

use icegate_common::{MemoryPressure, TenantResolver};

use crate::{engine::QueryEngine, infra::metrics::QueryMetrics};

/// Dependencies shared by every read-protocol server.
///
/// One struct rather than four parameters: `loki::run_with_port_tx` already took
/// six arguments against a ceiling of five ([RUST.md], "Function Design") and
/// the tenant resolver would have been the seventh, while three of the four
/// servers take the very same set. Built once in `cli::commands::run` and cloned
/// into each task — a clone is four refcount bumps, taken once per server at
/// startup.
///
/// [RUST.md]: https://github.com/icegatetech/icegate/blob/main/RUST.md
#[derive(Clone)]
pub struct QueryRuntime {
    /// Query engine creating the per-request sessions.
    pub engine: Arc<QueryEngine>,
    /// Recorder every surface reports its requests and its tenant refusals to.
    pub metrics: Arc<QueryMetrics>,
    /// Memory-pressure guard the shedding layers consult.
    pub pressure: MemoryPressure,
    /// The decision of whose tenant one request reads.
    pub tenant_resolver: TenantResolver,
}
