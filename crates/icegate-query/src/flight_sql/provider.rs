//! Per-request `SessionStateProvider` enforcing tenant isolation.
//!
//! `datafusion-flight-sql-server` calls
//! [`SessionStateProvider::new_context`] for every incoming Flight SQL
//! request. We use that hook to:
//!
//! 1. Extract the tenant identifier from the gRPC `x-scope-orgid`
//!    metadata header (falling back to the shared `default` tenant when
//!    absent or malformed, mirroring the Loki/Tempo behaviour).
//! 2. Build a fresh `SessionContext` via the shared [`QueryEngine`].
//! 3. Swap the session's `iceberg` catalog for a
//!    [`TenantScopedCatalogProvider`] decorator. The decorator:
//!    - injects `tenant_id = '<t>'` into every `scan()` at the
//!      `TableProvider` layer, and
//!    - hides the `tenant_id` column from the advertised schema.
//!
//! There are no convenience views: clients always go through the
//! qualified path `iceberg.icegate.<table>`. This keeps the tenancy
//! guarantee anchored to a single layer (the wrapper) — no shortcut
//! exists that could ever bypass it.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::CatalogProvider;
use datafusion::execution::context::SessionState;
use datafusion::prelude::SessionContext;
use datafusion_flight_sql_server::session::SessionStateProvider;
use icegate_common::ICEBERG_CATALOG;
use tonic::{Request, Status};

use super::tenant_catalog::TenantScopedCatalogProvider;
use super::tenant_id::resolve_tenant_id;
use crate::engine::QueryEngine;

/// IceGate-specific `SessionStateProvider`.
///
/// Owns an `Arc<QueryEngine>` so it can build fresh sessions on every
/// inbound request without contending with concurrent handlers. The
/// engine handles catalog caching internally; this provider only
/// installs the tenant-scoped catalog decorator.
pub(crate) struct IceGateSessionStateProvider {
    engine: Arc<QueryEngine>,
}

impl IceGateSessionStateProvider {
    /// Construct a new provider backed by the supplied engine.
    pub(crate) const fn new(engine: Arc<QueryEngine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl SessionStateProvider for IceGateSessionStateProvider {
    async fn new_context(&self, request: &Request<()>) -> Result<SessionState, Status> {
        let tenant_id = resolve_tenant_id(request.metadata());
        let ctx = self
            .engine
            .create_session()
            .await
            .map_err(|err| Status::internal(format!("flight-sql: session creation failed: {err}")))?;
        install_tenant_scope(&ctx, &tenant_id)
            .await
            .map_err(|err| Status::internal(format!("flight-sql: tenant scope installation failed: {err}")))?;
        Ok(ctx.state())
    }
}

/// Install the tenant-scoped catalog wrapper on this session.
///
/// Runs inside the session bootstrap path, *before* the
/// `FlightSqlService::with_sql_options` read-only guard kicks in, so
/// the `SET` statement here is permitted regardless of the SQL options
/// applied to user-submitted queries.
async fn install_tenant_scope(ctx: &SessionContext, tenant_id: &str) -> Result<(), datafusion::error::DataFusionError> {
    // Turn on `information_schema` so BI tools that introspect via
    // `SHOW TABLES` / `SELECT * FROM information_schema.tables` work
    // out of the box.
    ctx.sql("SET datafusion.catalog.information_schema = true").await?;

    // Wrap the iceberg catalog so every table it surfaces has the
    // tenant predicate injected into `scan()` at the `TableProvider`
    // layer AND the `tenant_id` column hidden from the schema. This
    // is the entire tenancy surface — no shortcut views, no alternate
    // path, just the wrapped catalog.
    let inner_catalog = ctx.catalog(ICEBERG_CATALOG).ok_or_else(|| {
        datafusion::error::DataFusionError::Internal(format!(
            "iceberg catalog `{ICEBERG_CATALOG}` not registered on the session"
        ))
    })?;
    let wrapped: Arc<dyn CatalogProvider> = Arc::new(TenantScopedCatalogProvider::new(inner_catalog, tenant_id));
    ctx.register_catalog(ICEBERG_CATALOG, wrapped);
    Ok(())
}

// Unit tests for tenant resolution live in `tenant_id.rs` so this file
// stays focused on the session-state-provider plumbing.
