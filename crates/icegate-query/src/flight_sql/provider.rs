//! Per-request `SessionStateProvider` enforcing tenant isolation.
//!
//! `datafusion-flight-sql-server` calls
//! [`SessionStateProvider::new_context`] for every incoming Flight SQL
//! request. We use that hook to:
//!
//! 1. Read the [`TenantId`] the tenant interceptor resolved and placed in
//!    the request extensions (see
//!    [`TenantPolicyInterceptor`](icegate_common::TenantPolicyInterceptor)).
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
use icegate_common::TenantId;
use tonic::{Request, Status};

use super::tenant_catalog::TenantScopedCatalogProvider;
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
        // `internal`, not `unauthenticated`: an absent extension means the
        // server was assembled without the tenant interceptor — a defect in the
        // wiring, not a property of the request. That the upstream service
        // carries the extensions of the original request into the one handed
        // here, on `GetFlightInfo` and on the `DoGet` that streams a statement's
        // rows alike, is pinned by `each_tenant_sees_only_their_own_rows` in
        // `tests/flight_sql/tenant.rs`.
        let tenant_id = request
            .extensions()
            .get::<TenantId>()
            .ok_or_else(|| Status::internal("flight-sql: the tenant interceptor did not run"))?;
        let ctx = self
            .engine
            .create_session()
            .await
            .map_err(|err| Status::internal(format!("flight-sql: session creation failed: {err}")))?;
        // The engine registers the iceberg catalog under its configured
        // `catalog_name` (default `iceberg`); look it up under the same
        // name rather than a hardcoded constant so a non-default catalog
        // name doesn't break every request.
        install_tenant_scope(&ctx, &self.engine.config().catalog_name, tenant_id.as_ref())
            .map_err(|err| Status::internal(format!("flight-sql: tenant scope installation failed: {err}")))?;
        Ok(ctx.state())
    }
}

/// Install the tenant-scoped catalog wrapper on this session.
///
/// `information_schema` is enabled once on the engine's `SessionConfig`
/// (see [`QueryEngine::create_session`]), so there is no per-request
/// `SET` statement here — only the catalog swap, which is synchronous.
fn install_tenant_scope(
    ctx: &SessionContext,
    catalog_name: &str,
    tenant_id: &str,
) -> Result<(), datafusion::error::DataFusionError> {
    // Wrap the iceberg catalog so every table it surfaces enforces the
    // tenant predicate at the row level inside `scan()` AND hides the
    // `tenant_id` column from the schema. This is the entire tenancy
    // surface — no shortcut views, no alternate path, just the wrapped
    // catalog.
    let inner_catalog = ctx.catalog(catalog_name).ok_or_else(|| {
        datafusion::error::DataFusionError::Internal(format!(
            "iceberg catalog `{catalog_name}` not registered on the session"
        ))
    })?;
    let wrapped: Arc<dyn CatalogProvider> = Arc::new(TenantScopedCatalogProvider::new(inner_catalog, tenant_id));
    ctx.register_catalog(catalog_name, wrapped);
    Ok(())
}
