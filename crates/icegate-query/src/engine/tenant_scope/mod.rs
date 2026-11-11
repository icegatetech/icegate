//! Tenant isolation for read surfaces that resolve tables through the session catalog, and a
//! tenant's figures from table metadata.
//!
//! [`install_tenant_scope`] swaps the session's Iceberg catalog for a decorator that enforces
//! `tenant_id = '<t>'` on every scan and hides the column (see [`catalog`]). A surface that
//! installs it adds no tenant predicate of its own: isolation stays anchored to this one layer,
//! so no two surfaces that install it can diverge in what a tenant may read.
//!
//! [`count_tenant_footprint`] reads a tenant's committed figures in one table from snapshot
//! metadata, without the decorator: it prunes to the tenant's identity partition, except a global
//! table, which is counted whole.

mod catalog;
mod record_counts;

use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use icegate_common::TenantId;

use self::catalog::TenantScopedCatalogProvider;
pub(crate) use self::record_counts::{TenantFootprint, count_tenant_footprint};

/// Install the tenant-scoped catalog decorator on `ctx`.
///
/// `information_schema` is enabled once on the engine's `SessionConfig` (see
/// [`QueryEngine::create_session`](crate::engine::QueryEngine::create_session)), so this is only
/// the catalog swap, which is synchronous. Every table the catalog surfaces afterwards is filtered
/// to `tenant_id` at the row level, except the global tables listed in
/// [`icegate_common::GLOBAL_TABLES`].
///
/// # Errors
///
/// Returns [`DataFusionError::Internal`] when `catalog_name` is not registered on `ctx`.
pub(crate) fn install_tenant_scope(
    ctx: &SessionContext,
    catalog_name: &str,
    tenant_id: &TenantId,
) -> Result<(), DataFusionError> {
    let inner_catalog = ctx.catalog(catalog_name).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "iceberg catalog `{catalog_name}` not registered on the session"
        ))
    })?;
    let scoped_catalog: Arc<dyn CatalogProvider> =
        Arc::new(TenantScopedCatalogProvider::new(inner_catalog, tenant_id.as_ref()));
    ctx.register_catalog(catalog_name, scoped_catalog);
    Ok(())
}
