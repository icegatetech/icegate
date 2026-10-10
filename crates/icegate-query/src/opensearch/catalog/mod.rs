//! The `OpenSearch` index catalog: where an index expression resolves to the tables a tenant may
//! read. Every `OpenSearch` handler that reaches table data starts here, so the tenant scope cannot
//! be skipped.

mod expression;
mod registry;

use std::sync::Arc;

use icegate_common::{ICEGATE_NAMESPACE, TenantId};

pub(crate) use self::expression::IndexExpression;
pub(crate) use self::registry::{IndexOptions, IndexSet};
use super::error::OpenSearchError;
use crate::engine::QueryEngine;
use crate::engine::tenant_scope::install_tenant_scope;

/// Lists the data indices a tenant may read, through its tenant-scoped catalog;
/// [`IndexSet::resolve`] applies an expression to them.
#[derive(Clone)]
pub(crate) struct IndexCatalog {
    /// The engine whose sessions list the tables.
    engine: Arc<QueryEngine>,
}

impl IndexCatalog {
    /// A catalog over the tables of `engine`.
    pub(crate) const fn new(engine: Arc<QueryEngine>) -> Self {
        Self { engine }
    }

    /// The data indices `tenant` sees: one per table its tenant-scoped catalog lists in the
    /// IceGate namespace, except dot-prefixed names, which are system indices.
    ///
    /// # Errors
    ///
    /// Returns `500` when the session cannot be created, the tenant scope cannot be installed, or
    /// the IceGate namespace is missing from the catalog.
    pub(crate) async fn list_indices(&self, tenant: &TenantId) -> Result<IndexSet, OpenSearchError> {
        let context = self.engine.create_session().await?;
        let catalog_name = &self.engine.config().catalog_name;
        install_tenant_scope(&context, catalog_name, tenant)
            .map_err(|error| OpenSearchError::internal(error.to_string()))?;
        let schema = context
            .catalog(catalog_name)
            .and_then(|catalog| catalog.schema(ICEGATE_NAMESPACE))
            .ok_or_else(|| {
                OpenSearchError::internal(format!(
                    "namespace `{ICEGATE_NAMESPACE}` not found in catalog `{catalog_name}`"
                ))
            })?;
        Ok(IndexSet::from_table_names(schema.table_names()))
    }
}
