//! The `OpenSearch` index catalog: where an index expression resolves to the tables a tenant may
//! read, and where those tables' fields are listed. Every `OpenSearch` handler that reaches table
//! data starts here, so the tenant scope cannot be skipped.

mod expression;
mod field;
mod native;
mod registry;

use std::sync::Arc;

use datafusion::catalog::SchemaProvider;
use icegate_common::{ICEGATE_NAMESPACE, TenantId};

pub(crate) use self::expression::IndexExpression;
pub(crate) use self::field::{FieldType, IndexField};
use self::native::list_native_fields;
use self::registry::Profile;
pub(crate) use self::registry::{IndexEntry, IndexOptions, IndexSet, read_index_options};
use super::error::OpenSearchError;
use crate::engine::QueryEngine;
use crate::engine::tenant_scope::install_tenant_scope;

/// Resolves indices and fields against the tables a tenant may read.
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

    /// Open a session scoped to `tenant`, with the data indices it sees: one per table its
    /// tenant-scoped catalog lists in the IceGate namespace, except dot-prefixed names, which are
    /// system indices. This is the only way an `OpenSearch` handler reaches table data, so every
    /// read goes through the tenant scope.
    ///
    /// # Errors
    ///
    /// Returns `500` when the session cannot be created, the tenant scope cannot be installed, or
    /// the IceGate namespace is missing from the catalog.
    pub(crate) async fn open_session(&self, tenant: &TenantId) -> Result<TenantSession, OpenSearchError> {
        let context = self.engine.create_session().await?;
        let catalog_name = &self.engine.config().catalog_name;
        install_tenant_scope(&context, catalog_name, tenant)
            .map_err(|error| OpenSearchError::internal(error.to_string()))?;
        let namespace = context
            .catalog(catalog_name)
            .and_then(|catalog| catalog.schema(ICEGATE_NAMESPACE))
            .ok_or_else(|| {
                OpenSearchError::internal(format!(
                    "namespace `{ICEGATE_NAMESPACE}` not found in catalog `{catalog_name}`"
                ))
            })?;
        let indices = IndexSet::from_table_names(namespace.table_names());
        Ok(TenantSession { namespace, indices })
    }
}

/// A tenant-scoped view of the catalog for one request.
#[derive(Debug)]
pub(crate) struct TenantSession {
    /// The tenant-scoped IceGate namespace: every table it yields is filtered to the tenant and
    /// hides `tenant_id`.
    namespace: Arc<dyn SchemaProvider>,
    /// The data indices the tenant sees, listed when the session opened.
    indices: IndexSet,
}

impl TenantSession {
    /// The data indices the tenant sees.
    pub(crate) const fn indices(&self) -> &IndexSet {
        &self.indices
    }

    /// The fields of `entry`, in schema column order. A MAP column lists as an `object` with no
    /// keys.
    ///
    /// # Errors
    ///
    /// Returns `404` when the table vanished since the session opened, and `500` when the table
    /// cannot be opened or a column type has no `OpenSearch` representation.
    pub(crate) async fn list_fields(&self, entry: &IndexEntry) -> Result<Vec<IndexField>, OpenSearchError> {
        let provider = self
            .namespace
            .table(entry.table())
            .await
            .map_err(|error| OpenSearchError::internal(error.to_string()))?
            .ok_or_else(|| OpenSearchError::index_not_found(entry.name()))?;
        match entry.profile() {
            Profile::Native => list_native_fields(&provider.schema()),
        }
    }
}
