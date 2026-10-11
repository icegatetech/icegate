//! State shared by every `OpenSearch` handler. It holds only `Arc`s and handles that wrap one
//! (`IndexCatalog`), so axum's per-request clone is cheap.

use std::sync::Arc;

use super::catalog::IndexCatalog;
use super::node::NodeIdentity;
use super::system_index::SystemIndexStore;

/// Cloned into each handler by axum.
#[derive(Clone)]
pub(crate) struct OpenSearchState {
    /// The node and cluster the listener reports.
    pub(crate) node: Arc<NodeIdentity>,
    /// The index catalog every data-reading handler resolves its indices through.
    pub(crate) catalog: IndexCatalog,
    /// The per-tenant store of the system indices.
    pub(crate) system_store: Arc<SystemIndexStore>,
}
