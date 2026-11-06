//! State shared by every `OpenSearch` handler. It holds only `Arc`s, handles that wrap one
//! (`IndexCatalog`) and plain numbers, so axum's per-request clone is cheap.

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
    /// `opensearch.system_indices.max_answer_bytes`. `_mget` enforces it while serializing, and
    /// `_bulk` carries it to the snapshot on each operation through `ReturnedSources`, so it is not
    /// one of `SystemIndexLimits`.
    pub(crate) max_answer_bytes: usize,
}
