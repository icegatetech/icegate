//! State shared by every `OpenSearch` handler. It holds only `Arc`s, so axum's per-request clone
//! is cheap.

use std::sync::Arc;

use super::node::NodeIdentity;

/// Cloned into each handler by axum.
#[derive(Clone)]
pub(crate) struct OpenSearchState {
    /// The node and cluster the listener reports.
    pub(crate) node: Arc<NodeIdentity>,
}
