//! Cluster and index-metadata endpoints. The cluster polls (`/`, `_nodes`, `_cluster/health`, the
//! query data sources) read no table, so a client polling them costs no catalog access.
//! `_resolve/index` and `_settings` list the tenant's tables through the index catalog and read no
//! rows.

pub(crate) mod datasources;
pub(crate) mod health;
pub(crate) mod info;
pub(crate) mod resolve;
pub(crate) mod settings;
