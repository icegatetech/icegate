//! Cluster and index-metadata endpoints. The cluster polls (`/`, `_nodes`, `_cluster/health`,
//! `_cluster/settings`, the query data sources), `_cat/plugins`, `_cat/templates` and the plugin
//! stubs (the observability object list, ML configs) read no table, so a client polling them costs
//! no catalog access. `_resolve/index`, `_settings` and `_cat/indices` list the tenant's
//! tables and system indices through the index catalog and read no rows; `_cat/indices` counts the
//! tenant's rows from snapshot metadata.

pub(crate) mod cat;
pub(crate) mod cluster_settings;
pub(crate) mod datasources;
pub(crate) mod health;
pub(crate) mod info;
pub(crate) mod observability;
pub(crate) mod resolve;
pub(crate) mod settings;
