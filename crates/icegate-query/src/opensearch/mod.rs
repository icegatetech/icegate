//! `OpenSearch`-compatible HTTP API.
//!
//! Cluster and index metadata, Query DSL, Lucene and PPL over IceGate's tables, and saved objects
//! in per-tenant system indices.
//!
//! The tenant of every request is decided by the shared tenant policy (`QueryConfig::tenant`), as
//! on every query surface. Rows are read through the tenant-scoped catalog of
//! `engine::tenant_scope`. Two metadata reads bypass it and scope themselves to the request's
//! tenant: MAP-key enumeration for field discovery, and the per-tenant footprint of
//! `_cat/indices`.

mod catalog;
mod cluster;
mod config;
mod error;
mod extract;
mod fields;
mod filter_path;
mod glob;
mod node;
mod routes;
mod server;
mod state;

pub use config::OpenSearchConfig;
pub use server::{run, run_with_port_tx};
