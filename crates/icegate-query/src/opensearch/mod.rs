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

pub(crate) mod catalog;
mod cluster;
mod config;
pub(crate) mod context;
pub(crate) mod error;
pub(crate) mod extract;
mod fields;
mod filter_path;
pub(crate) mod glob;
mod node;
mod ppl;
mod routes;
mod script;
pub(crate) mod search;
mod server;
mod state;
pub(crate) mod system_index;

pub use config::{OpenSearchConfig, PplLimitsConfig, SearchLimitsConfig, SystemIndicesConfig};
pub use server::{run, run_with_port_tx};
pub use system_index::{SystemObjectStore, build_system_object_store};
pub(crate) use system_index::{refuse_overlapping_locations, resolve_system_indices_location};
