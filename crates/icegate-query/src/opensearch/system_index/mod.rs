//! The per-tenant store of dot-prefixed system indices (saved objects).
//!
//! The store keeps each tenant's system indices on object storage under its own location, which
//! must not overlap a table location or the WAL base path. It needs conditional create, `ETag`
//! compare-and-swap and conditional GET; the listener probes them at startup and refuses to start
//! without them.

mod client;

pub use client::{SystemObjectStore, build_system_object_store};
pub(crate) use client::{
    parse_s3_location, probe_conditional_operations, refuse_overlapping_locations, refuse_table_location_overlap,
    resolve_system_indices_location,
};
