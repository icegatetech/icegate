//! The per-tenant store of dot-prefixed system indices (saved objects).
//!
//! The store keeps each tenant's system indices on object storage under its own location, which
//! must not overlap a table location or the WAL base path. It needs conditional create, `ETag`
//! compare-and-swap and conditional GET; the listener probes them at startup and refuses to start
//! without them.

mod api;
mod client;
mod codec;
pub(crate) mod documents;
mod snapshot;
mod store;

pub(crate) use api::{
    IndexKeyedAnswer, apply_aliases, create_system_index, delete_index_template, describe_system_index, get_alias,
    get_index_aliases, refresh_index,
};
pub use client::{SystemObjectStore, build_system_object_store};
pub(crate) use client::{
    parse_s3_location, probe_conditional_operations, refuse_overlapping_locations, refuse_table_location_overlap,
    resolve_system_indices_location,
};
pub(crate) use snapshot::{MappedField, SystemSnapshot};
pub(crate) use store::{CurrentSnapshot, SystemIndexLimits, SystemIndexStore};
