//! Parquet compaction for IceGate Iceberg tables.

// Each module carries its own `//!` documentation; adding an outer `///` here
// would concatenate the two and resolve the inner module's intra-doc links in
// THIS module's scope, breaking every one of them.
pub mod compactor;
pub mod config;
pub mod data;
pub mod manifest;
pub mod metrics;
pub mod tasks;

pub use compactor::{CompactJobSpec, Compactor, CompactorHandle};
