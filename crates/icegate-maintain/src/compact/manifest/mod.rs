//! Manifest compaction: repack small DATA manifests into fewer, larger ones.
//!
//! Every Shifter `fast_append` adds one manifest and carries the rest forward
//! unmerged, so the current snapshot's manifest count grows ~+1 per commit and
//! the query planner opens one object per manifest. Data compaction does not fix
//! this — it skips already-healthy partitions while manifests keep piling up.
//!
//! Split the same way as [`crate::compact::data`]: [`planner`] decides which
//! manifests to repack (pure, no I/O), [`rewrite`] commits the repack. No data
//! files and no delete manifests are touched.

// Documented by each module's own `//!` header; see the note in `compact/mod.rs`.
pub mod planner;
pub mod rewrite;
