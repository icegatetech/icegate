//! Data-file compaction: merge small Parquet files into target-sized ones.
//!
//! The three stages of one rewrite, one module each:
//!
//! 1. [`planner`] decides WHICH files to merge — pure bin-packing over a
//!    partition's file sizes, with no I/O.
//! 2. [`rewrite`] executes one planned group: k-way-merge through
//!    [`merge_source`], write target-sized Parquet, atomically replace the
//!    inputs.
//! 3. [`envelope`] guards the result: the content invariants a lossless merge
//!    must preserve, checked BEFORE the replace is committed.
//!
//! None of these modules depends on the jobmanager; the task wiring lives in
//! [`crate::compact::tasks`].

// Documented by each module's own `//!` header; see the note in `compact/mod.rs`.
pub mod envelope;
pub mod merge_source;
pub mod planner;
pub mod rewrite;
