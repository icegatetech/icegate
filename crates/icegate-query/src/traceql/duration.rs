//! TraceQL duration parsing.
//!
//! TraceQL durations are syntactically identical to LogQL durations, so
//! we re-export the existing parser from [`crate::logql::duration`].

pub use crate::logql::duration::{parse_duration, parse_duration_nanos, parse_duration_opt};
