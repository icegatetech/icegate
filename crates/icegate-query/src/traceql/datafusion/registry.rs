//! UDF registration for `TraceQL`.
//!
//! Currently empty: v1 reuses `LogQL` UDFs and does not register any
//! `TraceQL`-specific scalar / aggregate functions. The module is kept
//! as a placeholder so the future hook lives in an obvious location;
//! when the first `TraceQL`-specific UDF lands, add a `pub fn register`
//! and call it from [`super::DataFusionPlanner`]'s constructor.
