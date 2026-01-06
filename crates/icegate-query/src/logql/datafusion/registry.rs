//! Registry for LogQL UDFs and UDAFs in DataFusion.
//!
//! This module provides the central registry that manages all LogQL-specific
//! user-defined functions and aggregates.

use datafusion::{
    logical_expr::{AggregateUDF, ScalarUDF},
    prelude::SessionContext,
};

use super::{
    udaf::{AbsentOverTime, ArrayIntersectAgg, BytesOverTime, BytesRate, CountOverTime, RateOverTime},
    udf::{DateGrid, MapDropKeys, MapInsert, MapKeepKeys, ParseBytes, ParseDuration, ParseNumeric},
};

/// Registry for all `LogQL` UDFs and UDAFs.
///
/// Provides UDF/UDAF registration for LogQL-specific operations including:
/// - `map_keep_keys`: Filters a map to keep only specified keys
/// - `map_drop_keys`: Filters a map to remove specified keys
/// - `map_insert`: Inserts a key-value pair into a map
/// - `date_grid`: Generates grid timestamps for range aggregations
/// - `parse_numeric`: Parses string to Float64
/// - `parse_bytes`: Parses humanized byte strings (10KB, 5.5MB) to Float64
/// - `parse_duration`: Parses Go-style durations (5s, 1h30m) to Float64
/// - `count_over_time`: Counts timestamps in time-bucketed ranges
/// - `rate_over_time`: Counts timestamps and divides by range duration
/// - `bytes_over_time`: Sums byte lengths of body in time ranges
/// - `bytes_rate`: Sums byte lengths and divides by range duration
/// - `absent_over_time`: Returns 1 for time ranges with no samples
/// - `array_intersect_agg`: Finds intersection of multiple timestamp arrays
#[derive(Debug, Clone, Default)]
pub struct UdfRegistry;

impl UdfRegistry {
    /// Creates a new UDF registry.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Registers all UDFs and UDAFs with a DataFusion session context.
    ///
    /// Registers the following UDFs:
    /// - `map_keep_keys(map, keys_array)`: Keeps only keys present in array
    /// - `map_drop_keys(map, keys_array)`: Removes keys present in array
    /// - `map_insert(map, key, value)`: Inserts key-value pair into map
    /// - `date_grid(timestamp, start, end, step, range, offset, inverse)`: Generates grid timestamps
    /// - `parse_numeric(value)`: Parses string to Float64
    /// - `parse_bytes(value)`: Parses humanized byte string to Float64
    /// - `parse_duration(value, as_seconds)`: Parses Go-style duration to Float64
    ///
    /// Registers the following UDAFs:
    /// - `count_over_time`: Counts timestamps in time-bucketed ranges (sparse)
    /// - `rate_over_time`: Counts and divides by range duration (sparse)
    /// - `bytes_over_time`: Sums body byte lengths (sparse)
    /// - `bytes_rate`: Sums bytes and divides by range duration (sparse)
    /// - `absent_over_time`: Returns 1 for ranges with no samples
    /// - `array_intersect_agg`: Finds intersection of multiple timestamp arrays
    pub fn register_all(&self, session_ctx: &SessionContext) {
        // Scalar UDFs
        session_ctx.register_udf(ScalarUDF::from(MapKeepKeys::new()));
        session_ctx.register_udf(ScalarUDF::from(MapDropKeys::new()));
        session_ctx.register_udf(ScalarUDF::from(MapInsert::new()));
        session_ctx.register_udf(ScalarUDF::from(DateGrid::new()));
        session_ctx.register_udf(ScalarUDF::from(ParseNumeric::new()));
        session_ctx.register_udf(ScalarUDF::from(ParseBytes::new()));
        session_ctx.register_udf(ScalarUDF::from(ParseDuration::new()));

        // Aggregate UDAFs
        session_ctx.register_udaf(AggregateUDF::from(CountOverTime::new()));
        session_ctx.register_udaf(AggregateUDF::from(RateOverTime::new()));
        session_ctx.register_udaf(AggregateUDF::from(BytesOverTime::new()));
        session_ctx.register_udaf(AggregateUDF::from(BytesRate::new()));
        session_ctx.register_udaf(AggregateUDF::from(AbsentOverTime::new()));
        session_ctx.register_udaf(AggregateUDF::from(ArrayIntersectAgg::new()));
    }
}
