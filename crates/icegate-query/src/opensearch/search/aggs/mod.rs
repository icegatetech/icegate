//! Aggregations of the data `_search` path: parsing, planning over the matching rows, assembly
//! of the bucket tree and rendering.

mod assemble;
mod date_histogram;
mod distinct;
mod metrics;
mod multi_bucket;
mod parse;
mod percentiles;
mod plan;
mod render;
mod terms;

pub(crate) use parse::parse_aggregations;
pub(crate) use plan::{AggregationScope, execute_aggregations, plan_aggregation_rows};

#[cfg(test)]
mod test_support {
    use chrono::DateTime;

    /// The RFC 3339 instant `text` in UTC microseconds.
    pub(super) fn parse_instant_micros(text: &str) -> i64 {
        DateTime::parse_from_rfc3339(text)
            .expect("an RFC 3339 instant")
            .timestamp_micros()
    }
}
