//! Offset management for exactly-once delivery guarantees.
//!
//! Stores queue offsets in Iceberg snapshot summary to track the last
//! successfully committed offset per topic.

use iceberg::table::Table;

/// Key for storing the queue offset in snapshot summary.
pub const OFFSET_SUMMARY_KEY: &str = "icegate.queue.offset";

// TODO(crit): refactoring

/// Retrieves the last committed offset from the current snapshot's summary.
///
/// Returns `None` if no snapshot exists or no offset has been committed yet.
#[must_use]
pub fn get_committed_offset(table: &Table) -> Option<u64> {
    table.metadata().current_snapshot().and_then(|snapshot| {
        snapshot.summary().additional_properties.get(OFFSET_SUMMARY_KEY).and_then(|v| v.parse::<u64>().ok())
    })
}
