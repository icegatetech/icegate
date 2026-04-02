//! Shared merge-order contract for logs WAL and shift processing.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

/// Exact first-row boundary key for a sorted logs row group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowGroupBoundaryKey {
    /// Cloud account identifier.
    pub cloud_account_id: Option<String>,
    /// Service name.
    pub service_name: Option<String>,
    /// Timestamp in microseconds.
    pub timestamp_micros: Option<i64>,
}

impl RowGroupBoundaryKey {
    /// Compare two keys using the logs merge order.
    #[must_use]
    pub fn compare(&self, other: &Self) -> Ordering {
        let ordering = compare_option_ord(
            self.cloud_account_id.as_deref(),
            other.cloud_account_id.as_deref(),
            false,
            true,
        );
        if ordering != Ordering::Equal {
            return ordering;
        }

        let ordering = compare_option_ord(self.service_name.as_deref(), other.service_name.as_deref(), false, true);
        if ordering != Ordering::Equal {
            return ordering;
        }

        compare_option_ord(self.timestamp_micros, other.timestamp_micros, true, true)
    }
}

/// Compare optional scalar values with explicit null and direction semantics.
#[must_use]
pub fn compare_option_ord<T: Ord>(left: Option<T>, right: Option<T>, descending: bool, nulls_first: bool) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let ordering = left.cmp(&right);
            if descending { ordering.reverse() } else { ordering }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{RowGroupBoundaryKey, compare_option_ord};

    #[test]
    fn compare_option_ord_handles_desc_nulls_first() {
        assert_eq!(compare_option_ord(None::<i64>, Some(1), true, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(2), Some(1), true, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(1), Some(2), true, true), Ordering::Greater);
    }

    #[test]
    fn row_group_boundary_key_matches_logs_order() {
        let lower = RowGroupBoundaryKey {
            cloud_account_id: Some("acc-1".to_string()),
            service_name: Some("svc-1".to_string()),
            timestamp_micros: Some(20),
        };
        let higher = RowGroupBoundaryKey {
            cloud_account_id: Some("acc-1".to_string()),
            service_name: Some("svc-1".to_string()),
            timestamp_micros: Some(30),
        };
        let null_cloud = RowGroupBoundaryKey {
            cloud_account_id: None,
            service_name: Some("svc-9".to_string()),
            timestamp_micros: Some(1),
        };

        assert_eq!(null_cloud.compare(&lower), Ordering::Less);
        assert_eq!(higher.compare(&lower), Ordering::Less);
        assert_eq!(lower.compare(&higher), Ordering::Greater);
    }
}
