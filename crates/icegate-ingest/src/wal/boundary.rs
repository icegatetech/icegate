use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::error::{IngestError, Result};

/// Scalar value stored in a row-group boundary key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RowGroupBoundaryValue {
    /// UTF-8 string value.
    String(String),
    /// Timestamp value in microseconds.
    TimestampMicros(i64),
}

/// Inclusive boundary range for one sorted row group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RowGroupBoundaryRange {
    /// First key in the row group.
    pub(crate) min_key: RowGroupBoundaryKey,
    /// Last key in the row group.
    pub(crate) max_key: RowGroupBoundaryKey,
}

impl RowGroupBoundaryRange {
    /// Validate that both boundary keys use the same structure and define a non-decreasing range.
    pub(crate) fn validate(&self) -> Result<()> {
        self.min_key.validate_compatible_structure(&self.max_key)?;
        if self.min_key.compare_checked(&self.max_key)? == Ordering::Greater {
            return Err(IngestError::Shift("min_key must be <= max_key".to_string()));
        }
        Ok(())
    }
}

/// Boundary key for one end of a sorted row group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RowGroupBoundaryKey {
    /// Sort-field components in Iceberg sort-order order.
    pub(crate) components: Vec<RowGroupBoundaryComponent>,
}

impl RowGroupBoundaryKey {
    /// Validate that both keys use identical component structure.
    pub(crate) fn validate_compatible_structure(&self, other: &Self) -> Result<()> {
        if self.components.len() != other.components.len() {
            return Err(IngestError::Shift(format!(
                "incompatible boundary key structure: component count differs ({}, {})",
                self.components.len(),
                other.components.len()
            )));
        }

        for (idx, (left, right)) in self.components.iter().zip(other.components.iter()).enumerate() {
            left.validate_compatible_structure(right, idx)?;
        }

        Ok(())
    }

    /// Compare two boundary keys using the serialized sort semantics.
    pub(crate) fn compare_checked(&self, other: &Self) -> Result<Ordering> {
        self.validate_compatible_structure(other)?;
        Ok(self.compare(other))
    }

    pub(crate) fn compare(&self, other: &Self) -> Ordering {
        for (left, right) in self.components.iter().zip(other.components.iter()) {
            let ordering = left.compare_unchecked(right);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        self.components.len().cmp(&other.components.len())
    }
}
/// One sort-field component inside a row-group boundary key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RowGroupBoundaryComponent {
    /// Serialized field value.
    pub(crate) value: Option<RowGroupBoundaryValue>,
    /// Whether this field sorts descending.
    pub(crate) descending: bool,
    /// Whether nulls sort before non-null values.
    pub(crate) nulls_first: bool,
}

impl RowGroupBoundaryComponent {
    #[cfg(test)]
    pub(crate) fn string(value: Option<String>, descending: bool, nulls_first: bool) -> Self {
        Self {
            value: value.map(RowGroupBoundaryValue::String),
            descending,
            nulls_first,
        }
    }

    #[cfg(test)]
    pub(crate) const fn timestamp_micros(value: Option<i64>, descending: bool, nulls_first: bool) -> Self {
        Self {
            value: match value {
                Some(value) => Some(RowGroupBoundaryValue::TimestampMicros(value)),
                None => None,
            },
            descending,
            nulls_first,
        }
    }

    fn validate_compatible_structure(&self, other: &Self, index: usize) -> Result<()> {
        if self.descending != other.descending {
            return Err(IngestError::Shift(format!(
                "incompatible boundary key structure at component {index}: descending differs ({}, {})",
                self.descending, other.descending
            )));
        }
        if self.nulls_first != other.nulls_first {
            return Err(IngestError::Shift(format!(
                "incompatible boundary key structure at component {index}: nulls_first differs ({}, {})",
                self.nulls_first, other.nulls_first
            )));
        }

        match (&self.value, &other.value) {
            (Some(left), Some(right)) if boundary_value_discriminant(left) != boundary_value_discriminant(right) => {
                Err(IngestError::Shift(format!(
                    "incompatible boundary key structure at component {index}: value type differs ({}, {})",
                    left.type_name(),
                    right.type_name()
                )))
            }
            _ => Ok(()),
        }
    }

    fn compare_unchecked(&self, other: &Self) -> Ordering {
        debug_assert!(self.validate_compatible_structure(other, 0).is_ok());

        match (&self.value, &other.value) {
            (None, Some(_)) => {
                if self.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (Some(_), None) => {
                if self.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Some(RowGroupBoundaryValue::String(left)), Some(RowGroupBoundaryValue::String(right))) => {
                compare_option_ord(
                    Some(left.as_str()),
                    Some(right.as_str()),
                    self.descending,
                    self.nulls_first,
                )
            }
            (
                Some(RowGroupBoundaryValue::TimestampMicros(left)),
                Some(RowGroupBoundaryValue::TimestampMicros(right)),
            ) => compare_option_ord(Some(*left), Some(*right), self.descending, self.nulls_first),
            (None, None) | (Some(_), Some(_)) => Ordering::Equal,
        }
    }
}

impl RowGroupBoundaryValue {
    const fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::TimestampMicros(_) => "timestamp_micros",
        }
    }
}

pub(crate) fn compare_option_ord<T: Ord>(
    left: Option<T>,
    right: Option<T>,
    descending: bool,
    nulls_first: bool,
) -> Ordering {
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

const fn boundary_value_discriminant(value: &RowGroupBoundaryValue) -> u8 {
    match value {
        RowGroupBoundaryValue::String(_) => 0,
        RowGroupBoundaryValue::TimestampMicros(_) => 1,
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::{RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, compare_option_ord};

    #[test]
    fn compare_option_ord_handles_desc_nulls_first() {
        assert_eq!(compare_option_ord(None::<i64>, Some(1), true, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(2), Some(1), true, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(1), Some(2), true, true), Ordering::Greater);
    }

    #[test]
    fn row_group_boundary_key_matches_logs_order() {
        let lower = RowGroupBoundaryKey {
            components: vec![
                RowGroupBoundaryComponent::string(Some("acc-1".to_string()), false, true),
                RowGroupBoundaryComponent::string(Some("svc-1".to_string()), false, true),
                RowGroupBoundaryComponent::timestamp_micros(Some(20), true, true),
            ],
        };
        let higher = RowGroupBoundaryKey {
            components: vec![
                RowGroupBoundaryComponent::string(Some("acc-1".to_string()), false, true),
                RowGroupBoundaryComponent::string(Some("svc-1".to_string()), false, true),
                RowGroupBoundaryComponent::timestamp_micros(Some(30), true, true),
            ],
        };
        let null_cloud = RowGroupBoundaryKey {
            components: vec![
                RowGroupBoundaryComponent::string(None, false, true),
                RowGroupBoundaryComponent::string(Some("svc-9".to_string()), false, true),
                RowGroupBoundaryComponent::timestamp_micros(Some(1), true, true),
            ],
        };

        assert_eq!(
            null_cloud.compare_checked(&lower).expect("compatible keys"),
            Ordering::Less
        );
        assert_eq!(higher.compare_checked(&lower).expect("compatible keys"), Ordering::Less);
        assert_eq!(
            lower.compare_checked(&higher).expect("compatible keys"),
            Ordering::Greater
        );
    }

    #[test]
    fn row_group_boundary_range_preserves_key_order() {
        let min_key = RowGroupBoundaryKey {
            components: vec![
                RowGroupBoundaryComponent::string(Some("acc-1".to_string()), false, true),
                RowGroupBoundaryComponent::string(Some("svc-a".to_string()), false, true),
                RowGroupBoundaryComponent::timestamp_micros(Some(50), true, true),
            ],
        };
        let max_key = RowGroupBoundaryKey {
            components: vec![
                RowGroupBoundaryComponent::string(Some("acc-1".to_string()), false, true),
                RowGroupBoundaryComponent::string(Some("svc-b".to_string()), false, true),
                RowGroupBoundaryComponent::timestamp_micros(Some(10), true, true),
            ],
        };
        let range = RowGroupBoundaryRange { min_key, max_key };

        assert_eq!(
            range.min_key.compare_checked(&range.max_key).expect("compatible keys"),
            Ordering::Less
        );
    }

    #[test]
    fn row_group_boundary_range_handles_equal_prefix_and_timestamp_desc() {
        let min_key = RowGroupBoundaryKey {
            components: vec![
                RowGroupBoundaryComponent::string(Some("acc-1".to_string()), false, true),
                RowGroupBoundaryComponent::string(Some("svc-1".to_string()), false, true),
                RowGroupBoundaryComponent::timestamp_micros(Some(30), true, true),
            ],
        };
        let max_key = RowGroupBoundaryKey {
            components: vec![
                RowGroupBoundaryComponent::string(Some("acc-1".to_string()), false, true),
                RowGroupBoundaryComponent::string(Some("svc-1".to_string()), false, true),
                RowGroupBoundaryComponent::timestamp_micros(Some(10), true, true),
            ],
        };

        assert_eq!(
            min_key.compare_checked(&max_key).expect("compatible keys"),
            Ordering::Less
        );
    }

    #[test]
    fn row_group_boundary_key_rejects_different_component_count() {
        let shorter = RowGroupBoundaryKey {
            components: vec![RowGroupBoundaryComponent::string(
                Some("acc-1".to_string()),
                false,
                true,
            )],
        };
        let longer = RowGroupBoundaryKey {
            components: vec![
                RowGroupBoundaryComponent::string(Some("acc-1".to_string()), false, true),
                RowGroupBoundaryComponent::string(Some("svc-1".to_string()), false, true),
            ],
        };

        let err = shorter
            .compare_checked(&longer)
            .expect_err("different component count must be rejected");
        assert!(err.to_string().contains("component count differs"));
    }

    #[test]
    fn row_group_boundary_key_rejects_different_component_flags() {
        let ascending = RowGroupBoundaryKey {
            components: vec![RowGroupBoundaryComponent::string(
                Some("acc-1".to_string()),
                false,
                true,
            )],
        };
        let descending = RowGroupBoundaryKey {
            components: vec![RowGroupBoundaryComponent::string(Some("acc-1".to_string()), true, true)],
        };

        let err = ascending
            .compare_checked(&descending)
            .expect_err("different descending flag must be rejected");
        assert!(err.to_string().contains("descending differs"));
    }

    #[test]
    fn row_group_boundary_key_rejects_different_component_types() {
        let string_key = RowGroupBoundaryKey {
            components: vec![RowGroupBoundaryComponent::string(
                Some("acc-1".to_string()),
                false,
                true,
            )],
        };
        let timestamp_key = RowGroupBoundaryKey {
            components: vec![RowGroupBoundaryComponent::timestamp_micros(Some(10), false, true)],
        };

        let err = string_key
            .compare_checked(&timestamp_key)
            .expect_err("different component types must be rejected");
        assert!(err.to_string().contains("value type differs"));
    }
}
