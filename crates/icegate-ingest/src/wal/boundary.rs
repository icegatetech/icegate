use std::{cmp::Ordering, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::error::{IngestError, Result};

/// Scalar value stored in a row-group boundary key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RowGroupBoundaryValue {
    /// UTF-8 string value.
    String(String),
    /// Timestamp value in microseconds.
    TimestampMicros(i64),
    /// Fixed-length raw bytes (e.g. `trace_id`: 16 B, `span_id`: 8 B).
    FixedBytes(Vec<u8>),
}

/// Inclusive boundary range for one sorted row group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct RowGroupBoundaryRange {
    /// Sort-field column names, parallel to `min_key.components` / `max_key.components`.
    pub(crate) names: Arc<[String]>,
    /// First key in the row group.
    pub(crate) min_key: RowGroupBoundaryKey,
    /// Last key in the row group.
    pub(crate) max_key: RowGroupBoundaryKey,
}

impl RowGroupBoundaryRange {
    /// Validate that boundary keys and `names` share the same arity and that the range is
    /// non-decreasing.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.names.is_empty() {
            return Err(IngestError::Shift(
                "row group boundary range names must not be empty".to_string(),
            ));
        }
        if self.names.len() != self.min_key.components().len() || self.names.len() != self.max_key.components().len() {
            return Err(IngestError::Shift(format!(
                "row group boundary range arity mismatch: names={}, min_key={}, max_key={}",
                self.names.len(),
                self.min_key.components().len(),
                self.max_key.components().len()
            )));
        }
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
    components: Vec<RowGroupBoundaryComponent>,
}

impl RowGroupBoundaryKey {
    /// Create a new boundary key from the given components.
    pub(crate) const fn new(components: Vec<RowGroupBoundaryComponent>) -> Self {
        Self { components }
    }

    /// Return all sort-field components as a slice.
    pub(crate) fn components(&self) -> &[RowGroupBoundaryComponent] {
        &self.components
    }

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
            (Some(RowGroupBoundaryValue::FixedBytes(left)), Some(RowGroupBoundaryValue::FixedBytes(right)))
                if left.len() != right.len() =>
            {
                Err(IngestError::Shift(format!(
                    "incompatible boundary key structure at component {index}: fixed_bytes width differs ({} bytes, {} bytes)",
                    left.len(),
                    right.len()
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
            (Some(RowGroupBoundaryValue::FixedBytes(left)), Some(RowGroupBoundaryValue::FixedBytes(right))) => {
                compare_option_ord(
                    Some(left.as_slice()),
                    Some(right.as_slice()),
                    self.descending,
                    self.nulls_first,
                )
            }
            (None, None) | (Some(_), Some(_)) => Ordering::Equal,
        }
    }
}

impl RowGroupBoundaryValue {
    pub(crate) const fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::TimestampMicros(_) => "timestamp_micros",
            Self::FixedBytes(_) => "fixed_bytes",
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
        RowGroupBoundaryValue::FixedBytes(_) => 2,
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, sync::Arc};

    use super::{RowGroupBoundaryKey, RowGroupBoundaryRange, compare_option_ord};
    use crate::error::IngestError;
    use crate::wal::test_utils::{
        boundary_component_fixed_bytes, boundary_component_string, boundary_component_timestamp_micros,
    };

    #[test]
    fn compare_option_ord_handles_desc_nulls_first() {
        assert_eq!(compare_option_ord(None::<i64>, Some(1), true, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(2), Some(1), true, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(1), Some(2), true, true), Ordering::Greater);
    }

    #[test]
    fn row_group_boundary_key_matches_logs_order() {
        let lower = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-1".to_string()), false, true),
            boundary_component_timestamp_micros(Some(20), true, true),
        ]);
        let higher = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-1".to_string()), false, true),
            boundary_component_timestamp_micros(Some(30), true, true),
        ]);
        let null_service = RowGroupBoundaryKey::new(vec![
            boundary_component_string(None, false, true),
            boundary_component_timestamp_micros(Some(1), true, true),
        ]);

        assert_eq!(
            null_service.compare_checked(&lower).expect("compatible keys"),
            Ordering::Less
        );
        assert_eq!(higher.compare_checked(&lower).expect("compatible keys"), Ordering::Less);
        assert_eq!(
            lower.compare_checked(&higher).expect("compatible keys"),
            Ordering::Greater
        );
    }

    fn names() -> Arc<[String]> {
        Arc::from(["service_name".to_string(), "timestamp".to_string()])
    }

    #[test]
    fn row_group_boundary_range_preserves_key_order() {
        let min_key = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-a".to_string()), false, true),
            boundary_component_timestamp_micros(Some(50), true, true),
        ]);
        let max_key = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-b".to_string()), false, true),
            boundary_component_timestamp_micros(Some(10), true, true),
        ]);
        let range = RowGroupBoundaryRange {
            names: names(),
            min_key,
            max_key,
        };

        assert_eq!(
            range.min_key.compare_checked(&range.max_key).expect("compatible keys"),
            Ordering::Less
        );
    }

    #[test]
    fn row_group_boundary_range_rejects_names_arity_mismatch() {
        let range = RowGroupBoundaryRange {
            names: Arc::from(["timestamp".to_string()]),
            min_key: RowGroupBoundaryKey::new(vec![
                boundary_component_string(Some("svc-1".to_string()), false, true),
                boundary_component_timestamp_micros(Some(10), true, true),
            ]),
            max_key: RowGroupBoundaryKey::new(vec![
                boundary_component_string(Some("svc-1".to_string()), false, true),
                boundary_component_timestamp_micros(Some(20), true, true),
            ]),
        };
        let err = range.validate().expect_err("arity mismatch must be rejected");
        assert!(err.to_string().contains("arity mismatch"));
    }

    #[test]
    fn row_group_boundary_range_rejects_empty_names() {
        let range = RowGroupBoundaryRange {
            names: Arc::from([]),
            min_key: RowGroupBoundaryKey::new(Vec::new()),
            max_key: RowGroupBoundaryKey::new(Vec::new()),
        };
        let err = range.validate().expect_err("empty names must be rejected");
        assert!(err.to_string().contains("must not be empty"));
    }

    #[test]
    fn row_group_boundary_range_handles_equal_prefix_and_timestamp_desc() {
        let min_key = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-1".to_string()), false, true),
            boundary_component_timestamp_micros(Some(30), true, true),
        ]);
        let max_key = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-1".to_string()), false, true),
            boundary_component_timestamp_micros(Some(10), true, true),
        ]);

        assert_eq!(
            min_key.compare_checked(&max_key).expect("compatible keys"),
            Ordering::Less
        );
    }

    #[test]
    fn row_group_boundary_key_rejects_different_component_count() {
        let shorter = RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc-1".to_string()), false, true)]);
        let longer = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("acc-1".to_string()), false, true),
            boundary_component_string(Some("svc-1".to_string()), false, true),
        ]);

        let err = shorter
            .compare_checked(&longer)
            .expect_err("different component count must be rejected");
        assert!(err.to_string().contains("component count differs"));
    }

    #[test]
    fn row_group_boundary_key_rejects_different_component_flags() {
        let ascending =
            RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc-1".to_string()), false, true)]);
        let descending =
            RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc-1".to_string()), true, true)]);

        let err = ascending
            .compare_checked(&descending)
            .expect_err("different descending flag must be rejected");
        assert!(err.to_string().contains("descending differs"));
    }

    #[test]
    fn row_group_boundary_key_rejects_different_component_types() {
        let string_key =
            RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc-1".to_string()), false, true)]);
        let timestamp_key = RowGroupBoundaryKey::new(vec![boundary_component_timestamp_micros(Some(10), false, true)]);

        let err = string_key
            .compare_checked(&timestamp_key)
            .expect_err("different component types must be rejected");
        assert!(err.to_string().contains("value type differs"));
    }

    #[test]
    fn row_group_boundary_component_compares_fixed_bytes_lex() {
        // trace_id-style 16-byte sentinels: trace_a1 < trace_a2 < trace_b
        // when compared lexicographically.
        let trace_a1: Vec<u8> = vec![b'a', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_a2: Vec<u8> = vec![b'a', 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_b: Vec<u8> = vec![b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        let lower = RowGroupBoundaryKey {
            components: vec![boundary_component_fixed_bytes(Some(trace_a1), false, true)],
        };
        let middle = RowGroupBoundaryKey {
            components: vec![boundary_component_fixed_bytes(Some(trace_a2.clone()), false, true)],
        };
        let upper = RowGroupBoundaryKey {
            components: vec![boundary_component_fixed_bytes(Some(trace_b), false, true)],
        };

        assert_eq!(lower.compare_checked(&middle).expect("compatible keys"), Ordering::Less);
        assert_eq!(middle.compare_checked(&upper).expect("compatible keys"), Ordering::Less);
        // Descending direction reverses the order.
        let middle_desc = RowGroupBoundaryKey {
            components: vec![boundary_component_fixed_bytes(Some(trace_a2), true, true)],
        };
        let upper_desc = RowGroupBoundaryKey {
            components: vec![boundary_component_fixed_bytes(
                Some(vec![b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
                true,
                true,
            )],
        };
        assert_eq!(
            middle_desc.compare_checked(&upper_desc).expect("compatible keys"),
            Ordering::Greater
        );
    }

    #[test]
    fn row_group_boundary_key_rejects_mixed_string_and_fixed_bytes() {
        let string_key = RowGroupBoundaryKey {
            components: vec![boundary_component_string(Some("acc-1".to_string()), false, true)],
        };
        let fixed_key = RowGroupBoundaryKey {
            components: vec![boundary_component_fixed_bytes(
                Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                false,
                true,
            )],
        };

        let err = string_key
            .compare_checked(&fixed_key)
            .expect_err("string vs fixed_bytes must be rejected");
        assert!(err.to_string().contains("value type differs"));
    }

    #[test]
    fn row_group_boundary_key_rejects_mismatched_fixed_bytes_widths() {
        // A 16-byte (trace_id-shaped) component compared against an 8-byte
        // (span_id-shaped) component must fail compatibility — silently
        // accepting the mismatch lets `compare_unchecked` lex-compare slices
        // of unequal length and produce nonsense ordering across streams.
        let key_16 = RowGroupBoundaryKey {
            components: vec![boundary_component_fixed_bytes(Some(vec![0u8; 16]), false, true)],
        };
        let key_8 = RowGroupBoundaryKey {
            components: vec![boundary_component_fixed_bytes(Some(vec![0u8; 8]), false, true)],
        };

        match key_16.compare_checked(&key_8) {
            Ok(_) => panic!("mismatched fixed_bytes widths must be rejected"),
            Err(err) => assert!(matches!(err, IngestError::Shift(_))),
        }
    }

    #[test]
    fn row_group_boundary_value_round_trips_fixed_bytes_json() {
        use crate::wal::{deserialize_row_group_boundary_range, serialize_row_group_boundary_range};

        let original = RowGroupBoundaryRange {
            names: Arc::from([
                "service_name".to_string(),
                "trace_id".to_string(),
                "timestamp".to_string(),
            ]),
            min_key: RowGroupBoundaryKey {
                components: vec![
                    boundary_component_string(Some("svc-1".to_string()), false, true),
                    boundary_component_fixed_bytes(
                        Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                        false,
                        true,
                    ),
                    boundary_component_timestamp_micros(Some(30), true, true),
                ],
            },
            max_key: RowGroupBoundaryKey {
                components: vec![
                    boundary_component_string(Some("svc-1".to_string()), false, true),
                    boundary_component_fixed_bytes(
                        Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 99]),
                        false,
                        true,
                    ),
                    boundary_component_timestamp_micros(Some(10), true, true),
                ],
            },
        };

        let serialized = serialize_row_group_boundary_range(&original).expect("serialize");
        let restored = deserialize_row_group_boundary_range(&serialized).expect("deserialize");
        assert_eq!(restored, original);
    }
}
