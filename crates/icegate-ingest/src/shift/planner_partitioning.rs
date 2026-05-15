//! Schema-aware adapter between queue planning metadata and the sort planner.

use icegate_queue::{ExtractField, FieldExtractor, RowGroupPlanEntry, WAL_ROW_GROUP_METADATA_KEY};

use super::planner::{PLAN_FIELD_BOUNDARY_RANGE, PlanRowGroup, PlannerPartitionField, plan_entries_to_row_groups};
use crate::error::Result;

/// Plan-entry field key for the tenant identifier extracted from row-group statistics.
pub(super) const PLAN_FIELD_TENANT_ID: &str = "tenant_id";
/// Plan-entry field key for the physical timestamp range extracted from
/// row-group column statistics.
pub(super) const PLAN_FIELD_TIMESTAMP_RANGE: &str = "timestamp_range";
const TENANT_ID_COLUMN: &str = "tenant_id";
const TIMESTAMP_COLUMN: &str = "timestamp";

static CURRENT_PARTITION_FIELDS: &[PlannerPartitionField] = &[
    PlannerPartitionField::IdentityUtf8ColumnStats {
        plan_field: PLAN_FIELD_TENANT_ID,
        column_name: TENANT_ID_COLUMN,
    },
    PlannerPartitionField::DayTimestampColumnStats {
        plan_field: PLAN_FIELD_TIMESTAMP_RANGE,
    },
];

/// Current explicit planner partition spec for IceGate observability tables.
pub static CURRENT_PLANNER_PARTITION_SPEC: PlannerPartitionSpec = PlannerPartitionSpec {
    fields: CURRENT_PARTITION_FIELDS,
};

/// Limited partition spec understood by the shift planner adapter.
pub struct PlannerPartitionSpec {
    fields: &'static [PlannerPartitionField],
}

impl PlannerPartitionSpec {
    /// Build the field extraction instructions for [`icegate_queue::QueueReader::plan_segments`].
    pub(super) fn extract_fields(&self) -> Vec<ExtractField> {
        let mut fields = Vec::with_capacity(self.fields.len().saturating_add(1));
        for field in self.fields {
            match field {
                PlannerPartitionField::IdentityUtf8ColumnStats {
                    plan_field,
                    column_name,
                } => {
                    fields.push(ExtractField {
                        name: (*plan_field).to_string(),
                        extractor: FieldExtractor::ColumnStatsUtf8Singleton {
                            column_name: (*column_name).to_string(),
                        },
                    });
                }
                PlannerPartitionField::DayTimestampColumnStats { plan_field, .. } => {
                    fields.push(ExtractField {
                        name: (*plan_field).to_string(),
                        extractor: FieldExtractor::ColumnStatsTimestampMicrosRange {
                            column_name: TIMESTAMP_COLUMN.to_string(),
                        },
                    });
                }
            }
        }
        fields.push(ExtractField {
            name: PLAN_FIELD_BOUNDARY_RANGE.to_string(),
            extractor: FieldExtractor::FileKeyValueRowGroupPayload {
                key: WAL_ROW_GROUP_METADATA_KEY.to_string(),
            },
        });
        fields
    }

    /// Convert queue plan entries into schema-agnostic planner row groups.
    ///
    /// # Errors
    ///
    /// Returns an error if required extracted fields or boundary components are
    /// missing, invalid, empty, or use unsupported value types.
    pub(super) fn plan_entries_to_row_groups(&self, entries: Vec<RowGroupPlanEntry>) -> Result<Vec<PlanRowGroup>> {
        plan_entries_to_row_groups(self.fields, entries)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use icegate_queue::ExtractedValue;

    use super::*;
    use crate::{
        error::IngestError,
        shift::planner::{MICROS_PER_DAY, PartitionBucket, PartitionValue, RowGroupPartition},
        wal::{
            RowGroupBoundaryKey, RowGroupBoundaryRange, serialize_row_group_boundary_range,
            test_utils::boundary_component_string,
        },
    };

    fn minimal_boundary_payload() -> String {
        serialize_row_group_boundary_range(&RowGroupBoundaryRange {
            names: Arc::from(["service_name".to_string()]),
            min_key: RowGroupBoundaryKey::new(vec![boundary_component_string(Some("svc".to_string()), false, true)]),
            max_key: RowGroupBoundaryKey::new(vec![boundary_component_string(Some("svc".to_string()), false, true)]),
        })
        .expect("serialize metadata")
    }

    fn plan_entry(
        tenant: Option<&str>,
        boundary_payload: Option<String>,
        timestamp_range: Option<(i64, i64)>,
    ) -> RowGroupPlanEntry {
        let mut extracted = HashMap::new();
        if let Some(tenant) = tenant {
            extracted.insert(
                PLAN_FIELD_TENANT_ID.to_string(),
                ExtractedValue::Utf8(tenant.to_string()),
            );
        }
        if let Some(payload) = boundary_payload {
            extracted.insert(PLAN_FIELD_BOUNDARY_RANGE.to_string(), ExtractedValue::Utf8(payload));
        }
        if let Some((min_ts, max_ts)) = timestamp_range {
            extracted.insert(
                PLAN_FIELD_TIMESTAMP_RANGE.to_string(),
                ExtractedValue::TimestampMicrosRange(min_ts, max_ts),
            );
        }
        RowGroupPlanEntry {
            wal_offset: 7,
            row_group_idx: 3,
            row_group_bytes: 11,
            extracted,
        }
    }

    #[test]
    fn planner_partition_spec_builds_extract_fields() {
        let fields = CURRENT_PLANNER_PARTITION_SPEC.extract_fields();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, PLAN_FIELD_TENANT_ID);
        assert_eq!(fields[1].name, PLAN_FIELD_TIMESTAMP_RANGE);
        assert_eq!(fields[2].name, PLAN_FIELD_BOUNDARY_RANGE);
    }

    #[test]
    fn missing_identity_partition_field_is_error() {
        let err = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(None, Some(minimal_boundary_payload()), Some((1, 2)))])
            .expect_err("missing identity must error");
        assert!(matches!(err, IngestError::Shift(_)));
    }

    #[test]
    fn empty_identity_partition_field_is_error() {
        let err = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(
                Some(""),
                Some(minimal_boundary_payload()),
                Some((1, 2)),
            )])
            .expect_err("empty identity must error");
        assert!(matches!(err, IngestError::Shift(_)));
    }

    #[test]
    fn missing_boundary_payload_is_error() {
        let err = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(Some("tenant-a"), None, Some((1, 2)))])
            .expect_err("missing boundary must error");
        assert!(matches!(err, IngestError::Shift(_)));
    }

    #[test]
    fn missing_timestamp_range_is_error_when_required() {
        // timestamp is required in all current IceGate tables: absent column
        // stats indicate a WAL writer bug or data corruption, not an expected
        // all-null column.  The plan must fail-fast rather than silently
        // degrading to cross-partition overflow and losing day boundaries.
        let err = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(
                Some("tenant-a"),
                Some(minimal_boundary_payload()),
                None,
            )])
            .expect_err("missing required timestamp stats must error");
        assert!(matches!(err, IngestError::Shift(_)));
    }

    /// A boundary payload that is present but not a valid serialized
    /// [`RowGroupBoundaryRange`] must surface as a planner-side error rather
    /// than being silently accepted (which would corrupt clustering).
    #[test]
    fn malformed_boundary_payload_is_error() {
        let mut entry = plan_entry(Some("tenant-a"), None, Some((1, 2)));
        entry.extracted.insert(
            PLAN_FIELD_BOUNDARY_RANGE.to_string(),
            ExtractedValue::Utf8("not-json".to_string()),
        );
        let err = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![entry])
            .expect_err("malformed boundary payload must error");
        assert!(matches!(err, IngestError::Shift(_)));
    }

    /// Wrong-type guard: `timestamp_range` extractor is declared as
    /// `ColumnStatsTimestampMicrosRange` but if a hypothetical adapter ever
    /// supplied a UTF-8 value, the planner must reject the row group instead
    /// of silently treating it as cross-partition.
    #[test]
    fn timestamp_range_with_utf8_value_is_error() {
        let mut entry = plan_entry(Some("tenant-a"), Some(minimal_boundary_payload()), None);
        entry.extracted.insert(
            PLAN_FIELD_TIMESTAMP_RANGE.to_string(),
            ExtractedValue::Utf8("not-a-range".to_string()),
        );
        let err = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![entry])
            .expect_err("utf8 in timestamp_range must error");
        assert!(matches!(err, IngestError::Shift(_)));
    }

    /// Wrong-type guard: identity tenant field declared as `Utf8` must reject
    /// a `TimestampMicrosRange` payload.
    #[test]
    fn tenant_id_with_timestamp_range_value_is_error() {
        let mut entry = plan_entry(None, Some(minimal_boundary_payload()), Some((1, 2)));
        entry.extracted.insert(
            PLAN_FIELD_TENANT_ID.to_string(),
            ExtractedValue::TimestampMicrosRange(1, 2),
        );
        let err = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![entry])
            .expect_err("timestamp in tenant_id must error");
        assert!(matches!(err, IngestError::Shift(_)));
    }

    /// Edge case: timestamps straddling the unix epoch must use Euclidean
    /// division so that `-1µs` lands in day `-1` and `0µs` in day `0`.  Plain
    /// integer division would put both in day `0` and silently merge data
    /// across the epoch boundary.
    #[test]
    fn negative_timestamp_just_before_epoch_belongs_to_day_minus_one() {
        let row_groups = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![
                plan_entry(Some("tenant-a"), Some(minimal_boundary_payload()), Some((-1, -1))),
                plan_entry(Some("tenant-a"), Some(minimal_boundary_payload()), Some((0, 0))),
            ])
            .expect("ok");
        assert!(matches!(
            &row_groups[0].partition,
            RowGroupPartition::Single(bucket)
                if bucket == &PartitionBucket(vec![
                    PartitionValue::String(Arc::from("tenant-a")),
                    PartitionValue::Day(-1),
                ])
        ));
        assert!(matches!(
            &row_groups[1].partition,
            RowGroupPartition::Single(bucket)
                if bucket == &PartitionBucket(vec![
                    PartitionValue::String(Arc::from("tenant-a")),
                    PartitionValue::Day(0),
                ])
        ));
    }

    /// Timestamps anchored exactly at 00:00:00.000 UTC must fall on the *new*
    /// day, not the previous one — `div_euclid(MICROS_PER_DAY)` is half-open
    /// `[d, d+1)` per Iceberg's day-transform contract. A row group pinned to
    /// midnight of day `d` is in day `d`; a row group whose `max_ts` touches
    /// midnight of day `d+1` while `min_ts` sits in day `d` straddles the
    /// boundary.
    #[test]
    fn day_boundary_at_exact_midnight_utc() {
        // min_ts and max_ts both at the day-1 midnight boundary → entirely in
        // day 1, single bucket.
        let row_groups = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(
                Some("tenant-a"),
                Some(minimal_boundary_payload()),
                Some((MICROS_PER_DAY, MICROS_PER_DAY)),
            )])
            .expect("ok");
        assert!(matches!(
            &row_groups[0].partition,
            RowGroupPartition::Single(bucket)
                if bucket == &PartitionBucket(vec![
                    PartitionValue::String(Arc::from("tenant-a")),
                    PartitionValue::Day(1),
                ])
        ));

        // max_ts pinned exactly to the day-1 midnight while min_ts is the last
        // microsecond of day 0 → cross-partition (day 0 → day 1).  This nails
        // the half-open semantics: max_ts == MICROS_PER_DAY belongs to day 1,
        // not day 0.
        let row_groups = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(
                Some("tenant-a"),
                Some(minimal_boundary_payload()),
                Some((MICROS_PER_DAY - 1, MICROS_PER_DAY)),
            )])
            .expect("ok");
        assert!(matches!(
            &row_groups[0].partition,
            RowGroupPartition::CrossPartition(bucket)
                if bucket == &PartitionBucket(vec![
                    PartitionValue::String(Arc::from("tenant-a")),
                    PartitionValue::Day(0),
                    PartitionValue::Day(1),
                ])
        ));
    }

    #[test]
    fn row_group_within_one_day_gets_tenant_day_bucket() {
        let row_groups = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(
                Some("tenant-a"),
                Some(minimal_boundary_payload()),
                Some((1, 2)),
            )])
            .expect("ok");
        assert!(matches!(
            &row_groups[0].partition,
            RowGroupPartition::Single(bucket)
                if bucket == &PartitionBucket(vec![
                    PartitionValue::String(Arc::from("tenant-a")),
                    PartitionValue::Day(0)
                ])
        ));
    }

    #[test]
    fn row_group_crossing_day_boundary_is_cross_partition_with_day_range_bucket() {
        // A cross-day row group must land in CrossPartition, and its bucket
        // must include (min_day, max_day) so that row groups straddling
        // different day pairs are never packed into the same chunk.
        let row_groups = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(
                Some("tenant-a"),
                Some(minimal_boundary_payload()),
                Some((MICROS_PER_DAY - 1, MICROS_PER_DAY + 1)),
            )])
            .expect("ok");
        assert!(matches!(
            &row_groups[0].partition,
            RowGroupPartition::CrossPartition(bucket)
                if bucket == &PartitionBucket(vec![
                    PartitionValue::String(Arc::from("tenant-a")),
                    PartitionValue::Day(0),
                    PartitionValue::Day(1),
                ])
        ));
    }

    #[test]
    fn cross_day_row_groups_straddling_different_days_have_distinct_buckets() {
        // Two row groups of the same tenant each crossing a day boundary, but
        // at different day pairs, must receive distinct CrossPartition buckets.
        // Without this isolation, the planner could pack them into one chunk
        // and the Iceberg writer would fan out to multiple day-partition files.
        let rg_day0_1 = plan_entry(
            Some("tenant-a"),
            Some(minimal_boundary_payload()),
            Some((MICROS_PER_DAY - 1, MICROS_PER_DAY + 1)), // crosses day 0→1
        );
        let rg_day1_2 = plan_entry(
            Some("tenant-a"),
            Some(minimal_boundary_payload()),
            Some((2 * MICROS_PER_DAY - 1, 2 * MICROS_PER_DAY + 1)), // crosses day 1→2
        );
        let row_groups = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![rg_day0_1, rg_day1_2])
            .expect("ok");
        let bucket_0_1 = PartitionBucket(vec![
            PartitionValue::String(Arc::from("tenant-a")),
            PartitionValue::Day(0),
            PartitionValue::Day(1),
        ]);
        let bucket_1_2 = PartitionBucket(vec![
            PartitionValue::String(Arc::from("tenant-a")),
            PartitionValue::Day(1),
            PartitionValue::Day(2),
        ]);
        assert!(matches!(&row_groups[0].partition, RowGroupPartition::CrossPartition(b) if b == &bucket_0_1));
        assert!(matches!(&row_groups[1].partition, RowGroupPartition::CrossPartition(b) if b == &bucket_1_2));
    }

    /// Regression: with composite sort key `(account, service, ts DESC)`, a
    /// row group can have boundary timestamps both in day0 while interior
    /// rows live in day1. Column stats expose the physical range and force
    /// cross-partition bucketing — the old boundary-derived path would have
    /// claimed Single(day0) and let Iceberg fanout split the chunk silently.
    #[test]
    fn heterogeneous_prefix_with_interior_day_crossing_uses_column_stats() {
        let row_groups = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(
                Some("tenant-a"),
                Some(minimal_boundary_payload()),
                // Interior row at MICROS_PER_DAY + 1 reflected in column max,
                // even though hypothetical boundary timestamps would both
                // sit in day 0 (e.g. 10 and 20).
                Some((10, MICROS_PER_DAY + 1)),
            )])
            .expect("ok");
        assert!(matches!(&row_groups[0].partition, RowGroupPartition::CrossPartition(_)));
    }
}
