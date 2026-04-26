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
                PlannerPartitionField::DayTimestampColumnStats { plan_field } => {
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
        shift::planner::{MICROS_PER_DAY, PartitionBucket, PartitionValue, RowGroupPartition},
        wal::{
            RowGroupBoundaryKey, RowGroupBoundaryRange, serialize_row_group_boundary_range,
            test_utils::boundary_component_string,
        },
    };

    fn minimal_boundary_payload() -> String {
        serialize_row_group_boundary_range(&RowGroupBoundaryRange {
            names: Arc::from(["cloud_account_id".to_string()]),
            min_key: RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc".to_string()), false, true)]),
            max_key: RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc".to_string()), false, true)]),
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
        assert!(err.to_string().contains("missing tenant_id field"));
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
        assert!(err.to_string().contains("empty tenant_id"));
    }

    #[test]
    fn missing_boundary_payload_is_error() {
        let err = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(Some("tenant-a"), None, Some((1, 2)))])
            .expect_err("missing boundary must error");
        assert!(err.to_string().contains("missing boundary_range payload"));
    }

    #[test]
    fn missing_timestamp_range_is_cross_partition() {
        // Column stats absent (e.g. all-null timestamp column) → no proof of
        // single-day containment, fall back to cross-partition overflow.
        let row_groups = CURRENT_PLANNER_PARTITION_SPEC
            .plan_entries_to_row_groups(vec![plan_entry(
                Some("tenant-a"),
                Some(minimal_boundary_payload()),
                None,
            )])
            .expect("ok");
        assert!(matches!(
            &row_groups[0].partition,
            RowGroupPartition::CrossPartition(bucket)
                if bucket == &PartitionBucket(vec![PartitionValue::String("tenant-a".to_string())])
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
                    PartitionValue::String("tenant-a".to_string()),
                    PartitionValue::Day(0)
                ])
        ));
    }

    #[test]
    fn row_group_crossing_day_boundary_is_cross_partition() {
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
                if bucket == &PartitionBucket(vec![PartitionValue::String("tenant-a".to_string())])
        ));
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
