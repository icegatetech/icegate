use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, StringArray, UInt32Array},
    compute::take,
    record_batch::RecordBatch,
};
use icegate_common::{LOGS_TOPIC, METRICS_TOPIC, OPERATIONS_TOPIC, SPANS_TOPIC};
use icegate_queue::{PreparedWalRowGroup, WriteRequest};

use super::{
    RowGroupBoundaryRange, SortColumnCache, SortColumnsDescriptor, metadata::serialize_row_group_boundary_range,
    writer::PreparedWalWrite,
};
use crate::error::{IngestError, Result};

/// Ingest-specific pre-WAL sorter that preserves tenant-homogeneous row groups.
struct WalSorter {
    row_group_size: usize,
}

impl WalSorter {
    /// Create a new sorter for the configured WAL row-group size.
    const fn new(row_group_size: usize) -> Self {
        Self { row_group_size }
    }

    /// Sort a batch for WAL so that each output batch contains exactly one tenant.
    fn sort(
        &self,
        descriptor: &'static SortColumnsDescriptor,
        batch: &RecordBatch,
    ) -> Result<Vec<PreparedWalRowGroup>> {
        struct TenantGroup {
            row_indices: Vec<usize>,
        }

        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        if self.row_group_size == 0 {
            return Err(IngestError::Shift(
                "WAL row_group_size must be greater than zero".to_string(),
            ));
        }

        // TODO(high): it's fragile, what if the partitioning key (tenant_id) changes
        let tenant_idx = batch
            .schema()
            .index_of("tenant_id")
            .map_err(|err| IngestError::Shift(format!("batch is missing tenant_id: {err}")))?;
        let tenant_id_column = batch
            .column(tenant_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IngestError::Shift("tenant_id must be Utf8 for WAL sorting".to_string()))?;
        let sort_columns = SortColumnCache::try_new(batch, descriptor, "WAL sorting")?;

        let mut tenant_groups = Vec::new();
        let mut tenant_group_idx_by_id = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            if tenant_id_column.is_null(row_idx) {
                return Err(IngestError::Shift(
                    "tenant_id must be non-null before WAL sorting".to_string(),
                ));
            }
            let tenant_id = tenant_id_column.value(row_idx);
            let group_idx = tenant_group_idx_by_id.get(tenant_id).copied().unwrap_or_else(|| {
                let group_idx = tenant_groups.len();
                tenant_groups.push(TenantGroup {
                    row_indices: Vec::new(),
                });
                tenant_group_idx_by_id.insert(tenant_id, group_idx);
                group_idx
            });
            tenant_groups[group_idx].row_indices.push(row_idx);
        }

        let mut output = Vec::new();
        for tenant_group in &mut tenant_groups {
            tenant_group
                .row_indices
                .sort_unstable_by(|left, right| sort_columns.compare_indices(*left, *right));
            for row_indices in tenant_group.row_indices.chunks(self.row_group_size) {
                let (&first_row_idx, &last_row_idx) = row_indices.first().zip(row_indices.last()).ok_or_else(|| {
                    IngestError::Shift("cannot build boundary range from empty WAL row group".to_string())
                })?;
                let boundary_range =
                    row_group_boundary_range_from_cached_columns(&sort_columns, first_row_idx, last_row_idx)?;
                let row_indices = row_indices
                    .iter()
                    .copied()
                    .map(|row_idx| {
                        u32::try_from(row_idx).map_err(|_| IngestError::Shift("row index exceeds u32".to_string()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let row_group_batch = Self::take_rows(batch, UInt32Array::from(row_indices))?;
                let metadata = serialize_row_group_boundary_range(&boundary_range)?;
                output.push(PreparedWalRowGroup::new(row_group_batch).with_metadata(metadata));
            }
        }

        Ok(output)
    }

    fn take_rows(batch: &RecordBatch, row_idxs: UInt32Array) -> Result<RecordBatch> {
        let row_idxs = Arc::new(row_idxs);
        let columns = batch
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), row_idxs.as_ref(), None))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }
}

/// Prepare WAL batches for one logs ingest request.
pub(crate) fn sort_logs(
    batch: &RecordBatch,
    row_group_size: usize,
    trace_context: Option<String>,
) -> Result<Option<PreparedWalWrite>> {
    prepare(
        SortColumnsDescriptor::logs()?,
        LOGS_TOPIC,
        batch,
        row_group_size,
        trace_context,
    )
}

/// Prepare WAL batches for one spans ingest request.
pub(crate) fn sort_spans(
    batch: &RecordBatch,
    row_group_size: usize,
    trace_context: Option<String>,
) -> Result<Option<PreparedWalWrite>> {
    prepare(
        SortColumnsDescriptor::spans()?,
        SPANS_TOPIC,
        batch,
        row_group_size,
        trace_context,
    )
}

/// Prepare WAL batches for one metrics ingest request.
pub(crate) fn sort_metrics(
    batch: &RecordBatch,
    row_group_size: usize,
    trace_context: Option<String>,
) -> Result<Option<PreparedWalWrite>> {
    prepare(
        SortColumnsDescriptor::metrics()?,
        METRICS_TOPIC,
        batch,
        row_group_size,
        trace_context,
    )
}

/// Prepare WAL batches for one operations ingest request.
pub(crate) fn sort_operations(
    batch: &RecordBatch,
    row_group_size: usize,
    trace_context: Option<String>,
) -> Result<Option<PreparedWalWrite>> {
    prepare(
        SortColumnsDescriptor::operations()?,
        OPERATIONS_TOPIC,
        batch,
        row_group_size,
        trace_context,
    )
}

fn prepare(
    descriptor: &'static SortColumnsDescriptor,
    topic: &'static str,
    batch: &RecordBatch,
    row_group_size: usize,
    trace_context: Option<String>,
) -> Result<Option<PreparedWalWrite>> {
    let row_groups = WalSorter::new(row_group_size).sort(descriptor, batch)?;
    if row_groups.is_empty() {
        return Ok(None);
    }

    let records = row_groups.iter().map(|row_group| row_group.batch.num_rows()).sum();
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    Ok(Some(PreparedWalWrite {
        write_request: WriteRequest {
            topic: topic.to_string(),
            row_groups,
            response_tx,
            trace_context,
        },
        response_rx,
        records,
    }))
}

fn row_group_boundary_range_from_cached_columns(
    columns: &SortColumnCache,
    first_row_idx: usize,
    last_row_idx: usize,
) -> Result<RowGroupBoundaryRange> {
    let min_key = columns.boundary_key(first_row_idx);
    let max_key = columns.boundary_key(last_row_idx);
    let range = RowGroupBoundaryRange {
        names: columns.column_names(),
        min_key,
        max_key,
    };
    range.validate().map_err(|err| match err {
        IngestError::Shift(message) => IngestError::Shift(format!("invalid WAL row-group boundary range: {message}")),
        other => other,
    })?;

    Ok(range)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, FixedSizeBinaryArray, Int64Array, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use icegate_queue::{WriteResult, channel};

    use super::{WalSorter, sort_logs, sort_metrics, sort_operations, sort_spans};
    use crate::error::IngestError;
    use crate::wal::{
        RowGroupBoundaryKey, RowGroupBoundaryRange, RowGroupBoundaryValue, SortColumnsDescriptor, WalAckOutcome,
        deserialize_row_group_boundary_range, serialize_row_group_boundary_range, submit_sorted_rows_to_wal,
        test_utils::{
            boundary_component_string, boundary_component_timestamp_micros, logs_row_group_boundary_range_from_batch,
        },
    };

    fn logs_descriptor() -> &'static SortColumnsDescriptor {
        SortColumnsDescriptor::logs().expect("logs descriptor")
    }

    fn logs_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("row_id", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "tenant-b", "tenant-a", "tenant-a", "tenant-b", "tenant-a",
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("svc-2"),
                    Some("svc-1"),
                    Some("svc-2"),
                    None,
                    Some("svc-0"),
                ])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(10),
                    Some(20),
                    Some(30),
                    Some(40),
                    Some(50),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
            ],
        )
        .expect("logs batch")
    }

    fn tenant_values(batch: &RecordBatch) -> Vec<String> {
        let tenant_ids = batch.column(0).as_any().downcast_ref::<StringArray>().expect("tenant_id");
        (0..batch.num_rows())
            .map(|row_idx| tenant_ids.value(row_idx).to_string())
            .collect()
    }

    fn row_ids(batch: &RecordBatch) -> Vec<i64> {
        let row_ids = batch.column(3).as_any().downcast_ref::<Int64Array>().expect("row_id");
        (0..batch.num_rows()).map(|row_idx| row_ids.value(row_idx)).collect()
    }

    fn service_values(batch: &RecordBatch) -> Vec<Option<String>> {
        let values = batch.column(1).as_any().downcast_ref::<StringArray>().expect("service_name");
        (0..batch.num_rows())
            .map(|row_idx| (!values.is_null(row_idx)).then(|| values.value(row_idx).to_string()))
            .collect()
    }

    fn timestamp_values(batch: &RecordBatch) -> Vec<Option<i64>> {
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");
        (0..batch.num_rows())
            .map(|row_idx| (!values.is_null(row_idx)).then(|| values.value(row_idx)))
            .collect()
    }

    fn logs_names() -> std::sync::Arc<[String]> {
        std::sync::Arc::from(["service_name".to_string(), "timestamp".to_string()])
    }

    fn key(service_name: Option<&str>, timestamp_micros: Option<i64>) -> RowGroupBoundaryKey {
        RowGroupBoundaryKey::new(vec![
            boundary_component_string(service_name.map(str::to_string), false, true),
            boundary_component_timestamp_micros(timestamp_micros, true, true),
        ])
    }

    #[test]
    fn pre_wal_sorter_splits_mixed_tenants_into_homogeneous_batches() {
        let batches = WalSorter::new(2).sort(logs_descriptor(), &logs_batch()).expect("sort logs");

        assert_eq!(batches.len(), 3);
        assert_eq!(
            tenant_values(&batches[0].batch),
            vec!["tenant-b".to_string(), "tenant-b".to_string()]
        );
        assert_eq!(
            tenant_values(&batches[1].batch),
            vec!["tenant-a".to_string(), "tenant-a".to_string()]
        );
        assert_eq!(tenant_values(&batches[2].batch), vec!["tenant-a".to_string()]);
    }

    #[test]
    fn pre_wal_sorter_keeps_logs_sort_order_inside_tenant() {
        let batches = WalSorter::new(8).sort(logs_descriptor(), &logs_batch()).expect("sort logs");
        let tenant_a = &batches[1].batch;

        assert_eq!(
            service_values(tenant_a),
            vec![
                Some("svc-0".to_string()),
                Some("svc-1".to_string()),
                Some("svc-2".to_string())
            ]
        );
        assert_eq!(timestamp_values(tenant_a), vec![Some(50), Some(20), Some(30)]);
        assert_eq!(row_ids(tenant_a), vec![4, 1, 2]);
    }

    #[test]
    fn pre_wal_sorter_outputs_single_tenant_per_batch() {
        let batches = WalSorter::new(1).sort(logs_descriptor(), &logs_batch()).expect("sort logs");

        for batch in batches {
            let tenant_ids = tenant_values(&batch.batch);
            assert!(tenant_ids.iter().all(|tenant_id| tenant_id == &tenant_ids[0]));
        }
    }

    #[test]
    fn pre_wal_sorter_preserves_input_tenant_order() {
        let batches = WalSorter::new(2).sort(logs_descriptor(), &logs_batch()).expect("sort logs");

        let tenant_heads = batches
            .iter()
            .map(|row_group| tenant_values(&row_group.batch)[0].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            tenant_heads,
            vec!["tenant-b".to_string(), "tenant-a".to_string(), "tenant-a".to_string()]
        );
    }

    #[test]
    fn pre_wal_sorter_preserves_input_order_for_equal_sort_keys() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("row_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1"), Some("svc-1"), Some("svc-1")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(10), Some(10), Some(10)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![11, 12, 13])) as ArrayRef,
            ],
        )
        .expect("logs batch");

        let batches = WalSorter::new(8).sort(logs_descriptor(), &batch).expect("sort logs");

        assert_eq!(batches.len(), 1);
        assert_eq!(row_ids(&batches[0].batch), vec![11, 12, 13]);
    }

    #[test]
    fn pre_wal_sorter_keeps_order_across_multiple_row_groups_for_same_tenant() {
        let batches = WalSorter::new(2).sort(logs_descriptor(), &logs_batch()).expect("sort logs");

        assert_eq!(row_ids(&batches[1].batch), vec![4, 1]);
        assert_eq!(row_ids(&batches[2].batch), vec![2]);
    }

    #[test]
    fn pre_wal_sorter_metadata_matches_final_row_group_edges() {
        let batches = WalSorter::new(2).sort(logs_descriptor(), &logs_batch()).expect("sort logs");

        let ranges = batches
            .iter()
            .map(|row_group| {
                let metadata = row_group.metadata.as_deref().expect("metadata");
                deserialize_row_group_boundary_range(metadata).expect("deserialize metadata")
            })
            .collect::<Vec<_>>();

        assert_eq!(
            ranges[0],
            RowGroupBoundaryRange {
                names: logs_names(),
                min_key: key(None, Some(40)),
                max_key: key(Some("svc-2"), Some(10)),
            }
        );
        assert_eq!(
            ranges[1],
            RowGroupBoundaryRange {
                names: logs_names(),
                min_key: key(Some("svc-0"), Some(50)),
                max_key: key(Some("svc-1"), Some(20)),
            }
        );
        assert_eq!(
            ranges[2],
            RowGroupBoundaryRange {
                names: logs_names(),
                min_key: key(Some("svc-2"), Some(30)),
                max_key: key(Some("svc-2"), Some(30)),
            }
        );
    }

    #[test]
    fn logs_row_group_boundary_range_roundtrips_for_sorted_batch_edges() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant_id", DataType::Utf8, false),
                Field::new("service_name", DataType::Utf8, true),
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Field::new("row_id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1"), Some("svc-1"), Some("svc-2")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(20), Some(10), Some(30)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .expect("sorted logs batch");
        let expected = RowGroupBoundaryRange {
            names: logs_names(),
            min_key: key(Some("svc-1"), Some(20)),
            max_key: key(Some("svc-2"), Some(30)),
        };

        let range = logs_row_group_boundary_range_from_batch(&batch).expect("boundary range");
        let metadata = serialize_row_group_boundary_range(&range).expect("serialize metadata");
        let restored = deserialize_row_group_boundary_range(&metadata).expect("deserialize metadata");

        assert_eq!(range, expected);
        assert_eq!(restored, expected);
    }

    #[test]
    fn logs_row_group_boundary_range_ensures_min_key_lte_max_key() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant_id", DataType::Utf8, false),
                Field::new("service_name", DataType::Utf8, true),
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Field::new("row_id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1"), Some("svc-1")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(20), Some(10)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .expect("sorted logs batch");
        let range = logs_row_group_boundary_range_from_batch(&batch).expect("boundary range");

        assert_ne!(
            range.min_key.compare_checked(&range.max_key).expect("compatible keys"),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn logs_row_group_boundary_range_preserves_nulls_in_keys() {
        let batch = logs_batch().slice(3, 1);
        let range = logs_row_group_boundary_range_from_batch(&batch).expect("boundary range");

        assert_eq!(range.min_key.components()[0].value, None);
        assert_eq!(range.max_key.components()[0].value, None);
        assert_eq!(
            range.min_key.components()[1].value,
            Some(RowGroupBoundaryValue::TimestampMicros(40))
        );
        assert_eq!(
            range.max_key.components()[1].value,
            Some(RowGroupBoundaryValue::TimestampMicros(40))
        );
    }

    #[test]
    fn logs_row_group_boundary_range_works_for_equal_service_with_timestamp_desc() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("row_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1"), Some("svc-1")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(30), Some(10)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .expect("logs batch");

        let range = logs_row_group_boundary_range_from_batch(&batch).expect("boundary range");
        assert_eq!(
            range.min_key.components()[1].value,
            Some(RowGroupBoundaryValue::TimestampMicros(30))
        );
        assert_eq!(
            range.max_key.components()[1].value,
            Some(RowGroupBoundaryValue::TimestampMicros(10))
        );
        assert_ne!(
            range.min_key.compare_checked(&range.max_key).expect("compatible keys"),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn deserialize_logs_row_group_metadata_rejects_missing_boundary_fields() {
        let err = deserialize_row_group_boundary_range("{}").expect_err("metadata must be rejected");
        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn deserialize_logs_row_group_metadata_rejects_different_component_count() {
        let err = deserialize_row_group_boundary_range(
            r#"{
                "names": ["service_name"],
                "min_key": {"components":[{"value":{"String":"svc-1"},"descending":false,"nulls_first":true}]},
                "max_key": {"components":[
                    {"value":{"String":"svc-1"},"descending":false,"nulls_first":true},
                    {"value":{"String":"svc-2"},"descending":false,"nulls_first":true}
                ]}
            }"#,
        )
        .expect_err("metadata must be rejected");

        assert!(err.to_string().contains("arity mismatch"));
    }

    #[test]
    fn deserialize_logs_row_group_metadata_rejects_different_component_flags() {
        let err = deserialize_row_group_boundary_range(
            r#"{
                "names": ["service_name"],
                "min_key": {"components":[{"value":{"String":"svc-1"},"descending":false,"nulls_first":true}]},
                "max_key": {"components":[{"value":{"String":"svc-2"},"descending":true,"nulls_first":true}]}
            }"#,
        )
        .expect_err("metadata must be rejected");

        assert!(err.to_string().contains("descending differs"));
    }

    #[test]
    fn deserialize_logs_row_group_metadata_rejects_different_component_types() {
        let err = deserialize_row_group_boundary_range(
            r#"{
                "names": ["service_name"],
                "min_key": {"components":[{"value":{"String":"svc-1"},"descending":false,"nulls_first":true}]},
                "max_key": {"components":[{"value":{"TimestampMicros":10},"descending":false,"nulls_first":true}]}
            }"#,
        )
        .expect_err("metadata must be rejected");

        assert!(err.to_string().contains("value type differs"));
    }

    #[test]
    fn prepare_sorted_logs_for_wal_fails_when_tenant_column_is_missing() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("svc-1")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(10)])) as ArrayRef,
            ],
        )
        .expect("logs batch");

        let Err(error) = sort_logs(&batch, 4, None) else {
            panic!("expected prepare failure");
        };
        assert!(matches!(error, IngestError::Shift(_)));
    }

    #[tokio::test]
    async fn submit_sorted_rows_to_wal_sends_one_logical_request() {
        let batch = logs_batch();
        let (tx, mut rx) = channel(1);

        let writer = tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            assert_eq!(request.topic, icegate_common::LOGS_TOPIC);
            assert_eq!(request.row_groups.len(), 2);
            let total_rows = request
                .row_groups
                .iter()
                .map(|row_group| row_group.batch.num_rows())
                .sum::<usize>();
            assert_eq!(total_rows, 5);
            request
                .response_tx
                .send(WriteResult::success(7, total_rows, Some("trace-ack".to_string())))
                .expect("send ack");
        });

        let prepared = sort_logs(&batch, 4, Some("trace-request".to_string()))
            .expect("prepare wal write")
            .expect("prepared wal write");
        let pending = submit_sorted_rows_to_wal(&tx, prepared).await.expect("submit wal write");
        let summary = match pending.wait_for_ack().await.expect("wait for wal ack") {
            WalAckOutcome::Success(summary) => summary,
            WalAckOutcome::Partial(_) => panic!("unexpected partial wal write"),
        };
        writer.await.expect("writer task");

        assert_eq!(summary.offset, Some(7));
        assert_eq!(summary.records, 5);
        assert_eq!(summary.trace_context.as_deref(), Some("trace-ack"));
    }

    #[tokio::test]
    async fn wait_for_ack_returns_partial_failure() {
        let batch = logs_batch();
        let (tx, mut rx) = channel(1);

        let writer = tokio::spawn(async move {
            let request = rx.recv().await.expect("write request");
            request
                .response_tx
                .send(WriteResult::failed(
                    "partial wal failure",
                    Some("trace-ack".to_string()),
                ))
                .expect("send ack");
        });

        let prepared = sort_logs(&batch, 4, Some("trace-request".to_string()))
            .expect("prepare wal write")
            .expect("prepared wal write");
        let pending = submit_sorted_rows_to_wal(&tx, prepared).await.expect("submit wal write");
        let outcome = pending.wait_for_ack().await.expect("wait for wal ack");
        writer.await.expect("writer task");

        match outcome {
            WalAckOutcome::Success(_) => panic!("unexpected successful wal write"),
            WalAckOutcome::Partial(partial) => {
                assert_eq!(partial.rejected_records, 5);
                assert_eq!(partial.reason, "partial wal failure");
                assert_eq!(partial.trace_context.as_deref(), Some("trace-ack"));
            }
        }
    }

    #[test]
    fn sort_logs_regression_for_fixed_input() {
        // Fixed input. Do not mutate once this test is green; it is the contract
        // that the pipeline-generalization refactor must preserve.
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("row_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-b", "tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("svc-x"),
                    Some("svc-q"),
                    Some("svc-y"),
                    Some("svc-x"),
                ])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(100),
                    Some(200),
                    Some(50),
                    Some(75),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            ],
        )
        .expect("regression batch");

        let prepared = sort_logs(&batch, 8, None)
            .expect("prepare wal write")
            .expect("prepared wal write");

        assert_eq!(prepared.write_request.topic, icegate_common::LOGS_TOPIC);
        assert_eq!(prepared.write_request.row_groups.len(), 2);
        assert_eq!(prepared.records, 4);

        // Tenant-a group: 3 rows, sorted by (service_name asc, timestamp desc).
        let rg0 = &prepared.write_request.row_groups[0];
        assert_eq!(rg0.batch.num_rows(), 3);
        let row_ids_rg0: Vec<i64> = (0..rg0.batch.num_rows())
            .map(|i| {
                rg0.batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("row_id")
                    .value(i)
            })
            .collect();
        assert_eq!(row_ids_rg0, vec![1, 4, 3]); // svc-x/ts=100, svc-x/ts=75, svc-y/ts=50

        // Tenant-b group: 1 row.
        let rg1 = &prepared.write_request.row_groups[1];
        assert_eq!(rg1.batch.num_rows(), 1);
        let row_ids_rg1: Vec<i64> = (0..rg1.batch.num_rows())
            .map(|i| {
                rg1.batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("row_id")
                    .value(i)
            })
            .collect();
        assert_eq!(row_ids_rg1, vec![2]);

        // Boundary metadata is non-empty JSON for every row group.
        for rg in &prepared.write_request.row_groups {
            let metadata = rg.metadata.as_deref().expect("metadata");
            assert!(metadata.starts_with('{'));
            assert!(metadata.contains("\"min_key\""));
            assert!(metadata.contains("\"max_key\""));
        }
    }

    #[test]
    fn sort_spans_produces_spans_topic_and_tenant_homogeneous_groups() {
        // Build a minimal batch with only the fields the spans sort descriptor cares about:
        // tenant_id, service_name, trace_id, timestamp.
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            // `trace_id` is `FIXED_LEN_BYTE_ARRAY(16)` in storage; use raw
            // 16-byte values so the sort comparator picks the right path.
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("row_id", DataType::Int64, false),
        ]));
        // 16-byte sentinel values that preserve the prior alphabetic
        // ordering: the second byte distinguishes a1 < a2 < b for tenant-a's
        // two rows and tenant-b's row.
        let trace_a1: [u8; 16] = [b'a', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_a2: [u8; 16] = [b'a', 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_b: [u8; 16] = [b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_id_arr = FixedSizeBinaryArray::try_from_iter(
            [trace_b.as_slice(), trace_a2.as_slice(), trace_a1.as_slice()].into_iter(),
        )
        .expect("trace_id array");
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["tenant-b", "tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-2"), Some("svc-1"), Some("svc-1")])) as ArrayRef,
                Arc::new(trace_id_arr) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(30), Some(10), Some(20)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .expect("spans batch");

        let prepared = sort_spans(&batch, 8, None)
            .expect("prepare wal write")
            .expect("prepared wal write");

        assert_eq!(prepared.write_request.topic, icegate_common::SPANS_TOPIC);
        assert_eq!(prepared.write_request.row_groups.len(), 2);
        // Tenant-a group sorted by (service_name asc, trace_id asc, timestamp desc).
        // Both rows share svc-1, so sort by trace_id asc: row_id 3 (trace-a1), row_id 2 (trace-a2).
        let rg_a = prepared
            .write_request
            .row_groups
            .iter()
            .find(|rg| rg.batch.num_rows() == 2)
            .expect("tenant-a group");
        let row_ids: Vec<i64> = (0..rg_a.batch.num_rows())
            .map(|i| {
                rg_a.batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("row_id")
                    .value(i)
            })
            .collect();
        assert_eq!(row_ids, vec![3, 2]);
    }

    #[test]
    fn sort_metrics_produces_metrics_topic_and_tenant_homogeneous_groups() {
        use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

        // Minimal batch with the columns the metrics sort descriptor needs:
        // tenant_id, service_name, service_instance_id, metric_name, timestamp.
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("service_instance_id", DataType::Utf8, true),
            Field::new("metric_name", DataType::Utf8, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("row_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["tenant-b", "tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["svc", "svc", "svc"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("i-1"), Some("i-1"), Some("i-1")])) as ArrayRef,
                Arc::new(StringArray::from(vec!["m2", "m1", "m1"])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(30), Some(10), Some(20)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .expect("metrics batch");

        let prepared = sort_metrics(&batch, 8, None)
            .expect("prepare wal write")
            .expect("prepared wal write");

        assert_eq!(prepared.write_request.topic, icegate_common::METRICS_TOPIC);
        assert_eq!(prepared.write_request.row_groups.len(), 2); // tenant-a (2 rows) + tenant-b (1 row)

        // tenant-a rows share metric_name "m1"; sorted by timestamp DESC -> row_id 3 (ts=20) before row_id 2 (ts=10).
        let rg_a = prepared
            .write_request
            .row_groups
            .iter()
            .find(|rg| rg.batch.num_rows() == 2)
            .expect("tenant-a group");
        let row_ids: Vec<i64> = (0..rg_a.batch.num_rows())
            .map(|i| {
                rg_a.batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("row_id")
                    .value(i)
            })
            .collect();
        assert_eq!(row_ids, vec![3, 2]);
    }

    #[test]
    fn sort_operations_produces_operations_topic_and_tenant_homogeneous_groups() {
        // Operations sort order is (trace_id asc, timestamp desc) with no
        // service_name leg, so a minimal batch only needs tenant_id, trace_id,
        // and timestamp. `trace_id` is `FIXED_LEN_BYTE_ARRAY(16)` in storage.
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("row_id", DataType::Int64, false),
        ]));
        // tenant-a carries two rows sharing `trace_high` (to exercise the
        // `timestamp DESC` tiebreaker) plus one row on the lower `trace_low` (to
        // exercise the leading `trace_id ASC` leg). A wrong direction on either
        // leg changes the expected `row_id` order, so both are guarded.
        let trace_low: [u8; 16] = [b'a', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_high: [u8; 16] = [b'a', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_b: [u8; 16] = [b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_id_arr = FixedSizeBinaryArray::try_from_iter(
            [
                trace_high.as_slice(),
                trace_high.as_slice(),
                trace_low.as_slice(),
                trace_b.as_slice(),
            ]
            .into_iter(),
        )
        .expect("trace_id array");
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-a", "tenant-a", "tenant-b"])) as ArrayRef,
                Arc::new(trace_id_arr) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(10),
                    Some(20),
                    Some(5),
                    Some(30),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 4, 3])) as ArrayRef,
            ],
        )
        .expect("operations batch");

        let prepared = sort_operations(&batch, 8, None)
            .expect("prepare wal write")
            .expect("prepared wal write");

        assert_eq!(prepared.write_request.topic, icegate_common::OPERATIONS_TOPIC);
        assert_eq!(prepared.write_request.row_groups.len(), 2);

        let rg_a = prepared
            .write_request
            .row_groups
            .iter()
            .find(|rg| rg.batch.num_rows() == 3)
            .expect("tenant-a group");
        let row_ids: Vec<i64> = (0..rg_a.batch.num_rows())
            .map(|i| {
                rg_a.batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("row_id")
                    .value(i)
            })
            .collect();
        // trace_low (row_id 4) sorts ahead of trace_high; within trace_high the
        // later timestamp (row_id 2, ts=20) precedes the earlier (row_id 1, ts=10).
        assert_eq!(row_ids, vec![4, 2, 1]);
    }
}
