use std::{cmp::Ordering, collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, StringArray, TimestampMicrosecondArray, UInt32Array},
    compute::take,
    record_batch::RecordBatch,
};
use icegate_common::{LOGS_TOPIC, RowGroupBoundaryKey, RowGroupBoundaryRange, compare_option_ord};
use icegate_queue::{PreparedWalRowGroup, WriteChannel, WriteRequest, WriteResult};
use tokio::sync::oneshot;

use crate::error::{IngestError, Result};

// TODO(crit): remove hardcode
const LOGS_SORT_COLUMNS: [&str; 3] = ["cloud_account_id", "service_name", "timestamp"];

/// Summary of WAL write acknowledgements for one ingest request.
pub struct WalWriteSummary {
    /// WAL offset acknowledged for the request.
    pub offset: Option<u64>,
    /// Total rows acknowledged by the WAL writer.
    pub records: usize,
    /// Trace context returned by the WAL writer, if present.
    pub trace_context: Option<String>,
}

/// Partial WAL write result for one ingest request.
pub struct WalPartialWrite {
    /// Number of records rejected by the WAL writer.
    pub rejected_records: usize,
    /// Reason reported by the WAL writer.
    pub reason: String,
    /// Trace context returned by the WAL writer, if present.
    pub trace_context: Option<String>,
}

/// Final outcome after waiting for WAL acknowledgement.
pub enum WalAckOutcome {
    /// WAL fully accepted the request.
    Success(WalWriteSummary),
    /// WAL reported a partial failure for the request.
    Partial(WalPartialWrite),
}

/// Prepared WAL request ready to be submitted into the queue.
pub struct PreparedWalWrite {
    write_request: WriteRequest,
    response_rx: oneshot::Receiver<WriteResult>,
    records: usize,
}

/// Submitted WAL request waiting for writer acknowledgement.
pub struct PendingWalWrite {
    response_rx: oneshot::Receiver<WriteResult>,
    records: usize,
}

/// Ingest-specific pre-WAL sorter that preserves tenant-homogeneous row groups.
struct PreWalTenantSorter {
    row_group_size: usize,
}

struct TenantGroup {
    row_indices: Vec<u32>,
}

struct LogsSortColumnCache<'a> {
    cloud_account_ids: &'a StringArray,
    service_names: &'a StringArray,
    timestamps: &'a TimestampMicrosecondArray,
}

impl<'a> LogsSortColumnCache<'a> {
    fn new(batch: &'a RecordBatch) -> Result<Self> {
        Ok(Self {
            cloud_account_ids: string_column(batch, LOGS_SORT_COLUMNS[0], "WAL sorting")?,
            service_names: string_column(batch, LOGS_SORT_COLUMNS[1], "WAL sorting")?,
            timestamps: timestamp_column(batch, LOGS_SORT_COLUMNS[2], "WAL sorting")?,
        })
    }

    fn compare(&self, left: u32, right: u32) -> Ordering {
        let left = usize::try_from(left).expect("u32 row index fits into usize");
        let right = usize::try_from(right).expect("u32 row index fits into usize");

        let ordering = compare_option_ord(
            string_array_value(self.cloud_account_ids, left),
            string_array_value(self.cloud_account_ids, right),
            false,
            true,
        );
        if ordering != Ordering::Equal {
            return ordering;
        }

        let ordering = compare_option_ord(
            string_array_value(self.service_names, left),
            string_array_value(self.service_names, right),
            false,
            true,
        );
        if ordering != Ordering::Equal {
            return ordering;
        }

        let ordering = compare_option_ord(
            timestamp_array_value(self.timestamps, left),
            timestamp_array_value(self.timestamps, right),
            true,
            true,
        );
        if ordering != Ordering::Equal {
            return ordering;
        }

        left.cmp(&right)
    }

    fn boundary_range_for_sorted_rows(&self, row_indices: &[u32]) -> Result<RowGroupBoundaryRange> {
        let (&first_row_idx, &last_row_idx) = row_indices
            .first()
            .zip(row_indices.last())
            .ok_or_else(|| IngestError::Shift("cannot build boundary range from empty WAL row group".to_string()))?;
        logs_row_group_boundary_range_from_cached_columns(self, first_row_idx, last_row_idx)
    }
}

impl PreWalTenantSorter {
    /// Create a new sorter for the configured WAL row-group size.
    const fn new(row_group_size: usize) -> Self {
        Self { row_group_size }
    }

    /// Sort logs for WAL so that each output batch contains exactly one tenant.
    fn sort_logs(&self, batch: &RecordBatch) -> Result<Vec<PreparedWalRowGroup>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        if self.row_group_size == 0 {
            return Err(IngestError::Shift(
                "WAL row_group_size must be greater than zero".to_string(),
            ));
        }

        let tenant_idx = batch
            .schema()
            .index_of("tenant_id")
            .map_err(|err| IngestError::Shift(format!("logs batch is missing tenant_id: {err}")))?;
        let tenant_ids = batch
            .column(tenant_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IngestError::Shift("tenant_id must be Utf8 for WAL sorting".to_string()))?;
        let sort_columns = LogsSortColumnCache::new(batch)?;

        let mut tenant_groups = Vec::new();
        let mut tenant_group_idx_by_id = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            if tenant_ids.is_null(row_idx) {
                return Err(IngestError::Shift(
                    "tenant_id must be non-null before WAL sorting".to_string(),
                ));
            }
            let tenant_id = tenant_ids.value(row_idx);
            let row_idx =
                u32::try_from(row_idx).map_err(|_| IngestError::Shift("row index exceeds u32".to_string()))?;
            let group_idx = if let Some(group_idx) = tenant_group_idx_by_id.get(tenant_id).copied() {
                group_idx
            } else {
                let group_idx = tenant_groups.len();
                tenant_groups.push(TenantGroup {
                    row_indices: Vec::new(),
                });
                tenant_group_idx_by_id.insert(tenant_id, group_idx);
                group_idx
            };
            tenant_groups[group_idx].row_indices.push(row_idx);
        }

        let mut output = Vec::new();
        for tenant_group in &mut tenant_groups {
            tenant_group
                .row_indices
                .sort_unstable_by(|left, right| sort_columns.compare(*left, *right));
            for row_indices in tenant_group.row_indices.chunks(self.row_group_size) {
                let boundary_range = sort_columns.boundary_range_for_sorted_rows(row_indices)?;
                let row_group_batch = take_rows(batch, UInt32Array::from(row_indices.to_vec()))?;
                let metadata = serialize_logs_row_group_metadata(&boundary_range)?;
                output.push(PreparedWalRowGroup::new(row_group_batch).with_metadata(metadata));
            }
        }

        Ok(output)
    }
}

/// Prepare WAL batches for one ingest request.
pub fn prepare_sorted_logs_for_wal(
    batch: &RecordBatch,
    row_group_size: usize,
    trace_context: Option<String>,
) -> Result<Option<PreparedWalWrite>> {
    let row_groups = PreWalTenantSorter::new(row_group_size).sort_logs(batch)?;
    if row_groups.is_empty() {
        return Ok(None);
    }

    let records = row_groups.iter().map(|row_group| row_group.batch.num_rows()).sum();
    let (response_tx, response_rx) = oneshot::channel();
    Ok(Some(PreparedWalWrite {
        write_request: WriteRequest {
            topic: LOGS_TOPIC.to_string(),
            row_groups,
            response_tx,
            trace_context,
        },
        response_rx,
        records,
    }))
}

// TODO(crit): replace from wal_sort
/// Submit a prepared WAL request into the queue.
pub async fn submit_sorted_logs_to_wal(
    write_channel: &WriteChannel,
    prepared: PreparedWalWrite,
) -> Result<PendingWalWrite> {
    write_channel
        .send(prepared.write_request)
        .await
        .map_err(|err| IngestError::Io(std::io::Error::other(format!("WAL queue unavailable: {err}"))))?;

    Ok(PendingWalWrite {
        response_rx: prepared.response_rx,
        records: prepared.records,
    })
}

// TODO(crit): replace from wal_sort
impl PendingWalWrite {
    /// Wait for the WAL writer acknowledgement.
    pub async fn wait_for_ack(self) -> Result<WalAckOutcome> {
        let result = self.response_rx.await.map_err(|err| {
            IngestError::Io(std::io::Error::other(format!(
                "Failed to receive WAL write result: {err}"
            )))
        })?;
        match result {
            WriteResult::Success {
                offset,
                records,
                trace_context,
            } => Ok(WalAckOutcome::Success(WalWriteSummary {
                offset: Some(offset),
                records,
                trace_context,
            })),
            WriteResult::Failed { reason, trace_context } => Ok(WalAckOutcome::Partial(WalPartialWrite {
                rejected_records: self.records,
                reason,
                trace_context,
            })),
        }
    }
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

#[cfg(test)]
pub(crate) fn logs_row_group_boundary_range_from_batch(batch: &RecordBatch) -> Result<RowGroupBoundaryRange> {
    if batch.num_rows() == 0 {
        return Err(IngestError::Shift(
            "cannot build boundary range from empty WAL row group".to_string(),
        ));
    }
    let sort_columns = LogsSortColumnCache::new(batch)?;
    let last_row_idx =
        u32::try_from(batch.num_rows() - 1).map_err(|_| IngestError::Shift("row index exceeds u32".to_string()))?;
    logs_row_group_boundary_range_from_cached_columns(&sort_columns, 0, last_row_idx)
}

fn logs_row_group_boundary_range_from_cached_columns(
    columns: &LogsSortColumnCache<'_>,
    first_row_idx: u32,
    last_row_idx: u32,
) -> Result<RowGroupBoundaryRange> {
    let first_row_idx = usize::try_from(first_row_idx).expect("u32 row index fits into usize");
    let last_row_idx = usize::try_from(last_row_idx).expect("u32 row index fits into usize");
    let min_key = RowGroupBoundaryKey {
        cloud_account_id: string_array_value(columns.cloud_account_ids, first_row_idx).map(str::to_owned),
        service_name: string_array_value(columns.service_names, first_row_idx).map(str::to_owned),
        timestamp_micros: timestamp_array_value(columns.timestamps, first_row_idx),
    };
    let max_key = RowGroupBoundaryKey {
        cloud_account_id: string_array_value(columns.cloud_account_ids, last_row_idx).map(str::to_owned),
        service_name: string_array_value(columns.service_names, last_row_idx).map(str::to_owned),
        timestamp_micros: timestamp_array_value(columns.timestamps, last_row_idx),
    };

    if min_key.compare(&max_key) == std::cmp::Ordering::Greater {
        return Err(IngestError::Shift(
            "invalid WAL row-group boundary range: min_key must be <= max_key".to_string(),
        ));
    }

    Ok(RowGroupBoundaryRange { min_key, max_key })
}

pub(crate) fn serialize_logs_row_group_metadata(boundary_range: &RowGroupBoundaryRange) -> Result<String> {
    serde_json::to_string(boundary_range)
        .map_err(|err| IngestError::Shift(format!("failed to serialize WAL row-group metadata: {err}")))
}

pub(crate) fn deserialize_logs_row_group_metadata(metadata: &str) -> Result<RowGroupBoundaryRange> {
    let value: serde_json::Value = serde_json::from_str(metadata)
        .map_err(|err| IngestError::Shift(format!("failed to deserialize WAL row-group metadata: {err}")))?;
    let object = value.as_object().ok_or_else(|| {
        IngestError::Shift("failed to deserialize WAL row-group metadata: expected JSON object".to_string())
    })?;
    for key in ["min_key", "max_key"] {
        if !object.contains_key(key) {
            return Err(IngestError::Shift(format!(
                "failed to deserialize WAL row-group metadata: missing field '{key}'"
            )));
        }
    }
    let range: RowGroupBoundaryRange = serde_json::from_value(value)
        .map_err(|err| IngestError::Shift(format!("failed to deserialize WAL row-group metadata: {err}")))?;
    if range.min_key.compare(&range.max_key) == std::cmp::Ordering::Greater {
        return Err(IngestError::Shift(
            "failed to deserialize WAL row-group metadata: min_key must be <= max_key".to_string(),
        ));
    }
    Ok(range)
}

fn string_column<'a>(batch: &'a RecordBatch, column_name: &str, context: &str) -> Result<&'a StringArray> {
    let column_idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|err| IngestError::Shift(format!("logs batch is missing {column_name}: {err}")))?;
    batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| IngestError::Shift(format!("{column_name} must be Utf8 for {context}")))
}

fn timestamp_column<'a>(
    batch: &'a RecordBatch,
    column_name: &str,
    context: &str,
) -> Result<&'a TimestampMicrosecondArray> {
    let column_idx = batch
        .schema()
        .index_of(column_name)
        .map_err(|err| IngestError::Shift(format!("logs batch is missing {column_name}: {err}")))?;
    batch
        .column(column_idx)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| IngestError::Shift(format!("{column_name} must be Timestamp(Microsecond) for {context}")))
}

fn string_array_value<'a>(values: &'a StringArray, row_idx: usize) -> Option<&'a str> {
    (!values.is_null(row_idx)).then(|| values.value(row_idx))
}

fn timestamp_array_value(values: &TimestampMicrosecondArray, row_idx: usize) -> Option<i64> {
    (!values.is_null(row_idx)).then(|| values.value(row_idx))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use icegate_common::{RowGroupBoundaryKey, RowGroupBoundaryRange};
    use icegate_queue::{WriteResult, channel};

    use super::{
        PreWalTenantSorter, WalAckOutcome, deserialize_logs_row_group_metadata,
        logs_row_group_boundary_range_from_batch, prepare_sorted_logs_for_wal, serialize_logs_row_group_metadata,
        submit_sorted_logs_to_wal,
    };
    use crate::error::IngestError;

    fn logs_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("cloud_account_id", DataType::Utf8, true),
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
                    Some("acc-2"),
                    Some("acc-2"),
                    Some("acc-1"),
                    Some("acc-1"),
                    None,
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
        let row_ids = batch.column(4).as_any().downcast_ref::<Int64Array>().expect("row_id");
        (0..batch.num_rows()).map(|row_idx| row_ids.value(row_idx)).collect()
    }

    fn cloud_account_values(batch: &RecordBatch) -> Vec<Option<String>> {
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cloud_account_id");
        (0..batch.num_rows())
            .map(|row_idx| (!values.is_null(row_idx)).then(|| values.value(row_idx).to_string()))
            .collect()
    }

    fn service_values(batch: &RecordBatch) -> Vec<Option<String>> {
        let values = batch.column(2).as_any().downcast_ref::<StringArray>().expect("service_name");
        (0..batch.num_rows())
            .map(|row_idx| (!values.is_null(row_idx)).then(|| values.value(row_idx).to_string()))
            .collect()
    }

    fn timestamp_values(batch: &RecordBatch) -> Vec<Option<i64>> {
        let values = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");
        (0..batch.num_rows())
            .map(|row_idx| (!values.is_null(row_idx)).then(|| values.value(row_idx)))
            .collect()
    }

    #[test]
    fn pre_wal_sorter_splits_mixed_tenants_into_homogeneous_batches() {
        let batches = PreWalTenantSorter::new(2).sort_logs(&logs_batch()).expect("sort logs");

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
        let batches = PreWalTenantSorter::new(8).sort_logs(&logs_batch()).expect("sort logs");
        let tenant_a = &batches[1].batch;

        assert_eq!(
            cloud_account_values(tenant_a),
            vec![None, Some("acc-1".to_string()), Some("acc-2".to_string())]
        );
        assert_eq!(
            service_values(tenant_a),
            vec![
                Some("svc-0".to_string()),
                Some("svc-2".to_string()),
                Some("svc-1".to_string())
            ]
        );
        assert_eq!(timestamp_values(tenant_a), vec![Some(50), Some(30), Some(20)]);
        assert_eq!(row_ids(tenant_a), vec![4, 2, 1]);
    }

    #[test]
    fn pre_wal_sorter_outputs_single_tenant_per_batch() {
        let batches = PreWalTenantSorter::new(1).sort_logs(&logs_batch()).expect("sort logs");

        for batch in batches {
            let tenant_ids = tenant_values(&batch.batch);
            assert!(tenant_ids.iter().all(|tenant_id| tenant_id == &tenant_ids[0]));
        }
    }

    #[test]
    fn pre_wal_sorter_preserves_input_tenant_order() {
        let batches = PreWalTenantSorter::new(2).sort_logs(&logs_batch()).expect("sort logs");

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
            Field::new("cloud_account_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("row_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("acc-1"), Some("acc-1"), Some("acc-1")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1"), Some("svc-1"), Some("svc-1")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(10), Some(10), Some(10)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![11, 12, 13])) as ArrayRef,
            ],
        )
        .expect("logs batch");

        let batches = PreWalTenantSorter::new(8).sort_logs(&batch).expect("sort logs");

        assert_eq!(batches.len(), 1);
        assert_eq!(row_ids(&batches[0].batch), vec![11, 12, 13]);
    }

    #[test]
    fn pre_wal_sorter_keeps_order_across_multiple_row_groups_for_same_tenant() {
        let batches = PreWalTenantSorter::new(2).sort_logs(&logs_batch()).expect("sort logs");

        assert_eq!(row_ids(&batches[1].batch), vec![4, 2]);
        assert_eq!(row_ids(&batches[2].batch), vec![1]);
    }

    #[test]
    fn pre_wal_sorter_metadata_matches_final_row_group_edges() {
        let batches = PreWalTenantSorter::new(2).sort_logs(&logs_batch()).expect("sort logs");

        let ranges = batches
            .iter()
            .map(|row_group| {
                let metadata = row_group.metadata.as_deref().expect("metadata");
                deserialize_logs_row_group_metadata(metadata).expect("deserialize metadata")
            })
            .collect::<Vec<_>>();

        assert_eq!(
            ranges[0],
            RowGroupBoundaryRange {
                min_key: RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: None,
                    timestamp_micros: Some(40),
                },
                max_key: RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-2".to_string()),
                    service_name: Some("svc-2".to_string()),
                    timestamp_micros: Some(10),
                },
            }
        );
        assert_eq!(
            ranges[1],
            RowGroupBoundaryRange {
                min_key: RowGroupBoundaryKey {
                    cloud_account_id: None,
                    service_name: Some("svc-0".to_string()),
                    timestamp_micros: Some(50),
                },
                max_key: RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-1".to_string()),
                    service_name: Some("svc-2".to_string()),
                    timestamp_micros: Some(30),
                },
            }
        );
        assert_eq!(
            ranges[2],
            RowGroupBoundaryRange {
                min_key: RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-2".to_string()),
                    service_name: Some("svc-1".to_string()),
                    timestamp_micros: Some(20),
                },
                max_key: RowGroupBoundaryKey {
                    cloud_account_id: Some("acc-2".to_string()),
                    service_name: Some("svc-1".to_string()),
                    timestamp_micros: Some(20),
                },
            }
        );
    }

    #[test]
    fn logs_row_group_boundary_range_roundtrips_for_sorted_batch_edges() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant_id", DataType::Utf8, false),
                Field::new("cloud_account_id", DataType::Utf8, true),
                Field::new("service_name", DataType::Utf8, true),
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Field::new("row_id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("acc-2"), Some("acc-2"), Some("acc-3")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1"), Some("svc-1"), Some("svc-0")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(20), Some(10), Some(30)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )
        .expect("sorted logs batch");
        let expected = RowGroupBoundaryRange {
            min_key: RowGroupBoundaryKey {
                cloud_account_id: Some("acc-2".to_string()),
                service_name: Some("svc-1".to_string()),
                timestamp_micros: Some(20),
            },
            max_key: RowGroupBoundaryKey {
                cloud_account_id: Some("acc-3".to_string()),
                service_name: Some("svc-0".to_string()),
                timestamp_micros: Some(30),
            },
        };

        let range = logs_row_group_boundary_range_from_batch(&batch).expect("boundary range");
        let metadata = serialize_logs_row_group_metadata(&range).expect("serialize metadata");
        let restored = deserialize_logs_row_group_metadata(&metadata).expect("deserialize metadata");

        assert_eq!(range, expected);
        assert_eq!(restored, expected);
    }

    #[test]
    fn logs_row_group_boundary_range_ensures_min_key_lte_max_key() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant_id", DataType::Utf8, false),
                Field::new("cloud_account_id", DataType::Utf8, true),
                Field::new("service_name", DataType::Utf8, true),
                Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Field::new("row_id", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("acc-2"), Some("acc-2")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1"), Some("svc-1")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(20), Some(10)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .expect("sorted logs batch");
        let range = logs_row_group_boundary_range_from_batch(&batch).expect("boundary range");

        assert_ne!(range.min_key.compare(&range.max_key), std::cmp::Ordering::Greater);
    }

    #[test]
    fn logs_row_group_boundary_range_preserves_nulls_in_keys() {
        let batch = logs_batch().slice(4, 1);
        let range = logs_row_group_boundary_range_from_batch(&batch).expect("boundary range");

        assert_eq!(range.min_key.cloud_account_id, None);
        assert_eq!(range.max_key.cloud_account_id, None);
        assert_eq!(range.min_key.service_name.as_deref(), Some("svc-0"));
        assert_eq!(range.max_key.service_name.as_deref(), Some("svc-0"));
    }

    #[test]
    fn logs_row_group_boundary_range_works_for_equal_account_service_with_timestamp_desc() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("cloud_account_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("row_id", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["tenant-a", "tenant-a"])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("acc-1"), Some("acc-1")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1"), Some("svc-1")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(30), Some(10)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            ],
        )
        .expect("logs batch");

        let range = logs_row_group_boundary_range_from_batch(&batch).expect("boundary range");
        assert_eq!(range.min_key.timestamp_micros, Some(30));
        assert_eq!(range.max_key.timestamp_micros, Some(10));
        assert_ne!(range.min_key.compare(&range.max_key), std::cmp::Ordering::Greater);
    }

    #[test]
    fn deserialize_logs_row_group_metadata_rejects_missing_boundary_fields() {
        let err = deserialize_logs_row_group_metadata("{}").expect_err("metadata must be rejected");
        assert!(err.to_string().contains("missing field"));
    }

    #[test]
    fn prepare_sorted_logs_for_wal_fails_when_tenant_column_is_missing() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("cloud_account_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("acc-1")])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("svc-1")])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![Some(10)])) as ArrayRef,
            ],
        )
        .expect("logs batch");

        let error = match prepare_sorted_logs_for_wal(&batch, 4, None) {
            Ok(_) => panic!("expected prepare failure"),
            Err(error) => error,
        };
        assert!(matches!(error, IngestError::Shift(_)));
    }

    #[tokio::test]
    async fn submit_sorted_logs_to_wal_sends_one_logical_request() {
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

        let prepared = prepare_sorted_logs_for_wal(&batch, 4, Some("trace-request".to_string()))
            .expect("prepare wal write")
            .expect("prepared wal write");
        let pending = submit_sorted_logs_to_wal(&tx, prepared).await.expect("submit wal write");
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

        let prepared = prepare_sorted_logs_for_wal(&batch, 4, Some("trace-request".to_string()))
            .expect("prepare wal write")
            .expect("prepared wal write");
        let pending = submit_sorted_logs_to_wal(&tx, prepared).await.expect("submit wal write");
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
}
