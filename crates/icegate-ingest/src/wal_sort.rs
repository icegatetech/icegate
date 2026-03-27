use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{Array, StringArray, UInt32Array},
    compute::{SortColumn, SortOptions, lexsort_to_indices, take},
    record_batch::RecordBatch,
};
use icegate_common::LOGS_TOPIC;
use icegate_queue::{WriteChannel, WriteRequest, WriteResult};
use tokio::sync::oneshot;

use crate::error::{IngestError, Result};

// TODO(crit): нужно брать из схемы, а не хардкодить
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

impl PreWalTenantSorter {
    /// Create a new sorter for the configured WAL row-group size.
    const fn new(row_group_size: usize) -> Self {
        Self { row_group_size }
    }

    /// Sort logs for WAL so that each output batch contains exactly one tenant.
    fn sort_logs(&self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
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

        let mut row_idxs_by_tenant: HashMap<String, Vec<u32>> = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            if tenant_ids.is_null(row_idx) {
                return Err(IngestError::Shift(
                    "tenant_id must be non-null before WAL sorting".to_string(),
                ));
            }
            let tenant_id = tenant_ids.value(row_idx).to_string();
            let row_idx =
                u32::try_from(row_idx).map_err(|_| IngestError::Shift("row index exceeds u32".to_string()))?;
            row_idxs_by_tenant.entry(tenant_id).or_default().push(row_idx);
        }

        let mut output = Vec::new();
        for tenant_row_idxs in row_idxs_by_tenant.into_values() {
            // TODO(crit): можно ли обойтись без take?
            let tenant_batch = take_rows(batch, UInt32Array::from(tenant_row_idxs))?;
            let sorted_tenant_batch = sort_single_tenant_logs(&tenant_batch)?;
            let mut offset = 0usize;
            while offset < sorted_tenant_batch.num_rows() {
                // TODO(crit): точно ли нужно?
                let len = self.row_group_size.min(sorted_tenant_batch.num_rows() - offset);
                output.push(sorted_tenant_batch.slice(offset, len));
                offset += len;
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
    let batches = PreWalTenantSorter::new(row_group_size).sort_logs(batch)?;
    if batches.is_empty() {
        return Ok(None);
    }

    let records = batches.iter().map(RecordBatch::num_rows).sum();
    let (response_tx, response_rx) = oneshot::channel();
    Ok(Some(PreparedWalWrite {
        write_request: WriteRequest {
            topic: LOGS_TOPIC.to_string(),
            batches,
            response_tx,
            trace_context,
        },
        response_rx,
        records,
    }))
}

// TODO(crit): при чем тут wal_sort?
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

// TODO(crit): почему это тут?
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

fn sort_single_tenant_logs(batch: &RecordBatch) -> Result<RecordBatch> {
    // TODO(crit): нельзя ли оптимизировать?

    let sort_columns = LOGS_SORT_COLUMNS
        .iter()
        .map(|column_name| {
            let column_idx = batch
                .schema()
                .index_of(column_name)
                .map_err(|err| IngestError::Shift(format!("logs batch is missing {column_name}: {err}")))?;
            let descending = *column_name == "timestamp";
            Ok(SortColumn {
                values: batch.column(column_idx).clone(),
                options: Some(SortOptions {
                    descending,
                    nulls_first: true,
                }),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let indices = lexsort_to_indices(&sort_columns, None)?;
    let sorted_columns = batch
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(RecordBatch::try_new(batch.schema(), sorted_columns)?)
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
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, StringArray, TimestampMicrosecondArray},
        datatypes::{DataType, Field, Schema, TimeUnit},
        record_batch::RecordBatch,
    };
    use icegate_queue::{WriteResult, channel};

    use super::{PreWalTenantSorter, WalAckOutcome, prepare_sorted_logs_for_wal, submit_sorted_logs_to_wal};
    use crate::error::IngestError;

    fn logs_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tenant_id", DataType::Utf8, false),
            Field::new("cloud_account_id", DataType::Utf8, true),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), true),
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

    #[test]
    fn pre_wal_sorter_splits_mixed_tenants_into_homogeneous_batches() {
        let batches = PreWalTenantSorter::new(2).sort_logs(&logs_batch()).expect("sort logs");

        assert_eq!(batches.len(), 3);
        let tenant_batches = batches.iter().map(tenant_values).collect::<Vec<_>>();
        assert!(tenant_batches.contains(&vec!["tenant-a".to_string(), "tenant-a".to_string()]));
        assert!(tenant_batches.contains(&vec!["tenant-a".to_string()]));
        assert!(tenant_batches.contains(&vec!["tenant-b".to_string(), "tenant-b".to_string()]));
    }

    #[test]
    fn pre_wal_sorter_keeps_logs_sort_order_inside_tenant() {
        let batches = PreWalTenantSorter::new(8).sort_logs(&logs_batch()).expect("sort logs");
        let tenant_a = batches
            .iter()
            .find(|batch| tenant_values(batch).first().is_some_and(|tenant_id| tenant_id == "tenant-a"))
            .expect("tenant-a batch");
        let cloud_account_id = tenant_a
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("cloud_account_id");
        let service_name = tenant_a.column(2).as_any().downcast_ref::<StringArray>().expect("service_name");
        let timestamps = tenant_a
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");

        assert_eq!(cloud_account_id.is_null(0), true);
        assert_eq!(service_name.value(0), "svc-0");
        assert_eq!(cloud_account_id.value(1), "acc-1");
        assert_eq!(timestamps.value(1), 30);
        assert_eq!(cloud_account_id.value(2), "acc-2");
        assert_eq!(timestamps.value(2), 20);
    }

    #[test]
    fn pre_wal_sorter_outputs_single_tenant_per_batch() {
        let batches = PreWalTenantSorter::new(1).sort_logs(&logs_batch()).expect("sort logs");

        for batch in batches {
            let tenant_ids = tenant_values(&batch);
            assert!(tenant_ids.iter().all(|tenant_id| tenant_id == &tenant_ids[0]));
        }
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
            assert_eq!(request.batches.len(), 2);
            let total_rows = request.batches.iter().map(RecordBatch::num_rows).sum::<usize>();
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
