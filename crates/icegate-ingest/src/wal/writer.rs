use icegate_queue::{WriteChannel, WriteResult};
use tokio::sync::oneshot;

use crate::error::{IngestError, Result};

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
pub(crate) struct PreparedWalWrite {
    pub(crate) write_request: icegate_queue::WriteRequest,
    pub(crate) response_rx: oneshot::Receiver<WriteResult>,
    pub(crate) records: usize,
}

/// Submitted WAL request waiting for writer acknowledgement.
pub(crate) struct PendingWalWrite {
    response_rx: oneshot::Receiver<WriteResult>,
    records: usize,
}

/// Submit a prepared WAL request (logs or spans) into the queue.
pub(crate) async fn submit_sorted_rows_to_wal(
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
