use std::time::{Duration, Instant};

use icegate_queue::{WriteChannel, WriteResult};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::oneshot;
use tokio::time::timeout;

use crate::error::{IngestError, Result};

/// Hard deadline for a single ingest request to wait on its durable WAL
/// acknowledgement. Sits under the `OpenTelemetry` Collector's default 5 s
/// exporter timeout so that, when the WAL genuinely stalls, ingest sheds load
/// with a retryable status *before* the collector cancels the request and drops
/// the batch non-gracefully (GH-158).
pub(crate) const WAL_ACK_TIMEOUT: Duration = Duration::from_secs(3);

/// Emit a `warn!` when a WAL acknowledgement wait exceeds this threshold but
/// still completes under [`WAL_ACK_TIMEOUT`]. Surfaces the slow-but-succeeding
/// tail that precedes load-shedding on the otherwise metrics-only HTTP path.
const WAL_ACK_WARN_THRESHOLD: Duration = Duration::from_secs(2);

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
///
/// Uses a non-blocking [`try_send`](tokio::sync::mpsc::Sender::try_send): when
/// the bounded channel is full the ingest node is genuinely behind, so this
/// returns [`IngestError::QueueFull`] (retryable) to shed load rather than block
/// the caller until its client timeout fires (GH-158). A closed channel is a
/// shutdown-time condition and maps to an I/O error.
pub(crate) fn submit_sorted_rows_to_wal(
    write_channel: &WriteChannel,
    prepared: PreparedWalWrite,
) -> Result<PendingWalWrite> {
    match write_channel.try_send(prepared.write_request) {
        Ok(()) => Ok(PendingWalWrite {
            response_rx: prepared.response_rx,
            records: prepared.records,
        }),
        Err(TrySendError::Full(_)) => Err(IngestError::QueueFull),
        Err(TrySendError::Closed(_)) => Err(IngestError::Io(std::io::Error::other(
            "WAL queue unavailable: channel closed",
        ))),
    }
}

impl PendingWalWrite {
    /// Wait for the WAL writer acknowledgement, bounded by [`WAL_ACK_TIMEOUT`].
    ///
    /// Returns [`IngestError::AckTimeout`] (retryable) when the durable ack does
    /// not arrive within the deadline, so a stuck WAL sheds load gracefully
    /// instead of hanging until the client times out. A slow-but-successful ack
    /// (over [`WAL_ACK_WARN_THRESHOLD`]) is logged at `warn!`.
    pub async fn wait_for_ack(self) -> Result<WalAckOutcome> {
        let ack_start = Instant::now();
        let received = match timeout(WAL_ACK_TIMEOUT, self.response_rx).await {
            Ok(received) => received,
            Err(_elapsed) => {
                tracing::warn!(
                    timeout = ?WAL_ACK_TIMEOUT,
                    records = self.records,
                    "WAL acknowledgement deadline exceeded; shedding load (retryable)"
                );
                return Err(IngestError::AckTimeout);
            }
        };

        let ack_elapsed = ack_start.elapsed();
        if ack_elapsed >= WAL_ACK_WARN_THRESHOLD {
            tracing::warn!(elapsed = ?ack_elapsed, records = self.records, "slow WAL acknowledgement");
        }

        let result = received.map_err(|err| {
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
