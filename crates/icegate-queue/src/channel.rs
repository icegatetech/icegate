//! Channel types for queue communication.

use arrow::record_batch::RecordBatch;
use tokio::sync::{mpsc, oneshot};

/// Topic identifier (user-defined string).
pub type Topic = String;

/// Channel for sending write requests to the queue writer.
pub type WriteChannel = mpsc::Sender<WriteRequest>;

/// Receiver end of the write channel.
pub type WriteReceiver = mpsc::Receiver<WriteRequest>;

/// Message sent to the queue writer.
#[derive(Debug)]
pub struct WriteRequest {
    /// Topic name for this batch.
    pub topic: Topic,

    /// Arrow `RecordBatch` to write.
    pub batch: RecordBatch,

    /// Optional column name to group by when creating row groups.
    ///
    /// If specified, the batch will be sorted by this column and
    /// each unique value will be written as a separate row group.
    /// This enables efficient row group pruning on reads.
    pub group_by_column: Option<String>,

    /// Channel to send the write result back to the caller.
    pub response_tx: oneshot::Sender<WriteResult>,

    /// W3C trace context for distributed tracing (traceparent header format).
    ///
    /// Format: `{version}-{trace-id}-{parent-id}-{trace-flags}`
    /// Example: `00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01`
    pub trace_context: Option<String>,
}

/// Result of a write operation.
#[derive(Debug, Clone)]
pub enum WriteResult {
    /// Write succeeded.
    Success {
        /// Offset of the written segment.
        offset: u64,
        /// Number of records written.
        records: usize,
        /// W3C trace context for distributed tracing (traceparent header format).
        trace_context: Option<String>,
    },
    /// Write failed.
    Failed {
        /// Reason for failure.
        reason: String,
        /// W3C trace context for distributed tracing (traceparent header format).
        trace_context: Option<String>,
    },
}

impl WriteResult {
    /// Creates a success result.
    #[must_use]
    pub const fn success(offset: u64, records: usize, trace_context: Option<String>) -> Self {
        Self::Success {
            offset,
            records,
            trace_context,
        }
    }

    /// Creates a failure result.
    #[must_use]
    pub fn failed(reason: impl Into<String>, trace_context: Option<String>) -> Self {
        Self::Failed {
            reason: reason.into(),
            trace_context,
        }
    }

    /// Returns true if the write succeeded.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Success { .. })
    }

    /// Returns true if the write failed.
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self, Self::Failed { .. })
    }

    /// Returns the offset if successful.
    #[must_use]
    pub const fn offset(&self) -> Option<u64> {
        match self {
            Self::Success { offset, .. } => Some(*offset),
            Self::Failed { .. } => None,
        }
    }

    /// Returns the record count if successful.
    #[must_use]
    pub const fn records(&self) -> Option<usize> {
        match self {
            Self::Success { records, .. } => Some(*records),
            Self::Failed { .. } => None,
        }
    }

    /// Returns the failure reason if failed.
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Success { .. } => None,
            Self::Failed { reason, .. } => Some(reason),
        }
    }

    /// Returns the trace context if present.
    #[must_use]
    pub const fn trace_context(&self) -> &Option<String> {
        match self {
            Self::Success { trace_context, .. } | Self::Failed { trace_context, .. } => trace_context,
        }
    }
}

/// Creates a new write channel pair.
#[must_use]
pub fn channel(capacity: usize) -> (WriteChannel, WriteReceiver) {
    mpsc::channel(capacity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_result_success() {
        let result = WriteResult::success(42, 100, None);
        assert!(result.is_success());
        assert!(!result.is_failed());
        assert_eq!(result.offset(), Some(42));
        assert_eq!(result.records(), Some(100));
        assert_eq!(result.reason(), None);
        assert_eq!(result.trace_context(), &None);
    }

    #[test]
    fn test_write_result_success_with_trace() {
        let trace_ctx = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());
        let result = WriteResult::success(42, 100, trace_ctx.clone());
        assert!(result.is_success());
        assert_eq!(result.trace_context(), &trace_ctx);
    }

    #[test]
    fn test_write_result_failed() {
        let result = WriteResult::failed("connection timeout", None);
        assert!(!result.is_success());
        assert!(result.is_failed());
        assert_eq!(result.offset(), None);
        assert_eq!(result.records(), None);
        assert_eq!(result.reason(), Some("connection timeout"));
        assert_eq!(result.trace_context(), &None);
    }

    #[test]
    fn test_write_result_failed_with_trace() {
        let trace_ctx = Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());
        let result = WriteResult::failed("connection timeout", trace_ctx.clone());
        assert!(result.is_failed());
        assert_eq!(result.trace_context(), &trace_ctx);
    }
}
