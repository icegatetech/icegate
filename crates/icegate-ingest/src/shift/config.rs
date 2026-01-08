//! Shift configuration.

use icegate_queue::QueueConfig;
use serde::{Deserialize, Serialize};

/// Configuration for the shift process.
///
/// Controls how data is moved from the queue to Iceberg tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ShiftConfig {
    /// Queue configuration for reading segments.
    pub queue: QueueConfig,

    /// Parquet row group size (number of rows per row group).
    pub row_group_size: usize,

    /// Maximum file size in MB before rolling to a new file.
    pub max_file_size_mb: usize,

    /// Timeout in seconds to flush and commit when no new data arrives.
    pub flush_timeout_secs: u64,

    /// Polling interval in milliseconds when waiting for new segments.
    pub poll_interval_ms: u64,
}

impl Default for ShiftConfig {
    fn default() -> Self {
        Self {
            queue: QueueConfig::default(),
            row_group_size: 10_000,
            max_file_size_mb: 100,
            flush_timeout_secs: 60,
            poll_interval_ms: 1000,
        }
    }
}

impl ShiftConfig {
    /// Creates a new shift configuration with the given queue base path.
    #[must_use]
    pub fn new(queue_base_path: impl Into<String>) -> Self {
        Self {
            queue: QueueConfig::new(queue_base_path),
            ..Default::default()
        }
    }

    /// Sets the row group size.
    #[must_use]
    pub const fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size;
        self
    }

    /// Sets the maximum file size in MB.
    #[must_use]
    pub const fn with_max_file_size_mb(mut self, size_mb: usize) -> Self {
        self.max_file_size_mb = size_mb;
        self
    }

    /// Sets the flush timeout in seconds.
    #[must_use]
    pub const fn with_flush_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.flush_timeout_secs = timeout_secs;
        self
    }

    /// Sets the poll interval in milliseconds.
    #[must_use]
    pub const fn with_poll_interval_ms(mut self, interval_ms: u64) -> Self {
        self.poll_interval_ms = interval_ms;
        self
    }

    /// Returns the maximum file size in bytes.
    #[must_use]
    pub const fn max_file_size_bytes(&self) -> usize {
        self.max_file_size_mb * 1024 * 1024
    }
}
