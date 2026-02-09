//! Queue configuration.

use serde::{Deserialize, Serialize};

use crate::{QueueError, Result};

/// Queue read settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueReadConfig {}

/// Configuration for the queue.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueConfig {
    /// Common queue settings.
    pub common: QueueCommonConfig,
    /// Queue write settings.
    pub write: QueueWriteConfig,
    /// Queue read settings.
    pub read: QueueReadConfig,
}

/// Parquet compression codec.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionCodec {
    /// No compression.
    None,
    /// Snappy compression.
    Snappy,
    /// Gzip compression.
    Gzip,
    /// LZO compression.
    Lzo,
    /// Brotli compression.
    Brotli,
    /// LZ4 compression.
    Lz4,
    /// ZSTD compression (default).
    #[default]
    Zstd,
}

/// Common queue settings shared by read and write paths.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueCommonConfig {
    /// Base path for queue segments (e.g., "<s3://bucket/queue>" or "<file:///var/data>").
    pub base_path: String,

    /// Capacity of the write channel (bounded for backpressure).
    pub channel_capacity: usize,

    /// Maximum number of rows in a single parquet row group for WAL segments.
    /// It is better to synchronize with `ShiftWriteConfig::row_group_size`.
    pub max_row_group_size: usize,
}

impl Default for QueueCommonConfig {
    fn default() -> Self {
        Self {
            base_path: String::new(),
            channel_capacity: 1_024,
            max_row_group_size: 8_192,
        }
    }
}

/// Queue write settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueWriteConfig {
    /// Number of retry attempts for write operations.
    pub write_retries: usize,

    /// Compression codec for Parquet files.
    pub compression: CompressionCodec,

    /// Multiplier that controls how many row groups worth of rows can be accumulated before flush.
    /// Depends on `QueueCommonConfig::max_row_group_size`
    pub records_per_flush_multiplier: usize,

    /// Maximum bytes to accumulate before flush.
    /// It is better to synchronize with `ShiftReadConfig::max_input_bytes_per_task`.
    pub max_bytes_per_flush: usize,

    /// Maximum time to wait before flush (in milliseconds).
    pub flush_interval_ms: u64,
}

impl Default for QueueWriteConfig {
    fn default() -> Self {
        Self {
            write_retries: 3,
            compression: CompressionCodec::default(),
            records_per_flush_multiplier: 1,
            max_bytes_per_flush: 64 * 1024 * 1024, // 64 MB
            flush_interval_ms: 200,
        }
    }
}

impl QueueConfig {
    /// Creates a new queue configuration with the given base path.
    #[must_use]
    pub fn new(base_path: impl Into<String>) -> Self {
        let mut config = Self::default();
        config.common.base_path = base_path.into();
        config
    }

    /// Sets the channel capacity.
    #[must_use]
    pub const fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.common.channel_capacity = capacity;
        self
    }

    /// Sets the write retry count.
    #[must_use]
    pub const fn with_write_retries(mut self, retries: usize) -> Self {
        self.write.write_retries = retries;
        self
    }

    /// Sets the compression codec.
    #[must_use]
    pub const fn with_compression(mut self, compression: CompressionCodec) -> Self {
        self.write.compression = compression;
        self
    }

    /// Sets the maximum row group size (in rows).
    #[must_use]
    pub const fn with_max_row_group_size(mut self, max_records: usize) -> Self {
        self.common.max_row_group_size = max_records;
        self
    }

    /// Sets the flush multiplier by rows.
    #[must_use]
    pub const fn with_records_per_flush_multiplier(mut self, multiplier: usize) -> Self {
        self.write.records_per_flush_multiplier = multiplier;
        self
    }

    /// Sets the maximum bytes per flush.
    #[must_use]
    pub const fn with_max_bytes_per_flush(mut self, max_bytes: usize) -> Self {
        self.write.max_bytes_per_flush = max_bytes;
        self
    }

    /// Sets the flush interval in milliseconds.
    #[must_use]
    pub const fn with_flush_interval_ms(mut self, interval_ms: u64) -> Self {
        self.write.flush_interval_ms = interval_ms;
        self
    }

    /// Validates queue configuration values.
    ///
    /// # Errors
    ///
    /// Returns an error if a numeric limit is zero.
    pub fn validate(&self) -> Result<()> {
        if self.common.base_path.trim().is_empty() {
            return Err(QueueError::Config("common.base_path must not be empty".to_string()));
        }
        if self.common.max_row_group_size == 0 {
            return Err(QueueError::Config(
                "common.max_row_group_size must be greater than zero".to_string(),
            ));
        }
        if self.write.records_per_flush_multiplier == 0 {
            return Err(QueueError::Config(
                "write.records_per_flush_multiplier must be greater than zero".to_string(),
            ));
        }
        if self.write.max_bytes_per_flush == 0 {
            return Err(QueueError::Config(
                "write.max_bytes_per_flush must be greater than zero".to_string(),
            ));
        }
        if self.write.flush_interval_ms == 0 {
            return Err(QueueError::Config(
                "write.flush_interval_ms must be greater than zero".to_string(),
            ));
        }
        let _ = self
            .common
            .max_row_group_size
            .checked_mul(self.write.records_per_flush_multiplier)
            .ok_or_else(|| {
                QueueError::Config(
                    "common.max_row_group_size * write.records_per_flush_multiplier overflows usize".to_string(),
                )
            })?;

        Ok(())
    }

    /// Returns record-count threshold to trigger flush by rows.
    ///
    /// # Panics
    ///
    /// Panics if the config is invalid and multiplication overflows.
    /// Call [`Self::validate`] before using this value.
    #[must_use]
    pub fn flush_record_limit(&self) -> usize {
        self.common
            .max_row_group_size
            .checked_mul(self.write.records_per_flush_multiplier)
            .map_or_else(
                || panic!("validated queue config must not overflow flush_record_limit"),
                std::convert::identity,
            )
    }
}

impl CompressionCodec {
    /// Converts to parquet compression.
    #[must_use]
    pub fn to_parquet_compression(self) -> parquet::basic::Compression {
        use parquet::basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel};
        match self {
            Self::None => Compression::UNCOMPRESSED,
            Self::Snappy => Compression::SNAPPY,
            Self::Gzip => Compression::GZIP(GzipLevel::default()),
            Self::Lzo => Compression::LZO,
            Self::Brotli => Compression::BROTLI(BrotliLevel::default()),
            Self::Lz4 => Compression::LZ4,
            Self::Zstd => Compression::ZSTD(ZstdLevel::default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[test]
    fn test_default_config() {
        let config = QueueConfig::new("s3://bucket/queue");
        assert_eq!(config.common.base_path, "s3://bucket/queue");
        assert_eq!(config.common.channel_capacity, 1024);
        assert_eq!(config.common.max_row_group_size, 8192);
        assert_eq!(config.write.write_retries, 3);
        assert_eq!(config.write.records_per_flush_multiplier, 1);
        assert_eq!(config.write.max_bytes_per_flush, 64 * 1024 * 1024);
        assert_eq!(config.write.flush_interval_ms, 200);
    }

    #[test]
    fn test_builder_pattern() {
        let config = QueueConfig::new("s3://bucket/queue")
            .with_channel_capacity(512)
            .with_write_retries(5)
            .with_compression(CompressionCodec::Snappy)
            .with_max_row_group_size(5000)
            .with_records_per_flush_multiplier(2)
            .with_max_bytes_per_flush(32 * 1024 * 1024)
            .with_flush_interval_ms(1000);

        assert_eq!(config.common.channel_capacity, 512);
        assert_eq!(config.common.max_row_group_size, 5000);
        assert_eq!(config.write.write_retries, 5);
        assert!(matches!(config.write.compression, CompressionCodec::Snappy));
        assert_eq!(config.write.records_per_flush_multiplier, 2);
        assert_eq!(config.write.max_bytes_per_flush, 32 * 1024 * 1024);
        assert_eq!(config.write.flush_interval_ms, 1000);
    }

    #[test]
    fn test_validate_ok() {
        let config = QueueConfig::new("queue");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_empty_base_path() {
        let config = QueueConfig::new(" ");
        assert!(matches!(config.validate(), Err(QueueError::Config(_))));
    }

    #[test]
    fn test_validate_rejects_zero_max_row_group_size() {
        let config = QueueConfig::new("queue").with_max_row_group_size(0);
        assert!(matches!(config.validate(), Err(QueueError::Config(_))));
    }

    #[test]
    fn test_validate_rejects_zero_records_per_flush_multiplier() {
        let config = QueueConfig::new("queue").with_records_per_flush_multiplier(0);
        assert!(matches!(config.validate(), Err(QueueError::Config(_))));
    }

    #[derive(Debug, Deserialize)]
    struct QueueRoot {
        queue: QueueConfig,
    }

    #[test]
    fn test_deserialize_rejects_unknown_queue_fields() {
        let json = r#"
{
  "queue": {
    "base_path": "s3://queue"
  }
}
"#;
        let parsed = serde_json::from_str::<QueueRoot>(json);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_deserialize_accepts_nested_queue_fields() {
        let json = r#"
{
  "queue": {
    "common": {
      "base_path": "s3://queue",
      "max_row_group_size": 10000
    }
  }
}
"#;
        let parsed = serde_json::from_str::<QueueRoot>(json).expect("valid nested queue config");
        assert_eq!(parsed.queue.common.base_path, "s3://queue");
    }
}
