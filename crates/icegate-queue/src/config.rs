//! Queue configuration.

use serde::{Deserialize, Serialize};

/// Configuration for the queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct QueueConfig {
    /// Base path for queue segments (e.g., "<s3://bucket/queue>" or "<file:///var/data>").
    pub base_path: String,

    /// Capacity of the write channel (bounded for backpressure).
    pub channel_capacity: usize,

    /// Number of retry attempts for write operations.
    pub write_retries: usize,

    /// Compression codec for Parquet files.
    pub compression: CompressionCodec,

    /// Maximum number of records to accumulate before flush.
    pub max_records_per_flush: usize,

    /// Maximum bytes to accumulate before flush.
    pub max_bytes_per_flush: usize,

    /// Maximum time to wait before flush (in milliseconds).
    pub flush_interval_ms: u64,
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

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            base_path: String::new(),
            channel_capacity: 1024,
            write_retries: 3,
            compression: CompressionCodec::default(),
            max_records_per_flush: 10_000,
            max_bytes_per_flush: 64 * 1024 * 1024, // 64 MB
            flush_interval_ms: 1000,               // 1 second
        }
    }
}

impl QueueConfig {
    /// Creates a new queue configuration with the given base path.
    #[must_use]
    pub fn new(base_path: impl Into<String>) -> Self {
        Self {
            base_path: base_path.into(),
            ..Default::default()
        }
    }

    /// Sets the channel capacity.
    #[must_use]
    pub const fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Sets the write retry count.
    #[must_use]
    pub const fn with_write_retries(mut self, retries: usize) -> Self {
        self.write_retries = retries;
        self
    }

    /// Sets the compression codec.
    #[must_use]
    pub const fn with_compression(mut self, compression: CompressionCodec) -> Self {
        self.compression = compression;
        self
    }

    /// Sets the maximum records per flush.
    #[must_use]
    pub const fn with_max_records_per_flush(mut self, max_records: usize) -> Self {
        self.max_records_per_flush = max_records;
        self
    }

    /// Sets the maximum bytes per flush.
    #[must_use]
    pub const fn with_max_bytes_per_flush(mut self, max_bytes: usize) -> Self {
        self.max_bytes_per_flush = max_bytes;
        self
    }

    /// Sets the flush interval in milliseconds.
    #[must_use]
    pub const fn with_flush_interval_ms(mut self, interval_ms: u64) -> Self {
        self.flush_interval_ms = interval_ms;
        self
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
    use super::*;

    #[test]
    fn test_default_config() {
        let config = QueueConfig::new("s3://bucket/queue");
        assert_eq!(config.base_path, "s3://bucket/queue");
        assert_eq!(config.channel_capacity, 1024);
        assert_eq!(config.write_retries, 3);
        assert_eq!(config.max_records_per_flush, 10_000);
        assert_eq!(config.max_bytes_per_flush, 64 * 1024 * 1024);
        assert_eq!(config.flush_interval_ms, 1000);
    }

    #[test]
    fn test_builder_pattern() {
        let config = QueueConfig::new("s3://bucket/queue")
            .with_channel_capacity(512)
            .with_write_retries(5)
            .with_compression(CompressionCodec::Snappy)
            .with_max_records_per_flush(5000)
            .with_max_bytes_per_flush(32 * 1024 * 1024)
            .with_flush_interval_ms(1000);

        assert_eq!(config.channel_capacity, 512);
        assert_eq!(config.write_retries, 5);
        assert!(matches!(config.compression, CompressionCodec::Snappy));
        assert_eq!(config.max_records_per_flush, 5000);
        assert_eq!(config.max_bytes_per_flush, 32 * 1024 * 1024);
        assert_eq!(config.flush_interval_ms, 1000);
    }
}
