//! Segment types for queue storage.

use object_store::path::Path;
use serde::{Deserialize, Serialize};

use crate::{Topic, error::QueueError};

/// Number of digits for zero-padded offset (max u64 = 18446744073709551615 = 20 digits).
const OFFSET_DIGITS: usize = 20;

/// Unique identifier for a queue segment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SegmentId {
    /// Topic name.
    pub topic: Topic,
    /// Segment offset (monotonically increasing).
    pub offset: u64,
}

impl SegmentId {
    /// Creates a new segment ID.
    #[must_use]
    pub fn new(topic: impl Into<Topic>, offset: u64) -> Self {
        Self {
            topic: topic.into(),
            offset,
        }
    }

    /// Converts this segment ID to an object store path.
    ///
    /// Format: `{topic}/{offset}.parquet`
    #[must_use]
    pub fn to_path(&self) -> Path {
        Path::from(format!(
            "{}/{:0>width$}.parquet",
            self.topic,
            self.offset,
            width = OFFSET_DIGITS
        ))
    }

    /// Parses a segment ID from an object store path.
    ///
    /// Expected format: `{topic}/{offset}.parquet`
    pub fn from_path(path: &Path) -> Result<Self, QueueError> {
        let path_str = path.as_ref();
        let parts: Vec<&str> = path_str.split('/').collect();

        if parts.len() < 2 {
            return Err(QueueError::InvalidPath {
                path: path_str.to_string(),
            });
        }

        let filename = parts[parts.len() - 1];
        let topic = parts[..parts.len() - 1].join("/");

        let offset_str = filename.strip_suffix(".parquet").ok_or_else(|| QueueError::InvalidPath {
            path: path_str.to_string(),
        })?;

        let offset = offset_str.parse::<u64>().map_err(|_| QueueError::InvalidPath {
            path: path_str.to_string(),
        })?;

        Ok(Self { topic, offset })
    }
}

// TODO(crit): check
/// Status of a queue segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SegmentStatus {
    /// Segment is being written.
    Writing,
    /// Segment write completed successfully.
    Complete,
    /// Segment has been compacted to Iceberg.
    Compacted,
    /// Segment write failed.
    Failed,
}

/// Metadata for a queue segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    /// Topic name.
    pub topic: Topic,
    /// Segment offset.
    pub offset: u64,
    /// Number of records in the segment.
    pub record_count: i64,
    /// Size of the segment in bytes.
    pub size_bytes: u64,
    /// Number of row groups in the segment.
    pub row_group_count: usize,
    /// Segment status.
    pub status: SegmentStatus,
    /// Schema fingerprint (hash of Arrow schema).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_fingerprint: Option<String>,
    /// Creation timestamp (Unix epoch millis).
    pub created_at: u128,
}

impl SegmentMetadata {
    /// Creates new segment metadata.
    #[must_use]
    pub fn new(
        topic: impl Into<Topic>,
        offset: u64,
        record_count: i64,
        size_bytes: u64,
        row_group_count: usize,
    ) -> Self {
        Self {
            topic: topic.into(),
            offset,
            record_count,
            size_bytes,
            row_group_count,
            status: SegmentStatus::Complete,
            schema_fingerprint: None,
            // Safe cast: millis since epoch won't overflow u64 until year 584,554,049 AD
            #[allow(clippy::cast_possible_truncation)]
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0),
        }
    }

    /// Creates a segment ID from this metadata.
    #[must_use]
    pub fn segment_id(&self) -> SegmentId {
        SegmentId::new(&self.topic, self.offset)
    }

    /// Sets the schema fingerprint.
    #[must_use]
    pub fn with_schema_fingerprint(mut self, fingerprint: impl Into<String>) -> Self {
        self.schema_fingerprint = Some(fingerprint.into());
        self
    }

    /// Sets the status.
    #[must_use]
    pub const fn with_status(mut self, status: SegmentStatus) -> Self {
        self.status = status;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_id_to_path() {
        let id = SegmentId::new("logs", 42);
        assert_eq!(id.to_path().as_ref(), "logs/00000000000000000042.parquet");
    }

    #[test]
    fn test_segment_id_from_path() {
        let path = Path::from("logs/00000000000000000042.parquet");
        let id = SegmentId::from_path(&path).unwrap();
        assert_eq!(id.topic, "logs");
        assert_eq!(id.offset, 42);
    }

    #[test]
    fn test_segment_id_from_path_to_string() {
        let path = Path::from("logs/00000000000000000042.parquet");
        let id = SegmentId::from_path(&path).unwrap();
        assert_eq!(id.topic, "logs");
        assert_eq!(id.offset, 42);
        assert_eq!(format!("{}", id.to_path()), "logs/00000000000000000042.parquet");
    }

    #[test]
    fn test_segment_id_from_path_nested_topic() {
        let path = Path::from("tenant/acme/logs/00000000000000000001.parquet");
        let id = SegmentId::from_path(&path).unwrap();
        assert_eq!(id.topic, "tenant/acme/logs");
        assert_eq!(id.offset, 1);
    }

    #[test]
    fn test_segment_id_roundtrip() {
        let original = SegmentId::new("events", 999);
        let path = original.to_path();
        let parsed = SegmentId::from_path(&path).unwrap();
        assert_eq!(original, parsed);
    }
}
