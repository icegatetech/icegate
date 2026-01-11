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
    pub(crate) fn new(topic: impl Into<Topic>, offset: u64) -> Self {
        Self {
            topic: topic.into(),
            offset,
        }
    }

    /// Converts this segment ID to an object store path.
    ///
    /// Format: `{topic}/{offset}.parquet`
    #[must_use]
    pub(crate) fn to_relative_path(&self) -> Path {
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
    pub(crate) fn from_relative_path(path: &Path) -> Result<Self, QueueError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_id_to_path() {
        let id = SegmentId::new("logs", 42);
        assert_eq!(id.to_relative_path().as_ref(), "logs/00000000000000000042.parquet");
    }

    #[test]
    fn test_segment_id_from_path() {
        let path = Path::from("logs/00000000000000000042.parquet");
        let id = SegmentId::from_relative_path(&path).unwrap();
        assert_eq!(id.topic, "logs");
        assert_eq!(id.offset, 42);
    }

    #[test]
    fn test_segment_id_from_path_to_string() {
        let path = Path::from("logs/00000000000000000042.parquet");
        let id = SegmentId::from_relative_path(&path).unwrap();
        assert_eq!(id.topic, "logs");
        assert_eq!(id.offset, 42);
        assert_eq!(
            format!("{}", id.to_relative_path()),
            "logs/00000000000000000042.parquet"
        );
    }

    #[test]
    fn test_segment_id_from_path_nested_topic() {
        let path = Path::from("tenant/acme/logs/00000000000000000001.parquet");
        let id = SegmentId::from_relative_path(&path).unwrap();
        assert_eq!(id.topic, "tenant/acme/logs");
        assert_eq!(id.offset, 1);
    }

    #[test]
    fn test_segment_id_roundtrip() {
        let original = SegmentId::new("events", 999);
        let path = original.to_relative_path();
        let parsed = SegmentId::from_relative_path(&path).unwrap();
        assert_eq!(original, parsed);
    }
}
