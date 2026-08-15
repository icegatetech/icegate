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
    pub fn to_relative_path(&self) -> Path {
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

/// Builds the object store prefix holding one topic's segments under
/// `base_path` (empty `base_path` means the store root).
pub(crate) fn topic_prefix(base_path: &str, topic: &Topic) -> Path {
    if base_path.is_empty() {
        Path::from(format!("{topic}/"))
    } else {
        Path::from(format!("{base_path}/{topic}/"))
    }
}

/// Parses the segment offset out of a listed object key, or `None` when the key
/// is not a segment this crate produced.
///
/// Foreign objects do land under a topic prefix (a stray upload, a bucket shared
/// with another tool), and they sort ABOVE every zero-padded segment key because
/// digits precede letters — so a `.parquet` suffix check alone would let one
/// answer for the segments. Every listing-driven path (recovery, cleanup,
/// reading) therefore parses the key and skips what does not parse.
pub(crate) fn parse_segment_offset(base_path: &str, location: &Path) -> Option<u64> {
    let location_str = location.as_ref();
    if !location_str.ends_with(".parquet") {
        return None;
    }
    let relative_str = if base_path.is_empty() {
        location_str
    } else {
        location_str.strip_prefix(&format!("{base_path}/"))?
    };
    SegmentId::from_relative_path(&Path::from(relative_str))
        .ok()
        .map(|segment_id| segment_id.offset)
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
