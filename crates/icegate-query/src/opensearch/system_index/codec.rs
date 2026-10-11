//! The byte format of a snapshot object.
//!
//! A snapshot object is two concatenated zstd frames: a skippable frame holding the lineage JSON,
//! then the zstd frame of the snapshot JSON. The object therefore stays valid zstd (a decoder
//! skips the first frame), and a reader that needs only the lineage reads the skippable frame
//! with two ranged GETs: its eight-byte header, then the length the header declares. Landed
//! detection and garbage collection read lineages this way and never download a snapshot.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::snapshot::SystemSnapshot;
use crate::opensearch::error::OpenSearchError;

/// Magic number of the lineage frame: the first of the sixteen numbers zstd reserves for
/// skippable frames. Written little-endian.
const SKIPPABLE_FRAME_MAGIC: u32 = 0x184D_2A50;
/// Length of the lineage frame's header: the magic number and the payload length, four bytes
/// each.
pub(crate) const LINEAGE_FRAME_HEADER_LEN: usize = 8;

/// Where a snapshot object sits in its tenant's history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SnapshotLineage {
    /// Snapshot name of the snapshot object this one superseded; `None` for a tenant's first.
    pub(crate) predecessor: Option<String>,
    /// Ids of the operations this snapshot object applied, refused ones included: their outcomes
    /// were computed on this snapshot's predecessor.
    pub(crate) operation_ids: Vec<Uuid>,
}

/// A snapshot object ready to store.
pub(crate) struct EncodedSnapshotObject {
    /// The object's bytes.
    pub(crate) bytes: Bytes,
    /// Length of the snapshot JSON before compression, which the snapshot bound and the cache
    /// count.
    pub(crate) decoded_len: usize,
}

/// A snapshot read back from its object.
pub(crate) struct DecodedSnapshotObject {
    /// The snapshot.
    pub(crate) snapshot: SystemSnapshot,
    /// Length of the snapshot JSON after decompression.
    pub(crate) decoded_len: usize,
}

/// Encode `snapshot` with its `lineage` as one snapshot object, compressed at zstd's default
/// level.
///
/// # Errors
///
/// Returns `500` when serialization or compression fails, or the lineage outgrows a frame.
pub(crate) fn encode_snapshot_object(
    lineage: &SnapshotLineage,
    snapshot: &SystemSnapshot,
) -> Result<EncodedSnapshotObject, OpenSearchError> {
    let lineage_json = serde_json::to_vec(lineage)
        .map_err(|error| OpenSearchError::internal(format!("cannot encode a snapshot lineage: {error}")))?;
    let lineage_len = u32::try_from(lineage_json.len())
        .map_err(|_| OpenSearchError::internal("a snapshot lineage outgrows its frame"))?;
    let snapshot_json = serde_json::to_vec(snapshot)
        .map_err(|error| OpenSearchError::internal(format!("cannot encode a snapshot: {error}")))?;
    let mut bytes = Vec::with_capacity(LINEAGE_FRAME_HEADER_LEN + lineage_json.len());
    bytes.extend_from_slice(&SKIPPABLE_FRAME_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&lineage_len.to_le_bytes());
    bytes.extend_from_slice(&lineage_json);
    // Compressed straight into the object after the lineage frame: a frame buffer of its own would
    // cost one more allocation and copy of the compressed snapshot.
    zstd::stream::copy_encode(snapshot_json.as_slice(), &mut bytes, zstd::DEFAULT_COMPRESSION_LEVEL)
        .map_err(|error| OpenSearchError::internal(format!("cannot compress a snapshot: {error}")))?;
    Ok(EncodedSnapshotObject {
        bytes: Bytes::from(bytes),
        decoded_len: snapshot_json.len(),
    })
}

/// Decode a whole snapshot object.
///
/// # Errors
///
/// Returns `500` when the object is not a lineage frame followed by a zstd frame of a snapshot.
pub(crate) fn decode_snapshot_object(bytes: &[u8]) -> Result<DecodedSnapshotObject, OpenSearchError> {
    let frame_len = measure_lineage_frame(bytes)?;
    let snapshot_frame = bytes
        .get(frame_len..)
        .ok_or_else(|| build_corrupt_object_error("it ends inside its lineage frame"))?;
    let snapshot_json = zstd::decode_all(snapshot_frame)
        .map_err(|error| build_corrupt_object_error(&format!("its snapshot frame does not decompress: {error}")))?;
    let snapshot = serde_json::from_slice(&snapshot_json)
        .map_err(|error| build_corrupt_object_error(&format!("its snapshot is not a snapshot: {error}")))?;
    Ok(DecodedSnapshotObject {
        snapshot,
        decoded_len: snapshot_json.len(),
    })
}

/// Length of the whole lineage frame, header included, from the first
/// [`LINEAGE_FRAME_HEADER_LEN`] bytes of a snapshot object.
///
/// # Errors
///
/// Returns `500` when `header` is shorter than the header or does not start a lineage frame.
pub(crate) fn measure_lineage_frame(header: &[u8]) -> Result<usize, OpenSearchError> {
    Ok(LINEAGE_FRAME_HEADER_LEN + read_payload_len(header)?)
}

/// Decode the lineage from `head`, the first [`measure_lineage_frame`] bytes of a snapshot
/// object (more bytes are ignored).
///
/// # Errors
///
/// Returns `500` when `head` is truncated or its payload is not a lineage.
pub(crate) fn decode_lineage_frame(head: &[u8]) -> Result<SnapshotLineage, OpenSearchError> {
    let payload_len = read_payload_len(head)?;
    let payload = head
        .get(LINEAGE_FRAME_HEADER_LEN..LINEAGE_FRAME_HEADER_LEN + payload_len)
        .ok_or_else(|| build_corrupt_object_error("its lineage frame is truncated"))?;
    serde_json::from_slice(payload)
        .map_err(|error| build_corrupt_object_error(&format!("its lineage is not a lineage: {error}")))
}

/// The payload length the lineage frame's header declares.
fn read_payload_len(header: &[u8]) -> Result<usize, OpenSearchError> {
    let (magic, rest) = header
        .split_first_chunk::<4>()
        .ok_or_else(|| build_corrupt_object_error("its lineage frame header is truncated"))?;
    let (payload_len, _) = rest
        .split_first_chunk::<4>()
        .ok_or_else(|| build_corrupt_object_error("its lineage frame header is truncated"))?;
    if u32::from_le_bytes(*magic) != SKIPPABLE_FRAME_MAGIC {
        return Err(build_corrupt_object_error("it does not start with a lineage frame"));
    }
    usize::try_from(u32::from_le_bytes(*payload_len))
        .map_err(|_| build_corrupt_object_error("its lineage frame length does not fit in memory"))
}

/// The `500` for a snapshot object that does not follow this format.
fn build_corrupt_object_error(reason: &str) -> OpenSearchError {
    OpenSearchError::internal(format!("a system-index snapshot object is corrupt: {reason}"))
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::opensearch::system_index::snapshot::{AliasAction, SystemOperation};

    fn build_snapshot() -> SystemSnapshot {
        let mut snapshot = SystemSnapshot::default();
        snapshot
            .apply(&SystemOperation::CreateIndex {
                name: ".kibana_1".to_string(),
                mappings: json!({"dynamic": "strict"}),
                settings: json!({"number_of_shards": 1}),
            })
            .expect("create");
        snapshot
    }

    /// The zstd decoder is the independent oracle: it must skip the lineage frame and yield the
    /// snapshot JSON alone. The header bytes follow the zstd format's skippable-frame layout: the
    /// magic number 0x184D2A50 and the payload length, both little-endian.
    #[test]
    fn a_snapshot_object_is_valid_zstd_whose_skippable_frame_holds_the_lineage() {
        let snapshot = build_snapshot();
        let lineage = SnapshotLineage {
            predecessor: Some("snap-00000000-0000-4000-8000-000000000001".to_string()),
            operation_ids: vec![Uuid::from_u128(0x0000_0000_0000_4000_8000_0000_0000_0002)],
        };

        let encoded_object = encode_snapshot_object(&lineage, &snapshot).expect("encode");

        let decompressed_bytes = zstd::decode_all(&encoded_object.bytes[..]).expect("the object is valid zstd");
        assert_eq!(encoded_object.decoded_len, decompressed_bytes.len());
        let snapshot_json: Value = serde_json::from_slice(&decompressed_bytes).expect("snapshot JSON");
        assert_eq!(
            snapshot_json["indices"][".kibana_1"]["mappings"],
            json!({"dynamic": "strict"})
        );
        assert_eq!(&encoded_object.bytes[..4], &[0x50, 0x2A, 0x4D, 0x18]);
        let payload_len = usize::try_from(u32::from_le_bytes(
            encoded_object.bytes[4..8].try_into().expect("four length bytes"),
        ))
        .expect("a u32 fits");
        let lineage_json: Value =
            serde_json::from_slice(&encoded_object.bytes[8..8 + payload_len]).expect("lineage JSON");
        assert_eq!(
            lineage_json,
            json!({
                "predecessor": "snap-00000000-0000-4000-8000-000000000001",
                "operation_ids": ["00000000-0000-4000-8000-000000000002"]
            })
        );
        let decoded_object = decode_snapshot_object(&encoded_object.bytes).expect("decode");
        assert_eq!(decoded_object.snapshot, snapshot);
        assert_eq!(decoded_object.decoded_len, encoded_object.decoded_len);
    }

    /// The snapshot JSON is what every other replica and every restart reads: the alias map is
    /// written under `aliases`, each alias keyed to its index, and read back into the same snapshot.
    #[test]
    fn a_snapshot_holding_an_alias_round_trips_through_its_object_with_its_alias_map() {
        let mut snapshot = build_snapshot();
        snapshot
            .apply(&SystemOperation::ApplyAliasActions {
                actions: vec![AliasAction::Add {
                    index: ".kibana_1".to_string(),
                    alias: ".kibana".to_string(),
                }],
            })
            .expect("add the alias");
        let lineage = SnapshotLineage {
            predecessor: None,
            operation_ids: Vec::new(),
        };

        let encoded_object = encode_snapshot_object(&lineage, &snapshot).expect("encode");

        let decompressed_bytes = zstd::decode_all(&encoded_object.bytes[..]).expect("the object is valid zstd");
        let snapshot_json: Value = serde_json::from_slice(&decompressed_bytes).expect("snapshot JSON");
        assert_eq!(
            snapshot_json,
            json!({
                "indices": {".kibana_1": {"mappings": {"dynamic": "strict"}, "settings": {"number_of_shards": 1}}},
                "aliases": {".kibana": ".kibana_1"}
            })
        );
        let decoded_object = decode_snapshot_object(&encoded_object.bytes).expect("decode");
        assert_eq!(decoded_object.snapshot, snapshot);
        assert_eq!(decoded_object.snapshot.resolve_name(".kibana"), Some(".kibana_1"));
    }

    /// A snapshot object stored before snapshots held aliases has no `aliases` key. It is
    /// assembled here from the format's layout (the lineage frame's magic number and payload
    /// length, little-endian, then the zstd frame of the snapshot JSON), as such a writer stored it.
    #[test]
    fn a_snapshot_object_without_an_alias_map_decodes_with_its_indices_and_no_alias() {
        let lineage_json = br#"{"predecessor":null,"operation_ids":[]}"#;
        let snapshot_json =
            br#"{"indices":{".kibana_1":{"mappings":{"dynamic":"strict"},"settings":{"number_of_shards":1}}}}"#;
        let mut object_bytes = vec![0x50, 0x2A, 0x4D, 0x18];
        object_bytes.extend_from_slice(&u32::try_from(lineage_json.len()).expect("a short lineage").to_le_bytes());
        object_bytes.extend_from_slice(lineage_json);
        object_bytes.extend(zstd::encode_all(&snapshot_json[..], zstd::DEFAULT_COMPRESSION_LEVEL).expect("compress"));

        let decoded_object = decode_snapshot_object(&object_bytes).expect("decode");

        assert_eq!(decoded_object.snapshot.alias_names().count(), 0);
        assert_eq!(decoded_object.snapshot.resolve_name(".kibana"), None);
        assert_eq!(decoded_object.snapshot, build_snapshot());
    }

    #[test]
    fn the_lineage_frame_decodes_from_a_ranged_read_of_its_head_alone() {
        let lineage = SnapshotLineage {
            predecessor: Some("snap-11111111-1111-4111-8111-111111111111".to_string()),
            operation_ids: vec![Uuid::from_u128(1), Uuid::from_u128(2)],
        };
        let encoded_object = encode_snapshot_object(&lineage, &build_snapshot()).expect("encode");

        let frame_len = measure_lineage_frame(&encoded_object.bytes[..LINEAGE_FRAME_HEADER_LEN]).expect("the header");
        let head = &encoded_object.bytes[..frame_len];

        assert!(
            frame_len < encoded_object.bytes.len(),
            "the head excludes the snapshot frame"
        );
        assert_eq!(
            decode_lineage_frame(head).expect("the head alone decodes"),
            SnapshotLineage {
                predecessor: Some("snap-11111111-1111-4111-8111-111111111111".to_string()),
                operation_ids: vec![Uuid::from_u128(1), Uuid::from_u128(2)],
            }
        );
        let first_object = encode_snapshot_object(
            &SnapshotLineage {
                predecessor: None,
                operation_ids: Vec::new(),
            },
            &SystemSnapshot::default(),
        )
        .expect("encode");
        let first_frame_len =
            measure_lineage_frame(&first_object.bytes[..LINEAGE_FRAME_HEADER_LEN]).expect("the header");
        assert_eq!(
            decode_lineage_frame(&first_object.bytes[..first_frame_len]).expect("decode"),
            SnapshotLineage {
                predecessor: None,
                operation_ids: Vec::new(),
            }
        );
        assert!(
            measure_lineage_frame(&encoded_object.bytes[..7]).is_err(),
            "a truncated header"
        );
        assert!(
            decode_lineage_frame(&head[..head.len() - 1]).is_err(),
            "a truncated frame"
        );
        let mut wrong_magic = encoded_object.bytes.to_vec();
        wrong_magic[0] = 0x51;
        assert!(
            measure_lineage_frame(&wrong_magic).is_err(),
            "a frame that is not a lineage frame"
        );
    }
}
