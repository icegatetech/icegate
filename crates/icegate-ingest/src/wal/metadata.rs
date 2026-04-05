use super::RowGroupBoundaryRange;
use crate::error::{IngestError, Result};

pub(crate) fn serialize_logs_row_group_metadata(boundary_range: &RowGroupBoundaryRange) -> Result<String> {
    serde_json::to_string(boundary_range)
        .map_err(|err| IngestError::Shift(format!("failed to serialize WAL row-group metadata: {err}")))
}

pub(crate) fn deserialize_logs_row_group_metadata(metadata: &str) -> Result<RowGroupBoundaryRange> {
    let value: serde_json::Value = serde_json::from_str(metadata)
        .map_err(|err| IngestError::Shift(format!("failed to deserialize WAL row-group metadata: {err}")))?;
    let object = value.as_object().ok_or_else(|| {
        IngestError::Shift("failed to deserialize WAL row-group metadata: expected JSON object".to_string())
    })?;
    for key in ["min_key", "max_key"] {
        if !object.contains_key(key) {
            return Err(IngestError::Shift(format!(
                "failed to deserialize WAL row-group metadata: missing field '{key}'"
            )));
        }
    }
    let range: RowGroupBoundaryRange = serde_json::from_value(value)
        .map_err(|err| IngestError::Shift(format!("failed to deserialize WAL row-group metadata: {err}")))?;
    range.validate().map_err(|err| match err {
        IngestError::Shift(message) => {
            IngestError::Shift(format!("failed to deserialize WAL row-group metadata: {message}"))
        }
        other => other,
    })?;
    Ok(range)
}
