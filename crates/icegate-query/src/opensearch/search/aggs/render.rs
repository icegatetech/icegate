//! Assembled buckets rendered as the `aggregations` object of a search response.
//!
//! Every aggregation answers under its request name, verbatim. A `date_histogram` answers
//! `{buckets: [{key_as_string, key, doc_count, ..}]}`, `key` being the UTC epoch milliseconds of
//! the bucket's local start. A `terms` answers `{doc_count_error_upper_bound: 0,
//! sum_other_doc_count, buckets: [{key, doc_count, ..}]}`: the counts are exact, so the error
//! bound is always `0`. A sub-aggregation answers inside each bucket under its own name.

use serde_json::{Map, Value, json};

use super::assemble::{Bucket, BucketKey, BucketList};
use super::date_histogram::render_key_as_string;
use super::plan::{BucketLevel, LevelKind};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::MICROS_PER_MILLISECOND;

/// The answer of the aggregation instance `list`, whose level is `levels[0]` and whose deeper
/// levels follow it.
///
/// # Errors
///
/// `500` when `levels` is empty or a key does not fit its level.
pub(super) fn render_bucket_list(list: &BucketList, levels: &[BucketLevel<'_>]) -> Result<Value, OpenSearchError> {
    let level = levels
        .first()
        .ok_or_else(|| OpenSearchError::internal("an aggregation without levels"))?;
    let buckets = list
        .buckets
        .iter()
        .map(|bucket| render_bucket(bucket, levels))
        .collect::<Result<Vec<_>, _>>()?;
    match &level.kind {
        LevelKind::DateHistogram { .. } => Ok(json!({ "buckets": buckets })),
        LevelKind::Terms { .. } => Ok(json!({
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": list.other_doc_count,
            "buckets": buckets
        })),
    }
}

fn render_bucket(bucket: &Bucket, levels: &[BucketLevel<'_>]) -> Result<Value, OpenSearchError> {
    let (level, deeper) = levels
        .split_first()
        .ok_or_else(|| OpenSearchError::internal("a bucket without a level"))?;
    let mut object = Map::new();
    match (&level.kind, &bucket.key) {
        (LevelKind::DateHistogram { spec, .. }, BucketKey::Date(start_micros)) => {
            object.insert(
                "key_as_string".to_string(),
                json!(render_key_as_string(
                    *start_micros,
                    spec.time_zone.as_ref(),
                    spec.format.as_ref()
                )?),
            );
            object.insert(
                "key".to_string(),
                json!(start_micros.div_euclid(MICROS_PER_MILLISECOND)),
            );
        }
        (LevelKind::DateHistogram { .. }, _) => {
            return Err(OpenSearchError::internal(format!(
                "a key of [{}] is not a bucket start",
                level.name
            )));
        }
        (LevelKind::Terms { .. }, key) => insert_terms_key(&mut object, key)?,
    }
    object.insert("doc_count".to_string(), json!(bucket.doc_count));
    if let (Some(child), Some(child_level)) = (&bucket.child, deeper.first()) {
        object.insert(child_level.name.to_string(), render_bucket_list(child, deeper)?);
    }
    Ok(Value::Object(object))
}

/// A `terms` bucket's key: a keyword or an id (lowercase hex) as a string, a number as a number,
/// a boolean as `1` / `0` with `key_as_string` `"true"` / `"false"`, a date as epoch
/// milliseconds with an ISO-8601 UTC `key_as_string`.
fn insert_terms_key(object: &mut Map<String, Value>, key: &BucketKey) -> Result<(), OpenSearchError> {
    match key {
        BucketKey::Keyword(text) => {
            object.insert("key".to_string(), json!(text));
        }
        BucketKey::Long(value) => {
            object.insert("key".to_string(), json!(value));
        }
        BucketKey::Double(value) => {
            object.insert("key".to_string(), json!(value));
        }
        BucketKey::Boolean(value) => {
            object.insert("key".to_string(), json!(u8::from(*value)));
            object.insert("key_as_string".to_string(), json!(value.to_string()));
        }
        BucketKey::HexId(bytes) => {
            object.insert("key".to_string(), json!(hex::encode(bytes)));
        }
        BucketKey::Date(micros) => {
            object.insert("key".to_string(), json!(micros.div_euclid(MICROS_PER_MILLISECOND)));
            object.insert(
                "key_as_string".to_string(),
                json!(render_key_as_string(*micros, None, None)?),
            );
        }
    }
    Ok(())
}
