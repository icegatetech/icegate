//! Assembled buckets rendered as the `aggregations` object of a search response.
//!
//! Every aggregation answers under its request name, verbatim. A `date_histogram` answers
//! `{buckets: [{key_as_string, key, doc_count, ..}]}`, `key` being the UTC epoch milliseconds of
//! the bucket's local start; a sub-aggregation answers inside each bucket under its own name.

use serde_json::{Map, Value, json};

use super::assemble::{Bucket, BucketKey, BucketList};
use super::date_histogram::render_key_as_string;
use super::plan::{BucketLevel, LevelKind};
use crate::opensearch::error::OpenSearchError;

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
            object.insert("key".to_string(), json!(start_micros.div_euclid(1_000)));
        }
    }
    object.insert("doc_count".to_string(), json!(bucket.doc_count));
    if let (Some(child), Some(child_level)) = (&bucket.child, deeper.first()) {
        object.insert(child_level.name.to_string(), render_bucket_list(child, deeper)?);
    }
    Ok(Value::Object(object))
}
