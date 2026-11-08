//! The assembled buckets and metrics rendered as the `aggregations` object of a search response.
//!
//! Every aggregation answers under its request name, verbatim, in request order. A
//! `date_histogram` answers `{buckets: [{key_as_string, key, doc_count, ..}]}`, `key` being the
//! UTC epoch milliseconds of the bucket's local start. A `terms` answers
//! `{doc_count_error_upper_bound: 0, sum_other_doc_count, buckets: [{key, doc_count, ..}]}`: the
//! counts are exact, so the error bound is always `0`. A `histogram` key is the bucket's lower
//! bound, `index × interval`, as a double. A `range`, `date_range`, `filters` and `filter` answer
//! every listed bucket in request order: a `range` with its key and echoed bounds (an object keyed
//! by key when `keyed`), a `date_range` with its key and its bounds as epoch milliseconds and
//! strings, named `filters` as an object keyed by name and anonymous ones as an array of bucket
//! bodies, a `filter` as its single bucket's body. A sub-aggregation answers inside each bucket
//! under its own name; a metric answers as `render_metric` writes it.

use serde_json::{Map, Number, Value, json};

use super::assemble::{AggregateReader, AssembledAggregations, BucketKey, LevelInstance, RowRef, SelectedBucket};
use super::date_histogram::render_key_as_string;
use super::metrics::render_metric;
use super::multi_bucket::{FiltersBuckets, render_date_range_bounds, render_range_bounds};
use super::plan::{AggregationLayout, BucketLevel, LayoutNode, LevelKind, PlanInputs};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::MICROS_PER_MILLISECOND;

/// Render every top-level aggregation of `layout`.
///
/// # Errors
///
/// `500` for a bucket key or a statistic the plans cannot have produced.
pub(super) fn render_aggregations(
    layout: &AggregationLayout<'_>,
    inputs: &PlanInputs,
    assembled: &AssembledAggregations,
) -> Result<Map<String, Value>, OpenSearchError> {
    let renderer = Renderer {
        layout,
        inputs,
        assembled,
        reader: assembled.create_reader(),
    };
    let mut aggregations = Map::new();
    for &node in &layout.top {
        let (name, value) = renderer.render_node(node, &[], assembled.top_row)?;
        aggregations.insert(name, value);
    }
    Ok(aggregations)
}

/// What rendering reads: the layout, the per-metric inputs, and the selected buckets with the
/// aggregated rows their metrics are read from.
struct Renderer<'r, 'l> {
    layout: &'r AggregationLayout<'l>,
    inputs: &'r PlanInputs,
    assembled: &'r AssembledAggregations,
    reader: AggregateReader<'r>,
}

impl Renderer<'_, '_> {
    /// One aggregation inside the bucket whose path is `bucket_path` and whose aggregated row is
    /// `row` (the empty path and the top-level row at the top).
    ///
    /// # Errors
    ///
    /// The errors of [`render_aggregations`].
    fn render_node(
        &self,
        node: LayoutNode,
        bucket_path: &[BucketKey],
        row: Option<RowRef>,
    ) -> Result<(String, Value), OpenSearchError> {
        match node {
            LayoutNode::Metric(metric) => {
                let slot = &self.layout.metrics[metric];
                let is_date = self.inputs.metric_inputs[metric].is_date;
                let value = render_metric(slot.kind, is_date, &|statistic| {
                    self.reader.read_statistic(metric, statistic, row)
                })?;
                Ok((slot.name.to_string(), value))
            }
            LayoutNode::Bucket(level) => Ok((
                self.layout.levels[level].name.to_string(),
                self.render_level(level, bucket_path)?,
            )),
        }
    }

    /// The buckets of `level` under the parent bucket whose path is `parent_path`.
    ///
    /// # Errors
    ///
    /// The errors of [`render_aggregations`].
    fn render_level(&self, level: usize, parent_path: &[BucketKey]) -> Result<Value, OpenSearchError> {
        let empty_instance = LevelInstance::default();
        let instance = self.assembled.instances[level].get(parent_path).unwrap_or(&empty_instance);
        match &self.layout.levels[level].kind {
            LevelKind::Terms { .. } => Ok(json!({
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": instance.sum_other_doc_count,
                "buckets": self.render_keyed_buckets(level, parent_path, instance)?
            })),
            LevelKind::DateHistogram { .. } | LevelKind::Histogram { .. } => Ok(json!({
                "buckets": self.render_keyed_buckets(level, parent_path, instance)?
            })),
            LevelKind::Range { spec } => {
                let mut keyed_buckets = Map::new();
                let mut listed_buckets = Vec::with_capacity(instance.buckets.len());
                for (bucket, range) in instance.buckets.iter().zip(&spec.ranges) {
                    let (key, mut object) = render_range_bounds(range);
                    object.extend(self.render_bucket_body(level, parent_path, bucket)?);
                    if spec.is_keyed {
                        keyed_buckets.insert(key, Value::Object(object));
                    } else {
                        let mut entry = Map::new();
                        entry.insert("key".to_string(), Value::String(key));
                        entry.extend(object);
                        listed_buckets.push(Value::Object(entry));
                    }
                }
                let buckets = if spec.is_keyed {
                    Value::Object(keyed_buckets)
                } else {
                    Value::Array(listed_buckets)
                };
                Ok(json!({ "buckets": buckets }))
            }
            LevelKind::DateRange { spec, bounds } => {
                let mut listed_buckets = Vec::with_capacity(instance.buckets.len());
                for ((bucket, range), resolved_range) in instance.buckets.iter().zip(&spec.ranges).zip(bounds) {
                    let (key, object) = render_date_range_bounds(range, *resolved_range, spec.format)?;
                    let mut entry = Map::new();
                    entry.insert("key".to_string(), Value::String(key));
                    entry.extend(object);
                    entry.extend(self.render_bucket_body(level, parent_path, bucket)?);
                    listed_buckets.push(Value::Object(entry));
                }
                Ok(json!({ "buckets": listed_buckets }))
            }
            LevelKind::Filters { spec, .. } => {
                let buckets = match &spec.filters {
                    FiltersBuckets::Named(filters) => {
                        let mut keyed_buckets = Map::new();
                        for (bucket, (name, _)) in instance.buckets.iter().zip(filters) {
                            keyed_buckets.insert(
                                name.clone(),
                                Value::Object(self.render_bucket_body(level, parent_path, bucket)?),
                            );
                        }
                        Value::Object(keyed_buckets)
                    }
                    FiltersBuckets::Anonymous(_) => Value::Array(
                        instance
                            .buckets
                            .iter()
                            .map(|bucket| self.render_bucket_body(level, parent_path, bucket).map(Value::Object))
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                };
                Ok(json!({ "buckets": buckets }))
            }
            LevelKind::Filter { .. } => {
                let bucket = instance
                    .buckets
                    .first()
                    .ok_or_else(|| OpenSearchError::internal("a filter level answers no bucket"))?;
                Ok(Value::Object(self.render_bucket_body(level, parent_path, bucket)?))
            }
        }
    }

    /// An array of buckets that carry their own key fields.
    ///
    /// # Errors
    ///
    /// The errors of [`render_aggregations`].
    fn render_keyed_buckets(
        &self,
        level: usize,
        parent_path: &[BucketKey],
        instance: &LevelInstance,
    ) -> Result<Vec<Value>, OpenSearchError> {
        let bucket_level = &self.layout.levels[level];
        instance
            .buckets
            .iter()
            .map(|bucket| {
                let mut object = Map::new();
                insert_bucket_key(&mut object, bucket_level, &bucket.key)?;
                object.extend(self.render_bucket_body(level, parent_path, bucket)?);
                Ok(Value::Object(object))
            })
            .collect()
    }

    /// `doc_count` and the sub-aggregations of `bucket`.
    ///
    /// # Errors
    ///
    /// The errors of [`render_aggregations`].
    fn render_bucket_body(
        &self,
        level: usize,
        parent_path: &[BucketKey],
        bucket: &SelectedBucket,
    ) -> Result<Map<String, Value>, OpenSearchError> {
        let mut body = Map::new();
        body.insert("doc_count".to_string(), json!(bucket.doc_count));
        let mut bucket_path = Vec::with_capacity(parent_path.len() + 1);
        bucket_path.extend_from_slice(parent_path);
        bucket_path.push(bucket.key.clone());
        for &child in &self.layout.levels[level].children {
            let (name, value) = self.render_node(child, &bucket_path, bucket.row)?;
            body.insert(name, value);
        }
        Ok(body)
    }
}

/// The key fields of a bucket of `level`.
///
/// # Errors
///
/// `500` for a key that does not fit its level, or a `histogram` key past the double range.
fn insert_bucket_key(
    object: &mut Map<String, Value>,
    level: &BucketLevel<'_>,
    key: &BucketKey,
) -> Result<(), OpenSearchError> {
    match (&level.kind, key) {
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
            Ok(())
        }
        (LevelKind::Histogram { spec }, BucketKey::Long(index)) => {
            object.insert("key".to_string(), render_histogram_key(*index, spec.interval)?);
            Ok(())
        }
        (LevelKind::Terms { .. }, key) => insert_terms_key(object, key),
        (LevelKind::DateHistogram { .. } | LevelKind::Histogram { .. }, _) => Err(OpenSearchError::internal(format!(
            "a key of [{}] does not fit its level",
            level.name
        ))),
        (
            LevelKind::Range { .. }
            | LevelKind::DateRange { .. }
            | LevelKind::Filters { .. }
            | LevelKind::Filter { .. },
            _,
        ) => Err(OpenSearchError::internal(format!(
            "a bucket of [{}] renders its key with its bounds or its name",
            level.name
        ))),
    }
}

/// A `histogram` key: the lower bound `index × interval` of the bucket.
///
/// # Errors
///
/// `500` for a bound that is not a finite double.
#[expect(
    clippy::cast_precision_loss,
    reason = "the key renders as a double, which is the precision the response carries it at"
)]
fn render_histogram_key(index: i64, interval: f64) -> Result<Value, OpenSearchError> {
    let bound = index as f64 * interval;
    Number::from_f64(bound)
        .map(Value::Number)
        .ok_or_else(|| OpenSearchError::internal(format!("histogram key [{index}] x [{interval}] is not finite")))
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
