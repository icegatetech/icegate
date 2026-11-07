//! Bucket rows of the ranking plan assembled into the response tree.
//!
//! The ranking plan answers one row per surviving bucket of every level, in no order: the level,
//! the keys of the bucket and of its ancestors, and the document count. Assembly groups the rows
//! under their ancestors, orders each level, adds the empty buckets `min_doc_count: 0` asks for,
//! and counts every bucket it answers against `search.max_buckets`.

use std::collections::{BTreeMap, HashMap};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{Int32Type, Int64Type};
use datafusion::common::ScalarValue;

use super::date_histogram::fill_empty_buckets;
use super::plan::{BucketLevel, DOC_COUNT_COLUMN, LEVEL_COLUMN, LevelKind, name_key_column};
use crate::opensearch::error::OpenSearchError;

/// The buckets one search's aggregations may still answer.
pub(super) struct BucketBudget {
    max_buckets: usize,
    used: usize,
}

impl BucketBudget {
    /// A budget of `max_buckets` buckets, none used.
    pub(super) const fn new(max_buckets: usize) -> Self {
        Self { max_buckets, used: 0 }
    }

    /// Buckets left before the limit.
    pub(super) const fn remaining(&self) -> usize {
        self.max_buckets.saturating_sub(self.used)
    }

    /// Count `buckets` more.
    ///
    /// # Errors
    ///
    /// `400 too_many_buckets_exception` once more than `max_buckets` are counted.
    pub(super) fn consume(&mut self, buckets: usize) -> Result<(), OpenSearchError> {
        self.used = self.used.saturating_add(buckets);
        if self.used > self.max_buckets {
            return Err(OpenSearchError::too_many_buckets(self.max_buckets, self.used));
        }
        Ok(())
    }
}

/// The key of one bucket.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum BucketKey {
    /// A `date_histogram` bucket start, in UTC microseconds.
    Date(i64),
}

/// One answered bucket and its sub-aggregation's buckets.
pub(super) struct Bucket {
    /// The bucket's key.
    pub(super) key: BucketKey,
    /// The matching rows in the bucket.
    pub(super) doc_count: u64,
    /// The buckets of the sub-aggregation inside this bucket, when the level has one.
    pub(super) child: Option<BucketList>,
}

/// The buckets of one aggregation instance, in answer order.
pub(super) struct BucketList {
    /// The buckets.
    pub(super) buckets: Vec<Bucket>,
}

/// One surviving bucket as the ranking plan answered it.
struct GroupedBucket {
    key: BucketKey,
    doc_count: u64,
}

/// Surviving buckets keyed by their level and the keys of their ancestors.
type BucketIndex = HashMap<(usize, Vec<BucketKey>), Vec<GroupedBucket>>;

/// Assemble the bucket rows of one aggregation chain.
///
/// # Errors
///
/// `400 too_many_buckets_exception` when the rows and the empty buckets added exceed the budget;
/// `500` when a row does not have the shape the ranking plan answers.
pub(super) fn assemble_buckets(
    batches: &[RecordBatch],
    levels: &[BucketLevel<'_>],
    budget: &mut BucketBudget,
) -> Result<BucketList, OpenSearchError> {
    budget.consume(batches.iter().map(RecordBatch::num_rows).sum())?;
    let mut index = BucketIndex::new();
    for batch in batches {
        index_batch(batch, levels, &mut index)?;
    }
    let mut ancestors = Vec::with_capacity(levels.len());
    assemble_level(&index, levels, 0, &mut ancestors, budget)
}

fn index_batch(
    batch: &RecordBatch,
    levels: &[BucketLevel<'_>],
    index: &mut BucketIndex,
) -> Result<(), OpenSearchError> {
    let build_malformed_row_error = |what: &str| OpenSearchError::internal(format!("bucket rows: {what}"));
    let depths = read_column(batch, LEVEL_COLUMN)?
        .as_primitive_opt::<Int32Type>()
        .ok_or_else(|| build_malformed_row_error("the level is not Int32"))?;
    let doc_counts = read_column(batch, DOC_COUNT_COLUMN)?
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| build_malformed_row_error("the document count is not Int64"))?;
    let keys = (0..levels.len())
        .map(|depth| read_column(batch, &name_key_column(depth)))
        .collect::<Result<Vec<_>, _>>()?;
    for row in 0..batch.num_rows() {
        let depth = usize::try_from(depths.value(row)).map_err(|_| build_malformed_row_error("a negative level"))?;
        if depth >= levels.len() {
            return Err(build_malformed_row_error("a level outside the chain"));
        }
        let mut path = Vec::with_capacity(depth + 1);
        for (level, column) in levels.iter().zip(&keys).take(depth + 1) {
            path.push(read_key(level, column.as_ref(), row)?);
        }
        let key = path
            .pop()
            .ok_or_else(|| build_malformed_row_error("a level outside the chain"))?;
        let doc_count =
            u64::try_from(doc_counts.value(row)).map_err(|_| build_malformed_row_error("a negative count"))?;
        index.entry((depth, path)).or_default().push(GroupedBucket { key, doc_count });
    }
    Ok(())
}

fn read_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, OpenSearchError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| OpenSearchError::internal(format!("bucket rows lack the column [{name}]")))
}

/// The key of `level` in `row`.
fn read_key(level: &BucketLevel<'_>, column: &dyn Array, row: usize) -> Result<BucketKey, OpenSearchError> {
    let value = ScalarValue::try_from_array(column, row)
        .map_err(|error| OpenSearchError::internal(format!("bucket key of [{}]: {error}", level.name)))?;
    match (&level.kind, value) {
        (LevelKind::DateHistogram { .. }, ScalarValue::Int64(Some(start))) => Ok(BucketKey::Date(start)),
        (LevelKind::DateHistogram { .. }, other) => Err(OpenSearchError::internal(format!(
            "bucket key of [{}] has the unexpected value [{other}]",
            level.name
        ))),
    }
}

/// The ordered buckets of the instance of level `depth` under `ancestors`, and their children.
fn assemble_level(
    index: &BucketIndex,
    levels: &[BucketLevel<'_>],
    depth: usize,
    ancestors: &mut Vec<BucketKey>,
    budget: &mut BucketBudget,
) -> Result<BucketList, OpenSearchError> {
    let level = &levels[depth];
    let grouped_buckets = index.get(&(depth, ancestors.clone())).map_or(&[][..], Vec::as_slice);
    let ordered_buckets: Vec<(BucketKey, u64)> = match &level.kind {
        LevelKind::DateHistogram { spec, rounding } => {
            let mut counts: BTreeMap<i64, u64> = grouped_buckets
                .iter()
                .map(|bucket| match bucket.key {
                    BucketKey::Date(start) => (start, bucket.doc_count),
                })
                .collect();
            if spec.min_doc_count == 0 {
                fill_empty_buckets(&mut counts, rounding, spec.extended_bounds, budget)?;
            }
            counts
                .into_iter()
                .map(|(start, doc_count)| (BucketKey::Date(start), doc_count))
                .collect()
        }
    };
    let mut buckets = Vec::with_capacity(ordered_buckets.len());
    for (key, doc_count) in ordered_buckets {
        let child = if depth + 1 < levels.len() {
            ancestors.push(key.clone());
            let child = assemble_level(index, levels, depth + 1, ancestors, budget);
            ancestors.pop();
            Some(child?)
        } else {
            None
        };
        buckets.push(Bucket { key, doc_count, child });
    }
    Ok(BucketList { buckets })
}
