//! Bucket rows of the ranking plan assembled into the response tree.
//!
//! The ranking plan answers one row per surviving bucket of every level, in no order: the level,
//! the keys of the bucket and of its ancestors, the document count and, for a ranked level, the
//! document total of the bucket's partition. Assembly groups the rows under their ancestors,
//! orders each level, adds the empty buckets `min_doc_count: 0` asks for, counts the documents
//! of the buckets a ranking cut as other, and counts every bucket it answers against
//! `search.max_buckets`.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{Int32Type, Int64Type};
use datafusion::common::ScalarValue;

use super::date_histogram::fill_empty_buckets;
use super::plan::{BucketLevel, DOC_COUNT_COLUMN, LEVEL_COLUMN, LevelKind, PARTITION_TOTAL_COLUMN, name_key_column};
use super::terms::compare_buckets;
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
///
/// Keys of one level share a variant. They are ordered as the ranking plan orders them: keywords
/// and ids by bytes, numbers and dates numerically, doubles by IEEE total order, `false` before
/// `true`. A double has no derived total order, hence the hand-written `Ord`, with `Eq` and
/// `Hash` following it.
#[derive(Debug, Clone)]
pub(super) enum BucketKey {
    /// A keyword value.
    Keyword(String),
    /// An integer value.
    Long(i64),
    /// A double value.
    Double(f64),
    /// A boolean value.
    Boolean(bool),
    /// An encoded id, as its stored bytes.
    HexId(Vec<u8>),
    /// A date (a `terms` key) or a bucket start (a `date_histogram` key), in UTC microseconds.
    Date(i64),
}

impl BucketKey {
    /// The position of the variant, which orders keys of different variants.
    const fn rank_variant(&self) -> u8 {
        match self {
            Self::Keyword(_) => 0,
            Self::Long(_) => 1,
            Self::Double(_) => 2,
            Self::Boolean(_) => 3,
            Self::HexId(_) => 4,
            Self::Date(_) => 5,
        }
    }
}

impl Ord for BucketKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Keyword(left), Self::Keyword(right)) => left.cmp(right),
            (Self::Long(left), Self::Long(right)) | (Self::Date(left), Self::Date(right)) => left.cmp(right),
            (Self::Double(left), Self::Double(right)) => left.total_cmp(right),
            (Self::Boolean(left), Self::Boolean(right)) => left.cmp(right),
            (Self::HexId(left), Self::HexId(right)) => left.cmp(right),
            _ => self.rank_variant().cmp(&other.rank_variant()),
        }
    }
}

impl PartialOrd for BucketKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for BucketKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for BucketKey {}

impl Hash for BucketKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.rank_variant().hash(state);
        match self {
            Self::Keyword(text) => text.hash(state),
            Self::Long(value) | Self::Date(value) => value.hash(state),
            // Equal under `total_cmp` means equal bits, so the bits hash consistently with `Eq`.
            Self::Double(value) => value.to_bits().hash(state),
            Self::Boolean(value) => value.hash(state),
            Self::HexId(bytes) => bytes.hash(state),
        }
    }
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
    /// Documents of the partition in buckets the ranking cut; `0` for a level that is not ranked.
    pub(super) other_doc_count: u64,
}

/// One surviving bucket as the ranking plan answered it.
struct GroupedBucket {
    key: BucketKey,
    doc_count: u64,
    /// The partition's document count before the ranking cut, for a ranked level.
    partition_total: Option<u64>,
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
    let partition_totals = read_column(batch, PARTITION_TOTAL_COLUMN)?
        .as_primitive_opt::<Int64Type>()
        .ok_or_else(|| build_malformed_row_error("the partition total is not Int64"))?;
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
        let partition_total = if partition_totals.is_null(row) {
            None
        } else {
            Some(
                u64::try_from(partition_totals.value(row))
                    .map_err(|_| build_malformed_row_error("a negative partition total"))?,
            )
        };
        index.entry((depth, path)).or_default().push(GroupedBucket {
            key,
            doc_count,
            partition_total,
        });
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
        (LevelKind::Terms { .. }, ScalarValue::Utf8(Some(text))) => Ok(BucketKey::Keyword(text)),
        (LevelKind::Terms { .. }, ScalarValue::Int32(Some(value))) => Ok(BucketKey::Long(i64::from(value))),
        (LevelKind::Terms { .. }, ScalarValue::Int64(Some(value))) => Ok(BucketKey::Long(value)),
        (LevelKind::Terms { .. }, ScalarValue::Float64(Some(value))) => Ok(BucketKey::Double(value)),
        (LevelKind::Terms { .. }, ScalarValue::Boolean(Some(value))) => Ok(BucketKey::Boolean(value)),
        (LevelKind::Terms { .. }, ScalarValue::FixedSizeBinary(_, Some(bytes))) => Ok(BucketKey::HexId(bytes)),
        (LevelKind::Terms { .. }, ScalarValue::TimestampMicrosecond(Some(value), _)) => Ok(BucketKey::Date(value)),
        (_, other) => Err(OpenSearchError::internal(format!(
            "bucket key of [{}] has the unexpected value [{other}]",
            level.name
        ))),
    }
}

/// The documents of a ranked level's partition that its answered `grouped_buckets` do not hold.
///
/// # Errors
///
/// Returns an internal error when a bucket lacks its partition total, or when the total is below
/// the documents of the answered buckets: either is a malformed plan row, never a count of `0`.
fn count_other_documents(grouped_buckets: &[GroupedBucket]) -> Result<u64, OpenSearchError> {
    let mut partition_total = None;
    let mut answered_doc_count: u64 = 0;
    for bucket in grouped_buckets {
        partition_total = Some(
            bucket
                .partition_total
                .ok_or_else(|| OpenSearchError::internal("bucket rows: a ranked level without a partition total"))?,
        );
        answered_doc_count += bucket.doc_count;
    }
    partition_total.map_or(Ok(0), |partition_total| {
        partition_total
            .checked_sub(answered_doc_count)
            .ok_or_else(|| OpenSearchError::internal("bucket rows: a partition total below its answered buckets"))
    })
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
    let (ordered_buckets, other_doc_count): (Vec<(BucketKey, u64)>, u64) = match &level.kind {
        LevelKind::DateHistogram { spec, rounding } => {
            let mut counts = BTreeMap::new();
            for bucket in grouped_buckets {
                let BucketKey::Date(start) = bucket.key else {
                    return Err(OpenSearchError::internal(format!(
                        "a key of [{}] is not a bucket start",
                        level.name
                    )));
                };
                counts.insert(start, bucket.doc_count);
            }
            if spec.min_doc_count == 0 {
                fill_empty_buckets(&mut counts, rounding, spec.extended_bounds, budget)?;
            }
            let ordered_buckets = counts
                .into_iter()
                .map(|(start, doc_count)| (BucketKey::Date(start), doc_count))
                .collect();
            (ordered_buckets, 0)
        }
        LevelKind::Terms { spec, .. } => {
            let mut ordered_buckets: Vec<(BucketKey, u64)> = grouped_buckets
                .iter()
                .map(|bucket| (bucket.key.clone(), bucket.doc_count))
                .collect();
            ordered_buckets.sort_by(|left, right| compare_buckets(spec, (&left.0, left.1), (&right.0, right.1)));
            (ordered_buckets, count_other_documents(grouped_buckets)?)
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
    Ok(BucketList {
        buckets,
        other_doc_count,
    })
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::{BucketKey, GroupedBucket, count_other_documents};
    use crate::opensearch::error::render_envelope;

    /// A bucket of `doc_count` documents whose partition holds `partition_total`.
    fn build_grouped_bucket(doc_count: u64, partition_total: Option<u64>) -> GroupedBucket {
        GroupedBucket {
            key: BucketKey::Long(i64::try_from(doc_count).expect("a small count")),
            doc_count,
            partition_total,
        }
    }

    #[tokio::test]
    async fn other_documents_are_the_partition_total_less_the_answered_buckets_and_malformed_rows_are_refused() {
        let counted_cases = [
            (Vec::new(), 0),
            (vec![build_grouped_bucket(3, Some(3))], 0),
            (
                vec![build_grouped_bucket(3, Some(6)), build_grouped_bucket(2, Some(6))],
                1,
            ),
        ];
        for (grouped_buckets, other_doc_count) in counted_cases {
            assert_eq!(
                count_other_documents(&grouped_buckets).expect("well-formed rows"),
                other_doc_count
            );
        }

        let malformed_cases = [
            vec![build_grouped_bucket(3, None)],
            vec![build_grouped_bucket(3, Some(4)), build_grouped_bucket(2, None)],
            vec![build_grouped_bucket(3, Some(4)), build_grouped_bucket(2, Some(4))],
        ];
        for grouped_buckets in malformed_cases {
            let error = count_other_documents(&grouped_buckets).expect_err("a malformed plan row");
            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(envelope["error"]["type"], "exception");
        }
    }
}
