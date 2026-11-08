//! The collected aggregate rows read back into the buckets the response renders.
//!
//! Every aggregated row is a candidate bucket of the granularity its level mask names, filed
//! under the keys of its ancestors. Levels are selected top-down in preorder, and a level's
//! candidates are read only under the parent buckets that survived, so a bucket under a parent
//! that `size` or `min_doc_count` cut is never answered. A `terms` level ranks each parent's
//! candidates by its order, then by key ascending, and keeps `size` of them. A `date_histogram`
//! and a `histogram` level answer their buckets in key order and, with `min_doc_count: 0`, the
//! empty buckets between the first and the last key, widened to `extended_bounds`. Every
//! answered bucket, empty ones included, counts against `search.max_buckets` before it is
//! materialized.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{Int32Type, Int64Type};
use datafusion::common::ScalarValue;

use super::date_histogram::{BucketRounding, fill_empty_buckets};
use super::metrics::Statistic;
use super::parse::{DateHistogramSpec, HistogramSpec};
use super::plan::{
    AggregationLayout, BucketLevel, DOC_COUNT_COLUMN, Granularity, LEVEL_MASK_COLUMN, LevelKind, OrderMetric,
    name_key_column, name_statistic_column,
};
use super::terms::{RankedBucket, TermsSpec, compare_buckets};
use crate::opensearch::error::OpenSearchError;

/// 2^63: a floored `f64` is a `histogram` bucket index only strictly between its negation and
/// it, so every index converts to an `i64` other than [`HISTOGRAM_INDEX_OUT_OF_RANGE`].
pub(super) const HISTOGRAM_INDEX_LIMIT: f64 = 9_223_372_036_854_775_808.0;

/// The `histogram` key the plan gives a value whose bucket index lies outside
/// `HISTOGRAM_INDEX_LIMIT` (`NaN` included): no bucket index takes it, and reading it is a `400`.
pub(super) const HISTOGRAM_INDEX_OUT_OF_RANGE: i64 = i64::MIN;

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
/// Keys of one level share a variant. They are ordered as a `terms` ranking orders them:
/// keywords and ids by bytes, numbers and dates numerically, doubles by IEEE total order, `false`
/// before `true`. A double has no derived total order, hence the hand-written `Ord`, with `Eq`
/// and `Hash` following it.
#[derive(Debug, Clone)]
pub(super) enum BucketKey {
    /// A keyword value.
    Keyword(String),
    /// An integer value, or a `histogram` bucket index.
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

/// The rows one executed stage aggregate produced.
pub(super) struct StageOutput {
    members: Vec<(Granularity, i32)>,
    has_grouping_sets: bool,
    batches: Vec<RecordBatch>,
}

impl StageOutput {
    /// The batches of an executed stage, with the members its plan described.
    pub(super) const fn new(
        members: Vec<(Granularity, i32)>,
        has_grouping_sets: bool,
        batches: Vec<RecordBatch>,
    ) -> Self {
        Self {
            members,
            has_grouping_sets,
            batches,
        }
    }

    /// The granularity of a row whose level mask is `mask` (`None` without grouping sets).
    ///
    /// # Errors
    ///
    /// `500` for a mask no member of the stage carries.
    fn find_granularity(&self, mask: Option<i32>) -> Result<Granularity, OpenSearchError> {
        mask.map_or_else(
            || self.members.first(),
            |mask| self.members.iter().find(|(_, member_mask)| *member_mask == mask),
        )
        .map(|(granularity, _)| *granularity)
        .ok_or_else(|| OpenSearchError::internal(format!("level mask [{mask:?}] names no grouping set")))
    }
}

/// Where one aggregated row is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RowRef {
    stage: usize,
    batch: usize,
    row: usize,
}

/// A bucket the response answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SelectedBucket {
    /// The bucket's key.
    pub(super) key: BucketKey,
    /// The matching rows in the bucket.
    pub(super) doc_count: u64,
    /// The aggregated row holding the bucket's metrics; `None` for an empty bucket gap filling
    /// added, whose metrics are empty.
    pub(super) row: Option<RowRef>,
}

/// What one level answers under one parent bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct LevelInstance {
    /// The answered buckets, in response order.
    pub(super) buckets: Vec<SelectedBucket>,
    /// For `terms`, the documents of the admitted buckets the ranking cut.
    pub(super) sum_other_doc_count: u64,
}

/// The selected buckets of every level and the rows their metrics are read from.
pub(super) struct AssembledAggregations {
    outputs: Vec<StageOutput>,
    /// Per level, keyed by the keys of the parent bucket's path: what the level answers under
    /// that bucket. Every surviving parent bucket has an entry.
    pub(super) instances: Vec<HashMap<Vec<BucketKey>, LevelInstance>>,
    /// The aggregated row of the top-level metrics; `None` when there are none, or no row.
    pub(super) top_row: Option<RowRef>,
}

impl AssembledAggregations {
    /// A reader of the metric statistics the aggregates computed.
    pub(super) const fn create_reader(&self) -> AggregateReader<'_> {
        AggregateReader {
            outputs: self.outputs.as_slice(),
        }
    }
}

/// Reads metric statistics from the executed aggregates.
pub(super) struct AggregateReader<'a> {
    outputs: &'a [StageOutput],
}

impl AggregateReader<'_> {
    /// One statistic of `metric` in the aggregated row `row`; NULL for a bucket without a row.
    ///
    /// # Errors
    ///
    /// `500` for a row reference or a column the plans cannot have produced.
    pub(super) fn read_statistic(
        &self,
        metric: usize,
        statistic: Statistic,
        row: Option<RowRef>,
    ) -> Result<ScalarValue, OpenSearchError> {
        let Some(row) = row else {
            return Ok(ScalarValue::Null);
        };
        let batch = self
            .outputs
            .get(row.stage)
            .and_then(|output| output.batches.get(row.batch))
            .ok_or_else(|| OpenSearchError::internal("an aggregated row reference is out of range"))?;
        let column = read_column(batch, &name_statistic_column(metric, statistic))?;
        ScalarValue::try_from_array(column.as_ref(), row.row)
            .map_err(|error| OpenSearchError::internal(format!("statistic of metric [{metric}]: {error}")))
    }
}

/// An aggregated row read as a bucket of its level.
struct Candidate {
    key: BucketKey,
    doc_count: u64,
    row: RowRef,
}

/// The candidates of one level, by the keys of their parent bucket's path.
type CandidatesByParent = HashMap<Vec<BucketKey>, Vec<Candidate>>;

/// Select the buckets every level answers from the executed aggregates.
///
/// # Errors
///
/// `400 too_many_buckets_exception` once the answered buckets of every level, empty ones
/// included, exceed `max_buckets`; `400 illegal_argument_exception` for `histogram` bounds
/// outside the bucket range; `500` for a row the plans cannot have produced.
pub(super) fn assemble_aggregations(
    layout: &AggregationLayout<'_>,
    outputs: Vec<StageOutput>,
    max_buckets: usize,
) -> Result<AssembledAggregations, OpenSearchError> {
    let (mut candidates, top_row) = collect_candidates(layout, &outputs)?;
    let reader = AggregateReader { outputs: &outputs };
    let mut budget = BucketBudget::new(max_buckets);
    let mut instances: Vec<HashMap<Vec<BucketKey>, LevelInstance>> = Vec::with_capacity(layout.levels.len());
    for (level, bucket_level) in layout.levels.iter().enumerate() {
        let parent_paths = list_parent_paths(bucket_level, &instances);
        let mut level_instances = HashMap::with_capacity(parent_paths.len());
        for parent_path in parent_paths {
            let level_candidates = candidates[level].remove(&parent_path).unwrap_or_default();
            let instance = select_level(bucket_level, level_candidates, &reader, &mut budget)?;
            level_instances.insert(parent_path, instance);
        }
        instances.push(level_instances);
    }
    Ok(AssembledAggregations {
        outputs,
        instances,
        top_row,
    })
}

/// The paths of the buckets a level answers under: the empty path at the top, every selected
/// bucket of the parent level otherwise (empty ones included).
fn list_parent_paths(
    level: &BucketLevel<'_>,
    instances: &[HashMap<Vec<BucketKey>, LevelInstance>],
) -> Vec<Vec<BucketKey>> {
    let Some(parent) = level.find_parent() else {
        return vec![Vec::new()];
    };
    let mut parent_paths: Vec<Vec<BucketKey>> = instances[parent]
        .iter()
        .flat_map(|(path, instance)| {
            instance.buckets.iter().map(move |bucket| {
                let mut child_path = Vec::with_capacity(path.len() + 1);
                child_path.extend_from_slice(path);
                child_path.push(bucket.key.clone());
                child_path
            })
        })
        .collect();
    // The bucket budget is charged parent by parent, so a fixed order keeps the count a
    // refusal reports the same on every run of one request.
    parent_paths.sort_unstable();
    parent_paths
}

/// Every aggregated row of `outputs` as a candidate bucket of its level, filed under its parent
/// path, and the row of the top-level metrics; a row with a NULL key on its path is no
/// candidate.
///
/// # Errors
///
/// `500` for a row the plans cannot have produced.
fn collect_candidates(
    layout: &AggregationLayout<'_>,
    outputs: &[StageOutput],
) -> Result<(Vec<CandidatesByParent>, Option<RowRef>), OpenSearchError> {
    let mut candidates: Vec<CandidatesByParent> = layout.levels.iter().map(|_| HashMap::new()).collect();
    let mut top_row = None;
    for (stage, output) in outputs.iter().enumerate() {
        for (batch_index, batch) in output.batches.iter().enumerate() {
            let masks = if output.has_grouping_sets {
                Some(
                    read_column(batch, LEVEL_MASK_COLUMN)?
                        .as_primitive_opt::<Int32Type>()
                        .ok_or_else(|| OpenSearchError::internal("the level mask is not Int32"))?,
                )
            } else {
                None
            };
            let doc_counts = read_column(batch, DOC_COUNT_COLUMN)?
                .as_primitive_opt::<Int64Type>()
                .ok_or_else(|| OpenSearchError::internal("the document count is not Int64"))?;
            let key_columns = list_key_columns(layout, batch);
            for row in 0..batch.num_rows() {
                let position = RowRef {
                    stage,
                    batch: batch_index,
                    row,
                };
                match output.find_granularity(masks.map(|masks| masks.value(row)))? {
                    Granularity::Top => top_row = Some(position),
                    Granularity::Level(level) => {
                        let Some(mut path) = read_path(layout, &layout.levels[level].path, &key_columns, row)? else {
                            continue;
                        };
                        let key = path.pop().ok_or_else(|| OpenSearchError::internal("a level path is empty"))?;
                        let doc_count = u64::try_from(doc_counts.value(row))
                            .map_err(|_| OpenSearchError::internal("a negative document count"))?;
                        candidates[level].entry(path).or_default().push(Candidate {
                            key,
                            doc_count,
                            row: position,
                        });
                    }
                }
            }
        }
    }
    Ok((candidates, top_row))
}

/// The key column of every level in `batch`, by level index; `None` where the batch's
/// aggregate does not group by the level.
pub(super) fn list_key_columns<'b>(
    layout: &AggregationLayout<'_>,
    batch: &'b RecordBatch,
) -> Vec<Option<&'b ArrayRef>> {
    (0..layout.levels.len())
        .map(|level| batch.column_by_name(&name_key_column(level)))
        .collect()
}

/// The keys of `levels` in one aggregated row; `None` when one of them is NULL. A NULL key is a
/// row without a bucket at that level: it counted in the parent's `doc_count`, and it forms no
/// bucket at that level or below.
///
/// # Errors
///
/// `500` for a missing key column or a key of an unexpected type.
pub(super) fn read_path(
    layout: &AggregationLayout<'_>,
    levels: &[usize],
    key_columns: &[Option<&ArrayRef>],
    row: usize,
) -> Result<Option<Vec<BucketKey>>, OpenSearchError> {
    let mut keys = Vec::with_capacity(levels.len());
    for &level in levels {
        let column =
            key_columns.get(level).copied().flatten().ok_or_else(|| {
                OpenSearchError::internal(format!("the aggregated rows lack the key of level [{level}]"))
            })?;
        match read_level_key(&layout.levels[level], column.as_ref(), row)? {
            Some(key) => keys.push(key),
            None => return Ok(None),
        }
    }
    Ok(Some(keys))
}

/// The key of `level` in `row` of `column`; `None` for NULL.
///
/// # Errors
///
/// `400 illegal_argument_exception` naming the aggregation for a `histogram` value whose bucket
/// index lies outside the `i64` range ([`HISTOGRAM_INDEX_OUT_OF_RANGE`]); `500` for a key of
/// another type than the level's key expression yields.
pub(super) fn read_level_key(
    level: &BucketLevel<'_>,
    column: &dyn Array,
    row: usize,
) -> Result<Option<BucketKey>, OpenSearchError> {
    if column.is_null(row) {
        return Ok(None);
    }
    let value = ScalarValue::try_from_array(column, row)
        .map_err(|error| OpenSearchError::internal(format!("bucket key of [{}]: {error}", level.name)))?;
    let key = match (&level.kind, value) {
        (LevelKind::DateHistogram { .. }, ScalarValue::Int64(Some(start))) => BucketKey::Date(start),
        (LevelKind::Histogram { spec }, ScalarValue::Int64(Some(HISTOGRAM_INDEX_OUT_OF_RANGE))) => {
            return Err(refuse_histogram_index(level.name, spec.interval));
        }
        (LevelKind::Histogram { .. }, ScalarValue::Int64(Some(index))) => BucketKey::Long(index),
        (
            LevelKind::Terms { .. },
            ScalarValue::Utf8(Some(text)) | ScalarValue::LargeUtf8(Some(text)) | ScalarValue::Utf8View(Some(text)),
        ) => BucketKey::Keyword(text),
        (LevelKind::Terms { .. }, ScalarValue::Int32(Some(value))) => BucketKey::Long(i64::from(value)),
        (LevelKind::Terms { .. }, ScalarValue::Int64(Some(value))) => BucketKey::Long(value),
        (LevelKind::Terms { .. }, ScalarValue::Float64(Some(value))) => BucketKey::Double(value),
        (LevelKind::Terms { .. }, ScalarValue::Boolean(Some(value))) => BucketKey::Boolean(value),
        (LevelKind::Terms { .. }, ScalarValue::FixedSizeBinary(_, Some(bytes))) => BucketKey::HexId(bytes),
        (LevelKind::Terms { .. }, ScalarValue::TimestampMicrosecond(Some(value), _)) => BucketKey::Date(value),
        (_, other) => {
            return Err(OpenSearchError::internal(format!(
                "bucket key of [{}] has the unexpected value [{other}]",
                level.name
            )));
        }
    };
    Ok(Some(key))
}

/// The buckets `level` answers under one parent bucket, from that bucket's `candidates`.
///
/// # Errors
///
/// The errors of [`assemble_aggregations`] for one level instance.
fn select_level(
    level: &BucketLevel<'_>,
    candidates: Vec<Candidate>,
    reader: &AggregateReader<'_>,
    budget: &mut BucketBudget,
) -> Result<LevelInstance, OpenSearchError> {
    match &level.kind {
        LevelKind::Terms { spec } => select_terms(spec, &level.order_metrics, candidates, reader, budget),
        LevelKind::DateHistogram { spec, rounding } => {
            select_date_histogram(level.name, spec, rounding, candidates, budget)
        }
        LevelKind::Histogram { spec } => select_histogram(level.name, spec, candidates, budget),
    }
}

/// Rank a parent's `terms` candidates and keep `size` of them. Only keys present in the matching
/// rows are candidates, so `min_doc_count: 0` adds no empty bucket.
///
/// # Errors
///
/// `400 too_many_buckets_exception` past the budget; `500` for an ordering statistic the plans
/// cannot have produced.
fn select_terms(
    spec: &TermsSpec,
    order_metrics: &[Option<OrderMetric>],
    candidates: Vec<Candidate>,
    reader: &AggregateReader<'_>,
    budget: &mut BucketBudget,
) -> Result<LevelInstance, OpenSearchError> {
    let minimum = spec.min_doc_count.max(1);
    // Without a metric criterion the ranking reads no order value, and an empty `Vec` allocates
    // nothing.
    let has_metric_order = order_metrics.iter().any(Option::is_some);
    let mut ranked_candidates = Vec::with_capacity(candidates.len());
    for candidate in candidates.into_iter().filter(|candidate| candidate.doc_count >= minimum) {
        let order_values = if has_metric_order {
            order_metrics
                .iter()
                .map(|order_metric| match order_metric {
                    Some(metric) => read_order_value(reader, *metric, candidate.row),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        ranked_candidates.push((candidate, order_values));
    }
    let admitted_doc_count: u64 = ranked_candidates.iter().map(|(candidate, _)| candidate.doc_count).sum();
    let rank = |left: &(Candidate, Vec<Option<f64>>), right: &(Candidate, Vec<Option<f64>>)| {
        compare_buckets(
            spec,
            RankedBucket {
                key: &left.0.key,
                doc_count: left.0.doc_count,
                order_values: &left.1,
            },
            RankedBucket {
                key: &right.0.key,
                doc_count: right.0.doc_count,
                order_values: &right.1,
            },
        )
    };
    // Partition the `size` best candidates to the front in linear time, then sort only them.
    if ranked_candidates.len() > spec.size {
        ranked_candidates.select_nth_unstable_by(spec.size, rank);
        ranked_candidates.truncate(spec.size);
    }
    ranked_candidates.sort_by(rank);
    budget.consume(ranked_candidates.len())?;
    let kept_doc_count: u64 = ranked_candidates.iter().map(|(candidate, _)| candidate.doc_count).sum();
    Ok(LevelInstance {
        buckets: ranked_candidates
            .into_iter()
            .map(|(candidate, _)| select_candidate(candidate))
            .collect(),
        sum_other_doc_count: admitted_doc_count - kept_doc_count,
    })
}

/// The value of an ordering metric in the aggregated row `row`, as a double; `None` when the
/// bucket has no value for it.
///
/// # Errors
///
/// `500` for a statistic the plans cannot have produced.
pub(super) fn read_order_value(
    reader: &AggregateReader<'_>,
    metric: OrderMetric,
    row: RowRef,
) -> Result<Option<f64>, OpenSearchError> {
    convert_order_value(&reader.read_statistic(metric.metric, metric.statistic, Some(row))?)
}

/// An ordering statistic as a double; `None` when the bucket has no value for it.
///
/// # Errors
///
/// `500` for a statistic that is neither a `Float64` nor an `Int64` count.
#[expect(
    clippy::cast_precision_loss,
    reason = "a count above 2^53 loses its lowest bits as an order key, which ranks only such counts approximately"
)]
fn convert_order_value(value: &ScalarValue) -> Result<Option<f64>, OpenSearchError> {
    match value {
        ScalarValue::Float64(value) => Ok(*value),
        ScalarValue::Int64(value) => Ok(value.map(|count| count as f64)),
        ScalarValue::Null => Ok(None),
        other => Err(OpenSearchError::internal(format!(
            "ordering statistic [{other}] is neither a Float64 nor an Int64"
        ))),
    }
}

/// The buckets of a `date_histogram` level under one parent, in key order; with
/// `min_doc_count: 0`, the empty bucket starts between the first and the last bucket, widened
/// to `extended_bounds`.
///
/// # Errors
///
/// `400 too_many_buckets_exception` past the budget; `400 illegal_argument_exception` for bounds
/// outside the representable time range; `500` for a key that is not a bucket start.
fn select_date_histogram(
    name: &str,
    spec: &DateHistogramSpec,
    rounding: &BucketRounding,
    candidates: Vec<Candidate>,
    budget: &mut BucketBudget,
) -> Result<LevelInstance, OpenSearchError> {
    let mut counts: BTreeMap<i64, u64> = BTreeMap::new();
    let mut rows_by_start: HashMap<i64, Candidate> = HashMap::with_capacity(candidates.len());
    for candidate in candidates {
        let &BucketKey::Date(start) = &candidate.key else {
            return Err(OpenSearchError::internal(format!(
                "a key of [{name}] is not a bucket start"
            )));
        };
        if candidate.doc_count >= spec.min_doc_count {
            counts.insert(start, candidate.doc_count);
            rows_by_start.insert(start, candidate);
        }
    }
    budget.consume(counts.len())?;
    if spec.min_doc_count == 0 {
        fill_empty_buckets(&mut counts, rounding, spec.extended_bounds, budget)?;
    }
    let buckets = counts
        .into_keys()
        .map(|start| {
            rows_by_start
                .remove(&start)
                .map_or_else(|| create_empty_bucket(BucketKey::Date(start)), select_candidate)
        })
        .collect();
    Ok(LevelInstance {
        buckets,
        sum_other_doc_count: 0,
    })
}

/// The buckets of a `histogram` level under one parent, in key order; with `min_doc_count: 0`,
/// every bucket index between the first and the last bucket, widened to `extended_bounds`.
///
/// # Errors
///
/// `400 too_many_buckets_exception` past the budget, counted before any empty bucket is built;
/// `400 illegal_argument_exception` for bounds outside the bucket range; `500` for a key that is
/// not a bucket index.
fn select_histogram(
    name: &str,
    spec: &HistogramSpec,
    candidates: Vec<Candidate>,
    budget: &mut BucketBudget,
) -> Result<LevelInstance, OpenSearchError> {
    let mut rows_by_index: BTreeMap<i64, Candidate> = BTreeMap::new();
    for candidate in candidates {
        let &BucketKey::Long(index) = &candidate.key else {
            return Err(OpenSearchError::internal(format!(
                "a key of [{name}] is not a bucket index"
            )));
        };
        if candidate.doc_count >= spec.min_doc_count {
            rows_by_index.insert(index, candidate);
        }
    }
    if spec.min_doc_count > 0 {
        budget.consume(rows_by_index.len())?;
        return Ok(LevelInstance {
            buckets: rows_by_index.into_values().map(select_candidate).collect(),
            sum_other_doc_count: 0,
        });
    }
    let bounds = spec
        .extended_bounds
        .map(|(min, max)| {
            Ok::<_, OpenSearchError>((
                compute_histogram_index(name, min, spec.interval)?,
                compute_histogram_index(name, max, spec.interval)?,
            ))
        })
        .transpose()?;
    let observed_range = rows_by_index
        .keys()
        .next()
        .copied()
        .zip(rows_by_index.keys().next_back().copied());
    let Some((first, last)) = widen_key_range(observed_range, bounds) else {
        return Ok(LevelInstance::default());
    };
    // The span is counted before any bucket is built; one wider than `usize` is past every budget.
    let span = last
        .checked_sub(first)
        .and_then(|difference| difference.checked_add(1))
        .and_then(|span| usize::try_from(span).ok())
        .unwrap_or(usize::MAX);
    budget.consume(span)?;
    let buckets = (first..=last)
        .map(|index| {
            rows_by_index
                .remove(&index)
                .map_or_else(|| create_empty_bucket(BucketKey::Long(index)), select_candidate)
        })
        .collect();
    Ok(LevelInstance {
        buckets,
        sum_other_doc_count: 0,
    })
}

/// The key range the observed keys and the extended bounds cover together; `None` when neither
/// exists.
const fn widen_key_range(observed: Option<(i64, i64)>, bounds: Option<(i64, i64)>) -> Option<(i64, i64)> {
    match (observed, bounds) {
        (Some((first, last)), Some((min, max))) => Some((
            if first < min { first } else { min },
            if last > max { last } else { max },
        )),
        (Some(range), None) | (None, Some(range)) => Some(range),
        (None, None) => None,
    }
}

/// The index of the bucket of the `histogram` `name` holding the bound `value`:
/// `floor(value / interval)`.
///
/// # Errors
///
/// `400 illegal_argument_exception` naming `name` for an index outside `HISTOGRAM_INDEX_LIMIT`.
#[expect(
    clippy::cast_possible_truncation,
    reason = "the floored quotient is checked to lie within the i64 range before the conversion"
)]
fn compute_histogram_index(name: &str, value: f64, interval: f64) -> Result<i64, OpenSearchError> {
    let index = (value / interval).floor();
    if index > -HISTOGRAM_INDEX_LIMIT && index < HISTOGRAM_INDEX_LIMIT {
        Ok(index as i64)
    } else {
        Err(refuse_histogram_index(name, interval))
    }
}

/// The refusal of a value of the `histogram` `name` whose bucket index at `interval` lies
/// outside `HISTOGRAM_INDEX_LIMIT`.
fn refuse_histogram_index(name: &str, interval: f64) -> OpenSearchError {
    OpenSearchError::unsupported_aggregation(
        name,
        format!("a value of [{name}] lies outside the bucket range of interval [{interval}]"),
    )
}

/// The answered bucket of `candidate`, its metrics read from its aggregated row.
fn select_candidate(candidate: Candidate) -> SelectedBucket {
    SelectedBucket {
        key: candidate.key,
        doc_count: candidate.doc_count,
        row: Some(candidate.row),
    }
}

/// An empty bucket gap filling adds: no rows, no aggregated row.
pub(super) const fn create_empty_bucket(key: BucketKey) -> SelectedBucket {
    SelectedBucket {
        key,
        doc_count: 0,
        row: None,
    }
}

/// The column `name` of `batch`.
///
/// # Errors
///
/// `500` when the batch lacks it: the plans name every column the assembly reads.
fn read_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef, OpenSearchError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| OpenSearchError::internal(format!("bucket rows lack the column [{name}]")))
}
