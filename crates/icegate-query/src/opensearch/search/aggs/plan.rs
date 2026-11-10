//! Planning and executing a search's aggregations over its matching rows.
//!
//! The aggregation tree is laid out in preorder ([`AggregationLayout`]): every bucket
//! aggregation is a level whose key is computed from the matching rows, and every metric is
//! read at the level that declares it, or over every matching row at the top. A stage is one
//! `Aggregate` over GROUPING SETS: one set per level it computes, the key columns of the level's
//! path from the top, plus the empty set when it computes the top-level metrics. `grouping()`
//! tags each grouped row with the set it belongs to, so a level's buckets are told apart from
//! its ancestors' even when a key is NULL, and a parent-level metric is computed at the parent's
//! own granularity, never rebuilt from its children.
//!
//! A `range`, `date_range` or `filters` level whose buckets can overlap keys each row by the
//! list of the buckets it belongs to (`multi_bucket`). The levels whose paths cross the same
//! such levels share one stage, which reads the matching rows with those lists unnested, so a
//! row repeats only for the buckets it belongs to at those levels and below them. Every other
//! level and the top-level metrics share the stage without an unnest. A request without
//! overlapping buckets therefore has at most one stage, and each stage executes once, so it
//! reads the matching rows once. A `cardinality` is not computed by a stage: each is counted
//! exactly by a branch of its own over its granularity's unnested rows (`distinct`), which reads
//! the matching rows once more per counted metric; the counts of one granularity are joined on
//! its keys.
//!
//! A `terms` level ordered by count or key is ranked in its stage: a `row_number()` partitioned by
//! the parent keys, in the level's order then by key ascending, cuts it to `size` buckets per
//! parent bucket, and a windowed sum carries the documents of every bucket the parent admits, which
//! `sum_other_doc_count` reads. The rows collected for such a level are then bounded by its `size`
//! times its parent buckets, not by the cardinality of its keys. Selection reads the collected
//! rows in Rust (`assemble`), on the blocking pool: a level's buckets are read only under the
//! surviving buckets of its parent, which is the semi-join to the surviving ancestors, and a
//! `terms` level ordered by a metric is ranked there per parent bucket, by its order, then by key
//! ascending. `render` writes the response, on the blocking pool too.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::functions::expr_fn::{coalesce, floor};
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::functions_aggregate::grouping::grouping_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::{
    Aggregate, Expr, ExprFunctionExt, LogicalPlan, LogicalPlanBuilder, cast, col, grouping_set, lit, not, when,
};
use futures::future::try_join_all;
use serde_json::{Map, Value, json};

use super::assemble::{
    DistinctOutput, HISTOGRAM_INDEX_LIMIT, HISTOGRAM_INDEX_OUT_OF_RANGE, StageOutput, assemble_aggregations,
};
use super::date_histogram::BucketRounding;
use super::distinct::plan_exact_distinct;
use super::metrics::{
    MetricInput, MetricKind, MetricSpec, Statistic, plan_metric_input, plan_statistic, resolve_order_statistic,
};
use super::multi_bucket::{
    DateRangeBucketsSpec, FiltersSpec, RangeBucketsSpec, ResolvedDateRange, are_disjoint, plan_ordinals,
    plan_range_conditions, resolve_date_ranges,
};
use super::parse::{
    AggregationKind, AggregationTree, DateHistogramSpec, HistogramSpec, NamedAggregation, read_epoch_millis,
};
use super::percentiles::{
    PercentilesSpec, SharedDigest, find_percentile_order_element, group_shared_digests, plan_rank_counts_aggregate,
};
use super::pipeline::{apply_pipelines, validate_pipelines};
use super::render::render_aggregations;
use super::terms::{TermsOrder, TermsSpec, build_filter_predicate, build_key_literal};
use super::top_hits::{TopHitsResults, TopHitsSpec, collect_top_hits};
use crate::opensearch::catalog::{EncodedField, FieldType, FieldValue, IndexEntry, ResolvedField, TenantSession};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::MICROS_PER_MILLISECOND;
use crate::opensearch::search::execute::convert_plan_error;
use crate::opensearch::search::request::SortOrder;
use crate::opensearch::system_index::run_blocking_work;
use crate::query_dsl::ast::Query;
use crate::query_dsl::lower::{LoweringScope, lower_query};

/// Column of a bucket's document count.
pub(super) const DOC_COUNT_COLUMN: &str = "__doc_count";
/// Column of the `grouping()` bitmask: a set bit marks a key column the row's grouping set
/// omits, the first key passed to `grouping()` the highest bit ([`compute_level_mask`]; pinned
/// against DataFusion by
/// `bucket_tree_tests::grouping_marks_the_levels_a_set_omits_with_the_first_grouped_level_as_the_high_bit`).
pub(super) const LEVEL_MASK_COLUMN: &str = "__level_mask";
/// Most bucket aggregations one request may hold: one aggregate groups by all of them, and the
/// level mask is an `i32` with one bit per level.
const MAX_BUCKET_LEVELS: usize = 16;

/// The column of the key of the level at layout index `level`.
pub(super) fn name_key_column(level: usize) -> String {
    format!("__key_{level}")
}

/// The column of the per-row value of the metric at layout index `metric`.
pub(super) fn name_value_column(metric: usize) -> String {
    format!("__value_{metric}")
}

/// The column of one statistic of the metric at layout index `metric`, in a stage aggregate's
/// output.
pub(super) fn name_statistic_column(metric: usize, statistic: Statistic) -> String {
    format!("__metric_{metric}_{}", statistic.read_column_suffix())
}

/// The column of a stage's output carrying, beside each kept row of the `terms` level at layout
/// index `level` that is ranked in the plan, the documents of every bucket its parent admits
/// ([`TermsSpec::is_ranked_by_count_or_key`]).
pub(super) fn name_admitted_doc_count_column(level: usize) -> String {
    format!("__admitted_doc_count_{level}")
}

/// The column marking the rows that are admitted buckets of the ranked `terms` level at `level`;
/// dropped before the stage's output.
fn name_admitted_bucket_column(level: usize) -> String {
    format!("__admitted_bucket_{level}")
}

/// The column of a bucket's rank under its parent at the ranked `terms` level at `level`; dropped
/// before the stage's output.
fn name_bucket_rank_column(level: usize) -> String {
    format!("__bucket_rank_{level}")
}

/// The rows-plan column holding whether a row matches the `filter` / `filters` bucket query at
/// `ordinal` of [`AggregationLayout::bucket_queries`].
pub(super) fn name_bucket_filter_column(ordinal: usize) -> String {
    format!("__bucket_filter_{ordinal}")
}

/// The prefix of the rows-plan columns that carry the searched index's table columns for a
/// `top_hits`.
pub(super) const HIT_COLUMN_PREFIX: &str = "__hit_";

/// The rows-plan column carrying the table column `column` for a `top_hits`.
pub(super) fn name_hit_column(column: &str) -> String {
    format!("{HIT_COLUMN_PREFIX}{column}")
}

/// What executing aggregations needs from the request.
pub(crate) struct AggregationScope<'a> {
    /// The tenant session the plans execute in.
    pub(crate) session: &'a TenantSession,
    /// The searched indices, in request order: a `top_hits` renders its hits as `_search` hits
    /// of the one index it reads.
    pub(crate) entries: &'a [&'a IndexEntry],
    /// The most buckets the request may answer, every level of every aggregation counted.
    pub(crate) max_buckets: usize,
    /// The most hits the request's `top_hits` may render together: `search.max_result_window`.
    pub(crate) max_result_window: usize,
    /// The request's `now`, taken once per request, which `date_range` bounds resolve
    /// against.
    pub(crate) now: DateTime<Utc>,
}

/// Where an aggregation of the request sits in the layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LayoutNode {
    /// A bucket aggregation, by level index.
    Bucket(usize),
    /// A metric aggregation, by metric index.
    Metric(usize),
    /// A `top_hits` aggregation, by its index in [`AggregationLayout::top_hits`].
    TopHits(usize),
}

/// The buckets one grouping set computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Granularity {
    /// One bucket holding every matching row: the empty set of the top-level metrics.
    Top,
    /// The buckets of one level: the set of the key columns of its path.
    Level(usize),
}

/// The statistic a `terms` order criterion ranks by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OrderMetric {
    /// The ordering metric, a direct sub-aggregation of the level.
    pub(super) metric: usize,
    /// The statistic of that metric.
    pub(super) statistic: Statistic,
    /// The element of a list statistic the order reads (a percent of `percentiles`); `None` for
    /// a scalar statistic.
    pub(super) element: Option<usize>,
}

/// The bucket kinds a level makes, with what planning resolved for them.
pub(super) enum LevelKind<'a> {
    /// `date_histogram` buckets.
    DateHistogram {
        /// The request.
        spec: &'a DateHistogramSpec,
        /// Its rounding.
        rounding: BucketRounding,
    },
    /// `terms` buckets.
    Terms {
        /// The request.
        spec: &'a TermsSpec,
    },
    /// `histogram` buckets.
    Histogram {
        /// The request.
        spec: &'a HistogramSpec,
    },
    /// `range` buckets.
    Range {
        /// The request.
        spec: &'a RangeBucketsSpec,
    },
    /// `date_range` buckets.
    DateRange {
        /// The request.
        spec: &'a DateRangeBucketsSpec,
        /// Its ranges, resolved against the request's `now`.
        bounds: Vec<ResolvedDateRange>,
    },
    /// `filters` buckets: one per bucket query, the queries at `first_filter_ordinal ..
    /// first_filter_ordinal + filter_count` of the layout's `bucket_queries`.
    Filters {
        /// The request.
        spec: &'a FiltersSpec,
        /// The ordinal of the level's first bucket query.
        first_filter_ordinal: usize,
        /// How many bucket queries the level holds.
        filter_count: usize,
    },
    /// The `filter` bucket: the layout's bucket query at `filter_ordinal`.
    Filter {
        /// The ordinal of the level's bucket query.
        filter_ordinal: usize,
    },
}

/// A bucket aggregation of the request.
pub(super) struct BucketLevel<'a> {
    /// The aggregation's request name.
    pub(super) name: &'a str,
    /// The kind of buckets the level makes.
    pub(super) kind: LevelKind<'a>,
    /// The levels from the top down to this one, this one last.
    pub(super) path: Vec<usize>,
    /// The sub-aggregations, in request order.
    pub(super) children: Vec<LayoutNode>,
    /// For `terms`, the metric each criterion of the order reads, aligned with the spec's
    /// `order` (`None` for `_count` and `_key`); empty for the other kinds.
    pub(super) order_metrics: Vec<Option<OrderMetric>>,
    /// Whether a criterion of the order reads a distinct count, which is looked up by bucket
    /// path; decided once for the level, so ranking a parent's candidates does not re-derive it.
    pub(super) is_ordered_by_a_distinct_count: bool,
}

impl BucketLevel<'_> {
    /// The level this one nests in; `None` at the top.
    pub(super) fn find_parent(&self) -> Option<usize> {
        self.path.iter().rev().nth(1).copied()
    }
}

/// A metric aggregation of the request and where it is read.
pub(super) struct MetricSlot<'a> {
    /// The aggregation's request name.
    pub(super) name: &'a str,
    /// The metric computed.
    pub(super) kind: MetricKind,
    /// The field and `missing` it reads.
    pub(super) spec: &'a MetricSpec,
    /// The aggregation the slot answers. The kinds whose request carries more than a field and
    /// `missing` (`percentiles`, `percentile_ranks`) are read from it; `kind` names which one it
    /// is.
    pub(super) aggregation: &'a AggregationKind,
    /// The granularity the metric is computed at.
    pub(super) granularity: Granularity,
}

/// A `top_hits` aggregation of the request and the buckets it lists hits for.
pub(super) struct TopHitsSlot<'a> {
    /// The aggregation's request name.
    pub(super) name: &'a str,
    /// The request.
    pub(super) spec: &'a TopHitsSpec,
    /// The granularity whose buckets it lists hits for.
    pub(super) granularity: Granularity,
}

/// The request's aggregations in preorder: a parent level precedes its children.
pub(super) struct AggregationLayout<'a> {
    /// The top-level aggregations, in request order.
    pub(super) top: Vec<LayoutNode>,
    /// Every bucket aggregation, by level index.
    pub(super) levels: Vec<BucketLevel<'a>>,
    /// Every metric aggregation, by metric index.
    pub(super) metrics: Vec<MetricSlot<'a>>,
    /// Every `top_hits` aggregation, by its index.
    pub(super) top_hits: Vec<TopHitsSlot<'a>>,
    /// The query of every `filter` and `filters` bucket, in preorder; the position of a query is
    /// the ordinal of its column in the rows plan ([`name_bucket_filter_column`]).
    pub(super) bucket_queries: Vec<&'a Query>,
    /// The request's `now`, which `date_range` bounds resolve against.
    now: DateTime<Utc>,
}

impl<'a> AggregationLayout<'a> {
    /// Lay out `tree`, resolve every `terms` order path to its metric and every `date_range`
    /// bound against `now`, and number the bucket queries.
    ///
    /// # Errors
    ///
    /// `400 parsing_exception` for a metric or a `top_hits` with sub-aggregations; `400
    /// aggregation_execution_exception` for an order path naming no metric sub-aggregation of
    /// its level, or no statistic of that metric; `400 illegal_argument_exception` for more than
    /// `MAX_BUCKET_LEVELS` bucket aggregations in the request (naming the top-level aggregation
    /// that passes the limit under `aggregation`, the limit under `limit`), a `date_histogram`
    /// interval or offset out of range, and a `date_range` bound that is malformed or outside
    /// the representable time range ([`resolve_date_ranges`]).
    pub(super) fn new(tree: &'a AggregationTree, now: DateTime<Utc>) -> Result<Self, OpenSearchError> {
        let mut layout = Self {
            top: Vec::with_capacity(tree.aggregations.len()),
            levels: Vec::new(),
            metrics: Vec::new(),
            top_hits: Vec::new(),
            bucket_queries: Vec::new(),
            now,
        };
        for aggregation in &tree.aggregations {
            let node = layout.add_node(aggregation, &[])?;
            if layout.levels.len() > MAX_BUCKET_LEVELS {
                return Err(OpenSearchError::aggregation_depth_exceeded(
                    &aggregation.name,
                    MAX_BUCKET_LEVELS,
                ));
            }
            if let Some(node) = node {
                layout.top.push(node);
            }
        }
        Ok(layout)
    }

    /// Add `aggregation`, nested under the levels `ancestors`, and its sub-aggregations; `None` for
    /// a pipeline, which runs over the rendered tree and takes no node.
    ///
    /// # Errors
    ///
    /// The errors of [`AggregationLayout::new`].
    fn add_node(
        &mut self,
        aggregation: &'a NamedAggregation,
        ancestors: &[usize],
    ) -> Result<Option<LayoutNode>, OpenSearchError> {
        let node = match &aggregation.kind {
            AggregationKind::DateHistogram(spec) => {
                let rounding = BucketRounding::from_spec(spec)?;
                self.add_level(aggregation, LevelKind::DateHistogram { spec, rounding }, ancestors)
            }
            AggregationKind::Terms(spec) => self.add_level(aggregation, LevelKind::Terms { spec }, ancestors),
            AggregationKind::Histogram(spec) => self.add_level(aggregation, LevelKind::Histogram { spec }, ancestors),
            AggregationKind::Range(spec) => self.add_level(aggregation, LevelKind::Range { spec }, ancestors),
            AggregationKind::DateRange(spec) => {
                let bounds = resolve_date_ranges(&aggregation.name, spec, self.now)?;
                self.add_level(aggregation, LevelKind::DateRange { spec, bounds }, ancestors)
            }
            AggregationKind::Filters(spec) => {
                let first_filter_ordinal = self.bucket_queries.len();
                self.bucket_queries.extend(spec.iter_queries());
                let filter_count = self.bucket_queries.len() - first_filter_ordinal;
                let kind = LevelKind::Filters {
                    spec,
                    first_filter_ordinal,
                    filter_count,
                };
                self.add_level(aggregation, kind, ancestors)
            }
            AggregationKind::Filter(query) => {
                let filter_ordinal = self.bucket_queries.len();
                self.bucket_queries.push(query);
                self.add_level(aggregation, LevelKind::Filter { filter_ordinal }, ancestors)
            }
            AggregationKind::Avg(spec) => self.add_metric(aggregation, MetricKind::Avg, spec, ancestors),
            AggregationKind::Sum(spec) => self.add_metric(aggregation, MetricKind::Sum, spec, ancestors),
            AggregationKind::Min(spec) => self.add_metric(aggregation, MetricKind::Min, spec, ancestors),
            AggregationKind::Max(spec) => self.add_metric(aggregation, MetricKind::Max, spec, ancestors),
            AggregationKind::ValueCount(spec) => self.add_metric(aggregation, MetricKind::ValueCount, spec, ancestors),
            AggregationKind::Stats(spec) => self.add_metric(aggregation, MetricKind::Stats, spec, ancestors),
            AggregationKind::Cardinality(spec) => {
                self.add_metric(aggregation, MetricKind::Cardinality, spec, ancestors)
            }
            AggregationKind::Percentiles(spec) => {
                self.add_metric(aggregation, MetricKind::Percentiles, &spec.metric, ancestors)
            }
            AggregationKind::PercentileRanks(spec) => {
                self.add_metric(aggregation, MetricKind::PercentileRanks, &spec.metric, ancestors)
            }
            AggregationKind::ExtendedStats(spec) => {
                self.add_metric(aggregation, MetricKind::ExtendedStats, &spec.metric, ancestors)
            }
            AggregationKind::TopHits(spec) => self.add_top_hits(aggregation, spec, ancestors),
            // A pipeline runs over the rendered tree (`apply_pipelines`), so it takes no node.
            AggregationKind::CumulativeSum(_)
            | AggregationKind::Derivative(_)
            | AggregationKind::SerialDiff(_)
            | AggregationKind::MovingFn(_)
            | AggregationKind::MovingAvg(_)
            | AggregationKind::BucketScript(_)
            | AggregationKind::AvgBucket(_)
            | AggregationKind::SumBucket(_)
            | AggregationKind::MinBucket(_)
            | AggregationKind::MaxBucket(_) => return Ok(None),
        };
        node.map(Some)
    }

    /// Add the bucket aggregation `aggregation` as the next level, then its sub-aggregations,
    /// then the metrics its `terms` order reads.
    ///
    /// # Errors
    ///
    /// The errors of [`AggregationLayout::new`].
    fn add_level(
        &mut self,
        aggregation: &'a NamedAggregation,
        kind: LevelKind<'a>,
        ancestors: &[usize],
    ) -> Result<LayoutNode, OpenSearchError> {
        let level = self.levels.len();
        let mut path = Vec::with_capacity(ancestors.len() + 1);
        path.extend_from_slice(ancestors);
        path.push(level);
        let ranking_spec = match &kind {
            LevelKind::Terms { spec } => Some(*spec),
            LevelKind::DateHistogram { .. }
            | LevelKind::Histogram { .. }
            | LevelKind::Range { .. }
            | LevelKind::DateRange { .. }
            | LevelKind::Filters { .. }
            | LevelKind::Filter { .. } => None,
        };
        self.levels.push(BucketLevel {
            name: &aggregation.name,
            kind,
            path: path.clone(),
            children: Vec::new(),
            order_metrics: Vec::new(),
            is_ordered_by_a_distinct_count: false,
        });
        let children = aggregation
            .children
            .iter()
            .map(|child| self.add_node(child, &path))
            .filter_map(Result::transpose)
            .collect::<Result<Vec<_>, _>>()?;
        let order_metrics = match ranking_spec {
            Some(spec) => self.resolve_order_metrics(&aggregation.name, spec, &children)?,
            None => Vec::new(),
        };
        let added_level = &mut self.levels[level];
        added_level.children = children;
        added_level.is_ordered_by_a_distinct_count = order_metrics
            .iter()
            .flatten()
            .any(|order_metric| order_metric.statistic == Statistic::Distinct);
        added_level.order_metrics = order_metrics;
        Ok(LayoutNode::Bucket(level))
    }

    /// Add the metric aggregation `aggregation`, read at the granularity of the innermost of
    /// `ancestors`, or at the top without one.
    ///
    /// # Errors
    ///
    /// `400 parsing_exception` when `aggregation` has sub-aggregations.
    fn add_metric(
        &mut self,
        aggregation: &'a NamedAggregation,
        kind: MetricKind,
        spec: &'a MetricSpec,
        ancestors: &[usize],
    ) -> Result<LayoutNode, OpenSearchError> {
        refuse_sub_aggregations(aggregation, kind.read_type_name())?;
        self.metrics.push(MetricSlot {
            name: &aggregation.name,
            kind,
            spec,
            aggregation: &aggregation.kind,
            granularity: find_leaf_granularity(ancestors),
        });
        Ok(LayoutNode::Metric(self.metrics.len() - 1))
    }

    /// Add the `top_hits` aggregation `aggregation`, listing hits for the buckets of the
    /// innermost of `ancestors`, or for every matching row at the top without one.
    ///
    /// # Errors
    ///
    /// `400 parsing_exception` when `aggregation` has sub-aggregations.
    fn add_top_hits(
        &mut self,
        aggregation: &'a NamedAggregation,
        spec: &'a TopHitsSpec,
        ancestors: &[usize],
    ) -> Result<LayoutNode, OpenSearchError> {
        refuse_sub_aggregations(aggregation, "top_hits")?;
        self.top_hits.push(TopHitsSlot {
            name: &aggregation.name,
            spec,
            granularity: find_leaf_granularity(ancestors),
        });
        Ok(LayoutNode::TopHits(self.top_hits.len() - 1))
    }

    /// The metric each criterion of a `terms` level's order reads, among the level's direct
    /// metric sub-aggregations.
    ///
    /// # Errors
    ///
    /// `400 aggregation_execution_exception` for an order path naming no direct metric
    /// sub-aggregation of the level, no statistic of that metric, a percent a `percentiles` does
    /// not compute, or a percent of any other metric.
    fn resolve_order_metrics(
        &self,
        name: &str,
        spec: &TermsSpec,
        children: &[LayoutNode],
    ) -> Result<Vec<Option<OrderMetric>>, OpenSearchError> {
        spec.order
            .iter()
            .map(|criterion| match criterion {
                TermsOrder::Count(_) | TermsOrder::Key(_) => Ok(None),
                TermsOrder::Metric { path, .. } => {
                    let metric = children
                        .iter()
                        .find_map(|child| match *child {
                            LayoutNode::Metric(metric) if self.metrics[metric].name == path.aggregation => Some(metric),
                            LayoutNode::Metric(_) | LayoutNode::Bucket(_) | LayoutNode::TopHits(_) => None,
                        })
                        .ok_or_else(|| {
                            OpenSearchError::aggregation_execution(&format!(
                                "[terms] [{name}] is ordered by [{}], which names no metric sub-aggregation of it",
                                path.aggregation
                            ))
                        })?;
                    let slot = &self.metrics[metric];
                    // A percent orders by one element of the `quantiles` list the slot computes.
                    let order_metric = if let AggregationKind::Percentiles(spec) = slot.aggregation {
                        OrderMetric {
                            metric,
                            statistic: Statistic::Quantiles,
                            element: Some(find_percentile_order_element(spec, path)?),
                        }
                    } else if let Some(percent) = &path.percent {
                        // Another metric computes no percent; ordering by its own value instead
                        // would rank the buckets by something the path does not name.
                        return Err(OpenSearchError::aggregation_execution(&format!(
                            "order path [{}[{percent}]] selects a percent, which the [{}] aggregation [{}] does \
                             not compute",
                            path.aggregation,
                            slot.kind.read_type_name(),
                            path.aggregation
                        )));
                    } else {
                        OrderMetric {
                            metric,
                            statistic: resolve_order_statistic(slot.kind, path)?,
                            element: None,
                        }
                    };
                    Ok(Some(order_metric))
                }
            })
            .collect()
    }
}

/// The granularity a leaf aggregation (a metric or a `top_hits`) under the levels `ancestors` is
/// computed at: its parent level's buckets, or the top.
fn find_leaf_granularity(ancestors: &[usize]) -> Granularity {
    ancestors.last().map_or(Granularity::Top, |&level| Granularity::Level(level))
}

/// Refuse sub-aggregations under the leaf aggregation `aggregation` of type `type_name`.
///
/// # Errors
///
/// `400 parsing_exception` naming the aggregation and its type when it has one.
fn refuse_sub_aggregations(aggregation: &NamedAggregation, type_name: &str) -> Result<(), OpenSearchError> {
    if aggregation.children.is_empty() {
        return Ok(());
    }
    Err(OpenSearchError::parsing(format!(
        "aggregation [{}] of type [{type_name}] cannot accept sub-aggregations",
        aggregation.name
    )))
}

/// A rows-plan column an aggregation reads.
pub(super) struct FieldInput {
    /// The column.
    pub(super) expr: Expr,
    /// Its Arrow type: the type the field's field type maps to (`map_field_data_type`), or the
    /// kind's own type for a field no searched index maps (`choose_row_data_type`).
    pub(super) data_type: DataType,
}

/// The column of `rows` holding `field`, which [`plan_aggregation_rows`] names after the field.
/// The column is built from its name, never parsed, so a dotted field name stays one name.
///
/// # Errors
///
/// `500` when `rows` lacks the column, which `plan_aggregation_rows` always projects.
pub(super) fn read_row_field(rows: &DFSchema, field: &str, aggregation: &str) -> Result<FieldInput, OpenSearchError> {
    let column = rows.field_with_unqualified_name(field).map_err(|error| {
        OpenSearchError::internal(format!("the rows of [{aggregation}] lack the field [{field}]: {error}"))
    })?;
    Ok(FieldInput {
        expr: Expr::Column(Column::from_name(field)),
        data_type: column.data_type().clone(),
    })
}

/// The key a bucket level groups the rows by.
pub(super) struct LevelKey {
    /// Each row's key, or, when `is_membership_list`, the list of the ordinals of every bucket
    /// the row belongs to. A NULL key (or a NULL entry) puts the row in no bucket of the level
    /// or below it; the row still counts in its parent bucket.
    pub(super) expr: Expr,
    /// Whether `expr` is a membership list, which the planner unnests in a stage aggregate of
    /// the level's own.
    pub(super) is_membership_list: bool,
}

/// What the planner evaluates over the matching rows, by layout position.
pub(super) struct PlanInputs {
    /// The key of every level, by level index.
    pub(super) level_keys: Vec<LevelKey>,
    /// The input of every metric, by metric index.
    pub(super) metric_inputs: Vec<MetricInput>,
}

/// The key of every level and the input of every metric of `layout`, over the rows plan whose
/// schema is `rows`.
///
/// # Errors
///
/// `400 illegal_argument_exception` for a `missing` or an `include` / `exclude` value that does
/// not fit the field's type, for a pattern on a key that is not a string keyword, and for more
/// buckets in one level than an `Int32` ordinal numbers; `500` for a column the rows plan lacks.
pub(super) fn resolve_plan_inputs(
    layout: &AggregationLayout<'_>,
    rows: &DFSchema,
) -> Result<PlanInputs, OpenSearchError> {
    let level_keys = layout
        .levels
        .iter()
        .map(|level| plan_level_key(level, rows))
        .collect::<Result<Vec<_>, _>>()?;
    let metric_inputs = layout
        .metrics
        .iter()
        .map(|slot| {
            let field = read_row_field(rows, &slot.spec.field, slot.name)?;
            let fallback_field = slot
                .spec
                .fallback_field
                .as_deref()
                .map(|fallback_field| read_row_field(rows, fallback_field, slot.name))
                .transpose()?;
            plan_metric_input(slot.kind, slot.spec, field, fallback_field)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PlanInputs {
        level_keys,
        metric_inputs,
    })
}

/// The key of `level` for each matching row, as SQL over the rows plan.
///
/// A `histogram` value whose bucket index lies past the `i64` range is not refused here, since
/// it is known only at execution: its key is `HISTOGRAM_INDEX_OUT_OF_RANGE`, which
/// [`read_level_key`](super::assemble::read_level_key) refuses with a
/// `400 illegal_argument_exception` naming the aggregation.
///
/// A `range`, `date_range`, `filters` or `filter` level is keyed by bucket ordinal
/// ([`plan_ordinals`]): a scalar when no row can meet two of its conditions, a membership list
/// otherwise.
///
/// # Errors
///
/// The errors of [`resolve_plan_inputs`] for one level.
fn plan_level_key(level: &BucketLevel<'_>, rows: &DFSchema) -> Result<LevelKey, OpenSearchError> {
    let build_scalar_key = |expr: Expr| LevelKey {
        expr,
        is_membership_list: false,
    };
    match &level.kind {
        LevelKind::DateHistogram { spec, rounding } => {
            let field = read_row_field(rows, &spec.field, level.name)?.expr;
            let value = match &spec.missing {
                Some(missing) => {
                    let missing_micros = read_epoch_millis(level.name, "missing", missing)?
                        .checked_mul(MICROS_PER_MILLISECOND)
                        .ok_or_else(|| {
                            OpenSearchError::unsupported_aggregation(
                                level.name,
                                format!("[missing] of [{}] is out of range", level.name),
                            )
                        })?;
                    coalesce(vec![
                        field,
                        lit(ScalarValue::TimestampMicrosecond(Some(missing_micros), None)),
                    ])
                }
                None => field,
            };
            rounding.key_expression(value).map(build_scalar_key)
        }
        LevelKind::Terms { spec } => {
            let field = read_row_field(rows, &spec.field, level.name)?;
            let key = match &spec.missing {
                Some(missing) => coalesce(vec![
                    field.expr,
                    build_key_literal(level.name, "missing", missing, &field.data_type)?,
                ]),
                None => field.expr,
            };
            // `include` / `exclude` apply before ranking: a refused key becomes NULL, so its rows
            // form no group (and hold no aggregate state) and still count in the parent.
            match build_filter_predicate(level.name, spec, &key, &field.data_type)? {
                Some(admitted) => when(admitted, key).end().map(build_scalar_key).map_err(convert_plan_error),
                None => Ok(build_scalar_key(key)),
            }
        }
        LevelKind::Histogram { spec } => {
            // The key is the bucket index: integral, so gap filling walks exact indices. An index
            // outside the `i64` range (`NaN` included) has no `Int64` form, so it is keyed
            // `HISTOGRAM_INDEX_OUT_OF_RANGE`, which assembly refuses naming the aggregation.
            // `CASE` casts only the rows its condition admits: pinned by
            // `bucket_tree_tests::histogram_bucket_indices_past_the_i64_range_are_400s_naming_the_aggregation`.
            let field = read_row_field(rows, &spec.field, level.name)?.expr;
            let index = floor(cast(field, DataType::Float64) / lit(spec.interval));
            let is_in_range = index
                .clone()
                .gt(lit(-HISTOGRAM_INDEX_LIMIT))
                .and(index.clone().lt(lit(HISTOGRAM_INDEX_LIMIT)));
            when(is_in_range, cast(index.clone(), DataType::Int64))
                .when(index.is_not_null(), lit(HISTOGRAM_INDEX_OUT_OF_RANGE))
                .end()
                .map(build_scalar_key)
                .map_err(convert_plan_error)
        }
        LevelKind::Range { spec } => {
            let value = cast(read_row_field(rows, &spec.field, level.name)?.expr, DataType::Float64);
            let bounds: Vec<(Option<f64>, Option<f64>)> =
                spec.ranges.iter().map(|range| (range.from, range.to)).collect();
            plan_ordinals(plan_range_conditions(&value, &bounds), are_disjoint(&bounds))
        }
        LevelKind::DateRange { spec, bounds } => {
            let micros = cast(read_row_field(rows, &spec.field, level.name)?.expr, DataType::Int64);
            let bounds: Vec<(Option<i64>, Option<i64>)> =
                bounds.iter().map(|range| (range.from_micros, range.to_micros)).collect();
            plan_ordinals(plan_range_conditions(&micros, &bounds), are_disjoint(&bounds))
        }
        LevelKind::Filters {
            first_filter_ordinal,
            filter_count,
            ..
        } => {
            let conditions = (*first_filter_ordinal..first_filter_ordinal + filter_count)
                .map(|ordinal| col(name_bucket_filter_column(ordinal)))
                .collect();
            plan_ordinals(conditions, *filter_count == 1)
        }
        LevelKind::Filter { filter_ordinal } => {
            plan_ordinals(vec![col(name_bucket_filter_column(*filter_ordinal))], true)
        }
    }
}

/// One aggregate the planner executes, and how to tell its rows apart.
pub(super) struct StagePlan {
    /// The aggregate over the projected rows.
    pub(super) plan: LogicalPlan,
    /// Each granularity the aggregate computes, with the level mask of its rows.
    pub(super) members: Vec<(Granularity, i32)>,
    /// Whether the aggregate groups by GROUPING SETS and outputs `LEVEL_MASK_COLUMN`; without
    /// them it groups by nothing and its one row belongs to its only member.
    pub(super) has_grouping_sets: bool,
}

/// One exact distinct-count branch: the counts of `metrics` per bucket of `granularity`.
pub(super) struct DistinctPlan {
    /// The two-level distinct plan over the projected rows.
    pub(super) plan: LogicalPlan,
    /// The granularity whose buckets the counts belong to.
    pub(super) granularity: Granularity,
    /// The `cardinality` metrics counted, in the order of the plan's count columns.
    pub(super) metrics: Vec<usize>,
}

/// Every plan an aggregation request executes.
pub(super) struct AggregationPlans {
    /// The stage aggregates computing the buckets and the other metrics.
    pub(super) stages: Vec<StagePlan>,
    /// One distinct-count branch per granularity that holds a `cardinality`.
    pub(super) distinct_branches: Vec<DistinctPlan>,
}

/// The levels whose keys `granularity` groups by: the level's path, nothing at the top.
pub(super) fn list_granularity_levels<'l>(layout: &'l AggregationLayout<'_>, granularity: Granularity) -> &'l [usize] {
    match granularity {
        Granularity::Top => &[],
        Granularity::Level(level) => &layout.levels[level].path,
    }
}

/// The aggregates computing every bucket and metric of `layout` over `rows`; an empty layout
/// plans nothing.
///
/// Granularities whose paths cross the same membership-list levels share one stage aggregate
/// over the rows with those lists unnested. A row therefore repeats for the buckets it belongs to
/// at those levels and below them, never for an ancestor or a sibling. A request without
/// membership-list levels has at most one stage, and none when it has no bucket level and no
/// top-level metric but `cardinality`. `cardinality` metrics are counted exactly by one distinct
/// branch per granularity, over the same unnested rows. Each stage reads the matching rows once,
/// and each counted `cardinality` once more.
///
/// # Errors
///
/// `500` when a plan fails to type-check.
pub(super) fn plan_aggregations(
    rows: LogicalPlan,
    layout: &AggregationLayout<'_>,
    inputs: &PlanInputs,
) -> Result<AggregationPlans, OpenSearchError> {
    let mut stages: Vec<(Vec<usize>, Vec<Granularity>)> = Vec::new();
    for level in 0..layout.levels.len() {
        let granularity = Granularity::Level(level);
        add_stage_member(
            &mut stages,
            list_membership_chain(layout, inputs, granularity),
            granularity,
        );
    }
    // A `cardinality` computes no stage statistic, so it alone opens no top-level member. A
    // top-level metric reads the top stage's statistics, a top-level `top_hits` its document
    // count.
    if layout
        .metrics
        .iter()
        .any(|slot| slot.granularity == Granularity::Top && !slot.kind.list_statistics().is_empty())
        || layout.top_hits.iter().any(|slot| slot.granularity == Granularity::Top)
    {
        add_stage_member(&mut stages, Vec::new(), Granularity::Top);
    }
    let cardinalities_by_granularity = group_cardinalities(layout);
    if stages.is_empty() && cardinalities_by_granularity.is_empty() {
        return Ok(AggregationPlans {
            stages: Vec::new(),
            distinct_branches: Vec::new(),
        });
    }
    let projected_rows = project_inputs(rows, inputs)?.build().map_err(convert_plan_error)?;
    let distinct_branches = plan_distinct_branches(&projected_rows, layout, inputs, cardinalities_by_granularity)?;
    let stages = stages
        .into_iter()
        .map(|(chain, members)| plan_stage(unnest_memberships(&projected_rows, &chain)?, layout, &members))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AggregationPlans {
        stages,
        distinct_branches,
    })
}

/// The `cardinality` metrics of `layout`, grouped by the granularity they are read at, the
/// granularities in the order of their first metric.
fn group_cardinalities(layout: &AggregationLayout<'_>) -> Vec<(Granularity, Vec<usize>)> {
    let mut cardinalities_by_granularity: Vec<(Granularity, Vec<usize>)> = Vec::new();
    for (metric, slot) in layout.metrics.iter().enumerate() {
        if slot.kind == MetricKind::Cardinality {
            add_group_member(&mut cardinalities_by_granularity, slot.granularity, metric);
        }
    }
    cardinalities_by_granularity
}

/// One distinct-count branch per granularity of `cardinalities_by_granularity`, every
/// `cardinality` of the granularity counted by the same branch.
///
/// # Errors
///
/// `500` when a plan fails to type-check.
fn plan_distinct_branches(
    projected_rows: &LogicalPlan,
    layout: &AggregationLayout<'_>,
    inputs: &PlanInputs,
    cardinalities_by_granularity: Vec<(Granularity, Vec<usize>)>,
) -> Result<Vec<DistinctPlan>, OpenSearchError> {
    cardinalities_by_granularity
        .into_iter()
        .map(|(granularity, metrics)| {
            let relation = unnest_memberships(projected_rows, &list_membership_chain(layout, inputs, granularity))?;
            let keys: Vec<Expr> = list_granularity_levels(layout, granularity)
                .iter()
                .map(|level| col(name_key_column(*level)))
                .collect();
            let counted_expressions: Vec<Expr> = metrics.iter().map(|metric| col(name_value_column(*metric))).collect();
            let plan = plan_exact_distinct(relation, &keys, &counted_expressions)?
                .build()
                .map_err(convert_plan_error)?;
            Ok(DistinctPlan {
                plan,
                granularity,
                metrics,
            })
        })
        .collect()
}

/// The membership-list levels on `granularity`'s path, top first: the unnests its rows need.
pub(super) fn list_membership_chain(
    layout: &AggregationLayout<'_>,
    inputs: &PlanInputs,
    granularity: Granularity,
) -> Vec<usize> {
    list_granularity_levels(layout, granularity)
        .iter()
        .copied()
        .filter(|level| inputs.level_keys[*level].is_membership_list)
        .collect()
}

/// Add `member` to the stage of `chain`, opening that stage when it is the first member.
pub(super) fn add_stage_member(
    stages: &mut Vec<(Vec<usize>, Vec<Granularity>)>,
    chain: Vec<usize>,
    member: Granularity,
) {
    add_group_member(stages, chain, member);
}

/// Add `member` to the group of `key` in `groups`, opening that group, after the others, when it
/// is the first member: groups keep the order their first members arrived in.
fn add_group_member<K: PartialEq, M>(groups: &mut Vec<(K, Vec<M>)>, key: K, member: M) {
    if let Some((_, members)) = groups.iter_mut().find(|(group_key, _)| *group_key == key) {
        members.push(member);
        return;
    }
    groups.push((key, vec![member]));
}

/// The projected rows with the membership list of every level of `chain` unnested and its NULL
/// entries (conditions the row fails) dropped.
///
/// # Errors
///
/// `500` when the unnest or the filter fails to type-check.
pub(super) fn unnest_memberships(
    projected_rows: &LogicalPlan,
    chain: &[usize],
) -> Result<LogicalPlanBuilder, OpenSearchError> {
    chain
        .iter()
        .try_fold(LogicalPlanBuilder::from(projected_rows.clone()), |relation, level| {
            relation
                .unnest_column(Column::from_name(name_key_column(*level)))
                .and_then(|unnested_rows| unnested_rows.filter(col(name_key_column(*level)).is_not_null()))
                .map_err(convert_plan_error)
        })
}

/// The rows projected onto the level keys and metric values the aggregates read.
///
/// # Errors
///
/// `500` when the projection fails to type-check.
pub(super) fn project_inputs(rows: LogicalPlan, inputs: &PlanInputs) -> Result<LogicalPlanBuilder, OpenSearchError> {
    let keys = inputs
        .level_keys
        .iter()
        .enumerate()
        .map(|(level, key)| key.expr.clone().alias(name_key_column(level)));
    let values = inputs
        .metric_inputs
        .iter()
        .enumerate()
        .map(|(metric, input)| input.value.clone().alias(name_value_column(metric)));
    LogicalPlanBuilder::from(rows)
        .project(keys.chain(values))
        .map_err(convert_plan_error)
}

/// One aggregate over `relation` computing `members` and every metric read at one of them.
///
/// Every metric of the members is computed for every grouping set and read only at its own
/// granularity: one aggregate reads the rows once, where one aggregate per granularity would
/// read them once per granularity. The `percentiles` of the stage that read the same value share
/// one digest ([`SharedDigest`]); a `percentile_ranks` is one `rank_counts` aggregate; every other
/// statistic is one aggregate column of its metric.
///
/// # Errors
///
/// `500` when the aggregate fails to type-check.
pub(super) fn plan_stage(
    relation: LogicalPlanBuilder,
    layout: &AggregationLayout<'_>,
    members: &[Granularity],
) -> Result<StagePlan, OpenSearchError> {
    let mut aggregates = vec![count(lit(1_i64)).alias(DOC_COUNT_COLUMN)];
    let mut percentiles: Vec<(usize, &PercentilesSpec)> = Vec::new();
    for (metric, slot) in layout.metrics.iter().enumerate() {
        if !members.contains(&slot.granularity) {
            continue;
        }
        if let AggregationKind::Percentiles(spec) = slot.aggregation {
            percentiles.push((metric, spec));
        } else if let AggregationKind::PercentileRanks(spec) = slot.aggregation {
            aggregates.push(
                plan_rank_counts_aggregate(spec, col(name_value_column(metric)))
                    .alias(name_statistic_column(metric, Statistic::RankCounts)),
            );
        } else {
            let value = col(name_value_column(metric));
            for &statistic in slot.kind.list_statistics() {
                aggregates
                    .push(plan_statistic(statistic, value.clone())?.alias(name_statistic_column(metric, statistic)));
            }
        }
    }
    let digests = group_shared_digests(percentiles)?;
    aggregates.extend(digests.iter().map(SharedDigest::plan_digest));
    let grouped_levels: Vec<usize> = members
        .iter()
        .flat_map(|member| list_granularity_levels(layout, *member).iter().copied())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if grouped_levels.is_empty() {
        let aggregate = relation.aggregate(Vec::<Expr>::new(), aggregates).map_err(convert_plan_error)?;
        return Ok(StagePlan {
            plan: build_with_member_quantiles(aggregate, &digests)?,
            members: members.iter().map(|member| (*member, 0)).collect(),
            has_grouping_sets: false,
        });
    }
    let grouping_keys: Vec<Expr> = grouped_levels.iter().map(|level| col(name_key_column(*level))).collect();
    aggregates.push(grouping_udaf().call(grouping_keys).alias(LEVEL_MASK_COLUMN));
    let sets: Vec<Vec<Expr>> = members
        .iter()
        .map(|member| {
            list_granularity_levels(layout, *member)
                .iter()
                .map(|level| col(name_key_column(*level)))
                .collect()
        })
        .collect();
    let masks = members
        .iter()
        .map(|member| compute_level_mask(&grouped_levels, list_granularity_levels(layout, *member)))
        .collect::<Result<Vec<_>, _>>()?;
    let aggregate = relation
        .aggregate(vec![grouping_set(sets)], aggregates)
        .map_err(convert_plan_error)?;
    let members: Vec<(Granularity, i32)> = members.iter().copied().zip(masks).collect();
    Ok(StagePlan {
        plan: cut_ranked_terms_levels(build_with_member_quantiles(aggregate, &digests)?, layout, &members)?,
        members,
        has_grouping_sets: true,
    })
}

/// `grouped_rows`, a stage aggregate's output, with each `terms` level among `members` that ranks
/// by count or key ([`TermsSpec::is_ranked_by_count_or_key`]) cut to its `size` under every parent
/// bucket, so assembly reads at most `size` rows of such a level per parent bucket, whatever the
/// cardinality of its keys. A row is an admitted bucket of the level when its key path holds no
/// NULL and its document count reaches `min_doc_count`, the buckets assembly admits; the admitted
/// buckets of one parent are numbered by `row_number()` in the level's order and then by key
/// ascending, the order assembly ranks them by, so the rows kept are the buckets it keeps. Each
/// kept row carries the documents of every admitted bucket of its parent
/// ([`name_admitted_doc_count_column`]), which `sum_other_doc_count` reads. Rows of the other
/// granularities pass unchanged; without such a level `grouped_rows` is returned as it is.
///
/// # Errors
///
/// `500` when DataFusion refuses the projection, a window or the filter.
fn cut_ranked_terms_levels(
    grouped_rows: LogicalPlan,
    layout: &AggregationLayout<'_>,
    members: &[(Granularity, i32)],
) -> Result<LogicalPlan, OpenSearchError> {
    let ranked_levels: Vec<(usize, &TermsSpec, i32)> = members
        .iter()
        .filter_map(|&(granularity, mask)| match granularity {
            Granularity::Level(level) => match &layout.levels[level].kind {
                LevelKind::Terms { spec } => spec.is_ranked_by_count_or_key().then_some((level, *spec, mask)),
                LevelKind::DateHistogram { .. }
                | LevelKind::Histogram { .. }
                | LevelKind::Range { .. }
                | LevelKind::DateRange { .. }
                | LevelKind::Filters { .. }
                | LevelKind::Filter { .. } => None,
            },
            Granularity::Top => None,
        })
        .collect();
    if ranked_levels.is_empty() {
        return Ok(grouped_rows);
    }
    // The analyzer that resolves `grouping()` drops DataFusion's internal grouping-id column, so
    // no projection may name it (see `build_with_member_quantiles`).
    let output_columns: Vec<Expr> = grouped_rows
        .schema()
        .columns()
        .into_iter()
        .filter(|column| column.name != Aggregate::INTERNAL_GROUPING_ID)
        .map(Expr::Column)
        .collect();
    let mut flagged_columns = output_columns.clone();
    let mut cut_columns = output_columns;
    let mut windows = Vec::with_capacity(2 * ranked_levels.len());
    let mut is_kept = lit(true);
    for (level, spec, mask) in ranked_levels {
        let path = &layout.levels[level].path;
        let is_level_row = col(LEVEL_MASK_COLUMN).eq(lit(mask));
        let minimum = i64::try_from(spec.min_doc_count.max(1)).unwrap_or(i64::MAX);
        let is_admitted = path.iter().fold(
            is_level_row.clone().and(col(DOC_COUNT_COLUMN).gt_eq(lit(minimum))),
            |is_admitted, path_level| is_admitted.and(col(name_key_column(*path_level)).is_not_null()),
        );
        flagged_columns.push(is_admitted.alias(name_admitted_bucket_column(level)));
        // The flag leads the partition, so the rows of other granularities never share a
        // partition with the level's buckets.
        let partition: Vec<Expr> = std::iter::once(col(name_admitted_bucket_column(level)))
            .chain(path[..path.len() - 1].iter().map(|parent| col(name_key_column(*parent))))
            .collect();
        let key = col(name_key_column(level));
        let mut order = spec
            .order
            .iter()
            .map(|criterion| match criterion {
                TermsOrder::Count(direction) => Ok(col(DOC_COUNT_COLUMN).sort(*direction == SortOrder::Asc, false)),
                TermsOrder::Key(direction) => Ok(key.clone().sort(*direction == SortOrder::Asc, false)),
                TermsOrder::Metric { .. } => Err(OpenSearchError::internal(
                    "a terms level ordered by a metric was ranked in its stage aggregate",
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;
        order.push(key.sort(true, false));
        windows.push(
            row_number()
                .partition_by(partition.clone())
                .order_by(order)
                .build()
                .map_err(convert_plan_error)?
                .alias(name_bucket_rank_column(level)),
        );
        windows.push(
            Expr::from(WindowFunction::new(sum_udaf(), vec![col(DOC_COUNT_COLUMN)]))
                .partition_by(partition)
                .build()
                .map_err(convert_plan_error)?
                .alias(name_admitted_doc_count_column(level)),
        );
        let kept_buckets = u64::try_from(spec.size).unwrap_or(u64::MAX);
        is_kept = is_kept.and(not(is_level_row).or(
            col(name_admitted_bucket_column(level)).and(col(name_bucket_rank_column(level)).lt_eq(lit(kept_buckets))),
        ));
        cut_columns.push(col(name_admitted_doc_count_column(level)));
    }
    // Each window gets a node of its own, so no node mixes partitions or orders.
    windows
        .into_iter()
        .try_fold(
            LogicalPlanBuilder::from(grouped_rows)
                .project(flagged_columns)
                .map_err(convert_plan_error)?,
            |ranked_rows, window| ranked_rows.window(vec![window]),
        )
        .and_then(|ranked_rows| ranked_rows.filter(is_kept))
        .and_then(|kept_rows| kept_rows.project(cut_columns))
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)
}

/// `aggregate`, followed by the per-member `quantiles` columns of every shared digest. Without
/// a shared digest the aggregate is the plan, so no projection is paid for.
///
/// # Errors
///
/// `500` when the projection fails to type-check.
fn build_with_member_quantiles(
    aggregate: LogicalPlanBuilder,
    digests: &[SharedDigest<'_>],
) -> Result<LogicalPlan, OpenSearchError> {
    let member_quantiles: Vec<Expr> = digests.iter().flat_map(SharedDigest::plan_member_quantiles).collect();
    if member_quantiles.is_empty() {
        return aggregate.build().map_err(convert_plan_error);
    }
    // Every aggregate column keeps its name, the level mask included, so the assembler reads
    // the stage as before. The analyzer that resolves `grouping()` drops DataFusion's internal
    // grouping-id column, so the projection must not name it; pinned by
    // `bucket_tree_tests::percentiles_over_the_same_field_share_one_digest_and_read_their_own_percents`,
    // which fails when the projection names it.
    let aggregate_columns = aggregate
        .schema()
        .columns()
        .into_iter()
        .filter(|column| column.name != Aggregate::INTERNAL_GROUPING_ID)
        .map(Expr::Column);
    aggregate
        .project(aggregate_columns.chain(member_quantiles))
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)
}

/// The `grouping()` mask of the rows whose set is the key columns of `path`, when the aggregate
/// passes the keys of `grouped_levels` (ascending) to `grouping()`: the first argument is the
/// highest bit, and a set bit marks a level the set leaves out. DataFusion's layout of the mask
/// is pinned by
/// `bucket_tree_tests::grouping_marks_the_levels_a_set_omits_with_the_first_grouped_level_as_the_high_bit`,
/// which executes the aggregate.
///
/// # Errors
///
/// `500` for more grouped levels than the `i32` mask holds, which `MAX_BUCKET_LEVELS` rules
/// out.
pub(super) fn compute_level_mask(grouped_levels: &[usize], path: &[usize]) -> Result<i32, OpenSearchError> {
    let width = grouped_levels.len();
    grouped_levels
        .iter()
        .enumerate()
        .filter(|(_, level)| !path.contains(level))
        .try_fold(0_i32, |mask, (position, _)| {
            u32::try_from(width - 1 - position)
                .ok()
                .and_then(|shift| 1_i32.checked_shl(shift))
                .map(|bit| mask | bit)
        })
        .ok_or_else(|| OpenSearchError::internal("too many bucket levels for the level mask"))
}

/// Answer the aggregations of `tree` over `rows`.
///
/// `rows` holds one column per field the tree names, named by the field name and holding the
/// field's single type across the searched indices (a row lacking the field holds NULL), and one
/// `Boolean` column per `filter` / `filters` bucket query ([`plan_aggregation_rows`] builds it).
/// The answer maps each top-level aggregation's request name to its rendered result. The
/// aggregations are computed by at most one stage aggregate without an unnest, plus one per
/// distinct chain of levels whose buckets overlap, plus one distinct-count branch per granularity
/// holding a `cardinality` ([`plan_aggregations`]); every plan runs in `scope.session`, so the
/// plans share its memory pool and its limit. A `top_hits` runs after assembly, over the buckets
/// the response renders ([`collect_top_hits`]); `rows` then also carries the searched index's
/// table columns. The pipelines of `tree`, which [`plan_aggregation_rows`] validated, run last,
/// over the rendered tree ([`apply_pipelines`]). Assembly, the `top_hits` rendering, the
/// aggregations' rendering and the pipelines run on the blocking pool.
///
/// # Errors
///
/// The layout's errors ([`AggregationLayout::new`]); the errors of [`resolve_plan_inputs`];
/// `400 too_many_buckets_exception` past `scope.max_buckets`; `400 illegal_argument_exception`
/// naming a `histogram` whose bucket index for a value or a bound lies past the `i64` range;
/// the session's execution errors,
/// the memory limit among them; the errors of [`collect_top_hits`], the `top_hits` hits past
/// `scope.max_result_window` among them; the value errors of [`apply_pipelines`]; `500` when
/// planning fails or blocking work does not finish.
pub(crate) async fn execute_aggregations(
    rows: LogicalPlan,
    tree: Arc<AggregationTree>,
    scope: &AggregationScope<'_>,
) -> Result<Map<String, Value>, OpenSearchError> {
    let layout = AggregationLayout::new(&tree, scope.now)?;
    let inputs = resolve_plan_inputs(&layout, rows.schema())?;
    // Only a tree holding a `top_hits` reads the matching rows again, after assembly. Cloning a
    // plan copies its root node; its inputs are shared.
    let hit_rows = (!layout.top_hits.is_empty()).then(|| rows.clone());
    let plans = plan_aggregations(rows, &layout, &inputs)?;
    let session = scope.session;
    let stage_outputs = try_join_all(plans.stages.into_iter().map(
        |StagePlan {
             plan,
             members,
             has_grouping_sets,
         }| async move {
            Ok::<_, OpenSearchError>(StageOutput::new(
                members,
                has_grouping_sets,
                session.collect(plan).await?,
            ))
        },
    ));
    let distinct_outputs = try_join_all(plans.distinct_branches.into_iter().map(
        |DistinctPlan {
             plan,
             granularity,
             metrics,
         }| async move {
            Ok::<_, OpenSearchError>(DistinctOutput::new(granularity, metrics, session.collect(plan).await?))
        },
    ));
    let (outputs, distinct_outputs) = futures::try_join!(stage_outputs, distinct_outputs)?;
    // Assembly and rendering read every collected row and answered bucket, so they run on the
    // blocking pool. Blocking work borrows nothing from this task and a layout borrows its tree,
    // so each closure lays out its own `Arc` of the tree; the plan inputs are owned and move into
    // the rendering.
    let now = scope.now;
    let max_buckets = scope.max_buckets;
    let assembly_tree = Arc::clone(&tree);
    let assembled = run_blocking_work("assembling aggregation buckets", move || {
        let layout = AggregationLayout::new(&assembly_tree, now)?;
        assemble_aggregations(&layout, outputs, &distinct_outputs, max_buckets)
    })
    .await?;
    let top_hits_results = match hit_rows {
        Some(hit_rows) => collect_top_hits(&hit_rows, &layout, &inputs, &assembled, scope).await?,
        None => TopHitsResults::default(),
    };
    run_blocking_work("rendering aggregations", move || {
        let layout = AggregationLayout::new(&tree, now)?;
        let mut aggregations = render_aggregations(&layout, &inputs, &assembled, top_hits_results)?;
        apply_pipelines(&tree.aggregations, &mut aggregations)?;
        Ok(aggregations)
    })
    .await
}

/// One field the aggregations read, resolved in every searched index.
struct AggregationField {
    /// The field name, which is also the column name in the rows plan.
    name: String,
    /// The type the field holds in the rows plan.
    data_type: DataType,
    /// The field's value in each searched index, in `entries` order; `None` where the index
    /// lacks the field.
    values: Vec<Option<FieldValue>>,
}

/// The matching rows of a search, projected onto what `tree` reads.
///
/// One branch per index: its scan, filtered by the query lowered for that index, projected onto
/// one column per field (NULL where the index lacks the field), every column cast to the field's
/// single type, and onto one `Boolean` column per `filter` / `filters` bucket query
/// ([`name_bucket_filter_column`]): the bucket query lowered for that index, `false` where it
/// does not match, so a field the index does not map matches nothing there. The branches are
/// concatenated with `UNION ALL`; because every branch already has the same column types, no
/// union coercion takes place. With no index the plan is empty. A tree holding a `top_hits`
/// reads one index: each row then also carries every table column of that index under
/// [`name_hit_column`], and a search of several indices is a `400 illegal_argument_exception`
/// naming the aggregation under `aggregation`.
///
/// # Errors
///
/// The layout's errors ([`AggregationLayout::new`]); `400 aggregation_execution_exception` for a
/// field an aggregation reads that the searched indices map to different types, or store as
/// different Arrow types, with `field` and `indices_by_type` naming the indices of each type; `400
/// illegal_argument_exception` for a field an aggregation cannot read (as
/// [`check_field_for_aggregation`] lists them), with the `field`, its `field_type`, the `index`,
/// the `aggregation` and its `aggregation_type`; the structural pipeline errors of
/// [`validate_pipelines`], which are checked first, before any plan is built; the lowering's
/// errors; `500` when planning fails.
pub(crate) fn plan_aggregation_rows(
    session: &TenantSession,
    entries: &[&IndexEntry],
    query: &Query,
    now: DateTime<Utc>,
    tree: &AggregationTree,
) -> Result<LogicalPlan, OpenSearchError> {
    validate_pipelines(&tree.aggregations)?;
    let layout = AggregationLayout::new(tree, now)?;
    if let Some(slot) = layout.top_hits.first()
        && entries.len() > 1
    {
        let names: Vec<&str> = entries.iter().map(|entry| entry.name()).collect();
        return Err(OpenSearchError::unsupported_aggregation(
            slot.name,
            format!(
                "[top_hits] [{}] reads one index, and this search reaches [{}]",
                slot.name,
                names.join(", ")
            ),
        ));
    }
    let fields = resolve_aggregation_fields(session, entries, tree)?;
    let mut branches = Vec::with_capacity(entries.len());
    for (position, &entry) in entries.iter().enumerate() {
        let scope = LoweringScope { session, entry, now };
        let predicate = lower_query(query, &scope)?;
        let mut projection = fields
            .iter()
            .map(|field| build_branch_column(field, position))
            .collect::<Result<Vec<_>, _>>()?;
        for (ordinal, bucket_query) in layout.bucket_queries.iter().enumerate() {
            let bucket_match = lower_query(bucket_query, &scope)?;
            projection.push(coalesce(vec![bucket_match, lit(false)]).alias(name_bucket_filter_column(ordinal)));
        }
        let scan = session.scan_index(entry)?;
        if !layout.top_hits.is_empty() {
            // Every table column a hit renders, under a name no aggregated field takes.
            projection.extend(scan.schema().columns().into_iter().map(|column| {
                let name = name_hit_column(column.name());
                Expr::Column(column).alias(name)
            }));
        }
        let branch = scan
            .filter(predicate)
            .and_then(|builder| builder.project(projection))
            .and_then(LogicalPlanBuilder::build)
            .map_err(convert_plan_error)?;
        branches.push(branch);
    }
    let mut branches = branches.into_iter();
    let Some(first_branch) = branches.next() else {
        let mut nulls = fields
            .iter()
            .map(|field| build_typed_null(&field.data_type).map(|null| null.alias(field.name.as_str())))
            .collect::<Result<Vec<_>, _>>()?;
        nulls.extend(
            (0..layout.bucket_queries.len()).map(|ordinal| lit(false).alias(name_bucket_filter_column(ordinal))),
        );
        return LogicalPlanBuilder::empty(false)
            .project(nulls)
            .and_then(LogicalPlanBuilder::build)
            .map_err(convert_plan_error);
    };
    branches
        .try_fold(LogicalPlanBuilder::from(first_branch), LogicalPlanBuilder::union)
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)
}

/// The distinct fields the aggregations of `tree` read ([`AggregationKind::read_fields`]), each
/// checked against every aggregation that reads it; `filter` and `filters` read none. An encoded id whose stored column admits NULL
/// is refused with [`OpenSearchError::unordered_missing_id`]: its missing value renders `""`,
/// which no bucket or count over the stored bytes reads.
fn resolve_aggregation_fields(
    session: &TenantSession,
    entries: &[&IndexEntry],
    tree: &AggregationTree,
) -> Result<Vec<AggregationField>, OpenSearchError> {
    let mut fields: Vec<AggregationField> = Vec::new();
    for aggregation in list_aggregations(tree) {
        for name in aggregation.kind.read_fields() {
            let resolved: Vec<Option<ResolvedField>> =
                entries.iter().map(|entry| session.resolve_field(entry, name)).collect();
            check_field_for_aggregation(aggregation, name, entries, &resolved)?;
            for (entry, field) in entries.iter().zip(&resolved) {
                if let Some(field) = field
                    && session.is_nullable_encoded_id(entry, field)?
                {
                    return Err(OpenSearchError::unordered_missing_id(name, entry.name(), "aggregation"));
                }
            }
            if fields.iter().all(|field| field.name != name)
                && let Some(data_type) = choose_row_data_type(aggregation, &resolved)
            {
                fields.push(AggregationField {
                    name: name.to_string(),
                    data_type,
                    values: resolved.into_iter().map(|field| field.map(|field| field.value)).collect(),
                });
            }
        }
    }
    Ok(fields)
}

/// Every aggregation of `tree`, parents before their children.
fn list_aggregations(tree: &AggregationTree) -> Vec<&NamedAggregation> {
    let mut listed = Vec::new();
    let mut unvisited_aggregations: Vec<&NamedAggregation> = tree.aggregations.iter().rev().collect();
    while let Some(aggregation) = unvisited_aggregations.pop() {
        listed.push(aggregation);
        unvisited_aggregations.extend(aggregation.children.iter().rev());
    }
    listed
}

/// Refuse a field mapped to different types, or stored as different Arrow types, across the
/// searched indices, and a field `aggregation` cannot read. A bucket or a numeric metric reads
/// numbers (`histogram`, `range`; `avg`, `sum`, `min`, `max`, `stats`, `percentiles` and
/// `percentile_ranks` also booleans and dates), a `date_histogram` and a `date_range` dates, a
/// `terms`, a `value_count` and a `cardinality` anything but text and objects; `filter`,
/// `filters` and the pipelines read no field. `extended_stats` reads numbers only.
fn check_field_for_aggregation(
    aggregation: &NamedAggregation,
    field_name: &str,
    entries: &[&IndexEntry],
    resolved: &[Option<ResolvedField>],
) -> Result<(), OpenSearchError> {
    let mapped_fields: Vec<(&str, &ResolvedField)> = entries
        .iter()
        .zip(resolved)
        .filter_map(|(entry, field)| field.as_ref().map(|field| (entry.name(), field)))
        .collect();
    let type_names: Vec<(&str, &str)> = mapped_fields
        .iter()
        .map(|(index, field)| (*index, field.field_type.name()))
        .collect();
    check_single_kind(field_name, "it is", &type_names)?;
    // One mapped type can still be stored two ways (an encoded id in one index, a string in
    // another): the rows plan casts every branch to one Arrow type, and that cast would fail
    // during execution.
    let stored_types: Vec<(&str, DataType)> = mapped_fields
        .iter()
        .filter_map(|(index, field)| map_field_data_type(field).map(|data_type| (*index, data_type)))
        .collect();
    check_single_kind(field_name, "its values are stored as", &stored_types)?;
    let Some(&(index, field)) = mapped_fields.first() else {
        return Ok(());
    };
    let field_type = field.field_type;
    let is_number = matches!(field_type, FieldType::Integer | FieldType::Long | FieldType::Double);
    let is_metric_input = is_number || matches!(field_type, FieldType::Boolean | FieldType::Date);
    let is_text_or_object = matches!(field_type, FieldType::Text | FieldType::Object);
    let refuse = |kind_name: &str| {
        Err(OpenSearchError::unsupported_aggregation_field(
            field_name,
            field_type.name(),
            index,
            &aggregation.name,
            kind_name,
        ))
    };
    match &aggregation.kind {
        AggregationKind::DateHistogram(_) if !matches!(field_type, FieldType::Date) => refuse("date_histogram"),
        AggregationKind::Terms(_) if is_text_or_object => refuse("terms"),
        AggregationKind::Histogram(_) if !is_number => refuse("histogram"),
        AggregationKind::Range(_) if !is_number => refuse("range"),
        AggregationKind::DateRange(_) if !matches!(field_type, FieldType::Date) => refuse("date_range"),
        AggregationKind::Avg(_) if !is_metric_input => refuse("avg"),
        AggregationKind::Sum(_) if !is_metric_input => refuse("sum"),
        AggregationKind::Min(_) if !is_metric_input => refuse("min"),
        AggregationKind::Max(_) if !is_metric_input => refuse("max"),
        AggregationKind::Stats(_) if !is_metric_input => refuse("stats"),
        AggregationKind::Percentiles(_) if !is_metric_input => refuse("percentiles"),
        AggregationKind::PercentileRanks(_) if !is_metric_input => refuse("percentile_ranks"),
        AggregationKind::ExtendedStats(_) if !is_number => refuse("extended_stats"),
        AggregationKind::ValueCount(_) if is_text_or_object => refuse("value_count"),
        AggregationKind::Cardinality(_) if is_text_or_object => refuse("cardinality"),
        AggregationKind::DateHistogram(_)
        | AggregationKind::Terms(_)
        | AggregationKind::Histogram(_)
        | AggregationKind::Avg(_)
        | AggregationKind::Sum(_)
        | AggregationKind::Min(_)
        | AggregationKind::Max(_)
        | AggregationKind::ValueCount(_)
        | AggregationKind::Stats(_)
        | AggregationKind::Cardinality(_)
        | AggregationKind::Percentiles(_)
        | AggregationKind::PercentileRanks(_)
        | AggregationKind::Range(_)
        | AggregationKind::DateRange(_)
        | AggregationKind::Filters(_)
        | AggregationKind::Filter(_)
        | AggregationKind::ExtendedStats(_)
        | AggregationKind::TopHits(_)
        | AggregationKind::CumulativeSum(_)
        | AggregationKind::Derivative(_)
        | AggregationKind::SerialDiff(_)
        | AggregationKind::MovingFn(_)
        | AggregationKind::MovingAvg(_)
        | AggregationKind::BucketScript(_)
        | AggregationKind::AvgBucket(_)
        | AggregationKind::SumBucket(_)
        | AggregationKind::MinBucket(_)
        | AggregationKind::MaxBucket(_) => Ok(()),
    }
}

/// `400 aggregation_execution_exception` when the indices of `kinds` give the field more than one
/// kind (a mapped type, or a stored Arrow type), naming the indices of each in the reason, which
/// `subject` introduces, and under `indices_by_type`.
fn check_single_kind<K: Ord + Display>(
    field_name: &str,
    subject: &str,
    kinds: &[(&str, K)],
) -> Result<(), OpenSearchError> {
    let mut indices_by_kind: BTreeMap<&K, Vec<&str>> = BTreeMap::new();
    for (index, kind) in kinds {
        indices_by_kind.entry(kind).or_default().push(*index);
    }
    if indices_by_kind.len() < 2 {
        return Ok(());
    }
    let listing = indices_by_kind
        .iter()
        .map(|(kind, indices)| format!("[{kind}] in [{}]", indices.join(", ")))
        .collect::<Vec<_>>()
        .join(" and ");
    let indices_by_type: Map<String, Value> = indices_by_kind
        .iter()
        .map(|(kind, indices)| (kind.to_string(), json!(indices)))
        .collect();
    Err(OpenSearchError::aggregation_field_conflict(
        field_name,
        &format!("[{field_name}] cannot be aggregated: {subject} {listing}"),
        indices_by_type,
    ))
}

/// The type a field holds in the rows plan: the type of its first mapping, or the kind's own
/// type when no searched index maps it (the column is then NULL in every row); `None` for the
/// kinds that read no field.
fn choose_row_data_type(aggregation: &NamedAggregation, resolved: &[Option<ResolvedField>]) -> Option<DataType> {
    let fallback = match &aggregation.kind {
        AggregationKind::DateHistogram(_) | AggregationKind::DateRange(_) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        AggregationKind::Terms(_) | AggregationKind::ValueCount(_) | AggregationKind::Cardinality(_) => DataType::Utf8,
        AggregationKind::Histogram(_)
        | AggregationKind::Range(_)
        | AggregationKind::Avg(_)
        | AggregationKind::Sum(_)
        | AggregationKind::Min(_)
        | AggregationKind::Max(_)
        | AggregationKind::Stats(_)
        | AggregationKind::Percentiles(_)
        | AggregationKind::PercentileRanks(_)
        | AggregationKind::ExtendedStats(_) => DataType::Float64,
        AggregationKind::Filters(_)
        | AggregationKind::Filter(_)
        | AggregationKind::TopHits(_)
        | AggregationKind::CumulativeSum(_)
        | AggregationKind::Derivative(_)
        | AggregationKind::SerialDiff(_)
        | AggregationKind::MovingFn(_)
        | AggregationKind::MovingAvg(_)
        | AggregationKind::BucketScript(_)
        | AggregationKind::AvgBucket(_)
        | AggregationKind::SumBucket(_)
        | AggregationKind::MinBucket(_)
        | AggregationKind::MaxBucket(_) => return None,
    };
    Some(resolved.iter().flatten().find_map(map_field_data_type).unwrap_or(fallback))
}

/// The Arrow type a field of `field`'s type holds in the rows plan; `None` for a type no
/// aggregation reads.
const fn map_field_data_type(field: &ResolvedField) -> Option<DataType> {
    match (&field.value, field.field_type) {
        // An encoded hex id is read as its stored bytes, which bucket keys render in hex: no cast
        // turns fixed-size binary into text.
        (FieldValue::HexId { width, .. } | FieldValue::Encoded(EncodedField::HexIdOrEmpty { width, .. }), _) => {
            Some(DataType::FixedSizeBinary(*width))
        }
        (_, FieldType::Keyword) => Some(DataType::Utf8),
        (_, FieldType::Date) => Some(DataType::Timestamp(TimeUnit::Microsecond, None)),
        (_, FieldType::Integer) => Some(DataType::Int32),
        (_, FieldType::Long) => Some(DataType::Int64),
        (_, FieldType::Double) => Some(DataType::Float64),
        (_, FieldType::Boolean) => Some(DataType::Boolean),
        (_, FieldType::Text | FieldType::Object) => None,
    }
}

/// The column of `field` in the branch of the index at `position`.
fn build_branch_column(field: &AggregationField, position: usize) -> Result<Expr, OpenSearchError> {
    let value = match field.values.get(position).and_then(Option::as_ref) {
        Some(value) => cast(value.build_read_expr(), field.data_type.clone()),
        None => build_typed_null(&field.data_type)?,
    };
    Ok(value.alias(field.name.as_str()))
}

fn build_typed_null(data_type: &DataType) -> Result<Expr, OpenSearchError> {
    ScalarValue::try_from(data_type).map(lit).map_err(convert_plan_error)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::http::StatusCode;
    use chrono::{DateTime, Utc};
    use datafusion::arrow::array::{
        ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
        TimestampMicrosecondArray,
    };
    use datafusion::common::ScalarValue;
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};
    use icegate_common::LOGS_TABLE;
    use icegate_common::schema::{COL_BODY, COL_RESOURCE_ATTRIBUTES, COL_SEVERITY_TEXT, COL_TIMESTAMP};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serde_json::{Map, Value, json};

    use super::{AggregationScope, execute_aggregations, plan_aggregation_rows};
    use crate::opensearch::SearchLimitsConfig;
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::opensearch::extract::parse_parameters;
    use crate::opensearch::search::aggs::parse::parse_aggregations;
    use crate::opensearch::search::aggs::test_support::parse_instant_micros;
    use crate::opensearch::search::request::{SearchParameters, parse_search_request};
    use crate::query_dsl::parse::parse_query;
    use crate::test_support::{FIXTURE_COMMITTED_WAL_OFFSET, SearchFixture, TestCell, TestRow, find_index_entry};

    /// A bucket limit no test reaches unless it sets its own.
    pub(super) fn read_default_max_buckets() -> usize {
        SearchLimitsConfig::default().max_buckets
    }

    /// The request instant every planner test that does not state its own runs at; a test that
    /// depends on it asserts it.
    pub(super) const fn read_request_now() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_791_462_896_789_000).expect("a valid instant")
    }

    /// Rows of one nullable `Timestamp(µs, None)` column named `field`.
    fn build_date_rows(field: &str, instants: &[Option<&str>]) -> RecordBatch {
        let values: TimestampMicrosecondArray = instants.iter().map(|text| text.map(parse_instant_micros)).collect();
        RecordBatch::try_from_iter(vec![(field, Arc::new(values) as ArrayRef)]).expect("rows batch")
    }

    /// A scan of `rows`, standing for the matching rows of a search.
    pub(super) fn scan_rows(rows: RecordBatch) -> LogicalPlan {
        let table = MemTable::try_new(rows.schema(), vec![vec![rows]]).expect("rows table");
        LogicalPlanBuilder::scan("rows", provider_as_source(Arc::new(table)), None)
            .and_then(LogicalPlanBuilder::build)
            .expect("rows plan")
    }

    /// Answer `aggregations` over `rows` with the production entry point, executing its plans in
    /// a tenant session of an in-memory warehouse.
    pub(super) async fn aggregate_plan(
        rows: LogicalPlan,
        aggregations: &Value,
        max_buckets: usize,
    ) -> Result<Value, OpenSearchError> {
        let tree = parse_aggregations(aggregations)?;
        let fixture = SearchFixture::new().await;
        let session = fixture.open_session("tenant-a").await;
        let scope = AggregationScope {
            session: &session,
            entries: &[],
            max_buckets,
            max_result_window: SearchLimitsConfig::default().max_result_window,
            now: read_request_now(),
        };
        execute_aggregations(rows, Arc::new(tree), &scope).await.map(Value::Object)
    }

    async fn aggregate_rows(rows: RecordBatch, aggregations: Value) -> Value {
        aggregate_plan(scan_rows(rows), &aggregations, read_default_max_buckets())
            .await
            .expect("the aggregations answer")
    }

    /// The rows of the New York examples: four around the 2025-11-02 change, four around the
    /// 2026-03-08 change, each a quarter of an hour inside a local day.
    fn build_new_york_day_rows() -> RecordBatch {
        build_date_rows(
            "timestamp",
            &[
                Some("2025-11-02T03:30:00Z"),
                Some("2025-11-02T04:30:00Z"),
                Some("2025-11-03T04:30:00Z"),
                Some("2025-11-03T05:30:00Z"),
                Some("2026-03-08T04:30:00Z"),
                Some("2026-03-08T05:30:00Z"),
                Some("2026-03-09T03:30:00Z"),
                Some("2026-03-09T04:30:00Z"),
            ],
        )
    }

    fn build_expected_new_york_day_buckets() -> Value {
        json!([
            {"key_as_string": "2025-11-01T00:00:00.000-04:00", "key": 1_761_969_600_000_i64, "doc_count": 1},
            {"key_as_string": "2025-11-02T00:00:00.000-04:00", "key": 1_762_056_000_000_i64, "doc_count": 2},
            {"key_as_string": "2025-11-03T00:00:00.000-05:00", "key": 1_762_146_000_000_i64, "doc_count": 1},
            {"key_as_string": "2026-03-07T00:00:00.000-05:00", "key": 1_772_859_600_000_i64, "doc_count": 1},
            {"key_as_string": "2026-03-08T00:00:00.000-05:00", "key": 1_772_946_000_000_i64, "doc_count": 2},
            {"key_as_string": "2026-03-09T00:00:00.000-04:00", "key": 1_773_028_800_000_i64, "doc_count": 1}
        ])
    }

    #[tokio::test]
    async fn calendar_days_in_new_york_are_23_and_25_hours_keyed_at_local_midnight() {
        // 2025-11-02 lasts 25 hours (04:00Z to 05:00Z the next day): 2025-11-03T04:30Z is 23:30
        // EST and still in it. 2026-03-08 lasts 23 hours (05:00Z to 04:00Z): 2026-03-09T03:30Z is
        // 23:30 EDT and still in it.
        let answer = aggregate_rows(
            build_new_york_day_rows(),
            json!({"days": {"date_histogram": {
                "field": "timestamp", "calendar_interval": "1d", "time_zone": "America/New_York", "min_doc_count": 1
            }}}),
        )
        .await;

        assert_eq!(
            answer,
            json!({"days": {"buckets": build_expected_new_york_day_buckets()}})
        );
    }

    /// A log of `tenant` at the RFC 3339 instant `text`, every other column at its default.
    fn build_timestamped_log<'a>(tenant: &'a str, text: &str) -> TestRow<'a> {
        TestRow {
            tenant,
            cells: vec![(COL_TIMESTAMP, TestCell::Micros(parse_instant_micros(text)))],
        }
    }

    #[tokio::test]
    async fn calendar_days_in_new_york_over_committed_and_wal_rows_answer_only_the_tenants_rows() {
        // Tenant A has one row per local day: 2025-11-01 23:30 EDT and 2025-11-02 23:30 EST in
        // Parquet, 2025-11-03 00:30 EST in the WAL. Tenant B's rows fall in the last two days and
        // would turn their counts into 2 if they leaked.
        let fixture = SearchFixture::new().await;
        fixture
            .commit_rows(
                LOGS_TABLE,
                &[
                    build_timestamped_log("tenant-a", "2025-11-02T03:30:00Z"),
                    build_timestamped_log("tenant-b", "2025-11-02T04:30:00Z"),
                    build_timestamped_log("tenant-a", "2025-11-03T04:30:00Z"),
                ],
            )
            .await;
        fixture
            .write_wal_segment(
                LOGS_TABLE,
                FIXTURE_COMMITTED_WAL_OFFSET + 1,
                &[
                    build_timestamped_log("tenant-b", "2025-11-03T05:00:00Z"),
                    build_timestamped_log("tenant-a", "2025-11-03T05:30:00Z"),
                ],
            )
            .await;
        let session = fixture.open_session("tenant-a").await;
        let entry = find_index_entry(&session, LOGS_TABLE);
        let tree = parse_aggregations(&json!({"days": {"date_histogram": {
            "field": "timestamp", "calendar_interval": "1d", "time_zone": "America/New_York", "min_doc_count": 1
        }}}))
        .expect("a date_histogram");
        let query = parse_query(&json!({"match_all": {}})).expect("match_all");
        let now = DateTime::parse_from_rfc3339("2026-10-08T12:00:00Z")
            .expect("now")
            .with_timezone(&Utc);

        let rows = plan_aggregation_rows(&session, &[entry], &query, now, &tree).expect("the rows plan");
        let scope = AggregationScope {
            session: &session,
            entries: &[entry],
            max_buckets: read_default_max_buckets(),
            max_result_window: SearchLimitsConfig::default().max_result_window,
            now,
        };
        let answer = execute_aggregations(rows, Arc::new(tree), &scope)
            .await
            .expect("the aggregations answer");

        assert_eq!(
            Value::Object(answer),
            json!({"days": {"buckets": [
                {"key_as_string": "2025-11-01T00:00:00.000-04:00", "key": 1_761_969_600_000_i64, "doc_count": 1},
                {"key_as_string": "2025-11-02T00:00:00.000-04:00", "key": 1_762_056_000_000_i64, "doc_count": 1},
                {"key_as_string": "2025-11-03T00:00:00.000-05:00", "key": 1_762_146_000_000_i64, "doc_count": 1}
            ]}})
        );
    }

    #[tokio::test]
    async fn fixed_hours_in_new_york_skip_the_spring_forward_hour_and_repeat_the_fall_back_hour() {
        let rows = build_date_rows(
            "timestamp",
            &[
                Some("2025-11-02T05:30:00Z"),
                Some("2025-11-02T06:30:00Z"),
                Some("2025-11-02T07:30:00Z"),
                Some("2026-03-08T06:30:00Z"),
                Some("2026-03-08T07:30:00Z"),
            ],
        );

        let answer = aggregate_rows(
            rows,
            json!({"hours": {"date_histogram": {
                "field": "timestamp", "fixed_interval": "1h", "time_zone": "America/New_York", "min_doc_count": 1
            }}}),
        )
        .await;

        assert_eq!(
            answer,
            json!({"hours": {"buckets": [
                {"key_as_string": "2025-11-02T01:00:00.000-04:00", "key": 1_762_059_600_000_i64, "doc_count": 1},
                {"key_as_string": "2025-11-02T01:00:00.000-05:00", "key": 1_762_063_200_000_i64, "doc_count": 1},
                {"key_as_string": "2025-11-02T02:00:00.000-05:00", "key": 1_762_066_800_000_i64, "doc_count": 1},
                {"key_as_string": "2026-03-08T01:00:00.000-05:00", "key": 1_772_949_600_000_i64, "doc_count": 1},
                {"key_as_string": "2026-03-08T03:00:00.000-04:00", "key": 1_772_953_200_000_i64, "doc_count": 1}
            ]}})
        );
    }

    #[tokio::test]
    async fn kolkata_buckets_are_keyed_at_half_past_utc() {
        let rows = build_date_rows(
            "timestamp",
            &[Some("2026-10-08T17:00:00Z"), Some("2026-10-08T20:00:00Z")],
        );

        let answer = aggregate_rows(
            rows,
            json!({
                "days": {"date_histogram": {"field": "timestamp", "calendar_interval": "1d", "time_zone": "Asia/Kolkata", "min_doc_count": 1}},
                "hours": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "time_zone": "Asia/Kolkata", "min_doc_count": 1}}
            }),
        )
        .await;

        assert_eq!(
            answer,
            json!({
                "days": {"buckets": [
                    {"key_as_string": "2026-10-08T00:00:00.000+05:30", "key": 1_791_397_800_000_i64, "doc_count": 1},
                    {"key_as_string": "2026-10-09T00:00:00.000+05:30", "key": 1_791_484_200_000_i64, "doc_count": 1}
                ]},
                "hours": {"buckets": [
                    {"key_as_string": "2026-10-08T22:00:00.000+05:30", "key": 1_791_477_000_000_i64, "doc_count": 1},
                    {"key_as_string": "2026-10-09T01:00:00.000+05:30", "key": 1_791_487_800_000_i64, "doc_count": 1}
                ]}
            })
        );
    }

    #[tokio::test]
    async fn legacy_one_day_answers_like_calendar_one_day() {
        let answer = aggregate_rows(
            build_new_york_day_rows(),
            json!({
                "legacy": {"date_histogram": {"field": "timestamp", "interval": "1d", "time_zone": "America/New_York", "min_doc_count": 1}},
                "calendar": {"date_histogram": {"field": "timestamp", "calendar_interval": "1d", "time_zone": "America/New_York", "min_doc_count": 1}}
            }),
        )
        .await;

        assert_eq!(
            answer,
            json!({
                "legacy": {"buckets": build_expected_new_york_day_buckets()},
                "calendar": {"buckets": build_expected_new_york_day_buckets()}
            })
        );
    }

    #[tokio::test]
    async fn legacy_intervals_bucket_as_their_class_in_utc_and_new_york() {
        // `1M` is one calendar month; `30d` is a fixed 30 days binned from the epoch, so its
        // buckets start on 2026-01-07 and 2026-02-06.
        let utc = aggregate_rows(
            build_date_rows(
                "timestamp",
                &[Some("2026-01-15T12:00:00Z"), Some("2026-02-20T12:00:00Z")],
            ),
            json!({
                "month": {"date_histogram": {"field": "timestamp", "interval": "1M"}},
                "thirty_days": {"date_histogram": {"field": "timestamp", "interval": "30d"}}
            }),
        )
        .await;
        // `1w` is one local week from Monday; `7d` is a fixed seven days on the local clock from
        // 1970-01-01, a Thursday, so 2026-10-08 (a Thursday) starts its bucket.
        let new_york = aggregate_rows(
            build_date_rows("timestamp", &[Some("2026-10-08T12:00:00Z")]),
            json!({
                "week": {"date_histogram": {"field": "timestamp", "interval": "1w", "time_zone": "America/New_York"}},
                "seven_days": {"date_histogram": {"field": "timestamp", "interval": "7d", "time_zone": "America/New_York"}}
            }),
        )
        .await;

        assert_eq!(
            utc,
            json!({
                "month": {"buckets": [
                    {"key_as_string": "2026-01-01T00:00:00.000Z", "key": 1_767_225_600_000_i64, "doc_count": 1},
                    {"key_as_string": "2026-02-01T00:00:00.000Z", "key": 1_769_904_000_000_i64, "doc_count": 1}
                ]},
                "thirty_days": {"buckets": [
                    {"key_as_string": "2026-01-07T00:00:00.000Z", "key": 1_767_744_000_000_i64, "doc_count": 1},
                    {"key_as_string": "2026-02-06T00:00:00.000Z", "key": 1_770_336_000_000_i64, "doc_count": 1}
                ]}
            })
        );
        assert_eq!(
            new_york,
            json!({
                "week": {"buckets": [
                    {"key_as_string": "2026-10-05T00:00:00.000-04:00", "key": 1_791_172_800_000_i64, "doc_count": 1}
                ]},
                "seven_days": {"buckets": [
                    {"key_as_string": "2026-10-08T00:00:00.000-04:00", "key": 1_791_432_000_000_i64, "doc_count": 1}
                ]}
            })
        );
    }

    #[tokio::test]
    async fn gap_filling_in_a_dst_zone_enumerates_existing_local_bucket_starts() {
        let hours = json!({"hours": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "time_zone": "America/New_York"}}});
        // Spring forward: no bucket starts at local 02:00.
        let spring = aggregate_rows(
            build_date_rows(
                "timestamp",
                &[Some("2026-03-08T06:30:00Z"), Some("2026-03-08T08:30:00Z")],
            ),
            hours.clone(),
        )
        .await;
        // Fall back: local 01:00 starts two buckets.
        let fall = aggregate_rows(
            build_date_rows(
                "timestamp",
                &[Some("2025-11-02T04:30:00Z"), Some("2025-11-02T07:30:00Z")],
            ),
            hours,
        )
        .await;
        // The 23-hour local day is filled like any other day.
        let days = aggregate_rows(
            build_date_rows("timestamp", &[Some("2026-03-07T15:00:00Z"), Some("2026-03-09T15:00:00Z")]),
            json!({"days": {"date_histogram": {"field": "timestamp", "calendar_interval": "1d", "time_zone": "America/New_York"}}}),
        )
        .await;

        assert_eq!(
            spring,
            json!({"hours": {"buckets": [
                {"key_as_string": "2026-03-08T01:00:00.000-05:00", "key": 1_772_949_600_000_i64, "doc_count": 1},
                {"key_as_string": "2026-03-08T03:00:00.000-04:00", "key": 1_772_953_200_000_i64, "doc_count": 0},
                {"key_as_string": "2026-03-08T04:00:00.000-04:00", "key": 1_772_956_800_000_i64, "doc_count": 1}
            ]}})
        );
        assert_eq!(
            fall,
            json!({"hours": {"buckets": [
                {"key_as_string": "2025-11-02T00:00:00.000-04:00", "key": 1_762_056_000_000_i64, "doc_count": 1},
                {"key_as_string": "2025-11-02T01:00:00.000-04:00", "key": 1_762_059_600_000_i64, "doc_count": 0},
                {"key_as_string": "2025-11-02T01:00:00.000-05:00", "key": 1_762_063_200_000_i64, "doc_count": 0},
                {"key_as_string": "2025-11-02T02:00:00.000-05:00", "key": 1_762_066_800_000_i64, "doc_count": 1}
            ]}})
        );
        assert_eq!(
            days,
            json!({"days": {"buckets": [
                {"key_as_string": "2026-03-07T00:00:00.000-05:00", "key": 1_772_859_600_000_i64, "doc_count": 1},
                {"key_as_string": "2026-03-08T00:00:00.000-05:00", "key": 1_772_946_000_000_i64, "doc_count": 0},
                {"key_as_string": "2026-03-09T00:00:00.000-04:00", "key": 1_773_028_800_000_i64, "doc_count": 1}
            ]}})
        );
    }

    #[tokio::test]
    async fn extended_bounds_fill_empty_buckets_up_to_the_bounds() {
        // The bounds are 09:15Z and 12:00Z; the lower one rounds down to the 09:00Z bucket.
        let answer = aggregate_rows(
            build_date_rows("timestamp", &[Some("2026-10-08T10:30:00Z")]),
            json!({"hours": {"date_histogram": {
                "field": "timestamp", "fixed_interval": "1h", "min_doc_count": 0,
                "extended_bounds": {"min": 1_791_450_900_000_i64, "max": "1791460800000"}
            }}}),
        )
        .await;

        assert_eq!(
            answer,
            json!({"hours": {"buckets": [
                {"key_as_string": "2026-10-08T09:00:00.000Z", "key": 1_791_450_000_000_i64, "doc_count": 0},
                {"key_as_string": "2026-10-08T10:00:00.000Z", "key": 1_791_453_600_000_i64, "doc_count": 1},
                {"key_as_string": "2026-10-08T11:00:00.000Z", "key": 1_791_457_200_000_i64, "doc_count": 0},
                {"key_as_string": "2026-10-08T12:00:00.000Z", "key": 1_791_460_800_000_i64, "doc_count": 0}
            ]}})
        );
    }

    #[tokio::test]
    async fn offset_shifts_bucket_starts() {
        let rows = build_date_rows(
            "timestamp",
            &[
                Some("2026-10-08T05:00:00Z"),
                Some("2026-10-08T07:00:00Z"),
                Some("2026-10-08T10:10:00Z"),
                Some("2026-10-08T10:20:00Z"),
            ],
        );

        let answer = aggregate_rows(
            rows,
            json!({
                "days": {"date_histogram": {"field": "timestamp", "calendar_interval": "1d", "offset": "+6h", "min_doc_count": 1}},
                "hours": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "offset": "+15m", "min_doc_count": 1}}
            }),
        )
        .await;

        assert_eq!(
            answer,
            json!({
                "days": {"buckets": [
                    {"key_as_string": "2026-10-07T06:00:00.000Z", "key": 1_791_352_800_000_i64, "doc_count": 1},
                    {"key_as_string": "2026-10-08T06:00:00.000Z", "key": 1_791_439_200_000_i64, "doc_count": 3}
                ]},
                "hours": {"buckets": [
                    {"key_as_string": "2026-10-08T04:15:00.000Z", "key": 1_791_432_900_000_i64, "doc_count": 1},
                    {"key_as_string": "2026-10-08T06:15:00.000Z", "key": 1_791_440_100_000_i64, "doc_count": 1},
                    {"key_as_string": "2026-10-08T09:15:00.000Z", "key": 1_791_450_900_000_i64, "doc_count": 1},
                    {"key_as_string": "2026-10-08T10:15:00.000Z", "key": 1_791_454_500_000_i64, "doc_count": 1}
                ]}
            })
        );
    }

    #[tokio::test]
    async fn missing_places_rows_without_the_field_in_the_missing_bucket() {
        let rows = build_date_rows("observed", &[Some("2026-10-08T10:30:00Z"), None, None]);

        let answer = aggregate_rows(
            rows,
            json!({
                "with_missing": {"date_histogram": {"field": "observed", "fixed_interval": "1h", "missing": 1_791_457_200_000_i64}},
                "without_missing": {"date_histogram": {"field": "observed", "fixed_interval": "1h"}}
            }),
        )
        .await;

        assert_eq!(
            answer,
            json!({
                "with_missing": {"buckets": [
                    {"key_as_string": "2026-10-08T10:00:00.000Z", "key": 1_791_453_600_000_i64, "doc_count": 1},
                    {"key_as_string": "2026-10-08T11:00:00.000Z", "key": 1_791_457_200_000_i64, "doc_count": 2}
                ]},
                "without_missing": {"buckets": [
                    {"key_as_string": "2026-10-08T10:00:00.000Z", "key": 1_791_453_600_000_i64, "doc_count": 1}
                ]}
            })
        );
    }

    #[tokio::test]
    async fn key_as_string_follows_format() {
        let answer = aggregate_rows(
            build_date_rows("timestamp", &[Some("2026-10-08T10:30:00Z")]),
            json!({
                "epoch": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "format": "epoch_millis"}},
                "nanos": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "time_zone": "Asia/Kolkata", "format": "strict_date_optional_time_nanos"}},
                "default": {"date_histogram": {"field": "timestamp", "calendar_interval": "1h", "time_zone": "Asia/Kolkata"}}
            }),
        )
        .await;

        assert_eq!(
            answer,
            json!({
                "epoch": {"buckets": [
                    {"key_as_string": "1791453600000", "key": 1_791_453_600_000_i64, "doc_count": 1}
                ]},
                "nanos": {"buckets": [
                    {"key_as_string": "2026-10-08T16:00:00.000000+05:30", "key": 1_791_455_400_000_i64, "doc_count": 1}
                ]},
                "default": {"buckets": [
                    {"key_as_string": "2026-10-08T16:00:00.000+05:30", "key": 1_791_455_400_000_i64, "doc_count": 1}
                ]}
            })
        );
    }

    /// A log-shaped row: severity, service, timestamp, and a second, nullable date.
    struct LiteralRow<'a> {
        severity_text: Option<&'a str>,
        service_name: Option<&'a str>,
        timestamp: &'a str,
        observed: Option<&'a str>,
    }

    /// The batch of `rows`, one column per field of [`LiteralRow`].
    fn build_literal_rows(rows: &[LiteralRow<'_>]) -> RecordBatch {
        let severity: StringArray = rows.iter().map(|row| row.severity_text).collect();
        let service: StringArray = rows.iter().map(|row| row.service_name).collect();
        let timestamp: TimestampMicrosecondArray =
            rows.iter().map(|row| Some(parse_instant_micros(row.timestamp))).collect();
        let observed: TimestampMicrosecondArray =
            rows.iter().map(|row| row.observed.map(parse_instant_micros)).collect();
        RecordBatch::try_from_iter(vec![
            ("severity_text", Arc::new(severity) as ArrayRef),
            ("service_name", Arc::new(service) as ArrayRef),
            ("timestamp", Arc::new(timestamp) as ArrayRef),
            ("observed", Arc::new(observed) as ArrayRef),
        ])
        .expect("rows batch")
    }

    /// A `checkout` row of `severity_text` at `timestamp`, without an observed date.
    const fn build_severity_row<'a>(severity_text: Option<&'a str>, timestamp: &'a str) -> LiteralRow<'a> {
        LiteralRow {
            severity_text,
            service_name: Some("checkout"),
            timestamp,
            observed: None,
        }
    }

    /// The answer of a `terms` level: `other_doc_count` documents outside `buckets`.
    fn build_terms_list(other_doc_count: u64, buckets: &Value) -> Value {
        json!({"doc_count_error_upper_bound": 0, "sum_other_doc_count": other_doc_count, "buckets": buckets})
    }

    #[tokio::test]
    async fn a_logs_volume_style_terms_over_a_date_histogram_answers_the_stated_buckets() {
        let rows = build_literal_rows(&[
            build_severity_row(Some("ERROR"), "2026-10-08T10:05:00Z"),
            build_severity_row(Some("INFO"), "2026-10-08T10:10:00Z"),
            build_severity_row(Some("INFO"), "2026-10-08T10:20:00Z"),
            build_severity_row(Some("ERROR"), "2026-10-08T10:40:00Z"),
            build_severity_row(None, "2026-10-08T10:50:00Z"),
            build_severity_row(Some("INFO"), "2026-10-08T11:15:00Z"),
            build_severity_row(Some("WARN"), "2026-10-08T11:45:00Z"),
        ]);

        let answer = aggregate_rows(
            rows,
            json!({"2": {
                "terms": {"field": "severity_text", "size": 500, "order": {"_count": "desc"}, "min_doc_count": 0, "missing": "unknown"},
                "aggs": {"3": {"date_histogram": {
                    "field": "timestamp", "interval": "1h", "min_doc_count": 0,
                    "extended_bounds": {"min": 1_791_453_600_000_i64, "max": 1_791_457_200_000_i64},
                    "format": "epoch_millis"
                }}}
            }}),
        )
        .await;

        let build_hour_buckets = |ten: u64, eleven: u64| {
            json!({"buckets": [
                {"key_as_string": "1791453600000", "key": 1_791_453_600_000_i64, "doc_count": ten},
                {"key_as_string": "1791457200000", "key": 1_791_457_200_000_i64, "doc_count": eleven}
            ]})
        };
        // INFO (3) and ERROR (2) lead by count; WARN and unknown tie at 1 and follow by key in
        // byte order, `W` (0x57) before `u` (0x75).
        assert_eq!(
            answer,
            json!({"2": build_terms_list(0, &json!([
                {"key": "INFO", "doc_count": 3, "3": build_hour_buckets(2, 1)},
                {"key": "ERROR", "doc_count": 2, "3": build_hour_buckets(2, 0)},
                {"key": "WARN", "doc_count": 1, "3": build_hour_buckets(0, 1)},
                {"key": "unknown", "doc_count": 1, "3": build_hour_buckets(1, 0)}
            ]))})
        );
    }

    #[tokio::test]
    async fn a_value_suggestion_style_terms_with_include_and_terminate_after_answers_the_stated_keys() {
        let rows = build_literal_rows(&[
            LiteralRow {
                severity_text: Some("INFO"),
                service_name: Some("checkout"),
                timestamp: "2026-10-08T10:00:00Z",
                observed: None,
            },
            LiteralRow {
                severity_text: Some("INFO"),
                service_name: Some("cart"),
                timestamp: "2026-10-08T10:01:00Z",
                observed: None,
            },
            LiteralRow {
                severity_text: Some("INFO"),
                service_name: Some("checkout"),
                timestamp: "2026-10-08T10:02:00Z",
                observed: None,
            },
            LiteralRow {
                severity_text: Some("INFO"),
                service_name: None,
                timestamp: "2026-10-08T10:03:00Z",
                observed: None,
            },
            LiteralRow {
                severity_text: Some("INFO"),
                service_name: Some("cart"),
                timestamp: "2026-10-08T10:04:00Z",
                observed: None,
            },
            LiteralRow {
                severity_text: Some("INFO"),
                service_name: Some("checkout"),
                timestamp: "2026-10-08T10:05:00Z",
                observed: None,
            },
        ]);
        let parameters: SearchParameters = parse_parameters(None).expect("no parameters");
        let request = parse_search_request(
            br#"{"size":0,"terminate_after":100000,"timeout":"1000ms","query":{"match_all":{}},"aggs":{"suggestions":{"terms":{"field":"service_name","include":".*","execution_hint":"map","shard_size":10}}}}"#,
            &parameters,
        )
        .expect("a value-suggestion-style request parses");
        let aggregations = request.aggregations.expect("the request carries aggregations");

        let answer = aggregate_plan(scan_rows(rows), &aggregations, read_default_max_buckets())
            .await
            .expect("the aggregations answer");

        assert_eq!(request.size, 0);
        assert_eq!(
            answer,
            json!({"suggestions": build_terms_list(0, &json!([
                {"key": "checkout", "doc_count": 3},
                {"key": "cart", "doc_count": 2}
            ]))})
        );
    }

    #[tokio::test]
    async fn min_doc_count_zero_emits_only_keys_present_in_the_matching_rows() {
        let rows = build_literal_rows(&[
            build_severity_row(Some("WARN"), "2026-10-08T09:30:00Z"),
            build_severity_row(Some("ERROR"), "2026-10-08T10:05:00Z"),
            build_severity_row(Some("INFO"), "2026-10-08T10:20:00Z"),
        ]);
        // The filter stands for the request's time range: the WARN row lies outside it.
        let ten_o_clock = lit(ScalarValue::TimestampMicrosecond(
            Some(parse_instant_micros("2026-10-08T10:00:00Z")),
            None,
        ));
        let rows_in_time_range = LogicalPlanBuilder::from(scan_rows(rows))
            .filter(col("timestamp").gt_eq(ten_o_clock))
            .and_then(LogicalPlanBuilder::build)
            .expect("the rows in the time range");

        let answer = aggregate_plan(
            rows_in_time_range,
            &json!({"levels": {"terms": {"field": "severity_text", "min_doc_count": 0}}}),
            read_default_max_buckets(),
        )
        .await
        .expect("the aggregations answer");

        assert_eq!(
            answer,
            json!({"levels": build_terms_list(0, &json!([
                {"key": "ERROR", "doc_count": 1},
                {"key": "INFO", "doc_count": 1}
            ]))})
        );
    }

    #[tokio::test]
    async fn the_missing_bucket_appears_only_when_a_matching_row_lacks_the_field() {
        let request = json!({"levels": {"terms": {"field": "severity_text", "missing": "unknown"}}});
        let answer_without_gap = aggregate_rows(
            build_literal_rows(&[
                build_severity_row(Some("ERROR"), "2026-10-08T10:05:00Z"),
                build_severity_row(Some("INFO"), "2026-10-08T10:20:00Z"),
            ]),
            request.clone(),
        )
        .await;
        let answer_with_gap = aggregate_rows(
            build_literal_rows(&[
                build_severity_row(Some("ERROR"), "2026-10-08T10:05:00Z"),
                build_severity_row(Some("INFO"), "2026-10-08T10:20:00Z"),
                build_severity_row(None, "2026-10-08T10:30:00Z"),
            ]),
            request,
        )
        .await;

        assert_eq!(
            answer_without_gap,
            json!({"levels": build_terms_list(0, &json!([
                {"key": "ERROR", "doc_count": 1},
                {"key": "INFO", "doc_count": 1}
            ]))})
        );
        assert_eq!(
            answer_with_gap,
            json!({"levels": build_terms_list(0, &json!([
                {"key": "ERROR", "doc_count": 1},
                {"key": "INFO", "doc_count": 1},
                {"key": "unknown", "doc_count": 1}
            ]))})
        );
    }

    #[tokio::test]
    async fn count_ties_break_by_key_ascending() {
        let rows = build_literal_rows(&[
            build_severity_row(Some("b"), "2026-10-08T10:00:00Z"),
            build_severity_row(Some("b"), "2026-10-08T10:01:00Z"),
            build_severity_row(Some("a"), "2026-10-08T10:02:00Z"),
            build_severity_row(Some("a"), "2026-10-08T10:03:00Z"),
            build_severity_row(Some("c"), "2026-10-08T10:04:00Z"),
            build_severity_row(Some("c"), "2026-10-08T10:05:00Z"),
            build_severity_row(Some("d"), "2026-10-08T10:06:00Z"),
        ]);

        let answer = aggregate_rows(
            rows,
            json!({"levels": {"terms": {"field": "severity_text", "size": 3}}}),
        )
        .await;

        assert_eq!(
            answer,
            json!({"levels": build_terms_list(1, &json!([
                {"key": "a", "doc_count": 2},
                {"key": "b", "doc_count": 2},
                {"key": "c", "doc_count": 2}
            ]))})
        );
    }

    #[tokio::test]
    async fn a_null_child_key_counts_in_the_parent_but_emits_no_child_bucket() {
        let rows = build_literal_rows(&[
            LiteralRow {
                severity_text: Some("ERROR"),
                service_name: None,
                timestamp: "2026-10-08T10:05:00Z",
                observed: Some("2026-10-08T10:05:00Z"),
            },
            LiteralRow {
                severity_text: Some("ERROR"),
                service_name: None,
                timestamp: "2026-10-08T10:06:00Z",
                observed: None,
            },
            LiteralRow {
                severity_text: Some("INFO"),
                service_name: None,
                timestamp: "2026-10-08T10:20:00Z",
                observed: Some("2026-10-08T10:20:00Z"),
            },
        ]);

        let answer = aggregate_rows(
            rows,
            json!({"levels": {
                "terms": {"field": "severity_text"},
                "aggs": {"seen": {"date_histogram": {"field": "observed", "fixed_interval": "1h", "min_doc_count": 1}}}
            }}),
        )
        .await;

        let ten_o_clock = json!({"buckets": [
            {"key_as_string": "2026-10-08T10:00:00.000Z", "key": 1_791_453_600_000_i64, "doc_count": 1}
        ]});
        assert_eq!(
            answer,
            json!({"levels": build_terms_list(0, &json!([
                {"key": "ERROR", "doc_count": 2, "seen": ten_o_clock},
                {"key": "INFO", "doc_count": 1, "seen": ten_o_clock}
            ]))})
        );
    }

    #[tokio::test]
    async fn exclude_drops_matching_keys_before_ranking() {
        let mut rows = Vec::new();
        for (key, count) in [("a", 5), ("b", 4), ("c", 3)] {
            for _ in 0..count {
                rows.push(build_severity_row(Some(key), "2026-10-08T10:00:00Z"));
            }
        }

        let answer = aggregate_rows(
            build_literal_rows(&rows),
            json!({
                "by_pattern": {"terms": {"field": "severity_text", "size": 2, "exclude": "a"}},
                "by_values": {"terms": {"field": "severity_text", "size": 1, "exclude": ["b"]}}
            }),
        )
        .await;

        // Excluding `a` before ranking leaves room for `c`; `by_values` keeps `a` and counts `c`
        // (3 rows) as other.
        assert_eq!(
            answer,
            json!({
                "by_pattern": build_terms_list(0, &json!([
                    {"key": "b", "doc_count": 4},
                    {"key": "c", "doc_count": 3}
                ])),
                "by_values": build_terms_list(3, &json!([
                    {"key": "a", "doc_count": 5}
                ]))
            })
        );
    }

    #[tokio::test]
    async fn min_doc_count_above_one_drops_smaller_buckets() {
        let mut rows = Vec::new();
        for (key, count) in [("a", 1), ("b", 2), ("c", 3)] {
            for _ in 0..count {
                rows.push(build_severity_row(Some(key), "2026-10-08T10:00:00Z"));
            }
        }

        let answer = aggregate_rows(
            build_literal_rows(&rows),
            json!({"levels": {"terms": {"field": "severity_text", "min_doc_count": 2}}}),
        )
        .await;

        // `a` holds one document, below the minimum: it is dropped before ranking, so it is not
        // counted as other either.
        assert_eq!(
            answer,
            json!({"levels": build_terms_list(0, &json!([
                {"key": "c", "doc_count": 3},
                {"key": "b", "doc_count": 2}
            ]))})
        );
    }

    #[tokio::test]
    async fn terms_buckets_up_to_max_buckets_are_answered_and_one_more_is_a_400() {
        // Three distinct keys, `size` well above every limit tried.
        let build_rows = || {
            build_literal_rows(&[
                build_severity_row(Some("a"), "2026-10-08T10:00:00Z"),
                build_severity_row(Some("b"), "2026-10-08T10:01:00Z"),
                build_severity_row(Some("c"), "2026-10-08T10:02:00Z"),
            ])
        };
        let request = json!({"levels": {"terms": {"field": "severity_text", "size": 100}}});

        let below_limit = aggregate_plan(scan_rows(build_rows()), &request, 4)
            .await
            .expect("three buckets fit four");
        let at_limit = aggregate_plan(scan_rows(build_rows()), &request, 3)
            .await
            .expect("three buckets fit three");
        let past_limit = aggregate_plan(scan_rows(build_rows()), &request, 2)
            .await
            .expect_err("three buckets exceed two");

        let expected = json!({"levels": build_terms_list(0, &json!([
            {"key": "a", "doc_count": 1},
            {"key": "b", "doc_count": 1},
            {"key": "c", "doc_count": 1}
        ]))});
        assert_eq!(below_limit, expected);
        assert_eq!(at_limit, expected);
        assert_too_many_buckets(past_limit, 2).await;
    }

    /// Rows of one column of every type a `terms` key is stored as, three rows each.
    fn build_typed_rows() -> RecordBatch {
        let low_id = [0x01_u8; 16];
        let high_id = [0xfe_u8; 16];
        let small_values = Int32Array::from(vec![5, -2, 5]);
        let count = Int64Array::from(vec![3, -7, 3]);
        let ratio = Float64Array::from(vec![-1.5, 2.25, 2.25]);
        let flag = BooleanArray::from(vec![true, false, true]);
        let trace_id = FixedSizeBinaryArray::try_from_iter([high_id, low_id, high_id].into_iter()).expect("the ids");
        let seen_instants: TimestampMicrosecondArray = [
            "2026-10-08T10:05:00Z",
            "2026-10-08T10:05:00Z",
            "2026-10-08T11:30:00.250Z",
        ]
        .into_iter()
        .map(|text| Some(parse_instant_micros(text)))
        .collect();
        RecordBatch::try_from_iter(vec![
            ("small", Arc::new(small_values) as ArrayRef),
            ("count", Arc::new(count) as ArrayRef),
            ("ratio", Arc::new(ratio) as ArrayRef),
            ("flag", Arc::new(flag) as ArrayRef),
            ("trace_id", Arc::new(trace_id) as ArrayRef),
            ("seen", Arc::new(seen_instants) as ArrayRef),
        ])
        .expect("rows batch")
    }

    #[tokio::test]
    async fn terms_keys_of_every_stored_type_render_their_stated_key_and_key_as_string() {
        let build_key_ordered_terms = |field: &str| json!({"terms": {"field": field, "order": {"_key": "asc"}}});

        let answer = aggregate_rows(
            build_typed_rows(),
            json!({
                "small": build_key_ordered_terms("small"),
                "count": build_key_ordered_terms("count"),
                "ratio": build_key_ordered_terms("ratio"),
                "flag": build_key_ordered_terms("flag"),
                "trace_id": build_key_ordered_terms("trace_id"),
                "seen": build_key_ordered_terms("seen")
            }),
        )
        .await;

        assert_eq!(
            answer,
            json!({
                "small": build_terms_list(0, &json!([
                    {"key": -2, "doc_count": 1},
                    {"key": 5, "doc_count": 2}
                ])),
                "count": build_terms_list(0, &json!([
                    {"key": -7, "doc_count": 1},
                    {"key": 3, "doc_count": 2}
                ])),
                "ratio": build_terms_list(0, &json!([
                    {"key": -1.5, "doc_count": 1},
                    {"key": 2.25, "doc_count": 2}
                ])),
                "flag": build_terms_list(0, &json!([
                    {"key": 0, "key_as_string": "false", "doc_count": 1},
                    {"key": 1, "key_as_string": "true", "doc_count": 2}
                ])),
                "trace_id": build_terms_list(0, &json!([
                    {"key": "01010101010101010101010101010101", "doc_count": 1},
                    {"key": "fefefefefefefefefefefefefefefefe", "doc_count": 2}
                ])),
                "seen": build_terms_list(0, &json!([
                    {"key": 1_791_453_900_000_i64, "key_as_string": "2026-10-08T10:05:00.000Z", "doc_count": 2},
                    {"key": 1_791_459_000_250_i64, "key_as_string": "2026-10-08T11:30:00.250Z", "doc_count": 1}
                ]))
            })
        );
    }

    #[tokio::test]
    async fn terms_refuses_a_pattern_on_a_non_keyword_key_and_a_text_or_object_field() {
        for field in ["count", "small", "ratio", "flag", "seen", "trace_id"] {
            let error = aggregate_plan(
                scan_rows(build_typed_rows()),
                &json!({"levels": {"terms": {"field": field, "include": ".*"}}}),
                read_default_max_buckets(),
            )
            .await
            .expect_err("a pattern applies to string keywords only");

            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{field}");
            assert_eq!(envelope["error"]["type"], "illegal_argument_exception", "{field}");
            // Only the encoded id is told apart: it takes a list of hex ids instead.
            assert_eq!(
                envelope["error"]["field_storage"] == "encoded_id",
                field == "trace_id",
                "{field}"
            );
        }

        let fixture = SearchFixture::new().await;
        let session = fixture.open_session("tenant-a").await;
        let entry = find_index_entry(&session, LOGS_TABLE);
        let query = parse_query(&json!({"match_all": {}})).expect("match_all");
        let now = DateTime::parse_from_rfc3339("2026-10-08T12:00:00Z")
            .expect("now")
            .with_timezone(&Utc);
        for (field, field_type) in [(COL_BODY, "text"), (COL_RESOURCE_ATTRIBUTES, "object")] {
            let tree = parse_aggregations(&json!({"levels": {"terms": {"field": field}}})).expect("a terms");

            let error = plan_aggregation_rows(&session, &[entry], &query, now, &tree)
                .expect_err("a text or an object field is not aggregated");

            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{field}");
            assert_eq!(envelope["error"]["type"], "illegal_argument_exception", "{field}");
            assert_eq!(envelope["error"]["field"], field);
            assert_eq!(envelope["error"]["field_type"], field_type);
            assert_eq!(envelope["error"]["index"], LOGS_TABLE);
            assert_eq!(envelope["error"]["aggregation"], "levels");
            assert_eq!(envelope["error"]["aggregation_type"], "terms");
        }
    }

    #[tokio::test]
    async fn histograms_and_numeric_metrics_refuse_a_field_they_cannot_read_naming_it() {
        let fixture = SearchFixture::new().await;
        let session = fixture.open_session("tenant-a").await;
        let entry = find_index_entry(&session, LOGS_TABLE);
        let query = parse_query(&json!({"match_all": {}})).expect("match_all");
        let now = DateTime::parse_from_rfc3339("2026-10-08T12:00:00Z")
            .expect("now")
            .with_timezone(&Utc);
        // Each body with the field it reads, that field's type in `logs`, and the refused kind.
        for (aggregation, field, field_type, aggregation_type) in [
            (
                json!({"histogram": {"field": COL_SEVERITY_TEXT, "interval": 10}}),
                COL_SEVERITY_TEXT,
                "keyword",
                "histogram",
            ),
            (
                json!({"histogram": {"field": COL_TIMESTAMP, "interval": 10}}),
                COL_TIMESTAMP,
                "date",
                "histogram",
            ),
            (
                json!({"avg": {"field": COL_SEVERITY_TEXT}}),
                COL_SEVERITY_TEXT,
                "keyword",
                "avg",
            ),
            (json!({"sum": {"field": COL_BODY}}), COL_BODY, "text", "sum"),
            (
                json!({"min": {"field": COL_RESOURCE_ATTRIBUTES}}),
                COL_RESOURCE_ATTRIBUTES,
                "object",
                "min",
            ),
            (
                json!({"max": {"field": COL_SEVERITY_TEXT}}),
                COL_SEVERITY_TEXT,
                "keyword",
                "max",
            ),
            (
                json!({"stats": {"field": COL_SEVERITY_TEXT}}),
                COL_SEVERITY_TEXT,
                "keyword",
                "stats",
            ),
            (
                json!({"value_count": {"field": COL_BODY}}),
                COL_BODY,
                "text",
                "value_count",
            ),
            (
                json!({"cardinality": {"field": COL_BODY}}),
                COL_BODY,
                "text",
                "cardinality",
            ),
        ] {
            let tree = parse_aggregations(&json!({"refused": aggregation.clone()})).expect("an aggregation");

            let error = plan_aggregation_rows(&session, &[entry], &query, now, &tree)
                .expect_err("the field cannot be read by the aggregation");

            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{aggregation}");
            assert_eq!(envelope["error"]["type"], "illegal_argument_exception", "{aggregation}");
            assert_eq!(envelope["error"]["field"], field, "{aggregation}");
            assert_eq!(envelope["error"]["field_type"], field_type, "{aggregation}");
            assert_eq!(envelope["error"]["aggregation"], "refused", "{aggregation}");
            assert_eq!(envelope["error"]["aggregation_type"], aggregation_type, "{aggregation}");
        }
        // `value_count` and `cardinality` count the values of a keyword, and the numeric metrics
        // read a date.
        for aggregation in [
            json!({"value_count": {"field": COL_SEVERITY_TEXT}}),
            json!({"cardinality": {"field": COL_SEVERITY_TEXT}}),
            json!({"min": {"field": COL_TIMESTAMP}}),
        ] {
            let tree = parse_aggregations(&json!({"accepted": aggregation.clone()})).expect("an aggregation");

            plan_aggregation_rows(&session, &[entry], &query, now, &tree).expect("the field is read");
        }
    }

    #[tokio::test]
    async fn a_cardinality_of_a_field_no_index_maps_counts_zero_over_matching_rows() {
        // The rows plan holds a field no index maps as a NULL column of the type the kind falls
        // back to; `instants` proves the rows reach the distinct branch.
        let fixture = SearchFixture::new().await;
        fixture
            .commit_rows(
                LOGS_TABLE,
                &[
                    build_timestamped_log("tenant-a", "2026-10-08T10:05:00Z"),
                    build_timestamped_log("tenant-a", "2026-10-08T10:40:00Z"),
                ],
            )
            .await;
        let session = fixture.open_session("tenant-a").await;
        let entry = find_index_entry(&session, LOGS_TABLE);
        let tree = parse_aggregations(&json!({
            "unmapped": {"cardinality": {"field": "no_such_field"}},
            "instants": {"cardinality": {"field": COL_TIMESTAMP}}
        }))
        .expect("two cardinalities");
        let query = parse_query(&json!({"match_all": {}})).expect("match_all");
        let now = read_request_now();

        let rows = plan_aggregation_rows(&session, &[entry], &query, now, &tree).expect("the rows plan");
        let scope = AggregationScope {
            session: &session,
            entries: &[entry],
            max_buckets: read_default_max_buckets(),
            max_result_window: SearchLimitsConfig::default().max_result_window,
            now,
        };
        let answer = execute_aggregations(rows, Arc::new(tree), &scope)
            .await
            .expect("the aggregations answer");

        assert_eq!(
            Value::Object(answer),
            json!({"unmapped": {"value": 0}, "instants": {"value": 2}})
        );
    }

    #[tokio::test]
    async fn ranges_and_date_ranges_refuse_a_field_they_cannot_read_naming_it() {
        let fixture = SearchFixture::new().await;
        let session = fixture.open_session("tenant-a").await;
        let entry = find_index_entry(&session, LOGS_TABLE);
        let query = parse_query(&json!({"match_all": {}})).expect("match_all");
        // Each body with the field it reads, that field's type in `logs`, and the refused kind.
        for (aggregation, field, field_type, aggregation_type) in [
            (
                json!({"range": {"field": COL_SEVERITY_TEXT, "ranges": [{"to": 10}]}}),
                COL_SEVERITY_TEXT,
                "keyword",
                "range",
            ),
            (
                json!({"range": {"field": COL_TIMESTAMP, "ranges": [{"to": 10}]}}),
                COL_TIMESTAMP,
                "date",
                "range",
            ),
            (
                json!({"date_range": {"field": COL_SEVERITY_TEXT, "ranges": [{"to": "now"}]}}),
                COL_SEVERITY_TEXT,
                "keyword",
                "date_range",
            ),
        ] {
            let tree = parse_aggregations(&json!({"refused": aggregation.clone()})).expect("an aggregation");

            let error = plan_aggregation_rows(&session, &[entry], &query, read_request_now(), &tree)
                .expect_err("the field cannot be read by the aggregation");

            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{aggregation}");
            assert_eq!(envelope["error"]["type"], "illegal_argument_exception", "{aggregation}");
            assert_eq!(envelope["error"]["field"], field, "{aggregation}");
            assert_eq!(envelope["error"]["field_type"], field_type, "{aggregation}");
            assert_eq!(envelope["error"]["aggregation"], "refused", "{aggregation}");
            assert_eq!(envelope["error"]["aggregation_type"], aggregation_type, "{aggregation}");
        }
        // A `date_range` reads a date, and `filter` / `filters` read no field.
        for aggregation in [
            json!({"date_range": {"field": COL_TIMESTAMP, "ranges": [{"to": "now"}]}}),
            json!({"filter": {"term": {"severity_text": "ERROR"}}}),
            json!({"filters": {"filters": [{"match_all": {}}]}}),
        ] {
            let tree = parse_aggregations(&json!({"accepted": aggregation.clone()})).expect("an aggregation");

            plan_aggregation_rows(&session, &[entry], &query, read_request_now(), &tree)
                .expect("the aggregation plans");
        }
    }

    /// The keys one round of the ranking property draws from, each listed in ascending key
    /// order, so the position of a key is its rank in the stated order.
    enum KeyDomain {
        /// Keywords, in byte order.
        Keyword(&'static [&'static str]),
        /// Integers.
        Long(&'static [i64]),
        /// Doubles, a negative one among them.
        Double(&'static [f64]),
        /// `false` before `true`.
        Boolean,
    }

    impl KeyDomain {
        fn count_keys(&self) -> usize {
            match self {
                Self::Keyword(keys) => keys.len(),
                Self::Long(keys) => keys.len(),
                Self::Double(keys) => keys.len(),
                Self::Boolean => 2,
            }
        }

        /// The column of the keys at `positions`.
        fn build_column(&self, positions: &[usize]) -> ArrayRef {
            match self {
                Self::Keyword(keys) => Arc::new(StringArray::from_iter_values(positions.iter().map(|&at| keys[at]))),
                Self::Long(keys) => Arc::new(Int64Array::from_iter_values(positions.iter().map(|&at| keys[at]))),
                Self::Double(keys) => Arc::new(Float64Array::from_iter_values(positions.iter().map(|&at| keys[at]))),
                Self::Boolean => Arc::new(BooleanArray::from(
                    positions.iter().map(|&at| at == 1).collect::<Vec<_>>(),
                )),
            }
        }

        /// The rendered key fields of the key at `position`.
        fn render_key(&self, position: usize, doc_count: usize) -> Value {
            match self {
                Self::Keyword(keys) => json!({"key": keys[position], "doc_count": doc_count}),
                Self::Long(keys) => json!({"key": keys[position], "doc_count": doc_count}),
                Self::Double(keys) => json!({"key": keys[position], "doc_count": doc_count}),
                Self::Boolean => json!({
                    "key": position,
                    "key_as_string": if position == 1 { "true" } else { "false" },
                    "doc_count": doc_count
                }),
            }
        }
    }

    #[tokio::test]
    async fn terms_ranking_matches_an_independent_oracle() {
        let domains = [
            KeyDomain::Keyword(&["B", "a", "ab", "b", "c", "\u{e9}"]),
            KeyDomain::Long(&[-40, -3, 0, 7, 12, 900]),
            KeyDomain::Double(&[-2.5, -0.75, 0.5, 1.25, 3.0, 10.0]),
            KeyDomain::Boolean,
        ];
        // Each order with its criteria as (by count, ascending), most significant first.
        let orders: [(Value, &[(bool, bool)]); 6] = [
            (json!({"_count": "desc"}), &[(true, false)]),
            (json!({"_count": "asc"}), &[(true, true)]),
            (json!({"_key": "asc"}), &[(false, true)]),
            (json!({"_key": "desc"}), &[(false, false)]),
            (
                json!([{"_count": "asc"}, {"_key": "desc"}]),
                &[(true, true), (false, false)],
            ),
            (json!([{"_count": "desc"}]), &[(true, false)]),
        ];
        let fixture = SearchFixture::new().await;
        let session = fixture.open_session("tenant-a").await;
        let scope = AggregationScope {
            session: &session,
            entries: &[],
            max_buckets: read_default_max_buckets(),
            max_result_window: SearchLimitsConfig::default().max_result_window,
            now: read_request_now(),
        };
        let mut random = StdRng::seed_from_u64(20_261_011);
        let mut tied_rounds = 0;
        for domain in &domains {
            for (order, criteria) in &orders {
                let key_count = domain.count_keys();
                let row_count = random.random_range(12..40);
                let positions: Vec<usize> = (0..row_count).map(|_| random.random_range(0..key_count)).collect();
                let mut counts = vec![0_usize; key_count];
                for &position in &positions {
                    counts[position] += 1;
                }
                let present_positions: Vec<usize> = (0..key_count).filter(|&position| counts[position] > 0).collect();
                // `size` below the distinct keys, so the ranking cuts at least one.
                assert!(
                    present_positions.len() >= 2,
                    "the round draws at least two keys: {counts:?}"
                );
                let size = random.random_range(1..present_positions.len());
                let mut oracle = present_positions.clone();
                // A sort by key position is a sort by key, since each domain lists its keys in
                // ascending order; the key ascending ends every tie.
                oracle.sort_by(|&left, &right| {
                    criteria
                        .iter()
                        .map(|&(is_by_count, is_ascending)| {
                            let ordering = if is_by_count {
                                counts[left].cmp(&counts[right])
                            } else {
                                left.cmp(&right)
                            };
                            if is_ascending { ordering } else { ordering.reverse() }
                        })
                        .find(|ordering| ordering.is_ne())
                        .unwrap_or_else(|| left.cmp(&right))
                });
                if present_positions.iter().any(|&left| {
                    present_positions
                        .iter()
                        .any(|&right| left != right && counts[left] == counts[right])
                }) {
                    tied_rounds += 1;
                }
                let answered_positions: Vec<usize> = oracle[..size].to_vec();
                let other_doc_count: usize = oracle[size..].iter().map(|&position| counts[position]).sum();
                let expected = build_terms_list(
                    u64::try_from(other_doc_count).expect("a count"),
                    &Value::Array(
                        answered_positions
                            .iter()
                            .map(|&position| domain.render_key(position, counts[position]))
                            .collect(),
                    ),
                );
                let rows = RecordBatch::try_from_iter(vec![("k", domain.build_column(&positions))]).expect("rows");
                let tree =
                    parse_aggregations(&json!({"ranked": {"terms": {"field": "k", "size": size, "order": order}}}))
                        .expect("a terms");

                let answer = execute_aggregations(scan_rows(rows), Arc::new(tree), &scope)
                    .await
                    .expect("the aggregations answer");

                assert_eq!(
                    Value::Object(answer),
                    json!({"ranked": expected}),
                    "order {order}, size {size}, counts {counts:?}"
                );
            }
        }
        // The property is only as strong as its ties: at least half the rounds hold equal counts.
        assert!(tied_rounds >= 12, "{tied_rounds} of 24 rounds hold tied counts");
    }

    #[tokio::test]
    async fn a_chain_past_the_most_bucket_levels_is_refused_naming_its_top() {
        // Seventeen nested `date_histogram`s, `level_0` outermost.
        let mut aggregations = json!({"level_16": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h"}}});
        for depth in (0..16).rev() {
            aggregations = json!({format!("level_{depth}"): {
                "date_histogram": {"field": "timestamp", "fixed_interval": "1h"},
                "aggs": aggregations
            }});
        }

        let error = aggregate_plan(
            scan_rows(build_date_rows("timestamp", &[Some("2026-10-08T10:30:00Z")])),
            &aggregations,
            read_default_max_buckets(),
        )
        .await
        .expect_err("refused");

        // Seventeen bucket aggregations under one top-level aggregation pass the request-wide
        // limit, which names that top-level aggregation.
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["aggregation"], "level_0");
        assert_eq!(body["error"]["limit"], 16);
    }

    #[tokio::test]
    async fn bucket_aggregations_past_the_most_per_request_are_refused_naming_the_top_level_one() {
        // `outer` holds fifteen sibling `date_histogram`s: sixteen bucket aggregations in all.
        let siblings: Map<String, Value> = (1..16)
            .map(|position| {
                (
                    format!("sibling_{position}"),
                    json!({"date_histogram": {"field": "timestamp", "fixed_interval": "1h"}}),
                )
            })
            .collect();
        let at_limit = json!({"outer": {
            "date_histogram": {"field": "timestamp", "fixed_interval": "1h"},
            "aggs": siblings
        }});
        let mut past_limit = at_limit.clone();
        past_limit["extra"] = json!({"date_histogram": {"field": "timestamp", "fixed_interval": "1h"}});
        let rows = || scan_rows(build_date_rows("timestamp", &[Some("2026-10-08T10:30:00Z")]));

        let answer = aggregate_plan(rows(), &at_limit, read_default_max_buckets())
            .await
            .expect("sixteen bucket aggregations answer");
        let error = aggregate_plan(rows(), &past_limit, read_default_max_buckets())
            .await
            .expect_err("seventeen bucket aggregations are refused");

        let ten_o_clock = json!({"buckets": [
            {"key_as_string": "2026-10-08T10:00:00.000Z", "key": 1_791_453_600_000_i64, "doc_count": 1}
        ]});
        let outer_bucket = &answer["outer"]["buckets"][0];
        assert_eq!(outer_bucket["doc_count"], 1);
        for position in 1..16 {
            assert_eq!(
                outer_bucket[format!("sibling_{position}")],
                ten_o_clock,
                "sibling_{position}"
            );
        }
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["aggregation"], "extra");
        assert_eq!(body["error"]["limit"], 16);
    }

    #[tokio::test]
    async fn values_past_the_microsecond_range_are_400s() {
        // 9.3e15 ms is past the largest microsecond instant (about 9.22e15 ms); 200000 days fit
        // microseconds but not the nanoseconds `date_bin` reads.
        let past_range = 9_300_000_000_000_000_i64;
        let bodies = [
            json!({"late": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "missing": past_range}}}),
            json!({"late": {"date_histogram": {
                "field": "timestamp", "fixed_interval": "1h", "min_doc_count": 0,
                "extended_bounds": {"min": past_range, "max": past_range}
            }}}),
            json!({"late": {"date_histogram": {"field": "timestamp", "fixed_interval": "200000d"}}}),
        ];
        for body in bodies {
            let error = aggregate_plan(
                scan_rows(build_date_rows("timestamp", &[Some("2026-10-08T10:30:00Z")])),
                &body,
                read_default_max_buckets(),
            )
            .await
            .expect_err("refused");

            let (status, envelope) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
            assert_eq!(envelope["error"]["type"], "illegal_argument_exception", "{body}");
        }
    }

    /// Render `error` and check it is the bucket-limit refusal for `max`.
    pub(super) async fn assert_too_many_buckets(error: OpenSearchError, max: usize) {
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "search_phase_execution_exception");
        let cause = &body["error"]["root_cause"][0];
        assert_eq!(cause["type"], "too_many_buckets_exception");
        assert_eq!(cause["max_buckets"], max);
    }

    #[tokio::test]
    async fn date_histogram_buckets_at_max_buckets_are_answered_and_one_more_is_a_400() {
        // Five hourly buckets with a row each and no gap filling: the empty-bucket enumeration
        // never runs, so only the data buckets reach the limit.
        let rows = || {
            build_date_rows(
                "timestamp",
                &[
                    Some("2026-10-08T10:30:00Z"),
                    Some("2026-10-08T11:30:00Z"),
                    Some("2026-10-08T12:30:00Z"),
                    Some("2026-10-08T13:30:00Z"),
                    Some("2026-10-08T14:30:00Z"),
                ],
            )
        };
        let hours =
            json!({"hours": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "min_doc_count": 1}}});

        let at_limit = aggregate_plan(scan_rows(rows()), &hours, 5).await.expect("five buckets fit");
        let past_limit = aggregate_plan(scan_rows(rows()), &hours, 4)
            .await
            .expect_err("five buckets exceed four");

        assert_eq!(at_limit["hours"]["buckets"].as_array().map(Vec::len), Some(5));
        assert_too_many_buckets(past_limit, 4).await;
    }

    #[tokio::test]
    async fn two_top_level_aggregations_share_one_bucket_budget() {
        let rows = || {
            build_date_rows(
                "timestamp",
                &[Some("2026-10-08T10:30:00Z"), Some("2026-10-08T11:30:00Z")],
            )
        };
        let aggregations = json!({
            "first": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "min_doc_count": 1}},
            "second": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h", "min_doc_count": 1}}
        });

        let at_limit = aggregate_plan(scan_rows(rows()), &aggregations, 4)
            .await
            .expect("two buckets each fit four");
        let past_limit = aggregate_plan(scan_rows(rows()), &aggregations, 3)
            .await
            .expect_err("two buckets each exceed three");

        assert_eq!(at_limit["first"]["buckets"].as_array().map(Vec::len), Some(2));
        assert_eq!(at_limit["second"]["buckets"].as_array().map(Vec::len), Some(2));
        // Each aggregation alone fits three; only together do they exceed it.
        assert_too_many_buckets(past_limit, 3).await;
    }
}

#[cfg(test)]
mod bucket_tree_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use axum::http::StatusCode;
    use datafusion::arrow::array::{
        Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
        TimestampMicrosecondArray,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Int64Type, Schema, TimeUnit};
    use datafusion::common::Column;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::functions::expr_fn::coalesce;
    use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, col, lit};
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    use super::super::distinct::{name_distinct_count_column, plan_exact_distinct};
    use super::tests::{
        aggregate_plan, assert_too_many_buckets, read_default_max_buckets, read_request_now, scan_rows,
    };
    use super::{
        AggregationLayout, Granularity, LEVEL_MASK_COLUMN, compute_level_mask, name_bucket_filter_column,
        name_key_column, plan_aggregations, resolve_plan_inputs,
    };
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::opensearch::search::aggs::parse::parse_aggregations;
    use crate::opensearch::search::execute::convert_plan_error;

    // Instants of 2026-10-08 UTC, in microseconds since the epoch.
    const AT_10_05_00_000999: i64 = 1_791_453_900_000_999;
    const AT_10_15_30_123456: i64 = 1_791_454_530_123_456;
    const AT_10_40: i64 = 1_791_456_000_000_000;
    const AT_11_10: i64 = 1_791_457_800_000_000;
    const AT_11_30: i64 = 1_791_459_000_000_000;
    const AT_11_45_10_456789: i64 = 1_791_459_910_456_789;
    /// Keys of the hours starting 10:00 and 11:00 UTC on 2026-10-08, in epoch milliseconds.
    const HOUR_10_KEY: i64 = 1_791_453_600_000;
    const HOUR_11_KEY: i64 = 1_791_457_200_000;

    /// One literal input row of the table the component tests aggregate: a row the stage
    /// aggregate consumes, not one it outputs.
    struct SourceRow {
        timestamp_micros: i64,
        service_name: Option<String>,
        severity_text: Option<String>,
        duration_micros: Option<i64>,
        score: Option<f64>,
    }

    /// A row of the stated values.
    fn build_test_row(
        timestamp_micros: i64,
        service_name: Option<&str>,
        severity_text: Option<&str>,
        duration_micros: Option<i64>,
        score: Option<f64>,
    ) -> SourceRow {
        SourceRow {
            timestamp_micros,
            service_name: service_name.map(str::to_string),
            severity_text: severity_text.map(str::to_string),
            duration_micros,
            score,
        }
    }

    /// Six rows over two hours: services api (3), web (2) and db (1); severities INFO (4) and
    /// ERROR (2); web's ERROR row has no duration, web's INFO row no score.
    fn build_hourly_rows() -> Vec<SourceRow> {
        vec![
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(100), Some(1.5)),
            build_test_row(AT_10_15_30_123456, Some("api"), Some("ERROR"), Some(300), Some(2.5)),
            build_test_row(AT_10_40, Some("web"), Some("INFO"), Some(200), None),
            build_test_row(AT_11_10, Some("api"), Some("INFO"), Some(500), Some(4.0)),
            build_test_row(AT_11_30, Some("web"), Some("ERROR"), None, Some(3.0)),
            build_test_row(AT_11_45_10_456789, Some("db"), Some("INFO"), Some(50), Some(0.5)),
        ]
    }

    /// A scan of `rows` shaped as `plan_aggregation_rows` shapes the matching rows: one column per
    /// field, named by the field, in the Arrow type its field type maps to. `absent_field` is the
    /// NULL `Float64` column that function projects for a field no searched index maps.
    fn scan_test_rows(rows: &[SourceRow]) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("duration_micros", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("absent_field", DataType::Float64, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|row| row.timestamp_micros).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.service_name.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.severity_text.as_deref()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.duration_micros).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(rows.iter().map(|row| row.score).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(vec![None::<f64>; rows.len()])),
        ];
        scan_rows(RecordBatch::try_new(schema, columns).expect("build the test batch"))
    }

    /// Answer `aggregations` over `rows` with the production entry point
    /// (`execute_aggregations`), executing its plans in a tenant session (`aggregate_plan`).
    async fn run_aggregations(rows: &[SourceRow], aggregations: Value) -> Result<Value, OpenSearchError> {
        aggregate_plan(scan_test_rows(rows), &aggregations, read_default_max_buckets()).await
    }

    /// The test rows with one Boolean column per `filter` / `filters` bucket query of the
    /// request, as `plan_aggregation_rows` projects them. `predicates[ordinal]` stands for the
    /// query at `ordinal` (request order, preorder) lowered over the rows; the real lowering is
    /// per index and is pinned by the integration test
    /// `filter_and_filters_buckets_lower_their_queries_per_index`.
    fn scan_with_bucket_filters(rows: &[SourceRow], predicates: Vec<Expr>) -> LogicalPlan {
        let scan = scan_test_rows(rows);
        let mut projection: Vec<Expr> = scan
            .schema()
            .fields()
            .iter()
            .map(|field| Expr::Column(Column::from_name(field.name())))
            .collect();
        projection.extend(predicates.into_iter().enumerate().map(|(ordinal, predicate)| {
            coalesce(vec![predicate, lit(false)]).alias(name_bucket_filter_column(ordinal))
        }));
        LogicalPlanBuilder::from(scan)
            .project(projection)
            .and_then(LogicalPlanBuilder::build)
            .expect("project the bucket filters")
    }

    /// Answer `aggregations` over `rows` with the bucket queries standing as `predicates`
    /// ([`scan_with_bucket_filters`]).
    async fn run_filtered_aggregations(
        rows: &[SourceRow],
        predicates: Vec<Expr>,
        aggregations: Value,
    ) -> Result<Value, OpenSearchError> {
        aggregate_plan(
            scan_with_bucket_filters(rows, predicates),
            &aggregations,
            read_default_max_buckets(),
        )
        .await
    }

    /// `[key, doc_count, <child>]` of every bucket of `aggregation`; the other key renderings are
    /// left to the tests of the kind that renders them.
    fn summarize_buckets(aggregation: &Value, child: &str) -> Vec<Value> {
        aggregation["buckets"]
            .as_array()
            .expect("a bucket array")
            .iter()
            .map(|bucket| json!([bucket["key"], bucket["doc_count"], bucket[child]]))
            .collect()
    }

    /// The keys of the buckets of `aggregation`, in response order.
    fn list_bucket_keys(aggregation: &Value) -> Vec<Value> {
        aggregation["buckets"]
            .as_array()
            .expect("a bucket array")
            .iter()
            .map(|bucket| bucket["key"].clone())
            .collect()
    }

    /// Render `error` and check its status and type; return the rendered `error` object.
    async fn render_refusal(error: OpenSearchError, expected_type: &str) -> Value {
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], expected_type);
        body["error"].clone()
    }

    #[test]
    fn level_masks_set_one_bit_per_grouped_level_a_set_leaves_out() {
        // Grouped levels 0, 1 and 2: level 0 is the highest bit, and a set bit marks a level the
        // set leaves out.
        let grouped_levels = [0, 1, 2];
        let paths: [&[usize]; 4] = [&[0], &[0, 1], &[2], &[]];
        let masks: Vec<i32> = paths
            .iter()
            .map(|path| compute_level_mask(&grouped_levels, path).expect("three grouped levels"))
            .collect();
        assert_eq!(masks, vec![0b011, 0b001, 0b110, 0b111]);
    }

    #[tokio::test]
    async fn grouping_marks_the_levels_a_set_omits_with_the_first_grouped_level_as_the_high_bit() {
        // Level 0 `services`, level 1 `severities` under it, level 2 `hours` beside it: the
        // aggregate passes the keys of levels 0, 1 and 2 to `grouping()`, in that order. The masks
        // below are stated, not computed, so a change of DataFusion's bit layout fails here.
        let tree = parse_aggregations(&json!({
            "services": {
                "terms": {"field": "service_name"},
                "aggs": {"severities": {"terms": {"field": "severity_text"}}}
            },
            "hours": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h"}}
        }))
        .expect("parse the aggregations");
        let layout = AggregationLayout::new(&tree, read_request_now()).expect("lay out the aggregations");
        let rows = scan_test_rows(&build_hourly_rows());
        let inputs = resolve_plan_inputs(&layout, rows.schema()).expect("resolve the inputs");
        let mut plans = plan_aggregations(rows, &layout, &inputs).expect("plan the aggregate");
        assert_eq!(plans.stages.len(), 1);
        let stage = plans.stages.remove(0);

        let batches = SessionContext::new()
            .execute_logical_plan(stage.plan)
            .await
            .expect("plan the execution")
            .collect()
            .await
            .expect("execute the aggregate");

        let set_bits = [
            (Granularity::Level(0), 0b011),
            (Granularity::Level(1), 0b001),
            (Granularity::Level(2), 0b110),
        ];
        assert_eq!(stage.members, set_bits);
        // Per pattern of present keys (levels 0, 1, 2), the mask of every row of that set: three
        // services, five service and severity pairs, two hours. Every key of the rows is present,
        // so a NULL key marks a level the row's set omits.
        let mut masks_by_keys: BTreeMap<[bool; 3], Vec<i32>> = BTreeMap::new();
        for batch in &batches {
            let masks = batch
                .column_by_name(LEVEL_MASK_COLUMN)
                .expect("the mask column")
                .as_primitive::<Int32Type>();
            let keys: Vec<&ArrayRef> = (0..3)
                .map(|level| batch.column_by_name(&name_key_column(level)).expect("a key column"))
                .collect();
            for row in 0..batch.num_rows() {
                let present = [keys[0].is_valid(row), keys[1].is_valid(row), keys[2].is_valid(row)];
                masks_by_keys.entry(present).or_default().push(masks.value(row));
            }
        }
        assert_eq!(
            masks_by_keys,
            BTreeMap::from([
                ([true, false, false], vec![0b011; 3]),
                ([true, true, false], vec![0b001; 5]),
                ([false, false, true], vec![0b110; 2]),
            ])
        );
    }

    /// A `terms` level ordered by count or key reaches assembly cut to its `size` under each parent
    /// bucket, every parent counted, so assembly reads rows bounded by the request and not by the
    /// keys' cardinality; the answer stays the ranking's, `sum_other_doc_count` included.
    #[tokio::test]
    async fn count_and_key_ordered_terms_levels_reach_assembly_cut_to_size_per_parent() {
        let aggregations = json!({
            "services": {
                "terms": {"field": "service_name", "size": 1},
                "aggs": {"severities": {"terms": {"field": "severity_text", "size": 1, "order": {"_key": "asc"}}}}
            }
        });
        let tree = parse_aggregations(&aggregations).expect("parse the aggregations");
        let layout = AggregationLayout::new(&tree, read_request_now()).expect("lay out the aggregations");
        let rows = scan_test_rows(&build_hourly_rows());
        let inputs = resolve_plan_inputs(&layout, rows.schema()).expect("resolve the inputs");
        let mut plans = plan_aggregations(rows, &layout, &inputs).expect("plan the aggregate");
        assert_eq!(plans.stages.len(), 1);
        let stage = plans.stages.remove(0);
        let batches = SessionContext::new()
            .execute_logical_plan(stage.plan)
            .await
            .expect("plan the execution")
            .collect()
            .await
            .expect("execute the aggregate");

        // The key paths of the rows of each level, by the level's mask.
        let mut paths_by_level: BTreeMap<usize, Vec<Vec<String>>> = BTreeMap::new();
        for batch in &batches {
            let masks = batch
                .column_by_name(LEVEL_MASK_COLUMN)
                .expect("the mask column")
                .as_primitive::<Int32Type>();
            let keys: Vec<&StringArray> = (0..2)
                .map(|level| {
                    batch
                        .column_by_name(&name_key_column(level))
                        .expect("a key column")
                        .as_string::<i32>()
                })
                .collect();
            for row in 0..batch.num_rows() {
                let (level, _) = stage
                    .members
                    .iter()
                    .enumerate()
                    .find(|(_, (_, mask))| *mask == masks.value(row))
                    .expect("a member's mask");
                let path = keys[..=level].iter().map(|key| key.value(row).to_string()).collect();
                paths_by_level.entry(level).or_default().push(path);
            }
        }
        for paths in paths_by_level.values_mut() {
            paths.sort();
        }
        // Services: api 3, web 2, db 1. Severities by key: api ERROR and INFO, web ERROR and INFO,
        // db INFO; the first of each parent is kept, every parent counted.
        assert_eq!(
            paths_by_level,
            BTreeMap::from([
                (0, vec![vec!["api".to_string()]]),
                (
                    1,
                    vec![
                        vec!["api".to_string(), "ERROR".to_string()],
                        vec!["db".to_string(), "INFO".to_string()],
                        vec!["web".to_string(), "ERROR".to_string()],
                    ]
                ),
            ])
        );

        let answer = run_aggregations(&build_hourly_rows(), aggregations)
            .await
            .expect("aggregate the rows");
        assert_eq!(
            answer,
            json!({"services": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 3,
                "buckets": [{
                    "key": "api",
                    "doc_count": 3,
                    "severities": {
                        "doc_count_error_upper_bound": 0,
                        "sum_other_doc_count": 2,
                        "buckets": [{"key": "ERROR", "doc_count": 1}]
                    }
                }]
            }})
        );
    }

    #[tokio::test]
    async fn a_two_level_terms_and_date_histogram_with_metrics_reads_each_metric_at_its_level() {
        let aggregations = run_aggregations(
            &build_hourly_rows(),
            json!({
                "services": {
                    "terms": {"field": "service_name", "size": 10, "order": {"_key": "asc"}},
                    "aggs": {
                        "avg_duration": {"avg": {"field": "duration_micros"}},
                        "per_hour": {
                            "date_histogram": {"field": "timestamp", "fixed_interval": "1h"},
                            "aggs": {"max_duration": {"max": {"field": "duration_micros"}}}
                        }
                    }
                },
                "total_duration": {"sum": {"field": "duration_micros"}}
            }),
        )
        .await
        .expect("aggregate the rows");

        // 100 + 300 + 200 + 500 + 50; web's ERROR row has no duration.
        assert_eq!(aggregations["total_duration"], json!({"value": 1150.0}));
        let services = &aggregations["services"];
        assert_eq!(services["sum_other_doc_count"], json!(0));
        assert_eq!(
            summarize_buckets(services, "avg_duration"),
            vec![
                json!(["api", 3, {"value": 300.0}]),
                json!(["db", 1, {"value": 50.0}]),
                json!(["web", 2, {"value": 200.0}]),
            ]
        );
        let per_hour = |index: usize| summarize_buckets(&services["buckets"][index]["per_hour"], "max_duration");
        assert_eq!(
            per_hour(0),
            vec![
                json!([HOUR_10_KEY, 2, {"value": 300.0}]),
                json!([HOUR_11_KEY, 1, {"value": 500.0}])
            ]
        );
        assert_eq!(per_hour(1), vec![json!([HOUR_11_KEY, 1, {"value": 50.0}])]);
        assert_eq!(
            per_hour(2),
            vec![
                json!([HOUR_10_KEY, 1, {"value": 200.0}]),
                json!([HOUR_11_KEY, 1, {"value": null}])
            ]
        );
    }

    #[tokio::test]
    async fn parent_level_metrics_are_computed_at_the_parents_granularity() {
        // A pie-style request: the same metric name at both levels.
        let aggregations = run_aggregations(
            &build_hourly_rows(),
            json!({
                "severities": {
                    "terms": {"field": "severity_text", "order": {"_key": "asc"}},
                    "aggs": {
                        "1": {"avg": {"field": "duration_micros"}},
                        "services": {
                            "terms": {"field": "service_name", "order": {"_key": "asc"}},
                            "aggs": {"1": {"avg": {"field": "duration_micros"}}}
                        }
                    }
                }
            }),
        )
        .await
        .expect("aggregate the rows");

        let severities = &aggregations["severities"];
        // INFO's 212.5 is the mean of its four rows (100, 200, 500, 50), not the mean of its
        // services' means (300, 50, 200).
        assert_eq!(
            summarize_buckets(severities, "1"),
            vec![
                json!(["ERROR", 2, {"value": 300.0}]),
                json!(["INFO", 4, {"value": 212.5}])
            ]
        );
        assert_eq!(
            summarize_buckets(&severities["buckets"][0]["services"], "1"),
            vec![json!(["api", 1, {"value": 300.0}]), json!(["web", 1, {"value": null}])]
        );
        assert_eq!(
            summarize_buckets(&severities["buckets"][1]["services"], "1"),
            vec![
                json!(["api", 2, {"value": 300.0}]),
                json!(["db", 1, {"value": 50.0}]),
                json!(["web", 1, {"value": 200.0}]),
            ]
        );
    }

    /// Five rows whose maximum scores tie at 2.0 for api, db and web; cache tops at 1.5.
    fn build_tied_score_rows() -> Vec<SourceRow> {
        vec![
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(10), Some(2.0)),
            build_test_row(AT_10_15_30_123456, Some("api"), Some("INFO"), Some(20), Some(1.0)),
            build_test_row(AT_10_40, Some("web"), Some("INFO"), Some(30), Some(2.0)),
            build_test_row(AT_11_10, Some("db"), Some("INFO"), Some(40), Some(2.0)),
            build_test_row(AT_11_30, Some("cache"), Some("INFO"), Some(50), Some(1.5)),
        ]
    }

    #[tokio::test]
    async fn terms_ordered_by_a_child_metric_rank_by_that_metric_then_key() {
        let services_by_max = run_aggregations(
            &build_tied_score_rows(),
            json!({"services": {
                "terms": {"field": "service_name", "size": 3, "order": {"max_score": "desc"}},
                "aggs": {"max_score": {"max": {"field": "score"}}}
            }}),
        )
        .await
        .expect("aggregate the rows");
        // The three-way tie at 2.0 breaks by key ascending; cache (1.5) is cut by size.
        assert_eq!(
            summarize_buckets(&services_by_max["services"], "max_score"),
            vec![
                json!(["api", 2, {"value": 2.0}]),
                json!(["db", 1, {"value": 2.0}]),
                json!(["web", 1, {"value": 2.0}]),
            ]
        );
        assert_eq!(services_by_max["services"]["sum_other_doc_count"], json!(1));

        let services_by_min = run_aggregations(
            &build_tied_score_rows(),
            json!({"services": {
                "terms": {"field": "service_name", "size": 3, "order": {"score_stats.min": "asc"}},
                "aggs": {"score_stats": {"stats": {"field": "score"}}}
            }}),
        )
        .await
        .expect("aggregate the rows");
        // Minimum scores: api 1.0, cache 1.5, db 2.0, web 2.0; web is cut by size.
        assert_eq!(
            list_bucket_keys(&services_by_min["services"]),
            vec![json!("api"), json!("cache"), json!("db")]
        );
        assert_eq!(services_by_min["services"]["sum_other_doc_count"], json!(1));
    }

    #[tokio::test]
    async fn a_bucket_without_an_order_value_ranks_last_in_both_directions() {
        // web has no score at all, so its maximum score is missing.
        let rows = vec![
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(10), Some(1.0)),
            build_test_row(AT_10_40, Some("web"), Some("INFO"), Some(30), None),
            build_test_row(AT_11_10, Some("db"), Some("INFO"), Some(40), Some(2.0)),
        ];
        let ordered_by = |direction: &str| {
            json!({"services": {
                "terms": {"field": "service_name", "order": {"max_score": direction}},
                "aggs": {"max_score": {"max": {"field": "score"}}}
            }})
        };

        let ascending = run_aggregations(&rows, ordered_by("asc")).await.expect("aggregate the rows");
        let descending = run_aggregations(&rows, ordered_by("desc")).await.expect("aggregate the rows");

        assert_eq!(
            list_bucket_keys(&ascending["services"]),
            vec![json!("api"), json!("db"), json!("web")]
        );
        assert_eq!(
            list_bucket_keys(&descending["services"]),
            vec![json!("db"), json!("api"), json!("web")]
        );
    }

    #[tokio::test]
    async fn an_order_path_naming_no_metric_or_no_statistic_of_it_is_an_aggregation_execution_400() {
        for order in [
            // No sub-aggregation of that name.
            json!({"absent": "desc"}),
            // A bucket sub-aggregation, not a metric.
            json!({"per_hour": "desc"}),
            // `stats` without a statistic, and with one it does not compute.
            json!({"score_stats": "desc"}),
            json!({"score_stats.median": "desc"}),
            // A single-value metric read through a statistic other than `value`.
            json!({"max_score.min": "desc"}),
            json!({"distinct_scores.max": "desc"}),
        ] {
            let error = run_aggregations(
                &build_tied_score_rows(),
                json!({"services": {
                    "terms": {"field": "service_name", "order": order.clone()},
                    "aggs": {
                        "max_score": {"max": {"field": "score"}},
                        "distinct_scores": {"cardinality": {"field": "score"}},
                        "score_stats": {"stats": {"field": "score"}},
                        "per_hour": {"date_histogram": {"field": "timestamp", "fixed_interval": "1h"}}
                    }
                }}),
            )
            .await
            .expect_err("the order path must be refused");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{order}");
            assert_eq!(body["error"]["type"], "aggregation_execution_exception", "{order}");
        }
        // `<id>.value` reads a single-value metric's own value.
        let by_value = run_aggregations(
            &build_tied_score_rows(),
            json!({"services": {
                "terms": {"field": "service_name", "size": 1, "order": {"max_score.value": "asc"}},
                "aggs": {"max_score": {"max": {"field": "score"}}}
            }}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(list_bucket_keys(&by_value["services"]), vec![json!("cache")]);
        // And a cardinality's: api has two distinct scores, the others one, so the descending
        // order puts api first.
        let by_distinct_value = run_aggregations(
            &build_tied_score_rows(),
            json!({"services": {
                "terms": {"field": "service_name", "size": 1, "order": {"distinct_scores.value": "desc"}},
                "aggs": {"distinct_scores": {"cardinality": {"field": "score"}}}
            }}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(
            summarize_buckets(&by_distinct_value["services"], "distinct_scores"),
            vec![json!(["api", 2, {"value": 2}])]
        );
    }

    #[tokio::test]
    async fn a_metric_with_sub_aggregations_is_a_parsing_exception() {
        let error = run_aggregations(
            &build_hourly_rows(),
            json!({"slowest": {
                "max": {"field": "duration_micros"},
                "aggs": {"fastest": {"min": {"field": "duration_micros"}}}
            }}),
        )
        .await
        .expect_err("a metric holds no sub-aggregation");

        render_refusal(error, "parsing_exception").await;
    }

    #[tokio::test]
    async fn histogram_buckets_fill_with_min_doc_count_zero_up_to_extended_bounds() {
        // Durations 100, 300, 200, 500 and 50 fall in the buckets at 100, 300, 200, 500 and 0;
        // web's ERROR row has no duration and joins no bucket.
        let filled_histogram = run_aggregations(
            &build_hourly_rows(),
            json!({"durations": {"histogram": {
                "field": "duration_micros", "interval": 100, "min_doc_count": 0,
                "extended_bounds": {"min": 0, "max": 600}
            }}}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(
            filled_histogram["durations"],
            json!({"buckets": [
                {"key": 0.0, "doc_count": 1},
                {"key": 100.0, "doc_count": 1},
                {"key": 200.0, "doc_count": 1},
                {"key": 300.0, "doc_count": 1},
                {"key": 400.0, "doc_count": 0},
                {"key": 500.0, "doc_count": 1},
                {"key": 600.0, "doc_count": 0}
            ]})
        );

        let observed_histogram = run_aggregations(
            &build_hourly_rows(),
            json!({"durations": {"histogram": {"field": "duration_micros", "interval": 100, "min_doc_count": 1}}}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(
            observed_histogram["durations"],
            json!({"buckets": [
                {"key": 0.0, "doc_count": 1},
                {"key": 100.0, "doc_count": 1},
                {"key": 200.0, "doc_count": 1},
                {"key": 300.0, "doc_count": 1},
                {"key": 500.0, "doc_count": 1}
            ]})
        );
    }

    #[tokio::test]
    async fn negative_histogram_values_floor_toward_negative_infinity() {
        // -150 / 100 floors to -2, -100 and -1 to -1, 0 and 99 to 0; with an interval of 2.5, 5
        // falls in the bucket at 5.0 and -0.5 in the one at -2.5.
        let rows: Vec<SourceRow> = [-150, -100, -1, 0, 99]
            .into_iter()
            .map(|duration| build_test_row(AT_10_40, Some("api"), Some("INFO"), Some(duration), None))
            .collect();
        let fractional_rows = vec![
            build_test_row(AT_10_40, Some("api"), Some("INFO"), None, Some(5.0)),
            build_test_row(AT_10_40, Some("api"), Some("INFO"), None, Some(-0.5)),
        ];

        let durations = run_aggregations(
            &rows,
            json!({"durations": {"histogram": {"field": "duration_micros", "interval": 100}}}),
        )
        .await
        .expect("aggregate the rows");
        let scores = run_aggregations(
            &fractional_rows,
            json!({"scores": {"histogram": {"field": "score", "interval": 2.5, "min_doc_count": 1}}}),
        )
        .await
        .expect("aggregate the rows");

        assert_eq!(
            durations["durations"],
            json!({"buckets": [
                {"key": -200.0, "doc_count": 1},
                {"key": -100.0, "doc_count": 2},
                {"key": 0.0, "doc_count": 2}
            ]})
        );
        assert_eq!(
            scores["scores"],
            json!({"buckets": [
                {"key": -2.5, "doc_count": 1},
                {"key": 5.0, "doc_count": 1}
            ]})
        );
    }

    #[tokio::test]
    async fn histogram_buckets_at_max_buckets_are_answered_and_one_more_is_a_400() {
        // The filled histogram answers seven buckets (0 to 600), five of them holding rows; with
        // `min_doc_count: 1` only those five are answered. The filled span is counted before any
        // empty bucket is built.
        let filled = json!({"durations": {"histogram": {
            "field": "duration_micros", "interval": 100, "min_doc_count": 0,
            "extended_bounds": {"min": 0, "max": 600}
        }}});
        let observed =
            json!({"durations": {"histogram": {"field": "duration_micros", "interval": 100, "min_doc_count": 1}}});
        let rows = || scan_test_rows(&build_hourly_rows());

        let filled_at_limit = aggregate_plan(rows(), &filled, 7).await.expect("seven buckets fit");
        let filled_past_limit = aggregate_plan(rows(), &filled, 6).await.expect_err("seven buckets exceed six");
        let observed_at_limit = aggregate_plan(rows(), &observed, 5).await.expect("five buckets fit");
        let observed_past_limit = aggregate_plan(rows(), &observed, 4)
            .await
            .expect_err("five buckets exceed four");

        assert_eq!(
            filled_at_limit["durations"]["buckets"].as_array().map(Vec::len),
            Some(7)
        );
        assert_too_many_buckets(filled_past_limit, 6).await;
        assert_eq!(
            observed_at_limit["durations"]["buckets"].as_array().map(Vec::len),
            Some(5)
        );
        assert_too_many_buckets(observed_past_limit, 4).await;
    }

    #[tokio::test]
    async fn histogram_bucket_indices_past_the_i64_range_are_400s_naming_the_aggregation() {
        // With an interval of 1e-300, the bucket index of 1 is 1e300, past the i64 range: as an
        // `extended_bounds` bound, and as the value of a row. A NaN value has no index either.
        let zero_score = [build_test_row(AT_10_40, Some("api"), Some("INFO"), None, Some(0.0))];
        let unit_score = [build_test_row(AT_10_40, Some("api"), Some("INFO"), None, Some(1.0))];
        let nan_score = [build_test_row(
            AT_10_40,
            Some("api"),
            Some("INFO"),
            None,
            Some(f64::NAN),
        )];
        let bounded = json!({"scores": {"histogram": {
            "field": "score", "interval": 1e-300, "min_doc_count": 0,
            "extended_bounds": {"min": 0, "max": 1}
        }}});
        let observed = json!({"scores": {"histogram": {"field": "score", "interval": 1e-300, "min_doc_count": 1}}});
        let unit_interval = json!({"scores": {"histogram": {"field": "score", "interval": 1, "min_doc_count": 1}}});

        let errors = [
            run_aggregations(&zero_score, bounded).await,
            run_aggregations(&unit_score, observed).await,
            run_aggregations(&nan_score, unit_interval).await,
        ];

        for error in errors {
            let refusal = render_refusal(error.expect_err("refused"), "illegal_argument_exception").await;
            assert_eq!(refusal["aggregation"], "scores");
        }
    }

    #[tokio::test]
    async fn histogram_bounds_at_the_i64_index_limits_are_400s_and_one_step_inside_pass_the_check() {
        // With an interval of 1, a bound is its own index. -2^63 and 2^63 are refused; the doubles
        // one step inside them are indices, whose span then passes the bucket budget.
        let rows = [build_test_row(AT_10_40, Some("api"), Some("INFO"), None, Some(0.0))];
        let bounded = |min: f64, max: f64| {
            json!({"scores": {"histogram": {
                "field": "score", "interval": 1, "min_doc_count": 0,
                "extended_bounds": {"min": min, "max": max}
            }}})
        };
        let cases = [
            (-9_223_372_036_854_775_808.0, 0.0, "illegal_argument_exception"),
            (-9_223_372_036_854_774_784.0, 0.0, "search_phase_execution_exception"),
            (0.0, 9_223_372_036_854_775_808.0, "illegal_argument_exception"),
            (0.0, 9_223_372_036_854_774_784.0, "search_phase_execution_exception"),
        ];

        for (min, max, expected_type) in cases {
            let error = run_aggregations(&rows, bounded(min, max)).await.expect_err("refused");

            render_refusal(error, expected_type).await;
        }
    }

    #[tokio::test]
    async fn a_sum_or_mean_that_overflows_is_a_500() {
        // Two scores of f64::MAX sum past the largest double: `null` would claim no value.
        let rows = [
            build_test_row(AT_10_40, Some("api"), Some("INFO"), None, Some(f64::MAX)),
            build_test_row(AT_11_10, Some("api"), Some("INFO"), None, Some(f64::MAX)),
        ];
        for aggregations in [
            json!({"total": {"sum": {"field": "score"}}}),
            json!({"total": {"avg": {"field": "score"}}}),
            json!({"total": {"stats": {"field": "score"}}}),
        ] {
            let error = run_aggregations(&rows, aggregations.clone())
                .await
                .expect_err("an overflowing statistic is refused");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{aggregations}");
            assert_eq!(body["error"]["type"], "exception", "{aggregations}");
        }
    }

    #[tokio::test]
    async fn empty_metrics_render_null_and_an_empty_sum_renders_zero() {
        let aggregations = run_aggregations(
            &build_hourly_rows(),
            json!({
                "durations": {
                    "histogram": {
                        "field": "duration_micros", "interval": 100, "min_doc_count": 0,
                        "extended_bounds": {"min": 0, "max": 600}
                    },
                    "aggs": {
                        "avg": {"avg": {"field": "duration_micros"}},
                        "sum": {"sum": {"field": "duration_micros"}},
                        "min": {"min": {"field": "duration_micros"}},
                        "max": {"max": {"field": "duration_micros"}},
                        "count": {"value_count": {"field": "duration_micros"}},
                        "stats": {"stats": {"field": "duration_micros"}}
                    }
                },
                "avg_absent": {"avg": {"field": "absent_field"}},
                "sum_absent": {"sum": {"field": "absent_field"}}
            }),
        )
        .await
        .expect("aggregate the rows");

        let buckets = &aggregations["durations"]["buckets"];
        assert_eq!(
            buckets[1],
            json!({
                "key": 100.0, "doc_count": 1,
                "avg": {"value": 100.0}, "sum": {"value": 100.0},
                "min": {"value": 100.0}, "max": {"value": 100.0},
                "count": {"value": 1},
                "stats": {"count": 1, "min": 100.0, "max": 100.0, "avg": 100.0, "sum": 100.0}
            })
        );
        // The bucket at 400 is gap-filled: it has no aggregated row, so every metric is empty.
        assert_eq!(
            buckets[4],
            json!({
                "key": 400.0, "doc_count": 0,
                "avg": {"value": null}, "sum": {"value": 0.0},
                "min": {"value": null}, "max": {"value": null},
                "count": {"value": 0},
                "stats": {"count": 0, "min": null, "max": null, "avg": null, "sum": 0.0}
            })
        );
        assert_eq!(aggregations["avg_absent"], json!({"value": null}));
        assert_eq!(aggregations["sum_absent"], json!({"value": 0.0}));
    }

    #[tokio::test]
    async fn min_and_max_over_a_date_render_epoch_millis_and_a_value_as_string() {
        let aggregations = run_aggregations(
            &build_hourly_rows(),
            json!({"first": {"min": {"field": "timestamp"}}, "last": {"max": {"field": "timestamp"}}}),
        )
        .await
        .expect("aggregate the rows");
        // 10:05:00.000999 and 11:45:10.456789 floor to their milliseconds.
        assert_eq!(
            aggregations,
            json!({
                "first": {"value": 1_791_453_900_000.0, "value_as_string": "2026-10-08T10:05:00.000Z"},
                "last": {"value": 1_791_459_910_456.0, "value_as_string": "2026-10-08T11:45:10.456Z"}
            })
        );
    }

    #[tokio::test]
    async fn booleans_and_dates_feed_metrics_and_missing_stands_in_for_an_absent_value() {
        // `flag` reads as 1 or 0. `seen` holds instants before and after the epoch: -1001 µs
        // floors to -2 ms, -1 µs to -1 ms, 3000 µs is 3 ms. `amount` lacks a value in two rows.
        let rows = RecordBatch::try_from_iter(vec![
            (
                "flag",
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), Some(true), None])) as ArrayRef,
            ),
            (
                "seen",
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-1_001),
                    Some(-1),
                    Some(3_000),
                    None,
                ])) as ArrayRef,
            ),
            (
                "amount",
                Arc::new(Float64Array::from(vec![Some(2.0), None, Some(4.0), None])) as ArrayRef,
            ),
        ])
        .expect("build the test batch");

        let aggregations = aggregate_plan(
            scan_rows(rows),
            &json!({
                "flag_sum": {"sum": {"field": "flag"}},
                "flag_avg_with_missing": {"avg": {"field": "flag", "missing": 0}},
                "flag_count": {"value_count": {"field": "flag"}},
                "seen_stats": {"stats": {"field": "seen"}},
                "seen_min_with_missing": {"min": {"field": "seen", "missing": "-5"}},
                "amount_avg": {"avg": {"field": "amount"}},
                "amount_avg_with_missing": {"avg": {"field": "amount", "missing": 0}},
                "amount_max_with_missing": {"max": {"field": "amount", "missing": 7.5}},
                "amount_count": {"value_count": {"field": "amount"}}
            }),
            read_default_max_buckets(),
        )
        .await
        .expect("aggregate the rows");

        assert_eq!(
            aggregations,
            json!({
                "flag_sum": {"value": 2.0},
                // (1 + 0 + 1 + 0) / 4: the row without a flag counts as the missing 0.
                "flag_avg_with_missing": {"value": 0.5},
                "flag_count": {"value": 3},
                "seen_stats": {
                    "count": 3,
                    "min": -2.0, "min_as_string": "1969-12-31T23:59:59.998Z",
                    "max": 3.0, "max_as_string": "1970-01-01T00:00:00.003Z",
                    "avg": 0.0, "sum": 0.0
                },
                "seen_min_with_missing": {"value": -5.0, "value_as_string": "1969-12-31T23:59:59.995Z"},
                "amount_avg": {"value": 3.0},
                "amount_avg_with_missing": {"value": 1.5},
                "amount_max_with_missing": {"value": 7.5},
                "amount_count": {"value": 2}
            })
        );
    }

    #[tokio::test]
    async fn overlapping_filters_count_a_row_in_every_bucket_it_matches() {
        let aggregations = run_filtered_aggregations(
            &build_hourly_rows(),
            // `*` is `match_all`; `level:error` is `term severity_text: ERROR`.
            vec![lit(true), col("severity_text").eq(lit("ERROR"))],
            json!({
                "by_filter": {
                    "filters": {"filters": {
                        "*": {"match_all": {}},
                        "level:error": {"term": {"severity_text": "ERROR"}}
                    }},
                    "aggs": {"max_duration": {"max": {"field": "duration_micros"}}}
                },
                "rows_with_a_service": {"value_count": {"field": "service_name"}}
            }),
        )
        .await
        .expect("aggregate the rows");

        // Both ERROR rows count in "*" and in "level:error".
        assert_eq!(
            aggregations["by_filter"],
            json!({"buckets": {
                "*": {"doc_count": 6, "max_duration": {"value": 500.0}},
                "level:error": {"doc_count": 2, "max_duration": {"value": 300.0}}
            }})
        );
        // The sibling metric reads the rows of its own aggregate, before the unnest: each once.
        assert_eq!(aggregations["rows_with_a_service"], json!({"value": 6}));
    }

    #[tokio::test]
    async fn overlapping_ranges_count_a_row_in_every_range_with_a_sub_metric() {
        let aggregations = run_aggregations(
            &build_hourly_rows(),
            json!({
                "durations": {
                    "range": {"field": "duration_micros", "ranges": [
                        {"to": 250}, {"from": 100, "to": 400}, {"from": 300}
                    ]},
                    "aggs": {"max_duration": {"max": {"field": "duration_micros"}}}
                },
                "durations_seen": {"value_count": {"field": "duration_micros"}}
            }),
        )
        .await
        .expect("aggregate the rows");

        // Durations 100, 300, 200, 500 and 50: 50 is in the first range, 100 and 200 in the
        // first two, 300 in the last two, 500 in the last.
        assert_eq!(
            aggregations["durations"],
            json!({"buckets": [
                {"key": "*-250.0", "to": 250.0, "doc_count": 3, "max_duration": {"value": 200.0}},
                {"key": "100.0-400.0", "from": 100.0, "to": 400.0, "doc_count": 3, "max_duration": {"value": 300.0}},
                {"key": "300.0-*", "from": 300.0, "doc_count": 2, "max_duration": {"value": 500.0}}
            ]})
        );
        assert_eq!(aggregations["durations_seen"], json!({"value": 5}));
    }

    #[tokio::test]
    async fn keyed_ranges_echo_from_and_to_and_omit_open_ends() {
        // Disjoint ranges: the scalar key, no unnest.
        let aggregations = run_aggregations(
            &build_hourly_rows(),
            json!({"durations": {"range": {"field": "duration_micros", "keyed": true, "ranges": [
                {"to": 100}, {"from": 100, "to": 300}, {"from": 300, "to": 1000}, {"from": 1000, "key": "slow"}
            ]}}}),
        )
        .await
        .expect("aggregate the rows");

        assert_eq!(
            aggregations["durations"],
            json!({"buckets": {
                "*-100.0": {"to": 100.0, "doc_count": 1},
                "100.0-300.0": {"from": 100.0, "to": 300.0, "doc_count": 2},
                "300.0-1000.0": {"from": 300.0, "to": 1000.0, "doc_count": 2},
                "slow": {"from": 1000.0, "doc_count": 0}
            }})
        );
        let keys: Vec<&String> = aggregations["durations"]["buckets"]
            .as_object()
            .expect("keyed buckets")
            .keys()
            .collect();
        assert_eq!(keys, vec!["*-100.0", "100.0-300.0", "300.0-1000.0", "slow"]);
    }

    #[tokio::test]
    async fn date_range_keys_resolve_now_relative_bounds_in_asia_dubai() {
        // now is 2026-10-08T12:34:56.789Z, 16:34:56.789 in Dubai (UTC+4). now-3d is
        // 2026-10-05T16:34:56.789+04:00, rounded down to the Dubai day:
        // 2026-10-05T00:00:00+04:00, that is 2026-10-04T20:00:00Z.
        assert_eq!(read_request_now().timestamp_micros(), 1_791_462_896_789_000);
        let rows = vec![
            // 2026-10-04T19:59:59.999Z, one millisecond before the lower bound.
            build_test_row(1_791_143_999_999_000, Some("api"), Some("INFO"), None, None),
            // 2026-10-04T20:00:00.000Z, the lower bound.
            build_test_row(1_791_144_000_000_000, Some("api"), Some("INFO"), None, None),
            // 2026-10-08T12:34:56.788Z, one millisecond before now.
            build_test_row(1_791_462_896_788_000, Some("api"), Some("INFO"), None, None),
            // 2026-10-08T12:34:56.789Z, now: the exclusive upper bound.
            build_test_row(1_791_462_896_789_000, Some("api"), Some("INFO"), None, None),
        ];
        let aggregations = run_aggregations(
            &rows,
            json!({"recent": {"date_range": {
                "field": "timestamp",
                "time_zone": "Asia/Dubai",
                "ranges": [{"from": "now-3d/d", "to": "now"}]
            }}}),
        )
        .await
        .expect("aggregate the rows");

        assert_eq!(
            aggregations["recent"],
            json!({"buckets": [{
                "key": "2026-10-04T20:00:00.000Z-2026-10-08T12:34:56.789Z",
                "from": 1_791_144_000_000.0,
                "from_as_string": "2026-10-04T20:00:00.000Z",
                "to": 1_791_462_896_789.0,
                "to_as_string": "2026-10-08T12:34:56.789Z",
                "doc_count": 2
            }]})
        );
    }

    #[tokio::test]
    async fn named_filters_render_a_keyed_object_and_anonymous_filters_an_array() {
        let build_severity_predicate = |severity: &str| col("severity_text").eq(lit(severity.to_string()));
        let named_filters = run_filtered_aggregations(
            &build_hourly_rows(),
            vec![
                build_severity_predicate("ERROR"),
                build_severity_predicate("INFO"),
                build_severity_predicate("FATAL"),
            ],
            json!({"severities": {"filters": {"filters": {
                "errors": {"term": {"severity_text": "ERROR"}},
                "infos": {"term": {"severity_text": "INFO"}},
                "fatal": {"term": {"severity_text": "FATAL"}}
            }}}}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(
            named_filters["severities"],
            json!({"buckets": {
                "errors": {"doc_count": 2},
                "infos": {"doc_count": 4},
                "fatal": {"doc_count": 0}
            }})
        );
        let names: Vec<&String> = named_filters["severities"]["buckets"]
            .as_object()
            .expect("keyed buckets")
            .keys()
            .collect();
        assert_eq!(names, vec!["errors", "infos", "fatal"]);

        let anonymous_filters = run_filtered_aggregations(
            &build_hourly_rows(),
            vec![build_severity_predicate("ERROR"), build_severity_predicate("INFO")],
            json!({"severities": {"filters": {"filters": [
                {"term": {"severity_text": "ERROR"}},
                {"term": {"severity_text": "INFO"}}
            ]}}}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(
            anonymous_filters["severities"],
            json!({"buckets": [{"doc_count": 2}, {"doc_count": 4}]})
        );
    }

    #[tokio::test]
    async fn an_other_bucket_style_filters_request_counts_rows_outside_the_listed_terms() {
        let mut rows = build_hourly_rows();
        // 2026-10-08T11:50:00Z, a row without a service: outside every bucket.
        rows.push(build_test_row(
            1_791_460_200_000_000,
            None,
            Some("INFO"),
            Some(10),
            None,
        ));
        // The bucket query: an existing service other than api and web.
        let other_service = col("service_name")
            .is_not_null()
            .and(!coalesce(vec![col("service_name").eq(lit("api")), lit(false)]))
            .and(!coalesce(vec![col("service_name").eq(lit("web")), lit(false)]));
        let aggregations = run_filtered_aggregations(
            &rows,
            vec![other_service],
            json!({"other-filter": {
                "filters": {"filters": {"-api-web": {"bool": {
                    "must": [],
                    "filter": [{"exists": {"field": "service_name"}}],
                    "should": [],
                    "must_not": [
                        {"term": {"service_name": "api"}},
                        {"term": {"service_name": "web"}}
                    ]
                }}}},
                "aggs": {"1": {"max": {"field": "duration_micros"}}}
            }}),
        )
        .await
        .expect("aggregate the rows");

        // Only db's row has a service other than api and web.
        assert_eq!(
            aggregations["other-filter"],
            json!({"buckets": {"-api-web": {"doc_count": 1, "1": {"value": 50.0}}}})
        );
    }

    #[tokio::test]
    async fn a_filter_bucket_restricts_its_sub_aggregations() {
        let aggregations = run_filtered_aggregations(
            &build_hourly_rows(),
            vec![
                col("severity_text").eq(lit("ERROR")),
                col("severity_text").eq(lit("FATAL")),
            ],
            json!({
                "errors": {
                    "filter": {"term": {"severity_text": "ERROR"}},
                    "aggs": {"services": {"terms": {"field": "service_name", "order": {"_key": "asc"}}}}
                },
                "fatal": {
                    "filter": {"term": {"severity_text": "FATAL"}},
                    "aggs": {"services": {"terms": {"field": "service_name"}}}
                }
            }),
        )
        .await
        .expect("aggregate the rows");

        assert_eq!(aggregations["errors"]["doc_count"], json!(2));
        assert_eq!(
            summarize_buckets(&aggregations["errors"]["services"], "no_sub_aggregation"),
            vec![json!(["api", 1, null]), json!(["web", 1, null])]
        );
        // A filter no row matches still answers, with empty sub-aggregations.
        assert_eq!(
            aggregations["fatal"],
            json!({"doc_count": 0, "services": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
                "buckets": []
            }})
        );
    }

    #[tokio::test]
    async fn filters_under_a_terms_level_and_under_overlapping_ranges_count_rows_per_parent_bucket() {
        // Bucket queries in preorder: 0 and 1 under `services`, 2 and 3 under `durations`.
        let build_error_predicate = || col("severity_text").eq(lit("ERROR"));
        let aggregations = run_filtered_aggregations(
            &build_hourly_rows(),
            vec![build_error_predicate(), lit(true), build_error_predicate(), lit(true)],
            json!({
                "services": {
                    "terms": {"field": "service_name", "order": {"_key": "asc"}},
                    "aggs": {"levels": {
                        "filters": {"filters": {
                            "errors": {"term": {"severity_text": "ERROR"}},
                            "all": {"match_all": {}}
                        }},
                        "aggs": {"max_duration": {"max": {"field": "duration_micros"}}}
                    }}
                },
                "durations": {
                    "range": {"field": "duration_micros", "ranges": [{"to": 250}, {"from": 100}]},
                    "aggs": {"levels": {"filters": {"filters": {
                        "errors": {"term": {"severity_text": "ERROR"}},
                        "all": {"match_all": {}}
                    }}}}
                }
            }),
        )
        .await
        .expect("aggregate the rows");

        // api holds INFO 100, ERROR 300 and INFO 500; db INFO 50; web INFO 200 and an ERROR
        // without a duration.
        let build_levels = |errors: Value, all: Value| json!({"buckets": {"errors": errors, "all": all}});
        assert_eq!(
            aggregations["services"],
            json!({"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": [
                {"key": "api", "doc_count": 3, "levels": build_levels(
                    json!({"doc_count": 1, "max_duration": {"value": 300.0}}),
                    json!({"doc_count": 3, "max_duration": {"value": 500.0}})
                )},
                {"key": "db", "doc_count": 1, "levels": build_levels(
                    json!({"doc_count": 0, "max_duration": {"value": null}}),
                    json!({"doc_count": 1, "max_duration": {"value": 50.0}})
                )},
                {"key": "web", "doc_count": 2, "levels": build_levels(
                    json!({"doc_count": 1, "max_duration": {"value": null}}),
                    json!({"doc_count": 2, "max_duration": {"value": 200.0}})
                )}
            ]})
        );
        // Below 250: 100, 200 and 50, all INFO. From 100: 100, 300, 200 and 500, the 300 an
        // ERROR. The row of 100 and the row of 200 lie in both ranges and count under each.
        assert_eq!(
            aggregations["durations"],
            json!({"buckets": [
                {"key": "*-250.0", "to": 250.0, "doc_count": 3, "levels": build_levels(
                    json!({"doc_count": 0}), json!({"doc_count": 3})
                )},
                {"key": "100.0-*", "from": 100.0, "doc_count": 4, "levels": build_levels(
                    json!({"doc_count": 1}), json!({"doc_count": 4})
                )}
            ]})
        );
    }

    #[tokio::test]
    async fn fixed_buckets_at_max_buckets_are_answered_and_one_more_is_a_400() {
        // Every listed range is answered, the empty `from: 1000` one included, and counts.
        let build_ranges =
            |ranges: Value| json!({"durations": {"range": {"field": "duration_micros", "ranges": ranges}}});
        let request_below_limit = build_ranges(json!([{"to": 100}, {"from": 100}]));
        let request_at_limit = build_ranges(json!([{"to": 100}, {"from": 100, "to": 1000}, {"from": 1000}]));
        let request_above_limit = build_ranges(json!([
            {"to": 100},
            {"from": 100, "to": 1000},
            {"from": 1000},
            {"from": 5000}
        ]));
        let rows = || scan_test_rows(&build_hourly_rows());

        let answer_below_limit = aggregate_plan(rows(), &request_below_limit, 3)
            .await
            .expect("two buckets fit three");
        let answer_at_limit = aggregate_plan(rows(), &request_at_limit, 3)
            .await
            .expect("three buckets fit three");
        let error_above_limit = aggregate_plan(rows(), &request_above_limit, 3)
            .await
            .expect_err("four buckets exceed three");

        assert_eq!(
            summarize_buckets(&answer_below_limit["durations"], "no_sub_aggregation"),
            vec![json!(["*-100.0", 1, null]), json!(["100.0-*", 4, null])]
        );
        assert_eq!(
            summarize_buckets(&answer_at_limit["durations"], "no_sub_aggregation"),
            vec![
                json!(["*-100.0", 1, null]),
                json!(["100.0-1000.0", 4, null]),
                json!(["1000.0-*", 0, null])
            ]
        );
        assert_too_many_buckets(error_above_limit, 3).await;
    }

    #[tokio::test]
    async fn a_date_range_renders_a_named_key_an_open_end_and_its_format() {
        // `to` is a date string, `from` an integer of epoch milliseconds: both 11:00Z.
        let aggregations = run_aggregations(
            &build_hourly_rows(),
            json!({"hours": {"date_range": {
                "field": "timestamp",
                "format": "epoch_millis",
                "ranges": [
                    {"to": "2026-10-08T11:00:00Z", "key": "before_eleven"},
                    {"from": 1_791_457_200_000_i64}
                ]
            }}}),
        )
        .await
        .expect("aggregate the rows");

        // 10:05, 10:15 and 10:40 lie before 11:00; 11:10, 11:30 and 11:45 from it.
        assert_eq!(
            aggregations["hours"],
            json!({"buckets": [
                {
                    "key": "before_eleven",
                    "to": 1_791_457_200_000.0,
                    "to_as_string": "1791457200000",
                    "doc_count": 3
                },
                {
                    "key": "1791457200000-*",
                    "from": 1_791_457_200_000.0,
                    "from_as_string": "1791457200000",
                    "doc_count": 3
                }
            ]})
        );
    }

    #[tokio::test]
    async fn cardinality_is_exact_over_a_high_cardinality_column_written_by_the_test() {
        // 6000 rows cycle the services svc-0000 … svc-2499 (2500 distinct). Even rows are INFO
        // and odd rows ERROR; 2500 is even, so each severity sees the 1250 services of its
        // parity. The durations 0 … 5999 are all distinct. Ten more INFO rows have neither a
        // service nor a duration and count in no distinct value.
        let services: Vec<String> = (0..6000_i64).map(|index| format!("svc-{:04}", index % 2500)).collect();
        let mut rows: Vec<SourceRow> = services
            .iter()
            .zip(0_i64..)
            .map(|(service, index)| {
                let severity = if index % 2 == 0 { "INFO" } else { "ERROR" };
                build_test_row(
                    AT_10_05_00_000999 + index,
                    Some(service.as_str()),
                    Some(severity),
                    Some(index),
                    None,
                )
            })
            .collect();
        rows.extend((0..10_i64).map(|index| build_test_row(AT_11_10 + index, None, Some("INFO"), None, None)));

        let aggregations = run_aggregations(
            &rows,
            json!({
                "services": {"cardinality": {"field": "service_name"}},
                "durations": {"cardinality": {"field": "duration_micros"}},
                "severities": {
                    "terms": {"field": "severity_text", "order": {"_key": "asc"}},
                    "aggs": {"services": {"cardinality": {"field": "service_name"}}}
                }
            }),
        )
        .await
        .expect("aggregate the rows");

        assert_eq!(aggregations["services"], json!({"value": 2500}));
        assert_eq!(aggregations["durations"], json!({"value": 6000}));
        assert_eq!(
            summarize_buckets(&aggregations["severities"], "services"),
            vec![
                json!(["ERROR", 3000, {"value": 1250}]),
                json!(["INFO", 3010, {"value": 1250}])
            ]
        );
    }

    #[tokio::test]
    async fn terms_ordered_by_cardinality_under_a_date_histogram_rank_by_the_exact_count() {
        let rows = vec![
            // Hour 10: api has 3 severities, db 2, web 1.
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), None, None),
            build_test_row(AT_10_05_00_000999 + 1, Some("api"), Some("ERROR"), None, None),
            build_test_row(AT_10_05_00_000999 + 2, Some("api"), Some("WARN"), None, None),
            build_test_row(AT_10_15_30_123456, Some("web"), Some("INFO"), None, None),
            build_test_row(AT_10_15_30_123456 + 1, Some("web"), Some("INFO"), None, None),
            build_test_row(AT_10_40, Some("db"), Some("INFO"), None, None),
            build_test_row(AT_10_40 + 1, Some("db"), Some("ERROR"), None, None),
            // Hour 11: web has 4 severities, db 2, api 1.
            build_test_row(AT_11_10, Some("api"), Some("INFO"), None, None),
            build_test_row(AT_11_30, Some("web"), Some("INFO"), None, None),
            build_test_row(AT_11_30 + 1, Some("web"), Some("ERROR"), None, None),
            build_test_row(AT_11_30 + 2, Some("web"), Some("WARN"), None, None),
            build_test_row(AT_11_30 + 3, Some("web"), Some("DEBUG"), None, None),
            build_test_row(AT_11_45_10_456789, Some("db"), Some("INFO"), None, None),
            build_test_row(AT_11_45_10_456789 + 1, Some("db"), Some("ERROR"), None, None),
        ];
        let aggregations = run_aggregations(
            &rows,
            json!({"per_hour": {
                "date_histogram": {"field": "timestamp", "fixed_interval": "1h"},
                "aggs": {"services": {
                    "terms": {"field": "service_name", "size": 2, "order": {"severities": "desc"}},
                    "aggs": {"severities": {"cardinality": {"field": "severity_text"}}}
                }}
            }}),
        )
        .await
        .expect("aggregate the rows");

        let hours = &aggregations["per_hour"]["buckets"];
        assert_eq!(hours[0]["key"], json!(HOUR_10_KEY));
        assert_eq!(hours[0]["doc_count"], json!(7));
        assert_eq!(
            summarize_buckets(&hours[0]["services"], "severities"),
            vec![json!(["api", 3, {"value": 3}]), json!(["db", 2, {"value": 2}])]
        );
        assert_eq!(hours[0]["services"]["sum_other_doc_count"], json!(2));
        assert_eq!(hours[1]["key"], json!(HOUR_11_KEY));
        assert_eq!(hours[1]["doc_count"], json!(7));
        assert_eq!(
            summarize_buckets(&hours[1]["services"], "severities"),
            vec![json!(["web", 4, {"value": 4}]), json!(["db", 2, {"value": 2}])]
        );
        assert_eq!(hours[1]["services"]["sum_other_doc_count"], json!(1));
    }

    #[tokio::test]
    async fn a_two_level_pie_with_cardinality_at_both_levels_answers_the_stated_values() {
        let rows = vec![
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(100), None),
            build_test_row(AT_10_15_30_123456, Some("api"), Some("INFO"), Some(100), None),
            build_test_row(AT_10_40, Some("api"), Some("INFO"), Some(200), None),
            build_test_row(AT_11_10, Some("api"), Some("ERROR"), Some(300), None),
            build_test_row(AT_11_30, Some("web"), Some("INFO"), Some(100), None),
            build_test_row(AT_11_30 + 1, Some("web"), Some("ERROR"), Some(100), None),
            build_test_row(AT_11_45_10_456789, Some("web"), Some("ERROR"), Some(400), None),
        ];
        // A pie-style request: the metric "1" at both levels, each level ordered by it.
        let aggregations = run_aggregations(
            &rows,
            json!({"2": {
                "terms": {"field": "service_name", "size": 5, "order": {"1": "desc"}},
                "aggs": {
                    "1": {"cardinality": {"field": "duration_micros"}},
                    "3": {
                        "terms": {"field": "severity_text", "size": 5, "order": {"1": "desc"}},
                        "aggs": {"1": {"cardinality": {"field": "duration_micros"}}}
                    }
                }
            }}),
        )
        .await
        .expect("aggregate the rows");

        // api's durations {100, 200, 300}; web's {100, 400}.
        assert_eq!(
            summarize_buckets(&aggregations["2"], "1"),
            vec![json!(["api", 4, {"value": 3}]), json!(["web", 3, {"value": 2}])]
        );
        // api INFO {100, 200}, api ERROR {300}; web ERROR {100, 400}, web INFO {100}.
        assert_eq!(
            summarize_buckets(&aggregations["2"]["buckets"][0]["3"], "1"),
            vec![json!(["INFO", 3, {"value": 2}]), json!(["ERROR", 1, {"value": 1}])]
        );
        assert_eq!(
            summarize_buckets(&aggregations["2"]["buckets"][1]["3"], "1"),
            vec![json!(["ERROR", 2, {"value": 2}]), json!(["INFO", 1, {"value": 1}])]
        );
    }

    #[test]
    fn no_grouped_distinct_aggregate_over_a_non_integer_column_remains_in_the_plan() {
        let tree = parse_aggregations(&json!({
            "severities": {
                "terms": {"field": "severity_text"},
                "aggs": {"services": {"cardinality": {"field": "service_name"}}}
            },
            "all_services": {"cardinality": {"field": "service_name"}},
            "all_scores": {"cardinality": {"field": "score"}}
        }))
        .expect("parse the aggregations");
        let layout = AggregationLayout::new(&tree, read_request_now()).expect("lay out the aggregations");
        let rows = scan_test_rows(&build_hourly_rows());
        let inputs = resolve_plan_inputs(&layout, rows.schema()).expect("resolve the inputs");
        let plans = plan_aggregations(rows, &layout, &inputs).expect("plan the aggregations");
        assert_eq!(
            plans.distinct_branches.len(),
            2,
            "one distinct branch per granularity: the top and the severities level"
        );

        let state = SessionContext::new().state();
        let mut distinct_aggregates = Vec::new();
        let checked_plans = plans
            .stages
            .iter()
            .map(|stage| &stage.plan)
            .chain(plans.distinct_branches.iter().map(|branch| &branch.plan));
        for plan in checked_plans {
            let optimized = state.optimize(plan).expect("optimize the plan");
            optimized
                .apply(|node| {
                    if let LogicalPlan::Aggregate(aggregate) = node {
                        for expression in &aggregate.aggr_expr {
                            if let Expr::AggregateFunction(function) = expression.clone().unalias()
                                && function.params.distinct
                            {
                                let schema = aggregate.input.schema();
                                for argument in &function.params.args {
                                    if !argument.get_type(schema.as_ref())?.is_integer() {
                                        distinct_aggregates.push(expression.to_string());
                                    }
                                }
                            }
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .expect("walk the plan");
        }
        assert!(
            distinct_aggregates.is_empty(),
            "distinct aggregates over non-integer columns remain: {distinct_aggregates:?}"
        );
    }

    /// Six rows: four with a service and a severity, one without a service (INFO) and one without
    /// a severity (db); one of api's two ERROR rows has no duration.
    fn build_rows_with_null_keys_and_values() -> Vec<SourceRow> {
        vec![
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(100), None),
            build_test_row(AT_10_15_30_123456, Some("web"), Some("INFO"), Some(100), None),
            build_test_row(AT_10_40, None, Some("INFO"), Some(200), None),
            build_test_row(AT_11_10, Some("api"), Some("ERROR"), Some(300), None),
            build_test_row(AT_11_30, Some("api"), Some("ERROR"), None, None),
            build_test_row(AT_11_45_10_456789, Some("db"), None, Some(400), None),
        ]
    }

    #[tokio::test]
    async fn exact_distinct_counts_keep_the_null_key_group_once_and_count_no_null_value() {
        // Two counted expressions join their branches on the key; the NULL severity is a group of
        // its own on both sides, so the join keeps it once.
        let relation = LogicalPlanBuilder::from(scan_test_rows(&build_rows_with_null_keys_and_values()));
        let plan = plan_exact_distinct(
            relation,
            &[col("severity_text")],
            &[col("service_name"), col("duration_micros")],
        )
        .and_then(|builder| builder.build().map_err(convert_plan_error))
        .expect("plan the exact distinct counts");

        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .expect("plan the execution")
            .collect()
            .await
            .expect("execute the distinct counts");

        let mut counts_by_severity: BTreeMap<Option<String>, (i64, i64)> = BTreeMap::new();
        for batch in &batches {
            let severities = batch.column_by_name("severity_text").expect("the key column");
            let severities = severities.as_string::<i32>();
            let column = |position: usize| {
                batch
                    .column_by_name(&name_distinct_count_column(position))
                    .expect("a count column")
                    .as_primitive::<Int64Type>()
                    .clone()
            };
            let (services, durations) = (column(0), column(1));
            for row in 0..batch.num_rows() {
                let severity = severities.is_valid(row).then(|| severities.value(row).to_string());
                let previous = counts_by_severity.insert(severity, (services.value(row), durations.value(row)));
                assert_eq!(previous, None, "one row per group");
            }
        }
        assert_eq!(
            counts_by_severity,
            BTreeMap::from([
                (None, (1, 1)),
                (Some("ERROR".to_string()), (1, 1)),
                (Some("INFO".to_string()), (2, 2)),
            ])
        );
    }

    #[tokio::test]
    async fn two_cardinalities_under_one_terms_level_skip_the_null_key_and_null_values() {
        let aggregations = run_aggregations(
            &build_rows_with_null_keys_and_values(),
            json!({"severities": {
                "terms": {"field": "severity_text", "order": {"_key": "asc"}},
                "aggs": {
                    "services": {"cardinality": {"field": "service_name"}},
                    "durations": {"cardinality": {"field": "duration_micros"}}
                }
            }}),
        )
        .await
        .expect("aggregate the rows");

        // ERROR: {api} and {300}; INFO: {api, web} and {100, 200}. db's row has no severity and
        // forms no bucket.
        assert_eq!(
            aggregations["severities"]["buckets"],
            json!([
                {"key": "ERROR", "doc_count": 2, "services": {"value": 1}, "durations": {"value": 1}},
                {"key": "INFO", "doc_count": 3, "services": {"value": 2}, "durations": {"value": 2}}
            ])
        );
    }

    #[tokio::test]
    async fn three_top_level_cardinalities_plan_no_stage_and_answer_their_own_counts() {
        let request = json!({
            "services": {"cardinality": {"field": "service_name"}},
            "severities": {"cardinality": {"field": "severity_text"}},
            "durations": {"cardinality": {"field": "duration_micros"}}
        });
        let tree = parse_aggregations(&request).expect("parse the aggregations");
        let layout = AggregationLayout::new(&tree, read_request_now()).expect("lay out the aggregations");
        let rows = scan_test_rows(&build_hourly_rows());
        let inputs = resolve_plan_inputs(&layout, rows.schema()).expect("resolve the inputs");
        let plans = plan_aggregations(rows, &layout, &inputs).expect("plan the aggregations");
        // No bucket level and no metric a stage computes: a stage would count rows nothing reads.
        assert_eq!(plans.stages.len(), 0, "no stage aggregate");
        assert_eq!(plans.distinct_branches.len(), 1, "one branch for the top granularity");

        let aggregations = run_aggregations(&build_hourly_rows(), request)
            .await
            .expect("aggregate the rows");

        // Services {api, web, db}; severities {INFO, ERROR}; durations {100, 300, 200, 500, 50},
        // web's ERROR row having none.
        assert_eq!(
            aggregations,
            json!({
                "services": {"value": 3},
                "severities": {"value": 2},
                "durations": {"value": 5}
            })
        );
    }

    #[tokio::test]
    async fn an_empty_bucket_and_a_field_no_index_maps_count_zero_distinct_values() {
        // 12:40 UTC on 2026-10-08, and the key of its hour in epoch milliseconds.
        const AT_12_40: i64 = 1_791_463_200_000_000;
        const HOUR_12_KEY: i64 = 1_791_460_800_000;
        let rows = vec![
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), None, None),
            build_test_row(AT_10_40, Some("web"), Some("INFO"), None, None),
            build_test_row(AT_12_40, Some("api"), Some("INFO"), None, None),
        ];

        let aggregations = run_aggregations(
            &rows,
            json!({
                "per_hour": {
                    "date_histogram": {"field": "timestamp", "fixed_interval": "1h", "min_doc_count": 0},
                    "aggs": {"services": {"cardinality": {"field": "service_name"}}}
                },
                "unmapped": {"cardinality": {"field": "absent_field"}}
            }),
        )
        .await
        .expect("aggregate the rows");

        assert_eq!(
            summarize_buckets(&aggregations["per_hour"], "services"),
            vec![
                json!([HOUR_10_KEY, 2, {"value": 2}]),
                json!([HOUR_11_KEY, 0, {"value": 0}]),
                json!([HOUR_12_KEY, 1, {"value": 1}]),
            ]
        );
        assert_eq!(aggregations["unmapped"], json!({"value": 0}));
    }

    #[tokio::test]
    async fn percentiles_render_keyed_nn_n_keys_by_default_and_an_array_when_keyed_is_false() {
        // Each service holds one distinct duration, so every percentile equals it. The two
        // percentiles read the same field, so they share one digest under a grouping set.
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(100), None),
            build_test_row(AT_11_10, Some("api"), Some("INFO"), Some(100), None),
            build_test_row(AT_10_40, Some("web"), Some("INFO"), Some(250), None),
        ];
        let aggregations = run_aggregations(
            &rows,
            json!({"services": {
                "terms": {"field": "service_name", "order": {"_key": "asc"}},
                "aggs": {
                    "keyed": {"percentiles": {"field": "duration_micros", "percents": [50, 99]}},
                    "listed": {"percentiles": {"field": "duration_micros", "percents": [50, 99], "keyed": false}}
                }
            }}),
        )
        .await
        .expect("aggregate the rows");
        let buckets = &aggregations["services"]["buckets"];
        assert_eq!(buckets.as_array().map(Vec::len), Some(2));
        assert_eq!(buckets[0]["key"], json!("api"));
        assert_eq!(buckets[0]["keyed"], json!({"values": {"50.0": 100.0, "99.0": 100.0}}));
        assert_eq!(
            buckets[0]["listed"],
            json!({"values": [{"key": 50.0, "value": 100.0}, {"key": 99.0, "value": 100.0}]})
        );
        assert_eq!(buckets[1]["key"], json!("web"));
        assert_eq!(buckets[1]["keyed"], json!({"values": {"50.0": 250.0, "99.0": 250.0}}));
        assert_eq!(
            buckets[1]["listed"],
            json!({"values": [{"key": 50.0, "value": 250.0}, {"key": 99.0, "value": 250.0}]})
        );
    }

    #[tokio::test]
    async fn percentiles_without_percents_answer_the_seven_default_keys() {
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(400), None),
            build_test_row(AT_10_40, Some("web"), Some("INFO"), Some(400), None),
        ];
        let aggregations = run_aggregations(&rows, json!({"1": {"percentiles": {"field": "duration_micros"}}}))
            .await
            .expect("aggregate the rows");
        assert_eq!(
            aggregations["1"],
            json!({"values": {
                "1.0": 400.0, "5.0": 400.0, "25.0": 400.0, "50.0": 400.0,
                "75.0": 400.0, "95.0": 400.0, "99.0": 400.0
            }})
        );
    }

    #[tokio::test]
    async fn string_percents_normalize_to_one_decimal_keys() {
        let rows = [build_test_row(
            AT_10_05_00_000999,
            Some("api"),
            Some("INFO"),
            Some(300),
            None,
        )];
        let aggregations = run_aggregations(
            &rows,
            json!({"1": {"percentiles": {"field": "duration_micros", "percents": ["25", "50.5", 99]}}}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(
            aggregations["1"],
            json!({"values": {"25.0": 300.0, "50.5": 300.0, "99.0": 300.0}})
        );
    }

    #[tokio::test]
    async fn repeated_percents_render_one_keyed_entry_and_two_equal_listed_entries() {
        // A percent listed twice is estimated once and read at both of its positions.
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(300), None),
            build_test_row(AT_10_40, Some("api"), Some("INFO"), Some(300), None),
        ];
        let aggregations = run_aggregations(
            &rows,
            json!({
                "keyed": {"percentiles": {"field": "duration_micros", "percents": [50, 50]}},
                "listed": {"percentiles": {"field": "score", "percents": [50, 50], "keyed": false}},
                "listed_durations": {"percentiles": {"field": "duration_micros", "percents": [50, 50], "keyed": false}}
            }),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(aggregations["keyed"], json!({"values": {"50.0": 300.0}}));
        // No score is stored, so both entries of the listed form have no value.
        assert_eq!(
            aggregations["listed"],
            json!({"values": [{"key": 50.0, "value": null}, {"key": 50.0, "value": null}]})
        );
        assert_eq!(
            aggregations["listed_durations"],
            json!({"values": [{"key": 50.0, "value": 300.0}, {"key": 50.0, "value": 300.0}]})
        );
    }

    #[tokio::test]
    async fn a_single_percentiles_with_a_repeated_percent_reads_both_positions() {
        // The only percentiles of its stage: its digest is not shared, yet its list repeats a
        // percent, so each position reads the one estimate.
        let rows = [build_test_row(
            AT_10_05_00_000999,
            Some("api"),
            Some("INFO"),
            Some(700),
            None,
        )];
        let keyed_aggregations = run_aggregations(
            &rows,
            json!({"1": {"percentiles": {"field": "duration_micros", "percents": [50, 50]}}}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(keyed_aggregations["1"], json!({"values": {"50.0": 700.0}}));
        let listed_aggregations = run_aggregations(
            &rows,
            json!({"1": {"percentiles": {"field": "duration_micros", "percents": [50, 50], "keyed": false}}}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(
            listed_aggregations["1"],
            json!({"values": [{"key": 50.0, "value": 700.0}, {"key": 50.0, "value": 700.0}]})
        );
    }

    #[tokio::test]
    async fn percentile_ranks_are_exact() {
        // Four durations 100..400 and one row without a duration, which no rank counts.
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(100), None),
            build_test_row(AT_10_40, Some("api"), Some("INFO"), Some(200), None),
            build_test_row(AT_11_10, Some("api"), Some("INFO"), Some(300), None),
            build_test_row(AT_11_30, Some("api"), Some("INFO"), Some(400), None),
            build_test_row(AT_11_30, Some("web"), Some("ERROR"), None, None),
        ];
        let aggregations = run_aggregations(
            &rows,
            json!({"1": {"percentile_ranks": {"field": "duration_micros", "values": [250, 400, 50], "keyed": false}}}),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(
            aggregations["1"],
            json!({"values": [
                {"key": 250.0, "value": 50.0},
                {"key": 400.0, "value": 100.0},
                {"key": 50.0, "value": 0.0}
            ]})
        );
    }

    #[tokio::test]
    async fn terms_ordered_by_a_percentile_path_under_a_date_histogram_rank_by_that_percentile() {
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(100), None),
            build_test_row(AT_10_40, Some("web"), Some("INFO"), Some(300), None),
            build_test_row(AT_11_10, Some("api"), Some("INFO"), Some(500), None),
            build_test_row(AT_11_30, Some("web"), Some("INFO"), Some(200), None),
        ];
        for path in ["1[50.0]", "1['50.0']", "1.50"] {
            let aggregations = run_aggregations(
                &rows,
                json!({"hours": {
                    "date_histogram": {"field": "timestamp", "fixed_interval": "1h"},
                    "aggs": {"services": {
                        "terms": {"field": "service_name", "size": 5, "order": {path: "desc"}},
                        "aggs": {"1": {"percentiles": {"field": "duration_micros", "percents": [50]}}}
                    }}
                }}),
            )
            .await
            .expect("aggregate the rows");
            let hours = &aggregations["hours"]["buckets"];
            assert_eq!(hours.as_array().map(Vec::len), Some(2), "{path}");
            assert_eq!(hours[0]["key"], json!(HOUR_10_KEY), "{path}");
            assert_eq!(
                summarize_buckets(&hours[0]["services"], "1"),
                vec![
                    json!(["web", 1, {"values": {"50.0": 300.0}}]),
                    json!(["api", 1, {"values": {"50.0": 100.0}}]),
                ],
                "{path}"
            );
            assert_eq!(hours[1]["key"], json!(HOUR_11_KEY), "{path}");
            assert_eq!(
                summarize_buckets(&hours[1]["services"], "1"),
                vec![
                    json!(["api", 1, {"values": {"50.0": 500.0}}]),
                    json!(["web", 1, {"values": {"50.0": 200.0}}]),
                ],
                "{path}"
            );
        }
        // A percent the percentiles aggregation does not compute cannot order.
        let error = run_aggregations(
            &rows,
            json!({"services": {
                "terms": {"field": "service_name", "order": {"1[75.0]": "desc"}},
                "aggs": {"1": {"percentiles": {"field": "duration_micros", "percents": [50]}}}
            }}),
        )
        .await
        .expect_err("75 is not computed");
        render_refusal(error, "aggregation_execution_exception").await;
    }

    #[tokio::test]
    async fn a_terms_order_by_a_percentile_ranks_path_is_an_aggregation_execution_exception() {
        for order in [json!({"2[50.0]": "asc"}), json!({"2": "asc"}), json!({"2.50": "asc"})] {
            let error = run_aggregations(
                &build_hourly_rows(),
                json!({"services": {
                    "terms": {"field": "service_name", "order": order.clone()},
                    "aggs": {"2": {"percentile_ranks": {"field": "duration_micros", "values": [50]}}}
                }}),
            )
            .await
            .expect_err("a percentile_ranks cannot order buckets");
            render_refusal(error, "aggregation_execution_exception").await;
        }
    }

    #[tokio::test]
    async fn a_percent_order_path_on_a_metric_without_percents_is_an_aggregation_execution_exception() {
        // Without the refusal the path would order by the average itself.
        let error = run_aggregations(
            &build_hourly_rows(),
            json!({"services": {
                "terms": {"field": "service_name", "order": {"1[50.0]": "desc"}},
                "aggs": {"1": {"avg": {"field": "duration_micros"}}}
            }}),
        )
        .await
        .expect_err("an avg computes no percent");
        render_refusal(error, "aggregation_execution_exception").await;
    }

    #[tokio::test]
    async fn percentiles_over_the_same_field_share_one_digest_and_read_their_own_percents() {
        // Durations 100, 300, 500 and scores 1.5, 2.5, 4.0. Percent 0 is the minimum and 100
        // the maximum exactly, so each value is stated by hand.
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(100), Some(1.5)),
            build_test_row(AT_10_40, Some("api"), Some("INFO"), Some(300), Some(2.5)),
            build_test_row(AT_11_10, Some("api"), Some("INFO"), Some(500), Some(4.0)),
        ];
        let aggregations = json!({"services": {
            "terms": {"field": "service_name"},
            "aggs": {
                "a": {"percentiles": {"field": "duration_micros", "percents": [100, 0]}},
                "b": {"percentiles": {"field": "duration_micros", "percents": [0]}},
                "c": {"percentiles": {"field": "score", "percents": [100]}}
            }
        }});
        let rendered_aggregations = run_aggregations(&rows, aggregations.clone()).await.expect("aggregate the rows");
        let buckets = &rendered_aggregations["services"]["buckets"];
        assert_eq!(buckets.as_array().map(Vec::len), Some(1));
        let api = &buckets[0];
        assert_eq!(api["key"], json!("api"));
        assert_eq!(api["a"], json!({"values": {"100.0": 500.0, "0.0": 100.0}}));
        assert_eq!(api["b"], json!({"values": {"0.0": 100.0}}));
        assert_eq!(api["c"], json!({"values": {"100.0": 4.0}}));
        // One digest per field: "a" and "b" share the duration digest.
        let tree = parse_aggregations(&aggregations).expect("parse the request");
        let layout = AggregationLayout::new(&tree, read_request_now()).expect("lay out the request");
        let rows_plan = scan_test_rows(&rows);
        let inputs = resolve_plan_inputs(&layout, rows_plan.schema()).expect("resolve the inputs");
        let plans = plan_aggregations(rows_plan, &layout, &inputs).expect("plan the request");
        let mut digest_count = 0_usize;
        for stage in &plans.stages {
            stage
                .plan
                .apply(|node| {
                    if let LogicalPlan::Aggregate(aggregate) = node {
                        digest_count += aggregate
                            .aggr_expr
                            .iter()
                            .map(|expression| expression.clone().unalias())
                            .filter(|expression| {
                                matches!(
                                    expression,
                                    Expr::AggregateFunction(function) if function.func.name() == "multi_quantile"
                                )
                            })
                            .count();
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .expect("walk the plan");
        }
        assert_eq!(digest_count, 2);
    }

    #[tokio::test]
    async fn extended_stats_under_terms_answer_the_hand_computed_moments_per_bucket() {
        // api: 100, 200, 300, 600 (the hand computation of the metrics.rs unit test);
        // web: one row without a duration, so its statistics are empty.
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(100), None),
            build_test_row(AT_10_40, Some("api"), Some("INFO"), Some(200), None),
            build_test_row(AT_11_10, Some("api"), Some("INFO"), Some(300), None),
            build_test_row(AT_11_30, Some("api"), Some("INFO"), Some(600), None),
            build_test_row(AT_11_30, Some("web"), Some("ERROR"), None, None),
        ];
        let aggregations = run_aggregations(
            &rows,
            json!({"services": {
                "terms": {"field": "service_name", "order": {"_key": "asc"}},
                "aggs": {"1": {"extended_stats": {"field": "duration_micros", "sigma": 2}}}
            }}),
        )
        .await
        .expect("aggregate the rows");
        let api = &aggregations["services"]["buckets"][0]["1"];
        assert_eq!(api["count"], json!(4));
        assert_eq!(api["avg"], json!(300.0));
        assert_eq!(api["sum_of_squares"], json!(500_000.0));
        assert_eq!(api["variance_population"], json!(35_000.0));
        assert_eq!(api["variance_sampling"], json!(46_666.666_666_666_664));
        assert_eq!(api["std_deviation_bounds"]["upper"], json!(674.165_738_677_394_1));
        assert_eq!(
            api["std_deviation_bounds"]["lower_sampling"],
            json!(-132.049_379_893_857_35)
        );
        let web = &aggregations["services"]["buckets"][1]["1"];
        assert_eq!(web["count"], json!(0));
        assert_eq!(web["sum"], json!(0.0));
        assert_eq!(web["avg"], json!(null));
        assert_eq!(web["std_deviation_bounds"]["upper"], json!(null));
    }

    #[tokio::test]
    async fn terms_ordered_by_an_extended_stats_statistic_rank_by_it() {
        // api: 20 four times (max 20, sum 80, sum of squares 1600); web: 35 twice (max 35, sum
        // 70, sum of squares 2450); db: 45 (max 45, sum 45, sum of squares 2025). Each of the
        // three statistics ranks the services in a different order, so reading one statistic's
        // column for another fails.
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(20), None),
            build_test_row(AT_10_15_30_123456, Some("api"), Some("INFO"), Some(20), None),
            build_test_row(AT_10_40, Some("api"), Some("INFO"), Some(20), None),
            build_test_row(AT_11_10, Some("api"), Some("INFO"), Some(20), None),
            build_test_row(AT_10_40, Some("web"), Some("INFO"), Some(35), None),
            build_test_row(AT_11_10, Some("web"), Some("INFO"), Some(35), None),
            build_test_row(AT_10_05_00_000999, Some("db"), Some("INFO"), Some(45), None),
        ];
        for (order, expected_keys) in [
            ("1.max", vec![json!("db"), json!("web"), json!("api")]),
            ("1.sum", vec![json!("api"), json!("web"), json!("db")]),
            ("1.sum_of_squares", vec![json!("web"), json!("db"), json!("api")]),
        ] {
            let aggregations = run_aggregations(
                &rows,
                json!({"services": {
                    "terms": {"field": "service_name", "order": {order: "desc"}},
                    "aggs": {"1": {"extended_stats": {"field": "duration_micros"}}}
                }}),
            )
            .await
            .expect("aggregate the rows");
            assert_eq!(list_bucket_keys(&aggregations["services"]), expected_keys, "{order}");
        }
    }

    #[tokio::test]
    async fn terms_ordered_by_a_derived_extended_stats_statistic_is_an_aggregation_execution_exception() {
        let rows = [build_test_row(AT_10_40, Some("web"), Some("INFO"), Some(900), None)];
        let error = run_aggregations(
            &rows,
            json!({"services": {
                "terms": {"field": "service_name", "order": {"1.std_deviation": "desc"}},
                "aggs": {"1": {"extended_stats": {"field": "duration_micros"}}}
            }}),
        )
        .await
        .expect_err("std_deviation cannot order");
        render_refusal(error, "aggregation_execution_exception").await;
    }

    #[tokio::test]
    async fn a_top_hits_with_sub_aggregations_is_a_parsing_exception() {
        let error = run_aggregations(
            &build_hourly_rows(),
            json!({"latest": {
                "top_hits": {},
                "aggs": {"longest": {"max": {"field": "duration_micros"}}}
            }}),
        )
        .await
        .expect_err("a top_hits takes no sub-aggregations");
        render_refusal(error, "parsing_exception").await;
    }

    #[tokio::test]
    async fn a_terms_ordered_by_a_top_hits_is_an_aggregation_execution_exception() {
        // A `top_hits` computes no value a bucket could rank by.
        let error = run_aggregations(
            &build_hourly_rows(),
            json!({"services": {
                "terms": {"field": "service_name", "order": {"1": "desc"}},
                "aggs": {"1": {"top_hits": {}}}
            }}),
        )
        .await
        .expect_err("a top_hits cannot order");
        render_refusal(error, "aggregation_execution_exception").await;
    }

    /// 2026-10-08T12:15:00Z in microseconds and the key of its hour in epoch milliseconds.
    const AT_12_15: i64 = 1_791_461_700_000_000;
    const HOUR_12_KEY: i64 = 1_791_460_800_000;

    #[tokio::test]
    async fn a_metric_value_script_with_missing_applies_missing_before_the_script() {
        // Durations 1500, 2500 and one row without a duration.
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(1500), None),
            build_test_row(AT_10_40, Some("api"), Some("INFO"), Some(2500), None),
            build_test_row(AT_11_10, Some("api"), Some("INFO"), None, None),
        ];
        let aggregations = run_aggregations(
            &rows,
            json!({
                "1": {"avg": {"field": "duration_micros", "script": "_value / 1000", "missing": "0"}},
                "2": {"sum": {"field": "duration_micros", "script": "_value / 1000"}},
                "3": {"max": {"field": "duration_micros", "script": {"source": "(_value + 500) * 2", "lang": "painless"}}},
                "4": {"percentiles": {"field": "duration_micros", "script": "_value / 1000", "missing": 0, "percents": [100]}},
                "5": {"extended_stats": {"field": "duration_micros", "script": "_value / 1000"}}
            }),
        )
        .await
        .expect("aggregate the rows");
        // The missing row becomes 0 before the script: (1.5 + 2.5 + 0) / 3.
        assert_eq!(aggregations["1"], json!({"value": 1.333_333_333_333_333_3}));
        // Without missing, the row without a duration is skipped: 1.5 + 2.5.
        assert_eq!(aggregations["2"], json!({"value": 4.0}));
        assert_eq!(aggregations["3"], json!({"value": 6000.0}));
        assert_eq!(aggregations["4"], json!({"values": {"100.0": 2.5}}));
        assert_eq!(aggregations["5"]["count"], json!(2));
        assert_eq!(aggregations["5"]["sum"], json!(4.0));
        // A value script's non-finite result renders `null`: `x / 0` is infinite and `x % 0` is
        // not a number, and each statistic over them is too.
        let non_finite = run_aggregations(
            &rows,
            json!({
                "1": {"avg": {"field": "duration_micros", "script": "_value / 0"}},
                "2": {"sum": {"field": "duration_micros", "script": "_value % 0"}},
                "3": {"max": {"field": "duration_micros", "script": "-_value / 0"}},
                "4": {"percentiles": {"field": "duration_micros", "script": "_value / 0", "percents": [50]}},
                "5": {"extended_stats": {"field": "duration_micros", "script": "_value / 0"}}
            }),
        )
        .await
        .expect("aggregate the rows");
        assert_eq!(non_finite["1"], json!({"value": null}));
        assert_eq!(non_finite["2"], json!({"value": null}));
        assert_eq!(non_finite["3"], json!({"value": null}));
        assert_eq!(non_finite["4"], json!({"values": {"50.0": null}}));
        assert_eq!(non_finite["5"]["count"], json!(2));
        for statistic in [
            "min",
            "max",
            "avg",
            "sum",
            "sum_of_squares",
            "variance",
            "std_deviation",
        ] {
            assert_eq!(non_finite["5"][statistic], json!(null), "{statistic}");
        }
        for (aggregation, expected_type) in [
            (
                json!({"1": {"avg": {"field": "duration_micros", "script": "Math.log(_value)"}}}),
                "illegal_argument_exception",
            ),
            (
                json!({"1": {"value_count": {"field": "duration_micros", "script": "_value"}}}),
                "parsing_exception",
            ),
            (
                json!({"1": {"min": {"field": "timestamp", "script": "_value / 1000"}}}),
                "illegal_argument_exception",
            ),
        ] {
            let error = run_aggregations(&rows, aggregation.clone()).await.expect_err("refused");
            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{aggregation}");
            assert_eq!(body["error"]["type"], json!(expected_type), "{aggregation}");
        }
    }

    #[tokio::test]
    async fn pipelines_run_over_the_executed_date_histogram() {
        // Hour 10: 100 and 200; hour 11: no row (a filled gap); hour 12: 600.
        let rows = [
            build_test_row(AT_10_05_00_000999, Some("api"), Some("INFO"), Some(100), None),
            build_test_row(AT_10_40, Some("api"), Some("INFO"), Some(200), None),
            build_test_row(AT_12_15, Some("api"), Some("INFO"), Some(600), None),
        ];
        let aggregations = run_aggregations(
            &rows,
            json!({
                "hours": {
                    "date_histogram": {"field": "timestamp", "fixed_interval": "1h", "min_doc_count": 0, "format": "epoch_millis"},
                    "aggs": {
                        "1": {"sum": {"field": "duration_micros"}},
                        "3": {"cumulative_sum": {"buckets_path": "1"}},
                        "4": {"derivative": {"buckets_path": "1"}},
                        "5": {"moving_fn": {"buckets_path": "1", "window": 2, "script": "MovingFunctions.unweightedAvg(values)"}},
                        "6": {"bucket_script": {"buckets_path": {"s": "1", "c": "_count"}, "script": "params.s / params.c"}}
                    }
                },
                "7": {"max_bucket": {"buckets_path": "hours>1"}}
            }),
        )
        .await
        .expect("aggregate the rows");
        let hours = aggregations["hours"]["buckets"].as_array().expect("hour buckets");
        // `null` stands for a key the bucket does not carry; a carried `{value: null}` is written
        // as such.
        let summary: Vec<Value> = hours
            .iter()
            .map(|hour| {
                json!([
                    hour["key"],
                    hour["doc_count"],
                    hour.get("3"),
                    hour.get("4"),
                    hour.get("5"),
                    hour.get("6")
                ])
            })
            .collect();
        assert_eq!(
            summary,
            vec![
                json!([HOUR_10_KEY, 2, {"value": 300.0}, null, {"value": null}, {"value": 150.0}]),
                json!([HOUR_11_KEY, 0, {"value": 300.0}, {"value": null}, null, null]),
                json!([HOUR_12_KEY, 1, {"value": 900.0}, {"value": null}, {"value": 300.0}, {"value": 600.0}]),
            ]
        );
        assert_eq!(
            aggregations["7"],
            json!({"value": 600.0, "keys": [HOUR_12_KEY.to_string()]})
        );
    }
}
