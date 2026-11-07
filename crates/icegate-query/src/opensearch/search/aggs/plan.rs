//! Planning and executing a search's aggregations over its matching rows.
//!
//! Each top-level aggregation is a chain of bucket levels, each level the single sub-aggregation
//! of the one above. A chain is answered in two plans:
//!
//! 1. One `Aggregate` over GROUPING SETS: one set per level, the ROLLUP of the key prefixes.
//!    `grouping()` tags each grouped row with the level it belongs to, so a level's buckets are
//!    told apart from its ancestors' even when a key is NULL. The groups are collected once,
//!    and the second plan reads them from memory at every level, so the matching rows are
//!    scanned and grouped a single time however many levels the chain has.
//! 2. Over the collected groups: per level, the buckets whose own key is not NULL (a row without
//!    the key counts in its parent and emits no bucket) and that hold at least `min_doc_count`
//!    rows, semi-joined top-down to the surviving buckets of the level above. The levels are
//!    concatenated and capped at one row more than the bucket budget, so a search over the limit
//!    is refused without collecting every bucket.
//!
//! Assembly into the response tree, gap filling and ordering happen in Rust (`assemble`).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::functions_aggregate::grouping::grouping_udaf;
use datafusion::logical_expr::{
    Expr, JoinType, LogicalPlan, LogicalPlanBuilder, TableSource, cast, col, grouping_set, lit,
};
use serde_json::{Map, Value};

use super::assemble::{BucketBudget, assemble_buckets};
use super::date_histogram::BucketRounding;
use super::parse::{AggregationKind, AggregationTree, DateHistogramSpec, NamedAggregation, read_epoch_millis};
use super::render::render_bucket_list;
use crate::opensearch::catalog::{FieldType, FieldValue, IndexEntry, ResolvedField, TenantSession};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::doc_id::MICROS_PER_MILLISECOND;
use crate::opensearch::search::execute::convert_plan_error;
use crate::query_dsl::ast::Query;
use crate::query_dsl::lower::{LoweringScope, lower_query};

/// Column of the level a bucket row belongs to; `0` is the outermost.
pub(super) const LEVEL_COLUMN: &str = "__level";
/// Column of a bucket's document count.
pub(super) const DOC_COUNT_COLUMN: &str = "__doc_count";
/// Column of the `grouping()` bitmask: a set bit marks a key the row's grouping set omits, as
/// `grouping_marks_the_keys_a_level_omits_with_the_last_key_as_the_low_bit` pins.
const LEVEL_MASK_COLUMN: &str = "__level_mask";
/// The in-memory table holding the collected groups.
const GROUPS_TABLE: &str = "bucket_groups";
/// Most bucket levels one chain may nest; the level mask is an `i32`.
const MAX_BUCKET_LEVELS: usize = 16;

/// The column of the key of level `depth`.
pub(super) fn name_key_column(depth: usize) -> String {
    format!("__key_{depth}")
}

/// What executing aggregations needs from the request.
pub(crate) struct AggregationScope<'a> {
    /// The tenant session the plans execute in.
    pub(crate) session: &'a TenantSession,
    /// The most buckets the request may answer, every level of every aggregation counted.
    pub(crate) max_buckets: usize,
}

/// One bucket level of an aggregation chain, with what planning resolved for it.
pub(super) struct BucketLevel<'a> {
    /// The aggregation's request name.
    pub(super) name: &'a str,
    /// The kind of buckets the level makes.
    pub(super) kind: LevelKind<'a>,
}

/// The bucket kinds a level can make.
pub(super) enum LevelKind<'a> {
    /// `date_histogram` buckets.
    DateHistogram {
        /// The request.
        spec: &'a DateHistogramSpec,
        /// Its rounding.
        rounding: BucketRounding,
    },
}

impl BucketLevel<'_> {
    /// The key of this level for each matching row, as SQL over `rows`.
    fn key_expression(&self, rows: &LogicalPlan) -> Result<Expr, OpenSearchError> {
        match &self.kind {
            LevelKind::DateHistogram { spec, rounding } => {
                let field = find_row_column(rows, &spec.field, self.name)?;
                let value = match &spec.missing {
                    Some(missing) => {
                        let missing_micros = read_epoch_millis(self.name, "missing", missing)?
                            .checked_mul(MICROS_PER_MILLISECOND)
                            .ok_or_else(|| {
                                OpenSearchError::unsupported_aggregation(
                                    self.name,
                                    format!("[missing] of [{}] is out of range", self.name),
                                )
                            })?;
                        coalesce(vec![
                            field,
                            lit(ScalarValue::TimestampMicrosecond(Some(missing_micros), None)),
                        ])
                    }
                    None => field,
                };
                rounding.key_expression(value)
            }
        }
    }

    /// The smallest document count a bucket of this level needs, when it filters any: every
    /// grouped bucket already holds one document.
    fn read_minimum_doc_count(&self) -> Option<i64> {
        let minimum = match &self.kind {
            LevelKind::DateHistogram { spec, .. } => spec.min_doc_count,
        };
        (minimum > 1).then(|| i64::try_from(minimum).unwrap_or(i64::MAX))
    }
}

/// Answer the aggregations of `tree` over `rows`.
///
/// `rows` holds one column per field the tree names, named by the field name and holding the
/// field's single type across the searched indices; a row lacking the field holds NULL
/// ([`plan_aggregation_rows`] builds it). The answer maps each top-level aggregation's request
/// name to its rendered result.
///
/// # Errors
///
/// `400 too_many_buckets_exception` past `scope.max_buckets`; `400 illegal_argument_exception`
/// for a nesting not planned yet; `500` when planning or execution fails.
pub(crate) async fn execute_aggregations(
    rows: LogicalPlan,
    tree: &AggregationTree,
    scope: &AggregationScope<'_>,
) -> Result<Map<String, Value>, OpenSearchError> {
    let mut budget = BucketBudget::new(scope.max_buckets);
    let mut rendered = Map::new();
    for aggregation in &tree.aggregations {
        let chain = collect_chain(aggregation)?;
        let levels = plan_levels(&chain)?;
        let group_plan = plan_bucket_groups(rows.clone(), &levels)?;
        let groups = scope.session.collect(group_plan.clone()).await?;
        let survivor_plan = plan_surviving_buckets(&group_plan, groups, &levels, budget.remaining())?;
        let bucket_rows = scope.session.collect(survivor_plan).await?;
        let buckets = assemble_buckets(&bucket_rows, &levels, &mut budget)?;
        rendered.insert(aggregation.name.clone(), render_bucket_list(&buckets, &levels)?);
    }
    Ok(rendered)
}

/// The bucket levels of `aggregation`, outermost first.
fn collect_chain(aggregation: &NamedAggregation) -> Result<Vec<&NamedAggregation>, OpenSearchError> {
    let mut chain = vec![aggregation];
    let mut innermost_aggregation = aggregation;
    while let Some(child) = find_single_child(innermost_aggregation)? {
        chain.push(child);
        innermost_aggregation = child;
    }
    if chain.len() > MAX_BUCKET_LEVELS {
        return Err(OpenSearchError::aggregation_depth_exceeded(
            &aggregation.name,
            MAX_BUCKET_LEVELS,
        ));
    }
    check_supported_chain(&chain)?;
    Ok(chain)
}

fn find_single_child(aggregation: &NamedAggregation) -> Result<Option<&NamedAggregation>, OpenSearchError> {
    match aggregation.children.as_slice() {
        [] => Ok(None),
        [child] => Ok(Some(child)),
        _ => Err(OpenSearchError::unsupported_aggregation(
            &aggregation.name,
            format!(
                "[{}] has several sub-aggregations, which is not supported yet",
                aggregation.name
            ),
        )),
    }
}

/// The nestings answered today: one `date_histogram` without sub-aggregations. Any other nesting
/// is refused rather than answered in part.
fn check_supported_chain(chain: &[&NamedAggregation]) -> Result<(), OpenSearchError> {
    match chain {
        [_] => Ok(()),
        [parent, ..] => Err(OpenSearchError::unsupported_aggregation(
            &parent.name,
            format!("the sub-aggregations of [{}] are not supported yet", parent.name),
        )),
        [] => Err(OpenSearchError::internal("an empty aggregation chain")),
    }
}

fn plan_levels<'a>(chain: &[&'a NamedAggregation]) -> Result<Vec<BucketLevel<'a>>, OpenSearchError> {
    chain
        .iter()
        .map(|aggregation| {
            let kind = match &aggregation.kind {
                AggregationKind::DateHistogram(spec) => LevelKind::DateHistogram {
                    spec,
                    rounding: BucketRounding::from_spec(spec)?,
                },
            };
            Ok(BucketLevel {
                name: &aggregation.name,
                kind,
            })
        })
        .collect()
}

/// The column of `rows` holding `field`, which planning names after the field.
fn find_row_column(rows: &LogicalPlan, field: &str, aggregation: &str) -> Result<Expr, OpenSearchError> {
    if rows.schema().field_with_unqualified_name(field).is_err() {
        return Err(OpenSearchError::internal(format!(
            "the rows of [{aggregation}] lack the field [{field}]"
        )));
    }
    Ok(Expr::Column(Column::from_name(field)))
}

/// The `grouping()` mask of the rows of level `depth` among `level_count` levels: the bits of the
/// keys below the level are set, the last key being the least significant bit. That layout is
/// DataFusion's; `grouping_marks_the_keys_a_level_omits_with_the_last_key_as_the_low_bit` fails if
/// it changes.
fn compute_level_mask(level_count: usize, depth: usize) -> Result<i32, OpenSearchError> {
    let omitted = u32::try_from(level_count - 1 - depth)
        .map_err(|_| OpenSearchError::internal("too many bucket levels for the level mask"))?;
    1_i32
        .checked_shl(omitted)
        .map(|bit| bit - 1)
        .ok_or_else(|| OpenSearchError::internal("too many bucket levels for the level mask"))
}

/// Plan 1: the matching rows grouped over GROUPING SETS of the key prefixes.
fn plan_bucket_groups(rows: LogicalPlan, levels: &[BucketLevel<'_>]) -> Result<LogicalPlan, OpenSearchError> {
    let mut keyed = Vec::with_capacity(levels.len());
    for (depth, level) in levels.iter().enumerate() {
        keyed.push(level.key_expression(&rows)?.alias(name_key_column(depth)));
    }
    let keys: Vec<Expr> = (0..levels.len()).map(|depth| col(name_key_column(depth))).collect();
    let sets: Vec<Vec<Expr>> = (1..=keys.len()).map(|length| keys[..length].to_vec()).collect();
    let mut output = keys.clone();
    output.push(col(DOC_COUNT_COLUMN));
    output.push(col(LEVEL_MASK_COLUMN));
    LogicalPlanBuilder::from(rows)
        .project(keyed)
        .and_then(|builder| {
            builder.aggregate(
                vec![grouping_set(sets)],
                vec![
                    count(lit(1_i64)).alias(DOC_COUNT_COLUMN),
                    grouping_udaf().call(keys).alias(LEVEL_MASK_COLUMN),
                ],
            )
        })
        .and_then(|builder| builder.project(output))
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)
}

/// Plan 2: the surviving buckets of every level over the collected `groups`, capped at one row
/// more than `row_limit`.
fn plan_surviving_buckets(
    group_plan: &LogicalPlan,
    groups: Vec<RecordBatch>,
    levels: &[BucketLevel<'_>],
    row_limit: usize,
) -> Result<LogicalPlan, OpenSearchError> {
    let schema = groups
        .first()
        .map_or_else(|| Arc::new(group_plan.schema().as_arrow().clone()), RecordBatch::schema);
    let table = MemTable::try_new(schema, vec![groups]).map_err(convert_plan_error)?;
    let source = provider_as_source(Arc::new(table));
    let mut survivors: Vec<LogicalPlan> = Vec::with_capacity(levels.len());
    for (depth, level) in levels.iter().enumerate() {
        let survivor = plan_level_survivors(&source, levels.len(), depth, level, survivors.last())?;
        survivors.push(survivor);
    }
    let mut survivors = survivors.into_iter();
    let first = survivors
        .next()
        .ok_or_else(|| OpenSearchError::internal("an aggregation without levels"))?;
    survivors
        .try_fold(LogicalPlanBuilder::from(first), LogicalPlanBuilder::union)
        .and_then(|builder| builder.limit(0, Some(row_limit.saturating_add(1))))
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)
}

/// The surviving buckets of level `depth`: `LEVEL_COLUMN`, every key column, `DOC_COUNT_COLUMN`.
fn plan_level_survivors(
    source: &Arc<dyn TableSource>,
    level_count: usize,
    depth: usize,
    level: &BucketLevel<'_>,
    parent: Option<&LogicalPlan>,
) -> Result<LogicalPlan, OpenSearchError> {
    let mut predicate = col(LEVEL_MASK_COLUMN)
        .eq(lit(compute_level_mask(level_count, depth)?))
        .and(col(name_key_column(depth)).is_not_null());
    if let Some(minimum) = level.read_minimum_doc_count() {
        predicate = predicate.and(col(DOC_COUNT_COLUMN).gt_eq(lit(minimum)));
    }
    let mut builder = LogicalPlanBuilder::scan(GROUPS_TABLE, Arc::clone(source), None)
        .and_then(|builder| builder.filter(predicate))
        .map_err(convert_plan_error)?;
    if let Some(parent) = parent {
        builder = semi_join_parent(builder, parent.clone(), depth)?;
    }
    let depth_literal = i32::try_from(depth).map_err(|_| OpenSearchError::internal("too many bucket levels"))?;
    let mut output = vec![lit(depth_literal).alias(LEVEL_COLUMN)];
    output.extend((0..level_count).map(|position| col(name_key_column(position))));
    output.push(col(DOC_COUNT_COLUMN));
    builder
        .project(output)
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)
}

/// Keep the buckets of level `depth` whose ancestor keys name a surviving bucket of `parent`.
fn semi_join_parent(
    builder: LogicalPlanBuilder,
    parent: LogicalPlan,
    depth: usize,
) -> Result<LogicalPlanBuilder, OpenSearchError> {
    let level_alias = format!("level_{depth}");
    let parent_alias = format!("parent_{depth}");
    let level_keys: Vec<Column> = (0..depth)
        .map(|position| Column::new(Some(level_alias.as_str()), name_key_column(position)))
        .collect();
    let parent_keys: Vec<Column> = (0..depth)
        .map(|position| Column::new(Some(parent_alias.as_str()), name_key_column(position)))
        .collect();
    let parent = LogicalPlanBuilder::from(parent)
        .alias(parent_alias.as_str())
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)?;
    builder
        .alias(level_alias.as_str())
        .and_then(|builder| builder.join(parent, JoinType::LeftSemi, (level_keys, parent_keys), None))
        .map_err(convert_plan_error)
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

/// The matching rows of a search, projected onto the fields `tree` reads.
///
/// One branch per index: its scan, filtered by the query lowered for that index, projected onto
/// one column per field (NULL where the index lacks the field), every column cast to the field's
/// single type. The branches are concatenated with `UNION ALL`; because every branch already
/// has the same column types, no union coercion takes place. With no index the plan is empty.
///
/// # Errors
///
/// `400 illegal_argument_exception` for a field an aggregation cannot read (a `date_histogram`
/// over a field that is not a date); the lowering's errors; `500` when planning fails.
pub(crate) fn plan_aggregation_rows(
    session: &TenantSession,
    entries: &[&IndexEntry],
    query: &Query,
    now: DateTime<Utc>,
    tree: &AggregationTree,
) -> Result<LogicalPlan, OpenSearchError> {
    let fields = resolve_aggregation_fields(session, entries, tree)?;
    let mut branches = Vec::with_capacity(entries.len());
    for (position, &entry) in entries.iter().enumerate() {
        let scope = LoweringScope { session, entry, now };
        let predicate = lower_query(query, &scope)?;
        let projection = fields
            .iter()
            .map(|field| build_branch_column(field, position))
            .collect::<Result<Vec<_>, _>>()?;
        let branch = session
            .scan_index(entry)?
            .filter(predicate)
            .and_then(|builder| builder.project(projection))
            .and_then(LogicalPlanBuilder::build)
            .map_err(convert_plan_error)?;
        branches.push(branch);
    }
    let mut branches = branches.into_iter();
    let Some(first) = branches.next() else {
        let nulls = fields
            .iter()
            .map(|field| build_typed_null(&field.data_type).map(|null| null.alias(field.name.as_str())))
            .collect::<Result<Vec<_>, _>>()?;
        return LogicalPlanBuilder::empty(false)
            .project(nulls)
            .and_then(LogicalPlanBuilder::build)
            .map_err(convert_plan_error);
    };
    branches
        .try_fold(LogicalPlanBuilder::from(first), LogicalPlanBuilder::union)
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)
}

/// The distinct fields the aggregations of `tree` read, each checked against every aggregation
/// that reads it.
fn resolve_aggregation_fields(
    session: &TenantSession,
    entries: &[&IndexEntry],
    tree: &AggregationTree,
) -> Result<Vec<AggregationField>, OpenSearchError> {
    let mut fields: Vec<AggregationField> = Vec::new();
    for aggregation in list_aggregations(tree) {
        let name = aggregation.kind.field();
        let resolved: Vec<Option<ResolvedField>> =
            entries.iter().map(|entry| session.resolve_field(entry, name)).collect();
        check_field_for_aggregation(aggregation, entries, &resolved)?;
        if fields.iter().all(|field| field.name != name) {
            fields.push(AggregationField {
                name: name.to_string(),
                data_type: choose_row_data_type(aggregation, &resolved),
                values: resolved.into_iter().map(|field| field.map(|field| field.value)).collect(),
            });
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

/// Refuse a field `aggregation` cannot read in one of the indices that map it.
fn check_field_for_aggregation(
    aggregation: &NamedAggregation,
    entries: &[&IndexEntry],
    resolved: &[Option<ResolvedField>],
) -> Result<(), OpenSearchError> {
    let mapped_field_types = entries
        .iter()
        .zip(resolved)
        .filter_map(|(entry, field)| field.as_ref().map(|field| (entry.name(), field.field_type)));
    match &aggregation.kind {
        AggregationKind::DateHistogram(spec) => {
            for (index, field_type) in mapped_field_types {
                if !matches!(field_type, FieldType::Date) {
                    return Err(OpenSearchError::unsupported_aggregation_field(
                        &spec.field,
                        field_type.name(),
                        index,
                        &aggregation.name,
                        "date_histogram",
                    ));
                }
            }
            Ok(())
        }
    }
}

/// The type a field holds in the rows plan: the type of its first mapping, or the kind's own
/// type when no searched index maps it.
fn choose_row_data_type(aggregation: &NamedAggregation, resolved: &[Option<ResolvedField>]) -> DataType {
    resolved
        .iter()
        .flatten()
        .find_map(map_field_data_type)
        .unwrap_or(match &aggregation.kind {
            AggregationKind::DateHistogram(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
        })
}

/// The Arrow type a field of `field`'s type holds in the rows plan; `None` for a type no
/// aggregation reads.
const fn map_field_data_type(field: &ResolvedField) -> Option<DataType> {
    match (&field.value, field.field_type) {
        (FieldValue::HexId { width, .. }, _) => Some(DataType::FixedSizeBinary(*width)),
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
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use axum::http::StatusCode;
    use chrono::{DateTime, Utc};
    use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, TimestampMicrosecondArray};
    use datafusion::arrow::datatypes::{Int32Type, Int64Type};
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
    use datafusion::prelude::SessionContext;
    use icegate_common::LOGS_TABLE;
    use icegate_common::schema::COL_TIMESTAMP;
    use serde_json::{Value, json};

    use super::{
        AggregationScope, DOC_COUNT_COLUMN, LEVEL_MASK_COLUMN, MAX_BUCKET_LEVELS, compute_level_mask,
        execute_aggregations, find_single_child, name_key_column, plan_aggregation_rows, plan_bucket_groups,
        plan_levels, plan_surviving_buckets,
    };
    use crate::opensearch::SearchLimitsConfig;
    use crate::opensearch::error::{OpenSearchError, render_envelope};
    use crate::opensearch::search::aggs::parse::{AggregationTree, NamedAggregation, parse_aggregations};
    use crate::opensearch::search::aggs::test_support::parse_instant_micros;
    use crate::query_dsl::parse::parse_query;
    use crate::test_support::{FIXTURE_COMMITTED_WAL_OFFSET, SearchFixture, TestCell, TestRow, find_index_entry};

    /// A bucket limit no test reaches unless it sets its own.
    fn read_default_max_buckets() -> usize {
        SearchLimitsConfig::default().max_buckets
    }

    /// Rows of one nullable `Timestamp(µs, None)` column named `field`.
    fn build_date_rows(field: &str, instants: &[Option<&str>]) -> RecordBatch {
        let values: TimestampMicrosecondArray = instants.iter().map(|text| text.map(parse_instant_micros)).collect();
        RecordBatch::try_from_iter(vec![(field, Arc::new(values) as ArrayRef)]).expect("rows batch")
    }

    /// A scan of `rows`, standing for the matching rows of a search.
    fn scan_rows(rows: RecordBatch) -> LogicalPlan {
        let table = MemTable::try_new(rows.schema(), vec![vec![rows]]).expect("rows table");
        LogicalPlanBuilder::scan("rows", provider_as_source(Arc::new(table)), None)
            .and_then(LogicalPlanBuilder::build)
            .expect("rows plan")
    }

    /// Answer `aggregations` over `rows` with the production entry point, executing its plans in
    /// a tenant session of an in-memory warehouse.
    async fn aggregate_plan(
        rows: LogicalPlan,
        aggregations: &Value,
        max_buckets: usize,
    ) -> Result<Value, OpenSearchError> {
        let tree = parse_aggregations(aggregations)?;
        let fixture = SearchFixture::new().await;
        let session = fixture.open_session("tenant-a").await;
        let scope = AggregationScope {
            session: &session,
            max_buckets,
        };
        execute_aggregations(rows, &tree, &scope).await.map(Value::Object)
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
            max_buckets: read_default_max_buckets(),
        };
        let answer = execute_aggregations(rows, &tree, &scope)
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

    #[tokio::test]
    async fn sub_aggregations_of_a_date_histogram_are_refused_until_they_are_planned() {
        let error = aggregate_plan(
            scan_rows(build_date_rows("timestamp", &[Some("2026-10-08T10:30:00Z")])),
            &json!({"outer": {
                "date_histogram": {"field": "timestamp", "fixed_interval": "1h"},
                "aggs": {"inner": {"date_histogram": {"field": "timestamp", "fixed_interval": "1m"}}}
            }}),
            read_default_max_buckets(),
        )
        .await
        .expect_err("refused");

        assert_unsupported_aggregation(error, "outer").await;
    }

    /// Render `error` and check it is a `400 illegal_argument_exception` naming `aggregation`.
    async fn assert_unsupported_aggregation(error: OpenSearchError, aggregation: &str) {
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["aggregation"], aggregation);
    }

    #[tokio::test]
    async fn sibling_sub_aggregations_are_refused_naming_their_parent() {
        // The child lookup itself is called: the whole chain would also be refused, naming the
        // same parent, by the one-level check that runs after it.
        let tree = parse_aggregations(&json!({"outer": {
            "date_histogram": {"field": "timestamp", "fixed_interval": "1h"},
            "aggs": {
                "minutes": {"date_histogram": {"field": "timestamp", "fixed_interval": "1m"}},
                "seconds": {"date_histogram": {"field": "timestamp", "fixed_interval": "1s"}}
            }
        }}))
        .expect("two sibling date_histograms");
        let outer = tree.aggregations.first().expect("one top-level aggregation");

        let error = find_single_child(outer).expect_err("refused");

        assert_unsupported_aggregation(error, "outer").await;
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

        // `limit` is set by the depth refusal alone, not by the one-level check after it.
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert_eq!(body["error"]["aggregation"], "level_0");
        assert_eq!(body["error"]["limit"], MAX_BUCKET_LEVELS);
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

    /// The groups and the surviving bucket rows of `chain` over `rows`, the bucket rows capped at
    /// one past `row_limit`, each plan executed in a plain session.
    async fn collect_bucket_plans(
        rows: RecordBatch,
        chain: &[&NamedAggregation],
        row_limit: usize,
    ) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let context = SessionContext::new();
        let levels = plan_levels(chain).expect("the levels");
        let group_plan = plan_bucket_groups(scan_rows(rows), &levels).expect("the group plan");
        let groups = context
            .execute_logical_plan(group_plan.clone())
            .await
            .expect("execute the group plan")
            .collect()
            .await
            .expect("collect the groups");
        let survivor_plan =
            plan_surviving_buckets(&group_plan, groups.clone(), &levels, row_limit).expect("the survivor plan");
        let bucket_rows = context
            .execute_logical_plan(survivor_plan)
            .await
            .expect("execute the survivor plan")
            .collect()
            .await
            .expect("collect the bucket rows");
        (groups, bucket_rows)
    }

    /// The chain of `tree`'s first aggregation, its single child after it at every level.
    fn list_chain(tree: &AggregationTree) -> Vec<&NamedAggregation> {
        let mut chain = vec![&tree.aggregations[0]];
        while let [child] = chain[chain.len() - 1].children.as_slice() {
            chain.push(child);
        }
        chain
    }

    #[tokio::test]
    async fn grouping_marks_the_keys_a_level_omits_with_the_last_key_as_the_low_bit() {
        // Three levels (days, hours, minutes) over two rows of one hour: the day and hour levels
        // hold one group each, the minute level two.
        let tree = parse_aggregations(&json!({"days": {
            "date_histogram": {"field": "timestamp", "calendar_interval": "1d"},
            "aggs": {"hours": {
                "date_histogram": {"field": "timestamp", "calendar_interval": "1h"},
                "aggs": {"minutes": {"date_histogram": {"field": "timestamp", "calendar_interval": "1m"}}}
            }}
        }}))
        .expect("three nested date_histograms");
        let rows = build_date_rows(
            "timestamp",
            &[Some("2026-10-08T10:30:00Z"), Some("2026-10-08T10:31:00Z")],
        );

        let (groups, _) = collect_bucket_plans(rows, &list_chain(&tree), read_default_max_buckets()).await;

        // Mask -> (groups, rows counted) of the grouping set the mask tags.
        let mut levels: BTreeMap<i32, (usize, i64)> = BTreeMap::new();
        for batch in &groups {
            let masks = batch
                .column_by_name(LEVEL_MASK_COLUMN)
                .expect("the mask column")
                .as_primitive::<Int32Type>();
            let counts = batch
                .column_by_name(DOC_COUNT_COLUMN)
                .expect("the count column")
                .as_primitive::<Int64Type>();
            for row in 0..batch.num_rows() {
                let level = levels.entry(masks.value(row)).or_default();
                level.0 += 1;
                level.1 += counts.value(row);
            }
        }
        // The day level omits the hour and minute keys (bits 1 and 0), the hour level the minute
        // key (bit 0), the minute level none.
        assert_eq!(levels, BTreeMap::from([(3, (1, 2)), (1, (1, 2)), (0, (2, 2))]));
        assert_eq!(
            (0..3)
                .map(|depth| compute_level_mask(3, depth).expect("a mask"))
                .collect::<Vec<_>>(),
            vec![3, 1, 0]
        );
        // The key a level omits is NULL in its rows.
        for batch in &groups {
            let masks = batch
                .column_by_name(LEVEL_MASK_COLUMN)
                .expect("the mask column")
                .as_primitive::<Int32Type>();
            let minute_keys = batch.column_by_name(&name_key_column(2)).expect("the minute key column");
            for row in 0..batch.num_rows() {
                assert_eq!(minute_keys.is_null(row), masks.value(row) != 0, "row {row}");
            }
        }
    }

    /// Render `error` and check it is the bucket-limit refusal for `max`.
    async fn assert_too_many_buckets(error: OpenSearchError, max: usize) {
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "search_phase_execution_exception");
        let cause = &body["error"]["root_cause"][0];
        assert_eq!(cause["type"], "too_many_buckets_exception");
        assert_eq!(cause["max_buckets"], max);
    }

    #[tokio::test]
    async fn data_buckets_past_max_buckets_are_refused_after_collecting_one_bucket_past_the_limit() {
        // Five hourly buckets with a row each and no gap filling: the empty-bucket enumeration
        // never runs, so only the bucket rows reach the limit.
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
        let past_limit = aggregate_plan(scan_rows(rows()), &hours, 2)
            .await
            .expect_err("five buckets exceed two");
        let tree = parse_aggregations(&hours).expect("a date_histogram");
        let (_, capped_rows) = collect_bucket_plans(rows(), &list_chain(&tree), 2).await;

        assert_eq!(at_limit["hours"]["buckets"].as_array().map(Vec::len), Some(5));
        assert_too_many_buckets(past_limit, 2).await;
        // The bucket rows are capped at one past the limit: 3 of the 5 are collected.
        assert_eq!(capped_rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
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
