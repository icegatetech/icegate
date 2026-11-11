//! `top_hits`: the first rows of each bucket in a requested order, rendered as the `_search` hits
//! of the one index they come from.
//!
//! Each `top_hits` runs one branch after assembly, once the buckets the response renders are
//! known. The branch reads the matching rows again, restores the table columns
//! `plan_aggregation_rows` carries for it under their own names, adds the keys of the enclosing
//! buckets and unnests every membership list, so a row of two `filters` buckets is a candidate
//! hit of both. It keeps the rows of the non-empty buckets the response renders (a semi-join
//! against their key paths), numbers them by `row_number()` over those keys in `sort` order and
//! cuts at `size`: a bucket that `size` or `min_doc_count` dropped costs the window nothing.
//! Before any branch runs, the hits the request can render (each `top_hits`'s `size` times its
//! non-empty buckets, summed over the request) are bounded by `search.max_result_window`, the
//! window a `_search` page is bounded by. The kept rows render through the `_search` hits renderer
//! on the blocking pool, and each bucket takes the hits whose key path is its own.

use std::collections::HashMap;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, DFSchema, NullEquality, ScalarValue};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{
    Expr, ExprFunctionExt, JoinType, LogicalPlan, LogicalPlanBuilder, SortExpr, ident, lit,
};
use futures::future::try_join_all;
use serde_json::{Map, Value, json};

use super::assemble::{AssembledAggregations, BucketKey, extend_path, list_key_columns, read_path};
use super::parse::{IGNORED_EXECUTION_KEYS, expect_count, expect_object, report_unsupported_key};
use super::plan::{
    AggregationLayout, AggregationScope, Granularity, HIT_COLUMN_PREFIX, PlanInputs, TopHitsSlot,
    list_granularity_levels, list_membership_chain, name_hit_column, name_key_column, unnest_memberships,
};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::execute::{HitSortKey, convert_plan_error, lower_hit_sort, render_index_rows_as_hits};
use crate::opensearch::search::hits::choose_hit_score;
use crate::opensearch::search::request::{SearchParameters, SearchRequest, SortClause, parse_search_object};

/// The request type name of `top_hits`.
const TOP_HITS: &str = "top_hits";

/// Hits per bucket when the request names no `size`.
const DEFAULT_TOP_HITS_SIZE: usize = 3;

/// The largest accepted `size`; a larger one is refused, never truncated.
const MAX_TOP_HITS_SIZE: u64 = 100;

/// The members a `top_hits` reads; every one but `size` is read by the `_search` body parser.
/// The keys of [`IGNORED_EXECUTION_KEYS`] are accepted besides them and ignored.
const TOP_HITS_KEYS: [&str; 4] = ["size", "sort", "_source", "docvalue_fields"];

/// The per-bucket rank column of a branch; never rendered.
const TOP_HITS_RANK: &str = "__top_hits_rank";

/// A `top_hits` aggregation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TopHitsSpec {
    /// The hits every bucket renders, as a `_search` body states them: `size`, `sort`, `_source`
    /// and `docvalue_fields`; every other member keeps its `_search` default.
    pub(crate) request: SearchRequest,
}

/// The hits of one `top_hits`, by the key path of their bucket (empty at the top level), each
/// bucket's hits in rank order.
pub(super) type TopHitsByBucket = HashMap<Vec<BucketKey>, Vec<Value>>;

/// Parse the `top_hits` aggregation `name`: `size`, `sort`, `_source`, `docvalue_fields`; the
/// keys of [`IGNORED_EXECUTION_KEYS`] are accepted and ignored.
///
/// # Errors
///
/// `400 parsing_exception` for a body that is not an object, an unknown member, and a `size`
/// that is not a JSON integer from 0 to the limit; the `_search` body parser's errors for
/// `sort`, `_source` and `docvalue_fields`; `400 illegal_argument_exception` naming the
/// aggregation under `aggregation` for a `_doc` sort, an order decided by row hashes outside the
/// query plan.
pub(super) fn parse_top_hits(name: &str, value: &Value) -> Result<TopHitsSpec, OpenSearchError> {
    let body = expect_object(name, TOP_HITS, value)?;
    if let Some(key) = body
        .keys()
        .find(|key| !TOP_HITS_KEYS.contains(&key.as_str()) && !IGNORED_EXECUTION_KEYS.contains(&key.as_str()))
    {
        return Err(report_unsupported_key(TOP_HITS, name, key));
    }
    let size = body
        .get("size")
        .map(|size| {
            let count = expect_count(name, "size", size)?;
            usize::try_from(count)
                .ok()
                .filter(|_| count <= MAX_TOP_HITS_SIZE)
                .ok_or_else(|| {
                    OpenSearchError::parsing(format!(
                        "[size] of [{name}] must be at most {MAX_TOP_HITS_SIZE}, got [{count}]"
                    ))
                })
        })
        .transpose()?
        .unwrap_or(DEFAULT_TOP_HITS_SIZE);
    // `size` is bounded here and the execution keys are ignored; the other members read exactly
    // as `_search` reads them.
    let members: Map<String, Value> = body
        .iter()
        .filter(|(key, _)| key.as_str() != "size" && !IGNORED_EXECUTION_KEYS.contains(&key.as_str()))
        .map(|(key, member)| (key.clone(), member.clone()))
        .collect();
    let mut request = parse_search_object(members, &SearchParameters::default())?;
    if request.sort.iter().any(|clause| matches!(clause, SortClause::Doc { .. })) {
        return Err(OpenSearchError::unsupported_aggregation(
            name,
            format!("[{TOP_HITS}] [{name}] cannot sort on [_doc]; sort on a field"),
        ));
    }
    request.size = size;
    Ok(TopHitsSpec { request })
}

/// The hits of every `top_hits` of one request, by the aggregation's index in the layout.
#[derive(Debug, Default)]
pub(super) struct TopHitsResults {
    /// The hits of each `top_hits` that ran a branch, by its index.
    by_top_hits: HashMap<usize, TopHitsByBucket>,
}

impl TopHitsResults {
    /// Store the hits of the `top_hits` at index `top_hits_index`.
    pub(super) fn insert(&mut self, top_hits_index: usize, hits: TopHitsByBucket) {
        self.by_top_hits.insert(top_hits_index, hits);
    }

    /// Move out the hits of the bucket at `path` of the `top_hits` at index `top_hits_index`; a
    /// bucket the branch never saw gets none. Each bucket renders once, so moving avoids copying
    /// every hit.
    pub(super) fn take_bucket_hits(&mut self, top_hits_index: usize, path: &[BucketKey]) -> Vec<Value> {
        self.by_top_hits
            .get_mut(&top_hits_index)
            .and_then(|buckets| buckets.remove(path))
            .unwrap_or_default()
    }
}

/// Number the rows of `relation` per bucket (`partition`, the bucket key columns) in `order`,
/// keep the first `size` rows of each bucket, and emit them in rank order.
///
/// # Errors
///
/// `500` when DataFusion refuses the window, the filter or the sort.
pub(super) fn rank_bucket_rows(
    relation: LogicalPlanBuilder,
    partition: Vec<Expr>,
    order: Vec<SortExpr>,
    size: usize,
) -> Result<LogicalPlanBuilder, OpenSearchError> {
    let rank = row_number()
        .partition_by(partition)
        .order_by(order)
        .build()
        .map_err(convert_plan_error)?
        .alias(TOP_HITS_RANK);
    let limit = u64::try_from(size).unwrap_or(u64::MAX);
    relation
        .window(vec![rank])
        .and_then(|ranked_rows| ranked_rows.filter(ident(TOP_HITS_RANK).lt_eq(lit(limit))))
        .and_then(|kept_rows| kept_rows.sort(vec![ident(TOP_HITS_RANK).sort(true, false)]))
        .map_err(convert_plan_error)
}

/// What the `top_hits` branches of one request read besides their own aggregation.
struct HitsContext<'c, 'l> {
    /// The request's layout.
    layout: &'c AggregationLayout<'l>,
    /// The level keys of the layout.
    inputs: &'c PlanInputs,
    /// The buckets the response renders.
    assembled: &'c AssembledAggregations,
    /// The searched index's table columns, in scan order, by their own names.
    table_columns: Vec<String>,
}

/// Run the branch of every `top_hits` of `layout` over `rows` (the matching rows, which carry the
/// searched index's table columns under [`name_hit_column`] whenever the tree holds a
/// `top_hits`), keep the buckets `assembled` renders, and render each kept row as `_search`
/// renders it, on the blocking pool. A `size: 0` and a search of no index read nothing.
///
/// Before any branch runs, the hits the request can render are bounded: each `top_hits` renders up
/// to its `size` hits in every non-empty bucket the response answers (the one top-level bucket
/// when it has a matching row), and the hits of every `top_hits` together stay within
/// `scope.max_result_window`, the window a `_search` page is bounded by.
///
/// # Errors
///
/// [`OpenSearchError::top_hits_window_exceeded`] naming the first `top_hits`, in preorder, whose
/// hits pass the window; the sort and `fields` errors of `_search`; the branch's planning and
/// execution errors, the session's memory limit among them; `500` for more than one searched
/// index, which `plan_aggregation_rows` refuses first, and for a branch whose rows and hits
/// disagree.
pub(super) async fn collect_top_hits(
    rows: &LogicalPlan,
    layout: &AggregationLayout<'_>,
    inputs: &PlanInputs,
    assembled: &AssembledAggregations,
    scope: &AggregationScope<'_>,
) -> Result<TopHitsResults, OpenSearchError> {
    let mut results = TopHitsResults::default();
    let entry = match scope.entries {
        [] => return Ok(results),
        [entry] => *entry,
        [_, _, ..] => {
            return Err(OpenSearchError::internal(
                "a top_hits reads one index, and the rows plan reads several",
            ));
        }
    };
    check_top_hits_window(layout, assembled, scope.max_result_window)?;
    let context = HitsContext {
        layout,
        inputs,
        assembled,
        table_columns: list_hit_columns(rows.schema()),
    };
    let mut branches = Vec::with_capacity(layout.top_hits.len());
    for (top_hits_index, slot) in layout.top_hits.iter().enumerate() {
        if slot.spec.request.size == 0 {
            continue;
        }
        let sort_keys = lower_hit_sort(scope.session, entry, &slot.spec.request.sort)?;
        if let Some(plan) = plan_top_hits_branch(rows, &context, slot, sort_keys)? {
            branches.push((top_hits_index, slot, plan));
        }
    }
    // Each branch reads the matching rows again on its own, so the branches run concurrently.
    let session = scope.session;
    let branch_outputs = try_join_all(branches.into_iter().map(|(top_hits_index, slot, plan)| async move {
        Ok::<_, OpenSearchError>((top_hits_index, slot, session.collect(plan).await?))
    }))
    .await?;
    for (top_hits_index, slot, batches) in branch_outputs {
        let hits = render_index_rows_as_hits(
            scope.session,
            entry,
            &slot.spec.request,
            &batches,
            context.table_columns.len(),
        )
        .await?;
        let paths = read_hit_paths(layout, slot.granularity, &batches)?;
        if paths.len() != hits.len() {
            return Err(OpenSearchError::internal(format!(
                "the top_hits [{}] rendered {} hits from {} rows",
                slot.name,
                hits.len(),
                paths.len()
            )));
        }
        let mut by_bucket = TopHitsByBucket::new();
        for (path, hit) in paths.into_iter().zip(hits) {
            by_bucket.entry(path).or_default().push(hit);
        }
        results.insert(top_hits_index, by_bucket);
    }
    Ok(results)
}

/// Refuse the `top_hits` of `layout` when the hits they can render pass `max_result_window`: each
/// renders up to its `size` hits in every non-empty bucket `assembled` answers at its granularity,
/// and the hits of every `top_hits` count together.
///
/// # Errors
///
/// [`OpenSearchError::top_hits_window_exceeded`] naming the first `top_hits`, in preorder, whose
/// hits pass the window; `500` for a top-level document count the plans cannot have produced.
fn check_top_hits_window(
    layout: &AggregationLayout<'_>,
    assembled: &AssembledAggregations,
    max_result_window: usize,
) -> Result<(), OpenSearchError> {
    let mut hit_count: usize = 0;
    for slot in &layout.top_hits {
        let bucket_count = match slot.granularity {
            Granularity::Top => usize::from(assembled.create_reader().read_doc_count(assembled.top_row)? > 0),
            Granularity::Level(level) => count_surviving_buckets(assembled, level),
        };
        hit_count = hit_count.saturating_add(bucket_count.saturating_mul(slot.spec.request.size));
        if hit_count > max_result_window {
            return Err(OpenSearchError::top_hits_window_exceeded(slot.name, max_result_window));
        }
    }
    Ok(())
}

/// How many non-empty buckets the response renders at `level`: the buckets whose hits a `top_hits`
/// at that level lists ([`list_surviving_bucket_paths`]).
fn count_surviving_buckets(assembled: &AssembledAggregations, level: usize) -> usize {
    assembled.instances.get(level).map_or(0, |instances| {
        instances
            .values()
            .map(|instance| instance.buckets.iter().filter(|bucket| bucket.doc_count > 0).count())
            .sum()
    })
}

/// The table columns the rows plan carries for hits, in scan order, by their own names.
fn list_hit_columns(rows: &DFSchema) -> Vec<String> {
    rows.fields()
        .iter()
        .filter_map(|field| field.name().strip_prefix(HIT_COLUMN_PREFIX).map(str::to_string))
        .collect()
}

/// The branch of the `top_hits` `slot` over `rows`: the table columns under their own names, the
/// sort values after them (where `render_index_rows_as_hits` reads them), the keys of the
/// slot's bucket path, a row per bucket it belongs to, only the buckets the response renders,
/// ranked and cut at `size`. `None` when the response renders no non-empty bucket of the slot's
/// level, so nothing runs.
///
/// # Errors
///
/// `500` when DataFusion refuses a projection, an unnest, the join or the window, or when a
/// surviving key does not convert to its column's type.
fn plan_top_hits_branch(
    rows: &LogicalPlan,
    context: &HitsContext<'_, '_>,
    slot: &TopHitsSlot<'_>,
    sort_keys: Vec<HitSortKey>,
) -> Result<Option<LogicalPlan>, OpenSearchError> {
    let levels = list_granularity_levels(context.layout, slot.granularity);
    let surviving_paths = match slot.granularity {
        Granularity::Top => None,
        Granularity::Level(level) => {
            let paths = list_surviving_bucket_paths(context.assembled, level);
            if paths.is_empty() {
                return Ok(None);
            }
            Some(paths)
        }
    };
    let mut restored_columns: Vec<Expr> = context
        .table_columns
        .iter()
        .map(|column| ident(name_hit_column(column)).alias(column.as_str()))
        .collect();
    restored_columns.extend(
        levels
            .iter()
            .map(|level| context.inputs.level_keys[*level].expr.clone().alias(name_key_column(*level))),
    );
    // The sort values read the restored columns, so they are computed one projection later.
    let mut projection: Vec<Expr> = context.table_columns.iter().map(|column| ident(column.as_str())).collect();
    let mut order = Vec::with_capacity(sort_keys.len());
    for (position, key) in sort_keys.into_iter().enumerate() {
        let name = name_sort_value_column(position);
        order.push(ident(name.as_str()).sort(key.is_ascending, false));
        projection.push(key.value.alias(name));
    }
    projection.extend(levels.iter().map(|level| ident(name_key_column(*level))));
    let projected_rows = LogicalPlanBuilder::from(rows.clone())
        .project(restored_columns)
        .and_then(|restored_rows| restored_rows.project(projection))
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)?;
    let chain = list_membership_chain(context.layout, context.inputs, slot.granularity);
    let relation = unnest_memberships(&projected_rows, &chain)?;
    let relation = match surviving_paths {
        Some(paths) => keep_surviving_buckets(relation, levels, &paths)?,
        None => relation,
    };
    let partition = levels.iter().map(|level| ident(name_key_column(*level))).collect();
    rank_bucket_rows(relation, partition, order, slot.spec.request.size)?
        .build()
        .map(Some)
        .map_err(convert_plan_error)
}

/// The column of the sort value at `position` in a branch; never rendered.
fn name_sort_value_column(position: usize) -> String {
    format!("__top_hits_sort_{position}")
}

/// The column of a surviving bucket's key at `level` on the semi-join's right side; never
/// rendered.
fn name_surviving_key_column(level: usize) -> String {
    format!("__surviving_key_{level}")
}

/// The key paths, outermost first, of every non-empty bucket the response renders at `level`.
fn list_surviving_bucket_paths(assembled: &AssembledAggregations, level: usize) -> Vec<Vec<BucketKey>> {
    assembled
        .instances
        .get(level)
        .into_iter()
        .flat_map(|instances| {
            instances.iter().flat_map(|(parent_path, instance)| {
                instance
                    .buckets
                    .iter()
                    .filter(|bucket| bucket.doc_count > 0)
                    .map(move |bucket| extend_path(parent_path, &bucket.key))
            })
        })
        .collect()
}

/// Keep the rows of `relation` whose keys at `levels` equal the key path of one of
/// `surviving_paths`: a semi-join against the paths as a `VALUES` relation, every key converted
/// to its column's type. A surviving bucket's key is never NULL, so a NULL key matches nothing.
///
/// # Errors
///
/// `500` when a key does not convert or DataFusion refuses the join.
fn keep_surviving_buckets(
    relation: LogicalPlanBuilder,
    levels: &[usize],
    surviving_paths: &[Vec<BucketKey>],
) -> Result<LogicalPlanBuilder, OpenSearchError> {
    let key_types = levels
        .iter()
        .map(|level| {
            relation
                .schema()
                .field_with_unqualified_name(&name_key_column(*level))
                .map(|field| field.data_type().clone())
                .map_err(convert_plan_error)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let survivor_rows = surviving_paths
        .iter()
        .map(|keys| {
            keys.iter()
                .zip(&key_types)
                .map(|(key, data_type)| convert_bucket_key(key, data_type).map(lit))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let values = LogicalPlanBuilder::values(survivor_rows).map_err(convert_plan_error)?;
    // `VALUES` names its columns itself; the projection names them apart from the rows' own key
    // columns, so the join tells the two sides apart.
    let survivor_names: Vec<String> = levels.iter().map(|level| name_surviving_key_column(*level)).collect();
    let renamed_columns: Vec<Expr> = values
        .schema()
        .columns()
        .into_iter()
        .zip(&survivor_names)
        .map(|(column, name)| Expr::Column(column).alias(name.as_str()))
        .collect();
    let survivors = values
        .project(renamed_columns)
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)?;
    let row_keys: Vec<Column> = levels.iter().map(|level| Column::from_name(name_key_column(*level))).collect();
    let survivor_keys: Vec<Column> = survivor_names.iter().map(Column::from_name).collect();
    relation
        .join_detailed(
            survivors,
            JoinType::LeftSemi,
            (row_keys, survivor_keys),
            None,
            NullEquality::NullEqualsNothing,
        )
        .map_err(convert_plan_error)
}

/// `key` as a value of its key column's type `data_type`.
///
/// # Errors
///
/// `500` for a key that does not convert to `data_type`, which the planner's key columns never
/// produce.
fn convert_bucket_key(key: &BucketKey, data_type: &DataType) -> Result<ScalarValue, OpenSearchError> {
    let value = match key {
        BucketKey::Keyword(text) => ScalarValue::Utf8(Some(text.clone())),
        BucketKey::Long(number) | BucketKey::Date(number) => ScalarValue::Int64(Some(*number)),
        BucketKey::Double(number) => ScalarValue::Float64(Some(*number)),
        BucketKey::Boolean(flag) => ScalarValue::Boolean(Some(*flag)),
        BucketKey::HexId(bytes) => {
            let width = i32::try_from(bytes.len())
                .map_err(|_| OpenSearchError::internal("a bucket id is wider than an Arrow fixed-size binary"))?;
            ScalarValue::FixedSizeBinary(width, Some(bytes.clone()))
        }
    };
    value.cast_to(data_type).map_err(convert_plan_error)
}

/// The bucket key path of every row of `batches`, in batch then row order: the keys of the
/// levels on `granularity`'s path, empty at the top.
///
/// # Errors
///
/// `500` for a missing key column or a NULL key, which the semi-join never keeps.
fn read_hit_paths(
    layout: &AggregationLayout<'_>,
    granularity: Granularity,
    batches: &[RecordBatch],
) -> Result<Vec<Vec<BucketKey>>, OpenSearchError> {
    let levels = list_granularity_levels(layout, granularity);
    let mut paths = Vec::with_capacity(batches.iter().map(RecordBatch::num_rows).sum());
    for batch in batches {
        let key_columns = list_key_columns(layout, batch);
        for row in 0..batch.num_rows() {
            let path = read_path(layout, levels, &key_columns, row)?
                .ok_or_else(|| OpenSearchError::internal("a top_hits row has a NULL bucket key"))?;
            paths.push(path);
        }
    }
    Ok(paths)
}

/// The result of a `top_hits` for a bucket of `doc_count` rows: `hits.total` is that count, and
/// `max_score` the score [`choose_hit_score`] gives every hit (`null` without a hit).
pub(super) fn render_top_hits(spec: &TopHitsSpec, hits: Vec<Value>, doc_count: u64) -> Value {
    let max_score = if hits.is_empty() {
        Value::Null
    } else {
        choose_hit_score(&spec.request)
    };
    // Built member by member so the hits move into the answer instead of being serialized again.
    let mut hits_object = Map::new();
    hits_object.insert("total".to_string(), json!({"value": doc_count, "relation": "eq"}));
    hits_object.insert("max_score".to_string(), max_score);
    hits_object.insert("hits".to_string(), Value::Array(hits));
    let mut answer = Map::new();
    answer.insert("hits".to_string(), Value::Object(hits_object));
    Value::Object(answer)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{AsArray, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit, UInt64Type};
    use datafusion::datasource::{MemTable, provider_as_source};
    use datafusion::prelude::SessionContext;
    use serde_json::json;

    use super::*;
    use crate::opensearch::error::render_envelope;
    use crate::opensearch::search::hits::DateRenderFormat;
    use crate::opensearch::search::request::{DocValueField, SortOrder, SourceFilter};

    #[test]
    fn top_hits_parse_size_sort_source_and_docvalue_fields() {
        let spec = parse_top_hits(
            "1",
            &json!({
                "size": 2,
                "sort": [{"duration_micros": {"order": "desc"}}],
                "_source": {"includes": ["duration_micros"]},
                "docvalue_fields": [{"field": "timestamp", "format": "strict_date_time"}]
            }),
        )
        .expect("a full top_hits body");
        assert_eq!(spec.request.size, 2);
        assert_eq!(
            spec.request.sort,
            vec![SortClause::Field {
                field: "duration_micros".to_string(),
                order: SortOrder::Desc,
                unmapped_type: None,
                numeric_type: None,
            }]
        );
        assert_eq!(
            spec.request.source,
            SourceFilter::Paths {
                includes: vec!["duration_micros".to_string()],
                excludes: Vec::new(),
            }
        );
        assert_eq!(
            spec.request.docvalue_fields,
            vec![DocValueField {
                field: "timestamp".to_string(),
                format: Some(DateRenderFormat::StrictDateTime),
            }]
        );
        let default_spec = parse_top_hits("1", &json!({})).expect("every key is optional");
        assert_eq!(default_spec.request.size, 3);
        assert!(default_spec.request.sort.is_empty());
        assert_eq!(default_spec.request.source, SourceFilter::All);
        assert_eq!(
            parse_top_hits("1", &json!({"size": 100})).expect("at the limit").request.size,
            100
        );
        assert_eq!(
            parse_top_hits("1", &json!({"size": 0})).expect("no hits").request.size,
            0
        );
        // The shard-execution keys every aggregation accepts change nothing.
        assert_eq!(
            parse_top_hits(
                "1",
                &json!({"execution_hint": "map", "shard_size": 10, "collect_mode": "breadth_first", "size": 1})
            )
            .expect("ignored execution keys"),
            parse_top_hits("1", &json!({"size": 1})).expect("a size")
        );
    }

    #[tokio::test]
    async fn a_top_hits_size_above_the_limit_or_not_a_whole_number_is_a_parsing_exception() {
        for body in [
            json!({"size": 101}),
            json!({"size": -1}),
            json!({"size": 2.5}),
            json!({"size": "2"}),
        ] {
            let error = parse_top_hits("1", &body).expect_err("refused");
            let (_, envelope) = render_envelope(error).await;
            assert_eq!(envelope["error"]["type"], json!("parsing_exception"), "{body}");
        }
    }

    #[tokio::test]
    async fn a_top_hits_with_an_unknown_member_or_a_non_object_body_is_a_parsing_exception() {
        // `from` and `query` are `_search` members a `top_hits` does not read.
        for body in [json!({"from": 1}), json!({"query": {"match_all": {}}}), json!(["size"])] {
            let error = parse_top_hits("1", &body).expect_err("refused");
            let (_, envelope) = render_envelope(error).await;
            assert_eq!(envelope["error"]["type"], json!("parsing_exception"), "{body}");
        }
    }

    #[tokio::test]
    async fn a_top_hits_sorted_on_doc_is_an_illegal_argument_naming_it() {
        let error = parse_top_hits("latest", &json!({"sort": ["_doc"]})).expect_err("refused");
        let (_, envelope) = render_envelope(error).await;
        assert_eq!(envelope["error"]["type"], json!("illegal_argument_exception"));
        assert_eq!(envelope["error"]["aggregation"], json!("latest"));
    }

    #[tokio::test]
    async fn rank_bucket_rows_keeps_the_first_size_rows_of_each_bucket_in_sort_order() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket_key", DataType::Utf8, false),
            Field::new("duration_micros", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "cart", "checkout", "cart", "checkout", "checkout",
                ])),
                Arc::new(Int64Array::from(vec![200, 100, 400, 500, 300])),
            ],
        )
        .expect("the batch matches the schema");
        let table = MemTable::try_new(schema, vec![vec![batch]]).expect("a memory table");
        let rows = LogicalPlanBuilder::scan("rows", provider_as_source(Arc::new(table)), None).expect("a scan");
        let plan = rank_bucket_rows(
            rows,
            vec![ident("bucket_key")],
            vec![ident("duration_micros").sort(false, false)],
            2,
        )
        .and_then(|ranked_rows| ranked_rows.build().map_err(convert_plan_error))
        .expect("the window plans");
        let batches = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .expect("the plan executes")
            .collect()
            .await
            .expect("the plan runs");
        let mut kept_rows: Vec<(String, u64, i64)> = Vec::new();
        for batch in &batches {
            let keys = batch.column_by_name("bucket_key").expect("the key column").as_string::<i32>();
            let ranks = batch
                .column_by_name(TOP_HITS_RANK)
                .expect("the rank column")
                .as_primitive::<UInt64Type>();
            let durations = batch
                .column_by_name("duration_micros")
                .expect("the value column")
                .as_primitive::<Int64Type>();
            for row in 0..batch.num_rows() {
                kept_rows.push((keys.value(row).to_string(), ranks.value(row), durations.value(row)));
            }
        }
        // The rows come out in rank order, whatever their buckets.
        assert!(kept_rows.windows(2).all(|pair| pair[0].1 <= pair[1].1), "{kept_rows:?}");
        kept_rows.sort();
        assert_eq!(
            kept_rows,
            vec![
                ("cart".to_string(), 1, 400),
                ("cart".to_string(), 2, 200),
                ("checkout".to_string(), 1, 500),
                ("checkout".to_string(), 2, 300),
            ]
        );
    }

    #[test]
    fn bucket_keys_convert_to_the_values_of_their_key_columns() {
        // Each pair is a key form and a key column type the planner produces for it: a keyword
        // `terms` key, a membership ordinal, a `histogram` index or `long` key, a double, a
        // boolean, an encoded id, a `date_histogram` start and a date `terms` key.
        let cases = [
            (
                BucketKey::Keyword("cart".to_string()),
                DataType::Utf8,
                ScalarValue::Utf8(Some("cart".to_string())),
            ),
            (BucketKey::Long(2), DataType::Int32, ScalarValue::Int32(Some(2))),
            (BucketKey::Long(7), DataType::Int64, ScalarValue::Int64(Some(7))),
            (
                BucketKey::Double(1.5),
                DataType::Float64,
                ScalarValue::Float64(Some(1.5)),
            ),
            (
                BucketKey::Boolean(true),
                DataType::Boolean,
                ScalarValue::Boolean(Some(true)),
            ),
            (
                BucketKey::HexId(vec![0xab; 8]),
                DataType::FixedSizeBinary(8),
                ScalarValue::FixedSizeBinary(8, Some(vec![0xab; 8])),
            ),
            (
                BucketKey::Date(1_791_453_600_000_000),
                DataType::Int64,
                ScalarValue::Int64(Some(1_791_453_600_000_000)),
            ),
            (
                BucketKey::Date(1_791_453_600_000_000),
                DataType::Timestamp(TimeUnit::Microsecond, None),
                ScalarValue::TimestampMicrosecond(Some(1_791_453_600_000_000), None),
            ),
        ];
        for (key, data_type, expected) in cases {
            assert_eq!(
                convert_bucket_key(&key, &data_type).expect("the key converts"),
                expected,
                "{key:?}"
            );
        }
    }

    #[test]
    fn top_hits_results_hand_out_each_buckets_hits_once() {
        let mut results = TopHitsResults::default();
        let bucket = vec![BucketKey::Keyword("cart".to_string())];
        results.insert(3, TopHitsByBucket::from([(bucket.clone(), vec![json!({"_id": "a"})])]));
        assert_eq!(results.take_bucket_hits(3, &bucket), vec![json!({"_id": "a"})]);
        assert_eq!(results.take_bucket_hits(3, &bucket), Vec::<Value>::new());
        assert_eq!(results.take_bucket_hits(4, &bucket), Vec::<Value>::new());
    }

    #[test]
    fn top_hits_render_the_bucket_total_and_a_max_score_only_for_a_score_order() {
        let sorted_spec =
            parse_top_hits("1", &json!({"sort": [{"duration_micros": "desc"}]})).expect("a sorted top_hits");
        assert_eq!(
            render_top_hits(&sorted_spec, vec![json!({"_id": "a"})], 3),
            json!({"hits": {"total": {"value": 3, "relation": "eq"}, "max_score": null, "hits": [{"_id": "a"}]}})
        );
        let unsorted_spec = parse_top_hits("1", &json!({})).expect("an unsorted top_hits");
        assert_eq!(
            render_top_hits(&unsorted_spec, vec![json!({"_id": "b"})], 1),
            json!({"hits": {"total": {"value": 1, "relation": "eq"}, "max_score": 1.0, "hits": [{"_id": "b"}]}})
        );
        assert_eq!(
            render_top_hits(&unsorted_spec, Vec::new(), 0),
            json!({"hits": {"total": {"value": 0, "relation": "eq"}, "max_score": null, "hits": []}})
        );
    }
}
