//! Exact distinct counts in two grouping levels.
//!
//! No `count(DISTINCT)` aggregate is planned. Two plain aggregates count each distinct value
//! once: an inner `GROUP BY (keys, x)` keeps one row per distinct value of every group, and an
//! outer `GROUP BY keys` counts those rows. Each counted expression is a branch of its own; the
//! branches are joined on the keys, NULL keys equal, so every group of the input appears once.

use datafusion::common::{Column, NullEquality};
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{Expr, JoinType, LogicalPlan, LogicalPlanBuilder};

use crate::opensearch::error::OpenSearchError;
use crate::opensearch::search::execute::convert_plan_error;

/// Column of the distinct value inside a branch.
const DISTINCT_VALUE_COLUMN: &str = "__distinct_value";
/// Relation name of the left side of a branch join.
const LEFT_BRANCH: &str = "__distinct_left";
/// Relation name of the right side of a branch join.
const RIGHT_BRANCH: &str = "__distinct_right";

/// The column of the exact distinct count of `counted_expressions[position]` in the output of
/// [`plan_exact_distinct`].
pub(crate) fn name_distinct_count_column(position: usize) -> String {
    format!("__distinct_count_{position}")
}

/// Plan the exact number of distinct non-NULL values of each of `counted_expressions` per group
/// of `keys` over `input`.
///
/// The output has one row per group of `keys`, a group of NULL keys included, or one row in all
/// when `keys` is empty: the key columns under their output names, then one `Int64` count per
/// counted expression, named by [`name_distinct_count_column`]. No `count(DISTINCT)` aggregate
/// is planned.
///
/// # Errors
///
/// `500` when `counted_expressions` is empty or a plan fails to type-check.
pub(crate) fn plan_exact_distinct(
    input: LogicalPlanBuilder,
    keys: &[Expr],
    counted_expressions: &[Expr],
) -> Result<LogicalPlanBuilder, OpenSearchError> {
    let input = input.build().map_err(convert_plan_error)?;
    let mut branches = counted_expressions
        .iter()
        .enumerate()
        .map(|(position, counted_expression)| plan_distinct_branch(&input, keys, counted_expression, position));
    let first_branch = branches
        .next()
        .ok_or_else(|| OpenSearchError::internal("an exact distinct count needs an expression to count"))??;
    branches.try_fold(first_branch, |joined_branches, branch| {
        join_branches(joined_branches, branch?, keys.len())
    })
}

/// `GROUP BY (keys, counted_expression)`, then `GROUP BY keys` with
/// `count(counted_expression)`: the count of the counted expression at `position`.
fn plan_distinct_branch(
    input: &LogicalPlan,
    keys: &[Expr],
    counted_expression: &Expr,
    position: usize,
) -> Result<LogicalPlanBuilder, OpenSearchError> {
    let mut group_expressions = keys.to_vec();
    group_expressions.push(counted_expression.clone().alias(DISTINCT_VALUE_COLUMN));
    let inner_plan = LogicalPlanBuilder::from(input.clone())
        .aggregate(group_expressions, Vec::<Expr>::new())
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)?;
    // The inner output is referred to by position: a key's output name may carry a qualifier
    // that a column parsed from its name would lose.
    let outer_keys: Vec<Expr> = (0..keys.len())
        .map(|index| Expr::Column(Column::from(inner_plan.schema().qualified_field(index))))
        .collect();
    let distinct_value = Expr::Column(Column::from(inner_plan.schema().qualified_field(keys.len())));
    LogicalPlanBuilder::from(inner_plan)
        .aggregate(
            outer_keys,
            vec![count(distinct_value).alias(name_distinct_count_column(position))],
        )
        .map_err(convert_plan_error)
}

/// Join two branches on their leading `key_count` columns, keeping the left keys and every
/// count.
fn join_branches(
    left: LogicalPlanBuilder,
    right: LogicalPlanBuilder,
    key_count: usize,
) -> Result<LogicalPlanBuilder, OpenSearchError> {
    let left = left
        .alias(LEFT_BRANCH)
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)?;
    let right = right
        .alias(RIGHT_BRANCH)
        .and_then(LogicalPlanBuilder::build)
        .map_err(convert_plan_error)?;
    let kept_columns: Vec<Expr> = left
        .schema()
        .fields()
        .iter()
        .map(|field| Expr::Column(Column::new(Some(LEFT_BRANCH), field.name())))
        .chain(
            right
                .schema()
                .fields()
                .iter()
                .skip(key_count)
                .map(|field| Expr::Column(Column::new(Some(RIGHT_BRANCH), field.name()))),
        )
        .collect();
    let joined_branches = if key_count == 0 {
        LogicalPlanBuilder::from(left).cross_join(right)
    } else {
        let (left_keys, right_keys): (Vec<Column>, Vec<Column>) = left
            .schema()
            .fields()
            .iter()
            .take(key_count)
            .map(|field| {
                (
                    Column::new(Some(LEFT_BRANCH), field.name()),
                    Column::new(Some(RIGHT_BRANCH), field.name()),
                )
            })
            .unzip();
        LogicalPlanBuilder::from(left).join_detailed(
            right,
            JoinType::Inner,
            (left_keys, right_keys),
            None,
            NullEquality::NullEqualsNull,
        )
    }
    .map_err(convert_plan_error)?;
    joined_branches.project(kept_columns).map_err(convert_plan_error)
}
