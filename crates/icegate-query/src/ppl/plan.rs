//! The PPL planner: lowers a [`PplQuery`] to a DataFusion [`LogicalPlan`] over the tenant's
//! tables.
//!
//! Sources resolve through the tenant's [`TenantSession`], so the tenant scope, index
//! expressions and the case-sensitive name rules are those of every other front end. Each
//! command then transforms the plan of the commands before it; a field reference resolves
//! against the columns of the plan at that point.

use std::collections::HashSet;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{DECIMAL128_MAX_PRECISION, DataType, Field, FieldRef, Schema, TimeUnit};
use datafusion::common::format::DEFAULT_CAST_OPTIONS;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, DFSchemaRef, NullEquality, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::functions::expr_fn::{coalesce, date_part, floor, make_date, regexp_replace, to_char};
use datafusion::functions_aggregate::expr_fn::{array_agg, avg, count, max, min, sum};
use datafusion::functions_nested::expr_fn::array_element;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::expr::{Case, Lambda, LambdaVariable};
use datafusion::logical_expr::{
    Between, EmptyRelation, Expr, ExprFunctionExt, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
    SortExpr, binary_expr, cast, col, ident, lit, not, when,
};
use datafusion::optimizer::Analyzer;
use icegate_common::schema::COL_TENANT_ID;
use regex::Regex;

use super::ast::{
    BinCommand, BinSpan, CompareOperator, DedupCommand, FillnullCommand, FillnullReplacements, ParseCommand,
    PplCommand, PplExpr, PplGroupKey, PplLiteral, PplQuery, PplSortKey, PplSource, RankCommand, SpanUnit, StatsCommand,
    TimechartCommand,
};
use super::error::PplError;
use super::functions::{
    FunctionContext, PlannedAggregate, is_higher_order_function, lower_arithmetic, lower_cast, lower_function_call,
    match_string_literal, parse_timestamp_literal, plan_aggregate_call,
};
use crate::opensearch::catalog::{
    FieldListing, FieldType, FieldValue, IndexEntry, IndexExpression, IndexField, IndexOptions, IndexTarget, Profile,
    TenantSession,
};
use crate::opensearch::context::RequestContext;
use crate::opensearch::extract::ExpandWildcards;
use crate::opensearch::glob::matches_glob;
use crate::opensearch::search::aggs::plan_exact_distinct;
use crate::opensearch::search::doc_id::{
    MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MILLISECOND, MICROS_PER_MINUTE, MICROS_PER_SECOND, MICROS_PER_WEEK,
};

/// A planned PPL query: the plan and the jdbc schema of its output, column for column.
#[derive(Debug)]
pub(crate) struct PplPlan {
    /// The analyzed logical plan; executing it yields the output rows.
    pub(crate) plan: LogicalPlan,
    /// The output columns in order, with their PPL types.
    pub(crate) columns: Vec<PplColumn>,
}

/// One output column of a PPL query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PplColumn {
    /// The column name, as the query named it.
    pub(crate) name: String,
    /// The PPL type the jdbc schema reports for it.
    pub(crate) ppl_type: PplType,
}

/// The PPL type of an output column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PplType {
    /// Strings, and the hex rendering of fixed-size binary ids.
    String,
    /// 8-, 16- and 32-bit integers.
    Int,
    /// 64-bit integers.
    Bigint,
    /// Floating-point numbers.
    Double,
    /// Booleans.
    Boolean,
    /// Timestamps, rendered in UTC.
    Timestamp,
    /// Dates.
    Date,
    /// MAP and struct values, rendered as JSON objects.
    Struct,
    /// Lists, rendered as JSON arrays.
    Array,
}

impl PplType {
    /// The type name the jdbc `schema` reports.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int => "int",
            Self::Bigint => "bigint",
            Self::Double => "double",
            Self::Boolean => "boolean",
            Self::Timestamp => "timestamp",
            Self::Date => "date",
            Self::Struct => "struct",
            Self::Array => "array",
        }
    }

    /// The PPL type of an output column of Arrow type `data_type`.
    ///
    /// # Errors
    ///
    /// Returns [`PplError::Semantic`] naming `column` for a type PPL cannot return.
    fn from_data_type(column: &str, data_type: &DataType) -> Result<Self, PplError> {
        match data_type {
            DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::FixedSizeBinary(_)
            | DataType::Null => Ok(Self::String),
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => Ok(Self::Int),
            DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Ok(Self::Bigint),
            DataType::Float32 | DataType::Float64 => Ok(Self::Double),
            DataType::Boolean => Ok(Self::Boolean),
            DataType::Timestamp(_, _) => Ok(Self::Timestamp),
            DataType::Date32 => Ok(Self::Date),
            DataType::Map(_, _) | DataType::Struct(_) => Ok(Self::Struct),
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => Ok(Self::Array),
            other => Err(PplError::Semantic {
                reason: format!("column [{column}] has type {other}, which a PPL query cannot return"),
            }),
        }
    }
}

/// The plan under construction: the plan of the source and the commands applied so far.
struct Pipeline<'session> {
    builder: LogicalPlanBuilder,
    /// The source indices while the pipeline still carries their columns; a command that
    /// replaces the columns (an aggregation) ends it.
    source: Option<SourceBinding<'session>>,
    /// The keys of the last `sort`, over the current columns; empty when no `sort` came since the
    /// source or the last aggregation, where the order is the scan's. `dedup` keeps the first
    /// rows in this order. A `rename` of a sorted column moves its key to the new name; before a
    /// command removes or replaces a sorted column, the key's values are copied into a hidden
    /// order column (see [`hide_touched_sort_keys`]), so the order outlives the column.
    sort_keys: Vec<SortExpr>,
}

/// The indices a pipeline reads, for the field rules of their profiles (a promoted resource
/// attribute read through its column).
struct SourceBinding<'session> {
    session: &'session TenantSession,
    entries: Vec<&'session IndexEntry>,
    /// The column names an earlier `rename` moved or overwrote, or an `eval` or `bin` assigned:
    /// such a name no longer holds the source's column, so a promoted attribute must not read it.
    replaced_columns: HashSet<String>,
}

/// What an expression's field references resolve against.
struct ExpressionScope<'a> {
    /// The columns of the plan at the point of the expression.
    schema: &'a DFSchema,
    /// The source indices, while their columns are the pipeline's.
    source: Option<&'a SourceBinding<'a>>,
    /// The lambda parameters in scope, innermost last, with the element field they stand for.
    lambda_variables: Vec<(String, FieldRef)>,
    /// The request time.
    now: DateTime<Utc>,
}

impl<'a> ExpressionScope<'a> {
    const fn new(schema: &'a DFSchema, source: Option<&'a SourceBinding<'a>>, now: DateTime<Utc>) -> Self {
        Self {
            schema,
            source,
            lambda_variables: Vec::new(),
            now,
        }
    }

    const fn build_function_context(&self) -> FunctionContext<'a> {
        FunctionContext {
            schema: self.schema,
            now: self.now,
        }
    }
}

/// Plan `query` for the tenant of `session`.
///
/// The source resolves through the session's index catalog, case-sensitively, with the tenant
/// scope of every scan; `tenant_id` is never a column of the output, and decimal columns read as
/// doubles. Field references are case-sensitive. `where` follows SQL three-valued logic. A leaf
/// `fields` keeps only the listed columns, so the scan reads only those. `now()` and the other
/// request-time functions read `context.now`. `cluster_name` fills the `TABLE_CAT` column of
/// `describe`.
///
/// # Errors
///
/// Returns [`PplError::IndexNotFound`] for a source that names no index,
/// [`PplError::FieldNotFound`] for a reference to no column, [`PplError::UnsupportedFunction`]
/// for a function outside the library, and [`PplError::Semantic`] for a source on system
/// indices, a field typed differently across the source indices, or an expression whose types do
/// not fit.
pub(crate) async fn plan_ppl(
    query: &PplQuery,
    session: &TenantSession,
    context: &RequestContext,
    cluster_name: &str,
) -> Result<PplPlan, PplError> {
    let mut pipeline = match &query.source {
        PplSource::Index { expression } => plan_index_source(expression, session).await?,
        PplSource::Describe { index } => plan_describe(index, session, context.now, cluster_name).await?,
    };
    for command in &query.commands {
        pipeline = apply_command(pipeline, command, context.now)?;
    }
    finish_plan(pipeline.builder)
}

/// The rows of the data indices `expression` names.
///
/// Several indices are combined with UNION ALL over the union of their columns, in the order the
/// columns first appear; a column an index lacks is NULL in its rows. Column types are compared by
/// structure: the names and metadata of nested fields are each table's own (every table numbers
/// its Iceberg field ids independently), so they never make two types differ. A column typed
/// differently across the indices is refused, because no single PPL type could describe it.
async fn plan_index_source<'session>(
    expression: &str,
    session: &'session TenantSession,
) -> Result<Pipeline<'session>, PplError> {
    let entries = resolve_data_entries(expression, session).await?;
    let mut branches = Vec::with_capacity(entries.len());
    for entry in &entries {
        branches.push((entry.name(), scan_entry(session, entry)?));
    }
    Ok(Pipeline {
        builder: union_branches(branches)?,
        source: Some(SourceBinding {
            session,
            entries,
            replaced_columns: HashSet::new(),
        }),
        sort_keys: Vec::new(),
    })
}

/// The data indices `expression` names, resolved like every PPL source: open indices only, and an
/// expression that matches nothing is an error, never an empty source.
///
/// # Errors
///
/// `IndexNotFound` for a name or pattern that matches no index; `Semantic` when the expression
/// names system indices, which PPL never reads.
async fn resolve_data_entries<'session>(
    expression: &str,
    session: &'session TenantSession,
) -> Result<Vec<&'session IndexEntry>, PplError> {
    let index_expression = IndexExpression::parse(expression)?;
    let options = IndexOptions {
        ignore_unavailable: false,
        allow_no_indices: false,
        expand_wildcards: ExpandWildcards {
            is_open: true,
            is_hidden: false,
        },
    };
    match session.resolve_target(&index_expression, options).await? {
        IndexTarget::Data(entries) => Ok(entries),
        IndexTarget::System(names) => Err(PplError::Semantic {
            reason: format!("PPL reads data indices only; [{}] are system indices", names.join(", ")),
        }),
    }
}

/// The columns of one index (see [`project_source_columns`]).
fn scan_entry(session: &TenantSession, entry: &IndexEntry) -> Result<LogicalPlanBuilder, PplError> {
    // Every index is read through the native profile, which exposes the table's own columns; a
    // profile that renames or derives fields needs its own projection here.
    let Profile::Native = entry.profile();
    project_source_columns(session.scan_index(entry)?)
}

/// The columns of a scan without `tenant_id`, in the table's schema order. Decimal columns are
/// read as doubles, the one numeric type PPL reports for them. The columns keep the scan's
/// qualifier; field references are unqualified names, which resolve to them.
fn project_source_columns(scan: LogicalPlanBuilder) -> Result<LogicalPlanBuilder, PplError> {
    let columns: Vec<Expr> = scan
        .schema()
        .iter()
        .filter(|(_, field)| field.name() != COL_TENANT_ID)
        .map(|(qualifier, field)| {
            let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
            match field.data_type() {
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => cast(column, DataType::Float64).alias(field.name()),
                _ => column,
            }
        })
        .collect();
    Ok(scan.project(columns)?)
}

/// UNION ALL of `branches`, each projected explicitly onto the union of their columns. A column
/// takes the type of the first branch that has it, and every branch produces exactly that type,
/// nested field names and metadata included: a branch whose type differs only in those casts the
/// column to it, and a branch without the column yields a NULL of it.
fn union_branches(branches: Vec<(&str, LogicalPlanBuilder)>) -> Result<LogicalPlanBuilder, PplError> {
    if branches.len() == 1 {
        let mut branches = branches;
        return branches.pop().map(|(_, builder)| builder).ok_or_else(|| PplError::Internal {
            reason: "a PPL source without an index".to_string(),
        });
    }
    let mut columns: Vec<(String, DataType, &str)> = Vec::new();
    for (index, builder) in &branches {
        for field in builder.schema().fields() {
            match columns.iter().find(|(name, ..)| name == field.name()) {
                Some((name, data_type, first_index)) if !data_type.equals_datatype(field.data_type()) => {
                    return Err(PplError::Semantic {
                        reason: format!(
                            "field [{name}] has type {data_type} in [{first_index}] and {} in [{index}]",
                            field.data_type()
                        ),
                    });
                }
                Some(_) => {}
                None => columns.push((field.name().clone(), field.data_type().clone(), index)),
            }
        }
    }
    let mut union: Option<LogicalPlanBuilder> = None;
    for (_, builder) in branches {
        let projection = columns
            .iter()
            .map(
                |(name, data_type, _)| match builder.schema().field_with_unqualified_name(name) {
                    Ok(field) if field.data_type() == data_type => Ok(ident(name)),
                    Ok(_) => Ok(cast(ident(name), data_type.clone()).alias(name)),
                    Err(_) => Ok(lit(ScalarValue::try_from(data_type)?).alias(name)),
                },
            )
            .collect::<Result<Vec<_>, PplError>>()?;
        let branch = builder.project(projection)?.build()?;
        union = Some(match union {
            None => LogicalPlanBuilder::from(branch),
            Some(combined) => combined.union(branch)?,
        });
    }
    union.ok_or_else(|| PplError::Internal {
        reason: "a PPL source without an index".to_string(),
    })
}

fn apply_command<'session>(
    pipeline: Pipeline<'session>,
    command: &PplCommand,
    now: DateTime<Utc>,
) -> Result<Pipeline<'session>, PplError> {
    let Pipeline {
        builder,
        mut source,
        sort_keys,
    } = pipeline;
    let schema = Arc::clone(builder.schema());
    let build_scope = || ExpressionScope::new(&schema, source.as_ref(), now);
    let (builder, sort_keys) = match command {
        PplCommand::Where(predicate) => (builder.filter(lower_expression(predicate, &build_scope())?)?, sort_keys),
        PplCommand::Fields { is_exclusion, fields } => {
            let selected_rows = select_fields(builder.clone(), *is_exclusion, fields, &build_scope())?;
            let kept_names: HashSet<&str> = selected_rows
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            let is_removed = |column: &str| !kept_names.contains(column);
            if sort_keys.iter().any(|sort| reads_column(sort, is_removed)) {
                let (builder, sort_keys) = hide_touched_sort_keys(builder, sort_keys, is_removed)?;
                let hidden_schema = Arc::clone(builder.schema());
                let scope = ExpressionScope::new(&hidden_schema, source.as_ref(), now);
                (select_fields(builder, *is_exclusion, fields, &scope)?, sort_keys)
            } else {
                (selected_rows, sort_keys)
            }
        }
        PplCommand::Sort(keys) => {
            let sort_keys = lower_sort_keys(keys, &build_scope())?;
            (builder.sort(sort_keys.clone())?, sort_keys)
        }
        PplCommand::Head { count, offset } => (builder.limit(*offset, Some(*count))?, sort_keys),
        PplCommand::Eval(assignments) => {
            let is_assigned = |column: &str| assignments.iter().any(|(field, _)| field == column);
            let (builder, sort_keys) = hide_touched_sort_keys(builder, sort_keys, is_assigned)?;
            (evaluate_fields(builder, assignments, source.as_mut(), now)?, sort_keys)
        }
        PplCommand::Rename(renames) => {
            let renamed_columns = map_renamed_columns(&schema, renames)?;
            let is_overwritten =
                |column: &str| renamed_columns.iter().all(|(source_column, _)| source_column != column);
            let (builder, sort_keys) = hide_touched_sort_keys(builder, sort_keys, is_overwritten)?;
            if let Some(source) = source.as_mut() {
                source.replaced_columns.extend(
                    renames
                        .iter()
                        .filter(|(original_name, new_name)| original_name != new_name)
                        .flat_map(|(original_name, new_name)| [original_name.clone(), new_name.clone()]),
                );
            }
            let builder = rename_fields(builder, &renamed_columns)?;
            (builder, rename_sort_keys(sort_keys, &renamed_columns)?)
        }
        PplCommand::Dedup(dedup) => (deduplicate_rows(builder, dedup, &sort_keys, &build_scope())?, sort_keys),
        PplCommand::Bin(bin) => {
            let (builder, sort_keys) = hide_touched_sort_keys(builder, sort_keys, |column| column == bin.field)?;
            let hidden_schema = Arc::clone(builder.schema());
            let builder = bin_field(
                builder,
                bin,
                &ExpressionScope::new(&hidden_schema, source.as_ref(), now),
            )?;
            if let Some(source) = source.as_mut() {
                source.replaced_columns.insert(bin.field.clone());
            }
            (builder, sort_keys)
        }
        PplCommand::Stats(stats) => {
            return Ok(build_aggregated_pipeline(aggregate_rows(
                builder,
                stats,
                &build_scope(),
            )?));
        }
        PplCommand::Top(rank) => {
            return Ok(build_aggregated_pipeline(rank_values(
                builder,
                rank,
                true,
                &build_scope(),
            )?));
        }
        PplCommand::Rare(rank) => {
            return Ok(build_aggregated_pipeline(rank_values(
                builder,
                rank,
                false,
                &build_scope(),
            )?));
        }
        PplCommand::Timechart(chart) => {
            return Ok(build_aggregated_pipeline(chart_rows(builder, chart, &build_scope())?));
        }
        PplCommand::Parse(parse) => {
            let (builder, sort_keys, groups) =
                rewrite_columns(builder, sort_keys, source.as_ref(), now, |builder, scope| {
                    plan_parse(builder, parse, scope)
                })?;
            if let Some(source) = source.as_mut() {
                source.replaced_columns.extend(groups);
            }
            (builder, sort_keys)
        }
        PplCommand::Fillnull(fillnull) => {
            let (builder, sort_keys, filled) =
                rewrite_columns(builder, sort_keys, source.as_ref(), now, |builder, scope| {
                    plan_fillnull(builder, fillnull, scope)
                })?;
            if let Some(source) = source.as_mut() {
                source.replaced_columns.extend(filled);
            }
            (builder, sort_keys)
        }
    };
    Ok(Pipeline {
        builder,
        source,
        sort_keys,
    })
}

/// `builder` and `sort_keys` after a command that writes columns in place, and the names of the
/// columns it wrote. `plan_rewrite` plans the command over a scope and names the columns it wrote;
/// when a sort key reads one of them, the key is hidden first ([`hide_touched_sort_keys`]) and the
/// command is planned again over the columns with the hidden order column, so the order keeps the
/// values the rows were sorted by.
fn rewrite_columns(
    builder: LogicalPlanBuilder,
    sort_keys: Vec<SortExpr>,
    source: Option<&SourceBinding<'_>>,
    now: DateTime<Utc>,
    plan_rewrite: impl Fn(LogicalPlanBuilder, &ExpressionScope<'_>) -> Result<(LogicalPlanBuilder, Vec<String>), PplError>,
) -> Result<(LogicalPlanBuilder, Vec<SortExpr>, Vec<String>), PplError> {
    let schema = Arc::clone(builder.schema());
    let (rewritten_rows, written_columns) = plan_rewrite(builder.clone(), &ExpressionScope::new(&schema, source, now))?;
    let is_written = |column: &str| written_columns.iter().any(|written| written == column);
    if !sort_keys.iter().any(|sort| reads_column(sort, is_written)) {
        return Ok((rewritten_rows, sort_keys, written_columns));
    }
    let (builder, sort_keys) = hide_touched_sort_keys(builder, sort_keys, is_written)?;
    let hidden_schema = Arc::clone(builder.schema());
    let (rewritten_rows, written_columns) = plan_rewrite(builder, &ExpressionScope::new(&hidden_schema, source, now))?;
    Ok((rewritten_rows, sort_keys, written_columns))
}

/// The pipeline after a command that replaces the columns: no source columns remain and the
/// command defines its own row order.
const fn build_aggregated_pipeline<'session>(builder: LogicalPlanBuilder) -> Pipeline<'session> {
    Pipeline {
        builder,
        source: None,
        sort_keys: Vec::new(),
    }
}

/// The prefix of every relation and column name this planner adds beside the fields of a query.
/// The builder refuses a field name a query writes with this prefix, so no field of a query shares
/// a name with one of them.
pub(super) const RESERVED_NAME_PREFIX: &str = "__ppl_";

/// The prefix of the hidden order columns, within [`RESERVED_NAME_PREFIX`]. They hold the values
/// of sort keys whose columns a later command removed or replaced, travel with the rows through
/// every command up to an aggregation, and are never part of the output.
const ORDER_COLUMN_PREFIX: &str = "__ppl_order_";

/// Whether `name` is a hidden order column. The name alone tells, because no field of a query
/// starts with [`RESERVED_NAME_PREFIX`].
fn is_order_column(name: &str) -> bool {
    name.starts_with(ORDER_COLUMN_PREFIX)
}

/// Whether `sort` reads a column `is_touched` names.
fn reads_column(sort: &SortExpr, is_touched: impl Fn(&str) -> bool) -> bool {
    sort.expr.column_refs().iter().any(|column| is_touched(&column.name))
}

/// `builder` and `sort_keys` before a command that removes or replaces the columns
/// `is_touched` names: every sort key reading such a column is evaluated into a new hidden order
/// column and replaced by a key on it, so the order keeps the values the rows were sorted by. The
/// other keys and the builder are returned unchanged when no key reads such a column.
fn hide_touched_sort_keys(
    builder: LogicalPlanBuilder,
    sort_keys: Vec<SortExpr>,
    is_touched: impl Fn(&str) -> bool,
) -> Result<(LogicalPlanBuilder, Vec<SortExpr>), PplError> {
    if !sort_keys.iter().any(|sort| reads_column(sort, &is_touched)) {
        return Ok((builder, sort_keys));
    }
    let schema = Arc::clone(builder.schema());
    let mut projection: Vec<Expr> = schema.columns().into_iter().map(Expr::Column).collect();
    let mut next_index = 0;
    let mut name_free_order_column = || loop {
        let name = format!("{ORDER_COLUMN_PREFIX}{next_index}");
        next_index += 1;
        if schema.field_with_unqualified_name(&name).is_err() {
            return name;
        }
    };
    let hidden_sort_keys = sort_keys
        .into_iter()
        .map(|sort| {
            if !reads_column(&sort, &is_touched) {
                return sort;
            }
            let name = name_free_order_column();
            projection.push(sort.expr.alias(&name));
            SortExpr::new(ident(name), sort.asc, sort.nulls_first)
        })
        .collect();
    Ok((builder.project(projection)?, hidden_sort_keys))
}

/// `sort_keys` after a `rename` whose `renamed_columns` map each kept column to its output name:
/// a key's column references move to the new names. A sorted column the rename overwrites must
/// already be hidden ([`hide_touched_sort_keys`]).
fn rename_sort_keys(sort_keys: Vec<SortExpr>, renamed_columns: &[(String, String)]) -> Result<Vec<SortExpr>, PplError> {
    sort_keys
        .into_iter()
        .map(|SortExpr { expr, asc, nulls_first }| {
            let renamed_expression = expr
                .transform(|expression| {
                    let Expr::Column(column) = &expression else {
                        return Ok(Transformed::no(expression));
                    };
                    let new_name = renamed_columns
                        .iter()
                        .find(|(source_column, name)| source_column == &column.name && source_column != name)
                        .map(|(_, name)| name);
                    Ok(new_name.map_or_else(
                        || Transformed::no(expression),
                        |new_name| Transformed::yes(ident(new_name)),
                    ))
                })?
                .data;
            Ok(SortExpr::new(renamed_expression, asc, nulls_first))
        })
        .collect()
}

/// The Unix epoch is a Thursday; shifting by three days makes week buckets start on Mondays.
const WEEK_ALIGNMENT_MICROS: i64 = 3 * MICROS_PER_DAY;

/// The first year of the month index calendar buckets count from.
const EPOCH_YEAR: i64 = 1970;

/// The relation of the grouped aggregation that the distinct branches join.
const MAIN_RELATION: &str = "__ppl_main";

/// The relation of the per-bucket rows of a `timechart` that the series ranks join.
const CHART_RELATION: &str = "__ppl_chart";

/// The relation of the series ranks of a `timechart`.
const RANK_RELATION: &str = "__ppl_ranks";

/// The row count a grouped aggregation computes when every aggregation runs in a branch, so the
/// aggregation still yields its groups (one row without keys).
const ROW_COUNT_COLUMN: &str = "__ppl_rows";

/// The distinct value of a `values` branch.
const VALUE_COLUMN: &str = "__ppl_value";

/// The rank of a value combination (`top`, `rare`) or of a series (`timechart`).
const RANK_COLUMN: &str = "__ppl_rank";

/// The sum of a series' aggregation over all buckets, which ranks the `timechart` series.
const TOTAL_COLUMN: &str = "__ppl_total";

/// The series name in the ranks of a `timechart`.
const SERIES_COLUMN: &str = "__ppl_series";

/// The position of a row among the rows of its `dedup` combination.
const OCCURRENCE_COLUMN: &str = "__ppl_occurrence";

/// The label of the series beyond a `timechart` limit.
const OTHER_SERIES: &str = "OTHER";

/// The label of the NULL series of a `timechart` with a limit.
const NULL_SERIES: &str = "NULL";

/// One group key of an aggregation: its output name and the value rows are grouped by.
struct GroupKey {
    name: String,
    value: Expr,
    /// Whether the key is a `span`, listed before the field keys in the output.
    is_span: bool,
}

/// The internal column of the group key at `index`.
fn name_key_column(index: usize) -> String {
    format!("__ppl_key_{index}")
}

/// The internal column of the aggregation at `index`.
fn name_aggregate_column(index: usize) -> String {
    format!("__ppl_aggregate_{index}")
}

/// The relation of the branch of the aggregation at `index`.
fn name_branch_relation(index: usize) -> String {
    format!("__ppl_branch_{index}")
}

/// The column `name` of `relation`.
fn qualify_column(relation: &str, name: &str) -> Expr {
    Expr::Column(Column::new(Some(relation), name))
}

/// `stats`: one row per group, as the columns `<aggregations…>, <span key>, <field keys…>`, sorted
/// by the keys in that order, ascending with NULL first; a row whose time-span field is NULL forms
/// no group. The `Aggregations` section of `ppl/README.md` states the command's contract.
fn aggregate_rows(
    builder: LogicalPlanBuilder,
    stats: &StatsCommand,
    scope: &ExpressionScope<'_>,
) -> Result<LogicalPlanBuilder, PplError> {
    let mut keys = Vec::with_capacity(stats.by.len());
    let mut required_predicates = Vec::new();
    for key in &stats.by {
        match key {
            PplGroupKey::Field(name) => keys.push(GroupKey {
                name: name.clone(),
                value: resolve_field(name, scope)?,
                is_span: false,
            }),
            PplGroupKey::Span {
                field,
                length,
                unit,
                alias,
            } => {
                let value = resolve_field(field, scope)?;
                let (bucket, is_time) = lower_span(value.clone(), field, *length, *unit, scope)?;
                if is_time {
                    required_predicates.push(value.is_not_null());
                }
                keys.push(GroupKey {
                    name: alias.clone().unwrap_or_else(|| name_span(field, *length, *unit)),
                    value: bucket,
                    is_span: true,
                });
            }
        }
    }
    if !stats.is_bucket_nullable {
        required_predicates.extend(keys.iter().map(|key| key.value.clone().is_not_null()));
    }
    let aggregations = stats
        .aggregations
        .iter()
        .map(|(name, aggregation)| Ok((name.clone(), lower_aggregation(aggregation, scope)?)))
        .collect::<Result<Vec<_>, PplError>>()?;
    let grouped_rows = plan_grouped_aggregation(builder, &keys, &aggregations, required_predicates)?;
    let span_keys = keys.iter().filter(|key| key.is_span);
    let field_keys = keys.iter().filter(|key| !key.is_span);
    let sorts: Vec<SortExpr> = span_keys
        .chain(field_keys)
        .map(|key| ident(&key.name).sort(true, true))
        .collect();
    if sorts.is_empty() {
        return Ok(grouped_rows);
    }
    Ok(grouped_rows.sort(sorts)?)
}

/// Lower one aggregation of `stats` or `timechart`. `count(eval(p))` counts the rows where `p` is
/// true.
fn lower_aggregation(aggregation: &PplExpr, scope: &ExpressionScope<'_>) -> Result<PlannedAggregate, PplError> {
    let PplExpr::FunctionCall { name, arguments } = aggregation else {
        return Err(PplError::Internal {
            reason: "an aggregation that is not a function call; the builder yields only calls".to_string(),
        });
    };
    let arguments = arguments
        .iter()
        .map(|argument| match argument {
            PplExpr::FunctionCall {
                name: wrapper,
                arguments: predicate,
            } if wrapper == "eval" => {
                let [predicate] = predicate.as_slice() else {
                    return Err(PplError::Internal {
                        reason: "a count(eval()) without exactly one predicate; the builder yields one".to_string(),
                    });
                };
                Ok(when(lower_expression(predicate, scope)?, lit(1_i64)).end()?)
            }
            other => lower_expression(other, scope),
        })
        .collect::<Result<Vec<_>, _>>()?;
    plan_aggregate_call(name, arguments)
}

/// The grouped aggregation: one aggregate over the group keys for the plain aggregates and
/// percentiles, and one branch per exact distinct count or value list, joined back on the keys
/// (NULL keys join NULL keys). Output: the aggregations in order, then the span keys, then the
/// field keys.
///
/// The branches join as a full join: every branch reads the same keyed rows as the main
/// aggregation, so every branch group is a main group, and the full join returns exactly the main
/// groups, the NULL-key group included. The integration test
/// `values_is_distinct_sorted_null_free_and_uncapped` and the unit test
/// `grouped_distinct_counts_and_value_lists_keep_the_null_key_group` pin that the NULL-key group
/// is kept.
fn plan_grouped_aggregation(
    builder: LogicalPlanBuilder,
    keys: &[GroupKey],
    aggregations: &[(String, PlannedAggregate)],
    required_predicates: Vec<Expr>,
) -> Result<LogicalPlanBuilder, PplError> {
    let builder = match required_predicates.into_iter().reduce(Expr::and) {
        Some(predicate) => builder.filter(predicate)?,
        None => builder,
    };
    let mut keyed_columns: Vec<Expr> = builder.schema().columns().into_iter().map(Expr::Column).collect();
    keyed_columns.extend(
        keys.iter()
            .enumerate()
            .map(|(index, key)| key.value.clone().alias(name_key_column(index))),
    );
    let keyed_rows = builder.project(keyed_columns)?;
    let key_columns: Vec<Expr> = (0..keys.len()).map(|index| ident(name_key_column(index))).collect();
    let mut grouped_aggregates = Vec::with_capacity(aggregations.len());
    let mut branches = Vec::new();
    for (index, (_, aggregation)) in aggregations.iter().enumerate() {
        match aggregation {
            PlannedAggregate::Grouped(aggregate) | PlannedAggregate::Percentile(aggregate) => {
                grouped_aggregates.push(aggregate.clone().alias(name_aggregate_column(index)));
            }
            PlannedAggregate::DistinctCount(value) => {
                let distinct_counts =
                    plan_exact_distinct(keyed_rows.clone(), &key_columns, std::slice::from_ref(value))?;
                branches.push((index, select_distinct_count(distinct_counts, keys.len(), index)?));
            }
            PlannedAggregate::DistinctValues(value) => {
                branches.push((
                    index,
                    plan_distinct_values(keyed_rows.clone(), &key_columns, value, index)?,
                ));
            }
        }
    }
    if grouped_aggregates.is_empty() {
        grouped_aggregates.push(count(lit(1_i64)).alias(ROW_COUNT_COLUMN));
    }
    let mut combined_rows = keyed_rows
        .clone()
        .aggregate(key_columns, grouped_aggregates)?
        .alias(MAIN_RELATION)?;
    for (index, branch) in branches {
        let relation = name_branch_relation(index);
        let branch = LogicalPlanBuilder::from(branch).alias(relation.as_str())?.build()?;
        combined_rows = if keys.is_empty() {
            combined_rows.cross_join(branch)?
        } else {
            let left_keys: Vec<Column> = (0..keys.len())
                .map(|key| Column::new(Some(MAIN_RELATION), name_key_column(key)))
                .collect();
            let right_keys: Vec<Column> = (0..keys.len())
                .map(|key| Column::new(Some(relation.as_str()), name_key_column(key)))
                .collect();
            combined_rows.join_detailed(
                branch,
                JoinType::Full,
                (left_keys, right_keys),
                None,
                NullEquality::NullEqualsNull,
            )?
        };
    }
    let mut output = Vec::with_capacity(aggregations.len() + keys.len());
    for (index, (name, aggregation)) in aggregations.iter().enumerate() {
        let value = match aggregation {
            PlannedAggregate::Grouped(_) => qualify_column(MAIN_RELATION, &name_aggregate_column(index)),
            PlannedAggregate::Percentile(_) => {
                array_element(qualify_column(MAIN_RELATION, &name_aggregate_column(index)), lit(1_i64))
            }
            PlannedAggregate::DistinctCount(_) => coalesce(vec![
                qualify_column(&name_branch_relation(index), &name_aggregate_column(index)),
                lit(0_i64),
            ]),
            PlannedAggregate::DistinctValues(value) => {
                let element = value.get_type(keyed_rows.schema())?;
                let empty_list = ScalarValue::List(ScalarValue::new_list_nullable(&[], &element));
                coalesce(vec![
                    qualify_column(&name_branch_relation(index), &name_aggregate_column(index)),
                    lit(empty_list),
                ])
            }
        };
        output.push(value.alias(name));
    }
    let span_keys = keys.iter().enumerate().filter(|(_, key)| key.is_span);
    let field_keys = keys.iter().enumerate().filter(|(_, key)| !key.is_span);
    for (index, key) in span_keys.chain(field_keys) {
        output.push(qualify_column(MAIN_RELATION, &name_key_column(index)).alias(&key.name));
    }
    Ok(combined_rows.project(output)?)
}

/// The keys and the distinct count of an exact distinct plan, whose output lists the key columns
/// and then the count.
fn select_distinct_count(
    distinct_counts: LogicalPlanBuilder,
    key_count: usize,
    index: usize,
) -> Result<LogicalPlan, PplError> {
    let schema = Arc::clone(distinct_counts.schema());
    let mut projection: Vec<Expr> = (0..key_count).map(|key| ident(name_key_column(key))).collect();
    let (qualifier, field) = schema.qualified_field(key_count);
    projection.push(Expr::Column(Column::new(qualifier.cloned(), field.name())).alias(name_aggregate_column(index)));
    Ok(distinct_counts.project(projection)?.build()?)
}

/// The distinct non-NULL values of `value` per group, sorted: grouping by the keys and the value
/// first makes the values distinct with a grouped accumulator, then one sorted list per group.
fn plan_distinct_values(
    keyed_rows: LogicalPlanBuilder,
    key_columns: &[Expr],
    value: &Expr,
    index: usize,
) -> Result<LogicalPlan, PplError> {
    let mut distinct_keys = key_columns.to_vec();
    distinct_keys.push(value.clone().alias(VALUE_COLUMN));
    let collected_values = array_agg(ident(VALUE_COLUMN))
        .order_by(vec![ident(VALUE_COLUMN).sort(true, false)])
        .build()?
        .alias(name_aggregate_column(index));
    Ok(keyed_rows
        .filter(value.clone().is_not_null())?
        .aggregate(distinct_keys, Vec::<Expr>::new())?
        .aggregate(key_columns.to_vec(), vec![collected_values])?
        .build()?)
}

/// The bucket of `value` for a `span` of `length` `unit`, and whether it is a time bucket.
///
/// A numeric span of an integer field truncates toward zero (`(x / n) * n`) and of any other
/// numeric field floors (`floor(x / n) * n`). A time span buckets a timestamp: fixed units from
/// the Unix epoch, weeks from Mondays, months, quarters and years from the first day of their
/// calendar period, in UTC.
fn lower_span(
    value: Expr,
    field: &str,
    length: f64,
    unit: SpanUnit,
    scope: &ExpressionScope<'_>,
) -> Result<(Expr, bool), PplError> {
    let data_type = value.get_type(scope.schema)?;
    let is_time = is_time_type(&data_type);
    match (unit, is_time) {
        (SpanUnit::Numeric, false) => {
            if !data_type.is_numeric() {
                return Err(PplError::Semantic {
                    reason: format!("span takes a numeric or time field; [{field}] is {data_type}"),
                });
            }
            let is_integer_bucket = data_type.is_integer() && length.fract() == 0.0;
            if is_integer_bucket {
                let width = lit(convert_whole_width(length)?);
                let truncated_quotient = binary_expr(cast(value, DataType::Int64), Operator::Divide, width.clone());
                return Ok((binary_expr(truncated_quotient, Operator::Multiply, width), false));
            }
            let quotient = binary_expr(cast(value, DataType::Float64), Operator::Divide, lit(length));
            Ok((binary_expr(floor(quotient), Operator::Multiply, lit(length)), false))
        }
        (SpanUnit::Numeric, true) => Err(PplError::Semantic {
            reason: format!("span of the time field [{field}] needs a time unit"),
        }),
        (_, false) => Err(PplError::Semantic {
            reason: format!("span with a time unit takes a time field; [{field}] is {data_type}"),
        }),
        (unit, true) => {
            #[allow(clippy::cast_possible_truncation)]
            let length = length as i64;
            Ok((bucket_time(value, length, unit)?, true))
        }
    }
}

/// Whether `data_type` is a timestamp or a date, which time spans and bins bucket.
const fn is_time_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
    )
}

/// The UTC start of the `length`-`unit` bucket holding `value`, as a microsecond timestamp.
fn bucket_time(value: Expr, length: i64, unit: SpanUnit) -> Result<Expr, PplError> {
    let micros = convert_to_micros(value.clone());
    let bucket_fixed_width = |unit_micros: i64| -> Result<Expr, PplError> {
        let width = compute_bucket_width(length, unit_micros)?;
        Ok(convert_micros_to_timestamp(floor_to_multiple(micros.clone(), width)))
    };
    match unit {
        SpanUnit::Microsecond => bucket_fixed_width(1),
        SpanUnit::Millisecond => bucket_fixed_width(MICROS_PER_MILLISECOND),
        SpanUnit::Centisecond => bucket_fixed_width(10 * MICROS_PER_MILLISECOND),
        SpanUnit::Decisecond => bucket_fixed_width(100 * MICROS_PER_MILLISECOND),
        SpanUnit::Second => bucket_fixed_width(MICROS_PER_SECOND),
        SpanUnit::Minute => bucket_fixed_width(MICROS_PER_MINUTE),
        SpanUnit::Hour => bucket_fixed_width(MICROS_PER_HOUR),
        SpanUnit::Day => bucket_fixed_width(MICROS_PER_DAY),
        SpanUnit::Week => {
            let width = compute_bucket_width(length, MICROS_PER_WEEK)?;
            let shifted_micros = binary_expr(micros, Operator::Plus, lit(WEEK_ALIGNMENT_MICROS));
            let start = binary_expr(
                floor_to_multiple(shifted_micros, width),
                Operator::Minus,
                lit(WEEK_ALIGNMENT_MICROS),
            );
            Ok(convert_micros_to_timestamp(start))
        }
        SpanUnit::Month => Ok(compute_month_bucket_start(value, compute_bucket_width(length, 1)?)),
        SpanUnit::Quarter => Ok(compute_month_bucket_start(value, compute_bucket_width(length, 3)?)),
        SpanUnit::Year => Ok(compute_month_bucket_start(value, compute_bucket_width(length, 12)?)),
        SpanUnit::Numeric => Err(PplError::Internal {
            reason: "a time bucket without a time unit".to_string(),
        }),
    }
}

/// The width of `length` units of `unit_width` each: microseconds for the fixed units and weeks,
/// months for months, quarters and years.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] when twice the width overflows an `i64`: [`floor_modulo`] adds
/// the width to a remainder, which would then wrap into a wrong bucket.
fn compute_bucket_width(length: i64, unit_width: i64) -> Result<i64, PplError> {
    length
        .checked_mul(unit_width)
        .filter(|width| width.checked_mul(2).is_some())
        .ok_or_else(|| PplError::Semantic {
            reason: "span length is out of range".to_string(),
        })
}

/// 2^63, the least whole number an `i64` cannot hold; every whole double below it converts exactly.
const WHOLE_WIDTH_LIMIT: f64 = 9_223_372_036_854_775_808.0;

/// A whole-number span or bin `width` as an `i64`.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] for a width of 2^63 or more, which a cast would saturate to
/// another width than the query wrote.
fn convert_whole_width(width: f64) -> Result<i64, PplError> {
    if width >= WHOLE_WIDTH_LIMIT {
        return Err(PplError::Semantic {
            reason: "span length is out of range".to_string(),
        });
    }
    #[allow(clippy::cast_possible_truncation)]
    Ok(width as i64)
}

/// `value` as microseconds since the epoch.
fn convert_to_micros(value: Expr) -> Expr {
    cast(
        cast(value, DataType::Timestamp(TimeUnit::Microsecond, None)),
        DataType::Int64,
    )
}

/// `value` rounded down to a multiple of `width` (floor, also below zero).
fn floor_to_multiple(value: Expr, width: i64) -> Expr {
    binary_expr(value.clone(), Operator::Minus, floor_modulo(value, width))
}

/// `value` modulo `width`, in `0..width`.
fn floor_modulo(value: Expr, width: i64) -> Expr {
    let remainder = binary_expr(value, Operator::Modulo, lit(width));
    binary_expr(
        binary_expr(remainder, Operator::Plus, lit(width)),
        Operator::Modulo,
        lit(width),
    )
}

/// Microseconds since the epoch as a UTC timestamp.
fn convert_micros_to_timestamp(micros: Expr) -> Expr {
    cast(micros, DataType::Timestamp(TimeUnit::Microsecond, None))
}

/// The months since January 1970 of the month holding `value`.
fn compute_month_index(value: Expr) -> Expr {
    let year = cast(date_part(lit("year"), value.clone()), DataType::Int64);
    let month = cast(date_part(lit("month"), value), DataType::Int64);
    let years = binary_expr(year, Operator::Minus, lit(EPOCH_YEAR));
    binary_expr(
        binary_expr(years, Operator::Multiply, lit(12_i64)),
        Operator::Plus,
        binary_expr(month, Operator::Minus, lit(1_i64)),
    )
}

/// The first day of the month that starts the `months`-month bucket holding `value`, buckets
/// counted from January 1970, as a UTC timestamp.
fn compute_month_bucket_start(value: Expr, months: i64) -> Expr {
    let start = floor_to_multiple(compute_month_index(value), months);
    cast(
        compute_first_day_of_month(start),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    )
}

/// The date of the first day of month `index` counted from January 1970.
fn compute_first_day_of_month(index: Expr) -> Expr {
    let month_of_year = floor_modulo(index.clone(), 12);
    let years = binary_expr(
        binary_expr(index, Operator::Minus, month_of_year.clone()),
        Operator::Divide,
        lit(12_i64),
    );
    let year = binary_expr(years, Operator::Plus, lit(EPOCH_YEAR));
    let month = binary_expr(month_of_year, Operator::Plus, lit(1_i64));
    make_date(cast(year, DataType::Int32), cast(month, DataType::Int32), lit(1_i32))
}

/// The default name of a span key: `span(<field>,<length><unit>)`.
fn name_span(field: &str, length: f64, unit: SpanUnit) -> String {
    let unit = match unit {
        SpanUnit::Numeric => "",
        SpanUnit::Microsecond => "us",
        SpanUnit::Millisecond => "ms",
        SpanUnit::Centisecond => "cs",
        SpanUnit::Decisecond => "ds",
        SpanUnit::Second => "s",
        SpanUnit::Minute => "m",
        SpanUnit::Hour => "h",
        SpanUnit::Day => "d",
        SpanUnit::Week => "w",
        SpanUnit::Month => "M",
        SpanUnit::Quarter => "q",
        SpanUnit::Year => "y",
    };
    format!("span({field},{length}{unit})")
}

/// `top` / `rare`: per group of the `by` fields, the `count` most (`is_top`) or least frequent
/// value combinations of the fields with their count. Ties rank by the field values, ascending
/// and NULL last. Output: the `by` fields, the fields, then the count unless hidden; rows ordered
/// by the `by` fields, then rank.
fn rank_values(
    builder: LogicalPlanBuilder,
    rank: &RankCommand,
    is_top: bool,
    scope: &ExpressionScope<'_>,
) -> Result<LogicalPlanBuilder, PplError> {
    if scope.schema.field_with_unqualified_name(&rank.count_field).is_ok() {
        return Err(PplError::Semantic {
            reason: format!(
                "field [{}] already exists; name the count column with countfield='<name>'",
                rank.count_field
            ),
        });
    }
    let groups = rank
        .by
        .iter()
        .chain(&rank.fields)
        .map(|name| Ok((name.clone(), resolve_field(name, scope)?)))
        .collect::<Result<Vec<_>, PplError>>()?;
    let builder = match groups.iter().map(|(_, value)| value.clone().is_not_null()).reduce(Expr::and) {
        Some(predicate) if !rank.is_null_kept => builder.filter(predicate)?,
        _ => builder,
    };
    let counted_combinations = builder.aggregate(
        groups.iter().map(|(name, value)| value.clone().alias(name)),
        vec![count(lit(1_i64)).alias(&rank.count_field)],
    )?;
    let mut order = vec![ident(&rank.count_field).sort(!is_top, false)];
    order.extend(rank.fields.iter().map(|name| ident(name).sort(true, false)));
    let ranking = row_number()
        .partition_by(rank.by.iter().map(ident).collect())
        .order_by(order)
        .build()?
        .alias(RANK_COLUMN);
    let limit = u64::try_from(rank.count).unwrap_or(u64::MAX);
    let mut sorts: Vec<_> = rank.by.iter().map(|name| ident(name).sort(true, true)).collect();
    sorts.push(ident(RANK_COLUMN).sort(true, false));
    let mut output: Vec<Expr> = rank.by.iter().chain(&rank.fields).map(ident).collect();
    if rank.is_count_shown {
        output.push(ident(&rank.count_field));
    }
    Ok(counted_combinations
        .window(vec![ranking])?
        .filter(ident(RANK_COLUMN).lt_eq(lit(limit)))?
        .sort(sorts)?
        .project(output)?)
}

/// `dedup`: the first `count` rows of each combination of the fields in the order of
/// `sort_keys`, which the result keeps; without an earlier `sort` (`sort_keys` empty) the order
/// is the scan's, so which rows of a combination are kept is unspecified. Rows with a NULL field
/// are dropped, or with `keepempty` all kept.
fn deduplicate_rows(
    builder: LogicalPlanBuilder,
    dedup: &DedupCommand,
    sort_keys: &[SortExpr],
    scope: &ExpressionScope<'_>,
) -> Result<LogicalPlanBuilder, PplError> {
    let fields = dedup
        .fields
        .iter()
        .map(|name| resolve_field(name, scope))
        .collect::<Result<Vec<_>, _>>()?;
    let has_null_field = fields
        .iter()
        .map(|field| field.clone().is_null())
        .reduce(Expr::or)
        .ok_or_else(|| PplError::Internal {
            reason: "a dedup without a field; the grammar requires one".to_string(),
        })?;
    let builder = if dedup.keeps_empty {
        builder
    } else {
        builder.filter(not(has_null_field.clone()))?
    };
    let columns: Vec<Expr> = builder.schema().columns().into_iter().map(Expr::Column).collect();
    let occurrence = row_number().partition_by(fields);
    let occurrence = if sort_keys.is_empty() {
        occurrence
    } else {
        occurrence.order_by(sort_keys.to_vec())
    };
    let occurrence = occurrence.build()?.alias(OCCURRENCE_COLUMN);
    let is_within_count = ident(OCCURRENCE_COLUMN).lt_eq(lit(u64::try_from(dedup.count).unwrap_or(u64::MAX)));
    let is_kept = if dedup.keeps_empty {
        has_null_field.or(is_within_count)
    } else {
        is_within_count
    };
    let deduplicated_rows = builder.window(vec![occurrence])?.filter(is_kept)?;
    let deduplicated_rows = if sort_keys.is_empty() {
        deduplicated_rows
    } else {
        deduplicated_rows.sort(sort_keys.to_vec())?
    };
    Ok(deduplicated_rows.project(columns)?)
}

/// `timechart`: the aggregation per time bucket and series, as the columns `<time field>,
/// [<by field>], <aggregation>`, ordered by time then series, NULL last; empty buckets are not
/// filled.
///
/// With a `by` field and a `limit` above 0, the series beyond the limit merge into
/// [`OTHER_SERIES`] and a NULL series is [`NULL_SERIES`]; with `useother=false` the NULL series is
/// dropped together with the series beyond the limit. Series rank by the sum of the aggregation,
/// so there an aggregation that is not numeric is a [`PplError::Semantic`]. With `limit=0` a NULL series stays NULL. A
/// non-string series is compared as its string form. The `Timechart` section of `ppl/README.md`
/// states which series stay within the limit and how merged rows re-aggregate.
fn chart_rows(
    builder: LogicalPlanBuilder,
    chart: &TimechartCommand,
    scope: &ExpressionScope<'_>,
) -> Result<LogicalPlanBuilder, PplError> {
    let time = resolve_field(&chart.time_field, scope)?;
    let (bucket, is_time) = lower_span(
        time.clone(),
        &chart.time_field,
        chart.span_length,
        chart.span_unit,
        scope,
    )?;
    if !is_time {
        return Err(PplError::Semantic {
            reason: format!("timechart needs a time field; [{}] is not one", chart.time_field),
        });
    }
    let mut keys = vec![GroupKey {
        name: chart.time_field.clone(),
        value: bucket,
        is_span: true,
    }];
    if let Some(by) = &chart.by {
        keys.push(GroupKey {
            name: by.clone(),
            value: resolve_field(by, scope)?,
            is_span: false,
        });
    }
    let (aggregate_name, aggregation) = &chart.aggregation;
    let planned_aggregate = lower_aggregation(aggregation, scope)?;
    let grouped_rows = plan_grouped_aggregation(
        builder,
        &keys,
        &[(aggregate_name.clone(), planned_aggregate)],
        vec![time.is_not_null()],
    )?;
    let time_column = ident(&chart.time_field);
    let aggregate = ident(aggregate_name);
    let Some(by) = &chart.by else {
        return Ok(grouped_rows
            .sort(vec![time_column.clone().sort(true, false)])?
            .project(vec![time_column, aggregate])?);
    };
    if chart.limit == 0 {
        return Ok(grouped_rows
            .sort(vec![time_column.clone().sort(true, false), ident(by).sort(true, false)])?
            .project(vec![time_column, ident(by), aggregate])?);
    }
    let aggregate_type = grouped_rows.schema().field_with_unqualified_name(aggregate_name)?.data_type();
    if !aggregate_type.is_numeric() {
        return Err(PplError::Semantic {
            reason: format!(
                "timechart ranks series by the sum of [{aggregate_name}], which is not numeric ({aggregate_type}); \
                 set limit=0 to keep every series"
            ),
        });
    }
    let series_label = render_series_label(grouped_rows.schema(), by)?;
    let charted_rows = grouped_rows.project(vec![time_column, series_label.alias(by), aggregate])?;
    let ranking = row_number()
        .order_by(vec![
            ident(TOTAL_COLUMN).sort(false, false),
            ident(by).sort(true, false),
        ])
        .build()?
        .alias(RANK_COLUMN);
    let ranks = charted_rows
        .clone()
        .filter(ident(by).is_not_null())?
        .aggregate(vec![ident(by)], vec![sum(ident(aggregate_name)).alias(TOTAL_COLUMN)])?
        .window(vec![ranking])?
        .project(vec![ident(by).alias(SERIES_COLUMN), ident(RANK_COLUMN)])?
        .alias(RANK_RELATION)?
        .build()?;
    let ranked_rows = charted_rows.alias(CHART_RELATION)?.join_detailed(
        ranks,
        JoinType::Left,
        (
            vec![Column::new(Some(CHART_RELATION), by)],
            vec![Column::new(Some(RANK_RELATION), SERIES_COLUMN)],
        ),
        None,
        NullEquality::NullEqualsNothing,
    )?;
    let limit = u64::try_from(chart.limit).unwrap_or(u64::MAX);
    let is_kept = qualify_column(RANK_RELATION, RANK_COLUMN).lt_eq(lit(limit));
    let ranked_rows = if chart.uses_other {
        ranked_rows
    } else {
        ranked_rows.filter(is_kept.clone())?
    };
    let series = qualify_column(CHART_RELATION, by);
    let label = when(series.clone().is_null(), lit(NULL_SERIES))
        .when(is_kept, series)
        .otherwise(lit(OTHER_SERIES))?;
    let merged_aggregate = merge_series(aggregation, ident(aggregate_name))?;
    Ok(ranked_rows
        .project(vec![
            qualify_column(CHART_RELATION, &chart.time_field).alias(&chart.time_field),
            label.alias(by),
            qualify_column(CHART_RELATION, aggregate_name).alias(aggregate_name),
        ])?
        .aggregate(
            vec![ident(&chart.time_field), ident(by)],
            vec![merged_aggregate.alias(aggregate_name)],
        )?
        .sort(vec![
            ident(&chart.time_field).sort(true, false),
            ident(by).sort(true, false),
        ])?
        .project(vec![ident(&chart.time_field), ident(by), ident(aggregate_name)])?)
}

/// The series column of a chart rendered as a string label, refusing a series of objects or lists.
fn render_series_label(schema: &DFSchema, by: &str) -> Result<Expr, PplError> {
    let field = schema.field_with_unqualified_name(by)?;
    match field.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(ident(by)),
        DataType::Map(_, _) | DataType::Struct(_) => Err(PplError::Semantic {
            reason: format!("Cannot chart by [{by}] because it is an object."),
        }),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => Err(PplError::Semantic {
            reason: format!("Cannot chart by [{by}] because it holds multiple values."),
        }),
        _ => Ok(cast(ident(by), DataType::Utf8)),
    }
}

/// The aggregate that merges per-series values of `aggregation` into the [`OTHER_SERIES`] series.
fn merge_series(aggregation: &PplExpr, value: Expr) -> Result<Expr, PplError> {
    let PplExpr::FunctionCall { name, .. } = aggregation else {
        return Err(PplError::Internal {
            reason: "a timechart aggregation that is not a function call; the builder yields only calls".to_string(),
        });
    };
    Ok(match name.as_str() {
        "min" => min(value),
        "max" => max(value),
        "avg" => avg(value),
        _ => sum(value),
    })
}

/// `bin`: the field's value replaced by its bin. Numbers bin to `<start>-<end>` strings of a
/// whole-number width (`floor(x / w) * w`); a fractional width is refused. Timestamps bin to the
/// bucket start: fixed units from the Unix epoch (a bare number is hours), days to midnight UTC,
/// and months to the `yyyy-MM` string of the bucket's first month.
fn bin_field(
    builder: LogicalPlanBuilder,
    bin: &BinCommand,
    scope: &ExpressionScope<'_>,
) -> Result<LogicalPlanBuilder, PplError> {
    let value = resolve_field(&bin.field, scope)?;
    let data_type = value.get_type(scope.schema)?;
    let binned_value = match (bin.span, is_time_type(&data_type)) {
        (BinSpan::Numeric(width), false) => {
            if !data_type.is_numeric() {
                return Err(PplError::Semantic {
                    reason: format!("bin takes a numeric or time field; [{}] is {data_type}", bin.field),
                });
            }
            bin_number(value, width, &data_type)?
        }
        (BinSpan::Numeric(hours), true) => bin_time(value, read_whole_width(hours)?, SpanUnit::Hour)?,
        (BinSpan::Time { length, unit }, true) => bin_time(value, length, unit)?,
        (BinSpan::Time { .. }, false) => {
            return Err(PplError::Semantic {
                reason: format!(
                    "bin with a time span takes a time field; [{}] is {data_type}",
                    bin.field
                ),
            });
        }
    };
    let mut is_replaced = false;
    let mut projection: Vec<Expr> = scope
        .schema
        .fields()
        .iter()
        .map(|column| {
            if column.name() == &bin.field {
                is_replaced = true;
                binned_value.clone().alias(&bin.field)
            } else {
                ident(column.name())
            }
        })
        .collect();
    if !is_replaced {
        projection.push(binned_value.alias(&bin.field));
    }
    Ok(builder.project(projection)?)
}

/// A `bin` width that must be a whole number of at least 1.
fn read_whole_width(width: f64) -> Result<i64, PplError> {
    if width.fract() != 0.0 || width < 1.0 {
        return Err(PplError::Semantic {
            reason: format!("bin span [{width}] must be a whole number"),
        });
    }
    convert_whole_width(width)
}

/// The type the bounds of a number bin are computed in: a decimal without a fractional part, of
/// the greatest 128-bit precision. The bin of a value within one width of a 64-bit integer limit
/// starts or ends past that limit, and this type holds such a bound exactly.
const BIN_BOUND_TYPE: DataType = DataType::Decimal128(DECIMAL128_MAX_PRECISION, 0);

/// `<start>-<end>` of the whole-number-width bin holding `value` of type `data_type`. An integer
/// value bins exactly; any other number floors its `f64` quotient. Both bounds are computed in
/// [`BIN_BOUND_TYPE`], so a bin that reaches past a 64-bit integer limit renders its exact bounds.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] for a width that is not a whole number of at least 1, and for
/// one that, doubled, overflows an `i64`: that guard keeps the 64-bit [`floor_modulo`] of an
/// integer value exact. A non-integer value whose floored quotient is `NaN` or no 64-bit integer
/// fails the query at execution, when that quotient is cast to `i64`.
fn bin_number(value: Expr, width: f64, data_type: &DataType) -> Result<Expr, PplError> {
    let whole_width = read_whole_width(width)?;
    if whole_width.checked_mul(2).is_none() {
        return Err(PplError::Semantic {
            reason: format!("bin span [{width}] is out of range"),
        });
    }
    let width_bound = cast(lit(whole_width), BIN_BOUND_TYPE);
    let start = if data_type.is_integer() {
        let integer_value = cast(value, DataType::Int64);
        let remainder = floor_modulo(integer_value.clone(), whole_width);
        binary_expr(
            cast(integer_value, BIN_BOUND_TYPE),
            Operator::Minus,
            cast(remainder, BIN_BOUND_TYPE),
        )
    } else {
        let quotient = binary_expr(cast(value, DataType::Float64), Operator::Divide, lit(width));
        // Through `i64`, the floored quotient keeps both bounds below 2^125 in magnitude, inside
        // the precision of `BIN_BOUND_TYPE`.
        binary_expr(
            cast(cast(floor(quotient), DataType::Int64), BIN_BOUND_TYPE),
            Operator::Multiply,
            width_bound.clone(),
        )
    };
    let end = binary_expr(start.clone(), Operator::Plus, width_bound);
    let render_bound = |bound: Expr| cast(bound, DataType::Utf8);
    Ok(binary_expr(
        binary_expr(render_bound(start), Operator::StringConcat, lit("-")),
        Operator::StringConcat,
        render_bound(end),
    ))
}

/// The bin of a timestamp: fixed units bucket from the Unix epoch; a day bin starts at midnight
/// UTC of the day `floor(d / n) * n` days after the epoch; a month bin is the `yyyy-MM` string of
/// month `floor(i / n) * n` after January 1970. Both floor toward negative infinity, so an instant
/// before the epoch bins to a start at or before it.
///
/// # Errors
///
/// Returns [`PplError::Semantic`] for a fixed-unit length whose width [`compute_bucket_width`]
/// refuses, and for a day or month length above [`MAX_DAY_BIN_LENGTH`] or
/// [`MAX_MONTH_BIN_LENGTH`], whose bins before the epoch start outside the timestamp range.
fn bin_time(value: Expr, length: i64, unit: SpanUnit) -> Result<Expr, PplError> {
    match unit {
        SpanUnit::Day => {
            check_calendar_bin_length(length, MAX_DAY_BIN_LENGTH)?;
            let days = binary_expr(
                floor_to_multiple(convert_to_micros(value), MICROS_PER_DAY),
                Operator::Divide,
                lit(MICROS_PER_DAY),
            );
            Ok(convert_micros_to_timestamp(binary_expr(
                floor_to_calendar_multiple(days, length)?,
                Operator::Multiply,
                lit(MICROS_PER_DAY),
            )))
        }
        SpanUnit::Month => {
            check_calendar_bin_length(length, MAX_MONTH_BIN_LENGTH)?;
            let start = floor_to_calendar_multiple(compute_month_index(value), length)?;
            Ok(to_char(compute_first_day_of_month(start), lit("%Y-%m")))
        }
        other => bucket_time(value, length, other),
    }
}

/// Longest day span `bin` accepts: the days of the 64-bit microsecond range. A longer span would
/// start the bin of an instant before the epoch outside that range.
const MAX_DAY_BIN_LENGTH: i64 = i64::MAX / MICROS_PER_DAY;

/// Longest month span `bin` accepts: [`MAX_DAY_BIN_LENGTH`] counted in 31-day months, so the start
/// of every bin stays inside the timestamp range.
const MAX_MONTH_BIN_LENGTH: i64 = MAX_DAY_BIN_LENGTH / 31;

/// Refuse a day or month bin `length` above `max_length`.
fn check_calendar_bin_length(length: i64, max_length: i64) -> Result<(), PplError> {
    if length > max_length {
        return Err(PplError::Semantic {
            reason: "span length is out of range".to_string(),
        });
    }
    Ok(())
}

/// The greatest multiple of `length` at or below the day or month index `index`. Unlike
/// [`floor_to_multiple`] it never adds `length` to a remainder, so a length up to the calendar
/// limits cannot wrap: `index - index % length` is the floor unless the remainder is negative,
/// and then the floor is one `length` lower.
fn floor_to_calendar_multiple(index: Expr, length: i64) -> Result<Expr, PplError> {
    let remainder = binary_expr(index.clone(), Operator::Modulo, lit(length));
    let truncated = binary_expr(index, Operator::Minus, remainder.clone());
    Ok(when(
        remainder.lt(lit(0_i64)),
        binary_expr(truncated.clone(), Operator::Minus, lit(length)),
    )
    .otherwise(truncated)?)
}

/// Keep the listed fields in the order listed, or with `is_exclusion` drop them and keep the
/// rest in their order. A name listed twice is kept once. A listed field that is not a column
/// (a MAP key path such as ``log_attributes.`http.method` ``, or a promoted resource attribute)
/// is kept as a column named by its path. Patterns never match the hidden order columns, which
/// are always kept after the selected ones.
fn select_fields(
    builder: LogicalPlanBuilder,
    is_exclusion: bool,
    patterns: &[String],
    scope: &ExpressionScope<'_>,
) -> Result<LogicalPlanBuilder, PplError> {
    let (order_columns, names): (Vec<String>, Vec<String>) = scope
        .schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .partition(|name| is_order_column(name));
    if is_exclusion {
        let mut dropped_names: HashSet<&str> = HashSet::with_capacity(patterns.len());
        for pattern in patterns {
            dropped_names.extend(match_columns(pattern, &names)?);
        }
        let mut kept_columns: Vec<Expr> = names
            .iter()
            .filter(|name| !dropped_names.contains(name.as_str()))
            .map(ident)
            .collect();
        if kept_columns.is_empty() {
            return Err(PplError::Semantic {
                reason: "fields - removes every column".to_string(),
            });
        }
        kept_columns.extend(order_columns.iter().map(ident));
        return Ok(builder.project(kept_columns)?);
    }
    let mut selected_names: Vec<&str> = Vec::with_capacity(patterns.len());
    let mut projection: Vec<Expr> = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        let is_column_pattern = pattern.contains('*') || names.contains(pattern);
        if is_column_pattern {
            for name in match_columns(pattern, &names)? {
                if !selected_names.contains(&name) {
                    selected_names.push(name);
                    projection.push(ident(name));
                }
            }
        } else if !selected_names.contains(&pattern.as_str()) {
            selected_names.push(pattern);
            projection.push(resolve_field(pattern, scope)?.alias(pattern));
        }
    }
    projection.extend(
        order_columns
            .iter()
            .filter(|name| !selected_names.contains(&name.as_str()))
            .map(ident),
    );
    Ok(builder.project(projection)?)
}

/// The columns `pattern` names: the column of that exact name, or every column a `*` pattern
/// matches, in column order.
fn match_columns<'a>(pattern: &str, names: &'a [String]) -> Result<Vec<&'a str>, PplError> {
    let matched_names: Vec<&str> = if pattern.contains('*') {
        names
            .iter()
            .filter(|name| matches_glob(pattern, name))
            .map(String::as_str)
            .collect()
    } else {
        names.iter().filter(|name| *name == pattern).map(String::as_str).collect()
    };
    if matched_names.is_empty() {
        return Err(PplError::FieldNotFound {
            field: pattern.to_string(),
        });
    }
    Ok(matched_names)
}

/// The sort keys of `sort`: by `keys` in turn; NULL sorts first ascending and last descending.
/// Strings compare byte-wise.
fn lower_sort_keys(keys: &[PplSortKey], scope: &ExpressionScope<'_>) -> Result<Vec<SortExpr>, PplError> {
    keys.iter()
        .map(|key| {
            let is_ascending = !key.is_descending;
            Ok(resolve_field(&key.field, scope)?.sort(is_ascending, is_ascending))
        })
        .collect()
}

/// Apply the `eval` assignments in order, each seeing the fields of the ones before it. An
/// assignment to an existing field replaces it in place; a new field is appended. Each assigned
/// name is recorded as replaced on `source`.
fn evaluate_fields(
    builder: LogicalPlanBuilder,
    assignments: &[(String, PplExpr)],
    mut source: Option<&mut SourceBinding<'_>>,
    now: DateTime<Utc>,
) -> Result<LogicalPlanBuilder, PplError> {
    let mut builder = builder;
    for (field, value) in assignments {
        let schema = Arc::clone(builder.schema());
        let value_scope = ExpressionScope::new(&schema, source.as_deref(), now);
        let assigned_value = lower_expression(value, &value_scope)?.alias(field);
        if let Some(source) = source.as_deref_mut() {
            source.replaced_columns.insert(field.clone());
        }
        let mut projection: Vec<Expr> = schema.fields().iter().map(|column| ident(column.name())).collect();
        match schema.fields().iter().position(|column| column.name() == field) {
            Some(replaced_position) => projection[replaced_position] = assigned_value,
            None => projection.push(assigned_value),
        }
        builder = builder.project(projection)?;
    }
    Ok(builder)
}

/// The columns after `rename` pairs applied in order over the columns of `schema`, as `(source
/// column, output name)` in output order: each field keeps its position under its new name, and a
/// field already holding the new name is removed.
///
/// # Errors
///
/// Returns [`PplError::FieldNotFound`] for a pair whose original name is no column at that point.
fn map_renamed_columns(schema: &DFSchema, renames: &[(String, String)]) -> Result<Vec<(String, String)>, PplError> {
    let mut columns: Vec<(String, String)> = schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.name().clone()))
        .collect();
    for (original_name, new_name) in renames {
        let renamed_position =
            columns
                .iter()
                .position(|(_, name)| name == original_name)
                .ok_or_else(|| PplError::FieldNotFound {
                    field: original_name.clone(),
                })?;
        if original_name == new_name {
            continue;
        }
        let replaced_position = columns.iter().position(|(_, name)| name == new_name);
        columns[renamed_position].1.clone_from(new_name);
        if let Some(replaced_position) = replaced_position {
            columns.remove(replaced_position);
        }
    }
    Ok(columns)
}

/// `builder`'s columns as `renamed_columns` ([`map_renamed_columns`]) lists them, then every
/// hidden order column added since that mapping was taken.
fn rename_fields(
    builder: LogicalPlanBuilder,
    renamed_columns: &[(String, String)],
) -> Result<LogicalPlanBuilder, PplError> {
    let mut projection: Vec<Expr> = renamed_columns
        .iter()
        .map(|(source_column, name)| ident(source_column).alias(name))
        .collect();
    projection.extend(
        builder
            .schema()
            .fields()
            .iter()
            .map(|field| field.name())
            .filter(|name| {
                is_order_column(name) && renamed_columns.iter().all(|(source_column, _)| source_column != *name)
            })
            .map(ident),
    );
    Ok(builder.project(projection)?)
}

/// Lower `expression` to a DataFusion expression over the columns of `scope`.
fn lower_expression(expression: &PplExpr, scope: &ExpressionScope<'_>) -> Result<Expr, PplError> {
    match expression {
        PplExpr::Field(name) => resolve_field(name, scope),
        PplExpr::Literal(literal) => Ok(lower_literal(literal)),
        PplExpr::Compare { operator, left, right } => {
            let left = lower_expression(left, scope)?;
            let right = lower_expression(right, scope)?;
            let (left, right) = coerce_timestamp_operands(left, right, scope.schema)?;
            refuse_id_comparison(&left, &right, scope)?;
            Ok(match operator {
                CompareOperator::Equal => left.eq(right),
                CompareOperator::NotEqual => left.not_eq(right),
                CompareOperator::Less => left.lt(right),
                CompareOperator::LessOrEqual => left.lt_eq(right),
                CompareOperator::Greater => left.gt(right),
                CompareOperator::GreaterOrEqual => left.gt_eq(right),
            })
        }
        PplExpr::Not(inner) => Ok(not(lower_expression(inner, scope)?)),
        PplExpr::And(left, right) => Ok(lower_expression(left, scope)?.and(lower_expression(right, scope)?)),
        PplExpr::Or(left, right) => Ok(lower_expression(left, scope)?.or(lower_expression(right, scope)?)),
        PplExpr::Xor(left, right) => {
            let left = lower_expression(left, scope)?;
            let right = lower_expression(right, scope)?;
            Ok(left.clone().or(right.clone()).and(not(left.and(right))))
        }
        PplExpr::Arithmetic { operator, left, right } => Ok(lower_arithmetic(
            *operator,
            lower_expression(left, scope)?,
            lower_expression(right, scope)?,
        )),
        PplExpr::FunctionCall { name, arguments } => lower_call(name, arguments, scope),
        PplExpr::Case { branches, otherwise } => {
            // The builder never yields a case without a condition; the grammar requires one.
            if branches.is_empty() {
                return Err(PplError::Internal {
                    reason: "a case without a condition".to_string(),
                });
            }
            let when_then_pairs = branches
                .iter()
                .map(|(condition, value)| {
                    Ok((
                        Box::new(lower_expression(condition, scope)?),
                        Box::new(lower_expression(value, scope)?),
                    ))
                })
                .collect::<Result<Vec<_>, PplError>>()?;
            let else_value = otherwise
                .as_ref()
                .map(|value| lower_expression(value, scope).map(Box::new))
                .transpose()?;
            Ok(Expr::Case(Case::new(None, when_then_pairs, else_value)))
        }
        PplExpr::Cast { value, target } => lower_cast(
            lower_expression(value, scope)?,
            *target,
            &scope.build_function_context(),
        ),
        PplExpr::Lambda { .. } => Err(PplError::Semantic {
            reason: "a lambda is only an argument of an array function".to_string(),
        }),
        PplExpr::In {
            value,
            list,
            is_negated,
        } => {
            let mut value = lower_expression(value, scope)?;
            let mut listed_values = Vec::with_capacity(list.len());
            for item in list {
                let (folded_value, item) =
                    coerce_timestamp_operands(value, lower_expression(item, scope)?, scope.schema)?;
                value = folded_value;
                listed_values.push(item);
            }
            Ok(value.in_list(listed_values, *is_negated))
        }
        PplExpr::Between {
            value,
            low,
            high,
            is_negated,
        } => {
            let value = lower_expression(value, scope)?;
            let (value, low) = coerce_timestamp_operands(value, lower_expression(low, scope)?, scope.schema)?;
            let (value, high) = coerce_timestamp_operands(value, lower_expression(high, scope)?, scope.schema)?;
            Ok(Expr::Between(Between::new(
                Box::new(value),
                *is_negated,
                Box::new(low),
                Box::new(high),
            )))
        }
        PplExpr::IsNull { value, is_negated } => {
            let value = lower_expression(value, scope)?;
            Ok(if *is_negated {
                value.is_not_null()
            } else {
                value.is_null()
            })
        }
    }
}

/// Lower a function call. The lambda argument of an array function is lowered with its
/// parameter bound to the element field of the array argument; any other second argument of an
/// array function is refused here, so it never reaches execution.
fn lower_call(name: &str, arguments: &[PplExpr], scope: &ExpressionScope<'_>) -> Result<Expr, PplError> {
    if let ([list, lambda_argument], true) = (arguments, is_higher_order_function(name)) {
        let PplExpr::Lambda { parameters, body } = lambda_argument else {
            return Err(PplError::Semantic {
                reason: format!("{name} takes a lambda as its second argument"),
            });
        };
        let list = lower_expression(list, scope)?;
        let element = match list.get_type(scope.schema)? {
            DataType::List(element) | DataType::LargeList(element) | DataType::FixedSizeList(element, _) => element,
            other => {
                return Err(PplError::Semantic {
                    reason: format!("{name} takes an array as its first argument, not {other}"),
                });
            }
        };
        let [parameter] = parameters.as_slice() else {
            return Err(PplError::Semantic {
                reason: format!("the lambda of {name} takes one parameter"),
            });
        };
        let mut lambda_scope = ExpressionScope::new(scope.schema, scope.source, scope.now);
        lambda_scope.lambda_variables.clone_from(&scope.lambda_variables);
        lambda_scope.lambda_variables.push((parameter.clone(), element));
        let body = lower_expression(body, &lambda_scope)?;
        let lambda = Expr::Lambda(Lambda::new(parameters.clone(), body));
        return lower_function_call(name, vec![list, lambda], &scope.build_function_context());
    }
    let arguments = arguments
        .iter()
        .map(|argument| lower_expression(argument, scope))
        .collect::<Result<Vec<_>, _>>()?;
    lower_function_call(name, arguments, &scope.build_function_context())
}

fn lower_literal(literal: &PplLiteral) -> Expr {
    match literal {
        PplLiteral::String(value) => lit(value.clone()),
        PplLiteral::Integer(value) => lit(*value),
        PplLiteral::Double(value) => lit(*value),
        PplLiteral::Boolean(value) => lit(*value),
    }
}

/// The expression a field reference names, in this order: a lambda parameter in scope; the
/// column of that exact name; a resource attribute the source indices promote to a column; a
/// key of a MAP column, `<column>.<key>`, where the key may contain dots and an absent key is
/// NULL. Names are case-sensitive.
fn resolve_field(name: &str, scope: &ExpressionScope<'_>) -> Result<Expr, PplError> {
    if let Some((parameter, field)) = scope.lambda_variables.iter().rev().find(|(parameter, _)| parameter == name) {
        return Ok(Expr::LambdaVariable(LambdaVariable::new(
            parameter.clone(),
            Some(Arc::clone(field)),
        )));
    }
    if scope.schema.field_with_unqualified_name(name).is_ok() {
        return Ok(ident(name));
    }
    if let Some(column) = find_promoted_column(name, scope)? {
        return Ok(ident(column));
    }
    for (separator, _) in name.match_indices('.') {
        let (column, key) = (&name[..separator], &name[separator + 1..]);
        let is_map = scope
            .schema
            .field_with_unqualified_name(column)
            .is_ok_and(|field| matches!(field.data_type(), DataType::Map(_, _)));
        if is_map {
            return Ok(get_field(ident(column), key.to_string()));
        }
    }
    Err(PplError::FieldNotFound {
        field: name.to_string(),
    })
}

/// The column the source indices read `name` from when their profile promotes it (a resource
/// attribute ingest moved into a column, so the name differs from the column's). Indices that
/// disagree on it are refused: one PPL field cannot read a column in some indices and a MAP key
/// in others. A promoted column an earlier command removed, renamed or replaced is refused too:
/// the MAP never holds the attribute (ingest moved it), and a name an earlier command reassigned
/// holds another value.
fn find_promoted_column(name: &str, scope: &ExpressionScope<'_>) -> Result<Option<String>, PplError> {
    let Some(source) = scope.source else {
        return Ok(None);
    };
    let promoted_columns: Vec<Option<String>> = source
        .entries
        .iter()
        .map(
            |entry| match source.session.resolve_field(entry, name).map(|field| field.value) {
                Some(FieldValue::Column { column }) if column != name => Some(column),
                Some(FieldValue::Column { .. } | FieldValue::MapKey { .. } | FieldValue::HexId { .. }) | None => None,
            },
        )
        .collect();
    let Some(first_column) = promoted_columns.first().cloned().flatten() else {
        if promoted_columns.iter().any(Option::is_some) {
            return Err(build_promoted_conflict_error(name));
        }
        return Ok(None);
    };
    if promoted_columns.iter().any(|column| column.as_ref() != Some(&first_column)) {
        return Err(build_promoted_conflict_error(name));
    }
    if scope.schema.field_with_unqualified_name(&first_column).is_err()
        || source.replaced_columns.contains(&first_column)
    {
        return Err(PplError::Semantic {
            reason: format!(
                "field [{name}] reads the column [{first_column}], which an earlier command removed, renamed or replaced"
            ),
        });
    }
    Ok(Some(first_column))
}

/// The refusal of a promoted field the source indices disagree on.
fn build_promoted_conflict_error(name: &str) -> PplError {
    PplError::Semantic {
        reason: format!("field [{name}] reads a column in some source indices and not in others"),
    }
}

/// Refuse comparing a fixed-size binary id with a value of another type: the engine would
/// compare the value's bytes, not the hex the id renders as.
fn refuse_id_comparison(left: &Expr, right: &Expr, scope: &ExpressionScope<'_>) -> Result<(), PplError> {
    let left_type = left.get_type(scope.schema)?;
    let right_type = right.get_type(scope.schema)?;
    let is_id = |data_type: &DataType| matches!(data_type, DataType::FixedSizeBinary(_));
    if is_id(&left_type) == is_id(&right_type) {
        return Ok(());
    }
    let id = if is_id(&left_type) { left } else { right };
    Err(PplError::Semantic {
        reason: format!(
            "comparing the id field [{}] with a value of another type is not supported",
            id.schema_name()
        ),
    })
}

/// Folds a string literal compared with a timestamp operand into a microsecond timestamp literal,
/// read by [`parse_timestamp_literal`] and carrying the operand's zone.
///
/// Folding here, rather than leaving the comparison to DataFusion's coercion, gives the literal the
/// microsecond unit of the stored columns: the comparison stays a literal comparison in the stored
/// unit, which the WAL and the Iceberg scans can prune with, and a literal finer than a microsecond
/// is a 400 instead of a truncation. Operands of any other type pair are returned unchanged.
///
/// # Errors
///
/// `PplError::Semantic` when the string is not a timestamp literal, and when the type of the operand
/// compared with a string literal cannot be computed against `schema` (an ill-typed expression of
/// the query).
pub(crate) fn coerce_timestamp_operands(left: Expr, right: Expr, schema: &DFSchema) -> Result<(Expr, Expr), PplError> {
    let left = fold_string_operand(left, &right, schema)?;
    let right = fold_string_operand(right, &left, schema)?;
    Ok((left, right))
}

/// `operand` as a timestamp literal when it is a string literal and `other` a timestamp; otherwise
/// unchanged. The type of `other` is computed only for a string literal.
fn fold_string_operand(operand: Expr, other: &Expr, schema: &DFSchema) -> Result<Expr, PplError> {
    let Some(text) = match_string_literal(&operand) else {
        return Ok(operand);
    };
    let DataType::Timestamp(_, time_zone) = other.get_type(schema)? else {
        return Ok(operand);
    };
    Ok(lit(ScalarValue::TimestampMicrosecond(
        Some(parse_timestamp_literal(text)?),
        time_zone,
    )))
}

/// Build `builder` without its hidden order columns, run the analyzer so a type error is reported
/// as the query's error rather than at execution, and describe the output columns.
fn finish_plan(builder: LogicalPlanBuilder) -> Result<PplPlan, PplError> {
    let has_order_columns = builder.schema().fields().iter().any(|field| is_order_column(field.name()));
    let builder = if has_order_columns {
        let output_columns: Vec<Expr> = builder
            .schema()
            .columns()
            .into_iter()
            .filter(|column| !is_order_column(&column.name))
            .map(Expr::Column)
            .collect();
        builder.project(output_columns)?
    } else {
        builder
    };
    let plan = Analyzer::new().execute_and_check(builder.build()?, &ConfigOptions::default(), |_, _| {})?;
    let columns = plan
        .schema()
        .fields()
        .iter()
        .map(|field| {
            Ok(PplColumn {
                name: field.name().clone(),
                ppl_type: PplType::from_data_type(field.name(), field.data_type())?,
            })
        })
        .collect::<Result<Vec<_>, PplError>>()?;
    Ok(PplPlan { plan, columns })
}

/// The jdbc type of one `describe` column.
#[derive(Debug, Clone, Copy)]
enum DescribeColumnType {
    /// Rendered as `string`.
    Text,
    /// Rendered as `int`.
    Integer,
}

impl DescribeColumnType {
    /// The Arrow type a `describe` row carries in this column.
    const fn data_type(self) -> DataType {
        match self {
            Self::Text => DataType::Utf8,
            Self::Integer => DataType::Int32,
        }
    }
}

/// The JDBC `getColumns` columns of a `describe` row, in order. [`build_describe_row`] emits its
/// values in exactly this order.
const DESCRIBE_COLUMNS: [(&str, DescribeColumnType); 24] = [
    ("TABLE_CAT", DescribeColumnType::Text),
    ("TABLE_SCHEM", DescribeColumnType::Text),
    ("TABLE_NAME", DescribeColumnType::Text),
    ("COLUMN_NAME", DescribeColumnType::Text),
    ("DATA_TYPE", DescribeColumnType::Integer),
    ("TYPE_NAME", DescribeColumnType::Text),
    ("COLUMN_SIZE", DescribeColumnType::Integer),
    ("BUFFER_LENGTH", DescribeColumnType::Integer),
    ("DECIMAL_DIGITS", DescribeColumnType::Integer),
    ("NUM_PREC_RADIX", DescribeColumnType::Integer),
    ("NULLABLE", DescribeColumnType::Integer),
    ("REMARKS", DescribeColumnType::Text),
    ("COLUMN_DEF", DescribeColumnType::Text),
    ("SQL_DATA_TYPE", DescribeColumnType::Integer),
    ("SQL_DATETIME_SUB", DescribeColumnType::Integer),
    ("CHAR_OCTET_LENGTH", DescribeColumnType::Integer),
    ("ORDINAL_POSITION", DescribeColumnType::Integer),
    ("IS_NULLABLE", DescribeColumnType::Text),
    ("SCOPE_CATALOG", DescribeColumnType::Text),
    ("SCOPE_SCHEMA", DescribeColumnType::Text),
    ("SCOPE_TABLE", DescribeColumnType::Text),
    // Always null; typed `int` because `PplType` has no `smallint`.
    ("SOURCE_DATA_TYPE", DescribeColumnType::Integer),
    ("IS_AUTOINCREMENT", DescribeColumnType::Text),
    ("IS_GENERATEDCOLUMN", DescribeColumnType::Text),
];

/// `NUM_PREC_RADIX` of every `describe` row.
const DESCRIBE_NUM_PREC_RADIX: i32 = 10;

/// `NULLABLE` of every `describe` row: JDBC `columnNullableUnknown`, since no field promises
/// presence.
const DESCRIBE_NULLABLE: i32 = 2;

/// Plans `describe <index>`: one row per field of the index's field model, in field-model order
/// (schema column order, each MAP column followed by its keys in byte order, keys never split).
///
/// The rows are no index's columns, so the pipeline carries no source binding and no order.
///
/// # Errors
///
/// `IndexNotFound` for an unknown name; `Semantic` when the expression names system indices or
/// more than one data index.
async fn plan_describe<'session>(
    index: &str,
    session: &'session TenantSession,
    now: DateTime<Utc>,
    cluster_name: &str,
) -> Result<Pipeline<'session>, PplError> {
    let entry = match resolve_data_entries(index, session).await?.as_slice() {
        [entry] => *entry,
        matched_entries => {
            let names: Vec<&str> = matched_entries.iter().map(|entry| entry.name()).collect();
            return Err(PplError::Semantic {
                reason: format!("describe reads one index; [{index}] resolves to [{}]", names.join(", ")),
            });
        }
    };
    let fields = session.list_fields(entry, now, FieldListing::WithMapKeys).await?;
    let builder = build_describe_plan(
        cluster_name,
        index,
        fields.iter().map(|field: &IndexField| (field.name.as_str(), field.field_type)),
    )?;
    Ok(Pipeline {
        builder,
        source: None,
        sort_keys: Vec::new(),
    })
}

/// The `VALUES` plan of the `describe` rows, so later commands (`where`, `fields`, `sort`) apply to
/// it like to any other source. Its columns carry the [`DESCRIBE_COLUMNS`] names and types.
///
/// # Errors
///
/// `Internal` when DataFusion refuses the plan: IceGate builds it from constants, so a refusal is a
/// defect, never the query's.
fn build_describe_plan<'a>(
    cluster_name: &str,
    index: &str,
    fields: impl Iterator<Item = (&'a str, FieldType)>,
) -> Result<LogicalPlanBuilder, PplError> {
    let columns: Vec<Field> = DESCRIBE_COLUMNS
        .iter()
        .map(|(name, column_type)| Field::new(*name, column_type.data_type(), true))
        .collect();
    let report_defect = |error: DataFusionError| PplError::Internal {
        reason: format!("the describe plan failed: {error}"),
    };
    let schema: DFSchemaRef = Arc::new(DFSchema::try_from(Schema::new(columns)).map_err(report_defect)?);
    let rows = fields
        .enumerate()
        .map(|(position, (name, field_type))| build_describe_row(cluster_name, index, name, field_type, position))
        .collect::<Result<Vec<_>, PplError>>()?;
    // A `VALUES` plan needs at least one row; an index without fields answers the empty relation,
    // which carries the named schema itself.
    if rows.is_empty() {
        return Ok(LogicalPlanBuilder::from(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema,
        })));
    }
    // `values_with_schema` takes only the types of `schema` and names its columns `column1`,
    // `column2`, …, so a projection gives them the JDBC names.
    let names = DESCRIBE_COLUMNS
        .iter()
        .enumerate()
        .map(|(position, (name, _))| col(format!("column{}", position + 1)).alias(*name));
    LogicalPlanBuilder::values_with_schema(rows, &schema)
        .and_then(|values| values.project(names))
        .map_err(report_defect)
}

/// One `describe` row, in [`DESCRIBE_COLUMNS`] order.
fn build_describe_row(
    cluster_name: &str,
    index: &str,
    field_name: &str,
    field_type: FieldType,
    position: usize,
) -> Result<Vec<Expr>, PplError> {
    let ordinal = i32::try_from(position).map_err(|_| PplError::Semantic {
        reason: format!("[{index}] has more fields than describe can number"),
    })?;
    let build_text_literal = |value: &str| lit(ScalarValue::Utf8(Some(value.to_owned())));
    let build_null_text = || lit(ScalarValue::Utf8(None));
    let build_integer_literal = |value: i32| lit(ScalarValue::Int32(Some(value)));
    let build_null_integer = || lit(ScalarValue::Int32(None));
    Ok(vec![
        build_text_literal(cluster_name),
        build_null_text(),
        build_text_literal(index),
        build_text_literal(field_name),
        build_null_integer(),
        build_text_literal(name_describe_type(field_type)),
        build_null_integer(),
        build_null_integer(),
        build_null_integer(),
        build_integer_literal(DESCRIBE_NUM_PREC_RADIX),
        build_integer_literal(DESCRIBE_NULLABLE),
        build_null_text(),
        build_null_text(),
        build_null_integer(),
        build_null_integer(),
        build_null_integer(),
        build_integer_literal(ordinal),
        build_text_literal(""),
        build_null_text(),
        build_null_text(),
        build_null_text(),
        build_null_integer(),
        build_text_literal("NO"),
        build_text_literal(""),
    ])
}

/// The PPL type name `describe` reports for a field-model type.
const fn name_describe_type(field_type: FieldType) -> &'static str {
    match field_type {
        FieldType::Text | FieldType::Keyword => "string",
        FieldType::Date => "timestamp",
        FieldType::Integer => "int",
        FieldType::Long => "bigint",
        FieldType::Double => "double",
        FieldType::Boolean => "boolean",
        FieldType::Object => "struct",
    }
}

/// Whether a type holds text.
const fn is_string_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)
}

/// Plans `parse`: every named group becomes a string column computed from the source value.
/// Answers the plan and the names of the group columns, which replace or extend the input's
/// columns. Each group column costs one match of the pattern per row.
///
/// # Errors
///
/// `Semantic` for a pattern the regex dialect refuses, a pattern without a named group, a group
/// name with the reserved prefix, or a source that is not a string.
fn plan_parse(
    builder: LogicalPlanBuilder,
    command: &ParseCommand,
    scope: &ExpressionScope<'_>,
) -> Result<(LogicalPlanBuilder, Vec<String>), PplError> {
    let source = lower_expression(&command.source, scope)?;
    let source_type = source.get_type(scope.schema)?;
    if !is_string_type(&source_type) {
        return Err(PplError::Semantic {
            reason: format!("parse needs a string source; {source} has type {source_type}"),
        });
    }
    let refuse_pattern = |error: regex::Error| PplError::Semantic {
        reason: format!(
            "parse pattern '{}' is not a supported regular expression: {error}",
            command.pattern
        ),
    };
    // The pattern compiles on its own first: an unbalanced pattern such as `x)|(?<a>.*` would
    // otherwise close the wrapping group below and escape both anchors.
    let regex = Regex::new(&command.pattern).map_err(refuse_pattern)?;
    let groups: Vec<&str> = regex.capture_names().flatten().collect();
    if groups.is_empty() {
        return Err(PplError::Semantic {
            reason: format!("parse pattern '{}' has no named group (?<name>…)", command.pattern),
        });
    }
    if let Some(group) = groups.iter().find(|group| group.starts_with(RESERVED_NAME_PREFIX)) {
        return Err(PplError::Semantic {
            reason: format!("parse group [{group}] starts with the reserved prefix [{RESERVED_NAME_PREFIX}]"),
        });
    }
    // The whole value must match, so the pattern is anchored at both ends; the non-capturing group
    // keeps an alternation inside the pattern under both anchors and leaves group numbers
    // unchanged. A value the pattern does not match falls to the second branch, which matches any
    // whole value and takes part in no group, so every group's replacement there is "". The regex
    // dialect's alternation is leftmost-first, so a value the pattern matches keeps its groups.
    let extraction_pattern = format!("^(?:{})$|^(?s:.*)$", command.pattern);
    // Compiling the wrapped pattern refuses a pattern whose end the wrapping cannot follow, such
    // as one ending in an `(?x)` comment, which would swallow the closing parenthesis.
    Regex::new(&extraction_pattern).map_err(refuse_pattern)?;
    let mut projection = Vec::with_capacity(scope.schema.fields().len() + groups.len());
    for (qualifier, field) in scope.schema.iter() {
        let name = field.name().as_str();
        projection.push(if groups.contains(&name) {
            extract_named_group(&source, &extraction_pattern, name).alias(name)
        } else {
            Expr::Column(Column::from((qualifier, field)))
        });
    }
    for group in &groups {
        if !scope.schema.has_column_with_unqualified_name(group) {
            projection.push(extract_named_group(&source, &extraction_pattern, group).alias(*group));
        }
    }
    let group_names = groups.iter().map(|group| (*group).to_string()).collect();
    Ok((builder.project(projection)?, group_names))
}

/// The value of one named group: NULL for a NULL source, the group's text when the pattern matches
/// (`""` for a group that did not take part), `""` when it does not match (the catch-all branch of
/// `extraction_pattern`). A whole-value `regexp_replace` with `${name}` yields exactly that text.
fn extract_named_group(source: &Expr, extraction_pattern: &str, group: &str) -> Expr {
    regexp_replace(
        source.clone(),
        lit(extraction_pattern),
        lit(format!("${{{group}}}")),
        None,
    )
}

/// Plans `fillnull`: each selected column becomes `coalesce(column, replacement)` under its own
/// name. Answers the plan and the names of the filled columns. Hidden order columns are never
/// output columns, so a `fillnull` without a field list leaves them alone.
///
/// # Errors
///
/// `FieldNotFound` for a listed field that is not an output column; `Semantic` for a field listed
/// twice or a replacement whose type the column does not accept.
fn plan_fillnull(
    builder: LogicalPlanBuilder,
    command: &FillnullCommand,
    scope: &ExpressionScope<'_>,
) -> Result<(LogicalPlanBuilder, Vec<String>), PplError> {
    let schema = scope.schema;
    let replacements: Vec<(String, Expr)> = match &command.replacements {
        FillnullReplacements::AllFields(replacement) => {
            let value = lower_expression(replacement, scope)?;
            schema
                .fields()
                .iter()
                .filter(|field| !is_order_column(field.name()))
                .map(|field| (field.name().clone(), value.clone()))
                .collect()
        }
        FillnullReplacements::Fields(pairs) => {
            let mut lowered_replacements: Vec<(String, Expr)> = Vec::with_capacity(pairs.len());
            for (field, replacement) in pairs {
                if !schema.has_column_with_unqualified_name(field) {
                    return Err(PplError::FieldNotFound { field: field.clone() });
                }
                if lowered_replacements.iter().any(|(listed, _)| listed == field) {
                    return Err(PplError::Semantic {
                        reason: format!("fillnull lists field '{field}' more than once"),
                    });
                }
                lowered_replacements.push((field.clone(), lower_expression(replacement, scope)?));
            }
            lowered_replacements
        }
    };
    let mut projection = Vec::with_capacity(schema.fields().len());
    for (qualifier, field) in schema.iter() {
        let column = Expr::Column(Column::from((qualifier, field)));
        match replacements.iter().find(|(name, _)| name == field.name()) {
            Some((_, replacement)) => {
                let typed_replacement = cast_fillnull_replacement(replacement, field, schema)?;
                projection.push(coalesce(vec![column, typed_replacement]).alias(field.name()));
            }
            None => projection.push(column),
        }
    }
    let filled = replacements.into_iter().map(|(name, _)| name).collect();
    Ok((builder.project(projection)?, filled))
}

/// `replacement` cast to the type of `field` after checking that the column accepts it: strings
/// take strings, numbers the numbers [`is_numeric_replacement_accepted`] admits, booleans
/// booleans and timestamps timestamps. An id column (fixed-size binary), a MAP, struct or list
/// column takes nothing.
///
/// # Errors
///
/// `Semantic` naming the field and the PPL types of both (the Arrow type where PPL has none); for
/// a number the column refuses, naming the field, its type and the replacement value.
fn cast_fillnull_replacement(replacement: &Expr, field: &Field, schema: &DFSchema) -> Result<Expr, PplError> {
    let replacement_type = replacement.get_type(schema)?;
    let target = field.data_type();
    if target.is_numeric() && replacement_type.is_numeric() {
        if !is_numeric_replacement_accepted(replacement, &replacement_type, target) {
            return Err(PplError::Semantic {
                reason: format!(
                    "fillnull: field '{}' (type: {}) cannot hold the replacement value {} (type: {})",
                    field.name(),
                    name_ppl_type(field.name(), target),
                    replacement.human_display(),
                    name_ppl_type("replacement", &replacement_type)
                ),
            });
        }
        return Ok(cast(replacement.clone(), target.clone()));
    }
    let is_accepted = (is_string_type(target) && is_string_type(&replacement_type))
        || (*target == DataType::Boolean && replacement_type == DataType::Boolean)
        || (matches!(target, DataType::Timestamp(..)) && matches!(replacement_type, DataType::Timestamp(..)));
    if !is_accepted {
        let id_clause = if matches!(target, DataType::FixedSizeBinary(_)) {
            "; id columns accept no replacement"
        } else {
            ""
        };
        return Err(PplError::Semantic {
            reason: format!(
                "fillnull: replacement value type {} is not compatible with field '{}' (type: {}){id_clause}",
                name_ppl_type("replacement", &replacement_type),
                field.name(),
                name_ppl_type(field.name(), target)
            ),
        });
    }
    Ok(cast(replacement.clone(), target.clone()))
}

/// Whether the numeric column type `target` accepts the numeric `replacement` (of type
/// `replacement_type`), decided while planning, so the cast of an integer column's replacement never
/// truncates and never fails when the query runs.
///
/// An integer column takes only an integer it holds exactly: a literal inside the column's range,
/// or a computed integer whose type has no value outside that range. A fraction, a literal outside
/// the range and a computed integer of a wider type are refused. A floating-point or decimal column
/// takes any number, rounded to its type; only a literal the type cannot represent at all (outside
/// its range) is refused.
fn is_numeric_replacement_accepted(replacement: &Expr, replacement_type: &DataType, target: &DataType) -> bool {
    let literal = match replacement {
        Expr::Literal(value, _) => Some(value),
        _ => None,
    };
    if literal.is_some_and(|value| value.cast_to_with_options(target, &DEFAULT_CAST_OPTIONS).is_err()) {
        return false;
    }
    if !target.is_integer() {
        return true;
    }
    replacement_type.is_integer() && (literal.is_some() || is_integer_type_within(replacement_type, target))
}

/// Whether every value of the integer type `source` is a value of the integer type `target`.
fn is_integer_type_within(source: &DataType, target: &DataType) -> bool {
    let (Some(source_width), Some(target_width)) = (source.primitive_width(), target.primitive_width()) else {
        return false;
    };
    if source.is_signed_integer() == target.is_signed_integer() {
        source_width <= target_width
    } else {
        source.is_unsigned_integer() && source_width < target_width
    }
}

/// The PPL type name of `data_type`, or its Arrow name when PPL has no type for it.
fn name_ppl_type(column: &str, data_type: &DataType) -> String {
    PplType::from_data_type(column, data_type)
        .map_or_else(|_| data_type.to_string(), |ppl_type| ppl_type.name().to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::{DateTime, Datelike, Months, NaiveTime, TimeZone, Utc, Weekday};
    use datafusion::arrow::array::{
        Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float64Array, Int32Array, Int64Array, RecordBatch,
        StringArray, TimestampMicrosecondArray,
    };
    use datafusion::arrow::compute::concat_batches;
    use datafusion::arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit, TimestampMicrosecondType};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::error::DataFusionError;
    use datafusion::logical_expr::{
        Between, Expr, LogicalPlan, LogicalPlanBuilder, LogicalTableSource, col, ident, lit,
    };
    use datafusion::prelude::SessionContext;
    use icegate_common::schema::COL_TENANT_ID;
    use icegate_common::{OPERATIONS_TABLE, TenantId};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::{
        ExpressionScope, PplColumn, PplType, aggregate_rows, bin_time, bucket_time, build_describe_plan,
        coerce_timestamp_operands, lower_expression, plan_fillnull, plan_ppl, project_source_columns, select_fields,
        union_branches,
    };
    use crate::opensearch::catalog::FieldType;
    use crate::opensearch::context::RequestContext;
    use crate::ppl::ast::{FillnullCommand, PplCommand, PplExpr, PplLiteral, SpanUnit};
    use crate::ppl::error::PplError;
    use crate::ppl::functions::{FunctionContext, lower_function_call};
    use crate::ppl::parse_ppl;
    use crate::test_rows::{FINISH_REASONS, build_operation_row};
    use crate::test_support::{SearchFixture, TestCell};

    const TENANT_A: &str = "tenant-a";
    const TENANT_B: &str = "tenant-b";
    /// 2026-10-08T12:00:00Z.
    const NOON_MICROS: i64 = 1_791_460_800_000_000;
    /// Microseconds in a day, stated independently of the planner's constants.
    const DAY_MICROS: i64 = 86_400_000_000;

    /// How one bucket of a unit is measured: a fixed number of microseconds, weeks from Monday,
    /// or a number of calendar months.
    enum BucketStep {
        Fixed(i64),
        Weeks,
        Months(u32),
    }

    /// The step of `unit`, from the unit definitions (1 ms = 1 000 µs, 1 cs = 10 000 µs, …).
    fn read_bucket_step(unit: SpanUnit) -> BucketStep {
        match unit {
            SpanUnit::Microsecond => BucketStep::Fixed(1),
            SpanUnit::Millisecond => BucketStep::Fixed(1_000),
            SpanUnit::Centisecond => BucketStep::Fixed(10_000),
            SpanUnit::Decisecond => BucketStep::Fixed(100_000),
            SpanUnit::Second => BucketStep::Fixed(1_000_000),
            SpanUnit::Minute => BucketStep::Fixed(60_000_000),
            SpanUnit::Hour => BucketStep::Fixed(3_600_000_000),
            SpanUnit::Day => BucketStep::Fixed(DAY_MICROS),
            SpanUnit::Week => BucketStep::Weeks,
            SpanUnit::Month => BucketStep::Months(1),
            SpanUnit::Quarter => BucketStep::Months(3),
            SpanUnit::Year => BucketStep::Months(12),
            SpanUnit::Numeric => panic!("a numeric span has no time step"),
        }
    }

    /// `count` instants drawn with `rng` from 1900-01-01 (or the epoch, with `is_after_epoch`) to
    /// 2200-01-01, UTC.
    fn draw_instants(rng: &mut StdRng, count: usize, is_after_epoch: bool) -> Vec<i64> {
        let lower = if is_after_epoch {
            0
        } else {
            Utc.with_ymd_and_hms(1900, 1, 1, 0, 0, 0)
                .single()
                .expect("1900")
                .timestamp_micros()
        };
        let upper = Utc
            .with_ymd_and_hms(2200, 1, 1, 0, 0, 0)
            .single()
            .expect("2200")
            .timestamp_micros();
        (0..count).map(|_| rng.random_range(lower..upper)).collect()
    }

    /// `expression` over a column `t` holding `instants`, evaluated by DataFusion.
    async fn evaluate_over_instants(instants: &[i64], expression: Expr) -> ArrayRef {
        let column: ArrayRef = Arc::new(TimestampMicrosecondArray::from(instants.to_vec()));
        let batch = RecordBatch::try_from_iter([("t", column)]).expect("batch");
        let batches = SessionContext::new()
            .read_batch(batch)
            .expect("frame")
            .select(vec![expression.alias("bucket")])
            .expect("select")
            .collect()
            .await
            .expect("evaluate");
        let schema = batches.first().expect("at least one batch").schema();
        let merged_batch = concat_batches(&schema, &batches).expect("concatenate");
        Arc::clone(merged_batch.column(0))
    }

    /// The month index of `instant`: months since January 1970.
    fn read_month_index(instant: DateTime<Utc>) -> i64 {
        (i64::from(instant.year()) - 1970) * 12 + i64::from(instant.month0())
    }

    #[test]
    fn fields_minus_projects_only_the_remaining_columns_at_the_scan() {
        let table = Schema::new(vec![
            Field::new(COL_TENANT_ID, DataType::Utf8, false),
            Field::new("service_name", DataType::Utf8, true),
            Field::new("severity_text", DataType::Utf8, true),
            Field::new("body", DataType::Utf8, true),
        ]);
        let scan =
            LogicalPlanBuilder::scan("logs", Arc::new(LogicalTableSource::new(Arc::new(table))), None).expect("scan");
        let source = project_source_columns(scan).expect("source columns");
        let schema = Arc::clone(source.schema());
        let request_time = build_fixed_now();
        let scope = ExpressionScope::new(&schema, None, request_time);
        let dropped_names = ["severity_text".to_string(), "body".to_string()];
        let plan = select_fields(source, true, &dropped_names, &scope)
            .expect("fields -")
            .build()
            .expect("plan");
        let optimized_plan = SessionContext::new().state().optimize(&plan).expect("optimize");
        let mut scanned_columns = Vec::new();
        optimized_plan
            .apply(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    scanned_columns = scan
                        .projected_schema
                        .fields()
                        .iter()
                        .map(|field| field.name().clone())
                        .collect();
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .expect("walk");
        assert_eq!(scanned_columns, vec!["service_name".to_string()]);
    }

    /// Every table numbers its Iceberg fields on its own, so a map column read from two indices
    /// carries other field ids on its key and value fields in each. Every branch of the union
    /// still produces that column with the first index's type, field ids included, and a column
    /// one index lacks with the other index's type.
    #[test]
    fn union_branches_agree_on_the_first_index_type_when_nested_field_ids_differ() {
        let first_labels = build_labels_field("14", "15");
        let second_labels = build_labels_field("16", "17");
        // The second branch reaches the cast only with types that agree in structure but differ
        // in their nested field metadata.
        assert!(first_labels.data_type().equals_datatype(second_labels.data_type()));
        assert_ne!(first_labels.data_type(), second_labels.data_type());
        let first_table = Schema::new(vec![
            Field::new("count", DataType::Int64, false),
            first_labels.clone(),
            Field::new("message", DataType::Utf8, true),
        ]);
        let second_table = Schema::new(vec![
            Field::new("count", DataType::Int64, false),
            second_labels,
            Field::new("duration", DataType::Float64, true),
        ]);
        let first_scan =
            LogicalPlanBuilder::scan("first", Arc::new(LogicalTableSource::new(Arc::new(first_table))), None)
                .expect("first scan");
        let second_scan = LogicalPlanBuilder::scan(
            "second",
            Arc::new(LogicalTableSource::new(Arc::new(second_table))),
            None,
        )
        .expect("second scan");

        let plan = union_branches(vec![("first", first_scan), ("second", second_scan)])
            .expect("union")
            .build()
            .expect("plan");

        let LogicalPlan::Union(union) = plan else {
            panic!("two indices plan as a union");
        };
        let expected_columns = vec![
            ("count".to_string(), DataType::Int64),
            ("labels".to_string(), first_labels.data_type().clone()),
            ("message".to_string(), DataType::Utf8),
            ("duration".to_string(), DataType::Float64),
        ];
        let branch_columns: Vec<Vec<(String, DataType)>> = union
            .inputs
            .iter()
            .map(|branch| {
                branch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| (field.name().clone(), field.data_type().clone()))
                    .collect()
            })
            .collect();
        assert_eq!(branch_columns, vec![expected_columns.clone(), expected_columns]);
    }

    /// A `MAP<string, string>` field named `labels` whose key and value fields carry the Iceberg
    /// field ids `key_id` and `value_id`.
    fn build_labels_field(key_id: &str, value_id: &str) -> Field {
        let key_field = Field::new("key", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            key_id.to_string(),
        )]));
        let value_field = Field::new("value", DataType::Utf8, true).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            value_id.to_string(),
        )]));
        Field::new_map("labels", "key_value", key_field, value_field, false, true)
    }

    /// A `LIST<string>` column plans as a PPL `array` and reads back the elements stored in the
    /// requesting tenant's row, in stored order; the other tenant's row in the same append is not
    /// read.
    #[tokio::test]
    async fn list_column_plans_as_array_with_stored_elements() {
        let fixture = SearchFixture::new().await;
        fixture
            .commit_rows(
                OPERATIONS_TABLE,
                &[
                    build_operation_row(
                        TENANT_A,
                        "chat",
                        NOON_MICROS,
                        vec![(FINISH_REASONS, TestCell::TextList(&["stop", "length"]))],
                    ),
                    build_operation_row(
                        TENANT_B,
                        "chat",
                        NOON_MICROS,
                        vec![(FINISH_REASONS, TestCell::TextList(&["tool_calls"]))],
                    ),
                ],
            )
            .await;
        let session = fixture.open_session(TENANT_A).await;
        let context = RequestContext {
            tenant: TenantId::new(TENANT_A).expect("a valid tenant"),
            now: build_fixed_now(),
        };
        let query = parse_ppl("source=operations | fields finish_reasons").expect("query");

        let ppl_plan = plan_ppl(&query, &session, &context, "icegate").await.expect("plan");

        assert_eq!(
            ppl_plan.columns,
            vec![PplColumn {
                name: FINISH_REASONS.to_string(),
                ppl_type: PplType::Array,
            }]
        );
        let batches = session.collect(ppl_plan.plan).await.expect("rows");
        let mut lists: Vec<Vec<String>> = Vec::new();
        for batch in &batches {
            let column = batch.column(0).as_list::<i32>();
            for row in 0..column.len() {
                let elements = column.value(row);
                lists.push(
                    elements
                        .as_string::<i32>()
                        .iter()
                        .map(|element| element.expect("a stored element").to_string())
                        .collect(),
                );
            }
        }
        assert_eq!(lists, vec![vec!["stop".to_string(), "length".to_string()]]);
    }

    #[tokio::test]
    async fn every_time_span_bucket_holds_its_instant_and_starts_on_its_unit_boundary() {
        // The rules: fixed units bucket from the Unix epoch; weeks start on Mondays 00:00 UTC,
        // counted from Monday 1969-12-29 (the epoch shifted back three days); months, quarters
        // and years start on the first day of a month whose index since January 1970 is a
        // multiple of the bucket's month count.
        let first_monday = Utc.with_ymd_and_hms(1969, 12, 29, 0, 0, 0).single().expect("monday");
        let units = [
            SpanUnit::Microsecond,
            SpanUnit::Millisecond,
            SpanUnit::Centisecond,
            SpanUnit::Decisecond,
            SpanUnit::Second,
            SpanUnit::Minute,
            SpanUnit::Hour,
            SpanUnit::Day,
            SpanUnit::Week,
            SpanUnit::Month,
            SpanUnit::Quarter,
            SpanUnit::Year,
        ];
        let mut rng = StdRng::seed_from_u64(0x5BA9_0001);
        for unit in units {
            for length in [1_u32, 2, 5] {
                let instants = draw_instants(&mut rng, 200, false);
                let bucket = bucket_time(ident("t"), i64::from(length), unit).expect("time bucket");
                let starts = evaluate_over_instants(&instants, bucket).await;
                let starts = starts.as_primitive::<TimestampMicrosecondType>();
                assert_eq!(starts.null_count(), 0, "unit {unit:?}");
                for (row, &micros) in instants.iter().enumerate() {
                    let start_micros = starts.value(row);
                    let instant = DateTime::from_timestamp_micros(micros).expect("instant");
                    let start = DateTime::from_timestamp_micros(start_micros).expect("start");
                    let context = format!("unit {unit:?}, length {length}, instant {instant}, start {start}");
                    let end = match read_bucket_step(unit) {
                        BucketStep::Fixed(step) => {
                            let width = step * i64::from(length);
                            assert_eq!(start_micros.rem_euclid(width), 0, "{context}");
                            DateTime::from_timestamp_micros(start_micros + width).expect("end")
                        }
                        BucketStep::Weeks => {
                            let width = 7 * DAY_MICROS * i64::from(length);
                            assert_eq!(start.weekday(), Weekday::Mon, "{context}");
                            assert_eq!(start.time(), NaiveTime::MIN, "{context}");
                            let since_first_monday = start_micros - first_monday.timestamp_micros();
                            assert_eq!(since_first_monday.rem_euclid(width), 0, "{context}");
                            DateTime::from_timestamp_micros(start_micros + width).expect("end")
                        }
                        BucketStep::Months(months) => {
                            let total = months * length;
                            assert_eq!((start.day(), start.time()), (1, NaiveTime::MIN), "{context}");
                            assert_eq!(read_month_index(start).rem_euclid(i64::from(total)), 0, "{context}");
                            start.checked_add_months(Months::new(total)).expect("end")
                        }
                    };
                    assert!(start <= instant && instant < end, "{context}, end {end}");
                }
            }
        }
    }

    #[tokio::test]
    async fn every_day_and_month_bin_holds_its_instant_before_and_after_the_epoch() {
        // The stated bin rules: a day bin starts at midnight UTC of day `floor(d / n) * n` after
        // the epoch, a month bin is the `yyyy-MM` of month `floor(i / n) * n` after January 1970.
        // Instants from 1900, so day and month indices before the epoch are negative.
        let mut rng = StdRng::seed_from_u64(0x0B14_0002);
        for length in [1_u32, 2, 3, 7] {
            let instants = draw_instants(&mut rng, 200, false);
            let day_bin = bin_time(ident("t"), i64::from(length), SpanUnit::Day).expect("day bin");
            let days = evaluate_over_instants(&instants, day_bin).await;
            let days = days.as_primitive::<TimestampMicrosecondType>();
            let month_bin = bin_time(ident("t"), i64::from(length), SpanUnit::Month).expect("month bin");
            let months = evaluate_over_instants(&instants, month_bin).await;
            let months = months.as_string::<i32>();
            for (row, &micros) in instants.iter().enumerate() {
                let instant = DateTime::from_timestamp_micros(micros).expect("instant");
                let context = format!("length {length}, instant {instant}");
                let day_start = days.value(row);
                let width = DAY_MICROS * i64::from(length);
                assert_eq!(day_start.rem_euclid(width), 0, "{context}");
                assert!(day_start <= micros && micros < day_start + width, "{context}");
                let label = months.value(row);
                let (year, month) = label.split_once('-').expect("yyyy-MM");
                let year = year.parse::<i64>().expect("year");
                let month = month.parse::<i64>().expect("month");
                let index = (year - 1970) * 12 + month - 1;
                let instant_index = read_month_index(instant);
                assert_eq!(index.rem_euclid(i64::from(length)), 0, "{context}, label {label}");
                assert!(
                    index <= instant_index && instant_index < index + i64::from(length),
                    "{context}, label {label}"
                );
            }
        }
    }

    /// `stats` planned over one in-memory batch whose group key `k` is NULL, `a`, `a`, NULL and
    /// whose value `v` is `x`, `y`, `z`, `x`, in that row order.
    fn plan_stats_over_null_keys(context: &SessionContext, query_text: &str) -> LogicalPlan {
        let keys: ArrayRef = Arc::new(StringArray::from(vec![None, Some("a"), Some("a"), None]));
        let values: ArrayRef = Arc::new(StringArray::from(vec!["x", "y", "z", "x"]));
        let batch = RecordBatch::try_from_iter([("k", keys), ("v", values)]).expect("batch");
        let (_, input) = context.read_batch(batch).expect("frame").into_parts();
        let builder = LogicalPlanBuilder::from(input);
        let schema = Arc::clone(builder.schema());
        let request_time = build_fixed_now();
        let scope = ExpressionScope::new(&schema, None, request_time);
        let query = parse_ppl(query_text).expect("query");
        let [PplCommand::Stats(stats)] = query.commands.as_slice() else {
            panic!("one stats command: {query:?}");
        };
        aggregate_rows(builder, stats, &scope).expect("stats").build().expect("plan")
    }

    #[tokio::test]
    async fn grouped_distinct_counts_and_value_lists_keep_the_null_key_group() {
        let context = SessionContext::new();
        let plan = plan_stats_over_null_keys(
            &context,
            "source=x | stats count() as n, values(v) as vs, dc(v) as d by k",
        );
        let batches = context
            .execute_logical_plan(plan)
            .await
            .expect("frame")
            .collect()
            .await
            .expect("rows");
        let schema = batches.first().expect("at least one batch").schema();
        let merged_batch = concat_batches(&schema, &batches).expect("concatenate");
        let counts: Vec<i64> = merged_batch.column(0).as_primitive::<Int64Type>().values().to_vec();
        let lists = merged_batch.column(1).as_list::<i32>();
        let value_lists: Vec<Vec<String>> = (0..lists.len())
            .map(|row| {
                lists
                    .value(row)
                    .as_string::<i32>()
                    .iter()
                    .map(|value| value.expect("a non-NULL value").to_string())
                    .collect()
            })
            .collect();
        let distinct_counts: Vec<i64> = merged_batch.column(2).as_primitive::<Int64Type>().values().to_vec();
        let group_keys: Vec<Option<&str>> = merged_batch.column(3).as_string::<i32>().iter().collect();
        assert_eq!(counts, vec![2, 2]);
        assert_eq!(
            value_lists,
            vec![vec!["x".to_string()], vec!["y".to_string(), "z".to_string()]]
        );
        assert_eq!(distinct_counts, vec![1, 2]);
        assert_eq!(group_keys, vec![None, Some("a")]);
    }

    #[test]
    fn distinct_counts_and_value_lists_plan_no_distinct_aggregate() {
        let plan = plan_stats_over_null_keys(
            &SessionContext::new(),
            "source=x | stats dc(v) as d, values(v) as vs by k",
        );
        let mut aggregate_names = Vec::new();
        let mut distinct_names = Vec::new();
        plan.apply(|node| {
            node.apply_expressions(|expression| {
                expression.apply(|nested| {
                    if let Expr::AggregateFunction(function) = nested {
                        aggregate_names.push(function.func.name().to_string());
                        if function.params.distinct {
                            distinct_names.push(function.func.name().to_string());
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })
        })
        .expect("walk");
        aggregate_names.sort_unstable();
        aggregate_names.dedup();
        assert_eq!(aggregate_names, vec!["array_agg".to_string(), "count".to_string()]);
        assert_eq!(distinct_names, Vec::<String>::new());
    }

    /// A schema of a microsecond `timestamp` column and a string `body`.
    fn build_timestamp_and_body_schema() -> DFSchema {
        DFSchema::try_from(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("body", DataType::Utf8, true),
        ]))
        .expect("schema")
    }

    /// 2026-10-08T12:00:00Z, the request time of the unit tests that need one.
    fn build_fixed_now() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(NOON_MICROS).expect("noon")
    }

    #[test]
    fn a_string_compared_with_a_timestamp_operand_folds_and_other_operands_stay() {
        // 2026-10-08T12:00:00Z = 1_791_460_800 s and 2026-10-08T00:00:00Z = 1_791_417_600 s after
        // the epoch, computed outside the code.
        let schema = build_timestamp_and_body_schema();
        let (left, right) =
            coerce_timestamp_operands(col("timestamp"), lit("2026-10-08 12:00:00.000001"), &schema).expect("fold");
        assert_eq!(left, col("timestamp"));
        assert_eq!(
            right,
            lit(ScalarValue::TimestampMicrosecond(Some(1_791_460_800_000_001), None))
        );

        let (left, right) =
            coerce_timestamp_operands(lit("2026-10-08"), col("timestamp"), &schema).expect("fold on the left");
        assert_eq!(
            left,
            lit(ScalarValue::TimestampMicrosecond(Some(1_791_417_600_000_000), None))
        );
        assert_eq!(right, col("timestamp"));

        let (left, right) = coerce_timestamp_operands(col("body"), lit("2026-10-08"), &schema).expect("no fold");
        assert_eq!((left, right), (col("body"), lit("2026-10-08")));

        let (_, right) = coerce_timestamp_operands(
            col("timestamp"),
            lit(ScalarValue::Utf8View(Some("2026-10-08".to_owned()))),
            &schema,
        )
        .expect("a Utf8View literal folds");
        assert_eq!(
            right,
            lit(ScalarValue::TimestampMicrosecond(Some(1_791_417_600_000_000), None))
        );

        assert!(
            matches!(
                coerce_timestamp_operands(col("timestamp"), lit("2026-10-08 12:00:00.1234567"), &schema),
                Err(PplError::Semantic { .. })
            ),
            "seven fraction digits must be refused, not truncated"
        );
    }

    #[test]
    fn between_bounds_and_in_items_compared_with_a_timestamp_fold_to_microsecond_literals() {
        let schema = build_timestamp_and_body_schema();
        let scope = ExpressionScope::new(&schema, None, build_fixed_now());
        let timestamp_field = || Box::new(PplExpr::Field("timestamp".to_owned()));
        let string_literal = |text: &str| PplExpr::Literal(PplLiteral::String(text.to_owned()));
        let midnight = lit(ScalarValue::TimestampMicrosecond(Some(1_791_417_600_000_000), None));
        let noon_and_a_microsecond = lit(ScalarValue::TimestampMicrosecond(Some(1_791_460_800_000_001), None));

        let between = PplExpr::Between {
            value: timestamp_field(),
            low: Box::new(string_literal("2026-10-08")),
            high: Box::new(string_literal("2026-10-08 12:00:00.000001")),
            is_negated: false,
        };
        assert_eq!(
            lower_expression(&between, &scope).expect("between folds"),
            Expr::Between(Between::new(
                Box::new(ident("timestamp")),
                false,
                Box::new(midnight.clone()),
                Box::new(noon_and_a_microsecond.clone()),
            ))
        );

        let membership = PplExpr::In {
            value: timestamp_field(),
            list: vec![
                string_literal("2026-10-08"),
                string_literal("2026-10-08 12:00:00.000001"),
            ],
            is_negated: true,
        };
        assert_eq!(
            lower_expression(&membership, &scope).expect("in folds"),
            ident("timestamp").in_list(vec![midnight, noon_and_a_microsecond], true)
        );

        let string_membership = PplExpr::In {
            value: Box::new(PplExpr::Field("body".to_owned())),
            list: vec![string_literal("2026-10-08")],
            is_negated: false,
        };
        assert_eq!(
            lower_expression(&string_membership, &scope).expect("a string list stays"),
            ident("body").in_list(vec![lit("2026-10-08")], false)
        );

        for refused in [
            PplExpr::Between {
                value: timestamp_field(),
                low: Box::new(string_literal("2026-10-08 12:00:00.1234567")),
                high: Box::new(string_literal("2026-10-09")),
                is_negated: false,
            },
            PplExpr::In {
                value: timestamp_field(),
                list: vec![string_literal("2026-10-08 12:00:00.1234567")],
                is_negated: false,
            },
        ] {
            assert!(
                matches!(lower_expression(&refused, &scope), Err(PplError::Semantic { .. })),
                "{refused:?} must be refused"
            );
        }
    }

    #[test]
    fn a_timestamp_call_folds_a_literal_of_every_string_type_and_refuses_seven_fraction_digits() {
        let schema = DFSchema::empty();
        let functions = FunctionContext {
            schema: &schema,
            now: build_fixed_now(),
        };
        for argument in [
            ScalarValue::Utf8(Some("2026-10-08 12:00:00.000001".to_owned())),
            ScalarValue::LargeUtf8(Some("2026-10-08 12:00:00.000001".to_owned())),
            ScalarValue::Utf8View(Some("2026-10-08 12:00:00.000001".to_owned())),
        ] {
            let folded = lower_function_call("timestamp", vec![lit(argument.clone())], &functions).expect("folds");
            assert_eq!(
                folded,
                lit(ScalarValue::TimestampMicrosecond(Some(1_791_460_800_000_001), None)),
                "{argument:?}"
            );
        }
        for argument in [
            ScalarValue::Utf8(Some("2026-10-08 12:00:00.1234567".to_owned())),
            ScalarValue::Utf8View(Some("2026-10-08 12:00:00.1234567".to_owned())),
        ] {
            assert!(
                matches!(
                    lower_function_call("timestamp", vec![lit(argument.clone())], &functions),
                    Err(PplError::Semantic { .. })
                ),
                "{argument:?} must be refused"
            );
        }
    }

    #[test]
    fn the_describe_plan_names_and_types_its_columns_as_the_jdbc_columns() {
        // The JDBC `getColumns` columns, in order; `int` columns are 32-bit, the rest strings.
        let expected_columns: [(&str, DataType); 24] = [
            ("TABLE_CAT", DataType::Utf8),
            ("TABLE_SCHEM", DataType::Utf8),
            ("TABLE_NAME", DataType::Utf8),
            ("COLUMN_NAME", DataType::Utf8),
            ("DATA_TYPE", DataType::Int32),
            ("TYPE_NAME", DataType::Utf8),
            ("COLUMN_SIZE", DataType::Int32),
            ("BUFFER_LENGTH", DataType::Int32),
            ("DECIMAL_DIGITS", DataType::Int32),
            ("NUM_PREC_RADIX", DataType::Int32),
            ("NULLABLE", DataType::Int32),
            ("REMARKS", DataType::Utf8),
            ("COLUMN_DEF", DataType::Utf8),
            ("SQL_DATA_TYPE", DataType::Int32),
            ("SQL_DATETIME_SUB", DataType::Int32),
            ("CHAR_OCTET_LENGTH", DataType::Int32),
            ("ORDINAL_POSITION", DataType::Int32),
            ("IS_NULLABLE", DataType::Utf8),
            ("SCOPE_CATALOG", DataType::Utf8),
            ("SCOPE_SCHEMA", DataType::Utf8),
            ("SCOPE_TABLE", DataType::Utf8),
            ("SOURCE_DATA_TYPE", DataType::Int32),
            ("IS_AUTOINCREMENT", DataType::Utf8),
            ("IS_GENERATEDCOLUMN", DataType::Utf8),
        ];
        let expected: Vec<(&str, &DataType)> =
            expected_columns.iter().map(|(name, data_type)| (*name, data_type)).collect();
        for fields in [
            vec![],
            vec![("body", FieldType::Text), ("log_attributes", FieldType::Object)],
        ] {
            let builder = build_describe_plan("icegate", "logs", fields.into_iter()).expect("describe plan");
            let columns: Vec<(&str, &DataType)> = builder
                .schema()
                .fields()
                .iter()
                .map(|field| (field.name().as_str(), field.data_type()))
                .collect();
            assert_eq!(columns, expected);
        }
    }

    /// The one `fillnull` command of `source=t | <fillnull_text>`.
    fn parse_fillnull_command(fillnull_text: &str) -> FillnullCommand {
        let query = parse_ppl(&format!("source=t | {fillnull_text}")).expect("parse");
        let [PplCommand::Fillnull(command)] = query.commands.as_slice() else {
            panic!("{fillnull_text} is one fillnull command");
        };
        command.clone()
    }

    /// `fillnull_text` planned over `builder`.
    fn plan_fillnull_text(builder: LogicalPlanBuilder, fillnull_text: &str) -> Result<LogicalPlanBuilder, PplError> {
        let schema = Arc::clone(builder.schema());
        let scope = ExpressionScope::new(&schema, None, build_fixed_now());
        plan_fillnull(builder, &parse_fillnull_command(fillnull_text), &scope).map(|(planned, _)| planned)
    }

    /// One row of values and one row of NULLs, of an `int`, a `bigint`, a `double`, a
    /// `Decimal128(10, 2)`, a `boolean`, a microsecond `timestamp` and a `string` column.
    fn build_fillnull_batch() -> RecordBatch {
        let price = Decimal128Array::from(vec![Some(150), None])
            .with_precision_and_scale(10, 2)
            .expect("decimal");
        let columns: [(&str, ArrayRef); 7] = [
            ("retries", Arc::new(Int32Array::from(vec![Some(1), None]))),
            ("bytes", Arc::new(Int64Array::from(vec![Some(1), None]))),
            ("ratio", Arc::new(Float64Array::from(vec![Some(0.5), None]))),
            ("price", Arc::new(price)),
            ("cached", Arc::new(BooleanArray::from(vec![Some(true), None]))),
            ("seen", Arc::new(TimestampMicrosecondArray::from(vec![Some(1), None]))),
            ("note", Arc::new(StringArray::from(vec![Some("kept"), None]))),
        ];
        RecordBatch::try_from_iter(columns).expect("batch")
    }

    /// `fillnull_text` over [`build_fillnull_batch`], executed by DataFusion.
    async fn execute_fillnull(fillnull_text: &str) -> Result<RecordBatch, DataFusionError> {
        let context = SessionContext::new();
        let frame = context.read_batch(build_fillnull_batch()).expect("frame");
        let builder = LogicalPlanBuilder::from(frame.logical_plan().clone());
        let plan = plan_fillnull_text(builder, fillnull_text)
            .expect("planned")
            .build()
            .expect("plan");
        let batches = context.execute_logical_plan(plan).await?.collect().await?;
        let schema = batches.first().expect("at least one batch").schema();
        Ok(concat_batches(&schema, &batches).expect("concatenate"))
    }

    #[tokio::test]
    async fn fillnull_fills_each_column_type_with_a_value_it_accepts() {
        // 2026-10-08T12:00:00Z = 1_791_460_800 s after the epoch, computed outside the code; the
        // largest 32-bit integer is 2_147_483_647. 9_007_199_254_740_993 = 2^53 + 1 has no double, so
        // the double column holds the nearest one, 2^53; 0.125 at scale 2 rounds half away from zero
        // to 0.13 (unscaled 13).
        let filled = execute_fillnull(
            "fillnull using retries = 2147483647, bytes = 2 + 3, ratio = 9007199254740993, \
             price = 0.125, cached = false, seen = timestamp('2026-10-08 12:00:00'), note = 'none'",
        )
        .await
        .expect("execute");
        let price = Decimal128Array::from(vec![150, 13])
            .with_precision_and_scale(10, 2)
            .expect("decimal");
        let expected_columns: [(&str, ArrayRef); 7] = [
            ("retries", Arc::new(Int32Array::from(vec![1, 2_147_483_647]))),
            ("bytes", Arc::new(Int64Array::from(vec![1, 5]))),
            (
                "ratio",
                Arc::new(Float64Array::from(vec![0.5, 9_007_199_254_740_992.0])),
            ),
            ("price", Arc::new(price)),
            ("cached", Arc::new(BooleanArray::from(vec![true, false]))),
            (
                "seen",
                Arc::new(TimestampMicrosecondArray::from(vec![1, 1_791_460_800_000_000])),
            ),
            ("note", Arc::new(StringArray::from(vec!["kept", "none"]))),
        ];
        for (name, expected) in expected_columns {
            let column = filled.column_by_name(name).expect("filled column");
            assert_eq!(column, &expected, "{name}");
        }
    }

    #[test]
    fn fillnull_refuses_a_value_the_column_cannot_hold() {
        let mut fields: Vec<Field> = build_fillnull_batch()
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields.push(build_labels_field("1", "2"));
        let table = Arc::new(LogicalTableSource::new(Arc::new(Schema::new(fields))));
        let numeric_refusals = [
            // A fraction into an integer column would be truncated.
            (
                "fillnull using retries = 1.5",
                "fillnull: field 'retries' (type: int) cannot hold the replacement value 1.5 (type: double)",
            ),
            (
                "fillnull using bytes = 1.5",
                "fillnull: field 'bytes' (type: bigint) cannot hold the replacement value 1.5 (type: double)",
            ),
            // One above the largest 32-bit integer.
            (
                "fillnull using retries = 2147483648",
                "fillnull: field 'retries' (type: int) cannot hold the replacement value 2147483648 (type: bigint)",
            ),
            // A computed bigint may lie outside the int range, whatever its value turns out to be.
            (
                "fillnull using retries = 1 + 1",
                "fillnull: field 'retries' (type: int) cannot hold the replacement value 1 + 1 (type: bigint)",
            ),
            (
                "fillnull using retries = 2147483647 + 1",
                "fillnull: field 'retries' (type: int) cannot hold the replacement value 2147483647 + 1 (type: bigint)",
            ),
            // 10^11 needs 12 integer digits; Decimal128(10, 2) has 8.
            (
                "fillnull using price = 100000000000",
                "fillnull: field 'price' (type: Decimal128(10, 2)) cannot hold the replacement value 100000000000 (type: bigint)",
            ),
        ];
        for (fillnull_text, expected_reason) in numeric_refusals {
            let builder = LogicalPlanBuilder::scan("t", Arc::clone(&table) as _, None).expect("scan");
            assert_eq!(
                plan_fillnull_text(builder, fillnull_text).err(),
                Some(PplError::Semantic {
                    reason: expected_reason.to_string()
                }),
                "{fillnull_text}"
            );
        }
        for fillnull_text in [
            "fillnull using bytes = 'x'",
            "fillnull using cached = 1",
            "fillnull using seen = '2026-10-08'",
            "fillnull using note = 1",
            "fillnull using labels = 'x'",
            "fillnull using labels = 1",
        ] {
            let builder = LogicalPlanBuilder::scan("t", Arc::clone(&table) as _, None).expect("scan");
            assert!(
                matches!(
                    plan_fillnull_text(builder, fillnull_text),
                    Err(PplError::Semantic { .. })
                ),
                "{fillnull_text} must be refused"
            );
        }
    }
}
