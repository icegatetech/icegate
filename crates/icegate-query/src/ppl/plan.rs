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
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::expr::{Case, Lambda, LambdaVariable};
use datafusion::logical_expr::{Between, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, cast, ident, lit, not};
use datafusion::optimizer::Analyzer;
use icegate_common::schema::COL_TENANT_ID;

use super::ast::{CompareOperator, PplCommand, PplExpr, PplLiteral, PplQuery, PplSortKey, PplSource};
use super::error::PplError;
use super::functions::{FunctionContext, is_higher_order_function, lower_arithmetic, lower_cast, lower_function_call};
use crate::opensearch::catalog::{
    FieldValue, IndexEntry, IndexExpression, IndexOptions, IndexTarget, Profile, TenantSession,
};
use crate::opensearch::context::RequestContext;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::ExpandWildcards;
use crate::opensearch::glob::matches_glob;

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
}

/// The indices a pipeline reads, for the field rules of their profiles (a promoted resource
/// attribute read through its column).
struct SourceBinding<'session> {
    session: &'session TenantSession,
    entries: Vec<&'session IndexEntry>,
    /// The column names an earlier `rename` moved or overwrote, or an `eval` assigned: such a
    /// name no longer holds the source's column, so a promoted attribute must not read it.
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
/// request-time functions read `context.now`.
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
) -> Result<PplPlan, PplError> {
    let mut pipeline = match &query.source {
        PplSource::Index { expression } => plan_index_source(expression, session).await?,
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
    let index_expression = IndexExpression::parse(expression)?;
    let options = IndexOptions {
        ignore_unavailable: false,
        allow_no_indices: false,
        expand_wildcards: ExpandWildcards {
            is_open: true,
            is_hidden: false,
        },
    };
    let entries = match session.resolve_target(&index_expression, options).await? {
        IndexTarget::Data(entries) => entries,
        IndexTarget::System(names) => {
            return Err(OpenSearchError::illegal_argument(format!(
                "PPL reads data indices only; [{}] are system indices",
                names.join(", ")
            ))
            .into());
        }
    };
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
    })
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
    let Pipeline { builder, mut source } = pipeline;
    let schema = Arc::clone(builder.schema());
    let build_scope = || ExpressionScope::new(&schema, source.as_ref(), now);
    let builder = match command {
        PplCommand::Where(predicate) => builder.filter(lower_expression(predicate, &build_scope())?)?,
        PplCommand::Fields { is_exclusion, fields } => select_fields(builder, *is_exclusion, fields, &build_scope())?,
        PplCommand::Sort(keys) => sort_rows(builder, keys, &build_scope())?,
        PplCommand::Head { count, offset } => builder.limit(*offset, Some(*count))?,
        PplCommand::Eval(assignments) => evaluate_fields(builder, assignments, source.as_mut(), now)?,
        PplCommand::Rename(renames) => rename_fields(builder, renames, source.as_mut())?,
    };
    Ok(Pipeline { builder, source })
}

/// Keep the listed fields in the order listed, or with `is_exclusion` drop them and keep the
/// rest in their order. A name listed twice is kept once. A listed field that is not a column
/// (a MAP key path such as ``log_attributes.`http.method` ``, or a promoted resource attribute)
/// is kept as a column named by its path.
fn select_fields(
    builder: LogicalPlanBuilder,
    is_exclusion: bool,
    patterns: &[String],
    scope: &ExpressionScope<'_>,
) -> Result<LogicalPlanBuilder, PplError> {
    let names: Vec<String> = scope.schema.fields().iter().map(|field| field.name().clone()).collect();
    if is_exclusion {
        let mut dropped_names: HashSet<&str> = HashSet::with_capacity(patterns.len());
        for pattern in patterns {
            dropped_names.extend(match_columns(pattern, &names)?);
        }
        let kept_columns: Vec<Expr> = names
            .iter()
            .filter(|name| !dropped_names.contains(name.as_str()))
            .map(ident)
            .collect();
        if kept_columns.is_empty() {
            return Err(PplError::Semantic {
                reason: "fields - removes every column".to_string(),
            });
        }
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

/// Order the rows by `keys`; NULL sorts first ascending and last descending. Strings compare
/// byte-wise.
fn sort_rows(
    builder: LogicalPlanBuilder,
    keys: &[PplSortKey],
    scope: &ExpressionScope<'_>,
) -> Result<LogicalPlanBuilder, PplError> {
    let sorts = keys
        .iter()
        .map(|key| {
            let is_ascending = !key.is_descending;
            Ok(resolve_field(&key.field, scope)?.sort(is_ascending, is_ascending))
        })
        .collect::<Result<Vec<_>, PplError>>()?;
    Ok(builder.sort(sorts)?)
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

/// Apply `rename` pairs in order: each field keeps its position under its new name, and a field
/// already holding the new name is removed. Both names of a pair that changes a name are recorded
/// as replaced on `source`.
fn rename_fields(
    builder: LogicalPlanBuilder,
    renames: &[(String, String)],
    mut source: Option<&mut SourceBinding<'_>>,
) -> Result<LogicalPlanBuilder, PplError> {
    // (source column, output name) per output column, in order.
    let mut columns: Vec<(String, String)> = builder
        .schema()
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
        if let Some(source) = source.as_deref_mut() {
            source.replaced_columns.extend([original_name.clone(), new_name.clone()]);
        }
        let replaced_position = columns.iter().position(|(_, name)| name == new_name);
        columns[renamed_position].1.clone_from(new_name);
        if let Some(replaced_position) = replaced_position {
            columns.remove(replaced_position);
        }
    }
    let projection: Vec<Expr> = columns
        .iter()
        .map(|(source_column, name)| ident(source_column).alias(name))
        .collect();
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
            let listed_values = list
                .iter()
                .map(|item| lower_expression(item, scope))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(lower_expression(value, scope)?.in_list(listed_values, *is_negated))
        }
        PplExpr::Between {
            value,
            low,
            high,
            is_negated,
        } => Ok(Expr::Between(Between::new(
            Box::new(lower_expression(value, scope)?),
            *is_negated,
            Box::new(lower_expression(low, scope)?),
            Box::new(lower_expression(high, scope)?),
        ))),
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

/// Build `builder`, run the analyzer so a type error is reported as the query's error rather
/// than at execution, and describe the output columns.
fn finish_plan(builder: LogicalPlanBuilder) -> Result<PplPlan, PplError> {
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::DateTime;
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, LogicalTableSource};
    use datafusion::prelude::SessionContext;
    use icegate_common::schema::COL_TENANT_ID;
    use icegate_common::{OPERATIONS_TABLE, TenantId};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    use super::{ExpressionScope, PplColumn, PplType, plan_ppl, project_source_columns, select_fields, union_branches};
    use crate::opensearch::context::RequestContext;
    use crate::ppl::parse_ppl;
    use crate::test_rows::{FINISH_REASONS, build_operation_row};
    use crate::test_support::{SearchFixture, TestCell};

    const TENANT_A: &str = "tenant-a";
    const TENANT_B: &str = "tenant-b";
    /// 2026-10-08T12:00:00Z.
    const NOON_MICROS: i64 = 1_791_460_800_000_000;

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
        let request_time = DateTime::from_timestamp_micros(NOON_MICROS).expect("noon");
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
            now: DateTime::from_timestamp_micros(NOON_MICROS).expect("noon"),
        };
        let query = parse_ppl("source=operations | fields finish_reasons").expect("query");

        let ppl_plan = plan_ppl(&query, &session, &context).await.expect("plan");

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
}
