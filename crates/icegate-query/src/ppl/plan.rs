//! The PPL planner: lowers a [`PplQuery`] to a DataFusion [`LogicalPlan`] over the tenant's
//! tables.
//!
//! Sources resolve through the tenant's [`TenantSession`], so the tenant scope, index
//! expressions and the case-sensitive name rules are those of every other front end. Each
//! command then transforms the plan of the commands before it; a field reference resolves
//! against the columns of the plan at that point.

use std::collections::HashSet;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, cast, ident, lit, not};
use datafusion::optimizer::Analyzer;
use icegate_common::schema::COL_TENANT_ID;

use super::ast::{CompareOperator, PplCommand, PplExpr, PplLiteral, PplQuery, PplSortKey, PplSource};
use super::error::PplError;
use crate::opensearch::catalog::{IndexEntry, IndexExpression, IndexOptions, IndexTarget, Profile, TenantSession};
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
struct Pipeline {
    builder: LogicalPlanBuilder,
}

/// What an expression's field references resolve against.
struct ExpressionScope<'a> {
    /// The columns of the plan at the point of the expression.
    schema: &'a DFSchema,
}

/// Plan `query` for the tenant of `session`.
///
/// The source resolves through the session's index catalog, case-sensitively, with the tenant
/// scope of every scan; `tenant_id` is never a column of the output. Field references are
/// case-sensitive. `where` follows SQL three-valued logic. A leaf `fields` keeps only the listed
/// columns, so the scan reads only those.
///
/// # Errors
///
/// Returns [`PplError::IndexNotFound`] for a source that names no index,
/// [`PplError::FieldNotFound`] for a reference to no column, and [`PplError::Semantic`] for a
/// source on system indices, a field typed differently across the source indices, or an
/// expression whose types do not fit.
pub(crate) async fn plan_ppl(
    query: &PplQuery,
    session: &TenantSession,
    _context: &RequestContext,
) -> Result<PplPlan, PplError> {
    let mut pipeline = match &query.source {
        PplSource::Index { expression } => plan_index_source(expression, session).await?,
    };
    for command in &query.commands {
        pipeline = apply_command(pipeline, command)?;
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
async fn plan_index_source(expression: &str, session: &TenantSession) -> Result<Pipeline, PplError> {
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
    })
}

/// The columns of one index (see [`project_source_columns`]).
fn scan_entry(session: &TenantSession, entry: &IndexEntry) -> Result<LogicalPlanBuilder, PplError> {
    // Every index is read through the native profile, which exposes the table's own columns; a
    // profile that renames or derives fields needs its own projection here.
    let Profile::Native = entry.profile();
    project_source_columns(session.scan_index(entry)?)
}

/// The columns of a scan without `tenant_id`, in the table's schema order. The columns keep the
/// scan's qualifier; field references are unqualified names, which resolve to them.
fn project_source_columns(scan: LogicalPlanBuilder) -> Result<LogicalPlanBuilder, PplError> {
    let columns: Vec<Expr> = scan
        .schema()
        .iter()
        .filter(|(_, field)| field.name() != COL_TENANT_ID)
        .map(|(qualifier, field)| Expr::Column(Column::new(qualifier.cloned(), field.name())))
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

fn apply_command(pipeline: Pipeline, command: &PplCommand) -> Result<Pipeline, PplError> {
    let Pipeline { builder } = pipeline;
    let builder = match command {
        PplCommand::Where(predicate) => {
            let predicate = lower_expression(
                predicate,
                &ExpressionScope {
                    schema: builder.schema(),
                },
            )?;
            builder.filter(predicate)?
        }
        PplCommand::Fields { is_exclusion, fields } => select_fields(builder, *is_exclusion, fields)?,
        PplCommand::Sort(keys) => sort_rows(builder, keys)?,
        PplCommand::Head { count, offset } => builder.limit(*offset, Some(*count))?,
    };
    Ok(Pipeline { builder })
}

/// Keep the listed columns in the order listed, or with `is_exclusion` drop them and keep the
/// rest in their order. A name listed twice is kept once.
fn select_fields(
    builder: LogicalPlanBuilder,
    is_exclusion: bool,
    patterns: &[String],
) -> Result<LogicalPlanBuilder, PplError> {
    let names: Vec<String> = builder.schema().fields().iter().map(|field| field.name().clone()).collect();
    let mut selected_names: Vec<&str> = Vec::with_capacity(names.len());
    for pattern in patterns {
        for name in match_columns(pattern, &names)? {
            if !selected_names.contains(&name) {
                selected_names.push(name);
            }
        }
    }
    let kept_columns: Vec<Expr> = if is_exclusion {
        let dropped_names: HashSet<&str> = selected_names.into_iter().collect();
        names
            .iter()
            .filter(|name| !dropped_names.contains(name.as_str()))
            .map(ident)
            .collect()
    } else {
        selected_names.into_iter().map(ident).collect()
    };
    if kept_columns.is_empty() {
        return Err(PplError::Semantic {
            reason: "fields - removes every column".to_string(),
        });
    }
    Ok(builder.project(kept_columns)?)
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

/// Order the rows by `keys`; NULL sorts first ascending and last descending. String keys
/// compare byte-wise.
fn sort_rows(builder: LogicalPlanBuilder, keys: &[PplSortKey]) -> Result<LogicalPlanBuilder, PplError> {
    let scope = ExpressionScope {
        schema: builder.schema(),
    };
    let sorts = keys
        .iter()
        .map(|key| {
            let is_ascending = !key.is_descending;
            Ok(resolve_field(&key.field, &scope)?.sort(is_ascending, is_ascending))
        })
        .collect::<Result<Vec<_>, PplError>>()?;
    Ok(builder.sort(sorts)?)
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
    }
}

fn lower_literal(literal: &PplLiteral) -> Expr {
    match literal {
        PplLiteral::String(value) => lit(value.clone()),
        PplLiteral::Integer(value) => lit(*value),
        PplLiteral::Double(value) => lit(*value),
        PplLiteral::Boolean(value) => lit(*value),
    }
}

/// The expression a field reference names: the column of that exact name. Names are
/// case-sensitive.
fn resolve_field(name: &str, scope: &ExpressionScope<'_>) -> Result<Expr, PplError> {
    if scope.schema.field_with_unqualified_name(name).is_ok() {
        return Ok(ident(name));
    }
    Err(PplError::FieldNotFound {
        field: name.to_string(),
    })
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

    use super::{PplColumn, PplType, plan_ppl, project_source_columns, select_fields, union_branches};
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
        let dropped_names = ["severity_text".to_string(), "body".to_string()];
        let plan = select_fields(source, true, &dropped_names)
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
