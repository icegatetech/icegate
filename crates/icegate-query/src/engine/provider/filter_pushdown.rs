//! Which filters a scan of committed Iceberg data may be handed.
//!
//! Both table providers answer DataFusion's pushdown question through
//! [`is_kept_above_iceberg_scan`], so a filter reaches the Iceberg scan of a table with a WAL
//! topic exactly when it reaches the scan of a table without one.

use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::TreeNode;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::Expr;

/// Whether `filter` must stay above a scan of committed Iceberg data, evaluated by DataFusion
/// instead of being handed to the scan.
///
/// Two kinds of filter are kept above it:
///
/// - a null check (`IS NULL`, `IS NOT NULL`) of a struct, list or MAP column of `schema`: the
///   Iceberg predicate it translates to references a nested field, which does not bind, so the
///   query fails;
/// - a filter holding a cast to a floating type anywhere: the Iceberg translation drops a cast
///   and compares the bare column, so a decimal, 32-bit float or integer column meets a double
///   literal, which does not bind either.
///
/// Every other filter may be handed to the scan.
///
/// # Errors
///
/// Returns the error of the expression walk; the visitors here raise none.
pub(super) fn is_kept_above_iceberg_scan(filter: &Expr, schema: &Schema) -> DataFusionResult<bool> {
    Ok(has_nested_null_check(filter, schema)? || has_floating_cast(filter)?)
}

/// Whether `filter` holds a null check (`IS NULL`, `IS NOT NULL`) of a struct, list or MAP
/// column of `schema`.
fn has_nested_null_check(filter: &Expr, schema: &Schema) -> DataFusionResult<bool> {
    let is_nested_column = |operand: &Expr| {
        matches!(operand, Expr::Column(column)
            if schema.field_with_name(&column.name).is_ok_and(|field| field.data_type().is_nested()))
    };
    filter.exists(|node| {
        Ok(matches!(node, Expr::IsNull(operand) | Expr::IsNotNull(operand) if is_nested_column(operand)))
    })
}

/// Whether `filter` holds a cast to a floating-point type.
fn has_floating_cast(filter: &Expr) -> DataFusionResult<bool> {
    filter.exists(|node| Ok(matches!(node, Expr::Cast(cast) if cast.field.data_type().is_floating())))
}
