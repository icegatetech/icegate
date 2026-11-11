//! The relation of an ss4o profile index as a PPL source: the index's table projected onto the
//! profile's field names, read from the profile's own field table (`opensearch/catalog/ss4o.rs`),
//! so no field name, type or encoding is declared twice.
//!
//! The columns follow the rule the `OpenSearch` SQL plugin applies to an index mapping (see
//! `README.md`, Sources): every field whose parent is no declared object is a column, aliases
//! included, and an object is a struct of its members. The table's stored columns travel beside
//! them as hidden columns, so a dotted, aliased or promoted name resolves to its stored column and
//! a filter on it reaches the scan as a predicate on that column.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::functions::core::expr_fn::{coalesce, named_struct};
use datafusion::functions::encoding::expr_fn::encode;
use datafusion::logical_expr::{Expr, LogicalPlanBuilder, cast, ident, lit, when};
use icegate_common::schema::COL_TENANT_ID;

use super::error::PplError;
use crate::opensearch::catalog::{
    EncodedField, FieldValue, IndexEntry, ProfileFieldSource, ProfileTable, ResolvedField, TenantSession,
};

/// The prefix of the hidden columns that carry a profile source's stored columns, within the
/// planner's reserved prefix (`RESERVED_NAME_PREFIX` of `plan.rs`): no field a query writes
/// shares a name with one of them.
pub(super) const STORED_COLUMN_PREFIX: &str = "__ppl_stored_";

/// The hidden column that carries the stored column `column` of a profile source.
fn name_stored_column(column: &str) -> String {
    format!("{STORED_COLUMN_PREFIX}{column}")
}

/// The rows of the ss4o index `entry`, whose field table is `table`, under the tenant scope of
/// `session`: one column per field whose parent is no declared object, in declared order, then one
/// hidden column per stored column of the table but `tenant_id`.
///
/// The stored columns are renamed into their hidden columns first, and the fields read those, so
/// no projection names one input column under two outputs: DataFusion's leaf-projection pushdown
/// (`push_down_leaf_projections`) adds such a column twice when it merges a MAP lookup into the
/// projection, and the plan fails with a duplicate field (the `fields` cases with an attribute key
/// of `ss4o_logs_dotted_and_alias_fields_resolve_in_every_command` in
/// `tests/opensearch/ppl_ss4o.rs` fail without the renaming).
///
/// A leaf reads its stored value as a filter reads it: a time as its timestamp column, an id as
/// its fixed-width bytes, an encoded or derived field as its expression. An id whose stored column
/// admits NULL reads as hex text, `""` for NULL, as `_source` shows it. An alias reads its target.
/// An object is a struct of its members, NULL when every member is; an attribute object is its
/// MAP column. A field `_source` renders from a whole list of the row (span events and links, the
/// bucket, quantile and exemplar fields) is not a column (see [`resolve_stored_field`]).
///
/// # Errors
///
/// The errors of [`TenantSession::scan_index`]; an internal error for a declared field whose
/// stored column the table lacks.
pub(super) fn scan_ss4o_index(
    session: &TenantSession,
    entry: &IndexEntry,
    table: &ProfileTable,
) -> Result<LogicalPlanBuilder, PplError> {
    let scan = session.scan_index(entry)?;
    let (stored_columns, renamed_columns): (Vec<String>, Vec<Expr>) = scan
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() != COL_TENANT_ID)
        .map(|field| {
            let stored_column = name_stored_column(field.name());
            let renamed_column = ident(field.name()).alias(&stored_column);
            (stored_column, renamed_column)
        })
        .unzip();
    let stored_rows = scan.project(renamed_columns)?;
    let relation = ProfileRelation { session, entry, table };
    let mut projection = Vec::new();
    for (name, source) in table.iter_fields() {
        if relation.is_object_member(name) {
            continue;
        }
        if let Some(value) = relation.build_field_value(name, *source)? {
            projection.push(read_stored_columns(value)?.alias(*name));
        }
    }
    projection.extend(stored_columns.iter().map(ident));
    Ok(stored_rows.project(projection)?)
}

/// `value`, an expression over a profile source's stored columns, over their hidden columns.
fn read_stored_columns(value: Expr) -> Result<Expr, PplError> {
    let stored_value = value.transform(|expression| {
        Ok(match &expression {
            Expr::Column(column) => Transformed::yes(ident(name_stored_column(&column.name))),
            _ => Transformed::no(expression),
        })
    })?;
    Ok(stored_value.data)
}

/// The value the field `name` of the ss4o index `entry`, whose field table is `table`, reads over
/// the hidden stored columns of [`scan_ss4o_index`]: a declared field (a leaf, an alias, an object
/// or attribute object below another object) as the relation builds it, an attribute key or a
/// resource attribute ingest promoted to a column as its stored value. `None` for a name the
/// profile does not declare.
///
/// # Errors
///
/// A semantic refusal naming `name` for a field that is not a column of the relation (see
/// [`scan_ss4o_index`]) or a path below one: `_source` builds it from more than the row's own
/// stored values, so no PPL expression reads it. The errors of
/// [`TenantSession::is_nullable_encoded_id`].
pub(super) fn resolve_stored_field(
    session: &TenantSession,
    entry: &IndexEntry,
    table: &ProfileTable,
    name: &str,
) -> Result<Option<Expr>, PplError> {
    let declared_source = table
        .iter_fields()
        .find_map(|(field, source)| (*field == name).then_some(*source));
    let value = match declared_source {
        Some(source) => ProfileRelation { session, entry, table }.build_field_value(name, source)?,
        None => match session.resolve_field(entry, name) {
            Some(field) => build_leaf_value(session, entry, field)?,
            None => return Ok(None),
        },
    };
    let Some(value) = value else {
        return Err(PplError::semantic_naming(
            name,
            format!("PPL cannot read the field [{name}]: `_source` builds it from more than the row's stored values"),
        ));
    };
    read_stored_columns(value).map(Some)
}

/// The value of a resolved profile leaf as the relation reads it; `None` for a field that is not a
/// column of the relation (see [`scan_ss4o_index`]).
///
/// # Errors
///
/// The errors of [`TenantSession::is_nullable_encoded_id`].
fn build_leaf_value(
    session: &TenantSession,
    entry: &IndexEntry,
    field: ResolvedField,
) -> Result<Option<Expr>, PplError> {
    let field = field.into_lowering_view();
    if !is_read_from_row(&field.value) {
        return Ok(None);
    }
    let value = field.value.build_read_expr();
    if session.is_nullable_encoded_id(entry, &field)? {
        return Ok(Some(coalesce(vec![
            encode(cast(value, DataType::Binary), lit("hex")),
            lit(""),
        ])));
    }
    Ok(Some(value))
}

/// Whether `value` is computed from the row's own stored values in a shape `_source` shows. Span
/// events and links and the render-only fields are lists `_source` reshapes (member names, NULL
/// lists, computed bounds), and a cross-row field reads another row.
const fn is_read_from_row(value: &FieldValue) -> bool {
    match value {
        FieldValue::Encoded(encoded_field) => {
            !(encoded_field.is_render_only() || matches!(encoded_field, EncodedField::Events | EncodedField::Links))
        }
        FieldValue::CrossRow(_) => false,
        FieldValue::Column { .. } | FieldValue::MapKey { .. } | FieldValue::HexId { .. } | FieldValue::Derived(_) => {
            true
        }
    }
}

/// One ss4o index and its field table, while its relation is built.
struct ProfileRelation<'a> {
    session: &'a TenantSession,
    entry: &'a IndexEntry,
    table: &'a ProfileTable,
}

impl ProfileRelation<'_> {
    /// Whether the declared field `name` sits under a declared object, so the object's struct
    /// carries it rather than a column of its own.
    fn is_object_member(&self, name: &str) -> bool {
        name.rsplit_once('.')
            .is_some_and(|(parent, _)| self.table.iter_fields().any(|(field, _)| *field == parent))
    }

    /// The value of the declared field `name`, which reads `source`; `None` for a field that is
    /// not a column of the relation and for an object with no such member.
    ///
    /// # Errors
    ///
    /// An internal error for a leaf whose stored column the table lacks; the errors of
    /// [`build_leaf_value`].
    fn build_field_value(&self, name: &str, source: ProfileFieldSource) -> Result<Option<Expr>, PplError> {
        if let Some(field) = self.session.resolve_field(self.entry, name) {
            return build_leaf_value(self.session, self.entry, field);
        }
        if let ProfileFieldSource::Attributes(column) = source {
            return Ok(Some(ident(column)));
        }
        if source == ProfileFieldSource::Object {
            return self.build_object_value(name);
        }
        Err(PplError::Internal {
            reason: format!("the table lacks the column behind profile field [{name}]"),
        })
    }

    /// The struct of the object `name`: one member per declared field directly under it, named by
    /// its last segment, NULL when every member is NULL, as `_source` omits such an object.
    /// `None` when no member is a column of the relation.
    ///
    /// # Errors
    ///
    /// The errors of [`Self::build_field_value`] on a member.
    fn build_object_value(&self, name: &str) -> Result<Option<Expr>, PplError> {
        let mut members = Vec::new();
        let mut member_presences = Vec::new();
        for (field, source) in self.table.iter_fields() {
            let Some((parent, member)) = field.rsplit_once('.') else {
                continue;
            };
            if parent != name {
                continue;
            }
            if let Some(value) = self.build_field_value(field, *source)? {
                member_presences.push(value.clone().is_not_null());
                members.extend([lit(member), value]);
            }
        }
        let Some(is_present) = member_presences.into_iter().reduce(Expr::or) else {
            return Ok(None);
        };
        Ok(Some(when(is_present, named_struct(members)).end()?))
    }
}
