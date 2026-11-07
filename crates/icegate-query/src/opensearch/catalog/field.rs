//! The fields an index exposes, as `_field_caps` and `_mapping` report them, and a field name
//! resolved against one index.

use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::Expr;
use datafusion::prelude::ident;

/// An `OpenSearch` field type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldType {
    /// Analyzed full text: searchable, not aggregatable.
    Text,
    /// An exact string.
    Keyword,
    /// A point in time.
    Date,
    /// A 32-bit integer.
    Integer,
    /// A 64-bit integer.
    Long,
    /// A floating-point or decimal number.
    Double,
    /// `true` or `false`.
    Boolean,
    /// A structured value: present in `_source`, not itself searchable or aggregatable.
    Object,
}

impl FieldType {
    /// The type's `OpenSearch` name.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Keyword => "keyword",
            Self::Date => "date",
            Self::Integer => "integer",
            Self::Long => "long",
            Self::Double => "double",
            Self::Boolean => "boolean",
            Self::Object => "object",
        }
    }
}

/// One field of an index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexField {
    /// The name clients use. For a MAP key it is `<column>.<stored key>`.
    pub(crate) name: String,
    /// The `OpenSearch` type.
    pub(crate) field_type: FieldType,
    /// Queries may filter on the field.
    pub(crate) is_searchable: bool,
    /// Aggregations and sorts may use the field.
    pub(crate) is_aggregatable: bool,
    /// The structural field this one sits under: the MAP column of a key. Dots inside a stored
    /// key are never structure.
    pub(crate) parent: Option<String>,
}

impl IndexField {
    /// A searchable scalar field, aggregatable unless it is `text`.
    pub(crate) fn leaf(name: String, field_type: FieldType, parent: Option<String>) -> Self {
        Self {
            name,
            is_searchable: true,
            is_aggregatable: field_type != FieldType::Text,
            field_type,
            parent,
        }
    }

    /// An `object` field: present in `_source`, neither searchable nor aggregatable itself.
    pub(crate) const fn object(name: String) -> Self {
        Self {
            name,
            field_type: FieldType::Object,
            is_searchable: false,
            is_aggregatable: false,
            parent: None,
        }
    }
}

/// A field name resolved against one index: what the name means there and how to read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedField {
    /// The name the request used, kept for error reasons.
    pub(crate) name: String,
    /// The `OpenSearch` type the name has in that index.
    pub(crate) field_type: FieldType,
    /// Where the value is stored.
    pub(crate) value: FieldValue,
}

/// Where a resolved field's value is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FieldValue {
    /// A column, read as stored.
    Column {
        /// The column name.
        column: String,
    },
    /// One key of a `MAP<string, string>` column, looked up by its exact stored key. Dots in
    /// the key are part of the key.
    MapKey {
        /// The MAP column.
        column: String,
        /// The stored key.
        key: String,
    },
    /// A fixed-width binary id column, matched and rendered as lowercase hex of `width` bytes.
    HexId {
        /// The column name.
        column: String,
        /// The column's width in bytes.
        width: i32,
    },
}

impl FieldValue {
    /// The expression reading this value from a scan of its table.
    ///
    /// Column names go through `ident`, never through `col`: `col` parses a dotted name as a
    /// qualified reference, and no column name may pass through DataFusion's identifier
    /// normalization (names match case-sensitively).
    pub(crate) fn build_read_expr(&self) -> Expr {
        match self {
            Self::Column { column } | Self::HexId { column, .. } => ident(column),
            Self::MapKey { column, key } => get_field(ident(column), key.as_str()),
        }
    }
}

#[cfg(test)]
mod resolution_tests {
    use datafusion::arrow::datatypes::Schema;
    use icegate_common::schema::{
        COL_LOG_ATTRIBUTES, COL_RESOURCE_ATTRIBUTES, COL_SERVICE_INSTANCE_ID, COL_SERVICE_NAME, COL_TRACE_ID,
        logs_schema, metrics_schema, spans_schema,
    };
    use icegate_common::{LOGS_TABLE, METRICS_TABLE, SPANS_TABLE};

    use super::{FieldType, FieldValue, ResolvedField};
    use crate::opensearch::catalog::native::resolve_native_field;

    fn convert_arrow_schema(iceberg_schema: icegate_common::Result<iceberg::spec::Schema>) -> Schema {
        iceberg::arrow::schema_to_arrow_schema(&iceberg_schema.expect("iceberg schema")).expect("arrow schema")
    }

    fn build_column_field(name: &str, field_type: FieldType, column: &str) -> ResolvedField {
        ResolvedField {
            name: name.to_string(),
            field_type,
            value: FieldValue::Column {
                column: column.to_string(),
            },
        }
    }

    fn build_map_key_field(name: &str, column: &str, key: &str) -> ResolvedField {
        ResolvedField {
            name: name.to_string(),
            field_type: FieldType::Keyword,
            value: FieldValue::MapKey {
                column: column.to_string(),
                key: key.to_string(),
            },
        }
    }

    #[test]
    fn promoted_resource_keys_resolve_to_their_column_on_logs_and_metrics_and_to_the_map_on_spans() {
        let logs = convert_arrow_schema(logs_schema());
        for name in ["resource_attributes.service.name", "resource_attributes.service_name"] {
            assert_eq!(
                resolve_native_field(&logs, LOGS_TABLE, name),
                Some(build_column_field(name, FieldType::Keyword, COL_SERVICE_NAME)),
                "{name} on logs"
            );
        }

        let metrics = convert_arrow_schema(metrics_schema());
        for name in ["resource_attributes.service.name", "resource_attributes.service_name"] {
            assert_eq!(
                resolve_native_field(&metrics, METRICS_TABLE, name),
                Some(build_column_field(name, FieldType::Keyword, COL_SERVICE_NAME)),
                "{name} on metrics"
            );
        }
        for name in [
            "resource_attributes.service.instance.id",
            "resource_attributes.service_instance_id",
        ] {
            assert_eq!(
                resolve_native_field(&metrics, METRICS_TABLE, name),
                Some(build_column_field(name, FieldType::Keyword, COL_SERVICE_INSTANCE_ID)),
                "{name} on metrics"
            );
        }

        let spans = convert_arrow_schema(spans_schema());
        assert_eq!(
            resolve_native_field(&spans, SPANS_TABLE, "resource_attributes.service.name"),
            Some(build_map_key_field(
                "resource_attributes.service.name",
                COL_RESOURCE_ATTRIBUTES,
                "service.name"
            ))
        );
    }

    #[test]
    fn an_unmapped_field_resolves_to_nothing() {
        let logs = convert_arrow_schema(logs_schema());
        for name in [
            "no_such_field",
            "tenant_id",
            "body.inner",
            "resource_attributes.",
            "Severity_Text",
            "",
        ] {
            assert_eq!(resolve_native_field(&logs, LOGS_TABLE, name), None, "{name:?}");
        }
    }

    #[test]
    fn a_map_key_path_resolves_without_enumeration() {
        let logs = convert_arrow_schema(logs_schema());
        assert_eq!(
            resolve_native_field(&logs, LOGS_TABLE, "log_attributes.http.method"),
            Some(build_map_key_field(
                "log_attributes.http.method",
                COL_LOG_ATTRIBUTES,
                "http.method"
            ))
        );
        assert_eq!(
            resolve_native_field(&logs, LOGS_TABLE, "log_attributes.http"),
            Some(build_map_key_field("log_attributes.http", COL_LOG_ATTRIBUTES, "http"))
        );
        assert_eq!(
            resolve_native_field(&logs, LOGS_TABLE, "log_attributes.Never.Stored"),
            Some(build_map_key_field(
                "log_attributes.Never.Stored",
                COL_LOG_ATTRIBUTES,
                "Never.Stored"
            ))
        );
        assert_eq!(
            resolve_native_field(&logs, LOGS_TABLE, COL_TRACE_ID),
            Some(ResolvedField {
                name: COL_TRACE_ID.to_string(),
                field_type: FieldType::Keyword,
                value: FieldValue::HexId {
                    column: COL_TRACE_ID.to_string(),
                    width: 16,
                },
            })
        );
    }
}
