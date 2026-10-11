//! The native profile: a table's columns under their stored names.

use std::collections::BTreeSet;

use datafusion::arrow::datatypes::{DataType, Schema};
use icegate_common::schema::COL_BODY;

use super::field::{FieldType, IndexField};
use super::map_key_cache::MapKeys;
use crate::opensearch::error::OpenSearchError;

/// The fields of a table whose client-visible Arrow schema is `schema`, in schema column order.
/// The tenant scope has already removed `tenant_id` from that schema.
///
/// Each MAP column is an `object` followed by one `keyword` leaf `<column>.<key>` per key in
/// `map_keys`, in key byte order; a stored key with dots stays one leaf. A MAP column absent from
/// `map_keys` lists no leaves. Lists and structs are `object` fields; scalars are leaves.
///
/// # Errors
///
/// Returns `500` naming the column when its Arrow type has no `OpenSearch` representation: an
/// error is answered rather than a listing that silently lacks the column.
pub(crate) fn list_native_fields(schema: &Schema, map_keys: &MapKeys) -> Result<Vec<IndexField>, OpenSearchError> {
    let key_count: usize = map_keys.values().map(BTreeSet::len).sum();
    let mut fields = Vec::with_capacity(schema.fields().len() + key_count);
    for column in schema.fields() {
        let name = column.name();
        match column.data_type() {
            DataType::Map(_, _) => {
                fields.push(IndexField::object(name.clone()));
                fields.extend(
                    map_keys
                        .get(name)
                        .into_iter()
                        .flatten()
                        .map(|key| IndexField::leaf(format!("{name}.{key}"), FieldType::Keyword, Some(name.clone()))),
                );
            }
            DataType::List(_) | DataType::Struct(_) => fields.push(IndexField::object(name.clone())),
            data_type => fields.push(IndexField::leaf(name.clone(), map_scalar_type(name, data_type)?, None)),
        }
    }
    Ok(fields)
}

/// The `OpenSearch` type of the scalar column `name`. Strings are `keyword` except `body`, the one
/// full-text column; fixed-width binary columns are the hex-rendered ids, also `keyword`.
///
/// Exactly the listed Arrow types are typed: the forms in which table columns are read. Any other
/// type is refused rather than typed by a guess; an unsigned 64-bit value, for one, does not fit
/// `long`.
fn map_scalar_type(name: &str, data_type: &DataType) -> Result<FieldType, OpenSearchError> {
    match data_type {
        DataType::Utf8 if name == COL_BODY => Ok(FieldType::Text),
        DataType::Utf8 | DataType::FixedSizeBinary(_) => Ok(FieldType::Keyword),
        DataType::Timestamp(_, _) | DataType::Date32 => Ok(FieldType::Date),
        DataType::Int32 => Ok(FieldType::Integer),
        DataType::Int64 => Ok(FieldType::Long),
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => Ok(FieldType::Double),
        DataType::Boolean => Ok(FieldType::Boolean),
        // `DataType` has dozens of variants; the contract is "anything not listed above is an
        // error", so this one arm stays a catch-all.
        other => Err(OpenSearchError::internal(format!(
            "column `{name}` has Arrow type {other} that the OpenSearch API cannot represent"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Arc;

    use axum::http::StatusCode;
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use icegate_common::schema::{
        COL_BODY, COL_DURATION_MICROS, COL_INPUT_USD_PER_1M, COL_KIND, COL_LOG_ATTRIBUTES, COL_SEVERITY_TEXT,
        COL_SPAN_ID, COL_TIMESTAMP, COL_TRACE_ID, COL_VALUE_DOUBLE,
    };

    use super::*;
    use crate::opensearch::error::render_envelope;

    fn build_string_map_column(name: &str) -> Field {
        let entries = Field::new(
            "key_value",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            false,
        );
        Field::new(name, DataType::Map(Arc::new(entries), false), false)
    }

    fn index_fields_by_name(fields: &[IndexField]) -> BTreeMap<&str, &IndexField> {
        fields.iter().map(|field| (field.name.as_str(), field)).collect()
    }

    #[test]
    fn columns_map_to_opensearch_types_and_body_alone_is_text() {
        let schema = Schema::new(vec![
            Field::new(COL_BODY, DataType::Utf8, true),
            Field::new(COL_SEVERITY_TEXT, DataType::Utf8, true),
            Field::new(COL_TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), true),
            Field::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), true),
            Field::new(COL_KIND, DataType::Int32, true),
            Field::new(COL_DURATION_MICROS, DataType::Int64, false),
            Field::new(COL_VALUE_DOUBLE, DataType::Float64, true),
            Field::new(COL_INPUT_USD_PER_1M, DataType::Decimal128(18, 9), true),
            Field::new("is_monotonic", DataType::Boolean, true),
            Field::new("ratio", DataType::Float32, true),
            Field::new("day", DataType::Date32, true),
            build_string_map_column(COL_LOG_ATTRIBUTES),
        ]);

        let fields = list_native_fields(&schema, &MapKeys::new()).expect("every type is representable");

        let expected = [
            (COL_BODY, "text", true, false),
            (COL_SEVERITY_TEXT, "keyword", true, true),
            (COL_TIMESTAMP, "date", true, true),
            (COL_TRACE_ID, "keyword", true, true),
            (COL_SPAN_ID, "keyword", true, true),
            (COL_KIND, "integer", true, true),
            (COL_DURATION_MICROS, "long", true, true),
            (COL_VALUE_DOUBLE, "double", true, true),
            (COL_INPUT_USD_PER_1M, "double", true, true),
            ("is_monotonic", "boolean", true, true),
            ("ratio", "double", true, true),
            ("day", "date", true, true),
            (COL_LOG_ATTRIBUTES, "object", false, false),
        ];
        let fields_by_name = index_fields_by_name(&fields);
        assert_eq!(fields.len(), expected.len());
        for (name, field_type, is_searchable, is_aggregatable) in expected {
            let field = fields_by_name[name];
            assert_eq!(
                (
                    field.field_type.name(),
                    field.is_searchable,
                    field.is_aggregatable,
                    field.parent.as_deref()
                ),
                (field_type, is_searchable, is_aggregatable, None),
                "{name}"
            );
        }
    }

    #[test]
    fn lists_and_structs_are_objects_that_are_neither_searchable_nor_aggregatable() {
        let schema = Schema::new(vec![
            Field::new(
                "finish_reasons",
                DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "status",
                DataType::Struct(Fields::from(vec![Field::new("code", DataType::Int32, true)])),
                true,
            ),
        ]);

        let fields = list_native_fields(&schema, &MapKeys::new()).expect("every type is representable");

        assert_eq!(
            fields,
            vec![
                IndexField {
                    name: "finish_reasons".to_string(),
                    field_type: FieldType::Object,
                    is_searchable: false,
                    is_aggregatable: false,
                    parent: None,
                },
                IndexField {
                    name: "status".to_string(),
                    field_type: FieldType::Object,
                    is_searchable: false,
                    is_aggregatable: false,
                    parent: None,
                },
            ]
        );
    }

    /// A column the API cannot type is an error, never a listing that silently lacks it. Binary has
    /// no `OpenSearch` type; an unsigned 64-bit value does not fit `long`.
    #[tokio::test]
    async fn an_unrepresentable_column_type_is_a_500_instead_of_a_dropped_column() {
        for (name, data_type) in [("blob", DataType::Binary), ("count", DataType::UInt64)] {
            let schema = Schema::new(vec![Field::new(name, data_type, true)]);

            let error = list_native_fields(&schema, &MapKeys::new()).expect_err("no OpenSearch type");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{name}");
            assert_eq!(body["status"], 500, "{name}");
            assert_eq!(body["error"]["type"], "exception", "{name}");
        }
    }

    /// A stored key containing dots is one keyword leaf of its MAP column. No object parent is
    /// invented for the dots, so `http` and `http.method` can both be keys and both stay leaves.
    #[test]
    fn map_keys_with_dots_stay_keyword_leaves_of_their_map() {
        let schema = Schema::new(vec![build_string_map_column(COL_LOG_ATTRIBUTES)]);
        let keys: BTreeSet<String> = ["http", "http.method"].map(str::to_string).into();
        let map_keys = MapKeys::from([(COL_LOG_ATTRIBUTES.to_string(), keys)]);

        let fields = list_native_fields(&schema, &map_keys).expect("every type is representable");

        let field_summaries: Vec<(&str, &str, bool, bool, Option<&str>)> = fields
            .iter()
            .map(|field| {
                (
                    field.name.as_str(),
                    field.field_type.name(),
                    field.is_searchable,
                    field.is_aggregatable,
                    field.parent.as_deref(),
                )
            })
            .collect();
        let http_field = format!("{COL_LOG_ATTRIBUTES}.http");
        let method_field = format!("{COL_LOG_ATTRIBUTES}.http.method");
        assert_eq!(
            field_summaries,
            [
                (COL_LOG_ATTRIBUTES, "object", false, false, None),
                (http_field.as_str(), "keyword", true, true, Some(COL_LOG_ATTRIBUTES)),
                (method_field.as_str(), "keyword", true, true, Some(COL_LOG_ATTRIBUTES)),
            ]
        );
    }
}
