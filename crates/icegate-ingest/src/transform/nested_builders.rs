//! Arrow builder helpers for the nested `List<Struct>` columns the OTLP
//! transforms write (`spans.events`, `spans.links`, `operations.evaluations`).

use arrow::{
    array::{
        ArrayBuilder, FixedSizeBinaryBuilder, Float64Builder, Int32Builder, StringBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Fields, TimeUnit},
};

use super::attributes::{attribute_map_builder, extract_map_fields_from_nested_struct};

/// Error for a `StructBuilder` field slot that the schema says must exist.
///
/// Both the slot order and the slot types come from the schema's own `Fields`
/// (see [`nested_struct_builders`] and [`nested_field_index`]), so a lookup only
/// fails if the schema changed under us. Reporting instead of panicking keeps
/// that from taking down the ingest request path.
pub(crate) fn field_builder_missing(parent_column: &str, field: &str) -> crate::error::IngestError {
    crate::error::IngestError::Validation(format!(
        "'{parent_column}' struct builder is missing the '{field}' field slot"
    ))
}

/// Build one `StructBuilder` slot per field of a nested list-element struct.
///
/// `StructBuilder::new` pairs its `fields` and `builders` arguments by position,
/// so both must come from the same source or a schema reorder silently binds a
/// field to the wrong builder. Deriving the list here from `fields` makes that
/// impossible. Only the Arrow types the nested `spans` (`events`, `links`) and
/// `operations` (`evaluations`) structs use are handled; a new type is a schema
/// change that must be taught here.
pub(crate) fn nested_struct_builders(
    fields: &Fields,
    parent_column: &str,
) -> crate::error::Result<Vec<Box<dyn ArrayBuilder>>> {
    fields
        .iter()
        .map(|field| -> crate::error::Result<Box<dyn ArrayBuilder>> {
            Ok(match field.data_type() {
                DataType::Timestamp(TimeUnit::Microsecond, _) => Box::new(TimestampMicrosecondBuilder::new()),
                DataType::Utf8 => Box::new(StringBuilder::new()) as Box<dyn ArrayBuilder>,
                DataType::Int32 => Box::new(Int32Builder::new()),
                DataType::Float64 => Box::new(Float64Builder::new()),
                DataType::FixedSizeBinary(width) => Box::new(FixedSizeBinaryBuilder::new(*width)),
                DataType::Map(..) => {
                    let (key_field, value_field) = extract_map_fields_from_nested_struct(fields, field.name())?;
                    Box::new(attribute_map_builder(key_field, value_field))
                }
                other => {
                    return Err(crate::error::IngestError::Validation(format!(
                        "'{parent_column}.{}' has unsupported type {other}",
                        field.name()
                    )));
                }
            })
        })
        .collect()
}

/// Position of `field` within a nested list-element struct.
///
/// `StructBuilder` addresses slots by index, so the index must be read from the
/// same `Fields` the builder was constructed from rather than written as a
/// literal that a schema reorder would invalidate.
pub(crate) fn nested_field_index(fields: &Fields, parent_column: &str, field: &str) -> crate::error::Result<usize> {
    fields
        .iter()
        .position(|candidate| candidate.name() == field)
        .ok_or_else(|| field_builder_missing(parent_column, field))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;

    /// The point of deriving both halves from `Fields`: a reordered struct must
    /// still bind every name to a builder of the right type. The previous fixed
    /// 0..N layout would have paired `name` with the timestamp builder here.
    #[test]
    fn nested_struct_builders_follow_field_order_not_a_fixed_layout() {
        use icegate_common::schema::{COL_DROPPED_ATTRIBUTES_COUNT, COL_EVENTS, COL_NAME, COL_TIMESTAMP};

        // Deliberately not the order the spans schema declares.
        let fields = Fields::from(vec![
            Field::new(COL_NAME, DataType::Utf8, false),
            Field::new(COL_DROPPED_ATTRIBUTES_COUNT, DataType::Int32, false),
            Field::new(COL_TIMESTAMP, DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("ratio", DataType::Float64, true),
        ]);

        assert_eq!(nested_field_index(&fields, COL_EVENTS, COL_NAME).expect("name slot"), 0);
        assert_eq!(
            nested_field_index(&fields, COL_EVENTS, COL_DROPPED_ATTRIBUTES_COUNT).expect("dropped slot"),
            1
        );
        assert_eq!(
            nested_field_index(&fields, COL_EVENTS, COL_TIMESTAMP).expect("timestamp slot"),
            2
        );
        assert_eq!(nested_field_index(&fields, COL_EVENTS, "ratio").expect("ratio slot"), 3);

        let mut builders = nested_struct_builders(&fields, COL_EVENTS).expect("builders");
        assert_eq!(builders.len(), 4);
        assert!(builders[0].as_any_mut().downcast_mut::<StringBuilder>().is_some());
        assert!(builders[1].as_any_mut().downcast_mut::<Int32Builder>().is_some());
        assert!(builders[2].as_any_mut().downcast_mut::<TimestampMicrosecondBuilder>().is_some());
        assert!(builders[3].as_any_mut().downcast_mut::<Float64Builder>().is_some());
    }

    #[test]
    fn nested_field_index_reports_the_missing_field_by_name() {
        use icegate_common::schema::{COL_LINKS, COL_NAME, COL_TRACE_ID};

        let fields = Fields::from(vec![Field::new(COL_NAME, DataType::Utf8, false)]);
        let error = nested_field_index(&fields, COL_LINKS, COL_TRACE_ID).expect_err("must not resolve");
        let message = error.to_string();
        assert!(message.contains(COL_LINKS), "{message}");
        assert!(message.contains(COL_TRACE_ID), "{message}");
    }

    #[test]
    fn nested_struct_builders_rejects_a_type_it_was_not_taught() {
        use icegate_common::schema::COL_EVENTS;

        let fields = Fields::from(vec![Field::new("count", DataType::Int64, false)]);
        // `Vec<Box<dyn ArrayBuilder>>` is not `Debug`, so `expect_err` is unavailable.
        let Err(error) = nested_struct_builders(&fields, COL_EVENTS) else {
            panic!("an untaught field type must not produce a builder");
        };
        assert!(error.to_string().contains("count"), "{error}");
    }
}
