use std::{cmp::Ordering, sync::OnceLock};

use arrow::{
    array::{Array, StringArray, TimestampMicrosecondArray},
    record_batch::RecordBatch,
};
use iceberg::spec::{NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder, Transform};

use super::{RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryValue, compare_option_ord};
use crate::error::{IngestError, Result};

static LOGS_SORT_DESCRIPTOR: OnceLock<std::result::Result<SortColumnsDescriptor, String>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SortColumnDescriptor {
    column_name: String,
    primitive_type: PrimitiveType,
    descending: bool,
    nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SortColumnsDescriptor {
    columns: Vec<SortColumnDescriptor>,
}

// TODO(high): make a generic solution without binding to logs
impl SortColumnsDescriptor {
    pub(crate) fn logs() -> Result<&'static Self> {
        match LOGS_SORT_DESCRIPTOR.get_or_init(|| Self::try_from_logs_sort_order().map_err(|err| err.to_string())) {
            Ok(descriptor) => Ok(descriptor),
            Err(message) => Err(IngestError::Config(format!(
                "failed to initialize logs sort descriptor: {message}"
            ))),
        }
    }

    pub(crate) fn columns(&self) -> &[SortColumnDescriptor] {
        self.columns.as_slice()
    }

    fn try_from_logs_sort_order() -> Result<Self> {
        let schema = icegate_common::schema::logs_schema()?;
        let sort_order = icegate_common::schema::logs_sort_order(&schema)?;
        Self::try_from_schema_sort_order(&schema, &sort_order)
    }

    fn try_from_schema_sort_order(schema: &Schema, sort_order: &SortOrder) -> Result<Self> {
        let mut columns = Vec::with_capacity(sort_order.fields.len());

        for sort_field in &sort_order.fields {
            let schema_field = resolve_logs_sort_schema_field(schema, sort_field)?;
            let primitive_type = schema_field
                .field_type
                .as_primitive_type()
                .ok_or_else(|| {
                    IngestError::Shift(format!("logs sort field '{}' must be primitive", schema_field.name))
                })?
                .clone();
            columns.push(SortColumnDescriptor {
                column_name: schema_field.name.clone(),
                primitive_type,
                descending: matches!(sort_field.direction, SortDirection::Descending),
                nulls_first: matches!(sort_field.null_order, NullOrder::First),
            });
        }

        Ok(Self { columns })
    }
}

pub(crate) struct SortColumnCache {
    columns: Vec<CachedSortColumn>,
}

impl SortColumnCache {
    pub(crate) fn try_new(batch: &RecordBatch, descriptor: &SortColumnsDescriptor, context: &str) -> Result<Self> {
        let mut columns = Vec::with_capacity(descriptor.columns.len());

        for sort_column in descriptor.columns() {
            let column_name = sort_column.column_name.as_str();
            let column_idx = batch
                .schema()
                .index_of(column_name)
                .map_err(|err| IngestError::Shift(format!("{context} is missing {column_name}: {err}")))?;
            let column = batch.column(column_idx);
            let cached = match sort_column.primitive_type {
                PrimitiveType::String => CachedSortColumn::Utf8 {
                    values: column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| IngestError::Shift(format!("{column_name} must be Utf8 for {context}")))?
                        .clone(),
                    descending: sort_column.descending,
                    nulls_first: sort_column.nulls_first,
                },
                PrimitiveType::Timestamp => CachedSortColumn::TimestampMicrosecond {
                    values: column
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            IngestError::Shift(format!("{column_name} must be Timestamp(Microsecond) for {context}"))
                        })?
                        .clone(),
                    descending: sort_column.descending,
                    nulls_first: sort_column.nulls_first,
                },
                ref other => {
                    return Err(IngestError::Shift(format!(
                        "unsupported logs sort column type for '{column_name}' in {context}: {other}"
                    )));
                }
            };
            columns.push(cached);
        }

        Ok(Self { columns })
    }

    pub(crate) fn compare_indices(&self, left_row_idx: usize, right_row_idx: usize) -> Ordering {
        for column in &self.columns {
            let ordering = column.compare_same(left_row_idx, right_row_idx);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        left_row_idx.cmp(&right_row_idx)
    }

    pub(crate) fn compare_row(&self, left_row_idx: usize, right: &Self, right_row_idx: usize) -> Result<Ordering> {
        for (left, right) in self.columns.iter().zip(right.columns.iter()) {
            let ordering = left.compare_other(left_row_idx, right, right_row_idx)?;
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }

        Ok(Ordering::Equal)
    }

    pub(crate) fn boundary_key(&self, row_idx: usize) -> RowGroupBoundaryKey {
        RowGroupBoundaryKey {
            components: self.columns.iter().map(|column| column.boundary_component(row_idx)).collect(),
        }
    }
}

fn resolve_logs_sort_schema_field<'a>(
    schema: &'a Schema,
    sort_field: &SortField,
) -> Result<&'a iceberg::spec::NestedField> {
    if sort_field.transform != Transform::Identity {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            format!("unsupported logs merge sort transform: {}", sort_field.transform),
        )
        .into());
    }

    schema
        .field_by_id(sort_field.source_id)
        .map(std::convert::AsRef::as_ref)
        .ok_or_else(|| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("logs sort field source_id {} not found in schema", sort_field.source_id),
            )
            .into()
        })
}

enum CachedSortColumn {
    Utf8 {
        values: StringArray,
        descending: bool,
        nulls_first: bool,
    },
    TimestampMicrosecond {
        values: TimestampMicrosecondArray,
        descending: bool,
        nulls_first: bool,
    },
}

impl CachedSortColumn {
    fn compare_same(&self, left_row_idx: usize, right_row_idx: usize) -> Ordering {
        match self {
            Self::Utf8 {
                values,
                descending,
                nulls_first,
            } => compare_option_ord(
                string_array_value(values, left_row_idx),
                string_array_value(values, right_row_idx),
                *descending,
                *nulls_first,
            ),
            Self::TimestampMicrosecond {
                values,
                descending,
                nulls_first,
            } => compare_option_ord(
                timestamp_array_value(values, left_row_idx),
                timestamp_array_value(values, right_row_idx),
                *descending,
                *nulls_first,
            ),
        }
    }

    fn compare_other(&self, left_row_idx: usize, right: &Self, right_row_idx: usize) -> Result<Ordering> {
        match (self, right) {
            (
                Self::Utf8 {
                    values: left,
                    descending,
                    nulls_first,
                },
                Self::Utf8 { values: right, .. },
            ) => Ok(compare_option_ord(
                string_array_value(left, left_row_idx),
                string_array_value(right, right_row_idx),
                *descending,
                *nulls_first,
            )),
            (
                Self::TimestampMicrosecond {
                    values: left,
                    descending,
                    nulls_first,
                },
                Self::TimestampMicrosecond { values: right, .. },
            ) => Ok(compare_option_ord(
                timestamp_array_value(left, left_row_idx),
                timestamp_array_value(right, right_row_idx),
                *descending,
                *nulls_first,
            )),
            _ => Err(IngestError::Shift(
                "sort column cache types do not match across streams".to_string(),
            )),
        }
    }

    fn boundary_component(&self, row_idx: usize) -> RowGroupBoundaryComponent {
        match self {
            Self::Utf8 {
                values,
                descending,
                nulls_first,
            } => RowGroupBoundaryComponent {
                value: string_array_value(values, row_idx)
                    .map(|value| RowGroupBoundaryValue::String(value.to_string())),
                descending: *descending,
                nulls_first: *nulls_first,
            },
            Self::TimestampMicrosecond {
                values,
                descending,
                nulls_first,
            } => RowGroupBoundaryComponent {
                value: timestamp_array_value(values, row_idx).map(RowGroupBoundaryValue::TimestampMicros),
                descending: *descending,
                nulls_first: *nulls_first,
            },
        }
    }
}

fn string_array_value(values: &StringArray, row_idx: usize) -> Option<&str> {
    (!values.is_null(row_idx)).then(|| values.value(row_idx))
}

fn timestamp_array_value(values: &TimestampMicrosecondArray, row_idx: usize) -> Option<i64> {
    (!values.is_null(row_idx)).then(|| values.value(row_idx))
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{
        NestedField, NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder, Transform, Type,
    };

    use super::{SortColumnsDescriptor, resolve_logs_sort_schema_field};
    use crate::error::IngestError;

    #[test]
    fn logs_sort_descriptor_matches_logs_sort_order() {
        let schema = icegate_common::schema::logs_schema().expect("logs schema");
        let sort_order = icegate_common::schema::logs_sort_order(&schema).expect("logs sort order");
        let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");

        assert_eq!(descriptor.columns().len(), sort_order.fields.len());

        for (descriptor_column, sort_field) in descriptor.columns().iter().zip(&sort_order.fields) {
            let schema_field = resolve_logs_sort_schema_field(&schema, sort_field).expect("sort field must resolve");
            assert_eq!(descriptor_column.column_name, schema_field.name);
            assert_eq!(
                descriptor_column.primitive_type,
                schema_field
                    .field_type
                    .as_primitive_type()
                    .expect("sort field must be primitive")
                    .clone()
            );
            assert_eq!(
                descriptor_column.descending,
                matches!(sort_field.direction, SortDirection::Descending)
            );
            assert_eq!(
                descriptor_column.nulls_first,
                matches!(sort_field.null_order, NullOrder::First)
            );
        }
    }

    #[test]
    fn sort_descriptor_rejects_unsupported_transform_without_panic() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "tenant_id", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");
        let sort_order = SortOrder::builder()
            .with_sort_field(SortField {
                source_id: 1,
                transform: Transform::Bucket(16),
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .build_unbound()
            .expect("sort order");

        let err = SortColumnsDescriptor::try_from_schema_sort_order(&schema, &sort_order)
            .expect_err("unsupported transform must be rejected");

        match err {
            IngestError::Iceberg(err) => {
                assert!(err.to_string().contains("unsupported logs merge sort transform"));
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }

    #[test]
    fn sort_descriptor_rejects_missing_schema_field_without_panic() {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "tenant_id", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");
        let sort_order = SortOrder::builder()
            .with_sort_field(SortField {
                source_id: 99,
                transform: Transform::Identity,
                direction: SortDirection::Ascending,
                null_order: NullOrder::First,
            })
            .build_unbound()
            .expect("sort order");

        let err = SortColumnsDescriptor::try_from_schema_sort_order(&schema, &sort_order)
            .expect_err("missing source_id must be rejected");

        match err {
            IngestError::Iceberg(err) => {
                assert!(err.to_string().contains("source_id 99 not found"));
            }
            other => panic!("unexpected error variant: {other}"),
        }
    }
}
