use std::{
    cmp::Ordering,
    sync::{Arc, OnceLock},
};

use arrow::{
    array::{Array, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray},
    record_batch::RecordBatch,
};
use iceberg::spec::{NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder, Transform};

use super::{RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryValue, compare_option_ord};
use crate::error::{IngestError, Result};

static LOGS_SORT_DESCRIPTOR: OnceLock<std::result::Result<SortColumnsDescriptor, String>> = OnceLock::new();
static SPANS_SORT_DESCRIPTOR: OnceLock<std::result::Result<SortColumnsDescriptor, String>> = OnceLock::new();

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

impl SortColumnsDescriptor {
    pub(crate) fn logs() -> Result<&'static Self> {
        match LOGS_SORT_DESCRIPTOR.get_or_init(|| Self::try_from_logs().map_err(|err| err.to_string())) {
            Ok(descriptor) => Ok(descriptor),
            Err(message) => Err(IngestError::Config(format!(
                "failed to initialize logs sort descriptor: {message}"
            ))),
        }
    }

    pub(crate) fn spans() -> Result<&'static Self> {
        match SPANS_SORT_DESCRIPTOR.get_or_init(|| Self::try_from_spans().map_err(|err| err.to_string())) {
            Ok(descriptor) => Ok(descriptor),
            Err(message) => Err(IngestError::Config(format!(
                "failed to initialize spans sort descriptor: {message}"
            ))),
        }
    }

    pub(crate) fn columns(&self) -> &[SortColumnDescriptor] {
        self.columns.as_slice()
    }

    /// Sort-key column names in Iceberg sort-order order.
    pub(crate) fn column_names(&self) -> Arc<[String]> {
        self.columns.iter().map(|column| column.column_name.clone()).collect()
    }

    fn try_from_logs() -> Result<Self> {
        let schema = icegate_common::schema::logs_schema()?;
        let sort_order = icegate_common::schema::logs_sort_order(&schema)?;
        Self::try_from_schema_sort_order(&schema, &sort_order)
    }

    fn try_from_spans() -> Result<Self> {
        let schema = icegate_common::schema::spans_schema()?;
        let sort_order = icegate_common::schema::spans_sort_order(&schema)?;
        Self::try_from_schema_sort_order(&schema, &sort_order)
    }

    fn try_from_schema_sort_order(schema: &Schema, sort_order: &SortOrder) -> Result<Self> {
        let mut columns = Vec::with_capacity(sort_order.fields.len());

        for sort_field in &sort_order.fields {
            let schema_field = resolve_sort_schema_field(schema, sort_field)?;
            let primitive_type = schema_field
                .field_type
                .as_primitive_type()
                .ok_or_else(|| IngestError::Shift(format!("sort field '{}' must be primitive", schema_field.name)))?
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
    column_names: Arc<[String]>,
}

impl SortColumnCache {
    pub(crate) fn try_new(batch: &RecordBatch, descriptor: &SortColumnsDescriptor, context: &str) -> Result<Self> {
        let mut columns = Vec::with_capacity(descriptor.columns.len());
        let column_names = descriptor.column_names();

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
                PrimitiveType::Fixed(expected_width) => {
                    let arr = column.as_any().downcast_ref::<FixedSizeBinaryArray>().ok_or_else(|| {
                        IngestError::Shift(format!("{column_name} must be FixedSizeBinary for {context}"))
                    })?;
                    let actual_width = arr.value_length();
                    let expected_i32 = i32::try_from(expected_width).map_err(|_| {
                        IngestError::Shift(format!(
                            "{column_name} schema width {expected_width} overflows i32 for {context}"
                        ))
                    })?;
                    if actual_width != expected_i32 {
                        return Err(IngestError::Shift(format!(
                            "{column_name} FixedSizeBinary width mismatch for {context}: \
                             expected {expected_width}, array reports {actual_width}"
                        )));
                    }
                    CachedSortColumn::FixedBytes {
                        values: arr.clone(),
                        descending: sort_column.descending,
                        nulls_first: sort_column.nulls_first,
                    }
                }
                ref other => {
                    return Err(IngestError::Shift(format!(
                        "unsupported sort column type for '{column_name}' in {context}: {other}"
                    )));
                }
            };
            columns.push(cached);
        }

        Ok(Self { columns, column_names })
    }

    pub(crate) fn column_names(&self) -> Arc<[String]> {
        Arc::clone(&self.column_names)
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
        RowGroupBoundaryKey::new(self.columns.iter().map(|column| column.boundary_component(row_idx)).collect())
    }
}

fn resolve_sort_schema_field<'a>(schema: &'a Schema, sort_field: &SortField) -> Result<&'a iceberg::spec::NestedField> {
    if sort_field.transform != Transform::Identity {
        return Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            format!("unsupported merge sort transform: {}", sort_field.transform),
        )
        .into());
    }

    schema
        .field_by_id(sort_field.source_id)
        .map(std::convert::AsRef::as_ref)
        .ok_or_else(|| {
            iceberg::Error::new(
                iceberg::ErrorKind::DataInvalid,
                format!("sort field source_id {} not found in schema", sort_field.source_id),
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
    FixedBytes {
        values: FixedSizeBinaryArray,
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
            Self::FixedBytes {
                values,
                descending,
                nulls_first,
            } => compare_option_ord(
                fixed_bytes_array_value(values, left_row_idx),
                fixed_bytes_array_value(values, right_row_idx),
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
            (
                Self::FixedBytes {
                    values: left,
                    descending,
                    nulls_first,
                },
                Self::FixedBytes { values: right, .. },
            ) => Ok(compare_option_ord(
                fixed_bytes_array_value(left, left_row_idx),
                fixed_bytes_array_value(right, right_row_idx),
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
            Self::FixedBytes {
                values,
                descending,
                nulls_first,
            } => RowGroupBoundaryComponent {
                value: fixed_bytes_array_value(values, row_idx)
                    .map(|bytes| RowGroupBoundaryValue::FixedBytes(bytes.to_vec())),
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

fn fixed_bytes_array_value(values: &FixedSizeBinaryArray, row_idx: usize) -> Option<&[u8]> {
    (!values.is_null(row_idx)).then(|| values.value(row_idx))
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, sync::Arc};

    use arrow::{
        array::{ArrayRef, FixedSizeBinaryBuilder},
        datatypes::{DataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    };
    use iceberg::spec::{
        NestedField, NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder, Transform, Type,
    };

    use super::{SortColumnCache, SortColumnsDescriptor, resolve_sort_schema_field};
    use crate::error::IngestError;

    #[test]
    fn spans_sort_descriptor_matches_spans_sort_order() {
        let schema = icegate_common::schema::spans_schema().expect("spans schema");
        let sort_order = icegate_common::schema::spans_sort_order(&schema).expect("spans sort order");
        let descriptor = SortColumnsDescriptor::spans().expect("spans descriptor");

        assert_eq!(descriptor.columns().len(), sort_order.fields.len());

        for (descriptor_column, sort_field) in descriptor.columns().iter().zip(&sort_order.fields) {
            let schema_field = resolve_sort_schema_field(&schema, sort_field).expect("sort field must resolve");
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
    fn logs_sort_descriptor_matches_logs_sort_order() {
        let schema = icegate_common::schema::logs_schema().expect("logs schema");
        let sort_order = icegate_common::schema::logs_sort_order(&schema).expect("logs sort order");
        let descriptor = SortColumnsDescriptor::logs().expect("logs descriptor");

        assert_eq!(descriptor.columns().len(), sort_order.fields.len());

        for (descriptor_column, sort_field) in descriptor.columns().iter().zip(&sort_order.fields) {
            let schema_field = resolve_sort_schema_field(&schema, sort_field).expect("sort field must resolve");
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
                assert!(err.to_string().contains("unsupported merge sort transform"));
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

    /// Build a single-column descriptor over `Fixed(16)`. Used by the `FixedBytes`
    /// cache tests below to exercise the new sort path.
    fn fixed_bytes_descriptor(descending: bool, nulls_first: bool) -> SortColumnsDescriptor {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "trace_id", Type::Primitive(PrimitiveType::Fixed(16))).into(),
            ])
            .build()
            .expect("schema");
        let sort_order = SortOrder::builder()
            .with_sort_field(SortField {
                source_id: 1,
                transform: Transform::Identity,
                direction: if descending {
                    SortDirection::Descending
                } else {
                    SortDirection::Ascending
                },
                null_order: if nulls_first { NullOrder::First } else { NullOrder::Last },
            })
            .build_unbound()
            .expect("sort order");
        SortColumnsDescriptor::try_from_schema_sort_order(&schema, &sort_order).expect("descriptor")
    }

    /// Build a `RecordBatch` with a single `trace_id` `FixedSizeBinary(16)` column
    /// from optional 16-byte slices. `None` entries become null.
    fn fixed_bytes_batch(values: &[Option<[u8; 16]>]) -> RecordBatch {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "trace_id",
            DataType::FixedSizeBinary(16),
            true,
        )]));
        let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), 16);
        for value in values {
            match value {
                Some(bytes) => builder.append_value(bytes).expect("append fixed bytes"),
                None => builder.append_null(),
            }
        }
        let array: ArrayRef = Arc::new(builder.finish());
        RecordBatch::try_new(arrow_schema, vec![array]).expect("batch")
    }

    #[test]
    fn sort_column_cache_supports_fixed_bytes_sort() {
        // trace_a1 < trace_a2 < trace_b lexicographically.
        let trace_a1 = [b'a', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_a2 = [b'a', 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_b = [b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        let batch = fixed_bytes_batch(&[Some(trace_b), Some(trace_a2), Some(trace_a1)]);
        let descriptor = fixed_bytes_descriptor(false, true);
        let cache = SortColumnCache::try_new(&batch, &descriptor, "fixed-bytes test").expect("cache");

        // Row 0 = trace_b, row 1 = trace_a2, row 2 = trace_a1.
        assert_eq!(cache.compare_indices(0, 1), Ordering::Greater);
        assert_eq!(cache.compare_indices(1, 2), Ordering::Greater);
        assert_eq!(cache.compare_indices(2, 1), Ordering::Less);
        // Stable fallback to row index when keys equal — same row compares Equal
        // through the column, then row index tiebreaker yields Equal.
        assert_eq!(cache.compare_indices(0, 0), Ordering::Equal);
    }

    #[test]
    fn sort_column_cache_supports_fixed_bytes_sort_descending() {
        // trace_a1 < trace_a2 < trace_b lexicographically; under descending
        // sort the returned `Ordering` is reversed for unequal keys, while
        // equal-row comparisons still resolve to `Equal` via the row-index
        // tiebreaker.
        let trace_a1 = [b'a', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_a2 = [b'a', 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_b = [b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        let batch = fixed_bytes_batch(&[Some(trace_b), Some(trace_a2), Some(trace_a1)]);
        let descriptor = fixed_bytes_descriptor(true, true);
        let cache = SortColumnCache::try_new(&batch, &descriptor, "fixed-bytes desc test").expect("cache");

        // Row 0 = trace_b, row 1 = trace_a2, row 2 = trace_a1.
        // Ascending would yield Greater/Greater/Less; descending reverses
        // the unequal pairs.
        assert_eq!(cache.compare_indices(0, 1), Ordering::Less);
        assert_eq!(cache.compare_indices(1, 2), Ordering::Less);
        assert_eq!(cache.compare_indices(2, 1), Ordering::Greater);
        // Stable tiebreaker for equal rows is unaffected by direction.
        assert_eq!(cache.compare_indices(0, 0), Ordering::Equal);
    }

    #[test]
    fn sort_column_cache_compares_across_streams_for_fixed_bytes() {
        let descriptor = fixed_bytes_descriptor(false, true);
        let left_batch = fixed_bytes_batch(&[Some([b'a', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]);
        let right_batch = fixed_bytes_batch(&[Some([b'a', 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])]);
        let left = SortColumnCache::try_new(&left_batch, &descriptor, "left").expect("left cache");
        let right = SortColumnCache::try_new(&right_batch, &descriptor, "right").expect("right cache");

        assert_eq!(left.compare_row(0, &right, 0).expect("compare"), Ordering::Less);
        assert_eq!(right.compare_row(0, &left, 0).expect("compare"), Ordering::Greater);
    }

    #[test]
    fn sort_column_cache_handles_nulls_in_fixed_bytes() {
        let value = [b'a', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let batch = fixed_bytes_batch(&[None, Some(value)]);

        // nulls_first=true: null sorts before non-null.
        let descriptor = fixed_bytes_descriptor(false, true);
        let cache = SortColumnCache::try_new(&batch, &descriptor, "nulls-first").expect("cache");
        assert_eq!(cache.compare_indices(0, 1), Ordering::Less);

        // nulls_first=false: null sorts after non-null.
        let descriptor = fixed_bytes_descriptor(false, false);
        let cache = SortColumnCache::try_new(&batch, &descriptor, "nulls-last").expect("cache");
        assert_eq!(cache.compare_indices(0, 1), Ordering::Greater);
    }

    #[test]
    fn spans_sort_descriptor_includes_fixed_trace_id() {
        let descriptor = SortColumnsDescriptor::spans().expect("spans descriptor");
        let trace_id_column = descriptor
            .columns()
            .iter()
            .find(|column| column.column_name == "trace_id")
            .expect("spans sort order must include trace_id");
        assert_eq!(trace_id_column.primitive_type, PrimitiveType::Fixed(16));
    }

    #[test]
    fn sort_column_cache_rejects_fixed_size_binary_with_wrong_width() {
        // Descriptor declares Fixed(16) for trace_id, but the batch arrives
        // with FixedSizeBinary(8). The cache builder must surface this as
        // IngestError::Shift instead of silently accepting a wrong-width array
        // and producing nonsense ordering downstream.
        let descriptor = fixed_bytes_descriptor(false, true);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "trace_id",
            DataType::FixedSizeBinary(8),
            true,
        )]));
        let mut builder = FixedSizeBinaryBuilder::with_capacity(1, 8);
        builder.append_value([0u8; 8]).expect("append 8 bytes");
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(arrow_schema, vec![array]).expect("batch");

        match SortColumnCache::try_new(&batch, &descriptor, "wrong-width") {
            Ok(_) => panic!("must reject mismatched widths"),
            Err(err) => assert!(matches!(err, IngestError::Shift(_))),
        }
    }
}
