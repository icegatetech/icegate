//! Sort-key comparison plan and row-group boundary key types.
//!
//! These types turn a table's Iceberg sort order into a runtime comparison
//! plan and capture the min/max sort key of a sorted row group.
//!
//! A table's Iceberg sort order is compiled once into a
//! [`SortColumnsDescriptor`] (column name, primitive type, direction, and
//! null ordering per sort field). Given an Arrow [`RecordBatch`], a
//! [`SortColumnCache`] downcasts the sort columns once and then compares rows
//! either within a single batch ([`SortColumnCache::compare_indices`]) or
//! across two batches ([`SortColumnCache::compare_row`]). The inclusive
//! min/max sort key of a sorted row group is captured as a
//! [`RowGroupBoundaryRange`], which serializes to JSON for WAL metadata and is
//! reconstructed by the Shifter and the Parquet compaction scan.

use std::{
    cmp::Ordering,
    sync::{Arc, OnceLock},
};

use arrow::{
    array::{Array, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray},
    record_batch::RecordBatch,
};
use iceberg::spec::{NullOrder, PrimitiveType, Schema, SortDirection, SortField, SortOrder, Transform};
use serde::{Deserialize, Serialize};

use crate::{Error, Result};

static LOGS_SORT_DESCRIPTOR: OnceLock<std::result::Result<SortColumnsDescriptor, String>> = OnceLock::new();
static SPANS_SORT_DESCRIPTOR: OnceLock<std::result::Result<SortColumnsDescriptor, String>> = OnceLock::new();
static EVENTS_SORT_DESCRIPTOR: OnceLock<std::result::Result<SortColumnsDescriptor, String>> = OnceLock::new();
static METRICS_SORT_DESCRIPTOR: OnceLock<std::result::Result<SortColumnsDescriptor, String>> = OnceLock::new();
static OPERATIONS_SORT_DESCRIPTOR: OnceLock<std::result::Result<SortColumnsDescriptor, String>> = OnceLock::new();

/// One sort field compiled from an Iceberg sort order: the source column name,
/// its primitive type, and the direction / null ordering used when comparing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortColumnDescriptor {
    column_name: String,
    primitive_type: PrimitiveType,
    descending: bool,
    nulls_first: bool,
}

impl SortColumnDescriptor {
    /// Source column name for this sort field.
    #[must_use]
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Iceberg primitive type of the sort column.
    #[must_use]
    pub const fn primitive_type(&self) -> &PrimitiveType {
        &self.primitive_type
    }

    /// Whether this field sorts in descending order.
    #[must_use]
    pub const fn descending(&self) -> bool {
        self.descending
    }

    /// Whether nulls sort before non-null values.
    #[must_use]
    pub const fn nulls_first(&self) -> bool {
        self.nulls_first
    }
}

/// The full sort-key comparison plan for a table, derived once from its
/// Iceberg schema and sort order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortColumnsDescriptor {
    columns: Vec<SortColumnDescriptor>,
}

impl SortColumnsDescriptor {
    /// Cached sort descriptor for the `logs` table.
    ///
    /// # Errors
    ///
    /// Returns an error if the logs schema/sort order cannot be built or
    /// contains a non-primitive or unsupported sort field.
    pub fn logs() -> Result<&'static Self> {
        match LOGS_SORT_DESCRIPTOR.get_or_init(|| Self::try_from_logs().map_err(|err| err.to_string())) {
            Ok(descriptor) => Ok(descriptor),
            Err(message) => Err(Error::Config(format!(
                "failed to initialize logs sort descriptor: {message}"
            ))),
        }
    }

    /// Cached sort descriptor for the `spans` table.
    ///
    /// # Errors
    ///
    /// Returns an error if the spans schema/sort order cannot be built or
    /// contains a non-primitive or unsupported sort field.
    pub fn spans() -> Result<&'static Self> {
        match SPANS_SORT_DESCRIPTOR.get_or_init(|| Self::try_from_spans().map_err(|err| err.to_string())) {
            Ok(descriptor) => Ok(descriptor),
            Err(message) => Err(Error::Config(format!(
                "failed to initialize spans sort descriptor: {message}"
            ))),
        }
    }

    /// Cached sort descriptor for the `events` table.
    ///
    /// # Errors
    ///
    /// Returns an error if the events schema/sort order cannot be built or
    /// contains a non-primitive or unsupported sort field.
    pub fn events() -> Result<&'static Self> {
        match EVENTS_SORT_DESCRIPTOR.get_or_init(|| Self::try_from_events().map_err(|err| err.to_string())) {
            Ok(descriptor) => Ok(descriptor),
            Err(message) => Err(Error::Config(format!(
                "failed to initialize events sort descriptor: {message}"
            ))),
        }
    }

    /// Cached sort descriptor for the `metrics` table.
    ///
    /// # Errors
    ///
    /// Returns an error if the metrics schema/sort order cannot be built or
    /// contains a non-primitive or unsupported sort field.
    pub fn metrics() -> Result<&'static Self> {
        match METRICS_SORT_DESCRIPTOR.get_or_init(|| Self::try_from_metrics().map_err(|err| err.to_string())) {
            Ok(descriptor) => Ok(descriptor),
            Err(message) => Err(Error::Config(format!(
                "failed to initialize metrics sort descriptor: {message}"
            ))),
        }
    }

    /// Cached sort descriptor for the `operations` table.
    ///
    /// # Errors
    ///
    /// Returns an error if the operations schema/sort order cannot be built or
    /// contains a non-primitive or unsupported sort field.
    pub fn operations() -> Result<&'static Self> {
        match OPERATIONS_SORT_DESCRIPTOR.get_or_init(|| Self::try_from_operations().map_err(|err| err.to_string())) {
            Ok(descriptor) => Ok(descriptor),
            Err(message) => Err(Error::Config(format!(
                "failed to initialize operations sort descriptor: {message}"
            ))),
        }
    }

    /// All sort fields in Iceberg sort-order order.
    #[must_use]
    pub fn columns(&self) -> &[SortColumnDescriptor] {
        self.columns.as_slice()
    }

    /// Sort-key column names in Iceberg sort-order order.
    #[must_use]
    pub fn column_names(&self) -> Arc<[String]> {
        self.columns.iter().map(|column| column.column_name.clone()).collect()
    }

    fn try_from_logs() -> Result<Self> {
        let schema = crate::schema::logs_schema()?;
        let sort_order = crate::schema::logs_sort_order(&schema)?;
        Self::try_from_schema_sort_order(&schema, &sort_order)
    }

    fn try_from_spans() -> Result<Self> {
        let schema = crate::schema::spans_schema()?;
        let sort_order = crate::schema::spans_sort_order(&schema)?;
        Self::try_from_schema_sort_order(&schema, &sort_order)
    }

    fn try_from_events() -> Result<Self> {
        let schema = crate::schema::events_schema()?;
        let sort_order = crate::schema::events_sort_order(&schema)?;
        Self::try_from_schema_sort_order(&schema, &sort_order)
    }

    fn try_from_metrics() -> Result<Self> {
        let schema = crate::schema::metrics_schema()?;
        let sort_order = crate::schema::metrics_sort_order(&schema)?;
        Self::try_from_schema_sort_order(&schema, &sort_order)
    }

    fn try_from_operations() -> Result<Self> {
        let schema = crate::schema::operations_schema()?;
        let sort_order = crate::schema::operations_sort_order(&schema)?;
        Self::try_from_schema_sort_order(&schema, &sort_order)
    }

    fn try_from_schema_sort_order(schema: &Schema, sort_order: &SortOrder) -> Result<Self> {
        let mut columns = Vec::with_capacity(sort_order.fields.len());

        for sort_field in &sort_order.fields {
            let schema_field = resolve_sort_schema_field(schema, sort_field)?;
            let primitive_type = schema_field
                .field_type
                .as_primitive_type()
                .ok_or_else(|| Error::Write(format!("sort field '{}' must be primitive", schema_field.name)))?
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

/// Sort columns of a single [`RecordBatch`], downcast once for repeated
/// comparison.
pub struct SortColumnCache {
    columns: Vec<CachedSortColumn>,
    column_names: Arc<[String]>,
}

impl SortColumnCache {
    /// Downcast the sort columns described by `descriptor` out of `batch`.
    ///
    /// `context` is included in error messages to identify the call site
    /// (e.g. `"WAL sorting"`, `"merge input"`).
    ///
    /// # Errors
    ///
    /// Returns an error if a sort column is missing from the batch, has the
    /// wrong Arrow type for its declared primitive type, or (for
    /// `FixedSizeBinary`) has a width that disagrees with the schema.
    pub fn try_new(batch: &RecordBatch, descriptor: &SortColumnsDescriptor, context: &str) -> Result<Self> {
        let mut columns = Vec::with_capacity(descriptor.columns.len());
        let column_names = descriptor.column_names();

        for sort_column in descriptor.columns() {
            let column_name = sort_column.column_name.as_str();
            let column_idx = batch
                .schema()
                .index_of(column_name)
                .map_err(|err| Error::Write(format!("{context} is missing {column_name}: {err}")))?;
            let column = batch.column(column_idx);
            let cached = match sort_column.primitive_type {
                PrimitiveType::String => CachedSortColumn::Utf8 {
                    values: column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| Error::Write(format!("{column_name} must be Utf8 for {context}")))?
                        .clone(),
                    descending: sort_column.descending,
                    nulls_first: sort_column.nulls_first,
                },
                PrimitiveType::Timestamp => CachedSortColumn::TimestampMicrosecond {
                    values: column
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            Error::Write(format!("{column_name} must be Timestamp(Microsecond) for {context}"))
                        })?
                        .clone(),
                    descending: sort_column.descending,
                    nulls_first: sort_column.nulls_first,
                },
                PrimitiveType::Fixed(expected_width) => {
                    let arr = column
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .ok_or_else(|| Error::Write(format!("{column_name} must be FixedSizeBinary for {context}")))?;
                    let actual_width = arr.value_length();
                    let expected_i32 = i32::try_from(expected_width).map_err(|_| {
                        Error::Write(format!(
                            "{column_name} schema width {expected_width} overflows i32 for {context}"
                        ))
                    })?;
                    if actual_width != expected_i32 {
                        return Err(Error::Write(format!(
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
                    return Err(Error::Write(format!(
                        "unsupported sort column type for '{column_name}' in {context}: {other}"
                    )));
                }
            };
            columns.push(cached);
        }

        Ok(Self { columns, column_names })
    }

    /// Sort-key column names in Iceberg sort-order order.
    #[must_use]
    pub fn column_names(&self) -> Arc<[String]> {
        Arc::clone(&self.column_names)
    }

    /// Compare two rows of the same batch by sort key, breaking ties by row
    /// index so the ordering is stable.
    #[must_use]
    pub fn compare_indices(&self, left_row_idx: usize, right_row_idx: usize) -> Ordering {
        for column in &self.columns {
            let ordering = column.compare_same(left_row_idx, right_row_idx);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        left_row_idx.cmp(&right_row_idx)
    }

    /// Compare a row of this batch against a row of `right` by sort key.
    ///
    /// # Errors
    ///
    /// Returns an error if the two caches do not share the same sort-column
    /// arity (number of columns) or column types in the same order.
    pub fn compare_row(&self, left_row_idx: usize, right: &Self, right_row_idx: usize) -> Result<Ordering> {
        // Guard against differing arity before zipping: `zip` stops at the
        // shorter side, so without this check two caches of unequal column
        // count whose shared prefix compares equal would wrongly report
        // `Ordering::Equal`. Mirrors `RowGroupBoundaryKey::validate_compatible_structure`.
        if self.columns.len() != right.columns.len() {
            return Err(Error::Write(format!(
                "incompatible sort-column cache arity: column count differs ({}, {})",
                self.columns.len(),
                right.columns.len()
            )));
        }

        for (left, right) in self.columns.iter().zip(right.columns.iter()) {
            let ordering = left.compare_other(left_row_idx, right, right_row_idx)?;
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }

        Ok(Ordering::Equal)
    }

    /// Build the boundary key for a single row, capturing each sort field's
    /// value and ordering flags.
    #[must_use]
    pub fn boundary_key(&self, row_idx: usize) -> RowGroupBoundaryKey {
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
            _ => Err(Error::Write(
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

/// Scalar value stored in a row-group boundary key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RowGroupBoundaryValue {
    /// UTF-8 string value.
    String(String),
    /// Timestamp value in microseconds.
    TimestampMicros(i64),
    /// Fixed-length raw bytes (e.g. `trace_id`: 16 B, `span_id`: 8 B).
    FixedBytes(Vec<u8>),
}

impl RowGroupBoundaryValue {
    /// Human-readable type name used in compatibility error messages.
    #[must_use]
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::String(_) => "string",
            Self::TimestampMicros(_) => "timestamp_micros",
            Self::FixedBytes(_) => "fixed_bytes",
        }
    }
}

/// Inclusive boundary range for one sorted row group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowGroupBoundaryRange {
    /// Sort-field column names, parallel to `min_key.components` / `max_key.components`.
    pub names: Arc<[String]>,
    /// First key in the row group.
    pub min_key: RowGroupBoundaryKey,
    /// Last key in the row group.
    pub max_key: RowGroupBoundaryKey,
}

impl RowGroupBoundaryRange {
    /// Validate that boundary keys and `names` share the same arity and that the range is
    /// non-decreasing.
    ///
    /// # Errors
    ///
    /// Returns an error if `names` is empty, the key arities disagree, the key
    /// structures are incompatible, or `min_key` sorts after `max_key`.
    pub fn validate(&self) -> Result<()> {
        if self.names.is_empty() {
            return Err(Error::Write(
                "row group boundary range names must not be empty".to_string(),
            ));
        }
        if self.names.len() != self.min_key.components().len() || self.names.len() != self.max_key.components().len() {
            return Err(Error::Write(format!(
                "row group boundary range arity mismatch: names={}, min_key={}, max_key={}",
                self.names.len(),
                self.min_key.components().len(),
                self.max_key.components().len()
            )));
        }
        self.min_key.validate_compatible_structure(&self.max_key)?;
        if self.min_key.compare_checked(&self.max_key)? == Ordering::Greater {
            return Err(Error::Write("min_key must be <= max_key".to_string()));
        }
        Ok(())
    }
}

/// Boundary key for one end of a sorted row group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowGroupBoundaryKey {
    /// Sort-field components in Iceberg sort-order order.
    components: Vec<RowGroupBoundaryComponent>,
}

impl RowGroupBoundaryKey {
    /// Create a new boundary key from the given components.
    #[must_use]
    pub const fn new(components: Vec<RowGroupBoundaryComponent>) -> Self {
        Self { components }
    }

    /// Return all sort-field components as a slice.
    #[must_use]
    pub fn components(&self) -> &[RowGroupBoundaryComponent] {
        &self.components
    }

    /// Validate that both keys use identical component structure.
    ///
    /// # Errors
    ///
    /// Returns an error if the component counts differ or any component has a
    /// differing direction, null ordering, value type, or fixed-bytes width.
    pub fn validate_compatible_structure(&self, other: &Self) -> Result<()> {
        if self.components.len() != other.components.len() {
            return Err(Error::Write(format!(
                "incompatible boundary key structure: component count differs ({}, {})",
                self.components.len(),
                other.components.len()
            )));
        }

        for (idx, (left, right)) in self.components.iter().zip(other.components.iter()).enumerate() {
            left.validate_compatible_structure(right, idx)?;
        }

        Ok(())
    }

    /// Compare two boundary keys using the serialized sort semantics, first
    /// validating that the key structures are compatible.
    ///
    /// # Errors
    ///
    /// Returns an error if [`validate_compatible_structure`](Self::validate_compatible_structure)
    /// fails.
    pub fn compare_checked(&self, other: &Self) -> Result<Ordering> {
        self.validate_compatible_structure(other)?;
        Ok(self.compare(other))
    }

    /// Compare two boundary keys component-by-component, assuming compatible
    /// structure. Prefer [`compare_checked`](Self::compare_checked) when the
    /// structures are not already known to match.
    #[must_use]
    pub fn compare(&self, other: &Self) -> Ordering {
        for (left, right) in self.components.iter().zip(other.components.iter()) {
            let ordering = left.compare_unchecked(right);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        self.components.len().cmp(&other.components.len())
    }
}

/// One sort-field component inside a row-group boundary key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RowGroupBoundaryComponent {
    /// Serialized field value.
    pub value: Option<RowGroupBoundaryValue>,
    /// Whether this field sorts descending.
    pub descending: bool,
    /// Whether nulls sort before non-null values.
    pub nulls_first: bool,
}

impl RowGroupBoundaryComponent {
    fn validate_compatible_structure(&self, other: &Self, index: usize) -> Result<()> {
        if self.descending != other.descending {
            return Err(Error::Write(format!(
                "incompatible boundary key structure at component {index}: descending differs ({}, {})",
                self.descending, other.descending
            )));
        }
        if self.nulls_first != other.nulls_first {
            return Err(Error::Write(format!(
                "incompatible boundary key structure at component {index}: nulls_first differs ({}, {})",
                self.nulls_first, other.nulls_first
            )));
        }

        match (&self.value, &other.value) {
            (Some(left), Some(right)) if boundary_value_discriminant(left) != boundary_value_discriminant(right) => {
                Err(Error::Write(format!(
                    "incompatible boundary key structure at component {index}: value type differs ({}, {})",
                    left.type_name(),
                    right.type_name()
                )))
            }
            (Some(RowGroupBoundaryValue::FixedBytes(left)), Some(RowGroupBoundaryValue::FixedBytes(right)))
                if left.len() != right.len() =>
            {
                Err(Error::Write(format!(
                    "incompatible boundary key structure at component {index}: fixed_bytes width differs ({} bytes, {} bytes)",
                    left.len(),
                    right.len()
                )))
            }
            _ => Ok(()),
        }
    }

    fn compare_unchecked(&self, other: &Self) -> Ordering {
        debug_assert!(self.validate_compatible_structure(other, 0).is_ok());

        match (&self.value, &other.value) {
            (None, Some(_)) => {
                if self.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (Some(_), None) => {
                if self.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Some(RowGroupBoundaryValue::String(left)), Some(RowGroupBoundaryValue::String(right))) => {
                compare_option_ord(
                    Some(left.as_str()),
                    Some(right.as_str()),
                    self.descending,
                    self.nulls_first,
                )
            }
            (
                Some(RowGroupBoundaryValue::TimestampMicros(left)),
                Some(RowGroupBoundaryValue::TimestampMicros(right)),
            ) => compare_option_ord(Some(*left), Some(*right), self.descending, self.nulls_first),
            (Some(RowGroupBoundaryValue::FixedBytes(left)), Some(RowGroupBoundaryValue::FixedBytes(right))) => {
                compare_option_ord(
                    Some(left.as_slice()),
                    Some(right.as_slice()),
                    self.descending,
                    self.nulls_first,
                )
            }
            (None, None) | (Some(_), Some(_)) => Ordering::Equal,
        }
    }
}

/// Compare two optional `Ord` values with explicit direction and null
/// ordering, the building block for every sort-key comparison.
///
/// `descending` reverses the order of two present values; `nulls_first`
/// controls whether `None` sorts before or after `Some`.
#[must_use]
pub fn compare_option_ord<T: Ord>(left: Option<T>, right: Option<T>, descending: bool, nulls_first: bool) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (Some(_), None) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Some(left), Some(right)) => {
            let ordering = left.cmp(&right);
            if descending { ordering.reverse() } else { ordering }
        }
    }
}

const fn boundary_value_discriminant(value: &RowGroupBoundaryValue) -> u8 {
    match value {
        RowGroupBoundaryValue::String(_) => 0,
        RowGroupBoundaryValue::TimestampMicros(_) => 1,
        RowGroupBoundaryValue::FixedBytes(_) => 2,
    }
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

    use super::{
        RowGroupBoundaryComponent, RowGroupBoundaryKey, RowGroupBoundaryRange, RowGroupBoundaryValue, SortColumnCache,
        SortColumnsDescriptor, compare_option_ord, resolve_sort_schema_field,
    };
    use crate::Error;

    // Test-only boundary component constructors. These mirror the ingest WAL
    // `test_utils` helpers; they live here because the boundary tests moved
    // into this module and are gated behind `#[cfg(test)]`.
    fn boundary_component_string(
        value: Option<String>,
        descending: bool,
        nulls_first: bool,
    ) -> RowGroupBoundaryComponent {
        RowGroupBoundaryComponent {
            value: value.map(RowGroupBoundaryValue::String),
            descending,
            nulls_first,
        }
    }

    fn boundary_component_timestamp_micros(
        value: Option<i64>,
        descending: bool,
        nulls_first: bool,
    ) -> RowGroupBoundaryComponent {
        RowGroupBoundaryComponent {
            value: value.map(RowGroupBoundaryValue::TimestampMicros),
            descending,
            nulls_first,
        }
    }

    fn boundary_component_fixed_bytes(
        value: Option<Vec<u8>>,
        descending: bool,
        nulls_first: bool,
    ) -> RowGroupBoundaryComponent {
        RowGroupBoundaryComponent {
            value: value.map(RowGroupBoundaryValue::FixedBytes),
            descending,
            nulls_first,
        }
    }

    #[test]
    fn spans_sort_descriptor_matches_spans_sort_order() {
        let schema = crate::schema::spans_schema().expect("spans schema");
        let sort_order = crate::schema::spans_sort_order(&schema).expect("spans sort order");
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
        let schema = crate::schema::logs_schema().expect("logs schema");
        let sort_order = crate::schema::logs_sort_order(&schema).expect("logs sort order");
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
    fn metrics_sort_descriptor_matches_metrics_sort_order() {
        let schema = crate::schema::metrics_schema().expect("metrics schema");
        let sort_order = crate::schema::metrics_sort_order(&schema).expect("metrics sort order");
        let descriptor = SortColumnsDescriptor::metrics().expect("metrics descriptor");

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
    fn events_sort_descriptor_matches_events_sort_order() {
        let schema = crate::schema::events_schema().expect("events schema");
        let sort_order = crate::schema::events_sort_order(&schema).expect("events sort order");
        let descriptor = SortColumnsDescriptor::events().expect("events descriptor");

        // The events sort order has exactly two fields: service_name, timestamp.
        assert_eq!(descriptor.columns().len(), 2);
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
    fn operations_sort_descriptor_matches_operations_sort_order() {
        let schema = crate::schema::operations_schema().expect("operations schema");
        let sort_order = crate::schema::operations_sort_order(&schema).expect("operations sort order");
        let descriptor = SortColumnsDescriptor::operations().expect("operations descriptor");

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
            Error::Iceberg(err) => {
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
            Error::Iceberg(err) => {
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
        // Error::Write instead of silently accepting a wrong-width array
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
            Err(err) => assert!(matches!(err, Error::Write(_))),
        }
    }

    #[test]
    fn compare_option_ord_handles_desc_nulls_first() {
        assert_eq!(compare_option_ord(None::<i64>, Some(1), true, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(2), Some(1), true, true), Ordering::Less);
        assert_eq!(compare_option_ord(Some(1), Some(2), true, true), Ordering::Greater);
    }

    #[test]
    fn row_group_boundary_key_matches_logs_order() {
        let lower = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-1".to_string()), false, true),
            boundary_component_timestamp_micros(Some(20), true, true),
        ]);
        let higher = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-1".to_string()), false, true),
            boundary_component_timestamp_micros(Some(30), true, true),
        ]);
        let null_service = RowGroupBoundaryKey::new(vec![
            boundary_component_string(None, false, true),
            boundary_component_timestamp_micros(Some(1), true, true),
        ]);

        assert_eq!(
            null_service.compare_checked(&lower).expect("compatible keys"),
            Ordering::Less
        );
        assert_eq!(higher.compare_checked(&lower).expect("compatible keys"), Ordering::Less);
        assert_eq!(
            lower.compare_checked(&higher).expect("compatible keys"),
            Ordering::Greater
        );
    }

    fn names() -> Arc<[String]> {
        Arc::from(["service_name".to_string(), "timestamp".to_string()])
    }

    #[test]
    fn row_group_boundary_range_preserves_key_order() {
        let min_key = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-a".to_string()), false, true),
            boundary_component_timestamp_micros(Some(50), true, true),
        ]);
        let max_key = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-b".to_string()), false, true),
            boundary_component_timestamp_micros(Some(10), true, true),
        ]);
        let range = RowGroupBoundaryRange {
            names: names(),
            min_key,
            max_key,
        };

        assert_eq!(
            range.min_key.compare_checked(&range.max_key).expect("compatible keys"),
            Ordering::Less
        );
    }

    #[test]
    fn row_group_boundary_range_rejects_names_arity_mismatch() {
        let range = RowGroupBoundaryRange {
            names: Arc::from(["timestamp".to_string()]),
            min_key: RowGroupBoundaryKey::new(vec![
                boundary_component_string(Some("svc-1".to_string()), false, true),
                boundary_component_timestamp_micros(Some(10), true, true),
            ]),
            max_key: RowGroupBoundaryKey::new(vec![
                boundary_component_string(Some("svc-1".to_string()), false, true),
                boundary_component_timestamp_micros(Some(20), true, true),
            ]),
        };
        let err = range.validate().expect_err("arity mismatch must be rejected");
        assert!(matches!(err, Error::Write(message) if message.contains("arity mismatch")));
    }

    #[test]
    fn row_group_boundary_range_rejects_empty_names() {
        let range = RowGroupBoundaryRange {
            names: Arc::from([]),
            min_key: RowGroupBoundaryKey::new(Vec::new()),
            max_key: RowGroupBoundaryKey::new(Vec::new()),
        };
        let err = range.validate().expect_err("empty names must be rejected");
        assert!(matches!(err, Error::Write(message) if message.contains("must not be empty")));
    }

    #[test]
    fn row_group_boundary_range_handles_equal_prefix_and_timestamp_desc() {
        let min_key = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-1".to_string()), false, true),
            boundary_component_timestamp_micros(Some(30), true, true),
        ]);
        let max_key = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("svc-1".to_string()), false, true),
            boundary_component_timestamp_micros(Some(10), true, true),
        ]);

        assert_eq!(
            min_key.compare_checked(&max_key).expect("compatible keys"),
            Ordering::Less
        );
    }

    #[test]
    fn row_group_boundary_key_rejects_different_component_count() {
        let shorter = RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc-1".to_string()), false, true)]);
        let longer = RowGroupBoundaryKey::new(vec![
            boundary_component_string(Some("acc-1".to_string()), false, true),
            boundary_component_string(Some("svc-1".to_string()), false, true),
        ]);

        let err = shorter
            .compare_checked(&longer)
            .expect_err("different component count must be rejected");
        assert!(matches!(err, Error::Write(message) if message.contains("component count differs")));
    }

    #[test]
    fn row_group_boundary_key_rejects_different_component_flags() {
        let ascending =
            RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc-1".to_string()), false, true)]);
        let descending =
            RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc-1".to_string()), true, true)]);

        let err = ascending
            .compare_checked(&descending)
            .expect_err("different descending flag must be rejected");
        assert!(matches!(err, Error::Write(message) if message.contains("descending differs")));
    }

    #[test]
    fn row_group_boundary_key_rejects_different_component_types() {
        let string_key =
            RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc-1".to_string()), false, true)]);
        let timestamp_key = RowGroupBoundaryKey::new(vec![boundary_component_timestamp_micros(Some(10), false, true)]);

        let err = string_key
            .compare_checked(&timestamp_key)
            .expect_err("different component types must be rejected");
        assert!(matches!(err, Error::Write(message) if message.contains("value type differs")));
    }

    #[test]
    fn row_group_boundary_component_compares_fixed_bytes_lex() {
        // trace_id-style 16-byte sentinels: trace_a1 < trace_a2 < trace_b
        // when compared lexicographically.
        let trace_a1: Vec<u8> = vec![b'a', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_a2: Vec<u8> = vec![b'a', 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let trace_b: Vec<u8> = vec![b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        let lower = RowGroupBoundaryKey::new(vec![boundary_component_fixed_bytes(Some(trace_a1), false, true)]);
        let middle = RowGroupBoundaryKey::new(vec![boundary_component_fixed_bytes(
            Some(trace_a2.clone()),
            false,
            true,
        )]);
        let upper = RowGroupBoundaryKey::new(vec![boundary_component_fixed_bytes(Some(trace_b), false, true)]);

        assert_eq!(lower.compare_checked(&middle).expect("compatible keys"), Ordering::Less);
        assert_eq!(middle.compare_checked(&upper).expect("compatible keys"), Ordering::Less);
        // Descending direction reverses the order.
        let middle_desc = RowGroupBoundaryKey::new(vec![boundary_component_fixed_bytes(Some(trace_a2), true, true)]);
        let upper_desc = RowGroupBoundaryKey::new(vec![boundary_component_fixed_bytes(
            Some(vec![b'b', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]),
            true,
            true,
        )]);
        assert_eq!(
            middle_desc.compare_checked(&upper_desc).expect("compatible keys"),
            Ordering::Greater
        );
    }

    #[test]
    fn row_group_boundary_key_rejects_mixed_string_and_fixed_bytes() {
        let string_key =
            RowGroupBoundaryKey::new(vec![boundary_component_string(Some("acc-1".to_string()), false, true)]);
        let fixed_key = RowGroupBoundaryKey::new(vec![boundary_component_fixed_bytes(
            Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            false,
            true,
        )]);

        let err = string_key
            .compare_checked(&fixed_key)
            .expect_err("string vs fixed_bytes must be rejected");
        assert!(matches!(err, Error::Write(message) if message.contains("value type differs")));
    }

    #[test]
    fn row_group_boundary_key_rejects_mismatched_fixed_bytes_widths() {
        // A 16-byte (trace_id-shaped) component compared against an 8-byte
        // (span_id-shaped) component must fail compatibility — silently
        // accepting the mismatch lets `compare_unchecked` lex-compare slices
        // of unequal length and produce nonsense ordering across streams.
        let key_16 = RowGroupBoundaryKey::new(vec![boundary_component_fixed_bytes(Some(vec![0u8; 16]), false, true)]);
        let key_8 = RowGroupBoundaryKey::new(vec![boundary_component_fixed_bytes(Some(vec![0u8; 8]), false, true)]);

        match key_16.compare_checked(&key_8) {
            Ok(_) => panic!("mismatched fixed_bytes widths must be rejected"),
            Err(err) => assert!(matches!(err, Error::Write(_))),
        }
    }

    #[test]
    fn row_group_boundary_value_round_trips_fixed_bytes_json() {
        let original = RowGroupBoundaryRange {
            names: Arc::from([
                "service_name".to_string(),
                "trace_id".to_string(),
                "timestamp".to_string(),
            ]),
            min_key: RowGroupBoundaryKey::new(vec![
                boundary_component_string(Some("svc-1".to_string()), false, true),
                boundary_component_fixed_bytes(
                    Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
                    false,
                    true,
                ),
                boundary_component_timestamp_micros(Some(30), true, true),
            ]),
            max_key: RowGroupBoundaryKey::new(vec![
                boundary_component_string(Some("svc-1".to_string()), false, true),
                boundary_component_fixed_bytes(
                    Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 99]),
                    false,
                    true,
                ),
                boundary_component_timestamp_micros(Some(10), true, true),
            ]),
        };

        // Round-trip through serde_json directly: serialization lives in the
        // ingest WAL metadata module, but the value semantics under test are
        // owned here.
        let serialized = serde_json::to_string(&original).expect("serialize");
        let restored: RowGroupBoundaryRange = serde_json::from_str(&serialized).expect("deserialize");
        assert_eq!(restored, original);
    }
}
