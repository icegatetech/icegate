//! Utilities for building Iceberg data files from Parquet metadata.
//!
//! Extracts column-level statistics (lower/upper bounds, column sizes,
//! value counts, null counts) from parquet row-group metadata and populates
//! the Iceberg `DataFile` manifest entries. This enables file-level pruning
//! during scans — without bounds, the query engine must open every data file.

use std::collections::{HashMap, HashSet};

use iceberg::{
    arrow::ArrowFileReader,
    spec::{
        DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, ListType, Literal, MapType, NestedFieldRef,
        PrimitiveType, Schema, SchemaVisitor, Struct, StructType, Type, visit_schema,
    },
    table::Table,
};
use parquet::{arrow::async_reader::AsyncFileReader, file::statistics::Statistics};
use uuid::Uuid;

use crate::error::{IngestError, Result};

/// Builds Iceberg data files from parquet file paths by reading parquet metadata.
#[tracing::instrument(skip(table), fields(path_count = parquet_paths.len()), err)]
pub async fn data_files_from_parquet_paths(table: &Table, parquet_paths: &[String]) -> Result<Vec<DataFile>> {
    // TODO(med): We have at least 3 S3 API calls (HEAD, GET footer size, GET footer range).
    if parquet_paths.is_empty() {
        return Ok(Vec::new());
    }

    let partition_spec = table.metadata().default_partition_spec();
    let partition_type = partition_spec
        .partition_type(table.metadata().current_schema())
        .map_err(|e| IngestError::Shift(format!("failed to build partition type: {e}")))?;

    let schema = table.metadata().current_schema();
    let path_to_field_id = build_parquet_path_index(schema)?;

    let mut data_files = Vec::with_capacity(parquet_paths.len());
    for path in parquet_paths {
        let input_file = table
            .file_io()
            .new_input(path)
            .map_err(|e| IngestError::Shift(format!("failed to open parquet file '{path}': {e}")))?;
        let file_metadata = input_file
            .metadata()
            .await
            .map_err(|e| IngestError::Shift(format!("failed to stat parquet file '{path}': {e}")))?;
        let file_size_in_bytes = file_metadata.size;
        let reader = input_file
            .reader()
            .await
            .map_err(|e| IngestError::Shift(format!("failed to read parquet file '{path}': {e}")))?;

        let mut parquet_reader = ArrowFileReader::new(file_metadata, reader);
        let parquet_metadata = parquet_reader
            .get_metadata(None)
            .await
            .map_err(|e| IngestError::Shift(format!("failed to read parquet metadata '{path}': {e}")))?;
        let record_count = parquet_metadata.file_metadata().num_rows();
        let record_count = u64::try_from(record_count)
            .map_err(|_| IngestError::Shift(format!("parquet record count {record_count} is negative")))?;

        let col_stats = extract_column_statistics(schema, &parquet_metadata, &path_to_field_id);

        let partition = partition_from_path(path, partition_spec, &partition_type)?;
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.clone())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(file_size_in_bytes)
            .record_count(record_count)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(partition)
            .column_sizes(col_stats.column_sizes)
            .value_counts(col_stats.value_counts)
            .null_value_counts(col_stats.null_value_counts)
            .lower_bounds(col_stats.lower_bounds)
            .upper_bounds(col_stats.upper_bounds)
            .split_offsets(Some(
                parquet_metadata
                    .row_groups()
                    .iter()
                    .filter_map(parquet::file::metadata::RowGroupMetaData::file_offset)
                    .collect(),
            ))
            .build()
            .map_err(|e| IngestError::Shift(format!("failed to build data file for '{path}': {e}")))?;

        data_files.push(data_file);
    }

    Ok(data_files)
}

#[tracing::instrument(level = "debug", skip(partition_spec, partition_type))]
fn partition_from_path(
    file_path: &str,
    partition_spec: &iceberg::spec::PartitionSpec,
    partition_type: &iceberg::spec::StructType,
) -> Result<Struct> {
    if partition_spec.is_unpartitioned() {
        return Ok(Struct::empty());
    }

    let mut values_by_name = HashMap::new();
    for segment in file_path.split('/') {
        if let Some((name, value)) = segment.split_once('=') {
            values_by_name.insert(name, value);
        }
    }

    let mut fields = Vec::with_capacity(partition_spec.fields().len());
    for (index, field) in partition_spec.fields().iter().enumerate() {
        let value = values_by_name.get(field.name.as_str()).ok_or_else(|| {
            IngestError::Shift(format!(
                "missing partition value '{}' in path '{}'",
                field.name, file_path
            ))
        })?;

        if *value == "null" {
            fields.push(None);
            continue;
        }

        let field_type = &partition_type.fields()[index].field_type;
        let literal = parse_partition_literal(field_type, value)?;
        fields.push(Some(literal));
    }

    Ok(Struct::from_iter(fields))
}

fn parse_partition_literal(field_type: &Type, value: &str) -> Result<Literal> {
    match field_type {
        Type::Primitive(PrimitiveType::String) => Ok(Literal::string(value)),
        Type::Primitive(PrimitiveType::Int) => value
            .parse::<i32>()
            .map(Literal::int)
            .map_err(|e| IngestError::Shift(format!("invalid int partition value '{value}': {e}"))),
        Type::Primitive(PrimitiveType::Long) => value
            .parse::<i64>()
            .map(Literal::long)
            .map_err(|e| IngestError::Shift(format!("invalid long partition value '{value}': {e}"))),
        Type::Primitive(PrimitiveType::Boolean) => value
            .parse::<bool>()
            .map(Literal::bool)
            .map_err(|e| IngestError::Shift(format!("invalid bool partition value '{value}': {e}"))),
        Type::Primitive(PrimitiveType::Date) => {
            Literal::date_from_str(value).map_err(|e| IngestError::Shift(format!("invalid date '{value}': {e}")))
        }
        Type::Primitive(PrimitiveType::Timestamp) => Literal::timestamp_from_str(value)
            .map_err(|e| IngestError::Shift(format!("invalid timestamp '{value}': {e}"))),
        Type::Primitive(PrimitiveType::Timestamptz) => Literal::timestamptz_from_str(value)
            .map_err(|e| IngestError::Shift(format!("invalid timestamptz '{value}': {e}"))),
        Type::Primitive(PrimitiveType::Uuid) => Uuid::parse_str(value)
            .map(Literal::uuid)
            .map_err(|e| IngestError::Shift(format!("invalid uuid '{value}': {e}"))),
        _ => Err(IngestError::Shift(format!(
            "unsupported partition type '{field_type:?}'"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Parquet column path → Iceberg field ID mapping
// ---------------------------------------------------------------------------

/// Builds a mapping from parquet column dotted path names to Iceberg field IDs.
///
/// The parquet column path uses dot-separated segments matching the Iceberg
/// schema's field names. For example, a top-level field `timestamp` maps to
/// path `timestamp`, while a map's key/value use `key_value.key` and
/// `key_value.value` suffixes.
fn build_parquet_path_index(schema: &Schema) -> Result<HashMap<String, i32>> {
    let mut visitor = ParquetPathVisitor {
        path_to_id: HashMap::new(),
        field_names: Vec::new(),
        current_field_id: 0,
    };
    visit_schema(schema, &mut visitor).map_err(|e| IngestError::Shift(format!("schema visit failed: {e}")))?;
    Ok(visitor.path_to_id)
}

/// Schema visitor that builds a parquet column path to field ID index.
///
/// Mirrors the `IndexByParquetPathName` visitor from iceberg-rust's writer
/// module (which is `pub(crate)` and not accessible externally).
struct ParquetPathVisitor {
    path_to_id: HashMap<String, i32>,
    field_names: Vec<String>,
    current_field_id: i32,
}

/// Parquet's default field name for map entries.
const PARQUET_MAP_FIELD_NAME: &str = "key_value";

impl SchemaVisitor for ParquetPathVisitor {
    type T = ();

    fn before_struct_field(&mut self, field: &NestedFieldRef) -> std::result::Result<(), iceberg::Error> {
        self.field_names.push(field.name.clone());
        self.current_field_id = field.id;
        Ok(())
    }

    fn after_struct_field(&mut self, _field: &NestedFieldRef) -> std::result::Result<(), iceberg::Error> {
        self.field_names.pop();
        Ok(())
    }

    fn before_list_element(&mut self, field: &NestedFieldRef) -> std::result::Result<(), iceberg::Error> {
        self.field_names.push(format!("list.{}", field.name));
        self.current_field_id = field.id;
        Ok(())
    }

    fn after_list_element(&mut self, _field: &NestedFieldRef) -> std::result::Result<(), iceberg::Error> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_key(&mut self, field: &NestedFieldRef) -> std::result::Result<(), iceberg::Error> {
        self.field_names.push(format!("{PARQUET_MAP_FIELD_NAME}.key"));
        self.current_field_id = field.id;
        Ok(())
    }

    fn after_map_key(&mut self, _field: &NestedFieldRef) -> std::result::Result<(), iceberg::Error> {
        self.field_names.pop();
        Ok(())
    }

    fn before_map_value(&mut self, field: &NestedFieldRef) -> std::result::Result<(), iceberg::Error> {
        self.field_names.push(format!("{PARQUET_MAP_FIELD_NAME}.value"));
        self.current_field_id = field.id;
        Ok(())
    }

    fn after_map_value(&mut self, _field: &NestedFieldRef) -> std::result::Result<(), iceberg::Error> {
        self.field_names.pop();
        Ok(())
    }

    fn schema(&mut self, _schema: &Schema, _value: Self::T) -> std::result::Result<Self::T, iceberg::Error> {
        Ok(())
    }

    fn field(&mut self, _field: &NestedFieldRef, _value: Self::T) -> std::result::Result<Self::T, iceberg::Error> {
        Ok(())
    }

    fn r#struct(
        &mut self,
        _struct: &StructType,
        _results: Vec<Self::T>,
    ) -> std::result::Result<Self::T, iceberg::Error> {
        Ok(())
    }

    fn list(&mut self, _list: &ListType, _value: Self::T) -> std::result::Result<Self::T, iceberg::Error> {
        Ok(())
    }

    fn map(
        &mut self,
        _map: &MapType,
        _key_value: Self::T,
        _value: Self::T,
    ) -> std::result::Result<Self::T, iceberg::Error> {
        Ok(())
    }

    fn primitive(&mut self, _p: &PrimitiveType) -> std::result::Result<Self::T, iceberg::Error> {
        let full_name = self.field_names.join(".");
        self.path_to_id.insert(full_name, self.current_field_id);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Column statistics extraction
// ---------------------------------------------------------------------------

/// Column-level statistics extracted from parquet row-group metadata.
struct ColumnStatistics {
    /// Compressed byte sizes per column.
    column_sizes: HashMap<i32, u64>,
    /// Total value counts per column.
    value_counts: HashMap<i32, u64>,
    /// Null value counts per column.
    null_value_counts: HashMap<i32, u64>,
    /// File-level lower bounds per column (min of row-group mins).
    lower_bounds: HashMap<i32, Datum>,
    /// File-level upper bounds per column (max of row-group maxes).
    upper_bounds: HashMap<i32, Datum>,
}

/// Extracts column-level statistics from parquet row-group metadata.
///
/// Aggregates per-column sizes, value counts, null counts, and min/max bounds
/// across all row groups in a single parquet file. The resulting maps are keyed
/// by Iceberg field ID.
fn extract_column_statistics(
    schema: &Schema,
    parquet_metadata: &parquet::file::metadata::ParquetMetaData,
    path_to_field_id: &HashMap<String, i32>,
) -> ColumnStatistics {
    let mut column_sizes: HashMap<i32, u64> = HashMap::new();
    let mut value_counts: HashMap<i32, u64> = HashMap::new();
    let mut null_value_counts: HashMap<i32, u64> = HashMap::new();
    let mut lower_bounds: HashMap<i32, Datum> = HashMap::new();
    let mut upper_bounds: HashMap<i32, Datum> = HashMap::new();

    // Track fields where any row group lacks complete statistics.
    // Once a field is marked incomplete, its null counts / bounds are
    // removed and no longer accumulated — partial file-level stats are
    // worse than no stats because they can cause incorrect pruning.
    let mut incomplete_null_counts: HashSet<i32> = HashSet::new();
    let mut incomplete_lower: HashSet<i32> = HashSet::new();
    let mut incomplete_upper: HashSet<i32> = HashSet::new();

    for row_group in parquet_metadata.row_groups() {
        for col_meta in row_group.columns() {
            let parquet_path = col_meta.column_descr().path().string();
            let Some(&fid) = path_to_field_id.get(&parquet_path) else {
                continue;
            };

            // Parquet guarantees non-negative sizes/counts; cast is safe.
            #[allow(clippy::cast_sign_loss)]
            {
                *column_sizes.entry(fid).or_insert(0) += col_meta.compressed_size() as u64;
                *value_counts.entry(fid).or_insert(0) += col_meta.num_values() as u64;
            }

            let Some(stats) = col_meta.statistics() else {
                // No statistics at all — invalidate all stat fields.
                mark_incomplete(fid, &mut incomplete_null_counts, &mut null_value_counts);
                mark_incomplete(fid, &mut incomplete_lower, &mut lower_bounds);
                mark_incomplete(fid, &mut incomplete_upper, &mut upper_bounds);
                continue;
            };

            // --- null counts ---
            if !incomplete_null_counts.contains(&fid) {
                if let Some(null_count) = stats.null_count_opt() {
                    *null_value_counts.entry(fid).or_insert(0) += null_count;
                } else {
                    mark_incomplete(fid, &mut incomplete_null_counts, &mut null_value_counts);
                }
            }

            // --- lower / upper bounds ---
            let prim_type = schema.field_by_id(fid).and_then(|f| match *f.field_type {
                Type::Primitive(ref p) => Some(p.clone()),
                _ => None,
            });

            // Non-primitive fields cannot carry bounds.
            let Some(prim_type) = prim_type else {
                mark_incomplete(fid, &mut incomplete_lower, &mut lower_bounds);
                mark_incomplete(fid, &mut incomplete_upper, &mut upper_bounds);
                continue;
            };

            // Lower bound
            if !incomplete_lower.contains(&fid) {
                if stats.min_is_exact() {
                    match parquet_stat_min_as_datum(&prim_type, stats) {
                        Ok(Some(min_datum)) => update_lower_bound(&mut lower_bounds, fid, min_datum),
                        _ => mark_incomplete(fid, &mut incomplete_lower, &mut lower_bounds),
                    }
                } else {
                    mark_incomplete(fid, &mut incomplete_lower, &mut lower_bounds);
                }
            }

            // Upper bound
            if !incomplete_upper.contains(&fid) {
                if stats.max_is_exact() {
                    match parquet_stat_max_as_datum(&prim_type, stats) {
                        Ok(Some(max_datum)) => update_upper_bound(&mut upper_bounds, fid, max_datum),
                        _ => mark_incomplete(fid, &mut incomplete_upper, &mut upper_bounds),
                    }
                } else {
                    mark_incomplete(fid, &mut incomplete_upper, &mut upper_bounds);
                }
            }
        }
    }

    ColumnStatistics {
        column_sizes,
        value_counts,
        null_value_counts,
        lower_bounds,
        upper_bounds,
    }
}

/// Marks a field as incomplete and removes any previously accumulated value.
///
/// Once a field is marked incomplete it stays incomplete for the rest of
/// the file — partial aggregation across row groups would produce an
/// incorrect file-level statistic.
fn mark_incomplete<V>(fid: i32, incomplete: &mut HashSet<i32>, map: &mut HashMap<i32, V>) {
    if incomplete.insert(fid) {
        map.remove(&fid);
    }
}

/// Updates the file-level lower bound for a column (min of all row-group mins).
fn update_lower_bound(bounds: &mut HashMap<i32, Datum>, fid: i32, datum: Datum) {
    bounds
        .entry(fid)
        .and_modify(|existing| {
            if *existing > datum {
                *existing = datum.clone();
            }
        })
        .or_insert(datum);
}

/// Updates the file-level upper bound for a column (max of all row-group maxes).
fn update_upper_bound(bounds: &mut HashMap<i32, Datum>, fid: i32, datum: Datum) {
    bounds
        .entry(fid)
        .and_modify(|existing| {
            if *existing < datum {
                *existing = datum.clone();
            }
        })
        .or_insert(datum);
}

// ---------------------------------------------------------------------------
// Parquet Statistics → Iceberg Datum conversion
// ---------------------------------------------------------------------------

/// Converts the minimum value from parquet column statistics to an Iceberg `Datum`.
///
/// Returns `Ok(None)` when the parquet statistics type doesn't match the Iceberg
/// type (e.g., nested/complex types). Only handles primitive types relevant to
/// our schemas: Boolean, Int, Long, Float, Double, String, Timestamp, Timestamptz.
fn parquet_stat_min_as_datum(
    prim_type: &PrimitiveType,
    stats: &Statistics,
) -> std::result::Result<Option<Datum>, iceberg::Error> {
    Ok(match (prim_type, stats) {
        (PrimitiveType::Boolean, Statistics::Boolean(s)) => s.min_opt().map(|v| Datum::bool(*v)),
        (PrimitiveType::Int, Statistics::Int32(s)) => s.min_opt().map(|v| Datum::int(*v)),
        (PrimitiveType::Date, Statistics::Int32(s)) => s.min_opt().map(|v| Datum::date(*v)),
        (PrimitiveType::Long, Statistics::Int64(s)) => s.min_opt().map(|v| Datum::long(*v)),
        (PrimitiveType::Timestamp, Statistics::Int64(s)) => s.min_opt().map(|v| Datum::timestamp_micros(*v)),
        (PrimitiveType::Timestamptz, Statistics::Int64(s)) => s.min_opt().map(|v| Datum::timestamptz_micros(*v)),
        (PrimitiveType::Float, Statistics::Float(s)) => s.min_opt().map(|v| Datum::float(*v)),
        (PrimitiveType::Double, Statistics::Double(s)) => s.min_opt().map(|v| Datum::double(*v)),
        (PrimitiveType::String, Statistics::ByteArray(s)) => {
            let Some(val) = s.min_opt() else {
                return Ok(None);
            };
            Some(Datum::string(val.as_utf8()?))
        }
        _ => None,
    })
}

/// Converts the maximum value from parquet column statistics to an Iceberg `Datum`.
///
/// Mirror of [`parquet_stat_min_as_datum`] using `max_opt` instead of `min_opt`.
fn parquet_stat_max_as_datum(
    prim_type: &PrimitiveType,
    stats: &Statistics,
) -> std::result::Result<Option<Datum>, iceberg::Error> {
    Ok(match (prim_type, stats) {
        (PrimitiveType::Boolean, Statistics::Boolean(s)) => s.max_opt().map(|v| Datum::bool(*v)),
        (PrimitiveType::Int, Statistics::Int32(s)) => s.max_opt().map(|v| Datum::int(*v)),
        (PrimitiveType::Date, Statistics::Int32(s)) => s.max_opt().map(|v| Datum::date(*v)),
        (PrimitiveType::Long, Statistics::Int64(s)) => s.max_opt().map(|v| Datum::long(*v)),
        (PrimitiveType::Timestamp, Statistics::Int64(s)) => s.max_opt().map(|v| Datum::timestamp_micros(*v)),
        (PrimitiveType::Timestamptz, Statistics::Int64(s)) => s.max_opt().map(|v| Datum::timestamptz_micros(*v)),
        (PrimitiveType::Float, Statistics::Float(s)) => s.max_opt().map(|v| Datum::float(*v)),
        (PrimitiveType::Double, Statistics::Double(s)) => s.max_opt().map(|v| Datum::double(*v)),
        (PrimitiveType::String, Statistics::ByteArray(s)) => {
            let Some(val) = s.max_opt() else {
                return Ok(None);
            };
            Some(Datum::string(val.as_utf8()?))
        }
        _ => None,
    })
}
