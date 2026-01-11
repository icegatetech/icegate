//! Utilities for building Iceberg data files from Parquet metadata.

use std::collections::HashMap;

use iceberg::{
    arrow::ArrowFileReader,
    spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Literal, PrimitiveType, Struct, Type},
    table::Table,
};
use parquet::arrow::async_reader::AsyncFileReader;
use uuid::Uuid;

use crate::error::{IngestError, Result};

/// Builds Iceberg data files from parquet file paths by reading parquet metadata.
pub async fn data_files_from_parquet_paths(table: &Table, parquet_paths: &[String]) -> Result<Vec<DataFile>> {
    // TODO(med): We have at least 3 S3 API calls (HEAD, GET footer size, GET footer range).
    if parquet_paths.is_empty() {
        return Ok(Vec::new());
    }

    let partition_spec = table.metadata().default_partition_spec();
    let partition_type = partition_spec
        .partition_type(table.metadata().current_schema())
        .map_err(|e| IngestError::Shift(format!("failed to build partition type: {e}")))?;

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

        let partition = partition_from_path(path, partition_spec, &partition_type)?;
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.clone())
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(file_size_in_bytes)
            .record_count(record_count)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .partition(partition)
            .build()
            .map_err(|e| IngestError::Shift(format!("failed to build data file for '{path}': {e}")))?;

        data_files.push(data_file);
    }

    Ok(data_files)
}

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
