//! Table rows for `OpenSearch` integration tests, written through the production Parquet writer
//! with the production `logs` bloom-filter columns and column encodings and committed as one
//! Iceberg append, so each tenant's rows land in their own identity partition exactly as ingest
//! writes them. Every value a test relies on is passed in explicitly.

use std::error::Error;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, MapBuilder, MapFieldNames, StringArray, StringBuilder, TimestampMicrosecondArray, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use iceberg::Catalog;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use icegate_common::iceberg_write::{CommonRecordBatchStream, WriteConfig, write_record_batches_to_parquet};
use icegate_common::parquet_encoding::{LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS};
use icegate_common::schema::{
    COL_BODY, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES, COL_OBSERVED_TIMESTAMP, COL_RESOURCE_ATTRIBUTES,
    COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_TENANT_ID, COL_TIMESTAMP,
};
use icegate_common::{LOGS_TABLE, icegate_table_ident};
use tokio_util::sync::CancellationToken;

/// One `logs` row. Every column not named here is null, except the observed and ingested
/// timestamps, which equal `timestamp_micros`, and `scope_attributes`, which is empty.
pub struct LogRow<'a> {
    /// The tenant the row belongs to.
    pub tenant_id: &'a str,
    /// Event time, microseconds since the epoch.
    pub timestamp_micros: i64,
    /// `service_name`.
    pub service_name: Option<&'a str>,
    /// `severity_text`.
    pub severity_text: Option<&'a str>,
    /// `body`.
    pub body: Option<&'a str>,
    /// `resource_attributes` entries, in the order given.
    pub resource_attributes: &'a [(&'a str, &'a str)],
    /// `log_attributes` entries, in the order given.
    pub log_attributes: &'a [(&'a str, &'a str)],
}

/// The production `logs` bloom-filter columns and encodings; sizes far above what a test writes.
const LOGS_TEST_WRITE_CONFIG: WriteConfig = WriteConfig {
    row_group_size: 8192,
    data_page_size_limit_bytes: 1024 * 1024,
    max_file_size_bytes: 128 * 1024 * 1024,
    bloom_filter_columns: LOGS_BLOOM_COLUMNS,
    column_encodings: LOGS_COLUMN_ENCODINGS,
};

/// Write `rows` into `logs` and commit them as one append (one new snapshot).
pub async fn commit_log_rows(catalog: &Arc<dyn Catalog>, rows: &[LogRow<'_>]) -> Result<(), Box<dyn Error>> {
    let table = catalog.load_table(&icegate_table_ident(LOGS_TABLE)).await?;
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| build_log_column(field, rows))
        .collect::<Result<Vec<ArrayRef>, Box<dyn Error>>>()?;
    let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), columns)?;
    let batches: CommonRecordBatchStream = Box::pin(futures::stream::iter([Ok(batch)]));
    let written_files = write_record_batches_to_parquet(
        table.clone(),
        LOGS_TEST_WRITE_CONFIG,
        batches,
        &CancellationToken::new(),
    )
    .await?;
    let append_action = Transaction::new(&table).fast_append().add_data_files(written_files.data_files);
    append_action.apply(Transaction::new(&table))?.commit(&**catalog).await?;
    Ok(())
}

/// The column `field` of `logs` for `rows`.
fn build_log_column(field: &Field, rows: &[LogRow<'_>]) -> Result<ArrayRef, Box<dyn Error>> {
    let column: ArrayRef = match field.name().as_str() {
        COL_TENANT_ID => Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.tenant_id))),
        COL_SERVICE_NAME => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.service_name))),
        COL_SEVERITY_TEXT => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.severity_text))),
        COL_BODY => Arc::new(StringArray::from_iter(rows.iter().map(|row| row.body))),
        COL_TIMESTAMP | COL_OBSERVED_TIMESTAMP | COL_INGESTED_TIMESTAMP => {
            let DataType::Timestamp(TimeUnit::Microsecond, timezone) = field.data_type() else {
                return Err(format!("{} is not a microsecond timestamp", field.name()).into());
            };
            Arc::new(
                TimestampMicrosecondArray::from_iter_values(rows.iter().map(|row| row.timestamp_micros))
                    .with_timezone_opt(timezone.clone()),
            )
        }
        COL_RESOURCE_ATTRIBUTES => build_map_column(field, rows.iter().map(|row| row.resource_attributes))?,
        COL_LOG_ATTRIBUTES => build_map_column(field, rows.iter().map(|row| row.log_attributes))?,
        COL_SCOPE_ATTRIBUTES => build_map_column(field, rows.iter().map(|_| &[][..]))?,
        _ => new_null_array(field.data_type(), rows.len()),
    };
    Ok(column)
}

/// A MAP column built with the entry, key and value fields of `field` itself, so the Iceberg
/// field-id metadata `RecordBatch::try_new` compares is the table's own.
fn build_map_column<'a>(
    field: &Field,
    rows: impl Iterator<Item = &'a [(&'a str, &'a str)]>,
) -> Result<ArrayRef, Box<dyn Error>> {
    let DataType::Map(entries, _) = field.data_type() else {
        return Err(format!("{} is not a MAP column", field.name()).into());
    };
    let DataType::Struct(entry_fields) = entries.data_type() else {
        return Err(format!("the entries of {} are not a struct", field.name()).into());
    };
    let (Some(key_field), Some(value_field)) = (entry_fields.first(), entry_fields.get(1)) else {
        return Err(format!("the entries of {} lack a key or a value", field.name()).into());
    };
    let names = MapFieldNames {
        entry: entries.name().clone(),
        key: key_field.name().clone(),
        value: value_field.name().clone(),
    };
    let mut builder = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new())
        .with_keys_field(Arc::clone(key_field))
        .with_values_field(Arc::clone(value_field));
    for pairs in rows {
        for (key, value) in pairs {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }
        builder.append(true)?;
    }
    Ok(Arc::new(builder.finish()))
}
