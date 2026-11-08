//! Table rows for `OpenSearch` integration tests, written through the production Parquet writer
//! and committed as one Iceberg append that records the WAL offset, so each tenant's rows land in
//! their own identity partition exactly as ingest writes them and a merged WAL scan reads the
//! table. [`LogRow`] writes `logs`; [`Row`] writes any table and [`write_wal_segment`] a WAL
//! segment, both from the row helpers the crate's own tests share. Every value a test relies on
//! is passed in explicitly.

use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, MapBuilder, MapFieldNames, StringArray, StringBuilder, TimestampMicrosecondArray, new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use icegate_common::schema::{
    COL_BODY, COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES,
    COL_OBSERVED_TIMESTAMP, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT,
    COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP, COL_TRACE_ID,
};
use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE, OPERATIONS_TABLE};

pub use super::test_rows::{Cell, Row, RowWriteError, commit_rows, write_wal_segment};
use super::test_rows::{FINISH_REASONS, TEMPERATURE, build_operation_row, commit_batch, read_arrow_schema};

/// 2026-10-08T10:00:00Z in microseconds, the instant [`compute_micros_at_minute`] counts from.
const TEN_O_CLOCK_MICROS: i64 = 1_791_453_600_000_000;
const MINUTE_MICROS: i64 = 60_000_000;

/// The instant `minute` minutes past [`TEN_O_CLOCK_MICROS`], in microseconds: the time base every
/// minute-based row builder here counts from.
pub const fn compute_micros_at_minute(minute: i64) -> i64 {
    TEN_O_CLOCK_MICROS + minute * MINUTE_MICROS
}

/// A `checkout` log of `tenant_id` at the instant [`compute_micros_at_minute`] gives for `minute`.
pub const fn build_log_row_at_minute<'a>(
    tenant_id: &'a str,
    minute: i64,
    severity_text: &'a str,
    body: &'a str,
) -> LogRow<'a> {
    build_service_log_row_at_minute(tenant_id, minute, "checkout", severity_text, body)
}

/// A log of `tenant_id` and `service_name` at the instant [`compute_micros_at_minute`] gives for
/// `minute`.
pub const fn build_service_log_row_at_minute<'a>(
    tenant_id: &'a str,
    minute: i64,
    service_name: &'a str,
    severity_text: &'a str,
    body: &'a str,
) -> LogRow<'a> {
    LogRow {
        tenant_id,
        timestamp_micros: compute_micros_at_minute(minute),
        service_name: Some(service_name),
        severity_text: Some(severity_text),
        body: Some(body),
        resource_attributes: &[],
        log_attributes: &[],
    }
}

/// The same log as [`build_log_row_at_minute`], as a row of a WAL segment.
pub fn build_wal_log_row_at_minute<'a>(tenant: &'a str, minute: i64, severity_text: &'a str, body: &'a str) -> Row<'a> {
    Row {
        tenant,
        cells: vec![
            (COL_TIMESTAMP, Cell::Micros(compute_micros_at_minute(minute))),
            (COL_SERVICE_NAME, Cell::Text("checkout")),
            (COL_SEVERITY_TEXT, Cell::Text(severity_text)),
            (COL_BODY, Cell::Text(body)),
        ],
    }
}

/// Commit five logs of `tenant_id` as one append, by minute past [`TEN_O_CLOCK_MICROS`]:
///
/// | minute | `service_name` | `severity_text` | `body`                          |
/// |--------|----------------|-----------------|---------------------------------|
/// | 5      | `checkout`     | `ERROR`         | `payment declined for order 17` |
/// | 20     | `checkout`     | `INFO`          | `payment accepted for order 18` |
/// | 40     | `cart`         | `ERROR`         | `cart service unavailable`      |
/// | 70     | `checkout`     | `ERROR`         | `payment declined for order 19` |
/// | 150    | `checkout`     | `ERROR`         | `payment declined for order 20` |
pub async fn commit_five_checkout_and_cart_logs(
    catalog: &Arc<dyn Catalog>,
    tenant_id: &str,
) -> Result<(), RowWriteError> {
    commit_log_rows(
        catalog,
        &[
            build_service_log_row_at_minute(tenant_id, 5, "checkout", "ERROR", "payment declined for order 17"),
            build_service_log_row_at_minute(tenant_id, 20, "checkout", "INFO", "payment accepted for order 18"),
            build_service_log_row_at_minute(tenant_id, 40, "cart", "ERROR", "cart service unavailable"),
            build_service_log_row_at_minute(tenant_id, 70, "checkout", "ERROR", "payment declined for order 19"),
            build_service_log_row_at_minute(tenant_id, 150, "checkout", "ERROR", "payment declined for order 20"),
        ],
    )
    .await
}

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

/// Write `rows` into `logs` through the production writer as one append (one new snapshot); see
/// [`commit_batch`].
pub async fn commit_log_rows(catalog: &Arc<dyn Catalog>, rows: &[LogRow<'_>]) -> Result<(), RowWriteError> {
    let arrow_schema = read_arrow_schema(catalog, LOGS_TABLE).await?;
    let columns = arrow_schema
        .fields()
        .iter()
        .map(|field| build_log_column(field, rows))
        .collect::<Result<Vec<ArrayRef>, RowWriteError>>()?;
    let batch = RecordBatch::try_new(arrow_schema, columns)?;
    commit_batch(catalog, LOGS_TABLE, batch).await
}

/// The column `field` of `logs` for `rows`.
fn build_log_column(field: &Field, rows: &[LogRow<'_>]) -> Result<ArrayRef, RowWriteError> {
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
) -> Result<ArrayRef, RowWriteError> {
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

/// Create the exact-name table `name` with the schema `fields`, unpartitioned. A table read
/// through the tenant scope must carry a required `tenant_id` string column.
pub async fn create_exact_name_table(
    catalog: &Arc<dyn Catalog>,
    name: &str,
    fields: Vec<NestedField>,
) -> Result<(), RowWriteError> {
    let schema = IcebergSchema::builder().with_fields(fields.into_iter().map(Arc::new)).build()?;
    let creation = TableCreation::builder().name(name.to_string()).schema(schema).build();
    catalog
        .create_table(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), creation)
        .await?;
    Ok(())
}

/// A required string column, for test-created exact-name tables.
pub fn required_string(id: i32, name: &str) -> NestedField {
    NestedField::required(id, name, Type::Primitive(PrimitiveType::String))
}

/// One `operations` row. The operation runs from `timestamp_micros` to `end_timestamp_micros`
/// (`duration_micros` is their difference) and was ingested at `timestamp_micros`; every column
/// not named here is null, or its type's zero value when required.
pub struct OperationRow<'a> {
    /// The tenant the row belongs to.
    pub tenant_id: &'a str,
    /// `trace_id`.
    pub trace_id: [u8; 16],
    /// `span_id`.
    pub span_id: [u8; 8],
    /// Start time, microseconds since the epoch.
    pub timestamp_micros: i64,
    /// End time, microseconds since the epoch.
    pub end_timestamp_micros: i64,
    /// `operation_name`.
    pub operation_name: &'a str,
    /// `request_model`.
    pub request_model: Option<&'a str>,
    /// `input_tokens`.
    pub input_tokens: Option<i64>,
    /// `temperature`.
    pub temperature: Option<f64>,
    /// `finish_reasons`; `Some(&[])` is the empty list, `None` the null list.
    pub finish_reasons: Option<&'a [&'a str]>,
}

/// Write `rows` into `operations` as one append; see [`commit_rows`].
pub async fn commit_operation_rows(catalog: &Arc<dyn Catalog>, rows: &[OperationRow<'_>]) -> Result<(), RowWriteError> {
    let generic_rows: Vec<Row<'_>> = rows.iter().map(convert_operation_row).collect();
    commit_rows(catalog, OPERATIONS_TABLE, &generic_rows).await
}

/// The generic row of one operation, through the shared `build_operation_row`, which sets the
/// timestamp and the operation name.
fn convert_operation_row<'a>(row: &'a OperationRow<'a>) -> Row<'a> {
    let mut cells = vec![
        (COL_TRACE_ID, Cell::Bytes(&row.trace_id)),
        (COL_SPAN_ID, Cell::Bytes(&row.span_id)),
        (COL_END_TIMESTAMP, Cell::Micros(row.end_timestamp_micros)),
        (
            COL_DURATION_MICROS,
            Cell::Long(row.end_timestamp_micros - row.timestamp_micros),
        ),
        (COL_INGESTED_TIMESTAMP, Cell::Micros(row.timestamp_micros)),
    ];
    // `request_model` and `input_tokens` have no constant: no test of the crate's own layer
    // writes them, so a shared one would have no reader there. A renamed column still fails
    // the row builder, which refuses a cell naming a column the table lacks.
    cells.extend(row.request_model.map(|model| ("request_model", Cell::Text(model))));
    cells.extend(row.input_tokens.map(|tokens| ("input_tokens", Cell::Long(tokens))));
    cells.extend(row.temperature.map(|temperature| (TEMPERATURE, Cell::Double(temperature))));
    cells.extend(row.finish_reasons.map(|reasons| (FINISH_REASONS, Cell::TextList(reasons))));
    build_operation_row(row.tenant_id, row.operation_name, row.timestamp_micros, cells)
}
