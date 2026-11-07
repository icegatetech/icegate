//! Table rows with explicit values for tests, written the way production writes them.
//!
//! One file serves two test layers: the crate's inline tests declare it as a `cfg(test)`
//! module, and the `OpenSearch` integration binary includes the same file by path, because an
//! integration binary cannot reach the crate's `cfg(test)` modules. It therefore names only
//! external crates, never `crate::`. Every `pub` item has a reader in both layers; the rest
//! serve the `pub` items only.
//!
//! Tables are created from `icegate_common::schema`, Iceberg appends go through the production
//! Parquet writer with the table's production bloom-filter columns and encodings, and every
//! append records [`COMMITTED_WAL_OFFSET`], so a merged scan reads the WAL from the next offset.

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array, ListArray,
    MapBuilder, MapFieldNames, StringArray, StringBuilder, TimestampMicrosecondArray, new_empty_array, new_null_array,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use iceberg::spec::{DataFile, PartitionSpec, Schema, SortOrder};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use icegate_common::iceberg_write::{CommonRecordBatchStream, WriteConfig, write_record_batches_to_parquet};
use icegate_common::parquet_encoding::{
    EVENTS_BLOOM_COLUMNS, EVENTS_COLUMN_ENCODINGS, LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS, METRICS_BLOOM_COLUMNS,
    METRICS_COLUMN_ENCODINGS, OPERATIONS_BLOOM_COLUMNS, OPERATIONS_COLUMN_ENCODINGS, SPANS_BLOOM_COLUMNS,
    SPANS_COLUMN_ENCODINGS,
};
use icegate_common::parquet_writer::ColumnEncoding;
use icegate_common::schema::{self, COL_TENANT_ID, COL_TIMESTAMP};
use icegate_common::{
    EVENTS_TABLE, ICEGATE_NAMESPACE, LOGS_TABLE, METRICS_TABLE, OPERATIONS_TABLE, PRICES_TABLE, SPANS_TABLE,
    WAL_OFFSET_PROPERTY, icegate_table_ident,
};
use icegate_queue::SegmentId;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio_util::sync::CancellationToken;

/// Why a table could not be created or its rows written.
pub type RowWriteError = Box<dyn Error + Send + Sync>;

/// The WAL offset every committed snapshot records. A table with a commit is read from the WAL
/// from `COMMITTED_WAL_OFFSET + 1` on, so a test writes that table's segments from there.
pub const COMMITTED_WAL_OFFSET: u64 = 0;

/// One production table: its name, the schema, partition spec and sort order builders of
/// `icegate_common::schema`, and the bloom-filter columns and column encodings production writes
/// it with.
struct ProductionTable {
    /// The table name in the IceGate namespace.
    name: &'static str,
    /// Builds the table's Iceberg schema.
    build_schema: fn() -> icegate_common::Result<Schema>,
    /// Builds the table's partition spec over its schema.
    build_partition_spec: fn(&Schema) -> icegate_common::Result<PartitionSpec>,
    /// Builds the table's sort order over its schema.
    build_sort_order: fn(&Schema) -> icegate_common::Result<SortOrder>,
    /// The columns production writes a bloom filter for; empty when it writes none.
    bloom_filter_columns: &'static [&'static str],
    /// The column encodings production writes; empty when it keeps the Parquet defaults.
    column_encodings: &'static [ColumnEncoding],
}

/// Every table of the IceGate namespace.
const PRODUCTION_TABLES: [ProductionTable; 6] = [
    ProductionTable {
        name: LOGS_TABLE,
        build_schema: schema::logs_schema,
        build_partition_spec: schema::logs_partition_spec,
        build_sort_order: schema::logs_sort_order,
        bloom_filter_columns: LOGS_BLOOM_COLUMNS,
        column_encodings: LOGS_COLUMN_ENCODINGS,
    },
    ProductionTable {
        name: SPANS_TABLE,
        build_schema: schema::spans_schema,
        build_partition_spec: schema::spans_partition_spec,
        build_sort_order: schema::spans_sort_order,
        bloom_filter_columns: SPANS_BLOOM_COLUMNS,
        column_encodings: SPANS_COLUMN_ENCODINGS,
    },
    ProductionTable {
        name: EVENTS_TABLE,
        build_schema: schema::events_schema,
        build_partition_spec: schema::events_partition_spec,
        build_sort_order: schema::events_sort_order,
        bloom_filter_columns: EVENTS_BLOOM_COLUMNS,
        column_encodings: EVENTS_COLUMN_ENCODINGS,
    },
    ProductionTable {
        name: METRICS_TABLE,
        build_schema: schema::metrics_schema,
        build_partition_spec: schema::metrics_partition_spec,
        build_sort_order: schema::metrics_sort_order,
        bloom_filter_columns: METRICS_BLOOM_COLUMNS,
        column_encodings: METRICS_COLUMN_ENCODINGS,
    },
    ProductionTable {
        name: OPERATIONS_TABLE,
        build_schema: schema::operations_schema,
        build_partition_spec: schema::operations_partition_spec,
        build_sort_order: schema::operations_sort_order,
        bloom_filter_columns: OPERATIONS_BLOOM_COLUMNS,
        column_encodings: OPERATIONS_COLUMN_ENCODINGS,
    },
    // Production writes `prices` with no writer settings of its own.
    ProductionTable {
        name: PRICES_TABLE,
        build_schema: schema::prices_schema,
        build_partition_spec: schema::prices_partition_spec,
        build_sort_order: schema::prices_sort_order,
        bloom_filter_columns: &[],
        column_encodings: &[],
    },
];

/// Create every production table of the IceGate namespace, which must exist.
pub async fn create_production_tables(catalog: &Arc<dyn Catalog>) -> Result<(), RowWriteError> {
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    for table in PRODUCTION_TABLES {
        let table_schema = (table.build_schema)()?;
        let creation = TableCreation::builder()
            .name(table.name.to_string())
            .partition_spec((table.build_partition_spec)(&table_schema)?)
            .sort_order((table.build_sort_order)(&table_schema)?)
            .schema(table_schema)
            .build();
        catalog.create_table(&namespace, creation).await?;
    }
    Ok(())
}

/// One cell of a [`Row`], named by the column it fills.
pub enum Cell<'a> {
    /// A string.
    Text(&'a str),
    /// A timestamp in microseconds since the epoch.
    Micros(i64),
    /// The bytes of a fixed-width binary column.
    Bytes(&'a [u8]),
    /// The entries of a `MAP<string, string>` column, in stored order.
    Map(&'a [(&'a str, &'a str)]),
    /// A 64-bit integer.
    Long(i64),
    /// A double.
    Double(f64),
    /// A boolean.
    Bool(bool),
    /// A decimal, as its unscaled value at the column's own scale (`1.5` at scale 10 is
    /// `15_000_000_000`).
    Decimal(i128),
}

/// One table row with explicit values: its tenant and the cells it sets. Every other column is
/// null when nullable, and its type's zero value otherwise (empty string, zero, zero bytes,
/// empty map or list).
pub struct Row<'a> {
    /// The tenant the row belongs to.
    pub tenant: &'a str,
    /// The cells, by column name.
    pub cells: Vec<(&'a str, Cell<'a>)>,
}

/// The `operations` label column, which has no `COL_*` constant. [`build_rows_batch`] panics on
/// a cell naming a column the table lacks, so a renamed column fails the test that names it; the
/// same holds for the three columns below.
pub const OPERATION_NAME: &str = "operation_name";
/// The `operations` double column `temperature`, which has no `COL_*` constant.
pub const TEMPERATURE: &str = "temperature";
/// The `operations` boolean column `stream`, which has no `COL_*` constant.
pub const STREAM: &str = "stream";
/// The `operations` long column `top_k`, which has no `COL_*` constant.
pub const TOP_K: &str = "top_k";

/// An `operations` row of `tenant` at `micros`, labelled `label` in [`OPERATION_NAME`], with
/// `cells`.
pub fn build_operation_row<'a>(
    tenant: &'a str,
    label: &'a str,
    micros: i64,
    mut cells: Vec<(&'a str, Cell<'a>)>,
) -> Row<'a> {
    cells.push((COL_TIMESTAMP, Cell::Micros(micros)));
    cells.push((OPERATION_NAME, Cell::Text(label)));
    Row { tenant, cells }
}

/// Build one batch of `rows` over `schema`. Panics when a cell names a column `schema` lacks or
/// carries a value of another type, so a renamed column fails the test that names it.
///
/// # Errors
///
/// Fails when Arrow refuses a column or the batch.
fn build_rows_batch(schema: &SchemaRef, rows: &[Row<'_>]) -> Result<RecordBatch, RowWriteError> {
    for row in rows {
        for (column, _) in &row.cells {
            assert!(
                schema.field_with_name(column).is_ok(),
                "row names `{column}`, which the table lacks"
            );
        }
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| build_rows_column(field, rows))
        .collect::<Result<Vec<ArrayRef>, RowWriteError>>()?;
    Ok(RecordBatch::try_new(Arc::clone(schema), columns)?)
}

/// The cell `row` sets for `column`, if any.
fn find_row_cell<'r>(row: &'r Row<'_>, column: &str) -> Option<&'r Cell<'r>> {
    row.cells.iter().find(|(name, _)| *name == column).map(|(_, cell)| cell)
}

/// Panic unless no row of `rows` sets a cell for the column `name`, whose type no [`Cell`]
/// fills.
fn assert_no_cells(rows: &[Row<'_>], name: &str) {
    assert!(
        rows.iter().all(|row| find_row_cell(row, name).is_none()),
        "`{name}` takes no cells"
    );
}

/// The column `field` over `rows`: each row's cell for it, or the null or zero value the
/// column's nullability calls for. Panics on a cell of the wrong kind.
fn build_rows_column(field: &Field, rows: &[Row<'_>]) -> Result<ArrayRef, RowWriteError> {
    let name = field.name().as_str();
    if name == COL_TENANT_ID {
        return Ok(Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.tenant),
        )));
    }
    let is_required = !field.is_nullable();
    let column: ArrayRef = match field.data_type() {
        DataType::Utf8 => Arc::new(StringArray::from_iter(rows.iter().map(|row| {
            match find_row_cell(row, name) {
                Some(Cell::Text(text)) => Some((*text).to_string()),
                None => is_required.then(String::new),
                Some(_) => panic!("`{name}` takes Text"),
            }
        }))),
        DataType::Int32 => {
            assert_no_cells(rows, name);
            Arc::new(Int32Array::from_iter(rows.iter().map(|_| is_required.then_some(0))))
        }
        DataType::Int64 => Arc::new(Int64Array::from_iter(rows.iter().map(|row| {
            match find_row_cell(row, name) {
                Some(Cell::Long(value)) => Some(*value),
                None => is_required.then_some(0),
                Some(_) => panic!("`{name}` takes Long"),
            }
        }))),
        DataType::Float64 => Arc::new(Float64Array::from_iter(rows.iter().map(|row| {
            match find_row_cell(row, name) {
                Some(Cell::Double(value)) => Some(*value),
                None => is_required.then_some(0.0),
                Some(_) => panic!("`{name}` takes Double"),
            }
        }))),
        DataType::Boolean => Arc::new(BooleanArray::from_iter(rows.iter().map(|row| {
            match find_row_cell(row, name) {
                Some(Cell::Bool(value)) => Some(*value),
                None => is_required.then_some(false),
                Some(_) => panic!("`{name}` takes Bool"),
            }
        }))),
        DataType::Decimal128(precision, scale) => Arc::new(
            Decimal128Array::from_iter(rows.iter().map(|row| match find_row_cell(row, name) {
                Some(Cell::Decimal(value)) => Some(*value),
                None => is_required.then_some(0),
                Some(_) => panic!("`{name}` takes Decimal"),
            }))
            .with_precision_and_scale(*precision, *scale)?,
        ),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => Arc::new(
            TimestampMicrosecondArray::from_iter(rows.iter().map(|row| match find_row_cell(row, name) {
                Some(Cell::Micros(value)) => Some(*value),
                None => is_required.then_some(0),
                Some(_) => panic!("`{name}` takes Micros"),
            }))
            .with_timezone_opt(timezone.clone()),
        ),
        DataType::FixedSizeBinary(width) => {
            let zero = vec![0_u8; usize::try_from(*width)?];
            let values = rows.iter().map(|row| match find_row_cell(row, name) {
                Some(Cell::Bytes(bytes)) => Some(bytes.to_vec()),
                None => is_required.then(|| zero.clone()),
                Some(_) => panic!("`{name}` takes Bytes"),
            });
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(values, *width)?)
        }
        DataType::Map(entries, _) => {
            let DataType::Struct(entry_fields) = entries.data_type() else {
                return Err(format!("the entries of `{name}` are not a struct").into());
            };
            let (Some(key_field), Some(value_field)) = (entry_fields.first(), entry_fields.get(1)) else {
                return Err(format!("the entries of `{name}` lack a key or a value").into());
            };
            // The entry, key and value fields are the table's own, so the Iceberg field-id
            // metadata `RecordBatch::try_new` compares matches.
            let names = MapFieldNames {
                entry: entries.name().clone(),
                key: key_field.name().clone(),
                value: value_field.name().clone(),
            };
            let mut builder = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new())
                .with_keys_field(Arc::clone(key_field))
                .with_values_field(Arc::clone(value_field));
            for row in rows {
                match find_row_cell(row, name) {
                    Some(Cell::Map(pairs)) => {
                        for (key, value) in *pairs {
                            builder.keys().append_value(key);
                            builder.values().append_value(value);
                        }
                        builder.append(true)?;
                    }
                    None => builder.append(is_required)?,
                    Some(_) => panic!("`{name}` takes Map"),
                }
            }
            Arc::new(builder.finish())
        }
        DataType::List(item) => {
            assert_no_cells(rows, name);
            let offsets = OffsetBuffer::new(vec![0_i32; rows.len() + 1].into());
            let nulls = (!is_required).then(|| vec![false; rows.len()].into());
            Arc::new(ListArray::new(
                Arc::clone(item),
                offsets,
                new_empty_array(item.data_type()),
                nulls,
            ))
        }
        other => {
            assert_no_cells(rows, name);
            new_null_array(other, rows.len())
        }
    };
    Ok(column)
}

/// The production writer settings of `table_name`: its bloom-filter columns and column
/// encodings, with sizes far above what a test writes. A name outside [`PRODUCTION_TABLES`]
/// (an exact-name table a test creates) is written with none.
fn select_write_config(table_name: &str) -> WriteConfig {
    let (bloom_filter_columns, column_encodings) = PRODUCTION_TABLES
        .iter()
        .find(|table| table.name == table_name)
        .map_or((&[][..], &[][..]), |table| {
            (table.bloom_filter_columns, table.column_encodings)
        });
    WriteConfig {
        row_group_size: 8192,
        data_page_size_limit_bytes: 1024 * 1024,
        max_file_size_bytes: 128 * 1024 * 1024,
        bloom_filter_columns,
        column_encodings,
    }
}

/// The Arrow schema of `table_name`'s current Iceberg schema, field ids included.
pub async fn read_arrow_schema(catalog: &Arc<dyn Catalog>, table_name: &str) -> Result<SchemaRef, RowWriteError> {
    let table = catalog.load_table(&icegate_table_ident(table_name)).await?;
    Ok(Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?))
}

/// Append `data_files` to `table` as one snapshot recording [`COMMITTED_WAL_OFFSET`].
async fn commit_written_files(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    data_files: Vec<DataFile>,
) -> Result<(), RowWriteError> {
    let append = Transaction::new(table)
        .fast_append()
        .add_data_files(data_files)
        .set_snapshot_properties(HashMap::from([(
            WAL_OFFSET_PROPERTY.to_string(),
            COMMITTED_WAL_OFFSET.to_string(),
        )]));
    let transaction = append.apply(Transaction::new(table))?;
    transaction.commit(&**catalog).await?;
    Ok(())
}

/// Write `batch`, built over [`read_arrow_schema`] of `table_name`, through the production
/// Parquet writer as one append. The writer splits the batch by the table's partition spec, so
/// rows of two tenants land in two files of one snapshot.
pub async fn commit_batch(
    catalog: &Arc<dyn Catalog>,
    table_name: &str,
    batch: RecordBatch,
) -> Result<(), RowWriteError> {
    let table = catalog.load_table(&icegate_table_ident(table_name)).await?;
    let batches: CommonRecordBatchStream = Box::pin(futures::stream::iter([Ok(batch)]));
    let written_files = write_record_batches_to_parquet(
        table.clone(),
        select_write_config(table_name),
        batches,
        &CancellationToken::new(),
    )
    .await?;
    commit_written_files(catalog, &table, written_files.data_files).await
}

/// Write `rows` into `table_name` as one append, like [`commit_batch`].
pub async fn commit_rows(catalog: &Arc<dyn Catalog>, table_name: &str, rows: &[Row<'_>]) -> Result<(), RowWriteError> {
    let batch = build_rows_batch(&read_arrow_schema(catalog, table_name).await?, rows)?;
    commit_batch(catalog, table_name, batch).await
}

/// Write `rows`, of any tenants, as the WAL segment `offset` of `table_name`'s topic: one
/// Parquet file holding every row in order. The WAL is read as an unbroken run of offsets, from
/// `0` for a table with no commit and from `COMMITTED_WAL_OFFSET + 1` otherwise.
pub async fn write_wal_segment(
    wal_store: &Arc<dyn ObjectStore>,
    catalog: &Arc<dyn Catalog>,
    table_name: &str,
    offset: u64,
    rows: &[Row<'_>],
) -> Result<(), RowWriteError> {
    let arrow_schema = read_arrow_schema(catalog, table_name).await?;
    let batch = build_rows_batch(&arrow_schema, rows)?;
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, arrow_schema, Some(WriterProperties::builder().build()))?;
    writer.write(&batch)?;
    writer.close()?;
    let segment = SegmentId {
        topic: table_name.to_string(),
        offset,
    };
    wal_store.put(&segment.to_relative_path(), PutPayload::from(buffer)).await?;
    Ok(())
}
