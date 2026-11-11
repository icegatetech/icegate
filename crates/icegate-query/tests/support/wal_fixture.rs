//! Seeding of a table that has a WAL topic: a memory catalog holding the table, WAL
//! segments in an object store, and Shifter-style commits into Iceberg.
//!
//! Shared by the test binaries through `#[path]`, so each of them compiles its own copy.
#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, MapBuilder, MapFieldNames, StringBuilder};
use datafusion::arrow::datatypes::{DataType, SchemaRef as ArrowSchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use iceberg::spec::{DataFileFormat, Schema};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use icegate_common::{
    CatalogBackend, CatalogConfig, ICEGATE_NAMESPACE, IoHandle, WAL_OFFSET_PROPERTY, catalog::CatalogBuilder,
};
use icegate_queue::SegmentId;
use object_store::memory::InMemory;
use object_store::{ObjectStoreExt, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use tokio_util::sync::CancellationToken;

/// A memory catalog holding one empty, unpartitioned table named `table_name` with
/// `table_schema`, in the IceGate namespace.
///
/// The temp warehouse is returned so the caller keeps it alive for the test.
pub async fn build_catalog(table_name: &str, table_schema: Schema) -> (Arc<dyn Catalog>, tempfile::TempDir) {
    let warehouse = tempfile::tempdir().expect("tempdir");
    let catalog_config = CatalogConfig {
        backend: CatalogBackend::Memory,
        warehouse: warehouse.path().to_str().expect("warehouse path").to_string(),
        properties: HashMap::new(),
        cache: None,
    };
    let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("build catalog");

    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let creation = TableCreation::builder()
        .name(table_name.to_string())
        .schema(table_schema)
        .build();
    catalog.create_table(&namespace, creation).await.expect("create table");

    (catalog, warehouse)
}

/// The Arrow form of `table`'s current schema, which WAL segments and committed files share.
pub fn derive_arrow_schema(table: &Table) -> ArrowSchemaRef {
    Arc::new(iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema()).expect("arrow schema"))
}

/// An all-empty attribute map for `column`, one entry per row.
///
/// The key and value fields are cloned from the target column: each OTLP level
/// carries its own Iceberg field ids in that metadata, so an array built from
/// another level's fields is a genuine `DataType` mismatch and
/// `RecordBatch::try_new` rejects it.
pub fn build_empty_attribute_map(arrow_schema: &ArrowSchemaRef, column: &str, row_count: usize) -> ArrayRef {
    let field = arrow_schema.field_with_name(column).expect("attribute column");
    let DataType::Map(entries_field, _) = field.data_type() else {
        panic!("{column} must be a map column");
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        panic!("map entries must be a struct");
    };
    let mut builder = MapBuilder::new(
        Some(MapFieldNames {
            entry: entries_field.name().clone(),
            key: entry_fields[0].name().clone(),
            value: entry_fields[1].name().clone(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_keys_field(Arc::clone(&entry_fields[0]))
    .with_values_field(Arc::clone(&entry_fields[1]));
    for _ in 0..row_count {
        builder.append(true).expect("append empty attribute map");
    }
    Arc::new(builder.finish())
}

/// Write `batch` as the WAL segment at `offset` of `topic`.
pub async fn write_wal_segment(store: &InMemory, topic: &str, offset: u64, batch: &RecordBatch) {
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(WriterProperties::builder().build()))
        .expect("segment writer");
    writer.write(batch).expect("write segment batch");
    writer.close().expect("close segment");

    let segment = SegmentId {
        topic: topic.to_string(),
        offset,
    };
    store
        .put(&segment.to_relative_path(), PutPayload::from(buffer))
        .await
        .expect("put segment");
}

/// Commit `batch` into `table` and record `committed_offset` on the snapshot, the way
/// the Shifter does.
pub async fn commit_shifted_rows(catalog: &Arc<dyn Catalog>, table: &Table, batch: RecordBatch, committed_offset: u64) {
    let parquet_writer_builder = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );
    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata()).expect("location generator"),
        DefaultFileNameGenerator::new(format!("shift-{committed_offset}"), None, DataFileFormat::Parquet),
    );
    let mut data_file_writer = DataFileWriterBuilder::new(rolling_writer_builder)
        .build(None)
        .await
        .expect("data file writer");
    data_file_writer.write(batch).await.expect("write data file");
    let data_files = data_file_writer.close().await.expect("close data file writer");

    let transaction = Transaction::new(table);
    let transaction = transaction
        .fast_append()
        .add_data_files(data_files)
        .set_snapshot_properties(HashMap::from([(
            WAL_OFFSET_PROPERTY.to_string(),
            committed_offset.to_string(),
        )]))
        .apply(transaction)
        .expect("apply fast append");
    transaction.commit(catalog.as_ref()).await.expect("commit snapshot");
}
