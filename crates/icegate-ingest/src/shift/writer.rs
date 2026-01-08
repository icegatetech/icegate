use std::{collections::HashMap, sync::Arc};

use arrow::{compute::SortOptions, record_batch::RecordBatch};
use iceberg::{
    arrow::RecordBatchPartitionSplitter,
    spec::{DataFile, DataFileFormat},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
            ParquetWriterBuilder,
        },
        partitioning::{fanout_writer::FanoutWriter, PartitioningWriter},
    },
    Catalog,
};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use uuid::Uuid;

use super::offset::OFFSET_SUMMARY_KEY;
use crate::error::{IngestError, Result};

/// Result of writing batches into parquet data files.
pub struct WrittenDataFiles {
    /// Data files written by the shift task.
    pub data_files: Vec<DataFile>,
    /// Total rows written across all data files.
    pub rows_written: usize,
}

/// Iceberg writer for shift operations.
pub struct Writer {
    table: Table,
    catalog: Arc<dyn Catalog>,
    row_group_size: usize,
    max_file_size_bytes: usize,
}

impl Writer {
    /// Creates a new shift writer for the provided table and catalog.
    pub fn new(table: Table, catalog: Arc<dyn Catalog>, row_group_size: usize, max_file_size_bytes: usize) -> Self {
        Self {
            table,
            catalog,
            row_group_size,
            max_file_size_bytes,
        }
    }

    /// Writes record batches into parquet data files without committing to Iceberg.
    /// This method:
    /// 1. Sorts the batches by the table's sort order
    /// 2. Splits data by partition using `RecordBatchPartitionSplitter`
    /// 3. Writes each partition's data using `FanoutWriter`
    pub async fn write_parquet_files(&self, batches: Vec<RecordBatch>) -> Result<WrittenDataFiles> {
        // TODO(high): We have problem. If the number of batches in the WAL file is large, then we will create a large number of small parquet files for Iceberg (for each batch).
        // We either need to process several WAL files in one task, or compact Iceberg parquet files separately, or pre-partition records into a WAL Writer.

        if batches.is_empty() {
            return Ok(WrittenDataFiles {
                data_files: Vec::new(),
                rows_written: 0,
            });
        }

        tracing::info!("Start writting parquet file. Batches: {}", batches.len());
        let queue_schema = batches[0].schema();
        let combined_batch = arrow::compute::concat_batches(&queue_schema, &batches)?;
        let sorted_batch = sort_by_table_order(&self.table, combined_batch)?;
        tracing::debug!("Sorted batch has {} rows", sorted_batch.num_rows());

        let location_generator = DefaultLocationGenerator::new(self.table.metadata().clone())
            .map_err(|e| IngestError::Shift(format!("failed to create location generator: {e}")))?;

        // Generate unique file prefix with UUID to avoid conflicts
        let write_id = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(write_id.to_string(), None, DataFileFormat::Parquet);

        let writer_props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(self.row_group_size / 10)
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_size(self.row_group_size)
            .build();

        let parquet_writer_builder =
            ParquetWriterBuilder::new(writer_props, self.table.metadata().current_schema().clone());

        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            self.max_file_size_bytes,
            self.table.file_io().clone(),
            location_generator,
            file_name_generator,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create FanoutWriter for partitioned writes
        let mut fanout_writer = FanoutWriter::new(data_file_writer_builder);

        // Create partition splitter to compute partition keys from source columns
        let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
            self.table.metadata().current_schema().clone(),
            self.table.metadata().default_partition_spec().clone(),
        )
        .map_err(|e| IngestError::Shift(format!("failed to create partition splitter: {e}")))?;

        // Split batch by partition and write each partition
        let partitioned_batches = splitter
            .split(&sorted_batch)
            .map_err(|e| IngestError::Shift(format!("failed to split batch by partition: {e}")))?;
        let partitioned_batches_len = partitioned_batches.len();

        for (partition_key, partition_batch) in partitioned_batches {
            tracing::debug!(
                "Writing partition with {} rows: {}",
                partition_batch.num_rows(),
                partition_key.to_path()
            );
            fanout_writer
                .write(partition_key, partition_batch)
                .await
                .map_err(|e| IngestError::Shift(format!("failed to write partition batch: {e}")))?;
        }

        // Close writer and get data files
        let data_files: Vec<DataFile> = fanout_writer
            .close()
            .await
            .map_err(|e| IngestError::Shift(format!("failed to close fanout writer: {e}")))?;

        tracing::info!("Complite write {} parquet files for {} partitions", data_files.len(), partitioned_batches_len);

        Ok(WrittenDataFiles {
            data_files,
            rows_written: sorted_batch.num_rows(),
        })
    }

    /// Commits parquet data files to Iceberg with offset tracking.
    pub async fn commit_data_files(&self, data_files: Vec<DataFile>, record_type: &str, last_offset: u64) -> Result<usize> {
        if data_files.is_empty() {
            return Ok(0);
        }

        let total_rows: u64 = data_files.iter().map(DataFile::record_count).sum();
        let total_rows =
            usize::try_from(total_rows).map_err(|_| IngestError::Shift("row count exceeds usize".to_string()))?;

        tracing::info!(
            "Start commiting Iceberg with {} parquet files with {} total rows for '{}'",
            data_files.len(),
            total_rows,
            record_type
        );

        let tx = Transaction::new(&self.table);

        // TODO(crit): merge with offset.rs

        // Store offset in snapshot summary properties
        let mut snapshot_props = HashMap::new();
        snapshot_props.insert(OFFSET_SUMMARY_KEY.to_string(), last_offset.to_string());
        tracing::debug!("Setting snapshot property: {}={}", OFFSET_SUMMARY_KEY, last_offset);

        tracing::debug!("Building fast append action with {} data files", data_files.len());
        let append_action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_props)
            .add_data_files(data_files);
        tracing::debug!("Fast append action built");

        tracing::debug!("Applying fast append action to transaction");
        let tx = append_action
            .apply(tx)
            .map_err(|e| IngestError::Shift(format!("failed to apply fast append: {e}")))?;
        tracing::debug!("Fast append applied to transaction");

        // Commit transaction
        tracing::info!("Committing transaction to catalog (this may take a moment)...");
        let commit_result = tx.commit(self.catalog.as_ref()).await;
        match &commit_result {
            Ok(_) => tracing::debug!("Transaction commit succeeded"),
            Err(e) => tracing::error!("Transaction commit failed: {e}"),
        }
        commit_result.map_err(|e| IngestError::Shift(format!("failed to commit transaction: {e}")))?;

        tracing::info!(
            "Committed {} rows to Iceberg table, offset updated to {}",
            total_rows,
            last_offset
        );

        Ok(total_rows)
    }
}

/// Sorts a record batch by the table's sort order.
fn sort_by_table_order(table: &Table, batch: RecordBatch) -> Result<RecordBatch> {
    let sort_order = table.metadata().default_sort_order();

    if sort_order.fields.is_empty() {
        // No sort order defined, return as-is
        return Ok(batch);
    }

    let schema = table.metadata().current_schema();
    let batch_schema = batch.schema();

    // Build sort columns from Iceberg sort order
    let mut sort_columns = Vec::with_capacity(sort_order.fields.len());

    for sort_field in &sort_order.fields {
        // Get field name from schema using source_id
        let field = schema.field_by_id(sort_field.source_id).ok_or_else(|| {
            IngestError::Shift(format!(
                "sort field with id {} not found in schema",
                sort_field.source_id
            ))
        })?;

        // Find column index in batch
        let col_idx = batch_schema
            .index_of(&field.name)
            .map_err(|e| IngestError::Shift(format!("sort column '{}' not in batch: {e}", field.name)))?;

        let descending = matches!(sort_field.direction, iceberg::spec::SortDirection::Descending);

        let nulls_first = matches!(sort_field.null_order, iceberg::spec::NullOrder::First);

        sort_columns.push(arrow::compute::SortColumn {
            values: batch.column(col_idx).clone(),
            options: Some(SortOptions {
                descending,
                nulls_first,
            }),
        });
    }

    // Compute sort indices
    let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)?;

    // Reorder all columns
    let sorted_columns: Vec<_> = batch
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let sorted_batch = RecordBatch::try_new(batch_schema, sorted_columns)?;

    Ok(sorted_batch)
}
