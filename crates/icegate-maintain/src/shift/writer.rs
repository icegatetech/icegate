//! Iceberg writer for shift operations.
//!
//! Handles writing data to Iceberg tables with:
//! - Sorting by table's sort order
//! - Partitioning using `FanoutWriter` with automatic partition key computation
//! - File rollover at configured size
//! - Atomic commits with offset tracking

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
use parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties},
};
use uuid::Uuid;

use super::offset::OFFSET_SUMMARY_KEY;
use crate::error::{MaintainError, Result};

/// Writes record batches to an Iceberg table and commits with offset tracking.
///
/// This function:
/// 1. Sorts the batches by the table's sort order
/// 2. Splits data by partition using `RecordBatchPartitionSplitter`
/// 3. Writes each partition's data using `FanoutWriter`
/// 4. Commits all data files in a single transaction
/// 5. Updates the offset property atomically with the data commit
#[allow(clippy::too_many_lines)]
pub async fn write_and_commit(
    table: &Table,
    catalog: &Arc<dyn Catalog>,
    batches: Vec<RecordBatch>,
    topic: &str,
    new_offset: u64,
    row_group_size: usize,
    max_file_size_bytes: usize,
) -> Result<usize> {
    if batches.is_empty() {
        return Ok(0);
    }

    // Concatenate all batches into one for sorting
    tracing::debug!("Concatenating {} batches", batches.len());
    let queue_schema = batches[0].schema();
    let combined_batch = arrow::compute::concat_batches(&queue_schema, &batches)?;
    tracing::debug!("Combined batch has {} rows", combined_batch.num_rows());

    // Sort by table's sort order
    tracing::debug!("Sorting batch by table order");
    let sorted_batch = sort_by_table_order(table, combined_batch)?;
    tracing::debug!("Batch sorted successfully");

    let total_rows = sorted_batch.num_rows();

    // Build writer chain
    tracing::debug!("Building writer chain with FanoutWriter");
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| MaintainError::Shift(format!("failed to create location generator: {e}")))?;

    // Generate unique file prefix with UUID to avoid conflicts
    let write_id = Uuid::now_v7();
    let file_name_generator = DefaultFileNameGenerator::new(write_id.to_string(), None, DataFileFormat::Parquet);

    let writer_props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(row_group_size / 10)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(row_group_size)
        .build();

    let parquet_writer_builder = ParquetWriterBuilder::new(writer_props, table.metadata().current_schema().clone());

    let rolling_writer_builder = RollingFileWriterBuilder::new(
        parquet_writer_builder,
        max_file_size_bytes,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    // Create FanoutWriter for partitioned writes
    let mut fanout_writer = FanoutWriter::new(data_file_writer_builder);

    // Create partition splitter to compute partition keys from source columns
    tracing::debug!("Creating partition splitter with computed values");
    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().clone(),
    )
    .map_err(|e| MaintainError::Shift(format!("failed to create partition splitter: {e}")))?;

    // Split batch by partition and write each partition
    tracing::debug!("Splitting batch by partition values");
    let partitioned_batches = splitter
        .split(&sorted_batch)
        .map_err(|e| MaintainError::Shift(format!("failed to split batch by partition: {e}")))?;

    tracing::debug!("Writing {} partitions", partitioned_batches.len());
    for (partition_key, partition_batch) in partitioned_batches {
        tracing::debug!(
            "Writing partition with {} rows: {}",
            partition_batch.num_rows(),
            partition_key.to_path()
        );
        fanout_writer
            .write(partition_key, partition_batch)
            .await
            .map_err(|e| MaintainError::Shift(format!("failed to write partition batch: {e}")))?;
    }

    // Close writer and get data files
    tracing::debug!("Closing FanoutWriter");
    let data_files: Vec<DataFile> = fanout_writer
        .close()
        .await
        .map_err(|e| MaintainError::Shift(format!("failed to close fanout writer: {e}")))?;

    tracing::debug!("FanoutWriter closed, got {} data files", data_files.len());

    if data_files.is_empty() {
        return Ok(0);
    }

    tracing::info!(
        "Writing {} data files with {} total rows for topic '{}'",
        data_files.len(),
        total_rows,
        topic
    );

    // Create transaction with fast append and offset in snapshot summary
    tracing::debug!("Creating transaction with fast append");
    let tx = Transaction::new(table);

    // Store offset in snapshot summary properties
    let mut snapshot_props = HashMap::new();
    snapshot_props.insert(OFFSET_SUMMARY_KEY.to_string(), new_offset.to_string());
    tracing::debug!("Setting snapshot property: {}={}", OFFSET_SUMMARY_KEY, new_offset);

    tracing::debug!("Building fast append action with {} data files", data_files.len());
    let append_action = tx.fast_append().set_snapshot_properties(snapshot_props).add_data_files(data_files);
    tracing::debug!("Fast append action built");

    tracing::debug!("Applying fast append action to transaction");
    let tx = append_action
        .apply(tx)
        .map_err(|e| MaintainError::Shift(format!("failed to apply fast append: {e}")))?;
    tracing::debug!("Fast append applied to transaction");

    // Commit transaction
    tracing::info!("Committing transaction to catalog (this may take a moment)...");
    let commit_result = tx.commit(catalog.as_ref()).await;
    match &commit_result {
        Ok(_) => tracing::debug!("Transaction commit succeeded"),
        Err(e) => tracing::error!("Transaction commit failed: {e}"),
    }
    commit_result.map_err(|e| MaintainError::Shift(format!("failed to commit transaction: {e}")))?;

    tracing::info!(
        "Committed {} rows to Iceberg table, offset updated to {}",
        total_rows,
        new_offset
    );

    Ok(total_rows)
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
            MaintainError::Shift(format!(
                "sort field with id {} not found in schema",
                sort_field.source_id
            ))
        })?;

        // Find column index in batch
        let col_idx = batch_schema
            .index_of(&field.name)
            .map_err(|e| MaintainError::Shift(format!("sort column '{}' not in batch: {e}", field.name)))?;

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
