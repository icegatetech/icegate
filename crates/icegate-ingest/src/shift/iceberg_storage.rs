use std::{
    collections::HashMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::{compute::SortOptions, record_batch::RecordBatch};
use async_trait::async_trait;
use iceberg::spec::TableMetadata;
use iceberg::{
    Catalog, NamespaceIdent, TableIdent,
    arrow::RecordBatchPartitionSplitter,
    spec::{DataFile, DataFileFormat},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
        partitioning::{PartitioningWriter, fanout_writer::FanoutWriter},
    },
};
use icegate_common::{
    ICEGATE_NAMESPACE,
    retrier::{Retrier, RetrierConfig},
};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{config::ShiftConfig, parquet_meta_reader::data_files_from_parquet_paths};
use crate::error::{IngestError, Result};

/// Key for storing the queue offset in snapshot summary.
const OFFSET_SUMMARY_KEY: &str = "icegate.queue.offset";

/// Result of writing batches into parquet data files.
pub struct WrittenDataFiles {
    /// Data files written by the shift task.
    pub data_files: Vec<DataFile>,
    /// Total rows written across all data files.
    pub rows_written: usize,
}

/// Iceberg storage dependency surface for shift executors.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Fetch the last committed offset, if any.
    async fn get_last_offset(&self, cancel_token: &CancellationToken) -> Result<Option<u64>>;

    /// Write parquet files from record batches without committing.
    async fn write_record_batches(
        &self,
        batches: Vec<RecordBatch>,
        cancel_token: &CancellationToken,
    ) -> Result<WrittenDataFiles>;

    /// Build Iceberg data files from parquet paths.
    async fn get_data_files(&self, parquet_paths: &[String], cancel_token: &CancellationToken)
    -> Result<Vec<DataFile>>;

    /// Commit data files to Iceberg with the provided offset.
    async fn commit(
        &self,
        data_files: Vec<DataFile>,
        record_type: &str,
        last_offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<usize>;
}

/// Iceberg storage for shift operations.
pub struct IcebergStorage {
    loader: TableLoader,
    row_group_size: usize,
    max_file_size_mb: usize,
    retrier: Retrier,
}

impl IcebergStorage {
    /// Creates a new Iceberg storage for the provided table and catalog.
    pub fn new(catalog: Arc<dyn Catalog>, table: impl Into<String>, shift_config: &ShiftConfig) -> Self {
        let table_ident = TableIdent::new(NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), table.into());
        Self {
            loader: TableLoader::new(catalog, table_ident, shift_config.write.table_cache_ttl_secs),
            row_group_size: shift_config.write.row_group_size,
            max_file_size_mb: shift_config.write.max_file_size_mb,
            retrier: Retrier::new(RetrierConfig::default()),
        }
    }

    async fn load_table(&self, cancel_token: &CancellationToken) -> Result<Table> {
        self.retry(cancel_token, || self.loader.load_cached()).await
    }

    async fn load_table_fresh(&self, cancel_token: &CancellationToken) -> Result<Table> {
        self.retry(cancel_token, || self.loader.load_fresh()).await
    }

    /// Returns the last committed WAL offset from the Iceberg snapshot summary.
    pub async fn get_last_offset(&self, cancel_token: &CancellationToken) -> Result<Option<u64>> {
        let table = self.load_table_fresh(cancel_token).await?;
        Ok(table.metadata().current_snapshot().and_then(|snapshot| {
            snapshot
                .summary()
                .additional_properties
                .get(OFFSET_SUMMARY_KEY)
                .and_then(|v| v.parse::<u64>().ok())
        }))
    }

    /// Builds Iceberg data files from parquet file paths by reading parquet metadata.
    pub async fn get_data_files(
        &self,
        file_paths: &[String],
        cancel_token: &CancellationToken,
    ) -> Result<Vec<DataFile>> {
        let table = self.load_table_fresh(cancel_token).await?;
        self.retry(cancel_token, || data_files_from_parquet_paths(&table, file_paths))
            .await
    }

    /// Writes record batches into parquet data files without committing to Iceberg.
    /// This method:
    /// 1. Sorts the batches by the table's sort order
    /// 2. Splits data by partition using `RecordBatchPartitionSplitter`
    /// 3. Writes each partition's data using `FanoutWriter`
    pub async fn write_record_batches(
        &self,
        batches: Vec<RecordBatch>,
        cancel_token: &CancellationToken,
    ) -> Result<WrittenDataFiles> {
        let batches = Arc::new(batches);
        self.retry(cancel_token, || {
            let batches = Arc::clone(&batches);
            async move { self.write_parquet_files_once(batches.as_ref().clone(), cancel_token).await }
        })
        .await
    }

    #[tracing::instrument(skip(self, batches, cancel_token))]
    async fn write_parquet_files_once(
        &self,
        batches: Vec<RecordBatch>,
        cancel_token: &CancellationToken,
    ) -> Result<WrittenDataFiles> {
        if batches.is_empty() {
            return Ok(WrittenDataFiles {
                data_files: Vec::new(),
                rows_written: 0,
            });
        }

        tracing::info!("Start writing parquet file. Batches: {}", batches.len());
        let queue_schema = batches[0].schema();
        let combined_batch = arrow::compute::concat_batches(&queue_schema, &batches)?;
        let table = self.load_table(cancel_token).await?;
        let table_metadata = table.metadata().clone();
        let table_file_io = table.file_io().clone();
        let metadata_for_sort = table_metadata.clone();
        let span = tracing::Span::current();
        let sorted_batch = tokio::task::spawn_blocking(move || {
            span.in_scope(|| sort_by_table_order(&metadata_for_sort, combined_batch))
        })
        .await??;
        tracing::debug!("Sorted batch has {} rows", sorted_batch.num_rows());

        let location_generator = DefaultLocationGenerator::new(table_metadata.clone())
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

        let parquet_writer_builder = ParquetWriterBuilder::new(writer_props, table_metadata.current_schema().clone());

        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            self.max_file_size_mb * 1024 * 1024,
            table_file_io,
            location_generator,
            file_name_generator,
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

        // Create FanoutWriter for partitioned writes
        let mut fanout_writer = FanoutWriter::new(data_file_writer_builder);

        // Create partition splitter to compute partition keys from source columns
        let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
            table_metadata.current_schema().clone(),
            table_metadata.default_partition_spec().clone(),
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

        tracing::info!(
            "Complete write {} parquet files for {} partitions",
            data_files.len(),
            partitioned_batches_len
        );

        Ok(WrittenDataFiles {
            data_files,
            rows_written: sorted_batch.num_rows(),
        })
    }

    /// Commits parquet data files to Iceberg with offset tracking.
    pub async fn commit(
        &self,
        data_files: Vec<DataFile>,
        record_type: &str,
        last_offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<usize> {
        let data_files = Arc::new(data_files);
        let record_type = record_type.to_string();
        self.retry(cancel_token, || {
            let data_files = Arc::clone(&data_files);
            let record_type = record_type.clone();
            async move {
                self.commit_data_files_once(data_files.as_ref().clone(), &record_type, last_offset, cancel_token)
                    .await
            }
        })
        .await
    }

    async fn commit_data_files_once(
        &self,
        data_files: Vec<DataFile>,
        record_type: &str,
        last_offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<usize> {
        if data_files.is_empty() {
            return Ok(0);
        }

        let total_rows: u64 = data_files.iter().map(DataFile::record_count).sum();
        let total_rows =
            usize::try_from(total_rows).map_err(|_| IngestError::Shift("row count exceeds usize".to_string()))?;

        tracing::info!(
            "Start committing Iceberg with {} parquet files with {} total rows for '{}'",
            data_files.len(),
            total_rows,
            record_type
        );

        let table = self.load_table_fresh(cancel_token).await?;
        let tx = Transaction::new(&table);

        // Store offset in snapshot summary properties
        let mut snapshot_props = HashMap::new();
        snapshot_props.insert(OFFSET_SUMMARY_KEY.to_string(), last_offset.to_string());
        tracing::debug!("Setting snapshot property: {}={}", OFFSET_SUMMARY_KEY, last_offset);

        let append_action = tx
            .fast_append()
            .set_snapshot_properties(snapshot_props)
            .add_data_files(data_files);
        let tx = append_action
            .apply(tx)
            .map_err(|e| IngestError::Shift(format!("failed to apply fast append: {e}")))?;
        tracing::debug!("Fast append applied to transaction");

        // Commit transaction
        let commit_result = tx
            .commit(self.loader.catalog())
            .await
            .map_err(|e| IngestError::Shift(format!("failed to commit transaction: {e}")));
        match &commit_result {
            Ok(_) => tracing::debug!("Transaction commit succeeded"),
            Err(e) => tracing::error!("Transaction commit failed: {e}"),
        }
        commit_result?;

        tracing::info!(
            "Committed {} rows to Iceberg table, offset updated to {}",
            total_rows,
            last_offset
        );

        Ok(total_rows)
    }

    async fn retry<F, Fut, T>(&self, cancel_token: &CancellationToken, mut op: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let result = self
            .retrier
            .retry::<_, _, Result<T>, IngestError>(
                || {
                    let fut = op();
                    async move {
                        match fut.await {
                            Ok(value) => Ok((false, Ok(value))),
                            Err(err) => {
                                let retryable = err.is_retryable();
                                if retryable {
                                    tracing::debug!(?err, retryable, "Iceberg storage operation failed, retrying");
                                } else {
                                    tracing::debug!(?err, retryable, "Iceberg storage operation failed, not retryable");
                                }
                                Ok((retryable, Err(err)))
                            }
                        }
                    }
                },
                cancel_token,
            )
            .await?;

        match result {
            Ok(value) => Ok(value),
            Err(err) => Err(err),
        }
    }
}

#[async_trait]
impl Storage for IcebergStorage {
    async fn get_last_offset(&self, cancel_token: &CancellationToken) -> Result<Option<u64>> {
        self.get_last_offset(cancel_token).await
    }

    async fn write_record_batches(
        &self,
        batches: Vec<RecordBatch>,
        cancel_token: &CancellationToken,
    ) -> Result<WrittenDataFiles> {
        self.write_record_batches(batches, cancel_token).await
    }

    async fn get_data_files(
        &self,
        parquet_paths: &[String],
        cancel_token: &CancellationToken,
    ) -> Result<Vec<DataFile>> {
        self.get_data_files(parquet_paths, cancel_token).await
    }

    async fn commit(
        &self,
        data_files: Vec<DataFile>,
        record_type: &str,
        last_offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<usize> {
        self.commit(data_files, record_type, last_offset, cancel_token).await
    }
}

/// Sorts a record batch by the table's sort order.
fn sort_by_table_order(table_metadata: &TableMetadata, batch: RecordBatch) -> Result<RecordBatch> {
    let sort_order = table_metadata.default_sort_order();

    if sort_order.fields.is_empty() {
        // No sort order defined, return as-is
        return Ok(batch);
    }

    let schema = table_metadata.current_schema();
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

// Table loader with caching
struct TableLoader {
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    cache: RwLock<Option<CachedTable>>,
    ttl: Duration,
}

struct CachedTable {
    table: Table,
    loaded_at: Instant,
}

impl TableLoader {
    fn new(catalog: Arc<dyn Catalog>, table_ident: TableIdent, ttl_secs: u64) -> Self {
        Self {
            catalog,
            table_ident,
            cache: RwLock::new(None),
            ttl: Duration::from_secs(ttl_secs),
        }
    }

    fn catalog(&self) -> &dyn Catalog {
        self.catalog.as_ref()
    }

    async fn load_cached(&self) -> Result<Table> {
        if let Some(cached) = self.cache.read().await.as_ref() {
            if cached.loaded_at.elapsed() <= self.ttl {
                return Ok(cached.table.clone());
            }
        }

        self.load_fresh().await
    }

    async fn load_fresh(&self) -> Result<Table> {
        let table = self
            .catalog
            .load_table(&self.table_ident)
            .await
            .map_err(|e| IngestError::Shift(format!("failed to load table '{}': {e}", self.table_ident.name())))?;

        *self.cache.write().await = Some(CachedTable {
            table: table.clone(),
            loaded_at: Instant::now(),
        });

        Ok(table)
    }
}
