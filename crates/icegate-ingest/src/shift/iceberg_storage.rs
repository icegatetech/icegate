use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{Stream, TryStreamExt};
use iceberg::{
    Catalog, NamespaceIdent, TableIdent,
    arrow::RecordBatchPartitionSplitter,
    io::FileIO,
    spec::{DataFile, DataFileFormat},
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
        partitioning::{PartitioningWriter, fanout_writer::FanoutWriter},
    },
};
use icegate_common::{
    ICEGATE_NAMESPACE, WAL_OFFSET_PROPERTY,
    retrier::{Retrier, RetrierConfig},
};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::ColumnPath;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, warn};
use uuid::Uuid;

use super::{config::ShiftConfig, parquet_meta_reader::data_files_from_parquet_paths};
use crate::error::{IngestError, Result};

/// Wrapper around the default Iceberg location generator that records every
/// generated output path.
///
/// Parquet files may already be created in object storage before the write
/// pipeline finishes successfully. If the write later fails, the shift task
/// must remove those orphaned files. This wrapper keeps the generated paths so
/// cleanup can delete everything that was allocated during the failed write.
#[derive(Clone, Debug)]
struct TrackingLocationGenerator {
    inner: DefaultLocationGenerator,
    generated_paths: Arc<std::sync::Mutex<Vec<String>>>,
}

impl TrackingLocationGenerator {
    const fn new(inner: DefaultLocationGenerator, generated_paths: Arc<std::sync::Mutex<Vec<String>>>) -> Self {
        Self { inner, generated_paths }
    }
}

impl LocationGenerator for TrackingLocationGenerator {
    fn generate_location(&self, partition_key: Option<&iceberg::spec::PartitionKey>, file_name: &str) -> String {
        let path = self.inner.generate_location(partition_key, file_name);
        let mut generated_paths = match self.generated_paths.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        generated_paths.push(path.clone());
        path
    }
}

/// Result of writing batches into parquet data files.
pub struct WrittenDataFiles {
    /// Data files written by the shift task.
    pub data_files: Vec<DataFile>,
    /// Total rows written across all data files.
    pub rows_written: usize,
}

/// Stream of merge-ordered record batches written to Iceberg.
pub type BoxRecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Iceberg storage dependency surface for shift executors.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Fetch the last committed offset, if any.
    async fn get_last_offset(&self, cancel_token: &CancellationToken) -> Result<Option<u64>>;

    /// Write parquet files from record batches without committing.
    async fn write_record_batches(
        &self,
        batches: BoxRecordBatchStream,
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

/// Failover multiplier on top of the planner's upper bound. The writer flips
/// to a new file only when actual encoded parquet bytes overshoot
/// `upper_bound_input_bytes_per_task * factor` — i.e., parquet overhead
/// (footers, statistics, padding) plus a safety margin. In normal operation
/// the planner already shapes one chunk per shift task, so rollover should
/// not fire.
const WRITER_FILE_SIZE_FAILOVER_FACTOR: u64 = 2;

/// Compute the writer rollover budget from the planner's per-task upper bound.
///
/// The writer must only roll over as a failover (planner-shaped chunks already
/// target one file per task), so the budget is `upper_bound * 2` rather than
/// the planner's upper bound itself.  Encapsulated as a `const fn` so the
/// failover policy is verifiable in isolation.
pub(super) const fn writer_max_parquet_bytes(upper_bound_input_bytes: u64) -> u64 {
    upper_bound_input_bytes.saturating_mul(WRITER_FILE_SIZE_FAILOVER_FACTOR)
}

/// Iceberg storage for shift operations.
pub struct IcebergStorage {
    loader: TableLoader,
    row_group_size: usize,
    max_file_size_bytes: u64,
    /// Column names that should get a Parquet bloom filter when written.
    ///
    /// The list is owned by the caller (typically a per-table
    /// `ShiftJobSpec`); this struct stays table-agnostic and just
    /// forwards the list to the writer-properties builder.
    bloom_filter_columns: &'static [&'static str],
    retrier: Retrier,
}

impl IcebergStorage {
    /// Creates a new Iceberg storage for the provided table and catalog.
    ///
    /// `bloom_filter_columns` is the per-table list of columns that
    /// should have a Parquet bloom filter; pass `&[]` if none.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        table: impl Into<String>,
        shift_config: &ShiftConfig,
        bloom_filter_columns: &'static [&'static str],
    ) -> Self {
        let table_ident = TableIdent::new(NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), table.into());
        let max_file_size_bytes = writer_max_parquet_bytes(shift_config.read.upper_bound_input_bytes_per_task);
        Self {
            loader: TableLoader::new(catalog, table_ident, shift_config.write.table_cache_ttl_secs),
            row_group_size: shift_config.write.row_group_size,
            max_file_size_bytes,
            bloom_filter_columns,
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
                .get(WAL_OFFSET_PROPERTY)
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
    /// 1. Reads merge-ordered batches from the input stream
    /// 2. Splits every batch by partition using `RecordBatchPartitionSplitter`
    /// 3. Writes partition batches immediately via `FanoutWriter`
    pub async fn write_record_batches(
        &self,
        batches: BoxRecordBatchStream,
        cancel_token: &CancellationToken,
    ) -> Result<WrittenDataFiles> {
        // Move the entire Parquet write pipeline off the tokio async-worker
        // threads.  `write_parquet_files_once` performs CPU-heavy Arrow →
        // Parquet encoding (column encoding, ZSTD compression, statistics)
        // synchronously inside `AsyncArrowWriter::write`, which starves the
        // ingest hot-path (GRPC handler / WAL flush) when both share the
        // same tokio runtime.
        //
        // `spawn_blocking` runs on a dedicated thread-pool.  Inside it we
        // re-enter the runtime via `Handle::block_on` so that async S3 I/O
        // still resolves normally.
        let handle = tokio::runtime::Handle::current();
        let row_group_size = self.row_group_size;
        let max_file_size_bytes = self.max_file_size_bytes;
        let bloom_filter_columns = self.bloom_filter_columns;
        let cancel = cancel_token.clone();
        let table = self.load_table(cancel_token).await?;

        // Capture the current tracing span so that `write_parquet_files_once`
        // remains a child of the caller (e.g. `shift_run`) even though it
        // executes on a different thread.
        let span = tracing::Span::current();

        tokio::task::spawn_blocking(move || {
            let _guard = span.enter();
            handle.block_on(Self::write_parquet_files_once(
                table,
                row_group_size,
                max_file_size_bytes,
                bloom_filter_columns,
                batches,
                &cancel,
            ))
        })
        .await
        .map_err(|e| IngestError::Shift(format!("shift write task panicked: {e}")))?
    }

    #[tracing::instrument(skip(table, batches, cancel_token))]
    async fn write_parquet_files_once(
        table: Table,
        row_group_size: usize,
        max_file_size_bytes: u64,
        bloom_filter_columns: &'static [&'static str],
        mut batches: BoxRecordBatchStream,
        cancel_token: &CancellationToken,
    ) -> Result<WrittenDataFiles> {
        let table_metadata = table.metadata().clone();
        let table_file_io = table.file_io().clone();

        let generated_paths = Arc::new(std::sync::Mutex::new(Vec::new()));
        let location_generator = TrackingLocationGenerator::new(
            DefaultLocationGenerator::new(table_metadata.clone()).map_err(IngestError::Iceberg)?,
            Arc::clone(&generated_paths),
        );

        // Generate unique file prefix with UUID to avoid conflicts
        let write_id = Uuid::now_v7();
        let file_name_generator = DefaultFileNameGenerator::new(write_id.to_string(), None, DataFileFormat::Parquet);

        // TODO(med): Issue #101. Add semaphore/CPU budget. The limit should protect ingest from ZSTD spikes during multiple shift tasks.
        let writer_props = build_writer_properties(row_group_size, bloom_filter_columns);

        let parquet_writer_builder = ParquetWriterBuilder::new(writer_props, table_metadata.current_schema().clone());

        // Convert to `usize` for the iceberg writer API. On 64-bit targets `u64`
        // never overflows `usize`; saturate on 32-bit targets.
        let target_file_size = usize::try_from(max_file_size_bytes).unwrap_or(usize::MAX);
        let rolling_writer_builder = RollingFileWriterBuilder::new(
            parquet_writer_builder,
            target_file_size,
            table_file_io.clone(),
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
        .map_err(IngestError::Iceberg)?;

        let write_result = async {
            let mut rows_written = 0usize;
            let mut partitioned_batches_total = 0usize;
            while let Some(batch) = batches.try_next().await? {
                if cancel_token.is_cancelled() {
                    return Err(IngestError::Shift(
                        "shift task cancelled during parquet write".to_string(),
                    ));
                }
                rows_written = rows_written
                    .checked_add(batch.num_rows())
                    .ok_or_else(|| IngestError::Shift("rows written overflow".to_string()))?;
                let partitioned_batches = splitter
                    .split(&batch)
                    .map_err(|e| IngestError::Shift(format!("failed to split batch by partition: {e}")))?;
                partitioned_batches_total = partitioned_batches_total
                    .checked_add(partitioned_batches.len())
                    .ok_or_else(|| IngestError::Shift("partitioned batch count overflow".to_string()))?;

                for (partition_key, partition_batch) in partitioned_batches {
                    let partition_path = partition_key.to_path();
                    let span = tracing::info_span!(
                        "iceberg_partition_write",
                        partition_key = %partition_path,
                        rows = partition_batch.num_rows()
                    );
                    fanout_writer
                        .write(partition_key, partition_batch)
                        .instrument(span)
                        .await
                        .map_err(IngestError::Iceberg)?;
                }
            }

            // Close writer and get data files
            let span = tracing::info_span!("iceberg_write_close");
            let data_files: Vec<DataFile> =
                fanout_writer.close().instrument(span).await.map_err(IngestError::Iceberg)?;

            tracing::info!(
                "Complete write {} parquet files for {} partitions",
                data_files.len(),
                partitioned_batches_total
            );

            Ok(WrittenDataFiles {
                data_files,
                rows_written,
            })
        }
        .await;

        if write_result.is_err() {
            cleanup_generated_data_files(&table_file_io, &generated_paths).await;
        }

        write_result
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
        snapshot_props.insert(WAL_OFFSET_PROPERTY.to_string(), last_offset.to_string());
        tracing::debug!("Setting snapshot property: {}={}", WAL_OFFSET_PROPERTY, last_offset);

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

/// Builds the Parquet [`WriterProperties`] used for an Iceberg shift
/// write.
///
/// Per-column bloom filters are enabled for every column listed in
/// `bloom_filter_columns`. The NDV hint is set to `row_group_size`
/// because that is the upper bound on distinct values within a single
/// row group (every row contributes at most one new value), which
/// right-sizes the bloom filter without over-allocating.
///
/// This function is intentionally table-agnostic — the caller decides
/// which columns make sense to filter for the table being written.
fn build_writer_properties(row_group_size: usize, bloom_filter_columns: &[&str]) -> WriterProperties {
    let mut builder = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit((row_group_size / 10).max(1))
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .set_max_row_group_size(row_group_size);

    let ndv = u64::try_from(row_group_size).unwrap_or(u64::MAX);
    for col in bloom_filter_columns {
        let path = ColumnPath::from(*col);
        builder = builder
            .set_column_bloom_filter_enabled(path.clone(), true)
            .set_column_bloom_filter_ndv(path, ndv);
    }

    builder.build()
}

async fn cleanup_generated_data_files(file_io: &FileIO, generated_paths: &Arc<std::sync::Mutex<Vec<String>>>) {
    let generated = match generated_paths.lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    let mut unique_paths = HashSet::with_capacity(generated.len());

    for path in generated {
        if !unique_paths.insert(path.clone()) {
            continue;
        }
        match file_io.exists(&path).await {
            Ok(true) => {
                if let Err(err) = file_io.delete(&path).await {
                    warn!(path = %path, error = %err, "failed to cleanup uncommitted parquet file");
                }
            }
            Ok(false) => {}
            Err(err) => {
                warn!(path = %path, error = %err, "failed to check uncommitted parquet file existence");
            }
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
        batches: BoxRecordBatchStream,
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::StringArray,
        datatypes::{DataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    };
    use bytes::Bytes;
    use iceberg::io::FileIO;
    use parquet::{
        arrow::ArrowWriter,
        file::{
            metadata::ColumnChunkMetaData,
            reader::{FileReader, SerializedFileReader},
        },
    };

    use super::{build_writer_properties, cleanup_generated_data_files, writer_max_parquet_bytes};

    /// Failover policy guard: writer rollover budget must be exactly
    /// `upper_bound_input_bytes_per_task * 2`.  If this changes, the planner's
    /// "one chunk == one parquet file" invariant has to be re-verified — the
    /// writer would start splitting normal chunks instead of acting as a
    /// failover.
    #[test]
    fn writer_max_parquet_bytes_doubles_planner_upper_bound() {
        assert_eq!(writer_max_parquet_bytes(64 * 1024 * 1024), 128 * 1024 * 1024);
        assert_eq!(writer_max_parquet_bytes(128 * 1024 * 1024), 256 * 1024 * 1024);
    }

    /// Saturating multiplication must not panic on absurdly large inputs.
    #[test]
    fn writer_max_parquet_bytes_saturates_on_overflow() {
        assert_eq!(writer_max_parquet_bytes(u64::MAX), u64::MAX);
    }

    #[tokio::test]
    async fn cleanup_generated_data_files_deletes_existing_paths() {
        let file_io = FileIO::new_with_memory();
        let path_1 = "memory://cleanup/test-1.parquet";
        let path_2 = "memory://cleanup/test-2.parquet";

        file_io
            .new_output(path_1)
            .expect("output path_1")
            .write("one".into())
            .await
            .expect("write path_1");
        file_io
            .new_output(path_2)
            .expect("output path_2")
            .write("two".into())
            .await
            .expect("write path_2");

        let generated_paths = Arc::new(std::sync::Mutex::new(vec![
            path_1.to_string(),
            path_1.to_string(),
            path_2.to_string(),
            "memory://cleanup/missing.parquet".to_string(),
        ]));

        cleanup_generated_data_files(&file_io, &generated_paths).await;

        assert!(!file_io.exists(path_1).await.expect("exists path_1"));
        assert!(!file_io.exists(path_2).await.expect("exists path_2"));
    }

    /// Writes a tiny parquet file using [`build_writer_properties`]
    /// configured with `bloom_columns`, then returns — for each column
    /// in `schema_columns` — whether the file's first row group
    /// carries a bloom filter for that column.
    fn bloom_filter_presence(schema_columns: &[&str], bloom_columns: &[&str]) -> Vec<bool> {
        let schema = Arc::new(ArrowSchema::new(
            schema_columns
                .iter()
                .map(|c| Field::new(*c, DataType::Utf8, false))
                .collect::<Vec<_>>(),
        ));
        let arrays = schema_columns
            .iter()
            .map(|_| Arc::new(StringArray::from(vec!["a", "b", "c"])) as _)
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).expect("record batch");

        let props = build_writer_properties(1024, bloom_columns);
        let mut buffer = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).expect("arrow writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }

        let reader = SerializedFileReader::new(Bytes::from(buffer)).expect("serialized reader");
        let row_group = reader.metadata().row_group(0);
        schema_columns
            .iter()
            .map(|name| {
                row_group
                    .columns()
                    .iter()
                    .find(|c| c.column_path().string() == *name)
                    .and_then(ColumnChunkMetaData::bloom_filter_offset)
                    .is_some()
            })
            .collect()
    }

    #[test]
    fn writer_properties_emit_bloom_filter_for_listed_columns() {
        // The columns named in the bloom-filter list get a bloom
        // filter in the parquet footer; columns absent from the list
        // (here `service_name`) do not, even though they are part of
        // the same parquet file.
        let presence = bloom_filter_presence(&["trace_id", "span_id", "service_name"], &["trace_id", "span_id"]);
        assert_eq!(presence, vec![true, true, false]);
    }

    #[test]
    fn writer_properties_skip_bloom_filter_when_list_is_empty() {
        // No bloom filter columns configured ⇒ the parquet footer
        // carries no bloom filter offsets for any column.
        let presence = bloom_filter_presence(&["trace_id", "span_id"], &[]);
        assert_eq!(presence, vec![false, false]);
    }
}
