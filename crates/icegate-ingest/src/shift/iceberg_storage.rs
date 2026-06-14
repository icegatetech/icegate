use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt};
use iceberg::{
    Catalog, TableIdent,
    spec::DataFile,
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
};
/// Result of writing batches into parquet data files.
///
/// Re-exported from [`icegate_common::iceberg_write`] so the single write
/// pipeline owns the type; existing shift call sites keep referring to
/// `crate::shift::iceberg_storage::WrittenDataFiles`.
pub use icegate_common::iceberg_write::WrittenDataFiles;
/// Compute the writer rollover budget from the planner's per-task upper bound.
///
/// Re-exported from [`icegate_common::iceberg_write`]; ingest still sizes the
/// rolling writer from this failover policy.
pub use icegate_common::iceberg_write::writer_max_parquet_bytes;
use icegate_common::{
    WAL_OFFSET_PROPERTY,
    iceberg_write::{WriteConfig, write_record_batches_to_parquet},
    icegate_table_ident,
    parquet_writer::ColumnEncoding,
    resolve_wal_offset,
    retrier::{Retrier, RetrierConfig},
};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::{config::ShiftConfig, parquet_meta_reader::data_files_from_parquet_paths};
use crate::error::{IngestError, Result};

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

/// Iceberg storage for shift operations.
pub struct IcebergStorage {
    loader: TableLoader,
    row_group_size: usize,
    max_file_size_bytes: u64,
    /// Maximum Parquet data page size in bytes, forwarded to
    /// `WriterProperties::set_data_page_size_limit`.
    data_page_size_limit_bytes: usize,
    /// Column names that should get a Parquet bloom filter when written.
    ///
    /// The list is owned by the caller (typically a per-table
    /// `ShiftJobSpec`); this struct stays table-agnostic and just
    /// forwards the list to the writer-properties builder.
    bloom_filter_columns: &'static [&'static str],
    /// Per-column Parquet encoding overrides (empty = parquet-rs defaults).
    ///
    /// Forwarded to `set_column_encoding` on the writer-properties
    /// builder. Entries for columns absent from the table's schema are
    /// silently ignored, so a single combined list works across tables.
    column_encodings: &'static [ColumnEncoding],
    retrier: Retrier,
}

impl IcebergStorage {
    /// Creates a new Iceberg storage for the provided table and catalog.
    ///
    /// `bloom_filter_columns` is the per-table list of columns that
    /// should have a Parquet bloom filter; pass `&[]` if none.
    /// `column_encodings` is the per-column encoding override list;
    /// pass `&[]` to use parquet-rs defaults.
    pub fn new(
        catalog: Arc<dyn Catalog>,
        table: impl Into<String>,
        shift_config: &ShiftConfig,
        max_file_size_bytes: u64,
        bloom_filter_columns: &'static [&'static str],
        column_encodings: &'static [ColumnEncoding],
    ) -> Self {
        let table = table.into();
        let table_ident = icegate_table_ident(&table);
        Self {
            loader: TableLoader::new(catalog, table_ident, shift_config.write.table_cache_ttl_secs),
            row_group_size: shift_config.write.row_group_size,
            data_page_size_limit_bytes: shift_config.write.data_page_size_limit_bytes,
            max_file_size_bytes,
            bloom_filter_columns,
            column_encodings,
            retrier: Retrier::new(RetrierConfig::default()),
        }
    }

    async fn load_table(&self, cancel_token: &CancellationToken) -> Result<Table> {
        self.retry(cancel_token, || self.loader.load_cached()).await
    }

    async fn load_table_fresh(&self, cancel_token: &CancellationToken) -> Result<Table> {
        self.retry(cancel_token, || self.loader.load_fresh()).await
    }

    /// Returns the last committed WAL offset from the Iceberg snapshot history.
    ///
    /// Resolves the offset via [`resolve_wal_offset`], which walks the snapshot
    /// parent chain. Reading only the current snapshot would be wrong: compaction
    /// commits `replace` snapshots that omit [`WAL_OFFSET_PROPERTY`], so after a
    /// compaction the current snapshot has no offset and the Shifter would resume
    /// from 0, re-committing the whole WAL and duplicating every committed row.
    pub async fn get_last_offset(&self, cancel_token: &CancellationToken) -> Result<Option<u64>> {
        let table = self.load_table_fresh(cancel_token).await?;
        Ok(resolve_wal_offset(table.metadata())?)
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
    ///
    /// Delegates to the shared
    /// [`icegate_common::iceberg_write::write_record_batches_to_parquet`]
    /// pipeline (partition split → fanout rolling Parquet writer → orphan
    /// cleanup on failure), which also moves the CPU-heavy Arrow → Parquet
    /// encoding onto a `spawn_blocking` thread so the ingest hot path is not
    /// starved. This method only loads the (cached) table, assembles the
    /// [`WriteConfig`] from the storage's per-table tunables, and bridges the
    /// ingest stream's error type.
    pub async fn write_record_batches(
        &self,
        batches: BoxRecordBatchStream,
        cancel_token: &CancellationToken,
    ) -> Result<WrittenDataFiles> {
        let table = self.load_table(cancel_token).await?;
        let cfg = WriteConfig {
            row_group_size: self.row_group_size,
            data_page_size_limit_bytes: self.data_page_size_limit_bytes,
            max_file_size_bytes: self.max_file_size_bytes,
            bloom_filter_columns: self.bloom_filter_columns,
            column_encodings: self.column_encodings,
        };

        // Bridge the stream's error type: ingest yields `IngestError`, the
        // common pipeline wants `icegate_common::Error`. The orphan rule and
        // dependency direction forbid a `From<IngestError> for
        // icegate_common::Error` impl, so map explicitly at the call site.
        // TODO: Task 1.6 makes the merger natively yield `icegate_common::Error`,
        // dropping this bridge.
        let common_stream = batches.map_err(|e| icegate_common::Error::Write(e.to_string())).boxed();

        // `?` converts the returned `icegate_common::Error` into `IngestError`
        // via the existing `From` impl (write-pipeline messages map to `Shift`).
        Ok(write_record_batches_to_parquet(table, cfg, common_stream, cancel_token).await?)
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
