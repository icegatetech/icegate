//! Shift processor for moving data from queue to Iceberg.
//!
//! The processor continuously reads segments from the queue and commits
//! them to Iceberg tables with exactly-once delivery guarantees.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use arrow::record_batch::RecordBatch;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use icegate_common::{StorageBackend, StorageConfig, ICEGATE_NAMESPACE, LOGS_TABLE, LOGS_TOPIC};
use icegate_queue::QueueReader;
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, memory::InMemory, ObjectStore};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use super::{config::ShiftConfig, offset::get_committed_offset, writer::write_and_commit};
use crate::error::{MaintainError, Result};

/// Shift processor for a specific storage backend.
pub struct ShiftProcessor<S: ObjectStore> {
    catalog: Arc<dyn Catalog>,
    queue_reader: QueueReader<S>,
    config: ShiftConfig,
}

impl<S: ObjectStore> ShiftProcessor<S> {
    /// Creates a new shift processor.
    pub fn new(catalog: Arc<dyn Catalog>, store: Arc<S>, config: ShiftConfig) -> Self {
        let queue_reader = QueueReader::new(&config.queue.base_path, store);

        Self {
            catalog,
            queue_reader,
            config,
        }
    }

    /// Runs the shift processor until cancellation.
    ///
    /// The processor continuously reads from the queue and commits to Iceberg
    /// when no new data arrives for the configured timeout period.
    pub async fn run(&self, cancel_token: CancellationToken) -> Result<()> {
        // Load the logs table
        let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
        let table_ident = TableIdent::new(namespace, LOGS_TABLE.to_string());

        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(|e| MaintainError::Shift(format!("failed to load logs table: {e}")))?;

        // Get initial offset
        let mut offset = get_committed_offset(&table).map_or(0, |offset| offset + 1);
        tracing::info!("Resuming from committed offset: {}", offset);

        let poll_interval = Duration::from_millis(self.config.poll_interval_ms);
        let flush_interval = Duration::from_secs(self.config.flush_timeout_secs);

        let mut accumulated_batches: Vec<RecordBatch> = Vec::new();
        let mut last_flush_time = Instant::now();
        let mut highest_offset_in_batch: Option<u64> = None;

        tracing::info!(
            "Shift processor started, polling from offset {}, flush interval {:?}",
            offset,
            flush_interval
        );

        loop {
            // Check for cancellation
            if cancel_token.is_cancelled() {
                // Flush any remaining data before shutdown
                if !accumulated_batches.is_empty() {
                    if let Some(max_offset) = highest_offset_in_batch {
                        tracing::info!("Flushing remaining data before shutdown");
                        self.flush_and_commit(&table, &mut accumulated_batches, max_offset).await?;
                    }
                }
                tracing::info!("Shift processor stopped");
                return Ok(());
            }

            // Check if we should flush due to time interval (periodic flush)
            if !accumulated_batches.is_empty() && last_flush_time.elapsed() >= flush_interval {
                if let Some(max_offset) = highest_offset_in_batch {
                    tracing::info!(
                        "Flush interval reached ({:?}), committing {} batches",
                        flush_interval,
                        accumulated_batches.len()
                    );
                    self.flush_and_commit(&table, &mut accumulated_batches, max_offset).await?;
                    offset = max_offset + 1;
                    highest_offset_in_batch = None;
                    last_flush_time = Instant::now();
                }
            }

            // List available segments from current offset
            let segments = self.queue_reader.list_segments_from(&LOGS_TOPIC.to_string(), offset).await?;

            if segments.is_empty() {
                tracing::debug!("No new segments, sleeping for {:?}", poll_interval);
                // Wait before polling again
                tokio::select! {
                    () = sleep(poll_interval) => {},
                    () = cancel_token.cancelled() => {
                        continue; // Will handle cancellation at top of loop
                    }
                }
                continue;
            }

            // Read new segments
            for segment in segments {
                let offset = segment.offset;

                match self.queue_reader.read_segment(&LOGS_TOPIC.to_string(), offset).await {
                    Ok(batches) => {
                        if !batches.is_empty() {
                            let row_count: usize = batches.iter().map(RecordBatch::num_rows).sum();
                            tracing::debug!(
                                "Read segment {} with {} batches ({} rows)",
                                offset,
                                batches.len(),
                                row_count
                            );
                            accumulated_batches.extend(batches);
                            highest_offset_in_batch =
                                Some(highest_offset_in_batch.map_or(offset, |prev| prev.max(offset)));
                        }
                    },
                    Err(e) => {
                        tracing::warn!("Failed to read segment {}: {}, skipping", offset, e);
                    },
                }
            }

            // Update current offset to continue from next segment
            if let Some(max_offset) = highest_offset_in_batch {
                offset = max_offset + 1;
            }
        }
    }

    /// Flushes accumulated batches and commits to Iceberg.
    async fn flush_and_commit(
        &self,
        table: &iceberg::table::Table,
        batches: &mut Vec<RecordBatch>,
        new_offset: u64,
    ) -> Result<()> {
        let batch_data = std::mem::take(batches);

        let result = write_and_commit(
            table,
            &self.catalog,
            batch_data,
            LOGS_TOPIC,
            new_offset,
            self.config.row_group_size,
            self.config.max_file_size_bytes(),
        )
        .await;

        match result {
            Ok(rows_written) => {
                tracing::info!(
                    "Committed {} rows to Iceberg, offset now at {}",
                    rows_written,
                    new_offset
                );
                Ok(())
            },
            Err(e) => {
                tracing::error!("SHIFT ERROR: Failed to write and commit: {e}");
                Err(e)
            },
        }
    }
}

/// Creates an object store from storage configuration.
///
/// # Errors
///
/// Returns an error if the object store cannot be created.
#[allow(clippy::expect_used)] // Storage initialization failures are critical
pub fn create_object_store(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
    match &config.backend {
        StorageBackend::Memory => Ok(Arc::new(InMemory::new())),
        StorageBackend::FileSystem {
            root_path,
        } => {
            let store = LocalFileSystem::new_with_prefix(root_path)
                .map_err(|e| MaintainError::Shift(format!("failed to create local file system store: {e}")))?;
            Ok(Arc::new(store))
        },
        StorageBackend::S3(s3_config) => {
            let mut builder = AmazonS3Builder::new().with_bucket_name(&s3_config.bucket).with_region(&s3_config.region);

            if let Some(endpoint) = &s3_config.endpoint {
                builder = builder.with_endpoint(endpoint).with_allow_http(true);
            }

            // Use environment variables for credentials
            if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
                builder = builder.with_access_key_id(access_key);
            }
            if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                builder = builder.with_secret_access_key(secret_key);
            }

            let store = builder
                .build()
                .map_err(|e| MaintainError::Shift(format!("failed to create S3 object store: {e}")))?;
            Ok(Arc::new(store))
        },
    }
}

/// Runs the shift processor with the appropriate object store type.
///
/// This is a convenience function that handles the dynamic dispatch
/// for different storage backends. It parses the queue's `base_path` to
/// determine the storage type and bucket.
pub async fn run_shift(
    catalog: Arc<dyn Catalog>,
    storage_config: &StorageConfig,
    shift_config: ShiftConfig,
    cancel_token: CancellationToken,
) -> Result<()> {
    let base_path = &shift_config.queue.base_path;

    if base_path.starts_with("s3://") {
        // Parse S3 URL: s3://bucket/prefix
        let path_without_scheme = base_path.strip_prefix("s3://").unwrap_or(base_path);
        let (bucket, prefix) = path_without_scheme.split_once('/').map_or_else(
            || (path_without_scheme.to_string(), String::new()),
            |(b, p)| (b.to_string(), p.to_string()),
        );

        // Get S3 config from storage backend for endpoint/region settings
        let (endpoint, region) = match &storage_config.backend {
            StorageBackend::S3(s3_config) => (s3_config.endpoint.clone(), s3_config.region.clone()),
            _ => (None, "us-east-1".to_string()),
        };

        let mut builder = AmazonS3Builder::new().with_bucket_name(&bucket).with_region(&region);

        if let Some(endpoint) = &endpoint {
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }

        if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
            builder = builder.with_access_key_id(access_key);
        }
        if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret_key);
        }

        let store = Arc::new(
            builder
                .build()
                .map_err(|e| MaintainError::Shift(format!("failed to create S3 object store for queue: {e}")))?,
        );

        // Update shift config to use just the prefix (bucket is handled by store)
        let mut shift_config = shift_config;
        shift_config.queue.base_path = prefix;

        let processor = ShiftProcessor::new(catalog, store, shift_config);
        processor.run(cancel_token).await
    } else if base_path.starts_with("file://") || base_path.starts_with('/') {
        // Local filesystem
        let path = base_path.strip_prefix("file://").unwrap_or(base_path);
        let store = Arc::new(
            LocalFileSystem::new_with_prefix(path)
                .map_err(|e| MaintainError::Shift(format!("failed to create local file system store: {e}")))?,
        );

        let mut shift_config = shift_config;
        shift_config.queue.base_path = String::new();

        let processor = ShiftProcessor::new(catalog, store, shift_config);
        processor.run(cancel_token).await
    } else {
        // Memory or relative path - use memory store
        let store = Arc::new(InMemory::new());
        let processor = ShiftProcessor::new(catalog, store, shift_config);
        processor.run(cancel_token).await
    }
}
