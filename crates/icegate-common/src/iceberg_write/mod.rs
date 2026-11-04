//! Shared Iceberg Parquet write pipeline.
//!
//! This module owns the single implementation that turns a stream of Arrow
//! [`RecordBatch`]es into committed-ready Iceberg [`DataFile`]s: it builds the
//! Parquet writer properties (encoding, bloom filters, compression), splits
//! every batch by partition, fans the partitions out to a rolling Parquet
//! writer, and cleans up any orphaned data files if the write fails partway.
//!
//! It does **not** commit anything to a catalog — committing (WAL-offset
//! bookkeeping, transactions, table caching) stays with the caller. The
//! pipeline only produces the data files and the row count.
//!
//! Both the ingest shift path and the compaction path use this entry point so
//! the Arrow → Parquet encoding policy lives in exactly one place.

use std::{collections::HashSet, panic::AssertUnwindSafe, pin::Pin, sync::Arc};

use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use iceberg::{
    arrow::RecordBatchPartitionSplitter,
    io::FileIO,
    spec::{DataFile, DataFileFormat},
    table::Table,
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            ParquetWriterBuilder,
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator, LocationGenerator},
            rolling_writer::RollingFileWriterBuilder,
        },
        partitioning::{PartitioningWriter, fanout_writer::FanoutWriter, unpartitioned_writer::UnpartitionedWriter},
    },
};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, warn};
use uuid::Uuid;

use crate::{
    error::{CommonError, Result},
    parquet_writer::{ColumnEncoding, build_writer_properties},
};

/// Stream of merge-ordered record batches written to Iceberg.
///
/// Items are [`crate::error::Result<RecordBatch>`] so that an upstream read
/// error short-circuits the write. Callers whose source stream yields a
/// different error type must bridge it into [`CommonError`] at the call site
/// (the dependency direction forbids a blanket `From` impl in this crate).
pub type CommonRecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Result of writing batches into parquet data files.
pub struct WrittenDataFiles {
    /// Data files written (not yet committed to any catalog).
    pub data_files: Vec<DataFile>,
    /// Total rows written across all data files.
    pub rows_written: usize,
}

/// Scalar configuration for a single Parquet write.
///
/// Bundles the per-write tunables so the public entry point stays under the
/// project's argument-count limit. `Copy` because every field is a scalar or a
/// `'static` slice; `Debug` so the values appear in the write pipeline's
/// tracing span (matching the pre-extraction per-arg instrumentation).
#[derive(Clone, Copy, Debug)]
pub struct WriteConfig {
    /// Maximum number of rows per Parquet row group.
    pub row_group_size: usize,
    /// Maximum Parquet data page size in bytes, forwarded to
    /// `WriterProperties::set_data_page_size_limit`.
    pub data_page_size_limit_bytes: usize,
    /// Rolling-writer rollover budget in bytes (see [`writer_max_parquet_bytes`]).
    pub max_file_size_bytes: u64,
    /// Column names that should get a Parquet bloom filter when written.
    ///
    /// Owned by the caller (typically a per-table spec); the pipeline stays
    /// table-agnostic and forwards the list to the writer-properties builder.
    pub bloom_filter_columns: &'static [&'static str],
    /// Per-column Parquet encoding overrides (empty = parquet-rs defaults).
    ///
    /// Forwarded to `set_column_encoding`; entries for columns absent from the
    /// table's schema are silently ignored, so one combined list works across
    /// tables.
    pub column_encodings: &'static [ColumnEncoding],
}

/// Failover multiplier on top of the planner's upper bound. The writer flips
/// to a new file only when actual encoded parquet bytes overshoot
/// `upper_bound_input_mb_per_task * 1024 * 1024 * factor` — i.e., parquet overhead
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
#[must_use]
pub const fn writer_max_parquet_bytes(upper_bound_input_bytes: u64) -> u64 {
    upper_bound_input_bytes.saturating_mul(WRITER_FILE_SIZE_FAILOVER_FACTOR)
}

/// Wrapper around the default Iceberg location generator that records every
/// generated output path.
///
/// Parquet files may already be created in object storage before the write
/// pipeline finishes successfully. If the write later fails, the caller must
/// remove those orphaned files. This wrapper keeps the generated paths so
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

/// Writes record batches into parquet data files without committing to Iceberg.
///
/// This entry point:
/// 1. Reads merge-ordered batches from `batches`
/// 2. Splits every batch by partition using `RecordBatchPartitionSplitter`
/// 3. Writes partition batches immediately via `FanoutWriter`
///
/// The CPU-heavy Arrow → Parquet encoding (column encoding, ZSTD compression,
/// statistics) runs synchronously inside `AsyncArrowWriter::write`, which
/// would starve the caller's hot path (e.g. the ingest GRPC handler / WAL
/// flush) if it shared the same tokio worker threads. To avoid that, the whole
/// pipeline is moved onto a dedicated [`tokio::task::spawn_blocking`] thread;
/// inside it the runtime is re-entered via `Handle::block_on` so async S3 I/O
/// still resolves normally. The caller's tracing span is captured and entered
/// on the blocking thread so the pipeline stays a child of the caller.
///
/// # Arguments
///
/// * `table` - The target Iceberg table; its current schema and partition spec
///   drive encoding and partitioning.
/// * `cfg` - Scalar write configuration (row-group/page sizes, rollover
///   budget, bloom-filter columns, encoding overrides).
/// * `batches` - Merge-ordered source batches to encode.
/// * `cancel_token` - Cooperative cancellation; checked between source batches
///   and between per-partition writes (an in-flight write/close await is not
///   itself interruptible).
///
/// # Returns
///
/// The written data files and the total row count.
///
/// # Errors
///
/// Returns [`CommonError::Iceberg`] for writer/splitter failures and
/// [`CommonError::Write`] for cancellation, row-count overflow, partition-split
/// failures, or a panic on the blocking thread. A batch whose columns do not
/// match the table's current schema by position, name, type, and nullability is
/// refused with [`CommonError::BatchSchemaMismatch`]. On any error the partially written data files
/// are deleted from object storage before returning.
pub async fn write_record_batches_to_parquet(
    table: Table,
    cfg: WriteConfig,
    batches: CommonRecordBatchStream,
    cancel_token: &CancellationToken,
) -> Result<WrittenDataFiles> {
    // Move the entire Parquet write pipeline off the tokio async-worker
    // threads.  `write_parquet_files_once` performs CPU-heavy Arrow →
    // Parquet encoding (column encoding, ZSTD compression, statistics)
    // synchronously inside `AsyncArrowWriter::write`, which starves the
    // caller's hot-path (GRPC handler / WAL flush) when both share the
    // same tokio runtime.
    //
    // `spawn_blocking` runs on a dedicated thread-pool.  Inside it we
    // re-enter the runtime via `Handle::block_on` so that async S3 I/O
    // still resolves normally.
    let handle = tokio::runtime::Handle::current();
    let cancel = cancel_token.clone();

    // Capture the current tracing span so that `write_parquet_files_once`
    // remains a child of the caller (e.g. `shift_run`) even though it
    // executes on a different thread.
    let span = tracing::Span::current();

    tokio::task::spawn_blocking(move || {
        let _guard = span.enter();
        handle.block_on(write_parquet_files_once(table, cfg, batches, &cancel))
    })
    .await
    .map_err(|e| CommonError::Write(format!("shift write task panicked: {e}")))?
}

#[tracing::instrument(skip(table, batches, cancel_token))]
async fn write_parquet_files_once(
    table: Table,
    cfg: WriteConfig,
    batches: CommonRecordBatchStream,
    cancel_token: &CancellationToken,
) -> Result<WrittenDataFiles> {
    let table_metadata = table.metadata().clone();
    let table_file_io = table.file_io().clone();

    let generated_paths = Arc::new(std::sync::Mutex::new(Vec::new()));
    let location_generator = TrackingLocationGenerator::new(
        DefaultLocationGenerator::new(&table_metadata).map_err(CommonError::Iceberg)?,
        Arc::clone(&generated_paths),
    );

    // Generate unique file prefix with UUID to avoid conflicts
    let write_id = Uuid::now_v7();
    let file_name_generator = DefaultFileNameGenerator::new(write_id.to_string(), None, DataFileFormat::Parquet);

    // TODO(med): Issue #101. Add semaphore/CPU budget. The limit should protect ingest from ZSTD spikes during multiple shift tasks.
    let writer_props = build_writer_properties(
        cfg.row_group_size,
        cfg.data_page_size_limit_bytes,
        cfg.bloom_filter_columns,
        cfg.column_encodings,
    );

    let parquet_writer_builder = ParquetWriterBuilder::new(writer_props, table_metadata.current_schema().clone());
    let table_arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(table_metadata.current_schema())?);
    let mut batches = verify_batch_schemas(batches, table_arrow_schema);

    // Convert to `usize` for the iceberg writer API. On 64-bit targets `u64`
    // never overflows `usize`; saturate on 32-bit targets.
    let target_file_size = usize::try_from(cfg.max_file_size_bytes).unwrap_or(usize::MAX);
    let rolling_writer_builder = RollingFileWriterBuilder::new(
        parquet_writer_builder,
        target_file_size,
        table_file_io.clone(),
        location_generator,
        file_name_generator,
    );

    let data_file_writer_builder = DataFileWriterBuilder::new(rolling_writer_builder);

    // `RecordBatchPartitionSplitter` refuses to compute partition values for an
    // unpartitioned spec outright (there is nothing to compute — see
    // `PartitionValueCalculator::try_new`), and an unpartitioned table has only
    // ever one "partition": the whole table. `prices` is the one table in the
    // catalog this branch exists for.
    let unpartitioned = table_metadata.default_partition_spec().is_unpartitioned();

    // Run the encoding pipeline under `catch_unwind` so a panic on the blocking
    // thread (e.g. inside Parquet encoding or `FanoutWriter::close`) still
    // triggers cleanup of the partially written data files before unwinding.
    // Without this the unwind would skip the cleanup below and leak orphaned
    // objects, breaking the documented "deleted on any error, including a panic"
    // guarantee. `AssertUnwindSafe` is sound here: after catching we only run
    // cleanup (which tolerates a poisoned `generated_paths` mutex) and re-raise
    // the original panic, preserving the caller's panic-to-error mapping.
    let write_result = AssertUnwindSafe(async {
        if unpartitioned {
            write_unpartitioned_batches(data_file_writer_builder, &mut batches, cancel_token).await
        } else {
            let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
                table_metadata.current_schema().clone(),
                table_metadata.default_partition_spec().clone(),
            )
            .map_err(CommonError::Iceberg)?;
            write_partitioned_batches(data_file_writer_builder, &splitter, &mut batches, cancel_token).await
        }
    })
    .catch_unwind()
    .await;

    match write_result {
        Ok(result) => {
            if result.is_err() {
                cleanup_generated_data_files(&table_file_io, &generated_paths).await;
            }
            result
        }
        // A panic on the blocking thread must still clean up the partial writes
        // before unwinding, otherwise the orphaned objects leak. Re-raise the
        // panic so the caller still surfaces it as `CommonError::Write`.
        Err(panic_payload) => {
            cleanup_generated_data_files(&table_file_io, &generated_paths).await;
            std::panic::resume_unwind(panic_payload);
        }
    }
}

/// Fail the stream on the first batch whose schema [`verify_batch_schema`]
/// refuses against `table_schema`.
///
/// The full comparison runs once per distinct schema in the stream; see
/// [`VerifiedBatchSchemas`].
fn verify_batch_schemas(batches: CommonRecordBatchStream, table_schema: SchemaRef) -> CommonRecordBatchStream {
    let mut verified_schemas = VerifiedBatchSchemas::new(table_schema);
    batches
        .map(move |batch| {
            let batch = batch?;
            verified_schemas.verify_schema(batch.schema_ref())?;
            Ok(batch)
        })
        .boxed()
}

/// Batch schemas already checked against one table schema, one per distinct schema.
///
/// Merged output alternates between the schemas of its inputs (`slice` keeps the
/// input's schema), so remembering only the last verified schema would repeat the
/// full comparison at every switch. A parquet reader gives every batch a schema
/// `Arc` of its own, so a schema is recognised by `Arc` identity first and by value
/// otherwise: a batch with a remembered `Arc` costs a pointer comparison, one with
/// another `Arc` of a remembered schema a value comparison, and neither adds an
/// entry. Memory is one entry per distinct schema, not per batch.
#[derive(Debug)]
struct VerifiedBatchSchemas {
    table_schema: SchemaRef,
    verified_schemas: Vec<SchemaRef>,
}

impl VerifiedBatchSchemas {
    const fn new(table_schema: SchemaRef) -> Self {
        Self {
            table_schema,
            verified_schemas: Vec::new(),
        }
    }

    /// Check `batch_schema` against the table schema unless an equal schema passed before.
    ///
    /// # Errors
    ///
    /// Returns [`CommonError::BatchSchemaMismatch`] from [`verify_batch_schema`].
    fn verify_schema(&mut self, batch_schema: &SchemaRef) -> Result<()> {
        if self.verified_schemas.iter().any(|verified| Arc::ptr_eq(verified, batch_schema))
            || self.verified_schemas.iter().any(|verified| **verified == **batch_schema)
        {
            return Ok(());
        }
        verify_batch_schema(batch_schema, &self.table_schema)?;
        self.verified_schemas.push(Arc::clone(batch_schema));
        Ok(())
    }
}

/// Check that `batch_schema` binds to `table_schema` position by position.
///
/// Name, Arrow type, and nullability must match at every position; top-level
/// field metadata is not compared (pinned by
/// `verify_batch_schema_accepts_a_batch_matching_the_table_by_position`).
///
/// # Errors
///
/// Returns [`CommonError::BatchSchemaMismatch`] naming the first differing
/// column, or the first extra or missing one when the counts differ.
fn verify_batch_schema(batch_schema: &Schema, table_schema: &Schema) -> Result<()> {
    let batch_fields = batch_schema.fields();
    let table_fields = table_schema.fields();
    for (position, (batch_field, table_field)) in batch_fields.iter().zip(table_fields.iter()).enumerate() {
        if batch_field.name() != table_field.name()
            || batch_field.data_type() != table_field.data_type()
            || batch_field.is_nullable() != table_field.is_nullable()
        {
            return Err(CommonError::BatchSchemaMismatch(format!(
                "column {position} is '{}' {:?} (nullable: {}) in the batch but '{}' {:?} (nullable: {}) in the table",
                batch_field.name(),
                batch_field.data_type(),
                batch_field.is_nullable(),
                table_field.name(),
                table_field.data_type(),
                table_field.is_nullable(),
            )));
        }
    }
    match batch_fields.len().cmp(&table_fields.len()) {
        std::cmp::Ordering::Equal => Ok(()),
        std::cmp::Ordering::Greater => Err(CommonError::BatchSchemaMismatch(format!(
            "the batch carries column '{}' the table schema does not have",
            batch_fields[table_fields.len()].name()
        ))),
        std::cmp::Ordering::Less => Err(CommonError::BatchSchemaMismatch(format!(
            "the batch lacks column '{}' of the table schema",
            table_fields[batch_fields.len()].name()
        ))),
    }
}

/// Write every batch straight through a single writer with no partition key,
/// for a table whose partition spec is empty (`is_unpartitioned()`).
///
/// There is only ever one "partition" — the whole table — so this bypasses
/// `RecordBatchPartitionSplitter`/`FanoutWriter` entirely rather than fanning
/// out over a single, always-empty partition key.
async fn write_unpartitioned_batches(
    writer_builder: DataFileWriterBuilder<ParquetWriterBuilder, TrackingLocationGenerator, DefaultFileNameGenerator>,
    batches: &mut CommonRecordBatchStream,
    cancel_token: &CancellationToken,
) -> Result<WrittenDataFiles> {
    let mut writer = UnpartitionedWriter::new(writer_builder);
    let mut rows_written = 0usize;
    while let Some(batch) = batches.try_next().await? {
        if cancel_token.is_cancelled() {
            return Err(CommonError::Write(
                "shift task cancelled during parquet write".to_string(),
            ));
        }
        rows_written = rows_written
            .checked_add(batch.num_rows())
            .ok_or_else(|| CommonError::Write("rows written overflow".to_string()))?;
        writer.write(batch).await.map_err(CommonError::Iceberg)?;
    }

    let span = tracing::info_span!("iceberg_write_close");
    let data_files: Vec<DataFile> = writer.close().instrument(span).await.map_err(CommonError::Iceberg)?;
    tracing::info!(
        "Complete write {} parquet files for the unpartitioned table",
        data_files.len()
    );

    Ok(WrittenDataFiles {
        data_files,
        rows_written,
    })
}

/// Split every batch by partition and fan the partitions out across a
/// [`FanoutWriter`] built from `writer_builder`.
async fn write_partitioned_batches(
    writer_builder: DataFileWriterBuilder<ParquetWriterBuilder, TrackingLocationGenerator, DefaultFileNameGenerator>,
    splitter: &RecordBatchPartitionSplitter,
    batches: &mut CommonRecordBatchStream,
    cancel_token: &CancellationToken,
) -> Result<WrittenDataFiles> {
    let mut fanout_writer = FanoutWriter::new(writer_builder);
    let mut rows_written = 0usize;
    let mut partitioned_batches_total = 0usize;
    while let Some(batch) = batches.try_next().await? {
        if cancel_token.is_cancelled() {
            return Err(CommonError::Write(
                "shift task cancelled during parquet write".to_string(),
            ));
        }
        rows_written = rows_written
            .checked_add(batch.num_rows())
            .ok_or_else(|| CommonError::Write("rows written overflow".to_string()))?;
        let partitioned_batches = splitter
            .split(&batch)
            .map_err(|e| CommonError::Write(format!("failed to split batch by partition: {e}")))?;
        partitioned_batches_total = partitioned_batches_total
            .checked_add(partitioned_batches.len())
            .ok_or_else(|| CommonError::Write("partitioned batch count overflow".to_string()))?;

        for (partition_key, partition_batch) in partitioned_batches {
            // Re-check cancellation between partition writes too, not only
            // between source batches: a batch may split into many partitions,
            // and an individual `fanout_writer.write`/`close` await is not
            // itself cancellation-aware, so this bounds how much of a
            // cancelled batch still gets uploaded.
            if cancel_token.is_cancelled() {
                return Err(CommonError::Write(
                    "shift task cancelled during parquet write".to_string(),
                ));
            }
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
                .map_err(CommonError::Iceberg)?;
        }
    }

    // Close writer and get data files
    let span = tracing::info_span!("iceberg_write_close");
    let data_files: Vec<DataFile> = fanout_writer.close().instrument(span).await.map_err(CommonError::Iceberg)?;

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

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::{
            ArrayRef, FixedSizeBinaryArray, Int64Array, StringArray, StringBuilder, TimestampMicrosecondArray,
            new_null_array,
        },
        datatypes::{DataType, Field, Schema as ArrowSchema},
        record_batch::RecordBatch,
    };
    use bytes::Bytes;
    use futures::StreamExt;
    use iceberg::{
        NamespaceIdent, TableCreation,
        arrow::{ArrowFileReader, schema_to_arrow_schema},
        io::FileIO,
        spec::DataFile,
    };
    use parquet::{
        arrow::{ArrowWriter, async_reader::AsyncFileReader},
        basic::{Compression, Encoding, ZstdLevel},
        file::{
            metadata::{ColumnChunkMetaData, ParquetMetaData},
            reader::{FileReader, SerializedFileReader},
        },
    };
    use uuid::Uuid;

    use super::{WriteConfig, cleanup_generated_data_files, write_record_batches_to_parquet, writer_max_parquet_bytes};
    use crate::{
        ICEGATE_NAMESPACE,
        catalog::{CatalogBackend, CatalogConfig, IoHandle},
        parquet_encoding::{SPANS_BLOOM_COLUMNS, SPANS_COLUMN_ENCODINGS},
        parquet_writer::build_writer_properties,
        schema::{
            COL_DURATION_MICROS, COL_END_TIMESTAMP, COL_INGESTED_TIMESTAMP, COL_NAME, COL_SPAN_ID, COL_TENANT_ID,
            COL_TIMESTAMP, COL_TRACE_ID, spans_partition_spec, spans_schema, spans_sort_order,
        },
    };

    /// Failover policy guard: writer rollover budget must be exactly
    /// `upper_bound_bytes * 2`.  If this changes, the planner's
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

    fn id_and_name_schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    #[test]
    fn verify_batch_schema_accepts_a_batch_matching_the_table_by_position() {
        let field_id = HashMap::from([("PARQUET:field_id".to_string(), "1".to_string())]);
        let table = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(field_id),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert!(super::verify_batch_schema(&id_and_name_schema(), &table).is_ok());
    }

    #[test]
    fn verify_batch_schema_refuses_an_extra_a_missing_and_a_retyped_column() {
        let table = id_and_name_schema();
        let cases = [
            (
                "extra column",
                ArrowSchema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("name", DataType::Utf8, true),
                    Field::new("environment", DataType::Utf8, true),
                ]),
            ),
            (
                "missing column",
                ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]),
            ),
            (
                "retyped column",
                ArrowSchema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, true),
                ]),
            ),
        ];
        for (case, batch) in cases {
            let result = super::verify_batch_schema(&batch, &table);
            assert!(
                matches!(result, Err(crate::error::CommonError::BatchSchemaMismatch(_))),
                "{case}: got {result:?}"
            );
        }
    }

    /// The columns share one type, so only the name or the nullability can tell them
    /// apart: binding by position alone is what would write a column's data under
    /// another column's name.
    #[test]
    fn verify_batch_schema_refuses_a_column_of_another_name_or_nullability() {
        let table = ArrowSchema::new(vec![
            Field::new("first", DataType::Utf8, false),
            Field::new("second", DataType::Utf8, true),
        ]);
        let cases = [
            (
                "columns in the reverse order",
                ArrowSchema::new(vec![
                    Field::new("second", DataType::Utf8, false),
                    Field::new("first", DataType::Utf8, true),
                ]),
            ),
            (
                "renamed column",
                ArrowSchema::new(vec![
                    Field::new("first", DataType::Utf8, false),
                    Field::new("third", DataType::Utf8, true),
                ]),
            ),
            (
                "column of another nullability",
                ArrowSchema::new(vec![
                    Field::new("first", DataType::Utf8, true),
                    Field::new("second", DataType::Utf8, true),
                ]),
            ),
        ];
        for (case, batch) in cases {
            let result = super::verify_batch_schema(&batch, &table);
            assert!(
                matches!(result, Err(crate::error::CommonError::BatchSchemaMismatch(_))),
                "{case}: got {result:?}"
            );
        }
    }

    /// Merged output alternates between the schemas of its inputs: each schema is
    /// remembered after its first comparison, not only the last one seen.
    #[test]
    fn verified_batch_schemas_remember_every_alternating_schema() {
        let mut verified_schemas = super::VerifiedBatchSchemas::new(Arc::new(id_and_name_schema()));
        let first_input = Arc::new(id_and_name_schema());
        // Top-level field metadata is not compared against the table, so this schema
        // matches the table while differing from the first input by value.
        let second_input = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false)
                .with_metadata(HashMap::from([("PARQUET:field_id".to_string(), "1".to_string())])),
            Field::new("name", DataType::Utf8, true),
        ]));

        for schema in [&first_input, &second_input, &first_input, &second_input] {
            verified_schemas.verify_schema(schema).expect("schema matches the table");
        }

        assert_eq!(verified_schemas.verified_schemas.len(), 2);
    }

    /// A parquet reader gives every batch a schema `Arc` of its own: batches of one
    /// schema under distinct `Arc`s keep a single entry.
    #[test]
    fn verified_batch_schemas_keep_one_entry_for_one_schema_under_distinct_arcs() {
        let mut verified_schemas = super::VerifiedBatchSchemas::new(Arc::new(id_and_name_schema()));

        for _ in 0..3 {
            verified_schemas
                .verify_schema(&Arc::new(id_and_name_schema()))
                .expect("schema matches the table");
        }

        assert_eq!(verified_schemas.verified_schemas.len(), 1);
    }

    #[test]
    fn verified_batch_schemas_refuse_a_mismatched_schema_every_time() {
        let mut verified_schemas = super::VerifiedBatchSchemas::new(Arc::new(id_and_name_schema()));
        let retyped = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        for attempt in 0..2 {
            let result = verified_schemas.verify_schema(&retyped);
            assert!(
                matches!(result, Err(crate::error::CommonError::BatchSchemaMismatch(_))),
                "attempt {attempt}: got {result:?}"
            );
        }
        assert!(verified_schemas.verified_schemas.is_empty());
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

        let props = build_writer_properties(1024, 2 * 1024 * 1024, bloom_columns, &[]);
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

    /// Build an empty `MAP<Utf8,Utf8>` column of length `rows` whose type is
    /// **exactly** the named field of the target arrow schema. Required span map
    /// columns are non-nullable, so every row is an empty (zero-entry) map.
    ///
    /// Built via [`MapBuilder`] with the schema's own key/value `Field`s
    /// (carrying their `PARQUET:field_id` metadata) attached through
    /// `with_keys_field` / `with_values_field`. This reproduces the canonical
    /// `MapArray` layout the ingest transform produces — a hand-rolled
    /// `MapArray::new` is not recognised as a map by iceberg's value visitor and
    /// makes the writer fail field-id resolution.
    fn empty_string_map(field: &Field, rows: usize) -> ArrayRef {
        use arrow::array::{MapBuilder, MapFieldNames};

        let DataType::Map(entry_field, _) = field.data_type() else {
            panic!("{} must be a Map", field.name());
        };
        let DataType::Struct(kv_fields) = entry_field.data_type() else {
            panic!("map entry must be a Struct");
        };
        let key_field = Arc::clone(&kv_fields[0]);
        let value_field = Arc::clone(&kv_fields[1]);

        let mut builder = MapBuilder::new(
            Some(MapFieldNames {
                entry: entry_field.name().clone(),
                key: key_field.name().clone(),
                value: value_field.name().clone(),
            }),
            StringBuilder::new(),
            StringBuilder::new(),
        )
        .with_keys_field(key_field)
        .with_values_field(value_field);

        for _ in 0..rows {
            // No entries for this row; `append(true)` closes a valid empty map.
            builder.append(true).expect("append empty map row");
        }
        Arc::new(builder.finish())
    }

    /// Build one Arrow `RecordBatch` for the `spans` table with `rows` rows in a
    /// single `(tenant, day)` partition.
    ///
    /// The Arrow schema is derived from `table_schema` — the table's **current**
    /// iceberg schema, whose field ids the catalog may have reassigned on
    /// `create_table` (nested map/list children in particular are renumbered).
    /// The writer visits the batch paired with this exact schema, so the batch
    /// must carry the table's field ids, not a fresh `spans_schema()`'s. Column
    /// **names** are stable across reassignment, so the by-name population below
    /// is unaffected.
    ///
    /// Required columns are populated; optional scalar/list columns (events,
    /// links, `parent_span_id`, …) are all-null. `trace_id`/`span_id`/`timestamp`
    /// carry real, monotonically varying values so the configured encodings
    /// actually fire.
    fn spans_batch(table_schema: &iceberg::spec::Schema, rows: usize) -> RecordBatch {
        let arrow_schema = Arc::new(schema_to_arrow_schema(table_schema).expect("spans arrow schema"));

        // Each row gets a distinct, monotonically increasing value per column so
        // the DELTA encodings have non-trivial deltas to encode. `i` stays well
        // within every target type for realistic `rows`, so the conversions
        // below are exact (and asserted with `try_from`).
        let base_micros: i64 = 1_749_600_000_000_000; // 2026-06-11T00:00:00Z
        let ts: Vec<i64> = (0..rows)
            .map(|i| base_micros + i64::try_from(i).expect("row index fits i64"))
            .collect();
        let durations: Vec<i64> = (0..rows).map(|i| i64::try_from(i).expect("row index fits i64")).collect();
        let trace_vals: Vec<[u8; 16]> = (0..rows).map(|i| [u8::try_from(i % 256).unwrap_or(0); 16]).collect();
        let span_vals: Vec<[u8; 8]> = (0..rows).map(|i| [u8::try_from(i % 256).unwrap_or(0); 8]).collect();

        let columns: Vec<ArrayRef> = arrow_schema
            .fields()
            .iter()
            .map(|field| -> ArrayRef {
                match field.name().as_str() {
                    COL_TENANT_ID => Arc::new(StringArray::from(vec!["tenant-a"; rows])),
                    COL_TRACE_ID => Arc::new(
                        FixedSizeBinaryArray::try_from_iter(trace_vals.iter().map(|v| v.to_vec()))
                            .expect("trace_id array"),
                    ),
                    COL_SPAN_ID => Arc::new(
                        FixedSizeBinaryArray::try_from_iter(span_vals.iter().map(|v| v.to_vec()))
                            .expect("span_id array"),
                    ),
                    COL_TIMESTAMP | COL_END_TIMESTAMP | COL_INGESTED_TIMESTAMP => {
                        Arc::new(TimestampMicrosecondArray::from(ts.clone()))
                    }
                    COL_DURATION_MICROS => Arc::new(Int64Array::from(durations.clone())),
                    COL_NAME => Arc::new(StringArray::from(vec!["span"; rows])),
                    // Required MAP columns: resource_attributes, span_attributes.
                    _ if matches!(field.data_type(), DataType::Map(..)) => empty_string_map(field, rows),
                    // Everything else is optional (service_name, parent_span_id,
                    // trace_state, kind, …, events, links): typed all-null.
                    _ => new_null_array(field.data_type(), rows),
                }
            })
            .collect();

        RecordBatch::try_new(arrow_schema, columns).expect("spans record batch")
    }

    /// In-memory Iceberg table from the production spans schema/spec/sort.
    async fn create_spans_table() -> iceberg::table::Table {
        let catalog_config = CatalogConfig {
            backend: CatalogBackend::Memory,
            warehouse: format!("memory://iceberg-write-spans-{}", Uuid::new_v4()),
            properties: HashMap::new(),
            cache: None,
        };
        let catalog = crate::catalog::CatalogBuilder::from_config(
            &catalog_config,
            &IoHandle::noop(),
            super::CancellationToken::new(),
        )
        .await
        .expect("memory catalog");
        let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");

        let iceberg_schema = spans_schema().expect("spans schema");
        let partition_spec = spans_partition_spec(&iceberg_schema).expect("spans partition spec");
        let sort_order = spans_sort_order(&iceberg_schema).expect("spans sort order");
        let table_name = format!("spans_enc_{}", Uuid::new_v4().simple());
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name(table_name)
                    .schema(iceberg_schema)
                    .partition_spec(partition_spec.into_unbound())
                    .sort_order(sort_order)
                    .build(),
            )
            .await
            .expect("create spans table")
    }

    /// The write pipeline itself refuses a batch that does not carry the table's columns, whichever
    /// side is wider: the Parquet writer on its own would drop an extra column without a word.
    #[tokio::test]
    async fn write_refuses_a_batch_wider_or_narrower_than_the_table() {
        let table = create_spans_table().await;
        let batch = spans_batch(table.metadata().current_schema(), 4);

        let mut wider_fields = batch.schema().fields().to_vec();
        wider_fields.push(Arc::new(Field::new("environment", DataType::Utf8, true)));
        let mut wider_columns = batch.columns().to_vec();
        wider_columns.push(new_null_array(&DataType::Utf8, batch.num_rows()));
        let wider = RecordBatch::try_new(Arc::new(ArrowSchema::new(wider_fields)), wider_columns).expect("wider batch");
        let narrower = batch
            .project(&(0..batch.num_columns() - 1).collect::<Vec<_>>())
            .expect("narrower batch");

        for (case, batches) in [
            ("wider alone", vec![wider.clone()]),
            ("narrower alone", vec![narrower.clone()]),
            ("wider after a matching batch", vec![batch.clone(), wider]),
            ("narrower after a matching batch", vec![batch, narrower]),
        ] {
            let error = write_record_batches_to_parquet(
                table.clone(),
                WriteConfig {
                    row_group_size: 16,
                    data_page_size_limit_bytes: 2 * 1024 * 1024,
                    max_file_size_bytes: writer_max_parquet_bytes(64 * 1024 * 1024),
                    bloom_filter_columns: &[],
                    column_encodings: &[],
                },
                futures::stream::iter(batches.into_iter().map(Ok)).boxed(),
                &super::CancellationToken::new(),
            )
            .await
            .err();
            assert!(
                matches!(error, Some(crate::error::CommonError::BatchSchemaMismatch(_))),
                "{case}: got {error:?}"
            );
        }
    }

    /// End-to-end encoding regression for the real `spans` schema written
    /// through [`write_record_batches_to_parquet`]. Pins the GH-120 / GH-135
    /// invariants the in-memory shift E2E left unchecked:
    /// - `trace_id` / `span_id` use `DELTA_BYTE_ARRAY`
    /// - `timestamp` uses `DELTA_BINARY_PACKED`
    /// - bloom filters present on `trace_id` / `span_id`
    /// - bloom filters lie after the data of every row group: the writer
    ///   behind [`write_record_batches_to_parquet`] keeps
    ///   `BloomFilterPosition::End` from `build_writer_properties`
    /// - column compression is ZSTD
    #[tokio::test]
    async fn spans_write_pins_encoding_bloom_and_compression() {
        let table = create_spans_table().await;
        let file_io = table.file_io().clone();

        // Build the batch from the table's CURRENT schema: `create_table` may
        // reassign field ids (nested map/list children are renumbered), and the
        // writer pairs the batch with this schema by field id.
        let batch = spans_batch(table.metadata().current_schema(), 64);
        let stream = futures::stream::once(async { Ok(batch) }).boxed();
        let cancel = super::CancellationToken::new();
        // The bloom filter layout check needs more than one row group.
        let cfg = WriteConfig {
            row_group_size: 16,
            data_page_size_limit_bytes: 2 * 1024 * 1024,
            max_file_size_bytes: writer_max_parquet_bytes(64 * 1024 * 1024),
            bloom_filter_columns: SPANS_BLOOM_COLUMNS,
            column_encodings: SPANS_COLUMN_ENCODINGS,
        };

        let written = write_record_batches_to_parquet(table, cfg, stream, &cancel)
            .await
            .expect("write spans parquet");
        assert_eq!(written.rows_written, 64, "all rows written");
        assert_eq!(written.data_files.len(), 1, "single partition ⇒ single file");

        let footer = read_parquet_footer(&file_io, &written.data_files[0]).await;
        assert_eq!(footer.num_row_groups(), 4, "64 rows in row groups of 16");
        crate::parquet_writer::test_support::assert_bloom_filters_after_column_data(&footer);
        let row_group = footer.row_group(0);

        let column = |name: &str| -> &ColumnChunkMetaData {
            row_group
                .columns()
                .iter()
                .find(|c| c.column_path().string() == name)
                .unwrap_or_else(|| panic!("column {name} present in footer"))
        };

        // GH-135: trace_id / span_id encode as DELTA_BYTE_ARRAY, never dictionary.
        for id_col in [COL_TRACE_ID, COL_SPAN_ID] {
            let encodings: Vec<Encoding> = column(id_col).encodings().collect();
            assert!(
                encodings.contains(&Encoding::DELTA_BYTE_ARRAY),
                "{id_col} must use DELTA_BYTE_ARRAY, got {encodings:?}"
            );
            assert!(
                !encodings.contains(&Encoding::RLE_DICTIONARY),
                "{id_col} dictionary leaked despite override: {encodings:?}"
            );
        }

        // GH-120: timestamp encodes as DELTA_BINARY_PACKED.
        let ts_encodings: Vec<Encoding> = column(COL_TIMESTAMP).encodings().collect();
        assert!(
            ts_encodings.contains(&Encoding::DELTA_BINARY_PACKED),
            "timestamp must use DELTA_BINARY_PACKED, got {ts_encodings:?}"
        );

        // Bloom filters present on the high-cardinality id columns.
        assert!(
            column(COL_TRACE_ID).bloom_filter_offset().is_some(),
            "trace_id must carry a bloom filter"
        );
        assert!(
            column(COL_SPAN_ID).bloom_filter_offset().is_some(),
            "span_id must carry a bloom filter"
        );

        // Compression is ZSTD on every column chunk.
        for col in row_group.columns() {
            assert_eq!(
                col.compression(),
                Compression::ZSTD(ZstdLevel::default()),
                "column {} must be ZSTD compressed",
                col.column_path().string()
            );
        }
    }

    /// Read the parquet footer for a written data file back through the table's
    /// `FileIO` (same `OpenDAL` memory backend the writer used), so the test
    /// does not assume a local filesystem path. Mirrors the production
    /// `data_files_from_parquet_paths` reader wiring in ingest.
    async fn read_parquet_footer(file_io: &FileIO, data_file: &DataFile) -> Arc<ParquetMetaData> {
        let input = file_io.new_input(data_file.file_path()).expect("open written parquet");
        let file_metadata = input.metadata().await.expect("stat written parquet");
        let reader = input.reader().await.expect("parquet reader");
        let mut parquet_reader = ArrowFileReader::new(file_metadata, reader);
        parquet_reader.get_metadata(None).await.expect("parquet footer")
    }
}
