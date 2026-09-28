//! DataFusion metrics of one `IcegateIcebergScan` partition.
//!
//! The Iceberg reader counts its bloom filter phase in [`ScanMetrics`]; this
//! module registers the DataFusion counterparts and transfers the reader's
//! counters into them, so the effect shows up in `EXPLAIN ANALYZE`.

use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricType, PruningMetrics, RecordOutput,
};
use futures::Stream;
use iceberg::scan::ScanMetrics;

/// Metrics of one `IcegateIcebergScan` partition.
pub(super) struct IcebergScanMetrics {
    /// `output_rows`, `output_bytes`, `elapsed_compute`.
    baseline: BaselineMetrics,
    /// Row groups the bloom filter phase pruned and matched.
    row_groups_pruned_bloom_filter: PruningMetrics,
    /// Bloom filters the reader could not read, as counted by
    /// [`BloomFilterMetrics::read_errors`](iceberg::arrow::BloomFilterMetrics::read_errors);
    /// its doc states which failures are counted.
    bloom_filter_read_errors: Count,
}

impl IcebergScanMetrics {
    /// Registers the metrics of `partition` in `metrics`.
    pub(super) fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            row_groups_pruned_bloom_filter: MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .pruning_metrics("row_groups_pruned_bloom_filter", partition),
            bloom_filter_read_errors: MetricBuilder::new(metrics)
                .with_type(MetricType::Summary)
                .counter("bloom_filter_read_errors", partition),
        }
    }

    /// Wraps the batch stream of one Iceberg scan so that its batches and the
    /// reader's `scan_metrics` are tracked in these metrics.
    ///
    /// The bloom filter counters are complete once the returned stream has
    /// ended or has been dropped, whichever comes first.
    pub(super) fn track_batch_stream<S>(
        self,
        batches: S,
        scan_metrics: ScanMetrics,
    ) -> impl Stream<Item = DFResult<RecordBatch>> + Send
    where
        S: Stream<Item = DFResult<RecordBatch>> + Unpin + Send,
    {
        IcebergScanMetricsStream {
            batches,
            metrics: self,
            scan_metrics: Some(scan_metrics),
        }
    }
}

/// Batch stream that records output in [`IcebergScanMetrics`].
///
/// The reader's bloom filter counters are transferred once: when the stream
/// ends or when it is dropped, whichever comes first. The reader's stream ends
/// only after every file task has finished, and a dropped stream drops its
/// unfinished file tasks, so the counters no longer grow after the transfer and
/// a consumer that stops polling early still sees every count made so far.
struct IcebergScanMetricsStream<S> {
    batches: S,
    metrics: IcebergScanMetrics,
    /// The reader's counters; `None` once they have been transferred.
    scan_metrics: Option<ScanMetrics>,
}

impl<S> IcebergScanMetricsStream<S> {
    /// Adds the reader's bloom filter counters to the DataFusion metrics on the
    /// first call; later calls do nothing.
    fn transfer_bloom_filter_counts(&mut self) {
        // `take` moves the value out and leaves `None`, so the transfer cannot repeat.
        let Some(scan_metrics) = self.scan_metrics.take() else {
            return;
        };
        let bloom_filter = scan_metrics.bloom_filter();
        let pruning = &self.metrics.row_groups_pruned_bloom_filter;
        pruning.add_pruned(convert_reader_count(bloom_filter.row_groups_pruned()));
        pruning.add_matched(convert_reader_count(bloom_filter.row_groups_matched()));
        self.metrics
            .bloom_filter_read_errors
            .add(convert_reader_count(bloom_filter.read_errors()));
    }
}

impl<S> Stream for IcebergScanMetricsStream<S>
where
    S: Stream<Item = DFResult<RecordBatch>> + Unpin,
{
    type Item = DFResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Every field is `Unpin`, so the pinned stream can be borrowed mutably
        // as a plain `&mut Self`.
        let this = self.get_mut();
        match Pin::new(&mut this.batches).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch.record_output(&this.metrics.baseline)))),
            Poll::Ready(None) => {
                this.transfer_bloom_filter_counts();
                Poll::Ready(None)
            }
            other @ (Poll::Ready(Some(Err(_))) | Poll::Pending) => other,
        }
    }
}

impl<S> Drop for IcebergScanMetricsStream<S> {
    fn drop(&mut self) {
        self.transfer_bloom_filter_counts();
    }
}

/// The reader's counter as a DataFusion count, saturating at `usize::MAX` on
/// targets where `usize` is narrower than `u64`.
fn convert_reader_count(reader_count: u64) -> usize {
    usize::try_from(reader_count).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use datafusion::arrow::array::{FixedSizeBinaryArray, RecordBatch};
    use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
    use datafusion::error::{DataFusionError, Result as DFResult};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue, PruningMetrics};
    use futures::{StreamExt, TryStreamExt};
    use iceberg::Runtime;
    use iceberg::arrow::{ArrowReaderBuilder, ScanResult, schema_to_arrow_schema};
    use iceberg::expr::{Bind, Predicate, Reference};
    use iceberg::io::FileIO;
    use iceberg::scan::{FileScanTask, ScanMetrics};
    use iceberg::spec::{DataFileFormat, Datum, Schema};
    use icegate_common::parquet_encoding::{LOGS_BLOOM_COLUMNS, LOGS_COLUMN_ENCODINGS};
    use icegate_common::parquet_writer::build_writer_properties;
    use icegate_common::schema::{COL_SPAN_ID, COL_TRACE_ID, logs_schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::ParquetMetaDataReader;
    use parquet::file::properties::DEFAULT_PAGE_SIZE;

    use super::IcebergScanMetrics;

    /// Upper bound on reading the in-memory fixture file.
    const READ_TIMEOUT: Duration = Duration::from_secs(10);

    const TRACE_ID_FILE_PATH: &str = "memory:///bloom/trace_id.parquet";

    /// Rows per row group: the six fixture rows land in three row groups.
    const ROWS_PER_ROW_GROUP: usize = 2;

    /// Trace id `Tk`: 16 bytes `[k, 0, …]`.
    const fn build_trace_id(k: u8) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[0] = k;
        id
    }

    /// The canonical `logs` schema projected by name onto `column`.
    fn project_logs_arrow_schema(schema: &Schema, column: &str) -> ArrowSchemaRef {
        let arrow_schema = schema_to_arrow_schema(schema).unwrap();
        let index = arrow_schema.index_of(column).unwrap();
        Arc::new(arrow_schema.project(&[index]).unwrap())
    }

    /// A `span_id` batch of `row_count` rows.
    fn build_span_id_batch(row_count: u8) -> RecordBatch {
        let arrow_schema = project_logs_arrow_schema(&logs_schema().unwrap(), COL_SPAN_ID);
        let span_ids = FixedSizeBinaryArray::try_from_iter((0..row_count).map(|k| [k, 0, 0, 0, 0, 0, 0, 0])).unwrap();
        RecordBatch::try_new(arrow_schema, vec![Arc::new(span_ids)]).unwrap()
    }

    /// Reader counters of a scan that reads no file, so they stay zero.
    /// `ScanMetrics` has no public constructor; only `ArrowReader::read` makes one.
    fn create_empty_reader_counters() -> ScanMetrics {
        ArrowReaderBuilder::new(FileIO::new_with_memory(), Runtime::current())
            .build()
            .read(Box::pin(futures::stream::empty()))
            .unwrap()
            .metrics()
            .clone()
    }

    /// The `row_groups_pruned_bloom_filter` metric registered in `metrics`.
    fn find_row_groups_pruned_bloom_filter(metrics: &ExecutionPlanMetricsSet) -> PruningMetrics {
        metrics
            .clone_inner()
            .iter()
            .find_map(|metric| match metric.value() {
                MetricValue::PruningMetrics { name, pruning_metrics } if name == "row_groups_pruned_bloom_filter" => {
                    Some(pruning_metrics.clone())
                }
                _ => None,
            })
            .expect("row_groups_pruned_bloom_filter is registered")
    }

    /// The value of the `bloom_filter_read_errors` metric registered in `metrics`.
    fn find_bloom_filter_read_errors(metrics: &ExecutionPlanMetricsSet) -> usize {
        metrics
            .clone_inner()
            .iter()
            .find_map(|metric| match metric.value() {
                MetricValue::Count { name, count } if name == "bloom_filter_read_errors" => Some(count.value()),
                _ => None,
            })
            .expect("bloom_filter_read_errors is registered")
    }

    /// A `trace_id` Parquet file in memory storage, written with the production
    /// writer properties of `logs`.
    struct TraceIdFile {
        file_io: FileIO,
        logs_schema: Arc<Schema>,
        file_size: u64,
    }

    impl TraceIdFile {
        /// Reads the file with bloom filter pruning enabled, as `IcegateIcebergScan` does.
        fn scan_with_bloom_filter(&self, predicate: &Predicate) -> ScanResult {
            let trace_id_field_id = self.logs_schema.field_id_by_name(COL_TRACE_ID).unwrap();
            let task = FileScanTask::builder()
                .with_file_size_in_bytes(self.file_size)
                .with_start(0)
                .with_length(0)
                .with_data_file_path(TRACE_ID_FILE_PATH.to_owned())
                .with_data_file_format(DataFileFormat::Parquet)
                .with_schema(Arc::clone(&self.logs_schema))
                .with_project_field_ids(vec![trace_id_field_id])
                .with_predicate(Some(predicate.bind(Arc::clone(&self.logs_schema), true).unwrap()))
                .with_case_sensitive(true)
                .build();
            ArrowReaderBuilder::new(self.file_io.clone(), Runtime::current())
                .with_bloom_filter_enabled(true)
                .build()
                .read(Box::pin(futures::stream::iter([Ok(task)])))
                .unwrap()
        }
    }

    /// Writes row groups `(T1,T9) (T2,T8) (T3,T7)`, each with a `trace_id`
    /// bloom filter. With `should_corrupt_first_bloom_filter`, the bloom filter
    /// of row group 0 is zeroed, which no longer parses as a bloom filter.
    async fn write_trace_id_file(should_corrupt_first_bloom_filter: bool) -> TraceIdFile {
        let logs_schema = Arc::new(logs_schema().unwrap());
        let arrow_schema = project_logs_arrow_schema(&logs_schema, COL_TRACE_ID);
        let trace_ids =
            FixedSizeBinaryArray::try_from_iter([1u8, 9, 2, 8, 3, 7].into_iter().map(build_trace_id)).unwrap();
        let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), vec![Arc::new(trace_ids)]).unwrap();
        let writer_properties = build_writer_properties(
            ROWS_PER_ROW_GROUP,
            DEFAULT_PAGE_SIZE,
            LOGS_BLOOM_COLUMNS,
            LOGS_COLUMN_ENCODINGS,
        );
        let mut writer = ArrowWriter::try_new(Vec::new(), arrow_schema, Some(writer_properties)).unwrap();
        writer.write(&batch).unwrap();
        let contents = Bytes::from(writer.into_inner().unwrap());

        let metadata = ParquetMetaDataReader::new().parse_and_finish(&contents).unwrap();
        assert_eq!(metadata.num_row_groups(), 3);
        let trace_id_column = metadata
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .position(|column| column.name() == COL_TRACE_ID)
            .unwrap();
        assert!(
            metadata
                .row_groups()
                .iter()
                .all(|row_group| row_group.column(trace_id_column).bloom_filter_offset().is_some()),
            "every row group carries a trace_id bloom filter"
        );

        let contents = if should_corrupt_first_bloom_filter {
            let chunk = metadata.row_group(0).column(trace_id_column);
            let offset = usize::try_from(chunk.bloom_filter_offset().unwrap()).unwrap();
            let length = usize::try_from(chunk.bloom_filter_length().unwrap()).unwrap();
            let mut raw = Vec::from(contents);
            raw[offset..offset + length].fill(0);
            Bytes::from(raw)
        } else {
            contents
        };

        let file_io = FileIO::new_with_memory();
        let file_size = u64::try_from(contents.len()).unwrap();
        file_io.new_output(TRACE_ID_FILE_PATH).unwrap().write(contents).await.unwrap();
        TraceIdFile {
            file_io,
            logs_schema,
            file_size,
        }
    }

    #[tokio::test]
    async fn batches_of_the_reader_are_counted_in_output_rows() {
        let metrics = ExecutionPlanMetricsSet::new();
        let batches = futures::stream::iter(vec![Ok(build_span_id_batch(2)), Ok(build_span_id_batch(3))]);

        let tracked: Vec<RecordBatch> = IcebergScanMetrics::new(&metrics, 0)
            .track_batch_stream(batches, create_empty_reader_counters())
            .try_collect()
            .await
            .unwrap();

        let row_counts: Vec<usize> = tracked.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(row_counts, vec![2, 3]);
        assert_eq!(metrics.clone_inner().output_rows(), Some(5));
    }

    #[tokio::test]
    async fn a_reader_error_is_passed_to_the_consumer_unchanged() {
        let metrics = ExecutionPlanMetricsSet::new();
        let batches = futures::stream::iter(vec![
            Ok(build_span_id_batch(2)),
            Err(DataFusionError::Execution("injected read failure".to_owned())),
        ]);

        let items: Vec<DFResult<RecordBatch>> = IcebergScanMetrics::new(&metrics, 0)
            .track_batch_stream(batches, create_empty_reader_counters())
            .collect()
            .await;

        match items.as_slice() {
            [Ok(batch), Err(DataFusionError::Execution(_))] => assert_eq!(batch.num_rows(), 2),
            other => panic!("expected a batch and then the reader error, got {other:?}"),
        }
        assert_eq!(metrics.clone_inner().output_rows(), Some(2));
    }

    /// A consumer that stops polling early (`LIMIT`, a cancelled query) drops
    /// the stream before it ends, so the counts must be transferred on drop.
    #[tokio::test]
    async fn dropping_the_stream_before_its_end_transfers_the_bloom_filter_counts() {
        let file = write_trace_id_file(false).await;
        // Every row group's min/max range holds `T1` or `T3`, so all three reach
        // the bloom filter phase; only `(T2,T8)` holds neither.
        let scan = file.scan_with_bloom_filter(
            &Reference::new(COL_TRACE_ID).is_in([Datum::fixed(build_trace_id(1)), Datum::fixed(build_trace_id(3))]),
        );
        let reader_counters = scan.metrics().clone();
        let metrics = ExecutionPlanMetricsSet::new();
        let mut stream = IcebergScanMetrics::new(&metrics, 0).track_batch_stream(
            scan.stream().map_err(|error| DataFusionError::External(error.into())),
            reader_counters.clone(),
        );

        let first = tokio::time::timeout(READ_TIMEOUT, stream.next()).await.expect("scan timed out");
        assert!(matches!(first, Some(Ok(_))), "expected a first batch, got {first:?}");
        // The bloom filter phase of the only file runs before its first batch.
        let bloom_filter = reader_counters.bloom_filter();
        assert_eq!(
            (bloom_filter.row_groups_pruned(), bloom_filter.row_groups_matched()),
            (1, 2)
        );
        let pruning = find_row_groups_pruned_bloom_filter(&metrics);
        assert_eq!(
            (pruning.pruned(), pruning.matched()),
            (0, 0),
            "the stream has not ended, so nothing is transferred yet"
        );

        drop(stream);

        let pruning = find_row_groups_pruned_bloom_filter(&metrics);
        assert_eq!((pruning.pruned(), pruning.matched()), (1, 2));
    }

    #[tokio::test]
    async fn an_unreadable_bloom_filter_is_counted_in_bloom_filter_read_errors() {
        let file = write_trace_id_file(true).await;
        // Row group 0 is kept for its unreadable bloom filter, `(T2,T8)` and
        // `(T3,T7)` for holding the values, so every reader counter differs.
        let scan = file.scan_with_bloom_filter(
            &Reference::new(COL_TRACE_ID).is_in([Datum::fixed(build_trace_id(2)), Datum::fixed(build_trace_id(3))]),
        );
        let reader_counters = scan.metrics().clone();
        let metrics = ExecutionPlanMetricsSet::new();
        let stream = IcebergScanMetrics::new(&metrics, 0).track_batch_stream(
            scan.stream().map_err(|error| DataFusionError::External(error.into())),
            reader_counters.clone(),
        );

        tokio::time::timeout(READ_TIMEOUT, stream.try_collect::<Vec<RecordBatch>>())
            .await
            .expect("scan timed out")
            .unwrap();

        let bloom_filter = reader_counters.bloom_filter();
        assert_eq!(
            (
                bloom_filter.row_groups_pruned(),
                bloom_filter.row_groups_matched(),
                bloom_filter.read_errors()
            ),
            (0, 3, 1)
        );
        // 1 differs from both `row_groups_pruned` and `row_groups_matched`, so a
        // transfer from either of them would show 0 or 3 here.
        assert_eq!(find_bloom_filter_read_errors(&metrics), 1);
    }
}
