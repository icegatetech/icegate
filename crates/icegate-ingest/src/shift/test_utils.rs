//! Fixtures shared by the shift test modules: an in-memory WAL, the two storage doubles the
//! pipeline is driven against, and the parquet oracles used to inspect what was written.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::{
    array::{Array, ArrayRef, MapArray, StringArray, StructArray, TimestampMicrosecondArray, new_null_array},
    buffer::OffsetBuffer,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use iceberg::{
    Catalog, NamespaceIdent, TableCreation,
    io::FileIO,
    spec::{DataContentType, DataFile, DataFileBuilder, DataFileFormat, Struct},
    table::Table,
};
use icegate_common::{
    ICEGATE_NAMESPACE,
    catalog::{CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle},
    schema::{
        COL_BODY, COL_INGESTED_TIMESTAMP, COL_LOG_ATTRIBUTES, COL_OBSERVED_TIMESTAMP, COL_RESOURCE_ATTRIBUTES,
        COL_SCOPE_ATTRIBUTES, COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_SPAN_ID, COL_TENANT_ID, COL_TIMESTAMP,
        COL_TRACE_ID, logs_partition_spec, logs_schema, logs_sort_order,
    },
};
use icegate_queue::{PreparedWalRowGroup, RowGroupPlanEntry, SegmentsPlan};
use parquet::{
    arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    file::{
        reader::{FileReader, SerializedFileReader},
        statistics::Statistics,
    },
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    iceberg_storage::{BoxRecordBatchStream, IcebergStorage, Storage, WrittenDataFiles},
    plan_runner::{PLAN_FIELD_BOUNDARY_RANGE, PLAN_FIELD_TENANT_ID, PLAN_FIELD_TIMESTAMP_RANGE},
    timeout::TimeoutEstimator,
};
use crate::error::Result;

/// One WAL segment as the queue would hand it out: its offset and the row groups written into it.
#[derive(Clone)]
pub(super) struct WalSegment {
    /// Offset the segment occupies in the topic.
    pub(super) offset: u64,
    /// Row groups of the segment, in write order.
    pub(super) row_groups: Vec<PreparedWalRowGroup>,
}

/// Queue reader over an in-memory WAL: plans the segments at or above the requested offset and
/// serves the row groups behind them.
pub(super) struct WalQueueReader {
    segments: Vec<WalSegment>,
}

impl WalQueueReader {
    /// Reader over `segments`, planning exactly the row groups they contain.
    pub(super) fn new(segments: &[WalSegment]) -> Self {
        Self {
            segments: segments.to_vec(),
        }
    }

    /// Row groups of the segment at `offset`, empty when the topic holds no such segment.
    fn row_groups_at(&self, offset: u64) -> Vec<PreparedWalRowGroup> {
        self.segments
            .iter()
            .find(|segment| segment.offset == offset)
            .map(|segment| segment.row_groups.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl icegate_queue::QueueReader for WalQueueReader {
    /// Segments below `start_offset` are already committed and the queue does not list them again.
    /// Honouring the offset is what makes a second iteration plan only what the first one left.
    async fn plan_segments(
        &self,
        _topic: &icegate_queue::Topic,
        start_offset: u64,
        _fields: &[icegate_queue::ExtractField],
        _cancel_token: &CancellationToken,
    ) -> icegate_queue::Result<SegmentsPlan> {
        let pending_segments = self
            .segments
            .iter()
            .filter(|segment| segment.offset >= start_offset)
            .cloned()
            .collect::<Vec<_>>();
        Ok(build_segments_plan(&pending_segments))
    }

    async fn read_segment(
        &self,
        _topic: &icegate_queue::Topic,
        offset: u64,
        record_batch_idxs: &[usize],
        _cancel_token: &CancellationToken,
    ) -> icegate_queue::Result<icegate_queue::RecordBatchStream> {
        let row_groups = self.row_groups_at(offset);
        let requested = record_batch_idxs.to_vec();
        let batches = row_groups
            .into_iter()
            .enumerate()
            .filter_map(|(idx, row_group)| requested.contains(&idx).then_some(row_group.batch))
            .map(Ok)
            .collect::<Vec<_>>();
        Ok(Box::pin(futures::stream::iter(batches)))
    }
}

/// What the pipeline asked storage to do, in the order the calls arrived.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum StorageCall {
    /// A shift task wrote the parquet files at these paths.
    Write(Vec<String>),
    /// The commit task committed these paths under `last_offset`.
    Commit {
        /// Paths the commit task collected from the shift tasks it depends on.
        parquet_files: Vec<String>,
        /// Highest WAL offset the snapshot claims.
        last_offset: u64,
    },
}

/// The one call [`RecordingStorage`] fails, and how it fails it.
///
/// One fault per fixture: every case the pipeline tests drive exercises a single failing step,
/// and the storage double is shared by all three task runners.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum StorageFault {
    /// Every call succeeds.
    #[default]
    None,
    /// Every write fails, so every shift task issuing one fails too.
    Write,
    /// Writes carrying this tenant fail; writes of the other tenants succeed, which is what a
    /// partially failed fan-out looks like from storage.
    WriteForTenant(&'static str),
    /// Every committed-offset read after the plan task's own read fails - that read is the one the
    /// commit task performs.
    CommitLastOffsetRead,
    /// Building data files from the parquet paths fails.
    DataFilesRead,
    /// Every commit fails without recording an offset, which is what a snapshot conflict does.
    Commit,
    /// The first commit records its offset and then fails, which is what a commit whose response
    /// was lost looks like to the attempt that follows it.
    CommitResponseLostOnce,
}

/// Storage double that records what the pipeline wrote and committed without touching Iceberg.
///
/// `get_last_offset` answers with the offset of the last recorded commit, so a second iteration
/// sees exactly what the first one committed and the already-committed path is reachable.
#[derive(Default)]
pub(super) struct RecordingStorage {
    calls: Mutex<Vec<StorageCall>>,
    written_batches: Mutex<Vec<Vec<RecordBatch>>>,
    committed_offset: Mutex<Option<u64>>,
    last_offset_reads: AtomicUsize,
    commit_calls: AtomicUsize,
    fault: StorageFault,
}

impl RecordingStorage {
    /// Storage that accepts every call.
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Storage that fails the call `fault` names.
    pub(super) fn with_fault(fault: StorageFault) -> Self {
        Self {
            fault,
            ..Self::default()
        }
    }

    /// Calls the pipeline made, in arrival order.
    pub(super) async fn calls(&self) -> Vec<StorageCall> {
        self.calls.lock().await.clone()
    }

    /// Record batches handed to each write call, in arrival order.
    pub(super) async fn written_batches(&self) -> Vec<Vec<RecordBatch>> {
        self.written_batches.lock().await.clone()
    }

    /// The commit calls the pipeline made, in arrival order.
    pub(super) async fn commits(&self) -> Vec<StorageCall> {
        self.calls()
            .await
            .into_iter()
            .filter(|call| matches!(call, StorageCall::Commit { .. }))
            .collect()
    }

    /// Offset the last landed commit recorded, read past a configured read fault: what the table
    /// would carry, as opposed to what the pipeline is able to read back.
    pub(super) async fn committed_offset(&self) -> Option<u64> {
        *self.committed_offset.lock().await
    }
}

#[async_trait]
impl Storage for RecordingStorage {
    async fn get_last_offset(&self, _cancel_token: &CancellationToken) -> Result<Option<u64>> {
        let read_count = self.last_offset_reads.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fault == StorageFault::CommitLastOffsetRead && read_count > 1 {
            return Err(crate::error::IngestError::Shift(
                "committed offset read failure".to_string(),
            ));
        }
        Ok(*self.committed_offset.lock().await)
    }

    async fn write_record_batches(
        &self,
        batches: BoxRecordBatchStream,
        _cancel_token: &CancellationToken,
    ) -> Result<WrittenDataFiles> {
        use futures::TryStreamExt;

        let batches = batches.try_collect::<Vec<_>>().await?;
        let rows_written = batches.iter().map(RecordBatch::num_rows).sum();
        let fails_write = match self.fault {
            StorageFault::Write => true,
            StorageFault::WriteForTenant(tenant) => {
                tenant_ids_from_batches(&batches).iter().any(|tenant_id| tenant_id == tenant)
            }
            _ => false,
        };
        if fails_write {
            return Err(crate::error::IngestError::Shift("storage write failure".to_string()));
        }

        // One file per write call, named after the call's position: the commit task must receive
        // exactly the paths its own dependencies produced, so the paths have to differ per task.
        let mut written_batches = self.written_batches.lock().await;
        let path = format!("s3://warehouse/logs/part-{:05}.parquet", written_batches.len());
        written_batches.push(batches);
        drop(written_batches);
        self.calls.lock().await.push(StorageCall::Write(vec![path.clone()]));

        Ok(WrittenDataFiles {
            data_files: vec![data_file(&path, rows_written as u64)],
            rows_written,
        })
    }

    async fn get_data_files(
        &self,
        parquet_paths: &[String],
        _cancel_token: &CancellationToken,
    ) -> Result<Vec<DataFile>> {
        if self.fault == StorageFault::DataFilesRead {
            return Err(crate::error::IngestError::Shift("data files read failure".to_string()));
        }
        Ok(parquet_paths.iter().map(|path| data_file(path, 1)).collect())
    }

    async fn commit(
        &self,
        data_files: Vec<DataFile>,
        _record_type: &str,
        last_offset: u64,
        _cancel_token: &CancellationToken,
    ) -> Result<usize> {
        let commit_count = self.commit_calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fault == StorageFault::Commit {
            return Err(crate::error::IngestError::Shift("commit failure".to_string()));
        }

        let parquet_files = data_files.iter().map(|file| file.file_path().to_string()).collect();
        self.calls.lock().await.push(StorageCall::Commit {
            parquet_files,
            last_offset,
        });
        *self.committed_offset.lock().await = Some(last_offset);
        if self.fault == StorageFault::CommitResponseLostOnce && commit_count == 1 {
            return Err(crate::error::IngestError::Shift("commit response lost".to_string()));
        }
        Ok(data_files.len())
    }
}

/// Storage over a real [`IcebergStorage`], remembering the data files it wrote.
///
/// Used where the assertion is about the parquet actually produced rather than about the
/// orchestration around it.
pub(super) struct IcebergStorageWithHistory {
    inner: IcebergStorage,
    written_data_files: Mutex<Vec<DataFile>>,
}

impl IcebergStorageWithHistory {
    /// Wrap `inner`, recording every data file it writes.
    pub(super) fn new(inner: IcebergStorage) -> Self {
        Self {
            inner,
            written_data_files: Mutex::new(Vec::new()),
        }
    }

    /// Data files written so far, in write order.
    pub(super) async fn written_data_files(&self) -> Vec<DataFile> {
        self.written_data_files.lock().await.clone()
    }
}

#[async_trait]
impl Storage for IcebergStorageWithHistory {
    async fn get_last_offset(&self, cancel_token: &CancellationToken) -> Result<Option<u64>> {
        self.inner.get_last_offset(cancel_token).await
    }

    async fn write_record_batches(
        &self,
        batches: BoxRecordBatchStream,
        cancel_token: &CancellationToken,
    ) -> Result<WrittenDataFiles> {
        let written = self.inner.write_record_batches(batches, cancel_token).await?;
        self.written_data_files.lock().await.extend(written.data_files.iter().cloned());
        Ok(written)
    }

    async fn get_data_files(
        &self,
        parquet_paths: &[String],
        cancel_token: &CancellationToken,
    ) -> Result<Vec<DataFile>> {
        self.inner.get_data_files(parquet_paths, cancel_token).await
    }

    async fn commit(
        &self,
        data_files: Vec<DataFile>,
        record_type: &str,
        last_offset: u64,
        cancel_token: &CancellationToken,
    ) -> Result<usize> {
        self.inner.commit(data_files, record_type, last_offset, cancel_token).await
    }
}

/// Data file describing `rows` rows at `path`, with a fixed size so byte assertions stay stable.
pub(super) fn data_file(path: &str, rows: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(path.to_string())
        .file_format(DataFileFormat::Parquet)
        .file_size_in_bytes(128)
        .record_count(rows)
        .partition_spec_id(0)
        .partition(Struct::empty())
        .build()
        .expect("data file")
}

/// The plan the queue would return for `segments`, carrying the fields the planner extracts.
fn build_segments_plan(segments: &[WalSegment]) -> SegmentsPlan {
    let mut entries: Vec<RowGroupPlanEntry> = Vec::new();
    for segment in segments {
        for (row_group_idx, row_group) in segment.row_groups.iter().enumerate() {
            let tenant_ids = row_group
                .batch
                .column_by_name(COL_TENANT_ID)
                .expect("tenant_id column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("tenant_id string");
            let tenant_id = tenant_ids.value(0).to_string();
            let mut extracted = HashMap::new();
            extracted.insert(
                PLAN_FIELD_TENANT_ID.to_string(),
                icegate_queue::ExtractedValue::Utf8(tenant_id),
            );
            if let Some(payload) = row_group.metadata.clone() {
                extracted.insert(
                    PLAN_FIELD_BOUNDARY_RANGE.to_string(),
                    icegate_queue::ExtractedValue::Utf8(payload),
                );
            }
            // Extract physical timestamp min/max from the batch timestamp column. A
            // TimestampMicrosRange is required because CURRENT_PLANNER_PARTITION_SPEC has
            // required=true for the day field.
            let ts_col = row_group
                .batch
                .column_by_name(COL_TIMESTAMP)
                .expect("timestamp column")
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp micros");
            let mut valid_ts = (0..ts_col.len()).filter(|&i| ts_col.is_valid(i)).map(|i| ts_col.value(i));
            if let Some(first) = valid_ts.next() {
                let (min_ts, max_ts) = valid_ts.fold((first, first), |(mn, mx), v| (mn.min(v), mx.max(v)));
                extracted.insert(
                    PLAN_FIELD_TIMESTAMP_RANGE.to_string(),
                    icegate_queue::ExtractedValue::TimestampMicrosRange(min_ts, max_ts),
                );
            }
            entries.push(RowGroupPlanEntry {
                wal_offset: segment.offset,
                row_group_idx,
                row_group_bytes: 1,
                extracted,
            });
        }
    }

    let row_groups_total = entries.len();
    SegmentsPlan {
        entries,
        last_segment_offset: segments.iter().map(|segment| segment.offset).max(),
        segments_count: segments.len(),
        row_groups_total,
        input_bytes_total: row_groups_total as u64,
    }
}

/// A logs batch on the canonical schema: `(tenant_id, service_name, timestamp, row tag)` per row,
/// built against an explicit Arrow schema.
///
/// The row tag lands in `body` as `msg-<tag>`, which is what pins a row's identity through the
/// pipeline - the canonical schema carries no synthetic row id, and `body` is the only field the
/// sort key ignores. Every other column is filled with a fixed value: the cases here are about
/// partitioning, ordering, and what reaches Iceberg, not about the payload.
///
/// `schema` matters: a freshly derived [`crate::transform::logs_arrow_schema`] is only
/// self-consistent in isolation. `create_logs_table` (this module) builds its catalog with
/// `CatalogBackend::Memory` — the plain upstream `MemoryCatalogBuilder` — which reassigns
/// nested field ids (map children in particular) on `create_table`, so a batch destined for
/// the real writer must carry the created table's own current schema, not a freshly derived
/// one; see `icegate_common::iceberg_write`'s `spans_batch` helper for the identical
/// requirement on the spans side. This is specific to the `Memory` backend: the production
/// `S3Catalog` preserves the caller's field ids verbatim instead (`IcebergTableMetadata::create`
/// in `icegate-catalog-s3/src/domain/root.rs`, pinned by its `create_preserves_caller_field_ids`
/// regression test). Fixtures that never reach a real Iceberg writer (e.g. sorter-only tests)
/// may pass a fresh [`crate::transform::logs_arrow_schema`]; one that writes through
/// `create_logs_table` must pass `schema_to_arrow_schema(table.metadata().current_schema())`.
#[allow(clippy::needless_pass_by_value)]
pub(super) fn logs_ingest_batch_with_schema(schema: &Schema, rows: Vec<(&str, Option<&str>, i64, i64)>) -> RecordBatch {
    let schema = Arc::new(schema.clone());
    let row_count = rows.len();
    let timestamps = rows.iter().map(|(_, _, timestamp, _)| *timestamp).collect::<Vec<_>>();
    let timestamps: ArrayRef = Arc::new(TimestampMicrosecondArray::from(timestamps));

    // Columns are looked up by name and then laid out in the schema's own order: the canonical
    // schema owns which columns exist and where, and a fixture positioned by hand would break
    // silently the next time a field is added to it.
    let columns_by_name: HashMap<&str, ArrayRef> = HashMap::from([
        (
            COL_TENANT_ID,
            Arc::new(StringArray::from(
                rows.iter().map(|(tenant_id, _, _, _)| *tenant_id).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            COL_SERVICE_NAME,
            Arc::new(StringArray::from(
                rows.iter().map(|(_, service_name, _, _)| *service_name).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (COL_TIMESTAMP, Arc::clone(&timestamps)),
        (COL_OBSERVED_TIMESTAMP, Arc::clone(&timestamps)),
        (COL_INGESTED_TIMESTAMP, timestamps),
        (COL_TRACE_ID, new_null_array(&DataType::FixedSizeBinary(16), row_count)),
        (COL_SPAN_ID, new_null_array(&DataType::FixedSizeBinary(8), row_count)),
        (
            COL_SEVERITY_TEXT,
            Arc::new(StringArray::from(vec![Some("INFO"); row_count])) as ArrayRef,
        ),
        (
            COL_BODY,
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|(_, _, _, row_tag)| Some(row_tag_body(*row_tag)))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            COL_RESOURCE_ATTRIBUTES,
            empty_attributes_column(&schema, COL_RESOURCE_ATTRIBUTES, row_count),
        ),
        (
            COL_SCOPE_ATTRIBUTES,
            empty_attributes_column(&schema, COL_SCOPE_ATTRIBUTES, row_count),
        ),
        (
            COL_LOG_ATTRIBUTES,
            empty_attributes_column(&schema, COL_LOG_ATTRIBUTES, row_count),
        ),
    ]);
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            Arc::clone(
                columns_by_name
                    .get(field.name().as_str())
                    .unwrap_or_else(|| panic!("fixture must fill the '{}' column", field.name())),
            )
        })
        .collect::<Vec<_>>();

    RecordBatch::try_new(schema, columns).expect("logs ingest batch")
}

/// The `body` value carrying `row_tag`, which is how a fixture row is recognised downstream.
pub(super) fn row_tag_body(row_tag: i64) -> String {
    format!("msg-{row_tag}")
}

/// Two ingest batches over two tenants, in the order they were received - unsorted, interleaved,
/// and with a tie on the full sort key inside each tenant. One batch per WAL segment.
///
/// Built against a freshly derived schema; see [`logs_ingest_batch_with_schema`] for when that is
/// (and is not) safe. A test that writes through the real catalog must use
/// [`two_tenant_ingest_batches_with_schema`] instead.
pub(super) fn two_tenant_ingest_batches() -> [RecordBatch; 2] {
    two_tenant_ingest_batches_with_schema(&crate::transform::logs_arrow_schema().expect("logs arrow schema"))
}

/// As [`two_tenant_ingest_batches`], but built against an explicit Arrow schema — see
/// [`logs_ingest_batch_with_schema`].
pub(super) fn two_tenant_ingest_batches_with_schema(schema: &Schema) -> [RecordBatch; 2] {
    [
        logs_ingest_batch_with_schema(
            schema,
            vec![
                ("tenant-b", Some("svc-z"), 10, 900),
                ("tenant-a", Some("svc-1"), 100, 101),
                ("tenant-a", Some("svc-1"), 100, 102),
                ("tenant-b", Some("svc-y"), 20, 901),
                ("tenant-a", Some("svc-0"), 110, 103),
                ("tenant-a", Some("svc-a"), 50, 104),
            ],
        ),
        logs_ingest_batch_with_schema(
            schema,
            vec![
                ("tenant-a", Some("svc-1"), 100, 201),
                ("tenant-b", Some("svc-x"), 30, 902),
                ("tenant-a", Some("svc-2"), 90, 202),
                ("tenant-a", Some("svc-1"), 100, 203),
                ("tenant-b", Some("svc-a"), 70, 903),
            ],
        ),
    ]
}

/// Row bodies the shifted output must carry per tenant of [`two_tenant_ingest_batches`], in
/// physical order: the logs sort key (`service_name` ascending, `timestamp` descending), with WAL
/// arrival order breaking the ties. Written out rather than derived, so the expectation does not
/// go through the merge under test.
pub(super) fn expected_row_bodies(tenant: &str) -> Vec<String> {
    let row_tags: &[i64] = match tenant {
        "tenant-a" => &[103, 101, 102, 201, 203, 202, 104],
        "tenant-b" => &[903, 902, 901, 900],
        other => panic!("no expected rows for tenant '{other}'"),
    };
    row_tags.iter().copied().map(row_tag_body).collect()
}

/// An empty `MAP<Utf8,Utf8>` column of length `row_count`, typed exactly like the named
/// attribute-map field of `schema`, so it satisfies that required map of the canonical logs schema.
fn empty_attributes_column(schema: &Schema, column: &str, row_count: usize) -> ArrayRef {
    let attributes = schema.field_with_name(column).unwrap_or_else(|_| panic!("{column} field"));
    let DataType::Map(entry_field, ordered) = attributes.data_type() else {
        panic!("{column} must be a Map");
    };
    let DataType::Struct(entry_fields) = entry_field.data_type() else {
        panic!("map entry must be a Struct");
    };
    let keys: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    let values: ArrayRef = Arc::new(StringArray::from(Vec::<&str>::new()));
    let entries = StructArray::new(entry_fields.clone(), vec![keys, values], None);
    let offsets = OffsetBuffer::new(vec![0_i32; row_count + 1].into());
    Arc::new(MapArray::new(entry_field.clone(), offsets, entries, None, *ordered))
}

/// Row bodies of every row in `batches`, in physical order.
pub(super) fn row_bodies_from_batches(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let bodies = batch
                .column_by_name(COL_BODY)
                .expect("body column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("body string");
            (0..batch.num_rows())
                .map(|row_idx| bodies.value(row_idx).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Tenant ids of every row in `batches`, in physical order.
pub(super) fn tenant_ids_from_batches(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let tenant_ids = batch
                .column_by_name(COL_TENANT_ID)
                .expect("tenant_id column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("tenant_id string");
            (0..batch.num_rows())
                .map(|row_idx| tenant_ids.value(row_idx).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Create a memory-backed logs table matching the fixture batches, and the catalog holding it.
pub(crate) async fn create_logs_table(table_name: &str) -> (Arc<dyn Catalog>, Table) {
    let catalog_config = CatalogConfig {
        backend: CatalogBackend::Memory,
        warehouse: format!("memory://shift-pipeline-{}", Uuid::new_v4()),
        properties: HashMap::new(),
        cache: None,
    };
    let catalog = CatalogBuilder::from_config(&catalog_config, &IoHandle::noop(), CancellationToken::new())
        .await
        .expect("memory catalog");
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");

    let schema = logs_schema().expect("logs table schema");
    let partition_spec = logs_partition_spec(&schema).expect("logs partition spec");
    let sort_order = logs_sort_order(&schema).expect("logs sort order");
    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name(table_name.to_string())
                .schema(schema)
                .partition_spec(partition_spec)
                .sort_order(sort_order)
                .build(),
        )
        .await
        .expect("create logs table");
    (catalog, table)
}

/// One row of the fixture logs table, as read back from parquet.
#[derive(Debug, Eq, PartialEq)]
pub(super) struct LogOutputRow {
    /// Partition value of the row.
    pub(super) tenant_id: String,
    /// Leading sort-key component.
    pub(super) service_name: Option<String>,
    /// Trailing sort-key component, physically descending.
    pub(super) timestamp: i64,
    /// Body of the ingested row, which carries its fixture tag and so pins WAL-stable order for
    /// equal sort keys.
    pub(super) body: Option<String>,
}

fn nullable_string_value(array: &StringArray, row_idx: usize) -> Option<String> {
    (!array.is_null(row_idx)).then(|| array.value(row_idx).to_string())
}

/// Decode `batches` of the fixture logs table into rows, addressing columns by name.
pub(super) fn rows_from_record_batches(batches: &[RecordBatch]) -> Vec<LogOutputRow> {
    batches
        .iter()
        .flat_map(|batch| {
            let tenant_ids = batch
                .column_by_name(COL_TENANT_ID)
                .expect("tenant_id column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("tenant_id string");
            let service_names = batch
                .column_by_name(COL_SERVICE_NAME)
                .expect("service_name column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("service_name string");
            let timestamps = batch
                .column_by_name(COL_TIMESTAMP)
                .expect("timestamp column")
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp micros");
            let bodies = batch
                .column_by_name(COL_BODY)
                .expect("body column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("body string");

            (0..batch.num_rows())
                .map(|row_idx| LogOutputRow {
                    tenant_id: tenant_ids.value(row_idx).to_string(),
                    service_name: nullable_string_value(service_names, row_idx),
                    timestamp: timestamps.value(row_idx),
                    body: nullable_string_value(bodies, row_idx),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Order the logs sort key defines: service name ascending, timestamp descending.
pub(super) fn log_sort_key_cmp(left: &LogOutputRow, right: &LogOutputRow) -> std::cmp::Ordering {
    left.service_name
        .cmp(&right.service_name)
        .then_with(|| right.timestamp.cmp(&left.timestamp))
}

/// Read the parquet file at `path` back into rows.
pub(super) async fn read_parquet_output_rows(file_io: &FileIO, path: &str) -> Vec<LogOutputRow> {
    let bytes = file_io
        .new_input(path)
        .expect("parquet input")
        .read()
        .await
        .expect("read parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("parquet reader builder")
        .build()
        .expect("parquet record reader");
    let batches = reader.collect::<std::result::Result<Vec<_>, _>>().expect("read record batches");
    rows_from_record_batches(&batches)
}

/// Assert that every row group of `path` is sorted and that its statistics describe its own rows.
pub(super) async fn assert_parquet_row_group_bounds_match_sorted_rows(file_io: &FileIO, path: &str) {
    let bytes = file_io
        .new_input(path)
        .expect("parquet input")
        .read()
        .await
        .expect("read parquet");
    let reader = SerializedFileReader::new(bytes).expect("serialized parquet reader");
    let metadata = reader.metadata();
    assert!(metadata.num_row_groups() > 0, "parquet file must contain row groups");

    let rows = read_parquet_output_rows(file_io, path).await;
    let mut row_offset = 0usize;
    for row_group_idx in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(row_group_idx);
        let row_count = usize::try_from(row_group.num_rows()).expect("row group row count");
        let row_group_rows = &rows[row_offset..row_offset + row_count];
        row_offset += row_count;

        for window in row_group_rows.windows(2) {
            assert_ne!(
                log_sort_key_cmp(&window[0], &window[1]),
                std::cmp::Ordering::Greater,
                "row group {row_group_idx} must not contradict logs sort order"
            );
        }

        let service_stats = row_group
            .columns()
            .iter()
            .find(|column| column.column_path().string() == COL_SERVICE_NAME)
            .and_then(|column| column.statistics())
            .expect("service_name statistics");
        let Statistics::ByteArray(service_stats) = service_stats else {
            panic!("service_name must have byte-array statistics");
        };
        let min_service =
            std::str::from_utf8(service_stats.min_bytes_opt().expect("service min")).expect("service min utf8");
        let max_service =
            std::str::from_utf8(service_stats.max_bytes_opt().expect("service max")).expect("service max utf8");
        let actual_min_service = row_group_rows
            .iter()
            .filter_map(|row| row.service_name.as_deref())
            .min()
            .expect("actual min service");
        let actual_max_service = row_group_rows
            .iter()
            .filter_map(|row| row.service_name.as_deref())
            .max()
            .expect("actual max service");
        assert_eq!(min_service, actual_min_service);
        assert_eq!(max_service, actual_max_service);
    }
    assert_eq!(row_offset, rows.len(), "metadata row counts must cover all rows");
}

/// Task deadlines wide enough that a worker never declares a fixture task expired, which would
/// let a second worker take it over and make the assertions depend on scheduling.
pub(super) fn generous_timeouts() -> TimeoutEstimator {
    TimeoutEstimator::new(&super::config::ShiftTimeoutsConfig {
        plan_base_ms: 60_000,
        shift_base_ms: 60_000,
        shift_per_record_batch_ms: 1,
        shift_per_segment_ms: 1,
        commit_base_ms: 60_000,
        commit_per_parquet_file_ms: 1,
    })
}
