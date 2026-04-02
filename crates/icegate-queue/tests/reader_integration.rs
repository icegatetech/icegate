//! Integration tests for `ParquetQueueReader` with MinIO/S3.

mod common;

use std::{
    collections::HashMap,
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use futures::{TryStreamExt, stream::BoxStream};
use icegate_queue::{ParquetQueueReader, QueueConfig, QueueWriter, SegmentsPlan, WriteRequest, channel};
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, Result as ObjectStoreResult, path::Path,
};
use tokio::{
    sync::{Notify, oneshot},
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

fn normalize_plan(plan: &SegmentsPlan) -> Vec<(String, Vec<(u64, Vec<usize>)>, usize, u64)> {
    let mut normalized = plan
        .groups
        .iter()
        .map(|group| {
            let mut segments = group
                .segments
                .iter()
                .map(|segment| {
                    (
                        segment.segment_offset,
                        segment.row_groups.iter().map(|row_group| row_group.row_group_idx).collect(),
                    )
                })
                .collect::<Vec<_>>();
            segments.sort_by_key(|(offset, _)| *offset);
            (
                group.group_col_val.clone(),
                segments,
                group.record_batches_total,
                group.input_bytes_total,
            )
        })
        .collect::<Vec<_>>();
    normalized.sort();
    normalized
}

fn assert_is_forced_metadata_failure(err: &icegate_queue::QueueError) {
    match err {
        icegate_queue::QueueError::ObjectStore(object_store::Error::Generic { source, .. }) => {
            assert!(
                source.downcast_ref::<io::Error>().is_some(),
                "expected io::Error source in object_store generic error, got: {source}"
            );
        }
        other => panic!("expected QueueError::ObjectStore(Generic), got: {other}"),
    }
}

#[derive(Debug)]
struct DelayedObjectStore {
    inner: Arc<dyn ObjectStore>,
    delays: HashMap<u64, Duration>,
    active_metadata_reads: Option<Arc<AtomicUsize>>,
    max_active_metadata_reads: Option<Arc<AtomicUsize>>,
    metadata_concurrency_gate: Option<Arc<MetadataConcurrencyGate>>,
}

impl DelayedObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, delays_ms: HashMap<u64, u64>) -> Self {
        let delays = delays_ms
            .into_iter()
            .map(|(offset, ms)| (offset, Duration::from_millis(ms)))
            .collect();
        Self {
            inner,
            delays,
            active_metadata_reads: None,
            max_active_metadata_reads: None,
            metadata_concurrency_gate: None,
        }
    }

    fn with_metadata_read_probe(
        mut self,
        active_metadata_reads: Arc<AtomicUsize>,
        max_active_metadata_reads: Arc<AtomicUsize>,
    ) -> Self {
        self.active_metadata_reads = Some(active_metadata_reads);
        self.max_active_metadata_reads = Some(max_active_metadata_reads);
        self
    }

    fn with_metadata_concurrency_gate(mut self, metadata_concurrency_gate: Arc<MetadataConcurrencyGate>) -> Self {
        self.metadata_concurrency_gate = Some(metadata_concurrency_gate);
        self
    }

    fn delay_for_location(&self, location: &Path) -> Option<Duration> {
        let file_name = location.as_ref().rsplit('/').next()?;
        let offset = file_name.strip_suffix(".parquet")?.parse::<u64>().ok()?;
        self.delays.get(&offset).copied()
    }
}

fn update_max_seen(max: &AtomicUsize, value: usize) {
    let mut observed = max.load(Ordering::SeqCst);
    while value > observed {
        match max.compare_exchange(observed, value, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(new_observed) => observed = new_observed,
        }
    }
}

struct ActiveMetadataReadGuard {
    counter: Option<Arc<AtomicUsize>>,
}

impl ActiveMetadataReadGuard {
    const fn new(counter: Option<Arc<AtomicUsize>>) -> Self {
        Self { counter }
    }
}

impl Drop for ActiveMetadataReadGuard {
    fn drop(&mut self) {
        if let Some(counter) = &self.counter {
            counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

#[derive(Debug)]
struct MetadataConcurrencyGate {
    required_parallel_reads: usize,
    entered_reads: AtomicUsize,
    is_open: AtomicBool,
    notify: Notify,
    wait_timeout: Duration,
}

impl MetadataConcurrencyGate {
    fn new(required_parallel_reads: usize, wait_timeout: Duration) -> Self {
        Self {
            required_parallel_reads,
            entered_reads: AtomicUsize::new(0),
            is_open: AtomicBool::new(false),
            notify: Notify::new(),
            wait_timeout,
        }
    }

    async fn wait_until_open(&self) -> ObjectStoreResult<()> {
        let notified = self.notify.notified();
        if self.is_open.load(Ordering::SeqCst) {
            return Ok(());
        }
        let entered = self.entered_reads.fetch_add(1, Ordering::SeqCst) + 1;
        if entered >= self.required_parallel_reads {
            self.is_open.store(true, Ordering::SeqCst);
            self.notify.notify_waiters();
            return Ok(());
        }

        timeout(self.wait_timeout, notified)
            .await
            .map_err(|_| object_store::Error::Generic {
                store: "DelayedObjectStore",
                source: Box::new(io::Error::other(
                    "metadata concurrency gate timed out: metadata reads did not overlap",
                )),
            })?;
        Ok(())
    }
}

#[derive(Debug)]
struct FailingMetadataObjectStore {
    inner: Arc<dyn ObjectStore>,
    fail_offset: u64,
    delays: HashMap<u64, Duration>,
}

impl FailingMetadataObjectStore {
    fn new(inner: Arc<dyn ObjectStore>, fail_offset: u64, delays_ms: HashMap<u64, u64>) -> Self {
        let delays = delays_ms
            .into_iter()
            .map(|(offset, ms)| (offset, Duration::from_millis(ms)))
            .collect();
        Self {
            inner,
            fail_offset,
            delays,
        }
    }

    fn offset_for_location(location: &Path) -> Option<u64> {
        let file_name = location.as_ref().rsplit('/').next()?;
        file_name.strip_suffix(".parquet")?.parse::<u64>().ok()
    }

    fn delay_for_location(&self, location: &Path) -> Option<Duration> {
        let offset = Self::offset_for_location(location)?;
        self.delays.get(&offset).copied()
    }
}

impl fmt::Display for FailingMetadataObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingMetadataObjectStore")
    }
}

#[async_trait]
impl ObjectStore for FailingMetadataObjectStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        if options.head && Self::offset_for_location(location) == Some(self.fail_offset) {
            return Err(object_store::Error::Generic {
                store: "FailingMetadataObjectStore",
                source: Box::new(io::Error::other(format!(
                    "forced metadata read failure for offset {}",
                    self.fail_offset
                ))),
            });
        }
        if let Some(delay) = self.delay_for_location(location) {
            if options.head || options.range.is_some() {
                sleep(delay).await;
            }
        }
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

impl fmt::Display for DelayedObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DelayedObjectStore")
    }
}

#[async_trait]
impl ObjectStore for DelayedObjectStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> ObjectStoreResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> ObjectStoreResult<GetResult> {
        let is_metadata_read = options.head || options.range.is_some();
        let _active_guard = if is_metadata_read {
            self.active_metadata_reads.as_ref().map_or_else(
                || ActiveMetadataReadGuard::new(None),
                |active_counter| {
                    let current = active_counter.fetch_add(1, Ordering::SeqCst) + 1;
                    if let Some(max_counter) = &self.max_active_metadata_reads {
                        update_max_seen(max_counter, current);
                    }
                    ActiveMetadataReadGuard::new(Some(Arc::clone(active_counter)))
                },
            )
        } else {
            ActiveMetadataReadGuard::new(None)
        };
        if is_metadata_read {
            if let Some(gate) = &self.metadata_concurrency_gate {
                gate.wait_until_open().await?;
            }
        }
        if let Some(delay) = self.delay_for_location(location) {
            if options.head || options.range.is_some() {
                sleep(delay).await;
            }
        }
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &Path) -> ObjectStoreResult<()> {
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> ObjectStoreResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> ObjectStoreResult<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[tokio::test]
async fn test_list_segments() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Write 5 segments
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let batch = common::test_batch(10, 1)?;
    for _ in 0..5 {
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch.clone()]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    // List all segments
    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let segments = reader.list_segments(&"logs".to_string(), 0, &cancel).await.unwrap();

    assert_eq!(segments.len(), 5, "Should list all 5 segments");
    for (i, segment) in segments.iter().enumerate() {
        assert_eq!(segment.id.offset, i as u64, "Segment offset should match index");
        assert_eq!(segment.id.topic, "logs", "Topic should match");
    }
    Ok(())
}

#[tokio::test]
async fn test_list_segments_with_offset() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Write 5 segments (offsets 0-4)
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let batch = common::test_batch(10, 1)?;
    for _ in 0..5 {
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch.clone()]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    // List segments starting from offset 2
    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let segments = reader.list_segments(&"logs".to_string(), 2, &cancel).await.unwrap();

    assert_eq!(segments.len(), 3, "Should list segments 2, 3, 4");
    assert_eq!(segments[0].id.offset, 2);
    assert_eq!(segments[1].id.offset, 3);
    assert_eq!(segments[2].id.offset, 4);
    Ok(())
}

#[tokio::test]
async fn test_list_empty_topic() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let segments = reader.list_segments(&"nonexistent".to_string(), 0, &cancel).await.unwrap();

    assert_eq!(segments.len(), 0, "Non-existent topic should return empty list");
    Ok(())
}

#[tokio::test]
async fn test_read_single_segment() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Write phase
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let original_batch = common::test_batch(100, 5)?;
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![original_batch.clone()]),

        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();

    let write_result = response_rx.await.unwrap();
    let offset = write_result.offset().unwrap();

    drop(tx);
    handle.await.unwrap().unwrap();

    // Read phase
    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let batches = reader
        .read_segment(&"logs".to_string(), offset, &[0], &cancel)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    // Verify data
    assert_eq!(batches.len(), 1, "Should read one batch");
    assert_eq!(batches[0].num_rows(), original_batch.num_rows());
    assert_eq!(batches[0].schema(), original_batch.schema());
    Ok(())
}

#[tokio::test]
async fn test_read_segment_uses_configured_record_batch_size() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Write phase: one WAL segment with one row group of 25 rows.
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let original_batch = common::test_batch(25, 1)?;
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![original_batch]),

        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();
    let write_result = response_rx.await.unwrap();
    let offset = write_result.offset().unwrap();

    drop(tx);
    handle.await.unwrap().unwrap();

    // Read with smaller batch size to force 25 rows => 10 + 10 + 5.
    let reader = ParquetQueueReader::new("queue", store, 10)?;
    let cancel = CancellationToken::new();
    let batches = reader
        .read_segment(&"logs".to_string(), offset, &[0], &cancel)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.len(), 3);
    assert_eq!(batches[0].num_rows(), 10);
    assert_eq!(batches[1].num_rows(), 10);
    assert_eq!(batches[2].num_rows(), 5);
    Ok(())
}

#[tokio::test]
async fn test_read_specific_row_groups() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Write a single segment (will have 1 row group)
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let batch = common::test_batch(100, 3)?;
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![batch.clone()]),

        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();

    let write_result = response_rx.await.unwrap();
    let offset = write_result.offset().unwrap();

    drop(tx);
    handle.await.unwrap().unwrap();

    // Read the single row group
    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let batches = reader
        .read_segment(&"logs".to_string(), offset, &[0], &cancel)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.len(), 1, "Should read 1 row group");
    assert_eq!(batches[0].num_rows(), 100, "Row group should have all rows");
    Ok(())
}

#[tokio::test]
async fn test_plan_segments_with_grouping() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Write multiple batches - each batch will have 1 row group
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    // Write 3 segments with single tenant_id per batch
    // This ensures each segment/row group has a unique tenant_id value
    for _ in 0..3 {
        let batch = common::test_batch(20, 1)?; // Single tenant per batch
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    // Plan segments grouped by tenant_id
    // Note: Since each segment has rows for only tenant-0, planning will group them
    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let plan = reader
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 100, 64 * 1024 * 1024, &cancel)
        .await
        .unwrap();

    assert_eq!(plan.segments_count, 3, "Should scan 3 segments");
    assert_eq!(plan.last_segment_offset, Some(2), "Last segment should be offset 2");
    assert!(plan.record_batches_total > 0, "Should have row groups");
    assert!(!plan.groups.is_empty(), "Should have grouped plans");

    // Verify groups
    for group in &plan.groups {
        assert!(!group.group_col_val.is_empty(), "Group key should not be empty");
        assert!(!group.segments.is_empty(), "Group should have segments");
        assert!(group.record_batches_total > 0, "Group should have row groups");
    }
    Ok(())
}

#[tokio::test]
async fn test_plan_max_row_groups_limit() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Write 5 segments with single tenant per batch
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    for _ in 0..5 {
        let batch = common::test_batch(20, 1)?;
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    // Plan with small max_row_groups limit
    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let plan = reader
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 2, 64 * 1024 * 1024, &cancel)
        .await
        .unwrap();

    // Verify that groups respect the limit
    for group in &plan.groups {
        assert!(
            group.record_batches_total <= 2,
            "Group should respect max_row_groups limit of 2, got {}",
            group.record_batches_total
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_plan_segments_with_small_input_bytes_limit() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    for _ in 0..3 {
        let batch = common::test_batch(20, 1)?;
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let plan = reader
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 100, 1, &cancel)
        .await
        .unwrap();

    for group in &plan.groups {
        assert_eq!(
            group.record_batches_total, 1,
            "Each task should contain exactly one row group when byte limit is tiny"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_plan_segments_parallelism_preserves_plan_result() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, base_store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, base_store.clone());
    let handle = writer.start(rx);

    for _ in 0..8 {
        let batch = common::test_batch(200, 1)?;
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    let serial_active_metadata_reads = Arc::new(AtomicUsize::new(0));
    let serial_max_active_metadata_reads = Arc::new(AtomicUsize::new(0));
    let parallel_active_metadata_reads = Arc::new(AtomicUsize::new(0));
    let parallel_max_active_metadata_reads = Arc::new(AtomicUsize::new(0));

    let serial_store: Arc<dyn ObjectStore> = Arc::new(
        DelayedObjectStore::new(
            base_store.clone(),
            HashMap::from([(0_u64, 25_u64), (1_u64, 25_u64), (2_u64, 25_u64)]),
        )
        .with_metadata_read_probe(
            Arc::clone(&serial_active_metadata_reads),
            Arc::clone(&serial_max_active_metadata_reads),
        ),
    );
    let parallel_store: Arc<dyn ObjectStore> = Arc::new(
        DelayedObjectStore::new(
            base_store,
            HashMap::from([
                (0_u64, 25_u64),
                (1_u64, 25_u64),
                (2_u64, 25_u64),
                (3_u64, 25_u64),
                (4_u64, 25_u64),
                (5_u64, 25_u64),
                (6_u64, 25_u64),
                (7_u64, 25_u64),
            ]),
        )
        .with_metadata_read_probe(
            Arc::clone(&parallel_active_metadata_reads),
            Arc::clone(&parallel_max_active_metadata_reads),
        )
        .with_metadata_concurrency_gate(Arc::new(MetadataConcurrencyGate::new(2, Duration::from_secs(2)))),
    );

    let reader_serial = ParquetQueueReader::new("queue", serial_store, 8192)?.with_plan_segment_read_parallelism(1)?;
    let reader_parallel =
        ParquetQueueReader::new("queue", parallel_store, 8192)?.with_plan_segment_read_parallelism(8)?;
    let cancel = CancellationToken::new();

    let serial_plan = reader_serial
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 64, 64 * 1024 * 1024, &cancel)
        .await
        .unwrap();
    let parallel_plan = reader_parallel
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 64, 64 * 1024 * 1024, &cancel)
        .await
        .unwrap();

    assert_eq!(serial_plan.last_segment_offset, parallel_plan.last_segment_offset);
    assert_eq!(serial_plan.segments_count, parallel_plan.segments_count);
    assert_eq!(serial_plan.record_batches_total, parallel_plan.record_batches_total);
    assert_eq!(serial_plan.input_bytes_total, parallel_plan.input_bytes_total);
    assert_eq!(normalize_plan(&serial_plan), normalize_plan(&parallel_plan));
    assert_eq!(
        serial_max_active_metadata_reads.load(Ordering::SeqCst),
        1,
        "serial planning must keep exactly one in-flight metadata read"
    );
    assert!(
        parallel_max_active_metadata_reads.load(Ordering::SeqCst) >= 2,
        "parallel planning must overlap metadata reads"
    );
    assert_eq!(serial_active_metadata_reads.load(Ordering::SeqCst), 0);
    assert_eq!(parallel_active_metadata_reads.load(Ordering::SeqCst), 0);

    Ok(())
}

#[tokio::test]
async fn test_plan_segments_parallelism_preserves_plan_result_with_skewed_metadata_delays()
-> Result<(), Box<dyn std::error::Error>> {
    let (_minio, base_store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, base_store.clone());
    let handle = writer.start(rx);

    for _ in 0..8 {
        let batch = common::test_batch(200, 1)?;
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    let delays = HashMap::from([
        (0_u64, 250_u64),
        (1_u64, 20_u64),
        (2_u64, 180_u64),
        (3_u64, 10_u64),
        (4_u64, 140_u64),
        (5_u64, 5_u64),
        (6_u64, 90_u64),
        (7_u64, 1_u64),
    ]);
    let serial_active_metadata_reads = Arc::new(AtomicUsize::new(0));
    let serial_max_active_metadata_reads = Arc::new(AtomicUsize::new(0));
    let parallel_active_metadata_reads = Arc::new(AtomicUsize::new(0));
    let parallel_max_active_metadata_reads = Arc::new(AtomicUsize::new(0));

    let delayed_store_serial: Arc<dyn ObjectStore> = Arc::new(
        DelayedObjectStore::new(base_store.clone(), delays.clone()).with_metadata_read_probe(
            Arc::clone(&serial_active_metadata_reads),
            Arc::clone(&serial_max_active_metadata_reads),
        ),
    );
    let delayed_store_parallel: Arc<dyn ObjectStore> = Arc::new(
        DelayedObjectStore::new(base_store, delays)
            .with_metadata_read_probe(
                Arc::clone(&parallel_active_metadata_reads),
                Arc::clone(&parallel_max_active_metadata_reads),
            )
            .with_metadata_concurrency_gate(Arc::new(MetadataConcurrencyGate::new(2, Duration::from_secs(2)))),
    );

    let reader_serial =
        ParquetQueueReader::new("queue", delayed_store_serial, 8192)?.with_plan_segment_read_parallelism(1)?;
    let reader_parallel =
        ParquetQueueReader::new("queue", delayed_store_parallel, 8192)?.with_plan_segment_read_parallelism(8)?;
    let cancel = CancellationToken::new();

    let serial_plan = reader_serial
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 64, 64 * 1024 * 1024, &cancel)
        .await
        .unwrap();
    let parallel_plan = reader_parallel
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 64, 64 * 1024 * 1024, &cancel)
        .await
        .unwrap();

    assert_eq!(serial_plan.last_segment_offset, parallel_plan.last_segment_offset);
    assert_eq!(serial_plan.segments_count, parallel_plan.segments_count);
    assert_eq!(serial_plan.record_batches_total, parallel_plan.record_batches_total);
    assert_eq!(serial_plan.input_bytes_total, parallel_plan.input_bytes_total);
    assert_eq!(normalize_plan(&serial_plan), normalize_plan(&parallel_plan));
    assert_eq!(
        serial_max_active_metadata_reads.load(Ordering::SeqCst),
        1,
        "serial planning must keep exactly one in-flight metadata read"
    );
    assert!(
        parallel_max_active_metadata_reads.load(Ordering::SeqCst) >= 2,
        "parallel planning must overlap metadata reads"
    );
    assert_eq!(serial_active_metadata_reads.load(Ordering::SeqCst), 0);
    assert_eq!(parallel_active_metadata_reads.load(Ordering::SeqCst), 0);

    Ok(())
}

#[tokio::test]
async fn test_plan_segments_parallelism_preserves_plan_result_on_blocking_metadata_path()
-> Result<(), Box<dyn std::error::Error>> {
    let (_minio, base_store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue").with_max_row_group_size(2);
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, base_store.clone());
    let handle = writer.start(rx);

    for _ in 0..4 {
        // 128 rows with row_group_size=2 => >=64 row groups per segment (spawn_blocking path).
        let batch = common::test_batch(128, 1)?;
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    let serial_active_metadata_reads = Arc::new(AtomicUsize::new(0));
    let serial_max_active_metadata_reads = Arc::new(AtomicUsize::new(0));
    let parallel_active_metadata_reads = Arc::new(AtomicUsize::new(0));
    let parallel_max_active_metadata_reads = Arc::new(AtomicUsize::new(0));

    let serial_store: Arc<dyn ObjectStore> = Arc::new(
        DelayedObjectStore::new(base_store.clone(), HashMap::from([(0_u64, 20_u64), (1_u64, 20_u64)]))
            .with_metadata_read_probe(
                Arc::clone(&serial_active_metadata_reads),
                Arc::clone(&serial_max_active_metadata_reads),
            ),
    );
    let parallel_store: Arc<dyn ObjectStore> = Arc::new(
        DelayedObjectStore::new(
            base_store,
            HashMap::from([(0_u64, 20_u64), (1_u64, 20_u64), (2_u64, 20_u64)]),
        )
        .with_metadata_read_probe(
            Arc::clone(&parallel_active_metadata_reads),
            Arc::clone(&parallel_max_active_metadata_reads),
        )
        .with_metadata_concurrency_gate(Arc::new(MetadataConcurrencyGate::new(2, Duration::from_secs(2)))),
    );

    let reader_serial = ParquetQueueReader::new("queue", serial_store, 8192)?.with_plan_segment_read_parallelism(1)?;
    let reader_parallel =
        ParquetQueueReader::new("queue", parallel_store, 8192)?.with_plan_segment_read_parallelism(8)?;
    let cancel = CancellationToken::new();

    let serial_plan = reader_serial
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 1024, 64 * 1024 * 1024, &cancel)
        .await
        .unwrap();
    let parallel_plan = reader_parallel
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 1024, 64 * 1024 * 1024, &cancel)
        .await
        .unwrap();

    assert!(
        serial_plan.record_batches_total >= 64 * 4,
        "expected >=64 row groups per segment to force spawn_blocking path"
    );
    assert_eq!(serial_plan.last_segment_offset, parallel_plan.last_segment_offset);
    assert_eq!(serial_plan.segments_count, parallel_plan.segments_count);
    assert_eq!(serial_plan.record_batches_total, parallel_plan.record_batches_total);
    assert_eq!(serial_plan.input_bytes_total, parallel_plan.input_bytes_total);
    assert_eq!(normalize_plan(&serial_plan), normalize_plan(&parallel_plan));
    assert_eq!(
        serial_max_active_metadata_reads.load(Ordering::SeqCst),
        1,
        "serial planning must keep exactly one in-flight metadata read"
    );
    assert!(
        parallel_max_active_metadata_reads.load(Ordering::SeqCst) >= 2,
        "parallel planning must overlap metadata reads"
    );
    assert_eq!(serial_active_metadata_reads.load(Ordering::SeqCst), 0);
    assert_eq!(parallel_active_metadata_reads.load(Ordering::SeqCst), 0);

    Ok(())
}

#[tokio::test]
async fn test_plan_segments_parallel_fails_fast_on_metadata_error_without_partial_plan()
-> Result<(), Box<dyn std::error::Error>> {
    let (_minio, base_store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, base_store.clone());
    let handle = writer.start(rx);

    for _ in 0..4 {
        let batch = common::test_batch(100, 1)?;
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    let failing_store: Arc<dyn ObjectStore> = Arc::new(FailingMetadataObjectStore::new(
        base_store,
        0,
        HashMap::from([(1_u64, 1_500_u64), (2_u64, 1_500_u64), (3_u64, 1_500_u64)]),
    ));
    let reader = ParquetQueueReader::new("queue", failing_store, 8192)?.with_plan_segment_read_parallelism(8)?;
    let cancel = CancellationToken::new();
    let start = Instant::now();

    let Err(err) = reader
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 64, 64 * 1024 * 1024, &cancel)
        .await
    else {
        panic!("planning must fail when metadata read fails for one segment");
    };

    let elapsed = start.elapsed();
    assert_is_forced_metadata_failure(&err);
    assert!(
        elapsed < Duration::from_millis(800),
        "planning must fail-fast without returning partial plan, elapsed={elapsed:?}"
    );

    Ok(())
}

#[tokio::test]
async fn test_plan_segments_parallel_fails_fast_on_non_first_metadata_error_without_waiting_for_first()
-> Result<(), Box<dyn std::error::Error>> {
    let (_minio, base_store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, base_store.clone());
    let handle = writer.start(rx);

    for _ in 0..4 {
        let batch = common::test_batch(100, 1)?;
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();
    }

    drop(tx);
    handle.await.unwrap().unwrap();

    let failing_store: Arc<dyn ObjectStore> = Arc::new(FailingMetadataObjectStore::new(
        base_store,
        1,
        HashMap::from([(0_u64, 1_500_u64), (2_u64, 1_500_u64), (3_u64, 1_500_u64)]),
    ));
    let reader = ParquetQueueReader::new("queue", failing_store, 8192)?.with_plan_segment_read_parallelism(8)?;
    let cancel = CancellationToken::new();
    let start = Instant::now();

    let Err(err) = reader
        .plan_segments(&"logs".to_string(), 0, "tenant_id", 64, 64 * 1024 * 1024, &cancel)
        .await
    else {
        panic!("planning must fail when metadata read fails for one segment");
    };

    let elapsed = start.elapsed();
    assert_is_forced_metadata_failure(&err);
    assert!(
        elapsed < Duration::from_millis(800),
        "planning must fail-fast on non-first segment error without waiting for offset 0, elapsed={elapsed:?}"
    );

    Ok(())
}
