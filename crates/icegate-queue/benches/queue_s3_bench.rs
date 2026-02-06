//! Queue benchmarks with `MinIO` for S3 operations.
//!
//! Benchmarks cover:
//! 1. Write performance (small batches, large batches, concurrent topics)
//! 2. Write with grouping (`tenant_id` grouping vs no grouping)
//! 3. Read performance (list segments, read single segment, plan segments)
//! 4. End-to-end (write then read)

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout, missing_docs)]

use std::{sync::Arc, time::Duration};

use criterion::{Criterion, criterion_group, criterion_main};
use icegate_common::testing::{MinIOContainer, create_s3_bucket, create_s3_object_store};
use icegate_queue::{NoopQueueWriterEvents, ParquetQueueReader, QueueConfig, QueueWriter, WriteRequest, channel};
use tokio::{sync::oneshot, task::JoinHandle};
use tokio_util::sync::CancellationToken;

mod common;
use common::data::{generate_batches_for_throughput, generate_batches_with_grouping};

/// Helper to setup `MinIO`, create bucket, and return object store.
async fn setup_minio_and_bucket(bucket_name: &str) -> (MinIOContainer, Arc<dyn object_store::ObjectStore>) {
    let minio = MinIOContainer::start().await.expect("Failed to start MinIO");
    create_s3_bucket(minio.endpoint(), bucket_name)
        .await
        .expect("Failed to create bucket");
    let store = create_s3_object_store(minio.endpoint(), bucket_name).expect("Failed to create object store");
    (minio, store)
}

/// Helper to start queue writer and return writer handle and write channel.
fn start_writer(
    store: Arc<dyn object_store::ObjectStore>,
    base_path: &str,
) -> (JoinHandle<icegate_queue::Result<()>>, icegate_queue::WriteChannel) {
    let config = QueueConfig::new(base_path);
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store).with_events(Arc::new(NoopQueueWriterEvents));
    let handle = writer.start(rx);
    (handle, tx)
}

/// Helper to write a single batch and wait for result.
async fn write_batch(
    tx: &icegate_queue::WriteChannel,
    topic: &str,
    batch: arrow::record_batch::RecordBatch,
    group_by_column: Option<String>,
) {
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: topic.to_string(),
        batch,
        group_by_column,
        response_tx,
        trace_context: None,
    })
    .await
    .expect("Failed to send write request");
    response_rx.await.expect("Failed to receive write result");
}

/// Benchmark Group 1: Write Performance
fn write_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_performance");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Shared MinIO for all benchmarks in this group
    let (minio, store) = rt.block_on(async { setup_minio_and_bucket("write-perf").await });

    // Benchmark 1: Small batches - 10 batches × 100 records
    group.bench_function("small_batches", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
                let batches = generate_batches_for_throughput(10, 100);
                for batch in batches {
                    write_batch(&tx, "small", batch, None).await;
                }
                drop(tx);
                writer_handle.await.unwrap().unwrap();
            });
        });
    });

    // Benchmark 2: Large batches - 10 batches × 10,000 records
    group.bench_function("large_batches", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
                let batches = generate_batches_for_throughput(10, 10_000);
                for batch in batches {
                    write_batch(&tx, "large", batch, None).await;
                }
                drop(tx);
                writer_handle.await.unwrap().unwrap();
            });
        });
    });

    // Benchmark 3: Concurrent topics - 3 topics in parallel
    group.bench_function("concurrent_topics", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
                let batches_topic1 = generate_batches_for_throughput(10, 100);
                let batches_topic2 = generate_batches_for_throughput(10, 100);
                let batches_topic3 = generate_batches_for_throughput(10, 100);

                // Interleave writes from 3 topics
                for i in 0..10 {
                    write_batch(&tx, "topic1", batches_topic1[i].clone(), None).await;
                    write_batch(&tx, "topic2", batches_topic2[i].clone(), None).await;
                    write_batch(&tx, "topic3", batches_topic3[i].clone(), None).await;
                }
                drop(tx);
                writer_handle.await.unwrap().unwrap();
            });
        });
    });

    drop(group);
    // Explicitly drop MinIO within the runtime context
    rt.block_on(async {
        drop(minio);
    });
}

/// Benchmark Group 2: Write Grouping
fn write_grouping(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_grouping");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Shared MinIO for all benchmarks in this group
    let (minio, store) = rt.block_on(async { setup_minio_and_bucket("write-grouping").await });

    // Benchmark 4: With grouping - 10 batches with tenant_id grouping
    group.bench_function("with_grouping", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
                let batches = generate_batches_with_grouping(10, 100, 5);
                for batch in batches {
                    write_batch(&tx, "grouped", batch, Some("tenant_id".to_string())).await;
                }
                drop(tx);
                writer_handle.await.unwrap().unwrap();
            });
        });
    });

    // Benchmark 5: Without grouping - same data, no grouping
    group.bench_function("without_grouping", |b| {
        b.iter(|| {
            rt.block_on(async {
                let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
                let batches = generate_batches_with_grouping(10, 100, 5);
                for batch in batches {
                    write_batch(&tx, "ungrouped", batch, None).await;
                }
                drop(tx);
                writer_handle.await.unwrap().unwrap();
            });
        });
    });

    drop(group);
    // Explicitly drop MinIO within the runtime context
    rt.block_on(async {
        drop(minio);
    });
}

/// Benchmark Group 3: Read Performance
fn read_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_performance");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Create MinIO and pre-write segments
    let (minio, store) = rt.block_on(async {
        let (minio, store) = setup_minio_and_bucket("read-perf").await;
        let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");

        // Pre-write 10 segments for listing test (without grouping)
        let batches = generate_batches_for_throughput(10, 100);
        for batch in batches {
            write_batch(&tx, "reads", batch, None).await;
        }
        drop(tx);
        writer_handle.await.unwrap().unwrap();

        (minio, store)
    });

    let reader = ParquetQueueReader::new("queue".to_string(), Arc::clone(&store), 8192).unwrap();
    let topic = "reads".to_string();

    // Benchmark 6: List segments - list 10 pre-written segments
    group.bench_function("list_segments", |b| {
        b.iter(|| {
            rt.block_on(async {
                let cancel = CancellationToken::new();
                let _ = reader.list_segments(&topic, 0, &cancel).await.unwrap();
            });
        });
    });

    // Benchmark 7: Read single segment - read 1 segment with row groups
    group.bench_function("read_single_segment", |b| {
        b.iter(|| {
            rt.block_on(async {
                let cancel = CancellationToken::new();
                // Read row group 0 from segment 0
                let _ = reader.read_segment(&topic, 0, &[0], &cancel).await.unwrap();
            });
        });
    });

    // Benchmark 8: List and count segments
    group.bench_function("list_segments_count", |b| {
        b.iter(|| {
            rt.block_on(async {
                let cancel = CancellationToken::new();
                // Note: Planning without grouping since pre-written data isn't grouped
                let segments = reader.list_segments(&topic, 0, &cancel).await.unwrap();
                let _ = segments.len();
            });
        });
    });

    drop(group);
    // Explicitly drop MinIO within the runtime context
    rt.block_on(async {
        drop(minio);
    });
}

/// Benchmark Group 4: End-to-End
fn end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("end_to_end");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(40));

    let rt = tokio::runtime::Runtime::new().unwrap();

    // Setup: Shared MinIO for all benchmarks in this group
    let (minio, store) = rt.block_on(async { setup_minio_and_bucket("e2e").await });

    // Benchmark 9: Write then read - write 5 batches, read back via plan+read
    group.bench_function("write_then_read", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Write phase
                let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
                let batches = generate_batches_with_grouping(5, 100, 5);
                for batch in batches {
                    write_batch(&tx, "e2e", batch, Some("tenant_id".to_string())).await;
                }
                drop(tx);
                writer_handle.await.unwrap().unwrap();

                // Read phase
                let reader = ParquetQueueReader::new("queue".to_string(), Arc::clone(&store), 8192).unwrap();
                let cancel = CancellationToken::new();
                let topic_e2e = "e2e".to_string();

                // List and read segments directly
                let segments = reader.list_segments(&topic_e2e, 0, &cancel).await.unwrap();
                for segment in segments {
                    // Read first row group from each segment
                    let _ = reader.read_segment(&topic_e2e, segment.offset, &[0], &cancel).await.unwrap();
                }
            });
        });
    });

    drop(group);
    // Explicitly drop MinIO within the runtime context
    rt.block_on(async {
        drop(minio);
    });
}

criterion_group!(benches, write_performance, write_grouping, read_performance, end_to_end);
criterion_main!(benches);
