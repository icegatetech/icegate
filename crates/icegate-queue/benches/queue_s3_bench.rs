//! Queue benchmarks with `MinIO` for S3 operations.
//!
//! Benchmarks cover:
//! 1. Write performance (small batches, large batches, concurrent topics)
//! 2. Read performance (list segments, read single segment, plan segments)
//! 3. End-to-end (write then read)
//!
//! A single MinIO container is shared across all benchmark groups to avoid
//! repeated container startup overhead.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::print_stdout, missing_docs)]

use std::{sync::Arc, time::Duration};

use criterion::{Criterion, criterion_group, criterion_main};
use futures::TryStreamExt;
use icegate_common::testing::{MinIOContainer, create_s3_bucket, create_s3_object_store};
use icegate_queue::{
    NoopQueueWriterEvents, ParquetQueueReader, PreparedWalRowGroup, QueueConfig, QueueWriter, WriteRequest, channel,
};
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
async fn write_batch(tx: &icegate_queue::WriteChannel, topic: &str, batch: arrow::record_batch::RecordBatch) {
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: topic.to_string(),
        row_groups: vec![PreparedWalRowGroup::new(batch)],
        response_tx,
        trace_context: None,
    })
    .await
    .expect("Failed to send write request");
    response_rx.await.expect("Failed to receive write result");
}

/// All queue benchmarks sharing a single MinIO container.
fn queue_benchmarks(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    // Single MinIO container for all benchmark groups
    let (minio, store) = rt.block_on(async { setup_minio_and_bucket("bench").await });

    // --- Group 1: Write Performance ---
    {
        let mut group = c.benchmark_group("write_performance");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(30));

        // Benchmark 1: Small batches - 10 batches x 100 records
        group.bench_function("small_batches", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
                    let batches = generate_batches_for_throughput(10, 100);
                    for batch in batches {
                        write_batch(&tx, "small", batch).await;
                    }
                    drop(tx);
                    writer_handle.await.unwrap().unwrap();
                });
            });
        });

        // Benchmark 2: Large batches - 10 batches x 10,000 records
        group.bench_function("large_batches", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
                    let batches = generate_batches_for_throughput(10, 10_000);
                    for batch in batches {
                        write_batch(&tx, "large", batch).await;
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
                        write_batch(&tx, "topic1", batches_topic1[i].clone()).await;
                        write_batch(&tx, "topic2", batches_topic2[i].clone()).await;
                        write_batch(&tx, "topic3", batches_topic3[i].clone()).await;
                    }
                    drop(tx);
                    writer_handle.await.unwrap().unwrap();
                });
            });
        });

        drop(group);
    }

    // --- Group 2: Read Performance ---
    {
        // Pre-write segments for read benchmarks
        rt.block_on(async {
            let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
            let batches = generate_batches_for_throughput(10, 100);
            for batch in batches {
                write_batch(&tx, "reads", batch).await;
            }
            drop(tx);
            writer_handle.await.unwrap().unwrap();
        });

        let reader = ParquetQueueReader::new("queue".to_string(), Arc::clone(&store), 8192).unwrap();
        let topic = "reads".to_string();

        let mut group = c.benchmark_group("read_performance");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(15));

        // Benchmark 4: List segments
        group.bench_function("list_segments", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let cancel = CancellationToken::new();
                    let _ = reader.list_segments(&topic, 0, &cancel).await.unwrap();
                });
            });
        });

        // Benchmark 5: Read single segment
        group.bench_function("read_single_segment", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let cancel = CancellationToken::new();
                    let _ = reader
                        .read_segment(&topic, 0, &[0], &cancel)
                        .await
                        .unwrap()
                        .try_collect::<Vec<_>>()
                        .await
                        .unwrap();
                });
            });
        });

        // Benchmark 6: List and count segments
        group.bench_function("list_segments_count", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let cancel = CancellationToken::new();
                    let segments = reader.list_segments(&topic, 0, &cancel).await.unwrap();
                    let _ = segments.len();
                });
            });
        });

        drop(group);
    }

    // --- Group 3: End-to-End ---
    {
        let mut group = c.benchmark_group("end_to_end");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(30));

        // Benchmark 7: Write then read - write 5 batches, read back via plan+read
        group.bench_function("write_then_read", |b| {
            b.iter(|| {
                rt.block_on(async {
                    // Write phase
                    let (writer_handle, tx) = start_writer(Arc::clone(&store), "queue");
                    let batches = generate_batches_with_grouping(5, 100, 5);
                    for batch in batches {
                        write_batch(&tx, "e2e", batch).await;
                    }
                    drop(tx);
                    writer_handle.await.unwrap().unwrap();

                    // Read phase
                    let reader =
                        ParquetQueueReader::new("queue".to_string(), Arc::clone(&store), 8192).unwrap();
                    let cancel = CancellationToken::new();
                    let topic_e2e = "e2e".to_string();

                    // List and read segments directly
                    let segments = reader.list_segments(&topic_e2e, 0, &cancel).await.unwrap();
                    for segment in segments {
                        // Read first row group from each segment
                        let _ = reader
                            .read_segment(&topic_e2e, segment.id.offset, &[0], &cancel)
                            .await
                            .unwrap()
                            .try_collect::<Vec<_>>()
                            .await
                            .unwrap();
                    }
                });
            });
        });

        drop(group);
    }

    // Cleanup: drop MinIO within the runtime context
    rt.block_on(async {
        drop(minio);
    });
}

criterion_group!(benches, queue_benchmarks);
criterion_main!(benches);
