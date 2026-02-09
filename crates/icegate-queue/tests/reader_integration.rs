//! Integration tests for `ParquetQueueReader` with MinIO/S3.

mod common;

use icegate_queue::{ParquetQueueReader, QueueConfig, QueueWriter, WriteRequest, channel};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

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
            batch: batch.clone(),
            group_by_column: None,
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
        assert_eq!(segment.offset, i as u64, "Segment offset should match index");
        assert_eq!(segment.topic, "logs", "Topic should match");
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
            batch: batch.clone(),
            group_by_column: None,
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
    assert_eq!(segments[0].offset, 2);
    assert_eq!(segments[1].offset, 3);
    assert_eq!(segments[2].offset, 4);
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
        batch: original_batch.clone(),
        group_by_column: None,
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
    let batches = reader.read_segment(&"logs".to_string(), offset, &[0], &cancel).await.unwrap();

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
        batch: original_batch,
        group_by_column: None,
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
    let batches = reader.read_segment(&"logs".to_string(), offset, &[0], &cancel).await.unwrap();

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
        batch: batch.clone(),
        group_by_column: None,
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
    let batches = reader.read_segment(&"logs".to_string(), offset, &[0], &cancel).await.unwrap();

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
            batch,
            group_by_column: None,
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
            batch,
            group_by_column: None,
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
            batch,
            group_by_column: None,
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
