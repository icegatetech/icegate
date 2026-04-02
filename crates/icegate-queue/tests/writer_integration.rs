//! Integration tests for `QueueWriter` with MinIO/S3.

mod common;

use futures::TryStreamExt;
use icegate_queue::{QueueConfig, QueueWriter, WriteRequest, channel};
use object_store::path::Path;
use tokio::sync::oneshot;

#[tokio::test]
async fn test_write_single_batch_to_s3() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Setup writer
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    // Write batch
    let batch = common::test_batch(100, 5)?;
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![batch.clone()]),

        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();

    // Verify response
    let result = response_rx.await.unwrap();
    assert!(result.is_success(), "Write should succeed");
    assert_eq!(result.offset(), Some(0), "First write should have offset 0");
    assert_eq!(result.records(), Some(100), "Should write 100 records");

    // Verify file exists on S3
    let path = Path::from("queue/logs/00000000000000000000.parquet");
    let meta = store.head(&path).await.unwrap();
    assert!(meta.size > 0, "File should have non-zero size");

    // Cleanup
    drop(tx);
    handle.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test]
async fn test_sequential_writes_monotonic_offsets() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let batch = common::test_batch(10, 1)?;

    // Write 3 batches sequentially
    let mut offsets = Vec::new();
    for _ in 0..3 {
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "logs".to_string(),
            row_groups: common::prepared_row_groups(vec![batch.clone()]),

            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();

        let result = response_rx.await.unwrap();
        assert!(result.is_success());
        offsets.push(result.offset().unwrap());
    }

    // Verify monotonic offsets
    assert_eq!(offsets, vec![0, 1, 2], "Offsets should be monotonic starting from 0");

    // Verify all 3 files exist
    for offset in 0..3 {
        let path = Path::from(format!("queue/logs/{offset:0>20}.parquet"));
        assert!(store.head(&path).await.is_ok(), "File for offset {offset} should exist");
    }

    drop(tx);
    handle.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test]
async fn test_write_empty_batch() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    // Create empty batch
    let batch = common::test_batch(0, 1)?;
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![batch.clone()]),

        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();

    let result = response_rx.await.unwrap();
    // Empty batch should succeed but not create a segment
    assert!(result.is_success());

    // Verify no file was created
    let path = Path::from("queue/logs/00000000000000000000.parquet");
    let head_result = store.head(&path).await;
    assert!(
        matches!(head_result, Err(object_store::Error::NotFound { .. })),
        "Empty batch should not create a file, expected NotFound error"
    );

    drop(tx);
    handle.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test]
async fn test_write_with_base_path() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("my-custom-path");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let batch = common::test_batch(10, 1)?;
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![batch.clone()]),

        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();

    let result = response_rx.await.unwrap();
    assert!(result.is_success());
    assert_eq!(result.offset(), Some(0));

    // Verify file exists at custom base path
    let path = Path::from("my-custom-path/logs/00000000000000000000.parquet");
    let meta = store.head(&path).await.unwrap();
    assert!(meta.size > 0);

    drop(tx);
    handle.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test]
async fn test_channel_write_response() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store);
    let handle = writer.start(rx);

    let batch = common::test_batch(42, 1)?;
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![batch.clone()]),

        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();

    let result = response_rx.await.unwrap();
    assert!(result.is_success());
    assert_eq!(result.offset(), Some(0));
    assert_eq!(result.records(), Some(42), "Should report correct record count");

    drop(tx);
    handle.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test]
async fn test_multi_batch_request_gets_single_ack_and_multiple_row_groups() -> Result<(), Box<dyn std::error::Error>> {
    use icegate_queue::ParquetQueueReader;
    use tokio_util::sync::CancellationToken;

    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue").with_flush_interval_ms(50);
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let batch_a = common::test_batch(3, 1)?;
    let batch_b = common::test_batch(2, 1)?;
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![batch_a.clone(), batch_b.clone()]),
        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();

    let result = response_rx.await.unwrap();
    assert!(result.is_success());
    assert_eq!(result.offset(), Some(0));
    assert_eq!(result.records(), Some(5));

    drop(tx);
    handle.await.unwrap().unwrap();

    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let batches = reader
        .read_segment(&"logs".to_string(), 0, &[0, 1], &cancel)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].num_rows(), 3);
    assert_eq!(batches[1].num_rows(), 2);
    Ok(())
}

#[tokio::test]
async fn test_logs_row_group_boundary_metadata_roundtrips_through_wal_footer() -> Result<(), Box<dyn std::error::Error>>
{
    use icegate_common::RowGroupBoundaryKey;
    use icegate_queue::ParquetQueueReader;
    use tokio_util::sync::CancellationToken;

    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue").with_flush_interval_ms(50);
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let batch_a = common::logs_batch(vec![
        (Some("acc-1"), Some("svc-2"), Some(40), 1),
        (Some("acc-1"), Some("svc-2"), Some(30), 2),
    ]);
    let batch_b = common::logs_batch(vec![
        (Some("acc-2"), Some("svc-1"), Some(20), 3),
        (Some("acc-2"), Some("svc-1"), Some(10), 4),
    ]);
    let expected_a = common::logs_row_group_boundary_key(&batch_a);
    let expected_b = common::logs_row_group_boundary_key(&batch_b);

    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_logs_row_groups(vec![batch_a, batch_b]),
        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();

    let result = response_rx.await.unwrap();
    assert!(result.is_success());
    assert_eq!(result.offset(), Some(0));

    drop(tx);
    handle.await.unwrap().unwrap();

    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let metadata = reader
        .read_segment_row_group_metadata(&"logs".to_string(), 0, &CancellationToken::new())
        .await?;

    assert_eq!(metadata.len(), 2);
    assert_eq!(
        serde_json::from_str::<RowGroupBoundaryKey>(metadata.get(&0).expect("row group 0 metadata"))?,
        expected_a
    );
    assert_eq!(
        serde_json::from_str::<RowGroupBoundaryKey>(metadata.get(&1).expect("row group 1 metadata"))?,
        expected_b
    );
    Ok(())
}

#[tokio::test]
async fn test_requests_in_same_flush_share_offset_but_keep_own_record_counts() -> Result<(), Box<dyn std::error::Error>>
{
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    let config = QueueConfig::new("queue")
        .with_max_row_group_size(10)
        .with_records_per_flush_multiplier(10)
        .with_flush_interval_ms(50);
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store);
    let handle = writer.start(rx);

    let (response_tx1, response_rx1) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![common::test_batch(3, 1)?, common::test_batch(4, 1)?]),
        response_tx: response_tx1,
        trace_context: None,
    })
    .await
    .unwrap();

    let (response_tx2, response_rx2) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "logs".to_string(),
        row_groups: common::prepared_row_groups(vec![common::test_batch(5, 1)?]),
        response_tx: response_tx2,
        trace_context: None,
    })
    .await
    .unwrap();

    let result1 = response_rx1.await.unwrap();
    let result2 = response_rx2.await.unwrap();

    assert!(result1.is_success());
    assert!(result2.is_success());
    assert_eq!(result1.offset(), Some(0));
    assert_eq!(result2.offset(), Some(0));
    assert_eq!(result1.records(), Some(7));
    assert_eq!(result2.records(), Some(5));

    drop(tx);
    handle.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test]
async fn test_write_then_read_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    use icegate_queue::ParquetQueueReader;
    use tokio_util::sync::CancellationToken;

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
    assert!(write_result.is_success());
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
    assert_eq!(
        batches[0].num_rows(),
        original_batch.num_rows(),
        "Row count should match"
    );
    assert_eq!(batches[0].schema(), original_batch.schema(), "Schema should match");
    Ok(())
}

#[tokio::test]
async fn test_write_read_schema_preservation() -> Result<(), Box<dyn std::error::Error>> {
    use icegate_queue::ParquetQueueReader;
    use tokio_util::sync::CancellationToken;

    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Write phase
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let original_batch = common::test_batch(50, 2)?;
    let original_schema = original_batch.schema();

    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "events".to_string(),
        row_groups: common::prepared_row_groups(vec![original_batch.clone()]),

        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();

    let write_result = response_rx.await.unwrap();
    assert!(write_result.is_success());

    drop(tx);
    handle.await.unwrap().unwrap();

    // Read phase
    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let batches = reader
        .read_segment(&"events".to_string(), 0, &[0], &cancel)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.len(), 1);
    let read_schema = batches[0].schema();

    // Verify schema fields match
    assert_eq!(read_schema.fields().len(), original_schema.fields().len());
    for (read_field, orig_field) in read_schema.fields().iter().zip(original_schema.fields().iter()) {
        assert_eq!(read_field.name(), orig_field.name());
        assert_eq!(read_field.data_type(), orig_field.data_type());
    }
    Ok(())
}

#[tokio::test]
async fn test_write_read_with_compression() -> Result<(), Box<dyn std::error::Error>> {
    use icegate_queue::ParquetQueueReader;
    use tokio_util::sync::CancellationToken;

    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Write phase - default config uses ZSTD compression
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.common.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    let original_batch = common::test_batch(1000, 10)?;
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
    assert!(write_result.is_success());

    drop(tx);
    handle.await.unwrap().unwrap();

    // Read phase - should decompress automatically
    let reader = ParquetQueueReader::new("queue", store, 8192)?;
    let cancel = CancellationToken::new();
    let batches = reader
        .read_segment(&"logs".to_string(), 0, &[0], &cancel)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), original_batch.num_rows());

    // Verify data integrity (spot check first row)
    let original_col = original_batch.column(0);
    let read_col = batches[0].column(0);
    assert_eq!(
        original_col.slice(0, 1).as_ref(),
        read_col.slice(0, 1).as_ref(),
        "Data should match after compression/decompression"
    );
    Ok(())
}
