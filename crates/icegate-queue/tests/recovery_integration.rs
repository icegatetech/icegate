//! Integration tests for `QueueWriter` offset recovery with MinIO/S3.

mod common;

use icegate_queue::{QueueConfig, QueueWriter, WriteRequest, channel};
use tokio::sync::oneshot;

#[tokio::test]
async fn test_recovery_empty_store() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;

    // Create writer and recover on empty store
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    // First write should start at offset 0
    let batch = common::test_batch(10, 1)?;
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

    let result = response_rx.await.unwrap();
    assert!(result.is_success());
    assert_eq!(
        result.offset(),
        Some(0),
        "First write on empty store should be offset 0"
    );

    drop(tx);
    handle.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test]
async fn test_recovery_continues_from_max_offset() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;
    let batch = common::test_batch(10, 1)?;

    // First writer: write 3 segments (offsets 0, 1, 2)
    {
        let config = QueueConfig::new("queue");
        let (tx, rx) = channel(config.channel_capacity);
        let writer = QueueWriter::new(config, store.clone());
        let handle = writer.start(rx);

        for _ in 0..3 {
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
    }

    // Second writer: should recover and continue from offset 3
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

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

    let result = response_rx.await.unwrap();
    assert!(result.is_success());
    assert_eq!(
        result.offset(),
        Some(3),
        "Recovered writer should continue from offset 3"
    );

    drop(tx);
    handle.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test]
async fn test_recovery_multiple_topics() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;
    let batch = common::test_batch(10, 1)?;

    // First writer: write to multiple topics
    {
        let config = QueueConfig::new("queue");
        let (tx, rx) = channel(config.channel_capacity);
        let writer = QueueWriter::new(config, store.clone());
        let handle = writer.start(rx);

        // Write 3 segments to "logs"
        for _ in 0..3 {
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

        // Write 2 segments to "events"
        for _ in 0..2 {
            let (response_tx, response_rx) = oneshot::channel();
            tx.send(WriteRequest {
                topic: "events".to_string(),
                batch: batch.clone(),
                group_by_column: None,
                response_tx,
                trace_context: None,
            })
            .await
            .unwrap();
            response_rx.await.unwrap();
        }

        // Write 1 segment to "metrics"
        let (response_tx, response_rx) = oneshot::channel();
        tx.send(WriteRequest {
            topic: "metrics".to_string(),
            batch: batch.clone(),
            group_by_column: None,
            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();
        response_rx.await.unwrap();

        drop(tx);
        handle.await.unwrap().unwrap();
    }

    // Second writer: should recover independent offsets for each topic
    let config = QueueConfig::new("queue");
    let (tx, rx) = channel(config.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

    // Verify "logs" continues from offset 3
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
    let logs_result = response_rx.await.unwrap();
    assert_eq!(
        logs_result.offset(),
        Some(3),
        "logs topic should continue from offset 3"
    );

    // Verify "events" continues from offset 2
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "events".to_string(),
        batch: batch.clone(),
        group_by_column: None,
        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();
    let events_result = response_rx.await.unwrap();
    assert_eq!(
        events_result.offset(),
        Some(2),
        "events topic should continue from offset 2"
    );

    // Verify "metrics" continues from offset 1
    let (response_tx, response_rx) = oneshot::channel();
    tx.send(WriteRequest {
        topic: "metrics".to_string(),
        batch: batch.clone(),
        group_by_column: None,
        response_tx,
        trace_context: None,
    })
    .await
    .unwrap();
    let metrics_result = response_rx.await.unwrap();
    assert_eq!(
        metrics_result.offset(),
        Some(1),
        "metrics topic should continue from offset 1"
    );

    drop(tx);
    handle.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test]
async fn test_recovery_with_base_path() -> Result<(), Box<dyn std::error::Error>> {
    let (_minio, store, _bucket) = common::setup_queue_test().await?;
    let batch = common::test_batch(10, 1)?;

    // First writer: write with custom base_path
    {
        let config = QueueConfig::new("my-queue");
        let (tx, rx) = channel(config.channel_capacity);
        let writer = QueueWriter::new(config, store.clone());
        let handle = writer.start(rx);

        for _ in 0..2 {
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
    }

    // Second writer: recover with same base_path
    let config = QueueConfig::new("my-queue");
    let (tx, rx) = channel(config.channel_capacity);
    let writer = QueueWriter::new(config, store.clone());
    let handle = writer.start(rx);

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

    let result = response_rx.await.unwrap();
    assert!(result.is_success());
    assert_eq!(
        result.offset(),
        Some(2),
        "Recovered writer with base_path should continue from offset 2"
    );

    drop(tx);
    handle.await.unwrap().unwrap();

    // Verify no cross-contamination with different base_path
    let config_different = QueueConfig::new("different-queue");
    let (tx_diff, rx_diff) = channel(config_different.channel_capacity);
    let writer_diff = QueueWriter::new(config_different, store.clone());
    let handle_diff = writer_diff.start(rx_diff);

    let (response_tx, response_rx) = oneshot::channel();
    tx_diff
        .send(WriteRequest {
            topic: "logs".to_string(),
            batch: batch.clone(),
            group_by_column: None,
            response_tx,
            trace_context: None,
        })
        .await
        .unwrap();

    let result_diff = response_rx.await.unwrap();
    assert_eq!(
        result_diff.offset(),
        Some(0),
        "Different base_path should start from offset 0"
    );

    drop(tx_diff);
    handle_diff.await.unwrap().unwrap();
    Ok(())
}
