# icegate-queue

A generic WAL-based data queue with Parquet on object storage.

## Features

- **Durable writes**: Data is persisted to object storage before acknowledgment
- **Exactly-once semantics**: Uses `If-None-Match` for atomic writes
- **Sequential ordering**: Monotonically increasing offsets per topic
- **Row group partitioning**: Optional grouping by column for efficient reads
- **Backpressure**: Bounded channels prevent memory overflow
- **Recovery**: Automatic offset recovery on restart

## Usage

```rust
use icegate_queue::{QueueConfig, QueueWriter, WriteRequest, channel};
use arrow::record_batch::RecordBatch;
use tokio::sync::oneshot;
use std::sync::Arc;

// Create queue writer
let config = QueueConfig::new("s3://bucket/queue");
let store = Arc::new(object_store::memory::InMemory::new());
let writer = QueueWriter::new(config, store);

// Create channel and start writer
let (tx, rx) = channel(1024);
let _handle = writer.start(rx);

// Send write request
let (response_tx, response_rx) = oneshot::channel();
tx.send(WriteRequest {
    topic: "logs".to_string(),
    batch: record_batch,
    group_by_column: Some("tenant_id".to_string()),
    response_tx,
}).await?;

// Wait for result
let result = response_rx.await?;
```

## License

Apache-2.0
