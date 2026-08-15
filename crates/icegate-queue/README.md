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
use icegate_queue::{PreparedWalRowGroup, QueueConfig, QueueWriter, WriteRequest, channel};
use arrow::record_batch::RecordBatch;
use tokio::sync::oneshot;
use std::sync::Arc;

// Create queue writer
let config = QueueConfig::new("s3://bucket/queue");
let store = Arc::new(object_store::memory::InMemory::new());
let writer = QueueWriter::new(config, store);

// Create channel and start writer. `start` recovers the per-topic offset
// counters BEFORE it spawns anything, so a queue whose history cannot be
// resumed fails here rather than on the first write.
let (tx, rx) = channel(1024);
let _handle = writer.start(rx).await?;

// Send write request
let (response_tx, response_rx) = oneshot::channel();
tx.send(WriteRequest {
    topic: "logs".to_string(),
    row_groups: vec![PreparedWalRowGroup::new(record_batch)],
    response_tx,
    trace_context: None,
}).await?;

// Wait for result
let result = response_rx.await?;
```

## Startup and recovery

`QueueWriter::start` recovers each topic's offset counter before the writer
accepts anything, and returns an error instead of a handle when it cannot:

- the search for a topic's highest offset starts at the offset the caller
  declared consumed downstream (`with_committed_offsets`), or at any surviving
  segment when no such offset was given;
- a topic that HAS a committed offset but no segment at all is unresumable —
  something consumed segments the store says never existed — and recovery fails
  rather than picking a counter that could overwrite live data;
- holes left by retention cleanup do not confuse the search: it asks "does any
  segment exist above N", which stays monotone across a gap.

## Retention

`QueueCleaner` is the entry point for deleting segments: it takes an offset
bound from the caller and removes the tail at or below it, oldest first, with a
per-call delete budget, a dry-run mode, and a cancellation check between
batches. It decides nothing about safety — deriving a bound that no reader still
needs is the caller's job (in IceGate, `icegate-maintain`'s WAL cleanup).

A refused deletion is retried while the store reports the fault as transient
(throttling, a 5xx, a dropped connection) and skipped once the retries are spent
or the refusal is terminal — a lock, a policy, a missing permission. A key the
store says is already gone counts as deleted, since removing it is what the call
was after. Only an unbroken run of refused segments, one delete batch long, makes
the call give up on the topic for that cycle; the next cycle starts over from the
same bound.

## Segment listing

A listing asked for from an offset returns either nothing — the topic holds no
segment that high, the normal state of a queue its consumer has caught up with —
or an unbroken run of offsets from exactly that offset upwards. Both ways
retention cleanup can break that run are reported instead of returning a silently
shorter list: `SegmentsGone` when the run starts above the requested offset, and
`SegmentMissing` when a segment inside the run is gone, which is what cleanup
leaves behind when it steps over a segment the store refuses to delete. The
caller cannot tell a shortened answer from a complete one, and reading it as
complete drops committed rows.

## License

Apache-2.0
