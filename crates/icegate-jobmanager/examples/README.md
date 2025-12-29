# JobManager Examples

This directory contains examples demonstrating different features of the jobmanager library.

## Prerequisites

- **Rust**: Make sure you have Rust installed (1.70 or later)
- **Docker & Docker Compose**: Required to run MinIO (S3-compatible storage)

## Setup

### 1. Start MinIO

All examples require MinIO to be running. Start it using docker-compose from the project root:

```bash
docker-compose up -d
```

This will:
- Start MinIO on port 9000 (API) and 9001 (Console)
- Automatically create the required `jobs` bucket
- Use credentials: `minioadmin` / `minioadmin`

You can access the MinIO console at [http://localhost:9001](http://localhost:9001) to inspect the job state files.

### 2. Verify MinIO is Running

```bash
curl http://localhost:9000/minio/health/live
```

Should return: `200 OK`

## Examples

### 1. Simple Job (`simple_job.rs`)

A minimal example demonstrating basic job execution with a single task.

**Features:**
- Single task execution
- Basic S3 storage configuration
- Signal handling (Ctrl+C)

**Run:**
```bash
cargo run --example simple_job
```

**Expected Behavior:**
- Creates a job with one task
- Task simulates 100ms of work
- Job completes and exits

---

### 2. Simple Sequence Job (`simple_sequence_job.rs`)

Demonstrates sequential task execution with retry logic and multiple workers.

**Features:**
- Two sequential tasks (`first_step` → `second_step`)
- Dynamic task creation (first task creates the second)
- Simulated failures (30% random failure rate in first step)
- Automatic retry mechanism
- 5 concurrent workers
- Custom worker configuration (poll intervals, deadlines, heartbeat)

**Run:**
```bash
cargo run --example simple_sequence_job
```

**Expected Behavior:**
- First task executes and may fail randomly
- On failure, the task is retried automatically
- On success, second task is created and executed
- Multiple iterations demonstrate the retry mechanism
- Press Ctrl+C to stop

---

### 3. JSON Model Job (`json_model_job.rs`)

Shows how to pass structured data (JSON) between tasks using serde.

**Features:**
- JSON serialization/deserialization with serde
- Structured data passing between tasks
- Type-safe data models
- Task chaining with data flow

**Run:**
```bash
cargo run --example json_model_job
```

**Expected Behavior:**
- First task creates a `TaskData` struct with timestamp and metadata
- Data is serialized to JSON
- Second task receives and deserializes the JSON
- Logs show the structured data being passed
- Job completes successfully

---

## Inspecting Job State

### MinIO Console

Access the MinIO console at [http://localhost:9001](http://localhost:9001):
- Username: `minioadmin`
- Password: `minioadmin`

Navigate to the `jobs` bucket to see the job state files. Each job iteration creates a state file with the job and task information.

### CLI Access

You can also use the AWS CLI (configured for MinIO):

```bash
# List job state files
aws --endpoint-url http://localhost:9000 s3 ls s3://jobs/jobs/ --recursive

# Download a state file
aws --endpoint-url http://localhost:9000 s3 cp s3://jobs/jobs/my_job_code/state-*.json ./state.json
```

## Cleanup

### Stop MinIO

```bash
docker-compose down
```

### Remove MinIO Data

To completely clean up MinIO data (job states, iterations):

```bash
docker-compose down -v
rm -rf .tmp/minio_data
```

## Troubleshooting

### MinIO Connection Errors

If you see connection errors, verify MinIO is running:
```bash
docker ps | grep minio
```

Restart if needed:
```bash
docker-compose restart
```

### Port Already in Use

If ports 9000 or 9001 are already in use, modify `docker-compose.yml` to use different ports.

### Jobs Not Starting

Check the MinIO logs:
```bash
docker-compose logs jobmanager.minio
```

Ensure the `jobs` bucket was created:
```bash
docker-compose logs jobmanager.mc-minio
```

## Key Rust Patterns

These examples demonstrate idiomatic Rust patterns for the jobmanager library:

### Task Executor Function Signature

```rust
let executor: TaskExecutorFn = Arc::new(|task, manager, _cancel_token| {
    // Extract data before async block to avoid Send trait issues
    let task_id = task.id().to_string();

    Box::pin(async move {
        // Async work here
        manager.complete_task(&task_id, output)
    })
});
```

### JobRegistry Setup

```rust
// Create job definitions
let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
```

### Signal Handling

```rust
let handle = manager.start()?;
tokio::signal::ctrl_c().await?;
tracing::info!("Shutting down...");
handle.shutdown().await?;
```

### JSON Serialization

```rust
// Serialize
let json_data = serde_json::to_vec(&data)
    .map_err(|e| Error::Other(format!("JSON error: {}", e)))?;

// Deserialize
let data: TaskData = serde_json::from_slice(task.get_input())
    .map_err(|e| Error::Other(format!("JSON error: {}", e)))?;
```

## Next Steps

- Modify the examples to understand how different configurations affect behavior
- Try creating your own job definitions with custom task executors
- Experiment with different worker counts and polling intervals
- Explore the MinIO console to see how job state is persisted
