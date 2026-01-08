// This example runs the simple job with CBOR job-state serialization.

#![allow(missing_docs)]

use icegate_jobmanager::{Error, s3_storage::JobStateCodecKind};

#[path = "simple_job.rs"]
mod simple_job;

#[tokio::main]
async fn main() -> Result<(), Error> {
    simple_job::run_simple_job(JobStateCodecKind::Cbor, "simple-cbor".to_string()).await
}
