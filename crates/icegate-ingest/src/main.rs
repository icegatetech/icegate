//! `IceGate` Ingest binary
//!
//! Server app for ingesting OTLP data into `IceGate` observability data lake.

use clap::Parser;
use icegate_ingest::{cli::Cli, error::Result};
use tokio::runtime::Builder;

fn main() -> Result<()> {
    let cli = Cli::parse();

    let plan = icegate_ingest::runtime_threads::compute_runtime_threads();

    let mut builder = Builder::new_multi_thread();
    builder
        .worker_threads(plan.main_threads)
        .thread_name("icegate-ingest-main".to_string())
        .enable_all();
    #[cfg(tokio_unstable)]
    builder.enable_metrics_poll_time_histogram();
    let runtime = builder.build().map_err(icegate_ingest::error::IngestError::Io)?;

    runtime.block_on(cli.execute())
}
