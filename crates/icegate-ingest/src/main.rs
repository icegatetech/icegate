//! `IceGate` Ingest binary
//!
//! Server app for ingesting OTLP data into `IceGate` observability data lake.

use clap::Parser;
use icegate_ingest::{cli::Cli, error::Result};
use tokio::runtime::Builder;

fn main() -> Result<()> {
    let cli = Cli::parse();

    let plan = icegate_ingest::runtime_threads::compute_runtime_threads();

    let runtime = Builder::new_multi_thread()
        .worker_threads(plan.main_threads)
        .enable_all()
        .build()
        .map_err(icegate_ingest::error::IngestError::Io)?;

    runtime.block_on(cli.execute())
}
