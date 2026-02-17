//! `IceGate` Ingest binary
//!
//! Server app for ingesting OTLP data into `IceGate` observability data lake.

#![allow(clippy::print_stderr)]

use clap::Parser;
use icegate_ingest::{cli::Cli, error::Result};
use tokio::runtime::Builder;

fn main() -> Result<()> {
    let plan = icegate_ingest::runtime_threads::compute_runtime_threads();
    eprintln!(
        "TOKIO_MAIN_RUNTIME_THREADS={} TOKIO_SHIFT_RUNTIME_THREADS={} AVAILABLE_PARALLELISM={}",
        plan.main_threads, plan.shift_threads, plan.total
    );

    let runtime = Builder::new_multi_thread()
        .worker_threads(plan.main_threads)
        .enable_all()
        .build()
        .map_err(icegate_ingest::error::IngestError::Io)?;

    let cli = Cli::parse();
    runtime.block_on(cli.execute())
}
