//! `IceGate` Ingest binary
//!
//! Server app for ingesting OTLP data into `IceGate` observability data lake.

use clap::Parser;
use icegate_ingest::{cli::Cli, error::Result};
use tokio::runtime::Builder;

// jemalloc installs itself as the global allocator, replacing malloc/free — which is
// exactly what a sanitizer runtime must intercept. The two cannot coexist, so
// scripts/sanitize.sh passes --cfg icegate_sanitize to opt back out.
#[cfg(all(target_os = "linux", not(icegate_sanitize)))]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> Result<()> {
    let cli = Cli::parse();

    let plan = icegate_ingest::runtime_threads::compute_runtime_threads();

    let mut builder = Builder::new_multi_thread();
    builder
        .worker_threads(plan.main_threads)
        .thread_name("icegate-ingest-main")
        .enable_all();
    #[cfg(tokio_unstable)]
    builder.enable_metrics_poll_time_histogram();
    let runtime = builder.build().map_err(icegate_ingest::error::IngestError::Io)?;

    runtime.block_on(cli.execute())
}
