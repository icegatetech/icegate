//! `IceGate` engine Maintain binary
//!
//! CLI tool for maintenance operations on `IceGate` observability data lake.

#![allow(clippy::print_stderr)]

use clap::Parser;
use icegate_common::{TracingConfig, init_tracing};
use icegate_maintain::cli::{Cli, Commands};

// Use jemalloc on Linux for the long-running `run` compaction service: glibc's
// default malloc fragments its per-thread arenas across repeated rewrite cycles
// and rarely returns memory to the OS, producing the staircase RSS growth seen
// in the container. jemalloc reclaims aggressively via background threads.
// Mirrors the ingest and query binaries.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse CLI arguments first so tracing can be gated on the subcommand.
    let cli = Cli::parse();

    // Exhaustive on purpose: a new subcommand must decide where its subscriber
    // comes from, and the compiler is what forces that decision — an
    // uninstrumented binary logs nothing and fails silently.
    let _guard = match &cli.command {
        // The one-shot `migrate` commands carry no `tracing` config block and
        // report via their own stdout/stderr output, so they get the plain JSON
        // logger here (tracing disabled = no OTLP exporter).
        Commands::Migrate { .. } => Some(init_tracing(&TracingConfig {
            enabled: false,
            ..TracingConfig::default()
        })?),
        // The long-running `run` service initialises the subscriber from
        // `MaintainConfig::tracing` once it has read its config file, mirroring
        // ingest and query — nothing is installed here: the first `init_tracing`
        // call wins.
        Commands::Run { .. } => None,
    };

    // Execute command
    if let Err(e) = cli.execute().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }

    Ok(())
}
