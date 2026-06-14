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

    // The one-shot `migrate` commands keep tracing disabled (they report via
    // their own stdout/stderr output), while the long-running `run` service
    // must enable tracing so its spans and logs are emitted.
    let _guard = init_tracing(&TracingConfig {
        enabled: matches!(cli.command, Commands::Run { .. }),
        ..TracingConfig::default()
    })?;

    // Execute command
    if let Err(e) = cli.execute().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }

    Ok(())
}
