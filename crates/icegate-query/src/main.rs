//! `IceGate` engine Query binary
//!
//! Server app for querying data from `IceGate` observability data lake.

#![allow(clippy::print_stderr)]

use clap::Parser;
use icegate_query::cli::Cli;

// jemalloc installs itself as the global allocator, replacing malloc/free — which is
// exactly what a sanitizer runtime must intercept. The two cannot coexist, so
// scripts/sanitize.sh passes --cfg icegate_sanitize to opt back out.
#[cfg(all(target_os = "linux", not(icegate_sanitize)))]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Err(e) = cli.execute().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }

    Ok(())
}
