//! `IceGate` engine Query binary
//!
//! Server app for querying data from `IceGate` observability data lake.

#![allow(clippy::print_stderr)]

use clap::Parser;
use icegate_query::cli::Cli;

#[cfg(target_os = "linux")]
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
