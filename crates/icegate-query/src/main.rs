//! `IceGate` engine Query binary
//!
//! Server app for querying data from `IceGate` observability data lake.

#![allow(clippy::print_stderr)]

use clap::Parser;
use icegate_query::cli::Cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    if let Err(e) = cli.execute().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }

    Ok(())
}
