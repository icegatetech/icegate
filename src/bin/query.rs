//! `IceGate` engine Query binary
//!
//! Server app for querying data from `IceGate` observability data lake.

#![allow(clippy::print_stderr)]

use clap::Parser;
use icegate::query::cli::Cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")),
        )
        .init();

    // Parse CLI arguments
    let cli = Cli::parse();

    // Execute command
    if let Err(e) = cli.execute().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }

    Ok(())
}
