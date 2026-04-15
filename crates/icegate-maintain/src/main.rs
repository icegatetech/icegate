//! `IceGate` engine Maintain binary
//!
//! CLI tool for maintenance operations on `IceGate` observability data lake.

#![allow(clippy::print_stderr)]

use clap::Parser;
use icegate_common::{TracingConfig, init_tracing};
use icegate_maintain::cli::Cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing via the shared initializer (JSON format, no OTEL export)
    let _guard = init_tracing(&TracingConfig {
        enabled: false,
        ..TracingConfig::default()
    })?;

    // Parse CLI arguments
    let cli = Cli::parse();

    // Execute command
    if let Err(e) = cli.execute().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }

    Ok(())
}
