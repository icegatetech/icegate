//! `IceGate` Ingest binary
//!
//! Server app for ingesting OTLP data into `IceGate` observability data lake.

#![allow(clippy::print_stderr)]

use clap::Parser;
use icegate_ingest::{cli::Cli, error::Result};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse CLI arguments
    let cli = Cli::parse();
    cli.execute().await
}
