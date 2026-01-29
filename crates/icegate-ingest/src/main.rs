//! `IceGate` Ingest binary
//!
//! Server app for ingesting OTLP data into `IceGate` observability data lake.

#![allow(clippy::print_stderr)]

use clap::Parser;
use icegate_ingest::{cli::Cli, error::Result};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    cli.execute().await
}
