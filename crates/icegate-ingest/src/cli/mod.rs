//! CLI module for IceGate ingest binary

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::error::IngestError;

mod commands;

/// `IceGate` - Observability data lake OTLP ingestion
#[derive(Parser)]
#[command(name = "ingest")]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Subcommand to execute
    #[command(subcommand)]
    pub command: Commands,
}

/// Available CLI commands
#[derive(Subcommand)]
pub enum Commands {
    /// Show version information
    Version,

    /// Run OTLP ingestion servers
    Run {
        /// Path to configuration file
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
    },
}

impl Cli {
    /// Execute the CLI command
    ///
    /// # Errors
    ///
    /// Returns an error if the command execution fails
    pub async fn execute(self) -> Result<(), IngestError> {
        match self.command {
            Commands::Version => commands::version::execute(),
            Commands::Run {
                config,
            } => commands::run::execute(config).await,
        }
    }
}
