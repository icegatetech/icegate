//! CLI module for IceGate query binary

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use icegate_common::errors::IceGateError;

mod commands;

/// `IceGate` - Observability data lake query engine
#[derive(Parser)]
#[command(name = "query")]
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

    /// Run `IceGate` servers
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
    pub async fn execute(self) -> Result<(), IceGateError> {
        match self.command {
            Commands::Version => commands::version::execute(),
            Commands::Run {
                config,
            } => commands::run::execute(config).await,
        }
    }
}
