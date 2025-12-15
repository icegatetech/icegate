//! CLI module for IceGate maintain binary

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use icegate_common::errors::IceGateError;

mod commands;

/// `IceGate` - Observability data lake maintenance operations
#[derive(Parser)]
#[command(name = "maintain")]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Subcommand to execute
    #[command(subcommand)]
    pub command: Commands,
}

/// Available CLI commands
#[derive(Subcommand)]
pub enum Commands {
    /// Schema migration operations
    Migrate {
        /// Migration subcommand
        #[command(subcommand)]
        command: MigrateCommands,
    },
}

/// Migration subcommands
#[derive(Subcommand)]
pub enum MigrateCommands {
    /// Create all Iceberg tables
    Create {
        /// Path to configuration file
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        /// Show what would be done without executing
        #[arg(long, default_value = "false")]
        dry_run: bool,
    },
    /// Upgrade existing table schemas
    Upgrade {
        /// Path to configuration file
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
        /// Show what would be done without executing
        #[arg(long, default_value = "false")]
        dry_run: bool,
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
            Commands::Migrate {
                command,
            } => commands::migrate::execute(command).await,
        }
    }
}
