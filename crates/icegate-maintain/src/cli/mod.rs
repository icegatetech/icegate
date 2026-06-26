//! CLI module for IceGate maintain binary

use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::error::MaintainError;

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
    /// Run the long-running maintenance service (compaction).
    Run {
        /// Path to configuration file
        #[arg(short, long, value_name = "FILE")]
        config: PathBuf,
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
    pub async fn execute(self) -> Result<(), MaintainError> {
        match self.command {
            Commands::Migrate { command } => commands::migrate::execute(command).await,
            Commands::Run { config } => commands::run::execute(config).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Cli, Commands};

    #[test]
    fn parses_run_subcommand_with_config_path() {
        let cli = Cli::try_parse_from(["maintain", "run", "-c", "cfg.yaml"])
            .expect("`maintain run -c cfg.yaml` should parse");
        let Commands::Run { config } = cli.command else {
            panic!("expected the Run subcommand variant");
        };
        assert_eq!(config, std::path::PathBuf::from("cfg.yaml"));
    }
}
