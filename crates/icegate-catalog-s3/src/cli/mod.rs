//! Command-line interface for the standalone `CatalogServer`.

mod serve;

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tokio_util::sync::CancellationToken;

use crate::config::CatalogServerConfigError;
use crate::{CatalogServerConfig, S3Catalog};

/// Errors raised by the standalone `catalog` process.
///
/// Process-lifecycle failures (configuration, listener, server, stdout) are
/// their own variants rather than [`crate::Error`] ones: the library error
/// classifies catalog behaviour, and folding a TCP bind failure into it would
/// lie to a caller matching on that contract.
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// Server configuration failed to load or validate.
    #[error(transparent)]
    Config(#[from] CatalogServerConfigError),

    /// The listener socket could not be bound.
    #[error("failed to bind catalog listener: {0}")]
    Bind(#[source] std::io::Error),

    /// The REST server terminated with an error.
    #[error("catalog REST server failed: {0}")]
    Server(#[source] std::io::Error),

    /// The version could not be written to stdout.
    #[error("failed to write catalog version: {0}")]
    VersionWrite(#[source] std::io::Error),

    /// The catalog library reported a failure.
    #[error(transparent)]
    Catalog(#[from] crate::Error),
}

/// Standalone IceGate catalog process command line.
#[derive(Debug, Parser)]
#[command(name = "catalog", version, about = "IceGate read-only Iceberg REST Catalog server")]
pub struct CatalogCli {
    #[command(subcommand)]
    command: CatalogCommand,
}

/// Catalog process subcommands.
#[derive(Debug, Subcommand)]
enum CatalogCommand {
    /// Start the read-only `CatalogServer`.
    Serve {
        /// YAML or TOML server configuration file.
        #[arg(long)]
        config: PathBuf,
    },
    /// Print the catalog binary version.
    Version,
}

impl CatalogCli {
    /// Parse and execute the selected command.
    pub async fn execute(self) -> Result<(), CliError> {
        match self.command {
            CatalogCommand::Serve { config } => Self::serve(config).await,
            CatalogCommand::Version => Self::write_version(),
        }
    }

    async fn serve(config_path: PathBuf) -> Result<(), CliError> {
        let config = CatalogServerConfig::from_file(config_path)?;
        let cancellation = CancellationToken::new();
        let s3_config = config.to_s3_catalog_config()?;
        let file_io = config.build_file_io()?;
        let catalog = Arc::new(S3Catalog::new(&s3_config, file_io, cancellation.clone())?);
        serve::run(config, catalog, cancellation).await
    }

    fn write_version() -> Result<(), CliError> {
        let mut stdout = std::io::stdout().lock();
        writeln!(stdout, "{}", env!("CARGO_PKG_VERSION")).map_err(CliError::VersionWrite)
    }
}
