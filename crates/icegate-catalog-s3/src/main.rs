//! Standalone IceGate `CatalogServer` binary.

use clap::Parser;
use icegate_catalog_s3::cli::CatalogCli;

/// Parse CLI arguments and run the selected `CatalogServer` command.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let environment_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(environment_filter).try_init()?;
    CatalogCli::parse().execute().await?;
    Ok(())
}
