//! Migrate command implementation

use std::path::PathBuf;

use icegate_common::CatalogBuilder;

use crate::{
    cli::MigrateCommands,
    config::MaintainConfig,
    error::MaintainError,
    migrate::operations::{self, MigrationOperation},
};

/// Execute the migrate command
///
/// # Errors
///
/// Returns an error if migration fails
pub async fn execute(command: MigrateCommands) -> Result<(), MaintainError> {
    match command {
        MigrateCommands::Create {
            config,
            dry_run,
        } => create(config, dry_run).await,
        MigrateCommands::Upgrade {
            config,
            dry_run,
        } => upgrade(config, dry_run).await,
    }
}

/// Create all Iceberg tables
async fn create(config_path: PathBuf, dry_run: bool) -> Result<(), MaintainError> {
    tracing::info!("Loading configuration from {:?}", config_path);
    let config = MaintainConfig::from_file(&config_path).map_err(|e| MaintainError::Config(e.to_string()))?;

    tracing::info!("Initializing catalog");
    let catalog = CatalogBuilder::from_config(&config.catalog).await?;

    if dry_run {
        tracing::info!("Running in dry-run mode - no changes will be made");
    }

    let ops = operations::create_tables(&catalog, dry_run).await?;
    report_operations(&ops, dry_run);

    Ok(())
}

/// Upgrade existing table schemas
async fn upgrade(config_path: PathBuf, dry_run: bool) -> Result<(), MaintainError> {
    tracing::info!("Loading configuration from {:?}", config_path);
    let config = MaintainConfig::from_file(&config_path).map_err(|e| MaintainError::Config(e.to_string()))?;

    tracing::info!("Initializing catalog");
    let catalog = CatalogBuilder::from_config(&config.catalog).await?;

    if dry_run {
        tracing::info!("Running in dry-run mode - no changes will be made");
    }

    let ops = operations::upgrade_schemas(&catalog, dry_run).await?;
    report_operations(&ops, dry_run);

    Ok(())
}

/// Report migration operations to the user
fn report_operations(ops: &[MigrationOperation], dry_run: bool) {
    if ops.is_empty() {
        tracing::info!("No operations to perform");
        return;
    }

    let prefix = if dry_run { "Would" } else { "Completed" };
    for op in ops {
        log_operation(op, prefix);
    }

    let summary = if dry_run { "Would perform" } else { "Performed" };
    tracing::info!("{} {} operation(s)", summary, ops.len());
}

/// Log a single migration operation
fn log_operation(op: &MigrationOperation, prefix: &str) {
    match op {
        MigrationOperation::Create {
            table_name,
        } => {
            tracing::info!("{} create table: {}", prefix, table_name);
        },
        MigrationOperation::Upgrade {
            table_name,
        } => {
            tracing::info!("{} upgrade table: {}", prefix, table_name);
        },
    }
}
