//! Migrate command implementation

use std::path::PathBuf;

use icegate_common::{CancellationToken, CatalogBuilder, IoHandle};

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
        MigrateCommands::Create { config, dry_run } => create(config, dry_run).await,
        MigrateCommands::Upgrade { config, dry_run } => upgrade(config, dry_run).await,
    }
}

/// Create all Iceberg tables
async fn create(config_path: PathBuf, dry_run: bool) -> Result<(), MaintainError> {
    tracing::info!("Loading configuration from {:?}", config_path);
    let config = MaintainConfig::from_file(&config_path).map_err(|e| MaintainError::Config(e.to_string()))?;

    tracing::info!("Initializing catalog");
    let catalog = CatalogBuilder::from_config(&config.catalog, &IoHandle::noop(), CancellationToken::new()).await?;

    if dry_run {
        tracing::info!("Running in dry-run mode - no changes will be made");
    }

    let ops = operations::create_tables(&catalog, &config.snapshot_expiration, dry_run).await?;
    report_operations(&ops, dry_run);

    Ok(())
}

/// Upgrade existing table schemas
async fn upgrade(config_path: PathBuf, dry_run: bool) -> Result<(), MaintainError> {
    tracing::info!("Loading configuration from {:?}", config_path);
    let config = MaintainConfig::from_file(&config_path).map_err(|e| MaintainError::Config(e.to_string()))?;

    tracing::info!("Initializing catalog");
    let catalog = CatalogBuilder::from_config(&config.catalog, &IoHandle::noop(), CancellationToken::new()).await?;

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
    tracing::info!("{} {} operation(s)", summary, count_table_changes(ops));
}

/// How many of `ops` change a table; a table left for manual migration changes nothing.
fn count_table_changes(ops: &[MigrationOperation]) -> usize {
    ops.iter()
        .filter(|op| !matches!(op, MigrationOperation::ManualMigrationRequired { .. }))
        .count()
}

/// Log a single migration operation
fn log_operation(op: &MigrationOperation, prefix: &str) {
    match op {
        MigrationOperation::Create { table_name } => {
            tracing::info!("{} create table: {}", prefix, table_name);
        }
        MigrationOperation::Upgrade { table_name } => {
            tracing::info!("{} upgrade table: {}", prefix, table_name);
        }
        MigrationOperation::ManualMigrationRequired { table_name, reason } => {
            tracing::warn!("Table {} requires manual migration: {}", table_name, reason);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_table_left_for_manual_migration_is_not_counted_as_a_change() {
        let ops = [
            MigrationOperation::Create {
                table_name: "logs".to_string(),
            },
            MigrationOperation::Upgrade {
                table_name: "spans".to_string(),
            },
            MigrationOperation::ManualMigrationRequired {
                table_name: "events".to_string(),
                reason: "retyped column".to_string(),
            },
        ];

        assert_eq!(count_table_changes(&ops), 2);
    }
}
