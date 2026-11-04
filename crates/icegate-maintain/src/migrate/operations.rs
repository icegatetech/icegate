//! Migration operations

use std::{collections::HashMap, sync::Arc};

use iceberg::spec::{NestedField, NestedFieldRef, TableProperties};
use iceberg::table::Table;
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use icegate_common::{
    EVENTS_TABLE, ICEGATE_NAMESPACE, LOGS_TABLE, METRICS_TABLE, OPERATIONS_TABLE, PRICES_TABLE, SPANS_TABLE,
    WAL_OFFSET_PROPERTY,
    schema::{
        events_partition_spec, events_schema, events_sort_order, logs_partition_spec, logs_schema, logs_sort_order,
        metrics_partition_spec, metrics_schema, metrics_sort_order, operations_partition_spec, operations_schema,
        operations_sort_order, prices_partition_spec, prices_schema, prices_sort_order, spans_partition_spec,
        spans_schema, spans_sort_order,
    },
};

use crate::error::Result;
use crate::migrate::config::SnapshotExpirationConfig;

/// Migration operation types
#[derive(Debug, Clone)]
pub enum MigrationOperation {
    /// Create a new table
    Create {
        /// Name of the table to create
        table_name: String,
    },
    /// Upgrade an existing table schema
    Upgrade {
        /// Name of the table to upgrade
        table_name: String,
    },
    /// A table a real run would refuse: appending columns cannot bring it to
    /// the code's schema. Only a dry run of [`upgrade_schemas`] reports it; a
    /// real run returns an error instead.
    ManualMigrationRequired {
        /// Name of the table.
        table_name: String,
        /// Why appending columns cannot resolve the difference.
        reason: String,
    },
}

/// Table schema definition with partition spec and sort order
struct TableDefinition {
    name: &'static str,
    schema: iceberg::spec::Schema,
    partition_spec: iceberg::spec::PartitionSpec,
    sort_order: iceberg::spec::SortOrder,
    /// Whether this table's snapshots record the WAL queue offset in their
    /// summary. True for every table the Shifter writes, whose readers resolve
    /// that offset by walking the ancestor chain — expiration must be told to
    /// keep the chain down to its most recent carrier intact.
    carries_wal_offset: bool,
}

/// Build table definitions from schema module
fn build_table_definitions() -> Result<Vec<TableDefinition>> {
    let mut definitions = Vec::with_capacity(6);

    // Logs table
    let logs = logs_schema()?;
    let logs_partition = logs_partition_spec(&logs)?;
    let logs_sort = logs_sort_order(&logs)?;
    definitions.push(TableDefinition {
        name: LOGS_TABLE,
        schema: logs,
        partition_spec: logs_partition,
        sort_order: logs_sort,
        carries_wal_offset: true,
    });

    // Spans table
    let spans = spans_schema()?;
    let spans_partition = spans_partition_spec(&spans)?;
    let spans_sort = spans_sort_order(&spans)?;
    definitions.push(TableDefinition {
        name: SPANS_TABLE,
        schema: spans,
        partition_spec: spans_partition,
        sort_order: spans_sort,
        carries_wal_offset: true,
    });

    // Events table
    let events = events_schema()?;
    let events_partition = events_partition_spec(&events)?;
    let events_sort = events_sort_order(&events)?;
    definitions.push(TableDefinition {
        name: EVENTS_TABLE,
        schema: events,
        partition_spec: events_partition,
        sort_order: events_sort,
        carries_wal_offset: true,
    });

    // Metrics table
    let metrics = metrics_schema()?;
    let metrics_partition = metrics_partition_spec(&metrics)?;
    let metrics_sort = metrics_sort_order(&metrics)?;
    definitions.push(TableDefinition {
        name: METRICS_TABLE,
        schema: metrics,
        partition_spec: metrics_partition,
        sort_order: metrics_sort,
        carries_wal_offset: true,
    });

    // Operations table (LLM/GenAI projection over spans — the 5th physical table).
    let operations = operations_schema()?;
    let operations_partition = operations_partition_spec(&operations)?;
    let operations_sort = operations_sort_order(&operations)?;
    definitions.push(TableDefinition {
        name: OPERATIONS_TABLE,
        schema: operations,
        partition_spec: operations_partition,
        sort_order: operations_sort,
        carries_wal_offset: true,
    });

    // Prices table (global LLM rate card — the 6th physical table, and the only
    // one without `tenant_id` or partitioning).
    let prices = prices_schema()?;
    let prices_partition = prices_partition_spec(&prices)?;
    let prices_sort = prices_sort_order(&prices)?;
    definitions.push(TableDefinition {
        name: PRICES_TABLE,
        schema: prices,
        partition_spec: prices_partition,
        sort_order: prices_sort,
        // The pricing crawler writes this table, not the Shifter: its snapshots
        // carry no WAL offset, so expiration has no carrier to preserve here.
        carries_wal_offset: false,
    });

    Ok(definitions)
}

/// Create all observability tables in the catalog
///
/// Creates the following tables in the icegate namespace:
/// - logs: `OpenTelemetry` log records
/// - spans: Distributed trace spans
/// - events: Semantic events extracted from logs
/// - metrics: All metric types (gauge, sum, histogram, summary)
/// - operations: LLM/`GenAI` typed projection over spans
/// - prices: global LLM rate card (no `tenant_id`, unpartitioned)
///
/// Each table is created with appropriate partition specs and sort orders
/// for optimal query performance, and with the `expiration` policy written into
/// its properties — that stamp is what makes any later writer expire this
/// table's snapshots, so a table created without it keeps its history forever.
/// Existing tables are left untouched, properties included, with one
/// exception: an existing table gains the trailing optional columns the code
/// has added since it was created, reported as [`MigrationOperation::Upgrade`]
/// (see [`add_missing_columns`]). Deployments run only this command on
/// upgrade, so this is the path by which such a column reaches them.
///
/// # Return Value
///
/// Returns a list of operations. In dry-run mode, operations represent what
/// *would* happen, not what actually occurred.
///
/// # Errors
///
/// Returns an error if:
/// - `expiration` is out of the bounds of [`SnapshotExpirationConfig::validate`]
/// - Schema construction fails
/// - Namespace creation fails
/// - Table creation fails (except for "already exists" errors)
/// - Loading an existing table, or committing columns added to it, fails
/// - An existing table's schema differs from the code in a way appending
///   optional columns cannot resolve, or appending would give a column other
///   field ids than the schema module does
///   ([`crate::error::MaintainError::Migration`], naming every such table with
///   its reason; every other table is still created or evolved first)
#[allow(clippy::cognitive_complexity)]
pub async fn create_tables(
    catalog: &Arc<dyn Catalog>,
    expiration: &SnapshotExpirationConfig,
    dry_run: bool,
) -> Result<Vec<MigrationOperation>> {
    // A policy is stamped once and never re-stamped, so an out-of-range window
    // reaching a table's properties fails every later commit against it — ingest
    // included. The CLI validates the whole config, but this is a public entry
    // point and the stamp is what it exists to write.
    expiration.validate()?;

    let catalog_ref = catalog.as_ref();
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());

    // Ensure namespace exists
    if !dry_run {
        ensure_namespace_exists(catalog_ref, &namespace).await?;
    }

    let definitions = build_table_definitions()?;
    let mut operations = Vec::new();
    // Every other table is still created or evolved, so one incompatible table
    // does not hold the rest back and the error names all of them at once.
    let mut differing_tables: Vec<DifferingTable> = Vec::new();

    for def in definitions {
        let table_ident = TableIdent::new(namespace.clone(), def.name.to_string());

        // Check if table already exists
        let exists = catalog_ref.table_exists(&table_ident).await?;

        if exists {
            let table = catalog_ref.load_table(&table_ident).await?;
            match add_missing_columns(catalog_ref, &def, &table, dry_run).await? {
                ColumnAddition::ColumnsAdded => operations.push(MigrationOperation::Upgrade {
                    table_name: def.name.to_string(),
                }),
                ColumnAddition::SchemaCurrent => {
                    tracing::info!("Table {} already exists with the code's schema, skipping", def.name);
                }
                ColumnAddition::Unresolvable(drift) => {
                    log_schema_differences(def.name, table.metadata().current_schema(), &def.schema);
                    differing_tables.push(DifferingTable {
                        table_name: def.name.to_string(),
                        drift,
                    });
                }
            }
            continue;
        }

        if dry_run {
            tracing::info!("[dry-run] Would create table: {}", def.name);
        } else {
            tracing::info!("Creating table: {}", def.name);
            create_table(catalog_ref, &namespace, &def, expiration).await?;
            tracing::info!("Created table: {}", def.name);
        }

        operations.push(MigrationOperation::Create {
            table_name: def.name.to_string(),
        });
    }

    if !differing_tables.is_empty() {
        return Err(crate::error::MaintainError::Migration(format_upgrade_required_message(
            &differing_tables,
        )));
    }

    Ok(operations)
}

/// Ensure the namespace exists, creating it if necessary
async fn ensure_namespace_exists(catalog: &dyn Catalog, namespace: &NamespaceIdent) -> Result<()> {
    let exists = catalog.namespace_exists(namespace).await?;

    if !exists {
        tracing::info!("Creating namespace: {}", namespace);
        let properties: HashMap<String, String> = HashMap::from([
            ("owner".to_string(), "icegate".to_string()),
            ("description".to_string(), "IceGate observability data".to_string()),
        ]);
        catalog.create_namespace(namespace, properties).await?;
    }

    Ok(())
}

/// Create a single table in the catalog
async fn create_table(
    catalog: &dyn Catalog,
    namespace: &NamespaceIdent,
    def: &TableDefinition,
    expiration: &SnapshotExpirationConfig,
) -> Result<()> {
    let mut properties = expiration.build_table_properties(def.carries_wal_offset.then_some(WAL_OFFSET_PROPERTY));
    properties.extend([
        (
            TableProperties::PROPERTY_DEFAULT_FILE_FORMAT.to_string(),
            "parquet".to_string(),
        ),
        ("write.parquet.compression-codec".to_string(), "zstd".to_string()),
    ]);

    let table_creation = TableCreation::builder()
        .name(def.name.to_string())
        .schema(def.schema.clone())
        .partition_spec(def.partition_spec.clone())
        .sort_order(def.sort_order.clone())
        .properties(properties)
        .build();

    catalog.create_table(namespace, table_creation).await?;

    Ok(())
}

/// Report schema differences between the catalog and the code.
///
/// Checks each observability table for schema differences. The only
/// automatic evolution is the one [`create_tables`] also performs: appending
/// the code's new trailing optional columns to an existing table, reported as
/// [`MigrationOperation::Upgrade`]. Any other difference, including columns
/// the table would give other field ids than the schema module does, requires
/// operators to drop and re-create the affected table(s).
///
/// # Behavior
///
/// - **Dry-run mode**: logs differences for every mismatched table and
///   returns `Ok(operations)` with one entry per mismatch:
///   `MigrationOperation::Upgrade` when appending columns would resolve it,
///   `MigrationOperation::ManualMigrationRequired` when a real run would
///   refuse the table.
/// - **Non-dry-run mode**: appends columns to every table where that
///   resolves the mismatch, logs differences for every other mismatched
///   table discovered during the scan, then returns
///   `Err(MaintainError::Migration)` with an aggregated message naming
///   all tables that must be dropped and re-created, each with its reason. Tables that match the
///   code's schema pass silently; missing tables log a warning and are
///   skipped (operator must run `migrate create` first).
///
/// # Return Value
///
/// `Ok(Vec<MigrationOperation>)` in dry-run mode, or when every table is up
/// to date once columns were appended. Non-dry-run with at least one
/// mismatch that appending cannot resolve returns
/// `Err(MaintainError::Migration(..))`, after appending columns to every
/// table where that resolved the mismatch.
///
/// # Errors
///
/// Returns an error if:
/// - The catalog rejects a `table_exists` or `load_table` call, or a commit
///   appending columns.
/// - One or more tables have schemas that differ from the code in a way
///   appending columns cannot resolve, or appending would give a column other
///   field ids than the schema module does, and we're not in dry-run mode
///   (see `MaintainError::Migration`).
#[allow(clippy::cognitive_complexity)]
pub async fn upgrade_schemas(catalog: &Arc<dyn Catalog>, dry_run: bool) -> Result<Vec<MigrationOperation>> {
    let catalog_ref = catalog.as_ref();
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    let definitions = build_table_definitions()?;
    let mut operations = Vec::new();
    // Collect every mismatched table instead of bailing on the first one so
    // operators see the full remediation list in a single run.
    let mut differing_tables: Vec<DifferingTable> = Vec::new();

    for def in definitions {
        let table_ident = TableIdent::new(namespace.clone(), def.name.to_string());

        // Check if table exists
        let exists = catalog_ref.table_exists(&table_ident).await?;

        if !exists {
            tracing::warn!(
                "Table {} does not exist, skipping upgrade. Run 'migrate create' first.",
                def.name
            );
            continue;
        }

        // Load table and check schema
        let table = catalog_ref.load_table(&table_ident).await?;

        let current_schema = table.metadata().current_schema();
        let target_schema = &def.schema;

        // Compare schemas
        let needs_upgrade = schemas_differ(current_schema, target_schema);

        if needs_upgrade {
            log_schema_differences(def.name, current_schema, target_schema);
            let drift = match add_missing_columns(catalog_ref, &def, &table, dry_run).await? {
                ColumnAddition::ColumnsAdded => {
                    operations.push(MigrationOperation::Upgrade {
                        table_name: def.name.to_string(),
                    });
                    continue;
                }
                // `schemas_differ` found a difference, so appending nothing
                // cannot leave the table on the code's schema.
                ColumnAddition::SchemaCurrent => UnresolvableDrift::IncompatibleSchema,
                ColumnAddition::Unresolvable(drift) => drift,
            };
            if dry_run {
                operations.push(MigrationOperation::ManualMigrationRequired {
                    table_name: def.name.to_string(),
                    reason: drift.to_string(),
                });
            } else {
                differing_tables.push(DifferingTable {
                    table_name: def.name.to_string(),
                    drift,
                });
            }
        } else {
            tracing::info!("Table {} schema is up to date", def.name);
        }
    }

    if !dry_run && !differing_tables.is_empty() {
        return Err(crate::error::MaintainError::Migration(format_upgrade_required_message(
            &differing_tables,
        )));
    }

    Ok(operations)
}

/// Build the operator-facing error message when one or more tables' schemas
/// in the catalog differ from the code in a way appending trailing optional
/// columns cannot resolve. Accepts one or more tables, uses singular/plural wording
/// based on the count, joins names with `, ` for the bracketed list, and states
/// each table's reason, so the operator sees the full remediation list in one run.
fn format_upgrade_required_message(differing_tables: &[DifferingTable]) -> String {
    let (subject, remediation) = if differing_tables.len() == 1 {
        ("Table", "the table")
    } else {
        ("Tables", "the tables")
    };
    let list = differing_tables
        .iter()
        .map(|table| table.table_name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let reasons = differing_tables
        .iter()
        .map(|table| format!("{}: {}", table.table_name, table.drift))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "{subject} [{list}] schema in catalog differs from code ({reasons}). Only optional columns \
         appended at the end, with the field ids the table assigns them, are applied automatically; \
         drop {remediation} manually and re-run `icegate-maintain migrate create`."
    )
}

/// The fields `target` appends to `current`, when appending optional fields
/// is the whole of the difference; `None` for any other drift.
///
/// Every `current` field must match the `target` field at the same position
/// by name, type, and required flag, so only columns the code appends after
/// its last existing one are planned. An added field must be optional because
/// rows already in the table have no value for it. An empty slice means there
/// is nothing to add.
fn plan_column_additions<'a>(
    current: &iceberg::spec::Schema,
    target: &'a iceberg::spec::Schema,
) -> Option<&'a [NestedFieldRef]> {
    let current_fields = current.as_struct().fields();
    let (kept, added) = target.as_struct().fields().split_at_checked(current_fields.len())?;
    let is_prefix = current_fields.iter().zip(kept).all(|(current_field, target_field)| {
        current_field.name == target_field.name
            && current_field.required == target_field.required
            && types_equal(&current_field.field_type, &target_field.field_type)
    });
    (is_prefix && added.iter().all(|field| !field.required)).then_some(added)
}

/// Snapshot summary property naming the columns a schema-only commit added.
///
/// The property is what lets that commit carry a snapshot at all: an append
/// with neither data files nor properties is refused.
const ADDED_COLUMNS_PROPERTY: &str = "icegate.added-columns";

/// What [`add_missing_columns`] found, and in non-dry-run did, to an existing table.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ColumnAddition {
    /// The trailing optional columns of the code's schema were appended (in
    /// dry-run: would be).
    ColumnsAdded,
    /// The table already has the code's schema; nothing was touched.
    SchemaCurrent,
    /// Appending columns cannot bring the table to the code's schema; nothing
    /// was touched.
    Unresolvable(UnresolvableDrift),
}

/// Why appending columns cannot bring an existing table to the code's schema.
#[derive(Debug, Clone, PartialEq, Eq)]
enum UnresolvableDrift {
    /// The drift is not a set of trailing optional columns.
    IncompatibleSchema,
    /// The table would give a field of an appended column another id than the
    /// schema module does (see [`find_added_field_id_mismatch`]).
    FieldIdMismatch(FieldIdMismatch),
}

impl std::fmt::Display for UnresolvableDrift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncompatibleSchema => {
                f.write_str("the schema differs from the code beyond optional columns appended at the end")
            }
            Self::FieldIdMismatch(mismatch) => write!(
                f,
                "column '{}': field '{}' has id {} in the schema module, but the table would assign it {}",
                mismatch.column_name, mismatch.field_name, mismatch.code_id, mismatch.table_id
            ),
        }
    }
}

/// An existing table that appending columns cannot bring to the code's schema.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DifferingTable {
    table_name: String,
    drift: UnresolvableDrift,
}

/// Append to the existing `table` the optional columns `def`'s schema adds,
/// in one commit.
///
/// Rows already in the table read the new columns as NULL. The same commit
/// adds a snapshot with no data, so the current snapshot names the evolved
/// schema: scans project the current snapshot's schema, and without it every
/// reader would keep the old columns until the next append. That snapshot
/// carries the WAL offset of the snapshot it supersedes, resolved against the
/// base the commit lands on (pinned in the `icegatetech/iceberg-rust` fork by
/// `test_fast_append_inherits_summary_property_from_the_base_it_commits_on`),
/// so a reader resolving the offset from the current snapshot alone still
/// finds it, even when a shift commit lands first.
///
/// A table that carries the WAL offset and has no snapshot yet gets the
/// schema change alone: a stamp there would be its only snapshot, with no
/// offset to inherit, and `resolve_committed_offset` refuses such a table
/// instead of resuming the WAL from its start. The price is a race: a first
/// shift append committed concurrently with the migration, on the previous
/// schema, stays current, and readers project the previous columns until the
/// next append. A table without the WAL offset (`prices`) is stamped even with
/// no snapshot, which closes that race for it.
///
/// Columns whose field ids the table would assign differently from the schema
/// module (see [`find_added_field_id_mismatch`]) are not appended: the result
/// is [`ColumnAddition::Unresolvable`], in dry-run as well.
///
/// Only the schema changes otherwise: the table keeps the partition spec and
/// sort order it was created with.
async fn add_missing_columns(
    catalog: &dyn Catalog,
    def: &TableDefinition,
    table: &Table,
    dry_run: bool,
) -> Result<ColumnAddition> {
    let Some(columns) = plan_column_additions(table.metadata().current_schema(), &def.schema) else {
        return Ok(ColumnAddition::Unresolvable(UnresolvableDrift::IncompatibleSchema));
    };
    if columns.is_empty() {
        return Ok(ColumnAddition::SchemaCurrent);
    }
    if let Some(mismatch) = find_added_field_id_mismatch(columns, table.metadata().last_column_id()) {
        return Ok(ColumnAddition::Unresolvable(UnresolvableDrift::FieldIdMismatch(
            mismatch,
        )));
    }
    let names: Vec<&str> = columns.iter().map(|field| field.name.as_str()).collect();
    if dry_run {
        tracing::info!("[dry-run] Would add columns {:?} to table {}", names, def.name);
        return Ok(ColumnAddition::ColumnsAdded);
    }

    let tx = Transaction::new(table);
    let action = columns.iter().fold(tx.update_schema(), |action, field| {
        action.add_column(AddColumn::optional(&field.name, field.field_type.as_ref().clone()))
    });
    let tx = action.apply(tx)?;
    let tx = if def.carries_wal_offset && table.metadata().current_snapshot().is_none() {
        tx
    } else {
        // Actions apply in order within one commit, so this snapshot is stamped
        // with the schema the action above adds.
        let stamp = tx
            .fast_append()
            .set_snapshot_properties(HashMap::from([(ADDED_COLUMNS_PROPERTY.to_string(), names.join(","))]))
            .inherit_summary_property(WAL_OFFSET_PROPERTY);
        stamp.apply(tx)?
    };
    tx.commit(catalog).await?;
    tracing::info!("Added columns {:?} to table {}", names, def.name);
    Ok(ColumnAddition::ColumnsAdded)
}

/// A field of an added column whose id in the schema module differs from the
/// id the table assigns it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldIdMismatch {
    /// Name of the appended column.
    column_name: String,
    /// Name of the field: the column itself or one nested in it.
    field_name: String,
    /// Id the schema module gives the field.
    code_id: i32,
    /// Id the table assigns the field.
    table_id: i32,
}

/// The first field of `columns` that the table would give another id than the
/// schema module does; `None` when every id matches.
///
/// The table assigns an added column fresh ids, continuing from its
/// `last_column_id` in pre-order: a field takes the next id, then its nested
/// fields take theirs, a map's key before its value. The WAL and the Shifter
/// write Arrow data tagged with the schema module's ids, nested fields
/// included, and a batch whose nested ids differ from the table's never takes
/// the table's schema, so such a column must not be appended.
fn find_added_field_id_mismatch(columns: &[NestedFieldRef], last_column_id: i32) -> Option<FieldIdMismatch> {
    let mut next_id = last_column_id;
    columns
        .iter()
        .find_map(|column| find_field_id_mismatch(&column.name, column, &mut next_id))
}

/// The first field of `field`, in pre-order, whose id is not the id the table
/// assigns it; `next_id` is the last id assigned before `field` and is
/// advanced past every field visited.
fn find_field_id_mismatch(column_name: &str, field: &NestedField, next_id: &mut i32) -> Option<FieldIdMismatch> {
    use iceberg::spec::Type;

    *next_id = next_id.saturating_add(1);
    if field.id != *next_id {
        return Some(FieldIdMismatch {
            column_name: column_name.to_string(),
            field_name: field.name.clone(),
            code_id: field.id,
            table_id: *next_id,
        });
    }
    match field.field_type.as_ref() {
        Type::Primitive(_) | Type::Variant(_) => None,
        Type::Struct(struct_type) => struct_type
            .fields()
            .iter()
            .find_map(|nested| find_field_id_mismatch(column_name, nested, next_id)),
        Type::List(list_type) => find_field_id_mismatch(column_name, &list_type.element_field, next_id),
        Type::Map(map_type) => find_field_id_mismatch(column_name, &map_type.key_field, next_id)
            .or_else(|| find_field_id_mismatch(column_name, &map_type.value_field, next_id)),
    }
}

/// Check if two schemas differ
fn schemas_differ(current: &iceberg::spec::Schema, target: &iceberg::spec::Schema) -> bool {
    let current_fields = current.as_struct().fields();
    let target_fields = target.as_struct().fields();

    // Compare field counts
    if current_fields.len() != target_fields.len() {
        return true;
    }

    // Compare each field
    for target_field in target_fields {
        match current.field_by_name(target_field.name.as_str()) {
            Some(current_field) => {
                // Check if types match structurally (ignoring field IDs)
                if !types_equal(&current_field.field_type, &target_field.field_type) {
                    return true;
                }
                // Check required flag
                if current_field.required != target_field.required {
                    return true;
                }
            }
            None => {
                // Field doesn't exist in current schema
                return true;
            }
        }
    }

    false
}

/// Compare two Iceberg types for structural equality (ignoring field IDs).
///
/// This is needed because schemas loaded from the catalog may have different
/// field IDs than freshly constructed schemas, even when semantically
/// identical.
fn types_equal(a: &iceberg::spec::Type, b: &iceberg::spec::Type) -> bool {
    use iceberg::spec::Type;

    match (a, b) {
        (Type::Primitive(pa), Type::Primitive(pb)) => pa == pb,
        (Type::Struct(sa), Type::Struct(sb)) => {
            let fields_a = sa.fields();
            let fields_b = sb.fields();
            if fields_a.len() != fields_b.len() {
                return false;
            }
            // Compare fields by name and type (ignoring IDs)
            for (fa, fb) in fields_a.iter().zip(fields_b.iter()) {
                if fa.name != fb.name || fa.required != fb.required || !types_equal(&fa.field_type, &fb.field_type) {
                    return false;
                }
            }
            true
        }
        (Type::List(la), Type::List(lb)) => {
            la.element_field.required == lb.element_field.required
                && types_equal(&la.element_field.field_type, &lb.element_field.field_type)
        }
        (Type::Map(ma), Type::Map(mb)) => {
            types_equal(&ma.key_field.field_type, &mb.key_field.field_type)
                && ma.value_field.required == mb.value_field.required
                && types_equal(&ma.value_field.field_type, &mb.value_field.field_type)
        }
        _ => false, // Different type variants
    }
}

/// Log differences between two schemas
#[allow(clippy::cognitive_complexity)]
fn log_schema_differences(table_name: &str, current: &iceberg::spec::Schema, target: &iceberg::spec::Schema) {
    tracing::info!("Schema differences for table {}:", table_name);

    let current_fields = current.as_struct().fields();
    let target_fields = target.as_struct().fields();

    // Find new fields
    for target_field in target_fields {
        if current.field_by_name(target_field.name.as_str()).is_none() {
            tracing::info!("  + New field: {} ({:?})", target_field.name, target_field.field_type);
        }
    }

    // Find removed fields
    for current_field in current_fields {
        if target.field_by_name(current_field.name.as_str()).is_none() {
            tracing::info!(
                "  - Removed field: {} ({:?})",
                current_field.name,
                current_field.field_type
            );
        }
    }

    // Find modified fields
    for target_field in target_fields {
        if let Some(current_field) = current.field_by_name(target_field.name.as_str()) {
            let type_changed = !types_equal(&current_field.field_type, &target_field.field_type);
            let required_changed = current_field.required != target_field.required;

            if type_changed || required_changed {
                tracing::info!(
                    "  ~ Modified field: {} (type: {:?} -> {:?}, required: {} -> {})",
                    target_field.name,
                    current_field.field_type,
                    target_field.field_type,
                    current_field.required,
                    target_field.required
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incompatible_table(table_name: &str) -> DifferingTable {
        DifferingTable {
            table_name: table_name.to_string(),
            drift: UnresolvableDrift::IncompatibleSchema,
        }
    }

    #[test]
    fn format_upgrade_error_message_single_table_uses_singular_wording() {
        let msg = format_upgrade_required_message(&[incompatible_table("spans")]);
        assert!(msg.starts_with("Table ["));
        assert!(msg.contains("spans"));
        assert!(msg.contains("drop the table manually"));
        assert!(msg.contains("icegate-maintain migrate create"));
    }

    #[test]
    fn format_upgrade_error_message_multiple_tables_uses_plural_wording() {
        let msg = format_upgrade_required_message(&[incompatible_table("logs"), incompatible_table("spans")]);
        assert!(msg.starts_with("Tables ["));
        assert!(msg.contains("logs"));
        assert!(msg.contains("spans"));
        assert!(msg.contains("drop the tables manually"));
    }

    /// The message names each table's reason, so an id mismatch reads apart from schema drift.
    #[test]
    fn format_upgrade_error_message_states_each_table_reason() {
        let mismatch = DifferingTable {
            table_name: "operations".to_string(),
            drift: UnresolvableDrift::FieldIdMismatch(FieldIdMismatch {
                column_name: "evaluations".to_string(),
                field_name: "element".to_string(),
                code_id: 66,
                table_id: 67,
            }),
        };
        let msg = format_upgrade_required_message(&[incompatible_table("logs"), mismatch]);
        assert!(
            msg.contains(&format!("logs: {}", UnresolvableDrift::IncompatibleSchema)),
            "{msg}"
        );
        assert!(
            msg.contains("operations: column 'evaluations': field 'element' has id 66"),
            "{msg}"
        );
        assert!(msg.contains("would assign it 67"), "{msg}");
    }

    #[test]
    fn build_table_definitions_includes_operations_table() {
        let definitions = build_table_definitions().expect("build table definitions");
        // Phase 5 adds the 5th physical table; all four prior tables plus
        // operations must be present. Phase 6 adds the prices table.
        assert_eq!(definitions.len(), 6);

        let names: Vec<&str> = definitions.iter().map(|def| def.name).collect();
        assert!(names.contains(&LOGS_TABLE));
        assert!(names.contains(&SPANS_TABLE));
        assert!(names.contains(&EVENTS_TABLE));
        assert!(names.contains(&METRICS_TABLE));
        assert!(
            names.contains(&OPERATIONS_TABLE),
            "operations table must be registered for migrate create/upgrade"
        );

        // The operations definition must carry the schema_id(5) schema and the
        // tenant_id + day(timestamp) partition spec so `migrate create` builds
        // the table the shift planner expects.
        let operations = definitions
            .iter()
            .find(|def| def.name == OPERATIONS_TABLE)
            .expect("operations definition present");
        assert_eq!(operations.schema.schema_id(), 5);
        assert_eq!(operations.partition_spec.spec_id(), 5);
        assert_eq!(operations.sort_order.order_id, 5);
    }

    #[test]
    fn build_table_definitions_includes_prices() {
        let defs = build_table_definitions().expect("definitions build");
        assert_eq!(defs.len(), 6, "expected the five telemetry tables plus prices");

        let prices = defs
            .iter()
            .find(|d| d.name == icegate_common::PRICES_TABLE)
            .expect("prices definition present");
        assert!(
            prices.partition_spec.fields().is_empty(),
            "prices is the one unpartitioned table"
        );
    }

    /// Every table the Shifter writes must advertise a WAL-offset carrier to
    /// expiration; `prices`, written by the pricing crawler, must not — nothing
    /// stamps an offset onto its snapshots.
    #[test]
    fn only_shifter_written_tables_declare_a_wal_offset_carrier() {
        let definitions = build_table_definitions().expect("build table definitions");

        for def in &definitions {
            let expected = def.name != PRICES_TABLE;
            assert_eq!(
                def.carries_wal_offset,
                expected,
                "{} must{} declare a WAL-offset carrier",
                def.name,
                if expected { "" } else { " not" }
            );
        }
    }

    #[test]
    fn schemas_differ_detects_modified_nested_types() {
        use std::sync::Arc;

        use iceberg::spec::{MapType, NestedField, PrimitiveType, Type};

        let current = iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::required(1, "tenant_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(
                    2,
                    "attributes",
                    Type::Map(MapType::new(
                        Arc::new(NestedField::required(3, "key", Type::Primitive(PrimitiveType::String))),
                        Arc::new(NestedField::required(
                            4,
                            "value",
                            Type::Primitive(PrimitiveType::String),
                        )),
                    )),
                )
                .into(),
            ])
            .build()
            .expect("current schema");

        let target = iceberg::spec::Schema::builder()
            .with_schema_id(2)
            .with_fields(vec![
                NestedField::required(1, "tenant_id", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "attributes", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("target schema");

        assert!(schemas_differ(&current, &target));
    }

    /// A schema of `(name, required)` string fields with ids from 1.
    fn string_schema(fields: &[(&str, bool)]) -> iceberg::spec::Schema {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        let fields = fields
            .iter()
            .zip(1..)
            .map(|(&(name, required), id)| {
                NestedField::new(id, name, Type::Primitive(PrimitiveType::String), required).into()
            })
            .collect::<Vec<_>>();
        iceberg::spec::Schema::builder()
            .with_fields(fields)
            .build()
            .expect("test schema builds")
    }

    fn added_names(current: &iceberg::spec::Schema, target: &iceberg::spec::Schema) -> Option<Vec<String>> {
        plan_column_additions(current, target).map(|fields| fields.iter().map(|f| f.name.clone()).collect())
    }

    #[test]
    fn plan_column_additions_returns_the_optional_columns_appended_to_the_end() {
        let current = string_schema(&[("a", true), ("b", false)]);
        let target = string_schema(&[("a", true), ("b", false), ("c", false), ("d", false)]);
        assert_eq!(
            added_names(&current, &target),
            Some(vec!["c".to_string(), "d".to_string()])
        );
        assert_eq!(added_names(&target, &target), Some(Vec::new()), "no drift adds nothing");
    }

    #[test]
    fn plan_column_additions_refuses_drift_that_is_not_a_trailing_optional_column() {
        // Each case leaves drift only a drop-and-recreate can resolve: rows
        // already in the table cannot fill a required column, a column added
        // before an existing one is not an append, and a changed or removed
        // column is not an addition at all.
        let current = string_schema(&[("a", true), ("b", false)]);
        let cases = [
            (
                "required addition",
                string_schema(&[("a", true), ("b", false), ("c", true)]),
            ),
            (
                "addition before an existing column",
                string_schema(&[("a", true), ("c", false), ("b", false)]),
            ),
            (
                "changed required flag",
                string_schema(&[("a", false), ("b", false), ("c", false)]),
            ),
            ("removed column", string_schema(&[("a", true), ("c", false)])),
            // The table is ahead of this binary, as after a rollback; reading it as "nothing to
            // add" would let `migrate create` pass over a schema this binary does not know.
            ("table wider than the code", string_schema(&[("a", true)])),
        ];
        for (case, target) in cases {
            assert_eq!(added_names(&current, &target), None, "{case}");
        }
    }

    #[test]
    fn plan_column_additions_brings_the_previous_prices_schema_up_to_date() {
        // The live deployments' `prices` predates the time-of-day columns;
        // this is the drift `migrate create` resolves in place for them.
        let target = prices_schema().expect("prices schema builds");
        let previous = iceberg::spec::Schema::builder()
            .with_fields(
                target
                    .as_struct()
                    .fields()
                    .iter()
                    .filter(|f| f.name != "min_week_minute" && f.name != "max_week_minute")
                    .cloned(),
            )
            .build()
            .expect("previous prices schema builds");
        assert_eq!(
            added_names(&previous, &target),
            Some(vec!["min_week_minute".to_string(), "max_week_minute".to_string()])
        );
    }

    /// The table numbers an added column's fields in pre-order, a map's key before its value, and
    /// continues from the last id it assigned, so a nested mismatch or an earlier column's nested
    /// fields decide what comes after them. The WAL tags its data with the schema module's ids, so
    /// any disagreement has to stop the migration.
    #[test]
    fn find_added_field_id_mismatch_compares_nested_fields_in_preorder() {
        use iceberg::spec::{ListType, MapType, PrimitiveType, StructType, Type};

        let long = || Type::Primitive(PrimitiveType::Long);
        let list = |id, element_id| {
            NestedFieldRef::from(NestedField::optional(
                id,
                "column",
                Type::List(ListType::new(
                    NestedField::list_element(element_id, long(), false).into(),
                )),
            ))
        };
        let map = |key_id, value_id| {
            NestedFieldRef::from(NestedField::optional(
                11,
                "column",
                Type::Map(MapType::new(
                    NestedField::map_key_element(key_id, long()).into(),
                    NestedField::map_value_element(value_id, long(), false).into(),
                )),
            ))
        };
        let struct_of = |field_id| {
            NestedFieldRef::from(NestedField::optional(
                11,
                "column",
                Type::Struct(StructType::new(vec![
                    NestedField::optional(field_id, "a", long()).into(),
                ])),
            ))
        };
        let primitive = |id| NestedFieldRef::from(NestedField::optional(id, "next", long()));
        let mismatch = |column_name: &str, field_name: &str, code_id, table_id| {
            Some(FieldIdMismatch {
                column_name: column_name.to_string(),
                field_name: field_name.to_string(),
                code_id,
                table_id,
            })
        };

        let cases = [
            ("matching list", vec![list(11, 12)], None),
            (
                "list element off by one",
                vec![list(11, 13)],
                mismatch("column", "element", 13, 12),
            ),
            (
                "struct field off by one",
                vec![struct_of(13)],
                mismatch("column", "a", 13, 12),
            ),
            (
                "map key off by one",
                vec![map(13, 14)],
                mismatch("column", "key", 13, 12),
            ),
            (
                "map value off by one",
                vec![map(12, 14)],
                mismatch("column", "value", 14, 13),
            ),
            ("column after a nested one", vec![list(11, 12), primitive(13)], None),
            (
                "column after a nested one reusing its id",
                vec![list(11, 12), primitive(12)],
                mismatch("next", "next", 12, 13),
            ),
        ];
        for (case, columns, expected) in cases {
            assert_eq!(find_added_field_id_mismatch(&columns, 10), expected, "{case}");
        }
    }

    /// `logs`, `spans`, `events`, and `metrics` end in a required column today, so `create_tables`
    /// cannot show that appending works for them; the next release's column must not be the first to
    /// find out.
    #[tokio::test]
    async fn add_missing_columns_appends_an_optional_column_to_every_table_definition() {
        use iceberg::spec::{PrimitiveType, Schema, Type};
        use icegate_common::catalog::{CatalogBackend, CatalogBuilder, CatalogConfig, IoHandle};
        use tokio_util::sync::CancellationToken;

        const COMMITTED_OFFSET: &str = "42";
        const OTHER_PROPERTY: &str = "test.other-property";
        const ADDED_COLUMN: &str = "next_release_column";

        let catalog = CatalogBuilder::from_config(
            &CatalogConfig {
                backend: CatalogBackend::Memory,
                warehouse: format!("memory://migrate-operations-{}", uuid::Uuid::new_v4()),
                properties: HashMap::new(),
                cache: None,
            },
            &IoHandle::noop(),
            CancellationToken::new(),
        )
        .await
        .expect("memory catalog");
        let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("create namespace");

        let definitions = build_table_definitions().expect("build table definitions");
        assert_eq!(definitions.len(), 6);
        for def in definitions {
            create_table(catalog.as_ref(), &namespace, &def, &SnapshotExpirationConfig::default())
                .await
                .expect("create table");
            let ident = TableIdent::new(namespace.clone(), def.name.to_string());
            let table = catalog.load_table(&ident).await.expect("load table");
            let property = if def.name == PRICES_TABLE {
                OTHER_PROPERTY
            } else {
                WAL_OFFSET_PROPERTY
            };
            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .set_snapshot_properties(HashMap::from([(property.to_string(), COMMITTED_OFFSET.to_string())]))
                .apply(tx)
                .expect("snapshot applies");
            let before = tx.commit(catalog.as_ref()).await.expect("snapshot commits");
            let snapshots_before = before.metadata().snapshots().count();
            let added_id = before.metadata().last_column_id() + 1;
            // Built on the table's schema, not the definition's: the memory catalog renumbers fields
            // on `create_table`, so the definition's ids would collide with `added_id`.
            let current_fields = before.metadata().current_schema().as_struct().fields();
            let wider_definition = TableDefinition {
                name: def.name,
                schema: Schema::builder()
                    .with_fields(current_fields.iter().cloned().chain([Arc::new(NestedField::optional(
                        added_id,
                        ADDED_COLUMN,
                        Type::Primitive(PrimitiveType::String),
                    ))]))
                    .build()
                    .expect("wider schema builds"),
                partition_spec: def.partition_spec.clone(),
                sort_order: def.sort_order.clone(),
                carries_wal_offset: def.carries_wal_offset,
            };

            let result = add_missing_columns(catalog.as_ref(), &wider_definition, &before, false)
                .await
                .expect("add columns");

            assert_eq!(result, ColumnAddition::ColumnsAdded, "{}", def.name);
            let after = catalog.load_table(&ident).await.expect("load table");
            let metadata = after.metadata();
            assert_eq!(metadata.uuid(), before.metadata().uuid(), "{}", def.name);
            let added = metadata
                .current_schema()
                .field_by_name(ADDED_COLUMN)
                .unwrap_or_else(|| panic!("{} must gain the column", def.name));
            assert!(!added.required, "{}", def.name);
            assert_eq!(added.id, added_id, "{}", def.name);
            assert_eq!(metadata.snapshots().count(), snapshots_before + 1, "{}", def.name);
            let current = metadata.current_snapshot().expect("current snapshot");
            assert_eq!(current.schema_id(), Some(metadata.current_schema_id()), "{}", def.name);
            let expected_offset = (def.name != PRICES_TABLE).then(|| COMMITTED_OFFSET.to_string());
            assert_eq!(
                current.summary().additional_properties.get(WAL_OFFSET_PROPERTY),
                expected_offset.as_ref(),
                "{}",
                def.name
            );
        }
    }
}
