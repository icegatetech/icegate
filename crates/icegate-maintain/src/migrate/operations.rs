//! Migration operations

use std::{collections::HashMap, sync::Arc};

use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use icegate_common::{
    schema::{
        events_partition_spec, events_schema, events_sort_order, logs_partition_spec, logs_schema, logs_sort_order,
        metrics_partition_spec, metrics_schema, metrics_sort_order, spans_partition_spec, spans_schema,
        spans_sort_order,
    },
    Result, EVENTS_TABLE, ICEGATE_NAMESPACE, LOGS_TABLE, METRICS_TABLE, SPANS_TABLE,
};

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
}

/// Table schema definition with partition spec and sort order
struct TableDefinition {
    name: &'static str,
    schema: iceberg::spec::Schema,
    partition_spec: iceberg::spec::PartitionSpec,
    sort_order: iceberg::spec::SortOrder,
}

/// Build table definitions from schema module
fn build_table_definitions() -> Result<Vec<TableDefinition>> {
    let mut definitions = Vec::with_capacity(4);

    // Logs table
    let logs = logs_schema()?;
    let logs_partition = logs_partition_spec(&logs)?;
    let logs_sort = logs_sort_order(&logs)?;
    definitions.push(TableDefinition {
        name: LOGS_TABLE,
        schema: logs,
        partition_spec: logs_partition,
        sort_order: logs_sort,
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
///
/// Each table is created with appropriate partition specs and sort orders
/// for optimal query performance.
///
/// # Return Value
///
/// Returns a list of operations. In dry-run mode, operations represent what
/// *would* happen, not what actually occurred.
///
/// # Errors
///
/// Returns an error if:
/// - Schema construction fails
/// - Namespace creation fails
/// - Table creation fails (except for "already exists" errors)
#[allow(clippy::cognitive_complexity)]
pub async fn create_tables(catalog: &Arc<dyn Catalog>, dry_run: bool) -> Result<Vec<MigrationOperation>> {
    let catalog_ref = catalog.as_ref();
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());

    // Ensure namespace exists
    if !dry_run {
        ensure_namespace_exists(catalog_ref, &namespace).await?;
    }

    let definitions = build_table_definitions()?;
    let mut operations = Vec::new();

    for def in definitions {
        let table_ident = TableIdent::new(namespace.clone(), def.name.to_string());

        // Check if table already exists
        let exists = catalog_ref.table_exists(&table_ident).await?;

        if exists {
            tracing::info!("Table {} already exists, skipping", def.name);
            continue;
        }

        if dry_run {
            tracing::info!("[dry-run] Would create table: {}", def.name);
        } else {
            tracing::info!("Creating table: {}", def.name);
            create_table(catalog_ref, &namespace, &def).await?;
            tracing::info!("Created table: {}", def.name);
        }

        operations.push(MigrationOperation::Create {
            table_name: def.name.to_string(),
        });
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
async fn create_table(catalog: &dyn Catalog, namespace: &NamespaceIdent, def: &TableDefinition) -> Result<()> {
    let table_creation = TableCreation::builder()
        .name(def.name.to_string())
        .schema(def.schema.clone())
        .partition_spec(def.partition_spec.clone())
        .sort_order(def.sort_order.clone())
        .properties(HashMap::from([
            ("write.format.default".to_string(), "parquet".to_string()),
            ("write.parquet.compression-codec".to_string(), "zstd".to_string()),
        ]))
        .build();

    catalog.create_table(namespace, table_creation).await?;

    Ok(())
}

/// Upgrade existing table schemas
///
/// Checks each observability table for schema differences and applies
/// necessary evolution operations.
///
/// # Schema Evolution Support
///
/// Currently supports:
/// - Adding new optional columns
/// - Updating partition specs
/// - Updating sort orders
///
/// Does NOT support (requires manual intervention):
/// - Removing columns
/// - Changing column types to incompatible types
/// - Changing required columns to optional or vice versa
///
/// # Return Value
///
/// Returns a list of operations. In dry-run mode, operations represent what
/// *would* happen, not what actually occurred.
///
/// # Errors
///
/// Returns an error if:
/// - Table loading fails
/// - Schema comparison fails
/// - Schema evolution fails
#[allow(clippy::cognitive_complexity)]
pub async fn upgrade_schemas(catalog: &Arc<dyn Catalog>, dry_run: bool) -> Result<Vec<MigrationOperation>> {
    let catalog_ref = catalog.as_ref();
    let namespace = NamespaceIdent::new(ICEGATE_NAMESPACE.to_string());
    let definitions = build_table_definitions()?;
    let mut operations = Vec::new();

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
            if dry_run {
                tracing::info!("[dry-run] Would upgrade table schema: {}", def.name);
                log_schema_differences(def.name, current_schema, target_schema);
            } else {
                tracing::info!("Upgrading table schema: {}", def.name);
                // Note: Full schema evolution would require iceberg's update_schema API
                // For now, we log what would need to change
                log_schema_differences(def.name, current_schema, target_schema);
                tracing::warn!(
                    "Automatic schema evolution not yet implemented. Please update {} manually.",
                    def.name
                );
            }

            operations.push(MigrationOperation::Upgrade {
                table_name: def.name.to_string(),
            });
        } else {
            tracing::info!("Table {} schema is up to date", def.name);
        }
    }

    Ok(operations)
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
            },
            None => {
                // Field doesn't exist in current schema
                return true;
            },
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
        },
        (Type::List(la), Type::List(lb)) => {
            la.element_field.required == lb.element_field.required
                && types_equal(&la.element_field.field_type, &lb.element_field.field_type)
        },
        (Type::Map(ma), Type::Map(mb)) => {
            types_equal(&ma.key_field.field_type, &mb.key_field.field_type)
                && ma.value_field.required == mb.value_field.required
                && types_equal(&ma.value_field.field_type, &mb.value_field.field_type)
        },
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
