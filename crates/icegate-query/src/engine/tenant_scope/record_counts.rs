//! A tenant's committed footprint in one table, read from snapshot metadata alone.

use futures::TryStreamExt as _;
use iceberg::expr::Reference;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Datum, PartitionSpec, Transform};
use iceberg::table::Table;
use icegate_common::schema::COL_TENANT_ID;
use icegate_common::{GLOBAL_TABLES, TenantId};

use crate::error::{QueryError, Result};

/// The rows and data-file bytes a tenant holds in a table's current snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct TenantFootprint {
    /// Committed rows.
    pub(crate) record_count: u64,
    /// Bytes of the data files holding those rows.
    pub(crate) file_size_bytes: u64,
}

/// Count `tenant`'s committed rows and data-file bytes in `table`.
///
/// Plans the current snapshot with the single predicate `tenant_id = <tenant>` and sums the
/// planned files' record counts and sizes, so no data file is read. That is exact only because
/// every partition spec the table has ever had identity-partitions `tenant_id`, which is checked
/// first: each planned file then holds that tenant alone. A global table (no `tenant_id`,
/// identical rows for every tenant, listed in [`GLOBAL_TABLES`]) is counted whole. Rows still in
/// the WAL are not counted.
///
/// # Errors
///
/// Returns [`QueryError::Internal`] where the count would be wrong, so none is given: a table
/// that is not global and has no `tenant_id`; a partition spec without an identity field on
/// `tenant_id`; a planned file without a record count or with delete files; a sum overflowing
/// `u64`. Returns [`QueryError::Iceberg`] when planning fails.
pub(crate) async fn count_tenant_footprint(table: &Table, tenant: &TenantId) -> Result<TenantFootprint> {
    let is_global = GLOBAL_TABLES.contains(&table.identifier().name());
    if !is_global {
        refuse_table_without_tenant_partitioning(table)?;
    }
    if table.metadata().current_snapshot().is_none() {
        return Ok(TenantFootprint::default());
    }
    let mut scan = table.scan().select_empty();
    if !is_global {
        scan = scan.with_filter(Reference::new(COL_TENANT_ID).equal_to(Datum::string(tenant.as_ref())));
    }
    scan.build()?
        .plan_files()
        .await?
        .map_err(QueryError::from)
        .try_fold(TenantFootprint::default(), |footprint, task| async move {
            add_planned_file(footprint, &task)
        })
        .await
}

/// `footprint` with one planned data file's rows and bytes added.
///
/// # Errors
///
/// Returns [`QueryError::Internal`] for a file with delete files (its live row count is
/// unknown), a file planned without a record count, and a sum overflowing `u64`.
fn add_planned_file(footprint: TenantFootprint, task: &FileScanTask) -> Result<TenantFootprint> {
    if !task.deletes.is_empty() {
        return Err(QueryError::Internal(format!(
            "data file {} has delete files, so its live row count is unknown",
            task.data_file_path
        )));
    }
    let record_count = task.record_count.ok_or_else(|| {
        QueryError::Internal(format!(
            "data file {} was planned without a record count",
            task.data_file_path
        ))
    })?;
    let overflow = || QueryError::Internal("tenant footprint overflows u64".to_string());
    Ok(TenantFootprint {
        record_count: footprint.record_count.checked_add(record_count).ok_or_else(overflow)?,
        file_size_bytes: footprint
            .file_size_bytes
            .checked_add(task.file_size_in_bytes)
            .ok_or_else(overflow)?,
    })
}

/// Refuse a table whose files may hold several tenants: one without `tenant_id`, or with a
/// partition spec lacking an identity field on it.
fn refuse_table_without_tenant_partitioning(table: &Table) -> Result<()> {
    let metadata = table.metadata();
    let table_name = table.identifier().name();
    let tenant_field_id = metadata
        .current_schema()
        .field_by_name(COL_TENANT_ID)
        .ok_or_else(|| {
            QueryError::Internal(format!(
                "table `{table_name}` is not global and has no `{COL_TENANT_ID}`"
            ))
        })?
        .id;
    let is_identity_partitioned = |spec: &PartitionSpec| {
        spec.fields()
            .iter()
            .any(|field| field.source_id == tenant_field_id && field.transform == Transform::Identity)
    };
    if metadata.partition_specs_iter().all(|spec| is_identity_partitioned(spec)) {
        Ok(())
    } else {
        Err(QueryError::Internal(format!(
            "table `{table_name}` has a partition spec without an identity partition on \
             `{COL_TENANT_ID}`, so a tenant's rows cannot be counted from metadata"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use iceberg::scan::FileScanTaskDeleteFile;
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, PartitionSpec, Schema, UnboundPartitionSpec,
    };
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    use iceberg::{Catalog, NamespaceIdent, TableCreation};
    use icegate_common::{ICEGATE_NAMESPACE, LOGS_TABLE, PRICES_TABLE, schema};

    use super::*;
    use crate::test_support::{build_icegate_catalog, build_tenant};

    /// A planned data file of `file_size_in_bytes` bytes holding `record_count` rows, with
    /// `delete_file_count` positional delete files.
    fn build_planned_file(
        record_count: Option<u64>,
        file_size_in_bytes: u64,
        delete_file_count: usize,
    ) -> FileScanTask {
        let deletes = (0..delete_file_count)
            .map(|position| {
                FileScanTaskDeleteFile::builder()
                    .with_file_path(format!("s3://warehouse/deletes-{position}.parquet"))
                    .with_file_size_in_bytes(64)
                    .with_file_type(DataContentType::PositionDeletes)
                    .with_partition_spec_id(0)
                    .build()
            })
            .collect();
        FileScanTask::builder()
            .with_file_size_in_bytes(file_size_in_bytes)
            .with_start(0)
            .with_length(file_size_in_bytes)
            .with_record_count(record_count)
            .with_data_file_path("s3://warehouse/data.parquet".to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(Arc::new(Schema::builder().build().expect("an empty schema")))
            .with_project_field_ids(Vec::new())
            .with_deletes(deletes)
            .with_case_sensitive(true)
            .build()
    }

    /// Create the table `name` in the IceGate namespace with `table_schema`, partitioned by
    /// `partition_spec`, or unpartitioned when it is `None`.
    async fn create_icegate_table(
        catalog: &Arc<dyn Catalog>,
        name: &str,
        table_schema: Schema,
        partition_spec: Option<PartitionSpec>,
    ) -> Table {
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(table_schema)
            .partition_spec_opt(partition_spec.map(UnboundPartitionSpec::from))
            .build();
        catalog
            .create_table(&NamespaceIdent::new(ICEGATE_NAMESPACE.to_string()), creation)
            .await
            .expect("a table in the IceGate namespace")
    }

    #[test]
    fn a_planned_file_adds_its_record_count_and_size() {
        let footprint = TenantFootprint {
            record_count: 5,
            file_size_bytes: 100,
        };

        let summed_footprint = add_planned_file(footprint, &build_planned_file(Some(3), 40, 0)).expect("added");

        assert_eq!(
            summed_footprint,
            TenantFootprint {
                record_count: 8,
                file_size_bytes: 140,
            }
        );
    }

    /// Delete files hide rows the manifest still counts, so the file's live count is unknown. The
    /// same file without delete files is counted (see the test above).
    #[test]
    fn a_planned_file_with_delete_files_is_an_error() {
        let result = add_planned_file(TenantFootprint::default(), &build_planned_file(Some(3), 40, 1));

        assert!(matches!(result, Err(QueryError::Internal(_))), "{result:?}");
    }

    /// A missing count is unknown, never zero.
    #[test]
    fn a_planned_file_without_a_record_count_is_an_error() {
        let result = add_planned_file(TenantFootprint::default(), &build_planned_file(None, 40, 0));

        assert!(matches!(result, Err(QueryError::Internal(_))), "{result:?}");
    }

    /// One row or one byte beyond `u64::MAX` is refused rather than wrapped.
    #[test]
    fn a_footprint_overflowing_u64_is_an_error() {
        let footprint_at_row_limit = TenantFootprint {
            record_count: u64::MAX,
            file_size_bytes: 0,
        };
        let footprint_at_byte_limit = TenantFootprint {
            record_count: 0,
            file_size_bytes: u64::MAX,
        };

        for footprint in [footprint_at_row_limit, footprint_at_byte_limit] {
            let result = add_planned_file(footprint, &build_planned_file(Some(1), 1, 0));
            assert!(
                matches!(result, Err(QueryError::Internal(_))),
                "{footprint:?}: {result:?}"
            );
        }
    }

    /// A sum landing exactly on `u64::MAX` is still exact, so it is counted.
    #[test]
    fn a_footprint_summing_to_exactly_u64_max_is_counted() {
        let footprint = TenantFootprint {
            record_count: u64::MAX - 1,
            file_size_bytes: u64::MAX - 1,
        };

        let summed_footprint =
            add_planned_file(footprint, &build_planned_file(Some(1), 1, 0)).expect("at the boundary");

        assert_eq!(
            summed_footprint,
            TenantFootprint {
                record_count: u64::MAX,
                file_size_bytes: u64::MAX,
            }
        );
    }

    /// A table carrying `tenant_id` without an identity partition on it would hold several
    /// tenants per file; planning would count them all, so the count is refused instead. The
    /// table has no snapshot, so only the refusal can make the count fail.
    #[tokio::test]
    async fn count_tenant_footprint_refuses_a_table_not_partitioned_by_tenant() {
        let (_warehouse, catalog) = build_icegate_catalog().await;
        let table = create_icegate_table(
            &catalog,
            LOGS_TABLE,
            schema::logs_schema().expect("the logs schema"),
            None,
        )
        .await;

        let result = count_tenant_footprint(&table, &build_tenant("tenant-a")).await;

        assert!(matches!(result, Err(QueryError::Internal(_))), "{result:?}");
    }

    /// A table outside the global allowlist that has no `tenant_id` holds rows of no known
    /// tenant, so counting them for one would be a guess. `rates` is such a name; the table has
    /// no snapshot, so only the refusal can make the count fail.
    #[tokio::test]
    async fn count_tenant_footprint_refuses_a_non_global_table_without_tenant_id() {
        let (_warehouse, catalog) = build_icegate_catalog().await;
        let table = create_icegate_table(
            &catalog,
            "rates",
            schema::prices_schema().expect("the prices schema"),
            None,
        )
        .await;

        let result = count_tenant_footprint(&table, &build_tenant("tenant-a")).await;

        assert!(matches!(result, Err(QueryError::Internal(_))), "{result:?}");
    }

    /// `prices` carries no `tenant_id` and holds the same rows for every tenant, so each tenant
    /// is given the whole table: one committed file of 2 rows and 100 bytes. The file is listed
    /// in a manifest only, which is all planning reads.
    #[tokio::test]
    async fn a_global_table_is_counted_whole_for_every_tenant() {
        let (_warehouse, catalog) = build_icegate_catalog().await;
        let prices_schema = schema::prices_schema().expect("the prices schema");
        let prices_partition_spec = schema::prices_partition_spec(&prices_schema).expect("the prices partition spec");
        let table = create_icegate_table(&catalog, PRICES_TABLE, prices_schema, Some(prices_partition_spec)).await;
        let data_file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!("{}/data/prices.parquet", table.metadata().location()))
            .file_format(DataFileFormat::Parquet)
            .file_size_in_bytes(100)
            .record_count(2)
            .partition_spec_id(table.metadata().default_partition_spec_id())
            .build()
            .expect("a data file entry");
        let append_action = Transaction::new(&table).fast_append().add_data_files(vec![data_file]);
        let committed_table = append_action
            .apply(Transaction::new(&table))
            .expect("an append transaction")
            .commit(&*catalog)
            .await
            .expect("the append commits");

        for tenant in ["tenant-a", "tenant-b"] {
            let footprint = count_tenant_footprint(&committed_table, &build_tenant(tenant))
                .await
                .expect("prices is counted");
            assert_eq!(
                footprint,
                TenantFootprint {
                    record_count: 2,
                    file_size_bytes: 100,
                },
                "{tenant}"
            );
        }
    }
}
