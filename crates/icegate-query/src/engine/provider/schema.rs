//! Custom schema provider for the IceGate namespace.
//!
//! Routes WAL-backed tables (logs, spans, metrics, operations) to
//! [`IcegateTableProvider`] (merged Iceberg + WAL) while delegating other
//! tables to the standard `IcebergStaticTableProvider`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::{TableProvider, ViewTable};
use datafusion::error::Result as DFResult;
use datafusion::prelude::SessionContext;
use futures::future::try_join_all;
use iceberg::{Catalog, NamespaceIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use icegate_common::schema::{COL_VALID_FROM, PRICES_KEY_COLUMNS};
use icegate_queue::ParquetQueueReader;

use super::WalQueryConfig;
use super::table::IcegateTableProvider;

/// Tables that have a corresponding WAL topic and therefore use the merged
/// Iceberg + WAL provider. The slice value doubles as the WAL topic name —
/// table name == topic name by convention. Tables not listed here (e.g.
/// `events`, which has no WAL topic) fall through to the standard Iceberg-only
/// provider.
const WAL_MERGED_TABLES: &[&str] = &[
    icegate_common::LOGS_TOPIC,
    icegate_common::SPANS_TOPIC,
    icegate_common::METRICS_TOPIC,
    icegate_common::OPERATIONS_TOPIC,
];

/// SQL defining [`icegate_common::PRICES_EFFECTIVE_TABLE`] over the raw
/// `prices` observation log.
///
/// `prices` stores only `valid_from`: the sole Iceberg write path in this
/// codebase is `fast_append`, so a superseded row cannot be updated to carry an
/// end date. `valid_to` is therefore derived — the next revision's `valid_from`
/// for the same key, NULL for the row currently in effect.
///
/// This view is a DataFusion object: Flight SQL and the query APIs see it, but
/// **Trino reads the Iceberg catalog directly and will not**. External consumers
/// apply this window themselves, which is why the raw table is self-sufficient.
///
/// Every `prices` column is named through `icegate_common::schema`, so renaming
/// one there breaks this build rather than silently degrading into a planning
/// error at startup — the failure [`plan_prices_effective`] swallows.
static PRICES_EFFECTIVE_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT *, LEAD({valid_from}) OVER (\
             PARTITION BY {key} ORDER BY {valid_from}\
         ) AS {valid_to} FROM {prices}",
        valid_from = COL_VALID_FROM,
        key = PRICES_KEY_COLUMNS.join(", "),
        valid_to = COL_VALID_TO,
        prices = icegate_common::PRICES_TABLE,
    )
});

/// The column [`PRICES_EFFECTIVE_SQL`] derives, and the only one the view adds
/// to the `prices` schema.
///
/// Deliberately not in `icegate_common::schema`: no Iceberg table has this
/// column, so it is the view's contract rather than a schema-of-record entry.
const COL_VALID_TO: &str = "valid_to";

/// Plans [`PRICES_EFFECTIVE_SQL`] over `prices` as a standalone view.
///
/// The plan is built against a throwaway [`SessionContext`] holding only
/// `prices`, so the resulting `ViewTable` closes over `prices` directly and
/// resolves correctly once inserted into the caller's table map — no
/// dependency on the session it is later queried from.
///
/// # Errors
///
/// Returns a `DataFusionError` if `prices` cannot be registered into the
/// planning context, or if [`PRICES_EFFECTIVE_SQL`] fails to plan against it
/// (for example because the `prices` schema has drifted from the columns the
/// SQL references). Callers treat this as non-fatal: `prices_effective` is a
/// convenience view over `prices`, not a table other tables depend on.
async fn plan_prices_effective(prices: Arc<dyn TableProvider>) -> DFResult<Arc<dyn TableProvider>> {
    let planning_ctx = SessionContext::new();
    planning_ctx.register_table(icegate_common::PRICES_TABLE, prices)?;
    let plan = planning_ctx.sql(PRICES_EFFECTIVE_SQL.as_str()).await?.into_unoptimized_plan();
    Ok(Arc::new(ViewTable::new(plan, Some(PRICES_EFFECTIVE_SQL.clone()))))
}

/// Schema provider that substitutes `IcegateTableProvider` for WAL-backed
/// tables (logs, spans, metrics, operations) while using standard Iceberg
/// providers for all other tables.
pub(super) struct IcegateSchemaProvider {
    /// All tables in the namespace, keyed by name.
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl std::fmt::Debug for IcegateSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcegateSchemaProvider")
            .field("tables", &self.tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl IcegateSchemaProvider {
    /// Creates a new schema provider for the given namespace.
    ///
    /// Tables listed in [`WAL_MERGED_TABLES`] are wrapped in
    /// [`IcegateTableProvider`] (merged Iceberg + WAL). All other tables use
    /// `IcebergStaticTableProvider`.
    ///
    /// # Errors
    ///
    /// Returns an error if tables cannot be loaded from the catalog.
    #[tracing::instrument(skip(catalog, wal_reader, wal_config), fields(%namespace))]
    pub(super) async fn try_new(
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        wal_reader: Arc<ParquetQueueReader>,
        wal_config: WalQueryConfig,
    ) -> Result<Self, iceberg::Error> {
        let table_idents = catalog.list_tables(&namespace).await?;

        // Load all tables concurrently — each table requires a catalog
        // REST call, so parallelizing cuts wall-clock time significantly.
        // With only a handful of tables, unbounded concurrency is fine.
        let tables_loaded = try_join_all(table_idents.iter().map(|ident| {
            let name = ident.name().to_string();
            let catalog = Arc::clone(&catalog);
            let namespace = namespace.clone();
            let wal_reader = Arc::clone(&wal_reader);
            let wal_config = wal_config.clone();
            async move {
                // Look up the WAL topic in the allowlist; absence routes the
                // table through the standard Iceberg-only provider.
                let provider: Arc<dyn TableProvider> =
                    if let Some(topic) = WAL_MERGED_TABLES.iter().copied().find(|t| *t == name) {
                        // WAL-backed table: use our merged provider
                        let table_ident = iceberg::TableIdent::new(namespace, name.clone());
                        let provider =
                            IcegateTableProvider::try_new(catalog, table_ident, topic, wal_reader, wal_config).await?;
                        Arc::new(provider)
                    } else {
                        // Other tables: standard Iceberg static provider
                        let table = catalog.load_table(ident).await?;
                        let provider = IcebergStaticTableProvider::try_new_from_table(table).await?;
                        Arc::new(provider)
                    };
                Ok::<_, iceberg::Error>((name, provider))
            }
        }))
        .await?;

        let mut tables: HashMap<String, Arc<dyn TableProvider>> = tables_loaded.into_iter().collect();

        // Register the derived-`valid_to` view beside the raw table. This is a
        // convenience view, not a table anything else depends on, so a failure
        // here (e.g. `PRICES_EFFECTIVE_SQL` drifting from the `prices` schema)
        // must not take down the whole namespace — `prices` itself is already
        // in `tables` and stays queryable either way.
        if let Some(prices) = tables.get(icegate_common::PRICES_TABLE) {
            match plan_prices_effective(Arc::clone(prices)).await {
                Ok(view) => {
                    tables.insert(icegate_common::PRICES_EFFECTIVE_TABLE.to_string(), view);
                }
                Err(e) => tracing::error!(
                    error = %e,
                    "failed to register {}; the raw prices table remains queryable",
                    icegate_common::PRICES_EFFECTIVE_TABLE
                ),
            }
        }

        tracing::debug!(table_count = tables.len(), "Schema provider initialized");
        Ok(Self { tables })
    }
}

#[async_trait]
impl SchemaProvider for IcegateSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.get(name).cloned())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{
        Array, ArrayRef, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    use super::{COL_VALID_FROM, COL_VALID_TO, PRICES_EFFECTIVE_SQL, WAL_MERGED_TABLES};

    /// Guard against accidental drift in the WAL-merged table allowlist.
    /// The dispatch in `IcegateSchemaProvider::try_new` keys off this slice;
    /// removing an entry silently disables WAL merging for that table.
    #[test]
    fn wal_merged_tables_covers_logs_spans_metrics_and_operations() {
        assert!(WAL_MERGED_TABLES.contains(&icegate_common::LOGS_TOPIC));
        assert!(WAL_MERGED_TABLES.contains(&icegate_common::SPANS_TOPIC));
        assert!(WAL_MERGED_TABLES.contains(&icegate_common::METRICS_TOPIC));
        assert!(WAL_MERGED_TABLES.contains(&icegate_common::OPERATIONS_TOPIC));
    }

    /// `prices` has no WAL topic, so it must fall through to the plain Iceberg
    /// provider exactly as `events` does.
    #[test]
    fn prices_is_not_wal_merged() {
        assert!(!WAL_MERGED_TABLES.contains(&icegate_common::PRICES_TABLE));
    }

    /// The derived `valid_to` is the whole point of the view: without the LEAD
    /// correctly partitioned and ordered, revisions of different models
    /// interleave or `valid_to` values land on the wrong row. A substring match
    /// on the SQL text cannot catch that (it would pass even if `region` moved
    /// out of `PARTITION BY` or `ORDER BY valid_from` were dropped), so this
    /// executes the SQL and checks the derived values instead.
    #[tokio::test]
    async fn prices_effective_sql_derives_valid_to_over_the_full_key() {
        // Two revisions of one (provider, model, service_tier, region,
        // min_input_tokens) key, plus one row for a different model whose
        // valid_from falls between them. That third row is the point: a
        // `PARTITION BY` that doesn't actually partition would let it leak in as
        // the older revision's derived `valid_to` instead of NULL.
        const OLDER_VALID_FROM: i64 = 1_000_000;
        const OTHER_MODEL_VALID_FROM: i64 = 1_500_000;
        const NEWER_VALID_FROM: i64 = 2_000_000;

        // Converted from the real Iceberg schema, not hand-written, so the test
        // cannot silently drift from `prices_schema()`.
        let iceberg_schema = icegate_common::schema::prices_schema().expect("prices schema builds");
        let arrow_schema =
            Arc::new(iceberg::arrow::schema_to_arrow_schema(&iceberg_schema).expect("prices schema converts to arrow"));

        // Rate columns are `Decimal(38,10)`. Build them from unscaled `i128`
        // values (rate x 10^10) with the schema's precision/scale.
        let decimal = |unscaled: Vec<Option<i128>>| -> ArrayRef {
            Arc::new(
                Decimal128Array::from(unscaled)
                    .with_precision_and_scale(38, 10)
                    .expect("rate decimal precision/scale"),
            )
        };
        let empty_rates = || decimal(vec![None, None, None]);

        let batch = RecordBatch::try_new(
            Arc::clone(&arrow_schema),
            vec![
                Arc::new(StringArray::from(vec!["anthropic", "anthropic", "openai"])),
                Arc::new(StringArray::from(vec![
                    "claude-3-5-sonnet",
                    "claude-3-5-sonnet",
                    "gpt-4o",
                ])),
                Arc::new(StringArray::from(vec![None::<&str>, None, None])), // canonical_id
                Arc::new(StringArray::from(vec!["standard", "standard", "standard"])),
                Arc::new(StringArray::from(vec!["global", "global", "global"])),
                Arc::new(Int64Array::from(vec![0_i64, 0, 0])), // min_input_tokens
                Arc::new(Int64Array::from(vec![None::<i64>, None, None])), // max_input_tokens
                Arc::new(TimestampMicrosecondArray::from(vec![
                    OLDER_VALID_FROM,
                    NEWER_VALID_FROM,
                    OTHER_MODEL_VALID_FROM,
                ])),
                Arc::new(StringArray::from(vec!["vendor", "vendor", "vendor"])), // valid_from_source
                // 3.0 / 4.0 / 2.0 as unscaled Decimal(38,10) (rate x 10^10).
                decimal(vec![Some(30_000_000_000), Some(40_000_000_000), Some(20_000_000_000)]), // input_usd_per_1m
                empty_rates(),                                                                   // output_usd_per_1m
                empty_rates(), // cache_read_usd_per_1m
                empty_rates(), // cache_write_usd_per_1m
                empty_rates(), // reasoning_usd_per_1m
                empty_rates(), // request_usd
                empty_rates(), // image_input_usd_per_unit
                empty_rates(), // image_output_usd_per_unit
                empty_rates(), // audio_input_usd_per_second
                empty_rates(), // audio_output_usd_per_second
                Arc::new(StringArray::from(vec!["USD", "USD", "USD"])),
                Arc::new(StringArray::from(vec!["test", "test", "test"])), // source
                Arc::new(StringArray::from(vec![None::<&str>, None, None])), // source_url
            ],
        )
        .expect("record batch matches prices schema");

        let table = MemTable::try_new(Arc::clone(&arrow_schema), vec![vec![batch]]).expect("mem table builds");
        let ctx = SessionContext::new();
        ctx.register_table(icegate_common::PRICES_TABLE, Arc::new(table))
            .expect("prices registers");

        let results = ctx
            .sql(PRICES_EFFECTIVE_SQL.as_str())
            .await
            .expect("PRICES_EFFECTIVE_SQL plans")
            .collect()
            .await
            .expect("PRICES_EFFECTIVE_SQL executes");

        // Index derived valid_to by valid_from so assertions don't depend on
        // row order across (possibly multiple) result batches.
        let mut valid_to_by_valid_from: HashMap<i64, Option<i64>> = HashMap::new();
        for result_batch in &results {
            let valid_from_col = result_batch
                .column_by_name(COL_VALID_FROM)
                .expect("valid_from present")
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("valid_from is a timestamp column");
            let valid_to_col = result_batch
                .column_by_name(COL_VALID_TO)
                .expect("valid_to present")
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("valid_to is a timestamp column");
            for row in 0..result_batch.num_rows() {
                let valid_to = (!valid_to_col.is_null(row)).then(|| valid_to_col.value(row));
                valid_to_by_valid_from.insert(valid_from_col.value(row), valid_to);
            }
        }

        assert_eq!(
            valid_to_by_valid_from.get(&OLDER_VALID_FROM),
            Some(&Some(NEWER_VALID_FROM)),
            "the older revision's valid_to must equal the newer revision's valid_from"
        );
        assert_eq!(
            valid_to_by_valid_from.get(&NEWER_VALID_FROM),
            Some(&None),
            "the newer (current) revision must have a NULL valid_to"
        );
        assert_eq!(
            valid_to_by_valid_from.get(&OTHER_MODEL_VALID_FROM),
            Some(&None),
            "an unrelated model's valid_to must not leak in from a different partition"
        );
    }
}
