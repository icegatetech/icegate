//! The `OpenSearch` index catalog: where an index expression resolves to the tables a tenant may
//! read, and where those tables' fields and the tenant's figures in them are listed. Every
//! `OpenSearch` handler that reaches table data starts here, so the tenant scope cannot be
//! skipped. Two metadata reads do not go through it: MAP-key enumeration (see
//! [`TenantSession::list_fields`]) prunes to the tenant by its identity partition, and so does the
//! footprint count (see [`TenantSession::count_footprint`]), except in a global table, which it
//! counts whole.

mod expression;
mod field;
mod map_key_cache;
mod native;
mod registry;

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::catalog::SchemaProvider;
use datafusion::logical_expr::TableType;
use iceberg::expr::Predicate;
use icegate_common::schema::{
    COL_DATA_POINT_ATTRIBUTES, COL_LOG_ATTRIBUTES, COL_METADATA, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES,
    COL_SPAN_ATTRIBUTES,
};
use icegate_common::{ICEGATE_NAMESPACE, TenantId, icegate_table_ident};

pub(crate) use self::expression::IndexExpression;
pub(crate) use self::field::{FieldType, IndexField};
use self::map_key_cache::{MapKeyCache, MapKeys, MapKeysVersion};
use self::native::list_native_fields;
pub(crate) use self::registry::{IndexEntry, IndexOptions, IndexSet, read_index_options};
use self::registry::{Profile, find_telemetry_table};
use super::error::OpenSearchError;
use crate::engine::QueryEngine;
use crate::engine::metadata_scan::{MetadataScanConfig, scan_label_levels};
use crate::engine::tenant_scope::{TenantFootprint, count_tenant_footprint, install_tenant_scope};
use crate::error::QueryError;
use crate::infra::metrics::QueryMetrics;

/// An enumeration config for one MAP column. Field discovery lists raw stored keys, so nothing
/// is normalized, aliased or excluded beyond the system-reserved keys `metadata_scan` always
/// drops.
const fn build_map_key_config(map_column: &'static str) -> MetadataScanConfig {
    MetadataScanConfig {
        indexed_columns: &[],
        label_aliases: &[],
        excluded_map_keys: &[],
        map_column,
        normalize_keys: false,
    }
}

/// Every MAP column a telemetry table may carry; a table enumerates those its schema has.
const MAP_KEY_CONFIGS: [MetadataScanConfig; 6] = [
    build_map_key_config(COL_RESOURCE_ATTRIBUTES),
    build_map_key_config(COL_SCOPE_ATTRIBUTES),
    build_map_key_config(COL_LOG_ATTRIBUTES),
    build_map_key_config(COL_SPAN_ATTRIBUTES),
    build_map_key_config(COL_DATA_POINT_ATTRIBUTES),
    build_map_key_config(COL_METADATA),
];

/// How many tenants' MAP-key enumerations the listener keeps; each holds at most one key set per
/// telemetry table.
const MAP_KEY_CACHE_TENANTS: NonZeroUsize = match NonZeroUsize::new(1024) {
    Some(capacity) => capacity,
    // The literal is non-zero; this arm keeps the constant free of `expect`.
    None => NonZeroUsize::MIN,
};

/// Width of the buckets the discovery window's end is aligned to.
const DISCOVERY_WINDOW_BUCKET_SECS: i64 = 60;

/// Which fields a listing includes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldListing {
    /// The columns only: a MAP column is an `object` without leaves, and nothing is enumerated.
    ColumnsOnly,
    /// The columns, and the MAP keys of a telemetry table within the discovery window.
    WithMapKeys,
}

/// Settings of the index catalog, from the `opensearch` configuration section.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CatalogSettings {
    /// How far back from the request field discovery looks for MAP keys.
    pub(crate) field_discovery_window: Duration,
}

/// Resolves indices and fields against the tables a tenant may read.
#[derive(Clone)]
pub(crate) struct IndexCatalog {
    /// The engine whose sessions list the tables.
    engine: Arc<QueryEngine>,
    /// Where MAP-key enumerations are counted.
    metrics: Arc<QueryMetrics>,
    /// The catalog's settings, handed to every session.
    settings: CatalogSettings,
    /// MAP-key enumerations shared by every session of this catalog.
    map_key_cache: Arc<MapKeyCache>,
}

impl IndexCatalog {
    /// A catalog over the tables of `engine`, counting MAP-key enumerations into `metrics`.
    pub(crate) fn new(engine: Arc<QueryEngine>, metrics: Arc<QueryMetrics>, settings: CatalogSettings) -> Self {
        Self {
            engine,
            metrics,
            settings,
            map_key_cache: Arc::new(MapKeyCache::new(MAP_KEY_CACHE_TENANTS)),
        }
    }

    /// Open a session scoped to `tenant`, with the data indices it sees: one per table its
    /// tenant-scoped catalog lists in the IceGate namespace, except dot-prefixed names, which are
    /// system indices. This is the only way an `OpenSearch` handler reaches table data, so every
    /// read goes through the tenant scope, MAP-key enumeration and the footprint count aside (see
    /// [`TenantSession::list_fields`] and [`TenantSession::count_footprint`]).
    ///
    /// # Errors
    ///
    /// Returns `500` when the session cannot be created, the tenant scope cannot be installed, or
    /// the IceGate namespace is missing from the catalog.
    pub(crate) async fn open_session(&self, tenant: &TenantId) -> Result<TenantSession, OpenSearchError> {
        let context = self.engine.create_session().await?;
        let catalog_name = &self.engine.config().catalog_name;
        install_tenant_scope(&context, catalog_name, tenant)
            .map_err(|error| OpenSearchError::internal(error.to_string()))?;
        let namespace = context
            .catalog(catalog_name)
            .and_then(|catalog| catalog.schema(ICEGATE_NAMESPACE))
            .ok_or_else(|| {
                OpenSearchError::internal(format!(
                    "namespace `{ICEGATE_NAMESPACE}` not found in catalog `{catalog_name}`"
                ))
            })?;
        let indices = IndexSet::from_table_names(namespace.table_names());
        Ok(TenantSession {
            namespace,
            tenant: tenant.clone(),
            indices,
            engine: Arc::clone(&self.engine),
            metrics: Arc::clone(&self.metrics),
            settings: self.settings,
            map_key_cache: Arc::clone(&self.map_key_cache),
        })
    }
}

/// A tenant-scoped view of the catalog for one request.
#[derive(Debug)]
pub(crate) struct TenantSession {
    /// The tenant-scoped IceGate namespace: every table it yields is filtered to the tenant and
    /// hides `tenant_id`.
    namespace: Arc<dyn SchemaProvider>,
    /// The tenant the session is scoped to; MAP-key enumeration and the footprint count of a
    /// tenant-partitioned table prune to it.
    tenant: TenantId,
    /// The data indices the tenant sees, listed when the session opened.
    indices: IndexSet,
    /// The engine whose catalog MAP-key enumeration and the footprint count load tables from.
    engine: Arc<QueryEngine>,
    /// Where MAP-key enumerations are counted.
    metrics: Arc<QueryMetrics>,
    /// The settings of the catalog that opened the session.
    settings: CatalogSettings,
    /// MAP-key enumerations shared with every session of the same catalog.
    map_key_cache: Arc<MapKeyCache>,
}

impl TenantSession {
    /// The data indices the tenant sees.
    pub(crate) const fn indices(&self) -> &IndexSet {
        &self.indices
    }

    /// The fields of `entry`, in schema column order.
    ///
    /// With [`FieldListing::WithMapKeys`], a telemetry table's MAP columns list the tenant's keys
    /// in committed Iceberg data within the discovery window that ends at the end of the minute
    /// holding `now`; keys present only in the WAL are not listed. Exact-name tables and
    /// [`FieldListing::ColumnsOnly`] list no MAP keys and enumerate nothing. The enumeration does
    /// not go through the tenant-scoped catalog: it prunes to the tenant by its identity partition.
    ///
    /// # Errors
    ///
    /// Returns `404` when the table vanished since the session opened, and `500` when the table
    /// cannot be opened, the discovery window starts before the representable time range,
    /// planning or reading Parquet footers fails, or a column type has no `OpenSearch`
    /// representation.
    pub(crate) async fn list_fields(
        &self,
        entry: &IndexEntry,
        now: DateTime<Utc>,
        listing: FieldListing,
    ) -> Result<Vec<IndexField>, OpenSearchError> {
        let provider = self
            .namespace
            .table(entry.table())
            .await
            .map_err(|error| OpenSearchError::internal(error.to_string()))?
            .ok_or_else(|| OpenSearchError::index_not_found(entry.name()))?;
        let arrow_schema = provider.schema();
        let listed_map_keys = match (listing, find_telemetry_table(entry.table())) {
            (FieldListing::WithMapKeys, Some(telemetry_table)) => {
                Some(self.list_map_keys(telemetry_table, &arrow_schema, now).await?)
            }
            (FieldListing::WithMapKeys, None) | (FieldListing::ColumnsOnly, _) => None,
        };
        let empty_map_keys = MapKeys::new();
        let map_keys = listed_map_keys.as_deref().unwrap_or(&empty_map_keys);
        match entry.profile() {
            Profile::Native => list_native_fields(&arrow_schema, map_keys),
        }
    }

    /// The tenant's committed footprint in `entry`'s table; `None` for a view, which has no
    /// storage of its own. Like MAP-key enumeration, it does not go through the tenant-scoped
    /// catalog: [`count_tenant_footprint`] prunes to the tenant by its identity partition (a
    /// global table is counted whole).
    ///
    /// # Errors
    ///
    /// Returns `500` when the table cannot be loaded, or when [`count_tenant_footprint`] refuses
    /// to count it.
    pub(crate) async fn count_footprint(&self, entry: &IndexEntry) -> Result<Option<TenantFootprint>, OpenSearchError> {
        let table_type = self
            .namespace
            .table_type(entry.table())
            .await
            .map_err(|error| OpenSearchError::internal(error.to_string()))?;
        if table_type == Some(TableType::View) {
            return Ok(None);
        }
        let table = self
            .engine
            .catalog()
            .load_table(&icegate_table_ident(entry.table()))
            .await
            .map_err(QueryError::from)?;
        Ok(Some(count_tenant_footprint(&table, &self.tenant).await?))
    }

    /// The tenant's MAP keys in `table`: from the cache when the table's current snapshot and the
    /// aligned window end equal those of the cached enumeration, otherwise from a fresh
    /// enumeration, which is counted and cached.
    async fn list_map_keys(
        &self,
        table: &'static str,
        arrow_schema: &Schema,
        now: DateTime<Utc>,
    ) -> Result<Arc<MapKeys>, OpenSearchError> {
        let schema_carries_map_column = |config: &MetadataScanConfig| {
            arrow_schema
                .column_with_name(config.map_column)
                .is_some_and(|(_, field)| matches!(field.data_type(), DataType::Map(_, _)))
        };
        if !MAP_KEY_CONFIGS.iter().any(schema_carries_map_column) {
            return Ok(Arc::default());
        }
        let loaded_table = self
            .engine
            .catalog()
            .load_table(&icegate_table_ident(table))
            .await
            .map_err(QueryError::from)?;
        let window_end = align_discovery_window_end(now)?;
        let version = MapKeysVersion {
            snapshot_id: loaded_table.metadata().current_snapshot_id(),
            window_end_secs: window_end.timestamp(),
        };
        if let Some(keys) = self.map_key_cache.find_map_keys(&self.tenant, table, version).await {
            return Ok(keys);
        }
        // Collected only on a miss: a cache hit needs no configs.
        let configs: Vec<MetadataScanConfig> =
            MAP_KEY_CONFIGS.iter().copied().filter(schema_carries_map_column).collect();
        let window = TimeDelta::from_std(self.settings.field_discovery_window)
            .map_err(|error| OpenSearchError::internal(format!("field discovery window: {error}")))?;
        let window_start = window_end
            .checked_sub_signed(window)
            .ok_or_else(|| OpenSearchError::internal("field discovery window starts out of range"))?;
        let levels = scan_label_levels(
            &loaded_table,
            self.tenant.as_ref(),
            window_start,
            window_end,
            &configs,
            Predicate::AlwaysTrue,
        )
        .await
        .map_err(QueryError::from)?;
        self.metrics.add_map_key_enumeration(table);
        let keys: Arc<MapKeys> = Arc::new(
            configs
                .iter()
                .zip(levels)
                .map(|(config, keys)| (config.map_column.to_string(), keys))
                .collect(),
        );
        self.map_key_cache
            .store_map_keys(&self.tenant, table, version, Arc::clone(&keys))
            .await;
        Ok(keys)
    }
}

/// End of the discovery window for a request at `now`: the end of the minute that holds `now`.
/// Every request within one minute scans the same window, which is what lets [`MapKeyCache`]
/// answer as a fresh scan would.
///
/// # Errors
///
/// Returns `500` when the aligned end lies outside the representable time range.
fn align_discovery_window_end(now: DateTime<Utc>) -> Result<DateTime<Utc>, OpenSearchError> {
    let bucket_end_secs = (now.timestamp().div_euclid(DISCOVERY_WINDOW_BUCKET_SECS) + 1) * DISCOVERY_WINDOW_BUCKET_SECS;
    DateTime::from_timestamp(bucket_end_secs, 0)
        .ok_or_else(|| OpenSearchError::internal(format!("discovery window end {bucket_end_secs} is out of range")))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use iceberg::Catalog;
    use iceberg::spec::Type;
    use icegate_common::{EVENTS_TABLE, LOGS_TABLE, METRICS_TABLE, OPERATIONS_TABLE, SPANS_TABLE, schema};
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

    use super::*;
    use crate::infra::metrics::test_support::{build_meter_provider, find_counter_total};
    use crate::opensearch::error::render_envelope;
    use crate::opensearch::extract::ExpandWildcards;
    use crate::test_support::{LogAttributesRow, build_logs_warehouse, build_tenant, commit_log_attributes_rows};

    const TENANT_A: &str = "tenant-a";
    const TENANT_B: &str = "tenant-b";
    /// Inside the one-day window of every `now` below.
    const ROW_TIME: &str = "2026-10-10T12:00:00Z";
    /// The discovery window of the catalogs below, unless a test states its own.
    const ONE_DAY: Duration = Duration::from_hours(24);

    fn parse_instant(text: &str) -> DateTime<Utc> {
        text.parse().expect("an RFC 3339 instant")
    }

    /// A catalog over `engine` with a discovery window of `field_discovery_window`, counting its
    /// enumerations into `provider`.
    fn build_counted_catalog(
        engine: &Arc<QueryEngine>,
        provider: &SdkMeterProvider,
        field_discovery_window: Duration,
    ) -> IndexCatalog {
        IndexCatalog::new(
            Arc::clone(engine),
            Arc::new(QueryMetrics::new(&provider.meter("map_key_enumeration_test"))),
            CatalogSettings { field_discovery_window },
        )
    }

    /// The fields of `logs` listed by `session` at `now`; the error is the listing's own.
    async fn try_list_logs_fields(
        session: &TenantSession,
        now: &str,
        listing: FieldListing,
    ) -> Result<Vec<IndexField>, OpenSearchError> {
        let options = IndexOptions {
            ignore_unavailable: false,
            allow_no_indices: true,
            expand_wildcards: ExpandWildcards::parse(None).expect("the default expansion"),
        };
        let entries = session
            .indices()
            .resolve(
                &IndexExpression::parse(LOGS_TABLE).expect("a valid expression"),
                options,
            )
            .expect("logs resolves");
        let entry = entries.first().expect("one logs entry");
        session.list_fields(entry, parse_instant(now), listing).await
    }

    async fn list_logs_fields(session: &TenantSession, now: &str, listing: FieldListing) -> Vec<IndexField> {
        try_list_logs_fields(session, now, listing).await.expect("the fields of logs")
    }

    fn count_logs_enumerations(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> u64 {
        provider.force_flush().expect("flushed metrics");
        find_counter_total(
            exporter,
            "icegate_query_opensearch_map_key_enumerations",
            &[("table", LOGS_TABLE)],
        )
    }

    /// The names of the `log_attributes` key fields among `fields`, in listing order.
    fn list_log_attribute_fields(fields: &[IndexField]) -> Vec<&str> {
        fields
            .iter()
            .filter(|field| field.parent.as_deref() == Some(COL_LOG_ATTRIBUTES))
            .map(|field| field.name.as_str())
            .collect()
    }

    /// The field name of the `log_attributes` key `key`, built from the schema constant.
    fn build_log_attribute_field_name(key: &str) -> String {
        format!("{COL_LOG_ATTRIBUTES}.{key}")
    }

    /// Commit one `tenant-a` row at `timestamp` holding the one `log_attributes` entry `entry`.
    async fn commit_tenant_a_row(warehouse_catalog: &Arc<dyn Catalog>, timestamp: &str, entry: (&str, &str)) {
        commit_log_attributes_rows(
            warehouse_catalog,
            &[LogAttributesRow {
                tenant_id: TENANT_A,
                timestamp,
                log_attributes: &[entry],
            }],
        )
        .await;
    }

    /// A MAP column without an enumeration config would list as an `object` without keys and
    /// nothing would say so. Every top-level MAP column of every telemetry table has a config.
    #[test]
    fn every_map_column_of_a_telemetry_table_is_enumerated() {
        type SchemaBuilder = fn() -> icegate_common::Result<iceberg::spec::Schema>;
        let tables: [(&str, SchemaBuilder); 5] = [
            (LOGS_TABLE, schema::logs_schema),
            (SPANS_TABLE, schema::spans_schema),
            (EVENTS_TABLE, schema::events_schema),
            (METRICS_TABLE, schema::metrics_schema),
            (OPERATIONS_TABLE, schema::operations_schema),
        ];
        for (table, build_schema) in tables {
            let table_schema = build_schema().expect("a table schema");
            for column in table_schema.as_struct().fields() {
                if matches!(*column.field_type, Type::Map(_)) {
                    assert!(
                        MAP_KEY_CONFIGS.iter().any(|config| config.map_column == column.name),
                        "{table}.{} has no enumeration config",
                        column.name
                    );
                }
            }
        }
    }

    /// Every request within one minute scans the same window, which is what lets the cache answer
    /// as a fresh scan would. A request exactly on a minute boundary belongs to the minute that
    /// boundary starts.
    #[test]
    fn discovery_window_ends_at_the_end_of_the_minute_holding_the_request() {
        let cases = [
            ("2026-10-10T12:34:00Z", "2026-10-10T12:35:00Z"),
            ("2026-10-10T12:34:00.000001Z", "2026-10-10T12:35:00Z"),
            ("2026-10-10T12:34:59.999999Z", "2026-10-10T12:35:00Z"),
            ("2026-10-10T12:35:00Z", "2026-10-10T12:36:00Z"),
            ("2026-12-31T23:59:30Z", "2027-01-01T00:00:00Z"),
        ];
        for (now, expected) in cases {
            assert_eq!(
                align_discovery_window_end(parse_instant(now)).expect("in range"),
                parse_instant(expected),
                "{now}"
            );
        }
    }

    /// A columns-only listing lists the MAP column as a bare `object` and scans nothing.
    #[tokio::test]
    async fn a_columns_only_listing_enumerates_no_map_keys() {
        let warehouse = build_logs_warehouse().await;
        commit_tenant_a_row(&warehouse.catalog, ROW_TIME, ("http.method", "GET")).await;
        let (provider, exporter) = build_meter_provider();
        let session = build_counted_catalog(&warehouse.engine, &provider, ONE_DAY)
            .open_session(&build_tenant(TENANT_A))
            .await
            .expect("a session");

        let fields = list_logs_fields(&session, "2026-10-10T12:34:10Z", FieldListing::ColumnsOnly).await;

        assert_eq!(count_logs_enumerations(&provider, &exporter), 0);
        assert!(list_log_attribute_fields(&fields).is_empty());
        assert!(fields.contains(&IndexField::object(COL_LOG_ATTRIBUTES.to_string())));
    }

    #[tokio::test]
    async fn two_lookups_in_one_minute_and_one_snapshot_enumerate_once() {
        let warehouse = build_logs_warehouse().await;
        commit_tenant_a_row(&warehouse.catalog, ROW_TIME, ("http.method", "GET")).await;
        let (provider, exporter) = build_meter_provider();
        let session = build_counted_catalog(&warehouse.engine, &provider, ONE_DAY)
            .open_session(&build_tenant(TENANT_A))
            .await
            .expect("a session");

        let first_fields = list_logs_fields(&session, "2026-10-10T12:34:00Z", FieldListing::WithMapKeys).await;
        let second_fields = list_logs_fields(&session, "2026-10-10T12:34:59.999999Z", FieldListing::WithMapKeys).await;

        assert_eq!(count_logs_enumerations(&provider, &exporter), 1);
        assert_eq!(
            list_log_attribute_fields(&first_fields),
            [build_log_attribute_field_name("http.method")]
        );
        assert_eq!(first_fields, second_fields);
    }

    #[tokio::test]
    async fn a_lookup_in_the_next_minute_enumerates_again() {
        let warehouse = build_logs_warehouse().await;
        commit_tenant_a_row(&warehouse.catalog, ROW_TIME, ("http.method", "GET")).await;
        let (provider, exporter) = build_meter_provider();
        let session = build_counted_catalog(&warehouse.engine, &provider, ONE_DAY)
            .open_session(&build_tenant(TENANT_A))
            .await
            .expect("a session");

        list_logs_fields(&session, "2026-10-10T12:34:59.999999Z", FieldListing::WithMapKeys).await;
        let next_minute_fields = list_logs_fields(&session, "2026-10-10T12:35:00Z", FieldListing::WithMapKeys).await;

        assert_eq!(count_logs_enumerations(&provider, &exporter), 2);
        assert_eq!(
            list_log_attribute_fields(&next_minute_fields),
            [build_log_attribute_field_name("http.method")]
        );
    }

    #[tokio::test]
    async fn a_commit_between_two_lookups_enumerates_again() {
        let warehouse = build_logs_warehouse().await;
        commit_tenant_a_row(&warehouse.catalog, ROW_TIME, ("http.method", "GET")).await;
        let (provider, exporter) = build_meter_provider();
        let session = build_counted_catalog(&warehouse.engine, &provider, ONE_DAY)
            .open_session(&build_tenant(TENANT_A))
            .await
            .expect("a session");

        let fields_before_commit = list_logs_fields(&session, "2026-10-10T12:34:10Z", FieldListing::WithMapKeys).await;
        commit_tenant_a_row(&warehouse.catalog, ROW_TIME, ("user.id", "42")).await;
        let fields_after_commit = list_logs_fields(&session, "2026-10-10T12:34:20Z", FieldListing::WithMapKeys).await;

        assert_eq!(count_logs_enumerations(&provider, &exporter), 2);
        assert_eq!(
            list_log_attribute_fields(&fields_before_commit),
            [build_log_attribute_field_name("http.method")]
        );
        assert_eq!(
            list_log_attribute_fields(&fields_after_commit),
            [
                build_log_attribute_field_name("http.method"),
                build_log_attribute_field_name("user.id")
            ]
        );
    }

    /// The second listing of the first catalog is answered from its cache (still one
    /// enumeration); a second catalog, whose cache is empty, scans afresh. Both answers are equal,
    /// and neither holds the other tenant's key, although both tenants were committed together.
    #[tokio::test]
    async fn a_cached_key_listing_equals_a_fresh_listing() {
        let warehouse = build_logs_warehouse().await;
        commit_log_attributes_rows(
            &warehouse.catalog,
            &[
                LogAttributesRow {
                    tenant_id: TENANT_A,
                    timestamp: ROW_TIME,
                    log_attributes: &[("http.method", "GET")],
                },
                LogAttributesRow {
                    tenant_id: TENANT_B,
                    timestamp: ROW_TIME,
                    log_attributes: &[("b.only", "1")],
                },
            ],
        )
        .await;
        let (cached_provider, cached_exporter) = build_meter_provider();
        let (fresh_provider, fresh_exporter) = build_meter_provider();
        let cached_session = build_counted_catalog(&warehouse.engine, &cached_provider, ONE_DAY)
            .open_session(&build_tenant(TENANT_A))
            .await
            .expect("a session");
        let fresh_session = build_counted_catalog(&warehouse.engine, &fresh_provider, ONE_DAY)
            .open_session(&build_tenant(TENANT_A))
            .await
            .expect("a session");

        list_logs_fields(&cached_session, "2026-10-10T12:34:10Z", FieldListing::WithMapKeys).await;
        let cached_fields = list_logs_fields(&cached_session, "2026-10-10T12:34:30Z", FieldListing::WithMapKeys).await;
        let fresh_fields = list_logs_fields(&fresh_session, "2026-10-10T12:34:30Z", FieldListing::WithMapKeys).await;

        assert_eq!(count_logs_enumerations(&cached_provider, &cached_exporter), 1);
        assert_eq!(count_logs_enumerations(&fresh_provider, &fresh_exporter), 1);
        assert_eq!(cached_fields, fresh_fields);
        assert_eq!(
            list_log_attribute_fields(&cached_fields),
            [build_log_attribute_field_name("http.method")]
        );
    }

    /// For a request at 12:34:10 the one-day window runs from 2026-10-09T12:35:00Z through
    /// 2026-10-10T12:35:00Z, both ends included. Each row is its own append, so each lands in its
    /// own file: the rows one microsecond outside either end are listed only if the scan reads
    /// beyond the window.
    #[tokio::test]
    async fn only_keys_of_rows_inside_the_discovery_window_are_listed() {
        let warehouse = build_logs_warehouse().await;
        let rows = [
            ("2026-10-09T12:34:59.999999Z", "before.start"),
            ("2026-10-09T12:35:00Z", "at.start"),
            ("2026-10-10T12:35:00Z", "at.end"),
            ("2026-10-10T12:35:00.000001Z", "after.end"),
        ];
        for (timestamp, key) in rows {
            commit_tenant_a_row(&warehouse.catalog, timestamp, (key, "1")).await;
        }
        let (provider, _exporter) = build_meter_provider();
        let session = build_counted_catalog(&warehouse.engine, &provider, ONE_DAY)
            .open_session(&build_tenant(TENANT_A))
            .await
            .expect("a session");

        let fields = list_logs_fields(&session, "2026-10-10T12:34:10Z", FieldListing::WithMapKeys).await;

        assert_eq!(
            list_log_attribute_fields(&fields),
            [
                build_log_attribute_field_name("at.end"),
                build_log_attribute_field_name("at.start")
            ]
        );
    }

    /// A configured window reaching back before the earliest representable instant has no start
    /// to scan from, so the listing is a `500` rather than a scan over some other window. Both
    /// ways of overflowing are covered: a window too long for a time delta, and one whose start
    /// precedes the representable range.
    #[tokio::test]
    async fn a_discovery_window_starting_before_the_representable_range_is_a_500() {
        let warehouse = build_logs_warehouse().await;
        for window in [Duration::from_secs(u64::MAX), Duration::from_secs(10_000_000_000_000)] {
            let (provider, exporter) = build_meter_provider();
            let session = build_counted_catalog(&warehouse.engine, &provider, window)
                .open_session(&build_tenant(TENANT_A))
                .await
                .expect("a session");

            let error = try_list_logs_fields(&session, "2026-10-10T12:34:10Z", FieldListing::WithMapKeys)
                .await
                .expect_err("no representable window start");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{window:?}");
            assert_eq!(body["error"]["type"], "exception", "{window:?}");
            assert_eq!(count_logs_enumerations(&provider, &exporter), 0, "{window:?}");
        }
    }
}
