//! The `OpenSearch` index catalog: where an index expression resolves to the tables and the system
//! indices a tenant may read, and where those tables' fields and the tenant's figures in them are
//! listed. Every `OpenSearch` handler that reaches table data starts here, so the tenant scope
//! cannot be skipped. Two metadata reads do not go through it: MAP-key enumeration (see
//! [`TenantSession::list_fields`]) prunes to the tenant by its identity partition, and so does the
//! footprint count (see [`TenantSession::count_footprint`]), except in a global table, which it
//! counts whole.

mod cross_row;
mod data_prepper;
mod encoding;
mod expression;
mod field;
mod map_key_cache;
mod native;
mod registry;
mod ss4o;

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::SchemaProvider;
use datafusion::common::TableReference;
use datafusion::datasource::{TableProvider, provider_as_source};
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, TableType};
use datafusion::prelude::SessionContext;
use futures::StreamExt as _;
use iceberg::expr::Predicate;
use icegate_common::schema::{
    COL_DATA_POINT_ATTRIBUTES, COL_LOG_ATTRIBUTES, COL_METADATA, COL_RESOURCE_ATTRIBUTES, COL_SCOPE_ATTRIBUTES,
    COL_SPAN_ATTRIBUTES,
};
use icegate_common::{ICEGATE_NAMESPACE, TenantId, icegate_table_ident};
use tokio::sync::OnceCell;

use self::cross_row::{AttachedTraceRoots, build_trace_root_table, join_trace_roots};
pub(crate) use self::cross_row::{
    CrossRowField, TraceRoots, TraceSelection, lookup_page_trace_roots, lookup_trace_roots, select_candidate_traces,
};
use self::data_prepper::DATA_PREPPER_TABLE;
pub(crate) use self::data_prepper::{DATA_PREPPER_SPAN_INDEX, SPAN_END_TIME_FIELD, TRACE_ID_FIELD};
pub(crate) use self::encoding::{
    EncodedField, ProfileFieldSource, ProfileTable, lower_derived_exists, lower_derived_range, lower_derived_term,
    lower_encoded_exists, lower_encoded_range, lower_encoded_term, render_derived_value, render_profile_document,
    render_profile_field,
};
use self::encoding::{is_nullable_encoded_id, list_profile_fields, resolve_profile_field};
use self::expression::ExpressionPart;
pub(crate) use self::expression::IndexExpression;
pub(crate) use self::field::{FieldType, FieldValue, IndexField, ResolvedField, SelectedField};
use self::map_key_cache::{MapKeyCache, MapKeys, MapKeysVersion};
pub(crate) use self::native::native_column_type;
use self::native::{add_metric_value_field, list_native_fields, resolve_native_field};
pub(crate) use self::registry::{IndexEntry, IndexOptions, IndexSet, IndexTarget, Profile, read_index_options};
use self::registry::{check_table_uniqueness, choose_index_target, find_telemetry_table, select_system_names};
use self::ss4o::find_ss4o_table;
use super::error::OpenSearchError;
use super::system_index::{CurrentSnapshot, SystemIndexStore, SystemSnapshot};
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
    /// Size of the memory pool each session of the listener gets for itself; every plan the
    /// session runs, concurrent ones included, draws from it (`opensearch.max_query_memory_bytes`).
    pub(crate) max_query_memory_bytes: usize,
    /// How far before the earliest decorated span a trace's root is looked for
    /// (`opensearch.max_trace_lookback_secs`).
    pub(crate) max_trace_lookback: Duration,
    /// The most traces one search reads the roots of (`opensearch.max_cross_row_traces`).
    pub(crate) max_cross_row_traces: usize,
}

/// Resolves indices and fields against the tables and the system indices a tenant may read.
#[derive(Clone)]
pub(crate) struct IndexCatalog {
    /// The engine whose sessions list the tables.
    engine: Arc<QueryEngine>,
    /// Where MAP-key enumerations are counted.
    metrics: Arc<QueryMetrics>,
    /// The store the tenants' system indices are read from.
    system_store: Arc<SystemIndexStore>,
    /// The catalog's settings, handed to every session.
    settings: CatalogSettings,
    /// MAP-key enumerations shared by every session of this catalog.
    map_key_cache: Arc<MapKeyCache>,
}

impl IndexCatalog {
    /// A catalog over the tables of `engine` and the system indices of `system_store`, counting
    /// MAP-key enumerations into `metrics`.
    pub(crate) fn new(
        engine: Arc<QueryEngine>,
        metrics: Arc<QueryMetrics>,
        system_store: Arc<SystemIndexStore>,
        settings: CatalogSettings,
    ) -> Self {
        Self {
            engine,
            metrics,
            system_store,
            settings,
            map_key_cache: Arc::new(MapKeyCache::new(MAP_KEY_CACHE_TENANTS)),
        }
    }

    /// Open a session scoped to `tenant`, with the data indices it sees: one per table its
    /// tenant-scoped catalog lists in the IceGate namespace, except dot-prefixed names, which are
    /// system indices. The tenant's system indices are read from the store when the session first
    /// needs them (see [`TenantSession::read_system_snapshot`]). This is the only way an
    /// `OpenSearch` handler reaches table data, so every read goes through the tenant scope,
    /// MAP-key enumeration and the footprint count aside (see [`TenantSession::list_fields`] and
    /// [`TenantSession::count_footprint`]).
    ///
    /// # Errors
    ///
    /// Returns `500` when the session cannot be created, the tenant scope cannot be installed, or
    /// the IceGate namespace is missing from the catalog.
    pub(crate) async fn open_session(&self, tenant: &TenantId) -> Result<TenantSession, OpenSearchError> {
        let context = self
            .engine
            .create_session_with_memory_limit(self.settings.max_query_memory_bytes)
            .await?;
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
        let tables = open_index_tables(namespace.as_ref(), &indices).await;
        Ok(TenantSession {
            context,
            tables,
            namespace,
            tenant: tenant.clone(),
            indices,
            engine: Arc::clone(&self.engine),
            metrics: Arc::clone(&self.metrics),
            settings: self.settings,
            map_key_cache: Arc::clone(&self.map_key_cache),
            system_store: Arc::clone(&self.system_store),
            system_snapshot: OnceCell::new(),
            trace_roots: RwLock::new(HashMap::new()),
            unattached_cross_row_reads: AtomicUsize::new(0),
        })
    }
}

/// The field table of a non-native profile index; `None` for a native index, whose fields are
/// its columns.
pub(crate) fn find_profile_table(entry: &IndexEntry) -> Option<&'static ProfileTable> {
    match entry.profile() {
        Profile::Native => None,
        Profile::DataPrepper => Some(&DATA_PREPPER_TABLE),
        Profile::Ss4o => find_ss4o_table(entry.table()),
    }
}

/// Open the table of every data index in `indices` through the tenant-scoped `namespace`.
async fn open_index_tables(
    namespace: &dyn SchemaProvider,
    indices: &IndexSet,
) -> HashMap<String, Result<Arc<dyn TableProvider>, OpenSearchError>> {
    let mut tables = HashMap::with_capacity(indices.entries().len());
    for entry in indices.entries() {
        if tables.contains_key(entry.table()) {
            continue;
        }
        let opened_table = match namespace.table(entry.table()).await {
            Ok(Some(provider)) => Ok(provider),
            Ok(None) => Err(OpenSearchError::index_not_found(entry.name())),
            Err(error) => Err(OpenSearchError::internal(format!(
                "table `{}` cannot be opened: {error}",
                entry.table()
            ))),
        };
        tables.insert(entry.table().to_string(), opened_table);
    }
    tables
}

/// The system indices and aliases an index expression lists.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SystemListing {
    /// Concrete system indices the expression names, in name order.
    pub(crate) indices: Vec<String>,
    /// Aliases the expression names, in name order.
    pub(crate) aliases: Vec<String>,
}

impl SystemListing {
    /// The concrete indices the listing names: its indices and the index of each alias, in name
    /// order, each once.
    pub(crate) fn resolve_concrete_indices(&self, snapshot: &SystemSnapshot) -> Vec<String> {
        let concrete_indices: BTreeSet<&str> = self
            .indices
            .iter()
            .map(String::as_str)
            .chain(self.aliases.iter().filter_map(|alias| snapshot.resolve_name(alias)))
            .collect();
        concrete_indices.into_iter().map(str::to_string).collect()
    }
}

/// What a listing endpoint (`_resolve/index`, `_cat/indices`) shows for an expression: the data
/// and the system indices it names, side by side.
#[derive(Debug)]
pub(crate) struct IndexListing<'a> {
    /// Data indices, in name order.
    pub(crate) data_entries: Vec<&'a IndexEntry>,
    /// System indices and aliases.
    pub(crate) system_listing: SystemListing,
}

/// The batches a plan returned, held against the memory pool of the session that ran it
/// ([`TenantSession::collect_reserved_batches`]).
pub(crate) struct ReservedBatches {
    /// The plan's output, in the order [`TenantSession::collect`] returns it.
    pub(crate) batches: Vec<RecordBatch>,
    /// Holds the batches' memory until it is dropped; whatever the caller builds from the batches
    /// grows it.
    pub(crate) reservation: QueryReservation,
}

/// A reservation in one session's memory pool, which holds `opensearch.max_query_memory_bytes`.
/// Its memory returns to the pool when it is dropped.
pub(crate) struct QueryReservation {
    /// The reservation itself.
    reservation: MemoryReservation,
    /// The pool's size, which a refusal names.
    limit_bytes: usize,
}

impl QueryReservation {
    /// An empty reservation of a consumer named `consumer_name` in `pool`, a pool of `limit_bytes`.
    pub(crate) fn new(pool: &Arc<dyn MemoryPool>, consumer_name: &str, limit_bytes: usize) -> Self {
        Self {
            reservation: MemoryConsumer::new(consumer_name).register(pool),
            limit_bytes,
        }
    }

    /// Grow the reservation by `additional_bytes`.
    ///
    /// # Errors
    ///
    /// Returns `400 illegal_argument_exception` naming the per-query memory limit when the pool
    /// cannot grant the growth; the reservation keeps its size then.
    pub(crate) fn reserve_bytes(&self, additional_bytes: usize) -> Result<(), OpenSearchError> {
        self.reservation
            .try_grow(additional_bytes)
            .map_err(|error| map_execution_error(error, self.limit_bytes))
    }
}

/// Map a planning or execution failure of a session whose memory pool holds `limit_bytes` to the
/// envelope. The memory pool refusing a reservation, at any depth of the error chain, is the
/// client's 400 naming the per-query limit; any other failure keeps the mapping of `QueryError`.
fn map_execution_error(error: DataFusionError, limit_bytes: usize) -> OpenSearchError {
    if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
        return OpenSearchError::memory_limit_exceeded(limit_bytes);
    }
    OpenSearchError::from(QueryError::from(error))
}

/// A tenant-scoped view of the catalog for one request.
pub(crate) struct TenantSession {
    /// The tenant-scoped session every plan of this request executes in. Its runtime holds the
    /// WAL object store, and its catalog is the tenant-scoped one `namespace` was taken from.
    context: SessionContext,
    /// The table provider of each listed data index, keyed by table name, opened with the
    /// session.
    ///
    /// Opening a table of the tenant-scoped namespace does no I/O: the engine loaded every table
    /// when it built its catalog provider, and opening only wraps one in the tenant filter.
    /// Holding them here keeps field resolution and scan building synchronous for every front
    /// end, so the recursive DSL lowering and the Lucene visitor never await per leaf. A table
    /// that fails to open keeps its error here and fails only the requests that reach it.
    tables: HashMap<String, Result<Arc<dyn TableProvider>, OpenSearchError>>,
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
    /// The store the tenant's system indices are read from.
    system_store: Arc<SystemIndexStore>,
    /// The tenant's system-index state, read at most once per session, so every system-index
    /// decision of one request sees one snapshot.
    system_snapshot: OnceCell<CurrentSnapshot>,
    /// The trace roots attached to a Data Prepper index for the search being answered, by index
    /// name, each with the table its joined scans read. A scan of an attached index joins them,
    /// so its cross-row fields read the joined root columns. `_msearch` answers several searches
    /// on one session, one after another, so each search detaches the roots of its indices before
    /// it probes them.
    trace_roots: RwLock<HashMap<String, Arc<AttachedTraceRoots>>>,
    /// How many times a cross-row field was resolved while its index had no roots attached:
    /// the probe that tells a search it must attach them.
    unattached_cross_row_reads: AtomicUsize,
}

impl fmt::Debug for TenantSession {
    /// The session's identity: its tenant and the data indices it sees. The DataFusion context,
    /// the opened tables and the shared handles have no useful rendering.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TenantSession")
            .field("tenant", &self.tenant)
            .field("indices", &self.indices)
            .finish_non_exhaustive()
    }
}

impl TenantSession {
    /// The data indices the tenant sees.
    pub(crate) const fn indices(&self) -> &IndexSet {
        &self.indices
    }

    /// The opened table behind `entry`.
    fn open_table(&self, entry: &IndexEntry) -> Result<Arc<dyn TableProvider>, OpenSearchError> {
        match self.tables.get(entry.table()) {
            Some(Ok(provider)) => Ok(Arc::clone(provider)),
            Some(Err(error)) => Err(error.clone()),
            None => Err(OpenSearchError::index_not_found(entry.name())),
        }
    }

    /// Resolve the field `name` of `entry`: a column of a native index, a field of the profile's
    /// table otherwise (see [`find_profile_table`]).
    ///
    /// `None` means the name names nothing in this index: a query leaf on it matches nothing.
    /// An index whose table failed to open also resolves nothing; its scan reports the failure,
    /// and every search scans before it lowers.
    ///
    /// A cross-row field resolves to its joined root column once the session attached the roots
    /// of `entry`; before that it resolves to [`FieldValue::CrossRow`] and counts itself (see
    /// [`Self::count_cross_row_reads`]).
    pub(crate) fn resolve_field(&self, entry: &IndexEntry, name: &str) -> Option<ResolvedField> {
        let schema = self.open_table(entry).ok()?.schema();
        let resolved_field = find_profile_table(entry).map_or_else(
            || resolve_native_field(&schema, entry.table(), name),
            |table| resolve_profile_field(table, &schema, name),
        )?;
        let FieldValue::CrossRow(field) = resolved_field.value else {
            return Some(resolved_field);
        };
        // A poisoned lock reads as no attachment: the read counts itself, and the attachment the
        // search then makes reports the poisoning.
        if self.find_attached_trace_roots(entry).ok().flatten().is_some() {
            return Some(ResolvedField {
                value: FieldValue::Column {
                    column: field.joined_column().to_string(),
                },
                ..resolved_field
            });
        }
        self.unattached_cross_row_reads.fetch_add(1, Ordering::Relaxed);
        Some(resolved_field)
    }

    /// The catalog settings the session was opened with.
    pub(crate) const fn settings(&self) -> CatalogSettings {
        self.settings
    }

    /// Join `roots` to every later scan of `entry`, so its cross-row fields resolve to the
    /// joined root columns until [`Self::detach_trace_roots`] drops them. A second attachment
    /// replaces the first.
    ///
    /// # Errors
    ///
    /// Returns `500` when the roots cannot be built into a table, or when the lock was poisoned
    /// by a panic of another writer.
    pub(crate) fn attach_trace_roots(&self, entry: &IndexEntry, roots: TraceRoots) -> Result<(), OpenSearchError> {
        let table = build_trace_root_table(&roots)?;
        self.trace_roots
            .write()
            .map_err(|_| report_poisoned_trace_roots())?
            .insert(entry.name().to_string(), Arc::new(AttachedTraceRoots { roots, table }));
        Ok(())
    }

    /// Drop the roots attached to `entry`, so its cross-row fields resolve unattached again and
    /// count themselves; a no-op when none are attached. Roots one search of a session attached
    /// therefore never answer the cross-row reads of the next one.
    ///
    /// # Errors
    ///
    /// Returns `500` when the lock was poisoned by a panic of another writer.
    pub(crate) fn detach_trace_roots(&self, entry: &IndexEntry) -> Result<(), OpenSearchError> {
        self.trace_roots
            .write()
            .map_err(|_| report_poisoned_trace_roots())?
            .remove(entry.name());
        Ok(())
    }

    /// The roots attached to `entry`, if any.
    ///
    /// # Errors
    ///
    /// Returns `500` when the lock was poisoned by a panic of another writer.
    fn find_attached_trace_roots(
        &self,
        entry: &IndexEntry,
    ) -> Result<Option<Arc<AttachedTraceRoots>>, OpenSearchError> {
        Ok(self
            .trace_roots
            .read()
            .map_err(|_| report_poisoned_trace_roots())?
            .get(entry.name())
            .cloned())
    }

    /// The session's running count of unattached cross-row resolutions: how many times a
    /// cross-row field was resolved while its index had no roots attached. Resolutions against
    /// attached roots are not counted, so a probe compares the count before and after it runs
    /// to learn whether it read a cross-row field.
    pub(crate) fn count_cross_row_reads(&self) -> usize {
        self.unattached_cross_row_reads.load(Ordering::Relaxed)
    }

    /// Whether `field`, resolved in `entry`, is an encoded id whose stored column admits NULL
    /// (see [`is_nullable_encoded_id`]): a sort or an aggregation refuses it.
    ///
    /// # Errors
    ///
    /// Returns the error the table failed to open with.
    pub(crate) fn is_nullable_encoded_id(
        &self,
        entry: &IndexEntry,
        field: &ResolvedField,
    ) -> Result<bool, OpenSearchError> {
        if !matches!(field.value, FieldValue::Encoded(EncodedField::HexIdOrEmpty { .. })) {
            return Ok(false);
        }
        Ok(is_nullable_encoded_id(self.table_schema(entry)?.as_ref(), &field.value))
    }

    /// The Arrow type of `column` in `entry`'s tenant-scoped table, `None` when it has no such
    /// column. A joined root column (see [`Self::scan_index`]) has its cross-row field's type.
    ///
    /// # Errors
    ///
    /// Returns the error the table failed to open with.
    pub(crate) fn column_data_type(
        &self,
        entry: &IndexEntry,
        column: &str,
    ) -> Result<Option<DataType>, OpenSearchError> {
        if let Some(field) = CrossRowField::find_by_joined_column(column) {
            return Ok(Some(field.data_type()));
        }
        let schema = self.table_schema(entry)?;
        Ok(schema.field_with_name(column).ok().map(|field| field.data_type().clone()))
    }

    /// The tenant-scoped schema of `entry`'s table (no `tenant_id`), in column order: the
    /// columns every scan of the index yields.
    ///
    /// # Errors
    ///
    /// Returns the error the table failed to open with.
    pub(crate) fn table_schema(&self, entry: &IndexEntry) -> Result<SchemaRef, OpenSearchError> {
        Ok(self.open_table(entry)?.schema())
    }

    /// A [`Self::scan_raw`] of `entry`, joined with the trace roots the session attached to
    /// `entry` (see [`Self::attach_trace_roots`]) so its cross-row fields read the joined root
    /// columns; unjoined when none are attached.
    ///
    /// # Errors
    ///
    /// Returns the error the table failed to open with, or `500` when DataFusion refuses the scan
    /// or the join.
    pub(crate) fn scan_index(&self, entry: &IndexEntry) -> Result<LogicalPlanBuilder, OpenSearchError> {
        let scan = self.scan_raw(entry)?;
        match self.find_attached_trace_roots(entry)? {
            Some(attached_roots) => join_trace_roots(scan, Arc::clone(&attached_roots.table)),
            None => Ok(scan),
        }
    }

    /// A scan of `entry`'s table through the tenant scope, never joined with trace roots.
    ///
    /// The table reference is built from its exact parts, never parsed, so the name passes no
    /// identifier normalization. Every row the scan yields belongs to the session's tenant, and
    /// `tenant_id` is not among its columns.
    ///
    /// # Errors
    ///
    /// Returns the error the table failed to open with, or `500` when DataFusion refuses the scan.
    fn scan_raw(&self, entry: &IndexEntry) -> Result<LogicalPlanBuilder, OpenSearchError> {
        let provider = self.open_table(entry)?;
        let reference = TableReference::full(
            self.engine.config().catalog_name.as_str(),
            ICEGATE_NAMESPACE,
            entry.table(),
        );
        LogicalPlanBuilder::scan(reference, provider_as_source(provider), None)
            .map_err(|error| OpenSearchError::from(QueryError::from(error)))
    }

    /// Execute `plan` in the tenant-scoped session and collect its batches.
    ///
    /// # Errors
    ///
    /// Returns `400 illegal_argument_exception` naming the per-query memory limit when the
    /// session's memory pool refuses a reservation; any other planning or execution failure as
    /// the envelope `From<QueryError>` maps it to.
    pub(crate) async fn collect(&self, plan: LogicalPlan) -> Result<Vec<RecordBatch>, OpenSearchError> {
        let frame = self
            .context
            .execute_logical_plan(plan)
            .await
            .map_err(|error| self.map_execution_error(error))?;
        frame.collect().await.map_err(|error| self.map_execution_error(error))
    }

    /// Execute `plan` in the tenant-scoped session and collect its batches, as [`Self::collect`]
    /// does, with every batch held against the session's memory pool as it arrives.
    ///
    /// The batches a plan returns are reserved here, as they arrive, so the answer counts against
    /// the per-query memory limit whatever operators produced it. They are reserved at their Arrow
    /// memory size under a consumer named `consumer_name`; the returned [`ReservedBatches`] keeps
    /// the reservation until it is dropped, and the caller grows it for whatever it builds from
    /// the batches.
    ///
    /// # Errors
    ///
    /// Returns `400 illegal_argument_exception` naming the per-query memory limit when the pool
    /// refuses a reservation, an operator's or the output's; any other planning or execution
    /// failure as the envelope `From<QueryError>` maps it to.
    pub(crate) async fn collect_reserved_batches(
        &self,
        plan: LogicalPlan,
        consumer_name: &str,
    ) -> Result<ReservedBatches, OpenSearchError> {
        let limit_bytes = self.settings.max_query_memory_bytes;
        let frame = self
            .context
            .execute_logical_plan(plan)
            .await
            .map_err(|error| map_execution_error(error, limit_bytes))?;
        let mut stream = frame
            .execute_stream()
            .await
            .map_err(|error| map_execution_error(error, limit_bytes))?;
        let reservation = QueryReservation::new(&self.context.runtime_env().memory_pool, consumer_name, limit_bytes);
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.map_err(|error| map_execution_error(error, limit_bytes))?;
            reservation.reserve_bytes(batch.get_array_memory_size())?;
            batches.push(batch);
        }
        Ok(ReservedBatches { batches, reservation })
    }

    /// Map a planning or execution failure of this session to the envelope; see
    /// [`map_execution_error`].
    fn map_execution_error(&self, error: DataFusionError) -> OpenSearchError {
        map_execution_error(error, self.settings.max_query_memory_bytes)
    }

    /// The tenant's system-index state, read from the store on the first call of this session.
    ///
    /// # Errors
    ///
    /// Returns the store's read error.
    pub(crate) async fn read_system_snapshot(&self) -> Result<&CurrentSnapshot, OpenSearchError> {
        self.system_snapshot
            .get_or_try_init(|| self.system_store.read(&self.tenant))
            .await
    }

    /// Resolve `expression` for a request that reads one kind of index: data indices, or system
    /// indices (aliases resolved). The system snapshot is read only when a part names a system
    /// index or `expand_wildcards` includes hidden. The data side resolves before the system side,
    /// as in [`Self::list_targets`], so a missing name on both sides is reported by its data name on
    /// every endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OpenSearchError::mixed_index_target`] when the expression selects data and system
    /// indices together; [`OpenSearchError::table_reached_through_profiles`] when
    /// it selects data indices only and reaches one table through several profiles without naming
    /// each index (see [`IndexSet::resolve`]); `index_not_found_exception` per `options`; the
    /// store's read error.
    pub(crate) async fn resolve_target(
        &self,
        expression: &IndexExpression,
        options: IndexOptions,
    ) -> Result<IndexTarget<'_>, OpenSearchError> {
        let partition = expression.partition_system_parts(options.expand_wildcards.is_hidden);
        // Each side may come out empty: only both together decide whether nothing was named.
        let side_options = IndexOptions {
            allow_no_indices: true,
            ..options
        };
        if partition.system_parts.is_empty() {
            // No part names a system index, so a target is data indices, whose rows are read:
            // each table reached once.
            let data_entries = match &partition.data_expression {
                Some(data_expression) => self.indices.resolve(data_expression, side_options)?,
                None => Vec::new(),
            };
            return choose_index_target(expression, data_entries, Vec::new(), options.allow_no_indices);
        }
        let selected_data_entries = match &partition.data_expression {
            Some(data_expression) => self.indices.select_entries(data_expression, side_options)?,
            None => Vec::new(),
        };
        let system_listing = self.select_system_targets(&partition.system_parts, side_options).await?;
        let system_indices = self.resolve_listed_system_indices(system_listing).await?;
        // Only a target of data indices reads their rows, so only it needs each table reached
        // once; an expression that also names system indices keeps the mix's own 400.
        if let Some(data_expression) = &partition.data_expression
            && system_indices.is_empty()
        {
            check_table_uniqueness(data_expression, &selected_data_entries)?;
        }
        let data_entries = selected_data_entries.into_iter().map(|(entry, _)| entry).collect();
        choose_index_target(expression, data_entries, system_indices, options.allow_no_indices)
    }

    /// The concrete system indices `system_listing` names, in name order, each once: its indices
    /// as listed when it names no alias, otherwise those and the index of each alias. Only a
    /// listing naming an alias reads the snapshot, which [`Self::list_targets`] already read for
    /// this session when it listed the alias.
    ///
    /// # Errors
    ///
    /// Returns the store's read error.
    pub(crate) async fn resolve_listed_system_indices(
        &self,
        system_listing: SystemListing,
    ) -> Result<Vec<String>, OpenSearchError> {
        if system_listing.aliases.is_empty() {
            return Ok(system_listing.indices);
        }
        Ok(system_listing.resolve_concrete_indices(&self.read_system_snapshot().await?.snapshot))
    }

    /// List what `expression` names on both sides, for the endpoints that show data and system
    /// indices together. A table reached through several profiles is listed once per index, with
    /// no uniqueness check (see [`IndexSet::resolve_listing`]). The system snapshot is read as for
    /// [`Self::resolve_target`].
    ///
    /// # Errors
    ///
    /// Returns `index_not_found_exception` per `options`, and the store's read error.
    pub(crate) async fn list_targets(
        &self,
        expression: &IndexExpression,
        options: IndexOptions,
    ) -> Result<IndexListing<'_>, OpenSearchError> {
        let partition = expression.partition_system_parts(options.expand_wildcards.is_hidden);
        // Each side may come out empty: only both together decide whether nothing was named.
        let side_options = IndexOptions {
            allow_no_indices: true,
            ..options
        };
        let data_entries = match &partition.data_expression {
            Some(data_expression) => self.indices.resolve_listing(data_expression, side_options)?,
            None => Vec::new(),
        };
        let system_listing = if partition.system_parts.is_empty() {
            SystemListing::default()
        } else {
            self.select_system_targets(&partition.system_parts, side_options).await?
        };
        let is_nothing_named =
            data_entries.is_empty() && system_listing.indices.is_empty() && system_listing.aliases.is_empty();
        if is_nothing_named && !options.allow_no_indices {
            return Err(OpenSearchError::index_not_found(expression.text()));
        }
        Ok(IndexListing {
            data_entries,
            system_listing,
        })
    }

    /// The system indices and aliases `parts` select in the tenant's snapshot.
    async fn select_system_targets(
        &self,
        parts: &[ExpressionPart],
        options: IndexOptions,
    ) -> Result<SystemListing, OpenSearchError> {
        let snapshot = &self.read_system_snapshot().await?.snapshot;
        // Selection needs the names sorted; both lists come in name order, and no alias shares
        // an index's name.
        let names = merge_name_ordered_lists(snapshot.index_names(), snapshot.alias_names(), |name| *name);
        let (indices, aliases): (Vec<&str>, Vec<&str>) = select_system_names(&names, parts, options)?
            .into_iter()
            .partition(|name| snapshot.find_index(name).is_some());
        Ok(SystemListing {
            indices: indices.into_iter().map(str::to_string).collect(),
            aliases: aliases.into_iter().map(str::to_string).collect(),
        })
    }

    /// The fields of `entry`, in schema column order, or in its profile's declared order for an
    /// index of a non-native profile.
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
        match find_profile_table(entry) {
            None => {
                let mut fields = list_native_fields(&arrow_schema, map_keys)?;
                add_metric_value_field(entry.table(), &mut fields);
                Ok(fields)
            }
            Some(table) => list_profile_fields(table, &arrow_schema, map_keys),
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

/// The `500` for a trace-roots lock a panicking thread poisoned.
fn report_poisoned_trace_roots() -> OpenSearchError {
    OpenSearchError::internal("the session's trace roots lock is poisoned")
}

/// Merge `left` and `right`, each in name order, into one list in name order, in one pass over
/// both; on equal names the item of `left` comes first.
pub(crate) fn merge_name_ordered_lists<'a, T>(
    left: impl Iterator<Item = T>,
    right: impl Iterator<Item = T>,
    read_name: impl Fn(&T) -> &'a str,
) -> Vec<T> {
    let mut merged = Vec::with_capacity(left.size_hint().0 + right.size_hint().0);
    let mut left = left.peekable();
    let mut right = right.peekable();
    while let (Some(left_item), Some(right_item)) = (left.peek(), right.peek()) {
        let next_item = if read_name(right_item) < read_name(left_item) {
            right.next()
        } else {
            left.next()
        };
        merged.extend(next_item);
    }
    merged.extend(left);
    merged.extend(right);
    merged
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
    use rand::rngs::StdRng;
    use rand::{Rng as _, SeedableRng as _};

    use super::*;
    use crate::infra::metrics::test_support::{build_meter_provider, find_counter_total};
    use crate::opensearch::error::render_envelope;
    use crate::opensearch::extract::ExpandWildcards;
    use crate::test_support::{
        LogAttributesRow, SearchFixture, build_logs_warehouse, build_tenant, commit_log_attributes_rows,
        find_index_entry,
    };

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
            crate::test_support::build_in_memory_system_index_store(),
            CatalogSettings {
                field_discovery_window,
                max_query_memory_bytes: 1_073_741_824,
                max_trace_lookback: Duration::from_hours(1),
                max_cross_row_traces: 1_000,
            },
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

    /// Each side of a listing may come out empty on its own: only an expression naming nothing on
    /// either side is the missing-index `404`, and only when no indices are disallowed.
    #[tokio::test]
    async fn a_listing_naming_nothing_on_either_side_is_404_only_when_no_indices_are_disallowed() {
        let warehouse = build_logs_warehouse().await;
        let (provider, _exporter) = build_meter_provider();
        let session = build_counted_catalog(&warehouse.engine, &provider, ONE_DAY)
            .open_session(&build_tenant(TENANT_A))
            .await
            .expect("a session");
        let build_listing_options = |allow_no_indices: bool| IndexOptions {
            ignore_unavailable: true,
            allow_no_indices,
            expand_wildcards: ExpandWildcards::parse(Some("all")).expect("a served value"),
        };
        let nothing_expression = IndexExpression::parse("nothing*,.nothing*").expect("a valid expression");
        let logs_expression = IndexExpression::parse("logs,.nothing*").expect("a valid expression");

        let empty_listing = session
            .list_targets(&nothing_expression, build_listing_options(true))
            .await
            .expect("an empty listing");
        let error = session
            .list_targets(&nothing_expression, build_listing_options(false))
            .await
            .expect_err("nothing is named");
        let logs_listing = session
            .list_targets(&logs_expression, build_listing_options(false))
            .await
            .expect("logs is named");

        assert!(empty_listing.data_entries.is_empty());
        assert_eq!(empty_listing.system_listing, SystemListing::default());
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["index"], "nothing*,.nothing*");
        assert_eq!(
            logs_listing.data_entries.iter().map(|entry| entry.name()).collect::<Vec<_>>(),
            [LOGS_TABLE]
        );
        assert_eq!(logs_listing.system_listing, SystemListing::default());
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

    /// The merge takes the right item first, the left item first, or the rest of the side left
    /// over; these cases reach each, with either side empty, either side running out first,
    /// interleaved names and a tie.
    #[test]
    fn two_name_ordered_lists_merge_into_one_name_ordered_list() {
        let cases: [(&[&str], &[&str], &[&str]); 6] = [
            (&[], &[], &[]),
            (&[".a"], &[], &[".a"]),
            (&[], &[".a"], &[".a"]),
            (&[".b", ".d"], &[".a", ".c", ".e"], &[".a", ".b", ".c", ".d", ".e"]),
            (&[".a", ".b"], &[".c", ".d"], &[".a", ".b", ".c", ".d"]),
            (&[".c", ".d"], &[".a", ".b"], &[".a", ".b", ".c", ".d"]),
        ];
        for (left_names, right_names, expected_names) in cases {
            let merged_names =
                merge_name_ordered_lists(left_names.iter().copied(), right_names.iter().copied(), |name| *name);

            assert_eq!(merged_names, expected_names, "{left_names:?} and {right_names:?}");
        }
        let tied_items = merge_name_ordered_lists(
            [(".a", "left")].into_iter(),
            [(".a", "right")].into_iter(),
            |(name, _)| *name,
        );
        assert_eq!(tied_items, [(".a", "left"), (".a", "right")]);
    }

    /// Sorting the concatenation is the independent oracle; names are drawn from a small
    /// alphabet, so ties and long runs on one side are frequent.
    #[test]
    fn merging_agrees_with_sorting_the_concatenation_on_seeded_lists() {
        let mut generator = StdRng::seed_from_u64(0x5EED_3E26);
        let draw_sorted_names = |generator: &mut StdRng| -> Vec<String> {
            let name_count = generator.random_range(0..=6);
            let mut names: Vec<String> =
                (0..name_count).map(|_| format!(".{}", generator.random_range(0..10))).collect();
            names.sort_unstable();
            names
        };
        for _ in 0..1_000 {
            let left_names = draw_sorted_names(&mut generator);
            let right_names = draw_sorted_names(&mut generator);
            let mut expected_names: Vec<&str> = left_names.iter().chain(&right_names).map(String::as_str).collect();
            expected_names.sort_unstable();

            let merged_names = merge_name_ordered_lists(
                left_names.iter().map(String::as_str),
                right_names.iter().map(String::as_str),
                |name| *name,
            );

            assert_eq!(merged_names, expected_names, "{left_names:?} and {right_names:?}");
        }
    }

    /// A selection read back as comparable values: each field's resolution, or the MAP column a
    /// whole-map selection names.
    fn read_selected_fields(selected_fields: Vec<SelectedField>) -> Vec<(Option<ResolvedField>, Option<String>)> {
        selected_fields
            .into_iter()
            .map(|selected_field| match selected_field {
                SelectedField::Field(field) => (Some(field), None),
                SelectedField::AllMapValues { column } => (None, Some(column)),
            })
            .collect()
    }

    /// List and struct columns hold no value a full-text term compares with, so a pattern skips
    /// them; a MAP column it covers whole is one selection; and a pattern over the resource
    /// attributes reaches the column ingest moved `service.name` into.
    #[tokio::test]
    async fn a_field_pattern_skips_list_columns_and_reaches_promoted_resource_keys() {
        let fixture = SearchFixture::new().await;
        let session = fixture.open_session(TENANT_A).await;
        let spans = find_index_entry(&session, SPANS_TABLE);
        let logs = find_index_entry(&session, LOGS_TABLE);

        for list_pattern in ["event*", "link*"] {
            assert_eq!(
                read_selected_fields(session.expand_field_pattern(spans, list_pattern).expect("a selection")),
                Vec::new(),
                "[{list_pattern}] names only a list column"
            );
        }
        let every_span_field =
            read_selected_fields(session.expand_field_pattern(spans, "*").expect("a selection of every field"));
        for list_column in [schema::COL_EVENTS, schema::COL_LINKS] {
            assert!(
                every_span_field
                    .iter()
                    .all(|(field, _)| field.as_ref().is_none_or(|field| field.name != list_column)),
                "[*] skips [{list_column}]"
            );
        }
        assert!(every_span_field.contains(&(None, Some(COL_SPAN_ATTRIBUTES.to_string()))));
        assert!(every_span_field.contains(&(
            Some(ResolvedField {
                name: schema::COL_NAME.to_string(),
                field_type: FieldType::Keyword,
                value: FieldValue::Column {
                    column: schema::COL_NAME.to_string(),
                },
            }),
            None
        )));

        assert_eq!(
            read_selected_fields(
                session
                    .expand_field_pattern(logs, "resource_attributes.*")
                    .expect("a selection of every resource attribute")
            ),
            vec![
                (None, Some(COL_RESOURCE_ATTRIBUTES.to_string())),
                (
                    Some(ResolvedField {
                        name: schema::COL_SERVICE_NAME.to_string(),
                        field_type: FieldType::Keyword,
                        value: FieldValue::Column {
                            column: schema::COL_SERVICE_NAME.to_string(),
                        },
                    }),
                    None
                ),
            ]
        );
    }

    /// A name without `*` or `?` is not a glob: it resolves exactly like a field name, so a
    /// single MAP key is one keyword field rather than a partial selection of the map (which
    /// a glob refuses), and an unmapped name selects nothing.
    #[tokio::test]
    async fn a_field_pattern_without_glob_characters_resolves_like_a_field_name() {
        let fixture = SearchFixture::new().await;
        let session = fixture.open_session(TENANT_A).await;
        let logs = find_index_entry(&session, LOGS_TABLE);

        assert_eq!(
            read_selected_fields(
                session
                    .expand_field_pattern(logs, schema::COL_SERVICE_NAME)
                    .expect("a selection of one column")
            ),
            vec![(
                Some(ResolvedField {
                    name: String::from("service_name"),
                    field_type: FieldType::Keyword,
                    value: FieldValue::Column {
                        column: schema::COL_SERVICE_NAME.to_string(),
                    },
                }),
                None
            )]
        );
        assert_eq!(
            read_selected_fields(
                session
                    .expand_field_pattern(logs, "log_attributes.http.method")
                    .expect("a selection of one key")
            ),
            vec![(
                Some(ResolvedField {
                    name: String::from("log_attributes.http.method"),
                    field_type: FieldType::Keyword,
                    value: FieldValue::MapKey {
                        column: COL_LOG_ATTRIBUTES.to_string(),
                        key: String::from("http.method"),
                    },
                }),
                None
            )]
        );
        assert!(
            session
                .expand_field_pattern(logs, "no_such_field")
                .expect("an empty selection")
                .is_empty(),
            "an unmapped name selects nothing"
        );
    }
}
