//! Parquet-metadata-direct scan for tag/label discovery on Iceberg tables
//! whose schema follows the `logs`/`spans`/`metrics` shape (a tenant
//! partition column, a timestamp column, some indexed top-level string
//! columns and a single `MAP<String, String>` attributes column).
//!
//! This module bypasses DataFusion entirely for the
//! Loki `/labels`, `/label_values` and Tempo `/api/search/tags`,
//! `/api/search/tag/{name}/values` metadata endpoints. The flow per request is:
//!
//! 1. Push a tenant-plus-time iceberg `Predicate` (optionally extended by
//!    callers with query-specific matchers) into `Table::scan().plan_files()`.
//! 2. For each file, open an [`iceberg::arrow::ArrowFileReader`] and read
//!    only the Parquet footer via `ParquetRecordBatchStreamBuilder::new`.
//! 3. Indexed-column labels are derived from row-group `null_count`
//!    statistics (zero data pages read). Attribute-MAP keys are derived by
//!    projecting only the `attributes.*.key` sub-column's dictionary page —
//!    only the MAP key chunks are fetched from object storage.
//! 4. For value enumeration, one column is projected per request (either the
//!    requested indexed column, or the `attributes` MAP) and values are
//!    collected in-process.
//!
//! **Limitation:** this module scans only committed Iceberg data. WAL
//! (Write-Ahead Log) segments are excluded — recent writes that have not yet
//! been shifted to Iceberg will not appear in metadata discovery results.
//!
//! Semantics: over-approximation is permitted. Matchers that cannot be
//! translated to an iceberg predicate are silently omitted — this only widens
//! the set of row groups considered, never narrows it.
//!
//! ## Generalization across tables
//!
//! The per-table vocabulary (which columns are indexed, which alias to what,
//! which map keys to exclude) is captured in [`MetadataScanConfig`]. Callers
//! build a config for their table (logs, spans, metrics) and pass it to
//! [`scan_labels`] / [`scan_label_values`].

mod error;
mod labels;
mod parquet_reader;
pub(crate) mod predicate;
mod values;

use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
pub use error::MetadataScanError;
use futures::{StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::table::Table;
pub use predicate::base_predicate;

/// Maximum number of Parquet files processed concurrently per request.
const METADATA_SCAN_CONCURRENCY: usize = 16;

/// Per-table configuration driving the metadata scan.
///
/// Captures everything that differs between tables (logs, spans, metrics):
/// - Which top-level columns are "indexed labels/tags" that should surface
///   in label/tag enumeration.
/// - Which labels are aliases for an underlying column (Grafana-compatible
///   renames such as `level` → `severity_text` or `service` →
///   `service_name`).
/// - Which map keys to hide from discovery (typically high-cardinality IDs
///   that already have a dedicated column).
#[derive(Debug, Clone, Copy)]
pub struct MetadataScanConfig {
    /// Top-level indexed string columns whose distinct-value set is surfaced
    /// as part of the label/tag enumeration output. High-cardinality
    /// identifiers (`trace_id`, `span_id`) typically live in a separate
    /// whitelist used only for value enumeration — see callers.
    pub indexed_columns: &'static [&'static str],
    /// `(alias, underlying)` mappings. For logs: `("level", "severity_text")`
    /// and `("service", "service_name")`. Used both to surface the alias in
    /// `/labels` output and to resolve alias→column in `/label_values`
    /// lookups.
    pub label_aliases: &'static [(&'static str, &'static str)],
    /// MAP<String,String> attribute keys to silently drop from `/labels`
    /// enumeration output. Useful for high-cardinality duplicates of indexed
    /// columns (e.g. the `attributes["trace_id"]` copy of the `trace_id`
    /// column).
    pub excluded_map_keys: &'static [&'static str],
    /// Name of the MAP<STRING,STRING> column to scan for attribute keys and
    /// values. Logs use a single `attributes` column; spans (after the
    /// 2026-04-19 split) have `resource_attributes` and `span_attributes`
    /// scanned via two separate configs.
    pub map_column: &'static str,
    /// Whether `/label_values`-style matching normalizes a stored map key
    /// before comparing it to the requested name, replacing every `.` with
    /// `_`.
    ///
    /// Attribute keys are always stored OTel-dotted (`user.id`). Loki label
    /// names are restricted to `[a-zA-Z_][a-zA-Z0-9_]*`, so Loki callers
    /// request the wire-form name (`user_id`) and need this set. `TraceQL`
    /// attribute names carry no such restriction and are requested dotted
    /// (`user.id`), so Tempo/spans configs need this unset — normalizing
    /// unconditionally would compare a normalized stored key against a
    /// still-dotted request and stop matching entries that are already
    /// exact. See `values::stored_key_matches_label` for the matching rule
    /// and its over-approximation trade-off on normalization collisions.
    pub normalize_keys: bool,
}

impl MetadataScanConfig {
    /// Resolve a user-facing label name to its underlying indexed column
    /// name, if it is an alias. Otherwise returns the input unchanged.
    #[must_use]
    pub fn resolve_column<'a>(&self, label: &'a str) -> &'a str {
        for (alias, underlying) in self.label_aliases {
            if *alias == label {
                return underlying;
            }
        }
        label
    }

    /// Whether `label` refers to a top-level indexed column (after alias
    /// resolution).
    #[must_use]
    pub fn is_indexed(&self, label: &str) -> bool {
        let resolved = self.resolve_column(label);
        self.indexed_columns.contains(&resolved)
    }

    /// Aliases that should be surfaced alongside a given underlying column
    /// when it is reported as having values in a row group.
    ///
    /// E.g. for logs with `("level", "severity_text")`, when `severity_text`
    /// has values this yields `["level"]`.
    pub(crate) fn aliases_for<'a>(&'a self, underlying: &'a str) -> impl Iterator<Item = &'static str> + 'a {
        self.label_aliases
            .iter()
            .filter(move |(_, u)| *u == underlying)
            .map(|(a, _)| *a)
    }
}

/// Compute the set of label/tag names visible in `table` for the given
/// tenant, time range, and additional predicate.
///
/// `configs` is the set of attribute levels to enumerate — one config per MAP
/// column, since [`MetadataScanConfig`] addresses a single one. Every level is
/// resolved from ONE Iceberg plan and ONE open of each data file: the levels of
/// a table live in the same Parquet files, so planning and footer-decoding per
/// level would repeat that work for a result that is unioned anyway. The
/// returned set is the union across levels; per-level exclusions and wire-name
/// rendering still apply within each level before it joins the union.
///
/// `extra_predicate` is AND'd with the tenant+time base predicate (see
/// [`base_predicate`]). Pass [`Predicate::AlwaysTrue`] to disable.
///
/// # Errors
///
/// Returns an error if iceberg planning fails or if a referenced Parquet file
/// cannot be read.
#[tracing::instrument(
    skip(table, configs, extra_predicate),
    fields(
        tenant_id = %tenant_id,
        num_configs = configs.len(),
        num_files = tracing::field::Empty,
    )
)]
pub async fn scan_labels(
    table: &Table,
    tenant_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    configs: &[MetadataScanConfig],
    extra_predicate: Predicate,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let predicate = predicate::combine(base_predicate(tenant_id, start, end), extra_predicate);
    let plan_stream = plan_files(table, &predicate).await?;
    let file_io = table.file_io().clone();

    let mut num_files: usize = 0;
    let mut stream = plan_stream
        .map_err(MetadataScanError::Iceberg)
        .map_ok(|task| {
            let file_io = file_io.clone();
            let predicate = predicate.clone();
            async move { scan_labels_for_file(&file_io, task, &predicate, configs).await }
        })
        .try_buffer_unordered(METADATA_SCAN_CONCURRENCY);

    let mut result: BTreeSet<String> = BTreeSet::new();
    while let Some(r) = stream.next().await {
        result.extend(r?);
        num_files += 1;
    }
    tracing::Span::current().record("num_files", num_files);

    Ok(result)
}

/// Compute the distinct values of a single label in `table` for the given
/// tenant, time range, and additional predicate.
///
/// `label_name` may be an alias (e.g. `"level"`) — the alias is resolved to
/// its underlying column via each config's `label_aliases`.
///
/// `configs` is the set of attribute levels to enumerate; the result is their
/// union. As in [`scan_labels`], the levels share one Iceberg plan and one open
/// per data file. Within a file the work is split by how each config classifies
/// `label_name`: levels resolving it to a top-level column contribute a
/// dictionary read of that column (once per DISTINCT column, however many
/// configs name it), and levels resolving it to a map key are served by a single
/// projected pass over all their map columns at once.
///
/// The union is deliberate and matches the per-level enumeration this replaced:
/// a value present at more than one level surfaces once, and no level shadows
/// another. Value enumeration is an over-approximation — see
/// `values::collect_map_values_from_batch`. Callers needing per-row precedence
/// between two levels want [`scan_coalesced_map_label_values`] instead.
///
/// # Errors
///
/// Returns an error if iceberg planning fails or if a referenced Parquet file
/// cannot be read.
#[tracing::instrument(
    skip(table, configs, extra_predicate),
    fields(
        tenant_id = %tenant_id,
        label_name = %label_name,
        num_configs = configs.len(),
        num_files = tracing::field::Empty,
    )
)]
pub async fn scan_label_values(
    table: &Table,
    tenant_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    configs: &[MetadataScanConfig],
    label_name: &str,
    extra_predicate: Predicate,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let predicate = predicate::combine(base_predicate(tenant_id, start, end), extra_predicate);

    // Classification is a property of (label, config), not of a file, so it is
    // resolved once here instead of per data file. Distinct indexed columns
    // rather than one per config: several levels resolving `label_name` to the
    // same top-level column would otherwise re-read one dictionary page each.
    let mut indexed_columns: Vec<String> = Vec::new();
    let mut map_configs: Vec<&MetadataScanConfig> = Vec::new();
    for config in configs {
        match values::classify_label(label_name, config) {
            values::LabelKind::Indexed => {
                let column = config.resolve_column(label_name).to_string();
                if !indexed_columns.contains(&column) {
                    indexed_columns.push(column);
                }
            }
            values::LabelKind::MapAttribute => map_configs.push(config),
        }
    }
    let indexed_columns = &indexed_columns;
    let map_configs = &map_configs;

    let plan_stream = plan_files(table, &predicate).await?;
    let file_io = table.file_io().clone();

    let label_name = label_name.to_string();
    let mut num_files: usize = 0;
    let mut stream =
        plan_stream
            .map_err(MetadataScanError::Iceberg)
            .map_ok(|task| {
                let file_io = file_io.clone();
                let label = label_name.clone();
                let predicate = predicate.clone();
                async move {
                    scan_label_values_for_file(&file_io, task, &predicate, indexed_columns, map_configs, &label).await
                }
            })
            .try_buffer_unordered(METADATA_SCAN_CONCURRENCY);

    let mut result: BTreeSet<String> = BTreeSet::new();
    while let Some(r) = stream.next().await {
        result.extend(r?);
        num_files += 1;
    }
    tracing::Span::current().record("num_files", num_files);

    Ok(result)
}

/// Compute distinct MAP values with per-row primary-over-fallback precedence.
///
/// Both configs are treated as MAP sources even when `label_name` resembles an
/// indexed column. For each row, matching values from `primary_config` are
/// collected when present; `fallback_config` contributes only when that row has
/// no non-null primary match. This mirrors a `COALESCE(primary[key],
/// fallback[key])` lookup without materializing unrelated columns.
///
/// # Errors
///
/// Returns an error if Iceberg planning fails, a referenced Parquet file cannot
/// be read, or either projected attribute column has an unexpected Arrow type.
#[tracing::instrument(
    skip(table, primary_config, fallback_config, extra_predicate),
    fields(
        tenant_id = %tenant_id,
        label_name = %label_name,
        num_files = tracing::field::Empty,
    )
)]
pub async fn scan_coalesced_map_label_values(
    table: &Table,
    tenant_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    primary_config: &MetadataScanConfig,
    fallback_config: &MetadataScanConfig,
    label_name: &str,
    extra_predicate: Predicate,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let predicate = predicate::combine(base_predicate(tenant_id, start, end), extra_predicate);
    let plan_stream = plan_files(table, &predicate).await?;
    let file_io = table.file_io().clone();

    let label_name = label_name.to_string();
    let mut num_files: usize = 0;
    let mut stream = plan_stream
        .map_err(MetadataScanError::Iceberg)
        .map_ok(|task| {
            let file_io = file_io.clone();
            let label_name = label_name.clone();
            let predicate = predicate.clone();
            async move {
                scan_coalesced_map_label_values_for_file(
                    &file_io,
                    task,
                    &predicate,
                    primary_config,
                    fallback_config,
                    &label_name,
                )
                .await
            }
        })
        .try_buffer_unordered(METADATA_SCAN_CONCURRENCY);

    let mut result = BTreeSet::new();
    while let Some(file_values) = stream.next().await {
        result.extend(file_values?);
        num_files += 1;
    }
    tracing::Span::current().record("num_files", num_files);

    Ok(result)
}

/// Compute the distinct INT32 values of a single named column in `table`
/// for the given tenant, time range, and additional predicate.
///
/// Used for low-cardinality enum columns (e.g. spans `status_code` /
/// `kind`) that aren't surfaced through `MetadataScanConfig` because
/// their wire-format values are integers, not strings. Callers are
/// responsible for spelling the codes back to user-facing names.
///
/// # Errors
///
/// Returns an error if iceberg planning fails or if a referenced Parquet
/// file cannot be read.
#[tracing::instrument(
    skip(table, extra_predicate),
    fields(
        tenant_id = %tenant_id,
        column = %column,
        num_files = tracing::field::Empty,
    )
)]
pub async fn scan_label_int_values(
    table: &Table,
    tenant_id: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    column: &str,
    extra_predicate: Predicate,
) -> Result<BTreeSet<i32>, MetadataScanError> {
    let predicate = predicate::combine(base_predicate(tenant_id, start, end), extra_predicate);
    let plan_stream = plan_files(table, &predicate).await?;
    let file_io = table.file_io().clone();

    let column = column.to_string();
    let mut num_files: usize = 0;
    let mut stream = plan_stream
        .map_err(MetadataScanError::Iceberg)
        .map_ok(|task| {
            let file_io = file_io.clone();
            let predicate = predicate.clone();
            let column = column.clone();
            async move { scan_label_int_values_for_file(&file_io, task, &predicate, &column).await }
        })
        .try_buffer_unordered(METADATA_SCAN_CONCURRENCY);

    let mut result: BTreeSet<i32> = BTreeSet::new();
    while let Some(r) = stream.next().await {
        result.extend(r?);
        num_files += 1;
    }
    tracing::Span::current().record("num_files", num_files);

    Ok(result)
}

/// Plan the files to scan for the given predicate, returning a stream
/// of [`FileScanTask`]s.
///
/// The previous implementation called `try_collect()` and materialized
/// the entire `Vec<FileScanTask>` before returning — fine for tiny
/// tables but a memory hazard once a tenant accumulates tens of
/// thousands of data files. Returning the stream lets callers feed
/// `buffer_unordered` directly so per-file readers fire as soon as the
/// catalog yields the first task.
async fn plan_files(table: &Table, predicate: &Predicate) -> Result<FileScanTaskStream, MetadataScanError> {
    let scan = table
        .scan()
        .with_filter(predicate.clone())
        .build()
        .map_err(MetadataScanError::Iceberg)?;
    scan.plan_files().await.map_err(MetadataScanError::Iceberg)
}

/// Process one Parquet file for a label/tag enumeration request.
///
/// Neither stage materializes row batches:
/// - Indexed-column labels are derived from `null_count` in row-group
///   statistics (pure Parquet metadata, zero pages).
/// - MAP attribute keys are derived from the dictionary page of the
///   `attributes.*.key` sub-column (one range read per row group, data
///   pages never decoded).
#[tracing::instrument(skip_all, fields(file = %task.data_file_path))]
async fn scan_labels_for_file(
    file_io: &iceberg::io::FileIO,
    task: FileScanTask,
    predicate: &Predicate,
    configs: &[MetadataScanConfig],
) -> Result<BTreeSet<String>, MetadataScanError> {
    // Opened once and reused across levels: the footer decode and the file
    // handle are properties of the file, not of the level being enumerated.
    let (mut reader, metadata) = parquet_reader::open_file_direct(file_io, &task).await?;

    let mut out: BTreeSet<String> = BTreeSet::new();
    for config in configs {
        // Each level accumulates into its OWN set, because
        // `collect_map_keys_via_dict` finishes by applying that level's
        // wire-name rendering and exclusions to everything in the set it was
        // given. Sharing one set across levels would re-apply a later level's
        // policy to an earlier level's names.
        let mut level: BTreeSet<String> = BTreeSet::new();
        labels::collect_indexed_from_metadata(&metadata, config, &mut level);
        labels::collect_map_keys_via_dict(&mut reader, &metadata, predicate, config, &mut level).await?;
        out.append(&mut level);
    }
    Ok(out)
}

/// Process one Parquet file for a label/tag-value enumeration request.
///
/// The two cases need different readers, so each is opened at most once and
/// only when some config actually selected it:
///
/// - `indexed_columns` (e.g. `service_name`, `level`): open the file directly
///   and read each column's dictionary page. No row batches.
/// - `map_configs` (non-indexed labels): open the file via the record-batch
///   stream builder and project every one of their map columns in a single
///   pass, so we can correlate `key == label_name` rows with their values.
#[tracing::instrument(skip_all, fields(file = %task.data_file_path))]
async fn scan_label_values_for_file(
    file_io: &iceberg::io::FileIO,
    task: FileScanTask,
    predicate: &Predicate,
    indexed_columns: &[String],
    map_configs: &[&MetadataScanConfig],
    label_name: &str,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let mut out: BTreeSet<String> = BTreeSet::new();

    if !indexed_columns.is_empty() {
        let (mut reader, metadata) = parquet_reader::open_file_direct(file_io, &task).await?;
        for column in indexed_columns {
            values::collect_indexed_values_via_dict(&mut reader, &metadata, predicate, column, &mut out).await?;
        }
    }

    if !map_configs.is_empty() {
        let builder = parquet_reader::open_builder(file_io, &task).await?;
        values::stream_map_values(builder, predicate, map_configs, label_name, &mut out).await?;
    }

    Ok(out)
}

/// Process one Parquet file for a coalesced MAP tag-value enumeration.
#[tracing::instrument(skip_all, fields(file = %task.data_file_path))]
async fn scan_coalesced_map_label_values_for_file(
    file_io: &iceberg::io::FileIO,
    task: FileScanTask,
    predicate: &Predicate,
    primary_config: &MetadataScanConfig,
    fallback_config: &MetadataScanConfig,
    label_name: &str,
) -> Result<BTreeSet<String>, MetadataScanError> {
    let builder = parquet_reader::open_builder(file_io, &task).await?;
    let mut out = BTreeSet::new();
    values::stream_coalesced_map_values(
        builder,
        predicate,
        primary_config,
        fallback_config,
        label_name,
        &mut out,
    )
    .await?;
    Ok(out)
}

/// Process one Parquet file for an INT32-indexed tag-value enumeration
/// request (e.g. `status_code`, `kind`).
///
/// Mirrors [`scan_labels_for_file`] / [`scan_label_values_for_file`] so the
/// per-file logic lives next to its tracing span and can be unit-tested in
/// isolation. The actual scan goes through a dictionary-page read on the
/// indexed column with row-group statistics fallback for chunks that skip
/// dict encoding — see
/// [`values::collect_indexed_int_values_via_dict`].
#[tracing::instrument(skip_all, fields(file = %task.data_file_path, column = %column))]
async fn scan_label_int_values_for_file(
    file_io: &iceberg::io::FileIO,
    task: FileScanTask,
    predicate: &Predicate,
    column: &str,
) -> Result<BTreeSet<i32>, MetadataScanError> {
    let (mut reader, metadata) = parquet_reader::open_file_direct(file_io, &task).await?;
    let mut out: BTreeSet<i32> = BTreeSet::new();
    values::collect_indexed_int_values_via_dict(&mut reader, &metadata, predicate, column, &mut out).await?;
    Ok(out)
}
