//! `GET /_cat/indices[/<expression>]`, `GET /_cat/plugins` and `GET /_cat/templates/<pattern>`.
//!
//! Values are strings or null, and `format` is matched case-insensitively. `docs.count` and the
//! store sizes are the requesting tenant's committed figures, never table-wide ones; rows still
//! in the WAL are not counted, and a view's figures are null. In text output a null value is an
//! empty cell between its separators. `_cat/indices` orders its rows by `s` when given; a figure
//! sorts by its number, never by its rendering.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::iter;

use axum::Json;
use axum::extract::{Extension, State};
use axum::http::header;
use axum::response::{IntoResponse, Response};
use futures::future::try_join_all;
use icegate_common::TenantId;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::opensearch::catalog::{IndexEntry, IndexExpression, IndexOptions};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, ExpandWildcards, Parameters, PathParameters, deserialize_flag};
use crate::opensearch::node::derive_identifier;
use crate::opensearch::search::request::SortOrder;
use crate::opensearch::state::OpenSearchState;

/// A `_cat/indices` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatIndicesColumn {
    /// `health`: always `green`.
    Health,
    /// `status`: always `open`.
    Status,
    /// `index`: the index name.
    Index,
    /// `uuid`: derived from the tenant and the index name.
    Uuid,
    /// `pri`: the primary shard count, always one.
    PrimaryShards,
    /// `rep`: the replica count, always zero.
    ReplicaShards,
    /// `docs.count`: the tenant's committed rows, or a system index's documents.
    DocsCount,
    /// `docs.deleted`: always zero.
    DocsDeleted,
    /// `store.size`: the bytes of the tenant's data files.
    StoreSize,
    /// `pri.store.size`: equal to `store.size`, there being one primary and no replica.
    PrimaryStoreSize,
}

impl CatIndicesColumn {
    /// The name the column is requested by in `h` and rendered under.
    const fn name(self) -> &'static str {
        match self {
            Self::Health => "health",
            Self::Status => "status",
            Self::Index => "index",
            Self::Uuid => "uuid",
            Self::PrimaryShards => "pri",
            Self::ReplicaShards => "rep",
            Self::DocsCount => "docs.count",
            Self::DocsDeleted => "docs.deleted",
            Self::StoreSize => "store.size",
            Self::PrimaryStoreSize => "pri.store.size",
        }
    }

    /// The column named `name` in `h`.
    ///
    /// # Errors
    ///
    /// Returns `illegal_argument_exception` for a name no column renders under.
    fn parse(name: &str) -> Result<Self, OpenSearchError> {
        DEFAULT_COLUMNS
            .into_iter()
            .find(|column| column.name() == name)
            .ok_or_else(|| OpenSearchError::illegal_argument(format!("unknown _cat/indices column [{name}]")))
    }

    /// Whether the column renders one of the tenant's figures, which have to be counted from the
    /// table's snapshot metadata; every other column renders without that read.
    const fn is_footprint_column(self) -> bool {
        match self {
            Self::DocsCount | Self::StoreSize | Self::PrimaryStoreSize => true,
            Self::Health
            | Self::Status
            | Self::Index
            | Self::Uuid
            | Self::PrimaryShards
            | Self::ReplicaShards
            | Self::DocsDeleted => false,
        }
    }

    /// Whether the column's figure is a byte size, which `bytes` decides how to write.
    const fn is_byte_size_column(self) -> bool {
        match self {
            Self::StoreSize | Self::PrimaryStoreSize => true,
            Self::Health
            | Self::Status
            | Self::Index
            | Self::Uuid
            | Self::PrimaryShards
            | Self::ReplicaShards
            | Self::DocsCount
            | Self::DocsDeleted => false,
        }
    }
}

/// One `s` key: the column rows are ordered by and the direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CatSortKey {
    /// The column whose values order the rows; `h` need not render it.
    column: CatIndicesColumn,
    /// The direction.
    order: SortOrder,
}

impl CatSortKey {
    /// Read one `s` key, `<column>` (ascending) or `<column>:asc` / `<column>:desc`, the column
    /// named as `h` names it.
    ///
    /// # Errors
    ///
    /// Returns `illegal_argument_exception` for a name no column renders under or a direction
    /// other than `asc` or `desc`.
    fn parse(text: &str) -> Result<Self, OpenSearchError> {
        let (column_name, order) = match text.split_once(':') {
            None => (text, SortOrder::Asc),
            Some((column_name, "asc")) => (column_name, SortOrder::Asc),
            Some((column_name, "desc")) => (column_name, SortOrder::Desc),
            Some((_, unknown_order)) => {
                return Err(OpenSearchError::illegal_argument(format!(
                    "unknown _cat/indices sort order [{unknown_order}]; pass [asc] or [desc]"
                )));
            }
        };
        Ok(Self {
            column: CatIndicesColumn::parse(column_name)?,
            order,
        })
    }
}

/// Columns rendered when `h` is absent, in IceGate's order; also every column `h` may name, so
/// each variant of [`CatIndicesColumn`] is listed once.
const DEFAULT_COLUMNS: [CatIndicesColumn; 10] = [
    CatIndicesColumn::Health,
    CatIndicesColumn::Status,
    CatIndicesColumn::Index,
    CatIndicesColumn::Uuid,
    CatIndicesColumn::PrimaryShards,
    CatIndicesColumn::ReplicaShards,
    CatIndicesColumn::DocsCount,
    CatIndicesColumn::DocsDeleted,
    CatIndicesColumn::StoreSize,
    CatIndicesColumn::PrimaryStoreSize,
];

/// The plugin components `_cat/plugins` lists: the plugin APIs IceGate emulates, in component
/// order.
const EMULATED_PLUGIN_COMPONENTS: [&str; 2] = ["opensearch-observability", "opensearch-sql"];

/// The columns of `_cat/plugins`, in order.
const PLUGIN_COLUMNS: [&str; 3] = ["name", "component", "version"];

/// The columns of `_cat/templates`, in order.
const TEMPLATE_COLUMNS: [&str; 5] = ["name", "index_patterns", "order", "version", "composed_of"];

/// The figures of one `_cat/indices` row; `None` renders as null.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CatFigures {
    /// `docs.count`.
    document_count: Option<u64>,
    /// `store.size` and `pri.store.size`.
    store_bytes: Option<u64>,
}

/// One `_cat/indices` row before it is rendered: the index and its figures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CatIndexRow<'a> {
    /// The index name.
    index: &'a str,
    /// Its figures.
    figures: CatFigures,
}

/// One `_cat/indices` value before it is rendered. A figure stays a number, so `s` orders it
/// numerically and `bytes` decides only how it is written. A column always yields the same
/// variant, so the derived order between the variants never decides a sort.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum CatValue<'a> {
    /// A count or a byte size.
    Figure(u64),
    /// Any other value: borrowed when fixed or held by the request, owned only when computed
    /// for it. A constant column (`pri`, `rep`, `docs.deleted`) is text: every row holds the
    /// same value, so its representation never orders two rows.
    Text(Cow<'a, str>),
}

/// A row's value under one `s` key, ordered the way that key orders rows: a null value after
/// every value in either direction.
#[derive(Debug, PartialEq, Eq)]
struct CatSortValue<'a> {
    /// The value; `None` is null.
    value: Option<CatValue<'a>>,
    /// The key's direction.
    order: SortOrder,
}

impl Ord for CatSortValue<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.value, &other.value) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(value), Some(other_value)) => match self.order {
                SortOrder::Asc => value.cmp(other_value),
                SortOrder::Desc => other_value.cmp(value),
            },
        }
    }
}

impl PartialOrd for CatSortValue<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The rendering a `_cat` request asks for with `format`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CatFormat {
    /// Space-separated columns, one row per line (`format` absent or `txt`).
    Text,
    /// An array of objects keyed by column (`format=json`).
    Json,
}

impl CatFormat {
    /// Read `format`, matched case-insensitively.
    ///
    /// # Errors
    ///
    /// Returns `illegal_argument_exception` for any value other than `txt` or `json`.
    fn parse(text: Option<&str>) -> Result<Self, OpenSearchError> {
        match text {
            None => Ok(Self::Text),
            Some(format) if format.eq_ignore_ascii_case("txt") => Ok(Self::Text),
            Some(format) if format.eq_ignore_ascii_case("json") => Ok(Self::Json),
            Some(unknown_format) => Err(OpenSearchError::illegal_argument(format!(
                "format [{unknown_format}] is not served for _cat"
            ))),
        }
    }
}

/// One rendered `_cat` value, `None` when null: borrowed when fixed or held by the request, owned
/// only when computed for it.
type CatCell<'a> = Option<Cow<'a, str>>;

/// How a store size is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ByteRendering {
    /// `bytes` absent: `1.5kb`.
    Human,
    /// `bytes=b`: the plain number of bytes.
    Bytes,
}

/// Query parameters of `_cat/indices`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatIndicesParameters {
    /// `format`: `json` or text (`txt`), matched case-insensitively; absent means text.
    format: Option<String>,
    /// `h`: the comma list of columns to render.
    #[serde(rename = "h")]
    columns: Option<String>,
    /// `bytes`: `b` renders store sizes as plain byte counts; absent means human sizes.
    bytes: Option<String>,
    /// `v`: whether text output starts with a header line.
    #[serde(rename = "v", default, deserialize_with = "deserialize_flag")]
    has_header: Option<bool>,
    /// What the listing expands to, with or without an expression; absent means open indices.
    expand_wildcards: Option<String>,
    /// `s`: the comma list of `<column>[:asc|:desc]` keys the rows are ordered by; absent keeps
    /// the listing's order.
    #[serde(rename = "s")]
    sort: Option<String>,
}

impl EndpointParameters for CatIndicesParameters {
    const IGNORED: &'static [&'static str] = &["local", "cluster_manager_timeout", "master_timeout", "time"];
}

/// Query parameters of `_cat/plugins`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatPluginsParameters {
    /// `format`: `json` or text (`txt`), matched case-insensitively; absent means text.
    format: Option<String>,
    /// `v`: whether text output starts with a header line.
    #[serde(rename = "v", default, deserialize_with = "deserialize_flag")]
    has_header: Option<bool>,
}

impl EndpointParameters for CatPluginsParameters {
    const IGNORED: &'static [&'static str] = &["local", "cluster_manager_timeout", "master_timeout"];
}

/// Query parameters of `_cat/templates`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatTemplatesParameters {
    /// `format`: `json` or text (`txt`), matched case-insensitively; absent means text.
    format: Option<String>,
    /// `v`: whether text output starts with a header line.
    #[serde(rename = "v", default, deserialize_with = "deserialize_flag")]
    has_header: Option<bool>,
}

impl EndpointParameters for CatTemplatesParameters {
    const IGNORED: &'static [&'static str] = &["local", "cluster_manager_timeout", "master_timeout"];
}

/// `GET /_cat/indices[/<expression>]`; `GET /_cat/indices/` (trailing slash) names no index and
/// answers like `/_cat/indices`. Without an expression every index the tenant sees is listed,
/// exact-name tables included, as long as `expand_wildcards` expands to open indices; otherwise
/// nothing is listed. A dot-prefixed system index is listed only when a part starts with `.` or
/// `expand_wildcards` includes hidden; an alias lists the index it names. A system index's
/// `docs.count` is its document count, and its sizes are null.
///
/// With `s`, each later key breaks the ties of the keys before it, rows equal under every key
/// keep the listing's order, and a null value sorts last in either direction. A key may name a
/// column `h` does not render.
pub(crate) async fn list_cat_indices(
    State(state): State<OpenSearchState>,
    Extension(tenant): Extension<TenantId>,
    index: Option<PathParameters<String>>,
    Parameters(parameters): Parameters<CatIndicesParameters>,
) -> Result<Response, OpenSearchError> {
    let format = CatFormat::parse(parameters.format.as_deref())?;
    let columns: Vec<CatIndicesColumn> = match parameters.columns.as_deref() {
        Some(requested_columns) => requested_columns
            .split(',')
            .map(CatIndicesColumn::parse)
            .collect::<Result<_, _>>()?,
        None => DEFAULT_COLUMNS.to_vec(),
    };
    let sort_keys: Vec<CatSortKey> = match parameters.sort.as_deref() {
        Some(requested_keys) => requested_keys.split(',').map(CatSortKey::parse).collect::<Result<_, _>>()?,
        None => Vec::new(),
    };
    let byte_rendering = match parameters.bytes.as_deref() {
        None => ByteRendering::Human,
        Some("b") => ByteRendering::Bytes,
        Some(unknown_unit) => {
            return Err(OpenSearchError::illegal_argument(format!(
                "bytes [{unknown_unit}] is not served; omit it or pass [b]"
            )));
        }
    };
    let expand_wildcards = ExpandWildcards::parse(parameters.expand_wildcards.as_deref())?;
    let expression = index
        .map(|PathParameters(expression_text)| IndexExpression::parse(&expression_text))
        .transpose()?;
    let session = state.catalog.open_session(&tenant).await?;
    let (entries, system_indices): (Vec<&IndexEntry>, Vec<String>) = match expression {
        // No expression lists every index, exact-name tables included, which no wildcard selects.
        // Like a wildcard, it honours whether `expand_wildcards` expands to open indices, so it
        // lists nothing otherwise. The system indices are hidden, so they are listed only when
        // hidden indices are asked for too.
        None if expand_wildcards.is_open => {
            // Asking for hidden indices lists every system index; each alias names one of them,
            // so the aliases add none.
            let system_indices = if expand_wildcards.is_hidden {
                session
                    .read_system_snapshot()
                    .await?
                    .snapshot
                    .index_names()
                    .map(str::to_string)
                    .collect()
            } else {
                Vec::new()
            };
            (session.indices().entries().iter().collect(), system_indices)
        }
        None => (Vec::new(), Vec::new()),
        Some(expression) => {
            let options = IndexOptions {
                ignore_unavailable: false,
                allow_no_indices: true,
                expand_wildcards,
            };
            let listing = session.list_targets(&expression, options).await?;
            let system_indices = session.resolve_listed_system_indices(listing.system_listing).await?;
            (listing.data_entries, system_indices)
        }
    };
    // A count loads the table from the catalog and reads its manifests, so it runs only when a
    // requested column renders a figure or a sort key orders by one. Uncounted entries pair with
    // `None`, which no requested column or key reads.
    let needs_footprints = columns
        .iter()
        .copied()
        .chain(sort_keys.iter().map(|key| key.column))
        .any(CatIndicesColumn::is_footprint_column);
    let footprints = if needs_footprints {
        try_join_all(entries.iter().map(|entry| session.count_footprint(entry))).await?
    } else {
        Vec::new()
    };
    let mut rows: Vec<CatIndexRow<'_>> = entries
        .iter()
        .zip(footprints.into_iter().chain(iter::repeat(None)))
        .map(|(entry, footprint)| CatIndexRow {
            index: entry.name(),
            figures: CatFigures {
                document_count: footprint.map(|footprint| footprint.record_count),
                store_bytes: footprint.map(|footprint| footprint.file_size_bytes),
            },
        })
        .collect();
    // A system index has no storage of its own, so its sizes are null; its count is the number
    // of documents the tenant's snapshot holds for it. The session caches the snapshot, so this
    // is the snapshot the listing read, and a listing with no system index reads none.
    if !system_indices.is_empty() {
        let snapshot = &session.read_system_snapshot().await?.snapshot;
        for index in &system_indices {
            let system_index = snapshot
                .find_index(index)
                .ok_or_else(|| OpenSearchError::index_not_found(index))?;
            rows.push(CatIndexRow {
                index,
                figures: CatFigures {
                    document_count: Some(system_index.document_count()),
                    store_bytes: None,
                },
            });
        }
    }
    // The keys are derived once per row rather than once per comparison, since a `uuid` key
    // hashes; `sort_by_cached_key` is stable, so ties keep the listing's order.
    if !sort_keys.is_empty() {
        rows.sort_by_cached_key(|row| {
            sort_keys
                .iter()
                .map(|key| CatSortValue {
                    value: derive_cell_value(key.column, row.index, row.figures, &tenant),
                    order: key.order,
                })
                .collect::<Vec<_>>()
        });
    }
    let cells: Vec<Vec<CatCell<'_>>> = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| render_cell(*column, row.index, row.figures, &tenant, byte_rendering))
                .collect()
        })
        .collect();
    let column_names: Vec<&str> = columns.iter().copied().map(CatIndicesColumn::name).collect();
    Ok(render_rows(
        &column_names,
        cells,
        format,
        parameters.has_header.unwrap_or(false),
    ))
}

/// `GET /_cat/plugins`: one row per emulated plugin, named after the node and versioned
/// `<configured version>.0`. Reads no table.
pub(crate) async fn list_cat_plugins(
    State(state): State<OpenSearchState>,
    Parameters(parameters): Parameters<CatPluginsParameters>,
) -> Result<Response, OpenSearchError> {
    let format = CatFormat::parse(parameters.format.as_deref())?;
    let plugin_version = format!("{}.0", state.node.version);
    let rows: Vec<Vec<CatCell<'_>>> = EMULATED_PLUGIN_COMPONENTS
        .into_iter()
        .map(|component| {
            vec![
                Some(Cow::Borrowed(state.node.node_name.as_str())),
                Some(Cow::Borrowed(component)),
                Some(Cow::Borrowed(plugin_version.as_str())),
            ]
        })
        .collect();
    Ok(render_rows(
        &PLUGIN_COLUMNS,
        rows,
        format,
        parameters.has_header.unwrap_or(false),
    ))
}

/// `GET /_cat/templates/<pattern>`: no index template exists, so every pattern lists none. Reads
/// no table and no system index.
pub(crate) async fn list_index_templates(
    PathParameters(_): PathParameters<String>,
    Parameters(parameters): Parameters<CatTemplatesParameters>,
) -> Result<Response, OpenSearchError> {
    let format = CatFormat::parse(parameters.format.as_deref())?;
    Ok(render_rows(
        &TEMPLATE_COLUMNS,
        Vec::new(),
        format,
        parameters.has_header.unwrap_or(false),
    ))
}

/// The `column` value of the index `index`, `None` when null: a figure column is null when its
/// figure is `None`. The index `uuid` is derived from the tenant and the index name, so it is
/// stable per tenant and shared by no two tenants.
fn derive_cell_value<'a>(
    column: CatIndicesColumn,
    index: &'a str,
    figures: CatFigures,
    tenant: &TenantId,
) -> Option<CatValue<'a>> {
    match column {
        CatIndicesColumn::Health => Some(CatValue::Text(Cow::Borrowed("green"))),
        CatIndicesColumn::Status => Some(CatValue::Text(Cow::Borrowed("open"))),
        CatIndicesColumn::Index => Some(CatValue::Text(Cow::Borrowed(index))),
        CatIndicesColumn::Uuid => Some(CatValue::Text(Cow::Owned(derive_identifier(
            "index",
            &format!("{tenant}/{index}"),
        )))),
        CatIndicesColumn::PrimaryShards => Some(CatValue::Text(Cow::Borrowed("1"))),
        CatIndicesColumn::ReplicaShards | CatIndicesColumn::DocsDeleted => Some(CatValue::Text(Cow::Borrowed("0"))),
        CatIndicesColumn::DocsCount => figures.document_count.map(CatValue::Figure),
        CatIndicesColumn::StoreSize | CatIndicesColumn::PrimaryStoreSize => figures.store_bytes.map(CatValue::Figure),
    }
}

/// One `_cat/indices` cell of the index `index`: its [`derive_cell_value`] written out, a byte
/// size as `byte_rendering` asks.
fn render_cell<'a>(
    column: CatIndicesColumn,
    index: &'a str,
    figures: CatFigures,
    tenant: &TenantId,
    byte_rendering: ByteRendering,
) -> CatCell<'a> {
    derive_cell_value(column, index, figures, tenant).map(|value| match value {
        CatValue::Text(text) => text,
        CatValue::Figure(figure) if column.is_byte_size_column() && byte_rendering == ByteRendering::Human => {
            Cow::Owned(format_byte_size(figure))
        }
        CatValue::Figure(figure) => Cow::Owned(figure.to_string()),
    })
}

/// Render `_cat` rows in `format`. `has_header` (`v`) adds the column names as the first text
/// line; JSON keys every value by its column, in column order.
fn render_rows(columns: &[&str], rows: Vec<Vec<CatCell<'_>>>, format: CatFormat, has_header: bool) -> Response {
    match format {
        CatFormat::Json => Json(render_json_rows(columns, rows)).into_response(),
        CatFormat::Text => render_text_rows(columns, &rows, has_header),
    }
}

/// `rows` as a JSON array with one object per row, each value under its column and a null cell
/// as `null`.
fn render_json_rows(columns: &[&str], rows: Vec<Vec<CatCell<'_>>>) -> Value {
    Value::Array(
        rows.into_iter()
            .map(|row| {
                let object: Map<String, Value> = columns
                    .iter()
                    .zip(row)
                    .map(|(column, cell)| {
                        let value = cell.map_or(Value::Null, |text| Value::String(text.into_owned()));
                        ((*column).to_string(), value)
                    })
                    .collect();
                Value::Object(object)
            })
            .collect(),
    )
}

/// `rows` as `text/plain`: one line per row, cells separated by one space and a null cell empty,
/// after the column names when `has_header`.
fn render_text_rows(columns: &[&str], rows: &[Vec<CatCell<'_>>], has_header: bool) -> Response {
    let mut text = String::new();
    if has_header {
        push_text_line(&mut text, columns.iter().copied());
    }
    for row in rows {
        push_text_line(&mut text, row.iter().map(|cell| cell.as_deref().unwrap_or_default()));
    }
    ([(header::CONTENT_TYPE, "text/plain; charset=UTF-8")], text).into_response()
}

/// Append `cells` to `text` as one line, separated by single spaces.
fn push_text_line<'a>(text: &mut String, cells: impl Iterator<Item = &'a str>) {
    for (position, cell) in cells.enumerate() {
        if position > 0 {
            text.push(' ');
        }
        text.push_str(cell);
    }
    text.push('\n');
}

/// A byte count as `_cat` renders it without `bytes`: the largest binary unit (steps of 1024) the
/// count reaches, one decimal rounded half up, `.0` dropped. Integer arithmetic throughout, so no
/// count loses precision.
fn format_byte_size(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["b", "kb", "mb", "gb", "tb", "pb"];
    let bytes = u128::from(bytes);
    let mut unit = 0;
    let mut divisor: u128 = 1;
    while unit + 1 < UNITS.len() && bytes >= divisor * 1024 {
        divisor *= 1024;
        unit += 1;
    }
    let tenths = (bytes * 10 + divisor / 2) / divisor;
    let (whole, fraction) = (tenths / 10, tenths % 10);
    if fraction == 0 {
        format!("{whole}{}", UNITS[unit])
    } else {
        format!("{whole}.{fraction}{}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;
    use crate::opensearch::catalog::IndexSet;
    use crate::opensearch::error::render_envelope;
    use crate::test_support::build_tenant;

    /// The default `_cat/indices` columns and their order are IceGate's contract: a caller that
    /// sends no `h` reads them in this order, so a reorder fails here.
    #[test]
    fn default_columns_are_listed_in_the_contract_order() {
        assert_eq!(
            DEFAULT_COLUMNS.map(CatIndicesColumn::name),
            [
                "health",
                "status",
                "index",
                "uuid",
                "pri",
                "rep",
                "docs.count",
                "docs.deleted",
                "store.size",
                "pri.store.size"
            ]
        );
    }

    /// `h` names a column by the name it renders under, and only a known name is accepted.
    #[tokio::test]
    async fn every_default_column_parses_from_its_name_and_an_unknown_name_is_refused() {
        for column in DEFAULT_COLUMNS {
            assert_eq!(
                CatIndicesColumn::parse(column.name()).expect("a known column"),
                column,
                "{}",
                column.name()
            );
        }

        let error = CatIndicesColumn::parse("nonsense").expect_err("no such column");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
    }

    /// An index `uuid` is stable for one tenant and one index, and no two tenants or indices share
    /// one, so a client keying state by it never mixes two tenants' indices; two indices over one
    /// table (`spans` and its span profile indices) are two indices.
    #[test]
    fn index_uuids_differ_between_tenants_and_between_indices() {
        let index_set = IndexSet::from_table_names(["logs".to_string(), "spans".to_string()]);
        let [logs_entry, span_profile_entry, spans_entry, _, ss4o_traces_entry] =
            <&[IndexEntry; 5]>::try_from(index_set.entries())
                .expect("logs, otel-v1-apm-span-000001, spans and the ss4o logs and traces indices");
        let tenant_a = build_tenant("tenant-a");
        let tenant_b = build_tenant("tenant-b");
        let render_uuid = |entry: &IndexEntry, tenant: &TenantId| {
            let figures = CatFigures {
                document_count: None,
                store_bytes: None,
            };
            render_cell(
                CatIndicesColumn::Uuid,
                entry.name(),
                figures,
                tenant,
                ByteRendering::Human,
            )
            .map(Cow::into_owned)
        };

        let tenant_a_logs_uuid = render_uuid(logs_entry, &tenant_a);

        assert_eq!(tenant_a_logs_uuid, render_uuid(logs_entry, &tenant_a));
        assert_ne!(tenant_a_logs_uuid, render_uuid(logs_entry, &tenant_b));
        assert_ne!(tenant_a_logs_uuid, render_uuid(spans_entry, &tenant_a));
        assert_eq!(span_profile_entry.name(), "otel-v1-apm-span-000001");
        assert_ne!(
            render_uuid(span_profile_entry, &tenant_a),
            render_uuid(spans_entry, &tenant_a)
        );
        assert_eq!(ss4o_traces_entry.name(), "ss4o_traces-icegate-default");
        assert_ne!(
            render_uuid(ss4o_traces_entry, &tenant_a),
            render_uuid(spans_entry, &tenant_a)
        );
        assert_ne!(
            render_uuid(ss4o_traces_entry, &tenant_a),
            render_uuid(span_profile_entry, &tenant_a)
        );
    }

    /// `format` is matched case-insensitively, so its spelling never changes the answer; a value
    /// naming no served format is the caller's mistake.
    #[tokio::test]
    async fn cat_format_is_read_case_insensitively() {
        let cases = [
            (None, CatFormat::Text),
            (Some("txt"), CatFormat::Text),
            (Some("TXT"), CatFormat::Text),
            (Some("json"), CatFormat::Json),
            (Some("JSON"), CatFormat::Json),
            (Some("Json"), CatFormat::Json),
        ];
        for (format_text, expected) in cases {
            assert_eq!(
                CatFormat::parse(format_text).expect("a served format"),
                expected,
                "{format_text:?}"
            );
        }
        for unknown_format in ["yaml", ""] {
            let error = CatFormat::parse(Some(unknown_format)).expect_err("a format that is not served");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{unknown_format:?}");
            assert_eq!(
                body["error"]["type"], "illegal_argument_exception",
                "{unknown_format:?}"
            );
        }
    }

    /// Exactly the columns rendering a tenant figure need a count, so a listing without them reads
    /// no table metadata, and a listing with any of them is never left without its figures.
    #[test]
    fn exactly_the_figure_columns_are_footprint_columns() {
        let footprint_columns: Vec<&str> = DEFAULT_COLUMNS
            .into_iter()
            .filter(|column| column.is_footprint_column())
            .map(CatIndicesColumn::name)
            .collect();

        assert_eq!(footprint_columns, ["docs.count", "store.size", "pri.store.size"]);
    }

    /// Byte sizes without `bytes`: the largest unit the count reaches (steps of 1024), one
    /// decimal rounded half up, `.0` dropped. Worked by hand: 1023 stays bytes; 1025 is
    /// 1.0009 kb, which rounds to 1.0, rendered `1kb`; 1536 is 1.5 × 1024; 1048575 is 1023.999 kb
    /// (the unit steps only at 1024²), which rounds to 1024.0, rendered `1024kb`; 5.5 gb is
    /// 5 × 1024³ + 512 × 1024²; 1024⁵ is one pb.
    #[test]
    fn byte_sizes_render_with_one_decimal_and_binary_units() {
        let cases = [
            (0, "0b"),
            (1, "1b"),
            (1023, "1023b"),
            (1024, "1kb"),
            (1025, "1kb"),
            (1536, "1.5kb"),
            (1_048_575, "1024kb"),
            (1_048_576, "1mb"),
            (5 * 1024 * 1024 * 1024 + 512 * 1024 * 1024, "5.5gb"),
            (1_125_899_906_842_624, "1pb"),
        ];
        for (bytes, expected) in cases {
            assert_eq!(format_byte_size(bytes), expected, "{bytes}");
        }
    }
}
