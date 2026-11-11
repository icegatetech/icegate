//! The search request model every `_search` evaluator shares: the body keys and query-string
//! parameters a search accepts, parsed once into [`SearchRequest`].
//!
//! A body key is either applied, validated and not applied (`stored_fields`, `timeout`,
//! `highlight`, `terminate_after`), or refused: an unknown key is a `parsing_exception`, because
//! ignoring it would answer a different request.

use std::borrow::Cow;
use std::iter;

use serde::Deserialize;
use serde_json::{Map, Value};

use super::hits::DateRenderFormat;
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::{EndpointParameters, deserialize_flag};
use crate::opensearch::system_index::documents::{parse_json_body, split_paths};
use crate::query_dsl::ast::Query;
use crate::query_dsl::parse::parse_query;

/// Hits returned when a request names no `size`.
pub(crate) const DEFAULT_SEARCH_SIZE: usize = 10;

/// The total-hits threshold when a request names none and asks for no integer total.
pub(crate) const DEFAULT_TOTAL_HITS_THRESHOLD: u64 = 10_000;

/// The most clauses one `sort` may hold. Each clause costs a key per matching document and a
/// rendered value per hit, and repeating a clause costs the client nothing, so without this bound
/// a small request would multiply what a search holds in memory.
const MAX_SORT_CLAUSES: usize = 64;

/// The `numeric_type` values a field sort accepts.
const NUMERIC_TYPES: [&str; 4] = ["long", "double", "date", "date_nanos"];

/// A parsed search request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SearchRequest {
    /// The query; `match_all` when the body has none.
    pub(crate) query: Query,
    /// Hits skipped before the page.
    pub(crate) from: usize,
    /// Hits on the page.
    pub(crate) size: usize,
    /// The sort clauses, in priority order; empty for the default order.
    pub(crate) sort: Vec<SortClause>,
    /// Which part of `_source` each hit carries.
    pub(crate) source: SourceFilter,
    /// How `hits.total` is counted.
    pub(crate) track_total_hits: TrackTotalHits,
    /// Render `hits.total` as an integer.
    pub(crate) rest_total_hits_as_int: bool,
    /// Add `_seq_no` and `_primary_term` to each hit.
    pub(crate) seq_no_primary_term: bool,
    /// Add `_version: 1` to each hit.
    pub(crate) version: bool,
    /// The `aggs` / `aggregations` body, unparsed: the evaluator that supports them parses it.
    pub(crate) aggregations: Option<Value>,
    /// The sort values a page continues after; `None` when the body has no `search_after`.
    pub(crate) search_after: Option<Vec<Value>>,
    /// `fields`: values rendered under each hit's `fields`.
    pub(crate) fields: Vec<DocValueField>,
    /// `docvalue_fields`: rendered like `fields`; a name both carry keeps the `fields` value.
    pub(crate) docvalue_fields: Vec<DocValueField>,
    /// `terminate_after`: accepted and not applied; it adds `terminated_early: false`.
    pub(crate) terminate_after: Option<u64>,
}

/// One `fields` or `docvalue_fields` entry: a field name or glob, and an optional date format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocValueField {
    /// The field name, or a pattern with `*` / `?`.
    pub(crate) field: String,
    /// The date format of its values; `None` renders dates as `strict_date_optional_time`.
    pub(crate) format: Option<DateRenderFormat>,
}

impl DocValueField {
    /// Whether `field` is a glob pattern rather than one name.
    pub(crate) fn is_pattern(&self) -> bool {
        self.field.contains(['*', '?'])
    }
}

/// One sort clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SortClause {
    /// Sort on a field's values.
    Field {
        /// The field.
        field: String,
        /// The direction.
        order: SortOrder,
        /// The type assumed where the mapping lacks the field.
        unmapped_type: Option<String>,
        /// The numeric type the sort values are read as; validated, never applied (dates render
        /// as epoch milliseconds).
        numeric_type: Option<String>,
    },
    /// Sort on the score.
    Score {
        /// The direction (`desc` by default).
        order: SortOrder,
    },
    /// Sort in index order.
    Doc {
        /// The direction (`asc` by default).
        order: SortOrder,
    },
}

/// A sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SortOrder {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

impl SortOrder {
    /// The direction `text` names: `asc` or `desc`, and a `parsing_exception` for anything else.
    fn parse(text: &str) -> Result<Self, OpenSearchError> {
        match text {
            "asc" => Ok(Self::Asc),
            "desc" => Ok(Self::Desc),
            unknown_order => Err(OpenSearchError::parsing(format!(
                "unknown sort order [{unknown_order}]"
            ))),
        }
    }
}

/// Which part of `_source` a hit carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceFilter {
    /// The whole source.
    All,
    /// No `_source` key.
    Nothing,
    /// The selected paths (globs over dotted paths); an empty `includes` selects everything.
    Paths {
        /// Paths kept.
        includes: Vec<String>,
        /// Paths removed from the kept ones.
        excludes: Vec<String>,
    },
}

/// How `hits.total` is counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrackTotalHits {
    /// Count every hit.
    Exact,
    /// Count up to the threshold; above it the total is the threshold with relation `gte`.
    UpTo(u64),
    /// No total.
    Disabled,
}

/// Query-string parameters of `_search`.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SearchParameters {
    /// Overrides the body's `size`.
    pub(crate) size: Option<usize>,
    /// Overrides the body's `from`.
    pub(crate) from: Option<usize>,
    /// Overrides the body's `_source`: `true`, `false` or a comma list of paths.
    #[serde(rename = "_source")]
    pub(crate) source: Option<String>,
    /// Render `hits.total` as an integer.
    #[serde(default, deserialize_with = "deserialize_flag")]
    pub(crate) rest_total_hits_as_int: Option<bool>,
    /// Overrides the body's `track_total_hits`: `true`, `false` or an integer.
    pub(crate) track_total_hits: Option<String>,
    /// Overrides the body's `seq_no_primary_term`.
    #[serde(default, deserialize_with = "deserialize_flag")]
    pub(crate) seq_no_primary_term: Option<bool>,
    /// Index option: a missing concrete index is ignored rather than a 404.
    #[serde(default, deserialize_with = "deserialize_flag")]
    pub(crate) ignore_unavailable: Option<bool>,
    /// Index option: an expression that selects nothing answers no hits rather than a 404.
    #[serde(default, deserialize_with = "deserialize_flag")]
    pub(crate) allow_no_indices: Option<bool>,
    /// Index option: what wildcards expand to.
    pub(crate) expand_wildcards: Option<String>,
    /// Opens a scroll with this keep-alive (system indices only).
    pub(crate) scroll: Option<String>,
}

impl EndpointParameters for SearchParameters {
    /// Accepted with no effect: one replica answers from one shard, within the engine's own
    /// deadline, so there is no preference, shard concurrency, throttling or search type to
    /// choose.
    const IGNORED: &'static [&'static str] = &[
        "preference",
        "timeout",
        "max_concurrent_shard_requests",
        "ignore_throttled",
        "search_type",
    ];
}

/// Parse a search body (empty means `{}`) together with its query-string parameters.
///
/// # Errors
///
/// `parsing_exception` for malformed JSON, an unknown body key, a key of the wrong type, or a
/// `sort` of more than [`MAX_SORT_CLAUSES`] clauses; the errors of [`parse_query`];
/// `illegal_argument_exception` for a non-empty `script_fields`, an unsupported date format in
/// `fields` or `docvalue_fields`, an integer `track_total_hits` together with
/// `rest_total_hits_as_int=true` (see [`resolve_track_total_hits`]), or a malformed
/// `track_total_hits` parameter.
pub(crate) fn parse_search_request(
    body: &[u8],
    parameters: &SearchParameters,
) -> Result<SearchRequest, OpenSearchError> {
    let entries = parse_search_body(body)?
        .into_iter()
        .map(|(key, value)| (key, Cow::Owned(value)));
    parse_search_entries(entries, parameters)
}

/// Parse the members of a search body together with its query-string parameters. A `top_hits`
/// aggregation parses its own members through it, so they read exactly as `_search` reads them.
///
/// # Errors
///
/// `parsing_exception` for an unknown member, a member of the wrong type, or a `sort` of more
/// than [`MAX_SORT_CLAUSES`] clauses; the errors of [`parse_query`]; `illegal_argument_exception`
/// for a non-empty `script_fields`, an unsupported date format in `fields` or `docvalue_fields`,
/// an integer `track_total_hits` together with `rest_total_hits_as_int=true` (see
/// [`resolve_track_total_hits`]), or a malformed `track_total_hits` parameter.
pub(super) fn parse_search_object(
    object: Map<String, Value>,
    parameters: &SearchParameters,
) -> Result<SearchRequest, OpenSearchError> {
    parse_search_entries(
        object.into_iter().map(|(key, value)| (key, Cow::Owned(value))),
        parameters,
    )
}

/// Parse a search body already read as JSON (`null` means `{}`) together with its query-string
/// parameters. The body stays the caller's: only the `aggs` the request keeps are copied out of it.
///
/// # Errors
///
/// `parsing_exception` for a body that is neither `null` nor an object; past reading the JSON, the
/// errors of [`parse_search_request`].
pub(crate) fn parse_search_value(
    body: &Value,
    parameters: &SearchParameters,
) -> Result<SearchRequest, OpenSearchError> {
    match body {
        Value::Null => parse_search_entries(iter::empty::<(&str, Cow<'_, Value>)>(), parameters),
        Value::Object(object) => parse_search_entries(
            object.iter().map(|(key, value)| (key, Cow::Borrowed(value))),
            parameters,
        ),
        Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => Err(build_body_shape_error()),
    }
}

/// The request the entries of a search body and its query-string parameters describe. An entry's
/// value is owned or borrowed, so a borrowed body is copied only where the request keeps a value.
fn parse_search_entries<'a, K: AsRef<str>>(
    entries: impl IntoIterator<Item = (K, Cow<'a, Value>)>,
    parameters: &SearchParameters,
) -> Result<SearchRequest, OpenSearchError> {
    let mut request = SearchRequest {
        query: Query::MatchAll,
        from: 0,
        size: DEFAULT_SEARCH_SIZE,
        sort: Vec::new(),
        source: SourceFilter::All,
        track_total_hits: TrackTotalHits::UpTo(DEFAULT_TOTAL_HITS_THRESHOLD),
        rest_total_hits_as_int: parameters.rest_total_hits_as_int.unwrap_or(false),
        seq_no_primary_term: false,
        version: false,
        aggregations: None,
        search_after: None,
        fields: Vec::new(),
        docvalue_fields: Vec::new(),
        terminate_after: None,
    };
    let mut body_track_total_hits = None;
    for (key, value) in entries {
        let key = key.as_ref();
        match key {
            "query" => request.query = parse_query(&value)?,
            "from" => request.from = read_count(&value, key)?,
            "size" => request.size = read_count(&value, key)?,
            "sort" => request.sort = parse_sort(&value)?,
            "_source" => request.source = parse_body_source(&value)?,
            "track_total_hits" => body_track_total_hits = Some(parse_body_track_total_hits(&value)?),
            "seq_no_primary_term" => request.seq_no_primary_term = read_flag(&value, key)?,
            "version" => request.version = read_flag(&value, key)?,
            "aggs" | "aggregations" => request.aggregations = Some(value.into_owned()),
            "search_after" => {
                let Value::Array(values) = value.into_owned() else {
                    return Err(OpenSearchError::parsing("[search_after] must be an array"));
                };
                request.search_after = Some(values);
            }
            "fields" => request.fields = parse_doc_value_fields(&value, key)?,
            "docvalue_fields" => request.docvalue_fields = parse_doc_value_fields(&value, key)?,
            "terminate_after" => {
                request.terminate_after = Some(
                    value
                        .as_u64()
                        .ok_or_else(|| OpenSearchError::parsing("[terminate_after] must be a non-negative integer"))?,
                );
            }
            "stored_fields" => validate_stored_fields(&value)?,
            "script_fields" => refuse_script_fields(&value)?,
            "timeout" if value.is_string() => {}
            "timeout" => return Err(OpenSearchError::parsing("[timeout] must be a string such as 30000ms")),
            "highlight" if value.is_object() => {}
            "highlight" => return Err(OpenSearchError::parsing("[highlight] must be an object")),
            unknown_key => {
                return Err(OpenSearchError::parsing(format!(
                    "request does not support [{unknown_key}]"
                )));
            }
        }
    }
    if let Some(size) = parameters.size {
        request.size = size;
    }
    if let Some(from) = parameters.from {
        request.from = from;
    }
    if let Some(source) = parameters.source.as_deref() {
        request.source = parse_source_parameter(source);
    }
    if let Some(flag) = parameters.seq_no_primary_term {
        request.seq_no_primary_term = flag;
    }
    let requested_track_total_hits = match parameters.track_total_hits.as_deref() {
        Some(text) => Some(parse_track_total_hits_parameter(text)?),
        None => body_track_total_hits,
    };
    request.track_total_hits = resolve_track_total_hits(requested_track_total_hits, request.rest_total_hits_as_int)?;
    Ok(request)
}

/// The total-hit tracking of a request from its explicitly given `track_total_hits` (the query
/// string's `track_total_hits` when given, else the body's) and `rest_total_hits_as_int`.
///
/// # Errors
///
/// Returns a 400 `illegal_argument_exception` for an integer threshold together with
/// `rest_total_hits_as_int=true`: an integer total cannot say it is a lower bound.
pub(crate) fn resolve_track_total_hits(
    requested_track_total_hits: Option<TrackTotalHits>,
    rest_total_hits_as_int: bool,
) -> Result<TrackTotalHits, OpenSearchError> {
    match (requested_track_total_hits, rest_total_hits_as_int) {
        (None, true) => Ok(TrackTotalHits::Exact),
        (None, false) => Ok(TrackTotalHits::UpTo(DEFAULT_TOTAL_HITS_THRESHOLD)),
        (Some(TrackTotalHits::UpTo(threshold)), true) => Err(OpenSearchError::illegal_argument(format!(
            "[rest_total_hits_as_int] cannot be used when the total is tracked only up to [{threshold}]; \
             send track_total_hits=true"
        ))),
        (Some(track_total_hits), _) => Ok(track_total_hits),
    }
}

/// Parse the array of `fields` or `docvalue_fields` (named `key` in errors): each entry a
/// string or `{field, format}`.
///
/// # Errors
///
/// `parsing_exception` for a malformed entry, a non-string `field` or `format`, or an unknown
/// entry key; the 400 of [`DateRenderFormat::parse`] for an unknown format.
fn parse_doc_value_fields(value: &Value, key: &str) -> Result<Vec<DocValueField>, OpenSearchError> {
    let Value::Array(entries) = value else {
        return Err(OpenSearchError::parsing(format!("[{key}] must be an array")));
    };
    entries
        .iter()
        .map(|entry| match entry {
            Value::String(field) => Ok(DocValueField {
                field: field.clone(),
                format: None,
            }),
            Value::Object(object) => {
                let mut field = None;
                let mut format = None;
                for (name, item) in object {
                    match (name.as_str(), item) {
                        ("field", Value::String(text)) => field = Some(text.clone()),
                        ("format", Value::String(text)) => format = Some(DateRenderFormat::parse(text)?),
                        (entry_key @ ("field" | "format"), _) => {
                            return Err(OpenSearchError::parsing(format!(
                                "[{key}] entry key [{entry_key}] must be a string"
                            )));
                        }
                        (unknown_entry_key, _) => {
                            return Err(OpenSearchError::parsing(format!(
                                "[{key}] entry key [{unknown_entry_key}] is not supported"
                            )));
                        }
                    }
                }
                Ok(DocValueField {
                    field: field.ok_or_else(|| OpenSearchError::parsing(format!("[{key}] entry needs a [field]")))?,
                    format,
                })
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => Err(OpenSearchError::parsing(
                format!("[{key}] entries are field names or objects"),
            )),
        })
        .collect()
}

/// `stored_fields`: a string or an array of strings, accepted and never applied.
///
/// # Errors
///
/// `parsing_exception` for any other shape.
fn validate_stored_fields(value: &Value) -> Result<(), OpenSearchError> {
    let is_well_formed = match value {
        Value::String(_) => true,
        Value::Array(names) => names.iter().all(Value::is_string),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => false,
    };
    if is_well_formed {
        Ok(())
    } else {
        Err(OpenSearchError::parsing(
            "[stored_fields] must be a string or an array of strings",
        ))
    }
}

/// `script_fields`: accepted only when empty, because no script runs on the data path.
///
/// # Errors
///
/// `illegal_argument_exception` for an entry, `parsing_exception` for a value that is not an
/// object.
fn refuse_script_fields(value: &Value) -> Result<(), OpenSearchError> {
    match value {
        Value::Object(entries) if entries.is_empty() => Ok(()),
        Value::Object(_) => Err(OpenSearchError::illegal_argument(
            "[script_fields] is not supported: send it empty or leave it out",
        )),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Err(OpenSearchError::parsing("[script_fields] must be an object"))
        }
    }
}

/// The body as an object; an empty or blank body is an empty object.
///
/// # Errors
///
/// `parsing_exception` for malformed JSON or a body that is not an object.
pub(crate) fn parse_search_body(body: &[u8]) -> Result<Map<String, Value>, OpenSearchError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Map::new());
    }
    match parse_json_body(body)? {
        Value::Object(object) => Ok(object),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Array(_) => {
            Err(build_body_shape_error())
        }
    }
}

/// The refusal of a search body that is valid JSON but not an object.
fn build_body_shape_error() -> OpenSearchError {
    OpenSearchError::parsing("a search body must be a JSON object")
}

/// A non-negative JSON integer; `size` and `from` take numbers only.
fn read_count(value: &Value, key: &str) -> Result<usize, OpenSearchError> {
    value
        .as_u64()
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] must be a non-negative integer")))
}

/// A JSON boolean; a string or a number under `key` is a `parsing_exception`.
fn read_flag(value: &Value, key: &str) -> Result<bool, OpenSearchError> {
    value
        .as_bool()
        .ok_or_else(|| OpenSearchError::parsing(format!("[{key}] must be a boolean")))
}

/// The body's `track_total_hits`: a boolean or a non-negative JSON integer.
fn parse_body_track_total_hits(value: &Value) -> Result<TrackTotalHits, OpenSearchError> {
    let build_track_total_hits_error =
        || OpenSearchError::parsing("[track_total_hits] must be a boolean or a non-negative integer");
    match value {
        Value::Bool(true) => Ok(TrackTotalHits::Exact),
        Value::Bool(false) => Ok(TrackTotalHits::Disabled),
        Value::Number(number) => number
            .as_u64()
            .map(TrackTotalHits::UpTo)
            .ok_or_else(build_track_total_hits_error),
        Value::Null | Value::String(_) | Value::Array(_) | Value::Object(_) => Err(build_track_total_hits_error()),
    }
}

/// The query string's `track_total_hits`: `true`, `false` or a non-negative integer.
fn parse_track_total_hits_parameter(text: &str) -> Result<TrackTotalHits, OpenSearchError> {
    match text {
        "true" => Ok(TrackTotalHits::Exact),
        "false" => Ok(TrackTotalHits::Disabled),
        threshold_text => threshold_text.parse().map(TrackTotalHits::UpTo).map_err(|_| {
            OpenSearchError::illegal_argument(format!(
                "[track_total_hits] [{text}] is neither a boolean nor an integer"
            ))
        }),
    }
}

/// The body's `_source`: a boolean, a path, a list of paths or an `includes` / `excludes` object.
fn parse_body_source(value: &Value) -> Result<SourceFilter, OpenSearchError> {
    match value {
        Value::Bool(true) => Ok(SourceFilter::All),
        Value::Bool(false) => Ok(SourceFilter::Nothing),
        Value::String(_) | Value::Array(_) => Ok(SourceFilter::Paths {
            includes: read_paths(value, "_source")?,
            excludes: Vec::new(),
        }),
        Value::Object(object) => {
            if let Some(key) = object.keys().find(|key| !matches!(key.as_str(), "includes" | "excludes")) {
                return Err(OpenSearchError::parsing(format!("[_source] does not support [{key}]")));
            }
            let read_listed_paths =
                |key: &str| object.get(key).map_or_else(|| Ok(Vec::new()), |paths| read_paths(paths, key));
            Ok(SourceFilter::Paths {
                includes: read_listed_paths("includes")?,
                excludes: read_listed_paths("excludes")?,
            })
        }
        Value::Null | Value::Number(_) => Err(OpenSearchError::parsing(
            "[_source] must be a boolean, a path, a list of paths or an includes/excludes object",
        )),
    }
}

/// One path or a list of paths under `key`.
fn read_paths(value: &Value, key: &str) -> Result<Vec<String>, OpenSearchError> {
    let build_paths_error = || OpenSearchError::parsing(format!("[{key}] must be a path or a list of paths"));
    match value {
        Value::String(path) => Ok(vec![path.clone()]),
        Value::Array(paths) => paths
            .iter()
            .map(|path| path.as_str().map(str::to_string).ok_or_else(build_paths_error))
            .collect(),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => Err(build_paths_error()),
    }
}

/// The query-string `_source`: `true` (or empty), `false`, or a comma list of paths.
fn parse_source_parameter(text: &str) -> SourceFilter {
    match text {
        "true" | "" => SourceFilter::All,
        "false" => SourceFilter::Nothing,
        paths => SourceFilter::Paths {
            includes: split_paths(paths),
            excludes: Vec::new(),
        },
    }
}

/// The body's `sort`: a list of at most [`MAX_SORT_CLAUSES`] clauses, or one clause on its own.
fn parse_sort(value: &Value) -> Result<Vec<SortClause>, OpenSearchError> {
    match value {
        Value::Array(clauses) if clauses.len() > MAX_SORT_CLAUSES => Err(OpenSearchError::parsing(format!(
            "[sort] holds {} clauses, more than the limit of {MAX_SORT_CLAUSES}",
            clauses.len()
        ))),
        Value::Array(clauses) => clauses.iter().map(parse_sort_clause).collect(),
        single_clause => Ok(vec![parse_sort_clause(single_clause)?]),
    }
}

/// One sort clause: `"field"`, `{"field": "asc"}` or `{"field": {"order", "unmapped_type",
/// "numeric_type"}}`. On `_doc` and `_score` the object form takes `order`, and accepts and
/// ignores `unmapped_type`, since a document key has no mapping.
///
/// # Errors
///
/// `parsing_exception` for a malformed clause or an unknown option, and
/// `illegal_argument_exception` for a `numeric_type` outside [`NUMERIC_TYPES`], so no evaluator
/// receives one.
fn parse_sort_clause(value: &Value) -> Result<SortClause, OpenSearchError> {
    match value {
        Value::String(field) => Ok(build_sort_clause(field, None, None, None)),
        Value::Object(object) => {
            let mut entries = object.iter();
            let (Some((field, options)), None) = (entries.next(), entries.next()) else {
                return Err(OpenSearchError::parsing("a sort clause names exactly one field"));
            };
            match options {
                Value::String(order) => Ok(build_sort_clause(field, Some(SortOrder::parse(order)?), None, None)),
                Value::Object(options) => {
                    let is_metadata_sort = field == "_score" || field == "_doc";
                    let mut order = None;
                    let mut unmapped_type = None;
                    let mut numeric_type = None;
                    for (key, option) in options {
                        match (key.as_str(), option) {
                            ("order", Value::String(text)) => order = Some(SortOrder::parse(text)?),
                            ("unmapped_type", Value::String(_)) if is_metadata_sort => {}
                            ("unmapped_type", Value::String(type_name)) => unmapped_type = Some(type_name.clone()),
                            ("numeric_type", Value::String(type_name)) if !is_metadata_sort => {
                                numeric_type = Some(parse_numeric_type(type_name)?);
                            }
                            (unknown_option, _) => {
                                return Err(OpenSearchError::parsing(format!(
                                    "sort option [{unknown_option}] of [{field}] is not supported"
                                )));
                            }
                        }
                    }
                    Ok(build_sort_clause(field, order, unmapped_type, numeric_type))
                }
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => {
                    Err(OpenSearchError::parsing(format!("malformed sort clause for [{field}]")))
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => {
            Err(OpenSearchError::parsing("malformed sort clause"))
        }
    }
}

/// A field sort's `numeric_type`, one of [`NUMERIC_TYPES`].
///
/// # Errors
///
/// Returns `illegal_argument_exception` naming any other value.
fn parse_numeric_type(type_name: &str) -> Result<String, OpenSearchError> {
    if NUMERIC_TYPES.contains(&type_name) {
        return Ok(type_name.to_string());
    }
    Err(OpenSearchError::illegal_argument(format!(
        "[numeric_type] must be one of [{}], got [{type_name}]",
        NUMERIC_TYPES.join(", ")
    )))
}

/// The clause on `field`: `_score` defaults to `desc`, `_doc` and every field to `asc`; a field
/// clause carries its `unmapped_type` and `numeric_type`.
fn build_sort_clause(
    field: &str,
    order: Option<SortOrder>,
    unmapped_type: Option<String>,
    numeric_type: Option<String>,
) -> SortClause {
    match field {
        "_score" => SortClause::Score {
            order: order.unwrap_or(SortOrder::Desc),
        },
        "_doc" => SortClause::Doc {
            order: order.unwrap_or(SortOrder::Asc),
        },
        _ => SortClause::Field {
            field: field.to_string(),
            order: order.unwrap_or(SortOrder::Asc),
            unmapped_type,
            numeric_type,
        },
    }
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use serde_json::{Value, json};

    use super::*;
    use crate::opensearch::error::render_envelope;
    use crate::opensearch::extract::parse_parameters;
    use crate::query_dsl::ast::IdsQuery;

    fn parse_search_parameters(query_string: &str) -> SearchParameters {
        parse_parameters(Some(query_string)).expect("the parameters parse")
    }

    fn parse_request(body: &Value, query_string: &str) -> Result<SearchRequest, OpenSearchError> {
        parse_search_request(body.to_string().as_bytes(), &parse_search_parameters(query_string))
    }

    async fn assert_refused(body: &Value, query_string: &str, error_type: &str) {
        let error = parse_request(body, query_string).expect_err("the request is refused");
        let (status, answer) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body} ?{query_string}");
        assert_eq!(answer["error"]["type"], error_type, "{body} ?{query_string}");
    }

    #[tokio::test]
    async fn an_unknown_search_body_key_is_a_parsing_exception() {
        for body in [
            json!({"query": {"match_all": {}}, "rescore": {"window_size": 10}}),
            json!({"collapse": {"field": "x"}}),
            json!({"suggest": {}}),
        ] {
            assert_refused(&body, "", "parsing_exception").await;
        }
    }

    #[tokio::test]
    async fn a_search_body_value_of_the_wrong_shape_is_a_parsing_exception() {
        for body in [
            json!([1]),
            json!({"query": {"no_such_leaf": {}}}),
            json!({"size": "5"}),
            json!({"size": null}),
            json!({"from": -1}),
            json!({"version": "yes"}),
            json!({"seq_no_primary_term": "yes"}),
            json!({"track_total_hits": -1}),
            json!({"track_total_hits": 1.5}),
            json!({"track_total_hits": "all"}),
            json!({"track_total_hits": null}),
            json!({"_source": 1}),
            json!({"_source": null}),
            json!({"_source": [1]}),
            json!({"_source": {"include": ["type"]}}),
            json!({"_source": {"includes": 1}}),
        ] {
            assert_refused(&body, "", "parsing_exception").await;
        }
        let error = parse_search_request(b"not json", &parse_search_parameters("")).expect_err("malformed JSON");
        let (status, answer) = render_envelope(error).await;
        assert_eq!(
            (status, answer["error"]["type"].clone()),
            (StatusCode::BAD_REQUEST, json!("parsing_exception"))
        );
    }

    #[test]
    fn size_and_from_are_read_from_the_body_or_the_query_string() {
        let body_request = parse_request(&json!({"size": 5, "from": 2}), "").expect("parses");
        let parameter_request = parse_request(&json!({"size": 5, "from": 2}), "size=20&from=0").expect("parses");
        let default_request = parse_search_request(b"", &parse_search_parameters("")).expect("an empty body parses");
        let blank_body_request =
            parse_search_request(b" \n", &parse_search_parameters("")).expect("a blank body parses");

        assert_eq!((body_request.size, body_request.from), (5, 2));
        assert_eq!((parameter_request.size, parameter_request.from), (20, 0));
        assert_eq!(
            (
                default_request.size,
                default_request.from,
                default_request.query.clone()
            ),
            (10, 0, Query::MatchAll)
        );
        assert_eq!(default_request.source, SourceFilter::All);
        assert!(!default_request.seq_no_primary_term && !default_request.version);
        assert_eq!(default_request.aggregations, None);
        assert_eq!(blank_body_request, default_request);
    }

    /// A scroll reads its body as JSON once and keeps it: the request borrows from it, `null`
    /// stands for no body, and anything but an object is refused as the bytes would be.
    #[tokio::test]
    async fn a_body_read_as_json_parses_with_null_as_no_body() {
        let parameters = parse_search_parameters("size=3");
        let body = json!({"from": 1, "aggs": {"types": {"terms": {"field": "type"}}}});

        let request = parse_search_value(&body, &parameters).expect("an object parses");
        let null_request = parse_search_value(&Value::Null, &parameters).expect("null parses");

        assert_eq!((request.size, request.from), (3, 1));
        assert_eq!(
            request.aggregations,
            Some(json!({"types": {"terms": {"field": "type"}}}))
        );
        assert_eq!(
            (null_request.size, null_request.from, null_request.query),
            (3, 0, Query::MatchAll)
        );
        assert_eq!(null_request.aggregations, None);
        for non_object_body in [json!([1]), json!("x"), json!(5), json!(true)] {
            let error = parse_search_value(&non_object_body, &parameters).expect_err("refused");
            let (status, answer) = render_envelope(error).await;

            assert_eq!(status, StatusCode::BAD_REQUEST, "{non_object_body}");
            assert_eq!(answer["error"]["type"], "parsing_exception", "{non_object_body}");
        }
    }

    #[test]
    fn the_flags_query_and_aggregations_are_read_from_the_body() {
        let request = parse_request(
            &json!({
                "query": {"ids": {"values": ["dashboard:a"]}},
                "seq_no_primary_term": true,
                "version": true,
                "aggs": {"types": {"terms": {"field": "type"}}}
            }),
            "",
        )
        .expect("parses");
        let long_form_request = parse_request(&json!({"aggregations": {}}), "").expect("parses");
        let enabling_request =
            parse_request(&json!({"seq_no_primary_term": false}), "seq_no_primary_term=true").expect("parses");
        let disabling_request =
            parse_request(&json!({"seq_no_primary_term": true}), "seq_no_primary_term=false").expect("parses");

        assert_eq!(
            request.query,
            Query::Ids(IdsQuery {
                values: vec!["dashboard:a".to_string()]
            })
        );
        assert!(request.seq_no_primary_term && request.version);
        assert_eq!(
            request.aggregations,
            Some(json!({"types": {"terms": {"field": "type"}}}))
        );
        assert_eq!(long_form_request.aggregations, Some(json!({})));
        assert!(enabling_request.seq_no_primary_term);
        assert!(!disabling_request.seq_no_primary_term);
    }

    #[test]
    fn sort_clauses_parse_field_score_and_doc_forms_with_unmapped_type() {
        let request = parse_request(
            &json!({"sort": [
                "updated_at",
                {"type": "desc"},
                {"dashboard.title.keyword": {"order": "asc", "unmapped_type": "keyword"}},
                "_score",
                {"_doc": {"order": "desc"}},
                {"timestamp": {"order": "desc", "unmapped_type": "boolean", "numeric_type": "date_nanos"}},
                {"_doc": {"order": "desc", "unmapped_type": "boolean"}},
                {"_score": {"unmapped_type": "long"}},
                "_doc",
                {"_score": "asc"}
            ]}),
            "",
        )
        .expect("parses");
        let single_clause_request = parse_request(&json!({"sort": {"_id": {"order": "desc"}}}), "").expect("parses");

        assert_eq!(
            request.sort,
            vec![
                SortClause::Field {
                    field: "updated_at".to_string(),
                    order: SortOrder::Asc,
                    unmapped_type: None,
                    numeric_type: None,
                },
                SortClause::Field {
                    field: "type".to_string(),
                    order: SortOrder::Desc,
                    unmapped_type: None,
                    numeric_type: None,
                },
                SortClause::Field {
                    field: "dashboard.title.keyword".to_string(),
                    order: SortOrder::Asc,
                    unmapped_type: Some("keyword".to_string()),
                    numeric_type: None,
                },
                SortClause::Score { order: SortOrder::Desc },
                SortClause::Doc { order: SortOrder::Desc },
                SortClause::Field {
                    field: "timestamp".to_string(),
                    order: SortOrder::Desc,
                    unmapped_type: Some("boolean".to_string()),
                    numeric_type: Some("date_nanos".to_string()),
                },
                SortClause::Doc { order: SortOrder::Desc },
                SortClause::Score { order: SortOrder::Desc },
                SortClause::Doc { order: SortOrder::Asc },
                SortClause::Score { order: SortOrder::Asc },
            ]
        );
        assert_eq!(
            single_clause_request.sort,
            vec![SortClause::Field {
                field: "_id".to_string(),
                order: SortOrder::Desc,
                unmapped_type: None,
                numeric_type: None,
            }]
        );
    }

    /// An unknown option is refused rather than ignored, `numeric_type` included on `_doc` and
    /// `_score`, and so is every malformed clause.
    #[tokio::test]
    async fn sort_clauses_with_unknown_options_are_refused() {
        for body in [
            json!({"sort": [{"updated_at": {"order": "up"}}]}),
            json!({"sort": [{"updated_at": {"order": 1}}]}),
            json!({"sort": [{"updated_at": "up"}]}),
            json!({"sort": [{"updated_at": 1}]}),
            json!({"sort": [{"updated_at": {"missing": "_first"}}]}),
            json!({"sort": [{"updated_at": {"numeric_type": 5}}]}),
            json!({"sort": [{"_doc": {"missing": "_last"}}]}),
            json!({"sort": [{"_score": {"numeric_type": "long"}}]}),
            json!({"sort": [{"a": "asc", "b": "desc"}]}),
            json!({"sort": [{}]}),
            json!({"sort": [1]}),
        ] {
            assert_refused(&body, "", "parsing_exception").await;
        }
    }

    /// `numeric_type` is checked where every search request is parsed, so system and data indices
    /// refuse the same values.
    #[tokio::test]
    async fn a_numeric_type_outside_the_accepted_set_is_an_illegal_argument() {
        for numeric_type in ["long", "double", "date", "date_nanos"] {
            let request =
                parse_request(&json!({"sort": [{"timestamp": {"numeric_type": numeric_type}}]}), "").expect("parses");

            assert_eq!(
                request.sort,
                vec![SortClause::Field {
                    field: "timestamp".to_string(),
                    order: SortOrder::Asc,
                    unmapped_type: None,
                    numeric_type: Some(numeric_type.to_string()),
                }]
            );
        }
        for numeric_type in ["fortnight", "integer", "Long", ""] {
            let body = json!({"sort": [{"timestamp": {"numeric_type": numeric_type}}]});

            assert_refused(&body, "", "illegal_argument_exception").await;
        }
    }

    #[tokio::test]
    async fn search_after_parses_an_array_and_refuses_anything_else() {
        let request = parse_request(&json!({"search_after": [1_760_000_000_000_i64, "x", null]}), "").expect("parses");
        let parameters = parse_search_parameters("");
        let borrowed_body = json!({"search_after": ["y"]});

        assert_eq!(
            request.search_after,
            Some(vec![json!(1_760_000_000_000_i64), json!("x"), Value::Null])
        );
        assert_eq!(parse_request(&json!({}), "").expect("parses").search_after, None);
        assert_eq!(
            parse_search_value(&borrowed_body, &parameters).expect("parses").search_after,
            Some(vec![json!("y")])
        );
        for body in [
            json!({"search_after": 5}),
            json!({"search_after": "x"}),
            json!({"search_after": null}),
            json!({"search_after": {"a": 1}}),
        ] {
            assert_refused(&body, "", "parsing_exception").await;
        }
    }

    /// Repeating a clause costs the client nothing, so the limit counts clauses, repeated or not.
    #[tokio::test]
    async fn a_sort_is_accepted_up_to_its_clause_limit_and_refused_one_above_it() {
        let build_repeated_sort_body = |clause_count: usize| json!({"sort": vec!["labels"; clause_count]});

        for clause_count in [63, 64] {
            let request = parse_request(&build_repeated_sort_body(clause_count), "").expect("parses");

            assert_eq!(request.sort.len(), clause_count);
        }
        assert_refused(&build_repeated_sort_body(65), "", "parsing_exception").await;
    }

    #[tokio::test]
    async fn track_total_hits_resolves_against_rest_total_hits_as_int() {
        let cases = [
            (json!({}), "", TrackTotalHits::UpTo(10_000), false),
            (json!({}), "rest_total_hits_as_int=true", TrackTotalHits::Exact, true),
            (json!({}), "track_total_hits=false", TrackTotalHits::Disabled, false),
            (json!({"track_total_hits": 500}), "", TrackTotalHits::UpTo(500), false),
            (
                json!({"track_total_hits": true}),
                "rest_total_hits_as_int=true",
                TrackTotalHits::Exact,
                true,
            ),
            (
                json!({"track_total_hits": false}),
                "rest_total_hits_as_int=true",
                TrackTotalHits::Disabled,
                true,
            ),
            (
                json!({"track_total_hits": 500}),
                "track_total_hits=true",
                TrackTotalHits::Exact,
                false,
            ),
            (json!({}), "track_total_hits=25", TrackTotalHits::UpTo(25), false),
        ];
        for (body, query_string, expected_track_total_hits, rest_total_hits_as_int) in cases {
            let request = parse_request(&body, query_string).expect("parses");
            assert_eq!(
                (request.track_total_hits, request.rest_total_hits_as_int),
                (expected_track_total_hits, rest_total_hits_as_int),
                "{body} ?{query_string}"
            );
        }
        assert_refused(
            &json!({"track_total_hits": 500}),
            "rest_total_hits_as_int=true",
            "illegal_argument_exception",
        )
        .await;
        assert_refused(&json!({}), "track_total_hits=maybe", "illegal_argument_exception").await;
    }

    #[tokio::test]
    async fn rendering_options_parse_and_an_integer_total_threshold_refuses_integer_totals() {
        let no_parameters = parse_search_parameters("");
        let request = parse_search_request(
            br#"{"fields": ["*", {"field": "timestamp", "format": "strict_date_optional_time_nanos"}],
                "docvalue_fields": [{"field": "timestamp", "format": "date_time"}],
                "stored_fields": ["*"], "script_fields": {}, "timeout": "30000ms", "terminate_after": 100000,
                "highlight": {"fields": {"*": {}}, "fragment_size": 2147483647}}"#,
            &no_parameters,
        )
        .expect("a rendering-options body parses");
        assert_eq!(
            request.fields,
            vec![
                DocValueField {
                    field: "*".to_string(),
                    format: None
                },
                DocValueField {
                    field: "timestamp".to_string(),
                    format: Some(DateRenderFormat::StrictDateOptionalTimeNanos)
                },
            ]
        );
        assert_eq!(
            request.docvalue_fields,
            vec![DocValueField {
                field: "timestamp".to_string(),
                format: Some(DateRenderFormat::DateTime)
            }]
        );
        assert_eq!(request.terminate_after, Some(100_000));
        let single_stored_field_request =
            parse_request(&json!({"stored_fields": "_none_"}), "").expect("one stored field parses");
        assert_eq!(single_stored_field_request.fields, Vec::new());

        for (body, error_type) in [
            (
                json!({"script_fields": {"x": {"script": "1"}}}),
                "illegal_argument_exception",
            ),
            (json!({"script_fields": []}), "parsing_exception"),
            (
                json!({"fields": [{"field": "t", "format": "basic_date"}]}),
                "illegal_argument_exception",
            ),
            (json!({"fields": "timestamp"}), "parsing_exception"),
            (json!({"docvalue_fields": [1]}), "parsing_exception"),
            (json!({"fields": [{"format": "date_time"}]}), "parsing_exception"),
            (
                json!({"fields": [{"field": "t", "include_unmapped": true}]}),
                "parsing_exception",
            ),
            (json!({"fields": [{"field": 1}]}), "parsing_exception"),
            (
                json!({"docvalue_fields": [{"field": "t", "format": 1}]}),
                "parsing_exception",
            ),
            (json!({"stored_fields": 1}), "parsing_exception"),
            (json!({"stored_fields": ["*", 1]}), "parsing_exception"),
            (json!({"timeout": 30}), "parsing_exception"),
            (json!({"highlight": true}), "parsing_exception"),
            (json!({"terminate_after": -1}), "parsing_exception"),
            (json!({"terminate_after": "5"}), "parsing_exception"),
        ] {
            assert_refused(&body, "", error_type).await;
        }

        assert_eq!(resolve_track_total_hits(None, true), Ok(TrackTotalHits::Exact));
        assert_eq!(
            resolve_track_total_hits(None, false),
            Ok(TrackTotalHits::UpTo(DEFAULT_TOTAL_HITS_THRESHOLD))
        );
        assert_eq!(
            resolve_track_total_hits(Some(TrackTotalHits::Disabled), true),
            Ok(TrackTotalHits::Disabled)
        );
        assert_eq!(
            resolve_track_total_hits(Some(TrackTotalHits::UpTo(100)), false),
            Ok(TrackTotalHits::UpTo(100))
        );
        let error = resolve_track_total_hits(Some(TrackTotalHits::UpTo(100)), true)
            .expect_err("an integer threshold has no integer total");
        let (status, answer) = render_envelope(error).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(answer["error"]["type"], "illegal_argument_exception");
    }

    #[test]
    fn source_filters_parse_from_the_body_and_the_query_string() {
        let build_paths_filter = |includes: &[&str], excludes: &[&str]| SourceFilter::Paths {
            includes: includes.iter().map(|path| (*path).to_string()).collect(),
            excludes: excludes.iter().map(|path| (*path).to_string()).collect(),
        };
        let cases = [
            (json!({"_source": false}), "", SourceFilter::Nothing),
            (json!({"_source": true}), "", SourceFilter::All),
            (json!({"_source": "type"}), "", build_paths_filter(&["type"], &[])),
            (
                json!({"_source": ["type", "dashboard.*"]}),
                "",
                build_paths_filter(&["type", "dashboard.*"], &[]),
            ),
            (
                json!({"_source": {"includes": "dashboard.*", "excludes": ["dashboard.panelsJSON"]}}),
                "",
                build_paths_filter(&["dashboard.*"], &["dashboard.panelsJSON"]),
            ),
            (
                json!({"_source": {"excludes": "dashboard.panelsJSON"}}),
                "",
                build_paths_filter(&[], &["dashboard.panelsJSON"]),
            ),
            (
                json!({"_source": false}),
                "_source=references,type",
                build_paths_filter(&["references", "type"], &[]),
            ),
            (json!({}), "_source=false", SourceFilter::Nothing),
            (json!({"_source": false}), "_source=true", SourceFilter::All),
            (json!({"_source": false}), "_source=", SourceFilter::All),
        ];
        for (body, query_string, expected_filter) in cases {
            assert_eq!(
                parse_request(&body, query_string).expect("parses").source,
                expected_filter,
                "{body} ?{query_string}"
            );
        }
    }
}
