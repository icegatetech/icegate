//! The data indices one tenant sees and how an index expression selects them, how it selects the
//! tenant's system indices, and how a request that reads one kind of index is told which.

use icegate_common::{EVENTS_TABLE, LOGS_TABLE, METRICS_TABLE, OPERATIONS_TABLE, SPANS_TABLE};

use super::expression::{ExpressionPart, IndexExpression, Selection, has_wildcard};
use crate::opensearch::error::OpenSearchError;
use crate::opensearch::extract::ExpandWildcards;
use crate::opensearch::glob::matches_glob;

/// The telemetry tables. Each is served under its own name and is wildcard-visible; any other
/// table the tenant may read resolves only by its exact name.
pub(super) const TELEMETRY_TABLES: [&str; 5] = [LOGS_TABLE, SPANS_TABLE, EVENTS_TABLE, METRICS_TABLE, OPERATIONS_TABLE];

/// The telemetry table named `table`, as its `'static` constant; `None` for any other table.
pub(super) fn find_telemetry_table(table: &str) -> Option<&'static str> {
    TELEMETRY_TABLES.into_iter().find(|telemetry_table| *telemetry_table == table)
}

/// How resolution treats missing names and empty results. Each endpoint passes its own options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexOptions {
    /// A missing concrete name is skipped instead of answered `404`.
    pub(crate) ignore_unavailable: bool,
    /// An expression that selects nothing yields an empty list instead of `404`.
    pub(crate) allow_no_indices: bool,
    /// What wildcards expand to.
    pub(crate) expand_wildcards: ExpandWildcards,
}

/// The index options of an endpoint that reads named indices, from its query flags: a missing
/// concrete name is an error and an empty match is not, and wildcards expand to open indices,
/// unless the request says otherwise.
///
/// # Errors
///
/// Returns `illegal_argument_exception` for an unknown `expand_wildcards` value.
pub(crate) fn read_index_options(
    ignore_unavailable: Option<bool>,
    allow_no_indices: Option<bool>,
    expand_wildcards: Option<&str>,
) -> Result<IndexOptions, OpenSearchError> {
    Ok(IndexOptions {
        ignore_unavailable: ignore_unavailable.unwrap_or(false),
        allow_no_indices: allow_no_indices.unwrap_or(true),
        expand_wildcards: ExpandWildcards::parse(expand_wildcards)?,
    })
}

/// The naming profile an index name selects: how its field names map onto its table's columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Profile {
    /// Columns under their stored names.
    Native,
}

/// Something an index expression selects by name: a data index, or a system index or alias name.
trait SelectableIndex {
    /// The name the expression's parts match.
    fn index_name(&self) -> &str;
    /// Whether a wildcard can select it, or only its exact name.
    fn is_wildcard_visible(&self) -> bool;
}

/// A system index or alias name. Every one is wildcard-visible here: whether a wildcard reaches
/// the system side at all, the hidden rule, is decided before, by
/// [`IndexExpression::partition_system_parts`].
impl SelectableIndex for &str {
    fn index_name(&self) -> &str {
        self
    }

    fn is_wildcard_visible(&self) -> bool {
        true
    }
}

/// One data index the tenant sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    /// The index name clients use.
    name: String,
    /// The table the index reads.
    table: String,
    /// How the index's field names map onto the table's columns.
    profile: Profile,
    /// Whether a wildcard can select the index, or only its exact name.
    is_wildcard_visible: bool,
}

impl IndexEntry {
    /// The index name clients use.
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// The table this index reads. Equal to the name under the native profile.
    pub(crate) fn table(&self) -> &str {
        &self.table
    }

    /// The naming profile that maps this index's field names onto its table.
    pub(crate) const fn profile(&self) -> Profile {
        self.profile
    }
}

impl SelectableIndex for IndexEntry {
    fn index_name(&self) -> &str {
        &self.name
    }

    fn is_wildcard_visible(&self) -> bool {
        self.is_wildcard_visible
    }
}

/// Every data index one request may name, in name order, each name once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexSet {
    /// Sorted by name, without duplicates.
    entries: Vec<IndexEntry>,
}

impl IndexSet {
    /// Build the set from the table names a tenant-scoped catalog lists. Telemetry tables are
    /// wildcard-visible; every other table resolves only by its exact name. A dot-prefixed table
    /// name is dropped: dot-prefixed names are system indices, never data.
    pub(crate) fn from_table_names(names: impl IntoIterator<Item = String>) -> Self {
        let mut entries: Vec<IndexEntry> = names
            .into_iter()
            .filter(|name| !name.starts_with('.'))
            .map(|name| IndexEntry {
                is_wildcard_visible: find_telemetry_table(&name).is_some(),
                table: name.clone(),
                profile: Profile::Native,
                name,
            })
            .collect();
        entries.sort_by(|left_entry, right_entry| left_entry.name.cmp(&right_entry.name));
        entries.dedup_by(|left_entry, right_entry| left_entry.name == right_entry.name);
        Self { entries }
    }

    /// Every entry, in name order.
    pub(crate) fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// The entries `expression` selects under `options`, in name order, each at most once.
    ///
    /// # Errors
    ///
    /// Returns `index_not_found_exception` for a missing concrete name unless
    /// `ignore_unavailable`, and for an empty result when `allow_no_indices` is false (naming the
    /// whole expression).
    pub(crate) fn resolve(
        &self,
        expression: &IndexExpression,
        options: IndexOptions,
    ) -> Result<Vec<&IndexEntry>, OpenSearchError> {
        let resolved_entries: Vec<&IndexEntry> = match &expression.selection {
            Selection::AllVisible => self
                .entries
                .iter()
                .filter(|entry| is_expanded_by_wildcard(*entry, "*", options.expand_wildcards))
                .collect(),
            Selection::Parts(parts) => select_items_by_parts(&self.entries, parts, options)?.collect(),
        };
        if resolved_entries.is_empty() && !options.allow_no_indices {
            return Err(OpenSearchError::index_not_found(expression.text()));
        }
        Ok(resolved_entries)
    }
}

/// The items of `items` (sorted by name, each name once) that `parts` select under `options`, in
/// name order. The parts apply left to right: a wildcard inclusion adds what it expands to (see
/// [`is_expanded_by_wildcard`]), a concrete inclusion adds the item of that exact name, and an
/// exclusion removes every item it matches.
///
/// # Errors
///
/// Returns `index_not_found_exception` for a missing concrete name unless `ignore_unavailable`.
fn select_items_by_parts<'a, T: SelectableIndex>(
    items: &'a [T],
    parts: &[ExpressionPart],
    options: IndexOptions,
) -> Result<impl Iterator<Item = &'a T>, OpenSearchError> {
    let mut selection_flags = vec![false; items.len()];
    for part in parts {
        match part {
            ExpressionPart::Include(pattern) if has_wildcard(pattern) => {
                for (is_item_selected, item) in selection_flags.iter_mut().zip(items) {
                    *is_item_selected |= is_expanded_by_wildcard(item, pattern, options.expand_wildcards);
                }
            }
            // `items` is sorted by name, so a concrete name is one binary search.
            ExpressionPart::Include(concrete_name) => {
                match items.binary_search_by(|item| item.index_name().cmp(concrete_name.as_str())) {
                    Ok(position) => selection_flags[position] = true,
                    Err(_) if options.ignore_unavailable => {}
                    Err(_) => return Err(OpenSearchError::index_not_found(concrete_name)),
                }
            }
            ExpressionPart::Exclude(pattern) => {
                for (is_item_selected, item) in selection_flags.iter_mut().zip(items) {
                    *is_item_selected &= !matches_glob(pattern, item.index_name());
                }
            }
        }
    }
    Ok(items
        .iter()
        .zip(selection_flags)
        .filter_map(|(item, is_item_selected)| is_item_selected.then_some(item)))
}

/// What an index expression resolves to on a request that reads one kind of index.
#[derive(Debug)]
pub(crate) enum IndexTarget<'a> {
    /// Data indices, in name order.
    Data(Vec<&'a IndexEntry>),
    /// Concrete system indices, aliases resolved, in name order, each once.
    System(Vec<String>),
}

/// The names among `names` (a tenant's system index and alias names, sorted) that `parts` select
/// under `options`, in name order. Wildcards match only while open indices are expanded.
///
/// # Errors
///
/// Returns `index_not_found_exception` for a missing concrete name unless `ignore_unavailable`.
pub(super) fn select_system_names<'a>(
    names: &[&'a str],
    parts: &[ExpressionPart],
    options: IndexOptions,
) -> Result<Vec<&'a str>, OpenSearchError> {
    Ok(select_items_by_parts(names, parts, options)?.copied().collect())
}

/// Decide what `expression` targets from its resolved data and system sides: a request reads
/// data indices or system indices, never both.
///
/// # Errors
///
/// Returns `illegal_argument_exception` naming the system indices when both sides are non-empty,
/// and `index_not_found_exception` naming the expression when both are empty and
/// `allow_no_indices` is false.
pub(super) fn choose_index_target<'a>(
    expression: &IndexExpression,
    data_entries: Vec<&'a IndexEntry>,
    system_indices: Vec<String>,
    allow_no_indices: bool,
) -> Result<IndexTarget<'a>, OpenSearchError> {
    match (data_entries.is_empty(), system_indices.is_empty()) {
        (false, false) => Err(OpenSearchError::illegal_argument(format!(
            "[{}] mixes the system indices [{}] with data indices; one request reads one or the other",
            expression.text(),
            system_indices.join(",")
        ))),
        (true, false) => Ok(IndexTarget::System(system_indices)),
        (true, true) if !allow_no_indices => Err(OpenSearchError::index_not_found(expression.text())),
        (false | true, true) => Ok(IndexTarget::Data(data_entries)),
    }
}

/// Whether wildcard `pattern` expands to `item`: only while open indices are expanded, and only
/// to a wildcard-visible item. Without open indices no name is matched against the pattern.
fn is_expanded_by_wildcard<T: SelectableIndex>(item: &T, pattern: &str, expand_wildcards: ExpandWildcards) -> bool {
    expand_wildcards.is_open && item.is_wildcard_visible() && matches_glob(pattern, item.index_name())
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;
    use crate::opensearch::error::render_envelope;

    /// The five telemetry tables, two exact-name tables a tenant may also read, and a view.
    fn build_index_set() -> IndexSet {
        IndexSet::from_table_names(
            [
                "logs",
                "spans",
                "events",
                "metrics",
                "operations",
                "prices",
                "prices_effective",
                "marks",
            ]
            .map(str::to_string),
        )
    }

    /// The options a search applies by default: a missing name is an error, an empty match is not.
    fn build_search_options() -> IndexOptions {
        IndexOptions {
            ignore_unavailable: false,
            allow_no_indices: true,
            expand_wildcards: ExpandWildcards::parse(None).expect("the default expansion"),
        }
    }

    fn resolve_names(text: &str, options: IndexOptions) -> Vec<String> {
        build_index_set()
            .resolve(&IndexExpression::parse(text).expect("a valid expression"), options)
            .expect("the expression resolves")
            .into_iter()
            .map(|entry| entry.name().to_string())
            .collect()
    }

    #[test]
    fn star_selects_registry_indices_in_name_order_but_no_exact_name_table() {
        let telemetry_names = ["events", "logs", "metrics", "operations", "spans"];
        assert_eq!(resolve_names("*", build_search_options()), telemetry_names);
        assert_eq!(resolve_names("_all", build_search_options()), telemetry_names);
        assert_eq!(resolve_names("", build_search_options()), telemetry_names);
    }

    #[test]
    fn exclusions_and_single_character_wildcards_apply_in_order() {
        assert_eq!(
            resolve_names("*,-s*", build_search_options()),
            ["events", "logs", "metrics", "operations"]
        );
        assert_eq!(resolve_names("l?gs", build_search_options()), ["logs"]);
        assert_eq!(resolve_names("*,-*,logs", build_search_options()), ["logs"]);
    }

    #[test]
    fn an_exact_name_table_resolves_only_by_its_exact_name() {
        assert_eq!(resolve_names("marks", build_search_options()), ["marks"]);
        assert_eq!(resolve_names("m*", build_search_options()), ["metrics"]);
        assert_eq!(
            resolve_names("prices_effective", build_search_options()),
            ["prices_effective"]
        );
    }

    #[test]
    fn a_name_listed_twice_resolves_once() {
        assert_eq!(resolve_names("logs,logs,l*", build_search_options()), ["logs"]);
    }

    #[tokio::test]
    async fn a_missing_concrete_name_is_404_unless_ignore_unavailable() {
        let expression = IndexExpression::parse("logs,nope").expect("a valid expression");

        let error = build_index_set()
            .resolve(&expression, build_search_options())
            .expect_err("nope is missing");
        let lenient_options = IndexOptions {
            ignore_unavailable: true,
            ..build_search_options()
        };

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], "index_not_found_exception");
        assert_eq!(body["error"]["index"], "nope");
        assert_eq!(resolve_names("logs,nope", lenient_options), ["logs"]);
    }

    #[tokio::test]
    async fn an_empty_match_is_404_only_when_no_indices_are_disallowed() {
        let strict_options = IndexOptions {
            allow_no_indices: false,
            ..build_search_options()
        };

        let error = build_index_set()
            .resolve(
                &IndexExpression::parse("nope*").expect("a valid expression"),
                strict_options,
            )
            .expect_err("nothing matches nope*");

        assert!(resolve_names("nope*", build_search_options()).is_empty());
        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["index"], "nope*");
    }

    #[test]
    fn expand_wildcards_none_matches_nothing() {
        let no_expansion_options = IndexOptions {
            expand_wildcards: ExpandWildcards::parse(Some("none")).expect("a served value"),
            ..build_search_options()
        };

        assert!(resolve_names("*", no_expansion_options).is_empty());
        assert!(resolve_names("", no_expansion_options).is_empty());
        assert_eq!(resolve_names("logs", no_expansion_options), ["logs"]);
    }

    /// Index names match case-sensitively, so two names that differ only in case never reach one
    /// table: `Logs` names nothing, as a concrete name or through a wildcard.
    #[tokio::test]
    async fn index_names_match_case_sensitively() {
        let error = build_index_set()
            .resolve(
                &IndexExpression::parse("Logs").expect("a valid expression"),
                build_search_options(),
            )
            .expect_err("no index is named Logs");

        let (status, body) = render_envelope(error).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["index"], "Logs");
        assert!(resolve_names("L*", build_search_options()).is_empty());
    }

    /// Dot-prefixed names belong to system indices. A table that happens to carry one must not
    /// surface as a data index, by name or by wildcard: by name it is the missing-index `404`.
    #[tokio::test]
    async fn dot_prefixed_table_names_are_not_data_indices() {
        let index_set = IndexSet::from_table_names([".kibana".to_string(), "logs".to_string()]);

        let wildcard_entries = index_set
            .resolve(
                &IndexExpression::parse("*").expect("a valid expression"),
                build_search_options(),
            )
            .expect("resolves");
        let exact_name_error = index_set
            .resolve(
                &IndexExpression::parse(".kibana").expect("a valid expression"),
                build_search_options(),
            )
            .expect_err("no data index is named .kibana");

        assert_eq!(
            wildcard_entries.iter().map(|entry| entry.name()).collect::<Vec<_>>(),
            ["logs"]
        );
        let (status, body) = render_envelope(exact_name_error).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["type"], "index_not_found_exception");
        assert_eq!(body["error"]["index"], ".kibana");
    }

    /// A tenant's system index and alias names, sorted as the snapshot lists them.
    const SYSTEM_NAMES: [&str; 3] = [".kibana", ".kibana_1", ".opensearch_dashboards"];

    fn build_options(expand_wildcards: Option<&str>, ignore_unavailable: bool) -> IndexOptions {
        IndexOptions {
            ignore_unavailable,
            allow_no_indices: true,
            expand_wildcards: ExpandWildcards::parse(expand_wildcards).expect("a served value"),
        }
    }

    fn select_names(
        expression_text: &str,
        expand_wildcards: Option<&str>,
    ) -> Result<Vec<&'static str>, OpenSearchError> {
        let options = build_options(expand_wildcards, false);
        let partition = IndexExpression::parse(expression_text)
            .expect("a valid expression")
            .partition_system_parts(options.expand_wildcards.is_hidden);
        select_system_names(&SYSTEM_NAMES, &partition.system_parts, options)
    }

    /// IceGate's rule: dot-prefixed indices are hidden, so a wildcard reaches them only with
    /// `expand_wildcards` including hidden, while a pattern starting with `.` reaches them always.
    #[tokio::test]
    async fn dot_prefixed_names_resolve_to_system_indices_only_with_hidden_or_a_dot_pattern() {
        assert_eq!(select_names("*", None).expect("open"), Vec::<&str>::new());
        assert_eq!(select_names("", None).expect("open"), Vec::<&str>::new());
        assert_eq!(select_names("*", Some("all")).expect("all"), SYSTEM_NAMES);
        assert_eq!(select_names("_all", Some("open,hidden")).expect("hidden"), SYSTEM_NAMES);
        assert_eq!(
            select_names(".kib*", None).expect("a dot pattern"),
            [".kibana", ".kibana_1"]
        );
        assert_eq!(select_names(".kibana", None).expect("a dot name"), [".kibana"]);
        assert_eq!(
            select_names("*,-.kibana_1", Some("all")).expect("an exclusion"),
            [".kibana", ".opensearch_dashboards"]
        );
        assert_eq!(select_names(".k*", Some("none")).expect("none"), Vec::<&str>::new());
        assert_eq!(
            select_names("logs", Some("all")).expect("a data name"),
            Vec::<&str>::new()
        );
        let partition = IndexExpression::parse(".kib*").expect("valid").partition_system_parts(false);
        assert!(
            partition.data_expression.is_none(),
            "a dot-only expression has no data part"
        );

        let (status, body) = render_envelope(select_names(".missing", None).expect_err("a missing dot name")).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["index"], ".missing");
        let ignored_selection = select_system_names(
            &SYSTEM_NAMES,
            &IndexExpression::parse(".missing")
                .expect("valid")
                .partition_system_parts(false)
                .system_parts,
            build_options(None, true),
        );
        assert_eq!(ignored_selection.expect("ignore_unavailable"), Vec::<&str>::new());
    }

    #[tokio::test]
    async fn an_expression_mixing_system_and_data_indices_is_refused_naming_the_system_indices() {
        let index_set = IndexSet::from_table_names(["logs".to_string(), "spans".to_string()]);
        let options = build_options(None, false);
        let resolve_sides = |expression_text: &str| {
            let expression = IndexExpression::parse(expression_text).expect("a valid expression");
            let partition = expression.partition_system_parts(false);
            let data_entries = partition.data_expression.as_ref().map_or_else(Vec::new, |data_expression| {
                index_set.resolve(data_expression, options).expect("data resolves")
            });
            let system_indices: Vec<String> = select_system_names(&[".kibana_1"], &partition.system_parts, options)
                .expect("system resolves")
                .into_iter()
                .map(str::to_string)
                .collect();
            (expression, data_entries, system_indices)
        };

        let (expression, data_entries, system_indices) = resolve_sides("logs,.kibana_1");
        let refusal = choose_index_target(&expression, data_entries, system_indices, true).expect_err("mixed");
        let (status, body) = render_envelope(refusal).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["type"], "illegal_argument_exception");
        assert!(
            body["error"]["reason"]
                .as_str()
                .is_some_and(|reason| reason.contains(".kibana_1")),
            "{body}"
        );

        let (expression, data_entries, system_indices) = resolve_sides(".kibana_1");
        assert!(matches!(
            choose_index_target(&expression, data_entries, system_indices, true),
            Ok(IndexTarget::System(names)) if names == [".kibana_1"]
        ));
        let (expression, data_entries, system_indices) = resolve_sides("logs");
        assert!(matches!(
            choose_index_target(&expression, data_entries, system_indices, true),
            Ok(IndexTarget::Data(entries)) if entries.iter().map(|entry| entry.name()).eq(["logs"])
        ));
        let (expression, data_entries, system_indices) = resolve_sides("nothing*");
        assert!(matches!(
            choose_index_target(&expression, data_entries, system_indices, true),
            Ok(IndexTarget::Data(entries)) if entries.is_empty()
        ));
        let (expression, data_entries, system_indices) = resolve_sides("nothing*");
        let empty_refusal =
            choose_index_target(&expression, data_entries, system_indices, false).expect_err("no indices disallowed");
        assert_eq!(render_envelope(empty_refusal).await.0, StatusCode::NOT_FOUND);
    }
}
