//! The data indices one tenant sees, and how an index expression selects them.

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
        let mut selection_flags = vec![false; self.entries.len()];
        match &expression.selection {
            Selection::AllVisible => {
                for (is_entry_selected, entry) in selection_flags.iter_mut().zip(&self.entries) {
                    *is_entry_selected = is_expanded_by_wildcard(entry, "*", options.expand_wildcards);
                }
            }
            Selection::Parts(parts) => {
                for part in parts {
                    self.apply_part(part, options, &mut selection_flags)?;
                }
            }
        }
        let resolved_entries: Vec<&IndexEntry> = self
            .entries
            .iter()
            .zip(selection_flags)
            .filter_map(|(entry, is_entry_selected)| is_entry_selected.then_some(entry))
            .collect();
        if resolved_entries.is_empty() && !options.allow_no_indices {
            return Err(OpenSearchError::index_not_found(expression.text()));
        }
        Ok(resolved_entries)
    }

    /// Apply one part to the selection built by the parts before it.
    fn apply_part(
        &self,
        part: &ExpressionPart,
        options: IndexOptions,
        selection_flags: &mut [bool],
    ) -> Result<(), OpenSearchError> {
        match part {
            ExpressionPart::Include(pattern) if has_wildcard(pattern) => {
                for (is_entry_selected, entry) in selection_flags.iter_mut().zip(&self.entries) {
                    *is_entry_selected |= is_expanded_by_wildcard(entry, pattern, options.expand_wildcards);
                }
            }
            ExpressionPart::Include(name) => {
                // `entries` is sorted by name, so a concrete name is one binary search.
                match self.entries.binary_search_by(|entry| entry.name.as_str().cmp(name.as_str())) {
                    Ok(position) => selection_flags[position] = true,
                    Err(_) if options.ignore_unavailable => {}
                    Err(_) => return Err(OpenSearchError::index_not_found(name)),
                }
            }
            ExpressionPart::Exclude(pattern) => {
                for (is_entry_selected, entry) in selection_flags.iter_mut().zip(&self.entries) {
                    *is_entry_selected &= !matches_glob(pattern, &entry.name);
                }
            }
        }
        Ok(())
    }
}

/// Whether wildcard `pattern` expands to `entry`: only wildcard-visible entries take part, and
/// only while open indices are expanded.
fn is_expanded_by_wildcard(entry: &IndexEntry, pattern: &str, expand_wildcards: ExpandWildcards) -> bool {
    entry.is_wildcard_visible && expand_wildcards.is_open && matches_glob(pattern, &entry.name)
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
}
