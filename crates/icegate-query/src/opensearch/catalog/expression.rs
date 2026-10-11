//! Index expressions: the comma list of names and patterns one request targets.

use std::borrow::Cow;

use crate::opensearch::error::OpenSearchError;

/// One element of an index expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ExpressionPart {
    /// A concrete name or a wildcard pattern to add.
    Include(String),
    /// A pattern to remove from what the earlier parts selected.
    Exclude(String),
}

/// What an expression selects before it meets an index set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Selection {
    /// `""` or a lone `_all`: every wildcard-visible data index.
    AllVisible,
    /// The parts, applied left to right.
    Parts(Vec<ExpressionPart>),
}

/// A parsed index expression, as it arrives decoded from a URL path segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexExpression {
    /// The expression as the client sent it, decoded.
    text: String,
    /// What the expression selects.
    pub(super) selection: Selection,
}

impl IndexExpression {
    /// Parse an index expression the router has already percent-decoded.
    ///
    /// A `-pattern` part is an exclusion only after a wildcard part. Before one it is an
    /// inclusion: a concrete name, or a pattern when it holds `*` or `?` (which then arms
    /// exclusion for the parts after it); no data index starts with `-`, so it matches nothing.
    /// Names are kept as written: matching is case-sensitive.
    ///
    /// # Errors
    ///
    /// Returns `illegal_argument_exception` for an empty name, an empty exclusion, or a
    /// `_`-prefixed name other than a lone `_all`.
    pub(crate) fn parse(text: &str) -> Result<Self, OpenSearchError> {
        if text.is_empty() || text == "_all" {
            return Ok(Self {
                text: text.to_string(),
                selection: Selection::AllVisible,
            });
        }
        let mut parts = Vec::new();
        let mut has_seen_wildcard = false;
        for part in text.split(',') {
            if part.is_empty() {
                return Err(OpenSearchError::illegal_argument(format!(
                    "index expression [{text}] contains an empty name"
                )));
            }
            if has_seen_wildcard && let Some(excluded_pattern) = part.strip_prefix('-') {
                if excluded_pattern.is_empty() {
                    return Err(OpenSearchError::illegal_argument(format!(
                        "index expression [{text}] contains an empty exclusion"
                    )));
                }
                parts.push(ExpressionPart::Exclude(excluded_pattern.to_string()));
                continue;
            }
            if part.starts_with('_') {
                return Err(OpenSearchError::illegal_argument(format!(
                    "index name [{part}] must not start with '_'"
                )));
            }
            has_seen_wildcard |= has_wildcard(part);
            parts.push(ExpressionPart::Include(part.to_string()));
        }
        Ok(Self {
            text: text.to_string(),
            selection: Selection::Parts(parts),
        })
    }

    /// The expression as the client sent it, decoded.
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    /// Split the expression into its data part and its system parts.
    ///
    /// A part starting with `.` names system indices only. With `is_hidden`, a wildcard part and
    /// an expression that selects everything (`""`, `_all`) also select system indices, and every
    /// exclusion applies to them too. The data part keeps its parts' relative order; an exclusion
    /// that followed a system part may then lead it, and applied to the empty selection it removes
    /// nothing, as it removed no data index before.
    pub(crate) fn partition_system_parts(&self, is_hidden: bool) -> ExpressionPartition {
        let parts = match &self.selection {
            Selection::AllVisible => {
                let system_parts = if is_hidden {
                    vec![ExpressionPart::Include("*".to_string())]
                } else {
                    Vec::new()
                };
                return ExpressionPartition {
                    data_expression: Some(self.clone()),
                    system_parts,
                };
            }
            Selection::Parts(parts) => parts,
        };
        let has_system_part = parts
            .iter()
            .any(|part| matches!(part, ExpressionPart::Include(pattern) if pattern.starts_with('.')));
        if !has_system_part && !is_hidden {
            return ExpressionPartition {
                data_expression: Some(self.clone()),
                system_parts: Vec::new(),
            };
        }
        let mut data_parts = Vec::new();
        let mut system_parts = Vec::new();
        for part in parts {
            match part {
                ExpressionPart::Include(pattern) if pattern.starts_with('.') => system_parts.push(part.clone()),
                ExpressionPart::Include(pattern) => {
                    if is_hidden && has_wildcard(pattern) {
                        system_parts.push(part.clone());
                    }
                    data_parts.push(part.clone());
                }
                ExpressionPart::Exclude(pattern) => {
                    system_parts.push(part.clone());
                    if !pattern.starts_with('.') {
                        data_parts.push(part.clone());
                    }
                }
            }
        }
        let has_inclusion =
            |parts: &[ExpressionPart]| parts.iter().any(|part| matches!(part, ExpressionPart::Include(_)));
        let data_expression = has_inclusion(&data_parts).then(|| Self::from_parts(data_parts));
        if !has_inclusion(&system_parts) {
            system_parts.clear();
        }
        ExpressionPartition {
            data_expression,
            system_parts,
        }
    }

    /// An expression of already validated `parts`, its text rebuilt from them.
    fn from_parts(parts: Vec<ExpressionPart>) -> Self {
        let text = parts
            .iter()
            .map(|part| match part {
                ExpressionPart::Include(pattern) => Cow::Borrowed(pattern.as_str()),
                ExpressionPart::Exclude(pattern) => Cow::Owned(format!("-{pattern}")),
            })
            .collect::<Vec<_>>()
            .join(",");
        Self {
            text,
            selection: Selection::Parts(parts),
        }
    }
}

/// An index expression split by what each part can select.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExpressionPartition {
    /// The expression the data path resolves: the parts that can select a data index, which are
    /// the whole expression when no part starts with `.` and hidden indices are not expanded, or
    /// when the expression selects everything; `None` when no inclusion is left for the data path.
    pub(crate) data_expression: Option<IndexExpression>,
    /// The parts system indices are resolved with, in request order; empty when the expression
    /// cannot select a system index.
    pub(super) system_parts: Vec<ExpressionPart>,
}

/// Whether `pattern` holds a wildcard character.
pub(super) fn has_wildcard(pattern: &str) -> bool {
    pattern.contains(['*', '?'])
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;
    use crate::opensearch::error::render_envelope;

    /// The parts of `text`. An expression selecting every visible index has none, which fails
    /// every assertion below that expects parts.
    fn parse_parts(text: &str) -> Vec<ExpressionPart> {
        match IndexExpression::parse(text).expect("a valid expression").selection {
            Selection::Parts(parts) => parts,
            Selection::AllVisible => Vec::new(),
        }
    }

    #[test]
    fn empty_and_a_lone_all_select_every_visible_index() {
        for text in ["", "_all"] {
            assert_eq!(
                IndexExpression::parse(text).expect("a valid expression").selection,
                Selection::AllVisible,
                "{text:?}"
            );
        }
    }

    #[tokio::test]
    async fn underscore_prefixed_names_other_than_a_lone_all_are_refused() {
        for text in ["_cat", "logs,_internal", "_all,logs", "*,_internal"] {
            let error = IndexExpression::parse(text).expect_err("a `_`-prefixed name");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{text}");
        }
    }

    #[test]
    fn a_minus_part_excludes_only_after_a_wildcard() {
        assert_eq!(
            parse_parts("*,-spans"),
            vec![
                ExpressionPart::Include("*".to_string()),
                ExpressionPart::Exclude("spans".to_string())
            ]
        );
        assert_eq!(
            parse_parts("l?gs,-spans"),
            vec![
                ExpressionPart::Include("l?gs".to_string()),
                ExpressionPart::Exclude("spans".to_string())
            ]
        );
        assert_eq!(
            parse_parts("-spans"),
            vec![ExpressionPart::Include("-spans".to_string())]
        );
        // A leading `-` part that holds a wildcard is an inclusion pattern, and it arms exclusion.
        assert_eq!(
            parse_parts("-s*,-x"),
            vec![
                ExpressionPart::Include("-s*".to_string()),
                ExpressionPart::Exclude("x".to_string())
            ]
        );
        assert_eq!(
            parse_parts("logs,-spans"),
            vec![
                ExpressionPart::Include("logs".to_string()),
                ExpressionPart::Include("-spans".to_string())
            ]
        );
    }

    #[tokio::test]
    async fn an_empty_name_or_exclusion_is_refused() {
        for text in ["logs,,spans", "*,-", ","] {
            let error = IndexExpression::parse(text).expect_err("an empty part");

            let (status, body) = render_envelope(error).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{text:?}");
            assert_eq!(body["error"]["type"], "illegal_argument_exception", "{text:?}");
        }
    }
}
