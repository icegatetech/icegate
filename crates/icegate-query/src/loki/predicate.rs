//! Translate a `LogQL` [`Selector`] into an iceberg [`Predicate`] fragment
//! suitable for file and row-group pruning on the `logs` table.
//!
//! This module is intentionally Loki-specific. The generic tenant+time
//! predicate lives in [`crate::engine::metadata_scan::base_predicate`];
//! this module produces the extra predicate that `metadata_scan` then AND's
//! with the base.
//!
//! Semantics: over-approximation is permitted. Matchers that cannot be
//! translated (regex, MAP-only labels) are silently omitted. The only effect
//! of omission is a wider row-group set, never false negatives.

use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;

use crate::engine::metadata_scan::MetadataScanConfig;
use crate::logql::common::MatchOp;
use crate::logql::log::Selector;

/// Translate a `LogQL` selector into an iceberg predicate fragment.
///
/// Returns [`Predicate::AlwaysTrue`] if no matchers could be translated (all
/// were MAP-only, regex, or used unrecognized aliases). Callers can pass
/// the result to [`crate::engine::metadata_scan::scan_labels`] or
/// [`crate::engine::metadata_scan::scan_label_values`] as the
/// `extra_predicate`.
#[must_use]
pub fn selector_predicate(selector: &Selector, config: &MetadataScanConfig) -> Predicate {
    let mut out: Option<Predicate> = None;
    for m in &selector.matchers {
        if !config.is_indexed(&m.label) {
            continue; // MAP-only label: cannot prune, omit
        }
        let col = config.resolve_column(&m.label).to_string();
        let p = match m.op {
            MatchOp::Eq => Reference::new(col).equal_to(Datum::string(m.value.clone())),
            MatchOp::Neq => Reference::new(col).not_equal_to(Datum::string(m.value.clone())),
            MatchOp::Re | MatchOp::Nre => continue, // regex: cannot prune, omit
        };
        out = Some(match out {
            Some(acc) => acc.and(p),
            None => p,
        });
    }
    out.unwrap_or(Predicate::AlwaysTrue)
}

#[cfg(test)]
mod tests {
    use super::selector_predicate;
    use crate::engine::metadata_scan::MetadataScanConfig;
    use crate::logql::log::{LabelMatcher, Selector};

    /// Minimal logs-like config for test cases.
    const LOG_CFG: MetadataScanConfig = MetadataScanConfig {
        indexed_columns: &[
            "service_name",
            "severity_text",
            "trace_id",
            "span_id",
            "cloud_account_id",
        ],
        label_aliases: &[
            ("level", "severity_text"),
            ("detected_level", "severity_text"),
            ("service", "service_name"),
        ],
        excluded_map_keys: &[],
        map_column: "attributes",
    };

    #[test]
    fn selector_predicate_translates_eq_on_indexed_column() {
        let sel = Selector::new(vec![LabelMatcher::eq("service_name", "api")]);
        let p = selector_predicate(&sel, &LOG_CFG);
        let s = format!("{p:?}");
        assert!(s.contains("service_name"));
        assert!(s.contains("api"));
    }

    #[test]
    fn selector_predicate_translates_level_to_severity_text() {
        let sel = Selector::new(vec![LabelMatcher::eq("level", "error")]);
        let p = selector_predicate(&sel, &LOG_CFG);
        let s = format!("{p:?}");
        assert!(s.contains("severity_text"));
    }

    #[test]
    fn selector_predicate_omits_regex_matchers() {
        let sel = Selector::new(vec![LabelMatcher::re("service_name", "api-.*")]);
        let p = selector_predicate(&sel, &LOG_CFG);
        // No matchers → AlwaysTrue
        assert!(matches!(p, iceberg::expr::Predicate::AlwaysTrue));
    }

    #[test]
    fn selector_predicate_omits_map_only_labels() {
        let sel = Selector::new(vec![LabelMatcher::eq("pod", "web-1")]);
        let p = selector_predicate(&sel, &LOG_CFG);
        assert!(matches!(p, iceberg::expr::Predicate::AlwaysTrue));
    }

    #[test]
    fn selector_predicate_keeps_translatable_and_drops_untranslatable() {
        let sel = Selector::new(vec![
            LabelMatcher::eq("service_name", "api"),
            LabelMatcher::eq("pod", "web-1"),
            LabelMatcher::re("trace_id", "^abc"),
        ]);
        let p = selector_predicate(&sel, &LOG_CFG);
        let s = format!("{p:?}");
        assert!(s.contains("service_name"));
        assert!(!s.contains("pod"));
    }

    #[test]
    fn selector_predicate_translates_detected_level_to_severity_text() {
        let sel = Selector::new(vec![LabelMatcher::eq("detected_level", "warn")]);
        let p = selector_predicate(&sel, &LOG_CFG);
        let s = format!("{p:?}");
        assert!(s.contains("severity_text"));
    }

    #[test]
    fn selector_predicate_translates_service_to_service_name() {
        let sel = Selector::new(vec![LabelMatcher::eq("service", "api")]);
        let p = selector_predicate(&sel, &LOG_CFG);
        let s = format!("{p:?}");
        assert!(s.contains("service_name"));
    }
}
