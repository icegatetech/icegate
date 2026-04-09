//! Translate a `LogQL` `Selector` plus tenant/time bounds into an iceberg
//! `Predicate` for file and row-group pruning.
//!
//! Semantics: over-approximation is permitted (see spec section "Semantics").
//! Matchers that cannot be translated are silently omitted; the only effect
//! of omission is a wider row-group set, never false negatives.

use chrono::{DateTime, Utc};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use icegate_common::schema::{
    COL_SERVICE_NAME, COL_SEVERITY_TEXT, COL_TENANT_ID, COL_TIMESTAMP, LEVEL_ALIAS, LOG_INDEXED_ATTRIBUTE_COLUMNS,
};

use crate::logql::common::MatchOp;
use crate::logql::log::Selector;

/// Always-AND'ed base predicate: `tenant_id` equality and timestamp range.
#[must_use]
pub fn base_predicate(tenant_id: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Predicate {
    let tenant = Reference::new(COL_TENANT_ID).equal_to(Datum::string(tenant_id.to_string()));
    let ts_lo =
        Reference::new(COL_TIMESTAMP).greater_than_or_equal_to(Datum::timestamp_micros(start.timestamp_micros()));
    let ts_hi = Reference::new(COL_TIMESTAMP).less_than_or_equal_to(Datum::timestamp_micros(end.timestamp_micros()));
    tenant.and(ts_lo).and(ts_hi)
}

/// Translate a `LogQL` selector into an iceberg predicate fragment.
///
/// Returns `None` if no matchers could be translated (all were MAP-only or
/// regex). Callers should AND the result with [`base_predicate`].
#[must_use]
pub fn selector_predicate(selector: &Selector) -> Option<Predicate> {
    let mut out: Option<Predicate> = None;
    for m in &selector.matchers {
        if !is_indexed_column(&m.label) {
            continue; // MAP-only label: cannot prune, omit
        }
        let col = indexed_column_name(&m.label);
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
    out
}

/// Build the full predicate: base AND (selector predicate, if any).
#[must_use]
pub fn full_predicate(tenant_id: &str, start: DateTime<Utc>, end: DateTime<Utc>, selector: &Selector) -> Predicate {
    let base = base_predicate(tenant_id, start, end);
    match selector_predicate(selector) {
        Some(sel) => base.and(sel),
        None => base,
    }
}

/// Map a label name to its underlying indexed column name.
///
/// Handles Loki/Grafana aliases consistently with
/// `LogQLPlanner::map_label_to_internal_name`:
/// - `level`, `detected_level` → `severity_text`
/// - `service` → `service_name`
pub(super) fn indexed_column_name(label: &str) -> String {
    match label {
        LEVEL_ALIAS | "detected_level" => COL_SEVERITY_TEXT.to_string(),
        "service" => COL_SERVICE_NAME.to_string(),
        _ => label.to_string(),
    }
}

/// Whether the given label name maps to an indexed top-level column.
#[must_use]
pub fn is_indexed_column(label: &str) -> bool {
    let name = indexed_column_name(label);
    LOG_INDEXED_ATTRIBUTE_COLUMNS.contains(&name.as_str())
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};

    use super::{base_predicate, full_predicate, is_indexed_column, selector_predicate};
    use crate::logql::log::{LabelMatcher, Selector};

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    #[test]
    fn base_predicate_has_tenant_and_time() {
        let p = base_predicate("tid", ts(100), ts(200));
        let s = format!("{p:?}");
        assert!(s.contains("tenant_id"));
        assert!(s.contains("timestamp"));
    }

    #[test]
    fn selector_predicate_translates_eq_on_indexed_column() {
        let sel = Selector::new(vec![LabelMatcher::eq("service_name", "api")]);
        let p = selector_predicate(&sel).expect("should translate");
        let s = format!("{p:?}");
        assert!(s.contains("service_name"));
        assert!(s.contains("api"));
    }

    #[test]
    fn selector_predicate_translates_level_to_severity_text() {
        let sel = Selector::new(vec![LabelMatcher::eq("level", "error")]);
        let p = selector_predicate(&sel).expect("should translate");
        let s = format!("{p:?}");
        assert!(s.contains("severity_text"));
    }

    #[test]
    fn selector_predicate_omits_regex_matchers() {
        let sel = Selector::new(vec![LabelMatcher::re("service_name", "api-.*")]);
        assert!(selector_predicate(&sel).is_none());
    }

    #[test]
    fn selector_predicate_omits_map_only_labels() {
        let sel = Selector::new(vec![LabelMatcher::eq("pod", "web-1")]);
        assert!(selector_predicate(&sel).is_none());
    }

    #[test]
    fn selector_predicate_keeps_translatable_and_drops_untranslatable() {
        let sel = Selector::new(vec![
            LabelMatcher::eq("service_name", "api"),
            LabelMatcher::eq("pod", "web-1"),
            LabelMatcher::re("trace_id", "^abc"),
        ]);
        let p = selector_predicate(&sel).expect("should translate");
        let s = format!("{p:?}");
        assert!(s.contains("service_name"));
        assert!(!s.contains("pod"));
    }

    #[test]
    fn full_predicate_ands_base_and_selector() {
        let sel = Selector::new(vec![LabelMatcher::eq("service_name", "api")]);
        let p = full_predicate("tid", ts(100), ts(200), &sel);
        let s = format!("{p:?}");
        assert!(s.contains("tenant_id"));
        assert!(s.contains("timestamp"));
        assert!(s.contains("service_name"));
    }

    #[test]
    fn full_predicate_without_selector_is_base_only() {
        let p = full_predicate("tid", ts(100), ts(200), &Selector::empty());
        let s = format!("{p:?}");
        assert!(s.contains("tenant_id"));
        assert!(!s.contains("service_name"));
    }

    #[test]
    fn is_indexed_column_knows_level_alias() {
        assert!(is_indexed_column("service_name"));
        assert!(is_indexed_column("level"));
        assert!(!is_indexed_column("pod"));
    }

    #[test]
    fn is_indexed_column_knows_detected_level_alias() {
        assert!(is_indexed_column("detected_level"));
    }

    #[test]
    fn is_indexed_column_knows_service_alias() {
        assert!(is_indexed_column("service"));
    }

    #[test]
    fn selector_predicate_translates_detected_level_to_severity_text() {
        let sel = Selector::new(vec![LabelMatcher::eq("detected_level", "warn")]);
        let p = selector_predicate(&sel).expect("should translate");
        let s = format!("{p:?}");
        assert!(s.contains("severity_text"));
    }

    #[test]
    fn selector_predicate_translates_service_to_service_name() {
        let sel = Selector::new(vec![LabelMatcher::eq("service", "api")]);
        let p = selector_predicate(&sel).expect("should translate");
        let s = format!("{p:?}");
        assert!(s.contains("service_name"));
    }
}
