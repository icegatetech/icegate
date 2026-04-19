//! Construct the tenant+time iceberg `Predicate` that drives file and
//! row-group pruning for metadata scans.
//!
//! Higher-level selectors (`LogQL` label matchers, `TraceQL` filters, …) are
//! the concern of per-API callers. They build their own `Predicate` and
//! AND it with the base predicate via [`combine`].
//!
//! Semantics: over-approximation is permitted. Matchers that cannot be
//! translated are silently omitted by callers; the only effect is a wider
//! row-group set, never false negatives.

use chrono::{DateTime, Utc};
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::Datum;
use icegate_common::schema::{COL_TENANT_ID, COL_TIMESTAMP};

/// Always-AND'ed base predicate: `tenant_id` equality and timestamp range.
#[must_use]
pub fn base_predicate(tenant_id: &str, start: DateTime<Utc>, end: DateTime<Utc>) -> Predicate {
    let tenant = Reference::new(COL_TENANT_ID).equal_to(Datum::string(tenant_id.to_string()));
    let ts_lo =
        Reference::new(COL_TIMESTAMP).greater_than_or_equal_to(Datum::timestamp_micros(start.timestamp_micros()));
    let ts_hi = Reference::new(COL_TIMESTAMP).less_than_or_equal_to(Datum::timestamp_micros(end.timestamp_micros()));
    tenant.and(ts_lo).and(ts_hi)
}

/// Combine a base predicate with an optional extra predicate.
///
/// `Predicate::AlwaysTrue` is treated as "no extra predicate" so callers
/// can uniformly pass `Predicate::AlwaysTrue` to mean "just tenant and time".
#[must_use]
pub(crate) fn combine(base: Predicate, extra: Predicate) -> Predicate {
    match extra {
        Predicate::AlwaysTrue => base,
        other => base.and(other),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use iceberg::expr::Predicate;

    use super::{base_predicate, combine};

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
    fn combine_with_always_true_is_base() {
        let b = base_predicate("tid", ts(100), ts(200));
        let s_before = format!("{b:?}");
        let c = combine(b, Predicate::AlwaysTrue);
        assert_eq!(format!("{c:?}"), s_before);
    }
}
