//! The stored-attribute-key to wire-name mapping.
//!
//! Attribute keys are stored in OTel-native dotted form (`k8s.pod.name`), but
//! Loki label names are restricted to `[a-zA-Z_][a-zA-Z0-9_]*` — a dotted or
//! colon-bearing name is rejected upstream. Queries therefore address a stored
//! key by its wire name, and the mapping is resolved at read time.
//!
//! Every path that resolves a stored key against a wire name MUST go through
//! this module. The matcher path, the merged-map path used for series identity
//! and grouping, the displayed-label path, and label enumeration all read the
//! same stored bytes; if any two of them disagreed about which raw key owns a
//! wire name, a row could match a query through one value while being displayed
//! under another. A single definition is what makes that class of divergence
//! impossible rather than merely tested for.
//!
//! `TraceQL` imposes no such restriction and addresses attributes by their
//! dotted name, so the Tempo paths compare keys as stored and never call this.

use std::borrow::Cow;

/// Render a stored attribute key as the wire (Loki label) name addressing it.
///
/// Replaces every `.` with `_`. Borrows when the key has no `.` to replace,
/// which is the whole cost of the transform for an already-underscored key.
///
/// The mapping is many-to-one: `k8s.pod.name` and `k8s_pod_name` share a wire
/// name, and ingest deduplicates by the raw dotted string only, so both can
/// legitimately coexist in one map. Callers resolving a collision MUST take the
/// first match in stored order — see [`matches_wire_name`].
#[must_use]
pub fn normalize_attribute_key(key: &str) -> Cow<'_, str> {
    if key.contains('.') {
        Cow::Owned(key.replace('.', "_"))
    } else {
        Cow::Borrowed(key)
    }
}

/// Whether `stored_key` is addressed by the wire name `wire_name`.
///
/// Equivalent to `normalize_attribute_key(stored_key) == wire_name` but never
/// allocates: the two are compared byte-wise, treating a stored `.` as matching
/// a `_` in the wire name. The length check rejects most candidates before any
/// byte comparison, which matters because callers scan every entry of a map per
/// row.
#[must_use]
pub fn matches_wire_name(stored_key: &str, wire_name: &str) -> bool {
    stored_key.len() == wire_name.len()
        && stored_key.bytes().zip(wire_name.bytes()).all(|(stored, wire)| {
            // A stored `.` is rendered as `_`, so it matches ONLY `_` — a wire
            // name that still carries the `.` does not address it, exactly as
            // normalize-then-compare would decide.
            if stored == b'.' { wire == b'_' } else { stored == wire }
        })
}

#[cfg(test)]
mod tests {
    use super::{matches_wire_name, normalize_attribute_key};

    #[test]
    fn dotted_key_renders_as_its_underscored_wire_name() {
        assert_eq!(normalize_attribute_key("k8s.pod.name"), "k8s_pod_name");
    }

    #[test]
    fn already_underscored_key_is_borrowed_unchanged() {
        let key = normalize_attribute_key("pod");
        assert_eq!(key, "pod");
        assert!(matches!(key, std::borrow::Cow::Borrowed(_)));
    }

    #[test]
    fn matches_wire_name_agrees_with_normalize_on_every_shape() {
        // The allocation-free comparison is only safe while it is
        // indistinguishable from normalize-then-compare; pin that here rather
        // than trusting the two implementations to stay in step.
        for (stored, wire) in [
            ("k8s.pod.name", "k8s_pod_name"),
            ("k8s.pod.name", "k8s.pod.name"),
            ("pod", "pod"),
            ("pod", "node"),
            ("user.id", "user_id"),
            ("user.id", "user_idx"),
            ("", ""),
            ("a.b", "a_b"),
            ("a_b", "a.b"),
        ] {
            assert_eq!(
                matches_wire_name(stored, wire),
                normalize_attribute_key(stored) == wire,
                "stored={stored:?} wire={wire:?}"
            );
        }
    }

    #[test]
    fn wire_underscore_does_not_match_back_onto_a_stored_dot() {
        // The mapping is one-way: a stored `a_b` is addressed as `a_b`, never
        // as `a.b`. Only the stored side may carry the dot.
        assert!(!matches_wire_name("a_b", "a.b"));
        assert!(matches_wire_name("a.b", "a_b"));
    }
}
