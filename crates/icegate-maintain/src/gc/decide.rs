//! Pure orphan-classification logic. No I/O, so it is exhaustively unit-tested.

use std::collections::HashSet;

use chrono::{DateTime, Utc};

/// Reduce a storage path to a canonical bucket-relative object key.
///
/// Iceberg metadata stores full URIs (`s3://bucket/icegate/logs/data/...`)
/// while an object-store `list` yields bucket-relative keys
/// (`icegate/logs/data/...`). Both reduce to the same canonical key here: the
/// scheme + authority (`s3://bucket`) is stripped, as is any leading slash.
#[must_use]
pub fn canonicalize_key(path: &str) -> String {
    // Drop the scheme and authority (`s3://bucket`) when present, then strip
    // any leading slash from a bare path.
    path.find("://").map_or_else(
        || path.trim_start_matches('/').to_string(),
        |idx| {
            let after_scheme = &path[idx + 3..];
            // Drop the authority (bucket/host): everything up to and including
            // the first '/'. A URI with no path component canonicalizes to "".
            after_scheme.split_once('/').map_or("", |(_authority, rest)| rest).to_string()
        },
    )
}

/// Whether a swept object is a data file or an Iceberg metadata file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectClass {
    /// A parquet data file under `<table>/data/`.
    Data,
    /// An Iceberg metadata file under `<table>/metadata/`.
    Metadata,
}

/// The decision for a single listed object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The object is referenced by the current table metadata; keep it.
    Referenced,
    /// The object is unreferenced but inside the grace period; keep it.
    TooYoung,
    /// The object is orphaned metadata but metadata sweeping is disabled.
    SkipMetadataDisabled,
    /// The object is not under `data/` or `metadata/`; keep it (unknown layout).
    SkipUnknownLayout,
    /// The object is an orphan and may be deleted.
    Delete(ObjectClass),
}

/// Classify one listed object key against the referenced set and grace cutoff.
///
/// `key` and the entries of `referenced` must both be canonical keys (see
/// [`canonicalize_key`]). `table_prefix` is the canonical table-root key
/// (e.g. `icegate/logs`). An object is [`Decision::Delete`] only when it is
/// **not** referenced **and** its `last_modified` is at or before `cutoff`.
///
/// The function is generic over the `HashSet` hasher so callers are not forced
/// to use the default `RandomState`.
#[must_use]
pub fn classify<S: std::hash::BuildHasher>(
    key: &str,
    table_prefix: &str,
    referenced: &HashSet<String, S>,
    last_modified: DateTime<Utc>,
    cutoff: DateTime<Utc>,
    include_metadata: bool,
) -> Decision {
    if referenced.contains(key) {
        return Decision::Referenced;
    }
    // Require a `/` boundary after the prefix so a sibling key that merely
    // shares the prefix string (e.g. `icegate/logsdata/...` vs table root
    // `icegate/logs`) is not mistaken for a table-local object.
    let rel = key
        .strip_prefix(table_prefix)
        .and_then(|r| r.strip_prefix('/'))
        .map(|r| r.trim_start_matches('/'));
    let class = match rel {
        Some(r) if r.starts_with("data/") => ObjectClass::Data,
        Some(r) if r.starts_with("metadata/") => ObjectClass::Metadata,
        _ => return Decision::SkipUnknownLayout,
    };
    if matches!(class, ObjectClass::Metadata) && !include_metadata {
        return Decision::SkipMetadataDisabled;
    }
    if last_modified > cutoff {
        return Decision::TooYoung;
    }
    Decision::Delete(class)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use chrono::{TimeZone, Utc};

    use super::{Decision, ObjectClass, canonicalize_key, classify};

    #[test]
    fn canonicalize_strips_scheme_and_bucket() {
        assert_eq!(
            canonicalize_key("s3://warehouse/icegate/logs/data/f.parquet"),
            "icegate/logs/data/f.parquet"
        );
    }

    #[test]
    fn canonicalize_strips_leading_slash_when_no_scheme() {
        assert_eq!(
            canonicalize_key("/icegate/logs/data/f.parquet"),
            "icegate/logs/data/f.parquet"
        );
        assert_eq!(
            canonicalize_key("icegate/logs/data/f.parquet"),
            "icegate/logs/data/f.parquet"
        );
    }

    fn referenced(keys: &[&str]) -> HashSet<String> {
        keys.iter().map(|k| (*k).to_string()).collect()
    }

    #[test]
    fn referenced_object_is_kept() {
        let set = referenced(&["icegate/logs/data/live.parquet"]);
        let now = Utc.timestamp_opt(1_000_000, 0).unwrap();
        let decision = classify("icegate/logs/data/live.parquet", "icegate/logs", &set, now, now, true);
        assert_eq!(decision, Decision::Referenced);
    }

    #[test]
    fn unreferenced_old_data_is_deleted() {
        let set = referenced(&[]);
        let modified = Utc.timestamp_opt(1_000, 0).unwrap();
        let cutoff = Utc.timestamp_opt(2_000, 0).unwrap();
        let decision = classify(
            "icegate/logs/data/orphan.parquet",
            "icegate/logs",
            &set,
            modified,
            cutoff,
            true,
        );
        assert_eq!(decision, Decision::Delete(ObjectClass::Data));
    }

    #[test]
    fn unreferenced_young_object_is_too_young() {
        let set = referenced(&[]);
        let modified = Utc.timestamp_opt(3_000, 0).unwrap();
        let cutoff = Utc.timestamp_opt(2_000, 0).unwrap();
        let decision = classify(
            "icegate/logs/data/fresh.parquet",
            "icegate/logs",
            &set,
            modified,
            cutoff,
            true,
        );
        assert_eq!(decision, Decision::TooYoung);
    }

    #[test]
    fn unreferenced_old_metadata_is_deleted_when_included() {
        let set = referenced(&[]);
        let modified = Utc.timestamp_opt(1_000, 0).unwrap();
        let cutoff = Utc.timestamp_opt(2_000, 0).unwrap();
        let decision = classify(
            "icegate/logs/metadata/snap-1.avro",
            "icegate/logs",
            &set,
            modified,
            cutoff,
            true,
        );
        assert_eq!(decision, Decision::Delete(ObjectClass::Metadata));
    }

    #[test]
    fn metadata_is_skipped_when_excluded() {
        let set = referenced(&[]);
        let modified = Utc.timestamp_opt(1_000, 0).unwrap();
        let cutoff = Utc.timestamp_opt(2_000, 0).unwrap();
        let decision = classify(
            "icegate/logs/metadata/snap-1.avro",
            "icegate/logs",
            &set,
            modified,
            cutoff,
            false,
        );
        assert_eq!(decision, Decision::SkipMetadataDisabled);
    }

    #[test]
    fn sibling_prefix_without_boundary_is_unknown_layout() {
        // `icegate/logsdata/...` shares the table prefix `icegate/logs` as a raw
        // string but not as a path segment, so it must never be table-local.
        let set = referenced(&[]);
        let modified = Utc.timestamp_opt(1_000, 0).unwrap();
        let cutoff = Utc.timestamp_opt(2_000, 0).unwrap();
        let decision = classify(
            "icegate/logsdata/x.parquet",
            "icegate/logs",
            &set,
            modified,
            cutoff,
            true,
        );
        assert_eq!(decision, Decision::SkipUnknownLayout);
    }

    #[test]
    fn objects_outside_data_and_metadata_are_skipped() {
        let set = referenced(&[]);
        let modified = Utc.timestamp_opt(1_000, 0).unwrap();
        let cutoff = Utc.timestamp_opt(2_000, 0).unwrap();
        let decision = classify("icegate/logs/weird/x.bin", "icegate/logs", &set, modified, cutoff, true);
        assert_eq!(decision, Decision::SkipUnknownLayout);
    }

    #[test]
    fn equal_timestamp_is_at_the_grace_edge_and_deleted() {
        // last_modified == cutoff is NOT "> cutoff", so the object is collectable.
        let set = referenced(&[]);
        let t = Utc.timestamp_opt(2_000, 0).unwrap();
        let decision = classify("icegate/logs/data/edge.parquet", "icegate/logs", &set, t, t, true);
        assert_eq!(decision, Decision::Delete(ObjectClass::Data));
    }
}
