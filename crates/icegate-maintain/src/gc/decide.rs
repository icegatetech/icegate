//! Pure orphan-classification logic. No I/O, so it is exhaustively unit-tested.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use object_store::path::Path as ObjectPath;

use crate::error::MaintainError;

/// Parse an Iceberg file URI into its bucket-relative object key.
///
/// Iceberg metadata records absolute URIs (`s3://bucket/icegate/logs/data/f.parquet`),
/// while an object-store `list` yields bucket-relative keys
/// (`icegate/logs/data/f.parquet`). Dropping the scheme and authority places both
/// in the same key space, so the sweep can compare them directly (both as
/// [`ObjectPath`], the type `list` already returns).
///
/// The strip is textual, and the remainder is keyed through [`ObjectPath::from`]
/// — the same constructor a listed key arrives through — because the two must
/// normalise one object identically or the sweep deletes a live file. What
/// forbids percent-decoding here is that the escapes are not transport encoding:
/// Iceberg escapes a partition value into the path it then writes to, so a
/// `tenant_id` of `a:b` becomes the literal key `tenant_id=a%3Ab` and `%3A` is
/// three characters of the object's name. Decoding it invents a key that no
/// object has, and the miss reads as "unreferenced".
///
/// # Errors
///
/// Returns [`MaintainError::Storage`] if `uri` carries no `scheme://` or names no
/// object under its authority. Callers MUST treat this as fail-closed (delete
/// nothing): an unparseable referenced path could otherwise drop a live file.
pub(crate) fn parse_object_key(uri: &str) -> Result<ObjectPath, MaintainError> {
    let authority_and_key = uri
        .split_once("://")
        .map(|(_scheme, rest)| rest)
        .ok_or_else(|| MaintainError::Storage(format!("gc: malformed referenced URI '{uri}': no scheme")))?;
    let key = authority_and_key
        .split_once('/')
        .map(|(_authority, key)| key)
        .ok_or_else(|| MaintainError::Storage(format!("gc: referenced URI '{uri}' names no object")))?;
    Ok(ObjectPath::from(key))
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

impl Decision {
    /// Classify one listed object key against the referenced set and grace cutoff.
    ///
    /// `key` is a listed object's bucket-relative key and `referenced` holds the
    /// bucket-relative keys the table currently references (see
    /// [`parse_object_key`]). `table_prefix` is the bucket-relative table-root key
    /// (e.g. `icegate/logs`). An object is [`Decision::Delete`] only when it is
    /// **not** referenced **and** its `last_modified` is at or before `cutoff`.
    ///
    /// Generic over the `HashSet` hasher so callers are not forced to use the
    /// default `RandomState`.
    #[must_use]
    pub fn classify<S: std::hash::BuildHasher>(
        key: &ObjectPath,
        table_prefix: &str,
        referenced: &HashSet<ObjectPath, S>,
        last_modified: DateTime<Utc>,
        cutoff: DateTime<Utc>,
        include_metadata: bool,
    ) -> Self {
        if referenced.contains(key) {
            return Self::Referenced;
        }
        // Require a `/` boundary after the prefix so a sibling key that merely
        // shares the prefix string (e.g. `icegate/logsdata/...` vs table root
        // `icegate/logs`) is not mistaken for a table-local object.
        let rel = key
            .as_ref()
            .strip_prefix(table_prefix)
            .and_then(|r| r.strip_prefix('/'))
            .map(|r| r.trim_start_matches('/'));
        let class = match rel {
            Some(r) if r.starts_with("data/") => ObjectClass::Data,
            Some(r) if r.starts_with("metadata/") => ObjectClass::Metadata,
            _ => return Self::SkipUnknownLayout,
        };
        if matches!(class, ObjectClass::Metadata) && !include_metadata {
            return Self::SkipMetadataDisabled;
        }
        if last_modified > cutoff {
            return Self::TooYoung;
        }
        Self::Delete(class)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use chrono::{TimeZone, Utc};
    use object_store::path::Path as ObjectPath;

    use super::{Decision, ObjectClass, parse_object_key};
    use crate::error::MaintainError;

    #[test]
    fn parse_object_key_strips_scheme_and_bucket() {
        assert_eq!(
            parse_object_key("s3://warehouse/icegate/logs/data/f.parquet").unwrap().as_ref(),
            "icegate/logs/data/f.parquet"
        );
    }

    /// The referenced set and the object listing MUST agree on the key of one
    /// object. Iceberg percent-escapes partition values into the data-file path
    /// (`form_urlencoded`, so a tenant id's `:` becomes `%3A`), while a listed
    /// key reaches the sweep through `object_store`'s `Path`, whose encode set
    /// contains `%`. The two therefore normalise the same object differently
    /// unless the sweep keys both sides the same way -- and a disagreement here
    /// deletes a live file, because the delete path decodes back to the real key.
    #[test]
    fn an_escaped_partition_value_keys_the_same_on_both_sides() {
        // What Iceberg records in the manifest for a tenant id holding a colon.
        let uri = "s3://warehouse/icegate/logs/data/tenant_id=a%3Ab/timestamp_day=2026-09-09/f.parquet";
        // What `list` yields for that same object: the raw key, keyed through
        // `Path::from` exactly as `object_store_opendal` does.
        let listed = ObjectPath::from("icegate/logs/data/tenant_id=a%3Ab/timestamp_day=2026-09-09/f.parquet");

        assert_eq!(
            parse_object_key(uri).unwrap(),
            listed,
            "the referenced key and the listed key name the same object"
        );
    }

    /// The consequence of the disagreement above, at the boundary that acts on
    /// it: a live file of a colon-carrying tenant, old enough to be past the
    /// grace period, must be kept.
    #[test]
    fn a_referenced_file_under_an_escaped_partition_is_kept() {
        let uri = "s3://warehouse/icegate/logs/data/tenant_id=a%3Ab/timestamp_day=2026-09-09/f.parquet";
        let set: HashSet<ObjectPath> = std::iter::once(parse_object_key(uri).unwrap()).collect();
        let listed = ObjectPath::from("icegate/logs/data/tenant_id=a%3Ab/timestamp_day=2026-09-09/f.parquet");
        let modified = Utc.timestamp_opt(1_000, 0).unwrap();
        let cutoff = Utc.timestamp_opt(2_000, 0).unwrap();

        let decision = Decision::classify(&listed, "icegate/logs", &set, modified, cutoff, true);

        assert_eq!(decision, Decision::Referenced);
    }

    #[test]
    fn parse_object_key_rejects_a_scheme_less_path() {
        // A bare, scheme-less path is not a valid absolute URI; the sweep must
        // fail closed rather than silently mis-key a referenced file.
        assert!(matches!(
            parse_object_key("icegate/logs/data/f.parquet"),
            Err(MaintainError::Storage(_))
        ));
    }

    fn referenced(keys: &[&str]) -> HashSet<ObjectPath> {
        keys.iter().map(|k| ObjectPath::from(*k)).collect()
    }

    #[test]
    fn referenced_object_is_kept() {
        let set = referenced(&["icegate/logs/data/live.parquet"]);
        let now = Utc.timestamp_opt(1_000_000, 0).unwrap();
        let decision = Decision::classify(
            &ObjectPath::from("icegate/logs/data/live.parquet"),
            "icegate/logs",
            &set,
            now,
            now,
            true,
        );
        assert_eq!(decision, Decision::Referenced);
    }

    #[test]
    fn unreferenced_old_data_is_deleted() {
        let set = referenced(&[]);
        let modified = Utc.timestamp_opt(1_000, 0).unwrap();
        let cutoff = Utc.timestamp_opt(2_000, 0).unwrap();
        let decision = Decision::classify(
            &ObjectPath::from("icegate/logs/data/orphan.parquet"),
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
        let decision = Decision::classify(
            &ObjectPath::from("icegate/logs/data/fresh.parquet"),
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
        let decision = Decision::classify(
            &ObjectPath::from("icegate/logs/metadata/snap-1.avro"),
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
        let decision = Decision::classify(
            &ObjectPath::from("icegate/logs/metadata/snap-1.avro"),
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
        let decision = Decision::classify(
            &ObjectPath::from("icegate/logsdata/x.parquet"),
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
        let decision = Decision::classify(
            &ObjectPath::from("icegate/logs/weird/x.bin"),
            "icegate/logs",
            &set,
            modified,
            cutoff,
            true,
        );
        assert_eq!(decision, Decision::SkipUnknownLayout);
    }

    #[test]
    fn equal_timestamp_is_at_the_grace_edge_and_deleted() {
        // last_modified == cutoff is NOT "> cutoff", so the object is collectable.
        let set = referenced(&[]);
        let t = Utc.timestamp_opt(2_000, 0).unwrap();
        let decision = Decision::classify(
            &ObjectPath::from("icegate/logs/data/edge.parquet"),
            "icegate/logs",
            &set,
            t,
            t,
            true,
        );
        assert_eq!(decision, Decision::Delete(ObjectClass::Data));
    }
}
