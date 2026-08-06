//! Snapshot-retention policy that `migrate` writes onto the tables it creates.
//!
//! Expiration itself is not run here, nor anywhere in this crate: every commit
//! against a table — an ingest shift, a compaction rewrite, a pricing append —
//! resolves the policy from that table's own properties and carries the
//! `RemoveSnapshots` update along with whatever it was already writing. This
//! config is only the source of the values `migrate create` stamps into the
//! table, which is what makes them auditable in `metadata.json` instead of an
//! implicit default living in code.

use std::collections::HashMap;

use iceberg::spec::TableProperties;
use serde::{Deserialize, Serialize};

use crate::error::MaintainError;

/// Number of newest ancestors of the current snapshot that always survive.
///
/// IceGate does not use time travel, so history exists only to keep the WAL
/// offset and the in-flight readers of a snapshot reachable; this much slack is
/// well past both and still small enough that expiration is what bounds the
/// metadata, rather than the Iceberg spec default of a single ancestor.
const DEFAULT_MIN_SNAPSHOTS_TO_KEEP: usize = 100;

/// Age past which a snapshot is expired.
///
/// The floor is the query engine's provider cache (`query.engine.max_age_secs`):
/// a snapshot must outlive any cached reference to it, or a query can plan
/// against manifests the sweep has already collected.
const DEFAULT_MAX_SNAPSHOT_AGE_MS: i64 = 30 * 60 * 1000;

/// Number of superseded `metadata.json` versions a table keeps.
///
/// Beyond the retention window a stored `metadata.json` is not a restore point:
/// its snapshots' manifests and data files became orphans the moment those
/// snapshots expired and are swept once GC's grace period passes, so keeping
/// versions past the window only preserves files that no longer describe
/// recoverable data. Twice the snapshot window rather than exactly it: the S3
/// catalog resolves a lost commit ack by looking its own metadata file up in
/// the head's metadata-log (`icegate-catalog-s3` `root.rs`), and that verdict
/// is sound only while the log has not been truncated past it.
const DEFAULT_METADATA_PREVIOUS_VERSIONS_MAX: usize = 2 * DEFAULT_MIN_SNAPSHOTS_TO_KEEP;

/// Snapshot-retention policy stamped onto every table `migrate create` creates.
///
/// Field-for-field the `history.expire.*` table properties of the Iceberg spec,
/// plus the metadata-log bound that has to move with them.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotExpirationConfig {
    /// Whether created tables expire snapshots at all. Writing `false` here does
    /// not stop expiration on tables that already exist — the property on the
    /// table is what every writer resolves, so an existing table is turned off
    /// by setting `history.expire.enabled=false` on the table itself.
    pub enabled: bool,
    /// Newest ancestors of the current snapshot that always survive.
    pub min_snapshots_to_keep: usize,
    /// Age in milliseconds past which a snapshot is eligible for expiration.
    pub max_snapshot_age_ms: i64,
    /// Superseded `metadata.json` versions a table keeps.
    pub metadata_previous_versions_max: usize,
}

impl Default for SnapshotExpirationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_snapshots_to_keep: DEFAULT_MIN_SNAPSHOTS_TO_KEEP,
            max_snapshot_age_ms: DEFAULT_MAX_SNAPSHOT_AGE_MS,
            metadata_previous_versions_max: DEFAULT_METADATA_PREVIOUS_VERSIONS_MAX,
        }
    }
}

impl SnapshotExpirationConfig {
    /// The `history.expire.*` properties this policy contributes to a table.
    ///
    /// `preserved_summary_property` names a snapshot-summary key whose most
    /// recent carrier — and the ancestor chain leading to it — expiration must
    /// keep reachable. Tables whose snapshots carry no such key pass `None`:
    /// the property is a promise about that table's summaries, and naming a key
    /// nothing writes only tells every writer to look for a carrier that will
    /// never be there.
    pub fn build_table_properties(&self, preserved_summary_property: Option<&str>) -> HashMap<String, String> {
        // `enabled` is written either way: absent reads as "off" to a writer,
        // so an explicit `false` is what distinguishes a table deliberately
        // opted out from one created before this policy existed.
        let mut properties = HashMap::from([
            (
                TableProperties::PROPERTY_HISTORY_EXPIRE_ENABLED.to_string(),
                self.enabled.to_string(),
            ),
            (
                TableProperties::PROPERTY_HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP.to_string(),
                self.min_snapshots_to_keep.to_string(),
            ),
            (
                TableProperties::PROPERTY_HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS.to_string(),
                self.max_snapshot_age_ms.to_string(),
            ),
            (
                TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX.to_string(),
                self.metadata_previous_versions_max.to_string(),
            ),
        ]);

        if let Some(key) = preserved_summary_property {
            properties.insert(
                TableProperties::PROPERTY_HISTORY_EXPIRE_PRESERVE_SUMMARY_PROPERTY.to_string(),
                key.to_string(),
            );
        }

        properties
    }

    /// Validate the retention window.
    ///
    /// # Errors
    ///
    /// Returns [`MaintainError::Config`] when a bound is zero or negative, or
    /// when the metadata-log bound is shorter than the snapshot window — a
    /// table would then drop `metadata.json` versions covering snapshots it is
    /// still required to keep, and the S3 catalog loses the metadata-log
    /// evidence it resolves a lost commit ack with.
    pub fn validate(&self) -> Result<(), MaintainError> {
        if self.min_snapshots_to_keep == 0 {
            return Err(MaintainError::Config(
                "snapshot_expiration.min_snapshots_to_keep must be greater than zero".to_string(),
            ));
        }
        if self.max_snapshot_age_ms <= 0 {
            return Err(MaintainError::Config(
                "snapshot_expiration.max_snapshot_age_ms must be greater than zero".to_string(),
            ));
        }
        if self.metadata_previous_versions_max < self.min_snapshots_to_keep {
            return Err(MaintainError::Config(format!(
                "snapshot_expiration.metadata_previous_versions_max ({}) must be at least \
                 min_snapshots_to_keep ({}): the table would drop metadata.json versions \
                 covering snapshots it is still required to keep, and the S3 catalog resolves \
                 a lost commit ack by finding its own metadata file in the head's metadata-log \
                 — a log truncated past that point turns a landed commit into an unrecognised one",
                self.metadata_previous_versions_max, self.min_snapshots_to_keep
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use iceberg::spec::TableProperties;
    use icegate_common::WAL_OFFSET_PROPERTY;

    use super::SnapshotExpirationConfig;

    #[test]
    fn defaults_keep_a_hundred_snapshots_for_thirty_minutes() {
        let config = SnapshotExpirationConfig::default();

        assert!(config.enabled);
        assert_eq!(config.min_snapshots_to_keep, 100);
        assert_eq!(config.max_snapshot_age_ms, 1_800_000);
        assert_eq!(config.metadata_previous_versions_max, 200);
        config.validate().expect("the shipped defaults must validate");
    }

    /// `DEFAULT_MAX_AGE_SECS` of `icegate-query` `engine/config.rs`: the width of
    /// the query engine's provider cache. `icegate-maintain` does not depend on
    /// that crate, so this is a hand-synced copy — the one place the floor is
    /// named here, rather than a literal inside the assertion.
    const QUERY_PROVIDER_CACHE_SECS: i64 = 30;

    /// The window has to outlive the query engine's provider cache, or a cached
    /// provider can plan against a snapshot whose files GC has already swept.
    /// The chart checks the two *configured* values against each other; this
    /// checks the two defaults, which is what a Compose or hand-written
    /// deployment runs on.
    #[test]
    fn default_window_outlives_the_query_provider_cache() {
        assert!(
            SnapshotExpirationConfig::default().max_snapshot_age_ms > QUERY_PROVIDER_CACHE_SECS * 1000,
            "the default retention window must clear the query provider cache"
        );
    }

    #[test]
    fn properties_carry_the_window_and_the_preserved_summary_key() {
        let properties = SnapshotExpirationConfig::default().build_table_properties(Some(WAL_OFFSET_PROPERTY));

        assert_eq!(
            properties.get(TableProperties::PROPERTY_HISTORY_EXPIRE_ENABLED),
            Some(&"true".to_string())
        );
        assert_eq!(
            properties.get(TableProperties::PROPERTY_HISTORY_EXPIRE_MIN_SNAPSHOTS_TO_KEEP),
            Some(&"100".to_string())
        );
        assert_eq!(
            properties.get(TableProperties::PROPERTY_HISTORY_EXPIRE_MAX_SNAPSHOT_AGE_MS),
            Some(&"1800000".to_string())
        );
        assert_eq!(
            properties.get(TableProperties::PROPERTY_METADATA_PREVIOUS_VERSIONS_MAX),
            Some(&"200".to_string())
        );
        assert_eq!(
            properties.get(TableProperties::PROPERTY_HISTORY_EXPIRE_PRESERVE_SUMMARY_PROPERTY),
            Some(&"icegate.queue.offset".to_string())
        );
    }

    /// A table nothing stamps the WAL offset onto must not advertise a carrier:
    /// writers would look for one on every expiring commit and log that the
    /// history is unprotected, which for that table is simply the truth.
    #[test]
    fn no_preserved_key_is_written_when_the_table_carries_none() {
        let properties = SnapshotExpirationConfig::default().build_table_properties(None);

        assert!(
            !properties.contains_key(TableProperties::PROPERTY_HISTORY_EXPIRE_PRESERVE_SUMMARY_PROPERTY),
            "a table with no offset-carrying snapshots must not name a preserved summary key"
        );
    }

    /// Disabled is written as an explicit `false` rather than by omitting the
    /// key, so the table records the decision instead of looking untouched.
    #[test]
    fn disabled_policy_writes_an_explicit_false() {
        let config = SnapshotExpirationConfig {
            enabled: false,
            ..SnapshotExpirationConfig::default()
        };

        assert_eq!(
            config.build_table_properties(None).get("history.expire.enabled"),
            Some(&"false".to_string())
        );
    }

    #[test]
    fn validate_rejects_a_zero_snapshot_window() {
        let config = SnapshotExpirationConfig {
            min_snapshots_to_keep: 0,
            ..SnapshotExpirationConfig::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_rejects_a_non_positive_age() {
        for max_snapshot_age_ms in [0, -1] {
            let config = SnapshotExpirationConfig {
                max_snapshot_age_ms,
                ..SnapshotExpirationConfig::default()
            };

            assert!(
                config.validate().is_err(),
                "max_snapshot_age_ms {max_snapshot_age_ms} must be rejected"
            );
        }
    }

    #[test]
    fn validate_rejects_a_metadata_log_shorter_than_the_snapshot_window() {
        let config = SnapshotExpirationConfig {
            min_snapshots_to_keep: 10,
            metadata_previous_versions_max: 9,
            ..SnapshotExpirationConfig::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn validate_accepts_a_metadata_log_exactly_as_long_as_the_window() {
        let config = SnapshotExpirationConfig {
            min_snapshots_to_keep: 10,
            metadata_previous_versions_max: 10,
            ..SnapshotExpirationConfig::default()
        };

        assert!(config.validate().is_ok());
    }
}
