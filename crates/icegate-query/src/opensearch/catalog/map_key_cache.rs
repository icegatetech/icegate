//! MAP-key enumerations, reused while what they were computed from is unchanged.
//!
//! An enumeration is a function of the tenant, the table, the table's current snapshot and the
//! discovery window. The caller aligns the window's end to a fixed bucket, so two lookups with an
//! equal [`MapKeysVersion`] scan the same committed files over the same window: a hit returns what
//! a fresh scan would.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use icegate_common::TenantId;
use lru::LruCache;
use tokio::sync::Mutex;

/// The stored keys of each MAP column of one table: column name to keys, both in byte order.
pub(crate) type MapKeys = BTreeMap<String, BTreeSet<String>>;

/// What an enumeration was computed from, besides the tenant and the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MapKeysVersion {
    /// The table's current snapshot; `None` before its first commit.
    pub(crate) snapshot_id: Option<i64>,
    /// End of the discovery window, in seconds since the epoch.
    pub(crate) window_end_secs: i64,
}

/// One table's cached enumeration.
#[derive(Debug)]
struct CachedMapKeys {
    /// What the keys were enumerated at.
    version: MapKeysVersion,
    /// The enumerated keys, shared with every listing that reads them.
    keys: Arc<MapKeys>,
}

/// The MAP-key enumerations of the most recently used tenants, one entry per telemetry table.
///
/// Concurrent misses on one table each scan and store, the last store winning. A lookup answers
/// only for the exact version an entry was enumerated at, so a race costs a scan, never a wrong
/// answer.
#[derive(Debug)]
pub(crate) struct MapKeyCache {
    /// Tenant name to the enumerations of its tables, least recently used first out.
    tenants: Mutex<LruCache<String, BTreeMap<&'static str, CachedMapKeys>>>,
}

impl MapKeyCache {
    /// A cache holding the enumerations of at most `tenant_capacity` tenants.
    pub(crate) fn new(tenant_capacity: NonZeroUsize) -> Self {
        Self {
            tenants: Mutex::new(LruCache::new(tenant_capacity)),
        }
    }

    /// The keys of `table` for `tenant` if they were enumerated at exactly `version`. A lookup of
    /// a cached tenant makes it the most recently used.
    pub(crate) async fn find_map_keys(
        &self,
        tenant: &TenantId,
        table: &'static str,
        version: MapKeysVersion,
    ) -> Option<Arc<MapKeys>> {
        self.tenants
            .lock()
            .await
            .get(tenant.as_ref())?
            .get(table)
            .filter(|cached_keys| cached_keys.version == version)
            .map(|cached_keys| Arc::clone(&cached_keys.keys))
    }

    /// Keep `keys` as the enumeration of `table` for `tenant` at `version`, replacing the one
    /// stored before. Storing a tenant beyond the capacity evicts the least recently used one.
    pub(crate) async fn store_map_keys(
        &self,
        tenant: &TenantId,
        table: &'static str,
        version: MapKeysVersion,
        keys: Arc<MapKeys>,
    ) {
        self.tenants
            .lock()
            .await
            .get_or_insert_mut_ref(tenant.as_ref(), BTreeMap::new)
            .insert(table, CachedMapKeys { version, keys });
    }
}

#[cfg(test)]
mod tests {
    use icegate_common::schema::COL_LOG_ATTRIBUTES;
    use icegate_common::{LOGS_TABLE, SPANS_TABLE};

    use super::*;
    use crate::test_support::build_tenant;

    const fn build_version(snapshot_id: i64, window_end_secs: i64) -> MapKeysVersion {
        MapKeysVersion {
            snapshot_id: Some(snapshot_id),
            window_end_secs,
        }
    }

    fn build_map_keys(key: &str) -> Arc<MapKeys> {
        Arc::new(BTreeMap::from([(
            COL_LOG_ATTRIBUTES.to_string(),
            BTreeSet::from([key.to_string()]),
        )]))
    }

    const fn build_capacity(tenants: usize) -> NonZeroUsize {
        NonZeroUsize::new(tenants).expect("a non-zero capacity")
    }

    /// An entry answers only for the tenant, table and version it was enumerated at: a new
    /// snapshot or a later window end is a miss, so a hit is what a fresh scan would return.
    #[tokio::test]
    async fn an_entry_is_found_only_under_its_tenant_table_and_version() {
        let cache = MapKeyCache::new(build_capacity(8));
        cache
            .store_map_keys(
                &build_tenant("tenant-a"),
                LOGS_TABLE,
                build_version(1, 600),
                build_map_keys("http.method"),
            )
            .await;

        let tenant_a = build_tenant("tenant-a");
        assert_eq!(
            cache.find_map_keys(&tenant_a, LOGS_TABLE, build_version(1, 600)).await,
            Some(build_map_keys("http.method"))
        );
        assert_eq!(
            cache.find_map_keys(&tenant_a, LOGS_TABLE, build_version(2, 600)).await,
            None
        );
        assert_eq!(
            cache.find_map_keys(&tenant_a, LOGS_TABLE, build_version(1, 660)).await,
            None
        );
        assert_eq!(
            cache.find_map_keys(&tenant_a, SPANS_TABLE, build_version(1, 600)).await,
            None
        );
        assert_eq!(
            cache
                .find_map_keys(&build_tenant("tenant-b"), LOGS_TABLE, build_version(1, 600))
                .await,
            None
        );
        let before_first_commit = MapKeysVersion {
            snapshot_id: None,
            window_end_secs: 600,
        };
        assert_eq!(
            cache.find_map_keys(&tenant_a, LOGS_TABLE, before_first_commit).await,
            None
        );
    }

    #[tokio::test]
    async fn storing_a_newer_version_replaces_the_older_one() {
        let cache = MapKeyCache::new(build_capacity(8));
        let tenant_a = build_tenant("tenant-a");
        cache
            .store_map_keys(&tenant_a, LOGS_TABLE, build_version(1, 600), build_map_keys("old.key"))
            .await;

        cache
            .store_map_keys(&tenant_a, LOGS_TABLE, build_version(2, 600), build_map_keys("new.key"))
            .await;

        assert_eq!(
            cache.find_map_keys(&tenant_a, LOGS_TABLE, build_version(1, 600)).await,
            None
        );
        assert_eq!(
            cache.find_map_keys(&tenant_a, LOGS_TABLE, build_version(2, 600)).await,
            Some(build_map_keys("new.key"))
        );
    }

    /// The cache holds at most its capacity of tenants: storing one more evicts the least
    /// recently used tenant, and a lookup counts as a use.
    #[tokio::test]
    async fn the_least_recently_used_tenant_is_evicted_above_capacity() {
        let cache = MapKeyCache::new(build_capacity(2));
        let (tenant_a, tenant_b, tenant_c) = (
            build_tenant("tenant-a"),
            build_tenant("tenant-b"),
            build_tenant("tenant-c"),
        );
        cache
            .store_map_keys(&tenant_a, LOGS_TABLE, build_version(1, 600), build_map_keys("a"))
            .await;
        cache
            .store_map_keys(&tenant_b, LOGS_TABLE, build_version(1, 600), build_map_keys("b"))
            .await;
        assert_eq!(
            cache.find_map_keys(&tenant_a, LOGS_TABLE, build_version(1, 600)).await,
            Some(build_map_keys("a"))
        );

        cache
            .store_map_keys(&tenant_c, LOGS_TABLE, build_version(1, 600), build_map_keys("c"))
            .await;

        assert_eq!(
            cache.find_map_keys(&tenant_a, LOGS_TABLE, build_version(1, 600)).await,
            Some(build_map_keys("a"))
        );
        assert_eq!(
            cache.find_map_keys(&tenant_b, LOGS_TABLE, build_version(1, 600)).await,
            None
        );
        assert_eq!(
            cache.find_map_keys(&tenant_c, LOGS_TABLE, build_version(1, 600)).await,
            Some(build_map_keys("c"))
        );
    }
}
